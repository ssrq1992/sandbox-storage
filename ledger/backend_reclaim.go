package ledger

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

// AuthorizeBackendReclaim is the final storage-driver fence. It neither issues
// a Kubernetes delete nor stops a sandbox. Registered volumes must already be
// draining; unobserved backends receive a tombstone that blocks later Register.
// The authorization has no expiry because an unknown backend delete can outlive
// any RPC deadline. Callers must authenticate their CSI controller identity.
func (s *Store) AuthorizeBackendReclaim(ctx context.Context, driver, handle, operation string) (string, error) {
	if driver == "" || handle == "" || operation == "" {
		return "", errors.New("canonical backend and reclaim operation required")
	}
	key := (core.Volume{Driver: driver, Handle: handle}).Key()
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "ssf/backend/"+key); err != nil {
		return "", err
	}
	var previous string
	err = tx.QueryRow(ctx, `SELECT operation FROM ssf_backend_tombstones WHERE volume_key=$1`, key).Scan(&previous)
	if err == nil {
		if previous != operation {
			return "", core.ErrConflict
		}
		return core.Hash("ssf/backend-reclaim/v1\x00" + key + "\x00" + operation), tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	var uid string
	var draining bool
	err = tx.QueryRow(ctx, `SELECT pvc_uid,draining FROM ssf_volumes WHERE key=$1 FOR UPDATE`, key).Scan(&uid, &draining)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if err == nil {
		if !draining {
			return "", core.ErrPending
		}
		var active bool
		err = tx.QueryRow(ctx, `SELECT
            EXISTS(SELECT 1 FROM ssf_bindings WHERE volume_key=$1 AND state<>'Released') OR
            EXISTS(SELECT 1 FROM ssf_use_holds WHERE volume_key=$1) OR
            EXISTS(SELECT 1 FROM ssf_attachments WHERE volume_key=$1 AND state<>'Detached') OR
            EXISTS(SELECT 1 FROM ssf_volume_maintenance WHERE volume_key=$1 AND state='Active') OR
            EXISTS(SELECT 1 FROM ssf_idle_captures WHERE volume_key=$1 AND state<>'Released') OR
            EXISTS(SELECT 1 FROM ssf_volume_preparations WHERE pvc_uid=$2 AND state IN ('Preparing','ContentReady','NeedsRecovery'))`, key, uid).Scan(&active)
		if err != nil {
			return "", err
		}
		if active {
			return "", core.ErrPending
		}
		if _, err = tx.Exec(ctx, `INSERT INTO ssf_checkpoint_resources(uid) VALUES($1) ON CONFLICT DO NOTHING`, uid); err != nil {
			return "", err
		}
		var state string
		if err = tx.QueryRow(ctx, `SELECT state FROM ssf_checkpoint_resources WHERE uid=$1 FOR UPDATE`, uid).Scan(&state); err != nil {
			return "", err
		}
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_checkpoint_pins WHERE resource_uid=$1)`, uid).Scan(&active); err != nil {
			return "", err
		}
		if active {
			return "", core.ErrPending
		}
		if _, err = tx.Exec(ctx, `UPDATE ssf_checkpoint_resources SET state='Deleting' WHERE uid=$1`, uid); err != nil {
			return "", err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ssf_backend_tombstones(volume_key,operation) VALUES($1,$2)`, key, operation); err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return core.Hash("ssf/backend-reclaim/v1\x00" + key + "\x00" + operation), nil
}
