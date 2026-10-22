package ledger

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

// IdleCapture is a storage operation, not a sandbox run or a runtime barrier.
// Its identity and immutable receipt survive the caller and node processes.
type IdleCapture struct {
	ID      string
	Volume  core.Volume
	Digest  string
	State   string
	Receipt json.RawMessage
}

// BeginIdleCapture fences use before inspecting a published head. A crash has
// no lease expiry: retry the same operation or explicitly retire it. Ready
// captures no longer fence writers, but retain a source reference until retired.
func (s *Store) BeginIdleCapture(ctx context.Context, id string, volume core.Volume, input any) (IdleCapture, error) {
	return s.beginPublishedCapture(ctx, id, volume, input, false)
}

// BeginPublishedCapture is for managed drivers that publish final content before
// releasing a runtime binding. A paused logical hold may stay, but every owner
// run must have trusted StopRun evidence and no device/attachment may remain.
// The same volume-row fence prevents resume while the head is being pinned.
func (s *Store) BeginPublishedCapture(ctx context.Context, id string, volume core.Volume, input any) (IdleCapture, error) {
	return s.beginPublishedCapture(ctx, id, volume, input, true)
}
func (s *Store) beginPublishedCapture(ctx context.Context, id string, volume core.Volume, input any, publishedPause bool) (IdleCapture, error) {
	var result IdleCapture
	if id == "" || volume.Driver == "" || volume.Handle == "" || volume.PVC.UID == "" {
		return result, core.ErrConflict
	}
	digest := core.Digest(struct {
		Volume core.Volume
		Input  any
	}{volume, input})
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	// Lock the operation first, including the unknown-delete tombstone race.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "ssf/idle-capture/"+id); err != nil {
		return result, err
	}
	var key, uid, state, prior string
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT COALESCE(volume_key,''),COALESCE(pvc_uid,''),digest,state,receipt FROM ssf_idle_captures WHERE id=$1`, id).Scan(&key, &uid, &prior, &state, &raw)
	if err == nil {
		if key != volume.Key() || uid != volume.PVC.UID || prior != digest || state == "Released" {
			return result, core.ErrConflict
		}
		result = IdleCapture{ID: id, Volume: volume, Digest: digest, State: state, Receipt: raw}
		return result, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	var draining bool
	err = tx.QueryRow(ctx, `SELECT pvc_uid,draining FROM ssf_volumes WHERE key=$1 FOR UPDATE`, volume.Key()).Scan(&uid, &draining)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, core.ErrPending
	}
	if err != nil {
		return result, err
	}
	if uid != volume.PVC.UID || draining {
		return result, core.ErrConflict
	}
	var active bool
	err = tx.QueryRow(ctx, `SELECT
 EXISTS(SELECT 1 FROM ssf_bindings WHERE volume_key=$1 AND state<>'Released') OR
 EXISTS(SELECT 1 FROM ssf_use_holds h WHERE h.volume_key=$1 AND (NOT $3 OR h.reason<>'paused' OR NOT EXISTS(SELECT 1 FROM ssf_runs r WHERE r.owner_uid=h.owner_uid AND r.state='Stopped') OR EXISTS(SELECT 1 FROM ssf_runs r WHERE r.owner_uid=h.owner_uid AND r.state<>'Stopped'))) OR
 EXISTS(SELECT 1 FROM ssf_attachments WHERE volume_key=$1 AND state<>'Detached') OR
 EXISTS(SELECT 1 FROM ssf_volume_maintenance WHERE volume_key=$1 AND state='Active') OR
 EXISTS(SELECT 1 FROM ssf_volume_preparations WHERE pvc_uid=$2 AND state IN ('Preparing','ContentReady','NeedsRecovery')) OR
 EXISTS(SELECT 1 FROM ssf_idle_captures WHERE volume_key=$1 AND state='Capturing')`, volume.Key(), uid, publishedPause).Scan(&active)
	if err != nil {
		return result, err
	}
	if active {
		return result, core.ErrPending
	}
	_, err = tx.Exec(ctx, `INSERT INTO ssf_idle_captures(id,volume_key,pvc_uid,digest,state) VALUES($1,$2,$3,$4,'Capturing')`, id, volume.Key(), uid, digest)
	if err != nil {
		return result, err
	}
	result = IdleCapture{ID: id, Volume: volume, Digest: digest, State: "Capturing"}
	return result, tx.Commit(ctx)
}

// CommitIdleCapture fixes the verified receipt before new consumers can start.
// This stores metadata only; the caller must verify the immutable backend head.
func (s *Store) CommitIdleCapture(ctx context.Context, id, digest string, receipt any) error {
	if id == "" || digest == "" || receipt == nil {
		return core.ErrConflict
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if string(raw) == "null" {
		return core.ErrConflict
	}
	tag, err := s.executor(ctx).Exec(ctx, `UPDATE ssf_idle_captures SET state='Ready',receipt=$3 WHERE id=$1 AND digest=$2 AND (state='Capturing' OR (state='Ready' AND receipt=$3::jsonb))`, id, digest, raw)
	if err == nil && tag.RowsAffected() != 1 {
		return core.ErrConflict
	}
	return err
}

// RetireIdleCapture is also valid before creation. Keep the tombstone so delayed
// requests cannot reacquire a fence or recreate a deleted snapshot reference.
func (s *Store) RetireIdleCapture(ctx context.Context, id string) error {
	if id == "" {
		return core.ErrConflict
	}
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "ssf/idle-capture/"+id); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ssf_idle_captures(id,digest,state) VALUES($1,'','Released') ON CONFLICT(id) DO UPDATE SET state='Released'`, id)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
