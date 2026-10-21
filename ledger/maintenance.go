package ledger

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

func (s *Store) AllowedExpansion(ctx context.Context, uid string, target int64) (bool, error) {
	if uid == "" || target <= 0 {
		return false, nil
	}
	var allowed bool
	err := s.executor(ctx).QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM ssf_volumes v JOIN ssf_volume_maintenance m ON m.volume_key=v.key
 WHERE v.pvc_uid=$1 AND NOT v.draining AND m.state='Active' AND m.target_bytes=$2)`, uid, target).Scan(&allowed)
	return allowed, err
}

// BeginOfflineMaintenance fences new bindings before any external mutation.
// It has no lease: a process crash retains the fence until its durable operation
// is explicitly completed. Paused logical owners are active consumers too.
func (s *Store) BeginOfflineMaintenance(ctx context.Context, uid, token string, target int64) error {
	if uid == "" || token == "" || target <= 0 {
		return errors.New("maintenance identity and capacity required")
	}
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var key string
	var draining bool
	if err = tx.QueryRow(ctx, `SELECT key,draining FROM ssf_volumes WHERE pvc_uid=$1 FOR UPDATE`, uid).Scan(&key, &draining); err != nil {
		return err
	}
	if draining {
		return core.ErrConflict
	}
	var prior, state string
	var size int64
	err = tx.QueryRow(ctx, `SELECT token,target_bytes,state FROM ssf_volume_maintenance WHERE volume_key=$1`, key).Scan(&prior, &size, &state)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil && state == "Active" {
		if prior != token || size != target {
			return core.ErrConflict
		}
		return tx.Commit(ctx)
	}
	var active bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_bindings WHERE volume_key=$1 AND state<>'Released') OR EXISTS(SELECT 1 FROM ssf_use_holds WHERE volume_key=$1) OR EXISTS(SELECT 1 FROM ssf_attachments WHERE volume_key=$1 AND state<>'Detached')`, key).Scan(&active); err != nil {
		return err
	}
	if active {
		return core.ErrPending
	}
	_, err = tx.Exec(ctx, `INSERT INTO ssf_volume_maintenance(volume_key,token,target_bytes,state) VALUES($1,$2,$3,'Active') ON CONFLICT(volume_key) DO UPDATE SET token=excluded.token,target_bytes=excluded.target_bytes,state='Active'`, key, token, target)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Completion is idempotent only for the same immutable maintenance operation.
func (s *Store) CompleteOfflineMaintenance(ctx context.Context, uid, token string, target int64) error {
	tag, err := s.executor(ctx).Exec(ctx, `UPDATE ssf_volume_maintenance m SET state='Complete' FROM ssf_volumes v WHERE m.volume_key=v.key AND v.pvc_uid=$1 AND m.token=$2 AND m.target_bytes=$3`, uid, token, target)
	if err == nil && tag.RowsAffected() != 1 {
		return core.ErrConflict
	}
	return err
}
