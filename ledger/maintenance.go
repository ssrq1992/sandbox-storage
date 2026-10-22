package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

// BackendPVC resolves the identity already fixed by Register. It does not
// adopt a Kubernetes alias or create a new ledger volume for the driver.
func (s *Store) BackendPVC(ctx context.Context, driver, handle string) (string, error) {
	if driver == "" || handle == "" {
		return "", core.ErrConflict
	}
	var uid string
	err := s.executor(ctx).QueryRow(ctx, `SELECT pvc_uid FROM ssf_volumes WHERE key=$1`, (core.Volume{Driver: driver, Handle: handle}).Key()).Scan(&uid)
	if err == nil && uid == "" {
		err = core.ErrConflict
	}
	return uid, err
}

// ReserveBackendExpansion joins a facade's existing fence or obtains the same
// common offline fence for a native Kubernetes PVC resize. It never performs
// storage I/O. Active writers and paused holds keep the request pending.
func (s *Store) ReserveBackendExpansion(ctx context.Context, uid, operation string, target int64, requested ...int64) error {
	if uid == "" || operation == "" || target <= 0 {
		return core.ErrConflict
	}
	return s.Exclusive(ctx, "maintenance/reserve/"+uid, func(ctx context.Context) error {
		tx, err := s.executor(ctx).Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		var existing int64
		var key, token string
		err = tx.QueryRow(ctx, `SELECT v.key,m.token,m.target_bytes FROM ssf_volume_maintenance m JOIN ssf_volumes v ON v.key=m.volume_key WHERE v.pvc_uid=$1 AND m.state='Active' FOR UPDATE OF v,m`, uid).Scan(&key, &token, &existing)
		if err == nil {
			if existing == target {
				return tx.Commit(ctx)
			}
			// A native resizer may round a PVC request to its allocation unit.
			// Only the exact admitted request may be rounded, before any
			// backend operation has been authorized under this fence.
			if len(requested) != 1 || requested[0] != existing || target < existing {
				return core.ErrConflict
			}
			tag, updateErr := tx.Exec(ctx, `UPDATE ssf_volume_maintenance m SET target_bytes=$3 FROM ssf_volumes v WHERE m.volume_key=v.key AND v.pvc_uid=$1 AND m.state='Active' AND m.target_bytes=$2 AND NOT EXISTS(SELECT 1 FROM ssf_steps x WHERE x.id=$4) AND m.token=$5`, uid, existing, target, "maintenance/backend/"+core.Hash(key+"\x00"+token), token)
			if updateErr != nil {
				return updateErr
			}
			if tag.RowsAffected() != 1 {
				return core.ErrConflict
			}
			return tx.Commit(ctx)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err = tx.Rollback(ctx); err != nil {
			return err
		}
		return s.BeginOfflineMaintenance(ctx, uid, "backend/"+operation, target)
	})
}

// CompleteBackendExpansion accepts only the operation fixed by the original
// authorization. Call only after the native publication and driver catalog
// are durable, never merely after changing a PVC request or PV metadata.
func (s *Store) CompleteBackendExpansion(ctx context.Context, uid, operation string, target int64) error {
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var key, token, state string
	var capacity int64
	if err = tx.QueryRow(ctx, `SELECT v.key,m.token,m.target_bytes,m.state FROM ssf_volumes v JOIN ssf_volume_maintenance m ON m.volume_key=v.key WHERE v.pvc_uid=$1 FOR UPDATE OF v,m`, uid).Scan(&key, &token, &capacity, &state); err != nil {
		return err
	}
	// A replay of an earlier completed resize must not release a newer fence.
	if capacity > target || (capacity == target && state == "Complete") {
		return tx.Commit(ctx)
	}
	if target != capacity || state != "Active" {
		return core.ErrConflict
	}
	var pinned string
	if err = tx.QueryRow(ctx, `SELECT result->>'Operation' FROM ssf_steps WHERE id=$1 AND state='Done'`, "maintenance/backend/"+core.Hash(key+"\x00"+token)).Scan(&pinned); err != nil {
		return err
	}
	if pinned != operation {
		return core.ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE ssf_volume_maintenance SET state='Complete' WHERE volume_key=$1`, key); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// AuthorizeBackendExpansion binds a driver operation to an existing offline
// fence. A CSI request or StorageClass expansion flag alone grants no authority.
// Canonical backend identity and PVC UID must agree, including for aliases.
func (s *Store) AuthorizeBackendExpansion(ctx context.Context, driver, handle, uid, operation, head string, target int64) (string, error) {
	if driver == "" || handle == "" || uid == "" || operation == "" || head == "" || target <= 0 {
		return "", errors.New("complete maintenance backend identity required")
	}
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	key := (core.Volume{Driver: driver, Handle: handle}).Key()
	var actualUID string
	var draining bool
	if err = tx.QueryRow(ctx, `SELECT pvc_uid,draining FROM ssf_volumes WHERE key=$1 FOR UPDATE`, key).Scan(&actualUID, &draining); err != nil {
		return "", err
	}
	if draining || actualUID != uid {
		return "", core.ErrConflict
	}
	var token, state string
	var bytes int64
	if err = tx.QueryRow(ctx, `SELECT token,target_bytes,state FROM ssf_volume_maintenance WHERE volume_key=$1`, key).Scan(&token, &bytes, &state); err != nil {
		return "", err
	}
	if state != "Active" || bytes != target {
		return "", core.ErrConflict
	}
	raw, err := json.Marshal(struct {
		Version                                        int
		Driver, Handle, PVCUID, Token, Operation, Head string
		Target                                         int64
	}{1, driver, handle, uid, token, operation, head, target})
	if err != nil {
		return "", err
	}
	proof := core.Hash(string(raw))
	step := "maintenance/backend/" + core.Hash(key+"\x00"+token)
	tag, err := tx.Exec(ctx, `INSERT INTO ssf_steps(id,digest,state,result) VALUES($1,$2,'Done',$3) ON CONFLICT(id) DO UPDATE SET id=excluded.id WHERE ssf_steps.digest=excluded.digest`, step, proof, raw)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "", core.ErrConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return proof, nil
}

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
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_bindings WHERE volume_key=$1 AND state<>'Released') OR EXISTS(SELECT 1 FROM ssf_use_holds WHERE volume_key=$1) OR EXISTS(SELECT 1 FROM ssf_attachments WHERE volume_key=$1 AND state<>'Detached') OR EXISTS(SELECT 1 FROM ssf_idle_captures WHERE volume_key=$1 AND state='Capturing') OR EXISTS(SELECT 1 FROM ssf_volume_preparations WHERE pvc_uid=$2 AND state IN ('Preparing','ContentReady','NeedsRecovery'))`, key, uid).Scan(&active); err != nil {
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
