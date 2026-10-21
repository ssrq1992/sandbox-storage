package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"sort"

	"ssf.dev/ssf/core"
)

// RunEpoch is durable across retries and monotonically increases for new runs.
// Calling it does not authorize a writer; Acquire checks active bindings/holds.
func (s *Store) RunEpoch(ctx context.Context, owner, run string) (uint64, error) {
	if owner == "" || run == "" {
		return 0, errors.New("owner and run required")
	}
	_, err := s.executor(ctx).Exec(ctx, `INSERT INTO ssf_runs(owner_uid,run_uid) VALUES($1,$2) ON CONFLICT DO NOTHING`, owner, run)
	if err != nil {
		return 0, err
	}
	var epoch uint64
	var state string
	err = s.executor(ctx).QueryRow(ctx, `SELECT epoch,state FROM ssf_runs WHERE owner_uid=$1 AND run_uid=$2`, owner, run).Scan(&epoch, &state)
	if err == nil && state == "Stopped" {
		err = core.ErrConflict
	}
	return epoch, err
}

// RetainUse must precede releasing a paused run. Its volume-row lock is shared
// with Acquire and reclaim permits, so pausing cannot open a writer/delete gap.
func (s *Store) RetainUse(ctx context.Context, op, reason string) error {
	if reason == "" {
		return errors.New("hold reason required")
	}
	bs, err := s.Bindings(ctx, op)
	if err != nil {
		return err
	}
	if len(bs) == 0 {
		return errors.New("no bindings to retain")
	}
	sort.Slice(bs, func(i, j int) bool { return bs[i].Volume.Key() < bs[j].Volume.Key() })
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, b := range bs {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "volume/"+b.Volume.Key()); err != nil {
			return err
		}
		var draining bool
		if err = tx.QueryRow(ctx, `SELECT draining FROM ssf_volumes WHERE key=$1 FOR UPDATE`, b.Volume.Key()).Scan(&draining); err != nil {
			return err
		}
		// A draining volume may still acquire its existing owner's pause hold;
		// a permit can only exist after all active bindings have been released.
		var state string
		if err = tx.QueryRow(ctx, `SELECT state FROM ssf_bindings WHERE id=$1 FOR UPDATE`, b.ID).Scan(&state); err != nil {
			return err
		}
		if state == "Released" || state == "Releasing" {
			return core.ErrConflict
		}
		_, err = tx.Exec(ctx, `INSERT INTO ssf_use_holds(volume_key,owner_uid,readonly,reason) VALUES($1,$2,$3,$4) ON CONFLICT(volume_key,owner_uid) DO UPDATE SET readonly=ssf_use_holds.readonly AND excluded.readonly,reason=excluded.reason`, b.Volume.Key(), b.Owner(), b.Volume.ReadOnly, reason)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// StopRun is called only following verified process exit/fencing. An active
// binding still blocks the next run until normal Node/Controller release ends.
func (s *Store) StopRun(ctx context.Context, owner, run string) error {
	if owner == "" || run == "" {
		return errors.New("owner and run required")
	}
	return s.Exclusive(ctx, "owner/"+owner, func(ctx context.Context) error {
		_, err := s.executor(ctx).Exec(ctx, `INSERT INTO ssf_runs(owner_uid,run_uid,state) VALUES($1,$2,'Stopped') ON CONFLICT(owner_uid,run_uid) DO UPDATE SET state='Stopped'`, owner, run)
		return err
	})
}

// ReleaseUse is final owner deletion, not pause or failed resume. The caller
// first closes native admission for this owner and confirms all runs stopped.
func (s *Store) ReleaseUse(ctx context.Context, owner string) error {
	if owner == "" {
		return errors.New("owner required")
	}
	return s.Exclusive(ctx, "owner/"+owner, func(ctx context.Context) error {
		tx, err := s.executor(ctx).Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		rows, err := tx.Query(ctx, `SELECT v.key FROM ssf_volumes v JOIN ssf_use_holds h ON h.volume_key=v.key WHERE h.owner_uid=$1 ORDER BY v.key FOR UPDATE OF v`, owner)
		if err != nil {
			return err
		}
		for rows.Next() {
			var key string
			if err = rows.Scan(&key); err != nil {
				rows.Close()
				return err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		var n int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_bindings WHERE owner_uid=$1 AND state<>'Released'`, owner).Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return core.ErrPending
		}
		_, err = tx.Exec(ctx, `INSERT INTO ssf_owner_tombstones(owner_uid) VALUES($1) ON CONFLICT DO NOTHING`, owner)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `DELETE FROM ssf_use_holds WHERE owner_uid=$1`, owner)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
}

type CheckpointSet struct {
	ID         string
	OwnerUID   string
	State      string
	Resources  []string
	Descriptor json.RawMessage
}

// BeginCheckpoint records the native capture intent before any snapshot RPC.
func (s *Store) BeginCheckpoint(ctx context.Context, id, owner string, intent any) error {
	if id == "" || owner == "" {
		return errors.New("checkpoint identity required")
	}
	b, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	digest := core.Digest(intent)
	_, err = s.executor(ctx).Exec(ctx, `INSERT INTO ssf_checkpoint_sets(id,owner_uid,digest,state,descriptor) VALUES($1,$2,$3,'Capturing',$4) ON CONFLICT DO NOTHING`, id, owner, digest, b)
	if err != nil {
		return err
	}
	var prior, who, state string
	err = s.executor(ctx).QueryRow(ctx, `SELECT digest,owner_uid,state FROM ssf_checkpoint_sets WHERE id=$1`, id).Scan(&prior, &who, &state)
	if err == nil && (prior != digest || who != owner || state == "Released") {
		return core.ErrConflict
	}
	return err
}

func (s *Store) PinCheckpoint(ctx context.Context, id string, uids ...string) error {
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var state string
	if err = tx.QueryRow(ctx, `SELECT state FROM ssf_checkpoint_sets WHERE id=$1 FOR UPDATE`, id).Scan(&state); err != nil {
		return err
	}
	if state == "Released" {
		return core.ErrConflict
	}
	if state == "Ready" {
		for _, uid := range uids {
			var exists bool
			if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_checkpoint_pins WHERE checkpoint_id=$1 AND resource_uid=$2)`, id, uid).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return core.ErrConflict
			}
		}
		return tx.Commit(ctx)
	}
	uids = append([]string(nil), uids...)
	sort.Strings(uids)
	for _, uid := range uids {
		if uid == "" {
			return errors.New("resource UID required")
		}
		if _, err = tx.Exec(ctx, `INSERT INTO ssf_checkpoint_resources(uid) VALUES($1) ON CONFLICT DO NOTHING`, uid); err != nil {
			return err
		}
		if err = tx.QueryRow(ctx, `SELECT state FROM ssf_checkpoint_resources WHERE uid=$1 FOR UPDATE`, uid).Scan(&state); err != nil {
			return err
		}
		if state != "Live" {
			return core.ErrConflict
		}
		if _, err = tx.Exec(ctx, `INSERT INTO ssf_checkpoint_pins(checkpoint_id,resource_uid) VALUES($1,$2) ON CONFLICT DO NOTHING`, id, uid); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) CommitCheckpoint(ctx context.Context, id string, manifest any) error {
	b, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var state string
	var prior []byte
	if err = tx.QueryRow(ctx, `SELECT state,descriptor FROM ssf_checkpoint_sets WHERE id=$1 FOR UPDATE`, id).Scan(&state, &prior); err != nil {
		return err
	}
	if state == "Ready" {
		var a, c any
		if err = json.Unmarshal(prior, &a); err != nil {
			return err
		}
		if err = json.Unmarshal(b, &c); err != nil {
			return err
		}
		if core.Digest(a) != core.Digest(c) {
			return core.ErrConflict
		}
		return tx.Commit(ctx)
	}
	if state != "Capturing" {
		return core.ErrConflict
	}
	var n int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_checkpoint_pins WHERE checkpoint_id=$1`, id).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		return errors.New("checkpoint has no protected resources")
	}
	if _, err = tx.Exec(ctx, `UPDATE ssf_checkpoint_sets SET state='Ready',descriptor=$2 WHERE id=$1`, id, b); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Checkpoint(ctx context.Context, id string) (CheckpointSet, error) {
	var c CheckpointSet
	err := s.executor(ctx).QueryRow(ctx, `SELECT id,owner_uid,state,descriptor FROM ssf_checkpoint_sets WHERE id=$1`, id).Scan(&c.ID, &c.OwnerUID, &c.State, &c.Descriptor)
	if err != nil {
		return c, err
	}
	rows, err := s.executor(ctx).Query(ctx, `SELECT resource_uid FROM ssf_checkpoint_pins WHERE checkpoint_id=$1 ORDER BY resource_uid`, id)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var uid string
		if err = rows.Scan(&uid); err != nil {
			return c, err
		}
		c.Resources = append(c.Resources, uid)
	}
	return c, rows.Err()
}

func (s *Store) ReleaseCheckpoint(ctx context.Context, id string) error {
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE ssf_checkpoint_sets SET state='Released' WHERE id=$1`, id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM ssf_checkpoint_pins WHERE checkpoint_id=$1`, id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ssf_checkpoint_gc(checkpoint_id) SELECT id FROM ssf_checkpoint_sets WHERE id=$1 ON CONFLICT DO NOTHING`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// PermitSnapshotDelete leaves a tombstone. A concurrent new pin either wins
// first and prevents deletion, or observes Deleting and fails closed.
func (s *Store) PermitSnapshotDelete(ctx context.Context, uid string) error {
	if uid == "" {
		return errors.New("snapshot UID required")
	}
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO ssf_checkpoint_resources(uid) VALUES($1) ON CONFLICT DO NOTHING`, uid); err != nil {
		return err
	}
	var state string
	if err = tx.QueryRow(ctx, `SELECT state FROM ssf_checkpoint_resources WHERE uid=$1 FOR UPDATE`, uid).Scan(&state); err != nil {
		return err
	}
	var n int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_checkpoint_pins WHERE resource_uid=$1`, uid).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return core.ErrPending
	}
	if _, err = tx.Exec(ctx, `UPDATE ssf_checkpoint_resources SET state='Deleting' WHERE uid=$1`, uid); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) SnapshotDeleteAllowed(ctx context.Context, uid string) (bool, error) {
	var ok bool
	err := s.executor(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_checkpoint_resources WHERE uid=$1 AND state='Deleting')`, uid).Scan(&ok)
	return ok, err
}

func (s *Store) SnapshotTracked(ctx context.Context, uid string) (bool, error) {
	var ok bool
	err := s.executor(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_checkpoint_resources WHERE uid=$1)`, uid).Scan(&ok)
	return ok, err
}

// RetainCheckpoint creates an independent durable reference to a completed
// checkpoint. Template publication, fork children and active restores must
// retain before exposing a reference, then release their own ID on final cleanup.
// Copying pins and checking source state share one transaction, so releasing
// the parent cannot open a deletion window between lookup and retain.
func (s *Store) RetainCheckpoint(ctx context.Context, source, id, owner string) error {
	if source == "" || id == "" || owner == "" || source == id {
		return errors.New("distinct checkpoint reference identity required")
	}
	digest := core.Digest(map[string]string{"source": source, "owner": owner})
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var prior, who, state string
	err = tx.QueryRow(ctx, `SELECT digest,owner_uid,state FROM ssf_checkpoint_sets WHERE id=$1`, id).Scan(&prior, &who, &state)
	if err == nil {
		if prior != digest || who != owner || state != "Ready" {
			return core.ErrConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	var descriptor []byte
	if err = tx.QueryRow(ctx, `SELECT state,descriptor FROM ssf_checkpoint_sets WHERE id=$1 FOR SHARE`, source).Scan(&state, &descriptor); err != nil {
		return err
	}
	if state != "Ready" {
		return core.ErrConflict
	}
	tag, err := tx.Exec(ctx, `INSERT INTO ssf_checkpoint_sets(id,owner_uid,digest,state,descriptor) VALUES($1,$2,$3,'Capturing',$4) ON CONFLICT DO NOTHING`, id, owner, digest, descriptor)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if err = tx.QueryRow(ctx, `SELECT digest,owner_uid,state FROM ssf_checkpoint_sets WHERE id=$1`, id).Scan(&prior, &who, &state); err != nil {
			return err
		}
		if prior != digest || who != owner || state != "Ready" {
			return core.ErrConflict
		}
		return tx.Commit(ctx)
	}
	rows, err := tx.Query(ctx, `SELECT resource_uid FROM ssf_checkpoint_pins WHERE checkpoint_id=$1 ORDER BY resource_uid`, source)
	if err != nil {
		return err
	}
	var resources []string
	for rows.Next() {
		var uid string
		if err = rows.Scan(&uid); err != nil {
			rows.Close()
			return err
		}
		resources = append(resources, uid)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(resources) == 0 {
		return errors.New("source checkpoint has no protected resources")
	}
	for _, uid := range resources {
		if err = tx.QueryRow(ctx, `SELECT state FROM ssf_checkpoint_resources WHERE uid=$1 FOR UPDATE`, uid).Scan(&state); err != nil {
			return err
		}
		if state != "Live" {
			return core.ErrConflict
		}
		if _, err = tx.Exec(ctx, `INSERT INTO ssf_checkpoint_pins(checkpoint_id,resource_uid) VALUES($1,$2)`, id, uid); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE ssf_checkpoint_sets SET state='Ready' WHERE id=$1`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
