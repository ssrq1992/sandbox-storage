package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

// TrackDirectSnapshot is called before a head read. Guard protects the object
// immediately; callers must re-read its UID/finalizers after this transaction.
func (s *Store) TrackDirectSnapshot(ctx context.Context, q core.DirectSnapshotRef) error {
	if q.ID == "" || q.Namespace == "" || q.Name == "" || q.SnapshotUID == "" || q.ContentName == "" || q.ContentUID == "" || q.Volume.PVC.UID == "" || q.Volume.Driver == "" || q.Volume.Handle == "" || q.CreatedUnixNano <= 0 {
		return core.ErrConflict
	}
	raw, err := json.Marshal(q)
	if err != nil {
		return err
	}
	return s.Exclusive(ctx, "direct-snapshot/"+q.ID, func(ctx context.Context) error {
		tag, err := s.executor(ctx).Exec(ctx, `INSERT INTO ssf_direct_snapshots(id,snapshot_uid,content_uid,descriptor) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET id=excluded.id WHERE ssf_direct_snapshots.descriptor=excluded.descriptor AND NOT ssf_direct_snapshots.retired`, q.ID, q.SnapshotUID, q.ContentUID, raw)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return core.ErrConflict
		}
		reference := "direct-source/" + q.ID
		if err = s.BeginCheckpoint(ctx, reference, reference, q); err != nil {
			return err
		}
		if err = s.PinCheckpoint(ctx, reference, q.Volume.PVC.UID); err != nil {
			return err
		}
		return s.CommitCheckpoint(ctx, reference, q)
	})
}
func (s *Store) DirectSnapshot(ctx context.Context, id string) (core.DirectSnapshotRef, error) {
	var q core.DirectSnapshotRef
	var raw []byte
	err := s.executor(ctx).QueryRow(ctx, `SELECT descriptor FROM ssf_direct_snapshots WHERE id=$1 AND NOT retired`, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return q, core.ErrPending
	}
	if err != nil {
		return q, err
	}
	err = json.Unmarshal(raw, &q)
	return q, err
}

// DirectSnapshotDeletionPending allows native DELETE to set DeletionTimestamp.
// Native finalizers still own completion; CSI must obtain a separate durable
// PermitSnapshotDelete immediately before backend deletion.
func (s *Store) DirectSnapshotDeletionPending(ctx context.Context, uid string) (bool, error) {
	var allowed bool
	err := s.executor(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_direct_snapshots WHERE snapshot_uid=$1 OR content_uid=$1) AND NOT EXISTS(SELECT 1 FROM ssf_checkpoint_pins p JOIN ssf_direct_snapshots d ON (p.resource_uid=d.snapshot_uid OR p.resource_uid=d.content_uid) WHERE d.snapshot_uid=$1 OR d.content_uid=$1)`, uid).Scan(&allowed)
	return allowed, err
}

// RetainDirectCheckpoint transfers a completed runtime barrier to the direct
// Kubernetes snapshot lifetime. Retirement and retain use the same lock, so a
// late runtime response cannot resurrect a deleted snapshot reference.
func (s *Store) RetainDirectCheckpoint(ctx context.Context, id, checkpoint string) error {
	if id == "" || checkpoint == "" {
		return core.ErrConflict
	}
	return s.Exclusive(ctx, "direct-snapshot/"+id, func(ctx context.Context) error {
		if _, err := s.DirectSnapshot(ctx, id); err != nil {
			return err
		}
		return s.RetainCheckpoint(ctx, checkpoint, "direct-snapshot/"+id, "direct-snapshot/"+id)
	})
}
func (s *Store) RetireDirectSnapshot(ctx context.Context, id string) error {
	if id == "" {
		return core.ErrConflict
	}
	return s.Exclusive(ctx, "direct-snapshot/"+id, func(ctx context.Context) error {
		// Mark first. A crash retains pins; replay releases them. Never allow a
		// completion arriving in this window to install a new reference.
		if _, err := s.executor(ctx).Exec(ctx, `UPDATE ssf_direct_snapshots SET retired=true WHERE id=$1`, id); err != nil {
			return err
		}
		if err := s.ReleaseCheckpoint(ctx, "direct-source/"+id); err != nil {
			return err
		}
		if _, err := s.CompleteDirectSourceGC(ctx, "direct-source/"+id); err != nil {
			return err
		}
		return s.ReleaseCheckpoint(ctx, "direct-snapshot/"+id)
	})
}

func (s *Store) DirectSnapshots(ctx context.Context, after string, limit int) ([]core.DirectSnapshotRef, error) {
	if limit < 1 || limit > 1000 {
		return nil, core.ErrConflict
	}
	rows, err := s.executor(ctx).Query(ctx, `SELECT descriptor FROM ssf_direct_snapshots WHERE id>$1 AND NOT retired ORDER BY id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []core.DirectSnapshotRef
	for rows.Next() {
		var raw []byte
		var q core.DirectSnapshotRef
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &q); err != nil {
			return nil, err
		}
		result = append(result, q)
	}
	return result, rows.Err()
}

// CompleteDirectSourceGC recognizes a metadata-only source reference against
// its authoritative retired DirectSnapshot row. It created no native artifact
// or Kubernetes Snapshot, so runtime checkpoint GC must not interpret it as one.
func (s *Store) CompleteDirectSourceGC(ctx context.Context, id string) (bool, error) {
	tag, err := s.executor(ctx).Exec(ctx, `UPDATE ssf_checkpoint_gc g SET state='Complete' FROM ssf_direct_snapshots d,ssf_checkpoint_sets p WHERE g.checkpoint_id=$1 AND p.id=g.checkpoint_id AND p.state='Released' AND g.checkpoint_id='direct-source/'||d.id AND d.retired AND p.descriptor=d.descriptor`, id)
	return err == nil && tag.RowsAffected() == 1, err
}
