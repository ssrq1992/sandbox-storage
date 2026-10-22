package ledger

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

func (s *Store) RecordSnapshotCapture(ctx context.Context, q core.CaptureIntent, b core.Binding, c core.SnapshotCapture) error {
	digestOK := func(v string) bool { raw, err := hex.DecodeString(v); return err == nil && len(raw) == 32 }
	if err := q.Validate(); err != nil {
		return err
	}
	if c.CheckpointID != q.ID || c.BindingID != b.ID || c.Driver != b.Volume.Driver || c.Handle != b.Volume.Handle || c.PVC != b.Volume.PVC || c.PVC.UID == "" || c.SnapshotName != "ssf-cp-"+core.Hash(q.ID + "/" + b.ID)[:40] || c.CaptureID != core.Hash(q.ID+"/"+b.ID) || !digestOK(c.CaptureDigest) || !digestOK(c.HeadRevision) || c.CapacityBytes == 0 || c.FilesystemUUID == "" || b.Owner() != q.OwnerUID || b.Run() != q.RunUID || q.Classes[b.ID] == "" {
		return core.ErrConflict
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var state, digest string
	if err = tx.QueryRow(ctx, `SELECT state,digest FROM ssf_checkpoint_sets WHERE id=$1 FOR UPDATE`, q.ID).Scan(&state, &digest); err != nil {
		return err
	}
	if state != "Capturing" || digest != core.Digest(q) {
		return core.ErrConflict
	}
	var bindingRaw []byte
	if err = tx.QueryRow(ctx, `SELECT descriptor FROM ssf_bindings WHERE id=$1 AND operation_id=$2 AND state<>'Released'`, b.ID, q.Operation).Scan(&bindingRaw); err != nil {
		return err
	}
	var actual core.Binding
	if err = json.Unmarshal(bindingRaw, &actual); err != nil {
		return err
	}
	if actual.Volume.Key() != b.Volume.Key() || actual.Volume.PVC != b.Volume.PVC || actual.Owner() != b.Owner() || actual.Run() != b.Run() || actual.Epoch != b.Epoch {
		return core.ErrConflict
	}
	var pinned bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_checkpoint_pins WHERE checkpoint_id=$1 AND resource_uid=$2)`, q.ID, c.PVC.UID).Scan(&pinned); err != nil {
		return err
	}
	if !pinned {
		return errors.New("capture source must be pinned before publication handoff")
	}
	tag, err := tx.Exec(ctx, `INSERT INTO ssf_snapshot_captures(namespace,name,checkpoint_id,binding_id,descriptor) VALUES($1,$2,$3,$4,$5)
 ON CONFLICT(namespace,name) DO UPDATE SET name=excluded.name WHERE ssf_snapshot_captures.descriptor=excluded.descriptor`, c.PVC.Namespace, c.SnapshotName, c.CheckpointID, c.BindingID, raw)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return core.ErrConflict
	}
	return tx.Commit(ctx)
}
func (s *Store) SnapshotCapture(ctx context.Context, namespace, name string) (core.SnapshotCapture, error) {
	var c core.SnapshotCapture
	var raw []byte
	err := s.executor(ctx).QueryRow(ctx, `SELECT c.descriptor FROM ssf_snapshot_captures c JOIN ssf_checkpoint_sets p ON p.id=c.checkpoint_id WHERE c.namespace=$1 AND c.name=$2 AND p.state IN ('Capturing','Ready')`, namespace, name).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, core.ErrPending
	}
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(raw, &c)
	return c, err
}

// FixSnapshotContent shares the coordinator's immutable content identity step.
// The CSI sidecar can observe a Content before status propagation reaches the
// coordinator. Whichever observes it first fixes the same UID for both paths.
func (s *Store) FixSnapshotContent(ctx context.Context, checkpoint, binding, contentUID string) error {
	if contentUID == "" {
		return core.ErrConflict
	}
	var digest string
	err := s.executor(ctx).QueryRow(ctx, `SELECT p.digest FROM ssf_checkpoint_sets p JOIN ssf_snapshot_captures c ON c.checkpoint_id=p.id WHERE p.id=$1 AND c.binding_id=$2 AND p.state IN ('Capturing','Ready')`, checkpoint, binding).Scan(&digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return core.ErrConflict
	}
	if err != nil {
		return err
	}
	var fixed string
	if err = s.Step(ctx, "checkpoint/content/"+checkpoint+"/"+binding, digest, func(context.Context) (any, error) { return contentUID, nil }, &fixed); err != nil {
		return err
	}
	if fixed != contentUID {
		return core.ErrConflict
	}
	return nil
}
