package consume

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
)

// Reap releases only a completed, retired capture's own Snapshot/Content pair.
// Independent template/restore/fork pins must all be gone. Ledger tombstones
// prevent new references from racing the native Kubernetes reclamation.
func (c *Checkpoints) Reap(ctx context.Context, id string) error {
	if c.Ledger == nil || c.Kube == nil {
		return errors.New("checkpoint reclaim services required")
	}
	if _, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE ssf_checkpoint_gc SET updated=now() WHERE checkpoint_id=$1`, id); err != nil {
		return err
	}
	return c.Ledger.Exclusive(ctx, "checkpoint/"+id, func(ctx context.Context) error {
		checkpoint, err := c.Ledger.Checkpoint(ctx, id)
		if err != nil {
			return err
		}
		if checkpoint.State != "Released" {
			return core.ErrConflict
		}
		var manifest CaptureManifest
		if err = json.Unmarshal(checkpoint.Descriptor, &manifest); err != nil {
			return err
		}
		if manifest.Intent.ID == "" {
			var intent CaptureIntent
			if err = json.Unmarshal(checkpoint.Descriptor, &intent); err != nil {
				return err
			}
			if err = intent.Validate(); err != nil {
				return err
			}
			if intent.ID != id {
				return core.ErrConflict
			}
			return c.reapIncomplete(ctx, intent)
		}
		if err = manifest.Intent.Validate(); err != nil {
			return err
		}
		if manifest.Intent.ID == id {
			if len(manifest.Members) != len(manifest.Intent.Classes) {
				return core.ErrConflict
			}
			seen := map[string]bool{}
			for _, member := range manifest.Members {
				if seen[member.BindingID] || manifest.Intent.Classes[member.BindingID] == "" {
					return core.ErrConflict
				}
				seen[member.BindingID] = true
				if member.SnapshotUID == "" || member.ContentUID == "" || member.PVC.Namespace == "" || member.Snapshot == "" {
					return core.ErrConflict
				}
				if err = c.Ledger.PermitSnapshotDelete(ctx, member.SnapshotUID); err != nil {
					return err
				}
				if err = c.Ledger.PermitSnapshotDelete(ctx, member.ContentUID); err != nil {
					return err
				}
				if err = c.Kube.ReclaimCapturedSnapshot(ctx, member.PVC.Namespace, member.Snapshot, member.SnapshotUID, member.ContentName, member.ContentUID); err != nil {
					return err
				}
			}
		}
		_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE ssf_checkpoint_gc SET state='Complete' WHERE checkpoint_id=$1`, id)
		return err
	})
}

// An incomplete capture never became publishable. A permanent, small retirement
// tombstone keeps observing deterministic object names after uncertain API writes.
// Native artifact ownership can end separately; late CSI resources are reclaimed.
func (c *Checkpoints) reapIncomplete(ctx context.Context, q CaptureIntent) error {
	bindings, err := c.Ledger.Bindings(ctx, q.Operation)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		if q.Classes[binding.ID] == "" {
			continue
		}
		name := "ssf-cp-" + core.Hash(q.ID + "/" + binding.ID)[:40]
		var known kube.SnapshotState
		err = c.Ledger.ReadStep(ctx, "checkpoint/gc-observed/"+q.ID+"/"+binding.ID, &known)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) && !errors.Is(err, core.ErrPending) {
			return err
		}
		if known.UID == "" {
			err = c.Ledger.ReadStep(ctx, "checkpoint/snapshot/"+q.ID+"/"+binding.ID, &known)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) && !errors.Is(err, core.ErrPending) {
				return err
			}
		}
		state, err := c.Kube.ObserveSnapshotRetirement(ctx, binding.Volume.PVC.Namespace, name, binding.Volume.PVC.UID, known.UID)
		if err != nil {
			return err
		}
		if state.UID == "" {
			continue
		}
		var recorded kube.SnapshotState
		if err = c.Ledger.Step(ctx, "checkpoint/gc-observed/"+q.ID+"/"+binding.ID, core.Digest(q), func(context.Context) (any, error) { return state, nil }, &recorded); err != nil {
			return err
		}
		if recorded.UID != state.UID || (recorded.ContentUID != "" && state.ContentUID != "" && recorded.ContentUID != state.ContentUID) {
			return core.ErrConflict
		}
		if err = c.Ledger.PermitSnapshotDelete(ctx, state.UID); err != nil {
			return err
		}
		if state.ContentUID == "" {
			if err = c.Kube.DeleteSnapshot(ctx, binding.Volume.PVC.Namespace, name, state.UID); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			continue
		}
		var contentUID string
		if err = c.Ledger.Step(ctx, "checkpoint/gc-content/"+q.ID+"/"+binding.ID, core.Digest(q), func(context.Context) (any, error) { return state.ContentUID, nil }, &contentUID); err != nil {
			return err
		}
		if contentUID != state.ContentUID {
			return core.ErrConflict
		}
		if err = c.Ledger.PermitSnapshotDelete(ctx, contentUID); err != nil {
			return err
		}
		if err = c.Kube.ReclaimCapturedSnapshot(ctx, binding.Volume.PVC.Namespace, name, state.UID, state.Content, contentUID); err != nil && !errors.Is(err, core.ErrPending) {
			return err
		}
	}
	return core.ErrPending
}
