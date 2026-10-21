package consume

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

const CaptureVersion = "ssf.capture.v1"

// CaptureIntent identifies one immutable native memory/rootfs capture and every
// CSI volume in that same barrier. The runtime must keep its VMM paused until
// Advance succeeds or it abandons the checkpoint. Proofs are never persisted.
type CaptureIntent struct {
	Version        string            `json:"version"`
	ID             string            `json:"id"`
	Operation      string            `json:"operation"`
	OwnerUID       string            `json:"owner_uid"`
	RunUID         string            `json:"run_uid"`
	BarrierID      string            `json:"barrier_id"`
	NativeArtifact string            `json:"native_artifact"`
	NativeDigest   string            `json:"native_digest"`
	Classes        map[string]string `json:"classes"` // binding ID -> native VolumeSnapshotClass
}

type CaptureMember struct {
	ContentName string      `json:"content_name,omitempty"`
	BindingID   string      `json:"binding_id"`
	MountID     string      `json:"mount_id"`
	GuestPath   string      `json:"guest_path"`
	ReadOnly    bool        `json:"readonly"`
	PVC         core.PVCRef `json:"pvc"`
	Snapshot    string      `json:"snapshot"`
	SnapshotUID string      `json:"snapshot_uid"`
	ContentUID  string      `json:"content_uid"`
}
type CaptureManifest struct {
	NativeResourceUID string          `json:"native_resource_uid"`
	Intent            CaptureIntent   `json:"intent"`
	Members           []CaptureMember `json:"members"`
}

// CaptureVerifier is implemented by the trusted runtime plugin. Verification
// must bind the barrier, native artifact digest, run/epoch and all device
// identities to the currently paused VMM. A caller supplied boolean is not proof.
// After a plugin restart an unverified barrier must fail, not resume snapshots.
type CaptureVerifier interface {
	VerifyCapture(context.Context, CaptureIntent, []core.Binding, string) error
}

type Checkpoints struct {
	Ledger   *ledger.Store
	Kube     *kube.Client
	Evidence CaptureVerifier
}

func (q CaptureIntent) Validate() error {
	digest, err := hex.DecodeString(q.NativeDigest)
	if q.Version != CaptureVersion || q.ID == "" || q.Operation == "" || q.OwnerUID == "" || q.RunUID == "" || q.BarrierID == "" || q.NativeArtifact == "" || err != nil || len(digest) != 32 || len(q.Classes) == 0 || len(q.Classes) > 24 {
		return errors.New("invalid joint capture identity or native artifact digest")
	}
	for binding, class := range q.Classes {
		if binding == "" || class == "" {
			return errors.New("snapshot class required for every binding")
		}
	}
	return nil
}

// Advance uses native Snapshot resources only. ErrPending means the caller must
// retain the same barrier and retry, never thaw and reuse the capture identity.
// Durable pins are installed as soon as each API identity is visible, including
// partial results. Unknown outcomes are retained for reconciliation, not GC'd.
func (c *Checkpoints) Advance(ctx context.Context, q CaptureIntent, proof string) (CaptureManifest, error) {
	var result CaptureManifest
	if err := q.Validate(); err != nil {
		return result, err
	}
	if c.Ledger == nil || c.Kube == nil || c.Evidence == nil || proof == "" {
		return result, errors.New("trusted capture services and evidence required")
	}
	err := c.Ledger.Exclusive(ctx, "operation/"+q.Operation, func(ctx context.Context) error {
		return c.Ledger.Exclusive(ctx, "checkpoint/"+q.ID, func(ctx context.Context) error {
			if err := c.Ledger.BeginCheckpoint(ctx, q.ID, q.OwnerUID, q); err != nil {
				return err
			}
			prior, err := c.Ledger.Checkpoint(ctx, q.ID)
			if err != nil {
				return err
			}
			if prior.State == "Ready" {
				return json.Unmarshal(prior.Descriptor, &result)
			}
			operation, err := c.Ledger.Operation(ctx, q.Operation)
			if err != nil {
				return err
			}
			if operation.State != "Ready" {
				return errors.New("capture requires a committed ready run")
			}
			bindings, err := c.Ledger.Bindings(ctx, q.Operation)
			if err != nil {
				return err
			}
			if len(bindings) != len(q.Classes) {
				return errors.New("capture must include every bound volume")
			}
			for _, b := range bindings {
				if b.Owner() != q.OwnerUID || b.Run() != q.RunUID || b.State == "Released" || b.Volume.PVC.UID == "" || q.Classes[b.ID] == "" {
					return core.ErrConflict
				}
			}
			if err = c.Evidence.VerifyCapture(ctx, q, bindings, proof); err != nil {
				return err
			}
			nativeUID := "native-artifact:" + core.Hash(q.OwnerUID+"/"+q.RunUID+"/"+q.NativeArtifact+"/"+q.NativeDigest)
			if err = c.Ledger.PinCheckpoint(ctx, q.ID, nativeUID); err != nil {
				return err
			}
			for _, b := range bindings {
				if err = c.Ledger.PinCheckpoint(ctx, q.ID, b.Volume.PVC.UID); err != nil {
					return err
				}
			}
			result = CaptureManifest{NativeResourceUID: nativeUID, Intent: q, Members: make([]CaptureMember, 0, len(bindings))}
			pending := false
			for _, b := range bindings {
				name := "ssf-cp-" + core.Hash(q.ID + "/" + b.ID)[:40]
				var identity kube.SnapshotState
				// Persist the first observed UID before polling status; a deleted/recreated
				// object with the same name can never replace a checkpoint member.
				err = c.Ledger.Step(ctx, "checkpoint/snapshot/"+q.ID+"/"+b.ID, core.Digest(q), func(ctx context.Context) (any, error) {
					if err := c.Evidence.VerifyCapture(ctx, q, bindings, proof); err != nil {
						return nil, err
					}
					if err := c.Kube.Snapshot(ctx, b.Volume.PVC, name, q.Classes[b.ID]); err != nil {
						return nil, err
					}
					state, err := c.Kube.SnapshotStatus(ctx, b.Volume.PVC.Namespace, name)
					if err == nil && state.UID == "" {
						err = core.ErrPending
					}
					return state, err
				}, &identity)
				if err != nil {
					return err
				}
				if err = c.Ledger.PinCheckpoint(ctx, q.ID, identity.UID); err != nil {
					return err
				}
				state, err := c.Kube.SnapshotStatus(ctx, b.Volume.PVC.Namespace, name)
				if err != nil {
					return err
				}
				if state.UID != identity.UID || (identity.ContentUID != "" && state.ContentUID != identity.ContentUID) {
					return core.ErrConflict
				}
				if state.ContentUID != "" {
					// Pin each newly observed Content independently before returning Pending.
					var content string
					if err = c.Ledger.Step(ctx, "checkpoint/content/"+q.ID+"/"+b.ID, core.Digest(q), func(context.Context) (any, error) { return state.ContentUID, nil }, &content); err != nil {
						return err
					}
					if content != state.ContentUID {
						return core.ErrConflict
					}
					if err = c.Ledger.PinCheckpoint(ctx, q.ID, content); err != nil {
						return err
					}
				}
				if state.Reason != "" {
					return fmt.Errorf("checkpoint member %s: %s", b.MountID, state.Reason)
				}
				if !state.Ready {
					pending = true
				} else if state.SourceVolumeMode != "Block" {
					return errors.New("checkpoint requires Block snapshot content")
				}
				result.Members = append(result.Members, CaptureMember{BindingID: b.ID, MountID: b.MountID, GuestPath: b.GuestPath, ReadOnly: b.Volume.ReadOnly, PVC: b.Volume.PVC, Snapshot: name, SnapshotUID: state.UID, ContentUID: state.ContentUID, ContentName: state.Content})
			}
			// Even if all CSI snapshots finished, lost/fenced/thawed VMM evidence cannot
			// publish a joint native/CSI checkpoint.
			if err = c.Evidence.VerifyCapture(ctx, q, bindings, proof); err != nil {
				return err
			}
			if pending {
				return core.ErrPending
			}
			sort.Slice(result.Members, func(i, j int) bool { return result.Members[i].BindingID < result.Members[j].BindingID })
			return c.Ledger.CommitCheckpoint(ctx, q.ID, result)
		})
	})
	return result, err
}
