package consume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

const CaptureVersion = core.CaptureVersion

type CaptureIntent = core.CaptureIntent

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
	CaptureParticipants func(context.Context, CaptureIntent, []core.Binding, string) error
	// SealedParticipants identifies drivers whose immutable captures survive thaw.
	SealedParticipants  func(core.Binding) bool
	PrepareParticipants func(context.Context, CaptureIntent, []core.Binding, string) error
	PublishParticipants func(context.Context, CaptureIntent, []core.Binding, string) error
	Ledger              *ledger.Store
	Kube                *kube.Client
	Evidence            CaptureVerifier
}

// Advance uses native Snapshot resources only. ErrPending means the caller must
// retain the barrier unless Seal has durably completed for this exact intent.
// Durable pins are installed as soon as each API identity is visible, including
// partial results. Unknown outcomes are retained for reconciliation, not GC'd.
func (c *Checkpoints) Advance(ctx context.Context, q CaptureIntent, proof string) (CaptureManifest, error) {
	return c.advance(ctx, q, proof, false)
}

// Seal persists permission to thaw only after every participant is immutable and
// pinned. Generic CSI participants must already be Ready; managed participants
// provide durable captures before asynchronous publication.
func (c *Checkpoints) Seal(ctx context.Context, q CaptureIntent, proof string) (CaptureManifest, error) {
	return c.advance(ctx, q, proof, true)
}

func (c *Checkpoints) advance(ctx context.Context, q CaptureIntent, proof string, sealOnly bool) (CaptureManifest, error) {
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
			// This receipt is written under the checkpoint/operation locks only
			// after the final live-barrier check. It is never a caller boolean.
			var seal string
			sealKey := "checkpoint/sealed/" + q.ID
			sealDigest := core.Digest(struct {
				Intent   CaptureIntent
				Bindings []core.Binding
			}{q, bindings})
			sealErr := c.Ledger.ReadStep(ctx, sealKey, &seal)
			sealed := sealErr == nil
			if sealErr != nil && !errors.Is(sealErr, pgx.ErrNoRows) && !errors.Is(sealErr, core.ErrPending) {
				return sealErr
			}
			if sealed && seal != sealDigest {
				return core.ErrConflict
			}
			verify := func() error {
				if sealed {
					return nil
				}
				return c.Evidence.VerifyCapture(ctx, q, bindings, proof)
			}
			if err = verify(); err != nil {
				return err
			}
			if sealed && sealOnly {
				return nil
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
			if sealOnly {
				if c.PrepareParticipants == nil || c.PublishParticipants == nil || c.SealedParticipants == nil {
					return errors.New("sealed capture capability unavailable")
				}
				if err = c.PrepareParticipants(ctx, q, bindings, proof); err != nil {
					return err
				}
			} else if sealed {
				if c.PublishParticipants == nil {
					return errors.New("sealed publication capability unavailable")
				}
				if err = c.PublishParticipants(ctx, q, bindings, proof); err != nil {
					return err
				}
			} else if c.CaptureParticipants != nil {
				if err = c.CaptureParticipants(ctx, q, bindings, proof); err != nil {
					return err
				}
			}
			result = CaptureManifest{NativeResourceUID: nativeUID, Intent: q, Members: make([]CaptureMember, 0, len(bindings))}
			pending := false
			for _, b := range bindings {
				if sealOnly && c.SealedParticipants(b) {
					continue
				}
				name := "ssf-cp-" + core.Hash(q.ID + "/" + b.ID)[:40]
				var identity kube.SnapshotState
				// Persist the first observed UID before polling status; a deleted/recreated
				// object with the same name can never replace a checkpoint member.
				err = c.Ledger.Step(ctx, "checkpoint/snapshot/"+q.ID+"/"+b.ID, core.Digest(q), func(ctx context.Context) (any, error) {
					if err := verify(); err != nil {
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
			if err = verify(); err != nil {
				return err
			}
			if pending {
				return core.ErrPending
			}
			if sealOnly {
				return c.Ledger.Step(ctx, sealKey, core.Digest(q), func(context.Context) (any, error) { return sealDigest, nil }, &seal)
			}
			sort.Slice(result.Members, func(i, j int) bool { return result.Members[i].BindingID < result.Members[j].BindingID })
			return c.Ledger.CommitCheckpoint(ctx, q.ID, result)
		})
	})
	return result, err
}
