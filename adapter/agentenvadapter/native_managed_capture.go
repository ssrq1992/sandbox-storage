package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	api "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

// captureParticipants bridges native lifecycle intent to the common Node API.
// It does not call CSI directly or operate OverlayBD devices from the adapter.
func (c *NativeControl) captureParticipants(d NativeDispatch) func(context.Context, core.CaptureIntent, []core.Binding, string) error {
	return c.managedCapture(d, true, true)
}

func (c *NativeControl) managedCapture(d NativeDispatch, prepare, publish bool) func(context.Context, core.CaptureIntent, []core.Binding, string) error {
	return func(ctx context.Context, intent core.CaptureIntent, bindings []core.Binding, proof string) error {
		var managed []core.Binding
		for _, b := range bindings {
			if b.Volume.Driver == "overlaybd.csi.ssf.dev" {
				managed = append(managed, b)
			}
		}
		if len(managed) == 0 {
			return nil
		}
		if c.CaptureNode == nil {
			return errors.New("managed capture Node client unavailable")
		}
		client, err := c.CaptureNode(ctx, d.Result.Placement)
		if err != nil {
			return err
		}
		plan := d.Result.Plan
		plan.Expires = time.Now().Add(5 * time.Minute)
		token, err := auth.Sign(plan, c.SigningKey)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(intent)
		if err != nil {
			return err
		}
		requests := make(map[string]*api.DriverCaptureRequest, len(managed))
		for _, b := range managed {
			q := &api.DriverCaptureRequest{SignedNodePlan: token, BindingUid: b.ID, CaptureIntentJson: string(raw), Evidence: proof}
			requests[b.ID] = q
			if !prepare {
				continue
			}
			// Every participant seals before any potentially slow remote upload starts.
			result, err := client.PrepareCapture(ctx, q)
			if err != nil {
				return err
			}
			if result == nil || result.BindingUid != b.ID || result.CaptureId != core.Hash(intent.ID+"/"+b.ID) || result.BarrierId != intent.BarrierID || (result.State != "Captured" && result.State != "Published") {
				return errors.New("managed capture barrier receipt mismatch")
			}
		}
		if !publish {
			return nil
		}
		for _, b := range managed {
			result, err := client.PublishCapture(ctx, requests[b.ID])
			if err != nil {
				return err
			}
			if result == nil || result.State != "Published" || result.BindingUid != b.ID || result.CaptureId != core.Hash(intent.ID+"/"+b.ID) || result.BarrierId != intent.BarrierID {
				return errors.New("managed publication receipt mismatch")
			}
			capture := core.SnapshotCapture{CheckpointID: intent.ID, BindingID: b.ID, Driver: b.Volume.Driver, Handle: b.Volume.Handle, PVC: b.Volume.PVC, SnapshotName: "ssf-cp-" + core.Hash(intent.ID + "/" + b.ID)[:40], CaptureID: result.CaptureId, CaptureDigest: result.SealedRevision, HeadRevision: result.PublishedHead, CapacityBytes: result.CapacityBytes, FilesystemUUID: result.FilesystemUuid}
			if err = c.Ledger.RecordSnapshotCapture(ctx, intent, b, capture); err != nil {
				return err
			}
		}
		return nil
	}
}
