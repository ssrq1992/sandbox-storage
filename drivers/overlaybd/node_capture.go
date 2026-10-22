package overlaybd

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

// CaptureProof is supplied only after the runtime's authenticated freeze or
// stopped barrier has been verified. The caller persists its capture ID with
// the joint snapshot intent before invoking this boundary.
type CaptureProof struct {
	CaptureID, BarrierID, FreezeDigest, StoppedDigest, ExpectedHead string
}

// Capture seals under a verified native barrier and persists the receipt before
// returning. A caller may thaw only once every participant has such a receipt.
func (n *NodeOperations) Capture(ctx context.Context, id string, proof CaptureProof) (*api.CaptureReceipt, error) {
	if proof.CaptureID == "" || proof.BarrierID == "" || !validDigest(proof.FreezeDigest) || !validDigest(proof.ExpectedHead) || (proof.StoppedDigest != "" && !validDigest(proof.StoppedDigest)) {
		return nil, ErrTransition
	}
	record, err := n.Sessions.pinCapture(id, proof)
	if err != nil {
		return nil, err
	}
	if record.Captured != nil {
		return record.Captured, nil
	}
	session, err := n.Sessions.Get(id)
	if err != nil {
		return nil, err
	}
	request, err := DeviceRequest(session.Intent)
	if err != nil {
		return nil, err
	}
	capture, err := n.Backend.CaptureDevice(ctx, &api.CaptureDeviceRequest{Operation: request.Operation, SessionId: id, Grant: request.Grant, CaptureId: proof.CaptureID, BarrierId: proof.BarrierID, FreezeEvidenceDigest: proof.FreezeDigest, StoppedEvidenceDigest: proof.StoppedDigest})
	if err != nil {
		return nil, err
	}
	if capture == nil || !proto.Equal(capture.Operation, request.Operation) || capture.SessionId != id || capture.CaptureId != proof.CaptureID || capture.BarrierId != proof.BarrierID || capture.State != "Captured" || !validDigest(capture.ContentDigest) || capture.SealedRevision != capture.ContentDigest {
		return nil, errors.New("native capture receipt does not prove this barrier")
	}
	if err = n.Sessions.saveCapture(record, capture, nil); err != nil {
		return nil, err
	}
	return capture, nil
}

// PublishCaptured never calls CaptureDevice. Retrying after thaw or a process
// restart consumes only the same durable captured layers and expected parent.
func (n *NodeOperations) PublishCaptured(ctx context.Context, id, captureID string) (*api.VolumeReceipt, error) {
	record, err := n.Sessions.Capture(id, captureID)
	if err != nil {
		return nil, err
	}
	if record.Captured == nil {
		return nil, ErrTransition
	}
	if record.Published != nil {
		return record.Published, nil
	}
	session, err := n.Sessions.Get(id)
	if err != nil {
		return nil, err
	}
	if (session.State != "Staged" && !stoppedRecovery(session, record.Proof)) || session.ReleaseStarted || session.Evidence == nil {
		return nil, ErrTransition
	}
	request, err := DeviceRequest(session.Intent)
	if err != nil {
		return nil, err
	}
	var published *api.VolumeReceipt
	if session.Intent.Readonly {
		published, err = n.Backend.ResolveVolume(ctx, &api.ResolveVolumeRequest{RepositoryId: session.Intent.RepositoryID, VolumeId: session.Intent.VolumeID, HeadRevision: record.Proof.ExpectedHead})
	} else {
		published, err = n.Backend.PublishHead(ctx, &api.PublishHeadRequest{Operation: request.Operation, Grant: request.Grant, CaptureId: captureID, ExpectedHeadRevision: record.Proof.ExpectedHead})
	}
	if err != nil {
		return nil, err
	}
	if published == nil || published.Operation == nil || published.State != "ContentReady" || !validDigest(published.HeadRevision) || published.ContentDigest != published.HeadRevision || published.CapacityBytes != session.Evidence.CapacityBytes || published.FsUuid != session.Evidence.FSUUID || published.CapacityGeneration == 0 {
		return nil, errors.New("native publication receipt does not prove captured volume")
	}
	if session.Intent.Readonly {
		if published.Operation.RepositoryId != session.Intent.RepositoryID || published.Operation.VolumeId != session.Intent.VolumeID || published.HeadRevision != record.Proof.ExpectedHead {
			return nil, errors.New("readonly capture changed immutable head")
		}
	} else if !proto.Equal(published.Operation, request.Operation) {
		return nil, errors.New("publication operation identity changed")
	}
	if err = n.Sessions.saveCapture(record, nil, published); err != nil {
		return nil, err
	}
	return published, nil
}
func (n *NodeOperations) CaptureAndPublish(ctx context.Context, id string, proof CaptureProof) (*api.CaptureReceipt, *api.VolumeReceipt, error) {
	capture, err := n.Capture(ctx, id, proof)
	if err != nil {
		return nil, nil, err
	}
	published, err := n.PublishCaptured(ctx, id, proof.CaptureID)
	return capture, published, err
}
