package overlaybd

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

// releaseSwept first removes any residual publication mount of this session's
// device. A publish whose WAL update was lost leaves the kernel bind-mount
// behind while the publication record is gone; the native device stop would
// then fail with EBUSY forever. Only a session whose release was already
// authorized (ReleaseStarted) is swept, and only its own device's mounts;
// publishers without sweep support keep their original behavior.
func (n *CSINode) releaseSwept(ctx context.Context, s Session, intent ReleaseIntent) (Session, error) {
	if s.Evidence != nil && s.ReleaseStarted {
		if sweeper, ok := n.Mounts.(interface {
			SweepDevice(context.Context, DeviceEvidence) error
		}); ok {
			if err := sweeper.SweepDevice(ctx, *s.Evidence); err != nil {
				return Session{}, err
			}
		}
	}
	return n.Operations.Release(ctx, s.Intent.ID, intent)
}

// Release resumes only an already fenced and verified local cleanup. The CSI
// boundary must first unpublish actual mounts, return borrowed descriptors and
// invoke BeginRelease/BeginPreparationRelease with authenticated evidence.
func (n *NodeOperations) Release(ctx context.Context, id string, intent ReleaseIntent) (Session, error) {
	session, err := n.Sessions.Get(id)
	if err != nil {
		return Session{}, err
	}
	session, err = n.Sessions.PinRelease(session, intent)
	if err != nil {
		return Session{}, err
	}
	if session.State == "Released" {
		return session, nil
	}
	prepare, err := DeviceRequest(session.Intent)
	if err != nil {
		return Session{}, err
	}
	receipt, err := n.Backend.ReleaseDevice(ctx, &api.ReleaseDeviceRequest{
		Operation: prepare.Operation, SessionId: id, Grant: prepare.Grant,
		StoppedEvidenceDigest: intent.StopDigest, BorrowedRefsReleasedDigest: intent.BorrowedRefsDigest,
		FinalCaptureId: intent.CaptureID, PublishedHeadRevision: intent.PublishedHead,
		PreparationReleaseDigest: intent.PreparationDigest,
	})
	if err == nil && (receipt == nil || receipt.State != "Released" || !proto.Equal(receipt.Operation, prepare.Operation)) {
		err = errors.New("native release receipt does not prove this session")
	}
	if err != nil {
		if session.State != "NeedsRecovery" {
			_, saveErr := n.Sessions.MarkUnknown(session)
			err = errors.Join(err, saveErr)
		}
		return Session{}, err
	}
	return n.Sessions.RecordReleased(session, session.Evidence.DeviceIdentity)
}
