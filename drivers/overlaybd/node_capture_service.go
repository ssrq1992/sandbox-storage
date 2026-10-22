package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

func (n *CSINode) captureScope(q *v1.DriverCaptureRequest) (core.Plan, core.CaptureIntent, Session, error) {
	var intent core.CaptureIntent
	var session Session
	if q == nil || q.BindingUid == "" || len(q.CaptureIntentJson) > 65536 {
		return core.Plan{}, intent, session, status.Error(codes.InvalidArgument, "capture identity required")
	}
	p, err := auth.VerifyRevocation(q.SignedNodePlan, n.Authorization.Key, n.Authorization.Node, n.Authorization.NodeUID, n.Authorization.Boot)
	if err != nil {
		return p, intent, session, status.Error(codes.PermissionDenied, err.Error())
	}
	if err = json.Unmarshal([]byte(q.CaptureIntentJson), &intent); err != nil {
		return p, intent, session, status.Error(codes.InvalidArgument, "invalid capture intent")
	}
	if err = intent.Validate(); err != nil {
		return p, intent, session, status.Error(codes.InvalidArgument, err.Error())
	}
	if p.Purpose != "" || p.NodeIDs[DriverName] != n.Authorization.CSINode || p.Operation != intent.Operation || len(p.Bindings) != len(intent.Classes) {
		return p, intent, session, ErrConflict
	}
	var binding core.Binding
	count := 0
	for _, b := range p.Bindings {
		if b.Owner() != intent.OwnerUID || b.Run() != intent.RunUID || intent.Classes[b.ID] == "" {
			return p, intent, session, ErrConflict
		}
		if b.ID == q.BindingUid {
			binding = b
			count++
		}
	}
	if count != 1 || binding.Volume.Driver != DriverName || binding.Node != p.Node || binding.Tenant != p.Tenant || binding.Sandbox != p.Sandbox {
		return p, intent, session, ErrConflict
	}
	session, err = n.Operations.Sessions.Get(core.Hash("overlaybd/session/" + binding.ID))
	if err != nil {
		return p, intent, session, err
	}
	unsigned := p
	unsigned.ID = ""
	unsigned.Expires = time.Time{}
	if session.Intent.AuthorizationDigest != core.Digest(unsigned) || session.Intent.OwnerID != binding.OwnerUID || session.Intent.RunID != binding.RunUID || session.Intent.Epoch != binding.Epoch {
		return p, intent, session, ErrConflict
	}
	return p, intent, session, nil
}
func captureResult(binding string, r CaptureRecord) *v1.DriverCaptureResult {
	result := &v1.DriverCaptureResult{BindingUid: binding, CaptureId: r.Proof.CaptureID, BarrierId: r.Proof.BarrierID, State: "Capturing"}
	if r.Captured != nil {
		result.State = "Captured"
		result.SealedRevision = r.Captured.SealedRevision
	}
	if r.Published != nil {
		result.State = "Published"
		result.PublishedHead = r.Published.HeadRevision
		result.CapacityBytes = r.Published.CapacityBytes
		result.FilesystemUuid = r.Published.FsUuid
	}
	return result
}
func (s *ManagedNodeService) PrepareCapture(ctx context.Context, q *v1.DriverCaptureRequest) (*v1.DriverCaptureResult, error) {
	n := s.Node
	n.mu.Lock()
	defer n.mu.Unlock()
	p, intent, session, err := n.captureScope(q)
	if err != nil {
		return nil, err
	}
	id := core.Hash(intent.ID + "/" + q.BindingUid)
	if prior, err := n.Operations.Sessions.Capture(session.Intent.ID, id); err == nil && prior.Captured != nil {
		if prior.Proof.FreezeDigest != core.Digest(intent) {
			return nil, ErrConflict
		}
		return captureResult(q.BindingUid, prior), nil
	} else if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if _, err = auth.Verify(q.SignedNodePlan, n.Authorization.Key, n.Authorization.Node, n.Authorization.Boot, time.Now()); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	if n.CaptureEvidence == nil {
		return nil, status.Error(codes.FailedPrecondition, "native capture verifier unavailable")
	}
	if err = n.CaptureEvidence.VerifyCapture(ctx, intent, p.Bindings, q.Evidence); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	head := session.PublishedHead
	if head == "" {
		head = session.Intent.HeadRevision
	}
	proof := CaptureProof{CaptureID: id, BarrierID: intent.BarrierID, FreezeDigest: core.Digest(intent), ExpectedHead: head}
	if _, err = n.Operations.Capture(ctx, session.Intent.ID, proof); err != nil {
		return nil, err
	}
	record, err := n.Operations.Sessions.Capture(session.Intent.ID, id)
	if err != nil {
		return nil, err
	}
	return captureResult(q.BindingUid, record), nil
}
func (s *ManagedNodeService) InspectCapture(ctx context.Context, q *v1.DriverCaptureRequest) (*v1.DriverCaptureResult, error) {
	return s.capture(ctx, q, false)
}
func (s *ManagedNodeService) PublishCapture(ctx context.Context, q *v1.DriverCaptureRequest) (*v1.DriverCaptureResult, error) {
	return s.capture(ctx, q, true)
}
func (s *ManagedNodeService) capture(ctx context.Context, q *v1.DriverCaptureRequest, publish bool) (*v1.DriverCaptureResult, error) {
	n := s.Node
	n.mu.Lock()
	defer n.mu.Unlock()
	_, intent, session, err := n.captureScope(q)
	if err != nil {
		return nil, err
	}
	id := core.Hash(intent.ID + "/" + q.BindingUid)
	record, err := n.Operations.Sessions.Capture(session.Intent.ID, id)
	if err != nil {
		return nil, err
	}
	if record.Proof.FreezeDigest != core.Digest(intent) {
		return nil, ErrConflict
	}
	if publish {
		if _, err = n.Operations.PublishCaptured(ctx, session.Intent.ID, id); err != nil {
			return nil, err
		}
		record, err = n.Operations.Sessions.Capture(session.Intent.ID, id)
		if err != nil {
			return nil, err
		}
	}
	return captureResult(q.BindingUid, record), nil
}
