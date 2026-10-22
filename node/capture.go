package node

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

func (s *Service) PrepareCapture(ctx context.Context, q *v1.DriverCaptureRequest) (*v1.DriverCaptureResult, error) {
	return s.capture(ctx, q, "prepare")
}
func (s *Service) InspectCapture(ctx context.Context, q *v1.DriverCaptureRequest) (*v1.DriverCaptureResult, error) {
	return s.capture(ctx, q, "inspect")
}
func (s *Service) PublishCapture(ctx context.Context, q *v1.DriverCaptureRequest) (*v1.DriverCaptureResult, error) {
	return s.capture(ctx, q, "publish")
}

// The public node keeps its operation lock through the driver call so cleanup
// cannot unpublish a participant while capture/publication is in flight.
func (s *Service) capture(ctx context.Context, q *v1.DriverCaptureRequest, action string) (*v1.DriverCaptureResult, error) {
	if q == nil || q.BindingUid == "" || len(q.CaptureIntentJson) > 65536 {
		return nil, failure("", errors.New("capture request required"))
	}
	e := s.Engine
	p, err := auth.VerifyRevocation(q.SignedNodePlan, e.Key, e.NodeID, e.NodeUID, e.BootID)
	if err != nil {
		return nil, failure(q.BindingUid, err)
	}
	r, unlock, err := e.lockedRecord(p.Operation)
	defer unlock()
	if err != nil {
		return nil, failure(p.Operation, err)
	}
	unsigned := p
	unsigned.ID = ""
	unsigned.Expires = time.Time{}
	if r.Digest != core.Digest(unsigned) || r.Plan.Purpose != "" || r.State != "Ready" {
		return nil, failure(p.Operation, core.ErrConflict)
	}
	var intent core.CaptureIntent
	if err = json.Unmarshal([]byte(q.CaptureIntentJson), &intent); err != nil {
		return nil, failure(p.Operation, err)
	}
	if err = intent.Validate(); err != nil {
		return nil, failure(p.Operation, err)
	}
	if intent.Operation != p.Operation {
		return nil, failure(p.Operation, core.ErrConflict)
	}
	var binding core.Binding
	count := 0
	for _, b := range p.Bindings {
		if b.ID == q.BindingUid {
			binding = b
			count++
		}
	}
	if count != 1 || binding.Owner() != intent.OwnerUID || binding.Run() != intent.RunUID {
		return nil, failure(p.Operation, core.ErrConflict)
	}
	driver, err := e.Sessions.Node(ctx, binding.Volume.Driver)
	if err != nil {
		return nil, failure(p.Operation, err)
	}
	if driver.Managed == nil || !driver.Capture || driver.NodeID != p.NodeIDs[binding.Volume.Driver] {
		return nil, failure(p.Operation, errors.New("driver lacks negotiated capture capability"))
	}
	var result *v1.DriverCaptureResult
	switch action {
	case "prepare":
		result, err = driver.Managed.PrepareCapture(ctx, q)
	case "inspect":
		result, err = driver.Managed.InspectCapture(ctx, q)
	case "publish":
		result, err = driver.Managed.PublishCapture(ctx, q)
	default:
		return nil, failure(p.Operation, errors.New("invalid capture action"))
	}
	if err != nil {
		return nil, err
	}
	if result == nil || result.BindingUid != binding.ID || result.CaptureId != core.Hash(intent.ID+"/"+binding.ID) || result.BarrierId != intent.BarrierID {
		return nil, failure(p.Operation, errors.New("capture receipt scope mismatch"))
	}
	validDigest := func(value string) bool { raw, err := hex.DecodeString(value); return err == nil && len(raw) == 32 }
	if result.State != "Capturing" && result.State != "Captured" && result.State != "Published" {
		return nil, failure(p.Operation, errors.New("unknown capture state"))
	}
	if action == "prepare" && result.State == "Capturing" {
		return nil, failure(p.Operation, errors.New("capture is not durable yet"))
	}
	if result.State != "Capturing" && !validDigest(result.SealedRevision) {
		return nil, failure(p.Operation, errors.New("capture receipt lacks sealed revision"))
	}
	if action == "publish" && result.State != "Published" {
		return nil, failure(p.Operation, errors.New("capture publication incomplete"))
	}
	if result.State == "Published" && (!validDigest(result.PublishedHead) || result.CapacityBytes == 0 || result.FilesystemUuid == "") {
		return nil, failure(p.Operation, errors.New("capture publication receipt incomplete"))
	}
	return result, nil
}
