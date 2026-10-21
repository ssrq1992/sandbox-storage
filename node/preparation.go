package node

import (
	"context"
	"encoding/json"
	"errors"

	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
)

func contentRequest(r Record) (runtime.PreparationRequest, error) {
	if r.Plan.Purpose != core.PurposePreparation || r.Plan.Preparation == nil || len(r.Endpoints) != 1 {
		return runtime.PreparationRequest{}, errors.New("not a published content preparation")
	}
	return runtime.PreparationRequest{Operation: r.Plan.Operation, PlanDigest: core.PreparationDigest(r.Plan), Spec: *r.Plan.Preparation, Endpoint: r.Endpoints[0]}, nil
}

func validateContentReceipt(q runtime.PreparationRequest, r core.PreparationReceipt, released bool) error {
	rejection := released && r.Rejected && r.Released && r.UUID == ""
	if r.Protocol != core.PreparationProtocol || r.Operation != q.Operation || r.PlanDigest != q.PlanDigest ||
		r.BindingID != q.Endpoint.BindingID || r.VolumeUID != q.Endpoint.VolumeUID ||
		r.NodeUID != q.Endpoint.NodeUID || r.BootID != q.Endpoint.NodeBootID || r.Epoch != q.Endpoint.Epoch ||
		(!rejection && (r.UUID == "" || r.Rejected)) || r.DeviceIdentity != q.Endpoint.Identity || (released && !r.Released) ||
		(!rejection && q.Spec.ExpectedUUID != "" && r.UUID != q.Spec.ExpectedUUID) {
		return errors.New("invalid content preparation receipt")
	}
	return nil
}

func (e *Engine) PrepareContent(ctx context.Context, id, token string) (Record, error) {
	if e.Preparer == nil {
		return Record{}, runtime.ErrUnsupported
	}
	r, err := e.prepare(ctx, id, token, true)
	if err != nil {
		return r, err
	}
	r, unlock, err := e.lockedRecord(id)
	defer unlock()
	if err != nil {
		return r, err
	}
	q, err := contentRequest(r)
	if err != nil {
		return r, err
	}
	if r.Preparation != nil {
		return r, validateContentReceipt(q, *r.Preparation, false)
	}
	receipt, err := e.Preparer.PrepareVolume(ctx, q)
	if err != nil {
		return r, err
	}
	if err = validateContentReceipt(q, receipt, false); err != nil {
		return r, err
	}
	r.Preparation = &receipt
	return r, e.WAL.Save(r)
}

func (e *Engine) ReleaseContent(ctx context.Context, id, token string) (Record, error) {
	p, err := auth.VerifyRevocation(token, e.Key, e.NodeID, e.NodeUID, e.BootID)
	if err != nil {
		return Record{}, err
	}
	if p.Purpose != core.PurposePreparation || p.Operation != id {
		return Record{}, core.ErrConflict
	}
	r, unlock, err := e.lockedRecord(id)
	defer unlock()
	if err != nil {
		return r, err
	}
	if core.PreparationDigest(p) != core.PreparationDigest(r.Plan) {
		return r, core.ErrConflict
	}
	if r.State == "Released" {
		return r, nil
	}
	// Content preparation is invoked only after the published endpoint is
	// durably saved. A failed Stage/Publish with no endpoint therefore cannot
	// have started the adapter. Revoke that operation before CSI cleanup.
	if len(r.Endpoints) == 0 && r.Preparation == nil && len(r.Plan.Bindings) == 1 {
		b := r.Plan.Bindings[0]
		r.Preparation = &core.PreparationReceipt{
			Protocol: core.PreparationProtocol, Operation: id,
			PlanDigest: core.PreparationDigest(r.Plan), BindingID: b.ID,
			VolumeUID: b.Volume.PVC.UID, NodeUID: r.Plan.NodeUID,
			BootID: r.Plan.BootID, Epoch: b.Epoch, Released: true, Rejected: true,
		}
		r.State = "Releasing"
		if err = e.WAL.Save(r); err != nil {
			return r, err
		}
		return e.release(ctx, r)
	}
	if e.Preparer == nil {
		return r, runtime.ErrUnsupported
	}
	q, err := contentRequest(r)
	if err != nil {
		return r, err
	}
	receipt, err := e.Preparer.ReleasePreparation(ctx, q)
	if err != nil {
		return r, err
	}
	if err = validateContentReceipt(q, receipt, true); err != nil {
		return r, err
	}
	r.Preparation = &receipt
	r.State = "Releasing"
	if err = e.WAL.Save(r); err != nil {
		return r, err
	}
	return e.release(ctx, r)
}

func preparationResponse(r Record, id string, err error) (*v1.VolumePreparation, error) {
	if err != nil {
		return nil, failure(id, err)
	}
	raw, err := json.Marshal(r.Preparation)
	if err != nil {
		return nil, err
	}
	return &v1.VolumePreparation{OperationId: id, State: r.State, ReceiptJson: string(raw), Reason: r.Reason, Protocol: core.PreparationProtocol}, nil
}
func (s *Service) PrepareVolumeContent(ctx context.Context, q *v1.PrepareRequest) (*v1.VolumePreparation, error) {
	r, err := s.Engine.PrepareContent(ctx, q.RequestId, q.SignedNodePlan)
	return preparationResponse(r, q.RequestId, err)
}
func (s *Service) InspectVolumePreparation(ctx context.Context, q *v1.InspectRequest) (*v1.VolumePreparation, error) {
	r, err := s.Engine.WAL.Get(q.OperationId)
	if err == nil && r.Plan.Purpose != core.PurposePreparation {
		err = core.ErrConflict
	}
	return preparationResponse(r, q.OperationId, err)
}
func (s *Service) ReleaseVolumePreparation(ctx context.Context, q *v1.PrepareRequest) (*v1.VolumePreparation, error) {
	r, err := s.Engine.ReleaseContent(ctx, q.RequestId, q.SignedNodePlan)
	return preparationResponse(r, q.RequestId, err)
}
