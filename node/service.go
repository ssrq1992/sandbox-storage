package node

import (
	"context"
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/core"
)

type Service struct {
	v1.UnimplementedRuntimeStorageNodeServer
	Engine *Engine
}

func failure(id string, e error) error {
	code := codes.FailedPrecondition
	if errors.Is(e, core.ErrConflict) {
		code = codes.Aborted
	}
	if errors.Is(e, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	if errors.Is(e, context.Canceled) {
		code = codes.Canceled
	}
	s := status.New(code, e.Error())
	with, err := s.WithDetails(&v1.ErrorDetail{OperationId: id, Reason: e.Error(), RetryAfterMs: 250})
	if err == nil {
		return with.Err()
	}
	return s.Err()
}
func (s *Service) PrepareVolumes(ctx context.Context, q *v1.PrepareRequest) (*v1.PrepareResponse, error) {
	r, e := s.Engine.Prepare(ctx, q.RequestId, q.SignedNodePlan)
	if e != nil {
		return nil, failure(q.RequestId, e)
	}
	out := &v1.PrepareResponse{OperationId: r.Plan.Operation}
	for _, ep := range r.Endpoints {
		x := &v1.Endpoint{BindingUid: ep.BindingID, Epoch: ep.Epoch, GuestPath: ep.GuestPath, Readonly: ep.ReadOnly, NodeBootId: ep.NodeBootID}
		x.VolumeUid = ep.VolumeUID
		x.VolumeGeneration = ep.Generation
		x.NodeUid = ep.NodeUID
		x.CapabilityRevision = ep.Revision
		if ep.Kind == "BlockDevice" {
			x.Source = &v1.Endpoint_BlockDevice{BlockDevice: &v1.BlockDevice{ManagedTarget: ep.Path, Major: ep.Major, Minor: ep.Minor, SizeBytes: ep.Size, DeviceIdentity: ep.Identity}}
		} else {
			x.Source = &v1.Endpoint_FileTree{FileTree: &v1.FileTree{ManagedPath: ep.Path, MountIdentity: ep.Identity}}
		}
		out.Endpoints = append(out.Endpoints, x)
	}
	return out, nil
}
func (s *Service) MarkRuntimeReady(ctx context.Context, q *v1.RuntimeRequest) (*v1.Operation, error) {
	r, e := s.Engine.Ready(ctx, q.OperationId, q.SandboxInstanceUid, q.Evidence)
	return operation(r, q.OperationId, e)
}
func (s *Service) ReleaseVolumes(ctx context.Context, q *v1.RuntimeRequest) (*v1.Operation, error) {
	if _, e := s.Engine.WAL.Get(q.OperationId); errors.Is(e, ErrNotFound) && q.SignedNodePlan != "" {
		if e := s.Engine.CancelUnstarted(ctx, q.OperationId, q.SandboxInstanceUid, q.SignedNodePlan, q.Evidence); e != nil {
			return nil, failure(q.OperationId, e)
		}
	}
	r, e := s.Engine.Release(ctx, q.OperationId, q.SandboxInstanceUid, q.Evidence)
	return operation(r, q.OperationId, e)
}
func (s *Service) InspectOperation(ctx context.Context, q *v1.InspectRequest) (*v1.Operation, error) {
	r, e := s.Engine.WAL.Get(q.OperationId)
	return operation(r, q.OperationId, e)
}
func (s *Service) ExpandVolume(ctx context.Context, q *v1.ExpandRequest) (*v1.Operation, error) {
	e := s.Engine.Expand(ctx, q.OperationId, q.BindingUid, q.CapacityBytes)
	if e != nil {
		return nil, failure(q.OperationId, e)
	}
	return &v1.Operation{OperationId: q.OperationId, State: "NodeExpanded"}, nil
}
func operation(r Record, id string, e error) (*v1.Operation, error) {
	if e != nil {
		return nil, failure(id, e)
	}
	return &v1.Operation{OperationId: id, State: r.State, Reason: r.Reason}, nil
}
