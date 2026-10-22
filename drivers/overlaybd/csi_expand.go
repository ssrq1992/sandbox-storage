package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	"strconv"
)

type MaintenanceBackend interface {
	ExpansionBackend
	HeadBackend
	Describe(context.Context, *api.DescribeRequest, ...grpc.CallOption) (*api.BackendIdentity, error)
}
type CSIExpander struct {
	Journal    *Journal
	Backends   map[string]MaintenanceBackend
	Authorizer ExpansionAuthorizer
}

func expansionCapacity(r *csi.CapacityRange) (uint64, error) {
	if r == nil || r.RequiredBytes <= 0 || r.LimitBytes < 0 {
		return 0, status.Error(codes.InvalidArgument, "positive expansion range required")
	}
	const unit int64 = 1 << 30
	if r.RequiredBytes > math.MaxInt64-unit+1 {
		return 0, status.Error(codes.OutOfRange, "capacity overflow")
	}
	result := (r.RequiredBytes + unit - 1) / unit * unit
	if r.LimitBytes > 0 && result > r.LimitBytes {
		return 0, status.Error(codes.OutOfRange, "capacity range cannot satisfy GiB alignment")
	}
	return uint64(result), nil
}

func (e *CSIExpander) ControllerExpandVolume(ctx context.Context, q *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if q == nil || q.VolumeId == "" || q.CapacityRange == nil || q.CapacityRange.RequiredBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "volume and positive requested capacity required")
	}
	if e.Journal == nil || e.Authorizer == nil {
		return nil, status.Error(codes.FailedPrecondition, "maintenance authority unavailable")
	}
	if q.VolumeCapability != nil {
		readonly, err := blockMode([]*csi.VolumeCapability{q.VolumeCapability})
		if err != nil {
			return nil, err
		}
		if readonly {
			return nil, status.Error(codes.FailedPrecondition, "readonly expansion forbidden")
		}
	}
	capacity, err := expansionCapacity(q.CapacityRange)
	if err != nil {
		return nil, err
	}
	volume, err := e.Journal.Volume(ctx, q.VolumeId)
	if err != nil {
		return nil, err
	}
	if volume.Readonly {
		return nil, status.Error(codes.FailedPrecondition, "readonly native volume")
	}
	name := q.VolumeId + "/" + strconv.FormatUint(capacity, 10)
	if volume.CapacityBytes >= capacity {
		if q.CapacityRange.LimitBytes > 0 && volume.CapacityBytes > uint64(q.CapacityRange.LimitBytes) {
			return nil, status.Error(codes.OutOfRange, "existing capacity exceeds requested limit")
		}
		if op, lookupErr := e.Journal.Get(ctx, Identity("expand", name)); lookupErr == nil {
			if op.State != Succeeded {
				return nil, ErrConflict
			}
			if err = e.Authorizer.CompleteNativeExpansion(ctx, volume, op.ID, int64(capacity)); err != nil {
				return nil, err
			}
		} else if !errors.Is(lookupErr, ErrNotFound) {
			return nil, lookupErr
		}
		return &csi.ControllerExpandVolumeResponse{CapacityBytes: int64(volume.CapacityBytes), NodeExpansionRequired: false}, nil
	}
	backend := e.Backends[volume.RepositoryID]
	if backend == nil {
		return nil, status.Error(codes.Unavailable, "maintenance worker unavailable")
	}
	identity, err := backend.Describe(ctx, &api.DescribeRequest{})
	if err != nil {
		return nil, err
	}
	if err = ValidateBackend(identity, CapabilityOfflineExpansion); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err = e.Authorizer.ReserveNativeExpansion(ctx, volume, Identity("expand", name), int64(capacity), q.CapacityRange.RequiredBytes); err != nil {
		if errors.Is(err, core.ErrPending) || errors.Is(err, core.ErrConflict) {
			return nil, status.Error(codes.FailedPrecondition, "volume has an active writer, hold, preparation or maintenance operation")
		}
		return nil, err
	}
	var intent ExpansionIntent
	prior, err := e.Journal.Get(ctx, Identity("expand", name))
	if err == nil {
		if err = json.Unmarshal(prior.Input, &intent); err != nil {
			return nil, err
		}
		if intent.Volume.Handle != volume.Handle || intent.Capacity != capacity {
			return nil, ErrConflict
		}
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	} else {
		head, err := backend.ResolveVolume(ctx, &api.ResolveVolumeRequest{RepositoryId: volume.RepositoryID, VolumeId: volume.NativeID})
		if err != nil {
			return nil, err
		}
		if head == nil || head.Operation == nil || head.Operation.RepositoryId != volume.RepositoryID || head.Operation.VolumeId != volume.NativeID ||
			head.State != "ContentReady" || head.HeadRevision != head.ContentDigest || !validDigest(head.HeadRevision) ||
			head.CapacityGeneration == 0 || head.CapacityBytes != volume.CapacityBytes || head.FsUuid != volume.FSUUID || !sameNodes(head.AccessibleNodes, volume.AccessibleNodes) {
			return nil, status.Error(codes.FailedPrecondition, "maintenance source head is not proven")
		}
		intent = ExpansionIntent{Volume: volume, Head: head.HeadRevision, Generation: head.CapacityGeneration, Capacity: capacity}
	}
	_, err = (&Expander{Journal: e.Journal, Backend: backend, Authorizer: e.Authorizer}).Ensure(ctx, name, intent.Volume, intent.Head, intent.Generation, intent.Capacity)
	if err != nil {
		return nil, err
	}
	expanded, err := e.Journal.CommitExpansion(ctx, Identity("expand", name))
	if err != nil {
		return nil, err
	}
	if err = e.Authorizer.CompleteNativeExpansion(ctx, expanded, Identity("expand", name), int64(capacity)); err != nil {
		return nil, err
	}
	return &csi.ControllerExpandVolumeResponse{CapacityBytes: int64(expanded.CapacityBytes), NodeExpansionRequired: false}, nil
}
