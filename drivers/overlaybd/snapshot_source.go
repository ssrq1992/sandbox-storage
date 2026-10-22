package overlaybd

import (
	"context"
	"errors"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

// resolveSnapshotSource accepts only a previously authorized, published pin.
// Active-volume capture belongs to the coordinator/native barrier, not here.
func (j *Journal) resolveSnapshotSource(ctx context.Context, source *csi.VolumeContentSource, repository, name string, capacity uint64) (*api.VolumeSource, error) {
	snapshot := source.GetSnapshot()
	if snapshot == nil || snapshot.SnapshotId == "" {
		return nil, status.Error(codes.FailedPrecondition, "volume clone requires a protected capture; snapshot source required")
	}
	p, err := j.Snapshot(ctx, snapshot.SnapshotId)
	if errors.Is(err, ErrNotFound) {
		return nil, status.Error(codes.NotFound, "registered snapshot source missing")
	}
	if err != nil {
		return nil, err
	}
	repo, nativeID, err := ParseVolumeHandle(p.SourceHandle)
	if err != nil {
		return nil, err
	}
	if repo != repository {
		return nil, status.Error(codes.InvalidArgument, "cross-repository native clone is unsupported")
	}
	if capacity != p.CapacityBytes {
		return nil, status.Error(codes.OutOfRange, "native snapshot clone requires source capacity; expand offline after creation")
	}
	p, err = j.PinSnapshotSource(ctx, p.ID, Identity("create", name))
	if err != nil {
		return nil, err
	}
	return &api.VolumeSource{VolumeId: nativeID, HeadRevision: p.HeadRevision, SnapshotId: p.ID}, nil
}
