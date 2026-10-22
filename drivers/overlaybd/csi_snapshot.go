package overlaybd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"ssf.dev/ssf/core"
)

type SnapshotAuthority interface {
	Resolve(context.Context, string, string, map[string]string) (SnapshotPin, error)
	Delete(context.Context, SnapshotPin) error
}
type snapshotRetirement interface {
	Retire(context.Context, string) error
}

func (s *CSISnapshots) retire(ctx context.Context, id string) error {
	if authority, ok := s.Authority.(snapshotRetirement); ok {
		return authority.Retire(ctx, id)
	}
	return nil
}

type CSISnapshots struct {
	Journal   *Journal
	Authority SnapshotAuthority
}

func snapshotResponse(p SnapshotPin) *csi.Snapshot {
	return &csi.Snapshot{SnapshotId: p.ID, SourceVolumeId: p.SourceHandle, SizeBytes: int64(p.CapacityBytes), CreationTime: timestamppb.New(time.Unix(0, p.CreatedUnixNano)), ReadyToUse: true}
}
func (s *CSISnapshots) CreateSnapshot(ctx context.Context, q *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if q == nil || q.Name == "" || q.SourceVolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name and source required")
	}
	if s.Authority == nil {
		return nil, status.Error(codes.FailedPrecondition, "snapshot authority unavailable")
	}
	if _, _, err := ParseVolumeHandle(q.SourceVolumeId); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	for k := range q.Parameters {
		if k != snapshotNameKey && k != snapshotNamespaceKey && k != snapshotContentKey {
			return nil, status.Error(codes.InvalidArgument, "unknown snapshot parameter")
		}
	}
	if _, err := s.Journal.Reserve(ctx, "csi-snapshot", q.Name, q.SourceVolumeId, struct {
		Source     string
		Parameters map[string]string
	}{q.SourceVolumeId, q.Parameters}); err != nil {
		if errors.Is(err, ErrConflict) {
			return nil, status.Error(codes.AlreadyExists, "snapshot request identity changed")
		}
		return nil, status.Error(codes.Unavailable, "snapshot operation store unavailable")
	}
	if prior, err := s.Journal.Snapshot(ctx, Identity("snapshot", q.Name)); err == nil {
		if prior.SourceHandle != q.SourceVolumeId {
			return nil, status.Error(codes.AlreadyExists, "snapshot source changed")
		}
		return &csi.CreateSnapshotResponse{Snapshot: snapshotResponse(prior)}, nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	p, err := s.Authority.Resolve(ctx, q.Name, q.SourceVolumeId, q.Parameters)
	if errors.Is(err, core.ErrPending) {
		return nil, status.Error(codes.Aborted, "verified runtime capture pending")
	}
	if err != nil {
		return nil, err
	}
	if p.ID != Identity("snapshot", q.Name) || p.RequestName != q.Name || p.SourceHandle != q.SourceVolumeId {
		return nil, status.Error(codes.PermissionDenied, "snapshot authority scope mismatch")
	}
	if err = s.Journal.RegisterSnapshot(ctx, p); err != nil {
		if errors.Is(err, ErrConflict) {
			return nil, status.Error(codes.AlreadyExists, "snapshot identity conflicts")
		}
		return nil, status.Error(codes.Unavailable, "snapshot registration unavailable")
	}
	return &csi.CreateSnapshotResponse{Snapshot: snapshotResponse(p)}, nil
}
func (s *CSISnapshots) DeleteSnapshot(ctx context.Context, q *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if q == nil || q.SnapshotId == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID required")
	}
	p, err := s.Journal.Snapshot(ctx, q.SnapshotId)
	if errors.Is(err, ErrNotFound) {
		if err = s.Journal.DeleteSnapshotPin(ctx, q.SnapshotId); err != nil {
			return nil, err
		}
		if err = s.retire(ctx, q.SnapshotId); err != nil {
			return nil, err
		}
		return &csi.DeleteSnapshotResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	if s.Authority == nil {
		return nil, status.Error(codes.FailedPrecondition, "snapshot authority unavailable")
	}
	if err = s.Authority.Delete(ctx, p); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "snapshot referenced: %v", err)
	}
	if err = s.Journal.DeleteSnapshotPin(ctx, p.ID); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "snapshot clone references remain: %v", err)
	}
	if err = s.retire(ctx, p.ID); err != nil {
		return nil, err
	}
	return &csi.DeleteSnapshotResponse{}, nil
}
func (s *CSISnapshots) ListSnapshots(ctx context.Context, q *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	if q == nil || q.MaxEntries < 0 {
		return nil, status.Error(codes.InvalidArgument, "nonnegative page size required")
	}
	cursor := ""
	if q.StartingToken != "" {
		raw, err := base64.RawURLEncoding.DecodeString(q.StartingToken)
		if err != nil || base64.RawURLEncoding.EncodeToString(raw) != q.StartingToken {
			return nil, status.Error(codes.Aborted, "invalid snapshot cursor")
		}
		cursor = string(raw)
	}
	limit := q.MaxEntries
	if limit == 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.Journal.Pool.Query(ctx, `SELECT identity FROM ssf_overlaybd.snapshot_pins WHERE state='Ready' AND id>$1 AND ($2='' OR source_handle=$2) AND ($3='' OR id=$3) ORDER BY id LIMIT $4`, cursor, q.SourceVolumeId, q.SnapshotId, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	response := &csi.ListSnapshotsResponse{}
	var last string
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		var p SnapshotPin
		if err = json.Unmarshal(raw, &p); err != nil {
			return nil, err
		}
		if err = p.validate(); err != nil {
			return nil, err
		}
		if len(response.Entries) == int(limit) {
			response.NextToken = base64.RawURLEncoding.EncodeToString([]byte(last))
			break
		}
		response.Entries = append(response.Entries, &csi.ListSnapshotsResponse_Entry{Snapshot: snapshotResponse(p)})
		last = p.ID
	}
	return response, rows.Err()
}
