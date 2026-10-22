package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

type DeleteBackend interface {
	DeleteVolume(context.Context, *api.DeleteVolumeRequest, ...grpc.CallOption) (*api.OperationReceipt, error)
}

// DeleteAuthorizer holds a durable SSF drain fence, including active runs,
// preparation, paused owners and snapshot/reference policy. A timeout is not
// permission to delete, and authorizations may not expire during unknown I/O.
type DeleteAuthorizer interface {
	AuthorizeNativeDeletion(context.Context, CatalogVolume, string) (string, error)
}
type CSIDeleter struct {
	Journal    *Journal
	Backends   map[string]DeleteBackend
	Authorizer DeleteAuthorizer
}
type deleteIntent struct {
	Handle        string `json:"handle"`
	Authorization string `json:"authorization"`
}

func (d *CSIDeleter) DeleteVolume(ctx context.Context, request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if request == nil || request.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID required")
	}
	if d.Journal == nil || d.Authorizer == nil {
		return nil, status.Error(codes.FailedPrecondition, "durable SSF drain authorization unavailable")
	}
	if _, _, err := ParseVolumeHandle(request.VolumeId); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var raw []byte
	var state string
	err := d.Journal.Pool.QueryRow(ctx, `SELECT identity,state FROM ssf_overlaybd.volumes WHERE handle=$1`, request.VolumeId).Scan(&raw, &state)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && state == "Deleted") {
		return &csi.DeleteVolumeResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	var volume CatalogVolume
	if err = json.Unmarshal(raw, &volume); err != nil {
		return nil, status.Error(codes.DataLoss, "invalid driver volume record")
	}
	backend := d.Backends[volume.RepositoryID]
	if backend == nil {
		return nil, status.Error(codes.Unavailable, "native repository worker unavailable")
	}
	operationID := Identity("delete", request.VolumeId)
	authorization, err := d.Authorizer.AuthorizeNativeDeletion(ctx, volume, operationID)
	if err != nil {
		return nil, err
	}
	if !validDigest(authorization) {
		return nil, status.Error(codes.PermissionDenied, "invalid native drain authorization")
	}
	op, err := d.Journal.Reserve(ctx, "delete", request.VolumeId, volume.NativeID, deleteIntent{Handle: request.VolumeId, Authorization: authorization})
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "persist native deletion: %v", err)
	}
	if err = d.Journal.BeginVolumeDeletion(ctx, request.VolumeId); err != nil {
		return nil, err
	}
	if op.State == Succeeded {
		return d.markDeleted(ctx, request.VolumeId)
	}
	if op.State == Failed {
		return nil, status.Error(codes.FailedPrecondition, op.Reason)
	}
	if op.State == Pending {
		op, err = d.Journal.Finish(ctx, op, Applying, nil, "")
		if err != nil {
			return nil, err
		}
	}
	// Native deletion is an idempotent journal operation: it never allocates a
	// device or resets content. Reissue the same identity after an unknown reply.
	nativeRequest := &api.DeleteVolumeRequest{Operation: &api.OperationIdentity{OperationId: op.ID, InputDigest: op.Digest, RepositoryId: volume.RepositoryID, VolumeId: volume.NativeID}, DrainAuthorizationDigest: authorization}
	receipt, callErr := backend.DeleteVolume(ctx, nativeRequest)
	if callErr == nil && (receipt == nil || receipt.State != "Deleted" || !proto.Equal(receipt.Operation, nativeRequest.Operation)) {
		callErr = errors.New("native deletion receipt identity mismatch")
	}
	save, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if callErr != nil {
		if op.State == Applying {
			_, persistErr := d.Journal.Finish(save, op, NeedsRecovery, nil, callErr.Error())
			callErr = errors.Join(callErr, persistErr)
		}
		return nil, status.Errorf(codes.Aborted, "native deletion remains fenced: %v", callErr)
	}
	if _, err = d.Journal.Finish(save, op, Succeeded, receipt, ""); err != nil {
		return nil, err
	}
	return d.markDeleted(save, request.VolumeId)
}
func (d *CSIDeleter) markDeleted(ctx context.Context, handle string) (*csi.DeleteVolumeResponse, error) {
	tag, err := d.Journal.Pool.Exec(ctx, `UPDATE ssf_overlaybd.volumes SET state='Deleted' WHERE handle=$1 AND state IN ('Deleting','Deleted')`, handle)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrConflict
	}
	return &csi.DeleteVolumeResponse{}, nil
}
