package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/gateway"
	"ssf.dev/ssf/ledger"
)

type HeadBackend interface {
	ResolveVolume(context.Context, *api.ResolveVolumeRequest, ...grpc.CallOption) (*api.VolumeReceipt, error)
}
type CSIAttacher struct {
	Journal  *Journal
	Ledger   *ledger.Store
	Backends map[string]HeadBackend
}

func (a *CSIAttacher) ControllerPublishVolume(ctx context.Context, r *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if r == nil || r.VolumeId == "" || r.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume and node identity required")
	}
	if a.Journal == nil || a.Ledger == nil {
		return nil, status.Error(codes.FailedPrecondition, "SSF attachment authorization unavailable")
	}
	if err := (&gateway.LedgerAuthorizer{Store: a.Ledger, Driver: DriverName}).Publish(ctx, r); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	readonly, err := blockMode([]*csi.VolumeCapability{r.VolumeCapability})
	if err != nil {
		return nil, err
	}
	if readonly && !r.Readonly {
		return nil, status.Error(codes.InvalidArgument, "readonly capability requested as writable")
	}
	volume, err := a.Journal.Volume(ctx, r.VolumeId)
	if err != nil {
		return nil, err
	}
	if volume.Readonly && !r.Readonly {
		return nil, status.Error(codes.InvalidArgument, "native volume is readonly")
	}
	if len(volume.AccessibleNodes) > 0 && !slices.Contains(volume.AccessibleNodes, r.NodeId) {
		return nil, status.Error(codes.FailedPrecondition, "native repository is not accessible from requested node")
	}
	grant, err := a.Ledger.BackendAttachmentGrant(ctx, DriverName, volume.Handle, r.NodeId, r.Readonly)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "no matching durable SSF attachment grant")
	}
	// Include access semantics; the ledger fingerprint may be shared by callers
	// but a CSI retry must not replace a readonly attachment with a writer.
	grant = digest([]byte(grant + "\x00" + strconv.FormatBool(r.Readonly)))
	tx, err := a.Journal.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "overlaybd/attach/"+volume.Handle+"/"+r.NodeId); err != nil {
		return nil, err
	}
	var prior, state string
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT grant_digest,state,publish_context FROM ssf_overlaybd.attachments WHERE handle=$1 AND node_id=$2`, volume.Handle, r.NodeId).Scan(&prior, &state, &raw)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if state == "Attached" {
		if prior != grant {
			return nil, status.Error(codes.FailedPrecondition, "previous native attachment must be released")
		}
		var values map[string]string
		if err = json.Unmarshal(raw, &values); err != nil {
			return nil, err
		}
		return &csi.ControllerPublishVolumeResponse{PublishContext: values}, tx.Commit(ctx)
	}
	backend := a.Backends[volume.RepositoryID]
	if backend == nil {
		return nil, status.Error(codes.Unavailable, "native head worker unavailable")
	}
	head, err := backend.ResolveVolume(ctx, &api.ResolveVolumeRequest{RepositoryId: volume.RepositoryID, VolumeId: volume.NativeID})
	if err != nil {
		return nil, err
	}
	if head == nil || head.Operation == nil || head.Operation.RepositoryId != volume.RepositoryID || head.Operation.VolumeId != volume.NativeID || head.State != "ContentReady" || !validDigest(head.HeadRevision) || head.ContentDigest != head.HeadRevision || head.FsUuid != volume.FSUUID || head.CapacityBytes < volume.CapacityBytes || head.CapacityGeneration == 0 {
		return nil, status.Error(codes.DataLoss, "invalid published native head")
	}
	values := map[string]string{core.StageScopeKey: core.StageScopeBinding, core.PublishedFilesystemUUIDKey: head.FsUuid, "overlaybd.ssf.dev/head": head.HeadRevision, "overlaybd.ssf.dev/fs-uuid": head.FsUuid, "overlaybd.ssf.dev/capacity": strconv.FormatUint(head.CapacityBytes, 10), "overlaybd.ssf.dev/generation": strconv.FormatUint(head.CapacityGeneration, 10), "overlaybd.ssf.dev/attachment": grant}
	raw, err = json.Marshal(values)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ssf_overlaybd.attachments(handle,node_id,grant_digest,publish_context,state) VALUES($1,$2,$3,$4,'Attached') ON CONFLICT(handle,node_id) DO UPDATE SET grant_digest=excluded.grant_digest,publish_context=excluded.publish_context,state='Attached'`, volume.Handle, r.NodeId, grant, raw); err != nil {
		return nil, err
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: values}, tx.Commit(ctx)
}
func (a *CSIAttacher) ControllerUnpublishVolume(ctx context.Context, r *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if r == nil || r.VolumeId == "" || r.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume and node identity required")
	}
	if a.Journal == nil || a.Ledger == nil {
		return nil, status.Error(codes.FailedPrecondition, "SSF detach authorization unavailable")
	}
	var state string
	err := a.Journal.Pool.QueryRow(ctx, `SELECT state FROM ssf_overlaybd.attachments WHERE handle=$1 AND node_id=$2`, r.VolumeId, r.NodeId).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && state == "Detached" {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	if err := (&gateway.LedgerAuthorizer{Store: a.Ledger, Driver: DriverName}).Unpublish(ctx, r); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	_, err = a.Journal.Pool.Exec(ctx, `UPDATE ssf_overlaybd.attachments SET state='Detached' WHERE handle=$1 AND node_id=$2`, r.VolumeId, r.NodeId)
	return &csi.ControllerUnpublishVolumeResponse{}, err
}
