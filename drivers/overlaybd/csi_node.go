package overlaybd

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
	"time"
)

// BlockPublisher must inspect the actual mounted block identity on retries.
// A symlink or an mknod without a verified source device is not a publication.
type BlockPublisher interface {
	Publish(context.Context, DeviceEvidence, string, bool) error
	Unpublish(context.Context, DeviceEvidence, string) error
}

type CSINode struct {
	mu       sync.Mutex
	draining atomic.Bool
	csi.UnimplementedNodeServer
	Authorization   NodeAuthorization
	Operations      *NodeOperations
	Mounts          BlockPublisher
	Evidence        runtime.EvidenceVerifier
	CaptureEvidence runtime.CaptureVerifier
	Root            string
}

func (n *CSINode) authorize(ctx context.Context, handle string, cap *csi.VolumeCapability, values, publish map[string]string, stage, target string) (SessionIntent, error) {
	if n.Operations == nil || n.Operations.Sessions == nil || n.Mounts == nil || !filepath.IsAbs(n.Root) || filepath.Clean(n.Root) != n.Root {
		return SessionIntent{}, status.Error(codes.FailedPrecondition, "native node dependencies unavailable")
	}
	intent, err := n.Authorization.Authorize(ctx, handle, cap, values, publish)
	if err != nil {
		return intent, status.Error(codes.PermissionDenied, err.Error())
	}
	md, _ := metadata.FromIncomingContext(ctx)
	p, err := auth.Verify(md.Get(auth.NodePlanMetadata)[0], n.Authorization.Key, n.Authorization.Node, n.Authorization.Boot, time.Now())
	if err != nil {
		return intent, err
	}
	id := md.Get(auth.NodeBindingMetadata)[0]
	for _, b := range p.Bindings {
		if b.ID == id {
			expectedStage := filepath.Join(n.Root, "stage", core.Hash(b.Volume.Key()+"/"+b.ID))
			expectedTarget := filepath.Join(n.Root, "publish", core.Hash(b.ID))
			if stage != expectedStage || (target != "" && target != expectedTarget) {
				return intent, status.Error(codes.PermissionDenied, "node path does not match signed binding")
			}
		}
	}
	return intent, nil
}

func (n *CSINode) prepare(ctx context.Context, intent SessionIntent, publish map[string]string) (Session, error) {
	s, err := n.Operations.Prepare(ctx, intent)
	if err != nil {
		return s, err
	}
	capacity, err := strconv.ParseUint(publish["overlaybd.ssf.dev/capacity"], 10, 64)
	if err != nil || capacity == 0 || s.Evidence == nil || s.Evidence.FSUUID != publish["overlaybd.ssf.dev/fs-uuid"] || s.Evidence.CapacityBytes != capacity || s.Evidence.NodeBootID != n.Authorization.Boot {
		_, saveErr := n.Operations.Sessions.MarkUnknown(s)
		return s, errors.Join(errors.New("native device differs from pinned head or node boot"), saveErr)
	}
	return s, nil
}
func (n *CSINode) NodeStageVolume(ctx context.Context, r *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.draining.Load() {
		return nil, status.Error(codes.Unavailable, "OverlayBD node is draining")
	}

	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "stage request required")
	}
	intent, err := n.authorize(ctx, r.VolumeId, r.VolumeCapability, r.VolumeContext, r.PublishContext, r.StagingTargetPath, "")
	if err != nil {
		return nil, err
	}
	if err = n.Operations.Sessions.ClaimPath(NodePath{Path: r.StagingTargetPath, SessionID: intent.ID, Handle: r.VolumeId, Kind: "stage"}); err != nil {
		return nil, err
	}
	if _, err = n.prepare(ctx, intent, r.PublishContext); err != nil {
		return nil, err
	}
	return &csi.NodeStageVolumeResponse{}, nil
}
func (n *CSINode) NodePublishVolume(ctx context.Context, r *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.draining.Load() {
		return nil, status.Error(codes.Unavailable, "OverlayBD node is draining")
	}

	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "publish request required")
	}
	intent, err := n.authorize(ctx, r.VolumeId, r.VolumeCapability, r.VolumeContext, r.PublishContext, r.StagingTargetPath, r.TargetPath)
	if err != nil {
		return nil, err
	}
	if r.Readonly != intent.Readonly {
		return nil, status.Error(codes.PermissionDenied, "readonly differs from signed binding")
	}
	stage, err := n.Operations.Sessions.Path(r.StagingTargetPath)
	if err != nil || stage.SessionID != intent.ID || stage.Kind != "stage" || stage.Handle != r.VolumeId {
		return nil, status.Error(codes.FailedPrecondition, "matching stage required")
	}
	if err = n.Operations.Sessions.ClaimPath(NodePath{Path: r.TargetPath, SessionID: intent.ID, Handle: r.VolumeId, Kind: "publish"}); err != nil {
		return nil, err
	}
	s, err := n.prepare(ctx, intent, r.PublishContext)
	if err != nil {
		return nil, err
	}
	if err = n.Mounts.Publish(ctx, *s.Evidence, r.TargetPath, r.Readonly); err != nil {
		return nil, err
	}
	s, err = n.Operations.Sessions.RecordPublish(s, r.TargetPath)
	if err != nil {
		return nil, err
	}
	// Fence every runtime publication before returning it to the consumer. Even
	// response loss must require real stop/FD evidence before unpublication.
	if intent.Purpose == "runtime" {
		if _, err = n.Operations.Sessions.Borrow(s, r.TargetPath); err != nil {
			return nil, err
		}
	}
	return &csi.NodePublishVolumeResponse{}, nil
}
func (n *CSINode) NodeUnpublishVolume(ctx context.Context, r *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if r == nil || r.VolumeId == "" || r.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume and target required")
	}
	claim, err := n.Operations.Sessions.Path(r.TargetPath)
	if errors.Is(err, ErrNotFound) {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	if claim.Handle != r.VolumeId || claim.Kind != "publish" {
		return nil, status.Error(codes.PermissionDenied, "target identity mismatch")
	}
	s, err := n.Operations.Sessions.Get(claim.SessionID)
	if err != nil {
		return nil, err
	}
	if s.Evidence == nil {
		return nil, status.Error(codes.FailedPrecondition, "device evidence unavailable")
	}
	if ref, ok := s.Publishes[r.TargetPath]; ok && ref.Borrowed {
		return nil, status.Error(codes.FailedPrecondition, "native stop and FD release evidence required")
	}
	if s.Intent.Purpose == "preparation" && s.PreparationRelease == nil {
		return nil, status.Error(codes.FailedPrecondition, "preparation FD release evidence required")
	}
	// A publication whose reply/WAL update was lost is protected as well.
	if s.Intent.Purpose == "runtime" && s.ReleaseProof == "" {
		return nil, status.Error(codes.FailedPrecondition, "runtime release not authorized")
	}
	if err = n.Mounts.Unpublish(ctx, *s.Evidence, r.TargetPath); err != nil {
		return nil, err
	}
	if _, err = n.Operations.Sessions.RemovePublish(s, r.TargetPath); err != nil {
		return nil, err
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
func (n *CSINode) NodeUnstageVolume(ctx context.Context, r *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if r == nil || r.VolumeId == "" || r.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume and staging path required")
	}
	claim, err := n.Operations.Sessions.Path(r.StagingTargetPath)
	if errors.Is(err, ErrNotFound) {
		return &csi.NodeUnstageVolumeResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	if claim.Handle != r.VolumeId || claim.Kind != "stage" {
		return nil, status.Error(codes.PermissionDenied, "stage identity mismatch")
	}
	s, err := n.Operations.Sessions.Get(claim.SessionID)
	if err != nil {
		return nil, err
	}
	if s.State != "Released" {
		if s.Evidence == nil {
			return nil, status.Error(codes.FailedPrecondition, "verified native device release required")
		}
		if s.Intent.Purpose == "runtime" {
			err = n.finishStopped(ctx, s)
		} else {
			err = n.finishPreparation(ctx, s)
		}
		if err != nil {
			return nil, err
		}
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}
func (n *CSINode) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: n.Authorization.CSINode, AccessibleTopology: &csi.Topology{Segments: map[string]string{TopologyNodeKey: n.Authorization.CSINode}}}, nil
}
func (n *CSINode) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{Capabilities: []*csi.NodeServiceCapability{{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME}}}}}, nil
}
