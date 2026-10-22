package overlaybd

import (
	"context"
	"errors"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Controller exposes only implemented CSI capabilities. Snapshot, expansion
// capabilities are added when their corresponding protected paths
// are wired; embedding Unimplemented never advertises a successful fallback.
type Controller struct {
	csi.UnimplementedControllerServer
	Creator   *CSICreator
	Deleter   *CSIDeleter
	Attacher  *CSIAttacher
	Snapshots *CSISnapshots
	Expander  *CSIExpander
}

func (c *Controller) ControllerExpandVolume(ctx context.Context, q *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if c.Expander == nil {
		return nil, status.Error(codes.Unimplemented, "offline maintenance is not enabled")
	}
	return c.Expander.ControllerExpandVolume(ctx, q)
}

func (c *Controller) CreateVolume(ctx context.Context, r *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	return c.Creator.CreateVolume(ctx, r)
}
func (c *Controller) DeleteVolume(ctx context.Context, r *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	return c.Deleter.DeleteVolume(ctx, r)
}
func (c *Controller) ControllerPublishVolume(ctx context.Context, r *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return c.Attacher.ControllerPublishVolume(ctx, r)
}
func (c *Controller) ControllerUnpublishVolume(ctx context.Context, r *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return c.Attacher.ControllerUnpublishVolume(ctx, r)
}
func (c *Controller) CreateSnapshot(ctx context.Context, q *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if c.Snapshots == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot authority unavailable")
	}
	return c.Snapshots.CreateSnapshot(ctx, q)
}
func (c *Controller) DeleteSnapshot(ctx context.Context, q *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if c.Snapshots == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot authority unavailable")
	}
	return c.Snapshots.DeleteSnapshot(ctx, q)
}
func (c *Controller) ListSnapshots(ctx context.Context, q *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	if c.Snapshots == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot authority unavailable")
	}
	return c.Snapshots.ListSnapshots(ctx, q)
}
func (c *Controller) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	response := &csi.ControllerGetCapabilitiesResponse{}
	if c.Expander != nil {
		response.Capabilities = append(response.Capabilities, &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME}}})
	}
	for _, capability := range []csi.ControllerServiceCapability_RPC_Type{csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME, csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME, csi.ControllerServiceCapability_RPC_LIST_VOLUMES, csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES, csi.ControllerServiceCapability_RPC_GET_VOLUME} {
		response.Capabilities = append(response.Capabilities, &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: capability}}})
	}
	if c.Snapshots != nil {
		for _, capability := range []csi.ControllerServiceCapability_RPC_Type{csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT, csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS} {
			response.Capabilities = append(response.Capabilities, &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: capability}}})
		}
	}
	return response, nil
}
func (c *Controller) ValidateVolumeCapabilities(ctx context.Context, r *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if r == nil || r.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID required")
	}
	volume, err := c.Creator.Journal.Volume(ctx, r.VolumeId)
	if errors.Is(err, ErrNotFound) {
		return nil, status.Error(codes.NotFound, "native volume not registered")
	}
	if err != nil {
		return nil, err
	}
	readonly, err := blockMode(r.VolumeCapabilities)
	if err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}
	if !readonly && volume.Readonly {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: "native readonly content cannot become writable"}, nil
	}
	for key, value := range r.VolumeContext {
		if (key == "ssf.dev/volume-profile" && value != volume.ProfileRevision) || (key == "overlaybd.ssf.dev/repository" && value != volume.RepositoryID) {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: "volume context identity mismatch"}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: r.VolumeCapabilities, VolumeContext: r.VolumeContext, Parameters: r.Parameters}}, nil
}

type ControllerIdentity struct {
	csi.UnimplementedIdentityServer
	Journal          *Journal
	OfflineExpansion bool
}

func (i *ControllerIdentity) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: DriverName, VendorVersion: "0.1.0", Manifest: map[string]string{"native-backend-protocol": BackendProtocol}}, nil
}
func (i *ControllerIdentity) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	response := &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{
		{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}}},
		{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS}}},
	}}
	if i.OfflineExpansion {
		response.Capabilities = append(response.Capabilities, &csi.PluginCapability{Type: &csi.PluginCapability_VolumeExpansion_{VolumeExpansion: &csi.PluginCapability_VolumeExpansion{Type: csi.PluginCapability_VolumeExpansion_OFFLINE}}})
	}
	return response, nil
}
func (i *ControllerIdentity) Probe(ctx context.Context, _ *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	if i.Journal == nil || i.Journal.Pool == nil {
		return nil, status.Error(codes.FailedPrecondition, "driver catalog unavailable")
	}
	if err := i.Journal.Pool.Ping(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, "driver database unavailable")
	}
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
func RegisterController(server grpc.ServiceRegistrar, creator *CSICreator, deleter *CSIDeleter, attacher *CSIAttacher, snapshots ...*CSISnapshots) error {
	return registerController(server, creator, deleter, attacher, nil, snapshots...)
}

func RegisterControllerWithExpansion(server grpc.ServiceRegistrar, creator *CSICreator, deleter *CSIDeleter, attacher *CSIAttacher, expander *CSIExpander, snapshots ...*CSISnapshots) error {
	if expander == nil || creator == nil || expander.Journal != creator.Journal || expander.Authorizer == nil {
		return errors.New("complete offline expansion dependencies required")
	}
	for id := range creator.Repositories {
		if expander.Backends[id] == nil {
			return errors.New("maintenance worker missing")
		}
	}
	return registerController(server, creator, deleter, attacher, expander, snapshots...)
}

func registerController(server grpc.ServiceRegistrar, creator *CSICreator, deleter *CSIDeleter, attacher *CSIAttacher, expander *CSIExpander, snapshots ...*CSISnapshots) error {
	if attacher == nil || attacher.Ledger == nil || creator == nil || deleter == nil || creator.Journal == nil || creator.Journal != deleter.Journal || creator.Journal != attacher.Journal || deleter.Authorizer == nil || len(creator.Repositories) == 0 {
		return errors.New("complete native controller dependencies required")
	}
	for id, worker := range creator.Repositories {
		if worker.ID != id || worker.Backend == nil || deleter.Backends[id] == nil || attacher.Backends[id] == nil {
			return errors.New("repository creation/deletion worker mismatch")
		}
	}
	var snapshotService *CSISnapshots
	if len(snapshots) > 1 {
		return errors.New("one snapshot authority required")
	}
	if len(snapshots) == 1 {
		snapshotService = snapshots[0]
		if snapshotService == nil || snapshotService.Journal != creator.Journal || snapshotService.Authority == nil {
			return errors.New("complete snapshot dependencies required")
		}
	}
	csi.RegisterIdentityServer(server, &ControllerIdentity{Journal: creator.Journal, OfflineExpansion: expander != nil})
	csi.RegisterControllerServer(server, &Controller{Creator: creator, Deleter: deleter, Attacher: attacher, Snapshots: snapshotService, Expander: expander})
	return nil
}
