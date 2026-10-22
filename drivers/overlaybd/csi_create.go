package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

const TopologyNodeKey = "overlaybd.ssf.dev/node"

// RepositoryWorker is administrator configuration. A local repository names
// exactly one accessible node; a shared repository leaves Nodes empty. Backend
// is a native worker channel, never a CSI Node client.
type RepositoryWorker struct {
	ID      string
	Nodes   []string
	Backend VolumeBackend
}

type CSICreator struct {
	Journal      *Journal
	Repositories map[string]RepositoryWorker
	// ResolveSource must pin a registered source/capture under SSF protection.
	// With no override, only registered snapshot pins are accepted.
	// Missing or live-volume sources never fall back to an empty volume.
	ResolveSource func(context.Context, *csi.VolumeContentSource, string) (*api.VolumeSource, error)
}

func blockMode(capabilities []*csi.VolumeCapability) (bool, error) {
	if len(capabilities) == 0 {
		return false, status.Error(codes.InvalidArgument, "Block capability required")
	}
	var readonly bool
	for index, capability := range capabilities {
		if capability == nil || capability.GetBlock() == nil || capability.AccessMode == nil {
			return false, status.Error(codes.InvalidArgument, "OverlayBD requires Block volume capability")
		}
		mode := capability.AccessMode.Mode
		ro := mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY || mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
		if !ro && mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER && mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
			return false, status.Error(codes.InvalidArgument, "unsupported OverlayBD access mode")
		}
		if index > 0 && ro != readonly {
			return false, status.Error(codes.InvalidArgument, "conflicting readonly capabilities")
		}
		readonly = ro
	}
	return readonly, nil
}
func requestedCapacity(r *csi.CapacityRange) (uint64, error) {
	const unit int64 = 1 << 20
	required, limit := int64(1<<30), int64(0)
	if r != nil {
		if r.RequiredBytes < 0 || r.LimitBytes < 0 {
			return 0, status.Error(codes.InvalidArgument, "negative capacity")
		}
		limit = r.LimitBytes
		if r.RequiredBytes != 0 {
			required = r.RequiredBytes
		} else if limit > 0 && limit < required {
			required = limit
		}
	}
	if required > math.MaxInt64-(unit-1) {
		return 0, status.Error(codes.OutOfRange, "capacity overflow")
	}
	required = ((required + unit - 1) / unit) * unit
	if limit > 0 && required > limit {
		return 0, status.Error(codes.OutOfRange, "capacity cannot satisfy MiB alignment and limit")
	}
	return uint64(required), nil
}
func compatibleTopology(nodes []string, required *csi.TopologyRequirement) bool {
	if required == nil || len(required.Requisite) == 0 {
		return true
	}
	for _, topology := range required.Requisite {
		if topology == nil {
			continue
		}
		if len(topology.Segments) == 0 {
			return true
		}
		if len(topology.Segments) != 1 {
			continue
		}
		node := topology.Segments[TopologyNodeKey]
		if node != "" && (len(nodes) == 0 || slices.Contains(nodes, node)) {
			return true
		}
	}
	return false
}

// CreateVolume fixes the CSI request before invoking native storage. Replays
// retain the original source revision and worker even when a source advances.
func (c *CSICreator) CreateVolume(ctx context.Context, request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if request == nil || request.Name == "" || c.Journal == nil {
		return nil, status.Error(codes.InvalidArgument, "create name and configured catalog required")
	}
	readonly, err := blockMode(request.VolumeCapabilities)
	if err != nil {
		return nil, err
	}
	capacity, err := requestedCapacity(request.CapacityRange)
	if err != nil {
		return nil, err
	}
	repositoryID := request.Parameters["repository"]
	worker, exists := c.Repositories[repositoryID]
	if !exists || worker.ID != repositoryID || worker.Backend == nil {
		return nil, status.Error(codes.InvalidArgument, "unknown configured native repository")
	}
	if !compatibleTopology(worker.Nodes, request.AccessibilityRequirements) {
		return nil, status.Error(codes.ResourceExhausted, "native repository does not satisfy requested topology")
	}
	for key := range request.Parameters {
		switch key {
		case "repository", "csi.storage.k8s.io/pvc/name", "csi.storage.k8s.io/pvc/namespace", "csi.storage.k8s.io/pv/name":
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported OverlayBD parameter %q", key)
		}
	}
	if readonly && request.VolumeContentSource == nil {
		return nil, status.Error(codes.InvalidArgument, "readonly blank OverlayBD volume is unsupported")
	}
	// Store no CSI secrets. Bind every public create input including topology,
	// source object ID and metadata, so a retry cannot silently change meaning.
	input := proto.Clone(request).(*csi.CreateVolumeRequest)
	input.Secrets = nil
	if _, err = c.Journal.Reserve(ctx, "csi-create", request.Name, Identity("volume", request.Name), input); err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "CSI create identity conflicts: %v", err)
	}
	intent := CreateIntent{RepositoryID: repositoryID, CapacityBytes: capacity, Readonly: readonly, ProfileRevision: Profile}
	prior, lookupErr := c.Journal.Get(ctx, Identity("create", request.Name))
	if lookupErr == nil {
		if err = json.Unmarshal(prior.Input, &intent); err != nil {
			return nil, status.Error(codes.DataLoss, "stored native intent is corrupt")
		}
		if intent.RepositoryID != repositoryID || intent.CapacityBytes != capacity || intent.Readonly != readonly || intent.ProfileRevision != Profile {
			return nil, status.Error(codes.AlreadyExists, "native create inputs changed")
		}
	} else if !errors.Is(lookupErr, ErrNotFound) {
		return nil, lookupErr
	} else if request.VolumeContentSource != nil {
		if c.ResolveSource == nil {
			intent.Source, err = c.Journal.resolveSnapshotSource(ctx, request.VolumeContentSource, repositoryID, request.Name, capacity)
		} else {
			intent.Source, err = c.ResolveSource(ctx, request.VolumeContentSource, repositoryID)
		}
		if err != nil {
			return nil, err
		}
		if intent.Source == nil {
			return nil, status.Error(codes.FailedPrecondition, "source resolver returned no pinned content")
		}
	}
	receipt, err := (&Creator{Journal: c.Journal, Backend: worker.Backend}).Ensure(ctx, request.Name, intent)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "native create requires recovery: %v", err)
	}
	if !sameNodes(receipt.AccessibleNodes, worker.Nodes) {
		return nil, status.Error(codes.DataLoss, "native worker topology differs from configured repository")
	}
	volume, err := c.Journal.RegisterVolume(ctx, request.Name, "", Profile, readonly, receipt, request.VolumeContentSource)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "persist native volume identity: %v", err)
	}
	if c.ResolveSource == nil && intent.Source != nil && intent.Source.SnapshotId != "" {
		if err = c.Journal.ReleaseSnapshotSource(ctx, intent.Source.SnapshotId, Identity("create", request.Name)); err != nil {
			return nil, status.Errorf(codes.Aborted, "release completed clone source: %v", err)
		}
	}
	response := &csi.Volume{VolumeId: volume.Handle, CapacityBytes: int64(volume.CapacityBytes), ContentSource: request.VolumeContentSource,
		VolumeContext: map[string]string{"ssf.dev/volume-profile": Profile, "overlaybd.ssf.dev/repository": volume.RepositoryID}}
	for _, node := range volume.AccessibleNodes {
		response.AccessibleTopology = append(response.AccessibleTopology, &csi.Topology{Segments: map[string]string{TopologyNodeKey: node}})
	}
	return &csi.CreateVolumeResponse{Volume: response}, nil
}
func sameNodes(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}
