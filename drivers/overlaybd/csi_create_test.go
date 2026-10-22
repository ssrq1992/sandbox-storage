package overlaybd

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

func nativeBlockCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}
}
func TestNativeCSICapacityAndTopology(t *testing.T) {
	if size, err := requestedCapacity(&csi.CapacityRange{RequiredBytes: (1 << 20) + 1, LimitBytes: 2 << 20}); err != nil || size != 2<<20 {
		t.Fatalf("alignment: %d %v", size, err)
	}
	for _, input := range []*csi.CapacityRange{{RequiredBytes: math.MaxInt64}, {RequiredBytes: 2 << 20, LimitBytes: 1 << 20}, {RequiredBytes: -1}, {LimitBytes: 512}} {
		if _, err := requestedCapacity(input); err == nil {
			t.Fatalf("accepted capacity %+v", input)
		}
	}
	topology := &csi.TopologyRequirement{Requisite: []*csi.Topology{{Segments: map[string]string{TopologyNodeKey: "node-a"}}}}
	if !compatibleTopology([]string{"node-a"}, topology) || compatibleTopology([]string{"node-b"}, topology) || !compatibleTopology(nil, topology) {
		t.Fatal("repository topology mismatch")
	}
	if _, err := blockMode([]*csi.VolumeCapability{nativeBlockCapability()}); err != nil {
		t.Fatal(err)
	}
	if _, err := blockMode([]*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}, AccessMode: nativeBlockCapability().AccessMode}}); err == nil {
		t.Fatal("filesystem mount accepted for native Block profile")
	}
}

type readyVolumeBackend struct{ calls int }

func (b *readyVolumeBackend) EnsureVolume(_ context.Context, r *api.EnsureVolumeRequest, _ ...grpc.CallOption) (*api.VolumeReceipt, error) {
	b.calls++
	return &api.VolumeReceipt{Operation: r.Operation, State: "ContentReady", HeadRevision: strings.Repeat("b", 64), ContentDigest: strings.Repeat("b", 64), CapacityBytes: r.CapacityBytes, CapacityGeneration: 1, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a", AccessibleNodes: []string{"node-a"}}, nil
}
func (b *readyVolumeBackend) InspectVolume(context.Context, *api.InspectVolumeRequest, ...grpc.CallOption) (*api.VolumeReceipt, error) {
	return nil, fmt.Errorf("unexpected inspect")
}
func TestPostgresCSICreateReplayKeepsNativeIdentity(t *testing.T) {
	j := database(t)
	backend := new(readyVolumeBackend)
	creator := &CSICreator{Journal: j, Repositories: map[string]RepositoryWorker{"repo": {ID: "repo", Nodes: []string{"node-a"}, Backend: backend}}}
	request := &csi.CreateVolumeRequest{Name: fmt.Sprintf("pvc-%d", time.Now().UnixNano()), Parameters: map[string]string{"repository": "repo"}, VolumeCapabilities: []*csi.VolumeCapability{nativeBlockCapability()}, Secrets: map[string]string{"credential": "must-not-enter-driver-database"}}
	first, err := creator.CreateVolume(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := creator.CreateVolume(context.Background(), request)
	if err != nil || !proto.Equal(first, replay) || backend.calls != 1 {
		t.Fatalf("create repeated: %v calls=%d", err, backend.calls)
	}
	manifest, err := j.Get(context.Background(), Identity("csi-create", request.Name))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(manifest.Input), "must-not-enter") {
		t.Fatal("CSI secret persisted")
	}
	changed := proto.Clone(request).(*csi.CreateVolumeRequest)
	changed.CapacityRange = &csi.CapacityRange{RequiredBytes: 2 << 30}
	if _, err = creator.CreateVolume(context.Background(), changed); err == nil || backend.calls != 1 {
		t.Fatal("request name reused with new capacity")
	}
}

func TestPostgresCSICloneRetainsPinnedHeadAcrossRetry(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	p := snapshotFixture(t, j)
	if err := j.RegisterSnapshot(ctx, p); err != nil {
		t.Fatal(err)
	}
	backend := new(readyVolumeBackend)
	creator := &CSICreator{Journal: j, Repositories: map[string]RepositoryWorker{"repository": {ID: "repository", Nodes: []string{"node-a"}, Backend: backend}}}
	request := &csi.CreateVolumeRequest{Name: p.RequestName + "-clone", Parameters: map[string]string{"repository": "repository"}, VolumeCapabilities: []*csi.VolumeCapability{nativeBlockCapability()}, VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: p.ID}}}}
	first, err := creator.CreateVolume(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	// A completed clone owns an independent native head. Source deletion must
	// not make an idempotent retry resolve the source name again.
	if err = j.DeleteSnapshotPin(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	replay, err := creator.CreateVolume(ctx, request)
	if err != nil || !proto.Equal(first, replay) || backend.calls != 1 {
		t.Fatalf("clone replay: %v calls=%d", err, backend.calls)
	}
	op, err := j.Get(ctx, Identity("create", request.Name))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(op.Input), p.HeadRevision) || !strings.Contains(string(op.Input), p.ID) {
		t.Fatal("immutable source identity missing")
	}
	request.Name += "-missing"
	if _, err = creator.CreateVolume(ctx, request); err == nil || backend.calls != 1 {
		t.Fatal("missing snapshot became blank volume")
	}
}
