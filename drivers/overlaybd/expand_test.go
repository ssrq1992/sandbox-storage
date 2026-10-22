package overlaybd

import (
	"context"
	"errors"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/ledger"
)

type expansionFixture struct {
	request  *api.ExpandVolumeRequest
	calls    int
	resolves int
}

func (f *expansionFixture) ReserveNativeExpansion(context.Context, CatalogVolume, string, int64, ...int64) error {
	return nil
}
func (f *expansionFixture) CompleteNativeExpansion(context.Context, CatalogVolume, string, int64) error {
	return nil
}

func (f *expansionFixture) Describe(context.Context, *api.DescribeRequest, ...grpc.CallOption) (*api.BackendIdentity, error) {
	return &api.BackendIdentity{Protocol: BackendProtocol, InstanceId: "host", RepositoryIds: []string{"repo"}, Capabilities: []string{CapabilityOfflineExpansion, CapabilityDurableRestack}}, nil
}
func (f *expansionFixture) ResolveVolume(_ context.Context, q *api.ResolveVolumeRequest, _ ...grpc.CallOption) (*api.VolumeReceipt, error) {
	f.resolves++
	return &api.VolumeReceipt{Operation: &api.OperationIdentity{RepositoryId: q.RepositoryId, VolumeId: q.VolumeId}, State: "ContentReady", HeadRevision: strings.Repeat("b", 64), ContentDigest: strings.Repeat("b", 64), CapacityBytes: 1 << 30, CapacityGeneration: 1, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}, nil
}

func (f *expansionFixture) AuthorizeNativeExpansion(context.Context, CatalogVolume, string, string, int64) (string, error) {
	return strings.Repeat("a", 64), nil
}
func (f *expansionFixture) ExpandVolume(_ context.Context, q *api.ExpandVolumeRequest, _ ...grpc.CallOption) (*api.VolumeReceipt, error) {
	f.calls++
	if f.request == nil {
		f.request = proto.Clone(q).(*api.ExpandVolumeRequest)
		return nil, context.DeadlineExceeded
	}
	if !proto.Equal(f.request, q) {
		return nil, ErrConflict
	}
	return &api.VolumeReceipt{Operation: q.Operation, State: "ContentReady", HeadRevision: strings.Repeat("c", 64), ContentDigest: strings.Repeat("c", 64), CapacityBytes: q.CapacityBytes, CapacityGeneration: 2, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}, nil
}
func TestPostgresExpansionLostReplyKeepsExactOperation(t *testing.T) {
	j := database(t)
	fixture := new(expansionFixture)
	e := Expander{Journal: j, Backend: fixture, Authorizer: fixture}
	name := fmt.Sprintf("expand-%d", time.Now().UnixNano())
	head := strings.Repeat("b", 64)
	initial := &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: Identity("create", name), InputDigest: strings.Repeat("a", 64), RepositoryId: "repo", VolumeId: name}, State: "ContentReady", HeadRevision: head, ContentDigest: head, CapacityBytes: 1 << 30, CapacityGeneration: 1, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}
	volume, err := j.RegisterVolume(context.Background(), name, "", Profile, false, initial)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Ensure(context.Background(), name, volume, head, 1, 2<<30); err == nil {
		t.Fatal("lost response accepted")
	}
	op, err := j.Get(context.Background(), Identity("expand", name))
	if err != nil || op.State != NeedsRecovery {
		t.Fatalf("unknown result not retained: %v %s", err, op.State)
	}
	first, err := e.Ensure(context.Background(), name, volume, head, 1, 2<<30)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := e.Ensure(context.Background(), name, volume, head, 1, 2<<30)
	if err != nil || !proto.Equal(first, replay) || fixture.calls != 2 {
		t.Fatalf("replay called backend: %v", err)
	}
	if _, err = e.Ensure(context.Background(), name, volume, strings.Repeat("d", 64), 1, 2<<30); err == nil {
		t.Fatal("same operation retargeted current head")
	}
	if _, err = e.Ensure(context.Background(), name, volume, head, 1, 3<<30); err == nil {
		t.Fatal("same operation resized to another capacity")
	}
	for i := 0; i < 2; i++ {
		updated, err := j.CommitExpansion(context.Background(), op.ID)
		if err != nil || updated.CapacityBytes != 2<<30 || updated.InitialHead != head {
			t.Fatalf("capacity publication replay: %+v %v", updated, err)
		}
	}
	createdAgain, err := j.RegisterVolume(context.Background(), name, "", Profile, false, initial)
	if err != nil || createdAgain.CapacityBytes != 2<<30 {
		t.Fatalf("provisioner retry after expansion: %+v %v", createdAgain, err)
	}
}

func TestExpansionReceiptRejectsWrongGenerationContentAndIdentity(t *testing.T) {
	intent := ExpansionIntent{Volume: CatalogVolume{FSUUID: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a", AccessibleNodes: []string{"node-a"}}, Head: strings.Repeat("b", 64), Generation: 3, Capacity: 2 << 30}
	request := &api.ExpandVolumeRequest{Operation: &api.OperationIdentity{OperationId: "expand"}}
	valid := &api.VolumeReceipt{Operation: request.Operation, State: "ContentReady", HeadRevision: strings.Repeat("c", 64), ContentDigest: strings.Repeat("c", 64), CapacityBytes: 2 << 30, CapacityGeneration: 4, FsUuid: intent.Volume.FSUUID, AccessibleNodes: []string{"node-a"}}
	if err := validateExpansion(intent, request, valid); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*api.VolumeReceipt){
		func(r *api.VolumeReceipt) { r.CapacityGeneration = 3 },
		func(r *api.VolumeReceipt) { r.CapacityBytes = 1 << 30 },
		func(r *api.VolumeReceipt) { r.FsUuid = "other" },
		func(r *api.VolumeReceipt) { r.HeadRevision = intent.Head; r.ContentDigest = intent.Head },
		func(r *api.VolumeReceipt) { r.AccessibleNodes = []string{"node-b"} },
		func(r *api.VolumeReceipt) { r.Operation.OperationId = "other" },
	} {
		r := proto.Clone(valid).(*api.VolumeReceipt)
		mutate(r)
		if validateExpansion(intent, request, r) == nil {
			t.Fatal("invalid expansion receipt accepted")
		}
	}
}

func TestPostgresCSIExpansionPinsSourceAndReturnsFilesystemComplete(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	name := fmt.Sprintf("csi-expand-%d", time.Now().UnixNano())
	initial := &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: Identity("create", name), InputDigest: strings.Repeat("a", 64), RepositoryId: "repo", VolumeId: name}, State: "ContentReady", HeadRevision: strings.Repeat("b", 64), ContentDigest: strings.Repeat("b", 64), CapacityBytes: 1 << 30, CapacityGeneration: 1, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}
	volume, err := j.RegisterVolume(ctx, name, "", Profile, false, initial)
	if err != nil {
		t.Fatal(err)
	}
	backend := new(expansionFixture)
	service := CSIExpander{Journal: j, Backends: map[string]MaintenanceBackend{"repo": backend}, Authorizer: backend}
	q := &csi.ControllerExpandVolumeRequest{VolumeId: volume.Handle, CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30}, VolumeCapability: nativeBlockCapability()}
	if _, err = service.ControllerExpandVolume(ctx, q); err == nil {
		t.Fatal("lost response accepted")
	}
	result, err := service.ControllerExpandVolume(ctx, q)
	if err != nil || result.CapacityBytes != 2<<30 || result.NodeExpansionRequired {
		t.Fatalf("complete filesystem receipt: %+v %v", result, err)
	}
	if _, err = service.ControllerExpandVolume(ctx, q); err != nil {
		t.Fatal(err)
	}
	if backend.calls != 2 || backend.resolves != 1 {
		t.Fatalf("re-resolved or redispatched completed maintenance: %+v", backend)
	}
}

func TestPostgresCSIExpansionUsesPublicWriterFenceAndReleasesOnlyAfterPublication(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	name := fmt.Sprintf("fenced-expand-%d", time.Now().UnixNano())
	store := &ledger.Store{Pool: j.Pool}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	initial := &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: Identity("create", name), InputDigest: strings.Repeat("a", 64), RepositoryId: "repo", VolumeId: name}, State: "ContentReady", HeadRevision: strings.Repeat("b", 64), ContentDigest: strings.Repeat("b", 64), CapacityBytes: 1 << 30, CapacityGeneration: 1, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}
	volume, err := j.RegisterVolume(ctx, name, "", Profile, false, initial)
	if err != nil {
		t.Fatal(err)
	}
	public := core.Volume{Driver: DriverName, Handle: volume.Handle, PVC: core.PVCRef{UID: name}}
	if err = store.Register(ctx, public); err != nil {
		t.Fatal(err)
	}
	binding := core.Binding{ID: name + "binding", Volume: public, Sandbox: name + "vm", Node: "node"}
	if err = store.Acquire(ctx, name+"launch", "digest", []core.Binding{binding}); err != nil {
		t.Fatal(err)
	}
	backend := new(expansionFixture)
	service := CSIExpander{Journal: j, Backends: map[string]MaintenanceBackend{"repo": backend}, Authorizer: LedgerExpansion{Store: store}}
	q := &csi.ControllerExpandVolumeRequest{VolumeId: volume.Handle, CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30}}
	if _, err = service.ControllerExpandVolume(ctx, q); err == nil || backend.resolves != 0 {
		t.Fatal("live writer crossed maintenance", err)
	}
	if err = store.ReleaseBinding(ctx, binding.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = service.ControllerExpandVolume(ctx, q); err == nil {
		t.Fatal("lost response accepted")
	}
	binding.ID += "next"
	binding.Sandbox += "next"
	if err = store.Acquire(ctx, name+"next", "digest", []core.Binding{binding}); !errors.Is(err, core.ErrPending) {
		t.Fatal("writer crossed unknown resize", err)
	}
	if _, err = service.ControllerExpandVolume(ctx, q); err != nil {
		t.Fatal(err)
	}
	if allowed, err := store.AllowedExpansion(ctx, name, 2<<30); err != nil || allowed {
		t.Fatal("completed fence retained", err)
	}
	if err = store.Acquire(ctx, name+"next", "digest", []core.Binding{binding}); err != nil {
		t.Fatal(err)
	}
}
