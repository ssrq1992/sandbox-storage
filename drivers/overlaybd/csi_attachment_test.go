package overlaybd

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/ledger"
)

type headBackend struct {
	calls   int
	receipt *api.VolumeReceipt
}

func (b *headBackend) ResolveVolume(context.Context, *api.ResolveVolumeRequest, ...grpc.CallOption) (*api.VolumeReceipt, error) {
	b.calls++
	return b.receipt, nil
}
func TestPostgresNativeAttachmentPinsHeadAcrossUnknownResponse(t *testing.T) {
	journal := database(t)
	ctx := context.Background()
	store := &ledger.Store{Pool: journal.Pool}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("attach-test-%d", time.Now().UnixNano())
	receipt := &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: Identity("create", name), RepositoryId: "repo", VolumeId: Identity("volume", name), InputDigest: strings.Repeat("a", 64)}, State: "ContentReady", HeadRevision: strings.Repeat("b", 64), ContentDigest: strings.Repeat("b", 64), CapacityBytes: 1 << 30, CapacityGeneration: 1, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a", AccessibleNodes: []string{"node-a"}}
	native, err := journal.RegisterVolume(ctx, name, "", Profile, false, receipt)
	if err != nil {
		t.Fatal(err)
	}
	volume := core.Volume{Driver: DriverName, Handle: native.Handle, PVC: core.PVCRef{UID: name}, Capability: nativeBlockCapability()}
	if err = store.Register(ctx, volume); err != nil {
		t.Fatal(err)
	}
	binding := core.Binding{ID: name + "binding", Volume: volume, Sandbox: name + "sandbox", Node: "node-a"}
	if err = store.Acquire(ctx, name+"run", "input", []core.Binding{binding}); err != nil {
		t.Fatal(err)
	}
	rpc := metadata.NewIncomingContext(ctx, metadata.Pairs("ssf-binding-id", binding.ID))
	backend := &headBackend{receipt: receipt}
	attacher := &CSIAttacher{Journal: journal, Ledger: store, Backends: map[string]HeadBackend{"repo": backend}}
	request := &csi.ControllerPublishVolumeRequest{VolumeId: native.Handle, NodeId: "node-a", VolumeCapability: volume.Capability}
	if _, err = attacher.ControllerPublishVolume(ctx, request); err == nil {
		t.Fatal("missing binding identity authorized")
	}
	_, err = store.EnsureAttachment(ctx, volume.Key(), "node-a", core.Digest(volume), "node-a", func() (map[string]string, error) {
		_, e := attacher.ControllerPublishVolume(rpc, request)
		if e != nil {
			return nil, e
		}
		return nil, context.DeadlineExceeded
	})
	if err == nil || backend.calls != 1 {
		t.Fatalf("lost attach not persisted: %v", err)
	}
	values, err := store.EnsureAttachment(ctx, volume.Key(), "node-a", core.Digest(volume), "node-a", func() (map[string]string, error) {
		result, e := attacher.ControllerPublishVolume(rpc, request)
		if e != nil {
			return nil, e
		}
		return result.PublishContext, nil
	})
	if err != nil || backend.calls != 1 || values["overlaybd.ssf.dev/head"] != receipt.HeadRevision {
		t.Fatalf("head changed on attachment recovery: %v", err)
	}
	detach := &csi.ControllerUnpublishVolumeRequest{VolumeId: native.Handle, NodeId: "node-a"}
	if _, err = attacher.ControllerUnpublishVolume(rpc, detach); err == nil {
		t.Fatal("active attachment detached")
	}
	// Fixture establishes the trusted coordinator's post-NodeReleased boundary;
	// it is not evidence that a real ublk device was stopped.
	if err = store.BeginRelease(ctx, name+"run"); err != nil {
		t.Fatal(err)
	}
	if err = store.Exclusive(ctx, "volume/"+volume.Key(), func(ctx context.Context) error {
		return store.DetachAttachment(ctx, volume.Key(), "node-a", func(string) error { _, e := attacher.ControllerUnpublishVolume(rpc, detach); return e })
	}); err != nil {
		t.Fatal(err)
	}
}
