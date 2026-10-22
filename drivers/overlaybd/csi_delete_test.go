package overlaybd

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/ledger"
)

type lostDeleteBackend struct {
	calls int
	first *api.DeleteVolumeRequest
}

func (b *lostDeleteBackend) DeleteVolume(_ context.Context, r *api.DeleteVolumeRequest, _ ...grpc.CallOption) (*api.OperationReceipt, error) {
	b.calls++
	if b.first == nil {
		b.first = proto.Clone(r).(*api.DeleteVolumeRequest)
		return nil, context.DeadlineExceeded
	}
	if !proto.Equal(b.first, r) {
		return nil, ErrConflict
	}
	return &api.OperationReceipt{Operation: r.Operation, State: "Deleted"}, nil
}
func TestPostgresCSIDeleteRetainsDrainAcrossLostReply(t *testing.T) {
	journal := database(t)
	ctx := context.Background()
	store := &ledger.Store{Pool: journal.Pool}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("delete-test-%d", time.Now().UnixNano())
	receipt := &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: Identity("create", name), RepositoryId: "repo", VolumeId: Identity("volume", name), InputDigest: strings.Repeat("a", 64)}, State: "ContentReady", HeadRevision: strings.Repeat("b", 64), ContentDigest: strings.Repeat("b", 64), CapacityBytes: 1 << 30, CapacityGeneration: 1, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}
	native, err := journal.RegisterVolume(ctx, name, "", Profile, false, receipt)
	if err != nil {
		t.Fatal(err)
	}
	volume := core.Volume{Driver: DriverName, Handle: native.Handle, PVC: core.PVCRef{UID: name}}
	if err = store.Register(ctx, volume); err != nil {
		t.Fatal(err)
	}
	backend := new(lostDeleteBackend)
	driver := &CSIDeleter{Journal: journal, Backends: map[string]DeleteBackend{"repo": backend}, Authorizer: LedgerDeletion{Store: store}}
	request := &csi.DeleteVolumeRequest{VolumeId: native.Handle}
	if _, err = driver.DeleteVolume(ctx, request); err == nil || backend.calls != 0 {
		t.Fatal("non-draining native volume deleted")
	}
	if err = store.Drain(ctx, name); err != nil {
		t.Fatal(err)
	}
	if _, err = driver.DeleteVolume(ctx, request); err == nil || backend.calls != 1 {
		t.Fatal("lost native reply treated as complete")
	}
	if _, err = journal.Volume(ctx, native.Handle); err != ErrNotFound {
		t.Fatal("deleting volume remains available")
	}
	if err = store.Register(ctx, volume); err == nil {
		t.Fatal("unknown deletion lost public fence")
	}
	if _, err = driver.DeleteVolume(ctx, request); err != nil || backend.calls != 2 {
		t.Fatalf("delete retry: %v", err)
	}
	if _, err = driver.DeleteVolume(ctx, request); err != nil || backend.calls != 2 {
		t.Fatal("deleted tombstone redispatched")
	}
	if _, err = journal.RegisterVolume(ctx, name, "", Profile, false, receipt); err == nil {
		t.Fatal("old create response resurrected native volume")
	}
}
