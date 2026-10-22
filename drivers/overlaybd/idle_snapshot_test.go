package overlaybd

import (
	"context"
	"errors"
	"testing"

	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/ledger"
)

func TestPostgresIdleSnapshotCapturesOnceAndNeverReadsLiveHead(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	p := snapshotFixture(t, j)
	store := &ledger.Store{Pool: j.Pool}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	volume := core.Volume{Driver: DriverName, Handle: p.SourceHandle, PVC: core.PVCRef{UID: p.RequestName + "pvc"}}
	if err := store.Register(ctx, volume); err != nil {
		t.Fatal(err)
	}
	repo, native, err := ParseVolumeHandle(volume.Handle)
	if err != nil {
		t.Fatal(err)
	}
	backend := &headBackend{receipt: &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: "resolve", RepositoryId: repo, VolumeId: native, InputDigest: core.Hash("input")}, State: "ContentReady", HeadRevision: p.HeadRevision, ContentDigest: p.HeadRevision, CapacityBytes: p.CapacityBytes, FsUuid: p.FSUUID}}
	captures := &IdleSnapshots{Ledger: store, Journal: j, Backends: map[string]HeadBackend{repo: backend}}
	q := IdleSnapshotRequest{Name: p.RequestName, Volume: volume, SnapshotUID: p.SnapshotUID, ContentUID: p.ContentUID, CreatedUnixNano: p.CreatedUnixNano}
	binding := core.Binding{ID: p.RequestName + "binding", Volume: volume, Sandbox: p.RequestName + "run", Node: "node"}
	if err = store.Acquire(ctx, p.RequestName+"op", "input", []core.Binding{binding}); err != nil {
		t.Fatal(err)
	}
	if _, err = captures.Capture(ctx, q); !errors.Is(err, core.ErrPending) || backend.calls != 0 {
		t.Fatalf("read live head: %v calls=%d", err, backend.calls)
	}
	if err = store.ReleaseBinding(ctx, binding.ID); err != nil {
		t.Fatal(err)
	}
	// Invalid content must keep the fence and must not become an empty snapshot.
	backend.receipt.ContentDigest = core.Hash("wrong")
	if _, err = captures.Capture(ctx, q); err == nil {
		t.Fatal("invalid content accepted")
	}
	next := binding
	next.ID += "next"
	next.Sandbox += "next"
	if err = store.Acquire(ctx, p.RequestName+"next", "input", []core.Binding{next}); !errors.Is(err, core.ErrPending) {
		t.Fatalf("unknown capture lost fence: %v", err)
	}
	backend.receipt.ContentDigest = p.HeadRevision
	result, err := captures.Capture(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.RegisterSnapshot(ctx, result); err != nil {
		t.Fatal(err)
	}
	if err = store.Acquire(ctx, p.RequestName+"next", "input", []core.Binding{next}); err != nil {
		t.Fatal(err)
	}
	calls := backend.calls
	captures.Backends = nil
	replay, err := captures.Capture(ctx, q)
	if err != nil || replay.HeadRevision != result.HeadRevision || backend.calls != calls {
		t.Fatalf("replay reread live head: %v", err)
	}
	if err = store.RetireIdleCapture(ctx, result.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = captures.Capture(ctx, q); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("retired capture replayed: %v", err)
	}
}
