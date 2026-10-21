package ledger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"ssf.dev/ssf/core"
)

func lifecycleStore(t *testing.T) (*Store, context.Context, string) {
	t.Helper()
	url := os.Getenv("SSF_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated PostgreSQL: SSF_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		t.Fatalf("migration replay: %v", err)
	}
	return s, ctx, fmt.Sprintf("lifecycle-%d", time.Now().UnixNano())
}

func TestPauseHoldResumeEpochAndDeletion(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	v := core.Volume{Driver: "test", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	owner := id + "owner"
	run := id + "run1"
	epoch, err := s.RunEpoch(ctx, owner, run)
	if err != nil {
		t.Fatal(err)
	}
	b := core.Binding{ID: id + "b1", Volume: v, Sandbox: run, OwnerUID: owner, RunUID: run, Epoch: epoch, Node: "n"}
	if err = s.Acquire(ctx, id+"op1", "first", []core.Binding{b}); err != nil {
		t.Fatal(err)
	}
	if err = s.RetainUse(ctx, id+"op1", "paused"); err != nil {
		t.Fatal(err)
	}
	run2 := id + "run2"
	epoch2, err := s.RunEpoch(ctx, owner, run2)
	if err != nil {
		t.Fatal(err)
	}
	b2 := b
	b2.ID = id + "b2"
	b2.RunUID = run2
	b2.Sandbox = run2
	b2.Epoch = epoch2
	if err = s.Acquire(ctx, id+"op2", "second", []core.Binding{b2}); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("concurrent incarnation: %v", err)
	}
	if err = s.StopRun(ctx, owner, run); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RunEpoch(ctx, owner, run); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("resurrected stopped run: %v", err)
	}
	// Isolate ledger state transitions from CSI unpublish, which is tested by
	// Node/Coordinator integration; this does not assert actual VMM exit.
	if _, err = s.Pool.Exec(ctx, `UPDATE ssf_bindings SET state='Released' WHERE id=$1`, b.ID); err != nil {
		t.Fatal(err)
	}
	stranger := b
	stranger.ID = id + "stranger"
	stranger.OwnerUID = ""
	stranger.RunUID = ""
	stranger.Sandbox = id + "other"
	if err = s.Acquire(ctx, id+"other", "other", []core.Binding{stranger}); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("stole paused hold: %v", err)
	}
	bad := b2
	bad.Epoch = epoch
	if err = s.Acquire(ctx, id+"bad", "bad", []core.Binding{bad}); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("stale epoch admitted: %v", err)
	}
	if err = s.Acquire(ctx, id+"op2", "second", []core.Binding{b2}); err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseUse(ctx, owner); !errors.Is(err, core.ErrPending) {
		t.Fatalf("deleted active owner: %v", err)
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE ssf_bindings SET state='Released' WHERE id=$1`, b2.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.Drain(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err = s.Permit(ctx, id, "Delete", time.Minute); err == nil {
		t.Fatal("reclaimed held volume")
	}
	if err = s.ReleaseUse(ctx, owner); err != nil {
		t.Fatal(err)
	}
	if err = s.Permit(ctx, id, "Delete", time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointPinsAndDeleteTombstone(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	for _, cp := range []string{id + "a", id + "b"} {
		if err := s.BeginCheckpoint(ctx, cp, id, map[string]string{"intent": cp}); err != nil {
			t.Fatal(err)
		}
		if err := s.PinCheckpoint(ctx, cp, id+"snapshot", id+"content"); err != nil {
			t.Fatal(err)
		}
		if err := s.CommitCheckpoint(ctx, cp, map[string]string{"manifest": cp}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.PermitSnapshotDelete(ctx, id+"content"); !errors.Is(err, core.ErrPending) {
		t.Fatalf("deleted pinned content: %v", err)
	}
	if err := s.ReleaseCheckpoint(ctx, id+"a"); err != nil {
		t.Fatal(err)
	}
	if err := s.PermitSnapshotDelete(ctx, id+"content"); !errors.Is(err, core.ErrPending) {
		t.Fatalf("lost second reference: %v", err)
	}
	if err := s.ReleaseCheckpoint(ctx, id+"b"); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitCheckpoint(ctx, id+"b", nil); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("published released checkpoint: %v", err)
	}
	if err := s.PermitSnapshotDelete(ctx, id+"content"); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginCheckpoint(ctx, id+"c", id, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.PinCheckpoint(ctx, id+"c", id+"content"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("pinned deleting content: %v", err)
	}
}

func TestCheckpointReferenceOwnsNativeAndCSIResourcesAfterParentRelease(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	source, child := id+"source", id+"child"
	if err := s.BeginCheckpoint(ctx, source, "parent", map[string]string{"intent": "capture"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PinCheckpoint(ctx, source, id+"snapshot", id+"content", id+"native-artifact"); err != nil {
		t.Fatal(err)
	}
	if err := s.RetainCheckpoint(ctx, source, child, "template"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("retained incomplete capture: %v", err)
	}
	if err := s.CommitCheckpoint(ctx, source, map[string]string{"native_digest": core.Hash("native")}); err != nil {
		t.Fatal(err)
	}
	if err := s.RetainCheckpoint(ctx, source, child, "template"); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseCheckpoint(ctx, source); err != nil {
		t.Fatal(err)
	}
	// Retry works after the source owner has already been collected.
	if err := s.RetainCheckpoint(ctx, source, child, "template"); err != nil {
		t.Fatal(err)
	}
	if err := s.RetainCheckpoint(ctx, source, child, "other-owner"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("changed reference owner: %v", err)
	}
	if err := s.RetainCheckpoint(ctx, source, id+"late", "new"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("retained released parent: %v", err)
	}
	for _, resource := range []string{id + "snapshot", id + "content", id + "native-artifact"} {
		if err := s.PermitSnapshotDelete(ctx, resource); !errors.Is(err, core.ErrPending) {
			t.Fatalf("resource lost child protection: %s: %v", resource, err)
		}
	}
	got, err := s.Checkpoint(ctx, child)
	if err != nil || got.State != "Ready" || len(got.Resources) != 3 {
		t.Fatalf("child reference: %+v %v", got, err)
	}
	if err := s.ReleaseCheckpoint(ctx, child); err != nil {
		t.Fatal(err)
	}
	for _, resource := range got.Resources {
		if err := s.PermitSnapshotDelete(ctx, resource); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RetainCheckpoint(ctx, source, child, "template"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("resurrected child: %v", err)
	}
}
