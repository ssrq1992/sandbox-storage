package ledger

import (
	"errors"
	"ssf.dev/ssf/core"
	"testing"
)

func TestBackendReclaimBeforeObserverBlocksLateRegistration(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	volume := core.Volume{Driver: "native-test", Handle: id, PVC: core.PVCRef{UID: id}}
	proof, err := s.AuthorizeBackendReclaim(ctx, volume.Driver, volume.Handle, "delete-a")
	if err != nil || proof == "" {
		t.Fatalf("unobserved backend fence: %v", err)
	}
	replay, err := s.AuthorizeBackendReclaim(ctx, volume.Driver, volume.Handle, "delete-a")
	if err != nil || replay != proof {
		t.Fatal("reclaim proof changed")
	}
	if err = s.Register(ctx, volume); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("late observer resurrected backend: %v", err)
	}
	volume.PVC.UID += "-alias"
	if err = s.Register(ctx, volume); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("alias bypassed tombstone: %v", err)
	}
	if _, err = s.AuthorizeBackendReclaim(ctx, volume.Driver, volume.Handle, "delete-b"); !errors.Is(err, core.ErrConflict) {
		t.Fatal("reclaim identity replaced")
	}
}
func TestBackendReclaimWaitsForRunHoldAndCheckpoint(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	volume := core.Volume{Driver: "native-test", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, volume); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthorizeBackendReclaim(ctx, volume.Driver, volume.Handle, "delete"); !errors.Is(err, core.ErrPending) {
		t.Fatal("non-draining volume reclaimed")
	}
	binding := core.Binding{ID: id + "binding", Volume: volume, Sandbox: id + "sandbox", Node: "node"}
	if err := s.Acquire(ctx, id+"run", "digest", []core.Binding{binding}); err != nil {
		t.Fatal(err)
	}
	if err := s.RetainUse(ctx, id+"run", "paused"); err != nil {
		t.Fatal(err)
	}
	if err := s.Drain(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthorizeBackendReclaim(ctx, volume.Driver, volume.Handle, "delete"); !errors.Is(err, core.ErrPending) {
		t.Fatal("active run reclaimed")
	}
	if err := s.ReleaseBinding(ctx, binding.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthorizeBackendReclaim(ctx, volume.Driver, volume.Handle, "delete"); !errors.Is(err, core.ErrPending) {
		t.Fatal("paused use reclaimed")
	}
	if _, err := s.Pool.Exec(ctx, `DELETE FROM ssf_use_holds WHERE volume_key=$1`, volume.Key()); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginCheckpoint(ctx, id+"checkpoint", id+"owner", map[string]string{"run": id}); err != nil {
		t.Fatal(err)
	}
	if err := s.PinCheckpoint(ctx, id+"checkpoint", id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthorizeBackendReclaim(ctx, volume.Driver, volume.Handle, "delete"); !errors.Is(err, core.ErrPending) {
		t.Fatal("checkpoint source reclaimed")
	}
	if _, err := s.Pool.Exec(ctx, `DELETE FROM ssf_checkpoint_pins WHERE checkpoint_id=$1`, id+"checkpoint"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthorizeBackendReclaim(ctx, volume.Driver, volume.Handle, "delete"); err != nil {
		t.Fatal(err)
	}
	if err := s.PinCheckpoint(ctx, id+"checkpoint", id); err == nil {
		t.Fatal("late checkpoint crossed reclaim fence")
	}
}
