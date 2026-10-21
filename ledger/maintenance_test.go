package ledger

import (
	"errors"
	"ssf.dev/ssf/core"
	"testing"
)

func TestOfflineMaintenanceFencesWritersHoldsAndReclaim(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	v := core.Volume{Driver: "test", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginOfflineMaintenance(ctx, id, "expand", 4096); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginOfflineMaintenance(ctx, id, "expand", 4096); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginOfflineMaintenance(ctx, id, "different", 8192); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("maintenance identity replaced: %v", err)
	}
	b := core.Binding{ID: id + "b", Volume: v, Sandbox: id + "sandbox", Node: "n"}
	if err := s.Acquire(ctx, id+"op", "digest", []core.Binding{b}); !errors.Is(err, core.ErrPending) {
		t.Fatalf("writer crossed maintenance: %v", err)
	}
	if err := s.CompleteOfflineMaintenance(ctx, id, "wrong", 4096); !errors.Is(err, core.ErrConflict) {
		t.Fatal(err)
	}
	if err := s.CompleteOfflineMaintenance(ctx, id, "expand", 4096); err != nil {
		t.Fatal(err)
	}
	if err := s.Acquire(ctx, id+"op", "digest", []core.Binding{b}); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginOfflineMaintenance(ctx, id, "next", 8192); !errors.Is(err, core.ErrPending) {
		t.Fatalf("active writer resized: %v", err)
	}
	if err := s.RetainUse(ctx, id+"op", "paused"); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseBinding(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginOfflineMaintenance(ctx, id, "next", 8192); !errors.Is(err, core.ErrPending) {
		t.Fatalf("paused owner resized: %v", err)
	}
	if err := s.ReleaseUse(ctx, b.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginOfflineMaintenance(ctx, id, "next", 8192); err != nil {
		t.Fatal(err)
	}
	if err := s.Drain(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.Permit(ctx, id, "Retain", 1000000000); !errors.Is(err, core.ErrPending) {
		t.Fatalf("reclaim crossed maintenance: %v", err)
	}
}
