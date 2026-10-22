package ledger

import (
	"errors"
	"ssf.dev/ssf/core"
	"testing"
)

func TestBackendExpansionAuthorizationPinsOneOperation(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	v := core.Volume{Driver: "overlaybd-test", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	authorize := func(op, head, uid string, target int64) (string, error) {
		return s.AuthorizeBackendExpansion(ctx, v.Driver, v.Handle, uid, op, head, target)
	}
	if _, err := authorize("expand", "head", id, 8192); err == nil {
		t.Fatal("no maintenance fence")
	}
	if err := s.BeginOfflineMaintenance(ctx, id, "maintenance", 8192); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		uid   string
		bytes int64
	}{{"replacement", 8192}, {id, 4096}} {
		if _, err := authorize("expand", "head", item.uid, item.bytes); err == nil {
			t.Fatal("wrong fence accepted")
		}
	}
	proof, err := authorize("expand", "head", id, 8192)
	if err != nil || len(proof) != 64 {
		t.Fatalf("proof %q: %v", proof, err)
	}
	if again, err := authorize("expand", "head", id, 8192); err != nil || again != proof {
		t.Fatalf("replay: %v", err)
	}
	if _, err := authorize("other", "head", id, 8192); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("second operation: %v", err)
	}
	if _, err := authorize("expand", "other-head", id, 8192); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("retargeted head: %v", err)
	}
	if err := s.CompleteOfflineMaintenance(ctx, id, "maintenance", 8192); err != nil {
		t.Fatal(err)
	}
	if _, err := authorize("expand", "head", id, 8192); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("completed fence reused: %v", err)
	}
}

func TestBackendMaintenanceReservationAndCompletionNeverReleaseNewerOperation(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	v := core.Volume{Driver: "driver", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	if err := s.ReserveBackendExpansion(ctx, id, "first", 8192); err != nil {
		t.Fatal(err)
	}
	if err := s.ReserveBackendExpansion(ctx, id, "retry", 8192); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthorizeBackendExpansion(ctx, v.Driver, v.Handle, id, "first", "head", 8192); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteBackendExpansion(ctx, id, "wrong", 8192); !errors.Is(err, core.ErrConflict) {
		t.Fatal("wrong completion", err)
	}
	if err := s.CompleteBackendExpansion(ctx, id, "first", 8192); err != nil {
		t.Fatal(err)
	}
	if err := s.ReserveBackendExpansion(ctx, id, "second", 16384); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteBackendExpansion(ctx, id, "first", 8192); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AllowedExpansion(ctx, id, 16384); err != nil || !ok {
		t.Fatal("old completion released new fence", err)
	}
}

func TestMaintenanceRoundingCannotRetargetAnAuthorizedOperation(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	v := core.Volume{Driver: "driver", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginOfflineMaintenance(ctx, id, "facade", 1536); err != nil {
		t.Fatal(err)
	}
	if err := s.ReserveBackendExpansion(ctx, id, "driver", 2048, 1536); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AllowedExpansion(ctx, id, 2048); err != nil || !ok {
		t.Fatal(err)
	}
	if _, err := s.AuthorizeBackendExpansion(ctx, v.Driver, v.Handle, id, "driver", "head", 2048); err != nil {
		t.Fatal(err)
	}
	if err := s.ReserveBackendExpansion(ctx, id, "changed", 4096, 2048); !errors.Is(err, core.ErrConflict) {
		t.Fatal("authorized capacity changed", err)
	}
}

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
