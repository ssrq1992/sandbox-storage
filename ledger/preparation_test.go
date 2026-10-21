package ledger

import (
	"context"
	"os"
	"testing"
	"time"

	"ssf.dev/ssf/core"
)

func TestPreparationStateMachine(t *testing.T) {
	for _, tc := range []struct {
		from, to string
		ok       bool
	}{
		{"WaitingConsumer", "Preparing", true},
		{"WaitingBinding", "VolumeReady", false},
		{"Preparing", "ContentReady", true},
		{"Preparing", "VolumeReady", false},
		{"Preparing", "NeedsRecovery", true},
		{"NeedsRecovery", "ContentReady", true},
		{"NeedsRecovery", "PendingPreparation", false},
		{"ContentReady", "VolumeReady", true},
		{"VolumeReady", "Preparing", false},
		{"Failed", "Preparing", false},
	} {
		if got := preparationTransition(tc.from, tc.to); got != tc.ok {
			t.Errorf("%s -> %s = %v", tc.from, tc.to, got)
		}
	}
}

func TestPostgresPreparationCASAndImmutableIdentity(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	initial := PreparationRecord{PVC: core.PVCRef{Namespace: "test", Name: id, UID: id}, Profile: core.VolumeProfile{Name: "profile", Runtime: "agentenv", Version: core.PreparationProtocol, VolumeMode: "Block", Filesystem: "ext4", InitializeBlank: true}, SourceRevision: "source", SourceSpec: "spec", State: "WaitingBinding"}
	prior, err := s.ObservePreparation(ctx, initial)
	if err != nil {
		t.Fatal(err)
	}
	changed := initial
	changed.SourceRevision = "replacement"
	if _, err = s.ObservePreparation(ctx, changed); err == nil {
		t.Fatal("source identity changed under same PVC")
	}
	next := prior
	next.State = "Preparing"
	next.Operation = id + "operation"
	if err = s.SavePreparation(ctx, prior, next); err != nil {
		t.Fatal(err)
	}
	if err = s.SavePreparation(ctx, prior, next); err == nil {
		t.Fatal("stale coordinator overwrote current state")
	}
	prior = next
	next.State = "ContentReady"
	next.Receipt = core.PreparationReceipt{Operation: next.Operation, UUID: "filesystem"}
	if err = s.SavePreparation(ctx, prior, next); err != nil {
		t.Fatal(err)
	}
	prior = next
	next.State = "VolumeReady"
	if err = s.SavePreparation(ctx, prior, next); err == nil {
		t.Fatal("ready before device release")
	}
	next.Receipt.Released = true
	if err = s.SavePreparation(ctx, prior, next); err != nil {
		t.Fatal(err)
	}
	prior = next
	next.Receipt.UUID = "replacement"
	if err = s.SavePreparation(ctx, prior, next); err == nil {
		t.Fatal("filesystem identity changed")
	}
}

func TestPostgresPreparationHandoffRejectsOtherRun(t *testing.T) {
	dsn := os.Getenv("SSF_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires dedicated PostgreSQL: SSF_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := "prep-" + time.Now().Format("150405.000000000")
	v := core.Volume{Driver: "fixture", Handle: id, PVC: core.PVCRef{UID: id}}
	if err = s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	epoch, err := s.RunEpoch(ctx, id, id)
	if err != nil {
		t.Fatal(err)
	}
	b := core.Binding{ID: id, Volume: v, OwnerUID: id, RunUID: id, Epoch: epoch, Node: "node"}
	if err = s.Acquire(ctx, id, id, []core.Binding{b}); err != nil {
		t.Fatal(err)
	}
	owner, run := id+"-owner", id+"-run"
	if err = s.PreparationHandoff(ctx, id, owner, run); err != nil {
		t.Fatal(err)
	}
	if err = s.PreparationHandoff(ctx, id, owner, run+"-other"); err == nil {
		t.Fatal("another run replaced handoff")
	}
	if err = s.ReleaseBinding(ctx, id); err != nil {
		t.Fatal(err)
	}
	acquire := func(run string) error {
		epoch, err := s.RunEpoch(ctx, owner, run)
		if err != nil {
			return err
		}
		return s.Acquire(ctx, run, run, []core.Binding{{ID: run, Volume: v, Sandbox: run, OwnerUID: owner, RunUID: run, Epoch: epoch, Node: "node"}})
	}
	if err = acquire(run + "-other"); err == nil {
		t.Fatal("different run stole preparation hold")
	}
	if err = acquire(run); err != nil {
		t.Fatal(err)
	}
	var holds int
	if err = s.Session(ctx).QueryRow(ctx, `SELECT count(*) FROM ssf_use_holds WHERE owner_uid=$1`, owner).Scan(&holds); err != nil || holds != 0 {
		t.Fatalf("handoff not atomically consumed: count=%d err=%v", holds, err)
	}
}
