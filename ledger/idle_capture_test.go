package ledger

import (
	"errors"
	"fmt"
	"ssf.dev/ssf/core"
	"testing"
	"time"
)

func TestPostgresIdleCaptureFencesUseAndRetainsSource(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	v := core.Volume{Driver: "idle", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	capture, err := s.BeginIdleCapture(ctx, id, v, "snapshot-uid")
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.BeginIdleCapture(ctx, id, v, "snapshot-uid")
	if err != nil || again.Digest != capture.Digest {
		t.Fatalf("replay: %v", err)
	}
	if _, err = s.BeginIdleCapture(ctx, id, v, "replacement"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("identity replaced: %v", err)
	}
	if _, err = s.BeginIdleCapture(ctx, id+"other", v, "another"); !errors.Is(err, core.ErrPending) {
		t.Fatalf("two capture operations: %v", err)
	}
	b := core.Binding{ID: id + "binding", Volume: v, Sandbox: id + "sandbox", Node: "node"}
	if err = s.Acquire(ctx, id+"run", "digest", []core.Binding{b}); !errors.Is(err, core.ErrPending) {
		t.Fatalf("writer crossed capture: %v", err)
	}
	if err = s.BeginOfflineMaintenance(ctx, id, "expansion", 8192); !errors.Is(err, core.ErrPending) {
		t.Fatalf("expansion crossed capture: %v", err)
	}
	receipt := map[string]string{"head": "immutable-head"}
	if err = s.CommitIdleCapture(ctx, id, capture.Digest, receipt); err != nil {
		t.Fatal(err)
	}
	if err = s.CommitIdleCapture(ctx, id, capture.Digest, receipt); err != nil {
		t.Fatal(err)
	}
	if err = s.CommitIdleCapture(ctx, id, capture.Digest, map[string]string{"head": "replacement"}); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("head changed: %v", err)
	}
	if err = s.Acquire(ctx, id+"run", "digest", []core.Binding{b}); err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseBinding(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.Drain(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err = s.Permit(ctx, id, "Delete", time.Minute); !errors.Is(err, core.ErrPending) {
		t.Fatalf("source reclaimed while snapshot exists: %v", err)
	}
	if _, err = s.AuthorizeBackendReclaim(ctx, v.Driver, v.Handle, "delete"); !errors.Is(err, core.ErrPending) {
		t.Fatalf("backend reclaimed: %v", err)
	}
	if err = s.RetireIdleCapture(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err = s.CommitIdleCapture(ctx, id, capture.Digest, receipt); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("late receipt resurrected capture: %v", err)
	}
	if _, err = s.BeginIdleCapture(ctx, id, v, "snapshot-uid"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("retired capture restarted: %v", err)
	}
	if err = s.Permit(ctx, id, "Delete", time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresIdleCaptureRejectsBusyAndUnknownDeletion(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	v := core.Volume{Driver: "idle", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireIdleCapture(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginIdleCapture(ctx, id, v, "input"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("delete-before-create bypassed: %v", err)
	}
	b := core.Binding{ID: id + "binding", Volume: v, Sandbox: id + "sandbox", Node: "node"}
	if err := s.Acquire(ctx, id+"run", "digest", []core.Binding{b}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginIdleCapture(ctx, id+"active", v, "input"); !errors.Is(err, core.ErrPending) {
		t.Fatalf("active writer ignored: %v", err)
	}
	if err := s.RetainUse(ctx, id+"run", "paused"); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseBinding(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginIdleCapture(ctx, id+"paused", v, "input"); !errors.Is(err, core.ErrPending) {
		t.Fatalf("paused hold ignored: %v", err)
	}
}

func TestPostgresIdleCaptureCompetesWithWriter(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	for i := 0; i < 8; i++ {
		suffix := fmt.Sprintf("%s-%d", id, i)
		v := core.Volume{Driver: "idle", Handle: suffix, PVC: core.PVCRef{UID: suffix}}
		if err := s.Register(ctx, v); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		capture := make(chan error, 1)
		writer := make(chan error, 1)
		go func() { <-start; _, err := s.BeginIdleCapture(ctx, suffix, v, "snapshot"); capture <- err }()
		go func() {
			<-start
			writer <- s.Acquire(ctx, suffix+"run", "digest", []core.Binding{{ID: suffix + "binding", Volume: v, Sandbox: suffix + "sandbox", Node: "node"}})
		}()
		close(start)
		ce, we := <-capture, <-writer
		if (ce == nil) == (we == nil) {
			t.Fatalf("expected one winner, capture=%v writer=%v", ce, we)
		}
		loser := ce
		if loser == nil {
			loser = we
		}
		if !errors.Is(loser, core.ErrPending) {
			t.Fatalf("unexpected loser: %v", loser)
		}
	}
}

func TestPostgresIdleCaptureRejectsUnknownPreparationAndAlias(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	v := core.Volume{Driver: "idle", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	alias := v
	alias.PVC.UID = id + "other"
	if _, err := s.BeginIdleCapture(ctx, id, alias, "snapshot"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("alias acquired separate capture: %v", err)
	}
	for _, state := range []string{"Preparing", "ContentReady", "NeedsRecovery"} {
		if _, err := s.Pool.Exec(ctx, `INSERT INTO ssf_volume_preparations(pvc_uid,digest,state,descriptor) VALUES($1,'test',$2,'{}') ON CONFLICT(pvc_uid) DO UPDATE SET state=excluded.state`, id, state); err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginIdleCapture(ctx, id, v, "snapshot"); !errors.Is(err, core.ErrPending) {
			t.Fatalf("%s preparation bypassed: %v", state, err)
		}
		if err := s.BeginOfflineMaintenance(ctx, id, "expand", 8192); !errors.Is(err, core.ErrPending) {
			t.Fatalf("%s preparation expanded: %v", state, err)
		}
	}
}

func TestPostgresPublishedPauseSnapshotFencesResumeWithoutDroppingHold(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	v := core.Volume{Driver: "managed", Handle: id, PVC: core.PVCRef{UID: id}}
	if err := s.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	owner, run := id+"owner", id+"run"
	epoch, err := s.RunEpoch(ctx, owner, run)
	if err != nil {
		t.Fatal(err)
	}
	b := core.Binding{ID: id + "binding", Volume: v, OwnerUID: owner, RunUID: run, Epoch: epoch, Sandbox: run, Node: "node"}
	if err = s.Acquire(ctx, id+"op", "input", []core.Binding{b}); err != nil {
		t.Fatal(err)
	}
	if err = s.RetainUse(ctx, id+"op", "paused"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginPublishedCapture(ctx, id+"snapshot", v, "input"); !errors.Is(err, core.ErrPending) {
		t.Fatal("live binding bypassed", err)
	}
	if err = s.ReleaseBinding(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginPublishedCapture(ctx, id+"snapshot", v, "input"); !errors.Is(err, core.ErrPending) {
		t.Fatal("unknown process exit bypassed", err)
	}
	if err = s.StopRun(ctx, owner, run); err != nil {
		t.Fatal(err)
	}
	capture, err := s.BeginPublishedCapture(ctx, id+"snapshot", v, "input")
	if err != nil {
		t.Fatal(err)
	}
	next := b
	next.ID += "next"
	next.RunUID += "next"
	next.Sandbox = next.RunUID
	next.Epoch, err = s.RunEpoch(ctx, owner, next.RunUID)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Acquire(ctx, id+"resume", "input", []core.Binding{next}); !errors.Is(err, core.ErrPending) {
		t.Fatal("resume bypassed capture fence", err)
	}
	if err = s.CommitIdleCapture(ctx, capture.ID, capture.Digest, map[string]string{"head": "immutable"}); err != nil {
		t.Fatal(err)
	}
	var holds int
	if err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM ssf_use_holds WHERE volume_key=$1 AND owner_uid=$2`, v.Key(), owner).Scan(&holds); err != nil || holds != 1 {
		t.Fatal("snapshot consumed paused ownership", holds, err)
	}
	if err = s.Acquire(ctx, id+"resume", "input", []core.Binding{next}); err != nil {
		t.Fatal("resume blocked after durable capture", err)
	}
}
