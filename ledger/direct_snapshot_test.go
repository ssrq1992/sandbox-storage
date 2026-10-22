package ledger

import (
	"errors"
	"ssf.dev/ssf/core"
	"sync"
	"testing"
)

func TestPostgresDirectSnapshotRetainsBarrierAndRetirementWinsLateReply(t *testing.T) {
	s, ctx, id := lifecycleStore(t)
	q := core.DirectSnapshotRef{ID: id, Namespace: "test", Name: "snapshot", SnapshotUID: id + "-snap", ContentName: "content", ContentUID: id + "-content", CreatedUnixNano: 1, Volume: core.Volume{Driver: "driver", Handle: id, PVC: core.PVCRef{UID: id + "-pvc"}}}
	if err := s.TrackDirectSnapshot(ctx, q); err != nil {
		t.Fatal(err)
	}
	if err := s.PermitSnapshotDelete(ctx, q.Volume.PVC.UID); err == nil {
		t.Fatal("pending snapshot source unprotected")
	}
	cp := id + "-checkpoint"
	resource := id + "-artifact"
	if err := s.BeginCheckpoint(ctx, cp, "owner", map[string]string{"capture": cp}); err != nil {
		t.Fatal(err)
	}
	if err := s.PinCheckpoint(ctx, cp, resource); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitCheckpoint(ctx, cp, map[string]string{"capture": cp}); err != nil {
		t.Fatal(err)
	}
	if err := s.RetainDirectCheckpoint(ctx, id, cp); err != nil {
		t.Fatal(err)
	}
	if err := s.RetainDirectCheckpoint(ctx, id, cp); err != nil {
		t.Fatal("replay", err)
	}
	if err := s.ReleaseCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err := s.PermitSnapshotDelete(ctx, resource); err == nil {
		t.Fatal("direct snapshot lost native artifact reference")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.RetainDirectCheckpoint(ctx, id, cp)
			if err != nil && !errors.Is(err, core.ErrPending) {
				t.Error(err)
			}
		}()
	}
	if err := s.RetireDirectSnapshot(ctx, id); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := s.RetainDirectCheckpoint(ctx, id, cp); !errors.Is(err, core.ErrPending) {
		t.Fatal("late reply resurrected reference", err)
	}
	if err := s.TrackDirectSnapshot(ctx, q); !errors.Is(err, core.ErrConflict) {
		t.Fatal("retired object reopened", err)
	}
	if err := s.RetireDirectSnapshot(ctx, id); err != nil {
		t.Fatal("retirement replay", err)
	}
	if err := s.PermitSnapshotDelete(ctx, resource); err != nil {
		t.Fatal("retirement retained stale pin", err)
	}
	if err := s.PermitSnapshotDelete(ctx, q.Volume.PVC.UID); err != nil {
		t.Fatal("source pin retained after retirement", err)
	}
	var state string
	if err := s.Pool.QueryRow(ctx, `SELECT state FROM ssf_checkpoint_gc WHERE checkpoint_id=$1`, "direct-source/"+id).Scan(&state); err != nil || state != "Complete" {
		t.Fatal("metadata reference reached runtime GC", state, err)
	}
}
