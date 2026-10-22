package overlaybd

import (
	"context"
	"path/filepath"
	"ssf.dev/ssf/core"
	"testing"
)

func TestCaptureSurvivesRestartAndPublishesWithoutRecapture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.db")
	wal, err := OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	i := intent()
	i.HeadRevision = core.Hash("initial")
	i.AuthorizationDigest = core.Hash("grant")
	s, err := wal.Plan(i)
	s = mustSession(t, s, err)
	s, err = wal.BeginCreate(s)
	s = mustSession(t, s, err)
	e := evidence()
	e.HeadRevision = i.HeadRevision
	e.CapacityBytes = 4096
	e.FSUUID = "fs"
	s, err = wal.RecordDevice(s, e)
	mustSession(t, s, err)
	backend := &finalBackend{losePublish: true}
	n := NodeOperations{Sessions: wal, Backend: backend}
	proof := CaptureProof{CaptureID: "capture-one", BarrierID: "barrier-one", FreezeDigest: core.Hash("frozen"), ExpectedHead: i.HeadRevision}
	capture, err := n.Capture(context.Background(), i.ID, proof)
	if err != nil {
		t.Fatal(err)
	}
	if capture == nil || backend.publishes != 0 {
		t.Fatal("capture unexpectedly published")
	}
	other := proof
	other.CaptureID = "another"
	if _, err = n.Capture(context.Background(), i.ID, other); err != ErrConflict {
		t.Fatalf("overtook unpublished capture: %v", err)
	}
	changed := proof
	changed.FreezeDigest = core.Hash("different-barrier-proof")
	if _, err = n.Capture(context.Background(), i.ID, changed); err != ErrConflict {
		t.Fatalf("replaced barrier: %v", err)
	}
	if err = wal.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err = OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	n.Sessions = wal
	// The VM may already be thawed. Publishing must never execute a new restack.
	if _, err = n.PublishCaptured(context.Background(), i.ID, proof.CaptureID); err == nil {
		t.Fatal("lost reply succeeded")
	}
	if _, err = n.PublishCaptured(context.Background(), i.ID, proof.CaptureID); err != nil {
		t.Fatal(err)
	}
	if _, err = n.PublishCaptured(context.Background(), i.ID, proof.CaptureID); err != nil {
		t.Fatal(err)
	}
	if backend.captures != 1 || backend.publishes != 2 {
		t.Fatalf("capture=%d publish=%d", backend.captures, backend.publishes)
	}
	s, err = wal.Get(i.ID)
	if err != nil {
		t.Fatal(err)
	}
	if s.PublishedHead != core.Hash("published") {
		t.Fatal("published head not committed")
	}
	if _, err = n.PublishCaptured(context.Background(), i.ID, "never-captured"); err != ErrNotFound {
		t.Fatal("uncaptured data published")
	}
}

func TestRecoveryPublishesOnlyDurablyCapturedWork(t *testing.T) {
	wal, err := OpenSessions(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	i := intent()
	i.HeadRevision = core.Hash("initial")
	i.AuthorizationDigest = core.Hash("grant")
	s, err := wal.Plan(i)
	s = mustSession(t, s, err)
	s, err = wal.BeginCreate(s)
	s = mustSession(t, s, err)
	e := evidence()
	e.HeadRevision = i.HeadRevision
	e.CapacityBytes = 4096
	e.FSUUID = "fs"
	s, err = wal.RecordDevice(s, e)
	mustSession(t, s, err)
	backend := &finalBackend{}
	n := &CSINode{Operations: &NodeOperations{Sessions: wal, Backend: backend}}
	proof := CaptureProof{CaptureID: "capture", BarrierID: "barrier", FreezeDigest: core.Hash("freeze"), ExpectedHead: i.HeadRevision}
	if _, err = wal.pinCapture(i.ID, proof); err != nil {
		t.Fatal(err)
	}
	if err = n.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.captures != 0 || backend.publishes != 0 {
		t.Fatal("recovery guessed an unverified capture")
	}
	if _, err = n.Operations.Capture(context.Background(), i.ID, proof); err != nil {
		t.Fatal(err)
	}
	if err = n.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = n.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.captures != 1 || backend.publishes != 1 {
		t.Fatal("recovery repeated capture or publication")
	}
}
