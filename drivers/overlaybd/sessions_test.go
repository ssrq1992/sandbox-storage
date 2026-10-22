package overlaybd

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func intent() SessionIntent {
	return SessionIntent{Purpose: "runtime", ID: "session-a", RepositoryID: "repo-a", VolumeID: "vol-a", OperationID: "op-a", OwnerID: "owner-a", RunID: "run-a", Epoch: 7, NodeID: "node-a", HeadRevision: "head-a", AuthorizationDigest: "signed-grant-digest"}
}
func evidence() DeviceEvidence {
	return DeviceEvidence{SessionID: "session-a", OperationID: "op-a", Epoch: 7, HeadRevision: "head-a", NodeBootID: "boot-a", DaemonInstanceID: "daemon-a", DeviceIdentity: "device-a", Path: "/dev/ublkb0", Major: 252, CapacityBytes: 2 << 30, FSUUID: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}
}
func mustSession(t *testing.T, s Session, err error) Session {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSessionUnknownRestartCannotCreateSecondWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.db")
	wal, err := OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	x, err := wal.Plan(intent())
	x = mustSession(t, x, err)
	x, err = wal.BeginCreate(x)
	x = mustSession(t, x, err)
	x, err = wal.MarkUnknown(x)
	x = mustSession(t, x, err)
	if err = wal.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err = OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	x, err = wal.Get(x.Intent.ID)
	x = mustSession(t, x, err)
	if _, err = wal.BeginCreate(x); !errors.Is(err, ErrTransition) {
		t.Fatal("unknown create restarted")
	}
	other := intent()
	other.ID = "session-b"
	other.Epoch++
	other.RunID = "run-b"
	if _, err = wal.Plan(other); !errors.Is(err, ErrConflict) {
		t.Fatal("second local writer accepted")
	}
	wrong := evidence()
	wrong.Epoch--
	if _, err = wal.RecordDevice(x, wrong); err == nil {
		t.Fatal("stale receipt accepted")
	}
	x, err = wal.RecordDevice(x, evidence())
	mustSession(t, x, err)
}

func TestBorrowedFDAndReleaseTombstoneSurviveRetry(t *testing.T) {
	wal, err := OpenSessions(filepath.Join(t.TempDir(), "sessions.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	x, err := wal.Plan(intent())
	x = mustSession(t, x, err)
	x, err = wal.BeginCreate(x)
	x = mustSession(t, x, err)
	x, err = wal.RecordDevice(x, evidence())
	x = mustSession(t, x, err)
	x, err = wal.RecordPublish(x, "/managed/publish/a")
	x = mustSession(t, x, err)
	x, err = wal.Borrow(x, "/managed/publish/a")
	x = mustSession(t, x, err)
	if _, err = wal.RemovePublish(x, "/managed/publish/a"); err == nil {
		t.Fatal("removed borrowed FD")
	}
	proof := StopProof{SessionID: x.Intent.ID, RunID: x.Intent.RunID, Epoch: x.Intent.Epoch, DeviceIdentity: "device-a", Digest: "verified-native-stop"}
	stale := proof
	stale.Epoch--
	if _, err = wal.ReturnBorrow(x, "/managed/publish/a", stale); err == nil {
		t.Fatal("stale stop accepted")
	}
	x, err = wal.ReturnBorrow(x, "/managed/publish/a", proof)
	x = mustSession(t, x, err)
	x, err = wal.RemovePublish(x, "/managed/publish/a")
	x = mustSession(t, x, err)
	x, err = wal.BeginRelease(x, proof)
	x = mustSession(t, x, err)
	x, err = wal.MarkUnknown(x)
	x = mustSession(t, x, err)
	if _, err = wal.RecordDevice(x, evidence()); err == nil {
		t.Fatal("release tombstone resurrected")
	}
	if _, err = wal.RecordReleased(x, "different-device-same-path"); err == nil {
		t.Fatal("released reused device number")
	}
	x, err = wal.RecordReleased(x, "device-a")
	x = mustSession(t, x, err)
	other := intent()
	other.ID = "session-b"
	other.RunID = "run-b"
	other.Epoch++
	_, err = wal.Plan(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wal.BeginCreate(x); err == nil {
		t.Fatal("released session restarted")
	}
}

func TestPreparationUsesItsOwnReleaseEvidence(t *testing.T) {
	wal, err := OpenSessions(filepath.Join(t.TempDir(), "sessions.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	plan := intent()
	plan.Purpose = "preparation"
	x, err := wal.Plan(plan)
	x = mustSession(t, x, err)
	x, err = wal.BeginCreate(x)
	x = mustSession(t, x, err)
	x, err = wal.RecordDevice(x, evidence())
	x = mustSession(t, x, err)
	x, err = wal.RecordPublish(x, "/prepared")
	x = mustSession(t, x, err)
	if _, err = wal.Borrow(x, "/prepared"); err == nil {
		t.Fatal("preparation borrowed as VMM device")
	}
	x, err = wal.RemovePublish(x, "/prepared")
	x = mustSession(t, x, err)
	if _, err = wal.BeginRelease(x, StopProof{SessionID: plan.ID, RunID: plan.RunID, Epoch: plan.Epoch, DeviceIdentity: evidence().DeviceIdentity, Digest: "fake-stop"}); err == nil {
		t.Fatal("preparation accepted VMM stop")
	}
	proof := PreparationProof{SessionID: plan.ID, Epoch: plan.Epoch, DeviceIdentity: evidence().DeviceIdentity, NodeBootID: evidence().NodeBootID, DaemonInstanceID: evidence().DaemonInstanceID, Digest: strings.Repeat("a", 64)}
	stale := proof
	stale.DaemonInstanceID = "restarted"
	if _, err = wal.BeginPreparationRelease(x, stale); err == nil {
		t.Fatal("stale preparation receipt")
	}
	x, err = wal.BeginPreparationRelease(x, proof)
	x = mustSession(t, x, err)
	x, err = wal.RecordReleased(x, evidence().DeviceIdentity)
	mustSession(t, x, err)
}
