package overlaybd

import (
	"context"
	"fmt"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"strings"
	"sync"
	"testing"
	"time"
)

func snapshotFixture(t *testing.T, j *Journal) SnapshotPin {
	t.Helper()
	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	head := strings.Repeat("a", 64)
	receipt := &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: Identity("create", name), RepositoryId: "repository", VolumeId: Identity("volume", name), InputDigest: head}, State: "ContentReady", HeadRevision: head, ContentDigest: head, CapacityBytes: 1 << 30, CapacityGeneration: 1, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}
	v, err := j.RegisterVolume(context.Background(), name, "", Profile, false, receipt)
	if err != nil {
		t.Fatal(err)
	}
	return SnapshotPin{ID: Identity("snapshot", name), RequestName: name, SourceHandle: v.Handle, HeadRevision: head, CapacityBytes: v.CapacityBytes, FSUUID: v.FSUUID, SnapshotUID: name + "-snapshot", ContentUID: name + "-content", CaptureDigest: head, CreatedUnixNano: time.Now().UnixNano()}
}
func TestPostgresSnapshotClonePinsAndTombstones(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	p := snapshotFixture(t, j)
	if err := j.RegisterSnapshot(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := j.RegisterSnapshot(ctx, p); err != nil {
		t.Fatal(err)
	}
	changed := p
	changed.HeadRevision = strings.Repeat("b", 64)
	if err := j.RegisterSnapshot(ctx, changed); err != ErrConflict {
		t.Fatalf("head changed: %v", err)
	}
	if err := j.BeginVolumeDeletion(ctx, p.SourceHandle); err != ErrConflict {
		t.Fatalf("snapshot source deletion: %v", err)
	}
	if _, err := j.PinSnapshotSource(ctx, p.ID, "clone-"+p.ID); err != nil {
		t.Fatal(err)
	}
	if err := j.DeleteSnapshotPin(ctx, p.ID); err != ErrConflict {
		t.Fatalf("in-flight clone lost source: %v", err)
	}
	if err := j.ReleaseSnapshotSource(ctx, p.ID, "clone-"+p.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := j.PinSnapshotSource(ctx, p.ID, "clone-"+p.ID); err != ErrConflict {
		t.Fatalf("released clone revived: %v", err)
	}
	if err := j.DeleteSnapshotPin(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if err := j.RegisterSnapshot(ctx, p); err != ErrConflict {
		t.Fatalf("snapshot revived: %v", err)
	}
	if _, err := j.Snapshot(ctx, p.ID); err != ErrNotFound {
		t.Fatalf("deleted snapshot visible: %v", err)
	}
	if err := j.BeginVolumeDeletion(ctx, p.SourceHandle); err != nil {
		t.Fatal(err)
	}
	p.ID = Identity("snapshot", p.RequestName+"new")
	p.RequestName += "new"
	if err := j.RegisterSnapshot(ctx, p); err != ErrConflict {
		t.Fatalf("deleting volume acquired pin: %v", err)
	}
	if err := j.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestPostgresSnapshotRegistrationRacesVolumeDeletion(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	for i := 0; i < 12; i++ {
		p := snapshotFixture(t, j)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var pinErr, deleteErr error
		go func() { defer wg.Done(); <-start; pinErr = j.RegisterSnapshot(ctx, p) }()
		go func() { defer wg.Done(); <-start; deleteErr = j.BeginVolumeDeletion(ctx, p.SourceHandle) }()
		close(start)
		wg.Wait()
		if (pinErr == nil) == (deleteErr == nil) {
			t.Fatalf("exactly one operation must win: pin=%v delete=%v", pinErr, deleteErr)
		}
		if pinErr != nil && pinErr != ErrConflict {
			t.Fatal(pinErr)
		}
		if deleteErr != nil && deleteErr != ErrConflict {
			t.Fatal(deleteErr)
		}
	}
}

func TestPostgresSnapshotCancellationPreservesConcurrentPublication(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	for i := 0; i < 12; i++ {
		p := snapshotFixture(t, j)
		start := make(chan struct{})
		registered := make(chan error, 1)
		type outcome struct {
			cancelled bool
			err       error
		}
		cancelled := make(chan outcome, 1)
		go func() { <-start; registered <- j.RegisterSnapshot(ctx, p) }()
		go func() { <-start; b, e := j.CancelSnapshotRegistration(ctx, p.ID); cancelled <- outcome{b, e} }()
		close(start)
		registration, cancellation := <-registered, <-cancelled
		if cancellation.err != nil {
			t.Fatal(cancellation.err)
		}
		if registration == nil {
			if cancellation.cancelled {
				t.Fatal("successful Retain publication cancelled")
			}
			if _, err := j.Snapshot(ctx, p.ID); err != nil {
				t.Fatal(err)
			}
		} else if registration == ErrConflict {
			if !cancellation.cancelled {
				t.Fatal("neither registration nor cancellation won")
			}
			if err := j.RegisterSnapshot(ctx, p); err != ErrConflict {
				t.Fatalf("late register bypassed cancellation: %v", err)
			}
		} else {
			t.Fatal(registration)
		}
	}
}
