package overlaybd

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	api "ssf.dev/ssf/api/overlaybd/v1"
)

func TestCanonicalNativeHandles(t *testing.T) {
	handle, err := VolumeHandle("repo-a", "native-a")
	if err != nil {
		t.Fatal(err)
	}
	repo, id, err := ParseVolumeHandle(handle)
	if err != nil || repo != "repo-a" || id != "native-a" {
		t.Fatal("native identity did not round-trip")
	}
	for _, invalid := range []string{handle + "=", "obd1..", "repo/native", "", handle + ".suffix"} {
		if _, _, err = ParseVolumeHandle(invalid); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
	if _, err = VolumeHandle("../../legacy", "volume"); err == nil {
		t.Fatal("path accepted as repository identity")
	}
}
func TestPostgresCatalogPinsIdentityAndNeverRevivesDeletedVolume(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	receipt := &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: Identity("create", name), RepositoryId: "repository", VolumeId: Identity("volume", name), InputDigest: strings.Repeat("a", 64)},
		State: "ContentReady", HeadRevision: strings.Repeat("b", 64), ContentDigest: strings.Repeat("b", 64), CapacityBytes: 1 << 30, CapacityGeneration: 1, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}
	volume, err := j.RegisterVolume(ctx, name, "pvc-uid", Profile, false, receipt)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := j.RegisterVolume(ctx, name, "pvc-uid", Profile, false, receipt)
	if err != nil || replay.Handle != volume.Handle {
		t.Fatalf("register retry: %v", err)
	}
	if _, err = j.RegisterVolume(ctx, name, "replacement-pvc", Profile, false, receipt); err == nil {
		t.Fatal("PVC identity replaced")
	}
	if _, err = j.Volume(ctx, volume.Handle); err != nil {
		t.Fatal(err)
	}
	if _, err = j.Pool.Exec(ctx, `UPDATE ssf_overlaybd.volumes SET state='Deleted' WHERE handle=$1`, volume.Handle); err != nil {
		t.Fatal(err)
	}
	if _, err = j.RegisterVolume(ctx, name, "pvc-uid", Profile, false, receipt); err == nil {
		t.Fatal("deleted identity resurrected")
	}
	if _, err = j.Volume(ctx, volume.Handle); err != ErrNotFound {
		t.Fatalf("deleted volume exposed: %v", err)
	}
	if err = j.Migrate(ctx); err != nil {
		t.Fatalf("migration replay failed: %v", err)
	}
}
