package overlaybd

import (
	"context"
	"encoding/base64"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestVolumeCursorRejectsForeignTokens(t *testing.T) {
	for _, token := range []string{"!", base64.RawURLEncoding.EncodeToString([]byte("not-a-volume")), "eA=="} {
		if _, err := decodeVolumeCursor(token); status.Code(err) != codes.Aborted {
			t.Fatalf("cursor %q: %v", token, err)
		}
	}
}
func TestPostgresCSIQueriesPaginateAndExcludeDeleted(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	a := snapshotFixture(t, j)
	b := snapshotFixture(t, j)
	c := &Controller{Creator: &CSICreator{Journal: j}}
	if _, err := j.Pool.Exec(ctx, `INSERT INTO ssf_overlaybd.attachments(handle,node_id,grant_digest,publish_context,state) VALUES($1,'node-a','test','{}','Attached')`, a.SourceHandle); err != nil {
		t.Fatal(err)
	}
	got, err := c.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: a.SourceHandle})
	if err != nil || got.Volume.VolumeId != a.SourceHandle || len(got.Status.PublishedNodeIds) != 1 {
		t.Fatalf("get: %v %v", got, err)
	}
	seen := map[string]bool{}
	token := ""
	for pages := 0; pages < 10000; pages++ {
		page, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 1, StartingToken: token})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Entries) > 1 {
			t.Fatal("page exceeded limit")
		}
		for _, entry := range page.Entries {
			if seen[entry.Volume.VolumeId] {
				t.Fatal("duplicate cursor entry")
			}
			seen[entry.Volume.VolumeId] = true
		}
		if page.NextToken == "" {
			break
		}
		if page.NextToken == token {
			t.Fatal("cursor did not advance")
		}
		token = page.NextToken
	}
	if !seen[a.SourceHandle] || !seen[b.SourceHandle] {
		t.Fatal("volume missing from pages")
	}
	if err = j.BeginVolumeDeletion(ctx, b.SourceHandle); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: b.SourceHandle}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleting volume exposed: %v", err)
	}
}
