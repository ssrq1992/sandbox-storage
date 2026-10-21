package node

import (
	"context"
	"testing"
)

func TestRebootRequiresRuntimeReconciliation(t *testing.T) {
	n, _, key := setup(t)
	ctx := context.Background()
	if _, e := n.Prepare(ctx, "boot", plan(key, "boot")); e != nil {
		t.Fatal(e)
	}
	n.BootID = "new-boot"
	if e := n.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	r, e := n.WAL.Get("boot")
	if e != nil || r.State != "NeedsRuntimeReconcile" {
		t.Fatal("reboot silently dropped references")
	}
	holders, e := n.WAL.ForVolume(r.Plan.Bindings[0].Volume.Key())
	if e != nil || len(holders) != 1 {
		t.Fatal("lost pin on reboot")
	}
	if _, e = n.Release(ctx, "boot", "boot", "forged"); e == nil {
		t.Fatal("reboot authorized unverified cleanup")
	}
}
func TestReleasedRecordsLeaveActiveIndex(t *testing.T) {
	n, _, key := setup(t)
	ctx := context.Background()
	if _, e := n.Prepare(ctx, "index", plan(key, "index")); e != nil {
		t.Fatal(e)
	}
	if _, e := n.Release(ctx, "index", "index", "verified-test-state"); e != nil {
		t.Fatal(e)
	}
	active, e := n.WAL.Active()
	if e != nil || len(active) != 0 {
		t.Fatal("released operation in hot index")
	}
	if _, e = n.WAL.Get("index"); e != nil {
		t.Fatal("lost replay tombstone")
	}
}
