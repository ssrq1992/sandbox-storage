package node

import (
	"context"
	"ssf.dev/ssf/auth"
	"testing"
	"time"
)

func TestRevocationAfterExpiryAndBootFence(t *testing.T) {
	n, driver, key := setup(t)
	p, err := auth.Verify(plan(key, "old-run"), n.Key, n.NodeID, n.BootID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p.NodeUID = "physical-node"
	p.Expires = time.Now().Add(-time.Hour)
	token, err := auth.Sign(p, key)
	if err != nil {
		t.Fatal(err)
	}
	n.BootID = "new-boot"
	n.NodeUID = "replacement-node"
	if err = n.CancelUnstarted(context.Background(), p.Operation, p.Sandbox, token, "verified-test-state"); err == nil {
		t.Fatal("accepted different physical node")
	}
	n.NodeUID = p.NodeUID
	if err = n.CancelUnstarted(context.Background(), p.Operation, p.Sandbox, token, "forged"); err == nil {
		t.Fatal("accepted unverified stop")
	}
	if err = n.CancelUnstarted(context.Background(), p.Operation, p.Sandbox, token, "verified-test-state"); err != nil {
		t.Fatal(err)
	}
	r, err := n.WAL.Get(p.Operation)
	if err != nil || r.State != "Released" || r.Plan.BootID != p.BootID {
		t.Fatalf("missing original-plan tombstone: %+v %v", r, err)
	}
	if _, err = n.Prepare(context.Background(), p.Operation, token); err == nil {
		t.Fatal("revoked run resurrected")
	}
	if driver.stage != 0 || driver.publish != 0 {
		t.Fatal("revocation prepared storage")
	}
}
