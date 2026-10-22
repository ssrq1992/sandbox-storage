package node

import (
	"context"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	"testing"
	"time"
)

func TestBindingStageSurvivesRecoveryWithoutSharing(t *testing.T) {
	n, f, key := setup(t)
	ctx := context.Background()
	for _, op := range []string{"one", "two"} {
		p, err := auth.Verify(plan(key, op), n.Key, n.NodeID, n.BootID, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		p.PublishContexts = map[string]map[string]string{op: {core.StageScopeKey: core.StageScopeBinding}}
		token, err := auth.Sign(p, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = n.Prepare(ctx, op, token); err != nil {
			t.Fatal(err)
		}
	}
	if f.stage != 2 {
		t.Fatalf("separate sessions reused stage: %d", f.stage)
	}
	first, _ := n.WAL.Get("one")
	second, _ := n.WAL.Get("two")
	a, _ := n.paths(first.Plan, first.Plan.Bindings[0])
	b, _ := n.paths(second.Plan, second.Plan.Bindings[0])
	if a == b {
		t.Fatal("session paths collided")
	}
	// Simulate restart after durable release authorization, before cleanup RPC.
	first.State = "Releasing"
	if err := n.WAL.Save(first); err != nil {
		t.Fatal(err)
	}
	if err := n.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if f.unstage != 1 {
		t.Fatalf("other run prevented session cleanup: %d", f.unstage)
	}
	if _, err := n.Release(ctx, "two", "two", "verified-test-state"); err != nil {
		t.Fatal(err)
	}
	if f.unstage != 2 {
		t.Fatalf("missing session cleanup: %d", f.unstage)
	}
}

func TestUnknownStageScopeFailsBeforeDeviceIO(t *testing.T) {
	n, f, key := setup(t)
	p, err := auth.Verify(plan(key, "one"), n.Key, n.NodeID, n.BootID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p.PublishContexts = map[string]map[string]string{"one": {core.StageScopeKey: "future"}}
	token, _ := auth.Sign(p, key)
	if _, err = n.Prepare(context.Background(), "one", token); err == nil {
		t.Fatal("unknown scope accepted")
	}
	if f.stage != 0 || f.publish != 0 {
		t.Fatal("device operation before scope negotiation")
	}
}
