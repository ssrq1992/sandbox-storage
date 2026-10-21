package agentenvadapter

import (
	"context"
	"errors"
	"testing"

	"ssf.dev/ssf/core"
)

func TestNativeBackgroundComputeUsesSharedReservationAndFixedIdentity(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	node := NativeCandidate{ID: id + "node", KubeNode: "worker", NodeUID: "physical", BootID: "boot", Capacity: Resources{CPU: 2, MemoryMiB: 512}, Defaults: Resources{CPU: 1, MemoryMiB: 256}}
	c.Refresh = func(context.Context, NativeCandidate) (NativeCandidate, error) { return node, nil }
	choose := func(nodes []NativeCandidate) (NativeCandidate, error) { return nodes[0], nil }
	first, err := c.Compute(ctx, node.ID, "builder-one", node.Defaults, nil, []NativeCandidate{node}, choose)
	if err != nil {
		t.Fatal(err)
	}
	if first.ExpectedNativeID != "builder-one" || first.Candidate.ID != node.ID || len(first.ManagedMounts) != 0 {
		t.Fatalf("invalid builder dispatch: %+v", first)
	}
	replay, err := c.Compute(ctx, node.ID, "builder-one", node.Defaults, nil, []NativeCandidate{node}, choose)
	if err != nil || core.Digest(first) != core.Digest(replay) {
		t.Fatalf("builder replay changed: %v", err)
	}
	if _, err = c.Compute(ctx, node.ID, "builder-one", Resources{CPU: 2, MemoryMiB: 256}, nil, []NativeCandidate{node}, choose); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("builder changed resources: %v", err)
	}
	second, err := c.Compute(ctx, node.ID, "recorder-one", node.Defaults, nil, []NativeCandidate{node}, choose)
	if err != nil {
		t.Fatal(err)
	}
	if second.RunUID == first.RunUID || second.OwnerUID == first.OwnerUID {
		t.Fatal("recorder reused builder writer identity")
	}
	if _, err = c.Compute(ctx, node.ID, "builder-two", node.Defaults, nil, []NativeCandidate{node}, choose); err == nil {
		t.Fatal("background VMM exceeded shared compute capacity")
	}
	if _, err = c.Compute(ctx, node.ID, "invalid", Resources{}, nil, []NativeCandidate{node}, choose); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("unspecified background VM shape: %v", err)
	}
	if err = c.Ready(ctx, first.RequestID, node.ID, "another-builder", "proof"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("background native ID changed: %v", err)
	}
}
