package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"ssf.dev/ssf/core"
)

func TestNativeForkUsesIndependentAdmissionAndOriginalChildID(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	parent, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	node := NativeCandidate{ID: id + "node", KubeNode: "worker", NodeUID: "physical", BootID: "boot", Capacity: Resources{CPU: 2, MemoryMiB: 512}, Allocated: Resources{CPU: 1, MemoryMiB: 256}, Defaults: Resources{CPU: 1, MemoryMiB: 256}, SandboxIDs: []string{"parent-native"}}
	parent.Resources = node.Defaults
	parent.Candidate = node
	raw, _ := json.Marshal(parent)
	if _, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET dispatch=$2,native_id='parent-native' WHERE id=$1`, id, raw); err != nil {
		t.Fatal(err)
	}
	c.Refresh = func(context.Context, NativeCandidate) (NativeCandidate, error) { return node, nil }
	choose := func(nodes []NativeCandidate) (NativeCandidate, error) { return nodes[0], nil }
	child, err := c.Fork(ctx, id, node.ID, "native-child-1", nil, []NativeCandidate{node}, choose)
	if err != nil {
		t.Fatal(err)
	}
	if child.OwnerUID == parent.OwnerUID || child.RunUID == parent.RunUID || child.ExpectedNativeID != "native-child-1" || child.Candidate.ID != node.ID {
		t.Fatalf("fork reused parent identity or placement: %+v", child)
	}
	replay, err := c.Fork(ctx, id, node.ID, "native-child-1", nil, []NativeCandidate{node}, choose)
	if err != nil || core.Digest(replay) != core.Digest(child) {
		t.Fatalf("fork replay changed child: %+v %v", replay, err)
	}
	if _, err = c.Fork(ctx, id, node.ID, "native-child-2", nil, []NativeCandidate{node}, choose); err == nil {
		t.Fatal("fork bypassed persistent compute reservation")
	}
	if err = c.Ready(ctx, child.RequestID, node.ID, "different-native-id", "proof"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("fork changed original native child ID: %v", err)
	}
	if _, err = c.Fork(ctx, id, "other-node", "native-child-3", nil, []NativeCandidate{node}, choose); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("fork moved away from parent: %v", err)
	}
	if _, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Paused' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Fork(ctx, id, node.ID, "native-child-4", nil, []NativeCandidate{node}, choose); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("fork admitted a non-running source: %v", err)
	}
}
