package agentenvadapter

import (
	"errors"
	"ssf.dev/ssf/core"
	"testing"
)

func TestNativeCapacityDoesNotMixRoutingIDsWithNewResourceTotals(t *testing.T) {
	stale := NativeCandidate{ID: "node", Capacity: Resources{CPU: 99, MemoryMiB: 99999}, SandboxIDs: []string{"dormant-builder"}}
	sample := NativeCapacity{Version: 1, NodeID: "node", Capacity: Resources{CPU: 4, MemoryMiB: 1024}, Allocated: Resources{CPU: 2, MemoryMiB: 512}, SandboxIDs: []string{"running"}}
	fresh, err := applyNativeCapacity(stale, sample)
	if err != nil || fresh.Allocated != sample.Allocated || fresh.Capacity != sample.Capacity || len(fresh.SandboxIDs) != 1 || fresh.SandboxIDs[0] != "running" {
		t.Fatalf("mixed accounting samples: %+v %v", fresh, err)
	}
	for _, mutate := range []func(*NativeCapacity){
		func(p *NativeCapacity) { p.Version = 0 }, func(p *NativeCapacity) { p.NodeID = "replacement" },
		func(p *NativeCapacity) { p.Capacity.CPU = 0 }, func(p *NativeCapacity) { p.Allocated.MemoryMiB = -1 },
		func(p *NativeCapacity) { p.SandboxIDs = nil }, func(p *NativeCapacity) { p.SandboxIDs = []string{"running", "running"} },
	} {
		invalid := sample
		mutate(&invalid)
		if _, err := applyNativeCapacity(stale, invalid); !errors.Is(err, core.ErrConflict) {
			t.Fatalf("invalid projection admitted: %+v %v", invalid, err)
		}
	}
}
