package agentenvadapter

import "ssf.dev/ssf/core"

// NativeCapacity is one atomic native metadata projection. Heartbeat routing
// IDs are intentionally not used to discount committed compute reservations.
type NativeCapacity struct {
	Version    int       `json:"version"`
	NodeID     string    `json:"node_id"`
	Capacity   Resources `json:"capacity"`
	Allocated  Resources `json:"allocated"`
	SandboxIDs []string  `json:"sandbox_ids"`
}

func applyNativeCapacity(candidate NativeCandidate, projection NativeCapacity) (NativeCandidate, error) {
	if projection.Version != 1 || projection.NodeID != candidate.ID || projection.Capacity.CPU <= 0 || projection.Capacity.MemoryMiB < 128 || projection.Allocated.CPU < 0 || projection.Allocated.MemoryMiB < 0 || projection.SandboxIDs == nil {
		return candidate, core.ErrConflict
	}
	seen := map[string]bool{}
	for _, id := range projection.SandboxIDs {
		if !safeID.MatchString(id) || seen[id] {
			return candidate, core.ErrConflict
		}
		seen[id] = true
	}
	candidate.Capacity = projection.Capacity
	candidate.Allocated = projection.Allocated
	candidate.SandboxIDs = append([]string{}, projection.SandboxIDs...)
	return candidate, nil
}
