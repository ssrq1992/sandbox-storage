package agentenvadapter

import (
	"context"
	"encoding/json"
	"net/http"

	"ssf.dev/ssf/core"
)

// Compute admits native builder/recorder VMMs using the same durable capacity
// reservation as public launches. It does not create a sandbox or choose an ID.
func (c *NativeControl) Compute(ctx context.Context, node, nativeID string, resources Resources, checkpoint json.RawMessage, candidates []NativeCandidate, choose func([]NativeCandidate) (NativeCandidate, error)) (NativeDispatch, error) {
	if !safeID.MatchString(node) || !safeID.MatchString(nativeID) || resources.CPU <= 0 || resources.MemoryMiB < 128 {
		return NativeDispatch{}, core.ErrConflict
	}
	var source struct {
		Reference string `json:"reference"`
		Intent    struct {
			ID string `json:"id"`
		} `json:"intent"`
	}
	if len(checkpoint) > 0 && string(checkpoint) != "null" {
		if err := json.Unmarshal(checkpoint, &source); err != nil {
			return NativeDispatch{}, err
		}
	}
	if source.Reference == "" {
		source.Reference = source.Intent.ID
	}
	body, err := json.Marshal(map[string]any{"cpuCount": resources.CPU, "memoryMB": resources.MemoryMiB})
	if err != nil {
		return NativeDispatch{}, err
	}
	id := core.Hash(c.Principal.Tenant + "/compute/" + node + "/" + nativeID)
	return c.Prepare(ctx, NativeIntent{RequestID: id, Tenant: c.Principal.Tenant, Path: "/internal/compute", Body: body, BodyDigest: core.Hash(string(body)), FixedNode: node, ExpectedNativeID: nativeID, ComputeCheckpoint: source.Reference}, candidates, choose)
}

func (n *Node) nativeComputeAdmit(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
		NativeID   string          `json:"native_id"`
		Resources  Resources       `json:"resources"`
	}
	if err := decode(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	if n.Control == nil || n.isDraining() {
		problem(w, 503, core.ErrPending)
		return
	}
	if !safeID.MatchString(q.NativeID) || q.Resources.CPU <= 0 || q.Resources.MemoryMiB < 128 {
		problem(w, 400, core.ErrConflict)
		return
	}
	data, _ := json.Marshal(q)
	result, err := n.Control(r.Context(), NativeCommand{Action: "compute", NodeID: n.Config.NativeNodeID, Data: data})
	if err != nil {
		problem(w, 409, err)
		return
	}
	var request nativeAdmitRequest
	if err = json.Unmarshal(result, &request); err != nil {
		problem(w, 502, err)
		return
	}
	reply, err := n.prepareNative(r.Context(), request)
	if err != nil {
		problem(w, 409, err)
		return
	}
	respond(w, 200, reply)
}
