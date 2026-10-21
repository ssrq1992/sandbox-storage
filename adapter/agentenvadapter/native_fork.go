package agentenvadapter

import (
	"context"
	"encoding/json"

	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
)

// Fork prepares only a child's storage and compute reservation. The original
// native backend captures memory, chooses child IDs and starts every VMM.
func (c *NativeControl) Fork(ctx context.Context, parent, node, child string, checkpoint json.RawMessage, candidates []NativeCandidate, choose func([]NativeCandidate) (NativeCandidate, error)) (NativeDispatch, error) {
	var out NativeDispatch
	if !safeID.MatchString(child) {
		return out, core.ErrConflict
	}
	err := c.Ledger.Exclusive(ctx, "agentenv/native-run/"+parent, func(ctx context.Context) error {
		prior, err := c.Dispatch(ctx, parent)
		if err != nil {
			return err
		}
		if prior.Candidate.ID != node {
			return core.ErrConflict
		}
		var source struct {
			Reference string                `json:"reference"`
			Intent    consume.CaptureIntent `json:"intent"`
		}
		if len(checkpoint) > 0 && string(checkpoint) != "null" {
			if err = json.Unmarshal(checkpoint, &source); err != nil {
				return err
			}
		}
		ref := source.Reference
		if ref == "" {
			ref = source.Intent.ID
		}
		body, err := json.Marshal(map[string]string{"parent": parent, "child": child, "checkpoint": ref})
		if err != nil {
			return err
		}
		request := NativeIntent{RequestID: core.Hash(parent + "/fork/" + child), Tenant: c.Principal.Tenant, Path: "/internal/fork", Body: body, BodyDigest: core.Hash(string(body)), ForkOf: parent, ForkCheckpoint: ref, ExpectedNativeID: child, FixedNode: node}
		out, err = c.Prepare(ctx, request, candidates, choose)
		return err
	})
	return out, err
}
