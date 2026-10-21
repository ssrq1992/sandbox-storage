package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"ssf.dev/ssf/core"
)

func TestNativeCommandRejectsWrongRunBeforeLifecycleMutation(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	node := NativeCandidate{ID: id + "node", KubeNode: "worker", NodeUID: "physical", BootID: "boot", Capacity: Resources{CPU: 2, MemoryMiB: 512}, Defaults: Resources{CPU: 1, MemoryMiB: 256}}
	c.Refresh = func(context.Context, NativeCandidate) (NativeCandidate, error) { return node, nil }
	choose := func(nodes []NativeCandidate) (NativeCandidate, error) { return nodes[0], nil }
	d, err := c.Compute(ctx, node.ID, "builder", node.Defaults, nil, []NativeCandidate{node}, choose)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]string{"request_id": d.RequestID, "native_id": "builder"})
	for _, action := range []string{"ready", "pause", "cancel_pause", "resume", "fork", "delete", "stopped", "capture"} {
		for _, identity := range [][2]string{{"", d.RunUID}, {d.OwnerUID, "wrong-run"}} {
			_, err = c.Command(ctx, NativeCommand{Action: action, NodeID: node.ID, OwnerID: identity[0], RunID: identity[1], Data: data}, []NativeCandidate{node}, choose)
			if !errors.Is(err, core.ErrConflict) {
				t.Fatalf("%s accepted wrong run: %v", action, err)
			}
		}
	}
	var state string
	if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, d.RequestID).Scan(&state); err != nil || state != "Dispatched" {
		t.Fatalf("invalid command mutated state: %s %v", state, err)
	}
}

func TestNativeCleanupStatusProtectsReadinessRacingDelete(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	dispatch, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	invoke := func(ready bool) map[string]bool {
		data, _ := json.Marshal(map[string]any{"request_id": id, "ever_ready": ready})
		out, err := c.Command(ctx, NativeCommand{Action: "cleanup.status", NodeID: dispatch.Candidate.ID, OwnerID: dispatch.OwnerUID, RunID: dispatch.RunUID, Data: data}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return out.Data.(map[string]bool)
	}
	status := invoke(true)
	if status["terminal"] || !status["ever_ready"] || status["ephemeral"] {
		t.Fatalf("invalid live owner status: %+v", status)
	}
	if _, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Released' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	status = invoke(false)
	if !status["terminal"] || !status["ever_ready"] {
		t.Fatalf("readiness race could delete user data: %+v", status)
	}
}
