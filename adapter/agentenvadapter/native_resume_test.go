package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ssf.dev/ssf/core"
)

// This test requires a real PostgreSQL ledger; HTTP is only the trusted node
// fixture. It does not establish Linux/KVM or CSI end-to-end correctness.
func TestNativeResumeUsesOneSuccessorAndDeleteFollowsIt(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	if _, err := c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_runs WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	child := core.Hash(id + "/resume")
	t.Cleanup(func() {
		_, _ = c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_runs WHERE id=$1`, child)
		_, _ = c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_reservations WHERE id=ANY($1::text[])`, []string{"native/" + id, "native/" + child})
	})
	var dispatch NativeDispatch
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/evidence" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/operations/") {
			_ = json.NewEncoder(w).Encode(NodeRecord{State: "Ready", Request: StartRequest{Instance: dispatch.RunUID}, Runtime: RuntimeEvent{SandboxID: "native-id"}})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	c.HTTP = &HTTPClient{Client: server.Client()}
	node := NativeCandidate{ID: "node", KubeNode: "worker", NodeUID: "physical", BootID: "boot", PluginURL: server.URL, Capacity: Resources{CPU: 4, MemoryMiB: 4096}, Defaults: Resources{CPU: 1, MemoryMiB: 256}}
	c.Refresh = func(_ context.Context, _ NativeCandidate) (NativeCandidate, error) { return node, nil }
	choose := func(candidates []NativeCandidate) (NativeCandidate, error) { return candidates[0], nil }
	intent := NativeIntent{RequestID: id, Tenant: "test", Path: "/sandboxes-cold", Body: []byte("{}"), BodyDigest: core.Hash("{}")}
	var err error
	dispatch, err = c.Prepare(ctx, intent, []NativeCandidate{node}, choose)
	if err != nil {
		t.Fatal(err)
	}
	original := dispatch
	if err = c.Ready(ctx, id, "node", "native-id", "ready"); err != nil {
		t.Fatal(err)
	}
	if err = c.RetainPause(ctx, id, "node"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Resume(ctx, id, "node", "native-id", []NativeCandidate{node}, choose); err == nil {
		t.Fatal("resumed before actual stop")
	}
	if err = c.Stopped(ctx, id, "node", "stopped"); err != nil {
		t.Fatal(err)
	}
	dispatch, err = c.Resume(ctx, id, "node", "native-id", []NativeCandidate{node}, choose)
	if err != nil {
		t.Fatal(err)
	}
	if dispatch.OwnerUID != original.OwnerUID || dispatch.RunUID == original.RunUID || dispatch.RequestID != child {
		t.Fatal("invalid owner/run handoff")
	}
	repeated, err := c.Resume(ctx, id, "node", "native-id", []NativeCandidate{node}, choose)
	if err != nil || core.Digest(repeated) != core.Digest(dispatch) {
		t.Fatalf("resume replay created another run: %v", err)
	}
	if err = c.Stopped(ctx, id, "node", "late stop"); err != nil {
		t.Fatal(err)
	}
	if err = c.BeginDelete(ctx, id, "node"); err != nil {
		t.Fatal(err)
	}
	var state string
	if err = c.Ledger.Pool.QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, child).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "Deleting" {
		t.Fatalf("delete lost successor: %s", state)
	}
	if err = c.Stopped(ctx, child, "node", "stopped"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Resume(ctx, id, "node", "native-id", []NativeCandidate{node}, choose); err == nil {
		t.Fatal("deleted successor resurrected")
	}
}

func TestNativePausedResumeOnReplacementPodKeepsPhysicalPlacement(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	old, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	old.Resources = Resources{CPU: 1, MemoryMiB: 256}
	old.Candidate = NativeCandidate{ID: "old-pod", KubeNode: "worker", NodeUID: "physical", BootID: "boot"}
	raw, _ := json.Marshal(old)
	if _, err = c.Ledger.Pool.Exec(ctx, `UPDATE agentenv_native_runs SET dispatch=$2,state='Paused',native_id='native-id' WHERE id=$1`, id, raw); err != nil {
		t.Fatal(err)
	}
	child := core.Hash(id + "/resume")
	t.Cleanup(func() {
		_, _ = c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_runs WHERE id=$1`, child)
		_, _ = c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_reservations WHERE id=$1`, "native/"+child)
	})
	node := NativeCandidate{ID: "new-pod", KubeNode: "worker", NodeUID: "physical", BootID: "boot", Capacity: Resources{CPU: 4, MemoryMiB: 4096}, Defaults: old.Resources}
	c.Refresh = func(context.Context, NativeCandidate) (NativeCandidate, error) { return node, nil }
	choose := func(candidates []NativeCandidate) (NativeCandidate, error) { return candidates[0], nil }
	node.NodeUID = "other-physical-node"
	if _, err = c.Resume(ctx, id, "new-pod", "native-id", []NativeCandidate{node}, choose); err == nil {
		t.Fatal("paused memory migrated across physical nodes")
	}
	node.NodeUID = "physical"
	next, err := c.Resume(ctx, id, "new-pod", "native-id", []NativeCandidate{node}, choose)
	if err != nil {
		t.Fatal(err)
	}
	if next.Candidate.ID != "new-pod" || next.Candidate.NodeUID != old.Candidate.NodeUID || next.OwnerUID != old.OwnerUID {
		t.Fatalf("placement/owner lost: %+v", next)
	}
	if err = c.BeginDelete(ctx, id, "new-pod"); err != nil {
		t.Fatalf("old paused cleanup identity cannot find successor: %v", err)
	}
}

// A logical template mount may reference an internal clone name. Missing that
// clone is an error, never evidence that the logical name belongs to legacy.
func TestNativeResumeMissingMappedCloneNeverFallsBack(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	old, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	old.Resources = Resources{CPU: 1, MemoryMiB: 256}
	old.ManagedMounts = []NativeMount{{Name: "logical", Path: "/data"}}
	old.VolumeNames = map[string]string{"logical": "missing-clone"}
	old.MountIDs = map[string]string{"logical": "ext_data"}
	old.CheckpointReference = "retained-template-reference"
	raw, _ := json.Marshal(old)
	if _, err = c.Ledger.Pool.Exec(ctx, `UPDATE agentenv_native_runs SET dispatch=$2,state='Paused',native_id='native-id' WHERE id=$1`, id, raw); err != nil {
		t.Fatal(err)
	}
	c.Volumes = &NativeVolumes{Ledger: c.Ledger}
	node := NativeCandidate{ID: "node", KubeNode: "worker", NodeUID: "physical", BootID: "boot", Capacity: Resources{CPU: 4, MemoryMiB: 4096}, Defaults: old.Resources}
	c.Refresh = func(context.Context, NativeCandidate) (NativeCandidate, error) { return node, nil }
	_, err = c.Resume(ctx, id, "node", "native-id", []NativeCandidate{node}, func(nodes []NativeCandidate) (NativeCandidate, error) { return nodes[0], nil })
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing clone fell back or changed failure: %v", err)
	}
	var state string
	if err = c.Ledger.Pool.QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "Paused" {
		t.Fatalf("failed clone lookup consumed pause: %s", state)
	}
}
