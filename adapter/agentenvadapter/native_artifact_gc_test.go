package agentenvadapter

import (
	"encoding/json"
	"errors"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
	"testing"
)

func TestNativeArtifactRetirementRequiresTerminalOwnerAndAllNativePins(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	d, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	receipt := NativeCapture{BarrierID: "barrier", Artifact: "artifact", Digest: core.Hash("native")}
	q := consume.CaptureIntent{Version: consume.CaptureVersion, ID: core.Hash(id + "/capture/" + receipt.BarrierID), OwnerUID: d.OwnerUID, RunUID: d.RunUID, Operation: id + "op", BarrierID: receipt.BarrierID, NativeArtifact: receipt.Artifact, NativeDigest: receipt.Digest, Classes: map[string]string{"binding": "retain"}}
	native := "native-artifact:" + core.Hash(q.OwnerUID+"/"+q.RunUID+"/"+q.NativeArtifact+"/"+q.NativeDigest)
	if err = c.Ledger.BeginCheckpoint(ctx, q.ID, q.OwnerUID, q); err != nil {
		t.Fatal(err)
	}
	if err = c.Ledger.PinCheckpoint(ctx, q.ID, native); err != nil {
		t.Fatal(err)
	}
	if err = c.Ledger.CommitCheckpoint(ctx, q.ID, consume.CaptureManifest{Intent: q, NativeResourceUID: native}); err != nil {
		t.Fatal(err)
	}
	if err = c.Ledger.RetainCheckpoint(ctx, q.ID, q.ID+"child", "child"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.RetireNativeCapture(ctx, id, d.Candidate.ID, receipt.BarrierID); !errors.Is(err, core.ErrPending) {
		t.Fatalf("active owner artifact retired: %v", err)
	}
	if _, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Released' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err = c.RetireNativeCapture(ctx, id, d.Candidate.ID, receipt.BarrierID); !errors.Is(err, core.ErrPending) {
		t.Fatalf("child native artifact pin ignored: %v", err)
	}
	if err = c.Ledger.ReleaseCheckpoint(ctx, q.ID+"child"); err != nil {
		t.Fatal(err)
	}
	if collectible, err := c.RetireNativeCapture(ctx, id, d.Candidate.ID, receipt.BarrierID); err != nil || !collectible {
		t.Fatalf("retirement failed: %v %v", collectible, err)
	}
	// A delayed capture RPC cannot reopen the retired barrier.
	if _, err = c.Capture(ctx, id, d.Candidate.ID, receipt, "late"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("retired barrier reopened: %v", err)
	}
	data, _ := json.Marshal(map[string]any{"request_id": id, "capture": receipt})
	reply, err := c.Command(ctx, NativeCommand{Action: "capture.retire", NodeID: d.Candidate.ID, OwnerID: d.OwnerUID, RunID: d.RunUID, Data: data}, nil, nil)
	if err != nil || reply.Data.(map[string]bool)["collectible"] != true {
		t.Fatalf("retirement replay: %+v %v", reply, err)
	}
}
