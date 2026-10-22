package agentenvadapter

import (
	"encoding/json"
	"errors"
	"testing"

	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
)

func TestPostgresDirectCapturePinsRunAndRecoversCompletedBarrier(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	c.Principal.Namespaces = []string{"test"}
	d, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	v := core.Volume{Driver: "overlaybd.csi.ssf.dev", Handle: id, PVC: core.PVCRef{Namespace: "test", Name: "data", UID: id + "pvc"}}
	epoch, err := c.Ledger.RunEpoch(ctx, d.OwnerUID, d.RunUID)
	if err != nil {
		t.Fatal(err)
	}
	b := core.Binding{ID: id + "binding", Volume: v, OwnerUID: d.OwnerUID, RunUID: d.RunUID, Sandbox: d.RunUID, Node: d.Candidate.ID, Epoch: epoch, MountID: "ext_data", GuestPath: "/data"}
	if err = c.Ledger.Register(ctx, v); err != nil {
		t.Fatal(err)
	}
	d.Result.Operation = id + "op"
	d.Result.Plan.Bindings = []core.Binding{b}
	if err = c.Ledger.Acquire(ctx, d.Result.Operation, "input", []core.Binding{b}); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(d)
	if _, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET dispatch=$2,native_id='native' WHERE id=$1`, id, raw); err != nil {
		t.Fatal(err)
	}
	ref := core.DirectSnapshotRef{ID: id + "snapshot", Namespace: "test", Name: "snapshot", SnapshotUID: id + "s", ContentName: "content", ContentUID: id + "c", Volume: v, CreatedUnixNano: 1}
	if err = c.Ledger.TrackDirectSnapshot(ctx, ref); err != nil {
		t.Fatal(err)
	}
	task, err := c.directCaptureTask(ctx, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.RequestID != id || task.BindingID != b.ID || task.RunUID != d.RunUID || task.NativeID != "native" {
		t.Fatal("run identity lost")
	}
	if err = c.AuthorizeDirectCapture(ctx, id, d.Candidate.ID, d.OwnerUID, d.RunUID, task.Barrier); err != nil {
		t.Fatal(err)
	}
	if err = c.AuthorizeDirectCapture(ctx, id, d.Candidate.ID, d.OwnerUID, "wrong-run", task.Barrier); !errors.Is(err, core.ErrConflict) {
		t.Fatal("other run authorized", err)
	}
	checkpoint := core.Hash(id + "/capture/" + task.Barrier)
	intent := consume.CaptureIntent{Version: consume.CaptureVersion, ID: checkpoint, Operation: d.Result.Operation, OwnerUID: d.OwnerUID, RunUID: d.RunUID, BarrierID: task.Barrier, NativeArtifact: "artifact", NativeDigest: core.Hash("artifact"), Classes: map[string]string{b.ID: "snapshot-class"}}
	if err = c.Ledger.BeginCheckpoint(ctx, checkpoint, d.OwnerUID, intent); err != nil {
		t.Fatal(err)
	}
	if err = c.Ledger.PinCheckpoint(ctx, checkpoint, v.PVC.UID, "artifact:"+id); err != nil {
		t.Fatal(err)
	}
	capture := core.SnapshotCapture{CheckpointID: checkpoint, BindingID: b.ID, Driver: v.Driver, Handle: v.Handle, PVC: v.PVC, SnapshotName: "ssf-cp-" + core.Hash(checkpoint + "/" + b.ID)[:40], CaptureID: core.Hash(checkpoint + "/" + b.ID), CaptureDigest: core.Hash("sealed"), HeadRevision: core.Hash("published"), CapacityBytes: 1 << 30, FilesystemUUID: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}
	if err = c.Ledger.RecordSnapshotCapture(ctx, intent, b, capture); err != nil {
		t.Fatal(err)
	}
	if _, err = c.completedDirectCapture(ctx, task); !errors.Is(err, core.ErrPending) {
		t.Fatal("partial barrier exposed", err)
	}
	if err = c.Ledger.CommitCheckpoint(ctx, checkpoint, consume.CaptureManifest{Intent: intent, NativeResourceUID: "artifact:" + id}); err != nil {
		t.Fatal(err)
	}
	// A lost reply can arrive after the source run exits. Reuse the original
	// completed barrier, without a second RPC or selecting a newer idle head.
	if _, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Released' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	replay, err := c.directCaptureTask(ctx, ref.ID)
	if err != nil || core.Digest(replay) != core.Digest(task) {
		t.Fatal("task retargeted", err)
	}
	got, err := c.DirectCapture(ctx, ref.ID)
	if err != nil || core.Digest(got) != core.Digest(capture) {
		t.Fatal("response loss not recovered", err)
	}
	if err = c.Ledger.ReleaseCheckpoint(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err = c.Ledger.PermitSnapshotDelete(ctx, "artifact:"+id); err == nil {
		t.Fatal("direct reference lost artifact")
	}
	if err = c.Ledger.RetireDirectSnapshot(ctx, ref.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = c.DirectCapture(ctx, ref.ID); err == nil {
		t.Fatal("retired task executed")
	}
	if err = c.Ledger.PermitSnapshotDelete(ctx, "artifact:"+id); err != nil {
		t.Fatal(err)
	}
}
