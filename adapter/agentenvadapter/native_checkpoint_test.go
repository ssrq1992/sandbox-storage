package agentenvadapter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
)

func TestCaptureEvidenceCannotSurviveThawOrChangeIdentity(t *testing.T) {
	receipt := NativeCapture{BarrierID: "barrier", Artifact: "artifact", Digest: core.Hash("native capture")}
	if !receipt.valid() {
		t.Fatal("valid capture rejected")
	}
	for _, bad := range []NativeCapture{{BarrierID: "../barrier", Artifact: "artifact", Digest: receipt.Digest}, {BarrierID: "barrier", Artifact: "/host/path", Digest: receipt.Digest}, {BarrierID: "barrier", Artifact: "artifact", Digest: "true"}} {
		if bad.valid() {
			t.Fatal("invalid capture accepted")
		}
	}
	b := core.Binding{ID: "binding", OwnerUID: "owner", RunUID: "run", Epoch: 1, Volume: core.Volume{Driver: "driver", Handle: "handle", PVC: core.PVCRef{UID: "pvc"}}}
	record := NodeRecord{Request: StartRequest{ID: "request", Instance: "run"}, State: "Ready", Ticket: "ticket", BootID: "boot", PIDNamespace: "pidns", Native: &NativeDispatch{OwnerUID: "owner", RunUID: "run", Result: consume.Result{Operation: "operation", Plan: core.Plan{Bindings: []core.Binding{b}}}}, Runtime: RuntimeEvent{Sequence: 4, Ticket: "ticket", InstanceID: "instance", SandboxID: "native", BootID: "boot", Event: "capturing", Capture: &receipt}}
	n := Node{BootID: "boot", PIDNamespace: "pidns", Config: Config{RuntimeRoot: t.TempDir()}}
	q := captureEvidenceRequest{Intent: consume.CaptureIntent{ID: core.Hash("request/capture/barrier"), Operation: "operation", OwnerUID: "owner", RunUID: "run", BarrierID: "barrier", NativeArtifact: "artifact", NativeDigest: receipt.Digest}, Bindings: []core.Binding{b}, Proof: n.captureProof(record, receipt)}
	// There is deliberately no running VMM in this local test: even a complete
	// caller receipt must fail without a matching native outbox/process proof.
	if err := n.verifyCapture(record, q); err == nil {
		t.Fatal("accepted missing native event")
	}
	ev := record.Runtime
	ev.Sequence++
	ev.Event = "thawing"
	ev.Capture = nil
	raw, _ := json.Marshal(ev)
	if err := os.WriteFile(filepath.Join(n.Config.RuntimeRoot, "instance.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := n.verifyCapture(record, q); err == nil {
		t.Fatal("stale capture evidence survived thaw")
	}
	q.Proof = "forged"
	if err := n.verifyCapture(record, q); err == nil {
		t.Fatal("forged barrier proof")
	}
	q.Proof = n.captureProof(record, receipt)
	q.Bindings[0].Epoch++
	if err := n.verifyCapture(record, q); err == nil {
		t.Fatal("stale epoch")
	}
	q.Bindings[0].Epoch--
	record.DeleteRequested = true
	if err := n.verifyCapture(record, q); err == nil {
		t.Fatal("capture after delete intent")
	}
	record.DeleteRequested = false
	n.BootID = "newboot"
	if err := n.verifyCapture(record, q); err == nil {
		t.Fatal("capture after host reboot")
	}
}
