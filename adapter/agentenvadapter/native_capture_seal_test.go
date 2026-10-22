package agentenvadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"ssf.dev/ssf/core"
)

func TestNativeCapturePublicationAfterThawRequiresDurableSeal(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{Journal: journal}
	receipt := NativeCapture{BarrierID: "barrier", Artifact: "artifact", Digest: core.Hash("native")}
	_, err = journal.Update("request", func(r *NodeRecord) error {
		r.Request.ID = "request"
		r.Ticket = "ticket"
		r.Native = &NativeDispatch{OwnerUID: "owner", RunUID: "run"}
		r.Runtime.Capture = &receipt
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ready := false
	calls := 0
	n.Control = func(_ context.Context, q NativeCommand) (json.RawMessage, error) {
		calls++
		if q.Action != "capture.seal" && q.Action != "capture" {
			t.Fatalf("wrong command %s", q.Action)
		}
		return json.Marshal(map[string]bool{"ready": ready})
	}
	call := func(action string, receipt NativeCapture) int {
		body, _ := json.Marshal(map[string]any{"ticket": "ticket", "action": action, "capture": receipt})
		response := httptest.NewRecorder()
		n.nativeLifecycle(response, httptest.NewRequest("POST", "/storage/control", bytes.NewReader(body)))
		return response.Code
	}
	if call("capture.seal", receipt) != 200 {
		t.Fatal("pending seal request failed")
	}
	r, _ := journal.Get("request")
	if len(r.SealedCaptures) != 0 {
		t.Fatal("pending reply authorized thaw")
	}
	ready = true
	if call("capture.seal", receipt) != 200 {
		t.Fatal("seal failed")
	}
	if _, err = journal.Update("request", func(r *NodeRecord) error { r.Runtime.Capture = nil; return nil }); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	n.Journal, err = OpenJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Journal.Close()
	if call("capture", receipt) != 200 {
		t.Fatal("lost durable seal across restart/thaw")
	}
	before := calls
	changed := receipt
	changed.Digest = core.Hash("other content")
	if call("capture", changed) != 409 || calls != before {
		t.Fatal("changed capture reused thaw permission")
	}
	changed = receipt
	changed.BarrierID = "another"
	if call("capture", changed) != 409 || calls != before {
		t.Fatal("new barrier reused thaw permission")
	}
	if _, err = n.Journal.Update("request", func(r *NodeRecord) error { r.Stop = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if call("capture", receipt) != 409 || calls != before {
		t.Fatal("stopped run accepted capture")
	}
}

func TestNativeDirectCaptureRecoveryPublishesOnlyPersistedSeal(t *testing.T) {
	journal, err := OpenJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	barrier := "external_" + core.Hash("task")
	receipt := NativeCapture{BarrierID: barrier, Artifact: "artifact", Digest: core.Hash("native")}
	_, err = journal.Update("request", func(r *NodeRecord) error {
		r.Request.ID = "request"
		r.Ticket = "ticket"
		r.State = "Ready"
		r.Native = &NativeDispatch{OwnerUID: "owner", RunUID: "run"}
		r.Runtime.SandboxID = "native"
		r.SealedCaptures = map[string]string{barrier: core.Digest(receipt)}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{Journal: journal, Config: Config{NativeNodeID: "node"}}
	calls := 0
	n.Control = func(_ context.Context, q NativeCommand) (json.RawMessage, error) {
		calls++
		if q.Action != "capture" || q.RunID != "run" {
			t.Fatal("recovery changed lifecycle")
		}
		return json.RawMessage(`{"ready":true}`), nil
	}
	q := NativeStorageCapture{ID: "task", RequestID: "request", OwnerUID: "owner", RunUID: "run", NodeID: "node", NativeID: "native", Barrier: barrier, Capture: &receipt}
	call := func() int {
		raw, _ := json.Marshal(q)
		w := httptest.NewRecorder()
		n.nativeStorageCapture(w, httptest.NewRequest("POST", "/v1/native/capture", bytes.NewReader(raw)))
		return w.Code
	}
	// Agent is intentionally nil: recovery must not invoke a second native capture.
	if call() != 200 || calls != 1 {
		t.Fatal("sealed publication not recovered")
	}
	receipt.Digest = core.Hash("changed")
	if call() != 409 || calls != 1 {
		t.Fatal("changed receipt accepted")
	}
	receipt.Digest = core.Hash("native")
	if _, err = journal.Update("request", func(r *NodeRecord) error { r.Stop = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if call() != 409 || calls != 1 {
		t.Fatal("stopped VMM accepted recovery")
	}
}
