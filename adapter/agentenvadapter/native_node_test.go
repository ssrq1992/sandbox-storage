package agentenvadapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

func TestNativeComputeAdmissionDoesNotLaunchVMM(t *testing.T) {
	journal, err := OpenJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	key := bytes.Repeat([]byte("k"), 32)
	keyFile := filepath.Join(t.TempDir(), "dispatch-key")
	if err = os.WriteFile(keyFile, key, 0600); err != nil {
		t.Fatal(err)
	}
	n := &Node{Journal: journal, BootID: "boot", PIDNamespace: "pidns", Config: Config{NativeMode: true, NativeNodeID: "pod", NodeName: "node", NodeUID: "uid", DispatchKeyFile: keyFile, RuntimeRoot: t.TempDir()}}
	d := NativeDispatch{ExpectedNativeID: "native-sandbox", RequestID: "request-1", OwnerUID: "owner", RunUID: "run", BodyDigest: "body", Resources: Resources{CPU: 2, MemoryMiB: 512}, Candidate: NativeCandidate{ID: "pod", KubeNode: "node", NodeUID: "uid", BootID: "boot"}}
	raw, err := auth.SignJSONEnvelope(key, "agentenv.storage.v2", "scheduler", d.RequestID, d, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	q := nativeAdmitRequest{Context: base64.RawURLEncoding.EncodeToString(raw), BodyDigest: "body"}
	// No native HTTP client and no SSF client are supplied: compute admission
	// must only persist the plan and let the original Orchestrator launch.
	first, err := n.prepareNative(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	second, err := n.prepareNative(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if first["ticket"] != second["ticket"] {
		t.Fatal("retry issued a second launch ticket")
	}
	record, err := journal.Get(d.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Dispatched || record.Runtime.InstanceID != "" {
		t.Fatalf("unexpected launch state: %+v", record)
	}
	// Exercise the actual loopback wire shape consumed by Rust Vec<T>. A nil
	// Go slice must not become JSON null on the no-SSF-volume path.
	admitJSON, _ := json.Marshal(first)
	var admitWire map[string]json.RawMessage
	_ = json.Unmarshal(admitJSON, &admitWire)
	if string(admitWire["managed_mounts"]) != "[]" {
		t.Fatalf("null managed mounts: %s", admitJSON)
	}
	n.Config.HookTokenFile = keyFile
	if err = n.acceptEvent(RuntimeEvent{Sequence: 1, Ticket: record.Ticket, InstanceID: "native-instance", SandboxID: "wrong-native-id", Event: "prepare", BootID: "boot"}); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("accepted wrong child native ID: %v", err)
	}
	event, _ := json.Marshal(RuntimeEvent{Sequence: 1, Ticket: record.Ticket, InstanceID: "native-instance", SandboxID: "native-sandbox", Event: "prepare", BootID: "boot", Devices: []Device{}})
	request := httptest.NewRequest("POST", "/storage/prepare", bytes.NewReader(event))
	request.Header.Set("Authorization", "Bearer "+string(key))
	response := httptest.NewRecorder()
	n.Hooks(response, request)
	if response.Code != 200 {
		t.Fatalf("compute prepare: %d %s", response.Code, response.Body.String())
	}
	var wire map[string]json.RawMessage
	_ = json.Unmarshal(response.Body.Bytes(), &wire)
	if string(wire["devices"]) != "[]" {
		t.Fatalf("Rust Vec devices cannot deserialize null: %s", response.Body.String())
	}
	q.BodyDigest = "changed"
	if _, err = n.prepareNative(context.Background(), q); err == nil {
		t.Fatal("accepted changed public body")
	}
	q.BodyDigest = "body"
	if _, err = journal.Update(d.RequestID, func(r *NodeRecord) error { r.Stop = true; r.State = "Released"; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = n.prepareNative(context.Background(), q); err == nil {
		t.Fatal("replayed released launch")
	}
}

func TestNativeDeleteOutboxRetriesAfterControlFailure(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	id := "delete-request"
	_, err = journal.Update(id, func(r *NodeRecord) error {
		*r = NodeRecord{Request: StartRequest{ID: id}, State: "Released", Native: &NativeDispatch{OwnerUID: "owner", RunUID: "run"}, DeleteRequested: true}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{Journal: journal, Config: Config{NativeMode: true, NativeNodeID: "node"}}
	n.Control = func(context.Context, NativeCommand) (json.RawMessage, error) {
		return nil, errors.New("injected unavailable")
	}
	if err = n.syncNativeControl(context.Background(), id); err == nil {
		t.Fatal("lost control failure")
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	n.Journal = journal
	pending, err := journal.Pending()
	if err != nil || len(pending) != 1 || pending[0] != id {
		t.Fatalf("delete task lost across restart: %v %v", pending, err)
	}
	calls := 0
	n.Control = func(_ context.Context, c NativeCommand) (json.RawMessage, error) {
		calls++
		if c.Action != "delete" {
			t.Fatalf("unexpected action: %s", c.Action)
		}
		return json.RawMessage(`{"accepted":true}`), nil
	}
	for i := 0; i < 2; i++ {
		if err = n.syncNativeControl(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("acknowledged delete sent %d times", calls)
	}
	pending, err = journal.Pending()
	if err != nil || len(pending) != 0 {
		t.Fatalf("acknowledged task still pending: %v %v", pending, err)
	}
}

func TestNativeCancelUnstartedFencesPrepare(t *testing.T) {
	for _, associated := range []bool{false, true} {
		t.Run(fmt.Sprint(associated), func(t *testing.T) {
			journal, err := OpenJournal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			n := &Node{Journal: journal, BootID: "boot", PIDNamespace: "pidns", Config: Config{NativeMode: true, NativeNodeID: "node", NodeUID: "uid", RuntimeRoot: t.TempDir()}}
			_, err = journal.Update("request", func(r *NodeRecord) error {
				*r = NodeRecord{Request: StartRequest{ID: "request", Instance: "run"}, Ticket: "ticket", State: "Launching", Dispatched: true, BootID: "boot", NodeUID: "uid", PIDNamespace: "pidns", Native: &NativeDispatch{OwnerUID: "owner", RunUID: "run"}}
				if associated {
					r.Runtime = RuntimeEvent{InstanceID: "instance", SandboxID: "sandbox", Event: "prepare", Sequence: 1}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			n.Control = func(_ context.Context, c NativeCommand) (json.RawMessage, error) {
				calls++
				record, err := journal.Get("request")
				if err != nil {
					return nil, err
				}
				if c.Action != "stopped" {
					t.Fatalf("unexpected action %s", c.Action)
				}
				if err = n.verifyRuntime(record, "detached"); err != nil {
					t.Fatalf("no trusted non-dispatch evidence: %v", err)
				}
				return json.RawMessage(`{}`), nil
			}
			request := httptest.NewRequest("POST", "/storage/control", bytes.NewBufferString(`{"ticket":"ticket","action":"cancel_unstarted"}`))
			response := httptest.NewRecorder()
			n.nativeLifecycle(response, request)
			if associated {
				if response.Code != 409 || calls != 0 {
					t.Fatalf("cancelled associated runtime: %d calls=%d", response.Code, calls)
				}
			} else {
				if response.Code != 200 || calls != 1 {
					t.Fatalf("cancel failed: %d %s", response.Code, response.Body.String())
				}
				if err = n.acceptEvent(RuntimeEvent{Ticket: "ticket", InstanceID: "instance", SandboxID: "sandbox", BootID: "boot", Event: "prepare", Sequence: 1}); err == nil {
					t.Fatal("revoked ticket accepted new runtime")
				}
			}
		})
	}
}
