package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestNativeDrainRevokesUnassociatedDispatch(t *testing.T) {
	journal, err := OpenJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	_, err = journal.Update("request", func(record *NodeRecord) error {
		*record = NodeRecord{Request: StartRequest{ID: "request", Instance: "run"}, Native: &NativeDispatch{RunUID: "run"}, State: "Prepared", Dispatched: true, BootID: "boot", NodeUID: "uid", PIDNamespace: "pid"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	node := Node{Journal: journal, BootID: "boot", PIDNamespace: "pid", Config: Config{NativeMode: true, NodeUID: "uid", RuntimeRoot: t.TempDir()}}
	calls := 0
	node.Control = func(_ context.Context, command NativeCommand) (json.RawMessage, error) {
		if command.Action != "stopped" {
			t.Errorf("unexpected drain callback: %s", command.Action)
		}
		calls++
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = node.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := journal.Get("request")
	if err != nil || record.State != "Released" || record.Dispatched || calls != 1 {
		t.Fatalf("revocation did not finish: %+v %v calls=%d", record, err, calls)
	}
	if _, err = node.prepareNative(ctx, nativeAdmitRequest{}); err == nil {
		t.Fatal("draining node admitted another launch")
	}
}
func TestNativeDrainTimeoutDoesNotInventStopEvidence(t *testing.T) {
	journal, err := OpenJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	_, err = journal.Update("uncertain", func(record *NodeRecord) error {
		*record = NodeRecord{Request: StartRequest{ID: "uncertain", Instance: "run"}, Native: &NativeDispatch{RunUID: "run"}, State: "Ready", Dispatched: true, Runtime: RuntimeEvent{InstanceID: "associated"}, BootID: "boot", NodeUID: "uid", PIDNamespace: "pid"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	node := Node{Journal: journal, BootID: "boot", PIDNamespace: "pid", Config: Config{NativeMode: true, NodeUID: "uid", RuntimeRoot: t.TempDir()}}
	node.Control = func(context.Context, NativeCommand) (json.RawMessage, error) {
		t.Error("uncertain runtime emitted trusted callback")
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err = node.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ambiguous VMM did not retain resources: %v", err)
	}
	record, err := journal.Get("uncertain")
	if err != nil || record.State == "Released" {
		t.Fatalf("timeout released unknown run: %+v %v", record, err)
	}
}
