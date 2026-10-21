package agentenvadapter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeCleanupOutboxSurvivesProviderRestart(t *testing.T) {
	journalRoot := t.TempDir()
	journal, err := OpenJournal(journalRoot)
	if err != nil {
		t.Fatal(err)
	}
	_, err = journal.Update("request", func(record *NodeRecord) error {
		*record = NodeRecord{Request: StartRequest{ID: "request"}, State: "Released", Ticket: "ticket", Native: &NativeDispatch{OwnerUID: "owner", RunUID: "run"}}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenJournal(journalRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	root := t.TempDir()
	control := filepath.Join(root, "control")
	if err = os.Mkdir(control, 0700); err != nil {
		t.Fatal(err)
	}
	task := map[string]any{"version": 1, "action": "delete", "ticket": "forged", "request_id": "request", "owner_uid": "owner", "run_uid": "run"}
	path := filepath.Join(control, "request.json")
	write := func() {
		raw, _ := json.Marshal(task)
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	node := Node{Journal: journal, Config: Config{RuntimeRoot: root}}
	if err = node.importNativeCleanup(); err == nil {
		t.Fatal("accepted forged cleanup ticket")
	}
	record, err := journal.Get("request")
	if err != nil || record.DeleteRequested {
		t.Fatalf("forged task changed durable state: %+v %v", record, err)
	}
	task["ticket"] = "ticket"
	write()
	if err = node.importNativeCleanup(); err != nil {
		t.Fatal(err)
	}
	record, err = journal.Get("request")
	if err != nil || !record.DeleteRequested || record.Stop {
		t.Fatalf("task must set intent without inventing stop: %+v %v", record, err)
	}
	pending, err := journal.Pending()
	if err != nil || len(pending) != 1 || pending[0] != "request" {
		t.Fatalf("released record's cleanup lost: %v %v", pending, err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("durably imported task not acknowledged: %v", err)
	}
	if err = node.importNativeCleanup(); err != nil {
		t.Fatal(err)
	}
}
