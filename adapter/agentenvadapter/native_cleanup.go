package agentenvadapter

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"ssf.dev/ssf/core"
)

// importNativeCleanup reads the runtime's durable local outbox. A task is not
// process-exit evidence and never causes detach by itself. Ticket and run must
// match the already admitted node journal entry before deletion intent is set.
func (n *Node) importNativeCleanup() error {
	root := filepath.Join(n.Config.RuntimeRoot, "control")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var failures []error
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		if !safeID.MatchString(id) {
			failures = append(failures, core.ErrConflict)
			continue
		}
		path := filepath.Join(root, entry.Name())
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 65536 {
			failures = append(failures, errors.New("invalid native cleanup outbox file"))
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		var task struct {
			Version   int    `json:"version"`
			Action    string `json:"action"`
			Ticket    string `json:"ticket"`
			RequestID string `json:"request_id"`
			OwnerUID  string `json:"owner_uid"`
			RunUID    string `json:"run_uid"`
		}
		if json.Unmarshal(raw, &task) != nil || task.Version != 1 || task.Action != "delete" || task.RequestID != id {
			failures = append(failures, core.ErrConflict)
			continue
		}
		_, err = n.Journal.Update(id, func(record *NodeRecord) error {
			if record.Native == nil || record.Ticket != task.Ticket || record.Native.OwnerUID != task.OwnerUID || record.Native.RunUID != task.RunUID {
				return core.ErrConflict
			}
			record.DeleteRequested = true
			return nil
		})
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if err = os.Remove(path); err != nil {
			failures = append(failures, err)
			continue
		}
		directory, err := os.Open(root)
		if err == nil {
			err = directory.Sync()
			_ = directory.Close()
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
