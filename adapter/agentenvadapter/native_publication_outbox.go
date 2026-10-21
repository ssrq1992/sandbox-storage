package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"ssf.dev/ssf/core"
)

// Repository completion tasks survive either process restarting. The node
// independently reads native repository state; the file alone proves nothing.
func (n *Node) importNativePublications(ctx context.Context) error {
	n.publicationMu.Lock()
	defer n.publicationMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	root := filepath.Join(n.Config.RuntimeRoot, "publications")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var failures []error
	if len(entries) == 0 {
		n.publicationOffset = 0
		return nil
	}
	start := n.publicationOffset % len(entries)
	for i := 0; i < len(entries); i++ {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		index := (start + i) % len(entries)
		n.publicationOffset = (index + 1) % len(entries)
		entry := entries[index]
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		err = func() error {
			info, err := entry.Info()
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Size() > 65536 {
				return core.ErrConflict
			}
			path := filepath.Join(root, entry.Name())
			raw, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			var task struct {
				RepositoryID string `json:"repository_id"`
				Version      int    `json:"version"`
				Action       string `json:"action"`
				NativeID     string `json:"native_id"`
			}
			if err = json.Unmarshal(raw, &task); err != nil {
				return err
			}
			if task.Version != 1 || len(task.RepositoryID) != 64 || !isHex(task.RepositoryID) || !safeID.MatchString(task.NativeID) || entry.Name() != task.RepositoryID+"_"+task.NativeID+"-"+task.Action+".json" || (task.Action != "published" && task.Action != "deleted") {
				return core.ErrConflict
			}
			if n.Agent == nil || n.Control == nil {
				return errors.New("native repository verification unavailable")
			}
			var observation struct {
				RepositoryID string `json:"repository_id"`
				ID           string `json:"id"`
				Exists       *bool  `json:"exists"`
				Committed    *bool  `json:"committed"`
			}
			if err = n.Agent.Call(ctx, "GET", base(n.Config.AgentURL)+"/internal/storage/checkpoints/"+task.NativeID, nil, &observation); err != nil {
				return err
			}
			if observation.RepositoryID != task.RepositoryID || observation.ID != task.NativeID || observation.Exists == nil || observation.Committed == nil {
				return core.ErrConflict
			}
			if task.Action == "published" && (!*observation.Exists || !*observation.Committed) || task.Action == "deleted" && *observation.Exists {
				return core.ErrPending
			}
			data, _ := json.Marshal(map[string]string{"native_id": task.NativeID, "repository_id": task.RepositoryID})
			if _, err = n.Control(ctx, NativeCommand{Action: "checkpoint." + task.Action, NodeID: n.Config.NativeNodeID, Data: data}); err != nil {
				return err
			}
			if task.Action == "deleted" {
				if err = os.Remove(filepath.Join(root, task.RepositoryID+"_"+task.NativeID+"-published.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			if err = os.Remove(path); err != nil {
				return err
			}
			directory, err := os.Open(root)
			if err != nil {
				return err
			}
			defer directory.Close()
			return directory.Sync()
		}()
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
