package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"ssf.dev/ssf/drivers/overlaybd"
	"strings"
	"testing"
)

func TestEvidenceDoesNotExposeCredentialsOrCallARegularFileABlockDevice(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"host/device-operations", "daemon/sessions"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0700); err != nil {
			t.Fatal(err)
		}
	}
	device := filepath.Join(root, "fake-device")
	os.WriteFile(device, []byte("data"), 0600)
	raw := map[string]any{"version": 1, "intent": map[string]string{"operation_id": "op", "repository_id": "repo", "volume_id": "volume"}, "state": "Started", "checkpoints": map[string]any{"device": map[string]any{"device_path": device, "device_identity": "identity", "major": 1, "minor": 2}, "request": map[string]string{"credential": "DO_NOT_EXPORT"}, "head": map[string]any{"volume_id": "volume", "revision": "head", "capacity_generation": 1, "layers": []string{"layer"}}}}
	b, _ := json.Marshal(raw)
	os.WriteFile(filepath.Join(root, "host", "device-operations", "operation.json"), b, 0600)
	handle, _ := overlaybd.VolumeHandle("repo", "volume")
	result, err := collect(root, handle)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sessions) != 1 || result.Sessions[0].LiveBlock || len(result.Heads) != 1 {
		t.Fatal(result)
	}
	output, _ := json.Marshal(result)
	if strings.Contains(string(output), "DO_NOT_EXPORT") {
		t.Fatal("credentials exported")
	}
	other, _ := overlaybd.VolumeHandle("repo", "other")
	empty, err := collect(root, other)
	if err != nil || len(empty.Sessions) != 0 {
		t.Fatal("unrelated volume exported", err)
	}
	os.Symlink(filepath.Join(root, "host", "device-operations", "operation.json"), filepath.Join(root, "host", "device-operations", "link.json"))
	if _, err = collect(root, handle); err == nil {
		t.Fatal("journal symlink accepted")
	}
}
