package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestMaterializeSelectsExactNodeWithoutInventingUID(t *testing.T) {
	source := []byte(`{"nodes":{"worker-a":{"native":{"node_id":"worker-a"},"node":{"role":"node","node":"worker-a","csiNode":"worker-a"},"worker":{"role":"worker"}}}}`)
	root := filepath.Join(t.TempDir(), "private")
	if err := materialize(bytes.NewReader(source), "worker-a", root); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "node.json"))
	if err != nil {
		t.Fatal(err)
	}
	var node map[string]any
	if err = json.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	if node["nodeUID"] != nil {
		t.Fatal("invented a Kubernetes Node UID")
	}
	info, _ := os.Stat(filepath.Join(root, "node.json"))
	if info.Mode().Perm() != 0600 {
		t.Fatal("nonprivate output")
	}
	if err := materialize(bytes.NewReader(source), "worker-b", root); err == nil {
		t.Fatal("missing worker inventory accepted")
	}
	after, _ := os.ReadFile(filepath.Join(root, "node.json"))
	if !bytes.Equal(raw, after) {
		t.Fatal("failed selection changed prior configuration")
	}
	for _, bad := range [][]byte{append(append([]byte{}, source...), []byte(` {}`)...), bytes.Replace(source, []byte(`"node":"worker-a"`), []byte(`"node":"worker-b"`), 1)} {
		if err := materialize(bytes.NewReader(bad), "worker-a", root); err == nil {
			t.Fatal("mismatched or trailing inventory accepted")
		}
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err = os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if err = materialize(bytes.NewReader(source), "worker-a", alias); err == nil {
		t.Fatal("symlink output accepted")
	}
}
