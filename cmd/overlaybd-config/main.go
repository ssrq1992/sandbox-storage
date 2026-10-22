// overlaybd-config materializes an administrator's per-node configuration in a
// private init-container volume. It does not discover storage or issue grants.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var nodeName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,251}[a-z0-9]$`)

type nodeConfig struct {
	Native json.RawMessage `json:"native"`
	Node   json.RawMessage `json:"node"`
	Worker json.RawMessage `json:"worker"`
}
type inventory struct {
	Nodes map[string]nodeConfig `json:"nodes"`
}

func materialize(source io.Reader, name, root string) error {
	if len(name) == 0 || (len(name) != 1 && !nodeName.MatchString(name)) || (len(name) == 1 && (name[0] < 'a' || name[0] > 'z') && (name[0] < '0' || name[0] > '9')) {
		return errors.New("trusted Kubernetes node name required")
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return errors.New("canonical absolute output directory required")
	}
	var input inventory
	d := json.NewDecoder(io.LimitReader(source, 4<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&input); err != nil {
		return err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return errors.New("trailing inventory content")
	}
	config, ok := input.Nodes[name]
	if !ok {
		return errors.New("node absent from administrator inventory")
	}
	files := map[string]json.RawMessage{"native.json": config.Native, "node.json": config.Node, "worker.json": config.Worker}
	for file, raw := range files {
		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
			return fmt.Errorf("invalid %s", file)
		}
		switch file {
		case "native.json":
			if obj["node_id"] != name {
				return errors.New("native node identity mismatch")
			}
		case "node.json":
			if obj["node"] != name || obj["csiNode"] != name || obj["role"] != "node" {
				return errors.New("CSI node identity mismatch")
			}
		case "worker.json":
			if obj["role"] != "worker" {
				return errors.New("invalid worker role")
			}
		}
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("private non-symlink output directory required")
	}
	for name, raw := range files {
		file, err := os.CreateTemp(root, ".config-")
		if err != nil {
			return err
		}
		temporary := file.Name()
		err = func() error {
			defer file.Close()
			if _, err := file.Write(raw); err != nil {
				return err
			}
			if err := file.Sync(); err != nil {
				return err
			}
			return os.Rename(temporary, filepath.Join(root, name))
		}()
		if err != nil {
			os.Remove(temporary)
			return err
		}
	}
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func main() {
	source := flag.String("inventory", "", "administrator inventory JSON")
	root := flag.String("output", "/run/ssf-overlaybd-config/private", "private init output")
	flag.Parse()
	f, err := os.Open(*source)
	if err == nil {
		defer f.Close()
		err = materialize(f, os.Getenv("SSF_NODE_NAME"), *root)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
