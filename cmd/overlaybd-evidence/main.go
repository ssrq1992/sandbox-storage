// Read-only evidence for real-device validation. This is never an authority
// for mount, recovery, deletion, or bypassing the shared usage ledger.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
	"ssf.dev/ssf/drivers/overlaybd"
)

type evidence struct {
	Scope    string    `json:"scope"`
	Handle   string    `json:"handle"`
	Sessions []session `json:"sessions"`
	Heads    []head    `json:"heads"`
}
type session struct {
	Operation string `json:"operation"`
	State     string `json:"state"`
	Device    string `json:"device"`
	Identity  string `json:"identity"`
	LiveBlock bool   `json:"live_block"`
	Head      string `json:"head"`
}
type head struct {
	Revision       string `json:"revision"`
	Generation     uint64 `json:"generation"`
	Layers         int    `json:"layers"`
	FilesystemUUID string `json:"filesystem_uuid"`
	Capacity       uint64 `json:"capacity_bytes"`
}
type operationRecord struct {
	Version int `json:"version"`
	Intent  struct {
		Operation  string `json:"operation_id"`
		Repository string `json:"repository_id"`
		Volume     string `json:"volume_id"`
	} `json:"intent"`
	State       string                     `json:"state"`
	Checkpoints map[string]json.RawMessage `json:"checkpoints"`
}

func collect(root, handle string) (evidence, error) {
	out := evidence{Scope: "read-only-native-journal-and-device-observation", Handle: handle, Sessions: []session{}, Heads: []head{}}
	repo, volume, err := overlaybd.ParseVolumeHandle(handle)
	if err != nil {
		return out, err
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return out, fmt.Errorf("absolute canonical state root required")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return out, err
	}
	if !info.IsDir() {
		return out, fmt.Errorf("state root must be a real directory")
	}
	for _, component := range []string{"daemon/sessions", "host/device-operations", "host/volume-operations"} {
		err = filepath.WalkDir(filepath.Join(root, component), func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				if path == filepath.Join(root, component) && os.IsNotExist(walkErr) {
					return nil
				}
				return walkErr
			}
			if d.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink in native journal tree")
			}
			if d.IsDir() {
				return nil
			}
			if !strings.HasSuffix(d.Name(), ".json") {
				return nil
			}
			f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			st, err := f.Stat()
			if err != nil {
				return err
			}
			if !st.Mode().IsRegular() || st.Size() > 8<<20 {
				return fmt.Errorf("invalid native journal file")
			}
			var record operationRecord
			if err = json.NewDecoder(io.LimitReader(f, 8<<20)).Decode(&record); err != nil {
				return err
			}
			if record.Version != 1 || record.Intent.Repository != repo || record.Intent.Volume != volume {
				return nil
			}
			var plan struct {
				Head string `json:"head_revision"`
			}
			for _, key := range []string{"plan", "daemon_plan"} {
				if raw, ok := record.Checkpoints[key]; ok {
					if err := json.Unmarshal(raw, &plan); err != nil {
						return err
					}
				}
			}
			for key, raw := range record.Checkpoints {
				if key == "device" || key == "prepared" {
					var device struct {
						Path     string `json:"device_path"`
						Identity string `json:"device_identity"`
						Boot     string `json:"node_boot_id"`
						Major    uint64 `json:"major"`
						Minor    uint64 `json:"minor"`
					}
					if err = json.Unmarshal(raw, &device); err != nil {
						return err
					}
					row := session{Operation: record.Intent.Operation, State: record.State, Device: device.Path, Identity: device.Identity, Head: plan.Head}
					_, releasing := record.Checkpoints["release"]
					_, hostReleasing := record.Checkpoints["release-request"]
					bootValid := runtime.GOOS != "linux"
					if boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id"); err == nil {
						bootValid = strings.TrimSpace(string(boot)) == device.Boot && device.Boot != ""
					}
					if st, err := os.Lstat(device.Path); err == nil && bootValid && !releasing && !hostReleasing && record.State != "Completed" && st.Mode()&os.ModeDevice != 0 && st.Mode()&os.ModeCharDevice == 0 && device.Identity != "" {
						if sys, ok := st.Sys().(*syscall.Stat_t); ok {
							row.LiveBlock = uint64(unix.Major(uint64(sys.Rdev))) == device.Major && uint64(unix.Minor(uint64(sys.Rdev))) == device.Minor
						}
					}
					out.Sessions = append(out.Sessions, row)
				}
				var h struct {
					Revision       string            `json:"revision"`
					Generation     uint64            `json:"capacity_generation"`
					Layers         []json.RawMessage `json:"layers"`
					Volume         string            `json:"volume_id"`
					FilesystemUUID string            `json:"filesystem_uuid"`
					Capacity       uint64            `json:"capacity_bytes"`
				}
				if json.Unmarshal(raw, &h) == nil && h.Volume == volume && h.Revision != "" && h.Generation > 0 && len(h.Layers) > 0 {
					out.Heads = append(out.Heads, head{Revision: h.Revision, Generation: h.Generation, Layers: len(h.Layers), FilesystemUUID: h.FilesystemUUID, Capacity: h.Capacity})
				}
			}
			return nil
		})
		if err != nil {
			return out, err
		}
	}
	return out, nil
}
func main() {
	root := flag.String("state-root", "/var/lib/ssf/overlaybd", "private native state root")
	handle := flag.String("volume-handle", "", "exact CSI volume handle")
	flag.Parse()
	result, err := collect(*root, *handle)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
