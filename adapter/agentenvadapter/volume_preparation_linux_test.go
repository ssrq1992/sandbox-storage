//go:build linux

package agentenvadapter

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
)

// Opt-in tests allocate their own disposable loop device. No caller-supplied
// device path is accepted, so an existing data disk cannot be formatted.
func preparationLoop(t *testing.T) Device {
	t.Helper()
	if os.Getenv("SSF_PREPARATION_DEVICE_TEST") != "1" {
		t.Skip("requires dedicated Linux root fixture: SSF_PREPARATION_DEVICE_TEST=1")
	}
	if os.Geteuid() != 0 {
		t.Fatal("loop fixture requires root")
	}
	for _, tool := range []string{"losetup", "blkid", "mkfs.ext4", "e2fsck", "debugfs"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(t.TempDir(), "blank.img")
	f, err := os.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(64 << 20); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("losetup", "--find", "--show", file).CombinedOutput()
	if err != nil {
		t.Fatalf("losetup: %v %s", err, output)
	}
	device := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if output, err := exec.Command("losetup", "--detach", device).CombinedOutput(); err != nil {
			t.Errorf("detach: %v %s", err, output)
		}
	})
	var st unix.Stat_t
	if err = unix.Stat(device, &st); err != nil {
		t.Fatal(err)
	}
	return Device{ID: "fixture", Path: device, Major: unix.Major(st.Rdev), Minor: unix.Minor(st.Rdev), Bytes: 64 << 20}
}

func TestLinuxPreparationBlankAndReuse(t *testing.T) {
	d := preparationLoop(t)
	intent := ""
	read := func() (string, error) { return intent, nil }
	writes := 0
	write := func(id string) error { intent = id; writes++; return nil }
	uuid, err := prepareFilesystem(context.Background(), "fixture", d, true, true, read, write)
	if err != nil || uuid == "" || uuid != intent || writes != 1 {
		t.Fatalf("initial preparation: uuid=%s intent=%s writes=%d err=%v", uuid, intent, writes, err)
	}
	again, err := prepareFilesystem(context.Background(), "fixture", d, false, true, read, write)
	if err != nil || again != uuid || writes != 1 {
		t.Fatalf("reuse modified content: uuid=%s writes=%d err=%v", again, writes, err)
	}
}

func TestLinuxPreparationRejectsUnsafeContent(t *testing.T) {
	for _, kind := range []string{"readonly-blank", "restore-blank", "unknown-data", "unknown-format-outcome"} {
		t.Run(kind, func(t *testing.T) {
			d := preparationLoop(t)
			initialize := true
			intent := ""
			switch kind {
			case "readonly-blank":
				d.ReadOnly = true
			case "restore-blank":
				initialize = false
			case "unknown-format-outcome":
				intent = "00000000-0000-0000-0000-000000000001"
			case "unknown-data":
				f, err := os.OpenFile(d.Path, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteAt([]byte("existing customer bytes"), 4096)
				_ = f.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			written := false
			_, err := prepareFilesystem(context.Background(), "fixture", d, initialize, true, func() (string, error) { return intent, nil }, func(string) error { written = true; return nil })
			if err == nil || written {
				t.Fatalf("unsafe initialization: intentWritten=%v err=%v", written, err)
			}
		})
	}
}

func TestLinuxPreparationInheritedFDProtectsOrphan(t *testing.T) {
	d := preparationLoop(t)
	lock, err := os.Open(d.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		t.Fatal(err)
	}
	child := exec.Command("sleep", "30")
	child.ExtraFiles = []*os.File{lock}
	if err = child.Start(); err != nil {
		lock.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	// This mimics losing all parent FDs while the formatter still runs.
	_ = lock.Close()
	_, err = prepareFilesystem(context.Background(), "fixture", d, true, true, func() (string, error) { return "", nil }, func(string) error { t.Fatal("formatter started with inherited device lock"); return nil })
	if err == nil {
		t.Fatal("orphaned formatter did not retain exclusive access")
	}
}

// This is a boot-identity fixture, not a physical-machine reboot test.
func TestLinuxPreparationJournalAcrossBoot(t *testing.T) {
	d := preparationLoop(t)
	journal, err := OpenJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	node := &Node{Config: Config{NodeUID: "node-uid", DeviceRoot: filepath.Dir(d.Path)}, BootID: "first-boot", Journal: journal}
	q := runtime.PreparationRequest{Operation: "preparation-boot-fixture", PlanDigest: "first-plan", Spec: core.PreparationSpec{Profile: DescribeVolumeProfile(), PVC: core.PVCRef{Namespace: "test", Name: "data", UID: "pvc-uid"}, PVUID: "pv-uid", SourceRevision: core.Digest("blank"), Initialize: true}, Endpoint: core.Endpoint{VolumeUID: "pvc-uid", BindingID: d.ID, NodeUID: "node-uid", NodeBootID: node.BootID, Epoch: 1, Path: d.Path, Kind: "BlockDevice", Size: d.Bytes, Major: d.Major, Minor: d.Minor, Identity: "loop-fixture"}}
	first, err := node.PrepareVolume(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "marker")
	if err = os.WriteFile(marker, []byte("survives-preparation-reboot"), 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("debugfs", "-w", "-R", "write "+marker+" /keep", d.Path).CombinedOutput(); err != nil {
		t.Fatalf("debugfs write: %v %s", err, out)
	}
	node.BootID = "next-boot"
	q.Endpoint.NodeBootID = node.BootID
	q.PlanDigest = "next-plan"
	second, err := node.PrepareVolume(context.Background(), q)
	if err != nil || second.UUID != first.UUID {
		t.Fatalf("reboot preparation: %+v %v", second, err)
	}
	out, err := exec.Command("debugfs", "-R", "cat /keep", d.Path).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "survives-preparation-reboot") {
		t.Fatalf("reboot lost content: %v %s", err, out)
	}
	receipt, err := node.ReleasePreparation(context.Background(), q)
	if err != nil || !receipt.Released {
		t.Fatalf("release: %+v %v", receipt, err)
	}
}
