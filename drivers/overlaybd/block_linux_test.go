//go:build linux

package overlaybd

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLinuxPublisherRejectsSymlinkAndOutsideRoot(t *testing.T) {
	root := t.TempDir()
	m := LinuxBlockPublisher{Root: root}
	if _, err := m.target(filepath.Join(filepath.Dir(root), "outside"), true); err == nil {
		t.Fatal("outside target accepted")
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink("/dev/null", link); err != nil {
		t.Fatal(err)
	}
	if _, err := m.target(link, true); err == nil {
		t.Fatal("symlink accepted")
	}
	parent := filepath.Join(root, "parent")
	if err := os.Symlink(root, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := (LinuxBlockPublisher{Root: parent}).target(filepath.Join(parent, "child"), true); err == nil {
		t.Fatal("symlink parent accepted")
	}
}

// This is an isolated loop-device mount fixture, not an OverlayBD/VM test.
// Run explicitly through AgentENV's capability runner in a disposable Linux host.
func TestLinuxBlockPublicationFixture(t *testing.T) {
	if os.Getenv("SSF_TEST_LINUX_MOUNTS") != "1" {
		t.Skip("requires explicit Linux CAP_SYS_ADMIN loop mount fixture opt-in")
	}
	root := t.TempDir()
	backing := filepath.Join(root, "fixture.img")
	f, err := os.Create(backing)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(16 << 20); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	raw, err := exec.Command("losetup", "--find", "--show", backing).CombinedOutput()
	if err != nil {
		t.Fatalf("allocate isolated loop: %s %v", raw, err)
	}
	device := strings.TrimSpace(string(raw))
	target := filepath.Join(root, "published")
	defer func() {
		_ = unix.Unmount(target, 0)
		if raw, err := exec.Command("losetup", "--detach", device).CombinedOutput(); err != nil {
			t.Errorf("release fixture loop: %s %v", raw, err)
		}
	}()
	var st unix.Stat_t
	if err = unix.Stat(device, &st); err != nil {
		t.Fatal(err)
	}
	e := DeviceEvidence{Path: device, Major: unix.Major(uint64(st.Rdev)), Minor: unix.Minor(uint64(st.Rdev)), CapacityBytes: 16 << 20}
	m := LinuxBlockPublisher{Root: root}
	for _, readonly := range []bool{false, true} {
		if err = m.Publish(context.Background(), e, target, readonly); err != nil {
			t.Fatal(err)
		}
		if err = m.Publish(context.Background(), e, target, readonly); err != nil {
			t.Fatalf("retry: %v", err)
		}
		wrong := e
		wrong.Minor++
		if err = m.Unpublish(context.Background(), wrong, target); err == nil {
			t.Fatal("unmounted different device identity")
		}
		if err = m.Unpublish(context.Background(), e, target); err != nil {
			t.Fatal(err)
		}
		if err = m.Unpublish(context.Background(), e, target); err != nil {
			t.Fatalf("unpublish retry: %v", err)
		}
	}
}
