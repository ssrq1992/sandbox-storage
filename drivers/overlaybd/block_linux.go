//go:build linux

package overlaybd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

// LinuxBlockPublisher uses bind mounts, preserving the daemon-owned ublk node.
// Its root must be the private SSF node publish directory, never a user path.
type LinuxBlockPublisher struct{ Root string }

func openBlock(path string, e DeviceEvidence) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		f.Close()
		return nil, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFBLK || unix.Major(uint64(st.Rdev)) != e.Major || unix.Minor(uint64(st.Rdev)) != e.Minor {
		f.Close()
		return nil, errors.New("block device identity mismatch")
	}
	var size uint64
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&size)))
	if errno != 0 || size != e.CapacityBytes {
		f.Close()
		return nil, errors.New("block capacity mismatch")
	}
	return f, nil
}

func (m LinuxBlockPublisher) target(path string, create bool) (*os.File, error) {
	if !filepath.IsAbs(m.Root) || filepath.Clean(m.Root) != m.Root || filepath.Dir(path) != m.Root || filepath.Base(path) == "." {
		return nil, errors.New("target outside private publish root")
	}
	// Resolve each parent with O_NOFOLLOW. Keep the parent FD through target open;
	// mount operations below use the opened target FD, not a second path lookup.
	dir, err := unix.Open("/", unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(m.Root, "/"), "/") {
		next, e := unix.Openat(dir, part, unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(dir)
		if e != nil {
			return nil, e
		}
		dir = next
	}
	defer unix.Close(dir)
	flags := unix.O_PATH | unix.O_NOFOLLOW | unix.O_CLOEXEC
	fd, err := unix.Openat(dir, filepath.Base(path), flags, 0)
	if errors.Is(err, unix.ENOENT) && create {
		fd, err = unix.Openat(dir, filepath.Base(path), unix.O_CREAT|unix.O_EXCL|unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	}
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return nil, err
	}
	kind := st.Mode & unix.S_IFMT
	if kind != unix.S_IFREG && kind != unix.S_IFBLK {
		unix.Close(fd)
		return nil, errors.New("publish target is not a regular file or block mount")
	}
	return os.NewFile(uintptr(fd), path), nil
}

func mountedBlock(path string) (bool, bool, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, false, err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if len(fields) < 6 {
			continue
		}
		decoded := strings.NewReplacer("\\040", " ", "\\011", "\t", "\\012", "\n", "\\134", "\\").Replace(fields[4])
		if decoded == path {
			return true, strings.Contains(","+fields[5]+",", ",ro,"), nil
		}
	}
	return false, false, scan.Err()
}
func (m LinuxBlockPublisher) Publish(ctx context.Context, e DeviceEvidence, path string, readonly bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source, err := openBlock(e.Path, e)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := m.target(path, true)
	if err != nil {
		return err
	}
	defer target.Close()
	mounted, ro, err := mountedBlock(path)
	if err != nil {
		return err
	}
	if mounted {
		actual, err := openBlock(path, e)
		if err != nil {
			return err
		}
		actual.Close()
		if ro == readonly {
			return nil
		}
		if ro {
			return errors.New("readonly block publication cannot become writable")
		}
		// Retry the interrupted bind -> readonly-remount sequence below.
	} else {
		src := fmt.Sprintf("/proc/self/fd/%d", source.Fd())
		dst := fmt.Sprintf("/proc/self/fd/%d", target.Fd())
		if err = unix.Mount(src, dst, "", unix.MS_BIND, ""); err != nil {
			return err
		}
	}
	if readonly {
		// Reopen the mounted target: the pre-mount FD still references the covered file.
		mountedTarget, err := m.target(path, false)
		if err != nil {
			return err
		}
		defer mountedTarget.Close()
		if err = unix.Mount("", fmt.Sprintf("/proc/self/fd/%d", mountedTarget.Fd()), "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
			return err
		}
	}
	actual, err := openBlock(path, e)
	if err != nil {
		return err
	}
	actual.Close()
	mounted, ro, err = mountedBlock(path)
	if err != nil {
		return err
	}
	if !mounted || ro != readonly {
		return errors.New("block publication not visible with requested access")
	}
	return nil
}
func (m LinuxBlockPublisher) Unpublish(ctx context.Context, e DeviceEvidence, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	target, err := m.target(path, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer target.Close()
	mounted, _, err := mountedBlock(path)
	if err != nil || !mounted {
		return err
	}
	actual, err := openBlock(path, e)
	if err != nil {
		return err
	}
	actual.Close()
	// No lazy unmount: a busy native FD/mount remains a visible cleanup failure.
	return unix.Unmount(fmt.Sprintf("/proc/self/fd/%d", target.Fd()), 0)
}

// deviceMounts lists direct publication mount points under the private publish
// root whose mounted block identity matches the evidence. Mounts belonging to
// other devices (other sessions) are never included.
func (m LinuxBlockPublisher) deviceMounts(e DeviceEvidence) ([]string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var targets []string
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if len(fields) < 6 {
			continue
		}
		decoded := strings.NewReplacer("\\040", " ", "\\011", "\t", "\\012", "\n", "\\134", "\\").Replace(fields[4])
		if filepath.Dir(decoded) != m.Root || decoded == m.Root {
			continue
		}
		actual, err := openBlock(decoded, e)
		if err != nil {
			continue
		}
		actual.Close()
		targets = append(targets, decoded)
	}
	return targets, scan.Err()
}

// SweepDevice removes residual publication bind-mounts of one device under
// the private publish root. A publish whose WAL update was lost can leave the
// kernel mount behind while its publication record is gone; the native device
// stop would then fail with EBUSY forever. Release authorization for the
// device was already pinned by BeginRelease, so only this device's own stale
// mounts are unmounted; the loop is bounded against stacked mounts.
func (m LinuxBlockPublisher) SweepDevice(ctx context.Context, e DeviceEvidence) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(m.Root) || filepath.Clean(m.Root) != m.Root {
		return errors.New("private publish root required")
	}
	for range 16 {
		targets, err := m.deviceMounts(e)
		if err != nil {
			return err
		}
		if len(targets) == 0 {
			return nil
		}
		if err = m.Unpublish(ctx, e, targets[0]); err != nil {
			return err
		}
	}
	return errors.New("residual publication mounts did not drain")
}
