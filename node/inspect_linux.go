//go:build linux

package node

import (
	"bufio"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"ssf.dev/ssf/core"
	"strconv"
	"strings"
	"unsafe"
)

func inspect(path string, block, ro bool) (core.Endpoint, error) {
	var out core.Endpoint
	fi, e := os.Lstat(path)
	if e != nil {
		return out, e
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return out, errors.New("symlink target rejected")
	}
	out.Path = path
	out.ReadOnly = ro
	if block {
		var st unix.Stat_t
		if e = unix.Stat(path, &st); e != nil {
			return out, e
		}
		if st.Mode&unix.S_IFMT != unix.S_IFBLK {
			return out, errors.New("not a block device")
		}
		out.Kind = "BlockDevice"
		out.Major = unix.Major(uint64(st.Rdev))
		out.Minor = unix.Minor(uint64(st.Rdev))
		out.Identity = fmt.Sprintf("%d:%d", out.Major, out.Minor)
		f, e := os.Open(path)
		if e != nil {
			return out, e
		}
		defer f.Close()
		var size uint64
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&size)))
		if errno != 0 {
			return out, errno
		}
		out.Size = size
		return out, nil
	}
	if !fi.IsDir() {
		return out, errors.New("not a directory")
	}
	f, e := os.Open("/proc/self/mountinfo")
	if e != nil {
		return out, e
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			continue
		}
		mount := strings.NewReplacer("\\040", " ", "\\011", "\t", "\\012", "\n", "\\134", "\\").Replace(fields[4])
		if mount == path {
			if ro && !strings.Contains(","+fields[5]+",", ",ro,") {
				return out, errors.New("readonly mount required")
			}
			if _, e = strconv.Atoi(fields[0]); e != nil {
				return out, e
			}
			out.Kind = "FileTree"
			out.Identity = fields[0] + ":" + fields[2]
			return out, nil
		}
	}
	if e = scanner.Err(); e != nil {
		return out, e
	}
	return out, errors.New("target is not a visible mount")
}
