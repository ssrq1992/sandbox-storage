package sandboxschedadapter

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// hostProc points at the host /proc when it is mounted into the container.
// The node pod shares its own process namespace (so engine peers are visible
// via /proc), but SandboxSched sandboxes run in the agent pod with hostPID;
// their host PIDs are only visible through the host /proc mount.
var hostProc = func() string {
	if fi, err := os.Stat("/host/proc"); err == nil && fi.IsDir() {
		return "/host/proc"
	}
	return "/proc"
}()

// procPath resolves a /proc path against the host procfs when available.
func procPath(rel string) string { return filepath.Join(hostProc, rel) }

// pidAlive reports whether the host PID exists.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if hostProc != "/proc" {
		fi, err := os.Stat(procPath(strconv.Itoa(pid)))
		return err == nil && fi.IsDir()
	}
	err := unix.Kill(pid, 0)
	return err == nil || errors.Is(err, unix.EPERM)
}

// mountEntry is one parsed /proc/<pid>/mountinfo line.
type mountEntry struct {
	Dev        uint64 // major:minor as comparable value
	Root       string // root within the source filesystem
	MountPoint string // mount point within the namespace
	FSType     string // filesystem type after the separator field
	Source     string // source device or server:/path after the separator
}

func devID(major, minor uint32) uint64 { return uint64(major)<<32 | uint64(minor) }

func parseMountinfo(r *os.File) ([]mountEntry, error) {
	var out []mountEntry
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			continue
		}
		var major, minor uint32
		if _, err := fmt.Sscanf(fields[2], "%d:%d", &major, &minor); err != nil {
			continue
		}
		// Optional fields sit between the options and the "-" separator;
		// filesystem type and source follow the separator.
		fstype, source := "", ""
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				if i+1 < len(fields) {
					fstype = fields[i+1]
				}
				if i+2 < len(fields) {
					source = fields[i+2]
				}
				break
			}
		}
		out = append(out, mountEntry{Dev: devID(major, minor), Root: fields[3], MountPoint: unescapeMountinfo(fields[4]), FSType: fstype, Source: source})
	}
	return out, scanner.Err()
}

func unescapeMountinfo(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 <= len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func readMountinfo(path string) ([]mountEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseMountinfo(f)
}

// selfMountEntry returns the mount entry mounted exactly at target in the
// current namespace, if any.
func selfMountEntry(target string) (mountEntry, bool) {
	entries, err := readMountinfo("/proc/self/mountinfo")
	if err != nil {
		return mountEntry{}, false
	}
	for _, e := range entries {
		if e.MountPoint == target {
			return e, true
		}
	}
	return mountEntry{}, false
}

// pidHasNFSMount verifies that the process at pid has an NFS filesystem at
// guestPath carrying the expected export source: the kernel evidence used
// for direct-mount NFS bindings, where no device identity exists.
func pidHasNFSMount(pid int, source, guestPath string) bool {
	entries, err := readMountinfo(procPath(strconv.Itoa(pid) + "/mountinfo"))
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.MountPoint != guestPath || !strings.HasPrefix(e.FSType, "nfs") {
			continue
		}
		if source == "" || e.Source == source {
			return true
		}
	}
	return false
}

// unmountFilesystem flushes and unmounts a managed filesystem mount. Plain
// unmount is attempted first; a lazy detach is only used when the mount
// namespace of this service itself cannot release it.
func unmountFilesystem(target string) error {
	if _, ok := selfMountEntry(target); !ok {
		return nil
	}
	unix.Sync()
	if err := unix.Unmount(target, 0); err == nil {
		return nil
	} else if !errors.Is(err, unix.EBUSY) && !errors.Is(err, unix.EINVAL) {
		return err
	}
	var last error
	for attempt := 0; attempt < 10; attempt++ {
		time.Sleep(200 * time.Millisecond)
		last = unix.Unmount(target, 0)
		if last == nil {
			return nil
		}
	}
	if last != nil && errors.Is(last, unix.EBUSY) {
		if err := unix.Unmount(target, unix.MNT_DETACH); err == nil {
			return nil
		}
	}
	return last
}

// nfsResolveSource resolves the server part of an NFS export reference to an
// IP address. The raw mount(2) path performs hostname resolution through the
// kernel request-key upcall, which does not exist in this container, so the
// adapter resolves the address itself and mounts by IP. Callers persist the
// resolved form: mountinfo sources and journal records must compare equal.
func nfsResolveSource(source string) (string, error) {
	server, path, ok := strings.Cut(source, ":")
	if !ok || server == "" || !strings.HasPrefix(path, "/") {
		return "", errors.New("invalid nfs source")
	}
	if net.ParseIP(server) != nil {
		return source, nil
	}
	addrs, err := net.LookupHost(server)
	if err != nil {
		return "", fmt.Errorf("resolve nfs server %s: %w", server, err)
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("nfs server %s has no address", server)
	}
	return addrs[0] + ":" + path, nil
}

// mountNFS mounts an NFSv4 export directly at target. Soft mount semantics
// keep releases operable when the NFS server is unavailable: IO errors
// surface to the sandbox instead of hanging in uninterruptible state.
func mountNFS(source, target string, readonly bool) error {
	resolved, err := nfsResolveSource(source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	if entry, ok := selfMountEntry(target); ok {
		if strings.HasPrefix(entry.FSType, "nfs") {
			return nil // idempotent after recovery
		}
		return errors.New("target already carries a different filesystem")
	}
	flags := uintptr(0)
	if readonly {
		flags = unix.MS_RDONLY
	}
	// The kernel requires the resolved server address in the raw mount data
	// (mount.nfs normally injects it as addr=); without it the mount syscall
	// fails with EINVAL. The source is always an IP form here because
	// nfsResolveSource resolved it above.
	server, _, _ := strings.Cut(resolved, ":")
	data := "vers=4,proto=tcp,soft,timeo=100,retrans=3,addr=" + server
	if err := unix.Mount(resolved, target, "nfs", flags, data); err != nil {
		return fmt.Errorf("mount nfs %s at %s: %w", resolved, target, err)
	}
	return nil
}
