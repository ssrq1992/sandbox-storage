// Package agentenvadapter integrates AgentENV without adding project logic to SSF.
package agentenvadapter

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

var ErrUnsupported = errors.New("operation is not supported for externally managed volumes")
var safeID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

type Resources struct {
	CPU       int64 `json:"cpu"`
	MemoryMiB int64 `json:"memory_mib"`
}
type Principal struct {
	Tenant     string   `json:"tenant"`
	Namespaces []string `json:"namespaces"`
}
type StartRequest struct {
	Expand     map[string]int64 `json:"expand,omitempty"`
	ID         string           `json:"id"`
	Instance   string           `json:"instance"`
	Operation  string           `json:"operation"`
	Plan       string           `json:"plan"`
	Initialize map[string]bool  `json:"initialize"` // binding UID; control-authorized new claims only
	Resources  Resources        `json:"resources"`
}

// Device is the generic AgentENV external-storage contract. It contains no CSI concepts.
type Device struct {
	ID        string `json:"id"`
	Path      string `json:"path"`
	GuestPath string `json:"guest_path"`
	ReadOnly  bool   `json:"readonly"`
	Major     uint32 `json:"major"`
	Minor     uint32 `json:"minor"`
	Bytes     uint64 `json:"bytes"`
	UUID      string `json:"uuid"`
}
type RuntimeEvent struct {
	Capture      *NativeCapture `json:"capture,omitempty"`
	Sequence     uint64         `json:"sequence"`
	Ticket       string         `json:"ticket"`
	SandboxID    string         `json:"sandbox_id"`
	InstanceID   string         `json:"instance_id"`
	Event        string         `json:"event"`
	OwnerPID     int            `json:"owner_pid"`
	OwnerStart   string         `json:"owner_start"`
	PID          int            `json:"pid"`
	ProcessStart string         `json:"process_start"`
	BootID       string         `json:"boot_id"`
	Devices      []Device       `json:"devices,omitempty"`
}
type NodeRecord struct {
	EverReady          bool               `json:"ever_ready"`
	DeleteRequested    bool               `json:"delete_requested"`
	DeleteAcknowledged bool               `json:"delete_acknowledged"`
	ControlReady       bool               `json:"control_ready"`
	Native             *NativeDispatch    `json:"native,omitempty"`
	Timings            map[string]float64 `json:"timings_ms"`
	LaunchAt           time.Time          `json:"launch_at"`
	Request            StartRequest       `json:"request"`
	State              string             `json:"state"`
	Reason             string             `json:"reason,omitempty"`
	Ticket             string             `json:"ticket"`
	Stop               bool               `json:"stop"`
	Dispatched         bool               `json:"dispatched"`
	Response           json.RawMessage    `json:"response,omitempty"`
	Runtime            RuntimeEvent       `json:"runtime"`
	Devices            []Device           `json:"devices"`
	Format             map[string]string  `json:"format"`
	BootID             string             `json:"boot_id"`
	NodeUID            string             `json:"node_uid"`
	PIDNamespace       string             `json:"pid_namespace"`
	Updated            time.Time          `json:"updated"`
}

func newID() string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func (p Principal) Allows(ns string) bool {
	for _, v := range p.Namespaces {
		if ns == v {
			return true
		}
	}
	return false
}
