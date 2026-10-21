// Package core defines storage identities independent of any sandbox project.
package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

type PVCRef struct {
	Namespace string
	Name      string
	UID       string
}
type SecretRef struct {
	Namespace string
	Name      string
}
type Volume struct {
	UID        string
	PVC        PVCRef
	PVName     string
	PVUID      string
	Driver     string
	Handle     string
	Capability *csi.VolumeCapability
	Context    map[string]string
	ReadOnly   bool
	Secrets    map[string]SecretRef
	Generation uint64
}

func (v Volume) Key() string { return Hash(v.Driver + "\x00" + v.Handle) }
func Hash(s string) string   { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func Digest(v any) string    { b, _ := json.Marshal(v); return Hash(string(b)) }

type Binding struct {
	OwnerUID  string `json:",omitempty"`
	RunUID    string `json:",omitempty"`
	ID        string
	Volume    Volume
	Sandbox   string
	Tenant    string
	Node      string
	Epoch     uint64
	MountID   string
	GuestPath string
	State     string
}
type Mount struct {
	VolumeUID string
	PVC       PVCRef
	MountID   string
	GuestPath string
	ReadOnly  bool
}
type Placement struct {
	ID      string
	Node    string
	NodeUID string
	BootID  string
	Expires time.Time
}
type Plan struct {
	PreparedVolumes map[string]PreparationReceipt `json:",omitempty"`
	Purpose         string                        `json:",omitempty"`
	Preparation     *PreparationSpec              `json:",omitempty"`
	OwnerUID        string                        `json:",omitempty"`
	RunUID          string                        `json:",omitempty"`
	ID              string
	Operation       string
	Tenant          string
	Sandbox         string
	Node            string
	NodeUID         string
	BootID          string
	Expires         time.Time
	Bindings        []Binding
	PublishContexts map[string]map[string]string
	NodeIDs         map[string]string
	Revision        string
}

// Owner and Run keep old single-incarnation callers readable. New integrations
// must supply a fresh RunUID for every resume, while retaining OwnerUID.
func (b Binding) Owner() string {
	if b.OwnerUID != "" {
		return b.OwnerUID
	}
	return b.Sandbox
}
func (b Binding) Run() string {
	if b.RunUID != "" {
		return b.RunUID
	}
	return b.Sandbox
}

type Endpoint struct {
	VolumeUID  string
	Generation uint64
	NodeUID    string
	Revision   string
	BindingID  string
	Epoch      uint64
	Kind       string
	Path       string
	Identity   string
	ReadOnly   bool
	GuestPath  string
	NodeBootID string
	Size       uint64
	Major      uint32
	Minor      uint32
}
type Operation struct {
	ID      string
	Digest  string
	State   string
	Reason  string
	Updated time.Time
}

var ErrConflict = errors.New("conflicting operation or active resource")
var ErrPending = errors.New("operation pending; inspect or retry with same request ID")

func (v Volume) SingleNode() bool {
	switch v.Capability.GetAccessMode().GetMode() {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY, csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER, csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
		return true
	}
	return false
}
func (v Volume) SingleConsumer() bool {
	return v.Capability.GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER
}

func ValidateMounts(ms []Mount) error {
	if len(ms) == 0 {
		return errors.New("at least one mount is required")
	}
	seen := map[string]bool{}
	paths := map[string]bool{}
	for _, m := range ms {
		if m.MountID == "" || seen[m.MountID] || (m.VolumeUID == "" && (m.PVC.Namespace == "" || m.PVC.Name == "")) || (m.VolumeUID != "" && (m.PVC.Namespace != "" || m.PVC.Name != "" || m.PVC.UID != "")) {
			return errors.New("invalid or duplicate mount identity")
		}
		if !path.IsAbs(m.GuestPath) || path.Clean(m.GuestPath) != m.GuestPath || m.GuestPath == "/" || paths[m.GuestPath] {
			return fmt.Errorf("invalid guest path %q", m.GuestPath)
		}
		seen[m.MountID] = true
		paths[m.GuestPath] = true
	}
	return nil
}
