// Package sandboxschedadapter integrates SandboxSched (containerd + runc
// sandboxes) as an NFS-only storage service. The control plugin plans every
// launch inside the shared coordinator process: it resolves each requested
// claim to an NFS export and rejects anything that is not NFS. The node
// service mounts the resolved exports on the node and verifies sandbox mounts
// through kernel state.
//
// Protocol outline (all subjects use the SandboxSched sandbox ID):
//
//	bind    (node service -> coordinator LocalLaunch)
//	        -> resolve claims to NFS exports, plan the dispatch locally
//	ready   (agent -> node service kernel mount evidence
//	        + coordinator InvokeLifecycle -> Ready)
//	release (agent -> node service unmount
//	        + coordinator InvokeLifecycle -> Released)
//
// Mount evidence is computed by the node service from a per-bind ticket and
// re-verified against real kernel state (process and mountinfo) before any
// state transition is accepted.
package sandboxschedadapter

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"

	"ssf.dev/ssf/core"
)

// Principal is the tenant and PVC namespace scope of this runtime.
type Principal struct {
	Tenant     string   `json:"tenant"`
	Namespaces []string `json:"namespaces"`
}

func (p Principal) Allows(namespace string) bool {
	for _, n := range p.Namespaces {
		if n == namespace {
			return true
		}
	}
	return false
}

// ControlConfig configures the coordinator-hosted control plugin.
type ControlConfig struct {
	Principal   Principal `json:"principal"`
	DatabaseURL string    `json:"database_url"`
	Kubeconfig  string    `json:"kubeconfig"`
}

// LoadControlConfig reads the control plugin configuration. The database URL
// falls back to the shared coordinator environment.
func LoadControlConfig(path string) (ControlConfig, error) {
	var c ControlConfig
	raw, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(raw, &c); err != nil {
		return c, err
	}
	if c.DatabaseURL == "" {
		c.DatabaseURL = os.Getenv("SSF_DATABASE_URL")
	}
	if c.Principal.Tenant == "" || len(c.Principal.Namespaces) == 0 {
		return c, errors.New("sandboxsched principal required")
	}
	return c, nil
}

// NodeConfig configures the node-local service.
type NodeConfig struct {
	NodeName     string `json:"node_name"`
	Listen       string `json:"listen"`
	HealthListen string `json:"health_listen"`
	Cert         string `json:"cert"`
	Key          string `json:"key"`
	CA           string `json:"ca"`

	Coordinator struct {
		Endpoint   string `json:"endpoint"`
		Cert       string `json:"cert"`
		Key        string `json:"key"`
		CA         string `json:"ca"`
		Peer       string `json:"peer"`
		ServerName string `json:"server_name,omitempty"`
	} `json:"coordinator"`

	// Runtime names this plugin inside the coordinator.
	Runtime    string `json:"runtime"`
	APIKeyFile string `json:"api_key_file"`
	Root       string `json:"root"`

	Kubeconfig string `json:"kubeconfig"`
}

var safeID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// LoadNodeConfig reads the node service configuration.
func LoadNodeConfig(path string) (NodeConfig, error) {
	var c NodeConfig
	raw, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(raw, &c); err != nil {
		return c, err
	}
	if c.NodeName == "" {
		c.NodeName = os.Getenv("NODE_NAME")
	}
	if c.Root == "" {
		c.Root = "/var/lib/sandboxschedadapter"
	}
	if c.Listen == "" {
		c.Listen = ":9447"
	}
	if c.Runtime == "" {
		c.Runtime = "sandboxsched"
	}
	if c.NodeName == "" || c.APIKeyFile == "" || c.Cert == "" || c.Key == "" || c.CA == "" {
		return c, errors.New("sandboxsched node identity, API key and TLS required")
	}
	if c.Coordinator.Endpoint == "" || c.Coordinator.Peer == "" || c.Coordinator.Cert == "" {
		return c, errors.New("sandboxsched coordinator client required")
	}
	return c, nil
}

// BindMountRequest is one volume mount requested for a sandbox.
type BindMountRequest struct {
	Namespace string `json:"namespace"`
	Claim     string `json:"claim"`
	GuestPath string `json:"guest_path"`
	ReadOnly  bool   `json:"read_only"`
}

// BindRequest is the bind envelope sent to the control plugin.
type BindRequest struct {
	Sandbox string             `json:"sandbox"`
	Node    string             `json:"node"`
	BootID  string             `json:"boot_id"`
	Mounts  []BindMountRequest `json:"mounts"`
}

// DispatchBinding describes one resolved binding of a dispatch.
type DispatchBinding struct {
	BindingID string `json:"bindingID"`
	GuestPath string `json:"guestPath"`
	ReadOnly  bool   `json:"readOnly"`
	// Kind is always "nfs": a direct NFS export mount planned locally by
	// the control plugin. The field stays so legacy engine-backed journal
	// rows remain parseable and are rejected explicitly.
	Kind string `json:"kind,omitempty"`
	// Source is the resolved NFS export ("server:/path") of the binding.
	Source string `json:"source,omitempty"`
}

// Dispatch is the durable launch result returned to the node service.
type Dispatch struct {
	Operation string            `json:"operation"`
	Plan      string            `json:"plan"`
	RunUID    string            `json:"runUID"`
	// Kind is always "nfs" (see DispatchBinding.Kind).
	Kind      string            `json:"kind,omitempty"`
	Bindings  []DispatchBinding `json:"bindings"`
	Placement core.Placement    `json:"placement"`
	PlanJSON  core.Plan         `json:"planJSON"`
}

// ReadyRequest is the ready envelope handled by the control plugin.
type ReadyRequest struct {
	Action   string `json:"action"`
	Sandbox  string `json:"sandbox"`
	Instance string `json:"instance"`
	PID      int    `json:"pid"`
	Proof    string `json:"proof"`
}

// ReleaseRequest is the release envelope handled by the control plugin.
type ReleaseRequest struct {
	Action  string `json:"action"`
	Sandbox string `json:"sandbox"`
	Proof   string `json:"proof"`
}
