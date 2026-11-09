// Package sandboxschedadapter integrates SandboxSched (containerd + runc
// sandboxes) as an SSF runtime. The control plugin hosts launch planning
// inside the shared coordinator process; the node service runs next to the
// public SSF node agent, publishes volumes on the node, mounts the block
// device filesystems and verifies sandbox mounts through kernel state.
//
// Protocol outline (all subjects use the SandboxSched sandbox ID):
//
//	bind    (node service -> coordinator LocalLaunch)
//	        -> consume.Coordinator.Prepare (placement pinned to the sandbox node)
//	ready   (agent -> node service -> ssf-node MarkRuntimeReady
//	        + coordinator InvokeLifecycle -> ConfirmReady + Commit)
//	release (agent -> node service unmount + ssf-node ReleaseVolumes
//	        + coordinator InvokeLifecycle -> ConfirmStopped + Release)
//
// Evidence proofs are computed by the node service from a per-bind ticket and
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

// DriverConfig names a CSI controller endpoint used for controller publish.
type DriverConfig struct {
	Name               string `json:"name"`
	ControllerEndpoint string `json:"controllerEndpoint"`
	Revision           string `json:"revision"`
}

// ControlConfig configures the coordinator-hosted control plugin.
type ControlConfig struct {
	Namespace             string               `json:"namespace"`
	Principal             Principal            `json:"principal"`
	DatabaseURL           string               `json:"database_url"`
	Kubeconfig            string               `json:"kubeconfig"`
	Drivers               []DriverConfig       `json:"drivers"`
	SigningKey            string               `json:"signing_key"`
	Cert                  string               `json:"cert"`
	Key                   string               `json:"key"`
	CA                    string               `json:"ca"`
	PreparationNodePort   string               `json:"preparation_node_port"`
	PreparationServerName string               `json:"preparation_server_name"`
	NodeServicePort       string               `json:"node_service_port"`
	NodeServiceLabel      string               `json:"node_service_label"`
	NodeServerName        string               `json:"node_server_name"`
	VolumeProfiles        []core.VolumeProfile `json:"volume_profiles"`
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
	if c.PreparationNodePort == "" {
		c.PreparationNodePort = "9443"
	}
	if c.PreparationServerName == "" {
		return c, errors.New("preparation_server_name required")
	}
	if c.NodeServicePort == "" {
		c.NodeServicePort = "9447"
	}
	if c.NodeServiceLabel == "" {
		c.NodeServiceLabel = "app.kubernetes.io/name=sandboxsched-storage-node"
	}
	if c.NodeServerName == "" {
		return c, errors.New("node_server_name required")
	}
	if c.Namespace == "" || c.Principal.Tenant == "" || len(c.Principal.Namespaces) == 0 {
		return c, errors.New("sandboxsched namespace and principal required")
	}
	if c.SigningKey == "" || c.Cert == "" || c.Key == "" || c.CA == "" {
		return c, errors.New("sandboxsched signing key and control TLS required")
	}
	if len(c.Drivers) == 0 {
		return c, errors.New("sandboxsched CSI drivers required")
	}
	for _, p := range c.VolumeProfiles {
		if err := p.Validate(); err != nil {
			return c, err
		}
		if p.Runtime != c.Principal.Tenant {
			return c, errors.New("sandboxsched profile runtime mismatch")
		}
	}
	return c, nil
}

// NodeConfig configures the node-local service.
type NodeConfig struct {
	NodeName     string `json:"node_name"`
	Listen       string `json:"listen"`
	HealthListen string `json:"health_listen"`
	HookListen   string `json:"hook_listen"`
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

	SSFEndpoint   string `json:"ssf_endpoint"`
	SSFServerName string `json:"ssf_server_name"`
	APIKeyFile    string `json:"api_key_file"`
	Root          string `json:"root"`
	DeviceRoot    string `json:"device_root"`

	// EvidencePeers lists the SPIFFE IDs allowed to call the evidence API.
	EvidencePeers     []string `json:"evidence_peers"`
	HookTokenFile     string   `json:"hook_token_file"`
	PreparationRuntime string `json:"preparation_runtime"`

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
		c.Root = "/var/lib/ssf/sandboxschedadapter"
	}
	if c.DeviceRoot == "" {
		c.DeviceRoot = "/var/lib/ssf"
	}
	if c.Listen == "" {
		c.Listen = ":9447"
	}
	if c.HookListen == "" {
		c.HookListen = "127.0.0.1:9449"
	}
	if c.SSFEndpoint == "" {
		c.SSFEndpoint = "127.0.0.1:9443"
	}
	if c.PreparationRuntime == "" {
		c.PreparationRuntime = "sandboxsched"
	}
	if c.NodeName == "" || c.SSFEndpoint == "" || c.APIKeyFile == "" || c.Cert == "" || c.Key == "" || c.CA == "" {
		return c, errors.New("sandboxsched node identity, SSF endpoint, API key and TLS required")
	}
	if c.Coordinator.Endpoint == "" || c.Coordinator.Peer == "" || c.Coordinator.Cert == "" {
		return c, errors.New("sandboxsched coordinator client required")
	}
	if len(c.EvidencePeers) == 0 {
		return c, errors.New("sandboxsched evidence verifier required")
	}
	if c.HookTokenFile == "" {
		return c, errors.New("sandboxsched preparation hook token required")
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
	// Kind "" is the default engine-backed block volume; "nfs" marks a
	// direct NFS export mount that bypasses the storage engine entirely.
	Kind string `json:"kind,omitempty"`
	// Source is the resolved NFS export ("server:/path") of a nfs binding.
	Source string `json:"source,omitempty"`
}

// Dispatch is the durable launch result returned to the node service.
type Dispatch struct {
	Operation string            `json:"operation"`
	Plan      string            `json:"plan"`
	RunUID    string            `json:"runUID"`
	// Kind "" is the default engine-backed launch; "nfs" is a direct-mount
	// dispatch planned locally by the control plugin without consume.
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
