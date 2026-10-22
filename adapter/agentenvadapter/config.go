package agentenvadapter

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	control "ssf.dev/control"
)

type Config struct {
	Coordinator         control.Config `json:"coordinator"`
	Standalone          bool           `json:"standalone,omitempty"`
	StandaloneConfig    string         `json:"standalone_config,omitempty"`
	DrainTimeoutSeconds int64          `json:"drain_timeout_seconds,omitempty"`
	NativeMode          bool           `json:"native_mode"`
	HookTokenFile       string         `json:"hook_token_file"`
	DispatchKeyFile     string         `json:"dispatch_key_file"`
	SchedulerEndpoint   string         `json:"scheduler_endpoint"`
	NodeControlKeyFile  string         `json:"node_control_key_file"`
	NativeNodeID        string         `json:"native_node_id"`

	Listen        string   `json:"listen"`
	HealthListen  string   `json:"health_listen"`
	HookListen    string   `json:"hook_listen"`
	Cert          string   `json:"cert"`
	Key           string   `json:"key"`
	CA            string   `json:"ca"`
	ControlPeers  []string `json:"control_peers"`
	EvidencePeers []string `json:"evidence_peers"`
	Kubeconfig    string   `json:"kubeconfig"`
	PlanPublicKey string   `json:"plan_public_key"`
	NodeName      string   `json:"node_name"`
	NodeUID       string   `json:"node_uid"`
	SSFEndpoint   string   `json:"ssf_endpoint"`
	AgentURL      string   `json:"agent_url"`
	AgentKeyFile  string   `json:"agent_key_file"`
	Root          string   `json:"root"`
	DeviceRoot    string   `json:"device_root"`
	RuntimeRoot   string   `json:"runtime_root"`
}

func Load(path string) (Config, error) {
	var c Config
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	if c.NodeName == "" {
		c.NodeName = os.Getenv("NODE_NAME")
	}
	if c.Root == "" {
		c.Root = "/var/lib/agentenvadapter"
	}
	if c.DeviceRoot == "" {
		c.DeviceRoot = "/var/lib/ssf"
	}
	if c.RuntimeRoot == "" {
		c.RuntimeRoot = "/var/lib/agentenv-external"
	}
	if c.Listen == "" {
		c.Listen = ":9444"
	}
	if c.HookListen == "" {
		c.HookListen = "127.0.0.1:9445"
	}
	return c, e
}
func (c Config) Validate(role string) error {
	if c.Cert == "" || c.Key == "" || c.CA == "" {
		return errors.New("TLS certificate, key and CA required")
	}
	if c.StandaloneConfig != "" || c.SchedulerEndpoint != "" {
		return errors.New("embedded control configuration removed; configure independent coordinator")
	}
	if role != "node" || !c.NativeMode {
		return errors.New("only native_mode node plugin is supported")
	}
	{
		if c.NodeName == "" || c.SSFEndpoint == "" || len(c.ControlPeers) == 0 || len(c.EvidencePeers) == 0 {
			return errors.New("node identity, SSF endpoint and role peer lists required")
		}
		u, e := url.Parse(c.AgentURL)
		if e != nil || u.Scheme != "http" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") {
			return errors.New("AgentENV API must be on Pod loopback")
		}
	}
	return nil
}
