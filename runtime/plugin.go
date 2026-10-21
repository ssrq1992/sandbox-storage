package runtime

import (
	"errors"
	"fmt"
)

const ProtocolVersion = "ssf.runtime.v2"
const AgentENVBlockDevelopment = "agentenv-block-development-v1"
const AgentENVCaptureClone = "agentenv-capture-clone-v1"

// PluginIdentity describes runtime integration capabilities, not CSI backend
// capabilities. Admission must also verify the actual driver and node profile.
type PluginIdentity struct {
	VolumePreparation string                `json:"volume_preparation,omitempty"`
	Protocol          string                `json:"protocol"`
	Name              string                `json:"name"`
	Version           string                `json:"version"`
	Profiles          []string              `json:"profiles"`
	MaxPlanBytes      int                   `json:"max_plan_bytes"`
	Capabilities      LifecycleCapabilities `json:"capabilities"`
}
type LifecycleCapabilities struct {
	BlockDevice   bool `json:"block_device"`
	ReadOnly      bool `json:"readonly"`
	Freeze        bool `json:"freeze"`
	Capture       bool `json:"capture"`
	Restore       bool `json:"restore"`
	RetainedUse   bool `json:"retained_use"`
	OfflineExpand bool `json:"offline_expand"`
}

func (p PluginIdentity) Validate(required string) error {
	if p.Protocol != ProtocolVersion || p.Name == "" || p.Version == "" || p.MaxPlanBytes <= 0 {
		return errors.New("incompatible runtime plugin identity")
	}
	found := false
	for _, profile := range p.Profiles {
		if profile == required {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("runtime profile %q unavailable", required)
	}
	c := p.Capabilities
	if required == AgentENVBlockDevelopment && !(c.BlockDevice && c.ReadOnly) {
		return errors.New("incomplete block development profile")
	}
	if required == AgentENVCaptureClone && !(c.BlockDevice && c.ReadOnly && c.Freeze && c.Capture && c.Restore && c.RetainedUse) {
		return errors.New("incomplete capture/clone profile")
	}
	return nil
}
