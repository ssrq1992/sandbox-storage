package agentenvadapter

import (
	"errors"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
)

type NodeInfo struct {
	Plugin   *runtime.PluginIdentity `json:"plugin,omitempty"`
	Defaults Resources               `json:"defaults"`
	Name     string                  `json:"name"`
	UID      string                  `json:"uid"`
	BootID   string                  `json:"boot_id"`
	Ready    bool                    `json:"ready"`
}

const NativeStorageProtocol = "agentenv.storage.v2"
const NativeHookVersion = 4
const NativeMaxPlanBytes = 256 << 10

// NativeDefaults is read from the original authenticated node API. An old node
// reporting only CPU/memory cannot silently be scheduled with a new plugin.
type NativeDefaults struct {
	StorageCaptureRequests bool `json:"storage_capture_requests"`
	Resources
	StorageProtocol string `json:"storage_protocol"`
	HookVersion     int    `json:"storage_hook_version"`
}

func nativePluginIdentity(d NativeDefaults) (runtime.PluginIdentity, error) {
	if d.StorageProtocol != NativeStorageProtocol || (d.HookVersion != NativeHookVersion && d.HookVersion != 3) || d.CPU <= 0 || d.MemoryMiB < 128 {
		return runtime.PluginIdentity{}, errors.New("native StorageProvider protocol is incompatible")
	}
	// This describes implemented integration hooks, not vendor CSI certification.
	p := runtime.PluginIdentity{Protocol: runtime.ProtocolVersion, Name: "agentenv", Version: "3", Profiles: []string{runtime.AgentENVBlockDevelopment, runtime.AgentENVCaptureClone}, MaxPlanBytes: NativeMaxPlanBytes, Capabilities: runtime.LifecycleCapabilities{BlockDevice: true, ReadOnly: true, Freeze: true, Capture: true, Restore: true, RetainedUse: true, OfflineExpand: true}}
	if d.HookVersion == NativeHookVersion {
		p.Version = "4"
		p.VolumePreparation = core.PreparationProtocol
	}
	return p, nil
}
