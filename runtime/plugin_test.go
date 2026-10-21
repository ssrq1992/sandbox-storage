package runtime

import "testing"

func TestCaptureCloneRequiresCompleteProfile(t *testing.T) {
	p := PluginIdentity{Protocol: ProtocolVersion, Name: "test", Version: "1", Profiles: []string{AgentENVCaptureClone}, MaxPlanBytes: 1 << 20, Capabilities: LifecycleCapabilities{BlockDevice: true, ReadOnly: true, Freeze: true, Capture: true, Restore: true, RetainedUse: true}}
	if err := p.Validate(AgentENVCaptureClone); err != nil {
		t.Fatal(err)
	}
	p.Capabilities.RetainedUse = false
	if err := p.Validate(AgentENVCaptureClone); err == nil {
		t.Fatal("advertised profile without retained use")
	}
	p.Protocol = "unknown"
	if err := p.Validate(AgentENVCaptureClone); err == nil {
		t.Fatal("accepted incompatible version")
	}
}
