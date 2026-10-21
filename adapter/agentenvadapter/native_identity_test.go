package agentenvadapter

import (
	"ssf.dev/ssf/runtime"
	"testing"
)

func TestNativeIdentityRejectsOldRuntimeAndNegotiatesFullLifecycle(t *testing.T) {
	d := NativeDefaults{Resources: Resources{CPU: 2, MemoryMiB: 512}}
	if _, err := nativePluginIdentity(d); err == nil {
		t.Fatal("old native runtime admitted")
	}
	d.StorageProtocol = NativeStorageProtocol
	d.HookVersion = NativeHookVersion
	p, err := nativePluginIdentity(d)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Validate(runtime.AgentENVBlockDevelopment); err != nil {
		t.Fatal(err)
	}
	if err = p.Validate(runtime.AgentENVCaptureClone); err != nil {
		t.Fatal(err)
	}
	d.HookVersion++
	if _, err = nativePluginIdentity(d); err == nil {
		t.Fatal("unknown hook version admitted")
	}
}
