package overlaybd

import (
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"strings"
	"testing"
)

func TestBackendRequiresWholeLifecycleCapabilities(t *testing.T) {
	identity := &api.BackendIdentity{Protocol: BackendProtocol, InstanceId: "instance", RepositoryIds: []string{"repo"}, Capabilities: []string{CapabilityNativeVolume, CapabilityDeviceSession}}
	if ValidateBackend(identity) == nil {
		t.Fatal("cold-start-only backend admitted")
	}
	identity.Capabilities = append(identity.Capabilities, CapabilityLayerCapture, CapabilityHeadPublication)
	if ValidateBackend(identity) == nil {
		t.Fatal("old non-durable capture host admitted")
	}
	identity.Capabilities = append(identity.Capabilities, CapabilityDurableRestack)
	if err := ValidateBackend(identity); err != nil {
		t.Fatal(err)
	}
}

func TestVolumeReceiptCannotSubstituteAnEmptyOrDifferentSource(t *testing.T) {
	request := &api.EnsureVolumeRequest{Operation: &api.OperationIdentity{OperationId: "op", InputDigest: strings.Repeat("a", 64), VolumeId: "vol", RepositoryId: "repo"}, CapacityBytes: 4096, ProfileRevision: Profile, Source: &api.VolumeSource{SnapshotId: "snapshot", HeadRevision: "source-head"}}
	receipt := &api.VolumeReceipt{Operation: proto.Clone(request.Operation).(*api.OperationIdentity), State: "ContentReady", HeadRevision: strings.Repeat("b", 64), FsUuid: "9f0558e1-18cc-4a4a-adf9-51c6eb0efdb3", ContentDigest: strings.Repeat("b", 64), CapacityGeneration: 1, CapacityBytes: 4096}
	if err := ValidateVolumeReceipt(request, receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Operation.InputDigest = "empty-source-digest"
	if ValidateVolumeReceipt(request, receipt) == nil {
		t.Fatal("different input accepted")
	}
}

func TestDeviceReceiptRejectsWrongEpochAndDaemonIdentity(t *testing.T) {
	request := &api.PrepareDeviceRequest{Operation: &api.OperationIdentity{OperationId: "op", InputDigest: strings.Repeat("a", 64), RepositoryId: "repo", VolumeId: "vol"}, SessionId: "session", Grant: &api.UseGrant{Purpose: "runtime", Epoch: 4, RunId: "run", OwnerId: "owner", NodeId: "node", HeadRevision: "head", AuthorizationDigest: strings.Repeat("b", 64)}}
	request.Operation.InputDigest, _ = DeviceInputDigest(request)
	receipt := &api.DeviceReceipt{Operation: proto.Clone(request.Operation).(*api.OperationIdentity), SessionId: "session", Grant: proto.Clone(request.Grant).(*api.UseGrant), State: "Staged", NodeBootId: "boot", DaemonInstanceId: "daemon", DeviceIdentity: "device", CapacityBytes: 4096, FsUuid: "9f0558e1-18cc-4a4a-adf9-51c6eb0efdb3", DevicePath: "/dev/ublkb0", Major: 259}
	if err := ValidateDeviceReceipt(request, receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Grant.Epoch--
	if ValidateDeviceReceipt(request, receipt) == nil {
		t.Fatal("stale epoch accepted")
	}
	receipt.Grant.Epoch++
	receipt.DaemonInstanceId = ""
	if ValidateDeviceReceipt(request, receipt) == nil {
		t.Fatal("unidentified daemon accepted")
	}
}

func TestDeviceReceiptRejectsIncompleteAuthority(t *testing.T) {
	for _, grant := range []*api.UseGrant{nil, {}, {Epoch: 1}, {OwnerId: "owner", RunId: "run", Epoch: 1, NodeId: "node", HeadRevision: "head", AuthorizationDigest: "not-a-digest"}} {
		request := &api.PrepareDeviceRequest{Operation: &api.OperationIdentity{OperationId: "op", InputDigest: strings.Repeat("a", 64), RepositoryId: "repo", VolumeId: "vol"}, SessionId: "session", Grant: grant}
		receipt := &api.DeviceReceipt{Operation: request.Operation, SessionId: request.SessionId, Grant: grant, State: "Staged", NodeBootId: "boot", DaemonInstanceId: "daemon", DeviceIdentity: "device", Major: 259, CapacityBytes: 4096, FsUuid: "9f0558e1-18cc-4a4a-adf9-51c6eb0efdb3", DevicePath: "/dev/ublkb0"}
		if ValidateDeviceReceipt(request, receipt) == nil {
			t.Fatal("incomplete authority accepted")
		}
	}
}

func TestVolumeReceiptRejectsUnboundHeadDigest(t *testing.T) {
	request := &api.EnsureVolumeRequest{Operation: &api.OperationIdentity{OperationId: "op", InputDigest: strings.Repeat("a", 64), RepositoryId: "repo", VolumeId: "volume"}, CapacityBytes: 4096, ProfileRevision: Profile}
	receipt := &api.VolumeReceipt{Operation: request.Operation, State: "ContentReady", HeadRevision: strings.Repeat("b", 64), ContentDigest: strings.Repeat("c", 64), FsUuid: "9f0558e1-18cc-4a4a-adf9-51c6eb0efdb3", CapacityGeneration: 1, CapacityBytes: 4096}
	if ValidateVolumeReceipt(request, receipt) == nil {
		t.Fatal("content digest did not identify published head")
	}
	receipt.ContentDigest = receipt.HeadRevision
	if err := ValidateVolumeReceipt(request, receipt); err != nil {
		t.Fatal(err)
	}
}

func TestExpansionRequiresDurableRestackCapability(t *testing.T) {
	identity := &api.BackendIdentity{Protocol: BackendProtocol, InstanceId: "host", RepositoryIds: []string{"repo"}, Capabilities: []string{CapabilityOfflineExpansion}}
	if ValidateBackend(identity, CapabilityOfflineExpansion) == nil {
		t.Fatal("old expansion host admitted")
	}
	identity.Capabilities = append(identity.Capabilities, CapabilityDurableRestack)
	if err := ValidateBackend(identity, CapabilityOfflineExpansion); err != nil {
		t.Fatal(err)
	}
}
