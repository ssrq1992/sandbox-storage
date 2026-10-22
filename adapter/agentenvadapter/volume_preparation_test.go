package agentenvadapter

import (
	"context"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
	"testing"
)

func TestPreparationRebootKeepsLogicalDeviceAndFormatIntent(t *testing.T) {
	old := runtime.PreparationRequest{Operation: "operation", PlanDigest: "old-plan", Spec: core.PreparationSpec{Profile: DescribeVolumeProfile(), PVC: core.PVCRef{Namespace: "n", Name: "p", UID: "pvc"}, PVUID: "pv", SourceRevision: core.Digest("blank"), Initialize: true}, Endpoint: core.Endpoint{VolumeUID: "pvc", BindingID: "binding", NodeUID: "node", NodeBootID: "old-boot", Epoch: 3, Size: 4096, Path: "/device", Kind: "BlockDevice", Major: 7, Minor: 1, Identity: "old-device"}}
	next := old
	next.PlanDigest = "new-plan"
	next.Endpoint.NodeBootID = "new-boot"
	next.Endpoint.Minor = 2
	next.Endpoint.Identity = "new-device"
	if !preparationRequestReboot(old, next) {
		t.Fatal("same operation could not recover after reboot")
	}
	for _, field := range []string{"operation", "epoch", "volume", "source", "node", "size"} {
		t.Run(field, func(t *testing.T) {
			bad := next
			switch field {
			case "operation":
				bad.Operation = "other"
			case "epoch":
				bad.Endpoint.Epoch++
			case "volume":
				bad.Endpoint.VolumeUID = "other"
			case "source":
				bad.Spec.SourceRevision = "other"
			case "node":
				bad.Endpoint.NodeUID = "other"
			case "size":
				bad.Endpoint.Size++
			}
			if preparationRequestReboot(old, bad) {
				t.Fatal("reboot changed preparation authority")
			}
		})
	}
	node := &Node{}
	if _, err := node.Plan(context.Background(), old.Spec); err != nil {
		t.Fatal(err)
	}
	restored := old.Spec
	restored.SourceRevision = "snapshot-uid"
	if _, err := node.Plan(context.Background(), restored); err == nil {
		t.Fatal("restored source granted initialization")
	}
	restored.Initialize = false
	if _, err := node.Plan(context.Background(), restored); err != nil {
		t.Fatal(err)
	}
}

func TestOverlayBDPreparationNeverFormats(t *testing.T) {
	n := &Node{}
	spec := core.PreparationSpec{Profile: DescribeOverlayBDVolumeProfile(), PVC: core.PVCRef{UID: "pvc"}, PVUID: "pv", SourceRevision: core.Digest("blank"), ExpectedUUID: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a"}
	if _, err := n.Plan(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	spec.Initialize = true
	if _, err := n.Plan(context.Background(), spec); err == nil {
		t.Fatal("OverlayBD authorized generic formatting")
	}
	spec.Initialize = false
	spec.ExpectedUUID = ""
	if _, err := n.Plan(context.Background(), spec); err == nil {
		t.Fatal("OverlayBD accepted unpinned content")
	}
}
