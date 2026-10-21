package node

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
)

type contentFixture struct{ prepares, releases int }

func (f *contentFixture) PrepareVolume(_ context.Context, q runtime.PreparationRequest) (core.PreparationReceipt, error) {
	f.prepares++
	return contentFixtureReceipt(q), nil
}
func contentFixtureReceipt(q runtime.PreparationRequest) core.PreparationReceipt {
	return core.PreparationReceipt{Protocol: core.PreparationProtocol, Operation: q.Operation, PlanDigest: q.PlanDigest, BindingID: q.Endpoint.BindingID,
		VolumeUID: q.Endpoint.VolumeUID, NodeUID: q.Endpoint.NodeUID, BootID: q.Endpoint.NodeBootID, Epoch: q.Endpoint.Epoch, UUID: "fixture-ext4", DeviceIdentity: q.Endpoint.Identity}
}
func (f *contentFixture) InspectVolumePreparation(ctx context.Context, q runtime.PreparationRequest) (core.PreparationReceipt, error) {
	return contentFixtureReceipt(q), nil
}
func (f *contentFixture) ReleasePreparation(ctx context.Context, q runtime.PreparationRequest) (core.PreparationReceipt, error) {
	f.releases++
	r := contentFixtureReceipt(q)
	r.Released = true
	return r, nil
}

func TestContentPreparationHasNoRuntimeEvidenceAndReplays(t *testing.T) {
	engine, driver, key := setup(t)
	engine.Evidence = nil
	engine.NodeUID = "node-uid"
	engine.Inspect = func(path string, block, ro bool) (core.Endpoint, error) {
		return core.Endpoint{Kind: "BlockDevice", Path: path, Identity: "device", Size: 4096}, nil
	}
	fixture := &contentFixture{}
	engine.Preparer = fixture
	pvc := core.PVCRef{Namespace: "test", Name: "data", UID: "pvc"}
	p := core.Plan{Purpose: core.PurposePreparation, ID: "prep", Operation: "prep", Tenant: "t", Node: "n", NodeUID: "node-uid", BootID: "b", Expires: time.Now().Add(time.Hour),
		NodeIDs: map[string]string{"test": "csi-node"}, Preparation: &core.PreparationSpec{Profile: core.VolumeProfile{Name: "profile", Runtime: "agentenv", Version: core.PreparationProtocol, VolumeMode: "Block", Filesystem: "ext4", InitializeBlank: true}, PVC: pvc, PVUID: "pv", SourceRevision: "blank"},
		Bindings: []core.Binding{{ID: "binding", Node: "n", Tenant: "t", OwnerUID: "preparation-owner", RunUID: "preparation-run", Epoch: 3,
			Volume: core.Volume{UID: "pvc", PVC: pvc, PVUID: "pv", Driver: "test", Handle: "disk", Capability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}}}}
	token, err := auth.Sign(p, key)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = engine.Prepare(ctx, "prep", token); err == nil {
		t.Fatal("preparation admitted as runtime")
	}
	for i := 0; i < 2; i++ {
		if _, err = engine.PrepareContent(ctx, "prep", token); err != nil {
			t.Fatal(err)
		}
	}
	if fixture.prepares != 1 || driver.publish != 1 {
		t.Fatal("lost reply repeated preparation")
	}
	if _, err = engine.Ready(ctx, "prep", "", ""); err == nil {
		t.Fatal("preparation manufactured RuntimeReady")
	}
	previousToken := token
	engine.BootID = "next-boot"
	if err = engine.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	p.BootID = engine.BootID
	token, _ = auth.Sign(p, key)
	if _, err = engine.PrepareContent(ctx, "prep", previousToken); err == nil {
		t.Fatal("old boot authorized preparation")
	}
	if _, err = engine.PrepareContent(ctx, "prep", token); err != nil {
		t.Fatalf("same preparation did not resume after reboot: %v", err)
	}
	if fixture.prepares != 2 || driver.publish != 2 {
		t.Fatal("reboot did not republish and inspect original operation")
	}
	stale := p
	stale.Bindings = append([]core.Binding{}, p.Bindings...)
	stale.Bindings[0].Epoch--
	staleToken, _ := auth.Sign(stale, key)
	if _, err = engine.ReleaseContent(ctx, "prep", staleToken); err == nil {
		t.Fatal("stale epoch released current preparation")
	}
	for i := 0; i < 2; i++ {
		r, err := engine.ReleaseContent(ctx, "prep", token)
		if err != nil || r.State != "Released" {
			t.Fatalf("release: %+v %v", r, err)
		}
	}
	if fixture.releases != 1 || driver.unpublish != 1 || driver.unstage != 1 {
		t.Fatal("release replay repeated CSI mutation")
	}
}

func TestPreparationReceiptCannotCrossIdentity(t *testing.T) {
	q := runtime.PreparationRequest{Operation: "op", PlanDigest: "digest",
		Spec:     core.PreparationSpec{ExpectedUUID: "filesystem"},
		Endpoint: core.Endpoint{BindingID: "binding", VolumeUID: "pvc", NodeUID: "node", NodeBootID: "boot", Epoch: 7, Identity: "device"}}
	valid := core.PreparationReceipt{Protocol: core.PreparationProtocol, Operation: "op", PlanDigest: "digest",
		BindingID: "binding", VolumeUID: "pvc", NodeUID: "node", BootID: "boot", Epoch: 7,
		DeviceIdentity: "device", UUID: "filesystem", Released: true}
	if err := validateContentReceipt(q, valid, true); err != nil {
		t.Fatal(err)
	}
	rejected := valid
	rejected.UUID = ""
	rejected.Rejected = true
	if err := validateContentReceipt(q, rejected, true); err != nil {
		t.Fatal(err)
	}
	if err := validateContentReceipt(q, rejected, false); err == nil {
		t.Fatal("rejected preparation accepted for runtime consumption")
	}
	for name, mutate := range map[string]func(*core.PreparationReceipt){
		"volume":  func(r *core.PreparationReceipt) { r.VolumeUID = "replacement" },
		"epoch":   func(r *core.PreparationReceipt) { r.Epoch-- },
		"boot":    func(r *core.PreparationReceipt) { r.BootID = "previous" },
		"node":    func(r *core.PreparationReceipt) { r.NodeUID = "other" },
		"uuid":    func(r *core.PreparationReceipt) { r.UUID = "other" },
		"device":  func(r *core.PreparationReceipt) { r.DeviceIdentity = "other" },
		"release": func(r *core.PreparationReceipt) { r.Released = false },
		"plan":    func(r *core.PreparationReceipt) { r.PlanDigest = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			r := valid
			mutate(&r)
			if validateContentReceipt(q, r, true) == nil {
				t.Fatal("accepted mismatched evidence")
			}
		})
	}
}
