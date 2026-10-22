package overlaybd

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/metadata"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

type blockMountFixture struct{ published, unpublished int }

func (m *blockMountFixture) Publish(context.Context, DeviceEvidence, string, bool) error {
	m.published++
	return nil
}
func (m *blockMountFixture) Unpublish(context.Context, DeviceEvidence, string) error {
	m.unpublished++
	return nil
}

func TestCSINodeResponseLossBorrowAndCleanupFence(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	wal, err := OpenSessions(filepath.Join(t.TempDir(), "wal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	backend := new(deviceBackend)
	mounts := new(blockMountFixture)
	n := CSINode{Root: t.TempDir(), Authorization: NodeAuthorization{Key: pub, Node: "node", NodeUID: "uid", Boot: "boot", CSINode: "csi-node"}, Operations: &NodeOperations{Sessions: wal, Backend: backend}, Mounts: mounts}
	handle, _ := VolumeHandle("repo", "volume")
	cap := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}
	pc := map[string]string{core.StageScopeKey: core.StageScopeBinding, "overlaybd.ssf.dev/head": core.Hash("head"), "overlaybd.ssf.dev/attachment": core.Hash("grant"), "overlaybd.ssf.dev/fs-uuid": "29aff887-2fe8-431a-a1a5-ad5766b7bf0a", "overlaybd.ssf.dev/capacity": strconv.Itoa(1 << 30)}
	b := core.Binding{ID: "binding", OwnerUID: "owner", RunUID: "run", Epoch: 1, Node: "node", Tenant: "tenant", Sandbox: "sandbox", Volume: core.Volume{UID: "pv", Driver: DriverName, Handle: handle, Capability: cap}}
	p := core.Plan{ID: "plan", Operation: "op", Node: "node", NodeUID: "uid", BootID: "boot", Tenant: b.Tenant, Sandbox: b.Sandbox, Expires: time.Now().Add(time.Hour), NodeIDs: map[string]string{DriverName: "csi-node"}, Bindings: []core.Binding{b}, PublishContexts: map[string]map[string]string{b.ID: pc}}
	token, _ := auth.Sign(p, key)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(auth.NodePlanMetadata, token, auth.NodeBindingMetadata, b.ID))
	stage := filepath.Join(n.Root, "stage", core.Hash(b.Volume.Key()+"/"+b.ID))
	target := filepath.Join(n.Root, "publish", core.Hash(b.ID))
	request := &csi.NodeStageVolumeRequest{VolumeId: handle, StagingTargetPath: stage, VolumeCapability: cap, PublishContext: pc}
	if _, err = n.NodeStageVolume(ctx, request); err == nil {
		t.Fatal("lost native reply reported success")
	}
	if _, err = n.NodeStageVolume(ctx, request); err != nil {
		t.Fatal(err)
	}
	if backend.creates != 1 {
		t.Fatal("duplicate native device")
	}
	publish := &csi.NodePublishVolumeRequest{VolumeId: handle, StagingTargetPath: stage, TargetPath: target, VolumeCapability: cap, PublishContext: pc}
	if _, err = n.NodePublishVolume(ctx, publish); err != nil {
		t.Fatal(err)
	}
	s, err := wal.Get(core.Hash("overlaybd/session/" + b.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Publishes[target].Borrowed {
		t.Fatal("runtime publication returned without borrow fence")
	}
	unpublish := &csi.NodeUnpublishVolumeRequest{VolumeId: handle, TargetPath: target}
	if _, err = n.NodeUnpublishVolume(ctx, unpublish); err == nil || mounts.unpublished != 0 {
		t.Fatal("removed live runtime publication")
	}
	proof := StopProof{SessionID: s.Intent.ID, RunID: s.Intent.RunID, Epoch: s.Intent.Epoch, DeviceIdentity: s.Evidence.DeviceIdentity, Digest: core.Hash("verified-stop")}
	if _, err = wal.ReturnBorrow(s, target, proof); err != nil {
		t.Fatal(err)
	}
	if _, err = n.NodeUnpublishVolume(ctx, unpublish); err != nil {
		t.Fatal(err)
	}
	if _, err = n.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: handle, StagingTargetPath: stage}); err == nil {
		t.Fatal("device released without final publication")
	}
	publish.TargetPath = filepath.Join(n.Root, "publish", "untrusted")
	if _, err = n.NodePublishVolume(ctx, publish); err == nil {
		t.Fatal("unsigned target accepted")
	}
	if mounts.published != 1 {
		t.Fatal("unexpected mount side effect")
	}
}
