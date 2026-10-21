package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"path/filepath"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	driver "ssf.dev/ssf/drivers/csi"
	"testing"
	"time"
)

type fakeNode struct {
	csi.NodeClient
	stage, publish, unpublish, unstage int
	failPublish                        bool
}

func (f *fakeNode) NodeStageVolume(context.Context, *csi.NodeStageVolumeRequest, ...grpc.CallOption) (*csi.NodeStageVolumeResponse, error) {
	f.stage++
	return &csi.NodeStageVolumeResponse{}, nil
}
func (f *fakeNode) NodePublishVolume(context.Context, *csi.NodePublishVolumeRequest, ...grpc.CallOption) (*csi.NodePublishVolumeResponse, error) {
	f.publish++
	if f.failPublish {
		return nil, context.DeadlineExceeded
	}
	return &csi.NodePublishVolumeResponse{}, nil
}
func (f *fakeNode) NodeUnpublishVolume(context.Context, *csi.NodeUnpublishVolumeRequest, ...grpc.CallOption) (*csi.NodeUnpublishVolumeResponse, error) {
	f.unpublish++
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
func (f *fakeNode) NodeUnstageVolume(context.Context, *csi.NodeUnstageVolumeRequest, ...grpc.CallOption) (*csi.NodeUnstageVolumeResponse, error) {
	f.unstage++
	return &csi.NodeUnstageVolumeResponse{}, nil
}

type sessions struct{ n *fakeNode }

func (s sessions) Node(context.Context, string) (*driver.Session, error) {
	return &driver.Session{Node: s.n, NodeID: "csi-node", Stage: true}, nil
}

type evidence struct{}

func (evidence) Verify(_ context.Context, _, _, proof string) error {
	if proof != "verified-test-state" {
		return errors.New("unverified")
	}
	return nil
}
func setup(t testing.TB) (*Engine, *fakeNode, ed25519.PrivateKey) {
	t.Helper()
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	root := t.TempDir()
	wal, e := OpenWAL(filepath.Join(root, "state.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { wal.Close() })
	f := &fakeNode{}
	n := &Engine{WAL: wal, Sessions: sessions{f}, Evidence: evidence{}, Key: pub, NodeID: "n", BootID: "b", Root: root, Inspect: func(p string, block, ro bool) (core.Endpoint, error) {
		return core.Endpoint{Kind: "FileTree", Path: p, Identity: "test-mount", ReadOnly: ro}, nil
	}}
	return n, f, key
}
func plan(key ed25519.PrivateKey, op string) string {
	p := core.Plan{ID: op, Operation: op, Tenant: "t", Sandbox: op, Node: "n", BootID: "b", Expires: time.Now().Add(time.Hour), NodeIDs: map[string]string{"test": "csi-node"}, Bindings: []core.Binding{{ID: op, Node: "n", Sandbox: op, Tenant: "t", Epoch: 1, GuestPath: "/data", Volume: core.Volume{UID: "pv", Driver: "test", Handle: "disk", ReadOnly: true, Capability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY}}}}}}
	token, _ := auth.Sign(p, key)
	return token
}
func TestSharedStageAndReleaseEvidence(t *testing.T) {
	n, f, key := setup(t)
	ctx := context.Background()
	for _, op := range []string{"one", "two"} {
		if _, e := n.Prepare(ctx, op, plan(key, op)); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := n.Release(ctx, "one", "one", "forged"); e == nil {
		t.Fatal("accepted unverified release")
	}
	if f.unpublish != 0 {
		t.Fatal("unpublished without evidence")
	}
	if _, e := n.Release(ctx, "one", "one", "verified-test-state"); e != nil {
		t.Fatal(e)
	}
	if f.unstage != 0 {
		t.Fatal("unstaged shared resource")
	}
	if _, e := n.Release(ctx, "two", "two", "verified-test-state"); e != nil {
		t.Fatal(e)
	}
	if f.unstage != 1 {
		t.Fatalf("unstage count %d", f.unstage)
	}
	if _, e := n.Release(ctx, "two", "two", "verified-test-state"); e != nil {
		t.Fatal(e)
	}
	if f.unstage != 1 {
		t.Fatal("repeat release executed RPC")
	}
}
func TestUnknownPublishAndRetry(t *testing.T) {
	n, f, key := setup(t)
	ctx := context.Background()
	f.failPublish = true
	token := plan(key, "op")
	if _, e := n.Prepare(ctx, "op", token); e == nil {
		t.Fatal("expected timeout")
	}
	r, e := n.WAL.Get("op")
	if e != nil || r.Steps["publish/op"] != "Intent" {
		t.Fatal("unknown result lost")
	}
	f.failPublish = false
	if _, e = n.Prepare(ctx, "op", token); e != nil {
		t.Fatal(e)
	}
	if f.stage != 1 || f.publish != 2 {
		t.Fatal("retry did not resume correct step")
	}
	if _, e = n.Prepare(ctx, "op", token); e != nil {
		t.Fatal(e)
	}
	if f.publish != 2 {
		t.Fatal("duplicate publish")
	}
}
func TestWALRestart(t *testing.T) {
	file := filepath.Join(t.TempDir(), "wal.db")
	w, e := OpenWAL(file)
	if e != nil {
		t.Fatal(e)
	}
	r := Record{Plan: core.Plan{Operation: "o"}, State: "Releasing", Steps: map[string]string{"publish": "Intent"}}
	if e = w.Save(r); e != nil {
		t.Fatal(e)
	}
	w.Close()
	w, e = OpenWAL(file)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	got, e := w.Get("o")
	if e != nil || got.State != "Releasing" || got.Steps["publish"] != "Intent" {
		t.Fatal("WAL recovery lost intent")
	}
}
