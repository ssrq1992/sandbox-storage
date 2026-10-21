//go:build linux

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"os"
	"path/filepath"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	driver "ssf.dev/ssf/drivers/csi"
	"testing"
	"time"
)

type realSessions struct{ s *driver.Session }

func (s realSessions) Node(context.Context, string) (*driver.Session, error) { return s.s, nil }
func TestLinuxRealPublish(t *testing.T) {
	endpoint := os.Getenv("SSF_TEST_CSI_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires root Linux CSI fixture")
	}
	mode := os.Getenv("SSF_TEST_CSI_MODE")
	name := "test." + mode + ".ssf.dev"
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, e := driver.Dial(ctx, endpoint, name, "node", "test", nil)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Conn.Close()
	controller, e := driver.Dial(ctx, endpoint, name, "controller", "test", nil)
	if e != nil {
		t.Fatal(e)
	}
	defer controller.Conn.Close()
	cap := &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}
	if mode == "block" {
		cap.AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}
	} else {
		cap.AccessType = &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}
	}
	created, e := controller.Controller.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: t.Name() + time.Now().Format("150405.000000"), VolumeCapabilities: []*csi.VolumeCapability{cap}, CapacityRange: &csi.CapacityRange{RequiredBytes: 64 << 20}})
	if e != nil {
		t.Fatal(e)
	}
	v := core.Volume{Driver: name, Handle: created.Volume.VolumeId, Capability: cap}
	pubctx := map[string]string{}
	if controller.Attach {
		a, e := controller.Controller.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{VolumeId: v.Handle, NodeId: s.NodeID, VolumeCapability: cap})
		if e != nil {
			t.Fatal(e)
		}
		pubctx = a.PublishContext
	}
	root := t.TempDir()
	wal, e := OpenWAL(filepath.Join(root, "wal.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer wal.Close()
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	engine := &Engine{Root: root, WAL: wal, Sessions: realSessions{s}, Evidence: evidence{}, Key: pub, NodeID: "n", BootID: "boot"}
	p := core.Plan{ID: "p", Operation: "op", Tenant: "t", Sandbox: "vm", Node: "n", BootID: "boot", Expires: time.Now().Add(time.Minute), NodeIDs: map[string]string{name: s.NodeID}, PublishContexts: map[string]map[string]string{"b": pubctx}, Bindings: []core.Binding{{ID: "b", Epoch: 1, Node: "n", Sandbox: "vm", Tenant: "t", Volume: v, GuestPath: "/data"}}}
	token, _ := auth.Sign(p, key)
	r, e := engine.Prepare(ctx, "op", token)
	if e != nil {
		t.Fatal(e)
	}
	if mode == "file" {
		if e = os.WriteFile(filepath.Join(r.Endpoints[0].Path, "sentinel"), []byte("ssf"), 0600); e != nil {
			t.Fatal(e)
		}
	} else if r.Endpoints[0].Size < 64<<20 {
		t.Fatal("wrong block capacity")
	}
	if _, e = engine.Release(ctx, "op", "vm", "verified-test-state"); e != nil {
		t.Fatal(e)
	}
	if controller.Attach {
		if _, e = controller.Controller.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: v.Handle, NodeId: s.NodeID}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = controller.Controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: v.Handle}); e != nil {
		t.Fatal(e)
	}
}
