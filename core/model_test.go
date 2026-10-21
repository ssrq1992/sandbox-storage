package core

import (
	"encoding/json"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestCapabilityPersistence(t *testing.T) {
	for _, cap := range []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}, {AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4", MountFlags: []string{"noatime"}}}}} {
		v := Volume{Driver: "test", Handle: "../unsafe", Capability: cap}
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		var got Volume
		if e = json.Unmarshal(b, &got); e != nil {
			t.Fatal(e)
		}
		if !proto.Equal(v.Capability, got.Capability) || v.Key() != got.Key() {
			t.Fatal("lost CSI capability or identity")
		}
		if len(got.Key()) != 64 {
			t.Fatal("unsafe path identity")
		}
	}
}
func TestMountValidation(t *testing.T) {
	for _, p := range []string{"relative", "/", "/a/../b"} {
		if ValidateMounts([]Mount{{PVC: PVCRef{Namespace: "n", Name: "p"}, MountID: "m", GuestPath: p}}) == nil {
			t.Fatalf("accepted %s", p)
		}
	}
}
