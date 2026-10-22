package overlaybd

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/metadata"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

func TestNodePlanAuthorizationAndRenewal(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	a := NodeAuthorization{Key: pub, Node: "node", NodeUID: "uid", Boot: "boot", CSINode: "csi-node"}
	handle, _ := VolumeHandle("repo", "volume")
	capability := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}
	pc := map[string]string{core.StageScopeKey: core.StageScopeBinding, "overlaybd.ssf.dev/head": core.Hash("head"), "overlaybd.ssf.dev/attachment": core.Hash("grant")}
	b := core.Binding{ID: "binding", OwnerUID: "owner", RunUID: "run", Epoch: 1, Node: a.Node, Tenant: "tenant", Sandbox: "sandbox", Volume: core.Volume{Driver: DriverName, Handle: handle, Capability: capability}}
	p := core.Plan{ID: "plan", Operation: "op", Node: a.Node, NodeUID: a.NodeUID, BootID: a.Boot, Tenant: b.Tenant, Sandbox: b.Sandbox, Expires: time.Now().Add(time.Hour), NodeIDs: map[string]string{DriverName: a.CSINode}, Bindings: []core.Binding{b}, PublishContexts: map[string]map[string]string{b.ID: pc}}
	authorize := func(plan core.Plan) (SessionIntent, error) {
		token, _ := auth.Sign(plan, key)
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(auth.NodePlanMetadata, token, auth.NodeBindingMetadata, b.ID))
		return a.Authorize(ctx, handle, capability, nil, pc)
	}
	first, err := authorize(p)
	if err != nil {
		t.Fatal(err)
	}
	p.ID = "renewed"
	p.Expires = p.Expires.Add(time.Hour)
	renewed, err := authorize(p)
	if err != nil || first != renewed {
		t.Fatalf("renewal changed session: %+v %v", renewed, err)
	}
	for _, tc := range []struct {
		name   string
		change func(*core.Plan)
	}{
		{"wrong boot", func(p *core.Plan) { p.BootID = "other" }},
		{"wrong node UID", func(p *core.Plan) { p.NodeUID = "other" }},
		{"expired", func(p *core.Plan) { p.Expires = time.Now().Add(-time.Hour) }},
		{"wrong CSI node", func(p *core.Plan) { p.NodeIDs = map[string]string{DriverName: "other"} }},
		{"duplicate binding", func(p *core.Plan) { p.Bindings = append(p.Bindings, p.Bindings[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := p
			tc.change(&q)
			if _, err := authorize(q); err == nil {
				t.Fatal("accepted invalid scope")
			}
		})
	}
	token, _ := auth.Sign(p, key)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(auth.NodePlanMetadata, token, auth.NodeBindingMetadata, b.ID, auth.NodeBindingMetadata, b.ID))
	if _, err := a.Authorize(ctx, handle, capability, nil, pc); err == nil {
		t.Fatal("ambiguous metadata accepted")
	}
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs(auth.NodePlanMetadata, token, auth.NodeBindingMetadata, b.ID))
	if _, err := a.Authorize(ctx, handle, capability, map[string]string{"injected": "value"}, pc); err == nil {
		t.Fatal("unsigned context accepted")
	}
	altered := map[string]string{core.StageScopeKey: core.StageScopeBinding, "overlaybd.ssf.dev/head": core.Hash("other"), "overlaybd.ssf.dev/attachment": core.Hash("grant")}
	if _, err := a.Authorize(ctx, handle, capability, nil, altered); err == nil {
		t.Fatal("unsigned head accepted")
	}
}
