package overlaybd

import (
	"context"
	"crypto/ed25519"
	"errors"
	"maps"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

// NodeAuthorization verifies the complete coordinator plan, not user-supplied
// volume attributes or an untrusted annotation. Node is the SSF physical node;
// CSINode is the identity advertised by this driver on that node.
type NodeAuthorization struct {
	Key                          ed25519.PublicKey
	Node, NodeUID, Boot, CSINode string
}

func (a NodeAuthorization) Authorize(ctx context.Context, handle string, capability *csi.VolumeCapability, values, publish map[string]string) (SessionIntent, error) {
	fail := func() (SessionIntent, error) {
		return SessionIntent{}, errors.New("invalid OverlayBD node authorization")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	tokens, bindings := md.Get(auth.NodePlanMetadata), md.Get(auth.NodeBindingMetadata)
	if len(tokens) != 1 || len(bindings) != 1 || a.Node == "" || a.NodeUID == "" || a.Boot == "" || a.CSINode == "" {
		return fail()
	}
	p, err := auth.Verify(tokens[0], a.Key, a.Node, a.Boot, time.Now())
	if err != nil {
		return SessionIntent{}, err
	}
	if p.NodeUID != a.NodeUID || p.NodeIDs[DriverName] != a.CSINode || p.Operation == "" {
		return fail()
	}
	var b core.Binding
	count := 0
	for _, candidate := range p.Bindings {
		if candidate.ID == bindings[0] {
			b = candidate
			count++
		}
	}
	if count != 1 || b.Node != p.Node || b.Tenant != p.Tenant || b.Sandbox != p.Sandbox || b.Volume.Driver != DriverName || b.Volume.Handle != handle || capability == nil || capability.GetBlock() == nil || !proto.Equal(capability, b.Volume.Capability) || !maps.Equal(values, b.Volume.Context) || !maps.Equal(publish, p.PublishContexts[b.ID]) || publish[core.StageScopeKey] != core.StageScopeBinding || !validDigest(publish["overlaybd.ssf.dev/head"]) || !validDigest(publish["overlaybd.ssf.dev/attachment"]) {
		return fail()
	}
	repo, volume, err := ParseVolumeHandle(handle)
	if err != nil {
		return SessionIntent{}, err
	}
	purpose := "runtime"
	if p.Purpose == core.PurposePreparation {
		purpose = "preparation"
	}
	// Renewal changes only token ID/expiry; it cannot authorize a second device.
	p.ID = ""
	p.Expires = time.Time{}
	intent := SessionIntent{Purpose: purpose, ID: core.Hash("overlaybd/session/" + b.ID), RepositoryID: repo, VolumeID: volume, OperationID: core.Hash("overlaybd/prepare/" + b.ID), OwnerID: b.OwnerUID, RunID: b.RunUID, Epoch: b.Epoch, NodeID: a.CSINode, HeadRevision: publish["overlaybd.ssf.dev/head"], AuthorizationDigest: core.Digest(p), Readonly: b.Volume.ReadOnly}
	if !validIntent(intent) {
		return fail()
	}
	return intent, nil
}
