package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"maps"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/ledger"
)

type LedgerAuthorizer struct {
	Store  *ledger.Store
	Driver string
}

func (a *LedgerAuthorizer) binding(ctx context.Context, handle, nodeID string, detach bool) (core.Binding, error) {
	var b core.Binding
	md, _ := metadata.FromIncomingContext(ctx)
	ids := md.Get("ssf-binding-id")
	if len(ids) != 1 {
		return b, errors.New("consumption binding metadata required")
	}
	var data []byte
	var state, attachmentState, actualNode string
	e := a.Store.Pool.QueryRow(ctx, `SELECT b.descriptor,b.state,a.state,a.node_id FROM ssf_bindings b JOIN ssf_attachments a ON a.volume_key=b.volume_key AND a.node=b.node WHERE b.id=$1`, ids[0]).Scan(&data, &state, &attachmentState, &actualNode)
	if e != nil {
		return b, e
	}
	if e = json.Unmarshal(data, &b); e != nil {
		return b, e
	}
	if b.Volume.Driver != a.Driver || b.Volume.Handle != handle || actualNode != nodeID {
		return b, core.ErrConflict
	}
	if detach {
		if state != "Releasing" || attachmentState != "Detaching" {
			return b, core.ErrConflict
		}
	} else if state != "Acquired" || attachmentState != "Attaching" {
		return b, core.ErrConflict
	}
	return b, nil
}
func (a *LedgerAuthorizer) Publish(ctx context.Context, q *csi.ControllerPublishVolumeRequest) error {
	b, e := a.binding(ctx, q.VolumeId, q.NodeId, false)
	if e != nil {
		return e
	}
	if b.Volume.ReadOnly != q.Readonly || !proto.Equal(b.Volume.Capability, q.VolumeCapability) || !maps.Equal(b.Volume.Context, q.VolumeContext) {
		return core.ErrConflict
	}
	return nil
}
func (a *LedgerAuthorizer) Unpublish(ctx context.Context, q *csi.ControllerUnpublishVolumeRequest) error {
	b, e := a.binding(ctx, q.VolumeId, q.NodeId, true)
	if e != nil {
		return e
	}
	n, e := a.Store.CountOther(ctx, b.Volume.Key(), b.Node, b.ID)
	if e != nil {
		return e
	}
	if n != 0 {
		return core.ErrConflict
	}
	return nil
}
