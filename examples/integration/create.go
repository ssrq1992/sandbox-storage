// Package integration demonstrates lifecycle embedding without project-specific code.
package integration

import (
	"context"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
	sdk "ssf.dev/ssf/sdk/go"
)

type Runtime interface {
	runtime.Adapter
	Evidence(context.Context, string, string) (string, error)
}

func Create(ctx context.Context, c *consume.Coordinator, n *sdk.Client, r Runtime, q consume.Request) (consume.Result, error) {
	result, e := c.Prepare(ctx, q)
	if e != nil {
		return result, e
	}
	prepared, e := n.Prepare(ctx, result.Operation, result.SignedPlan)
	if e != nil {
		return result, e
	}
	eps := make([]core.Endpoint, 0, len(prepared.Endpoints))
	for _, ep := range prepared.Endpoints {
		x := core.Endpoint{BindingID: ep.BindingUid, Epoch: ep.Epoch, GuestPath: ep.GuestPath, ReadOnly: ep.Readonly, NodeBootID: ep.NodeBootId, VolumeUID: ep.VolumeUid, Generation: ep.VolumeGeneration, NodeUID: ep.NodeUid, Revision: ep.CapabilityRevision}
		if b := ep.GetBlockDevice(); b != nil {
			x.Kind = "BlockDevice"
			x.Path = b.ManagedTarget
			x.Identity = b.DeviceIdentity
			x.Major = b.Major
			x.Minor = b.Minor
			x.Size = b.SizeBytes
		} else if f := ep.GetFileTree(); f != nil {
			x.Kind = "FileTree"
			x.Path = f.ManagedPath
			x.Identity = f.MountIdentity
		}
		eps = append(eps, x)
	}
	if e = r.AttachPrepared(ctx, q.Sandbox, eps); e != nil {
		return result, e
	}
	if e = r.WaitGuestReady(ctx, q.Sandbox); e != nil {
		return result, e
	}
	proof, e := r.Evidence(ctx, q.Sandbox, "ready")
	if e != nil {
		return result, e
	}
	_, e = n.Node.MarkRuntimeReady(ctx, &v1.RuntimeRequest{OperationId: result.Operation, SandboxInstanceUid: q.Sandbox, Evidence: proof})
	if e != nil {
		return result, e
	}
	if e = c.ConfirmReady(ctx, result, q.Sandbox, proof); e != nil {
		return result, e
	}
	return result, c.Commit(ctx, result)
}

// Cleanup is also the compensation handler for partial Create failures. Retain
// the returned operation in the project's journal even when Create fails.
func Cleanup(ctx context.Context, c *consume.Coordinator, n *sdk.Client, r Runtime, result consume.Result, sandbox string) error {
	if e := r.DetachAndConfirm(ctx, sandbox); e != nil {
		return e
	}
	proof, e := r.Evidence(ctx, sandbox, "detached")
	if e != nil {
		return e
	}
	if e = c.ConfirmStopped(ctx, result, proof); e != nil {
		return e
	}
	return c.Release(ctx, result.Operation, func(ctx context.Context, id string) error {
		_, e := n.Node.ReleaseVolumes(ctx, &v1.RuntimeRequest{OperationId: id, SandboxInstanceUid: sandbox, Evidence: proof, SignedNodePlan: result.SignedPlan})
		return e
	})
}
