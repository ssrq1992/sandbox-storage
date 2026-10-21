package consume

import (
	"context"
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/metadata"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
)

// AbortUnpublished compensates an interrupted Prepare before a node plan was
// ever delivered. The integration MUST durably revoke dispatch and prove that
// no node/runtime can start this operation. A timed-out node RPC is not proof.
// An ambiguous binding/attach is resolved on its original node before release.
func (c *Coordinator) AbortUnpublished(ctx context.Context, op string, noDispatch func(context.Context) error) error {
	if noDispatch == nil {
		return errors.New("durable no-dispatch proof required")
	}
	return c.Ledger.Exclusive(ctx, "operation/"+op, func(ctx context.Context) error {
		if e := noDispatch(ctx); e != nil {
			return e
		}
		state, e := c.Ledger.Operation(ctx, op)
		if errors.Is(e, pgx.ErrNoRows) {
			return nil
		}
		if e != nil {
			return e
		}
		if state.State == "Released" {
			return nil
		}
		var mounts []core.Mount
		e = c.Ledger.ReadStep(ctx, "resolve/"+op, &mounts)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) && !errors.Is(e, core.ErrPending) {
			return e
		}
		for _, m := range mounts {
			p, e := c.Kube.API.CoreV1().PersistentVolumeClaims(m.PVC.Namespace).Get(ctx, m.PVC.Name, metav1.GetOptions{})
			if e != nil {
				return e
			}
			if string(p.UID) != m.PVC.UID {
				return core.ErrConflict
			}
			if p.Status.Phase != v1.ClaimBound && (p.Spec.VolumeName != "" || p.Annotations["volume.kubernetes.io/selected-node"] != "") {
				return core.ErrPending
			}
		}
		bs, e := c.Ledger.Bindings(ctx, op)
		if e != nil {
			return e
		}
		// Complete only already-issued ambiguous Attach intents. Never attach a
		// previously untouched volume merely to undo a partial multi-volume acquire.
		for _, b := range bs {
			state, nodeID, e := c.Ledger.AttachmentState(ctx, b.Volume.Key(), b.Node)
			if e != nil {
				return e
			}
			if state != "Attaching" {
				continue
			}
			s, e := c.Drivers.Controller(ctx, b.Volume.Driver)
			if e != nil {
				return e
			}
			_, e = c.Ledger.EnsureAttachment(ctx, b.Volume.Key(), b.Node, core.Digest(b.Volume), nodeID, func() (map[string]string, error) {
				if !s.Attach {
					return map[string]string{}, nil
				}
				sec, e := c.Kube.Secrets(ctx, b.Volume, "controller-publish")
				if e != nil {
					return nil, e
				}
				r, e := s.Controller.ControllerPublishVolume(metadata.AppendToOutgoingContext(ctx, "ssf-binding-id", b.ID), &csi.ControllerPublishVolumeRequest{VolumeId: b.Volume.Handle, NodeId: nodeID, VolumeCapability: b.Volume.Capability, Readonly: b.Volume.ReadOnly, VolumeContext: b.Volume.Context, Secrets: sec})
				if e != nil {
					return nil, e
				}
				return r.PublishContext, nil
			})
			if e != nil {
				return e
			}
		}
		if e = c.Ledger.BeginRelease(ctx, op); e != nil {
			return e
		}
		for _, b := range bs {
			if b.State == "Released" {
				continue
			}
			e = c.Ledger.Exclusive(ctx, "volume/"+b.Volume.Key(), func(ctx context.Context) error {
				n, e := c.Ledger.CountOther(ctx, b.Volume.Key(), b.Node, b.ID)
				if e != nil {
					return e
				}
				if n == 0 {
					state, _, e := c.Ledger.AttachmentState(ctx, b.Volume.Key(), b.Node)
					if e != nil {
						return e
					}
					if state != "" && state != "Detached" {
						s, e := c.Drivers.Controller(ctx, b.Volume.Driver)
						if e != nil {
							return e
						}
						e = c.Ledger.DetachAttachment(ctx, b.Volume.Key(), b.Node, func(nodeID string) error {
							if !s.Attach {
								return nil
							}
							sec, e := c.Kube.Secrets(ctx, b.Volume, "controller-publish")
							if e != nil {
								return e
							}
							_, e = s.Controller.ControllerUnpublishVolume(metadata.AppendToOutgoingContext(ctx, "ssf-binding-id", b.ID), &csi.ControllerUnpublishVolumeRequest{VolumeId: b.Volume.Handle, NodeId: nodeID, Secrets: sec})
							return e
						})
						if e != nil {
							return e
						}
					}
				}
				return c.Ledger.ReleaseBinding(ctx, b.ID)
			})
			if e != nil {
				return e
			}
		}
		return c.Ledger.SetOperation(ctx, op, "Released", "")
	})
}
