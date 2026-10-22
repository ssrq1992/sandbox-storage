// Package consume implements shared storage consumption, hosted by the public
// coordinator service or an exclusively authorized runtime integration.
package consume

import (
	"context"
	"crypto/ed25519"
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/metadata"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	driver "ssf.dev/ssf/drivers/csi"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
	"ssf.dev/ssf/runtime"
	"time"
)

type Controllers interface {
	Controller(context.Context, string) (*driver.Session, error)
	NodeID(context.Context, string, string) (string, error)
}
type Authorizer interface {
	Authorize(context.Context, string, core.PVCRef, bool) error
}
type Coordinator struct {
	EnsureVolumeReady func(context.Context, core.Volume, core.Placement, string, string) (core.PreparationReceipt, error)
	Kube              *kube.Client
	Ledger            *ledger.Store
	Placement         runtime.PlacementProvider
	Drivers           Controllers
	Authorizer        Authorizer
	SigningKey        ed25519.PrivateKey
	PlanTTL           time.Duration
	Evidence          runtime.EvidenceVerifier
}
type Request struct {
	Preparation         *core.PreparationSpec `json:",omitempty"`
	ID, Tenant, Sandbox string
	OwnerUID, RunUID    string `json:",omitempty"`
	Mounts              []core.Mount
}
type Result struct {
	Operation  string
	Placement  core.Placement
	Plan       core.Plan
	SignedPlan string
}
type attachment struct {
	NodeID   string
	Context  map[string]string
	Required bool
}

func (c *Coordinator) Prepare(ctx context.Context, q Request) (Result, error) {
	var result Result
	op := core.Hash(q.Tenant + "\x00" + q.Sandbox + "\x00" + q.ID)
	err := c.Ledger.Exclusive(ctx, "operation/"+op, func(ctx context.Context) error { var err error; result, err = c.prepare(ctx, q); return err })
	return result, err
}
func (c *Coordinator) prepare(ctx context.Context, q Request) (Result, error) {
	var result Result
	if q.ID == "" || q.Tenant == "" || (q.Sandbox == "" && q.Preparation == nil) {
		return result, errors.New("request identity required")
	}
	if q.Preparation != nil && (q.Sandbox != "" || len(q.Mounts) != 1 || q.OwnerUID == "" || q.RunUID == "") {
		return result, errors.New("invalid preparation request scope")
	}
	if err := core.ValidateMounts(q.Mounts); err != nil {
		return result, err
	}
	if c.Authorizer == nil {
		return result, errors.New("authorizer required")
	}
	if err := c.Kube.CheckProtection(ctx); err != nil {
		return result, err
	}
	// Persist the caller's request before resolving mutable names. Retry identity
	// is scoped to tenant and sandbox, not a tenant-controlled bare request ID.
	op := core.Hash(q.Tenant + "\x00" + q.Sandbox + "\x00" + q.ID)
	result.Operation = op
	if err := c.Ledger.Begin(ctx, op, core.Digest(q)); err != nil {
		return result, err
	}
	if err := c.Ledger.Step(ctx, "request/"+op, core.Digest(q), func(ctx context.Context) (any, error) { return q, nil }, nil); err != nil {
		return result, err
	}
	pinned := q.Mounts
	err := c.Ledger.Step(ctx, "resolve/"+op, core.Digest(q), func(ctx context.Context) (any, error) {
		out := append([]core.Mount(nil), q.Mounts...)
		for i, m := range out {
			if m.VolumeUID != "" {
				v, e := c.Ledger.Lookup(ctx, m.VolumeUID)
				if e != nil {
					return nil, e
				}
				m.PVC = v.PVC
			}
			if err := c.Authorizer.Authorize(ctx, q.Tenant, m.PVC, !m.ReadOnly); err != nil {
				return nil, err
			}
			ref, err := c.Kube.Pin(ctx, m.PVC)
			if err != nil {
				return nil, err
			}
			out[i].PVC = ref
		}
		return out, nil
	}, &pinned)
	if err != nil {
		return result, err
	}
	volumes := make([]core.Volume, len(pinned))
	for i, m := range pinned {
		if err := c.Authorizer.Authorize(ctx, q.Tenant, m.PVC, !m.ReadOnly); err != nil {
			return result, err
		}
		v, err := c.Kube.Resolve(ctx, m.PVC, m.ReadOnly)
		if err != nil && !errors.Is(err, core.ErrPending) {
			return result, err
		}
		if errors.Is(err, core.ErrPending) {
			v = core.Volume{PVC: m.PVC, UID: m.PVC.UID, ReadOnly: m.ReadOnly}
		}
		volumes[i] = v
	}
	var place core.Placement
	err = c.Ledger.Step(ctx, "placement/"+op, core.Digest(pinned), func(ctx context.Context) (any, error) { return c.Placement.Reserve(ctx, op, volumes) }, &place)
	if err != nil {
		return result, err
	}
	result.Placement = place
	if err = c.validatePlacement(ctx, &place); err != nil {
		return result, err
	}
	bs := make([]core.Binding, len(volumes))
	prepared := map[string]core.PreparationReceipt{}
	owner, run := q.OwnerUID, q.RunUID
	if owner == "" {
		owner = q.Sandbox
	}
	if run == "" {
		run = q.Sandbox
	}
	epoch, err := c.Ledger.RunEpoch(ctx, owner, run)
	if err != nil {
		return result, err
	}
	boundClient := *c.Kube
	boundClient.Selection = c.Ledger
	for i, v := range volumes {
		if v.Handle == "" {
			if err = c.Ledger.AuthorizeNode(ctx, pinned[i].PVC.UID, place.Node, place.ID, place.Expires); err != nil {
				return result, err
			}
			v, err = boundClient.EnsureBound(ctx, pinned[i].PVC, place.Node, pinned[i].ReadOnly, func(ctx context.Context) error {
				if e := c.validatePlacement(ctx, &place); e != nil {
					return e
				}
				return c.Ledger.AuthorizeNode(ctx, pinned[i].PVC.UID, place.Node, place.ID, place.Expires)
			})
			if err != nil {
				return result, err
			}
			v.ReadOnly = v.ReadOnly || pinned[i].ReadOnly
		}
		if err = c.Kube.CheckTopology(ctx, v, place.Node); err != nil {
			return result, err
		}
		if err = c.Kube.CheckOwnership(ctx, v); err != nil {
			return result, err
		}
		if err = c.Ledger.Register(ctx, v); err != nil {
			return result, err
		}
		if q.Preparation == nil && c.EnsureVolumeReady != nil {
			receipt, e := c.EnsureVolumeReady(ctx, v, place, owner, run)
			if e != nil {
				err = e
				return result, err
			}
			if receipt.Protocol != "" {
				if receipt.Rejected || !receipt.Released || receipt.UUID == "" || receipt.VolumeUID != v.PVC.UID {
					return result, core.ErrConflict
				}
				prepared[core.Hash(op+"/"+pinned[i].MountID)] = receipt
			}
			if err = c.validatePlacement(ctx, &place); err != nil {
				return result, err
			}
		}
		bs[i] = core.Binding{ID: core.Hash(op + "/" + pinned[i].MountID), Volume: v, Sandbox: q.Sandbox, OwnerUID: owner, RunUID: run, Tenant: q.Tenant, Node: place.Node, Epoch: epoch, MountID: pinned[i].MountID, GuestPath: pinned[i].GuestPath, State: "Acquired"}
	}
	if err = c.Ledger.Acquire(ctx, op, core.Digest(q), bs); err != nil {
		return result, err
	}
	ttl := c.PlanTTL
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	p := core.Plan{ID: core.Hash(op + "/plan"), Operation: op, Tenant: q.Tenant, Sandbox: q.Sandbox, OwnerUID: owner, RunUID: run, Node: place.Node, NodeUID: place.NodeUID, BootID: place.BootID, Expires: time.Now().Add(ttl), Bindings: bs, PublishContexts: map[string]map[string]string{}, NodeIDs: map[string]string{}, Revision: "v1"}
	if len(prepared) > 0 {
		p.PreparedVolumes = prepared
	}
	if q.Preparation != nil {
		p.Purpose = core.PurposePreparation
		spec := *q.Preparation
		spec.PVC = bs[0].Volume.PVC
		spec.PVUID = bs[0].Volume.PVUID
		p.Preparation = &spec
	}
	for _, b := range bs {
		s, err := c.Drivers.Controller(ctx, b.Volume.Driver)
		if err != nil {
			return result, err
		}
		if err = c.Ledger.Step(ctx, "validate/"+b.ID, core.Digest(b.Volume), func(ctx context.Context) (any, error) {
			v, e := s.Controller.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{VolumeId: b.Volume.Handle, VolumeCapabilities: []*csi.VolumeCapability{b.Volume.Capability}, VolumeContext: b.Volume.Context})
			if e != nil {
				return nil, e
			}
			if v.Confirmed == nil {
				return nil, errors.New("driver did not confirm requested volume capability")
			}
			return true, nil
		}, nil); err != nil {
			return result, err
		}
		nodeID, err := c.Drivers.NodeID(ctx, place.Node, b.Volume.Driver)
		if err != nil {
			return result, err
		}
		p.NodeIDs[b.Volume.Driver] = nodeID
		var attached attachment
		err = c.Ledger.Step(ctx, "attach/"+b.ID, core.Digest(b), func(ctx context.Context) (any, error) {
			publish, err := c.Ledger.EnsureAttachment(ctx, b.Volume.Key(), b.Node, core.Digest(b.Volume), nodeID, func() (map[string]string, error) {
				if !s.Attach {
					return map[string]string{}, nil
				}
				sec, err := c.Kube.Secrets(ctx, b.Volume, "controller-publish")
				if err != nil {
					return nil, err
				}
				v, err := s.Controller.ControllerPublishVolume(metadata.AppendToOutgoingContext(ctx, "ssf-binding-id", b.ID), &csi.ControllerPublishVolumeRequest{VolumeId: b.Volume.Handle, NodeId: nodeID, VolumeCapability: b.Volume.Capability, Readonly: b.Volume.ReadOnly, VolumeContext: b.Volume.Context, Secrets: sec})
				if err != nil {
					return nil, err
				}
				return v.PublishContext, nil
			})
			return attachment{NodeID: nodeID, Context: publish, Required: s.Attach}, err
		}, &attached)
		if err != nil {
			return result, err
		}
		if attached.NodeID != nodeID {
			return result, errors.New("CSI node identity changed since attachment")
		}
		p.PublishContexts[b.ID] = attached.Context
		if p.Preparation != nil && attached.Context[core.PublishedFilesystemUUIDKey] != "" {
			uuid := attached.Context[core.PublishedFilesystemUUIDKey]
			if p.Preparation.Initialize || (p.Preparation.ExpectedUUID != "" && p.Preparation.ExpectedUUID != uuid) {
				return result, errors.New("driver-published filesystem conflicts with preparation intent")
			}
			p.Preparation.ExpectedUUID = uuid
		}
	}
	if err = c.validatePlacement(ctx, &place); err != nil {
		return result, err
	}
	if place.Expires.Before(p.Expires) {
		p.Expires = place.Expires
	}
	result.Placement = place
	result.Plan = p
	result.SignedPlan, err = auth.Sign(p, c.SigningKey)
	if err != nil {
		return result, err
	}
	err = c.Ledger.Step(ctx, "plan/"+op, readyPlanDigest(p), func(context.Context) (any, error) { return p, nil }, nil)
	if err != nil {
		return result, err
	}
	err = c.Ledger.SetOperation(ctx, op, "Planned", "")
	return result, err
}

// Commit is called only after the node and runtime report readiness.
func (c *Coordinator) Commit(ctx context.Context, r Result) error {
	if r.Plan.Purpose != "" {
		return errors.New("preparation cannot commit a sandbox run")
	}
	return c.Ledger.Exclusive(ctx, "operation/"+r.Operation, func(ctx context.Context) error {
		o, e := c.Ledger.Operation(ctx, r.Operation)
		if e != nil {
			return e
		}
		if (o.State != "Planned" && o.State != "Ready") || r.Plan.Operation != r.Operation {
			return core.ErrConflict
		}
		var receipt ReadyReceipt
		if e = c.Ledger.ReadStep(ctx, "ready/"+r.Operation, &receipt); e != nil {
			return e
		}
		if receipt.PlanDigest != readyPlanDigest(r.Plan) {
			return core.ErrConflict
		}
		if o.State == "Ready" {
			return nil
		}
		if e = c.validatePlacement(ctx, &r.Placement); e != nil {
			return e
		}
		e = c.Ledger.Step(ctx, "commit/"+r.Operation, core.Digest([]string{r.Placement.ID, r.Placement.Node, r.Plan.Sandbox}), func(ctx context.Context) (any, error) {
			return true, c.Placement.Commit(ctx, r.Placement, r.Plan.Sandbox)
		}, nil)
		if e != nil {
			return e
		}
		return c.Ledger.SetOperation(ctx, r.Operation, "Ready", "")
	})
}

// Release requires authenticated node evidence that all local Unpublish and
// last-holder Unstage operations finished. The integration verifies this RPC.
func (c *Coordinator) Release(ctx context.Context, op string, nodeReleased func(context.Context, string) error) error {
	return c.Ledger.Exclusive(ctx, "operation/"+op, func(ctx context.Context) error { return c.release(ctx, op, nodeReleased) })
}
func (c *Coordinator) release(ctx context.Context, op string, nodeReleased func(context.Context, string) error) error {
	if nodeReleased == nil {
		return errors.New("node release confirmation required")
	}
	if err := nodeReleased(ctx, op); err != nil {
		return err
	}
	if err := c.Ledger.BeginRelease(ctx, op); err != nil {
		return err
	}
	bs, err := c.Ledger.Bindings(ctx, op)
	if err != nil {
		return err
	}
	for _, b := range bs {
		if b.State == "Released" {
			continue
		}
		err = c.Ledger.Exclusive(ctx, "volume/"+b.Volume.Key(), func(ctx context.Context) error {
			n, err := c.Ledger.CountOther(ctx, b.Volume.Key(), b.Node, b.ID)
			if err != nil {
				return err
			}
			if n == 0 {
				s, err := c.Drivers.Controller(ctx, b.Volume.Driver)
				if err != nil {
					return err
				}
				var attached attachment
				if err := c.Ledger.ReadStep(ctx, "attach/"+b.ID, &attached); err != nil {
					return err
				}
				nodeID := attached.NodeID
				err = c.Ledger.Step(ctx, "detach/"+b.ID, core.Digest(b), func(ctx context.Context) (any, error) {
					if !attached.Required {
						return true, c.Ledger.DetachAttachment(ctx, b.Volume.Key(), b.Node, func(string) error { return nil })
					}
					sec, err := c.Kube.Secrets(ctx, b.Volume, "controller-publish")
					if err != nil {
						return nil, err
					}
					err = c.Ledger.DetachAttachment(ctx, b.Volume.Key(), b.Node, func(actualNodeID string) error {
						if actualNodeID != nodeID {
							return core.ErrConflict
						}
						_, e := s.Controller.ControllerUnpublishVolume(metadata.AppendToOutgoingContext(ctx, "ssf-binding-id", b.ID), &csi.ControllerUnpublishVolumeRequest{VolumeId: b.Volume.Handle, NodeId: actualNodeID, Secrets: sec})
						return e
					})
					return true, err
				}, nil)
				if err != nil {
					return err
				}
			}
			return c.Ledger.ReleaseBinding(ctx, b.ID)
		})
		if err != nil {
			return err
		}
	}
	return c.Ledger.SetOperation(ctx, op, "Released", "")
}
func (c *Coordinator) Drain(ctx context.Context, uid, action string) error {
	if err := c.Ledger.Drain(ctx, uid); err != nil {
		return err
	}
	return c.Ledger.Permit(ctx, uid, action, 5*time.Minute)
}
