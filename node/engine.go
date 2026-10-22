package node

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"os"
	"path/filepath"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	driver "ssf.dev/ssf/drivers/csi"
	"ssf.dev/ssf/runtime"
	"time"
)

type Sessions interface {
	Node(context.Context, string) (*driver.Session, error)
}
type Secrets interface {
	Secrets(context.Context, core.Volume, string) (map[string]string, error)
}
type Engine struct {
	Preparer             runtime.VolumePreparation
	locks                keyedLocks
	WAL                  *WAL
	Sessions             Sessions
	Secrets              Secrets
	Evidence             runtime.EvidenceVerifier
	Key                  ed25519.PublicKey
	NodeID, BootID, Root string
	NodeUID              string
	Inspect              func(string, bool, bool) (core.Endpoint, error)
	Observe              func(string, time.Duration, error)
}

func (e *Engine) step(r *Record, key string, fn func() error) (result error) {
	start := time.Now()
	defer func() {
		if e.Observe != nil {
			kind := key
			for i, c := range key {
				if c == '/' {
					kind = key[:i]
					break
				}
			}
			e.Observe(kind, time.Since(start), result)
		}
	}()
	if r.Steps[key] == "Done" {
		return nil
	}
	r.Steps[key] = "Intent"
	if err := e.WAL.Save(*r); err != nil {
		return err
	}
	if err := fn(); err != nil {
		r.Reason = err.Error()
		_ = e.WAL.Save(*r)
		return err
	}
	r.Steps[key] = "Done"
	return e.WAL.Save(*r)
}

// Stage scope comes from the signed controller publish context persisted in WAL.
// A per-binding device session must never reuse another run's staging resource.
func (e *Engine) paths(p core.Plan, b core.Binding) (string, string) {
	key := b.Volume.Key()
	if p.PublishContexts[b.ID][core.StageScopeKey] == core.StageScopeBinding {
		key = core.Hash(key + "/" + b.ID)
	}
	return filepath.Join(e.Root, "stage", key), filepath.Join(e.Root, "publish", core.Hash(b.ID))
}
func (e *Engine) secret(ctx context.Context, v core.Volume, p string) (map[string]string, error) {
	if e.Secrets == nil {
		if len(v.Secrets) > 0 {
			return nil, errors.New("secret resolver unavailable")
		}
		return nil, nil
	}
	return e.Secrets.Secrets(ctx, v, p)
}
func (e *Engine) Prepare(ctx context.Context, request, token string) (Record, error) {
	return e.prepare(ctx, request, token, false)
}
func (e *Engine) prepare(ctx context.Context, request, token string, content bool) (Record, error) {
	p, err := auth.Verify(token, e.Key, e.NodeID, e.BootID, time.Now())
	if err != nil {
		return Record{}, err
	}
	if (p.Purpose == core.PurposePreparation) != content {
		return Record{}, errors.New("node operation purpose mismatch")
	}
	if request != p.Operation {
		return Record{}, core.ErrConflict
	}
	if e.NodeUID != "" && p.NodeUID != e.NodeUID {
		return Record{}, errors.New("Kubernetes node UID mismatch")
	}
	unlock := e.locks.lock(p)
	defer unlock()
	r, err := e.WAL.Get(request)
	digestPlan := p
	digestPlan.Expires = time.Time{}
	digestPlan.ID = ""
	digest := core.Digest(digestPlan)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return r, err
		}
		r = Record{SignedPlan: token, Plan: p, Digest: digest, State: "Preparing", Steps: map[string]string{}}
		if err = e.WAL.Save(r); err != nil {
			return r, err
		}
	} else {
		if content && core.PreparationReboot(r.Plan, p) &&
			r.State != "Released" && r.State != "Releasing" &&
			(r.Preparation == nil || !r.Preparation.Released) {
			if r.Preparation != nil {
				if e.Preparer == nil {
					return r, errors.New("preparation WAL inspection unavailable")
				}
				prior, err := contentRequest(r)
				if err != nil {
					return r, err
				}
				proof, err := e.Preparer.InspectVolumePreparation(ctx, prior)
				if err != nil {
					return r, err
				}
				if err = validateContentReceipt(prior, proof, false); err != nil || proof.Released || proof.UUID != r.Preparation.UUID {
					return r, errors.New("preparation recovery lost original adapter content evidence")
				}
			}
			// The current boot is verified by the signed plan and local boot
			// identity above. Old formatter processes cannot survive it.
			// Re-publish CSI paths; the adapter keeps the same format WAL.
			r.Plan, r.Digest, r.State = p, digest, "Preparing"
			r.Steps = map[string]string{}
			r.Endpoints, r.Preparation = nil, nil
			r.Reason = ""
			if err = e.WAL.Save(r); err != nil {
				return r, err
			}
		}
		if r.Digest != digest || r.State == "Released" || r.State == "Releasing" || r.State == "NeedsRuntimeReconcile" {
			return r, core.ErrConflict
		}
		if r.State == "Prepared" || r.State == "Ready" {
			check := e.Inspect
			if check == nil {
				check = inspect
			}
			for _, ep := range r.Endpoints {
				actual, err := check(ep.Path, ep.Kind == "BlockDevice", ep.ReadOnly)
				if err != nil {
					return r, err
				}
				if actual.Identity != ep.Identity {
					return r, errors.New("prepared endpoint identity changed")
				}
			}
			return r, nil
		}
		r.Plan = p
		r.SignedPlan = token
	}
	endpoints := make([]core.Endpoint, 0, len(p.Bindings))
	for _, b := range p.Bindings {
		if b.Node != p.Node || b.Sandbox != p.Sandbox || b.Tenant != p.Tenant || b.Volume.Handle == "" || b.Volume.Capability == nil {
			return r, errors.New("binding scope mismatch")
		}
		s, err := e.Sessions.Node(ctx, b.Volume.Driver)
		if err != nil {
			return r, err
		}
		if p.NodeIDs[b.Volume.Driver] != s.NodeID {
			return r, errors.New("CSI node identity changed")
		}
		scope := p.PublishContexts[b.ID][core.StageScopeKey]
		if scope != "" && scope != core.StageScopeBinding {
			return r, errors.New("unsupported CSI staging scope")
		}
		stage, target := e.paths(r.Plan, b)
		if err = e.checkCompatibility(r, b); err != nil {
			return r, err
		}
		if s.Stage {
			secret, err := e.secret(ctx, b.Volume, "node-stage")
			if err != nil {
				return r, err
			}
			err = e.step(&r, "stage/"+b.ID, func() error {
				if reusable, err := e.reusableStage(r.Plan, b); err != nil {
					return err
				} else if reusable {
					return nil
				}
				if err := os.MkdirAll(stage, 0700); err != nil {
					return err
				}
				_, err := s.Node.NodeStageVolume(auth.WithNodePlan(ctx, token, b.ID), &csi.NodeStageVolumeRequest{VolumeId: b.Volume.Handle, StagingTargetPath: stage, VolumeCapability: b.Volume.Capability, VolumeContext: b.Volume.Context, PublishContext: p.PublishContexts[b.ID], Secrets: secret})
				return err
			})
			if err != nil {
				return r, err
			}
		}
		secret, err := e.secret(ctx, b.Volume, "node-publish")
		if err != nil {
			return r, err
		}
		err = e.step(&r, "publish/"+b.ID, func() error {
			if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
				return err
			}
			if b.Volume.Capability.GetBlock() != nil {
				if st, err := os.Lstat(target); os.IsNotExist(err) {
					f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
					if err != nil {
						return err
					}
					f.Close()
				} else if err != nil {
					return err
				} else if st.Mode()&os.ModeSymlink != 0 {
					return errors.New("symlink block target rejected")
				}
			} else {
				if err := os.MkdirAll(target, 0700); err != nil {
					return err
				}
			}
			st := ""
			if s.Stage {
				st = stage
			}
			_, err := s.Node.NodePublishVolume(auth.WithNodePlan(ctx, token, b.ID), &csi.NodePublishVolumeRequest{VolumeId: b.Volume.Handle, StagingTargetPath: st, TargetPath: target, VolumeCapability: b.Volume.Capability, VolumeContext: b.Volume.Context, PublishContext: p.PublishContexts[b.ID], Readonly: b.Volume.ReadOnly, Secrets: secret})
			return err
		})
		if err != nil {
			return r, err
		}
		inspector := e.Inspect
		if inspector == nil {
			inspector = inspect
		}
		ep, err := inspector(target, b.Volume.Capability.GetBlock() != nil, b.Volume.ReadOnly)
		if err != nil {
			return r, err
		}
		ep.BindingID = b.ID
		ep.Epoch = b.Epoch
		ep.GuestPath = b.GuestPath
		ep.NodeBootID = e.BootID
		ep.NodeUID = p.NodeUID
		ep.VolumeUID = b.Volume.UID
		ep.Generation = b.Volume.Generation
		ep.Revision = s.Revision
		endpoints = append(endpoints, ep)
	}
	r.Endpoints = endpoints
	if r.State != "Ready" {
		r.State = "Prepared"
	}
	r.Reason = ""
	err = e.WAL.Save(r)
	return r, err
}

func (e *Engine) reusableStage(p core.Plan, b core.Binding) (bool, error) {
	records, err := e.WAL.ForVolume(b.Volume.Key())
	if err != nil {
		return false, err
	}
	check := e.Inspect
	if check == nil {
		check = inspect
	}
	for _, r := range records {
		if r.State != "Prepared" && r.State != "Ready" {
			continue
		}
		for _, old := range r.Plan.Bindings {
			wanted, _ := e.paths(p, b)
			existing, _ := e.paths(r.Plan, old)
			if wanted != existing {
				continue
			}
			if old.ID == b.ID || old.Volume.Key() != b.Volume.Key() || r.Steps["stage/"+old.ID] != "Done" {
				continue
			}
			_, target := e.paths(r.Plan, old)
			if _, err := check(target, old.Volume.Capability.GetBlock() != nil, old.Volume.ReadOnly); err == nil {
				return true, nil
			}
		}
	}
	return false, nil
}
func (e *Engine) checkCompatibility(r Record, b core.Binding) error {
	rs, err := e.WAL.ForVolume(b.Volume.Key())
	if err != nil {
		return err
	}
	for _, old := range rs {
		if old.State == "Released" {
			continue
		}
		for _, x := range old.Plan.Bindings {
			if x.ID == b.ID || x.Volume.Key() != b.Volume.Key() || old.Steps["unpublish/"+x.ID] == "Done" {
				continue
			}
			if core.Digest(x.Volume.Capability) != core.Digest(b.Volume.Capability) || core.Digest(x.Volume.Context) != core.Digest(b.Volume.Context) || core.Digest(x.Volume.Secrets) != core.Digest(b.Volume.Secrets) || x.Volume.ReadOnly != b.Volume.ReadOnly {
				return core.ErrConflict
			}
		}
	}
	return nil
}
func (e *Engine) Ready(ctx context.Context, id, sandbox, proof string) (Record, error) {
	r, unlock, err := e.lockedRecord(id)
	defer unlock()
	if err != nil {
		return r, err
	}
	if r.Plan.Purpose != "" || r.Plan.Sandbox != sandbox || (r.State != "Prepared" && r.State != "Ready") {
		return r, core.ErrConflict
	}
	if r.Plan.BootID != e.BootID {
		return r, errors.New("runtime readiness belongs to previous node boot")
	}
	if e.Evidence == nil {
		return r, errors.New("runtime evidence verifier required")
	}
	if err = e.Evidence.Verify(ctx, sandbox, "ready", proof); err != nil {
		return r, err
	}
	r.State = "Ready"
	return r, e.WAL.Save(r)
}
func (e *Engine) Release(ctx context.Context, id, sandbox, proof string) (Record, error) {
	r, unlock, err := e.lockedRecord(id)
	defer unlock()
	if err != nil {
		return r, err
	}
	if r.Plan.Purpose != "" || r.Plan.Sandbox != sandbox {
		return r, core.ErrConflict
	}
	if r.State == "Released" {
		return r, nil
	}
	if r.State != "Releasing" {
		if e.Evidence == nil {
			return r, errors.New("runtime detachment verifier required")
		}
		if err = e.Evidence.Verify(ctx, sandbox, "detached", proof); err != nil {
			return r, err
		}
		r.State = "Releasing"
		r.ReleaseEvidence = proof
		if err = e.WAL.Save(r); err != nil {
			return r, err
		}
	}
	return e.release(ctx, r)
}
func (e *Engine) release(ctx context.Context, r Record) (Record, error) {
	for i := len(r.Plan.Bindings) - 1; i >= 0; i-- {
		b := r.Plan.Bindings[i]
		s, err := e.Sessions.Node(ctx, b.Volume.Driver)
		if err != nil {
			return r, err
		}
		stage, target := e.paths(r.Plan, b)
		if s.NodeID != r.Plan.NodeIDs[b.Volume.Driver] {
			return r, errors.New("CSI node identity changed during release")
		}
		if s.Managed != nil && r.Plan.Purpose == "" && (r.Steps["stage/"+b.ID] != "" || r.Steps["publish/"+b.ID] != "") {
			if r.SignedPlan == "" || r.ReleaseEvidence == "" {
				return r, errors.New("managed driver cleanup lacks durable signed plan or runtime evidence")
			}
			err = e.step(&r, "finalize/"+b.ID, func() error {
				receipt, err := s.Managed.FinalizeRuntime(ctx, &v1.DriverFinalizeRequest{SignedNodePlan: r.SignedPlan, BindingUid: b.ID, DetachedEvidence: r.ReleaseEvidence})
				if err == nil && (receipt == nil || receipt.OperationId != b.ID || (receipt.State != "Finalized" && receipt.State != "Unstarted")) {
					return errors.New("managed runtime finalization receipt mismatch")
				}
				return err
			})
			if err != nil {
				return r, err
			}
		}
		if s.Managed != nil && r.Plan.Purpose == core.PurposePreparation && (r.Steps["stage/"+b.ID] != "" || r.Steps["publish/"+b.ID] != "") {
			if r.SignedPlan == "" || r.Preparation == nil || !r.Preparation.Released {
				return r, errors.New("managed preparation cleanup lacks durable release receipt")
			}
			raw, err := json.Marshal(r.Preparation)
			if err != nil {
				return r, err
			}
			err = e.step(&r, "finalize/"+b.ID, func() error {
				receipt, err := s.Managed.FinalizePreparation(ctx, &v1.DriverPreparationReleaseRequest{SignedNodePlan: r.SignedPlan, BindingUid: b.ID, ReceiptJson: string(raw)})
				if err == nil && (receipt == nil || receipt.OperationId != b.ID || (receipt.State != "Finalized" && receipt.State != "Unstarted")) {
					return errors.New("managed preparation finalization receipt mismatch")
				}
				return err
			})
			if err != nil {
				return r, err
			}
		}
		if r.Steps["publish/"+b.ID] != "" {
			err = e.step(&r, "unpublish/"+b.ID, func() error {
				_, err := s.Node.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: b.Volume.Handle, TargetPath: target})
				return err
			})
			if err != nil {
				return r, err
			}
		}
		holders, err := e.stageHolders(r, b)
		if err != nil {
			return r, err
		}
		if holders == 0 && r.Steps["stage/"+b.ID] != "" {
			err = e.step(&r, "unstage/"+b.ID, func() error {
				_, err := s.Node.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: b.Volume.Handle, StagingTargetPath: stage})
				return err
			})
			if err != nil {
				return r, err
			}
		}
	}
	r.State = "Released"
	r.Reason = ""
	return r, e.WAL.Save(r)
}
func (e *Engine) stageHolders(current Record, b core.Binding) (int, error) {
	rs, err := e.WAL.ForVolume(b.Volume.Key())
	if err != nil {
		return 0, err
	}
	n := 0
	for _, r := range rs {
		if r.Plan.Operation == current.Plan.Operation {
			r = current
		}
		if r.State == "Released" {
			continue
		}
		for _, x := range r.Plan.Bindings {
			wanted, _ := e.paths(current.Plan, b)
			existing, _ := e.paths(r.Plan, x)
			if wanted != existing {
				continue
			}
			if x.ID != b.ID && x.Volume.Key() == b.Volume.Key() && r.Steps["stage/"+x.ID] != "" && r.Steps["unpublish/"+x.ID] != "Done" {
				n++
			}
		}
	}
	return n, nil
}

// Recover only completes already authorized releases. It never invents runtime
// detachment evidence or starts a new prepare from an expired plan.
func (e *Engine) Recover(ctx context.Context) error {
	rs, err := e.WAL.Active()
	if err != nil {
		return err
	}
	for _, r := range rs {
		if r.State == "Releasing" {
			if r.Plan.Purpose == core.PurposePreparation {
				current, unlock, e2 := e.lockedRecord(r.Plan.Operation)
				if e2 != nil {
					return e2
				}
				if current.Preparation == nil || !current.Preparation.Released {
					unlock()
					return errors.New("preparation release lacks durable evidence")
				}
				_, e2 = e.release(ctx, current)
				unlock()
				if e2 != nil {
					return e2
				}
				continue
			}
			if _, err = e.Release(ctx, r.Plan.Operation, r.Plan.Sandbox, ""); err != nil {
				return err
			}
		} else if r.State == "Prepared" || r.State == "Ready" {
			current, unlock, e2 := e.lockedRecord(r.Plan.Operation)
			if e2 != nil {
				return e2
			}
			if current.State == "Prepared" || current.State == "Ready" {
				reason := ""
				if current.Plan.BootID != e.BootID {
					reason = "node reboot requires runtime reconciliation"
				} else {
					check := e.Inspect
					if check == nil {
						check = inspect
					}
					for _, ep := range current.Endpoints {
						actual, e2 := check(ep.Path, ep.Kind == "BlockDevice", ep.ReadOnly)
						if e2 != nil || actual.Identity != ep.Identity {
							reason = "published endpoint changed; runtime reconciliation required"
							break
						}
					}
				}
				if reason != "" {
					current.State = "NeedsRuntimeReconcile"
					current.Reason = reason
					err = e.WAL.Save(current)
				}
			}
			unlock()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (e *Engine) Expand(ctx context.Context, id, binding string, size int64) error {
	if size <= 0 {
		return errors.New("positive capacity required")
	}
	r, unlock, err := e.lockedRecord(id)
	defer unlock()
	if err != nil {
		return err
	}
	if r.State != "Prepared" && r.State != "Ready" {
		return core.ErrConflict
	}
	for _, b := range r.Plan.Bindings {
		if b.ID != binding {
			continue
		}
		s, err := e.Sessions.Node(ctx, b.Volume.Driver)
		if err != nil {
			return err
		}
		if !s.Expand {
			return runtime.ErrUnsupported
		}
		sec, err := e.secret(ctx, b.Volume, "node-expand")
		if err != nil {
			return err
		}
		stage, target := e.paths(r.Plan, b)
		authorizer, ok := e.Secrets.(interface {
			AuthorizeExpansion(context.Context, core.Volume, int64) error
		})
		if !ok {
			return errors.New("native expansion authorizer required")
		}
		if err = authorizer.AuthorizeExpansion(ctx, b.Volume, size); err != nil {
			return err
		}
		return e.step(&r, "expand/"+binding+"/"+core.Digest(size), func() error {
			_, err := s.Node.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{VolumeId: b.Volume.Handle, VolumePath: target, StagingTargetPath: stage, CapacityRange: &csi.CapacityRange{RequiredBytes: size}, VolumeCapability: b.Volume.Capability, Secrets: sec})
			return err
		})
	}
	return errors.New("binding not found")
}
