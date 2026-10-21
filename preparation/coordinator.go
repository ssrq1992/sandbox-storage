// Package preparation coordinates runtime content on Kubernetes-owned volumes.
// It reuses consumption's CSI and ledger machinery without inventing a VM.
package preparation

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

type NodeClient interface {
	PrepareVolumeContent(context.Context, *api.PrepareRequest) (*api.VolumePreparation, error)
	InspectVolumePreparation(context.Context, *api.InspectRequest) (*api.VolumePreparation, error)
	ReleaseVolumePreparation(context.Context, *api.PrepareRequest) (*api.VolumePreparation, error)
}
type Coordinator struct {
	Kube       *kube.Client
	Ledger     *ledger.Store
	Drivers    consume.Controllers
	SigningKey ed25519.PrivateKey
	Authorizer consume.Authorizer
	Tenant     string
	Namespaces []string
	Profiles   []core.VolumeProfile
	Choose     func(context.Context, core.Volume) (core.Placement, error)
	Validate   func(context.Context, core.Placement) error
	Refresh    func(context.Context, core.Placement) (core.Placement, error)
	Node       func(context.Context, core.Placement) (NodeClient, error)
}

// Observe pins a PVC even when a WFFC volume has no backend identity yet.
func (c *Coordinator) Observe(ctx context.Context, ref core.PVCRef) (ledger.PreparationRecord, bool, error) {
	var empty ledger.PreparationRecord
	p, err := c.Kube.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return empty, false, err
	}
	if ref.UID != "" && ref.UID != string(p.UID) {
		return empty, false, core.ErrConflict
	}
	profile, managed, err := c.Kube.VolumeProfile(ctx, p, c.Profiles)
	if err != nil || !managed {
		return empty, managed, err
	}
	if err = kube.ValidatePreparedClaim(p, profile); err != nil {
		return empty, true, err
	}
	if err = c.Authorizer.Authorize(ctx, c.Tenant, core.PVCRef{Namespace: p.Namespace, Name: p.Name, UID: string(p.UID)}, false); err != nil {
		return empty, true, err
	}
	state := "PendingPreparation"
	if p.Status.Phase != v1.ClaimBound {
		state = "WaitingBinding"
		sc, e := c.Kube.API.StorageV1().StorageClasses().Get(ctx, *p.Spec.StorageClassName, metav1.GetOptions{})
		if e != nil {
			return empty, true, e
		}
		if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storage.VolumeBindingWaitForFirstConsumer {
			state = "WaitingConsumer"
		}
	}
	sourceSpec := core.Digest(struct {
		Source *v1.TypedLocalObjectReference
		Ref    *v1.TypedObjectReference
	}{p.Spec.DataSource, p.Spec.DataSourceRef})
	prior, err := c.Ledger.Preparation(ctx, string(p.UID))
	if err == nil {
		if prior.PVC.Namespace != p.Namespace || prior.PVC.Name != p.Name || prior.Profile != profile || prior.SourceSpec != sourceSpec || prior.SourceRevision != p.Annotations[kube.SourceRevisionAnnotation] {
			return empty, true, core.ErrConflict
		}
		return prior, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return empty, true, err
	}
	revision := p.Annotations[kube.SourceRevisionAnnotation]
	if revision == "" {
		return empty, true, errors.New("PVC source identity was not pinned by admission")
	}
	record, err := c.Ledger.ObservePreparation(ctx, ledger.PreparationRecord{
		PVC: core.PVCRef{Namespace: p.Namespace, Name: p.Name, UID: string(p.UID)}, Profile: profile,
		SourceRevision: revision, SourceSpec: sourceSpec, State: state})
	return record, true, err
}

// Ensure is shared by autonomous Immediate preparation and native creation.
// A nonzero placement is the existing scheduler's WFFC/compute decision.
func (c *Coordinator) Ensure(ctx context.Context, v core.Volume, place core.Placement, owner, run string) (core.PreparationReceipt, error) {
	var receipt core.PreparationReceipt
	record, managed, err := c.Observe(ctx, v.PVC)
	if err != nil || !managed {
		return receipt, err
	}
	err = c.Ledger.Exclusive(ctx, "preparation/"+record.PVC.UID, func(ctx context.Context) error {
		record, e := c.Ledger.Preparation(ctx, record.PVC.UID)
		if e != nil {
			return e
		}
		if record.State == "VolumeReady" {
			if record.Plan.Preparation == nil || len(record.Plan.Bindings) != 1 || record.Plan.Preparation.PVUID != v.PVUID || record.Plan.Bindings[0].Volume.Key() != v.Key() {
				return core.ErrConflict
			}
			receipt = record.Receipt
			return c.project(ctx, record)
		}
		if record.State == "Failed" {
			return fmt.Errorf("volume preparation failed: %s", record.Reason)
		}
		claim, e := c.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
		if e != nil {
			return e
		}
		if claim.Spec.DataSource != nil || claim.Spec.DataSourceRef != nil {
			// A restored block device must already carry the source filesystem.
			// Native K8s source protection handles the backend copy; never mkfs.
			if claim.Status.Phase != v1.ClaimBound {
				return core.ErrPending
			}
			if record.State != "ContentReady" {
				source, e := c.Kube.PreparationSourceRevision(ctx, claim)
				if e != nil {
					return e
				}
				if source != record.SourceRevision {
					return errors.New("restoration source identity changed before content readiness")
				}
			}
		}
		if record.Plan.Node != "" {
			place = core.Placement{ID: record.Operation, Node: record.Plan.Node, NodeUID: record.Plan.NodeUID, BootID: record.Plan.BootID, Expires: time.Now().Add(5 * time.Minute)}
			if c.Refresh != nil {
				current, err := c.Refresh(ctx, place)
				if err != nil {
					return err
				}
				if current.Node != place.Node || current.NodeUID != place.NodeUID || current.BootID == "" {
					return core.ErrConflict
				}
				place = current
			}
		} else if place.Node == "" {
			if c.Choose == nil {
				return errors.New("preparation placement provider unavailable")
			}
			place, e = c.Choose(ctx, v)
			if e != nil {
				return e
			}
		}
		if c.Validate == nil || c.Node == nil {
			return errors.New("preparation node registry unavailable")
		}
		if e = c.Validate(ctx, place); e != nil {
			return e
		}
		co := &consume.Coordinator{Kube: c.Kube, Ledger: c.Ledger, Drivers: c.Drivers, SigningKey: c.SigningKey,
			Authorizer: c.Authorizer, Placement: fixedPlacement{place, c.Validate}, PlanTTL: 5 * time.Minute}
		id := core.Hash("volume-preparation/" + record.PVC.UID + "/" + record.IntentDigest())
		spec := core.PreparationSpec{Profile: record.Profile, PVC: v.PVC, PVUID: v.PVUID, SourceRevision: record.SourceRevision,
			Initialize: record.Profile.InitializeBlank && !v.ReadOnly && claim.Spec.DataSource == nil && claim.Spec.DataSourceRef == nil}
		if record.Plan.Node != "" && record.State != "ContentReady" {
			client, err := c.Node(ctx, place)
			if err != nil {
				return err
			}
			observed, inspectErr := client.InspectVolumePreparation(ctx, &api.InspectRequest{OperationId: record.Operation})
			if inspectErr != nil && place.BootID != record.Plan.BootID {
				// An unavailable old WAL is not permission to overwrite a
				// possibly released operation with a newly boot-scoped plan.
				next := record
				next.State, next.Reason = "NeedsRecovery", inspectErr.Error()
				if err := c.Ledger.SavePreparation(ctx, record, next); err != nil {
					return err
				}
				return inspectErr
			}
			if inspectErr == nil && observed != nil && observed.State == "Releasing" {
				token, err := auth.Sign(record.Plan, c.SigningKey)
				if err != nil {
					return err
				}
				observed, inspectErr = client.ReleaseVolumePreparation(ctx, &api.PrepareRequest{RequestId: record.Operation, SignedNodePlan: token})
				if inspectErr != nil {
					return inspectErr
				}
			}
			if inspectErr == nil && observed != nil && observed.State == "Released" {
				proof, err := readReceipt(observed, record, true)
				if err != nil {
					return err
				}
				next := record
				next.Receipt = proof
				next.State = "ContentReady"
				if proof.Rejected {
					next.State, next.Reason = "Failed", "preparation was durably revoked"
					if err := co.Release(ctx, record.Operation, func(context.Context, string) error { return nil }); err != nil {
						return err
					}
				}
				if err := c.Ledger.SavePreparation(ctx, record, next); err != nil {
					return err
				}
				record = next
				if proof.Rejected {
					return core.ErrConflict
				}
			}
		}
		if record.State != "ContentReady" {
			var result consume.Result
			if record.Plan.Node == "" {
				result, e = co.Prepare(ctx, consume.Request{ID: id, Tenant: c.Tenant, OwnerUID: "volume-preparation/" + record.PVC.UID, RunUID: id,
					Preparation: &spec, Mounts: []core.Mount{{PVC: v.PVC, MountID: "content", GuestPath: "/volume", ReadOnly: v.ReadOnly}}})
			} else {
				result.Operation, result.Placement, result.Plan = record.Operation, place, record.Plan
				result.Plan.BootID, result.Plan.Expires = place.BootID, time.Now().Add(5*time.Minute)
				result.SignedPlan, e = auth.Sign(result.Plan, c.SigningKey)
			}
			if e != nil {
				return e
			}
			next := record
			next.State = "Preparing"
			next.Operation = result.Operation
			next.Plan = result.Plan
			if record.State == "NeedsRecovery" {
				next.State = "NeedsRecovery"
			}
			if e = c.Ledger.SavePreparation(ctx, record, next); e != nil {
				return e
			}
			record = next
			place = core.Placement{ID: record.Operation, Node: record.Plan.Node, NodeUID: record.Plan.NodeUID, BootID: record.Plan.BootID, Expires: record.Plan.Expires}
			client, e := c.Node(ctx, place)
			if e != nil {
				return e
			}
			response, e := client.PrepareVolumeContent(ctx, &api.PrepareRequest{RequestId: record.Operation, SignedNodePlan: result.SignedPlan})
			if e != nil {
				next = record
				next.State = "NeedsRecovery"
				next.Reason = e.Error()
				if save := c.Ledger.SavePreparation(ctx, record, next); save != nil {
					return save
				}
				record = next
				if response, releaseErr := client.ReleaseVolumePreparation(ctx, &api.PrepareRequest{RequestId: record.Operation, SignedNodePlan: result.SignedPlan}); releaseErr == nil {
					if rejected, proofErr := readReceipt(response, record, true); proofErr == nil && rejected.Rejected {
						releaseErr = co.Release(ctx, record.Operation, func(ctx context.Context, id string) error {
							observed, err := client.InspectVolumePreparation(ctx, &api.InspectRequest{OperationId: id})
							if err != nil {
								return err
							}
							proof, err := readReceipt(observed, record, true)
							if err == nil && !proof.Rejected {
								return core.ErrConflict
							}
							return err
						})
						if releaseErr == nil {
							next = record
							next.State = "Failed"
							next.Receipt = rejected
							if save := c.Ledger.SavePreparation(ctx, record, next); save != nil {
								return save
							}
							_ = c.project(ctx, next)
						}
					}
				}
				return e
			}
			receipt, e = readReceipt(response, record, false)
			if e != nil {
				return e
			}
			next = record
			next.State = "ContentReady"
			next.Receipt = receipt
			next.Reason = ""
			if e = c.Ledger.SavePreparation(ctx, record, next); e != nil {
				return e
			}
			record = next
		}
		client, e := c.Node(ctx, place)
		if e != nil {
			return e
		}
		token, e := auth.Sign(record.Plan, c.SigningKey)
		if e != nil {
			return e
		}
		response, e := client.ReleaseVolumePreparation(ctx, &api.PrepareRequest{RequestId: record.Operation, SignedNodePlan: token})
		if e != nil {
			return e
		}
		receipt, e = readReceipt(response, record, true)
		if e != nil {
			return e
		}
		if receipt.Rejected {
			return core.ErrConflict
		}
		// Install the incoming owner's hold before releasing the preparation
		// binding; Acquire consumes it atomically. No writer/delete gap.
		if owner != "" {
			if e = c.Ledger.PreparationHandoff(ctx, record.Operation, owner, run); e != nil {
				return e
			}
		}
		e = co.Release(ctx, record.Operation, func(ctx context.Context, id string) error {
			observed, e := client.InspectVolumePreparation(ctx, &api.InspectRequest{OperationId: id})
			if e != nil {
				return e
			}
			_, e = readReceipt(observed, record, true)
			return e
		})
		if e != nil {
			return e
		}
		next := record
		next.State = "VolumeReady"
		next.Receipt = receipt
		if e = c.Ledger.SavePreparation(ctx, record, next); e != nil {
			return e
		}
		return c.project(ctx, next)
	})
	return receipt, err
}

func readReceipt(response *api.VolumePreparation, r ledger.PreparationRecord, released bool) (core.PreparationReceipt, error) {
	var receipt core.PreparationReceipt
	if len(r.Plan.Bindings) != 1 {
		return receipt, errors.New("preparation requires exactly one binding")
	}
	if response == nil || response.Protocol != core.PreparationProtocol || response.OperationId != r.Operation ||
		(released && response.State != "Released") || (!released && response.State != "Prepared") {
		return receipt, errors.New("node did not confirm preparation state")
	}
	if err := json.Unmarshal([]byte(response.ReceiptJson), &receipt); err != nil {
		return receipt, err
	}
	b := r.Plan.Bindings[0]
	rejection := released && receipt.Rejected && receipt.Released && receipt.UUID == ""
	if receipt.Protocol != core.PreparationProtocol || receipt.Operation != r.Operation || receipt.PlanDigest != core.PreparationDigest(r.Plan) ||
		receipt.BindingID != b.ID || receipt.VolumeUID != r.PVC.UID || receipt.NodeUID != r.Plan.NodeUID || receipt.BootID != r.Plan.BootID ||
		receipt.Epoch != b.Epoch || (!rejection && (receipt.UUID == "" || receipt.Rejected)) || (released && !receipt.Released) {
		return receipt, errors.New("node preparation receipt identity mismatch")
	}
	if r.Receipt.UUID != "" && receipt.UUID != r.Receipt.UUID {
		return receipt, core.ErrConflict
	}
	return receipt, nil
}

type fixedPlacement struct {
	placement core.Placement
	validate  func(context.Context, core.Placement) error
}

func (p fixedPlacement) Reserve(ctx context.Context, _ string, _ []core.Volume) (core.Placement, error) {
	return p.placement, p.validate(ctx, p.placement)
}
func (p fixedPlacement) Validate(ctx context.Context, v core.Placement) error {
	return p.validate(ctx, v)
}
func (p fixedPlacement) Renew(ctx context.Context, v core.Placement) (core.Placement, error) {
	v.Expires = time.Now().Add(5 * time.Minute)
	return v, p.validate(ctx, v)
}
func (p fixedPlacement) Commit(context.Context, core.Placement, string) error {
	return errors.New("preparation does not commit compute")
}
func (p fixedPlacement) Release(context.Context, core.Placement) error { return nil }

func (c *Coordinator) Receipt(ctx context.Context, uid string) (core.PreparationReceipt, error) {
	r, err := c.Ledger.Preparation(ctx, uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return core.PreparationReceipt{}, nil
	}
	if err == nil && r.State != "VolumeReady" {
		err = core.ErrPending
	}
	return r.Receipt, err
}
