package preparation

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// Reconcile observes native PVCs. In particular, it never creates a consumer
// Pod to force WFFC binding. The normal scheduler supplies that placement.
func (c *Coordinator) Reconcile(ctx context.Context) error {
	var failures []error
	for _, namespace := range c.Namespaces {
		claims, err := c.Kube.API.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			failures = append(failures, err)
			continue
		}
		for i := range claims.Items {
			p := &claims.Items[i]
			if p.Annotations[kube.VolumeProfileAnnotation] == "" {
				continue
			}
			work, cancel := context.WithTimeout(ctx, 5*time.Minute)
			err := c.reconcileClaim(work, p)
			cancel()
			if err != nil && !errors.Is(err, core.ErrPending) {
				failures = append(failures, fmt.Errorf("%s/%s: %w", namespace, p.Name, err))
			}
		}
	}
	return errors.Join(failures...)
}

func (c *Coordinator) Run(ctx context.Context, interval time.Duration, report func(error)) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	for {
		if err := c.Reconcile(ctx); err != nil && report != nil {
			report(err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (c *Coordinator) reconcileClaim(ctx context.Context, p *v1.PersistentVolumeClaim) error {
	ref := core.PVCRef{Namespace: p.Namespace, Name: p.Name, UID: string(p.UID)}
	if p.DeletionTimestamp != nil {
		return c.drainClaim(ctx, ref)
	}
	record, managed, err := c.Observe(ctx, ref)
	if err != nil || !managed {
		return err
	}
	if err = c.project(ctx, record); err != nil {
		return err
	}
	if p.Status.Phase != v1.ClaimBound {
		return nil
	}
	// A newly bound WFFC claim remains part of the native scheduler's
	// reservation. Its first preparation must use that consumer's placement;
	// the observer only recovers an already persisted preparation plan.
	if record.Plan.Node == "" {
		class, err := c.Kube.API.StorageV1().StorageClasses().Get(ctx, *p.Spec.StorageClassName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if class.VolumeBindingMode != nil && *class.VolumeBindingMode == storage.VolumeBindingWaitForFirstConsumer {
			return nil
		}
	}
	v, err := c.Kube.Resolve(ctx, ref, slices.Contains(p.Spec.AccessModes, v1.ReadOnlyMany))
	if err != nil {
		return err
	}
	_, err = c.Ensure(ctx, v, core.Placement{}, "", "")
	return err
}

// Deletion closes admission without stopping a VM. Active bindings, paused
// holds and attachments retain the native finalizer until their owners drain.
func (c *Coordinator) drainClaim(ctx context.Context, ref core.PVCRef) error {
	return c.Ledger.Exclusive(ctx, "preparation/"+ref.UID, func(ctx context.Context) error {
		if err := c.Ledger.DrainIdentity(ctx, ref); err != nil {
			return err
		}
		r, err := c.Ledger.Preparation(ctx, ref.UID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil && r.Plan.Node == "" {
			// No signed plan can leave the coordinator before this record has
			// its plan. The same advisory lock and the persisted drain close
			// dispatch while compensating a partially completed Attach.
			id := core.Hash("volume-preparation/" + r.PVC.UID + "/" + r.IntentDigest())
			op := core.Hash(c.Tenant + "\x00\x00" + id)
			co := consume.Coordinator{Kube: c.Kube, Ledger: c.Ledger, Drivers: c.Drivers}
			if err = co.AbortUnpublished(ctx, op, func(context.Context) error { return nil }); err != nil {
				return err
			}
		}
		// Unknown device outcomes must be reconciled before removal, even if
		// the controller never received a completion response.
		if err == nil && (r.State == "Preparing" || r.State == "NeedsRecovery" || r.State == "ContentReady") {
			if err = c.releaseDeletingPreparation(ctx, r); err != nil {
				return err
			}
		}
		if err = c.Ledger.Permit(ctx, ref.UID, "Retain", time.Minute); err != nil {
			return err
		}
		p, err := c.Kube.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(p.UID) != ref.UID || p.DeletionTimestamp == nil {
			return core.ErrConflict
		}
		p.Finalizers = slices.DeleteFunc(p.Finalizers, func(s string) bool { return s == kube.VolumeProtectionFinalizer })
		_, err = c.Kube.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Update(ctx, p, metav1.UpdateOptions{})
		return err
	})
}

// Deletion never starts a formatter. Release asks the original node to prove
// that its device processes and descriptors have stopped, then removes CSI
// resources. Unknown format outcomes retain their WAL and finalizer.
func (c *Coordinator) releaseDeletingPreparation(ctx context.Context, r ledger.PreparationRecord) error {
	if c.Node == nil || r.Operation == "" || r.Plan.Node == "" {
		return core.ErrPending
	}
	place := core.Placement{ID: r.Operation, Node: r.Plan.Node, NodeUID: r.Plan.NodeUID, BootID: r.Plan.BootID}
	client, err := c.Node(ctx, place)
	if err != nil {
		return err
	}
	token, err := auth.Sign(r.Plan, c.SigningKey)
	if err != nil {
		return err
	}
	response, err := client.ReleaseVolumePreparation(ctx, &api.PrepareRequest{RequestId: r.Operation, SignedNodePlan: token})
	if err != nil {
		return err
	}
	receipt, err := readReceipt(response, r, true)
	if err != nil {
		return err
	}
	co := consume.Coordinator{Kube: c.Kube, Ledger: c.Ledger, Drivers: c.Drivers}
	if err = co.Release(ctx, r.Operation, func(ctx context.Context, id string) error {
		response, err := client.InspectVolumePreparation(ctx, &api.InspectRequest{OperationId: id})
		if err != nil {
			return err
		}
		_, err = readReceipt(response, r, true)
		return err
	}); err != nil {
		return err
	}
	next := r
	next.Receipt = receipt
	if receipt.Rejected {
		next.State = "Failed"
		next.Reason = "PVC deleted before content preparation completed"
		return c.Ledger.SavePreparation(ctx, r, next)
	}
	if r.State != "ContentReady" {
		next.State = "ContentReady"
		if err = c.Ledger.SavePreparation(ctx, r, next); err != nil {
			return err
		}
		r = next
	}
	next.State = "VolumeReady"
	return c.Ledger.SavePreparation(ctx, r, next)
}
