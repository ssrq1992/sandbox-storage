package overlaybd

import (
	"context"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
	"time"
)

// RecoverDirectSnapshot cancels only a read-only head inspection whose original
// Kubernetes object is deleting/gone. Registered snapshots remain the native
// snapshot-controller/CSI DeleteSnapshot responsibility, including Retain policy.
func (a *KubernetesCaptureAuthority) RecoverDirectSnapshot(ctx context.Context, q core.DirectSnapshotRef) error {
	if a.Idle == nil {
		return nil
	}
	if _, err := a.Idle.Journal.Snapshot(ctx, q.ID); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	snapshot, se := a.Kube.Dynamic.Resource(directSnapshotResource).Namespace(q.Namespace).Get(ctx, q.Name, metav1.GetOptions{})
	content, ce := a.Kube.Dynamic.Resource(directContentResource).Get(ctx, q.ContentName, metav1.GetOptions{})
	if se != nil && !apierrors.IsNotFound(se) {
		return se
	}
	if ce != nil && !apierrors.IsNotFound(ce) {
		return ce
	}
	gone := apierrors.IsNotFound(se) || apierrors.IsNotFound(ce)
	if se == nil {
		gone = gone || string(snapshot.GetUID()) != q.SnapshotUID || snapshot.GetDeletionTimestamp() != nil
	}
	if ce == nil {
		gone = gone || string(content.GetUID()) != q.ContentUID || content.GetDeletionTimestamp() != nil
	}
	if !gone {
		return nil
	}
	// Cancellation and registration share the same transaction lock. A Retain
	// snapshot registered after our initial read must remain untouched.
	cancelled, err := a.Idle.Journal.CancelSnapshotRegistration(ctx, q.ID)
	if err != nil {
		return err
	}
	if !cancelled {
		return nil
	}
	if err := a.Delete(ctx, SnapshotPin{SnapshotUID: q.SnapshotUID, ContentUID: q.ContentUID}); err != nil {
		return err
	}
	return a.Retire(ctx, q.ID)
}
func (a *KubernetesCaptureAuthority) RunDirectRecovery(ctx context.Context, report func(error)) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	cursor := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		items, err := a.Ledger.DirectSnapshots(call, cursor, 128)
		if err == nil {
			for _, q := range items {
				if e := a.RecoverDirectSnapshot(call, q); e != nil && report != nil {
					report(e)
				}
				cursor = q.ID
			}
			if len(items) < 128 {
				cursor = ""
			}
		}
		cancel()
		if err != nil && report != nil {
			report(err)
		}
	}
}
