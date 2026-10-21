package consume

import (
	"context"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
	"time"
)

// DeletePVC also accepts an unconsumed Pending claim. ClaimDeleted means only
// native object deletion was observed; it is not evidence of a backend Delete RPC.
func (c *Coordinator) DeletePVC(ctx context.Context, ref core.PVCRef, action string) (string, error) {
	if ref.UID == "" {
		return "", errors.New("expected PVC UID required")
	}
	p, e := c.Kube.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if e != nil && !apierrors.IsNotFound(e) {
		return "", e
	}
	if e == nil && string(p.UID) != ref.UID {
		return "", core.ErrConflict
	}
	if e = c.Ledger.DrainIdentity(ctx, ref); e != nil {
		return "", e
	}
	if e = c.Ledger.Permit(ctx, ref.UID, action, 5*time.Minute); e != nil {
		return "", e
	}
	pvs, e := c.Kube.API.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if e != nil {
		return "", e
	}
	for _, pv := range pvs.Items {
		if pv.Spec.ClaimRef != nil && string(pv.Spec.ClaimRef.UID) == ref.UID {
			if pv.Spec.CSI == nil {
				return "", errors.New("non-CSI PV is outside SSF")
			}
			v := core.Volume{PVC: ref, PVName: pv.Name, PVUID: string(pv.UID)}
			return c.Kube.Reclaim(ctx, v, action, c.Ledger)
		}
	}
	e = c.Kube.DeletePVC(ctx, ref)
	if apierrors.IsNotFound(e) {
		return "ClaimDeleted", nil
	}
	if e != nil {
		return "", e
	}
	return "ReclaimPending", nil
}
