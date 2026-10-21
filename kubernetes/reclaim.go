package kubernetes

import (
	"context"
	"errors"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
)

type PermitChecker interface {
	Allowed(context.Context, string, string) (bool, error)
}

// Reclaim advances only Kubernetes objects. Provisioner remains the sole caller
// of DeleteVolume. Repeated calls observe native completion, not RPC guesses.
func (c *Client) Reclaim(ctx context.Context, v core.Volume, action string, permits PermitChecker) (string, error) {
	if action != "Retain" && action != "Delete" {
		return "", errors.New("invalid reclaim action")
	}
	ok, e := permits.Allowed(ctx, v.PVC.UID, action)
	if e != nil {
		return "", e
	}
	if !ok {
		return "", errors.New("drain permit required")
	}
	pv, e := c.API.CoreV1().PersistentVolumes().Get(ctx, v.PVName, metav1.GetOptions{})
	if apierrors.IsNotFound(e) && action == "Delete" {
		return "Deleted", nil
	}
	if e != nil {
		return "", e
	}
	if string(pv.UID) != v.PVUID || pv.Spec.ClaimRef == nil || string(pv.Spec.ClaimRef.UID) != v.PVC.UID {
		return "", core.ErrConflict
	}
	if action == "Retain" && pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
		return "", errors.New("Delete reclaim already enabled; cannot promise Retain")
	}
	if action == "Delete" && pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimDelete {
		pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimDelete
		if _, e = c.API.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{}); e != nil {
			return "", e
		}
	}
	e = c.DeletePVC(ctx, v.PVC)
	if e != nil && !apierrors.IsNotFound(e) {
		return "", e
	}
	if action == "Delete" {
		return "ReclaimPending", nil
	}
	if apierrors.IsNotFound(e) && pv.Status.Phase == v1.VolumeReleased {
		return "Retained", nil
	}
	return "ReclaimPending", nil
}
func (c *Client) RestoreSnapshot(ctx context.Context, namespace, name, snapshot, class, size string) (*v1.PersistentVolumeClaim, error) {
	group := "snapshot.storage.k8s.io"
	p := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Spec: v1.PersistentVolumeClaimSpec{StorageClassName: &class, AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, DataSource: &v1.TypedLocalObjectReference{APIGroup: &group, Kind: "VolumeSnapshot", Name: snapshot}}}
	q, e := parseCapacity(size)
	if e != nil {
		return nil, e
	}
	p.Spec.Resources.Requests = v1.ResourceList{v1.ResourceStorage: q}
	return c.CreatePVC(ctx, p)
}
