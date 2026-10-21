package guard

import (
	"context"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (h *Handler) initialBinding(ctx context.Context, old, next *v1.PersistentVolume) (bool, string) {
	ref := next.Spec.ClaimRef
	p, e := h.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if e != nil || p.UID != ref.UID {
		return false, "claim identity mismatch"
	}
	if p.Spec.StorageClassName == nil {
		return true, ""
	}
	sc, e := h.API.StorageV1().StorageClasses().Get(ctx, *p.Spec.StorageClassName, metav1.GetOptions{})
	if e != nil {
		return false, "cannot verify binding mode"
	}
	if sc.VolumeBindingMode == nil || *sc.VolumeBindingMode != storage.VolumeBindingWaitForFirstConsumer {
		return true, ""
	}
	if p.Spec.VolumeName == old.Name {
		return true, ""
	}
	ok, e := h.Ledger.AllowedSelection(ctx, string(ref.UID), "", string(old.UID))
	if e != nil || !ok {
		return false, "static WFFC candidate reservation required"
	}
	return true, ""
}
