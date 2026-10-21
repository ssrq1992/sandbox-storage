package kubernetes

import (
	"context"
	"errors"
	"fmt"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"ssf.dev/ssf/core"
)

// CheckPendingTopology filters a compute candidate before native WFFC binding.
// It never selects a PV or writes selected-node; the Kubernetes binder remains
// authoritative and the Coordinator rechecks the actual PV after binding.
func (c *Client) CheckPendingTopology(ctx context.Context, ref core.PVCRef, node string) error {
	claim, e := c.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if e != nil {
		return e
	}
	if ref.UID != "" && ref.UID != string(claim.UID) {
		return core.ErrConflict
	}
	if claim.DeletionTimestamp != nil {
		return core.ErrConflict
	}
	if selected := claim.Annotations["volume.kubernetes.io/selected-node"]; selected != "" && selected != node {
		return ErrTopology
	}
	if claim.Spec.StorageClassName == nil {
		return errors.New("storage class required")
	}
	if claim.Spec.VolumeName != "" {
		pv, e := c.API.CoreV1().PersistentVolumes().Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
		if e != nil {
			return e
		}
		return c.CheckTopology(ctx, core.Volume{PVName: pv.Name, PVUID: string(pv.UID)}, node)
	}
	sc, e := c.API.StorageV1().StorageClasses().Get(ctx, *claim.Spec.StorageClassName, metav1.GetOptions{})
	if e != nil {
		return e
	}
	// Immediate provisioning has no scheduler-selected topology. Wait for its
	// actual PV before reserving compute rather than guessing a future zone.
	if sc.VolumeBindingMode == nil || *sc.VolumeBindingMode == storagev1.VolumeBindingImmediate {
		return core.ErrPending
	}
	if sc.Provisioner == "kubernetes.io/no-provisioner" || claim.Spec.Selector != nil {
		list, e := c.API.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{LabelSelector: ManagedLabel + "=true"})
		if e != nil {
			return e
		}
		found := false
		for _, pv := range list.Items {
			if pv.Spec.CSI == nil || pv.Spec.StorageClassName != sc.Name || pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain || pv.DeletionTimestamp != nil {
				continue
			}
			if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.UID != claim.UID {
				continue
			}
			if pv.Spec.ClaimRef == nil && pv.Status.Phase != v1.VolumeAvailable {
				continue
			}
			if claim.Spec.Selector != nil {
				sel, e := metav1.LabelSelectorAsSelector(claim.Spec.Selector)
				if e != nil {
					return e
				}
				if !sel.Matches(labels.Set(pv.Labels)) {
					continue
				}
			}
			want, have := claim.Spec.Resources.Requests[v1.ResourceStorage], pv.Spec.Capacity[v1.ResourceStorage]
			if have.Cmp(want) < 0 || pv.Spec.VolumeMode == nil || *pv.Spec.VolumeMode != v1.PersistentVolumeBlock {
				continue
			}
			compatible := true
			for _, a := range claim.Spec.AccessModes {
				ok := false
				for _, b := range pv.Spec.AccessModes {
					ok = ok || a == b
				}
				compatible = compatible && ok
			}
			if !compatible {
				continue
			}
			if e = c.CheckTopology(ctx, core.Volume{PVName: pv.Name, PVUID: string(pv.UID)}, node); e == nil {
				found = true
				break
			} else if !errors.Is(e, ErrTopology) {
				return e
			}
		}
		if !found {
			return fmt.Errorf("%w: no static PV compatible with node and claim", ErrTopology)
		}
	}
	if len(sc.AllowedTopologies) == 0 {
		return nil
	}
	n, e := c.API.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if e != nil {
		return e
	}
	for _, term := range sc.AllowedTopologies {
		ok := true
		for _, expr := range term.MatchLabelExpressions {
			found := false
			for _, v := range expr.Values {
				if n.Labels[expr.Key] == v {
					found = true
				}
			}
			ok = ok && found
		}
		if ok {
			return nil
		}
	}
	return fmt.Errorf("%w: StorageClass topology excludes node", ErrTopology)
}
