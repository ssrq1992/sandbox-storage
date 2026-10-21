package kubernetes

import (
	"context"
	"errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sort"
	"ssf.dev/ssf/core"
)

// SelectStatic is deterministic; native PV Controller still finalizes binding.
// Only administrator-labelled candidates are eligible for automatic selection.
func (c *Client) SelectStatic(ctx context.Context, ref core.PVCRef, node string) (bool, error) {
	p, e := c.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if e != nil {
		return false, e
	}
	if string(p.UID) != ref.UID {
		return false, core.ErrConflict
	}
	if p.Spec.VolumeName != "" {
		return true, nil
	}
	if p.Annotations["volume.kubernetes.io/selected-node"] != "" {
		return false, nil
	}
	list, e := c.API.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{LabelSelector: ManagedLabel + "=true"})
	if e != nil {
		return false, e
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })
	selected := ""
	if c.Selection != nil {
		selected, e = c.Selection.Candidate(ctx, ref.UID)
		if e != nil {
			return false, e
		}
	}
	for _, v := range list.Items {
		if p.Spec.Selector != nil {
			selector, e := metav1.LabelSelectorAsSelector(p.Spec.Selector)
			if e != nil {
				return false, e
			}
			if !selector.Matches(labels.Set(v.Labels)) {
				continue
			}
		}
		if selected != "" && string(v.UID) != selected {
			continue
		}
		if v.Spec.ClaimRef != nil && v.Spec.ClaimRef.UID == p.UID {
			return true, nil
		}
		if v.Status.Phase != v1.VolumeAvailable || v.Spec.ClaimRef != nil || v.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
			continue
		}
		if v.Spec.CSI == nil {
			continue
		}
		if p.Spec.StorageClassName == nil || v.Spec.StorageClassName != *p.Spec.StorageClassName {
			continue
		}
		want, have := p.Spec.Resources.Requests[v1.ResourceStorage], v.Spec.Capacity[v1.ResourceStorage]
		if have.Cmp(want) < 0 {
			continue
		}
		pm, vm := v1.PersistentVolumeFilesystem, v1.PersistentVolumeFilesystem
		if p.Spec.VolumeMode != nil {
			pm = *p.Spec.VolumeMode
		}
		if v.Spec.VolumeMode != nil {
			vm = *v.Spec.VolumeMode
		}
		if pm != vm {
			continue
		}
		compatible := true
		for _, m := range p.Spec.AccessModes {
			found := false
			for _, x := range v.Spec.AccessModes {
				found = found || m == x
			}
			compatible = compatible && found
		}
		if !compatible {
			continue
		}
		if e = c.checkAffinity(ctx, &v, node); e != nil {
			if errors.Is(e, ErrTopology) {
				continue
			}
			return false, e
		}
		// Never select another candidate after an ambiguous Update result.
		e = c.ReserveStatic(ctx, ref, v.Name, node)
		return e == nil, e
	}
	if selected != "" {
		return false, core.ErrPending
	}
	return false, nil
}
