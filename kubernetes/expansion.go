package kubernetes

import (
	"context"
	"errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
)

func (c *Client) AuthorizeExpansion(ctx context.Context, v core.Volume, size int64) error {
	p, e := c.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
	if e != nil {
		return e
	}
	pv, e := c.API.CoreV1().PersistentVolumes().Get(ctx, v.PVName, metav1.GetOptions{})
	if e != nil {
		return e
	}
	if string(p.UID) != v.PVC.UID || string(pv.UID) != v.PVUID || p.Spec.VolumeName != pv.Name {
		return core.ErrConflict
	}
	requested := p.Spec.Resources.Requests[v1.ResourceStorage]
	capacity := pv.Spec.Capacity[v1.ResourceStorage]
	if size > requested.Value() || size > capacity.Value() {
		return errors.New("native Controller expansion has not authorized capacity")
	}
	return nil
}
