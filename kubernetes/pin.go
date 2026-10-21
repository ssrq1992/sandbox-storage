package kubernetes

import (
	"context"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
)

func (c *Client) Pin(ctx context.Context, r core.PVCRef) (core.PVCRef, error) {
	p, e := c.API.CoreV1().PersistentVolumeClaims(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
	if e != nil {
		return r, e
	}
	if r.UID != "" && r.UID != string(p.UID) {
		return r, core.ErrConflict
	}
	if p.Labels[ManagedLabel] != "true" || p.DeletionTimestamp != nil {
		return r, errors.New("PVC outside protected scope")
	}
	r.UID = string(p.UID)
	return r, nil
}
