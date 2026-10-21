package kubernetes

import (
	"context"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
)

func (c *Client) CheckOwnership(ctx context.Context, v core.Volume) error {
	pods, e := c.API.CoreV1().Pods(v.PVC.Namespace).List(ctx, metav1.ListOptions{})
	if e != nil {
		return e
	}
	for _, p := range pods.Items {
		for _, volume := range p.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == v.PVC.Name {
				return errors.New("native Pod reference must be drained before SSF admission")
			}
		}
	}
	attachments, e := c.API.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if e != nil {
		return e
	}
	for _, a := range attachments.Items {
		if a.Spec.Source.PersistentVolumeName != nil && *a.Spec.Source.PersistentVolumeName == v.PVName {
			return errors.New("native VolumeAttachment must be drained before direct CSI consumption")
		}
	}
	return nil
}
