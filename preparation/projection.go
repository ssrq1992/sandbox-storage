package preparation

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

// project is only an observable projection. The ledger remains authoritative.
// Failed Kubernetes updates are retried without repeating device preparation.
func (c *Coordinator) project(ctx context.Context, r ledger.PreparationRecord) error {
	p, err := c.Kube.API.CoreV1().PersistentVolumeClaims(r.PVC.Namespace).Get(ctx, r.PVC.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(p.UID) != r.PVC.UID {
		return core.ErrConflict
	}
	if p.Annotations == nil {
		p.Annotations = map[string]string{}
	}
	p.Annotations[kube.PreparationStateAnnotation] = r.State
	p.Annotations[kube.PreparationRevisionAnnotation] = r.IntentDigest()
	p.Annotations[kube.PreparationReasonAnnotation] = r.Reason
	_, err = c.Kube.API.CoreV1().PersistentVolumeClaims(p.Namespace).Update(ctx, p, metav1.UpdateOptions{})
	return err
}
