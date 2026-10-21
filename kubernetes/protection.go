package kubernetes

import (
	"context"
	"errors"
	admission "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CheckProtection fails closed if the cluster's SSF admission barrier is absent.
// Deployments must restrict who can update this configuration and its Service.
func (c *Client) CheckProtection(ctx context.Context) error {
	cfg, e := c.API.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, "ssf-guard", metav1.GetOptions{})
	if e != nil {
		return e
	}
	protected := map[string]bool{}
	for _, w := range cfg.Webhooks {
		if w.FailurePolicy == nil || *w.FailurePolicy != admission.Fail || w.SideEffects == nil || *w.SideEffects != admission.SideEffectClassNone || w.ClientConfig.Service == nil || len(w.ClientConfig.CABundle) == 0 {
			continue
		}
		// k8s >= 1.37 defaults unset webhook selectors to an empty object {};
		// an empty selector matches everything, so it does not narrow protection.
		selectorEmpty := func(s *metav1.LabelSelector) bool {
			return s == nil || (len(s.MatchLabels) == 0 && len(s.MatchExpressions) == 0)
		}
		if !selectorEmpty(w.NamespaceSelector) || !selectorEmpty(w.ObjectSelector) || len(w.MatchConditions) > 0 {
			continue
		}
		for _, r := range w.Rules {
			allops := map[admission.OperationType]bool{}
			for _, o := range r.Operations {
				allops[o] = true
			}
			for _, resource := range r.Resources {
				if resource == "pods" {
					if allops[admission.Create] && allops[admission.Update] {
						protected[resource] = true
					}
				} else if allops[admission.Create] && allops[admission.Update] && allops[admission.Delete] {
					protected[resource] = true
				}
			}
		}
	}
	for _, r := range []string{"pods", "persistentvolumeclaims", "persistentvolumes"} {
		if !protected[r] {
			return errors.New("SSF admission protection missing or narrowed")
		}
	}
	return nil
}
