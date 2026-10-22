package kubernetes

import (
	"context"
	a "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func snapshotWebhookFixture() *a.ValidatingWebhookConfiguration {
	fail, none, all := a.Fail, a.SideEffectClassNone, a.AllScopes
	return &a.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "ssf-guard"}, Webhooks: []a.ValidatingWebhook{{Name: "snapshots.ssf.dev", FailurePolicy: &fail, SideEffects: &none, ClientConfig: a.WebhookClientConfig{Service: &a.ServiceReference{Name: "guard", Namespace: "ssf"}, CABundle: []byte("fixture")}, Rules: []a.RuleWithOperations{{Operations: []a.OperationType{a.Create, a.Update, a.Delete}, Rule: a.Rule{APIGroups: []string{"snapshot.storage.k8s.io"}, APIVersions: []string{"v1"}, Resources: []string{"volumesnapshots", "volumesnapshotcontents"}, Scope: &all}}}}}}
}
func TestSnapshotProtectionRejectsNarrowedRules(t *testing.T) {
	cases := []struct {
		name   string
		change func(*a.ValidatingWebhookConfiguration)
	}{
		{"wrong group", func(c *a.ValidatingWebhookConfiguration) { c.Webhooks[0].Rules[0].APIGroups = []string{"wrong"} }},
		{"wrong version", func(c *a.ValidatingWebhookConfiguration) { c.Webhooks[0].Rules[0].APIVersions = []string{"v1beta1"} }},
		{"namespaced only", func(c *a.ValidatingWebhookConfiguration) { x := a.NamespacedScope; c.Webhooks[0].Rules[0].Scope = &x }},
		{"missing delete", func(c *a.ValidatingWebhookConfiguration) {
			c.Webhooks[0].Rules[0].Operations = []a.OperationType{a.Create, a.Update}
		}},
		{"selector", func(c *a.ValidatingWebhookConfiguration) {
			c.Webhooks[0].ObjectSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"managed": "true"}}
		}},
		{"ignore", func(c *a.ValidatingWebhookConfiguration) { x := a.Ignore; c.Webhooks[0].FailurePolicy = &x }},
		{"only snapshot", func(c *a.ValidatingWebhookConfiguration) {
			c.Webhooks[0].Rules[0].Resources = []string{"volumesnapshots"}
		}},
	}
	ctx := context.Background()
	if err := (&Client{API: fake.NewSimpleClientset(snapshotWebhookFixture())}).CheckSnapshotProtection(ctx); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := snapshotWebhookFixture()
			tc.change(c)
			if err := (&Client{API: fake.NewSimpleClientset(c)}).CheckSnapshotProtection(ctx); err == nil {
				t.Fatal("narrowed protection accepted")
			}
		})
	}
	c := snapshotWebhookFixture()
	c.Webhooks[0].Rules[0].Operations = []a.OperationType{a.OperationAll}
	if err := (&Client{API: fake.NewSimpleClientset(c)}).CheckSnapshotProtection(ctx); err != nil {
		t.Fatal(err)
	}
}
