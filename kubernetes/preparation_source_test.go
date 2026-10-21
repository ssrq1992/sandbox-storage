package kubernetes

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPreparationSourcePinsUID(t *testing.T) {
	ctx := context.Background()
	source := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "n", UID: "original"}, Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound}}
	c := &Client{API: fake.NewClientset(source)}
	target := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "n"}, Spec: v1.PersistentVolumeClaimSpec{DataSource: &v1.TypedLocalObjectReference{Kind: "PersistentVolumeClaim", Name: "source"}}}
	first, err := c.PreparationSourceRevision(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	source.UID = "replacement"
	if _, err = c.API.CoreV1().PersistentVolumeClaims("n").Update(ctx, source, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	second, err := c.PreparationSourceRevision(ctx, target)
	if err != nil || first == second {
		t.Fatalf("source replacement not detected: %v", err)
	}
	foreign := "other"
	target.Spec.DataSourceRef = &v1.TypedObjectReference{Kind: "PersistentVolumeClaim", Name: "source", Namespace: &foreign}
	if _, err = c.PreparationSourceRevision(ctx, target); err == nil {
		t.Fatal("cross-namespace source accepted")
	}
	target.Spec.DataSourceRef = nil
	target.Spec.DataSource.Kind = "Unknown"
	if _, err = c.PreparationSourceRevision(ctx, target); err == nil {
		t.Fatal("unknown source could be mistaken for blank")
	}
}
