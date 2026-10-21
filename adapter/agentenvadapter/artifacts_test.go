package agentenvadapter

import (
	"context"
	"errors"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"testing"
)

func TestImmediatePendingDoesNotGuessTopology(t *testing.T) {
	mode := storage.VolumeBindingImmediate
	class := "immediate"
	claim := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "p", UID: "uid"}, Spec: v1.PersistentVolumeClaimSpec{StorageClassName: &class}}
	client := &kube.Client{API: fake.NewClientset(claim, &storage.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: class}, Provisioner: "example.csi", VolumeBindingMode: &mode})}
	if err := client.CheckPendingTopology(context.Background(), core.PVCRef{Namespace: "n", Name: "p", UID: "uid"}, "unknown-future-zone"); !errors.Is(err, core.ErrPending) {
		t.Fatal("reserved compute before Immediate PV topology known", err)
	}
}
