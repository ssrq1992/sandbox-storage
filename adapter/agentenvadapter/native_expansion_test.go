package agentenvadapter

import (
	"errors"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"testing"
)

func TestNativeOfflineExpansionRetainsFenceAndResumesAfterController(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	mode := v1.PersistentVolumeBlock
	sc := id
	allowed := true
	claim := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: "test", UID: types.UID(id), Labels: map[string]string{kube.ManagedLabel: "true"}}, Spec: v1.PersistentVolumeClaimSpec{VolumeName: id, StorageClassName: &sc, VolumeMode: &mode, AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, Resources: v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}}, Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound}}
	pv := &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: id, UID: types.UID(id + "pv")}, Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "test", VolumeHandle: id}}, ClaimRef: &v1.ObjectReference{Namespace: "test", Name: id, UID: types.UID(id)}, PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimRetain, Capacity: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}}
	api := fake.NewSimpleClientset(claim, pv, &storage.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: sc}, AllowVolumeExpansion: &allowed})
	s := NativeVolumes{Ledger: c.Ledger, Kube: &kube.Client{API: api}}
	original := NativeVolume{Tenant: "test", Name: id, Provider: "ssf", PVC: core.PVCRef{Namespace: "test", Name: id, UID: id}, Bytes: 1 << 20, StorageClass: sc, State: "Bound"}
	if err := s.save(ctx, original); err != nil {
		t.Fatal(err)
	}
	got, err := s.Expand(ctx, "test", id, 2<<20)
	if !errors.Is(err, core.ErrPending) || got.State != "Expanding" || got.View().Status != "pending" {
		t.Fatalf("missing durable expansion intent: %+v %v", got, err)
	}
	volume, err := s.Kube.Resolve(ctx, original.PVC, false)
	if err != nil {
		t.Fatal(err)
	}
	binding := core.Binding{ID: id + "b", Volume: volume, Sandbox: id + "sandbox", Node: "n"}
	if err = s.Ledger.Acquire(ctx, id+"op", "digest", []core.Binding{binding}); !errors.Is(err, core.ErrPending) {
		t.Fatalf("writer crossed pending controller expansion: %v", err)
	}
	if err = s.Delete(ctx, "test", id); !errors.Is(err, core.ErrPending) {
		t.Fatalf("delete removed expansion recovery intent: %v", err)
	}
	observed, err := s.Observe(ctx, "test", id)
	if err != nil || observed.State != "Expanding" || observed.Bytes != 1<<20 {
		t.Fatalf("observation lost barrier: %+v %v", observed, err)
	}
	pv, err = api.CoreV1().PersistentVolumes().Get(ctx, id, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pv.Spec.Capacity[v1.ResourceStorage] = resource.MustParse("2Mi")
	if _, err = api.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Expand(ctx, "test", id, 2<<20)
	if err != nil || got.State != "Bound" || got.ExpansionRequested != 0 || got.ExpansionTarget != 2<<20 || got.Bytes != 2<<20 {
		t.Fatalf("controller completion lost node expansion: %+v %v", got, err)
	}
	if _, err = s.Expand(ctx, "test", id, 2<<20); err != nil {
		t.Fatal(err)
	}
	if err = s.Ledger.Acquire(ctx, id+"op", "digest", []core.Binding{binding}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Expand(ctx, "test", id, 3<<20)
	if !errors.Is(err, core.ErrPending) || got.State != "Bound" || got.ExpansionTarget != 2<<20 {
		t.Fatalf("active writer altered device: %+v %v", got, err)
	}
	current, _ := api.CoreV1().PersistentVolumeClaims("test").Get(ctx, id, metav1.GetOptions{})
	if current.Spec.Resources.Requests.Storage().Value() != 2<<20 {
		t.Fatal("controller resized active writer")
	}
	if _, err = s.Expand(ctx, "test", id, 1<<20); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("shrink accepted: %v", err)
	}
}
