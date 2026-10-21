package kubernetes

import (
	"context"
	"errors"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"ssf.dev/ssf/core"
	"testing"
	"time"
)

func fixture() (*Client, core.PVCRef) {
	mode := v1.PersistentVolumeBlock
	sc := "test"
	p := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n", UID: "uid", Labels: map[string]string{ManagedLabel: "true"}}, Spec: v1.PersistentVolumeClaimSpec{VolumeName: "v", StorageClassName: &sc, VolumeMode: &mode, AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}}, Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound}}
	v := &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "v", UID: "pv"}, Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "test", VolumeHandle: "disk"}}, ClaimRef: &v1.ObjectReference{Namespace: "n", Name: "p", UID: "uid"}, PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimRetain}}
	return &Client{API: fake.NewClientset(p, v, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}})}, core.PVCRef{Namespace: "n", Name: "p", UID: "uid"}
}
func TestResolveIdentityAndBlock(t *testing.T) {
	c, ref := fixture()
	v, e := c.Resolve(context.Background(), ref, false)
	if e != nil {
		t.Fatal(e)
	}
	if v.Handle != "disk" || v.Capability.GetBlock() == nil {
		t.Fatal("wrong backend capability")
	}
	ref.UID = "recreated"
	if _, e = c.Resolve(context.Background(), ref, false); !errors.Is(e, core.ErrConflict) {
		t.Fatal("name reused without UID check")
	}
	pv, _ := c.API.CoreV1().PersistentVolumes().Get(context.Background(), "v", metav1.GetOptions{})
	if pv.Labels[ManagedLabel] != "true" {
		t.Fatal("dynamic PV not protected")
	}
}
func TestWFFCSelectionIsSticky(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	binding := storage.VolumeBindingWaitForFirstConsumer
	sc := "test"
	p := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n", UID: "uid", Labels: map[string]string{ManagedLabel: "true"}}, Spec: v1.PersistentVolumeClaimSpec{StorageClassName: &sc}}
	c := &Client{API: fake.NewClientset(p, &storage.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: sc}, Provisioner: "test", VolumeBindingMode: &binding})}
	ref := core.PVCRef{Namespace: "n", Name: "p", UID: "uid"}
	_, e := c.EnsureBound(ctx, ref, "node", false, func(context.Context) error { return nil })
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	actual, _ := c.API.CoreV1().PersistentVolumeClaims("n").Get(context.Background(), "p", metav1.GetOptions{})
	if actual.Annotations["volume.kubernetes.io/selected-node"] != "node" {
		t.Fatal("selection not persisted")
	}
	_, e = c.EnsureBound(context.Background(), ref, "other", false, func(context.Context) error { return nil })
	if !errors.Is(e, core.ErrConflict) {
		t.Fatal("changed unknown provisioning node")
	}
}
func TestStaticReservation(t *testing.T) {
	ctx := context.Background()
	c, ref := fixture()
	p, _ := c.API.CoreV1().PersistentVolumeClaims("n").Get(ctx, "p", metav1.GetOptions{})
	p.Spec.VolumeName = ""
	p.Spec.Resources.Requests = v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Gi")}
	p.Status.Phase = v1.ClaimPending
	c.API.CoreV1().PersistentVolumeClaims("n").Update(ctx, p, metav1.UpdateOptions{})
	v, _ := c.API.CoreV1().PersistentVolumes().Get(ctx, "v", metav1.GetOptions{})
	v.Labels = map[string]string{ManagedLabel: "true"}
	v.Spec.StorageClassName = "test"
	v.Spec.ClaimRef = nil
	v.Spec.VolumeMode = p.Spec.VolumeMode
	v.Spec.AccessModes = p.Spec.AccessModes
	v.Spec.Capacity = v1.ResourceList{v1.ResourceStorage: resource.MustParse("2Gi")}
	v.Status.Phase = v1.VolumeAvailable
	c.API.CoreV1().PersistentVolumes().Update(ctx, v, metav1.UpdateOptions{})
	// A lexically earlier but incompatible candidate must not block a later PV.
	wrong := v.DeepCopy()
	wrong.Name, wrong.UID = "a-wrong-zone", "wrong-zone"
	wrong.Spec.NodeAffinity = &v1.VolumeNodeAffinity{Required: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{{Key: "zone", Operator: v1.NodeSelectorOpIn, Values: []string{"other"}}}}}}}
	if _, err := c.API.CoreV1().PersistentVolumes().Create(ctx, wrong, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ok, e := c.SelectStatic(ctx, ref, "node")
	if e != nil || !ok {
		t.Fatalf("%v %v", ok, e)
	}
	v, _ = c.API.CoreV1().PersistentVolumes().Get(ctx, "v", metav1.GetOptions{})
	if v.Spec.ClaimRef == nil || v.Spec.ClaimRef.UID != "uid" {
		t.Fatal("claim UID not reserved")
	}
}
