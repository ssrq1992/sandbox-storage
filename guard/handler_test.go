package guard

import (
	"context"
	"encoding/json"
	admission "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	kube "ssf.dev/ssf/kubernetes"
	"testing"
)

type permits bool

func (p permits) AllowedSelection(context.Context, string, string, string) (bool, error) {
	return bool(p), nil
}

func (p permits) Allowed(context.Context, string, string) (bool, error) { return bool(p), nil }
func TestPVCDeleteRequiresPermit(t *testing.T) {
	obj, _ := json.Marshal(v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "p", UID: "uid", Labels: map[string]string{kube.ManagedLabel: "true"}}})
	r := &admission.AdmissionRequest{Kind: metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"}, Operation: admission.Delete, OldObject: runtime.RawExtension{Raw: obj}}
	h := &Handler{Ledger: permits(false), API: fake.NewClientset()}
	if ok, _ := h.check(context.Background(), r); ok {
		t.Fatal("unprotected deletion")
	}
	h.Ledger = permits(true)
	if ok, _ := h.check(context.Background(), r); !ok {
		t.Fatal("permit rejected")
	}
}
func TestNativePodCannotStealManagedVolume(t *testing.T) {
	p := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n", Labels: map[string]string{kube.ManagedLabel: "true"}}}
	h := &Handler{Ledger: permits(false), API: fake.NewClientset(p)}
	obj, _ := json.Marshal(v1.Pod{Spec: v1.PodSpec{Volumes: []v1.Volume{{Name: "data", VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: "p"}}}}}})
	ok, _ := h.check(context.Background(), &admission.AdmissionRequest{Kind: metav1.GroupVersionKind{Kind: "Pod"}, Namespace: "n", Operation: admission.Create, Object: runtime.RawExtension{Raw: obj}})
	if ok {
		t.Fatal("native Pod stole SSF volume")
	}
}

func TestSelectedNodeNeedsPlacementPermit(t *testing.T) {
	old := v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n", UID: "uid", Labels: map[string]string{kube.ManagedLabel: "true"}}}
	next := old.DeepCopy()
	next.Annotations = map[string]string{"volume.kubernetes.io/selected-node": "worker"}
	before, _ := json.Marshal(old)
	after, _ := json.Marshal(next)
	req := &admission.AdmissionRequest{Kind: metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"}, Operation: admission.Update, OldObject: runtime.RawExtension{Raw: before}, Object: runtime.RawExtension{Raw: after}}
	h := &Handler{Ledger: permits(false), API: fake.NewClientset()}
	if ok, _ := h.check(context.Background(), req); ok {
		t.Fatal("selection without permit")
	}
	h.Ledger = permits(true)
	if ok, _ := h.check(context.Background(), req); !ok {
		t.Fatal("valid selection denied")
	}
}
