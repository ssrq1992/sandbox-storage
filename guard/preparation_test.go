package guard

import (
	"context"
	"encoding/json"
	"testing"

	admission "k8s.io/api/admission/v1"
	authentication "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
)

func TestProfiledPVCProtection(t *testing.T) {
	ctx := context.Background()
	profile := core.VolumeProfile{Name: "ext4", Runtime: "agentenv", Version: core.PreparationProtocol, VolumeMode: "Block", Filesystem: "ext4", InitializeBlank: true}
	retain := v1.PersistentVolumeReclaimRetain
	class := "vendor"
	api := fake.NewClientset(&storage.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: class, UID: "class-uid", Annotations: map[string]string{kube.VolumeProfileAnnotation: profile.Name}}, Provisioner: "vendor.csi", ReclaimPolicy: &retain})
	h := &Handler{API: api, Ledger: permits(false), VolumeProfiles: []core.VolumeProfile{profile}, ManagedNamespaces: []string{"tenant"}, CoordinatorUsers: []string{"preparer"}}
	claim := v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "tenant", UID: "uid"}, Spec: v1.PersistentVolumeClaimSpec{StorageClassName: &class}}
	raw, _ := json.Marshal(claim)
	req := &admission.AdmissionRequest{Kind: metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"}, Namespace: "tenant", Operation: admission.Create, Object: runtime.RawExtension{Raw: raw}}
	patch, err := h.mutate(ctx, req)
	if err != nil || len(patch) == 0 {
		t.Fatalf("defaulting: %s %v", patch, err)
	}
	// Validate the desired result without depending on a JSON-patch library.
	mode := v1.PersistentVolumeBlock
	claim.Spec.VolumeMode = &mode
	claim.Labels = map[string]string{kube.ManagedLabel: "true"}
	claim.Annotations = map[string]string{kube.VolumeProfileAnnotation: profile.Name, kube.ProfileRevisionAnnotation: core.Digest(profile), kube.ProfileClassUIDAnnotation: "class-uid"}
	claim.Annotations[kube.SourceRevisionAnnotation] = core.Digest("blank")
	claim.Finalizers = []string{kube.VolumeProtectionFinalizer}
	raw, _ = json.Marshal(claim)
	req.Object.Raw = raw
	if ok, reason := h.check(ctx, req); !ok {
		t.Fatal(reason)
	}
	req.Operation = admission.Delete
	req.OldObject.Raw = raw
	if ok, reason := h.check(ctx, req); !ok {
		t.Fatalf("native DELETE should enter finalizer drain: %s", reason)
	}
	req.Operation = admission.Update
	next := claim.DeepCopy()
	next.Annotations[kube.PreparationStateAnnotation] = "VolumeReady"
	req.Object.Raw, _ = json.Marshal(next)
	if ok, _ := h.check(ctx, req); ok {
		t.Fatal("client forged readiness")
	}
	req.UserInfo = authentication.UserInfo{Username: "preparer"}
	if ok, reason := h.check(ctx, req); !ok {
		t.Fatal(reason)
	}
	next.Finalizers = nil
	req.Object.Raw, _ = json.Marshal(next)
	if ok, _ := h.check(ctx, req); ok {
		t.Fatal("coordinator removed finalizer before DELETE/drain")
	}
}
