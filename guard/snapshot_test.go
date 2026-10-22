package guard

import (
	"context"
	"encoding/json"
	"testing"

	admission "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kube "ssf.dev/ssf/kubernetes"
)

type checkpointPermits struct {
	permits
	tracked, deletion bool
}

func (p checkpointPermits) SnapshotTracked(context.Context, string) (bool, error) {
	return p.tracked, nil
}
func (p checkpointPermits) SnapshotDeleteAllowed(context.Context, string) (bool, error) {
	return p.deletion, nil
}
func snapshotRequest(kind string, op admission.Operation, old, next map[string]any) *admission.AdmissionRequest {
	var a, b []byte
	if old != nil {
		old["apiVersion"] = "snapshot.storage.k8s.io/v1"
		old["kind"] = kind
		a, _ = json.Marshal(old)
	}
	if next != nil {
		next["apiVersion"] = "snapshot.storage.k8s.io/v1"
		next["kind"] = kind
		b, _ = json.Marshal(next)
	}
	return &admission.AdmissionRequest{Kind: metav1.GroupVersionKind{Kind: kind}, Operation: op, OldObject: runtime.RawExtension{Raw: a}, Object: runtime.RawExtension{Raw: b}}
}
func TestSnapshotLedgerProtectionSurvivesMissingLabel(t *testing.T) {
	obj := map[string]any{"metadata": map[string]any{"uid": "uid"}, "spec": map[string]any{"source": "original"}}
	h := &Handler{Ledger: checkpointPermits{tracked: true}}
	r := snapshotRequest("VolumeSnapshot", admission.Delete, obj, nil)
	if ok, _ := h.check(context.Background(), r); ok {
		t.Fatal("deleted tracked snapshot without permit")
	}
	h.Ledger = checkpointPermits{tracked: true, deletion: true}
	if ok, reason := h.check(context.Background(), r); !ok {
		t.Fatal(reason)
	}
	next := map[string]any{"metadata": obj["metadata"], "spec": map[string]any{"source": "replacement"}}
	if ok, _ := h.check(context.Background(), snapshotRequest("VolumeSnapshot", admission.Update, obj, next)); ok {
		t.Fatal("replaced pinned source")
	}
}
func TestUnlabelledContentInheritsSnapshotProtection(t *testing.T) {
	snap := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot", "metadata": map[string]any{"name": "snap", "namespace": "ns", "uid": "snap-uid", "labels": map[string]any{kube.ManagedLabel: "true"}}}}
	h := &Handler{Ledger: checkpointPermits{}, Dynamic: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), snap)}
	obj := map[string]any{"metadata": map[string]any{"uid": "content-uid"}, "spec": map[string]any{"volumeSnapshotRef": map[string]any{"name": "snap", "namespace": "ns", "uid": "snap-uid"}, "deletionPolicy": "Retain"}}
	if ok, _ := h.check(context.Background(), snapshotRequest("VolumeSnapshotContent", admission.Delete, obj, nil)); ok {
		t.Fatal("deleted content before pin registration")
	}
	next := map[string]any{"metadata": obj["metadata"], "spec": map[string]any{"volumeSnapshotRef": obj["spec"].(map[string]any)["volumeSnapshotRef"], "deletionPolicy": "Delete"}}
	if ok, _ := h.check(context.Background(), snapshotRequest("VolumeSnapshotContent", admission.Update, obj, next)); ok {
		t.Fatal("changed deletion policy")
	}
	if ok, _ := h.check(context.Background(), snapshotRequest("VolumeSnapshotContent", admission.Create, nil, next)); ok {
		t.Fatal("created managed content with Delete policy")
	}
	h.Ledger = checkpointPermits{tracked: true, deletion: true}
	if ok, reason := h.check(context.Background(), snapshotRequest("VolumeSnapshotContent", admission.Update, obj, next)); !ok {
		t.Fatalf("authorized reclamation denied: %s", reason)
	}
	if ok, reason := h.check(context.Background(), snapshotRequest("VolumeSnapshotContent", admission.Update, obj, obj)); !ok {
		t.Fatal(reason)
	}
}

func TestUnmanagedOrphanContentKeepsNativeGC(t *testing.T) {
	h := &Handler{Ledger: checkpointPermits{}, Dynamic: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())}
	obj := map[string]any{"metadata": map[string]any{"uid": "content"}, "spec": map[string]any{"volumeSnapshotRef": map[string]any{"name": "gone", "namespace": "ns", "uid": "unmanaged-snapshot"}}}
	if ok, reason := h.check(context.Background(), snapshotRequest("VolumeSnapshotContent", admission.Delete, obj, nil)); !ok {
		t.Fatal(reason)
	}
}

func TestManagedSnapshotFinalizerRemovalRequiresDedicatedIdentity(t *testing.T) {
	old := map[string]any{"metadata": map[string]any{"uid": "uid", "finalizers": []any{"snapshot.storage.kubernetes.io/volumesnapshotcontent-bound-protection"}}, "spec": map[string]any{"driver": "test"}}
	next := map[string]any{"metadata": map[string]any{"uid": "uid"}, "spec": map[string]any{"driver": "test"}}
	h := &Handler{Ledger: checkpointPermits{tracked: true, deletion: true}, CoordinatorUsers: []string{"coordinator"}, SnapshotControllerUsers: []string{"snapshot-controller"}}
	r := snapshotRequest("VolumeSnapshotContent", admission.Update, old, next)
	for _, user := range []string{"user", "coordinator"} {
		r.UserInfo.Username = user
		if ok, _ := h.check(context.Background(), r); ok {
			t.Fatalf("%s removed storage finalizer", user)
		}
	}
	r.UserInfo.Username = "snapshot-controller"
	if ok, reason := h.check(context.Background(), r); !ok {
		t.Fatal(reason)
	}
}

type directSnapshotPermitFixture struct {
	checkpointPermits
	pending bool
}

func (p directSnapshotPermitFixture) DirectSnapshotDeletionPending(context.Context, string) (bool, error) {
	return p.pending, nil
}
func TestDirectSnapshotDeleteRetainsNativeFinalizerBoundary(t *testing.T) {
	h := &Handler{Ledger: directSnapshotPermitFixture{checkpointPermits: checkpointPermits{tracked: true}, pending: true}}
	for _, kind := range []string{"VolumeSnapshot", "VolumeSnapshotContent"} {
		finalizer := kube.SnapshotBoundFinalizer
		if kind == "VolumeSnapshotContent" {
			finalizer = kube.SnapshotContentBoundFinalizer
		}
		old := map[string]any{"metadata": map[string]any{"uid": "uid", "finalizers": []any{finalizer}}, "spec": map[string]any{}}
		if ok, reason := h.check(context.Background(), snapshotRequest(kind, admission.Delete, old, nil)); !ok {
			t.Fatal(reason)
		}
		delete(old["metadata"].(map[string]any), "finalizers")
		if ok, _ := h.check(context.Background(), snapshotRequest(kind, admission.Delete, old, nil)); ok {
			t.Fatal("direct deletion bypassed native protection")
		}
	}
	h.Ledger = directSnapshotPermitFixture{checkpointPermits: checkpointPermits{tracked: true}, pending: false}
	old := map[string]any{"metadata": map[string]any{"uid": "uid", "finalizers": []any{kube.SnapshotBoundFinalizer}}, "spec": map[string]any{}}
	if ok, _ := h.check(context.Background(), snapshotRequest("VolumeSnapshot", admission.Delete, old, nil)); ok {
		t.Fatal("live reference ignored")
	}
}
