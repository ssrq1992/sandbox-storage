package kubernetes

import (
	"context"
	"errors"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	"ssf.dev/ssf/core"
)

func TestSnapshotPinsSourceIdentityAndRequiresRetainClass(t *testing.T) {
	ctx := context.Background()
	pvc := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "ns", UID: "source-uid", Labels: map[string]string{ManagedLabel: "true"}}}
	cls := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass", "metadata": map[string]any{"name": "retain"}, "deletionPolicy": "Retain", "driver": "test.csi"}}
	c := &Client{API: fake.NewClientset(pvc), Dynamic: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cls)}
	source := core.PVCRef{Namespace: "ns", Name: "source", UID: "source-uid"}
	for i := 0; i < 2; i++ {
		if err := c.Snapshot(ctx, source, "checkpoint", "retain"); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := c.Dynamic.Resource(snapshots).Namespace("ns").Get(ctx, "checkpoint", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if snap.GetLabels()[ManagedLabel] != "true" || snap.GetAnnotations()[snapshotSourceUID] != source.UID {
		t.Fatal("snapshot unprotected")
	}
	source.UID = "recreated"
	if err = c.Snapshot(ctx, source, "checkpoint", "retain"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("retargeted source: %v", err)
	}
	cls.Object["deletionPolicy"] = "Delete"
	if _, err = c.Dynamic.Resource(snapshotClasses).Update(ctx, cls, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	source.UID = "source-uid"
	if err = c.Snapshot(ctx, source, "other", "retain"); err == nil {
		t.Fatal("accepted unprotected deletion policy")
	}
}

func TestSnapshotReadyRequiresMatchingContentUID(t *testing.T) {
	ctx := context.Background()
	snap := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot", "metadata": map[string]any{"name": "snap", "namespace": "ns", "uid": "snap-uid", "labels": map[string]any{ManagedLabel: "true"}}, "status": map[string]any{"readyToUse": true, "boundVolumeSnapshotContentName": "content"}}}
	content := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent", "metadata": map[string]any{"name": "content", "uid": "content-uid"}, "spec": map[string]any{"volumeSnapshotRef": map[string]any{"name": "snap", "namespace": "ns", "uid": "other-uid"}}}}
	c := &Client{Dynamic: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), snap, content)}
	if _, err := c.SnapshotStatus(ctx, "ns", "snap"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("wrong content reference: %v", err)
	}
	_ = unstructured.SetNestedField(content.Object, "snap-uid", "spec", "volumeSnapshotRef", "uid")
	if _, err := c.Dynamic.Resource(snapshotContents).Update(ctx, content, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	state, err := c.SnapshotStatus(ctx, "ns", "snap")
	if err != nil {
		t.Fatal(err)
	}
	if !state.Ready || state.ContentUID != "content-uid" {
		t.Fatalf("missing durable content identity: %+v", state)
	}
}
