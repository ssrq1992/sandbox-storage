package guard

import (
	"context"
	"encoding/json"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"reflect"

	admission "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kube "ssf.dev/ssf/kubernetes"
)

type snapshotPermits interface {
	SnapshotDeleteAllowed(context.Context, string) (bool, error)
	SnapshotTracked(context.Context, string) (bool, error)
}

func (h *Handler) checkSnapshot(ctx context.Context, r *admission.AdmissionRequest) (bool, string) {
	var old, next unstructured.Unstructured
	if len(r.OldObject.Raw) > 0 && json.Unmarshal(r.OldObject.Raw, &old) != nil {
		return false, "invalid old snapshot"
	}
	if len(r.Object.Raw) > 0 && json.Unmarshal(r.Object.Raw, &next) != nil {
		return false, "invalid snapshot"
	}
	managed := old.GetLabels()[kube.ManagedLabel] == "true" || next.GetLabels()[kube.ManagedLabel] == "true"
	p, authority := h.Ledger.(snapshotPermits)
	uid := string(old.GetUID())
	if uid == "" {
		uid = string(next.GetUID())
	}
	if authority && uid != "" {
		tracked, err := p.SnapshotTracked(ctx, uid)
		if err != nil {
			return false, "checkpoint authority unavailable"
		}
		managed = managed || tracked
	}
	// Snapshot-controller does not copy labels from Snapshot to Content.
	// Resolve its immutable reference before permitting mutation or deletion.
	if r.Kind.Kind == "VolumeSnapshotContent" && !managed {
		obj := old
		if obj.Object == nil {
			obj = next
		}
		ns, _, _ := unstructured.NestedString(obj.Object, "spec", "volumeSnapshotRef", "namespace")
		name, _, _ := unstructured.NestedString(obj.Object, "spec", "volumeSnapshotRef", "name")
		refUID, _, _ := unstructured.NestedString(obj.Object, "spec", "volumeSnapshotRef", "uid")
		if authority && refUID != "" {
			tracked, err := p.SnapshotTracked(ctx, refUID)
			if err != nil {
				return false, "checkpoint authority unavailable"
			}
			managed = tracked
		}
		if !managed && ns != "" && name != "" {
			if h.Dynamic == nil {
				return false, "snapshot lookup unavailable"
			}
			snap, err := h.Dynamic.Resource(schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) && authority && refUID != "" {
				// A managed Snapshot cannot disappear without a durable delete
				// tombstone, checked above. Preserve native GC for other CSI users.
				return true, ""
			}
			if err != nil {
				return false, "cannot establish snapshot ownership"
			}
			if refUID != "" && string(snap.GetUID()) != refUID {
				return false, "snapshot reference identity changed"
			}
			managed = snap.GetLabels()[kube.ManagedLabel] == "true"
		}
	}
	if !managed {
		return true, ""
	}
	if r.Operation == admission.Create && r.Kind.Kind == "VolumeSnapshotContent" {
		policy, _, _ := unstructured.NestedString(next.Object, "spec", "deletionPolicy")
		if policy != "Retain" {
			return false, "managed checkpoint content must start with Retain policy"
		}
	}
	if r.Operation == admission.Delete {
		p, ok := h.Ledger.(snapshotPermits)
		if !ok {
			return false, "checkpoint authority unavailable"
		}
		allowed, err := p.SnapshotDeleteAllowed(ctx, string(old.GetUID()))
		if err != nil || !allowed {
			return false, "checkpoint references must be released before deletion"
		}
		return true, ""
	}
	if r.Operation == admission.Update {
		if old.GetLabels()[kube.ManagedLabel] == "true" && next.GetLabels()[kube.ManagedLabel] != "true" {
			return false, "cannot remove checkpoint protection"
		}
		if old.GetAnnotations()["ssf.dev/source-pvc-uid"] != next.GetAnnotations()["ssf.dev/source-pvc-uid"] {
			return false, "snapshot source UID annotation is immutable"
		}
		if !reflect.DeepEqual(old.Object["spec"], next.Object["spec"]) {
			// The sole mutable spec field is Content's Retain -> Delete transition,
			// and only after the shared ledger has tombstoned it for reclamation.
			if r.Kind.Kind != "VolumeSnapshotContent" || !authority {
				return false, "managed snapshot source and identity are immutable"
			}
			before, after := old.DeepCopy(), next.DeepCopy()
			a, _, _ := unstructured.NestedString(before.Object, "spec", "deletionPolicy")
			b, _, _ := unstructured.NestedString(after.Object, "spec", "deletionPolicy")
			unstructured.RemoveNestedField(before.Object, "spec", "deletionPolicy")
			unstructured.RemoveNestedField(after.Object, "spec", "deletionPolicy")
			if a != "Retain" || b != "Delete" || !reflect.DeepEqual(before.Object["spec"], after.Object["spec"]) {
				return false, "managed snapshot source and identity are immutable"
			}
			allowed, err := p.SnapshotDeleteAllowed(ctx, uid)
			if err != nil || !allowed {
				return false, "checkpoint references prevent reclaim policy change"
			}
		}
	}
	return true, ""
}
