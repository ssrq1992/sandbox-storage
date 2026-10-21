package kubernetes

import (
	"context"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	"ssf.dev/ssf/core"
)

var snapshots = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}

type SnapshotState struct {
	UID, Content, ContentUID, Reason string
	SourceVolumeMode                 string
	Ready                            bool
}

func (c *Client) SnapshotStatus(ctx context.Context, namespace, name string) (SnapshotState, error) {
	var s SnapshotState
	if c.Dynamic == nil {
		return s, errors.New("snapshot client unavailable")
	}
	v, e := c.Dynamic.Resource(snapshots).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if e != nil {
		return s, e
	}
	if v.GetLabels()[ManagedLabel] != "true" || v.GetDeletionTimestamp() != nil {
		return s, errors.New("snapshot is unprotected or deleting")
	}
	s.UID = string(v.GetUID())
	s.Ready, _, _ = unstructured.NestedBool(v.Object, "status", "readyToUse")
	s.Content, _, _ = unstructured.NestedString(v.Object, "status", "boundVolumeSnapshotContentName")
	s.Reason, _, _ = unstructured.NestedString(v.Object, "status", "error", "message")
	if s.Content != "" {
		content, err := c.Dynamic.Resource(snapshotContents).Get(ctx, s.Content, metav1.GetOptions{})
		if err != nil {
			return s, err
		}
		refUID, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "uid")
		refName, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "name")
		refNamespace, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
		if refUID != s.UID || refName != name || refNamespace != namespace || content.GetDeletionTimestamp() != nil {
			return s, core.ErrConflict
		}
		s.SourceVolumeMode, _, _ = unstructured.NestedString(content.Object, "spec", "sourceVolumeMode")
		s.ContentUID = string(content.GetUID())
		if s.ContentUID == "" {
			return s, errors.New("snapshot content UID unavailable")
		}
	}
	if s.Ready && (s.UID == "" || s.ContentUID == "") {
		return s, errors.New("ready snapshot lacks bound content identity")
	}
	return s, nil
}
func (c *Client) DeleteSnapshot(ctx context.Context, namespace, name, uid string) error {
	if c.Dynamic == nil || uid == "" {
		return errors.New("snapshot client and UID required")
	}
	id := types.UID(uid)
	return c.Dynamic.Resource(snapshots).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &id}})
}

var snapshotContents = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents"}
var snapshotClasses = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotclasses"}

const snapshotSourceUID = "ssf.dev/source-pvc-uid"

// Snapshot writes only native resources. The snapshot-controller/CSI sidecar
// owns CreateSnapshot; callers must keep their native capture barrier until all
// members report ready and the checkpoint is jointly committed.
func (c *Client) Snapshot(ctx context.Context, source core.PVCRef, name, class string) error {
	if c.Dynamic == nil || source.UID == "" || name == "" || class == "" {
		return errors.New("snapshot client, pinned source, name and class required")
	}
	p, err := c.API.CoreV1().PersistentVolumeClaims(source.Namespace).Get(ctx, source.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(p.UID) != source.UID {
		return core.ErrConflict
	}
	if p.Labels[ManagedLabel] != "true" || p.DeletionTimestamp != nil {
		return errors.New("snapshot source is unprotected or deleting")
	}
	cls, err := c.Dynamic.Resource(snapshotClasses).Get(ctx, class, metav1.GetOptions{})
	if err != nil {
		return err
	}
	policy, _, _ := unstructured.NestedString(cls.Object, "deletionPolicy")
	if policy != "Retain" {
		return errors.New("checkpoint snapshot class must use Retain")
	}
	desired := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
		"metadata": map[string]any{"name": name, "namespace": source.Namespace, "labels": map[string]any{ManagedLabel: "true"}, "annotations": map[string]any{snapshotSourceUID: source.UID}},
		"spec":     map[string]any{"volumeSnapshotClassName": class, "source": map[string]any{"persistentVolumeClaimName": source.Name}},
	}}
	_, err = c.Dynamic.Resource(snapshots).Namespace(source.Namespace).Create(ctx, desired, metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	prior, err := c.Dynamic.Resource(snapshots).Namespace(source.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if prior.GetDeletionTimestamp() != nil || prior.GetAnnotations()[snapshotSourceUID] != source.UID || prior.GetLabels()[ManagedLabel] != "true" || !reflect.DeepEqual(prior.Object["spec"], desired.Object["spec"]) {
		return core.ErrConflict
	}
	return nil
}

// ReclaimCapturedSnapshot uses Kubernetes snapshot-controller and external
// snapshotter for backend deletion. Exact object UIDs and reference identity
// remain required even after one half of the pair has disappeared on a retry.
func (c *Client) ReclaimCapturedSnapshot(ctx context.Context, namespace, name, uid, contentName, contentUID string) error {
	if c.Dynamic == nil || namespace == "" || name == "" || uid == "" || contentUID == "" {
		return errors.New("complete snapshot reclaim identity required")
	}
	snapshot, err := c.Dynamic.Resource(snapshots).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if apierrors.IsNotFound(err) {
		snapshot = nil
	}
	if err == nil {
		if string(snapshot.GetUID()) != uid {
			return core.ErrConflict
		}
		linked, _, _ := unstructured.NestedString(snapshot.Object, "status", "boundVolumeSnapshotContentName")
		if linked == "" || (contentName != "" && linked != contentName) {
			return core.ErrConflict
		}
		contentName = linked
	}
	if contentName == "" {
		// Older capture manifests retained the immutable UID only. Never substitute
		// an object by a reused name; identify its surviving Content by that UID.
		items, e := c.Dynamic.Resource(snapshotContents).List(ctx, metav1.ListOptions{})
		if e != nil {
			return e
		}
		for _, item := range items.Items {
			if string(item.GetUID()) == contentUID {
				contentName = item.GetName()
				break
			}
		}
		if contentName == "" && apierrors.IsNotFound(err) {
			return nil
		}
		if contentName == "" {
			return core.ErrConflict
		}
	}
	content, contentErr := c.Dynamic.Resource(snapshotContents).Get(ctx, contentName, metav1.GetOptions{})
	if contentErr != nil && !apierrors.IsNotFound(contentErr) {
		return contentErr
	}
	if contentErr == nil {
		refUID, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "uid")
		refName, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "name")
		refNamespace, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
		if string(content.GetUID()) != contentUID || refUID != uid || refName != name || refNamespace != namespace {
			return core.ErrConflict
		}
		policy, _, _ := unstructured.NestedString(content.Object, "spec", "deletionPolicy")
		if policy != "Delete" {
			if policy != "Retain" || content.GetDeletionTimestamp() != nil {
				return core.ErrConflict
			}
			if e := unstructured.SetNestedField(content.Object, "Delete", "spec", "deletionPolicy"); e != nil {
				return e
			}
			if _, e := c.Dynamic.Resource(snapshotContents).Update(ctx, content, metav1.UpdateOptions{}); e != nil {
				return e
			}
		}
	}
	if snapshot != nil {
		if e := c.DeleteSnapshot(ctx, namespace, name, uid); e != nil && !apierrors.IsNotFound(e) {
			return e
		}
	}
	if contentErr == nil {
		pinned := types.UID(contentUID)
		if e := c.Dynamic.Resource(snapshotContents).Delete(ctx, contentName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pinned}}); e != nil && !apierrors.IsNotFound(e) {
			return e
		}
	}
	// DELETE acceptance is not proof that the sidecar finished backend reclaim.
	if _, e := c.Dynamic.Resource(snapshots).Namespace(namespace).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(e) {
		if e != nil {
			return e
		}
		return core.ErrPending
	}
	if _, e := c.Dynamic.Resource(snapshotContents).Get(ctx, contentName, metav1.GetOptions{}); !apierrors.IsNotFound(e) {
		if e != nil {
			return e
		}
		return core.ErrPending
	}
	return nil
}

// ObserveSnapshotRetirement also sees terminating objects and Content created
// after a timed-out Snapshot create. It never treats a reused name as identity.
func (c *Client) ObserveSnapshotRetirement(ctx context.Context, namespace, name, sourceUID, expectedUID string) (SnapshotState, error) {
	state := SnapshotState{UID: expectedUID}
	if c.Dynamic == nil {
		return state, errors.New("snapshot client unavailable")
	}
	snapshot, err := c.Dynamic.Resource(snapshots).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return state, err
	}
	if err == nil {
		if snapshot.GetLabels()[ManagedLabel] != "true" || snapshot.GetAnnotations()[snapshotSourceUID] != sourceUID || (expectedUID != "" && string(snapshot.GetUID()) != expectedUID) {
			return state, core.ErrConflict
		}
		state.UID = string(snapshot.GetUID())
	}
	if state.UID == "" {
		return state, nil
	}
	contents, err := c.Dynamic.Resource(snapshotContents).List(ctx, metav1.ListOptions{})
	if err != nil {
		return state, err
	}
	for _, content := range contents.Items {
		refUID, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "uid")
		if refUID != state.UID {
			continue
		}
		refName, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "name")
		refNamespace, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
		if refName != name || refNamespace != namespace || state.ContentUID != "" {
			return state, core.ErrConflict
		}
		state.Content = content.GetName()
		state.ContentUID = string(content.GetUID())
	}
	return state, nil
}
