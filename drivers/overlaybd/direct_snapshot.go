package overlaybd

import (
	"context"
	"errors"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kschema "k8s.io/apimachinery/pkg/runtime/schema"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
)

var directSnapshotResource = kschema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}
var directContentResource = kschema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents"}

func snapshotField(o *unstructured.Unstructured, path ...string) string {
	v, _, _ := unstructured.NestedString(o.Object, path...)
	return v
}

// resolveDirect installs Guard-visible identities before a second live API read.
// Unlabelled native snapshots are supported; a managed checkpoint cannot fall
// back here when its verified native barrier is absent.
func (a *KubernetesCaptureAuthority) resolveDirect(ctx context.Context, request, handle string, parameters map[string]string) (SnapshotPin, error) {
	var result SnapshotPin
	ns, name, cn := parameters[snapshotNamespaceKey], parameters[snapshotNameKey], parameters[snapshotContentKey]
	snapshot, err := a.Kube.Dynamic.Resource(directSnapshotResource).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return result, err
	}
	if snapshot.GetLabels()[kube.ManagedLabel] == "true" {
		return result, core.ErrPending
	}
	content, err := a.Kube.Dynamic.Resource(directContentResource).Get(ctx, cn, metav1.GetOptions{})
	if err != nil {
		return result, err
	}
	if snapshot.GetUID() == "" || content.GetUID() == "" || snapshot.GetDeletionTimestamp() != nil || content.GetDeletionTimestamp() != nil {
		return result, core.ErrConflict
	}
	if !slices.Contains(snapshot.GetFinalizers(), kube.SnapshotBoundFinalizer) || !slices.Contains(content.GetFinalizers(), kube.SnapshotContentBoundFinalizer) {
		return result, core.ErrPending
	}
	className := snapshotField(snapshot, "spec", "volumeSnapshotClassName")
	sourceName := snapshotField(snapshot, "spec", "source", "persistentVolumeClaimName")
	if className == "" || sourceName == "" || snapshotField(content, "spec", "volumeSnapshotClassName") != className || snapshotField(content, "spec", "driver") != DriverName || snapshotField(content, "spec", "source", "volumeHandle") != handle || snapshotField(content, "spec", "sourceVolumeMode") != "Block" || snapshotField(content, "spec", "volumeSnapshotRef", "uid") != string(snapshot.GetUID()) || snapshotField(content, "spec", "volumeSnapshotRef", "name") != name || snapshotField(content, "spec", "volumeSnapshotRef", "namespace") != ns {
		return result, core.ErrConflict
	}
	classes := kschema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotclasses"}
	class, err := a.Kube.Dynamic.Resource(classes).Get(ctx, className, metav1.GetOptions{})
	if err != nil {
		return result, err
	}
	policy := snapshotField(content, "spec", "deletionPolicy")
	if snapshotField(class, "driver") != DriverName || snapshotField(class, "deletionPolicy") != policy || (policy != "Retain" && policy != "Delete") {
		return result, core.ErrConflict
	}
	volume, err := a.Kube.Resolve(ctx, core.PVCRef{Namespace: ns, Name: sourceName}, true)
	if err != nil {
		return result, err
	}
	if volume.Driver != DriverName || volume.Handle != handle || volume.Capability == nil || volume.Capability.GetBlock() == nil {
		return result, core.ErrConflict
	}
	ref := core.DirectSnapshotRef{ID: Identity("snapshot", request), Namespace: ns, Name: name, SnapshotUID: string(snapshot.GetUID()), ContentName: cn, ContentUID: string(content.GetUID()), Volume: volume, CreatedUnixNano: content.GetCreationTimestamp().Time.UnixNano()}
	if err = a.Ledger.TrackDirectSnapshot(ctx, ref); err != nil {
		return result, err
	}
	// Do not rely on a finalizer observed before Guard tracking was installed.
	again, err := a.Kube.Dynamic.Resource(directSnapshotResource).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return result, err
	}
	next, err := a.Kube.Dynamic.Resource(directContentResource).Get(ctx, cn, metav1.GetOptions{})
	if err != nil {
		return result, err
	}
	if again.GetDeletionTimestamp() != nil || next.GetDeletionTimestamp() != nil {
		return result, core.ErrConflict
	}
	if again.GetUID() != snapshot.GetUID() || next.GetUID() != content.GetUID() || !slices.Contains(again.GetFinalizers(), kube.SnapshotBoundFinalizer) || !slices.Contains(next.GetFinalizers(), kube.SnapshotContentBoundFinalizer) || core.Digest(again.Object["spec"]) != core.Digest(snapshot.Object["spec"]) || core.Digest(next.Object["spec"]) != core.Digest(content.Object["spec"]) {
		return result, core.ErrConflict
	}
	if a.Live == nil {
		return a.Idle.Capture(ctx, IdleSnapshotRequest{Name: request, Volume: volume, SnapshotUID: ref.SnapshotUID, ContentUID: ref.ContentUID, CreatedUnixNano: ref.CreatedUnixNano})
	}
	// Ask the persisted live operation first. Once selected, an owner stopping
	// cannot silently switch this snapshot to a later idle published head.
	capture, err := a.Live.Capture(ctx, ref)
	if errors.Is(err, ErrIdleCaptureEligible) {
		return a.Idle.Capture(ctx, IdleSnapshotRequest{Name: request, Volume: volume, SnapshotUID: ref.SnapshotUID, ContentUID: ref.ContentUID, CreatedUnixNano: ref.CreatedUnixNano})
	}
	if err != nil {
		return result, err
	}
	if capture.PVC != volume.PVC || capture.Driver != volume.Driver || capture.Handle != volume.Handle {
		return result, core.ErrConflict
	}
	// The runtime has retained the completed joint checkpoint for this direct
	// reference. Do not use a current head or treat an annotation as evidence.
	result = SnapshotPin{ID: ref.ID, RequestName: request, SourceHandle: handle, HeadRevision: capture.HeadRevision, CapacityBytes: capture.CapacityBytes, FSUUID: capture.FilesystemUUID, SnapshotUID: ref.SnapshotUID, ContentUID: ref.ContentUID, CaptureDigest: capture.CaptureDigest, CreatedUnixNano: ref.CreatedUnixNano}
	return result, result.validate()
}

// Retire runs after Driver deletion (including unknown-ID tombstones). Never
// release a source reference merely because native Kubernetes requested DELETE.
func (a *KubernetesCaptureAuthority) Retire(ctx context.Context, id string) error {
	if a.Ledger == nil {
		return errors.New("snapshot ledger unavailable")
	}
	if err := a.Ledger.RetireIdleCapture(ctx, id); err != nil {
		return err
	}
	return a.Ledger.RetireDirectSnapshot(ctx, id)
}
