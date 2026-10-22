package overlaybd

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"math"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kschema "k8s.io/apimachinery/pkg/runtime/schema"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

const snapshotNameKey = "csi.storage.k8s.io/volumesnapshot/name"
const snapshotNamespaceKey = "csi.storage.k8s.io/volumesnapshot/namespace"
const snapshotContentKey = "csi.storage.k8s.io/volumesnapshotcontent/name"

// KubernetesCaptureAuthority accepts only a runtime-verified durable capture
// with matching live Kubernetes identities. Metadata selects records; it never
// grants capture permission or permits choosing the latest head of a live VM.
type KubernetesCaptureAuthority struct {
	Live   LiveSnapshotSource
	Idle   *IdleSnapshots
	Kube   *kube.Client
	Ledger *ledger.Store
}

func (a *KubernetesCaptureAuthority) Resolve(ctx context.Context, request, handle string, parameters map[string]string) (SnapshotPin, error) {
	var p SnapshotPin
	if a.Kube == nil || a.Kube.Dynamic == nil || a.Kube.API == nil || a.Ledger == nil {
		return p, errors.New("snapshot authority unavailable")
	}
	if err := a.Kube.CheckSnapshotProtection(ctx); err != nil {
		return p, err
	}
	ns, name, contentName := parameters[snapshotNamespaceKey], parameters[snapshotNameKey], parameters[snapshotContentKey]
	if ns == "" || name == "" || contentName == "" {
		return p, errors.New("snapshotter metadata required")
	}
	capture, err := a.Ledger.SnapshotCapture(ctx, ns, name)
	if errors.Is(err, core.ErrPending) && a.Idle != nil {
		return a.resolveDirect(ctx, request, handle, parameters)
	}
	if err != nil {
		return p, err
	}
	if capture.Driver != DriverName || capture.Handle != handle || capture.CapacityBytes > math.MaxInt64 {
		return p, core.ErrConflict
	}
	// The coordinator must have fixed the first observed Snapshot UID. The
	// sidecar may race that write; wait rather than adopting a replacement.
	var fixed kube.SnapshotState
	err = a.Ledger.ReadStep(ctx, "checkpoint/snapshot/"+capture.CheckpointID+"/"+capture.BindingID, &fixed)
	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, core.ErrPending) {
		return p, core.ErrPending
	}
	if err != nil {
		return p, err
	}
	if fixed.UID == "" {
		return p, core.ErrConflict
	}
	gvr := kschema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}
	snapshot, err := a.Kube.Dynamic.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return p, err
	}
	gvr.Resource = "volumesnapshotcontents"
	content, err := a.Kube.Dynamic.Resource(gvr).Get(ctx, contentName, metav1.GetOptions{})
	if err != nil {
		return p, err
	}
	get := func(object *unstructured.Unstructured, fields ...string) string {
		v, _, _ := unstructured.NestedString(object.Object, fields...)
		return v
	}
	if string(snapshot.GetUID()) != fixed.UID || (fixed.ContentUID != "" && string(content.GetUID()) != fixed.ContentUID) || content.GetUID() == "" || snapshot.GetDeletionTimestamp() != nil || content.GetDeletionTimestamp() != nil || snapshot.GetLabels()[kube.ManagedLabel] != "true" || snapshot.GetAnnotations()["ssf.dev/source-pvc-uid"] != capture.PVC.UID || get(snapshot, "spec", "source", "persistentVolumeClaimName") != capture.PVC.Name || get(content, "spec", "driver") != DriverName || get(content, "spec", "source", "volumeHandle") != handle || get(content, "spec", "sourceVolumeMode") != "Block" || get(content, "spec", "volumeSnapshotRef", "name") != name || get(content, "spec", "volumeSnapshotRef", "namespace") != ns || get(content, "spec", "volumeSnapshotRef", "uid") != string(snapshot.GetUID()) {
		return p, core.ErrConflict
	}
	pvc, err := a.Kube.API.CoreV1().PersistentVolumeClaims(ns).Get(ctx, capture.PVC.Name, metav1.GetOptions{})
	if err != nil {
		return p, err
	}
	if string(pvc.UID) != capture.PVC.UID || pvc.DeletionTimestamp != nil {
		return p, core.ErrConflict
	}
	created := content.GetCreationTimestamp().Time.UnixNano()
	if created <= 0 {
		return p, errors.New("snapshot content creation time missing")
	}
	if err = a.Ledger.FixSnapshotContent(ctx, capture.CheckpointID, capture.BindingID, string(content.GetUID())); err != nil {
		return p, err
	}
	// Pins are installed before the driver exposes a snapshot handle, including
	// the window before the coordinator observes Content.status.
	if err = a.Ledger.PinCheckpoint(ctx, capture.CheckpointID, string(snapshot.GetUID()), string(content.GetUID())); err != nil {
		return p, err
	}
	p = SnapshotPin{ID: Identity("snapshot", request), RequestName: request, SourceHandle: handle, HeadRevision: capture.HeadRevision, CapacityBytes: capture.CapacityBytes, FSUUID: capture.FilesystemUUID, SnapshotUID: string(snapshot.GetUID()), ContentUID: string(content.GetUID()), CaptureDigest: capture.CaptureDigest, CreatedUnixNano: created}
	return p, p.validate()
}
func (a *KubernetesCaptureAuthority) Delete(ctx context.Context, p SnapshotPin) error {
	if a.Ledger == nil {
		return errors.New("snapshot ledger unavailable")
	}
	if err := a.Ledger.PermitSnapshotDelete(ctx, p.SnapshotUID); err != nil {
		return err
	}
	return a.Ledger.PermitSnapshotDelete(ctx, p.ContentUID)
}
