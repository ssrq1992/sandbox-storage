package overlaybd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	admission "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	kschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

// Exercises actual PostgreSQL and CSI handlers with Kubernetes/volume fixtures.
// It does not assert native restack, VM freeze or real OverlayBD data I/O.
func TestPostgresVerifiedCaptureToCSISnapshotCloneAndDelete(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	p := snapshotFixture(t, j)
	store := &ledger.Store{Pool: j.Pool}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := p.RequestName
	ref := core.PVCRef{Namespace: "test", Name: id, UID: id + "-pvc"}
	volume := core.Volume{UID: id + "-pv", Driver: DriverName, Handle: p.SourceHandle, PVC: ref}
	if err := store.Register(ctx, volume); err != nil {
		t.Fatal(err)
	}
	owner, run := id+"owner", id+"run"
	epoch, err := store.RunEpoch(ctx, owner, run)
	if err != nil {
		t.Fatal(err)
	}
	b := core.Binding{ID: id + "binding", Volume: volume, OwnerUID: owner, RunUID: run, Epoch: epoch, Sandbox: run, Node: "node"}
	op := id + "op"
	if err = store.Acquire(ctx, op, "input", []core.Binding{b}); err != nil {
		t.Fatal(err)
	}
	intent := core.CaptureIntent{Version: core.CaptureVersion, ID: id + "checkpoint", Operation: op, OwnerUID: owner, RunUID: run, BarrierID: "barrier", NativeArtifact: "artifact", NativeDigest: core.Hash("native"), Classes: map[string]string{b.ID: "class"}}
	if err = store.BeginCheckpoint(ctx, intent.ID, owner, intent); err != nil {
		t.Fatal(err)
	}
	capture := core.SnapshotCapture{CheckpointID: intent.ID, BindingID: b.ID, Driver: DriverName, Handle: p.SourceHandle, PVC: ref, SnapshotName: "ssf-cp-" + core.Hash(intent.ID + "/" + b.ID)[:40], CaptureID: core.Hash(intent.ID + "/" + b.ID), CaptureDigest: core.Hash("sealed"), HeadRevision: p.HeadRevision, CapacityBytes: p.CapacityBytes, FilesystemUUID: p.FSUUID}
	if err = store.RecordSnapshotCapture(ctx, intent, b, capture); err == nil {
		t.Fatal("unprotected capture source accepted")
	}
	if err = store.PinCheckpoint(ctx, intent.ID, ref.UID); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordSnapshotCapture(ctx, intent, b, capture); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordSnapshotCapture(ctx, intent, b, capture); err != nil {
		t.Fatal(err)
	}
	changed := capture
	changed.HeadRevision = core.Hash("different")
	if err = store.RecordSnapshotCapture(ctx, intent, b, changed); err == nil {
		t.Fatal("captured head changed")
	}
	snapshotUID, contentUID := id+"snapshot", id+"content"
	contentName := id + "content-name"
	snapshot := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot", "metadata": map[string]any{"namespace": ref.Namespace, "name": capture.SnapshotName, "uid": snapshotUID, "labels": map[string]any{kube.ManagedLabel: "true"}, "annotations": map[string]any{"ssf.dev/source-pvc-uid": ref.UID}}, "spec": map[string]any{"source": map[string]any{"persistentVolumeClaimName": ref.Name}}}}
	content := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent", "metadata": map[string]any{"name": contentName, "uid": contentUID}, "spec": map[string]any{"driver": DriverName, "sourceVolumeMode": "Block", "source": map[string]any{"volumeHandle": p.SourceHandle}, "volumeSnapshotRef": map[string]any{"name": capture.SnapshotName, "namespace": ref.Namespace, "uid": snapshotUID}}}}
	content.SetCreationTimestamp(metav1.NewTime(time.Now().UTC()))
	objects := dynamicfake.NewSimpleDynamicClient(kruntime.NewScheme(), snapshot, content)
	cluster := kubefake.NewSimpleClientset(&v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace, UID: types.UID(ref.UID)}})

	fail, none, all := admission.Fail, admission.SideEffectClassNone, admission.AllScopes
	_, err = cluster.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx, &admission.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "ssf-guard"}, Webhooks: []admission.ValidatingWebhook{{Name: "snapshot.ssf.dev", FailurePolicy: &fail, SideEffects: &none, ClientConfig: admission.WebhookClientConfig{Service: &admission.ServiceReference{Name: "guard", Namespace: "ssf"}, CABundle: []byte("fixture")}, Rules: []admission.RuleWithOperations{{Operations: []admission.OperationType{admission.Create, admission.Update, admission.Delete}, Rule: admission.Rule{APIGroups: []string{"snapshot.storage.k8s.io"}, APIVersions: []string{"v1"}, Resources: []string{"volumesnapshots", "volumesnapshotcontents"}, Scope: &all}}}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service := &CSISnapshots{Journal: j, Authority: &KubernetesCaptureAuthority{Kube: &kube.Client{API: cluster, Dynamic: objects}, Ledger: store}}
	request := &csi.CreateSnapshotRequest{Name: id + "csi-snapshot", SourceVolumeId: p.SourceHandle, Parameters: map[string]string{snapshotNameKey: capture.SnapshotName, snapshotNamespaceKey: ref.Namespace, snapshotContentKey: contentName}}

	if _, err := service.Authority.Resolve(ctx, request.Name, request.SourceVolumeId, request.Parameters); !errors.Is(err, core.ErrPending) {
		t.Fatalf("unfixed Snapshot UID accepted: %v", err)
	}
	var fixed kube.SnapshotState
	if err := store.Step(ctx, "checkpoint/snapshot/"+intent.ID+"/"+b.ID, core.Digest(intent), func(context.Context) (any, error) { return kube.SnapshotState{UID: snapshotUID}, nil }, &fixed); err != nil {
		t.Fatal(err)
	}
	// A valid ledger capture cannot authorize a substituted Kubernetes identity.
	resource := objects.Resource(kschema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents"})
	for _, field := range []struct {
		name  string
		path  []string
		value string
	}{
		{"snapshot UID", []string{"spec", "volumeSnapshotRef", "uid"}, "replacement"},
		{"source handle", []string{"spec", "source", "volumeHandle"}, "replacement"},
		{"driver", []string{"spec", "driver"}, "other.csi"},
		{"volume mode", []string{"spec", "sourceVolumeMode"}, "Filesystem"},
	} {
		t.Run(field.name, func(t *testing.T) {
			changed := content.DeepCopy()
			if err := unstructured.SetNestedField(changed.Object, field.value, field.path...); err != nil {
				t.Fatal(err)
			}
			if _, err := resource.Update(ctx, changed, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := service.Authority.Resolve(ctx, request.Name, request.SourceVolumeId, request.Parameters); !errors.Is(err, core.ErrConflict) {
				t.Fatalf("substituted identity accepted: %v", err)
			}
			if _, err := resource.Update(ctx, content.DeepCopy(), metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		})
	}
	replacement, err := cluster.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	replacement.UID = "recreated-pvc"
	if _, err = cluster.CoreV1().PersistentVolumeClaims(ref.Namespace).Update(ctx, replacement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Authority.Resolve(ctx, request.Name, request.SourceVolumeId, request.Parameters); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("recreated PVC accepted: %v", err)
	}
	replacement.UID = types.UID(ref.UID)
	if _, err = cluster.CoreV1().PersistentVolumeClaims(ref.Namespace).Update(ctx, replacement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	snapshotResource := objects.Resource(kschema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}).Namespace(ref.Namespace)
	substituted := snapshot.DeepCopy()
	substituted.SetUID("replacement-snapshot")
	substitutedContent := content.DeepCopy()
	if err := unstructured.SetNestedField(substitutedContent.Object, "replacement-snapshot", "spec", "volumeSnapshotRef", "uid"); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshotResource.Update(ctx, substituted, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := resource.Update(ctx, substitutedContent, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Authority.Resolve(ctx, request.Name, request.SourceVolumeId, request.Parameters); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("replacement Snapshot/Content pair accepted: %v", err)
	}
	if _, err := snapshotResource.Update(ctx, snapshot.DeepCopy(), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := resource.Update(ctx, content.DeepCopy(), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	// The sidecar fixes Content identity before the coordinator sees status.
	if _, err := service.Authority.Resolve(ctx, request.Name, request.SourceVolumeId, request.Parameters); err != nil {
		t.Fatal(err)
	}
	replacedContent := content.DeepCopy()
	replacedContent.SetUID("recreated-content")
	if _, err := resource.Update(ctx, replacedContent, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Authority.Resolve(ctx, request.Name, request.SourceVolumeId, request.Parameters); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("recreated Content accepted: %v", err)
	}
	if _, err := resource.Update(ctx, content.DeepCopy(), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateSnapshot(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !created.Snapshot.ReadyToUse || created.Snapshot.SourceVolumeId != p.SourceHandle {
		t.Fatal("incorrect snapshot")
	}
	replay, err := service.CreateSnapshot(ctx, request)
	if err != nil || replay.Snapshot.SnapshotId != created.Snapshot.SnapshotId {
		t.Fatalf("replay: %v", err)
	}
	list, err := service.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SnapshotId: created.Snapshot.SnapshotId})
	if err != nil || len(list.Entries) != 1 {
		t.Fatalf("snapshot query: %v", err)
	}
	deletion := &csi.DeleteSnapshotRequest{SnapshotId: created.Snapshot.SnapshotId}
	if _, err = service.DeleteSnapshot(ctx, deletion); err == nil {
		t.Fatal("live checkpoint lost snapshot")
	}
	backend := &readyVolumeBackend{}
	creator := &CSICreator{Journal: j, Repositories: map[string]RepositoryWorker{"repository": {ID: "repository", Nodes: []string{"node-a"}, Backend: backend}}}
	clone, err := creator.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: id + "clone", Parameters: map[string]string{"repository": "repository"}, VolumeCapabilities: []*csi.VolumeCapability{nativeBlockCapability()}, VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: created.Snapshot.SnapshotId}}}})
	if err != nil || clone.Volume.VolumeId == p.SourceHandle {
		t.Fatalf("independent clone: %v", err)
	}
	if err = store.ReleaseCheckpoint(ctx, intent.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = service.DeleteSnapshot(ctx, deletion); err != nil {
		t.Fatal(err)
	}
	if _, err = service.DeleteSnapshot(ctx, deletion); err != nil {
		t.Fatal(err)
	}
}
func TestPostgresSnapshotDeleteBeforeRegisterLeavesTombstone(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	p := snapshotFixture(t, j)
	if err := j.DeleteSnapshotPin(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if err := j.RegisterSnapshot(ctx, p); err != ErrConflict {
		t.Fatalf("late capture resurrected deleted snapshot: %v", err)
	}
}

func TestPostgresConcurrentSnapshotRegisterDeleteCannotResurrect(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	for attempt := 0; attempt < 12; attempt++ {
		p := snapshotFixture(t, j)
		start := make(chan struct{})
		registered := make(chan error, 1)
		deleted := make(chan error, 1)
		go func() { <-start; registered <- j.RegisterSnapshot(ctx, p) }()
		go func() { <-start; deleted <- j.DeleteSnapshotPin(ctx, p.ID) }()
		close(start)
		if err := <-registered; err != nil && !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
		if err := <-deleted; err != nil {
			t.Fatal(err)
		}
		if _, err := j.Snapshot(ctx, p.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleted snapshot visible: %v", err)
		}
		if err := j.RegisterSnapshot(ctx, p); !errors.Is(err, ErrConflict) {
			t.Fatalf("late registration bypassed tombstone: %v", err)
		}
	}
}
