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
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

func directFixture(t *testing.T) (*CSISnapshots, *csi.CreateSnapshotRequest, *headBackend) {
	t.Helper()
	j := database(t)
	p := snapshotFixture(t, j)
	ctx := context.Background()
	store := &ledger.Store{Pool: j.Pool}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	repo, native, err := ParseVolumeHandle(p.SourceHandle)
	if err != nil {
		t.Fatal(err)
	}
	backend := &headBackend{receipt: &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: "resolve", RepositoryId: repo, VolumeId: native, InputDigest: core.Hash("input")}, State: "ContentReady", HeadRevision: p.HeadRevision, ContentDigest: p.HeadRevision, CapacityBytes: p.CapacityBytes, FsUuid: p.FSUUID}}
	ns, name, cn := "test", p.RequestName+"snapshot", p.RequestName+"content"
	snap := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot", "metadata": map[string]any{"namespace": ns, "name": name, "uid": p.SnapshotUID}, "spec": map[string]any{"volumeSnapshotClassName": "direct", "source": map[string]any{"persistentVolumeClaimName": p.RequestName}}}}
	snap.SetFinalizers([]string{kube.SnapshotBoundFinalizer})
	content := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent", "metadata": map[string]any{"name": cn, "uid": p.ContentUID}, "spec": map[string]any{"volumeSnapshotClassName": "direct", "driver": DriverName, "deletionPolicy": "Delete", "sourceVolumeMode": "Block", "source": map[string]any{"volumeHandle": p.SourceHandle}, "volumeSnapshotRef": map[string]any{"name": name, "namespace": ns, "uid": p.SnapshotUID}}}}
	content.SetCreationTimestamp(metav1.NewTime(time.Now()))
	content.SetFinalizers([]string{kube.SnapshotContentBoundFinalizer})
	class := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass", "metadata": map[string]any{"name": "direct"}, "driver": DriverName, "deletionPolicy": "Delete"}}
	block := v1.PersistentVolumeBlock
	claim := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: p.RequestName, Namespace: ns, UID: types.UID(p.RequestName + "pvc"), Labels: map[string]string{kube.ManagedLabel: "true"}}, Spec: v1.PersistentVolumeClaimSpec{VolumeName: p.RequestName + "pv", VolumeMode: &block, AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}}, Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound}}
	pv := &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: claim.Spec.VolumeName, UID: types.UID(p.RequestName + "pv-uid"), Labels: map[string]string{kube.ManagedLabel: "true"}}, Spec: v1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimRetain, VolumeMode: &block, ClaimRef: &v1.ObjectReference{Namespace: ns, Name: claim.Name, UID: claim.UID}, PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: DriverName, VolumeHandle: p.SourceHandle}}}}
	fail, none, all := admission.Fail, admission.SideEffectClassNone, admission.AllScopes
	webhook := &admission.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "ssf-guard"}, Webhooks: []admission.ValidatingWebhook{{Name: "snapshot.ssf.dev", FailurePolicy: &fail, SideEffects: &none, ClientConfig: admission.WebhookClientConfig{Service: &admission.ServiceReference{Name: "guard", Namespace: "ssf"}, CABundle: []byte("fixture")}, Rules: []admission.RuleWithOperations{{Operations: []admission.OperationType{admission.Create, admission.Update, admission.Delete}, Rule: admission.Rule{APIGroups: []string{"snapshot.storage.k8s.io"}, APIVersions: []string{"v1"}, Resources: []string{"volumesnapshots", "volumesnapshotcontents"}, Scope: &all}}}}}}
	cluster := &kube.Client{API: kubefake.NewSimpleClientset(claim, pv, webhook), Dynamic: dynamicfake.NewSimpleDynamicClient(kruntime.NewScheme(), snap, content, class)}
	volume, err := cluster.Resolve(ctx, core.PVCRef{Namespace: ns, Name: claim.Name}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Register(ctx, volume); err != nil {
		t.Fatal(err)
	}
	authority := &KubernetesCaptureAuthority{Kube: cluster, Ledger: store, Idle: &IdleSnapshots{Journal: j, Ledger: store, Backends: map[string]HeadBackend{repo: backend}}}
	return &CSISnapshots{Journal: j, Authority: authority}, &csi.CreateSnapshotRequest{Name: p.RequestName + "csi", SourceVolumeId: p.SourceHandle, Parameters: map[string]string{snapshotNameKey: name, snapshotNamespaceKey: ns, snapshotContentKey: cn}}, backend
}

func TestPostgresDirectKubernetesIdleSnapshotAndDeletion(t *testing.T) {
	service, q, backend := directFixture(t)
	ctx := context.Background()
	a := service.Authority.(*KubernetesCaptureAuthority)
	result, err := service.CreateSnapshot(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Snapshot.ReadyToUse || backend.calls != 1 {
		t.Fatal("idle snapshot not captured once")
	}
	ref, err := a.Ledger.DirectSnapshot(ctx, result.Snapshot.SnapshotId)
	if err != nil {
		t.Fatal(err)
	}
	if tracked, err := a.Ledger.SnapshotTracked(ctx, ref.ContentUID); err != nil || !tracked {
		t.Fatalf("Content not Guard-visible: %v", err)
	}
	if allowed, err := a.Ledger.DirectSnapshotDeletionPending(ctx, ref.SnapshotUID); err != nil || !allowed {
		t.Fatalf("native DELETE not permitted: %v", err)
	}

	checkpoint := ref.ID + "consumer"
	if err = a.Ledger.BeginCheckpoint(ctx, checkpoint, "consumer", map[string]string{"source": ref.ID}); err != nil {
		t.Fatal(err)
	}
	if err = a.Ledger.PinCheckpoint(ctx, checkpoint, ref.ContentUID); err != nil {
		t.Fatal(err)
	}
	if allowed, err := a.Ledger.DirectSnapshotDeletionPending(ctx, ref.SnapshotUID); err != nil || allowed {
		t.Fatalf("Content-only reference did not protect Snapshot: %v", err)
	}
	if err = a.Ledger.ReleaseCheckpoint(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err = service.CreateSnapshot(ctx, q); err != nil || backend.calls != 1 {
		t.Fatalf("replay recaptured: %v", err)
	}
	if err = a.Ledger.Drain(ctx, ref.Volume.PVC.UID); err != nil {
		t.Fatal(err)
	}
	if err = a.Ledger.Permit(ctx, ref.Volume.PVC.UID, "Delete", time.Minute); !errors.Is(err, core.ErrPending) {
		t.Fatalf("source reclaimed: %v", err)
	}
	if _, err = service.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: ref.ID}); err != nil {
		t.Fatal(err)
	}
	if err = a.Ledger.Permit(ctx, ref.Volume.PVC.UID, "Delete", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err = service.CreateSnapshot(ctx, q); err == nil {
		t.Fatal("deleted snapshot recreated")
	}
}
func TestPostgresDirectSnapshotCancelledReadReleasesFence(t *testing.T) {
	service, q, backend := directFixture(t)
	ctx := context.Background()
	a := service.Authority.(*KubernetesCaptureAuthority)
	backend.receipt.ContentDigest = core.Hash("invalid")
	if _, err := service.CreateSnapshot(ctx, q); err == nil {
		t.Fatal("invalid head accepted")
	}
	ref, err := a.Ledger.DirectSnapshot(ctx, Identity("snapshot", q.Name))
	if err != nil {
		t.Fatal(err)
	}
	object, err := a.Kube.Dynamic.Resource(directSnapshotResource).Namespace(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	object.SetDeletionTimestamp(&now)
	if _, err = a.Kube.Dynamic.Resource(directSnapshotResource).Namespace(ref.Namespace).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = a.RecoverDirectSnapshot(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Ledger.DirectSnapshot(ctx, ref.ID); !errors.Is(err, core.ErrPending) {
		t.Fatalf("cancelled identity remained active: %v", err)
	}
	if err = a.Ledger.Drain(ctx, ref.Volume.PVC.UID); err != nil {
		t.Fatal(err)
	}
	if err = a.Ledger.Permit(ctx, ref.Volume.PVC.UID, "Delete", time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresDirectSnapshotRejectsAliasRequestIdentity(t *testing.T) {
	service, q, backend := directFixture(t)
	ctx := context.Background()
	if _, err := service.CreateSnapshot(ctx, q); err != nil {
		t.Fatal(err)
	}
	q.Name += "-alias"
	if _, err := service.CreateSnapshot(ctx, q); err == nil {
		t.Fatal("same Snapshot UID registered under a second operation")
	}
	if backend.calls != 1 {
		t.Fatal("alias started another capture")
	}
}

type liveSnapshotFunc func(context.Context, core.DirectSnapshotRef) (core.SnapshotCapture, error)

func (f liveSnapshotFunc) Capture(ctx context.Context, q core.DirectSnapshotRef) (core.SnapshotCapture, error) {
	return f(ctx, q)
}
func TestPostgresDirectLiveCaptureNeverFallsBackToIdleHead(t *testing.T) {
	service, q, backend := directFixture(t)
	ctx := context.Background()
	a := service.Authority.(*KubernetesCaptureAuthority)
	calls := 0
	a.Live = liveSnapshotFunc(func(context.Context, core.DirectSnapshotRef) (core.SnapshotCapture, error) {
		calls++
		return core.SnapshotCapture{}, core.ErrPending
	})
	if _, err := service.CreateSnapshot(ctx, q); err == nil || backend.calls != 0 {
		t.Fatal("pending live capture fell back to idle head", err)
	}
	a.Live = liveSnapshotFunc(func(ctx context.Context, ref core.DirectSnapshotRef) (core.SnapshotCapture, error) {
		calls++
		if _, err := a.Ledger.DirectSnapshot(ctx, ref.ID); err != nil {
			t.Fatal("identity not protected before dispatch", err)
		}
		return core.SnapshotCapture{CheckpointID: "checkpoint", BindingID: "binding", CaptureID: "capture", CaptureDigest: core.Hash("capture"), PVC: ref.Volume.PVC, Driver: ref.Volume.Driver, Handle: ref.Volume.Handle, HeadRevision: core.Hash("fresh-live-head"), CapacityBytes: 1 << 30, FilesystemUUID: backend.receipt.FsUuid}, nil
	})
	result, err := service.CreateSnapshot(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := service.Journal.Snapshot(ctx, result.Snapshot.SnapshotId)
	if err != nil || pin.HeadRevision != core.Hash("fresh-live-head") || backend.calls != 0 {
		t.Fatal("wrong capture used", err)
	}
	if _, err = service.CreateSnapshot(ctx, q); err != nil || calls != 2 {
		t.Fatal("repeat took a newer capture", err)
	}
}
