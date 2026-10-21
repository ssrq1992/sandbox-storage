package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
)

func TestNativeCheckpointReferencesAndIndependentClonePVCs(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	c.Principal.Namespaces = []string{"test"}
	d, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	member := consume.CaptureMember{BindingID: id + "binding", MountID: "ext_data", GuestPath: "/data", PVC: core.PVCRef{Namespace: "test", Name: "data", UID: id + "pvc"}, Snapshot: "snapshot", SnapshotUID: id + "snapshot", ContentUID: id + "content"}
	m := consume.CaptureManifest{NativeResourceUID: id + "native", Intent: consume.CaptureIntent{Version: consume.CaptureVersion, ID: id + "capture", Operation: id + "op", OwnerUID: d.OwnerUID, RunUID: d.RunUID, BarrierID: "barrier", NativeArtifact: "native", NativeDigest: core.Hash("native"), Classes: map[string]string{member.BindingID: "retain"}}, Members: []consume.CaptureMember{member}}
	readonlyMember := consume.CaptureMember{BindingID: id + "readonly", MountID: "ext_readonly", GuestPath: "/readonly", ReadOnly: true, PVC: core.PVCRef{Namespace: "test", Name: id + "readonly", UID: id + "readonlypvc"}, Snapshot: "readonly-snapshot", SnapshotUID: id + "readonlysnapshot", ContentUID: id + "readonlycontent"}
	m.Members = append(m.Members, readonlyMember)
	m.Intent.Classes[readonlyMember.BindingID] = "retain"
	if err = c.Ledger.BeginCheckpoint(ctx, m.Intent.ID, d.OwnerUID, m.Intent); err != nil {
		t.Fatal(err)
	}
	if err = c.Ledger.PinCheckpoint(ctx, m.Intent.ID, m.NativeResourceUID, member.SnapshotUID, member.ContentUID, readonlyMember.SnapshotUID, readonlyMember.ContentUID); err != nil {
		t.Fatal(err)
	}
	if _, err = c.RetainNativeCheckpoint(ctx, m.Intent.ID, "template", "template"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("incomplete source: %v", err)
	}
	if err = c.Ledger.CommitCheckpoint(ctx, m.Intent.ID, m); err != nil {
		t.Fatal(err)
	}
	source := NativeVolume{Provider: "ssf", Tenant: "test", Name: "data", PVC: member.PVC, StorageClass: "block", SnapshotClass: "retain", Bytes: 1 << 20, State: "Bound"}
	var fixed NativeCapturedVolume
	if err = c.Ledger.Step(ctx, "agentenv/capture-volume/"+m.Intent.ID+"/"+member.BindingID, member.PVC.UID, func(context.Context) (any, error) {
		return NativeCapturedVolume{Volume: source, Index: 1, MountName: "logical-data"}, nil
	}, &fixed); err != nil {
		t.Fatal(err)
	}
	readonlyVolume := NativeVolume{Provider: "ssf", Tenant: "test", Name: readonlyMember.PVC.Name, PVC: readonlyMember.PVC, StorageClass: "block", SnapshotClass: "retain", Bytes: 1 << 20, State: "Bound", ReadOnly: true}
	if err = c.Ledger.Step(ctx, "agentenv/capture-volume/"+m.Intent.ID+"/"+readonlyMember.BindingID, readonlyMember.PVC.UID, func(context.Context) (any, error) { return NativeCapturedVolume{Volume: readonlyVolume, Index: 0}, nil }, &fixed); err != nil {
		t.Fatal(err)
	}
	templateID := id + "template"
	ref, err := c.RetainNativeCheckpoint(ctx, m.Intent.ID, "template", templateID)
	if err != nil {
		t.Fatal(err)
	}
	other := *c
	other.Principal.Tenant = "other"
	if _, err = other.RetainNativeCheckpoint(ctx, ref, "fork", "other"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("tenant isolation: %v", err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	published, err := c.PublishNativeCheckpoint(ctx, raw, templateID)
	if err != nil || published["reference"] != ref {
		t.Fatalf("publication reference: %+v %v", published, err)
	}
	raw, err = json.Marshal(published)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.PublishNativeCheckpoint(ctx, raw, templateID); err != nil {
		t.Fatalf("republish retained descriptor: %v", err)
	}
	if err = c.Ledger.ReleaseCheckpoint(ctx, m.Intent.ID); err != nil {
		t.Fatal(err)
	}
	if replay, err := c.RetainNativeCheckpoint(ctx, m.Intent.ID, "template", templateID); err != nil || replay != ref {
		t.Fatalf("retain replay after parent release: %s %v", replay, err)
	}
	if err = c.Ledger.PermitSnapshotDelete(ctx, member.ContentUID); !errors.Is(err, core.ErrPending) {
		t.Fatalf("template lost protection: %v", err)
	}
	snap := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot", "metadata": map[string]any{"name": "snapshot", "namespace": "test", "uid": member.SnapshotUID, "labels": map[string]any{kube.ManagedLabel: "true"}}, "status": map[string]any{"readyToUse": true, "boundVolumeSnapshotContentName": "content"}}}
	content := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent", "metadata": map[string]any{"name": "content", "uid": member.ContentUID}, "spec": map[string]any{"sourceVolumeMode": "Block", "volumeSnapshotRef": map[string]any{"name": "snapshot", "namespace": "test", "uid": member.SnapshotUID}}}}
	retain := v1.PersistentVolumeReclaimRetain
	api := fake.NewSimpleClientset(&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "block"}, ReclaimPolicy: &retain})
	api.PrependReactor("create", "persistentvolumeclaims", func(a ktesting.Action) (bool, kruntime.Object, error) {
		p := a.(ktesting.CreateAction).GetObject().(*v1.PersistentVolumeClaim)
		p.UID = types.UID(id + p.Name)
		return false, nil, nil
	})
	roSnap := snap.DeepCopy()
	roSnap.SetName(readonlyMember.Snapshot)
	roSnap.SetUID(types.UID(readonlyMember.SnapshotUID))
	roSnap.Object["status"] = map[string]any{"readyToUse": true, "boundVolumeSnapshotContentName": "readonly-content"}
	roContent := content.DeepCopy()
	roContent.SetName("readonly-content")
	roContent.SetUID(types.UID(readonlyMember.ContentUID))
	roContent.Object["spec"] = map[string]any{"sourceVolumeMode": "Block", "volumeSnapshotRef": map[string]any{"name": readonlyMember.Snapshot, "namespace": "test", "uid": readonlyMember.SnapshotUID}}
	dyn := dynamicfake.NewSimpleDynamicClient(kruntime.NewScheme(), snap, content, roSnap, roContent)
	c.Volumes = &NativeVolumes{Ledger: c.Ledger, Kube: &kube.Client{API: api, Dynamic: dyn}}
	if err = c.Volumes.save(ctx, readonlyVolume); err != nil {
		t.Fatal(err)
	}
	var previous core.PVCRef
	for _, child := range []string{id + "child1", id + "child2"} {
		childRef, err := c.RetainNativeCheckpoint(ctx, ref, "fork", child)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.CloneNativeCheckpoint(ctx, childRef, "wrong-child"); !errors.Is(err, core.ErrConflict) {
			t.Fatalf("reference not bound to child: %v", err)
		}
		clones, err := c.CloneNativeCheckpoint(ctx, childRef, child)
		if err != nil {
			t.Fatal(err)
		}
		if len(clones) != 2 || clones[0].MountID != readonlyMember.MountID || clones[0].Volume.PVC != readonlyVolume.PVC || clones[1].Mount.Name != "logical-data" || clones[1].MountID != member.MountID || clones[1].Volume.InitializeExt4 || clones[1].Volume.PVC == source.PVC || clones[1].Volume.PVC == previous {
			t.Fatalf("unsafe clone: %+v", clones)
		}
		previous = clones[1].Volume.PVC
		p, err := api.CoreV1().PersistentVolumeClaims("test").Get(ctx, previous.Name, metav1.GetOptions{})
		if err != nil || p.Spec.DataSource == nil || p.Spec.DataSource.Name != member.Snapshot {
			t.Fatalf("clone not native snapshot restore: %+v %v", p, err)
		}
		replay, err := c.CloneNativeCheckpoint(ctx, childRef, child)
		if err != nil || replay[1].Volume.PVC != previous {
			t.Fatalf("clone replay: %+v %v", replay, err)
		}
	}
	// Template inheritance recreates even RO volumes from the checkpoint.
	// Deleting the original volume must not invalidate the committed template.
	readonlyVolume.State = "Deleted"
	if err = c.Volumes.save(ctx, readonlyVolume); err != nil {
		t.Fatal(err)
	}
	restoreChild := id + "restore"
	restoreRef, err := c.RetainNativeCheckpoint(ctx, ref, "restore", restoreChild)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := c.CloneNativeCheckpoint(ctx, restoreRef, restoreChild)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored) != 2 || !restored[0].Volume.ReadOnly || restored[0].Volume.PVC == readonlyVolume.PVC || restored[0].Volume.InitializeExt4 {
		t.Fatalf("template inherited mutable source ownership: %+v", restored)
	}
	// Native scheduling consumes cloned PVCs, retaining the captured device
	// order and IDs even after the source registry entry has been deleted.
	fail, none := admissionv1.Fail, admissionv1.SideEffectClassNone
	_, err = api.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx, &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "ssf-guard"}, Webhooks: []admissionv1.ValidatingWebhook{{
			Name: "guard.ssf.test", FailurePolicy: &fail, SideEffects: &none,
			ClientConfig: admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Name: "guard", Namespace: "test"}, CABundle: []byte("fixture")},
			Rules:        []admissionv1.RuleWithOperations{{Operations: []admissionv1.OperationType{admissionv1.Create, admissionv1.Update, admissionv1.Delete}, Rule: admissionv1.Rule{Resources: []string{"pods", "persistentvolumeclaims", "persistentvolumes"}}}},
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Kube = c.Volumes.Kube
	c.Refresh = func(_ context.Context, n NativeCandidate) (NativeCandidate, error) { return n, nil }
	c.ProjectTemplate = func(context.Context, string, []NativeCandidate) (NativeTemplate, error) {
		return NativeTemplate{ID: templateID, Resources: Resources{CPU: 1, MemoryMiB: 256}, CheckpointID: ref}, nil
	}
	launch := NativeIntent{RequestID: id + "launch", Tenant: "test", Path: "/v2/sandboxes", Body: []byte(`{"templateID":"alias"}`)}
	launch.BodyDigest = core.Hash(string(launch.Body))
	for i := 0; i < 2; i++ {
		if _, err = c.Prepare(ctx, launch, nil, func([]NativeCandidate) (NativeCandidate, error) { return NativeCandidate{}, core.ErrPending }); err == nil {
			t.Fatal("launched without a candidate")
		}
		var run string
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT run_uid FROM agentenv_native_runs WHERE id=$1`, launch.RequestID).Scan(&run); err != nil {
			t.Fatal(err)
		}
		var request consume.Request
		operation := core.Hash("test" + "\x00" + run + "\x00" + launch.RequestID)
		if err = c.Ledger.ReadStep(ctx, "request/"+operation, &request); err != nil {
			t.Fatal(err)
		}
		if len(request.Mounts) != 2 || request.Mounts[0].MountID != readonlyMember.MountID || request.Mounts[1].MountID != member.MountID || request.Mounts[0].PVC == readonlyVolume.PVC || request.Mounts[1].PVC == source.PVC {
			t.Fatalf("native template preparation used source volumes or reordered slots: %+v", request.Mounts)
		}
	}
	// Replacements retain every captured device on private paths. The complete
	// plan is persisted before placement, including retry-stable private PVCs.
	replacement, err := c.Volumes.Create(ctx, c.Principal, NativeVolumeRequest{Provider: "ssf", Name: id + "replacement", Namespace: "test", StorageClass: "block", Bytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mounts []NativeMount
	}{
		{"empty", []NativeMount{}}, {"replace", []NativeMount{{Name: replacement.Name, Path: "/data"}}},
	} {
		body, _ := json.Marshal(map[string]any{"templateID": "alias", "volumeMounts": test.mounts})
		intent := NativeIntent{RequestID: id + test.name, Tenant: "test", Path: "/v2/sandboxes", Body: body, BodyDigest: core.Hash(string(body))}
		var priorPVCs []core.PVCRef
		for attempt := 0; attempt < 2; attempt++ {
			if _, err = c.Prepare(ctx, intent, nil, func([]NativeCandidate) (NativeCandidate, error) { return NativeCandidate{}, core.ErrPending }); err == nil {
				t.Fatal("placed without a native candidate")
			}
			var run string
			if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT run_uid FROM agentenv_native_runs WHERE id=$1`, intent.RequestID).Scan(&run); err != nil {
				t.Fatal(err)
			}
			var request consume.Request
			operation := core.Hash("test" + "\x00" + run + "\x00" + intent.RequestID)
			if err = c.Ledger.ReadStep(ctx, "request/"+operation, &request); err != nil {
				t.Fatal(err)
			}
			if len(request.Mounts) != 2+len(test.mounts) {
				t.Fatalf("lost frozen devices: %+v", request.Mounts)
			}
			for index, mount := range request.Mounts[:2] {
				if !strings.HasPrefix(mount.GuestPath, NativeRetainedPrefix) {
					t.Fatalf("old volume exposed after replacement: %+v", mount)
				}
				if attempt > 0 && mount.PVC != priorPVCs[index] {
					t.Fatal("replacement retry changed retained PVC")
				}
				childRef := "agentenv/ref/" + core.Hash("test/restore/"+intent.RequestID)
				name := "clone-" + core.Hash(childRef + "/" + intent.RequestID + "/" + []string{readonlyMember.BindingID, member.BindingID}[index])[:40]
				private, e := c.Volumes.Get(ctx, "test", name)
				if e != nil || !private.Private || private.InternalOwner != intent.RequestID {
					t.Fatalf("retained volume lacks owned GC identity: %+v %v", private, e)
				}
			}
			priorPVCs = []core.PVCRef{request.Mounts[0].PVC, request.Mounts[1].PVC}
			if len(test.mounts) > 0 {
				fresh := request.Mounts[2]
				if fresh.PVC != replacement.PVC || fresh.GuestPath != "/data" || fresh.MountID == member.MountID {
					t.Fatalf("new device reused frozen identity: %+v", fresh)
				}
			}
		}
	}
	beforeInvalid := len(api.Actions())
	invalid := NativeIntent{RequestID: id + "null", Tenant: "test", Path: "/v2/sandboxes", Body: []byte(`{"templateID":"alias","volumeMounts":null}`)}
	invalid.BodyDigest = core.Hash(string(invalid.Body))
	if _, err = c.Prepare(ctx, invalid, nil, nil); err == nil || len(api.Actions()) != beforeInvalid {
		t.Fatal("invalid mount request created resources")
	}
	// A same-name replacement must fail before creating any new PVC.
	content.SetUID("replacement")
	_, err = dyn.Resource(schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents"}).Update(ctx, content, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	child := id + "child3"
	childRef, err := c.RetainNativeCheckpoint(ctx, ref, "fork", child)
	if err != nil {
		t.Fatal(err)
	}
	before := len(api.Actions())
	if _, err = c.CloneNativeCheckpoint(ctx, childRef, child); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("replaced content used: %v", err)
	}
	for _, a := range api.Actions()[before:] {
		if a.GetVerb() == "create" {
			t.Fatal("created PVC from replaced checkpoint")
		}
	}
}
