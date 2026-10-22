package consume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

type captureProof struct {
	fail   bool
	calls  int
	failAt int
}

func (p *captureProof) VerifyCapture(_ context.Context, q CaptureIntent, bs []core.Binding, proof string) error {
	p.calls++
	if p.fail || (p.failAt > 0 && p.calls >= p.failAt) || proof != "trusted-frozen-fixture" || q.BarrierID == "" || len(bs) != 2 {
		return errors.New("unverified paused VMM")
	}
	return nil
}
func TestCaptureRejectsIncompleteIdentity(t *testing.T) {
	q := CaptureIntent{Version: CaptureVersion, ID: "id", Operation: "op", OwnerUID: "owner", RunUID: "run", BarrierID: "barrier", NativeArtifact: "native-version", NativeDigest: core.Hash("native"), Classes: map[string]string{"binding": "retain"}}
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	q.NativeDigest = "caller-asserted-ok"
	if err := q.Validate(); err == nil {
		t.Fatal("accepted non-digest native artifact")
	}
	q.NativeDigest = core.Hash("native")
	q.Classes = map[string]string{"binding": ""}
	if err := q.Validate(); err == nil {
		t.Fatal("accepted missing SnapshotClass")
	}
}

func TestPostgresJointCapturePinsPartialResultsAndRequiresLiveBarrier(t *testing.T) {
	testJointCapture(t, false)
}
func TestPostgresJointCaptureSealsMixedVolumesBeforeThaw(t *testing.T) {
	testJointCapture(t, true)
}
func testJointCapture(t *testing.T, sealedMode bool) {
	url := os.Getenv("SSF_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated PostgreSQL")
	}
	ctx := context.Background()
	db, err := ledger.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("capture-%d", time.Now().UnixNano())
	q := CaptureIntent{Version: CaptureVersion, ID: id, Operation: id + "op", OwnerUID: id + "owner", RunUID: id + "run", BarrierID: id + "barrier", NativeArtifact: id + "native", NativeDigest: core.Hash("memory/rootfs/legacy"), Classes: map[string]string{}}
	api := fake.NewClientset()
	cls := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass", "metadata": map[string]any{"name": "retain"}, "driver": "fixture", "deletionPolicy": "Retain"}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(kruntime.NewScheme(), map[schema.GroupVersionResource]string{
		{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}:        "VolumeSnapshotList",
		{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents"}: "VolumeSnapshotContentList",
	}, cls)
	snaps := schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}
	contents := schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents"}
	dyn.PrependReactor("create", "volumesnapshots", func(a ktesting.Action) (bool, kruntime.Object, error) {
		obj := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
		obj.SetUID(types.UID(id + obj.GetName()))
		return false, nil, nil
	})
	var bindings []core.Binding
	epoch, err := db.RunEpoch(ctx, q.OwnerUID, q.RunUID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		ref := core.PVCRef{Namespace: "ns", Name: fmt.Sprintf("data%d", i), UID: fmt.Sprintf("%spvc%d", id, i)}
		_, err = api.CoreV1().PersistentVolumeClaims("ns").Create(ctx, &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: "ns", UID: types.UID(ref.UID), Labels: map[string]string{kube.ManagedLabel: "true"}}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		v := core.Volume{Driver: "fixture", Handle: ref.UID, PVC: ref}
		if err = db.Register(ctx, v); err != nil {
			t.Fatal(err)
		}
		b := core.Binding{ID: fmt.Sprintf("%sb%d", id, i), Volume: v, OwnerUID: q.OwnerUID, RunUID: q.RunUID, Sandbox: q.RunUID, Epoch: epoch, Node: "node", MountID: ref.Name, GuestPath: "/" + ref.Name}
		bindings = append(bindings, b)
		q.Classes[b.ID] = "retain"
	}
	if err = db.Acquire(ctx, q.Operation, core.Digest(bindings), bindings); err != nil {
		t.Fatal(err)
	}
	if err = db.SetOperation(ctx, q.Operation, "Ready", ""); err != nil {
		t.Fatal(err)
	}
	proof := &captureProof{fail: true}
	c := &Checkpoints{Ledger: db, Kube: &kube.Client{API: api, Dynamic: dyn}, Evidence: proof}
	if _, err = c.Advance(ctx, q, "trusted-frozen-fixture"); err == nil {
		t.Fatal("accepted missing VMM barrier")
	}
	for _, a := range dyn.Actions() {
		if a.GetVerb() == "create" {
			t.Fatal("created snapshot before trusted barrier")
		}
	}
	proof.fail = false
	partial, err := c.Advance(ctx, q, "trusted-frozen-fixture")
	if !errors.Is(err, core.ErrPending) {
		t.Fatalf("pending snapshots: %v", err)
	}
	if len(partial.Members) != 2 {
		t.Fatalf("partial members: %+v", partial)
	}
	cp, err := db.Checkpoint(ctx, q.ID)
	if err != nil || cp.State != "Capturing" || len(cp.Resources) != 5 {
		t.Fatalf("partial checkpoint %+v %v", cp, err)
	}
	for _, member := range partial.Members {
		if err = db.PermitSnapshotDelete(ctx, member.SnapshotUID); !errors.Is(err, core.ErrPending) {
			t.Fatalf("partial member unprotected: %v", err)
		}
		snap, err := dyn.Resource(snaps).Namespace("ns").Get(ctx, member.Snapshot, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		contentName := "content-" + member.Snapshot
		content := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent", "metadata": map[string]any{"name": contentName, "uid": contentName + id}, "spec": map[string]any{"deletionPolicy": "Retain", "sourceVolumeMode": "Block", "volumeSnapshotRef": map[string]any{"namespace": "ns", "name": member.Snapshot, "uid": member.SnapshotUID}}}}
		if _, err = dyn.Resource(contents).Create(ctx, content, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		snap.Object["status"] = map[string]any{"readyToUse": true, "boundVolumeSnapshotContentName": contentName}
		if _, err = dyn.Resource(snaps).Namespace("ns").Update(ctx, snap, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// Evidence can disappear after all CSI snapshots are ready, including between
	// the initial and final verification. That must not publish the checkpoint.
	proof.failAt = proof.calls + 2
	if _, err = c.Advance(ctx, q, "trusted-frozen-fixture"); err == nil {
		t.Fatal("published after barrier loss")
	}
	cp, _ = db.Checkpoint(ctx, q.ID)
	if cp.State != "Capturing" || len(cp.Resources) != 7 {
		t.Fatalf("lost pending pins: %+v", cp)
	}
	proof.failAt = 0
	// A new Content at the same name must not silently replace the captured one.
	member := partial.Members[0]
	contentName := "content-" + member.Snapshot
	content, _ := dyn.Resource(contents).Get(ctx, contentName, metav1.GetOptions{})
	originalUID := content.GetUID()
	content.SetUID("replacement")
	if _, err = dyn.Resource(contents).Update(ctx, content, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Advance(ctx, q, "trusted-frozen-fixture"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("replaced content accepted: %v", err)
	}
	content.SetUID(originalUID)
	if _, err = dyn.Resource(contents).Update(ctx, content, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	prepared, published := 0, 0
	if sealedMode {
		// One managed participant can seal independently; the generic member
		// already has its immutable Kubernetes snapshot and Content pinned.
		c.SealedParticipants = func(b core.Binding) bool { return b.ID == bindings[0].ID }
		c.PrepareParticipants = func(context.Context, CaptureIntent, []core.Binding, string) error { prepared++; return nil }
		c.PublishParticipants = func(context.Context, CaptureIntent, []core.Binding, string) error {
			if !proof.fail {
				t.Error("publication did not follow simulated thaw")
			}
			published++
			return nil
		}
		proof.failAt = proof.calls + 2
		if _, err = c.Seal(ctx, q, "trusted-frozen-fixture"); err == nil {
			t.Fatal("sealed after final barrier loss")
		}
		var stored string
		if err = db.ReadStep(ctx, "checkpoint/sealed/"+q.ID, &stored); err == nil {
			t.Fatal("failed barrier granted thaw")
		}
		proof.failAt = 0
		if _, err = c.Seal(ctx, q, "trusted-frozen-fixture"); err != nil {
			t.Fatal(err)
		}
		proof.fail = true // Only durable sealed layers may now be published.
		before := proof.calls
		if _, err = c.Seal(ctx, q, "trusted-frozen-fixture"); err != nil || proof.calls != before {
			t.Fatalf("seal replay after thaw: %v", err)
		}
	}
	complete, err := c.Advance(ctx, q, "trusted-frozen-fixture")
	if sealedMode && (prepared != 2 || published != 1) {
		t.Fatalf("phase calls: prepare=%d publish=%d", prepared, published)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(complete.Members) != 2 {
		t.Fatal("incomplete capture published")
	}
	cp, _ = db.Checkpoint(ctx, q.ID)
	if cp.State != "Ready" {
		t.Fatal(cp.State)
	}
	proof.fail = true // A committed receipt remains readable after source thaw.
	if _, err = c.Advance(ctx, q, "trusted-frozen-fixture"); err != nil {
		t.Fatalf("committed replay: %v", err)
	}
	changed := q
	changed.NativeDigest = core.Hash("different memory")
	if _, err = c.Advance(ctx, changed, "trusted-frozen-fixture"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("changed native capture: %v", err)
	}
	if err = db.RetainCheckpoint(ctx, q.ID, q.ID+"template", "template"); err != nil {
		t.Fatal(err)
	}
	if err = db.ReleaseCheckpoint(ctx, q.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Advance(ctx, q, "trusted-frozen-fixture"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("resurrected released checkpoint: %v", err)
	}
	actions := len(dyn.Actions())
	if err = c.Reap(ctx, q.ID); !errors.Is(err, core.ErrPending) {
		t.Fatalf("collected published snapshot: %v", err)
	}
	if len(dyn.Actions()) != actions {
		t.Fatal("reclaim touched Kubernetes while pins exist")
	}
	if err = db.ReleaseCheckpoint(ctx, q.ID+"template"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = c.Reap(ctx, q.ID); err != nil {
			t.Fatal(err)
		}
	}
	var gcState string
	if err = db.Session(ctx).QueryRow(ctx, `SELECT state FROM ssf_checkpoint_gc WHERE checkpoint_id=$1`, q.ID).Scan(&gcState); err != nil || gcState != "Complete" {
		t.Fatalf("reclaim not durable: %s %v", gcState, err)
	}
	updates, deletes := 0, 0
	for _, action := range dyn.Actions()[actions:] {
		if action.GetVerb() == "update" && action.GetResource().Resource == "volumesnapshotcontents" {
			obj := action.(ktesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			policy, _, _ := unstructured.NestedString(obj.Object, "spec", "deletionPolicy")
			if policy != "Delete" {
				t.Fatal("backend snapshot would be leaked with Retain")
			}
			updates++
		}
		if action.GetVerb() == "delete" {
			options := action.(ktesting.DeleteAction).GetDeleteOptions()
			if options.Preconditions == nil || options.Preconditions.UID == nil {
				t.Fatal("reclaim omitted immutable UID precondition")
			}
			deletes++
		}
	}
	if updates != 2 || deletes != 4 {
		t.Fatalf("incomplete native snapshot reclaim: updates=%d deletes=%d", updates, deletes)
	}

	// A timed-out capture may create Kubernetes objects after retirement.
	// Keep its identity tombstone and reap late arrivals without reopening it.
	incomplete := q
	incomplete.ID = q.ID + "incomplete"
	if err = db.BeginCheckpoint(ctx, incomplete.ID, incomplete.OwnerUID, incomplete); err != nil {
		t.Fatal(err)
	}
	if err = db.ReleaseCheckpoint(ctx, incomplete.ID); err != nil {
		t.Fatal(err)
	}
	if err = c.Reap(ctx, incomplete.ID); !errors.Is(err, core.ErrPending) {
		t.Fatalf("unknown create outcome discarded: %v", err)
	}
	b := bindings[0]
	lateName := "ssf-cp-" + core.Hash(incomplete.ID + "/" + b.ID)[:40]
	lateUID := id + lateName
	lateContent := lateName + "content"
	late := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
		"metadata": map[string]any{"namespace": b.Volume.PVC.Namespace, "name": lateName, "uid": lateUID, "labels": map[string]any{kube.ManagedLabel: "true"}, "annotations": map[string]any{"ssf.dev/source-pvc-uid": b.Volume.PVC.UID}},
		"status":   map[string]any{"boundVolumeSnapshotContentName": lateContent},
	}}
	if _, err = dyn.Resource(snaps).Namespace(b.Volume.PVC.Namespace).Create(ctx, late, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	contentLate := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent",
		"metadata": map[string]any{"name": lateContent, "uid": lateContent + "uid"},
		"spec":     map[string]any{"deletionPolicy": "Retain", "volumeSnapshotRef": map[string]any{"uid": lateUID, "name": lateName, "namespace": b.Volume.PVC.Namespace}},
	}}
	if _, err = dyn.Resource(contents).Create(ctx, contentLate, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = c.Reap(ctx, incomplete.ID); !errors.Is(err, core.ErrPending) {
		t.Fatalf("incomplete tombstone lost: %v", err)
	}
	if _, err = dyn.Resource(contents).Get(ctx, lateContent, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("late content leaked: %v", err)
	}
}

func TestPostgresManagedSealDefersKubernetesAndUploadUntilAfterThaw(t *testing.T) {
	url := os.Getenv("SSF_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated PostgreSQL")
	}
	ctx := context.Background()
	db, err := ledger.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("capture-%d", time.Now().UnixNano())
	q := CaptureIntent{Version: CaptureVersion, ID: id, Operation: id + "op", OwnerUID: id + "owner", RunUID: id + "run", BarrierID: id + "barrier", NativeArtifact: id + "native", NativeDigest: core.Hash("memory/rootfs/legacy"), Classes: map[string]string{}}
	api := fake.NewClientset()
	cls := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass", "metadata": map[string]any{"name": "retain"}, "driver": "fixture", "deletionPolicy": "Retain"}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(kruntime.NewScheme(), map[schema.GroupVersionResource]string{
		{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}:        "VolumeSnapshotList",
		{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents"}: "VolumeSnapshotContentList",
	}, cls)
	dyn.PrependReactor("create", "volumesnapshots", func(a ktesting.Action) (bool, kruntime.Object, error) {
		obj := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
		obj.SetUID(types.UID(id + obj.GetName()))
		return false, nil, nil
	})
	var bindings []core.Binding
	epoch, err := db.RunEpoch(ctx, q.OwnerUID, q.RunUID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		ref := core.PVCRef{Namespace: "ns", Name: fmt.Sprintf("data%d", i), UID: fmt.Sprintf("%spvc%d", id, i)}
		_, err = api.CoreV1().PersistentVolumeClaims("ns").Create(ctx, &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: "ns", UID: types.UID(ref.UID), Labels: map[string]string{kube.ManagedLabel: "true"}}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		v := core.Volume{Driver: "fixture", Handle: ref.UID, PVC: ref}
		if err = db.Register(ctx, v); err != nil {
			t.Fatal(err)
		}
		b := core.Binding{ID: fmt.Sprintf("%sb%d", id, i), Volume: v, OwnerUID: q.OwnerUID, RunUID: q.RunUID, Sandbox: q.RunUID, Epoch: epoch, Node: "node", MountID: ref.Name, GuestPath: "/" + ref.Name}
		bindings = append(bindings, b)
		q.Classes[b.ID] = "retain"
	}
	if err = db.Acquire(ctx, q.Operation, core.Digest(bindings), bindings); err != nil {
		t.Fatal(err)
	}
	if err = db.SetOperation(ctx, q.Operation, "Ready", ""); err != nil {
		t.Fatal(err)
	}
	proof := &captureProof{fail: true}
	c := &Checkpoints{Ledger: db, Kube: &kube.Client{API: api, Dynamic: dyn}, Evidence: proof}

	proof.fail = false
	prepares, publishes := 0, 0
	failPrepare := true
	c.SealedParticipants = func(core.Binding) bool { return true }
	c.PrepareParticipants = func(context.Context, CaptureIntent, []core.Binding, string) error {
		prepares++
		if failPrepare {
			return errors.New("partial participant capture")
		}
		return nil
	}
	c.PublishParticipants = func(context.Context, CaptureIntent, []core.Binding, string) error { publishes++; return nil }
	if _, err = c.Seal(ctx, q, "trusted-frozen-fixture"); err == nil {
		t.Fatal("partial capture permitted thaw")
	}
	var receipt string
	if err = db.ReadStep(ctx, "checkpoint/sealed/"+q.ID, &receipt); err == nil {
		t.Fatal("partial seal persisted")
	}
	failPrepare = false
	if _, err = c.Seal(ctx, q, "trusted-frozen-fixture"); err != nil {
		t.Fatal(err)
	}
	if prepares != 2 || publishes != 0 {
		t.Fatal("seal performed publication")
	}
	for _, action := range dyn.Actions() {
		if action.GetVerb() == "create" {
			t.Fatal("seal created managed snapshot before publication")
		}
	}
	proof.fail = true
	calls := proof.calls
	if _, err = c.Seal(ctx, q, "trusted-frozen-fixture"); err != nil {
		t.Fatal(err)
	}
	if prepares != 2 || proof.calls != calls {
		t.Fatal("replay recaptured or required live VMM")
	}
	manifest, err := c.Advance(ctx, q, "trusted-frozen-fixture")
	if !errors.Is(err, core.ErrPending) || len(manifest.Members) != 2 || publishes != 1 || proof.calls != calls {
		t.Fatalf("post-thaw publication: %+v %v prepares=%d publishes=%d proof=%d", manifest, err, prepares, publishes, proof.calls)
	}
}
