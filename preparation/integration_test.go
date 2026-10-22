package preparation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	admission "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	api "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	driver "ssf.dev/ssf/drivers/csi"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

type prepAuth struct{}

func (prepAuth) Authorize(context.Context, string, core.PVCRef, bool) error { return nil }

type prepDriver struct{ csi.ControllerClient }

func (prepDriver) ValidateVolumeCapabilities(_ context.Context, q *csi.ValidateVolumeCapabilitiesRequest, _ ...grpc.CallOption) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: q.VolumeCapabilities}}, nil
}
func (prepDriver) ControllerPublishVolume(context.Context, *csi.ControllerPublishVolumeRequest, ...grpc.CallOption) (*csi.ControllerPublishVolumeResponse, error) {
	return &csi.ControllerPublishVolumeResponse{}, nil
}
func (prepDriver) ControllerUnpublishVolume(context.Context, *csi.ControllerUnpublishVolumeRequest, ...grpc.CallOption) (*csi.ControllerUnpublishVolumeResponse, error) {
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

type prepDrivers struct{}

func (prepDrivers) Controller(context.Context, string) (*driver.Session, error) {
	return &driver.Session{Controller: prepDriver{}, Attach: true}, nil
}
func (prepDrivers) NodeID(context.Context, string, string) (string, error) { return "csi-node", nil }

type prepNode struct {
	mu                 sync.Mutex
	key                ed25519.PublicKey
	receipt            core.PreparationReceipt
	prepares           int
	failRelease        bool
	boot               string
	losePrepareReply   bool
	inspectUnavailable bool
}

func (n *prepNode) response() (*api.VolumePreparation, error) {
	raw, _ := json.Marshal(n.receipt)
	state := "Prepared"
	if n.receipt.Released {
		state = "Released"
	}
	return &api.VolumePreparation{OperationId: n.receipt.Operation, State: state, Protocol: core.PreparationProtocol, ReceiptJson: string(raw)}, nil
}
func (n *prepNode) PrepareVolumeContent(_ context.Context, q *api.PrepareRequest) (*api.VolumePreparation, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	boot := n.boot
	if boot == "" {
		boot = "boot"
	}
	p, e := auth.Verify(q.SignedNodePlan, n.key, "node", boot, time.Now())
	if e != nil {
		return nil, e
	}
	if p.Sandbox != "" || p.Purpose != core.PurposePreparation {
		return nil, errors.New("fake sandbox preparation")
	}
	b := p.Bindings[0]
	n.prepares++
	n.receipt = core.PreparationReceipt{Protocol: core.PreparationProtocol, Operation: q.RequestId, PlanDigest: core.PreparationDigest(p), BindingID: b.ID, VolumeUID: b.Volume.PVC.UID, NodeUID: p.NodeUID, BootID: p.BootID, Epoch: b.Epoch, UUID: "filesystem"}
	if n.losePrepareReply {
		return nil, errors.New("injected lost preparation reply")
	}
	return n.response()
}
func (n *prepNode) InspectVolumePreparation(context.Context, *api.InspectRequest) (*api.VolumePreparation, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.inspectUnavailable {
		return nil, errors.New("node WAL unavailable")
	}
	return n.response()
}
func (n *prepNode) ReleaseVolumePreparation(context.Context, *api.PrepareRequest) (*api.VolumePreparation, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failRelease {
		return nil, errors.New("injected release failure")
	}
	n.receipt.Released = true
	return n.response()
}

func preparationFixture(t *testing.T) (*Coordinator, core.Volume, *prepNode) {
	t.Helper()
	dsn := os.Getenv("SSF_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires dedicated PostgreSQL")
	}
	ctx := context.Background()
	db, e := ledger.Open(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(db.Close)
	if e = db.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	id := "preparation-" + time.Now().Format("150405.000000000")
	profile := core.VolumeProfile{Name: "profile", Runtime: "agentenv", Version: core.PreparationProtocol, VolumeMode: "Block", Filesystem: "ext4", InitializeBlank: true}
	mode := v1.PersistentVolumeBlock
	retain := v1.PersistentVolumeReclaimRetain
	class := "profiled"
	binding := storage.VolumeBindingImmediate
	p := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: "test", UID: types.UID(id), Labels: map[string]string{kube.ManagedLabel: "true"}, Finalizers: []string{kube.VolumeProtectionFinalizer}, Annotations: map[string]string{kube.VolumeProfileAnnotation: profile.Name, kube.ProfileRevisionAnnotation: core.Digest(profile), kube.ProfileClassUIDAnnotation: "class-uid", kube.SourceRevisionAnnotation: core.Digest("blank")}}, Spec: v1.PersistentVolumeClaimSpec{StorageClassName: &class, VolumeName: id, VolumeMode: &mode, AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, Resources: v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Gi")}}}, Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound}}
	pv := &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: id, UID: types.UID("pv-" + id)}, Spec: v1.PersistentVolumeSpec{StorageClassName: class, VolumeMode: &mode, Capacity: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Gi")}, AccessModes: p.Spec.AccessModes, ClaimRef: &v1.ObjectReference{Namespace: p.Namespace, Name: p.Name, UID: p.UID}, PersistentVolumeReclaimPolicy: retain, PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "fixture", VolumeHandle: id}}}}
	fail := admission.Fail
	none := admission.SideEffectClassNone
	gate := &admission.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "ssf-guard"}, Webhooks: []admission.ValidatingWebhook{{FailurePolicy: &fail, SideEffects: &none, ClientConfig: admission.WebhookClientConfig{Service: &admission.ServiceReference{Name: "ssf-guard", Namespace: "ssf-system"}, CABundle: []byte("fixture")}, Rules: []admission.RuleWithOperations{{Operations: []admission.OperationType{admission.Create, admission.Update, admission.Delete}, Rule: admission.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods", "persistentvolumeclaims", "persistentvolumes"}}}}}}}
	client := &kube.Client{API: fake.NewClientset(p, pv, gate, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}, &storage.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: class, UID: "class-uid", Annotations: map[string]string{kube.VolumeProfileAnnotation: profile.Name}}, Provisioner: "fixture", ReclaimPolicy: &retain, VolumeBindingMode: &binding})}
	public, key, _ := ed25519.GenerateKey(rand.Reader)
	node := &prepNode{key: public}
	c := &Coordinator{Kube: client, Ledger: db, SigningKey: key, Drivers: prepDrivers{}, Authorizer: prepAuth{}, Tenant: id, Namespaces: []string{"test"}, Profiles: []core.VolumeProfile{profile}, Choose: func(context.Context, core.Volume) (core.Placement, error) {
		return core.Placement{ID: "placement", Node: "node", NodeUID: "node-uid", BootID: "boot", Expires: time.Now().Add(time.Hour)}, nil
	}, Validate: func(context.Context, core.Placement) error { return nil }, Node: func(context.Context, core.Placement) (NodeClient, error) { return node, nil }}
	v, e := client.Resolve(ctx, core.PVCRef{Namespace: p.Namespace, Name: p.Name, UID: string(p.UID)}, false)
	if e != nil {
		t.Fatal(e)
	}
	return c, v, node
}
func TestPostgresConcurrentPreparationAndDeletion(t *testing.T) {
	c, v, n := preparationFixture(t)
	ctx := context.Background()
	results := make(chan error, 2)
	for range 2 {
		go func() { _, err := c.Ensure(ctx, v, core.Placement{}, "", ""); results <- err }()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if n.prepares != 1 {
		t.Fatalf("concurrent coordinators started %d preparers", n.prepares)
	}
	r, err := c.Ledger.Preparation(ctx, v.PVC.UID)
	if err != nil || r.State != "VolumeReady" {
		t.Fatalf("ready: %+v %v", r, err)
	}
	claim, err := c.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	claim.DeletionTimestamp = &now
	if _, err = c.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = c.drainClaim(ctx, v.PVC); err != nil {
		t.Fatal(err)
	}
	claim, _ = c.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
	if len(claim.Finalizers) != 0 {
		t.Fatal("unreferenced PVC finalizer retained")
	}
}
func TestPostgresDeletingContentReadyRetriesRelease(t *testing.T) {
	c, v, n := preparationFixture(t)
	ctx := context.Background()
	n.failRelease = true
	if _, err := c.Ensure(ctx, v, core.Placement{}, "", ""); err == nil {
		t.Fatal("release failure accepted")
	}
	claim, _ := c.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
	now := metav1.Now()
	claim.DeletionTimestamp = &now
	if _, err := c.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.drainClaim(ctx, v.PVC); err == nil {
		t.Fatal("removed protection before release")
	}
	n.failRelease = false
	if err := c.drainClaim(ctx, v.PVC); err != nil {
		t.Fatal(err)
	}
	if n.prepares != 1 {
		t.Fatal("deletion started another formatter")
	}
}

func TestPostgresPreparationRebootPreservesOperation(t *testing.T) {
	c, v, n := preparationFixture(t)
	ctx := context.Background()
	n.losePrepareReply = true
	n.failRelease = true
	_, initialErr := c.Ensure(ctx, v, core.Placement{}, "", "")
	if initialErr == nil {
		t.Fatal("lost reply not surfaced")
	}
	previous, err := c.Ledger.Preparation(ctx, v.PVC.UID)
	if err != nil || previous.State != "NeedsRecovery" {
		t.Fatalf("%+v %v; initial preparation error: %v", previous, err, initialErr)
	}
	n.boot = "new-boot"
	n.losePrepareReply = false
	n.failRelease = false
	c.Refresh = func(_ context.Context, p core.Placement) (core.Placement, error) { p.BootID = n.boot; return p, nil }
	n.inspectUnavailable = true
	if _, err = c.Ensure(ctx, v, core.Placement{}, "", ""); err == nil {
		t.Fatal("reboot ignored unavailable old WAL")
	}
	unchanged, err := c.Ledger.Preparation(ctx, v.PVC.UID)
	if err != nil || unchanged.Plan.BootID != previous.Plan.BootID {
		t.Fatal("unknown old operation was rebound")
	}
	n.inspectUnavailable = false
	if _, err = c.Ensure(ctx, v, core.Placement{}, "", ""); err != nil {
		t.Fatal(err)
	}
	next, err := c.Ledger.Preparation(ctx, v.PVC.UID)
	if err != nil {
		t.Fatal(err)
	}
	if next.State != "VolumeReady" || next.Operation != previous.Operation || next.Plan.Bindings[0].Epoch != previous.Plan.Bindings[0].Epoch || next.Receipt.UUID != n.receipt.UUID || next.Plan.BootID != n.boot {
		t.Fatal("reboot changed preparation identity or failed to finish")
	}
}
