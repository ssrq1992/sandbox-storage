package consume

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	admission "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"ssf.dev/ssf/core"
	driver "ssf.dev/ssf/drivers/csi"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
	"testing"
	"time"
)

type authorizer struct{}

func (authorizer) Authorize(context.Context, string, core.PVCRef, bool) error { return nil }

type placement struct{}

func (placement) Reserve(context.Context, string, []core.Volume) (core.Placement, error) {
	return core.Placement{ID: "lease", Node: "node", NodeUID: "node-uid", BootID: "boot", Expires: time.Now().Add(time.Hour)}, nil
}
func (placement) Validate(context.Context, core.Placement) error { return nil }
func (placement) Renew(_ context.Context, p core.Placement) (core.Placement, error) {
	p.Expires = time.Now().Add(time.Hour)
	return p, nil
}
func (placement) Commit(context.Context, core.Placement, string) error { return nil }
func (placement) Release(context.Context, core.Placement) error        { return nil }

type controller struct {
	csi.ControllerClient
	attach, detach int
}

func (c *controller) ValidateVolumeCapabilities(_ context.Context, q *csi.ValidateVolumeCapabilitiesRequest, _ ...grpc.CallOption) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: q.VolumeCapabilities}}, nil
}
func (c *controller) ControllerPublishVolume(context.Context, *csi.ControllerPublishVolumeRequest, ...grpc.CallOption) (*csi.ControllerPublishVolumeResponse, error) {
	c.attach++
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{"attachment": "current"}}, nil
}
func (c *controller) ControllerUnpublishVolume(context.Context, *csi.ControllerUnpublishVolumeRequest, ...grpc.CallOption) (*csi.ControllerUnpublishVolumeResponse, error) {
	c.detach++
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

type drivers struct{ c *controller }

func (d drivers) Controller(context.Context, string) (*driver.Session, error) {
	return &driver.Session{Controller: d.c, Attach: true}, nil
}
func (drivers) NodeID(context.Context, string, string) (string, error) { return "actual-csi-node", nil }

type testEvidence struct{ reject bool }

func (e testEvidence) Verify(context.Context, string, string, string) error {
	if e.reject {
		return errors.New("runtime not ready")
	}
	return nil
}

func TestPostgresConsumptionLifecycle(t *testing.T) {
	url := os.Getenv("SSF_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated PostgreSQL")
	}
	ctx := context.Background()
	db, e := ledger.Open(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = db.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	id := time.Now().Format("150405.000000")
	ref := core.PVCRef{Namespace: "n", Name: "p", UID: id}
	p := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n", UID: types.UID(id), Labels: map[string]string{kube.ManagedLabel: "true"}}, Spec: v1.PersistentVolumeClaimSpec{VolumeName: "pv", AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}}, Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound}}
	pv := &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: types.UID("pv-" + id)}, Spec: v1.PersistentVolumeSpec{ClaimRef: &v1.ObjectReference{Namespace: "n", Name: "p", UID: p.UID}, PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimRetain, PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "test", VolumeHandle: id}}}}
	fail := admission.Fail
	none := admission.SideEffectClassNone
	gate := &admission.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "ssf-guard"}, Webhooks: []admission.ValidatingWebhook{{FailurePolicy: &fail, SideEffects: &none, ClientConfig: admission.WebhookClientConfig{Service: &admission.ServiceReference{Name: "ssf-guard", Namespace: "ssf-system"}, CABundle: []byte("test")}, Rules: []admission.RuleWithOperations{{Operations: []admission.OperationType{admission.Create, admission.Update, admission.Delete}, Rule: admission.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods", "persistentvolumeclaims", "persistentvolumes"}}}}}}}
	api := fake.NewClientset(p, pv, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}, gate)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	backend := &controller{}
	c := &Coordinator{Kube: &kube.Client{API: api}, Ledger: db, Placement: placement{}, Drivers: drivers{backend}, Authorizer: authorizer{}, SigningKey: key}
	var results []Result
	for _, instance := range []string{"one", "two"} {
		r, e := c.Prepare(ctx, Request{ID: id + instance, Tenant: "tenant", Sandbox: id + instance, Mounts: []core.Mount{{PVC: ref, MountID: "data", GuestPath: "/data", ReadOnly: true}}})
		if e != nil {
			t.Fatal(e)
		}
		results = append(results, r)
	}
	for _, r := range results {
		if e = c.Commit(ctx, r); e == nil {
			t.Fatal("committed without trusted receipt")
		}
		c.Evidence = testEvidence{reject: true}
		if e = c.ConfirmReady(ctx, r, r.Plan.Sandbox, "proof"); e == nil {
			t.Fatal("accepted unready runtime")
		}
		c.Evidence = testEvidence{}
		forged := r
		forged.Plan.Node = "different"
		if e = c.ConfirmReady(ctx, forged, r.Plan.Sandbox, "proof"); !errors.Is(e, core.ErrConflict) {
			t.Fatalf("forged plan: %v", e)
		}
		if e = c.ConfirmReady(ctx, r, r.Plan.Sandbox, "proof"); e != nil {
			t.Fatal(e)
		}
		if e = c.Commit(ctx, r); e != nil {
			t.Fatal(e)
		}
		if e = c.Commit(ctx, r); e != nil {
			t.Fatalf("commit replay: %v", e)
		}
		if e = c.Commit(ctx, forged); !errors.Is(e, core.ErrConflict) {
			t.Fatalf("forged commit replay: %v", e)
		}
	}
	if backend.attach != 1 {
		t.Fatalf("attachment not reused: %d", backend.attach)
	}
	if e = c.Release(ctx, results[0].Operation, func(context.Context, string) error { return nil }); e != nil {
		t.Fatal(e)
	}
	if backend.detach != 0 {
		t.Fatal("detached shared attachment")
	}
	if e = c.Release(ctx, results[1].Operation, func(context.Context, string) error { return nil }); e != nil {
		t.Fatal(e)
	}
	if backend.detach != 1 {
		t.Fatal("last holder not detached")
	}
	if e = c.Drain(ctx, id, "Retain"); e != nil {
		t.Fatal(e)
	}
}
