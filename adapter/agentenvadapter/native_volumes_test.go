package agentenvadapter

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"math"
	kube "ssf.dev/ssf/kubernetes"
	"testing"

	"ssf.dev/ssf/core"
)

func TestNativeObservedCapacityIncludesBackendRounding(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	claim := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: "test", UID: types.UID(id)}, Spec: v1.PersistentVolumeClaimSpec{Resources: v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")}}}, Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound, Capacity: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Gi")}}}
	v := NativeVolume{Name: id, Tenant: "test", PVC: core.PVCRef{Namespace: "test", Name: id, UID: id}, Bytes: 1 << 20, State: "Pending"}
	s := NativeVolumes{Ledger: c.Ledger, Kube: &kube.Client{API: fake.NewSimpleClientset(claim)}}
	if err := s.save(ctx, v); err != nil {
		t.Fatal(err)
	}
	observed, err := s.Observe(ctx, "test", id)
	if err != nil || observed.Bytes != 1<<30 {
		t.Fatalf("capture could clone to a too-small PVC: %+v %v", observed, err)
	}
	if observed.IntentDigest != v.IntentDigest {
		t.Fatal("observation rewrote create intent")
	}
}

func TestNativeVolumeRejectsUnsafeSourcesBeforeProvisioning(t *testing.T) {
	provider := &NativeVolumes{} // validation must not touch Kubernetes or the ledger
	principal := Principal{Tenant: "tenant", Namespaces: []string{"namespace"}}
	base := NativeVolumeRequest{Name: "data", Namespace: "namespace", StorageClass: "block", Bytes: 1 << 20}
	for name, mutate := range map[string]func(*NativeVolumeRequest){
		"uninitialized-readonly": func(q *NativeVolumeRequest) { q.ReadOnly = true },
		"unknown-provider":       func(q *NativeVolumeRequest) { q.Provider = "unknown" },
		"multiple-sources": func(q *NativeVolumeRequest) {
			q.Import = &core.PVCRef{Namespace: "namespace", Name: "existing", UID: "uid"}
			q.Snapshot = "snapshot"
		},
		"unauthorized-namespace": func(q *NativeVolumeRequest) { q.Namespace = "another" },
	} {
		t.Run(name, func(t *testing.T) {
			q := base
			mutate(&q)
			if _, err := provider.Create(context.Background(), principal, q); err == nil {
				t.Fatal("unsafe source accepted")
			}
		})
	}
}
func TestNativeVolumeViewDoesNotOverflowCapacity(t *testing.T) {
	v := NativeVolume{Name: "data", Provider: "ssf", Bytes: math.MaxInt64, State: "Bound", ReadOnly: true}
	view := v.View()
	if view.SizeMB != uint64(1)<<43 || view.Mode != "ro" || view.Status != "ready" {
		t.Fatalf("invalid public volume view: %+v", view)
	}
	if view.Capabilities["snapshot"] || view.Capabilities["offlineExpand"] {
		t.Fatal("unfinished capability advertised")
	}
}

func TestNativeInitializationCanContinueOnlyAfterPriorOperationReleased(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	s := NativeVolumes{Ledger: c.Ledger}
	prior, next := id+"old", id+"new"
	if err := c.Ledger.Begin(ctx, prior, "old"); err != nil {
		t.Fatal(err)
	}
	if err := c.Ledger.Begin(ctx, next, "new"); err != nil {
		t.Fatal(err)
	}
	v := NativeVolume{Tenant: "test", Name: id, InitializeExt4: true, InitializationOwner: prior}
	if err := s.save(ctx, v); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AuthorizeInitialization(ctx, "test", id, next); err != nil || ok {
		t.Fatalf("unknown prior initializer replaced: %v %v", ok, err)
	}
	if err := c.Ledger.SetOperation(ctx, prior, "Released", ""); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AuthorizeInitialization(ctx, "test", id, next); err != nil || !ok {
		t.Fatalf("safe blank-volume retry blocked: %v %v", ok, err)
	}
	if ok, err := s.AuthorizeInitialization(ctx, "test", id, prior); err != nil || ok {
		t.Fatalf("old initializer resurrected: %v %v", ok, err)
	}
	v.InitializeExt4 = false
	v.InitializationOwner = ""
	if err := s.save(ctx, v); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AuthorizeInitialization(ctx, "test", id, next); err != nil || ok {
		t.Fatalf("import/restore initialization granted: %v %v", ok, err)
	}
}
