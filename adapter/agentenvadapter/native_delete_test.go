package agentenvadapter

import (
	"errors"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
)

func TestNativeVolumeDeleteRespectsPauseHoldAndUID(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	ref := core.PVCRef{Namespace: "test", Name: id, UID: id}
	api := fake.NewSimpleClientset(&v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: "test", UID: types.UID(id), Labels: map[string]string{kube.ManagedLabel: "true"}}})
	volumes := &NativeVolumes{Ledger: c.Ledger, Kube: &kube.Client{API: api}}
	volume := NativeVolume{Name: id, Tenant: "test", Provider: "ssf", PVC: ref, State: "Pending"}
	if err := volumes.save(ctx, volume); err != nil {
		t.Fatal(err)
	}
	physical := core.Volume{PVC: ref, Driver: "test", Handle: id}
	if err := c.Ledger.Register(ctx, physical); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ledger.Pool.Exec(ctx, `INSERT INTO ssf_use_holds(volume_key,owner_uid,readonly,reason) VALUES($1,$2,false,'paused')`, physical.Key(), id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = c.Ledger.Pool.Exec(ctx, `DELETE FROM ssf_use_holds WHERE owner_uid=$1`, id)
		_, _ = c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_volumes WHERE tenant='test' AND name=$1`, id)
	})
	if err := volumes.Delete(ctx, "test", id); !errors.Is(err, core.ErrPending) {
		t.Fatalf("paused hold not respected: %v", err)
	}
	for _, action := range api.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("PVC deleted while paused owner holds it")
		}
	}
	if err := c.Ledger.ReleaseUse(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := volumes.Delete(ctx, "test", id); !errors.Is(err, core.ErrPending) {
		t.Fatalf("DELETE acceptance mistaken for completion: %v", err)
	}
	observed, err := volumes.Observe(ctx, "test", id)
	if err != nil || observed.State != "Deleting" {
		t.Fatalf("observe erased deletion intent: %+v %v", observed, err)
	}
	if err := volumes.Delete(ctx, "test", id); err != nil {
		t.Fatal(err)
	}
	observed, err = volumes.Get(ctx, "test", id)
	if err != nil || observed.State != "Deleted" {
		t.Fatalf("missing tombstone: %+v %v", observed, err)
	}
	if err := volumes.Delete(ctx, "test", id); err != nil {
		t.Fatalf("delete replay: %v", err)
	}
}
