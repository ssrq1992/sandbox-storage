package agentenvadapter

import (
	"encoding/json"
	"errors"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"testing"
)

func TestFailedInternalCloneGCNeverDeletesExposedOrSharedVolumes(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	intent, _ := json.Marshal(NativeIntent{Tenant: c.Principal.Tenant, Path: "/v2/sandboxes"})
	if _, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Released',ever_ready=true,intent=$2 WHERE id=$1`, id, intent); err != nil {
		t.Fatal(err)
	}
	claim := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: "test", UID: types.UID(id + "pvc"), Labels: map[string]string{kube.ManagedLabel: "true"}, Annotations: map[string]string{"agentenv.ssf.dev/intent": "digest"}}, Spec: v1.PersistentVolumeClaimSpec{VolumeName: id + "pv"}}
	pv := &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: id + "pv", UID: types.UID(id + "pvuid"), Labels: map[string]string{kube.ManagedLabel: "true"}}, Spec: v1.PersistentVolumeSpec{ClaimRef: &v1.ObjectReference{Namespace: "test", Name: id, UID: claim.UID}, PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimRetain, PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "test", VolumeHandle: id}}}}
	api := fake.NewSimpleClientset(claim, pv)
	c.Kube = &kube.Client{API: api}
	c.Volumes = &NativeVolumes{Ledger: c.Ledger, Kube: c.Kube}
	volume := NativeVolume{Tenant: c.Principal.Tenant, Name: id, Provider: "ssf", InternalOwner: id, PVC: core.PVCRef{Namespace: "test", Name: id, UID: string(claim.UID)}, IntentDigest: "digest", State: "Bound"}
	if err := c.Volumes.save(ctx, volume); err != nil {
		t.Fatal(err)
	}
	if err := c.reclaimNativeClone(ctx, id, id); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("exposed user volume reclaimed: %v", err)
	}
	if len(api.Actions()) != 0 {
		t.Fatal("user volume touched")
	}
	if _, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET ever_ready=false,state='Dispatched' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := c.reclaimNativeClone(ctx, id, id); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("unknown writer volume reclaimed: %v", err)
	}
	if _, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Released' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	volume.InternalOwner = "another-owner"
	if err := c.Volumes.save(ctx, volume); err != nil {
		t.Fatal(err)
	}
	if err := c.reclaimNativeClone(ctx, id, id); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("shared volume reclaimed: %v", err)
	}
	volume.InternalOwner = id
	if err := c.Volumes.save(ctx, volume); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		err := c.reclaimNativeClone(ctx, id, id)
		if err != nil && !errors.Is(err, core.ErrPending) {
			t.Fatal(err)
		}
	}
	got, err := c.Volumes.Get(ctx, c.Principal.Tenant, id)
	if err != nil || got.State != "Deleted" {
		t.Fatalf("reclaim incomplete: %+v %v", got, err)
	}
	if _, err = api.CoreV1().PersistentVolumeClaims("test").Get(ctx, id, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("PVC retained: %v", err)
	}
	if _, err = api.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("PV retained: %v", err)
	}
}
