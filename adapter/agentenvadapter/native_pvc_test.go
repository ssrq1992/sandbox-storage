package agentenvadapter

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/preparation"
)

func TestNativePVCReferencePinsUIDWithoutVisibleAlias(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	c.Principal.Tenant = id
	c.Principal.Namespaces = []string{"test"}
	c.DefaultNamespace = "test"
	profile := DescribeVolumeProfile()
	mode, retain := v1.PersistentVolumeBlock, v1.PersistentVolumeReclaimRetain
	class := "profiled"
	claim := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "test", UID: types.UID(id + "pvc"),
		Labels: map[string]string{kube.ManagedLabel: "true"}, Finalizers: []string{kube.VolumeProtectionFinalizer},
		Annotations: map[string]string{kube.VolumeProfileAnnotation: profile.Name, kube.ProfileRevisionAnnotation: core.Digest(profile),
			kube.ProfileClassUIDAnnotation: "class-uid", kube.SourceRevisionAnnotation: core.Digest("blank")}},
		Spec: v1.PersistentVolumeClaimSpec{StorageClassName: &class, VolumeMode: &mode, AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Gi")}}}}
	c.Kube = &kube.Client{API: fake.NewClientset(claim, &storage.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: class, UID: "class-uid",
		Annotations: map[string]string{kube.VolumeProfileAnnotation: profile.Name}}, ReclaimPolicy: &retain})}
	c.Volumes = &NativeVolumes{Ledger: c.Ledger, Kube: c.Kube}
	c.Preparation = &preparation.Coordinator{Kube: c.Kube, Ledger: c.Ledger, Authorizer: namespaceAuth{c.Principal}, Tenant: c.Principal.Tenant, Profiles: []core.VolumeProfile{profile}}
	mount := NativeMount{PVC: &NativePVCSource{Name: "data"}, Path: "/data"}
	resolved, err := c.resolvePVCMount(ctx, id+"direct", 0, mount)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Volumes.Get(ctx, c.Principal.Tenant, resolved.Name)
	if err != nil || !v.DirectPVC || !v.PVCSource || v.PVC.UID != string(claim.UID) || v.InitializeExt4 {
		t.Fatalf("bad direct descriptor: %+v %v", v, err)
	}
	visible, err := c.Volumes.List(ctx, c.Principal.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range visible {
		if item.Name == resolved.Name {
			t.Fatal("direct reference exposed as a volume registration")
		}
	}
	if _, err = c.Volumes.Lookup(ctx, c.Principal.Tenant, resolved.Name); err == nil {
		t.Fatal("direct reference exposed through alias lookup")
	}
	if err = c.Volumes.Delete(ctx, c.Principal.Tenant, resolved.Name); err == nil {
		t.Fatal("facade deleted Kubernetes-owned PVC")
	}
	mount.PVC.Namespace = "foreign"
	if _, err = c.resolvePVCMount(ctx, id+"foreign", 0, mount); err == nil {
		t.Fatal("cross-namespace reference accepted")
	}
	mount.PVC.Namespace = ""
	c.DefaultNamespace = ""
	if _, err = c.resolvePVCMount(ctx, id+"default", 0, mount); err == nil {
		t.Fatal("implicit namespace guessed")
	}
	c.DefaultNamespace = "test"
	claim.UID = types.UID(id + "replacement")
	if _, err = c.Kube.API.CoreV1().PersistentVolumeClaims("test").Update(context.Background(), claim, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.resolvePVCMount(ctx, id+"direct", 0, mount); err == nil {
		t.Fatal("retry rebound to replacement PVC")
	}
}
