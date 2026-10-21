package kubernetes

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"ssf.dev/ssf/core"
	"testing"
	"time"
)

func TestClusterNativeProvisioning(t *testing.T) {
	config := os.Getenv("SSF_TEST_KUBECONFIG")
	if config == "" {
		t.Skip("requires dedicated kind fixture kubeconfig")
	}
	cfg, e := clientcmd.BuildConfigFromFlags("", config)
	if e != nil {
		t.Fatal(e)
	}
	api, e := kubernetes.NewForConfig(cfg)
	if e != nil {
		t.Fatal(e)
	}
	c := &Client{API: api}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	nodes, e := api.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "ssf.dev/enabled=true"})
	if e != nil || len(nodes.Items) == 0 {
		t.Fatalf("fixture node unavailable: %v", e)
	}
	ns := "ssf-e2e-" + time.Now().Format("150405")
	if _, e = api.CoreV1().Namespaces().Create(ctx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); e != nil {
		t.Fatal(e)
	}
	t.Log("Retain test resources left for inspection in", ns)
	for _, class := range []string{"ssf-file-immediate", "ssf-file-wffc", "ssf-block-immediate", "ssf-block-wffc"} {
		t.Run(class, func(t *testing.T) {
			mode := v1.PersistentVolumeFilesystem
			if class == "ssf-block-immediate" || class == "ssf-block-wffc" {
				mode = v1.PersistentVolumeBlock
			}
			p, e := c.CreatePVC(ctx, &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: class, Namespace: ns}, Spec: v1.PersistentVolumeClaimSpec{StorageClassName: &class, VolumeMode: &mode, AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, Resources: v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("64Mi")}}}})
			if e != nil {
				t.Fatal(e)
			}
			v, e := c.EnsureBound(ctx, core.PVCRef{Namespace: ns, Name: p.Name, UID: string(p.UID)}, nodes.Items[0].Name, false, func(context.Context) error { return nil })
			if e != nil {
				t.Fatal(e)
			}
			if v.Handle == "" || v.PVUID == "" {
				t.Fatal("native binding missing identity")
			}
		})
	}
}
