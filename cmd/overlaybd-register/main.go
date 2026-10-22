// overlaybd-register validates an idle managed native volume and emits a
// prebound static PV. It never creates/formats a backend or applies Kubernetes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/config"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/drivers/overlaybd"
	"ssf.dev/ssf/ledger"
)

func run() error {
	var q overlaybd.StaticRegistration
	flag.StringVar(&q.Repository, "repository", "", "isolated managed repository ID")
	flag.StringVar(&q.NativeID, "native-volume", "", "existing managed native volume ID")
	flag.StringVar(&q.ExpectedHead, "expected-head", "", "administrator-verified immutable current head digest")
	flag.StringVar(&q.PVName, "pv", "", "new static PV name, already selected by PVC spec.volumeName")
	flag.BoolVar(&q.Readonly, "readonly", false, "require a native readonly volume and ReadOnlyMany PVC")
	namespace := flag.String("namespace", "", "PVC namespace")
	pvc := flag.String("pvc", "", "existing protected Pending PVC name")
	kubeconfig := flag.String("kubeconfig", "", "Kubernetes client configuration")
	database := flag.String("database-url-file", "", "file containing the dedicated driver/public ledger database URL")
	socket := flag.String("backend-socket", "", "private native host UDS; run on the repository worker")
	flag.Parse()
	if *namespace == "" || *pvc == "" {
		return errors.New("explicit namespace/PVC required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg, err := config.Kube(*kubeconfig)
	if err != nil {
		return err
	}
	cluster, err := k8s.NewForConfig(cfg)
	if err != nil {
		return err
	}
	claim, err := cluster.CoreV1().PersistentVolumeClaims(*namespace).Get(ctx, *pvc, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if claim.Spec.StorageClassName == nil {
		return errors.New("explicit OverlayBD StorageClass required")
	}
	class, err := cluster.StorageV1().StorageClasses().Get(ctx, *claim.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	prior, err := cluster.CoreV1().PersistentVolumes().Get(ctx, q.PVName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil && (prior.DeletionTimestamp != nil || prior.Spec.ClaimRef == nil || prior.Spec.ClaimRef.UID != claim.UID || prior.Spec.CSI == nil || prior.Spec.CSI.Driver != overlaybd.DriverName) {
		return errors.New("existing PV belongs to another claim or driver")
	}
	url, err := os.ReadFile(*database)
	if err != nil {
		return err
	}
	store, err := ledger.Open(ctx, strings.TrimSpace(string(url)))
	if err != nil {
		return err
	}
	defer store.Close()
	// Schema setup remains a deployment responsibility; registration never runs
	// migrations against an operator's database.
	journal := &overlaybd.Journal{Pool: store.Pool}
	backend, err := overlaybd.DialBackend(ctx, *socket, overlaybd.CapabilityNativeVolume, overlaybd.CapabilityStaticRegistration)
	if err != nil {
		return err
	}
	defer backend.Close()
	pv, err := journal.RegisterStatic(ctx, api.NewNativeStorageBackendClient(backend), q, claim, class)
	if err != nil {
		return err
	}
	next, err := cluster.CoreV1().PersistentVolumeClaims(*namespace).Get(ctx, *pvc, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if next.UID != claim.UID || next.DeletionTimestamp != nil || core.Digest(next.Spec) != core.Digest(claim.Spec) {
		return errors.New("PVC changed during registration; do not apply the PV")
	}
	if prior != nil && core.Digest(prior.Spec) != core.Digest(pv.Spec) {
		return errors.New("existing PV differs; refusing replacement manifest")
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(pv)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
