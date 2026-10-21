// cleanup-pvc only deletes PVCs created by a recorded native PVC test run.
// Kubernetes UID preconditions prevent deleting a same-name replacement.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type claim struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
	Created   bool   `json:"created_by_test"`
}

func run() error {
	report := flag.String("report", "", "native_pvc_e2e report.json")
	kubeconfig := flag.String("kubeconfig", "", "explicit test kubeconfig")
	execute := flag.Bool("execute", false, "submit native PVC DELETE requests with UID preconditions")
	flag.Parse()
	raw, err := os.ReadFile(*report)
	if err != nil {
		return err
	}
	var evidence struct {
		Version int     `json:"version"`
		Scope   string  `json:"scope"`
		Run     string  `json:"run"`
		Claims  []claim `json:"pvcs"`
	}
	if err = json.Unmarshal(raw, &evidence); err != nil {
		return err
	}
	if evidence.Version != 1 || evidence.Scope != "kubernetes-pvc-native-api" || evidence.Run == "" {
		return errors.New("not a native PVC test report")
	}
	var claims []claim
	for _, p := range evidence.Claims {
		if !p.Created {
			continue
		}
		if p.UID == "" || p.Namespace == "" || !strings.HasPrefix(p.Name, evidence.Run+"-") {
			return errors.New("PVC is not owned by this test run")
		}
		claims = append(claims, p)
	}
	if !*execute {
		return json.NewEncoder(os.Stdout).Encode(claims)
	}
	if *kubeconfig == "" {
		return errors.New("explicit test kubeconfig required")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	for _, p := range claims {
		uid := types.UID(p.UID)
		err = client.CoreV1().PersistentVolumeClaims(p.Namespace).Delete(context.Background(), p.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		fmt.Printf("DELETE accepted for %s/%s (%s); finalizer drain and PV retention remain authoritative\n", p.Namespace, p.Name, p.UID)
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
