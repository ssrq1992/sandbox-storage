// Package config loads shared runtime configuration helpers.
package config

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Kube resolves a Kubernetes client configuration: the in-cluster
// configuration when no kubeconfig path is given, the named file otherwise.
func Kube(path string) (*rest.Config, error) {
	if path == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", path)
}
