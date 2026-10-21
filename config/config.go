package config

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"ssf.dev/ssf/core"
	driver "ssf.dev/ssf/drivers/csi"
)

type Driver struct{ Name, ControllerEndpoint, NodeEndpoint, Revision string }
type Config struct {
	PreparationEndpoint, PreparationTokenFile                                                                                                         string
	VolumeProfiles                                                                                                                                    []core.VolumeProfile
	ManagedNamespaces, CoordinatorUsers                                                                                                               []string
	Listen, HealthListen, DatabaseURL, Kubeconfig, Root, NodeName, BootID, PlanPublicKey, Cert, Key, CA, DriverName, DriverEndpoint, EvidenceEndpoint string
	AllowedPeers                                                                                                                                      []string
	Drivers                                                                                                                                           []Driver
}

func Load(file string) (Config, error) {
	var c Config
	b, e := os.ReadFile(file)
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	if c.DatabaseURL == "" {
		c.DatabaseURL = os.Getenv("SSF_DATABASE_URL")
	}
	return c, e
}
func Kube(path string) (*rest.Config, error) {
	if path == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", path)
}

type Registry struct {
	Config   Config
	Registry driver.Registry
	TLS      *tls.Config
	API      kubernetes.Interface
}

func (r *Registry) session(ctx context.Context, name, role string) (*driver.Session, error) {
	for _, d := range r.Config.Drivers {
		if d.Name == name {
			ep := d.ControllerEndpoint
			if role == "node" {
				ep = d.NodeEndpoint
			}
			return r.Registry.Get(ctx, ep, name, role, d.Revision, r.TLS)
		}
	}
	return nil, errors.New("driver not registered")
}
func (r *Registry) Node(ctx context.Context, name string) (*driver.Session, error) {
	return r.session(ctx, name, "node")
}
func (r *Registry) Controller(ctx context.Context, name string) (*driver.Session, error) {
	return r.session(ctx, name, "controller")
}
func (r *Registry) NodeID(ctx context.Context, node, driver string) (string, error) {
	n, e := r.API.StorageV1().CSINodes().Get(ctx, node, metav1.GetOptions{})
	if e != nil {
		return "", e
	}
	for _, d := range n.Spec.Drivers {
		if d.Name == driver && d.NodeID != "" {
			return d.NodeID, nil
		}
	}
	return "", errors.New("CSI node identity not registered")
}
