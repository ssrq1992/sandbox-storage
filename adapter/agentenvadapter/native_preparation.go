package agentenvadapter

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/preparation"
)

type preparationClient struct{ api.RuntimeStorageNodeClient }

func (c preparationClient) PrepareVolumeContent(ctx context.Context, q *api.PrepareRequest) (*api.VolumePreparation, error) {
	return c.RuntimeStorageNodeClient.PrepareVolumeContent(ctx, q)
}
func (c preparationClient) InspectVolumePreparation(ctx context.Context, q *api.InspectRequest) (*api.VolumePreparation, error) {
	return c.RuntimeStorageNodeClient.InspectVolumePreparation(ctx, q)
}
func (c preparationClient) ReleaseVolumePreparation(ctx context.Context, q *api.PrepareRequest) (*api.VolumePreparation, error) {
	return c.RuntimeStorageNodeClient.ReleaseVolumePreparation(ctx, q)
}

// The independent coordinator hosts this worker. Discovery still passes through
// Refresh and the original Scheduler's authenticated heartbeat projection.
func (c *NativeControl) openPreparation(cfg NativeConfig, tlsConfig *tls.Config) (func(), error) {
	if len(cfg.VolumeProfiles) == 0 {
		return func() {}, nil
	}
	if err := kube.ValidateProfiles(cfg.VolumeProfiles); err != nil {
		return nil, err
	}
	for _, p := range cfg.VolumeProfiles {
		if !supportedVolumeProfile(p) {
			return nil, errors.New("unsupported AgentENV content profile")
		}
	}
	if cfg.PreparationNodePort == "" {
		return nil, errors.New("preparation_node_port must identify the common SSF Node API")
	}
	discover := func(ctx context.Context, node string, admission bool) ([]NativeCandidate, error) {
		var inputs []NativeCandidate
		if cfg.Standalone != nil {
			inputs = []NativeCandidate{{ID: cfg.Standalone.NodeID, Endpoint: cfg.Standalone.Endpoint}}
		} else {
			options := metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=agentenv-node"}
			if node != "" {
				options.FieldSelector = "spec.nodeName=" + node
			}
			pods, err := c.Kube.API.CoreV1().Pods(cfg.Namespace).List(ctx, options)
			if err != nil {
				return nil, err
			}
			for _, p := range pods.Items {
				if p.Status.PodIP != "" {
					inputs = append(inputs, NativeCandidate{ID: p.Name, Endpoint: "http://" + net.JoinHostPort(p.Status.PodIP, "8000")})
				}
			}
		}
		var candidates []NativeCandidate
		for _, input := range inputs {
			refresh := c.Refresh
			if !admission && c.RefreshStopped != nil {
				refresh = c.RefreshStopped
			}
			candidate, err := refresh(ctx, input)
			if err == nil && (node == "" || candidate.KubeNode == node) {
				candidates = append(candidates, candidate)
			}
		}
		if len(candidates) == 0 {
			return nil, core.ErrPending
		}
		return candidates, nil
	}
	resolve := func(ctx context.Context, p core.Placement, requireBoot bool) (NativeCandidate, error) {
		candidates, err := discover(ctx, p.Node, false)
		if err != nil {
			return NativeCandidate{}, err
		}
		for _, candidate := range candidates {
			if candidate.NodeUID == p.NodeUID && (!requireBoot || candidate.BootID == p.BootID) {
				return candidate, nil
			}
		}
		return NativeCandidate{}, core.ErrConflict
	}
	var mu sync.Mutex
	connections := map[string]*grpc.ClientConn{}
	co := &preparation.Coordinator{Kube: c.Kube, Ledger: c.Ledger, Drivers: c.Drivers, SigningKey: c.SigningKey, Authorizer: namespaceAuth{c.Principal},
		Tenant: c.Principal.Tenant, Namespaces: append([]string{}, c.Principal.Namespaces...), Profiles: cfg.VolumeProfiles}
	co.Choose = func(ctx context.Context, v core.Volume) (core.Placement, error) {
		candidates, err := discover(ctx, "", true)
		if err != nil {
			return core.Placement{}, err
		}
		for _, candidate := range candidates {
			if err := c.Kube.CheckTopology(ctx, v, candidate.KubeNode); err != nil {
				if errors.Is(err, kube.ErrTopology) {
					continue
				}
				return core.Placement{}, err
			}
			return core.Placement{ID: "preparation/" + v.PVC.UID, Node: candidate.KubeNode, NodeUID: candidate.NodeUID, BootID: candidate.BootID, Expires: time.Now().Add(5 * time.Minute)}, nil
		}
		return core.Placement{}, core.ErrPending
	}
	co.Validate = func(ctx context.Context, p core.Placement) error { _, err := resolve(ctx, p, true); return err }
	co.Refresh = func(ctx context.Context, p core.Placement) (core.Placement, error) {
		candidate, err := resolve(ctx, p, false)
		if err != nil {
			return p, err
		}
		p.BootID, p.Expires = candidate.BootID, time.Now().Add(5*time.Minute)
		return p, nil
	}
	co.Node = func(ctx context.Context, p core.Placement) (preparation.NodeClient, error) {
		// Stop/inspect may address an earlier boot on the same physical node;
		// Prepare still verifies the signed current boot at the Node API.
		candidate, err := resolve(ctx, p, false)
		if err != nil {
			return nil, err
		}
		endpoint, err := url.Parse(candidate.Endpoint)
		if err != nil {
			return nil, err
		}
		address := net.JoinHostPort(endpoint.Hostname(), cfg.PreparationNodePort)
		mu.Lock()
		defer mu.Unlock()
		conn := connections[address]
		if conn == nil {
			settings := tlsConfig.Clone()
			settings.ServerName = cfg.PreparationServerName
			conn, err = grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(settings)))
			if err != nil {
				return nil, err
			}
			connections[address] = conn
		}
		return preparationClient{api.NewRuntimeStorageNodeClient(conn)}, nil
	}
	c.CaptureNode = func(ctx context.Context, p core.Placement) (api.RuntimeStorageNodeClient, error) {
		client, err := co.Node(ctx, p)
		if err != nil {
			return nil, err
		}
		native, ok := client.(preparationClient)
		if !ok {
			return nil, errors.New("common Node client unavailable")
		}
		return native.RuntimeStorageNodeClient, nil
	}
	c.Preparation = co
	return func() {
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
	}, nil
}
