package sandboxschedadapter

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/config"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
	"ssf.dev/ssf/preparation"
)

// createRunsTable creates the plugin's own ledger table.
func createRunsTable(ctx context.Context, db *ledger.Store) error {
	_, err := db.Session(ctx).Exec(ctx, `
CREATE TABLE IF NOT EXISTS sandboxsched_runs (
  id text PRIMARY KEY,
  digest text NOT NULL,
  tenant text NOT NULL,
  owner_uid text NOT NULL,
  run_uid text NOT NULL,
  state text NOT NULL,
  dispatch jsonb NOT NULL,
  created timestamptz NOT NULL DEFAULT now(),
  updated timestamptz NOT NULL DEFAULT now()
)`)
	if err != nil {
		return err
	}
	_, err = db.Session(ctx).Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_sandboxsched_runs_state ON sandboxsched_runs(state)`)
	return err
}

// OpenSandboxSchedControl opens the coordinator-hosted control plugin state.
func OpenSandboxSchedControl(ctx context.Context, cfg ControlConfig) (*SandboxControl, func(), error) {
	db, err := ledger.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, err
	}
	fail := func(e error) (*SandboxControl, func(), error) { db.Close(); return nil, nil, e }
	if err = db.Migrate(ctx); err != nil {
		return fail(err)
	}
	if err = createRunsTable(ctx, db); err != nil {
		return fail(err)
	}
	rest, err := config.Kube(cfg.Kubeconfig)
	if err != nil {
		return fail(err)
	}
	api, err := kubernetes.NewForConfig(rest)
	if err != nil {
		return fail(err)
	}
	dyn, err := dynamic.NewForConfig(rest)
	if err != nil {
		return fail(err)
	}
	clientTLS, err := auth.TLS(cfg.Cert, cfg.Key, cfg.CA, false)
	if err != nil {
		return fail(err)
	}
	key, err := readKey(cfg.SigningKey, ed25519.PrivateKeySize)
	if err != nil {
		return fail(err)
	}
	rejectRedirect := func(*http.Request, []*http.Request) error { return errors.New("storage redirect forbidden") }
	pluginTLS := clientTLS.Clone()
	pluginTLS.ServerName = cfg.NodeServerName
	pluginHTTP := &http.Client{Transport: &http.Transport{TLSClientConfig: pluginTLS}, Timeout: 30 * time.Second, CheckRedirect: rejectRedirect}
	k := &kube.Client{API: api, Dynamic: dyn, Selection: db}
	control := &SandboxControl{Ledger: db, Kube: k, SigningKey: ed25519.PrivateKey(key), Principal: cfg.Principal,
		Config: cfg, PluginHTTP: pluginHTTP,
		Drivers: &config.Registry{Config: config.Config{Drivers: configDrivers(cfg.Drivers)}, API: api, TLS: clientTLS}}
	closePreparation, err := openSandboxPreparation(control, cfg, clientTLS, pluginHTTP)
	if err != nil {
		return fail(err)
	}
	// Drive autonomous Immediate-mode preparation for the principal
	// namespaces: managed claims become prepared without waiting for a
	// consumer sandbox.
	prepDone := make(chan struct{})
	prepCtx, cancelPrep := context.WithCancel(ctx)
	go func() {
		defer close(prepDone)
		control.Preparation.Run(prepCtx, 10*time.Second, func(err error) { slog.Warn("volume preparation retained work", "error", err) })
	}()
	return control, func() { closePreparation(); cancelPrep(); <-prepDone; db.Close() }, nil
}

func configDrivers(drivers []DriverConfig) []config.Driver {
	out := make([]config.Driver, 0, len(drivers))
	for _, d := range drivers {
		out = append(out, config.Driver{Name: d.Name, ControllerEndpoint: d.ControllerEndpoint, Revision: d.Revision})
	}
	return out
}

// preparationClient adapts the node gRPC client to the preparation contract.
type preparationClient struct{ v1.RuntimeStorageNodeClient }

func (c preparationClient) PrepareVolumeContent(ctx context.Context, q *v1.PrepareRequest) (*v1.VolumePreparation, error) {
	return c.RuntimeStorageNodeClient.PrepareVolumeContent(ctx, q)
}

func (c preparationClient) InspectVolumePreparation(ctx context.Context, q *v1.InspectRequest) (*v1.VolumePreparation, error) {
	return c.RuntimeStorageNodeClient.InspectVolumePreparation(ctx, q)
}

func (c preparationClient) ReleaseVolumePreparation(ctx context.Context, q *v1.PrepareRequest) (*v1.VolumePreparation, error) {
	return c.RuntimeStorageNodeClient.ReleaseVolumePreparation(ctx, q)
}

// nodeInfo is the node service identity projection.
type nodeInfo struct {
	Name   string `json:"name"`
	UID    string `json:"uid"`
	BootID string `json:"boot_id"`
	Ready  bool   `json:"ready"`
}

// openSandboxPreparation wires the WFFC volume preparation flow. Preparation
// plans travel to the public node agent; the engine forwards content work to
// the node service preparation hook.
func openSandboxPreparation(control *SandboxControl, cfg ControlConfig, tlsConfig *tls.Config, pluginHTTP *http.Client) (func(), error) {
	if len(cfg.VolumeProfiles) == 0 {
		return func() {}, nil
	}
	if err := kube.ValidateProfiles(cfg.VolumeProfiles); err != nil {
		return nil, err
	}
	nodePods := func(ctx context.Context, node string) ([]string, error) {
		options := metav1.ListOptions{LabelSelector: cfg.NodeServiceLabel}
		if node != "" {
			options.FieldSelector = "spec.nodeName=" + node
		}
		pods, err := control.Kube.API.CoreV1().Pods(cfg.Namespace).List(ctx, options)
		if err != nil {
			return nil, err
		}
		var out []string
		for _, p := range pods.Items {
			if p.Status.PodIP != "" && p.DeletionTimestamp == nil {
				out = append(out, p.Status.PodIP)
			}
		}
		return out, nil
	}
	queryInfo := func(ctx context.Context, podIP string) (nodeInfo, error) {
		var info nodeInfo
		req, err := http.NewRequestWithContext(ctx, "GET", "https://"+net.JoinHostPort(podIP, cfg.NodeServicePort)+"/v1/info", nil)
		if err != nil {
			return info, err
		}
		resp, err := pluginHTTP.Do(req)
		if err != nil {
			return info, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return info, fmt.Errorf("node service info HTTP %d", resp.StatusCode)
		}
		return info, json.NewDecoder(resp.Body).Decode(&info)
	}
	var mu sync.Mutex
	connections := map[string]*grpc.ClientConn{}
	co := &preparation.Coordinator{Kube: control.Kube, Ledger: control.Ledger, Drivers: control.Drivers, SigningKey: control.SigningKey, Authorizer: tenantAuth{control.Principal},
		Tenant: control.Principal.Tenant, Namespaces: append([]string{}, control.Principal.Namespaces...), Profiles: cfg.VolumeProfiles}
	co.Choose = func(ctx context.Context, v core.Volume) (core.Placement, error) {
		addresses, err := nodePods(ctx, "")
		if err != nil {
			return core.Placement{}, err
		}
		for _, podIP := range addresses {
			pods, err := control.Kube.API.CoreV1().Pods(cfg.Namespace).List(ctx, metav1.ListOptions{
				LabelSelector: cfg.NodeServiceLabel, FieldSelector: "status.podIP=" + podIP})
			if err != nil || len(pods.Items) == 0 {
				continue
			}
			nodeName := pods.Items[0].Spec.NodeName
			if err := control.Kube.CheckTopology(ctx, v, nodeName); err != nil {
				continue
			}
			node, err := control.Kube.API.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			if err != nil {
				return core.Placement{}, err
			}
			info, err := queryInfo(ctx, podIP)
			if err != nil || !info.Ready || info.BootID == "" {
				continue
			}
			return core.Placement{ID: "preparation/" + v.PVC.UID, Node: node.Name, NodeUID: string(node.UID), BootID: info.BootID, Expires: time.Now().Add(5 * time.Minute)}, nil
		}
		return core.Placement{}, core.ErrPending
	}
	co.Validate = func(ctx context.Context, p core.Placement) error {
		node, err := control.Kube.API.CoreV1().Nodes().Get(ctx, p.Node, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(node.UID) != p.NodeUID || node.DeletionTimestamp != nil {
			return core.ErrConflict
		}
		return nil
	}
	co.Refresh = func(ctx context.Context, p core.Placement) (core.Placement, error) {
		if err := co.Validate(ctx, p); err != nil {
			return p, err
		}
		p.Expires = time.Now().Add(5 * time.Minute)
		return p, nil
	}
	co.Node = func(ctx context.Context, p core.Placement) (preparation.NodeClient, error) {
		addresses, err := nodePods(ctx, p.Node)
		if err != nil {
			return nil, err
		}
		if len(addresses) == 0 {
			return nil, core.ErrPending
		}
		address := net.JoinHostPort(addresses[0], cfg.PreparationNodePort)
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
		return preparationClient{v1.NewRuntimeStorageNodeClient(conn)}, nil
	}
	control.Preparation = co
	return func() {
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
	}, nil
}
