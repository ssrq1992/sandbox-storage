package agentenvadapter

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/config"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
	"ssf.dev/ssf/runtime"
)

type NativeConfig struct {
	VolumeProfiles        []core.VolumeProfile `json:"volume_profiles,omitempty"`
	PreparationNodePort   string               `json:"preparation_node_port,omitempty"`
	PreparationServerName string               `json:"preparation_server_name,omitempty"`
	Standalone            *NativeStandalone    `json:"standalone,omitempty"`
	RequiredProfile       string               `json:"required_profile,omitempty"`
	RecoverySeconds       int64                `json:"recovery_seconds,omitempty"`
	DatabaseURL           string               `json:"database_url"`
	Kubeconfig            string               `json:"kubeconfig"`
	Namespace             string               `json:"namespace"`
	Principal             Principal            `json:"principal"`
	Drivers               []config.Driver      `json:"drivers"`
	SigningKey            string               `json:"signing_key"`
	Cert                  string               `json:"cert"`
	Key                   string               `json:"key"`
	CA                    string               `json:"ca"`
	AgentKeyFile          string               `json:"agent_key_file"`
	GatewayKeyFile        string               `json:"gateway_key_file"`
	DispatchKeyFile       string               `json:"dispatch_key_file"`
	NodeKeyFile           string               `json:"node_key_file"`
	PluginPort            string               `json:"plugin_port"`
	PluginServerName      string               `json:"plugin_server_name,omitempty"`
}

func LoadNativeConfig(path string) (NativeConfig, error) {
	var cfg NativeConfig
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	err = json.Unmarshal(b, &cfg)
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = os.Getenv("SSF_DATABASE_URL")
	}
	if cfg.RequiredProfile == "" {
		cfg.RequiredProfile = runtime.AgentENVCaptureClone
	}
	if cfg.PluginPort == "" {
		cfg.PluginPort = "9444"
	}
	if err == nil && (cfg.Namespace == "" || cfg.Principal.Tenant == "" || len(cfg.Principal.Namespaces) == 0) {
		err = errors.New("native plugin namespace and principal required")
	}
	return cfg, err
}
func OpenNativeControl(ctx context.Context, cfg NativeConfig) (*NativeControl, func(), error) {
	if cfg.RequiredProfile == "" {
		cfg.RequiredProfile = runtime.AgentENVCaptureClone
	}
	db, err := ledger.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, err
	}
	fail := func(e error) (*NativeControl, func(), error) { db.Close(); return nil, nil, e }
	if err = db.Migrate(ctx); err != nil {
		return fail(err)
	}
	if err = MigrateNative(ctx, db); err != nil {
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
	tls, err := auth.TLS(cfg.Cert, cfg.Key, cfg.CA, false)
	if err != nil {
		return fail(err)
	}
	key, err := readKey(cfg.SigningKey, ed25519.PrivateKeySize)
	if err != nil {
		return fail(err)
	}
	rejectRedirect := func(*http.Request, []*http.Request) error { return errors.New("storage redirect forbidden") }
	pluginTLS := tls.Clone()
	pluginTLS.ServerName = cfg.PluginServerName
	client := &HTTPClient{Client: &http.Client{Transport: &http.Transport{TLSClientConfig: pluginTLS}, Timeout: 30 * time.Second, CheckRedirect: rejectRedirect}}
	k := &kube.Client{API: api, Dynamic: dyn, Selection: db}
	control := &NativeControl{Ledger: db, Kube: k, SigningKey: ed25519.PrivateKey(key), Principal: cfg.Principal, HTTP: client, Drivers: &config.Registry{Config: config.Config{Drivers: cfg.Drivers}, API: api, TLS: tls}}
	control.Volumes = &NativeVolumes{Ledger: db, Kube: k}
	control.DefaultNamespace = cfg.Namespace
	apiKey, err := os.ReadFile(cfg.AgentKeyFile)
	if err != nil {
		return fail(err)
	}
	native := &HTTPClient{APIKey: strings.TrimSpace(string(apiKey)), Client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: rejectRedirect}}
	refresh := func(ctx context.Context, c NativeCandidate, admission bool) (NativeCandidate, error) {
		if cfg.Standalone != nil {
			return control.refreshStandalone(ctx, c, admission, cfg, native)
		}
		pod, err := api.CoreV1().Pods(cfg.Namespace).Get(ctx, c.ID, metav1.GetOptions{})
		if err != nil {
			return c, err
		}
		if (admission && pod.DeletionTimestamp != nil) || pod.Spec.NodeName == "" || pod.Status.PodIP == "" || pod.Labels["app.kubernetes.io/name"] != "agentenv-node" {
			return c, errors.New("native node Pod is terminating or unplaced")
		}
		if !admission {
			c.Endpoint = "http://" + net.JoinHostPort(pod.Status.PodIP, "8000")
		}
		endpoint, err := url.Parse(c.Endpoint)
		if err != nil {
			return c, err
		}
		// Native discovery supplies the endpoint. Verify it against current Pod
		// identity instead of trusting an arbitrary URL from a storage request.
		if endpoint.Hostname() != pod.Status.PodIP && !strings.HasPrefix(endpoint.Hostname(), pod.Name+".") {
			return c, errors.New("native endpoint differs from discovered Pod")
		}
		host := endpoint.Hostname()
		c.PluginURL = "https://" + net.JoinHostPort(host, cfg.PluginPort)
		var info NodeInfo
		if err = client.Call(ctx, "GET", c.PluginURL+"/v1/info", nil, &info); err != nil {
			return c, err
		}
		node, err := api.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			return c, err
		}
		if info.Name != node.Name || info.UID != string(node.UID) || info.BootID == "" || (admission && (!info.Ready || node.Spec.Unschedulable || node.DeletionTimestamp != nil)) {
			return c, errors.New("native/CSI node identity not ready")
		}
		if admission {
			if info.Plugin == nil {
				return c, errors.New("native plugin identity missing")
			}
			if err := info.Plugin.Validate(cfg.RequiredProfile); err != nil {
				return c, err
			}
			if len(cfg.VolumeProfiles) > 0 && info.Plugin.VolumePreparation != core.PreparationProtocol {
				return c, errors.New("native node lacks volume preparation protocol")
			}
		}
		c.KubeNode = node.Name
		c.NodeUID = string(node.UID)
		c.BootID = info.BootID
		c.Defaults = info.Defaults
		if admission {
			var projection NativeCapacity
			if err = native.Call(ctx, "GET", base(c.Endpoint)+"/internal/storage/capacity", nil, &projection); err != nil {
				return c, err
			}
			c, err = applyNativeCapacity(c, projection)
			if err != nil {
				return c, err
			}
		}
		return c, nil
	}
	control.Refresh = func(ctx context.Context, c NativeCandidate) (NativeCandidate, error) { return refresh(ctx, c, true) }
	control.RefreshStopped = func(ctx context.Context, c NativeCandidate) (NativeCandidate, error) { return refresh(ctx, c, false) }
	control.RecoverStopped = func(ctx context.Context, previous NativeCandidate) (NativeCandidate, error) {
		if cfg.Standalone != nil {
			return control.stopCandidate(ctx, previous, cfg.Standalone.NodeID)
		}
		pods, err := api.CoreV1().Pods(cfg.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/name=agentenv-node",
			FieldSelector: "spec.nodeName=" + previous.KubeNode,
		})
		if err != nil {
			return NativeCandidate{}, err
		}
		for _, pod := range pods.Items {
			candidate, err := control.stopCandidate(ctx, previous, pod.Name)
			if err == nil {
				return candidate, nil
			}
		}
		return NativeCandidate{}, errors.New("no same-host stop verifier available")
	}
	control.ProjectTemplate = func(ctx context.Context, reference string, candidates []NativeCandidate) (NativeTemplate, error) {
		if reference == "" || len(candidates) == 0 {
			return NativeTemplate{}, errors.New("template reference and a live native node required")
		}
		var projection NativeTemplate
		err := native.Call(ctx, "GET", base(candidates[0].Endpoint)+"/internal/storage/templates/"+url.PathEscape(reference), nil, &projection)
		if err != nil {
			return projection, err
		}
		if projection.ID == "" || projection.Resources.CPU <= 0 || projection.Resources.MemoryMiB <= 0 {
			return projection, errors.New("invalid native template projection")
		}
		return projection, nil
	}
	closePreparation, err := control.openPreparation(cfg, tls)
	if err != nil {
		return fail(err)
	}
	return control, func() { closePreparation(); db.Close() }, nil
}
