package agentenvadapter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/http"
	"net/url"
	"os"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	"strings"
	"time"
)

// Standalone is one local native runtime, not a replacement scheduler or worker
// inventory. Kubernetes remains the PVC/PV and CSI management authority.
type NativeStandalone struct {
	NodeID    string `json:"node_id"`
	KubeNode  string `json:"kube_node"`
	Endpoint  string `json:"endpoint"`
	PluginURL string `json:"plugin_url"`
}

func (s NativeStandalone) Validate() error {
	if !safeID.MatchString(s.NodeID) || s.KubeNode == "" {
		return errors.New("standalone native and Kubernetes node identity required")
	}
	for _, item := range []struct{ raw, scheme string }{{s.Endpoint, "http"}, {s.PluginURL, "https"}} {
		u, err := url.Parse(item.raw)
		if err != nil || u.Scheme != item.scheme || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
			return errors.New("standalone endpoints must be fixed authenticated host loopback")
		}
	}
	return nil
}

func (c *NativeControl) refreshStandalone(ctx context.Context, candidate NativeCandidate, admission bool, cfg NativeConfig, native *HTTPClient) (NativeCandidate, error) {
	fixed := cfg.Standalone
	if fixed == nil || fixed.Validate() != nil || candidate.ID != fixed.NodeID {
		return candidate, core.ErrConflict
	}
	candidate.Endpoint = fixed.Endpoint
	candidate.PluginURL = fixed.PluginURL
	node, err := c.Kube.API.CoreV1().Nodes().Get(ctx, fixed.KubeNode, metav1.GetOptions{})
	if err != nil {
		return candidate, err
	}
	var info NodeInfo
	if err = c.HTTP.Call(ctx, "GET", base(fixed.PluginURL)+"/v1/info", nil, &info); err != nil {
		return candidate, err
	}
	if info.Name != node.Name || info.UID != string(node.UID) || info.BootID == "" {
		return candidate, core.ErrConflict
	}
	if admission {
		if !info.Ready || node.Spec.Unschedulable || node.DeletionTimestamp != nil || info.Plugin == nil {
			return candidate, core.ErrPending
		}
		if err = info.Plugin.Validate(cfg.RequiredProfile); err != nil {
			return candidate, err
		}
		if len(cfg.VolumeProfiles) > 0 && info.Plugin.VolumePreparation != core.PreparationProtocol {
			return candidate, errors.New("standalone node lacks volume preparation protocol")
		}
		var projection NativeCapacity
		if err = native.Call(ctx, "GET", base(fixed.Endpoint)+"/internal/storage/capacity", nil, &projection); err != nil {
			return candidate, err
		}
		candidate, err = applyNativeCapacity(candidate, projection)
		if err != nil {
			return candidate, err
		}
	}
	candidate.KubeNode = node.Name
	candidate.NodeUID = string(node.UID)
	candidate.BootID = info.BootID
	candidate.Defaults = info.Defaults
	return candidate, nil
}

func signLocalDispatch(key []byte, d NativeDispatch) (json.RawMessage, error) {
	signed, err := auth.SignJSONEnvelope(key, NativeStorageProtocol, "scheduler", d.RequestID, d, time.Now())
	if err != nil {
		return nil, err
	}
	return json.Marshal(nativeAdmitRequest{Context: base64.RawURLEncoding.EncodeToString(signed), BodyDigest: d.BodyDigest})
}

func (n *Node) nativeLocalAdmit(w http.ResponseWriter, r *http.Request) {
	if n.LocalPrepare == nil || n.isDraining() {
		problem(w, 409, ErrUnsupported)
		return
	}
	var q struct {
		RequestID string `json:"request_id"`
		Path      string `json:"path"`
		Body      []byte `json:"body"`
	}
	if err := decode(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	if !safeID.MatchString(q.RequestID) || len(q.Body) > 512*1024 || (q.Path != "/sandboxes-cold" && q.Path != "/sandboxes" && q.Path != "/v2/sandboxes") {
		problem(w, 400, core.ErrConflict)
		return
	}
	dispatch, err := n.LocalPrepare(r.Context(), NativeIntent{RequestID: q.RequestID, Path: q.Path, Body: q.Body, BodyDigest: core.Hash(string(q.Body))})
	if err != nil {
		problem(w, 409, err)
		return
	}
	key, err := os.ReadFile(n.Config.DispatchKeyFile)
	if err != nil {
		problem(w, 503, err)
		return
	}
	raw, err := signLocalDispatch([]byte(strings.TrimSpace(string(key))), dispatch)
	if err != nil {
		problem(w, 503, err)
		return
	}
	var admission nativeAdmitRequest
	if err = json.Unmarshal(raw, &admission); err != nil {
		problem(w, 503, err)
		return
	}
	reply, err := n.prepareNative(r.Context(), admission)
	if err != nil {
		problem(w, 409, err)
		return
	}
	respond(w, 200, reply)
}
