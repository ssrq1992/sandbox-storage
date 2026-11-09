package sandboxschedadapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	control "ssf.dev/control"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/config"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
)

// Node is the SandboxSched node-local storage service.
type Node struct {
	Config           NodeConfig
	NodeUID          string
	BootID           string
	Journal          *Journal
	SSF              v1.RuntimeStorageNodeClient
	Control          *control.Client
	Call             func(ctx context.Context, method string, envelope []byte) (json.RawMessage, error)
	APIKey           string
	contentMu        sync.Mutex
	mu               sync.Mutex
}

// Run starts the node service until the context is cancelled.
func Run(ctx context.Context, c NodeConfig) error {
	serverTLS, err := authTLSVariant(c.Cert, c.Key, c.CA)
	if err != nil {
		return err
	}
	// The agent-facing API authenticates with an API key, while the SSF node
	// stack verifies itself with mTLS: accept both on one listener.
	pool := x509.NewCertPool()
	if raw, e := os.ReadFile(c.CA); e == nil {
		pool.AppendCertsFromPEM(raw)
	}
	serverTLS = serverTLS.Clone()
	serverTLS.ClientAuth = tls.VerifyClientCertIfGiven
	serverTLS.ClientCAs = pool
	api, err := kubernetesClient(c.Kubeconfig)
	if err != nil {
		return err
	}
	node, err := api.CoreV1().Nodes().Get(ctx, c.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return err
	}
	key, err := os.ReadFile(c.APIKeyFile)
	if err != nil {
		return err
	}
	j, err := OpenJournal(filepath.Join(c.Root, "journal"))
	if err != nil {
		return err
	}
	defer j.Close()
	clientTLS, err := authTLSVariant(c.Cert, c.Key, c.CA)
	if err != nil {
		return err
	}
	grpcTLS := clientTLS.Clone()
	grpcTLS.ServerName = c.SSFServerName
	conn, err := grpc.NewClient(c.SSFEndpoint, grpc.WithTransportCredentials(credentials.NewTLS(grpcTLS)))
	if err != nil {
		return err
	}
	defer conn.Close()
	controlClient, err := control.New(control.Config{Endpoint: c.Coordinator.Endpoint, Cert: c.Coordinator.Cert, Key: c.Coordinator.Key, CA: c.Coordinator.CA, Peer: c.Coordinator.Peer, ServerName: c.Coordinator.ServerName})
	if err != nil {
		return err
	}
	n := &Node{
		Config:           c,
		NodeUID:          string(node.UID),
		BootID:           strings.TrimSpace(string(boot)),
		Journal:          j,
		SSF:              v1.NewRuntimeStorageNodeClient(conn),
		Control:          controlClient,
		APIKey:           strings.TrimSpace(string(key)),
	}
	n.Call = func(ctx context.Context, method string, envelope []byte) (json.RawMessage, error) {
		reply, err := n.Control.Call(ctx, method, control.Request{Runtime: n.Config.PreparationRuntime, Envelope: envelope})
		if err != nil {
			return nil, err
		}
		return reply.Envelope, nil
	}
	go n.recoverLoop(ctx)
	handler := &http.Server{Addr: c.Listen, Handler: n, TLSConfig: serverTLS, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	hook := &http.Server{Addr: c.HookListen, Handler: http.HandlerFunc(n.preparationHook), ReadHeaderTimeout: 10 * time.Second}
	defer hook.Close()
	go func() {
		if err := hook.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "hook listener:", err)
		}
	}()
	health := &http.Server{Addr: c.HealthListen, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })}
	defer health.Close()
	go func() {
		if err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "health listener:", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = handler.Shutdown(shutdown)
		_ = hook.Shutdown(shutdown)
	}()
	err = handler.ListenAndServeTLS("", "")
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func rejectRedirect(*http.Request, []*http.Request) error { return errors.New("redirect forbidden") }

// authTLSVariant loads a TLS client configuration.
func authTLSVariant(cert, key, ca string) (*tls.Config, error) {
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, errors.New("invalid CA")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, RootCAs: pool}, nil
}

func kubernetesClient(kubeconfig string) (*kubernetes.Clientset, error) {
	rest, err := config.Kube(kubeconfig)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(rest)
}

func problem(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func respondJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	defer r.Body.Close()
	d := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := d.Decode(v); err != nil {
		return err
	}
	return nil
}

// ServeHTTP routes the node service API.
func (n *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/bind", "/v1/ready", "/v1/release":
		if r.Method != "POST" {
			problem(w, 405, errors.New("POST required"))
			return
		}
		if !n.authorizeAPIKey(r) {
			problem(w, 403, errors.New("sandboxsched API key required"))
			return
		}
		n.sandboxAPI(w, r)
	case "/v1/info":
		if !n.authorizeAPIKey(r) && !n.authorizeEvidencePeer(r) {
			problem(w, 403, errors.New("sandboxsched API key or control peer required"))
			return
		}
		n.sandboxAPI(w, r)
	case "/v1/evidence":
		if r.Method != "POST" {
			problem(w, 405, errors.New("POST required"))
			return
		}
		if !n.authorizeEvidencePeer(r) {
			problem(w, 403, errors.New("evidence peer forbidden"))
			return
		}
		n.evidence(w, r)
	case "/v1/node-released":
		if !n.authorizeEvidencePeer(r) {
			problem(w, 403, errors.New("control peer forbidden"))
			return
		}
		n.nodeReleased(w, r)
	default:
		problem(w, 404, errors.New("not found"))
	}
}

func (n *Node) authorizeAPIKey(r *http.Request) bool {
	return subtleEqual(r.Header.Get("X-API-Key"), n.APIKey)
}

func subtleEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := 0; i < len(a); i++ {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

func hasPeer(state *tls.ConnectionState, peers []string) bool {
	if state == nil || len(state.VerifiedChains) == 0 {
		return false
	}
	for _, u := range state.VerifiedChains[0][0].URIs {
		for _, p := range peers {
			if p != "" && u.String() == p {
				return true
			}
		}
	}
	return false
}

func (n *Node) authorizeEvidencePeer(r *http.Request) bool {
	return hasPeer(r.TLS, n.Config.EvidencePeers)
}

func (n *Node) sandboxAPI(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/bind":
		n.bind(w, r)
	case "/v1/ready":
		n.ready(w, r)
	case "/v1/release":
		n.release(w, r)
	case "/v1/info":
		respondJSON(w, 200, map[string]any{"name": n.Config.NodeName, "uid": n.NodeUID, "boot_id": n.BootID, "ready": true, "runtime": n.Config.PreparationRuntime})
	}
}

// bindResponse is returned to the sandbox agent.
type bindResponse struct {
	Sandbox   string `json:"sandbox"`
	Operation string `json:"operation"`
	State     string `json:"state"`
	Mounts    []struct {
		GuestPath string `json:"guest_path"`
		HostPath  string `json:"host_path"`
		ReadOnly  bool   `json:"read_only"`
	} `json:"mounts"`
}

func (n *Node) bind(w http.ResponseWriter, r *http.Request) {
	var q BindRequest
	if err := decodeBody(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	if !safeID.MatchString(q.Sandbox) || len(q.Mounts) == 0 {
		problem(w, 400, errors.New("invalid bind request"))
		return
	}
	// The service is node-local: an omitted node or boot identity defaults
	// to this node's own.
	if q.Node == "" {
		q.Node = n.Config.NodeName
	}
	if q.Node != n.Config.NodeName {
		problem(w, 400, errors.New("bind request targets another node"))
		return
	}
	if q.BootID == "" {
		q.BootID = n.BootID
	}
	if q.BootID != n.BootID {
		problem(w, 409, errors.New("node boot changed"))
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if existing, err := n.Journal.Bind(q.Sandbox); err == nil {
		if existing.State == "Released" || existing.State == "Stopped" {
			// The sandbox was re-created after a full release: plan a fresh
			// bind (the journal record is overwritten by the new launch).
			out, err := n.bindNew(r.Context(), q)
			if err != nil {
				problem(w, 503, err)
				return
			}
			respondJSON(w, 200, out)
			return
		}
		out, err := n.reconcileExisting(existing, q)
		if err == nil {
			respondJSON(w, 200, out)
			return
		}
		problem(w, 409, err)
		return
	} else if !errors.Is(err, ErrNotFound) {
		problem(w, 500, err)
		return
	}
	out, err := n.bindNew(r.Context(), q)
	if err != nil {
		problem(w, 503, err)
		return
	}
	respondJSON(w, 200, out)
}

// reconcileExisting re-establishes or returns an existing bind.
func (n *Node) reconcileExisting(record BindRecord, q BindRequest) (bindResponse, error) {
	var out bindResponse
	switch record.State {
	case "Released", "Stopped":
		return out, core.ErrConflict
	case "Prepared", "Ready":
		if len(record.Mounts) != len(q.Mounts) {
			return out, core.ErrConflict
		}
		for i, m := range record.Mounts {
			if q.Mounts[i].GuestPath != m.GuestPath || q.Mounts[i].ReadOnly != m.ReadOnly {
				return out, core.ErrConflict
			}
		}
	default:
		return out, core.ErrConflict
	}
	// Re-mount if this service recovered without its mounts.
	for _, m := range record.Mounts {
		if _, ok := selfMountEntry(m.HostPath); ok {
			continue
		}
		if m.Kind == "nfs" {
			if err := mountNFS(m.Source, m.HostPath, m.ReadOnly); err != nil {
				return out, err
			}
			continue
		}
		if err := mountFilesystem(m.Device, m.HostPath, m.ReadOnly); err != nil {
			return out, err
		}
	}
	out.Sandbox = record.Sandbox
	out.Operation = record.Operation
	out.State = record.State
	for _, m := range record.Mounts {
		out.Mounts = append(out.Mounts, struct {
			GuestPath string `json:"guest_path"`
			HostPath  string `json:"host_path"`
			ReadOnly  bool   `json:"read_only"`
		}{GuestPath: m.GuestPath, HostPath: m.HostPath, ReadOnly: m.ReadOnly})
	}
	return out, nil
}

// bindNew performs the full bind flow for a new sandbox.
func (n *Node) bindNew(ctx context.Context, q BindRequest) (bindResponse, error) {
	var out bindResponse
	envelope, err := json.Marshal(q)
	if err != nil {
		return out, err
	}
	raw, err := n.Call(ctx, "LocalLaunch", envelope)
	if err != nil {
		return out, err
	}
	var dispatch Dispatch
	if err = json.Unmarshal(raw, &dispatch); err != nil {
		return out, err
	}
	ticket, err := newTicket()
	if err != nil {
		return out, err
	}
	record := BindRecord{Sandbox: q.Sandbox, Ticket: ticket, BootID: n.BootID, Operation: dispatch.Operation, Plan: dispatch.Plan, RunUID: dispatch.RunUID, Kind: dispatch.Kind, State: "Prepared", CreatedAt: time.Now().UTC()}
	if dispatch.Kind == "nfs" {
		if err = n.mountNFSBindings(&record, dispatch.Bindings); err != nil {
			return out, err
		}
	} else if err = n.publishBlockBindings(ctx, &record, dispatch); err != nil {
		return out, err
	}
	if err = n.Journal.PutBind(record); err != nil {
		return out, err
	}
	out.Sandbox = record.Sandbox
	out.Operation = record.Operation
	out.State = record.State
	for _, m := range record.Mounts {
		out.Mounts = append(out.Mounts, struct {
			GuestPath string `json:"guest_path"`
			HostPath  string `json:"host_path"`
			ReadOnly  bool   `json:"read_only"`
		}{GuestPath: m.GuestPath, HostPath: m.HostPath, ReadOnly: m.ReadOnly})
	}
	return out, nil
}

// mountNFSBindings mounts every direct NFS export of a dispatch. The control
// plugin already resolved each binding to an NFS source; the node service
// resolves the server address, mounts by IP and records the resolved source
// so kernel evidence and recovery remounts stay consistent.
func (n *Node) mountNFSBindings(record *BindRecord, bindings []DispatchBinding) error {
	for _, b := range bindings {
		if b.Kind != "nfs" || b.Source == "" {
			return errors.New("invalid nfs dispatch binding")
		}
		resolved, err := nfsResolveSource(b.Source)
		if err != nil {
			return err
		}
		host := filepath.Join(n.Config.Root, "mounts", b.BindingID)
		if err = mountNFS(resolved, host, b.ReadOnly); err != nil {
			return err
		}
		record.Mounts = append(record.Mounts, BindMount{BindingID: b.BindingID, GuestPath: b.GuestPath, HostPath: host, Kind: "nfs", Source: resolved, ReadOnly: b.ReadOnly})
	}
	return nil
}

// publishBlockBindings runs the engine node publish for a dispatch and mounts
// the ext4 filesystem of every published block device.
func (n *Node) publishBlockBindings(ctx context.Context, record *BindRecord, dispatch Dispatch) error {
	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	endpoints, err := n.SSF.PrepareVolumes(publishCtx, &v1.PrepareRequest{RequestId: dispatch.Operation, SignedNodePlan: dispatch.Plan})
	if err != nil {
		return fmt.Errorf("node publish failed: %w", err)
	}
	if len(endpoints.Endpoints) != len(dispatch.Bindings) {
		return errors.New("endpoint count differs from bindings")
	}
	byGuest := map[string]DispatchBinding{}
	for _, b := range dispatch.Bindings {
		byGuest[b.GuestPath] = b
	}
	for _, ep := range endpoints.Endpoints {
		block := ep.GetBlockDevice()
		if block == nil {
			return errors.New("only block device endpoints are supported")
		}
		binding, ok := byGuest[ep.GuestPath]
		if !ok {
			return errors.New("endpoint guest path mismatch")
		}
		if err = verifyDevice(block.ManagedTarget, block.Major, block.Minor, n.Config.DeviceRoot); err != nil {
			return err
		}
		host := filepath.Join(n.Config.Root, "mounts", binding.BindingID)
		if err = mountFilesystem(block.ManagedTarget, host, binding.ReadOnly); err != nil {
			return err
		}
		dev, err := statDevice(host)
		if err != nil {
			return err
		}
		record.Mounts = append(record.Mounts, BindMount{BindingID: binding.BindingID, GuestPath: binding.GuestPath, HostPath: host, Device: block.ManagedTarget, Dev: dev, ReadOnly: binding.ReadOnly})
	}
	return nil
}

// proof computes the evidence proof for a subject and action.
func (n *Node) proof(record BindRecord, subject, action string) string {
	return core.Hash(record.Ticket + "\x00" + subject + "\x00" + record.BootID + "\x00" + action)
}

// ready confirms a running sandbox with its mounts.
func (n *Node) ready(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Sandbox  string `json:"sandbox"`
		Instance string `json:"instance"`
		PID      int    `json:"pid"`
	}
	if err := decodeBody(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	record, err := n.Journal.Bind(q.Sandbox)
	if err != nil {
		problem(w, 404, err)
		return
	}
	if record.State != "Prepared" && record.State != "Ready" {
		problem(w, 409, errors.New("bind not active"))
		return
	}
	if !pidAlive(q.PID) {
		problem(w, 409, errors.New("sandbox process is not running"))
		return
	}
	for _, m := range record.Mounts {
		if m.Kind == "nfs" {
			if !pidHasNFSMount(q.PID, m.Source, m.GuestPath) {
				problem(w, 409, fmt.Errorf("sandbox lacks mount at %s", m.GuestPath))
				return
			}
			continue
		}
		if !pidHasMount(q.PID, m.Dev, m.GuestPath) {
			problem(w, 409, fmt.Errorf("sandbox lacks mount at %s", m.GuestPath))
			return
		}
	}
	if _, err = n.Journal.UpdateBind(q.Sandbox, func(x *BindRecord) error {
		x.Instance, x.PID = q.Instance, q.PID
		return nil
	}); err != nil {
		problem(w, 500, err)
		return
	}
	// The storage subject is the per-incarnation launch ID: the engine
	// requires SandboxInstanceUid == Plan.Sandbox and the driver verifies
	// b.RunUID, both of which carry the launch ID (not the business ID).
	instance := record.RunUID
	if record.Kind != "nfs" {
		if _, err = n.SSF.MarkRuntimeReady(r.Context(), &v1.RuntimeRequest{OperationId: record.Operation, SandboxInstanceUid: instance, Evidence: n.proof(record, instance, "ready")}); err != nil {
			problem(w, 409, fmt.Errorf("runtime ready rejected: %w", err))
			return
		}
	}
	readyCtx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	envelope, err := json.Marshal(ReadyRequest{Action: "ready", Sandbox: q.Sandbox, Instance: instance, PID: q.PID, Proof: n.proof(record, instance, "ready")})
	if err != nil {
		problem(w, 500, err)
		return
	}
	if _, err = n.Call(readyCtx, "InvokeLifecycle", envelope); err != nil {
		problem(w, 409, fmt.Errorf("launch commit rejected: %w", err))
		return
	}
	if _, err = n.Journal.UpdateBind(q.Sandbox, func(x *BindRecord) error { x.State = "Ready"; return nil }); err != nil {
		problem(w, 500, err)
		return
	}
	w.WriteHeader(204)
}

// release stops one sandbox bind and releases its volumes.
func (n *Node) release(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Sandbox string `json:"sandbox"`
	}
	if err := decodeBody(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.releaseLocked(r.Context(), q.Sandbox); err != nil {
		problem(w, 409, err)
		return
	}
	w.WriteHeader(204)
}

func (n *Node) releaseLocked(ctx context.Context, sandbox string) error {
	record, err := n.Journal.Bind(sandbox)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	switch record.State {
	case "Released":
		return nil
	case "Prepared", "Ready", "Detached":
	default:
		return errors.New("bind state does not allow release")
	}
	// The sandbox must be gone: no live process and no namespace still
	// referencing any managed filesystem.
	if record.PID > 0 && pidAlive(record.PID) {
		return errors.New("sandbox process still runs")
	}
	for _, m := range record.Mounts {
		if err := unmountFilesystem(m.HostPath); err != nil {
			return fmt.Errorf("unmount %s: %w", m.HostPath, err)
		}
	}
	subject := record.RunUID
	releaseCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if record.Kind != "nfs" {
		for _, m := range record.Mounts {
			holders, err := devHolders(m.Dev)
			if err != nil {
				return err
			}
			if holders > 0 {
				return fmt.Errorf("filesystem of %s is still referenced by %d processes", m.GuestPath, holders)
			}
		}
		if _, err = n.SSF.ReleaseVolumes(releaseCtx, &v1.RuntimeRequest{OperationId: record.Operation, SandboxInstanceUid: subject, Evidence: n.proof(record, subject, "detached"), SignedNodePlan: record.Plan}); err != nil {
			return fmt.Errorf("node release rejected: %w", err)
		}
		// Detached attests the engine-side release: the control plane's Release
		// callback (/v1/node-released) accepts this state as node confirmation.
		if _, err := n.Journal.UpdateBind(sandbox, func(x *BindRecord) error { x.State = "Detached"; return nil }); err != nil {
			return err
		}
	}
	envelope, err := json.Marshal(ReleaseRequest{Action: "release", Sandbox: sandbox, Proof: n.proof(record, subject, "detached")})
	if err != nil {
		return err
	}
	if _, err = n.Call(releaseCtx, "InvokeLifecycle", envelope); err != nil {
		return fmt.Errorf("ledger release rejected: %w", err)
	}
	_, err = n.Journal.UpdateBind(sandbox, func(x *BindRecord) error { x.State = "Released"; return nil })
	return err
}

// evidence verifies SandboxSched runtime evidence.
func (n *Node) evidence(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Sandbox  string `json:"sandbox"`
		Action   string `json:"action"`
		Evidence string `json:"evidence"`
	}
	if err := decodeBody(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	// No n.mu here: the engine and the control plane call this endpoint
	// from inside bind/ready/release, which hold n.mu across those RPCs.
	// The journal (bbolt) is safe for concurrent readers on its own.
	record, err := n.Journal.Bind(q.Sandbox)
	if err != nil {
		// SSF callers verify with the storage subject (the launch ID), not
		// the business sandbox ID the journal is keyed by.
		if records, listErr := n.Journal.Binds(); listErr == nil {
			for _, rec := range records {
				if rec.RunUID == q.Sandbox || rec.Instance == q.Sandbox {
					record, err = rec, nil
					break
				}
			}
		}
	}
	if err != nil || (record.Sandbox != q.Sandbox && record.Instance != q.Sandbox && record.RunUID != q.Sandbox) {
		// Not a SandboxSched sandbox: no other runtime is hosted by this
		// service, so the subject is unknown.
		problem(w, 404, errors.New("unknown sandbox"))
		return
	}
	if n.proof(record, q.Sandbox, q.Action) != q.Evidence {
		problem(w, 403, errors.New("unknown evidence"))
		return
	}
	if err := n.verifyLive(record, q.Action); err != nil {
		problem(w, 409, err)
		return
	}
	w.WriteHeader(204)
}

// verifyLive re-observes kernel state for the requested transition.
func (n *Node) verifyLive(record BindRecord, action string) error {
	if record.BootID != n.BootID {
		if action == "detached" {
			return nil
		}
		return errors.New("stale node incarnation")
	}
	switch action {
	case "ready":
		if !pidAlive(record.PID) {
			return errors.New("sandbox process is not running")
		}
		for _, m := range record.Mounts {
			verified := m.Kind == "nfs" && pidHasNFSMount(record.PID, m.Source, m.GuestPath)
			if m.Kind != "nfs" {
				verified = pidHasMount(record.PID, m.Dev, m.GuestPath)
			}
			if !verified {
				return fmt.Errorf("sandbox lacks mount at %s", m.GuestPath)
			}
		}
		return nil
	case "detached":
		if pidAlive(record.PID) {
			return errors.New("sandbox process still runs")
		}
		for _, m := range record.Mounts {
			if m.Kind == "nfs" {
				continue // no device identity: unmount success is the evidence
			}
			holders, err := devHolders(m.Dev)
			if err != nil {
				return err
			}
			if holders > 0 {
				return fmt.Errorf("filesystem of %s is still referenced by %d processes", m.GuestPath, holders)
			}
		}
		return nil
	default:
		return errors.New("unsupported evidence action")
	}
}

// nodeReleased answers the control plugin's release fence: the node-side
// operation must already be released through the SSF node agent.
func (n *Node) nodeReleased(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Operation string `json:"operation"`
	}
	if err := decodeBody(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	// No n.mu: the control plane calls this from Release while the node
	// service holds n.mu across the release RPCs. Journal reads are safe.
	records, err := n.Journal.Binds()
	if err != nil {
		problem(w, 500, err)
		return
	}
	for _, record := range records {
		if record.Operation == q.Operation {
			if record.State == "Released" || record.State == "Detached" {
				w.WriteHeader(204)
				return
			}
			problem(w, 409, errors.New("node operation not released"))
			return
		}
	}
	w.WriteHeader(204)
}

// preparationHook serves the SSF node preparation forward. SandboxSched
// volumes are initialized locally; other runtimes are not hosted.
func (n *Node) preparationHook(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/storage/volume-preparation" || r.Method != "POST" {
		problem(w, 404, errors.New("not found"))
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+n.hookToken() {
		problem(w, 403, errors.New("storage hook authentication required"))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	defer r.Body.Close()
	if err != nil {
		problem(w, 400, err)
		return
	}
	var request preparationEnvelope
	if err := json.Unmarshal(body, &request); err != nil {
		problem(w, 400, err)
		return
	}
	if request.Request.Spec.Profile.Runtime == n.Config.PreparationRuntime {
		n.volumePreparationHook(w, request)
		return
	}
	problem(w, 404, errors.New("unsupported preparation runtime"))
}

type preparationEnvelope struct {
	Action  string                     `json:"action"`
	Request runtime.PreparationRequest `json:"request"`
}

func (n *Node) volumePreparationHook(w http.ResponseWriter, q preparationEnvelope) {
	var receipt core.PreparationReceipt
	var err error
	switch q.Action {
	case "prepare":
		receipt, err = n.PrepareVolume(context.Background(), q.Request)
	case "inspect":
		receipt, err = n.InspectVolumePreparation(context.Background(), q.Request)
	case "release":
		receipt, err = n.ReleaseVolumePreparation(context.Background(), q.Request)
	default:
		err = errors.New("unknown volume preparation action")
	}
	if err != nil {
		problem(w, 409, err)
		return
	}
	respondJSON(w, 200, receipt)
}

func (n *Node) hookToken() string {
	raw, err := os.ReadFile(n.Config.HookTokenFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// recoverLoop reconciles journal state with actual sandbox state.
func (n *Node) recoverLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.reconcile(ctx)
		}
	}
}

func (n *Node) reconcile(ctx context.Context) {
	n.mu.Lock()
	defer n.mu.Unlock()
	records, err := n.Journal.Binds()
	if err != nil {
		return
	}
	for _, record := range records {
		switch record.State {
		case "Prepared":
			// A bind that never reached ready: the agent may still be mid
			// create, so only reclaim long-abandoned binds.
			if time.Since(record.UpdatedAt) > 10*time.Minute {
				if err := n.releaseLocked(ctx, record.Sandbox); err != nil {
					fmt.Fprintln(os.Stderr, "sandboxsched recovery release:", record.Sandbox, err)
				}
			}
		case "Ready":
			if record.PID > 0 && !pidAlive(record.PID) {
				if err := n.releaseLocked(ctx, record.Sandbox); err != nil {
					fmt.Fprintln(os.Stderr, "sandboxsched recovery release:", record.Sandbox, err)
				}
			}
		}
	}
}

// Start is the entry point used by the command.
func Start(ctx context.Context, configPath string) error {
	cfg, err := LoadNodeConfig(configPath)
	if err != nil {
		return err
	}
	return Run(ctx, cfg)
}
