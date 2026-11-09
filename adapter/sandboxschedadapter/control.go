package sandboxschedadapter

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"ssf.dev/control"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
	"ssf.dev/ssf/preparation"
	"ssf.dev/ssf/runtime"
)

// SandboxControl hosts the shared state of the coordinator-side plugin.
type SandboxControl struct {
	Ledger      *ledger.Store
	Kube        *kube.Client
	SigningKey  ed25519.PrivateKey
	Principal   Principal
	Drivers     consume.Controllers
	Config      ControlConfig
	Preparation *preparation.Coordinator
	PluginHTTP  *http.Client
}

// ControlService implements the coordinator runtime plugin contract.
type ControlService struct {
	Control *SandboxControl
	Config  ControlConfig
}

// Call dispatches a control method from the node service.
func (s *ControlService) Call(ctx context.Context, method string, req control.Request) (control.Response, error) {
	var out control.Response
	switch method {
	case "LocalLaunch":
		var q BindRequest
		if e := json.Unmarshal(req.Envelope, &q); e != nil {
			return out, e
		}
		envelope, e := s.bind(ctx, q)
		if e != nil {
			return out, e
		}
		out.Envelope = envelope
		return out, nil
	case "InvokeLifecycle":
		var head struct {
			Action string `json:"action"`
		}
		if e := json.Unmarshal(req.Envelope, &head); e != nil {
			return out, e
		}
		switch head.Action {
		case "ready":
			var signal ReadyRequest
			if e := json.Unmarshal(req.Envelope, &signal); e != nil {
				return out, e
			}
			return out, s.ready(ctx, signal)
		case "release":
			var signal ReleaseRequest
			if e := json.Unmarshal(req.Envelope, &signal); e != nil {
				return out, e
			}
			return out, s.release(ctx, signal)
		}
		return out, errors.New("unsupported lifecycle action")
	}
	return out, errors.New("unsupported control method")
}

// requestPlacement pins launch placement to the sandbox's own node.
type requestPlacement struct {
	control *ControlService
	node    string
	bootID  string
}

func (p *requestPlacement) Reserve(ctx context.Context, id string, _ []core.Volume) (core.Placement, error) {
	node, err := p.control.Control.Kube.API.CoreV1().Nodes().Get(ctx, p.node, metav1.GetOptions{})
	if err != nil {
		return core.Placement{}, err
	}
	if node.Spec.Unschedulable || node.DeletionTimestamp != nil {
		return core.Placement{}, errors.New("pinned node is not schedulable")
	}
	if p.bootID == "" {
		return core.Placement{}, errors.New("node boot identity required")
	}
	return core.Placement{ID: "sandboxsched/" + id, Node: node.Name, NodeUID: string(node.UID), BootID: p.bootID, Expires: time.Now().Add(5 * time.Minute)}, nil
}

func (p *requestPlacement) Validate(ctx context.Context, placement core.Placement) error {
	node, err := p.control.Control.Kube.API.CoreV1().Nodes().Get(ctx, placement.Node, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(node.UID) != placement.NodeUID || node.DeletionTimestamp != nil {
		return core.ErrConflict
	}
	return nil
}

func (p *requestPlacement) Renew(ctx context.Context, placement core.Placement) (core.Placement, error) {
	if err := p.Validate(ctx, placement); err != nil {
		return placement, err
	}
	placement.Expires = time.Now().Add(5 * time.Minute)
	return placement, nil
}

func (p *requestPlacement) Commit(ctx context.Context, placement core.Placement, _ string) error {
	return p.Validate(ctx, placement)
}

func (p *requestPlacement) Release(context.Context, core.Placement) error { return nil }

// coordinator builds a consume coordinator for one control call.
func (s *ControlService) coordinator(q BindRequest) *consume.Coordinator {
	co := &consume.Coordinator{
		Ledger:     s.Control.Ledger,
		Kube:       s.Control.Kube,
		Drivers:    s.Control.Drivers,
		Placement:  &requestPlacement{control: s, node: q.Node, bootID: q.BootID},
		SigningKey: s.Control.SigningKey,
		Authorizer: tenantAuth{s.Control.Principal},
		PlanTTL:    5 * time.Minute,
	}
	if s.Control.Preparation != nil {
		co.EnsureVolumeReady = s.Control.Preparation.Ensure
	}
	if endpoint, err := s.nodeServiceURL(q.Node); err == nil {
		co.Evidence = &runtime.RemoteEvidence{URL: endpoint + "/v1/evidence", Client: s.Control.PluginHTTP}
	}
	return co
}

type tenantAuth struct{ principal Principal }

func (a tenantAuth) Authorize(_ context.Context, tenant string, ref core.PVCRef, _ bool) error {
	if tenant != a.principal.Tenant || !a.principal.Allows(ref.Namespace) {
		return errors.New("PVC not authorized")
	}
	return nil
}

// nodeServiceURL discovers the node service pod on a node.
func (s *ControlService) nodeServiceURL(node string) (string, error) {
	pods, err := s.Control.Kube.API.CoreV1().Pods(s.Config.Namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: s.Config.NodeServiceLabel,
		FieldSelector: "spec.nodeName=" + node,
	})
	if err != nil {
		return "", err
	}
	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" && pod.DeletionTimestamp == nil {
			return "https://" + net.JoinHostPort(pod.Status.PodIP, s.Config.NodeServicePort), nil
		}
	}
	return "", errors.New("sandboxsched node service pod not found")
}

func dispatchOf(result consume.Result) Dispatch {
	d := Dispatch{Operation: result.Operation, Plan: result.SignedPlan, RunUID: result.Plan.RunUID, Placement: result.Placement, PlanJSON: result.Plan}
	for _, b := range result.Plan.Bindings {
		d.Bindings = append(d.Bindings, DispatchBinding{BindingID: b.ID, GuestPath: b.GuestPath, ReadOnly: b.Volume.ReadOnly})
	}
	return d
}

// newLaunchID mints the per-incarnation storage subject. The engine, the
// CSI driver and the coordinator all verify one shared proof string, so the
// binding RunUID must equal the plan sandbox; both differ from the business
// sandbox ID so a re-created sandbox can never collide with the released
// operation of its predecessor.
func newLaunchID() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "sx-" + hex.EncodeToString(b), nil
}

// nfsSourceOf extracts the NFS export reference of a PV, if it is one.
// Both the in-tree nfs source and the nfs.csi.k8s.io CSI source resolve to
// "server:/path".
func nfsSourceOf(pv *corev1.PersistentVolume) (string, bool) {
	if pv == nil {
		return "", false
	}
	if pv.Spec.NFS != nil {
		server, path := pv.Spec.NFS.Server, pv.Spec.NFS.Path
		if server != "" && strings.HasPrefix(path, "/") {
			return server + ":" + path, true
		}
		return "", false
	}
	if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == "nfs.csi.k8s.io" {
		server, share := pv.Spec.CSI.VolumeAttributes["server"], pv.Spec.CSI.VolumeAttributes["share"]
		if server != "" && strings.HasPrefix(share, "/") {
			return server + ":" + share, true
		}
	}
	return "", false
}

// classifyMounts resolves every requested claim and decides whether the bind
// is a direct-mount NFS bind or an engine-backed launch. Only homogeneous
// sets are supported; engine-backed claims keep their existing behavior and
// are rejected by consume exactly as before.
func (s *ControlService) classifyMounts(ctx context.Context, q BindRequest) (string, []string, error) {
	sources := make([]string, len(q.Mounts))
	nfs := 0
	for i, m := range q.Mounts {
		pvc, err := s.Control.Kube.API.CoreV1().PersistentVolumeClaims(m.Namespace).Get(ctx, m.Claim, metav1.GetOptions{})
		if err != nil {
			return "", nil, err
		}
		if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
			continue // unbound: the engine path reports it as before
		}
		pv, err := s.Control.Kube.API.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return "", nil, err
		}
		if source, ok := nfsSourceOf(pv); ok {
			sources[i] = source
			nfs++
		}
	}
	if nfs > 0 && nfs < len(q.Mounts) {
		return "", nil, errors.New("mixed nfs and engine-backed mounts are not supported")
	}
	if nfs == len(q.Mounts) {
		return "nfs", sources, nil
	}
	return "", sources, nil
}

// bind plans a launch and persists the durable dispatch.
func (s *ControlService) bind(ctx context.Context, q BindRequest) (json.RawMessage, error) {
	if !safeID.MatchString(q.Sandbox) || q.Node == "" || len(q.Mounts) == 0 {
		return nil, errors.New("invalid bind request")
	}
	seen := map[string]bool{}
	for _, m := range q.Mounts {
		if m.Namespace == "" || m.Claim == "" || !safeID.MatchString(m.Claim) || !s.Control.Principal.Allows(m.Namespace) {
			return nil, errors.New("invalid mount claim")
		}
		if m.GuestPath == "" || m.GuestPath[0] != '/' || strings.Contains(m.GuestPath, "..") || m.GuestPath == "/" || seen[m.GuestPath] {
			return nil, errors.New("invalid mount guest path")
		}
		seen[m.GuestPath] = true
	}
	kind, sources, err := s.classifyMounts(ctx, q)
	if err != nil {
		return nil, err
	}
	digest := core.Digest(q)
	var dispatch Dispatch
	err = s.Control.Ledger.Exclusive(ctx, "sandboxsched/run/"+q.Sandbox, func(ctx context.Context) error {
		var state, savedDigest, saved string
		err := s.Control.Ledger.Session(ctx).QueryRow(ctx,
			`SELECT state, digest, dispatch FROM sandboxsched_runs WHERE id=$1`, q.Sandbox).Scan(&state, &savedDigest, &saved)
		if err == nil {
			if savedDigest != digest {
				return core.ErrConflict
			}
			if e := json.Unmarshal([]byte(saved), &dispatch); e != nil {
				return e
			}
			// The digest covers the claim names but not the volume class:
			// a recreated claim of the other class under the same name must
			// not silently reuse the persisted dispatch of the old class.
			if dispatch.Kind != kind {
				return core.ErrConflict
			}
			if state != "Released" {
				return nil
			}
			// The sandbox was re-created after a full release: retire the
			// finished run row so a fresh launch can be planned under the
			// same business ID.
			if _, err = s.Control.Ledger.Session(ctx).Exec(ctx,
				`DELETE FROM sandboxsched_runs WHERE id=$1 AND state='Released'`, q.Sandbox); err != nil {
				return err
			}
		}
		launch, err := newLaunchID()
		if err != nil {
			return err
		}
		if kind == "nfs" {
			// Direct-mount bind: the control plugin plans the launch locally.
			// No consume operation or engine session exists for NFS exports,
			// so the dispatch carries each resolved export source instead.
			dispatch = Dispatch{Kind: "nfs", Operation: launch, RunUID: launch}
			for i, m := range q.Mounts {
				binding, err := newTicket()
				if err != nil {
					return err
				}
				dispatch.Bindings = append(dispatch.Bindings, DispatchBinding{
					BindingID: binding, GuestPath: m.GuestPath, ReadOnly: m.ReadOnly, Kind: "nfs", Source: sources[i]})
			}
		} else {
			co := s.coordinator(q)
			mounts := make([]core.Mount, 0, len(q.Mounts))
			for i, m := range q.Mounts {
				mounts = append(mounts, core.Mount{PVC: core.PVCRef{Namespace: m.Namespace, Name: m.Claim}, MountID: fmt.Sprintf("m%d", i), GuestPath: m.GuestPath, ReadOnly: m.ReadOnly})
			}
			result, err := co.Prepare(ctx, consume.Request{ID: launch, Tenant: s.Control.Principal.Tenant, Sandbox: launch, OwnerUID: launch, RunUID: launch, Mounts: mounts})
			if err != nil {
				return err
			}
			dispatch = dispatchOf(result)
		}
		raw, err := json.Marshal(dispatch)
		if err != nil {
			return err
		}
		_, err = s.Control.Ledger.Session(ctx).Exec(ctx,
			`INSERT INTO sandboxsched_runs(id,digest,tenant,owner_uid,run_uid,state,dispatch) VALUES($1,$2,$3,$4,$5,'Planned',$6)`,
			q.Sandbox, digest, s.Control.Principal.Tenant, q.Sandbox, launch, string(raw))
		return err
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(dispatch)
}

// storedDispatch is a persisted dispatch with its ledger state.
type storedDispatch struct {
	Dispatch
	State string
}

// loadDispatch reads a persisted dispatch.
func (s *ControlService) loadDispatch(ctx context.Context, sandbox string) (storedDispatch, error) {
	var state, raw string
	err := s.Control.Ledger.Session(ctx).QueryRow(ctx,
		`SELECT state, dispatch FROM sandboxsched_runs WHERE id=$1`, sandbox).Scan(&state, &raw)
	if err != nil {
		return storedDispatch{}, err
	}
	var d Dispatch
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		return storedDispatch{}, err
	}
	return storedDispatch{Dispatch: d, State: state}, nil
}

// ready confirms the running sandbox and commits the launch.
func (s *ControlService) ready(ctx context.Context, signal ReadyRequest) error {
	d, err := s.loadDispatch(ctx, signal.Sandbox)
	if err != nil {
		return err
	}
	if d.State != "Planned" && d.State != "Ready" {
		return core.ErrConflict
	}
	if signal.Instance == "" || signal.Proof == "" {
		return errors.New("ready requires instance and proof")
	}
	if signal.Instance != d.RunUID {
		return core.ErrConflict
	}
	if d.Kind == "nfs" {
		// Direct-mount bind: the node service verified the kernel evidence;
		// there is no engine operation to confirm or commit.
		_, err = s.Control.Ledger.Session(ctx).Exec(ctx,
			`UPDATE sandboxsched_runs SET state='Ready', updated=now() WHERE id=$1 AND state IN ('Planned','Ready')`, signal.Sandbox)
		return err
	}
	co := s.coordinator(BindRequest{Node: d.Placement.Node})
	result := consume.Result{Operation: d.Operation, Placement: d.Placement, Plan: d.PlanJSON, SignedPlan: d.Plan}
	if err = co.ConfirmReady(ctx, result, signal.Instance, signal.Proof); err != nil {
		return err
	}
	if err = co.Commit(ctx, result); err != nil {
		return err
	}
	_, err = s.Control.Ledger.Session(ctx).Exec(ctx,
		`UPDATE sandboxsched_runs SET state='Ready', updated=now() WHERE id=$1 AND state IN ('Planned','Ready')`, signal.Sandbox)
	return err
}

// release confirms the stopped sandbox and releases ledger ownership.
func (s *ControlService) release(ctx context.Context, signal ReleaseRequest) error {
	d, err := s.loadDispatch(ctx, signal.Sandbox)
	if err != nil {
		return err
	}
	if d.State == "Released" {
		return nil
	}
	if d.State != "Planned" && d.State != "Ready" {
		return core.ErrConflict
	}
	if signal.Proof == "" {
		return errors.New("release requires proof")
	}
	if d.Kind == "nfs" {
		// Direct-mount bind: the node service already unmounted the exports;
		// only the business run row remains to retire.
		_, err = s.Control.Ledger.Session(ctx).Exec(ctx,
			`UPDATE sandboxsched_runs SET state='Released', updated=now() WHERE id=$1 AND state IN ('Planned','Ready')`, signal.Sandbox)
		return err
	}
	co := s.coordinator(BindRequest{Node: d.Placement.Node})
	result := consume.Result{Operation: d.Operation, Placement: d.Placement, Plan: d.PlanJSON, SignedPlan: d.Plan}
	if err = co.ConfirmStopped(ctx, result, signal.Proof); err != nil {
		return err
	}
	if err = co.Release(ctx, d.Operation, func(ctx context.Context, _ string) error {
		endpoint, e := s.nodeServiceURL(d.Placement.Node)
		if e != nil {
			return e
		}
		body, e := json.Marshal(map[string]string{"operation": d.Operation})
		if e != nil {
			return e
		}
		req, e := http.NewRequestWithContext(ctx, "POST", endpoint+"/v1/node-released", strings.NewReader(string(body)))
		if e != nil {
			return e
		}
		req.Header.Set("Content-Type", "application/json")
		resp, e := s.Control.PluginHTTP.Do(req)
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		if resp.StatusCode != 204 {
			return errors.New("node release not confirmed")
		}
		return nil
	}); err != nil {
		return err
	}
	_, err = s.Control.Ledger.Session(ctx).Exec(ctx,
		`UPDATE sandboxsched_runs SET state='Released', updated=now() WHERE id=$1 AND state IN ('Planned','Ready')`, signal.Sandbox)
	return err
}

// readKey loads a base64 signing key.
func readKey(file string, size int) ([]byte, error) {
	b, e := os.ReadFile(file)
	if e != nil {
		return nil, e
	}
	v, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if e != nil {
		return nil, e
	}
	if len(v) != size {
		return nil, errors.New("invalid signing key size")
	}
	return v, nil
}
