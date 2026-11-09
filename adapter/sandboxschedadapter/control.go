package sandboxschedadapter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"ssf.dev/control"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

// SandboxControl hosts the shared state of the coordinator-side plugin.
type SandboxControl struct {
	Ledger    *ledger.Store
	Kube      *kube.Client
	Principal Principal
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

// newLaunchID mints the per-incarnation storage subject. The coordinator and
// the node service verify one shared proof string, so the binding RunUID must
// equal the launch subject; it differs from the business sandbox ID so a
// re-created sandbox can never collide with the released operation of its
// predecessor.
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

// resolveNFSSources resolves every requested claim to its NFS export source.
// Only NFS volumes are supported: any claim that is missing, unbound or not
// backed by an NFS export is rejected here, before a launch is planned.
func (s *ControlService) resolveNFSSources(ctx context.Context, q BindRequest) ([]string, error) {
	sources := make([]string, len(q.Mounts))
	for i, m := range q.Mounts {
		pvc, err := s.Control.Kube.API.CoreV1().PersistentVolumeClaims(m.Namespace).Get(ctx, m.Claim, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
			return nil, errors.New("claim " + m.Claim + " is not bound")
		}
		pv, err := s.Control.Kube.API.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		source, ok := nfsSourceOf(pv)
		if !ok {
			return nil, errors.New("claim " + m.Claim + " is not an NFS volume")
		}
		sources[i] = source
	}
	return sources, nil
}

// bind plans an NFS-only launch and persists the durable dispatch.
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
	sources, err := s.resolveNFSSources(ctx, q)
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
			// a recreated claim of another class under the same name must
			// not silently reuse the persisted dispatch of the old class.
			// Legacy engine-backed rows are always incompatible.
			if dispatch.Kind != "nfs" {
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
		// Direct-mount bind: the control plugin plans the launch locally.
		// No engine operation or session exists for NFS exports, so the
		// dispatch carries each resolved export source instead.
		dispatch = Dispatch{Kind: "nfs", Operation: launch, RunUID: launch}
		for i, m := range q.Mounts {
			binding, err := newTicket()
			if err != nil {
				return err
			}
			dispatch.Bindings = append(dispatch.Bindings, DispatchBinding{
				BindingID: binding, GuestPath: m.GuestPath, ReadOnly: m.ReadOnly, Kind: "nfs", Source: sources[i]})
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
	// Direct-mount bind: the node service verified the kernel evidence;
	// only the business run row remains to commit.
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
	// Direct-mount bind: the node service already unmounted the exports;
	// only the business run row remains to retire.
	_, err = s.Control.Ledger.Session(ctx).Exec(ctx,
		`UPDATE sandboxsched_runs SET state='Released', updated=now() WHERE id=$1 AND state IN ('Planned','Ready')`, signal.Sandbox)
	return err
}
