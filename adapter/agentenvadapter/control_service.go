package agentenvadapter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"os"
	control "ssf.dev/control"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	"strings"
	"time"
)

// ControlService is the runtime registration at the public coordinator boundary.
// Only this service opens NativeControl. Native Scheduler keeps node selection.
type ControlService struct {
	AdmissionAllowed                 func() bool
	Control                          *NativeControl
	Config                           NativeConfig
	GatewayKey, DispatchKey, NodeKey []byte
	Observe                          func(context.Context) ([]control.Node, error)
}

func LoadControlKey(path string) ([]byte, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	b = []byte(strings.TrimSpace(string(b)))
	if len(b) < 32 {
		return nil, errors.New("control key too short")
	}
	return b, nil
}
func (s *ControlService) candidates(ctx context.Context, inputs []control.Node) ([]NativeCandidate, error) {
	live, e := s.Observe(ctx)
	if e != nil {
		return nil, e
	}
	wanted := map[string]bool{}
	for _, n := range inputs {
		wanted[n.ID] = true
	}
	out := []NativeCandidate{}
	for _, n := range live {
		if inputs == nil || wanted[n.ID] {
			out = append(out, NativeCandidate{ID: n.ID, Endpoint: n.Endpoint})
		}
	}
	return out, nil
}
func nativeWire(n NativeCandidate) control.Node {
	return control.Node{ID: n.ID, Endpoint: n.Endpoint, Capacity: control.Resources{CPU: n.Capacity.CPU, MemoryMiB: n.Capacity.MemoryMiB}, Allocated: control.Resources{CPU: n.Allocated.CPU, MemoryMiB: n.Allocated.MemoryMiB}, SandboxIDs: n.SandboxIDs}
}
func (s *ControlService) intent(raw []byte) (NativeIntent, error) { return s.intentAt(raw, time.Now()) }
func (s *ControlService) intentAt(raw []byte, now time.Time) (NativeIntent, error) {
	var q NativeIntent
	e, err := auth.VerifyJSONEnvelope(s.GatewayKey, raw, NativeStorageProtocol, "gateway", now)
	if err != nil {
		return q, &control.Error{Code: "unauthenticated", Message: err.Error()}
	}
	var v struct {
		Method     string `json:"method"`
		Path       string `json:"path"`
		Body       []byte `json:"body"`
		BodyDigest string `json:"body_digest"`
		NodeID     string `json:"node_id"`
		OwnerID    string `json:"owner_id"`
	}
	if err = json.Unmarshal(e.Payload, &v); err != nil {
		return q, &control.Error{Code: "invalid", Message: err.Error()}
	}
	if v.NodeID != "" || v.OwnerID != "" {
		return q, &control.Error{Code: "permission", Message: "gateway cannot impersonate pinned native lifecycle"}
	}
	if v.Method != "POST" || !safeID.MatchString(e.RequestID) || v.NodeID != "" || v.OwnerID != "" || len(v.Body) > 512*1024 || v.BodyDigest != core.Hash(string(v.Body)) || (v.Path != "/sandboxes-cold" && v.Path != "/sandboxes" && v.Path != "/v2/sandboxes") {
		return q, &control.Error{Code: "invalid", Message: "invalid native launch intent"}
	}
	return NativeIntent{RequestID: e.RequestID, Tenant: s.Control.Principal.Tenant, Path: v.Path, Body: v.Body, BodyDigest: v.BodyDigest}, nil
}
func (s *ControlService) dispatch(d NativeDispatch) (control.Response, error) {
	signed, e := auth.SignJSONEnvelope(s.DispatchKey, NativeStorageProtocol, "scheduler", d.RequestID, d, time.Now())
	n := nativeWire(d.Candidate)
	return control.Response{LaunchID: d.RequestID, State: "Dispatched", Selected: &n, Dispatch: signed}, e
}
func (s *ControlService) inspect(ctx context.Context, q NativeIntent) (control.Response, error) {
	out := control.Response{LaunchID: q.RequestID}
	var raw []byte
	var digest, run string
	e := s.Control.Ledger.Session(ctx).QueryRow(ctx, `SELECT state,dispatch,digest,run_uid FROM agentenv_native_runs WHERE id=$1`, q.RequestID).Scan(&out.State, &raw, &digest, &run)
	if errors.Is(e, pgx.ErrNoRows) {
		out.State = "Absent"
		return out, nil
	}
	if e != nil {
		return out, e
	}
	if digest != core.Digest(q) {
		return out, core.ErrConflict
	}
	if out.State == "Dispatched" || out.State == "Ready" {
		var d NativeDispatch
		if e = json.Unmarshal(raw, &d); e != nil {
			return out, e
		}
		reply, err := s.dispatch(d)
		reply.State = out.State
		return reply, err
	}
	// A reservation is retained even if Attach timed out. Never choose another
	// node until the existing operation has been reconciled by the same launch.
	a := NativeAdmission{Ledger: s.Control.Ledger}
	for _, id := range []string{"native/" + q.RequestID, core.Hash(q.Tenant + "\x00" + run + "\x00" + q.RequestID)} {
		r, err := a.load(ctx, id)
		if err == nil && r.State != "Released" {
			n := nativeWire(r.Candidate)
			out.Selected = &n
			break
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return out, err
		}
	}
	return out, nil
}
func fixedChoice(nodes []NativeCandidate) (NativeCandidate, error) {
	if len(nodes) != 1 {
		return NativeCandidate{}, errors.New("original native placement required")
	}
	return nodes[0], nil
}
func (s *ControlService) Call(ctx context.Context, method string, req control.Request) (control.Response, error) {
	var out control.Response
	if method == "CaptureVolume" {
		var q struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(req.Envelope, &q); err != nil {
			return out, err
		}
		capture, err := s.Control.DirectCapture(ctx, q.ID)
		if errors.Is(err, ErrNoLiveNativeCapture) {
			out.State = "Idle"
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out.Envelope, err = json.Marshal(capture)
		return out, err
	}
	if method == "InvokeLifecycle" || method == "LocalLaunch" {
		return s.invoke(ctx, method, req)
	}
	q, e := s.intent(req.Envelope)
	if e != nil && method != "ResolveLaunch" && req.LaunchID != "" {
		// A resolved launch survives a long WFFC/Attach timeout. An expired
		// proof may only reopen the exact persisted intent over the already
		// authenticated Scheduler mTLS channel; it cannot create a new launch.
		var raw []byte
		if err := s.Control.Ledger.Session(ctx).QueryRow(ctx, `SELECT intent FROM agentenv_native_runs WHERE id=$1`, req.LaunchID).Scan(&raw); err == nil {
			var saved NativeIntent
			var envelope auth.JSONEnvelope
			if json.Unmarshal(raw, &saved) == nil && json.Unmarshal(req.Envelope, &envelope) == nil {
				proof, err := s.intentAt(req.Envelope, time.Unix(envelope.IssuedAt, 0))
				if err == nil && saved.RequestID == req.LaunchID && core.Digest(proof) == core.Digest(saved) {
					q, e = saved, nil
				}
			}
		}
	}
	if e != nil {
		return out, e
	}
	if req.LaunchID != "" && req.LaunchID != q.RequestID {
		return out, core.ErrConflict
	}
	prior, e := s.inspect(ctx, q)
	if e != nil {
		return out, e
	}
	if method == "InspectOperation" {
		return prior, nil
	}
	if method == "PrepareLaunch" && prior.State == "Absent" {
		return out, core.ErrConflict
	}
	if len(prior.Dispatch) > 0 {
		return prior, nil
	}
	candidates, e := s.candidates(ctx, req.Candidates)
	if e != nil {
		return out, e
	}
	if method == "ResolveLaunch" {
		d, err := s.Control.prepareLaunch(ctx, q, candidates, nil, true)
		if err != nil {
			return out, err
		}
		// Resolve the pinned mount identities through the common Kubernetes logic.
		volumes := []core.Volume{}
		for _, m := range d.ManagedMounts {
			name := m.Name
			if d.VolumeNames[name] != "" {
				name = d.VolumeNames[name]
			}
			v, err := s.Control.Volumes.Get(ctx, q.Tenant, name)
			if err != nil {
				return out, err
			}
			volume, err := s.Control.Kube.Resolve(ctx, v.PVC, v.ReadOnly)
			if errors.Is(err, core.ErrPending) {
				volume = core.Volume{PVC: v.PVC, UID: v.PVC.UID, ReadOnly: v.ReadOnly}
			} else if err != nil {
				return out, err
			}
			volumes = append(volumes, volume)
		}
		a := NativeAdmission{Ledger: s.Control.Ledger, Kube: s.Control.Kube, Refresh: s.Control.Refresh, Candidates: candidates, Resources: d.Resources}
		feasible, err := a.Feasible(ctx, volumes)
		if err != nil {
			return out, err
		}
		out = prior
		out.Resources = control.Resources{CPU: d.Resources.CPU, MemoryMiB: d.Resources.MemoryMiB}
		for _, n := range feasible {
			if prior.Selected == nil || n.ID == prior.Selected.ID {
				out.Candidates = append(out.Candidates, nativeWire(n))
			}
		}
		return out, nil
	}
	if method != "PrepareLaunch" || req.Selected == "" {
		return out, core.ErrConflict
	}
	if prior.Selected != nil && prior.Selected.ID != req.Selected {
		return out, core.ErrConflict
	}
	selected := []NativeCandidate{}
	for _, n := range candidates {
		if n.ID == req.Selected {
			selected = append(selected, n)
		}
	}
	if len(selected) != 1 {
		return out, core.ErrPending
	}
	d, e := s.Control.Prepare(ctx, q, selected, fixedChoice)
	if e != nil {
		// Retryable means there is positively no reservation, not just an RPC error.
		state, inspectErr := s.inspect(ctx, q)
		if errors.Is(e, ErrNativeCapacity) && inspectErr == nil && state.Selected == nil {
			return out, &control.Error{Code: "capacity", Message: e.Error()}
		}
		return out, e
	}
	return s.dispatch(d)
}
func (s *ControlService) invoke(ctx context.Context, method string, req control.Request) (control.Response, error) {
	var out control.Response
	envelope, e := auth.VerifyJSONEnvelope(s.NodeKey, req.Envelope, NativeStorageProtocol, "node", time.Now())
	if e != nil {
		return out, e
	}
	var command NativeCommand
	if e = json.Unmarshal(envelope.Payload, &command); e != nil {
		return out, e
	}
	if method == "LocalLaunch" {
		if s.Config.Standalone == nil || command.NodeID != s.Config.Standalone.NodeID {
			return out, core.ErrConflict
		}
		var q NativeIntent
		if e = json.Unmarshal(command.Data, &q); e != nil {
			return out, e
		}
		if q.ForkOf != "" || q.ResumeOf != "" || q.OwnerUID != "" || q.ComputeCheckpoint != "" || q.ExpectedNativeID != "" || !safeID.MatchString(q.RequestID) || (q.Path != "/sandboxes-cold" && q.Path != "/sandboxes" && q.Path != "/v2/sandboxes") {
			return out, core.ErrConflict
		}
		q.Tenant = s.Control.Principal.Tenant
		q.FixedNode = command.NodeID
		d, err := s.Control.Prepare(ctx, q, []NativeCandidate{{ID: command.NodeID, Endpoint: s.Config.Standalone.Endpoint}}, fixedChoice)
		if err != nil {
			return out, err
		}
		out.Envelope, err = auth.SignJSONEnvelope(s.DispatchKey, NativeStorageProtocol, "scheduler", envelope.RequestID, d, time.Now())
		return out, err
	}
	if (command.Action == "resume" || command.Action == "fork" || command.Action == "compute") && s.AdmissionAllowed != nil && !s.AdmissionAllowed() {
		return out, core.ErrPending
	}
	// Cleanup can arrive after discovery drains; identity is still verified from
	// the actual Pod/Node/boot and the node's persisted stop evidence.
	if _, e = s.Control.RefreshStopped(ctx, NativeCandidate{ID: command.NodeID}); e != nil {
		return out, e
	}
	var candidates []NativeCandidate
	if command.Action == "resume" || command.Action == "fork" || command.Action == "compute" {
		candidates, e = s.candidates(ctx, []control.Node{{ID: command.NodeID}})
		if e != nil {
			return out, e
		}
		if len(candidates) != 1 {
			return out, core.ErrPending
		}
	}
	reply, e := s.Control.Command(ctx, command, candidates, fixedChoice)
	if e != nil {
		return out, e
	}
	result := reply.Data
	if reply.Dispatch != nil {
		signed, err := auth.SignJSONEnvelope(s.DispatchKey, NativeStorageProtocol, "scheduler", reply.Dispatch.RequestID, reply.Dispatch, time.Now())
		if err != nil {
			return out, err
		}
		result = map[string]any{"context": base64.RawURLEncoding.EncodeToString(signed), "body_digest": reply.Dispatch.BodyDigest}
	}
	out.Envelope, e = auth.SignJSONEnvelope(s.DispatchKey, NativeStorageProtocol, "scheduler", envelope.RequestID, result, time.Now())
	return out, e
}
