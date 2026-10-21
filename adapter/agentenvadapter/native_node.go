package agentenvadapter

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

func (n *Node) nativeCompute(r NodeRecord) bool {
	return r.Native != nil && len(r.Native.ManagedMounts) == 0
}
func (n *Node) authorizeNativeHook(r *http.Request) bool {
	key, err := os.ReadFile(n.Config.HookTokenFile)
	if err != nil {
		return false
	}
	token := strings.TrimSpace(string(key))
	values := r.Header.Values("Authorization")
	return len(token) >= 32 && len(values) == 1 && subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+token)) == 1
}

type nativeAdmitRequest struct {
	Context    string `json:"context"`
	BodyDigest string `json:"body_digest"`
}

func (n *Node) nativeAdmit(w http.ResponseWriter, r *http.Request) {
	var q nativeAdmitRequest
	if err := decode(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	reply, err := n.prepareNative(r.Context(), q)
	if err != nil {
		problem(w, 409, err)
		return
	}
	respond(w, 200, reply)
}
func (n *Node) prepareNative(ctx context.Context, q nativeAdmitRequest) (map[string]any, error) {
	if n.isDraining() {
		return nil, errors.New("native storage node is draining")
	}
	raw, err := base64.RawURLEncoding.DecodeString(q.Context)
	if err != nil {
		return nil, err
	}
	key, err := os.ReadFile(n.Config.DispatchKeyFile)
	if err != nil {
		return nil, err
	}
	envelope, err := auth.VerifyJSONEnvelope([]byte(strings.TrimSpace(string(key))), raw, "agentenv.storage.v2", "scheduler", time.Now())
	if err != nil {
		return nil, err
	}
	if len(envelope.Payload) > NativeMaxPlanBytes {
		return nil, errors.New("native storage plan exceeds negotiated size limit")
	}
	var d NativeDispatch
	if err = json.Unmarshal(envelope.Payload, &d); err != nil {
		return nil, err
	}
	if !safeID.MatchString(d.RequestID) || d.RunUID == "" || d.OwnerUID == "" || d.Resources.CPU <= 0 || d.Resources.MemoryMiB <= 0 {
		return nil, errors.New("invalid native dispatch identity")
	}
	if envelope.RequestID != d.RequestID || d.BodyDigest != q.BodyDigest || d.Candidate.ID != n.Config.NativeNodeID || d.Candidate.KubeNode != n.Config.NodeName || d.Candidate.NodeUID != n.Config.NodeUID || d.Candidate.BootID != n.BootID {
		return nil, core.ErrConflict
	}
	start := StartRequest{ID: d.RequestID, Instance: d.RunUID, Operation: d.Result.Operation, Plan: d.Result.SignedPlan, Resources: d.Resources, Initialize: d.Initialize, Expand: d.Expand}
	var record NodeRecord
	if len(d.ManagedMounts) > 0 {
		record, err = n.register(start, false, &d)
		if err != nil {
			return nil, err
		}
	}
	record, err = n.Journal.Update(d.RequestID, func(x *NodeRecord) error {
		if x.State == "Released" || x.Stop || x.DeleteRequested {
			return core.ErrConflict
		}
		if x.Native != nil && core.Digest(x.Native) != core.Digest(d) {
			return core.ErrConflict
		}
		if x.Request.ID == "" {
			if len(d.ManagedMounts) > 0 {
				return errors.New("native volume registration missing")
			}
			*x = NodeRecord{Request: start, Ticket: newID(), State: "Prepared", BootID: n.BootID, NodeUID: n.Config.NodeUID, PIDNamespace: n.PIDNamespace, Devices: []Device{}, Format: map[string]string{}}
		}
		x.Native = &d
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err = n.Work(ctx, d.RequestID); err != nil {
		return nil, err
	}
	record, err = n.Journal.Get(d.RequestID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"ticket": record.Ticket, "owner_uid": d.OwnerUID, "run_uid": d.RunUID, "request_id": d.RequestID, "template_pin": d.TemplatePin, "checkpoint_source": d.CheckpointSource, "managed_mounts": append([]NativeMount{}, d.ManagedMounts...), "native_id": record.Runtime.SandboxID, "replace_mounts": d.ReplaceMounts, "relocations": append([]NativeRelocation{}, d.Relocations...)}, nil
}

func (n *Node) syncNativeControl(ctx context.Context, id string) error {
	r, err := n.Journal.Get(id)
	if err != nil {
		return err
	}
	if r.Native == nil || n.Control == nil {
		return nil
	}
	if r.DeleteRequested && !r.DeleteAcknowledged {
		data, _ := json.Marshal(map[string]any{"request_id": id, "native_id": r.Runtime.SandboxID, "ever_ready": r.EverReady})
		if _, err = n.Control(ctx, NativeCommand{Action: "delete", NodeID: n.Config.NativeNodeID, OwnerID: r.Native.OwnerUID, RunID: r.Native.RunUID, Data: data}); err != nil {
			return err
		}
		if _, err = n.Journal.Update(id, func(x *NodeRecord) error { x.DeleteAcknowledged = true; return nil }); err != nil {
			return err
		}
	}
	action := ""
	if r.State == "Ready" && !r.Stop && !r.ControlReady && !r.DeleteRequested {
		action = "ready"
	}
	if r.State == "Stopped" {
		action = "stopped"
	}
	if action == "" {
		return nil
	}
	proofAction := action
	if action == "stopped" {
		proofAction = "detached"
	}
	data, _ := json.Marshal(map[string]any{"request_id": id, "native_id": r.Runtime.SandboxID, "proof": n.proof(r, proofAction), "ever_ready": r.EverReady})
	if _, err = n.Control(ctx, NativeCommand{Action: action, NodeID: n.Config.NativeNodeID, OwnerID: r.Native.OwnerUID, RunID: r.Native.RunUID, Data: data}); err != nil {
		return err
	}
	_, err = n.Journal.Update(id, func(x *NodeRecord) error {
		if action == "ready" {
			x.ControlReady = true
		} else {
			x.State = "Released"
		}
		return nil
	})
	return err
}

func (n *Node) nativeLifecycle(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Ticket     string          `json:"ticket"`
		Action     string          `json:"action"`
		Capture    *NativeCapture  `json:"capture,omitempty"`
		Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
		ChildID    string          `json:"child_id,omitempty"`
	}
	if err := decode(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	if (q.Action == "resume" || q.Action == "fork") && n.isDraining() {
		problem(w, 409, errors.New("native storage node is draining"))
		return
	}
	record, err := n.Journal.ByTicket(q.Ticket)
	if err != nil || record.Native == nil || n.Control == nil {
		problem(w, 409, errors.New("native storage session unavailable"))
		return
	}
	if q.Action == "capture" {
		if q.Capture == nil || record.Runtime.Capture == nil || core.Digest(q.Capture) != core.Digest(record.Runtime.Capture) || record.Stop {
			problem(w, 409, core.ErrConflict)
			return
		}
		data, _ := json.Marshal(map[string]any{"request_id": record.Request.ID, "capture": q.Capture, "proof": n.captureProof(record, *q.Capture)})
		result, err := n.Control(r.Context(), NativeCommand{Action: "capture", NodeID: n.Config.NativeNodeID, OwnerID: record.Native.OwnerUID, RunID: record.Native.RunUID, Data: data})
		if err != nil {
			problem(w, 409, err)
			return
		}
		respond(w, 200, json.RawMessage(result))
		return
	}
	if q.Action == "cancel_unstarted" {
		_, err = n.Journal.Update(record.Request.ID, func(x *NodeRecord) error {
			// This transaction also serializes against the native prepare event.
			// Once a runtime instance is associated, process evidence is required.
			if x.Runtime.InstanceID != "" {
				return core.ErrConflict
			}
			x.Stop = true
			x.Dispatched = false
			return nil
		})
		if err == nil {
			err = n.Work(r.Context(), record.Request.ID)
		}
		if err == nil {
			err = n.syncNativeControl(r.Context(), record.Request.ID)
		}
		if err != nil {
			problem(w, 409, err)
			return
		}
		respond(w, 200, map[string]bool{"accepted": true})
		return
	}
	if q.Action == "delete" {
		if _, err = n.Journal.Update(record.Request.ID, func(x *NodeRecord) error { x.DeleteRequested = true; return nil }); err == nil {
			err = n.syncNativeControl(r.Context(), record.Request.ID)
		}
		if err != nil {
			problem(w, 409, err)
			return
		}
		respond(w, 200, map[string]bool{"accepted": true})
		return
	}
	switch q.Action {
	case "pause", "cancel_pause", "resume", "fork", "capture.retire", "cleanup.status":
	default:
		problem(w, 400, ErrUnsupported)
		return
	}
	dataFields := map[string]any{"request_id": record.Request.ID, "native_id": record.Runtime.SandboxID}
	if q.Action == "fork" {
		if record.State != "Ready" || record.Runtime.Event != "ready" || record.Stop || record.DeleteRequested || !safeID.MatchString(q.ChildID) {
			problem(w, 409, core.ErrConflict)
			return
		}
		dataFields["native_id"] = q.ChildID
		dataFields["checkpoint"] = q.Checkpoint
	}
	if q.Action == "cleanup.status" {
		dataFields["ever_ready"] = record.EverReady
	}
	if q.Action == "capture.retire" {
		if q.Capture == nil || !safeID.MatchString(q.Capture.BarrierID) {
			problem(w, 400, core.ErrConflict)
			return
		}
		dataFields["capture"] = q.Capture
	}
	data, _ := json.Marshal(dataFields)
	result, err := n.controlResumeWithHandoffWait(r.Context(), q.Action, record, data)
	if err != nil {
		problem(w, 409, err)
		return
	}
	if q.Action == "resume" || q.Action == "fork" {
		var next nativeAdmitRequest
		if err = json.Unmarshal(result, &next); err != nil {
			problem(w, 502, err)
			return
		}
		reply, err := n.prepareNative(r.Context(), next)
		if err != nil {
			problem(w, 409, err)
			return
		}
		respond(w, 200, reply)
		return
	}
	respond(w, 200, json.RawMessage(result))
}

// The storage provider hands a paused owner's exclusive use to a new run
// only after the old VMM has actually stopped, and the runtime can observe
// the paused state before that stop receipt has reached the scheduler. A
// resume racing that settling tail must wait for the handoff instead of
// surfacing the transient conflict to the caller.
const resumeHandoffWait = 60 * time.Second

// controlResumeWithHandoffWait forwards a lifecycle control command and, for
// resume only, retries while the scheduler still reports the previous run as
// holding the resource exclusively.
func (n *Node) controlResumeWithHandoffWait(ctx context.Context, action string, record NodeRecord, data []byte) (json.RawMessage, error) {
	command := NativeCommand{Action: action, NodeID: n.Config.NativeNodeID, OwnerID: record.Native.OwnerUID, RunID: record.Native.RunUID, Data: data}
	deadline := time.Now().Add(resumeHandoffWait)
	for {
		result, err := n.Control(ctx, command)
		if err == nil || action != "resume" || !isHandoffConflict(err) || time.Now().After(deadline) {
			return result, err
		}
		slog.Warn("resume handoff still settling; retrying", "request_id", record.Request.ID, "error", err.Error())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// isHandoffConflict reports whether the storage scheduler rejected the
// command because the previous run still holds the resource exclusively.
func isHandoffConflict(err error) bool {
	if errors.Is(err, core.ErrConflict) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.FailedPrecondition
	}
	return false
}

func (n *Node) nativeVolumeControl(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Action string          `json:"action"`
		Data   json.RawMessage `json:"data"`
	}
	if err := decode(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	switch q.Action {
	case "lookup", "list", "create", "claim_legacy", "delete", "expand":
	default:
		problem(w, 400, ErrUnsupported)
		return
	}
	if n.Control == nil {
		problem(w, 503, errors.New("storage control unavailable"))
		return
	}
	result, err := n.Control(r.Context(), NativeCommand{Action: "volumes." + q.Action, NodeID: n.Config.NativeNodeID, Data: q.Data})
	if err != nil {
		problem(w, 409, err)
		return
	}
	respond(w, 200, json.RawMessage(result))
}

// nativeCancel is controller-authenticated revocation. It never invokes the
// native sandbox API and never treats a dispatched VMM as already stopped.
func (n *Node) nativeCancel(w http.ResponseWriter, r *http.Request) {
	var d NativeDispatch
	if err := decode(w, r, &d); err != nil {
		problem(w, 400, err)
		return
	}
	if !safeID.MatchString(d.RequestID) || d.RunUID == "" || d.Candidate.KubeNode != n.Config.NodeName || d.Candidate.NodeUID != n.Config.NodeUID || d.Candidate.BootID == "" || (d.Candidate.ID != n.Config.NativeNodeID && d.Candidate.BootID == n.BootID) {
		problem(w, 409, core.ErrConflict)
		return
	}
	start := StartRequest{ID: d.RequestID, Instance: d.RunUID, Operation: d.Result.Operation, Plan: d.Result.SignedPlan, Resources: d.Resources, Initialize: d.Initialize, Expand: d.Expand}
	var err error
	if len(d.ManagedMounts) > 0 {
		_, err = n.register(start, true, &d)
	} else {
		_, err = n.Journal.Update(d.RequestID, func(x *NodeRecord) error {
			if x.Request.ID != "" && (x.Request.Instance != d.RunUID || x.Native == nil || core.Digest(x.Native) != core.Digest(d)) {
				return core.ErrConflict
			}
			if x.Request.ID == "" {
				*x = NodeRecord{Request: start, Native: &d, Ticket: newID(), State: "Queued", BootID: n.BootID, NodeUID: n.Config.NodeUID, PIDNamespace: n.PIDNamespace}
			}
			x.Stop = true
			return nil
		})
	}
	if err == nil {
		err = n.Work(r.Context(), d.RequestID)
	}
	if err == nil {
		err = n.syncNativeControl(r.Context(), d.RequestID)
	}
	if err != nil {
		problem(w, 409, err)
		return
	}
	record, err := n.Journal.Get(d.RequestID)
	if err != nil {
		problem(w, 500, err)
		return
	}
	respond(w, 200, record)
}

// Native repository hooks share the authenticated loopback boundary, but have
// no sandbox ticket: immutable templates outlive their capturing run.
func (n *Node) nativeCheckpointControl(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Action string          `json:"action"`
		Data   json.RawMessage `json:"data"`
	}
	if err := decode(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	if q.Action != "publish" && q.Action != "delete_begin" {
		problem(w, 400, ErrUnsupported)
		return
	}
	if n.Control == nil {
		problem(w, 503, errors.New("storage control unavailable"))
		return
	}
	if q.Action == "delete_begin" {
		_ = n.importNativePublications(r.Context())
	}
	result, err := n.Control(r.Context(), NativeCommand{Action: "checkpoint." + q.Action, NodeID: n.Config.NativeNodeID, Data: q.Data})
	if err != nil {
		problem(w, 409, err)
		return
	}
	respond(w, 200, json.RawMessage(result))
}
