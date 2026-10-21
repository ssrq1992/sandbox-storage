package agentenvadapter

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/observe"
	"strings"
	"sync"
	"time"
)

type StorageNode interface {
	PrepareVolumes(context.Context, *v1.PrepareRequest) (*v1.PrepareResponse, error)
	Ready(context.Context, string, string, string) error
	Release(context.Context, string, string, string, string) error
}
type Node struct {
	contentMu sync.Mutex
	workLocks sync.Map // operation-scoped preparation/lifecycle serialization

	LocalPrepare      func(context.Context, NativeIntent) (NativeDispatch, error)
	publicationMu     sync.Mutex
	publicationOffset int
	Control           func(context.Context, NativeCommand) (json.RawMessage, error)
	Config            Config
	PIDNamespace      string
	BootID            string
	Key               ed25519.PublicKey
	Journal           *Journal
	SSF               StorageNode
	Agent             *HTTPClient
	Metrics           *observe.Registry
	Context           context.Context
	workMu            sync.Mutex
	draining          bool
	workers           sync.WaitGroup
	running           sync.Map
	changed           sync.Map
}

func (n *Node) Register(q StartRequest, stop bool) (NodeRecord, error) {
	return n.register(q, stop, nil)
}
func (n *Node) register(q StartRequest, stop bool, native *NativeDispatch) (NodeRecord, error) {
	if !safeID.MatchString(q.ID) || q.Instance == "" {
		return NodeRecord{}, errors.New("operation and instance required")
	}
	old, e := n.Journal.Get(q.ID)
	revoked := e == nil && old.Request.Plan == ""
	if revoked && old.Request.Instance != q.Instance {
		return old, core.ErrConflict
	}
	if e == nil && !revoked {
		a, b := old.Request, q
		a.Plan = ""
		b.Plan = ""
		if core.Digest(a) != core.Digest(b) {
			return old, core.ErrConflict
		}
		if q.Plan != old.Request.Plan {
			fresh, err := auth.Verify(q.Plan, n.Key, n.Config.NodeName, n.BootID, time.Now())
			if err != nil {
				return old, err
			}
			prior, err := auth.Verify(old.Request.Plan, n.Key, n.Config.NodeName, n.BootID, time.Unix(0, 0))
			if err != nil {
				return old, err
			}
			fresh.Expires = prior.Expires
			if core.Digest(fresh) != core.Digest(prior) {
				return old, core.ErrConflict
			}
		}
		return n.Journal.Update(q.ID, func(r *NodeRecord) error {
			if native != nil {
				if r.Native != nil && core.Digest(r.Native) != core.Digest(native) {
					return core.ErrConflict
				}
				r.Native = native
			}
			if stop {
				r.Stop = true
			}
			r.Request.Plan = q.Plan
			return nil
		})
	}
	if e != nil && !errors.Is(e, ErrNotFound) {
		return old, e
	}
	var p core.Plan
	if stop {
		p, e = auth.VerifyRevocation(q.Plan, n.Key, n.Config.NodeName, n.Config.NodeUID, n.BootID)
	} else {
		p, e = auth.Verify(q.Plan, n.Key, n.Config.NodeName, n.BootID, time.Now())
	}
	if e != nil {
		return old, e
	}
	if p.Operation != q.Operation || p.Sandbox != q.Instance || p.NodeUID != n.Config.NodeUID {
		return old, core.ErrConflict
	}
	for _, b := range p.Bindings {
		if b.Volume.Capability.GetBlock() == nil {
			return old, ErrUnsupported
		}
	}
	return n.Journal.Update(q.ID, func(r *NodeRecord) error {
		if r.Request.Plan != "" || r.Request.Instance != "" && r.Request.Instance != q.Instance {
			return core.ErrConflict
		}
		stop = stop || r.Stop
		*r = NodeRecord{Native: native, Request: q, State: "Queued", Ticket: newID(), Stop: stop, BootID: n.BootID, NodeUID: n.Config.NodeUID, PIDNamespace: n.PIDNamespace, Format: map[string]string{}}
		return nil
	})
}
func (n *Node) Kick(id string) {
	n.workMu.Lock()
	if n.draining {
		n.workMu.Unlock()
		return
	}
	if _, yes := n.running.LoadOrStore(id, true); yes {
		n.workMu.Unlock()
		return
	}
	n.workers.Add(1)
	n.workMu.Unlock()
	go func() {
		defer n.workers.Done()
		defer n.running.Delete(id)
		defer n.signal(id)
		ctx, cancel := context.WithTimeout(n.Context, 2*time.Minute)
		defer cancel()
		start := time.Now()
		e := n.Work(ctx, id)
		// The native create response can race the ready callback while this
		// worker is already active. Complete the newly durable transition now.
		if e == nil {
			if r, err := n.Journal.Get(id); err == nil && (r.State == "GuestReady" || r.Stop && r.State != "Stopped" && r.State != "Released") {
				e = n.Work(ctx, id)
			}
		}
		if e == nil && n.Config.NativeMode {
			e = n.syncNativeControl(ctx, id)
		}
		if n.Metrics != nil {
			n.Metrics.Observe("adapter_node", time.Since(start), e)
		}
		if e != nil {
			_, _ = n.Journal.Update(id, func(r *NodeRecord) error { r.Reason = e.Error(); return nil })
			slog.Warn("node operation retained", "operation", id, "error", e)
		}
	}()
}
func (n *Node) Recover(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		if n.Config.NativeMode {
			if err := n.importNativePublications(ctx); err != nil {
				slog.Warn("native publication outbox retained", "error", err)
			}
			if err := n.importNativeCleanup(); err != nil {
				slog.Warn("native cleanup outbox retained", "error", err)
			}
		}
		ids, e := n.Journal.Pending()
		if e == nil {
			for _, id := range ids {
				n.Kick(id)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
func (n *Node) Work(ctx context.Context, id string) error {
	lock, _ := n.workLocks.LoadOrStore(id, &sync.Mutex{})
	lock.(*sync.Mutex).Lock()
	defer lock.(*sync.Mutex).Unlock()
	r, e := n.Journal.Get(id)
	if e != nil {
		return e
	}
	if r.State == "Released" || (r.Request.Plan == "" && !n.nativeCompute(r)) {
		return nil
	}
	if r.BootID != n.BootID {
		if r.NodeUID != n.Config.NodeUID {
			return errors.New("node object replaced; references retained")
		}
		_, e = n.Journal.Update(id, func(x *NodeRecord) error {
			x.Stop = true
			x.State = "Stopped"
			x.Reason = "old runtime terminated by host reboot"
			return nil
		})
		return e
	}
	if r.PIDNamespace != n.PIDNamespace {
		return errors.New("Pod PID namespace replaced without host reboot; old VMM absence requires host fencing")
	}
	if r.Dispatched && r.Runtime.InstanceID == "" {
		ev, found, err := n.discover(r.Ticket)
		if err != nil {
			return err
		}
		if found {
			first := ev
			first.Sequence = 1
			first.Event = "prepare"
			first.PID = 0
			first.ProcessStart = ""
			first.Devices = nil
			if err = n.acceptEvent(first); err != nil {
				return err
			}
			r, err = n.Journal.Get(id)
			if err != nil {
				return err
			}
		}
	}
	// Native journals close the delivery gap between a durable runtime transition
	// and its HTTP notification. They are written only by the trusted runtime.
	if r.Runtime.InstanceID != "" {
		if ev, e := n.nativeEvent(r.Runtime.InstanceID); e == nil && ev.Ticket == r.Ticket {
			if e = n.acceptEvent(ev); e != nil {
				return e
			}
			r, _ = n.Journal.Get(id)
		}
	}
	if r.Stop || r.State == "Stopped" {
		return n.stop(ctx, r)
	}
	if r.State == "Ready" {
		if !processAlive(r.Runtime.OwnerPID, r.Runtime.OwnerStart) {
			_, e = n.Journal.Update(id, func(x *NodeRecord) error {
				x.Stop = true
				x.Reason = "native owner exited; stopping orphan before release"
				return nil
			})
			return e
		}
		if r.Native != nil && (r.Runtime.Event == "capturing" || r.Runtime.Event == "thawing") {
			return n.verifyRuntime(r, "capture-held")
		}
		return n.verifyRuntime(r, "ready")
	}
	if len(r.Devices) == 0 && !n.nativeCompute(r) {
		_, e = n.Journal.Update(id, func(x *NodeRecord) error { x.State = "Preparing"; return nil })
		if e != nil {
			return e
		}
		start := time.Now()
		p, e := n.SSF.PrepareVolumes(ctx, &v1.PrepareRequest{RequestId: r.Request.Operation, SignedNodePlan: r.Request.Plan})
		if n.Metrics != nil {
			n.Metrics.Observe("prepare", time.Since(start), e)
		}
		if e != nil {
			_, _ = n.Journal.Update(id, func(x *NodeRecord) error { x.Stop = true; return nil })
			return e
		}
		var ds []Device
		for _, ep := range p.Endpoints {
			b := ep.GetBlockDevice()
			if b == nil {
				return ErrUnsupported
			}
			d := Device{ID: ep.BindingUid, Path: b.ManagedTarget, GuestPath: ep.GuestPath, ReadOnly: ep.Readonly, Major: b.Major, Minor: b.Minor, Bytes: b.SizeBytes}
			if e = verifyDevice(d, n.Config.DeviceRoot); e != nil {
				_, _ = n.Journal.Update(id, func(x *NodeRecord) error { x.Stop = true; return nil })
				return e
			}
			if target := r.Request.Expand[d.ID]; target > 0 {
				expander, ok := n.SSF.(interface {
					Expand(context.Context, string, string, int64) error
				})
				if !ok || d.ReadOnly {
					return ErrUnsupported
				}
				if e = expander.Expand(ctx, r.Request.Operation, d.ID, target); e != nil {
					return e
				}
				if d, e = refreshExpandedDevice(d, n.Config.DeviceRoot, target); e != nil {
					return e
				}
			}
			d.UUID, e = n.filesystem(ctx, id, d, r.Request.Initialize[d.ID])
			if e != nil {
				_, _ = n.Journal.Update(id, func(x *NodeRecord) error { x.Stop = true; return nil })
				return e
			}
			if r.Request.Expand[d.ID] > 0 {
				if e = n.expandFilesystem(ctx, id, d); e != nil {
					return e
				}
			}
			ds = append(ds, d)
		}
		r, e = n.Journal.Update(id, func(x *NodeRecord) error {
			x.Devices = ds
			x.State = "Prepared"
			if x.Timings == nil {
				x.Timings = map[string]float64{}
			}
			elapsed(x.Timings, "prepare_ms", start)
			return nil
		})
		if e != nil {
			_, _ = n.Journal.Update(id, func(x *NodeRecord) error { x.Stop = true; return nil })
			return e
		}
	}
	if r.State == "GuestReady" {
		if e = n.verifyRuntime(r, "ready"); e != nil {
			return e
		}
		if !n.nativeCompute(r) {
			e = n.SSF.Ready(ctx, r.Request.Operation, r.Request.Instance, n.proof(r, "ready"))
		}
		if e != nil {
			return e
		}
		_, e = n.Journal.Update(id, func(x *NodeRecord) error {
			if !x.Stop {
				x.State = "Ready"
			}
			return nil
		})
		return e
	}
	if !r.Dispatched {
		r, e = n.Journal.Update(id, func(x *NodeRecord) error {
			if x.Stop {
				return core.ErrConflict
			}
			x.LaunchAt = time.Now()
			x.Dispatched = true
			x.State = "Launching"
			return nil
		})
		if e != nil {
			return e
		}
		// Admission ends here. The original Orchestrator owns every VMM.
		return nil
	}
	return nil
}
func (n *Node) stop(ctx context.Context, r NodeRecord) error {
	if r.State == "Released" || r.State == "Stopped" {
		return nil
	}
	if !r.Dispatched && r.Runtime.InstanceID == "" {
		_, e := n.Journal.Update(r.Request.ID, func(x *NodeRecord) error { x.State = "Stopped"; x.Stop = true; return nil })
		return e
	}

	if e := n.verifyRuntime(r, "detached"); e != nil {
		if r.Runtime.PID <= 0 || processAlive(r.Runtime.OwnerPID, r.Runtime.OwnerStart) {
			return e
		}
		if e = stopOrphan(ctx, r.Runtime); e != nil {
			return e
		}
		if e = n.verifyRuntime(r, "detached"); e != nil {
			return e
		}
	}
	_, e := n.Journal.Update(r.Request.ID, func(x *NodeRecord) error { x.State = "Stopped"; x.Stop = true; return nil })
	return e
}
func (n *Node) proof(r NodeRecord, action string) string {
	return core.Hash(r.Ticket + "\x00" + r.Request.Instance + "\x00" + r.BootID + "\x00" + action)
}
func (n *Node) nativeEvent(instance string) (RuntimeEvent, error) {
	var ev RuntimeEvent
	if !safeID.MatchString(instance) {
		return ev, errors.New("invalid native instance")
	}
	b, e := os.ReadFile(filepath.Join(n.Config.RuntimeRoot, instance+".json"))
	if e != nil {
		return ev, e
	}
	e = json.Unmarshal(b, &ev)
	return ev, e
}
func (n *Node) verifyRuntime(r NodeRecord, action string) error {
	if r.BootID != n.BootID {
		if action == "detached" && r.Stop && r.NodeUID == n.Config.NodeUID {
			return nil
		}
		return errors.New("stale node incarnation")
	}
	if r.PIDNamespace != n.PIDNamespace {
		return errors.New("PID namespace changed; retain old process references")
	}
	if action == "detached" && !r.Dispatched && r.Runtime.InstanceID == "" && r.Stop {
		return nil
	}
	ev, e := n.nativeEvent(r.Runtime.InstanceID)
	if e != nil {
		return e
	}
	if ev.Ticket != r.Ticket || ev.InstanceID != r.Runtime.InstanceID || ev.BootID != n.BootID {
		return core.ErrConflict
	}
	if action == "capture-held" {
		if r.Native == nil || r.Stop || (ev.Event != "capturing" && ev.Event != "thawing") || core.Digest(ev) != core.Digest(r.Runtime) || !slices.Equal(ev.Devices, r.Devices) {
			return core.ErrConflict
		}
		return verifyProcessDevices(ev, r.Devices)
	}
	if action == "ready" {
		if ev.Event != "ready" || r.Stop {
			return errors.New("runtime is not ready")
		}
		if !slices.Equal(ev.Devices, r.Devices) {
			return errors.New("guest device evidence mismatch")
		}
		return verifyProcessDevices(ev, r.Devices)
	}
	if action != "detached" {
		return ErrUnsupported
	}
	if ev.Event == "stopped" {
		if ev.PID > 0 && processAlive(ev.PID, ev.ProcessStart) {
			return errors.New("VMM still running")
		}
		return nil
	}
	// If the process was durably registered, its exit prevents future use by that
	// instance. A prepared/launching record without a PID remains ambiguous.
	if ev.PID > 0 && ev.ProcessStart != "" && !processAlive(ev.PID, ev.ProcessStart) && !processAlive(ev.OwnerPID, ev.OwnerStart) {
		return nil
	}
	return errors.New("runtime may still execute or use devices; waiting for stopped evidence")
}
func (n *Node) acceptEvent(ev RuntimeEvent) error {
	r, e := n.Journal.ByTicket(ev.Ticket)
	if e != nil {
		return e
	}
	if !safeID.MatchString(ev.InstanceID) || !safeID.MatchString(ev.SandboxID) || ev.BootID != n.BootID {
		return core.ErrConflict
	}
	_, e = n.Journal.Update(r.Request.ID, func(x *NodeRecord) error {
		if x.Native != nil && x.Native.ExpectedNativeID != "" && x.Native.ExpectedNativeID != ev.SandboxID {
			return core.ErrConflict
		}
		if x.State == "Released" {
			return core.ErrConflict
		}
		if x.Runtime.InstanceID != "" && (x.Runtime.InstanceID != ev.InstanceID || x.Runtime.SandboxID != ev.SandboxID) {
			return core.ErrConflict
		}
		if ev.Sequence == 0 {
			return errors.New("event sequence required")
		}
		if x.Runtime.Sequence >= ev.Sequence {
			if x.Runtime.Sequence == ev.Sequence && core.Digest(x.Runtime) != core.Digest(ev) {
				return core.ErrConflict
			}
			return nil
		}
		if ev.Event == "prepare" {
			if x.Stop && x.Runtime.InstanceID == "" {
				return core.ErrConflict
			}
			if x.Runtime.Event != "" {
				return nil
			}
		} else if x.Runtime.InstanceID == "" {
			return errors.New("runtime not registered")
		}
		if x.Runtime.Event == "stopped" && ev.Event != "stopped" {
			return errors.New("stale event after stop")
		}
		switch ev.Event {
		case "prepare":
		case "launching":
		case "starting":
			if ev.PID <= 0 || ev.ProcessStart == "" {
				return errors.New("VMM identity required")
			}
		case "capturing":
			if x.Native == nil || x.Stop || x.DeleteRequested || x.State != "Ready" || ev.Capture == nil || !ev.Capture.valid() || !slices.Equal(ev.Devices, x.Devices) {
				return core.ErrConflict
			}
			if err := verifyProcessDevices(ev, x.Devices); err != nil {
				return err
			}
		case "thawing":
			if x.Native == nil || (x.Runtime.Event != "capturing" && x.Runtime.Event != "ready" && x.Runtime.Event != "thawing") || ev.Capture != nil {
				return core.ErrConflict
			}
		case "ready":
			x.EverReady = true
			if !slices.Equal(ev.Devices, x.Devices) {
				return errors.New("device proof differs")
			}
			if !x.Stop {
				if e := verifyProcessDevices(ev, x.Devices); e != nil {
					return e
				}
				if x.Timings == nil {
					x.Timings = map[string]float64{}
				}
				if x.Runtime.Event != "ready" && !x.LaunchAt.IsZero() {
					x.Timings["launch_guest_ready_ms"] = float64(time.Since(x.LaunchAt)) / float64(time.Millisecond)
				}
				x.State = "GuestReady"
			}
		case "stopped":
			if ev.PID > 0 && processAlive(ev.PID, ev.ProcessStart) {
				return errors.New("VMM has not exited")
			}
			x.Stop = true
			x.State = "Stopped"
		default:
			return errors.New("unknown runtime transition")
		}
		x.Runtime = ev
		return nil
	})
	return e
}
func (n *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/capture/evidence" && r.Method == "POST" {
		n.captureEvidence(w, r)
		return
	}
	peer := identity(r)
	if r.URL.Path == "/v1/evidence" {
		if !allowed(peer, n.Config.EvidencePeers) {
			problem(w, 403, errors.New("evidence peer forbidden"))
			return
		}
		var q struct {
			Sandbox  string `json:"sandbox"`
			Action   string `json:"action"`
			Evidence string `json:"evidence"`
		}
		if e := decode(w, r, &q); e != nil {
			problem(w, 400, e)
			return
		}
		ids, e := n.Journal.Pending()
		if e != nil {
			problem(w, 503, e)
			return
		}
		for _, id := range ids {
			x, e := n.Journal.Get(id)
			if e == nil && x.Request.Instance == q.Sandbox && n.proof(x, q.Action) == q.Evidence {
				if e = n.verifyRuntime(x, q.Action); e != nil {
					problem(w, 409, e)
					return
				}
				w.WriteHeader(204)
				return
			}
		}
		problem(w, 403, errors.New("unknown evidence"))
		return
	}
	if !allowed(peer, n.Config.ControlPeers) {
		problem(w, 403, errors.New("control peer forbidden"))
		return
	}
	if n.Config.NativeMode && r.URL.Path == "/v1/native/cancel" && r.Method == "POST" {
		n.nativeCancel(w, r)
		return
	}
	if r.URL.Path == "/v1/info" && r.Method == "GET" {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		err := n.Agent.Call(ctx, "GET", base(n.Config.AgentURL)+"/health", nil, nil)
		info := NodeInfo{Name: n.Config.NodeName, UID: n.Config.NodeUID, BootID: n.BootID}
		if err == nil && n.Config.NativeMode {
			var defaults NativeDefaults
			err = n.Agent.Call(ctx, "GET", base(n.Config.AgentURL)+"/internal/storage/defaults", nil, &defaults)
			if err == nil {
				plugin, protocolErr := nativePluginIdentity(defaults)
				err = protocolErr
				if err == nil {
					info.Plugin = &plugin
					info.Defaults = defaults.Resources
				}
			}
		}
		info.Ready = err == nil && !n.isDraining()
		respond(w, 200, info)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "v1" || parts[1] != "operations" {
		problem(w, 404, ErrNotFound)
		return
	}
	x, e := n.Journal.Get(parts[2])
	if e != nil {
		problem(w, 404, e)
		return
	}

	if len(parts) == 3 && r.Method == "GET" {
		respond(w, 200, x)
		return
	}
	if len(parts) == 4 && r.Method == "POST" {
		switch parts[3] {
		case "stop":
			x, e = n.Journal.Update(parts[2], func(v *NodeRecord) error { v.Stop = true; return nil })
			if e == nil {
				n.Kick(parts[2])
			}
		case "release":
			if x.State == "Released" {
				respond(w, 200, x)
				return
			}
			if e = n.verifyRuntime(x, "detached"); e == nil && !n.nativeCompute(x) {
				e = n.SSF.Release(r.Context(), x.Request.Operation, x.Request.Instance, n.proof(x, "detached"), x.Request.Plan)
			}
			if e == nil {
				x, e = n.Journal.Update(parts[2], func(v *NodeRecord) error { v.State = "Released"; return nil })
			}
		default:
			e = ErrUnsupported
		}
		if e != nil {
			problem(w, 409, e)
			return
		}
		respond(w, 200, x)
		return
	}
	problem(w, 405, ErrUnsupported)
}
func (n *Node) Hooks(w http.ResponseWriter, r *http.Request) {
	if n.Config.NativeMode {
		if !n.authorizeNativeHook(r) {
			problem(w, 403, errors.New("storage hook authentication required"))
			return
		}
		if r.Method == "POST" && r.URL.Path == "/storage/volume-preparation" {
			n.volumePreparationHook(w, r)
			return
		}
		if r.Method == "POST" && r.URL.Path == "/storage/checkpoints" {
			n.nativeCheckpointControl(w, r)
			return
		}
		if r.Method == "POST" && r.URL.Path == "/storage/volumes" {
			n.nativeVolumeControl(w, r)
			return
		}
		if r.Method == "POST" && r.URL.Path == "/storage/control" {
			n.nativeLifecycle(w, r)
			return
		}
		if r.Method == "POST" && r.URL.Path == "/storage/compute" {
			n.nativeComputeAdmit(w, r)
			return
		}
		if r.Method == "POST" && r.URL.Path == "/storage/local-admit" {
			n.nativeLocalAdmit(w, r)
			return
		}
		if r.Method == "POST" && r.URL.Path == "/storage/admit" {
			n.nativeAdmit(w, r)
			return
		}
	}

	if r.Method != "POST" {
		problem(w, 405, ErrUnsupported)
		return
	}
	// Existing extension hooks coexist; only the storage protocol grants devices.
	if r.URL.Path == "/sandbox-hook/start-fresh" || r.URL.Path == "/sandbox-hook/start-resume" || r.URL.Path == "/sandbox-hook/stop" {
		respond(w, 200, map[string]any{})
		return
	}
	if r.URL.Path != "/storage/prepare" && r.URL.Path != "/storage/event" {
		problem(w, 404, ErrUnsupported)
		return
	}
	var ev RuntimeEvent
	if e := decode(w, r, &ev); e != nil {
		problem(w, 400, e)
		return
	}
	if e := n.acceptEvent(ev); e != nil {
		problem(w, 409, e)
		return
	}
	x, e := n.Journal.ByTicket(ev.Ticket)
	if e != nil {
		problem(w, 500, e)
		return
	}
	if x.Stop && ev.Event != "stopped" {
		n.Kick(x.Request.ID)
		problem(w, 409, errors.New("launch revoked"))
		return
	}
	if r.URL.Path == "/storage/prepare" {
		if len(x.Devices) == 0 && !n.nativeCompute(x) {
			problem(w, 409, errors.New("node volumes not prepared"))
			return
		}
		respond(w, 200, map[string]any{"devices": append([]Device{}, x.Devices...), "resources": x.Request.Resources})
		return
	}
	if n.Config.NativeMode && (ev.Event == "ready" || ev.Event == "stopped") {
		if e = n.Work(r.Context(), x.Request.ID); e == nil {
			e = n.syncNativeControl(r.Context(), x.Request.ID)
		}
		if e != nil {
			problem(w, 409, e)
			return
		}
	} else {
		n.Kick(x.Request.ID)
	}
	respond(w, 200, map[string]bool{"accepted": true})
}

// Notifications accelerate the normal request path; durable polling remains
// only the recovery fallback after a notification or process is lost.
func (n *Node) signal(id string) {
	if ch, ok := n.changed.LoadAndDelete(id); ok {
		close(ch.(chan struct{}))
	}
}
func (n *Node) wait(ctx context.Context, id string, prior NodeRecord) NodeRecord {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		ch, _ := n.changed.LoadOrStore(id, make(chan struct{}))
		r, e := n.Journal.Get(id)
		if e == nil {
			prior = r
			if r.State == "Ready" || r.State == "Stopped" || r.State == "Released" || r.Reason != "" {
				return r
			}
		}
		select {
		case <-ctx.Done():
			return prior
		case <-ch.(chan struct{}):
		}
	}
}

// A native transition is fsynced before its callback. Scan only the trusted
// runtime journal to recover a lost initial registration response.
func (n *Node) discover(ticket string) (RuntimeEvent, bool, error) {
	var found RuntimeEvent
	entries, e := os.ReadDir(n.Config.RuntimeRoot)
	if os.IsNotExist(e) {
		return found, false, nil
	}
	if e != nil {
		return found, false, e
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		ev, e := n.nativeEvent(strings.TrimSuffix(entry.Name(), ".json"))
		if e != nil {
			return found, false, e
		}
		if ev.Ticket != ticket {
			continue
		}
		if found.InstanceID != "" && found.InstanceID != ev.InstanceID {
			return found, false, errors.New("multiple native instances for one ticket; retain references")
		}
		found = ev
	}
	return found, found.InstanceID != "", nil
}
