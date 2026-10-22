package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

var ErrNoLiveNativeCapture = errors.New("no live native capture selected")

// NativeStorageCapture is a request for the original runtime's barrier. It
// carries no device path, CSI command or permission to create/delete a VM.
type NativeStorageCapture struct {
	ID        string                 `json:"id"`
	RequestID string                 `json:"request_id"`
	OwnerUID  string                 `json:"owner_uid"`
	RunUID    string                 `json:"run_uid"`
	NativeID  string                 `json:"native_id"`
	NodeID    string                 `json:"node_id"`
	BindingID string                 `json:"binding_id"`
	Barrier   string                 `json:"barrier"`
	Source    core.DirectSnapshotRef `json:"source"`
	Capture   *NativeCapture         `json:"capture,omitempty"`
}

func (c *NativeControl) directCaptureTask(ctx context.Context, id string) (NativeStorageCapture, error) {
	var task NativeStorageCapture
	err := c.Ledger.Exclusive(ctx, "direct-snapshot/"+id, func(ctx context.Context) error {
		ref, err := c.Ledger.DirectSnapshot(ctx, id)
		if err != nil {
			return err
		}
		if !c.Principal.Allows(ref.Namespace) {
			return core.ErrConflict
		}
		var raw []byte
		err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT descriptor FROM agentenv_native_direct_captures WHERE id=$1`, id).Scan(&raw)
		if err == nil {
			if err = json.Unmarshal(raw, &task); err != nil {
				return err
			}
			if core.Digest(task.Source) != core.Digest(ref) {
				return core.ErrConflict
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var dispatch []byte
		err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT r.id,r.native_id,b.id,r.dispatch FROM agentenv_native_runs r JOIN ssf_bindings b ON b.operation_id=r.dispatch->'result'->>'Operation' WHERE r.state='Ready' AND r.native_id<>'' AND r.intent->>'tenant'=$1 AND b.volume_key=$2 AND b.state='Acquired' ORDER BY r.id,b.id LIMIT 1`, c.Principal.Tenant, ref.Volume.Key()).Scan(&task.RequestID, &task.NativeID, &task.BindingID, &dispatch)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoLiveNativeCapture
		}
		if err != nil {
			return err
		}
		var d NativeDispatch
		if err = json.Unmarshal(dispatch, &d); err != nil {
			return err
		}
		found := false
		for _, b := range d.Result.Plan.Bindings {
			if b.ID == task.BindingID && b.Volume.Key() == ref.Volume.Key() && b.Volume.PVC == ref.Volume.PVC {
				found = true
			}
		}
		if !found {
			return core.ErrConflict
		}
		task.ID = id
		task.Barrier = "external_" + core.Hash(id)
		task.OwnerUID = d.OwnerUID
		task.RunUID = d.RunUID
		task.NodeID = d.Candidate.ID
		task.Source = ref
		raw, err = json.Marshal(task)
		if err != nil {
			return err
		}
		_, err = c.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_direct_captures(id,request_id,barrier,binding_id,descriptor) VALUES($1,$2,$3,$4,$5)`, id, task.RequestID, task.Barrier, task.BindingID, raw)
		return err
	})
	return task, err
}

func (c *NativeControl) completedDirectCapture(ctx context.Context, t NativeStorageCapture) (core.SnapshotCapture, error) {
	var result core.SnapshotCapture
	checkpoint := core.Hash(t.RequestID + "/capture/" + t.Barrier)
	cp, err := c.Ledger.Checkpoint(ctx, checkpoint)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, core.ErrPending
	}
	if err != nil {
		return result, err
	}
	if cp.State != "Ready" {
		return result, core.ErrPending
	}
	result, err = c.Ledger.SnapshotCapture(ctx, t.Source.Namespace, "ssf-cp-"+core.Hash(checkpoint + "/" + t.BindingID)[:40])
	if err != nil {
		return result, err
	}
	if result.CheckpointID != checkpoint || result.BindingID != t.BindingID || result.PVC != t.Source.Volume.PVC || result.Driver != t.Source.Volume.Driver || result.Handle != t.Source.Volume.Handle {
		return result, core.ErrConflict
	}
	if err = c.Ledger.RetainDirectCheckpoint(ctx, t.ID, checkpoint); err != nil {
		return result, err
	}
	return result, nil
}

func (c *NativeControl) DirectCapture(ctx context.Context, id string) (core.SnapshotCapture, error) {
	var out core.SnapshotCapture
	if id == "" {
		return out, core.ErrConflict
	}
	task, err := c.directCaptureTask(ctx, id)
	if err != nil {
		return out, err
	}
	if result, e := c.completedDirectCapture(ctx, task); !errors.Is(e, core.ErrPending) {
		return result, e
	}
	checkpoint := core.Hash(task.RequestID + "/capture/" + task.Barrier)
	if cp, e := c.Ledger.Checkpoint(ctx, checkpoint); e == nil && cp.State == "Capturing" {
		var intent core.CaptureIntent
		if err = json.Unmarshal(cp.Descriptor, &intent); err != nil {
			return out, err
		}
		if intent.ID != checkpoint || intent.OwnerUID != task.OwnerUID || intent.RunUID != task.RunUID || intent.BarrierID != task.Barrier {
			return out, core.ErrConflict
		}
		receipt := NativeCapture{BarrierID: intent.BarrierID, Artifact: intent.NativeArtifact, Digest: intent.NativeDigest}
		if !receipt.valid() {
			return out, core.ErrConflict
		}
		task.Capture = &receipt
	} else if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return out, e
	}
	d, err := c.Dispatch(ctx, task.RequestID)
	if err != nil {
		return out, err
	}
	node, err := c.physicalCandidate(ctx, d.Candidate, task.NodeID)
	if err != nil {
		return out, err
	}
	if node.BootID != d.Candidate.BootID {
		return out, core.ErrConflict
	}
	if c.HTTP == nil {
		return out, core.ErrPending
	}
	// Never hold the run/checkpoint lock while invoking native capture: its
	// trusted callbacks need those same locks to seal and publish participants.
	if err = c.HTTP.Call(ctx, "POST", base(node.PluginURL)+"/v1/native/capture", task, nil); err != nil {
		if result, e := c.completedDirectCapture(ctx, task); e == nil {
			return result, nil
		}
		return out, err
	}
	return c.completedDirectCapture(ctx, task)
}

func (c *NativeControl) AuthorizeDirectCapture(ctx context.Context, request, node, owner, run, barrier string) error {
	var raw []byte
	if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT descriptor FROM agentenv_native_direct_captures WHERE request_id=$1 AND barrier=$2`, request, barrier).Scan(&raw); err != nil {
		return err
	}
	var t NativeStorageCapture
	if err := json.Unmarshal(raw, &t); err != nil {
		return err
	}
	if t.RequestID != request || t.NodeID != node || t.OwnerUID != owner || t.RunUID != run {
		return core.ErrConflict
	}
	if _, err := c.Ledger.DirectSnapshot(ctx, t.ID); err != nil {
		return err
	}
	var ready bool
	if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state='Ready' AND native_id=$2 FROM agentenv_native_runs WHERE id=$1`, request, t.NativeID).Scan(&ready); err != nil {
		return err
	}
	if !ready {
		return core.ErrConflict
	}
	return nil
}

func (n *Node) nativeStorageCapture(w http.ResponseWriter, r *http.Request) {
	var q NativeStorageCapture
	if err := decode(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	record, err := n.Journal.Get(q.RequestID)
	if err != nil || record.Native == nil || record.Stop || record.DeleteRequested || record.State != "Ready" || record.Runtime.SandboxID != q.NativeID || record.Native.OwnerUID != q.OwnerUID || record.Native.RunUID != q.RunUID || q.NodeID != n.Config.NativeNodeID || q.Barrier != "external_"+core.Hash(q.ID) {
		problem(w, 409, core.ErrConflict)
		return
	}
	if q.Capture != nil {
		if q.Capture.BarrierID != q.Barrier || !q.Capture.valid() || record.SealedCaptures[q.Barrier] != core.Digest(q.Capture) || n.Control == nil {
			problem(w, 409, core.ErrPending)
			return
		}
		data, _ := json.Marshal(map[string]any{"request_id": q.RequestID, "capture": q.Capture, "proof": n.captureProof(record, *q.Capture)})
		if _, err = n.Control(r.Context(), NativeCommand{Action: "capture", NodeID: q.NodeID, OwnerID: q.OwnerUID, RunID: q.RunUID, Data: data}); err != nil {
			problem(w, 409, err)
			return
		}
		respond(w, 200, map[string]bool{"accepted": true})
		return
	}
	var defaults NativeDefaults
	if n.Agent == nil {
		problem(w, 409, core.ErrPending)
		return
	}
	if err = n.Agent.Call(r.Context(), "GET", base(n.Config.AgentURL)+"/internal/storage/defaults", nil, &defaults); err != nil {
		problem(w, 409, err)
		return
	}
	if !defaults.StorageCaptureRequests {
		problem(w, 409, errors.New("native storage capture request capability unavailable"))
		return
	}
	if err = n.Agent.Call(r.Context(), "POST", base(n.Config.AgentURL)+"/internal/storage/captures", q, nil); err != nil {
		problem(w, 409, err)
		return
	}
	respond(w, 200, map[string]bool{"accepted": true})
}
