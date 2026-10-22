package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"ssf.dev/ssf/core"
)

// NativeCommandResult is transport-neutral. The independent coordinator
// authenticates the caller and signs any returned dispatch.
type NativeCommandResult struct {
	Data     any
	Dispatch *NativeDispatch
}

func (c *NativeControl) Command(ctx context.Context, command NativeCommand, candidates []NativeCandidate, choose func([]NativeCandidate) (NativeCandidate, error)) (NativeCommandResult, error) {
	out := NativeCommandResult{Data: map[string]bool{"accepted": true}}
	var event struct {
		EverReady    bool            `json:"ever_ready"`
		RequestID    string          `json:"request_id"`
		NativeID     string          `json:"native_id"`
		RepositoryID string          `json:"repository_id"`
		Checkpoint   json.RawMessage `json:"checkpoint"`
		Resources    Resources       `json:"resources"`
		Proof        string          `json:"proof"`
		Capture      NativeCapture   `json:"capture"`
		Bytes        int64           `json:"bytes"`
		Name         string          `json:"name"`
	}
	if len(command.Data) > 0 {
		if err := json.Unmarshal(command.Data, &event); err != nil {
			return out, err
		}
	}
	switch command.Action {
	case "ready", "pause", "cancel_pause", "resume", "fork", "delete", "stopped", "capture", "capture.seal", "capture.request", "capture.retire", "cleanup.status":
		prior, err := c.Dispatch(ctx, event.RequestID)
		if err != nil {
			return out, err
		}
		if command.OwnerID != prior.OwnerUID || command.RunID != prior.RunUID {
			return out, core.ErrConflict
		}
		if event.EverReady && (command.Action == "delete" || command.Action == "stopped" || command.Action == "cleanup.status") {
			if _, err = c.physicalCandidate(ctx, prior.Candidate, command.NodeID); err != nil {
				return out, err
			}
			// Conservative protection only, never a Ready/Commit authorization.
			// Preserve a volume if native readiness raced final delete delivery.
			if _, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET ever_ready=true WHERE id=$1`, event.RequestID); err != nil {
				return out, err
			}
		}
	}
	var err error
	switch command.Action {
	case "cleanup.status":
		dispatch, e := c.Dispatch(ctx, event.RequestID)
		if e != nil {
			return out, e
		}
		if _, e = c.physicalCandidate(ctx, dispatch.Candidate, command.NodeID); e != nil {
			return out, e
		}
		var terminal, everReady, ephemeral bool
		err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT r.state IN ('Released','Revoked','Resumed') AND NOT EXISTS(SELECT 1 FROM agentenv_native_runs other WHERE other.owner_uid=r.owner_uid AND other.state NOT IN ('Released','Revoked','Resumed')), EXISTS(SELECT 1 FROM agentenv_native_runs ready WHERE ready.owner_uid=r.owner_uid AND ready.ever_ready), coalesce(r.intent->>'path'='/internal/compute',false) FROM agentenv_native_runs r WHERE r.id=$1 AND r.intent->>'tenant'=$2`, event.RequestID, c.Principal.Tenant).Scan(&terminal, &everReady, &ephemeral)
		out.Data = map[string]bool{"terminal": terminal, "ever_ready": everReady, "ephemeral": ephemeral}
	case "checkpoint.publish":
		out.Data, err = c.BeginNativePublication(ctx, event.Checkpoint, event.NativeID, event.RepositoryID)
	case "checkpoint.published":
		err = c.NativePublished(ctx, event.NativeID, event.RepositoryID)
	case "checkpoint.delete_begin":
		var managed bool
		managed, err = c.BeginNativePublicationDelete(ctx, event.NativeID, event.RepositoryID)
		out.Data = map[string]bool{"managed": managed}
	case "checkpoint.deleted":
		err = c.CompleteNativePublicationDelete(ctx, event.NativeID, event.RepositoryID)
	case "capture.retire":
		var collectible bool
		collectible, err = c.RetireNativeCapture(ctx, event.RequestID, command.NodeID, event.Capture.BarrierID)
		out.Data = map[string]bool{"collectible": collectible}
		if errors.Is(err, core.ErrPending) {
			err = nil
		}
	case "capture.request":
		err = c.AuthorizeDirectCapture(ctx, event.RequestID, command.NodeID, command.OwnerID, command.RunID, event.Capture.BarrierID)
	case "capture", "capture.seal":
		manifest, captureErr := c.capturePhase(ctx, event.RequestID, command.NodeID, event.Capture, event.Proof, command.Action == "capture.seal")
		if errors.Is(captureErr, core.ErrPending) {
			out.Data = map[string]any{"ready": false}
		} else {
			out.Data = map[string]any{"ready": captureErr == nil, "checkpoint": manifest}
			err = captureErr
		}
	case "ready":
		err = c.Ready(ctx, event.RequestID, command.NodeID, event.NativeID, event.Proof)
	case "pause":
		err = c.RetainPause(ctx, event.RequestID, command.NodeID)
	case "cancel_pause":
		err = c.CancelPause(ctx, event.RequestID, command.NodeID)
	case "resume", "fork", "compute":
		var dispatch NativeDispatch
		switch command.Action {
		case "resume":
			dispatch, err = c.Resume(ctx, event.RequestID, command.NodeID, event.NativeID, candidates, choose)
		case "fork":
			dispatch, err = c.Fork(ctx, event.RequestID, command.NodeID, event.NativeID, event.Checkpoint, candidates, choose)
		case "compute":
			dispatch, err = c.Compute(ctx, command.NodeID, event.NativeID, event.Resources, event.Checkpoint, candidates, choose)
		}
		if err == nil {
			out.Dispatch = &dispatch
		}
	case "delete":
		err = c.BeginDelete(ctx, event.RequestID, command.NodeID)
	case "stopped":
		err = c.Stopped(ctx, event.RequestID, command.NodeID, event.Proof)
	case "volumes.claim_legacy":
		err = c.Volumes.ClaimLegacy(ctx, c.Principal.Tenant, event.Name)
	case "volumes.expand":
		var volume NativeVolume
		volume, err = c.Volumes.Expand(ctx, c.Principal.Tenant, event.Name, event.Bytes)
		out.Data = volume.View()
		if errors.Is(err, core.ErrPending) {
			err = nil
		}
	case "volumes.delete":
		err = c.Volumes.Delete(ctx, c.Principal.Tenant, event.Name)
	case "volumes.lookup":
		out.Data, err = c.Volumes.Lookup(ctx, c.Principal.Tenant, event.Name)
	case "volumes.get":
		out.Data, err = c.Volumes.Get(ctx, c.Principal.Tenant, event.Name)
	case "volumes.list":
		var volumes []NativeVolume
		volumes, err = c.Volumes.List(ctx, c.Principal.Tenant)
		views := make([]NativeVolumeView, 0, len(volumes))
		for _, volume := range volumes {
			views = append(views, volume.View())
		}
		out.Data = views
	case "volumes.create":
		var request NativeVolumeRequest
		if err = json.Unmarshal(command.Data, &request); err == nil {
			var volume NativeVolume
			volume, err = c.Volumes.Create(ctx, c.Principal, request)
			out.Data = volume.View()
		}
	default:
		err = ErrUnsupported
	}
	return out, err
}
