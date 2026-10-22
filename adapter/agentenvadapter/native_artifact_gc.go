package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
)

// RetireNativeCapture is requested by the original artifact owner, never by a
// lease timeout. Every incarnation of that logical owner must be terminal.
// Native artifact pins held by templates/children still prevent local cleanup.
func (c *NativeControl) RetireNativeCapture(ctx context.Context, request, node, barrier string) (bool, error) {
	if !safeID.MatchString(barrier) {
		return false, core.ErrConflict
	}
	collectible := false
	err := c.Ledger.Exclusive(ctx, "agentenv/native-run/"+request, func(ctx context.Context) error {
		d, err := c.Dispatch(ctx, request)
		if err != nil {
			return err
		}
		if _, err = c.physicalCandidate(ctx, d.Candidate, node); err != nil {
			return err
		}
		var pendingDirect bool
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agentenv_native_direct_captures c JOIN ssf_direct_snapshots s ON s.id=c.id WHERE c.request_id=$1 AND c.barrier=$2 AND NOT s.retired)`, request, barrier).Scan(&pendingDirect); err != nil {
			return err
		}
		if pendingDirect {
			return core.ErrPending
		}
		var terminal bool
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT r.state IN ('Released','Revoked','Resumed') AND NOT EXISTS(SELECT 1 FROM agentenv_native_runs other WHERE other.owner_uid=r.owner_uid AND other.state NOT IN ('Released','Revoked','Resumed')) FROM agentenv_native_runs r WHERE r.id=$1 AND r.intent->>'tenant'=$2`, request, c.Principal.Tenant).Scan(&terminal); err != nil {
			return err
		}
		if !terminal {
			return core.ErrPending
		}
		if _, err = c.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_retired_captures(request_id,barrier_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, request, barrier); err != nil {
			return err
		}
		id := core.Hash(request + "/capture/" + barrier)
		cp, err := c.Ledger.Checkpoint(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			collectible = true
			return nil
		}
		if err != nil {
			return err
		}
		var manifest consume.CaptureManifest
		if err = json.Unmarshal(cp.Descriptor, &manifest); err != nil {
			return err
		}
		intent := manifest.Intent
		if intent.ID == "" {
			if err = json.Unmarshal(cp.Descriptor, &intent); err != nil {
				return err
			}
		}
		if intent.ID != id || intent.RunUID != d.RunUID || intent.OwnerUID != d.OwnerUID || intent.BarrierID != barrier {
			return core.ErrConflict
		}
		if err = c.Ledger.ReleaseCheckpoint(ctx, id); err != nil {
			return err
		}
		native := manifest.NativeResourceUID
		if native == "" {
			native = "native-artifact:" + core.Hash(intent.OwnerUID+"/"+intent.RunUID+"/"+intent.NativeArtifact+"/"+intent.NativeDigest)
		}
		if err = c.Ledger.PermitSnapshotDelete(ctx, native); err != nil {
			return err
		}
		collectible = true
		return nil
	})
	return collectible, err
}
