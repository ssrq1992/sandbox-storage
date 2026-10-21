package agentenvadapter

import (
	"context"
	"errors"
	"log/slog"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
)

// A retained restore/Fork reference follows the logical owner through resume.
// Unstarted is deliberately excluded: that request may be retried with a fresh
// run. Unknown dispatch, pause and failed-resume ownership never expire by time.
func (c *NativeControl) recoverNativeReferences(ctx context.Context) error {
	rows, err := c.Ledger.Session(ctx).Query(ctx, `SELECT r.id,cp.id FROM agentenv_native_runs r JOIN ssf_checkpoint_sets cp ON cp.owner_uid IN ($1||'/restore/'||r.id,$1||'/fork/'||r.id) WHERE r.intent->>'tenant'=$1 AND cp.state='Ready' AND r.state IN ('Released','Revoked','Resumed') AND NOT EXISTS(SELECT 1 FROM agentenv_native_runs other WHERE other.owner_uid=r.owner_uid AND other.state NOT IN ('Released','Revoked','Resumed')) LIMIT 100`, c.Principal.Tenant)
	if err != nil {
		return err
	}
	var refs [][2]string
	for rows.Next() {
		var ref [2]string
		if err = rows.Scan(&ref[0], &ref[1]); err != nil {
			rows.Close()
			return err
		}
		refs = append(refs, ref)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, ref := range refs {
		err = c.Ledger.Exclusive(ctx, "agentenv/native-run/"+ref[0], func(ctx context.Context) error {
			var terminal bool
			if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT r.state IN ('Released','Revoked','Resumed') AND NOT EXISTS(SELECT 1 FROM agentenv_native_runs other WHERE other.owner_uid=r.owner_uid AND other.state NOT IN ('Released','Revoked','Resumed')) FROM agentenv_native_runs r WHERE r.id=$1 AND r.intent->>'tenant'=$2`, ref[0], c.Principal.Tenant).Scan(&terminal); err != nil {
				return err
			}
			if !terminal {
				return core.ErrPending
			}
			if err := c.Ledger.ReleaseCheckpoint(ctx, ref[1]); err != nil {
				return err
			}
			_, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_template_consumers SET state='Released' WHERE tenant=$1 AND reference=$2`, c.Principal.Tenant, ref[1])
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Reap only captures belonging to this authenticated tenant. Retired references
// have no separate Kubernetes objects; their original capture owns reclamation.
func (c *NativeControl) recoverNativeCheckpointGC(ctx context.Context) error {
	rows, err := c.Ledger.Session(ctx).Query(ctx, `SELECT cp.id FROM ssf_checkpoint_gc gc JOIN ssf_checkpoint_sets cp ON cp.id=gc.checkpoint_id JOIN agentenv_native_runs r ON r.run_uid=coalesce(cp.descriptor->'intent'->>'run_uid',cp.descriptor->>'run_uid') WHERE gc.state='Pending' AND cp.state='Released' AND coalesce(cp.descriptor->'intent'->>'version',cp.descriptor->>'version')=$2 AND r.intent->>'tenant'=$1 ORDER BY gc.updated,cp.id LIMIT 100`, c.Principal.Tenant, consume.CaptureVersion)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	collector := consume.Checkpoints{Ledger: c.Ledger, Kube: c.Kube}
	for _, id := range ids {
		if err = collector.Reap(ctx, id); err != nil && !errors.Is(err, core.ErrPending) {
			slog.Warn("native checkpoint reclaim retained", "checkpoint", id, "error", err)
		}
	}
	return nil
}
