package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

// RecoverNative never repeats a public create request. It completes durable
// preparation, installs node revocation tombstones for expired unknown dispatch,
// and lets the original Orchestrator/process evidence decide when release is safe.
func (c *NativeControl) RecoverNative(ctx context.Context, grace time.Duration) {
	if c.Preparation != nil {
		done := make(chan struct{})
		prepctx, cancel := context.WithCancel(ctx)
		go func() {
			defer close(done)
			c.Preparation.Run(prepctx, 10*time.Second, func(err error) { slog.Warn("volume preparation retained work", "error", err) })
		}()
		defer func() { cancel(); <-done }()
	}
	if grace <= 0 {
		grace = 10 * time.Minute
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := c.recoverNativePass(ctx, grace); err != nil && ctx.Err() == nil {
			slog.Warn("native storage recovery retained work", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (c *NativeControl) recoverNativePass(ctx context.Context, grace time.Duration) error {
	if err := c.recoverNativeCloneGC(ctx); err != nil {
		return err
	}
	if err := c.recoverNativeCheckpointGC(ctx); err != nil {
		return err
	}
	if err := c.recoverNativeReferences(ctx); err != nil {
		return err
	}
	if err := c.recoverNativeTemplateConsumers(ctx); err != nil {
		return err
	}
	rows, err := c.Ledger.Session(ctx).Query(ctx, `SELECT id,state,intent,owner_uid,run_uid FROM agentenv_native_runs WHERE state IN ('Cancelling','Unpublished') OR (state IN ('Intent','Dispatched') AND updated < now()-($1 * interval '1 second')) ORDER BY updated LIMIT 100`, int64(grace/time.Second))
	if err != nil {
		return err
	}
	type pending struct {
		id, state, owner, run string
		intent                NativeIntent
	}
	var pendingRuns []pending
	for rows.Next() {
		var p pending
		var raw []byte
		if err = rows.Scan(&p.id, &p.state, &raw, &p.owner, &p.run); err != nil {
			rows.Close()
			return err
		}
		if err = json.Unmarshal(raw, &p.intent); err != nil {
			rows.Close()
			return err
		}
		pendingRuns = append(pendingRuns, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range pendingRuns {
		work, cancel := context.WithTimeout(ctx, 30*time.Second)
		if p.state == "Unpublished" {
			err = c.abortNativeUnpublished(work, p.id)
		} else if p.state == "Intent" {
			err = c.recoverNativeIntent(work, p.intent, p.owner, p.run, grace)
		} else {
			err = c.cancelExpiredDispatch(work, p.id, grace)
		}
		cancel()
		if err != nil {
			slog.Warn("native run recovery pending", "request_id", p.id, "error", err)
		}
	}
	rows, err = c.Ledger.Session(ctx).Query(ctx, `SELECT tenant,name FROM agentenv_native_volumes WHERE descriptor->>'state' IN ('Deleting','Expanding') OR coalesce((descriptor->>'expansionRequested')::bigint,0)>0 OR coalesce(descriptor->>'sourcePin','')<>'' LIMIT 100`)
	if err != nil {
		return err
	}
	var names [][2]string
	for rows.Next() {
		var name [2]string
		if err = rows.Scan(&name[0], &name[1]); err != nil {
			rows.Close()
			return err
		}
		names = append(names, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, name := range names {
		work, cancel := context.WithTimeout(ctx, 30*time.Second)
		volume, readErr := c.Volumes.Get(work, name[0], name[1])
		if readErr != nil {
			err = readErr
		} else if volume.ExpansionRequested > 0 {
			_, err = c.Volumes.Expand(work, name[0], name[1], volume.ExpansionRequested)
		} else if volume.State == "Deleting" {
			err = c.Volumes.Delete(work, name[0], name[1])
		} else {
			_, err = c.Volumes.Observe(work, name[0], name[1])
		}
		cancel()
		if err != nil {
			slog.Warn("native restore source retained", "volume", name[1], "error", err)
		}
	}
	return nil
}
func (c *NativeControl) recoverNativeIntent(ctx context.Context, q NativeIntent, owner, run string, grace time.Duration) error {
	op := core.Hash(q.Tenant + "\x00" + run + "\x00" + q.RequestID)
	admission := &NativeAdmission{Ledger: c.Ledger, Refresh: c.Refresh}
	reservation, err := admission.load(ctx, op)
	if errors.Is(err, pgx.ErrNoRows) {
		reservation, err = admission.load(ctx, "native/"+q.RequestID)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return c.Ledger.Exclusive(ctx, "agentenv/native-run/"+q.RequestID, func(ctx context.Context) error {
			var expired bool
			if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state='Intent' AND updated < now()-($2 * interval '1 second') FROM agentenv_native_runs WHERE id=$1`, q.RequestID, int64(grace/time.Second)).Scan(&expired); err != nil {
				return err
			}
			if !expired {
				return nil
			}
			// The foreground handler may have reserved placement after our
			// initial read. Recheck while holding its native-run lock.
			for _, reservationID := range []string{op, "native/" + q.RequestID} {
				if _, err := admission.load(ctx, reservationID); err == nil {
					return core.ErrPending
				} else if !errors.Is(err, pgx.ErrNoRows) {
					return err
				}
			}
			bindings, err := c.Ledger.Bindings(ctx, op)
			if err != nil {
				return err
			}
			if len(bindings) > 0 {
				return core.ErrPending
			}
			// No reservation means Prepare has not reached binding selection or Attach.
			next := "Unstarted"
			if q.ResumeOf == "" {
				next = "Revoked"
				if err = c.Ledger.ReleaseUse(ctx, owner); err != nil {
					return err
				}
			}
			if err = c.Ledger.SetOperation(ctx, op, "Released", ""); err != nil {
				return err
			}
			_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state=$2,updated=now() WHERE id=$1`, q.RequestID, next)
			return err
		})
	}
	if err != nil {
		return err
	}
	if reservation.State == "Released" {
		return core.ErrConflict
	}
	_, err = c.Prepare(ctx, q, []NativeCandidate{reservation.Candidate}, func(cs []NativeCandidate) (NativeCandidate, error) {
		if len(cs) != 1 {
			return NativeCandidate{}, core.ErrConflict
		}
		return cs[0], nil
	})
	return err
}
func (c *NativeControl) cancelExpiredDispatch(ctx context.Context, id string, grace time.Duration) error {
	cancelRun := false
	err := c.Ledger.Exclusive(ctx, "agentenv/native-run/"+id, func(ctx context.Context) error {
		tag, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Cancelling' WHERE id=$1 AND (state='Cancelling' OR (state='Dispatched' AND updated < now()-($2 * interval '1 second')))`, id, int64(grace/time.Second))
		if err == nil {
			cancelRun = tag.RowsAffected() == 1
		}
		return err
	})
	if err != nil || !cancelRun {
		return err
	}
	d, err := c.Dispatch(ctx, id)
	if err != nil {
		return err
	}
	endpoint := d.Candidate.PluginURL
	if c.RecoverStopped != nil {
		candidate, err := c.RecoverStopped(ctx, d.Candidate)
		if err != nil {
			return err
		}
		endpoint = candidate.PluginURL
	}
	var record NodeRecord
	return c.HTTP.Call(ctx, "POST", base(endpoint)+"/v1/native/cancel", d, &record)
}
