package agentenvadapter

import (
	"context"
	"encoding/json"

	"ssf.dev/ssf/core"
)

// abortNativeUnpublished is only for a persisted state that never crossed the
// native dispatch boundary. It must never compensate Dispatched/Cancelling by
// assuming that a timed-out call did not start a VM.
func (c *NativeControl) abortNativeUnpublished(ctx context.Context, id string) error {
	return c.Ledger.Exclusive(ctx, "agentenv/native-run/"+id, func(ctx context.Context) error {
		var state string
		var raw []byte
		if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state,intent FROM agentenv_native_runs WHERE id=$1`, id).Scan(&state, &raw); err != nil {
			return err
		}
		if state == "Revoked" || state == "Unstarted" {
			return nil
		}
		if state != "Unpublished" {
			return core.ErrConflict
		}
		var intent NativeIntent
		if err := json.Unmarshal(raw, &intent); err != nil {
			return err
		}
		d, err := c.Dispatch(ctx, id)
		if err != nil {
			return err
		}
		if d.Result.Operation != "" {
			err = c.coordinator(nil, "").AbortUnpublished(ctx, d.Result.Operation, func(ctx context.Context) error {
				var current string
				if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, id).Scan(&current); err != nil {
					return err
				}
				if current != "Unpublished" {
					return core.ErrConflict
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		if err = c.Ledger.StopRun(ctx, d.OwnerUID, d.RunUID); err != nil {
			return err
		}
		admission := NativeAdmission{Ledger: c.Ledger}
		if err = admission.Release(ctx, d.Reservation); err != nil {
			return err
		}
		next := "Unstarted"
		if intent.ResumeOf == "" {
			if err = c.Ledger.ReleaseUse(ctx, d.OwnerUID); err != nil {
				return err
			}
			next = "Revoked"
		}
		_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state=$2,updated=now() WHERE id=$1 AND state='Unpublished'`, id, next)
		return err
	})
}
