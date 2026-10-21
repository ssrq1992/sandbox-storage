package consume

import (
	"context"
	"errors"
	"time"

	"ssf.dev/ssf/core"
)

type ReadyReceipt struct{ PlanDigest, Instance, EvidenceDigest string }

func readyPlanDigest(p core.Plan) string { p.Expires = time.Time{}; return core.Digest(p) }

// ConfirmReady never trusts a caller's boolean. Evidence must be re-observed by
// the registered node/runtime verifier before the durable receipt is recorded.
func (c *Coordinator) ConfirmReady(ctx context.Context, r Result, instance, proof string) error {
	if c.Evidence == nil || instance == "" || proof == "" {
		return errors.New("trusted runtime evidence required")
	}
	return c.Ledger.Exclusive(ctx, "operation/"+r.Operation, func(ctx context.Context) error {
		o, err := c.Ledger.Operation(ctx, r.Operation)
		if err != nil {
			return err
		}
		if o.State != "Planned" && o.State != "Ready" {
			return core.ErrConflict
		}
		if r.Plan.Operation != r.Operation || instance != r.Plan.Sandbox {
			return core.ErrConflict
		}
		var persisted core.Plan
		if err = c.Ledger.ReadStep(ctx, "plan/"+r.Operation, &persisted); err != nil {
			return err
		}
		if readyPlanDigest(persisted) != readyPlanDigest(r.Plan) {
			return core.ErrConflict
		}
		if err = c.Evidence.Verify(ctx, r.Plan.Sandbox, "ready", proof); err != nil {
			return err
		}
		receipt := ReadyReceipt{readyPlanDigest(r.Plan), instance, core.Hash(proof)}
		return c.Ledger.Step(ctx, "ready/"+r.Operation, core.Digest(receipt), func(context.Context) (any, error) { return receipt, nil }, nil)
	})
}

// RetainPaused is invoked before the native runtime releases its devices.
func (c *Coordinator) RetainPaused(ctx context.Context, op string) error {
	return c.Ledger.RetainUse(ctx, op, "paused")
}

func (c *Coordinator) ConfirmStopped(ctx context.Context, r Result, proof string) error {
	if c.Evidence == nil || proof == "" {
		return errors.New("trusted stop evidence required")
	}
	var persisted core.Plan
	if err := c.Ledger.ReadStep(ctx, "plan/"+r.Operation, &persisted); err != nil {
		return err
	}
	if persisted.Operation != r.Operation || readyPlanDigest(persisted) != readyPlanDigest(r.Plan) {
		return core.ErrConflict
	}
	if err := c.Evidence.Verify(ctx, r.Plan.Sandbox, "detached", proof); err != nil {
		return err
	}
	owner, run := r.Plan.OwnerUID, r.Plan.RunUID
	if owner == "" {
		owner = r.Plan.Sandbox
	}
	if run == "" {
		run = r.Plan.Sandbox
	}
	return c.Ledger.StopRun(ctx, owner, run)
}
