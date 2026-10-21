package consume

import (
	"context"
	"ssf.dev/ssf/core"
	"time"
)

func (c *Coordinator) Resume(ctx context.Context, operation string) (Result, error) {
	var q Request
	if e := c.Ledger.ReadStep(ctx, "request/"+operation, &q); e != nil {
		return Result{Operation: operation}, e
	}
	return c.Prepare(ctx, q)
}

// RecoverPending re-enters the project's durable lifecycle handler; it does not
// manufacture scheduler reservations or runtime evidence. The same operation
// ID must be used by normal requests and this handler.
func (c *Coordinator) RecoverPending(ctx context.Context, handle func(context.Context, core.Operation) error) error {
	ids, e := c.Ledger.Pending(ctx)
	if e != nil {
		return e
	}
	for _, id := range ids {
		op, e := c.Ledger.Operation(ctx, id)
		if e != nil {
			return e
		}
		if e = handle(ctx, op); e != nil {
			_ = c.Ledger.SetOperation(ctx, id, op.State, e.Error())
		}
	}
	return nil
}
func (c *Coordinator) RunRecovery(ctx context.Context, interval time.Duration, handle func(context.Context, core.Operation) error) error {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if e := c.RecoverPending(ctx, handle); e != nil {
			return e
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
func (c *Coordinator) Reclaim(ctx context.Context, uid, action string) (string, error) {
	if e := c.Drain(ctx, uid, action); e != nil {
		return "", e
	}
	v, e := c.Ledger.Lookup(ctx, uid)
	if e != nil {
		return "", e
	}
	return c.Kube.Reclaim(ctx, v, action, c.Ledger)
}
