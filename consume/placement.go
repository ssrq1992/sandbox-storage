package consume

import (
	"context"
	"errors"
	"ssf.dev/ssf/core"
	"time"
)

func (c *Coordinator) validatePlacement(ctx context.Context, p *core.Placement) error {
	if p.ID == "" || p.Node == "" || p.BootID == "" || p.NodeUID == "" {
		return errors.New("complete placement identity required")
	}
	e := c.Placement.Validate(ctx, *p)
	if e == nil && time.Now().Before(p.Expires) {
		return nil
	}
	next, e := c.Placement.Renew(ctx, *p)
	if e != nil {
		return e
	}
	if next.ID != p.ID || next.Node != p.Node || next.NodeUID != p.NodeUID || next.BootID != p.BootID || !time.Now().Before(next.Expires) {
		return errors.New("renew must retain selected node, boot and lease identity")
	}
	if e = c.Placement.Validate(ctx, next); e != nil {
		return e
	}
	*p = next
	return nil
}
