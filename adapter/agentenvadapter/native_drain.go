package agentenvadapter

import (
	"context"
	"time"
)

func (n *Node) isDraining() bool { n.workMu.Lock(); defer n.workMu.Unlock(); return n.draining }

// Drain keeps the evidence server and public SSF connection alive after the
// native VMM owner exits. Expiry retains WAL references; it is never stop proof.
func (n *Node) Drain(ctx context.Context) error {
	n.workMu.Lock()
	n.draining = true
	n.workMu.Unlock()
	done := make(chan struct{})
	go func() { n.workers.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	for {
		if err := n.importNativeCleanup(); err != nil {
			return err
		}
		ids, err := n.Journal.Pending()
		if err != nil {
			return err
		}
		content, err := n.Journal.pendingContent()
		if err != nil {
			return err
		}
		if len(ids) == 0 && content == 0 {
			return nil
		}
		for _, id := range ids {
			record, err := n.Journal.Get(id)
			if err != nil {
				return err
			}
			if record.Native != nil && record.Runtime.InstanceID == "" {
				// The journal transaction serializes against Prepare's association. Once
				// associated, only process evidence can stop a possibly launched runtime.
				_, err = n.Journal.Update(id, func(current *NodeRecord) error {
					if current.Runtime.InstanceID == "" {
						current.Stop = true
						current.Dispatched = false
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			if err = n.Work(ctx, id); err == nil {
				_ = n.syncNativeControl(ctx, id)
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
