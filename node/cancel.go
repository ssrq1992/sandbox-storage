package node

import (
	"context"
	"errors"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

// CancelUnstarted installs a tombstone before acknowledging a plan that has not
// reached this node. A delayed Prepare can therefore never resurrect it.
func (e *Engine) CancelUnstarted(ctx context.Context, id, sandbox, token, proof string) error {
	p, err := auth.VerifyRevocation(token, e.Key, e.NodeID, e.NodeUID, e.BootID)
	if err != nil {
		return err
	}
	if p.Purpose != "" || p.Operation != id || p.Sandbox != sandbox {
		return core.ErrConflict
	}
	unlock := e.locks.lock(p)
	defer unlock()
	_, err = e.WAL.Get(id)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	if e.Evidence == nil {
		return errors.New("runtime detachment verifier required")
	}
	if err = e.Evidence.Verify(ctx, sandbox, "detached", proof); err != nil {
		return err
	}
	return e.WAL.Save(Record{Plan: p, State: "Released", Steps: map[string]string{}})
}
