package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Reconcile resumes only operations whose irreversible prerequisites are
// already durable. It never creates devices, invents stop evidence, unmounts
// live targets, or retries a capture after an unverified thaw.
func (n *CSINode) Reconcile(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	var captures []CaptureRecord
	var releases []Session
	err := n.Operations.Sessions.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(capturesBucket); b != nil {
			if err := b.ForEach(func(_, raw []byte) error {
				var r CaptureRecord
				if err := json.Unmarshal(raw, &r); err != nil {
					return err
				}
				if r.Captured != nil && r.Published == nil {
					captures = append(captures, r)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return tx.Bucket(sessionsBucket).ForEach(func(_, raw []byte) error {
			s, err := decodeSession(raw)
			if err != nil {
				return err
			}
			if s.ReleaseStarted && s.ReleaseIntent != nil && s.State != "Released" {
				releases = append(releases, s)
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	var failures []error
	for _, r := range captures {
		if ctx.Err() != nil {
			return errors.Join(append(failures, ctx.Err())...)
		}
		if _, err = n.Operations.PublishCaptured(ctx, r.SessionID, r.Proof.CaptureID); err != nil {
			failures = append(failures, fmt.Errorf("publish capture %s: %w", r.Proof.CaptureID, err))
		}
	}
	for _, s := range releases {
		if ctx.Err() != nil {
			return errors.Join(append(failures, ctx.Err())...)
		}
		if _, err = n.releaseSwept(ctx, s, *s.ReleaseIntent); err != nil {
			failures = append(failures, fmt.Errorf("release session %s: %w", s.Intent.ID, err))
		}
	}
	return errors.Join(failures...)
}

// RunRecovery is joined by the process owner before closing the WAL/backend.
func (n *CSINode) RunRecovery(ctx context.Context, report func(error)) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := n.Reconcile(attempt)
		cancel()
		if err != nil && ctx.Err() == nil && report != nil {
			report(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
