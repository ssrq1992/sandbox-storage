package overlaybd

import (
	"encoding/json"
	"errors"

	bolt "go.etcd.io/bbolt"
)

// BeginDrain fences new device admissions, serialized with Stage/Publish.
// Capture and release keep serving until the original runtime supplies proof.
func (n *CSINode) BeginDrain() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.draining.Store(true)
}

// DrainPending is an observation, never a forced release or stop assertion.
// All nonterminal WAL states, borrowed FDs and unpublished captures keep the
// process alive. Corruption fails closed, including on an otherwise empty node.
func (n *CSINode) DrainPending() (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Operations == nil || n.Operations.Sessions == nil {
		return 0, errors.New("node session WAL unavailable")
	}
	pending := 0
	err := n.Operations.Sessions.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket(sessionsBucket).ForEach(func(_, raw []byte) error {
			s, err := decodeSession(raw)
			if err != nil {
				return err
			}
			if s.State != "Released" || len(s.Publishes) != 0 {
				pending++
			}
			return nil
		}); err != nil {
			return err
		}
		if b := tx.Bucket(capturesBucket); b != nil {
			return b.ForEach(func(_, raw []byte) error {
				var capture CaptureRecord
				if err := json.Unmarshal(raw, &capture); err != nil {
					return err
				}
				if capture.SessionID == "" || capture.Proof.CaptureID == "" {
					return errors.New("corrupt capture WAL")
				}
				if capture.Published == nil {
					pending++
				}
				return nil
			})
		}
		return nil
	})
	return pending, err
}
