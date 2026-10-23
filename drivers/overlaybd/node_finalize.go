package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	bolt "go.etcd.io/bbolt"
	"ssf.dev/ssf/core"
)

// Finalization is written before capture. It fixes the last expected head and
// real stop/FD evidence across retries, including a lost head-publication reply.
type Finalization struct {
	SessionID          string `json:"session_id"`
	StopDigest         string `json:"stop_digest"`
	BorrowedRefsDigest string `json:"borrowed_refs_digest"`
	ExpectedHead       string `json:"expected_head"`
	CaptureID          string `json:"capture_id"`
	PublishedHead      string `json:"published_head,omitempty"`
}

var finalizationsBucket = []byte("overlaybd-finalizations-v1")

func (s *Sessions) pinFinalization(f Finalization) (Finalization, error) {
	var result Finalization
	err := s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(finalizationsBucket)
		if err != nil {
			return err
		}
		if raw := b.Get([]byte(f.SessionID)); raw != nil {
			if err = json.Unmarshal(raw, &result); err != nil {
				return err
			}
			prior := result
			prior.PublishedHead = ""
			if prior != f {
				return ErrConflict
			}
			return nil
		}
		raw, err := json.Marshal(f)
		if err != nil {
			return err
		}
		result = f
		return b.Put([]byte(f.SessionID), raw)
	})
	return result, err
}
func (s *Sessions) finalization(id string) (Finalization, error) {
	var f Finalization
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(finalizationsBucket)
		if b == nil {
			return ErrNotFound
		}
		raw := b.Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &f)
	})
	return f, err
}
func (s *Sessions) publishFinalization(expected Finalization, head string) error {
	if !validDigest(head) {
		return errors.New("invalid final published head")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(finalizationsBucket)
		if b == nil {
			return ErrNotFound
		}
		var current Finalization
		if err := json.Unmarshal(b.Get([]byte(expected.SessionID)), &current); err != nil {
			return err
		}
		if current != expected {
			return ErrConflict
		}
		current.PublishedHead = head
		raw, err := json.Marshal(current)
		if err != nil {
			return err
		}
		return b.Put([]byte(current.SessionID), raw)
	})
}

// FinalizeStopped is called by the authenticated node lifecycle extension, not
// by ordinary CSI cleanup. Verification includes actual VMM exit and FD return.
// The opaque evidence is checked by the configured runtime, never trusted as a
// caller-supplied boolean. The CSI publication remains until this completes.
func (n *CSINode) FinalizeStopped(ctx context.Context, id, evidence, expectedHead string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Evidence == nil {
		return errors.New("runtime detachment verifier required")
	}
	s, err := n.Operations.Sessions.Get(id)
	if err != nil {
		return err
	}
	if s.Intent.Purpose != "runtime" || s.Evidence == nil {
		return ErrTransition
	}
	if err = n.Evidence.Verify(ctx, s.Intent.RunID, "detached", evidence); err != nil {
		return err
	}
	// The verifier's detached contract covers both stop and returned descriptors.
	proof := core.Hash(evidence)
	f := Finalization{SessionID: id, StopDigest: proof, BorrowedRefsDigest: core.Hash("fd-return/" + id + "/" + proof), ExpectedHead: expectedHead, CaptureID: core.Hash("final/" + id)}
	if !validDigest(expectedHead) {
		return errors.New("final capture requires a pinned expected head")
	}
	f, err = n.Operations.Sessions.pinFinalization(f)
	if err != nil {
		return err
	}
	if !s.Intent.Readonly && f.PublishedHead == "" {
		_, published, err := n.Operations.CaptureAndPublish(ctx, id, CaptureProof{CaptureID: f.CaptureID, BarrierID: f.CaptureID, FreezeDigest: proof, StoppedDigest: proof, ExpectedHead: expectedHead})
		if err != nil {
			return err
		}
		if err = n.Operations.Sessions.publishFinalization(f, published.HeadRevision); err != nil {
			return err
		}
	}
	s, err = n.Operations.Sessions.Get(id)
	if err != nil {
		return err
	}
	stop := StopProof{SessionID: id, RunID: s.Intent.RunID, Epoch: s.Intent.Epoch, DeviceIdentity: s.Evidence.DeviceIdentity, Digest: proof}
	targets := make([]string, 0, len(s.Publishes))
	for target := range s.Publishes {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	for _, target := range targets {
		s, err = n.Operations.Sessions.ReturnBorrow(s, target, stop)
		if err != nil {
			return err
		}
	}
	// No publication may exist if Stage succeeded but Publish was never reached.
	if len(targets) == 0 && (s.State == "Staged" || s.State == "NeedsRecovery") {
		_, err = n.Operations.Sessions.BeginRelease(s, stop)
	}
	return err
}

func (n *CSINode) finishStopped(ctx context.Context, s Session) error {
	f, err := n.Operations.Sessions.finalization(s.Intent.ID)
	if err != nil {
		return err
	}
	if !s.Intent.Readonly && f.PublishedHead == "" {
		return errors.New("final volume publication incomplete")
	}
	if s.State == "Staged" || (s.State == "NeedsRecovery" && !s.ReleaseStarted) {
		s, err = n.Operations.Sessions.BeginRelease(s, StopProof{SessionID: s.Intent.ID, RunID: s.Intent.RunID, Epoch: s.Intent.Epoch, DeviceIdentity: s.Evidence.DeviceIdentity, Digest: f.StopDigest})
		if err != nil {
			return err
		}
	}
	_, err = n.releaseSwept(ctx, s, ReleaseIntent{StopDigest: f.StopDigest, BorrowedRefsDigest: f.BorrowedRefsDigest, CaptureID: f.CaptureID, PublishedHead: f.PublishedHead})
	return err
}
