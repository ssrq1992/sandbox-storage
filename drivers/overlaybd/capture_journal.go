package overlaybd

import (
	"encoding/json"
	"errors"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
)

// A captured receipt permits publication after thaw; the immutable native
// layers remain pinned by the native session journal until publication.
type CaptureRecord struct {
	SessionID string              `json:"session_id"`
	Proof     CaptureProof        `json:"proof"`
	Captured  *api.CaptureReceipt `json:"captured,omitempty"`
	Published *api.VolumeReceipt  `json:"published,omitempty"`
}

var capturesBucket = []byte("overlaybd-captures-v1")

func captureKey(id, capture string) []byte { return []byte(id + "\x00" + capture) }

func (s *Sessions) Capture(id, capture string) (CaptureRecord, error) {
	var record CaptureRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(capturesBucket)
		if b == nil {
			return ErrNotFound
		}
		raw := b.Get(captureKey(id, capture))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &record)
	})
	return record, err
}
func (s *Sessions) pinCapture(id string, proof CaptureProof) (CaptureRecord, error) {
	record := CaptureRecord{SessionID: id, Proof: proof}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(capturesBucket)
		if err != nil {
			return err
		}
		key := captureKey(id, proof.CaptureID)
		if raw := b.Get(key); raw != nil {
			if err = json.Unmarshal(raw, &record); err != nil {
				return err
			}
			if record.SessionID != id || record.Proof != proof {
				return ErrConflict
			}
			return nil
		}
		session, err := decodeSession(tx.Bucket(sessionsBucket).Get([]byte(id)))
		if err != nil {
			return err
		}
		if (session.State != "Staged" && !stoppedRecovery(session, proof)) || session.Evidence == nil || session.ReleaseStarted || session.ReleaseProof != "" || session.Intent.Purpose != "runtime" {
			return ErrTransition
		}
		if session.State == "NeedsRecovery" {
			finals := tx.Bucket(finalizationsBucket)
			if finals == nil {
				return ErrTransition
			}
			var final Finalization
			if err = json.Unmarshal(finals.Get([]byte(id)), &final); err != nil {
				return err
			}
			if final.CaptureID != proof.CaptureID || final.StopDigest != proof.StoppedDigest || final.ExpectedHead != proof.ExpectedHead {
				return ErrConflict
			}
		}
		head := session.PublishedHead
		if head == "" {
			head = session.Intent.HeadRevision
		}
		if head != proof.ExpectedHead {
			return ErrConflict
		}
		// A second barrier cannot overtake an unknown capture/publication.
		if err = b.ForEach(func(_, raw []byte) error {
			var prior CaptureRecord
			if err := json.Unmarshal(raw, &prior); err != nil {
				return err
			}
			if prior.SessionID == id && prior.Published == nil {
				return ErrConflict
			}
			return nil
		}); err != nil {
			return err
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return b.Put(key, raw)
	})
	return record, err
}
func (s *Sessions) saveCapture(expected CaptureRecord, captured *api.CaptureReceipt, published *api.VolumeReceipt) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(capturesBucket)
		if b == nil {
			return ErrNotFound
		}
		key := captureKey(expected.SessionID, expected.Proof.CaptureID)
		var record CaptureRecord
		raw := b.Get(key)
		if raw == nil {
			return ErrNotFound
		}
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if record.SessionID != expected.SessionID || record.Proof != expected.Proof {
			return ErrConflict
		}
		if captured != nil {
			if record.Captured != nil && !proto.Equal(record.Captured, captured) {
				return ErrConflict
			}
			record.Captured = captured
		}
		if published != nil {
			if record.Captured == nil {
				return ErrTransition
			}
			if record.Published != nil {
				if !proto.Equal(record.Published, published) {
					return ErrConflict
				}
				return nil // Never move the session head back when replaying an old capture.
			}
			sb := tx.Bucket(sessionsBucket)
			session, err := decodeSession(sb.Get([]byte(expected.SessionID)))
			if err != nil {
				return err
			}
			if (session.State != "Staged" && !stoppedRecovery(session, record.Proof)) || session.ReleaseStarted {
				return ErrTransition
			}
			head := session.PublishedHead
			if head == "" {
				head = session.Intent.HeadRevision
			}
			if head != record.Proof.ExpectedHead {
				return ErrConflict
			}
			if !validDigest(published.HeadRevision) {
				return errors.New("invalid published capture head")
			}
			session.PublishedHead = published.HeadRevision
			session.Revision++
			if err = putSession(sb, session); err != nil {
				return err
			}
			record.Published = published
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return b.Put(key, raw)
	})
}

// A recovery session is never made Staged again. Only the already verified
// final stop barrier may consume its old upper and proceed toward release.
func stoppedRecovery(s Session, p CaptureProof) bool {
	return s.State == "NeedsRecovery" && s.Intent.Purpose == "runtime" && s.Evidence != nil && validDigest(p.StoppedDigest) && p.StoppedDigest == p.FreezeDigest && p.CaptureID == core.Hash("final/"+s.Intent.ID) && p.BarrierID == p.CaptureID
}
