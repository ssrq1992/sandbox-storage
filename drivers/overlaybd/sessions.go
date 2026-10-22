package overlaybd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// SessionIntent is an immutable projection of an already authorized SSF use.
// Persisting it does not issue or renew that authorization.
type SessionIntent struct {
	Purpose             string `json:"purpose"`
	ID                  string `json:"id"`
	RepositoryID        string `json:"repository_id"`
	VolumeID            string `json:"volume_id"`
	OperationID         string `json:"operation_id"`
	OwnerID             string `json:"owner_id"`
	RunID               string `json:"run_id"`
	Epoch               uint64 `json:"epoch"`
	NodeID              string `json:"node_id"`
	HeadRevision        string `json:"head_revision"`
	AuthorizationDigest string `json:"authorization_digest"`
	Readonly            bool   `json:"readonly"`
}

type DeviceEvidence struct {
	SessionID        string `json:"session_id"`
	OperationID      string `json:"operation_id"`
	Epoch            uint64 `json:"epoch"`
	HeadRevision     string `json:"head_revision"`
	NodeBootID       string `json:"node_boot_id"`
	DaemonInstanceID string `json:"daemon_instance_id"`
	DeviceIdentity   string `json:"device_identity"`
	Path             string `json:"path"`
	Major            uint32 `json:"major"`
	Minor            uint32 `json:"minor"`
	CapacityBytes    uint64 `json:"capacity_bytes"`
	FSUUID           string `json:"fs_uuid"`
	Readonly         bool   `json:"readonly"`
}

type PublishReference struct {
	Target   string `json:"target"`
	RunID    string `json:"run_id"`
	Epoch    uint64 `json:"epoch"`
	Borrowed bool   `json:"borrowed"`
}

type Session struct {
	PreparationRelease *PreparationProof           `json:"preparation_release,omitempty"`
	PublishedHead      string                      `json:"published_head,omitempty"`
	Intent             SessionIntent               `json:"intent"`
	State              string                      `json:"state"`
	Revision           uint64                      `json:"revision"`
	Evidence           *DeviceEvidence             `json:"evidence,omitempty"`
	Publishes          map[string]PublishReference `json:"publishes"`
	ReleaseProof       string                      `json:"release_proof,omitempty"`
	ReleaseStarted     bool                        `json:"release_started,omitempty"`
	ReleaseIntent      *ReleaseIntent              `json:"release_intent,omitempty"`
}

type Sessions struct{ db *bolt.DB }

var sessionsBucket = []byte("overlaybd-sessions-v1")

func OpenSessions(path string) (*Sessions, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("absolute session WAL path required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	if err = db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucketIfNotExists(sessionsBucket); return err }); err != nil {
		db.Close()
		return nil, err
	}
	return &Sessions{db: db}, nil
}
func (s *Sessions) Close() error { return s.db.Close() }

func validIntent(i SessionIntent) bool {
	return (i.Purpose == "runtime" || i.Purpose == "preparation") && i.ID != "" && i.RepositoryID != "" && i.VolumeID != "" && i.OperationID != "" && i.OwnerID != "" && i.RunID != "" && i.Epoch > 0 && i.NodeID != "" && i.HeadRevision != "" && i.AuthorizationDigest != ""
}

func decodeSession(raw []byte) (Session, error) {
	var result Session
	if len(raw) == 0 {
		return result, ErrNotFound
	}
	err := json.Unmarshal(raw, &result)
	if err == nil && (!validIntent(result.Intent) || result.Revision == 0 || result.Publishes == nil) {
		err = errors.New("corrupt OverlayBD session WAL")
	}
	return result, err
}

func putSession(b *bolt.Bucket, session Session) error {
	raw, err := json.Marshal(session)
	if err != nil {
		return err
	}
	return b.Put([]byte(session.Intent.ID), raw)
}

func (s *Sessions) Plan(intent SessionIntent) (Session, error) {
	if !validIntent(intent) {
		return Session{}, errors.New("incomplete authorized session intent")
	}
	var result Session
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(sessionsBucket)
		if raw := b.Get([]byte(intent.ID)); raw != nil {
			existing, err := decodeSession(raw)
			if err != nil {
				return err
			}
			if existing.Intent != intent {
				return ErrConflict
			}
			result = existing
			return nil
		}
		// This is only a local backstop. Cross-node use is exclusively granted by SSF.
		if err := b.ForEach(func(_, raw []byte) error {
			existing, err := decodeSession(raw)
			if err != nil {
				return err
			}
			i := existing.Intent
			if i.RepositoryID == intent.RepositoryID && i.VolumeID == intent.VolumeID && existing.State != "Released" && (!i.Readonly || !intent.Readonly) {
				return ErrConflict
			}
			return nil
		}); err != nil {
			return err
		}
		result = Session{Intent: intent, State: "Planned", Revision: 1, Publishes: map[string]PublishReference{}}
		return putSession(b, result)
	})
	return result, err
}

func (s *Sessions) Get(id string) (Session, error) {
	var result Session
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		result, err = decodeSession(tx.Bucket(sessionsBucket).Get([]byte(id)))
		return err
	})
	return result, err
}

func (s *Sessions) update(expected Session, change func(*Session) error) (Session, error) {
	var result Session
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(sessionsBucket)
		current, err := decodeSession(b.Get([]byte(expected.Intent.ID)))
		if err != nil {
			return err
		}
		if current.Revision != expected.Revision || current.Intent != expected.Intent {
			return ErrConflict
		}
		if err = change(&current); err != nil {
			return err
		}
		current.Revision++
		if err = putSession(b, current); err != nil {
			return err
		}
		result = current
		return nil
	})
	return result, err
}

// BeginCreate is fsynced before contacting the daemon. Unknown results must be
// inspected using this operation, not retried with a new session identity.
func (s *Sessions) BeginCreate(expected Session) (Session, error) {
	return s.update(expected, func(x *Session) error {
		if x.State != "Planned" {
			return ErrTransition
		}
		x.State = "Creating"
		return nil
	})
}

func (s *Sessions) MarkUnknown(expected Session) (Session, error) {
	return s.update(expected, func(x *Session) error {
		if x.State != "Creating" && x.State != "Staged" && x.State != "Releasing" {
			return ErrTransition
		}
		x.State = "NeedsRecovery"
		return nil
	})
}

func (s *Sessions) RecordDevice(expected Session, evidence DeviceEvidence) (Session, error) {
	return s.update(expected, func(x *Session) error {
		i := x.Intent
		if (x.State != "Creating" && x.State != "NeedsRecovery") || x.ReleaseStarted {
			return ErrTransition
		}
		if evidence.SessionID != i.ID || evidence.OperationID != i.OperationID || evidence.Epoch != i.Epoch || evidence.HeadRevision != i.HeadRevision || evidence.Readonly != i.Readonly || evidence.NodeBootID == "" || evidence.DaemonInstanceID == "" || evidence.DeviceIdentity == "" || evidence.FSUUID == "" || evidence.CapacityBytes == 0 || evidence.CapacityBytes%512 != 0 || !filepath.IsAbs(evidence.Path) {
			return errors.New("device receipt does not prove the planned session")
		}
		if x.Evidence != nil && *x.Evidence != evidence {
			return errors.New("device identity changed during recovery")
		}
		x.Evidence = &evidence
		x.State = "Staged"
		return nil
	})
}

// RecordPublish is called only after the actual block publication is verified.
func (s *Sessions) RecordPublish(expected Session, target string) (Session, error) {
	return s.update(expected, func(x *Session) error {
		if x.State != "Staged" || x.Evidence == nil || x.ReleaseProof != "" || x.PreparationRelease != nil || !filepath.IsAbs(target) || filepath.Clean(target) != target {
			return ErrTransition
		}
		if old, ok := x.Publishes[target]; ok {
			if old.RunID != x.Intent.RunID || old.Epoch != x.Intent.Epoch {
				return ErrConflict
			}
			return nil
		}
		x.Publishes[target] = PublishReference{Target: target, RunID: x.Intent.RunID, Epoch: x.Intent.Epoch}
		return nil
	})
}

func (s *Sessions) Borrow(expected Session, target string) (Session, error) {
	return s.update(expected, func(x *Session) error {
		ref, ok := x.Publishes[target]
		if !ok || x.Intent.Purpose != "runtime" || x.State != "Staged" || x.ReleaseProof != "" {
			return ErrTransition
		}
		ref.Borrowed = true
		x.Publishes[target] = ref
		return nil
	})
}

// Proof is an already verified native stop/detach receipt, not a client boolean.
// Its authorization is checked by the node service; the WAL pins its identity.
type StopProof struct {
	SessionID, RunID, DeviceIdentity, Digest string
	Epoch                                    uint64
}

func (p StopProof) matches(s Session) bool {
	return s.Intent.Purpose == "runtime" && p.Digest != "" && p.SessionID == s.Intent.ID && p.RunID == s.Intent.RunID && p.Epoch == s.Intent.Epoch && s.Evidence != nil && p.DeviceIdentity == s.Evidence.DeviceIdentity
}

// PreparationProof confirms that preparer processes, FDs and temporary mounts
// were released. It is deliberately not a fabricated VMM stop receipt.
type PreparationProof struct {
	SessionID, DeviceIdentity, NodeBootID, DaemonInstanceID, Digest string
	Epoch                                                           uint64
}

func (s *Sessions) BeginPreparationRelease(expected Session, proof PreparationProof) (Session, error) {
	return s.update(expected, func(x *Session) error {
		if x.Intent.Purpose != "preparation" || x.State != "Staged" || len(x.Publishes) != 0 || x.Evidence == nil || proof.SessionID != x.Intent.ID || proof.Epoch != x.Intent.Epoch || proof.DeviceIdentity != x.Evidence.DeviceIdentity || proof.NodeBootID != x.Evidence.NodeBootID || proof.DaemonInstanceID != x.Evidence.DaemonInstanceID || !validDigest(proof.Digest) {
			return ErrTransition
		}
		x.ReleaseProof = proof.Digest
		x.ReleaseStarted = true
		x.State = "Releasing"
		return nil
	})
}

func (s *Sessions) ReturnBorrow(expected Session, target string, proof StopProof) (Session, error) {
	return s.update(expected, func(x *Session) error {
		if !proof.matches(*x) {
			return errors.New("stop proof belongs to another device/run")
		}
		ref, ok := x.Publishes[target]
		if !ok {
			return ErrNotFound
		}
		ref.Borrowed = false
		x.Publishes[target] = ref
		x.ReleaseProof = proof.Digest
		return nil
	})
}

func (s *Sessions) RemovePublish(expected Session, target string) (Session, error) {
	return s.update(expected, func(x *Session) error {
		ref, ok := x.Publishes[target]
		if !ok {
			return nil
		}
		if ref.Borrowed {
			return errors.New("native FD is still borrowed")
		}
		delete(x.Publishes, target)
		return nil
	})
}

func (s *Sessions) BeginRelease(expected Session, proof StopProof) (Session, error) {
	return s.update(expected, func(x *Session) error {
		if (x.State != "Staged" && x.State != "NeedsRecovery") || len(x.Publishes) != 0 || !proof.matches(*x) {
			return ErrTransition
		}
		x.ReleaseProof = proof.Digest
		x.ReleaseStarted = true
		x.State = "Releasing"
		return nil
	})
}

func (s *Sessions) RecordReleased(expected Session, deviceIdentity string) (Session, error) {
	return s.update(expected, func(x *Session) error {
		if (x.State != "Releasing" && x.State != "NeedsRecovery") || !x.ReleaseStarted || x.ReleaseProof == "" || len(x.Publishes) != 0 || x.Evidence == nil || x.Evidence.DeviceIdentity != deviceIdentity {
			return fmt.Errorf("release receipt does not match protected device: %w", ErrTransition)
		}
		x.State = "Released"
		return nil
	})
}

// ReleaseIntent pins the final publication and detached FD evidence before a
// native release RPC. A response loss cannot change the cleanup authorization.
type ReleaseIntent struct {
	StopDigest         string `json:"stop_digest,omitempty"`
	BorrowedRefsDigest string `json:"borrowed_refs_digest,omitempty"`
	CaptureID          string `json:"capture_id,omitempty"`
	PublishedHead      string `json:"published_head,omitempty"`
	PreparationDigest  string `json:"preparation_digest,omitempty"`
}

func (s *Sessions) PinRelease(expected Session, intent ReleaseIntent) (Session, error) {
	return s.update(expected, func(x *Session) error {
		if !x.ReleaseStarted || (x.State != "Releasing" && x.State != "NeedsRecovery" && x.State != "Released") || len(x.Publishes) != 0 || x.Evidence == nil {
			return ErrTransition
		}
		if x.Intent.Purpose == "preparation" {
			if intent.PreparationDigest != x.ReleaseProof || !validDigest(intent.PreparationDigest) || intent.StopDigest != "" || intent.CaptureID != "" || intent.PublishedHead != "" || intent.BorrowedRefsDigest != "" {
				return ErrConflict
			}
		} else {
			if intent.StopDigest != x.ReleaseProof || !validDigest(intent.StopDigest) || !validDigest(intent.BorrowedRefsDigest) || intent.PreparationDigest != "" {
				return ErrConflict
			}
			if !x.Intent.Readonly && (intent.CaptureID == "" || !validDigest(intent.PublishedHead)) {
				return ErrConflict
			}
		}
		if x.ReleaseIntent != nil && *x.ReleaseIntent != intent {
			return ErrConflict
		}
		if x.State == "Released" && x.ReleaseIntent == nil {
			return ErrTransition
		}
		x.ReleaseIntent = &intent
		return nil
	})
}

func (s *Sessions) PinPreparationRelease(expected Session, proof PreparationProof) (Session, error) {
	return s.update(expected, func(x *Session) error {
		if x.Intent.Purpose != "preparation" || x.Evidence == nil || proof.SessionID != x.Intent.ID || proof.Epoch != x.Intent.Epoch || proof.DeviceIdentity != x.Evidence.DeviceIdentity || proof.NodeBootID != x.Evidence.NodeBootID || proof.DaemonInstanceID != x.Evidence.DaemonInstanceID || !validDigest(proof.Digest) {
			return ErrConflict
		}
		if x.PreparationRelease != nil && *x.PreparationRelease != proof {
			return ErrConflict
		}
		x.PreparationRelease = &proof
		return nil
	})
}
