package overlaybd

import (
	"encoding/json"
	"errors"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

// A path claim precedes any native or mount side effect. Claims remain as
// tombstones after release, so a retry cannot redirect a stale CSI target.
type NodePath struct {
	Path      string `json:"path"`
	SessionID string `json:"session_id"`
	Handle    string `json:"handle"`
	Kind      string `json:"kind"`
}

var nodePathsBucket = []byte("overlaybd-node-paths-v1")

func (s *Sessions) ClaimPath(claim NodePath) error {
	if !filepath.IsAbs(claim.Path) || filepath.Clean(claim.Path) != claim.Path || claim.SessionID == "" || claim.Handle == "" || (claim.Kind != "stage" && claim.Kind != "publish") {
		return errors.New("invalid native node path claim")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(nodePathsBucket)
		if err != nil {
			return err
		}
		if raw := b.Get([]byte(claim.Path)); raw != nil {
			var prior NodePath
			if err = json.Unmarshal(raw, &prior); err != nil {
				return err
			}
			if prior != claim {
				return ErrConflict
			}
			return nil
		}
		raw, err := json.Marshal(claim)
		if err != nil {
			return err
		}
		return b.Put([]byte(claim.Path), raw)
	})
}
func (s *Sessions) Path(path string) (NodePath, error) {
	var result NodePath
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(nodePathsBucket)
		if b == nil {
			return ErrNotFound
		}
		raw := b.Get([]byte(path))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &result)
	})
	return result, err
}
