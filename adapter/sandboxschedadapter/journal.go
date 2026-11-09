package sandboxschedadapter

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// BindMount is one published volume mount of a sandbox bind.
type BindMount struct {
	BindingID string `json:"bindingID"`
	GuestPath string `json:"guestPath"`
	HostPath  string `json:"hostPath"`
	Device    string `json:"device"`
	Dev       uint64 `json:"dev"` // device id (stat.Dev) for kernel evidence
	ReadOnly  bool   `json:"readOnly"`
	// Kind is always "nfs": a directly mounted NFS export (Device/Dev
	// unused). The field stays so legacy engine-backed rows remain
	// parseable and are rejected explicitly.
	Kind string `json:"kind,omitempty"`
	// Source is the resolved NFS export ("ip:/path") of an nfs mount.
	Source string `json:"source,omitempty"`
}

// BindRecord is the durable node-local state of one sandbox bind.
type BindRecord struct {
	Sandbox   string      `json:"sandbox"`
	Ticket    string      `json:"ticket"`
	BootID    string      `json:"bootID"`
	Operation string      `json:"operation"`
	Plan      string      `json:"plan"`
	RunUID    string      `json:"runUID"`
	// Kind is always "nfs"; legacy engine-backed rows are rejected.
	Kind      string      `json:"kind,omitempty"`
	Instance  string      `json:"instance"`
	PID       int         `json:"pid"`
	Mounts    []BindMount `json:"mounts"`
	State     string      `json:"state"`
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

var bucketBinds = []byte("binds")

// Journal is the node-local durable state store.
type Journal struct{ DB *bolt.DB }

// OpenJournal opens (creating if needed) the journal under root.
func OpenJournal(root string) (*Journal, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(root, "journal.db"), 0o600, &bolt.Options{Timeout: 30 * time.Second, NoSync: false})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(bucketBinds)
		return e
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Journal{DB: db}, nil
}

// Close releases the journal.
func (j *Journal) Close() error { return j.DB.Close() }

// ErrNotFound marks a missing journal record.
var ErrNotFound = errors.New("sandboxsched journal record not found")

// Bind loads one bind record by sandbox ID.
func (j *Journal) Bind(sandbox string) (BindRecord, error) {
	var r BindRecord
	err := j.DB.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketBinds).Get([]byte(sandbox))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &r)
	})
	return r, err
}

// PutBind stores a bind record.
func (j *Journal) PutBind(r BindRecord) error {
	r.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return j.DB.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketBinds).Put([]byte(r.Sandbox), raw) })
}

// UpdateBind mutates a bind record under the write lock.
func (j *Journal) UpdateBind(sandbox string, change func(*BindRecord) error) (BindRecord, error) {
	var r BindRecord
	err := j.DB.Update(func(tx *bolt.Tx) error {
		key := []byte(sandbox)
		raw := tx.Bucket(bucketBinds).Get(key)
		if raw == nil {
			return ErrNotFound
		}
		if e := json.Unmarshal(raw, &r); e != nil {
			return e
		}
		if e := change(&r); e != nil {
			return e
		}
		r.UpdatedAt = time.Now().UTC()
		out, e := json.Marshal(r)
		if e != nil {
			return e
		}
		return tx.Bucket(bucketBinds).Put(key, out)
	})
	return r, err
}

// Binds lists all bind records.
func (j *Journal) Binds() ([]BindRecord, error) {
	var out []BindRecord
	err := j.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketBinds).ForEach(func(_, raw []byte) error {
			var r BindRecord
			if e := json.Unmarshal(raw, &r); e != nil {
				return e
			}
			out = append(out, r)
			return nil
		})
	})
	return out, err
}

func newTicket() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
