package agentenvadapter

import (
	"encoding/json"
	"errors"
	bolt "go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"ssf.dev/ssf/core"
	"time"
)

var ErrNotFound = errors.New("node operation not found")

type Journal struct{ DB *bolt.DB }

func OpenJournal(root string) (*Journal, error) {
	if e := os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	db, e := bolt.Open(filepath.Join(root, "adapter.db"), 0600, &bolt.Options{Timeout: time.Second})
	if e != nil {
		return nil, e
	}
	e = db.Update(func(tx *bolt.Tx) error {
		for _, b := range []string{"operations", "tickets", "volume-preparations"} {
			if _, e := tx.CreateBucketIfNotExists([]byte(b)); e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		db.Close()
		return nil, e
	}
	return &Journal{db}, nil
}
func (j *Journal) Close() error { return j.DB.Close() }
func (j *Journal) Get(id string) (NodeRecord, error) {
	var r NodeRecord
	e := j.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("operations")).Get([]byte(id))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &r)
	})
	return r, e
}
func (j *Journal) Update(id string, f func(*NodeRecord) error) (NodeRecord, error) {
	var r NodeRecord
	e := j.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("operations"))
		v := b.Get([]byte(id))
		if v != nil {
			if e := json.Unmarshal(v, &r); e != nil {
				return e
			}
		}
		if e := f(&r); e != nil {
			return e
		}
		r.Updated = time.Now().UTC()
		data, e := json.Marshal(r)
		if e != nil {
			return e
		}
		if r.Ticket != "" {
			if e = tx.Bucket([]byte("tickets")).Put([]byte(core.Hash(r.Ticket)), []byte(id)); e != nil {
				return e
			}
		}
		return b.Put([]byte(id), data)
	})
	return r, e
}
func (j *Journal) ByTicket(ticket string) (NodeRecord, error) {
	var id string
	e := j.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tickets")).Get([]byte(core.Hash(ticket)))
		if b == nil {
			return ErrNotFound
		}
		id = string(b)
		return nil
	})
	if e != nil {
		return NodeRecord{}, e
	}
	return j.Get(id)
}
func (j *Journal) Pending() ([]string, error) {
	var out []string
	e := j.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("operations")).ForEach(func(k, v []byte) error {
			var r NodeRecord
			if e := json.Unmarshal(v, &r); e != nil {
				return e
			}
			if r.State != "Released" || (r.DeleteRequested && !r.DeleteAcknowledged) {
				out = append(out, string(k))
			}
			return nil
		})
	})
	return out, e
}
