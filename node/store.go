package node

import (
	"bytes"
	"encoding/json"
	"errors"
	bolt "go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"ssf.dev/ssf/core"
	"time"
)

type Record struct {
	SignedPlan      string                   `json:",omitempty"`
	ReleaseEvidence string                   `json:",omitempty"`
	Preparation     *core.PreparationReceipt `json:",omitempty"`
	Plan            core.Plan
	Digest          string
	State           string
	Steps           map[string]string
	Endpoints       []core.Endpoint
	Reason          string
}
type WAL struct{ DB *bolt.DB }

var ErrNotFound = errors.New("unknown operation")

func OpenWAL(path string) (*WAL, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	db, e := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if e != nil {
		return nil, e
	}
	w := &WAL{db}
	e = db.Update(func(t *bolt.Tx) error {
		for _, name := range []string{"operations", "active", "holders"} {
			if _, e := t.CreateBucketIfNotExists([]byte(name)); e != nil {
				return e
			}
		}
		return t.Bucket([]byte("operations")).ForEach(func(_, b []byte) error {
			var r Record
			if e := json.Unmarshal(b, &r); e != nil {
				return e
			}
			return indexRecord(t, r)
		})
	})
	if e != nil {
		db.Close()
		return nil, e
	}
	return w, nil
}
func (w *WAL) Save(r Record) error {
	b, e := json.Marshal(r)
	if e != nil {
		return e
	}
	return w.DB.Update(func(t *bolt.Tx) error {
		if e := t.Bucket([]byte("operations")).Put([]byte(r.Plan.Operation), b); e != nil {
			return e
		}
		return indexRecord(t, r)
	})
}
func (w *WAL) Get(id string) (Record, error) {
	var r Record
	e := w.DB.View(func(t *bolt.Tx) error {
		b := t.Bucket([]byte("operations")).Get([]byte(id))
		if b == nil {
			return ErrNotFound
		}
		return json.Unmarshal(b, &r)
	})
	return r, e
}
func (w *WAL) All() ([]Record, error) {
	var out []Record
	e := w.DB.View(func(t *bolt.Tx) error {
		return t.Bucket([]byte("operations")).ForEach(func(_, b []byte) error {
			var r Record
			if e := json.Unmarshal(b, &r); e != nil {
				return e
			}
			out = append(out, r)
			return nil
		})
	})
	return out, e
}
func (w *WAL) Close() error { return w.DB.Close() }

func indexRecord(t *bolt.Tx, r Record) error {
	active := t.Bucket([]byte("active"))
	holders := t.Bucket([]byte("holders"))
	id := []byte(r.Plan.Operation)
	if r.State == "Released" {
		if e := active.Delete(id); e != nil {
			return e
		}
	} else {
		if e := active.Put(id, id); e != nil {
			return e
		}
	}
	for _, b := range r.Plan.Bindings {
		k := []byte(b.Volume.Key() + "/" + r.Plan.Operation)
		if r.State == "Released" {
			if e := holders.Delete(k); e != nil {
				return e
			}
		} else if e := holders.Put(k, id); e != nil {
			return e
		}
	}
	return nil
}
func (w *WAL) Active() ([]Record, error) {
	var records []Record
	e := w.DB.View(func(t *bolt.Tx) error {
		return t.Bucket([]byte("active")).ForEach(func(_, id []byte) error {
			var r Record
			if e := json.Unmarshal(t.Bucket([]byte("operations")).Get(id), &r); e != nil {
				return e
			}
			records = append(records, r)
			return nil
		})
	})
	return records, e
}
func (w *WAL) ForVolume(key string) ([]Record, error) {
	var records []Record
	e := w.DB.View(func(t *bolt.Tx) error {
		c := t.Bucket([]byte("holders")).Cursor()
		prefix := []byte(key + "/")
		for k, id := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, id = c.Next() {
			var r Record
			if e := json.Unmarshal(t.Bucket([]byte("operations")).Get(id), &r); e != nil {
				return e
			}
			records = append(records, r)
		}
		return nil
	})
	return records, e
}
