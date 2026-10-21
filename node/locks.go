package node

import (
	"sort"
	"ssf.dev/ssf/core"
	"sync"
)

type keyedLocks struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}
type lockEntry struct {
	mu   sync.Mutex
	refs int
}

func (k *keyedLocks) lock(p core.Plan) func() {
	keys := []string{"op/" + p.Operation}
	seen := map[string]bool{}
	for _, b := range p.Bindings {
		key := "volume/" + b.Volume.Key()
		if !seen[key] {
			keys = append(keys, key)
			seen[key] = true
		}
	}
	sort.Strings(keys)
	entries := make([]*lockEntry, len(keys))
	k.mu.Lock()
	if k.entries == nil {
		k.entries = map[string]*lockEntry{}
	}
	for i, key := range keys {
		e := k.entries[key]
		if e == nil {
			e = &lockEntry{}
			k.entries[key] = e
		}
		e.refs++
		entries[i] = e
	}
	k.mu.Unlock()
	for _, e := range entries {
		e.mu.Lock()
	}
	return func() {
		for i := len(entries) - 1; i >= 0; i-- {
			entries[i].mu.Unlock()
		}
		k.mu.Lock()
		defer k.mu.Unlock()
		for i, key := range keys {
			entries[i].refs--
			if entries[i].refs == 0 {
				delete(k.entries, key)
			}
		}
	}
}
func (e *Engine) lockedRecord(id string) (Record, func(), error) {
	r, err := e.WAL.Get(id)
	if err != nil {
		return r, func() {}, err
	}
	unlock := e.locks.lock(r.Plan)
	r, err = e.WAL.Get(id)
	if err != nil {
		unlock()
		return r, func() {}, err
	}
	return r, unlock, nil
}
