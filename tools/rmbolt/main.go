package main

import (
	"encoding/json"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
)

// rmbolt surgically removes one session and every reference to it from the
// overlaybd node plugin session WAL. Usage: rmbolt <db> <session-id>
func main() {
	if len(os.Args) != 3 {
		fmt.Println("usage: rmbolt <db> <session-id>")
		os.Exit(1)
	}
	dbPath, sid := os.Args[1], os.Args[2]
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	defer db.Close()

	sessionsBucket := []byte("overlaybd-sessions-v1")
	pathsBucket := []byte("overlaybd-node-paths-v1")
	finalBucket := []byte("overlaybd-finalizations-v1")

	err = db.Update(func(tx *bolt.Tx) error {
		if b := tx.Bucket(sessionsBucket); b != nil {
			if raw := b.Get([]byte(sid)); raw != nil {
				if e := b.Delete([]byte(sid)); e != nil {
					return e
				}
				fmt.Printf("deleted session %s (was: %s)\n", sid, truncate(raw))
			} else {
				fmt.Printf("session %s not found in sessions bucket\n", sid)
			}
		}
		if b := tx.Bucket(pathsBucket); b != nil {
			var doomed [][]byte
			e := b.ForEach(func(k, v []byte) error {
				var claim struct {
					Path       string `json:"path"`
					SessionID  string `json:"session_id"`
					Handle     string `json:"handle"`
					Kind       string `json:"kind"`
				}
				if json.Unmarshal(v, &claim) == nil && claim.SessionID == sid {
					doomed = append(doomed, append([]byte(nil), k...))
				}
				return nil
			})
			if e != nil {
				return e
			}
			for _, k := range doomed {
				if e := b.Delete(k); e != nil {
					return e
				}
				fmt.Printf("deleted path claim %s\n", k)
			}
		}
		if b := tx.Bucket(finalBucket); b != nil {
			if raw := b.Get([]byte(sid)); raw != nil {
				if e := b.Delete([]byte(sid)); e != nil {
					return e
				}
				fmt.Printf("deleted finalization %s (was: %s)\n", sid, truncate(raw))
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println("update:", err)
		os.Exit(1)
	}
	fmt.Println("done")
}

func truncate(b []byte) string {
	if len(b) > 120 {
		return string(b[:120]) + "..."
	}
	return string(b)
}
