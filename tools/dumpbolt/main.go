package main

import (
	"encoding/json"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: dumpbolt <db>")
		os.Exit(1)
	}
	db, err := bolt.Open(os.Args[1], 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	defer db.Close()
	db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			fmt.Printf("== bucket %s ==\n", name)
			return b.ForEach(func(k, v []byte) error {
				out := string(v)
				var pretty any
				if json.Unmarshal(v, &pretty) == nil {
					raw, _ := json.MarshalIndent(pretty, "", "  ")
					out = string(raw)
				}
				fmt.Printf("--- key=%s\n%s\n", k, out)
				return nil
			})
		})
	})
}
