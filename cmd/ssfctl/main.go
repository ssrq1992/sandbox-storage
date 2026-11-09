package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"ssf.dev/ssf/ledger"
	"time"
)

func main() {
	action := flag.String("action", "migrate", "migrate, keygen")
	db := flag.String("database-url", "", "PostgreSQL URL; defaults to SSF_DATABASE_URL")
	key := flag.String("key", "", "keygen output prefix")
	flag.Parse()
	if e := run(*action, *db, *key); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(action, db, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch action {
	case "keygen":
		if key == "" {
			return fmt.Errorf("key output prefix required")
		}
		pub, priv, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			return e
		}
		if e = os.WriteFile(key+".key", []byte(base64.StdEncoding.EncodeToString(priv)), 0600); e != nil {
			return e
		}
		return os.WriteFile(key+".pub", []byte(base64.StdEncoding.EncodeToString(pub)), 0644)
	case "migrate":
		if db == "" {
			db = os.Getenv("SSF_DATABASE_URL")
		}
		s, e := ledger.Open(ctx, db)
		if e != nil {
			return e
		}
		defer s.Close()
		return s.Migrate(ctx)
	default:
		return fmt.Errorf("unknown action")
	}
}
