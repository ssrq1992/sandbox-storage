package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"os"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/ledger"
	"time"
)

func main() {
	action := flag.String("action", "inspect", "inspect, migrate, keygen")
	db := flag.String("database-url", "", "PostgreSQL URL; defaults to SSF_DATABASE_URL")
	addr := flag.String("address", "localhost:9443", "node endpoint")
	cert := flag.String("cert", "", "client certificate")
	key := flag.String("key", "", "client key or keygen output prefix")
	ca := flag.String("ca", "", "CA")
	id := flag.String("operation", "", "operation ID")
	flag.Parse()
	if e := run(*action, *db, *addr, *cert, *key, *ca, *id); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(action, db, addr, cert, key, ca, id string) error {
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
	case "inspect":
		t, e := auth.TLS(cert, key, ca, false)
		if e != nil {
			return e
		}
		conn, e := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(t)))
		if e != nil {
			return e
		}
		defer conn.Close()
		out, e := v1.NewRuntimeStorageNodeClient(conn).InspectOperation(ctx, &v1.InspectRequest{OperationId: id})
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(out)
	default:
		return fmt.Errorf("unknown action")
	}
}
