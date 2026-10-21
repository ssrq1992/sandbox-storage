package consume

import (
	"context"
	"errors"
	"os"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/ledger"
	"testing"
	"time"
)

func TestAbortRequiresExplicitNoDispatchProof(t *testing.T) {
	c := &Coordinator{}
	if c.AbortUnpublished(context.Background(), "op", nil) == nil {
		t.Fatal("missing no-dispatch proof accepted")
	}
}
func TestPostgresAbortUnattachedPartialAcquire(t *testing.T) {
	url := os.Getenv("SSF_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated PostgreSQL")
	}
	ctx := context.Background()
	db, e := ledger.Open(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = db.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	id := "abort-" + time.Now().Format("150405.000000000")
	v := core.Volume{UID: id, Driver: "test", Handle: id, PVC: core.PVCRef{Namespace: "n", Name: "p", UID: id}}
	if e = db.Register(ctx, v); e != nil {
		t.Fatal(e)
	}
	b := core.Binding{ID: id, Volume: v, Node: "node", Sandbox: id, Tenant: "tenant", MountID: "data", GuestPath: "/data"}
	if e = db.Acquire(ctx, id, "digest", []core.Binding{b}); e != nil {
		t.Fatal(e)
	}
	c := &Coordinator{Ledger: db} // No CSI clients: untouched volumes must not be attached.
	denied := errors.New("dispatch uncertain")
	if e = c.AbortUnpublished(ctx, id, func(context.Context) error { return denied }); !errors.Is(e, denied) {
		t.Fatal(e)
	}
	if e = c.AbortUnpublished(ctx, id, func(context.Context) error { return nil }); e != nil {
		t.Fatal(e)
	}
	bs, e := db.Bindings(ctx, id)
	if e != nil || len(bs) != 1 || bs[0].State != "Released" {
		t.Fatal(bs, e)
	}
	if e = c.AbortUnpublished(ctx, id, func(context.Context) error { return nil }); e != nil {
		t.Fatal("non-idempotent compensation", e)
	}
}
