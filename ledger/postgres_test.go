package ledger

import (
	"context"
	"os"
	"ssf.dev/ssf/core"
	"sync"
	"testing"
	"time"
)

func TestPostgresConcurrentWriterAndDrain(t *testing.T) {
	url := os.Getenv("SSF_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated PostgreSQL: SSF_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	s, e := Open(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	unique := time.Now().Format("150405.000000000")
	v := core.Volume{Driver: "test", Handle: unique, PVC: core.PVCRef{UID: unique}}
	if e = s.Register(ctx, v); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	result := make(chan error, 2)
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			b := core.Binding{ID: unique + id, Volume: v, Sandbox: unique + id, Node: "node"}
			result <- s.Acquire(ctx, unique+id, id, []core.Binding{b})
		}(id)
	}
	wg.Wait()
	close(result)
	ok := 0
	for e := range result {
		if e == nil {
			ok++
		}
	}
	if ok != 1 {
		t.Fatalf("exclusive writers admitted: %d", ok)
	}
	if e = s.Drain(ctx, unique); e != nil {
		t.Fatal(e)
	}
	if e = s.Permit(ctx, unique, "Delete", time.Minute); e == nil {
		t.Fatal("permitted deletion with active writer")
	}
}
