package node

import (
	"context"
	"testing"
)

// This is local replay overhead with a mock CSI transport, not VM launch latency.
func BenchmarkPreparedReplay(b *testing.B) {
	n, _, key := setup(b)
	token := plan(key, "bench")
	ctx := context.Background()
	if _, e := n.Prepare(ctx, "bench", token); e != nil {
		b.Fatal(e)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, e := n.Prepare(ctx, "bench", token); e != nil {
			b.Fatal(e)
		}
	}
}
func TestCanceledPlanCannotResurrect(t *testing.T) {
	n, f, key := setup(t)
	token := plan(key, "cancelled")
	ctx := context.Background()
	if e := n.CancelUnstarted(ctx, "cancelled", "cancelled", token, "verified-test-state"); e != nil {
		t.Fatal(e)
	}
	if _, e := n.Prepare(ctx, "cancelled", token); e == nil {
		t.Fatal("late prepare resurrected cancelled plan")
	}
	if f.stage != 0 || f.publish != 0 {
		t.Fatal("cancel issued mount RPC")
	}
}
