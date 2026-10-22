package overlaybd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCanonicalIntentPreservesLargeIntegers(t *testing.T) {
	a, err := canonical(map[string]any{"capacity": uint64(9007199254740993), "repository": "local"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := canonical(map[string]any{"repository": "local", "capacity": uint64(9007199254740993)})
	c, _ := canonical(map[string]any{"repository": "local", "capacity": uint64(9007199254740992)})
	if string(a) != string(b) || string(a) == string(c) {
		t.Fatal("intent normalization loses identity or precision")
	}
}

func TestUnknownCannotRestartAndTerminalCannotReopen(t *testing.T) {
	for _, from := range []OperationState{NeedsRecovery, Succeeded, Failed} {
		for _, next := range []OperationState{Pending, Applying} {
			if validTransition(from, next) {
				t.Fatalf("unsafe transition %s -> %s", from, next)
			}
		}
	}
	if !validTransition(NeedsRecovery, Succeeded) {
		t.Fatal("an inspected durable result must be recordable")
	}
}

func database(t *testing.T) *Journal {
	t.Helper()
	dsn := os.Getenv("SSF_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("NOT_EXECUTED: SSF_TEST_DATABASE_URL required for PostgreSQL integration")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	j := &Journal{Pool: pool}
	if err := j.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := j.Migrate(context.Background()); err != nil {
		t.Fatal("migration replay:", err)
	}
	return j
}

func TestPostgresConcurrentReservationAndUnknownRecovery(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	input := map[string]any{"capacity_bytes": int64(2 << 30), "repository": "repo-a"}
	var wg sync.WaitGroup
	ops := make(chan Operation, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op, err := j.Reserve(ctx, "create", name, "vol_a", input)
			if err != nil {
				errs <- err
			} else {
				ops <- op
			}
		}()
	}
	wg.Wait()
	close(ops)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var original Operation
	for op := range ops {
		if original.ID != "" && original.ID != op.ID {
			t.Fatal("duplicate identity")
		}
		original = op
	}
	if _, err := j.Reserve(ctx, "create", name, "vol_a", map[string]any{"capacity_bytes": 3 << 30}); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed intent accepted: %v", err)
	}
	claimed, err := j.Finish(ctx, original, Applying, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Finish(ctx, original, Applying, nil, ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("second executor accepted: %v", err)
	}
	unknown, err := j.Finish(ctx, claimed, NeedsRecovery, nil, "reply lost")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Finish(ctx, unknown, Applying, nil, ""); !errors.Is(err, ErrTransition) {
		t.Fatal("unknown operation restarted")
	}
	if _, err := j.Finish(ctx, unknown, Succeeded, nil, ""); err == nil {
		t.Fatal("success without receipt")
	}
	completed, err := j.Finish(ctx, unknown, Succeeded, map[string]any{"head": "head-a", "operation": original.ID}, "")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := j.Reserve(ctx, "create", name, "vol_a", input)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Revision != completed.Revision || replay.State != Succeeded || string(replay.Result) != string(completed.Result) {
		t.Fatal("lost completed receipt")
	}
}
