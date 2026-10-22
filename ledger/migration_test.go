package ledger

import (
	"context"
	"testing"
	"time"
)

func TestCompletedMigrationDoesNotRequestDDLAgainstLiveBindings(t *testing.T) {
	store, ctx, _ := lifecycleStore(t)
	transaction, err := store.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer transaction.Rollback(ctx)
	// An ordinary live transaction can retain this relation read lock. An
	// ALTER TABLE replay would block waiting for AccessExclusiveLock.
	if _, err = transaction.Exec(ctx, `SELECT id FROM ssf_bindings LIMIT 0`); err != nil {
		t.Fatal(err)
	}
	check, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = store.Migrate(check); err != nil {
		t.Fatalf("completed migration requested DDL locks: %v", err)
	}
}
