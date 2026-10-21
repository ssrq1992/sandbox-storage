package ledger

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sessionKey struct{}
type Executor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Begin(context.Context) (pgx.Tx, error)
}

func (s *Store) executor(ctx context.Context) Executor {
	if c, ok := ctx.Value(sessionKey{}).(*pgxpool.Conn); ok {
		return c
	}
	return s.Pool
}

// Session exposes the executor bound to an Exclusive callback. Embedded native
// plugins use this for their own tables, avoiding pool starvation while an
// advisory lock already owns a connection. It must not escape the callback.
func (s *Store) Session(ctx context.Context) Executor { return s.executor(ctx) }
