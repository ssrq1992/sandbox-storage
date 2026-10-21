package ledger

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
	"time"
)

func (s *Store) Candidate(ctx context.Context, uid string) (string, error) {
	var id string
	e := s.executor(ctx).QueryRow(ctx, `SELECT pv_uid FROM ssf_selection_permits WHERE pvc_uid=$1`, uid).Scan(&id)
	if errors.Is(e, pgx.ErrNoRows) {
		return "", nil
	}
	return id, e
}

func (s *Store) AuthorizeNode(ctx context.Context, uid, node, lease string, expires time.Time) error {
	tag, e := s.executor(ctx).Exec(ctx, `INSERT INTO ssf_selection_permits(pvc_uid,node,lease,expires) VALUES($1,$2,$3,$4) ON CONFLICT(pvc_uid) DO UPDATE SET expires=excluded.expires WHERE ssf_selection_permits.node=excluded.node AND ssf_selection_permits.lease=excluded.lease`, uid, node, lease, expires)
	if e == nil && tag.RowsAffected() != 1 {
		return core.ErrConflict
	}
	return e
}
func (s *Store) ReserveCandidate(ctx context.Context, uid, pv, node string) error {
	tag, e := s.executor(ctx).Exec(ctx, `UPDATE ssf_selection_permits SET pv_uid=$2 WHERE pvc_uid=$1 AND node=$3 AND (pv_uid='' OR pv_uid=$2) AND expires>now()`, uid, pv, node)
	if e == nil && tag.RowsAffected() != 1 {
		return core.ErrConflict
	}
	return e
}
func (s *Store) AllowedSelection(ctx context.Context, uid, node, pv string) (bool, error) {
	var ok bool
	e := s.executor(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_selection_permits WHERE pvc_uid=$1 AND ($2='' OR node=$2) AND ($3='' OR pv_uid=$3) AND expires>now())`, uid, node, pv).Scan(&ok)
	return ok, e
}
