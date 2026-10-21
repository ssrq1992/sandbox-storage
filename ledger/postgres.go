// Package ledger owns consumption records, never backend volume provisioning.
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"sort"
	"ssf.dev/ssf/core"
	"strings"
	"time"
)

type Store struct {
	Pool    *pgxpool.Pool
	Observe func(string, time.Duration, error)
}

const Schema = `
CREATE TABLE IF NOT EXISTS ssf_volumes (key text PRIMARY KEY, pvc_uid text NOT NULL UNIQUE, descriptor jsonb NOT NULL, draining boolean NOT NULL DEFAULT false);
CREATE TABLE IF NOT EXISTS ssf_volume_maintenance(volume_key text PRIMARY KEY REFERENCES ssf_volumes(key), token text NOT NULL, target_bytes bigint NOT NULL CHECK(target_bytes>0), state text NOT NULL);
CREATE TABLE IF NOT EXISTS ssf_operations (id text PRIMARY KEY, digest text NOT NULL, state text NOT NULL, reason text NOT NULL DEFAULT '', updated timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS ssf_bindings (id text PRIMARY KEY, volume_key text NOT NULL REFERENCES ssf_volumes(key), operation_id text NOT NULL REFERENCES ssf_operations(id), node text NOT NULL, sandbox text NOT NULL, readonly boolean NOT NULL, state text NOT NULL, descriptor jsonb NOT NULL);
CREATE INDEX IF NOT EXISTS ssf_bindings_volume ON ssf_bindings(volume_key);
CREATE TABLE IF NOT EXISTS ssf_steps (id text PRIMARY KEY, digest text NOT NULL, state text NOT NULL, result jsonb, updated timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS ssf_attachments (volume_key text NOT NULL REFERENCES ssf_volumes(key), node text NOT NULL, fingerprint text NOT NULL, node_id text NOT NULL, state text NOT NULL, publish_context jsonb, PRIMARY KEY(volume_key,node));
CREATE TABLE IF NOT EXISTS ssf_permits (pvc_uid text PRIMARY KEY, action text NOT NULL, expires timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS ssf_selection_permits (pvc_uid text PRIMARY KEY, node text NOT NULL, lease text NOT NULL, pv_uid text NOT NULL DEFAULT '', expires timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS ssf_meta (version integer PRIMARY KEY);
INSERT INTO ssf_meta(version) VALUES (1) ON CONFLICT DO NOTHING;
ALTER TABLE ssf_bindings ADD COLUMN IF NOT EXISTS owner_uid text NOT NULL DEFAULT '';
ALTER TABLE ssf_bindings ADD COLUMN IF NOT EXISTS run_uid text NOT NULL DEFAULT '';
UPDATE ssf_bindings SET owner_uid=sandbox WHERE owner_uid='';
UPDATE ssf_bindings SET run_uid=sandbox WHERE run_uid='';
CREATE SEQUENCE IF NOT EXISTS ssf_run_epoch;
CREATE TABLE IF NOT EXISTS ssf_owner_tombstones(owner_uid text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS ssf_runs(owner_uid text NOT NULL,run_uid text NOT NULL,epoch bigint NOT NULL DEFAULT nextval('ssf_run_epoch'),state text NOT NULL DEFAULT 'Active',PRIMARY KEY(owner_uid,run_uid));
CREATE TABLE IF NOT EXISTS ssf_use_holds(volume_key text NOT NULL REFERENCES ssf_volumes(key),owner_uid text NOT NULL,readonly boolean NOT NULL,reason text NOT NULL,PRIMARY KEY(volume_key,owner_uid));
CREATE TABLE IF NOT EXISTS ssf_checkpoint_sets(id text PRIMARY KEY,owner_uid text NOT NULL,digest text NOT NULL,state text NOT NULL,descriptor jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS ssf_checkpoint_resources(uid text PRIMARY KEY,state text NOT NULL DEFAULT 'Live');
CREATE TABLE IF NOT EXISTS ssf_checkpoint_pins(checkpoint_id text NOT NULL REFERENCES ssf_checkpoint_sets(id),resource_uid text NOT NULL REFERENCES ssf_checkpoint_resources(uid),PRIMARY KEY(checkpoint_id,resource_uid));
CREATE TABLE IF NOT EXISTS ssf_checkpoint_gc(checkpoint_id text PRIMARY KEY REFERENCES ssf_checkpoint_sets(id),state text NOT NULL DEFAULT 'Pending',updated timestamptz NOT NULL DEFAULT now());
ALTER TABLE ssf_checkpoint_gc ADD COLUMN IF NOT EXISTS updated timestamptz NOT NULL DEFAULT now();
INSERT INTO ssf_meta(version) VALUES (2) ON CONFLICT DO NOTHING;
INSERT INTO ssf_meta(version) VALUES (3) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS ssf_volume_preparations (
 pvc_uid text PRIMARY KEY, digest text NOT NULL, state text NOT NULL,
 descriptor jsonb NOT NULL, updated timestamptz NOT NULL DEFAULT now()
);
INSERT INTO ssf_meta(version) VALUES (4) ON CONFLICT DO NOTHING;
-- Preserve source-PVC references for captures made before preparation support.
INSERT INTO ssf_checkpoint_resources(uid)
 SELECT DISTINCT v.pvc_uid FROM ssf_checkpoint_sets cp
 JOIN ssf_bindings b ON b.operation_id=COALESCE(cp.descriptor->>'operation',cp.descriptor->'intent'->>'operation')
 JOIN ssf_volumes v ON v.key=b.volume_key
 WHERE cp.state<>'Released' AND v.pvc_uid<>''
 ON CONFLICT DO NOTHING;
INSERT INTO ssf_checkpoint_pins(checkpoint_id,resource_uid)
 SELECT DISTINCT cp.id,v.pvc_uid FROM ssf_checkpoint_sets cp
 JOIN ssf_bindings b ON b.operation_id=COALESCE(cp.descriptor->>'operation',cp.descriptor->'intent'->>'operation')
 JOIN ssf_volumes v ON v.key=b.volume_key
 WHERE cp.state<>'Released' AND v.pvc_uid<>''
 ON CONFLICT DO NOTHING;
DELETE FROM ssf_permits WHERE pvc_uid IN (SELECT resource_uid FROM ssf_checkpoint_pins);
INSERT INTO ssf_meta(version) VALUES (5) ON CONFLICT DO NOTHING;`

func Open(ctx context.Context, url string) (*Store, error) {
	p, e := pgxpool.New(ctx, url)
	if e != nil {
		return nil, e
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, e
	}
	return &Store{Pool: p}, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	return s.Exclusive(ctx, "ssf/schema-migration", func(ctx context.Context) error {
		tx, err := s.executor(ctx).Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, Schema); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
}
func (s *Store) Close() { s.Pool.Close() }
func (s *Store) Register(ctx context.Context, v core.Volume) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = s.executor(ctx).Exec(ctx, `INSERT INTO ssf_volumes(key,pvc_uid,descriptor) VALUES($1,$2,$3) ON CONFLICT(key) DO NOTHING`, v.Key(), v.PVC.UID, b)
	if e != nil {
		return e
	}
	var uid string
	e = s.executor(ctx).QueryRow(ctx, `SELECT pvc_uid FROM ssf_volumes WHERE key=$1`, v.Key()).Scan(&uid)
	if e == nil && uid != v.PVC.UID {
		return core.ErrConflict
	}
	return e
}

// Acquire serializes on canonical physical volume rows in stable order. Writer
// admission is deliberately exclusive across sandbox instances, including RWO.
func (s *Store) Acquire(ctx context.Context, op, digest string, bs []core.Binding) error {
	if op == "" || digest == "" || len(bs) == 0 {
		return errors.New("invalid acquire")
	}
	tx, e := s.executor(ctx).Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	_, e = tx.Exec(ctx, `INSERT INTO ssf_operations(id,digest,state) VALUES($1,$2,'Acquiring') ON CONFLICT DO NOTHING`, op, digest)
	if e != nil {
		return e
	}
	var prior, state string
	if e = tx.QueryRow(ctx, `SELECT digest,state FROM ssf_operations WHERE id=$1 FOR UPDATE`, op).Scan(&prior, &state); e != nil {
		return e
	}
	if prior != digest {
		return core.ErrConflict
	}
	if state != "Acquiring" && state != "Resolving" {
		if state == "Released" || state == "Releasing" {
			return core.ErrConflict
		}
		return tx.Commit(ctx)
	}
	keys := map[string]bool{}
	owners := map[string]bool{}
	for _, b := range bs {
		keys[b.Volume.Key()] = true
		owners[b.Owner()] = true
	}
	ownerIDs := make([]string, 0, len(owners))
	for owner := range owners {
		ownerIDs = append(ownerIDs, owner)
	}
	sort.Strings(ownerIDs)
	for _, owner := range ownerIDs {
		if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "owner/"+owner); e != nil {
			return e
		}
		var deleted bool
		if e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_owner_tombstones WHERE owner_uid=$1)`, owner).Scan(&deleted); e != nil {
			return e
		}
		if deleted {
			return core.ErrConflict
		}
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	for _, k := range ordered {
		if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "volume/"+k); e != nil {
			return e
		}
		var draining bool
		if e = tx.QueryRow(ctx, `SELECT draining FROM ssf_volumes WHERE key=$1 FOR UPDATE`, k).Scan(&draining); e != nil {
			return e
		}
		if draining {
			return core.ErrConflict
		}
		var maintenance bool
		if e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_volume_maintenance WHERE volume_key=$1 AND state='Active')`, k).Scan(&maintenance); e != nil {
			return e
		}
		if maintenance {
			return core.ErrPending
		}
	}
	for _, b := range bs {
		var n int
		if b.Owner() == "" || b.Run() == "" {
			return errors.New("binding owner and run required")
		}
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_bindings WHERE owner_uid=$1 AND run_uid<>$2 AND state<>'Released'`, b.Owner(), b.Run()).Scan(&n); e != nil {
			return e
		}
		if n != 0 {
			return core.ErrConflict
		}
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_use_holds WHERE volume_key=$1 AND owner_uid=$2 AND reason LIKE 'preparation-handoff/%' AND reason<>$3`, b.Volume.Key(), b.Owner(), "preparation-handoff/"+b.Run()).Scan(&n); e != nil {
			return e
		}
		if n != 0 {
			return core.ErrConflict
		}
		// A logical owner is not permission to run two VMM incarnations. Only
		// the exact operation/run may re-enter; stopped runs never resurrect.
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_runs WHERE owner_uid=$1 AND run_uid=$2 AND state='Stopped'`, b.Owner(), b.Run()).Scan(&n); e != nil {
			return e
		}
		if n != 0 {
			return core.ErrConflict
		}
		if b.OwnerUID != "" || b.RunUID != "" {
			var epoch uint64
			if e = tx.QueryRow(ctx, `SELECT epoch FROM ssf_runs WHERE owner_uid=$1 AND run_uid=$2`, b.Owner(), b.Run()).Scan(&epoch); e != nil {
				return e
			}
			if epoch != b.Epoch {
				return core.ErrConflict
			}
		}
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_use_holds WHERE volume_key=$1 AND owner_uid<>$2 AND (NOT readonly OR NOT $3 OR $4)`, b.Volume.Key(), b.Owner(), b.Volume.ReadOnly, b.Volume.SingleConsumer()).Scan(&n); e != nil {
			return e
		}
		if n != 0 {
			return core.ErrConflict
		}
		if b.Volume.SingleNode() {
			if e = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_bindings WHERE volume_key=$1 AND node<>$2 AND state<>'Released'`, b.Volume.Key(), b.Node).Scan(&n); e != nil {
				return e
			}
			if n > 0 {
				return core.ErrConflict
			}
		}
		if b.Volume.SingleConsumer() {
			if e = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_bindings WHERE volume_key=$1 AND operation_id<>$2 AND state<>'Released'`, b.Volume.Key(), op).Scan(&n); e != nil {
				return e
			}
			if n > 0 {
				return core.ErrConflict
			}
		}
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_bindings WHERE volume_key=$1 AND state='Releasing'`, b.Volume.Key()).Scan(&n); e != nil {
			return e
		}
		if n > 0 {
			return core.ErrPending
		}
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_bindings WHERE volume_key=$1 AND state<>'Released' AND operation_id<>$2 AND (NOT readonly OR NOT $3)`, b.Volume.Key(), op, b.Volume.ReadOnly).Scan(&n); e != nil {
			return e
		}
		if n > 0 {
			return core.ErrConflict
		}
		data, e := json.Marshal(b)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, `INSERT INTO ssf_bindings(id,volume_key,operation_id,node,sandbox,readonly,state,descriptor,owner_uid,run_uid) VALUES($1,$2,$3,$4,$5,$6,'Acquired',$7,$8,$9) ON CONFLICT(id) DO NOTHING`, b.ID, b.Volume.Key(), op, b.Node, b.Sandbox, b.Volume.ReadOnly, data, b.Owner(), b.Run())
		if e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `DELETE FROM ssf_use_holds WHERE volume_key=$1 AND owner_uid=$2 AND reason=$3`, b.Volume.Key(), b.Owner(), "preparation-handoff/"+b.Run()); e != nil {
			return e
		}
	}
	_, e = tx.Exec(ctx, `UPDATE ssf_operations SET state='Acquired',updated=now() WHERE id=$1`, op)
	if e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Operation(ctx context.Context, id string) (core.Operation, error) {
	var o core.Operation
	e := s.executor(ctx).QueryRow(ctx, `SELECT id,digest,state,reason,updated FROM ssf_operations WHERE id=$1`, id).Scan(&o.ID, &o.Digest, &o.State, &o.Reason, &o.Updated)
	return o, e
}
func (s *Store) Begin(ctx context.Context, id, digest string) error {
	_, e := s.executor(ctx).Exec(ctx, `INSERT INTO ssf_operations(id,digest,state) VALUES($1,$2,'Resolving') ON CONFLICT DO NOTHING`, id, digest)
	if e != nil {
		return e
	}
	o, e := s.Operation(ctx, id)
	if e != nil {
		return e
	}
	if o.Digest != digest || o.State == "Released" || o.State == "Releasing" {
		return core.ErrConflict
	}
	return nil
}
func (s *Store) SetOperation(ctx context.Context, id, state, reason string) error {
	_, e := s.executor(ctx).Exec(ctx, `UPDATE ssf_operations SET state=$2,reason=$3,updated=now() WHERE id=$1`, id, state, reason)
	return e
}
func (s *Store) Bindings(ctx context.Context, op string) ([]core.Binding, error) {
	rows, e := s.executor(ctx).Query(ctx, `SELECT descriptor,state FROM ssf_bindings WHERE operation_id=$1 ORDER BY id`, op)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []core.Binding
	for rows.Next() {
		var data []byte
		var state string
		if e = rows.Scan(&data, &state); e != nil {
			return nil, e
		}
		var b core.Binding
		if e = json.Unmarshal(data, &b); e != nil {
			return nil, e
		}
		b.State = state
		out = append(out, b)
	}
	return out, rows.Err()
}

// Exclusive uses a session advisory lock held across an idempotent external RPC.
// A lost connection does not authorize a conflicting RPC: durable step state and
// identical input digest must still be checked by callers.
func (s *Store) Exclusive(ctx context.Context, key string, fn func(context.Context) error) error {
	c, ok := ctx.Value(sessionKey{}).(*pgxpool.Conn)
	if !ok {
		var err error
		c, err = s.Pool.Acquire(ctx)
		if err != nil {
			return err
		}
		defer c.Release()
		ctx = context.WithValue(ctx, sessionKey{}, c)
	}
	_, e := c.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1,0))`, key)
	if e != nil {
		return e
	}
	defer func() {
		unlock, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, e := c.Exec(unlock, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, key); e != nil {
			c.Conn().Close(unlock)
		}
	}()
	return fn(ctx)
}
func (s *Store) Step(ctx context.Context, id, digest string, fn func(context.Context) (any, error), out any) (result error) {
	start := time.Now()
	defer func() {
		if s.Observe != nil {
			s.Observe(strings.SplitN(id, "/", 2)[0], time.Since(start), result)
		}
	}()
	return s.Exclusive(ctx, "step/"+id, func(ctx context.Context) error {
		_, e := s.executor(ctx).Exec(ctx, `INSERT INTO ssf_steps(id,digest,state) VALUES($1,$2,'Intent') ON CONFLICT DO NOTHING`, id, digest)
		if e != nil {
			return e
		}
		var d, st string
		var data []byte
		e = s.executor(ctx).QueryRow(ctx, `SELECT digest,state,result FROM ssf_steps WHERE id=$1`, id).Scan(&d, &st, &data)
		if e != nil {
			return e
		}
		if d != digest {
			return core.ErrConflict
		}
		if st == "Done" {
			if out != nil {
				return json.Unmarshal(data, out)
			}
			return nil
		}
		v, e := fn(ctx)
		if e != nil {
			return e
		}
		data, e = json.Marshal(v)
		if e != nil {
			return e
		}
		_, e = s.executor(ctx).Exec(ctx, `UPDATE ssf_steps SET state='Done',result=$2,updated=now() WHERE id=$1`, id, data)
		if e == nil && out != nil {
			e = json.Unmarshal(data, out)
		}
		return e
	})
}
func (s *Store) Drain(ctx context.Context, uid string) error {
	tag, e := s.executor(ctx).Exec(ctx, `UPDATE ssf_volumes SET draining=true WHERE pvc_uid=$1`, uid)
	if e == nil && tag.RowsAffected() != 1 {
		return errors.New("unknown volume")
	}
	return e
}
func (s *Store) Permit(ctx context.Context, uid, action string, ttl time.Duration) error {
	if action != "Retain" && action != "Delete" {
		return errors.New("invalid reclaim action")
	}
	tx, e := s.executor(ctx).Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var key string
	var draining bool
	if e = tx.QueryRow(ctx, `SELECT key,draining FROM ssf_volumes WHERE pvc_uid=$1 FOR UPDATE`, uid).Scan(&key, &draining); e != nil {
		return e
	}
	var n int
	e = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM ssf_bindings WHERE volume_key=$1 AND state<>'Released') + (SELECT count(*) FROM ssf_use_holds WHERE volume_key=$1) + (SELECT count(*) FROM ssf_volume_maintenance WHERE volume_key=$1 AND state='Active') + (SELECT count(*) FROM ssf_attachments WHERE volume_key=$1 AND state<>'Detached')`, key).Scan(&n)
	if e != nil {
		return e
	}
	if !draining || n != 0 {
		return core.ErrPending
	}
	if _, e = tx.Exec(ctx, `INSERT INTO ssf_checkpoint_resources(uid) VALUES($1) ON CONFLICT DO NOTHING`, uid); e != nil {
		return e
	}
	var resourceState string
	if e = tx.QueryRow(ctx, `SELECT state FROM ssf_checkpoint_resources WHERE uid=$1 FOR UPDATE`, uid).Scan(&resourceState); e != nil {
		return e
	}
	if e = tx.QueryRow(ctx, `SELECT count(*) FROM ssf_checkpoint_pins WHERE resource_uid=$1`, uid).Scan(&n); e != nil {
		return e
	}
	if n != 0 {
		return core.ErrPending
	}
	if _, e = tx.Exec(ctx, `UPDATE ssf_checkpoint_resources SET state='Deleting' WHERE uid=$1`, uid); e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `INSERT INTO ssf_permits(pvc_uid,action,expires) VALUES($1,$2,$3) ON CONFLICT(pvc_uid) DO UPDATE SET action=excluded.action,expires=excluded.expires`, uid, action, time.Now().Add(ttl))
	if e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Allowed(ctx context.Context, uid, action string) (bool, error) {
	var ok bool
	e := s.executor(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_permits WHERE pvc_uid=$1 AND action=$2 AND expires>now())`, uid, action).Scan(&ok)
	return ok, e
}
func (s *Store) CountOther(ctx context.Context, key, node, binding string) (int, error) {
	var n int
	e := s.executor(ctx).QueryRow(ctx, `SELECT count(*) FROM ssf_bindings WHERE volume_key=$1 AND node=$2 AND id<>$3 AND state<>'Released'`, key, node, binding).Scan(&n)
	return n, e
}
func (s *Store) ReleaseBinding(ctx context.Context, id string) error {
	_, e := s.executor(ctx).Exec(ctx, `UPDATE ssf_bindings SET state='Released' WHERE id=$1`, id)
	return e
}
func (s *Store) Lookup(ctx context.Context, uid string) (core.Volume, error) {
	var v core.Volume
	var b []byte
	e := s.executor(ctx).QueryRow(ctx, `SELECT descriptor FROM ssf_volumes WHERE pvc_uid=$1`, uid).Scan(&b)
	if e == nil {
		e = json.Unmarshal(b, &v)
	}
	return v, e
}
func (s *Store) Pending(ctx context.Context) ([]string, error) {
	rows, e := s.executor(ctx).Query(ctx, `SELECT id FROM ssf_operations WHERE state NOT IN ('Ready','Released') ORDER BY updated`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			return nil, e
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// BeginRelease closes admission before any detach can enter an unknown state.
func (s *Store) BeginRelease(ctx context.Context, op string) error {
	tx, e := s.executor(ctx).Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var state string
	if e = tx.QueryRow(ctx, `SELECT state FROM ssf_operations WHERE id=$1 FOR UPDATE`, op).Scan(&state); e != nil {
		return e
	}
	rows, e := tx.Query(ctx, `SELECT DISTINCT volume_key FROM ssf_bindings WHERE operation_id=$1 ORDER BY volume_key`, op)
	if e != nil {
		return e
	}
	var keys []string
	for rows.Next() {
		var k string
		if e = rows.Scan(&k); e != nil {
			rows.Close()
			return e
		}
		keys = append(keys, k)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, k := range keys {
		if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "volume/"+k); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(ctx, `UPDATE ssf_bindings SET state='Releasing' WHERE operation_id=$1 AND state<>'Released'`, op); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE ssf_operations SET state='Releasing' WHERE id=$1 AND state<>'Released'`, op); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) ReadStep(ctx context.Context, id string, out any) error {
	var state string
	var b []byte
	e := s.executor(ctx).QueryRow(ctx, `SELECT state,result FROM ssf_steps WHERE id=$1`, id).Scan(&state, &b)
	if e != nil {
		return e
	}
	if state != "Done" {
		return core.ErrPending
	}
	return json.Unmarshal(b, out)
}
