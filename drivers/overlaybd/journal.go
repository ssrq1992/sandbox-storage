// Package overlaybd implements the persistent boundaries of the native
// OverlayBD CSI driver. Runtime use authorization remains in the SSF ledger.
package overlaybd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DriverName = "overlaybd.csi.ssf.dev"
const BackendProtocol = "ssf.overlaybd.backend.v1"
const Profile = "agentenv-overlaybd-ext4-v1"

var (
	ErrConflict   = errors.New("overlaybd operation identity or revision conflict")
	ErrNotFound   = errors.New("overlaybd operation not found")
	ErrTransition = errors.New("unsafe overlaybd operation transition")
)

type OperationState string

const (
	Pending       OperationState = "Pending"
	Applying      OperationState = "Applying"
	NeedsRecovery OperationState = "NeedsRecovery"
	Succeeded     OperationState = "Succeeded"
	Failed        OperationState = "Failed"
)

// Journal stores intent before any repository or daemon side effect. It is
// separate from the public ledger and cannot independently grant a writer.
type Journal struct{ Pool *pgxpool.Pool }

type Operation struct {
	ID       string          `json:"id"`
	Kind     string          `json:"kind"`
	Name     string          `json:"name"`
	Digest   string          `json:"digest"`
	VolumeID string          `json:"volume_id"`
	State    OperationState  `json:"state"`
	Revision int64           `json:"revision"`
	Input    json.RawMessage `json:"input"`
	Result   json.RawMessage `json:"result,omitempty"`
	Reason   string          `json:"reason,omitempty"`
}

const schema = `
CREATE SCHEMA IF NOT EXISTS ssf_overlaybd;
CREATE TABLE IF NOT EXISTS ssf_overlaybd.operations (
 id text PRIMARY KEY,
 kind text NOT NULL,
 name text NOT NULL,
 digest text NOT NULL,
 volume_id text NOT NULL,
 state text NOT NULL CHECK (state IN ('Pending','Applying','NeedsRecovery','Succeeded','Failed')),
 revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
 input jsonb NOT NULL,
 result jsonb,
 reason text NOT NULL DEFAULT '',
 updated timestamptz NOT NULL DEFAULT now(),
 UNIQUE (kind, name)
);
CREATE INDEX IF NOT EXISTS operations_recovery ON ssf_overlaybd.operations (state, updated);
CREATE TABLE IF NOT EXISTS ssf_overlaybd.schema_versions (version integer PRIMARY KEY);
INSERT INTO ssf_overlaybd.schema_versions VALUES (1) ON CONFLICT DO NOTHING;
`

func (j *Journal) Migrate(ctx context.Context) error {
	if j.Pool == nil {
		return errors.New("overlaybd journal requires PostgreSQL")
	}
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('ssf-overlaybd-schema',0))`); err != nil {
		return err
	}
	var versionTable *string
	if err = tx.QueryRow(ctx, `SELECT to_regclass('ssf_overlaybd.schema_versions')::text`).Scan(&versionTable); err != nil {
		return err
	}
	if versionTable != nil {
		var version int
		if err = tx.QueryRow(ctx, `SELECT COALESCE(max(version),0) FROM ssf_overlaybd.schema_versions`).Scan(&version); err != nil {
			return err
		}
		if version > 5 {
			return errors.New("OverlayBD schema is newer than this driver")
		}
		if version == 5 {
			return tx.Commit(ctx)
		}
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, catalogSchema); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, snapshotReferenceSchema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func digest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

// Identity is deterministic across service replicas and process restarts.
func Identity(kind, name string) string { return "obd_" + digest([]byte(kind+"\x00"+name)) }

func canonical(input any) ([]byte, error) {
	// Marshal a typed value, then normalize JSON numbers without float64 loss.
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// Reserve returns the original immutable intent on retries. Different input
// under the same CSI request name never starts another backend operation.
func (j *Journal) Reserve(ctx context.Context, kind, name, volumeID string, input any) (Operation, error) {
	if kind == "" || name == "" || volumeID == "" {
		return Operation{}, errors.New("operation kind, name and volume ID are required")
	}
	raw, err := canonical(input)
	if err != nil {
		return Operation{}, err
	}
	id, hash := Identity(kind, name), digest(raw)
	_, err = j.Pool.Exec(ctx, `INSERT INTO ssf_overlaybd.operations(id,kind,name,digest,volume_id,state,input)
 VALUES($1,$2,$3,$4,$5,'Pending',$6) ON CONFLICT(kind,name) DO NOTHING`, id, kind, name, hash, volumeID, raw)
	if err != nil {
		return Operation{}, err
	}
	op, err := j.Get(ctx, id)
	if err != nil {
		return Operation{}, err
	}
	if op.Digest != hash || op.VolumeID != volumeID {
		return Operation{}, ErrConflict
	}
	return op, nil
}

const columns = `id,kind,name,digest,volume_id,state,revision,input,result,reason`

func scan(row pgx.Row) (Operation, error) {
	var op Operation
	err := row.Scan(&op.ID, &op.Kind, &op.Name, &op.Digest, &op.VolumeID, &op.State, &op.Revision, &op.Input, &op.Result, &op.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return op, err
}

func (j *Journal) Get(ctx context.Context, id string) (Operation, error) {
	return scan(j.Pool.QueryRow(ctx, `SELECT `+columns+` FROM ssf_overlaybd.operations WHERE id=$1`, id))
}

func validTransition(from, to OperationState) bool {
	switch from {
	case Pending:
		return to == Applying || to == Failed
	case Applying:
		return to == NeedsRecovery || to == Succeeded || to == Failed
	case NeedsRecovery:
		return to == Succeeded || to == Failed
	default:
		return false
	}
}

// Finish uses the exact revision the executor claimed. Recovery can record an
// inspected result, but may not reset an unknown operation to Pending/Applying.
func (j *Journal) Finish(ctx context.Context, expected Operation, next OperationState, result any, reason string) (Operation, error) {
	if !validTransition(expected.State, next) {
		return Operation{}, ErrTransition
	}
	var raw []byte
	var err error
	if result != nil {
		raw, err = canonical(result)
		if err != nil {
			return Operation{}, err
		}
	}
	if next == Succeeded && (len(raw) == 0 || bytes.Equal(raw, []byte("null"))) {
		return Operation{}, errors.New("success requires a durable backend receipt")
	}
	op, err := scan(j.Pool.QueryRow(ctx, `UPDATE ssf_overlaybd.operations SET state=$4,result=$5,reason=$6,revision=revision+1,updated=now()
 WHERE id=$1 AND revision=$2 AND state=$3 RETURNING `+columns, expected.ID, expected.Revision, expected.State, next, raw, reason))
	if errors.Is(err, ErrNotFound) {
		return Operation{}, ErrConflict
	}
	return op, err
}

// Recoverable includes Applying: a service may have died before recording that
// its remote call had an unknown outcome. Callers must Inspect, never reformat.
func (j *Journal) Recoverable(ctx context.Context, limit int) ([]Operation, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("invalid recovery page size %d", limit)
	}
	rows, err := j.Pool.Query(ctx, `SELECT `+columns+` FROM ssf_overlaybd.operations WHERE state IN ('Applying','NeedsRecovery') ORDER BY updated,id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Operation
	for rows.Next() {
		op, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}
