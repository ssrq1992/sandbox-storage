package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SnapshotPin is installed only after the caller has verified the native
// barrier (for live content) and the immutable published head. Kubernetes
// annotations alone are not a capture authorization.
type SnapshotPin struct {
	ID              string `json:"id"`
	RequestName     string `json:"request_name"`
	SourceHandle    string `json:"source_handle"`
	HeadRevision    string `json:"head_revision"`
	CapacityBytes   uint64 `json:"capacity_bytes"`
	FSUUID          string `json:"fs_uuid"`
	SnapshotUID     string `json:"snapshot_uid"`
	ContentUID      string `json:"content_uid"`
	CaptureDigest   string `json:"capture_digest"`
	CreatedUnixNano int64  `json:"created_unix_nano"`
}

const snapshotReferenceSchema = `
CREATE TABLE IF NOT EXISTS ssf_overlaybd.snapshot_references (
 operation_id text PRIMARY KEY,
 snapshot_id text NOT NULL REFERENCES ssf_overlaybd.snapshot_pins(id),
 state text NOT NULL CHECK(state IN ('Pinned','Released'))
);
CREATE INDEX IF NOT EXISTS snapshot_references_live ON ssf_overlaybd.snapshot_references(snapshot_id) WHERE state='Pinned';
CREATE TABLE IF NOT EXISTS ssf_overlaybd.snapshot_tombstones(id text PRIMARY KEY);
CREATE UNIQUE INDEX IF NOT EXISTS snapshot_content_identity ON ssf_overlaybd.snapshot_pins((identity->>'content_uid'));
CREATE UNIQUE INDEX IF NOT EXISTS snapshot_object_identity ON ssf_overlaybd.snapshot_pins((identity->>'snapshot_uid'));
INSERT INTO ssf_overlaybd.schema_versions VALUES (4),(5) ON CONFLICT DO NOTHING;
`

func (p SnapshotPin) validate() error {
	if p.ID != Identity("snapshot", p.RequestName) || p.RequestName == "" || !validDigest(p.HeadRevision) || !validDigest(p.CaptureDigest) || !validFilesystemUUID(p.FSUUID) || p.CapacityBytes == 0 || p.CapacityBytes > math.MaxInt64 || p.CreatedUnixNano <= 0 || p.SnapshotUID == "" || p.ContentUID == "" {
		return errors.New("verified immutable snapshot identity required")
	}
	_, _, err := ParseVolumeHandle(p.SourceHandle)
	return err
}

// RegisterSnapshot serializes with volume deletion. A deleted snapshot name
// is a permanent tombstone; retries cannot republish it with another head.
func (j *Journal) RegisterSnapshot(ctx context.Context, p SnapshotPin) error {
	if err := p.validate(); err != nil {
		return err
	}
	raw, err := canonical(p)
	if err != nil {
		return err
	}
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "overlaybd/snapshot/"+p.ID); err != nil {
		return err
	}
	var deleted bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_overlaybd.snapshot_tombstones WHERE id=$1)`, p.ID).Scan(&deleted); err != nil {
		return err
	}
	if deleted {
		return ErrConflict
	}
	var state string
	if err = tx.QueryRow(ctx, `SELECT state FROM ssf_overlaybd.volumes WHERE handle=$1 FOR UPDATE`, p.SourceHandle).Scan(&state); err != nil {
		return err
	}
	if state != "Ready" {
		return ErrConflict
	}
	tag, err := tx.Exec(ctx, `INSERT INTO ssf_overlaybd.snapshot_pins(id,request_name,source_handle,head_revision,identity)
 VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET id=excluded.id
 WHERE ssf_overlaybd.snapshot_pins.state='Ready' AND ssf_overlaybd.snapshot_pins.identity=excluded.identity`, p.ID, p.RequestName, p.SourceHandle, p.HeadRevision, raw)
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == "23505" {
			return ErrConflict
		}
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return tx.Commit(ctx)
}

func (j *Journal) Snapshot(ctx context.Context, id string) (SnapshotPin, error) {
	var raw []byte
	err := j.Pool.QueryRow(ctx, `SELECT identity FROM ssf_overlaybd.snapshot_pins WHERE id=$1 AND state='Ready'`, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return SnapshotPin{}, ErrNotFound
	}
	if err != nil {
		return SnapshotPin{}, err
	}
	var p SnapshotPin
	if err = json.Unmarshal(raw, &p); err != nil {
		return p, err
	}
	if err = p.validate(); err != nil {
		return p, err
	}
	if p.ID != id {
		return p, ErrConflict
	}
	return p, nil
}

// PinSnapshotSource fixes the source before a clone can be dispatched. Released
// references remain tombstoned, so a delayed retry cannot reacquire ownership.
func (j *Journal) PinSnapshotSource(ctx context.Context, id, operation string) (SnapshotPin, error) {
	if operation == "" {
		return SnapshotPin{}, errors.New("clone operation required")
	}
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		return SnapshotPin{}, err
	}
	defer tx.Rollback(context.Background())
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT identity FROM ssf_overlaybd.snapshot_pins WHERE id=$1 AND state='Ready' FOR UPDATE`, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return SnapshotPin{}, ErrNotFound
	}
	if err != nil {
		return SnapshotPin{}, err
	}
	var p SnapshotPin
	if err = json.Unmarshal(raw, &p); err != nil {
		return p, err
	}
	if err = p.validate(); err != nil {
		return p, err
	}
	if p.ID != id {
		return p, ErrConflict
	}
	tag, err := tx.Exec(ctx, `INSERT INTO ssf_overlaybd.snapshot_references(operation_id,snapshot_id,state) VALUES($1,$2,'Pinned')
 ON CONFLICT(operation_id) DO UPDATE SET operation_id=excluded.operation_id
 WHERE ssf_overlaybd.snapshot_references.snapshot_id=excluded.snapshot_id AND ssf_overlaybd.snapshot_references.state='Pinned'`, operation, id)
	if err != nil {
		return p, err
	}
	if tag.RowsAffected() != 1 {
		return p, ErrConflict
	}
	return p, tx.Commit(ctx)
}

// ReleaseSnapshotSource is called only after the clone has durably published
// its independent native head, or has been definitively cancelled before I/O.
func (j *Journal) ReleaseSnapshotSource(ctx context.Context, id, operation string) error {
	tag, err := j.Pool.Exec(ctx, `UPDATE ssf_overlaybd.snapshot_references SET state='Released' WHERE operation_id=$1 AND snapshot_id=$2`, operation, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return nil
}

// DeleteSnapshotPin is logical deletion only. Physical layer collection must
// additionally account for all published heads and native layer references.
func (j *Journal) DeleteSnapshotPin(ctx context.Context, id string) error {
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "overlaybd/snapshot/"+id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ssf_overlaybd.snapshot_tombstones(id) VALUES($1) ON CONFLICT DO NOTHING`, id); err != nil {
		return err
	}
	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM ssf_overlaybd.snapshot_pins WHERE id=$1 FOR UPDATE`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	var pinned bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_overlaybd.snapshot_references WHERE snapshot_id=$1 AND state='Pinned')`, id).Scan(&pinned); err != nil {
		return err
	}
	if pinned {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE ssf_overlaybd.snapshot_pins SET state='Deleted' WHERE id=$1`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// BeginVolumeDeletion shares the source-row lock with RegisterSnapshot. This
// closes the gap between checking references and dispatching native deletion.
func (j *Journal) BeginVolumeDeletion(ctx context.Context, handle string) error {
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var state string
	if err = tx.QueryRow(ctx, `SELECT state FROM ssf_overlaybd.volumes WHERE handle=$1 FOR UPDATE`, handle).Scan(&state); err != nil {
		return err
	}
	if state == "Deleted" {
		return tx.Commit(ctx)
	}
	var pinned bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_overlaybd.snapshot_pins WHERE source_handle=$1 AND state<>'Deleted')`, handle).Scan(&pinned); err != nil {
		return err
	}
	if pinned {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE ssf_overlaybd.volumes SET state='Deleting' WHERE handle=$1`, handle); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CancelSnapshotRegistration wins only before publication in the Driver catalog.
// It must not turn a concurrent successful Retain snapshot into a deletion.
func (j *Journal) CancelSnapshotRegistration(ctx context.Context, id string) (bool, error) {
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "overlaybd/snapshot/"+id); err != nil {
		return false, err
	}
	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM ssf_overlaybd.snapshot_pins WHERE id=$1 FOR UPDATE`, id).Scan(&state)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if err == nil && state != "Deleted" {
		return false, tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ssf_overlaybd.snapshot_tombstones(id) VALUES($1) ON CONFLICT DO NOTHING`, id); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
