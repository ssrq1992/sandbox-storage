package ledger

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

type PreparationRecord struct {
	PVC            core.PVCRef             `json:"pvc"`
	Profile        core.VolumeProfile      `json:"profile"`
	SourceRevision string                  `json:"sourceRevision"`
	SourceSpec     string                  `json:"sourceSpec"`
	State          string                  `json:"state"`
	Operation      string                  `json:"operation,omitempty"`
	Plan           core.Plan               `json:"plan"`
	Receipt        core.PreparationReceipt `json:"receipt"`
	Reason         string                  `json:"reason,omitempty"`
}

func (r PreparationRecord) IntentDigest() string {
	return core.Digest(struct {
		PVC        core.PVCRef
		Profile    core.VolumeProfile
		Source     string
		SourceSpec string
	}{r.PVC, r.Profile, r.SourceRevision, r.SourceSpec})
}

func (s *Store) Preparation(ctx context.Context, uid string) (PreparationRecord, error) {
	var r PreparationRecord
	var raw []byte
	err := s.executor(ctx).QueryRow(ctx, `SELECT descriptor FROM ssf_volume_preparations WHERE pvc_uid=$1`, uid).Scan(&raw)
	if err == nil {
		err = json.Unmarshal(raw, &r)
	}
	return r, err
}

// ObservePreparation may run before a PV exists. Names are never the key.
func (s *Store) ObservePreparation(ctx context.Context, r PreparationRecord) (PreparationRecord, error) {
	if r.PVC.UID == "" || r.PVC.Namespace == "" || r.PVC.Name == "" || r.SourceRevision == "" || r.Profile.Validate() != nil {
		return r, errors.New("preparation identity, source and profile required")
	}
	if r.State != "WaitingBinding" && r.State != "WaitingConsumer" && r.State != "PendingPreparation" {
		return r, errors.New("invalid initial preparation state")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	_, err = s.executor(ctx).Exec(ctx, `INSERT INTO ssf_volume_preparations(pvc_uid,digest,state,descriptor) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, r.PVC.UID, r.IntentDigest(), r.State, raw)
	if err != nil {
		return r, err
	}
	prior, err := s.Preparation(ctx, r.PVC.UID)
	if err == nil && prior.IntentDigest() != r.IntentDigest() {
		err = core.ErrConflict
	}
	return prior, err
}

// SavePreparation is used under the PVC preparation advisory lock. CAS prevents
// a lost database session or stale worker from moving a newer attempt backward.
func (s *Store) SavePreparation(ctx context.Context, previous, next PreparationRecord) error {
	if previous.IntentDigest() != next.IntentDigest() {
		return core.ErrConflict
	}
	if !preparationTransition(previous.State, next.State) {
		return errors.New("invalid preparation transition")
	}
	if previous.Operation != "" && previous.Operation != next.Operation {
		return core.ErrConflict
	}
	if previous.Plan.Node != "" && core.PreparationDigest(previous.Plan) != core.PreparationDigest(next.Plan) &&
		!core.PreparationReboot(previous.Plan, next.Plan) {
		return core.ErrConflict
	}
	if previous.Receipt.UUID != "" && previous.Receipt.UUID != next.Receipt.UUID {
		return core.ErrConflict
	}
	if next.State == "VolumeReady" && (next.Receipt.Rejected || !next.Receipt.Released || next.Receipt.UUID == "" || next.Receipt.Operation != next.Operation) {
		return errors.New("volume ready requires released preparation evidence")
	}
	old, err := json.Marshal(previous)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	tag, err := s.executor(ctx).Exec(ctx, `UPDATE ssf_volume_preparations SET state=$2,descriptor=$3,updated=now() WHERE pvc_uid=$1 AND descriptor=$4::jsonb`, next.PVC.UID, next.State, raw, old)
	if err == nil && tag.RowsAffected() != 1 {
		return core.ErrConflict
	}
	return err
}

func preparationTransition(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case "WaitingBinding", "WaitingConsumer", "PendingPreparation":
		return to == "Preparing" || to == "Failed" || to == "PendingPreparation"
	case "Preparing", "NeedsRecovery":
		return to == "ContentReady" || to == "NeedsRecovery" || to == "Failed"
	case "ContentReady":
		return to == "VolumeReady" || to == "NeedsRecovery"
	}
	return false
}

func (s *Store) HasPreparation(ctx context.Context, uid string) (bool, error) {
	_, err := s.Preparation(ctx, uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// PreparationHandoff reserves the released device for exactly one incoming
// run. It shares the owner/volume lock order with Acquire and deletion.
func (s *Store) PreparationHandoff(ctx context.Context, op, owner, run string) error {
	if owner == "" || run == "" {
		return errors.New("handoff owner and run required")
	}
	bs, err := s.Bindings(ctx, op)
	if err != nil {
		return err
	}
	if len(bs) != 1 || bs[0].Sandbox != "" {
		return core.ErrConflict
	}
	b := bs[0]
	tx, err := s.executor(ctx).Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, key := range []string{"owner/" + owner, "volume/" + b.Volume.Key()} {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
			return err
		}
	}
	var blocked bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_owner_tombstones WHERE owner_uid=$1) OR EXISTS(SELECT 1 FROM ssf_volumes WHERE key=$2 AND draining)`, owner, b.Volume.Key()).Scan(&blocked); err != nil {
		return err
	}
	if blocked {
		return core.ErrConflict
	}
	var state string
	if err = tx.QueryRow(ctx, `SELECT state FROM ssf_bindings WHERE id=$1 FOR UPDATE`, b.ID).Scan(&state); err != nil {
		return err
	}
	// A retry after controller release is allowed only if the original hold
	// still exists; it must never create a new hold across an unprotected gap.
	reason := "preparation-handoff/" + run
	if state == "Released" {
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_use_holds WHERE volume_key=$1 AND owner_uid=$2 AND reason=$3)`, b.Volume.Key(), owner, reason).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return core.ErrConflict
		}
		return tx.Commit(ctx)
	}
	var conflicting bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ssf_use_holds WHERE volume_key=$1 AND (owner_uid<>$2 OR reason<>$3))`, b.Volume.Key(), owner, reason).Scan(&conflicting); err != nil {
		return err
	}
	if conflicting {
		return core.ErrConflict
	}
	_, err = tx.Exec(ctx, `INSERT INTO ssf_use_holds(volume_key,owner_uid,readonly,reason) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, b.Volume.Key(), owner, b.Volume.ReadOnly, reason)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
