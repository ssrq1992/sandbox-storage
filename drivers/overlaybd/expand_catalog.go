package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

// CommitExpansion changes only current capacity after a durable, validated
// native publication. Initial create head and input remain immutable so that
// provisioner retries cannot shrink/reinitialize an already expanded volume.
func (j *Journal) CommitExpansion(ctx context.Context, operationID string) (CatalogVolume, error) {
	var empty CatalogVolume
	op, err := j.Get(ctx, operationID)
	if err != nil {
		return empty, err
	}
	if op.Kind != "expand" || op.State != Succeeded {
		return empty, ErrTransition
	}
	var intent ExpansionIntent
	var receipt api.VolumeReceipt
	if err = json.Unmarshal(op.Input, &intent); err != nil {
		return empty, err
	}
	if err = json.Unmarshal(op.Result, &receipt); err != nil {
		return empty, err
	}
	request := &api.ExpandVolumeRequest{Operation: &api.OperationIdentity{OperationId: op.ID, InputDigest: op.Digest, RepositoryId: intent.Volume.RepositoryID, VolumeId: op.VolumeID}}
	if err = validateExpansion(intent, request, &receipt); err != nil {
		return empty, err
	}
	if intent.Volume.NativeID != op.VolumeID || intent.Capacity <= intent.Volume.CapacityBytes {
		return empty, ErrConflict
	}
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback(ctx)
	var raw []byte
	var state string
	if err = tx.QueryRow(ctx, `SELECT identity,state FROM ssf_overlaybd.volumes WHERE handle=$1 FOR UPDATE`, intent.Volume.Handle).Scan(&raw, &state); err != nil {
		return empty, err
	}
	if state != "Ready" {
		return empty, ErrConflict
	}
	var current CatalogVolume
	if err = json.Unmarshal(raw, &current); err != nil {
		return empty, err
	}
	originalSize := current.CapacityBytes
	current.CapacityBytes = intent.Volume.CapacityBytes
	equal, err := canonical(current)
	if err != nil {
		return empty, err
	}
	expected, err := canonical(intent.Volume)
	if err != nil {
		return empty, err
	}
	if string(equal) != string(expected) {
		return empty, errors.New("catalog identity changed during expansion")
	}
	if originalSize != intent.Volume.CapacityBytes && originalSize < intent.Capacity {
		return empty, ErrConflict
	}
	current.CapacityBytes = max(originalSize, intent.Capacity)
	raw, err = canonical(current)
	if err != nil {
		return empty, err
	}
	if _, err = tx.Exec(ctx, `UPDATE ssf_overlaybd.volumes SET identity=$2 WHERE handle=$1`, current.Handle, raw); err != nil {
		return empty, err
	}
	return current, tx.Commit(ctx)
}
