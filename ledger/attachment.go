package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

// EnsureAttachment shares current publish_context only while a durable holder
// keeps the attachment alive. Detached generations are never reused.
func (s *Store) EnsureAttachment(ctx context.Context, key, node, fingerprint, nodeID string, attach func() (map[string]string, error)) (map[string]string, error) {
	var out map[string]string
	e := s.Exclusive(ctx, "volume/"+key, func(ctx context.Context) error {
		var oldFingerprint, oldNode, state string
		var data []byte
		e := s.executor(ctx).QueryRow(ctx, `SELECT fingerprint,node_id,state,publish_context FROM ssf_attachments WHERE volume_key=$1 AND node=$2`, key, node).Scan(&oldFingerprint, &oldNode, &state, &data)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		if state != "" && state != "Detached" {
			if oldFingerprint != fingerprint || oldNode != nodeID {
				return core.ErrConflict
			}
			if state == "Detaching" {
				return core.ErrPending
			}
			if state == "Attached" {
				return json.Unmarshal(data, &out)
			}
		}
		_, e = s.executor(ctx).Exec(ctx, `INSERT INTO ssf_attachments(volume_key,node,fingerprint,node_id,state) VALUES($1,$2,$3,$4,'Attaching') ON CONFLICT(volume_key,node) DO UPDATE SET fingerprint=excluded.fingerprint,node_id=excluded.node_id,state='Attaching',publish_context=NULL`, key, node, fingerprint, nodeID)
		if e != nil {
			return e
		}
		out, e = attach()
		if e != nil {
			return e
		}
		data, e = json.Marshal(out)
		if e != nil {
			return e
		}
		_, e = s.executor(ctx).Exec(ctx, `UPDATE ssf_attachments SET state='Attached',publish_context=$3 WHERE volume_key=$1 AND node=$2`, key, node, data)
		return e
	})
	return out, e
}

// DetachAttachment must be called under Exclusive("volume/"+key) after checking
// the last holder. Its durable intent blocks conflicting reattachment on retry.
func (s *Store) DetachAttachment(ctx context.Context, key, node string, detach func(string) error) error {
	var nodeID, state string
	e := s.executor(ctx).QueryRow(ctx, `SELECT node_id,state FROM ssf_attachments WHERE volume_key=$1 AND node=$2`, key, node).Scan(&nodeID, &state)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if state == "Detached" {
		return nil
	}
	if state == "Attaching" {
		return core.ErrPending
	}
	if _, e = s.executor(ctx).Exec(ctx, `UPDATE ssf_attachments SET state='Detaching' WHERE volume_key=$1 AND node=$2`, key, node); e != nil {
		return e
	}
	if e = detach(nodeID); e != nil {
		return e
	}
	_, e = s.executor(ctx).Exec(ctx, `UPDATE ssf_attachments SET state='Detached',publish_context=NULL WHERE volume_key=$1 AND node=$2`, key, node)
	return e
}

// AttachmentState is an observation, never authorization to detach. Missing
// records mean no attachment intent was issued through this ledger.
func (s *Store) AttachmentState(ctx context.Context, key, node string) (state, nodeID string, err error) {
	err = s.executor(ctx).QueryRow(ctx, `SELECT state,node_id FROM ssf_attachments WHERE volume_key=$1 AND node=$2`, key, node).Scan(&state, &nodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	return
}
