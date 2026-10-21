package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
)

// BeginNativePublication precedes the repository write. Unknown publication
// outcomes stay Publishing and cannot be collected as an absent template.
func (c *NativeControl) BeginNativePublication(ctx context.Context, checkpoint json.RawMessage, id, repository string) (map[string]any, error) {
	var out map[string]any
	if !safeID.MatchString(id) || len(repository) != 64 || !isHex(repository) {
		return nil, core.ErrConflict
	}
	err := c.Ledger.Exclusive(ctx, "agentenv/publication/"+c.Principal.Tenant+"/"+repository+"/"+id, func(ctx context.Context) error {
		var state string
		err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_publications WHERE tenant=$1 AND native_id=$2 AND repository_id=$3`, c.Principal.Tenant, id, repository).Scan(&state)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil && state != "Publishing" && state != "Published" {
			return core.ErrConflict
		}
		out, err = c.PublishNativeCheckpoint(ctx, checkpoint, core.Hash(repository+"/"+id))
		if err != nil {
			return err
		}
		raw, err := json.Marshal(out)
		if err != nil {
			return err
		}
		_, err = c.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_publications(tenant,native_id,reference,descriptor,repository_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, c.Principal.Tenant, id, out["reference"], raw, repository)
		return err
	})
	return out, err
}

// Published is a native repository completion receipt, sent after its commit.
// Repeated completion cannot reopen a deleting or deleted immutable ID.
func (c *NativeControl) NativePublished(ctx context.Context, id, repository string) error {
	tag, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_publications SET state='Published' WHERE tenant=$1 AND native_id=$2 AND repository_id=$3 AND state IN ('Publishing','Published')`, c.Principal.Tenant, id, repository)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return core.ErrConflict
	}
	// Repository publication has its own independent pin. Transfer protection
	// away from the transient capture, including after a crash/replayed receipt.
	var descriptor []byte
	if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT descriptor FROM agentenv_native_publications WHERE tenant=$1 AND native_id=$2 AND repository_id=$3`, c.Principal.Tenant, id, repository).Scan(&descriptor); err != nil {
		return err
	}
	var manifest consume.CaptureManifest
	if err = json.Unmarshal(descriptor, &manifest); err != nil {
		return err
	}
	if manifest.Intent.ID != "" {
		return c.Ledger.ReleaseCheckpoint(ctx, manifest.Intent.ID)
	}
	return nil
}

func (c *NativeControl) BeginNativePublicationDelete(ctx context.Context, id, repository string) (bool, error) {
	managed := false
	err := c.Ledger.Exclusive(ctx, "agentenv/publication/"+c.Principal.Tenant+"/"+repository+"/"+id, func(ctx context.Context) error {
		var state string
		err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_publications WHERE tenant=$1 AND native_id=$2 AND repository_id=$3`, c.Principal.Tenant, id, repository).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		managed = true
		if state == "Publishing" {
			return core.ErrPending
		}
		if state == "Deleted" || state == "Deleting" {
			return nil
		}
		if state != "Published" {
			return core.ErrConflict
		}
		var consuming bool
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT EXISTS(
            SELECT 1 FROM agentenv_native_template_consumers c
            JOIN agentenv_native_publications p ON p.reference=c.source_reference AND p.tenant=c.tenant
            WHERE p.tenant=$1 AND p.native_id=$2 AND p.repository_id=$3 AND c.state='Active')`, c.Principal.Tenant, id, repository).Scan(&consuming); err != nil {
			return err
		}
		if consuming {
			return core.ErrPending
		}
		_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_publications SET state='Deleting' WHERE tenant=$1 AND native_id=$2 AND repository_id=$3`, c.Principal.Tenant, id, repository)
		return err
	})
	return managed, err
}

// CompleteNativePublicationDelete is invoked only after the authenticated node
// plugin verifies the original repository's explicit immutable-ID absence.
// A failed GET, HTTP 404 or an unresolved alias is not that receipt.
func (c *NativeControl) CompleteNativePublicationDelete(ctx context.Context, id, repository string) error {
	return c.Ledger.Exclusive(ctx, "agentenv/publication/"+c.Principal.Tenant+"/"+repository+"/"+id, func(ctx context.Context) error {
		var state, ref string
		if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state,reference FROM agentenv_native_publications WHERE tenant=$1 AND native_id=$2 AND repository_id=$3`, c.Principal.Tenant, id, repository).Scan(&state, &ref); err != nil {
			return err
		}
		if state == "Deleted" {
			return nil
		}
		if state != "Deleting" {
			return core.ErrConflict
		}
		if err := c.Ledger.ReleaseCheckpoint(ctx, ref); err != nil {
			return err
		}
		_, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_publications SET state='Deleted' WHERE tenant=$1 AND native_id=$2 AND repository_id=$3`, c.Principal.Tenant, id, repository)
		return err
	})
}
