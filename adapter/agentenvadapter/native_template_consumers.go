package agentenvadapter

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
)

// Storage pins alone cannot protect the native repository directory. Serialize
// template consumption against the original repository's delete admission too.
func (c *NativeControl) RetainNativeCheckpoint(ctx context.Context, source, kind, consumer string) (string, error) {
	if !safeID.MatchString(consumer) {
		return "", core.ErrConflict
	}
	if kind != "restore" && kind != "fork" {
		return c.retainNativeCheckpoint(ctx, source, kind, consumer)
	}
	var id, repository string
	err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT native_id,repository_id FROM agentenv_native_publications WHERE tenant=$1 AND reference=$2`, c.Principal.Tenant, source).Scan(&id, &repository)
	if errors.Is(err, pgx.ErrNoRows) {
		return c.retainNativeCheckpoint(ctx, source, kind, consumer)
	}
	if err != nil {
		return "", err
	}
	ref := "agentenv/ref/" + core.Hash(c.Principal.Tenant+"/"+kind+"/"+consumer)
	err = c.Ledger.Exclusive(ctx, "agentenv/publication/"+c.Principal.Tenant+"/"+repository+"/"+id, func(ctx context.Context) error {
		var state string
		if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_publications WHERE tenant=$1 AND native_id=$2 AND repository_id=$3`, c.Principal.Tenant, id, repository).Scan(&state); err != nil {
			return err
		}
		if state != "Published" && state != "Publishing" {
			return core.ErrConflict
		}
		// Persist before retaining/copying any artifact. An interrupted attempt keeps
		// the repository protected until its native request is conclusively settled.
		if _, err := c.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_template_consumers(reference,tenant,source_reference,request_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, ref, c.Principal.Tenant, source, consumer); err != nil {
			return err
		}
		var prior, tenant, request string
		if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT source_reference,tenant,request_id FROM agentenv_native_template_consumers WHERE reference=$1`, ref).Scan(&prior, &tenant, &request); err != nil {
			return err
		}
		if prior != source || tenant != c.Principal.Tenant || request != consumer {
			return core.ErrConflict
		}
		if _, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_template_consumers SET state='Active' WHERE reference=$1`, ref); err != nil {
			return err
		}
		retained, err := c.retainNativeCheckpoint(ctx, source, kind, consumer)
		if err == nil && retained != ref {
			return core.ErrConflict
		}
		return err
	})
	return ref, err
}

// Called after trusted Node Ready or after a persisted no-running-VM outcome.
// This releases the source repository read obligation, not CSI snapshot pins.
func (c *NativeControl) settleNativeTemplateConsumer(ctx context.Context, requestID string) error {
	_, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_template_consumers c SET state='Consumed'
 WHERE c.tenant=$1 AND c.request_id=$2 AND EXISTS(SELECT 1 FROM agentenv_native_runs r
 WHERE r.id=c.request_id AND r.state IN ('Ready','Released','Paused','Resumed','FailedResume','Unstarted','Revoked'))`, c.Principal.Tenant, requestID)
	return err
}

func (c *NativeControl) recoverNativeTemplateConsumers(ctx context.Context) error {
	rows, err := c.Ledger.Session(ctx).Query(ctx, `SELECT DISTINCT c.request_id FROM agentenv_native_template_consumers c JOIN agentenv_native_runs r ON r.id=c.request_id WHERE c.tenant=$1 AND c.state='Active' AND r.state IN ('Ready','Released','Paused','Resumed','FailedResume','Unstarted','Revoked') LIMIT 100`, c.Principal.Tenant)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err = c.Ledger.Exclusive(ctx, "agentenv/native-run/"+id, func(ctx context.Context) error { return c.settleNativeTemplateConsumer(ctx, id) }); err != nil {
			return err
		}
	}
	return nil
}
