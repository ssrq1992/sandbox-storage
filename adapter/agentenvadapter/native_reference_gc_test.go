package agentenvadapter

import (
	"encoding/json"
	"errors"
	"ssf.dev/ssf/core"
	"testing"
)

func TestNativeReferenceGCWaitsForLogicalOwnerAndKeepsRetryableRuns(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	intent, _ := json.Marshal(NativeIntent{Tenant: c.Principal.Tenant})
	for _, state := range []string{"Ready", "Paused", "Dispatched", "Unstarted", "FailedResume", "Released", "Revoked", "Resumed"} {
		request := id + state
		owner := request + "owner"
		ref := request + "ref"
		_, err := c.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_runs(id,digest,owner_uid,run_uid,state,intent) VALUES($1,'digest',$2,$3,$4,$5)`, request, owner, request+"run", state, intent)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Ledger.BeginCheckpoint(ctx, ref, c.Principal.Tenant+"/restore/"+request, map[string]string{"test": request}); err != nil {
			t.Fatal(err)
		}
		if err = c.Ledger.PinCheckpoint(ctx, ref, request+"snapshot"); err != nil {
			t.Fatal(err)
		}
		if err = c.Ledger.CommitCheckpoint(ctx, ref, map[string]string{"test": request}); err != nil {
			t.Fatal(err)
		}
		if state == "Resumed" {
			_, err = c.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_runs(id,digest,owner_uid,run_uid,state,intent) VALUES($1,'digest',$2,$3,'Ready',$4)`, request+"next", owner, request+"next-run", intent)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := c.recoverNativeReferences(ctx); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"Ready", "Paused", "Dispatched", "Unstarted", "FailedResume", "Released", "Revoked", "Resumed"} {
		err := c.Ledger.PermitSnapshotDelete(ctx, id+state+"snapshot")
		if state == "Released" || state == "Revoked" {
			if err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, core.ErrPending) {
			t.Fatalf("collected %s reference: %v", state, err)
		}
	}
	if _, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Released' WHERE id=$1`, id+"Resumednext"); err != nil {
		t.Fatal(err)
	}
	if err := c.recoverNativeReferences(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Ledger.PermitSnapshotDelete(ctx, id+"Resumedsnapshot"); err != nil {
		t.Fatal(err)
	}
}
