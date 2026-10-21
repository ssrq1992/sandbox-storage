package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"ssf.dev/ssf/core"
	"ssf.dev/ssf/ledger"
)

func nativeLifecycleStore(t *testing.T) (*NativeControl, context.Context, string) {
	t.Helper()
	url := os.Getenv("SSF_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated PostgreSQL: SSF_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	s, err := ledger.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = MigrateNative(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err = MigrateNative(ctx, s); err != nil {
		t.Fatalf("native migration replay: %v", err)
	}
	id := fmt.Sprintf("native-lifecycle-%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = s.Pool.Exec(ctx, `DELETE FROM agentenv_native_runs WHERE id=$1`, id) })
	d := NativeDispatch{RequestID: id, OwnerUID: id + "owner", RunUID: id + "run", Candidate: NativeCandidate{ID: "node"}}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	intent := NativeIntent{RequestID: id, Tenant: "test", Body: []byte("{}"), BodyDigest: core.Hash("{}")}
	intentRaw, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO agentenv_native_runs(id,digest,owner_uid,run_uid,state,intent,dispatch) VALUES($1,$2,$3,$4,'Ready',$5,$6)`, id, core.Digest(intent), d.OwnerUID, d.RunUID, intentRaw, raw)
	if err != nil {
		t.Fatal(err)
	}
	return &NativeControl{Ledger: s, Principal: Principal{Tenant: "test"}}, ctx, id
}

func TestNativePauseStateTransitions(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	if err := c.RetainPause(ctx, id, "wrong-node"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("wrong node: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := c.RetainPause(ctx, id, "node"); err != nil {
			t.Fatal(err)
		}
	}
	var state string
	if err := c.Ledger.Pool.QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "Pausing" {
		t.Fatalf("pause state: %s", state)
	}
	for i := 0; i < 2; i++ {
		if err := c.CancelPause(ctx, id, "node"); err != nil {
			t.Fatal(err)
		}
	}
	for _, terminal := range []string{"Paused", "Released", "Revoked", "FailedResume", "Resumed", "Deleting"} {
		if _, err := c.Ledger.Pool.Exec(ctx, `UPDATE agentenv_native_runs SET state=$2 WHERE id=$1`, id, terminal); err != nil {
			t.Fatal(err)
		}
		if err := c.RetainPause(ctx, id, "node"); !errors.Is(err, core.ErrConflict) {
			t.Fatalf("pause resurrected %s: %v", terminal, err)
		}
		if err := c.CancelPause(ctx, id, "node"); !errors.Is(err, core.ErrConflict) {
			t.Fatalf("cancel resurrected %s: %v", terminal, err)
		}
		if err := c.Ready(ctx, id, "node", "native-id", "proof"); !errors.Is(err, core.ErrConflict) {
			t.Fatalf("ready resurrected %s: %v", terminal, err)
		}
		intent := NativeIntent{RequestID: id, Tenant: "test", Body: []byte("{}"), BodyDigest: core.Hash("{}")}
		if _, err := c.Prepare(ctx, intent, nil, nil); !errors.Is(err, core.ErrConflict) {
			t.Fatalf("prepare resurrected %s: %v", terminal, err)
		}
		if terminal == "Resumed" {
			if err := c.Stopped(ctx, id, "node", "duplicate"); err != nil {
				t.Fatalf("late stopped event must not touch successor: %v", err)
			}
		}
	}
}

func TestNativeUnplacedResumeRecoveryPreservesHold(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	d, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	volume := core.Volume{Driver: "test", Handle: id, PVC: core.PVCRef{UID: id}}
	if err = c.Ledger.Register(ctx, volume); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Ledger.Pool.Exec(ctx, `INSERT INTO ssf_use_holds(volume_key,owner_uid,readonly,reason) VALUES($1,$2,false,'paused')`, volume.Key(), d.OwnerUID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = c.Ledger.Pool.Exec(ctx, `DELETE FROM ssf_use_holds WHERE volume_key=$1`, volume.Key()) })
	q := NativeIntent{RequestID: id, Tenant: "test", Body: []byte("{}"), BodyDigest: core.Hash("{}"), ResumeOf: "paused-parent", OwnerUID: d.OwnerUID}
	raw, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Ledger.Pool.Exec(ctx, `UPDATE agentenv_native_runs SET state='Intent',intent=$2,digest=$3,updated=now()-interval '1 hour' WHERE id=$1`, id, raw, core.Digest(q)); err != nil {
		t.Fatal(err)
	}
	if err = c.recoverNativeIntent(ctx, q, d.OwnerUID, d.RunUID, time.Minute); err != nil {
		t.Fatal(err)
	}
	var state string
	var holds int
	if err = c.Ledger.Pool.QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err = c.Ledger.Pool.QueryRow(ctx, `SELECT count(*) FROM ssf_use_holds WHERE owner_uid=$1`, d.OwnerUID).Scan(&holds); err != nil {
		t.Fatal(err)
	}
	if state != "Unstarted" || holds != 1 {
		t.Fatalf("resume compensation lost paused ownership: state=%s holds=%d", state, holds)
	}
}

func TestNativeUnpublishedCompensationNeverTreatsDispatchAsUnsent(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	d, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"Ready", "Dispatched", "Cancelling", "Pausing"} {
		if _, err = c.Ledger.Pool.Exec(ctx, `UPDATE agentenv_native_runs SET state=$2 WHERE id=$1`, id, state); err != nil {
			t.Fatal(err)
		}
		if err = c.abortNativeUnpublished(ctx, id); !errors.Is(err, core.ErrConflict) {
			t.Fatalf("compensated possibly dispatched %s: %v", state, err)
		}
	}
	if _, err = c.Ledger.RunEpoch(ctx, d.OwnerUID, d.RunUID); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Ledger.Pool.Exec(ctx, `UPDATE agentenv_native_runs SET state='Unpublished' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err = c.abortNativeUnpublished(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err = c.abortNativeUnpublished(ctx, id); err != nil {
		t.Fatal(err)
	}
	var state string
	if err = c.Ledger.Pool.QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "Revoked" {
		t.Fatal(state)
	}
	if _, err = c.Ledger.RunEpoch(ctx, d.OwnerUID, d.RunUID); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("unpublished run resurrected: %v", err)
	}
}
