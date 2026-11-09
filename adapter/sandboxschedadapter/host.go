package sandboxschedadapter

import (
	"context"

	"k8s.io/client-go/kubernetes"

	"ssf.dev/ssf/config"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

// createRunsTable creates the plugin's own ledger table.
func createRunsTable(ctx context.Context, db *ledger.Store) error {
	_, err := db.Session(ctx).Exec(ctx, `
CREATE TABLE IF NOT EXISTS sandboxsched_runs (
  id text PRIMARY KEY,
  digest text NOT NULL,
  tenant text NOT NULL,
  owner_uid text NOT NULL,
  run_uid text NOT NULL,
  state text NOT NULL,
  dispatch jsonb NOT NULL,
  created timestamptz NOT NULL DEFAULT now(),
  updated timestamptz NOT NULL DEFAULT now()
)`)
	if err != nil {
		return err
	}
	_, err = db.Session(ctx).Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_sandboxsched_runs_state ON sandboxsched_runs(state)`)
	return err
}

// OpenSandboxSchedControl opens the coordinator-hosted control plugin state.
func OpenSandboxSchedControl(ctx context.Context, cfg ControlConfig) (*SandboxControl, func(), error) {
	db, err := ledger.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, err
	}
	fail := func(e error) (*SandboxControl, func(), error) { db.Close(); return nil, nil, e }
	if err = db.Migrate(ctx); err != nil {
		return fail(err)
	}
	if err = createRunsTable(ctx, db); err != nil {
		return fail(err)
	}
	rest, err := config.Kube(cfg.Kubeconfig)
	if err != nil {
		return fail(err)
	}
	api, err := kubernetes.NewForConfig(rest)
	if err != nil {
		return fail(err)
	}
	k := &kube.Client{API: api, Selection: db}
	control := &SandboxControl{Ledger: db, Kube: k, Principal: cfg.Principal}
	return control, func() { db.Close() }, nil
}
