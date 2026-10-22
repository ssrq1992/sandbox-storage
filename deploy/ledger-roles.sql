-- Apply as database owner after ssfctl migrate. These are NOLOGIN group roles;
-- grant them to separately provisioned authenticated service login roles.
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='ssf_guard_reader') THEN CREATE ROLE ssf_guard_reader NOLOGIN; END IF; END $$;
GRANT USAGE ON SCHEMA public TO ssf_guard_reader;
GRANT SELECT ON ssf_permits, ssf_selection_permits TO ssf_guard_reader;
GRANT SELECT ON ssf_volumes, ssf_volume_maintenance TO ssf_guard_reader;
GRANT SELECT ON ssf_checkpoint_resources, ssf_checkpoint_pins, ssf_direct_snapshots TO ssf_guard_reader;

DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='ssf_gateway_reader') THEN CREATE ROLE ssf_gateway_reader NOLOGIN; END IF; END $$;
GRANT USAGE ON SCHEMA public TO ssf_gateway_reader;
GRANT SELECT ON ssf_bindings, ssf_attachments TO ssf_gateway_reader;

DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='ssf_consumer') THEN CREATE ROLE ssf_consumer NOLOGIN; END IF; END $$;
GRANT USAGE ON SCHEMA public TO ssf_consumer;
GRANT SELECT, INSERT, UPDATE ON ssf_volumes, ssf_operations, ssf_bindings,
    ssf_steps, ssf_attachments, ssf_permits, ssf_selection_permits TO ssf_consumer;
GRANT SELECT ON ssf_meta TO ssf_consumer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ssf_volume_preparations,
    ssf_use_holds, ssf_runs, ssf_owner_tombstones, ssf_volume_maintenance,
    ssf_checkpoint_sets, ssf_checkpoint_resources, ssf_checkpoint_pins,
    ssf_checkpoint_gc, ssf_snapshot_captures, ssf_idle_captures, ssf_direct_snapshots,
    ssf_backend_tombstones TO ssf_consumer;
GRANT USAGE, SELECT ON SEQUENCE ssf_run_epoch TO ssf_consumer;
