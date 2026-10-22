#!/usr/bin/env bash
# Native type/build verification only; this does not run KVM or CSI tests.
set -euo pipefail
if [[ "$(uname -s)" != Linux ]]; then
  echo 'NOT_EXECUTED: AgentENV requires Linux io_uring, ublk and native build dependencies.' >&2
  exit 2
fi
SSF_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AGENTENV_ROOT="${AGENTENV_ROOT:-${SSF_ROOT}/../AgentENV}"
for tool in cargo rustc protoc cmake pkg-config clang go; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "NOT_EXECUTED: missing required build tool: ${tool}" >&2
    exit 2
  fi
done
cd "$AGENTENV_ROOT"
cargo check --locked -p agentenv --all-targets
cargo clippy --locked -p aenv-volume-core --features content --all-targets -- -D warnings
cargo clippy --locked --manifest-path "$SSF_ROOT/drivers/overlaybd/native/Cargo.toml" --features device-host --all-targets -- -D warnings
cargo test --locked -p aenv-volume-core --features content
cargo test --locked -p overlaybd --lib checkpointed_restack
cargo test --locked -p overlaybd --lib test_create_snapshot_and_restack
cargo test --locked -p overlaybd --lib ext4_stat::tests
cargo test --locked -p uvm-ublk-daemon --lib
cargo test --locked --manifest-path "$SSF_ROOT/drivers/overlaybd/native/Cargo.toml" --features device-host
cargo build --locked --manifest-path "$SSF_ROOT/drivers/overlaybd/native/Cargo.toml" --features device-host --bin ssf-overlaybd-native
cargo test --locked -p agentenv --lib orchestrator::service::tests::fork_terminal_capture_keeps_ownership_when_stop_is_unknown
cargo test --locked -p agentenv --lib orchestrator::service::tests::provider_failed_launch_keeps_native_handle_until_confirmed_stop
cargo test --locked -p agentenv --lib orchestrator::service::tests::storage_capacity_ids_match_allocations_and_exclude_dormant_builds
cargo test --locked -p agentenv --lib orchestrator::service::tests::prepared_fork_keeps_independent_identity_and_partial_success
cargo test --locked -p agentenv --lib orchestrator::service::tests::provider_context_preserves_native_pause_and_requires_child_admission
cargo test --locked -p agentenv --lib provider_template_requires_matching_admitted_checkpoint
cargo test --locked -p agentenv --lib snapshot::repository::backends::posixfs::catalog::tests::repository_identity_is_durable_and_catalog_scoped
cargo test --locked -p agentenv --lib volume::tests::captured_backing_does_not_follow_or_rewind_the_live_volume_head
cargo test --locked -p agentenv --lib orchestrator::persistence::file_backed::tests
cargo test --locked -p agentenv --lib sandbox::extra_drive::tests
cargo test --locked -p agentenv --lib api::impls::storage::tests
cargo test --locked -p agentenv --lib sandbox::firecracker::startup_pack::tests
cargo test --locked --manifest-path "$SSF_ROOT/adapter/agentenvadapter/runtime/Cargo.toml"
cd services
go test ./...

cd "$SSF_ROOT"
go build ./cmd/overlaybd-csi
go build ./cmd/overlaybd-evidence
go test -race ./node ./auth ./drivers/overlaybd
go vet ./node ./auth ./drivers/overlaybd ./drivers/csi ./cmd/overlaybd-csi
python3 scripts/check_generated.py
