#!/usr/bin/env bash
set -euo pipefail
test "$(uname -s)" = Linux
test "$(id -u)" = 0
command -v losetup >/dev/null
command -v mount >/dev/null
mkdir -p test-results
go build -o test-results/ssf-test-driver ./cmd/ssf-test-driver
work=$(mktemp -d)
driver_pid=''
trap 'if test -n "$driver_pid"; then kill "$driver_pid" 2>/dev/null || true; wait "$driver_pid" 2>/dev/null || true; fi' EXIT
for mode in file block; do
  attach=false
  if test "$mode" = block; then attach=true; fi
  test-results/ssf-test-driver -socket "$work/$mode.sock" -root "$work/data-$mode" -mode "$mode" -node test-node -attach="$attach" >"test-results/driver-$mode.log" 2>&1 &
  driver_pid=$!
  for attempt in $(seq 1 50); do test -S "$work/$mode.sock" && break; sleep 0.1; done
  SSF_TEST_CSI_ENDPOINT="unix://$work/$mode.sock" SSF_TEST_CSI_MODE="$mode" go test -count=1 -v ./node -run TestLinuxRealPublish | tee "test-results/linux-$mode.txt"
  kill "$driver_pid"
  wait "$driver_pid" || true
  driver_pid=''
done
# Retain fixture paths on failure so unknown mounts are inspected, never rm -rf a live mount.
printf 'Fixture directory: %s\n' "$work"
