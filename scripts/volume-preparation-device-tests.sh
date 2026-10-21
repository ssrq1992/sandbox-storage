#!/usr/bin/env bash
# Disposable loop-device tests; run explicitly on a dedicated Linux test host.
set -euo pipefail
test "$(uname -s)" = Linux
test "$(id -u)" = 0
for tool in go losetup blkid mkfs.ext4 e2fsck debugfs; do
  command -v "$tool" >/dev/null
done
cd "$(dirname "$0")/.."
mkdir -p validation/volume-preparation
SSF_PREPARATION_DEVICE_TEST=1 go test -count=1 -timeout=5m -json \
  ./adapter/agentenvadapter -run '^TestLinuxPreparation' \
  | tee validation/volume-preparation/linux-device-tests.jsonl
