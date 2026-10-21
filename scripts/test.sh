#!/usr/bin/env bash
set -euo pipefail
mkdir -p test-results
go test -json ./... >test-results/unit.json
go test -race -json ./... >test-results/race.json
go vet ./...
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/...
python3 scripts/check_generated.py
go test ./node -run '^$' -bench BenchmarkPreparedReplay -benchmem >test-results/benchmark.txt
