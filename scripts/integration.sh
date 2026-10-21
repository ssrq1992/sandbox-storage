#!/usr/bin/env bash
set -euo pipefail
command -v docker >/dev/null
mkdir -p test-results
name="ssf-postgres-$$"
docker run -d --name "$name" -e POSTGRES_PASSWORD=ssf-test-only -e POSTGRES_DB=ssf -p 127.0.0.1::5432 postgres:17.4 >/dev/null
trap 'docker rm -f "$name" >/dev/null' EXIT
for attempt in $(seq 1 60); do if docker exec "$name" pg_isready -U postgres >/dev/null 2>&1; then break; fi; sleep 1; done
port=$(docker port "$name" 5432/tcp | awk -F: '{print $NF}')
SSF_TEST_DATABASE_URL="postgres://postgres:ssf-test-only@127.0.0.1:$port/ssf?sslmode=disable" go test -race -count=1 -v ./ledger ./consume | tee test-results/postgres.txt
docker build -f testing/Dockerfile -t ssf-test:0.1.0 .
docker run --rm --privileged --entrypoint bash -v "$PWD/test-results:/src/test-results" ssf-test:0.1.0 scripts/linux-mount-tests.sh
