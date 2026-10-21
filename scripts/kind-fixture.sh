#!/usr/bin/env bash
set -euo pipefail
command -v kind >/dev/null
command -v kubectl >/dev/null
cluster=${SSF_TEST_CLUSTER:-ssf-test}
kind create cluster --name "$cluster" --image kindest/node:v1.35.0 --config testing/kind.yaml
docker build -f testing/Dockerfile -t ssf-test:0.1.0 .
kind load docker-image ssf-test:0.1.0 --name "$cluster"
python3 scripts/render_fixture.py | kubectl --context "kind-$cluster" apply -f -
kubectl --context "kind-$cluster" -n ssf-fixture rollout status daemonset/ssf-file --timeout=180s
kubectl --context "kind-$cluster" -n ssf-fixture rollout status daemonset/ssf-block --timeout=180s
printf 'Disposable cluster ready: %s. Run cluster tests with SSF_TEST_KUBECONFIG; install Guard separately before production-like protection tests.\n' "$cluster"
