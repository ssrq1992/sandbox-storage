#!/usr/bin/env bash
set -euo pipefail
context="kind-${SSF_TEST_CLUSTER:-ssf-test}"
base=https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.0
for name in volumesnapshotclasses volumesnapshotcontents volumesnapshots; do
  kubectl --context "$context" apply -f "$base/client/config/crd/snapshot.storage.k8s.io_${name}.yaml"
done
kubectl --context "$context" apply -f "$base/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml"
kubectl --context "$context" apply -f "$base/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml"
kubectl --context "$context" -n kube-system set image deployment/snapshot-controller snapshot-controller=registry.k8s.io/sig-storage/snapshot-controller:v8.2.0
for mode in file block; do
  patch=$(python3 - "$mode" <<'PY'
import json,sys
print(json.dumps({"spec":{"template":{"spec":{"containers":[{"name":"snapshotter","image":"registry.k8s.io/sig-storage/csi-snapshotter:v8.2.0","args":["--csi-address=/csi/csi.sock","--leader-election"],"volumeMounts":[{"name":"socket","mountPath":"/csi"}]}]}}}}))
PY
)
  kubectl --context "$context" -n ssf-fixture patch daemonset "ssf-$mode" --type=strategic -p "$patch"
  cat <<YAML | kubectl --context "$context" apply -f -
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: ssf-$mode
  annotations:
    snapshot.storage.kubernetes.io/is-default-class: "true"
driver: test.$mode.ssf.dev
deletionPolicy: Retain
YAML
done
