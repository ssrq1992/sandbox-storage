"""Render disposable CSI fixtures. Cluster-admin belongs only to test namespace."""
import json
import argparse
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--fault-file", help="enable disposable-fixture faults at an absolute /data/ path")
args = parser.parse_args()
if args.fault_file and not args.fault_file.startswith("/data/"): parser.error("fault file must be under /data/")

objects = [
    {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "ssf-fixture"}},
    {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "fixture", "namespace": "ssf-fixture"}},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": {"name": "ssf-fixture"}, "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"}, "subjects": [{"kind": "ServiceAccount", "name": "fixture", "namespace": "ssf-fixture"}]},
]
for mode in ("file", "block"):
    name = f"test.{mode}.ssf.dev"
    plugin = f"/var/lib/kubelet/plugins/{name}"
    volumes = [
        {"name": "socket", "hostPath": {"path": plugin, "type": "DirectoryOrCreate"}},
        {"name": "registration", "hostPath": {"path": "/var/lib/kubelet/plugins_registry", "type": "Directory"}},
        {"name": "state", "hostPath": {"path": "/var/lib/ssf", "type": "DirectoryOrCreate"}},
        {"name": "data", "hostPath": {"path": f"/var/lib/ssf-fixture/{mode}", "type": "DirectoryOrCreate"}},
        {"name": "dev", "hostPath": {"path": "/dev", "type": "Directory"}},
    ]
    containers = [
        {"name": "driver", "image": "ssf-test:0.1.0", "imagePullPolicy": "IfNotPresent", "args": ["-mode", mode, "-root", "/data", "-node", "$(NODE_NAME)", f"-attach={'true' if mode == 'block' else 'false'}"], "env": [{"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}], "securityContext": {"privileged": True}, "volumeMounts": [{"name": "socket", "mountPath": "/csi"}, {"name": "state", "mountPath": "/var/lib/ssf", "mountPropagation": "Bidirectional"}, {"name": "data", "mountPath": "/data"}, {"name": "dev", "mountPath": "/dev"}]},
        {"name": "registrar", "image": "registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.13.0", "args": ["--csi-address=/csi/csi.sock", f"--kubelet-registration-path={plugin}/csi.sock"], "volumeMounts": [{"name": "socket", "mountPath": "/csi"}, {"name": "registration", "mountPath": "/registration"}]},
        {"name": "provisioner", "image": "registry.k8s.io/sig-storage/csi-provisioner:v5.2.0", "args": ["--csi-address=/csi/csi.sock", "--leader-election", "--extra-create-metadata"], "volumeMounts": [{"name": "socket", "mountPath": "/csi"}]},
        {"name": "resizer", "image": "registry.k8s.io/sig-storage/csi-resizer:v1.13.2", "args": ["--csi-address=/csi/csi.sock", "--leader-election"], "volumeMounts": [{"name": "socket", "mountPath": "/csi"}]},
    ]
    if args.fault_file:
        containers[0]["command"] = ["/usr/local/bin/ssf-test-driver"]
        containers[0]["args"] += ["--fault-file", args.fault_file]
    objects.append({"apiVersion": "apps/v1", "kind": "DaemonSet", "metadata": {"name": f"ssf-{mode}", "namespace": "ssf-fixture"}, "spec": {"selector": {"matchLabels": {"app": f"ssf-{mode}"}}, "template": {"metadata": {"labels": {"app": f"ssf-{mode}"}}, "spec": {"serviceAccountName": "fixture", "tolerations": [{"operator": "Exists"}], "containers": containers, "volumes": volumes}}}})
    objects.append({"apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver", "metadata": {"name": name}, "spec": {"attachRequired": mode == "block", "podInfoOnMount": False, "volumeLifecycleModes": ["Persistent"]}})
    for binding in ("Immediate", "WaitForFirstConsumer"):
        objects.append({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {"name": f"ssf-{mode}-{'immediate' if binding == 'Immediate' else 'wffc'}"}, "provisioner": name, "reclaimPolicy": "Retain", "volumeBindingMode": binding, "allowVolumeExpansion": True})
print(json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}, indent=2))
