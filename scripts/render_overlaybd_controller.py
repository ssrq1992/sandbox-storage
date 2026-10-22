#!/usr/bin/env python3
"""Render the OverlayBD controller only; never contact or mutate a cluster.

Use the same worker inventory as render_agentenv_native.py. Database, TLS
secrets, Guard and snapshot CRDs are separately installed dependencies.
"""
import argparse
import json
import re
from pathlib import Path


def render(namespace, image, repository, endpoint, nodes, offline_expansion=False):
    if not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", namespace) or len(namespace) > 63:
        raise ValueError("invalid namespace")
    if not repository or not endpoint or not image:
        raise ValueError("image, repository and authenticated worker endpoint required")
    if "://" in endpoint or not re.fullmatch(r"[A-Za-z0-9.-]+:[0-9]+", endpoint):
        raise ValueError("worker endpoint must be certificate DNS name:port")
    name = "ssf-overlaybd-controller"
    labels = {"app": name}
    metadata = {"name": name, "namespace": namespace}
    cluster_name = f"{namespace}-{name}"
    config = {
        "role": "controller", "socket": "/run/csi/private/csi.sock",
        "databaseURLFile": "/etc/ssf/database-url", "certificate": "/etc/ssf-tls/tls.crt",
        "key": "/etc/ssf-tls/tls.key", "ca": "/etc/ssf-tls/ca.crt",
        "repositories": [{"id": repository, "endpoint": endpoint, "nodes": nodes}],
        "captureCoordinator": {"endpoint": f"https://ssf-coordinator.{namespace}.svc:9446",
            "cert": "/etc/ssf-tls/tls.crt", "key": "/etc/ssf-tls/tls.key", "ca": "/etc/ssf-tls/ca.crt",
            "peer": "spiffe://ssf/agentenv/coordinator"},
    }
    socket = {"name": "socket", "mountPath": "/run/csi"}
    config['offlineExpansion'] = offline_expansion
    containers = [{
        "name": "driver", "image": image, "args": ["-config", "/etc/overlaybd/controller.json"],
        "readinessProbe": {"exec": {"command": ["/usr/local/bin/overlaybd-csi", "-config", "/etc/overlaybd/controller.json", "-probe"]}, "periodSeconds": 10, "timeoutSeconds": 6},
        "securityContext": {"runAsUser": 0, "allowPrivilegeEscalation": False,
                            "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
        "volumeMounts": [socket, {"name": "config", "mountPath": "/etc/overlaybd", "readOnly": True},
                         {"name": "database", "mountPath": "/etc/ssf", "readOnly": True},
                         {"name": "tls", "mountPath": "/etc/ssf-tls", "readOnly": True}],
    }]
    # Keep the repository's pinned sidecar baseline. Runtime compatibility still
    # needs Linux/Kubernetes verification; rendering is not that verification.
    for sidecar, version in (("provisioner", "v5.2.0"), ("snapshotter", "v8.2.0")):
        containers.append({
            "name": sidecar, "image": f"registry.k8s.io/sig-storage/csi-{sidecar}:{version}",
            "args": ["--csi-address=/run/csi/private/csi.sock", "--leader-election",
                     f"--leader-election-namespace={namespace}", "--extra-create-metadata"],
            "securityContext": {"runAsUser": 0, "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
            "volumeMounts": [socket],
        })
    if offline_expansion:
        containers.append({"name":"resizer", "image":"registry.k8s.io/sig-storage/csi-resizer:v1.13.2",
                           "args":["--csi-address=/run/csi/private/csi.sock","--leader-election",
                                   f"--leader-election-namespace={namespace}","--handle-volume-inuse-error=false"],
                           "securityContext":{"runAsUser":0,"allowPrivilegeEscalation":False,"readOnlyRootFilesystem":True,"capabilities":{"drop":["ALL"]}},
                           "volumeMounts":[socket]})
    # Attach is driven by the SSF gateway and shared ledger. Do not add a second
    # external-attacher owner. Expansion remains an explicit deployment capability.
    rules = [
        {"apiGroups": ["admissionregistration.k8s.io"], "resources": ["validatingwebhookconfigurations"], "resourceNames": ["ssf-guard"], "verbs": ["get"]},
        {"apiGroups": [""], "resources": ["persistentvolumes"], "verbs": ["get", "list", "watch", "create", "delete", "update", "patch"]},
        {"apiGroups": [""], "resources": ["persistentvolumeclaims", "persistentvolumeclaims/status"], "verbs": ["get", "list", "watch", "update", "patch"]},
        {"apiGroups": [""], "resources": ["nodes"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["events"], "verbs": ["get", "list", "watch", "create", "update", "patch"]},
        {"apiGroups": ["storage.k8s.io"], "resources": ["storageclasses", "csinodes", "volumeattachments"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["snapshot.storage.k8s.io"], "resources": ["volumesnapshots", "volumesnapshotclasses"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["snapshot.storage.k8s.io"], "resources": ["volumesnapshotcontents", "volumesnapshotcontents/status"], "verbs": ["get", "list", "watch", "update", "patch"]},
    ]
    items = [
        {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata},
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata, "data": {"controller.json": json.dumps(config, indent=2)}},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": {"name": cluster_name}, "rules": rules},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": {"name": cluster_name},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": cluster_name},
         "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": namespace}]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": metadata,
         "rules": [{"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]}]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": metadata,
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
         "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": namespace}]},
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": metadata,
         "spec": {"replicas": 1, "selector": {"matchLabels": labels},
                  "template": {"metadata": {"labels": labels}, "spec": {
                      "serviceAccountName": name, "terminationGracePeriodSeconds": 300,
                      "containers": containers,
                      "volumes": [{"name": "socket", "emptyDir": {}},
                                  {"name": "config", "configMap": {"name": name}},
                                  {"name": "database", "secret": {"secretName": "ssf-overlaybd-database", "defaultMode": 256}},
                                  {"name": "tls", "secret": {"secretName": "ssf-overlaybd-controller-tls", "defaultMode": 256}}],
                  }}}},
    ]
    return {"apiVersion": "v1", "kind": "List", "items": items}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--repository")
    parser.add_argument("--offline-expansion", action="store_true")
    parser.add_argument("--inventory", help="same explicit per-node inventory used by the native overlay")
    parser.add_argument("--worker-endpoint")
    placement = parser.add_mutually_exclusive_group()
    placement.add_argument("--local-node", help="only node with this local repository")
    placement.add_argument("--shared-repository", action="store_true", help="verified shared POSIX/OSS repository")
    args = parser.parse_args()
    try:
        if args.inventory:
            if args.repository or args.worker_endpoint or args.local_node or args.shared_repository or args.offline_expansion:
                parser.error('inventory cannot be combined with single-worker flags; set offline_expansion in the inventory')
            from overlaybd_node_overlay import build_inventory
            source = json.loads(Path(args.inventory).read_text())
            _, repositories = build_inventory(source)
            first = repositories[0]
            result = render(args.namespace, args.image, first['id'], first['endpoint'], first['nodes'], source.get('offline_expansion', False))
            config_map = next(item for item in result['items'] if item['kind']=='ConfigMap')
            config = json.loads(config_map['data']['controller.json'])
            config['repositories'] = repositories
            config_map['data']['controller.json'] = json.dumps(config, indent=2)
        else:
            if not args.repository or not args.worker_endpoint or not (args.local_node or args.shared_repository):
                parser.error('repository, worker endpoint and explicit local/shared placement required')
            result = render(args.namespace, args.image, args.repository, args.worker_endpoint,
                            [args.local_node] if args.local_node else [], args.offline_expansion)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
