#!/usr/bin/env python3
"""Collect read-only native/SSF Kubernetes evidence; never collect Secret objects."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--context', required=True)
parser.add_argument('--namespace', default='agentenv-system')
parser.add_argument('--output', required=True)
args = parser.parse_args()
out = Path(args.output).resolve()
out.mkdir(mode=0o700, parents=True, exist_ok=False)
os.umask(0o077)
report = {'time': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'context': args.context,
          'namespace': args.namespace, 'scope': 'read-only-cluster-evidence', 'checks': [], 'e2e_passed': False}

def collect(name, arguments):
    command = ['kubectl', '--context', args.context, '--request-timeout=20s', *arguments]
    try:
        result = subprocess.run(command, capture_output=True, timeout=30)
        (out / (name + '.log')).write_bytes(result.stdout)
        if result.stderr:
            (out / (name + '.stderr')).write_bytes(result.stderr)
        report['checks'].append({'name': name, 'status': 'collected' if result.returncode == 0 else 'failed', 'exit_code': result.returncode})
        return result.stdout if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired) as error:
        report['checks'].append({'name': name, 'status': 'failed', 'reason': str(error)})
        return None

collect('version', ['version', '-o', 'json'])
for resource in ('deployments', 'daemonsets', 'services', 'endpointslices', 'events', 'persistentvolumeclaims', 'volumesnapshots'):
    collect(resource, ['get', resource, '-n', args.namespace, '-o', 'json'])
for resource in ('nodes', 'csinodes', 'csidrivers', 'storageclasses', 'persistentvolumes', 'volumeattachments', 'volumesnapshotcontents', 'volumesnapshotclasses'):
    collect(resource, ['get', resource, '-o', 'json'])
pods = collect('native-pods', ['get', 'pods', '-n', args.namespace, '-l',
                              'app.kubernetes.io/name in (agentenv-node,agentenv-scheduler,agentenv-gateway,ssf-coordinator)', '-o', 'json'])
if pods:
    for pod in json.loads(pods).get('items', []):
        name = pod['metadata']['name']
        if not re.fullmatch(r'[a-z0-9][a-z0-9.-]*', name):
            raise ValueError('invalid Pod name in API response')
        for container in pod['spec'].get('containers', []) + pod['spec'].get('initContainers', []):
            container_name = container['name']
            if container_name not in {'agentenv', 'gateway', 'scheduler', 'agentenv-storage', 'ssf-node', 'coordinator',
                                      'overlaybd-daemon', 'overlaybd-native', 'overlaybd-worker',
                                      'overlaybd-csi', 'overlaybd-registrar'}:
                continue
            collect(name + '-' + container_name, ['logs', '-n', args.namespace, name, '-c', container_name, '--timestamps', '--tail=5000'])
controller_pods = collect('overlaybd-controller-pods', ['get', 'pods', '-n', args.namespace,
                           '-l', 'app=ssf-overlaybd-controller', '-o', 'json'])
if controller_pods:
    for pod in json.loads(controller_pods).get('items', []):
        name = pod['metadata']['name']
        for container in pod['spec'].get('containers', []):
            collect(name + '-' + container['name'], ['logs', '-n', args.namespace, name,
                    '-c', container['name'], '--timestamps', '--tail=5000'])
(out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
print(out / 'report.json')
raise SystemExit(1 if any(check['status'] == 'failed' for check in report['checks']) else 0)
