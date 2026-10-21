#!/usr/bin/env python3
"""Read-only prerequisites for native AgentENV/SSF real-environment tests.

No installation, secret dumping, resource mutation, or deployment is performed.
A Kubernetes context must be explicitly supplied before cluster queries run.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--mode', choices=['cluster', 'standalone'], default='cluster')
parser.add_argument('--context', help='explicit kubectl context for read-only checks')
parser.add_argument('--namespace', default='agentenv-system')
parser.add_argument('--storage-class')
parser.add_argument('--snapshot-class')
parser.add_argument('--volume-preparation', action='store_true',
                    help='also require the direct-PVC profile and mutating admission')
parser.add_argument('--output', default='native-preflight.json')
args = parser.parse_args()
checks = []

def record(name, passed, detail):
    checks.append({'name': name, 'status': 'passed' if passed else 'failed', 'detail': detail})

def skipped(name, reason):
    checks.append({'name': name, 'status': 'not_executed', 'detail': reason})

def cluster_json(*arguments):
    command = ['kubectl', '--context', args.context, '--request-timeout=15s', *arguments, '-o', 'json']
    result = subprocess.run(command, capture_output=True, text=True, timeout=25)
    if result.returncode:
        raise RuntimeError(result.stderr.strip()[:1000])
    return json.loads(result.stdout)

record('linux-host', platform.system() == 'Linux', platform.system())
record('kvm-device', os.access('/dev/kvm', os.R_OK | os.W_OK), '/dev/kvm must be readable and writable by test runner')
for tool in ('cargo', 'rustc', 'go', 'protoc', 'cmake', 'clang', 'pkg-config', 'kubectl', 'openssl'):
    record('tool-' + tool, shutil.which(tool) is not None, shutil.which(tool) or 'missing')
for directory in ('/var/lib/ssf', '/var/lib/aenv'):
    record('state-' + directory, Path(directory).is_dir(), 'persistent node state directory required')
if args.context and shutil.which('kubectl'):
    queries = [('native-deployments', ('get', 'deployments', '-n', args.namespace)),
               ('native-daemonsets', ('get', 'daemonsets', '-n', args.namespace)),
               ('native-services', ('get', 'services', '-n', args.namespace)),
               ('csi-nodes', ('get', 'csinodes'))]
    if args.mode == 'standalone':
        queries = [('csi-nodes', ('get', 'csinodes'))]
    for name, arguments in queries:
        try:
            result = cluster_json(*arguments)
            items = result.get('items', [])
            names = [item['metadata']['name'] for item in items]
            if name == 'native-deployments':
                expected = {'agentenv-gateway', 'agentenv-scheduler'}
            elif name == 'native-daemonsets':
                expected = {'agentenv-node'}
            elif name == 'native-services':
                expected = {'agentenv-nodes', 'agentenv-scheduler'}
            else:
                expected = set()
            record(name, bool(items) and expected.issubset(names), {'observed': names, 'required': sorted(expected)})
        except (RuntimeError, subprocess.TimeoutExpired, ValueError) as error:
            record(name, False, str(error))
    for name, resource, selection in [('storage-class', 'storageclass', args.storage_class),
                                      ('snapshot-class', 'volumesnapshotclass', args.snapshot_class)]:
        if not selection:
            skipped(name, 'supply --' + name + ' to verify the chosen vendor class')
            continue
        try:
            obj = cluster_json('get', resource, selection)
            if resource == 'storageclass':
                detail = {field: obj.get(field) for field in ('provisioner', 'reclaimPolicy', 'volumeBindingMode', 'allowVolumeExpansion')}
                valid = obj.get('reclaimPolicy') == 'Retain' and obj.get('volumeBindingMode', 'Immediate') in ('Immediate', 'WaitForFirstConsumer')
                if args.volume_preparation:
                    profile = obj.get('metadata', {}).get('annotations', {}).get('ssf.dev/volume-profile')
                    detail['volumeProfile'] = profile
                    valid = valid and profile == 'agentenv-block-ext4-v1'
            else:
                detail = {field: obj.get(field) for field in ('driver', 'deletionPolicy')}
                valid = obj.get('deletionPolicy') == 'Retain'
            record(name, valid, detail)
        except (RuntimeError, subprocess.TimeoutExpired, ValueError) as error:
            record(name, False, str(error))
    if args.volume_preparation:
        try:
            obj = cluster_json('get', 'mutatingwebhookconfiguration', 'ssf-volume-profile')
            hooks = obj.get('webhooks', [])
            valid = any(h.get('failurePolicy') == 'Fail'
                        and h.get('clientConfig', {}).get('service', {}).get('path') == '/mutate'
                        and h.get('clientConfig', {}).get('caBundle')
                        for h in hooks)
            record('preparation-admission', bool(valid), 'fail-closed profile mutation with CA required')
        except (RuntimeError, subprocess.TimeoutExpired, ValueError) as error:
            record('preparation-admission', False, str(error))
else:
    skipped('kubernetes', 'explicit --context and kubectl required; no cluster queried')
skipped('runtime-data-semantics', 'preflight does not prove guest I/O, lifecycle, fencing, snapshot consistency or CSI behavior')
report = {'time': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'scope': 'prerequisites-only',
          'context': args.context, 'checks': checks, 'e2e_passed': False}
Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
print(args.output)
sys.exit(1 if any(check['status'] == 'failed' for check in checks) else 0)
