#!/usr/bin/env python3
"""Kubernetes PVC -> original AgentENV API -> guest data persistence.

No initializer Pod, formatting command or /volumes import is used. Execution
requires --execute. Failed/unknown sandbox creates are not automatically retried.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

from native_lifecycle_e2e import Suite, require


class PVCSuite(Suite):
    def kube(self, *arguments, body=None):
        command = [self.args.kubectl]
        if self.args.context:
            command += ['--context', self.args.context]
        command += list(arguments)
        result = subprocess.run(command, input=None if body is None else json.dumps(body),
                                capture_output=True, text=True, timeout=self.args.timeout)
        require(result.returncode == 0, 'kubectl operation failed: ' + result.stderr[:2000])
        return json.loads(result.stdout) if result.stdout.strip() else None

    def claim(self, name):
        return self.kube('get', 'pvc', name, '-n', self.args.namespace, '-o', 'json')

    def provision(self, name):
        claim = self.kube('create', '-f', '-', '-o', 'json', body={
            'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim',
            'metadata': {'name': name, 'namespace': self.args.namespace},
            'spec': {'storageClassName': self.args.storage_class, 'volumeMode': 'Block',
                     'accessModes': ['ReadWriteOnce'],
                     'resources': {'requests': {'storage': str(self.args.size_mb) + 'Mi'}}}})
        self.report.setdefault('pvcs', []).append({
            'namespace': self.args.namespace, 'name': name, 'uid': claim['metadata']['uid'],
            'created_by_test': True})
        self.save()
        return claim

    def mounts(self, name):
        return [{'pvc': {'namespace': self.args.namespace, 'name': name}, 'path': '/data'}]

    def check(self, sandbox, value):
        require(self.guest(sandbox, 'cat /data/ssf-pvc-marker') == value,
                'PVC data changed or /data was not mounted')

    def run(self):
        self.report.update(scope='kubernetes-pvc-native-api', volume_source='direct-pvc',
                           real_device_formatting_executed_by_test=False)
        state = {}

        def create_pvc():
            name = self.args.pvc or self.id + '-data'
            claim = self.claim(name) if self.args.pvc else self.provision(name)
            state.update(name=name, uid=claim['metadata']['uid'])
            if self.args.pvc:
                self.report.setdefault('pvcs', []).append({
                    'namespace': self.args.namespace, 'name': name,
                    'uid': state['uid'], 'created_by_test': False})
            sc = self.kube('get', 'storageclass', self.args.storage_class, '-o', 'json')
            require(sc['metadata'].get('annotations', {}).get('ssf.dev/volume-profile')
                    == 'agentenv-block-ext4-v1', 'StorageClass has no AgentENV preparation profile')
            mode = sc.get('volumeBindingMode', 'Immediate')
            self.report['binding_mode'] = mode
            if mode == 'Immediate':
                deadline = time.monotonic() + self.args.timeout
                while True:
                    observed = self.claim(name)
                    require(observed['metadata']['uid'] == state['uid'], 'PVC was recreated')
                    status = observed['metadata'].get('annotations', {}).get('ssf.dev/preparation-state')
                    require(status != 'Failed', 'automatic preparation failed')
                    if status == 'VolumeReady':
                        break
                    require(time.monotonic() < deadline, 'Immediate PVC did not prepare without a sandbox')
                    time.sleep(1)
            else:
                require(mode == 'WaitForFirstConsumer', 'unknown binding mode')
            pods = self.kube('get', 'pods', '-n', self.args.namespace, '-o', 'json')
            require(not any(volume.get('persistentVolumeClaim', {}).get('claimName') == name
                            for pod in pods['items'] for volume in pod['spec'].get('volumes', [])),
                    'test PVC has a native Pod consumer')

        def first_sandbox():
            state['sandbox'] = self.create('/sandboxes-cold', {
                'image': self.args.image, 'timeout': 3600,
                'volumeMounts': self.mounts(state['name'])})
            self.guest(state['sandbox'],
                       'printf %s ' + shlex.quote(self.id) + ' > /data/ssf-pvc-marker; sync')
            self.check(state['sandbox'], self.id)
            claim = self.claim(state['name'])
            require(claim['metadata']['uid'] == state['uid'], 'launch changed PVC identity')
            require(claim['status']['phase'] == 'Bound', 'sandbox succeeded without Bound PVC')
            pv = self.kube('get', 'pv', claim['spec']['volumeName'], '-o', 'json')
            self.report['pv'] = {'name': pv['metadata']['name'], 'uid': pv['metadata']['uid']}

        def recreate():
            self.api('DELETE', '/sandboxes/' + state['sandbox']['sandboxID'], expected=204)
            require(self.claim(state['name'])['metadata']['uid'] == state['uid'],
                    'sandbox deletion removed the user PVC')
            state['sandbox'] = self.create('/sandboxes-cold', {
                'image': self.args.image, 'timeout': 3600,
                'volumeMounts': self.mounts(state['name'])})
            self.check(state['sandbox'], self.id)

        def resume():
            original = state['sandbox']['sandboxID']
            for endpoint in ['/sandboxes/' + original + '/resume',
                             '/v2/sandboxes/' + original + '/connect']:
                self.api('POST', '/sandboxes/' + original + '/pause', expected=204)
                self.api('POST', '/sandboxes-cold', {'image': self.args.image,
                         'volumeMounts': self.mounts(state['name'])}, 409)
                state['sandbox'] = self.api('POST', endpoint, {'timeout': 3600}, 201)
                require(state['sandbox']['sandboxID'] == original, 'resume changed native ID')
                self.check(state['sandbox'], self.id)

        def capture_and_fork():
            snapshot = self.api('POST', '/sandboxes/' + state['sandbox']['sandboxID'] +
                                '/snapshots', {'name': self.id + '-snapshot'}, 201)
            state['snapshot'] = snapshot['snapshotID']
            self.report['snapshots'].append(state['snapshot'])
            children = self.api('POST', '/sandboxes/' + state['sandbox']['sandboxID'] +
                                '/fork', {'count': 2, 'timeout': 3600}, 201)
            for child in children:
                if (child.get('sandbox') or {}).get('sandboxID'):
                    self.report['sandboxes'].append(child['sandbox']['sandboxID'])
            self.save()
            require(len(children) == 2 and all(child.get('sandbox') and not child.get('error')
                                               for child in children), 'Fork returned partial failures')
            for child in children:
                self.check(child['sandbox'], self.id)
            self.guest(children[0]['sandbox'], 'printf child > /data/ssf-pvc-marker; sync')
            self.check(children[0]['sandbox'], 'child')
            self.check(children[1]['sandbox'], self.id)
            self.check(state['sandbox'], self.id)

        def template():
            restored = self.create('/v2/sandboxes', {'templateID': state['snapshot'], 'timeout': 3600})
            self.check(restored, self.id)
            empty = self.create('/v2/sandboxes', {'templateID': state['snapshot'],
                                                'timeout': 3600, 'volumeMounts': []})
            self.guest(empty, '! grep -q " /data " /proc/mounts; ! test -f /data/ssf-pvc-marker')
            replacement = self.id + '-replacement'
            self.provision(replacement)
            fresh = self.create('/v2/sandboxes', {'templateID': state['snapshot'], 'timeout': 3600,
                                                 'volumeMounts': self.mounts(replacement)})
            self.guest(fresh, '! test -f /data/ssf-pvc-marker; printf replacement > /data/ssf-pvc-marker; sync')
            self.check(fresh, 'replacement')
            self.check(state['sandbox'], self.id)

        cases = [('P01-or-P02-kubernetes-pvc', create_pvc),
                 ('native-create-and-write', first_sandbox),
                 ('delete-recreate-preserves-data', recreate)]
        if self.args.lifecycle:
            cases += [('pause-hold-and-resume', resume),
                      ('snapshot-and-independent-fork', capture_and_fork),
                      ('template-inherit-empty-replace', template)]
        failed = False
        for name, action in cases:
            row = {'name': name, 'status': 'not_executed'}
            self.report['cases'].append(row)
            if not failed:
                try:
                    action()
                    row['status'] = 'passed'
                except Exception as error:
                    row.update(status='failed', error=str(error))
                    failed = True
            else:
                row['reason'] = 'prerequisite failed'
            self.save()
        self.report['status'] = 'failed' if failed else 'passed'
        self.report['limitations'] += [
            'Remaining P01-P12 failure/driver combinations require separate runs',
            'Resources are retained for inspection; cleanup must use recorded identities']
        self.save()
        return int(failed)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--url', required=True)
    p.add_argument('--image', required=True)
    p.add_argument('--namespace', required=True)
    p.add_argument('--storage-class', required=True)
    p.add_argument('--pvc', help='use an existing dedicated test PVC instead of creating one')
    p.add_argument('--driver-kind', choices=['fixture', 'vendor'], required=True)
    p.add_argument('--api-key-env', default='AENV_API_KEY')
    p.add_argument('--ca')
    p.add_argument('--kubectl', default='kubectl')
    p.add_argument('--context')
    p.add_argument('--size-mb', type=int, default=1024)
    p.add_argument('--envd-port', type=int, default=49983)
    p.add_argument('--timeout', type=int, default=600)
    p.add_argument('--lifecycle', action='store_true')
    p.add_argument('--output', required=True)
    args = p.parse_args()
    if not args.execute:
        p.error('--execute is required; this creates real PVCs and VMs')
    if not os.environ.get(args.api_key_env):
        p.error('API key environment variable is empty')
    if args.size_mb <= 0:
        p.error('size must be positive')
    result = PVCSuite(args).run()
    print(Path(args.output) / 'report.json')
    return result


if __name__ == '__main__':
    raise SystemExit(main())
