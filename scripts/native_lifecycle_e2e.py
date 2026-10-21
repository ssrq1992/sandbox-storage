#!/usr/bin/env python3
"""Exercise the original AgentENV Gateway API and envd proxy against a real deployment.

This creates billable resources only with --execute. No prototype API or fake
success path exists. A failed/unknown create is never automatically retried.
"""
import argparse
import base64
import datetime
import json
import os
from pathlib import Path
import shlex
import ssl
import struct
import time
import urllib.error
import urllib.request
import uuid


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def redact(value):
    if isinstance(value, dict):
        return {key: ('[REDACTED]' if any(word in key.lower() for word in
                ('token', 'ticket', 'password', 'secret', 'authorization')) else redact(item))
                for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


class Suite:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.output)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.key = os.environ[args.api_key_env]
        self.opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(
            context=ssl.create_default_context(cafile=args.ca)))
        self.id = 'native-' + uuid.uuid4().hex[:20]
        self.report = {'version': 1, 'run': self.id, 'started': datetime.datetime.now(
            datetime.timezone.utc).isoformat(), 'status': 'running', 'driver_kind': args.driver_kind,
            'scope': 'native-api-and-guest-lifecycle', 'real_environment_executed': True,
            'volume_source': 'import' if getattr(args, 'import_pvc', None) else 'dynamic',
            'storage_class': getattr(args, 'storage_class', None),
            'mixed_legacy': getattr(args, 'mixed', False),
            'cases': [], 'requests': [], 'sandboxes': [], 'volumes': [], 'snapshots': [],
            'limitations': ['Does not certify node/process fault recovery or CSI detach ordering',
                            'Fixture results are not vendor CSI certification']}
        self.save()

    def save(self):
        path = self.root / 'report.tmp'
        with path.open('w') as output:
            os.chmod(path, 0o600)
            json.dump(redact(self.report), output, indent=2)
            output.flush()
            os.fsync(output.fileno())
        os.replace(path, self.root / 'report.json')
        fd = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def api(self, method, path, body=None, expected=200):
        receipt = {'method': method, 'path': path, 'body': redact(body), 'state': 'pending'}
        self.report['requests'].append(receipt)
        self.save()  # request uncertainty survives an interrupted test runner
        request = urllib.request.Request(self.args.url.rstrip('/') + path,
            data=None if body is None else json.dumps(body).encode(), method=method,
            headers={'X-API-Key': self.key, 'Content-Type': 'application/json'})
        try:
            with self.opener.open(request, timeout=self.args.timeout) as response:
                data = response.read(4 * 1024 * 1024 + 1)
                receipt.update(status=response.status, state='received')
                require(len(data) <= 4 * 1024 * 1024, 'oversized API response')
                result = json.loads(data) if data else None
                receipt['response'] = redact(result)
                # Preserve unexpected successful creates before rejecting their
                # status; cleanup must still know the native instance identity.
                if response.status == 201 and path in {'/sandboxes', '/sandboxes-cold', '/v2/sandboxes'}:
                    if isinstance(result, dict) and result.get('sandboxID'):
                        if result['sandboxID'] not in self.report['sandboxes']:
                            self.report['sandboxes'].append(result['sandboxID'])
                require(response.status == expected, f'{method} {path}: expected {expected}, got {response.status}')
                return result
        except urllib.error.HTTPError as error:
            receipt.update(state='rejected', status=error.code)
            with error:
                data = error.read(4 * 1024 * 1024 + 1)
            require(len(data) <= 4 * 1024 * 1024, 'oversized API rejection')
            try:
                result = json.loads(data) if data else None
            except ValueError:
                result = {'message': data.decode(errors='replace')}
            receipt['response'] = redact(result)
            if error.code == expected:
                return result
            raise AssertionError(f'{method} {path}: HTTP {error.code}; expected {expected}') from error
        finally:
            self.save()

    def create(self, path, body):
        sandbox = self.api('POST', path, body, 201)
        require(isinstance(sandbox, dict) and sandbox.get('sandboxID'), 'native sandboxID missing')
        if sandbox['sandboxID'] not in self.report['sandboxes']:
            self.report['sandboxes'].append(sandbox['sandboxID'])
        self.save()
        return sandbox

    def guest(self, sandbox, command):
        payload = json.dumps({'process': {'cmd': '/bin/sh', 'args': ['-c', 'set -eu; ' + command],
                                        'cwd': '/'}, 'stdin': False}).encode()
        headers = {'X-API-Key': self.key, 'x-agentenv-sandbox-id': sandbox['sandboxID'],
                   'x-agentenv-target-port': str(self.args.envd_port),
                   'Content-Type': 'application/connect+json', 'Connect-Protocol-Version': '1',
                   'Authorization': 'Basic ' + base64.b64encode(b'root:').decode()}
        for source, target in [('envdAccessToken', 'X-Access-Token'),
                               ('trafficAccessToken', 'e2b-traffic-access-token')]:
            if sandbox.get(source):
                headers[target] = sandbox[source]
        request = urllib.request.Request(self.args.url.rstrip('/') + '/proxy/process.Process/Start',
            data=b'\0' + struct.pack('>I', len(payload)) + payload, headers=headers)
        code, output, total = None, bytearray(), 0
        with self.opener.open(request, timeout=self.args.timeout) as response:
            def exact(size):
                data = bytearray()
                while len(data) < size:
                    chunk = response.read(size - len(data))
                    require(bool(chunk), 'truncated guest stream')
                    data.extend(chunk)
                return bytes(data)
            while True:
                first = response.read(1)
                if not first:
                    break
                flags, size = first[0], struct.unpack('>I', exact(4))[0]
                total += size
                require(not flags & 1 and total <= 4 * 1024 * 1024, 'invalid/oversized guest stream')
                event = json.loads(exact(size))
                if flags & 2:
                    require(not event.get('error'), 'envd stream returned an error')
                    break
                event = event.get('event', {})
                output.extend(base64.b64decode(event.get('data', {}).get('stdout', '')))
                if 'end' in event:
                    code = int(event['end'].get('exitCode', 0))
        require(code == 0, f'guest process did not confirm success: exit={code}')
        return output.decode()

    def run(self):
        state = {}
        def cold():
            name = self.id + '-volume'
            storage = {'namespace': self.args.namespace, 'storageClass': self.args.storage_class,
                       'snapshotClass': self.args.snapshot_class}
            if self.args.import_pvc:
                storage['import'] = self.args.import_pvc
            volume = self.api('POST', '/volumes', {'name': name, 'provider': 'ssf',
                'sizeMB': self.args.size_mb, 'mode': 'exclusive', 'storage': storage}, 201)
            self.report['volumes'].append({'name': name, 'descriptor': volume, 'imported': bool(self.args.import_pvc)})
            state['mounts'] = [{'name': name, 'path': '/mnt/ssf-e2e'}]
            if self.args.mixed:
                legacy_name = self.id + '-legacy'
                legacy = self.api('POST', '/volumes', {'name': legacy_name,
                    'sizeMB': self.args.size_mb, 'mode': 'exclusive'}, 201)
                self.report['volumes'].append({'name': legacy_name, 'descriptor': legacy, 'imported': False})
                state['mounts'].append({'name': legacy_name, 'path': '/mnt/legacy-e2e'})
            state['source'] = self.create('/sandboxes-cold', {'image': self.args.image,
                'timeout': 3600, 'volumeMounts': state['mounts']})
            write(state['source'], self.id)
        def write(sandbox, value):
            paths = ['/mnt/ssf-e2e'] + (['/mnt/legacy-e2e'] if self.args.mixed else [])
            self.guest(sandbox, '; '.join('printf %s ' + shlex.quote(value) + ' > ' + path + '/marker'
                                        for path in paths) + '; sync')
        def check(sandbox, value=None, legacy=True):
            paths = ['/mnt/ssf-e2e'] + (['/mnt/legacy-e2e'] if self.args.mixed and legacy else [])
            for path in paths:
                require(self.guest(sandbox, 'cat ' + path + '/marker') == (value or self.id),
                        'persistent data mismatch at ' + path)
        def reject_writer():
            # A successful conflicting create is retained in the report and
            # fails this case; the runner never hides an unexpected second VM.
            self.api('POST', '/sandboxes-cold', {'image': self.args.image,
                'timeout': 3600, 'volumeMounts': state['mounts']}, 409)
        def running_writer_conflict():
            reject_writer()
            check(state['source'])
        def pause_resume():
            sandbox = state['source']
            self.api('POST', '/sandboxes/' + sandbox['sandboxID'] + '/pause', expected=204)
            reject_writer()  # paused UseHold must still exclude a new writer
            resumed = self.api('POST', '/sandboxes/' + sandbox['sandboxID'] + '/resume',
                               {'timeout': 3600}, 201)
            require(resumed['sandboxID'] == sandbox['sandboxID'], 'resume changed native ID')
            state['source'] = resumed
            check(resumed)
        def connect_resume():
            sandbox = state['source']
            self.api('POST', '/sandboxes/' + sandbox['sandboxID'] + '/pause', expected=204)
            resumed = self.api('POST', '/v2/sandboxes/' + sandbox['sandboxID'] + '/connect',
                               {'timeout': 3600}, 201)
            require(resumed['sandboxID'] == sandbox['sandboxID'], 'connect resume changed native ID')
            state['source'] = resumed
            check(resumed)
        def offline_expand():
            name = state['mounts'][0]['name']
            original = self.api('GET', '/volumes/' + name)
            target = max(original['sizeMB'], self.args.size_mb) * 2
            before = [int(value) for value in self.guest(state['source'],
                "stat -f -c '%b %S' /mnt/ssf-e2e").split()]
            require(len(before) == 2, 'filesystem capacity observation missing')
            self.api('POST', '/volumes/' + name + '/expand', {'sizeMB': target}, 202)
            # A pending resize must neither mutate a running filesystem nor
            # prevent the same logical owner from completing pause/resume.
            pause_resume()
            self.api('DELETE', '/sandboxes/' + state['source']['sandboxID'], expected=204)
            deadline = time.monotonic() + self.args.timeout
            while True:
                volume = self.api('GET', '/volumes/' + name)
                if volume['status'] == 'ready' and volume['sizeMB'] >= target:
                    break
                require(time.monotonic() < deadline, 'offline controller expansion did not complete')
                time.sleep(1)
            state['source'] = self.create('/sandboxes-cold', {'image': self.args.image,
                'timeout': 3600, 'volumeMounts': state['mounts']})
            check(state['source'])
            after = [int(value) for value in self.guest(state['source'],
                "stat -f -c '%b %S' /mnt/ssf-e2e").split()]
            require(len(after) == 2 and after[0] * after[1] > before[0] * before[1],
                    'guest ext4 did not grow after offline expansion')
            self.api('POST', '/volumes/' + name + '/expand', {'sizeMB': 1}, 409)
        def snapshot():
            # Keep a real guest FD open across memory capture. Replacement must
            # preserve that old inode on a retained clone without exposing it
            # through the new public volume mount.
            hold = "exec 9< /mnt/ssf-e2e/marker; while :; do cat /proc/self/fd/9 > /tmp/ssf-held-marker; sleep 1; done"
            self.guest(state['source'], 'sh -c ' + shlex.quote(hold) + ' >/tmp/ssf-held-log 2>&1 </dev/null &')
            held_marker(state['source'])
            snapshot = self.api('POST', '/sandboxes/' + state['source']['sandboxID'] +
                                '/snapshots', {'name': self.id + '-snapshot'}, 201)
            state['snapshot'] = snapshot['snapshotID']
            self.report['snapshots'].append(state['snapshot'])
            check(state['source'])  # snapshot must leave its source running
        def fork():
            children = self.api('POST', '/sandboxes/' + state['source']['sandboxID'] +
                                '/fork', {'count': 2, 'timeout': 3600}, 201)
            require(isinstance(children, list) and len(children) == 2, 'Fork result cardinality changed')
            # Record every success before asserting: native Fork permits partial success.
            for child in children:
                if (child.get('sandbox') or {}).get('sandboxID'):
                    self.report['sandboxes'].append(child['sandbox']['sandboxID'])
            self.save()
            require(all(child.get('sandbox') and not child.get('error') for child in children),
                    'Fork returned partial failures')
            ids = [child['sandbox']['sandboxID'] for child in children]
            require(len(set(ids + [state['source']['sandboxID']])) == 3, 'Fork reused native ID')
            for child in children:
                check(child['sandbox'])
            write(children[0]['sandbox'], 'child')
            check(children[0]['sandbox'], 'child')
            check(children[1]['sandbox'])
            check(state['source'])
        def template():
            restored = self.create('/v2/sandboxes', {'templateID': state['snapshot'], 'timeout': 3600})
            check(restored)  # omitted volumeMounts inherits the checkpoint
            write(restored, 'template')
            check(restored, 'template')
            check(state['source'])
        def held_marker(sandbox):
            output = self.guest(sandbox, 'rm -f /tmp/ssf-held-marker; i=0; '
                'while [ ! -s /tmp/ssf-held-marker ] && [ "$i" -lt 10 ]; do i=$((i+1)); sleep 1; done; '
                'cat /tmp/ssf-held-marker')
            require(output == self.id, 'restored open descriptor lost its captured filesystem')
        def empty_mounts():
            restored = self.create('/v2/sandboxes', {'templateID': state['snapshot'],
                'timeout': 3600, 'volumeMounts': []})
            self.guest(restored, '! grep -q " /mnt/ssf-e2e " /proc/mounts; ! test -f /mnt/ssf-e2e/marker')
            if self.args.mixed:
                self.guest(restored, '! grep -q " /mnt/legacy-e2e " /proc/mounts')
            held_marker(restored)
            check(state['source'])
        def replacement_mounts():
            name = self.id + '-replacement'
            volume = self.api('POST', '/volumes', {'name': name, 'provider': 'ssf',
                'sizeMB': self.args.size_mb, 'mode': 'exclusive', 'storage': {
                    'namespace': self.args.namespace, 'storageClass': self.args.storage_class,
                    'snapshotClass': self.args.snapshot_class}}, 201)
            self.report['volumes'].append({'name': name, 'descriptor': volume, 'imported': False})
            restored = self.create('/v2/sandboxes', {'templateID': state['snapshot'], 'timeout': 3600,
                'volumeMounts': [{'name': name, 'path': '/mnt/ssf-e2e'}]})
            self.guest(restored, '! test -f /mnt/ssf-e2e/marker; '
                       'printf replacement > /mnt/ssf-e2e/marker; sync')
            check(restored, 'replacement', legacy=False)
            held_marker(restored)
            if self.args.mixed:
                self.guest(restored, '! grep -q " /mnt/legacy-e2e " /proc/mounts')
            check(state['source'])
        cases = [('cold-data', cold), ('running-writer-conflict', running_writer_conflict),
                 ('pause-resume-and-writer-hold', pause_resume),
                 ('connect-resume', connect_resume), ('offline-expand', offline_expand),
                 ('snapshot-source-running', snapshot), ('fork-independent-writers', fork),
                 ('template-inheritance', template), ('template-empty-mounts', empty_mounts),
                 ('template-replacement-mounts', replacement_mounts)]
        failed = False
        for name, action in cases:
            row = {'name': name, 'status': 'not_executed'}
            self.report['cases'].append(row)
            if not failed:
                started = time.monotonic()
                try:
                    action()
                    row['status'] = 'passed'
                except Exception as error:
                    row.update(status='failed', error=str(error))
                    failed = True
                row['duration_seconds'] = time.monotonic() - started
            else:
                row['reason'] = 'prerequisite failed; no success inferred'
            self.save()
        self.report['status'] = 'failed' if failed else 'passed'
        self.save()
        return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--url', required=True)
    parser.add_argument('--api-key-env', default='AENV_API_KEY')
    parser.add_argument('--ca')
    parser.add_argument('--image', required=True)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--storage-class', required=True)
    parser.add_argument('--snapshot-class', required=True)
    parser.add_argument('--driver-kind', choices=['fixture', 'vendor'], required=True)
    parser.add_argument('--import-pvc', help='JSON file with namespace/name/uid of a dedicated initialized test PVC')
    parser.add_argument('--mixed', action='store_true', help='also verify a legacy volume in the same captures')
    parser.add_argument('--size-mb', type=int, default=1024)
    parser.add_argument('--envd-port', type=int, default=49983)
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if not args.execute:
        parser.error('--execute is required; this test creates real volumes and VMs')
    if not os.environ.get(args.api_key_env):
        parser.error('API key environment variable is empty')
    if args.import_pvc:
        args.import_pvc = json.loads(Path(args.import_pvc).read_text())
        if set(args.import_pvc) != {'namespace', 'name', 'uid'} or not all(
                isinstance(value, str) and value for value in args.import_pvc.values()):
            parser.error('import PVC must specify namespace, name and uid')
        if args.import_pvc['namespace'] != args.namespace:
            parser.error('import PVC namespace differs from --namespace')
    result = Suite(args).run()
    print(str(Path(args.output) / 'report.json'))
    return result


if __name__ == '__main__':
    raise SystemExit(main())
