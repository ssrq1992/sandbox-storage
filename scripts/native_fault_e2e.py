#!/usr/bin/env python3
"""Run explicit real-environment fault scenarios with mandatory recovery assertions.
Scenario JSON is trusted local code: argv arrays, never shell interpolation.
Only the disposable ssf-test-driver supports CSI injection. Production processes
can be restarted by explicit scenario commands. Nothing runs without --execute.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import uuid

STAGES = ['reserve', 'bind', 'attach', 'dispatch', 'spawn', 'ready-commit',
          'pause', 'freeze', 'publish', 'release',
          'preparation-acquire', 'preparation-publish', 'preparation-format',
          'preparation-receipt', 'preparation-release', 'preparation-handoff',
          'preparation-delete',
          'overlaybd-create', 'overlaybd-device-create', 'overlaybd-restack',
          'overlaybd-upload', 'overlaybd-head-commit', 'overlaybd-daemon-restart',
          'overlaybd-expansion-tool', 'overlaybd-expansion-seal']


def command(value):
    if not isinstance(value, list) or not value or not all(isinstance(x, str) and x and '\0' not in x for x in value):
        raise ValueError('commands must be nonempty argv arrays')
    return value


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--scenarios', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--context', required=True)
    p.add_argument('--namespace', required=True)
    p.add_argument('--fixture-pod')
    p.add_argument('--fixture-container', default='driver')
    p.add_argument('--driver-kind', choices=('unspecified', 'fixture', 'vendor', 'native-overlaybd'),
                   default='unspecified', help='Recorded evidence scope; native OverlayBD cannot use fixture CSI injection')
    p.add_argument('--timeout', type=int, default=900)
    a = p.parse_args()
    spec = json.loads(Path(a.scenarios).read_text())
    if spec.get('version') != 1 or not isinstance(spec.get('cases'), list):
        p.error('expected version 1 cases')
    names = set()
    for case in spec['cases']:
        if case['stage'] not in STAGES or case['name'] in names:
            p.error('invalid stage or duplicate case')
        names.add(case['name'])
        for key in ('workload', 'recover', 'assertions'):
            if not isinstance(case.get(key), list) or not case[key]:
                p.error('each scenario needs workload, recover and assertions command arrays')
            for argv in case[key]: command(argv)
        for argv in case.get('inject', []): command(argv)
        if not case.get('inject') and not case.get('csi_fault'):
            p.error('scenario must actually inject a fault')
        if case.get('csi_fault') and not a.fixture_pod:
            p.error('CSI fault requires explicit --fixture-pod')
        if case.get('csi_fault') and a.driver_kind in ('vendor', 'native-overlaybd'):
            p.error('fixture CSI injection cannot validate a real vendor or native OverlayBD driver')
        if case['stage'].startswith('overlaybd-') and a.driver_kind != 'native-overlaybd':
            p.error('OverlayBD failure stages require --driver-kind native-overlaybd')
    out = Path(a.output)
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.umask(0o077)
    report = {'version': 1, 'scope': 'real-fault-scenarios', 'executed': a.execute,
              'driver_kind': a.driver_kind,
              'context': a.context, 'time': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'cases': []}
    def save():
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    def run(argv, expected=0):
        # Do not persist command output/arguments: operator commands may carry credentials.
        result = subprocess.run(command(argv), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                timeout=a.timeout, env=dict(os.environ, SSF_TEST_CONTEXT=a.context, SSF_TEST_NAMESPACE=a.namespace))
        if expected is not None and result.returncode != expected:
            raise RuntimeError('scenario command failed with exit ' + str(result.returncode))
        return result.returncode
    kubectl = ['kubectl', '--context', a.context, '--request-timeout=30s']
    fault_path = None
    def fixture_write(data):
        subprocess.run(kubectl + ['exec', '-i', '-n', a.namespace, a.fixture_pod, '-c', a.fixture_container,
                       '--', 'sh', '-ceu', 'umask 077; cat > "$1.tmp"; mv "$1.tmp" "$1"', 'ssf-fixture', fault_path],
                       input=json.dumps(data).encode(), stdout=subprocess.DEVNULL, timeout=45, check=True)
    if a.execute and any(c.get('csi_fault') for c in spec['cases']):
        pod = json.loads(subprocess.check_output(kubectl + ['get', 'pod', a.fixture_pod, '-n', a.namespace, '-o', 'json']))
        containers = [c for c in pod['spec']['containers'] if c['name'] == a.fixture_container]
        if len(containers) != 1:
            p.error('fixture container not found')
        args = containers[0].get('command', []) + containers[0].get('args', [])
        if not any('ssf-test-driver' in x for x in args):
            p.error('refusing to inject into a non-fixture container')
        for index, arg in enumerate(args):
            if arg.startswith('--fault-file='): fault_path = arg.split('=', 1)[1]
            if arg == '--fault-file' and index + 1 < len(args): fault_path = args[index + 1]
        if not fault_path or not fault_path.startswith('/'):
            p.error('fixture must explicitly enable --fault-file with absolute path')
    failed = False
    try:
        for case in spec['cases']:
            row = {'name': case['name'], 'stage': case['stage'], 'status': 'not_executed'}
            report['cases'].append(row)
            save()
            if not a.execute:
                continue
            try:
                if case.get('csi_fault'):
                    fault = {'version': 1, 'id': uuid.uuid4().hex, 'rules': case['csi_fault']}
                    fixture_write(fault)
                for argv in case.get('inject', []): run(argv)
                row['workload_exit_codes'] = [run(argv, None) for argv in case['workload']]
                if case.get('csi_fault'):
                    state = json.loads(subprocess.check_output(kubectl + ['exec', '-n', a.namespace, a.fixture_pod,
                        '-c', a.fixture_container, '--', 'cat', fault_path + '.state']))
                    if state.get('id') != fault['id'] or not all(state.get('counts', {}).get(f'{i}/{rule["phase"]}', 0) >= rule.get('count', 1)
                            for i, rule in enumerate(fault['rules'])):
                        raise RuntimeError('injection was not observed for every rule')
                    row['injection_counts'] = state['counts']
                row['injected'] = True
            except Exception as error:
                row['injection_or_workload_error'] = type(error).__name__ + ': ' + str(error)
            finally:
                try:
                    if case.get('csi_fault'): fixture_write({'version': 1, 'id': uuid.uuid4().hex, 'rules': []})
                    for argv in case['recover']: run(argv)
                    for argv in case['assertions']: run(argv)
                    row['status'] = 'passed' if row.get('injected') else 'failed'
                except Exception as error:
                    row.update(status='failed', recovery_error=type(error).__name__ + ': ' + str(error))
                failed |= row['status'] == 'failed'
                save()
            if failed:
                break  # do not stack another failure over unreconciled resources
    finally:
        covered = {c['stage'] for c in report['cases'] if c['status'] == 'passed'}
        report['unverified_stages'] = [s for s in STAGES if s not in covered]
        report['complete_fault_matrix'] = not report['unverified_stages']
        report['status'] = 'failed' if failed else ('passed' if a.execute else 'not_executed')
        save()
    print(out / 'report.json')
    return int(failed)

if __name__ == '__main__':
    raise SystemExit(main())
