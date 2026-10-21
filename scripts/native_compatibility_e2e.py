#!/usr/bin/env python3
"""Verify original AgentENV API with provider disabled or enabled without SSF volumes.
Optional trusted scenario commands exercise environment-specific SDK/WS/P2P paths.
Reports every omitted path as not_executed. Uses no alternate sandbox API.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
from native_lifecycle_e2e import Suite, require

EXTERNAL = ['custom-extension', 'http-proxy', 'websocket-proxy', 'template-build', 'p2p']


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--url', required=True)
    p.add_argument('--image', required=True)
    p.add_argument('--api-key-env', default='AENV_API_KEY')
    p.add_argument('--ca')
    p.add_argument('--envd-port', type=int, default=49983)
    p.add_argument('--timeout', type=int, default=600)
    p.add_argument('--output', required=True)
    p.add_argument('--provider-mode', required=True, choices=['disabled', 'enabled-no-ssf'])
    p.add_argument('--scenarios', help='trusted JSON map of external case name to argv array')
    a = p.parse_args()
    a.driver_kind = 'none'
    if not a.execute or not os.environ.get(a.api_key_env):
        p.error('--execute and API key environment variable required')
    extra = json.loads(Path(a.scenarios).read_text()) if a.scenarios else {}
    if not isinstance(extra, dict) or set(extra) - set(EXTERNAL):
        p.error('unknown compatibility case')
    for argv in extra.values():
        if not isinstance(argv, list) or not argv or not all(isinstance(x, str) and x for x in argv):
            p.error('scenario must be a nonempty argv array')
    suite = Suite(a)
    suite.report['provider_mode'] = a.provider_mode
    suite.report['limitations'] = ['Provider mode is operator-selected; attach deployment evidence to prove actual configuration.']
    state = {}
    def cold():
        state['source'] = suite.create('/sandboxes-cold', {'image': a.image, 'timeout': 3600})
    def list_get():
        require(suite.api('GET', '/sandboxes/' + state['source']['sandboxID'])['sandboxID'] == state['source']['sandboxID'], 'get identity changed')
        for path in ['/sandboxes', '/v2/sandboxes']:
            result = suite.api('GET', path)
            require(state['source']['sandboxID'] in json.dumps(result), 'list omitted native ID')
    def stream():
        require(suite.guest(state['source'], 'printf native-compatible') == 'native-compatible', 'proxy stream mismatch')
    def template():
        snapshot = suite.api('POST', '/sandboxes/' + state['source']['sandboxID'] + '/snapshots', {'name': suite.id + '-compat'}, 201)
        suite.report['snapshots'].append(snapshot['snapshotID'])
        for endpoint in ['/sandboxes', '/v2/sandboxes']:
            child = suite.create(endpoint, {'templateID': snapshot['snapshotID'], 'timeout': 3600})
            require(suite.guest(child, 'printf template-compatible') == 'template-compatible', 'template not runnable')
    def pause():
        identity = state['source']['sandboxID']
        suite.api('POST', '/sandboxes/' + identity + '/pause', expected=204)
        restored = suite.api('POST', '/v2/sandboxes/' + identity + '/connect', {'timeout': 3600}, 201)
        require(restored['sandboxID'] == identity, 'connect changed native ID')
        state['source'] = restored
    def timeout():
        # autoPause absent defaults to pause-on-timeout (upstream AgentENV semantics);
        # this case verifies the delete path, so opt out of pause explicitly.
        child = suite.create('/sandboxes-cold', {'image': a.image, 'timeout': 3600, 'autoPause': False})
        identity = child['sandboxID']
        suite.api('POST', '/sandboxes/' + identity + '/timeout', {'timeout': 1}, 204)
        deadline = time.monotonic() + a.timeout
        while True:
            # List is side-effect-free and does not auto-resume an expired instance.
            if identity not in json.dumps(suite.api('GET', '/v2/sandboxes')):
                break
            require(time.monotonic() < deadline, 'timeout did not expire the sandbox')
            time.sleep(1)
    def auto_pause():
        child = suite.create('/sandboxes-cold', {'image': a.image, 'timeout': 2, 'autoPause': True})
        identity = child['sandboxID']
        deadline = time.monotonic() + a.timeout
        while True:
            observed = suite.api('GET', '/sandboxes/' + identity)
            if str(observed.get('state', '')).lower() == 'paused': break
            require(time.monotonic() < deadline, 'automatic pause was not observed')
            time.sleep(1)
        resumed = suite.api('POST', '/v2/sandboxes/' + identity + '/connect', {'timeout': 3600}, 201)
        require(resumed['sandboxID'] == identity, 'automatic resume changed native ID')
        suite.guest(resumed, 'true')
    def delete():
        identity = state['source']['sandboxID']
        suite.api('DELETE', '/sandboxes/' + identity, expected=204)
        suite.api('GET', '/sandboxes/' + identity, expected=404)
    failed = False
    cases = [('cold', cold), ('list-get-v1-v2', list_get), ('stream-proxy', stream),
             ('templates-v1-v2', template), ('pause-connect', pause), ('timeout', timeout),
             ('automatic-pause-resume', auto_pause), ('delete', delete)]
    for name, action in cases:
        row = {'name': name, 'status': 'not_executed'}
        suite.report['cases'].append(row)
        if not failed:
            try:
                action()
                row['status'] = 'passed'
            except Exception as error:
                row.update(status='failed', error=str(error))
                failed = True
        suite.save()
    for name in EXTERNAL:
        row = {'name': name, 'status': 'not_executed', 'reason': 'requires environment-specific SDK/server scenario'}
        suite.report['cases'].append(row)
        if name in extra and not failed:
            try:
                result = subprocess.run(extra[name], timeout=a.timeout, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    env=dict(os.environ, SSF_TEST_URL=a.url, SSF_TEST_IMAGE=a.image, SSF_TEST_CONTEXT_MODE=a.provider_mode))
                require(result.returncode == 0, 'external compatibility scenario failed')
                row.update(status='passed', reason='operator scenario completed')
            except Exception as error:
                row.update(status='failed', reason=str(error))
                failed = True
        suite.save()
    suite.report['status'] = 'failed' if failed else 'passed'
    suite.report['complete_compatibility_matrix'] = all(c['status'] == 'passed' for c in suite.report['cases'])
    suite.save()
    print(suite.root / 'report.json')
    return int(failed)

if __name__ == '__main__':
    raise SystemExit(main())
