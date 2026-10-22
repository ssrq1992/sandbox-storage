#!/usr/bin/env python3
"""Local foundation checks only. Does not claim native lifecycle/E2E coverage."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--database', action='store_true', help='require a dedicated SSF_TEST_DATABASE_URL and execute database tests')
parser.add_argument('--output', default='validation/native-foundation')
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
os.chdir(root)
out = Path(args.output).resolve()
out.mkdir(parents=True, exist_ok=True)
def source_hashes():
    result = {}
    ignored = {'.git', 'target', 'node_modules', 'validation', '__pycache__', 'vendor'}
    extensions = {'.go', '.rs', '.proto', '.toml', '.json', '.yaml', '.yml', '.py', '.sh', '.mod', '.sum'}
    for source in (root, root.parent / 'AgentENV'):
        for directory, directories, files in os.walk(source):
            directories[:] = sorted(d for d in directories if d not in ignored)
            for name in sorted(files):
                path = Path(directory) / name
                if path.suffix in extensions or name in {'Cargo.lock', 'Dockerfile'} or name.startswith('Dockerfile.'):
                    result[str(path.relative_to(root.parent))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result

baseline_hashes = source_hashes()
(out / 'source-sha256.json').write_text(json.dumps(baseline_hashes, indent=2) + '\n')
report = {'scope': 'native-integration-foundation-only', 'time': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'host': platform.system(), 'checks': []}

def run(name, command, cwd=root, env=None, events=False):
    print(name, flush=True)
    with (out / (name + '.log')).open('w') as log:
        result = subprocess.run(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT)
    check = {'name': name, 'status': 'passed' if result.returncode == 0 else 'failed', 'exit_code': result.returncode, 'log': name + '.log'}
    if events:
        skipped = []
        for line in (out / (name + '.log')).read_text().splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get('Action') == 'skip' and event.get('Test'):
                skipped.append(event.get('Package', '') + '/' + event.get('Test', ''))
        check['skipped_tests'] = skipped
        if skipped and result.returncode == 0:
            check['status'] = 'passed_with_skips'
    report['checks'].append(check)

run('control-dependency-boundary', ['python3', 'scripts/check_control_boundary.py'])
run('control-sdk', ['go', 'test', '-race', './...'], cwd=root / 'sdk/go/control')
run('native-tool-contracts', ['python3', '-m', 'unittest', 'discover', '-s', 'scripts/tests', '-v'])
run('ssf-unit', ['go', 'test', '-count=1', '-json', './...'], events=True)
run('ssf-race', ['go', 'test', '-race', '-count=1', '-json', './...'], events=True)
run('ssf-vet', ['go', 'vet', './...'])
env = dict(os.environ, GOOS='linux', GOARCH='amd64', CGO_ENABLED='0')
run('ssf-linux-build', ['go', 'build', './cmd/...', './adapter/agentenvadapter/cmd/...'], env=env)
run('ssf-generated', ['python3', 'scripts/check_generated.py'])
run('native-openapi-compatible', ['go', 'run', './tools/check-openapi', '-baseline', '6cccaa7842bd5be2051111d4f74d9e37aa721244'])
run('agentenv-services-unit', ['go', 'test', '-count=1', '-json', './...'], cwd=root.parent / 'AgentENV/services', events=True)
run('agentenv-services-race', ['go', 'test', '-race', '-count=1', '-json', './...'], cwd=root.parent / 'AgentENV/services', events=True)
run('agentenv-services-vet', ['go', 'vet', './...'], cwd=root.parent / 'AgentENV/services')
run('agentenv-services-linux-build', ['go', 'build', './scheduler/cmd', './gateway/cmd'], cwd=root.parent / 'AgentENV/services', env=env)


if args.database:
    if not os.environ.get('SSF_TEST_DATABASE_URL'):
        report['checks'].append({'name': 'postgres-integration', 'status': 'failed', 'reason': 'SSF_TEST_DATABASE_URL must identify a dedicated test database'})
    else:
        run('postgres-integration', ['go', 'test', '-race', '-count=1', '-json', './ledger', './consume', './adapter/agentenvadapter'], events=True)
else:
    report['checks'].append({'name': 'postgres-integration', 'status': 'not_executed', 'reason': 'requires --database and dedicated SSF_TEST_DATABASE_URL'})
for name, reason in [
    ('rust-typecheck', 'requires Linux Rust toolchain and native build dependencies; not run by this foundation script'),
    ('linux-kvm-k8s-csi-e2e', 'requires completed native integration and real environment'),
]:
    report['checks'].append({'name': name, 'status': 'not_executed', 'reason': reason})
report['checks'].append({'name': 'source-stability', 'status': 'passed' if baseline_hashes == source_hashes() else 'failed', 'reason': 'validation must refer to one unchanged source snapshot'})
report['complete_plan'] = False
(out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
print(out / 'report.json', flush=True)
raise SystemExit(any(c['status'] == 'failed' for c in report['checks']))
