#!/usr/bin/env python3
"""Run SSF ledger/native-plugin tests in a disposable local PostgreSQL cluster.

Requires preinstalled PostgreSQL binaries; never connects to an existing DB.
Only a private Unix socket is enabled. The cluster is stopped and removed on exit.
"""
import argparse
import datetime
import getpass
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
from urllib.parse import urlencode

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--postgres-bin', required=True)
parser.add_argument('--output', default='validation/native-database')
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
binaries = Path(args.postgres_bin).resolve()
out = (root / args.output).resolve()
out.mkdir(parents=True, exist_ok=True)
for name in ('initdb', 'pg_ctl', 'createdb', 'postgres'):
    if not os.access(binaries / name, os.X_OK):
        parser.error('missing executable: ' + str(binaries / name))
report = {'scope': 'postgres-ledger-native-plugin-and-overlaybd-journal-with-fixtures',
          'time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'real_csi': False, 'real_kvm': False, 'status': 'failed'}
source_hashes = {}
for source in (root / 'ledger', root / 'consume', root / 'preparation', root / 'adapter/agentenvadapter',
               root / 'drivers/overlaybd', root / 'gateway'):
    for path in source.rglob('*.go'):
        source_hashes[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
(out / 'source-sha256.json').write_text(json.dumps(source_hashes, indent=2) + '\n')
exit_code = 1
temporary = tempfile.mkdtemp(prefix='ssf-pg-')
cluster = Path(temporary)
try:
    cluster.chmod(0o700)
    data = cluster / 'data'
    socket = cluster / 'socket'
    socket.mkdir(mode=0o700)
    started = False
    try:
        with (out / 'setup.log').open('w') as log:
            subprocess.run([str(binaries / 'initdb'), '-D', str(data), '--no-locale', '--encoding=UTF8',
                            '--auth-local=trust', '--auth-host=reject'], stdout=log, stderr=subprocess.STDOUT, check=True)
            options = shlex.join(['-h', '', '-k', str(socket), '-c', 'unix_socket_permissions=0700'])
            subprocess.run([str(binaries / 'pg_ctl'), '-D', str(data), '-l', str(out / 'postgres.log'),
                            '-o', options, '-w', '-t', '30', 'start'], stdout=log, stderr=subprocess.STDOUT, check=True)
            started = True
            subprocess.run([str(binaries / 'createdb'), '-h', str(socket), 'ssf_native_test'],
                           stdout=log, stderr=subprocess.STDOUT, check=True)
        connection = 'postgresql:///ssf_native_test?' + urlencode({'host': str(socket), 'user': getpass.getuser(), 'sslmode': 'disable'})
        env = dict(os.environ, SSF_TEST_DATABASE_URL=connection, SSF_ADAPTER_TEST_DATABASE_URL=connection)
        with (out / 'tests.log').open('w') as log:
            result = subprocess.run(['go', 'test', '-race', '-count=1', '-timeout=5m', '-json',
                                     './ledger', './consume', './preparation', './adapter/agentenvadapter',
                                     './drivers/overlaybd', './gateway'],
                                    cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        report['status'] = 'passed' if result.returncode == 0 else 'failed'
        report['exit_code'] = result.returncode
        report['skipped_tests'] = []
        for line in (out / 'tests.log').read_text().splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get('Action') == 'skip' and event.get('Test'):
                report['skipped_tests'].append(event['Package'] + '/' + event['Test'])
        if report['skipped_tests'] and report['status'] == 'passed':
            report['status'] = 'passed_with_skips'
        exit_code = result.returncode
        # Exercise the delivered read-only invariant tool on actual PostgreSQL,
        # including a deliberately corrupted writer fixture that must fail.
        if result.returncode == 0:
            import re
            subprocess.run([str(binaries / 'createdb'), '-h', str(socket), 'ssf_invariant_test'], check=True, stdout=subprocess.DEVNULL)
            invariant_dsn = connection.replace('/ssf_native_test?', '/ssf_invariant_test?')
            invariant_env = dict(os.environ, PGDATABASE='ssf_invariant_test', PGHOST=str(socket), PGUSER=getpass.getuser(), SSF_DATABASE_URL=invariant_dsn)
            schema = re.search(r'const Schema = `(.*?)`', (root/'ledger/postgres.go').read_text(), re.S).group(1)
            def sql(statement):
                subprocess.run([str(binaries/'psql'), '-X', '-q', '-v', 'ON_ERROR_STOP=1'], input=statement,
                    text=True, env=invariant_env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            sql(schema)
            sql((root/'deploy/ledger-roles.sql').read_text())
            sql("SET ROLE ssf_consumer; SELECT id FROM ssf_idle_captures LIMIT 0; SELECT namespace FROM ssf_snapshot_captures LIMIT 0; SELECT volume_key FROM ssf_backend_tombstones LIMIT 0; RESET ROLE;")
            report['consumer_role_schema_access'] = 'passed'
            assertion = ['python3', 'scripts/native_invariants.py', '--psql', str(binaries/'psql'), '--output', str(out/'invariants-clean.json')]
            clean = subprocess.run(assertion, cwd=root, env=invariant_env, stdout=subprocess.DEVNULL).returncode
            sql("INSERT INTO ssf_volumes VALUES('volume','pvc','{}',false); INSERT INTO ssf_use_holds VALUES('volume','owner-a',false,'fixture'),('volume','owner-b',false,'fixture'); INSERT INTO ssf_idle_captures(id,volume_key,pvc_uid,digest,state) VALUES('idle','volume','pvc','fixture','Capturing');")
            assertion[-1] = str(out/'invariants-corrupt-fixture.json')
            corrupt = subprocess.run(assertion, cwd=root, env=invariant_env, stdout=subprocess.DEVNULL).returncode
            corrupt_checks = json.loads((out/'invariants-corrupt-fixture.json').read_text())['checks']
            idle_detected = any(c['name'] == 'idle-capture-use-conflict' and c['violations'] > 0 for c in corrupt_checks)
            report['idle_capture_invariant_detected'] = idle_detected
            report['invariant_tool'] = {'clean_exit': clean, 'corrupt_fixture_exit': corrupt, 'status': 'passed' if clean == 0 and corrupt == 1 else 'failed'}
            if clean != 0 or corrupt != 1 or not idle_detected:
                report['status'] = 'failed'
                exit_code = 1
    except (OSError, subprocess.CalledProcessError) as error:
        report['status'] = 'failed'
        exit_code = 1
        report['error'] = str(error)
    finally:
        if started or (data / 'postmaster.pid').exists():
            with (out / 'shutdown.log').open('w') as log:
                result = subprocess.run([str(binaries / 'pg_ctl'), '-D', str(data), '-m', 'fast', '-w', 'stop'],
                                        stdout=log, stderr=subprocess.STDOUT)
            report['shutdown_exit_code'] = result.returncode
            if result.returncode:
                report['status'] = 'failed'
                exit_code = result.returncode
finally:
    if (cluster / 'data' / 'postmaster.pid').exists():
        report['preserved_cluster'] = str(cluster)
        report['status'] = 'failed'
        exit_code = 1
    else:
        shutil.rmtree(cluster)
(out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
print(out / 'report.json')
raise SystemExit(exit_code)
