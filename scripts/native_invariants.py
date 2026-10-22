#!/usr/bin/env python3
"""Read-only PostgreSQL invariant assertions for fault/recovery scenarios.
Use a least-privilege read-only login. No row identities or DSN are printed.
This proves ledger consistency only; guest/process/device evidence is separate.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import urlsplit, parse_qs, unquote

QUERIES = {
    'preparation-ready-without-released-content': """SELECT count(*) FROM ssf_volume_preparations
      WHERE state='VolumeReady' AND (
        coalesce(descriptor->'receipt'->>'uuid','')='' OR
        coalesce((descriptor->'receipt'->>'released')::boolean,false)=false OR
        coalesce((descriptor->'receipt'->>'rejected')::boolean,false)=true)""",
    'preparation-uses-sandbox-identity': """SELECT count(*) FROM ssf_volume_preparations p
      JOIN ssf_bindings b ON b.operation_id=p.descriptor->>'operation'
      WHERE coalesce(b.sandbox,'')<>''""",
    'exclusive-owner-collision': """SELECT count(*) FROM (
      SELECT volume_key FROM (
        SELECT volume_key,owner_uid,readonly FROM ssf_bindings WHERE state<>'Released'
        UNION ALL SELECT volume_key,owner_uid,readonly FROM ssf_use_holds
      ) u GROUP BY volume_key HAVING bool_or(NOT readonly) AND count(DISTINCT owner_uid)>1
    ) x""",
    'concurrent-owner-runs': """SELECT count(*) FROM (
      SELECT owner_uid FROM ssf_bindings WHERE state<>'Released'
      GROUP BY owner_uid HAVING count(DISTINCT run_uid)>1
    ) x""",
    'maintenance-conflict': """SELECT count(*) FROM ssf_volume_maintenance m WHERE m.state='Active' AND (
      EXISTS(SELECT 1 FROM ssf_bindings b WHERE b.volume_key=m.volume_key AND b.state<>'Released') OR
      EXISTS(SELECT 1 FROM ssf_use_holds h WHERE h.volume_key=m.volume_key) OR
      EXISTS(SELECT 1 FROM ssf_attachments a WHERE a.volume_key=m.volume_key AND a.state<>'Detached'))""",
    'idle-capture-use-conflict': """SELECT count(*) FROM ssf_idle_captures c WHERE c.state='Capturing' AND (
      EXISTS(SELECT 1 FROM ssf_bindings b WHERE b.volume_key=c.volume_key AND b.state<>'Released') OR
      EXISTS(SELECT 1 FROM ssf_use_holds h WHERE h.volume_key=c.volume_key) OR
      EXISTS(SELECT 1 FROM ssf_attachments a WHERE a.volume_key=c.volume_key AND a.state<>'Detached') OR
      EXISTS(SELECT 1 FROM ssf_volume_maintenance m WHERE m.volume_key=c.volume_key AND m.state='Active') OR
      EXISTS(SELECT 1 FROM ssf_volume_preparations p WHERE p.pvc_uid=c.pvc_uid AND p.state IN ('Preparing','ContentReady','NeedsRecovery')))""",
    'idle-capture-missing-receipt': "SELECT count(*) FROM ssf_idle_captures WHERE state='Ready' AND (receipt IS NULL OR receipt='null'::jsonb)",
    'reclaim-idle-capture-reference': """SELECT count(*) FROM ssf_backend_tombstones t JOIN ssf_idle_captures c
      ON c.volume_key=t.volume_key WHERE c.state<>'Released'""",
    'reclaim-live-reference': """SELECT count(*) FROM ssf_checkpoint_resources r
      JOIN ssf_checkpoint_pins p ON p.resource_uid=r.uid
      JOIN ssf_checkpoint_sets c ON c.id=p.checkpoint_id WHERE r.state='Deleting' AND c.state<>'Released'""",
}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--psql', default='psql')
    p.add_argument('--database-env', default='SSF_DATABASE_URL')
    p.add_argument('--output', required=True)
    a = p.parse_args()
    if not os.environ.get(a.database_env): p.error('database environment variable is empty')
    sql = 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n'
    sql += "SELECT json_build_object(" + ','.join("'" + name + "',(" + query + ')' for name, query in QUERIES.items()) + ");\nROLLBACK;\n"
    address = urlsplit(os.environ[a.database_env])
    if address.scheme not in {'postgresql', 'postgres'} or not address.path.strip('/'):
        p.error('database variable must be a PostgreSQL URL')
    env = {k:v for k,v in os.environ.items() if not k.startswith('PG')}
    env.update(PGDATABASE=unquote(address.path.lstrip('/')), PGCONNECT_TIMEOUT='15')
    for key,value in [('PGHOST', address.hostname), ('PGPORT', address.port), ('PGUSER', unquote(address.username or '')), ('PGPASSWORD', unquote(address.password or ''))]:
        if value: env[key] = str(value)
    options = {'host':'PGHOST','port':'PGPORT','user':'PGUSER','password':'PGPASSWORD',
               'sslmode':'PGSSLMODE','sslrootcert':'PGSSLROOTCERT','sslcert':'PGSSLCERT','sslkey':'PGSSLKEY',
               'application_name':'PGAPPNAME','connect_timeout':'PGCONNECT_TIMEOUT','options':'PGOPTIONS'}
    for key, values in parse_qs(address.query).items():
        if key not in options or len(values) != 1: p.error('unsupported or repeated database URL option')
        env[options[key]] = values[0]
    report = {'scope': 'read-only-ledger-invariants', 'time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'vmm_guest_and_devices_verified': False, 'checks': [], 'status': 'failed'}
    try:
        result = subprocess.run([a.psql, '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1'], input=sql,
            text=True, capture_output=True, env=env, timeout=30)
        if result.returncode: raise RuntimeError('database query failed; credentials and server output suppressed')
        values = json.loads(result.stdout.strip())
        if set(values) != set(QUERIES): raise ValueError('incomplete database assertion result')
        report['checks'] = [{'name': name, 'violations': count, 'status': 'passed' if count == 0 else 'failed'}
                            for name, count in values.items()]
        report['status'] = 'passed' if all(x['status'] == 'passed' for x in report['checks']) else 'failed'
    except Exception as error:
        report['error'] = type(error).__name__
    Path(a.output).write_text(json.dumps(report, indent=2) + '\n')
    print(a.output)
    return int(report['status'] != 'passed')

if __name__ == '__main__': raise SystemExit(main())
