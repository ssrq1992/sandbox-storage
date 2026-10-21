#!/usr/bin/env python3
"""Clean up only known native IDs from native_lifecycle_e2e.py evidence.

Without --execute print a concrete deletion plan. Volumes are retained unless
--volumes is specified. Unknown create outcomes require manual reconciliation.
"""
import argparse
import json
import os
from pathlib import Path
import re
import ssl
import urllib.error
import urllib.request

from native_lifecycle_e2e import NoRedirect


def plan(report, templates=False, volumes=False):
    if report.get('version') != 1 or report.get('scope') not in {'native-api-and-guest-lifecycle','kubernetes-pvc-native-api'}:
        raise ValueError('not a native lifecycle report')
    operations = [('sandbox', item) for item in dict.fromkeys(report['sandboxes'])]
    if templates:
        operations += [('template', item) for item in dict.fromkeys(report['snapshots'])]
    if volumes:
        operations += [('volume', item['name']) for item in report['volumes'] if not item.get('imported')]
    for kind, identity in operations:
        if not isinstance(identity, str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,128}', identity):
            raise ValueError('invalid recorded native ID')
        if kind == 'volume' and not identity.startswith(report['run'] + '-'):
            raise ValueError('volume is not owned by this test run')
    return [{'kind': kind, 'method': 'DELETE', 'path': '/' + {'sandbox': 'sandboxes', 'template': 'templates', 'volume': 'volumes'}[kind] + '/' + identity}
            for kind, identity in operations]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--api-key-env', default='AENV_API_KEY')
    parser.add_argument('--ca')
    parser.add_argument('--templates', action='store_true')
    parser.add_argument('--volumes', action='store_true')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    report = json.loads(Path(args.report).read_text())
    operations = plan(report, args.templates, args.volumes)
    if not args.execute:
        print(json.dumps({'operations': operations, 'unknown_requests': [
            item for item in report['requests'] if item['state'] == 'pending']}, indent=2))
        return 0
    key = os.environ[args.api_key_env]
    opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(
        context=ssl.create_default_context(cafile=args.ca)))
    failed = False
    evidence = Path(args.report).with_name('cleanup.jsonl')
    with evidence.open('a') as output:
        os.chmod(evidence, 0o600)
        for operation in operations:
            output.write(json.dumps({**operation, 'state': 'pending'}) + '\n')
            output.flush()
            os.fsync(output.fileno())
            try:
                request = urllib.request.Request(args.url.rstrip('/') + operation['path'],
                    method='DELETE', headers={'X-API-Key': key})
                with opener.open(request, timeout=120) as response:
                    if response.status != 204:
                        raise RuntimeError('unexpected HTTP status ' + str(response.status))
                result = 'api-deleted'  # not a CSI detach receipt
            except urllib.error.HTTPError as error:
                result = 'already-absent' if error.code == 404 else 'failed-http-' + str(error.code)
                failed |= error.code != 404
            except Exception as error:
                result = 'unknown-' + type(error).__name__
                failed = True
            output.write(json.dumps({**operation, 'state': result}) + '\n')
            output.flush()
            os.fsync(output.fileno())
            if failed:
                break  # preserve dependent templates/volumes after uncertain stop
    return int(failed)


if __name__ == '__main__':
    raise SystemExit(main())
