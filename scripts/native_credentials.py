#!/usr/bin/env python3
"""Generate local native-plugin credentials and Kubernetes Secret JSON.
Never applies resources; refuses to overwrite an existing output directory.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import secrets
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--out', required=True)
parser.add_argument('--namespace', default='agentenv-system')
parser.add_argument('--native-api-key-file', help='reuse the existing native API key; otherwise generate one')
parser.add_argument('--database-url-file', help='optional file whose contents become the ledger Secret; never printed')
args = parser.parse_args()
if len(args.namespace) > 63 or not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', args.namespace):
    parser.error('invalid Kubernetes namespace')
root = Path(__file__).resolve().parents[1]
out = Path(args.out).resolve()
out.mkdir(mode=0o700, parents=True, exist_ok=False)
os.umask(0o077)

def run(arguments, cwd=out):
    with (out / 'generation.log').open('ab') as log:
        subprocess.run(arguments, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, check=True)

ca = out / 'ca'
ca.mkdir(mode=0o700)
(ca / 'openssl.cnf').write_text('[req]\ndistinguished_name=dn\nx509_extensions=extensions\nprompt=no\n[dn]\nCN=SSF native local CA\n[extensions]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n')
run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-sha256', '-days', '365',
     '-config', str(ca / 'openssl.cnf'), '-keyout', str(ca / 'ca.key'), '-out', str(ca / 'ca.crt')])
objects = []

def secret(name, content):
    directory = out / (name + '-files')
    directory.mkdir(mode=0o700)
    for key, value in content.items():
        (directory / key).write_bytes(value)
        (directory / key).chmod(0o600)
    objects.append({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': name, 'namespace': args.namespace},
                    'type': 'Opaque', 'data': {key: base64.b64encode(value).decode() for key, value in content.items()}})

for name, identity, dns in [
    ('agentenv-storage-control-tls', 'scheduler', 'agentenv-scheduler.' + args.namespace + '.svc'),
    ('agentenv-storage-coordinator-tls', 'coordinator', 'ssf-coordinator.' + args.namespace + '.svc'),
    ('agentenv-storage-node-tls', 'node', 'agentenv-storage-node.' + args.namespace + '.svc'),
    ('agentenv-ssf-node-tls', 'ssf-node', 'agentenv-ssf-node.' + args.namespace + '.svc'),
]:
    leaf = out / name
    leaf.mkdir(mode=0o700)
    (leaf / 'extensions.cnf').write_text('basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:' + dns + ',DNS:localhost,IP:127.0.0.1,URI:spiffe://ssf/agentenv/' + identity + '\n')
    run(['openssl', 'req', '-new', '-newkey', 'rsa:2048', '-nodes', '-sha256', '-subj', '/CN=' + dns,
         '-keyout', str(leaf / 'tls.key'), '-out', str(leaf / 'request.csr')])
    run(['openssl', 'x509', '-req', '-sha256', '-days', '90', '-in', str(leaf / 'request.csr'),
         '-CA', str(ca / 'ca.crt'), '-CAkey', str(ca / 'ca.key'), '-set_serial', '0x' + secrets.token_hex(16),
         '-extfile', str(leaf / 'extensions.cnf'), '-out', str(leaf / 'tls.crt')])
    run(['openssl', 'verify', '-CAfile', str(ca / 'ca.crt'), str(leaf / 'tls.crt')])
    secret(name, {'tls.crt': (leaf / 'tls.crt').read_bytes(), 'tls.key': (leaf / 'tls.key').read_bytes(), 'ca.crt': (ca / 'ca.crt').read_bytes()})
run(['go', 'run', './cmd/ssfctl', '-action', 'keygen', '-key', str(out / 'plan')], cwd=root)
gateway, dispatch, node, hook = [secrets.token_hex(32).encode() for _ in range(4)]
secret('agentenv-storage-gateway', {'gateway-key': gateway})
secret('agentenv-storage-control', {'gateway-key': gateway, 'dispatch-key': dispatch, 'node-key': node, 'plan.key': (out / 'plan.key').read_bytes()})
secret('agentenv-storage-node', {'dispatch-key': dispatch, 'node-key': node, 'hook-token': hook, 'plan.pub': (out / 'plan.pub').read_bytes()})
if args.database_url_file:
    secret('agentenv-storage-ledger', {'url': Path(args.database_url_file).read_bytes().strip()})
api_key = Path(args.native_api_key_file).read_bytes().strip() if args.native_api_key_file else ('e2b_' + secrets.token_hex(32)).encode()
(out / 'AENV_API_KEY').write_bytes(api_key)
(out / 'AENV_API_KEY').chmod(0o600)
manifest = out / 'secrets.json'
manifest.write_text(json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': objects}, indent=2) + '\n')
manifest.chmod(0o600)
print(manifest)
