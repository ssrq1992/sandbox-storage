#!/usr/bin/env python3
"""Run portable native safety modules in isolation; never substitutes for AgentENV typecheck."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--cargo', default='cargo')
parser.add_argument('--output', default='validation/native-rust-modules')
parser.add_argument('--offline', action='store_true')
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
source = root.parent / 'AgentENV'
out = Path(args.output)
if not out.is_absolute():
    out = root / out
out.mkdir(parents=True, exist_ok=True)
modules = {name: root / 'adapter/agentenvadapter/runtime/src' / (name + '.rs') for name in ('freeze', 'artifacts', 'transition')}
report = {'scope': 'isolated-portable-rust-safety-modules',
          'time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'agentenv_typechecked': False, 'kvm_executed': False,
          'source_sha256': {str(path.relative_to(root.parent)): hashlib.sha256(path.read_bytes()).hexdigest() for path in modules.values()}}
with tempfile.TemporaryDirectory(prefix='ssf-native-rust-') as tmp:
    project = Path(tmp)
    (project / 'src').mkdir()
    (project / 'Cargo.toml').write_text('''[package]
name = "ssf-native-portable-checks"
version = "0.1.0"
edition = "2021"
[dependencies]
anyhow = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tempfile = "3"
''')
    # Seed the exact native dependency versions, including locked/yanked versions.
    # Cargo prunes unused entries only in this disposable project.
    shutil.copyfile(source / 'Cargo.lock', project / 'Cargo.lock')
    (project / 'src/lib.rs').write_text(''.join('#[path=' + json.dumps(str(path)) + ']\npub mod ' + name + ';\n' for name, path in modules.items()))
    command = [args.cargo, 'test', '--manifest-path', str(project / 'Cargo.toml')]
    if args.offline:
        command.append('--offline')
    with (out / 'tests.log').open('w') as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, env=os.environ.copy())
    report.update(status='passed' if result.returncode == 0 else 'failed', exit_code=result.returncode)
(out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
print(out / 'report.json')
raise SystemExit(result.returncode)
