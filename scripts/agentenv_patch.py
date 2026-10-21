#!/usr/bin/env python3
"""Package current native changes and verify exact replay without git add/commit.
The patch is a source artifact, not a declaration of complete integration.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--agentenv', default=str(Path(__file__).resolve().parents[2] / 'AgentENV'))
parser.add_argument('--baseline', default='6cccaa7842bd5be2051111d4f74d9e37aa721244')
parser.add_argument('--output', required=True)
args = parser.parse_args()
root = Path(args.agentenv).resolve()
output = Path(args.output).resolve()

def git(*arguments, accepted=(0,), cwd=root):
    result = subprocess.run(['git', *arguments], cwd=cwd, capture_output=True)
    if result.returncode not in accepted:
        raise RuntimeError(result.stderr.decode(errors='replace'))
    return result.stdout

tracked = git('diff', '--name-only', '-z', args.baseline, '--').decode().split('\0')
untracked = git('ls-files', '--others', '--exclude-standard', '-z').decode().split('\0')
paths = sorted({name for name in tracked + untracked if name})
for name in paths:
    path = Path(name)
    if path.is_absolute() or '..' in path.parts or (root / path).is_symlink():
        raise RuntimeError('unsafe or unsupported patch path: ' + name)
patch = git('diff', '--binary', '--no-ext-diff', args.baseline, '--')
for name in untracked:
    if name:
        patch += git('diff', '--no-index', '--binary', '--no-ext-diff', '--', '/dev/null', name, accepted=(0, 1))
output.parent.mkdir(parents=True, exist_ok=True)
output.write_bytes(patch)
with tempfile.TemporaryDirectory(prefix='ssf-patch-replay-') as temporary:
    replay = Path(temporary)
    for name in tracked:
        if not name:
            continue
        result = subprocess.run(['git', 'show', args.baseline + ':' + name], cwd=root, capture_output=True)
        if result.returncode == 0:
            path = replay / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(result.stdout)
            mode = git('ls-tree', args.baseline, '--', name).split()[0]
            path.chmod(0o755 if mode == b'100755' else 0o644)
    git('apply', '--check', str(output), cwd=replay)
    git('apply', str(output), cwd=replay)
    hashes = {}
    for name in paths:
        current, restored = root / name, replay / name
        if current.exists() != restored.exists():
            raise RuntimeError('replay existence mismatch: ' + name)
        if current.exists():
            if current.read_bytes() != restored.read_bytes() or (current.stat().st_mode & 0o111) != (restored.stat().st_mode & 0o111):
                raise RuntimeError('replay content/mode mismatch: ' + name)
            hashes[name] = hashlib.sha256(current.read_bytes()).hexdigest()
report = {'baseline': args.baseline, 'patch_sha256': hashlib.sha256(patch).hexdigest(),
          'replay': 'passed', 'files': hashes, 'complete_plan': False}
output.with_suffix(output.suffix + '.json').write_text(json.dumps(report, indent=2) + '\n')
print(str(output))
print('exact replay passed for ' + str(len(paths)) + ' changed paths')
