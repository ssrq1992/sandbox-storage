#!/usr/bin/env python3
"""Verify actual imports and ownership of the independent coordinator build."""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
native = root.parent / 'AgentENV'
def deps(directory, *packages):
    return subprocess.check_output(['go', 'list', '-deps', *packages], cwd=directory, text=True).splitlines()
scheduler = deps(native / 'services', './scheduler/cmd')
for dependency in scheduler:
    assert not dependency.startswith(('ssf.dev/ssf/', 'github.com/jackc/', 'github.com/container-storage-interface/')), dependency
client = deps(root / 'sdk/go/control', '.')
assert not any(d.startswith(('k8s.io/', 'ssf.dev/ssf/', 'google.golang.org/grpc', 'github.com/')) for d in client)
for directory in ('consume', 'ledger', 'preparation', 'coordinator'):
    assert not any('agentenvadapter' in d or 'agentenv/services' in d for d in deps(root, './' + directory))
assert not (native / 'services/go.ssf.mod').exists()
assert not (native / 'services/scheduler/cmd/storage_ssf.go').exists()
assert not (native / 'deploy/k8s/overlays/ssf').exists()
assert 'OpenNativeControl(' not in (root / 'adapter/agentenvadapter/server.go').read_text()
assert 'func (n *Node) openStandalone' not in (root / 'adapter/agentenvadapter/native_standalone.go').read_text()
for path in (root / 'adapter/agentenvadapter/runtime/src').glob('*.rs'):
    assert 'crate::sandbox::' not in path.read_text() and 'crate::cfg::' not in path.read_text(), path
assert '.send()' not in (native / 'src/sandbox/storage/mod.rs').read_text()
print('dependency boundary passed: native strategy/host + independent common coordinator/ledger + adapter')
