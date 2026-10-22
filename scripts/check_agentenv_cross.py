#!/usr/bin/env python3
"""Type-check AgentENV Linux targets using an explicitly installed Zig toolchain.
This does not link or run AgentENV, native Rust tests, KVM, Kubernetes or CSI.
No toolchain is downloaded and no global configuration is changed.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cargo', required=True, type=Path)
    parser.add_argument('--zig', required=True, type=Path)
    parser.add_argument('--protoc', required=True, type=Path)
    parser.add_argument('--libclang', required=True, type=Path)
    parser.add_argument('--output', default='validation/native-rust-cross/current', type=Path)
    parser.add_argument('--jobs', default=4, type=int)
    parser.add_argument('--lib-only', action='store_true')
    parser.add_argument('--offline', action='store_true')
    target_group=parser.add_mutually_exclusive_group()
    target_group.add_argument('--ublk-daemon',action='store_true',help='Type-check native daemon including its test targets; does not execute Linux tests')
    target_group.add_argument('--overlaybd-host', action='store_true',
                        help='Check the native OverlayBD host including its volume engine')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    native = root.parent / 'AgentENV'
    out = args.output if args.output.is_absolute() else root / args.output
    out.mkdir(parents=True, exist_ok=True)
    # Do not let overlapping checks truncate each other's log/report.
    output_lock = (out / '.run.lock').open('a')
    try:
        fcntl.flock(output_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f'check already running in {out}', file=sys.stderr)
        return 2
    report = dict(scope='linux-target-rust-typecheck', host=platform.platform(),
                  target='x86_64-unknown-linux-gnu', native_tests_executed=False,
                  kvm_executed=False, csi_executed=False,
                  started=datetime.datetime.now(datetime.timezone.utc).isoformat())

    def hashes():
        result = {}
        for source in (native, root / 'adapter/agentenvadapter/runtime', root / 'drivers/overlaybd/native'):
            for directory, dirs, files in os.walk(source):
                dirs[:] = sorted(d for d in dirs if d not in {'.git', 'target', 'node_modules', '.venv'})
                for name in sorted(files):
                    path = Path(directory) / name
                    if path.suffix == '.rs' or name in {'Cargo.toml', 'Cargo.lock'}:
                        result[str(path.relative_to(root.parent))] = hashlib.sha256(path.read_bytes()).hexdigest()
        return result

    try:
        if args.jobs < 1:
            raise ValueError('--jobs must be positive')
        for path in [args.cargo, args.zig, args.protoc]:
            if not path.is_file() or not os.access(path, os.X_OK):
                raise ValueError(f'executable required: {path}')
        if not args.libclang.is_dir():
            raise ValueError('--libclang must identify the directory containing libclang')
        zig = args.zig.resolve()
        includes = [zig.parent / 'lib' / path for path in [
            'include', 'libc/include/x86-linux-gnu', 'libc/include/generic-glibc',
            'libc/include/x86-linux-any', 'libc/include/any-linux-any']]
        if any(not path.is_dir() for path in includes):
            raise ValueError('Zig distribution must include Linux libc headers')
        wrappers = out.resolve() / 'toolchain'
        wrappers.mkdir(exist_ok=True)
        for name, mode in [('cc', 'cc'), ('cxx', 'c++'), ('ar', 'ar')]:
            # All paths are Python literals and subprocess argument arrays.
            wrapper = wrappers / name
            wrapper.write_text(f'#!{sys.executable}\nimport os,sys\n'
                + 'args=[arg for arg in sys.argv[1:] if not arg.startswith("--target=")]\n'
                + f'os.execv({str(zig)!r}, [{str(zig)!r}, {mode!r}] + '
                + ('args)\n' if mode == 'ar' else '["-target","x86_64-linux-gnu"] + args)\n'))
            wrapper.chmod(0o755)
        env = os.environ.copy()
        env.update(CC_x86_64_unknown_linux_gnu=str(wrappers / 'cc'),
                   CXX_x86_64_unknown_linux_gnu=str(wrappers / 'cxx'),
                   AR_x86_64_unknown_linux_gnu=str(wrappers / 'ar'),
                   CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=str(wrappers / 'cc'),
                   PKG_CONFIG_ALLOW_CROSS='1', LIBCLANG_PATH=str(args.libclang.resolve()),
                   PROTOC=str(args.protoc.resolve()))
        env['PATH'] = str(args.cargo.absolute().parent) + os.pathsep + env.get('PATH', '')
        env['BINDGEN_EXTRA_CLANG_ARGS_x86_64_unknown_linux_gnu'] = shlex.join(
            ['--target=x86_64-unknown-linux-gnu', '-nostdinc']
            + [value for path in includes for value in ['-isystem', str(path)]])
        command = [str(args.cargo.absolute()), 'check', '--locked', '-p', 'agentenv',
                   '--lib' if args.lib_only else '--all-targets', '--target', report['target'],
                   '--features', 'vendored-openssl', '-j', str(args.jobs)]
        if args.overlaybd_host:
            env['CARGO_TARGET_DIR'] = str(native / 'target')
            command = [str(args.cargo.absolute()), 'check', '--locked', '--manifest-path',
                       str(root / 'drivers/overlaybd/native/Cargo.toml'),
                       '--lib' if args.lib_only else '--all-targets', '--target', report['target'],
                       '--features', 'device-host', '-j', str(args.jobs)]
            report['scope'] = 'linux-target-overlaybd-host-typecheck'
        if args.ublk_daemon:
            command = [str(args.cargo.absolute()), 'check', '--locked', '-p', 'uvm-ublk-daemon',
                       '--lib' if args.lib_only else '--all-targets', '--target', report['target'],
                       '-j',str(args.jobs)]
            report['scope']='linux-target-ublk-daemon-typecheck'
        if args.offline:
            command.append('--offline')
        report['command'] = command
        report['tool_versions'] = {}
        for name, command_version in [('cargo', [str(args.cargo.absolute()), '--version']),
                                      ('zig', [str(zig), 'version']),
                                      ('protoc', [str(args.protoc.absolute()), '--version'])]:
            report['tool_versions'][name] = subprocess.check_output(command_version, env=env, text=True).strip()
        before = hashes()
        with (out / 'check.log').open('w') as log:
            result = subprocess.run(command, cwd=native, env=env, stdout=log, stderr=subprocess.STDOUT)
        report.update(exit_code=result.returncode, source_stable=before == hashes(), source_sha256=before)
        report['status'] = 'passed' if result.returncode == 0 and report['source_stable'] else 'failed'
        code = 0 if report['status'] == 'passed' else 1
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        report.update(status='not_executed', reason=str(error))
        code = 2
    report['finished'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(out / 'report.json')
    return code


if __name__ == '__main__':
    raise SystemExit(main())
