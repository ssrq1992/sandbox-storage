#!/usr/bin/env python3
"""Compare the preserved native patch and current native worktree against upstream."""
import argparse
from pathlib import Path
import subprocess
import json

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--baseline',required=True,type=Path)
p.add_argument('--output',required=True,type=Path)
a=p.parse_args()
root=Path(__file__).resolve().parents[2]/'AgentENV'
def git(*args):return subprocess.check_output(['git',*args],cwd=root,text=True)
upstream=set(git('ls-tree','-r','--name-only','HEAD').splitlines())
def parse(raw):
    out={}
    for line in raw.splitlines():
        added,removed,name=line.split('\t',2)
        if added!='-':out[name]=(int(added),int(removed))
    return out
def classify(name):
    if name.endswith('.pb.go') or '/generated/' in name:return 'generated'
    if name.endswith('_test.go') or '/tests/' in name or name.startswith('tests/') or name=='src/orchestrator/tests.rs':return 'separate_tests'
    if name.startswith('storage/volume-core/'):return 'shared_native_library'
    if name.startswith('deploy/'):return 'deployment'
    if Path(name).name in {'Cargo.toml','Cargo.lock','go.mod','go.sum','go.ssf.mod','go.ssf.sum','.gitignore'}:return 'dependencies_config'
    if name.startswith('crates/aenv/'):return 'sdk_cli'
    if name not in upstream:return 'added_integration_modules'
    return 'existing_core_including_inline_tests'
def summarize(files):
    groups={}
    for name,(added,removed) in files.items():
        row=groups.setdefault(classify(name),{'files':0,'added':0,'removed':0})
        row['files']+=1;row['added']+=added;row['removed']+=removed
    return dict(groups=groups,total={'files':len(files),'added':sum(v[0] for v in files.values()),'removed':sum(v[1] for v in files.values())},files=files)
before=parse(git('apply','--numstat',str(a.baseline.resolve())))
after=parse(git('diff','--numstat','HEAD'))
for name in git('ls-files','--others','--exclude-standard').splitlines():
    path=root/name
    if path.is_file():after[name]=(len(path.read_bytes().splitlines()),0)
report={'baseline_patch':str(a.baseline.resolve()),'upstream':git('rev-parse','HEAD').strip(),'before':summarize(before),'after':summarize(after),'note':'Counts are additions/deletions against upstream, not removals of native features. Inline tests remain in their production source category; added integration modules counted separately. Existing user deployment fixes retained.'}
a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({stage:report[stage]['total'] for stage in ('before','after')}))
