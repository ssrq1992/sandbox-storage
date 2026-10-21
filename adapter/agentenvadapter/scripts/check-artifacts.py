#!/usr/bin/env python3
"""Validate current documentation links and architecture boundaries."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote
root=Path(__file__).resolve().parents[4]
adapter=root/'SSF/adapter/agentenvadapter'
errors=[]
for file in [* (root/'design').glob('*.md'), *adapter.rglob('*.md')]:
    for link in re.findall(r'\]\(([^)]+)\)',file.read_text()):
        link=unquote(link.split('#')[0].split(' "')[0])
        if not link or re.match(r'\w+://',link): continue
        if not (file.parent/link).exists(): errors.append(f'{file.relative_to(root)}: missing {link}')
for file in (root/'SSF').rglob('*.go'):
    if 'adapter' in file.relative_to(root/'SSF').parts or 'cmd' in file.relative_to(root/'SSF').parts or 'tools' in file.relative_to(root/'SSF').parts: continue
    if 'ssf.dev/ssf/adapter/' in file.read_text(): errors.append('core imports adapter: '+str(file))
expected='9733935a2c0e2d144b67c8404361c835e7f8ed45d2b508992cb230da31b3d5dc'
if hashlib.sha256((root/'design/assets/K8S-CSI架构分析.md').read_bytes()).hexdigest()!=expected: errors.append('protected CSI document changed')
for file in adapter.rglob('*.json'): json.loads(file.read_text())
for file in (root/'SSF/scripts').glob('*.sh'): subprocess.run(['bash','-n',str(file)],check=True)
report={'status':'failed' if errors else 'passed','errors':errors,'checks':['relative Markdown links','core dependency boundary','protected CSI document SHA256','JSON parsing','bash syntax']}
out=root/'SSF/validation/artifacts';out.mkdir(parents=True,exist_ok=True)
(out/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(report,ensure_ascii=False,indent=2))
raise SystemExit(bool(errors))
