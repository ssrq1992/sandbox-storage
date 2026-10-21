#!/usr/bin/env python3
"""Reproduce native OpenAPI Rust bindings; no downloads or source mutation."""
import argparse
from pathlib import Path
import re
import subprocess
import tempfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--java', default='java')
p.add_argument('--jar', required=True, help='local OpenAPI Generator 7.22.0 jar')
p.add_argument('--rustfmt', default='rustfmt')
a = p.parse_args()
root = Path(__file__).resolve().parents[2] / 'AgentENV'
version = subprocess.check_output([a.java, '-jar', a.jar, 'version'], text=True).strip()
if version != '7.22.0':
    p.error('expected generator 7.22.0, got ' + version)
with tempfile.TemporaryDirectory(prefix='ssf-openapi-check-') as directory:
    temp = Path(directory)
    subprocess.run([a.java, '-jar', a.jar, 'generate', '-g', 'rust-axum', '-i', str(root / 'src/api/openapi.yml'), '-o', directory,
                    '--additional-properties=packageName=agentenv_http_server,hideGenerationTimestamp=true'], check=True, stdout=subprocess.DEVNULL)
    api = temp / 'src/apis/mod.rs'
    text = api.read_text()
    pattern = re.compile(r'(?s)/// API Key Authentication - Header\.\r?\n\s*#\[async_trait::async_trait\]\r?\n\s*pub trait ApiKeyAuthHeader \{.*?\n\}\r?\n\r?\n')
    matches = list(pattern.finditer(text))
    if not matches:
        raise SystemExit('expected native duplicate-auth postprocessing input')
    api.write_text(text[:matches[0].end()] + pattern.sub('', text[matches[0].end():]))
    generated = sorted((temp / 'src').rglob('*.rs'))
    for file in generated:
        subprocess.run([a.rustfmt, '--edition', '2024', '--style-edition', '2024', str(file)], check=True)
    stale = []
    for file in generated:
        actual = root / 'src/api/generated' / file.relative_to(temp)
        if not actual.exists() or file.read_bytes() != actual.read_bytes():
            stale.append(str(actual.relative_to(root)))
    if stale:
        raise SystemExit('stale generated files: ' + ', '.join(stale))
    print(f'Native OpenAPI generation reproducible: {len(generated)} Rust files')
