#!/usr/bin/env python3
"""Render the native incremental Kustomize overlay; never contact a cluster."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--kustomize', default='kustomize')
parser.add_argument('--namespace', default='agentenv-system')
parser.add_argument('--api-key-file', required=True)
parser.add_argument('--driver', required=True)
parser.add_argument('--controller-endpoint', required=True)
parser.add_argument('--revision', default='1')
parser.add_argument('--agent-image', default='agentenv-runtime:latest')
parser.add_argument('--scheduler-image', default='agentenv-scheduler:latest')
parser.add_argument('--gateway-image', default='agentenv-gateway:latest')
parser.add_argument('--adapter-image', default='agentenvadapter:latest')
parser.add_argument('--ssf-image', default='ssf:latest')
parser.add_argument('--output', required=True)
parser.add_argument('--overlaybd-inventory', help='explicit native OverlayBD worker inventory JSON')
parser.add_argument('--overlaybd-image', default='ssf-overlaybd:latest')
args = parser.parse_args()
if len(args.namespace)>63 or not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?',args.namespace):
    parser.error('invalid namespace')
key = Path(args.api_key_file).read_text().strip()
if not re.fullmatch(r'[a-zA-Z0-9_.-]{32,256}',key):
    parser.error('native API key must be a 32-256 character token')
root = Path(__file__).resolve().parents[2] / 'AgentENV'
output = Path(args.output).resolve()
with tempfile.TemporaryDirectory(prefix='ssf-native-render-') as temporary:
    base = Path(temporary) / 'base'
    overlay = Path(temporary) / 'overlays' / 'ssf'
    shutil.copytree(root / 'deploy/k8s/base',base)
    shutil.copytree(Path(__file__).resolve().parents[1] / 'adapter/agentenvadapter/deploy/k8s',overlay)
    kp=overlay/'kustomization.yaml'
    kp.write_text(kp.read_text().replace('../../../../../AgentENV/deploy/k8s/base','../../base'))
    shutil.copyfile(root / 'config/default.toml',base / 'config/agentenv.toml')
    original = (base / 'kustomization.yaml').read_text()
    if original.count('      - AENV_API_KEY=')!=1:
        raise RuntimeError('native auth generator changed; refusing ambiguous replacement')
    (base / 'kustomization.yaml').write_text(original.replace('      - AENV_API_KEY=', '      - AENV_API_KEY='+key))
    scheduler_path=base / 'config/scheduler.json'
    scheduler=json.loads(scheduler_path.read_text())
    scheduler['scheduler']['discovery']['kubernetes']['namespace']=args.namespace
    scheduler_path.write_text(json.dumps(scheduler,indent=2)+'\n')
    control_path=overlay / 'config/control.json'
    control=json.loads(control_path.read_text())
    control['namespace']=args.namespace
    control['principal']['namespaces']=[args.namespace]
    control['plugin_server_name']='agentenv-storage-node.'+args.namespace+'.svc'
    control['preparation_server_name']='agentenv-ssf-node.'+args.namespace+'.svc'
    control['drivers']=[{'Name':args.driver,'ControllerEndpoint':args.controller_endpoint,'Revision':args.revision}]
    control_path.write_text(json.dumps(control,indent=2)+'\n')
    for name in ('coordinator','scheduler','node'):
        path=overlay/'config'/f'{name}.json'
        path.write_text(path.read_text().replace('.agentenv-system.svc','.'+args.namespace+'.svc'))
    node_path=overlay / 'config/ssf-node.json'
    node=json.loads(node_path.read_text())
    node['Drivers']=[{'Name':args.driver,'NodeEndpoint':'unix:///var/lib/kubelet/plugins/'+args.driver+'/csi.sock','Revision':args.revision}]
    node_path.write_text(json.dumps(node,indent=2)+'\n')
    if args.overlaybd_inventory:
        from overlaybd_node_overlay import install, DRIVER
        if args.driver != DRIVER:
            parser.error('OverlayBD inventory requires its dedicated CSI driver name')
        install(overlay, json.loads(Path(args.overlaybd_inventory).read_text()), args.namespace, args.overlaybd_image)
    subprocess.run([args.kustomize,'edit','set','namespace',args.namespace],cwd=overlay,check=True,capture_output=True)
    subprocess.run([args.kustomize,'edit','set','image',
                    'agentenv-runtime='+args.agent_image,'agentenv-scheduler='+args.scheduler_image,
                    'agentenv-gateway='+args.gateway_image,'agentenvadapter='+args.adapter_image,'ssf='+args.ssf_image],
                   cwd=overlay,check=True,capture_output=True)
    result=subprocess.run([args.kustomize,'build',str(overlay)],capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr.decode(errors='replace'))
    rendered=result.stdout
    output.parent.mkdir(parents=True,exist_ok=True)
    # Rendered manifests contain the native auth Secret. Do not print them.
    descriptor=os.open(output,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(descriptor,'wb') as stream:
        stream.write(rendered)
    output.chmod(0o600)
print(output)
