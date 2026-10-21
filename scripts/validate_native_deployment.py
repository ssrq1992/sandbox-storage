#!/usr/bin/env python3
"""Verify native overlay rendering and generated credentials without a cluster."""
import argparse
import base64
import datetime
import json
from pathlib import Path
import subprocess
import tempfile

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--kustomize',required=True)
parser.add_argument('--output',default='validation/native-deployment')
args=parser.parse_args()
root=Path(__file__).resolve().parents[1]
out=(root/args.output).resolve()
out.mkdir(parents=True,exist_ok=True)
report={'scope':'offline-render-and-credentials-only','time':datetime.datetime.now(datetime.timezone.utc).isoformat(),'cluster_deployed':False,'checks':[]}
with tempfile.TemporaryDirectory(prefix='ssf-deployment-validation-') as temporary:
    for namespace in ('agentenv-system','ssf-native-validation'):
        credentials=Path(temporary)/namespace
        rendered=Path(temporary)/(namespace+'.yaml')
        commands=[['python3','scripts/native_credentials.py','--out',str(credentials),'--namespace',namespace],
                  ['python3','scripts/render_agentenv_native.py','--kustomize',args.kustomize,'--namespace',namespace,
                   '--api-key-file',str(credentials/'AENV_API_KEY'),'--driver','fixture.csi.ssf.dev',
                   '--controller-endpoint','fixture-controller.ssf-system.svc:9443','--output',str(rendered)],
                  ['go','run','./tools/check-native-deploy','-file',str(rendered),'-namespace',namespace]]
        try:
            with (out/(namespace+'.log')).open('w') as log:
                for command in commands:
                    subprocess.run(command,cwd=root,stdout=log,stderr=subprocess.STDOUT,check=True)
            objects=json.loads((credentials/'secrets.json').read_text())['items']
            by_name={obj['metadata']['name']:obj for obj in objects}
            def value(name,key):
                return base64.b64decode(by_name[name]['data'][key],validate=True)
            gateway=value('agentenv-storage-gateway','gateway-key')
            dispatch=value('agentenv-storage-control','dispatch-key')
            node=value('agentenv-storage-control','node-key')
            hook=value('agentenv-storage-node','hook-token')
            assert len({gateway,dispatch,node,hook})==4 and min(map(len,(gateway,dispatch,node,hook)))>=32
            assert value('agentenv-storage-control','gateway-key')==gateway
            assert value('agentenv-storage-node','dispatch-key')==dispatch
            assert value('agentenv-storage-node','node-key')==node
            assert len(base64.b64decode(value('agentenv-storage-control','plan.key'),validate=True))==64
            assert len(base64.b64decode(value('agentenv-storage-node','plan.pub'),validate=True))==32
            assert rendered.stat().st_mode & 0o777==0o600
            assert (credentials/'secrets.json').stat().st_mode & 0o777==0o600
            assert all(obj['metadata']['namespace']==namespace for obj in objects)
            report['checks'].append({'name':namespace,'status':'passed'})
        except (AssertionError,KeyError,ValueError,OSError,subprocess.CalledProcessError) as error:
            report['checks'].append({'name':namespace,'status':'failed','reason':type(error).__name__})
report['status']='failed' if any(check['status']=='failed' for check in report['checks']) else 'passed'
(out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
print(out/'report.json')
raise SystemExit(report['status']=='failed')
