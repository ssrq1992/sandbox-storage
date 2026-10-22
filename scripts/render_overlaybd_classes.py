#!/usr/bin/env python3
"""Render dedicated native OverlayBD classes; never modify existing classes."""
import argparse
import hashlib
import json
import re
from pathlib import Path
from overlaybd_node_overlay import build_inventory, DRIVER, PROFILE


def render(inventory, default_snapshot=False):
    _, repositories = build_inventory(inventory)
    items=[]
    for repository in repositories:
        identity=repository['id']
        component=re.sub('[^a-z0-9-]','-',identity.lower()).strip('-')[:35] or 'repo'
        prefix='overlaybd-'+component+'-'+hashlib.sha256(identity.encode()).hexdigest()[:8]
        for mode,suffix in [('Immediate','immediate'),('WaitForFirstConsumer','wffc')]:
            item={'apiVersion':'storage.k8s.io/v1','kind':'StorageClass',
                  'metadata':{'name':prefix+'-'+suffix,'annotations':{'ssf.dev/volume-profile':PROFILE['name']}},
                  'provisioner':DRIVER,'parameters':{'repository':identity},
                  'reclaimPolicy':'Retain','volumeBindingMode':mode,'allowVolumeExpansion':bool(inventory.get('offline_expansion',False))}
            if repository['nodes']:
                item['allowedTopologies']=[{'matchLabelExpressions':[{'key':'overlaybd.ssf.dev/node','values':repository['nodes']}]}]
            items.append(item)
    metadata={'name':'overlaybd-native-retain'}
    if default_snapshot:
        metadata['annotations']={'snapshot.storage.kubernetes.io/is-default-class':'true'}
    items.append({'apiVersion':'snapshot.storage.k8s.io/v1','kind':'VolumeSnapshotClass','metadata':metadata,
                  'driver':DRIVER,'deletionPolicy':'Retain'})
    return {'apiVersion':'v1','kind':'List','items':items}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory',required=True)
    parser.add_argument('--default-snapshot-class',action='store_true',help='explicitly declare the new snapshot class default for this driver')
    args=parser.parse_args()
    print(json.dumps(render(json.loads(Path(args.inventory).read_text()),args.default_snapshot_class),indent=2))

if __name__=='__main__': main()
