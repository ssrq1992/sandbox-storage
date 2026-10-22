import copy
import json
from pathlib import Path
import unittest
from overlaybd_node_overlay import build_inventory, resources

class OverlayBDDeployment(unittest.TestCase):
    def setUp(self):
        self.source=json.loads((Path(__file__).resolve().parents[1]/'drivers/overlaybd/deploy/nodes.local.example.json').read_text())
    def test_original_sidecars_are_preserved_and_driver_has_no_control_key(self):
        objects,patches,repositories=resources(self.source,'test','image@sha256:fixture')
        existing=[{'name':'ssf-node'},{'name':'agentenv-storage'}]
        for p in patches:
            if p['path'].endswith('/initContainers/0'):
                existing.insert(0,p['value'])
        self.assertEqual([c['name'] for c in existing],['overlaybd-config','overlaybd-daemon','overlaybd-native','overlaybd-worker','overlaybd-csi','overlaybd-registrar','ssf-node','agentenv-storage'])
        driver=existing[4]
        self.assertNotIn('storage-node-keys',[m['name'] for m in driver['volumeMounts']])
        public=next(p['value'] for p in patches if p['path'].endswith('/volumes/-') and p['value']['name']=='overlaybd-plan-public')
        self.assertEqual(public['secret']['items'],[{'key':'plan.pub','path':'plan.pub'}])
        self.assertEqual(repositories[0]['nodes'],['node-a'])
        self.assertEqual(len([o for o in objects if o['kind']=='CSIDriver']),1)
    def test_expansion_requires_native_tools_and_mounts_them_readonly(self):
        self.source['offline_expansion']=True
        with self.assertRaises(ValueError): resources(self.source,'test','image')
        self.source['overlaybd_tools_volume']={'hostPath':{'path':'/var/lib/aenv/env/overlaybd','type':'Directory'}}
        objects,patches,_=resources(self.source,'test','image')
        daemon=next(p['value'] for p in patches if p['value'].get('name')=='overlaybd-daemon')
        self.assertIn('--config',daemon['args'])
        mount=next(m for m in daemon['volumeMounts'] if m['name']=='overlaybd-tools')
        self.assertTrue(mount['readOnly'])
        self.assertIn('daemon.toml',objects[0]['data'])

    def test_local_and_shared_identity_cannot_be_conflated(self):
        source=copy.deepcopy(self.source)
        other=copy.deepcopy(source['nodes'][0]);other['node']='node-b';other['accessible_nodes']=['node-b']
        source['nodes'].append(other)
        with self.assertRaises(ValueError): build_inventory(source)
        source=copy.deepcopy(self.source);source['nodes'][0]['accessible_nodes']=[]
        with self.assertRaises(ValueError): resources(source,'test','image')
        source['repository_volume']={'nfs':{'server':'nfs.test','path':'/ssf-overlaybd'}}
        nodes,repos=build_inventory(source)
        self.assertEqual(nodes['nodes']['node-a']['native']['accessible_nodes'],[])
        self.assertEqual(repos[0]['nodes'],[])
        resources(source,'test','image')
    def test_shared_repository_must_have_one_backend_definition(self):
        source=copy.deepcopy(self.source)
        for n in source['nodes']: n['accessible_nodes']=[]
        other=copy.deepcopy(source['nodes'][0]);other.update(node='node-b',repository_backend='oss',oss={'bucket':'other','prefix':'different'})
        source['nodes'].append(other)
        with self.assertRaises(ValueError): build_inventory(source)

if __name__=='__main__': unittest.main()
