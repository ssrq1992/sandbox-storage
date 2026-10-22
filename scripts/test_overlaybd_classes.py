import json
from pathlib import Path
import unittest
from render_overlaybd_classes import render
from render_overlaybd_controller import render as controller

class Classes(unittest.TestCase):
    def test_expansion_is_explicit_and_has_resizer_status_permissions(self):
        result=controller('test','image','repo','node-a:9455',['node-a'],True)
        config=next(x for x in result['items'] if x['kind']=='ConfigMap')
        self.assertTrue(json.loads(config['data']['controller.json'])['offlineExpansion'])
        pod=next(x for x in result['items'] if x['kind']=='Deployment')['spec']['template']['spec']
        names=[x['name'] for x in pod['containers']]
        self.assertIn('resizer',names)
        self.assertNotIn('attacher',names)
        roles=next(x for x in result['items'] if x['kind']=='ClusterRole')['rules']
        self.assertTrue(any('persistentvolumeclaims/status' in rule['resources'] for rule in roles))

    def test_class_topology_profile_and_opt_in_default(self):
        source=json.loads((Path(__file__).resolve().parents[1]/'drivers/overlaybd/deploy/nodes.local.example.json').read_text())
        result=render(source)
        classes=[v for v in result['items'] if v['kind']=='StorageClass']
        self.assertEqual({v['volumeBindingMode'] for v in classes},{'Immediate','WaitForFirstConsumer'})
        for v in classes:
            self.assertEqual(v['parameters'],{'repository':source['nodes'][0]['repository']})
            self.assertEqual(v['allowedTopologies'][0]['matchLabelExpressions'][0]['values'],['node-a'])
            self.assertEqual(v['reclaimPolicy'],'Retain')
            self.assertFalse(v['allowVolumeExpansion'])
            self.assertNotIn('storageclass.kubernetes.io/is-default-class',v['metadata']['annotations'])
        self.assertNotIn('annotations',result['items'][-1]['metadata'])
        expanded=render(dict(source,offline_expansion=True))
        self.assertTrue(all(v['allowVolumeExpansion'] for v in expanded['items'] if v['kind']=='StorageClass'))
        self.assertEqual(render(source,True)['items'][-1]['metadata']['annotations'],{'snapshot.storage.kubernetes.io/is-default-class':'true'})

if __name__=='__main__':unittest.main()
