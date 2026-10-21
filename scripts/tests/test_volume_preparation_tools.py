"""Local rendering contracts; no API server or device is exercised."""
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from render_volume_preparation import render


class PreparationRenderTests(unittest.TestCase):
    def test_vendor_settings_preserved_and_class_not_overwritten(self):
        original = {'kind': 'StorageClass', 'metadata': {'name': 'vendor', 'uid': 'old',
                    'annotations': {'storageclass.kubernetes.io/is-default-class': 'true'}},
                    'provisioner': 'vendor.example/csi', 'parameters': {'pool': 'original'},
                    'allowedTopologies': [{'matchLabelExpressions': [{'key': 'zone', 'values': ['a']}]}]}
        before = copy.deepcopy(original)
        result = render(original, 'agentenv-block', 'WaitForFirstConsumer', 'tenant', 'ssf-system', b'ca')
        self.assertEqual(original, before)
        config, webhook, sc = result['items']
        self.assertEqual(sc['parameters'], original['parameters'])
        self.assertEqual(sc['provisioner'], original['provisioner'])
        self.assertEqual(sc['allowedTopologies'], original['allowedTopologies'])
        self.assertEqual(sc['reclaimPolicy'], 'Retain')
        self.assertNotIn('uid', sc['metadata'])
        self.assertNotIn('storageclass.kubernetes.io/is-default-class', sc['metadata']['annotations'])
        self.assertEqual(webhook['webhooks'][0]['failurePolicy'], 'Fail')
        self.assertEqual(json.loads(config['data']['config.json'])['CoordinatorUsers'],
                         ['system:serviceaccount:tenant:agentenv-scheduler'])
        with self.assertRaises(ValueError):
            render(original, 'vendor', 'Immediate', 'tenant', 'ssf-system', b'ca')
