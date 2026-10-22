import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from native_pvc_e2e import expected_profile, validate_overlaybd_evidence


class OverlayBDEvidenceTests(unittest.TestCase):
    def proof(self):
        return [{'evidence': {'handle': 'obd1.cmVwbw.dm9sdW1l',
            'scope': 'read-only-native-journal-and-device-observation',
            'sessions': [{'live_block': True, 'identity': 'session-device',
                          'device': '/dev/ublkb12', 'head': 'digest'}],
            'heads': [{'revision': 'digest', 'layers': 1, 'generation': 1, 'filesystem_uuid': '29aff887-2fe8-431a-a1a5-ad5766b7bf0a', 'capacity_bytes': 1073741824}]}}]

    def test_native_profile_is_distinct(self):
        self.assertEqual(expected_profile('native-overlaybd'), 'agentenv-overlaybd-ext4-v1')
        self.assertEqual(expected_profile('fixture'), 'agentenv-block-ext4-v1')

    def test_requires_both_layers_and_live_ublk_for_exact_handle(self):
        handle = self.proof()[0]['evidence']['handle']
        validate_overlaybd_evidence(handle, self.proof())
        for field, value in [('handle', 'other'), ('heads', []), ('sessions', [])]:
            proof = self.proof()
            proof[0]['evidence'][field] = value
            with self.assertRaises(AssertionError):
                validate_overlaybd_evidence(handle, proof)
        for field, value in [('live_block', False), ('device', '/dev/loop0'), ('identity', ''), ('head', 'other-head')]:
            proof = self.proof()
            proof[0]['evidence']['sessions'][0][field] = value
            with self.assertRaises(AssertionError):
                validate_overlaybd_evidence(handle, proof)


if __name__ == '__main__':
    unittest.main()
