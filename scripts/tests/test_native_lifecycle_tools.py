"""Tool-contract tests only; these never certify a real sandbox or CSI driver."""
import argparse
import io
import json
import os
from pathlib import Path
import struct
import sys
import tempfile
import urllib.error
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
from native_lifecycle_e2e import Suite
from native_cleanup import plan


class Response(io.BytesIO):
    status = 200


class Opener:
    def __init__(self, response):
        self.response = response
        self.request = None

    def open(self, request, **kwargs):
        self.request = request
        return self.response


def frame(value, flags=0):
    data = json.dumps(value).encode()
    return bytes([flags]) + struct.pack('>I', len(data)) + data


class NativeToolTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        args = argparse.Namespace(output=str(Path(self.tmp.name) / 'evidence'),
            url='https://gateway.invalid', api_key_env='SSF_UNIT_KEY', ca=None,
            driver_kind='fixture', timeout=1, envd_port=49983)
        with patch.dict(os.environ, {'SSF_UNIT_KEY': 'test-api-key'}):
            self.suite = Suite(args)

    def test_guest_uses_native_proxy_and_requires_exit(self):
        response = Response(frame({'event': {'end': {'exitCode': 0}}}) + frame({}, 2))
        opener = Opener(response)
        self.suite.opener = opener
        self.assertEqual(self.suite.guest({'sandboxID': 'native-id'}, 'true'), '')
        self.assertEqual(opener.request.full_url, 'https://gateway.invalid/proxy/process.Process/Start')
        self.assertEqual(opener.request.get_header('X-agentenv-sandbox-id'), 'native-id')
        self.assertNotIn('/v1/sandboxes', opener.request.full_url)

    def test_missing_exit_is_not_success(self):
        self.suite.opener = Opener(Response(frame({}, 2)))
        with self.assertRaises(AssertionError):
            self.suite.guest({'sandboxID': 'native-id'}, 'true')

    def test_truncated_stream_is_not_success(self):
        self.suite.opener = Opener(Response(b'\0\0\0\0\x20{}'))
        with self.assertRaisesRegex(AssertionError, 'truncated'):
            self.suite.guest({'sandboxID': 'native-id'}, 'true')

    def test_api_does_not_accept_200_for_create(self):
        self.suite.opener = Opener(Response(b'{}'))
        with self.assertRaises(AssertionError):
            self.suite.create('/sandboxes-cold', {'image': 'image'})
        self.assertFalse(self.suite.report['sandboxes'])

    def test_unexpected_successful_writer_is_recorded_for_cleanup(self):
        response = Response(b'{"sandboxID":"unexpected-writer"}')
        response.status = 201
        self.suite.opener = Opener(response)
        with self.assertRaises(AssertionError):
            self.suite.api('POST', '/sandboxes-cold', {'image': 'image'}, expected=409)
        self.assertEqual(self.suite.report['sandboxes'], ['unexpected-writer'])

    def test_rejection_requires_the_expected_status(self):
        class Rejected:
            def __init__(self, code): self.code = code
            def open(self, request, **kwargs):
                raise urllib.error.HTTPError(request.full_url, self.code, 'rejected', {},
                                             io.BytesIO(b'{"code":409}'))
        self.suite.opener = Rejected(409)
        self.assertEqual(self.suite.api('POST', '/sandboxes-cold', {}, 409), {'code': 409})
        self.suite.opener = Rejected(503)
        with self.assertRaises(AssertionError):
            self.suite.api('POST', '/sandboxes-cold', {}, 409)

    def test_report_redacts_credentials(self):
        response = Response(b'{"sandboxID":"native-id","envdAccessToken":"sensitive"}')
        response.status = 201
        self.suite.opener = Opener(response)
        self.suite.create('/sandboxes-cold', {'image': 'image'})
        saved = (self.suite.root / 'report.json').read_text()
        self.assertNotIn('sensitive', saved)
        self.assertNotIn('test-api-key', saved)
        self.assertIn('[REDACTED]', saved)

    def test_cleanup_keeps_user_volumes_and_checks_ownership(self):
        report = self.suite.report
        report['sandboxes'] = ['native-id', 'native-id']
        report['snapshots'] = ['snapshot-id']
        report['volumes'] = [{'name': 'unrelated-user-volume'}]
        self.assertEqual(plan(report), [{'kind': 'sandbox', 'method': 'DELETE',
                                         'path': '/sandboxes/native-id'}])
        with self.assertRaises(ValueError):
            plan(report, volumes=True)
        report['volumes'] = [{'name': 'imported-user-volume', 'imported': True}]
        self.assertEqual(len(plan(report, volumes=True)), 1)
        report['sandboxes'] = ['../escape']
        with self.assertRaises(ValueError):
            plan(report)


if __name__ == '__main__':
    unittest.main()
