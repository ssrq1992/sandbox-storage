import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / 'native_fault_e2e.py'


class NativeFaultScopeTests(unittest.TestCase):
    def run_case(self, stage, kind, fixture=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case = {'name': 'interruption', 'stage': stage}
            # Dry-run must never invoke even the supplied workload commands.
            for key in ('workload', 'inject', 'recover', 'assertions'):
                case[key] = [['/nonexistent/ssf-dry-run-command']]
            if fixture:
                case['csi_fault'] = [{'phase': 'before', 'count': 1}]
            spec = root / 'scenarios.json'
            spec.write_text(json.dumps({'version': 1, 'cases': [case]}))
            output = root / 'result'
            result = subprocess.run([sys.executable, str(SCRIPT), '--scenarios', str(spec),
                                     '--output', str(output), '--context', 'isolated-test',
                                     '--namespace', 'ssf-test', '--driver-kind', kind,
                                     '--fixture-pod', 'fixture'], capture_output=True, text=True)
            report = json.loads((output / 'report.json').read_text()) if output.exists() else None
            return result, report

    def test_overlaybd_dry_run_is_explicitly_not_executed(self):
        result, report = self.run_case('overlaybd-expansion-seal', 'native-overlaybd')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(report['driver_kind'], 'native-overlaybd')
        self.assertFalse(report['executed'])
        self.assertEqual(report['cases'][0]['status'], 'not_executed')

    def test_fixture_cannot_be_reported_as_native_overlaybd(self):
        result, report = self.run_case('overlaybd-restack', 'native-overlaybd', fixture=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(report)

    def test_overlaybd_stage_requires_explicit_driver(self):
        for kind in ('unspecified', 'fixture', 'vendor'):
            result, report = self.run_case('overlaybd-device-create', kind)
            self.assertNotEqual(result.returncode, 0)
            self.assertIsNone(report)


if __name__ == '__main__':
    unittest.main()
