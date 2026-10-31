"""Offline checks for coverage-warning coordination across heartbeat steps."""
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import tempfile
import threading
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from scripts.digest_coverage_warning import warn_coverage_unconfirmed

ROOT = Path(__file__).resolve().parents[1]


def heartbeat_function(namespace):
    import sys
    names = ('notifications', 'notifications.receipt_alerts', 'notifications.unified',
             'notifications.approved_gates', 'quant_platform_kit', 'quant_platform_kit.common',
             'quant_platform_kit.common.execution_receipts',
             'quant_platform_kit.common.operational_notification_localization',
             'quant_platform_kit.common.runtime_heartbeat_policy')
    modules = {}
    for name in names:
        module = types.ModuleType(name)
        module.__path__ = []
        module.__getattr__ = lambda key: lambda *args, **kwargs: None
        modules[name] = module
    spec = importlib.util.spec_from_file_location('test_heartbeat', ROOT / 'scripts/execution_report_heartbeat.py')
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    for key, value in namespace.items():
        setattr(module, key, value)
    return module._send_normal_heartbeat


class CoverageWarningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / 'attempt')
        self.env = patch.dict(os.environ, {'DIGEST_COVERAGE_ALERT_ATTEMPT_PATH': self.path}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.messages = []

    def send(self, text):
        self.messages.append(text)
        return True

    def heartbeat(self, send=None):
        def fail(*args, **kwargs):
            raise RuntimeError('private exception')
        namespace = {'unified_enabled': lambda: True, 'publish_receipt': fail,
                     'render_receipt_alert': lambda **kwargs: '', '_notification_locale': lambda: 'zh',
                     '_workflow_run_url': lambda: '', '_send_telegram': send or self.send,
                     '_send_normal_heartbeat_legacy': lambda *a, **k: 0,
                     'warn_coverage_unconfirmed': warn_coverage_unconfirmed}
        return heartbeat_function(namespace)('synthetic', '', source_reports=[{'execution_receipt': {'synthetic': True}}])

    def emitter(self, send=None):
        import sys
        def fail(*args, **kwargs):
            raise RuntimeError('private exception')
        unified = types.ModuleType('notifications.unified')
        unified.emit_completed_candidates = fail
        heartbeat = types.ModuleType('scripts.execution_report_heartbeat')
        heartbeat._notification_locale = lambda: 'zh'
        heartbeat._send_telegram = send or self.send
        spec = importlib.util.spec_from_file_location('test_emitter', ROOT / 'scripts/emit_unified_digest_candidates.py')
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'notifications': types.ModuleType('notifications'),
                                     'notifications.unified': unified,
                                     'scripts.execution_report_heartbeat': heartbeat}):
            spec.loader.exec_module(module)
            with contextlib.redirect_stdout(io.StringIO()):
                return module.main()

    def test_heartbeat_and_emitter_fail_once(self):
        self.assertEqual(self.heartbeat(), 1)
        self.assertEqual(self.emitter(), 1)
        self.assertEqual(len(self.messages), 1)

    def test_emitter_alone_and_unrelated_alert(self):
        namespace = {'unified_enabled': lambda: True,
                     'publish_receipt': lambda legacy, **kwargs: legacy(),
                     'render_receipt_alert': lambda **kwargs: 'unrelated urgent alert',
                     '_notification_locale': lambda: 'zh', '_workflow_run_url': lambda: '',
                     '_send_telegram': self.send, '_send_normal_heartbeat_legacy': lambda *a, **k: 0,
                     'warn_coverage_unconfirmed': warn_coverage_unconfirmed}
        self.assertEqual(heartbeat_function(namespace)('synthetic', '', quiet_eligible=False), 0)
        self.assertEqual(self.emitter(), 1)
        self.assertEqual(len(self.messages), 2)

    def test_unknown_delivery_never_blindly_retried(self):
        for behavior in ('false', 'throw'):
            with self.subTest(behavior=behavior), tempfile.TemporaryDirectory() as directory:
                os.environ['DIGEST_COVERAGE_ALERT_ATTEMPT_PATH'] = str(Path(directory) / 'marker')
                calls = []
                def send(text):
                    calls.append(text)
                    if behavior == 'throw':
                        raise RuntimeError('private exception')
                    return False
                self.assertEqual(self.heartbeat(send), 1)
                self.assertEqual(self.emitter(send), 1)
                self.assertEqual(len(calls), 1)
                markers = list(Path(directory).glob('marker-*'))
                self.assertEqual(len(markers), 1)
                self.assertEqual(markers[0].read_text(), 'attempted\n')

    def test_distinct_job_or_run_marker(self):
        os.environ['DIGEST_TARGET_BINDINGS_JSON'] = 'synthetic-binding'
        self.assertTrue(warn_coverage_unconfirmed(self.send, 'en'))
        os.environ['DIGEST_TARGET_ID'] = 'synthetic-target-b'
        self.assertTrue(warn_coverage_unconfirmed(self.send, 'en'))
        os.environ['DIGEST_COVERAGE_ALERT_ATTEMPT_PATH'] = self.path + '-different-job-run-target'
        self.assertTrue(warn_coverage_unconfirmed(self.send, 'en'))
        self.assertEqual(len(self.messages), 3)

    def test_marker_creation_failure_keeps_alert_and_failure(self):
        os.environ['DIGEST_COVERAGE_ALERT_ATTEMPT_PATH'] = str(Path(self.temp.name) / 'missing' / 'marker')
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertTrue(warn_coverage_unconfirmed(self.send, 'zh'))
            self.assertEqual(self.emitter(), 1)
        self.assertEqual(len(self.messages), 2)

    def test_missing_or_invalid_marker_never_claims_attempt(self):
        self.assertTrue(warn_coverage_unconfirmed(self.send, 'zh'))
        marker = next(Path(self.temp.name).glob('attempt-*'))
        marker.write_bytes(b'\xff')
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertTrue(warn_coverage_unconfirmed(self.send, 'zh'))
            del os.environ['DIGEST_COVERAGE_ALERT_ATTEMPT_PATH']
            os.environ['GITHUB_ACTIONS'] = 'true'
            self.assertTrue(warn_coverage_unconfirmed(self.send, 'zh'))
        self.assertEqual(len(self.messages), 3)

    def test_incomplete_temporary_marker_is_never_visible_as_reservation(self):
        real_link = os.link
        first_before_link = threading.Event()
        release_first = threading.Event()
        lock = threading.Lock()
        calls = []

        def delayed_link(source, destination):
            with lock:
                first = not calls
                calls.append(source)
            if first:
                first_before_link.set()
                self.assertTrue(release_first.wait(5))
            return real_link(source, destination)

        with patch('scripts.digest_coverage_warning.os.link', side_effect=delayed_link):
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(warn_coverage_unconfirmed, self.send, 'zh')
                self.assertTrue(first_before_link.wait(5))
                second = executor.submit(warn_coverage_unconfirmed, self.send, 'zh')
                self.assertTrue(second.result(timeout=5))
                release_first.set()
                self.assertFalse(first.result(timeout=5))
        self.assertEqual(len(self.messages), 1)
        self.assertEqual(len(list(Path(self.temp.name).glob('attempt-*'))), 1)
        self.assertEqual(len(list(Path(self.temp.name).glob('.digest-attempt-*'))), 0)

    def test_languages_are_bounded_and_private(self):
        for locale in ('zh', 'en'):
            with self.subTest(locale=locale), tempfile.TemporaryDirectory() as directory:
                os.environ['DIGEST_COVERAGE_ALERT_ATTEMPT_PATH'] = str(Path(directory) / 'marker')
                sent = []
                self.assertTrue(warn_coverage_unconfirmed(lambda text: sent.append(text) or True, locale))
                self.assertIn('Schwab', sent[0])
                self.assertNotIn('private exception', sent[0])
                self.assertNotIn('http', sent[0].lower())

    def test_workflow_uses_same_job_path_in_both_steps(self):
        workflow = (ROOT / '.github/workflows/execution-report-heartbeat.yml').read_text()
        marker = ('DIGEST_COVERAGE_ALERT_ATTEMPT_PATH: ${{ runner.temp }}/schwab-digest-coverage-'
                  '${{ github.run_id }}-${{ github.run_attempt }}-${{ github.job }}')
        self.assertEqual(workflow.count(marker), 2)
        for name in ('Check recent execution report', 'Publish completed-window candidates'):
            step = workflow.split(f'- name: {name}', 1)[1].split('\n      - name:', 1)[0]
            self.assertIn(marker, step)


if __name__ == '__main__':
    unittest.main()
