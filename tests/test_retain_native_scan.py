"""Native command/output retention controls, without external services."""

import contextlib
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts import ai_review_evidence, retain_native_scan


ROOT = Path(__file__).resolve().parents[1]
BASE = "a" * 40
HEAD = "b" * 40


class NativeRetentionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True, capture_output=True)
        tree = subprocess.run(["git", "mktree"], cwd=self.root, input="", text=True,
                              check=True, capture_output=True).stdout.strip()
        def commit(message, parent=None):
            args = ["git", "-c", "user.name=Synthetic Review", "-c",
                    "user.email=synthetic@example.invalid", "commit-tree", tree, "-m", message]
            if parent is not None:
                args.extend(["-p", parent])
            return subprocess.run(args, cwd=self.root, text=True, check=True,
                                  capture_output=True).stdout.strip()
        self.base = commit("test: baseline fixture")
        self.head = commit("test: candidate fixture", self.base)
        self.divergent = commit("test: divergent fixture")
        self.tree = tree
        self.binary = self.root / "gitleaks"
        self.output = self.root / "evidence"
        self.binary.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, pathlib, signal, sys\n"
            "target = pathlib.Path(next(x.split('=', 1)[1] for x in sys.argv if x.startswith('--report-path=')))\n"
            "kind = os.environ.get('SYNTHETIC_REPORT', 'valid')\n"
            "if kind == 'valid': target.write_text('[]')\n"
            "elif kind == 'finding': target.write_text('[{\"File\": \"private/path.txt\", \"StartLine\": 7, \"RuleID\": \"rule\"}]')\n"
            "elif kind == 'malformed': target.write_text('{')\n"
            "elif kind == 'object': target.write_text('{}')\n"
            "elif kind == 'symlink': target.symlink_to(pathlib.Path(__file__))\n"
            "elif kind == 'fifo': os.mkfifo(target)\n"
            "elif kind == 'oversized': target.write_text(' ' * 1000001)\n"
            "elif kind == 'deep': target.write_text('[' * 20000 + ']' * 20000)\n"
            "elif kind == 'nonfinite': target.write_text('[NaN]')\n"
            "elif kind == 'overflow': target.write_text('[1e400]')\n"
            "if os.environ.get('SYNTHETIC_CAPTURE_ERROR'): (target.parent / 'scanner-exit.txt').mkdir()\n"
            "if os.environ.get('SYNTHETIC_RECEIPT_ERROR'): (target.parent / 'capture.json').mkdir()\n"
            "if os.environ.get('SYNTHETIC_SIGNAL'): os.kill(os.getpid(), signal.SIGTERM)\n"
            "sys.exit(int(os.environ.get('SYNTHETIC_SCANNER_EXIT', '0')))\n")
        self.binary.chmod(0o700)

    def invoke(self, *, code=0, report="valid", binary=None, base=None, head=None, **environment):
        env = {"PATH": os.environ["PATH"], "SYNTHETIC_SCANNER_EXIT": str(code),
               "SYNTHETIC_REPORT": report, **environment}
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/retain_native_scan.py"),
             "--binary", str(self.binary) if binary is None else binary, "--output", str(self.output),
             "--base", self.base if base is None else base,
             "--head", self.head if head is None else head, "--scope", "event-range"],
            cwd=self.root, env=env, capture_output=True, text=True, timeout=5, check=False)
        return result

    def test_clean_native_report_and_original_exit_are_retained(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.output / "report.json").read_bytes(), b"[]")
        self.assertEqual((self.output / "scanner-exit.txt").read_text(), "0\n")
        receipt = json.loads((self.output / "capture.json").read_text())
        self.assertTrue(receipt["capture_complete"])
        self.assertTrue(receipt["native_process_started"])
        self.assertEqual(receipt["native_returncode"], 0)
        self.assertEqual(receipt["authentication"], "UNVERIFIED")
        self.assertEqual(receipt["coverage"], "UNVERIFIED")
        invocation = json.loads((self.output / "invocation.json").read_text())
        self.assertEqual(invocation["arguments"], [str(self.binary), "git", ".", "--redact",
                         "--no-banner", "--log-opts=" + self.base + ".." + self.head,
                         "--report-format=json", "--report-path=" + str(self.output / "report.json")])
        self.assertEqual(invocation["configured_version"], "8.30.1")
        self.assertEqual(invocation["base_sha"], self.base)
        self.assertEqual(invocation["head_sha"], self.head)
        self.assertEqual(invocation["revision_preflight"]["status"], "passed")
        self.assertEqual(len(invocation["binary_sha256"]), 64)
        self.assertNotIn("[]", result.stdout + result.stderr)

    def test_job_summary_has_counts_only(self):
        summary = self.root / "summary.md"
        result = self.invoke(code=1, report="finding", GITHUB_STEP_SUMMARY=str(summary))
        self.assertEqual(result.returncode, 1)
        text = summary.read_text()
        self.assertIn("findings in report: 1", text)
        self.assertIn("native exit code: 1", text)
        self.assertIn("capture complete: yes", text)
        for private in ("private/path.txt", "StartLine", "rule", self.base, self.head):
            self.assertNotIn(private, text)
            self.assertNotIn(private, result.stdout + result.stderr)

    def test_summary_failure_does_not_change_the_result(self):
        result = self.invoke(GITHUB_STEP_SUMMARY=str(self.root))
        self.assertEqual(result.returncode, 0)

    def test_native_failure_codes_survive_complete_capture(self):
        for code in (1, 2, 7, 127):
            with self.subTest(code=code):
                self.output = self.root / str(code)
                result = self.invoke(code=code)
                self.assertEqual(result.returncode, code)
                self.assertEqual((self.output / "scanner-exit.txt").read_text(), str(code) + "\n")

    def test_relative_binary_is_bound_to_the_current_directory(self):
        result = self.invoke(binary="./gitleaks")
        self.assertEqual(result.returncode, 0, result.stderr)
        invocation = json.loads((self.output / "invocation.json").read_text())
        self.assertEqual(invocation["arguments"][0], str(self.binary))

    def test_invalid_or_unavailable_commit_stops_before_native_scan(self):
        for base in ("f" * 40, "0" * 40, "short", self.tree):
            with self.subTest(base=base):
                self.output = self.root / ("invalid-" + base)
                result = self.invoke(base=base)
                self.assertEqual(result.returncode, 1)
                self.assertFalse((self.output / "scanner-exit.txt").exists())
                self.assertFalse((self.output / "report.json").exists())
                invocation = json.loads((self.output / "invocation.json").read_text())
                self.assertEqual(invocation["revision_preflight"]["status"], "failed")

    def test_missing_head_is_not_a_clean_native_scan(self):
        result = self.invoke(head="f" * 40)
        self.assertEqual(result.returncode, 1)
        self.assertFalse((self.output / "scanner-exit.txt").exists())
        self.assertFalse((self.output / "report.json").exists())

    def test_existing_divergent_commits_are_valid_inputs(self):
        result = self.invoke(base=self.divergent)
        self.assertEqual(result.returncode, 0, result.stderr)
        invocation = json.loads((self.output / "invocation.json").read_text())
        self.assertEqual(invocation["base_sha"], self.divergent)
        self.assertEqual(invocation["revision_preflight"]["status"], "passed")

    def test_native_failure_codes_survive_missing_or_invalid_reports(self):
        for code in (1, 2, 7, 127):
            for report in ("missing", "malformed", "object", "deep", "nonfinite", "overflow"):
                with self.subTest(code=code, report=report):
                    self.output = self.root / (str(code) + report)
                    result = self.invoke(code=code, report=report)
                    self.assertEqual(result.returncode, code)
                    self.assertEqual((self.output / "scanner-exit.txt").read_text(), str(code) + "\n")
                    self.assertFalse(json.loads((self.output / "capture.json").read_text())["capture_complete"])

    def test_clean_exit_with_unusable_report_fails_collection(self):
        for report in ("missing", "malformed", "object", "symlink", "fifo", "oversized",
                       "deep", "nonfinite", "overflow"):
            with self.subTest(report=report):
                self.output = self.root / report
                result = self.invoke(report=report)
                self.assertEqual(result.returncode, 1)
                self.assertEqual((self.output / "scanner-exit.txt").read_text(), "0\n")
                self.assertFalse(json.loads((self.output / "capture.json").read_text())["capture_complete"])

    def test_missing_native_executable_does_not_fabricate_a_process_exit(self):
        self.binary.unlink()
        result = self.invoke()
        self.assertEqual(result.returncode, 127)
        self.assertFalse((self.output / "scanner-exit.txt").exists())
        self.assertFalse((self.output / "report.json").exists())
        self.assertFalse((self.output / "capture.json").exists())

    def test_nonexecutable_native_binary_is_not_success(self):
        self.binary.chmod(0o600)
        self.assertEqual(self.invoke().returncode, 126)
        self.assertFalse((self.output / "scanner-exit.txt").exists())

    def test_native_signal_keeps_raw_negative_returncode_and_shell_exit(self):
        result = self.invoke(SYNTHETIC_SIGNAL="1")
        self.assertEqual(result.returncode, 143)
        self.assertEqual((self.output / "scanner-exit.txt").read_text(), "-15\n")
        receipt = json.loads((self.output / "capture.json").read_text())
        self.assertEqual((receipt["native_returncode"], receipt["shell_exit"]), (-15, 143))

    def test_capture_error_fails_clean_scan_and_preserves_native_failure(self):
        for code in (0, 2, 7):
            with self.subTest(code=code):
                self.output = self.root / str(code)
                result = self.invoke(code=code, SYNTHETIC_CAPTURE_ERROR="1")
                self.assertEqual(result.returncode, code or 1)
                self.assertFalse(json.loads((self.output / "capture.json").read_text())["capture_complete"])

    def test_receipt_error_fails_clean_scan_and_preserves_native_failure(self):
        for code in (0, 2, 7):
            with self.subTest(code=code):
                self.output = self.root / str(code)
                result = self.invoke(code=code, SYNTHETIC_RECEIPT_ERROR="1")
                self.assertEqual(result.returncode, code or 1)
                self.assertEqual((self.output / "scanner-exit.txt").read_text(), str(code) + "\n")

    def test_existing_output_cannot_reuse_a_stale_success(self):
        self.output.mkdir()
        (self.output / "report.json").write_text("[]")
        with mock.patch.object(retain_native_scan.subprocess, "run") as run:
            with contextlib.redirect_stderr(io.StringIO()):
                code = retain_native_scan.retain_scan(self.binary, self.output, BASE, HEAD, scope="event-range")
        self.assertEqual(code, 1)
        run.assert_not_called()
        self.assertFalse((self.output / "scanner-exit.txt").exists())

    def test_invocation_capture_failure_stops_before_scanning(self):
        with mock.patch.object(retain_native_scan, "_write_json", side_effect=OSError):
            with mock.patch.object(retain_native_scan.subprocess, "run") as run:
                with mock.patch.object(retain_native_scan, "_revision_preflight",
                                       return_value={"status": "passed"}):
                    with contextlib.redirect_stderr(io.StringIO()):
                        code = retain_native_scan.retain_scan(self.binary, self.output, BASE, HEAD, scope="event-range")
        self.assertEqual(code, 1)
        run.assert_not_called()


class WorkflowPrivacyTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / ".github/workflows/engineering-governance.yml").read_text()

    def test_no_report_artifact_is_uploaded_from_a_public_repository(self):
        self.assertNotIn("upload-artifact", self.workflow)
        self.assertNotIn("gitleaks-evidence/retrospective", self.workflow)

    def test_scan_revisions_come_from_the_event_only(self):
        self.assertNotIn("REVIEW_BASE", self.workflow)
        self.assertNotIn("REVIEW_HEAD", self.workflow)
        self.assertNotIn("retrospective-review-range", self.workflow)
        self.assertIn("github.event.pull_request.base.sha", self.workflow)
        self.assertIn("github.event.pull_request.head.sha", self.workflow)


class RecordedReviewTests(unittest.TestCase):
    def setUp(self):
        self.document = json.loads((ROOT / "docs/evidence/offline-governance-review-v1.json").read_text())

    def test_historical_review_document_is_consistent_and_remains_blocked(self):
        result = ai_review_evidence.validate_evidence(self.document)
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["structural_status"], "CONSISTENT")
        self.assertEqual(result["authentication"], "UNVERIFIED")
        self.assertEqual(result["required_check_coverage"], "UNVERIFIED")
        self.assertEqual(result["operational_authority"], "NONE")
        self.assertEqual(self.document["required_checks"], [])
        self.assertEqual(self.document["checks"], [])
        self.assertIn("required_checks_missing", result["reason_codes"])

    def test_historical_document_cannot_describe_another_head(self):
        result = ai_review_evidence.validate_evidence(
            self.document, expected_base=self.document["base_sha"], expected_head=HEAD)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("expected_revision_mismatch", result["reason_codes"])

    def test_declaring_approval_does_not_add_an_acceptance_path(self):
        document = copy.deepcopy(self.document)
        document["owner_approved"] = True
        self.assertEqual(ai_review_evidence.validate_evidence(document)["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
