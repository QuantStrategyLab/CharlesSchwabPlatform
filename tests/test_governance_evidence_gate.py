"""Synthetic evidence and command-boundary tests; no scanner or review authority."""

import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts import ai_review_evidence, governance_evidence_gate


ROOT = Path(__file__).resolve().parents[1]
BASE = "a" * 40
HEAD = "b" * 40
MARKER = "synthetic-sensitive-marker"
SPEC = importlib.util.spec_from_file_location(
    "synthetic_review_fixture", ROOT / "tests/test_ai_review_evidence.py")
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)


def native_finding():
    return {"RuleID": "generic-api-key", "File": "tests/synthetic-input.py",
            "StartLine": 7, "EndLine": 7, "Commit": HEAD,
            "Secret": MARKER, "Match": MARKER, "Description": MARKER}


def workflow_script(name):
    """Read the actual indented Bash body without running any setup step."""
    workflow = (ROOT / ".github/workflows/engineering-governance.yml").read_text()
    section = workflow.split("      - name: " + name + "\n", 1)[1]
    body = section.split("        run: |\n", 1)[1]
    result = []
    for line in body.splitlines():
        if line and not line.startswith("          "):
            break
        result.append(line[10:] if line else "")
    return "\n".join(result) + "\n"


class GateEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.report = self.directory / "report.json"
        self.capture = self.directory / "exit.txt"
        self.review = self.directory / "review.json"
        self.report.write_text("[]")
        self.capture.write_text("0\n")
        self.document = FIXTURE.fixture()
        self.review.write_text(json.dumps(self.document))

    def inspect(self, **changes):
        args = {"report_path": self.report, "exit_path": self.capture,
                "review_path": self.review, "base": BASE, "head": HEAD}
        args.update(changes)
        result = governance_evidence_gate.inspect_gate(**args)
        self.assertIn(result["status"], {"BLOCKED", "FAIL"})
        for key in ("execution_authorized", "production_ready", "merge_authorized",
                    "owner_approval_granted"):
            self.assertIs(result[key], False)
        self.assertNotIn(MARKER, json.dumps(result))
        self.assertNotIn(str(self.directory), json.dumps(result))
        return result

    def arguments(self):
        return ["--gitleaks-report", str(self.report), "--scanner-exit-file", str(self.capture),
                "--ai-review", str(self.review), "--base", BASE, "--head", HEAD]

    def test_consistent_matching_evidence_is_blocked_without_authentication(self):
        result = self.inspect()
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["gitleaks"]["metadata_consistency"], "CONSISTENT")
        self.assertEqual(result["ai_review"]["structural_status"], "CONSISTENT")
        self.assertIn("AI_REVIEW_TRUSTED_PROVENANCE_UNAVAILABLE", result["reason_codes"])

    def test_missing_input_is_not_an_empty_successful_report(self):
        for attribute in ("report_path", "exit_path", "review_path"):
            with self.subTest(attribute=attribute):
                result = self.inspect(**{attribute: self.directory / "absent"})
                self.assertEqual(result["status"], "BLOCKED")

    def test_nonzero_scanner_exit_survives_absent_or_invalid_reports(self):
        for code in (1, 2, 7, 127, -9):
            self.capture.write_text(str(code))
            for payload in (None, "[]", "invalid-json", json.dumps([native_finding()])):
                with self.subTest(code=code, payload=payload is None):
                    if payload is None:
                        self.report.unlink(missing_ok=True)
                    else:
                        self.report.write_text(payload)
                    result = self.inspect()
                    self.assertEqual(result["status"], "FAIL")
                    self.assertIn("GITLEAKS_SCANNER_EXIT_NONZERO", result["reason_codes"])

    def test_findings_with_zero_exit_fail_without_exposing_location_or_values(self):
        self.report.write_text(json.dumps([native_finding()]))
        result = self.inspect()
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["gitleaks"]["reported_finding_count"], 1)
        self.assertNotIn("tests/synthetic-input.py", json.dumps(result))
        self.assertNotIn("generic-api-key", json.dumps(result))
        self.assertNotIn(HEAD, json.dumps(result))

    def test_malformed_exit_capture_fails(self):
        for value in ("", "true", "0.0", "+0", "01", "-0", "256", "-256", "0\n\n", "9" * 20):
            with self.subTest(value=value):
                self.capture.write_text(value)
                result = self.inspect()
                self.assertEqual(result["status"], "FAIL")
                self.assertIn("SCANNER_EXIT_CAPTURE_INVALID", result["reason_codes"])

    def test_nonregular_exit_capture_does_not_block_or_follow_symlinks(self):
        link = self.directory / "link"
        link.symlink_to(self.capture)
        fifo = self.directory / "fifo"
        os.mkfifo(fifo)
        for path in (self.directory, link, fifo):
            with self.subTest(kind=path.name):
                result = self.inspect(exit_path=path)
                self.assertEqual(result["status"], "FAIL")

    def test_each_local_evidence_file_is_opened_once(self):
        original = os.open
        with mock.patch.object(os, "open", wraps=original) as opened:
            self.inspect()
        paths = [os.fspath(call.args[0]) for call in opened.call_args_list]
        for path in (self.capture, self.report, self.review):
            self.assertEqual(paths.count(str(path)), 1)

    def test_unrelated_review_range_is_a_failure_not_only_unverified_provenance(self):
        for base, head in (("c" * 40, HEAD), (BASE, "c" * 40)):
            result = self.inspect(base=base, head=head)
            self.assertEqual(result["status"], "FAIL")
            self.assertIn("AI_REVIEW_EXPECTED_REVISION_MISMATCH", result["reason_codes"])

    def test_missing_malformed_or_wrong_type_event_range_fails(self):
        for base, head in ((None, None), (BASE, None), (None, HEAD), (BASE, BASE),
                           (True, HEAD), (BASE, [HEAD]), ("0" * 40, HEAD),
                           (BASE.upper(), HEAD), (BASE, "not-a-revision")):
            with self.subTest(base_type=type(base).__name__, head_type=type(head).__name__):
                result = self.inspect(base=base, head=head)
                self.assertEqual(result["status"], "FAIL")
                self.assertIn("EVENT_REVISION_RANGE_INVALID", result["reason_codes"])

    def test_caller_approval_flags_and_review_names_cannot_authenticate_evidence(self):
        for field in ("owner_approved", "authenticated", "independent"):
            document = copy.deepcopy(self.document)
            document[field] = True
            self.review.write_text(json.dumps(document))
            result = self.inspect()
            self.assertEqual(result["status"], "FAIL")
            self.assertIn("AI_REVIEW_INVALID_SCHEMA", result["reason_codes"])

    def test_malformed_or_oversized_review_remains_failed(self):
        for raw in ("{", '{"schema_version":1,"schema_version":1}', " " * 262145):
            self.review.write_text(raw)
            self.assertEqual(self.inspect()["status"], "FAIL")

    def test_unexpected_success_result_from_an_inspector_cannot_grant_acceptance(self):
        with mock.patch.object(ai_review_evidence, "load_and_validate", return_value={"status": "PASS"}):
            result = self.inspect()
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["reason_codes"], ["INSPECTOR_RESULT_CONTRACT_INVALID"])

    def test_cli_blocked_exit_and_argument_errors_are_not_success(self):
        for arguments, expected in ((self.arguments(), 2), (["--invalid", MARKER], 1)):
            completed = subprocess.run([sys.executable, "-m", "scripts.governance_evidence_gate", *arguments],
                                       cwd=ROOT, capture_output=True, text=True, check=False, timeout=5)
            self.assertEqual(completed.returncode, expected, completed.stderr)
            payload = json.loads(completed.stdout)
            self.assertIn(payload["status"], {"BLOCKED", "FAIL"})
            self.assertNotIn(MARKER, completed.stdout + completed.stderr)

    def test_unexpected_failure_is_fixed_code_without_exception_details(self):
        output = io.StringIO()
        with mock.patch.object(governance_evidence_gate, "inspect_gate", side_effect=RuntimeError(MARKER)):
            with contextlib.redirect_stdout(output):
                code = governance_evidence_gate.main(self.arguments())
        self.assertEqual(code, 1)
        self.assertNotIn(MARKER, output.getvalue())
        self.assertEqual(json.loads(output.getvalue())["reason_codes"], ["INTERNAL_GATE_ERROR"])


class RevisionBindingTests(unittest.TestCase):
    def test_absent_constraints_preserve_the_existing_contract(self):
        document = FIXTURE.fixture()
        original = ai_review_evidence.validate_evidence(document)
        self.assertEqual(original, ai_review_evidence.validate_evidence(
            document, expected_base=None, expected_head=None))
        self.assertEqual(original["status"], "BLOCKED")

    def test_matching_constraints_do_not_authenticate_review(self):
        document = FIXTURE.fixture()
        result = ai_review_evidence.validate_evidence(document, expected_base=BASE, expected_head=HEAD)
        self.assertEqual(result, ai_review_evidence.validate_evidence(document))
        self.assertEqual(result["authentication"], "UNVERIFIED")

    def test_partial_malformed_and_wrong_type_constraints_fail(self):
        for base, head in ((BASE, None), (None, HEAD), (True, HEAD), (BASE, 1),
                           ([BASE], HEAD), (BASE, {}), (BASE, BASE),
                           ("0" * 40, HEAD), (BASE, "b" * 39)):
            with self.subTest(base_type=type(base).__name__, head_type=type(head).__name__):
                result = ai_review_evidence.validate_evidence(
                    FIXTURE.fixture(), expected_base=base, expected_head=head)
                self.assertEqual(result["status"], "FAIL")
                self.assertIn("invalid_expected_revision_range", result["reason_codes"])

    def test_mismatched_expected_range_fails(self):
        for base, head in (("c" * 40, HEAD), (BASE, "c" * 40)):
            result = ai_review_evidence.validate_evidence(
                FIXTURE.fixture(), expected_base=base, expected_head=head)
            self.assertEqual(result["status"], "FAIL")
            self.assertIn("expected_revision_mismatch", result["reason_codes"])


class WorkflowCommandBoundaryTests(unittest.TestCase):
    def run_scan(self, *, code=0, pull_request=True, base=BASE, missing_tool=False):
        # Execute only the existing Bash body with a local scanner stub. This
        # tests native exit propagation, not scanner findings or authenticity.
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            binary = temporary / "gitleaks-bin/gitleaks"
            binary.parent.mkdir()
            called = temporary / "called.json"
            if not missing_tool:
                binary.write_text(
                    "#!/usr/bin/env python3\nimport json, os, pathlib, sys\n"
                    "pathlib.Path(os.environ['RUNNER_TEMP'], 'called.json').write_text(json.dumps(sys.argv[1:]))\n"
                    "sys.exit(int(os.environ['SYNTHETIC_SCANNER_EXIT']))\n")
                binary.chmod(0o700)
            environment = {"PATH": os.environ["PATH"], "RUNNER_TEMP": directory,
                           "PR_BASE": base if pull_request else "",
                           "PR_HEAD": HEAD if pull_request else "",
                           "PUSH_BASE": "" if pull_request else base,
                           "PUSH_HEAD": "" if pull_request else HEAD,
                           "SYNTHETIC_SCANNER_EXIT": str(code)}
            completed = subprocess.run(
                ["bash", "-c", workflow_script("Scan added commits with redacted findings")],
                cwd=ROOT, env=environment, capture_output=True, text=True,
                check=False, timeout=5)
            arguments = json.loads(called.read_text()) if called.exists() else None
            return completed.returncode, arguments

    def test_native_scanner_exit_codes_are_preserved(self):
        for code in (0, 1, 2, 7, 127):
            with self.subTest(code=code):
                actual, arguments = self.run_scan(code=code)
                self.assertEqual(actual, code)
                self.assertEqual(arguments, ["git", ".", "--redact", "--no-banner",
                                             "--log-opts=" + BASE + ".." + HEAD])

    def test_push_range_uses_the_existing_event_fallback(self):
        code, arguments = self.run_scan(pull_request=False)
        self.assertEqual(code, 0)
        self.assertIn("--log-opts=" + BASE + ".." + HEAD, arguments)

    def test_missing_or_zero_base_stops_before_scanning(self):
        for base in ("", "0" * 40):
            with self.subTest(base=base):
                code, arguments = self.run_scan(base=base)
                self.assertEqual(code, 1)
                self.assertIsNone(arguments)

    def test_missing_scanner_is_not_a_successful_contract_result(self):
        code, arguments = self.run_scan(missing_tool=True)
        self.assertEqual(code, 127)
        self.assertIsNone(arguments)


if __name__ == "__main__":
    unittest.main()
