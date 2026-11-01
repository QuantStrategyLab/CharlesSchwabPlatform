"""Synthetic offline reports; no scanner, repository history, or finding data."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import gitleaks_report_evidence as evidence


BASE = "a" * 40
HEAD = "b" * 40
SENTINEL = "SYNTHETIC_PRIVATE_VALUE_DO_NOT_EMIT"


def finding(**updates):
    value = {"RuleID": "generic-api-key", "File": "tests/fixture.py",
             "StartLine": 3, "EndLine": 4, "Commit": "c" * 40}
    value.update(updates)
    return value


def encode(value):
    return json.dumps(value).encode()


def validate(raw, **updates):
    captures = {"scanner_exit_code": 1, "base": BASE, "head": HEAD}
    captures.update(updates)
    return evidence.validate_report(raw, **captures)


class ReportEvidenceTests(unittest.TestCase):
    def assert_safe(self, result):
        self.assertIn(result["status"], {"BLOCKED", "FAIL"})
        self.assertFalse(result["execution_authorized"])
        self.assertFalse(result["production_ready"])
        self.assertFalse(result["scanner"]["authenticated"])
        self.assertFalse(result["scan_range"]["authenticated"])
        self.assertIn("SCANNER_EXECUTION_UNVERIFIED", result["gaps"])
        self.assertIn("SCANNER_VERSION_UNVERIFIED", result["gaps"])
        self.assertIn("REDACTION_UNVERIFIED", result["gaps"])
        self.assertIn("REPORT_PROVENANCE_UNVERIFIED", result["gaps"])
        self.assertIn("SCAN_COVERAGE_UNVERIFIED", result["gaps"])
        self.assertNotIn(SENTINEL, json.dumps(result))

    def assert_fail(self, raw, code, **updates):
        result = validate(raw, **updates)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn(code, result["issues"])
        self.assert_safe(result)
        return result

    def test_emits_only_validated_finding_metadata(self):
        row = finding(**{key: SENTINEL for key in (
            "Match", "Secret", "Author", "Email", "Date", "Message", "Tags",
            "Fingerprint", "Description", "SymlinkFile", "unknown_field")})
        raw = encode([row])
        result = validate(raw)
        self.assertEqual(result["finding_metadata"], [finding()])
        self.assertEqual(result["metadata_consistency"], "CONSISTENT")
        self.assertEqual(result["scanner"]["exit_code"], 1)
        self.assertEqual(result["scanner"]["outcome"], "EXIT_NONZERO")
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(raw, encode([row]))
        self.assert_safe(result)

    def test_explicit_empty_report_with_zero_exit_stays_blocked(self):
        result = validate(b"[]", scanner_exit_code=0)
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["metadata_consistency"], "CONSISTENT")
        self.assertEqual(result["report_state"], "EXPLICIT_EMPTY")
        self.assertEqual(result["reported_finding_count"], 0)
        self.assertEqual(result["scanner"]["outcome"], "EXIT_ZERO")
        self.assert_safe(result)

    def test_missing_report_and_empty_file_are_not_an_empty_report(self):
        missing = validate(None, scanner_exit_code=0)
        self.assertEqual(missing["status"], "BLOCKED")
        self.assertEqual(missing["report_state"], "MISSING")
        self.assertIsNone(missing["reported_finding_count"])
        self.assertIn("REPORT_MISSING", missing["issues"])
        self.assert_fail(b"", "JSON_INVALID", scanner_exit_code=0)
        self.assert_safe(missing)

    def test_missing_commit_is_blocked_without_inventing_a_revision(self):
        for commit in (None, ""):
            with self.subTest(commit=commit):
                result = validate(encode([finding(Commit=commit)]))
                self.assertEqual(result["metadata_consistency"], "BLOCKED")
                self.assertIn("COMMIT_MISSING", result["issues"])
                self.assertNotIn("Commit", result["finding_metadata"][0])
                self.assert_safe(result)
        row = finding()
        del row["Commit"]
        self.assertIn("COMMIT_MISSING", validate(encode([row]))["issues"])

    def test_unsafe_paths_fail_without_echoing_values(self):
        paths = ("/private/file.py", "../file.py", "tests/../file.py", "./file.py",
                 "tests//file.py", "tests/", "C:\\private\\file.py", "https://example.invalid/x",
                 "tests/a\n.py", "tests/a\t.py", "tests/a\x00.py", "tests/a\x1b.py",
                 "tests/\ud800.py", "tests/a%2fb.py", "tests/space name.py",
                 "tests/U1234567.py", "tests/account_12345678.py",
                 "tests/ghp_" + "x" * 36, "tests/" + "d" * 40 + ".py", "x" * 513)
        for value in paths:
            with self.subTest(path_index=paths.index(value)):
                result = self.assert_fail(encode([finding(File=value)]), "FILE_INVALID")
                self.assertEqual(result["finding_metadata"], [])

    def test_ordinary_repository_paths_are_retained(self):
        for value in (".github/workflows/ci.yml", "application/account_facts.py", "README.md"):
            with self.subTest(path=value):
                self.assertEqual(validate(encode([finding(File=value)]))["finding_metadata"][0]["File"], value)

    def test_invalid_rule_line_and_commit_metadata_fails(self):
        cases = (("RuleID", SENTINEL, "RULE_ID_INVALID"),
                 ("RuleID", "rule\ncommand", "RULE_ID_INVALID"),
                 ("RuleID", "x" * 65, "RULE_ID_INVALID"),
                 ("StartLine", True, "LINE_RANGE_INVALID"),
                 ("StartLine", 0, "LINE_RANGE_INVALID"),
                 ("StartLine", 5, "LINE_RANGE_INVALID"),
                 ("EndLine", 4.0, "LINE_RANGE_INVALID"),
                 ("EndLine", 2 ** 31, "LINE_RANGE_INVALID"),
                 ("Commit", "C" * 40, "COMMIT_INVALID"),
                 ("Commit", "0" * 40, "COMMIT_INVALID"),
                 ("Commit", SENTINEL, "COMMIT_INVALID"))
        for key, value, code in cases:
            with self.subTest(field=key, code=code):
                self.assert_fail(encode([finding(**{key: value})]), code)

    def test_missing_required_metadata_and_invalid_rows_fail(self):
        for key in ("RuleID", "File", "StartLine", "EndLine"):
            row = finding()
            del row[key]
            with self.subTest(field=key):
                self.assert_fail(encode([row]), "FINDING_METADATA_REQUIRED")
        for value in ({}, None, "text", [None], [False], [[finding()]]):
            with self.subTest(kind=type(value).__name__):
                self.assert_fail(encode(value), "REPORT_ARRAY_REQUIRED" if type(value) is not list else "FINDING_OBJECT_REQUIRED")

    def test_duplicate_keys_are_rejected_including_suppressed_fields(self):
        for raw in (b'[{"RuleID":"a","RuleID":"b"}]',
                    b'[{"Secret":{"value":1,"value":2}}]'):
            self.assert_fail(raw, "JSON_DUPLICATE_KEY")

    def test_nonfinite_numbers_are_rejected_even_in_suppressed_fields(self):
        for value in (b"NaN", b"Infinity", b"-Infinity", b"1e9999"):
            self.assert_fail(b'[{"Secret":' + value + b'}]', "JSON_NONFINITE_NUMBER")

    def test_malformed_and_nonbyte_reports_fail(self):
        for raw in (b"[", b"[true,]", b"\xff", b"[] trailing"):
            self.assert_fail(raw, "JSON_INVALID")
        for raw in ("[]", [], {}, bytearray(b"[]")):
            self.assert_fail(raw, "REPORT_BYTES_REQUIRED")

    def test_size_depth_and_finding_count_are_bounded(self):
        self.assert_fail(b" " * (evidence.MAX_REPORT_BYTES + 1), "REPORT_SIZE_EXCEEDED")
        self.assert_fail(b"[" * (evidence.MAX_JSON_DEPTH + 1) + b"]" * (evidence.MAX_JSON_DEPTH + 1), "JSON_DEPTH_EXCEEDED")
        self.assert_fail(encode([{}] * (evidence.MAX_FINDINGS + 1)), "FINDING_COUNT_EXCEEDED")
        result = validate(encode([finding(Secret="[" * 100 + "\\\"" + "]" * 100)]))
        self.assertEqual(result["metadata_consistency"], "CONSISTENT")

    def test_scanner_exit_report_contradictions_fail(self):
        self.assert_fail(encode([finding()]), "ZERO_EXIT_WITH_FINDINGS", scanner_exit_code=0)
        self.assert_fail(b"[]", "NONZERO_EXIT_WITH_EMPTY_REPORT", scanner_exit_code=1)

    def test_a_finding_cannot_belong_to_the_excluded_base_revision(self):
        self.assert_fail(encode([finding(Commit=BASE)]), "COMMIT_EQUALS_EXCLUDED_BASE")
        result = validate(encode([finding(Commit=HEAD)]))
        self.assertEqual(result["metadata_consistency"], "CONSISTENT")

    def test_nonzero_scanner_exit_is_never_masked_by_missing_report(self):
        for code in (1, 2, 255, -9):
            with self.subTest(exit_code=code):
                result = self.assert_fail(None, "SCANNER_EXIT_NONZERO", scanner_exit_code=code)
                self.assertEqual(result["scanner"]["exit_code"], code)

    def test_missing_captures_stay_unknown_and_cannot_mask_invalid_metadata(self):
        result = validate(b"[]", scanner_exit_code=None, base=None, head=None)
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["scanner"]["outcome"], "UNKNOWN")
        for code in ("SCANNER_EXIT_MISSING", "BASE_MISSING", "HEAD_MISSING"):
            self.assertIn(code, result["issues"])
        self.assert_fail(encode([finding(File="../bad")]), "FILE_INVALID", scanner_exit_code=None, base=None)
        self.assert_safe(result)

    def test_invalid_captured_exit_and_revisions_fail_without_echo(self):
        for value in (True, "0", 1.0, 256, -256):
            with self.subTest(kind=type(value).__name__):
                self.assert_fail(b"[]", "SCANNER_EXIT_INVALID", scanner_exit_code=value)
        for field in ("base", "head"):
            for value in (SENTINEL, "0" * 40, "A" * 40, 123):
                self.assert_fail(b"[]", field.upper() + "_INVALID", **{field: value})

    def test_supplied_claims_cannot_authenticate_the_report(self):
        result = validate(encode([finding(scanner_version="8.30.1", redacted=True,
                                          provenance_verified=True, approved=True)]))
        self.assert_safe(result)
        self.assertEqual(result["finding_metadata"], [finding()])

    def test_parser_does_not_open_files_or_run_external_operations(self):
        with patch("builtins.open", side_effect=AssertionError("unexpected file read")), \
                patch("os.system", side_effect=AssertionError("unexpected command")), \
                patch("subprocess.run", side_effect=AssertionError("unexpected subprocess")), \
                patch("socket.socket", side_effect=AssertionError("unexpected network")):
            self.assert_safe(validate(encode([finding()])))


class LocalCliTests(unittest.TestCase):
    def run_cli(self, *args):
        result = subprocess.run([sys.executable, evidence.__file__, *args],
                                capture_output=True, text=True, timeout=5, check=False)
        self.assertEqual(result.stderr, "")
        self.assertNotIn(SENTINEL, result.stdout)
        return result.returncode, json.loads(result.stdout)

    def args(self, path):
        return ("--report", str(path), "--scanner-exit-code", "0", "--base", BASE, "--head", HEAD)

    def test_cli_reads_a_regular_file_but_never_returns_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            report.write_bytes(b"[]")
            code, result = self.run_cli(*self.args(report))
            self.assertEqual(code, 2)
            self.assertEqual(result["status"], "BLOCKED")
            self.assertEqual(result["report_state"], "EXPLICIT_EMPTY")
            self.assertNotIn(str(report), json.dumps(result))

    def test_cli_distinguishes_a_missing_report(self):
        with tempfile.TemporaryDirectory() as directory:
            code, result = self.run_cli(*self.args(Path(directory) / "missing.json"))
            self.assertEqual(code, 2)
            self.assertEqual(result["report_state"], "MISSING")

    def test_cli_rejects_oversized_and_nonregular_files(self):
        with tempfile.TemporaryDirectory() as directory:
            large = Path(directory) / "large.json"
            large.write_bytes(b" " * (evidence.MAX_REPORT_BYTES + 1))
            code, result = self.run_cli(*self.args(large))
            self.assertEqual(code, 1)
            self.assertIn("REPORT_SIZE_EXCEEDED", result["issues"])
            code, result = self.run_cli(*self.args(directory))
            self.assertEqual(code, 1)
            self.assertIn("REGULAR_REPORT_REQUIRED", result["issues"])
            if hasattr(os, "mkfifo"):
                fifo = Path(directory) / "report.pipe"
                os.mkfifo(fifo)
                code, result = self.run_cli(*self.args(fifo))
                self.assertEqual(code, 1)
                self.assertIn("REGULAR_REPORT_REQUIRED", result["issues"])

    @unittest.skipUnless(hasattr(os, "O_NOFOLLOW"), "Safe local open requires O_NOFOLLOW")
    def test_cli_does_not_follow_final_component_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            report.write_bytes(b"[]")
            link = Path(directory) / "link.json"
            link.symlink_to(report)
            code, result = self.run_cli(*self.args(link))
            self.assertNotEqual(code, 0)
            self.assertEqual(result["report_state"], "UNREADABLE")
            self.assertIn("REPORT_UNREADABLE", result["issues"])

    @unittest.skipUnless(hasattr(os, "O_NOFOLLOW"), "Safe local open requires O_NOFOLLOW")
    def test_cli_preserves_nonzero_scanner_exit_with_unreadable_report(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            report.write_bytes(b"[]")
            link = Path(directory) / "link.json"
            link.symlink_to(report)
            args = list(self.args(link))
            args[3] = "7"
            code, result = self.run_cli(*args)
            self.assertEqual(code, 1)
            self.assertEqual(result["scanner"]["exit_code"], 7)
            self.assertEqual(result["scanner"]["outcome"], "EXIT_NONZERO")
            self.assertIn("SCANNER_EXIT_NONZERO", result["issues"])
            self.assertIn("REPORT_UNREADABLE", result["issues"])

    def test_cli_argument_errors_do_not_echo_arguments(self):
        for args in (("--unexpected", SENTINEL), ("--scanner-exit-code", SENTINEL)):
            code, result = self.run_cli(*args)
            self.assertEqual(code, 1)
            self.assertIn("CLI_ARGUMENTS_INVALID", result["issues"])

    def test_unexpected_exceptions_are_suppressed_by_the_cli(self):
        output = io.StringIO()
        with patch.object(evidence, "_read_local_report", side_effect=RuntimeError(SENTINEL)), redirect_stdout(output):
            code = evidence.main(["--report", SENTINEL, "--scanner-exit-code", "-9"])
        result = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertIn("INTERNAL_VALIDATOR_ERROR", result["issues"])
        self.assertEqual(result["scanner"]["exit_code"], -9)
        self.assertNotIn(SENTINEL, output.getvalue())


if __name__ == "__main__":
    unittest.main()
