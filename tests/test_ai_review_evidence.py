"""Synthetic tests for local review-evidence consistency checks."""

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ai_review_evidence.py"
SPEC = importlib.util.spec_from_file_location("ai_review_evidence", SCRIPT)
evidence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evidence)
BASE = "a" * 40
HEAD = "b" * 40


def scope_digest(document):
    scope = {
        key: document[key]
        for key in ("base_sha", "head_sha", "changed_files", "inventory_complete")
    }
    scope["files"] = sorted(document["files"], key=lambda item: item["path"])
    return hashlib.sha256(
        json.dumps(scope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def fixture():
    document = {
        "schema_version": 1,
        "base_sha": BASE,
        "head_sha": HEAD,
        "changed_files": 1,
        "inventory_complete": True,
        "files": [{"path": "docs/review-evidence.md", "status": "added"}],
        "author_id": "author-agent",
        "required_checks": ["unit-tests", "lint"],
        "checks": [
            {"observation_id": "check-1", "name": "unit-tests", "head_sha": HEAD,
             "status": "passed"},
            {"observation_id": "check-2", "name": "lint", "head_sha": HEAD,
             "status": "passed"},
        ],
        "review": {
            "reviewer_id": "review-agent",
            "base_sha": BASE,
            "head_sha": HEAD,
            "scope_sha256": "",
            "status": "completed",
            "findings": [],
        },
    }
    document["review"]["scope_sha256"] = scope_digest(document)
    return document


def replace_files(document, files):
    document["files"] = files
    document["changed_files"] = len(files)
    document["review"]["scope_sha256"] = scope_digest(document)


class ReviewEvidenceTests(unittest.TestCase):
    def assert_result(self, document, status, reason=None):
        result = evidence.validate_evidence(document)
        self.assertEqual(result["status"], status, result)
        self.assertEqual(result["authentication"], "UNVERIFIED")
        self.assertEqual(result["required_check_coverage"], "UNVERIFIED")
        self.assertEqual(result["operational_authority"], "NONE")
        if reason:
            self.assertIn(reason, result["reason_codes"])
        return result

    def test_consistent_names_and_passed_checks_never_authenticate_a_review(self):
        result = self.assert_result(fixture(), "BLOCKED", "trusted_provenance_unavailable")
        self.assertEqual(result["structural_status"], "CONSISTENT")
        self.assertEqual(result["risk_tier"], "low")

    def test_caller_flags_and_actor_claims_are_rejected_without_echo(self):
        for key in ("independent", "authenticated", "owner_approved", "github_actor"):
            document = fixture()
            document[key] = "sensitive-synthetic-marker"
            result = self.assert_result(document, "FAIL", "invalid_schema")
            self.assertNotIn("sensitive-synthetic-marker", json.dumps(result))

    def test_exact_revision_binding_rejects_invalid_or_equal_revisions(self):
        for value in ("B" * 40, "b" * 39, "b" * 41, 40, True, "b" * 39 + "g", "0" * 40):
            document = fixture()
            document["head_sha"] = value
            self.assert_result(document, "FAIL", "invalid_revision")
        document = fixture()
        document["base_sha"] = HEAD
        self.assert_result(document, "FAIL", "invalid_revision_range")

    def test_null_revision_is_invalid_in_every_observation(self):
        for location in ("base", "check", "review", "finding"):
            document = fixture()
            if location == "base":
                document["base_sha"] = "0" * 40
            elif location == "check":
                document["checks"][0]["head_sha"] = "0" * 40
            elif location == "review":
                document["review"]["head_sha"] = "0" * 40
            else:
                document["review"]["findings"] = [
                    {"id": "F-1", "severity": "low", "disposition": "open", "head_sha": "0" * 40}
                ]
            expected = {"base": "invalid_revision", "check": "invalid_checks",
                        "review": "invalid_review", "finding": "invalid_findings"}
            result = self.assert_result(document, "FAIL", expected[location])
            self.assertEqual(result["structural_status"], "INCONSISTENT")

    def test_inventory_count_must_match_complete_scope(self):
        for count in (0, 2, -1, True, 1.0, 3001):
            document = fixture()
            document["changed_files"] = count
            self.assert_result(document, "FAIL", "invalid_inventory")

    def test_incomplete_inventory_is_never_accepted(self):
        document = fixture()
        document["inventory_complete"] = False
        document["review"]["scope_sha256"] = scope_digest(document)
        self.assert_result(document, "BLOCKED", "inventory_incomplete")

    def test_duplicate_inventory_paths_and_missing_rename_source_fail(self):
        cases = [
            [{"path": "docs/a.md", "status": "added"}] * 2,
            [{"path": "docs/a.md", "status": "renamed"}],
            [{"path": "docs/a.md", "status": "modified", "previous_path": "docs/b.md"}],
            [{"path": "docs/a.md", "status": "renamed", "previous_path": "docs/a.md"}],
        ]
        for files in cases:
            document = fixture()
            replace_files(document, files)
            self.assert_result(document, "FAIL", "invalid_inventory")

    def test_unsafe_paths_fail_for_current_and_previous_names(self):
        paths = ["../a", "/a", "a/../b", "a//b", "./a", "C:/a", "a\\b",
                 "a\nb", "a\x00b", "a\u202eb", "a b", "a/."]
        for path in paths:
            for field in ("path", "previous_path"):
                document = fixture()
                item = {"path": "docs/new.md", "status": "renamed",
                        "previous_path": "docs/old.md"}
                item[field] = path
                replace_files(document, [item])
                self.assert_result(document, "FAIL")

    def test_risk_uses_whole_pr_and_previous_paths(self):
        document = fixture()
        replace_files(document, [
            {"path": "application/execution_service.py", "status": "modified"},
            {"path": "docs/review-evidence.md", "status": "added"},
        ])
        self.assertEqual(self.assert_result(document, "BLOCKED")["risk_tier"], "high")
        replace_files(document, [{"path": "docs/archived.md", "status": "renamed",
                                  "previous_path": ".github/workflows/release.yml"}])
        self.assertEqual(self.assert_result(document, "BLOCKED")["risk_tier"], "high")

    def test_review_of_latest_delta_cannot_cover_whole_pr(self):
        document = fixture()
        document["files"].append({"path": "strategy_runtime.py", "status": "modified"})
        document["changed_files"] = 2
        self.assert_result(document, "FAIL", "review_scope_mismatch")

    def test_scope_digest_includes_base_status_and_rename_source(self):
        for field, value in (("base_sha", "c" * 40),):
            document = fixture()
            document[field] = value
            document["review"][field] = value
            self.assert_result(document, "FAIL", "review_scope_mismatch")
        document = fixture()
        document["files"][0]["status"] = "modified"
        self.assert_result(document, "FAIL", "review_scope_mismatch")
        document = fixture()
        replace_files(document, [{"path": "docs/new.md", "status": "renamed",
                                  "previous_path": "docs/old.md"}])
        document["files"][0]["previous_path"] = "docs/another.md"
        self.assert_result(document, "FAIL", "review_scope_mismatch")

    def test_tiers_are_computed_and_cannot_be_downgraded(self):
        paths = {
            "scripts/ai_review_evidence.py": "standard",
            "scripts/gitleaks_report_evidence.py": "standard",
            "tests/test_execution.py": "low",
            ".github/engineering.md": "standard",
            "scripts/general_helper.py": "standard",
            "application/client.py": "high",
            "scripts/setRiskControls.py": "high",
            ".github/workflows/ci.yml": "high",
            ".github/CODEOWNERS": "high",
        }
        for path, tier in paths.items():
            document = fixture()
            replace_files(document, [{"path": path, "status": "modified"}])
            self.assertEqual(self.assert_result(document, "BLOCKED")["risk_tier"], tier)
        document["declared_risk_tier"] = "low"
        result = self.assert_result(document, "FAIL", "risk_tier_understated")
        self.assertEqual(result["risk_tier"], "high")
        document = fixture()
        document["declared_risk_tier"] = "high"
        self.assertEqual(self.assert_result(document, "BLOCKED")["risk_tier"], "high")

    def test_executable_examples_and_infrastructure_have_no_low_risk_exemption(self):
        for path, tier in (
            ("examples/basic.py", "standard"),
            ("examples/send_order.py", "high"),
            ("fixtures/update_runtime.py", "high"),
            ("modules/service.tf", "high"),
            ("docker-compose.yml", "high"),
            ("compose.yaml", "high"),
            ("scripts/rotate_token.py", "high"),
            ("scripts/trade.py", "high"),
        ):
            with self.subTest(path=path):
                document = fixture()
                replace_files(document, [{"path": path, "status": "modified"}])
                self.assertEqual(self.assert_result(document, "BLOCKED")["risk_tier"], tier)

    def test_inventory_order_does_not_change_review_scope(self):
        document = fixture()
        replace_files(document, [{"path": "docs/b.md", "status": "added"},
                                 {"path": "docs/a.md", "status": "added"}])
        document["files"].reverse()
        self.assert_result(document, "BLOCKED")

    def test_gate_policy_and_rename_sources_raise_the_whole_pr_tier(self):
        for path in (".github/codex_auto_merge_policy.json", "CODEOWNERS"):
            document = fixture()
            replace_files(document, [{"path": path, "status": "modified"}])
            self.assertEqual(self.assert_result(document, "BLOCKED")["risk_tier"], "high")
            replace_files(document, [{"path": "docs/old-policy.json", "status": "renamed",
                                     "previous_path": path}])
            self.assertEqual(self.assert_result(document, "BLOCKED")["risk_tier"], "high")

    def test_known_root_runtime_entrypoints_raise_current_and_previous_path_risk(self):
        for path in ("main.py", "gunicorn.conf.py"):
            for state in ("modified", "renamed", "copied"):
                with self.subTest(path=path, state=state):
                    document = fixture()
                    item = {"path": path, "status": state}
                    if state in {"renamed", "copied"}:
                        item = {"path": "docs/archived-entrypoint.py", "status": state,
                                "previous_path": path}
                    replace_files(document, [item])
                    result = self.assert_result(document, "BLOCKED")
                    self.assertEqual(result["risk_tier"], "high")
                    self.assertEqual(result["required_review_depth"],
                                     "independent_ai_review_with_boundary_and_failure_analysis")

    def test_missing_or_nonpassing_checks_remain_blocked(self):
        document = fixture()
        document["checks"] = []
        self.assert_result(document, "BLOCKED", "required_check_missing")
        for status in ("pending", "error", "skipped", "missing"):
            document = fixture()
            document["checks"][0]["status"] = status
            self.assert_result(document, "BLOCKED", "check_not_successful")

    def test_failed_and_contradictory_observations_fail_at_every_tier(self):
        for tier in ("low", "standard", "high"):
            document = fixture()
            document["declared_risk_tier"] = tier
            document["checks"][0]["status"] = "failed"
            self.assert_result(document, "FAIL", "check_failed")
            document["checks"].append({"observation_id": "check-3", "name": "unit-tests",
                                       "head_sha": HEAD, "status": "passed"})
            self.assert_result(document, "FAIL", "contradictory_check_observations")

    def test_unlisted_failed_observation_is_not_hidden(self):
        document = fixture()
        document["checks"].append({"observation_id": "extra", "name": "security",
                                   "head_sha": HEAD, "status": "failed"})
        self.assert_result(document, "FAIL", "check_failed")

    def test_checks_cannot_use_stale_head_or_duplicate_observation_ids(self):
        document = fixture()
        document["checks"][0]["head_sha"] = BASE
        self.assert_result(document, "FAIL", "check_head_mismatch")
        document = fixture()
        document["checks"][1]["observation_id"] = "check-1"
        self.assert_result(document, "FAIL", "invalid_checks")

    def test_empty_check_policy_does_not_prove_coverage(self):
        document = fixture()
        document["required_checks"] = []
        self.assert_result(document, "BLOCKED", "required_checks_missing")

    def test_review_requires_distinct_identity_exact_head_and_base(self):
        document = fixture()
        document["review"]["reviewer_id"] = document["author_id"]
        self.assert_result(document, "FAIL", "reviewer_matches_author")
        for field in ("head_sha", "base_sha"):
            document = fixture()
            document["review"][field] = "c" * 40
            self.assert_result(document, "FAIL", "review_revision_mismatch")

    def test_review_status_is_not_upgraded_to_completed(self):
        for status in ("pending", "error", "skipped", "missing"):
            document = fixture()
            document["review"]["status"] = status
            self.assert_result(document, "BLOCKED", "review_not_completed")
        document["review"]["status"] = "failed"
        self.assert_result(document, "FAIL", "review_failed")

    def test_finding_disposition_and_resolution_require_head_bound_evidence(self):
        document = fixture()
        finding = {"id": "F-1", "severity": "high", "disposition": "open", "head_sha": HEAD}
        document["review"]["findings"] = [finding]
        self.assert_result(document, "FAIL", "finding_open")
        finding["disposition"] = "resolved"
        self.assert_result(document, "FAIL", "finding_not_verified")
        finding["verification_check"] = "unit-tests"
        self.assert_result(document, "BLOCKED")
        finding["head_sha"] = BASE
        self.assert_result(document, "FAIL", "finding_revision_mismatch")

    def test_unknown_disposition_or_review_authentication_claim_is_rejected(self):
        document = fixture()
        document["review"]["independent"] = True
        self.assert_result(document, "FAIL", "invalid_schema")
        document = fixture()
        document["review"]["findings"] = [
            {"id": "F-1", "severity": "critical", "disposition": "accepted_risk", "head_sha": HEAD}
        ]
        self.assert_result(document, "FAIL", "invalid_findings")

    def test_input_is_not_mutated(self):
        document = fixture()
        before = copy.deepcopy(document)
        self.assert_result(document, "BLOCKED")
        self.assertEqual(document, before)

    def test_non_json_types_deep_inputs_and_nonfinite_values_are_rejected(self):
        for value in (None, [], {"bad": float("nan")}, {"bad": float("inf")}, {"bad": object()}):
            self.assert_result(value, "FAIL")
        value = "leaf"
        for _ in range(20):
            value = [value]
        self.assert_result(value, "FAIL", "input_limits_exceeded")


class LocalFileTests(unittest.TestCase):
    def run_cli(self, path):
        return subprocess.run([sys.executable, str(SCRIPT), str(path)], capture_output=True,
                              text=True, timeout=5, check=False)

    def test_cli_blocks_consistent_input_and_outputs_only_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            path.write_text(json.dumps(fixture()), encoding="utf-8")
            run = self.run_cli(path)
            self.assertEqual(run.returncode, 2, run.stderr)
            self.assertEqual(run.stderr, "")
            result = json.loads(run.stdout)
            self.assertEqual(result["status"], "BLOCKED")
            self.assertEqual(result["structural_status"], "CONSISTENT")

    def test_duplicate_nonfinite_malformed_and_oversized_json_fail_without_echo(self):
        cases = [
            b'{"head_sha":"synthetic-secret","head_sha":"again"}',
            b'{"value":NaN}', b'{"value":Infinity}', b'{"value":1e999}',
            b'{"synthetic-secret":', b'\xff', b" " * (262144 + 1),
            b"[" * 1000 + b"0" + b"]" * 1000,
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            for raw in cases:
                path.write_bytes(raw)
                run = self.run_cli(path)
                self.assertEqual(run.returncode, 1, run.stderr)
                self.assertEqual(json.loads(run.stdout)["status"], "FAIL")
                self.assertNotIn("synthetic-secret", run.stdout + run.stderr)
                self.assertNotIn("Traceback", run.stdout + run.stderr)

    def test_directories_symlinks_and_fifos_fail_without_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            file = root / "input.json"
            file.write_text(json.dumps(fixture()), encoding="utf-8")
            link = root / "link.json"
            link.symlink_to(file)
            fifo = root / "fifo.json"
            os.mkfifo(fifo)
            for path in (root, link, fifo):
                run = self.run_cli(path)
                self.assertEqual(run.returncode, 1)
                self.assertEqual(json.loads(run.stdout)["status"], "FAIL")
                self.assertNotIn(str(path), run.stdout + run.stderr)

    def test_missing_file_is_blocked_as_unavailable_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing-synthetic-secret"
            run = self.run_cli(path)
            self.assertEqual(run.returncode, 2)
            result = json.loads(run.stdout)
            self.assertEqual(result["status"], "BLOCKED")
            self.assertEqual(result["structural_status"], "UNVERIFIED")
            self.assertEqual(result["reason_codes"], ["input_file_unavailable"])
            self.assertNotIn(str(path), run.stdout + run.stderr)

    def test_cli_does_not_echo_invalid_arguments_or_use_stdin(self):
        for arguments in ([], ["--synthetic-secret"], ["a", "b"], ["-"]):
            run = subprocess.run([sys.executable, str(SCRIPT), *arguments],
                                 input="synthetic-secret", capture_output=True, text=True,
                                 timeout=5, check=False)
            self.assertEqual(run.returncode, 1)
            self.assertEqual(json.loads(run.stdout)["status"], "FAIL")
            self.assertNotIn("synthetic-secret", run.stdout + run.stderr)

    def test_help_is_usage_only_and_does_not_claim_evidence_acceptance(self):
        run = subprocess.run([sys.executable, str(SCRIPT), "--help"], capture_output=True,
                             text=True, timeout=5, check=False)
        self.assertEqual(run.returncode, 0)
        self.assertIn("Usage:", run.stdout)
        self.assertNotIn('"status":', run.stdout)
        self.assertEqual(run.stderr, "")


if __name__ == "__main__":
    unittest.main()
