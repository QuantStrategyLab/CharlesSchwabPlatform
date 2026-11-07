"""Verify the trusted gate delegates to the shared governance action.

The Dependabot commit-identity predicate and the clearing-label recorder used to
live inside `.github/workflows/risk-gate.yml` and were executed from here. They
now ship with the pinned `QuantStrategyLab/.github/pr-governance` action, where
`scripts/test-gate.cjs` and `scripts/test-labels.cjs` cover them. This suite
keeps the caller-side contract explicit so a migration cannot silently drop the
identity or head-binding guarantees.
"""

import json
import re
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/risk-gate.yml"
SIGNAL = ROOT / ".github/workflows/pr-review-signal.yml"
POLICY = ROOT / ".github/governance/policy.json"


class SharedActionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.workflow = WORKFLOW.read_text()

    def test_every_job_pins_the_same_immutable_revision(self):
        uses = re.findall(
            r"uses: QuantStrategyLab/\.github/pr-governance@([0-9a-f]{40})",
            self.workflow,
        )
        self.assertEqual(len(uses), 2)
        self.assertEqual(len(set(uses)), 1)

    def test_recording_and_evaluation_are_separate_jobs(self):
        recorder, evaluate = self.workflow.split("\n  evaluate:\n", 1)
        self.assertIn("record-label: 'true'", recorder)
        self.assertIn("record-label: 'false'", evaluate)
        # Only a label addition carries authenticated head evidence.
        self.assertIn("github.event.action == 'labeled'", recorder)
        # The record must not ride on an evaluation a later event can replace.
        self.assertNotIn("needs:", recorder)

    def test_review_signals_are_relayed_by_the_permissionless_workflow(self):
        self.assertIn('workflows: ["PR review signal"]', self.workflow)
        signal = SIGNAL.read_text()
        self.assertIn("name: PR review signal", signal)
        self.assertIn("permissions:\n  contents: read\n", signal)
        self.assertNotIn("pull_request_target", signal)

    def test_owner_identity_comes_from_protected_variables(self):
        self.assertEqual(
            self.workflow.count("GOVERNANCE_OWNER_ID: ${{ vars.RISK_GATE_OWNER_ID }}"), 2)
        self.assertEqual(
            self.workflow.count("GOVERNANCE_OWNER_LOGIN: ${{ vars.RISK_GATE_OWNER }}"), 2)
        self.assertNotIn("APPROVAL_OWNER:", self.workflow)

    def test_the_policy_binds_the_codex_reviewer(self):
        policy = json.loads(POLICY.read_text())
        self.assertEqual(policy["version"], "pr-governance/v3")
        self.assertEqual(policy["mode"], "enforce")
        self.assertEqual(policy["reviewer"], {
            "provider": "codex",
            "app_id": 1144995,
            "user_id": 199175422,
            "login": "chatgpt-codex-connector[bot]",
        })

    def test_protected_paths_stay_high_risk(self):
        policy = json.loads(POLICY.read_text())
        self.assertIn("scripts/", policy["r2_paths"])
        self.assertIn(".github/governance/", policy["r2_paths"])
        self.assertNotIn("docs/", policy["r2_paths"])


if __name__ == "__main__":
    unittest.main()
