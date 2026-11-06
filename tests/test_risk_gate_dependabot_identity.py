"""Execute the trusted workflow predicate against synthetic commit metadata."""

import json
import copy
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DependabotIdentityTests(unittest.TestCase):
    def evaluate(self, commits=None, *, transport_error=False):
        workflow = (ROOT / ".github/workflows/risk-gate.yml").read_text()
        start = workflow.index("async function dependabotHead(prNumber) {")
        end = workflow.index("async function eligibleActionFiles(baseRef)", start)
        predicate = workflow[start:end]
        fixture = json.dumps({"commits": commits, "transportError": transport_error})
        script = """
const fixture = JSON.parse(process.argv[1]);
const repo = {owner: 'example-org', repo: 'example-repo'};
const github = {
  rest: {pulls: {listCommits: 'listCommits'}},
  paginate: async () => {
    if (fixture.transportError) throw new Error('Synthetic transport failure');
    return fixture.commits;
  },
};
""" + predicate + """
dependabotHead(1).then(value => process.stdout.write(JSON.stringify(value)))
  .catch(() => process.exit(2));
"""
        result = subprocess.run(
            ["node", "-e", script, fixture], capture_output=True, text=True,
            timeout=10, check=False,
        )
        if transport_error:
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")
            return None
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    @staticmethod
    def commit(*, author="dependabot[bot]", committer="web-flow",
               email="noreply@github.com", verified=True):
        return {
            "author": {"login": author}, "committer": {"login": committer},
            "commit": {
                "committer": {"email": email},
                "verification": {"verified": verified},
            },
        }

    def test_authentic_verified_dependabot_commits_are_eligible(self):
        self.assertTrue(self.evaluate([self.commit(), self.commit()]))

    def test_a_commit_outside_the_bot_identity_is_ineligible(self):
        # Dependabot's own commits are authored by the bot and committed by
        # GitHub as `web-flow` from `noreply@github.com`; anything else either
        # was not the bot or was not made by GitHub for it.
        for commit in (
            self.commit(author="example-human"),
            self.commit(committer="dependabot[bot]"),
            self.commit(committer="example-human"),
            self.commit(email="example@example.com"),
            self.commit(email=None),
            self.commit(verified=False),
            self.commit(verified="true"),
            {},
            {"author": {"login": "dependabot[bot]"}},
        ):
            with self.subTest(commit=commit):
                self.assertFalse(self.evaluate([self.commit(), commit]))

    def test_empty_or_truncated_history_is_ineligible(self):
        for commits in ([], [self.commit()] * 250):
            with self.subTest(count=len(commits)):
                self.assertFalse(self.evaluate(commits))

    def test_transport_failure_remains_an_error(self):
        self.evaluate(transport_error=True)


class ClearingLabelRecorderTests(unittest.TestCase):
    def fixture(self):
        return {
            "owner": "example-approver", "sha": "head-a", "number": 7,
            "payload": {
                "label": {"name": "agent-approved"},
                "sender": {"login": "example-approver"},
                "pull_request": {
                    "head": {"sha": "head-a"}, "state": "open", "number": 7,
                    "updated_at": "2026-01-01T00:00:00Z",
                },
                "private_value": "synthetic-secret-must-not-appear",
            },
            "events": [{
                "id": 11, "event": "labeled", "label": {"name": "agent-approved"},
                "actor": {"login": "example-approver"},
                "created_at": "2026-01-01T00:00:00Z",
                "private_value": "synthetic-secret-must-not-appear",
            }],
        }

    def evaluate(self, fixture):
        workflow = (ROOT / ".github/workflows/risk-gate.yml").read_text()
        recorder = workflow.split("          script: |\n", 1)[1].split("\n  evaluate:", 1)[0]
        script = """
const fixture = JSON.parse(process.argv[1]);
process.env.APPROVAL_OWNER = fixture.owner;
const context = {payload: fixture.payload, sha: fixture.sha,
  issue: {number: fixture.number}, repo: {owner: 'example-org', repo: 'example-repo'}};
const result = {info: [], warnings: [], writes: [], reads: []};
const core = {info: value => result.info.push(value), warning: value => result.warnings.push(value)};
const github = {
  paginate: async (_, args) => {
    result.reads.push(args);
    if (fixture.readError) throw new Error('Synthetic failure');
    return fixture.events;
  },
  rest: {issues: {listEvents: 'listEvents'}, repos: {createCommitStatus: async args => {
    if (fixture.writeError) throw new Error('Synthetic failure');
    result.writes.push(args);
    if (fixture.writeThenError) throw new Error('Synthetic response loss');
  }}},
};
(async () => {
  try { await (async () => {
""" + recorder + """
  })(); } catch (_) { result.failed = true; }
  process.stdout.write(JSON.stringify(result));
})();
"""
        result = subprocess.run(
            ["node", "-e", script, json.dumps(fixture)], capture_output=True,
            text=True, timeout=10, check=True,
        )
        self.assertNotIn("synthetic-secret-must-not-appear", result.stdout)
        return json.loads(result.stdout)

    def test_noop_guards_log_reason_and_warn_only_for_clearing_labels(self):
        cases = (
            ("non_clearing_label", ("label", "name"), "risk:high"),
            ("approver_mismatch", ("sender", "login"), "another-actor"),
            ("missing_head", ("pull_request", "head", "sha"), None),
            ("pr_not_open", ("pull_request", "state"), "closed"),
            ("missing_pr_number", ("pull_request", "number"), None),
            ("missing_updated_at", ("pull_request", "updated_at"), None),
        )
        for reason, path, value in cases:
            with self.subTest(reason=reason):
                fixture = self.fixture()
                target = fixture["payload"]
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                result = self.evaluate(fixture)
                self.assertEqual(result["info"], [f"clearing-label-recorder reason={reason}"])
                self.assertEqual(len(result["warnings"]), reason != "non_clearing_label")
                self.assertEqual(result["writes"], [])
                self.assertEqual(result["reads"], [])
        fixture = self.fixture()
        fixture["owner"] = ""
        self.assertIn("approver_mismatch", self.evaluate(fixture)["warnings"][0])

    def test_exact_event_binding_is_preserved(self):
        for change in ("timestamp", "actor", "label", "event", "duplicate", "empty"):
            with self.subTest(change=change):
                fixture = self.fixture()
                if change == "timestamp":
                    fixture["events"][0]["created_at"] = "2026-01-01T00:00:01Z"
                elif change == "actor":
                    fixture["events"][0]["actor"]["login"] = "another-actor"
                elif change == "label":
                    fixture["events"][0]["label"]["name"] = "owner-approved"
                elif change == "event":
                    fixture["events"][0]["event"] = "unlabeled"
                elif change == "duplicate":
                    fixture["events"].append(copy.deepcopy(fixture["events"][0]))
                else:
                    fixture["events"] = []
                result = self.evaluate(fixture)
                self.assertEqual(result["writes"], [])
                self.assertIn("no_status_record reason=match_count_ne_1", result["warnings"][0])
                evidence = json.loads(result["info"][0].split("event_match=", 1)[1])
                self.assertEqual(evidence["match_count"], 2 if change == "duplicate" else 0)
                self.assertEqual(set(evidence), {
                    "payload_updated_at", "match_count", "head_equal", "pr_number_equal",
                })
                if change == "timestamp":
                    candidate = json.loads(result["info"][1].split("event_candidate=", 1)[1])
                    self.assertEqual(candidate["issue_event_created_at"], "2026-01-01T00:00:01Z")
                    self.assertEqual(set(candidate), set(evidence) | {"issue_event_created_at"})

    def test_success_still_binds_payload_head_pr_and_event_id(self):
        fixture = self.fixture()
        # A newer event must never replace the event matching this delivery.
        newer = copy.deepcopy(fixture["events"][0])
        newer.update(id=12, created_at="2026-01-01T00:00:01Z")
        fixture["events"].extend([dict(newer, event="unlabeled"), newer])
        fixture.update(sha="base-head", number=8)
        result = self.evaluate(fixture)
        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["writes"], [{
            "owner": "example-org", "repo": "example-repo", "sha": "head-a",
            "context": "clearing-label", "state": "success",
            "description": "agent-approved for PR 7 at event 11 on this head",
        }])
        evidence = json.loads(result["info"][0].split("event_match=", 1)[1])
        self.assertFalse(evidence["head_equal"])
        self.assertFalse(evidence["pr_number_equal"])

    def test_api_failures_warn_and_remain_errors(self):
        for failure, reason in (("readError", "event_read_failed"),
                                ("writeError", "status_write_failed"),
                                ("writeThenError", "status_write_failed")):
            with self.subTest(failure=failure):
                fixture = self.fixture()
                fixture[failure] = True
                result = self.evaluate(fixture)
                self.assertTrue(result["failed"])
                self.assertEqual(len(result["writes"]), failure == "writeThenError")
                outcome = "no_status_record" if failure == "readError" else "status_write_unconfirmed"
                self.assertIn(f"{outcome} reason={reason}", result["warnings"][0])


if __name__ == "__main__":
    unittest.main()
