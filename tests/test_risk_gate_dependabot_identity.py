"""Execute the trusted workflow predicate against synthetic commit metadata."""

import json
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
    def commit(*, author="dependabot[bot]", committer="dependabot[bot]",
               verified=True):
        return {
            "author": {"login": author}, "committer": {"login": committer},
            "commit": {"verification": {"verified": verified}},
        }

    def test_authentic_verified_dependabot_commits_are_eligible(self):
        self.assertTrue(self.evaluate([self.commit(), self.commit()]))

    def test_web_interface_committer_cannot_impersonate_dependabot(self):
        self.assertFalse(self.evaluate([self.commit(committer="web-flow")]))

    def test_unsigned_invalid_or_human_commits_are_ineligible(self):
        for commit in (
            self.commit(verified=False), self.commit(verified="true"),
            self.commit(author="example-human"),
            self.commit(committer="example-human"), {},
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
