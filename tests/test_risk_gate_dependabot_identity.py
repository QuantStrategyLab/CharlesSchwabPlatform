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


if __name__ == "__main__":
    unittest.main()
