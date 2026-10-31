"""Regression tests for the documentation checker.

The checker is copied into 26 repositories, so a rule that silently stops
working fails 26 times at once and nobody notices. Each test here pins one
decision that was easy to get wrong:

  * the leak heuristics must not report the placeholder forms the guide asks
    for, or the report fills with non-defects and gets ignored
  * the .gitignore block must be required, not suggested, or the layer that
    stops a note being committed disappears without a trace
  * phase 1 must stay quiet about naming, or every open pull request goes red
    the day the checker lands

Run with `python scripts/test_style.py`.
"""

from __future__ import annotations

import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "check_style", Path(__file__).with_name("check_style.py")
)
check_style = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_style)

GITIGNORE_BLOCK = "\n".join(check_style.INTERNAL_NOTE_GITIGNORE) + "\n"
SHA = "8e8ec51884bf8abb0a7ca699fc2da39fad134f89"


class CheckerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.root = Path(self._temp.name)
        self.write("README.md", "# t\n")

    def write(self, rel: str, text: str) -> Path:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def git(self, *args: str) -> None:
        subprocess.run(
            ["git", "-C", str(self.root), *args], check=True, capture_output=True
        )

    def report(self, phase: int = 2) -> check_style.Report:
        return check_style.Report(check_style.PHASE_BLOCKING[phase])

    def messages(self, report: check_style.Report, category: str) -> list[str]:
        return [m for c, m in report.findings if c == category]


class NamingTest(CheckerTestCase):
    def test_underscore_is_flagged(self):
        self.write("docs/hk_equity_runtime.md", "# x\n")
        report = self.report()
        check_style.check_naming(self.root, report)
        self.assertTrue(any("underscores" in m for m in self.messages(report, "naming")))

    def test_date_is_flagged_outside_record_paths(self):
        self.write("docs/2026-10-30-release.md", "# x\n")
        report = self.report()
        check_style.check_naming(self.root, report)
        self.assertTrue(any("dates belong" in m for m in self.messages(report, "naming")))

    def test_date_is_allowed_in_record_paths(self):
        self.write("docs/research/2026-10-30-revision.md", "# x\n")
        report = self.report()
        check_style.check_naming(self.root, report)
        self.assertFalse(any("dates belong" in m for m in self.messages(report, "naming")))

    def test_en_suffix_is_flagged(self):
        self.write("docs/runtime-baseline.en.md", "# x\n")
        report = self.report()
        check_style.check_naming(self.root, report)
        self.assertTrue(any(".en.md is not a suffix" in m for m in self.messages(report, "naming")))

    def test_uppercase_convention_is_allowed(self):
        self.write("SECURITY.md", "# x\n")
        report = self.report()
        check_style.check_naming(self.root, report)
        self.assertEqual(self.messages(report, "naming"), [])


class LeakTest(CheckerTestCase):
    def findings(self, text: str) -> list[str]:
        self.write("docs/page.md", text)
        report = self.report()
        check_style.check_leaks(self.root, report)
        return [m for _, m in report.findings]

    def test_home_path_is_reported(self):
        found = self.findings("Config lives in `/Users/someone/projects/app`.\n")
        self.assertTrue(any("absolute home path" in m for m in found))

    def test_placeholder_bucket_is_not_reported(self):
        for text in (
            "Write to `gs://<bucket>/strategy-artifacts/x.csv`.\n",
            "The default is `gs://.../universe.csv`.\n",
            "bucket 一般是 `gs://run-sources-${PROJECT_ID}-${REGION}`\n",
            "Try `gs://your-bucket/path/to/signal.json`.\n",
            "Try `gs://example-bucket/releases/current`.\n",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.findings(text), [])

    def test_real_bucket_is_reported(self):
        found = self.findings("Default root `gs://acme-prod-inputs/research/v2/input/`.\n")
        self.assertTrue(any("non-placeholder bucket" in m for m in found))

    def test_sha_in_url_is_not_reported(self):
        text = (
            "- [Store](https://github.com/QuantStrategyLab/QuantPlatformKit/blob/"
            f"{SHA}/src/store.py)\n"
        )
        self.assertEqual(self.findings(text), [])

    def test_sha_in_code_span_is_not_reported(self):
        self.assertEqual(self.findings(f"Pinned at `{SHA}`.\n"), [])

    def test_sha_in_fenced_block_is_not_reported(self):
        text = f'```json\n{{"config_revision": "{SHA}"}}\n```\n'
        self.assertEqual(self.findings(text), [])

    def test_sha_in_link_label_is_not_reported(self):
        text = f"[{SHA}](https://github.com/QuantStrategyLab/x/commit/{SHA})\n"
        self.assertEqual(self.findings(text), [])

    def test_repeated_character_placeholder_is_not_reported(self):
        self.assertEqual(self.findings(f"`{'0' * 40}`\n"), [])

    def test_bare_sha_in_prose_is_reported(self):
        found = self.findings(f"Merged from PR #712 (head {SHA}, open).\n")
        self.assertTrue(any("40-character hex string in prose" in m for m in found))


class InternalNoteTest(CheckerTestCase):
    def test_missing_gitignore_entries_are_hygiene(self):
        report = self.report()
        check_style.check_internal_paths(self.root, report)
        found = self.messages(report, "hygiene")
        self.assertEqual(len(found), 1)
        self.assertIn("docs/audits/", found[0])

    def test_complete_gitignore_passes(self):
        self.write(".gitignore", GITIGNORE_BLOCK)
        report = self.report()
        check_style.check_internal_paths(self.root, report)
        self.assertEqual(self.messages(report, "hygiene"), [])

    def test_hygiene_blocks_in_phase_one(self):
        report = self.report(phase=1)
        check_style.check_internal_paths(self.root, report)
        self.assertEqual(len(report.blocking_findings()), 1)

    def test_audit_directory_is_reported(self):
        self.write(".gitignore", GITIGNORE_BLOCK)
        self.write("docs/audits/2026-10-30-review.md", "# x\n")
        report = self.report()
        check_style.check_internal_paths(self.root, report)
        self.assertTrue(any("plan/handoff directory" in m for m in self.messages(report, "content")))


class AgentFileTest(CheckerTestCase):
    def test_missing_agents_md_is_reported(self):
        report = self.report()
        check_style.check_agent_files(self.root, report)
        self.assertTrue(any("AGENTS.md is missing" in m for m in self.messages(report, "agents")))

    def test_untracked_agents_md_is_reported(self):
        self.write("AGENTS.md", "# rules\n")
        self.write("CLAUDE.md", "@AGENTS.md\n")
        self.git("init", "--quiet")
        report = self.report()
        check_style.check_agent_files(self.root, report)
        found = self.messages(report, "agents")
        self.assertTrue(any("not tracked by Git" in m for m in found))

    def test_tracked_agents_md_passes(self):
        self.write("AGENTS.md", "# rules\n\n" + check_style.CONTENT_BOUNDARY + "\n")
        self.write("CLAUDE.md", "@AGENTS.md\n")
        self.git("init", "--quiet")
        self.git("add", "AGENTS.md", "CLAUDE.md")
        report = self.report()
        check_style.check_agent_files(self.root, report)
        self.assertEqual(self.messages(report, "agents"), [])

    def test_content_boundary_block_is_required(self):
        # A rule that lives only in the guide is read by whoever opens the
        # guide. The block is the copy that reaches an agent before it writes,
        # so its absence is a finding rather than a preference.
        self.write("AGENTS.md", "# rules\n")
        self.write("CLAUDE.md", "@AGENTS.md\n")
        self.git("init", "--quiet")
        self.git("add", "AGENTS.md", "CLAUDE.md")
        report = self.report()
        check_style.check_agent_files(self.root, report)
        self.assertTrue(
            any("content-boundary" in m for m in self.messages(report, "agents"))
        )

    def test_claude_must_be_the_single_line(self):
        self.write("AGENTS.md", "# rules\n")
        self.write("CLAUDE.md", "# rules\n\ncopied body\n")
        self.git("init", "--quiet")
        self.git("add", "AGENTS.md", "CLAUDE.md")
        report = self.report()
        check_style.check_agent_files(self.root, report)
        self.assertTrue(any("@AGENTS.md" in m for m in self.messages(report, "agents")))


class LanguageTest(CheckerTestCase):
    """Section 2 — the word list reports usage, not mention."""

    def test_marketing_word_in_prose_is_reported(self):
        self.write("docs/note.md", "# Note\n\nA seamless handover.\n")
        report = self.report()
        check_style.check_language(self.root, report)
        self.assertTrue(any("seamless" in m for m in self.messages(report, "language")))

    def test_marketing_word_in_a_code_span_is_a_mention(self):
        # The guide writes the words it forbids. Reporting them there makes the
        # guide the largest offender in the organisation.
        self.write("docs/note.md", "# Note\n\nAvoid `seamless` and `world-class`.\n")
        report = self.report()
        check_style.check_language(self.root, report)
        self.assertEqual(self.messages(report, "language"), [])

    def test_marketing_word_in_a_fenced_block_is_a_mention(self):
        self.write("docs/note.md", "# Note\n\n```markdown\nA seamless handover.\n```\n")
        report = self.report()
        check_style.check_language(self.root, report)
        self.assertEqual(self.messages(report, "language"), [])

    def test_technical_terms_are_not_marketing(self):
        # A robust estimator and statistical power are terms of art; the list
        # that flagged them reported twelve non-defects in the research docs.
        self.write(
            "docs/note.md",
            "# Note\n\nThe robust plateau holds, and the test is more powerful "
            "than the Reality Check.\n",
        )
        report = self.report()
        check_style.check_language(self.root, report)
        self.assertEqual(self.messages(report, "language"), [])

    def test_dated_section_heading_is_not_a_dated_title(self):
        self.write(
            "docs/note.md",
            "# Contract acceptance\n\n## Bounded run acceptance (2026-10-05)\n",
        )
        report = self.report()
        check_style.check_language(self.root, report)
        self.assertEqual(self.messages(report, "language"), [])

    def test_dated_document_title_is_reported(self):
        self.write("docs/note.md", "# Console audit, 2026-10-15\n")
        report = self.report()
        check_style.check_language(self.root, report)
        self.assertTrue(any("dated title" in m for m in self.messages(report, "language")))


class CrossRepositoryTest(CheckerTestCase):
    def test_cross_repository_reference_never_blocks(self):
        # The scheduled link job resolves these against the sibling's default
        # branch. A finding that says "not checked" cannot block a merge.
        # Two levels up: one level stays inside the repository, and on macOS
        # `/tmp` resolves through a symlink, so a one-level fixture passed here
        # and failed on the Linux runner.
        self.write("docs/note.md", "# Note\n\n[peer](../../Sibling/docs/x.md)\n")
        report = self.report(phase=2)
        check_style.check_links(self.root, report)
        self.assertTrue(any(c == "xref" for c, _ in report.findings))
        self.assertEqual(report.blocking_findings(), [])


class DocsBaselineTest(CheckerTestCase):
    def test_public_repository_needs_the_reader_file_set(self):
        self.write("LICENSE", "MIT\n")
        report = self.report()
        check_style.check_docs_baseline(self.root, report)
        found = self.messages(report, "docs")
        self.assertTrue(any("CONTRIBUTING.md" in m for m in found))
        self.assertTrue(any("CODE_OF_CONDUCT.md" in m for m in found))

    def test_public_repository_with_the_file_set_passes(self):
        self.write("LICENSE", "MIT\n")
        for name in ("CONTRIBUTING.md", "SECURITY.md", "CODE_OF_CONDUCT.md"):
            self.write(name, "# " + name + "\n")
        report = self.report()
        check_style.check_docs_baseline(self.root, report)
        self.assertEqual(self.messages(report, "docs"), [])

    def test_private_repository_needs_only_the_credential_doc(self):
        # No LICENSE means the repository grants nothing to outside readers, so
        # a public disclosure process would be a document nobody runs.
        self.write("SECURITY.md", "# Credentials\n")
        report = self.report()
        check_style.check_docs_baseline(self.root, report)
        self.assertEqual(self.messages(report, "docs"), [])

    def test_private_repository_without_the_credential_doc_is_reported(self):
        report = self.report()
        check_style.check_docs_baseline(self.root, report)
        self.assertTrue(any("SECURITY.md" in m for m in self.messages(report, "docs")))


class PhaseTest(CheckerTestCase):
    def test_phase_one_does_not_block_naming(self):
        self.write("docs/hk_equity_runtime.md", "# x\n")
        report = self.report(phase=1)
        check_style.check_naming(self.root, report)
        self.assertTrue(report.findings)
        self.assertEqual(report.blocking_findings(), [])

    def test_phase_two_blocks_naming(self):
        self.write("docs/hk_equity_runtime.md", "# x\n")
        report = self.report(phase=2)
        check_style.check_naming(self.root, report)
        self.assertTrue(report.blocking_findings())


if __name__ == "__main__":
    unittest.main(verbosity=2)
