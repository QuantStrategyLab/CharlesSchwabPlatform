"""Regression tests for the documentation checker.

This checker runs against every repository that adopts the rule set, from one
copy or many, so a rule that silently stops working fails everywhere at once
and nobody notices. Each test here pins one decision that was easy to get
wrong:

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
import io
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
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

    def run_main(self, *extra: str) -> tuple[int, str]:
        with redirect_stdout(io.StringIO()) as captured:
            code = check_style.main(["--root", str(self.root), "--phase", "2", *extra])
        return code, captured.getvalue()


class NamingTest(CheckerTestCase):
    def test_underscore_is_flagged(self):
        self.write("docs/hk_equity_runtime.md", "# x\n")
        report = self.report()
        check_style.check_naming(self.root, report)
        self.assertTrue(any("underscores" in m for m in self.messages(report, "naming")))

    def test_github_templates_keep_their_underscore(self):
        # GitHub only reads pull_request_template.md / issue_template.md with the
        # underscore; renaming them to hyphens disables the template.
        self.write(".github/pull_request_template.md", "## Summary\n")
        self.write(".github/issue_template.md", "---\nname: bug\n---\n")
        report = self.report()
        check_style.check_naming(self.root, report)
        self.assertEqual(self.messages(report, "naming"), [])

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

    def test_root_proposal_has_its_owner_requested_name(self):
        self.write("PROPOSAL.md", "# Proposed governance change\n")
        report = self.report()
        check_style.check_naming(self.root, report)
        self.assertEqual(self.messages(report, "naming"), [])

    def test_nested_proposal_still_requires_lowercase(self):
        self.write("docs/PROPOSAL.md", "# x\n")
        report = self.report()
        check_style.check_naming(self.root, report)
        self.assertTrue(any("lowercase" in m for m in self.messages(report, "naming")))

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

    def test_placeholder_subdomain_is_not_reported(self):
        # Section 13 asks for a placeholder where a deployed subdomain would
        # appear. Reporting the guide's own recommended form is how a check
        # teaches people to ignore it.
        for text in (
            "Open https://YOUR_SUBDOMAIN.workers.dev/admin.\n",
            "Set the host to https://your-subdomain.workers.dev.\n",
            "The example uses https://xxx.workers.dev.\n",
            "Deploy to https://<subdomain>.workers.dev.\n",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.findings(text), [])

    def test_real_subdomain_is_reported(self):
        found = self.findings("Open https://pigbibi.workers.dev/admin.\n")
        self.assertTrue(any("personal subdomain" in m for m in found))

    def test_subdomain_in_a_fenced_block_is_an_example(self):
        # The guide shows forbidden shapes inside fenced blocks. Installing a
        # predicate on the rule must not start reporting those examples, or a
        # narrowing fix silently widens what blocks.
        text = (
            "Do not publish a real host:\n\n"
            "```markdown\n"
            "Do not publish https://demo.workers.dev as a real host.\n"
            "```\n"
        )
        self.assertEqual(self.findings(text), [])

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

    def test_changes_directory_and_obsolete_engineering_rule_are_reported(self):
        self.write(".gitignore", GITIGNORE_BLOCK + "!.github/engineering.md\n")
        self.write("docs/changes/task/change-card.md", "# x\n")
        report = self.report()
        check_style.check_internal_paths(self.root, report)
        self.assertTrue(any("engineering.md" in m for m in self.messages(report, "hygiene")))
        self.assertTrue(any("plan/handoff directory" in m for m in self.messages(report, "content")))

    def test_acceptance_policy_blocks_public_only(self):
        self.write(".gitignore", GITIGNORE_BLOCK)
        self.write("docs/acceptance-policy.md", "# policy\n")
        pub = self.report(phase=2)
        check_style.check_internal_paths(self.root, pub)
        self.assertTrue(any(cat == "publish" for cat, _ in pub.blocking_findings()))
        _, output = self.run_main("--visibility", "private")
        self.assertTrue(
            any(line.startswith("NOTE") and "acceptance-policy.md" in line for line in output.splitlines())
        )


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

    def test_retired_engineering_doc_and_long_agents_are_reported(self):
        long_body = "# rules\n" + "\n".join(f"line {i}" for i in range(125)) + "\n" + check_style.CONTENT_BOUNDARY + "\n"
        self.write("AGENTS.md", long_body)
        self.write("CLAUDE.md", "@AGENTS.md\n")
        self.write(".github/engineering.md", "# legacy\n")
        self.git("init", "--quiet")
        self.git("add", "AGENTS.md", "CLAUDE.md")
        report = self.report()
        check_style.check_agent_files(self.root, report)
        found = self.messages(report, "agents")
        self.assertTrue(any("engineering.md is retired" in m for m in found))
        self.assertTrue(any("exceeds 120 lines" in m for m in found))


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


class AdvisoryModeTest(CheckerTestCase):
    """Adoption has to be possible before compliance.

    A repository adds the check while it still has findings. If that pull
    request goes red, the check is removed instead of the findings fixed, and
    the repository ends up with neither.
    """

    def test_advisory_reports_the_findings_it_would_have_blocked_on(self):
        self.write("docs/hk_equity_runtime.md", "# x\n")
        code, output = self.run_main("--advisory")
        self.assertEqual(code, 0)
        self.assertIn("underscores", output)
        self.assertIn("advisory run", output)

    def test_without_advisory_the_same_findings_still_fail(self):
        self.write("docs/hk_equity_runtime.md", "# x\n")
        code, _ = self.run_main()
        self.assertEqual(code, 1)


class VisibilityTest(CheckerTestCase):
    """The publication rules govern publishing, not existence.

    A private repository publishes nothing, and a deployment document may need
    the real host name to be usable — an OAuth callback URL with a placeholder
    in it cannot be configured. Blocking there would force a document to be
    wrong in order to pass.
    """

    def deploy_doc(self) -> None:
        self.write(
            "docs/deployment.md",
            "Callback: https://invoice.pigbibi.workers.dev/auth/github/callback\n"
            "Data lives in /home/ubuntu/app/data.\n",
        )

    def test_public_repository_blocks_published_identifiers(self):
        self.deploy_doc()
        _, output = self.run_main("--visibility", "public")
        self.assertTrue(
            any(line.startswith("ERROR") and "personal subdomain" in line
                for line in output.splitlines())
        )

    def test_private_repository_reports_them_without_blocking(self):
        self.deploy_doc()
        _, output = self.run_main("--visibility", "private")
        self.assertFalse(
            any(line.startswith("ERROR") and "personal subdomain" in line
                for line in output.splitlines())
        )
        self.assertTrue(
            any(line.startswith("NOTE") and "personal subdomain" in line
                for line in output.splitlines())
        )
        self.assertIn("private repository", output)

    def test_the_default_is_public(self):
        # Forgetting the flag must not switch a leak rule off.
        self.deploy_doc()
        _, output = self.run_main()
        self.assertIn("(public)", output)
        self.assertTrue(
            any(line.startswith("ERROR") and "personal subdomain" in line
                for line in output.splitlines())
        )

    def test_private_still_blocks_a_missing_security_policy(self):
        # Visibility narrows the publication rules only; it is not a blanket
        # exemption for everything the checker knows.
        _, output = self.run_main("--visibility", "private")
        self.assertTrue(
            any(line.startswith("ERROR") and "SECURITY.md" in line
                for line in output.splitlines())
        )




class ReadmeHygieneTest(CheckerTestCase):
    def test_readme_patch_prose_phase_heading_and_post_license_block(self):
        self.write(
            "README.md",
            "# Demo\n\n## P0-P6 Runtime Authority\n\nIn this PR the gate remains unchanged.\n\n## License\n\nMIT\n\n## Weekly secret scan\n\nExtra\n",
        )
        self.write(
            "README.zh-CN.md",
            "# 示例\n\n## 许可证\n\nMIT\n\n## 每周密钥扫描\n\nExtra\n",
        )
        report = self.report(phase=2)
        check_style.check_readme(self.root, report)
        blocking = [m for c, m in report.blocking_findings() if c == "content"]
        self.assertTrue(any("phase code heading" in m for m in blocking))
        self.assertTrue(any("In this PR" in m for m in blocking))
        self.assertTrue(any("after License" in m and "README.md" in m for m in blocking))
        self.assertTrue(any("after License" in m and "README.zh-CN.md" in m for m in blocking))

    def test_clean_readme_and_fenced_examples_pass(self):
        self.write(
            "README.md",
            "# Demo\n\nUses `remains unchanged` in code.\n\n```markdown\nIn this PR\n```\n\n## Weekly secret scan\n\nRuns weekly.\n\n## License\n\nMIT\n",
        )
        report = self.report(phase=2)
        check_style.check_readme(self.root, report)
        self.assertEqual(report.blocking_findings(), [])


class ExampleMentionTest(CheckerTestCase):
    """Rules must not fire on text that is showing what not to do.

    Both cases below came from real repositories: a reference page documenting
    how to attach an image, and a design guide listing the filler words it
    forbids. Each was blocked by the rule it was teaching the reader to obey.
    """

    def test_a_link_inside_inline_code_is_an_example(self):
        self.write(
            "docs/pr-attachments.md",
            "# t\n\nReference it as `![alt](./settings-after.png)` to place it inline.\n",
        )
        report = self.report()
        # The temporary directory is not resolved; check_links resolves the
        # target, so an unresolved root would look like a sibling checkout.
        check_style.check_links(self.root.resolve(), report)
        self.assertEqual(self.messages(report, "links"), [])

    def test_a_real_broken_link_is_still_reported(self):
        self.write("docs/page.md", "# t\n\nSee [the settings](./settings-after.png).\n")
        report = self.report()
        check_style.check_links(self.root.resolve(), report)
        self.assertEqual(len(self.messages(report, "links")), 1)

    def test_a_quoted_word_list_is_not_marketing_language(self):
        self.write(
            "docs/design.md",
            '# t\n\n* **NO filler verbs.** "Elevate", "Seamless", "Unleash"'
            " \u2192 concrete verbs only.\n",
        )
        report = self.report()
        check_style.check_language(self.root, report)
        self.assertEqual(self.messages(report, "language"), [])

    def test_one_quoted_word_is_still_a_claim(self):
        # The exemption is about lists. A single quoted word is a word used.
        self.write("docs/page.md", "# t\n\nOur 'Seamless' onboarding flow.\n")
        report = self.report()
        check_style.check_language(self.root, report)
        self.assertEqual(len(self.messages(report, "language")), 1)


class GitVisibilityTest(CheckerTestCase):
    def test_gitignored_notes_do_not_block_while_staged_notes_do(self):
        self.write(".gitignore", GITIGNORE_BLOCK)
        self.write("AGENTS.md", "# rules\n\n" + check_style.CONTENT_BOUNDARY + "\n")
        self.write("CLAUDE.md", "@AGENTS.md\n")
        self.write("SECURITY.md", "# Security\n")
        self.git("init", "--quiet")
        self.git("config", "user.name", "Test User")
        self.git("config", "user.email", "test@example.com")
        self.git("add", ".gitignore", "AGENTS.md", "CLAUDE.md", "SECURITY.md", "README.md")

        self.write("docs/changes/task/change-card.md", "# Card\n")
        self.write("docs/plans/draft.md", "# Draft\n")
        self.write("docs/acceptance-policy.md", "# Policy\n")

        code, output = self.run_main()
        self.assertEqual(code, 0, msg=output)

        self.git("add", "-f", "docs/changes/task/change-card.md", "docs/acceptance-policy.md")
        blocked_code, blocked_output = self.run_main()
        self.assertEqual(blocked_code, 1, msg=blocked_output)
        self.assertIn("docs/changes/task/change-card.md", blocked_output)
        self.assertIn("docs/acceptance-policy.md", blocked_output)


if __name__ == "__main__":
    unittest.main(verbosity=2)
