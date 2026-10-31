#!/usr/bin/env python3
"""Check a repository against docs/STYLE_GUIDE.md.

The style guide is 500 lines of prose and nothing enforced it. This script turns
the mechanically checkable parts into exit codes, so a pull request that breaks
them fails instead of relying on a reviewer remembering section 7.

Checks come from the guide, not from invention:

  section 2   marketing words, filler openings, emoji in headings
  section 4   language suffixes
  section 5   content that stays out of public repositories
  section 6   internal note paths
  section 7   file naming
  section 13  the grep list the guide already tells reviewers to run

Usage:
    python scripts/check_style.py [--root PATH] [--phase {1,2}]

Phases exist because 26 repositories already carry violations. Turning all of
them into blocking failures on day one means every open pull request goes red
and the check gets deleted. Phase 1 blocks only the defects that leak private
data or break a reader's navigation; naming and README structure are reported
but do not fail. Phase 2 blocks everything, and should be switched on once the
naming cleanup is done.

Exit code 0 when nothing in the blocking set is found.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import unicodedata
import urllib.parse
from pathlib import Path
from typing import NamedTuple

SKIP_DIRS = {
    ".git", "node_modules", ".venv", "venv", "__pycache__", "build", "dist",
    ".gradle", ".mypy_cache", ".pytest_cache", ".ruff_cache", "site-packages",
    ".idea", ".vscode",
}

# Section 7: these keep their uppercase spelling because tools and readers
# expect them.
CONVENTIONAL_UPPER = {
    "README", "CHANGELOG", "SECURITY", "CONTRIBUTING", "CODE_OF_CONDUCT",
    "LICENSE", "SUPPORT", "GOVERNANCE", "AUTHORS", "NOTICE", "AGENTS",
    "CLAUDE", "THIRD_PARTY_NOTICES", "TRADEMARK", "ARCHITECTURE",
    "CONFIGURATION", "GLOSSARY", "STYLE_GUIDE", "PULL_REQUEST_TEMPLATE",
    "ISSUE_TEMPLATE", "FUNDING", "SKILL", "INDEX",
}

DATE_IN_NAME = re.compile(r"(?<!\d)20\d{2}[-_]?\d{2}[-_]?\d{2}(?!\d)")

# The block `tools/sync_agents.py` writes into AGENTS.md. A rule stated only in
# the guide is read by whoever opens the guide; this one is read by every agent
# that opens the repository, which is the only point early enough to matter.
CONTENT_BOUNDARY = "<!-- content-boundary:begin -->"

# A date in the name is a defect when the file describes something that keeps
# changing, because the date goes stale while the name stays. It is the
# identity of the record when the file is a snapshot of one date: two analyses
# of the same subject from different months are different documents, and
# dropping the date leaves them fighting over one name.
#
# Section 7 therefore exempts the two places that hold records. ADRs carry a
# sequence number rather than a date, but they are the same kind of thing.
DATE_BEARING_PATHS = ("docs/adr/", "docs/research/")

# Section 3 outline. The guide calls it "a default, not a required template",
# so a missing section is never blocking.
README_OUTLINE = [
    "status", "what this is", "what this is not", "quick start",
    "configuration", "architecture", "repository layout", "contributing",
    "security", "license",
]

# Section 2, "Habits to avoid".
#
# `robust` and `powerful` are deliberately absent. Both are technical terms in
# this corpus — a robust estimator, statistical power — and a rule that flags
# them reported twelve non-defects in the research docs and one quotation of
# Hansen's SPA test. A word that is marketing in a landing page and a term of
# art in a research note cannot be decided by a word list, so the list keeps
# only the words that have no technical sense here.
MARKETING_WORDS = [
    "seamless", "cutting-edge", "comprehensive",
    "world-class", "next-generation", "state-of-the-art", "best-in-class",
]
FILLER_PHRASES = [
    "it is worth noting", "it's worth noting", "in summary", "let's dive in",
    "needless to say", "at the end of the day",
]

# Section 13 says "non-placeholder buckets" and section 7 line 104 says the
# placeholder form is `<bucket>`, `<project-id>`, `$PROJECTS_ROOT`. So a
# `gs://` URI whose bucket segment is a placeholder is the correct form, not a
# finding. `synthetic-` is on the list because the organisation's fixtures are
# named that way; without it every fixture a doc quotes becomes a finding the
# day the doc is written.
PLACEHOLDER_BUCKET = re.compile(
    r"^[<{$]|[<>{}]|\.\.\."
    r"|^(?:your|my|example|sample|placeholder|shared|synthetic)[-_]"
    r"|^bucket(?:-|$)"
)

URL_TOKEN = re.compile(r"https?://[^\s)\]\"'<>`]+")
FENCED_BLOCK = re.compile(r"^```.*?^```", re.DOTALL | re.MULTILINE)


class FileContext(NamedTuple):
    """Per-file material the leak predicates need to judge a match."""

    url_spans: list[tuple[int, int]]
    prose: str  # the text with fenced blocks blanked out, offsets preserved
    masked: str  # `prose` with inline code blanked out too


def phrase_is_prose(text: str, match: re.Match, ctx: FileContext) -> bool:
    """A rule about phrasing describes prose; backticks name the phrase.

    The style guide writes `this round` when it means the words, and a value
    rule such as `gs://` is the opposite case: a bucket inside backticks is
    still a bucket. That is why this is a per-rule predicate and not a blanket
    skip on code spans.
    """
    return bool(ctx.masked[match.start():match.end()].strip())


def bucket_is_real(text: str, match: re.Match, ctx: FileContext) -> bool:
    bucket = match.group(0)[len("gs://"):].split("/", 1)[0]
    # A sentence-final period is punctuation, not part of the name. An ellipsis
    # is the placeholder, so it must survive this.
    if bucket.endswith(".") and not bucket.endswith("..."):
        bucket = bucket[:-1]
    return bool(bucket) and not PLACEHOLDER_BUCKET.search(bucket)


def hex_is_prose(text: str, match: re.Match, ctx: FileContext) -> bool:
    """Section 5 forbids 40-character hex *in prose* — CI head SHAs pasted in.

    A revision reference is a different thing and the guide asks for it: line
    377 writes `<40-char sha>` and the cross-repo links pin `blob/<sha>/`. So
    the three forms that carry a revision are exempt, and what is left is the
    case the rule was written for.
    """
    if ctx.prose[match.start()] == " ":
        return False  # inside a fenced block: a contract example, not prose
    for start, end in ctx.url_spans:
        if start <= match.start() < end:
            return False  # a link target
    line_start = text.rfind("\n", 0, match.start()) + 1
    line_end = text.find("\n", match.end())
    line = text[line_start: line_end if line_end != -1 else len(text)]
    column = match.start() - line_start
    if line[:column].count("`") % 2 == 1:
        return False  # a quoted identifier
    if text[match.end(): match.end() + 2] == "](":
        return False  # the label of a link whose target carries the same sha
    return len(set(match.group(0))) != 1  # 000...0 and friends, a placeholder


# Section 13 grep list plus section 5 categories that are recognisable.
# The lookbehind keeps URL paths such as https://example.com/home/legal/ out of
# the results; "/home/" inside a URL is not a local path.
#
# Two categories, because they carry different weight. A real home path is
# evidence that something leaked from a workstation. The rest are the guide's
# own "prompt to check, not a certain failure" greps.
#
# The last column is a predicate deciding whether a match is a finding at all.
# Both heuristics in this list match far more than they mean: `gs://` matches
# `gs://<bucket>/...`, which is the form section 13 asks for, and a 40-character
# hex string matches every commit SHA, which is how a cross-repository reference
# is supposed to be pinned. Without the predicates the two rules reported 522
# findings across the organisation and not one of them was a defect, which is
# the same as having no rule: the report gets scrolled past.
LEAK_PATTERNS = [
    (re.compile(r"(?<![\w.])/(?:Users|home)/[^\s`)\]\"'<>|]{2,}"),
     "paths", "absolute home path (section 13)", None),
    (re.compile(r"\bgs://[^\s`)\]\"'<>]+"),
     "content", "non-placeholder bucket (section 13)", bucket_is_real),
    (re.compile(r"\b[\w-]+\.workers\.dev\b"),
     "content", "personal subdomain (section 13)", None),
    (re.compile(r"\b[0-9a-f]{40}\b"),
     "content", "40-character hex string in prose (section 5)", hex_is_prose),
    # `| PR |` is a table header; `| PR #12 |` and `| #12 |` are the rows this
    # rule is about. Matching the header too reported the guide's own row
    # describing the rule.
    (re.compile(r"\|\s*(?:PR\s*#?\d+|#\d+)"),
     "content", "PR-number status table (section 5)", None),
    (re.compile(r"\b(this round|this task|in this task)\b", re.IGNORECASE),
     "content", "task-scope wording (section 5)", phrase_is_prose),
    (re.compile(r"\b(the owner approved|approved by)\b", re.IGNORECASE),
     "content", "owner approval note (section 5)", phrase_is_prose),
]


# Section 6: paths for notes that must never be committed. The four wildcard
# entries are spelled with [Aa] character classes because .gitignore is
# case-sensitive on some platforms and `*audit*.md` alone lets `Audit.md` past.
INTERNAL_NOTE_GITIGNORE = [
    "*[Aa][Uu][Dd][Ii][Tt]*.md",
    "*[Hh][Aa][Nn][Dd][Oo][Ff][Ff]*.md",
    "*-[Rr][Ee][Vv][Ii][Ee][Ww]-*.md",
    "*_[Rr][Ee][Vv][Ii][Ee][Ww]_*.md",
    "docs/audits/",
    "docs/plans/",
    "docs/handoffs/",
    "docs/superpowers/",
    ".local/",
    "ops-notes/",
    "*.local.md",
    "*.internal.md",
]

# Directories whose contents are working material, never reader-facing docs.
INTERNAL_NOTE_DIRS = re.compile(
    r"(^|/)(docs/plans|docs/handoffs|docs/superpowers|docs/audits)/"
)

DATED_TITLE = re.compile(r"(audit|acceptance).*20\d{2}-\d{2}-\d{2}", re.IGNORECASE)

# Blocking categories per phase.
#
# Phase 1 blocks only what is unambiguous: a leaked local path, a link a reader
# cannot follow, a missing rules file, a .gitignore that has stopped keeping
# working notes out. Everything else is reported as NOTE. Phase 2 blocks the
# rest once the naming cleanup is done. README structure never blocks, because
# section 3 is a default and section 4 says so.
PHASE_BLOCKING = {
    1: {"paths", "links", "agents", "hygiene"},
    2: {"paths", "links", "agents", "hygiene", "content", "naming", "language"},
}


class Report:
    def __init__(self, blocking: set[str]) -> None:
        self.blocking = blocking
        self.findings: list[tuple[str, str]] = []

    def add(self, category: str, message: str) -> None:
        self.findings.append((category, message))

    def blocking_findings(self) -> list[tuple[str, str]]:
        return [f for f in self.findings if f[0] in self.blocking]

    def advisory_findings(self) -> list[tuple[str, str]]:
        return [f for f in self.findings if f[0] not in self.blocking]


def iter_files(root: Path):
    for path in sorted(root.rglob("*")):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.is_file():
            yield path


def is_markdown(path: Path) -> bool:
    return path.suffix.lower() in {".md", ".markdown"}


def has_emoji(text: str) -> bool:
    for char in text:
        if ord(char) < 0x2000:
            continue
        if unicodedata.category(char) == "So":
            return True
    return False


def strip_fenced_blocks(text: str) -> str:
    """Drop ``` fenced blocks.

    Documentation about documentation contains example links such as
    `[简体中文](README.zh-CN.md)`. Those are illustrations, not links, and
    checking them reports failures the author cannot fix.
    """
    return re.sub(r"^```.*?^```", "", text, flags=re.DOTALL | re.MULTILINE)


def check_naming(root: Path, report: Report) -> None:
    for path in iter_files(root):
        if not is_markdown(path):
            continue
        name = path.name
        rel = path.relative_to(root).as_posix()
        core = name[:-3]

        if " " in name:
            report.add("naming", f"{rel}: file name contains a space")

        if core.endswith(".zh-CN"):
            core = core[: -len(".zh-CN")]
        elif core.endswith(".zh-cn"):
            report.add("naming", f"{rel}: language suffix must be .zh-CN.md, not .zh-cn.md")
            core = core[: -len(".zh-cn")]
        elif core.endswith(".en"):
            report.add(
                "naming",
                f"{rel}: English is the base name (section 4); .en.md is not a suffix",
            )

        if core not in CONVENTIONAL_UPPER:
            if core != core.lower():
                report.add("naming", f"{rel}: file name must be lowercase (section 7)")
            if "_" in core:
                report.add("naming", f"{rel}: use hyphens, not underscores (section 7)")

        if DATE_IN_NAME.search(core) and not rel.startswith(DATE_BEARING_PATHS):
            report.add("naming", f"{rel}: dates belong in history, not file names (section 7)")


def check_readme(root: Path, report: Report) -> None:
    readme = root / "README.md"
    if not readme.exists():
        report.add("readme", "README.md is missing")
        return
    text = readme.read_text(encoding="utf-8", errors="ignore")
    headings = [h.strip().lower() for h in re.findall(r"^#{2,3}\s+(.+)$", text, re.MULTILINE)]
    missing = [
        section for section in README_OUTLINE
        if not any(section in heading for heading in headings)
    ]
    if missing:
        report.add(
            "readme",
            "README.md: section 3 outline not followed — missing "
            + ", ".join(missing)
            + " (a default, not a requirement)",
        )


def check_links(root: Path, report: Report) -> None:
    for path in iter_files(root):
        if not is_markdown(path):
            continue
        text = strip_fenced_blocks(path.read_text(encoding="utf-8", errors="ignore"))
        for match in re.finditer(r"\[[^\]]*\]\(([^)\s]+)\)", text):
            target = match.group(1)
            if target.startswith(("http://", "https://", "mailto:", "#", "<")):
                continue
            resolved = (path.parent / urllib.parse.unquote(target.split("#")[0])).resolve()
            # A link that climbs out of the repository points at a sibling
            # checkout. CI only has this repository, so the target cannot exist
            # there — that is not a broken link, it is a reference this check
            # cannot verify. Reporting it as broken makes the check unusable in
            # CI while passing locally, which is the worst of both.
            if not resolved.is_relative_to(root):
                # `xref` is not in PHASE_BLOCKING, so this stays a note in
                # both phases. `tools/check_cross_repo_links.py` resolves these
                # against the sibling's default branch on a schedule; a check
                # that says "not checked" has no standing to block.
                report.add(
                    "xref",
                    f"{path.relative_to(root).as_posix()}: cross-repository reference, not checked -> {target}",
                )
                continue
            if not resolved.exists():
                report.add(
                    "links",
                    f"{path.relative_to(root).as_posix()}: broken relative link -> {target}",
                )


def check_agent_files(root: Path, report: Report) -> None:
    tracked = subprocess.run(
        ["git", "ls-files", "--cached", "-z", "--", "AGENTS.md", "CLAUDE.md"],
        cwd=root, capture_output=True, text=True, check=False,
    )
    names = set(tracked.stdout.split("\0")) if tracked.returncode == 0 else set()

    for name in ("AGENTS.md", "CLAUDE.md"):
        path = root / name
        if not path.exists():
            report.add("agents", f"{name} is missing")
        elif name not in names:
            report.add("agents", f"{name} is not tracked by Git; a local copy is not a committed rule")

    claude = root / "CLAUDE.md"
    if claude.exists():
        content = claude.read_text(encoding="utf-8", errors="ignore").strip()
        if content != "@AGENTS.md":
            report.add("agents", "CLAUDE.md must contain the single line @AGENTS.md")

    agents = root / "AGENTS.md"
    if agents.exists():
        text = agents.read_text(encoding="utf-8", errors="ignore")
        if CONTENT_BOUNDARY not in text:
            report.add(
                "agents",
                "AGENTS.md is missing the content-boundary block, see section 5",
            )


def check_language(root: Path, report: Report) -> None:
    marketing = re.compile(
        r"\b(" + "|".join(re.escape(w) for w in MARKETING_WORDS) + r")\b", re.IGNORECASE
    )
    filler = re.compile(
        r"\b(" + "|".join(re.escape(p) for p in FILLER_PHRASES) + r")\b", re.IGNORECASE
    )
    for path in iter_files(root):
        if not is_markdown(path):
            continue
        rel = path.relative_to(root).as_posix()
        # A word inside a fenced block or a code span is being named, not used.
        # The guide lists the words it forbids, and without this the guide is
        # the largest offender in the organisation.
        text = blank_code_spans(blank_fenced_blocks(path.read_text(encoding="utf-8", errors="ignore")))
        first_heading = True
        for number, line in enumerate(text.splitlines(), 1):
            for match in marketing.finditer(line):
                report.add("language", f"{rel}:{number}: marketing word '{match.group(0)}' (section 2)")
            for match in filler.finditer(line):
                report.add("language", f"{rel}:{number}: filler phrase '{match.group(0)}' (section 2)")
            if line.startswith("#") and has_emoji(line):
                report.add("language", f"{rel}:{number}: emoji in heading (section 2)")
            if line.startswith("#"):
                # Section 5 is about the title a reader sees in a list of
                # documents. A dated section inside a research log is a
                # chronology, not a title.
                if first_heading and DATED_TITLE.search(line):
                    report.add("language", f"{rel}:{number}: dated title in a reader-facing doc (section 5)")
                first_heading = False


CODE_SPAN = re.compile(r"`[^`\n]+`")


def blank_code_spans(text: str) -> str:
    """Replace inline code with spaces, keeping offsets.

    A word in backticks is a mention: the style guide writes `robust` when it
    means the word, and a research note writes a commit id as a value. Neither
    is prose, and both were reported as prose.
    """
    return CODE_SPAN.sub(lambda m: " " * len(m.group(0)), text)


def blank_fenced_blocks(text: str) -> str:
    """Replace fenced blocks with spaces, keeping every offset in place.

    `hex_is_prose` needs to know whether a match sits in a contract example or
    in running text, and it needs the character offsets to stay usable for the
    line and backtick lookups that follow.
    """
    def blank(match: re.Match) -> str:
        return re.sub(r"[^\n]", " ", match.group(0))

    return FENCED_BLOCK.sub(blank, text)


def check_leaks(root: Path, report: Report) -> None:
    for path in iter_files(root):
        if not is_markdown(path):
            continue
        rel = path.relative_to(root).as_posix()
        text = path.read_text(encoding="utf-8", errors="ignore")
        prose = blank_fenced_blocks(text)
        ctx = FileContext(
            url_spans=[(m.start(), m.end()) for m in URL_TOKEN.finditer(text)],
            prose=prose,
            masked=blank_code_spans(prose),
        )
        for pattern, category, label, predicate in LEAK_PATTERNS:
            for match in pattern.finditer(text):
                # A pattern without a predicate describes prose, and a fenced
                # block is not prose. The style guide shows each of these
                # shapes as an example of what to avoid.
                if predicate is None and not ctx.prose[match.start():match.end()].strip():
                    continue
                if predicate is not None and not predicate(text, match, ctx):
                    continue
                report.add(category, f"{rel}: {label}: {match.group(0)[:40]}")


def check_internal_paths(root: Path, report: Report) -> None:
    gitignore = root / ".gitignore"
    text = gitignore.read_text(encoding="utf-8", errors="ignore") if gitignore.exists() else ""
    missing = [pattern for pattern in INTERNAL_NOTE_GITIGNORE if pattern not in text]
    if missing:
        # Blocking in both phases. This is the first of the three layers that
        # keep working notes out of a public repository, and it is the only one
        # that acts before the note is committed at all. A repository that
        # loses it stops preventing the mistake and starts detecting it, which
        # is a different and much later guarantee.
        report.add(
            "hygiene",
            ".gitignore: internal note paths not ignored — " + ", ".join(missing),
        )

    for path in iter_files(root):
        if not is_markdown(path):
            continue
        rel = path.relative_to(root).as_posix()
        if INTERNAL_NOTE_DIRS.search(rel):
            report.add("content", f"{rel}: plan/handoff directory in a public repo (section 5)")
        if re.search(r"(^|/)(out|output)/[^/]+/", rel):
            report.add("content", f"{rel}: committed output directory (section 5)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="repository root (default: cwd)")
    parser.add_argument(
        "--phase", type=int, choices=(1, 2), default=1,
        help="1 blocks leaks and broken links; 2 also blocks naming (default: 1)",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    report = Report(PHASE_BLOCKING[args.phase])

    check_naming(root, report)
    check_readme(root, report)
    check_links(root, report)
    check_agent_files(root, report)
    check_language(root, report)
    check_leaks(root, report)
    check_internal_paths(root, report)

    blocking = report.blocking_findings()
    advisory = report.advisory_findings()

    for category, message in blocking:
        print(f"ERROR  [{category}] {message}")
    for category, message in advisory:
        print(f"NOTE   [{category}] {message}")

    print()
    print(
        f"phase {args.phase}: {len(blocking)} blocking, {len(advisory)} advisory. "
        "Advisory findings become blocking in phase 2."
    )

    return 1 if blocking else 0


if __name__ == "__main__":
    sys.exit(main())
