#!/usr/bin/env python3
"""Translate the Codex GitHub App's PR review into a check run for branch protection.

Two modes:
  WAIT  — on PR opened/synchronize: create pending check, poll for Codex review,
          fall back to pass after timeout (Codex might be broken → don't block).
  REACT — on Codex bot review submitted: update check immediately.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

API_BASE = "https://api.github.com"
BOT_LOGIN = "chatgpt-codex-connector[bot]"
CHECK_NAME = "Codex Review Gate"
DETAIL_URL = "https://github.com/apps/chatgpt-codex-connector"

# ── helpers ──────────────────────────────────────────────────────────────────


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)))
    except ValueError:
        return default


def github_request(
    token: str, method: str, path: str, payload: dict[str, Any] | None = None
) -> Any:
    url = f"{API_BASE}{path}" if not path.startswith("https://") else path
    data = json.dumps(payload).encode() if payload else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "codex-review-gate",
    }
    if payload:
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {method} {url}: {exc.code} {detail[:500]}") from exc
    return json.loads(body) if body else {}


# ── review lookup ────────────────────────────────────────────────────────────


def get_codex_review(token: str, repo: str, pr_number: int) -> dict[str, Any] | None:
    reviews = github_request(token, "GET", f"/repos/{repo}/pulls/{pr_number}/reviews?per_page=100")
    if not isinstance(reviews, list):
        return None
    for r in reversed(reviews):
        if isinstance(r, dict) and (r.get("user") or {}).get("login") == BOT_LOGIN:
            return r
    return None


# ── check run management ─────────────────────────────────────────────────────


def get_existing_check_run(token: str, repo: str, head_sha: str) -> dict[str, Any] | None:
    result = github_request(
        token, "GET", f"/repos/{repo}/commits/{head_sha}/check-runs?per_page=50&filter=latest"
    )
    runs = result.get("check_runs", []) if isinstance(result, dict) else []
    for run in runs:
        if isinstance(run, dict) and run.get("name") == CHECK_NAME:
            return run
    return None


def upsert_check_run(
    token: str,
    repo: str,
    head_sha: str,
    *,
    status: str,       # "queued" | "in_progress" | "completed"
    conclusion: str | None,
    title: str,
    summary: str,
) -> dict[str, Any]:
    existing = get_existing_check_run(token, repo, head_sha)
    body: dict[str, Any] = {
        "name": CHECK_NAME,
        "head_sha": head_sha,
        "status": status,
        "details_url": DETAIL_URL,
        "output": {"title": title, "summary": summary},
    }
    if conclusion:
        body["conclusion"] = conclusion
    if status and status != "completed":
        body.pop("conclusion", None)

    if existing and existing.get("id"):
        url = f"/repos/{repo}/check-runs/{existing['id']}"
        return github_request(token, "PATCH", url, body)
    else:
        return github_request(token, "POST", f"/repos/{repo}/check-runs", body)


# ── state → decision ─────────────────────────────────────────────────────────


def review_decision(review: dict[str, Any] | None) -> tuple[str, str, str]:
    """Return (conclusion, title, summary) for a given review."""
    if review is None:
        return (
            "success",
            "Codex: no review — passed through",
            "The Codex GitHub App has not reviewed this PR.\n\n"
            "- Automatic reviews may be disabled in Codex settings.\n"
            "- Or mention `@codex review` in a comment to request one.\n"
            "- This check passes so development is not blocked.",
        )

    state = (review.get("state") or "").strip().upper()
    submitted_at = review.get("submitted_at", "unknown time")
    review_url = review.get("html_url", "")

    if state == "CHANGES_REQUESTED":
        body = (review.get("body") or "").strip()
        snippet = (body[:500] + "...") if len(body) > 500 else body
        return (
            "failure",
            "Codex: changes requested — MERGE BLOCKED",
            f"Codex **requested changes** at {submitted_at}.\n\n"
            + (f"---\n\n{snippet}\n\n---\n\n" if snippet else "")
            + "**Fix:** Push a new commit addressing the feedback.\n"
            + f"[View full review]({review_url})",
        )
    if state == "APPROVED":
        return (
            "success",
            "Codex: approved",
            f"Codex **approved** this PR at {submitted_at}.\n\n[View review]({review_url})",
        )
    # COMMENTED / DISMISSED / PENDING
    return (
        "success",
        f"Codex: reviewed ({state.lower()})",
        f"Codex submitted a `{state}` review at {submitted_at}. "
        "Not a blocking review — merge is allowed.\n\n"
        f"[View review]({review_url})",
    )


# ── main logic ───────────────────────────────────────────────────────────────


def main() -> int:
    token = env("GH_TOKEN") or env("GITHUB_TOKEN")
    if not token:
        print("::error::GH_TOKEN required", file=sys.stderr)
        return 1

    repo = env("GITHUB_REPOSITORY")
    if not repo:
        print("::error::GITHUB_REPOSITORY not set", file=sys.stderr)
        return 1

    event_path = Path(os.environ.get("GITHUB_EVENT_PATH", ""))
    if not event_path.exists():
        print("::error::GITHUB_EVENT_PATH missing", file=sys.stderr)
        return 1

    event = json.loads(event_path.read_text(encoding="utf-8"))
    event_name = env("GITHUB_EVENT_NAME", "")

    # Resolve PR number + head SHA
    pr = event.get("pull_request") or {}
    pr_number = pr.get("number")
    head_sha = (pr.get("head") or {}).get("sha")

    if not pr_number or not head_sha:
        print(f"::warning::Cannot resolve PR: number={pr_number} sha={head_sha}")
        return 0

    print(f"PR #{pr_number}  sha={head_sha[:12]}  event={event_name}")

    # ── REACT mode: Codex just submitted a review ──────────────────────
    review_event = event.get("review") or {}
    review_user = (review_event.get("user") or {}).get("login", "")

    if event_name == "pull_request_review" and review_user == BOT_LOGIN:
        conclusion, title, summary = review_decision(review_event)
        upsert_check_run(token, repo, head_sha, status="completed",
                         conclusion=conclusion, title=title, summary=summary)
        print(f"REACT → {conclusion}: {title}")
        return 1 if conclusion == "failure" else 0

    # ── WAIT mode: PR opened/synchronized ─────────────────────────────
    # First check if Codex already reviewed
    try:
        existing_review = get_codex_review(token, repo, pr_number)
    except RuntimeError as exc:
        print(f"::warning::Cannot fetch reviews: {exc}")
        return 0

    if existing_review is not None:
        conclusion, title, summary = review_decision(existing_review)
        upsert_check_run(token, repo, head_sha, status="completed",
                         conclusion=conclusion, title=title, summary=summary)
        print(f"EXISTING → {conclusion}: {title}")
        return 1 if conclusion == "failure" else 0

    # No review yet → set pending and poll
    poll_seconds = env_int("CODEX_GATE_POLL_SECONDS", 30)
    max_wait = env_int("CODEX_GATE_MAX_WAIT_MINUTES", 10)
    deadline = time.time() + max_wait * 60

    upsert_check_run(
        token, repo, head_sha,
        status="in_progress",
        conclusion=None,
        title="Codex: waiting for review…",
        summary=(
            "Waiting for the Codex GitHub App to review this PR.\n\n"
            f"Polling every {poll_seconds}s for up to {max_wait} min.\n"
            "If Codex does not respond in time, the check passes through "
            "to avoid blocking development.\n\n"
            "Ensure automatic reviews are enabled in Codex settings, "
            "or mention `@codex review` in a comment."
        ),
    )
    print(f"WAIT → polling every {poll_seconds}s for up to {max_wait}min")

    while time.time() < deadline:
        time.sleep(poll_seconds)
        try:
            review = get_codex_review(token, repo, pr_number)
        except RuntimeError:
            continue  # transient API error → retry

        if review is not None:
            conclusion, title, summary = review_decision(review)
            upsert_check_run(token, repo, head_sha, status="completed",
                             conclusion=conclusion, title=title, summary=summary)
            print(f"WAIT → found review → {conclusion}: {title}")
            return 1 if conclusion == "failure" else 0

    # ── Timeout: Codex didn't respond ─────────────────────────────────
    title = "Codex: timeout — passed through"
    summary = (
        f"Codex did not submit a review within {max_wait} minutes.\n\n"
        "**Possible causes:**\n"
        "- Codex subscription may be paused or expired\n"
        "- Automatic reviews may be disabled for this repo\n"
        "- Codex service may be experiencing issues\n\n"
        "**This check passes** so development is not blocked.\n"
        "Request a manual review from a teammate before merging."
    )
    upsert_check_run(token, repo, head_sha, status="completed",
                     conclusion="success", title=title, summary=summary)
    print(f"TIMEOUT → passed through (no Codex response in {max_wait}min)")

    # Write decision artifact
    out_dir = Path("data/output/codex_review_gate")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "gate_decision.json").write_text(
        json.dumps({
            "repo": repo, "pr_number": pr_number, "head_sha": head_sha,
            "mode": "wait_timeout", "conclusion": "success", "title": title,
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
