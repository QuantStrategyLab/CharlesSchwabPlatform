#!/usr/bin/env python3
"""Cross-platform daily digest aggregator (evidence in → render out).

Collects only platforms/strategies that actually ran from:
  1) candidates JSON file / DIGEST_CANDIDATES_JSON (preferred; producer-authored)
  2) optional GitHub Actions workflow conclusions (allowlisted; existence only —
     fill/order counts stay unknown unless a candidate row supplies them)

Does not trade, does not embed tokens, does not invent missing platforms.
Aggregation keys include account/target identity. Source read failures are
recorded in coverage/receipt and must not render as “link healthy / no runs”.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import daily_digest_notify as digest  # noqa: E402
import daily_digest_delivery_axes as delivery_axes  # noqa: E402
import merge_digest_candidates as candidate_contract  # noqa: E402
from notification_mode import consolidation_enabled, routine_mode  # noqa: E402

DEFAULT_PLATFORM_CONFIG = ROOT / "platform-config.json"
NY_TZ = ZoneInfo("America/New_York")
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

_OBSERVATION_FIELDS = (
    "strategy_label",
    "equity",
    "equity_usd",
    "equity_currency",
    "currency",
    "holdings",
    "positions",
    "signal_summary",
    "rebalance_kind",
    "rebalance_conclusion",
    "tips",
    "tip",
    "account_hint",
    "account_scope",
    "execution_mode",
)


def _has_holdings(row: Mapping[str, Any]) -> bool:
    return row.get("holdings") not in (None, "", [], ()) or row.get(
        "positions"
    ) not in (None, "", [], ())


@dataclass
class GithubEvidenceResult:
    rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    sources_queried: int = 0
    sources_matched: int = 0


@dataclass
class CollectResult:
    runs: list[digest.DigestRunEntry]
    coverage: digest.EvidenceCoverage
    merged_rows: list[dict[str, Any]] = field(default_factory=list)
    notification_summary: dict[str, Any] | None = None


def resolve_business_day(
    *,
    explicit: str | None = None,
    now: datetime | None = None,
) -> str:
    """US equity session date for post-close digest (America/New_York calendar day)."""

    if explicit and str(explicit).strip():
        value = str(explicit).strip()
        date.fromisoformat(value)
        return value
    stamp = now or datetime.now(tz=timezone.utc)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    if consolidation_enabled():
        local = stamp.astimezone(ZoneInfo("Asia/Singapore"))
        end_day = local.date() - timedelta(days=local.hour < DIGEST_CUTOFF_HOUR_SHANGHAI)
        return (end_day - timedelta(days=1)).isoformat()
    return stamp.astimezone(NY_TZ).date().isoformat()


DIGEST_CUTOFF_HOUR_SHANGHAI = 6


def digest_window_for(business_day: str) -> dict[str, str]:
    """NOTIFY-04 B1: Shanghai cut-off window for a New York business day.

    The report for NY session ``B`` is dated ``B+1`` in Shanghai and covers
    ``[B 06:00, B+1 06:00)`` Asia/Shanghai — i.e. ``[B-1 22:00, B 22:00)`` UTC.
    The US close (16:00 NY = 04:00/05:00 Shanghai) of ``B`` falls inside it, and
    a 24/7 crypto runtime contributes the cycles started inside the window.
    """

    day = date.fromisoformat(business_day)
    start_local = datetime(
        day.year, day.month, day.day, DIGEST_CUTOFF_HOUR_SHANGHAI, tzinfo=SHANGHAI_TZ
    )
    end_local = start_local + timedelta(days=1)
    window = {
        "timezone": "Asia/Shanghai",
        "business_day": day.isoformat(),
        "report_date": end_local.date().isoformat(),
        "start_local": start_local.strftime("%Y-%m-%d %H:%M"),
        "end_local": end_local.strftime("%Y-%m-%d %H:%M"),
        "start_utc": start_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "end_utc": end_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    if consolidation_enabled():
        window.update(timezone="Asia/Singapore", window_id=f"sgt-0600:{business_day}")
    return window


def resolve_layout(raw: str | None = None) -> str:
    return digest.normalize_layout(raw or os.environ.get("DIGEST_LAYOUT") or "v1")


def section_for_profile(config: Mapping[str, Any], strategy_profile: str) -> str:
    """Infer the digest section from platform-config (explicit row value wins)."""

    meta = (config.get("strategies") or {}).get(strategy_profile) or {}
    if not isinstance(meta, Mapping):
        return "us_daily"
    if str(meta.get("domain") or "") == "crypto":
        return "dca" if "dca" in strategy_profile else "crypto_intraday"
    if str(meta.get("scheduler_profile") or "") == "us_dca_month_end":
        return "dca"
    return "us_daily"


def resolve_locale(raw: str | None = None) -> digest.DigestLocale:
    return digest.normalize_locale(
        raw
        or os.environ.get("QSL_NOTIFY_LANG")
        or os.environ.get("NOTIFY_LANG")
        or "zh"
    )


def load_platform_config(path: Path | None = None) -> dict[str, Any]:
    config_path = path or Path(
        os.environ.get("QSL_PLATFORM_CONFIG") or DEFAULT_PLATFORM_CONFIG
    )
    return json.loads(config_path.read_text(encoding="utf-8"))


def digest_contract(config: Mapping[str, Any]) -> dict[str, Any]:
    return dict(config["notifications"]["quant_sentinel"]["daily_digest"])


def window_label_for(contract: Mapping[str, Any]) -> str:
    schedule = contract.get("schedule") or {}
    tz = str(schedule.get("timezone") or "Asia/Shanghai")
    cron = str(schedule.get("cron") or "0 6 * * 2-6")
    return f"{tz} cron `{cron}`"


def _as_candidate_rows(raw: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in candidate_contract._rows(raw):
        row = dict(item)
        if not str(row.get("evidence_provenance") or "").strip():
            row["evidence_provenance"] = "candidates"
        out.append(row)
    return out


def load_candidates_from_file(path: Path | None) -> tuple[list[dict[str, Any]], bool, bool]:
    """Return (rows, configured, loaded).

    configured: env/path asked for candidates.
    loaded: file/JSON was successfully parsed (may be empty list).
    """

    configured = False
    if path is None:
        env_path = (os.environ.get("DIGEST_CANDIDATES_PATH") or "").strip()
        path = Path(env_path) if env_path else None
    if path is not None:
        configured = True
        if not path.is_file():
            raise FileNotFoundError(f"candidates file not found: {path}")
        return _as_candidate_rows(json.loads(path.read_text(encoding="utf-8"))), True, True
    inline = (os.environ.get("DIGEST_CANDIDATES_JSON") or "").strip()
    if inline:
        configured = True
        return _as_candidate_rows(json.loads(inline)), True, True
    return [], False, False


CANDIDATE_STORE_FAILURE_REASONS = frozenset({
    "candidate_store_auth_failed", "candidate_store_list_failed", "candidate_store_copy_failed",
    "candidate_store_merge_failed", "candidate_store_outcome_missing", "candidate_store_outcome_invalid",
    "candidate_input_unavailable_or_invalid",
})


def candidate_store_failures() -> list[dict[str, str]]:
    """Read the workflow's explicit private-store outcome; never log its URI."""
    configured = bool((os.environ.get("DIGEST_CANDIDATES_GCS_URI") or "").strip())
    state = (os.environ.get("DIGEST_CANDIDATES_STORE_STATUS") or "").strip()
    if state == "loaded" or (not configured and state in {"", "disabled"}):
        return []
    reason = (os.environ.get("DIGEST_CANDIDATES_STORE_REASON") or "").strip()
    if state != "failed" or reason not in CANDIDATE_STORE_FAILURE_REASONS:
        reason = "candidate_store_outcome_missing" if not state else "candidate_store_outcome_invalid"
    return [{"source_kind": "candidate_store", "reason_code": reason}]


def _gh_api_get(url: str, token: str) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "QuantRuntimeSettings-daily-digest-aggregator",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"GitHub API HTTP {exc.code} for {url}: {body}") from exc


def business_day_utc_window(business_day: str) -> tuple[datetime, datetime]:
    """Inclusive NY business-day window expressed in UTC."""

    day = date.fromisoformat(business_day)
    start_ny = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=NY_TZ)
    end_ny = start_ny + timedelta(days=1)
    return start_ny.astimezone(timezone.utc), end_ny.astimezone(timezone.utc)


def default_workflow_allowlist(config: Mapping[str, Any]) -> list[dict[str, str]]:
    """Allowlisted workflows that may contribute *run existence* evidence only.

    Counts remain unknown unless a candidates file supplies them. Heartbeat-only
    monitor workflows are intentionally omitted.
    """

    platforms = config.get("platforms") or {}
    rows: list[dict[str, str]] = []
    # Fallback only when platform-config allowlist is absent.
    # Keep in sync with calibrated present paths (P0-04); never list 404 stems.
    defaults: dict[str, tuple[str, str, str]] = {
        # workflow → strategy_profile → evidence_role
        "longbridge": (
            "publish-runtime-daily-once.yml",
            "russell_top50_leader_rotation",
            "paper_projection_once",
        ),
        "schwab": (
            "runtime-daily-sync.yml",
            "soxl_soxx_trend_income",
            "manual_daily_projection",
        ),
    }
    configured = (
        (config.get("notifications") or {})
        .get("quant_sentinel", {})
        .get("daily_digest", {})
        .get("aggregator", {})
        .get("github_workflow_allowlist")
    )
    if isinstance(configured, list) and configured:
        for item in configured:
            if not isinstance(item, Mapping):
                continue
            platform_id = str(item.get("platform_id") or "").strip()
            workflow = str(item.get("workflow") or "").strip()
            strategy = str(item.get("strategy_profile") or "unspecified").strip()
            repo = str(item.get("repository") or "").strip()
            if not platform_id or not workflow:
                continue
            if not repo and platform_id in platforms:
                repo = str(platforms[platform_id].get("repository") or "").strip()
            if not repo:
                continue
            row = {
                "platform_id": platform_id,
                "repository": repo,
                "workflow": workflow,
                "strategy_profile": strategy or "unspecified",
            }
            evidence_role = str(item.get("evidence_role") or "").strip()
            if evidence_role:
                row["evidence_role"] = evidence_role
            path_status = str(item.get("path_status") or "").strip()
            if path_status:
                row["path_status"] = path_status
            rows.append(row)
        return rows

    for platform_id, (workflow, strategy, evidence_role) in defaults.items():
        meta = platforms.get(platform_id) or {}
        repo = str(meta.get("repository") or "").strip()
        if not repo:
            continue
        rows.append(
            {
                "platform_id": platform_id,
                "repository": repo,
                "workflow": workflow,
                "strategy_profile": strategy,
                "evidence_role": evidence_role,
                "path_status": "present",
            }
        )
    return rows


def collect_from_github_workflows(
    *,
    business_day: str,
    config: Mapping[str, Any],
    token: str | None = None,
    allowlist: Sequence[Mapping[str, str]] | None = None,
) -> GithubEvidenceResult:
    """Include a platform only when an allowlisted workflow succeeded that day.

    Never invents fill/order counts (null + counts_unknown). API errors are
    recorded as source failures — callers must not treat empty rows alone as
    verified idle / link-healthy.
    """

    result = GithubEvidenceResult()
    gh_token = (
        token or os.environ.get("DIGEST_GH_TOKEN") or os.environ.get("GH_TOKEN") or ""
    ).strip()
    entries = list(allowlist or default_workflow_allowlist(config))
    if not entries:
        return result
    if not gh_token:
        result.failures.append(
            {
                "platform_id": "*",
                "workflow": "*",
                "repository": "*",
                "reason_code": "github_token_missing",
                "message": "DIGEST_GH_TOKEN/GH_TOKEN absent; github evidence not queried",
            }
        )
        return result

    start_utc, end_utc = business_day_utc_window(business_day)
    seen: set[tuple[str, str, str, str]] = set()
    for entry in entries:
        platform_id = entry["platform_id"]
        repository = entry["repository"]
        workflow = entry["workflow"]
        strategy = entry["strategy_profile"]
        evidence_role = str(entry.get("evidence_role") or "").strip()
        owner_repo = repository.split("/", 1)
        if len(owner_repo) != 2:
            result.failures.append(
                {
                    "platform_id": platform_id,
                    "workflow": workflow,
                    "repository": repository,
                    "reason_code": "invalid_repository",
                    "message": f"repository must be owner/repo, got {repository!r}",
                }
            )
            continue
        owner, repo = owner_repo
        result.sources_queried += 1
        matched = False
        failure_reason = "github_api_error"
        try:
            # GitHub accepts one event value, not a comma-separated OR filter.
            # Exhaust both event queries/pages before declaring no match.
            for event in ("workflow_dispatch", "schedule"):
                page = 1
                seen_run_count = 0
                while True:
                    query = urllib.parse.urlencode({
                        "event": event,
                        "status": "completed",
                        "per_page": "100",
                        "page": str(page),
                        "created": f">={start_utc.date().isoformat()}",
                    })
                    url = (
                        f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/"
                        f"{urllib.parse.quote(workflow)}/runs?{query}"
                    )
                    payload = _gh_api_get(url, gh_token)
                    runs = payload.get("workflow_runs") if isinstance(payload, Mapping) else None
                    if not isinstance(runs, list):
                        failure_reason = "github_payload_invalid"
                        raise ValueError("workflow_runs missing or not a list")
                    for run in runs:
                        if not isinstance(run, Mapping):
                            continue
                        if str(run.get("conclusion") or "") != "success":
                            continue
                        updated = str(run.get("updated_at") or run.get("created_at") or "")
                        try:
                            stamp = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                            if stamp.tzinfo is None:
                                raise ValueError("workflow timestamp lacks timezone")
                        except ValueError:
                            continue
                        if start_utc <= stamp < end_utc:
                            matched = True
                            break
                    seen_run_count += len(runs)
                    count = payload.get("total_count")
                    if matched:
                        break
                    if type(count) is not int or count < seen_run_count:
                        failure_reason = "github_payload_invalid"
                        raise ValueError("invalid GitHub total_count for received rows")
                    if count > seen_run_count and len(runs) < 100:
                        failure_reason = "github_pagination_incomplete"
                        raise ValueError("short GitHub page contradicts total_count")
                    if seen_run_count == count:
                        break
                    if page >= 10:
                        failure_reason = "github_pagination_incomplete"
                        raise ValueError("GitHub workflow result limit reached without complete evidence")
                    page += 1
                if matched:
                    break
        except (RuntimeError, ValueError, urllib.error.URLError, TimeoutError) as exc:
            failure = {
                "platform_id": platform_id,
                "workflow": workflow,
                "repository": repository,
                "reason_code": failure_reason,
                "message": str(exc)[:300],
            }
            result.failures.append(failure)
            print(
                f"digest evidence failure {platform_id}/{workflow}: {exc}",
                file=sys.stderr,
            )
            continue
        if not matched:
            continue
        stub = {
            "platform_id": platform_id,
            "strategy_profile": strategy,
            "opaque_account_uid": "",
            "target_id": "",
            "actually_ran": True,
            # Existence only — do not assert verified zero fills/orders.
            "fill_count": None,
            "order_count": None,
            "cycle_count": 1,
            "status": "ok",
            "note": (
                f"github_workflow:{workflow}"
                + (f";evidence_role:{evidence_role}" if evidence_role else "")
            ),
            "field_status": {
                "fill_count": "counts_unknown",
                "order_count": "counts_unknown",
                "cycle_count": "known",
            },
            "reason_code": "github_workflow_existence_only",
            "evidence_provenance": "github_workflow_stub",
        }
        key = digest.identity_key(stub)
        if key in seen:
            continue
        seen.add(key)
        result.sources_matched += 1
        result.rows.append(stub)
    return result


def _merge_count_field(
    winner: dict[str, Any],
    loser: dict[str, Any],
    field_name: str,
) -> None:
    """Fill known counts into unknown winner slots; never invent 0 from stub."""

    w_value, w_status = digest.resolve_count_field(
        winner, field_name, legacy_default_zero=False
    )
    l_value, l_status = digest.resolve_count_field(
        loser, field_name, legacy_default_zero=False
    )
    if w_status == "known" and w_value is not None:
        return
    if l_status == "known" and l_value is not None:
        winner[field_name] = l_value
        status_map = dict(winner.get("field_status") or {})
        status_map[field_name] = "known"
        winner["field_status"] = status_map
        # Drop existence-only reason if we now have a known producer count.
        if str(winner.get("reason_code") or "") == "github_workflow_existence_only":
            if field_name in {"fill_count", "order_count"}:
                winner["reason_code"] = str(loser.get("reason_code") or "") or (
                    "merged_known_counts"
                )


def _should_cross_fill_observation(winner: Mapping[str, Any], loser: Mapping[str, Any]) -> bool:
    """Only cross-fill observation fields when both sides share a real identity."""

    if digest.identity_is_unknown(winner) or digest.identity_is_unknown(loser):
        return False
    return digest.identity_key(winner) == digest.identity_key(loser)


def merge_candidates(*groups: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Merge evidence by identity key; do not mix different accounts/targets.

    Same identity: dedupe, prefer richer known evidence. Unknown-identity rows
    do not cross-fill observation fields onto each other. Different identities
    never merge.
    """

    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    unknown_rows: list[dict[str, Any]] = []

    for group in groups:
        for raw in group:
            platform_id = str(raw.get("platform_id") or "").strip()
            strategy = str(raw.get("strategy_profile") or "").strip()
            if not platform_id or not strategy:
                continue
            candidate = dict(raw)
            key = digest.identity_key(candidate)

            if digest.identity_is_unknown(candidate):
                # Keep unknown-identity rows separate unless exact duplicate key
                # already present with identical provenance note (dedupe only).
                existing_unknown = None
                for idx, row in enumerate(unknown_rows):
                    if digest.identity_key(row) == key and str(row.get("note") or "") == str(
                        candidate.get("note") or ""
                    ) and str(row.get("evidence_provenance") or "") == str(
                        candidate.get("evidence_provenance") or ""
                    ):
                        existing_unknown = (idx, row)
                        break
                if existing_unknown is None:
                    unknown_rows.append(candidate)
                    continue
                idx, existing = existing_unknown
                if digest.observation_score(candidate) >= digest.observation_score(existing):
                    # Replace whole row; do not patch fields across unknowns.
                    unknown_rows[idx] = candidate
                continue

            existing = merged.get(key)
            if existing is None:
                merged[key] = candidate
                continue
            if digest.observation_score(candidate) >= digest.observation_score(existing):
                winner = dict(candidate)
                loser = existing
            else:
                winner = dict(existing)
                loser = candidate
            if _should_cross_fill_observation(winner, loser):
                winner_had_holdings = _has_holdings(winner)
                for field_name in _OBSERVATION_FIELDS:
                    if winner.get(field_name) in (None, "", [], ()):
                        if loser.get(field_name) not in (None, "", [], ()):
                            winner[field_name] = loser[field_name]
                # holdings_scope labels one holdings list; keep it paired with
                # whichever side supplied the holdings that were kept.
                if not winner_had_holdings and _has_holdings(winner):
                    if loser.get("holdings_scope") not in (None, ""):
                        winner["holdings_scope"] = loser["holdings_scope"]
                    else:
                        winner.pop("holdings_scope", None)
                for count_field in ("fill_count", "order_count", "cycle_count"):
                    _merge_count_field(winner, loser, count_field)
                # Preserve identity fields from either side if winner omitted.
                for id_field in ("opaque_account_uid", "target_id"):
                    if not str(winner.get(id_field) or "").strip():
                        if str(loser.get(id_field) or "").strip():
                            winner[id_field] = loser[id_field]
            merged[key] = winner

    return list(merged.values()) + unknown_rows


def strategy_label_from_config(
    config: Mapping[str, Any],
    strategy_profile: str,
    *,
    locale: str = "zh",
) -> str:
    """Public display label from platform-config (not invented numbers)."""

    strategies = config.get("strategies") or {}
    meta = strategies.get(strategy_profile) or {}
    if not isinstance(meta, Mapping):
        return ""
    if str(locale).lower().startswith("en"):
        return str(meta.get("label_en") or meta.get("label") or "").strip()
    return str(meta.get("label") or meta.get("label_en") or "").strip()


def enrich_candidate_labels(
    rows: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    *,
    locale: str = "zh",
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        if not str(row.get("section") or "").strip():
            row["section"] = section_for_profile(
                config, str(row.get("strategy_profile") or "")
            )
        if str(row.get("strategy_profile") or "").strip():
            label = strategy_label_from_config(
                config, str(row.get("strategy_profile") or ""), locale=locale
            )
            if label:
                row["strategy_label"] = label
        out.append(row)
    return out


CANDIDATE_STALE_REASON_OLDER = "candidate_business_day_stale"
CANDIDATE_STALE_REASON_FUTURE = "candidate_business_day_future"
CANDIDATE_STALE_REASON_UNDATED = "candidate_business_day_missing"
CANDIDATE_STALE_REASON_INVALID = "candidate_business_day_invalid"


def _target_key(row: Mapping[str, Any]) -> str:
    """Match the target, then account fallback; never guess across anonymous rows."""
    platform = str(row.get("platform_id") or "").strip()
    target = str(row.get("target_id") or "").strip()
    account = str(row.get("opaque_account_uid") or "").strip()
    if not platform or not (target or account):
        return ""
    identity = [platform, "target" if target else "account", target or account]
    return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def _is_legacy(row: Mapping[str, Any]) -> bool:
    return row.get("legacy") is True or row.get("source_legacy") is True or row.get("source_kind") == "legacy"


def candidate_business_day(row: Mapping[str, Any]) -> tuple[str, str]:
    """Return ``(iso_day, status)`` for one producer candidate row.

    Sources, in order: ``business_day`` (ISO date, first 10 chars) then
    ``as_of`` (ISO datetime → America/New_York calendar day; naive = UTC).
    ``status`` is ``ok`` / ``missing`` / ``invalid``.
    """

    raw_day = str(row.get("business_day") or "").strip()
    if raw_day:
        try:
            return date.fromisoformat(raw_day[:10]).isoformat(), "ok"
        except ValueError:
            return "", "invalid"
    raw_as_of = str(row.get("as_of") or "").strip()
    if raw_as_of:
        try:
            stamp = datetime.fromisoformat(raw_as_of.replace("Z", "+00:00"))
        except ValueError:
            return "", "invalid"
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return stamp.astimezone(NY_TZ).date().isoformat(), "ok"
    return "", "missing"


def partition_candidates_by_business_day(
    rows: Sequence[Mapping[str, Any]],
    business_day: str,
    *,
    config: Mapping[str, Any] | None = None,
    locale: str = "zh",
    stats: dict[str, int] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """NOTIFY-04: keep only candidates whose business day equals the digest day.

    Producer candidates are injected out-of-band (Environment secret / file), so
    a row that was valid yesterday must never be rendered as today's evidence.
    Rows with a different, missing, or unparseable business day are dropped
    (fail closed). Returns ``(fresh_rows, stale_notes)``; stale notes are
    private receipt details (no amounts) for the digest note. The public receipt
    projects counts only, never target fingerprints or display hints.
    """

    target = date.fromisoformat(business_day).isoformat()
    fresh: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    current_targets = {
        _target_key(row) for row in rows
        if not _is_legacy(row) and candidate_business_day(row) == (target, "ok") and _target_key(row)
    }
    suppressed = 0
    for raw in rows:
        row = dict(raw)
        day, status = candidate_business_day(row)
        key = _target_key(row)
        if _is_legacy(row) or (status == "ok" and day < target and key and key in current_targets):
            suppressed += 1
            continue
        if status == "ok" and day == target:
            fresh.append(row)
            continue
        if status == "missing":
            reason = CANDIDATE_STALE_REASON_UNDATED
        elif status == "invalid":
            reason = CANDIDATE_STALE_REASON_INVALID
        elif day < target:
            reason = CANDIDATE_STALE_REASON_OLDER
        else:
            reason = CANDIDATE_STALE_REASON_FUTURE
        profile = str(row.get("strategy_profile") or "").strip()
        label = str(row.get("strategy_label") or "").strip()
        if not label and config is not None and profile:
            label = strategy_label_from_config(config, profile, locale=locale)
        stale.append(
            {
                "platform_id": str(row.get("platform_id") or "").strip(),
                "strategy_profile": profile,
                "strategy_label": label,
                "candidate_business_day": day,
                "digest_business_day": target,
                "reason_code": reason,
                "target_key": key,
                "account_hint": str(row.get("account_hint") or "").strip(),
            }
        )
    if stats is not None:
        stats["suppressed_candidates_count"] = suppressed
    return fresh, stale


def collect_digest_runs(
    *,
    business_day: str,
    config: Mapping[str, Any],
    candidates_path: Path | None = None,
    include_github: bool = True,
    locale: str = "zh",
) -> CollectResult:
    store_failures = candidate_store_failures()
    try:
        loaded_rows, candidates_configured, candidates_loaded = load_candidates_from_file(candidates_path)
    except (OSError, ValueError):
        # Only an already failed intake may downgrade an unusable fallback.
        # Ordinary malformed/absent configured sources retain their rejection.
        if (os.environ.get("DIGEST_CANDIDATES_STORE_STATUS") or "").strip() != "failed":
            raise
        loaded_rows, candidates_configured, candidates_loaded = [], True, False
        store_failures.append({"source_kind": "candidate_store", "reason_code": "candidate_input_unavailable_or_invalid"})
    candidates_configured = candidates_configured or bool(
        (os.environ.get("DIGEST_CANDIDATES_GCS_URI") or "").strip()
    )
    partition_stats: dict[str, int] = {}
    if consolidation_enabled():
        import notification_candidate_adapter as adapter
        current, stale = adapter.partition(loaded_rows, business_day)
        loaded_rows = current + stale
    file_rows, stale_notes = partition_candidates_by_business_day(
        loaded_rows, business_day, config=config, locale=locale, stats=partition_stats
    )
    missing_targets = []
    stale_by_target = {item["target_key"]: item for item in stale_notes if item.get("target_key")}
    current_targets = {_target_key(row) for row in file_rows if _target_key(row)}
    for item in stale_notes:
        missing_targets.append(dict(item, reason_code="EVIDENCE_STALE_BUSINESS_DAY"))
    expected_raw = os.environ.get("DIGEST_EXPECTED_TARGETS_JSON", "").strip()
    expected = json.loads(expected_raw) if expected_raw else digest_contract(config).get("aggregator", {}).get("expected_targets", [])
    if routine_mode() == "daily_digest" and not expected:
        raise ValueError("digest_expected_targets_required")
    if not isinstance(expected, list):
        raise ValueError("digest_expected_targets_invalid")
    if consolidation_enabled():
        adapter.validate_expected(expected)
        # In unified mode a different strategy/account on the same target is
        # not evidence for the protected expected binding.
        observed = {tuple(row.get(name) for name in adapter.IDENTITY[:4]) for row in file_rows}
    for row in expected:
        if not isinstance(row, Mapping) or not _target_key(row) or not row.get("strategy_profile"):
            raise ValueError("digest_expected_targets_invalid")
        key = _target_key(row)
        absent = tuple(row.get(name) for name in adapter.IDENTITY[:4]) not in observed if consolidation_enabled() else key not in current_targets
        if absent and key not in stale_by_target:
            reason = "EVIDENCE_UNREADABLE" if store_failures else (
                "EVIDENCE_NOT_FOUND" if candidates_configured else "SOURCE_NOT_CONFIGURED"
            )
            missing_targets.append({
                "platform_id": row["platform_id"], "strategy_profile": row["strategy_profile"],
                "strategy_label": str(row.get("strategy_label") or ""), "target_key": key,
                "account_hint": str(row.get("account_hint") or ""), "reason_code": reason,
            })
    github_result = GithubEvidenceResult()
    if include_github:
        github_result = collect_from_github_workflows(
            business_day=business_day, config=config
        )
    notification_summary = None
    if consolidation_enabled():
        merged, notification_summary = adapter.aggregate(file_rows, expected)
    else:
        merged = merge_candidates(file_rows, github_result.rows)
    enriched = enrich_candidate_labels(merged, config, locale=locale)
    for row in enriched:
        if all(digest.resolve_count_field(row, name)[1] == "known" for name in ("fill_count", "order_count")):
            continue
        if row.get("reason_code") in digest.MISSING_EVIDENCE_REASONS:
            continue
        row["missing_evidence_reason"] = "EVIDENCE_UNREADABLE" if store_failures else (
            "EVIDENCE_STALE_BUSINESS_DAY" if _target_key(row) in stale_by_target else
            "SOURCE_NOT_CONFIGURED" if not candidates_configured else "EVIDENCE_NOT_FOUND"
        )
    runs = digest.filter_runs_for_digest(enriched)
    coverage = digest.EvidenceCoverage(
        candidates_configured=candidates_configured,
        candidates_loaded=candidates_loaded,
        candidates_count=len(file_rows),
        github_enabled=include_github,
        github_sources_queried=github_result.sources_queried,
        github_sources_matched=github_result.sources_matched,
        github_failures=tuple(github_result.failures),
        candidate_failures=tuple(store_failures),
        stale_candidates=tuple(stale_notes),
        missing_targets=tuple(missing_targets),
        suppressed_candidates_count=partition_stats.get("suppressed_candidates_count", 0),
        legacy_candidates_only=bool(partition_stats.get("suppressed_candidates_count") and not file_rows),
    )
    return CollectResult(runs=runs, coverage=coverage, merged_rows=enriched, notification_summary=notification_summary)


def _run_receipt_dict(entry: digest.DigestRunEntry) -> dict[str, Any]:
    payload = asdict(entry)
    # holdings are nested dataclasses already flattened by asdict
    payload["identity"] = {
        "platform_id": entry.platform_id,
        "strategy_profile": entry.strategy_profile,
        "opaque_account_uid": entry.opaque_account_uid
        or digest.UNKNOWN_ACCOUNT_UID,
        "target_id": entry.target_id or digest.UNKNOWN_TARGET_ID,
    }
    provenance = entry.evidence_provenance or "unknown"
    payload["field_provenance"] = {
        "fill_count": (
            "unknown"
            if entry.fill_count_status == "unknown"
            else ("github_stub" if provenance.startswith("github") else "candidates")
        ),
        "order_count": (
            "unknown"
            if entry.order_count_status == "unknown"
            else ("github_stub" if provenance.startswith("github") else "candidates")
        ),
        "cycle_count": (
            "unknown"
            if entry.cycle_count_status == "unknown"
            else ("github_stub" if provenance.startswith("github") else "candidates")
        ),
        "equity": "candidates" if entry.equity is not None else "absent",
        "holdings": "candidates" if entry.holdings else "absent",
        "signal_summary": "candidates" if entry.signal_summary else "absent",
        "rebalance": (
            "candidates"
            if entry.rebalance_kind or entry.rebalance_conclusion
            else "absent"
        ),
        "evidence_provenance": provenance,
    }
    return payload


def build_digest_payload(
    *,
    business_day: str | None = None,
    locale: str | None = None,
    candidates_path: Path | None = None,
    include_github: bool = True,
    config_path: Path | None = None,
    layout: str | None = None,
) -> tuple[digest.DailyDigestInput, str, CollectResult]:
    config = load_platform_config(config_path)
    contract = digest_contract(config)
    day = resolve_business_day(explicit=business_day)
    loc = resolve_locale(locale)
    collected = collect_digest_runs(
        business_day=day,
        config=config,
        candidates_path=candidates_path,
        include_github=include_github,
        locale=loc,
    )
    evidence_status = "has_runs" if collected.runs else (
        "verified_idle"
        if collected.coverage.is_complete_for_verified_idle
        else "evidence_unknown"
    )
    payload = digest.DailyDigestInput(
        business_day=day,
        window_label=window_label_for(contract),
        locale=loc,
        runs=tuple(collected.runs),
        evidence_coverage=collected.coverage,
        evidence_status=evidence_status,
        layout=resolve_layout(layout),
        window=digest_window_for(day),
        notification_summary=collected.notification_summary,
    )
    from unified_daily_digest import enabled, render
    return payload, render(payload) if enabled() else digest.render_daily_digest(payload), collected


def build_receipt(
    payload: digest.DailyDigestInput,
    collected: CollectResult,
    *,
    message_chars: int,
) -> dict[str, Any]:
    evidence_status = digest.resolve_evidence_status(payload)
    fills = digest.global_total_fills(payload)
    if consolidation_enabled() and (collected.notification_summary or {}).get("counts_unknown"):
        fills = None
    receipt = {
        "schema_version": "qsl.daily_digest_receipt.v1",
        "business_day": payload.business_day,
        "window_label": payload.window_label,
        "layout": digest.normalize_layout(str(payload.layout)),
        "window": dict(payload.window or {}),
        "locale": digest.normalize_locale(str(payload.locale)),
        "run_count": len(payload.runs),
        "total_fills": fills,
        "total_fills_status": "unknown" if fills is None else "known",
        "evidence_status": evidence_status,
        "source_coverage": collected.coverage.to_dict(),
        "failures": list(collected.coverage.github_failures) + list(collected.coverage.candidate_failures),
        "stale_candidates": [dict(item) for item in collected.coverage.stale_candidates],
        "runs": [_run_receipt_dict(entry) for entry in payload.runs],
        "message_chars": message_chars,
    }
    if consolidation_enabled():
        receipt["notification_summary"] = dict(collected.notification_summary or {})
        if receipt["notification_summary"].get("run_count_unknown"):
            receipt["run_count"] = None
    # B11-b: additive dual-write of delivery axes (projection only).
    return delivery_axes.attach_delivery_axes(receipt)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Aggregate QuantSentinel daily digest evidence and render message body."
    )
    parser.add_argument("--business-day", default=os.environ.get("DIGEST_BUSINESS_DAY"))
    parser.add_argument("--locale", default=None)
    parser.add_argument("--candidates", type=Path, default=None)
    parser.add_argument(
        "--no-github",
        action="store_true",
        help="Do not query GitHub Actions workflow conclusions.",
    )
    parser.add_argument(
        "--platform-config",
        type=Path,
        default=None,
        help="Override platform-config.json path.",
    )
    parser.add_argument(
        "--write-json",
        type=Path,
        default=None,
        help="Optional path to write structured digest receipt (no secrets).",
    )
    parser.add_argument(
        "--write-text",
        type=Path,
        default=None,
        help="Optional path to write rendered Telegram body.",
    )
    args = parser.parse_args(argv)

    payload, text, collected = build_digest_payload(
        business_day=args.business_day,
        locale=args.locale,
        candidates_path=args.candidates,
        include_github=not args.no_github,
        config_path=args.platform_config,
    )
    if args.write_text:
        args.write_text.parent.mkdir(parents=True, exist_ok=True)
        args.write_text.write_text(text + "\n", encoding="utf-8")
    if args.write_json:
        receipt = build_receipt(payload, collected, message_chars=len(text))
        args.write_json.parent.mkdir(parents=True, exist_ok=True)
        args.write_json.write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
