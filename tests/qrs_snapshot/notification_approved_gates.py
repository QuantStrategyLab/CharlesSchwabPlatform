"""Validate and render the two approved Schwab notification gates, per run."""
from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping

CONSECUTIVE = "consecutive_loss_limit_exceeded"
DAILY = "daily_loss_limit_exceeded"
ROUTINE = {"routine_cycle", "healthy_heartbeat", "expected_block_observed"}


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def clean_reasons(values):
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= 2:
        return []
    reasons, seen = [], set()
    for value in values:
        if not isinstance(value, Mapping):
            return []
        code, observed, limit = (value.get(k) for k in ("reason_code", "observed_value", "limit"))
        if not isinstance(code, str) or code in seen:
            return []
        seen.add(code)
        if code == CONSECUTIVE:
            if not (type(observed) is type(limit) is int and 5 == limit < observed <= 999999999):
                return []
        elif code == DAILY:
            equity = value.get("equity_usd")
            if not all(_number(v) for v in (observed, limit, equity)) or not (observed >= limit > 0 and equity > 0):
                return []
            if not math.isclose(limit, equity * (0.01 + 0.04 * 2000 / (2000 + equity)), rel_tol=1e-12):
                return []
        else:
            return []
        reason = dict(reason_code=code, observed_value=observed, limit=limit)
        if code == DAILY:
            reason["equity_usd"] = equity
        reasons.append(reason)
    return reasons


def merge_reasons(previous, row, merged):
    """Union compatible observations of this run; conflicts stay disqualifying."""
    by_code = {}
    invalid = previous.get("_approved_gate_conflict") or row.get("_approved_gate_conflict")
    for observation in (previous, row):
        block = observation.get("block_reason")
        if block and (not isinstance(block, Mapping) or block.get("reason_code") not in {"strategy_risk_rejected", CONSECUTIVE}):
            invalid = True
        if observation.get("reason_code") not in (None, "", "execution_evidence_missing_or_attention"):
            invalid = True
        if "approved_gate_reasons" not in observation:
            if observation.get("schema_version") != "qsl.notification_event.v1" and (block or observation.get("status") == "alert"):
                invalid = True
            continue
        reasons = clean_reasons(observation["approved_gate_reasons"])
        if not reasons:
            invalid = True
        for reason in reasons:
            code = reason["reason_code"]
            if code in by_code and by_code[code] != reason:
                invalid = True
            by_code[code] = reason
    if by_code:
        merged["approved_gate_reasons"] = list(by_code.values())
    if invalid:
        merged["_approved_gate_conflict"] = True


def validated_reasons(row, facts):
    """Archive-approved structured evidence plus its own bound event facts only.

    Raw events alone cannot approve a gate. Receipt validation belongs to the
    producer's archive projection; an unknown/urgent event always overrides it.
    """
    reasons = clean_reasons(row.get("approved_gate_reasons"))
    if not reasons or row.get("_approved_gate_conflict") or row.get("platform_id") != "schwab":
        return []
    if row.get("schema_version") == "qsl.notification_event.v1" or row.get("actually_ran") is not True:
        return []
    if not all(isinstance(row.get(k), str) and row[k].strip() for k in
               ("platform_id", "opaque_account_uid", "target_id", "strategy_profile", "source_run_id")):
        return []
    from daily_digest_notify import resolve_count_field
    if resolve_count_field(row, "order_count") != (0, "known") or resolve_count_field(row, "cycle_count") != (1, "known"):
        return []
    fills, _ = resolve_count_field(row, "fill_count")
    if fills not in (None, 0):
        return []
    block = row.get("block_reason")
    if not isinstance(block, Mapping) or block.get("reason_code") not in {"strategy_risk_rejected", CONSECUTIVE}:
        return []
    by_code = {r["reason_code"]: r for r in reasons}
    if block["reason_code"] == CONSECUTIVE:
        if clean_reasons([block]) != [by_code.get(CONSECUTIVE)]:
            return []
    if row.get("reason_code") not in (None, "", "execution_evidence_missing_or_attention"):
        return []
    for fact in facts:
        if fact.get("event_kind") not in ROUTINE:
            return []
        if any(fact.get(k) != row.get(k) for k in
               ("platform_id", "opaque_account_uid", "target_id", "strategy_profile", "source_run_id")):
            return []
        event_reasons = clean_reasons(fact.get("approved_gate_reasons"))
        if "approved_gate_reasons" in fact and (not event_reasons or any(by_code.get(r["reason_code"]) != r for r in event_reasons)):
            return []
        if fact.get("event_kind") == "expected_block_observed":
            evidence = "approved-gate:" + hashlib.sha256(json.dumps(event_reasons, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            if not event_reasons or fact.get("expected_block_evidence") != evidence:
                return []
    return reasons


def render_reasons(reasons, locale):
    parts = []
    for reason in clean_reasons(reasons):
        observed, limit = str(reason["observed_value"]), str(reason["limit"])
        if reason["reason_code"] == CONSECUTIVE:
            parts.append(f"连亏熔断：连续亏损 {observed}>{limit}" if locale == "zh" else f"Consecutive-loss gate: consecutive losses {observed}>{limit}")
        else:
            equity = str(reason["equity_usd"])
            rule = f"E*(0.01+0.04*2000/(2000+E)), E={equity} USD"
            parts.append(f"日亏损上限：日亏损 {observed}≥{limit} USD（{rule}）" if locale == "zh" else f"Daily-loss gate: daily loss {observed}>={limit} USD ({rule})")
    if not parts:
        return ""
    parts.append("本轮未下单" if locale == "zh" else "No order this run")
    return "；".join(parts) if locale == "zh" else "; ".join(parts)
