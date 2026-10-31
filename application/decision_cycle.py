"""Per-cycle decision clock and independently supplied artifact expectations."""
from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
from inspect import signature
from typing import Mapping

_current = ContextVar("strategy_decision_cycle", default=None)


def _owner():
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return threading.get_ident(), task


@contextmanager
def decision_cycle(*, decision_time=None, expected_sha256_by_reference=None):
    now = decision_time if decision_time is not None else datetime.now(timezone.utc).replace(microsecond=0)
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("decision_time must be timezone-aware")
    if now.microsecond:
        raise ValueError("decision_time must have whole-second precision")
    if expected_sha256_by_reference is not None and not isinstance(expected_sha256_by_reference, Mapping):
        raise ValueError("expected artifact digests must be a mapping")
    context = {"active": True, "owner": _owner(), "decision_time": now.astimezone(timezone.utc),
               "expected_sha256_by_reference": (dict(expected_sha256_by_reference)
                   if expected_sha256_by_reference is not None else None)}
    token = _current.set(context)
    try:
        yield context
    finally:
        context["active"] = False
        _current.reset(token)


def cycle_handler(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        if current_cycle() is not None:
            return fn(*args, **kwargs)
        with decision_cycle():
            return fn(*args, **kwargs)
    return wrapped


def current_cycle():
    context = _current.get()
    return context if context is not None and context["active"] and context["owner"] == _owner() else None


def plugin_load_options(loader):
    """Forward only supported inputs; an older dependency cannot verify them."""
    context = current_cycle()
    if context is None:
        return {}
    parameters = signature(loader).parameters
    expected = context["expected_sha256_by_reference"]
    if expected is not None and "expected_sha256_by_reference" not in parameters:
        raise ValueError("configured loader cannot verify expected artifact digests")
    options = {}
    if "decision_time" in parameters:
        options["decision_time"] = context["decision_time"]
    if expected is not None:
        options["expected_sha256_by_reference"] = expected
    return options



def receipt_capabilities(ctx):
    """Describe only loaded bytes and the exact metadata supplied to strategy."""
    cycle = current_cycle()
    if cycle is None:
        return {}
    clock = cycle["decision_time"]
    result = {"decision_time": clock, "logical_evaluation_time": clock}
    report = cycle.get("report")
    if not isinstance(report, dict) or not report.get("run_id"):
        return result
    loaded = report.get("runtime_release_receipt") or {}
    release = loaded.get("strategy_release") or {} if isinstance(loaded, Mapping) else {}
    metadata = getattr(ctx.portfolio, "metadata", {}) or {}
    injected = metadata.get("strategy_plugins", {}) if isinstance(metadata, Mapping) else {}
    observations = []
    for signal in cycle.get("plugin_signals", ()):
        reference = getattr(signal, "source_uri", None) or getattr(signal, "local_path", None)
        digest = getattr(signal, "artifact_sha256", None)
        status = getattr(signal, "digest_status", "unknown")
        if not isinstance(reference, str) or not reference:
            continue
        payload = injected.get(getattr(signal, "plugin", None)) if isinstance(injected, Mapping) else None
        try:
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
            payload_digest = hashlib.sha256(canonical).hexdigest() if isinstance(payload, Mapping) else None
        except (TypeError, ValueError):
            payload_digest = None
        loader_payload_digest = getattr(signal, "consumer_payload_sha256", None)
        if (not isinstance(digest, str) or status not in {"verified", "unverified"}
                or not isinstance(loader_payload_digest, str) or payload_digest != loader_payload_digest):
            observations.append({"reference": reference, "consumer_key": getattr(signal, "plugin", ""),
                "status": "unavailable", "artifact_sha256": None, "digest_status": "unknown",
                "availability_at": None, "observed_at": None, "consumer_payload_sha256": None})
        else:
            observations.append({"reference": reference, "consumer_key": getattr(signal, "plugin", ""),
                "status": "observed", "artifact_sha256": digest, "digest_status": status,
                "availability_at": None, "observed_at": getattr(signal, "observed_at", None),
                "consumer_payload_sha256": loader_payload_digest})
    result["decision_consumption_receipt"] = {"run_id": report["run_id"], "decision_at": clock,
        "strategy_revision": release.get("strategy_revision"), "config_sha256": release.get("config_sha256"),
        "observations": observations}
    return result


def capture_decision(decision):
    cycle = current_cycle()
    if cycle is not None:
        cycle["decision"] = decision
    return decision


def attach_cycle_decision_receipt(report):
    cycle = current_cycle()
    if cycle is None or cycle.get("report") is not report:
        return
    decision = cycle.get("decision")
    diagnostics = getattr(decision, "diagnostics", {})
    receipt = diagnostics.get("decision_consumption_receipt") if isinstance(diagnostics, Mapping) else None
    if receipt is None:
        report.setdefault("diagnostics", {})["decision_consumption_receipt_status"] = "unavailable"
        return
    try:
        from quant_platform_kit.common.decision_consumption_receipts import attach_decision_consumption_receipt
        attach_decision_consumption_receipt(report, receipt, post_risk_decision=decision)
        if report.get("execution_receipt") is not None:
            try:
                from quant_platform_kit.common.decision_consumption_receipts import attach_decision_execution_link
                attach_decision_execution_link(report, receipt)
            except Exception:
                report.setdefault("diagnostics", {})["decision_execution_link_status"] = "unavailable"
    except Exception:
        report.setdefault("diagnostics", {})["decision_consumption_receipt_status"] = "unavailable"
