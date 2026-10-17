"""Read-only notification annotations; failures never change a trading decision."""
from __future__ import annotations


def annotate_decision(plan, decision):
    try:
        from .unified import enabled
        if not enabled():
            return
        from .approved_gates import decision_gate_reasons
        from .block_reason import decision_block_reason
        reason = decision_block_reason(decision)
        if reason:
            plan.setdefault("execution", {})["notification_block_reason"] = reason
        approved = decision_gate_reasons(decision)
        if approved:
            plan.setdefault("execution", {})["notification_approved_gates"] = approved
    except Exception:
        print("unified_notification_failed reason=decision_observation_unknown", flush=True)


def annotate_daily_gate(execution, admission, snapshot, portfolio, decision, *, resolution, formula):
    try:
        from .unified import enabled
        if not enabled():
            return {}
        from .approved_gates import daily_gate_reasons
        approved = daily_gate_reasons(admission, snapshot, resolution(), formula())
        kwargs = {"guard_reason_codes": tuple(admission.reason_codes or ())}
        if (approved and isinstance(execution, dict) and decision.level.value == "action"
                and set(decision.reason_codes) <= {"new_risk_prohibited"}):
            previous = execution.get("notification_approved_gates") or []
            execution["notification_approved_gates"] = previous if approved[0] in previous else [*previous, *approved]
            kwargs["approved_gate_reasons"] = approved
        return kwargs
    except Exception:
        print("unified_notification_failed reason=daily_gate_observation_unknown", flush=True)
        return {}


def annotate_summary(summary, result, *, dry_run):
    try:
        from .unified import enabled
        if not enabled():
            return
        from .digest_snapshot.digest_candidates import cycle_evidence
        from .approved_gates import clean_gate_reasons
        execution = dict(getattr(result, "execution", {}) or {})
        summary["notification_digest"] = cycle_evidence(execution,
            tuple(getattr(result, "submitted_orders", ()) or ()), dry_run=dry_run)
        for key in ("notification_events", "notification_block_reason", "notification_attention_reason_codes", "notification_gate_completion"):
            if execution.get(key):
                summary[key] = execution[key]
        if execution.get("notification_approved_gates"):
            summary["notification_approved_gates"] = clean_gate_reasons(execution["notification_approved_gates"])
    except Exception:
        print("unified_notification_failed reason=summary_observation_unknown", flush=True)


def publish_cycle_safely(result, *, config, legacy):
    attempted = False
    original_error = None

    def original():
        nonlocal attempted, original_error
        attempted = True
        try:
            return legacy()
        except Exception as exc:
            original_error = exc
            raise
    try:
        from .unified import publish_cycle
        return publish_cycle(result, config=config, legacy=original)
    except Exception:
        if original_error is not None:
            raise original_error
        print("unified_notification_failed reason=cycle_observation_unknown", flush=True)
        if not attempted:
            return original()


def publish_incident(legacy, **kwargs):
    attempted = False
    original_error = None
    def original():
        nonlocal attempted, original_error
        attempted = True
        try:
            return legacy()
        except Exception as exc:
            original_error = exc
            raise
    try:
        from .unified import publish_incident as publish
        return publish(original, **kwargs)
    except Exception:
        if original_error is not None:
            raise original_error
        print("unified_notification_failed reason=incident_observation_unknown", flush=True)
        if not attempted:
            return original()


from contextlib import contextmanager


@contextmanager
def notification_context(report):
    manager = None
    try:
        from .unified import notification_context as context
        manager = context(report)
        manager.__enter__()
    except Exception:
        manager = None
        print("unified_notification_failed reason=context_observation_unknown", flush=True)
    try:
        yield
    finally:
        if manager is not None:
            try:
                manager.__exit__(None, None, None)
            except Exception:
                print("unified_notification_failed reason=context_release_unknown", flush=True)
