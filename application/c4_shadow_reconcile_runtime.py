"""Runtime wiring for RECONCILE_ONLY → C4 zero-submit diagnostics.

Default path never invents a final RiskEngine APPROVE. When no real assessment
provider can produce a same-account / same-strategy / same-UTC-as_of assessment,
the result is an explicit C4 PARKED envelope with reason ``assessment missing``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any

from quant_platform_kit.common.broker_reconciliation import BrokerReconciliationEvidence
from us_equity_strategies.research.c4_shadow_zero_submit_cycle import (
    consume_c4_shadow_zero_submit_cycle,
)

from application.broker_reconciliation import SchwabReconciliationObservations
from application.c4_shadow_reconcile_assembly import (
    assemble_c4_shadow_zero_submit_from_reconcile_observations,
)

FinalRiskAssessmentProvider = Callable[..., Mapping[str, object] | None]

_ASSESSMENT_MISSING = "assessment missing"
_SAFE_DIAGNOSTIC_KEYS = (
    "status",
    "reason_codes",
    "no_order",
    "proposed_orders_count",
    "submission_attempted",
    "execution_permitted",
    "execution_authorized",
    "evidence_digest",
)


def _refresh_evidence_digest(result: dict[str, object]) -> dict[str, object]:
    result.pop("evidence_digest", None)
    encoded = json.dumps(
        result, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    result["evidence_digest"] = hashlib.sha256(encoded).hexdigest()
    return result


def _parked_assessment_missing() -> dict[str, object]:
    result = consume_c4_shadow_zero_submit_cycle(
        account_snapshot={},
        open_orders_snapshot={},
        risk_engine_result={},
    )
    result["reason_codes"] = (_ASSESSMENT_MISSING,)
    return _refresh_evidence_digest(result)


def _resolve_assessment(
    *,
    final_risk_assessment_provider: FinalRiskAssessmentProvider | None,
    account_id: str,
    strategy_id: str,
    as_of: str,
    observations: SchwabReconciliationObservations,
    reconciliation_evidence: BrokerReconciliationEvidence,
) -> Mapping[str, object] | None:
    if not callable(final_risk_assessment_provider):
        return None
    try:
        assessment = final_risk_assessment_provider(
            account_id=account_id,
            strategy_id=strategy_id,
            as_of=as_of,
            observations=observations,
            reconciliation_evidence=reconciliation_evidence,
        )
    except Exception:
        return None
    if not isinstance(assessment, Mapping) or not assessment:
        return None
    return assessment


def run_c4_shadow_zero_submit_for_reconcile(
    *,
    observations: SchwabReconciliationObservations,
    reconciliation_evidence: BrokerReconciliationEvidence,
    account_id: str,
    strategy_id: str,
    as_of: str,
    final_risk_assessment_provider: FinalRiskAssessmentProvider | None = None,
) -> dict[str, object]:
    """Produce an auditable C4 zero-submit result for the reconcile path.

    Does not open a broker client, submit or cancel orders, or invent RiskEngine
    APPROVE assessments from empty/synthetic inputs.
    """

    assessment = _resolve_assessment(
        final_risk_assessment_provider=final_risk_assessment_provider,
        account_id=account_id,
        strategy_id=strategy_id,
        as_of=as_of,
        observations=observations,
        reconciliation_evidence=reconciliation_evidence,
    )
    if assessment is None:
        return _parked_assessment_missing()
    return assemble_c4_shadow_zero_submit_from_reconcile_observations(
        observations=observations,
        reconciliation_evidence=reconciliation_evidence,
        account_id=account_id,
        strategy_id=strategy_id,
        as_of=as_of,
        final_risk_assessment=assessment,
    )


def c4_shadow_zero_submit_to_safe_diagnostics(
    result: Mapping[str, object],
) -> dict[str, object]:
    """Redact C4 output for internal runtime reports; never include account rows."""

    reason_codes = result.get("reason_codes") or ()
    if isinstance(reason_codes, (str, bytes)):
        reason_codes = (str(reason_codes),)
    else:
        reason_codes = tuple(str(item) for item in reason_codes)
    proposed = result.get("proposed_orders") or []
    proposed_count = len(proposed) if isinstance(proposed, (list, tuple)) else 0
    digest = result.get("evidence_digest")
    safe: dict[str, object] = {
        "status": str(result.get("status") or "PARKED"),
        "reason_codes": list(reason_codes),
        "no_order": result.get("no_order") is True,
        "proposed_orders_count": proposed_count,
        "submission_attempted": result.get("submission_attempted") is True,
        "execution_permitted": result.get("execution_permitted") is True,
        "execution_authorized": result.get("execution_authorized") is True,
    }
    if isinstance(digest, str) and len(digest) == 64:
        safe["evidence_digest"] = digest
    # Drop any unexpected keys by construction.
    return {key: safe[key] for key in _SAFE_DIAGNOSTIC_KEYS if key in safe}


def resolve_c4_reconcile_binding(
    *,
    runtime_target: Any,
    reconciliation_evidence: BrokerReconciliationEvidence,
) -> tuple[str, str, str] | None:
    """Bind C4 identity to the runtime target and evidence observed_at (UTC)."""

    account_id = str(getattr(runtime_target, "account_scope", "") or "").strip()
    strategy_id = str(getattr(runtime_target, "strategy_profile", "") or "").strip()
    try:
        as_of = str(reconciliation_evidence.to_dict().get("observed_at") or "").strip()
    except Exception:
        return None
    if not account_id or not strategy_id or not as_of:
        return None
    return account_id, strategy_id, as_of


def build_reconcile_c4_report_attachment(
    *,
    observations: SchwabReconciliationObservations,
    reconciliation_evidence: BrokerReconciliationEvidence,
    runtime_target: Any,
    final_risk_assessment_provider: FinalRiskAssessmentProvider | None = None,
) -> dict[str, object]:
    """Build summary/diagnostics fragments for the internal reconcile report only."""

    binding = resolve_c4_reconcile_binding(
        runtime_target=runtime_target,
        reconciliation_evidence=reconciliation_evidence,
    )
    if binding is None:
        result = _parked_assessment_missing()
    else:
        account_id, strategy_id, as_of = binding
        result = run_c4_shadow_zero_submit_for_reconcile(
            observations=observations,
            reconciliation_evidence=reconciliation_evidence,
            account_id=account_id,
            strategy_id=strategy_id,
            as_of=as_of,
            final_risk_assessment_provider=final_risk_assessment_provider,
        )
    safe = c4_shadow_zero_submit_to_safe_diagnostics(result)
    return {
        "summary": {
            "c4_shadow_status": safe["status"],
            "c4_shadow_no_order": safe["no_order"],
            "c4_shadow_submission_attempted": safe["submission_attempted"],
            "c4_shadow_execution_permitted": safe["execution_permitted"],
        },
        "diagnostics": {"c4_shadow_zero_submit": safe},
    }


__all__ = [
    "FinalRiskAssessmentProvider",
    "build_reconcile_c4_report_attachment",
    "c4_shadow_zero_submit_to_safe_diagnostics",
    "resolve_c4_reconcile_binding",
    "run_c4_shadow_zero_submit_for_reconcile",
]
