"""Thin adapter: map Schwab halted-cycle facts onto QPK execution_kernel T2/T3.

N13: redundant fail-closed only. Does not own claims or change allow paths.
T1 is intentionally not enforced here (dedup-off live paths may lack a claim).
"""

from __future__ import annotations

from quant_platform_kit.execution_kernel import (
    ExecutionGuardDecision,
    SubmissionCertainty,
    SubmissionRetrySnapshot,
    SubmissionUnknownSnapshot,
    deny_blind_retry_after_uncertain_transport,
    deny_new_cycle_when_submission_unknown,
)


def evaluate_halted_resubmit_guards(
    *,
    submission_outcome_unknown: bool,
) -> tuple[ExecutionGuardDecision, ExecutionGuardDecision]:
    """Evaluate T2/T3 when the cycle already halted and a further submit is requested.

    When ``submission_outcome_unknown`` is true, both guards must deny (reconcile
    before new-cycle submit or blind retry). When false, T2/T3 allow — the caller
    still keeps its local ``submission_halted`` early-return.
    """
    certainty = (
        SubmissionCertainty.UNKNOWN
        if submission_outcome_unknown
        else SubmissionCertainty.TERMINAL
    )
    t2 = deny_new_cycle_when_submission_unknown(
        SubmissionUnknownSnapshot(
            submission_certainty=certainty,
            requesting_new_cycle_submit=True,
        )
    )
    t3 = deny_blind_retry_after_uncertain_transport(
        SubmissionRetrySnapshot(
            transport_uncertain=bool(submission_outcome_unknown),
            reconciled=False,
            requesting_blind_retry=True,
        )
    )
    return t2, t3


def should_block_halted_resubmit(*, submission_outcome_unknown: bool) -> bool:
    """Return True when T2 or T3 denies a further submit after halt.

    Safe for wiring beside an existing ``if submission_halted: return False``:
    unknown outcomes → deny; otherwise allow from kernel (local halt still wins).
    """
    t2, t3 = evaluate_halted_resubmit_guards(
        submission_outcome_unknown=submission_outcome_unknown,
    )
    return (not t2.allowed) or (not t3.allowed)
