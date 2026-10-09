"""Thin adapter: map Schwab facts onto QPK execution_kernel T1–T3.

N13 / ADR-B (N13-schwab-t1-adr):
- T2/T3: redundant consult on halted resubmit (#505); local halt still wins.
- T1: always consultable; enforce only when ``execution_kernel_t1_enforce``
  (or env SCHWAB_EXECUTION_KERNEL_T1_ENFORCE) is on **and** dedup is enabled.
  Dedup-off live is never blocked by T1 under this ADR; never forge
  ``identity_held=True`` for dedup-off.
"""

from __future__ import annotations

from quant_platform_kit.execution_kernel import (
    ExecutionGuardDecision,
    ExecutionMode,
    LiveSubmitSnapshot,
    SubmissionCertainty,
    SubmissionRetrySnapshot,
    SubmissionUnknownSnapshot,
    deny_blind_retry_after_uncertain_transport,
    deny_live_submit_without_durable_identity,
    deny_new_cycle_when_submission_unknown,
)


def consult_t1_live_submit(
    *,
    identity_held: bool,
    dry_run_bypass: bool = False,
    dry_run_only: bool = False,
    mode: ExecutionMode | None = None,
) -> ExecutionGuardDecision:
    """Consult T1 for a live-cycle submit attempt.

    Pass ``identity_held`` honestly (claim acquired). Do **not** set it True
    merely because ``execution_dedup_enabled`` is False.
    """
    resolved_mode = mode
    if resolved_mode is None:
        resolved_mode = ExecutionMode.DRY_RUN if dry_run_only else ExecutionMode.LIVE
    return deny_live_submit_without_durable_identity(
        LiveSubmitSnapshot(
            mode=resolved_mode,
            dry_run_bypass=bool(dry_run_bypass),
            identity_held=bool(identity_held),
        )
    )


def should_block_live_submit_for_t1(
    *,
    enforce: bool,
    execution_dedup_enabled: bool,
    identity_held: bool,
    dry_run_bypass: bool = False,
    dry_run_only: bool = False,
    mode: ExecutionMode | None = None,
) -> bool:
    """Return True only when enforce is on, dedup gate applies, and T1 denies.

    When ``execution_dedup_enabled`` is False the T1 enforce predicate is N/A
    (ADR-B): never block and never pretend identity is held.
    When ``enforce`` is False: consult is the caller's job; this returns False.
    """
    if not enforce:
        return False
    if not execution_dedup_enabled:
        return False
    decision = consult_t1_live_submit(
        identity_held=identity_held,
        dry_run_bypass=dry_run_bypass,
        dry_run_only=dry_run_only,
        mode=mode,
    )
    return not decision.allowed


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
