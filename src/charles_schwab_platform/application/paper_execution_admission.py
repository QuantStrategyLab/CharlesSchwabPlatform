"""Fail-closed PAPER admission adapter for Charles Schwab command consumers.

This module deliberately has no broker, portfolio, or order-preview imports.
PAPER command consumers call it before calculating a preview or simulating an
execution.  The reusable QPK contract verifies the immutable command and its
embedded deterministic-risk receipt; the shared runtime gate verifies the
running release attestation immediately before any paper-side effect.

The adapter is opt-in and defaults to disabled.  It never authorizes a live
runtime, and it does not alter the existing Schwab rebalance or live-order
paths.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from quant_platform_kit.common.execution_commands import ExecutionCommand
from quant_platform_kit.common.paper_execution_admission import (
    PaperExecutionAdmissionDecision,
    evaluate_paper_execution_admission,
)
from quant_platform_kit.common.runtime_command_gate import (
    RuntimeCommandAction,
    RuntimeCommandExposureEffect,
    RuntimeCommandGateDecision,
    RuntimeCommandGateEnforcement,
    RuntimeCommandGatePolicy,
    evaluate_runtime_command_gate,
)
from quant_platform_kit.common.strategy_release import StrategyReleaseIdentity


SCHWAB_PAPER_EXECUTION_ADMISSION_SCHEMA_VERSION = "schwab.paper-execution-admission.v1"
_PAPER_ADMISSION_GATE_POLICY = RuntimeCommandGatePolicy(
    enforcement=RuntimeCommandGateEnforcement.ENFORCE,
)
_PAPER_RUNTIME_REQUIRED = "paper_runtime_required"
_ADMISSION_EVALUATION_FAILED = "paper_admission_evaluation_failed"


@dataclass(frozen=True)
class SchwabPaperExecutionAdmissionResult:
    """Safe admission outcome consumable by existing runtime reports.

    ``preview_allowed`` and ``paper_execution_allowed`` are deliberately both
    false for a blocked command.  A caller must not calculate or persist a
    paper preview until this adapter has admitted the command.
    """

    enabled: bool
    paper_runtime: bool
    status: str
    preview_allowed: bool
    paper_execution_allowed: bool
    reason_codes: tuple[str, ...] = ()
    paper_risk_admission: PaperExecutionAdmissionDecision | None = None
    runtime_command_gate: RuntimeCommandGateDecision | None = None

    def to_report_payload(self) -> dict[str, object]:
        """Return redacted report data without command intent or order details."""

        payload: dict[str, object] = {
            "schema_version": SCHWAB_PAPER_EXECUTION_ADMISSION_SCHEMA_VERSION,
            "enabled": self.enabled,
            "paper_runtime": self.paper_runtime,
            "status": self.status,
            "preview_allowed": self.preview_allowed,
            "paper_execution_allowed": self.paper_execution_allowed,
            "reason_codes": list(self.reason_codes),
        }
        if self.paper_risk_admission is not None:
            payload["paper_risk_admission"] = {
                "command_id": self.paper_risk_admission.command_id,
                "disposition": self.paper_risk_admission.disposition.value,
                "integrity_findings": list(self.paper_risk_admission.integrity_findings),
                "receipt_sha256": self.paper_risk_admission.receipt_sha256,
            }
        if self.runtime_command_gate is not None:
            payload["runtime_command_gate"] = self.runtime_command_gate.to_receipt()
        return payload


@dataclass(frozen=True)
class SchwabPaperExecutionAdmissionAdapter:
    """Apply the shared PAPER admission contract at the Schwab boundary."""

    enabled: bool = False
    dry_run_only: bool = False
    runtime_execution_mode: str = ""

    @classmethod
    def from_runtime_settings(cls, settings: object) -> "SchwabPaperExecutionAdmissionAdapter":
        runtime_target = getattr(settings, "runtime_target", None)
        return cls(
            enabled=bool(getattr(settings, "paper_execution_admission_enabled", False)),
            dry_run_only=bool(getattr(settings, "dry_run_only", False)),
            runtime_execution_mode=str(getattr(runtime_target, "execution_mode", "") or ""),
        )

    @property
    def paper_runtime(self) -> bool:
        return self.dry_run_only and self.runtime_execution_mode.strip().lower() == "paper"

    def admit(
        self,
        *,
        command: ExecutionCommand | None,
        as_of_session: object,
        runtime_release_receipt: Mapping[str, Any] | None,
        expected_strategy_release: StrategyReleaseIdentity | Mapping[str, object] | None,
        exposure_effect: RuntimeCommandExposureEffect | str = RuntimeCommandExposureEffect.UNKNOWN,
    ) -> SchwabPaperExecutionAdmissionResult:
        """Admit one command before any PAPER preview or simulation.

        A successful result only permits a PAPER-side preview/simulation.  This
        adapter has no broker dependency and cannot submit an order itself.
        """

        if not self.enabled:
            return SchwabPaperExecutionAdmissionResult(
                enabled=False,
                paper_runtime=self.paper_runtime,
                status="disabled",
                preview_allowed=False,
                paper_execution_allowed=False,
            )
        if not self.paper_runtime:
            return SchwabPaperExecutionAdmissionResult(
                enabled=True,
                paper_runtime=False,
                status="blocked",
                preview_allowed=False,
                paper_execution_allowed=False,
                reason_codes=(_PAPER_RUNTIME_REQUIRED,),
            )

        try:
            paper_risk_admission = evaluate_paper_execution_admission(
                command=command,
                expected_strategy_release=expected_strategy_release,
            )
            runtime_command_gate = evaluate_runtime_command_gate(
                action=RuntimeCommandAction.SUBMIT,
                exposure_effect=exposure_effect,
                command=command,
                as_of_session=as_of_session,
                runtime_release_receipt=runtime_release_receipt,
                expected_strategy_release=expected_strategy_release,
                integrity_findings=paper_risk_admission.integrity_findings,
                policy=_PAPER_ADMISSION_GATE_POLICY,
            )
        except Exception:
            # The command boundary must never surface untrusted payload data or
            # fall through to a preview after a malformed external object.
            return SchwabPaperExecutionAdmissionResult(
                enabled=True,
                paper_runtime=True,
                status="blocked",
                preview_allowed=False,
                paper_execution_allowed=False,
                reason_codes=(_ADMISSION_EVALUATION_FAILED,),
            )

        allowed = bool(runtime_command_gate.broker_write_allowed)
        return SchwabPaperExecutionAdmissionResult(
            enabled=True,
            paper_runtime=True,
            status="admitted" if allowed else "blocked",
            preview_allowed=allowed,
            paper_execution_allowed=allowed,
            reason_codes=tuple(runtime_command_gate.reasons),
            paper_risk_admission=paper_risk_admission,
            runtime_command_gate=runtime_command_gate,
        )


__all__ = (
    "SCHWAB_PAPER_EXECUTION_ADMISSION_SCHEMA_VERSION",
    "SchwabPaperExecutionAdmissionAdapter",
    "SchwabPaperExecutionAdmissionResult",
)
