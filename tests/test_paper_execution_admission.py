from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from application.paper_execution_admission import SchwabPaperExecutionAdmissionAdapter  # noqa: E402
from quant_platform_kit.common.execution_commands import ExecutionCommand  # noqa: E402
from quant_platform_kit.common.paper_execution_admission import (  # noqa: E402
    PAPER_RISK_ADMISSION_RECEIPT_INTENT_FIELD,
    PaperRiskAdmissionDisposition,
    build_paper_risk_admission_receipt,
)
from quant_platform_kit.common.runtime_command_gate import RuntimeCommandExposureEffect  # noqa: E402
from quant_platform_kit.common.strategy_release import build_runtime_loaded_receipt  # noqa: E402


def _release_identity(*, release_id: str = "soxl-p2-v3.20260824") -> dict[str, str]:
    return {
        "release_id": release_id,
        "manifest_sha256": "a" * 64,
        "strategy_revision": "soxl-p2-v3",
        "config_sha256": "b" * 64,
        "risk_policy_sha256": "c" * 64,
        "evidence_sha256": "d" * 64,
        "plugin_bundle_sha256": "e" * 64,
        "effective_session": "2026-08-25",
    }


def _command(*, include_receipt: bool = True) -> ExecutionCommand:
    release = _release_identity()
    intent: dict[str, object] = {"strategy_release": release, "targets": {"SOXL": 0.25}}
    if include_receipt:
        intent[PAPER_RISK_ADMISSION_RECEIPT_INTENT_FIELD] = build_paper_risk_admission_receipt(
            strategy_profile="soxl_soxx_trend_income",
            release_id=release["release_id"],
            risk_policy_sha256=release["risk_policy_sha256"],
            decision_digest="d" * 64,
            effective_session="2026-08-25",
            disposition=PaperRiskAdmissionDisposition.ALLOW_NEW_RISK,
            reason_codes=(),
        ).to_dict()
    return ExecutionCommand.from_decision(
        platform="schwab",
        account_scope="paper",
        strategy_profile="soxl_soxx_trend_income",
        execution_mode="paper",
        signal_date="2026-08-24",
        effective_date="2026-08-25",
        execution_timing_contract="next_trading_day",
        decision_digest="d" * 64,
        intent=intent,
        created_at="2026-08-24T20:00:00+00:00",
    )


def _paper_adapter(*, enabled: bool = True) -> SchwabPaperExecutionAdmissionAdapter:
    return SchwabPaperExecutionAdmissionAdapter(
        enabled=enabled,
        dry_run_only=True,
        runtime_execution_mode="paper",
    )


def _admit(adapter: SchwabPaperExecutionAdmissionAdapter, command: ExecutionCommand | None):
    release = _release_identity()
    return adapter.admit(
        command=command,
        as_of_session="2026-08-25",
        runtime_release_receipt=build_runtime_loaded_receipt(strategy_release=release),
        expected_strategy_release=release,
        exposure_effect=RuntimeCommandExposureEffect.INCREASES,
    )


def test_paper_admission_is_disabled_by_default_without_affecting_other_paths() -> None:
    result = _admit(_paper_adapter(enabled=False), _command())

    assert result.status == "disabled"
    assert result.preview_allowed is False
    assert result.paper_execution_allowed is False
    assert "runtime_command_gate" not in result.to_report_payload()


def test_paper_admission_allows_only_attested_immutable_paper_command() -> None:
    result = _admit(_paper_adapter(), _command())

    assert result.status == "admitted"
    assert result.preview_allowed is True
    assert result.paper_execution_allowed is True
    payload = result.to_report_payload()
    assert payload["runtime_command_gate"]["enforcement"] == "enforce"
    assert payload["runtime_command_gate"]["broker_write_allowed"] is True
    assert payload["paper_risk_admission"]["disposition"] == "allow_new_risk"


def test_paper_admission_fails_closed_for_missing_risk_receipt_before_preview() -> None:
    result = _admit(_paper_adapter(), _command(include_receipt=False))

    assert result.status == "blocked"
    assert result.preview_allowed is False
    assert result.paper_execution_allowed is False
    assert "paper_risk_admission_receipt_missing" in result.reason_codes


def test_paper_admission_fails_closed_for_mutated_command_or_runtime_release_mismatch() -> None:
    mutated = _command()
    object.__setattr__(mutated, "command_id", "cmd-mutated-after-durable-store")
    mutation_result = _admit(_paper_adapter(), mutated)

    release = _release_identity()
    mismatch_result = _paper_adapter().admit(
        command=_command(),
        as_of_session="2026-08-25",
        runtime_release_receipt=build_runtime_loaded_receipt(
            strategy_release=_release_identity(release_id="other-p2-v3.20260824")
        ),
        expected_strategy_release=release,
        exposure_effect=RuntimeCommandExposureEffect.INCREASES,
    )

    for result in (mutation_result, mismatch_result):
        assert result.status == "blocked"
        assert result.preview_allowed is False
        assert result.paper_execution_allowed is False
    assert "command_digest_mismatch" in mutation_result.reason_codes
    assert "release_identity_mismatch" in mismatch_result.reason_codes


def test_paper_admission_never_runs_in_a_live_runtime() -> None:
    live_adapter = SchwabPaperExecutionAdmissionAdapter(
        enabled=True,
        dry_run_only=False,
        runtime_execution_mode="live",
    )

    result = _admit(live_adapter, _command())

    assert result.status == "blocked"
    assert result.paper_runtime is False
    assert result.preview_allowed is False
    assert result.paper_execution_allowed is False
    assert result.reason_codes == ("paper_runtime_required",)
