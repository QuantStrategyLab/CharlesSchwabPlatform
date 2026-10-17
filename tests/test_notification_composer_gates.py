"""Actual composer -> notification runtime/archive -> pinned QRS, with fake IO."""
import copy
import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

from application.runtime_composer import SchwabRuntimeComposer
from application.runtime_report_summary import summarize_execution_cycle_result
from application.runtime_reporting_adapters import build_runtime_reporting_adapters
from notifications import unified
from quant_platform_kit.common.execution_receipts import build_execution_receipt
from quant_platform_kit.common.runtime_reports import append_runtime_report_error, build_runtime_report_base, finalize_runtime_report
from quant_platform_kit.common.runtime_target import resolve_runtime_target_from_env
from test_notification_approved_gates import CONSECUTIVE, DAILY, execution
from test_notification_qrs_roundtrip import SOURCE_AT, producer as make_producer, qrs_roundtrip


@pytest.fixture
def producer(monkeypatch):
    return make_producer.__wrapped__(monkeypatch)


def composer_run(producer, reasons, *, change=None):
    """Use the production composer and serializer; only IO adapters are fake."""
    def forbidden(*args, **kwargs):
        pytest.fail("No broker, persistence, or live sender in synthetic replay")

    target = json.loads(producer.env["RUNTIME_TARGET_JSON"])
    target["execution_mode"] = "live"
    producer.env["RUNTIME_TARGET_JSON"] = json.dumps(target)
    runtime = resolve_runtime_target_from_env(env=producer.env, expected_platform_id="schwab")
    composer = SchwabRuntimeComposer(
        project_id=None, service_name="synthetic-service", secret_id="", app_key=None, app_secret=None,
        token_path="", strategy_profile="strategy-a", strategy_domain="us_equity",
        strategy_display_name="Synthetic", strategy_display_name_localized="Synthetic", notify_lang="en",
        tg_token=None, tg_chat_id=None, managed_symbols=(), benchmark_symbol="",
        signal_effective_after_trading_days=1, dry_run_only=False, limit_buy_premium=1.0,
        sell_settle_delay_sec=0, post_sell_refresh_attempts=0, post_sell_refresh_interval_sec=0,
        safe_haven_cash_substitute_threshold_usd=0, broker_adapters=None, strategy_adapters=None,
        client_builder=forbidden, run_id_builder=lambda: "composer-business-run", event_logger=forbidden,
        report_builder=build_runtime_report_base, report_persister=forbidden,
        env_reader=lambda name, default="": default, printer=forbidden, runtime_target=runtime,
        reporting_builder=lambda **kwargs: build_runtime_reporting_adapters(clock=lambda: SOURCE_AT, **kwargs),
    )
    _, source = composer.build_reporting_adapters().start_run()
    assert source["platform"] == "charles_schwab"
    source["runtime_release_receipt"] = {"attestation_state": "self_attested", "strategy_release": {"strategy_revision": "a" * 40}}
    data = execution(reasons)
    if CONSECUTIVE in reasons:
        data["notification_block_reason"] = copy.deepcopy(CONSECUTIVE)
    if change == "unknown_block":
        data["no_op_reason"] = "unknown_risk_gate"
    if change == "order_error":
        data["order_errors"] = ["synthetic order error"]
    result = SimpleNamespace(execution=data, submitted_orders=(), trade_logs=())
    with unified.notification_context(source):
        if change is None:
            assert unified.stage_approved_gate_observation(profile="strategy-a", reasons=reasons)
        unified.publish_cycle(result, config=SimpleNamespace(dry_run_only=False, strategy_profile="strategy-a"),
            legacy=lambda: producer.sends.append("cycle") or {"message_id": "synthetic-cycle-receipt"})
    finalize_runtime_report(source, status="ok", finished_at=SOURCE_AT + timedelta(minutes=1),
        summary=summarize_execution_cycle_result(result, dry_run=False))
    if change == "order_error":
        append_runtime_report_error(source, stage="order", message="synthetic order error")
    source["execution_receipt"] = build_execution_receipt(platform="schwab", strategy_profile="strategy-a",
        strategy_revision="a" * 40, execution_mode="live", outcome="risk_blocked", observed_at=source["finished_at"])
    if change == "missing_receipt":
        source.pop("execution_receipt")
    producer.reports.append(source)
    return source


@pytest.mark.parametrize("reasons", [[DAILY], [CONSECUTIVE], [CONSECUTIVE, DAILY]], ids=["daily", "consecutive", "both"])
@pytest.mark.parametrize("locale", ["zh", "en"])
def test_actual_composer_approved_gates_reach_final_qrs(monkeypatch, tmp_path, producer, reasons, locale):
    source = composer_run(producer, reasons)
    before = copy.deepcopy(source)
    producer.reports.append(copy.deepcopy(source))
    assert producer.export(producer.env)["uploaded"] is True
    output = producer.uploads[-1]
    assert len(output["runs"]) == 1
    assert output["runs"][0]["approved_gate_reasons"] == reasons
    runs, summary, text, receipt, _ = qrs_roundtrip(monkeypatch, tmp_path, [output, copy.deepcopy(output)], locale=locale)
    assert source == before and source["platform"] == "charles_schwab"
    assert not producer.sends and summary["immediate_events"] == 0
    assert runs[0]["approved_gate_reasons"] == reasons
    assert receipt["runs"][0]["approved_gate_reasons"] == tuple(reasons)
    assert receipt["run_count"] == 1 and runs[0]["order_count"] == 0
    assert runs[0]["fill_count"] == source["summary"]["notification_digest"].get("fill_count")
    for reason in reasons:
        if reason == DAILY:
            assert ("日亏损上限" if locale == "zh" else "Daily-loss gate") in text
            assert ("80≥60" if locale == "zh" else "80>=60") in text
            assert "2000" in text
        else:
            assert ("连亏熔断" if locale == "zh" else "Consecutive-loss gate") in text
            assert "6>5" in text
    assert ("本轮未下单" if locale == "zh" else "No order this run") in text


@pytest.mark.parametrize("change", ["unknown_block", "missing_receipt", "order_error"])
def test_actual_composer_urgent_cases_stay_immediate_and_deduplicate(monkeypatch, tmp_path, producer, change):
    source = composer_run(producer, [DAILY], change=change)
    before = copy.deepcopy(source)
    if change == "missing_receipt":
        # Actual monitor notification entry point, with a unique fake sender
        # receipt; repeated observations must not blindly resend.
        for _ in range(2):
            unified.publish_receipt(lambda: producer.sends.append("missing receipt") or {"message_id": "synthetic-monitor-receipt"},
                kind="real_error", reason="receipt_missing", observed_at=SOURCE_AT)
    producer.export(producer.env)
    output = producer.uploads[-1]
    assert "approved_gate_reasons" not in output["runs"][0]
    runs, summary, _, receipt, _ = qrs_roundtrip(monkeypatch, tmp_path, [output, copy.deepcopy(output)])
    assert source == before and len(runs) == receipt["run_count"] == 1
    assert len(producer.sends) == 1
    assert summary["immediate_events"] == 1
    assert not receipt["runs"][0]["approved_gate_reasons"]


@pytest.mark.parametrize("nested_alias", [False, True])
def test_notification_alias_copy_preserves_report_and_all_other_bindings(producer, nested_alias):
    source = composer_run(producer, [DAILY])
    if nested_alias:
        source["runtime_target"]["platform_id"] = "charles_schwab"
    before = copy.deepcopy(source)
    producer.export(producer.env)
    assert source == before
    assert len(producer.uploads[-1]["runs"]) == 1
    for key, wrong in (("strategy_profile", "other"), ("service_name", "other"), ("account_selector", ["other"]),
                       ("account_scope", "other"), ("deployment_selector", "other"), ("dry_run_only", True), ("platform_id", "other")):
        changed = copy.deepcopy(source)
        changed["runtime_target"][key] = wrong
        # Direct validator entry point must reject every non-alias mismatch;
        # archive filtering may skip mismatched protected targets first.
        with pytest.raises(ValueError, match="candidate_source_identity_mismatch"):
            unified._project_runs([changed], expected_target=json.loads(producer.env["RUNTIME_TARGET_JSON"]),
                opaque_account_uid="opaque-a", target_id="target-a", now=SOURCE_AT)
    assert source == before
