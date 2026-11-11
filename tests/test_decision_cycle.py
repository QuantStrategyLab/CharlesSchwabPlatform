from candidate_dependency_guard import require_candidate_dependencies

require_candidate_dependencies()

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from application.decision_cycle import cycle_handler, current_cycle, decision_cycle, plugin_load_options
from quant_platform_kit.common.strategy_plugins import load_configured_strategy_plugin_signals, parse_strategy_plugin_mounts


def test_nested_handlers_reuse_clock_and_reset_on_exception():
    clock = datetime(2026, 6, 19, 12, tzinfo=timezone.utc)
    @cycle_handler
    def nested():
        return current_cycle()["decision_time"]
    @cycle_handler
    def outer():
        assert nested() == clock
        raise RuntimeError("synthetic")
    with pytest.raises(RuntimeError), decision_cycle(decision_time=clock):
        outer()
    assert current_cycle() is None
    with decision_cycle(decision_time=datetime(2026, 6, 20, tzinfo=timezone.utc)):
        assert nested() != clock


def test_child_task_does_not_inherit_active_cycle():
    async def run():
        with decision_cycle():
            assert current_cycle() is not None
            async def child():
                return current_cycle()
            assert await asyncio.create_task(child()) is None
    asyncio.run(run())


def test_configured_loader_uses_clock_and_independent_digest(tmp_path):
    path = tmp_path / "signal.json"
    payload = {"strategy": "tqqq_growth_income", "plugin": "crisis_response_shadow", "mode": "shadow",
        "schema_version": "crisis_response_shadow.v1", "as_of": "2026-06-20"}
    raw = json.dumps(payload).encode()
    path.write_bytes(raw)
    mounts = parse_strategy_plugin_mounts([{"strategy": "tqqq_growth_income", "plugin": "crisis_response_shadow", "signal_path": str(path)}])
    clock = datetime(2026, 6, 19, tzinfo=timezone.utc)
    with decision_cycle(decision_time=clock, expected_sha256_by_reference={str(path): hashlib.sha256(raw).hexdigest()}):
        with pytest.raises(ValueError, match="future"):
            load_configured_strategy_plugin_signals(mounts, strategy_profile="tqqq_growth_income", **plugin_load_options(load_configured_strategy_plugin_signals))
    payload["as_of"] = "2026-06-19"
    raw = json.dumps(payload).encode()
    path.write_bytes(raw)
    with decision_cycle(decision_time=clock, expected_sha256_by_reference={str(path): hashlib.sha256(raw).hexdigest()}):
        signal, = load_configured_strategy_plugin_signals(mounts, strategy_profile="tqqq_growth_income", **plugin_load_options(load_configured_strategy_plugin_signals))
        assert signal.digest_status == "verified"
        assert signal.observation_time_status == "date_not_future"
    with decision_cycle(decision_time=clock):
        signal, = load_configured_strategy_plugin_signals(mounts, strategy_profile="tqqq_growth_income", **plugin_load_options(load_configured_strategy_plugin_signals))
        assert signal.digest_status == "unverified"


def test_actual_runtime_adapter_forwards_cycle_to_configured_loader(tmp_path):
    from application.runtime_strategy_adapters import SchwabRuntimeStrategyAdapters
    path = tmp_path / "signal.json"
    raw = json.dumps({"strategy": "tqqq_growth_income", "plugin": "crisis_response_shadow", "mode": "shadow",
        "schema_version": "crisis_response_shadow.v1", "as_of": "2026-06-19"}).encode()
    path.write_bytes(raw)
    adapter = SchwabRuntimeStrategyAdapters(strategy_runtime=None, strategy_profile="tqqq_growth_income",
        strategy_runtime_config={}, available_inputs=(), benchmark_symbol="QQQ", managed_symbols=(),
        signal_text_fn=None, translator=None, broker_adapters=None, build_strategy_evaluation_inputs_fn=None,
        map_strategy_decision_to_plan_fn=None, build_strategy_plugin_report_payload_fn=None,
        load_configured_strategy_plugin_signals_fn=load_configured_strategy_plugin_signals,
        parse_strategy_plugin_mounts_fn=parse_strategy_plugin_mounts)
    mounts = [{"strategy": "tqqq_growth_income", "plugin": "crisis_response_shadow", "signal_path": str(path)}]
    clock = datetime(2026, 6, 19, tzinfo=timezone.utc)
    with decision_cycle(decision_time=clock, expected_sha256_by_reference={str(path): hashlib.sha256(raw).hexdigest()}):
        signals, error = adapter.load_strategy_plugin_signals(mounts)
    assert error is None and signals[0].digest_status == "verified"
    assert signals[0].observation_time_status == "date_not_future"
    with decision_cycle(decision_time=clock, expected_sha256_by_reference={str(path): "0" * 64}):
        signals, error = adapter.load_strategy_plugin_signals(mounts)
    assert signals == () and "sha256 mismatch" in error
    with decision_cycle(decision_time=datetime(2026, 6, 18, tzinfo=timezone.utc)):
        signals, error = adapter.load_strategy_plugin_signals(mounts)
    assert signals == () and "future" in error


def test_actual_strategy_gate_receipt_attaches_to_origin_report():
    from application.daily_strategy_state import evaluate_with_daily_state
    from application.decision_cycle import attach_cycle_decision_receipt, capture_decision
    from quant_platform_kit.common.models import PortfolioSnapshot
    from quant_platform_kit.common.strategy_contracts import StrategyContext
    from us_equity_strategies.entrypoints import compute_tqqq_growth_income_decision
    clock = datetime.now(timezone.utc).replace(microsecond=0)
    report = {"run_id": "synthetic-run", "strategy_profile": "tqqq_growth_income", "decision_at": clock.isoformat()}
    ctx = StrategyContext(as_of=clock.date().isoformat(),
        portfolio=PortfolioSnapshot(as_of=clock, total_equity=100000.0, buying_power=100000.0),
        market_data={"benchmark_history": [{"close": 100 + i * .5} for i in range(260)]},
        runtime_config={"income_layer_enabled": False})
    entrypoint = SimpleNamespace(manifest=SimpleNamespace(profile="tqqq_growth_income"),
        evaluate=compute_tqqq_growth_income_decision)
    with decision_cycle(decision_time=clock) as cycle:
        cycle["report"] = report
        decision = capture_decision(evaluate_with_daily_state(entrypoint, ctx, None))
        from quant_platform_kit.common.execution_receipts import build_execution_receipt
        report["execution_receipt"] = build_execution_receipt(
            platform="schwab", strategy_profile="tqqq_growth_income", strategy_revision="a" * 40,
            execution_mode="live", outcome="no_action", observed_at=clock)
        attach_cycle_decision_receipt(report)
    receipt = report["decision_consumption_receipt"]
    assert receipt["run_id"] == "synthetic-run"
    assert receipt["pre_risk_target_sha256"] and receipt["post_risk_target_sha256"]
    assert receipt["execution_receipt_sha256"] is None
    link = report["decision_execution_link"]
    assert link["run_id"] == "synthetic-run" and link["provenance_status"] == "partial"
    assert link["order_records_sha256"] is None and link["fill_records_sha256"] is None
    assert decision.diagnostics["decision_consumption_receipt_status"] == "recorded"


def test_loaded_plugin_is_bound_to_actual_strategy_receipt(tmp_path):
    from application.daily_strategy_state import evaluate_with_daily_state
    from application.decision_cycle import attach_cycle_decision_receipt, capture_decision
    from quant_platform_kit.common.models import PortfolioSnapshot
    from quant_platform_kit.common.strategy_contracts import StrategyContext
    from quant_platform_kit.common.strategy_plugins import attach_strategy_plugin_metadata
    from us_equity_strategies.entrypoints import compute_tqqq_growth_income_decision
    clock = datetime.now(timezone.utc).replace(microsecond=0)
    path = tmp_path / "market-regime.json"
    raw = json.dumps({"strategy": "tqqq_growth_income", "plugin": "market_regime_control", "mode": "shadow",
        "schema_version": "market_regime_control.v1", "as_of": clock.date().isoformat(),
        "canonical_route": "risk_off", "execution_controls": {"strategy_runtime_metadata_allowed": True}}).encode()
    path.write_bytes(raw)
    mounts = parse_strategy_plugin_mounts([{"strategy": "tqqq_growth_income", "plugin": "market_regime_control", "signal_path": str(path)}])
    report = {"run_id": "synthetic-run", "strategy_profile": "tqqq_growth_income", "decision_at": clock.isoformat()}
    entrypoint = SimpleNamespace(manifest=SimpleNamespace(profile="tqqq_growth_income"), evaluate=compute_tqqq_growth_income_decision)
    with decision_cycle(decision_time=clock, expected_sha256_by_reference={str(path): hashlib.sha256(raw).hexdigest()}) as cycle:
        cycle["report"] = report
        signals = load_configured_strategy_plugin_signals(mounts, strategy_profile="tqqq_growth_income", **plugin_load_options(load_configured_strategy_plugin_signals))
        cycle["plugin_signals"] = signals
        cycle["plugin_observed_at"] = datetime.now(timezone.utc)
        portfolio = attach_strategy_plugin_metadata(PortfolioSnapshot(as_of=clock, total_equity=100000.0, buying_power=100000.0), signals)
        ctx = StrategyContext(as_of=clock.date().isoformat(), portfolio=portfolio,
            market_data={"benchmark_history": [{"close": 100 + i * .5} for i in range(260)]},
            runtime_config={"income_layer_enabled": False, "market_regime_control_enabled": True})
        decision = capture_decision(evaluate_with_daily_state(entrypoint, ctx, None))
        attach_cycle_decision_receipt(report)
    row, = report["decision_consumption_receipt"]["observations"]
    assert row["reference"] == str(path)
    assert row["status"] == "observed" and row["digest_status"] == "verified"
    assert row["availability_at"] is None and row["availability_status"] == "unknown"
    assert row["read_status"] == "after_decision" and row["authority"] == "denied"
    assert decision.diagnostics["decision_consumption_receipt_status"] == "recorded"


def test_receipt_does_not_pair_artifact_with_changed_injected_payload(tmp_path):
    from application.decision_cycle import receipt_capabilities
    from quant_platform_kit.common.models import PortfolioSnapshot
    from quant_platform_kit.common.strategy_contracts import StrategyContext
    from quant_platform_kit.common.strategy_plugins import attach_strategy_plugin_metadata
    clock = datetime.now(timezone.utc).replace(microsecond=0)
    path = tmp_path / "market-regime.json"
    raw = json.dumps({"strategy": "tqqq_growth_income", "plugin": "market_regime_control", "mode": "shadow",
        "schema_version": "market_regime_control.v1", "as_of": clock.date().isoformat(),
        "canonical_route": "risk_off", "execution_controls": {"strategy_runtime_metadata_allowed": True}}).encode()
    path.write_bytes(raw)
    mounts = parse_strategy_plugin_mounts([{"strategy": "tqqq_growth_income", "plugin": "market_regime_control", "signal_path": str(path)}])
    with decision_cycle(decision_time=clock) as cycle:
        cycle["report"] = {"run_id": "synthetic-run", "strategy_profile": "tqqq_growth_income"}
        signal, = load_configured_strategy_plugin_signals(mounts, **plugin_load_options(load_configured_strategy_plugin_signals))
        cycle["plugin_signals"] = (signal,)
        portfolio = attach_strategy_plugin_metadata(PortfolioSnapshot(as_of=clock, total_equity=100000.0), (signal,))
        portfolio.metadata["strategy_plugins"]["market_regime_control"]["canonical_route"] = "no_action"
        ctx = StrategyContext(as_of=clock.date().isoformat(), portfolio=portfolio)
        row, = receipt_capabilities(ctx)["decision_consumption_receipt"]["observations"]
    assert row["status"] == "unavailable" and row["artifact_sha256"] is None


def test_real_order_fact_is_linked_without_inventing_fills():
    from application.daily_strategy_state import evaluate_with_daily_state
    from application.decision_cycle import attach_cycle_decision_receipt, capture_cycle_result_evidence, capture_decision
    from quant_platform_kit.common.execution_receipts import build_execution_receipt
    from quant_platform_kit.common.models import PortfolioSnapshot
    from quant_platform_kit.common.strategy_contracts import StrategyContext
    from us_equity_strategies.entrypoints import compute_tqqq_growth_income_decision
    clock = datetime.now(timezone.utc).replace(microsecond=0)
    report = {"run_id": "synthetic-run", "strategy_profile": "tqqq_growth_income", "decision_at": clock.isoformat()}
    ctx = StrategyContext(as_of=clock.date().isoformat(), portfolio=PortfolioSnapshot(as_of=clock, total_equity=100000., buying_power=100000.),
        market_data={"benchmark_history": [{"close": 100+i*.5} for i in range(260)]}, runtime_config={"income_layer_enabled": False})
    entrypoint = SimpleNamespace(manifest=SimpleNamespace(profile="tqqq_growth_income"), evaluate=compute_tqqq_growth_income_decision)
    order = {"symbol": "TQQQ", "side": "buy", "status": "accepted", "broker_order_id": "123456789"}
    source = SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=(order,))
    with decision_cycle(decision_time=clock) as cycle:
        cycle["report"] = report
        capture_decision(evaluate_with_daily_state(entrypoint, ctx, None))
        capture_cycle_result_evidence(source, dry_run=False)
        report["execution_receipt"] = build_execution_receipt(platform="schwab", strategy_profile="tqqq_growth_income",
            strategy_revision="a"*40, execution_mode="live", outcome="reconciliation_required", observed_at=clock)
        attach_cycle_decision_receipt(report)
    link = report["decision_execution_link"]
    expected = hashlib.sha256(json.dumps([order], sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()).hexdigest()
    assert link["order_records_sha256"] == expected and link["fill_records_sha256"] is None
    assert link["provenance_status"] == "partial"


def test_evidence_extraction_failure_remains_unknown(monkeypatch):
    import application.execution_evidence as evidence
    from application.decision_cycle import capture_cycle_result_evidence
    def unavailable(*args, **kwargs):
        raise RuntimeError("synthetic read failure")
    monkeypatch.setattr(evidence, "extract_execution_evidence", unavailable)
    with decision_cycle() as cycle:
        capture_cycle_result_evidence(object(), dry_run=False)
        assert cycle["execution_order_records"] is None
        assert cycle["execution_fill_records"] is None
