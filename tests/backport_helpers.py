"""Synthetic IO helpers adapted to the frozen production APIs."""
import copy
import datetime as dt
import json
from types import SimpleNamespace
import pandas as pd
from application.runtime_strategy_adapters import SchwabRuntimeStrategyAdapters
from notifications import unified
from scripts import execution_report_heartbeat as heartbeat
from test_unified_notifications import MemoryStore
from quant_platform_kit.common.execution_receipts import build_execution_receipt
NOW = dt.datetime(2026, 10, 5, 13, tzinfo=dt.timezone.utc)
TARGET = SimpleNamespace(service='synthetic-service', profile='synthetic_profile', scope='synthetic')
def legacy_report():
    return {'service_name': TARGET.service, 'strategy_profile': TARGET.profile, 'account_scope': TARGET.scope,
            'started_at': '2026-10-05T12:00:00Z', 'finished_at': '2026-10-05T12:01:00Z',
            'status': 'ok', 'dry_run': False, 'errors': [],
            'summary': {'execution_status': 'no_action', 'orders_submitted_count': 0},
            'diagnostics': {}, 'market': 'US', 'market_calendar': 'NYSE', 'market_timezone': 'America/New_York'}


def mock_heartbeat(monkeypatch, payload):
    monkeypatch.setattr(heartbeat, '_runtime_target_enabled', lambda: True)
    monkeypatch.setattr(heartbeat, 'runtime_target_configuration_present', lambda env: False)
    monkeypatch.setattr(heartbeat, 'load_runtime_targets', lambda env: [])
    monkeypatch.setattr(heartbeat, '_hydrate_runtime_target_schedules', lambda targets, **kw: targets)
    monkeypatch.setattr(heartbeat, 'filter_due_targets', lambda *args, **kw: ([], False))
    monkeypatch.setattr(heartbeat, '_heartbeat_skip_reason_for_schedule', lambda *args: None)
    monkeypatch.setattr(heartbeat, '_load_required_services', lambda: [])
    monkeypatch.setattr(heartbeat, '_report_globs', lambda *args: ['gs://mock/reports'])
    monkeypatch.setattr(heartbeat, '_list_gcs_objects', lambda *args, **kw: [{'url': 'gs://mock/report.json', 'metadata': {'updated': NOW.isoformat()}}])
    monkeypatch.setattr(heartbeat, '_cat_gcs_json', lambda *args, **kw: payload)
    sent = []
    monkeypatch.setattr(heartbeat, '_send_telegram', lambda msg, **kw: sent.append(msg) or True)
    return sent


def decision(observed=6, limit=5):
    return SimpleNamespace(risk_flags=("rejected:circuit_breaker",), diagnostics={
        "risk_gate": "REJECT", "consecutive_losses": observed,
        "reason": f"连续亏损 {observed} 笔 > {limit} 熔断"})


def _adapters(*, profile: str = "nasdaq_sp500_smart_dca", signal_symbols=("QQQ", "SPY"), history_by_symbol=None):
    history_by_symbol = history_by_symbol or {
        "QQQ": pd.Series([100.0, 101.0], index=pd.to_datetime(["2026-01-01", "2026-01-02"])),
        "SPY": pd.Series([200.0, 201.0], index=pd.to_datetime(["2026-01-01", "2026-01-02"])),
    }

    class BrokerAdapters:
        def build_market_history_loader(self, _market_data_port):
            def load(_client, symbol, *_args, **_kwargs):
                key = str(symbol).strip().upper()
                if key not in history_by_symbol:
                    return pd.Series(dtype=float)
                return history_by_symbol[key]

            return load

        def build_price_history(self, _market_data_port, symbol: str):
            return [{"close": 1.0, "high": 1.0, "low": 1.0}]

    return SchwabRuntimeStrategyAdapters(
        strategy_runtime=SimpleNamespace(),
        strategy_profile=profile,
        strategy_runtime_config={"signal_symbols": signal_symbols},
        available_inputs=frozenset({"market_history", "portfolio_snapshot"}),
        benchmark_symbol="QQQ",
        managed_symbols=("QQQM", "SPLG"),
        signal_text_fn=str,
        translator=lambda key, **_kwargs: key,
        broker_adapters=BrokerAdapters(),
        build_strategy_evaluation_inputs_fn=lambda **_kwargs: {},
        map_strategy_decision_to_plan_fn=lambda *_args, **_kwargs: {},
        build_strategy_plugin_report_payload_fn=lambda *_args, **_kwargs: {},
        load_configured_strategy_plugin_signals_fn=lambda *_args, **_kwargs: (),
        parse_strategy_plugin_mounts_fn=lambda raw: raw,
    )


def configure(monkeypatch):
    store = MemoryStore()
    monkeypatch.setenv(unified.FLAG, "true")
    monkeypatch.setenv("NOTIFY_LANG", "zh")
    monkeypatch.setenv("DIGEST_OPAQUE_ACCOUNT_UID", "opaque-a")
    monkeypatch.setenv("DIGEST_TARGET_ID", "target-a")
    monkeypatch.setenv("GITHUB_RUN_ID", "run-synthetic")
    monkeypatch.setenv("RUNTIME_TARGET_JSON", json.dumps({"strategy_profile": "synthetic_profile"}))
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda *a, **kw: store)
    return store

def report(outcome="no_action", **updates):
    value = legacy_report()
    value.update(schema_version="runtime_report.v1", platform="charles_schwab", run_id="synthetic-run",
        runtime_target={"platform_id":"schwab", "strategy_profile": TARGET.profile,
            "account_scope": TARGET.scope, "account_selector":["synthetic"], "execution_mode":"live"},
        runtime_release_receipt={"attestation_state":"self_attested", "strategy_release":{"strategy_revision":"a"*40}})
    value.update(updates)
    value["execution_receipt"] = build_execution_receipt(platform="schwab", strategy_profile=value["strategy_profile"],
        strategy_revision="a"*40, execution_mode="live", outcome=outcome, observed_at=value["finished_at"])
    return value

