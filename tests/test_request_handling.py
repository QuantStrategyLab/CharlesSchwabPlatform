import importlib
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
PLATFORM_KIT_SRC = ROOT.parent / "QuantPlatformKit" / "src"
if str(PLATFORM_KIT_SRC) not in sys.path:
    sys.path.insert(0, str(PLATFORM_KIT_SRC))
UES_SRC = ROOT.parent / "UsEquityStrategies" / "src"
if str(UES_SRC) not in sys.path:
    sys.path.insert(0, str(UES_SRC))


def install_stub_modules(strategy_plugin_mounts_json=None, notify_lang="en"):
    flask_module = types.ModuleType("flask")

    class Flask:
        def __init__(self, _name):
            self._routes = {}

        def route(self, path, methods=None):
            def decorator(func):
                self._routes[(path, tuple(methods or []))] = func
                return func

            return decorator

        def test_request_context(self, *_args, **_kwargs):
            class _Context:
                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, exc_type, exc, tb):
                    return False

            return _Context()

        def run(self, *args, **kwargs):
            return None

    flask_module.Flask = Flask

    requests_module = types.ModuleType("requests")
    requests_module.post = lambda *args, **kwargs: None
    pandas_module = types.ModuleType("pandas")

    class FakeStrategyPluginAlertStateSettings:
        @classmethod
        def from_env(cls, **kwargs):
            return types.SimpleNamespace(**kwargs)

    def _strategy_plugin_label(signal) -> str:
        return "危机观察通知" if getattr(signal, "plugin", "") == "crisis_response_shadow" else getattr(signal, "plugin", "")

    def _strategy_plugin_status(signal) -> str:
        route = getattr(signal, "canonical_route", None)
        if route == "true_crisis":
            return "真危机"
        if route == "no_action":
            return "未触发"
        return str(route or "")

    def _strategy_plugin_action(signal) -> str:
        action = getattr(signal, "suggested_action", None)
        if action == "defend":
            return "防守"
        if action == "watch_only":
            return "仅观察，不自动交易"
        return str(action or "")

    def _coerce_strategy_plugin_signal(payload):
        data = dict(payload or {})
        return types.SimpleNamespace(
            strategy=data.get("strategy"),
            plugin=data.get("plugin"),
            mode=data.get("mode"),
            configured_mode=data.get("configured_mode"),
            effective_mode=data.get("effective_mode") or data.get("mode"),
            schema_version=data.get("schema_version"),
            as_of=data.get("as_of"),
            canonical_route=data.get("canonical_route"),
            suggested_action=data.get("suggested_action"),
            would_trade_if_enabled=bool(data.get("would_trade_if_enabled")),
            execution_controls=dict(data.get("execution_controls") or {}),
        )

    def _parse_strategy_plugin_mounts(raw_mounts):
        if not raw_mounts:
            return []
        config = json.loads(raw_mounts)
        mounts = list(config.get("strategy_plugins") or [])
        for mount in mounts:
            if "mode" in mount:
                raise ValueError("platform plugin mount config must not set mode")
        return mounts

    def _load_strategy_plugin_signals(raw_mounts):
        try:
            mounts = _parse_strategy_plugin_mounts(raw_mounts)
            signals = []
            for mount in mounts:
                if mount.get("enabled") is False:
                    continue
                signal_path = mount.get("signal_path")
                if not signal_path:
                    continue
                payload = json.loads(Path(signal_path).read_text(encoding="utf-8"))
                signals.append(_coerce_strategy_plugin_signal(payload))
            return tuple(signals), None
        except Exception as exc:
            return (), str(exc)

    def _build_strategy_plugin_summary(signal):
        return {
            "strategy": getattr(signal, "strategy", None),
            "plugin": getattr(signal, "plugin", None),
            "effective_mode": getattr(signal, "effective_mode", None),
            "canonical_route": getattr(signal, "canonical_route", None),
            "suggested_action": getattr(signal, "suggested_action", None),
            "would_trade_if_enabled": bool(getattr(signal, "would_trade_if_enabled", False)),
            "execution_controls": dict(getattr(signal, "execution_controls", {}) or {}),
        }

    def _attach_strategy_plugin_report(report, *, signals, error=None):
        report.setdefault("summary", {})
        report.setdefault("diagnostics", {})
        if signals:
            report["summary"]["strategy_plugins"] = [
                _build_strategy_plugin_summary(signal)
                for signal in signals
            ]
        if error:
            report["diagnostics"]["strategy_plugin_error"] = error

    def _finalize_runtime_report(report, *, status, summary=None, diagnostics=None):
        report["status"] = status
        if summary:
            report.setdefault("summary", {}).update(summary)
        if diagnostics:
            report.setdefault("diagnostics", {}).update(diagnostics)
        return report

    def _append_runtime_report_error(report, **fields):
        report.setdefault("diagnostics", {}).setdefault("errors", []).append(dict(fields))
        return report

    def _build_runtime_report_base(*_args, **kwargs):
        managed_symbols = kwargs.get("managed_symbols") or ("TQQQ", "BOXX", "SPYI", "QQQI")
        signal_delay = kwargs.get("signal_effective_after_trading_days")
        return {
            "status": "pending",
            "strategy_profile": kwargs.get("strategy_profile", "tqqq_growth_income"),
            "summary": {
                "strategy_display_name": kwargs.get("strategy_display_name", "TQQQ Growth Income"),
                "managed_symbols": list(managed_symbols),
                "execution_timing_contract": "next_trading_day" if signal_delay == 1 else "same_day",
                "signal_date": "2026-01-01",
                "effective_date": "2026-01-02",
            },
            "run_source": "cloud_run",
            "dry_run": bool(kwargs.get("dry_run_only")),
            "diagnostics": {},
        }

    class FakeReportingAdapters:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def build_log_context(self):
            run_id_builder = self.kwargs.get("run_id_builder") or (lambda: "test-run")
            return types.SimpleNamespace(run_id=run_id_builder())

        def log_event(self, *args, **kwargs):
            event_logger = self.kwargs.get("event_logger")
            if event_logger is not None:
                return event_logger(*args, **kwargs)
            return None

        def build_report(self, _log_context):
            return _build_runtime_report_base(**self.kwargs)

        def persist_execution_report(self, _report):
            return "/tmp/report.json"

    class FakeNotificationAdapters:
        def publish_cycle_notification(self, *, detailed_text, compact_text):
            return None

    class FakeRuntimeComposer:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def send_tg_message(self, *_args, **_kwargs):
            return None

        def build_notification_adapters(self):
            return FakeNotificationAdapters()

        def build_reporting_adapters(self):
            return FakeReportingAdapters(**self.kwargs)

        def build_client(self):
            return object()

        def load_strategy_plugin_signals(self, mounts_json):
            return _load_strategy_plugin_signals(mounts_json)

        def attach_strategy_plugin_report(self, report, *, signals, error=None):
            _attach_strategy_plugin_report(report, signals=signals, error=error)

        def build_rebalance_runtime(self, *_args, **_kwargs):
            return types.SimpleNamespace()

        def build_rebalance_config(self, **kwargs):
            return types.SimpleNamespace(**kwargs)

    class FakeBrokerAdapters:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def build_price_history(self, _market_data_port, symbol):
            return self.kwargs["fetch_daily_price_history_fn"](None, symbol)

        def build_market_history_loader(self, market_data_port):
            return lambda symbol: self.build_price_history(market_data_port, symbol)

        def fetch_managed_snapshot(self, client):
            return self.kwargs["fetch_account_snapshot_fn"](client, strategy_symbols=self.kwargs.get("managed_symbols", ()))

        def build_market_data_port(self, _client):
            return types.SimpleNamespace()

    class FakeStrategyAdapters:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def translate_strategy_plugin_value(self, category, raw_value):
            if category == "canonical_route":
                return _strategy_plugin_status(types.SimpleNamespace(canonical_route=raw_value))
            if category == "suggested_action":
                return _strategy_plugin_action(types.SimpleNamespace(suggested_action=raw_value))
            return str(raw_value or "")

        def build_strategy_plugin_notification_lines(self, signals):
            return tuple(
                f"插件：{_strategy_plugin_label(signal)} | 启用：是 | 状态：{_strategy_plugin_status(signal)} | 提醒：{_strategy_plugin_action(signal)}"
                for signal in signals
            )

        def build_strategy_plugin_alert_messages(self, signals):
            return self.build_strategy_plugin_notification_lines(signals)

        def fetch_reference_history(self, market_data_port):
            return []

        def build_semiconductor_indicators(self, market_data_source, *, trend_window):
            fetch_history = self.kwargs["broker_adapters"].kwargs["fetch_daily_price_history_fn"]

            def build(symbol):
                closes = [float(row["close"]) for row in fetch_history(market_data_source, symbol)]
                trend_values = closes[-trend_window:]
                ma20_values = closes[-20:]
                return {
                    "price": closes[-1],
                    "ma_trend": sum(trend_values) / len(trend_values),
                    "ma20": sum(ma20_values) / len(ma20_values),
                    "ma20_slope": ma20_values[-1] - ma20_values[0],
                    "rsi14": 100.0 if closes[-1] >= closes[-15] else 0.0,
                    "realized_volatility_10": 0.0,
                    "realized_volatility_20": 0.0,
                    "realized_volatility": 0.0,
                }

            return {"soxl": build("SOXL"), "soxx": build("SOXX")}

        def build_account_state_from_snapshot(self, snapshot):
            managed_symbols = tuple(self.kwargs.get("managed_symbols") or ())
            market_values = {
                str(position.symbol).upper(): float(position.market_value or 0.0)
                for position in getattr(snapshot, "positions", ()) or ()
                if str(position.symbol).upper() in managed_symbols
            }
            available_cash = float((getattr(snapshot, "metadata", {}) or {}).get("cash_available_for_trading", snapshot.buying_power or 0.0))
            return {
                "available_cash": available_cash,
                "market_values": market_values,
                "total_strategy_equity": available_cash + sum(market_values.values()),
            }

        def resolve_rebalance_plan(self, **_kwargs):
            return {}

    runtime_broker_adapters_module = types.ModuleType("application.runtime_broker_adapters")
    runtime_broker_adapters_module.build_runtime_broker_adapters = lambda **kwargs: FakeBrokerAdapters(**kwargs)

    runtime_composer_module = types.ModuleType("application.runtime_composer")
    runtime_composer_module.build_runtime_composer = lambda **kwargs: FakeRuntimeComposer(**kwargs)

    runtime_strategy_adapters_module = types.ModuleType("application.runtime_strategy_adapters")
    runtime_strategy_adapters_module.build_runtime_strategy_adapters = lambda **kwargs: FakeStrategyAdapters(**kwargs)

    rebalance_service_module = types.ModuleType("application.rebalance_service")
    rebalance_service_module.run_strategy_core = lambda *args, **kwargs: None

    signal_snapshot_module = types.ModuleType("application.signal_snapshot")
    signal_snapshot_module.build_signal_snapshot = lambda *args, **kwargs: {}

    decision_mapper_module = types.ModuleType("decision_mapper")
    decision_mapper_module.map_strategy_decision_to_plan = lambda *args, **kwargs: {}

    cloud_run_module = types.ModuleType("entrypoints.cloud_run")
    cloud_run_module.is_market_open_today = lambda: True

    telegram_module = types.ModuleType("notifications.telegram")
    telegram_module.build_signal_text = lambda translator: (lambda key, **kwargs: translator(key, **kwargs))
    telegram_module.build_strategy_display_name = lambda translator: (
        lambda _profile, fallback_name="": fallback_name
    )
    telegram_module.build_translator = lambda _lang: (lambda key, **_kwargs: key)

    quant_platform_kit_module = types.ModuleType("quant_platform_kit")
    quant_platform_kit_module.__path__ = []
    qpk_common_module = types.ModuleType("quant_platform_kit.common")
    qpk_common_module.__path__ = []

    qpk_plugin_alerts_module = types.ModuleType("quant_platform_kit.notifications.strategy_plugin_alerts")
    qpk_plugin_alerts_module.StrategyPluginAlertStateSettings = FakeStrategyPluginAlertStateSettings
    qpk_plugin_alerts_module.build_strategy_plugin_alert_context_label = (
        lambda *, platform_id, strategy_profile, service_name, runtime_target=None: f"{platform_id}:{strategy_profile}:{service_name}"
    )
    qpk_plugin_alerts_module.publish_strategy_plugin_alerts = (
        lambda *args, **kwargs: types.SimpleNamespace(attach_to_report=lambda _report: None)
    )

    qpk_schwab_module = types.ModuleType("quant_platform_kit.schwab")
    qpk_schwab_module.fetch_account_snapshot = lambda *args, **kwargs: None
    qpk_schwab_module.fetch_default_daily_price_history_candles = lambda *args, **kwargs: []
    qpk_schwab_module.fetch_quotes = lambda *args, **kwargs: {}
    qpk_schwab_module.get_client_from_secret = lambda *args, **kwargs: None
    qpk_schwab_module.submit_equity_order = lambda *args, **kwargs: None

    qpk_runtime_reports_module = types.ModuleType("quant_platform_kit.common.runtime_reports")
    qpk_runtime_reports_module.append_runtime_report_error = _append_runtime_report_error
    qpk_runtime_reports_module.build_runtime_report_base = _build_runtime_report_base
    qpk_runtime_reports_module.finalize_runtime_report = _finalize_runtime_report
    qpk_runtime_reports_module.persist_runtime_report = lambda *args, **kwargs: None

    qpk_strategy_plugins_module = types.ModuleType("quant_platform_kit.common.strategy_plugins")
    qpk_strategy_plugins_module.build_strategy_plugin_report_payload = lambda signal, *args, **kwargs: _build_strategy_plugin_summary(signal)
    qpk_strategy_plugins_module.load_configured_strategy_plugin_signals = lambda raw_mounts, *args, **kwargs: _load_strategy_plugin_signals(raw_mounts)[0]
    qpk_strategy_plugins_module.parse_strategy_plugin_mounts = lambda raw_mounts, *args, **kwargs: _parse_strategy_plugin_mounts(raw_mounts)

    qpk_strategy_contracts_module = types.ModuleType("quant_platform_kit.strategy_contracts")
    qpk_strategy_contracts_module.build_strategy_evaluation_inputs = lambda *args, **kwargs: {}

    runtime_config_support_module = types.ModuleType("runtime_config_support")
    runtime_config_support_module.load_platform_runtime_settings = lambda: types.SimpleNamespace(
        strategy_profile="tqqq_growth_income",
        strategy_display_name="TQQQ Growth Income",
        strategy_domain="us_equity",
        notify_lang=notify_lang,
        dry_run_only=False,
        reserved_cash_floor_usd=150.0,
        reserved_cash_ratio=0.03,
        strategy_plugin_mounts_json=strategy_plugin_mounts_json,
        strategy_plugin_alert_email_recipients=(),
        strategy_plugin_alert_email_sender_email=None,
        strategy_plugin_alert_email_sender_password=None,
        strategy_plugin_alert_sms_recipients=(),
        strategy_plugin_alert_sms_account_id=None,
        strategy_plugin_alert_sms_auth_token=None,
        runtime_target=None,
    )

    strategy_runtime_module = types.ModuleType("strategy_runtime")
    strategy_runtime_module.load_strategy_runtime = lambda *_args, **_kwargs: types.SimpleNamespace(
        merged_runtime_config={
            "benchmark_symbol": "QQQ",
            "managed_symbols": ("TQQQ", "BOXX", "SPYI", "QQQI"),
        },
        managed_symbols=("TQQQ", "BOXX", "SPYI", "QQQI"),
        benchmark_symbol="QQQ",
        runtime_adapter=types.SimpleNamespace(
            available_inputs=frozenset({"benchmark_history", "portfolio_snapshot"}),
            runtime_policy=types.SimpleNamespace(signal_effective_after_trading_days=1),
        ),
        evaluate=lambda **_kwargs: None,
    )

    runtime_logging_module = types.ModuleType("runtime_logging")
    runtime_logging_module.build_run_id = lambda *args, **kwargs: "test-run"
    runtime_logging_module.emit_runtime_log = lambda *args, **kwargs: None

    google_module = types.ModuleType("google")
    google_module.__path__ = []

    google_auth_module = types.ModuleType("google.auth")
    google_auth_module.default = lambda *args, **kwargs: (None, None)
    google_auth_transport_module = types.ModuleType("google.auth.transport")
    google_auth_transport_requests_module = types.ModuleType("google.auth.transport.requests")
    google_auth_transport_requests_module.Request = type("Request", (), {})
    google_oauth2_module = types.ModuleType("google.oauth2")
    google_oauth2_id_token_module = types.ModuleType("google.oauth2.id_token")
    google_oauth2_id_token_module.fetch_id_token = lambda *_args, **_kwargs: "id-token"

    google_cloud_module = types.ModuleType("google.cloud")
    google_cloud_module.__path__ = []
    google_secretmanager_module = types.ModuleType("google.cloud.secretmanager_v1")

    schwab_module = types.ModuleType("schwab")
    auth_module = types.ModuleType("schwab.auth")
    client_module = types.ModuleType("schwab.client")
    equities_module = types.ModuleType("schwab.orders.equities")
    equities_module.equity_buy_market = lambda *args, **kwargs: None
    equities_module.equity_sell_market = lambda *args, **kwargs: None
    equities_module.equity_buy_limit = lambda *args, **kwargs: None

    pandas_market_calendars = types.ModuleType("pandas_market_calendars")

    modules = {
        "flask": flask_module,
        "requests": requests_module,
        "pandas": pandas_module,
        "application.runtime_broker_adapters": runtime_broker_adapters_module,
        "application.runtime_composer": runtime_composer_module,
        "application.runtime_strategy_adapters": runtime_strategy_adapters_module,
        "application.rebalance_service": rebalance_service_module,
        "application.signal_snapshot": signal_snapshot_module,
        "decision_mapper": decision_mapper_module,
        "entrypoints.cloud_run": cloud_run_module,
        "notifications.telegram": telegram_module,
        "quant_platform_kit": quant_platform_kit_module,
        "quant_platform_kit.common": qpk_common_module,
        "quant_platform_kit.notifications.strategy_plugin_alerts": qpk_plugin_alerts_module,
        "quant_platform_kit.schwab": qpk_schwab_module,
        "quant_platform_kit.common.runtime_reports": qpk_runtime_reports_module,
        "quant_platform_kit.common.strategy_plugins": qpk_strategy_plugins_module,
        "quant_platform_kit.strategy_contracts": qpk_strategy_contracts_module,
        "runtime_config_support": runtime_config_support_module,
        "strategy_runtime": strategy_runtime_module,
        "runtime_logging": runtime_logging_module,
        "google": google_module,
        "google.auth": google_auth_module,
        "google.auth.transport": google_auth_transport_module,
        "google.auth.transport.requests": google_auth_transport_requests_module,
        "google.oauth2": google_oauth2_module,
        "google.oauth2.id_token": google_oauth2_id_token_module,
        "google.cloud": google_cloud_module,
        "google.cloud.secretmanager_v1": google_secretmanager_module,
        "schwab": schwab_module,
        "schwab.auth": auth_module,
        "schwab.client": client_module,
        "schwab.orders.equities": equities_module,
        "pandas_market_calendars": pandas_market_calendars,
    }
    return patch.dict(sys.modules, modules)


def load_module(*, strategy_plugin_mounts_json=None, notify_lang="en"):
    with install_stub_modules(
        strategy_plugin_mounts_json=strategy_plugin_mounts_json,
        notify_lang=notify_lang,
    ):
        with patch.dict(
            os.environ,
            {
                "SCHWAB_API_KEY": "app-key",
                "SCHWAB_APP_SECRET": "app-secret",
                "GLOBAL_TELEGRAM_CHAT_ID": "shared-chat-id",
            },
            clear=False,
        ):
            sys.modules.pop("main", None)
            module = importlib.import_module("main")
            return importlib.reload(module)


class RequestHandlingTests(unittest.TestCase):
    def test_cloud_run_route_contracts_are_registered(self):
        module = load_module()

        self.assertIs(module.app._routes[("/", ("POST", "GET"))], module.handle_schwab)
        self.assertIs(module.app._routes[("/run", ("POST", "GET"))], module.handle_schwab)
        self.assertIs(
            module.app._routes[("/precheck", ("POST", "GET"))],
            module.handle_schwab_dry_run,
        )
        self.assertIs(
            module.app._routes[("/dry-run", ("POST", "GET"))],
            module.handle_schwab_dry_run,
        )
        self.assertIs(
            module.app._routes[("/probe", ("POST", "GET"))],
            module.handle_schwab_probe,
        )
        self.assertIs(
            module.app._routes[("/monitor-dispatch", ("POST", "GET"))],
            module.handle_monitor_dispatch,
        )
        self.assertIs(module.app._routes[("/health", ("GET",))], module.health)

    def test_handle_monitor_dispatch_post_dispatches_due_targets(self):
        module = load_module()
        observed = {}

        def fake_dispatch(targets):
            observed["targets"] = targets
            return {"ok": True, "dispatches_due": 0}

        with patch.object(module, "request_method", lambda: "POST"), \
            patch.object(module, "load_monitor_targets", lambda: [{"service_name": "charles-schwab-quant-service"}]), \
            patch.object(module, "dispatch_due_monitors", fake_dispatch):
            body, status, headers = module.handle_monitor_dispatch()

        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertIn('"dispatches_due": 0', body)
        self.assertEqual(observed["targets"][0]["service_name"], "charles-schwab-quant-service")

    def test_health_route_returns_ok(self):
        module = load_module()

        with module.app.test_request_context("/health", method="GET"):
            body, status = module.health()

        self.assertEqual(status, 200)
        self.assertEqual(body, "OK")

    def test_build_strategy_runtime_overrides_applies_dca_settings(self):
        module = load_module()
        settings = types.SimpleNamespace(
            income_layer_enabled=None,
            income_layer_start_usd=None,
            income_layer_max_ratio=None,
            dca_mode="smart",
            dca_base_investment_usd=500.0,
        )

        self.assertEqual(
            module.build_strategy_runtime_overrides("nasdaq_sp500_smart_dca", settings),
            {
                "investment_amount_mode": "fixed",
                "smart_multiplier_enabled": True,
                "base_investment_usd": 500.0,
            },
        )

    def test_handle_schwab_returns_market_closed(self):
        module = load_module()
        module.get_client_from_secret = lambda *args, **kwargs: object()
        module.is_market_open_today = lambda: False
        module.run_strategy_core = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not run"))

        with patch.dict(
            os.environ,
            {
                "STRATEGY_PLUGIN_ALERT_TELEGRAM_BOT_TOKEN": "plugin-token",
                "STRATEGY_PLUGIN_ALERT_TELEGRAM_CHAT_IDS": "plugin-chat",
            },
            clear=False,
        ):
            with module.app.test_request_context("/run", method="POST"):
                body, status = module.handle_schwab()

        self.assertEqual(status, 200)
        self.assertEqual(body, "Market Closed")

    def test_handle_schwab_runs_strategy_when_market_open(self):
        module = load_module()
        observed = {"called": False}

        module.get_client_from_secret = lambda *args, **kwargs: object()
        module.is_market_open_today = lambda: True

        def fake_run_strategy_core(client, now_ny, **_kwargs):
            observed["called"] = True
            self.assertIsNotNone(client)
            self.assertIsNone(now_ny)

        module.run_strategy_core = fake_run_strategy_core

        with module.app.test_request_context("/run", method="POST"):
            body, status = module.handle_schwab()

        self.assertEqual(status, 200)
        self.assertEqual(body, "OK")
        self.assertTrue(observed["called"])

    def test_handle_schwab_runtime_error_fallback_sends_telegram(self):
        module = load_module()
        observed = {"payloads": []}

        class FakeResponse:
            status_code = 200

        def fake_post(_url, *, json, timeout):
            observed["payloads"].append((json, timeout))
            return FakeResponse()

        module.TG_TOKEN = "token-1"
        module.TG_CHAT_ID = "chat-1"
        module.requests.post = fake_post
        module._handle_schwab_cycle = lambda: (_ for _ in ()).throw(RuntimeError("boom"))

        with module.app.test_request_context("/run", method="POST"):
            body, status = module.handle_schwab()

        self.assertEqual(status, 500)
        self.assertEqual(body, "Error")
        self.assertEqual(len(observed["payloads"]), 1)
        self.assertEqual(observed["payloads"][0][0]["chat_id"], "chat-1")
        self.assertIn("Schwab strategy run failed", observed["payloads"][0][0]["text"])
        self.assertIn("RuntimeError: boom", observed["payloads"][0][0]["text"])

    def test_handle_schwab_runtime_error_fallback_uses_chinese_copy(self):
        module = load_module(notify_lang="zh")
        observed = {"payloads": []}

        class FakeResponse:
            status_code = 200

        def fake_post(_url, *, json, timeout):
            observed["payloads"].append((json, timeout))
            return FakeResponse()

        module.TG_TOKEN = "token-1"
        module.TG_CHAT_ID = "chat-1"
        module.requests.post = fake_post
        module._handle_schwab_cycle = lambda: (_ for _ in ()).throw(RuntimeError("boom"))

        with module.app.test_request_context("/run", method="POST"):
            body, status = module.handle_schwab()

        self.assertEqual(status, 500)
        self.assertEqual(body, "Error")
        text = observed["payloads"][0][0]["text"]
        self.assertIn("Schwab 策略运行失败", text)
        self.assertIn("服务:", text)
        self.assertIn("错误: RuntimeError: boom", text)

    def test_handle_schwab_sends_escalated_strategy_plugin_alert(self):
        module = load_module()
        signal = types.SimpleNamespace(
            plugin="crisis_response_shadow",
            effective_mode="shadow",
            canonical_route="true_crisis",
            suggested_action="defend",
            would_trade_if_enabled=True,
            as_of="2026-05-24",
        )
        observed = {"alerts": []}

        module.get_client_from_secret = lambda *args, **kwargs: object()
        module.is_market_open_today = lambda: True
        module.load_strategy_plugin_signals = lambda: ((signal,), None)
        module.attach_strategy_plugin_report = lambda *args, **kwargs: None

        def fake_dispatch(signals, **kwargs):
            observed["alerts"].append((tuple(signals), kwargs))
            return types.SimpleNamespace(attach_to_report=lambda _report: None)

        module.dispatch_strategy_plugin_alerts = fake_dispatch
        module.run_strategy_core = lambda *_args, **_kwargs: None

        with module.app.test_request_context("/run", method="POST"):
            body, status = module.handle_schwab()

        self.assertEqual(status, 200)
        self.assertEqual(body, "OK")
        self.assertEqual(len(observed["alerts"]), 1)
        self.assertEqual(observed["alerts"][0][0], (signal,))
        self.assertIn("schwab", observed["alerts"][0][1]["context_label"])
        self.assertIs(observed["alerts"][0][1]["notification_settings"], module.RUNTIME_SETTINGS)
        self.assertIsNotNone(observed["alerts"][0][1]["state_settings"])

    def test_handle_schwab_dry_run_uses_dry_run_override(self):
        module = load_module()
        observed = {"called": False, "dry_run_only_override": None}

        module.get_client_from_secret = lambda *args, **kwargs: object()
        module.is_market_open_today = lambda: True
        module.load_strategy_plugin_signals = lambda: ((), None)
        module.attach_strategy_plugin_report = lambda *args, **kwargs: None
        module.build_execution_report = lambda log_context, **_kwargs: {"status": "pending"}
        module.persist_execution_report = lambda report, **_kwargs: "/tmp/report.json"
        module.emit_runtime_log = lambda *args, **kwargs: None

        def fake_run_strategy_core(client, now_ny, **kwargs):
            observed["called"] = True
            observed["dry_run_only_override"] = kwargs.get("dry_run_only_override")
            self.assertIsNotNone(client)
            self.assertIsNone(now_ny)

        module.run_strategy_core = fake_run_strategy_core

        with module.app.test_request_context("/dry-run", method="POST"):
            body, status = module.handle_schwab_dry_run()

        self.assertEqual(status, 200)
        self.assertEqual(body, "Dry Run OK")
        self.assertTrue(observed["called"])
        self.assertTrue(observed["dry_run_only_override"])

    def test_handle_schwab_dry_run_stays_silent_when_market_closed(self):
        module = load_module()
        observed = {"notifications": []}

        module.get_client_from_secret = lambda *args, **kwargs: object()
        module.is_market_open_today = lambda: False
        module.load_strategy_plugin_signals = lambda: ((), None)
        module.attach_strategy_plugin_report = lambda *args, **kwargs: None
        module.build_execution_report = lambda log_context, **_kwargs: {"status": "pending"}
        module.persist_execution_report = lambda report, **_kwargs: "/tmp/report.json"
        module.emit_runtime_log = lambda *args, **kwargs: None

        class FakeNotifications:
            def publish_cycle_notification(self, *, detailed_text, compact_text):
                observed["notifications"].append((detailed_text, compact_text))

        class FakeComposer:
            def build_reporting_adapters(self):
                return types.SimpleNamespace(
                    build_log_context=lambda: types.SimpleNamespace(run_id="run-001"),
                    log_event=lambda *args, **kwargs: None,
                    persist_execution_report=lambda report: "/tmp/report.json",
                )

            def build_notification_adapters(self):
                return FakeNotifications()

            def build_client(self):
                return object()

        module.build_composer = lambda *, dry_run_only_override=None: FakeComposer()

        with module.app.test_request_context("/dry-run", method="POST"):
            body, status = module.handle_schwab_dry_run()

        self.assertEqual(status, 200)
        self.assertEqual(body, "Market Closed")
        self.assertEqual(len(observed["notifications"]), 0)

    def test_handle_schwab_probe_checks_account_snapshot_without_notifications(self):
        module = load_module()
        observed = {"client_called": False, "snapshot_called": False}

        module.load_strategy_plugin_signals = lambda: (_ for _ in ()).throw(
            AssertionError("health probe should not load strategy plugins")
        )
        module.attach_strategy_plugin_report = lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("health probe should not attach strategy plugin reports")
        )
        module.build_execution_report = lambda log_context, **_kwargs: {"status": "pending"}
        module.persist_execution_report = lambda report, **_kwargs: observed.setdefault("report", dict(report)) or "/tmp/report.json"
        module.emit_runtime_log = lambda *args, **kwargs: None
        module.fetch_account_snapshot = lambda client, *, strategy_symbols=(): observed.__setitem__("snapshot_called", True) or types.SimpleNamespace(
            buying_power=123.0,
            total_equity=456.0,
            positions=(),
        )

        class FakeComposer:
            def build_reporting_adapters(self):
                return types.SimpleNamespace(
                    build_log_context=lambda: types.SimpleNamespace(run_id="run-001"),
                    log_event=lambda *args, **kwargs: None,
                    persist_execution_report=lambda report: "/tmp/report.json",
                )

            def build_client(self):
                observed["client_called"] = True
                return object()

            def build_notification_adapters(self):
                raise AssertionError("probe success should stay silent")

        module.build_composer = lambda *, dry_run_only_override=None: FakeComposer()

        with module.app.test_request_context("/probe", method="POST"):
            body, status = module.handle_schwab_probe()

        self.assertEqual(status, 200)
        self.assertEqual(body, "Probe OK")
        self.assertTrue(observed["client_called"])
        self.assertTrue(observed["snapshot_called"])
        self.assertEqual(observed["report"]["status"], "ok")

    def test_handle_schwab_emits_structured_runtime_events(self):
        module = load_module()
        observed = []

        module.build_run_id = lambda: "run-001"
        module.emit_runtime_log = lambda context, event, **fields: observed.append((context.run_id, event, fields))
        module.get_client_from_secret = lambda *args, **kwargs: object()
        module.is_market_open_today = lambda: True
        module.run_strategy_core = lambda *_args, **_kwargs: None

        with module.app.test_request_context("/", method="POST"):
            body, status = module.handle_schwab()

        self.assertEqual(status, 200)
        self.assertEqual(body, "OK")
        self.assertEqual(
            [event for _run_id, event, _fields in observed],
            ["strategy_cycle_received", "strategy_cycle_started", "strategy_cycle_completed"],
        )
        self.assertTrue(all(run_id == "run-001" for run_id, _event, _fields in observed))

    def test_handle_schwab_persists_machine_readable_report(self):
        module = load_module()
        observed = {}

        module.build_run_id = lambda: "run-001"
        module.get_client_from_secret = lambda *args, **kwargs: object()
        module.is_market_open_today = lambda: True
        module.run_strategy_core = lambda *_args, **_kwargs: None
        module.persist_execution_report = lambda report: observed.setdefault("report", report) or "/tmp/report.json"

        with module.app.test_request_context("/", method="POST"):
            body, status = module.handle_schwab()

        self.assertEqual(status, 200)
        self.assertEqual(body, "OK")
        self.assertEqual(observed["report"]["status"], "ok")
        self.assertEqual(observed["report"]["strategy_profile"], "tqqq_growth_income")
        self.assertEqual(
            observed["report"]["summary"]["strategy_display_name"],
            "TQQQ Growth Income",
        )
        self.assertEqual(observed["report"]["run_source"], "cloud_run")
        self.assertFalse(observed["report"]["dry_run"])
        self.assertEqual(
            observed["report"]["summary"]["managed_symbols"],
            ["TQQQ", "BOXX", "SPYI", "QQQI"],
        )
        self.assertEqual(observed["report"]["summary"]["execution_timing_contract"], "next_trading_day")
        self.assertTrue(observed["report"]["summary"]["signal_date"])
        self.assertTrue(observed["report"]["summary"]["effective_date"])

    def test_handle_schwab_attaches_strategy_plugin_report(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            signal_path = Path(temp_dir) / "latest_signal.json"
            signal_path.write_text(
                json.dumps(
                    {
                        "strategy": "tqqq_growth_income",
                        "plugin": "crisis_response_shadow",
                        "mode": "shadow",
                        "configured_mode": "shadow",
                        "effective_mode": "shadow",
                        "schema_version": "crisis_response_shadow.v1",
                        "as_of": "2026-04-17",
                        "canonical_route": "no_action",
                        "suggested_action": "monitor",
                        "would_trade_if_enabled": False,
                        "execution_controls": {
                            "broker_order_allowed": False,
                            "live_allocation_mutation_allowed": False,
                        },
                    }
                ),
                encoding="utf-8",
            )
            mount_config = json.dumps(
                {
                    "strategy_plugins": [
                        {
                            "strategy": "tqqq_growth_income",
                            "plugin": "crisis_response_shadow",
                            "signal_path": str(signal_path),
                            "enabled": True,
                        }
                    ]
                }
            )
            module = load_module(strategy_plugin_mounts_json=mount_config)
            observed = {}

            module.get_client_from_secret = lambda *args, **kwargs: object()
            module.is_market_open_today = lambda: True
            module.persist_execution_report = (
                lambda report: observed.setdefault("report", report) or "/tmp/report.json"
            )

            def fake_run_strategy_core(client, now_ny, *, strategy_plugin_signals=(), **_kwargs):
                observed["signals"] = strategy_plugin_signals
                self.assertIsNotNone(client)
                self.assertIsNone(now_ny)

            module.run_strategy_core = fake_run_strategy_core

            with module.app.test_request_context("/", method="POST"):
                body, status = module.handle_schwab()

        self.assertEqual(status, 200)
        self.assertEqual(body, "OK")
        self.assertEqual(len(observed["signals"]), 1)
        plugin_summary = observed["report"]["summary"]["strategy_plugins"][0]
        self.assertEqual(plugin_summary["strategy"], "tqqq_growth_income")
        self.assertEqual(plugin_summary["plugin"], "crisis_response_shadow")
        self.assertEqual(plugin_summary["effective_mode"], "shadow")
        self.assertEqual(plugin_summary["canonical_route"], "no_action")
        self.assertEqual(plugin_summary["suggested_action"], "monitor")

    def test_handle_schwab_rehearses_triggered_shadow_plugin_report(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            signal_path = Path(temp_dir) / "latest_signal.json"
            signal_path.write_text(
                json.dumps(
                    {
                        "strategy": "tqqq_growth_income",
                        "plugin": "crisis_response_shadow",
                        "mode": "shadow",
                        "configured_mode": "shadow",
                        "effective_mode": "shadow",
                        "schema_version": "crisis_response_shadow.v1",
                        "as_of": "2008-03-10",
                        "canonical_route": "true_crisis",
                        "suggested_action": "defend",
                        "would_trade_if_enabled": True,
                        "execution_controls": {
                            "broker_order_allowed": False,
                            "live_allocation_mutation_allowed": False,
                            "repository_broker_write_allowed": False,
                            "repository_allocation_mutation_allowed": False,
                        },
                    }
                ),
                encoding="utf-8",
            )
            mount_config = json.dumps(
                {
                    "strategy_plugins": [
                        {
                            "strategy": "tqqq_growth_income",
                            "plugin": "crisis_response_shadow",
                            "signal_path": str(signal_path),
                            "enabled": True,
                            "expected_mode": "shadow",
                        }
                    ]
                }
            )
            module = load_module(strategy_plugin_mounts_json=mount_config)
            observed = {}

            module.get_client_from_secret = lambda *args, **kwargs: object()
            module.is_market_open_today = lambda: True
            module.persist_execution_report = (
                lambda report: observed.setdefault("report", report) or "/tmp/report.json"
            )

            def fake_run_strategy_core(client, now_ny, *, strategy_plugin_signals=(), **_kwargs):
                observed["signals"] = strategy_plugin_signals
                self.assertEqual(len(strategy_plugin_signals), 1)
                signal = strategy_plugin_signals[0]
                self.assertTrue(signal.would_trade_if_enabled)
                self.assertEqual(signal.canonical_route, "true_crisis")
                self.assertEqual(signal.suggested_action, "defend")
                self.assertFalse(signal.execution_controls["broker_order_allowed"])
                self.assertFalse(signal.execution_controls["live_allocation_mutation_allowed"])

            module.run_strategy_core = fake_run_strategy_core

            with module.app.test_request_context("/", method="POST"):
                body, status = module.handle_schwab()

        self.assertEqual(status, 200)
        self.assertEqual(body, "OK")
        plugin_summary = observed["report"]["summary"]["strategy_plugins"][0]
        self.assertEqual(plugin_summary["canonical_route"], "true_crisis")
        self.assertEqual(plugin_summary["suggested_action"], "defend")
        self.assertTrue(plugin_summary["would_trade_if_enabled"])
        self.assertFalse(plugin_summary["execution_controls"]["broker_order_allowed"])
        self.assertFalse(plugin_summary["execution_controls"]["live_allocation_mutation_allowed"])

    def test_strategy_plugin_notification_line_uses_i18n(self):
        module = load_module(notify_lang="zh")
        signal = types.SimpleNamespace(
            plugin="crisis_response_shadow",
            effective_mode="shadow",
            canonical_route="no_action",
            suggested_action="watch_only",
        )

        lines = module.build_strategy_plugin_notification_lines((signal,))

        self.assertEqual(len(lines), 1)
        self.assertIn("插件：危机观察通知", lines[0])
        self.assertIn("状态：未触发", lines[0])
        self.assertIn("提醒：仅观察，不自动交易", lines[0])

    def test_strategy_plugin_notification_line_renders_triggered_shadow_signal(self):
        module = load_module(notify_lang="zh")
        signal = types.SimpleNamespace(
            plugin="crisis_response_shadow",
            effective_mode="shadow",
            canonical_route="true_crisis",
            suggested_action="defend",
        )

        lines = module.build_strategy_plugin_notification_lines((signal,))

        self.assertEqual(len(lines), 1)
        self.assertIn("状态：真危机", lines[0])
        self.assertIn("提醒：防守", lines[0])

    def test_handle_schwab_reports_plugin_config_error_without_blocking_strategy(self):
        mount_config = json.dumps(
            {
                "strategy_plugins": [
                    {
                        "strategy": "tqqq_growth_income",
                        "plugin": "crisis_response_shadow",
                        "mode": "shadow",
                        "signal_path": "/tmp/missing_signal.json",
                    }
                ]
            }
        )
        module = load_module(strategy_plugin_mounts_json=mount_config)
        observed = {"called": False}

        module.get_client_from_secret = lambda *args, **kwargs: object()
        module.is_market_open_today = lambda: True
        module.persist_execution_report = lambda report: observed.setdefault("report", report) or "/tmp/report.json"

        def fake_run_strategy_core(client, now_ny, *, strategy_plugin_signals=(), **_kwargs):
            observed["called"] = True
            self.assertEqual(strategy_plugin_signals, ())

        module.run_strategy_core = fake_run_strategy_core

        with module.app.test_request_context("/", method="POST"):
            body, status = module.handle_schwab()

        self.assertEqual(status, 200)
        self.assertEqual(body, "OK")
        self.assertTrue(observed["called"])
        self.assertIn(
            "platform plugin mount config must not set mode",
            observed["report"]["diagnostics"]["strategy_plugin_error"],
        )

    def test_build_account_state_from_snapshot_uses_strategy_symbols(self):
        module = load_module()
        snapshot = types.SimpleNamespace(
            total_equity=50000.0,
            buying_power=12000.0,
            positions=(
                types.SimpleNamespace(symbol="TQQQ", quantity=5, market_value=1000.0),
                types.SimpleNamespace(symbol="BOXX", quantity=10, market_value=5000.0),
                types.SimpleNamespace(symbol="QQQ", quantity=99, market_value=9999.0),
            ),
            metadata={"cash_available_for_trading": 8000.0},
        )

        account_state = module.build_account_state_from_snapshot(snapshot)

        self.assertEqual(account_state["available_cash"], 8000.0)
        self.assertEqual(account_state["market_values"]["TQQQ"], 1000.0)
        self.assertEqual(account_state["market_values"]["BOXX"], 5000.0)
        self.assertNotIn("QQQ", account_state["market_values"])
        self.assertEqual(account_state["total_strategy_equity"], 14000.0)

    def test_build_semiconductor_indicators_uses_soxl_and_soxx_histories(self):
        module = load_module()

        def fake_history(_client, symbol):
            if symbol == "SOXL":
                return [{"close": 100.0 + idx} for idx in range(160)]
            if symbol == "SOXX":
                return [{"close": 210.0 + idx} for idx in range(160)]
            raise AssertionError(f"unexpected symbol {symbol}")

        module.fetch_default_daily_price_history_candles = fake_history

        indicators = module.build_semiconductor_indicators(object(), trend_window=150)

        self.assertEqual(indicators["soxl"]["price"], 259.0)
        self.assertAlmostEqual(indicators["soxl"]["ma_trend"], sum(100.0 + idx for idx in range(10, 160)) / 150)
        self.assertEqual(indicators["soxx"]["price"], 369.0)
        self.assertAlmostEqual(indicators["soxx"]["ma_trend"], sum(210.0 + idx for idx in range(10, 160)) / 150)
        self.assertAlmostEqual(indicators["soxx"]["ma20"], sum(210.0 + idx for idx in range(140, 160)) / 20)
        self.assertGreater(indicators["soxx"]["ma20_slope"], 0.0)
        self.assertEqual(indicators["soxx"]["rsi14"], 100.0)
        self.assertIn("realized_volatility_10", indicators["soxx"])
        self.assertIn("realized_volatility_20", indicators["soxx"])
        self.assertEqual(
            indicators["soxx"]["realized_volatility"],
            indicators["soxx"]["realized_volatility_20"],
        )


if __name__ == "__main__":
    unittest.main()
