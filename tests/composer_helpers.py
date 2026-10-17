"""Build the real old-base report; external adapters never perform IO."""
import json
from application.runtime_composer import SchwabRuntimeComposer
from application.runtime_reporting_adapters import build_runtime_reporting_adapters
from quant_platform_kit.common.runtime_reports import build_runtime_report_base
from quant_platform_kit.common.runtime_target import resolve_runtime_target_from_env


def composer_report(env, started_at, run_id):
    def forbidden(*args, **kwargs):
        raise AssertionError("External IO is forbidden")
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    target["execution_mode"] = "live"
    runtime = resolve_runtime_target_from_env(env={**env, "RUNTIME_TARGET_JSON": json.dumps(target)}, expected_platform_id="schwab")
    composer = SchwabRuntimeComposer(
        project_id=None, service_name="synthetic-service", secret_id="", app_key=None, app_secret=None,
        token_path="", strategy_profile="strategy-a", strategy_domain="us_equity",
        strategy_display_name="Synthetic", strategy_display_name_localized="Synthetic", notify_lang="en",
        tg_token=None, tg_chat_id=None, managed_symbols=(), benchmark_symbol="",
        signal_effective_after_trading_days=1, dry_run_only=False, limit_buy_premium=1.0,
        sell_settle_delay_sec=0, post_sell_refresh_attempts=0, post_sell_refresh_interval_sec=0,
        safe_haven_cash_substitute_threshold_usd=0, broker_adapters=None, strategy_adapters=None,
        client_builder=forbidden, run_id_builder=lambda: run_id, event_logger=forbidden,
        report_builder=build_runtime_report_base, report_persister=forbidden,
        env_reader=lambda name, default="": default, printer=forbidden, runtime_target=runtime,
        reporting_builder=lambda **kwargs: build_runtime_reporting_adapters(clock=lambda: started_at, **kwargs),
    )
    _, source = composer.build_reporting_adapters().start_run()
    assert source["platform"] == "charles_schwab"
    source["runtime_release_receipt"] = {"attestation_state":"self_attested", "strategy_release":{"strategy_revision":"a"*40}}
    return source
