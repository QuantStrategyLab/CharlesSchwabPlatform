"""Offline public-loader characterization captured before ARCH-08B.

The frozen loader and dataclass are from main at
 bd5239fbfc077679ef987ae797e5c74052c640c8. They deliberately preserve the
complete constructor and its evaluation order as an independent oracle.
Only unrelated strategy setup and external I/O boundaries are injected;
the installed, locked QPK parsers execute normally.
"""
from __future__ import annotations

import ast
import dataclasses
import inspect
import os
import sys
from types import SimpleNamespace

import pytest

import runtime_config_support as config

BASELINE_LOADER = r'''
def load_platform_runtime_settings() -> PlatformRuntimeSettings:
    runtime_target = resolve_runtime_target_from_env(env=os.environ, expected_platform_id=SCHWAB_PLATFORM)
    trusted_runtime_risk_policy = _load_trusted_runtime_risk_policy()
    dry_run_only = resolve_dry_run_env(os.environ, "SCHWAB_DRY_RUN_ONLY")
    paper_execution_admission_enabled = resolve_optional_bool_env(
        "SCHWAB_PAPER_EXECUTION_ADMISSION_ENABLED",
        default=False,
    )
    paper_execution_command_consumer_enabled = resolve_optional_bool_env(
        "SCHWAB_PAPER_EXECUTION_COMMAND_CONSUMER_ENABLED",
        default=False,
    )
    if paper_execution_admission_enabled and (
        not dry_run_only or str(runtime_target.execution_mode or "").strip().lower() != "paper"
    ):
        raise EnvironmentError(
            "SCHWAB_PAPER_EXECUTION_ADMISSION_ENABLED requires a dry-run PAPER runtime target"
        )
    if paper_execution_command_consumer_enabled and (
        not dry_run_only or str(runtime_target.execution_mode or "").strip().lower() != "paper"
    ):
        raise EnvironmentError(
            "SCHWAB_PAPER_EXECUTION_COMMAND_CONSUMER_ENABLED requires a dry-run PAPER runtime target"
        )
    strategy_definition = resolve_strategy_definition(
        runtime_target.strategy_profile,
        platform_id=SCHWAB_PLATFORM,
    )
    strategy_metadata = resolve_strategy_metadata(
        strategy_definition.profile,
        platform_id=SCHWAB_PLATFORM,
    )
    runtime_paths = resolve_strategy_runtime_path_settings(
        strategy_catalog=get_strategy_catalog(),
        strategy_definition=strategy_definition,
        strategy_metadata=strategy_metadata,
        platform_env_prefix="SCHWAB",
        env=os.environ,
        repo_root=Path(__file__).resolve().parent,
    )
    return PlatformRuntimeSettings(
        strategy_profile=runtime_paths.strategy_profile,
        strategy_display_name=runtime_paths.strategy_display_name,
        strategy_domain=runtime_paths.strategy_domain,
        strategy_metadata=strategy_metadata,
        notify_lang=os.getenv("QSL_NOTIFY_LANG") or os.getenv("NOTIFY_LANG", DEFAULT_NOTIFY_LANG),
        dry_run_only=dry_run_only,
        runtime_target_enabled=(
            _runtime_target_enabled_env()
            and runtime_target_permits_standard_execution(runtime_target)
        ),
        paper_execution_admission_enabled=paper_execution_admission_enabled,
        paper_execution_command_consumer_enabled=paper_execution_command_consumer_enabled,
        reserved_cash_floor_usd=_resolve_non_negative_float_env(
            "SCHWAB_MIN_RESERVED_CASH_USD",
            default=DEFAULT_RESERVED_CASH_FLOOR_USD,
        ),
        reserved_cash_ratio=_resolve_ratio_env(
            "SCHWAB_RESERVED_CASH_RATIO",
            default=DEFAULT_RESERVED_CASH_RATIO,
        ),
        safe_haven_cash_substitute_threshold_usd=_resolve_non_negative_float_env(
            "SCHWAB_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD",
            default=DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD,
        ),
        cash_only_execution=resolve_cash_only_execution_env(
            os.environ,
            platform_env_prefix="SCHWAB",
        ),
        income_layer_enabled=_optional_bool_env("INCOME_LAYER_ENABLED"),
        income_layer_start_usd=_optional_non_negative_float_env("INCOME_LAYER_START_USD"),
        income_layer_max_ratio=resolve_optional_ratio_env("INCOME_LAYER_MAX_RATIO"),
        dca_mode=resolve_optional_dca_mode_env("DCA_MODE"),
        dca_base_investment_usd=resolve_optional_positive_float_env("DCA_BASE_INVESTMENT_USD"),
        market_signal_handoff_index_uri=_first_non_empty(
            os.getenv("SCHWAB_MARKET_SIGNAL_HANDOFF_INDEX_URI"),
            os.getenv("MARKET_SIGNAL_HANDOFF_INDEX_URI"),
        ),
        market_signal_handoff_manifest_uri=_first_non_empty(
            os.getenv("SCHWAB_MARKET_SIGNAL_HANDOFF_MANIFEST_URI"),
            os.getenv("MARKET_SIGNAL_HANDOFF_MANIFEST_URI"),
        ),
        market_signal_consumption_audit_uri=_first_non_empty(
            os.getenv("SCHWAB_MARKET_SIGNAL_CONSUMPTION_AUDIT_URI"),
            os.getenv("MARKET_SIGNAL_CONSUMPTION_AUDIT_URI"),
        ),
        market_signal_cache_dir=_first_non_empty(
            os.getenv("SCHWAB_MARKET_SIGNAL_CACHE_DIR"),
            os.getenv("MARKET_SIGNAL_CACHE_DIR"),
        ),
        market_signal_required=resolve_bool_value(
            _first_non_empty(
                os.getenv("SCHWAB_MARKET_SIGNAL_REQUIRED"),
                os.getenv("MARKET_SIGNAL_REQUIRED"),
                "false",
            )
        ),
        market_signal_fallback_mode=_first_non_empty(
            os.getenv("SCHWAB_MARKET_SIGNAL_FALLBACK_MODE"),
            os.getenv("MARKET_SIGNAL_FALLBACK_MODE"),
        ),
        market_signal_max_stale_days=_optional_int(
            _first_non_empty(
                os.getenv("SCHWAB_MARKET_SIGNAL_MAX_STALE_DAYS"),
                os.getenv("SCHWAB_MARKET_SIGNAL_FALLBACK_MAX_STALE_DAYS"),
                os.getenv("MARKET_SIGNAL_MAX_STALE_DAYS"),
                os.getenv("MARKET_SIGNAL_FALLBACK_MAX_STALE_DAYS"),
            )
        ),
        feature_snapshot_path=runtime_paths.feature_snapshot_path,
        feature_snapshot_manifest_path=runtime_paths.feature_snapshot_manifest_path,
        feature_snapshot_fallback_mode=_first_non_empty(
            os.getenv("SCHWAB_FEATURE_SNAPSHOT_FALLBACK_MODE"),
            os.getenv("FEATURE_SNAPSHOT_FALLBACK_MODE"),
        ),
        feature_snapshot_fallback_cache_dir=_first_non_empty(
            os.getenv("SCHWAB_FEATURE_SNAPSHOT_FALLBACK_CACHE_DIR"),
            os.getenv("FEATURE_SNAPSHOT_FALLBACK_CACHE_DIR"),
        ),
        feature_snapshot_fallback_max_stale_days=_optional_int(
            _first_non_empty(
                os.getenv("SCHWAB_FEATURE_SNAPSHOT_MAX_STALE_DAYS"),
                os.getenv("SCHWAB_FEATURE_SNAPSHOT_FALLBACK_MAX_STALE_DAYS"),
                os.getenv("FEATURE_SNAPSHOT_MAX_STALE_DAYS"),
                os.getenv("FEATURE_SNAPSHOT_FALLBACK_MAX_STALE_DAYS"),
            )
        ),
        strategy_config_path=runtime_paths.strategy_config_path,
        strategy_config_source=runtime_paths.strategy_config_source,
        strategy_plugin_mounts_json=(
            os.getenv("SCHWAB_STRATEGY_PLUGIN_MOUNTS_JSON")
            or os.getenv("QSL_STRATEGY_PLUGIN_MOUNTS_JSON")
            or os.getenv("STRATEGY_PLUGIN_MOUNTS_JSON")
        ),
        strategy_plugin_alert_channels=resolve_split_env_list("STRATEGY_PLUGIN_ALERT_CHANNELS"),
        strategy_plugin_alert_email_recipients=resolve_split_env_list("STRATEGY_PLUGIN_ALERT_EMAIL_RECIPIENTS"),
        strategy_plugin_alert_email_sender_email=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SENDER_EMAIL")),
        strategy_plugin_alert_email_sender_password=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SENDER_PASSWORD")
        ),
        strategy_plugin_alert_email_smtp_host=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SMTP_HOST")),
        strategy_plugin_alert_email_smtp_port=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SMTP_PORT")),
        strategy_plugin_alert_email_smtp_security=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SMTP_SECURITY")
        ),
        strategy_plugin_alert_sms_recipients=resolve_split_env_list("STRATEGY_PLUGIN_ALERT_SMS_RECIPIENTS"),
        strategy_plugin_alert_sms_provider=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_SMS_PROVIDER")),
        strategy_plugin_alert_sms_account_id=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_SMS_ACCOUNT_ID")),
        strategy_plugin_alert_sms_auth_token=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_SMS_AUTH_TOKEN")),
        strategy_plugin_alert_sms_sender=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_SMS_SENDER")),
        strategy_plugin_alert_sms_messaging_service_id=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_SMS_MESSAGING_SERVICE_ID")
        ),
        strategy_plugin_alert_sms_api_base_url=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_SMS_API_BASE_URL")),
        strategy_plugin_alert_sms_body_max_chars=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_SMS_BODY_MAX_CHARS")
        ),
        strategy_plugin_alert_push_recipients=resolve_split_env_list("STRATEGY_PLUGIN_ALERT_PUSH_RECIPIENTS"),
        strategy_plugin_alert_push_provider=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_PUSH_PROVIDER")),
        strategy_plugin_alert_push_app_token=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_PUSH_APP_TOKEN")),
        strategy_plugin_alert_push_access_token=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_PUSH_ACCESS_TOKEN")),
        strategy_plugin_alert_push_api_base_url=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_PUSH_API_BASE_URL")),
        strategy_plugin_alert_push_device=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_PUSH_DEVICE")),
        strategy_plugin_alert_push_priority=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_PUSH_PRIORITY")),
        strategy_plugin_alert_push_tags=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_PUSH_TAGS")),
        strategy_plugin_alert_push_body_max_chars=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_PUSH_BODY_MAX_CHARS")
        ),
        strategy_plugin_alert_telegram_chat_ids=resolve_split_env_list(
            "STRATEGY_PLUGIN_ALERT_TELEGRAM_CHAT_IDS"
        ),
        strategy_plugin_alert_telegram_bot_token=_first_non_empty(
            os.getenv("QSL_STRATEGY_PLUGIN_ALERT_TELEGRAM_BOT_TOKEN")
            or os.getenv("STRATEGY_PLUGIN_ALERT_TELEGRAM_BOT_TOKEN")
        ),
        strategy_plugin_alert_telegram_api_base_url=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_TELEGRAM_API_BASE_URL")
        ),
        strategy_plugin_alert_telegram_parse_mode=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_TELEGRAM_PARSE_MODE")
        ),
        strategy_plugin_alert_telegram_disable_web_page_preview=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_TELEGRAM_DISABLE_WEB_PAGE_PREVIEW")
        ),
        strategy_plugin_alert_telegram_body_max_chars=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_TELEGRAM_BODY_MAX_CHARS")
        ),
        notification_channel=os.getenv("NOTIFICATION_CHANNEL", "telegram"),
        wecom_webhook_url=os.getenv("NOTIFICATION_WECOM_WEBHOOK_URL"),
        dingtalk_webhook_url=os.getenv("NOTIFICATION_DINGTALK_WEBHOOK_URL"),
        feishu_webhook_url=os.getenv("NOTIFICATION_FEISHU_WEBHOOK_URL"),
        serverchan_webhook_url=os.getenv("NOTIFICATION_SERVERCHAN_WEBHOOK_URL"),
        runtime_target=runtime_target,
        trusted_runtime_risk_policy=trusted_runtime_risk_policy,
    )
'''

BASELINE_SETTINGS = r'''
@dataclass(frozen=True)
class PlatformRuntimeSettings:
    strategy_profile: str
    strategy_display_name: str
    strategy_domain: str
    notify_lang: str
    dry_run_only: bool
    runtime_target_enabled: bool = False
    paper_execution_admission_enabled: bool = False
    paper_execution_command_consumer_enabled: bool = False
    reserved_cash_floor_usd: float = DEFAULT_RESERVED_CASH_FLOOR_USD
    reserved_cash_ratio: float = DEFAULT_RESERVED_CASH_RATIO
    safe_haven_cash_substitute_threshold_usd: float = DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD
    cash_only_execution: bool = True
    income_layer_enabled: bool | None = None
    income_layer_start_usd: float | None = None
    income_layer_max_ratio: float | None = None
    dca_mode: str | None = None
    dca_base_investment_usd: float | None = None
    market_signal_handoff_index_uri: str | None = None
    market_signal_handoff_manifest_uri: str | None = None
    market_signal_consumption_audit_uri: str | None = None
    market_signal_cache_dir: str | None = None
    market_signal_required: bool = False
    market_signal_fallback_mode: str | None = None
    market_signal_max_stale_days: int | None = None
    feature_snapshot_path: str | None = None
    feature_snapshot_manifest_path: str | None = None
    feature_snapshot_fallback_mode: str | None = None
    feature_snapshot_fallback_cache_dir: str | None = None
    feature_snapshot_fallback_max_stale_days: int | None = None
    strategy_config_path: str | None = None
    strategy_config_source: str | None = None
    strategy_plugin_mounts_json: str | None = None
    strategy_plugin_alert_channels: tuple[str, ...] = ()
    strategy_plugin_alert_email_recipients: tuple[str, ...] = ()
    strategy_plugin_alert_email_sender_email: str | None = None
    strategy_plugin_alert_email_sender_password: str | None = None
    strategy_plugin_alert_email_smtp_host: str | None = None
    strategy_plugin_alert_email_smtp_port: str | None = None
    strategy_plugin_alert_email_smtp_security: str | None = None
    strategy_plugin_alert_sms_recipients: tuple[str, ...] = ()
    strategy_plugin_alert_sms_provider: str | None = None
    strategy_plugin_alert_sms_account_id: str | None = None
    strategy_plugin_alert_sms_auth_token: str | None = None
    strategy_plugin_alert_sms_sender: str | None = None
    strategy_plugin_alert_sms_messaging_service_id: str | None = None
    strategy_plugin_alert_sms_api_base_url: str | None = None
    strategy_plugin_alert_sms_body_max_chars: str | None = None
    strategy_plugin_alert_push_recipients: tuple[str, ...] = ()
    strategy_plugin_alert_push_provider: str | None = None
    strategy_plugin_alert_push_app_token: str | None = None
    strategy_plugin_alert_push_access_token: str | None = None
    strategy_plugin_alert_push_api_base_url: str | None = None
    strategy_plugin_alert_push_device: str | None = None
    strategy_plugin_alert_push_priority: str | None = None
    strategy_plugin_alert_push_tags: str | None = None
    strategy_plugin_alert_push_body_max_chars: str | None = None
    strategy_plugin_alert_telegram_chat_ids: tuple[str, ...] = ()
    strategy_plugin_alert_telegram_bot_token: str | None = None
    strategy_plugin_alert_telegram_api_base_url: str | None = None
    strategy_plugin_alert_telegram_parse_mode: str | None = None
    strategy_plugin_alert_telegram_disable_web_page_preview: str | None = None
    strategy_plugin_alert_telegram_body_max_chars: str | None = None
    notification_channel: str = "telegram"
    wecom_webhook_url: str | None = None
    dingtalk_webhook_url: str | None = None
    feishu_webhook_url: str | None = None
    serverchan_webhook_url: str | None = None
    runtime_target: RuntimeTarget | None = None
    strategy_metadata: Any = None
    trusted_runtime_risk_policy: Mapping[str, Any] | None = None
'''

ALERT_FIELDS = tuple(
    field.name for field in dataclasses.fields(config.PlatformRuntimeSettings)
    if field.name.startswith("strategy_plugin_alert_")
)
LIST_FIELDS = {
    "strategy_plugin_alert_channels", "strategy_plugin_alert_email_recipients",
    "strategy_plugin_alert_sms_recipients", "strategy_plugin_alert_push_recipients",
    "strategy_plugin_alert_telegram_chat_ids",
}
ALERT_ENV = tuple(name.upper() for name in ALERT_FIELDS)
QSL_TOKEN = "QSL_STRATEGY_PLUGIN_ALERT_TELEGRAM_BOT_TOKEN"
STABLE_SETTINGS_TYPE = config.PlatformRuntimeSettings


class SentinelError(RuntimeError):
    pass


@pytest.fixture
def harness():
    target = SimpleNamespace(strategy_profile="synthetic-profile", execution_mode="live")
    definition = SimpleNamespace(profile="synthetic-profile")
    metadata = object()
    paths = SimpleNamespace(
        strategy_profile="synthetic-profile", strategy_display_name="Synthetic strategy",
        strategy_domain="synthetic-domain", feature_snapshot_path="synthetic-feature",
        feature_snapshot_manifest_path="synthetic-manifest",
        strategy_config_path="synthetic-config", strategy_config_source="synthetic-source",
    )
    catalog = object()

    def run(values=None, *, baseline=False, fail_at=None, early=False, sentinel=None):
        trace = []
        sentinel = sentinel or SentinelError("synthetic failure")

        def event(*item):
            trace.append(item)
            if len(trace) - 1 == fail_at:
                raise sentinel

        class Environment(dict):
            def get(self, key, default=None):
                event("env", key, default)
                return super().get(key, default)

            def __getitem__(self, key):
                event("getitem", key)
                return super().__getitem__(key)

        environment = Environment(RUNTIME_TARGET_JSON='{"runtime_risk_limits":{"synthetic_limit":7}}')
        environment.update(values or {})
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "environ", environment)
            setup = {
                "resolve_runtime_target_from_env": target,
                "resolve_strategy_definition": definition,
                "resolve_strategy_metadata": metadata,
                "resolve_strategy_runtime_path_settings": paths,
                "get_strategy_catalog": catalog,
            }
            for name, value in setup.items():
                def boundary(*args, _name=name, _value=value, **kwargs):
                    event("dependency", _name, args, kwargs)
                    if early and _name == "resolve_runtime_target_from_env":
                        raise sentinel
                    return _value
                patch.setattr(config, name, boundary)

            for name in tuple(vars(config)):
                if name in setup or name == "resolve_telegram_token":
                    continue
                if not (name.startswith(("resolve_", "_resolve_", "_optional_"))
                        or name in {"_first_non_empty", "_load_trusted_runtime_risk_policy",
                                    "_runtime_target_enabled_env", "_qqqi_income_ratio_env",
                                    "_runtime_execution_window_trading_days_env"}):
                    continue
                original = getattr(config, name)
                if not callable(original):
                    continue
                def dependency(*args, _name=name, _original=original, **kwargs):
                    event("dependency", _name, args, kwargs)
                    value = _original(*args, **kwargs)
                    if _name == "_load_trusted_runtime_risk_policy":
                        event("policy_result", value)
                    return value
                patch.setattr(config, name, dependency)

            def constructor(**kwargs):
                event("construct")
                return STABLE_SETTINGS_TYPE(**kwargs)
            patch.setattr(config, "PlatformRuntimeSettings", constructor)
            if baseline:
                # The function resolves the same live module globals and patch locations.
                namespace = {}
                exec(compile(BASELINE_LOADER, "<arch08b-baseline-loader>", "exec"),
                     config.__dict__, namespace)
                load = namespace["load_platform_runtime_settings"]
            else:
                load = config.load_platform_runtime_settings

            try:
                result = load()
            except Exception as error:
                return error, trace
            policy = next(item[1] for item in trace if item[0] == "policy_result")
            assert result.trusted_runtime_risk_policy is policy
            return result, trace

    return run, target, metadata


def compare(harness, values=None, **kwargs):
    run, target, metadata = harness
    original, expected_trace = run(values, baseline=True, **kwargs)
    actual, actual_trace = run(values, **kwargs)
    assert actual_trace == expected_trace
    if isinstance(original, Exception):
        assert type(actual) is type(original)
        assert str(actual) == str(original)
        if "sentinel" in kwargs:
            assert actual is original is kwargs["sentinel"]
        assert ("construct",) not in actual_trace
    else:
        assert type(actual) is STABLE_SETTINGS_TYPE
        assert actual == original
        for field in dataclasses.fields(actual):
            assert type(getattr(actual, field.name)) is type(getattr(original, field.name))
        assert actual.runtime_target is original.runtime_target is target
        assert actual.strategy_metadata is original.strategy_metadata is metadata
    return actual, actual_trace


def test_c01_defaults_and_c09_contract(harness):
    result, trace = compare(harness)
    assert len(ALERT_FIELDS) == 30
    for name in ALERT_FIELDS:
        assert getattr(result, name) == (() if name in LIST_FIELDS else None)
    assert trace[-1] == ("construct",)
    namespace = dict(config.__dict__)
    exec(compile(BASELINE_SETTINGS, "<arch08b-baseline-settings>", "exec"), namespace)
    expected = namespace["PlatformRuntimeSettings"]
    def field_contract(cls):
        return [tuple(getattr(field, name) for name in (
            "name", "type", "default", "default_factory", "init", "repr", "hash", "compare",
            "metadata", "kw_only",
        )) for field in dataclasses.fields(cls)]
    assert field_contract(STABLE_SETTINGS_TYPE) == field_contract(expected)
    assert inspect.signature(STABLE_SETTINGS_TYPE) == inspect.signature(expected)
    assert STABLE_SETTINGS_TYPE.__dataclass_params__.frozen is True
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.notify_lang = "changed"
    namespace = {}
    exec(compile(BASELINE_LOADER, "<arch08b-baseline-loader>", "exec"), config.__dict__, namespace)
    assert inspect.signature(config.load_platform_runtime_settings) == inspect.signature(
        namespace["load_platform_runtime_settings"]
    )


def test_c02_all_fields_have_distinct_values(harness):
    values = {name.upper(): f"synthetic-{index}" for index, name in enumerate(ALERT_FIELDS)}
    result, _ = compare(harness, values)
    for index, name in enumerate(ALERT_FIELDS):
        expected = f"synthetic-{index}"
        assert getattr(result, name) == ((expected,) if name in LIST_FIELDS else expected)


@pytest.mark.parametrize("raw", ["", "   ", "  synthetic value  "])
def test_c03_empty_and_whitespace(harness, raw):
    compare(harness, dict.fromkeys(ALERT_ENV, raw))


@pytest.mark.parametrize("raw", ["beta;alpha,,beta; ;gamma", "gamma,alpha;beta,alpha", ", ; ; ,"])
def test_c04_csv_order_duplicates_and_blanks(harness, raw):
    compare(harness, {name.upper(): raw for name in LIST_FIELDS})


@pytest.mark.parametrize("qsl", [None, "", "   ", "  synthetic-qsl  "])
def test_c05_telegram_precedence_and_short_circuit(harness, qsl):
    values = {"STRATEGY_PLUGIN_ALERT_TELEGRAM_BOT_TOKEN": "synthetic-legacy"}
    if qsl is not None:
        values[QSL_TOKEN] = qsl
    result, trace = compare(harness, values)
    expected = None if qsl == "   " else "synthetic-qsl" if qsl else "synthetic-legacy"
    assert result.strategy_plugin_alert_telegram_bot_token == expected
    legacy_reads = [item for item in trace if item[:2] == ("env", "STRATEGY_PLUGIN_ALERT_TELEGRAM_BOT_TOKEN")]
    assert bool(legacy_reads) is (not bool(qsl))


@pytest.mark.parametrize("raw", ["0", "false", "00587", "-1", "unbounded"])
def test_c06_opaque_strings(harness, raw):
    result, _ = compare(harness, dict.fromkeys(ALERT_ENV, raw))
    for name in ALERT_FIELDS:
        assert getattr(result, name) == ((raw,) if name in LIST_FIELDS else raw)


def test_c07_fresh_reads_on_each_call(harness):
    for value in ("synthetic-first", "synthetic-second", ""):
        compare(harness, dict.fromkeys(ALERT_ENV, value))


def test_c08_every_alert_read_and_parser_failure(harness, subtests):
    run = harness[0]
    # No QSL token means the legacy read is exercised as well.
    values = {name: "synthetic-value" for name in ALERT_ENV}
    _, trace = run(values, baseline=True)
    start = next(i for i, event in enumerate(trace)
                 if event[:2] == ("dependency", "resolve_split_env_list"))
    end = next(i for i, event in enumerate(trace)
               if event[:2] == ("env", "NOTIFICATION_CHANNEL"))
    for index in range(start, end):
        with subtests.test(stage=index, event=trace[index][:2]):
            sentinel = SentinelError(f"synthetic stage {index}")
            error, failed_trace = compare(harness, values, fail_at=index, sentinel=sentinel)
            assert error is sentinel
            assert failed_trace == trace[:index + 1]
            assert not any(event[:2] == ("env", "NOTIFICATION_CHANNEL")
                           for event in failed_trace)


def test_c10_initialization_failure_precedes_alert_reads(harness):
    error, trace = compare(harness, early=True, sentinel=SentinelError("synthetic early failure"))
    assert isinstance(error, SentinelError)
    assert not any("STRATEGY_PLUGIN_ALERT" in str(event) for event in trace)


def test_c10_existing_validation_precedes_alert_reads(harness):
    error, trace = compare(harness, {"SCHWAB_RESERVED_CASH_RATIO": "1.25"})
    assert isinstance(error, ValueError)
    assert not any("STRATEGY_PLUGIN_ALERT" in str(event) for event in trace)


def test_c11_harness_restores_environment_globals_and_modules(harness):
    environment = os.environ
    globals_before = dict(config.__dict__)
    modules_before = dict(sys.modules)
    compare(harness, dict.fromkeys(ALERT_ENV, "synthetic-cleanup"))
    assert os.environ is environment
    assert config.__dict__ == globals_before
    assert sys.modules == modules_before


def test_normalized_loader_ast_matches_frozen_baseline():
    tree = ast.parse(inspect.getsource(config.load_platform_runtime_settings))
    helper = getattr(config, "_load_strategy_plugin_alert_settings", None)
    if helper is not None:
        helper_tree = ast.parse(inspect.getsource(helper))
        returned = helper_tree.body[0].body[0].value
        assert isinstance(returned, ast.Dict)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id == "PlatformRuntimeSettings":
                    expanded = []
                    for keyword in node.keywords:
                        if keyword.arg is None:
                            assert ast.dump(keyword.value) == ast.dump(ast.parse(
                                "_load_strategy_plugin_alert_settings()", mode="eval").body)
                            expanded.extend(ast.keyword(arg=key.value, value=value)
                                            for key, value in zip(returned.keys, returned.values))
                        else:
                            expanded.append(keyword)
                    node.keywords = expanded
    assert ast.dump(tree, include_attributes=False) == ast.dump(
        ast.parse(BASELINE_LOADER), include_attributes=False
    )


@pytest.mark.parametrize("values", [
    {"RUNTIME_TARGET_JSON": "invalid-json"},
    {"RUNTIME_TARGET_JSON": "[]"},
    {"RUNTIME_TARGET_JSON": '{"runtime_risk_limits":[]}'},
    {"SCHWAB_PAPER_EXECUTION_ADMISSION_ENABLED": "true"},
    {"SCHWAB_PAPER_EXECUTION_COMMAND_CONSUMER_ENABLED": "true"},
    {"RUNTIME_TARGET_ENABLED": "invalid-bool"},
    {"SCHWAB_MIN_RESERVED_CASH_USD": "-1"},
    {"INCOME_LAYER_START_USD": "nan"},
])
def test_c10_existing_early_validation_failures(harness, values):
    error, trace = compare(harness, values)
    assert isinstance(error, Exception)
    assert not any("STRATEGY_PLUGIN_ALERT" in str(event) for event in trace)


def test_c10_failure_at_each_pre_alert_stage(harness, subtests):
    run = harness[0]
    _, trace = run(baseline=True)
    end = next(i for i, event in enumerate(trace)
               if event[:2] == ("dependency", "resolve_split_env_list"))
    for index in range(end):
        with subtests.test(stage=index, event=trace[index][:2]):
            sentinel = SentinelError(f"synthetic early stage {index}")
            error, failed_trace = compare(harness, fail_at=index, sentinel=sentinel)
            assert error is sentinel
            assert failed_trace == trace[:index + 1]
            assert not any("STRATEGY_PLUGIN_ALERT" in str(event) for event in failed_trace)


def test_c09_non_alert_channels_and_reference_identity(harness):
    values = {
        "NOTIFICATION_CHANNEL": "synthetic-channel",
        "NOTIFICATION_WECOM_WEBHOOK_URL": "https://wecom.example.test/synthetic",
        "NOTIFICATION_DINGTALK_WEBHOOK_URL": "https://dingtalk.example.test/synthetic",
        "NOTIFICATION_FEISHU_WEBHOOK_URL": "https://feishu.example.test/synthetic",
        "NOTIFICATION_SERVERCHAN_WEBHOOK_URL": "https://serverchan.example.test/synthetic",
    }
    result, trace = compare(harness, values)
    assert result.notification_channel == "synthetic-channel"
    for field, variable in (
        ("wecom_webhook_url", "NOTIFICATION_WECOM_WEBHOOK_URL"),
        ("dingtalk_webhook_url", "NOTIFICATION_DINGTALK_WEBHOOK_URL"),
        ("feishu_webhook_url", "NOTIFICATION_FEISHU_WEBHOOK_URL"),
        ("serverchan_webhook_url", "NOTIFICATION_SERVERCHAN_WEBHOOK_URL"),
    ):
        assert getattr(result, field) == values[variable]
    policy = next(item[1] for item in trace if item[0] == "policy_result")
    assert result.trusted_runtime_risk_policy is policy
    assert result.trusted_runtime_risk_policy == {"synthetic_limit": 7}
