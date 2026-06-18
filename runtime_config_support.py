from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

from quant_platform_kit.common.runtime_config import (
    resolve_bool_value,
    resolve_strategy_runtime_path_settings,
)
from quant_platform_kit.common.runtime_target import (
    RuntimeTarget,
    resolve_runtime_target_from_env,
)
from strategy_registry import (
    SCHWAB_PLATFORM,
    resolve_strategy_definition,
    resolve_strategy_metadata,
)
from us_equity_strategies import get_strategy_catalog

DEFAULT_NOTIFY_LANG = "en"
DEFAULT_RESERVED_CASH_FLOOR_USD = 0.0
DEFAULT_RESERVED_CASH_RATIO = 0.0
DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD = 1000.0


@dataclass(frozen=True)
class PlatformRuntimeSettings:
    strategy_profile: str
    strategy_display_name: str
    strategy_domain: str
    notify_lang: str
    dry_run_only: bool
    runtime_target_enabled: bool = True
    reserved_cash_floor_usd: float = DEFAULT_RESERVED_CASH_FLOOR_USD
    reserved_cash_ratio: float = DEFAULT_RESERVED_CASH_RATIO
    safe_haven_cash_substitute_threshold_usd: float = DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD
    income_layer_enabled: bool | None = None
    income_layer_start_usd: float | None = None
    income_layer_max_ratio: float | None = None
    dca_mode: str | None = None
    dca_base_investment_usd: float | None = None
    feature_snapshot_path: str | None = None
    feature_snapshot_manifest_path: str | None = None
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
    runtime_target: RuntimeTarget | None = None


def _resolve_non_negative_float_env(name: str, *, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return float(default)
    value = float(raw_value)
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value}")
    return value


def _resolve_ratio_env(name: str, *, default: float) -> float:
    value = _resolve_non_negative_float_env(name, default=default)
    if value > 1.0:
        raise ValueError(f"{name} must be in [0,1], got {value}")
    return value


def _optional_bool_env(name: str) -> bool | None:
    raw_value = os.getenv(name)
    if raw_value is None or str(raw_value).strip() == "":
        return None
    value = str(raw_value).strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be boolean, got {raw_value!r}")


def _optional_ratio_env(name: str) -> float | None:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return None
    value = float(raw_value)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value}")
    if not (0.0 <= value <= 1.0):
        raise ValueError(f"{name} must be in [0,1], got {value}")
    return value


def _optional_non_negative_float_env(name: str) -> float | None:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return None
    value = float(raw_value)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value}")
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value}")
    return value


def _optional_positive_float_env(name: str) -> float | None:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return None
    value = float(raw_value)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value}")
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _optional_dca_mode_env(name: str) -> str | None:
    raw_value = os.getenv(name)
    if raw_value is None or str(raw_value).strip() == "":
        return None
    value = str(raw_value).strip().lower()
    aliases = {
        "ordinary": "fixed",
        "ordinary_dca": "fixed",
        "fixed_dca": "fixed",
        "smart_dca": "smart",
    }
    mode = aliases.get(value, value)
    if mode not in {"fixed", "smart"}:
        raise ValueError(f"{name} must be fixed or smart, got {raw_value!r}")
    return mode


def _runtime_target_enabled_env() -> bool:
    value = _optional_bool_env("RUNTIME_TARGET_ENABLED")
    return True if value is None else value


def _first_non_empty(*raw_values: str | None) -> str | None:
    for raw_value in raw_values:
        value = str(raw_value or "").strip()
        if value:
            return value
    return None


def _split_env_list(raw_value: str | None) -> tuple[str, ...]:
    if raw_value is None:
        return ()
    items = []
    seen = set()
    for value in str(raw_value).replace(";", ",").replace("\n", ",").split(","):
        item = value.strip()
        if not item or item in seen:
            continue
        items.append(item)
        seen.add(item)
    return tuple(items)


def resolve_strategy_profile(raw_value: str | None = None) -> str:
    return resolve_strategy_definition(
        raw_value if raw_value is not None else os.getenv("STRATEGY_PROFILE"),
        platform_id=SCHWAB_PLATFORM,
    ).profile


def load_platform_runtime_settings() -> PlatformRuntimeSettings:
    runtime_target = resolve_runtime_target_from_env(env=os.environ, expected_platform_id=SCHWAB_PLATFORM)
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
        notify_lang=os.getenv("NOTIFY_LANG", DEFAULT_NOTIFY_LANG),
        dry_run_only=resolve_bool_value(os.getenv("SCHWAB_DRY_RUN_ONLY")),
        runtime_target_enabled=_runtime_target_enabled_env(),
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
        income_layer_enabled=_optional_bool_env("INCOME_LAYER_ENABLED"),
        income_layer_start_usd=_optional_non_negative_float_env("INCOME_LAYER_START_USD"),
        income_layer_max_ratio=_optional_ratio_env("INCOME_LAYER_MAX_RATIO"),
        dca_mode=_optional_dca_mode_env("DCA_MODE"),
        dca_base_investment_usd=_optional_positive_float_env("DCA_BASE_INVESTMENT_USD"),
        feature_snapshot_path=runtime_paths.feature_snapshot_path,
        feature_snapshot_manifest_path=runtime_paths.feature_snapshot_manifest_path,
        strategy_config_path=runtime_paths.strategy_config_path,
        strategy_config_source=runtime_paths.strategy_config_source,
        strategy_plugin_mounts_json=(
            os.getenv("SCHWAB_STRATEGY_PLUGIN_MOUNTS_JSON")
            or os.getenv("STRATEGY_PLUGIN_MOUNTS_JSON")
        ),
        strategy_plugin_alert_channels=_split_env_list(os.getenv("STRATEGY_PLUGIN_ALERT_CHANNELS")),
        strategy_plugin_alert_email_recipients=_split_env_list(os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_RECIPIENTS")),
        strategy_plugin_alert_email_sender_email=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SENDER_EMAIL")),
        strategy_plugin_alert_email_sender_password=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SENDER_PASSWORD")
        ),
        strategy_plugin_alert_email_smtp_host=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SMTP_HOST")),
        strategy_plugin_alert_email_smtp_port=_first_non_empty(os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SMTP_PORT")),
        strategy_plugin_alert_email_smtp_security=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_EMAIL_SMTP_SECURITY")
        ),
        strategy_plugin_alert_sms_recipients=_split_env_list(os.getenv("STRATEGY_PLUGIN_ALERT_SMS_RECIPIENTS")),
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
        strategy_plugin_alert_push_recipients=_split_env_list(os.getenv("STRATEGY_PLUGIN_ALERT_PUSH_RECIPIENTS")),
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
        strategy_plugin_alert_telegram_chat_ids=_split_env_list(
            os.getenv("STRATEGY_PLUGIN_ALERT_TELEGRAM_CHAT_IDS")
        ),
        strategy_plugin_alert_telegram_bot_token=_first_non_empty(
            os.getenv("STRATEGY_PLUGIN_ALERT_TELEGRAM_BOT_TOKEN")
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
        runtime_target=runtime_target,
    )
