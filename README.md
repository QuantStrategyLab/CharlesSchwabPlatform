# Schwab Trinity Strategy Bot

<!-- qsl-doc-overview:start -->

> ⚠️ 投资有风险，不构成投资建议，仅供学习交流用途。
> ⚠️ Investing involves risk. This project does not provide investment advice and is for educational and research purposes only.

## Open-source overview / 开源项目入口

| Item | Description |
| --- | --- |
| Project type | execution platform |
| What it does | Charles Schwab execution platform for US equity strategies with token refresh integration and external strategy loading. |
| 中文说明 | Schwab 美股执行平台，负责 token、账户、策略加载和 dry-run/live 执行边界。 |
| Current status | Execution platform. Token and account settings are sensitive and must not be committed. |

### Quick start

- `python -m pip install -e '.[test]'`
- `python -m pytest -q`

### Deploy / operate safely

Run dry-run first, confirm token refresh and account settings, then deploy via the documented Cloud Run/GitHub Actions path.

### Strategy performance / evidence boundary

Strategy performance is documented in strategy/snapshot repos, not in this platform adapter.

> Detailed runbooks, migration notes, workflow internals, and historical decisions are kept below. Start with this overview before using the lower-level operational sections.

<!-- qsl-doc-overview:end -->

> Risk warning: this project is not investment advice and is provided for study and engineering validation only.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Platform](https://img.shields.io/badge/Broker-Charles%20Schwab-00a0df)
![Strategy](https://img.shields.io/badge/Strategy-Trinity%20Hybrid-orange)
![GCP](https://img.shields.io/badge/GCP-Cloud%20Run-4285F4)

Language: [English](README.md) | [中文](README.zh-CN.md)

---

Automated trading service for Charles Schwab accounts, deployed on GCP Cloud Run. This repository runs shared `us_equity` strategy profiles from `UsEquityStrategies`; strategy logic, cadence, asset universes, parameters, and research/backtest notes live in that strategy repository.
The runtime now carries a structured `RuntimeTarget` / `RUNTIME_TARGET_JSON` alongside the compatibility `STRATEGY_PROFILE` selector. Strategy-owned defaults come from `UsEquityStrategies`; platform variables are only explicit overrides.

This repository uses `QuantPlatformKit` for Schwab client bootstrap, account snapshot access, market data, and order submission. Cloud Run deploys this repository directly.
The Schwab runtime can execute the five current `runtime_enabled` `us_equity` profiles from `UsEquityStrategies`. Tech/Communication is retained in strategy research history, but it is not currently enabled as a Schwab live profile.

Full strategy documentation now lives in [`UsEquityStrategies`](https://github.com/QuantStrategyLab/UsEquityStrategies). The sections below focus on Schwab runtime behavior, profile enablement, deployment, and credentials.
This runtime matrix is the authoritative enablement source for Schwab. `UsEquityStrategies` carries strategy-layer logic, cadence, compatibility, and metadata.
`STRATEGY_PROFILE` remains the compatibility selector for strategy routing, while `RuntimeTarget` describes the running service identity.

### Execution boundary

The mainline runtime now follows one path only:

- `main.py` assembles `StrategyContext` plus platform overrides
- `strategy_runtime.py` loads the unified strategy entrypoint
- `entrypoint.evaluate(ctx)` returns a shared `StrategyDecision`
- `decision_mapper.py` converts that decision into Schwab orders, notifications, and runtime updates

Platform execution no longer depends on `strategy/allocation.py`, hard-coded strategy symbol lists, or direct reads of strategy-private config constants.

### Execution safety

Schwab executes shared `StrategyDecision` objects through a value-mode runtime
plan. Weight-target strategies are translated to value targets using the account
snapshot total equity. If a new or empty account reports non-positive total
equity, the mapper now returns a value-mode `no_execute` plan with zero target
values instead of attempting order translation.


**Schwab profile status**

| Canonical profile | Display name | Eligible | Enabled | Domain | Runtime note |
| --- | --- | --- | --- | --- | --- |
| `global_etf_rotation` | Global ETF Rotation | Yes | Yes | `us_equity` | enabled weight-mode rotation line |
| `russell_1000_multi_factor_defensive` | Russell 1000 Multi-Factor | Yes | Yes | `us_equity` | enabled feature-snapshot stock baseline |
| `mega_cap_leader_rotation_top50_balanced` | Mega Cap Leader Rotation Top50 Balanced | Yes | Yes | `us_equity` | selectable balanced Top50 monthly leader rotation |
| `tqqq_growth_income` | TQQQ Growth Income | Yes | Yes | `us_equity` | selectable growth line |
| `soxl_soxx_trend_income` | SOXL/SOXX Semiconductor Trend Income | Yes | Yes | `us_equity` | enabled value-mode alternative |

Check the current matrix locally:

```bash
python3 scripts/print_strategy_profile_status.py
```

### Strategy documentation boundary

Strategy logic, cadence, asset universes, parameters, and research/backtest notes live in `UsEquityStrategies`. This platform README keeps only Schwab profile enablement, env vars, deployment wiring, broker execution behavior, and notification transport.

### Notifications and orders

Telegram notifications include structured execution and heartbeat messages, with English and Chinese variants. Strategy-specific signal/status fields come from the selected `UsEquityStrategies` profile; Schwab-specific handling covers account snapshot access, order submission, and runtime error reporting.

Each HTTP request runs one broker execution cycle. The Cloud Scheduler cron should follow the strategy-layer cadence in `UsEquityStrategies`.

### Environment variables

| Variable | Description |
|----------|-------------|
| `SCHWAB_API_KEY` | Schwab API key; recommended to inject from Secret Manager secret `charles-schwab-api-key` |
| `SCHWAB_APP_SECRET` | Schwab API secret; recommended to inject from Secret Manager secret `charles-schwab-app-secret` |
| `TELEGRAM_TOKEN` | Telegram bot token; recommended to inject from Secret Manager secret `charles-schwab-telegram-token` |
| `GLOBAL_TELEGRAM_CHAT_ID` | Telegram chat ID used by this service. |
| `GOOGLE_CLOUD_PROJECT` | GCP project ID |
| `STRATEGY_PROFILE` | Strategy profile selector. Set explicitly per deployment to one `runtime_enabled` `us_equity` profile. |
| `SCHWAB_STRATEGY_PLUGIN_MOUNTS_JSON` | Optional Schwab-side strategy plugin mount JSON. Prefer this Schwab-specific variable; `STRATEGY_PLUGIN_MOUNTS_JSON` is only a shared fallback. |
| `CRISIS_ALERT_CHANNELS` | Optional crisis alert channel list: `email`, `sms`, `push`, and/or `telegram`. |
| `CRISIS_ALERT_EMAIL_RECIPIENTS` | Optional comma/semicolon/newline-separated email-form recipients. Use a normal mailbox for email-only delivery, or a Google Voice-associated mailbox/address to also trigger Google Voice prompts. |
| `CRISIS_ALERT_EMAIL_SENDER_EMAIL` | Optional sender email address used for crisis alert email. Gmail is the default transport, but the sender naming is provider-neutral. |
| `CRISIS_ALERT_EMAIL_SENDER_PASSWORD` | Sender SMTP password or app password. For Cloud Run, prefer `CRISIS_ALERT_EMAIL_SENDER_PASSWORD_SECRET_NAME` in env sync. |
| `CRISIS_ALERT_EMAIL_SMTP_HOST` | Optional SMTP host override. Defaults to Gmail SMTP when unset. |
| `CRISIS_ALERT_EMAIL_SMTP_PORT` | Optional SMTP port override. Defaults to `465` when unset. |
| `CRISIS_ALERT_EMAIL_SMTP_SECURITY` | Optional SMTP security override: `ssl`, `starttls`, or `none`. Defaults to `ssl` when unset. |
| `CRISIS_ALERT_TELEGRAM_CHAT_IDS` | Dedicated crisis-alert Telegram chat IDs. Separate from the strategy-cycle Telegram chat. |
| `CRISIS_ALERT_TELEGRAM_BOT_TOKEN` | Dedicated crisis-alert Telegram bot token. Prefer `CRISIS_ALERT_TELEGRAM_BOT_TOKEN_SECRET_NAME` in env sync. |
| `SCHWAB_MIN_RESERVED_CASH_USD` | Optional platform cash-reserve floor in USD. Default policy: keep `3% of total equity` with no fixed USD floor. Runtime formula: `max(floor, ratio * total_equity)`. |
| `SCHWAB_RESERVED_CASH_RATIO` | Optional platform cash-reserve ratio. Default policy: keep `3% of total equity` with no fixed USD floor. Runtime formula: `max(floor, ratio * total_equity)`. |
| `SCHWAB_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD` | Safe-haven/cash-sweep target values below this USD amount are kept as cash instead of buying BOXX/BIL. Default `1000`. |
| `INCOME_THRESHOLD_USD` | Optional strategy override for the income-layer threshold. Leave unset to use the `UsEquityStrategies` live default, which disables the income layer for normal account sizes. |
| `QQQI_INCOME_RATIO` | Optional strategy override for QQQI share of the income layer, 0–1. Only relevant when the income layer is enabled. |
| `DUAL_DRIVE_UNLEVERED_SYMBOL` | Optional strategy override for the tradable unlevered growth sleeve. Leave unset for `QQQ`; set to `QQQM` only when the deployment intentionally uses QQQM instead of QQQ. |
| `NOTIFY_LANG` | Notification language: `en` (English, default) or `zh` (Chinese) |

Strategy plugin mount JSON belongs to platform/deployment configuration, not strategy code. It decides which plugin artifacts this runtime reads, and must not set `mode`; the plugin artifact is self-identifying and carries the effective mode. Invalid plugin mount config is recorded in the runtime report diagnostics and does not block the base strategy cycle.

When the `crisis_response_shadow` plugin is mounted, the normal Telegram cycle message still includes the compact plugin line. If the plugin signal escalates beyond `no_action` (for example `canonical_route=true_crisis`, `suggested_action=defend`/`blocked`, or `would_trade_if_enabled=true`), the service also sends independent crisis alerts through configured `CRISIS_ALERT_CHANNELS` channels: `email`, `sms`, `push`, and/or `telegram`.
Alert results are written into the runtime report. Duplicate suppression uses stable plugin alert keys and stores markers under `STRATEGY_PLUGIN_ALERT_STATE_GCS_URI` when set, otherwise `EXECUTION_REPORT_GCS_URI`, with a local `/tmp` marker fallback.

`GLOBAL_TELEGRAM_CHAT_ID`, `NOTIFY_LANG`, `CRISIS_ALERT_CHANNELS`, and shared crisis alert settings under `CRISIS_ALERT_EMAIL_*`/`CRISIS_ALERT_PUSH_*` are good candidates for cross-project sharing when the same alert policy applies. `TELEGRAM_TOKEN`, Schwab API credentials, and other runtime secrets should remain repository-specific; alert tokens and passwords should stay in GitHub Secret or GCP Secret Manager.

The Schwab OAuth token payload is read from Secret Manager secret `schwab_token`.

Recommended Secret Manager runtime secrets in the `charlesschwabquant` project:

- `schwab_token`
- `charles-schwab-api-key`
- `charles-schwab-app-secret`
- `charles-schwab-telegram-token`

### GitHub-managed Cloud Run deploy and env sync

This repo includes `.github/workflows/sync-cloud-run-env.yml` for GitHub-managed Cloud Run automation. Set `ENABLE_GITHUB_CLOUD_RUN_DEPLOY=true` to build and deploy the container image from GitHub Actions; set `ENABLE_GITHUB_ENV_SYNC=true` to sync runtime env vars; set `ENABLE_MAIN_PUSH_CLOUD_RUN_AUTOMATION=true` when `main` pushes should run the same automation. You can enable these flags independently during migration from a Google Cloud Trigger.

Recommended setup:

- **Repository Variables**
  - `ENABLE_GITHUB_CLOUD_RUN_DEPLOY` = `true` to let GitHub Actions build/push/deploy the Cloud Run image
  - `ENABLE_GITHUB_ENV_SYNC` = `true`
  - `ENABLE_MAIN_PUSH_CLOUD_RUN_AUTOMATION` = `true` to allow `main` pushes to run the deploy/env-sync workflow; manual `workflow_dispatch` runs do not require this flag
  - `CLOUD_RUN_REGION`
  - `CLOUD_RUN_SERVICE`
  - Optional: `GCP_ARTIFACT_REGISTRY_HOSTNAME` when Artifact Registry is not in the Cloud Run region (default: `<CLOUD_RUN_REGION>-docker.pkg.dev`)
  - `TELEGRAM_TOKEN_SECRET_NAME` (recommended: `charles-schwab-telegram-token`)
  - `SCHWAB_API_KEY_SECRET_NAME` (recommended: `charles-schwab-api-key`)
  - `SCHWAB_APP_SECRET_SECRET_NAME` (recommended: `charles-schwab-app-secret`)
  - `STRATEGY_PROFILE` (set explicitly to one enabled profile: `global_etf_rotation`, `mega_cap_leader_rotation_top50_balanced`, `russell_1000_multi_factor_defensive`, `tqqq_growth_income`, or `soxl_soxx_trend_income`)
  - Optional: `SCHWAB_FEATURE_SNAPSHOT_PATH`, `SCHWAB_FEATURE_SNAPSHOT_MANIFEST_PATH`, `SCHWAB_STRATEGY_CONFIG_PATH` for feature-snapshot profiles
  - Optional: `SCHWAB_STRATEGY_PLUGIN_MOUNTS_JSON` for strategy plugin artifact mounts. Do not include `mode` in this platform mount JSON.
  - Optional: `SCHWAB_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD` for the platform cash/BOXX small-notional cutoff.
  - Optional: `INCOME_THRESHOLD_USD` (strategy override only)
  - Optional: `QQQI_INCOME_RATIO` (strategy override only)
  - Optional: `DUAL_DRIVE_UNLEVERED_SYMBOL` (strategy override only)
  - Optional: `CRISIS_ALERT_EMAIL_RECIPIENTS`, `CRISIS_ALERT_EMAIL_SENDER_EMAIL`, `CRISIS_ALERT_EMAIL_SENDER_PASSWORD_SECRET_NAME`
  - Optional: `CRISIS_ALERT_EMAIL_SMTP_HOST`, `CRISIS_ALERT_EMAIL_SMTP_PORT`, `CRISIS_ALERT_EMAIL_SMTP_SECURITY`
  - Optional: `GOOGLE_CLOUD_PROJECT`
- **Repository Secrets**
  - Optional fallback only: `TELEGRAM_TOKEN`
  - Optional fallback only: `SCHWAB_API_KEY`
  - Optional fallback only: `SCHWAB_APP_SECRET`
  - Optional fallback only: `CRISIS_ALERT_EMAIL_SENDER_PASSWORD`
- **Shared Variables already supported**
  - `GLOBAL_TELEGRAM_CHAT_ID`
  - `NOTIFY_LANG`
  - `CRISIS_ALERT_EMAIL_RECIPIENTS`
  - `CRISIS_ALERT_EMAIL_SENDER_EMAIL`
  - `CRISIS_ALERT_EMAIL_SENDER_PASSWORD_SECRET_NAME`
  - optional SMTP overrides: `CRISIS_ALERT_EMAIL_SMTP_HOST`, `CRISIS_ALERT_EMAIL_SMTP_PORT`, `CRISIS_ALERT_EMAIL_SMTP_SECURITY`

On every push to `main`, the workflow can build and deploy the configured Cloud Run service, update it with the values above, and remove `TELEGRAM_CHAT_ID`.

Important:

- The workflow only becomes strict when `ENABLE_GITHUB_ENV_SYNC=true`. If this variable is unset, the sync job is skipped and the old Google Cloud Trigger + manual Cloud Run env setup keeps working. When enabled, it resolves the selected profile's snapshot/config requirements from `scripts/print_strategy_profile_status.py --json` instead of a hard-coded strategy-name list.
- Push-triggered automation only runs when `ENABLE_MAIN_PUSH_CLOUD_RUN_AUTOMATION=true`; manual `workflow_dispatch` keeps using the deploy/env-sync switches above.
- The deploy path only becomes active when `ENABLE_GITHUB_CLOUD_RUN_DEPLOY=true`. If it is unset, an existing Cloud Build trigger can keep owning code deployment while this workflow only syncs env.
- `STRATEGY_PROFILE` is driven by the platform capability matrix plus a rollout allowlist derived from `runtime_enabled` strategy metadata. Today `enabled` includes five live `us_equity` profiles: `global_etf_rotation`, `mega_cap_leader_rotation_top50_balanced`, `russell_1000_multi_factor_defensive`, `tqqq_growth_income`, and `soxl_soxx_trend_income`; archived research-only profiles, including Tech/Communication, are not enabled.
- The current strategy domain is `us_equity`, and the repo now keeps a thin strategy registry so future expansion can grow by domain + profile instead of mixing strategy and platform in one layer.
- `INCOME_THRESHOLD_USD`, `QQQI_INCOME_RATIO`, and `DUAL_DRIVE_UNLEVERED_SYMBOL` are optional env-sync overrides, not platform defaults. Leave them unset to inherit the `UsEquityStrategies` profile defaults; the current `tqqq_growth_income` live default is the no-income QQQ/TQQQ dual-drive mode. Set `DUAL_DRIVE_UNLEVERED_SYMBOL=QQQM` only when the deployment intentionally uses QQQM instead of whole-share QQQ.
- GitHub now authenticates to Google Cloud with OIDC + Workload Identity Federation. `GCP_SA_KEY` is no longer required for this workflow.
- GitHub deploy uses the repository Dockerfile and Artifact Registry. The deploy service account needs Artifact Registry writer, Cloud Run admin, and service-account user permissions for the runtime service account.
- The Telegram token and Schwab API credentials should live in Secret Manager and be referenced by the secret-name variables above. Across multiple quant repos, keep shared settings limited to low-coupling values such as `GLOBAL_TELEGRAM_CHAT_ID`, `NOTIFY_LANG`, and the generic `CRISIS_ALERT_EMAIL_*` crisis mail contract when the same alert policy applies.

### Runtime guard alerting

`.github/workflows/runtime-guard.yml` is a second notification layer for failures
outside the Schwab Flask handler. It reads Cloud Logging for recent Cloud
Scheduler errors and Cloud Run request/runtime failures, then sends Telegram
directly through `CRISIS_ALERT_TELEGRAM_BOT_TOKEN` +
`CRISIS_ALERT_TELEGRAM_CHAT_IDS` or the fallback `TELEGRAM_TOKEN` +
`GLOBAL_TELEGRAM_CHAT_ID`.

The guard does not invoke Cloud Run trading routes. It is meant to catch cases
where Scheduler cannot reach the service, OIDC/IAM/audience is wrong, Cloud Run
returns 4xx/5xx, or the container fails before app-level Telegram fallback code
can run.

Required setup:

- keep `CLOUD_RUN_SERVICE` or `RUNTIME_GUARD_CLOUD_RUN_SERVICES` populated with
  the service names to monitor
- grant the GitHub deploy service account `roles/logging.viewer` on
  `charlesschwabquant`
- keep Telegram chat/token variables or secrets configured in GitHub
- optionally set `RUNTIME_GUARD_SCHEDULER_JOB_PATTERN` to a regex that limits
  Scheduler log checks to this deployment's jobs

The scheduled guard runs every 30 minutes. For a missed-run heartbeat, set
`RUNTIME_GUARD_REQUIRE_SUCCESS=true` and choose
`RUNTIME_GUARD_LOOKBACK_MINUTES` so the window covers the expected Scheduler run.
The default leaves the heartbeat check off to avoid false alerts outside the
active trading window.

`Execution Report Heartbeat` (`.github/workflows/execution-report-heartbeat.yml`)
is the stricter completion check. It runs on weekdays after the expected market
window and verifies that a recent runtime report exists under
`EXECUTION_REPORT_GCS_URI`. It reads the latest report JSON and alerts if no
recent report exists or the recent reports have rejected statuses such as
`error`. The deploy service account needs object read/list access on the report
bucket.

### Deployment unit and naming

- `QuantPlatformKit` is only a shared dependency; Cloud Run still deploys `CharlesSchwabPlatform` itself.
- Recommended Cloud Run service name: `charles-schwab-quant-service`.
- If you later rename or move this repository, reselect the GitHub source in Cloud Build / Cloud Run trigger instead of assuming the previous source binding will follow the rename.
- For the shared deployment model and trigger migration checklist, see [`QuantPlatformKit/docs/deployment_model.md`](../QuantPlatformKit/docs/deployment_model.md).

Deploy as a Cloud Run service and trigger two open-window checks plus the close-window execution: `"/precheck"` at open+15 minutes, `"/probe"` at open+30 minutes, and the root URL near the close window. Set the Scheduler OIDC audience to the Cloud Run service root URL, not the route path. Base the cron cadence on `UsEquityStrategies`. Entry points: Flask routes `"/precheck"`, `"/probe"`, and `"/"` in `main.py`.
