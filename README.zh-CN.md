# Schwab Trinity Strategy Bot

> ⚠️ 投资有风险，不构成投资建议，仅供学习交流用途。

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Platform](https://img.shields.io/badge/Broker-Charles%20Schwab-00a0df)
![Strategy](https://img.shields.io/badge/Strategy-Trinity%20Hybrid-orange)
![GCP](https://img.shields.io/badge/GCP-Cloud%20Run-4285F4)

语言: [中文](README.zh-CN.md) | [English](README.md)

---

## 中文

基于 Charles Schwab 账户的自动化交易服务，部署在 GCP Cloud Run 上。这个仓库负责运行 `UsEquityStrategies` 里的共享 `us_equity` 策略档位；策略逻辑、策略频率、标的池、参数和研究/回测说明都放在策略仓库。

这个仓库通过 `QuantPlatformKit` 复用 Schwab client 初始化、账户快照、行情读取和下单逻辑。Cloud Run 直接部署这个仓库。
Schwab runtime 现在可以直接执行 `UsEquityStrategies` 里的 6 条 `runtime_enabled` `us_equity` 策略：`global_etf_rotation`、`mega_cap_leader_rotation_top50_balanced`、`russell_1000_multi_factor_defensive`、`tqqq_growth_income`、`soxl_soxx_trend_income` 和 `tech_communication_pullback_enhancement`。较弱或重复的研究 profile 已从 Schwab 可配置入口移除。

完整策略说明现在放在 [`UsEquityStrategies`](https://github.com/QuantStrategyLab/UsEquityStrategies)。下面这些章节只保留 Schwab 运行时、profile 启用状态、部署和凭据说明。

### 执行边界

当前主线运行路径已经统一为：

- `main.py` 负责组装 `StrategyContext` 和平台 override
- `strategy_runtime.py` 负责加载统一策略入口
- `entrypoint.evaluate(ctx)` 返回共享的 `StrategyDecision`
- `decision_mapper.py` 再把决策转换成 Schwab 订单、通知和运行时更新

平台执行主线已经不再依赖 `strategy/allocation.py`、硬编码策略符号列表，也不再直接读取策略私有配置常量。

### 执行安全

Schwab 运行时会把共享的 `StrategyDecision` 转换成 value-mode 运行计划。
weight-target 策略会用账户快照里的 total equity 换算成目标金额。如果新账户或空账户
返回的 total equity 非正数，mapper 现在会生成带 `no_execute` 标记、目标金额为 0
的 value-mode 计划，而不是继续进入订单翻译并抛出校验错误。

### 策略文档边界

策略逻辑、策略频率、标的池、参数和研究/回测说明都放在 `UsEquityStrategies`。这个平台 README 只保留 Schwab profile 启用状态、环境变量、部署 wiring、券商执行行为和通知通道说明。

### 通知和订单

Telegram 通知包含结构化的调仓和心跳消息，支持中英文切换。策略相关的信号/状态字段来自当前选择的 `UsEquityStrategies` profile；Schwab 侧负责账户快照、下单和运行时异常处理。

每个 HTTP 请求执行一次券商运行周期。Cloud Scheduler 应在开盘后 15 分钟跑 `"/precheck"`，30 分钟跑 `"/probe"`，临近收盘跑 `"/"`；OIDC audience 也要指向 Cloud Run 服务根 URL，而不是路由路径。cron 频率以 `UsEquityStrategies` 里的策略层频率为准。

### 环境变量

| 变量 | 说明 |
|------|------|
| `SCHWAB_API_KEY` | Schwab API 密钥；建议通过 Secret Manager 的 `charles-schwab-api-key` 注入 |
| `SCHWAB_APP_SECRET` | Schwab API 密钥；建议通过 Secret Manager 的 `charles-schwab-app-secret` 注入 |
| `TELEGRAM_TOKEN` | Telegram 机器人 Token；建议通过 Secret Manager 的 `charles-schwab-telegram-token` 注入 |
| `GLOBAL_TELEGRAM_CHAT_ID` | 这个服务使用的 Telegram Chat ID。 |
| `GOOGLE_CLOUD_PROJECT` | GCP 项目 ID |
| `STRATEGY_PROFILE` | 策略档位选择。每个部署都要显式设置；当前已启用值包括 `global_etf_rotation`、`mega_cap_leader_rotation_top50_balanced`、`russell_1000_multi_factor_defensive`、`tqqq_growth_income`、`soxl_soxx_trend_income` 和 `tech_communication_pullback_enhancement` |
| `SCHWAB_STRATEGY_PLUGIN_MOUNTS_JSON` | 可选的 Schwab 侧策略插件挂载 JSON。优先使用这个 Schwab 专用变量；`STRATEGY_PLUGIN_MOUNTS_JSON` 只作为共享 fallback。 |
| `CRISIS_ALERT_CHANNELS` | 可选危机告警通道列表：`email`、`sms`、`push` 和/或 `telegram`。 |
| `CRISIS_ALERT_EMAIL_RECIPIENTS` | 可选通知收件邮箱。普通邮箱只收邮件；关联 Google Voice 的邮箱/地址会额外触发 Google Voice 提醒。支持逗号、分号或换行分隔。 |
| `CRISIS_ALERT_EMAIL_SENDER_EMAIL` | 可选通知发送方邮箱。默认传输走 Gmail SMTP，但命名不绑定 Gmail。 |
| `CRISIS_ALERT_EMAIL_SENDER_PASSWORD` | 发送方 SMTP 密码或 app password。Cloud Run env sync 建议配置 `CRISIS_ALERT_EMAIL_SENDER_PASSWORD_SECRET_NAME`。 |
| `CRISIS_ALERT_EMAIL_SMTP_HOST` | 可选 SMTP host 覆盖。不设置时默认 Gmail SMTP。 |
| `CRISIS_ALERT_EMAIL_SMTP_PORT` | 可选 SMTP port 覆盖。不设置时默认 `465`。 |
| `CRISIS_ALERT_EMAIL_SMTP_SECURITY` | 可选 SMTP 加密方式：`ssl`、`starttls` 或 `none`。不设置时默认 `ssl`。 |
| `CRISIS_ALERT_TELEGRAM_CHAT_IDS` | 危机告警专用 Telegram chat ID，和常规策略周期 Telegram 分开。 |
| `CRISIS_ALERT_TELEGRAM_BOT_TOKEN` | 危机告警专用 Telegram bot token。Cloud Run env sync 建议配置 `CRISIS_ALERT_TELEGRAM_BOT_TOKEN_SECRET_NAME`。 |
| `SCHWAB_MIN_RESERVED_CASH_USD` | 可选的平台保留现金下限，单位 USD。默认策略是：不设置固定 USD 下限，仅保留 `总资产 3%`。运行时公式：`max(下限, 比例 * 总资产)`。 |
| `SCHWAB_RESERVED_CASH_RATIO` | 可选的平台保留现金比例。默认策略是：不设置固定 USD 下限，仅保留 `总资产 3%`。运行时公式：`max(下限, 比例 * 总资产)`。 |
| `SCHWAB_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD` | `BOXX`/`BIL` 等避险现金替代标的目标金额低于该 USD 门槛时保留现金，不买入。默认 `1000`。 |
| `INCOME_THRESHOLD_USD` | 可选的收入层启动阈值覆盖。不填时使用 `UsEquityStrategies` 的实盘默认值，也就是普通账户规模下关闭收入层。 |
| `QQQI_INCOME_RATIO` | 可选的 QQQI 收入层占比覆盖，0–1。只有启用收入层时才有意义。 |
| `DUAL_DRIVE_UNLEVERED_SYMBOL` | 可选的 `tqqq_growth_income` 非杠杆增长袖子交易标的覆盖。不填时使用 `QQQ`；小账户可以设置为 `QQQM`，但主信号仍使用 `QQQ`。 |
| `NOTIFY_LANG` | 通知语言: `en`（英文，默认）或 `zh`（中文） |

策略插件挂载 JSON 属于平台/部署配置，不属于策略代码。它只决定当前 runtime 读取哪些插件 artifact，不能在挂载里设置 `mode`；插件 artifact 自带身份和有效 mode。插件挂载配置错误会写入 runtime report diagnostics，不阻断基础策略运行周期。

如果挂载了 `crisis_response_shadow` 插件，常规策略周期 Telegram 仍会包含插件摘要行。当插件信号升级到非 `no_action`（例如 `canonical_route=true_crisis`、`suggested_action=defend`/`blocked`，或 `would_trade_if_enabled=true`）时，服务还会按 `CRISIS_ALERT_CHANNELS` 配置额外发送独立危机通知：`email`、`sms`、`push` 和/或 `telegram`。
告警结果会写入 runtime report。重复发送抑制使用稳定的插件告警 key；如配置了 `STRATEGY_PLUGIN_ALERT_STATE_GCS_URI` 则写入该前缀，否则复用 `EXECUTION_REPORT_GCS_URI`，并有本地 `/tmp` marker fallback。

如果你在多个 quant 仓库之间保留一层共享配置，通常只建议共享 `GLOBAL_TELEGRAM_CHAT_ID`、`NOTIFY_LANG`、`CRISIS_ALERT_CHANNELS`，以及同一套危机告警策略下的 `CRISIS_ALERT_EMAIL_*`/`CRISIS_ALERT_PUSH_*`。`TELEGRAM_TOKEN`、Schwab API key 这些仍然应该由这个仓库自己管理；告警 token 和密码也应该留在 GitHub Secret 或 GCP Secret Manager。

Schwab OAuth token payload 当前从 Secret Manager 的 `schwab_token` 里读取。

建议在 `charlesschwabquant` 项目里同时维护这些运行时 secret：

- `schwab_token`
- `charles-schwab-api-key`
- `charles-schwab-app-secret`
- `charles-schwab-telegram-token`

### GitHub 统一管理 Cloud Run 部署和环境变量

这个仓库提供 `.github/workflows/sync-cloud-run-env.yml` 作为 GitHub 管理 Cloud Run 的入口。设置 `ENABLE_GITHUB_CLOUD_RUN_DEPLOY=true` 时，GitHub Actions 会构建并发布容器镜像；设置 `ENABLE_GITHUB_ENV_SYNC=true` 时，GitHub Actions 会同步运行时环境变量；设置 `ENABLE_MAIN_PUSH_CLOUD_RUN_AUTOMATION=true` 时，`main` 分支 push 才会触发同一套自动化。迁移期间这些开关可以独立启用，旧的 Google Cloud Trigger 可以先保留。这个 workflow 现在也会发出 `RUNTIME_TARGET_JSON`，让控制面带上结构化运行目标，而不是只看 `STRATEGY_PROFILE`。

推荐配置方式：

- **仓库级 Variables**
  - `ENABLE_GITHUB_CLOUD_RUN_DEPLOY` = `true`（让 GitHub Actions 负责 build/push/deploy）
  - `ENABLE_GITHUB_ENV_SYNC` = `true`
  - `ENABLE_MAIN_PUSH_CLOUD_RUN_AUTOMATION` = `true`（允许 `main` push 触发 deploy/env-sync workflow；手动 `workflow_dispatch` 不要求这个开关）
  - `CLOUD_RUN_REGION`
  - `CLOUD_RUN_SERVICE`
  - 可选：`GCP_ARTIFACT_REGISTRY_HOSTNAME`（Artifact Registry 不在 Cloud Run region 时才需要；默认 `<CLOUD_RUN_REGION>-docker.pkg.dev`）
  - `TELEGRAM_TOKEN_SECRET_NAME`（建议：`charles-schwab-telegram-token`）
  - `SCHWAB_API_KEY_SECRET_NAME`（建议：`charles-schwab-api-key`）
  - `SCHWAB_APP_SECRET_SECRET_NAME`（建议：`charles-schwab-app-secret`）
  - `STRATEGY_PROFILE`（显式设置为任一已启用 profile：`global_etf_rotation`、`mega_cap_leader_rotation_top50_balanced`、`russell_1000_multi_factor_defensive`、`tqqq_growth_income`、`soxl_soxx_trend_income` 或 `tech_communication_pullback_enhancement`）
  - 可选：`SCHWAB_FEATURE_SNAPSHOT_PATH`、`SCHWAB_FEATURE_SNAPSHOT_MANIFEST_PATH`、`SCHWAB_STRATEGY_CONFIG_PATH`，用于 feature-snapshot 策略
  - 可选：`SCHWAB_STRATEGY_PLUGIN_MOUNTS_JSON`，用于策略插件 artifact 挂载。不要在这个平台挂载 JSON 里放 `mode`
  - 可选：`SCHWAB_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD`，用于平台层现金/BOXX 小额门槛
  - 可选：`INCOME_THRESHOLD_USD`（仅策略 override）
  - 可选：`QQQI_INCOME_RATIO`（仅策略 override）
  - 可选：`DUAL_DRIVE_UNLEVERED_SYMBOL`（仅策略 override）
  - 可选：`CRISIS_ALERT_EMAIL_RECIPIENTS`、`CRISIS_ALERT_EMAIL_SENDER_EMAIL`、`CRISIS_ALERT_EMAIL_SENDER_PASSWORD_SECRET_NAME`
  - 可选：`CRISIS_ALERT_EMAIL_SMTP_HOST`、`CRISIS_ALERT_EMAIL_SMTP_PORT`、`CRISIS_ALERT_EMAIL_SMTP_SECURITY`
  - 可选：`GOOGLE_CLOUD_PROJECT`
- **仓库级 Secrets**
  - 仅保留为 fallback：`TELEGRAM_TOKEN`
  - 仅保留为 fallback：`SCHWAB_API_KEY`
  - 仅保留为 fallback：`SCHWAB_APP_SECRET`
  - 仅保留为 fallback：`CRISIS_ALERT_EMAIL_SENDER_PASSWORD`
- **已支持的共享 Variables**
  - `GLOBAL_TELEGRAM_CHAT_ID`
  - `NOTIFY_LANG`
  - `CRISIS_ALERT_EMAIL_RECIPIENTS`
  - `CRISIS_ALERT_EMAIL_SENDER_EMAIL`
  - `CRISIS_ALERT_EMAIL_SENDER_PASSWORD_SECRET_NAME`
  - 可选 SMTP 覆盖：`CRISIS_ALERT_EMAIL_SMTP_HOST`、`CRISIS_ALERT_EMAIL_SMTP_PORT`、`CRISIS_ALERT_EMAIL_SMTP_SECURITY`

每次 push 到 `main` 时，这个 workflow 可以构建并部署配置的 Cloud Run 服务，把上面这些值同步到服务里，并删除旧的 `TELEGRAM_CHAT_ID`。

注意：

- 只有在 `ENABLE_GITHUB_ENV_SYNC=true` 时，这个 workflow 才会严格校验并执行同步。没打开时会直接跳过，不影响原来 Google Cloud Trigger + 手工 Cloud Run env 的老流程。打开后，它会通过 `scripts/print_strategy_profile_status.py --json` 动态解析目标策略需要的 snapshot/config 输入，不再维护硬编码策略名列表。
- 只有在 `ENABLE_MAIN_PUSH_CLOUD_RUN_AUTOMATION=true` 时，push 到 `main` 才会触发自动化；手动 `workflow_dispatch` 继续只受部署/同步开关控制。
- 只有在 `ENABLE_GITHUB_CLOUD_RUN_DEPLOY=true` 时，GitHub Actions 才会接管代码部署；没打开时，旧的 Cloud Build trigger 仍可继续负责发布。
- `STRATEGY_PROFILE` 现在由平台能力矩阵和从 `runtime_enabled` 策略元数据派生的 rollout allowlist 一起决定。当前 `enabled` 包含 6 条 live `us_equity` 策略：`global_etf_rotation`、`mega_cap_leader_rotation_top50_balanced`、`russell_1000_multi_factor_defensive`、`tqqq_growth_income`、`soxl_soxx_trend_income` 和 `tech_communication_pullback_enhancement`；research-only 存档 profile 仍保留能力矩阵兼容性，但不会启用。`RUNTIME_TARGET_JSON` 则表示实际运行目标，`STRATEGY_PROFILE` 继续只负责兼容选择策略实现。
- 当前策略域是 `us_equity`，本地策略注册表只用于域和 profile 校验；结构化运行目标会通过 `RUNTIME_TARGET_JSON` 往下传。
- `INCOME_THRESHOLD_USD`、`QQQI_INCOME_RATIO` 和 `DUAL_DRIVE_UNLEVERED_SYMBOL` 在 env-sync 里是可选 override，不是平台默认值来源。不填时会继承 `UsEquityStrategies` 的 profile 默认值；当前 `tqqq_growth_income` 实盘默认是不带收入层的 QQQ/TQQQ 双轮模式。只有在明确要用 QQQM 替代整股 QQQ 时，才设置 `DUAL_DRIVE_UNLEVERED_SYMBOL=QQQM`。
- GitHub 现在通过 OIDC + Workload Identity Federation 登录 Google Cloud，这个 workflow 不再需要 `GCP_SA_KEY`。
- GitHub 部署路径使用仓库里的 Dockerfile 和 Artifact Registry。部署服务账号需要 Artifact Registry 写入、Cloud Run 管理，以及对 runtime service account 的 service-account user 权限。
- Telegram token 和 Schwab API 凭据建议放到 Secret Manager，并通过上面的 secret-name 变量引用。对多个 quant 仓库来说，真正适合跨项目共享的是低耦合配置，例如 `GLOBAL_TELEGRAM_CHAT_ID`、`NOTIFY_LANG`，以及通用的 `CRISIS_ALERT_EMAIL_*` 危机邮件契约。

### Runtime Guard 告警

`.github/workflows/runtime-guard.yml` 是 Schwab Flask handler 之外的第二层通知。它只读取
Cloud Logging 中最近的 Cloud Scheduler 错误和 Cloud Run 请求/运行失败，然后直接通过
`CRISIS_ALERT_TELEGRAM_BOT_TOKEN` + `CRISIS_ALERT_TELEGRAM_CHAT_IDS` 或 fallback 的
`TELEGRAM_TOKEN` + `GLOBAL_TELEGRAM_CHAT_ID` 发 Telegram。

这个 guard 不会调用 Cloud Run 的交易路由，主要覆盖 Scheduler 没打到服务、
OIDC/IAM/audience 配错、Cloud Run 返回 4xx/5xx、或容器在 app-level Telegram fallback
执行前就失败的情况。

需要的配置：

- `CLOUD_RUN_SERVICE` 或 `RUNTIME_GUARD_CLOUD_RUN_SERVICES` 中有要监控的服务名
- GitHub deploy service account 需要 `charlesschwabquant` 项目级 `roles/logging.viewer`
- GitHub 中继续配置 Telegram chat/token 变量或 secrets
- 可选设置 `RUNTIME_GUARD_SCHEDULER_JOB_PATTERN`，用正则把 Scheduler 日志限制到本部署的 job

默认计划每 30 分钟检查一次。若要做 missed-run 心跳，设置
`RUNTIME_GUARD_REQUIRE_SUCCESS=true`，并把 `RUNTIME_GUARD_LOOKBACK_MINUTES` 设成覆盖预期
Scheduler 运行时间的窗口。默认不强制心跳，避免非交易窗口误报。

更严格的完成检查是 `Execution Report Heartbeat`
（`.github/workflows/execution-report-heartbeat.yml`）。它会在工作日预期市场窗口后检查
`EXECUTION_REPORT_GCS_URI` 下最近的 runtime report JSON，读取 `status/stage/errors`，
如果没有近期 report 或 report 状态为 `error` 等失败状态就发 Telegram。GitHub deploy
service account 需要对 report bucket 有对象读取/列举权限。

### 部署单元和命名建议

- `QuantPlatformKit` 只是共享依赖，不单独部署；Cloud Run 继续只部署 `CharlesSchwabPlatform`。
- 推荐 Cloud Run 服务名：`charles-schwab-quant-service`。
- 如果后面改 GitHub 仓库名或再次迁组织，Cloud Build / Cloud Run 里的 GitHub 来源需要重新选择，不要假设旧绑定会自动跟过去。
- 统一部署模型和触发器迁移清单见 [`QuantPlatformKit/docs/deployment_model.md`](../QuantPlatformKit/docs/deployment_model.md)。

部署为 Cloud Run 服务，按策略层 cadence 配三次定时触发：开盘后 15 分钟用 `"/precheck"` 做预检，开盘后 30 分钟用 `"/probe"` 做连接探针，临近收盘用 `"/"` 做正式执行。Scheduler 的 OIDC audience 要指向 Cloud Run 服务根 URL，不要拼到 `"/probe"` 或 `"/precheck"`。入口：`main.py` 中的 Flask 路由 `"/precheck"`、`"/probe"` 和 `"/"`。
