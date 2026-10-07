# Schwab 日报心跳接线（默认关闭）

2026-10-08。`execution-report-heartbeat.yml` 的 `heartbeat` job 新增一个默认关闭的
daily publisher step，在既有 22:20 UTC 调度内复用同一 WIF/uv 流程和已受保护配置。
它不新增 cron，不改原交易 Scheduler、heartbeat 通知行为或账户配置。

位置保留在原 heartbeat/evidence 之后。条件显式使用 `!cancelled()`，并要求
`install_runtime_deps.outcome == 'success'` 和本次 WIF 成功。因此原健康检查
或证据同步失败仍可记录日报；认证、依赖安装失败或运行取消不发布。日报的失败
不会跳过已经执行的原健康通知，原步骤顺序与行为不变。

GitHub默认条件语义：[官方表达式说明](https://docs.github.com/en/actions/reference/workflows-and-actions/expressions#status-check-functions)。

## 启用

- 只有当仓库变量 `RUNTIME_DAILY_SYNC_ENABLED` 精确等于字符串 `true` 时才执行；
  缺失、`false`、`True` 等都不执行。
- 只有在本次运行中某个认证步骤明确成功
  （`steps.gcp_auth_primary.outcome == 'success'` 或
  `steps.gcp_auth_retry.outcome == 'success'`）时才执行。
- 命令：`uv run --no-sync python scripts/run_runtime_daily_from_reports.py --publish`。
- 关闭时（默认）该 step 被跳过，原 heartbeat 与 execution-evidence 行为不变。

已受保护来源（沿用 `runtime-daily-sync.yml` 同名值）：

- job 级已有：`RUNTIME_TARGET_JSON`、`CLOUD_RUN_SERVICE_TARGETS_JSON`、
  `CLOUD_RUN_SERVICE(_S)`、`RUNTIME_HEARTBEAT_MARKET_*`、
  `RUNTIME_HEARTBEAT_PUBLICATION_GRACE_MINUTES`、
  `RUNTIME_HEARTBEAT_SCHEDULER_LOCATION`、`CLOUD_SCHEDULER_MAIN_TIME`、
  `GCP_PROJECT_ID`。
- step 级新增：`SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX`（`secrets`）、
  `GCP_REGION: us-central1`（与 `publish_account_facts_from_reports.REGION` 一致）。

密钥边界：`EXECUTION_EVIDENCE_SYNC_TOKEN` 只在 publisher step 注入，不下放到 job；
`EXECUTION_EVIDENCE_SYNC_URL` 由 runner 内部设为原 `SYNC_URL`，workflow 不提供。
不猜造账户/报告 prefix，不扩 IAM，不新增依赖。

## ACK 语义

`--publish` 走已有 publisher：一次 POST 到原 QRS `/api/runtime-daily/sync`，校验固定
ACK 键集合 `{ok, stored, platform, target_key, business_date, account_key}`。成功时
runner 状态为 `stored_acknowledged`、`account_attribution=receiver_reported_account`。

这只证明**接收端报告的存储归属**，不证明登录页面已展示该记录（`GET /api/runtime-daily`
需登录会话）。页面读回仍由 Codex 通过正常登录会话另行验收。不把私有 `account_key`
写入日志或 artifact；stdout 仅固定状态/布尔。

## 未验证

- 本改动只做本地合成/工作流静态验证；未执行任何真实 GCS/HTTP/QRS/Telegram/券商调用，
  未启用开关，未 dispatch、未部署。
- 真实日报发布、ACK 与登录页面归属仍须在获准执行端由 Codex 核验。
