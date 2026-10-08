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

- 日报步骤优先使用受保护 Secret `SCHWAB_RUNTIME_DAILY_TARGET_JSON`；未配置时回退到
  原 `vars.RUNTIME_TARGET_JSON` 或 `secrets.RUNTIME_TARGET_JSON`。该 Secret 应为原私有
  `RUNTIME_TARGET_JSON` 的精确副本，只将 `binding.account_hash` 更新为经原应用和用户确认的
  native account hash。不要用历史 alias 或未经核实的报告字段生成它。手动日报 workflow 在
  job 范围采用此优先级；计划心跳只在日报 publisher step 覆盖目标，其他心跳、报告与风险
  步骤继续使用原 `RUNTIME_TARGET_JSON`。
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

## 既有 Schwab Secret 的只读前置诊断

`runtime-daily-sync.yml` 另有默认关闭的 `diagnose_source_access` 手动输入，与
`publish` 互斥。选择该项只用 workflow 原有 ADC 检查 QPK runtime 实际读取的
`charlesschwabquant/schwab_token` Secret 元数据、`latest` 版本是否启用，以及
对该 Secret 调用 `testIamPermissions`，观察响应是否列出
`secretmanager.versions.access`；请求不读取 Secret payload，
不访问 Schwab、不刷新 token，也不修改 IAM。固定输出不包含资源名、错误正文或凭据。

权限字段名为 `permission_reported`，仅表示该 API 响应报告了该权限。Google 明确说明
`testIamPermissions` 可能 fail-open，不能用来证明 payload 实际可访问。诊断只证明
Secret 元数据和版本状态可读，并记录权限观察；不证明 token 新鲜、账户身份匹配或
session 可安全使用。此 metadata 诊断是可选辅助项；身份核验不要求 Secret Manager
metadata Viewer，也不以 `testIamPermissions` 的观察结果代替真实读取。

### 原生账户身份核验与恢复

`verify_native_identity` 是另一个默认关闭的手动输入，与 `publish` 和
`diagnose_source_access` 互斥。它使用既有云端身份在内存中读取 `schwab_token`，不输出
token 或原生账户号/hash，也不刷新或写回 token；随后只调用一次有界的 Schwab 原生账户号
只读接口，并将结果与受保护运行目标中预先配置的身份精确比较。日报 report 自己携带的
身份字段不能证明预期身份。此模式始终进行实时原生账户核验，即使日报发布 Secret 已配置。

`publish` 有两种受控身份路径：配置了 `SCHWAB_RUNTIME_DAILY_TARGET_JSON` 时，只在日报发布
步骤向 runner 注入该 Secret；runner 要求它与当前完整 `RUNTIME_TARGET_JSON` 完全一致，且
目标和绑定身份均通过现有校验。此时将用户确认的受保护绑定作为稳定的预期身份，不请求
Schwab 原生账户接口；仍会核验归档报告的目标、原生 hash、来源与时间、当前运行 revision，
并保留 source-binding 和 receiver ACK 校验。这个绑定是稳定配置依据，不是发布时对券商
当前账户身份的动态核验。缺少该 Secret 时，发布沿用原行为：先调用有界的原生账户号只读
接口；配置不匹配或格式无效时，在读取报告和 POST 前停止。只有受保护配置由原应用及用户
确认后才能设置，不能由公开布尔、历史 alias 或报告自身生成。该 Secret 不改变券商凭据、
交易权限、运行目标或风险配置。

旧实时核验路径遇到 `token_expired`、身份不匹配、多重匹配、响应无效或身份读取不可用时，
会停止发布。用稳定受保护绑定的发布路径不读取 Schwab token，因此不受 access token 到期
影响；`check_token_load` 和 `verify_native_identity` 的实际 token 校验行为不变。本 workflow
不会自动刷新或写回 token。其他固定读取原因按类别检查既有云身份、payload access 权限、
Secret/版本状态或服务可用性；无需为此增加 metadata Viewer。不要下载、打印或粘贴 token 内容。

### 只读检查 token 加载状态

默认关闭的 `check_token_load` 手动输入与制备、`publish`、metadata 诊断和原生身份核验
互斥。它只通过现有云端身份读取一次 `schwab_token` payload，在内存中校验结构和到期时间，
不调用 Schwab、不读取日报、不发送 POST，也不刷新或写回 token。读取请求有固定超时并关闭
SDK 自动重试；输出仅为固定状态码，不含异常文本、资源路径或 token 内容。

固定原因 `token_dependency_unavailable`、`token_adc_unavailable`、
`token_permission_denied`、`token_not_found`、`token_version_unavailable`、
`token_network_unavailable`、`token_load_failed` 按底层异常类型区分依赖不可用、ADC 不可用、
payload 权限拒绝、资源未找到、版本不可用、网络不可用及未知读取失败。
`token_permission_denied` 只对应 payload access 调用的权限异常；
metadata 检查中的 403 或权限观察不能推出此结论。`token_expired` 表示缓存 payload 的结构可读，
但其中 token 已过期；由既有获批的 Schwab token 负责人流程重新授权/轮换后，再单独运行
`check_token_load`。`token_payload_valid` 只证明 payload 可读取且格式/有效期通过，不证明 Schwab
接受该 token，也不证明原生账户身份匹配。需要确认身份时运行 `verify_native_identity`；
`publish` 仍会在读日报和 POST 之前自行执行身份核验。

## 未验证

- 本改动只做本地合成/工作流静态验证；未执行任何真实 GCS/HTTP/QRS/Telegram/券商调用，
  未启用开关，未 dispatch、未部署。
- 真实日报发布、ACK 与登录页面归属仍须在获准执行端由 Codex 核验。
