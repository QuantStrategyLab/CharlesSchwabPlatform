# CharlesSchwabPlatform

CharlesSchwabPlatform 是 QuantStrategyLab 多仓库交易系统里真正和 Charles Schwab 经纪商 API 打交道的执行 runtime。策略逻辑本身在别处——`UsEquityStrategies` 和 `UsEquitySnapshotPipelines`；这个仓库负责把 runtime-enabled 的策略 profile 接到 Schwab 相关的具体环节：认证、下单、dry-run/live 控制、通知，以及 Cloud Run 部署。如果你要处理 Schwab 连接、执行安全机制，或者这个平台怎么部署和运维，就是在这个仓库里。

[English README](README.md)

> 投资有风险。本项目不构成投资建议，仅用于学习、研究和工程审阅。

## 架构角色

- **层级**：`执行平台`。
- **职责**：Charles Schwab 美股执行运行时。
- **事实源/归属**：Schwab 连接、token/runtime 集成、dry-run/live 控制。
- **消费对象**：UsEquityStrategies、UsEquitySnapshotPipelines artifacts、QuantPlatformKit、QuantRuntimeSettings。
- **禁止事项**：承载策略研究逻辑或把凭据写入 Git。

实盘执行必须先取得持久化原子 claim，失败时关闭执行。Cloud Run 的请求并发数和最大实例数都必须保持为 `1`；原子 claim 负责阻止新旧 revision 重叠时重复向券商提交。未完成的 claim 不会自动过期，必须先核对订单和执行报告，再由人工恢复。

## 运行边界

- 只加载策略包暴露的 runtime-enabled profile。
- 负责券商/API 连接、dry-run 检查、通知和部署配置。
- 凭据必须放在 GitHub Secrets、云密钥系统或券商专用密钥系统中，不能提交到 Git。
- 任何 live 下单路径启用前，都应先从 dry-run 或 paper mode 开始。

## 普通 profile 与 snapshot-backed profile

普通 runtime profile 通常可以直接基于 market history 或 portfolio state 执行。Snapshot-backed profile 需要先从对应 snapshot pipeline 获取当前 artifact bundle，平台才应该执行。平台不应该自行判断策略资格，而应消费策略仓和 snapshot 仓发布的状态与产物。

## 安全部署顺序

1. 在 Git 之外配置 secrets 和 runtime variables。
2. 先以 dry-run 模式运行 workflow 或服务。
3. 检查生成订单、日志、通知和 reconciliation 输出。
4. 确认回滚步骤和 artifact 版本。
5. 上述检查清楚后，再启用定时任务或 live 执行。

## 仓库结构

- `tests/`：单元测试、契约测试和回归测试。
- `.github/workflows/`：CI、定时任务、发布或部署 workflow。
- `scripts/`：运维脚本和本地辅助工具。
- `research/`：研究配置和非 live 候选产物。

## 快速开始

```bash
uv sync --frozen --extra test
uv run --no-sync ruff check --exclude external .
uv run --no-sync python scripts/check_qpk_pin_consistency.py
```

## 延伸文档

- 暂无独立 `docs/` 目录；请先阅读本 README 和 workflow 文件。

## 社区和安全

- 贡献前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)，确认 PR 范围、本地校验和文档要求。
- 讨论、issue 和 review 请遵守 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。
- 涉及密钥、自动化、券商/交易所或云资源的漏洞请按 [SECURITY.md](SECURITY.md) 私密报告；不要为 secret 或实盘风险开公开 issue。

## 许可证

详见 [LICENSE](LICENSE)。

## 只读日报 caller 候选

`scripts/publish_runtime_daily_from_reports.py` 只为 `charles-schwab-quant-service / soxl_soxx_trend_income / live` 准备隐私安全的日报投影。身份必须独立来自唯一的现有 runtime policy 及 `RUNTIME_TARGET_JSON.runtime_risk_limits.binding.account_hash`；只有与该 hash 逐字相等的报告才能作为投影输入，既有 account-facts binding ID 从独立身份派生，不能由报告自己证明预期账户。

对于通过非账户资格门的报告，身份跳过分支细分为：缺 observation 的 `source_observation_missing`、缺 hash 键的 `source_hash_missing`、hash 已提供但为 null/非字符串/空白/带首尾空白的 `source_identity_invalid_shape`。只有合法非空、无首尾空白的字符串与独立 hash 逐字不同，才记 `source_identity_mismatch`；不 trim、不忽略大小写、不从其他别名补身份。非对象 payload/summary/observation 仍保留原有坏报告排除及 incomplete 行为。此前通用 mismatch 不能证明实际属于哪类。producer 的 observation 本来可缺省，跳过、错误、旧路径或观察投影未成立都可能不写入；仅凭缺字段不能断言账户配置错误。

既有 schema/platform/scope/current-revision/URI/run-id/time/大小资格门保持原判定，先于报告身份比较执行。本就不合格的报告继续排除并保留 read_error/incomplete，不能仅因身份不同或缺失阻塞其它合格报告；不扩大任何报告的准入集合。合格报告的身份失败无论排列顺序都仍整批停止。旧业务日不是豁免：通过既有时间与当前 revision 谓词的报告仍必须逐字匹配独立账户。

manual runner 只读已封存投影的两个有界字段：白名单 `daily_status` 和现有 runs 列表长度 `projected_run_count`（整数0–20），不新增第二套准入元数据。全排除明确为 `daily_status=read_incomplete`、`projected_run_count=0`、runs为空、read_errors含 `coverage_unconfirmed`、coverage incomplete、`fills.count=null`。`status=prepared` 仅表示制备结束；零投影run不代表零交易、完整历史或健康状态。摘要不改变投影、seal、来源身份或发布行为。

在该 exact mismatch 已经终止 prepare 后，manual runner 可附加四个固定整数诊断，只看同一已读内存批次的前20项：`mismatch_provenance_passed`、`mismatch_provenance_failed`、`mismatch_provenance_unknown`、`mismatch_passed_ascii_case_only`。前三项只统计合法非空 mismatch 对原非账户 scope/current-revision/URI/time/大小门的通过、不通过或无法判断；第四项只在通过组中统计“原字符串不同、均为ASCII、转小写的副本相同”。这是语法诊断，不代表券商官方身份等价、准入通过或允许规范化 hash/binding digest。原生券商账户 hash、账户响应摘要和复合 source-binding digest 仍是不同字段。

原 reason、skipped、exit2、无 projection/POST 均不变，不增加来源请求。诊断上下文缺失或计数非法时不附计数，不伪造零结果；各计数0–20，前三项合计最多20。这些数字不能推断未读archive，截断/读错事实和 coverage incomplete 保持不变；通过既有时间条件也不代表报告属于当前业务日。不输出任何单项 hash、长度、指纹、账户、URI、时间、revision 值或自由异常。

仅当已校验 seal 的投影 runs 为零时，runner 可从同一次已读内存批次附加固定 `zero_run_*` 诊断。`zero_run_entries` 是0–20条已解码输入的数量，不是尝试读取的对象数或接纳数；`zero_run_read_failed`、`zero_run_truncated` 保留 reader 原布尔值。十个互斥首失败计数的后缀为 `uri_invalid`、`time_invalid`、`schema_invalid`、`scope_invalid`、`revision_mismatch`、`path_mismatch`、`time_order_invalid`、`size_invalid`、`unevaluable`、`provenance_passed`，遵循原 URI、时间解析、schema/scope、revision、路径、时间顺序、size 检查次序，合计恰等于输入数。异常或无法安全判断的形状保持 unevaluable；解码前已被 reader 丢弃的对象不在计数内，read_failed 不能还原其具体原因。

`zero_run_other_business_date` 只统计既有 sealed projection 中固定的跨业务日排除原因。非账户门通过后仍可能因 observation 容器异常被丢弃，或在 exact 身份检查通过后因旧业务日被投影排除；`provenance_passed` 因此不是接纳数，零 runs 也不证明所有报告都未过来源门。合格旧日报告的身份失败仍拒绝整个批次。该诊断不判断或归一化账户身份；上下文、批次上限、计数或 seal 非法时不附诊断，不伪造零值。不增加来源读取、prefix 扫描、Cloud Run 环境读取、凭据访问、workflow 变更或发布。计数不能证明 producer bucket/prefix 一致或 archive 完整，`coverage_unconfirmed`、incomplete 与 null fills 保持不变；只输出固定布尔及有界整数，不含路径、hash、账户、run ID、日期、revision 或自由异常。

准备与读取分离：默认命令只准备空的 incomplete 投影，不联网、不发布。可调用的有界归档 reader 复用既有 URI 合同，最多读取 20 份报告并限制字节；latest-only、截断列表都不代表全天完整。先前未决报告的覆盖和保留范围尚未核验，首版即使枚举耗尽也始终 incomplete。schedule 复用既有纯 scheduler/calendar policy，仅映射已证实成熟的当前业务日 due，其余保持 unevaluable。

发布须单独显式调用函数，只用 `EXECUTION_EVIDENCE_SYNC_TOKEN`、固定 HTTPS QRS 日报地址和 `X-QSL-Source-Binding-ID`。禁止 redirect，单次请求，有界验证 stored ACK，不自动重试，不回退到账户事实或 dispatch token。命令不发布、不调用 heartbeat/account-facts main，只输出固定 reason code；原报告、hash、source ID、URI、金额、订单、凭据及自由异常文本不得进入日志或共享产物。

此源码候选不启用任何 workflow。另行批准切换前，仍须在原云权限内核验现有注入的独立身份、receiver registry/header gate、精确报告前缀和已有 IAM、serving revision、实际 scheduler、全天与先前未决覆盖以及既有 endpoint/token 引用。真实投影预检、stored ACK、登录态账户/日期回读、生产页面和自然周期各自验收；合成测试不能证明真实连通。

ACK 的 account_key 是可信 receiver 返回的 canonical UI 别名，不代表 caller 独立核验了该别名。物理账户归属依赖 caller 对独立 hash 的精确比较，以及 receiver 每次 POST 对当前 protected binding 的匹配。合法响应仅标 `stored_acknowledged` / `receiver_reported_account`，仍须登录态账户/日期回读；已有可信预期 UI key 时可额外比较，但不新增重复别名变量或权限。复用现有 `RUNTIME_HEARTBEAT_PUBLICATION_GRACE_MINUTES`，错误值保持 schedule unevaluable。

准备时还会固定 canonical body 与 source identity 的摘要。打开任何 transport 前，发布函数拒绝准备后内容或身份的改变，也拒绝没有准备证据的手工构造结果；请求和 ACK 预期都使用核验后的同一字节快照。这是进程内防止误变的约束，不是对掌握 Python 代码的恶意调用者的安全边界，receiver 校验仍然必需。

### 手动接线与真实验收分离

`runtime-daily-sync.yml` 是独立的 main-only 手动 workflow，合并源码不会触发云读取或日报 POST。原 account-facts 的必填输入、默认行为和 heartbeat 定时任务均不变。新入口默认只 prepare，仅显式 typed `publish` 输入才增加一次发布尝试；prepare 步骤不注入发布 token。两种模式复用已有 WIF 身份与锁定依赖，不新建 environment 门、凭据或 IAM 授权，不部署、不调用券商。

明确运行后，`scripts/run_runtime_daily_from_reports.py` 通过有界只读 metadata adapter 核对固定服务实际 serving traffic 和既有 scheduler 候选，再复用已合并的有界报告 reader 与 caller。只接受稳定、唯一且总计 100% 流量的 revision。scheduler 必须唯一、enabled，且指向同一服务的既定 `/run` 路径；只比较该 URI，绝不调用。暂停、歧义、目标不符或无法核验均保持 `unevaluable`，声明 cron 或 TTL 不能替代实际事实。原 caller CLI 继续离线。

首次真实 prepare 仍是原云权限边界内的独立验收步骤。报告 prefix、身份/hash、私有 metadata、原报告和自由异常文本不进入共享输出；日报 endpoint 仅在新发布步骤局部使用，不修改旧 execution-evidence URL。只输出固定分类：`prepared` 或 `stored_acknowledged` 加 `completeness=incomplete` 不代表全天健康或登录态回读通过。coverage 仍无条件 false；当前业务日截至观察点的完整枚举、保留的先前未决历史及终态证据、真实 due obligations 均需另行证明。没有人工 complete 开关，枚举耗尽、空目录或少于20份报告都不能提供该证明。
