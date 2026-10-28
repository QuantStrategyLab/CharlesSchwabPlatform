# Schwab 试点状态只读核验

手动运行 `Schwab Pilot State Readback`，历史报告检查 `inspect_historical_outcome` 默认关闭。只在原仓库 `main`、首次尝试执行；不自动重跑。调用独立的受保护 workflow，沿用 metadata/native identity 包的原 Variable 优先、Secret 回退、原 WIF 与云身份来源，不复制项目、服务、账户或存储映射。原声明仅在一个操作步骤中以 Secret 环境变量消费。它不调用券商、不读 token、不刷新、不写 Secret、不修改配置或 claim、不解除暂停、不归档、不发通知。

程序从 Cloud Run 服务选唯一 100% serving revision；校对六项身份及 actual-serving 风险绑定账户 hash 与原声明相等，存储操作前后再校对 serving revision。账本读取使用该原生 hash，而不是控制面的 `account_scope`。`PAUSED` 原样保留；只报告结构化原因字段是否存在，不把字段存在或账本内容解释成暂停原因。

存储位置只按 actual-serving 环境中原 QPK 工厂的优先级选择：`SCHWAB_EXECUTION_STATE_CLOUD_URI` / 旧 GCS 名称，其次 `QSL_EXECUTION_REPORT_CLOUD_URI` / 旧 GCS 名称，再次 `EXECUTION_REPORT_CLOUD_URI` / 旧 GCS 名称。新名称即使为空也不会退回对应旧名称。没有可用位置、选中 Secret 引用、非 GCP/非 GCS 或读取失败时明确结束；不读取 Secret 值、不猜 bucket、不读取 Mac 本地状态。工厂关闭本地 fallback。

输出 artifact `schwab-pilot-state-safe` 仅包含固定状态、布尔、revision SHA-256、owner 是否存在及是否匹配当前策略、账本摘要和下述结构计数。owner 不存在只有在远端 exists 明确返回 false 时成立；对象存在但不可读、消失或结构错误仍为未知。owner 相同允许多个调用者，不能证明 OAuth grant 排他。依赖的标准输出、错误输出和 Python 日志在读存储期间被捕获；不输出原账户、金额、URI、marker、业务日期、原因自由文本或异常正文。

账本固定复用当前冻结 QPK `2d6626f2ae2852cd74c0ac050b62968a4e06d074` 的 `calculate_recent_ledger_digest`、私有 `_recent_ledger_entries` 及同版摘要格式：前摘要、分类窗口、后摘要三轮读取，每轮 claims/outcomes 两个命名空间各取按键排序最近至多 256 个对象正文。`body_record_limit_per_namespace=256` 是每轮上限，`body_read_passes=3` 说明三轮；不是全局只读 512 个正文。前缀列举仍可能列出全部对象，SDK 沿用原只读重试。没有额外重试循环，也没有全局请求时限硬保证；workflow 的 10 分钟是外层停止限制。三份摘要与条数都相等才称观察窗口稳定；这不是 CAS、排他锁或全历史证明。

分类窗口只在内存按 `marker_key` 配对，输出 claims/outcomes 对象数、配对数、窗口内缺对方记录数、明确 UNKNOWN、待对账、记录无待处理、无法判定和无效记录数。缺 outcome 仅指本次最近窗口没有配对，不证明全历史不存在。

状态判断沿本仓 `_recorded_execution_replay`：`pending_reconciliation` 且有 `submitted_orders.status == "unknown"` 才计明确 UNKNOWN；其他结构有效的 `pending_reconciliation` 计待对账。二者计数互斥。只有原合同认可的 `completed`/`no_action`/`no_op` 且 pending count 为 0、orders 为空，才计“记录无待处理”，不证明券商订单终态。其他状态保留无法判定；不新增终态映射。结构、身份、日期与键不一致或 JSON 无效时计无效，不能把其余有效子集的配对/UNKNOWN 数冒称完整总数，这些字段保持 null。

只有窗口稳定、每条结构有效且状态均能按原合同分类时，分类为 observed；无效、无法判定、读失败或变化均保留 `partial_state` 和适用的 null。明确 UNKNOWN 或未配对可被完整计数，这只说明诊断已读全所选窗口，不能证明已解决。恢复、全历史、券商完备和 grant 排他标志始终 false；本包不增加生产门禁、不清 claim，也不改变交易或恢复流程。

`invalid_first_failure_counts` 用代码定义的固定键说明每条记录遇到的第一处不符合当前诊断合同之处。按检查顺序为 `json`、`payload_structure`、`payload_schema`、`metadata_structure`、`key_shape_or_arity`、`uri_mismatch`、`identity_fields`、`mode`、`date_key`、`claim_state`、`outcome_shape`、`pending_count`；重复记录计 `duplicate_record`，其余无法分类的解析失败计 `unclassifiable`。每条无效记录只计一次，固定计数之和等于 `invalid_records_count`；完成前或窗口不稳定时该对象为 null。不输出原字段值、动态字段名、原 schema、键或错误正文。

这些类别区分严格 JSON 无法解码、对象结构、schema/键/metadata 等合同不符，但不自动证明数据损坏。例如七段键不符合当前八段键合同，历史 schema 不匹配也不能直接当成损坏；本入口不新增兼容读取、终态映射或自动修复。first-failure 不是该记录全部问题，也不是暂停原因。需要一次实际读回后才能知道真实记录落在哪类；合成回归不能代替实际原因。存在无效记录时原配对/UNKNOWN 精确总数仍为 null，`partial_state` 与所有资格 false 不变。

`identity_failure_field_counts` 只进一步解释首个失败为 `identity_fields` 的记录，分别统计
`account_scope`、`strategy_profile` 的固定状态：`missing`（缺字段）、`not_string`（含 null）、
`blank`、`exact_match`、`hex_digest_match`、`key_match_only`、`mismatch`。每个字段的计数和分别等于身份失败记录数，
不是额外的订单数；稳定窗口中没有身份失败则均为零，尚未完成稳定读取则为 null。
`key_match_only` 表示记录值和当前预期仅在原 QPK 有损键规范化后与路径对应段相同，
不能证明原始账户或策略身份相同，更不能作为兼容读取的许可。策略字段始终要求原值精确相等；
账户仅允许原值精确相等，或双方均为合法 64 位 ASCII hex 且解码后的摘要字节相同。
`hex_digest_match` 仅描述后一种账户表示（例如另一策略字段失败时），不适用于策略名。
前置 schema/键等失败不检查这些字段。不输出原值、规范化值或其哈希，不改变配对、恢复或暂停判断。

`identity_key_difference_counts` 仅细分 `key_match_only`：`ascii_case_only` 表示仅 ASCII 大小写差异，
`surrounding_whitespace_only` 表示仅首尾空白，`whitespace_and_ascii_case` 表示两者同时存在，
`other_key_collision` 表示其他原值被有损规范化到同一键。每个字段的计数和等于对应的
`key_match_only` 数；窗口尚未稳定读取时为 null。ASCII 判断避免把 Unicode 大小写折叠混同为普通大小写差异。
这些原因只帮助定位未被摘要合同接受的历史写入差异，不能据有损键相等接受记录或解除暂停。

`account_hash_hex_representation_matches_count` 统计结构、身份及原状态合同有效的记录中，原字符串不同
但合法 64 位 hex 摘要字节相同的条数；读取未完成或窗口不稳定时为 null。此规则与现役
`strategy_runtime` 的账户摘要语义一致，避免将 hex 大小写表示误报为另一账户。其他不合法 hex、
Unicode、长度、标点及策略差异仍拒绝。它不证明旧记录原字符串与当前 broker 返回原字符串相同，
不改生产绑定或账本，也不消除缺 outcome、UNKNOWN、待对账或不完整窗口。

`outcome_shape_field_counts` 只细分首个失败为 `outcome_shape` 的记录，每条失败在每个固定字段各计一次。
`orders_pending_count` 分缺失、非整数（含 bool）、负数、有效；`submitted_orders` 分缺失、非 list、
非对象元素、无法按原合同判别的不可哈希订单状态、有效。`execution_status` 仅输出缺失、非字符串、
空串，或原白名单 `pending_reconciliation/completed/no_action/no_op`；其他原值统一计 `other_string`。
`broker_submission_done` 只区分缺失、非 bool、有效，不输出值。有效字段不使整条记录有效，原总体
partial 及配对/UNKNOWN 等 null 保留，不将缺字段补为零或空数组。

`writer_source_reference` 仅统计原对象或 metadata 是否含固定已知 source/git/runtime revision 键：
缺失或 `present_unverified`。它不输出原值，不把自述字段当作经验证的写入源码，不按业务 key、
日期或当前 checkout 推断版本。原 claim/outcome 合同没有必需的 writer commit provenance，缺乏
独立来源绑定时写入源码仍不可证。未完成稳定读取时整组为 null，没有该类失败时固定计数均为零。

## 可选历史报告线索

仅显式开启 `inspect_historical_outcome` 时读取报告。要求现役仍为 PAUSED、账本窗口稳定且恰好一条无效记录，
该记录的首个失败必须是 outcome_shape。否则不读报告。严格 ISO effective_date 无法确定时不猜日期。
报告源来自 actual-serving 的原报告环境变量，必须为原固定 Schwab/SOXL 报告前缀且与账本同桶；
不接受外部 URI、账户、日期输入，不扩大原 WIF 权限。

运行 composer 把报告根目录交给 QPK，报告本身的 account_scope 未设置；QPK 的
`build_runtime_report_cloud_uri` 依次追加平台、策略、月份和 run_id。诊断复用此构造器从原根目录
得到报告前缀，再校验固定路径及同桶，不把根目录误当完整前缀，也不尝试其他拼接或回退路径。

仅列举 effective_date 前一日零时至后两日零时的固定三天 UTC 窗口；最多读取 20 个报告正文，
每个上限 1 MiB，读取绑定列举所得 generation，禁止重试；第 21 项只用于识别截断，不下载正文。
读取失败的数量为 null，截断的数量仅表示已检查子集；耗尽窗口也不证明归档留存完整或券商历史完整。
`reports_read` 表示报告读取流程已尝试，不代表所有正文已读成功。

`historical_outcome` 只输出固定原因、窗口/候选数量及布尔值，不输出日期、路径、账户、marker、
订单或报告原文。`date_candidates_count` 要求原报告身份及 signal/effective 日期相同，
`identity_date_candidates_count` 额外要求六项声明精确一致及账户观察 hash 按现有摘要合同一致。
`candidates_with_revision_count` 仅表示候选带合法格式 revision 名，不证明该 revision 的写入来源。

`date_candidate_identity_field_counts` 只细分上述日期候选。六项声明逐字段计固定的缺失、类型不支持、
格式不支持、精确匹配、不匹配；selector 仍按原单元素字符串 list 格式，不转换旧字符串/别名。
账户 hash 另分观察对象不可用、缺字段、类型不支持、非支持的摘要格式、精确匹配、合法 hex 表示匹配、
不同摘要。非摘要字符串的原有精确匹配规则保留，其他字符串不做近似接受。各字段计数和等于已检查的
日期候选数，字段存在可由除 missing 外的计数求和，不能把缺失或不支持格式称为另一个账户。
没有完成读取或读取失败时整组为 null；完整读到空窗口时为零；截断时只描述已检查子集。
这些细分不改变原候选判定，不输出任何字段原值，也不补写声明、账本或来源证明。

现有报告合同没有把 ledger marker 与完整券商订单明细绑定。即使唯一候选、revision 存在且报告
订单计数为零，结果仍为 unknown；任意额外 marker 字段也不被视为受支持的关联证据。
`exact_execution_link_supported/writer_source_verified/broker_order_mapping_verified/recovery_qualified`
始终 false；账本 partial、null 配对和 UNKNOWN 总数、PAUSED 保持。此诊断只能缩小可检查归档，
不能补写旧 outcome 或替代对应券商订单、执行报告与独立来源核验。

## 对账 namespace 修正与证据边界

原执行器按券商原生账户 hash 写 execution claim/outcome，但对账候选曾用控制面 `account_scope`（常为 `live`）读账本，可能错误看到空范围。现在只在观察身份明确匹配且原生 hash 有效时读取正确范围，缺失或不匹配就拒绝，不回退 `live`。控制面身份及 broker evidence 的账户摘要合同不变。回归覆盖了旧 scope 为 0、原生 hash 下有 UNKNOWN 提交结果时旧空基线必须不匹配；不消除原有 UNKNOWN、partial 或缺失成交覆盖。

勘误：当前本包源码的订单查询已是 365 日 entered-time 窗口、`max_results=3000`；未触达上限时 open-orders 覆盖可成立，recent-executions 覆盖仍为 false。此前交付文字提到“7 日”不能代表本次当前源码；本次源码核查也不证明现役镜像已使用该版本。历史因 namespace 错配得出的 0 条结果不能作为无未决状态的证据。此入口只补只读状态证据，恢复仍需独立核对券商、账本、有效政策和完整零提交周期。
