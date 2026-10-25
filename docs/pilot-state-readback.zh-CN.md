# Schwab 试点状态只读核验

手动运行 `Schwab Pilot State Readback`，无输入。只在原仓库 `main`、首次尝试执行；不自动重跑。调用独立的受保护 workflow，沿用 metadata/native identity 包的原 Variable 优先、Secret 回退、原 WIF 与云身份来源，不复制项目、服务、账户或存储映射。原声明仅在一个操作步骤中以 Secret 环境变量消费。它不调用券商、不读 token、不刷新、不写 Secret、不修改配置或 claim、不解除暂停、不归档、不发通知。

程序从 Cloud Run 服务选唯一 100% serving revision；校对六项身份及 actual-serving 风险绑定账户 hash 与原声明相等，存储操作前后再校对 serving revision。账本读取使用该原生 hash，而不是控制面的 `account_scope`。`PAUSED` 原样保留；只报告结构化原因字段是否存在，不把字段存在或账本内容解释成暂停原因。

存储位置只按 actual-serving 环境中原 QPK 工厂的优先级选择：`SCHWAB_EXECUTION_STATE_CLOUD_URI` / 旧 GCS 名称，其次 `QSL_EXECUTION_REPORT_CLOUD_URI` / 旧 GCS 名称，再次 `EXECUTION_REPORT_CLOUD_URI` / 旧 GCS 名称。新名称即使为空也不会退回对应旧名称。没有可用位置、选中 Secret 引用、非 GCP/非 GCS 或读取失败时明确结束；不读取 Secret 值、不猜 bucket、不读取 Mac 本地状态。工厂关闭本地 fallback。

输出 artifact `schwab-pilot-state-safe` 仅包含固定状态、布尔、revision SHA-256、owner 是否存在及是否匹配当前策略、账本内容摘要与条数。owner 不存在只有在远端 exists 明确返回 false 时成立；对象存在但不可读、消失或结构错误仍为未知。owner 相同允许多个调用者，不能证明 OAuth grant 排他。依赖的标准输出、错误输出和 Python 日志在读存储期间被捕获；不输出原账户、金额、URI、marker、原因自由文本或异常正文。

账本复用原 `calculate_recent_ledger_digest`：claims/outcomes 两个命名空间各读取按键排序最近至多 256 个对象正文，前缀列举仍可能列出该范围全部对象，SDK 沿用原只读重试。没有额外重试循环，也没有全局请求时限硬保证；workflow 的 10 分钟是外层停止限制。摘要不解析每条业务结果，不证明未决 claim 为零、全历史齐全、券商订单/成交齐全或可恢复交易。读取一部分后失败时保留 `partial_state`，不能把未知变成 0。

## 对账 namespace 修正与证据边界

原执行器按券商原生账户 hash 写 execution claim/outcome，但对账候选曾用控制面 `account_scope`（常为 `live`）读账本，可能错误看到空范围。现在只在观察身份明确匹配且原生 hash 有效时读取正确范围，缺失或不匹配就拒绝，不回退 `live`。控制面身份及 broker evidence 的账户摘要合同不变。回归覆盖了旧 scope 为 0、原生 hash 下有 UNKNOWN 提交结果时旧空基线必须不匹配；不消除原有 UNKNOWN、partial 或缺失成交覆盖。

勘误：当前本包源码的订单查询已是 365 日 entered-time 窗口、`max_results=3000`；未触达上限时 open-orders 覆盖可成立，recent-executions 覆盖仍为 false。此前交付文字提到“7 日”不能代表本次当前源码；本次源码核查也不证明现役镜像已使用该版本。历史因 namespace 错配得出的 0 条结果不能作为无未决状态的证据。此入口只补只读状态证据，恢复仍需独立核对券商、账本、有效政策和完整零提交周期。
