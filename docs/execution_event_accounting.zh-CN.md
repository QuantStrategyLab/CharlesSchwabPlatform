# Schwab 执行事件入账（离线实现）

本文记录 Milestone C 的 E 工作线。`application.execution_event_accounting.ExecutionEventLedger` 是一个本地离线账本；它接收已标准化的增量成交事件，不连接券商，不解析原生订单报告，也未接入生产下单路由。

## 内部离线合同

下单意图先持久化 `intent_id → order_id → owner_id / symbol / side` 绑定。订单身份暂时未知时可以保存内部意图，但必须继续安全拒绝，不能按标的、数量或近似时间猜券商订单。只有经精确查询证据确认后，调用方才可显式绑定该订单身份。

每笔入账事件须有稳定 `event_id`、已绑定的 `order_id`、正数增量 `quantity` 与 `price`、明确的非负 `fee` 及明确标为逐笔的 `fee_source`、`event_time` 及 `event_type=FILL`。累计费用、订单级 commission 或未说明逐笔范围的来源会被拒绝，不能摊到每笔成交。事件摘要由规范化内容按固定 JSON 编码生成 SHA-256。owner、symbol 和 side 从持久化意图取得；若传入的 owner / symbol / side 与意图冲突则拒绝。事件内容相同的重放不重复入账；同 ID 不同摘要会隔离并拒绝。缺少费用或费用来源保持未知并拒绝；更正或冲销类型记录为 unsupported，不作为普通成交记账。

订单累计成交数量只用于和已记录的增量成交合计核对，绝不被转换成成交。`PENDING_CANCEL` 仍是非终态。终态累计数大于（或不等于）已记录的增量成交时，状态为 `incomplete`，预留保持 `pending`；补齐成交流后会重新核对。确认撤单不会冲掉之前已证明的成交。成交、费用、owner 份额和待结算条目按一个事件各记一次；待结算条目只表示尚待结算，不代表可用现金或已结算余额。

账本以确定性 JSON 快照保存意图、订单状态、事件、隔离事件、owner 经济账和账户合计。每个 owner 的预留金额独立累计，账户总预留是 owner 预留之和；一个 owner 的订单不会引用另一个 owner 的预留。临时文件写入并 `fsync` 后通过 `os.replace` 原子替换，再同步目录。重启从快照重建状态，同一事件重放保持不变。这只证明本地快照和账本层的幂等；不证明网络 exactly-once、跨主机 fencing 或券商业务恢复。

## 固定验收

`scripts/run_offline_execution_event_acceptance.py` 对六组纯合成输入执行实际 ledger 方法，并将 E06 接到现有本地 `offline_fault_acceptance` seam：

| 组 | 路径与输入 | 结果 |
|---|---|---|
| E01 | 注册意图 → 增量成交 → PENDING_CANCEL → 迟到成交 → CANCELED；重复同一事件 | 两笔成交共 3 股，成交本金 299.00，费用 0.60，现金经济变动 -299.60；重复回报不重复入账 |
| E02 | 原子快照 → 新 ledger 实例加载 → 重放同一事件 | 前后快照相同，重放被判重复，经济状态不变 |
| E03 | 两个 owner 各自持有独立的 BOXX 买卖订单 | owner 持仓、费用、现金与待结算条目合计等于账户总额；每笔事件只出现一次 |
| E04 | 应答身份未知 → 内部意图保持无 broker ID 且不记成交；随后提供精确合成订单身份及成交详情 | 精确身份路径可由事件账本核对；禁止重试由 E06 的既有 claim/outcome 路径验证 |
| E05 | 终态报告数量大于成交详情 → 同 ID 冲突 → REVERSAL | 未知成交不补造，预留不释放；冲突隔离，更正/冲销拒绝 |
| E06 | 运行既有 response-loss、claim restart、safe query、risk-pause 离线入口 | 保留旧 claim/outcome 与风险/查询行为；资金供给回归另由列出的 pytest 命令验证 |

运行器结果标记 `research_only=true`、`offline=true`、`no_account_connection=true`。E01–E05 使用合成标准化事件；E06 的风险暂停与 claim/query 证据也来自合成本地边界，不是生产验收。

## Schwab 原生能力边界

| 能力 | 结论 | 可证明范围 |
|---|---|---|
| 查询订单、按精确 order ID 查询、状态枚举 | `documented_surface` | 官方公开接口与本地 `schwab-py 1.5.1` 表面可查订单及状态；只支持订单查询/状态判断 |
| 每笔成交的稳定原生 ID | `unsupported_unverified` | 当前允许材料未证明，不能用价量时间拼造天然唯一 ID |
| 每笔成交费用 | `unsupported_unverified` | 累计 commission 不等同逐笔费用；没有逐笔明确费用不得记为零 |
| correction / reversal 原生字段 | `unsupported_unverified` | 未证实原生字段；内部离线账本拒绝这些事件类型 |

因此 E 的执行事件账务为 `implemented_offline`；Schwab 原生逐笔成交解析及原生费用入账仍未验证。订单查询表面可用不能填补成交流或费用的证据缺口，也不能据此恢复未知订单或启用交易。
