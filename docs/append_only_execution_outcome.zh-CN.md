# Schwab 不可变执行 outcome

Schwab 在执行前保留原子 claim marker 作为唯一的重复下单拦截依据。执行周期返回后，平台调用 QPK 的 `record_outcome`，在独立的 `execution_outcomes/` 路径以仅创建方式记录终态；它不会覆盖 claim marker。

因此 Cloud Run 运行账户只需读取和创建对象权限。不要为覆盖 marker 赋予广泛的删除或对象管理员权限。已有 outcome 时记录操作会安全返回，claim 继续阻止同一信号重复提交。

## 2026-09-27 离线故障验收

固定 base `fba4896d21efbf5b9c2bdf0db75823150e1427af`。结果来自 `application/offline_fault_acceptance.py` 的合成离线运行，标记为 research_only、offline、no_account_connection。下表只记录现有 seam 的能力与缺口，不表示部署、账户连接或业务周期成功。

| 场景 | 分类 | 实际调用 | 限制 |
|---|---|---|---|
| accepted submit 后应答丢失 | verified-safe-reject | `claim_execution_marker`、`execute_rebalance_cycle`、本地 `record_outcome` | 没有 broker order id，不能编造同一订单身份再查询。第二次 claim 失败所以第二次提交为 0。不证明网络恰好一次送达。 |
| 部分成交、撤单请求、迟到/重复回报 | unsupported | `collect_read_only_reconciliation_observations` | `recent_executions_complete` 为 false，`filledQuantity` 不是带时间的成交流。`PENDING_CANCEL` 仍留在未完成订单里，不等于已撤。费用与重复成交的恰好一次无法证明。 |
| claim 持久化附近重启 | verified-safe-reject | 本地 `claim_execution_marker`，重启后同一目录再次 claim，然后 `collect_read_only_reconciliation_observations` | claim 无 TTL。对账未完成前 continue 提交为 0，也不把未完成 claim 当成未成交清除。这不是同一订单已恢复。 |
| 查询 429、断开、未知 | verified-safe-reject | `fetch_managed_snapshot` 的账户重试、`build_order_status_fetcher`、`execute_rebalance_cycle` | 有界重试只在账户快照。订单查询 429 只调用一次。断开没有得到现金快照，未知回报没有把数量写成 0。`unknown_pending_orders` 使该周期新增风险提交为 0。 |
| 同一 BOXX 多 owner | unsupported | 对账观察与持仓/现金会计事实 | 净持仓只能核对合计数量。事实里没有 owner。同一现金只记录一次，不能分成多份 owner 现金。 |
| 风险暂停 | supported | `execute_rebalance_cycle`，暂停后的只读订单查询 | 分列请求、执行日志中的 `NEW_RISK_PROHIBITED`、以及新增风险提交 0。未调用撤单，也没有市价清仓。卖出不在该 gate 的禁止范围内；本场景持仓放在缩放后的目标上。这不是已部署的生产暂停开关。 |
