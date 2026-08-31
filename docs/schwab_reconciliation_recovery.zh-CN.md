# Schwab 冻结实盘基线的只读对账

当 `live_continuity.state=RECONCILE_ONLY` 时，`POST /reconcile` 可以建立一份
`schwab_reconciliation_candidate.v1`。它只读取账户身份、全部持仓、现金/购买力、
近七日订单与成交，以及本地幂等执行账本；不会调用下单、撤单、策略、插件或状态切换。

响应和运行报告只包含 SHA-256、布尔核验结果、记录数量及稳定原因码。账户 hash、余额、
仓位、订单与成交明细不得写入响应、日志、通知或公开工件。

缺少任一读接口、账户身份不一致、私有预期摘要未配置、账本读取失败或任一摘要不匹配时，
候选一律保持阻断。`permits_active_lkg=true` 也只是候选事实，不能直接修改
`RUNTIME_TARGET_JSON`。

恢复既有基线仍按共享 QPK 契约进行：两份有时间间隔的收据、独立复核、双审、账户持有人
确认、确认后的新收据，以及控制面精确 CAS
`RECONCILE_ONLY -> ACTIVE_LKG`。该端点不实现 CAS，也不提供订单权限。
