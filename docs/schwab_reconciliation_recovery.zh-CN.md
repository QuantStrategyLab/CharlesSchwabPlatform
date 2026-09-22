# Schwab 冻结实盘基线的只读对账

当 `live_continuity.state=RECONCILE_ONLY` 时，`POST /reconcile` 可以建立一份
`schwab_reconciliation_candidate.v1`。它只读取账户身份、全部持仓、现金/购买力、
近一年订单窗口内的挂单观测与本地幂等执行账本；不会调用下单、撤单、策略、插件或状态切换。

该入口默认关闭。只有显式设置 `SCHWAB_BROKER_RECONCILIATION_ENABLED=true`、
只读 collector 可用且运行目标仍为 `RECONCILE_ONLY` 时，才会创建券商客户端。
返回成功前还必须通过 QPK `broker_reconciliation_evidence.v1` 回执校验。

响应和运行报告只包含 SHA-256、布尔核验结果、记录数量及稳定原因码。账户 hash、余额、
仓位、订单与成交明细不得写入响应、日志、通知或公开工件。

缺少任一读接口、账户身份不一致、私有预期摘要未配置、账本读取失败或任一摘要不匹配时，
候选一律保持阻断。`permits_active_lkg=true` 也只是候选事实，不能直接修改
`RUNTIME_TARGET_JSON`。

恢复既有基线仍按共享 QPK 契约进行：两份有时间间隔的收据、独立复核、双审、账户持有人
确认、确认后的新收据，以及控制面精确 CAS
`RECONCILE_ONLY -> ACTIVE_LKG`。该端点不实现 CAS，也不提供订单权限。

运行 `Runtime Target Lifecycle` 只会发布无订单的健康状态，不会调用
`POST /reconcile`。当目标仍为 `RECONCILE_ONLY`，它必须报告为 `disabled`（但保留
原本的 `live` 预期执行通道），因此常规执行心跳不会把冻结基线误报为已启用。

## 覆盖完整性（EX-06）

只读采集对挂单在官方依据下可证明完整：单次查询最近 365 天（覆盖官方 GTC
最长 180 calendar days），并显式 `max_results=3000`（Trader API OAS3 默认上限）。
仅当响应合法且返回条数**严格少于** 3000 时，`open_orders_complete=true`，
`coverage.open_orders_complete` 同步为 true；达到或超过上限时保持 false，并给出稳定
脱敏原因码 `open_orders_max_results_limit_reached`。继续只把非终态订单归入
`open_orders`。

成交流水仍标记 `recent_executions_complete=false`（累计 filled 数量不是带时间戳的
成交流）。候选 `to_safe_dict()` 额外暴露脱敏 `coverage` 诊断（回看天数、查询语义、
返回条数、上限是否命中和原因码）。查询窗口固定为 365 天，调用方不能缩短后仍宣称完整。

生产恢复完整对账仍要求挂单与成交两侧均可证明；本改动只让 C4 所需的挂单观测在
未截断时可证明完整，不授予订单提交权限，也不把成交完整性升格为 true。

## C4 zero-submit 装配旁路（研究/影子）

在只读采集已经得到 `SchwabReconciliationObservations` 与
`BrokerReconciliationEvidence` 之后，可用纯函数
`assemble_c4_shadow_zero_submit_from_reconcile_observations` 把账户/挂单两份物化
快照与调用方另行提供的最终 RiskEngine assessment 交给
`materialize_c4_shadow_zero_submit_cycle`。该旁路：

- 不创建券商客户端，不调用下单/撤单，不改 `POST /reconcile` 行为；
- 不发明 RiskEngine `APPROVE`，也不把 `recent_executions_complete` 伪装成 true；
- 挂单覆盖不完整、身份/digest/策略/时点不一致、或风险结果非零提交语义时，输出
  `PARKED`，并固定 `proposed_orders=[]`、`submission_attempted=false`、
  `execution_permitted=false`、`no_order=true`。

自然禁止提交周期仍需外部只读授权与部署后的真实读回；本旁路只提供可审计的本地
装配入口。
