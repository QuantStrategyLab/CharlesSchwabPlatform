# Schwab 冻结实盘基线的只读对账

## 现役恢复顺序

- 普通重启或已有授权下的临时故障恢复：沿用仍有效的账户和配置基线及原保护，不因重启或短暂故障重新登记基线或增加完整禁提交周期；重启不改变连续性状态或清除 claim。
- `PAUSED`：按具体暂停原因核对直接相关事实并保留原 claim；人工暂停须有账户持有人明确启用授权。未知历史只有实际影响当前资金、未结义务或执行防重时才阻断相关操作，不要求完整重建所有历史。现有冻结基线仍有效时，普通停用、启用不需要重新登记。此页的 `POST /reconcile` 不提供暂停状态转换。
- `RECONCILE_ONLY`：按下文冻结基线对账流程操作；该状态不能由 `PAUSED` 为使用对账入口而强行替换。

历史结果无法补齐时保留 `unknown`，不补写终态、清除或绕过原 claim；只核对对本次账户和执行有实际影响的未决风险。必要时在受保护环境核对当前持仓、现金、未结订单或相关账务变化。历史完整重建、专用恢复控制器或另一次泛化的历史风险接受均不是普通启用的通用门槛。

2026-10-29 的 `observe_current` 只报告持仓存在、现金有观测、365 天订单查询窗口内未见挂单且返回数未触上限。它没有保存可供核对的持仓或现金明细，近期成交和历史关联仍未知；该窗口结果不能独立证明全账户没有未结订单，也不足以建立账务起点。真实数据应留在受保护的原有环境，不写入公开运行报告或本机。

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

恢复仍按当前固定版本的共享 QPK 契约进行：可信来源的 v2 候选至少需要一份新鲜收据，
模型审查为 advisory；人工明确确认后，还需新的只读收据及控制面精确 CAS
`RECONCILE_ONLY -> ACTIVE_LKG`。来源、账户、订单和账本等真实事实仍须独立核验。
该端点不实现 CAS，也不提供订单权限。

`coverage.comparison_status` 区分 `not_configured`（缺少预期摘要）、`not_compared`
（覆盖不完整）、`matched` 与 `mismatched`。读取失败时请求失败，不生成候选；
`evidence` 中兼容契约的 false 本身不能区分未比较与不匹配，应结合上述状态和阻断码。
现金摘要只比较 `cash_balance`；`buying_power` 和 `total_equity` 仍作为实时观测保留。
旧完整观测摘要不能转换为现金摘要，也不能仅凭摘要差异断定实际错账；迁移须依据
受保护的原始材料独立核验和批准，不能把当前观测自动当作预期基准。

运行 `Runtime Target Lifecycle` 只会发布无订单的健康状态，不会调用
`POST /reconcile`。当目标仍为 `RECONCILE_ONLY`，它必须报告为 `disabled`（但保留
原本的 `live` 预期执行通道），因此常规执行心跳不会把冻结基线误报为已启用。

## 覆盖完整性（EX-06）

只读采集单次查询最近 365 天（覆盖官方 GTC 最长 180 calendar days），并显式
`max_results=3000`（Trader API OAS3 默认上限）。现有候选实现将响应合法且返回条数
**严格少于** 3000 记为 `open_orders_complete=true`；达到或超过上限时保持 false，
并给出稳定脱敏原因码 `open_orders_max_results_limit_reached`。这一标记反映实现的
窗口和上限判断，不能单独证明窗口外不存在未结订单或其他提供方边界。继续只把非终态
订单归入 `open_orders`。

成交流水仍标记 `recent_executions_complete=false`（累计 filled 数量不是带时间戳的
成交流）。候选 `to_safe_dict()` 额外暴露脱敏 `coverage` 诊断（回看天数、查询语义、
返回条数、上限是否命中和原因码）。查询窗口固定为 365 天，调用方不能缩短后仍宣称完整。

生产恢复完整对账仍要求挂单与成交两侧均可证明；窗口未触上限不授予订单提交权限，
也不把成交完整性升格为 true。

## C4 zero-submit 装配旁路（研究/影子）

在只读采集已经得到 `SchwabReconciliationObservations` 与
`BrokerReconciliationEvidence` 之后，纯函数
`assemble_c4_shadow_zero_submit_from_reconcile_observations` 把账户/挂单两份物化
快照与调用方另行提供的最终 RiskEngine assessment 交给
`materialize_c4_shadow_zero_submit_cycle`。

`POST /reconcile` 成功路径会通过 `build_reconcile_c4_report_attachment` 调用同一旁路，
把脱敏后的 C4 状态写入**内部**运行报告 `summary` / `diagnostics`；公开 HTTP 响应仍只
返回 `schwab_reconciliation_candidate.v1`，不包含账户明细或 C4 原始材料。
生产批准的 UES 版本若未包含该研究模块，服务仍可启动；内部 C4 诊断标为
`PARKED/C4_CONSUMER_UNAVAILABLE`，券商对账候选与交易保护不受该旁路影响。

该旁路：

- 不创建券商客户端，不调用下单/撤单，不改变公开响应契约；
- 不发明 RiskEngine `APPROVE`，也不把 `recent_executions_complete` 伪装成 true；
- 默认没有真实最终 RiskEngine assessment provider（`FINAL_RISK_ASSESSMENT_PROVIDER`）时，
  显式输出 `PARKED`，`reason_codes` 含 `assessment missing`，并固定
  `proposed_orders=[]`、`submission_attempted=false`、`execution_permitted=false`、
  `no_order=true`；
- 只有同一账户/策略/UTC `as_of` 的真实 provider 能给出最终 assessment，且挂单覆盖、
  身份/digest/时点满足时，才可能得到 `READY_SHADOW_ZERO_SUBMIT`；任一不满足仍为
  `PARKED`。

自然禁止提交周期仍需外部只读授权与部署后的真实读回；本旁路只提供可审计的本地
装配与报告入口。
