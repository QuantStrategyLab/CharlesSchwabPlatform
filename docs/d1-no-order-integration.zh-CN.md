# D1 本地无下单集成

## Scope

本文件只覆盖 CharlesSchwabPlatform 当前工作区的 D1-A：一条本地、显式、无下单的集成入口，以及只读原生订单解析。不包含 D2 观察、真实账户采样、下单、撤单、改单或生产配置。

入口命令是 `python scripts/run_d1_no_order_acceptance.py`。普通安装后的控制台入口名为 `schwab-d1-no-order-acceptance`，指向 `scripts.run_d1_no_order_acceptance:main`。

## Evidence

- 策略调用的是已安装的 `soxl_soxx_trend_income` 入口 `CallableStrategyEntrypoint.evaluate`。合成上下文只提供 SOXL/SOXX 的价格与趋势均线，并关闭期权层和收入层覆盖，避免额外输入。
- 计划调用 `decision_mapper.map_strategy_decision_to_plan`。
- 风险结果来自该入口内部已经调用的共享风险门。当前合成账户没有资本基数绑定时，结果为 `risk_gate=REJECT`、`rejected:capital_base`，目标数量为 0。
- claim 调用 `application.execution_claim.claim_execution_marker`，写入调用方给出的本地目录。
- 三股合成例调用现有 `ExecutionEventLedger`：两笔显式逐笔成交，本金 299.00，费用 0.60，现金经济变动 -299.60。费用来源是测试声明的 `synthetic_declared_per_fill_fee`，不是券商费用。
- 原生解析使用已安装的 `schwab-py` 1.5.1。直接测试和验收脚本的订单、活动、持仓输入都标记为 `synthetic`，`native_observed=false`。

## Method

无下单入口先跑策略、计划和 claim，并直接探测最终传输对象。该对象的 `place_order`、`cancel_order`、`replace_order` 和 `submit_order` 在接触客户对象之前抛出 `NoOrderTransportError`。验收还调用既有 `execute_rebalance_cycle` 的合成正向计划，把其 `execution_port` 接到同一传输对象；正常执行路径产生一次提交意图，传输拒绝且客户对象零调用。既有执行服务保守地将其记为 `unknown`、要求对账，不把拒绝包装为成交。`dry_run` 标志不是这道边界。

恢复由新的 Python 进程加载同一本地账本、claim 和原生部分事实。重复的显式成交只记一次。第二次 claim 返回未取得。

原生解析只复制 SDK 已读取的订单字段、`Client.Order.Status`、权益/期权指令，以及订单腿上的代码、数量和资产类型。`orderId` 只在记录里已经存在时保留。`filledQuantity` 只作为累计数量留下，语义是 `not_an_increment`。

## Implementation

- `application.d1_no_order_integration.NoOrderBrokerTransport` 是最终传输。它保存客户对象的身份，但不调用客户方法。
- `run_no_order_cycle` 写 `cycle.json` 和本地 claim。
- `record_synthetic_explicit_case` 把上述三股合成例写入现有账本 `ledger.json`。
- `import_recorded_native_observations` 写 `native_partial.json`。只有账本里已经绑定的订单才把累计数量交给 `record_order_update` 做核对。未绑定订单保持未分配，不指定 owner。解析结果不调用 `record_execution_event`。
- `observe_expected_cycle` 读取收据内部的 `cycle_date`，用既有本地 marker store 为缺失日期只记录一次告警事件；不调用通知服务。文件修改时间不代替业务日期。
- `application.d1_native_read_only.parse_recorded_native` 不做网络请求。

没有新增第二套账本、风险门、策略或券商消息框架。`execution_event_accounting` 与 `runtime_broker_adapters` 未改。

## Validation

直接测试：

- `tests/test_d1_native_read_only.py`
- `tests/test_d1_no_order_integration.py`
- `python scripts/run_d1_no_order_acceptance.py`

新进程恢复断言在集成测试和验收脚本中执行。另在私有测试副本中，claim 与已知事件落盘后强制终止子进程，再从另一进程恢复并核对经济数值及幂等性。传输测试使用会记录调用的客户替身，并要求记录为空。
既有离线故障验收的风险暂停场景提供请求、执行日志接收、禁新增风险效果和安全只读查询四阶段证据；本地入口调用该真实执行逻辑时屏蔽通知发送。
缺失收据在本地首次发现时产生一个事件；新进程复查不会重复产生，也不会发送通知。

## Results

风险拒绝路径的正目标数为 0，四个交易方法被最终传输拒绝，客户调用数为 0。合成账本恢复后仍是 3 股、本金 299.00、费用 0.60、现金经济变动 -299.60，重放为重复，快照不变。

原生合成订单 `9001` 保留订单号、`FILLED` 和累计数量 3。费用、逐笔事件号和 owner 均为空，没有生成成交事件。带有已绑定订单 `42` 的累计数量 2 只更新累计核对，已记录增量成交仍为 0，对账状态为 `incomplete`。

## Limitations

以下字段没有被已安装 SDK 证明，本入口保持未支持，不用合成值填补：

- 逐笔成交的稳定原生事件号。活动集合里的 `activityId` 或 `executionLegs` 不升格为账本事件。
- 逐笔费用。订单级 `commission`、`fees` 或交易响应正文不记为成交费用，也不写成 0。
- 更正、冲销或公司行动。
- 用净持仓给 owner 分配仓位。外部标的保持 `owner_assigned=false`。
- 把 `filledQuantity` 换成逐笔增量。
- 交易查询响应正文。SDK 只有查询参数和交易类型枚举，没有响应字段模型。

这些合成测试不是 `native_observed`，也不是账户只读采样。账户采样由 SOL 另行处理。

## Disposition

D1-A 本地实现需要由 SOL 复核、提交与建立草稿 PR。D2 未启动。
