# UES 4a39438 → 76e04b1 交易差异审查

结论：建议把 #607 的正常 merge commit `76e04b18180f21f7e04bd1f6e116a3206e2c30fc` 作为后续迁移候选，避免为 QNDX 再做一次包迁移。本次不启用 QNDX，不换 live 版本。

## 精确源码范围

| 区间 | 完整 DAG 提交数，含 merge | 净变更文件 |
|---|---:|---:|
| `4a3943883cd6b5bbfe32a559e56a91b40a81b7ce..9d1544d1476dd19b1da61b16f37cee7f9126caf4` | 177 | 201 |
| `9d1544d1476dd19b1da61b16f37cee7f9126caf4..76e04b18180f21f7e04bd1f6e116a3206e2c30fc` | 37 | 74 |
| 旧基线至完整候选 | 214 | 266 |

三个区间都确认祖先关系。完整 `git diff --binary --full-index old new`、路径清单、提交父链和 SHA-256 清单保存在本地 evidence；[完整 compare](https://github.com/QuantStrategyLab/UsEquityStrategies/compare/4a3943883cd6b5bbfe32a559e56a91b40a81b7ce...76e04b18180f21f7e04bd1f6e116a3206e2c30fc) 可复查。当前 UES main 另有后续提交，本次不包含它们。
逐提交分类见 `COMMIT_REVIEW.tsv`：原 177 项采用前序已逐项审查的分类，本次复查实际 production 净差异；新增 37 项逐项列出。新增区间只有 #607 改 production 代码，其他为研究、文档、测试或工具，不把研究 cap 实验当成 live 限额变更。

## 需要交易行为审查的提交

| 提交 / PR | 具体影响及审查 |
|---|---|
| `ecab783c8d47743f7eea92ebd18d7d12f163f108` / [#571](https://github.com/QuantStrategyLab/UsEquityStrategies/pull/571) | Nasdaq smart DCA 增加预取历史输入。有效技术指标 snapshot 优先，次选预取历史；采样时刻与输入来源可能改变信号倍数及买入金额。 |
| `1857aaad97517b3f7d2c61eed28fccadc05fa60a` / [#572](https://github.com/QuantStrategyLab/UsEquityStrategies/pull/572) | entrypoint 验证 `market_data.prefetched_market_history` 是 Mapping，并传入计划器，使新输入契约在正常平台调用生效。 |
| `9d1544d1476dd19b1da61b16f37cee7f9126caf4` / [#573](https://github.com/QuantStrategyLab/UsEquityStrategies/pull/573) | smart 模式逐信号标的取 snapshot 或 prefetch；都缺少即抛错，拒绝计划。删除策略内部 broker `market_history` I/O fallback。普通非 smart DCA 不受这项输入要求影响。输入不足导致少交易/不交易是预期风险，不得恢复隐式查询让发布通过。 |
| `06dc92fc7968ab459022ae492e40cec7b4d13c74` / [#549](https://github.com/QuantStrategyLab/UsEquityStrategies/pull/549) | TQQQ 共享纯计算核心提取 pre-plugin helper。保留 holdings-derived prior state、比较和 truthiness。当前旧→候选两端独立加载纯模块，对 1,152 个边界组合和 seed=607 的 10,000 个随机案例比较所有 decision 字段；11,152 项零差异。只支持本函数的合成范围，不是券商验收。 |
| `da053e310c97cba7ea309d64b2a5d81e695e8ef3` / [#564](https://github.com/QuantStrategyLab/UsEquityStrategies/pull/564) | SOXL/TQQQ 私有 builder 增加显式 research session successor。正常 entrypoint 默认 None，仍走原日历函数。不得把 research successor 注入 live 来绕过 T+1。 |
| `76e04b18180f21f7e04bd1f6e116a3206e2c30fc` / [#607](https://github.com/QuantStrategyLab/UsEquityStrategies/pull/607) | 显式 `new_buy_execution_symbol=QNDX` 才把 Nasdaq 新定投份额路由到 QNDX；保留已有 QQQ/QQQM 的 snapshot target，不卖旧仓，信号仍是 QQQ/SPY。QQQ+QQQM+QNDX 合计暴露分别通过等价 ticker 的既有限额及原总限额。要求有效 capital evidence，拒绝时无 positions；聚合值仅供风控，不返回为执行订单。默认 None；alias 本身不启用。TQQQ/rebalancing combos 排除。当前两个 target 不选择此 preset。 |
| 原区间 11 项 dependency 变更/merge | UES 的 QPK requirement 从 `a9093762…` 演进到 `f6f2079f…`，不能因提交标题含 research 就忽略间接影响。两平台有效 uv override 均固定 `bfe55791adcd8867790e15dba6fd35304e4093c0`，本方案保持它。未逐一审计历史 QPK 内部差异；实际镜像安装包仍须后续验收。 |

## SOXL / TQQQ 与限额

旧→候选的 `soxl_soxx_trend_income.py`、`tqqq_growth_income.py` 两个主策略文件逐字节一致；全部原有 bundled config 与 signals 文件一致。catalog/manifest 的变更只属于 Nasdaq DCA 的 alias、描述和默认关闭的开关；SOXL/TQQQ config 不随本次改动。TQQQ 共享核心与两个 private builder 的变化已在上表展开，不能笼统称整个包无交易行为变化。

迁移的硬约束：**不放宽 SOXL 0.679、总名义仓位 0.97、现金比例 0.03，不改变 TQQQ 当前参数与资金授权。** Schwab 健康 target 实际绑定的 `nominal_caps.SOXL=0.679`、`total_nominal_exposure_cap=0.97`、`binding.reserved_cash_ratio=0.03` 已重新读取并断言；其他 symbols/caps/leverage/daily loss/exit 参数同样保持原值。

**TQQQ 的证据边界必须保留：**选中的 IBKR TQQQ 冻结 target 没有 `runtime_risk_limits` 字段，不能据此声称 TQQQ 也绑定了 0.679/0.97。新 allowlisted revision readback 确认 `IBKR_RESERVED_CASH_RATIO=0.03`，没有显式 `IBKR_STRATEGY_CONFIG_PATH`。包内 TQQQ 原有 cash/dual-drive 默认为 0.02，未在本次变化；平台现金覆盖不等于所有内部计算阶段都已证明按 0.03 限制。因此最终 evidence 必须证明有效 TQQQ 限额及其执行路径；若用户要求两策略都执行同一组 0.679/0.97/0.03，需要明确的既有授权/实现证据，不能本轮添加限额或用报告措辞补齐。此项列为正式发布阻断条件，不顺带修复。

订单类型、频率、账户、费用、options、cash-only 设置、plugins、对账、T+1 和反重发保护不在本次变更范围。尤其 IBKR TQQQ 现有 cash-only 开关不应被 SOXL 约定覆盖。

## 实际验证与未完成项

- 固定候选 UES + 固定 QPK bfe5579，本地选定 DCA/QNDX/core/entrypoint/risk/catalog 测试：115 passed，40 subtests passed。
- 固定 QPK release-readiness/manifest 契约：13 passed。
- 两平台 continuity 测试分别在各自独立 Python 进程运行：各 17 passed；初次将两仓库一起加载发生 `scripts` 同名模块冲突，4 failed / 30 passed，随后隔离进程消除了测试加载问题，未改源码。
- 26 项本地合同检查通过；包含旧 target 接受、当前错误绑定拒绝、篡改拒绝、synthetic state/fingerprint 关系，以及缺 release 输入时官方工具拒绝出 manifest。
- TQQQ 核心 11,152 个合成案例零差异。

这些验证不等于全量 CI、源码升级后平台兼容、实际镜像 installed-package、release promotion evidence、线上 HTTP/券商验收或真实订单恢复。
本次没有可接受的新 canonical release 输入；官方 readiness 结果为 false，未生成新 manifest 或新可加载冻结 baseline。完整云端 evidence 和内容绑定的接受仍须补齐，不能把本稿/单测标成 promotion eligible。
