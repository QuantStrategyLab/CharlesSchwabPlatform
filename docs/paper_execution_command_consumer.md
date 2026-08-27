# Schwab 纸面延迟执行命令消费者

`/paper-command-consumer` 是一个默认关闭、独立的纸面审计入口。它只在命令已经通过共享的 release、风险收据、平台、账户作用域和策略档案绑定后，才读取 Schwab 的账户快照与行情，并记录模拟提案；它不构造 `ExecutionPort`，不调用下单 API，也不改变 `/run`、`/dry-run` 或 `/probe`。

## 隔离条件

必须同时满足以下条件，否则入口拒绝运行：

- `SCHWAB_PAPER_EXECUTION_COMMAND_CONSUMER_ENABLED=true`；
- `SCHWAB_DRY_RUN_ONLY=true` 且 runtime target 为 `paper`；
- `RUNTIME_TARGET_ENABLED=false`，仅部署到独立服务，不能复用常规 Schwab 服务；
- `SCHWAB_CASH_ONLY_EXECUTION=true`；
- runtime target 提供完整的 `strategy_release` 和非空 `account_scope`；
- `SCHWAB_EXECUTION_COMMAND_CLOUD_URI` 指向新的专用、create-only 命令前缀，不能位于执行报告或 marker 前缀之下。

消费者把命令的 `strategy_symbols` 与当前受管 symbol 集合精确比较，并将现金加持仓市值与账户权益对账。未知持仓、空/过期报价、空头或任一不一致都会生成 fail-closed 的 `rejected` 审计结果；不会把失败转换为新指令或真实订单。

## 命令意图

Schwab 纸面命令需使用 `schwab.paper-execution-intent.v1`，包含：

- `target_mode: "value"`；
- 精确覆盖受管标的的非负 `targets`；
- 相同的 `strategy_symbols`；
- 保留字段 `strategy_release` 与内容寻址的纸面风险收据。

命令生产与独立服务的实际部署仍需单独审核。本次实现不会启用环境变量、创建服务、修改 Scheduler 或调用 Schwab 账户。
