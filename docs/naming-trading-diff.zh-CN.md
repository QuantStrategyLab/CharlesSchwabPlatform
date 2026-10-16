# 交易差异说明：CharlesSchwabPlatform

基线：`86f81f20f346e91d78d8de66ba49b7c475ede100`。作者自审与 AST 检查，不声称独立人工审查。

只改通知显示名解析、固定显示配置副本、测试和说明。账户 key、策略 profile、RuntimeTarget、fingerprint、release/continuity binding、Secret 名称、存储 key、通知去重键均不改。交易依赖及锁文件未改；SOXL 0.679 / 总仓位 0.97 / 现金 0.03 未改。不部署交易服务。

现有源码中的券商、订单、风险、状态和报告持久化调用表达式及其参数比较结果：

| 文件 | 敏感调用数 | 参数不变 |
|---|---:|---|
| `notifications/display_names.py` | 0 | 是 |
| `notifications/telegram.py` | 0 | 是 |

新增解析器只读同目录的生成 JSON 及可选显示环境配置，不联网、不调用券商、不写持久状态。显示值不得作为机器标识再输入交易调用。固定副本来自 QRS 的生成器，避免只为改名升级交易依赖。PR 保持 draft；即使 CI 通过，也未批准实际运行版本采用。
