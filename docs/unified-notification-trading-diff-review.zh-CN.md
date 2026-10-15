# 交易差异审查：CharlesSchwabPlatform

审查方式：本次作者自审，辅以 AST 调用参数比较；不声称独立人工审查。
基线：`6169934e220f335f8aadae6a435f34a5242d4092`。范围为当前 PR 相对基线的全部改动。

结论：源码差异仅涉及通知路由、附加的通知报告事实、已有监测入口的候选投影、通知配置透传、测试和说明。交易依赖、锁文件、UES/QPK pin、信号、仓位、资金限额、订单构造、券商调用、状态 claim 和风险门禁保持原样。

现有文件中的券商、订单、风险、交易状态和报告持久化调用，已对调用表达式及全部参数做 AST 比较：

| 文件 | 敏感调用数量 | 参数完全相同 |
|---|---:|---|
| `application/rebalance_service.py` | 6 | 是 |
| `application/runtime_report_summary.py` | 0 | 是 |
| `main.py` | 16 | 是 |

新增模块不调用券商。只有既有云监测器在 `daily_digest` 且目标明确启用时，才读已有私有报告并写候选；不启用账户、不修改订单或交易排程。关闭模式保持既有行为。失败、未决订单、风险、认证及持久化问题保留即时通知；已确认的普通订单可通过 `QSL_DIGEST_ROUTINE_ORDERS=false` 继续即时发送。

通知代码取自 QPK `783bcc464cef89abd05b63e765cad00490b012c5`，有意不升级平台交易依赖。三个源文件逐字节相同：

| 文件 | SHA256 |
|---|---|
| `digest_routing.py` | `60c3f258c4d1a03bc49c7d03b392c02c316541726dfacb0b1d4fae6dbd2e5017` |
| `digest_candidates.py` | `a6a0d10f90a17906c45393a84e819c8d71f430300e977fdb4fa1f824e26878f5` |
| `digest_archive.py` | `b9a92299adfddff42c62f7d493845e0cbde91df7e475a5cb6bf2114446045b49` |

合并条件：相关验证通过、PR 当前精确 head 的 CI 全绿、当前远端主线的 push / workflow_run / 可复用 workflow 调用链无自动部署。否则保留草稿。此审查不批准从旧生产 revision 升级到整个当前主线；生产到候选的完整差异、UES 指纹与 baseline 仍须按后续发布方案单独审查。
