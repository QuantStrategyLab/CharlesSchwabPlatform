# Routine daily digest migration (owner approval required)

This patch uses the existing archived-report producer and central daily digest.
`QSL_ROUTINE_NOTIFICATION_MODE` defaults to `immediate`; `daily_digest` defers
only verified healthy no-order cycle notices. Original results, persistence,
guard enforcement, pending/unknown orders and urgent alerts remain unchanged.
Real order activity also remains immediate during migration.

The attention adapter forwards actual admission reason codes to the shared
zh-CN/en formatter. A daily-loss block explains the code and asks the owner to
review equity, cash flows and the configured loss limit. Intent confirmation
does not release the guard.

The QPK pin is a notification-only backport on the exact former consumer pin;
no unrelated risk-engine changes are included. This draft does not deploy,
change runtime settings, enable the mode or alter private recipient mappings.

Before approving a one-platform rollout, verify matching business-day private
candidates, complete coverage, actual central delivery and immediate alert
delivery. Retain legacy delivery until replacement is verified. Configure
`NOTIFY_LANG=zh-CN` or `en` independently; the central renderer already supports
both languages. Stale/unknown evidence must stay visibly stale/unknown.

中文：这份草稿只接入已有日报，不启用生产开关。正常无单消息可进入一份日报；
风控拒绝、订单结果未知、待对账、运行异常和真实订单仍即时通知。
负责人核验候选日期、覆盖和真实送达后，再批准部署及单平台切换。
