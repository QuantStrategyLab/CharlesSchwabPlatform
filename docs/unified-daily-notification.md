# Unified daily notification (code only)

`QSL_ROUTINE_NOTIFICATION_MODE` defaults to `immediate`. Exact `daily_digest`
opts into one central bilingual digest. `QSL_DIGEST_ROUTINE_ORDERS=false` retains
immediate confirmed order notices. Failed, pending, uncertain, risk, persistence
and authentication outcomes retain immediate alerts. Healthy idle cycles and
routine plugin observations do not produce individual pushes.

QPK source reference: `b0d10862cfc0ffe9e1b868861b937a9cac6e7201` (notification modules only).

The notification-only QPK source snapshot in `notifications/digest_snapshot/` avoids an
unrelated trading dependency upgrade. Keep these three modules byte-identical
to the reviewed QPK notification modules. Existing QPK/strategy pins, order
execution, funding, risk, account enablement and durable report writes are unchanged.

Before enabling routing, validate the exact active target's private archived
report, candidate store and QRS receiver. Existing cloud monitor wiring uses
`RUNTIME_TARGET_ENABLED=true`, exact protected `RUNTIME_TARGET_JSON`,
`EXECUTION_REPORT_GCS_URI`, `DIGEST_CANDIDATES_GCS_URI`, `DIGEST_STORE_TARGET`,
`DIGEST_OPAQUE_ACCOUNT_UID` and `DIGEST_TARGET_ID`. Missing configuration stops
projection; disabled targets stay disabled. Do not infer private identities.
The producer aggregates every unique cycle in the completed 06:00 SGT window;
missing evidence is unknown, never zero. No new collector or scheduler is added.

No deployment, variable application, IAM, secret or live message is authorized
by this PR. Source adoption and delivery require separate approved release work.

For multiple active targets, protected `DIGEST_TARGET_BINDINGS_JSON` supplies a
list of per-target environment mappings. `DIGEST_NOTIFICATION_SCHEDULE_JSON`
can reuse the existing heartbeat scheduler/calendar declaration to prove a
window was not due. Missing due reports stay errors; closed configured sessions
produce an `actually_ran=false` marker without inventing zero trading counts.
