# Unified daily notification (code only)

`QSL_ROUTINE_NOTIFICATION_MODE` defaults to `immediate`. Exact `daily_digest`
opts into one central bilingual digest. `QSL_DIGEST_ROUTINE_ORDERS=false` retains
immediate confirmed order notices. Failed, pending, uncertain, risk, persistence
and authentication outcomes retain immediate alerts. Healthy idle cycles and
routine plugin observations do not produce individual pushes.

QPK source reference: `4cc5e55251269923771f90cd661bb62a5cfdf0cd` (notification modules only).

The notification-only QPK source snapshot in `notifications/digest_snapshot/` avoids an
unrelated trading dependency upgrade. `digest_routing.py` and `digest_archive.py`
remain byte-identical to that reference. `digest_candidates.py` additionally
appends Schwab-only `block_reason` facts through `notifications.block_reason`.
Existing QPK/strategy pins, order
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

Fill counts require individual execution identifiers. A completed order response
proves an order completed, not how many fills it received. Missing fill counts
remain unknown; confirmed routine orders still appear without a false diagnostic.

The optional `block_reason` object carries `reason_code`, `observed_value`, `limit`
and `order_submitted_this_run`. Counts and limits come from the completed gate's
diagnostics, including its emitted limit; this projection has no separate risk
threshold or private account configuration. Unrecognized or inconsistent reasons
are omitted. Only the latest real run's validated risk-blocked receipt, with no
submission, pending orders or errors, permits `order_submitted_this_run=false`.
It never establishes daily zero counts. Older reports retain the generic blocked
wording, with a run fact only when the existing receipt proves it. The runtime
daily receiver projection is unchanged; the new object goes only to notifications.
