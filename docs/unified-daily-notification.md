# Unified daily notification (code only)

An accepted order notice reports broker acceptance and says that a fill is not
yet confirmed. An order identifier alone does not establish a partial or full
fill. A sell notice does not assert that the account's risk has decreased.

Successful orders, no-trade cycles and healthy receipt checks are recorded for
the next scheduled daily digest in the selected language, regardless of the unified routing
flag. Failure, unconfirmed submission, unresolved reconciliation, risk,
persistence and authentication incidents retain immediate alerts. If digest
coverage cannot be confirmed, an independent operational alert identifies that
gap without sending the routine trade message.

When an execution-report heartbeat sends an order-status attention alert, it
directs the operator to check the broker's order and fill records without
asserting that an order filled. A Schwab acknowledgement awaiting reconciliation
is routine only when the archived cycle contains positive, consistent acceptance
evidence; missing or contradictory evidence remains attention. The original
receipt still says reconciliation is required, and fill counts stay unknown
until individually confirmed. The workflow link remains available without a web
preview. The manual healthy-check option cannot force an immediate message.

The notification-only source snapshot follows QPK revision
`2b04882058666ebc81d1650d93fa2105ec4ba3d8`. The trading dependency remains
pinned separately to the existing revision in `pyproject.toml`.

The notification-only QPK source snapshot in `notifications/digest_snapshot/`
includes Schwab-specific per-run projection and `block_reason` facts through
`notifications.block_reason`. The trading and strategy pins, order execution,
funding, risk, account enablement and durable report writes are unchanged.

Before deploying this policy, validate the exact active target's private archived
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

When `SCHWAB_UNIFIED_NOTIFICATION_ENABLED=true`, successful execution-report
persistence also publishes that completed report's notification candidate. The
producer merges runs by `source_run_id` into the exact configured
`schwab/<business-day>/<slot>.json` object. It uses a single generation-conditional
write. Identical reports are idempotent; conflicting identities or source facts
stop the write. Missing order and fill evidence stays unknown.

Incident markers retain their existing directory. Diagnostic event staging uses
`schwab/notification-markers/events/`; it cannot establish a business run or
replace an archived report. Archive projection reads the events saved in each
report and any explicit receipt observation bound to that same source run. It
does not list the notification bucket. Producers need access only to their marker
directory and exact daily candidate objects, in addition to existing report reads.

When the normal live entry is blocked by runtime admission, unified mode archives
a `skipped` report before returning. Its exact
`runtime_execution_not_admitted` diagnostic projects an `actually_ran=false`
row with `cycle_count=0` and unknown order/fill counts, only after the archive
acknowledges persistence. This path builds reporting adapters only; it does not
build a broker client, load strategy signals, submit orders or send a message.
The existing generation checks and source-conflict protection also apply to
these non-execution rows. Failed persistence leaves coverage unknown. No
retrospective report is inferred from a probe or an HTTP success.

Completed-window exports also preserve explicit `market_closed` skip reports
as non-run evidence with the archive's actual source ID and unknown trade
counts. A schedule-proven idle window has no source run ID. Both forms survive
the generation-checked store without inventing a trade count. The QRS receiver
keeps unknown counts separate from target coverage; its test-only snapshot
records the base source and exact backported patch source and hash in
`tests/qrs_snapshot/provenance.json`. The runtime QPK snapshot and dependency
pins are unchanged.

Runtime Guard notifications keep the localized summary and workflow link. Raw
provider audit entries remain in their original restricted logs and are not
passed as Telegram technical details, including when the deployed formatter is
pinned to an older shared package. Guard detection, deduplication and failure
exit codes are unchanged.
