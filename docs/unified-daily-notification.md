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

## Scheduled heartbeat identity resolution

The heartbeat and completed-window projector use
`scripts/with_heartbeat_digest_binding.py` when running from the heartbeat workflow.
Existing complete workflow bindings and multi-target bindings retain their
current validation path. For an enabled single target with missing digest values,
the wrapper reads the sole 100% serving Cloud Run revision using the existing
workflow principal and explicit account. Its runtime identity must exactly match
the configured platform, profile, service, account selectors, scope and dry-run
mode. Only the existing plain-text digest UID, target, slot and destination are
consumed, in process memory; Secret Manager references are never dereferenced.
Configured values must agree with the serving binding. A changing traffic
observation is rejected. A missing schedule is taken only from the configured
runtime target's main schedule and timezone.

Missing source values return `digest_identity_owner_input_required` or
`digest_schedule_owner_input_required`. The owner must supply the approved
private binding; do not generate a UID or infer an identity from an account label.
Metadata read failures and identity conflicts remain explicit failures. This
wrapper does not change GitHub settings, runtime flags, receiver bindings,
notification routes or cloud permissions. It does not establish delivery success.

## Publication before the central daily read

GitHub schedule events can start late. A heartbeat workflow is therefore not a
reliable deadline trigger for the 07:00 Asia/Shanghai read. Keep its existing
monitoring checks, but run the archive-only publisher independently in an
approved Cloud Run Job using the platform image and its frozen dependencies.

Command: `/app/.venv/bin/python -m scripts.publish_digest_before_read`.
The command imports the existing producer, never the service entrypoint. It does
not run a drill, refresh a broker session, invoke a trading route, send Telegram,
or access Secret Manager. Archive reads use the Storage SDK and the job's
attached identity; the image does not need gcloud. Existing candidate writes,
merge semantics and identity checks remain authoritative.

Use one job per existing central target, with its exact protected runtime JSON,
archive prefix, opaque UID, target ID, destination and slot. No identifiers can
be inferred from a display label. Supply the existing notification schedule so
an idle candidate is only produced when the completed window was not due.
Multi-target binding JSON is rejected by this entrypoint: one successful target
cannot mask another missing target.

The job requires `DIGEST_PUBLISH_ENABLED=true`,
`QSL_ROUTINE_NOTIFICATION_MODE=daily_digest`, and the existing
`RUNTIME_TARGET_ENABLED=true`. These are publisher-job settings, not changes to
the trading service. Preserve the platform's existing unified-notification flag
and any notification marker prefix needed by its existing producer. Do not
inject broker credentials or Telegram token secrets into this job.

The deployment proposal is:

| Setting | Proposed value |
| --- | --- |
| Cloud Scheduler | `10 6 * * *`, timezone `Asia/Shanghai` |
| Scheduler target | OAuth-authenticated Cloud Run Jobs `:run` API for this job |
| Scheduler retries | 0; do not blindly retry an uncertain execution request |
| Job tasks / parallelism | 1 / 1 |
| Job retries | 0; an uncertain write is not proof of failure |
| Job task timeout | 600 seconds |
| Image | Reviewed immutable platform image containing this entrypoint |
| Publication window | Start at or after 06:00 and before 06:40 |
| In-process watchdog | 600 seconds, including projection and readback |
| Readback deadline | Before 06:55, leaving five minutes before the reader |

The business day is the day before the job's Shanghai date, for the completed
[06:00, 06:00) window. A normal run starts at 06:10 and finishes by 06:20. A late
start, disabled/missing binding, due-but-absent report, unconfirmed write, wrong
business day or identity, failed readback, or expired deadline exits nonzero.
An interrupted write remains unknown and must be inspected before any retry.
The success record contains only the business day and a verified-readback flag.

Deployment, Scheduler creation and IAM are separate operations requiring their
own authorization. The Scheduler identity needs permission to execute only the
job; the job identity needs the existing archive-read and candidate-store
permissions (including notification state used by the existing producer).
Do not substitute a project-wide role or copy the trading runtime's credentials.
A merged change or a successful unit test does not prove the job is deployed.

After a separately authorized rollout, verify a natural job execution and the
exact candidate object generation created before 07:00, then check the central
reader's date, slot, identity and coverage. A Scheduler API acceptance alone is
not publication success. Keep missing/invalid coverage visible; do not substitute
old candidates, zeros, or an artificial `not_due` result.

References: [GitHub schedule timing](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)
and [Cloud Run scheduled jobs](https://docs.cloud.google.com/run/docs/execute/jobs-on-schedule).
