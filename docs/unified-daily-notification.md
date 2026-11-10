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

Run the manual `Digest candidate job release` workflow from the default
branch. The production environment supports `verify`, which reads back the
pinned Cloud Run Job image through a dedicated short-lived Google identity.
Deploy and rollback are available for the isolated test environment. The
workflow does not invoke the producer or change its schedule.

[Organization container release contract](https://github.com/QuantStrategyLab/.github/blob/main/docs/deployment-workflows.md)

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

The heartbeat candidate publisher uses the same bounded SDK archive reader.
A successfully listed empty month is empty input, including when a completed
window spans two UTC months. Permission, timeout and JSON failures still abort
publication. An empty window produces idle evidence only after the existing
schedule check confirms that no run was due.

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
[06:00, 06:00) window. A normal run starts at 06:10 and finishes by 06:20.
The publisher does not depend on the Actions `Digest Candidates Daily` workflow,
whose schedule can be delayed beyond the central read. It projects from the
archive itself and waits up to 480 seconds, polling every 30 seconds, for source
or candidate readiness within the existing 600-second watchdog. Only the exact
pre-write `candidate_source_absent` and `candidate_due_source_absent` errors permit
another projection, with a fresh archive read and current time. Once projection
returns, only a missing exact candidate object is polled; publication is never
repeated. A negative upload acknowledgement alone is not success: the dated slot
must pass the same schema, identity and nonempty-row readback checks.

Persistent absence exits nonzero with `candidate_publication_wait_timeout`.
The wall clock is checked after every wait and before success; crossing 06:55 or
the original local date returns `candidate_publication_deadline_missed`.
The process watchdog returns `candidate_publication_watchdog_timeout`.
Disabled/missing bindings, invalid objects, permission/transport failures and
source conflicts still fail immediately. An interrupted write remains unknown
and is never retried. No missing due report becomes idle evidence.
The success record contains only the business day and a verified-readback flag.
These source changes do not update the deployed image or job configuration.

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

The heartbeat `scripts.emit_unified_digest_candidates` command reports fixed
`stage` and `error_code` values for archive reads, candidate reads, projection,
merge and candidate writes. It also identifies binding, source-selection,
schedule and output-serialization failures. Only exact local codes and fixed
storage categories are exposed; unknown errors use `unexpected_error`.
Raw exception text, object paths and credential context stay out of output,
including output from the existing coverage-alert fallback.
`uploaded=false` is the failure flag, not proof that no write took effect.
Candidate writes retain one generation-conditional attempt with retries disabled;
inspect existing evidence before retrying an uncertain result.

## Shared risk-block message format

The attention adapter delegates formatting to QPK's
`risk.attention.render_attention_notification` when that feature is available.
It passes the existing notification report's `started_at` and optional
`console_run_url` as display-only evidence. Account labels show only the last
two characters. Guard codes remain visible alongside Chinese or English
explanations. Missing time, link and financial facts remain explicitly unknown.
Schwab's equity-baseline loss wording and baseline provenance are preserved.
Notification routing, approved-gate classification, transition keys and risk
permissions remain unchanged.

The current pinned QPK predates this feature, so the adapter retains its legacy
format until a coordinated pin update. The review bot should adopt the QPK
format change before updating the consumer pin. No pin, deployment or live/risk
switch is changed by this patch. A run-specific console link requires the console
producer to supply `console_run_url`; an account overview URL is not a run link.

## Summary projection of archived execution facts

The notification projection preserves inner execution attention even if report
persistence completed successfully. Receipt publication after a scheduled time
is distinct from a receipt outside its recorded source run interval; the latter
is flagged `receipt_time_unconfirmed` without moving the source business window
or changing heartbeat acceptance. Missing fills remain unknown.

Candidate merging normalizes documented optional null defaults, count-status
spelling and equivalent timestamp offsets. Identity, counters, execution state
and other factual differences still fail before the generation-guarded upload.
This projection does not change reconciliation, broker calls or alert routing.

Execution cycles use the same summary builder for early notification candidates
and the completed archive. The outer `runtime_report.v1` status remains `ok` or
`error`: blocked, failed, missing and unknown execution outcomes use `error`;
recognized completed, preview and broker-submission outcomes use `ok`.
A positively completed zero-order cycle without a submission status is normalized
to `no_action` when the existing completion and failure checks pass. An empty
dry-run preview uses the same checks and reports `dry_run_completed`; its digest
remains ineligible and live order and fill counts remain unknown. Missing
completion evidence and conflicting failures still use `error`.
Verified consecutive-loss and daily-loss blocks retain their structured reasons
with an outer `error`. They remain rejected by the execution heartbeat, while
their notification is deferred to the digest only after receipt validation and
confirmed candidate publication. Other failures still alert immediately.
These validated gate-only errors can still publish the same cycle's account
observation through the account-facts projector. Receipt, source identity,
currency and freshness validation remain required; this does not accept execution.
`summary.execution_status` retains the precise outcome. An outer `ok` still does
not prove a fill or reconciliation. Probe and skipped-report semantics are unchanged.

Plugin `routine_observation_counts` are carried from the current run's summary
into both projections. An absent field stays unknown; an explicitly empty map
means the plugin observation completed with no routine signals. These states
remain distinct in canonical comparison. Existing contradictory candidates are
not rewritten or normalized away. Source identity, factual conflict rejection
and generation-conditional writes continue to apply on replay and restart.

## Candidate warning display

The existing candidate-persistence warning uses the shared five sections:
Result, Scope, Evidence, Coverage and Next step. Its QPK presentation snapshot
has a separate byte/source manifest under `notifications/digest_snapshot`;
other snapshots and runtime dependency pins are unchanged.

The warning states that inclusion is unconfirmed, with unknown orders/fills and
an unknown business window when none was supplied. A run link uses only valid
current Actions context; absent context remains unavailable. It does not copy
private storage paths or raw exceptions. Existing attempt-marker identity,
atomic publication, sender callback, strict boolean acknowledgement and
failure behavior are unchanged. This copy change does not resolve an actual
candidate conflict or prove central delivery.
