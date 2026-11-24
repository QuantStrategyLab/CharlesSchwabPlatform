English | [简体中文](runtime-daily-heartbeat-sync.zh-CN.md)

# Schwab Daily Report Heartbeat Wiring (Default Off)

The `heartbeat` job in `.github/workflows/execution-report-heartbeat.yml`
includes a default-disabled daily publisher step on the existing 22:20 UTC
schedule, reusing the same WIF/`uv` flow and protected configuration. It adds
no cron triggers and does not alter the trading Scheduler, heartbeat
notifications, or account configuration.

The step stays after the existing heartbeat and evidence steps. Its condition
explicitly uses `!cancelled()` and requires
`install_runtime_deps.outcome == 'success'` plus a successful WIF authentication
step in the current run. A health-check or evidence-sync failure still allows
daily-report recording, while authentication failures, dependency installation
failures, or cancellations skip publishing. A daily-report failure never skips
prior health notifications, keeping existing step order and behavior intact.

`Publish read-only runtime execution evidence` likewise runs independently of
health-check and candidate-publish outcomes as long as the run is not canceled,
dependencies installed, and auth succeeded in this run. Missing acceptable
receipts or candidate conflicts therefore do not create an additional
execution-evidence gap on the console. Prior failure conclusions are preserved,
and the export step continues using existing actions, sources, and step-scoped
credentials without calling strategies or the broker.

GitHub default condition semantics: [workflow expressions reference](https://docs.github.com/en/actions/reference/workflows-and-actions/expressions#status-check-functions).

## Enabling

- Executes only when repository variable `RUNTIME_DAILY_SYNC_ENABLED` equals the
  exact string `true`; missing, `false`, `True`, or other values skip it.
- Executes only when an authentication step succeeded in the current run
  (`steps.gcp_auth_primary.outcome == 'success'` or
  `steps.gcp_auth_retry.outcome == 'success'`).
- Command: `uv run --no-sync python scripts/run_runtime_daily_from_reports.py --publish`.
- When off (default), the step is skipped and prior heartbeat and
  execution-evidence behavior is unchanged.

Protected sources (reusing `runtime-daily-sync.yml` names):

- The daily step prefers protected Secret `SCHWAB_RUNTIME_DAILY_TARGET_JSON`
  and falls back to `vars.RUNTIME_TARGET_JSON` or `secrets.RUNTIME_TARGET_JSON`.
  That Secret must be an exact copy of private `RUNTIME_TARGET_JSON` with only
  `binding.account_hash` updated to the application- and user-confirmed native
  account hash; never generate it from historical aliases or unverified report
  fields. The manual daily workflow applies this precedence at job scope,
  whereas scheduled heartbeat overrides the target only in the daily publisher
  step while other heartbeat, reporting, and risk steps continue using
  `RUNTIME_TARGET_JSON`.
- Existing job-level configuration: `RUNTIME_TARGET_JSON`,
  `CLOUD_RUN_SERVICE_TARGETS_JSON`, `CLOUD_RUN_SERVICE(_S)`,
  `RUNTIME_HEARTBEAT_MARKET_*`, `RUNTIME_HEARTBEAT_PUBLICATION_GRACE_MINUTES`,
  `RUNTIME_HEARTBEAT_SCHEDULER_LOCATION`, `CLOUD_SCHEDULER_MAIN_TIME`,
  `GCP_PROJECT_ID`.
- Step-level additions: `SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX` (`secrets`),
  `GCP_REGION: us-central1` (matching `publish_account_facts_from_reports.REGION`).

Secret boundary: `EXECUTION_EVIDENCE_SYNC_TOKEN` and `QSL_CONSOLE_BASE_URL`
(`vars.QSL_CONSOLE_BASE_URL || vars.EXECUTION_EVIDENCE_SYNC_URL`) are injected
at the publisher step only and never exposed at job scope; the runner derives
`EXECUTION_EVIDENCE_SYNC_URL` for `/api/runtime-daily/sync` from that approved
HTTPS origin and fails closed when missing or invalid. Never invent
account/report prefixes, widen IAM, or add dependencies.

## ACK semantics

`--publish` calls the existing publisher: one POST to QRS `/api/runtime-daily/sync`,
validating the fixed ACK key set
`{ok, stored, platform, target_key, business_date, account_key}`. On success, the
runner reports `stored_acknowledged` and
`account_attribution=receiver_reported_account`.

This proves **receiver-reported storage attribution** only, not that the
authenticated UI displays the record (`GET /api/runtime-daily` requires a login
session). Page readback still requires separate verification through a normal
logged-in session. Private `account_key` values are never written to logs or
artifacts; stdout emits fixed status/boolean values only.

## Read-only precheck for the existing Schwab Secret

`runtime-daily-sync.yml` also provides a default-disabled `diagnose_source_access`
manual input, mutually exclusive with `publish`. When selected, it uses existing
workflow ADC to inspect metadata on the `<project-id>/schwab-oauth-token` Secret
read by the QPK runtime, whether `latest` is enabled, and `testIamPermissions`
for `secretmanager.versions.access`. It never reads the Secret payload, calls
Schwab, refreshes tokens, or modifies IAM. Fixed output omits resource names,
error bodies, and credentials.

`permission_reported` records only whether the API response listed that
permission. Google documents that `testIamPermissions` can fail open, so it
cannot prove the payload is actually readable. This diagnostic proves only that
Secret metadata and version state are readable and records permission
observations; it does not prove token freshness, account identity match, or safe
session usability. It is optional and never requires Secret Manager Metadata
Viewer or substitutes for real reads.

### Native account identity verification and recovery

`verify_native_identity` is another default-disabled manual input, mutually
exclusive with `publish` and `diagnose_source_access`. It uses existing cloud
identity to read `schwab-oauth-token` in memory without printing tokens or
native account numbers/hashes, and without refreshing or writing back tokens. It
then performs one bounded read-only Schwab `accountNumbers` call and compares
the result exactly against the configured protected target identity. Identity
fields carried inside daily reports do not prove expected identity. This mode
always runs live native account verification even when the daily publish Secret
is configured.

`publish` has two controlled identity paths: when
`SCHWAB_RUNTIME_DAILY_TARGET_JSON` is configured, it is injected into the runner
only in the daily publish step; the runner requires full equality with
`RUNTIME_TARGET_JSON` (aside from the confirmed binding) and passing
target/binding validation. It then uses that user-confirmed protected binding as
the stable expected identity without calling Schwab's native account endpoint,
while still validating the archived report's target, native hash, source,
timestamp, active serving revision, source binding, and receiver ACK. That
binding is static configuration evidence, not a live broker identity check at
publish time. When the Secret is absent, publishing keeps the prior behavior:
calling the bounded native `accountNumbers` read endpoint first and stopping
before report reads or POSTs if configuration mismatches or validation fails.
Configure this Secret only after application and user confirmation—never from
public booleans, historical aliases, or report self-claims. It does not alter
broker credentials, trading permissions, runtime targets, or risk configuration.

The live verification path stops publishing on `token_expired`, identity
mismatch, multiple matches, invalid responses, or identity read failures.
Publishing via the stable protected binding does not read the Schwab token and
is unaffected by access-token expiry; `check_token_load` and
`verify_native_identity` keep their full token verification behavior. This
workflow never refreshes or writes back tokens. Other fixed failure reasons
inspect existing cloud identity, payload access permission, Secret/version
state, or service availability without needing Metadata Viewer. Never download,
print, or paste token contents.

### Read-only token load check

The default-disabled `check_token_load` manual input is mutually exclusive with
prepare, `publish`, metadata diagnostics, and native identity verification. It
uses existing cloud identity to read `schwab-oauth-token` payload once in memory
and validate its structure and expiry without calling Schwab, reading daily
reports, sending POSTs, or refreshing/writing back tokens. The read call uses a
fixed timeout with SDK auto-retries disabled; output contains fixed status codes
only, with no exception text, resource paths, or token contents.

Fixed reasons (`token_dependency_unavailable`, `token_adc_unavailable`,
`token_permission_denied`, `token_not_found`, `token_version_unavailable`,
`token_network_unavailable`, `token_load_failed`) classify underlying exceptions
into dependency unavailable, ADC unavailable, payload permission denied,
resource not found, version unavailable, network unavailable, and unknown load
failure. `token_permission_denied` corresponds strictly to payload access
exceptions; metadata 403s or `testIamPermissions` observations do not establish
it. `token_expired` indicates the cached payload structure is readable but
expired; re-authorize/rotate through the approved Schwab token workflow before
re-running `check_token_load`. `token_payload_valid` proves only readable
payload structure and valid expiry, not Schwab acceptance or native account
identity match; run `verify_native_identity` for identity, and `publish` still
checks identity before reading reports and posting.

## Verification boundaries

- Local checks cover synthetic inputs and static workflow validation only; no
  live GCS, HTTP, QRS, Telegram, or broker calls are executed, switches remain
  off, and nothing is dispatched or deployed.
- Live daily-report publishing, ACK receipts, and login-page attribution still
  require verification in an authorized execution environment.
