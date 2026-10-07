# CharlesSchwabPlatform

CharlesSchwabPlatform is the execution runtime that actually talks to Charles Schwab's brokerage API for US equities, as part of QuantStrategyLab's larger multi-repository trading system. It takes runtime-enabled strategy profiles — the trading logic itself lives elsewhere, in `UsEquityStrategies` and `UsEquitySnapshotPipelines` — and handles the Schwab-specific plumbing: authentication, order placement, dry-run/live gating, notifications, and Cloud Run deployment. If you're working on Schwab connectivity, execution safety, or how this platform gets deployed and operated, this is the repository you want.

[Chinese README](README.zh-CN.md)

> Investing involves risk. This project does not provide investment advice and is for education, research, and engineering review only.

## Architecture role

- **Layer**: `runtime-platform`.
- **Responsibility**: Charles Schwab US equity execution runtime.
- **Owns**: Schwab connectivity, token/runtime integration, dry-run/live controls.
- **Consumes**: UsEquityStrategies, UsEquitySnapshotPipelines artifacts, QuantPlatformKit, QuantRuntimeSettings.
- **Must not**: own strategy research logic or store credentials in Git.

Live execution is fail-closed behind a durable atomic execution claim. Cloud Run must keep both request concurrency and maximum instances at `1`; the claim is the cross-revision guard that prevents duplicate broker submission if requests overlap. An unresolved claim never expires automatically and requires order/report reconciliation before manual recovery.

## Runtime boundary

- Loads only runtime-enabled strategy profiles exposed by the strategy packages.
- Handles broker/API connectivity, dry-run checks, notifications, and deployment settings.
- Must keep credentials in GitHub Secrets, cloud secret stores, or the broker-specific secret system, never in Git.
- Should start with dry-run or paper mode before any live order path is enabled.
- Account production-drift status is consumed only from the account risk snapshot (or the portfolio snapshot fallback); unbound research performance stores are not used by the Schwab execution gate.

## Direct vs snapshot-backed profiles

Direct runtime profiles can usually run from market history or portfolio state. Snapshot-backed profiles need a current artifact bundle from the matching snapshot pipeline before this platform should execute them. The platform should not invent strategy eligibility; it should consume the status and artifacts published by the strategy and snapshot repositories.

## Deploy safely

1. Configure secrets and runtime variables outside Git.
2. Run the workflow or service in dry-run mode.
3. Review generated orders, logs, notifications, and reconciliation output.
4. Confirm rollback steps and artifact versions.
5. Enable scheduled or live execution only after the above checks are clear.

## Repository layout

- `tests/`: unit, contract, and regression tests.
- `.github/workflows/`: CI, scheduled jobs, release, or deployment workflows.
- `scripts/`: operator scripts and local helpers.
- `research/`: research configs and non-live candidate artifacts.

## Paper command consumer

The isolated, default-disabled Schwab paper command consumer is documented in
[`docs/paper_execution_command_consumer.md`](docs/paper_execution_command_consumer.md).

## Quick start

```bash
uv sync --frozen --extra test
uv run --no-sync ruff check --exclude external .
uv run --no-sync python scripts/check_qpk_pin_consistency.py
```

## Useful docs

- No separate `docs/` directory yet; start with this README and the workflow files.

## Community and security

- See [CONTRIBUTING.md](CONTRIBUTING.md) for pull request scope, local verification, and documentation expectations.
- Follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for maintainer and contributor conduct.
- Report credential, automation, broker, exchange, or cloud-resource vulnerabilities through [SECURITY.md](SECURITY.md); do not open public issues for secrets or live-execution risk.

## License

See [LICENSE](LICENSE).

## Read-only daily caller candidate

`scripts/publish_runtime_daily_from_reports.py` prepares a privacy-safe daily projection for only `charles-schwab-quant-service / soxl_soxx_trend_income / live`. The source identity comes independently from the unique existing runtime policy and `RUNTIME_TARGET_JSON.runtime_risk_limits.binding.account_hash`; every report admitted as projection input must match that hash exactly. The established account-facts binding ID is derived from the independent identity. Reports never establish their own expected account.

For reports that pass non-account qualification, the identity-skip gate distinguishes a missing observation (`source_observation_missing`), an absent hash key (`source_hash_missing`), and a present null/non-string/empty/blank/padded hash (`source_identity_invalid_shape`). Only a valid nonempty, unpadded string that differs exactly from the independent hash uses `source_identity_mismatch`. No hash is trimmed, case-normalized or recovered from an alias. Malformed non-object payload/summary/observation containers retain the existing bad-report/incomplete handling. Older generic mismatch results cannot establish which category occurred. Observation is optional in the producer, including supported skipped/error/legacy or failed observation-projection paths; absence alone does not prove the configured account is wrong.

The unchanged schema/platform/scope/current-revision/URI/run-id/time/size qualification gate runs before report identity comparison. Reports already rejected by those gates remain excluded with read-error/incomplete evidence and cannot block other qualified reports solely because their identity is different or unavailable. This does not admit any previously ineligible report. A qualified identity failure still stops the whole batch, in any order. An older business date is not an exemption: a report that passes the existing time and revision predicates still faces the exact account check.

The manual runner reads two bounded fields from the existing sealed projection: an allowlisted `daily_status` and integer `projected_run_count` (0–20), the length of that record's runs list. No second admission metadata is created. All-excluded evidence is explicitly `daily_status=read_incomplete`, `projected_run_count=0`, empty runs, read errors including `coverage_unconfirmed`, incomplete coverage and `fills.count=null`. `status=prepared` means preparation finished; zero projected runs is not zero trades, complete history or a healthy day. The summary cannot change the projection, its seal, source identity or publication behavior.

Only when that sealed projection has zero runs, the runner may append fixed `zero_run_*` diagnostics from the same already-read batch. `zero_run_entries` counts supplied decoded entries (0–20), not attempted objects or admitted reports; `zero_run_read_failed` and `zero_run_truncated` preserve the reader's booleans. Ten mutually exclusive first-failure counters have suffixes `uri_invalid`, `time_invalid`, `schema_invalid`, `scope_invalid`, `revision_mismatch`, `path_mismatch`, `time_order_invalid`, `size_invalid`, `unevaluable` and `provenance_passed`. They follow the original URI/time-parse/schema-and-scope/revision/path/time-order/size evaluation order and total exactly the entry count. Opaque or unexpected failures remain unevaluable. Objects rejected before decoding are absent from these counts; their lost detail cannot be recovered from the read-failure flag.

`zero_run_other_business_date` counts only that existing sealed projection's fixed business-date exclusions. A report passing non-account provenance can still be discarded for a malformed observation container, or, after exact identity admission, for an older business date. Thus `provenance_passed` is not an admission count, and zero runs is not evidence that every report failed provenance. Prior-day qualified identity failures still stop the whole batch. The diagnostic does not evaluate or normalize account identity. Invalid context, oversized batches, counters or seals omit diagnostics rather than fabricate zeros. No source reread, broader prefix scan, Cloud Run environment read, credential access, workflow change or publication is added. These counts neither verify the producer's bucket/prefix nor establish full archive coverage; existing `coverage_unconfirmed` and incomplete/null-fill semantics remain unchanged. Only fixed booleans and bounded integers are output, without paths, hashes, account values, run IDs, dates, revisions or free-form errors.

After that exact mismatch has already stopped preparation, the manual runner may append four fixed integer diagnostics from only the same supplied first 20 in-memory entries: `mismatch_provenance_passed`, `mismatch_provenance_failed`, `mismatch_provenance_unknown`, and `mismatch_passed_ascii_case_only`. The first three count valid nonempty mismatching hashes whose original non-account scope/current-revision/URI/time/size checks pass, fail, or cannot be evaluated. Only the passing group can contribute to the fourth count: differing ASCII strings whose lowercase copies match. This is a syntactic diagnostic, not official broker identity equivalence, an admission rule or permission to normalize either hash or its binding digest. The native broker account hash, account-response digest and compound source-binding digest remain separate fields.

The original reason, skipped status, exit code 2, absent projection and no-publication outcome remain unchanged. No extra source request occurs. Missing diagnostic context or invalid counters adds no counters; it does not invent a zero result. Each count is bounded by 20 and the first three total at most 20. These counts say nothing about unread archives; truncation/read-error facts and incomplete coverage remain unchanged. Passing the existing time predicate does not assert a current-business-day report. No individual hash, length, fingerprint, account, URI, timestamp, revision value or exception detail is output.

Preparation and source reads are separate. The default command prepares an empty, incomplete projection without network access or publication. The callable bounded archive reader uses the existing report URI contract, at most 20 reports and bounded bytes. It never treats a latest report or a truncated listing as a full day. Prior-unresolved coverage/retention is not yet verified, so this first reader always reports incomplete coverage, even after listing exhaustion. The existing pure scheduler/calendar policy supplies only proven matured current-business-day due facts; all other states remain unevaluable.

Publication is a separate explicitly invoked function, using only `EXECUTION_EVIDENCE_SYNC_TOKEN`, the fixed HTTPS QRS daily endpoint and `X-QSL-Source-Binding-ID`. It disables redirects, attempts one request, and bounds and validates the stored ACK. There is no automatic retry or account-facts/dispatch token fallback. The command does not publish or invoke heartbeat/account-facts mains, and prints fixed reason codes only. Raw reports, hashes, source IDs, URIs, amounts, orders, credentials and exception text must stay private.

This source candidate does not activate any workflow. Before a separately approved cutover, verify the already-injected independent identity, receiver registry/header gate, exact report prefix and existing IAM, serving revision, effective scheduler facts, full-day/prior-unresolved coverage and intended endpoint/token references inside the existing cloud boundary. Real-data preparation, a stored ACK, authenticated account/date readback, production UI and a natural cycle remain separate acceptance steps. Synthetic tests do not establish live connectivity.

The ACK account key is the trusted receiver’s canonical UI alias, not an independently verified caller identity. Physical attribution relies on the caller’s exact independent hash check and the receiver’s per-POST comparison against its current protected binding. A valid response is only `stored_acknowledged` with `receiver_reported_account`; authenticated account/date readback is still required. An already available trusted expected UI key may be compared, but no duplicate alias variable or permission is introduced. The existing `RUNTIME_HEARTBEAT_PUBLICATION_GRACE_MINUTES` is honored; malformed values leave schedule unevaluable.

Preparation also seals a canonical body/source-identity digest. Before any transport is opened, publication rejects changes to that body or identity and rejects manually constructed results without preparation evidence. The verified byte snapshot is used for both the request and ACK expectations. This is an in-process guard against accidental mutation, not a security boundary against a caller that controls Python code; receiver validation still applies.

### Manual wiring, separate from real-data acceptance

`runtime-daily-sync.yml` is a separate main-only, manually dispatched workflow; merging its source cannot trigger cloud reads or a daily POST. Existing account-facts required inputs, its default behavior, and scheduled heartbeat jobs are unchanged. The new workflow defaults to preparation; only its explicit typed `publish` input adds one publication attempt. Preparation has no publication token. Both modes use the existing WIF identity and locked dependencies, with no new environment gate, credential, IAM grant, deployment or broker invocation.

An explicit run invokes `scripts/run_runtime_daily_from_reports.py`, which reads the fixed service's actual serving traffic and its existing scheduler candidates through a bounded read-only metadata adapter, then reuses the merged bounded report reader and caller. Only one stable 100-percent serving revision is admitted. A scheduler must be uniquely resolved, enabled, and target that same service's `/run` route; the route is compared, never invoked. Paused, ambiguous, mismatched or unavailable scheduler facts remain `unevaluable`; a declared cron or TTL is not actual-state evidence. The original caller CLI remains offline.

The first real preparation is a separate acceptance step inside the existing cloud boundary. Report prefixes, identity/hash, private metadata, raw reports and exception details must never enter shared output. The daily endpoint is local to the new publisher step; the legacy execution-evidence URL is not changed. Output is limited to fixed classifications. `prepared` or `stored_acknowledged` with `completeness=incomplete` is not a healthy-day or authenticated-readback claim. Coverage remains unconditionally false: proving current-day-as-of-cut enumeration, retained prior unresolved history, terminal evidence and actual due obligations is still required. There is no operator completeness override; listing exhaustion, an empty directory or fewer than 20 reports do not supply that proof.
