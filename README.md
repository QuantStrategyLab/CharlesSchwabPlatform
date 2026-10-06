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

`scripts/publish_runtime_daily_from_reports.py` prepares a privacy-safe daily projection for only `charles-schwab-quant-service / soxl_soxx_trend_income / live`. The source identity comes independently from the unique existing runtime policy and `RUNTIME_TARGET_JSON.runtime_risk_limits.binding.account_hash`; every report must match that hash exactly before the established account-facts binding ID is derived. Reports never establish their own expected account.

Preparation and source reads are separate. The default command prepares an empty, incomplete projection without network access or publication. The callable bounded archive reader uses the existing report URI contract, at most 20 reports and bounded bytes. It never treats a latest report or a truncated listing as a full day. Prior-unresolved coverage/retention is not yet verified, so this first reader always reports incomplete coverage, even after listing exhaustion. The existing pure scheduler/calendar policy supplies only proven matured current-business-day due facts; all other states remain unevaluable.

Publication is a separate explicitly invoked function, using only `EXECUTION_EVIDENCE_SYNC_TOKEN`, the fixed HTTPS QRS daily endpoint and `X-QSL-Source-Binding-ID`. It disables redirects, attempts one request, and bounds and validates the stored ACK. There is no automatic retry or account-facts/dispatch token fallback. The command does not publish or invoke heartbeat/account-facts mains, and prints fixed reason codes only. Raw reports, hashes, source IDs, URIs, amounts, orders, credentials and exception text must stay private.

This source candidate does not activate any workflow. Before a separately approved cutover, verify the already-injected independent identity, receiver registry/header gate, exact report prefix and existing IAM, serving revision, effective scheduler facts, full-day/prior-unresolved coverage and intended endpoint/token references inside the existing cloud boundary. Real-data preparation, a stored ACK, authenticated account/date readback, production UI and a natural cycle remain separate acceptance steps. Synthetic tests do not establish live connectivity.

The ACK account key is the trusted receiver’s canonical UI alias, not an independently verified caller identity. Physical attribution relies on the caller’s exact independent hash check and the receiver’s per-POST comparison against its current protected binding. A valid response is only `stored_acknowledged` with `receiver_reported_account`; authenticated account/date readback is still required. An already available trusted expected UI key may be compared, but no duplicate alias variable or permission is introduced. The existing `RUNTIME_HEARTBEAT_PUBLICATION_GRACE_MINUTES` is honored; malformed values leave schedule unevaluable.

Preparation also seals a canonical body/source-identity digest. Before any transport is opened, publication rejects changes to that body or identity and rejects manually constructed results without preparation evidence. The verified byte snapshot is used for both the request and ACK expectations. This is an in-process guard against accidental mutation, not a security boundary against a caller that controls Python code; receiver validation still applies.
