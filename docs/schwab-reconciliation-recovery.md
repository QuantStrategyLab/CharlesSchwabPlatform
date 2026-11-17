English | [简体中文](schwab-reconciliation-recovery.zh-CN.md)

# Schwab Frozen Live-Baseline Read-Only Reconciliation

## Active recovery order

- Normal restart or transient fault recovery under existing authorization: reuse the still-valid
  account and configuration baseline along with existing guards, without re-registering the baseline
  or requiring a full no-submit cycle; restarts do not change continuity state or clear claims.
- `PAUSED`: verify facts directly tied to the pause reason and keep existing claims; manual pauses
  require explicit account-holder authorization to re-enable. Unknown history blocks related
  operations only when it actually affects current funds, open obligations, or duplicate-submission
  prevention, without requiring full reconstruction of all history. When the frozen baseline remains
  valid, routine disable and enable actions do not require re-registration. `POST /reconcile`
  provides no pause state transitions.
- `RECONCILE_ONLY`: follow the frozen-baseline reconciliation flow below; `PAUSED` must not be forced
  into `RECONCILE_ONLY` merely to invoke the reconcile endpoint.

Keep unrecoverable historical outcomes as `unknown` without fabricating terminal states or clearing
or bypassing existing claims; verify only open risks that affect the current account and execution.
Inspect current positions, cash, open orders, or related accounting facts in a protected environment
when needed. Full historical reconstruction, a dedicated recovery controller, or another blanket
historical risk acceptance is not a universal prerequisite for normal enablement.

The 2026-10-29 `observe_current` check recorded only that positions exist, cash was observed, and no
open orders appeared within the 365-day order query window below the result cap. It did not retain
position or cash details for reconciliation, and recent fills and historical links remain unknown;
that window result cannot independently prove zero open orders across the account or establish an
accounting opening baseline. Keep real data in its protected environment rather than public run
reports or local workstations.

When `live_continuity.state=RECONCILE_ONLY`, `POST /reconcile` builds a
`schwab_reconciliation_candidate.v1`. It reads account identity, all positions, cash/buying power,
open-order observations over the past year, and the local idempotent execution ledger only; it never
calls order placement, cancellation, strategies, plugins, or state switches.

This endpoint is disabled by default. A broker client is created only when
`SCHWAB_BROKER_RECONCILIATION_ENABLED=true`, a read-only collector is available, and the runtime
target remains `RECONCILE_ONLY`. Before returning success, the candidate must also pass
QuantPlatformKit `broker_reconciliation_evidence.v1` receipt validation.

Responses and run reports contain only SHA-256 digests, boolean check results, record counts, and
stable reason codes. Account hashes, balances, positions, orders, and fill details must never appear
in responses, logs, notifications, or public artifacts.

Missing any read interface, account identity mismatch, unconfigured private expected digests, ledger
read failure, or any digest mismatch keeps the candidate blocked. Even `permits_active_lkg=true` is
a candidate fact only and cannot modify `RUNTIME_TARGET_JSON` directly.

Recovery still follows the pinned QuantPlatformKit contract: a v2 candidate from a trusted source
requires at least one fresh receipt, with model review advisory; after explicit human confirmation,
recovery requires a fresh read-only receipt and an exact control-plane CAS
`RECONCILE_ONLY -> ACTIVE_LKG`. Source, account, order, and ledger facts still require independent
verification. This endpoint implements no CAS and grants no order authority.

`coverage.comparison_status` distinguishes `not_configured` (missing expected digests),
`not_compared` (incomplete coverage), `matched`, and `mismatched`. Read failures fail the request
without creating a candidate; compatibility `false` flags inside `evidence` cannot distinguish
uncompared from mismatched on their own, so check `comparison_status` and blocker codes together.
Cash digests compare `cash_balance` only, while `buying_power` and `total_equity` remain live
observations. Legacy full-observation digests cannot be converted into cash digests, and digest
differences alone do not prove an actual accounting error; migrations require independent
verification and approval from protected source materials rather than automatically treating current
observations as expected baselines.

Running `Runtime Target Lifecycle` publishes order-free health status only and never calls
`POST /reconcile`. While the target stays `RECONCILE_ONLY`, it must report `disabled` (preserving
the underlying `live` expected execution lane) so routine execution heartbeats never misreport a
frozen baseline as enabled.

## Coverage completeness (EX-06)

Read-only collection issues one query over the past 365 days (covering the official 180-calendar-day
GTC maximum) with explicit `max_results=3000` (Trader API OAS3 default cap). The candidate
implementation records `open_orders_complete=true` when a valid response returns **strictly fewer**
than 3000 records; at or above the cap it stays `false` with sanitized reason code
`open_orders_max_results_limit_reached`. This flag reflects the implemented window and cap check and
cannot independently prove no open orders exist outside the window or across other provider
boundaries. Only non-terminal orders enter `open_orders`.

Execution streams remain `recent_executions_complete=false` (cumulative filled quantities are not a
timestamped fill stream). Candidate `to_safe_dict()` also exposes sanitized `coverage` diagnostics
(lookback days, query semantics, returned count, cap-hit flag, and reason code). The 365-day query
window is fixed; callers cannot shorten it while still claiming completeness.

These open-order and execution-stream coverage requirements apply to the `RECONCILE_ONLY`
reconciliation candidate implementation and are not a universal prerequisite for ordinary disable or
enable actions. Staying below the result cap grants no order submission permission and never promotes
execution completeness to `true`.

## C4 zero-submit assembly bypass (research/shadow)

After read-only collection produces `SchwabReconciliationObservations` and
`BrokerReconciliationEvidence`, pure function
`assemble_c4_shadow_zero_submit_from_reconcile_observations` passes the materialized account and
open-order snapshots along with any caller-supplied final `RiskEngine` assessment to
`materialize_c4_shadow_zero_submit_cycle`.

The `POST /reconcile` success path invokes the same bypass through
`build_reconcile_c4_report_attachment`, writing sanitized C4 status into **internal** run report
`summary` / `diagnostics`; the public HTTP response still returns `schwab_reconciliation_candidate.v1`
only, without account details or raw C4 materials. If the production-approved `UsEquityStrategies`
version omits this research module, the service still starts and internal C4 diagnostics report
`PARKED/C4_CONSUMER_UNAVAILABLE`, leaving broker reconciliation candidates and trading guards
unaffected.

This bypass:

- Creates no broker client, submits or cancels no orders, and leaves the public response contract
  unchanged;
- Never invents `RiskEngine` `APPROVE` and never disguises `recent_executions_complete` as `true`;
- Without a real final `RiskEngine` assessment provider (`FINAL_RISK_ASSESSMENT_PROVIDER`), outputs
  `PARKED` with `reason_codes` containing `assessment missing` and fixed `proposed_orders=[]`,
  `submission_attempted=false`, `execution_permitted=false`, and `no_order=true`;
- Produces `READY_SHADOW_ZERO_SUBMIT` only when a real provider returns a final assessment for the
  matching account, strategy, and UTC `as_of`, and open-order coverage, identity/digest, and
  timestamp checks all hold; otherwise remains `PARKED`.

Natural no-submit cycles still require external read-only authorization and post-deployment
readback; this bypass provides an auditable local assembly and reporting hook only.
