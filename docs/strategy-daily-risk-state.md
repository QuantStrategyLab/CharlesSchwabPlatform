# Daily strategy risk state

The strategy runtime accepts an optional caller-supplied daily state binding. The
binding identifies the strategy, logical account scope, frozen candidate and
configuration, durable state store, effective session, frozen input digest and
expected predecessor session. A first transition additionally requires a
bootstrap binding independently authorized by the caller. This interface does
not derive these values from runtime configuration or establish authorization.

The runtime reads and validates the scoped chain and confirms a proposed state
transition before using the persisted head. Missing, malformed, conflicting or
unavailable state cannot authorize a new risk increase. Without an execution
fence that binds the state and intent to broker submission, state enrollment
preserves independently valid reductions and does not authorize risk increases,
even after the state transition is persisted. Default runtime configuration does
not enroll this binding. Configurations using a distinct volatility exit
threshold and partial retention require durable state enrollment before relying
on stateful increases; no migration is implied by this interface.

An explicitly supplied binding requires the supported daily state strategy
contract and the `tqqq_growth_income` entry point. Other profiles are rejected
before strategy evaluation; they do not consume this TQQQ-specific state.
If the installed strategy package lacks that capability, evaluation
stops before calling the strategy. Passing an unavailable-state marker to a
legacy entry point that does not interpret it is not a valid fallback. The
default path without a binding does not import the candidate contract.

Each cycle uses one UTC whole-second decision clock for supported plugin and
risk-gate checks. The trading session remains a separate value. Independently
supplied expected artifact digests are matched to exact references; absent
bindings remain unverified. The decision receipt records actual pre- and
post-gate target digests when the shared runtime supports them, and a separate
partial link records an actual execution receipt when available. An artifact
read time does not prove that it was published or available before the decision.
Receipt observation failures identify the source, candidate write, archive
read, projection, coverage, archive write or recovery stage without exposing
private source paths. A healthy receipt keeps its original business-run identity.

For Schwab submission, acceptance requires a canonical broker order ID. If an
HTTP status is supplied with acceptance, it must be the integer 200 or 201.
Rejection is confirmed only by an integer HTTP status from 400 through 499,
excluding 408. Missing rejection status, or invalid or contradictory status
evidence, requires reconciliation; unresolved continuity prevents replay.

The execution link may include a digest of broker-identified order records
observed in the completed strategy cycle. Missing, incomplete, duplicate,
unknown-outcome and dry-run records remain unknown. An order acknowledgement
or a filled-order status is not an individual fill: fill evidence is included
only when explicit, uniquely identified execution records are present. The
link remains partial provenance and does not prove end-to-end causality.

### Enrolled account journal submission

The optional joint path requires an independently enrolled account journal and a previously established equity baseline. The caller supplies a canonical account snapshot and separately approved per-order risk facts keyed by the journal order digest. These inputs do not authenticate their own origin. The portfolio consumed by the strategy must match the account observation in account, source digest, currency, valuation time, equity, cash and complete position quantities. Missing or inconsistent evidence blocks submission. The context digest is checked against the authoritative strategy transition before planning and again before reservation. The mapped allocation is bound to the gated decision and checked at the submission boundary.

The journal serializes state changes and order admission; pending or unknown outcomes remain blocking across session reconstruction. It cannot make broker submission atomic or establish missing broker fills. Enrollment, account identity, source verification and policy approval are external prerequisites. If a subsequent account risk adjustment changes the mapped allocation, the current binding denies that submission and requires a new evaluation.

The platform validates the submission response before the journal records it.
Transport exceptions, HTTP 408 or server errors, inconsistent response statuses,
and acknowledgements without a valid broker order identifier remain unknown.
Schwab identifiers must be positive integers or their canonical ASCII decimal
strings. Malformed identifiers remain in the reported evidence for reconciliation
and never establish a confirmed acknowledgement. This
validation also applies to adapters that previously labelled every HTTP error
as a rejection. The cycle stops with reconciliation pending; reconstruction of
the session does not release the original order reservation.

The receipt extractor uses the same order-identity rule as submission. An
adapter result with an `accepted` label and a malformed identifier cannot
establish complete order evidence, even if its cycle summary claims submission
is done. Integer and string forms of the same canonical identifier are treated
as duplicates. Rejected evidence remains in the original runtime result for
reconciliation; extraction does not alter broker facts or manufacture fills.

## Offline source-evidence checks

`scripts.historical_source_evidence.validate_source_evidence(document)` checks
supplied source fields and declared associations without importing the runtime,
reading files or contacting a broker. It accepts an already decoded JSON object
with `schema_version: qsl.schwab.source_evidence.v1`. The result separates local
field consistency from historical acceptance:

| Result field | Meaning |
| --- | --- |
| `status` | `FAIL` for an observed contradiction or malformed input; otherwise `BLOCKED`. |
| `local_consistency` | `CONSISTENT` when the supported supplied fields agree, `BLOCKED` for missing or unsupported evidence, or `FAIL` for an observed contradiction. |
| `source_authentication` | Always `UNVERIFIED`; matching declared hashes or aliases does not authenticate their origin. |
| `execution_authorized`, `production_ready`, `historical_parity_established` | Always `false`, including a locally consistent result. |

`findings` contains fixed reason codes and bounded source categories, without
echoing payload values, object locations or account identifiers. `unresolved`
keeps original source bytes, account identity, deployed release, input
availability, complete run coverage, individual fills, state continuity and
order mapping explicitly unverified. Caller flags such as `passed`, `verified`,
`complete` and `execution_validated` cannot remove these limits.

### Supplied document and source fields

`binding` declares `run_id`, `strategy_profile`, logical `account_scope`,
`platform: charles_schwab`, `execution_environment`, `execution_mode`, the cycle's
boolean `dry_run`, `record_kind`, `server`, `lifecycle_id`, `account_alias`,
`release_id` and an aware `as_of` observation cutoff. `record_kind` distinguishes
`execution`, `reconciliation` and `diagnostic`; it is separate from the runtime
environment. An alias has the form `acct_` followed by 16–64 lowercase hexadecimal
characters. These labels are declared associations, not physical account proof.

`binding.release` contains the supplied release identity: `release_id`,
`strategy_revision`, `effective_session`, `manifest_sha256`, `config_sha256`,
`risk_policy_sha256`, `evidence_sha256` and `plugin_bundle_sha256`. A supplied
state transition also requires `binding.candidate_id` and `binding.input_sha256`.
The checker compares the report's `runtime_loaded_receipt.v1` identity with this
declaration. `self_attested` remains a runtime claim; it does not establish which
release was deployed or authenticate source code.

`records` contains at most one envelope of each supported kind. Every envelope
has `kind`, `source_id`, `runtime_source_id`, `association`, `producer_sha256`,
`payload`, `payload_sha256`, `event_at`, `available_at`, `observed_at` and
`normalized`. `association` repeats the binding identity fields through
`release_id`. `source_hashes` declares the expected producer digest by kind.
`payload_sha256` is SHA-256 of the supplied JSON with sorted keys, compact
separators, ASCII escaping and non-finite numbers prohibited. It covers the
supplied view, which may have been redacted; it is distinct from a protected
original object's digest. Declared source hashes and locators are never fetched
or treated as independently verified originals.

| Kind | Native fields checked | Boundary |
| --- | --- | --- |
| `runtime_report` | `runtime_report.v1`, platform, run/profile/logical scope, cycle `dry_run`, `runtime_target`, loaded release identity, start/decision/finish clocks | Required association anchor. Report status and counts do not prove executions or coverage. |
| `execution_receipt` | Exact nine-field `qsl_execution_receipt.v1` wire, content-derived receipt ID, profile/revision/mode, outcome/confirmation pair, observation clock | The native receipt has no `run_id`, account identity, order rows or fill rows. Its run association belongs only in the envelope. |
| `order_request` | The supplied single-leg equity dollar market-buy request: `quantityType: DOLLARS`, quantity, symbol and instruction | Supports the explicit QuantPlatformKit dollar-order builder. Other request builders, including SDK share-order serialization, remain unsupported. |
| `submission_response` | Supplied `ExecutionReport` fields: `status`, `broker_order_id`, symbol, side, quantity and `raw_payload.status_code` | This is the adapter's reported response, not independently authenticated HTTP bytes or an individual execution. |
| `submitted_orders` | Supplied cycle rows: status, canonical ID representation, symbol, side, quantity and optional `notional_usd` | Duplicate canonical IDs fail. Cross-source checks cover one explicitly selected row; they do not establish complete order coverage. |
| `daily_state_transition` | `strategy_risk_state_transition.v1` wire/hash, scoped identity, session/input digest and supported `tqqq_daily_state.v1` fields | No predecessor-chain replay, bootstrap authorization or execution fence. An unknown schema or SOXL state adapter remains blocked. |

No native schema is invented for request, response or cycle rows that do not
carry one. If both a report's embedded execution receipt and a standalone
receipt associated with that report are supplied, their observed payloads must
agree. A conflicting receipt fails; a missing embedded observation is not
manufactured from the envelope. For an order request, `normalized` supplies `symbol`, `side`, decimal
`quantity` and `quantity_unit`. For a submission response it supplies
`broker_order_id`, decimal `quantity`, `quantity_unit`, `outcome` and boolean
`reservation_released`. For cycle rows it selects `broker_order_id` and supplies
decimal `quantity` and `quantity_unit`. Other supported kinds use an empty
`normalized` object. If any of the three order kinds is supplied, all three are
required for the cross-source check. Missing source fields stay missing; the
envelope does not insert them into a native record.

### Environment, quantity and outcome semantics

The native `runtime_target.execution_environment` and `dry_run_only` fields are
required for local environment checks. Legacy `execution_mode` equals `paper`
when the target is dry-run-only and `live` otherwise. A broker Paper target may
therefore carry `execution_mode: live`. The receipt retains that legacy field;
it cannot establish the actual account environment. The cycle's `dry_run` remains
separate because a Schwab cycle can receive an explicit dry-run override.
Server, lifecycle and account alias associations remain externally unverified.
No IBKR Gateway account rules or Schwab Paper execution adapter are implied.

Schwab `quantityType: DOLLARS` and a cycle row's `notional_usd` identify amounts,
not share counts. The explicit dollar builder emits at least USD 1.00 with at
most two decimal places. Its transmitted amount can differ from the adapter's
reported unrounded quantity: the checker compares the request with the recorded
`notional_usd`, and the response with the cycle's recorded `quantity`. It does
not calculate target sizing, round a new order, infer shares from a price or
validate settlement. Native numeric quantities must be finite positive JSON
numbers; normalized quantities are positive decimal strings with at most
12 fractional digits and a maximum of 10^18. Booleans are not quantities.

HTTP 200/201 with a valid supplied ID supports an acknowledgement only. HTTP
408, server errors and absent transport outcomes remain unknown; they cannot
support a rejection or reservation release. A contradictory source or normalized
label fails. A transport exception without a captured response remains missing
evidence. Even a correctly paired `filled` receipt label remains blocked without
individual execution records; this checker implements no raw fill adapter.

`redaction.order_id_representation` declares `synthetic`, `preserved`, `replaced`
or `unknown`. `redaction.original_order_id_state` declares `unknown`, `canonical`
or an observed `invalid` state. Canonical numeric checks apply only to the
supplied representation. A replaced or unknown ID remains locally blocked even
when its replacement is a valid decimal string and the caller claims the
original was canonical. An observed invalid original remains a failure after
replacement. A `preserved` declaration still does not authenticate the original
bytes or the account/order association. The checker never reads a redaction map.

Envelope clocks must satisfy `event_at <= available_at <= observed_at <= as_of`.
The report event matches native `finished_at`; the receipt event matches native
`observed_at`. The cycle establishes its whole-second decision clock before
report construction; `started_at` and release `loaded_at` may therefore be
later, including across a second boundary. Start, decision and load timestamps
must still be no later than the report finish. The terminal fallback path can
create a receipt after the report has been finalized. Such a receipt retains
`BLOCKED` timing association instead of being classified as an impossible
source sequence; caller flags cannot remove that uncertainty.
Order events are checked in request/response/cycle order within
the supplied decision-to-finish interval. Other envelope timestamps remain
declared observations, not newly manufactured native fields. These comparisons
do not prove when historical strategy inputs became available.

The function preserves the input and returns structured failures for malformed,
non-finite, oversized, cyclic or excessively nested values. Synthetic examples
and counterexamples are maintained in
[`tests/test_historical_source_evidence.py`](../tests/test_historical_source_evidence.py).
They validate this offline contract only; they do not constitute a production
replay or permission to export, deploy or execute orders.
