English | [简体中文](digest-candidates-producer.zh-CN.md)

# Schwab DIGEST_CANDIDATES Producer

Repository: `CharlesSchwabPlatform`  
Central consumer: `QuantRuntimeSettings` (`daily_digest_aggregator` / Environment `runtime-strategy-switch`)

## Goal

Generate central daily-digest candidate JSON from verified Schwab runtime daily projections (and optional account-facts), and pass it into `QuantRuntimeSettings` through protected channels without uploading public Actions artifacts by default or modifying the `QuantRuntimeSettings` production Environment automatically.

## Candidate contract

The optional `block_reason` object carries `reason_code`, `observed_value`, `limit`, and `order_submitted_this_run`. Consecutive-loss observations and limits come from completed risk-guard diagnostics instead of setting separate thresholds in notifications; unrecognized or inconsistent reasons remain unknown. `order_submitted_this_run` is attached only when the latest actual run has a valid risk-block receipt proving no submission, no pending orders, and no errors; it does not prove zero orders or fills across the full day. Older reports do not fabricate counts or limits, and older candidates continue working as before. This object applies only to notifications; runtime daily receiver contracts, trading decisions, capital settings, and dependency pins do not change.

`platform_id` is fixed to **`schwab`** (not `charles_schwab`).

Minimal sanitized example (synthetic placeholder values):

```json
{
  "schema_version": "qsl.digest_candidates.v1",
  "runs": [
    {
      "platform_id": "schwab",
      "strategy_profile": "soxl_soxx_trend_income",
      "opaque_account_uid": "acct_opaque_example",
      "target_id": "schwab/example-target",
      "actually_ran": true,
      "fill_count": null,
      "order_count": null,
      "cycle_count": 1,
      "field_status": {
        "fill_count": "counts_unknown",
        "order_count": "counts_unknown",
        "cycle_count": "known"
      },
      "evidence_provenance": "candidates",
      "reason_code": "schwab_fills_not_connected",
      "signal_summary": "no_signal",
      "rebalance_kind": "no_rebalance",
      "status": "ok"
    }
  ],
  "producer_status": "projected",
  "producer_reason": ""
}
```

### Field sources and unknowns

| Field | Source | When unknown |
| --- | --- | --- |
| `platform_id` | Fixed `schwab` | — |
| `strategy_profile` | Daily `record.target.strategy_profile` | Falls back to documented default `soxl_soxx_trend_income` (sole target) |
| `opaque_account_uid` | Protected env / binding `account_hash` (already opaque) | Empty string + `opaque_account_uid_absent` |
| `target_id` | `SCHWAB_DIGEST_TARGET_ID` or `SCHWAB_ACCOUNT_FACTS_TARGET_ID` | Empty string + `target_id_absent` |
| `actually_ran` | `true` only when a covering run has activity | No activity -> no run emitted |
| `fill_count` / `order_count` | Daily `fills`; currently `source=not_connected` | Must stay `null` + `counts_unknown`; never write `0` |
| `cycle_count` | Number of covering runs | — |
| `equity` | Optional account-facts `broker_reported_balances[].net_assets` (read-only archive projection; selector accepts legacy `["live"]` or native `[account_hash]`) | Omit field; do not guess. Does not require covering runs: an `actually_ran=false` equity row can still be emitted |
| `holdings` + `holdings_scope` | Runtime `summary.account_observation.broker_reported_positions` (strategy symbols only, `broker_reported_positions_scope=strategy_symbols_only`) -> archived facts projection -> candidate `holdings` (float) + `holdings_scope=strategy_symbols_only` | Missing, errored, malformed, or negative positions -> omit both fields (never emit an empty list); strip before sending account-facts sync payloads to the console (exact-key contract) |
| `signal_summary` / `rebalance_*` | Mapped from run `activity` | No activity -> omit row |

## Local projection (synthetic or existing JSON)

```bash
python3 scripts/project_digest_candidates.py \
  --daily-projection /path/to/daily-projection.json \
  --account-facts /path/to/optional-account-facts.json \
  --opaque-account-uid 'acct_opaque_example' \
  --target-id 'schwab/example-target' \
  --output /tmp/schwab-digest-candidates.json
```

Stdout prints only safe summary counts (`status`, `runs`) with no UID, target ID, or equity figures.

Unit tests:

```bash
python3 -m pytest tests/test_project_digest_candidates.py -q
```

## Workflow (optional, off by default)

`.github/workflows/runtime-daily-sync.yml` adds the `emit_digest_candidates` input (default `false`).

When enabled, it runs `scripts/emit_digest_candidates.py` outside the prepare/publish success path:

- Reuses the existing WIF identity, report prefix, and runtime target
- Writes ephemeral output to `$RUNNER_TEMP/schwab-digest-candidates.json`
- Equity: calls `project_schwab_account_facts_history` read-only against the same archive (never POSTs account-facts or calls the broker); on success writes `$RUNNER_TEMP/schwab-digest-account-facts.json` and populates candidate `equity`
- Also accepts an explicit `SCHWAB_DIGEST_ACCOUNT_FACTS_PATH` pointing to an existing facts JSON (takes precedence over archive projection)
- Never calls `actions/upload-artifact` by default
- Prints only safe summary fields (`equity_present`, `runs`, `fill_count_null`, `account_facts_source`, `identity_*_present`) with no UID or monetary amounts

Required protected configuration (never commit to a public repository):

| Name | Purpose |
| --- | --- |
| `SCHWAB_DIGEST_OPAQUE_ACCOUNT_UID` | Optional; defaults to binding `account_hash` |
| `SCHWAB_DIGEST_TARGET_ID` | Primary target ID; falls back to `SCHWAB_ACCOUNT_FACTS_TARGET_ID` |
| `SCHWAB_ACCOUNT_FACTS_SERVICE_NAME` | Cloud Run service name used for archive equity projection |
| `SCHWAB_NET_ASSETS_CURRENCY` | Must be confirmed `USD`; otherwise `equity` is omitted |
| `SCHWAB_CASH_CURRENCY` | Optional; when `USD`, facts may include cash (digest uses `net_assets` only) |
| Existing daily secrets/vars | Same configuration used by `runtime-daily-sync` |

Optional private artifact (off by default; enable only for authorized QRS ingestion, 1-day retention):

```bash
gh workflow run "Schwab Runtime Daily Manual" -R QuantStrategyLab/CharlesSchwabPlatform --ref main \
  -f emit_digest_candidates=true -f capture_digest_candidates_artifact=true
```

Dry-run emit (does not modify the QRS Environment or send Telegram messages):

```bash
gh workflow run "Schwab Runtime Daily Manual" -R QuantStrategyLab/CharlesSchwabPlatform --ref main \
  -f emit_digest_candidates=true
# Check logs for equity_present / fill_count_null / runs; no public artifact is uploaded
```

## Injecting into QRS (manual, no automatic Environment edits)

Central documentation: `QuantRuntimeSettings` `docs/digest-candidates-wiring.md` (and `docs/digest-candidates-wiring.zh-CN.md`).

Recommended steps:

1. Generate candidate JSON from synthetic fixtures or an authorized manual `emit_digest_candidates` run.
2. Confirm the JSON contains no raw account numbers or tokens, and that `fill_count` / `order_count` are `null` rather than `0` (until live fills are wired and verified).
3. Have an authorized maintainer store the full JSON in the `QuantRuntimeSettings` **`runtime-strategy-switch`** Environment secret **`DIGEST_CANDIDATES_JSON`** (or write a file in a private pre-step and set `DIGEST_CANDIDATES_PATH`).
4. Trigger `workflow_dispatch` on `daily-digest-notify.yml` in `QuantRuntimeSettings` (default `dry_run=true`) and check the receipt:
   - `source_coverage.candidates_loaded=true`
   - Schwab run `field_provenance.fill_count` is not verified zero
   - Rendered message states fills are unknown rather than zero fills
5. Only consider disabling `dry_run` after confirming the dry-run receipt; this producer never writes to the QRS Environment automatically.

The ephemeral file can also be synced to private GCS for consumption by a protected job, keeping public artifacts disabled.

## Boundaries with quant and other platforms

- Does not modify LB-HK, Firstrade sync, Cloud Run ingress, production strategies, or risk budgets.
- Does not refresh Schwab tokens or POST account-facts or runtime-daily (the emit path performs prepare, read-only facts projection, and ephemeral local output only).
- Only Schwab is covered here; wiring IBKR belongs in a separate platform pull request if needed later.

## Baseline unknowns

- Whether production `SCHWAB_DIGEST_TARGET_ID` / opaque UID already match the console binding: unknown (for maintainers to confirm); defaults fall back to binding hash and `SCHWAB_ACCOUNT_FACTS_TARGET_ID`.
- Whether same-day archive reports fall inside the account-facts `MAX_AGE` (36h) window: unknown; outside the window `equity_present=false` without fabricating values.
- Daily `zero_run_scope_target_scope` (no covering runs) is decoupled from digest equity: equity comes from account-facts while daily scope is inspected separately.
- Holdings are mapped only when archived facts include `broker_reported_positions`, which appear in new archives after deployment and are omitted beforehand.
- When fills move from `not_connected` to countable: unknown; never emit `0` before that wiring is verified.
- QRS `DIGEST_CANDIDATES_JSON` still requires manual injection; `emit` never modifies the Environment automatically.
