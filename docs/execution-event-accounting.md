English | [简体中文](execution-event-accounting.zh-CN.md)

# Schwab Execution Event Accounting (Offline)

This document covers the offline execution-event accounting workstream. `application.execution_event_accounting.ExecutionEventLedger` is a local offline ledger that accepts normalized incremental fill events; it does not connect to a broker, parse native order reports, or wire into live order routing.

## Internal offline contract

Order intents first persist an `intent_id -> order_id -> owner_id / symbol / side` binding. When the order identity is temporarily unknown, an internal intent can still be stored, but it continues rejecting safely and never guesses a broker order by symbol, quantity, or approximate time. Callers may bind the order identity explicitly only after exact query evidence confirms it.

Every recorded fill event must provide a stable `event_id`, bound `order_id`, positive incremental `quantity` and `price`, explicit non-negative `fee` with a per-fill `fee_source`, `event_time`, and `event_type=FILL`. Cumulative fees, order-level commissions, and sources that do not specify per-fill scope are rejected rather than prorated across fills. An event digest is computed as a SHA-256 hash over canonical JSON encoding. `owner_id`, `symbol`, and `side` are taken from the persisted intent and rejected if incoming fields conflict. Replaying an identical event is idempotent; reusing an `event_id` with a different digest quarantines and rejects the event. Missing fees or fee sources remain unknown and are rejected; correction and reversal event types are recorded as `unsupported` rather than booked as ordinary fills.

Cumulative order `filledQuantity` is used only to reconcile against the sum of recorded incremental fills and is never converted into fills; out-of-order order updates keep a high-water mark and reject decreases. `PENDING_CANCEL` remains non-terminal. When a terminal cumulative fill count exceeds (or differs from) recorded incremental fills, the order status becomes `incomplete` and reservations remain `pending`, re-reconciling as later fills arrive. Same-ID digest conflicts quarantine both the event and the order, keeping the order `incomplete` and retaining its reservation without rewriting already recorded fills. Confirmed cancellation never erases previously proven fills. Fills, fees, owner allocations, and pending settlement entries are each recorded once per event; pending settlement entries indicate unsettled activity and do not represent available cash or settled balances.

The ledger persists intents, order states, events, quarantined events, owner accounting, and account totals in a deterministic JSON snapshot. Intent reservations with unknown order identity stay attributed to the original owner and count toward total account reservations; once an exact order ID is bound, the reservation moves from the unbound entry to the order entry without double counting. Each owner's reservation accumulates independently and account total reservations equal their sum; one owner's order never draws on another owner's reservation. Snapshots are written to a temporary file, flushed with `fsync`, swapped atomically with `os.replace`, and followed by a directory sync. Restarting from the snapshot restores state and keeps identical event replays idempotent. This proves local snapshot and ledger idempotency only; it does not prove network exactly-once delivery, cross-host fencing, or broker recovery.

## Fixed acceptance

`scripts/run_offline_execution_event_acceptance.py` runs actual ledger methods against six synthetic test cases and wires E06 into the existing local `offline_fault_acceptance` seam:

| Case | Path and inputs | Result |
|---|---|---|
| E01 | Register intent -> incremental fill -> `PENDING_CANCEL` -> late fill -> `CANCELED`; replay same event | Two fills totaling 3 shares, principal 299.00, fees 0.60, cash economic delta -299.60; duplicate replay is not double-booked |
| E02 | Atomic snapshot -> load in a new ledger instance -> replay same event | Pre- and post-restart snapshots match, replay is flagged duplicate, economic state is unchanged |
| E03 | Two owners each holding independent buy and sell `BOXX` orders | Sum of owner positions, fees, cash, and pending settlement entries equals account totals; each event appears once |
| E04 | Unknown order identity on response -> intent stays without broker ID and records no fills -> supply exact synthetic order identity and fill details | Exact-identity path reconciles in the event ledger; retry suppression stays verified via the existing E06 claim/outcome path |
| E05 | Terminal report quantity exceeds fill details -> same-ID digest conflict -> `REVERSAL` | Unknown fills are not fabricated and reservations stay pending; conflict is quarantined and corrections/reversals are rejected |
| E06 | Run existing offline response-loss, claim-restart, safe-query, and risk-pause entrypoints | Preserves prior claim/outcome, risk, and query behavior; funding regressions are verified via the listed pytest commands |

Runner output is marked `research_only=true`, `offline=true`, and `no_account_connection=true`. Cases E01–E05 use synthetic normalized events; E06 risk-pause and claim/query evidence also comes from synthetic local boundaries rather than production acceptance.

## Schwab native capability boundaries

| Capability | Status | Provable scope |
|---|---|---|
| Order queries, exact order-ID lookup, status enums | `documented_surface` | Public API docs and local `schwab-py 1.5.1` expose order lookups and statuses; supports order query and status inspection only |
| Stable native per-fill ID | `unsupported_unverified` | Not proven by available materials; synthetic unique IDs cannot be fabricated from price, quantity, and timestamp |
| Per-fill fee | `unsupported_unverified` | Cumulative commission does not establish per-fill fees; missing explicit per-fill fees must not be recorded as zero |
| Native correction / reversal fields | `unsupported_unverified` | Native fields are unverified; the offline ledger rejects these event types |

Execution-event accounting is therefore `implemented_offline`; native Schwab per-fill parsing and native fee booking remain unverified. Order-query surface availability does not close the fill-stream or fee evidence gaps, recover unknown orders, or enable trading.
