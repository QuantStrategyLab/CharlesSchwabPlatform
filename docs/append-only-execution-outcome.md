English | [简体中文](append-only-execution-outcome.zh-CN.md)

# Schwab Append-Only Execution Outcomes

Before submitting an order, Schwab reserves an atomic claim marker as the sole duplicate-order guard. After the execution cycle returns, the platform calls QuantPlatformKit's `record_outcome` to write a create-only terminal record under a separate `execution_outcomes/` path; it never overwrites the claim marker.

The Cloud Run service account therefore needs only object read and create permissions. Do not grant broad delete or object admin permissions to overwrite markers. If an outcome already exists, recording returns safely and the claim continues to block duplicate submission for the same signal.

## Offline fault acceptance

Results come from synthetic offline runs of `application/offline_fault_acceptance.py` tagged `research_only`, `offline`, and `no_account_connection`. The table records existing seam capabilities and gaps only; it does not demonstrate deployment, account connectivity, or live-cycle success.

| Scenario | Classification | Actual calls | Limitations |
|---|---|---|---|
| Response lost after accepted submit | `verified-safe-reject` | `claim_execution_marker`, `execute_rebalance_cycle`, local `record_outcome` | No broker order ID is available, so the same order identity cannot be invented for follow-up queries. The second claim fails and second submission count is 0. Does not prove network exactly-once delivery. |
| Partial fills, cancel requests, late or duplicate reports | `unsupported` | `collect_read_only_reconciliation_observations` | `recent_executions_complete` is false, and `filledQuantity` is not a timestamped fill stream. `PENDING_CANCEL` stays in open orders and does not mean canceled. Per-fill fees and duplicate fill deduplication cannot be proven. |
| Restart around claim persistence | `verified-safe-reject` | Local `claim_execution_marker`, repeat claim in the same directory after restart, then `collect_read_only_reconciliation_observations` | Claims have no TTL. Before reconciliation completes, submissions remain 0 and unresolved claims are not cleared as unfilled. This does not mean the order itself recovered. |
| Query 429, disconnect, unknown | `verified-safe-reject` | `fetch_managed_snapshot` account retries, `build_order_status_fetcher`, `execute_rebalance_cycle` | Bounded retries cover account snapshots only; order-status 429 queries run once. Disconnect leaves no cash snapshot, and unknown returns do not coerce quantities to 0. `unknown_pending_orders` keeps new-risk submissions at 0 for that cycle. |
| Multi-owner on shared `BOXX` | `unsupported` | Reconciliation observations and position/cash accounting facts | Net positions reconcile aggregate quantity only; facts carry no owner. Cash is recorded once and cannot be split across multiple owners. |
| Risk pause | `supported` | `execute_rebalance_cycle`, read-only order query after pause | Separates the request, `NEW_RISK_PROHIBITED` in execution logs, and 0 new-risk submissions. No cancels or market liquidations are issued. Sells are outside this gate's prohibition; this scenario places holdings at the scaled target. Not a deployed production pause switch. |
