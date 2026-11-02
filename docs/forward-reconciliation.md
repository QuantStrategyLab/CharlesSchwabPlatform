# Forward account qualification

From November 3, 2026 at 00:00 America/New_York (05:00 UTC), the archived
account-facts projector adds `forward_reconciliation` to its local result. This
is an informational accounting qualification, with no trading admission, risk,
recovery, baseline reset or notification action. Earlier reports retain their
existing projection. No historical collection is required.

The current source cannot prove full-account reconciliation. The overall result
and each incomplete dimension remain `UNRECONCILED`, with `complete=false`.
A missing opening remains `opening_snapshot_missing`; a later observation keeps
its actual timestamp and does not prove an opening at midnight. This projector
does not store or establish an opening baseline.

## Inventory

`broker_reported_positions_scope=stocks_only` identifies a stock projection,
including an explicitly empty stock list. `broker_reported_positions_complete`
is now always false for this scope. Legacy archived stock observations with a
boolean true flag remain readable, but the output flag is false. New false-flag
observations also require `broker_stock_inventory_status=available`; unknown
stock observations do not establish an empty stock list. Consumers must
inspect scope and must not infer an empty full account from empty stocks.

Same-response observations and the existing read-only inventory collector also
retain `broker_native_position_facts`: native asset type, symbol, long quantity,
short quantity and their difference. Known non-stock asset types are retained
without treating them as execution positions. Missing or malformed facts produce
null, never zero. The list is bounded to 512 rows; quantities use the existing
bounded decimal parser. These private report facts carry no full-account coverage
claim, price, currency inference or historical cost requirement. Instrument
identity beyond the native symbol and all instrument semantics remain unqualified.

## Events

The existing raw `account_external_flows` report remains the source of native
activity IDs, types, amounts, transfer items and settlement metadata. It is not a
cash and quantity ledger. Native correction links, corporate actions, FX,
in-kind movements and settlement legs have not been qualified at this boundary.
The qualification reports every ledger category as `UNRECONCILED`, independently
of `externalFlow.windows[].coverage` or a caller's `source_complete` assertion.
Neither field proves complete pagination or a complete daily ledger.

A usable informational window receipt requires exact account binding, USD,
a list shorter than 3000 raw rows, an explicit caller receipt and a positive
window of at most two days ending no later than the account observation. It
must start on or after the forward start. The receipt retains the exact timezone
aware window and row count; pagination remains `NOT_PROVEN`. Missing or invalid
receipts produce fixed reasons without echoing raw identifiers or exceptions.
No broker query, pagination loop or historical backfill is added.

## Cash and time

Native `cashBalance` is carried only after the existing cash-specific source and
independent USD declaration checks. Available trading cash, withdrawal cash,
buying power and sleeve cash never substitute for it. Empty projected cash is
unknown. Settled cash, unsettled cash and full per-currency coverage remain
unqualified, even when native USD cash is available.

The pinned QuantPlatformKit adapter maps `currentBalances.cashBalance` to
`broker_cash_balance` and tags it `cashBalance`. Its snapshot timestamp is local
observation completion time, not a broker source timestamp. The adapter also
preserves a `client_observation_window` in `schwab_position_quantities`.
The observation carries this interval only when its digest matches the snapshot
response digest and its completion equals the snapshot timestamp. The report
projector additionally checks that it lies inside the runtime report interval.
The qualification preserves that observation time, derives a separate New York
reporting date, and carries the runtime report start/end explicitly as report
times. Source as-of stays null. Collection start/end are retained only for that
validated client window; missing or inconsistent windows stay null. It does not manufacture
a zero-duration collection interval. Legacy history envelope timestamp fields
retain their existing point-observation meaning.

`prepare_sync_payload` excludes `forward_reconciliation` and the private native
position facts. The existing receiver schema and publication allowlist are
unchanged. This change does not establish runtime adoption, authentic opening,
financial matches, scheduling or notification delivery.
