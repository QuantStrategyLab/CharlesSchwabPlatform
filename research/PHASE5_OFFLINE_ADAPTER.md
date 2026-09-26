# Phase 5 Schwab offline research adapter

## Frozen scope and provenance

This slice is limited to a research-only, pure local conversion and feasibility
check for the already frozen QQQM/TQQQ/BOXX Phase 4 v3 candidate. It changes no
Schwab execution, account, quote, release, risk or production configuration
path. It does not grant paper, shadow or live authority.

The exact write whitelist is:

- `research/phase5_offline_adapter.py`: one pure local adapter, with no runtime registration;
- `tests/test_phase5_offline_adapter.py`: direct synthetic and opt-in approved-local-input checks;
- `research/PHASE5_OFFLINE_ADAPTER.md`: this contract and evidence.

Base: CharlesSchwabPlatform `origin/main` at
`baff23153ed1e08dee77f3231287ad599d869e1f`. Locked dependencies in
`uv.lock`: UES `4a3943883cd6b5bbfe32a559e56a91b40a81b7ce`, QPK
`c7646a7168b3dafa763ef7751a182d23e8de7790`. The fixed research
consumer is the separate UES worktree at
`aa555fa7cf04d85805de6301de468884755ccdde`; its unmerged research code is
not substituted into the platform lock. The frozen Phase 4 v3 policy SHA256 is
`95d19f727a5e7cae1d9ab5a4251d260acca0f5370ef075c85312a06887e2e20e`.
The source manifest SHA256 is
`cb14a511083c824a748d137a271c93cfe0e8adf38f648905b26e37decf4c6182`.

`origin/main` CI runs on pull requests and pushes to `main` only. Deployment,
environment sync and runtime lifecycle workflows require explicit dispatch;
the Dependabot auto-merge gate is restricted to non-draft Dependabot PRs.
The planned research branch and draft PR therefore do not trigger deployment,
release or automatic merge under the inspected workflows.

## Supported comparison contract

The comparison is between a **research target**, a **pure offline proposed
whole-share change**, and the research ledger's separately produced first-open
simulated fill. A proposal is not an order submission or broker fill. The
first signal is 2023-03-27 and the next common-session open is 2023-03-28;
the eight predeclared Phase 5 scale paths at 10 bps per executed side are the
initial real-input check. No result is selected by return.

The initial supported domain is long-only, cash-only initial holdings with
QQQM/TQQQ/BOXX, no option or collateral obligations, no prior pending sale or
dividend claim, whole-share quantity, 1% outer cash target, and TQQQ member
budget retained for the member. The adapter must keep BOXX as a target ceiling
and distinguish its shares from settled cash. It must charge the declared
10 bps once per executed buy or sale, in the declared TQQQ → QQQM → BOXX
purchase order. It must not spend member reserve, receivable or future sale
proceeds. Unsupported state returns an explicit status; it is not silently
treated as an empty account. Additional pure states may be supported only when
their account identity and event timing can be checked directly.

The input state is immediately before the execution open, after effective
company actions have been applied. The adapter may release an explicitly
dated pending sale at its due common-session index, but it does not infer
corporate actions or the historical known-at time of a dividend. Unpaid
receivables remain separate from settled cash; recognized TQQQ receivables
affect the member-reserve calculation only. A paid but restricted amount is
already part of settled cash and must not be counted twice.

Direct acceptance cases: eight frozen first-open paths; no action; one-share
funding boundary; insufficient settled cash; a dated sale pending through the
next session and released at the second subsequent common session; same-symbol
opposing member intents returning HOLD rather than silently netting away gross
costs; repeated pure evaluation; a later quote change that cannot alter the
prior-close target; and explicit HOLD for options and collateral. Dollar
accounting tolerance is `1e-6` USD; share counts must be exact integers. The
complete historical window, production admission and live execution are
outside this slice.

## Starting HOLD evidence

The prior native dry-run mapping used the frozen outer cash target as
`reserved_cash`, but did not retain the TQQQ member's internal cash. The
Schwab cash sweep spent remaining buying power without the research BOXX
target ceiling; the default small-account safe-haven substitution cleared
four small BOXX targets. The 1.005 order-limit premium and the frozen 10 bps
research executed-cost model are distinct. Only one of eight synthetic
first-open dry-run share sets matched the frozen research fills. This is a
mapping diagnosis, not a proof that every local adapter is incompatible.

## Preimplementation Astra decision

Astra independently reviewed the fixed UES contract and existing Schwab pure
calculation seams without private data. It returned GO for a pure single-step
target-to-feasible-proposal adapter using the frozen open-price, whole-share,
10 bps and funding-order semantics. Existing paper value proposals are
fractional, while Schwab's execution quantity helper uses the separate 1.005
limit-price reserve and top-up semantics; neither is authoritative for the
research fill. The adapter may report these as different representations but
must not change the native execution behavior. Opposite gross member trades in
one asset have no frozen ownership-transfer rule and remain unsupported. The
GO covers implementation of a bounded offline research contract, not a
historical or native-execution PASS.

## Implemented offline comparison and direct evidence

`adapt_phase5_first_platform_offline` accepts only the frozen v3 research
candidate IDs and budget policy version. It converts the prior-close USD
targets to whole-share targets at the execution open, then applies the frozen
research funding order: release only due pending sales, sell excess shares into
a T+2 queue, buy TQQQ before QQQM before BOXX from free settled cash, charge
the declared per-side cost, and retain the TQQQ member reserve and outer cash
target. It returns member targets, net account changes, hypothetical fills,
costs, pending claims, post-open cash and shares, and an account wealth
identity error. The output is explicitly an offline research simulation; it
cannot be passed to the production execution cycle as an order.

The approved private manifest and ledger checks were read from the existing
local restricted research copy, with no new data acquisition. Eight frozen
first-open Phase 5 paths at 10 bps, comprising four principal scales and two
candidate paths, matched the research ledger exactly in whole-share account
changes, final shares, settled cash, charged cost and member reserve. One path
reported an explicit funding shortfall. The maximum observed cash, cost and
member-reserve difference across the eight paths was zero USD at the
`1e-6` USD acceptance tolerance; the account identity error was within that
tolerance for every path. This is first-open alignment with the frozen
research ledger, not an independent broker fill or full historical replay.

Six synthetic direct checks additionally cover the fee-inclusive one-share
boundary, repeat evaluation, T+2 proceeds, forbidden same-symbol member
ownership transfer, restricted cash and unpaid receivables, option/collateral
HOLD, and signal/quote date causality. The approved-local check is opt-in and
skips in ordinary CI. Private source rows, raw prices and ledger contents are
not part of this repository or its PR artifacts.

Native Schwab execution mapping remains HOLD: its limit-price reserve,
safe-haven substitution and cash sweep have a different contract. This slice
validates only the named research proposal and hypothetical post-open funding
state. It does not validate option collateral, external cash flows, generic
cross-member ownership transfers, corporate-action reconstruction, the
complete historical window, or real execution authority.

## Incremental independent review

Astra reviewed the implemented adapter against the frozen UES `_execute`
funding contract and returned GO for this bounded draft PR. It found no
blocking funds or time-causality mismatch: due sale proceeds only are released,
new sales settle at the second subsequent common session, TQQQ member reserve
and restricted paid cash remain protected, and BOXX remains capped by the
prior-close research target. Astra separately ran the synthetic suite with no
private-input environment: six passed and the private comparison skipped.
Its review did not independently read or reproduce the eight private paths;
those aggregate results are the direct local check reported above. Native
execution and full historical equivalence remain outside the GO.
