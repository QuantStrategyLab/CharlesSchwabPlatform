# Schwab Trinity Strategy Bot

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Platform](https://img.shields.io/badge/Broker-Charles%20Schwab-00a0df)
![Strategy](https://img.shields.io/badge/Strategy-Trinity%20Hybrid-orange)
![GCP](https://img.shields.io/badge/GCP-Cloud%20Run-4285F4)

Automated trading service for Charles Schwab accounts, deployed on GCP Cloud Run. Allocates capital across three layers: **attack (TQQQ)** driven by QQQ MA200 + ATR bands with staged exits, **income (SPYI / QQQI)** when equity exceeds a threshold, and **defense (BOXX)** for idle cash. Each run fetches data, computes targets, places orders, and optionally notifies via Telegram.

---

## Logic overview

- **Data**: QQQ (signals), TQQQ, SPYI, QQQI, BOXX; daily. Indicators: 200-day SMA (MA200), 14-day ATR%.
- **Bands (QQQ)**: `entry_line = MA200 * (1 + f(ATR%))`, `exit_line = MA200 * (1 - g(ATR%))`. TQQQ size and exits are driven by QQQ vs these levels.

---

## Attack layer (TQQQ)

- **Instrument**: TQQQ (3x Nasdaq).
- **Size**: `agg_ratio` from `get_hybrid_allocation(strategy_equity, qqq_p, exit_line)`; applied only to strategy-layer equity (total minus income layer).
- **Rules** (when holding TQQQ):
  - QQQ &lt; exit_line → target TQQQ = 0 (full exit).
  - exit_line ≤ QQQ &lt; MA200 → target = agg_ratio × 0.33 (staged reduction).
  - QQQ ≥ MA200 → target = agg_ratio (full allocation).
- **Entry**: If not holding TQQQ and QQQ &gt; entry_line → target = agg_ratio.
- **Orders**: Sell TQQQ via market; buy TQQQ via limit at ask × 1.005.

---

## Income layer (SPYI / QQQI)

- **Purpose**: Dividend/income allocation when equity is large enough; not used for strategy-layer sizing.
- **Instruments**: SPYI (S&P 500 income), QQQI (Nasdaq income).
- **Activation**: `get_income_ratio(total_equity)` is 0 below `INCOME_THRESHOLD_USD` (default 100000); ramps to 40% by 2× threshold; capped at 60% above that.
- **Split**: `QQQI_INCOME_RATIO` (default 0.5) → QQQI share = income_ratio × QQQI_INCOME_RATIO, SPYI = remainder.
- **Rebalancing**: Targets are enforced each run; excess SPYI/QQQI is sold when above target.

---

## Defense layer (BOXX and cash)

- **Instrument**: BOXX (short-duration / cash-like).
- **Reserve**: `CASH_RESERVE_RATIO` (default 5%) of strategy equity is kept as cash.
- **Target**: Strategy equity minus reserve minus target TQQQ; surplus buying power after SPYI/QQQI/TQQQ orders is used to buy BOXX (market order) when enough for 2+ shares.

---

## Rebalance and orders

- **Frequency**: One full cycle per HTTP request (e.g. Cloud Scheduler on trading days).
- **Threshold**: Trades only when |current_mv − target_mv| &gt; 1% of total equity per symbol.
- **Order types**: Limit buy at ask × 1.005 for TQQQ/SPYI/QQQI; market for BOXX buy and all sells.
- **Notifications**: Telegram sent only when at least one order is placed; otherwise log line only.

---

## Deployment and environment

| Variable | Description |
|----------|-------------|
| `SCHWAB_API_KEY` | Schwab API key |
| `SCHWAB_APP_SECRET` | Schwab API secret |
| `TELEGRAM_TOKEN` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Telegram chat ID |
| `GOOGLE_CLOUD_PROJECT` | GCP project ID |
| `INCOME_THRESHOLD_USD` | Equity threshold to enable income layer (default 100000) |
| `QQQI_INCOME_RATIO` | QQQI share of income layer, 0–1 (default 0.5) |

Deploy as a Cloud Run service and trigger the root URL on a schedule (e.g. once per trading day). Entry point: Flask route `"/"` in `main.py`.
