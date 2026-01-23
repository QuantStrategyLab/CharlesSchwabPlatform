import os
import time
import traceback
import requests
import json
import numpy as np
import pandas as pd
import pytz
import pandas_market_calendars as mcal
from datetime import datetime
from flask import Flask
import google.auth

from schwab import auth, client
from schwab.orders.equities import equity_buy_market, equity_sell_market, equity_buy_limit

try:
    import google.cloud.secretmanager_v1 as secret_manager
except ImportError:
    from google.cloud import secret_manager

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def get_project_id():
    try:
        _, project_id = google.auth.default()
        return project_id if project_id else os.getenv("GOOGLE_CLOUD_PROJECT")
    except Exception:
        return os.getenv("GOOGLE_CLOUD_PROJECT")

PROJECT_ID = get_project_id()
APP_KEY = os.getenv("SCHWAB_API_KEY")
APP_SECRET = os.getenv("SCHWAB_APP_SECRET")
TG_TOKEN = os.getenv("TELEGRAM_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SECRET_ID = "SCHWAB_TOKENS"
TOKEN_PATH = '/tmp/token.json'

CASH_RESERVE_RATIO = 0.05
INCOME_THRESHOLD_USD = float(os.getenv("INCOME_THRESHOLD_USD", "100000"))
QQQI_INCOME_RATIO = float(os.getenv("QQQI_INCOME_RATIO", "0.5"))


def send_tg_message(message):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": message}, timeout=15)
    except Exception as e:
        print(f"Telegram send failed: {e}", flush=True)


# ---------------------------------------------------------------------------
# GCP Secret Manager and Schwab client
# ---------------------------------------------------------------------------
def get_secret_from_gcp():
    client_sm = secret_manager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
    response = client_sm.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_schwab_client_init():
    raw_data = get_secret_from_gcp()
    with open(TOKEN_PATH, 'w') as f:
        f.write(raw_data)
    return auth.client_from_token_file(TOKEN_PATH, APP_KEY, APP_SECRET)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def check_submission(resp, symbol):
    if resp.status_code in [200, 201]:
        location = resp.headers.get('Location', '')
        order_id = location.split('/')[-1] if location else None
        return True, order_id
    return False, f"{resp.status_code} {resp.text}"


def safe_api_call(response, context="API"):
    if response.status_code not in (200, 201):
        raise Exception(f"{context} failed: {response.status_code} {response.text}")
    try:
        return response.json()
    except json.JSONDecodeError:
        raise Exception(f"{context} invalid JSON: {response.text}")


def get_hybrid_allocation(total_equity_usd, qqq_p, stop_line):
    if total_equity_usd <= 70000:
        target_agg = float(np.interp(total_equity_usd, [0, 15000, 30000, 70000], [1.0, 0.95, 0.85, 0.70]))
    elif total_equity_usd <= 140000:
        target_agg = float(np.interp(total_equity_usd, [70000, 140000], [0.70, 0.50]))
    else:
        if qqq_p <= stop_line:
            target_agg = 0.0
        else:
            risk = max(0.01, (qqq_p - stop_line) / qqq_p * 3.0)
            target_agg = min(0.50, 0.30 / risk)
    target_yield = max(0.0, 1.0 - target_agg)
    return target_agg, target_yield


def get_income_ratio(total_equity_usd: float) -> float:
    """
    Income layer target as fraction of total equity. Not used for strategy-layer sizing.
    Below INCOME_THRESHOLD_USD: 0. Between threshold and 2x: linear to 40%. Above 2x: 60% cap.
    """
    if total_equity_usd < INCOME_THRESHOLD_USD:
        return 0.0
    if total_equity_usd <= 2 * INCOME_THRESHOLD_USD:
        return float(np.interp(
            total_equity_usd,
            [INCOME_THRESHOLD_USD, 2 * INCOME_THRESHOLD_USD],
            [0.0, 0.40],
        ))
    return 0.60


# ---------------------------------------------------------------------------
# Strategy execution
# ---------------------------------------------------------------------------
def run_strategy_core(c, now_ny):
    # Fetch QQQ history
    raw_resp_qqq = c.get_price_history('QQQ',
        period_type=client.Client.PriceHistory.PeriodType.YEAR,
        period=client.Client.PriceHistory.Period.TWO_YEARS,
        frequency_type=client.Client.PriceHistory.FrequencyType.DAILY,
        frequency=client.Client.PriceHistory.Frequency.DAILY)
    resp_qqq = safe_api_call(raw_resp_qqq, "QQQ history")
    if 'candles' not in resp_qqq:
        raise Exception(f"QQQ response missing candles: {resp_qqq}")

    df_qqq = pd.DataFrame(resp_qqq['candles'])
    qqq_p = df_qqq['close'].iloc[-1]
    ma200 = df_qqq['close'].rolling(200).mean().iloc[-1]

    tr = pd.concat([
        df_qqq['high'] - df_qqq['low'],
        abs(df_qqq['high'] - df_qqq['close'].shift(1)),
        abs(df_qqq['low'] - df_qqq['close'].shift(1))
    ], axis=1).max(axis=1)
    atr_pct = tr.rolling(14).mean().iloc[-1] / qqq_p
    exit_line = ma200 * max(0.92, min(0.98, 1.0 - (atr_pct * 2.0)))
    entry_line = ma200 * max(1.02, min(1.08, 1.0 + (atr_pct * 2.5)))

    # Account snapshot (strategy symbols only)
    raw_acct_nums = c.get_account_numbers()
    acct_hash = safe_api_call(raw_acct_nums, "Account numbers")[0]['hashValue']
    strategy_symbols = ["TQQQ", "BOXX", "SPYI", "QQQI"]

    raw_acc_resp = c.get_account(acct_hash, fields=client.Client.Account.Fields.POSITIONS)
    acc_resp = safe_api_call(raw_acc_resp, "Account positions")
    acc = acc_resp['securitiesAccount']
    current_bal = acc.get('currentBalances', {})

    cash_for_equity = float(current_bal.get('cashAvailableForTrading', 0.0))
    raw_withdrawable = float(current_bal.get('cashAvailableForWithdrawal', 0.0))
    real_buying_power = max(0.0, raw_withdrawable)

    mv = {s: 0.0 for s in strategy_symbols}
    qty = {s: 0 for s in strategy_symbols}
    if 'positions' in acc:
        for p in acc['positions']:
            s = p['instrument']['symbol']
            if s in mv:
                mv[s] = float(p['marketValue'])
                qty[s] = int(p['longQuantity'])

    total_equity = cash_for_equity + sum(mv.values())

    # Income layer: target size from total equity only
    income_ratio = get_income_ratio(total_equity)
    target_income_val = total_equity * income_ratio
    target_spyi_val = target_income_val * (1.0 - QQQI_INCOME_RATIO)
    target_qqqi_val = target_income_val * QQQI_INCOME_RATIO

    # Strategy layer: remainder allocated to TQQQ + BOXX
    strategy_equity = max(0.0, total_equity - target_income_val)
    reserved = strategy_equity * CASH_RESERVE_RATIO
    agg_ratio, _ = get_hybrid_allocation(strategy_equity, qqq_p, exit_line)

    # TQQQ signal: staged exit when between MA200 and exit_line, full exit below exit_line
    target_tqqq_ratio, icon, reason = 0.0, "idle", "no signal"
    if qty["TQQQ"] > 0:
        if qqq_p < exit_line:
            target_tqqq_ratio, icon, reason = 0.0, "exit", "below exit line"
        elif qqq_p < ma200:
            target_tqqq_ratio, icon, reason = agg_ratio * 0.33, "reduce", "between MA200 and exit"
        else:
            target_tqqq_ratio, icon, reason = agg_ratio, "hold", "above MA200"
    elif qqq_p > entry_line:
        target_tqqq_ratio, icon, reason = agg_ratio, "entry", "above entry line"

    target_tqqq_val = strategy_equity * target_tqqq_ratio
    target_boxx_val = max(0.0, (strategy_equity - reserved) - target_tqqq_val)
    threshold = total_equity * 0.01

    dashboard = (
        f"Equity ${total_equity:,.2f} | TQQQ ${mv['TQQQ']:,.2f} SPYI ${mv['SPYI']:,.2f} QQQI ${mv['QQQI']:,.2f} BOXX ${mv['BOXX']:,.2f} | "
        f"Signal {icon} {reason} | QQQ {qqq_p:.2f} MA200 {ma200:.2f} Exit {exit_line:.2f}"
    )

    # Quotes and order execution
    raw_quotes = c.get_quotes(strategy_symbols)
    quotes_data = safe_api_call(raw_quotes, "Quotes")
    quotes = {sym: quotes_data[sym]['quote'] for sym in strategy_symbols}
    trade_logs = []

    def execute_fire_forget(symbol, action_type, quantity, price=None):
        if quantity <= 0:
            return
        try:
            p_str = "{:.2f}".format(price) if price else None
            if action_type == 'SELL':
                order = equity_sell_market(symbol, quantity)
                label = f"SELL {symbol}"
            elif action_type == 'BUY_LIMIT':
                order = equity_buy_limit(symbol, quantity, p_str)
                label = f"BUY_LIMIT {symbol} @ {p_str}"
            elif action_type == 'BUY_MARKET':
                order = equity_buy_market(symbol, quantity)
                label = f"BUY_MKT {symbol}"
            else:
                return
            resp = c.place_order(acct_hash, order)
            success, info = check_submission(resp, symbol)
            if success:
                trade_logs.append(f"OK {label} qty={quantity} id={info}")
            else:
                trade_logs.append(f"FAIL {label} {info}")
        except Exception as e:
            trade_logs.append(f"ERR {symbol} {e}")

    # Sell phase
    sell_executed = False
    if mv["TQQQ"] > (target_tqqq_val + threshold):
        q = int((mv["TQQQ"] - target_tqqq_val) // quotes["TQQQ"]['lastPrice'])
        execute_fire_forget('TQQQ', 'SELL', q)
        sell_executed = True
    if mv["SPYI"] > (target_spyi_val + threshold):
        q = int((mv["SPYI"] - target_spyi_val) // quotes["SPYI"]['lastPrice'])
        execute_fire_forget('SPYI', 'SELL', q)
        sell_executed = True
    if mv["QQQI"] > (target_qqqi_val + threshold):
        q = int((mv["QQQI"] - target_qqqi_val) // quotes["QQQI"]['lastPrice'])
        execute_fire_forget('QQQI', 'SELL', q)
        sell_executed = True
    if mv["BOXX"] > (target_boxx_val + threshold):
        q = int((mv["BOXX"] - target_boxx_val) // quotes["BOXX"]['lastPrice'])
        execute_fire_forget('BOXX', 'SELL', q)
        sell_executed = True

    if sell_executed:
        time.sleep(3)

    # Buy phase
    est_buying_power = max(0, real_buying_power - reserved)
    for sym, target_val in [('SPYI', target_spyi_val), ('QQQI', target_qqqi_val), ('TQQQ', target_tqqq_val)]:
        if mv[sym] < (target_val - threshold):
            amt_to_spend = min(target_val - mv[sym], est_buying_power)
            if amt_to_spend > 0:
                ask = quotes[sym]['askPrice']
                q = int(amt_to_spend // ask)
                if q > 0:
                    limit_p = round(ask * 1.005, 2)
                    execute_fire_forget(sym, 'BUY_LIMIT', q, limit_p)
                    est_buying_power -= (q * limit_p)

    if est_buying_power > quotes["BOXX"]['lastPrice'] * 2:
        q = int(est_buying_power // quotes["BOXX"]['lastPrice'])
        if q > 0:
            execute_fire_forget('BOXX', 'BUY_MARKET', q)

    if trade_logs:
        send_tg_message(f"Trades\n{dashboard}\n" + "\n".join(trade_logs))
    else:
        print(dashboard, flush=True)


@app.route("/", methods=["POST", "GET"])
def handle_schwab():
    try:
        c = get_schwab_client_init()
        tz_ny = pytz.timezone('America/New_York')
        now_ny = datetime.now(tz_ny)
        nyse = mcal.get_calendar('NASDAQ')
        schedule = nyse.schedule(start_date=now_ny.date(), end_date=now_ny.date())
        if schedule.empty:
            return "Market Closed", 200
        run_strategy_core(c, now_ny)
        return "OK", 200
    except Exception:
        send_tg_message(f"Error\n{traceback.format_exc()}")
        return "Error", 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
