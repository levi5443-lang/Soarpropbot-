"""
APEX → MT5 Trade Execution Bot
Reads signals from the XAU Signals Telegram bot and executes them
on a Soar Funding MT5 account with full rule compliance.

Rules enforced:
- Max 3% risk per trade ($300 on $10k account)
- Daily drawdown limit: 4% ($400) — alerts at 3% ($300 used)
- Total drawdown limit: 8% ($800) — alerts at 7% ($700 used)
- News window: blocks trades 5 min before/after high-impact events
- Asks admin before trading when drawdown is close
- Lot size calculated from 3% account balance / distance to stop loss
"""

import os
import json
import asyncio
import logging
import re
import math
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import httpx
from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)
from telegram.constants import ParseMode

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("apex_mt5_bot")

# ── Config ─────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")
TWELVE_DATA_KEY   = os.environ.get("TWELVE_DATA_KEY", "22719eb0a9804e4da8b0247dc5545f7b")
WEBHOOK_SECRET    = os.environ.get("WEBHOOK_SECRET", "apex_secret_2026")

# ── Multi-account config ───────────────────────────────────────────────────
# Add up to 10 accounts via Render env vars.
# Format: ACCOUNT_N_TOKEN, ACCOUNT_N_ID, ACCOUNT_N_NAME, ACCOUNT_N_BALANCE
# N = 1 through 10. Only accounts with both TOKEN and ID are loaded.
def load_accounts() -> list:
    """
    Load all configured MT5 accounts from environment variables.
    Returns list of account dicts.
    Supports up to 10 accounts — add more env vars to add more accounts.
    """
    accounts = []
    for n in range(1, 11):
        token   = os.environ.get(f"ACCOUNT_{n}_TOKEN", "")
        acct_id = os.environ.get(f"ACCOUNT_{n}_ID", "")
        name    = os.environ.get(f"ACCOUNT_{n}_NAME", f"Account {n}")
        balance = float(os.environ.get(f"ACCOUNT_{n}_BALANCE", "10000"))
        if token and acct_id:
            accounts.append({
                "token":   token,
                "id":      acct_id,
                "name":    name,
                "balance": balance,
            })
    # Fallback to legacy single-account env vars if no numbered accounts found
    if not accounts:
        token   = os.environ.get("METAAPI_TOKEN", "")
        acct_id = os.environ.get("MT5_ACCOUNT_ID", "")
        if token and acct_id:
            accounts.append({
                "token":   token,
                "id":      acct_id,
                "name":    "Soar",
                "balance": 10000.0,
            })
    return accounts

ACCOUNTS = load_accounts()

# ── Risk settings ──────────────────────────────────────────────────────────
MAX_DAILY_LOSS_PCT  = 0.04
MAX_TOTAL_LOSS_PCT  = 0.08
ALERT_BUFFER_PCT    = 0.01

# Volatility-based risk — Levz decides per signal condition
RISK_BY_VOLATILITY = {
    "EXPANDING":   0.010,   # 1.0% — strong momentum, full risk
    "NORMAL":      0.008,   # 0.8% — standard conditions
    "CONTRACTING": 0.005,   # 0.5% — compressed market, protect capital
    "UNKNOWN":     0.005,   # 0.5% — when unsure, be conservative
}

def get_risk_pct_for_signal(signal: dict) -> float:
    """
    Determine risk % based on signal's volatility regime.
    Falls back to position_size_pct from signal if provided and lower.
    """
    # Use signal's recommended size if provided
    signal_pct = signal.get("position_size_pct")
    if signal_pct:
        signal_risk = float(signal_pct) / 100
    else:
        signal_risk = 0.01

    # Never exceed signal's own recommendation
    return min(signal_risk, 0.01)  # hard cap at 1% for prop safety

def dynamic_limits(balance: float) -> dict:
    return {
        "max_risk_usd":    balance * 0.01,
        "max_daily_usd":   balance * MAX_DAILY_LOSS_PCT,
        "max_total_usd":   balance * MAX_TOTAL_LOSS_PCT,
        "alert_daily_usd": balance * (MAX_DAILY_LOSS_PCT - ALERT_BUFFER_PCT),
        "alert_total_usd": balance * (MAX_TOTAL_LOSS_PCT - ALERT_BUFFER_PCT),
    }

# ── Legacy constants — calculated from default starting balance ────────────
# Used by single-account functions that haven't been migrated yet
_DEFAULT_BALANCE      = float(os.environ.get("ACCOUNT_1_BALANCE", "10000"))
ACCOUNT_BALANCE_START = _DEFAULT_BALANCE
MAX_RISK_PCT       = 0.01
MAX_RISK_USD       = _DEFAULT_BALANCE * MAX_RISK_PCT
MAX_DAILY_LOSS_USD = _DEFAULT_BALANCE * MAX_DAILY_LOSS_PCT
MAX_TOTAL_LOSS_USD = _DEFAULT_BALANCE * MAX_TOTAL_LOSS_PCT
ALERT_DAILY_USD    = _DEFAULT_BALANCE * (MAX_DAILY_LOSS_PCT - ALERT_BUFFER_PCT)
ALERT_TOTAL_USD    = _DEFAULT_BALANCE * (MAX_TOTAL_LOSS_PCT - ALERT_BUFFER_PCT)

UTC = ZoneInfo("UTC")
META_BASE = "https://mt-client-api-v1.london.agiliumtrade.ai"

# ── State ──────────────────────────────────────────────────────────────────
PENDING_TRADES: dict = {}   # trade_id -> signal dict awaiting approval
TRADE_LOG:      list = []   # all executed trades across all accounts
SIGNAL_QUEUE:   list = []   # signals buffered for priority ordering

# Stats cache — populated by push from XAU Signals bot
STATS_CACHE: dict = {}

# Per-account daily closed P&L tracker: { account_id: {"pnl": float, "date": str} }
ACCOUNT_DAILY_PNL: dict = {}


def check_daily_reset_for(account_id: str):
    """Reset daily P&L for a specific account at midnight UTC."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    if account_id not in ACCOUNT_DAILY_PNL:
        ACCOUNT_DAILY_PNL[account_id] = {"pnl": 0.0, "date": today}
    elif ACCOUNT_DAILY_PNL[account_id]["date"] != today:
        ACCOUNT_DAILY_PNL[account_id] = {"pnl": 0.0, "date": today}
        log.info("Daily P&L reset for account: " + account_id)


def record_closed_pnl_for(account_id: str, pnl_usd: float):
    check_daily_reset_for(account_id)
    ACCOUNT_DAILY_PNL[account_id]["pnl"] += pnl_usd


def get_daily_closed_loss_for(account_id: str) -> float:
    check_daily_reset_for(account_id)
    return abs(min(ACCOUNT_DAILY_PNL[account_id]["pnl"], 0))


# Legacy single-account helpers for backward compat
def check_daily_reset():
    if ACCOUNTS:
        check_daily_reset_for(ACCOUNTS[0]["id"])

def record_closed_pnl(pnl_usd: float):
    if ACCOUNTS:
        record_closed_pnl_for(ACCOUNTS[0]["id"], pnl_usd)

def get_daily_closed_loss() -> float:
    if ACCOUNTS:
        return get_daily_closed_loss_for(ACCOUNTS[0]["id"])
    return 0.0

# Grade priority — higher number = higher priority
GRADE_PRIORITY = {"A": 4, "A+": 5, "B+": 3, "B": 2, "C": 1}

# Breakeven settings
# After TP1 is hit, move SL to entry + this buffer (in pips)
BREAKEVEN_BUFFER_PIPS = {"XAUUSD": 8, "default": 3}   # Gold gets more room

# How long to buffer signals before sorting and executing (seconds)
QUEUE_WINDOW_SECONDS = 60

# Min remaining daily budget to execute each grade
# If daily budget remaining < threshold, skip that grade
GRADE_BUDGET_THRESHOLD = {
    "A":  0,      # Always execute Grade A if any budget left
    "A+": 0,      # Always execute Grade A+
    "B+": 150,    # Need at least $150 remaining for B+
    "B":  200,    # Need at least $200 remaining for B
    "C":  99999,  # Never execute C
}

# ── MetaAPI helpers ────────────────────────────────────────────────────────
async def meta_get(path: str, token: str = None, account_id: str = None) -> dict:
    """Fetch from MetaAPI. Uses first account by default for backward compat."""
    if not token and ACCOUNTS:
        token      = ACCOUNTS[0]["token"]
        account_id = ACCOUNTS[0]["id"]
    url = f"{META_BASE}/users/current/accounts/{account_id}{path}"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, headers={
            "auth-token": token,
            "Content-Type": "application/json"
        })
        r.raise_for_status()
        return r.json()


async def meta_post(path: str, body: dict, token: str = None, account_id: str = None) -> dict:
    """Post to MetaAPI. Uses first account by default for backward compat."""
    if not token and ACCOUNTS:
        token      = ACCOUNTS[0]["token"]
        account_id = ACCOUNTS[0]["id"]
    url = f"{META_BASE}/users/current/accounts/{account_id}{path}"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(url, json=body, headers={
            "auth-token": token,
            "Content-Type": "application/json"
        })
        r.raise_for_status()
        return r.json()


async def meta_modify(position_id: str, stop_loss: float, take_profit: float) -> dict:
    """Modify an open position's SL and TP."""
    try:
        result = await meta_post("/trade", {
            "actionType":  "POSITION_MODIFY",
            "positionId":  position_id,
            "stopLoss":    stop_loss,
            "takeProfit":  take_profit,
        })
        return result
    except Exception as e:
        log.error("Modify position error: " + str(e))
        return {}


def get_breakeven_stop(pair: str, entry: float, direction: str) -> float:
    """Calculate the breakeven stop — entry + buffer in pips."""
    buffer_pips = BREAKEVEN_BUFFER_PIPS.get(pair, BREAKEVEN_BUFFER_PIPS["default"])
    pair_clean  = pair.replace("/", "")
    if pair_clean == "XAUUSD":
        buffer = buffer_pips * 0.1
    elif "JPY" in pair_clean:
        buffer = buffer_pips * 0.01
    else:
        buffer = buffer_pips * 0.0001
    if direction == "BUY":
        return round(entry + buffer, 5)
    else:
        return round(entry - buffer, 5)


async def meta_cancel_order(order_id: str) -> dict:
    """Cancel a pending limit order by order ID."""
    try:
        result = await meta_post("/trade", {
            "actionType": "ORDER_CANCEL",
            "orderId":    order_id,
        })
        return result
    except Exception as e:
        log.error("Cancel order error: " + str(e))
        return {}


def parse_dt(iso_str: str) -> datetime:
    """Parse ISO datetime string and ensure it is timezone-aware (UTC)."""
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


async def get_open_positions() -> list:
    """Fetch all open positions from MetaAPI."""
    try:
        return await meta_get("/positions")
    except Exception as e:
        log.error("get_open_positions error: " + str(e))
        return []


async def get_pending_orders() -> list:
    """Fetch all pending (unfilled) orders from MetaAPI."""
    try:
        return await meta_get("/orders")
    except Exception as e:
        log.error("get_pending_orders error: " + str(e))
        return []


async def get_account_info() -> dict:
    """Fetch live account balance, equity, and drawdown status."""
    try:
        info = await meta_get("/account-information")
        return {
            "balance":    info.get("balance", ACCOUNT_BALANCE_START),
            "equity":     info.get("equity", ACCOUNT_BALANCE_START),
            "margin":     info.get("margin", 0),
            "free_margin":info.get("freeMargin", ACCOUNT_BALANCE_START),
            "profit":     info.get("profit", 0),
        }
    except Exception as e:
        log.error("get_account_info error: " + str(e))
        return {"balance": ACCOUNT_BALANCE_START, "equity": ACCOUNT_BALANCE_START, "profit": 0}


async def get_daily_pnl() -> float:
    """Daily P&L = open positions profit + today's closed trade P&L."""
    try:
        positions = await meta_get("/positions")
        open_pnl  = sum(p.get("profit", 0) for p in positions)
        return open_pnl + DAILY_CLOSED_PNL
    except Exception as e:
        log.error("get_daily_pnl error: " + str(e))
        return DAILY_CLOSED_PNL


async def fetch_atr(pair: str, interval: str = "15min", period: int = 14) -> float | None:
    """
    Fetch ATR for a pair via Twelve Data.
    Returns ATR value or None if unavailable.
    Used to calculate dynamic slippage tolerance.
    """
    symbol = pair.replace(".", "")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.twelvedata.com/atr",
                params={
                    "symbol":     symbol + "/USD" if "USD" not in symbol else symbol,
                    "interval":   interval,
                    "time_period": period,
                    "outputsize": 1,
                    "apikey":     TWELVE_DATA_KEY,
                },
                headers={"X-API-Version": "last"},
            )
            data = r.json()
            values = data.get("values", [])
            if values:
                return float(values[0].get("atr", 0))
    except Exception as e:
        log.warning("fetch_atr error for " + pair + ": " + str(e))
    return None


def calc_pip_distance(pair: str, price_a: float, price_b: float) -> float:
    """Calculate pip distance between two prices for a given pair."""
    diff = abs(price_a - price_b)
    if pair == "XAUUSD":
        return diff * 10
    if "JPY" in pair:
        return diff * 100
    return diff * 10000


def atr_to_pips(pair: str, atr: float) -> float:
    """Convert ATR value to pips for a given pair."""
    if pair == "XAUUSD":
        return atr * 10
    if "JPY" in pair:
        return atr * 100
    return atr * 10000


async def check_slippage_tolerance(pair: str, entry: float, current_price: float, direction: str) -> tuple[bool, str]:
    """
    Check if current price has moved too far from entry to justify market execution.
    Tolerance = 0.5x ATR in pips.
    Returns (ok_to_execute, reason_string)
    """
    atr = await fetch_atr(pair.replace(".", ""))
    if atr is None:
        # If ATR unavailable — allow execution with warning
        log.warning("ATR unavailable for " + pair + " — allowing execution without slippage check")
        return True, "ATR unavailable — executed without slippage check"

    atr_pips      = atr_to_pips(pair.replace(".", ""), atr)
    tolerance_pips = round(atr_pips * 0.5, 1)
    slippage_pips  = calc_pip_distance(pair.replace(".", ""), entry, current_price)

    if slippage_pips > tolerance_pips:
        reason = (
            "Slippage too large — price moved " + str(round(slippage_pips, 1)) + " pips from entry " +
            str(entry) + " (ATR tolerance: " + str(tolerance_pips) + " pips). Trade skipped."
        )
        return False, reason

    return True, "Within ATR tolerance (" + str(round(slippage_pips, 1)) + "/" + str(tolerance_pips) + " pips)"


async def place_order(signal: dict, lot_size: float) -> dict:
    """Place an order on MT5 via MetaAPI using the order type specified by Levz."""
    pair        = signal["pair"].replace("/", "") + "."
    pair_price  = signal["pair"].replace("/", "")
    dirn        = signal["direction"]
    entry       = float(signal["entry_price"])
    sl          = float(signal["stop_loss"])
    tp1         = float(signal["tp1"])
    tp2         = signal.get("tp2")
    order_type  = signal.get("order_type", "LIMIT").upper()

    token    = signal.get("token") or (ACCOUNTS[0]["token"] if ACCOUNTS else "")
    acct_id  = signal.get("id")    or (ACCOUNTS[0]["id"]    if ACCOUNTS else "")

    # Get current price
    try:
        await meta_get("/account-information", token, acct_id)
        positions = await meta_get("/symbols/" + pair_price + "/currentPrice", token, acct_id)
        ask = float(positions.get("ask", entry))
        bid = float(positions.get("bid", entry))
    except Exception as e:
        log.warning("Price fetch failed: " + str(e) + " — using entry as fallback")
        ask = entry
        bid = entry

    # ── Map Levz order type to MetaAPI action ─────────────────────────────
    if order_type == "MARKET":
        action     = "ORDER_TYPE_BUY" if dirn == "BUY" else "ORDER_TYPE_SELL"
        open_price = ask if dirn == "BUY" else bid
        log.info(pair + ": MARKET order at " + str(open_price))

    elif order_type == "STOP":
        if dirn == "BUY":
            # BUY STOP: entry is ABOVE current price — triggers when price breaks UP through entry
            if ask >= entry:
                ok, reason = await check_slippage_tolerance(pair, entry, ask, dirn)
                if not ok:
                    raise ValueError("SLIPPAGE_EXCEEDED: " + reason)
                action, open_price = "ORDER_TYPE_BUY", ask
                log.info(pair + ": BUY STOP already triggered — market at " + str(ask))
            else:
                action, open_price = "ORDER_TYPE_BUY_STOP", entry
                log.info(pair + ": BUY STOP set at " + str(entry))
        else:
            # SELL STOP: entry is BELOW current price — triggers when price breaks DOWN through entry
            if bid <= entry:
                ok, reason = await check_slippage_tolerance(pair, entry, bid, dirn)
                if not ok:
                    raise ValueError("SLIPPAGE_EXCEEDED: " + reason)
                action, open_price = "ORDER_TYPE_SELL", bid
                log.info(pair + ": SELL STOP already triggered — market at " + str(bid))
            else:
                action, open_price = "ORDER_TYPE_SELL_STOP", entry
                log.info(pair + ": SELL STOP set at " + str(entry))

    elif order_type == "STOP_LIMIT":
        action     = "ORDER_TYPE_BUY_STOP_LIMIT" if dirn == "BUY" else "ORDER_TYPE_SELL_STOP_LIMIT"
        open_price = entry
        log.info(pair + ": " + order_type + " at " + str(entry))

    else:
        # LIMIT (default)
        if dirn == "BUY":
            # BUY LIMIT: entry is BELOW current price — wait for price to pull back down to entry
            if ask <= entry:
                ok, reason = await check_slippage_tolerance(pair, entry, ask, dirn)
                if not ok:
                    raise ValueError("SLIPPAGE_EXCEEDED: " + reason)
                action, open_price = "ORDER_TYPE_BUY", ask
                log.info(pair + ": BUY LIMIT triggered — price pulled back, market at " + str(ask))
            else:
                action, open_price = "ORDER_TYPE_BUY_LIMIT", entry
                log.info(pair + ": BUY LIMIT placed at " + str(entry) + " waiting for pullback")
        else:
            # SELL LIMIT: entry is ABOVE current price — wait for price to rally up to entry
            if bid >= entry:
                ok, reason = await check_slippage_tolerance(pair, entry, bid, dirn)
                if not ok:
                    raise ValueError("SLIPPAGE_EXCEEDED: " + reason)
                action, open_price = "ORDER_TYPE_SELL", bid
                log.info(pair + ": SELL LIMIT triggered — price rallied to entry, market at " + str(bid))
            else:
                action, open_price = "ORDER_TYPE_SELL_LIMIT", entry
                log.info(pair + ": SELL LIMIT placed at " + str(entry) + " waiting for rally")

    # ── Build order body with correct MetaAPI field names ─────────────────
    body = {
        "symbol":     pair,
        "actionType": action,
        "volume":     round(lot_size, 2),
        "stopLoss":   sl,
        "takeProfit": tp1,
        "comment":    "LEVZ_SIGNAL_" + order_type,
    }

    # Pending orders use openPrice; market orders don't need it
    if action not in ("ORDER_TYPE_BUY", "ORDER_TYPE_SELL"):
        body["openPrice"] = open_price

    # STOP_LIMIT requires stopLimitPrice — set slightly inside the entry
    if order_type == "STOP_LIMIT":
        if dirn == "BUY":
            body["stopLimitPrice"] = round(open_price - 0.0002 if "JPY" not in pair else open_price - 0.02, 5)
        else:
            body["stopLimitPrice"] = round(open_price + 0.0002 if "JPY" not in pair else open_price + 0.02, 5)
        if pair.replace(".", "") == "XAUUSD":
            body["stopLimitPrice"] = round(open_price + (0.5 if dirn == "SELL" else -0.5), 2)

    result = await meta_post("/trade", body, token, acct_id)
    log.info("Order body sent: " + str(body))
    log.info("Order result: " + str(result))
    return result


# ── Risk engine ────────────────────────────────────────────────────────────
def calc_pip_value(pair: str) -> float:
    """Approximate pip value in USD per standard lot."""
    pair = pair.replace("/", "")
    if "JPY" in pair:
        return 9.09    # ~$9.09 per pip for JPY pairs
    if pair == "XAUUSD":
        return 10.0    # $10 per pip (0.01 move) for Gold
    return 10.0        # standard $10 per pip for majors


def calc_lot_size(pair: str, entry: float, stop_loss: float,
                  balance: float, risk_pct: float = 0.03) -> float:
    """
    Calculate lot size so that if stop loss is hit,
    the loss equals exactly risk_pct of balance.
    Never exceeds MAX_RISK_USD.
    """
    risk_usd    = min(balance * risk_pct, MAX_RISK_USD)
    pip_value   = calc_pip_value(pair)

    # Distance to stop in pips
    pair_clean = pair.replace("/", "")
    if "JPY" in pair_clean:
        pips = abs(entry - stop_loss) * 100
    elif pair_clean == "XAUUSD":
        pips = abs(entry - stop_loss) * 10
    else:
        pips = abs(entry - stop_loss) * 10000

    if pips <= 0:
        log.warning("Zero pip distance — using 0.01 lot minimum")
        return 0.01

    raw_lots = risk_usd / (pips * pip_value)
    # Round down to 2 decimal places (don't round up — stays within risk)
    lots = math.floor(raw_lots * 100) / 100
    return max(0.01, min(lots, 10.0))   # floor 0.01, ceiling 10 lots


async def check_drawdown_limits(bot) -> tuple[bool, str]:
    """
    Check if we're within safe drawdown limits.
    Returns (safe_to_trade, reason_if_not_safe)
    """
    info      = await get_account_info()
    daily_pnl = await get_daily_pnl()

    balance   = info["balance"]
    daily_loss = abs(min(daily_pnl, 0))    # only losses count
    total_loss = max(0, ACCOUNT_BALANCE_START - balance)

    # Hard blocks
    if daily_loss >= MAX_DAILY_LOSS_USD:
        return False, "DAILY LIMIT HIT — " + str(round(daily_loss, 2)) + " lost today. Max $" + str(MAX_DAILY_LOSS_USD) + ". No more trades today."

    if total_loss >= MAX_TOTAL_LOSS_USD:
        return False, "TOTAL DRAWDOWN LIMIT HIT — $" + str(round(total_loss, 2)) + " total loss. Account at risk. Stop trading."

    # Soft alerts (within 1%)
    if daily_loss >= ALERT_DAILY_USD:
        msg = (
            "DRAWDOWN ALERT\n\n"
            "Daily loss: $" + str(round(daily_loss, 2)) + " of $" + str(MAX_DAILY_LOSS_USD) + " limit\n"
            "Only $" + str(round(MAX_DAILY_LOSS_USD - daily_loss, 2)) + " remaining today\n\n"
            "Trade carefully — you are close to the daily limit."
        )
        try:
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

    if total_loss >= ALERT_TOTAL_USD:
        msg = (
            "TOTAL DRAWDOWN ALERT\n\n"
            "Total loss: $" + str(round(total_loss, 2)) + " of $" + str(MAX_TOTAL_LOSS_USD) + " limit\n"
            "Only $" + str(round(MAX_TOTAL_LOSS_USD - total_loss, 2)) + " remaining\n\n"
            "Account health at risk. Review open positions."
        )
        try:
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

    return True, "OK"


# ── News window check ──────────────────────────────────────────────────────
async def is_news_window_active(pair: str) -> tuple[bool, str]:
    """
    Check if we're within 5 minutes of a high-impact news event
    using Twelve Data's economic calendar.
    """
    try:
        now = datetime.now(UTC)
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.twelvedata.com/economic_calendar",
                params={
                    "apikey":   TWELVE_DATA_KEY,
                    "importance": "high",
                    "start_date": now.strftime("%Y-%m-%d"),
                    "end_date":   now.strftime("%Y-%m-%d"),
                }
            )
            data = r.json()

        events = data.get("result", {}).get("list", [])
        window = timedelta(minutes=NEWS_WINDOW_MINUTES)

        for event in events:
            try:
                event_time = datetime.fromisoformat(event["date"].replace("Z", "+00:00"))
                diff = abs((now - event_time).total_seconds() / 60)
                if diff <= NEWS_WINDOW_MINUTES:
                    return True, event.get("event", "High-impact news") + " at " + event_time.strftime("%H:%M UTC")
            except Exception:
                continue

        return False, ""
    except Exception as e:
        log.error("News check error: " + str(e))
        return False, ""   # fail open — don't block if API is down


# ── Signal parser ──────────────────────────────────────────────────────────
def parse_signal_from_message(text: str) -> dict | None:
    """
    Parse a structured signal message from the XAU Signals bot.
    Expects the standard APEX signal format.
    """
    # Strip markdown formatting before parsing
    clean = text.replace("*", "").replace("_", "").replace("`", "")
    if "APEX SIGNAL" not in clean and "XAU SIGNAL" not in clean:
        return None
    text = clean  # use cleaned version for all parsing below

    try:
        s = {}

        # Pair
        pair_m = re.search(r"(?:APEX SIGNAL|XAU SIGNAL)\s*[-\u2014]\s*([A-Z/]+)", text)
        if pair_m:
            s["pair"] = pair_m.group(1).replace("/", "")

        # Direction
        if "BUY" in text:
            s["direction"] = "BUY"
        elif "SELL" in text:
            s["direction"] = "SELL"
        else:
            return None

        # Grade
        grade_m = re.search(r"Grade ([A-C][+]?)", text)
        s["grade"] = grade_m.group(1) if grade_m else "B"

        # Confidence
        conf_m = re.search(r"Conf: ([\d.]+)/10", text)
        s["confidence"] = float(conf_m.group(1)) if conf_m else 5.0

        # Order type — parse from signal message
        order_type_m = re.search(r"Order Type:\s*(LIMIT|STOP_LIMIT|STOP|MARKET)", text)
        s["order_type"] = order_type_m.group(1) if order_type_m else "LIMIT"

        # Entry (handle backtick-wrapped values from Markdown)
        entry_m = re.search(r"Entry:\s*`?([\d.]+)`?", text)
        s["entry_price"] = float(entry_m.group(1)) if entry_m else None

        # Stop
        stop_m = re.search(r"Stop:\s*`?([\d.]+)`?", text)
        s["stop_loss"] = float(stop_m.group(1)) if stop_m else None

        # TP1
        tp1_m = re.search(r"TP1[^:]*:\s*`?([\d.]+)`?", text)
        s["tp1"] = float(tp1_m.group(1)) if tp1_m else None

        # TP2
        tp2_m = re.search(r"TP2[^:]*:\s*`?([\d.]+)`?", text)
        s["tp2"] = float(tp2_m.group(1)) if tp2_m else None

        # RR
        rr_m = re.search(r"RR:\s*([\d.]+)(?::\d)?", text)
        s["rr_ratio"] = float(rr_m.group(1)) if rr_m else 2.0

        # Position size recommended by APEX
        size_m = re.search(r"Size:\s*([\d.]+)%", text)
        s["position_size_pct"] = float(size_m.group(1)) if size_m else None

        # Signal fired time — parsed from header e.g. "07:16 UTC - 15 Apr 2026"
        time_m = re.search(r"(\d{2}:\d{2}) UTC - (\d{2} \w{3} \d{4})", text)
        if time_m:
            try:
                s["fired_at"] = datetime.strptime(
                    time_m.group(1) + " " + time_m.group(2), "%H:%M %d %b %Y"
                ).isoformat()
            except Exception:
                s["fired_at"] = datetime.now(UTC).isoformat()
        else:
            s["fired_at"] = datetime.now(UTC).isoformat()

        # Session
        sess_m = re.search(r"(London|New York|Asian|Overlap)", text)
        s["session"] = sess_m.group(1) if sess_m else "Unknown"

        # Validate required fields
        required = ["pair", "direction", "entry_price", "stop_loss", "tp1"]
        if not all(s.get(k) for k in required):
            return None

        s["signal_id"] = s["pair"] + "_" + datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        s["received_at"] = datetime.now(UTC).isoformat()
        return s

    except Exception as e:
        log.error("Signal parse error: " + str(e))
        return None


# ── Trade execution flow ───────────────────────────────────────────────────

# ── Signal priority queue ──────────────────────────────────────────────────
def grade_score(grade: str) -> int:
    return GRADE_PRIORITY.get(grade, 0)


def sort_queue(queue: list) -> list:
    """Sort signals by grade (highest first), then by confidence."""
    return sorted(
        queue,
        key=lambda s: (grade_score(s.get("grade", "C")), s.get("confidence", 0)),
        reverse=True,
    )


async def flush_queue(bot):
    """
    Process all signals currently in the queue, sorted by grade.
    Highest grade executes first. Skips lower grades if budget is tight.
    """
    if not SIGNAL_QUEUE:
        return

    sorted_signals = sort_queue(list(SIGNAL_QUEUE))
    SIGNAL_QUEUE.clear()

    info       = await get_account_info()
    daily_pnl  = await get_daily_pnl()
    daily_loss = abs(min(daily_pnl, 0))
    remaining  = MAX_DAILY_LOSS_USD - daily_loss

    count = len(sorted_signals)
    grades = [s.get("grade", "?") for s in sorted_signals]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=(
            "SIGNAL QUEUE - " + str(count) + " signal(s) ranked\n\n" +
            "Order: " + " > ".join(grades) + "\n" +
            "Daily budget remaining: $" + str(round(remaining, 2)) + "\n\n" +
            "Executing highest grade first..."
        ),
        parse_mode=ParseMode.MARKDOWN,
    )

    for signal in sorted_signals:
        grade     = signal.get("grade", "C")
        threshold = GRADE_BUDGET_THRESHOLD.get(grade, 99999)

        # Refresh remaining budget each iteration
        daily_pnl2  = await get_daily_pnl()
        daily_loss2 = abs(min(daily_pnl2, 0))
        remaining2  = MAX_DAILY_LOSS_USD - daily_loss2

        if remaining2 < threshold:
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=(
                    "Signal skipped: " + signal.get("pair", "?") + " " +
                    signal.get("direction", "?") + " Grade " + grade + "\n" +
                    "Budget remaining $" + str(round(remaining2, 2)) +
                    " below minimum $" + str(threshold) + " required for Grade " + grade
                ),
                parse_mode=ParseMode.MARKDOWN,
            )
            continue

        await process_signal(bot, signal)
        await asyncio.sleep(1)   # small gap between executions


async def process_signal(bot, signal: dict):
    """
    Full pipeline: validate → risk check → news check →
    ask admin if close to limits → execute or skip.
    """
    pair   = signal.get("pair", "?")
    dirn   = signal.get("direction", "?")
    grade  = signal.get("grade", "?")
    entry  = signal.get("entry_price")
    sl     = signal.get("stop_loss")

    log.info("Processing signal: " + pair + " " + dirn + " Grade " + grade)

    # 1. Only trade A and B grade signals
    if grade not in ("A", "B", "B+"):
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="Signal skipped: " + pair + " " + dirn + " — Grade " + grade + " below threshold (min B).",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # 2. News trading — add-on purchased, no news restrictions
    # All signals trade freely including around high-impact events

    # 3. Check drawdown limits
    info      = await get_account_info()
    daily_pnl = await get_daily_pnl()
    balance    = info["balance"]
    lim        = dynamic_limits(balance)
    daily_loss = abs(min(daily_pnl, 0))
    total_loss = max(0, ACCOUNT_BALANCE_START - balance)

    # Hard blocks
    if daily_loss >= lim["max_daily_usd"]:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="Signal BLOCKED: Daily limit reached ($" + str(round(daily_loss, 2)) + " / $" + str(round(lim["max_daily_usd"], 2)) + " based on balance $" + str(round(balance, 2)) + "). No more trades today.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if total_loss >= lim["max_total_usd"]:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="Signal BLOCKED: Total drawdown reached ($" + str(round(total_loss, 2)) + " / $" + str(round(lim["max_total_usd"], 2)) + "). Stop trading.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # 4. Calculate lot size
    # Use APEX's recommended position size, capped at 3% max
    signal_size_pct = signal.get("position_size_pct")
    if signal_size_pct and float(signal_size_pct) > 0:
        risk_pct = min(float(signal_size_pct) / 100, MAX_RISK_PCT)
    else:
        risk_pct = MAX_RISK_PCT
    lot_size = calc_lot_size(pair, entry, sl, balance, risk_pct)
    risk_usd = min(balance * risk_pct, MAX_RISK_USD)

    # 5. Ask admin if within 1% of daily limit
    remaining_daily = lim["max_daily_usd"] - daily_loss
    if remaining_daily < (balance * ALERT_BUFFER_PCT):
        trade_id = signal["signal_id"]
        PENDING_TRADES[trade_id] = signal

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("YES — Execute", callback_data="approve_" + trade_id),
                InlineKeyboardButton("NO — Skip", callback_data="reject_" + trade_id),
            ]
        ])
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=(
                "DRAWDOWN ALERT — Approval Required\n\n"
                "Signal: " + pair + " " + dirn + " Grade " + grade + "\n"
                "Entry: " + str(entry) + " | SL: " + str(sl) + "\n"
                "Lot size: " + str(lot_size) + " | Risk: $" + str(round(risk_usd, 2)) + "\n\n"
                "Daily loss so far: $" + str(round(daily_loss, 2)) + " / $" + str(MAX_DAILY_LOSS_USD) + "\n"
                "Remaining daily buffer: $" + str(round(remaining_daily, 2)) + "\n\n"
                "Do you want to execute this trade?"
            ),
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # 6. Execute immediately if all checks pass
    await execute_trade(bot, signal, lot_size, risk_usd)


async def execute_trade(bot, signal: dict, lot_size: float, risk_usd: float):
    """Place the order on MT5 and notify admin."""
    pair  = signal["pair"]
    dirn  = signal["direction"]
    entry = signal["entry_price"]
    sl    = signal["stop_loss"]
    tp1   = signal["tp1"]
    tp2   = signal.get("tp2")

    # Get live balance for risk % display
    try:
        info    = await get_account_info()
        balance = info["balance"]
    except Exception:
        balance = ACCOUNT_BALANCE_START

    try:
        result   = await place_order(signal, lot_size)
        order_id = result.get("orderId", "unknown")

        # ── Detect failed orders ───────────────────────────────────────────
        # MetaAPI returns error details in result if order was rejected
        error_msg    = result.get("message", "") or result.get("error", "") or result.get("description", "")
        error_code   = result.get("numericCode", "") or result.get("stringCode", "")
        order_failed = (
            order_id == "unknown" and not result.get("positionId")
        ) or result.get("error") or "error" in str(result).lower()

        if order_failed:
            # Build detailed failure reason
            reason = "Unknown reason"
            if error_msg:
                reason = str(error_msg)
            elif error_code:
                reason = "Error code: " + str(error_code)
            elif order_id == "unknown":
                reason = "No order ID returned — order may have been rejected by broker"

            log.error("Order FAILED: " + pair + " " + dirn + " — " + reason)

            # Get current price for diagnostics
            try:
                positions = await meta_get("/symbols/" + pair.replace("/","").replace(".","") + "/currentPrice")
                ask = positions.get("ask", "n/a")
                bid = positions.get("bid", "n/a")
                price_info = "Ask: " + str(ask) + " | Bid: " + str(bid)
            except Exception:
                price_info = "Price unavailable"

            # Get account info for margin diagnostics
            try:
                acct = await get_account_info()
                margin_free = acct.get("freeMargin", "n/a")
                margin_info = "Free margin: $" + str(round(float(margin_free), 2)) if margin_free != "n/a" else "Margin unavailable"
            except Exception:
                margin_info = "Margin unavailable"

            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=(
                    "⚠️ *ORDER FAILED*\n\n" +
                    pair + " " + dirn + " - Grade " + str(signal.get("grade")) + "\n\n" +
                    "*Reason:* " + reason + "\n"
                    "*Entry attempted:* " + str(entry) + "\n" +
                    "*" + price_info + "*\n" +
                    "*Lot size:* " + str(lot_size) + "\n" +
                    "*Risk:* $" + str(round(risk_usd, 2)) + "\n" +
                    "*" + margin_info + "*\n\n" +
                    "_Full result: " + str(result)[:200] + "_"
                ),
                parse_mode=ParseMode.MARKDOWN,
            )
            return

        # ── Successful order ───────────────────────────────────────────────
        executed_at = datetime.now(UTC).isoformat()
        TRADE_LOG.append({
            "signal_id":    signal.get("signal_id"),
            "pair":         pair,
            "direction":    dirn,
            "grade":        signal.get("grade"),
            "entry":        entry,
            "sl":           sl,
            "tp1":          tp1,
            "tp2":          tp2,
            "lots":         lot_size,
            "risk_usd":     risk_usd,
            "order_id":     order_id,
            "position_id":  result.get("positionId", ""),
            "signal_fired_at": signal.get("fired_at", ""),
            "executed_at":  executed_at,
            "closed_at":    None,
            "status":       "OPEN",
            "breakeven_set": False,
            "closed_pnl":   None,
            "valid_for_hours": float(signal.get("valid_for_hours") or 4),
            "order_type":   "LIMIT",
            "filled":       False,
        })

        signal_time = signal.get("fired_at", "")
        exec_time   = datetime.now(UTC).strftime("%H:%M UTC")
        if signal_time:
            try:
                fired_dt   = datetime.fromisoformat(signal_time.replace("Z", "+00:00"))
                signal_fmt = fired_dt.strftime("%H:%M UTC")
            except Exception:
                signal_fmt = str(signal_time)[:16]
        else:
            signal_fmt = "unknown"

        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=(
                "TRADE EXECUTED\n\n" +
                pair + " " + dirn + " - Grade " + str(signal.get("grade")) + "\n\n" +
                "Entry: " + str(entry) + "\n" +
                "Stop Loss: " + str(sl) + "\n" +
                "TP1: " + str(tp1) + "\n" +
                "TP2: " + str(tp2 or "n/a") + "\n" +
                "Lot size: " + str(lot_size) + "\n" +
                "Risk: $" + str(round(risk_usd, 2)) + " (" + str(round(risk_usd / balance * 100, 1)) + "%)\n\n" +
                "Signal fired: " + signal_fmt + "\n" +
                "Order placed: " + exec_time + "\n" +
                "Order ID: " + str(order_id)
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        log.info("Trade executed: " + pair + " " + dirn + " lots=" + str(lot_size))

    except Exception as e:
        err_str = str(e)
        if "SLIPPAGE_EXCEEDED" in err_str:
            # Clean notification — not an error, just a skipped trade
            reason = err_str.replace("SLIPPAGE_EXCEEDED: ", "")
            log.info("Trade skipped — slippage: " + pair + " " + dirn)
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=(
                    "⏭ *TRADE SKIPPED — Slippage*\n\n" +
                    pair + " " + dirn + " - Grade " + str(signal.get("grade", "?")) + "\n\n" +
                    reason + "\n\n" +
                    "_Levz protected the account from a bad fill._"
                ),
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            log.error("Trade execution error: " + err_str)
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=(
                    "⚠️ *TRADE EXCEPTION*\n\n" +
                    pair + " " + dirn + "\n" +
                    "*Error:* " + err_str + "\n\n" +
                    "_Check Render logs for full traceback._"
                ),
                parse_mode=ParseMode.MARKDOWN,
            )



# ── Multi-account execution ────────────────────────────────────────────────
async def execute_signal_for_account(bot, signal: dict, account: dict):
    """
    Execute a signal for a single MT5 account.
    Uses per-account balance, drawdown limits and volatility-based risk.
    """
    pair  = signal.get("pair", "?")
    dirn  = signal.get("direction", "?")
    grade = signal.get("grade", "?")
    name  = account["name"]
    token = account["token"]
    acct  = account["id"]

    try:
        # Get live balance for this account
        info    = await meta_get("/account-information", token, acct)
        balance = float(info.get("balance", account["balance"]))
        equity  = float(info.get("equity", balance))
        lim     = dynamic_limits(balance)

        # Check drawdown limits
        daily_loss = get_daily_closed_loss_for(acct)
        total_loss = balance - equity

        if daily_loss >= lim["max_daily_usd"]:
            log.info(name + ": daily limit reached — skipping " + pair)
            return
        if total_loss >= lim["max_total_usd"]:
            log.info(name + ": total drawdown reached — skipping " + pair)
            return

        # Alert if approaching limits
        if daily_loss >= lim["alert_daily_usd"]:
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text="⚠️ *" + name + "* — Daily drawdown alert: $" + str(round(daily_loss, 2)) + " used",
                parse_mode=ParseMode.MARKDOWN,
            )

        # Volatility-based risk
        risk_pct = get_risk_pct_for_signal(signal)
        sl       = float(signal.get("stop_loss", 0))
        entry    = float(signal.get("entry_price", 0))
        lot_size = calc_lot_size(pair, entry, sl, balance, risk_pct)
        risk_usd = balance * risk_pct

        # Place order using this account's credentials
        signal_copy          = dict(signal)
        signal_copy["token"] = token
        signal_copy["id"]    = acct

        result   = await place_order(signal_copy, lot_size)
        order_id = result.get("orderId", "unknown")

        # Detect failed order
        order_failed = (order_id == "unknown" and not result.get("positionId")) or result.get("error")
        if order_failed:
            error_msg = result.get("message", "") or result.get("description", "No order ID returned")
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=(
                    "⚠️ *ORDER FAILED*\n"
                    "Account: *" + name + "*\n\n" +
                    pair + " " + dirn + " Grade " + grade + "\n" +
                    "*Reason:* " + str(error_msg)
                ),
                parse_mode=ParseMode.MARKDOWN,
            )
            return

        # Log trade
        TRADE_LOG.append({
            "account":      name,
            "account_id":   acct,
            "pair":         pair,
            "direction":    dirn,
            "grade":        grade,
            "entry":        entry,
            "sl":           sl,
            "tp1":          signal.get("tp1"),
            "tp2":          signal.get("tp2"),
            "lots":         lot_size,
            "risk_usd":     risk_usd,
            "order_id":     order_id,
            "executed_at":  datetime.now(UTC).isoformat(),
            "status":       "OPEN",
        })

        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=(
                "✅ *TRADE EXECUTED*\n"
                "Account: *" + name + "*\n\n" +
                pair + " " + dirn + " — Grade " + grade + "\n" +
                "Entry: " + str(entry) + " | SL: " + str(sl) + "\n" +
                "Lots: " + str(lot_size) + " | Risk: $" + str(round(risk_usd, 2)) +
                " (" + str(round(risk_pct * 100, 1)) + "%)\n" +
                "Order ID: " + str(order_id)
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        log.info(name + ": executed " + pair + " " + dirn + " lots=" + str(lot_size))

    except Exception as e:
        err_str = str(e)
        if "SLIPPAGE_EXCEEDED" in err_str:
            reason = err_str.replace("SLIPPAGE_EXCEEDED: ", "")
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=(
                    "⏭ *TRADE SKIPPED — Slippage*\n"
                    "Account: *" + name + "*\n\n" +
                    pair + " " + dirn + "\n" + reason +
                    "\n\n_Levz protected the account from a bad fill._"
                ),
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            log.error(name + " execution error: " + err_str)
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text="⚠️ *TRADE EXCEPTION*\nAccount: *" + name + "*\n" + pair + " " + dirn + "\n*Error:* " + err_str,
                parse_mode=ParseMode.MARKDOWN,
            )


async def execute_signal_all_accounts(bot, signal: dict):
    """Execute a signal across all configured MT5 accounts simultaneously."""
    if not ACCOUNTS:
        log.warning("No accounts configured — signal not executed")
        return

    pair  = signal.get("pair", "?")
    dirn  = signal.get("direction", "?")
    grade = signal.get("grade", "?")

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=(
            "📡 *Signal executing on " + str(len(ACCOUNTS)) + " account(s)*\n" +
            pair + " " + dirn + " — Grade " + grade
        ),
        parse_mode=ParseMode.MARKDOWN,
    )

    # Execute on all accounts simultaneously
    tasks = [execute_signal_for_account(bot, signal, acct) for acct in ACCOUNTS]
    await asyncio.gather(*tasks, return_exceptions=True)


# ── Telegram handlers ──────────────────────────────────────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Listen for signal messages from XAU Signals bridge or manual forward.
    """
    if not update.message or not update.message.text:
        return

    chat_id = str(update.effective_chat.id)
    log.info("Message received from chat_id: " + chat_id + " | expected: " + str(TELEGRAM_CHAT_ID))

    if chat_id != str(TELEGRAM_CHAT_ID):
        log.info("Ignoring message from unexpected chat: " + chat_id)
        return

    text   = update.message.text
    log.info("Processing message: " + text[:80])
    signal = parse_signal_from_message(text)
    log.info("Signal parsed: " + str(signal is not None) + " | APEX SIGNAL in text: " + str("APEX SIGNAL" in text))

    if signal:
        SIGNAL_QUEUE.append(signal)
        queue_count = len(SIGNAL_QUEUE)
        await update.message.reply_text(
            "Signal queued: " + signal["pair"] + " " + signal["direction"] + " Grade " + signal["grade"] + "\n" +
            "Queue size: " + str(queue_count) + " signal(s). Executing by grade in " + str(QUEUE_WINDOW_SECONDS) + "s...",
            parse_mode=ParseMode.MARKDOWN,
        )
        # Schedule queue flush after QUEUE_WINDOW_SECONDS
        # Uses job_queue to avoid duplicate flushes
        jobs = ctx.job_queue.get_jobs_by_name("flush_queue")
        if not jobs:
            ctx.job_queue.run_once(
                lambda c: asyncio.create_task(flush_queue(c.bot)),
                when=QUEUE_WINDOW_SECONDS,
                name="flush_queue",
            )


async def handle_approval(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle approve/reject button presses for drawdown-close trades."""
    query   = update.callback_query
    await query.answer()
    data    = query.data

    if data.startswith("approve_"):
        trade_id = data.replace("approve_", "")
        signal   = PENDING_TRADES.pop(trade_id, None)
        if not signal:
            await query.edit_message_text("Trade expired or already processed.")
            return
        info     = await get_account_info()
        lot_size = calc_lot_size(signal["pair"], signal["entry_price"], signal["stop_loss"], info["balance"])
        risk_usd = min(info["balance"] * MAX_RISK_PCT, MAX_RISK_USD)
        await query.edit_message_text("Approved. Executing " + signal["pair"] + " " + signal["direction"] + "...")
        await execute_trade(ctx.bot, signal, lot_size, risk_usd)

    elif data.startswith("reject_"):
        trade_id = data.replace("reject_", "")
        signal   = PENDING_TRADES.pop(trade_id, None)
        pair     = signal["pair"] if signal else "unknown"
        await query.edit_message_text("Trade skipped: " + pair + " — you chose not to execute near drawdown limit.")


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "APEX MT5 Execution Bot\n\n"
        "Monitoring for XAU Signals. Commands:\n"
        "/account — live account status\n"
        "/risk — current risk metrics\n"
        "/trades — open trades\n"
        "/log — today's executed trades\n"
        "/status — bot status",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_account(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return
    info      = await get_account_info()
    daily_pnl  = await get_daily_pnl()
    balance    = info["balance"]
    lim        = dynamic_limits(balance)
    daily_loss = abs(min(daily_pnl, 0))
    total_loss = max(0, ACCOUNT_BALANCE_START - balance)

    daily_pct = daily_loss / lim["max_daily_usd"] * 100 if lim["max_daily_usd"] else 0
    total_pct = total_loss / lim["max_total_usd"] * 100 if lim["max_total_usd"] else 0

    daily_bar = min(10, round(daily_pct / 10))
    total_bar = min(10, round(total_pct / 10))
    d_bar = "#" * daily_bar + "." * (10 - daily_bar)
    t_bar = "#" * total_bar + "." * (10 - total_bar)
    closed_today = round(DAILY_CLOSED_PNL, 2)

    await update.message.reply_text(
        "ACCOUNT STATUS\n\n"
        "Balance:  $" + str(round(balance, 2)) + "\n"
        "Equity:   $" + str(round(info["equity"], 2)) + "\n"
        "Open P&L: $" + str(round(info["profit"], 2)) + "\n"
        "Closed today: $" + str(closed_today) + "\n\n"
        "Daily loss:  $" + str(round(daily_loss, 2)) + " / $" + str(round(lim["max_daily_usd"], 2)) + "\n"
        "`" + d_bar + "` " + str(round(daily_pct, 1)) + "% / 100%\n\n"
        "Total loss:  $" + str(round(total_loss, 2)) + " / $" + str(round(lim["max_total_usd"], 2)) + "\n"
        "`" + t_bar + "` " + str(round(total_pct, 1)) + "% / 100%",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_risk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return
    info = await get_account_info()
    balance = info["balance"]
    max_risk = min(balance * MAX_RISK_PCT, MAX_RISK_USD)
    await update.message.reply_text(
        "RISK METRICS\n\n"
        "Account balance: $" + str(round(balance, 2)) + "\n"
        "Max risk per trade: $" + str(round(max_risk, 2)) + " (" + str(int(MAX_RISK_PCT * 100)) + "%)\n\n"
        "Daily drawdown limit:  $" + str(MAX_DAILY_LOSS_USD) + " (" + str(int(MAX_DAILY_LOSS_PCT * 100)) + "%)\n"
        "Alert at:              $" + str(ALERT_DAILY_USD) + " used\n\n"
        "Total drawdown limit:  $" + str(MAX_TOTAL_LOSS_USD) + " (" + str(int(MAX_TOTAL_LOSS_PCT * 100)) + "%)\n"
        "Alert at:              $" + str(ALERT_TOTAL_USD) + " used\n\n"
        "News trading: UNRESTRICTED (add-on purchased)",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_trades(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return
    try:
        positions = await meta_get("/positions")
        if not positions:
            await update.message.reply_text("No open trades.")
            return
        lines = ["OPEN TRADES — " + str(len(positions)) + "\n"]
        total_pnl = 0
        for p in positions:
            pnl = p.get("profit", 0)
            total_pnl += pnl
            em = "+" if pnl >= 0 else ""
            lines.append(
                p.get("symbol", "?") + " " + p.get("type", "?").replace("POSITION_TYPE_", "") +
                "  lots: " + str(p.get("volume", "?")) +
                "  P&L: " + em + str(round(pnl, 2))
            )
        lines.append("\nTotal open P&L: $" + str(round(total_pnl, 2)))
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await update.message.reply_text("Error fetching positions: " + str(e))


async def cmd_log(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return
    today = datetime.now(UTC).date().isoformat()
    today_trades = [t for t in TRADE_LOG if t.get("executed_at", "").startswith(today)]
    if not today_trades:
        await update.message.reply_text("No trades executed today.")
        return
    lines = ["TODAY'S TRADES — " + str(len(today_trades)) + "\n"]
    for t in today_trades:
        lines.append(
            t["pair"] + " " + t["direction"] + " " + t["grade"] +
            "  lots:" + str(t["lots"]) + "  risk:$" + str(round(t["risk_usd"], 2))
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return
    pending = len(PENDING_TRADES)
    logged  = len(TRADE_LOG)
    queued  = len(SIGNAL_QUEUE)
    await update.message.reply_text(
        "APEX MT5 Bot - Online\n\n" +
        "Signals in queue: " + str(queued) + "\n" +
        "Pending approvals: " + str(pending) + "\n" +
        "Trades logged: " + str(logged) + "\n\n" +
        "Grade priority: A+ > A > B+ > B\n" +
        "Queue window: " + str(QUEUE_WINDOW_SECONDS) + "s\n\n" +
        "Max risk per trade: " + str(int(MAX_RISK_PCT * 100)) + "%\n" +
        "Daily limit: " + str(int(MAX_DAILY_LOSS_PCT * 100)) + "%\n" +
        "Total limit: " + str(int(MAX_TOTAL_LOSS_PCT * 100)) + "%\n" +
        "News trading: UNRESTRICTED (add-on purchased)\n\n" +
        "Listening for XAU Signals...",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── Scheduled drawdown monitor ─────────────────────────────────────────────
async def monitor_drawdown(ctx: ContextTypes.DEFAULT_TYPE):
    """Run every 30 minutes to check account health proactively."""
    safe, reason = await check_drawdown_limits(ctx.bot)
    if not safe:
        await ctx.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="DRAWDOWN MONITOR\n\n" + reason,
            parse_mode=ParseMode.MARKDOWN,
        )
        log.warning("Drawdown limit reached: " + reason)


async def monitor_breakeven(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Check open positions every 5 minutes.
    1. If TP1 reached — move SL to breakeven
    2. If position no longer exists — mark as closed and record P&L
    """
    check_daily_reset()
    open_trades = [t for t in TRADE_LOG if t.get("status") == "OPEN"]
    if not open_trades:
        return

    # Get current open positions from MetaAPI
    try:
        positions = await meta_get("/positions")
        open_position_ids = {str(p.get("id", "")) for p in positions}
    except Exception as e:
        log.error("monitor_breakeven positions fetch: " + str(e))
        return

    for trade in open_trades:
        pair   = trade.get("pair")
        dirn   = trade.get("direction")
        entry  = trade.get("entry")
        tp1    = trade.get("tp1")
        tp2    = trade.get("tp2")
        pos_id = str(trade.get("position_id", ""))

        # Check if position was closed
        if pos_id and pos_id not in open_position_ids and pos_id != "":
            # Position closed — estimate P&L from price vs entry
            try:
                current = await fetch_current_price(pair)
                if dirn == "BUY":
                    pnl_pips = calc_pips(pair, current, entry)
                    if current < entry:
                        pnl_pips = -pnl_pips
                else:
                    pnl_pips = calc_pips(pair, entry, current)
                    if current > entry:
                        pnl_pips = -pnl_pips

                # Estimate USD P&L
                pnl_usd = pnl_pips * calc_pip_value(pair) * trade.get("lots", 0.01)
                trade["status"]     = "CLOSED"
                trade["closed_pnl"] = round(pnl_usd, 2)
                closed_at           = datetime.now(UTC)
                trade["closed_at"]  = closed_at.isoformat()
                record_closed_pnl(pnl_usd)

                result = "WIN" if pnl_usd > 0 else "LOSS"
                em     = "✅" if result == "WIN" else "❌"

                # Calculate duration from entry to close
                duration_str = "unknown"
                executed_at  = trade.get("executed_at", "")
                if executed_at:
                    try:
                        exec_dt  = datetime.fromisoformat(executed_at)
                        duration = closed_at.replace(tzinfo=None) - exec_dt.replace(tzinfo=None)
                        total_m  = int(duration.total_seconds() / 60)
                        if total_m >= 60:
                            duration_str = str(total_m // 60) + "h " + str(total_m % 60) + "m"
                        else:
                            duration_str = str(total_m) + "m"
                    except Exception:
                        duration_str = "unknown"

                # Format signal fired time
                signal_fired = trade.get("signal_fired_at", "")
                if signal_fired:
                    try:
                        sf_dt      = datetime.fromisoformat(signal_fired.replace("Z", "+00:00"))
                        signal_str = sf_dt.strftime("%H:%M UTC")
                    except Exception:
                        signal_str = str(signal_fired)[:16]
                else:
                    signal_str = "unknown"

                # Format entry time
                entry_str = executed_at[:16].replace("T", " ") + " UTC" if executed_at else "unknown"

                log.info("Position closed detected: " + pair + " " + result + " $" + str(round(pnl_usd, 2)))
                await ctx.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=(
                        em + " *Position Closed - " + pair + "*\n\n" +
                        "Result: *" + result + "*\n" +
                        "Est. P&L: $" + str(round(pnl_usd, 2)) + "\n\n" +
                        "Signal fired:  " + signal_str + "\n" +
                        "Trade entered: " + entry_str + "\n" +
                        "Closed:        " + closed_at.strftime("%H:%M UTC") + "\n" +
                        "Duration:      " + duration_str + "\n\n" +
                        "Daily closed P&L: $" + str(round(DAILY_CLOSED_PNL, 2))
                    ),
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception as e:
                log.error("Closed trade detection error: " + str(e))
            continue

        if not all([pair, dirn, entry, tp1, pos_id]) or trade.get("breakeven_set"):
            continue

        try:
            current = await fetch_current_price(pair)
        except Exception as e:
            log.error("Price fetch in breakeven monitor: " + str(e))
            continue

        tp1_hit = (dirn == "BUY" and current >= tp1) or (dirn == "SELL" and current <= tp1)

        if tp1_hit:
            be_stop  = get_breakeven_stop(pair, entry, dirn)
            be_pips  = BREAKEVEN_BUFFER_PIPS.get(pair, BREAKEVEN_BUFFER_PIPS["default"])
            try:
                await meta_modify(pos_id, be_stop, tp2 or tp1)
                trade["breakeven_set"] = True
                trade["sl"] = be_stop
                log.info("Breakeven set: " + pair + " SL moved to " + str(be_stop))
                await ctx.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=(
                        "*BREAKEVEN SET - " + str(pair) + "*\n\n" +
                        str(dirn) + " | TP1 reached at " + fp(tp1) + "\n" +
                        "SL moved to *" + str(be_stop) + "* (entry + " + str(be_pips) + " pips)\n" +
                        "Targeting TP2: " + fp(tp2)
                    ),
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception as e:
                log.error("Breakeven modify failed: " + str(e))


async def monitor_order_expiry(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Every 30 minutes — check all OPEN pending limit orders.
    If placed more than valid_for_hours ago and still unfilled, cancel them.
    """
    pending_trades = [
        t for t in TRADE_LOG
        if t.get("status") == "OPEN" and t.get("filled") == False
        and t.get("order_id", "unknown") != "unknown"
    ]
    if not pending_trades:
        return

    now = datetime.now(UTC)

    # Get live pending orders and positions from MetaAPI
    live_orders    = await get_pending_orders()
    live_order_ids = {str(o.get("id", "")) for o in live_orders}

    try:
        live_positions    = await get_open_positions()
        live_position_ids = {str(p.get("id", "")) for p in live_positions}
    except Exception:
        live_position_ids = set()

    for trade in pending_trades:
        order_id  = str(trade.get("order_id", ""))
        pair      = trade.get("pair", "?")
        dirn      = trade.get("direction", "?")
        entry     = trade.get("entry", 0)
        valid_h   = float(trade.get("valid_for_hours", 4))
        placed_at = trade.get("executed_at", "")

        if not placed_at:
            continue

        try:
            placed_dt = parse_dt(placed_at)
            age_hours = (now - placed_dt).total_seconds() / 3600
        except Exception:
            continue

        # Check if it became a live position
        if order_id in live_position_ids:
            trade["filled"]    = True
            trade["filled_at"] = now.isoformat()
            log.info("Order confirmed filled as position: " + order_id)
            continue

        # Check if expired
        if age_hours >= valid_h:
            if order_id in live_order_ids:
                # Still pending in MT5 — cancel it
                try:
                    await meta_cancel_order(order_id)
                    trade["status"]    = "EXPIRED"
                    trade["closed_at"] = now.isoformat()
                    log.info("Order cancelled — expired: " + pair + " " + order_id)
                    await ctx.bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=(
                            "Order Expired and Cancelled\n\n" +
                            pair + " " + dirn + " Grade " + str(trade.get("grade", "?")) + "\n" +
                            "Entry: " + str(entry) + "\n" +
                            "Placed: " + placed_at[:16].replace("T", " ") + " UTC\n" +
                            "Validity: " + str(valid_h) + "h\n" +
                            "Age: " + str(round(age_hours, 1)) + "h\n\n" +
                            "Price never reached the entry level."
                        ),
                    )
                except Exception as e:
                    log.error("Failed to cancel order " + order_id + ": " + str(e))
            else:
                # Not in live orders and not a position
                # Mark as EXPIRED — never assume it filled
                trade["status"]    = "EXPIRED"
                trade["closed_at"] = now.isoformat()
                log.info("Order " + order_id + " not found in MT5 — marked EXPIRED (not filled)")


# ── Main ───────────────────────────────────────────────────────────────────
# ── Signal webhook server ──────────────────────────────────────────────────
async def handle_webhook(request):
    """
    Receive signals directly from the XAU Signals bot via HTTP POST.
    Bypasses Telegram bridge — direct bot-to-bot communication.
    """
    try:
        secret = request.headers.get("X-Secret", "")
        if secret != WEBHOOK_SECRET:
            log.warning("Webhook: invalid secret")
            return web.Response(status=403, text="Forbidden")

        data = await request.json()
        signal_text = data.get("signal_text", "")

        if not signal_text:
            return web.Response(status=400, text="No signal_text")

        signal = parse_signal_from_message(signal_text)
        if signal:
            log.info("Webhook signal received: " + signal.get("pair", "?") + " " + signal.get("direction", "?") + " Grade " + str(signal.get("grade")))
            SIGNAL_QUEUE.append(signal)
            asyncio.create_task(flush_queue_direct())
            return web.Response(status=200, text="OK")
        else:
            # Log first 200 chars to see what arrived
            preview = signal_text[:200].replace("\n", " ")
            log.info("Webhook: no signal parsed. Text preview: " + preview)
            return web.Response(status=200, text="No signal")

    except Exception as e:
        log.error("Webhook error: " + str(e))
        return web.Response(status=500, text=str(e))


async def flush_queue_direct():
    """Flush signal queue and execute across all configured MT5 accounts."""
    await asyncio.sleep(5)
    if not SIGNAL_QUEUE:
        return

    # Weekend guard
    now_utc = datetime.now(UTC)
    weekday = now_utc.weekday()
    if weekday == 5 or weekday == 6 or (weekday == 4 and now_utc.hour >= 22):
        log.info("Weekend — market closed. Signal queue not executed.")
        SIGNAL_QUEUE.clear()
        return

    sorted_signals = sort_queue(list(SIGNAL_QUEUE))
    SIGNAL_QUEUE.clear()

    from telegram import Bot
    bot = Bot(token=TELEGRAM_TOKEN)

    count  = len(sorted_signals)
    grades = [s.get("grade", "?") for s in sorted_signals]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=(
            "📡 *SIGNAL QUEUE — " + str(count) + " signal(s)*\n" +
            "Order: " + " > ".join(grades) + "\n" +
            "Executing on *" + str(len(ACCOUNTS)) + " account(s)*..."
        ),
        parse_mode=ParseMode.MARKDOWN,
    )

    for signal in sorted_signals:
        await execute_signal_all_accounts(bot, signal)
        await asyncio.sleep(1)


async def handle_stats_update(request):
    """Receive stats push from XAU Signals bot and cache it."""
    try:
        secret = request.headers.get("X-Secret", "")
        if secret != WEBHOOK_SECRET:
            return web.Response(status=403, text="Forbidden")
        data = await request.json()
        STATS_CACHE.update(data)
        log.info("Stats cache updated: " + str(data.get("total_trades", 0)) + " trades")
        return web.Response(status=200, text="OK")
    except Exception as e:
        log.error("Stats update error: " + str(e))
        return web.Response(status=500, text=str(e))


async def handle_stats_get(request):
    """Serve stats to website."""
    import json as _json
    data = dict(STATS_CACHE)
    # Remove recent_signals from stats endpoint — separate endpoint for that
    data.pop("recent_signals", None)
    return web.Response(
        text=_json.dumps(data),
        content_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"},
    )


async def handle_recent_get(request):
    """Serve recent signals to website."""
    import json as _json
    signals = STATS_CACHE.get("recent_signals", [])
    return web.Response(
        text=_json.dumps({"signals": signals}),
        content_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"},
    )


async def start_webhook_server():
    """Start the aiohttp webhook server on port 8080."""
    app_web = web.Application()
    app_web.router.add_post("/signal",          handle_webhook)
    app_web.router.add_post("/api/stats/update", handle_stats_update)
    app_web.router.add_get("/api/stats",          handle_stats_get)
    app_web.router.add_get("/api/recent",         handle_recent_get)
    app_web.router.add_get("/health",             lambda r: web.Response(text="OK"))
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    log.info("Webhook server running on port 8080")


def main():
    log.info("Starting APEX MT5 Execution Bot...")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("account", cmd_account))
    app.add_handler(CommandHandler("risk",    cmd_risk))
    app.add_handler(CommandHandler("trades",  cmd_trades))
    app.add_handler(CommandHandler("log",     cmd_log))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CallbackQueryHandler(handle_approval))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Monitor drawdown every 30 minutes
    app.job_queue.run_repeating(
        monitor_drawdown,
        interval=1800,
        first=60,
        name="drawdown_monitor",
    )

    # Monitor breakeven every 5 minutes
    app.job_queue.run_repeating(
        monitor_breakeven,
        interval=300,
        first=120,
        name="breakeven_monitor",
    )

    # Monitor pending order expiry every 30 minutes
    app.job_queue.run_repeating(
        monitor_order_expiry,
        interval=1800,
        first=300,
        name="expiry_monitor",
    )

    log.info("APEX MT5 Bot running on webhook + Telegram polling.")

    async def post_init(application):
        await start_webhook_server()

    app.post_init = post_init
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
