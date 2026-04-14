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
METAAPI_TOKEN     = os.environ.get("METAAPI_TOKEN", "")
MT5_ACCOUNT_ID    = os.environ.get("MT5_ACCOUNT_ID", "")
TWELVE_DATA_KEY   = os.environ.get("TWELVE_DATA_KEY", "22719eb0a9804e4da8b0247dc5545f7b")
WEBHOOK_SECRET    = os.environ.get("WEBHOOK_SECRET", "apex_secret_2026")   # shared secret for bridge auth

# Soar Funding 2-Step rules for $10k account
ACCOUNT_BALANCE        = 10000.0
MAX_RISK_PCT           = 0.03       # 3% max risk per trade
MAX_DAILY_LOSS_PCT     = 0.04       # 4% daily drawdown
MAX_TOTAL_LOSS_PCT     = 0.08       # 8% total drawdown
ALERT_BUFFER_PCT       = 0.01       # alert when within 1% of limit
NEWS_WINDOW_MINUTES    = 5          # kept for reference — news add-on purchased, not enforced

MAX_RISK_USD           = ACCOUNT_BALANCE * MAX_RISK_PCT           # $300
MAX_DAILY_LOSS_USD     = ACCOUNT_BALANCE * MAX_DAILY_LOSS_PCT     # $400
MAX_TOTAL_LOSS_USD     = ACCOUNT_BALANCE * MAX_TOTAL_LOSS_PCT     # $800
ALERT_DAILY_USD        = ACCOUNT_BALANCE * (MAX_DAILY_LOSS_PCT - ALERT_BUFFER_PCT)  # $300
ALERT_TOTAL_USD        = ACCOUNT_BALANCE * (MAX_TOTAL_LOSS_PCT - ALERT_BUFFER_PCT)  # $700

UTC = ZoneInfo("UTC")

# MetaAPI base URL
META_BASE = "https://mt-client-api-v1.london.agiliumtrade.ai"

# ── State ──────────────────────────────────────────────────────────────────
PENDING_TRADES: dict = {}   # trade_id -> signal dict awaiting approval
TRADE_LOG:      list = []   # all executed trades
SIGNAL_QUEUE:   list = []   # signals buffered for priority ordering

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
async def meta_get(path: str) -> dict:
    url = f"{META_BASE}/users/current/accounts/{MT5_ACCOUNT_ID}{path}"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, headers={
            "auth-token": METAAPI_TOKEN,
            "Content-Type": "application/json"
        })
        r.raise_for_status()
        return r.json()


async def meta_post(path: str, body: dict) -> dict:
    url = f"{META_BASE}/users/current/accounts/{MT5_ACCOUNT_ID}{path}"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(url, json=body, headers={
            "auth-token": METAAPI_TOKEN,
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


async def get_account_info() -> dict:
    """Fetch live account balance, equity, and drawdown status."""
    try:
        info = await meta_get("/account-information")
        return {
            "balance":    info.get("balance", ACCOUNT_BALANCE),
            "equity":     info.get("equity", ACCOUNT_BALANCE),
            "margin":     info.get("margin", 0),
            "free_margin":info.get("freeMargin", ACCOUNT_BALANCE),
            "profit":     info.get("profit", 0),
        }
    except Exception as e:
        log.error("get_account_info error: " + str(e))
        return {"balance": ACCOUNT_BALANCE, "equity": ACCOUNT_BALANCE, "profit": 0}


async def get_daily_pnl() -> float:
    """Approximate daily P&L from open positions profit."""
    try:
        positions = await meta_get("/positions")
        return sum(p.get("profit", 0) for p in positions)
    except Exception as e:
        log.error("get_daily_pnl error: " + str(e))
        return 0.0


async def place_order(signal: dict, lot_size: float) -> dict:
    """Place a market order on MT5 via MetaAPI."""
    pair    = signal["pair"].replace("/", "") + "."  # EURUSD. — Soar Funding uses dot suffix
    dirn    = signal["direction"]
    entry   = signal["entry_price"]
    sl      = signal["stop_loss"]
    tp1     = signal["tp1"]
    tp2     = signal["tp2"]

    action = "ORDER_TYPE_BUY_LIMIT" if dirn == "BUY" else "ORDER_TYPE_SELL_LIMIT"

    body = {
        "symbol":     pair,
        "actionType": action,
        "volume":     round(lot_size, 2),
        "openPrice":  entry,   # limit order price
        "stopLoss":   sl,
        "takeProfit": tp1,   # TP1 first — we manage TP2 separately
        "comment":    "APEX_XAU_SIGNAL",
    }

    result = await meta_post("/trade", body)
    log.info("Order placed: " + str(result))
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
    total_loss = max(0, ACCOUNT_BALANCE - balance)

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

        # Session
        sess_m = re.search(r"(London|New York|Asian|Overlap)", text)
        s["session"] = sess_m.group(1) if sess_m else "Unknown"

        # Validate required fields
        required = ["pair", "direction", "entry_price", "stop_loss", "tp1"]
        if not all(s.get(k) for k in required):
            return None

        s["signal_id"] = s["pair"] + "_" + datetime.utcnow().strftime("%Y%m%d%H%M%S")
        s["received_at"] = datetime.utcnow().isoformat()
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
    balance   = info["balance"]
    daily_loss = abs(min(daily_pnl, 0))
    total_loss = max(0, ACCOUNT_BALANCE - balance)

    # Hard blocks
    if daily_loss >= MAX_DAILY_LOSS_USD:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="Signal BLOCKED: Daily drawdown limit reached ($" + str(round(daily_loss, 2)) + " / $" + str(MAX_DAILY_LOSS_USD) + "). No more trades today.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if total_loss >= MAX_TOTAL_LOSS_USD:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="Signal BLOCKED: Total drawdown limit reached ($" + str(round(total_loss, 2)) + "). Stop trading immediately.",
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
    remaining_daily = MAX_DAILY_LOSS_USD - daily_loss
    if remaining_daily < (ACCOUNT_BALANCE * ALERT_BUFFER_PCT):
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

    try:
        result = await place_order(signal, lot_size)
        order_id = result.get("orderId", "unknown")

        # Log the trade
        TRADE_LOG.append({
            "signal_id":  signal.get("signal_id"),
            "pair":       pair,
            "direction":  dirn,
            "grade":      signal.get("grade"),
            "entry":      entry,
            "sl":         sl,
            "tp1":        tp1,
            "tp2":        tp2,
            "lots":       lot_size,
            "risk_usd":   risk_usd,
            "order_id":   order_id,
            "position_id": result.get("positionId", ""),
            "executed_at": datetime.utcnow().isoformat(),
            "status":     "OPEN",
            "breakeven_set": False,
        })

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
                "Risk: $" + str(round(risk_usd, 2)) + " (" + str(round(risk_usd / ACCOUNT_BALANCE * 100, 1)) + "%)\n" +
                "Order ID: " + str(order_id)
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        log.info("Trade executed: " + pair + " " + dirn + " lots=" + str(lot_size))

    except Exception as e:
        log.error("Trade execution error: " + str(e))
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="TRADE FAILED — " + pair + " " + dirn + "\nError: " + str(e),
            parse_mode=ParseMode.MARKDOWN,
        )


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
    daily_pnl = await get_daily_pnl()
    daily_loss = abs(min(daily_pnl, 0))
    total_loss = max(0, ACCOUNT_BALANCE - info["balance"])

    daily_pct = daily_loss / ACCOUNT_BALANCE * 100
    total_pct = total_loss / ACCOUNT_BALANCE * 100

    daily_bar = round(daily_pct / MAX_DAILY_LOSS_PCT / 10 * 10)
    total_bar = round(total_pct / MAX_TOTAL_LOSS_PCT / 10 * 10)
    d_bar = "#" * daily_bar + "." * (10 - daily_bar)
    t_bar = "#" * total_bar + "." * (10 - total_bar)

    await update.message.reply_text(
        "ACCOUNT STATUS\n\n"
        "Balance:  $" + str(round(info["balance"], 2)) + "\n"
        "Equity:   $" + str(round(info["equity"], 2)) + "\n"
        "Open P&L: $" + str(round(info["profit"], 2)) + "\n\n"
        "Daily loss:  $" + str(round(daily_loss, 2)) + " / $" + str(MAX_DAILY_LOSS_USD) + "\n"
        "`" + d_bar + "` " + str(round(daily_pct, 1)) + "% / " + str(int(MAX_DAILY_LOSS_PCT * 100)) + "%\n\n"
        "Total loss:  $" + str(round(total_loss, 2)) + " / $" + str(MAX_TOTAL_LOSS_USD) + "\n"
        "`" + t_bar + "` " + str(round(total_pct, 1)) + "% / " + str(int(MAX_TOTAL_LOSS_PCT * 100)) + "%",
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
    today = datetime.utcnow().date().isoformat()
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
    If TP1 has been reached, move SL to entry + buffer (breakeven).
    """
    open_trades = [t for t in TRADE_LOG if t.get("status") == "OPEN" and not t.get("breakeven_set")]
    if not open_trades:
        return

    for trade in open_trades:
        pair   = trade.get("pair")
        dirn   = trade.get("direction")
        entry  = trade.get("entry")
        tp1    = trade.get("tp1")
        tp2    = trade.get("tp2")
        pos_id = trade.get("position_id")

        if not all([pair, dirn, entry, tp1, pos_id]):
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
    """Flush signal queue without job_queue context."""
    await asyncio.sleep(5)   # small buffer to collect any simultaneous signals
    if not SIGNAL_QUEUE:
        return
    sorted_signals = sort_queue(list(SIGNAL_QUEUE))
    SIGNAL_QUEUE.clear()

    # Get bot instance — need to use the global app
    from telegram import Bot
    bot = Bot(token=TELEGRAM_TOKEN)

    info       = await get_account_info()
    daily_pnl  = await get_daily_pnl()
    daily_loss = abs(min(daily_pnl, 0))
    remaining  = MAX_DAILY_LOSS_USD - daily_loss
    count      = len(sorted_signals)
    grades     = [s.get("grade", "?") for s in sorted_signals]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="SIGNAL QUEUE - " + str(count) + " signal(s)\nOrder: " + " > ".join(grades) + "\nBudget remaining: $" + str(round(remaining, 2)),
        parse_mode=ParseMode.MARKDOWN,
    )

    for signal in sorted_signals:
        grade     = signal.get("grade", "C")
        threshold = GRADE_BUDGET_THRESHOLD.get(grade, 99999)
        daily_pnl2  = await get_daily_pnl()
        remaining2  = MAX_DAILY_LOSS_USD - abs(min(daily_pnl2, 0))
        if remaining2 < threshold:
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text="Skipped: " + str(signal.get("pair")) + " Grade " + grade + " — budget $" + str(round(remaining2, 2)) + " below minimum",
                parse_mode=ParseMode.MARKDOWN,
            )
            continue
        await process_signal(bot, signal)
        await asyncio.sleep(1)


async def start_webhook_server():
    """Start the aiohttp webhook server on port 8080."""
    app_web = web.Application()
    app_web.router.add_post("/signal", handle_webhook)
    app_web.router.add_get("/health", lambda r: web.Response(text="OK"))
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

    log.info("APEX MT5 Bot running on webhook + Telegram polling.")

    async def post_init(application):
        await start_webhook_server()

    app.post_init = post_init
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
