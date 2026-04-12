# APEX MT5 Execution Bot — Setup Guide

Automatically executes XAU Signals on your Soar Funding MT5 account
with full rule compliance and 3% max risk per trade.

---

## What it does

1. You forward a signal from XAU Signals to this bot
2. Bot checks: grade (A/B only), news window, daily drawdown, total drawdown
3. If all clear: executes the trade on MT5 automatically
4. If close to drawdown limit: asks you first with YES/NO buttons
5. Monitors account health every 30 minutes

---

## Risk rules enforced

| Rule | Value |
|---|---|
| Max risk per trade | 3% ($300 on $10k) |
| Daily drawdown limit | 4% ($400) — Soar Funding rule |
| Total drawdown limit | 8% ($800) — Soar Funding rule |
| Alert threshold | Within 1% of either limit |
| News window | 5 min before/after high-impact events |
| Min grade to trade | B or above |

---

## Step 1 — Connect MT5 via MetaAPI (free)

MetaAPI is a cloud service that connects Python to MetaTrader accounts.

1. Go to **metaapi.cloud** and create a free account
2. Click **Add account**
3. Enter your Soar Funding MT5 login, password, and server name
   (find these in your Soar Funding dashboard)
4. Copy your **Auth Token** from the API access section
5. Copy your **Account ID** from the account you just added

---

## Step 2 — Create a new Telegram bot for execution

This should be a SEPARATE bot from your XAU Signals bot.
You don't want signal subscribers seeing execution confirmations.

1. Go to @BotFather → /newbot
2. Name it something private like "APEX Executor" 
3. Copy the token

---

## Step 3 — Deploy on Render

Same process as the XAU Signals bot:

1. Create a new GitHub repo called `apex-mt5-bot`
2. Upload `apex_mt5_bot.py` and `requirements.txt`
3. Create a new Background Worker on Render
4. Connect the GitHub repo
5. Set environment variables:
   - TELEGRAM_BOT_TOKEN = your new executor bot token
   - TELEGRAM_CHAT_ID = your personal chat ID (same as before)
   - METAAPI_TOKEN = from metaapi.cloud
   - MT5_ACCOUNT_ID = from metaapi.cloud
   - TWELVE_DATA_KEY = 22719eb0a9804e4da8b0247dc5545f7b
6. Build command: pip install -r requirements.txt
7. Start command: python apex_mt5_bot.py

---

## Step 4 — How to use it

When XAU Signals sends you a signal on Telegram:
1. Forward that message to your APEX Executor bot
2. The bot parses it, runs all checks, and either:
   - Executes immediately (all clear)
   - Asks you for approval (close to drawdown)
   - Blocks it (limit hit, news window, bad grade)

---

## Bot commands

| Command | What it does |
|---|---|
| /account | Live balance, equity, drawdown status with progress bars |
| /risk | Current risk settings and limits |
| /trades | All open positions with live P&L |
| /log | Today's executed trades |
| /status | Bot health and settings |

---

## Important notes

- Always test on a DEMO account first before running on your funded account
- MetaAPI free tier allows 1 account connection — sufficient for this use case
- The bot forwards the signal from text — ensure the XAU Signals message format
  hasn't changed (it looks for "APEX SIGNAL" or "XAU SIGNAL" in the message)
- News data comes from Twelve Data's economic calendar — same API key you already have
