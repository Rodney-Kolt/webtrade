# Telegram Signal Assistant

Listens to Telegram channels for trading signals, applies configurable safety
filters, and forwards clean plain-text alerts to your personal Saved Messages
(or any chat you choose). No trades are executed automatically – every alert
ends with **"MANUAL TRADE – open Pocket Option yourself."**

---

## How it works

1. The app authenticates to Telegram as **your user account** (not a bot) using
   the [Telethon](https://docs.telethon.dev) library.
2. It watches every message in the channels you configure.
3. When a message matches a trading-signal pattern (e.g. `BUY EUR/USD CALL 5min`),
   it runs the signal through safety filters.
4. If the signal passes, a formatted plain-text alert is sent to your Saved
   Messages (or a specified chat).
5. A lightweight Flask server runs on port **10000** and exposes a `/health`
   endpoint for Render's health checks.

---

## Quick start (local)

### 1. Get Telegram API credentials

Go to <https://my.telegram.org/apps>, log in, and create an application.
Copy the **App api_id** and **App api_hash**.

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your real values
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run

```bash
python app.py
```

On the **first run** Telethon will prompt you for your phone number and the
confirmation code Telegram sends you. After that a `signal_assistant.session`
file is created and subsequent runs authenticate automatically.

---

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `TG_API_ID` | ✅ | — | Telegram API ID from my.telegram.org |
| `TG_API_HASH` | ✅ | — | Telegram API hash from my.telegram.org |
| `TG_PHONE` | ✅ | — | Your phone number (international format) |
| `TG_CHANNELS` | ✅ | — | Comma-separated channel usernames or IDs |
| `TG_MY_CHAT_ID` | ✅ | — | Destination chat (`me` or numeric ID) |
| `PAYOUT_THRESHOLD` | ❌ | `70` | Minimum payout % to accept a signal |
| `COOLDOWN_SECONDS` | ❌ | `60` | Duplicate-signal suppression window |
| `TRADING_HOURS_START` | ❌ | *(any)* | UTC start of allowed trading window (HH:MM) |
| `TRADING_HOURS_END` | ❌ | *(any)* | UTC end of allowed trading window (HH:MM) |
| `BLACKLISTED_ASSETS` | ❌ | *(none)* | Comma-separated asset pairs to ignore |

---

## Deploying on Render

### 1. Push your code to GitHub (without `.env` or `*.session` files)

Add these lines to `.gitignore`:

```
.env
*.session
*.session-journal
```

### 2. Create a new Render Web Service

- **Environment**: Python 3
- **Build command**: `pip install -r requirements.txt`
- **Start command**: `python app.py`
- **Port**: `10000`

### 3. Add environment variables in Render's dashboard

Go to **Environment → Environment Variables** and add every variable from
`.env.example` with your real values.

### 4. Handle the first-time Telethon login

Telethon needs an interactive login the first time. The easiest approach is:

1. Run `python app.py` **locally** once to complete the phone/code challenge.
2. Upload the generated `signal_assistant.session` file to Render as a
   **Secret File** (Dashboard → Environment → Secret Files, path:
   `signal_assistant.session`).
3. Deploy – Render will find the session file and skip the interactive prompt.

### 5. Health check

Render will ping `https://<your-app>.onrender.com/health`. The endpoint returns
`200 OK` as long as the Flask server is running.

---

## Signal patterns recognised

The parser detects messages containing patterns like:

```
BUY EUR/USD CALL 5min
SELL GBP/JPY PUT 3min
EUR/USD CALL 5min 85%
CALL BTC/USD 1min
BUY AUD/CAD PUT 2m 78%
```

Extracted fields: **asset**, **direction** (CALL/PUT), **timeframe**, and
optionally **payout %**.

---

## Safety filters (in order)

1. **Asset blacklist** – drops signals for configured pairs.
2. **Payout threshold** – drops signals whose payout is below the minimum
   (only applied when the signal message includes a payout value).
3. **Trading hours** – drops signals outside the configured UTC window.
4. **Cooldown** – drops duplicate signals within the configured window.

---

## Example alert

```
TRADING SIGNAL RECEIVED
──────────────────────────────
Asset     : EUR/USD
Direction : CALL
Timeframe : 5min
Payout    : 85%
Received  : 2026-04-30 14:23:07 UTC
Source    : My Signal Channel
──────────────────────────────
MANUAL TRADE – open Pocket Option yourself.
```

---

## Disclaimer

This tool is for informational purposes only. It does **not** execute trades.
All trading decisions are made manually by you. Trading binary options carries
significant financial risk.
