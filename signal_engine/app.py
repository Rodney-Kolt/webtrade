"""
Fundamental OTC Signal Engine
==============================
Generates manual trading signal alerts for Pocket Option OTC instruments
based on real macroeconomic calendar data and news sentiment.

SAFETY: This engine NEVER connects to any trading platform API.
        All signals are informational only. Trade manually.
"""

import asyncio
import logging
import os
import threading
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

import requests
from dotenv import load_dotenv
from flask import Flask
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------------
# Logging – console + rotating file
# ---------------------------------------------------------------------------
LOG_FILE = "signal_engine.log"

log_formatter = logging.Formatter(
    "%(asctime)s UTC | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logging.Formatter.converter = time.gmtime  # force UTC timestamps
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

log = logging.getLogger("signal_engine")

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(f"Required env var '{name}' is not set.")
    return val


def _optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


TG_API_ID: int = int(_require("TG_API_ID"))
TG_API_HASH: str = _require("TG_API_HASH")
TG_PHONE: str = _require("TG_PHONE")
TG_MY_CHAT_ID: str | int = (
    "me"
    if _require("TG_MY_CHAT_ID").lower() == "me"
    else int(_require("TG_MY_CHAT_ID"))
)

FINNWORLDS_API_KEY: str = _require("FINNWORLDS_API_KEY")
NEWS_API_KEY: str = _require("NEWS_API_KEY")

SCAN_INTERVAL_SECONDS: int = int(_optional("SCAN_INTERVAL_SECONDS", "900"))  # 15 min
COOLDOWN_SECONDS: int = int(_optional("COOLDOWN_SECONDS", "3600"))           # 1 hour

log.info(
    "Config loaded | scan_interval=%ds | cooldown=%ds",
    SCAN_INTERVAL_SECONDS,
    COOLDOWN_SECONDS,
)

# ---------------------------------------------------------------------------
# OTC Instrument Map
# Maps currency codes to the OTC pairs they appear in, with their side.
# side: "base" means the currency is the base (left), "quote" means right.
# ---------------------------------------------------------------------------
OTC_INSTRUMENTS: list[dict] = [
    {"name": "EUR/USD OTC", "base": "EUR", "quote": "USD"},
    {"name": "GBP/USD OTC", "base": "GBP", "quote": "USD"},
    {"name": "USD/JPY OTC", "base": "USD", "quote": "JPY"},
    {"name": "USD/CHF OTC", "base": "USD", "quote": "CHF"},
    {"name": "AUD/USD OTC", "base": "AUD", "quote": "USD"},
    {"name": "USD/CAD OTC", "base": "USD", "quote": "CAD"},
    {"name": "NZD/USD OTC", "base": "NZD", "quote": "USD"},
    {"name": "EUR/GBP OTC", "base": "EUR", "quote": "GBP"},
    {"name": "EUR/JPY OTC", "base": "EUR", "quote": "JPY"},
    {"name": "GBP/JPY OTC", "base": "GBP", "quote": "JPY"},
]

# Map country codes from Finnworlds to currency codes
COUNTRY_TO_CURRENCY: dict[str, str] = {
    "us": "USD",
    "gb": "GBP",
    "eu": "EUR",
    "jp": "JPY",
    "au": "AUD",
    "ca": "CAD",
    "nz": "NZD",
    "ch": "CHF",
}

# Event keywords that indicate bullish/bearish bias when actual beats consensus
BULLISH_EVENT_KEYWORDS: list[str] = [
    "employment", "nonfarm", "payroll", "gdp", "retail sales",
    "manufacturing", "pmi", "cpi", "interest rate", "rate decision",
    "trade balance", "consumer confidence", "industrial production",
]

# ---------------------------------------------------------------------------
# Cooldown tracker
# ---------------------------------------------------------------------------
# key: "INSTRUMENT:DIRECTION" → timestamp of last alert sent
_cooldown_cache: dict[str, float] = {}
_cooldown_lock = threading.Lock()


def _is_on_cooldown(instrument: str, direction: str) -> bool:
    key = f"{instrument}:{direction}"
    now = time.monotonic()
    with _cooldown_lock:
        last = _cooldown_cache.get(key)
        if last is not None and (now - last) < COOLDOWN_SECONDS:
            log.debug("Cooldown active for %s | elapsed=%.0fs", key, now - last)
            return True
        _cooldown_cache[key] = now
        return False


# ---------------------------------------------------------------------------
# Module 1 – Fundamental Data (Finnworlds Macroeconomic Calendar)
# ---------------------------------------------------------------------------
FINNWORLDS_URL = "https://api.finnworlds.com/api/v1/macrocalendar"
FINNWORLDS_COUNTRIES = "us,gb,eu,jp,au,ca,nz,ch"


def fetch_macro_events() -> list[dict]:
    """
    Fetch today's high-impact macroeconomic events from Finnworlds.
    Returns a list of raw event dicts.
    """
    log.info("[Macro] Fetching macroeconomic calendar from Finnworlds...")
    try:
        resp = requests.get(
            FINNWORLDS_URL,
            params={"key": FINNWORLDS_API_KEY, "country": FINNWORLDS_COUNTRIES},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        log.debug("[Macro] Raw response keys: %s", list(data.keys()))

        # Finnworlds returns {"result": {"calendar": [...]}} or similar
        events = []
        if isinstance(data, dict):
            # Try common response shapes
            events = (
                data.get("result", {}).get("calendar", [])
                or data.get("calendar", [])
                or data.get("data", [])
                or []
            )
        elif isinstance(data, list):
            events = data

        log.info("[Macro] Total events fetched: %d", len(events))
        return events

    except requests.exceptions.Timeout:
        log.warning("[Macro] Request timed out.")
    except requests.exceptions.HTTPError as exc:
        log.warning("[Macro] HTTP error: %s", exc)
    except Exception as exc:
        log.error("[Macro] Unexpected error: %s", exc, exc_info=True)
    return []


def analyse_macro_events(events: list[dict]) -> dict[str, str]:
    """
    Analyse high-impact events and return a bias map.
    Returns: {"USD": "BULLISH", "EUR": "BEARISH", ...}
    """
    bias: dict[str, str] = {}

    for event in events:
        # Filter: only high-impact (impact == "3" or 3)
        impact = str(event.get("impact", "")).strip()
        if impact != "3":
            continue

        country = str(event.get("country", "")).lower().strip()
        currency = COUNTRY_TO_CURRENCY.get(country)
        if not currency:
            continue

        event_name = str(event.get("event", "") or event.get("name", "")).lower()
        actual_raw = str(event.get("actual", "")).replace("%", "").replace("K", "000").strip()
        consensus_raw = str(event.get("consensus", "") or event.get("forecast", "")).replace("%", "").replace("K", "000").strip()

        if not actual_raw or not consensus_raw:
            log.debug("[Macro] Skipping '%s' – missing actual or consensus.", event_name)
            continue

        try:
            actual = float(actual_raw)
            consensus = float(consensus_raw)
        except ValueError:
            log.debug("[Macro] Could not parse numbers for '%s'.", event_name)
            continue

        # Check if this event type is one we have a rule for
        is_relevant = any(kw in event_name for kw in BULLISH_EVENT_KEYWORDS)
        if not is_relevant:
            log.debug("[Macro] Event '%s' not in relevant keyword list.", event_name)
            continue

        # Determine bias: actual > consensus = bullish for that currency
        if actual > consensus:
            new_bias = "BULLISH"
        elif actual < consensus:
            new_bias = "BEARISH"
        else:
            continue  # no surprise, no signal

        # If we already have a conflicting bias, mark as NEUTRAL
        existing = bias.get(currency)
        if existing and existing != new_bias:
            bias[currency] = "NEUTRAL"
            log.info(
                "[Macro] %s | '%s' | actual=%.2f vs consensus=%.2f → CONFLICTING → NEUTRAL",
                currency, event_name, actual, consensus,
            )
        else:
            bias[currency] = new_bias
            log.info(
                "[Macro] %s | '%s' | actual=%.2f vs consensus=%.2f → %s",
                currency, event_name, actual, consensus, new_bias,
            )

    log.info("[Macro] Currency bias summary: %s", bias if bias else "no signals")
    return bias


# ---------------------------------------------------------------------------
# Module 2 – News Sentiment (NewsAPI)
# ---------------------------------------------------------------------------
NEWS_API_URL = "https://newsapi.org/v2/top-headlines"

RISK_OFF_KEYWORDS: list[str] = [
    "recession", "crash", "sell-off", "selloff", "crisis", "collapse",
    "default", "downturn", "contraction", "layoffs", "bankruptcy",
    "inflation surge", "rate hike", "war", "conflict", "sanctions",
]
RISK_ON_KEYWORDS: list[str] = [
    "growth", "rally", "optimism", "recovery", "expansion", "surge",
    "record high", "beat expectations", "strong jobs", "rate cut",
    "stimulus", "boom", "bullish", "upgrade",
]

# Safe-haven currencies strengthen on RISK OFF
SAFE_HAVEN_CURRENCIES: set[str] = {"USD", "JPY", "CHF"}
# Risk currencies weaken on RISK OFF
RISK_CURRENCIES: set[str] = {"AUD", "NZD", "CAD", "GBP", "EUR"}


def fetch_news_sentiment() -> str:
    """
    Fetch top business headlines and return overall sentiment:
    "RISK_OFF", "RISK_ON", or "NEUTRAL".
    """
    log.info("[News] Fetching top business headlines from NewsAPI...")
    try:
        resp = requests.get(
            NEWS_API_URL,
            params={
                "apiKey": NEWS_API_KEY,
                "category": "business",
                "language": "en",
                "pageSize": 40,
            },
            timeout=15,
        )
        resp.raise_for_status()
        articles = resp.json().get("articles", [])
        log.info("[News] Articles fetched: %d", len(articles))

        risk_off_score = 0
        risk_on_score = 0

        for article in articles:
            title = (article.get("title") or "").lower()
            description = (article.get("description") or "").lower()
            text = f"{title} {description}"

            for kw in RISK_OFF_KEYWORDS:
                if kw in text:
                    risk_off_score += 1
                    log.debug("[News] RISK_OFF hit: '%s' in '%s'", kw, title[:80])

            for kw in RISK_ON_KEYWORDS:
                if kw in text:
                    risk_on_score += 1
                    log.debug("[News] RISK_ON hit: '%s' in '%s'", kw, title[:80])

        log.info(
            "[News] Sentiment scores → RISK_OFF=%d | RISK_ON=%d",
            risk_off_score, risk_on_score,
        )

        if risk_off_score > risk_on_score and risk_off_score >= 2:
            sentiment = "RISK_OFF"
        elif risk_on_score > risk_off_score and risk_on_score >= 2:
            sentiment = "RISK_ON"
        else:
            sentiment = "NEUTRAL"

        log.info("[News] Overall sentiment: %s", sentiment)
        return sentiment

    except requests.exceptions.Timeout:
        log.warning("[News] Request timed out.")
    except requests.exceptions.HTTPError as exc:
        log.warning("[News] HTTP error: %s", exc)
    except Exception as exc:
        log.error("[News] Unexpected error: %s", exc, exc_info=True)
    return "NEUTRAL"


# ---------------------------------------------------------------------------
# Module 3 – OTC Signal Generator
# ---------------------------------------------------------------------------
def generate_signals(
    macro_bias: dict[str, str],
    sentiment: str,
) -> list[dict]:
    """
    Combine macro bias and news sentiment to produce OTC trade signals.
    Returns a list of signal dicts.
    """
    signals: list[dict] = []

    # Build a combined currency bias, starting from macro data
    combined_bias: dict[str, str] = dict(macro_bias)

    # Apply sentiment overlay
    if sentiment == "RISK_OFF":
        log.info("[Signals] Applying RISK_OFF sentiment overlay.")
        for ccy in SAFE_HAVEN_CURRENCIES:
            existing = combined_bias.get(ccy)
            if existing == "BEARISH":
                combined_bias[ccy] = "NEUTRAL"  # conflicting signals
                log.info("[Signals] %s: macro BEARISH vs sentiment BULLISH → NEUTRAL", ccy)
            elif not existing:
                combined_bias[ccy] = "BULLISH"
        for ccy in RISK_CURRENCIES:
            existing = combined_bias.get(ccy)
            if existing == "BULLISH":
                combined_bias[ccy] = "NEUTRAL"
                log.info("[Signals] %s: macro BULLISH vs sentiment BEARISH → NEUTRAL", ccy)
            elif not existing:
                combined_bias[ccy] = "BEARISH"

    elif sentiment == "RISK_ON":
        log.info("[Signals] Applying RISK_ON sentiment overlay.")
        for ccy in RISK_CURRENCIES:
            existing = combined_bias.get(ccy)
            if existing == "BEARISH":
                combined_bias[ccy] = "NEUTRAL"
                log.info("[Signals] %s: macro BEARISH vs sentiment BULLISH → NEUTRAL", ccy)
            elif not existing:
                combined_bias[ccy] = "BULLISH"
        for ccy in SAFE_HAVEN_CURRENCIES:
            existing = combined_bias.get(ccy)
            if existing == "BULLISH":
                combined_bias[ccy] = "NEUTRAL"
                log.info("[Signals] %s: macro BULLISH vs sentiment BEARISH → NEUTRAL", ccy)
            elif not existing:
                combined_bias[ccy] = "BEARISH"

    log.info("[Signals] Combined currency bias: %s", combined_bias)

    # Map biases to OTC instruments
    for instrument in OTC_INSTRUMENTS:
        base = instrument["base"]
        quote = instrument["quote"]
        name = instrument["name"]

        base_bias = combined_bias.get(base, "NEUTRAL")
        quote_bias = combined_bias.get(quote, "NEUTRAL")

        direction: str | None = None
        reason_parts: list[str] = []

        # Base bullish OR quote bearish → CALL (price goes up)
        if base_bias == "BULLISH" and quote_bias != "BULLISH":
            direction = "CALL"
            reason_parts.append(f"{base} fundamentally BULLISH")
        elif quote_bias == "BEARISH" and base_bias != "BEARISH":
            direction = "CALL"
            reason_parts.append(f"{quote} fundamentally BEARISH")
        # Base bearish OR quote bullish → PUT (price goes down)
        elif base_bias == "BEARISH" and quote_bias != "BEARISH":
            direction = "PUT"
            reason_parts.append(f"{base} fundamentally BEARISH")
        elif quote_bias == "BULLISH" and base_bias != "BULLISH":
            direction = "PUT"
            reason_parts.append(f"{quote} fundamentally BULLISH")

        if direction is None:
            log.debug("[Signals] %s → no clear directional bias.", name)
            continue

        if sentiment != "NEUTRAL":
            reason_parts.append(f"news sentiment: {sentiment.replace('_', ' ')}")

        reason = " | ".join(reason_parts)

        signals.append({
            "instrument": name,
            "direction": direction,
            "expiry": "5 min",
            "reason": reason,
            "generated_at": datetime.now(timezone.utc),
        })
        log.info("[Signals] Generated: %s | %s | %s", name, direction, reason)

    log.info("[Signals] Total signals generated: %d", len(signals))
    return signals


# ---------------------------------------------------------------------------
# Module 4 – Telegram Notification
# ---------------------------------------------------------------------------
SESSION_NAME = "signal_engine"
_tg_client: TelegramClient | None = None
_tg_loop: asyncio.AbstractEventLoop | None = None


def _format_signal_message(signal: dict) -> str:
    """Format a single signal as a plain-text Telegram message."""
    ts = signal["generated_at"].strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "OTC SIGNAL ALERT",
        "=" * 32,
        f"Instrument : {signal['instrument']}",
        f"Direction  : {signal['direction']}",
        f"Type       : CALL" if signal["direction"] == "CALL" else f"Type       : PUT",
        f"Expiry     : {signal['expiry']}",
        f"Reason     : {signal['reason']}",
        f"Generated  : {ts} UTC",
        "=" * 32,
        "MANUAL TRADE ONLY",
        "Open Pocket Option and place this trade yourself.",
        "WARNING: Model-generated signal. Always confirm manually.",
    ]
    return "\n".join(lines)


def send_telegram_alert(signal: dict) -> None:
    """Send a formatted signal alert via Telegram (thread-safe)."""
    if _tg_client is None or _tg_loop is None:
        log.warning("[Telegram] Client not ready. Cannot send alert.")
        return

    message = _format_signal_message(signal)

    async def _send():
        try:
            await _tg_client.send_message(TG_MY_CHAT_ID, message)
            log.info(
                "[Telegram] Alert sent: %s | %s",
                signal["instrument"],
                signal["direction"],
            )
        except Exception as exc:
            log.error("[Telegram] Failed to send alert: %s", exc, exc_info=True)

    future = asyncio.run_coroutine_threadsafe(_send(), _tg_loop)
    try:
        future.result(timeout=30)
    except Exception as exc:
        log.error("[Telegram] run_coroutine_threadsafe error: %s", exc)


# ---------------------------------------------------------------------------
# Module 5 – Scheduler / Main scan loop
# ---------------------------------------------------------------------------
def run_scan_cycle() -> None:
    """
    Execute one full scan cycle:
    1. Fetch macro events
    2. Fetch news sentiment
    3. Generate signals
    4. Send non-cooldown signals via Telegram
    """
    cycle_start = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info("SCAN CYCLE STARTED at %s UTC", cycle_start.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    # Step 1: Macro data
    events = fetch_macro_events()
    macro_bias = analyse_macro_events(events)

    # Step 2: News sentiment
    sentiment = fetch_news_sentiment()

    # Step 3: Generate signals
    signals = generate_signals(macro_bias, sentiment)

    if not signals:
        log.info("SCAN CYCLE COMPLETE – no actionable signals this cycle.")
        return

    # Step 4: Send alerts (respecting cooldown)
    sent_count = 0
    skipped_count = 0
    for signal in signals:
        if _is_on_cooldown(signal["instrument"], signal["direction"]):
            log.info(
                "[Cooldown] Skipping %s | %s",
                signal["instrument"],
                signal["direction"],
            )
            skipped_count += 1
            continue
        send_telegram_alert(signal)
        sent_count += 1

    log.info(
        "SCAN CYCLE COMPLETE | sent=%d | skipped(cooldown)=%d | total=%d",
        sent_count, skipped_count, len(signals),
    )


def _scheduler_loop() -> None:
    """Run scan cycles on a fixed interval forever."""
    log.info("[Scheduler] Starting. First scan in 10 seconds...")
    time.sleep(10)  # brief delay to let Telegram client connect first

    while True:
        try:
            run_scan_cycle()
        except Exception as exc:
            log.error("[Scheduler] Unhandled error in scan cycle: %s", exc, exc_info=True)

        log.info("[Scheduler] Next scan in %d seconds.", SCAN_INTERVAL_SECONDS)
        time.sleep(SCAN_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Telegram client thread
# ---------------------------------------------------------------------------
async def _run_telegram_client() -> None:
    """Authenticate and keep the Telegram client connected."""
    global _tg_client, _tg_loop

    _tg_loop = asyncio.get_running_loop()
    tg = TelegramClient(SESSION_NAME, TG_API_ID, TG_API_HASH)
    _tg_client = tg

    try:
        await tg.start(phone=TG_PHONE)
    except SessionPasswordNeededError:
        log.critical(
            "[Telegram] Two-step verification is enabled. "
            "Set TG_2FA_PASSWORD env var and handle it manually."
        )
        return

    me = await tg.get_me()
    log.info(
        "[Telegram] Authenticated as: %s (id=%s)",
        getattr(me, "username", None) or getattr(me, "first_name", "unknown"),
        me.id,
    )

    # Send a startup notification
    try:
        await tg.send_message(
            TG_MY_CHAT_ID,
            "Fundamental OTC Signal Engine started.\n"
            "Scanning every 15 minutes for high-impact macro events.\n"
            "MANUAL TRADE ONLY – never connected to any broker API.",
        )
    except Exception as exc:
        log.warning("[Telegram] Could not send startup message: %s", exc)

    await tg.run_until_disconnected()


def _start_telegram_thread() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run_telegram_client())
    except Exception as exc:
        log.critical("[Telegram] Client thread crashed: %s", exc, exc_info=True)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Flask web server
# ---------------------------------------------------------------------------
flask_app = Flask(__name__)


@flask_app.route("/health")
def health():
    return "OK", 200


@flask_app.route("/")
def index():
    uptime = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"Fundamental OTC Signal Engine is running.\n"
        f"Server time: {uptime} UTC\n"
        f"Log file: {LOG_FILE}"
    ), 200


@flask_app.route("/status")
def status():
    """Return a brief status summary."""
    with _cooldown_lock:
        active_cooldowns = len(_cooldown_cache)
    tg_connected = _tg_client is not None and _tg_client.is_connected()
    return (
        f"Telegram connected : {tg_connected}\n"
        f"Active cooldowns   : {active_cooldowns}\n"
        f"Scan interval      : {SCAN_INTERVAL_SECONDS}s\n"
        f"Cooldown window    : {COOLDOWN_SECONDS}s\n"
    ), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("Fundamental OTC Signal Engine – starting up")
    log.info("=" * 60)

    # 1. Start Telegram client in background thread
    tg_thread = threading.Thread(
        target=_start_telegram_thread, daemon=True, name="TelegramClient"
    )
    tg_thread.start()

    # 2. Start scheduler in background thread
    scheduler_thread = threading.Thread(
        target=_scheduler_loop, daemon=True, name="Scheduler"
    )
    scheduler_thread.start()

    # 3. Start Flask (blocking – keeps the process alive on Render)
    log.info("Flask starting on port 10000...")
    flask_app.run(host="0.0.0.0", port=10000)
