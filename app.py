"""
Telegram Signal Assistant Bot
Listens to configured Telegram channels for trading signals,
applies safety filters, and forwards clean plain-text alerts
to your personal Saved Messages (or a specified chat).
"""

import asyncio
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone

from flask import Flask
from telethon import TelegramClient, events
from telethon.tl.types import Message

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise EnvironmentError(f"Required environment variable '{name}' is not set.")
    return value


def _optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


API_ID: int = int(_require("TG_API_ID"))
API_HASH: str = _require("TG_API_HASH")
PHONE: str = _require("TG_PHONE")

# Comma-separated channel usernames or numeric IDs, e.g. "signalchan1,-1001234567890"
TG_CHANNELS_RAW: str = _require("TG_CHANNELS")
CHANNELS: list[str | int] = []
for ch in TG_CHANNELS_RAW.split(","):
    ch = ch.strip()
    if not ch:
        continue
    try:
        CHANNELS.append(int(ch))
    except ValueError:
        CHANNELS.append(ch)

# Where to forward signals. Use "me" for Saved Messages or a numeric chat ID.
MY_CHAT_ID_RAW: str = _require("TG_MY_CHAT_ID")
MY_CHAT_ID: str | int = MY_CHAT_ID_RAW if MY_CHAT_ID_RAW.lower() == "me" else int(MY_CHAT_ID_RAW)

# Safety filter configuration
PAYOUT_THRESHOLD: float = float(_optional("PAYOUT_THRESHOLD", "70"))
COOLDOWN_SECONDS: int = int(_optional("COOLDOWN_SECONDS", "60"))
TRADING_HOURS_START: str = _optional("TRADING_HOURS_START", "")   # e.g. "08:00"
TRADING_HOURS_END: str = _optional("TRADING_HOURS_END", "")       # e.g. "22:00"
BLACKLISTED_ASSETS_RAW: str = _optional("BLACKLISTED_ASSETS", "")
BLACKLISTED_ASSETS: set[str] = {
    a.strip().upper()
    for a in BLACKLISTED_ASSETS_RAW.split(",")
    if a.strip()
}

# ---------------------------------------------------------------------------
# Signal parsing
# ---------------------------------------------------------------------------
# Matches patterns like:
#   BUY EUR/USD CALL 5min
#   SELL GBP/JPY PUT 3min
#   BUY BTC/USD CALL 1m
#   EUR/USD CALL 5 min
#   CALL EUR/USD 5min
# Groups: direction_prefix, asset, direction_suffix, timeframe_value, timeframe_unit
_SIGNAL_RE = re.compile(
    r"""
    (?:
        (?P<dir_pre>BUY|SELL)\s+          # optional leading BUY/SELL
    )?
    (?P<asset>[A-Z]{2,6}[/\-][A-Z]{2,6}) # asset pair, e.g. EUR/USD or BTC-USD
    \s+
    (?P<direction>CALL|PUT)               # direction
    \s+
    (?P<tf_value>\d+)\s*(?P<tf_unit>min|m|hour|h|sec|s) # timeframe
    |
    (?P<direction2>CALL|PUT)              # direction-first variant
    \s+
    (?P<asset2>[A-Z]{2,6}[/\-][A-Z]{2,6})
    \s+
    (?P<tf_value2>\d+)\s*(?P<tf_unit2>min|m|hour|h|sec|s)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Optional payout extraction: "85%", "payout 85", "85% payout"
_PAYOUT_RE = re.compile(r"(\d{2,3})\s*%", re.IGNORECASE)


def parse_signal(text: str) -> dict | None:
    """
    Attempt to extract a trading signal from raw message text.
    Returns a dict with keys: asset, direction, timeframe, payout, extra
    or None if no signal is detected.
    """
    # Normalise whitespace and uppercase for matching
    normalised = " ".join(text.upper().split())

    match = _SIGNAL_RE.search(normalised)
    if not match:
        return None

    g = match.groupdict()

    # Resolve asset and direction from whichever variant matched
    if g.get("asset"):
        asset = g["asset"].upper().replace("-", "/")
        direction = g["direction"].upper()
        tf_value = g["tf_value"]
        tf_unit = g["tf_unit"].lower()
    else:
        asset = g["asset2"].upper().replace("-", "/")
        direction = g["direction2"].upper()
        tf_value = g["tf_value2"]
        tf_unit = g["tf_unit2"].lower()

    # Normalise timeframe unit label
    unit_map = {"m": "min", "min": "min", "h": "hour", "hour": "hour", "s": "sec", "sec": "sec"}
    tf_unit_label = unit_map.get(tf_unit, tf_unit)
    timeframe = f"{tf_value}{tf_unit_label}"

    # Extract payout if present
    payout_matches = _PAYOUT_RE.findall(normalised)
    payout: float | None = None
    if payout_matches:
        # Take the first percentage that looks like a payout (50–100)
        for p in payout_matches:
            val = float(p)
            if 50 <= val <= 100:
                payout = val
                break

    # Capture any extra context: strip the matched portion and clean up
    extra_text = normalised[: match.start()].strip() + " " + normalised[match.end() :].strip()
    extra_text = extra_text.strip()
    # Remove the payout string from extra so it's not duplicated
    extra_text = _PAYOUT_RE.sub("", extra_text).strip(" %-")
    extra_text = re.sub(r"\s{2,}", " ", extra_text).strip()

    return {
        "asset": asset,
        "direction": direction,
        "timeframe": timeframe,
        "payout": payout,
        "extra": extra_text if extra_text else None,
    }


# ---------------------------------------------------------------------------
# Safety filters
# ---------------------------------------------------------------------------
# Cooldown tracker: maps signal key -> last seen timestamp
_cooldown_cache: dict[str, float] = {}
_cooldown_lock = threading.Lock()


def _signal_key(signal: dict) -> str:
    return f"{signal['asset']}:{signal['direction']}:{signal['timeframe']}"


def check_filters(signal: dict) -> tuple[bool, str]:
    """
    Run all safety filters against a parsed signal.
    Returns (passed: bool, reason: str).
    """
    asset = signal["asset"]
    payout = signal["payout"]

    # 1. Asset blacklist
    if asset in BLACKLISTED_ASSETS:
        return False, f"asset '{asset}' is blacklisted"

    # 2. Payout threshold (only checked when payout is present in the signal)
    if payout is not None and payout < PAYOUT_THRESHOLD:
        return False, f"payout {payout}% is below threshold {PAYOUT_THRESHOLD}%"

    # 3. Trading hours (UTC)
    if TRADING_HOURS_START and TRADING_HOURS_END:
        now_utc = datetime.now(timezone.utc)
        try:
            start_h, start_m = map(int, TRADING_HOURS_START.split(":"))
            end_h, end_m = map(int, TRADING_HOURS_END.split(":"))
            start_minutes = start_h * 60 + start_m
            end_minutes = end_h * 60 + end_m
            now_minutes = now_utc.hour * 60 + now_utc.minute
            if start_minutes <= end_minutes:
                in_window = start_minutes <= now_minutes < end_minutes
            else:
                # Overnight window, e.g. 22:00 – 06:00
                in_window = now_minutes >= start_minutes or now_minutes < end_minutes
            if not in_window:
                return False, (
                    f"outside trading hours "
                    f"({TRADING_HOURS_START}–{TRADING_HOURS_END} UTC)"
                )
        except ValueError:
            log.warning("Invalid TRADING_HOURS format; skipping hours filter.")

    # 4. Cooldown
    key = _signal_key(signal)
    now_ts = time.monotonic()
    with _cooldown_lock:
        last_seen = _cooldown_cache.get(key)
        if last_seen is not None and (now_ts - last_seen) < COOLDOWN_SECONDS:
            elapsed = int(now_ts - last_seen)
            return False, f"duplicate signal (cooldown: {elapsed}s / {COOLDOWN_SECONDS}s)"
        _cooldown_cache[key] = now_ts

    return True, "passed"


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------
def format_alert(signal: dict, source_chat: str, received_at: datetime) -> str:
    """
    Build a plain-text alert message. No markdown links, no buttons.
    """
    lines = [
        "TRADING SIGNAL RECEIVED",
        "─" * 30,
        f"Asset     : {signal['asset']}",
        f"Direction : {signal['direction']}",
        f"Timeframe : {signal['timeframe']}",
    ]
    if signal["payout"] is not None:
        lines.append(f"Payout    : {signal['payout']:.0f}%")
    lines.append(f"Received  : {received_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    lines.append(f"Source    : {source_chat}")
    if signal["extra"]:
        lines.append(f"Note      : {signal['extra']}")
    lines.append("─" * 30)
    lines.append("MANUAL TRADE – open Pocket Option yourself.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Telegram client & event handler
# ---------------------------------------------------------------------------
# Use a persistent session file so you only need to authenticate once.
SESSION_NAME = "signal_assistant"

# Client is created inside the background thread where the event loop lives.
# This module-level variable is set before the handler is registered.
client: TelegramClient | None = None


# Populated after client connects
_resolved_entities: list = []


async def _resolve_channels(tg: TelegramClient) -> list:
    """Resolve channel identifiers to Telethon entity objects."""
    entities = []
    for ch in CHANNELS:
        try:
            entity = await tg.get_entity(ch)
            entities.append(entity)
            log.info("Monitoring channel: %s (id=%s)", getattr(entity, "title", ch), entity.id)
        except Exception as exc:
            log.error("Could not resolve channel '%s': %s", ch, exc)
    return entities


async def _run_client() -> None:
    """
    Create the TelegramClient, register the event handler, authenticate,
    and run until disconnected.  Everything happens inside this coroutine
    so the client is always created in the thread that owns the event loop.
    """
    global client, _resolved_entities

    # Create the client here, inside the running loop
    tg = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    client = tg

    @tg.on(events.NewMessage())
    async def _on_new_message(event: events.NewMessage.Event) -> None:
        """Handle every incoming message and decide whether to forward it."""
        msg: Message = event.message
        text = msg.raw_text or ""
        if not text:
            return

        # Only process messages from monitored channels
        try:
            chat = await event.get_chat()
            chat_id = chat.id
        except Exception:
            return

        monitored_ids = {getattr(e, "id", None) for e in _resolved_entities}
        if chat_id not in monitored_ids:
            return

        source_name = getattr(chat, "title", str(chat_id))
        received_at = datetime.now(timezone.utc)

        log.info(
            "[%s] New message from '%s': %.120s",
            received_at.strftime("%H:%M:%S"),
            source_name,
            text,
        )

        signal = parse_signal(text)
        if signal is None:
            log.info("  -> No signal pattern detected. Skipping.")
            return

        log.info(
            "  -> Signal detected: %s %s %s (payout=%s)",
            signal["asset"],
            signal["direction"],
            signal["timeframe"],
            signal["payout"],
        )

        passed, reason = check_filters(signal)
        if not passed:
            log.info("  -> FILTERED OUT: %s", reason)
            return

        log.info("  -> PASSED all filters. Forwarding alert.")
        alert_text = format_alert(signal, source_name, received_at)

        try:
            await tg.send_message(MY_CHAT_ID, alert_text)
            log.info("  -> Alert sent to chat '%s'.", MY_CHAT_ID)
        except Exception as exc:
            log.error("  -> Failed to send alert: %s", exc)

    await tg.start(phone=PHONE)
    log.info("Telethon client authenticated successfully.")
    _resolved_entities = await _resolve_channels(tg)
    if not _resolved_entities:
        log.warning("No channels resolved. The bot will not receive any signals.")
    log.info("Listening for signals...")
    await tg.run_until_disconnected()


def _start_telegram_thread() -> None:
    """Run the asyncio event loop for Telethon in a dedicated daemon thread."""
    # Create a brand-new event loop for this thread.
    # Python 3.10+ no longer creates a default loop automatically.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run_client())
    except Exception as exc:
        log.critical("Telegram client crashed: %s", exc, exc_info=True)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Flask web server
# ---------------------------------------------------------------------------
flask_app = Flask(__name__)


@flask_app.route("/health")
def health() -> tuple[str, int]:
    return "OK", 200


@flask_app.route("/")
def index() -> tuple[str, int]:
    return "Telegram Signal Assistant is running.", 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Starting Telegram Signal Assistant…")
    log.info(
        "Config: payout_threshold=%.0f%% | cooldown=%ds | blacklist=%s | hours=%s–%s",
        PAYOUT_THRESHOLD,
        COOLDOWN_SECONDS,
        BLACKLISTED_ASSETS or "none",
        TRADING_HOURS_START or "any",
        TRADING_HOURS_END or "any",
    )

    # Start Telegram listener in background thread
    tg_thread = threading.Thread(target=_start_telegram_thread, daemon=True, name="TelegramClient")
    tg_thread.start()

    # Start Flask (blocking)
    flask_app.run(host="0.0.0.0", port=10000)
