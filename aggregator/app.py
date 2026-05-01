"""
OTC Signal Aggregator Bot
=========================
Aggregates fundamental, technical, news, and social signals for
Pocket Option OTC assets and delivers manual trade alerts via Telegram.

SAFETY: This bot NEVER connects to Pocket Option's trading API.
        All signals are informational only. You trade manually.
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import asyncio
import collections
import json
import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Optional

import ecocal
import requests
import websocket
from dotenv import load_dotenv
from flask import Flask
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FILE = "aggregator.log"
_fmt = logging.Formatter(
    "%(asctime)s UTC | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.Formatter.converter = time.gmtime

_fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
_fh.setFormatter(_fmt)
_ch = logging.StreamHandler()
_ch.setFormatter(_fmt)

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(_fh)
logging.getLogger().addHandler(_ch)

log = logging.getLogger("aggregator")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _req(k: str) -> str:
    v = os.environ.get(k, "").strip()
    if not v:
        raise EnvironmentError(f"Required env var '{k}' is not set.")
    return v

def _opt(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

NEWSAPI_KEY      = _req("NEWSAPI_KEY")
BOT_TOKEN        = _req("TELEGRAM_BOT_TOKEN")
# Pocket Option ci_session cookie (URL-encoded value from browser DevTools)
POCKET_CI_SESSION = _opt("POCKET_CI_SESSION", "")
SCAN_INTERVAL    = int(_opt("SCAN_INTERVAL_SECONDS", "900"))   # 15 min
SIGNAL_COOLDOWN  = int(_opt("SIGNAL_COOLDOWN_SECONDS", "60"))  # 1 min dedup

log.info("Config | scan=%ds | cooldown=%ds", SCAN_INTERVAL, SIGNAL_COOLDOWN)

# ---------------------------------------------------------------------------
# OTC Instruments
# ---------------------------------------------------------------------------
OTC_PAIRS = [
    {"name": "EUR/USD OTC", "base": "EUR", "quote": "USD", "ws_id": "eurusd_otc"},
    {"name": "GBP/USD OTC", "base": "GBP", "quote": "USD", "ws_id": "gbpusd_otc"},
    {"name": "USD/JPY OTC", "base": "USD", "quote": "JPY", "ws_id": "usdjpy_otc"},
    {"name": "AUD/USD OTC", "base": "AUD", "quote": "USD", "ws_id": "audusd_otc"},
    {"name": "USD/CHF OTC", "base": "USD", "quote": "CHF", "ws_id": "usdchf_otc"},
    {"name": "USD/CAD OTC", "base": "USD", "quote": "CAD", "ws_id": "usdcad_otc"},
    {"name": "NZD/USD OTC", "base": "NZD", "quote": "USD", "ws_id": "nzdusd_otc"},
    {"name": "EUR/GBP OTC", "base": "EUR", "quote": "GBP", "ws_id": "eurgbp_otc"},
    {"name": "EUR/JPY OTC", "base": "EUR", "quote": "JPY", "ws_id": "eurjpy_otc"},
    {"name": "GBP/JPY OTC", "base": "GBP", "quote": "JPY", "ws_id": "gbpjpy_otc"},
]

COUNTRY_TO_CCY = {
    "us": "USD", "gb": "GBP", "eu": "EUR",
    "jp": "JPY", "au": "AUD", "ca": "CAD",
    "nz": "NZD", "ch": "CHF",
}

SAFE_HAVENS  = {"USD", "JPY", "CHF"}
RISK_CCYS    = {"AUD", "NZD", "CAD", "GBP", "EUR"}

MACRO_KEYWORDS = [
    "employment", "nonfarm", "payroll", "gdp", "retail sales",
    "manufacturing", "pmi", "cpi", "interest rate", "rate decision",
    "trade balance", "consumer confidence", "industrial production",
]

# ---------------------------------------------------------------------------
# Shared state (thread-safe via locks)
# ---------------------------------------------------------------------------
_state_lock = threading.Lock()

# Last fetch timestamps
_last_fetch: dict[str, Optional[datetime]] = {
    "fundamentals": None,
    "news":         None,
    "social":       None,
    "technical":    None,
}

# Latest results from each module
_macro_bias:    dict[str, str]  = {}   # {"USD": "BULLISH", ...}
_news_sentiment: str            = "NEUTRAL"
_social_mentions: dict[str, int] = {}  # {"EUR": 3, "USD": 7, ...}
_technical_signals: dict[str, Optional[str]] = {}  # {"EUR/USD OTC": "CALL", ...}

# Candle buffers: ws_id -> deque of close prices (last 30 1-min closes)
_candle_closes: dict[str, collections.deque] = {
    p["ws_id"]: collections.deque(maxlen=30) for p in OTC_PAIRS
}
_candle_open_time: dict[str, Optional[float]] = {p["ws_id"]: None for p in OTC_PAIRS}
_candle_current:   dict[str, list]            = {p["ws_id"]: [] for p in OTC_PAIRS}

# Signal history
_signal_history: list[dict] = []
_signal_cooldown_cache: dict[str, float] = {}  # "ASSET:DIR" -> monotonic ts
_signals_today: int = 0
_bot_start_time: datetime = datetime.now(timezone.utc)

# Subscribers (chat_ids that want push notifications)
_subscribers: set[int] = set()

# Reference to the running PTB Application (set in main)
_ptb_app: Optional[Application] = None

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _ts(dt: Optional[datetime]) -> str:
    if dt is None:
        return "never"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

def _on_cooldown(asset: str, direction: str) -> bool:
    key = f"{asset}:{direction}"
    now = time.monotonic()
    with _state_lock:
        last = _signal_cooldown_cache.get(key)
        if last and (now - last) < SIGNAL_COOLDOWN:
            return True
        _signal_cooldown_cache[key] = now
    return False

# ---------------------------------------------------------------------------
# Module 1 – Fundamentals (ecocal – Forex Factory economic calendar)
# ---------------------------------------------------------------------------
# ecocal scrapes the Forex Factory calendar and returns structured events
# with 'actual', 'consensus', and 'previous' values – no API key needed.

ECOCAL_IMPACT_HIGH = 3   # ecocal uses integer 1/2/3 for low/medium/high

def fetch_fundamentals() -> dict[str, str]:
    """
    Fetch today's high-impact macro events via ecocal and return a
    currency bias map: {"USD": "BULLISH", "EUR": "BEARISH", ...}
    """
    log.info("[Fundamentals] Fetching economic calendar via ecocal...")
    try:
        today = datetime.now(timezone.utc).date()
        cal = ecocal.Calendar(date=str(today))
        events = cal.events          # list of ecocal.Event objects
        log.info("[Fundamentals] Total events today: %d", len(events))

        bias: dict[str, str] = {}

        for ev in events:
            # Filter: high impact only
            impact = getattr(ev, "impact", None)
            # ecocal may return impact as int or string "High"/"Medium"/"Low"
            if isinstance(impact, int):
                if impact < ECOCAL_IMPACT_HIGH:
                    continue
            elif isinstance(impact, str):
                if impact.lower() not in ("high", "3"):
                    continue
            else:
                continue

            currency = str(getattr(ev, "currency", "") or "").upper().strip()
            if not currency or currency not in COUNTRY_TO_CCY.values():
                continue

            event_name = str(getattr(ev, "name", "") or "").lower()
            if not any(kw in event_name for kw in MACRO_KEYWORDS):
                log.debug("[Fundamentals] Skipping '%s' – not in keyword list.", event_name[:60])
                continue

            # Parse actual and consensus values
            def _parse_val(raw) -> Optional[float]:
                if raw is None:
                    return None
                s = str(raw).replace("%", "").replace("K", "000") \
                            .replace("M", "000000").replace(",", "").strip()
                if not s or s in ("-", "N/A", ""):
                    return None
                try:
                    return float(s)
                except ValueError:
                    return None

            actual    = _parse_val(getattr(ev, "actual",    None))
            consensus = _parse_val(getattr(ev, "consensus", None)
                                   or getattr(ev, "forecast", None))

            if actual is None or consensus is None:
                log.debug("[Fundamentals] Skipping '%s' – missing actual/consensus.", event_name[:60])
                continue

            new_bias = (
                "BULLISH" if actual > consensus else
                "BEARISH" if actual < consensus else
                None
            )
            if new_bias is None:
                continue

            existing = bias.get(currency)
            if existing and existing != new_bias:
                bias[currency] = "NEUTRAL"
                log.info("[Fundamentals] %s conflicting signals → NEUTRAL", currency)
            else:
                bias[currency] = new_bias
                log.info(
                    "[Fundamentals] %s → %s | event='%s' actual=%s consensus=%s",
                    currency, new_bias, event_name[:60], actual, consensus,
                )

        with _state_lock:
            _macro_bias.clear()
            _macro_bias.update(bias)
            _last_fetch["fundamentals"] = _now_utc()

        log.info("[Fundamentals] Bias summary: %s", bias or "none")
        return bias

    except Exception as exc:
        log.error("[Fundamentals] Error: %s", exc, exc_info=True)
        return {}

# ---------------------------------------------------------------------------
# Module 2 – News Sentiment (NewsAPI)
# ---------------------------------------------------------------------------
NEWS_URL = "https://newsapi.org/v2/top-headlines"
RISK_OFF_KW = ["recession","crash","sell-off","selloff","crisis","collapse",
               "default","downturn","layoffs","bankruptcy","rate hike","war","sanctions"]
RISK_ON_KW  = ["growth","rally","optimism","recovery","expansion","surge",
               "record high","beat expectations","strong jobs","rate cut",
               "stimulus","boom","bullish","upgrade"]

def fetch_news() -> str:
    """Return "RISK_OFF", "RISK_ON", or "NEUTRAL"."""
    log.info("[News] Fetching headlines...")
    try:
        r = requests.get(
            NEWS_URL,
            params={"apiKey": NEWSAPI_KEY, "category": "business",
                    "language": "en", "pageSize": 40},
            timeout=15,
        )
        r.raise_for_status()
        articles = r.json().get("articles", [])
        log.info("[News] Articles: %d", len(articles))

        off_score = on_score = 0
        for a in articles:
            text = f"{a.get('title','')} {a.get('description','')}".lower()
            off_score += sum(1 for kw in RISK_OFF_KW if kw in text)
            on_score  += sum(1 for kw in RISK_ON_KW  if kw in text)

        log.info("[News] RISK_OFF=%d RISK_ON=%d", off_score, on_score)
        if off_score > on_score and off_score >= 2:
            sentiment = "RISK_OFF"
        elif on_score > off_score and on_score >= 2:
            sentiment = "RISK_ON"
        else:
            sentiment = "NEUTRAL"

        with _state_lock:
            global _news_sentiment
            _news_sentiment = sentiment
            _last_fetch["news"] = _now_utc()

        log.info("[News] Sentiment: %s", sentiment)
        return sentiment

    except Exception as exc:
        log.error("[News] Error: %s", exc, exc_info=True)
        return "NEUTRAL"

# ---------------------------------------------------------------------------
# Module 3 – Social (Reddit r/Forex)
# ---------------------------------------------------------------------------
REDDIT_URL = "https://www.reddit.com/r/Forex/top.json"
SOCIAL_CCYS = ["EUR", "USD", "GBP", "JPY", "AUD", "CAD", "NZD", "CHF"]

def fetch_social() -> dict[str, int]:
    """
    Scrape r/Forex top posts (last hour) and count currency mentions.
    Returns {"EUR": 4, "USD": 9, ...}
    """
    log.info("[Social] Fetching r/Forex top posts...")
    try:
        r = requests.get(
            REDDIT_URL,
            params={"t": "hour", "limit": 25},
            headers={"User-Agent": "OTCSignalBot/1.0"},
            timeout=15,
        )
        r.raise_for_status()
        posts = r.json().get("data", {}).get("children", [])
        log.info("[Social] Posts fetched: %d", len(posts))

        counts: dict[str, int] = {c: 0 for c in SOCIAL_CCYS}
        for post in posts:
            d = post.get("data", {})
            text = f"{d.get('title','')} {d.get('selftext','')}".upper()
            for ccy in SOCIAL_CCYS:
                counts[ccy] += text.count(ccy)

        # Filter to only currencies with at least 1 mention
        mentions = {k: v for k, v in counts.items() if v > 0}

        with _state_lock:
            _social_mentions.clear()
            _social_mentions.update(mentions)
            _last_fetch["social"] = _now_utc()

        log.info("[Social] Mentions: %s", mentions or "none")
        return mentions

    except Exception as exc:
        log.error("[Social] Error: %s", exc, exc_info=True)
        return {}

# ---------------------------------------------------------------------------
# Module 4 – Technical (Pocket Option WebSocket price stream)
# ---------------------------------------------------------------------------
PO_WS_URL = "wss://api.po.market/socket.io/?EIO=4&transport=websocket"

# RSI calculation
def _calc_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# EMA calculation
def _calc_ema(closes: list[float], period: int) -> Optional[float]:
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def _evaluate_technical(ws_id: str) -> Optional[str]:
    """
    Given accumulated 1-min closes for a pair, return "CALL", "PUT", or None.
    Signal fires when RSI is oversold/overbought AND EMA5/EMA20 confirms.
    """
    with _state_lock:
        closes = list(_candle_closes[ws_id])

    if len(closes) < 21:
        log.debug("[Technical] %s: not enough candles (%d)", ws_id, len(closes))
        return None

    rsi  = _calc_rsi(closes)
    ema5 = _calc_ema(closes, 5)
    ema20 = _calc_ema(closes, 20)

    if rsi is None or ema5 is None or ema20 is None:
        return None

    log.debug("[Technical] %s RSI=%.1f EMA5=%.5f EMA20=%.5f", ws_id, rsi, ema5, ema20)

    if rsi < 30 and ema5 > ema20:   # oversold + bullish crossover
        return "CALL"
    if rsi > 70 and ema5 < ema20:   # overbought + bearish crossover
        return "PUT"
    return None


def _on_ws_message(ws_obj, message: str) -> None:
    """
    Parse Pocket Option WebSocket price ticks and build 1-min candles.
    PO sends Socket.IO frames; price ticks look like:
    42["price",{"asset":"eurusd_otc","time":1234567890,"price":1.08432}]
    """
    try:
        # Socket.IO handshake frames
        if message.startswith("0") or message.startswith("40"):
            # Authenticate with ci_session cookie if provided
            if POCKET_CI_SESSION:
                ws_obj.send(f'42["auth",{{"ci_session":"{POCKET_CI_SESSION}"}}]')
            # Subscribe to all OTC pairs
            for pair in OTC_PAIRS:
                ws_obj.send(f'42["subscribe",{{"asset":"{pair["ws_id"]}"}}]')
            return

        if not message.startswith("42"):
            return

        payload = json.loads(message[2:])
        if not isinstance(payload, list) or len(payload) < 2:
            return
        event_name = payload[0]
        data = payload[1]

        if event_name not in ("price", "tick", "candle", "quote"):
            return

        ws_id = data.get("asset") or data.get("symbol") or data.get("pair")
        price = float(data.get("price") or data.get("close") or data.get("value") or 0)
        tick_time = float(data.get("time") or time.time())

        if not ws_id or price == 0:
            return

        with _state_lock:
            if ws_id not in _candle_closes:
                return

            open_t = _candle_open_time[ws_id]
            current = _candle_current[ws_id]

            if open_t is None:
                _candle_open_time[ws_id] = tick_time
                _candle_current[ws_id] = [price]
            elif tick_time - open_t >= 60:
                # Close the candle
                close_price = current[-1] if current else price
                _candle_closes[ws_id].append(close_price)
                _candle_open_time[ws_id] = tick_time
                _candle_current[ws_id] = [price]
                log.debug("[Technical] %s new candle close=%.5f (total=%d)",
                          ws_id, close_price, len(_candle_closes[ws_id]))
            else:
                _candle_current[ws_id].append(price)

    except Exception as exc:
        log.debug("[Technical] WS parse error: %s", exc)


def _on_ws_error(ws_obj, error) -> None:
    log.warning("[Technical] WebSocket error: %s", error)

def _on_ws_close(ws_obj, code, msg) -> None:
    log.info("[Technical] WebSocket closed (code=%s). Will reconnect.", code)

def _on_ws_open(ws_obj) -> None:
    log.info("[Technical] WebSocket connected.")
    if POCKET_CI_SESSION:
        ws_obj.send(f'42["auth",{{"ci_session":"{POCKET_CI_SESSION}"}}]')
    for pair in OTC_PAIRS:
        ws_obj.send(f'42["subscribe",{{"asset":"{pair["ws_id"]}"}}]')


def _run_websocket_forever() -> None:
    """Keep the WebSocket alive with auto-reconnect."""
    while True:
        try:
            log.info("[Technical] Connecting to Pocket Option WebSocket...")
            ws = websocket.WebSocketApp(
                PO_WS_URL,
                on_open=_on_ws_open,
                on_message=_on_ws_message,
                on_error=_on_ws_error,
                on_close=_on_ws_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as exc:
            log.error("[Technical] WebSocket thread error: %s", exc)
        log.info("[Technical] Reconnecting in 15s...")
        time.sleep(15)


def run_technical_analysis() -> dict[str, Optional[str]]:
    """Evaluate technical signals for all pairs and update shared state."""
    results: dict[str, Optional[str]] = {}
    for pair in OTC_PAIRS:
        sig = _evaluate_technical(pair["ws_id"])
        results[pair["name"]] = sig
        if sig:
            log.info("[Technical] %s → %s", pair["name"], sig)

    with _state_lock:
        _technical_signals.clear()
        _technical_signals.update(results)
        _last_fetch["technical"] = _now_utc()

    return results

# ---------------------------------------------------------------------------
# Module 5 – Signal Combiner
# ---------------------------------------------------------------------------
def combine_signals() -> list[dict]:
    """
    Combine all four data sources and emit signals where at least
    two sources agree on direction for the same OTC pair.
    Returns a list of validated signal dicts.
    """
    with _state_lock:
        macro   = dict(_macro_bias)
        news    = _news_sentiment
        social  = dict(_social_mentions)
        tech    = dict(_technical_signals)

    signals: list[dict] = []

    # Build per-currency directional votes from each source
    # vote structure: {currency: {"CALL": [source,...], "PUT": [source,...]}}
    def _add_vote(votes, ccy, direction, source):
        votes.setdefault(ccy, {"CALL": [], "PUT": []})
        votes[ccy][direction].append(source)

    currency_votes: dict[str, dict] = {}

    # --- Fundamentals votes ---
    for ccy, bias in macro.items():
        if bias == "BULLISH":
            _add_vote(currency_votes, ccy, "CALL", "fundamentals")
        elif bias == "BEARISH":
            _add_vote(currency_votes, ccy, "PUT", "fundamentals")

    # --- News sentiment votes ---
    if news == "RISK_OFF":
        for ccy in SAFE_HAVENS:
            _add_vote(currency_votes, ccy, "CALL", "news")
        for ccy in RISK_CCYS:
            _add_vote(currency_votes, ccy, "PUT", "news")
    elif news == "RISK_ON":
        for ccy in RISK_CCYS:
            _add_vote(currency_votes, ccy, "CALL", "news")
        for ccy in SAFE_HAVENS:
            _add_vote(currency_votes, ccy, "PUT", "news")

    # --- Social votes (high mention = crowd attention = momentum signal) ---
    if social:
        max_mentions = max(social.values(), default=0)
        for ccy, count in social.items():
            if count >= max(3, max_mentions * 0.5):
                # High social attention: align with macro if available, else neutral
                macro_dir = macro.get(ccy)
                if macro_dir == "BULLISH":
                    _add_vote(currency_votes, ccy, "CALL", "social")
                elif macro_dir == "BEARISH":
                    _add_vote(currency_votes, ccy, "PUT", "social")
                # If no macro signal, social alone doesn't vote

    log.debug("[Combiner] Currency votes: %s", currency_votes)

    # --- Map currency votes to OTC pairs ---
    for pair in OTC_PAIRS:
        base  = pair["base"]
        quote = pair["quote"]
        name  = pair["name"]

        # Collect all sources that agree on a direction for this pair
        call_sources: list[str] = []
        put_sources:  list[str] = []

        # Base currency CALL → pair CALL; Base currency PUT → pair PUT
        base_votes = currency_votes.get(base, {"CALL": [], "PUT": []})
        call_sources.extend(base_votes["CALL"])
        put_sources.extend(base_votes["PUT"])

        # Quote currency CALL → pair PUT; Quote currency PUT → pair CALL
        quote_votes = currency_votes.get(quote, {"CALL": [], "PUT": []})
        call_sources.extend(quote_votes["PUT"])   # quote bearish = pair goes up
        put_sources.extend(quote_votes["CALL"])   # quote bullish = pair goes down

        # Technical signal for this pair
        tech_sig = tech.get(name)
        if tech_sig == "CALL":
            call_sources.append("technical")
        elif tech_sig == "PUT":
            put_sources.append("technical")

        # Deduplicate sources
        call_sources = list(dict.fromkeys(call_sources))
        put_sources  = list(dict.fromkeys(put_sources))

        log.debug("[Combiner] %s | CALL_sources=%s | PUT_sources=%s",
                  name, call_sources, put_sources)

        # Fire signal only if at least 2 sources agree
        direction: Optional[str] = None
        sources:   list[str]     = []

        if len(call_sources) >= 2 and len(call_sources) > len(put_sources):
            direction = "CALL"
            sources   = call_sources
        elif len(put_sources) >= 2 and len(put_sources) > len(call_sources):
            direction = "PUT"
            sources   = put_sources

        if direction is None:
            continue

        # Build human-readable reason
        reason_map = {
            "fundamentals": _build_fundamental_reason(base, quote, macro),
            "news":         f"news sentiment: {news.replace('_', ' ')}",
            "social":       f"high social chatter on {_social_top(social, base, quote)}",
            "technical":    _build_technical_reason(name, tech),
        }
        reason_parts = [reason_map[s] for s in sources if s in reason_map]
        reason = " + ".join(reason_parts)

        confidence = f"HIGH ({len(sources)} sources)" if len(sources) >= 3 else f"MEDIUM ({len(sources)} sources)"

        signal = {
            "asset":      name,
            "direction":  direction,
            "expiry":     "3 minutes",
            "confidence": confidence,
            "sources":    sources,
            "reason":     reason,
            "generated_at": _now_utc(),
        }
        signals.append(signal)
        log.info("[Combiner] Signal: %s | %s | %s | %s", name, direction, confidence, reason)

    return signals


def _build_fundamental_reason(base: str, quote: str, macro: dict) -> str:
    parts = []
    if macro.get(base) == "BULLISH":
        parts.append(f"{base} fundamentally strong")
    elif macro.get(base) == "BEARISH":
        parts.append(f"{base} fundamentally weak")
    if macro.get(quote) == "BULLISH":
        parts.append(f"{quote} fundamentally strong")
    elif macro.get(quote) == "BEARISH":
        parts.append(f"{quote} fundamentally weak")
    return " / ".join(parts) if parts else "macro event"


def _build_technical_reason(pair_name: str, tech: dict) -> str:
    sig = tech.get(pair_name)
    if sig == "CALL":
        return "RSI oversold + EMA5 crossed above EMA20"
    if sig == "PUT":
        return "RSI overbought + EMA5 crossed below EMA20"
    return "technical indicator"


def _social_top(social: dict, base: str, quote: str) -> str:
    relevant = {k: v for k, v in social.items() if k in (base, quote)}
    if not relevant:
        return "pair"
    top = max(relevant, key=relevant.get)
    return top


# ---------------------------------------------------------------------------
# Signal formatting
# ---------------------------------------------------------------------------
def format_signal(sig: dict) -> str:
    ts = sig["generated_at"].strftime("%Y-%m-%d %H:%M:%S UTC")
    sources_str = " + ".join(sig["sources"])
    return (
        "OTC SIGNAL\n"
        f"Asset      : {sig['asset']}\n"
        f"Direction  : {sig['direction']}\n"
        f"Expiry     : {sig['expiry']}\n"
        f"Confidence : {sig['confidence']}\n"
        f"Sources    : {sources_str}\n"
        f"Reason     : {sig['reason']}\n"
        f"Time       : {ts}\n"
        "─────────────────────────────\n"
        "MANUAL TRADE ONLY\n"
        "Open Pocket Option yourself."
    )

# ---------------------------------------------------------------------------
# Scan cycle (runs every SCAN_INTERVAL seconds)
# ---------------------------------------------------------------------------
def run_scan_cycle() -> None:
    global _signals_today
    cycle_start = _now_utc()
    log.info("=" * 60)
    log.info("SCAN CYCLE | %s", cycle_start.strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("=" * 60)

    fetch_fundamentals()
    fetch_news()
    fetch_social()
    run_technical_analysis()

    signals = combine_signals()

    if not signals:
        log.info("SCAN COMPLETE – no actionable signals.")
        return

    for sig in signals:
        if _on_cooldown(sig["asset"], sig["direction"]):
            log.info("[Cooldown] Skipping %s %s", sig["asset"], sig["direction"])
            continue

        with _state_lock:
            _signal_history.append(sig)
            if len(_signal_history) > 200:
                _signal_history.pop(0)
            _signals_today += 1

        log.info("[Signal] FIRED: %s | %s | %s", sig["asset"], sig["direction"], sig["confidence"])

        # Push to subscribers
        _push_signal(sig)

    log.info("SCAN COMPLETE | total_today=%d", _signals_today)


def _push_signal(sig: dict) -> None:
    """Send signal to all subscribed Telegram users."""
    if _ptb_app is None:
        return
    with _state_lock:
        subs = set(_subscribers)
    if not subs:
        return

    text = format_signal(sig)
    for chat_id in subs:
        asyncio.run_coroutine_threadsafe(
            _ptb_app.bot.send_message(chat_id=chat_id, text=text),
            _ptb_app.bot._loop if hasattr(_ptb_app.bot, "_loop") else asyncio.get_event_loop(),
        )


def _scheduler_loop() -> None:
    log.info("[Scheduler] Starting. First scan in 15s...")
    time.sleep(15)
    while True:
        try:
            run_scan_cycle()
        except Exception as exc:
            log.error("[Scheduler] Unhandled error: %s", exc, exc_info=True)
        log.info("[Scheduler] Next scan in %ds.", SCAN_INTERVAL)
        time.sleep(SCAN_INTERVAL)

# ---------------------------------------------------------------------------
# Telegram Bot Commands
# ---------------------------------------------------------------------------
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    with _state_lock:
        f_time = _ts(_last_fetch["fundamentals"])
        n_time = _ts(_last_fetch["news"])
        s_time = _ts(_last_fetch["social"])
        t_time = _ts(_last_fetch["technical"])
        macro  = dict(_macro_bias)
        news   = _news_sentiment
        subs   = len(_subscribers)

    text = (
        "OTC Signal Aggregator Bot\n"
        "══════════════════════════\n"
        "Aggregates fundamentals, news, social, and technical\n"
        "signals for Pocket Option OTC assets.\n\n"
        "DATA STATUS\n"
        f"  Fundamentals : {f_time}\n"
        f"  News         : {n_time}\n"
        f"  Social       : {s_time}\n"
        f"  Technical    : {t_time}\n\n"
        f"MACRO BIAS   : {macro or 'pending first scan'}\n"
        f"NEWS MOOD    : {news}\n"
        f"SUBSCRIBERS  : {subs}\n\n"
        "COMMANDS\n"
        "  /latest      – most recent signal\n"
        "  /subscribe   – enable push alerts\n"
        "  /unsubscribe – disable push alerts\n"
        "  /data        – last fetch times\n"
        "  /status      – uptime & signal count\n\n"
        "MANUAL TRADE ONLY. Never connected to any broker API."
    )
    await update.message.reply_text(text)


async def cmd_latest(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    with _state_lock:
        history = list(_signal_history)

    if not history:
        await update.message.reply_text(
            "No signals generated yet.\n"
            "Scans run every 15 minutes. Try again shortly."
        )
        return

    latest = history[-1]
    await update.message.reply_text(format_signal(latest))


async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    with _state_lock:
        already = chat_id in _subscribers
        _subscribers.add(chat_id)
    if already:
        await update.message.reply_text("You are already subscribed to push alerts.")
    else:
        await update.message.reply_text(
            "Subscribed! You will receive signals as soon as they are generated.\n"
            "Use /unsubscribe to stop."
        )
    log.info("[Bot] Chat %d subscribed.", chat_id)


async def cmd_unsubscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    with _state_lock:
        was_subbed = chat_id in _subscribers
        _subscribers.discard(chat_id)
    if was_subbed:
        await update.message.reply_text("Unsubscribed. You will no longer receive push alerts.")
    else:
        await update.message.reply_text("You were not subscribed.")
    log.info("[Bot] Chat %d unsubscribed.", chat_id)


async def cmd_data(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    with _state_lock:
        fetches = dict(_last_fetch)
        macro   = dict(_macro_bias)
        news    = _news_sentiment
        social  = dict(_social_mentions)

    text = (
        "LAST FETCH TIMES\n"
        "══════════════════════════\n"
        f"Fundamentals : {_ts(fetches['fundamentals'])}\n"
        f"News         : {_ts(fetches['news'])}\n"
        f"Social       : {_ts(fetches['social'])}\n"
        f"Technical    : {_ts(fetches['technical'])}\n\n"
        "CURRENT DATA\n"
        f"Macro bias   : {macro or 'none'}\n"
        f"News mood    : {news}\n"
        f"Social top   : {dict(sorted(social.items(), key=lambda x: -x[1])[:5]) or 'none'}\n"
    )
    await update.message.reply_text(text)


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    now = _now_utc()
    uptime = now - _bot_start_time
    hours, rem = divmod(int(uptime.total_seconds()), 3600)
    minutes = rem // 60

    with _state_lock:
        today_count = _signals_today
        subs        = len(_subscribers)
        history_len = len(_signal_history)

    text = (
        "BOT STATUS\n"
        "══════════════════════════\n"
        f"Uptime          : {hours}h {minutes}m\n"
        f"Started         : {_ts(_bot_start_time)}\n"
        f"Signals today   : {today_count}\n"
        f"Total in memory : {history_len}\n"
        f"Subscribers     : {subs}\n"
        f"Scan interval   : {SCAN_INTERVAL}s\n"
        f"Signal cooldown : {SIGNAL_COOLDOWN}s\n"
        f"Log file        : {LOG_FILE}\n"
    )
    await update.message.reply_text(text)


# ---------------------------------------------------------------------------
# Flask web server
# ---------------------------------------------------------------------------
flask_app = Flask(__name__)

@flask_app.route("/health")
def health():
    return "OK", 200

@flask_app.route("/")
def index():
    return f"OTC Signal Aggregator running | {_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC", 200

@flask_app.route("/status")
def web_status():
    with _state_lock:
        return {
            "uptime_seconds": int((_now_utc() - _bot_start_time).total_seconds()),
            "signals_today":  _signals_today,
            "subscribers":    len(_subscribers),
            "last_fetch":     {k: _ts(v) for k, v in _last_fetch.items()},
            "macro_bias":     _macro_bias,
            "news_sentiment": _news_sentiment,
        }, 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def _run_flask() -> None:
    log.info("[Flask] Starting on port 10000...")
    flask_app.run(host="0.0.0.0", port=10000)


async def _run_bot() -> None:
    """Build and run the PTB application."""
    global _ptb_app

    app = Application.builder().token(BOT_TOKEN).build()
    _ptb_app = app

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("latest",      cmd_latest))
    app.add_handler(CommandHandler("subscribe",   cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(CommandHandler("data",        cmd_data))
    app.add_handler(CommandHandler("status",      cmd_status))

    log.info("[Bot] Starting Telegram bot (polling)...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    # Keep running until process is killed
    while True:
        await asyncio.sleep(3600)


def _start_bot_thread() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run_bot())
    except Exception as exc:
        log.critical("[Bot] Thread crashed: %s", exc, exc_info=True)
    finally:
        loop.close()


if __name__ == "__main__":
    log.info("=" * 60)
    log.info("OTC Signal Aggregator Bot – starting")
    log.info("=" * 60)

    # 1. WebSocket price stream (daemon)
    ws_thread = threading.Thread(
        target=_run_websocket_forever, daemon=True, name="WebSocket"
    )
    ws_thread.start()

    # 2. Scan scheduler (daemon)
    sched_thread = threading.Thread(
        target=_scheduler_loop, daemon=True, name="Scheduler"
    )
    sched_thread.start()

    # 3. Telegram bot (daemon)
    bot_thread = threading.Thread(
        target=_start_bot_thread, daemon=True, name="TelegramBot"
    )
    bot_thread.start()

    # 4. Flask – blocks main thread, keeps Render alive
    _run_flask()
