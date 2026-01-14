\
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import os
import random
import re
import time
import datetime as dt
import math
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List, Any

import aiohttp
import numpy as np
import pandas as pd
import websockets
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from ta.volume import OnBalanceVolumeIndicator, MFIIndicator
from zoneinfo import ZoneInfo

import logging

import db_store

logger = logging.getLogger("crypto-signal")

# --- i18n template safety guard (prevents leaking {placeholders} to users) ---
_UNFILLED_RE = re.compile(r'(?<!\{)\{[a-zA-Z0-9_]+\}(?!\})')

def _sanitize_template_text(uid: int, text: str, ctx: str = "") -> str:
    if not text:
        return text
    hits = _UNFILLED_RE.findall(text)
    if hits:
        logger.error("Unfilled i18n placeholders for uid=%s ctx=%s hits=%s text=%r", uid, ctx, sorted(set(hits)), text)
        text = _UNFILLED_RE.sub("", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

async def safe_send(bot, chat_id: int, text: str, *, ctx: str = "", **kwargs):
    text = _sanitize_template_text(chat_id, text, ctx=ctx)
    # Never recurse. Send via bot API.
    return await bot.send_message(chat_id, text, **kwargs)



# ------------------ ENV helpers ------------------
def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "yes", "y", "on")


# --- i18n helpers (loaded from i18n.json; no hardcoded auto-close texts) ---
I18N_FILE = Path(__file__).with_name("i18n.json")
LANG_FILE = Path("langs.json")

_I18N_CACHE: dict = {}
_I18N_MTIME: float = 0.0
_LANG_CACHE: dict[int, str] = {}
_LANG_CACHE_MTIME: float = 0.0

def _load_i18n() -> dict:
    global _I18N_CACHE, _I18N_MTIME
    try:
        if I18N_FILE.exists():
            mt = I18N_FILE.stat().st_mtime
            if _I18N_CACHE and mt == _I18N_MTIME:
                return _I18N_CACHE
            data = json.loads(I18N_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "ru" in data and "en" in data:
                _I18N_CACHE = data
                _I18N_MTIME = mt
                return data
    except Exception:
        pass
    if not _I18N_CACHE:
        _I18N_CACHE = {"ru": {}, "en": {}}
    return _I18N_CACHE

def _load_langs_if_needed() -> None:
    global _LANG_CACHE, _LANG_CACHE_MTIME
    try:
        if not LANG_FILE.exists():
            _LANG_CACHE = {}
            _LANG_CACHE_MTIME = 0.0
            return
        mt = LANG_FILE.stat().st_mtime
        if mt == _LANG_CACHE_MTIME:
            return
        raw = json.loads(LANG_FILE.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            tmp: dict[int, str] = {}
            for k, v in raw.items():
                try:
                    uid = int(k)
                except Exception:
                    continue
                vv = (v or "").lower().strip()
                tmp[uid] = "en" if vv == "en" else "ru"
            _LANG_CACHE = tmp
            _LANG_CACHE_MTIME = mt
    except Exception:
        pass

def _get_lang(uid: int) -> str:
    _load_langs_if_needed()
    return _LANG_CACHE.get(int(uid), "ru")

def _tr(uid: int, key: str) -> str:
    lang = _get_lang(uid)
    i18n = _load_i18n()
    return i18n.get(lang, i18n.get("en", {})).get(key, key)

class _SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"

def _trf(uid: int, key: str, **kwargs) -> str:
    tmpl = _tr(uid, key)
    try:
        return str(tmpl).format_map(_SafeDict(**kwargs))
    except Exception:
        return str(tmpl)

def _market_label(uid: int, market: str) -> str:
    return _tr(uid, "lbl_spot") if str(market).upper() == "SPOT" else _tr(uid, "lbl_futures")
# --- /i18n helpers ---


# ------------------ backend-only metrics helpers ------------------

def calc_profit_pct(entry: float, tp: float, direction: str) -> float:
    """Estimated profit percent from entry to tp for LONG/SHORT."""
    try:
        entry = float(entry)
        tp = float(tp)
        if entry == 0:
            return 0.0
        side = str(direction or "LONG").upper().strip()
        if side == "SHORT":
            return (entry - tp) / entry * 100.0
        return (tp - entry) / entry * 100.0
    except Exception:
        return 0.0

def default_futures_leverage() -> str:
    v = os.getenv("FUTURES_LEVERAGE_DEFAULT", "5").strip()
    return v or "5"

def open_metrics(sig: "Signal") -> dict:
    """Return placeholders for OPEN templates, computed only here (backend-only)."""
    side = (getattr(sig, "direction", "") or "LONG").upper().strip()
    mkt = (getattr(sig, "market", "") or "FUTURES").upper().strip()
    # prefer TP2 for estimate; fallback to TP1
    tp = getattr(sig, "tp2", 0.0) or getattr(sig, "tp1", 0.0) or 0.0
    entry = getattr(sig, "entry", 0.0) or 0.0
    profit = calc_profit_pct(entry, tp, side)
    return {
        "side": side,
        "market": mkt,
        "profit": round(float(profit), 1),
        "lev": default_futures_leverage(),
        "rr": round(float(getattr(sig, "rr", 0.0) or 0.0), 1),
    }

def fmt_pnl_pct(p: float) -> str:
    try:
        p = float(p)
        sign = "+" if p > 0 else ""
        return f"{sign}{p:.1f}%"
    except Exception:
        return "0.0%"

TOP_N = max(10, _env_int("TOP_N", 50))
SCAN_INTERVAL_SECONDS = max(30, _env_int("SCAN_INTERVAL_SECONDS", 150))
CONFIDENCE_MIN = max(0, min(100, _env_int("CONFIDENCE_MIN", 80)))
COOLDOWN_MINUTES = max(1, _env_int("COOLDOWN_MINUTES", 180))

USE_REAL_PRICE = _env_bool("USE_REAL_PRICE", True)
TRACK_INTERVAL_SECONDS = max(1, _env_int("TRACK_INTERVAL_SECONDS", 3))
# Backward-compatible default partial close percent (legacy)
TP1_PARTIAL_CLOSE_PCT = max(0, min(100, _env_int("TP1_PARTIAL_CLOSE_PCT", 50)))

# New per-market settings
TP1_PARTIAL_CLOSE_PCT_SPOT = max(0, min(100, _env_int("TP1_PARTIAL_CLOSE_PCT_SPOT", TP1_PARTIAL_CLOSE_PCT)))
TP1_PARTIAL_CLOSE_PCT_FUTURES = max(0, min(100, _env_int("TP1_PARTIAL_CLOSE_PCT_FUTURES", TP1_PARTIAL_CLOSE_PCT)))

BE_AFTER_TP1_SPOT = _env_bool("BE_AFTER_TP1_SPOT", True)
BE_AFTER_TP1_FUTURES = _env_bool("BE_AFTER_TP1_FUTURES", True)

# Fee-protected BE: exit remaining position at entry +/- fee buffer so BE is not negative after fees (model)
BE_FEE_BUFFER = _env_bool("BE_FEE_BUFFER", True)
FEE_RATE_SPOT = max(0.0, _env_float("FEE_RATE_SPOT", 0.001))        # e.g. 0.001 = 0.10%
FEE_RATE_FUTURES = max(0.0, _env_float("FEE_RATE_FUTURES", 0.001))  # e.g. 0.001 = 0.10%
FEE_BUFFER_MULTIPLIER = max(0.0, _env_float("FEE_BUFFER_MULTIPLIER", 2.0))  # 2 = entry+exit fees
ATR_MULT_SL = max(0.5, _env_float("ATR_MULT_SL", 1.5))
TP1_R = max(0.5, _env_float("TP1_R", 1.5))
TP2_R = max(1.0, _env_float("TP2_R", 3.0))

# News filter
NEWS_FILTER = _env_bool("NEWS_FILTER", False)
CRYPTOPANIC_TOKEN = os.getenv("CRYPTOPANIC_TOKEN", "").strip()
CRYPTOPANIC_PUBLIC = _env_bool("CRYPTOPANIC_PUBLIC", False)
CRYPTOPANIC_REGIONS = os.getenv("CRYPTOPANIC_REGIONS", "en").strip() or "en"
CRYPTOPANIC_KIND = os.getenv("CRYPTOPANIC_KIND", "news").strip() or "news"
NEWS_LOOKBACK_MIN = max(5, _env_int("NEWS_LOOKBACK_MIN", 60))
NEWS_ACTION = os.getenv("NEWS_ACTION", "FUTURES_OFF").strip().upper()  # FUTURES_OFF / PAUSE_ALL

# Macro filter
MACRO_FILTER = _env_bool("MACRO_FILTER", False)

# Orderbook filter (optional)
ORDERBOOK_FILTER = _env_bool("ORDERBOOK_FILTER", False)
MACRO_ACTION = os.getenv("MACRO_ACTION", "FUTURES_OFF").strip().upper()  # FUTURES_OFF / PAUSE_ALL
BLACKOUT_BEFORE_MIN = max(0, _env_int("BLACKOUT_BEFORE_MIN", 30))
BLACKOUT_AFTER_MIN = max(0, _env_int("BLACKOUT_AFTER_MIN", 45))
TZ_NAME = os.getenv("TZ_NAME", "Europe/Berlin").strip() or "Europe/Berlin"

FOMC_DECISION_HOUR_ET = max(0, min(23, _env_int("FOMC_DECISION_HOUR_ET", 14)))
FOMC_DECISION_MINUTE_ET = max(0, min(59, _env_int("FOMC_DECISION_MINUTE_ET", 0)))

# ------------------ Models ------------------
@dataclass(frozen=True)
class Signal:
    signal_id: int
    market: str
    symbol: str
    direction: str
    timeframe: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr: float
    confidence: int
    confirmations: str
    risk_note: str
    ts: float

@dataclass
class UserTrade:
    user_id: int
    signal: Signal
    tp1_hit: bool = False
    sl_moved_to_be: bool = False
    be_price: float = 0.0
    active: bool = True
    result: str = "ACTIVE"  # ACTIVE / TP1 / WIN / LOSS / BE / CLOSED
    last_price: float = 0.0
    opened_ts: float = 0.0


# ------------------ Trade performance stats (spot/futures) ------------------
TRADE_STATS_FILE = Path("trade_stats.json")

# structure:
# {
#   "spot": {"days": {"YYYY-MM-DD": {...}}, "weeks": {"YYYY-WNN": {...}}},
#   "futures": {...}
# }
def _empty_bucket() -> Dict[str, float]:
    return {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "be": 0,
        "tp1_hits": 0,
        "sum_pnl_pct": 0.0,
    }

def _safe_pct(x: float) -> float:
    if not math.isfinite(x):
        return 0.0
    return float(x)

def _market_key(market: str) -> str:
    m = (market or "FUTURES").strip().upper()
    return "spot" if m == "SPOT" else "futures"


def _tp1_partial_close_pct(market: str) -> float:
    mk = _market_key(market)
    return float(TP1_PARTIAL_CLOSE_PCT_SPOT if mk == "spot" else TP1_PARTIAL_CLOSE_PCT_FUTURES)


# Backward-compat alias (older code referenced _partial_close_pct)
def _partial_close_pct(market: str) -> float:
    return _tp1_partial_close_pct(market)


# Backward-compat alias (older code referenced _be_enabled)
def _be_enabled(market: str) -> bool:
    return _be_after_tp1(market)


def _be_after_tp1(market: str) -> bool:
    mk = _market_key(market)
    return bool(BE_AFTER_TP1_SPOT if mk == "spot" else BE_AFTER_TP1_FUTURES)

def _fee_rate(market: str) -> float:
    mk = _market_key(market)
    return float(FEE_RATE_SPOT if mk == "spot" else FEE_RATE_FUTURES)

def _be_exit_price(entry: float, side: str, market: str) -> float:
    """Return modeled BE exit price that covers fees on remaining position."""
    entry = float(entry)
    if not BE_FEE_BUFFER:
        return entry
    fr = max(0.0, _fee_rate(market))
    buf = max(0.0, float(FEE_BUFFER_MULTIPLIER)) * fr
    side = (side or "LONG").upper()
    if side == "SHORT":
        return entry * (1.0 - buf)
    return entry * (1.0 + buf)


def _day_key_tz(ts: float) -> str:
    d = dt.datetime.fromtimestamp(ts, tz=ZoneInfo(TZ_NAME)).date()
    return d.isoformat()

def _week_key_tz(ts: float) -> str:
    d = dt.datetime.fromtimestamp(ts, tz=ZoneInfo(TZ_NAME)).date()
    y, w, _ = d.isocalendar()
    return f"{y}-W{int(w):02d}"

def _calc_effective_pnl_pct(trade: "UserTrade", close_price: float, close_reason: str) -> float:
    s = trade.signal
    entry = float(s.entry or 0.0)
    if entry <= 0:
        return 0.0

    side = (s.direction or "LONG").upper()
    def pnl_pct_for(price: float) -> float:
        price = float(price)
        if side == "SHORT":
            return (entry - price) / entry * 100.0
        return (price - entry) / entry * 100.0

    # No TP1 hit => full position closes at close_price
    if not trade.tp1_hit:
        return _safe_pct(pnl_pct_for(close_price))

    # TP1 hit => partial close at TP1, rest closes at BE or TP2 (or manual)
    p = max(0.0, min(100.0, float(_tp1_partial_close_pct(s.market))))
    a = p / 100.0
    tp1_price = float(s.tp1 or close_price)

    pnl_tp1 = pnl_pct_for(tp1_price) * a

    # Remaining portion:
    if close_reason == "WIN":
        # rest closes at TP2 (if provided), else close_price
        tp2_price = float(s.tp2 or close_price)
        pnl_rest = pnl_pct_for(tp2_price) * (1.0 - a)
    elif close_reason == "BE":
        pnl_rest = 0.0
    else:
        # manual close / unknown: use close_price for remaining
        pnl_rest = pnl_pct_for(close_price) * (1.0 - a)

    return _safe_pct(pnl_tp1 + pnl_rest)

def _bump_stats(store: dict, market: str, ts: float, close_reason: str, pnl_pct: float, tp1_hit: bool) -> None:
    mk = _market_key(market)
    dayk = _day_key_tz(ts)
    weekk = _week_key_tz(ts)

    mroot = store.setdefault(mk, {"days": {}, "weeks": {}})
    days = mroot.setdefault("days", {})
    weeks = mroot.setdefault("weeks", {})

    def get_bucket(root: dict, k: str) -> dict:
        b = root.get(k)
        if not isinstance(b, dict):
            b = _empty_bucket()
            root[k] = b
        # ensure keys
        for kk, vv in _empty_bucket().items():
            if kk not in b:
                b[kk] = vv
        return b

    bd = get_bucket(days, dayk)
    bw = get_bucket(weeks, weekk)

    def apply(bucket: dict) -> None:
        bucket["trades"] = int(bucket.get("trades", 0)) + 1
        # wins/losses based on pnl sign (если WIN но ушёл в минус — засчитываем как LOSS)
        if pnl_pct > 0:
            bucket["wins"] = int(bucket.get("wins", 0)) + 1
        elif pnl_pct < 0:
            bucket["losses"] = int(bucket.get("losses", 0)) + 1
        else:
            bucket["be"] = int(bucket.get("be", 0)) + 1

        if tp1_hit:
            bucket["tp1_hits"] = int(bucket.get("tp1_hits", 0)) + 1

        bucket["sum_pnl_pct"] = float(bucket.get("sum_pnl_pct", 0.0)) + float(pnl_pct)

    apply(bd)
    apply(bw)

@dataclass(frozen=True)

class MacroEvent:
    name: str
    type: str
    start_ts_utc: float

# ------------------ News risk filter (CryptoPanic) ------------------
class NewsFilter:
    # Use Developer v2 endpoint (matches API Reference in CryptoPanic Developers panel)
    BASE_URL = "https://cryptopanic.com/api/developer/v2/posts/"

    def __init__(self) -> None:
        self._last_block: tuple[str, float] | None = None
        self._cache: Dict[str, Tuple[float, str]] = {}
        self._cache_ttl = 60
        # prevent hanging network calls
        self._timeout = aiohttp.ClientTimeout(total=8)

    def enabled(self) -> bool:
        return NEWS_FILTER and bool(CRYPTOPANIC_TOKEN)

    def base_coin(self, symbol: str) -> str:
        # Accept formats: BTCUSDT, BTC/USDT, BTC-USDT, etc.
        s = str(symbol).upper().replace("/", "").replace("-", "").strip()
        for q in ("USDT", "USDC", "BUSD", "USD", "FDUSD", "TUSD", "DAI", "EUR", "BTC", "ETH"):
            if s.endswith(q) and len(s) > len(q):
                return s[:-len(q)]
        return s

    async def action_for_symbol(self, session: aiohttp.ClientSession, symbol: str) -> str:
        if not self.enabled():
            return "ALLOW"

        coin = self.base_coin(symbol)
        key = f"{coin}:{NEWS_ACTION}:{NEWS_LOOKBACK_MIN}"
        now = time.time()
        cached = self._cache.get(key)
        if cached and (now - cached[0]) < self._cache_ttl:
            return cached[1]

        action = "ALLOW"
        try:
            params = {
                "auth_token": CRYPTOPANIC_TOKEN,
                "currencies": coin,
                "filter": "important",
                # reduce noise; can be overridden via env if desired
                "kind": CRYPTOPANIC_KIND,
                "regions": CRYPTOPANIC_REGIONS,
            }
            if CRYPTOPANIC_PUBLIC:
                params["public"] = "true"

            async with session.get(self.BASE_URL, params=params, timeout=self._timeout) as r:
                if r.status != 200:
                    action = "ALLOW"
                else:
                    data = await r.json()
                    posts = (data or {}).get("results", []) or []
                    cutoff = now - (NEWS_LOOKBACK_MIN * 60)
                    recent = False
                    recent_post = None
                    for p in posts[:20]:
                        published_at = p.get("published_at") or p.get("created_at")
                        if not published_at:
                            continue
                        try:
                            ts = pd.to_datetime(published_at.replace("Z", "+00:00")).timestamp()
                        except Exception:
                            continue
                        if ts >= cutoff:
                            recent = True
                            recent_post = p
                            break
                    if recent:
                        action = NEWS_ACTION if NEWS_ACTION in ("FUTURES_OFF", "PAUSE_ALL") else "FUTURES_OFF"
                        # remember last block for status (reason, until_ts)
                        reason = coin
                        try:
                            if recent_post:
                                title = (recent_post.get("title") or recent_post.get("slug") or "").strip()
                                src = None
                                if isinstance(recent_post.get("source"), dict):
                                    src = (recent_post["source"].get("title") or recent_post["source"].get("name"))
                                if not src:
                                    src = (recent_post.get("domain") or recent_post.get("source") or "")
                                src = (src or "").strip()
                                # CryptoPanic 'important' filter is used; keep it in text for clarity
                                parts = []
                                if src:
                                    parts.append(src)
                                parts.append("Important")
                                if title:
                                    parts.append(title)
                                reason = "CryptoPanic — " + " / ".join(parts)
                        except Exception:
                            pass
                        self._last_block = (reason, now + NEWS_LOOKBACK_MIN * 60)
        except Exception:
            action = "ALLOW"

        self._cache[key] = (now, action)
        return action

# ------------------ Macro calendar (AUTO fetch) ------------------

    def last_block_info(self) -> tuple[str, float] | None:
        """Return (reason, until_ts) for the last time news filter triggered."""
        return self._last_block

class MacroCalendar:
    BLS_URL = "https://www.bls.gov/schedule/news_release/current_year.asp"
    FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

    def __init__(self) -> None:
        self._events: List[MacroEvent] = []
        self._last_fetch_ts: float = 0.0
        self._fetch_ttl: float = 6 * 60 * 60
        self._notified: Dict[str, float] = {}
        self._notify_cooldown = 6 * 60 * 60

    def enabled(self) -> bool:
        return MACRO_FILTER

    async def ensure_loaded(self, session: aiohttp.ClientSession) -> None:
        if not self.enabled():
            self._events = []
            return
        now = time.time()
        if self._events and (now - self._last_fetch_ts) < self._fetch_ttl:
            return
        await self.fetch(session)

    async def fetch(self, session: aiohttp.ClientSession) -> None:
        events: List[MacroEvent] = []

        # BLS schedule
        try:
            async with session.get(self.BLS_URL) as r:
                r.raise_for_status()
                html = await r.text()

            tables = pd.read_html(html)
            for t in tables:
                cols = [c.strip().lower() for c in t.columns.astype(str).tolist()]
                if not ("release" in cols and "date" in cols):
                    continue
                t2 = t.copy()
                t2.columns = cols
                for _, row in t2.iterrows():
                    release = str(row.get("release", "")).strip()
                    date_str = str(row.get("date", "")).strip()
                    time_str = str(row.get("time", "")).strip()
                    if not release or not date_str or not time_str:
                        continue

                    rel_l = release.lower()
                    if "consumer price index" in rel_l:
                        name = "CPI (US Inflation)"
                        etype = "CPI"
                    elif "employment situation" in rel_l:
                        name = "NFP (US Jobs)"
                        etype = "NFP"
                    else:
                        continue

                    try:
                        dt_et = pd.to_datetime(f"{date_str} {time_str}")
                    except Exception:
                        continue
                    dt_et = dt_et.tz_localize(ZoneInfo("America/New_York"), ambiguous="NaT", nonexistent="shift_forward")
                    dt_utc = dt_et.tz_convert(ZoneInfo("UTC")).timestamp()
                    events.append(MacroEvent(name=name, type=etype, start_ts_utc=float(dt_utc)))
                break
        except Exception:
            pass

        # FOMC calendar (best-effort parse)
        try:
            async with session.get(self.FOMC_URL) as r:
                r.raise_for_status()
                txt = await r.text()

            year = pd.Timestamp.utcnow().year
            months = ["January","February","March","April","May","June","July","August","September","October","November","December"]
            for m in months:
                pattern = rf"{m}\\s*\\n\\s*(\\d{{1,2}})\\s*[-–]\\s*(\\d{{1,2}})"
                m1 = re.search(pattern, txt, flags=re.IGNORECASE)
                if not m1:
                    continue
                d2 = int(m1.group(2))
                try:
                    dt_et = pd.Timestamp(year=year, month=months.index(m)+1, day=d2, hour=FOMC_DECISION_HOUR_ET, minute=FOMC_DECISION_MINUTE_ET, tz="America/New_York")
                except Exception:
                    continue
                events.append(MacroEvent(name="FOMC Rate Decision", type="FED", start_ts_utc=float(dt_et.tz_convert("UTC").timestamp())))
        except Exception:
            pass

        now = time.time()
        filtered = [e for e in events if (now - 2*24*3600) <= e.start_ts_utc <= (now + 370*24*3600)]
        filtered.sort(key=lambda e: e.start_ts_utc)
        self._events = filtered
        self._last_fetch_ts = time.time()

    def blackout_window(self, ev: MacroEvent) -> Tuple[float, float]:
        start = ev.start_ts_utc - (BLACKOUT_BEFORE_MIN * 60)
        end = ev.start_ts_utc + (BLACKOUT_AFTER_MIN * 60)
        return start, end

    def current_action(self) -> Tuple[str, Optional[MacroEvent], Optional[Tuple[float, float]]]:
        if not self.enabled():
            return "ALLOW", None, None
        now = time.time()
        for ev in self._events:
            w0, w1 = self.blackout_window(ev)
            if w0 <= now <= w1:
                act = MACRO_ACTION if MACRO_ACTION in ("FUTURES_OFF", "PAUSE_ALL") else "FUTURES_OFF"
                return act, ev, (w0, w1)
        return "ALLOW", None, None

    def status(self) -> tuple[str, MacroEvent | None, tuple[float, float] | None]:
        """Return (action, event, (w0,w1)) for current moment."""
        return self.current_action()

    def next_event(self) -> Optional[Tuple[MacroEvent, Tuple[float, float]]]:
        if not self.enabled():
            return None
        now = time.time()
        for ev in self._events:
            w0, w1 = self.blackout_window(ev)
            if now < w0:
                return ev, (w0, w1)
        return None

    def should_notify(self, ev: MacroEvent) -> bool:
        key = f"{ev.type}:{ev.start_ts_utc}"
        now = time.time()
        last = self._notified.get(key, 0.0)
        if (now - last) >= self._notify_cooldown:
            self._notified[key] = now
            return True
        return False

# ------------------ Price feed (WS, Binance) ------------------
class PriceFeed:
    def __init__(self) -> None:
        self._prices: Dict[Tuple[str, str], float] = {}
        self._tasks: Dict[Tuple[str, str], asyncio.Task] = {}

    def get_latest(self, market: str, symbol: str) -> Optional[float]:
        return self._prices.get((market.upper(), symbol.upper()))

    def _set_latest(self, market: str, symbol: str, price: float) -> None:
        self._prices[(market.upper(), symbol.upper())] = float(price)

    async def ensure_stream(self, market: str, symbol: str) -> None:
        key = (market.upper(), symbol.upper())
        t = self._tasks.get(key)
        if t and not t.done():
            return
        self._tasks[key] = asyncio.create_task(self._run_stream(market, symbol))

    async def _run_stream(self, market: str, symbol: str) -> None:
        m = market.upper()
        sym = symbol.lower()
        url = f"wss://stream.binance.com:9443/ws/{sym}@trade" if m == "SPOT" else f"wss://fstream.binance.com/ws/{sym}@trade"
        backoff = 1
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    backoff = 1
                    async for msg in ws:
                        data = json.loads(msg)
                        p = data.get("p")
                        if p is not None:
                            self._set_latest(m, symbol, float(p))
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def mock_price(self, market: str, symbol: str, base: float | None = None) -> float:
        key = ("MOCK:" + market.upper(), symbol.upper())
        seed = (float(base) if base is not None and base > 0 else None)
        price = self._prices.get(key, seed if seed is not None else random.uniform(100, 50000))
        price *= random.uniform(0.996, 1.004)
        self._prices[key] = price
        return price

# ------------------ Exchange data (candles) ------------------
class MultiExchangeData:
    BINANCE_SPOT = "https://api.binance.com"
    BYBIT = "https://api.bybit.com"
    OKX = "https://www.okx.com"

    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def get_top_usdt_symbols(self, n: int) -> List[str]:
        assert self.session is not None
        url = f"{self.BINANCE_SPOT}/api/v3/ticker/24hr"
        async with self.session.get(url) as r:
            r.raise_for_status()
            data = await r.json()

        items = []
        for t in data:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
                continue
            try:
                qv = float(t.get("quoteVolume", "0") or 0)
            except Exception:
                qv = 0.0
            items.append((qv, sym))
        items.sort(reverse=True, key=lambda x: x[0])
        return [sym for _, sym in items[:n]]

    async def _get_json(self, url: str, params: Optional[Dict[str, str]] = None) -> Any:
        assert self.session is not None
        async with self.session.get(url, params=params) as r:
            r.raise_for_status()
            return await r.json()

    def _df_from_ohlcv(self, rows: List[List[Any]], order: str) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()

        if order == "binance":
            df = pd.DataFrame(rows, columns=[
                "open_time","open","high","low","close","volume",
                "close_time","quote_volume","n_trades","taker_base","taker_quote","ignore"
            ])
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            for col in ("open","high","low","close","volume","quote_volume"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
        elif order == "bybit":
            df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume","turnover"])
            df["open_time"] = pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="ms")
            for col in ("open","high","low","close","volume","turnover"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume","vol_ccy","vol_quote","confirm"])
            df["open_time"] = pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="ms")
            for col in ("open","high","low","close","volume","vol_ccy","vol_quote"):
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["open","high","low","close"]).sort_values("open_time").reset_index(drop=True)
        return df

    async def klines_binance(self, symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
        url = f"{self.BINANCE_SPOT}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": str(limit)}
        raw = await self._get_json(url, params=params)
        return self._df_from_ohlcv(raw, "binance")

    
    async def depth_binance(self, symbol: str, limit: int = 20) -> Optional[dict]:
        """Fetch top-of-book depth from Binance Spot. Returns None on errors.
        When ORDERBOOK_FILTER is enabled, you can use this to compute imbalance.
        """
        url = f"{self.BINANCE_SPOT}/api/v3/depth"
        params = {"symbol": symbol, "limit": str(limit)}
        try:
            data = await self._get_json(url, params=params)
            return data if isinstance(data, dict) else None
        except Exception:
            return None
    async def klines_bybit(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        interval_map = {"15m":"15", "1h":"60", "4h":"240"}
        itv = interval_map.get(interval, "15")
        url = f"{self.BYBIT}/v5/market/kline"
        params = {"category": "spot", "symbol": symbol, "interval": itv, "limit": str(limit)}
        data = await self._get_json(url, params=params)
        rows = (data or {}).get("result", {}).get("list", []) or []
        rows = list(reversed(rows))
        return self._df_from_ohlcv(rows, "bybit")

    def okx_inst(self, symbol: str) -> str:
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT"
        return symbol

    async def klines_okx(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        bar_map = {"15m":"15m", "1h":"1H", "4h":"4H"}
        bar = bar_map.get(interval, "15m")
        inst = self.okx_inst(symbol)
        url = f"{self.OKX}/api/v5/market/candles"
        params = {"instId": inst, "bar": bar, "limit": str(limit)}
        data = await self._get_json(url, params=params)
        rows = (data or {}).get("data", []) or []
        rows = list(reversed(rows))
        return self._df_from_ohlcv(rows, "okx")

# ------------------ Indicators / engine ------------------
def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add core indicators used by the engine.
    Expected columns: open, high, low, close, (optional) volume.
    """
    df = df.copy()
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    # Trend
    try:
        df["ema50"] = EMAIndicator(close, window=50).ema_indicator()
        df["ema200"] = EMAIndicator(close, window=200).ema_indicator()
    except Exception:
        df["ema50"] = np.nan
        df["ema200"] = np.nan

    # Momentum
    try:
        df["rsi"] = RSIIndicator(close, window=14).rsi()
    except Exception:
        df["rsi"] = np.nan

    try:
        macd = MACD(close, window_slow=26, window_fast=12, window_sign=9)
        df["macd"] = macd.macd()
        df["macd_signal"] = macd.macd_signal()
        df["macd_hist"] = macd.macd_diff()
    except Exception:
        df["macd"] = np.nan
        df["macd_signal"] = np.nan
        df["macd_hist"] = np.nan

    # Trend strength
    try:
        df["adx"] = ADXIndicator(high, low, close, window=14).adx()
    except Exception:
        df["adx"] = np.nan

    # Volatility
    try:
        df["atr"] = AverageTrueRange(high, low, close, window=14).average_true_range()
    except Exception:
        df["atr"] = np.nan

    # Bollinger Bands
    try:
        bb = BollingerBands(close, window=20, window_dev=2)
        df["bb_mavg"] = bb.bollinger_mavg()
        df["bb_high"] = bb.bollinger_hband()
        df["bb_low"] = bb.bollinger_lband()
    except Exception:
        df["bb_mavg"] = np.nan
        df["bb_high"] = np.nan
        df["bb_low"] = np.nan

    # Volume-based (optional)
    if "volume" in df.columns:
        vol = df["volume"].astype(float).fillna(0.0)
        df["vol_sma20"] = vol.rolling(window=20, min_periods=5).mean()

        try:
            df["obv"] = OnBalanceVolumeIndicator(close, vol).on_balance_volume()
        except Exception:
            df["obv"] = np.nan

        try:
            df["mfi"] = MFIIndicator(high, low, close, vol, window=14).money_flow_index()
        except Exception:
            df["mfi"] = np.nan

        # VWAP (cumulative)
        try:
            tp = (high + low + close) / 3.0
            pv = tp * vol
            cum_vol = vol.cumsum().replace(0, np.nan)
            df["vwap"] = pv.cumsum() / cum_vol
        except Exception:
            df["vwap"] = np.nan
    else:
        df["vol_sma20"] = np.nan
        df["obv"] = np.nan
        df["mfi"] = np.nan
        df["vwap"] = np.nan

    return df


def _trend_dir(df: pd.DataFrame) -> Optional[str]:
    row = df.iloc[-1]
    if pd.isna(row["ema50"]) or pd.isna(row["ema200"]):
        return None
    return "LONG" if row["ema50"] > row["ema200"] else ("SHORT" if row["ema50"] < row["ema200"] else None)

def _trigger_15m(direction: str, df15i: pd.DataFrame) -> bool:
    if len(df15i) < 25:
        return False
    last = df15i.iloc[-1]
    prev = df15i.iloc[-2]

    # Required indicators
    req = ("rsi","macd","macd_signal","macd_hist","bb_mavg","bb_high","bb_low","vol_sma20","mfi")
    if any(pd.isna(last.get(x, np.nan)) for x in req) or pd.isna(prev.get("macd_hist", np.nan)):
        return False

    # MACD momentum confirmation
    if direction == "LONG":
        macd_ok = (last["macd"] > last["macd_signal"]) and (last["macd_hist"] > prev["macd_hist"])
    else:
        macd_ok = (last["macd"] < last["macd_signal"]) and (last["macd_hist"] < prev["macd_hist"])
    if not macd_ok:
        return False

    # RSI cross 50 with guard rails
    if direction == "LONG":
        rsi_ok = (prev["rsi"] < 50) and (last["rsi"] >= 50) and (last["rsi"] <= 70)
    else:
        rsi_ok = (prev["rsi"] > 50) and (last["rsi"] <= 50) and (last["rsi"] >= 30)
    if not rsi_ok:
        return False

    # Bollinger filter: avoid extremes
    close = float(last["close"])
    if direction == "LONG":
        if not (close > float(last["bb_mavg"]) and close < float(last["bb_high"])):
            return False
        if float(last["mfi"]) >= 80:
            return False
    else:
        if not (close < float(last["bb_mavg"]) and close > float(last["bb_low"])):
            return False
        if float(last["mfi"]) <= 20:
            return False

    # Volume activity: require above-average volume
    vol = float(last.get("volume", 0.0) or 0.0)
    vol_sma = float(last["vol_sma20"])
    if vol_sma > 0 and vol < vol_sma * 1.05:
        return False

    return True

def _build_levels(direction: str, entry: float, atr: float) -> Tuple[float, float, float, float]:
    R = max(1e-9, ATR_MULT_SL * atr)
    if direction == "LONG":
        sl = entry - R
        tp1 = entry + TP1_R * R
        tp2 = entry + TP2_R * R
    else:
        sl = entry + R
        tp1 = entry - TP1_R * R
        tp2 = entry - TP2_R * R
    rr = abs(tp2 - entry) / abs(entry - sl)
    return sl, tp1, tp2, rr

def _confidence(adx4: float, adx1: float, rsi15: float, atr_pct: float) -> int:
    score = 0
    score += 0 if np.isnan(adx4) else int(min(25, max(0, (adx4 - 15) * 1.25)))
    score += 0 if np.isnan(adx1) else int(min(25, max(0, (adx1 - 15) * 1.25)))
    if not np.isnan(rsi15):
        score += 20 if 40 <= rsi15 <= 60 else (15 if 35 <= rsi15 <= 65 else 8)
    score += 20 if 0.3 <= atr_pct <= 3.0 else (14 if 0.2 <= atr_pct <= 4.0 else 8)
    score += 10
    return int(max(0, min(100, score)))

def evaluate_on_exchange(df15: pd.DataFrame, df1h: pd.DataFrame, df4h: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df15.empty or df1h.empty or df4h.empty:
        return None
    df15i = _add_indicators(df15)
    df1hi = _add_indicators(df1h)
    df4hi = _add_indicators(df4h)
    dir4 = _trend_dir(df4hi)
    dir1 = _trend_dir(df1hi)
    if dir4 is None or dir1 is None or dir4 != dir1:
        return None
    adx4 = float(df4hi.iloc[-1]["adx"]) if not pd.isna(df4hi.iloc[-1]["adx"]) else np.nan
    adx1 = float(df1hi.iloc[-1]["adx"]) if not pd.isna(df1hi.iloc[-1]["adx"]) else np.nan
    if np.isnan(adx4) or adx4 < 20:
        return None
    if np.isnan(adx1) or adx1 < 18:
        return None
    if not _trigger_15m(dir1, df15i):
        return None
    last15 = df15i.iloc[-1]
    entry = float(last15["close"])
    atr = float(last15["atr"]) if not pd.isna(last15["atr"]) else 0.0
    if atr <= 0:
        return None
    atr_pct = (atr / entry) * 100.0 if entry > 0 else 0.0
    sl, tp1, tp2, rr = _build_levels(dir1, entry, atr)

    # --- Advanced TA snapshot (for display + scoring) ---
    rsi15 = float(last15.get("rsi", np.nan))
    macd_hist15 = float(last15.get("macd_hist", np.nan))
    bb_low = float(last15.get("bb_low", np.nan))
    bb_mid = float(last15.get("bb_mavg", np.nan))
    bb_high = float(last15.get("bb_high", np.nan))
    vol = float(last15.get("volume", 0.0) or 0.0)
    vol_sma = float(last15.get("vol_sma20", np.nan))
    vol_rel = (vol / vol_sma) if (vol_sma and not np.isnan(vol_sma) and vol_sma > 0) else np.nan

    pattern, pat_bias = _candle_pattern(df15i)
    support, resistance = _nearest_levels(df1hi, lookback=180, swing=3)

    vwap15 = float(last15.get("vwap", np.nan))
    vwap_bias = "—"
    try:
        if not np.isnan(vwap15):
            vwap_bias = "above" if entry >= vwap15 else "below"
    except Exception:
        pass

    bb_pos = "—"
    try:
        if not (np.isnan(bb_low) or np.isnan(bb_mid) or np.isnan(bb_high)):
            if entry >= bb_high:
                bb_pos = "upper"
            elif entry <= bb_low:
                bb_pos = "lower"
            else:
                bb_pos = "mid"
    except Exception:
        pass

    # --- Score (0..100) used as confidence ---
    score = 50
    # trend already aligned on 1h/4h, reward stronger trend
    if not np.isnan(adx1) and adx1 >= 25:
        score += 10
    if not np.isnan(adx4) and adx4 >= 25:
        score += 10

    # momentum confirmation
    if not np.isnan(macd_hist15):
        if dir1 == "LONG" and macd_hist15 > 0:
            score += 10
        if dir1 == "SHORT" and macd_hist15 < 0:
            score += 10

    # RSI quality zone
    if not np.isnan(rsi15):
        if dir1 == "LONG" and (50 <= rsi15 <= 70):
            score += 8
        if dir1 == "SHORT" and (30 <= rsi15 <= 50):
            score += 8

    # Bollinger position (avoid extremes; mid/upper for long, mid/lower for short)
    if bb_pos != "—":
        if dir1 == "LONG" and bb_pos in ("mid", "upper"):
            score += 4
        if dir1 == "SHORT" and bb_pos in ("mid", "lower"):
            score += 4

    # volume activity
    if not np.isnan(vol_rel):
        if vol_rel >= 1.05:
            score += 6
        elif vol_rel >= 0.95:
            score += 3

    # candle pattern bias
    if pat_bias != 0:
        if dir1 == "LONG" and pat_bias > 0:
            score += 6
        if dir1 == "SHORT" and pat_bias < 0:
            score += 6

    # proximity to support/resistance (better R:R)
    try:
        if support and dir1 == "LONG":
            dist = abs(entry - support) / max(1e-12, entry)
            if dist <= 0.006:
                score += 4
        if resistance and dir1 == "SHORT":
            dist = abs(resistance - entry) / max(1e-12, entry)
            if dist <= 0.006:
                score += 4
    except Exception:
        pass

    # VWAP bias
    if vwap_bias != "—":
        if dir1 == "LONG" and vwap_bias == "above":
            score += 2
        if dir1 == "SHORT" and vwap_bias == "below":
            score += 2

    score = int(max(0, min(100, score)))

    # Keep legacy confidence too (for logs/back-compat)
    conf_legacy = _confidence(adx4, adx1, rsi15 if not np.isnan(rsi15) else 50.0, atr_pct)

    ta = {
        "trend": f"{dir4}/{dir1}",
        "rsi15": rsi15,
        "macd_hist15": macd_hist15,
        "adx1": adx1,
        "adx4": adx4,
        "atr_pct": atr_pct,
        "bb_pos": bb_pos,
        "vol_rel": vol_rel,
        "pattern": pattern,
        "support": support,
        "resistance": resistance,
        "vwap_bias": vwap_bias,
        "score": score,
    }

    return {
        "direction": dir1,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "rr": rr,
        "confidence": score,
        "confidence_legacy": int(conf_legacy),
        "adx1": adx1,
        "atr_pct": atr_pct,
        "ta": ta,
        "ta_block": _fmt_ta_block(ta),
    }

def choose_market(adx1_max: float, atr_pct_max: float) -> str:
    return "FUTURES" if (not np.isnan(adx1_max)) and adx1_max >= 28 and atr_pct_max >= 0.8 else "SPOT"

# ------------------ Backend ------------------
class Backend:
    def __init__(self) -> None:
        self.feed = PriceFeed()
        self.news = NewsFilter()
        self.macro = MacroCalendar()
        # Trades are stored in PostgreSQL (db_store)
        self._last_signal_ts: Dict[str, float] = {}
        self._signal_seq = 1

        self.last_signal: Optional[Signal] = None
        self.last_spot_signal: Optional[Signal] = None
        self.last_futures_signal: Optional[Signal] = None
        self.scanned_symbols_last: int = 0
        self.last_news_action: str = "ALLOW"
        self.last_macro_action: str = "ALLOW"
        self.scanner_running: bool = True
        self.last_scan_ts: float = 0.0
        self.trade_stats: dict = {}
        self._load_trade_stats()


    def next_signal_id(self) -> int:
        sid = self._signal_seq
        self._signal_seq += 1
        return sid

    def can_emit(self, symbol: str) -> bool:
        ts = self._last_signal_ts.get(symbol.upper(), 0.0)
        return (time.time() - ts) >= (COOLDOWN_MINUTES * 60)

    def mark_emitted(self, symbol: str) -> None:
        self._last_signal_ts[symbol.upper()] = time.time()

    async def open_trade(self, user_id: int, signal: Signal, orig_text: str) -> bool:
        """Persist trade in Postgres. Returns False if already opened."""
        inserted, _tid = await db_store.open_trade_once(
            user_id=int(user_id),
            signal_id=int(signal.signal_id),
            market=(signal.market or "FUTURES").upper(),
            symbol=signal.symbol,
            side=(signal.direction or "LONG").upper(),
            entry=float(signal.entry or 0.0),
            tp1=float(signal.tp1) if signal.tp1 is not None else None,
            tp2=float(signal.tp2) if signal.tp2 is not None else None,
            sl=float(signal.sl) if signal.sl is not None else None,
            orig_text=orig_text or "",
        )
        return bool(inserted)


    async def remove_trade(self, user_id: int, signal_id: int) -> bool:
        row = await db_store.get_trade_by_user_signal(int(user_id), int(signal_id))
        if not row:
            return False
        trade_id = int(row["id"])
        # manual close (keep last known price if present)
        close_price = float(row.get("entry") or 0.0)
        try:
            await db_store.close_trade(trade_id, status="CLOSED", price=close_price, pnl_total_pct=float(row.get("pnl_total_pct") or 0.0))
        except Exception:
            # best effort
            await db_store.close_trade(trade_id, status="CLOSED")
        return True


    # ---------------- trade stats helpers ----------------
    def _load_trade_stats(self) -> None:
        self.trade_stats = {"spot": {"days": {}, "weeks": {}}, "futures": {"days": {}, "weeks": {}}}
        if TRADE_STATS_FILE.exists():
            try:
                data = json.loads(TRADE_STATS_FILE.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    self.trade_stats.update(data)
            except Exception:
                logger.exception("scanner_loop exception")

    def _save_trade_stats(self) -> None:
        try:
            TRADE_STATS_FILE.write_text(json.dumps(self.trade_stats, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        except Exception:
            pass

    def record_trade_close(self, trade: UserTrade, close_reason: str, close_price: float, close_ts: float | None = None) -> None:
        ts = float(close_ts if close_ts is not None else time.time())
        pnl_pct = _calc_effective_pnl_pct(trade, float(close_price), close_reason)
        _bump_stats(self.trade_stats, trade.signal.market, ts, close_reason, pnl_pct, bool(trade.tp1_hit))
        self._save_trade_stats()

    def _bucket(self, market: str, period: str, key: str) -> dict:
        mk = _market_key(market)
        root = (self.trade_stats or {}).get(mk, {})
        per = root.get(period, {}) if isinstance(root, dict) else {}
        b = per.get(key, {}) if isinstance(per, dict) else {}
        if not isinstance(b, dict):
            b = _empty_bucket()
        # normalize keys
        base = _empty_bucket()
        for k, v in base.items():
            if k not in b:
                b[k] = v
        return b

    async def perf_today(self, user_id: int, market: str) -> dict:
        now = dt.datetime.now(dt.timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + dt.timedelta(days=1)
        return await db_store.perf_bucket(int(user_id), (market or "FUTURES").upper(), since=start, until=end)

    async def perf_week(self, user_id: int, market: str) -> dict:
        now = dt.datetime.now(dt.timezone.utc)
        # ISO week start (Monday)
        start = (now - dt.timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + dt.timedelta(days=7)
        return await db_store.perf_bucket(int(user_id), (market or "FUTURES").upper(), since=start, until=end)

    async def report_daily(self, user_id: int, market: str, days: int = 7, tz: str = "UTC") -> list[dict]:
        return await db_store.daily_report(int(user_id), (market or "FUTURES").upper(), days=int(days), tz=str(tz))

    async def report_weekly(self, user_id: int, market: str, weeks: int = 4, tz: str = "UTC") -> list[dict]:
        return await db_store.weekly_report(int(user_id), (market or "FUTURES").upper(), weeks=int(weeks), tz=str(tz))

    async def get_trade(self, user_id: int, signal_id: int) -> Optional[dict]:
        return await db_store.get_trade_by_user_signal(int(user_id), int(signal_id))

    async def get_user_trades(self, user_id: int) -> list[dict]:
        return await db_store.list_user_trades(int(user_id), include_closed=False, limit=50)

    def get_next_macro(self) -> Optional[Tuple[MacroEvent, Tuple[float, float]]]:
        return self.macro.next_event()

    def get_macro_status(self) -> dict:
        act, ev, win = self.macro.status()
        until_ts = None
        reason = None
        window = None
        if act != "ALLOW" and ev and win:
            w0, w1 = win
            until_ts = w1
            window = (w0, w1)
            reason = getattr(ev, "name", None) or getattr(ev, "title", None) or getattr(ev, "type", None)
        return {"action": act, "event": ev, "window": window, "until_ts": until_ts, "reason": reason}

    def get_news_status(self) -> dict:
        act = getattr(self, "last_news_action", "ALLOW")
        info = self.news.last_block_info() if hasattr(self, "news") else None
        reason = None
        until_ts = None
        if info:
            coin, until = info
            reason = coin
            until_ts = until
        return {"action": act, "reason": reason, "until_ts": until_ts}

    
    async def _fetch_rest_price(self, market: str, symbol: str) -> float | None:
        """REST fallback for price when websocket hasn't produced a tick yet."""
        m = (market or "FUTURES").upper()
        sym = (symbol or "").upper()
        if not sym:
            return None
        url = "https://api.binance.com/api/v3/ticker/price" if m == "SPOT" else "https://fapi.binance.com/fapi/v1/ticker/price"
        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(url, params={"symbol": sym}) as r:
                    if r.status != 200:
                        return None
                    data = await r.json()
            p = data.get("price")
            return float(p) if p is not None else None
        except Exception:
            return None

    async def _get_price(self, signal: Signal) -> float:
        market = (signal.market or "FUTURES").upper()
        base = float(getattr(signal, "entry", 0) or 0) or None

        # Mock mode: keep prices around entry to avoid nonsense.
        if not USE_REAL_PRICE:
            return self.feed.mock_price(market, signal.symbol, base=base)

        # Real mode: websocket first, REST fallback, then (last resort) base/ mock near base.
        await self.feed.ensure_stream(market, signal.symbol)
        latest = self.feed.get_latest(market, signal.symbol)
        if latest is not None:
            return float(latest)

        rest = await self._fetch_rest_price(market, signal.symbol)
        if rest is not None and rest > 0:
            self.feed._set_latest(market, signal.symbol, float(rest))
            return float(rest)

        # Last resort: keep near entry (never random huge).
        if base is not None and base > 0:
            return self.feed.mock_price(market, signal.symbol, base=base)
        return self.feed.mock_price(market, signal.symbol, base=None)

    async def check_signal_openable(self, signal: Signal) -> tuple[bool, str, float]:
        """Return (allowed, reason_code, current_price).

        reason_code is one of: OK, TP2, TP1, SL, TIME, ERROR
        """
        try:
            now = time.time()
            ttl = int(os.getenv("SIGNAL_OPEN_TTL_SECONDS", "1800"))  # 30 min default
            sig_ts = float(getattr(signal, "ts", 0) or 0)
            if sig_ts and ttl > 0 and (now - sig_ts) > ttl:
                price = await self._get_price(signal)
                return False, "TIME", float(price)

            price = float(await self._get_price(signal))

            direction = (getattr(signal, "direction", None) or getattr(signal, "side", None) or "").upper()
            is_short = "SHORT" in direction
            is_long = not is_short

            tp1 = getattr(signal, "tp1", None)
            tp2 = getattr(signal, "tp2", None)
            sl = getattr(signal, "sl", None)

            tp1_f = float(tp1) if tp1 is not None else None
            tp2_f = float(tp2) if tp2 is not None else None
            sl_f = float(sl) if sl is not None else None

            if is_long:
                if tp2_f is not None and price >= tp2_f:
                    return False, "TP2", price
                if tp1_f is not None and price >= tp1_f:
                    return False, "TP1", price
                if sl_f is not None and price <= sl_f:
                    return False, "SL", price
            else:
                if tp2_f is not None and price <= tp2_f:
                    return False, "TP2", price
                if tp1_f is not None and price <= tp1_f:
                    return False, "TP1", price
                if sl_f is not None and price >= sl_f:
                    return False, "SL", price

            return True, "OK", price
        except Exception:
            try:
                price = float(await self._get_price(signal))
            except Exception:
                price = 0.0
            return False, "ERROR", price

    async def track_loop(self, bot) -> None:
        """Main tracker loop. Reads ACTIVE/TP1 trades from PostgreSQL and updates their status."""
        while True:
            try:
                rows = await db_store.list_active_trades(limit=500)
            except Exception:
                logger.exception("track_loop: failed to load active trades from DB")
                rows = []

            for row in rows:
                try:
                    trade_id = int(row["id"])
                    uid = int(row["user_id"])
                    market = (row.get("market") or "FUTURES").upper()
                    symbol = str(row.get("symbol") or "")
                    side = str(row.get("side") or "LONG").upper()

                    s = Signal(
                        signal_id=int(row.get("signal_id") or 0),
                        market=market,
                        symbol=symbol,
                        direction=side,
                        timeframe="",
                        entry=float(row.get("entry") or 0.0),
                        sl=float(row.get("sl") or 0.0),
                        tp1=float(row.get("tp1") or 0.0),
                        tp2=float(row.get("tp2") or 0.0),
                        rr=0.0,
                        confidence=0,
                        confirmations="",
                        risk_note="",
                        ts=float(dt.datetime.now(dt.timezone.utc).timestamp()),
                    )

                    price = await self._get_price(s)
                    price_f = float(price)

                    def hit_tp(lvl: float) -> bool:
                        return price_f >= lvl if side == "LONG" else price_f <= lvl

                    def hit_sl(lvl: float) -> bool:
                        return price_f <= lvl if side == "LONG" else price_f >= lvl

                    status = str(row.get("status") or "ACTIVE").upper()
                    tp1_hit = bool(row.get("tp1_hit"))
                    be_price = float(row.get("be_price") or 0.0) if row.get("be_price") is not None else 0.0

                    # 1) SL before TP1 -> LOSS
                    if not tp1_hit and s.sl and hit_sl(float(s.sl)):
                        pnl = calc_profit_pct(s.entry, float(s.sl), side)
                        await safe_send(bot, uid, _trf(uid, "msg_auto_loss",
                            symbol=s.symbol,
                            status="LOSS",
                        ), ctx="msg_auto_loss")
                        await db_store.close_trade(trade_id, status="LOSS", price=float(s.sl), pnl_total_pct=float(pnl))
                        continue

                    # 2) TP1 -> partial close + BE
                    if not tp1_hit and s.tp1 and hit_tp(float(s.tp1)):
                        be_px = _be_exit_price(s.entry, side, market)
                        await safe_send(bot, uid, _trf(uid, "msg_auto_tp1",
                            symbol=s.symbol,
                            closed_pct=int(_partial_close_pct(market)),
                            be_price=f"{float(be_px):.6f}",
                            status="TP1",
                        ), ctx="msg_auto_tp1")
                        await db_store.set_tp1(trade_id, be_price=float(be_px), price=float(s.tp1), pnl_pct=float(calc_profit_pct(s.entry, float(s.tp1), side)))
                        continue

                    # 3) After TP1: BE close
                    if tp1_hit and _be_enabled(market):
                        be_lvl = be_price if be_price else _be_exit_price(s.entry, side, market)
                        if hit_sl(float(be_lvl)):
                            await safe_send(bot, uid, _trf(uid, "msg_auto_be",
                                symbol=s.symbol,
                                be_price=f"{float(be_lvl):.6f}",
                                status="BE",
                            ), ctx="msg_auto_be")
                            await db_store.close_trade(trade_id, status="BE", price=float(be_lvl), pnl_total_pct=0.0)
                            continue

                    # 4) TP2 -> WIN
                    if s.tp2 and hit_tp(float(s.tp2)):
                        pnl = calc_profit_pct(s.entry, float(s.tp2), side)
                        await safe_send(bot, uid, _trf(uid, "msg_auto_win",
                            symbol=s.symbol,
                            pnl_total=fmt_pnl_pct(float(pnl)),
                            status="WIN",
                        ), ctx="msg_auto_win")
                        await db_store.close_trade(trade_id, status="WIN", price=float(s.tp2), pnl_total_pct=float(pnl))
                        continue

                except Exception:
                    logger.exception("track_loop: error while tracking trade row=%r", row)

            await asyncio.sleep(TRACK_INTERVAL_SECONDS)

    async def scanner_loop(self, emit_signal_cb, emit_macro_alert_cb) -> None:
        while True:
            start = time.time()
            logger.info("SCAN tick start top_n=%s interval=%ss news_filter=%s macro_filter=%s", TOP_N, SCAN_INTERVAL_SECONDS, bool(NEWS_FILTER and CRYPTOPANIC_TOKEN), bool(MACRO_FILTER))
            logger.info("[scanner] tick start TOP_N=%s interval=%ss", TOP_N, SCAN_INTERVAL_SECONDS)
            self.last_scan_ts = start
            try:
                async with MultiExchangeData() as api:
                    await self.macro.ensure_loaded(api.session)  # type: ignore[arg-type]

                    symbols = await api.get_top_usdt_symbols(TOP_N)
                    self.scanned_symbols_last = len(symbols)
                    logger.info("[scanner] symbols loaded: %s", self.scanned_symbols_last)

                    mac_act, mac_ev, mac_win = self.macro.current_action()
                    self.last_macro_action = mac_act
                    if MACRO_FILTER:
                        logger.info("[scanner] macro action=%s next=%s window=%s", mac_act, getattr(mac_ev, "name", None) if mac_ev else None, mac_win)

                    if mac_act != "ALLOW" and mac_ev and mac_win and self.macro.should_notify(mac_ev):
                        logger.info("[macro] alert: action=%s event=%s window=%s", mac_act, getattr(mac_ev, "name", None), mac_win)
                        await emit_macro_alert_cb(mac_act, mac_ev, mac_win, TZ_NAME)

                    for sym in symbols:
                        if not self.can_emit(sym):
                            continue
                        if mac_act == "PAUSE_ALL":
                            continue

                        # News action
                        try:
                            if self.news.enabled():
                                news_act = await self.news.action_for_symbol(api.session, sym)  # type: ignore[arg-type]
                            else:
                                news_act = "ALLOW"
                        except Exception:
                            news_act = "ALLOW"
                        self.last_news_action = news_act
                        if news_act == "PAUSE_ALL":
                            continue

                        async def fetch_exchange(name: str):
                            try:
                                if name == "BINANCE":
                                    df15 = await api.klines_binance(sym, "15m", 250)
                                    df1h = await api.klines_binance(sym, "1h", 250)
                                    df4h = await api.klines_binance(sym, "4h", 250)
                                elif name == "BYBIT":
                                    df15 = await api.klines_bybit(sym, "15m", 200)
                                    df1h = await api.klines_bybit(sym, "1h", 200)
                                    df4h = await api.klines_bybit(sym, "4h", 200)
                                else:
                                    df15 = await api.klines_okx(sym, "15m", 200)
                                    df1h = await api.klines_okx(sym, "1h", 200)
                                    df4h = await api.klines_okx(sym, "4h", 200)
                                res = evaluate_on_exchange(df15, df1h, df4h)
                                return name, res
                            except Exception:
                                return name, None

                        results = await asyncio.gather(fetch_exchange("BINANCE"), fetch_exchange("BYBIT"), fetch_exchange("OKX"))
                        good = [(name, r) for (name, r) in results if r is not None]
                        if len(good) < 2:
                            continue

                        dirs: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
                        for name, r in good:
                            dirs.setdefault(r["direction"], []).append((name, r))
                        best_dir, supporters = max(dirs.items(), key=lambda kv: len(kv[1]))
                        if len(supporters) < 2:
                            continue

                        entry = float(np.median([s[1]["entry"] for s in supporters]))
                        sl = float(np.median([s[1]["sl"] for s in supporters]))
                        tp1 = float(np.median([s[1]["tp1"] for s in supporters]))
                        tp2 = float(np.median([s[1]["tp2"] for s in supporters]))
                        rr = float(np.median([s[1]["rr"] for s in supporters]))
                        conf = int(np.median([s[1]["confidence"] for s in supporters]))

                        if conf < CONFIDENCE_MIN or rr < 2.0:
                            continue

                        market = choose_market(float(np.nanmax([s[1]["adx1"] for s in supporters])), float(np.nanmax([s[1]["atr_pct"] for s in supporters])))
                        risk_notes = []

                        # Add TA summary (real indicators) to the signal card
                        try:
                            best_supporter = max(supporters, key=lambda s: int(s[1].get("confidence", 0)))
                            ta_block = str(best_supporter[1].get("ta_block", "") or "").strip()
                            if ta_block:
                                risk_notes.append(ta_block)
                        except Exception:
                            pass


                        if news_act == "FUTURES_OFF" and market == "FUTURES":
                            market = "SPOT"
                            risk_notes.append("⚠️ Futures paused due to news")
                        if mac_act == "FUTURES_OFF" and market == "FUTURES":
                            market = "SPOT"
                            risk_notes.append("⚠️ Futures signals are temporarily disabled (macro)")

                        conf_names = "+".join(sorted([s[0] for s in supporters]))

                        sid = self.next_signal_id()
                        sig = Signal(
                            signal_id=sid,
                            market=market,
                            symbol=sym,
                            direction=best_dir,
                            timeframe="15m/1h/4h",
                            entry=entry,
                            sl=sl,
                            tp1=tp1,
                            tp2=tp2,
                            rr=rr,
                            confidence=conf,
                            confirmations=conf_names,
                            risk_note="\\n".join(risk_notes).strip(),
                            ts=time.time(),
                        )

                        self.mark_emitted(sym)
                        self.last_signal = sig
                        if sig.market == "SPOT":
                            self.last_spot_signal = sig
                        else:
                            self.last_futures_signal = sig

                        logger.info("[signal] emit %s %s %s conf=%s rr=%.2f notes=%s", sig.symbol, sig.market, sig.direction, sig.confidence, sig.rr, (sig.risk_note or "-")[:120])
                        logger.info("SIGNAL %s %s %s conf=%s rr=%.2f notes=%s", sig.market, sig.symbol, sig.direction, sig.confidence, sig.rr, (sig.risk_note or "-"))
                        logger.info("SIGNAL found %s %s %s conf=%s rr=%.2f exch=%s", sig.market, sig.symbol, sig.direction, sig.confidence, float(sig.rr), sig.confirmations)
                        await emit_signal_cb(sig)
                        await asyncio.sleep(2)

            except Exception:
                logger.exception("scanner_loop error")

            elapsed = time.time() - start
            logger.info("SCAN tick done scanned=%s elapsed=%.1fs last_news=%s last_macro=%s", int(getattr(self, "scanned_symbols_last", 0) or 0), elapsed, getattr(self, "last_news_action", "?"), getattr(self, "last_macro_action", "?"))
            await asyncio.sleep(max(5, SCAN_INTERVAL_SECONDS - elapsed))
