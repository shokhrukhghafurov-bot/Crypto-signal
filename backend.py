\
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List, Any

import aiohttp
import numpy as np
import pandas as pd
import websockets
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
from zoneinfo import ZoneInfo

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

TOP_N = max(10, _env_int("TOP_N", 50))
SCAN_INTERVAL_SECONDS = max(30, _env_int("SCAN_INTERVAL_SECONDS", 150))
CONFIDENCE_MIN = max(0, min(100, _env_int("CONFIDENCE_MIN", 80)))
COOLDOWN_MINUTES = max(1, _env_int("COOLDOWN_MINUTES", 180))

USE_REAL_PRICE = _env_bool("USE_REAL_PRICE", True)
TRACK_INTERVAL_SECONDS = max(1, _env_int("TRACK_INTERVAL_SECONDS", 3))
TP1_PARTIAL_CLOSE_PCT = max(0, min(100, _env_int("TP1_PARTIAL_CLOSE_PCT", 50)))

ATR_MULT_SL = max(0.5, _env_float("ATR_MULT_SL", 1.5))
TP1_R = max(0.5, _env_float("TP1_R", 1.5))
TP2_R = max(1.0, _env_float("TP2_R", 3.0))

# News filter
NEWS_FILTER = _env_bool("NEWS_FILTER", False)
CRYPTOPANIC_TOKEN = os.getenv("CRYPTOPANIC_TOKEN", "").strip()
NEWS_LOOKBACK_MIN = max(5, _env_int("NEWS_LOOKBACK_MIN", 60))
NEWS_ACTION = os.getenv("NEWS_ACTION", "FUTURES_OFF").strip().upper()  # FUTURES_OFF / PAUSE_ALL

# Macro filter
MACRO_FILTER = _env_bool("MACRO_FILTER", False)
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
    p = max(0.0, min(100.0, float(TP1_PARTIAL_CLOSE_PCT)))
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
    BASE_URL = "https://cryptopanic.com/api/v1/posts/"

    def __init__(self) -> None:
        self._cache: Dict[str, Tuple[float, str]] = {}
        self._cache_ttl = 60

    def enabled(self) -> bool:
        return NEWS_FILTER and bool(CRYPTOPANIC_TOKEN)

    def base_coin(self, symbol: str) -> str:
        s = symbol.upper()
        return s[:-4] if s.endswith("USDT") else s[:3]

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
                "public": "true",
            }
            async with session.get(self.BASE_URL, params=params) as r:
                if r.status != 200:
                    action = "ALLOW"
                else:
                    data = await r.json()
                    posts = (data or {}).get("results", []) or []
                    cutoff = now - (NEWS_LOOKBACK_MIN * 60)
                    recent = False
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
                            break
                    if recent:
                        action = NEWS_ACTION if NEWS_ACTION in ("FUTURES_OFF", "PAUSE_ALL") else "FUTURES_OFF"
        except Exception:
            action = "ALLOW"

        self._cache[key] = (now, action)
        return action

# ------------------ Macro calendar (AUTO fetch) ------------------
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

    def mock_price(self, market: str, symbol: str) -> float:
        key = ("MOCK:" + market.upper(), symbol.upper())
        price = self._prices.get(key, random.uniform(100, 50000))
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
    close = df["close"]
    high = df["high"]
    low = df["low"]
    df = df.copy()
    df["ema50"] = EMAIndicator(close, window=50).ema_indicator()
    df["ema200"] = EMAIndicator(close, window=200).ema_indicator()
    df["rsi"] = RSIIndicator(close, window=14).rsi()
    macd = MACD(close, window_slow=26, window_fast=12, window_sign=9)
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_hist"] = macd.macd_diff()
    df["adx"] = ADXIndicator(high, low, close, window=14).adx()
    atr = AverageTrueRange(high, low, close, window=14)
    df["atr"] = atr.average_true_range()
    return df

def _trend_dir(df: pd.DataFrame) -> Optional[str]:
    row = df.iloc[-1]
    if pd.isna(row["ema50"]) or pd.isna(row["ema200"]):
        return None
    return "LONG" if row["ema50"] > row["ema200"] else ("SHORT" if row["ema50"] < row["ema200"] else None)

def _trigger_15m(direction: str, df15i: pd.DataFrame) -> bool:
    if len(df15i) < 5:
        return False
    last = df15i.iloc[-1]
    prev = df15i.iloc[-2]
    if any(pd.isna(last[x]) for x in ("rsi","macd","macd_signal","macd_hist")) or pd.isna(prev["macd_hist"]):
        return False
    macd_ok = (last["macd"] > last["macd_signal"] and last["macd_hist"] > prev["macd_hist"]) if direction == "LONG" else (last["macd"] < last["macd_signal"] and last["macd_hist"] < prev["macd_hist"])
    if not macd_ok:
        return False
    if direction == "LONG":
        return prev["rsi"] < 50 and last["rsi"] >= 50 and last["rsi"] <= 70
    return prev["rsi"] > 50 and last["rsi"] <= 50 and last["rsi"] >= 30

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
    conf = _confidence(adx4, adx1, float(last15["rsi"]), atr_pct)
    return {"direction": dir1, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "rr": rr, "confidence": conf, "adx1": adx1, "atr_pct": atr_pct}

def choose_market(adx1_max: float, atr_pct_max: float) -> str:
    return "FUTURES" if (not np.isnan(adx1_max)) and adx1_max >= 28 and atr_pct_max >= 0.8 else "SPOT"

# ------------------ Backend ------------------
class Backend:
    def __init__(self) -> None:
        self.feed = PriceFeed()
        self.news = NewsFilter()
        self.macro = MacroCalendar()

        self.trades: Dict[Tuple[int, int], UserTrade] = {}
        self._last_signal_ts: Dict[str, float] = {}
        self._signal_seq = 1

        self.last_signal: Optional[Signal] = None
        self.last_spot_signal: Optional[Signal] = None
        self.last_futures_signal: Optional[Signal] = None
        self.scanned_symbols_last: int = 0
        self.last_news_action: str = "ALLOW"
        self.last_macro_action: str = "ALLOW"
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

    def open_trade(self, user_id: int, signal: Signal) -> None:
        self.trades[(user_id, signal.signal_id)] = UserTrade(user_id=user_id, signal=signal, opened_ts=time.time())

    def remove_trade(self, user_id: int, signal_id: int) -> bool:
        t = self.trades.pop((user_id, signal_id), None)
        if t is None:
            return False
        # manual close: record stats using last known price (or entry)
        close_price = float(t.last_price or t.signal.entry or 0.0)
        try:
            self.record_trade_close(t, "CLOSED", close_price, time.time())
        except Exception:
            pass
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
                pass

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

    def perf_today(self, market: str) -> dict:
        key = _day_key_tz(time.time())
        return self._bucket(market, "days", key)

    def perf_week(self, market: str) -> dict:
        key = _week_key_tz(time.time())
        return self._bucket(market, "weeks", key)

    def report_daily(self, market: str, days: int = 7) -> list[str]:
        now = dt.datetime.now(tz=ZoneInfo(TZ_NAME)).date()
        out: list[str] = []
        for i in range(days - 1, -1, -1):
            d = now - dt.timedelta(days=i)
            k = d.isoformat()
            b = self._bucket(market, "days", k)
            trades = int(b.get("trades", 0))
            wins = int(b.get("wins", 0))
            losses = int(b.get("losses", 0))
            pnl = float(b.get("sum_pnl_pct", 0.0))
            wr = (wins / trades * 100.0) if trades else 0.0
            out.append(f"{k}: trades={trades} winrate={wr:.1f}% pnl={pnl:+.2f}%")
        return out

    def report_weekly(self, market: str, weeks: int = 4) -> list[str]:
        today = dt.datetime.now(tz=ZoneInfo(TZ_NAME)).date()
        out: list[str] = []
        seen = set()
        for i in range(weeks - 1, -1, -1):
            d = today - dt.timedelta(days=7 * i)
            k = _week_key_tz(dt.datetime(d.year, d.month, d.day, tzinfo=ZoneInfo(TZ_NAME)).timestamp())
            if k in seen:
                continue
            seen.add(k)
            b = self._bucket(market, "weeks", k)
            trades = int(b.get("trades", 0))
            wins = int(b.get("wins", 0))
            losses = int(b.get("losses", 0))
            pnl = float(b.get("sum_pnl_pct", 0.0))
            wr = (wins / trades * 100.0) if trades else 0.0
            out.append(f"{k}: trades={trades} winrate={wr:.1f}% pnl={pnl:+.2f}%")
        return out

    def get_trade(self, user_id: int, signal_id: int) -> Optional[UserTrade]:
        return self.trades.get((user_id, signal_id))

    def get_user_trades(self, user_id: int) -> List[UserTrade]:
        return sorted([t for (uid, _), t in self.trades.items() if uid == user_id], key=lambda x: x.opened_ts, reverse=True)

    def get_next_macro(self) -> Optional[Tuple[MacroEvent, Tuple[float, float]]]:
        return self.macro.next_event()

    async def _get_price(self, signal: Signal) -> float:
        market = (signal.market or "FUTURES").upper()
        if not USE_REAL_PRICE:
            return self.feed.mock_price(market, signal.symbol)
        await self.feed.ensure_stream(market, signal.symbol)
        latest = self.feed.get_latest(market, signal.symbol)
        return latest if latest is not None else self.feed.mock_price(market, signal.symbol)

    async def track_loop(self, bot) -> None:
        while True:
            # iterate over snapshot to allow removals
            for key, trade in list(self.trades.items()):
                if not trade.active:
                    continue

                s = trade.signal
                price = await self._get_price(s)
                trade.last_price = float(price)

                hit_tp = lambda lvl: price >= lvl if s.direction == "LONG" else price <= lvl
                hit_sl = lambda lvl: price <= lvl if s.direction == "LONG" else price >= lvl

                if not trade.tp1_hit and hit_sl(s.sl):
                    trade.active = False
                    trade.result = "LOSS"
                    await bot.send_message(trade.user_id,
                        f"❌ SIGNAL AUTO CLOSED — STOP LOSS\\n\\n🪙 {s.symbol} ({s.market})\\nPrice: {price:.6f}\\nStatus: LOSS 🔴")
                    # auto-remove
                    self.trades.pop(key, None)
                    continue

                if not trade.tp1_hit and hit_tp(s.tp1):
                    trade.tp1_hit = True
                    trade.sl_moved_to_be = True
                    trade.result = "TP1"
                    await bot.send_message(trade.user_id,
                        f"🟡 TP1 HIT\\n\\n🪙 {s.symbol} ({s.market})\\nPrice: {price:.6f}\\nClosed: {TP1_PARTIAL_CLOSE_PCT}%\\nSL moved to Entry (BE)")
                    continue

                if trade.tp1_hit and trade.sl_moved_to_be and hit_sl(s.entry):
                    trade.active = False
                    trade.result = "BE"
                    await bot.send_message(trade.user_id,
                        f"⚪ SIGNAL AUTO CLOSED — BREAK EVEN\\n\\n🪙 {s.symbol} ({s.market})\\nPrice: {price:.6f}\\nStatus: SAFE ⚪")
                    self.trades.pop(key, None)
                    continue

                if hit_tp(s.tp2):
                    trade.active = False
                    trade.result = "WIN"
                    await bot.send_message(trade.user_id,
                        f"✅ SIGNAL AUTO CLOSED — TP2 HIT\\n\\n🪙 {s.symbol} ({s.market})\\nPrice: {price:.6f}\\nStatus: WIN 🟢")
                    self.trades.pop(key, None)
                    continue

            await asyncio.sleep(TRACK_INTERVAL_SECONDS)

    async def scanner_loop(self, emit_signal_cb, emit_macro_alert_cb) -> None:
        while True:
            start = time.time()
            try:
                async with MultiExchangeData() as api:
                    await self.macro.ensure_loaded(api.session)  # type: ignore[arg-type]

                    symbols = await api.get_top_usdt_symbols(TOP_N)
                    self.scanned_symbols_last = len(symbols)

                    mac_act, mac_ev, mac_win = self.macro.current_action()
                    self.last_macro_action = mac_act

                    if mac_act != "ALLOW" and mac_ev and mac_win and self.macro.should_notify(mac_ev):
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

                        await emit_signal_cb(sig)
                        await asyncio.sleep(2)

            except Exception:
                pass

            elapsed = time.time() - start
            await asyncio.sleep(max(5, SCAN_INTERVAL_SECONDS - elapsed))
