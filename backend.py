\
from __future__ import annotations

import asyncio
import json
import os
import random
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

# ------------------ Models ------------------
@dataclass(frozen=True)
class Signal:
    signal_id: int
    market: str      # SPOT / FUTURES
    symbol: str
    direction: str   # LONG / SHORT
    timeframe: str   # "15m/1h/4h"
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr: float
    confidence: int
    confirmations: str  # e.g. "BINANCE+BYBIT"
    ts: float

@dataclass
class UserTrade:
    user_id: int
    signal: Signal
    tp1_hit: bool = False
    sl_moved_to_be: bool = False
    active: bool = True

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

    async def _get_json(self, url: str) -> Any:
        assert self.session is not None
        async with self.session.get(url) as r:
            r.raise_for_status()
            return await r.json()

    def _df_from_ohlcv(self, rows: List[List[Any]], order: str) -> pd.DataFrame:
        # order: "binance" => [open_time, open, high, low, close, ...]
        # order: "bybit" => [start, open, high, low, close, volume, turnover]
        # order: "okx" => [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
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
        else:  # okx
            df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume","vol_ccy","vol_quote","confirm"])
            df["open_time"] = pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="ms")
            for col in ("open","high","low","close","volume","vol_ccy","vol_quote"):
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["open","high","low","close"]).sort_values("open_time").reset_index(drop=True)
        return df

    async def klines_binance(self, symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
        url = f"{self.BINANCE_SPOT}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        raw = await self._get_json(url)
        return self._df_from_ohlcv(raw, "binance")

    async def klines_bybit(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        # Bybit v5: interval is minutes: 15, 60, 240
        interval_map = {"15m":"15", "1h":"60", "4h":"240"}
        itv = interval_map.get(interval, "15")
        url = f"{self.BYBIT}/v5/market/kline?category=spot&symbol={symbol}&interval={itv}&limit={limit}"
        data = await self._get_json(url)
        rows = (data or {}).get("result", {}).get("list", [])
        # bybit returns newest-first, reverse
        rows = list(reversed(rows))
        return self._df_from_ohlcv(rows, "bybit")

    def okx_inst(self, symbol: str) -> str:
        # BTCUSDT -> BTC-USDT (simple mapping)
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT"
        return symbol

    async def klines_okx(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        bar_map = {"15m":"15m", "1h":"1H", "4h":"4H"}
        bar = bar_map.get(interval, "15m")
        inst = self.okx_inst(symbol)
        url = f"{self.OKX}/api/v5/market/candles?instId={inst}&bar={bar}&limit={limit}"
        data = await self._get_json(url)
        rows = (data or {}).get("data", [])
        # okx returns newest-first, reverse
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
    if row["ema50"] > row["ema200"]:
        return "LONG"
    if row["ema50"] < row["ema200"]:
        return "SHORT"
    return None

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
    else:
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
    # ADX 4h (0..25)
    score += 0 if np.isnan(adx4) else int(min(25, max(0, (adx4 - 15) * 1.25)))
    # ADX 1h (0..25)
    score += 0 if np.isnan(adx1) else int(min(25, max(0, (adx1 - 15) * 1.25)))
    # RSI zone (0..20)
    if not np.isnan(rsi15):
        if 40 <= rsi15 <= 60:
            score += 20
        elif 35 <= rsi15 <= 65:
            score += 15
        else:
            score += 8
    # ATR% (0..20)
    if 0.3 <= atr_pct <= 3.0:
        score += 20
    elif 0.2 <= atr_pct <= 4.0:
        score += 14
    else:
        score += 8
    # Base trend alignment bonus (10)
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

    return {
        "direction": dir1,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "rr": rr,
        "confidence": conf,
        "adx1": adx1,
        "atr_pct": atr_pct,
    }

def choose_market(adx1_max: float, atr_pct_max: float) -> str:
    if (not np.isnan(adx1_max)) and adx1_max >= 28 and atr_pct_max >= 0.8:
        return "FUTURES"
    return "SPOT"

# ------------------ Backend (scanner + tracking) ------------------
class Backend:
    def __init__(self) -> None:
        self.feed = PriceFeed()
        self.trades: Dict[Tuple[int, int], UserTrade] = {}
        self._last_signal_ts: Dict[str, float] = {}
        self._signal_seq = 1

        self.last_scan_ts: float = 0.0
        self.last_signal: Optional[Signal] = None
        self.scanned_symbols_last: int = 0

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
        self.trades[(user_id, signal.signal_id)] = UserTrade(user_id=user_id, signal=signal)

    async def _get_price(self, signal: Signal) -> float:
        market = (signal.market or "FUTURES").upper()
        if not USE_REAL_PRICE:
            return self.feed.mock_price(market, signal.symbol)
        await self.feed.ensure_stream(market, signal.symbol)
        latest = self.feed.get_latest(market, signal.symbol)
        return latest if latest is not None else self.feed.mock_price(market, signal.symbol)

    async def track_loop(self, bot) -> None:
        while True:
            for trade in list(self.trades.values()):
                if not trade.active:
                    continue

                s = trade.signal
                price = await self._get_price(s)

                hit_tp = lambda lvl: price >= lvl if s.direction == "LONG" else price <= lvl
                hit_sl = lambda lvl: price <= lvl if s.direction == "LONG" else price >= lvl

                if not trade.tp1_hit and hit_sl(s.sl):
                    trade.active = False
                    await bot.send_message(trade.user_id,
                        f"❌ SIGNAL AUTO CLOSED — STOP LOSS\\n\\n🪙 {s.symbol} ({s.market})\\nPrice: {price:.6f}\\nStatus: LOSS 🔴")
                    continue

                if not trade.tp1_hit and hit_tp(s.tp1):
                    trade.tp1_hit = True
                    trade.sl_moved_to_be = True
                    await bot.send_message(trade.user_id,
                        f"🟡 TP1 HIT\\n\\n🪙 {s.symbol} ({s.market})\\nPrice: {price:.6f}\\nClosed: {TP1_PARTIAL_CLOSE_PCT}%\\nSL moved to Entry (BE)")
                    continue

                if trade.tp1_hit and trade.sl_moved_to_be and hit_sl(s.entry):
                    trade.active = False
                    await bot.send_message(trade.user_id,
                        f"⚪ SIGNAL AUTO CLOSED — BREAK EVEN\\n\\n🪙 {s.symbol} ({s.market})\\nPrice: {price:.6f}\\nStatus: SAFE ⚪")
                    continue

                if hit_tp(s.tp2):
                    trade.active = False
                    await bot.send_message(trade.user_id,
                        f"✅ SIGNAL AUTO CLOSED — TP2 HIT\\n\\n🪙 {s.symbol} ({s.market})\\nPrice: {price:.6f}\\nStatus: WIN 🟢")
                    continue

            await asyncio.sleep(TRACK_INTERVAL_SECONDS)

    async def scanner_loop(self, emit_signal_cb) -> None:
        while True:
            start = time.time()
            self.last_scan_ts = start
            try:
                async with MultiExchangeData() as api:
                    symbols = await api.get_top_usdt_symbols(TOP_N)
                    self.scanned_symbols_last = len(symbols)

                    for sym in symbols:
                        if not self.can_emit(sym):
                            continue

                        # Fetch candles concurrently per exchange and timeframe
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
                                else:  # OKX
                                    df15 = await api.klines_okx(sym, "15m", 200)
                                    df1h = await api.klines_okx(sym, "1h", 200)
                                    df4h = await api.klines_okx(sym, "4h", 200)
                                res = evaluate_on_exchange(df15, df1h, df4h)
                                return name, res
                            except Exception:
                                return name, None

                        tasks = [fetch_exchange("BINANCE"), fetch_exchange("BYBIT"), fetch_exchange("OKX")]
                        results = await asyncio.gather(*tasks)

                        good = [(name, r) for (name, r) in results if r is not None]
                        if len(good) < 2:
                            continue

                        # Need 2/3 confirmation of direction
                        dirs = {}
                        for name, r in good:
                            dirs.setdefault(r["direction"], []).append((name, r))
                        # choose direction with max supporters
                        best_dir, supporters = max(dirs.items(), key=lambda kv: len(kv[1]))
                        if len(supporters) < 2:
                            continue

                        # Build final signal from supporters: median entry and levels
                        entries = [s[1]["entry"] for s in supporters]
                        sls = [s[1]["sl"] for s in supporters]
                        tp1s = [s[1]["tp1"] for s in supporters]
                        tp2s = [s[1]["tp2"] for s in supporters]
                        rrs = [s[1]["rr"] for s in supporters]
                        confs = [s[1]["confidence"] for s in supporters]
                        adx1s = [s[1]["adx1"] for s in supporters]
                        atrp = [s[1]["atr_pct"] for s in supporters]

                        entry = float(np.median(entries))
                        sl = float(np.median(sls))
                        tp1 = float(np.median(tp1s))
                        tp2 = float(np.median(tp2s))
                        rr = float(np.median(rrs))
                        conf = int(np.median(confs))

                        if conf < CONFIDENCE_MIN or rr < 2.0:
                            continue

                        market = choose_market(float(np.nanmax(adx1s)), float(np.nanmax(atrp)))
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
                            ts=time.time(),
                        )

                        self.mark_emitted(sym)
                        self.last_signal = sig
                        await emit_signal_cb(sig)
                        await asyncio.sleep(2)

            except Exception:
                pass

            elapsed = time.time() - start
            await asyncio.sleep(max(5, SCAN_INTERVAL_SECONDS - elapsed))
