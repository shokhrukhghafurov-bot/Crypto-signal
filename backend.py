\
from __future__ import annotations

import asyncio
import json
import os
import random
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import websockets

def _env_int(name: str, default: int) -> int:
    val = os.getenv(name, "").strip()
    if not val:
        return default
    try:
        return int(val)
    except Exception:
        return default

def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name, "").strip().lower()
    if val == "":
        return default
    return val in ("1", "true", "yes", "y", "on")

TP1_PARTIAL_CLOSE_PCT = max(0, min(100, _env_int("TP1_PARTIAL_CLOSE_PCT", 50)))
TRACK_INTERVAL_SECONDS = max(1, _env_int("TRACK_INTERVAL_SECONDS", 3))
USE_REAL_PRICE = _env_bool("USE_REAL_PRICE", False)

@dataclass(frozen=True)
class Signal:
    signal_id: int
    market: str      # SPOT / FUTURES
    symbol: str
    direction: str   # LONG / SHORT
    entry: float
    sl: float
    tp1: float
    tp2: float

@dataclass
class UserTrade:
    user_id: int
    signal: Signal
    tp1_hit: bool = False
    sl_moved_to_be: bool = False
    active: bool = True

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

        if m == "SPOT":
            url = f"wss://stream.binance.com:9443/ws/{sym}@trade"
        else:
            url = f"wss://fstream.binance.com/ws/{sym}@trade"

        backoff = 1
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    backoff = 1
                    async for msg in ws:
                        data = json.loads(msg)
                        p = data.get("p")
                        if p is None:
                            continue
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

class Backend:
    def __init__(self) -> None:
        self.trades: Dict[Tuple[int, int], UserTrade] = {}
        self.feed = PriceFeed()

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

                def hit_tp(level: float) -> bool:
                    return price >= level if s.direction == "LONG" else price <= level

                def hit_sl(level: float) -> bool:
                    return price <= level if s.direction == "LONG" else price >= level

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
