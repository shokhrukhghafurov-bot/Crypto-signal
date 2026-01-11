\
from __future__ import annotations

import asyncio
import json
import os
import random
from dataclasses import dataclass
from typing import Dict, Optional

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
BINANCE_MARKET = os.getenv("BINANCE_MARKET", "FUTURES").strip().upper()  # FUTURES/SPOT

@dataclass
class Signal:
    signal_id: int
    symbol: str
    direction: str  # LONG / SHORT
    entry: float
    sl: float
    tp1: float
    tp2: float
    market: str = "FUTURES"  # FUTURES/SPOT (for price source)

@dataclass
class UserTrade:
    user_id: int
    signal: Signal
    tp1_hit: bool = False
    sl_moved_to_be: bool = False
    active: bool = True

class PriceFeed:
    \"\"\"Provides latest price for a symbol.
    - If USE_REAL_PRICE=1 -> Binance WebSocket trade stream
    - else -> mock random walk
    \"\"\"

    def __init__(self) -> None:
        self._prices: Dict[str, float] = {}
        self._tasks: Dict[str, asyncio.Task] = {}

    def _key(self, symbol: str, market: str) -> str:
        return f\"{market}:{symbol.upper()}\"

    def get_latest(self, symbol: str, market: str) -> Optional[float]:
        return self._prices.get(self._key(symbol, market))

    def _set_latest(self, symbol: str, market: str, price: float) -> None:
        self._prices[self._key(symbol, market)] = float(price)

    async def ensure_stream(self, symbol: str, market: str) -> None:
        key = self._key(symbol, market)
        if key in self._tasks and not self._tasks[key].done():
            return
        self._tasks[key] = asyncio.create_task(self._run_stream(symbol, market))

    async def _run_stream(self, symbol: str, market: str) -> None:
        sym = symbol.lower()
        market = market.upper()
        if market == "SPOT":
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
                        # trade price: "p" (spot) and also "p" for futures trade stream
                        p = data.get("p")
                        if p is None:
                            continue
                        self._set_latest(symbol, market, float(p))
            except Exception:
                # reconnect with backoff
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    # Mock (used when USE_REAL_PRICE=0)
    def mock_price(self, symbol: str) -> float:
        symbol = symbol.upper()
        price = self._prices.get(self._key(symbol, "MOCK"), random.uniform(100, 50000))
        price *= random.uniform(0.996, 1.004)
        self._prices[self._key(symbol, "MOCK")] = price
        return price

class Backend:
    def __init__(self) -> None:
        self.trades: Dict[int, UserTrade] = {}
        self.feed = PriceFeed()

    def open_trade(self, user_id: int, signal: Signal) -> None:
        self.trades[user_id] = UserTrade(user_id=user_id, signal=signal)

    async def _get_price(self, signal: Signal) -> float:
        if not USE_REAL_PRICE:
            return self.feed.mock_price(signal.symbol)

        market = (signal.market or BINANCE_MARKET or "FUTURES").upper()
        await self.feed.ensure_stream(signal.symbol, market)
        latest = self.feed.get_latest(signal.symbol, market)
        # if stream not ready yet -> fallback to mock once
        return latest if latest is not None else self.feed.mock_price(signal.symbol)

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

                # SL before TP1
                if not trade.tp1_hit and hit_sl(s.sl):
                    trade.active = False
                    await bot.send_message(
                        chat_id=trade.user_id,
                        text=(
                            "❌ SIGNAL AUTO CLOSED — STOP LOSS\\n\\n"
                            f"🪙 {s.symbol}\\n"
                            f"Price: {price:.6f}\\n"
                            "Status: LOSS 🔴"
                        ),
                    )
                    continue

                # TP1
                if not trade.tp1_hit and hit_tp(s.tp1):
                    trade.tp1_hit = True
                    trade.sl_moved_to_be = True
                    await bot.send_message(
                        chat_id=trade.user_id,
                        text=(
                            "🟡 TP1 HIT\\n\\n"
                            f"🪙 {s.symbol}\\n"
                            f"Price: {price:.6f}\\n"
                            f"Closed: {TP1_PARTIAL_CLOSE_PCT}%\\n"
                            "SL moved to Entry (BE)"
                        ),
                    )
                    continue

                # After TP1: return to Entry -> BE close
                if trade.tp1_hit and trade.sl_moved_to_be and hit_sl(s.entry):
                    trade.active = False
                    await bot.send_message(
                        chat_id=trade.user_id,
                        text=(
                            "⚪ SIGNAL AUTO CLOSED — BREAK EVEN\\n\\n"
                            f"🪙 {s.symbol}\\n"
                            f"Price: {price:.6f}\\n"
                            "Status: SAFE ⚪"
                        ),
                    )
                    continue

                # TP2
                if hit_tp(s.tp2):
                    trade.active = False
                    await bot.send_message(
                        chat_id=trade.user_id,
                        text=(
                            "✅ SIGNAL AUTO CLOSED — TP2 HIT\\n\\n"
                            f"🪙 {s.symbol}\\n"
                            f"Price: {price:.6f}\\n"
                            "Status: WIN 🟢"
                        ),
                    )
                    continue

            await asyncio.sleep(TRACK_INTERVAL_SECONDS)
