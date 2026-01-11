from __future__ import annotations

import asyncio
import os
import random
from dataclasses import dataclass
from typing import Dict

def _env_int(name: str, default: int) -> int:
    val = os.getenv(name, "").strip()
    if not val:
        return default
    try:
        return int(val)
    except Exception:
        return default

TP1_PARTIAL_CLOSE_PCT = max(0, min(100, _env_int("TP1_PARTIAL_CLOSE_PCT", 50)))
TRACK_INTERVAL_SECONDS = max(1, _env_int("TRACK_INTERVAL_SECONDS", 3))

@dataclass
class Signal:
    signal_id: int
    symbol: str
    direction: str  # LONG / SHORT
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

class Backend:
    """Minimal backend:
    - keeps opened trades in memory
    - uses MOCK price feed (random walk)
    Replace mock_price() with real exchange feed later.
    """

    def __init__(self) -> None:
        self.trades: Dict[int, UserTrade] = {}
        self.prices: Dict[str, float] = {}

    def mock_price(self, symbol: str) -> float:
        symbol = symbol.upper()
        price = self.prices.get(symbol, random.uniform(100, 50000))
        price *= random.uniform(0.996, 1.004)  # +/-0.4%
        self.prices[symbol] = price
        return price

    def open_trade(self, user_id: int, signal: Signal) -> None:
        self.trades[user_id] = UserTrade(user_id=user_id, signal=signal)

    async def track_loop(self, bot) -> None:
        while True:
            for trade in list(self.trades.values()):
                if not trade.active:
                    continue

                s = trade.signal
                price = self.mock_price(s.symbol)

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
                            "❌ SIGNAL AUTO CLOSED — STOP LOSS\n\n"
                            f"🪙 {s.symbol}\n"
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
                            "🟡 TP1 HIT\n\n"
                            f"🪙 {s.symbol}\n"
                            f"Closed: {TP1_PARTIAL_CLOSE_PCT}%\n"
                            "SL moved to Entry (BE)"
                        ),
                    )
                    continue

                # After TP1, return to Entry -> BE close
                if trade.tp1_hit and trade.sl_moved_to_be and hit_sl(s.entry):
                    trade.active = False
                    await bot.send_message(
                        chat_id=trade.user_id,
                        text=(
                            "⚪ SIGNAL AUTO CLOSED — BREAK EVEN\n\n"
                            f"🪙 {s.symbol}\n"
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
                            "✅ SIGNAL AUTO CLOSED — TP2 HIT\n\n"
                            f"🪙 {s.symbol}\n"
                            "Status: WIN 🟢"
                        ),
                    )
                    continue

            await asyncio.sleep(TRACK_INTERVAL_SECONDS)
