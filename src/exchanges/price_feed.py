from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Dict

@dataclass
class PriceTick:
    symbol: str
    price: float
    ts: float

class MockPriceFeed:
    """Demo price feed (random walk).
    Replace this with real exchange WebSocket/REST.
    """

    def __init__(self) -> None:
        self._prices: Dict[str, float] = {}

    def set_start_price(self, symbol: str, price: float) -> None:
        self._prices[symbol.upper()] = float(price)

    async def get_price(self, symbol: str) -> PriceTick:
        sym = symbol.upper()
        p = self._prices.get(sym, 0.0)
        if p <= 0:
            # start from random baseline if unknown
            p = random.uniform(10, 50000)
        # random walk step
        step = random.uniform(-0.004, 0.004)  # +/-0.4%
        p = max(0.0000001, p * (1.0 + step))
        self._prices[sym] = p
        return PriceTick(symbol=sym, price=p, ts=time.time())
