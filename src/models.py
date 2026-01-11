from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

class MarketType(str, Enum):
    SPOT = "SPOT"
    FUTURES = "FUTURES"

class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class SignalStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"

class PositionStatus(str, Enum):
    TRACKING = "TRACKING"   # user clicked "opened"
    CLOSED = "CLOSED"

class CloseReason(str, Enum):
    TP2 = "TP2"
    SL = "SL"
    BE = "BE"

@dataclass
class Signal:
    id: int
    symbol: str
    market: MarketType
    direction: Direction
    timeframe: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr: float
    confidence: int
    message_id: int
    created_at: str

@dataclass
class UserPosition:
    id: int
    user_id: int
    signal_id: int
    opened_at: str
    status: PositionStatus
    tp1_hit: int  # 0/1
    tp1_close_pct: int
    sl_moved_to_be: int  # 0/1
    closed_at: Optional[str]
    close_reason: Optional[str]
