from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from aiogram import Bot

from ..config import Settings
from .. import db
from ..exchanges.price_feed import MockPriceFeed
from .signal_templates import render_tp1_hit, render_closed

@dataclass
class SignalRow:
    id: int
    symbol: str
    market: str
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float

async def _get_signal(signal_id: int) -> Optional[SignalRow]:
    row = await db.fetchone("SELECT id, symbol, market, direction, entry, sl, tp1, tp2 FROM signals WHERE id=?", (signal_id,))
    if not row:
        return None
    return SignalRow(
        id=int(row["id"]),
        symbol=str(row["symbol"]),
        market=str(row["market"]),
        direction=str(row["direction"]),
        entry=float(row["entry"]),
        sl=float(row["sl"]),
        tp1=float(row["tp1"]),
        tp2=float(row["tp2"]),
    )

def _hit_level(direction: str, price: float, level: float, kind: str) -> bool:
    # For LONG: TP if price >= level; SL if price <= level
    # For SHORT: TP if price <= level; SL if price >= level
    if direction == "LONG":
        return price >= level if kind == "TP" else price <= level
    return price <= level if kind == "TP" else price >= level

async def ensure_user_position(user_id: int, signal_id: int, tp1_close_pct: int) -> None:
    opened_at = datetime.now(timezone.utc).isoformat()
    try:
        await db.execute(
            """INSERT INTO user_positions(user_id, signal_id, opened_at, status, tp1_hit, tp1_close_pct, sl_moved_to_be)
               VALUES(?,?,?,?,?,?,?)""",
            (user_id, signal_id, opened_at, "TRACKING", 0, tp1_close_pct, 0),
        )
    except Exception:
        # UNIQUE(user_id, signal_id) prevents duplicates
        pass

async def tracking_loop(bot: Bot, settings: Settings, feed: MockPriceFeed) -> None:
    """Background loop:
    - iterates tracking positions
    - checks price
    - emits TP1/TP2/SL/BE updates
    """
    while True:
        try:
            rows = await db.fetchall(
                """SELECT id, user_id, signal_id, status, tp1_hit, tp1_close_pct, sl_moved_to_be
                   FROM user_positions
                   WHERE status='TRACKING'"""
            )
            for r in rows:
                pos_id = int(r["id"])
                user_id = int(r["user_id"])
                signal_id = int(r["signal_id"])
                tp1_hit = int(r["tp1_hit"])
                tp1_close_pct = int(r["tp1_close_pct"])
                sl_moved_to_be = int(r["sl_moved_to_be"])

                sig = await _get_signal(signal_id)
                if not sig:
                    continue

                tick = await feed.get_price(sig.symbol)
                price = tick.price

                # Determine active SL:
                active_sl = sig.entry if (tp1_hit == 1 and sl_moved_to_be == 1) else sig.sl

                # 1) SL (before TP1) or after TP1+BE SL is BE
                if _hit_level(sig.direction, price, active_sl, "SL"):
                    # If tp1 already hit and SL moved to BE => BE close
                    reason = "BE" if (tp1_hit == 1 and sl_moved_to_be == 1) else "SL"
                    await _close_position(bot, user_id, pos_id, sig.symbol, reason)
                    continue

                # 2) TP1
                if tp1_hit == 0 and _hit_level(sig.direction, price, sig.tp1, "TP"):
                    # mark TP1 hit and move SL to BE
                    await db.execute(
                        "UPDATE user_positions SET tp1_hit=1, sl_moved_to_be=1 WHERE id=?",
                        (pos_id,),
                    )
                    await bot.send_message(chat_id=user_id, text=render_tp1_hit(sig.symbol, tp1_close_pct, moved_to_be=True))
                    continue

                # 3) TP2 (after TP1 or directly)
                if _hit_level(sig.direction, price, sig.tp2, "TP"):
                    await _close_position(bot, user_id, pos_id, sig.symbol, "TP2")
                    continue

        except Exception as e:
            # keep loop alive
            try:
                await bot.send_message(chat_id=settings.admin_ids[0], text=f"⚠️ tracking_loop error: {e}")
            except Exception:
                pass

        await asyncio.sleep(settings.track_interval_seconds)

async def _close_position(bot: Bot, user_id: int, pos_id: int, symbol: str, reason: str) -> None:
    closed_at = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "UPDATE user_positions SET status='CLOSED', closed_at=?, close_reason=? WHERE id=?",
        (closed_at, reason, pos_id),
    )
    # Result text placeholder (you can calculate R/% precisely once you store actual fill price)
    result_text = "Result: tracked by levels"
    await bot.send_message(chat_id=user_id, text=render_closed(symbol, reason, result_text))
