from __future__ import annotations

from datetime import datetime, timezone

from aiogram import Bot

from .. import db
from ..keyboards import opened_keyboard
from .signal_templates import render_signal_text

async def create_and_post_signal(
    bot: Bot,
    vip_channel_id: int,
    *,
    symbol: str,
    market: str,
    direction: str,
    timeframe: str,
    entry: float,
    sl: float,
    tp1: float,
    tp2: float,
    rr: float,
    confidence: int,
) -> int:
    text = render_signal_text(
        market=market,
        symbol=symbol,
        direction=direction,
        timeframe=timeframe,
        entry=entry,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        rr=rr,
        confidence=confidence,
    )
    msg = await bot.send_message(
        chat_id=vip_channel_id,
        text=text,
        reply_markup=opened_keyboard(signal_id=0),  # placeholder
        disable_web_page_preview=True,
    )

    created_at = datetime.now(timezone.utc).isoformat()

    signal_id = await db.execute_returning_id(
        """INSERT INTO signals(symbol, market, direction, timeframe, entry, sl, tp1, tp2, rr, confidence, message_id, created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (symbol.upper(), market, direction, timeframe, entry, sl, tp1, tp2, rr, confidence, msg.message_id, created_at),
    )

    # Update keyboard with real signal_id
    await bot.edit_message_reply_markup(
        chat_id=vip_channel_id,
        message_id=msg.message_id,
        reply_markup=opened_keyboard(signal_id=signal_id),
    )

    return signal_id
