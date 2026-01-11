from __future__ import annotations

import asyncio
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

from backend import Backend, Signal

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Put it into .env (local) or Railway Variables.")

# Optionally control demo signal market (SPOT/FUTURES) via env BINANCE_MARKET
DEMO_MARKET = os.getenv("BINANCE_MARKET", "FUTURES").strip().upper()

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
backend = Backend()

# Demo signal (replace with real signal generator later)
DEMO_SIGNAL = Signal(
    signal_id=1,
    symbol="BTCUSDT",
    direction="SHORT",
    entry=42300,
    sl=42900,
    tp1=41500,
    tp2=40800,
    market=DEMO_MARKET,
)

@dp.message(Command("start"))
async def start(message: types.Message) -> None:
    await message.answer(
        "VIP Signals Bot (MVP)\n\n"
        "Commands:\n"
        "• /signal — post a demo signal with button\n\n"
        "Tap ✅ ОТКРЫЛ СДЕЛКУ to start tracking and get AUTO CLOSED updates."
    )

@dp.message(Command("signal"))
async def send_signal(message: types.Message) -> None:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ОТКРЫЛ СДЕЛКУ", callback_data=f"open:{DEMO_SIGNAL.signal_id}")

    await message.answer(
        "🔴 FUTURES SIGNAL\n\n"
        f"🪙 {DEMO_SIGNAL.symbol}\n"
        f"📉 {DEMO_SIGNAL.direction}\n"
        f"Market: {DEMO_SIGNAL.market}\n\n"
        f"Entry: {DEMO_SIGNAL.entry}\n"
        f"SL: {DEMO_SIGNAL.sl}\n"
        f"TP1: {DEMO_SIGNAL.tp1}\n"
        f"TP2: {DEMO_SIGNAL.tp2}\n\n"
        "Нажми кнопку ниже после того, как открыл сделку:",
        reply_markup=kb.as_markup(),
    )

@dp.callback_query(lambda c: (c.data or "").startswith("open:"))
async def opened(call: types.CallbackQuery) -> None:
    backend.open_trade(call.from_user.id, DEMO_SIGNAL)

    await call.answer("✅ Зафиксировано. Бот начал сопровождение.")
    try:
        await bot.send_message(
            chat_id=call.from_user.id,
            text="✅ Сделка зафиксирована. Я начну сопровождение и сообщу авто-закрытие (TP1/TP2/BE/SL).",
        )
    except Exception:
        pass

async def main() -> None:
    asyncio.create_task(backend.track_loop(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
