from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import datetime as dt

from backend import Backend, Signal, MacroEvent

load_dotenv()

def _must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} is missing. Put it into Railway Variables or .env.")
    return v

BOT_TOKEN = _must_env("BOT_TOKEN")

ADMIN_IDS: List[int] = []
_raw_admins = os.getenv("ADMIN_IDS", "").strip()
if _raw_admins:
    for part in _raw_admins.split(","):
        part = part.strip()
        if part:
            ADMIN_IDS.append(int(part))

def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
backend = Backend()

USERS_FILE = Path("users.json")
USERS: Set[int] = set()

def load_users() -> None:
    global USERS
    if USERS_FILE.exists():
        try:
            data = json.loads(USERS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                USERS = set(int(x) for x in data)
        except Exception:
            USERS = set()

def save_users() -> None:
    try:
        USERS_FILE.write_text(json.dumps(sorted(USERS)), encoding="utf-8")
    except Exception:
        pass

SIGNALS: Dict[int, Signal] = {}

def _signal_text(s: Signal) -> str:
    header = "🟢 SPOT SIGNAL" if s.market == "SPOT" else "🔴 FUTURES SIGNAL"
    arrow = "📈 LONG" if s.direction == "LONG" else "📉 SHORT"
    risk_line = f"\n\n{s.risk_note}" if (s.risk_note or '').strip() else ""
    return (
        f"{header}\n\n"
        f"🪙 {s.symbol}\n"
        f"{arrow}\n"
        f"⏱ TF: {s.timeframe}\n\n"
        f"Entry: {s.entry:.6f}\n"
        f"SL: {s.sl:.6f}\n"
        f"TP1: {s.tp1:.6f}\n"
        f"TP2: {s.tp2:.6f}\n\n"
        f"RR: 1:{s.rr:.2f}\n"
        f"Confidence: {s.confidence}/100\n"
        f"Confirm: {s.confirmations}"
        f"{risk_line}\n\n"
        "Нажми кнопку ниже после того, как открыл сделку:"
    )

def _fmt_hhmm(ts_utc: float, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    d = dt.datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC")).astimezone(tz)
    return d.strftime("%H:%M")

async def broadcast_signal(sig: Signal) -> None:
    SIGNALS[sig.signal_id] = sig
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ОТКРЫЛ СДЕЛКУ", callback_data=f"open:{sig.signal_id}")

    for uid in list(USERS):
        try:
            await bot.send_message(uid, _signal_text(sig), reply_markup=kb.as_markup())
        except Exception:
            pass

async def broadcast_macro_alert(action: str, ev: MacroEvent, win: Tuple[float, float], tz_name: str) -> None:
    w0, w1 = win
    title = "⚠️ Macro Event Ahead"
    body = f"{ev.name}\nBlackout: {_fmt_hhmm(w0, tz_name)} – {_fmt_hhmm(w1, tz_name)}\n\n"
    tail = "Futures signals are temporarily disabled." if action == "FUTURES_OFF" else "Signals are temporarily paused."
    msg = f"{title}\n\n{body}{tail}"

    for uid in list(USERS):
        try:
            await bot.send_message(uid, msg)
        except Exception:
            pass

@dp.message(Command("start"))
async def start(message: types.Message) -> None:
    if message.from_user:
        USERS.add(message.from_user.id)
        save_users()
    await message.answer(
        "PRO Auto-Scanner Bot (2/3 multi-exchange + news + macro AUTO)\n\n"
        "✅ Ты подписан на сигналы.\n"
        "Я сканирую рынок 24/7 и фильтрую сигналы по теханализу + новости + макро-события.\n\n"
        "После кнопки ✅ ОТКРЫЛ СДЕЛКУ — сопровождение и авто-закрытие (TP1/TP2/BE/SL)."
    )

@dp.message(Command("status"))
async def status(message: types.Message) -> None:
    if message.from_user is None or not _is_admin(message.from_user.id):
        return
    ls = backend.last_signal
    await message.answer(
        f"Users: {len(USERS)}\n"
        f"TopN: {os.getenv('TOP_N','50')}\n"
        f"Last scan symbols: {backend.scanned_symbols_last}\n"
        f"News action: {backend.last_news_action}\n"
        f"Macro action: {backend.last_macro_action}\n"
        f"Last signal: {ls.symbol if ls else 'none'}"
    )

@dp.callback_query(lambda c: (c.data or '').startswith('open:'))
async def opened(call: types.CallbackQuery) -> None:
    try:
        signal_id = int((call.data or "").split(":", 1)[1])
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    sig = SIGNALS.get(signal_id)
    if not sig:
        await call.answer("Сигнал уже не доступен", show_alert=True)
        return

    backend.open_trade(call.from_user.id, sig)
    await call.answer("✅ Зафиксировано. Бот начал сопровождение.")
    try:
        await bot.send_message(call.from_user.id, f"✅ Ок. Сопровождаю {sig.symbol} ({sig.market}). Жди авто-закрытие.")
    except Exception:
        pass

async def main() -> None:
    load_users()
    asyncio.create_task(backend.track_loop(bot))
    asyncio.create_task(backend.scanner_loop(broadcast_signal, broadcast_macro_alert))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
