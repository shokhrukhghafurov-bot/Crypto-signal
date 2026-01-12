from __future__ import annotations

import asyncio
import os
from typing import Dict, List

import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

from backend import Backend, Signal

load_dotenv()

def _must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} is missing. Put it into .env (local) or Railway Variables.")
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

DB_PATH = "bot.db"

SIGNALS: Dict[int, Signal] = {}
NEXT_SIGNAL_ID = 1

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS vip_users (user_id INTEGER PRIMARY KEY)")
        await db.commit()

async def vip_add(user_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO vip_users(user_id) VALUES(?)", (user_id,))
        await db.commit()

async def vip_del(user_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM vip_users WHERE user_id=?", (user_id,))
        await db.commit()

async def vip_list() -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM vip_users ORDER BY user_id")
        rows = await cur.fetchall()
        return [int(r[0]) for r in rows]

def _parse_float(x: str) -> float:
    return float(x.replace(" ", "").replace(",", "."))

def _signal_text(s: Signal) -> str:
    header = "🟢 SPOT SIGNAL" if s.market == "SPOT" else "🔴 FUTURES SIGNAL"
    arrow = "📈 LONG" if s.direction == "LONG" else "📉 SHORT"
    return (
        f"{header}\n\n"
        f"🪙 {s.symbol}\n"
        f"{arrow}\n"
        f"Market: {s.market}\n\n"
        f"Entry: {s.entry}\n"
        f"SL: {s.sl}\n"
        f"TP1: {s.tp1}\n"
        f"TP2: {s.tp2}\n\n"
        "Нажми кнопку ниже после того, как открыл сделку:"
    )

@dp.message(Command("start"))
async def start(message: types.Message) -> None:
    await message.answer(
        "Universal Signals Bot\n\n"
        "Сигналы приходят в личку (VIP only).\n"
        "После кнопки ✅ ОТКРЫЛ СДЕЛКУ я сопровождаю сделку и пришлю авто-закрытие (TP1/TP2/BE/SL)."
    )

@dp.message(Command("vip_add"))
async def cmd_vip_add(message: types.Message) -> None:
    if message.from_user is None or not _is_admin(message.from_user.id):
        await message.answer("⛔️ Только админ.")
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Формат: /vip_add <user_id>")
        return
    uid = int(parts[1])
    await vip_add(uid)
    await message.answer(f"✅ Добавлен VIP: {uid}")

@dp.message(Command("vip_del"))
async def cmd_vip_del(message: types.Message) -> None:
    if message.from_user is None or not _is_admin(message.from_user.id):
        await message.answer("⛔️ Только админ.")
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Формат: /vip_del <user_id>")
        return
    uid = int(parts[1])
    await vip_del(uid)
    await message.answer(f"✅ Удалён VIP: {uid}")

@dp.message(Command("vip_list"))
async def cmd_vip_list(message: types.Message) -> None:
    if message.from_user is None or not _is_admin(message.from_user.id):
        await message.answer("⛔️ Только админ.")
        return
    users = await vip_list()
    await message.answer("VIP users:\n" + ("\n".join(str(u) for u in users) if users else "(empty)"))

@dp.message(Command("signal"))
async def signal_cmd(message: types.Message) -> None:
    if message.from_user is None or not _is_admin(message.from_user.id):
        await message.answer("⛔️ Команда доступна только админам.")
        return

    parts = (message.text or "").split()
    if len(parts) != 8:
        await message.answer(
            "Формат:\n"
            "/signal <SPOT|FUTURES> <SYMBOL> <LONG|SHORT> <ENTRY> <SL> <TP1> <TP2>\n\n"
            "Пример:\n"
            "/signal FUTURES BTCUSDT SHORT 42300 42900 41500 40800"
        )
        return

    global NEXT_SIGNAL_ID
    _, market, symbol, direction, entry, sl, tp1, tp2 = parts

    market = market.upper()
    direction = direction.upper()
    symbol = symbol.upper()

    if market not in ("SPOT", "FUTURES"):
        await message.answer("market должен быть SPOT или FUTURES")
        return
    if direction not in ("LONG", "SHORT"):
        await message.answer("direction должен быть LONG или SHORT")
        return

    s = Signal(
        signal_id=NEXT_SIGNAL_ID,
        market=market,
        symbol=symbol,
        direction=direction,
        entry=_parse_float(entry),
        sl=_parse_float(sl),
        tp1=_parse_float(tp1),
        tp2=_parse_float(tp2),
    )
    SIGNALS[s.signal_id] = s
    NEXT_SIGNAL_ID += 1

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ОТКРЫЛ СДЕЛКУ", callback_data=f"open:{s.signal_id}")

    vip_users = await vip_list()
    if not vip_users:
        await message.answer("⚠️ VIP список пуст. Добавь пользователей через /vip_add <user_id>.")
        return

    sent = 0
    for uid in vip_users:
        try:
            await bot.send_message(uid, _signal_text(s), reply_markup=kb.as_markup())
            sent += 1
        except Exception:
            pass

    await message.answer(f"✅ Сигнал разослан VIP: {sent}/{len(vip_users)}. signal_id={s.signal_id}")

@dp.callback_query(lambda c: (c.data or "").startswith("open:"))
async def opened(call: types.CallbackQuery) -> None:
    try:
        signal_id = int((call.data or "").split(":", 1)[1])
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    s = SIGNALS.get(signal_id)
    if not s:
        await call.answer("Сигнал не найден", show_alert=True)
        return

    backend.open_trade(call.from_user.id, s)
    await call.answer("✅ Зафиксировано. Бот начал сопровождение.")
    await bot.send_message(call.from_user.id, f"✅ Ок. Сопровождаю {s.symbol} ({s.market}). Жди авто-закрытие.")

async def main() -> None:
    await init_db()
    asyncio.create_task(backend.track_loop(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
