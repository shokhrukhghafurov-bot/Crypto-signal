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

TZ_NAME = os.getenv("TZ_NAME", "Europe/Berlin").strip() or "Europe/Berlin"
TZ = ZoneInfo(TZ_NAME)

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

def menu_kb() -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Status", callback_data="menu:status")
    kb.button(text="🟢 Spot live", callback_data="menu:spot")
    kb.button(text="🔴 Futures live", callback_data="menu:futures")
    kb.button(text="📂 My trades", callback_data="menu:trades")
    kb.adjust(2, 2)
    return kb.as_markup()

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

def _fmt_hhmm(ts_utc: float) -> str:
    d = dt.datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC")).astimezone(TZ)
    return d.strftime("%H:%M")

def _fmt_countdown(seconds: float) -> str:
    if seconds < 0:
        seconds = 0
    m = int(seconds // 60)
    h = m // 60
    m = m % 60
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"

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
    # (tz_name comes from backend; we format with our TZ too)
    w0, w1 = win
    title = "⚠️ Macro Event Ahead"
    body = f"{ev.name}\nBlackout: {_fmt_hhmm(w0)} – {_fmt_hhmm(w1)}\n\n"
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
        "PRO Auto-Scanner Bot\n\n"
        "✅ Ты подписан на сигналы.\n"
        "Нажимай кнопки ниже:",
        reply_markup=menu_kb(),
    )

@dp.callback_query(lambda c: (c.data or "").startswith("menu:"))
async def menu_handler(call: types.CallbackQuery) -> None:
    action = (call.data or "").split(":", 1)[1]
    await call.answer()

    if action == "status":
        next_macro = backend.get_next_macro()
        macro_line = "Next macro: none"
        if next_macro:
            ev, (w0, w1) = next_macro
            secs = w0 - time.time()  # type: ignore[name-defined]
            macro_line = f"Next macro: {ev.name} | Blackout {_fmt_hhmm(w0)}–{_fmt_hhmm(w1)} | in {_fmt_countdown(secs)}"

        txt = (
            f"Users: {len(USERS)}\n"
            f"TopN: {os.getenv('TOP_N','50')}\n"
            f"Scan interval: {os.getenv('SCAN_INTERVAL_SECONDS','150')}s\n"
            f"News action: {backend.last_news_action}\n"
            f"Macro action: {backend.last_macro_action}\n"
            f"{macro_line}"
        )
        if call.from_user and _is_admin(call.from_user.id):
            ls = backend.last_signal
            if ls:
                txt += f"\nLast signal: {ls.symbol} {ls.market} {ls.direction} conf={ls.confidence}"
        await bot.send_message(call.from_user.id, txt, reply_markup=menu_kb())
        return

    if action in ("spot", "futures"):
        sig = backend.last_spot_signal if action == "spot" else backend.last_futures_signal
        if not sig:
            await bot.send_message(call.from_user.id, "No live signal yet. Wait for scanner.", reply_markup=menu_kb())
            return
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ ОТКРЫЛ СДЕЛКУ", callback_data=f"open:{sig.signal_id}")
        await bot.send_message(call.from_user.id, _signal_text(sig), reply_markup=kb.as_markup())
        return

    if action == "trades":
        trades = backend.get_user_trades(call.from_user.id)
        if not trades:
            await bot.send_message(call.from_user.id, "You have no opened trades yet. Open a signal first ✅", reply_markup=menu_kb())
            return
        lines = []
        for t in sorted(trades, key=lambda x: x.signal.ts, reverse=True)[:20]:
            s = t.signal
            status = t.result
            price = f"{t.last_price:.6f}" if t.last_price else "-"
            lines.append(f"• {s.symbol} {s.market} {s.direction} | status={status} | last={price}")
        await bot.send_message(call.from_user.id, "📂 My trades\n\n" + "\n".join(lines), reply_markup=menu_kb())
        return

@dp.callback_query(lambda c: (c.data or "").startswith("open:"))
async def opened(call: types.CallbackQuery) -> None:
    try:
        signal_id = int((call.data or "").split(":", 1)[1])
    except Exception:
        await call.answer("Error", show_alert=True)
        return

    sig = SIGNALS.get(signal_id)
    if not sig:
        await call.answer("Signal not available", show_alert=True)
        return

    backend.open_trade(call.from_user.id, sig)
    await call.answer("✅ Opened. Tracking started.")
    await bot.send_message(call.from_user.id, f"✅ Trade opened: {sig.symbol} ({sig.market}). Use 📂 My trades to see status.", reply_markup=menu_kb())

async def main() -> None:
    import time  # used in menu handler countdown
    globals()["time"] = time

    load_users()
    asyncio.create_task(backend.track_loop(bot))
    asyncio.create_task(backend.scanner_loop(broadcast_signal, broadcast_macro_alert))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
