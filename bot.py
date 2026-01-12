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
ORIGINAL_SIGNAL_TEXT: Dict[int, str] = {}

# ---------------- signal stats (daily/weekly) ----------------
STATS_FILE = Path("signal_stats.json")
SIGNAL_STATS: Dict[str, Dict[str, int]] = {"days": {}, "weeks": {}}

def load_signal_stats() -> None:
    global SIGNAL_STATS
    if STATS_FILE.exists():
        try:
            data = json.loads(STATS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                days = data.get("days") or {}
                weeks = data.get("weeks") or {}
                if isinstance(days, dict) and isinstance(weeks, dict):
                    SIGNAL_STATS = {
                        "days": {str(k): int(v) for k, v in days.items()},
                        "weeks": {str(k): int(v) for k, v in weeks.items()},
                    }
        except Exception:
            SIGNAL_STATS = {"days": {}, "weeks": {}}

def save_signal_stats() -> None:
    try:
        STATS_FILE.write_text(json.dumps(SIGNAL_STATS, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    except Exception:
        pass

def _day_key(d: dt.date) -> str:
    return d.isoformat()

def _week_key(d: dt.date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{int(w):02d}"

def bump_signal_stats() -> None:
    # Counts are global (for all users) and respect TZ_NAME
    now = dt.datetime.now(TZ)
    dk = _day_key(now.date())
    wk = _week_key(now.date())
    SIGNAL_STATS["days"][dk] = int(SIGNAL_STATS["days"].get(dk, 0)) + 1
    SIGNAL_STATS["weeks"][wk] = int(SIGNAL_STATS["weeks"].get(wk, 0)) + 1
    save_signal_stats()

def _signals_today() -> int:
    dk = _day_key(dt.datetime.now(TZ).date())
    return int(SIGNAL_STATS["days"].get(dk, 0))

def _signals_this_week() -> int:
    wk = _week_key(dt.datetime.now(TZ).date())
    return int(SIGNAL_STATS["weeks"].get(wk, 0))

def _daily_report_lines(days: int = 7) -> list[str]:
    today = dt.datetime.now(TZ).date()
    out: list[str] = []
    for i in range(days - 1, -1, -1):
        d = today - dt.timedelta(days=i)
        k = _day_key(d)
        cnt = int(SIGNAL_STATS["days"].get(k, 0))
        out.append(f"{k}: {cnt}")
    return out

def _weekly_report_lines(weeks: int = 4) -> list[str]:
    today = dt.datetime.now(TZ).date()
    # go back (weeks-1) weeks and build iso week keys
    out: list[str] = []
    seen = set()
    for i in range(weeks - 1, -1, -1):
        d = today - dt.timedelta(days=7*i)
        k = _week_key(d)
        if k in seen:
            continue
        seen.add(k)
        cnt = int(SIGNAL_STATS["weeks"].get(k, 0))
        out.append(f"{k}: {cnt}")
    return out

# Status auto-refresh (per user)
STATUS_TASKS: Dict[int, asyncio.Task] = {}


async def _send_long(chat_id: int, text: str, reply_markup=None) -> None:
    # Telegram message limit ~4096 chars. Send in chunks if needed.
    max_len = 3800
    if len(text) <= max_len:
        await bot.send_message(chat_id, text, reply_markup=reply_markup)
        return
    parts = []
    cur = ""
    for line in text.splitlines(True):
        if len(cur) + len(line) > max_len and cur:
            parts.append(cur)
            cur = ""
        cur += line
    if cur:
        parts.append(cur)

    for i, part in enumerate(parts):
        await bot.send_message(chat_id, part, reply_markup=reply_markup if i == len(parts)-1 else None)

def _fmt_perf(b: dict) -> str:
    trades = int(b.get("trades", 0))
    wins = int(b.get("wins", 0))
    losses = int(b.get("losses", 0))
    be = int(b.get("be", 0))
    tp1 = int(b.get("tp1_hits", 0))
    pnl = float(b.get("sum_pnl_pct", 0.0))
    wr = (wins / trades * 100.0) if trades else 0.0
    return f"Trades: {trades} | Wins: {wins} | Losses: {losses} | BE: {be} | TP1: {tp1}\nWinrate: {wr:.1f}%\nPnL: {pnl:+.2f}%"

def _build_status_text() -> str:
    next_macro = backend.get_next_macro()
    macro_line = "Next macro: none"

    macro_action = backend.last_macro_action
    macro_prefix = "🟢" if macro_action == "ALLOW" else "🔴"

    if next_macro:
        ev, (w0, w1) = next_macro
        secs = w0 - time.time()
        next_prefix = "🟡" if macro_action == "ALLOW" else "🔴"
        macro_line = f"{next_prefix} Next macro: {ev.name} | Blackout {_fmt_hhmm(w0)}–{_fmt_hhmm(w1)} | in {_fmt_countdown(secs)}"

    # performance (separate Spot / Futures)
    spot_today = backend.perf_today("SPOT")
    fut_today = backend.perf_today("FUTURES")
    spot_week = backend.perf_week("SPOT")
    fut_week = backend.perf_week("FUTURES")

    scan_line = "Scanner status: RUNNING 🟢"

    txt = (
        f"{scan_line}\n"
        f"News action: {backend.last_news_action}\n"
        f"{macro_prefix} Macro action: {macro_action}\n"
        f"{macro_line}\n\n"
        f"📊 Performance — Today\n"
        f"🟢 SPOT\n{_fmt_perf(spot_today)}\n\n"
        f"🔴 FUTURES\n{_fmt_perf(fut_today)}\n\n"
        f"📊 Performance — This week\n"
        f"🟢 SPOT\n{_fmt_perf(spot_week)}\n\n"
        f"🔴 FUTURES\n{_fmt_perf(fut_week)}\n\n"
    )
    return txt

async def _status_autorefresh(uid: int, chat_id: int, message_id: int, seconds: int = 120, interval: int = 5) -> None:
    # Refresh countdown + macro status for a short window to avoid spam/rate limits
    end = time.time() + max(10, seconds)
    last_txt: str | None = None
    while time.time() < end:
        await asyncio.sleep(interval)
        try:
            txt = _build_status_text()
            if txt == last_txt:
                continue
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=txt, reply_markup=menu_kb())
            last_txt = txt
        except Exception:
            return

# ---------------- UI helpers ----------------

def menu_kb() -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Status", callback_data="menu:status")
    kb.button(text="📈 Stats", callback_data="menu:stats")
    kb.button(text="🟢 Spot live", callback_data="menu:spot")
    kb.button(text="🔴 Futures live", callback_data="menu:futures")
    kb.button(text="📂 My trades", callback_data="trades:page:0")
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

def _trade_status_emoji(status: str) -> str:
    return {
        "ACTIVE": "⏳",
        "TP1": "🟡",
        "WIN": "🟢",
        "LOSS": "🔴",
        "BE": "⚪",
        "CLOSED": "✅",
    }.get(status, "⏳")

# ---------------- broadcasting ----------------
async def broadcast_signal(sig: Signal) -> None:
    SIGNALS[sig.signal_id] = sig
    ORIGINAL_SIGNAL_TEXT[sig.signal_id] = _signal_text(sig)
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
    body = f"{ev.name}\nBlackout: {_fmt_hhmm(w0)} – {_fmt_hhmm(w1)}\n\n"
    tail = "Futures signals are temporarily disabled." if action == "FUTURES_OFF" else "Signals are temporarily paused."
    msg = f"{title}\n\n{body}{tail}"
    for uid in list(USERS):
        try:
            await bot.send_message(uid, msg)
        except Exception:
            pass

# ---------------- commands ----------------
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

# ---------------- menu callbacks ----------------
@dp.callback_query(lambda c: (c.data or "").startswith("menu:"))
async def menu_handler(call: types.CallbackQuery) -> None:
    action = (call.data or "").split(":", 1)[1]
    await call.answer()

    if action == "status":
        # cancel previous auto-refresh task (if any)
        uid = call.from_user.id if call.from_user else 0
        t = STATUS_TASKS.pop(uid, None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass

        try:
            txt = _build_status_text()
        except Exception as e:
            txt = f"Status error: {e}"
        if call.from_user and _is_admin(call.from_user.id) and backend.last_signal:
            ls = backend.last_signal
            txt += f"\nLast signal: {ls.symbol} {ls.market} {ls.direction} conf={ls.confidence}"

        # prefer editing the current menu message (better UX)
        try:
            if call.message:
                await bot.edit_message_text(chat_id=call.from_user.id, message_id=call.message.message_id, text=txt, reply_markup=menu_kb())
                msg = call.message
            else:
                msg = await bot.send_message(call.from_user.id, txt, reply_markup=menu_kb())
        except Exception:
            msg = await bot.send_message(call.from_user.id, txt, reply_markup=menu_kb())

        # auto-refresh countdown for a short window
        task = asyncio.create_task(_status_autorefresh(uid, msg.chat.id, msg.message_id))
        STATUS_TASKS[uid] = task
        return

    if action == "stats":
        # cancel previous auto-refresh task (if any)
        uid = call.from_user.id if call.from_user else 0
        t = STATUS_TASKS.pop(uid, None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass

        try:
            spot_daily = backend.report_daily("SPOT", 7)
            fut_daily = backend.report_daily("FUTURES", 7)
            spot_weekly = backend.report_weekly("SPOT", 4)
            fut_weekly = backend.report_weekly("FUTURES", 4)
        except Exception:
            spot_daily, fut_daily, spot_weekly, fut_weekly = [], [], [], []

        parts = []
        parts.append("📈 Trading statistics")
        parts.append("")
        parts.append("📅 Daily (last 7d)")
        parts.append("🟢 SPOT:")
        parts.append("\n".join(spot_daily) if spot_daily else "no data")
        parts.append("")
        parts.append("🔴 FUTURES:")
        parts.append("\n".join(fut_daily) if fut_daily else "no data")
        parts.append("")
        parts.append("🗓️ Weekly (last 4w)")
        parts.append("🟢 SPOT:")
        parts.append("\n".join(spot_weekly) if spot_weekly else "no data")
        parts.append("")
        parts.append("🔴 FUTURES:")
        parts.append("\n".join(fut_weekly) if fut_weekly else "no data")
        txt = "\n".join(parts)

        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Refresh", callback_data="menu:stats")
        kb.button(text="📊 Status", callback_data="menu:status")
        kb.button(text="🏠 Menu", callback_data="menu:status")
        kb.adjust(2, 1)

        # Prefer editing the same message (avoid duplicates)
        try:
            if call.message and len(txt) < 3800:
                await bot.edit_message_text(chat_id=call.from_user.id, message_id=call.message.message_id, text=txt, reply_markup=kb.as_markup())
            else:
                await _send_long(call.from_user.id, txt, reply_markup=kb.as_markup())
        except Exception:
            await _send_long(call.from_user.id, txt, reply_markup=kb.as_markup())
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

# ---------------- trades list (with buttons) ----------------
PAGE_SIZE = 10

def _trades_page_kb(trades: List, offset: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for t in trades:
        s = t.signal
        label = f"{_trade_status_emoji(t.result)} {s.symbol} {s.market} ({t.result})"
        kb.button(text=label, callback_data=f"trade:view:{s.signal_id}")
    # pagination
    nav = InlineKeyboardBuilder()
    if offset > 0:
        nav.button(text="⬅ Prev", callback_data=f"trades:page:{max(0, offset-PAGE_SIZE)}")
    if len(trades) == PAGE_SIZE:
        nav.button(text="Next ➡", callback_data=f"trades:page:{offset+PAGE_SIZE}")
    if nav.buttons:
        kb.adjust(1)
        kb.row(*[b for b in nav.buttons])
    kb.row(types.InlineKeyboardButton(text="🏠 Menu", callback_data="menu:status"))
    return kb.as_markup()

@dp.callback_query(lambda c: (c.data or "").startswith("trades:page:"))
async def trades_page(call: types.CallbackQuery) -> None:
    await call.answer()
    try:
        offset = int((call.data or "").split(":")[-1])
    except Exception:
        offset = 0

    all_trades = backend.get_user_trades(call.from_user.id)
    if not all_trades:
        await bot.send_message(call.from_user.id, "You have no opened trades yet. Open a signal first ✅", reply_markup=menu_kb())
        return

    page = all_trades[offset:offset+PAGE_SIZE]
    txt = f"📂 My trades (showing {offset+1}-{min(offset+PAGE_SIZE, len(all_trades))} of {len(all_trades)})\nTap a trade to open details."
    await bot.send_message(call.from_user.id, txt, reply_markup=_trades_page_kb(page, offset))

# ---------------- trade card ----------------
def _trade_card_text(t) -> str:
    s = t.signal
    last_price = f"{t.last_price:.6f}" if getattr(t, "last_price", 0.0) else "-"
    parts = [
        "📌 Trade",
        "",
        f"🪙 {s.symbol} | {s.market} | {s.direction}",
        f"TF: {s.timeframe}",
        "",
        f"Entry: {s.entry:.6f}",
        f"SL: {s.sl:.6f}",
        f"TP1: {s.tp1:.6f}",
        f"TP2: {s.tp2:.6f}",
        "",
        f"Status: {t.result} {_trade_status_emoji(t.result)}",
        f"Last price: {last_price}",
        "",
        "Buttons:",
        "• 🔄 Refresh",
        "• 📌 Show original signal",
        "• ✅ I CLOSED (remove)",
    ]
    return "\n".join(parts)

def _trade_card_kb(signal_id: int, back_offset: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Refresh", callback_data=f"trade:refresh:{signal_id}:{back_offset}")
    kb.button(text="📌 Show original signal", callback_data=f"trade:orig:{signal_id}:{back_offset}")
    kb.button(text="✅ I CLOSED (remove)", callback_data=f"trade:close:{signal_id}:{back_offset}")
    kb.button(text="⬅ Back", callback_data=f"trades:page:{back_offset}")
    kb.adjust(2, 2)
    return kb.as_markup()

@dp.callback_query(lambda c: (c.data or "").startswith("trade:view:"))
async def trade_view(call: types.CallbackQuery) -> None:
    await call.answer()
    try:
        signal_id = int((call.data or "").split(":")[-1])
    except Exception:
        return
    t = backend.get_trade(call.from_user.id, signal_id)
    if not t:
        await bot.send_message(call.from_user.id, "Trade not found (maybe already closed).", reply_markup=menu_kb())
        return
    await bot.send_message(call.from_user.id, _trade_card_text(t), reply_markup=_trade_card_kb(signal_id, 0))

@dp.callback_query(lambda c: (c.data or "").startswith("trade:refresh:"))
async def trade_refresh(call: types.CallbackQuery) -> None:
    await call.answer()
    parts = (call.data or "").split(":")
    try:
        signal_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return
    t = backend.get_trade(call.from_user.id, signal_id)
    if not t:
        await bot.send_message(call.from_user.id, "Trade not found (maybe already closed).", reply_markup=menu_kb())
        return
    await bot.send_message(call.from_user.id, _trade_card_text(t), reply_markup=_trade_card_kb(signal_id, back_offset))

@dp.callback_query(lambda c: (c.data or "").startswith("trade:close:"))
async def trade_close(call: types.CallbackQuery) -> None:
    await call.answer()
    parts = (call.data or "").split(":")
    try:
        signal_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return
    removed = backend.remove_trade(call.from_user.id, signal_id)
    if removed:
        await bot.send_message(call.from_user.id, "✅ Removed from My trades.", reply_markup=menu_kb())
    else:
        await bot.send_message(call.from_user.id, "Trade not found.", reply_markup=menu_kb())

@dp.callback_query(lambda c: (c.data or "").startswith("trade:orig:"))
async def trade_orig(call: types.CallbackQuery) -> None:
    await call.answer()
    parts = (call.data or "").split(":")
    try:
        signal_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return

    text = ORIGINAL_SIGNAL_TEXT.get(signal_id)
    if not text:
        await bot.send_message(call.from_user.id, "Original signal is not available (maybe very old).", reply_markup=menu_kb())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="⬅ Back to trade", callback_data=f"trade:view:{signal_id}")
    kb.button(text="📂 My trades", callback_data=f"trades:page:{back_offset}")
    kb.button(text="🏠 Menu", callback_data="menu:status")
    kb.adjust(1, 2)

    await bot.send_message(call.from_user.id, "📌 Original signal (1:1)\n\n" + text, reply_markup=kb.as_markup())

# ---------------- open signal ----------------
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
    await bot.send_message(call.from_user.id, f"✅ Trade opened: {sig.symbol} ({sig.market}). Check 📂 My trades.", reply_markup=menu_kb())

async def main() -> None:
    import time
    globals()["time"] = time

    load_users()
    asyncio.create_task(backend.track_loop(bot))
    asyncio.create_task(backend.scanner_loop(broadcast_signal, broadcast_macro_alert))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
