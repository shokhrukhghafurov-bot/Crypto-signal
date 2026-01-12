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


LANG_FILE = Path("langs.json")
LANG: Dict[int, str] = {}  # user_id -> "ru" | "en"

def load_langs() -> None:
    global LANG
    if LANG_FILE.exists():
        try:
            data = json.loads(LANG_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                LANG = {int(k): str(v) for k, v in data.items()}
        except Exception:
            LANG = {}

def save_langs() -> None:
    try:
        LANG_FILE.write_text(json.dumps({str(k): v for k, v in LANG.items()}, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    except Exception:
        pass

def get_lang(uid: int) -> str:
    v = (LANG.get(uid) or "").lower().strip()
    return "en" if v == "en" else "ru"

def set_lang(uid: int, lang: str) -> None:
    LANG[uid] = "en" if (lang or "").lower().startswith("en") else "ru"
    save_langs()

I18N = {
    "ru": {
        "val_paused": "ПАУЗА",
        "val_block": "ЗАПРЕЩЕНО",
        "val_allow": "РАЗРЕШЕНО",
        "lbl_trades": "Сделки",
        "lbl_wins": "Плюс",
        "lbl_losses": "Минус",
        "lbl_be": "BE",
        "lbl_tp1": "TP1",
        "lbl_winrate": "Процент побед",
        "lbl_pnl": "PnL",
        "lbl_spot": "СПОТ",
        "lbl_futures": "ФЬЮЧЕРСЫ",
        "lbl_blackout": "Блэкаут",
        "lbl_in": "через",
        "lbl_none": "нет",
        "scanner_run": "Сканер: работает 🟢",
        "news_action": "Новости: {v}",
        "macro_action": "Макро: {v}",
        "next_macro": "Следующее макро: {v}",
        "perf_today": "📊 Результаты — Сегодня",
        "perf_week": "📊 Результаты — На этой неделе",
        "daily_title": "📅 По дням (7д)",
        "weekly_title": "🗓️ По неделям (4н)",
        "choose_lang": "🌐 Выбери язык / Choose language:",
        "btn_ru": "🇷🇺 Русский",
        "btn_en": "🇬🇧 English",
        "welcome": "PRO Auto-Scanner Bot\n\n✅ Ты подписан на сигналы.\nНажимай кнопки ниже:",
        "m_status": "📊 Статус",
        "m_stats": "📈 Статистика",
        "m_spot": "🟢 Спот live",
        "m_fut": "🔴 Фьючерсы live",
        "m_trades": "📂 Мои сделки",
        "refresh": "🔄 Обновить",
        "back": "⬅️ Назад",
        "no_live": "Пока нет live-сигнала. Ждём сканер.",
        "stats_title": "📈 Торговая статистика",
        "no_closed": "нет закрытых сделок",
        "tip_closed": "Подсказка: статистика появляется только после закрытия сделки (TP2 / SL / BE / вручную).",
    },
    "en": {
        "trade_last_price": "Last price",
        "trade_status": "Status",
        "mytrades_of": "of",
        "mytrades_showing": "showing",
        "lbl_trades": "Trades",
        "lbl_wins": "Wins",
        "lbl_losses": "Losses",
        "lbl_be": "BE",
        "lbl_tp1": "TP1",
        "lbl_winrate": "Win rate",
        "lbl_pnl": "PnL",
        "lbl_spot": "SPOT",
        "lbl_futures": "FUTURES",
        "lbl_blackout": "Blackout",
        "lbl_in": "in",
        "lbl_none": "none",
        "scanner_run": "Scanner status: RUNNING 🟢",
        "news_action": "News action: {v}",
        "macro_action": "Macro action: {v}",
        "next_macro": "Next macro: {v}",
        "perf_today": "📊 Performance — Today",
        "perf_week": "📊 Performance — This week",
        "daily_title": "📅 Daily (last 7d)",
        "weekly_title": "🗓️ Weekly (last 4w)",
        "choose_lang": "🌐 Choose language:",
        "btn_ru": "🇷🇺 Русский",
        "btn_en": "🇬🇧 English",
        "welcome": "PRO Auto-Scanner Bot\n\n✅ You are subscribed to signals.\nUse the buttons below:",
        "m_status": "📊 Status",
        "m_stats": "📈 Stats",
        "m_spot": "🟢 Spot live",
        "m_fut": "🔴 Futures live",
        "m_trades": "📂 My trades",
        "refresh": "🔄 Refresh",
        "back": "⬅️ Back",
        "no_live": "No live signal yet. Wait for scanner.",
        "stats_title": "📈 Trading statistics",
        "no_closed": "no closed trades",
        "tip_closed": "Tip: stats appear only after a trade is CLOSED (TP2 / SL / BE / manual).",
    }
}

def tr(uid: int, key: str) -> str:
    lang = get_lang(uid)
    return I18N.get(lang, I18N["en"]).get(key, I18N["en"].get(key, key))


def _val(uid: int, v: str) -> str:
    vv = (v or '').strip().upper()
    if vv == 'ALLOW':
        return tr(uid, 'val_allow')
    if vv in ('BLOCK', 'DENY', 'DISALLOW'):
        return tr(uid, 'val_block')
    if vv in ('PAUSE_ALL', 'PAUSED', 'FUTURES_OFF'):
        return tr(uid, 'val_paused')
    return v

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

CURRENT_VIEW: dict[int, str] = {}

def _nav_enter(user_id: int, view: str) -> None:
    prev = CURRENT_VIEW.get(user_id)
    if view != "back":
        if prev and prev != view:
            PREV_VIEW[user_id] = prev
        CURRENT_VIEW[user_id] = view

def _nav_back(user_id: int, default: str = "status") -> str:
    return PREV_VIEW.get(user_id) or default

PREV_VIEW: dict[int, str] = {}

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


async def _render_in_place(call: types.CallbackQuery, txt: str, kb: types.InlineKeyboardMarkup) -> types.Message:
    """Prefer editing the message that contains the pressed button."""
    try:
        if call.message:
            await bot.edit_message_text(chat_id=call.from_user.id, message_id=call.message.message_id, text=txt, reply_markup=kb)
            return call.message
    except Exception:
        pass
    return await bot.send_message(call.from_user.id, txt, reply_markup=kb)

    for i, part in enumerate(parts):
        await bot.send_message(chat_id, part, reply_markup=reply_markup if i == len(parts)-1 else None)

def _fmt_perf(uid: int, b: dict) -> str:
    trades = int(b.get("trades", 0))
    wins = int(b.get("wins", 0))
    losses = int(b.get("losses", 0))
    be = int(b.get("be", 0))
    tp1 = int(b.get("tp1_hits", 0))
    pnl = float(b.get("sum_pnl_pct", 0.0))
    wr = (wins / trades * 100.0) if trades else 0.0

    return (
        f"{tr(uid, 'lbl_trades')}: {trades} | "
        f"{tr(uid, 'lbl_wins')}: {wins} | "
        f"{tr(uid, 'lbl_losses')}: {losses} | "
        f"{tr(uid, 'lbl_be')}: {be} | "
        f"{tr(uid, 'lbl_tp1')}: {tp1}\n"
        f"{tr(uid, 'lbl_winrate')}: {wr:.1f}%\n"
        f"{tr(uid, 'lbl_pnl')}: {pnl:+.2f}%"
    )

def _build_status_text(uid: int = 0) -> str:
    next_macro = backend.get_next_macro()
    macro_action = backend.last_macro_action
    macro_prefix = "🟢" if macro_action == "ALLOW" else "🔴"

    macro_line = tr(uid, "next_macro").format(v=tr(uid, "lbl_none"))
    if next_macro:
        ev, (w0, w1) = next_macro
        secs = w0 - time.time()
        next_prefix = "🟡" if macro_action == "ALLOW" else "🔴"
        # next macro + blackout + countdown
        macro_line = f"{next_prefix} {tr(uid, 'next_macro').format(v=ev.name)} | {tr(uid, 'lbl_blackout')} {_fmt_hhmm(w0)}–{_fmt_hhmm(w1)} | {tr(uid, 'lbl_in')} {_fmt_countdown(secs)}"

    scan_line = tr(uid, "scanner_run")
    news_line = tr(uid, "news_action").format(v=_val(uid, backend.last_news_action))
    macro_line2 = tr(uid, "macro_action").format(v=_val(uid, macro_action))

    txt = "\n".join([scan_line, news_line, f"{macro_prefix} {macro_line2}", macro_line])
    return txt

async def _status_autorefresh(uid: int, chat_id: int, message_id: int, seconds: int = 120, interval: int = 5) -> None:
    # Refresh countdown + macro status for a short window to avoid spam/rate limits
    end = time.time() + max(10, seconds)
    last_txt: str | None = None
    while time.time() < end:
        await asyncio.sleep(interval)
        try:
            txt = _build_status_text(uid)
            if txt == last_txt:
                continue
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=txt, reply_markup=menu_kb(uid))
            last_txt = txt
        except Exception:
            return

# ---------------- UI helpers ----------------

def menu_kb(uid: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "m_status"), callback_data="menu:status")
    kb.button(text=tr(uid, "m_stats"), callback_data="menu:stats")
    kb.button(text=tr(uid, "m_spot"), callback_data="menu:spot")
    kb.button(text=tr(uid, "m_fut"), callback_data="menu:futures")
    kb.button(text=tr(uid, "m_trades"), callback_data="trades:page:0")
    kb.adjust(2, 2)
    return kb.as_markup()

def _signal_text(s: Signal, uid: int = 0) -> str:
    header = tr(uid, "sig_spot_hdr") if s.market == "SPOT" else tr(uid, "sig_fut_hdr")
    arrow = tr(uid, "sig_long") if s.direction == "LONG" else tr(uid, "sig_short")
    parts = [
        header,
        "",
        f"🪙 {s.symbol}",
        arrow,
        f"⏱ {tr(uid, 'lbl_tf')}: {s.timeframe}",
        "",
        f"{tr(uid, 'lbl_entry')}: {s.entry:.6f}",
        f"{tr(uid, 'lbl_sl')}: {s.sl:.6f}",
        f"{tr(uid, 'lbl_tp1')}: {s.tp1:.6f}",
        f"{tr(uid, 'lbl_tp2')}: {s.tp2:.6f}",
        "",
        f"{tr(uid, 'lbl_rr')}: 1:{s.rr:.2f}",
        f"{tr(uid, 'lbl_conf')}: {s.confidence}/100",
    ]
    if (s.risk_note or '').strip():
        parts += ["", str(s.risk_note)]
    return "\n".join(parts)

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
    ORIGINAL_SIGNAL_TEXT[sig.signal_id] = _signal_text(sig, 0)
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "btn_open_trade"), callback_data=f"open:{sig.signal_id}")

    for uid in list(USERS):
        try:
            await bot.send_message(uid, _signal_text(sig, uid), reply_markup=kb.as_markup())
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
    uid = message.from_user.id if message.from_user else 0
    if uid:
        USERS.add(uid)
        save_users()

    # If language not chosen yet, ask first
    if uid and uid not in LANG:
        kb = InlineKeyboardBuilder()
        kb.button(text=tr(uid, "btn_ru"), callback_data="lang:ru")
        kb.button(text=tr(uid, "btn_en"), callback_data="lang:en")
        kb.adjust(2)
        await message.answer(tr(uid, "choose_lang"), reply_markup=kb.as_markup())
        return

    await message.answer(tr(uid, "welcome"), reply_markup=menu_kb(uid))


# ---------------- language selection ----------------
@dp.callback_query(lambda c: (c.data or "").startswith("lang:"))
async def lang_choose(call: types.CallbackQuery) -> None:
    await call.answer()
    uid = call.from_user.id if call.from_user else 0
    lang = (call.data or "lang:ru").split(":", 1)[1]
    if uid:
        set_lang(uid, lang)
    # show welcome + menu in-place
    try:
        if call.message:
            await bot.edit_message_text(chat_id=uid, message_id=call.message.message_id, text=tr(uid, "welcome"), reply_markup=menu_kb(uid))
            return
    except Exception:
        pass
    await bot.send_message(uid, tr(uid, "welcome"), reply_markup=menu_kb(uid))

# ---------------- menu callbacks ----------------
@dp.callback_query(lambda c: (c.data or "").startswith("menu:"))
async def menu_handler(call: types.CallbackQuery) -> None:
    action = (call.data or "").split(":", 1)[1]
    uid = call.from_user.id if call.from_user else 0
    _nav_enter(uid, action)
    if action == "back":
        action = _nav_back(uid, "status")

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
            txt = _build_status_text(uid)
        except Exception as e:
            txt = f"Status error: {e}"
        if call.from_user and _is_admin(call.from_user.id) and backend.last_signal:
            ls = backend.last_signal
            txt += f"\nLast signal: {ls.symbol} {ls.market} {ls.direction} conf={ls.confidence}"

        msg = await _render_in_place(call, txt, menu_kb(call.from_user.id))

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

        # performance (separate Spot / Futures)
        spot_today = backend.perf_today("SPOT")
        fut_today = backend.perf_today("FUTURES")
        spot_week = backend.perf_week("SPOT")
        fut_week = backend.perf_week("FUTURES")

        try:
            spot_daily = backend.report_daily("SPOT", 7)
            fut_daily = backend.report_daily("FUTURES", 7)
            spot_weekly = backend.report_weekly("SPOT", 4)
            fut_weekly = backend.report_weekly("FUTURES", 4)
        except Exception:
            spot_daily, fut_daily, spot_weekly, fut_weekly = [], [], [], []

        spot_daily_nz = [x for x in spot_daily if "trades=0" not in x]
        fut_daily_nz = [x for x in fut_daily if "trades=0" not in x]
        spot_weekly_nz = [x for x in spot_weekly if "trades=0" not in x]
        fut_weekly_nz = [x for x in fut_weekly if "trades=0" not in x]

        parts = []
        parts.append(tr(uid, "stats_title"))
        parts.append("")
        parts.append(tr(uid, "perf_today"))
        parts.append("🟢 " + tr(uid, "lbl_spot"))
        parts.append(_fmt_perf(uid, spot_today))
        parts.append("")
        parts.append("🔴 " + tr(uid, "lbl_futures"))
        parts.append(_fmt_perf(uid, fut_today))
        parts.append("")
        parts.append(tr(uid, "perf_week"))
        parts.append("🟢 " + tr(uid, "lbl_spot"))
        parts.append(_fmt_perf(uid, spot_week))
        parts.append("")
        parts.append("🔴 " + tr(uid, "lbl_futures"))
        parts.append(_fmt_perf(uid, fut_week))
        parts.append("")
        parts.append(tr(uid, "daily_title"))
        parts.append("🟢 " + tr(uid, "lbl_spot") + ":")
        parts.append("\n".join(spot_daily_nz) if spot_daily_nz else tr(uid, "no_closed"))
        parts.append("")
        parts.append("🔴 " + tr(uid, "lbl_futures") + ":")
        parts.append("\n".join(fut_daily_nz) if fut_daily_nz else tr(uid, "no_closed"))
        parts.append("")
        parts.append(tr(uid, "weekly_title"))
        parts.append("🟢 " + tr(uid, "lbl_spot") + ":")
        parts.append("\n".join(spot_weekly_nz) if spot_weekly_nz else tr(uid, "no_closed"))
        parts.append("")
        parts.append("🔴 " + tr(uid, "lbl_futures") + ":")
        parts.append("\n".join(fut_weekly_nz) if fut_weekly_nz else tr(uid, "no_closed"))
        parts.append("")
        parts.append(tr(uid, "tip_closed"))
        txt = "\n".join(parts)

        kb = InlineKeyboardBuilder()
        kb.button(text=tr(uid, "refresh"), callback_data="menu:stats")
        kb.button(text=tr(uid, "back"), callback_data="menu:back")
        kb.adjust(2)
        await _render_in_place(call, txt, kb.as_markup())
        return

    if action == "back":
        prev = _get_last_view(call.from_user.id, "status")
        # fallback to status
        call.data = f"menu:{prev}"
        await menu_handler(call)
        return


    if action in ("spot", "futures"):
        sig = backend.last_spot_signal if action == "spot" else backend.last_futures_signal
        if not sig:
            await _render_in_place(call, tr(uid, "no_live"), menu_kb(uid))
            return
        kb = InlineKeyboardBuilder()
        kb.button(text=tr(uid, "btn_open_trade"), callback_data=f"open:{sig.signal_id}")
        await bot.send_message(call.from_user.id, _signal_text(sig, uid), reply_markup=kb.as_markup())
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
        await bot.send_message(call.from_user.id, tr(uid, "mytrades_empty"), reply_markup=menu_kb(uid))
        return

    page = all_trades[offset:offset+PAGE_SIZE]
    txt = (
        f"{tr(uid, 'mytrades_title')} ({tr(uid, 'mytrades_showing')} {offset+1}-{min(offset+PAGE_SIZE, len(all_trades))} {tr(uid, 'mytrades_of')} {len(all_trades)})\n"
        + tr(uid, "mytrades_tap")
    )
    await bot.send_message(call.from_user.id, txt, reply_markup=_trades_page_kb(page, offset))

# ---------------- trade card ----------------
def _trade_card_text(t, uid: int = 0) -> str:
    s = t.signal
    last_price = f"{t.last_price:.6f}" if getattr(t, "last_price", 0.0) else "-"
    parts = [
        tr(uid, "trade_title"),
        "",
        f"🪙 {s.symbol} | {s.market} | {s.direction}",
        f"{tr(uid, 'lbl_tf')}: {s.timeframe}",
        "",
        f"{tr(uid, 'lbl_entry')}: {s.entry:.6f}",
        f"{tr(uid, 'lbl_sl')}: {s.sl:.6f}",
        f"{tr(uid, 'lbl_tp1')}: {s.tp1:.6f}",
        f"{tr(uid, 'lbl_tp2')}: {s.tp2:.6f}",
        "",
        f"{tr(uid, 'trade_status')}: {t.result} {_trade_status_emoji(t.result)}",
        f"{tr(uid, 'trade_last_price')}: {last_price}",
    ]
    return "\n".join(parts)

def _trade_card_kb(signal_id: int, back_offset: int = 0, uid: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "btn_refresh"), callback_data=f"trade:refresh:{signal_id}:{back_offset}")
    kb.button(text=tr(uid, "btn_show_orig"), callback_data=f"trade:orig:{signal_id}:{back_offset}")
    kb.button(text=tr(uid, "btn_close"), callback_data=f"trade:close:{signal_id}:{back_offset}")
    kb.button(text=tr(uid, "btn_back"), callback_data=f"trades:page:{back_offset}")
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
        await bot.send_message(call.from_user.id, tr(uid, "trade_not_found"), reply_markup=menu_kb(uid))
        return
    await bot.send_message(call.from_user.id, _trade_card_text(t, uid), reply_markup=_trade_card_kb(signal_id, 0, uid))

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
        await bot.send_message(call.from_user.id, tr(uid, "trade_not_found"), reply_markup=menu_kb(uid))
        return
    await bot.send_message(call.from_user.id, _trade_card_text(t, uid), reply_markup=_trade_card_kb(signal_id, back_offset, uid))

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
        await bot.send_message(call.from_user.id, tr(uid, "trade_removed"), reply_markup=menu_kb(uid))
    else:
        await bot.send_message(call.from_user.id, tr(uid, "trade_not_found"), reply_markup=menu_kb(uid))

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
        await bot.send_message(call.from_user.id, tr(uid, "orig_not_available"), reply_markup=menu_kb(uid))
        return

    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "btn_back_to_trade"), callback_data=f"trade:view:{signal_id}")
    kb.button(text="📂 My trades", callback_data=f"trades:page:{back_offset}")
    kb.button(text="🏠 Menu", callback_data="menu:status")
    kb.adjust(1, 2)

    await bot.send_message(call.from_user.id, tr(uid, "orig_title") + "\n\n" + text, reply_markup=kb.as_markup())

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
    await bot.send_message(call.from_user.id, f"✅ Trade opened: {sig.symbol} ({sig.market}). Check 📂 My trades.", reply_markup=menu_kb(call.from_user.id))

async def main() -> None:
    import time
    globals()["time"] = time

    load_users()
    load_langs()
    asyncio.create_task(backend.track_loop(bot))
    asyncio.create_task(backend.scanner_loop(broadcast_signal, broadcast_macro_alert))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
