from __future__ import annotations

import asyncio
import json
import os
import logging
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from dotenv import load_dotenv
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import datetime as dt

import asyncpg
from aiohttp import web

import db_store

from backend import Backend, Signal, MacroEvent, open_metrics

load_dotenv()

# --- time parsing helpers ---
def _parse_iso_dt(v):
    """Parse ISO datetime string to tz-aware datetime (UTC).

    Accepts values like "2026-02-15T20:55:00.000Z" or with offset.
    Returns dt.datetime or None.
    """
    if v is None or v == "":
        return None
    if isinstance(v, dt.datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=dt.timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            d = dt.datetime.fromisoformat(s)
        except Exception:
            return None
        return d if d.tzinfo is not None else d.replace(tzinfo=dt.timezone.utc)
    return None

# ---- logging to stdout (Railway Deploy Logs) ----
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("bot")

# --- i18n template safety guard (prevents leaking {placeholders} to users) ---
_UNFILLED_RE = re.compile(r'(?<!\{)\{[a-zA-Z0-9_]+\}(?!\})')

def _sanitize_template_text(uid: int, text: str, ctx: str = "") -> str:
    """Guard against unfilled {placeholders} leaking to users."""
    if not text:
        return text
    hits = _UNFILLED_RE.findall(text)
    if hits:
        logger.error("Unfilled i18n placeholders for uid=%s ctx=%s hits=%s text=%r", uid, ctx, sorted(set(hits)), text)
        text = _UNFILLED_RE.sub("", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

async def safe_send(chat_id: int, text: str, *, ctx: str = "", **kwargs):
    text = _sanitize_template_text(chat_id, text, ctx=ctx)
    # Never recurse. Send via bot API.
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except TelegramForbiddenError:
        # User blocked the bot or cannot be contacted
        await set_user_blocked(chat_id, blocked=True)
        raise
    except TelegramBadRequest as e:
        if "chat not found" in str(e).lower():
            # Chat does not exist / user never started the bot
            await set_user_blocked(chat_id, blocked=True)
        raise

async def safe_edit_text(chat_id: int, message_id: int, text: str, *, ctx: str = "", **kwargs):
    text = _sanitize_template_text(chat_id, text, ctx=ctx)
    return await bot.edit_message_text(text=text, chat_id=chat_id, message_id=message_id, **kwargs)


# -----------------------------------------------


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

# Timezone
# NOTE: "MSK" is a common shorthand, but it's not a valid IANA tz database key.
# Use "Europe/Moscow" (MSK / UTC+3) instead.
# Default to Moscow time for the bot (can be overridden via TZ_NAME env).
_tz_raw = (os.getenv("TZ_NAME", "Europe/Moscow") or "").strip()

_TZ_ALIASES = {
    "MSK": "Europe/Moscow",
    "MOSCOW": "Europe/Moscow",
    "RUSSIA/MOSCOW": "Europe/Moscow",
    "EUROPE/MOSCOW": "Europe/Moscow",
    "UTC+3": "Europe/Moscow",
    "GMT+3": "Europe/Moscow",
}

TZ_NAME = _TZ_ALIASES.get(_tz_raw.upper(), _tz_raw) or "Europe/Moscow"
try:
    TZ = ZoneInfo(TZ_NAME)
except (ZoneInfoNotFoundError, FileNotFoundError):
    # If tzdata isn't installed or the key is invalid, fall back safely.
    # Prefer Moscow for your bot (as requested), otherwise UTC.
    try:
        TZ_NAME = "Europe/Moscow"
        TZ = ZoneInfo(TZ_NAME)
    except Exception:
        TZ_NAME = "UTC"
        TZ = ZoneInfo("UTC")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
backend = Backend()

# Keep last broadcast signals for 'Spot live' / 'Futures live' buttons
LAST_SIGNAL_BY_MARKET = {"SPOT": None, "FUTURES": None}

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
pool: asyncpg.Pool | None = None

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


I18N_FILE = Path(__file__).with_name("i18n.json")

def load_i18n() -> dict:
    # Load i18n from external file (preferred). Fall back to embedded defaults if needed.
    try:
        if I18N_FILE.exists():
            data = json.loads(I18N_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "en" in data:
                return data
    except Exception:
        pass
    return {"ru": {}, "en": {}}

I18N = load_i18n()

# Support username (without @) is configurable via env
SUPPORT_USERNAME = os.getenv('SUPPORT_USERNAME', 'cryptoarb_web_bot_admin').lstrip('@')


def tr(uid: int, key: str) -> str:
    lang = get_lang(uid)
    tmpl = I18N.get(lang, I18N['en']).get(key, I18N['en'].get(key, key))
    # Allow using @{support} placeholder in i18n texts, configured via SUPPORT_USERNAME env
    if isinstance(tmpl, str) and '{support}' in tmpl:
        return tmpl.replace('{support}', SUPPORT_USERNAME)
    return tmpl




class _SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"

def trf(uid: int, key: str, **kwargs) -> str:
    # Inject support placeholder for templates using @{support}
    kwargs.setdefault('support', SUPPORT_USERNAME)
    tmpl = tr(uid, key)
    try:
        return str(tmpl).format_map(_SafeDict(**kwargs))
    except Exception:
        return str(tmpl)

def tr_action(uid: int, v: str) -> str:
    vv = (v or "").upper().strip()
    if vv == "ALLOW":
        return tr(uid, "action_allow")
    if vv in {"PAUSE", "PAUSED", "STOP", "BLOCK"}:
        return tr(uid, "action_pause")
    return v

def tr_market(uid: int, market: str) -> str:
    return tr(uid, "lbl_spot") if (market or "").upper().strip() == "SPOT" else tr(uid, "lbl_futures")

SIGNALS: Dict[int, Signal] = {}
ORIGINAL_SIGNAL_TEXT: Dict[tuple[int,int], str] = {}  # (uid, signal_id) -> text

# Anti-duplicate protection for broadcasted signals.
# Some scanners can emit the same signal multiple times (sometimes with different IDs).
_SENT_SIG_CACHE: Dict[str, float] = {}  # signature -> last_sent_ts
_SENT_SIG_TTL_SEC = int(os.getenv("SENT_SIG_TTL_SEC", "600"))  # default 10 minutes

# ---------------- signal stats (daily/weekly) ----------------
STATS_FILE = Path("signal_stats.json")
SIGNAL_STATS: Dict[str, Dict[str, int]] = {"days": {}, "weeks": {}}

# Trade stats file (written by backend.py). We expose READ-ONLY aggregates via admin HTTP.
TRADE_STATS_FILE = Path("trade_stats.json")

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

def _signals_this_month() -> int:
    """Return number of signals sent in current month (based on SIGNAL_STATS['days'])."""
    today = dt.datetime.now(TZ).date()
    prefix = f"{today.year:04d}-{today.month:02d}-"
    total = 0
    for k, v in (SIGNAL_STATS.get("days") or {}).items():
        if isinstance(k, str) and k.startswith(prefix):
            try:
                total += int(v)
            except Exception:
                pass
    return int(total)

def _safe_load_json(path: Path) -> dict:
    try:
        if path.exists():
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

def _sum_trade_buckets(days_map: dict, keys: list[str]) -> dict:
    """Sum trade buckets (wins/losses/be/trades/tp1_hits/sum_pnl_pct) across given day keys."""
    out = {"trades": 0, "wins": 0, "losses": 0, "be": 0, "tp1_hits": 0, "sum_pnl_pct": 0.0}
    for k in keys:
        b = (days_map or {}).get(k) or {}
        try:
            out["trades"] += int(b.get("trades", 0) or 0)
            out["wins"] += int(b.get("wins", 0) or 0)
            out["losses"] += int(b.get("losses", 0) or 0)
            out["be"] += int(b.get("be", 0) or 0)
            out["tp1_hits"] += int(b.get("tp1_hits", 0) or 0)
            out["sum_pnl_pct"] += float(b.get("sum_pnl_pct", 0.0) or 0.0)
        except Exception:
            continue
    return out

def _trade_stats_agg() -> dict:
    """Aggregate trade stats by market for day/week/month. Read-only."""
    store = _safe_load_json(TRADE_STATS_FILE)
    today = dt.datetime.now(TZ).date()
    dayk = _day_key(today)
    weekk = _week_key(today)
    prefix = f"{today.year:04d}-{today.month:02d}-"

    out: dict[str, dict] = {}
    for mk in ("spot", "futures"):
        mroot = store.get(mk) if isinstance(store, dict) else None
        mroot = mroot if isinstance(mroot, dict) else {"days": {}, "weeks": {}}
        days = mroot.get("days") if isinstance(mroot.get("days"), dict) else {}
        weeks = mroot.get("weeks") if isinstance(mroot.get("weeks"), dict) else {}

        # day
        day_b = days.get(dayk) if isinstance(days.get(dayk), dict) else {"trades": 0, "wins": 0, "losses": 0, "be": 0, "tp1_hits": 0, "sum_pnl_pct": 0.0}
        # week
        week_b = weeks.get(weekk) if isinstance(weeks.get(weekk), dict) else {"trades": 0, "wins": 0, "losses": 0, "be": 0, "tp1_hits": 0, "sum_pnl_pct": 0.0}
        # month = sum days in current month
        month_keys = [k for k in days.keys() if isinstance(k, str) and k.startswith(prefix)]
        month_b = _sum_trade_buckets(days, month_keys)

        out[mk] = {
            "day": day_b,
            "week": week_b,
            "month": month_b,
        }
    return out

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


async def safe_edit_markup(chat_id: int, message_id: int, reply_markup, *, ctx: str = ""):
    """Edit message reply_markup only (e.g., remove inline keyboard)."""
    try:
        return await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=reply_markup)
    except Exception:
        return None

async def _send_long(chat_id: int, text: str, reply_markup=None) -> None:
    # Telegram message limit ~4096 chars. Send in chunks if needed.
    max_len = 3800
    if len(text) <= max_len:
        await safe_send(chat_id, text, reply_markup=reply_markup)
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



async def safe_edit(message: types.Message | None, txt: str, kb: types.InlineKeyboardMarkup) -> None:
    """Edit message text if possible; otherwise send a new message."""
    try:
        if message:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=message.message_id,
                text=txt,
                reply_markup=kb,
            )
            return
    except Exception:
        # message may be too old/not modified/etc. Fall back to send.
        pass

    try:
        if message:
            await safe_send(message.chat.id, txt, reply_markup=kb)
    except Exception:
        pass


async def _render_in_place(call: types.CallbackQuery, txt: str, kb: types.InlineKeyboardMarkup) -> types.Message:
    """Prefer editing the message that contains the pressed button."""
    try:
        if call.message:
            await bot.edit_message_text(chat_id=call.from_user.id, message_id=call.message.message_id, text=txt, reply_markup=kb)
            return call.message
    except Exception:
        pass
    return await safe_send(call.from_user.id, txt, reply_markup=kb)


async def safe_edit(message: types.Message | None, text: str, kb: types.InlineKeyboardMarkup | None = None) -> None:
    """Safely update an existing bot message.

    Telegram has a 4096 char limit for message text. This helper:
    - tries to edit the existing message (best UX)
    - if edit fails (or text is too long), sends new message(s)

    IMPORTANT: never raise from here; menu callbacks must not crash the bot.
    """

    if not message:
        return

    chat_id = message.chat.id
    msg_id = message.message_id

    def _split_text(s: str, limit: int = 3900) -> list[str]:
        s = s or ""
        if len(s) <= limit:
            return [s]
        out: list[str] = []
        buf: list[str] = []
        cur = 0
        for line in s.split("\n"):
            piece = line + "\n"
            if cur + len(piece) > limit and buf:
                out.append("".join(buf).rstrip("\n"))
                buf = [piece]
                cur = len(piece)
            else:
                buf.append(piece)
                cur += len(piece)
        if buf:
            out.append("".join(buf).rstrip("\n"))
        return out

    parts = _split_text(text)

    # Try to edit with the first chunk
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=parts[0],
            reply_markup=kb if len(parts) == 1 else None,
        )
    except Exception:
        # If we can't edit (old message, same text, etc.) - just send new
        try:
            await safe_send(chat_id, parts[0], reply_markup=kb if len(parts) == 1 else None)
        except Exception:
            logger.exception("safe_edit: failed to edit/send first chunk")

    # Send remaining chunks (if any)
    if len(parts) > 1:
        for i, part in enumerate(parts[1:], start=1):
            try:
                await safe_send(chat_id, part, reply_markup=kb if i == len(parts) - 1 else None)
            except Exception:
                logger.exception("safe_edit: failed to send chunk %s/%s", i + 1, len(parts))

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

async def _build_status_text(uid: int = 0) -> str:
    next_macro = backend.get_next_macro()
    macro_action = (backend.last_macro_action or "ALLOW").upper()
    macro_prefix = "🟢" if macro_action == "ALLOW" else "🔴"

    macro_line = tr(uid, "next_macro").format(v=tr(uid, "lbl_none"))
    if next_macro:
        ev, (w0, w1) = next_macro
        secs = max(0, w0 - time.time())
        macro_line = f"🟡 {ev}: {_fmt_hhmm(w0)}–{_fmt_hhmm(w1)} | {tr(uid, 'lbl_in')} {_fmt_countdown(secs)}"

    scan_line = tr(uid, "scanner_run")
    news_line = tr(uid, "news_action").format(v=tr_action(uid, backend.last_news_action))
    macro_line2 = tr(uid, "macro_action").format(v=tr_action(uid, macro_action))

    enabled = True
    try:
        enabled = await get_notify_signals(uid) if uid else True
    except Exception:
        enabled = True
    state = tr(uid, "notify_status_on") if enabled else tr(uid, "notify_status_off")
    notif_line = tr(uid, "status_notify").format(v=state)

    return "\n".join([scan_line, news_line, f"{macro_prefix} {macro_line2}", macro_line, notif_line])

async def _status_autorefresh(uid: int, chat_id: int, message_id: int, seconds: int = 120, interval: int = 5) -> None:
    # Refresh countdown + macro status for a short window to avoid spam/rate limits
    end = time.time() + max(10, seconds)
    last_txt: str | None = None
    while time.time() < end:
        await asyncio.sleep(interval)
        try:
            txt = await _build_status_text(uid)
            if txt == last_txt:
                continue
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=txt, reply_markup=menu_kb(uid))
            last_txt = txt
        except Exception:
            return


# ---------------- DB helpers ----------------

async def init_db() -> None:
    """Init Postgres pool and ensure required columns exist."""
    global pool
    if not DATABASE_URL:
        # Allow running without Postgres locally, but notifications will be disabled.
        pool = None
        return
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, timeout=8, command_timeout=10)
    # Share the same pool with trade storage
    db_store.set_pool(pool)
    await db_store.ensure_schema()
    # Users table migrations (signal access columns) live in db_store
    await db_store.ensure_users_columns()

async def ensure_user(user_id: int) -> None:
    if not pool or not user_id:
        return
    # Create user row if missing and grant 24h Signal trial ONCE.
    # IMPORTANT: does NOT touch Arbitrage access.
    try:
        await db_store.ensure_user_signal_trial(int(user_id))
    except Exception:
        logger.exception("ensure_user: failed")


async def set_user_blocked(user_id: int, blocked: bool = True) -> None:
    """Disable broadcasts for users who blocked the bot / never started it (prevents spammy 'chat not found')."""
    if not pool or not user_id:
        return
    async with pool.acquire() as conn:
        if blocked:
            await conn.execute(
                """UPDATE users
                      SET is_blocked = TRUE,
                          notify_signals = FALSE
                    WHERE telegram_id = $1;""",
                user_id,
            )
        else:
            await conn.execute(
                """UPDATE users
                      SET is_blocked = FALSE
                    WHERE telegram_id = $1;""",
                user_id,
            )


async def get_user_row(user_id: int):
    if not pool or not user_id:
        return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT telegram_id,
                   is_blocked,
                   notify_signals,
                   -- signal access (new)
                   signal_enabled,
                   signal_expires_at,
                   -- legacy shared access (fallback for older DBs)
                   expires_at
              FROM users
             WHERE telegram_id=$1
            """,
            user_id,
        )

def _access_status_from_row(row) -> str:
    # Global maintenance switch (Signal bot only).
    # Enable via env: SIGNAL_MAINTENANCE=1 or MAINTENANCE_MODE=1
    try:
        import os
        mm = (os.getenv("SIGNAL_MAINTENANCE") or os.getenv("MAINTENANCE_MODE") or "").strip().lower()
        if mm in {"1", "true", "yes", "on"}:
            return "maintenance"
    except Exception:
        pass

    if row is None:
        return "no_user"
    try:
        if bool(row.get("is_blocked")):
            return "blocked"
    except Exception:
        pass
    # Signal bot access is time-based.
    # IMPORTANT: signal_enabled must NOT block access.
    # It is used to control whether the user receives new signals (broadcast),
    # not whether the user can open the bot/menu.
    signal_expires_at = row.get("signal_expires_at")

    # Backward compatibility: if signal_expires_at is missing but legacy expires_at exists,
    # treat legacy expiry as signal access to avoid locking out existing users.
    if signal_expires_at is None:
        legacy_exp = row.get("expires_at")
        if legacy_exp is not None:
            signal_expires_at = legacy_exp

    # NULL expiry means "lifetime" access
    if signal_expires_at is None:
        return "ok"

    try:
        now = dt.datetime.now(dt.timezone.utc)
        if signal_expires_at < now:
            return "expired"
    except Exception:
        return "expired"
    return "ok"

async def get_access_status(user_id: int) -> str:
    row = await get_user_row(user_id)
    return _access_status_from_row(row)

async def get_notify_signals(user_id: int) -> bool:
    row = await get_user_row(user_id)
    if row is None:
        return True
    v = row.get("notify_signals")
    return True if v is None else bool(v)

async def toggle_notify_signals(user_id: int) -> bool:
    if not pool or not user_id:
        return True
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE users
                   SET notify_signals = NOT COALESCE(notify_signals, TRUE)
                 WHERE telegram_id=$1
             RETURNING notify_signals""",
            user_id,
        )
        if row is None:
            await ensure_user(user_id)
            return True
        return bool(row["notify_signals"])


async def set_notify_signals(user_id: int, value: bool) -> bool:
    """Set notify_signals explicitly. Returns the saved value."""
    if not pool or not user_id:
        return True
    await ensure_user(user_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE users
                   SET notify_signals = $2
                 WHERE telegram_id=$1
             RETURNING notify_signals""",
            user_id, bool(value)
        )
        if row is None:
            # if user row didn't exist for some reason, ensure_user created it with TRUE
            return bool(value)
        return bool(row["notify_signals"])

async def get_broadcast_user_ids() -> List[int]:
    """Users allowed to receive signals."""
    if not pool:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT telegram_id
                 FROM users
                 WHERE COALESCE(is_blocked, FALSE) = FALSE
                   AND COALESCE(signal_enabled, FALSE) = TRUE
                   AND (signal_expires_at IS NULL OR signal_expires_at > now())
                   AND COALESCE(notify_signals, TRUE) = TRUE"""
        )
        return [int(r["telegram_id"]) for r in rows if r and r.get("telegram_id") is not None]


# ---------------- UI helpers ----------------

def menu_kb(uid: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "m_status"), callback_data="menu:status")
    kb.button(text=tr(uid, "m_stats"), callback_data="menu:stats")
    kb.button(text=tr(uid, "m_spot"), callback_data="menu:spot")
    kb.button(text=tr(uid, "m_fut"), callback_data="menu:futures")
    kb.button(text=tr(uid, "m_trades"), callback_data="trades:page:0")
    kb.button(text=tr(uid, "m_notify"), callback_data="menu:notify")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def notify_kb(uid: int, enabled: bool) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    # Show only one toggle button to keep UI clean
    if enabled:
        kb.button(text=tr(uid, "btn_notify_off"), callback_data="notify:set:off")
    else:
        kb.button(text=tr(uid, "btn_notify_on"), callback_data="notify:set:on")
    kb.adjust(1)
    kb.button(text=tr(uid, "btn_back"), callback_data="notify:back")
    kb.adjust(1)
    return kb.as_markup()




def _signal_text(uid: int, s: Signal) -> str:
    header = tr(uid, 'sig_spot_header') if s.market == 'SPOT' else tr(uid, 'sig_fut_header')
    market_banner = tr(uid, 'sig_spot_new') if s.market == 'SPOT' else tr(uid, 'sig_fut_new')

    # Visual marker near symbol (kept simple to avoid hard-depending on any exchange)
    symbol_emoji = "🟢" if s.market == 'SPOT' else "🌕"

    # Direction should be shown only for FUTURES.
    # For SPOT it's always "buy/long" and printing "ЛОНГ" confuses users.
    direction_line = ""
    if s.market != 'SPOT':
        arrow = tr(uid, 'sig_long') if s.direction == 'LONG' else tr(uid, 'sig_short')
        direction_line = arrow + "\n"

    def _fmt_exchanges(raw: str) -> str:
        xs = [x.strip() for x in (raw or '').replace(',', '+').split('+') if x.strip()]
        pretty = []
        for x in xs:
            u = x.upper()
            if u == 'BINANCE':
                pretty.append('Binance')
            elif u == 'BYBIT':
                pretty.append('Bybit')
            elif u == 'OKX':
                pretty.append('OKX')
            else:
                pretty.append(x)
        out: List[str] = []
        for p in pretty:
            if p not in out:
                out.append(p)
        return ' • '.join(out)

    def _tp_lines() -> List[str]:
        tps_obj = getattr(s, 'tps', None)
        if isinstance(tps_obj, (list, tuple)) and len(tps_obj) > 0:
            lines: List[str] = []
            for i, v in enumerate(tps_obj, start=1):
                try:
                    fv = float(v)
                except Exception:
                    continue
                if fv <= 0:
                    continue
                lines.append(f"TP{i}: {fv:.6f}")
            if lines:
                return lines

        lines: List[str] = []
        lines.append(f"TP1: {s.tp1:.6f}")
        try:
            if float(s.tp2) > 0 and abs(float(s.tp2) - float(s.tp1)) > 1e-12:
                lines.append(f"TP2: {s.tp2:.6f}")
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            # Do not spam stack traces for common delivery issues
            msg = str(e).lower()
            if "chat not found" in msg or "bot was blocked by the user" in msg:
                logger.warning("Skip uid=%s: %s", uid, e)
            else:
                logger.exception("Failed to send message to uid=%s", uid)
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            msg = str(e).lower()
            if "chat not found" in msg or "bot was blocked by the user" in msg:
                logger.warning("Skip uid=%s: %s", uid, e)
            else:
                logger.exception("Failed to send message to uid=%s", uid)
        except Exception:
            logger.exception("Failed to send message to uid=%s", uid)
        return lines

    ex_line_raw = _fmt_exchanges(getattr(s, 'confirmations', '') or '')
    exchanges_line = f"{tr(uid, 'sig_exchanges')}: {ex_line_raw}\n" if ex_line_raw else ""
    # Keep TF on its own line; timeframe string already like 15m/1h/4h
    tf_line = f"{tr(uid, 'sig_tf')}: {s.timeframe}"

    tp_lines = "\n".join(_tp_lines())

    rr_line = f"{tr(uid, 'sig_rr')}: 1:{s.rr:.2f}"
    conf_line = f"{tr(uid, 'sig_confidence')}: {s.confidence}/100"
    confirm_line = f"{tr(uid, 'sig_confirm')}: {s.confirmations}"

    risk_note = (s.risk_note or '').strip()
    risk_block = f"\n\n{risk_note}" if risk_note else ""

    return trf(uid, "msg_open_card",
        market_banner=market_banner,
        header=header,
        symbol_emoji=symbol_emoji,
        symbol=s.symbol,
        direction_line=direction_line,
        exchanges_line=exchanges_line,
        tf_line=tf_line,
        entry_label=tr(uid, 'sig_entry'),
        entry=f"{s.entry:.6f}",
        sl=f"{s.sl:.6f}",
        tp_lines=tp_lines,
        rr_line=rr_line,
        conf_line=conf_line,
        confirm_line=confirm_line,
        risk_block=risk_block,
        open_prompt=tr(uid, 'sig_open_prompt')
    )

def _fmt_hhmm(ts_utc: float) -> str:
    d = dt.datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC")).astimezone(TZ)
    return d.strftime("%H:%M")

def _fmt_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if "/" in s:
        a, b = s.split("/", 1)
        return f"{a.strip()} / {b.strip()}"
    return s

def _fmt_price(v) -> str:
    try:
        if v is None:
            return "—"
        return f"{float(v):.6f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v) if v is not None else "—"

def _calc_profit_pct(sig: Signal) -> float:
    """Model expected profit % using TP2 if available, otherwise TP1."""
    try:
        entry = float(sig.entry)
        tp = float(sig.tp2) if float(getattr(sig, "tp2", 0.0) or 0.0) > 0 else float(sig.tp1)
        side = (sig.direction or "LONG").upper().strip()
        if entry <= 0:
            return 0.0
        if side == "SHORT":
            return (entry - tp) / entry * 100.0
        return (tp - entry) / entry * 100.0
    except Exception:
        return 0.0

def _open_card_text(uid: int, sig: Signal) -> str:
    sym_disp = _fmt_symbol(sig.symbol)
    base = sym_disp.split("/")[0].strip().replace(" ", "") if sym_disp else "SYMBOL"
    exchanges = (getattr(sig, "confirmations", "") or "").replace("+", " • ")
    if not exchanges:
        exchanges = tr(uid, "lbl_none")

    # backend-only metrics (profit/leverage/rr computed in backend.py)
    mx = open_metrics(sig)
    profit = mx.get("profit", 0.0)
    hhmm = _fmt_hhmm(float(getattr(sig, "ts", 0.0) or 0.0))

    side = mx.get("side", (sig.direction or "LONG")).upper().strip()
    mkt = mx.get("market", (sig.market or "FUTURES")).upper().strip()
    lev = str(mx.get("lev", "5"))
    rr = mx.get("rr", getattr(sig, "rr", 0.0))

    if mkt == "SPOT":
        return trf(
            uid,
            "open_spot",
            symbol=sym_disp,
            exchanges=exchanges,
            entry=f"{float(sig.entry):.5f}",
            tp1=f"{float(sig.tp1):.5f}",
            tp2=f"{float(sig.tp2):.5f}",
            sl=f"{float(sig.sl):.5f}",
            profit=f"{float(profit):.1f}",
            time=hhmm,
            tag=base,
        )

    return trf(
        uid,
        "open_futures",
        symbol=sym_disp,
        exchanges=exchanges,
        side=side,
        lev=lev,
        entry=f"{float(sig.entry):.5f}",
        tp1=f"{float(sig.tp1):.5f}",
        tp2=f"{float(sig.tp2):.5f}",
        sl=f"{float(sig.sl):.5f}",
        rr=f"{float(rr):.1f}",
        profit=f"{float(profit):.1f}",
        time=hhmm,
        tag=base,
    )

def _too_late_reason_key(reason: str) -> str:
    return {
        "TP2": "sig_too_late_reason_tp2",
        "TP1": "sig_too_late_reason_tp1",
        "SL": "sig_too_late_reason_sl",
        "TIME": "sig_too_late_reason_time",
        "ERROR": "sig_too_late_reason_error",
        "OK": "sig_too_late_reason_error",
    }.get((reason or "").upper(), "sig_too_late_reason_error")


async def _expire_signal_message(call: types.CallbackQuery, sig: Signal, reason: str, price: float) -> None:
    """Remove the OPEN button and mark the signal as expired in-place."""
    uid = call.from_user.id
    reason_key = _too_late_reason_key(reason)
    note = tr(uid, "sig_expired_note").format(
        reason=tr(uid, reason_key),
        price=_fmt_price(price),
    )
    try:
        current_text = (call.message.text or "").rstrip()
        if "⛔" not in current_text:
            new_text = current_text + "\n\n" + note
        else:
            new_text = current_text
        await call.message.edit_text(new_text, reply_markup=None)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass


def _too_late_text(uid: int, sig: Signal, reason: str, price: float) -> str:
    mkt = (sig.market or "FUTURES").upper()
    mkt_emoji = "🟢" if mkt == "SPOT" else "🔴"
    sym = _fmt_symbol(sig.symbol)
    hhmm = _fmt_hhmm(float(getattr(sig, "ts", 0.0) or 0.0))

    reason_key = _too_late_reason_key(reason)

    return tr(uid, "sig_too_late_body").format(
        market_emoji=mkt_emoji,
        market=mkt,
        symbol=sym,
        reason=tr(uid, reason_key),
        price=_fmt_price(price),
        tp1=_fmt_price(getattr(sig, "tp1", None)),
        tp2=_fmt_price(getattr(sig, "tp2", None)),
        sl=_fmt_price(getattr(sig, "sl", None)),
        time=hhmm,
    )


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
        "ACTIVE": "🟢",
        "TP1": "🟡",
        "WIN": "🟣",
        "LOSS": "🔴",
        "BE": "⚪",
        "CLOSED": "✅",
    }.get(status, "⏳")

# ---------------- broadcasting ----------------
_SIGNAL_SETTINGS_CACHE = {"ts": 0.0, "data": {"pause_signals": False, "maintenance_mode": False}}

async def get_signal_global_settings(force: bool = False) -> dict:
    """Return global Signal-bot settings with a short in-memory cache."""
    import time as _time
    now = _time.time()
    if (not force) and (now - float(_SIGNAL_SETTINGS_CACHE.get("ts", 0.0)) < 5.0):
        return dict(_SIGNAL_SETTINGS_CACHE.get("data") or {})
    try:
        data = await db_store.get_signal_bot_settings()
        _SIGNAL_SETTINGS_CACHE["data"] = {
            "pause_signals": bool(data.get("pause_signals")),
            "maintenance_mode": bool(data.get("maintenance_mode")),
        }
    except Exception:
        # If DB unavailable, default to not paused (do not brick bot).
        _SIGNAL_SETTINGS_CACHE["data"] = {"pause_signals": False, "maintenance_mode": False}
    _SIGNAL_SETTINGS_CACHE["ts"] = now
    return dict(_SIGNAL_SETTINGS_CACHE.get("data") or {})

async def broadcast_signal(sig: Signal) -> None:
    # Global pause (Signal bot): полностью отключает рассылку НОВЫХ сигналов.
    try:
        st = await get_signal_global_settings()
        if st.get("pause_signals"):
            logger.info("Signal broadcast paused: skip %s %s %s", sig.market, sig.symbol, sig.direction)
            return
    except Exception:
        pass
    # Deduplicate: avoid sending the same signal twice (common when scanner emits duplicates).
    import time, hashlib
    now = time.time()
    for k, ts in list(_SENT_SIG_CACHE.items()):
        if now - ts > _SENT_SIG_TTL_SEC:
            _SENT_SIG_CACHE.pop(k, None)

    sig_raw = f"{sig.market}|{sig.symbol}|{sig.direction}|{sig.timeframe}|{sig.entry}|{sig.sl}|{sig.tp1}|{sig.tp2}|{sig.confirmations}"
    sig_key = hashlib.sha1(sig_raw.encode('utf-8')).hexdigest()
    if sig_key in _SENT_SIG_CACHE:
        logger.info("Skip duplicate signal %s %s %s (within ttl)", sig.market, sig.symbol, sig.direction)
        return
    _SENT_SIG_CACHE[sig_key] = now

    # Assign a globally unique signal_id from DB sequence (survives restarts).
    # This prevents collisions that cause "already opened" for unrelated signals after container restart.
    try:
        sig.signal_id = await db_store.next_signal_id()
    except Exception as e:
        # Fallback to existing id if DB is unavailable; still log for visibility.
        logger.error("Failed to allocate signal_id from DB sequence: %s", e)
    # Save as last live signal for menu buttons
    try:
        LAST_SIGNAL_BY_MARKET["SPOT" if sig.market == "SPOT" else "FUTURES"] = sig
    except Exception:
        pass

    logger.info("Broadcast signal id=%s %s %s %s conf=%s rr=%.2f", sig.signal_id, sig.market, sig.symbol, sig.direction, sig.confidence, float(sig.rr))
    SIGNALS[sig.signal_id] = sig
    # Persist 'signals sent' counters in Postgres (dedup by sig_key)
    try:
        await db_store.record_signal_sent(sig_key=sig_key, market=str(sig.market).upper(), signal_id=int(sig.signal_id or 0))
    except Exception:
        pass
    ORIGINAL_SIGNAL_TEXT[(0, sig.signal_id)] = _signal_text(0, sig)
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(0, "btn_opened"), callback_data=f"open:{sig.signal_id}")

    uids = await get_broadcast_user_ids()
    for uid in uids:
        try:
            ORIGINAL_SIGNAL_TEXT[(uid, sig.signal_id)] = _signal_text(uid, sig)
            kb_u = InlineKeyboardBuilder()
            kb_u.button(text=tr(uid, "btn_opened"), callback_data=f"open:{sig.signal_id}")
            await safe_send(uid, _signal_text(uid, sig), reply_markup=kb_u.as_markup())
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            # Do not spam stack traces for common delivery issues
            msg = str(e).lower()
            if "chat not found" in msg or "bot was blocked by the user" in msg:
                logger.warning("Skip uid=%s: %s", uid, e)
            else:
                logger.exception("Failed to send message to uid=%s", uid)
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            msg = str(e).lower()
            if "chat not found" in msg or "bot was blocked by the user" in msg:
                logger.warning("Skip uid=%s: %s", uid, e)
            else:
                logger.exception("Failed to send message to uid=%s", uid)
        except Exception:
            logger.exception("Failed to send message to uid=%s", uid)


async def broadcast_macro_alert(action: str, ev: MacroEvent, win: Tuple[float, float], tz_name: str) -> None:
    w0, w1 = win
    uids = await get_broadcast_user_ids()
    for uid in uids:
        try:
            title = tr(uid, 'macro_title')
            body = f"{ev.name}\n{tr(uid, 'macro_blackout')}: {_fmt_hhmm(w0)} – {_fmt_hhmm(w1)}\n\n"
            tail = tr(uid, 'macro_tail_fut_off') if action == 'FUTURES_OFF' else tr(uid, 'macro_tail_paused')
            await safe_send(uid, f"{title}\n\n{body}{tail}")
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            # Do not spam stack traces for common delivery issues
            msg = str(e).lower()
            if "chat not found" in msg or "bot was blocked by the user" in msg:
                logger.warning("Skip uid=%s: %s", uid, e)
            else:
                logger.exception("Failed to send message to uid=%s", uid)
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            msg = str(e).lower()
            if "chat not found" in msg or "bot was blocked by the user" in msg:
                logger.warning("Skip uid=%s: %s", uid, e)
            else:
                logger.exception("Failed to send message to uid=%s", uid)
        except Exception:
            logger.exception("Failed to send message to uid=%s", uid)

# ---------------- commands ----------------
@dp.message(Command("start"))
async def start(message: types.Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    if uid:
        await ensure_user(uid)
        await set_user_blocked(uid, blocked=False)

    # If language not chosen yet, ask first
    if uid and uid not in LANG:
        kb = InlineKeyboardBuilder()
        kb.button(text=tr(uid, "btn_ru"), callback_data="lang:ru")
        kb.button(text=tr(uid, "btn_en"), callback_data="lang:en")
        kb.adjust(2)
        await message.answer(tr(uid, "choose_lang"), reply_markup=kb.as_markup())
        return

    # Shared access control (Postgres)
    access = await get_access_status(uid) if uid else "no_user"
    if access != "ok":
        await message.answer(tr(uid, f"access_{access}"), reply_markup=menu_kb(uid))
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
    await safe_send(uid, tr(uid, "welcome"), reply_markup=menu_kb(uid))

# ---------------- menu callbacks ----------------
def _fmt_stats_block_ru(title: str, b: dict | None = None) -> str:
    b = b or {}
    trades = int(b.get("trades", 0))
    wins = int(b.get("wins", 0))
    losses = int(b.get("losses", 0))
    be = int(b.get("be", 0))
    tp1 = int(b.get("tp1_hits", 0))
    wr = (wins / trades * 100.0) if trades else 0.0
    pnl = float(b.get("sum_pnl_pct", 0.0))
    return (
        f"{title}\n"
        f"Сделки: {trades} | Плюс: {wins} | Минус: {losses} | BE: {be} | TP1: {tp1}\n"
        f"Процент побед: {wr:.1f}%\n"
        f"PnL: {pnl:+.2f}%"
    )

def _fmt_stats_block_en(title: str, b: dict | None = None) -> str:
    b = b or {}
    trades = int(b.get("trades", 0))
    wins = int(b.get("wins", 0))
    losses = int(b.get("losses", 0))
    be = int(b.get("be", 0))
    tp1 = int(b.get("tp1_hits", 0))
    wr = (wins / trades * 100.0) if trades else 0.0
    pnl = float(b.get("sum_pnl_pct", 0.0))
    return (
        f"{title}\n"
        f"Trades: {trades} | Wins: {wins} | Losses: {losses} | BE: {be} | TP1: {tp1}\n"
        f"Win rate: {wr:.1f}%\n"
        f"PnL: {pnl:+.2f}%"
    )

def _parse_report_lines(lines: list[str]) -> list[tuple[str,int,float,float]]:
    # returns (key, trades, winrate, pnl)
    out = []
    rx = re.compile(r"^(?P<k>[^:]+):\s*trades=(?P<t>\d+)\s+winrate=(?P<wr>[\d\.]+)%\s+pnl=(?P<pnl>[\+\-]?[\d\.]+)%\s*$")
    for ln in lines:
        m = rx.match((ln or "").strip())
        if not m:
            continue
        out.append((m.group("k"), int(m.group("t")), float(m.group("wr")), float(m.group("pnl"))))
    return out

async def stats_text(uid: int) -> str:
    """Render trading statistics for the user from Postgres (backend-only)."""
    lang = get_lang(uid)
    tz_name = os.getenv("TZ", "UTC")

    # ---- buckets ----
    spot_today = await backend.perf_today(uid, "SPOT")
    fut_today = await backend.perf_today(uid, "FUTURES")
    spot_week = await backend.perf_week(uid, "SPOT")
    fut_week = await backend.perf_week(uid, "FUTURES")

    spot_days = await backend.report_daily(uid, "SPOT", 7, tz=tz_name)
    fut_days = await backend.report_daily(uid, "FUTURES", 7, tz=tz_name)

    spot_weeks = await backend.report_weekly(uid, "SPOT", 4, tz=tz_name)
    fut_weeks = await backend.report_weekly(uid, "FUTURES", 4, tz=tz_name)

    # ---- formatting helpers ----
    def block_title(market: str) -> str:
        if market == "SPOT":
            return f"🟢 {tr(uid, 'lbl_spot')}"
        return f"🔴 {tr(uid, 'lbl_futures')}"

    def fmt_block(title: str, b: dict) -> str:
        if lang == "en":
            return _fmt_stats_block_en(title, b)
        return _fmt_stats_block_ru(title, b)

    def fmt_lines(rows: list[dict]) -> list[str]:
        out: list[str] = []
        for r in rows:
            # day is date, week is text label
            key = r.get("day")
            if key is not None:
                k = key.isoformat() if hasattr(key, "isoformat") else str(key)
            else:
                k = str(r.get("week"))
            trades = int(r.get("trades", 0) or 0)
            wins = int(r.get("wins", 0) or 0)
            pnl = float(r.get("sum_pnl_pct", 0.0) or 0.0)
            wr = (wins / trades * 100.0) if trades else 0.0
            if lang == "en":
                out.append(f"{k}: trades {trades} | winrate {wr:.1f}% | pnl {pnl:+.2f}%")
            else:
                out.append(f"{k}: Сделки {trades} | Победы {wr:.1f}% | PnL {pnl:+.2f}%")
        return out

    no_closed = tr(uid, "no_closed")

    parts: list[str] = []
    parts.append(tr(uid, "stats_title"))
    parts.append("")
    parts.append(tr(uid, "perf_today"))
    parts.append(fmt_block(block_title("SPOT"), spot_today))
    parts.append("")
    parts.append(fmt_block(block_title("FUTURES"), fut_today))
    parts.append("")
    parts.append(tr(uid, "perf_week"))
    parts.append(fmt_block(block_title("SPOT"), spot_week))
    parts.append("")
    parts.append(fmt_block(block_title("FUTURES"), fut_week))
    parts.append("")
    parts.append(tr(uid, "daily_title"))
    parts.append(f"🟢 {tr(uid,'lbl_spot')}:")
    if not spot_days:
        parts.append(no_closed)
    else:
        parts.extend(fmt_lines(spot_days))
    parts.append("")
    parts.append(f"🔴 {tr(uid,'lbl_futures')}:")
    if not fut_days:
        parts.append(no_closed)
    else:
        parts.extend(fmt_lines(fut_days))
    parts.append("")
    parts.append(tr(uid, "weekly_title"))
    parts.append(f"🟢 {tr(uid,'lbl_spot')}:")
    if not spot_weeks:
        parts.append(no_closed)
    else:
        parts.extend(fmt_lines(spot_weeks))
    parts.append("")
    parts.append(f"🔴 {tr(uid,'lbl_futures')}:")
    if not fut_weeks:
        parts.append(no_closed)
    else:
        parts.extend(fmt_lines(fut_weeks))
    parts.append("")
    hint = tr(uid, "stats_hint") if "stats_hint" in I18N.get(lang, {}) else ""
    if hint:
        parts.append(hint)

    return "\n".join(parts).strip()
@dp.callback_query(lambda c: (c.data or "").startswith("menu:"))
async def menu_handler(call: types.CallbackQuery) -> None:
    action = (call.data or "").split(":", 1)[1]
    uid = call.from_user.id if call.from_user else 0
    _nav_enter(uid, action)
    if action == "back":
        action = _nav_back(uid, "status")

    await call.answer()

    # Shared access control (Postgres)
    access = await get_access_status(uid) if uid else "no_user"
    if action not in ("status", "notify") and access != "ok":
        await safe_edit(call.message, tr(uid, f"access_{access}"), menu_kb(uid))
        return

    # ---- STATUS (main screen) ----
    if action == "status":
        t = STATUS_TASKS.pop(uid, None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass

        await ensure_user(uid)
        await set_user_blocked(uid, blocked=False)

        # Notifications toggle state (per user)
        enabled = None
        try:
            import asyncio as _asyncio
            enabled = await _asyncio.wait_for(get_notify_signals(uid), timeout=2.5)
        except Exception:
            enabled = None

        # Global states
        try:
            scanner_on = bool(getattr(backend, "scanner_running", True))
        except Exception:
            scanner_on = True

        # News / Macro status with reason + timer
        try:
            news_stat = backend.get_news_status()
        except Exception:
            news_stat = {"action": "ALLOW", "reason": None, "until_ts": None}

        try:
            macro_stat = backend.get_macro_status()
        except Exception:
            macro_stat = {"action": "ALLOW", "reason": None, "until_ts": None, "event": None, "window": None}

        def _is_allow(action: str) -> bool:
            return (action or "").upper() == "ALLOW"

        scanner_state = tr(uid, "status_scanner_on") if scanner_on else tr(uid, "status_scanner_off")
        news_state = tr(uid, "status_allow") if _is_allow(news_stat.get("action")) else tr(uid, "status_block")
        macro_action = (macro_stat.get("action") or "ALLOW").upper()
        macro_state = tr(uid, "status_allow") if _is_allow(macro_action) else tr(uid, "status_block")
        macro_icon = "🟢" if _is_allow(macro_action) else "🔴"
        notif_state = (tr(uid, "status_notif_on") if enabled is True else (tr(uid, "status_notif_off") if enabled is False else tr(uid, "status_unknown")))

        # Next macro (if known)
        next_macro = tr(uid, "status_next_macro_none")
        try:
            nm = backend.get_next_macro()
        except Exception:
            nm = None
        if nm:
            ev, (w0, w1) = nm
            name = getattr(ev, "name", None) or getattr(ev, "type", None) or "-"
            hhmm = _fmt_hhmm(getattr(ev, "start_ts_utc", time.time()))
            next_macro = f"{name} {hhmm}"

        lines = [
            tr(uid, "status_title"),
            "",
            trf(uid, "status_scanner", state=scanner_state),
            trf(uid, "status_news", state=news_state),
        ]

        # News details when blocked
        if not _is_allow(news_stat.get("action")):
            reason = news_stat.get("reason")
            if reason:
                lines.append(trf(uid, "status_news_reason", reason=reason))
            until_ts = news_stat.get("until_ts")
            if until_ts:
                left = _fmt_countdown(float(until_ts) - time.time())
                lines.append(trf(uid, "status_news_timer", left=left))

        lines.append(trf(uid, "status_macro", icon=macro_icon, state=macro_state))

        # Macro details when blocked
        if not _is_allow(macro_action):
            reason = macro_stat.get("reason")
            if reason:
                lines.append(trf(uid, "status_macro_reason", reason=reason))
            win = macro_stat.get("window")
            if win and isinstance(win, (tuple, list)) and len(win) == 2:
                w0, w1 = float(win[0]), float(win[1])
                lines.append(trf(uid, "status_macro_window", before=_fmt_hhmm(w0), after=_fmt_hhmm(w1)))
            until_ts = macro_stat.get("until_ts")
            if until_ts:
                left = _fmt_countdown(float(until_ts) - time.time())
                lines.append(trf(uid, "status_macro_timer", left=left))
            # Timer to end of blackout window (more explicit)
            if not until_ts and win and isinstance(win, (tuple, list)) and len(win) == 2:
                until_ts = float(win[1])
            if until_ts:
                left_end = _fmt_countdown(float(until_ts) - time.time())
                lines.append(trf(uid, "status_macro_blackout_timer", left=left_end))

        lines.append(trf(uid, "status_next_macro", value=(next_macro or tr(uid, "lbl_none"))))
        lines.append(trf(uid, "status_notifications", state=notif_state))

        txt = "\n".join(lines)
        await safe_edit(call.message, txt, menu_kb(uid))
        return

# ---- STATS ----
    if action == "stats":
        t = STATUS_TASKS.pop(uid, None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass

        txt = await stats_text(uid)
        await safe_edit(call.message, txt, menu_kb(uid))
        return

    # ---- LIVE SIGNALS ----
    if action in ("spot", "futures"):
        market = "SPOT" if action == "spot" else "FUTURES"
        sig = LAST_SIGNAL_BY_MARKET.get(market)
        if not sig:
            await safe_edit(call.message, tr(uid, "no_live_signals"), menu_kb(uid))
            return

        allowed, reason, price = await backend.check_signal_openable(sig)
        if not allowed:
            # Drop stale signal from Live caches
            if LAST_SIGNAL_BY_MARKET.get(market) and LAST_SIGNAL_BY_MARKET[market].signal_id == sig.signal_id:
                LAST_SIGNAL_BY_MARKET[market] = None
            await safe_edit(call.message, tr(uid, "no_live_signals"), menu_kb(uid))
            return

        kb = InlineKeyboardBuilder()
        kb.button(text=tr(uid, "btn_opened"), callback_data=f"open:{sig.signal_id}")
        kb.adjust(1)
        await safe_edit(call.message, _signal_text(uid, sig), kb.as_markup())
        return

    # ---- NOTIFICATIONS ----
    if action == "notify":
        if uid:
            await ensure_user(uid)
            await set_user_blocked(uid, blocked=False)
            enabled = await get_notify_signals(uid)
            state = tr(uid, "notify_status_on") if enabled else tr(uid, "notify_status_off")
            desc = tr(uid, "notify_desc_on") if enabled else tr(uid, "notify_desc_off")
            txt = trf(uid, "notify_screen", title=tr(uid, "notify_title"), state=state, desc=desc)
            await safe_send(uid, txt, reply_markup=notify_kb(uid, enabled))
        return

    # fallback
    await safe_edit(call.message, tr(uid, "welcome"), menu_kb(uid))


# ---------------- notifications callbacks ----------------
@dp.callback_query(lambda c: (c.data or "").startswith("notify:"))
async def notify_handler(call: types.CallbackQuery) -> None:
    """Handle inline buttons inside Notifications screen."""
    await call.answer()
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    parts = (call.data or "").split(":")
    # notify:set:on | notify:set:off | notify:back
    try:
        action = parts[1]
    except Exception:
        action = ""

    # shared access check (same as menu)
    access = await get_access_status(uid)
    if access != "ok":
        await safe_edit(call.message, tr(uid, f"access_{access}"), menu_kb(uid))
        return

    if action == "back":
        # Return to main menu (do not delete messages)
        await safe_edit(call.message, tr(uid, "welcome"), menu_kb(uid))
        return

    if action == "set":
        # Explicit ON/OFF
        value = (parts[2] if len(parts) > 2 else "").lower()
        await ensure_user(uid)
        await set_user_blocked(uid, blocked=False)
        if value == "on":
            enabled = await set_notify_signals(uid, True)
        elif value == "off":
            enabled = await set_notify_signals(uid, False)
        else:
            enabled = await get_notify_signals(uid)

        state = tr(uid, "notify_status_on") if enabled else tr(uid, "notify_status_off")
        desc = tr(uid, "notify_desc_on") if enabled else tr(uid, "notify_desc_off")
        txt = trf(uid, "notify_screen", title=tr(uid, "notify_title"), state=state, desc=desc)
        await safe_edit(call.message, txt, notify_kb(uid, enabled))
        return

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
            txt = await _build_status_text(uid)
        except Exception as e:
            txt = f"Status error: {e}"
        if call.from_user and _is_admin(call.from_user.id) and backend.last_signal:
            ls = backend.last_signal
            extra_dir = f" {ls.direction}" if getattr(ls, 'market', '') != 'SPOT' else ""
            txt += f"\nLast signal: {ls.symbol} {ls.market}{extra_dir} conf={ls.confidence}"

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
        spot_today = await backend.perf_today("SPOT")
        fut_today = await backend.perf_today("FUTURES")
        spot_week = await backend.perf_week("SPOT")
        fut_week = await backend.perf_week("FUTURES")

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
        kb.button(text=tr(0, "btn_opened"), callback_data=f"open:{sig.signal_id}")
        await safe_send(call.from_user.id, _signal_text(sig), reply_markup=kb.as_markup())
        return


# ---------------- trades list (with buttons) ----------------
PAGE_SIZE = 10

def _trades_page_kb(uid: int, trades: list[dict], offset: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for t in trades:
        trade_id = int(t.get('id') or 0)
        symbol = str(t.get("symbol") or "")
        market = str(t.get("market") or "FUTURES").upper()
        status = str(t.get("status") or "ACTIVE").upper()
        label = f"{_trade_status_emoji(status)} {symbol} {tr_market(uid, market)} ({status})"
        kb.button(text=label, callback_data=f"trade:view:{int(t.get('id') or 0)}:{offset}")
    # pagination
    nav = InlineKeyboardBuilder()
    if offset > 0:
        nav.button(text=tr(uid, "btn_prev"), callback_data=f"trades:page:{max(0, offset-PAGE_SIZE)}")
    if len(trades) == PAGE_SIZE:
        nav.button(text=tr(uid, "btn_next"), callback_data=f"trades:page:{offset+PAGE_SIZE}")
    if nav.buttons:
        kb.adjust(1)
        kb.row(*[b for b in nav.buttons])
    kb.row(types.InlineKeyboardButton(text=tr(uid, "m_menu"), callback_data="menu:status"))
    return kb.as_markup()

@dp.callback_query(lambda c: (c.data or "").startswith("trades:page:"))
async def trades_page(call: types.CallbackQuery) -> None:
    await call.answer()
    try:
        offset = int((call.data or "").split(":")[-1])
    except Exception:
        offset = 0

    all_trades = await backend.get_user_trades(call.from_user.id)
    if not all_trades:
        await safe_send(call.from_user.id, tr(call.from_user.id, "my_trades_empty"), reply_markup=menu_kb(call.from_user.id))
        return

    page = all_trades[offset:offset+PAGE_SIZE]
    txt = tr(call.from_user.id, "my_trades_title").format(a=offset+1, b=min(offset+PAGE_SIZE, len(all_trades)), n=len(all_trades))
    await safe_send(call.from_user.id, txt, reply_markup=_trades_page_kb(call.from_user.id, page, offset))

# ---------------- trade card ----------------

def _trade_card_text(uid: int, t: dict) -> str:
    symbol = str(t.get("symbol") or "")
    market = str(t.get("market") or "FUTURES").upper()
    side = str(t.get("side") or "LONG").upper()
    status = str(t.get("status") or "ACTIVE").upper()

    entry = float(t.get("entry") or 0.0)
    tp1 = t.get("tp1")
    tp2 = t.get("tp2")
    sl = t.get("sl")

    head = f"🪙 {symbol} | {tr_market(uid, market)}"
    if market != "SPOT":
        head += f" | {side}"

    parts = [
        "📌 " + tr(uid, "m_trades"),
        "",
        head,
        "",
        f"{tr(uid, 'sig_entry')}: {entry:.6f}",
    ]
    if sl is not None:
        parts.append(f"SL: {float(sl):.6f}")
    if tp1 is not None:
        parts.append(f"TP1: {float(tp1):.6f}")
    if tp2 is not None and float(tp2) > 0 and (tp1 is None or abs(float(tp2) - float(tp1)) > 1e-12):
        parts.append(f"TP2: {float(tp2):.6f}")

    # Live price info (shown when loaded via get_trade_live)
    if t.get('price_f') is not None:
        try:
            px = float(t.get('price_f') or 0.0)
            src = str(t.get('price_src') or '')
            parts.append("")
            parts.append(f"💹 {tr(uid, 'lbl_price_now')}: {px:.6f}")
            if src:
                parts.append(f"🔌 {tr(uid, 'lbl_price_src')}: {src}")
            checks = []
            if t.get('sl') is not None:
                checks.append(f"SL: {'✅' if t.get('hit_sl') else '❌'}")
            if t.get('tp1') is not None:
                checks.append(f"{tr(uid, 'lbl_tp1')}: {'✅' if t.get('hit_tp1') else '❌'}")
            if t.get('tp2') is not None and float(t.get('tp2') or 0) > 0:
                checks.append(f"TP2: {'✅' if t.get('hit_tp2') else '❌'}")
            if checks:
                parts.append(f"🧪 {tr(uid, 'lbl_check')}: " + ' '.join(checks))
        except Exception:
            pass

    parts += [
        "",
        f"{tr(uid, 'sig_status')}: {status} {_trade_status_emoji(status)}",
    ]

    pnl = t.get("pnl_total_pct")
    if pnl is not None and status in ("WIN","LOSS","BE","CLOSED"):
        try:
            parts.append(f"PnL: {float(pnl):+.2f}%")
        except Exception:
            pass

    return "\n".join(parts)

def _trade_card_kb(uid: int, trade_id: int, back_offset: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "sig_btn_refresh"), callback_data=f"trade:refresh:{trade_id}:{back_offset}")
    kb.button(text=tr(uid, "sig_btn_orig"), callback_data=f"trade:orig:{trade_id}:{back_offset}")
    kb.button(text=tr(uid, "sig_btn_close"), callback_data=f"trade:close:{trade_id}:{back_offset}")
    kb.button(text=tr(uid, "sig_btn_back"), callback_data=f"trades:page:{back_offset}")
    kb.adjust(2, 2)
    return kb.as_markup()

@dp.callback_query(lambda c: (c.data or "").startswith("trade:view:"))
async def trade_view(call: types.CallbackQuery) -> None:
    await call.answer()
    parts = (call.data or "").split(":")
    try:
        trade_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return
    t = await backend.get_trade_live_by_id(call.from_user.id, trade_id)
    if not t:
        await safe_send(call.from_user.id, tr(call.from_user.id, "trade_not_found"), reply_markup=menu_kb(call.from_user.id))
        return
    await safe_send(call.from_user.id, _trade_card_text(call.from_user.id, t), reply_markup=_trade_card_kb(call.from_user.id, trade_id, back_offset))

@dp.callback_query(lambda c: (c.data or "").startswith("trade:refresh:"))
async def trade_refresh(call: types.CallbackQuery) -> None:
    await call.answer()
    parts = (call.data or "").split(":")
    try:
        trade_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return
    t = await backend.get_trade_live_by_id(call.from_user.id, trade_id)
    if not t:
        await safe_send(call.from_user.id, tr(call.from_user.id, "trade_not_found"), reply_markup=menu_kb(call.from_user.id))
        return
    await safe_send(call.from_user.id, _trade_card_text(call.from_user.id, t), reply_markup=_trade_card_kb(call.from_user.id, trade_id, back_offset))

@dp.callback_query(lambda c: (c.data or "").startswith("trade:close:"))
async def trade_close(call: types.CallbackQuery) -> None:
    await call.answer()
    parts = (call.data or "").split(":")
    try:
        trade_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return
    removed = await backend.remove_trade_by_id(call.from_user.id, trade_id)
    if removed:
        await safe_send(call.from_user.id, tr(call.from_user.id, "trade_removed"), reply_markup=menu_kb(call.from_user.id))
    else:
        await safe_send(call.from_user.id, tr(call.from_user.id, "trade_removed_fail"), reply_markup=menu_kb(call.from_user.id))

@dp.callback_query(lambda c: (c.data or "").startswith("trade:orig:"))
async def trade_orig(call: types.CallbackQuery) -> None:
    await call.answer()
    parts = (call.data or "").split(":")
    try:
        trade_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return

    t = await backend.get_trade_live_by_id(call.from_user.id, trade_id)
    text = (t.get("orig_text") if isinstance(t, dict) else None) if t else None
    if not text:
        await safe_send(call.from_user.id, tr(call.from_user.id, "sig_orig_title") + ": " + tr(call.from_user.id, "lbl_none"), reply_markup=menu_kb(call.from_user.id))
        return

    kb = InlineKeyboardBuilder()
    kb.button(text=tr(call.from_user.id, "sig_btn_back_trade"), callback_data=f"trade:view:{trade_id}:{back_offset}")
    kb.button(text=tr(call.from_user.id, "sig_btn_my_trades"), callback_data=f"trades:page:{back_offset}")
    kb.button(text=tr(call.from_user.id, "m_menu"), callback_data="menu:status")
    kb.adjust(1, 2)

    await safe_send(call.from_user.id, "📌 Original signal (1:1)\n\n" + text, reply_markup=kb.as_markup())

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

    allowed, reason, price = await backend.check_signal_openable(sig)
    if not allowed:
        # remove stale from Live cache
        mkt = (sig.market or "FUTURES").upper()
        if LAST_SIGNAL_BY_MARKET.get(mkt) and LAST_SIGNAL_BY_MARKET[mkt].signal_id == sig.signal_id:
            LAST_SIGNAL_BY_MARKET[mkt] = None
        await _expire_signal_message(call, sig, reason, price)
        await call.answer(tr(call.from_user.id, "sig_too_late_toast"), show_alert=True)
        await safe_send(call.from_user.id, _too_late_text(call.from_user.id, sig, reason, price), reply_markup=menu_kb(call.from_user.id))
        return

    opened_ok = await backend.open_trade(call.from_user.id, sig, orig_text=_signal_text(call.from_user.id, sig))
    if not opened_ok:
        # IMPORTANT:
        # Treat as "already opened" ONLY if the user has an ACTIVE/TP1 trade for this signal_id.
        # This also protects from rare signal_id collisions after restarts / legacy DB constraints.
        row = None
        try:
            row = await backend.get_trade(call.from_user.id, int(sig.signal_id))
        except Exception:
            row = None

        st = str((row or {}).get('status') or '').upper() if isinstance(row, dict) else ''
        if row and st in ("ACTIVE", "TP1"):
            # Already opened -> remove button to prevent retries
            try:
                if call.message:
                    await safe_edit_markup(call.from_user.id, call.message.message_id, None)
            except Exception:
                pass
            await call.answer(tr(call.from_user.id, "sig_already_opened_toast"), show_alert=True)
            await safe_send(call.from_user.id, tr(call.from_user.id, "sig_already_opened_msg"), reply_markup=menu_kb(call.from_user.id))
            return

        # No ACTIVE trade found, but insert was blocked.
        # Re-try with a fresh unique signal_id (prevents "phantom already opened" situations).
        try:
            new_sid = await db_store.next_signal_id()
        except Exception:
            import time as _time
            new_sid = int(_time.time() * 1000)

        try:
            SIGNALS.pop(int(signal_id), None)
        except Exception:
            pass

        from dataclasses import replace as _dc_replace

        new_sig = sig
        try:
            new_sig = _dc_replace(sig, signal_id=int(new_sid))
        except Exception:
            new_sig = sig

        SIGNALS[int(new_sid)] = new_sig
        sig = new_sig

        opened_ok = await backend.open_trade(call.from_user.id, sig, orig_text=_signal_text(call.from_user.id, sig))
        if not opened_ok:
            # Fallback: show message if still blocked
            try:
                if call.message:
                    await safe_edit_markup(call.from_user.id, call.message.message_id, None)
            except Exception:
                pass
            await call.answer(tr(call.from_user.id, "sig_already_opened_toast"), show_alert=True)
            await safe_send(call.from_user.id, tr(call.from_user.id, "sig_already_opened_msg"), reply_markup=menu_kb(call.from_user.id))
            return

    # Remove the ✅ ОТКРЫЛ СДЕЛКУ button from the original NEW SIGNAL message
    try:
        if call.message:
            await safe_edit_markup(call.from_user.id, call.message.message_id, None)
    except Exception:
        pass

    await call.answer(tr(call.from_user.id, "trade_opened_toast"))
    # IMPORTANT: send OPEN card only after user pressed ✅ ОТКРЫЛ СДЕЛКУ
    await safe_send(
        call.from_user.id,
        _open_card_text(call.from_user.id, sig),
        reply_markup=menu_kb(call.from_user.id),
    )

async def main() -> None:
    import time
    globals()["time"] = time

    logger.info("Bot starting; TZ=%s", TZ_NAME)
    await init_db()
    load_langs()

    # --- Admin HTTP API for Status panel (Signal bot access) ---
    # NOTE: Railway public domain requires an HTTP server listening on $PORT.
    # We run a tiny aiohttp server alongside the Telegram bot.
    async def _admin_http_app() -> web.Application:
        app = web.Application()

        # ---- Basic Auth ----
        ADMIN_USER = os.getenv("SIGNAL_ADMIN_USER") or os.getenv("ADMIN_USER") or "admin"
        ADMIN_PASS = os.getenv("SIGNAL_ADMIN_PASS") or os.getenv("ADMIN_PASS") or "password"

        def _unauthorized() -> web.Response:
            return web.Response(status=401, headers={"WWW-Authenticate": 'Basic realm="signal-admin"'})

        def _check_basic(request: web.Request) -> bool:
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Basic "):
                return False
            import base64
            try:
                raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
                u, p = raw.split(":", 1)
                return (u == ADMIN_USER) and (p == ADMIN_PASS)
            except Exception:
                return False

        # ---- CORS (for browser-based Status panel) ----
        @web.middleware
        async def cors_mw(request: web.Request, handler):
            # Preflight
            if request.method == "OPTIONS":
                resp = web.Response(status=204)
            else:
                resp = await handler(request)
            origin = request.headers.get("Origin")
            resp.headers["Access-Control-Allow-Origin"] = origin or "*"
            resp.headers["Vary"] = "Origin"
            resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PATCH,OPTIONS"
            resp.headers["Access-Control-Allow-Headers"] = "Authorization,Content-Type"
            resp.headers["Access-Control-Max-Age"] = "86400"
            return resp

        app.middlewares.append(cors_mw)

        async def health(_: web.Request) -> web.Response:
            return web.json_response({"ok": True, "service": "crypto-signal"})

        async def list_users(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            q = (request.query.get("q") or "").strip()
            flt = (request.query.get("filter") or "all").strip().lower()
            async with pool.acquire() as conn:
                where = []
                args = []
                if q:
                    where.append("CAST(telegram_id AS TEXT) ILIKE $%d" % (len(args)+1))
                    args.append(f"%{q}%")
                if flt == "active":
                    # Access is determined ONLY by expiry time + block, NOT by signal_enabled.
                    where.append("COALESCE(is_blocked,FALSE)=FALSE")
                    where.append("(signal_expires_at IS NULL OR signal_expires_at > now())")
                elif flt == "expired":
                    # Expired access (but not blocked). signal_enabled does not matter here.
                    where.append("COALESCE(is_blocked,FALSE)=FALSE")
                    where.append("signal_expires_at IS NOT NULL")
                    where.append("signal_expires_at <= now()")
                elif flt == "blocked":
                    where.append("COALESCE(is_blocked,FALSE)=TRUE")
                where_sql = ("WHERE " + " AND ".join(where)) if where else ""
                rows = await conn.fetch(
                    f"""
                    SELECT telegram_id, signal_enabled, signal_expires_at, COALESCE(is_blocked,FALSE) AS is_blocked
                    FROM users
                    {where_sql}
                    ORDER BY telegram_id DESC
                    LIMIT 500
                    """,
                    *args,
                )
            out = []
            for r in rows:
                out.append({
                    "telegram_id": int(r["telegram_id"]),
                    "signal_enabled": bool(r["signal_enabled"]),
                    "signal_expires_at": (r["signal_expires_at"].isoformat() if r["signal_expires_at"] else None),
                    "is_blocked": bool(r["is_blocked"]),
                })
            return web.json_response({"items": out})

        async def signal_stats(request: web.Request) -> web.Response:
            """Read-only aggregated statistics for the admin panel.

            Returns counts for:
              - signals sent: day/week/month
              - trade outcomes & real pnl%: day/week/month per market (SPOT/FUTURES)

            Trade stats are computed from Postgres trade_events (WIN/LOSS/BE/CLOSE + TP1).
            This gives correct PnL% that matches how positions were closed.
            """
            if not _check_basic(request):
                return _unauthorized()

            now_tz = dt.datetime.now(TZ)

            def _to_utc(d: dt.datetime) -> dt.datetime:
                if d.tzinfo is None:
                    d = d.replace(tzinfo=TZ)
                return d.astimezone(dt.timezone.utc)

            # Period boundaries in bot TZ (MSK by default)
            day_start = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
            week_start = (now_tz - dt.timedelta(days=now_tz.isoweekday() - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
            month_start = now_tz.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

            ranges = {
                "day": (_to_utc(day_start), _to_utc(now_tz)),
                "week": (_to_utc(week_start), _to_utc(now_tz)),
                "month": (_to_utc(month_start), _to_utc(now_tz)),
            }
            # Signals sent counters are stored in Postgres (signal_sent_events).
            # Backward compatible fields: day/week/month totals + split day_spot/day_futures/...
            signals: dict = {}
            for k, (since, until) in ranges.items():
                spot_n = 0
                fut_n = 0
                if pool is not None:
                    try:
                        counts = await db_store.count_signal_sent_by_market(since=since, until=until)
                        spot_n = int(counts.get('SPOT') or 0)
                        fut_n = int(counts.get('FUTURES') or 0)
                    except Exception:
                        spot_n = 0
                        fut_n = 0
                total_n = spot_n + fut_n
                # legacy totals
                signals[k] = total_n
                # explicit totals + split
                signals[f"{k}_total"] = total_n
                signals[f"{k}_spot"] = spot_n
                signals[f"{k}_futures"] = fut_n

            async def _mk(market: str) -> dict:
                out: dict[str, dict] = {}
                for k, (since, until) in ranges.items():
                    b = await db_store.perf_bucket_global(market, since=since, until=until)
                    trades = int(b.get('trades') or 0)
                    wins = int(b.get('wins') or 0)      # TP2 hits (WIN)
                    losses = int(b.get('losses') or 0)  # SL hits (LOSS)
                    be = int(b.get('be') or 0)          # BE hits
                    tp1 = int(b.get('tp1_hits') or 0)   # TP1 hits (partial)
                    closes = int(b.get('closes') or 0)  # manual CLOSE
                    manual = max(0, closes)
                    sum_pnl = float(b.get('sum_pnl_pct') or 0.0)
                    avg_pnl = (sum_pnl / trades) if trades else 0.0
                    out[k] = {
                        "trades": trades,
                        "tp2": wins,
                        "sl": losses,
                        "be": be,
                        "tp1": tp1,
                        "manual": manual,
                        "sum_pnl_pct": sum_pnl,
                        "avg_pnl_pct": avg_pnl,
                    }
                return out

            perf = {
                "spot": await _mk('SPOT'),
                "futures": await _mk('FUTURES'),
            }

            return web.json_response({
                "ok": True,
                "signals": signals,
                "perf": perf,
            })

        async def save_user(request: web.Request) -> web.Response:
            """Create/update Signal access for a user.

            Expected JSON:
              telegram_id (required)
              signal_enabled (optional)
              signal_expires_at (optional ISO)
              add_days (optional int)
              is_blocked (optional bool)
            """
            if not _check_basic(request):
                return _unauthorized()
            data = await request.json()
            tid = int(data.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)

            enabled = bool(data.get("signal_enabled", True))
            expires_raw = data.get("signal_expires_at")
            expires = _parse_iso_dt(expires_raw)
            if expires_raw and (expires is None):
                return web.json_response({"ok": False, "error": "bad signal_expires_at"}, status=400)
            add_days = int(data.get("add_days") or 0)
            is_blocked = data.get("is_blocked")

            await ensure_user(tid)
            async with pool.acquire() as conn:
                if add_days:
                    await conn.execute(
                        """UPDATE users
                               SET signal_enabled = TRUE,
                                   signal_expires_at = COALESCE(signal_expires_at, now()) + make_interval(days => $2)
                             WHERE telegram_id=$1""",
                        tid, add_days,
                    )
                else:
                    if expires:
                        await conn.execute(
                            """UPDATE users
                                   SET signal_enabled=$2,
                                       signal_expires_at=$3::timestamptz
                                 WHERE telegram_id=$1""",
                            tid, enabled, expires,
                        )
                    else:
                        await conn.execute(
                            """UPDATE users
                                   SET signal_enabled=$2
                                 WHERE telegram_id=$1""",
                            tid, enabled,
                        )

                if is_blocked is not None:
                    try:
                        await conn.execute(
                            "UPDATE users SET is_blocked=$2 WHERE telegram_id=$1",
                            tid, bool(is_blocked),
                        )
                    except Exception:
                        # Column may not exist on some legacy DBs
                        pass

            return web.json_response({"ok": True})

        async def patch_user(request: web.Request) -> web.Response:
            """Compat endpoint: PATCH /api/infra/admin/signal/users/{telegram_id}"""
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            data = await request.json()
            data["telegram_id"] = tid
            # reuse save_user logic by running the same update code inline
            enabled = bool(data.get("signal_enabled", True))
            expires_raw = data.get("signal_expires_at")
            expires = _parse_iso_dt(expires_raw)
            if expires_raw and (expires is None):
                return web.json_response({"ok": False, "error": "bad signal_expires_at"}, status=400)
            add_days = int(data.get("add_days") or 0)
            is_blocked = data.get("is_blocked")

            await ensure_user(tid)
            async with pool.acquire() as conn:
                if add_days:
                    await conn.execute(
                        """UPDATE users
                               SET signal_enabled = TRUE,
                                   signal_expires_at = COALESCE(signal_expires_at, now()) + make_interval(days => $2)
                             WHERE telegram_id=$1""",
                        tid, add_days,
                    )
                else:
                    if expires:
                        await conn.execute(
                            """UPDATE users
                                   SET signal_enabled=$2,
                                       signal_expires_at=$3::timestamptz
                                 WHERE telegram_id=$1""",
                            tid, enabled, expires,
                        )
                    else:
                        await conn.execute(
                            """UPDATE users
                                   SET signal_enabled=$2
                                 WHERE telegram_id=$1""",
                            tid, enabled,
                        )

                if is_blocked is not None:
                    try:
                        await conn.execute(
                            "UPDATE users SET is_blocked=$2 WHERE telegram_id=$1",
                            tid, bool(is_blocked),
                        )
                    except Exception:
                        pass
            return web.json_response({"ok": True})

        async def block_user(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            async with pool.acquire() as conn:
                await conn.execute("UPDATE users SET is_blocked=TRUE WHERE telegram_id=$1", tid)
            return web.json_response({"ok": True})

        async def unblock_user(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            async with pool.acquire() as conn:
                await conn.execute("UPDATE users SET is_blocked=FALSE WHERE telegram_id=$1", tid)
            return web.json_response({"ok": True})


        # --- Signal bot global settings (pause new signals) ---
        async def get_signal_settings(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            try:
                data = await db_store.get_signal_bot_settings()
                return web.json_response({"ok": True, **data})
            except Exception as e:
                return web.json_response({"ok": False, "error": str(e)}, status=500)

        async def set_signal_settings(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            try:
                payload = await request.json()
            except Exception:
                payload = {}
            pause_signals = bool(payload.get("pause_signals", False))
            maintenance_mode = bool(payload.get("maintenance_mode", False))
            try:
                await db_store.set_signal_bot_settings(
                    pause_signals=pause_signals,
                    maintenance_mode=maintenance_mode,
                )
                # refresh in-memory cache quickly
                try:
                    await get_signal_global_settings(force=True)
                except Exception:
                    pass
                return web.json_response({"ok": True})
            except Exception as e:
                return web.json_response({"ok": False, "error": str(e)}, status=500)


        # Routes
        app.router.add_route("GET", "/health", health)
        app.router.add_route("GET", "/api/infra/admin/signal/stats", signal_stats)
        app.router.add_route("GET", "/api/infra/admin/signal/users", list_users)
        app.router.add_route("POST", "/api/infra/admin/signal/users", save_user)
        app.router.add_route("PATCH", "/api/infra/admin/signal/users/{telegram_id}", patch_user)
        app.router.add_route("POST", "/api/infra/admin/signal/users/{telegram_id}/block", block_user)
        app.router.add_route("POST", "/api/infra/admin/signal/users/{telegram_id}/unblock", unblock_user)
        app.router.add_route("GET", "/api/infra/admin/signal/settings", get_signal_settings)
        app.router.add_route("POST", "/api/infra/admin/signal/settings", set_signal_settings)
        # Allow preflight
        app.router.add_route("OPTIONS", "/{tail:.*}", lambda r: web.Response(status=204))
        return app

    async def _start_http_server() -> None:
        try:
            port = int(os.getenv("PORT", "8080"))
        except Exception:
            port = 8080
        app = await _admin_http_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=port)
        await site.start()
        logger.info("Admin HTTP API started on 0.0.0.0:%s", port)

    asyncio.create_task(_start_http_server())
    logger.info("Starting track_loop")
    asyncio.create_task(backend.track_loop(bot))
    logger.info("Starting scanner_loop interval=%ss top_n=%s", os.getenv('SCAN_INTERVAL_SECONDS',''), os.getenv('TOP_N',''))
    asyncio.create_task(backend.scanner_loop(broadcast_signal, broadcast_macro_alert))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
