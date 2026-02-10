from __future__ import annotations

import asyncio
import json
import os
import logging
import re
import math
import statistics
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import replace

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

from backend import Backend, Signal, MacroEvent, open_metrics, validate_autotrade_keys, ExchangeAPIError, autotrade_execute, autotrade_manager_loop, autotrade_healthcheck, autotrade_stress_test
import time

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



def tr(uid: int, key: str, **kwargs) -> str:
    """Translate key for user's language.

    Strict mode:
      - if STRICT_I18N=1 (default), missing keys raise KeyError to force adding them to i18n.json.
      - if STRICT_I18N=0, missing keys fall back to EN, then to the key itself (legacy behavior).
    """
    lang = get_lang(uid)
    strict = os.getenv("STRICT_I18N", "1").lower() not in ("0", "false", "no", "off")
    # prefer requested lang, then EN
    if lang in I18N and key in I18N.get(lang, {}):
        tmpl = I18N[lang][key]
    elif "en" in I18N and key in I18N.get("en", {}):
        tmpl = I18N["en"][key]
    else:
        if strict:
            raise KeyError(f"i18n key not found: {key!r} (lang={lang!r}). Add it to {I18N_FILE.name}.")
        tmpl = key

    # Allow using {support} placeholder in i18n texts, configured via SUPPORT_USERNAME env
    if isinstance(tmpl, str) and "{support}" in tmpl:
        tmpl = tmpl.replace("{support}", SUPPORT_USERNAME)
    if kwargs and isinstance(tmpl, str):
        try:
            tmpl = tmpl.format(**kwargs)
        except Exception:
            pass
    return tmpl


def trf_old(uid: int, key: str, **kwargs) -> str:
    """Translate + safe-format (missing placeholders stay visible)."""
    tmpl = tr(uid, key)
    if not kwargs:
        return tmpl
    try:
        return str(tmpl).format_map(_SafeDict(**kwargs))
    except Exception as e:
        # In strict mode, formatting errors must be visible immediately.
        strict = os.getenv("STRICT_I18N", "1").lower() not in ("0", "false", "no", "off")
        if strict:
            raise
        logger.exception("i18n format error: key=%s kwargs=%s", key, kwargs)
        return str(tmpl)


def audit_i18n_keys(strict: bool | None = None) -> None:
    """Static audit: ensure every tr()/trf() key referenced in bot.py exists in i18n.json."""
    if strict is None:
        strict = os.getenv("STRICT_I18N", "1").lower() not in ("0", "false", "no", "off")

    try:
        src = Path(__file__).read_text(encoding="utf-8")
    except Exception:
        logger.exception("i18n audit: failed to read source")
        return

    keys = set(re.findall(r'\btrf?\(\s*[^,]+,\s*["\']([^"\']+)["\']\s*\)', src))
    missing = []
    for k in sorted(keys):
        for lang in ("ru", "en"):
            if k not in I18N.get(lang, {}):
                missing.append((lang, k))

    if missing:
        # Log a compact report
        lines = ["i18n audit: missing keys:"]
        for lang, k in missing[:200]:
            lines.append(f"- {lang}: {k}")
        if len(missing) > 200:
            lines.append(f"... and {len(missing)-200} more")
        msg = "\n".join(lines)
        if strict:
            raise RuntimeError(msg)
        logger.error(msg)



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

# ---------------- bot statistics ----------------
# NOTE: Statistics are stored ONLY in Postgres (signal_sent_events + signal_tracks).

def _normalize_side_for_stats(direction: str) -> str:
    """Normalize direction to LONG/SHORT for signal_tracks DB constraints.

    Broadcast text may be localized (e.g. "ШОРТ"/"ЛОНГ").
    signal_tracks.side is constrained to ('LONG','SHORT'), so we map common variants.
    """
    d = str(direction or "").strip().upper()
    if not d:
        return "LONG"
    # RU
    if "ШОРТ" in d:
        return "SHORT"
    if "ЛОНГ" in d:
        return "LONG"
    # EN
    if "SHORT" in d or d in ("SELL", "S"):
        return "SHORT"
    if "LONG" in d or d in ("BUY", "B"):
        return "LONG"
    # Fallback to keep DB insert valid
    return "LONG"
# No local JSON files are used.


# (removed legacy weekly report based on local json stats)


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



async def safe_edit_old(message: types.Message | None, txt: str, kb: types.InlineKeyboardMarkup) -> None:
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
    except Exception as e:
        # If the message content is identical, Telegram returns "message is not modified".
        # In that case we do nothing (avoid spamming a new message).
        if "message is not modified" in str(e).lower():
            return
        # If we can't edit (old message, etc.) - just send new
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
    return await status_text(uid, include_subscribed=False, include_hint=False)

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
    # Hide Auto-trade button globally when auto-trade is paused (admin)
    if not bool(AUTOTRADE_BOT_GLOBAL.get("pause_autotrade")):
        kb.button(text=tr(uid, "m_autotrade"), callback_data="menu:autotrade")
    kb.button(text=tr(uid, "m_trades"), callback_data="trades:page:0")
    kb.button(text=tr(uid, "m_notify"), callback_data="menu:notify")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()


def notify_kb(uid: int, enabled: bool) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    # Show only one toggle button to keep UI clean
    if enabled:
        kb.button(text=tr(uid, "btn_notify_off"), callback_data="notify:set:off")
    else:
        kb.button(text=tr(uid, "btn_notify_on"), callback_data="notify:set:on")
    kb.adjust(1)
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:back")
    kb.adjust(1)
    return kb.as_markup()


# ---------------- Auto-trade UI + input state ----------------

from cryptography.fernet import Fernet, InvalidToken

AUTOTRADE_INPUT: Dict[int, Dict[str, str]] = {}

# Per-user locks to prevent race conditions on rapid button presses / concurrent inputs
_USER_LOCKS: Dict[int, asyncio.Lock] = {}
def _user_lock(uid: int) -> asyncio.Lock:
    lock = _USER_LOCKS.get(int(uid))
    if lock is None:
        lock = asyncio.Lock()
        _USER_LOCKS[int(uid)] = lock
    return lock


# Auto-trade stats UI state
AUTOTRADE_STATS_STATE: Dict[int, Dict[str, str]] = {}

# Notify only on API errors (anti-spam)
AUTOTRADE_API_ERR_LAST: Dict[tuple[int, str, str], float] = {}
AUTOTRADE_API_ERR_COOLDOWN_SEC = 300

# Global Auto-trade bot settings cache (admin panel)
AUTOTRADE_BOT_GLOBAL: Dict[str, object] = {
    "pause_autotrade": False,
    "maintenance_mode": False,
    "updated_at": None,
}

async def _refresh_autotrade_bot_global_once() -> None:
    """Load global auto-trade pause/maintenance flags from DB into in-memory cache."""
    try:
        st = await db_store.get_autotrade_bot_settings()
        AUTOTRADE_BOT_GLOBAL["pause_autotrade"] = bool(st.get("pause_autotrade"))
        AUTOTRADE_BOT_GLOBAL["maintenance_mode"] = bool(st.get("maintenance_mode"))
        AUTOTRADE_BOT_GLOBAL["updated_at"] = st.get("updated_at")
    except Exception:
        # FAIL-CLOSED: if we can't read DB settings, block new auto-trade opens.
        # This prevents a situation where admin enabled pause/maintenance but DB read failed,
        # leading to unintended new trades.
        logger.exception("Auto-trade global settings load failed; fail-closed (pause+maintenance)")
        AUTOTRADE_BOT_GLOBAL["pause_autotrade"] = True
        AUTOTRADE_BOT_GLOBAL["maintenance_mode"] = True
        AUTOTRADE_BOT_GLOBAL["updated_at"] = None
        return

async def _autotrade_bot_global_loop() -> None:
    while True:
        try:
            await _refresh_autotrade_bot_global_once()
        except Exception:
            pass
        await asyncio.sleep(5)


def _autotrade_gate_text(uid: int) -> Optional[str]:
    """Return i18n text if autotrade is globally blocked (maintenance/pause)."""
    if bool(AUTOTRADE_BOT_GLOBAL.get("maintenance_mode")):
        return tr(uid, "at_maintenance_block")
    if bool(AUTOTRADE_BOT_GLOBAL.get("pause_autotrade")):
        return tr(uid, "at_pause_block")
    return None



async def _autotrade_access_status(uid: int) -> str:
    """Auto-trade access status from admin subscription flags (users table).
    Returns: ok | blocked | expired | disabled | no_user
    """
    if not uid:
        return "no_user"
    try:
        acc = await db_store.get_autotrade_access(uid)
    except Exception:
        acc = None
    if not acc:
        return "no_user"
    try:
        if bool(acc.get("is_blocked")):
            return "blocked"
    except Exception:
        pass
    enabled = bool(acc.get("autotrade_enabled"))
    exp = acc.get("autotrade_expires_at")
    if not enabled:
        return "disabled"
    if exp is None:
        return "ok"
    try:
        now = dt.datetime.now(dt.timezone.utc)
        if exp < now:
            return "expired"
    except Exception:
        return "expired"
    return "ok"

async def _autotrade_unavailable_text(uid: int) -> str:
    st = await _autotrade_access_status(uid)
    if st == "ok":
        return ""
    if st == "blocked":
        return tr(uid, "access_blocked")
    if st == "expired":
        return tr(uid, "at_unavailable_expired")
    return tr(uid, "at_unavailable")

async def _notify_autotrade_api_error(uid: int, exchange: str, market_type: str, error_text: str) -> None:
    import time
    key = (int(uid), str(exchange or '').lower(), str(market_type or '').lower())
    now = time.time()
    last = AUTOTRADE_API_ERR_LAST.get(key, 0.0)
    if now - last < AUTOTRADE_API_ERR_COOLDOWN_SEC:
        return
    AUTOTRADE_API_ERR_LAST[key] = now
    # Minimal message; RU/EN based on user language
    title = tr(uid, "at_api_error_title")
    ex = (exchange or '').upper()
    mt = (market_type or '').upper()
    msg = f"{title} ({ex} {mt})\n{error_text}"
    try:
        await safe_send(uid, msg)
    except Exception:
        pass

def _fernet() -> Fernet:
    k = (os.getenv("AUTOTRADE_MASTER_KEY") or "").strip()
    if not k:
        raise RuntimeError("AUTOTRADE_MASTER_KEY env is missing")
    return Fernet(k.encode("utf-8"))

def _key_status_map(keys: List[Dict[str, any]]) -> Dict[str, Dict[str, any]]:
    out: Dict[str, Dict[str, any]] = {}
    for r in keys or []:
        ex = str(r.get("exchange") or "").lower()
        mt = str(r.get("market_type") or "").lower()
        out[f"{ex}:{mt}"] = r
    return out


def spot_priority_text(uid: int, pr: list[str], keys: list[dict] | None = None) -> str:
    """Render SPOT exchange priority screen.

    Shows priority order and whether keys are connected (active) per exchange.
    """
    names = {
        "binance": "Binance",
        "bybit": "Bybit",
        "okx": "OKX",
        "mexc": "MEXC",
        "gateio": "Gate.io",
    }
    km = _key_status_map(keys or [])
    def _conn_mark(ex: str) -> str:
        r = km.get(f"{ex}:spot") or {}
        return "✅" if bool(r.get("is_active")) else "➕"

    lines = [tr(uid, "at_spot_header"), "", "🏦 Приоритет бирж SPOT (1 → 5):"]
    for i, ex in enumerate(pr, 1):
        lines.append(f"{i}) {names.get(ex, ex)} {_conn_mark(ex)}")
    lines.append("")
    lines.append("Бот откроет сделку на первой бирже из вашего списка, которая:")
    lines.append("• есть в подтверждении сигнала (confirmations)")
    lines.append("• подключена у вас (есть API ключи SPOT)")
    lines.append("• ключи активны (проверены)")
    return "\n".join(lines)

def spot_priority_kb(uid: int, pr: list[str], keys: list[dict] | None = None) -> types.InlineKeyboardMarkup:
    """Keyboard for SPOT priority: per exchange shows (UP/DOWN) then (CONNECT/DISCONNECT)."""
    kb = InlineKeyboardBuilder()
    label = {"binance":"Binance","bybit":"Bybit","okx":"OKX","mexc":"MEXC","gateio":"Gate.io"}
    km = _key_status_map(keys or [])
    for ex in pr:
        # Row 1: priority
        kb.button(text=f"⬆️ {label.get(ex, ex)}", callback_data=f"at:prmove:spot:up:{ex}")
        kb.button(text=f"⬇️ {label.get(ex, ex)}", callback_data=f"at:prmove:spot:down:{ex}")
        # Row 2: connect/disconnect
        active = bool((km.get(f"{ex}:spot") or {}).get("is_active"))
        kb.button(text=f"✅ {label.get(ex, ex)}" if active else f"➕ {label.get(ex, ex)}", callback_data=f"at:keys:set:{ex}:spot")
        kb.button(text=tr(uid, "at_btn_disconnect"), callback_data=f"at:keysoff:spot:{ex}" if active else "at:noop")
    kb.adjust(2)
    kb.button(text=tr(uid, "btn_back"), callback_data="at:back")
    kb.adjust(2)
    return kb.as_markup()


def autotrade_kb(uid: int, s: Dict[str, any], keys: List[Dict[str, any]]) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    spot_on = bool(s.get("spot_enabled"))
    fut_on = bool(s.get("futures_enabled"))
    kb.button(text=(tr(uid, "at_spot_on") if spot_on else tr(uid, "at_spot_off")), callback_data="at:toggle:spot")
    kb.button(text=(tr(uid, "at_fut_on") if fut_on else tr(uid, "at_fut_off")), callback_data="at:toggle:futures")
    kb.adjust(2)

    # Settings
    kb.button(text=tr(uid, "at_spot_exchange"), callback_data="at:ex:spot")
    kb.button(text=tr(uid, "at_fut_exchange"), callback_data="at:ex:futures")
    kb.adjust(2)
    kb.button(text=tr(uid, "at_spot_amount"), callback_data="at:set:spot_amount")
    kb.button(text=tr(uid, "at_fut_margin"), callback_data="at:set:fut_margin")
    kb.adjust(2)
    kb.button(text=tr(uid, "at_fut_leverage"), callback_data="at:set:fut_leverage")
    kb.button(text=tr(uid, "at_fut_cap"), callback_data="at:set:fut_cap")
    kb.adjust(2)

    kb.button(text=tr(uid, "at_keys"), callback_data="at:keys")
    # Back to Auto-trade main screen
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:autotrade")
    kb.adjust(1)
    return kb.as_markup()


def autotrade_text(uid: int, s: Dict[str, any], keys: List[Dict[str, any]]) -> str:
    km = _key_status_map(keys)
    def ks(ex: str, mt: str) -> str:
        r = km.get(f"{ex}:{mt}")
        if not r or not bool(r.get("is_active")):
            return "❌"
        # Active key: show ✅ only if we have a successful validation timestamp and no error.
        if r.get("last_ok_at") and not r.get("last_error"):
            return "✅"
        return "⚠️"
    # SPOT: show connected exchanges in priority order (user may connect multiple).
    prio_csv = str(s.get("spot_exchange_priority") or "binance,bybit,okx,mexc,gateio")
    prio_all = [p.strip().lower() for p in prio_csv.split(",") if p.strip()]
    allowed_spot_all = ["binance","bybit","okx","mexc","gateio"]
    prio = [x for x in prio_all if x in allowed_spot_all]
    for x in allowed_spot_all:
        if x not in prio:
            prio.append(x)

    km2 = _key_status_map(keys)
    connected = [ex for ex in prio if bool((km2.get(f"{ex}:spot") or {}).get("is_active"))]
    # Render: if nothing connected -> keep legacy single exchange for clarity.
    if connected:
        pretty = {"binance":"Binance","bybit":"Bybit","okx":"OKX","mexc":"MEXC","gateio":"Gate.io"}
        spot_ex = " > ".join([pretty.get(ex, ex) for ex in connected])
    else:
        spot_ex = str(s.get("spot_exchange") or "binance")

    fut_ex = str(s.get("futures_exchange") or "binance")
    spot_amt = float(s.get("spot_amount_per_trade") or 0.0)
    fut_margin = float(s.get("futures_margin_per_trade") or 0.0)
    fut_lev = int(s.get("futures_leverage") or 1)
    fut_cap = float(s.get("futures_cap") or 0.0)

    # Build keys status block based on last signal confirmations (if available)
    lang = (LANG.get(int(uid), "ru") if isinstance(uid, int) else "ru")
    def _conf_set(sig_obj: Any) -> set[str]:
        if not sig_obj:
            return set()
        # Prefer explicit per-market venue list if present
        conf = str((getattr(sig_obj, "available_exchanges", "") or getattr(sig_obj, "confirmations", "") or "")).strip()
        if not conf:
            return set()
        return {p.strip().lower() for p in conf.split("+") if p.strip()}

    allowed_spot = ["binance", "bybit", "okx", "mexc", "gateio"]
    allowed_fut = ["binance", "bybit", "okx"]

    last_spot_sig = getattr(backend, "last_spot_signal", None) or LAST_SIGNAL_BY_MARKET.get("SPOT")
    last_fut_sig = getattr(backend, "last_futures_signal", None) or LAST_SIGNAL_BY_MARKET.get("FUTURES")

    spot_conf = _conf_set(last_spot_sig)
    fut_conf = _conf_set(last_fut_sig)

    # Parse user SPOT priority from settings (already stored as csv)
    csv_prio = str(s.get("spot_exchange_priority") or "binance,bybit,okx,mexc,gateio")
    prio = [p.strip().lower() for p in csv_prio.split(",") if p.strip()]
    prio = [x for x in prio if x in allowed_spot]
    for x in allowed_spot:
        if x not in prio:
            prio.append(x)

    def _eligible(ex: str, mt: str) -> bool:
        # Eligible for trading if key is active (✅ or ⚠️). ❌ means missing/inactive.
        return ks(ex, mt) != "❌"

    def _pick_spot_exchange() -> str | None:
        if not spot_conf:
            return None
        for ex in prio:
            if ex in spot_conf and _eligible(ex, "spot"):
                return ex
        return None

    def _pick_fut_exchange() -> str | None:
        if not fut_conf:
            return None
        ex = fut_ex.lower().strip()
        # Try user's selected exchange first, then any other eligible futures venue
        if ex in allowed_fut and ex in fut_conf and _eligible(ex, "futures"):
            return ex
        for alt in allowed_fut:
            if alt in fut_conf and _eligible(alt, "futures"):
                return alt
        return None

    # Render block
    parts: list[str] = []
    if spot_conf:
        items = [ex for ex in allowed_spot if ex in spot_conf]
        items_txt = " | ".join([f"{ex.upper()}: {ks(ex,'spot')}" for ex in items])
        parts.append(("SPOT подтверждение: " if lang == "ru" else "SPOT confirmed: ") + items_txt)
        chosen = _pick_spot_exchange()
        if chosen and bool(s.get("spot_enabled")):
            parts.append(("SPOT будет открыт на: " if lang == "ru" else "SPOT will open on: ") + chosen.upper())
        elif bool(s.get("spot_enabled")):
            parts.append(("SPOT: пропуск (нет ключей по confirmations)" if lang == "ru" else "SPOT: skipped (no keys for confirmations)"))
    else:
        items_txt = " | ".join([f"{ex.upper()}: {ks(ex,'spot')}" for ex in allowed_spot])
        parts.append(("SPOT ключи: " if lang == "ru" else "SPOT keys: ") + items_txt)

    if fut_conf:
        items = [ex for ex in allowed_fut if ex in fut_conf]
        items_txt = " | ".join([f"{ex.upper()}: {ks(ex,'futures')}" for ex in items])
        parts.append(("FUTURES подтверждение: " if lang == "ru" else "FUTURES confirmed: ") + items_txt)
        chosen = _pick_fut_exchange()
        if chosen and bool(s.get("futures_enabled")):
            parts.append(("FUTURES будет открыт на: " if lang == "ru" else "FUTURES will open on: ") + chosen.upper())
        elif bool(s.get("futures_enabled")):
            parts.append(("FUTURES: пропуск (нет ключей/не совпало подтверждение)" if lang == "ru" else "FUTURES: skipped (no keys/not confirmed)"))
    else:
        items_txt = " | ".join([f"{ex.upper()}: {ks(ex,'futures')}" for ex in allowed_fut])
        parts.append(("FUTURES ключи: " if lang == "ru" else "FUTURES keys: ") + items_txt)

    keys_status_block = "\n".join(parts).strip()

    return trf(
        uid,
        "at_screen",
        spot_state=(tr(uid, "at_state_on") if s.get("spot_enabled") else tr(uid, "at_state_off")),
        fut_state=(tr(uid, "at_state_on") if s.get("futures_enabled") else tr(uid, "at_state_off")),
        spot_ex=spot_ex,
        fut_ex=fut_ex,
        spot_amt=f"{spot_amt:g}",
        fut_margin=f"{fut_margin:g}",
        fut_lev=str(fut_lev),
        fut_cap=f"{fut_cap:g}",
        keys_status_block=keys_status_block,
        b_spot=ks("binance","spot"),
        b_fut=ks("binance","futures"),
        y_spot=ks("bybit","spot"),
        y_fut=ks("bybit","futures"),
    )

def _calc_effective_futures_cap(ui_cap: float, winrate: float | None) -> float:
    """Compute effective futures cap from UI cap and winrate.
    Base = ui_cap * 0.65, then apply winrate multiplier, clamp to ui_cap, round down to 10.
    If winrate is None -> assume 50.
    """
    cap = float(ui_cap or 0.0)
    if cap <= 0:
        return 0.0
    wr = 50.0 if winrate is None else float(winrate)

    base = cap * 0.70
    if wr < 40:
        k = 0.6
    elif wr < 50:
        k = 0.8
    elif wr < 60:
        k = 1.0
    elif wr < 70:
        k = 1.1
    else:
        k = 1.25

    eff = base * k
    eff = min(eff, cap)
    eff = math.floor(eff / 10.0) * 10.0
    return float(eff)



def autotrade_main_text(uid: int, s: Dict[str, any]) -> str:
    """Compact Auto-trade screen (main), per UX request."""
    # Keys list may be injected into state dict by the caller. Keep it optional.
    # We must NOT reference a free variable here (caused NameError in v4).
    keys: List[Dict[str, any]] = []
    try:
        v = s.get("keys") or s.get("autotrade_keys") or []
        if isinstance(v, list):
            keys = v
    except Exception:
        keys = []
    # SPOT: show connected exchanges in priority order (user may connect multiple).
    prio_csv = str(s.get("spot_exchange_priority") or "binance,bybit,okx,mexc,gateio")
    prio_all = [p.strip().lower() for p in prio_csv.split(",") if p.strip()]
    allowed_spot_all = ["binance","bybit","okx","mexc","gateio"]
    prio = [x for x in prio_all if x in allowed_spot_all]
    for x in allowed_spot_all:
        if x not in prio:
            prio.append(x)

    km2 = _key_status_map(keys)
    connected = [ex for ex in prio if bool((km2.get(f"{ex}:spot") or {}).get("is_active"))]
    # Render: if nothing connected -> keep legacy single exchange for clarity.
    if connected:
        pretty = {"binance":"Binance","bybit":"Bybit","okx":"OKX","mexc":"MEXC","gateio":"Gate.io"}
        spot_ex = " > ".join([pretty.get(ex, ex) for ex in connected])
    else:
        spot_ex = str(s.get("spot_exchange") or "binance").capitalize()
    fut_ex = str(s.get("futures_exchange") or "binance").capitalize()
    spot_amt = float(s.get("spot_amount_per_trade") or 0.0)
    fut_margin = float(s.get("futures_margin_per_trade") or 0.0)
    fut_lev = int(s.get("futures_leverage") or 1)
    fut_cap = float(s.get("futures_cap") or 0.0)
    wr = s.get("futures_winrate")
    try:
        wr = float(wr) if wr is not None else None
    except Exception:
        wr = None
    fut_cap_eff = _calc_effective_futures_cap(fut_cap, wr)
    closed_n = s.get('futures_closed_n', 0)
    try:
        closed_n = int(closed_n)
    except Exception:
        closed_n = 0
    # Variant B: after 5 closed FUTURES autotrade deals, minimum effective cap is 40
    if closed_n >= 5 and fut_cap_eff < 40.0:
        fut_cap_eff = 40.0
    spot_on = bool(s.get("spot_enabled"))
    fut_on = bool(s.get("futures_enabled"))
    spot_state = tr(uid, "at_state_on") if spot_on else tr(uid, "at_state_off")
    fut_state = tr(uid, "at_state_on") if fut_on else tr(uid, "at_state_off")
    title = tr(uid, "at_title")
    spot_header = tr(uid, "at_spot_header")
    fut_header = tr(uid, "at_futures_header")
    lbl_ex = tr(uid, "at_label_exchange")
    lbl_spot_amt = tr(uid, "at_label_spot_amount")
    lbl_fut_margin = tr(uid, "at_label_futures_margin")
    lbl_lev = tr(uid, "at_label_leverage")
    lbl_cap = tr(uid, "at_label_cap")
    cap_auto_spot = tr(uid, "at_cap_auto_spot")

    return (
        f"{title}\n\n"
        f"{spot_header}: {spot_state}\n"
        f"{lbl_ex}: {spot_ex}\n"
        f"{lbl_spot_amt}: {spot_amt:g} USDT\n"
        f"{lbl_cap}: {cap_auto_spot}\n\n"
        f"{fut_header}: {fut_state}\n"
        f"{lbl_ex}: {fut_ex}\n"
        f"{lbl_fut_margin}: {fut_margin:g} USDT\n"
        f"{lbl_lev}: {fut_lev}x\n"
        f"{tr(uid, 'at_label_cap_setting')}: {fut_cap:g} USDT\n"

        f"{tr(uid, 'at_label_effective_cap_now')}: {fut_cap_eff:g} USDT"
    )


def autotrade_main_kb(uid: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "at_menu_settings"), callback_data="atmenu:settings")
    kb.button(text=tr(uid, "at_menu_stats"), callback_data="atmenu:stats")
    kb.adjust(2)
    kb.button(text=tr(uid, "at_menu_info"), callback_data="atmenu:info")
    kb.adjust(1)
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:status")
    kb.adjust(1)
    return kb.as_markup()


def autotrade_info_kb(uid: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:autotrade")
    kb.adjust(1)
    return kb.as_markup()


def autotrade_stats_kb(uid: int, *, market_type: str, period: str) -> types.InlineKeyboardMarkup:
    mt = (market_type or "all").lower()
    pr = (period or "today").lower()
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "at_stats_type_spot") + (" ✅" if mt == "spot" else ""), callback_data="atstats:type:spot")
    kb.button(text=tr(uid, "at_stats_type_futures") + (" ✅" if mt == "futures" else ""), callback_data="atstats:type:futures")
    kb.button(text=tr(uid, "at_stats_type_all") + (" ✅" if mt == "all" else ""), callback_data="atstats:type:all")
    kb.adjust(3)
    kb.button(text=tr(uid, "at_stats_period_today") + (" ✅" if pr == "today" else ""), callback_data="atstats:period:today")
    kb.button(text=tr(uid, "at_stats_period_week") + (" ✅" if pr == "week" else ""), callback_data="atstats:period:week")
    kb.button(text=tr(uid, "at_stats_period_month") + (" ✅" if pr == "month" else ""), callback_data="atstats:period:month")
    kb.adjust(3)
    kb.button(text=tr(uid, "at_stats_refresh"), callback_data="atstats:refresh")
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:autotrade")
    kb.adjust(1)
    return kb.as_markup()


def _fmt_pnl_line(pnl: float, roi: float) -> str:
    sign = "+" if pnl > 0 else ("" if pnl < 0 else "")
    roi_sign = "+" if roi > 0 else ("" if roi < 0 else "")
    return f"{sign}{pnl:.2f} USDT ({roi_sign}{roi:.2f}%)"


def autotrade_keys_kb(uid: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    # SPOT supports multiple exchanges
    for ex in ("binance", "bybit", "okx", "mexc", "gateio"):
        kb.button(text=f"{ex.upper()} SPOT", callback_data=f"at:keys:set:{ex}:spot")
        kb.adjust(1)
    # FUTURES only Binance / Bybit
    kb.button(text="BINANCE FUTURES", callback_data="at:keys:set:binance:futures")
    kb.button(text="BYBIT FUTURES", callback_data="at:keys:set:bybit:futures")
    kb.adjust(2)
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:autotrade")
    kb.adjust(1)
    return kb.as_markup()




def _signal_text(uid: int, s: Signal, *, autotrade_hint: str = "") -> str:
    header = tr(uid, 'sig_spot_header') if s.market == 'SPOT' else tr(uid, 'sig_fut_header')
    market_banner = tr(uid, 'sig_spot_new') if s.market == 'SPOT' else tr(uid, 'sig_fut_new')
    # ⚡ MID tag (only for MID timeframe; does not affect old signals)
    if getattr(s, 'timeframe', '') == '5m/30m/1h':
        header = f"{tr(uid, 'sig_mid_trend_tag')}\n{header}"


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

    # Exchanges line: where the pair exists for the given market (executable venues).
    raw_avail = (getattr(s, 'available_exchanges', '') or '').strip() or (getattr(s, 'confirmations', '') or '')
    ex_line_raw = _fmt_exchanges(raw_avail)
    exchanges_line = f"{tr(uid, 'sig_exchanges')}: {ex_line_raw}\n" if ex_line_raw else ""
    # Keep TF on its own line; timeframe string already like 15m/1h/4h
    tf_line = f"{tr(uid, 'sig_tf')}: {s.timeframe}"

    tp_lines = "\n".join(_tp_lines())

    rr_line = f"{tr(uid, 'sig_rr')}: 1:{s.rr:.2f}"
    conf_line = f"{tr(uid, 'sig_confidence')}: {s.confidence}/100"
    # Confirm line: primary venue that produced the signal.
    src_ex = (getattr(s, 'source_exchange', '') or '').strip()
    confirm_line = f"{tr(uid, 'sig_confirm')}: {src_ex}"

    autotrade_line = f"{tr(uid, 'sig_autotrade')}: {autotrade_hint}\n" if autotrade_hint else ""

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
        autotrade_line=autotrade_line,
        risk_block=risk_block,
        open_prompt=tr(uid, 'sig_open_prompt')
    )

def _fmt_hhmm(ts_utc: float) -> str:
    d = dt.datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC")).astimezone(TZ)
    return d.strftime("%H:%M")

def _fmt_dt_msk(v) -> str:
    """Format datetime as Moscow time (TZ / Europe/Moscow)."""
    try:
        if not v:
            return "—"
        if isinstance(v, dt.datetime):
            d = v
        elif isinstance(v, str):
            s = v.strip()
            if not s:
                return "—"
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            d = dt.datetime.fromisoformat(s)
        else:
            return "—"

        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        d = d.astimezone(TZ)
        return d.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "—"

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
    raw_avail = (getattr(sig, "available_exchanges", "") or "").strip() or (getattr(sig, "confirmations", "") or "")
    exchanges = str(raw_avail).replace("+", " • ")
    if not exchanges:
        exchanges = tr(uid, "status_next_macro_none")

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
async def broadcast_signal(sig: Signal) -> None:
    # Global pause/maintenance for sending NEW signals (Signal Bot admin setting)
    try:
        st = await db_store.get_signal_bot_settings()
        if bool(st.get('pause_signals')):
            logger.info("Signal broadcasting is paused (pause_signals=true). Skipping new signal %s %s", sig.market, sig.symbol)
            return
        # Maintenance mode: do NOT send new signals to users.
        # IMPORTANT: we also keep LAST_SIGNAL_BY_MARKET unchanged so users can still open the last
        # live signal that was broadcasted BEFORE maintenance was enabled.
        if bool(st.get('maintenance_mode')):
            logger.info("Signal broadcasting is in maintenance mode. Skipping new signal %s %s", sig.market, sig.symbol)
            return
    except Exception:
        # best effort: if settings table not ready, don't block
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
        sid = await db_store.next_signal_id()
        sig = replace(sig, signal_id=sid)
    except Exception as e:
        # Fallback: derive a stable non-zero signal_id from sig_key so:
        # - tracking rows (signal_tracks) still exist
        # - open/close callbacks still work across restarts
        # This keeps "Signals closed (outcomes)" dashboard functional even if DB sequence is unavailable.
        logger.error("Failed to allocate signal_id from DB sequence: %s", e)
        try:
            if not getattr(sig, "signal_id", 0):
                # Take first 8 bytes of SHA1(sig_raw) -> uint64 -> fit into BIGINT safely.
                _h = bytes.fromhex(sig_key)[:8]
                sid2 = int.from_bytes(_h, "big", signed=False)
                if sid2 == 0:
                    sid2 = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
                sig = replace(sig, signal_id=sid2)
        except Exception:
            pass
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

    # Persist bot-level signal tracker (independent from users) for outcomes (TP/SL/BE) statistics.
    try:
        side_stats = _normalize_side_for_stats(getattr(sig, 'direction', ''))
        await db_store.upsert_signal_track(
            signal_id=int(sig.signal_id or 0),
            sig_key=sig_key,
            market=str(sig.market).upper(),
            symbol=str(sig.symbol),
            side=side_stats,
            entry=float(sig.entry or 0.0),
            tp1=(float(sig.tp1) if sig.tp1 is not None else None),
            tp2=(float(sig.tp2) if sig.tp2 is not None else None),
            sl=(float(sig.sl) if sig.sl is not None else None),
        )
    except Exception:
        pass
    ORIGINAL_SIGNAL_TEXT[(0, sig.signal_id)] = _signal_text(0, sig)
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(0, "btn_opened"), callback_data=f"open:{sig.signal_id}")

    uids = await get_broadcast_user_ids()
    for uid in uids:
        try:
            # Per-user Auto-trade hint: show whether Auto-trade is enabled and which exchange is selected.
            at_hint = ""
            try:
                st = await db_store.get_autotrade_settings(uid)
                mkt = str(sig.market or "FUTURES").upper().strip()
                if mkt == "SPOT":
                    en = bool(st.get("spot_enabled"))
                    ex = str(st.get("spot_exchange") or "").upper().strip()
                else:
                    en = bool(st.get("futures_enabled"))
                    ex = str(st.get("futures_exchange") or "").upper().strip()
                at_hint = ("🟢 " + ex) if (en and ex) else "🔴 OFF"
            except Exception:
                at_hint = ""

            ORIGINAL_SIGNAL_TEXT[(uid, sig.signal_id)] = _signal_text(uid, sig, autotrade_hint=at_hint)
            kb_u = InlineKeyboardBuilder()
            kb_u.button(text=tr(uid, "btn_opened"), callback_data=f"open:{sig.signal_id}")
            await safe_send(uid, _signal_text(uid, sig, autotrade_hint=at_hint), reply_markup=kb_u.as_markup())

            # Auto-trade: execute real orders asynchronously, without affecting broadcast.
            async def _run_autotrade(_uid: int, _sig: Signal):
                try:
                    # Global auto-trade pause/maintenance (admin panel)
                    try:
                        gst = await db_store.get_autotrade_bot_settings()
                        if bool(gst.get("pause_autotrade")) or bool(gst.get("maintenance_mode")):
                            logger.info("Auto-trade skipped by global setting (pause/maintenance) uid=%s", _uid)
                            return
                    except Exception:
                        # FAIL-CLOSED: if DB read failed, do NOT open new trades.
                        logger.exception("Auto-trade skipped: failed to read global pause/maintenance (fail-closed) uid=%s", _uid)
                        return

                    res = await autotrade_execute(_uid, _sig)
                    err = res.get("api_error") if isinstance(res, dict) else None
                    if err:
                        # notify ONLY for API errors
                        st = await db_store.get_autotrade_settings(_uid)
                        mt = "spot" if _sig.market == "SPOT" else "futures"
                        ex = str(st.get("spot_exchange" if mt == "spot" else "futures_exchange") or "").lower()
                        await _notify_autotrade_api_error(_uid, ex, mt, str(err)[:500])
                except Exception as e:
                    # unexpected errors are treated as API errors for visibility
                    st = await db_store.get_autotrade_settings(_uid)
                    mt = "spot" if _sig.market == "SPOT" else "futures"
                    ex = str(st.get("spot_exchange" if mt == "spot" else "futures_exchange") or "").lower()
                    await _notify_autotrade_api_error(_uid, ex, mt, f"{type(e).__name__}: {e}"[:500])

            asyncio.create_task(_run_autotrade(uid, sig))
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
        # NOTE: do not auto-unblock user on status view

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

    await message.answer(await status_text(uid, include_subscribed=True, include_hint=True), reply_markup=menu_kb(uid))


# ---------------- language selection ----------------

@dp.message(Command("autotrade_health"))
async def cmd_autotrade_health(message: types.Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        return
    snap = await autotrade_healthcheck()
    open_cnt = int(snap.get("open_positions") or 0)
    stale_cnt = int(snap.get("stale_reserved_positions") or 0)
    sm = int(snap.get("stale_minutes") or os.getenv("AUTOTRADE_STALE_MINUTES", "10") or 10)
    lines = [
        "🩺 *Autotrade health*",
        f"Open positions: `{open_cnt}`",
        f"Stale RESERVED (> {sm} min): `{stale_cnt}`",
    ]
    if stale_cnt and snap.get("stale_samples"):
        lines.append("")
        lines.append("*Samples:*")
        for r in (snap.get("stale_samples") or [])[:5]:
            lines.append(f"- id={r.get('id')} u={r.get('user_id')} {r.get('exchange')} {r.get('market_type')} {r.get('symbol')} {r.get('api_order_ref')}")
    await message.answer("\n".join(lines), parse_mode="Markdown")


@dp.message(Command("autotrade_stress"))
async def cmd_autotrade_stress(message: types.Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        return

    # Usage: /autotrade_stress [SYMBOL] [spot|futures] [N]
    parts = (message.text or "").strip().split()
    symbol = parts[1] if len(parts) >= 2 else "BTCUSDT"
    market = parts[2] if len(parts) >= 3 else "SPOT"
    try:
        n = int(parts[3]) if len(parts) >= 4 else 50
    except Exception:
        n = 50

    await message.answer(f"⏱ Running stress-test (dry-run): {symbol} {market} x{n} ...")
    res = await autotrade_stress_test(admin_user_id=uid, symbol=symbol, market=market, n=n)
    health = res.get("health") or {}
    errs = res.get("errors") or []
    txt = [
        "✅ *Stress-test finished (DRY-RUN)*",
        f"symbol: `{res.get('symbol')}` market: `{res.get('market')}`",
        f"tasks: `{res.get('n')}` ok_results: `{res.get('ok_results')}`",
        f"errors: `{len(errs)}`",
        f"health.open_positions: `{health.get('open_positions')}`",
        f"health.stale_reserved_positions: `{health.get('stale_reserved_positions')}`",
    ]
    if errs:
        txt.append("")
        txt.append("*First errors:*")
        for e in errs[:5]:
            txt.append(f"- `{e}`")
    await message.answer("\n".join(txt), parse_mode="Markdown")

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
            await bot.edit_message_text(chat_id=uid, message_id=call.message.message_id, text=await status_text(uid, include_subscribed=True, include_hint=True), reply_markup=menu_kb(uid))
            return
    except Exception:
        pass
    await safe_send(uid, await status_text(uid, include_subscribed=True, include_hint=True), reply_markup=menu_kb(uid))

# ---------------- menu callbacks ----------------
def _fmt_stats_block_ru(uid: int, title: str, b: dict | None = None) -> str:
    b = b or {}
    trades = int(b.get("trades", 0))
    wins = int(b.get("wins", 0))
    losses = int(b.get("losses", 0))
    be = int(b.get("be", 0))
    tp1 = int(b.get("tp1_hits", 0))
    # WinRate: WIN/(WIN+LOSS)
    denom = max(1, wins + losses)
    wr = (wins / denom) * 100.0
    # NOTE: stats buckets use 'sum_pnl_pct' (sum of per-trade PnL%),
    # not 'pnl_pct'. Using the wrong key makes PnL always show 0.00%.
    pnl = float(b.get("sum_pnl_pct", 0.0))

    return (
        f"{title}\n"
        f"{tr(uid, 'lbl_trades')}: {trades} | {tr(uid, 'lbl_tp2')}: {wins} | {tr(uid, 'lbl_sl')}: {losses} | "
        f"{tr(uid, 'lbl_be')}: {be} | {tr(uid, 'lbl_tp1')}: {tp1}\n"
        f"{tr(uid, 'lbl_winrate')}: {wr:.1f}%\n"
        f"{tr(uid, 'lbl_pnl')}: {pnl:+.2f}%"
    )


def _fmt_stats_block_en(title: str, b: dict | None = None) -> str:
    b = b or {}
    trades = int(b.get("trades", 0))
    wins = int(b.get("wins", 0))
    losses = int(b.get("losses", 0))
    be = int(b.get("be", 0))
    tp1 = int(b.get("tp1_hits", 0))
    # WinRate should be based on WIN/(WIN+LOSS). BE does not affect WinRate.
    denom = (wins + losses)
    wr = (wins / denom * 100.0) if denom else 0.0
    pnl = float(b.get("sum_pnl_pct", 0.0))
    return (
        f"{title}\n"
        f"Trades: {trades} | TP2: {wins} | SL: {losses} | BE: {be} | TP1: {tp1}\n"
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

async def status_text(uid: int, *, include_subscribed: bool = False, include_hint: bool = False) -> str:
    """Build a pretty status text used both on /start and on the 'Status' screen."""
    # Notifications toggle state (per user)
    enabled: Optional[bool] = None
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
    notif_state = tr(uid, "status_notif_on") if enabled is True else (tr(uid, "status_notif_off") if enabled is False else tr(uid, "status_unknown"))

    # Next macro (if known)
    next_macro = tr(uid, "status_next_macro_none")
    try:
        nm = backend.get_next_macro()
    except Exception:
        nm = None
    if nm:
        try:
            ev, (w0, w1) = nm
            name = getattr(ev, "name", None) or getattr(ev, "type", None) or "-"
            hhmm = _fmt_hhmm(getattr(ev, "start_ts_utc", time.time()))
            next_macro = f"{name} {hhmm}"
        except Exception:
            pass

    out_lines: List[str] = [tr(uid, "status_title")]

    if include_subscribed:
        out_lines.append("")
        out_lines.append(tr(uid, "welcome_subscribed"))

    # Show user id (easy to copy)
    if uid:
        out_lines.append(trf(uid, 'status_user_id', id=uid))

    out_lines.append("")
    out_lines.append(trf(uid, "status_scanner", state=scanner_state))

    # News and Macro each on its own line. No space-padding: Telegram uses proportional fonts,
    # and "alignment with spaces" will always drift between clients.
    news_icon = "🟢" if _is_allow(news_stat.get("action")) else "🔴"
    out_lines.append(f"{trf(uid, 'status_news', state=news_state)}{news_icon}")
    out_lines.append(trf(uid, 'status_macro', state=macro_state, icon=macro_icon))

    # News details when blocked
    if not _is_allow(news_stat.get("action")):
        reason = news_stat.get("reason")
        if reason:
            out_lines.append(trf(uid, "status_news_reason", reason=reason))
        until_ts = news_stat.get("until_ts")
        if until_ts:
            left = _fmt_countdown(float(until_ts) - time.time())
            out_lines.append(trf(uid, "status_news_timer", left=left))

    # (macro line is already included above)

    # Macro details when blocked
    if not _is_allow(macro_action):
        try:
            reason = macro_stat.get("reason")
            if reason:
                out_lines.append(trf(uid, "status_macro_reason", reason=reason))
            win = macro_stat.get("window")
            if win and isinstance(win, (tuple, list)) and len(win) == 2:
                w0, w1 = float(win[0]), float(win[1])
                out_lines.append(trf(uid, "status_macro_window", before=_fmt_hhmm(w0), after=_fmt_hhmm(w1)))
            until_ts = macro_stat.get("until_ts")
            if until_ts:
                left = _fmt_countdown(float(until_ts) - time.time())
                out_lines.append(trf(uid, "status_macro_timer", left=left))
            # Timer to end of blackout window (more explicit)
            if not until_ts and win and isinstance(win, (tuple, list)) and len(win) == 2:
                until_ts = float(win[1])
            if until_ts:
                left_end = _fmt_countdown(float(until_ts) - time.time())
                out_lines.append(trf(uid, "status_macro_blackout_timer", left=left_end))
        except Exception:
            pass

    out_lines.append(trf(uid, "status_next_macro", value=(next_macro or tr(uid, "status_next_macro_none"))))

    # Signal bot global state (pause / maintenance) from admin dashboard
    try:
        sb = await db_store.get_signal_bot_settings()
        if bool(sb.get("maintenance_mode")):
            sig_state = tr(uid, "sig_state_maintenance")
        elif bool(sb.get("pause_signals")):
            sig_state = tr(uid, "sig_state_paused")
        else:
            sig_state = tr(uid, "sig_state_on")
        out_lines.append(trf(uid, "status_signals", state=sig_state))
    except Exception:
        # If settings are unavailable, just skip the line
        pass

    out_lines.append(trf(uid, "status_notifications", state=notif_state))

    if include_hint:
        out_lines.append("")
        out_lines.append(tr(uid, "welcome_hint"))

    return "\n".join(out_lines)

async def stats_text(uid: int) -> str:
    """Render trading statistics for the user from Postgres (backend-only)."""
    # Used by nested helpers below
    lang = get_lang(uid)
    # IMPORTANT: use the bot's configured IANA timezone (TZ_NAME), not env "TZ".
    # Many servers/users set TZ to aliases like "MSK" which are not valid for Postgres AT TIME ZONE.
    tz_name = TZ_NAME

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
        return _fmt_stats_block_ru(uid, title, b)

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
            losses = int(r.get("losses", 0) or 0)
            pnl = float(r.get("sum_pnl_pct", 0.0) or 0.0)
            denom = (wins + losses)
            wr = (wins / denom * 100.0) if denom else 0.0
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
        # NOTE: do not auto-unblock user on status view

        txt = await status_text(uid, include_subscribed=False, include_hint=False)
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

        try:
            txt = await stats_text(uid)
        except Exception as e:
            logger.exception("menu:stats failed for uid=%s", uid)
            txt = tr(uid, "stats_error") if ("stats_error" in I18N.get(get_lang(uid), {})) else ("Ошибка статистики. Попробуй позже.")
            # Append a short technical hint for admins only
            if _is_admin(uid):
                txt += f"\n\nERR: {type(e).__name__}: {e}"
        await safe_edit(call.message, txt, menu_kb(uid))
        return

    # ---- LIVE SIGNALS ----
    if action in ("spot", "futures"):
        market = "SPOT" if action == "spot" else "FUTURES"

        # If maintenance mode is enabled, do not show live signal cards.
        # Users should see a clear explanation instead of confusing "no live".
        try:
            st = await db_store.get_signal_bot_settings()
            if bool(st.get('maintenance_mode')):
                await safe_edit(call.message, tr(uid, "sig_maintenance_live"), menu_kb(uid))
                return
        except Exception:
            pass

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
            # NOTE: do not auto-unblock user on /start
            enabled = await get_notify_signals(uid)
            state = tr(uid, "notify_status_on") if enabled else tr(uid, "notify_status_off")
            desc = tr(uid, "notify_desc_on") if enabled else tr(uid, "notify_desc_off")
            txt = trf(uid, "notify_screen", title=tr(uid, "notify_title"), state=state, desc=desc)
            await safe_send(uid, txt, reply_markup=notify_kb(uid, enabled))
        return

    # ---- AUTO-TRADE ----
    if action == "autotrade":
        # Global auto-trade pause/maintenance (admin)
        gt = _autotrade_gate_text(uid)
        if gt:
            await safe_edit(call.message, gt, menu_kb(uid))
            return
        # Per-user subscription gate (admin: autotrade_enabled / autotrade_expires_at)
        ut = await _autotrade_unavailable_text(uid)
        if ut:
            await safe_edit(call.message, ut, menu_kb(uid))
            return

        try:
            st = await db_store.get_autotrade_settings(uid)
        except Exception:
            st = {
                "spot_enabled": False,
                "futures_enabled": False,
                "spot_exchange": "binance",
                "futures_exchange": "binance",
                "spot_amount_per_trade": 0.0,
                "futures_margin_per_trade": 0.0,
                "futures_leverage": 1,
                "futures_cap": 0.0,
            }
        # Attach winrate for UI / effective cap (best-effort)
        try:
            fn = getattr(db_store, "get_autotrade_winrate", None)
            if fn:
                st["futures_winrate"] = await fn(user_id=uid, market_type="futures", last_n=10)
        except Exception:
            st["futures_winrate"] = None

        # Attach autotrade keys status so the main screen can show connected SPOT exchanges.
        # This is best-effort; UI should still render if DB query fails.
        try:
            st["keys"] = await db_store.get_autotrade_keys_status(uid)
        except Exception:
            st["keys"] = []

        await safe_edit(call.message, autotrade_main_text(uid, st), autotrade_main_kb(uid))
        return

    # fallback
    await safe_edit(call.message, await status_text(uid, include_subscribed=True, include_hint=True), menu_kb(uid))


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
        await safe_edit(call.message, await status_text(uid, include_subscribed=True, include_hint=True), menu_kb(uid))
        return

    if action == "set":
        # Explicit ON/OFF
        value = (parts[2] if len(parts) > 2 else "").lower()
        await ensure_user(uid)
        # NOTE: do not auto-unblock user on /start
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
        await safe_send(call.from_user.id, _signal_text(uid, sig), reply_markup=kb.as_markup())
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


# ---------------- auto-trade callbacks ----------------
@dp.callback_query(lambda c: (c.data or "").startswith("at:"))
async def autotrade_callback(call: types.CallbackQuery) -> None:
    await call.answer()
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return
    async with _user_lock(uid):

        # Shared access control
        access = await get_access_status(uid)
        if access != "ok":
            await safe_edit(call.message, tr(uid, f"access_{access}"), menu_kb(uid))
            return

        gt = _autotrade_gate_text(uid)
        if gt:
            await safe_edit(call.message, gt, menu_kb(uid))
            return

        ut = await _autotrade_unavailable_text(uid)
        if ut:
            await safe_edit(call.message, ut, menu_kb(uid))
            return

        parts = (call.data or "").split(":")
        # at:toggle:spot | at:toggle:futures | at:set:* | at:ex:* | at:keys | at:keys:set:EX:MT
        action = parts[1] if len(parts) > 1 else ""

        # Load current state
        st = await db_store.get_autotrade_settings(uid)
        keys = await db_store.get_autotrade_keys_status(uid)

        if action == "toggle" and len(parts) >= 3:
            mt = parts[2]
            if mt in ("spot", "futures"):
                await db_store.toggle_autotrade_toggle(uid, mt)
            st = await db_store.get_autotrade_settings(uid)
            await safe_edit(call.message, autotrade_text(uid, st, keys), autotrade_kb(uid, st, keys))
            return
        if action == "keys":
            # at:keys or at:keys:set:ex:mt
            if len(parts) == 2:
                await safe_edit(call.message, tr(uid, "at_keys_screen"), autotrade_keys_kb(uid))
                return
            if len(parts) >= 5 and parts[2] == "set":
                ex = parts[3]
                mt = parts[4]
                AUTOTRADE_INPUT[uid] = {"mode": "keys", "exchange": ex, "market_type": mt, "step": "api_key"}
                await safe_send(uid, trf(uid, "at_enter_api_key", exchange=ex.upper(), market=mt.upper()))
                return

        if action == "set" and len(parts) >= 3:
            field = parts[2]
            if field == "spot_amount":
                AUTOTRADE_INPUT[uid] = {"mode": "set", "field": "spot_amount"}
                await safe_send(uid, tr(uid, "at_enter_spot_amount"))
                return
            if field == "fut_margin":
                AUTOTRADE_INPUT[uid] = {"mode": "set", "field": "fut_margin"}
                await safe_send(uid, tr(uid, "at_enter_fut_margin"))
                return
            if field == "fut_leverage":
                AUTOTRADE_INPUT[uid] = {"mode": "set", "field": "fut_leverage"}
                await safe_send(uid, tr(uid, "at_enter_fut_leverage"))
                return
            if field == "fut_cap":
                AUTOTRADE_INPUT[uid] = {"mode": "set", "field": "fut_cap"}
                await safe_send(uid, tr(uid, "at_enter_fut_cap"))
                return

        if action == "ex" and len(parts) >= 3:
            mt = parts[2]
            if mt == "spot":
                pr = await db_store.get_spot_exchange_priority(uid)
                keys2 = await db_store.get_autotrade_keys_status(uid)
                await safe_edit(call.message, spot_priority_text(uid, pr, keys2), spot_priority_kb(uid, pr, keys2))
                return
            # futures exchange choices (only Binance/Bybit)
            kb = InlineKeyboardBuilder()
            kb.button(text="Binance", callback_data=f"at:exset:{mt}:binance")
            kb.button(text="Bybit", callback_data=f"at:exset:{mt}:bybit")
            kb.adjust(2)
            kb.button(text=tr(uid, "btn_back"), callback_data="menu:autotrade")
            kb.adjust(1)
            await safe_edit(call.message, tr(uid, "at_choose_exchange"), kb.as_markup())
            return

        # SPOT exchange priority move (up/down) for 5 exchanges
        # callback: at:prmove:spot:up:<exchange> or at:prmove:spot:down:<exchange>
        if action == "prmove" and len(parts) >= 5:
            mt = parts[2]
            direction = parts[3]
            ex = parts[4]
            if mt != "spot":
                return
            pr = await db_store.get_spot_exchange_priority(uid)
            ex = (ex or "").lower().strip()
            if ex in pr:
                i = pr.index(ex)
                if direction == "up" and i > 0:
                    pr[i - 1], pr[i] = pr[i], pr[i - 1]
                elif direction == "down" and i < len(pr) - 1:
                    pr[i + 1], pr[i] = pr[i], pr[i + 1]
                await db_store.set_spot_exchange_priority(uid, pr)
            # re-render
            pr2 = await db_store.get_spot_exchange_priority(uid)
            keys2 = await db_store.get_autotrade_keys_status(uid)
            await safe_edit(call.message, spot_priority_text(uid, pr2, keys2), spot_priority_kb(uid, pr2, keys2))
            return

        # Disable (disconnect) SPOT keys for a specific exchange (user action)
        # callback: at:keysoff:spot:<exchange>
        if action == "keysoff" and len(parts) >= 4:
            mt = parts[2]
            ex = (parts[3] if len(parts) > 3 else "").lower().strip()
            if mt != "spot":
                return
            try:
                await db_store.disable_autotrade_key(user_id=uid, exchange=ex, market_type="spot")
            except Exception:
                pass
            pr2 = await db_store.get_spot_exchange_priority(uid)
            keys2 = await db_store.get_autotrade_keys_status(uid)
            await safe_edit(call.message, spot_priority_text(uid, pr2, keys2), spot_priority_kb(uid, pr2, keys2))
            return

        # No-op button (used when "disconnect" is pressed for not-connected exchange)
        if action == "noop":
            return

        if action == "exset" and len(parts) >= 4:
            mt = parts[2]
            ex = parts[3]
            await db_store.set_autotrade_exchange(uid, "spot" if mt == "spot" else "futures", ex)
            st = await db_store.get_autotrade_settings(uid)
            await safe_edit(call.message, autotrade_text(uid, st, keys), autotrade_kb(uid, st, keys))
            return

        # default: return to autotrade screen
        st = await db_store.get_autotrade_settings(uid)
        keys = await db_store.get_autotrade_keys_status(uid)
        await safe_edit(call.message, autotrade_text(uid, st, keys), autotrade_kb(uid, st, keys))

@dp.callback_query(lambda c: (c.data or "").startswith("atmenu:"))
async def autotrade_menu_subscreens(call: types.CallbackQuery) -> None:
    await call.answer()
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    access = await get_access_status(uid)
    if access != "ok":
        await safe_edit(call.message, tr(uid, f"access_{access}"), menu_kb(uid))
        return

    gt = _autotrade_gate_text(uid)
    if gt:
        await safe_edit(call.message, gt, menu_kb(uid))
        return

    ut = await _autotrade_unavailable_text(uid)
    if ut:
        await safe_edit(call.message, ut, menu_kb(uid))
        return

    action = (call.data or "").split(":", 1)[1]
    if action == "settings":
        st = await db_store.get_autotrade_settings(uid)
        keys = await db_store.get_autotrade_keys_status(uid)
        await safe_edit(call.message, autotrade_text(uid, st, keys), autotrade_kb(uid, st, keys))
        return

    if action == "stats":
        # initialize state
        AUTOTRADE_STATS_STATE.setdefault(uid, {"market_type": "all", "period": "today"})
        state = AUTOTRADE_STATS_STATE[uid]
        stats = await db_store.get_autotrade_stats(user_id=uid, market_type=state["market_type"], period=state["period"])
        txt = _render_autotrade_stats_text(uid, state["market_type"], state["period"], stats)
        await safe_edit(call.message, txt, autotrade_stats_kb(uid, market_type=state["market_type"], period=state["period"]))
        return

    if action == "info":
        await safe_edit(call.message, tr(uid, "at_info_text"), autotrade_info_kb(uid))
        return

    if action == "faq":
        await safe_edit(call.message, autotrade_faq_text(uid), autotrade_faq_kb(uid))
        return

    await safe_edit(call.message, await status_text(uid, include_subscribed=True, include_hint=True), menu_kb(uid))


def autotrade_info_kb(uid: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "at_menu_faq"), callback_data="atmenu:faq")
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:autotrade")
    kb.adjust(1)
    return kb.as_markup()


def autotrade_faq_text(uid: int) -> str:
    """FAQ text from i18n."""
    title = tr(uid, 'at_faq_title')
    body = tr(uid, 'at_faq_text')
    # Optional support line
    support = f"\n\n@{SUPPORT_USERNAME}" if SUPPORT_USERNAME else ''
    return f"{title}\n\n{body}{support}"


def autotrade_faq_kb(uid: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "btn_back"), callback_data="atmenu:info")
    kb.adjust(1)
    return kb.as_markup()


def _render_autotrade_stats_text(uid: int, market_type: str, period: str, stats: Dict[str, any]) -> str:
    mt = (market_type or "all").lower()
    pr = (period or "today").lower()
    mt_label = "🌕 SPOT" if mt == "spot" else ("⚡ FUTURES" if mt == "futures" else "📊 ВСЕ")
    pr_label = "Сегодня" if pr == "today" else ("Неделя" if pr == "week" else "Месяц")

    opened = int(stats.get("opened") or 0)
    closed_plus = int(stats.get("closed_plus") or 0)
    closed_minus = int(stats.get("closed_minus") or 0)
    active = int(stats.get("active") or 0)
    pnl_total = float(stats.get("pnl_total") or 0.0)
    roi = float(stats.get("roi_percent") or 0.0)

    return (
        "📊 Auto-trade | Статистика\n"
        f"🔍 Тип: {mt_label}\n"
        f"📅 Период: {pr_label}\n\n"
        f"📂 Открыто сделок: {opened}\n"
        f"✅ Закрыто в плюс: {closed_plus}\n"
        f"❌ Закрыто в минус: {closed_minus}\n"
        f"⏳ Активных сейчас: {active}\n\n"
        "💰 PnL (USDT / %):\n"
        f"• Итог: {_fmt_pnl_line(pnl_total, roi)}"
    )


@dp.callback_query(lambda c: (c.data or "").startswith("atstats:"))
async def autotrade_stats_callback(call: types.CallbackQuery) -> None:
    await call.answer()
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    access = await get_access_status(uid)
    if access != "ok":
        await safe_edit(call.message, tr(uid, f"access_{access}"), menu_kb(uid))
        return

    state = AUTOTRADE_STATS_STATE.setdefault(uid, {"market_type": "all", "period": "today"})
    parts = (call.data or "").split(":")
    # atstats:type:spot | atstats:period:today | atstats:refresh
    if len(parts) >= 2:
        if parts[1] == "type" and len(parts) >= 3:
            mt = parts[2].lower()
            if mt in ("spot", "futures", "all"):
                state["market_type"] = mt
        elif parts[1] == "period" and len(parts) >= 3:
            pr = parts[2].lower()
            if pr in ("today", "week", "month"):
                state["period"] = pr
        # refresh -> no state change

    stats = await db_store.get_autotrade_stats(user_id=uid, market_type=state["market_type"], period=state["period"])
    txt = _render_autotrade_stats_text(uid, state["market_type"], state["period"], stats)
    await safe_edit(call.message, txt, autotrade_stats_kb(uid, market_type=state["market_type"], period=state["period"]))


@dp.message()
async def autotrade_input_handler(message: types.Message) -> None:
    """Handle text input for Auto-trade configuration.

    We only consume messages when the user is in AUTOTRADE_INPUT state.
    """
    uid = message.from_user.id if message.from_user else 0
    if not uid:
        return
    async with _user_lock(uid):
        state = AUTOTRADE_INPUT.get(uid)
        if not state:
            return

        # Access check
        access = await get_access_status(uid)
        if access != "ok":
            AUTOTRADE_INPUT.pop(uid, None)
            await message.answer(tr(uid, f"access_{access}"), reply_markup=menu_kb(uid))
            return

        text = (message.text or "").strip()
        if not text:
            return

        if state.get("mode") == "set":
            field = state.get("field") or ""
            success = False
            try:
                if field in ("spot_amount", "fut_margin", "fut_cap"):
                    val = float(text.replace(",", "."))
                    if val < 0:
                        raise ValueError
                    if field == "spot_amount":
                        # Hard minimum: SPOT amount per trade must be >= 15 USDT
                        if val < 15:
                            await message.answer(tr(uid, "at_min_spot_amount"))
                            return
                        await db_store.set_autotrade_amount(uid, "spot", val)
                    elif field == "fut_margin":
                        # Hard minimum: FUTURES margin per trade must be >= 10 USDT
                        if val < 10:
                            await message.answer(tr(uid, "at_min_fut_margin"))
                            return
                        await db_store.set_autotrade_amount(uid, "futures", val)
                    elif field == "fut_cap":
                        await db_store.set_autotrade_futures_cap(uid, val)
                elif field == "fut_leverage":
                    lev = int(float(text.replace(",", ".")))
                    if lev < 1 or lev > 125:
                        raise ValueError
                    await db_store.set_autotrade_futures_leverage(uid, lev)
                else:
                    # unknown field
                    return
                success = True
            except Exception:
                # Keep simple: ask again
                await message.answer(tr(uid, "bad_number"))
                return

            if success:
                AUTOTRADE_INPUT.pop(uid, None)

            st = await db_store.get_autotrade_settings(uid)
            keys = await db_store.get_autotrade_keys_status(uid)
            await message.answer(autotrade_text(uid, st, keys), reply_markup=autotrade_kb(uid, st, keys))
            return

        if state.get("mode") == "keys":
            ex = (state.get("exchange") or "binance").lower()
            mt = (state.get("market_type") or "spot").lower()
            step = state.get("step") or "api_key"

            if step == "passphrase":
                api_key = (state.get("api_key") or "").strip()
                api_secret = (state.get("api_secret") or "").strip()
                passphrase = text
                AUTOTRADE_INPUT.pop(uid, None)

                # Validate (READ + TRADE). If TRADE missing -> reject.
                try:
                    res = await validate_autotrade_keys(exchange=ex, market_type=mt, api_key=api_key, api_secret=api_secret, passphrase=passphrase)
                except Exception as e:
                    res = {"ok": False, "read_ok": False, "trade_ok": False, "error": str(e)}

                raw_err = str(res.get("error") or "")
                if not bool(res.get("ok")):
                    # map error
                    await message.answer(trf(uid, "at_keys_api_error", exchange=ex.upper(), market=mt.upper()) + ("\n\n" + raw_err[:200] if raw_err else ""))
                    try:
                        await db_store.mark_autotrade_key_error(user_id=uid, exchange=ex, market_type=mt, error=raw_err or "invalid", deactivate=True)
                    except Exception:
                        pass
                elif not bool(res.get("trade_ok")):
                    await message.answer(tr(uid, "at_keys_trade_missing"))
                    try:
                        await db_store.mark_autotrade_key_error(user_id=uid, exchange=ex, market_type=mt, error="trade_permission_missing", deactivate=True)
                    except Exception:
                        pass
                else:
                    # Encrypt + store active
                    try:
                        f = _fernet()
                    except Exception:
                        await message.answer(tr(uid, "at_master_key_missing"))
                    else:
                        api_key_enc = f.encrypt(api_key.encode("utf-8")).decode("utf-8")
                        api_secret_enc = f.encrypt(api_secret.encode("utf-8")).decode("utf-8")
                        passphrase_enc = f.encrypt(passphrase.encode("utf-8")).decode("utf-8")
                        await db_store.upsert_autotrade_keys(
                            user_id=uid,
                            exchange=ex,
                            market_type=mt,
                            api_key_enc=api_key_enc,
                            api_secret_enc=api_secret_enc,
                            passphrase_enc=passphrase_enc,
                            is_active=True,
                            last_error=None,
                        )
                    await message.answer(tr(uid, "at_keys_ok"))

                st = await db_store.get_autotrade_settings(uid)
                keys = await db_store.get_autotrade_keys_status(uid)
                await message.answer(autotrade_text(uid, st, keys), reply_markup=autotrade_kb(uid, st, keys))
                return

            if step == "api_key":
                state["api_key"] = text
                state["step"] = "api_secret"
                AUTOTRADE_INPUT[uid] = state
                await message.answer(tr(uid, "at_enter_api_secret"))
                return

            if step == "api_secret":
                api_key = (state.get("api_key") or "").strip()
                api_secret = text

                # OKX requires an extra passphrase
                if ex == "okx":
                    state["api_secret"] = api_secret
                    state["step"] = "passphrase"
                    AUTOTRADE_INPUT[uid] = state
                    await message.answer(tr(uid, "at_enter_passphrase", exchange=ex.upper(), market=mt.upper()))
                    return

                AUTOTRADE_INPUT.pop(uid, None)

                # Basic pre-validation to avoid confusing raw exchange errors when
                # user pastes a wrong value (e.g. random short text). Exchange key
                # lengths differ, so keep it permissive for Bybit (their API key can
                # be < 20 chars) while still blocking obvious junk.
                api_key_s = api_key.strip()
                api_secret_s = str(api_secret).strip()
                # Universal permissive validation (all exchanges):
                # - no spaces / newlines / tabs inside key or secret
                # - minimal length thresholds
                # We rely on validate_autotrade_keys() for real API validation.
                if (re.search(r"\s", api_key_s) is not None) or (re.search(r"\s", api_secret_s) is not None):
                    bad_format = True
                else:
                    bad_format = (len(api_key_s) < 8) or (len(api_secret_s) < 16)

                if bad_format:
                    try:
                        await db_store.mark_autotrade_key_error(
                            user_id=uid,
                            exchange=ex,
                            market_type=mt,
                            error="bad_format",
                            deactivate=True,
                        )
                    except Exception:
                        pass
                    await message.answer(trf(uid, "at_keys_bad_format", exchange=ex.upper(), market=mt.upper()))

                    st = await db_store.get_autotrade_settings(uid)
                    keys = await db_store.get_autotrade_keys_status(uid)
                    await message.answer(autotrade_text(uid, st, keys), reply_markup=autotrade_kb(uid, st, keys))
                    return

                # Validate (READ + TRADE). If TRADE missing -> reject.
                try:
                    res = await validate_autotrade_keys(exchange=ex, market_type=mt, api_key=api_key, api_secret=api_secret)
                except Exception as e:
                    res = {"ok": False, "read_ok": False, "trade_ok": False, "error": str(e)}

                if not res.get("ok"):
                    raw_err = str(res.get("error") or "API error")

                    # Map raw exchange errors to user-friendly messages.
                    raw_low = raw_err.lower()
                    user_key = "at_keys_api_error"

                    if ex == "binance":
                        if ("api-key format invalid" in raw_low) or ("api key format invalid" in raw_low) or ("-2014" in raw_low):
                            user_key = "at_keys_binance_format"
                        elif ("-1021" in raw_low) or ("timestamp" in raw_low):
                            user_key = "at_keys_binance_time"
                        elif ("-1003" in raw_low) or ("too many requests" in raw_low) or ("rate limit" in raw_low):
                            user_key = "at_keys_binance_rate"
                        elif ("permission" in raw_low) or ("not authorized" in raw_low) or ("unauthorized" in raw_low):
                            user_key = "at_keys_binance_perm"
                        elif ("-2015" in raw_low) or ("invalid api-key" in raw_low) or ("invalid api key" in raw_low):
                            user_key = "at_keys_binance_invalid"

                    elif ex == "bybit":
                        if ("10003" in raw_low) or ("api key is invalid" in raw_low) or ("invalid api key" in raw_low):
                            user_key = "at_keys_bybit_invalid"
                        elif ("invalid signature" in raw_low) or (("signature" in raw_low) and ("invalid" in raw_low)) or ("error sign" in raw_low):
                            user_key = "at_keys_bybit_signature"
                        elif ("permission" in raw_low) or ("not authorized" in raw_low) or ("forbidden" in raw_low) or ("denied" in raw_low):
                            user_key = "at_keys_bybit_perm"
                        elif ("timestamp" in raw_low) or ("time window" in raw_low) or ("recv_window" in raw_low):
                            user_key = "at_keys_bybit_time"
                        elif ("too many requests" in raw_low) or ("rate limit" in raw_low):
                            user_key = "at_keys_bybit_rate"

                    elif ex == "okx":
                        if ("50111" in raw_low) or ("invalid ok-access-key" in raw_low):
                            user_key = "at_keys_okx_invalid"
                        elif ("passphrase" in raw_low) and ("invalid" in raw_low):
                            user_key = "at_keys_okx_passphrase"
                        elif ("whitelist" in raw_low) or (("ip" in raw_low) and ("not" in raw_low) and ("allowed" in raw_low)):
                            user_key = "at_keys_okx_ip"
                        elif ("permission" in raw_low) or ("forbidden" in raw_low) or ("not authorized" in raw_low):
                            user_key = "at_keys_okx_perm"
                        elif ("too many requests" in raw_low) or ("rate limit" in raw_low):
                            user_key = "at_keys_okx_rate"

                    elif ex == "mexc":
                        if ("invalid api key" in raw_low) or ("api key not exists" in raw_low) or ("api key information invalid" in raw_low):
                            user_key = "at_keys_mexc_invalid"
                        elif ("invalid signature" in raw_low) or (("signature" in raw_low) and ("invalid" in raw_low)):
                            user_key = "at_keys_mexc_signature"
                        elif ("whitelist" in raw_low) or (("ip" in raw_low) and ("not" in raw_low) and ("allowed" in raw_low)):
                            user_key = "at_keys_mexc_ip"
                        elif ("permission" in raw_low) or ("forbidden" in raw_low) or ("not authorized" in raw_low):
                            user_key = "at_keys_mexc_perm"
                        elif ("too many requests" in raw_low) or ("rate limit" in raw_low):
                            user_key = "at_keys_mexc_rate"

                    elif ex == "gateio":
                        if ("invalid key" in raw_low) or ("invalid api key" in raw_low) or ("authentication failed" in raw_low):
                            user_key = "at_keys_gate_invalid"
                        elif ("invalid signature" in raw_low) or (("signature" in raw_low) and ("invalid" in raw_low)):
                            user_key = "at_keys_gate_signature"
                        elif ("whitelist" in raw_low) or (("ip" in raw_low) and ("not" in raw_low) and ("allowed" in raw_low)):
                            user_key = "at_keys_gate_ip"
                        elif ("permission" in raw_low) or ("forbidden" in raw_low) or ("not authorized" in raw_low):
                            user_key = "at_keys_gate_perm"
                        elif ("too many requests" in raw_low) or ("rate limit" in raw_low):
                            user_key = "at_keys_gate_rate"

                    # Generic IP restriction fallback
                    if user_key == "at_keys_api_error":
                        if ("whitelist" in raw_low) or (("restricted" in raw_low) and ("ip" in raw_low)):
                            user_key = "at_keys_ip_restricted"
                    # Store raw error (inactive) so UI shows ❌ and for troubleshooting.
                    try:
                        await db_store.mark_autotrade_key_error(user_id=uid, exchange=ex, market_type=mt, error=raw_err, deactivate=True)
                    except Exception:
                        pass
                    await message.answer(trf(uid, user_key, exchange=ex.upper(), market=mt.upper()))
                elif not res.get("trade_ok"):
                    try:
                        await db_store.mark_autotrade_key_error(user_id=uid, exchange=ex, market_type=mt, error="trade_permission_missing", deactivate=True)
                    except Exception:
                        pass
                    await message.answer(tr(uid, "at_keys_trade_missing"))
                else:
                    # Encrypt + store active
                    try:
                        f = _fernet()
                    except Exception:
                        await message.answer(tr(uid, "at_master_key_missing"))
                    else:
                        api_key_enc = f.encrypt(api_key.encode("utf-8")).decode("utf-8")
                        api_secret_enc = f.encrypt(api_secret.encode("utf-8")).decode("utf-8")
                        await db_store.upsert_autotrade_keys(
                            user_id=uid,
                            exchange=ex,
                            market_type=mt,
                            api_key_enc=api_key_enc,
                            api_secret_enc=api_secret_enc,
                            passphrase_enc=None,
                            is_active=True,
                            last_error=None,
                        )
                        # Validate keys against the exchange right away (prevents "green" status for invalid keys)
                        try:
                            v = await validate_autotrade_keys(
                                exchange=ex,
                                market_type=mt,
                                api_key=api_key,
                                api_secret=api_secret,
                                passphrase=None,
                            )
                        except Exception as e:
                            # Network/temporary error: keep keys active but inform user
                            await message.answer(tr(uid, "at_keys_api_error", exchange=ex, market=mt) + "\n\n" + str(e)[:200])
                        else:
                            if not bool(v.get("ok")):
                                err = str(v.get("error") or "invalid").strip()
                                # Deactivate on credential/permission issues
                                await db_store.mark_autotrade_key_error(
                                    user_id=uid,
                                    exchange=ex,
                                    market_type=mt,
                                    error=err,
                                    deactivate=True,
                                )
                                low = err.lower()
                                if "ip" in low and "restrict" in low:
                                    await message.answer(tr(uid, "at_keys_ip_restricted"))
                                elif ex == "binance":
                                    await message.answer(tr(uid, "at_keys_binance_invalid"))
                                elif ex == "bybit":
                                    await message.answer(tr(uid, "at_keys_bybit_invalid"))
                                else:
                                    await message.answer(tr(uid, "at_keys_invalid"))
                            else:
                                await message.answer(tr(uid, "at_keys_ok"))

                st = await db_store.get_autotrade_settings(uid)
                keys = await db_store.get_autotrade_keys_status(uid)
                await message.answer(autotrade_text(uid, st, keys), reply_markup=autotrade_kb(uid, st, keys))
                return

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
    had_live_block = False
    if t.get('price_f') is not None:
        try:
            px = float(t.get('price_f') or 0.0)
            src = str(t.get('price_src') or '')
            parts.append("")
            parts.append(f"💹 {tr(uid, 'lbl_price_now')}: {px:.6f}")
            if src:
                parts.append(f"🔌 {tr(uid, 'lbl_price_src')}: {src}")            # "Checks" should reflect executed state when TP/SL already happened.
            # Otherwise it can show ❌ for TP1 after price moved away, even though TP1 was reached earlier.
            st = str(t.get('status') or status).upper()
            hit_sl = bool(t.get('hit_sl')) or st in ('LOSS',)
            hit_tp1 = bool(t.get('hit_tp1')) or st in ('TP1','WIN','TP2')
            hit_tp2 = bool(t.get('hit_tp2')) or st in ('WIN','TP2')

            checks = []
            if t.get('sl') is not None:
                checks.append(f"SL: {'✅' if hit_sl else '❌'}")
            if t.get('tp1') is not None:
                checks.append(f"{tr(uid, 'lbl_tp1')}: {'✅' if hit_tp1 else '❌'}")
            if t.get('tp2') is not None and float(t.get('tp2') or 0) > 0:
                checks.append(f"TP2: {'✅' if hit_tp2 else '❌'}")
            if checks:
                parts.append(f"🧪 {tr(uid, 'lbl_check')}: " + ' '.join(checks))
            had_live_block = True
        except Exception:
            pass

    # Opened datetime (MSK)
    opened_at = _fmt_dt_msk(t.get('opened_at'))
    opened_lbl = tr(uid, 'trade_opened_at')
    opened_line = f"🕒 {opened_lbl}: {opened_at} (MSK)"
    if opened_at and opened_at != "—":
        parts.append(opened_line)

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
        row_sym = str((row or {}).get('symbol') or '').upper() if isinstance(row, dict) else ''
        row_mkt = str((row or {}).get('market') or '').upper() if isinstance(row, dict) else ''
        sig_sym = str(getattr(sig, 'symbol', '') or '').upper()
        sig_mkt = str(getattr(sig, 'market', '') or '').upper()
        same_trade = bool(row) and st in ("ACTIVE", "TP1") and row_sym == sig_sym and row_mkt == sig_mkt
        if same_trade:
            # Already opened -> remove button to prevent retries
            try:
                if call.message:
                    await safe_edit_markup(call.from_user.id, call.message.message_id, None)
            except Exception:
                pass
            await call.answer(tr(call.from_user.id, "sig_already_opened_toast"), show_alert=True)
            await safe_send(call.from_user.id, tr(call.from_user.id, "sig_already_opened_msg"), reply_markup=menu_kb(call.from_user.id))
            return
        # If we found an ACTIVE trade for this signal_id but it belongs to another signal (symbol/market mismatch),
        # treat it as a signal_id collision and retry with a fresh signal_id below.
        if row and st in ("ACTIVE", "TP1") and not same_trade:
            try:
                logger.warning("signal_id collision: uid=%s signal_id=%s row=%s/%s sig=%s/%s", call.from_user.id, sig.signal_id, row_mkt, row_sym, sig_mkt, sig_sym)
            except Exception:
                pass
            row = None

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



# ---------------- BOT-level signal outcome watcher (independent from users) ----------------

_SIG_WATCH_INTERVAL_SEC = max(5, int(os.getenv("SIG_WATCH_INTERVAL_SEC", "15")))
_BE_BUFFER_PCT = float(os.getenv("BE_BUFFER_PCT", "0.001") or 0.001)  # 0.1% default
_BE_CONFIRM_SEC = max(0, int(os.getenv("BE_CONFIRM_SEC", "60") or 60))  # require staying past BE trigger for N seconds
_SIG_TP1_PARTIAL_PCT = float(os.getenv("SIG_TP1_PARTIAL_CLOSE_PCT", "50") or 50)  # model partial close at TP1
_SIG_BE_ARM_PCT_TO_TP2 = max(0.0, min(1.0, float(os.getenv('BE_ARM_PCT_TO_TP2', '0') or 0.0)))

def _be_is_armed_sig(side: str, price: float, tp1: float | None, tp2: float | None) -> bool:
    """Delayed BE arming: arm BE only after price moves from TP1 towards TP2 by BE_ARM_PCT_TO_TP2 fraction."""
    pct = float(_SIG_BE_ARM_PCT_TO_TP2 or 0.0)
    if pct <= 0:
        return True
    try:
        tp1f = float(tp1 or 0.0)
        tp2f = float(tp2 or 0.0)
        pf = float(price or 0.0)
    except Exception:
        return True
    if tp1f <= 0 or tp2f <= 0:
        return True
    side_u = (side or 'LONG').upper()
    if side_u == 'LONG':
        if tp2f <= tp1f:
            return True
        thr = tp1f + (tp2f - tp1f) * pct
        return pf >= thr
    else:
        if tp2f >= tp1f:
            return True
        thr = tp1f - (tp1f - tp2f) * pct
        return pf <= thr


async def _fetch_binance_price(symbol: str, *, futures: bool) -> float:
    """Best-effort public price from Binance (spot or futures)."""
    # Binance endpoints require the plain concatenated symbol (e.g., BTCUSDT).
    # We normalize common variants that scanners or UI can emit: "BTC/USDT", "BTC-USDT",
    # "BTC_USDT", etc. Keep only A-Z0-9 to avoid silent 0.0 prices -> never-closing outcomes.
    symbol = str(symbol or "").upper().strip()
    try:
        symbol = re.sub(r"[^A-Z0-9]", "", symbol)
    except Exception:
        # Fallback normalization (no regex)
        symbol = symbol.replace("/", "").replace("-", "").replace("_", "").replace(":", "")
    if not symbol:
        return 0.0
    url = ("https://fapi.binance.com/fapi/v1/ticker/price" if futures else "https://api.binance.com/api/v3/ticker/price")
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, params={"symbol": symbol}) as r:
                data = await r.json(content_type=None)
                return float((data or {}).get("price") or 0.0)
    except Exception:
        return 0.0


def _okx_inst(symbol: str, *, futures: bool) -> str:
    """Convert BTCUSDT -> BTC-USDT (spot) or BTC-USDT-SWAP (futures)."""
    s = str(symbol or "").upper().replace("/", "").replace("-", "").strip()
    if s.endswith("USDT") and len(s) > 4:
        base = s[:-4]
        return f"{base}-USDT-SWAP" if futures else f"{base}-USDT"
    return s


def _gate_pair(symbol: str) -> str:
    """Convert BTCUSDT -> BTC_USDT for Gate.io."""
    s = str(symbol or "").upper().replace("/", "").replace("-", "").strip()
    if s.endswith("USDT") and len(s) > 4:
        return f"{s[:-4]}_USDT"
    return s


async def _fetch_bybit_price(symbol: str, *, futures: bool) -> float:
    """Public last price from Bybit v5 market tickers."""
    symbol = str(symbol or "").upper().replace("/", "").replace("-", "").strip()
    if not symbol:
        return 0.0
    category = "linear" if futures else "spot"
    url = "https://api.bybit.com/v5/market/tickers"
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, params={"category": category, "symbol": symbol}) as r:
                data = await r.json(content_type=None)
                lst = (((data or {}).get("result") or {}).get("list") or [])
                if lst and isinstance(lst, list):
                    return float((lst[0] or {}).get("lastPrice") or 0.0)
    except Exception:
        pass
    return 0.0


async def _fetch_okx_price(symbol: str, *, futures: bool) -> float:
    """Public last price from OKX tickers."""
    inst = _okx_inst(symbol, futures=futures)
    if not inst:
        return 0.0
    url = "https://www.okx.com/api/v5/market/ticker"
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, params={"instId": inst}) as r:
                data = await r.json(content_type=None)
                arr = (data or {}).get("data") or []
                if arr and isinstance(arr, list):
                    return float((arr[0] or {}).get("last") or 0.0)
    except Exception:
        pass
    return 0.0


async def _fetch_mexc_price(symbol: str) -> float:
    """Public last price from MEXC spot."""
    symbol = str(symbol or "").upper().replace("/", "").replace("-", "").strip()
    if not symbol:
        return 0.0
    url = "https://api.mexc.com/api/v3/ticker/price"
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, params={"symbol": symbol}) as r:
                data = await r.json(content_type=None)
                return float((data or {}).get("price") or 0.0)
    except Exception:
        return 0.0


async def _fetch_gateio_price(symbol: str) -> float:
    """Public last price from Gate.io spot."""
    pair = _gate_pair(symbol)
    if not pair:
        return 0.0
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, params={"currency_pair": pair}) as r:
                data = await r.json(content_type=None)
                if isinstance(data, list) and data:
                    return float((data[0] or {}).get("last") or 0.0)
    except Exception:
        pass
    return 0.0


def _parse_price_order(env_name: str, default: str) -> list[str]:
    raw = (os.getenv(env_name, default) or default).strip()
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    return parts or [p.strip().lower() for p in default.split(",") if p.strip()]


async def _fetch_signal_price(symbol: str, *, market: str) -> tuple[float, str]:
    """Get price with fallback sources. Returns (price, source)."""
    m = (market or "SPOT").upper()
    sym = str(symbol or "").upper().strip()
    if not sym:
        return 0.0, ""

    async def _safe(coro) -> float:
        try:
            v = await coro
            v = float(v or 0.0)
            return v if v > 0 else 0.0
        except Exception:
            return 0.0

    if m == "FUTURES":
        # Robust decision price: median of available sources (Binance+Bybit+OKX) to reduce single-source noise.
        # Enable/disable via SIG_PRICE_FUTURES_MEDIAN (default on).
        use_med = (os.getenv("SIG_PRICE_FUTURES_MEDIAN", "1").strip().lower() not in ("0","false","no","off"))
        if use_med:
            b = await _safe(_fetch_binance_price(sym, futures=True))
            y = await _safe(_fetch_bybit_price(sym, futures=True))
            o = await _safe(_fetch_okx_price(sym, futures=True))
            srcs = [(b, "binance"), (y, "bybit"), (o, "okx")]
            vals = [v for v,_ in srcs if v > 0]
            if len(vals) >= 2:
                return float(statistics.median(vals)), "MEDIAN(" + "+".join([s.upper() for v,s in srcs if v > 0]) + ")"
            elif len(vals) == 1:
                only = [s for v,s in srcs if v > 0][0]
                return float(vals[0]), only

        # Fallback order-based
        order = _parse_price_order("SIG_PRICE_ORDER_FUTURES", "binance,bybit,okx")
        for src in order:
            if src == "binance":
                px = await _safe(_fetch_binance_price(sym, futures=True))
            elif src == "bybit":
                px = await _safe(_fetch_bybit_price(sym, futures=True))
            elif src == "okx":
                px = await _safe(_fetch_okx_price(sym, futures=True))
            else:
                px = 0.0
            if px > 0:
                return px, src
        return 0.0, ""

    # SPOT
    order = _parse_price_order("SIG_PRICE_ORDER_SPOT", "binance,bybit,okx,mexc,gateio")
    for src in order:
        if src == "binance":
            px = await _safe(_fetch_binance_price(sym, futures=False))
        elif src == "bybit":
            px = await _safe(_fetch_bybit_price(sym, futures=False))
        elif src == "okx":
            px = await _safe(_fetch_okx_price(sym, futures=False))
        elif src == "mexc":
            px = await _safe(_fetch_mexc_price(sym))
        elif src in ("gate", "gateio"):
            px = await _safe(_fetch_gateio_price(sym))
        else:
            px = 0.0
        if px > 0:
            return px, src
    return 0.0, ""

def _hit_tp(side: str, price: float, lvl: float) -> bool:
    return price >= lvl if side == "LONG" else price <= lvl

def _hit_sl(side: str, price: float, lvl: float) -> bool:
    return price <= lvl if side == "LONG" else price >= lvl

def _pnl_pct(side: str, entry: float, close: float) -> float:
    if entry <= 0:
        return 0.0
    if side == "LONG":
        return (close - entry) / entry * 100.0
    return (entry - close) / entry * 100.0


# --- Exchange-like PnL model for SIGNAL outcomes (admin stats) ---
# This is still an approximation (no funding / no orderbook slippage),
# but it matches how exchanges compute ROI:
#   - spot: %PnL on notional
#   - futures: %ROI on margin = price_move% * leverage
# Fees (taker) are applied on entry + exit notional.

_SIG_SPOT_TAKER_FEE_PCT = float(os.getenv("SIG_SPOT_TAKER_FEE_PCT", "0.10") or 0.10)  # 0.10% default
_SIG_FUT_TAKER_FEE_PCT = float(os.getenv("SIG_FUT_TAKER_FEE_PCT", "0.06") or 0.06)    # 0.06% default
_SIG_FUT_LEVERAGE = float(os.getenv("SIG_FUT_LEVERAGE", "1") or 1)
_SIG_SLIPPAGE_PCT = float(os.getenv("SIG_SLIPPAGE_PCT", "0.00") or 0.0)  # per fill, in %

def _sig_leverage(market: str) -> float:
    try:
        if (market or "SPOT").upper() == "FUTURES":
            return max(1.0, float(_SIG_FUT_LEVERAGE))
    except Exception:
        pass
    return 1.0

def _sig_fee_pct(market: str) -> float:
    try:
        return float(_SIG_FUT_TAKER_FEE_PCT) if (market or "SPOT").upper() == "FUTURES" else float(_SIG_SPOT_TAKER_FEE_PCT)
    except Exception:
        return 0.0

def _sig_net_pnl_pct(*, market: str, side: str, entry: float, close: float, part_entry_to_close: float = 1.0) -> float:
    """Net PnL% like exchange.

    part_entry_to_close is a weight of notional closed at 'close'.
    Use it for partial TP1 models.
    """
    if entry <= 0:
        return 0.0
    L = _sig_leverage(market)
    fee = max(0.0, _sig_fee_pct(market))
    slip = max(0.0, float(_SIG_SLIPPAGE_PCT))
    # Price move component
    move = _pnl_pct(side, entry, close) * L
    # Fees/slippage: entry always on 100% notional; exit on part notional
    # Convert fee% (of notional) into ROI% (of margin) for futures => multiply by L.
    cost = (fee + slip) * L * (1.0 + max(0.0, min(1.0, float(part_entry_to_close))))
    return float(move - cost)

def _sig_net_pnl_two_targets(*, market: str, side: str, entry: float, tp1: float, tp2: float, part: float) -> float:
    """Net PnL for partial TP1 then TP2. part in [0..1]."""
    p = max(0.0, min(1.0, float(part)))
    pnl1 = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=tp1, part_entry_to_close=p)
    pnl2 = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=tp2, part_entry_to_close=(1.0 - p))
    # Entry cost is counted twice above (once in each leg). Fix it:
    # In reality: entry fee/slippage once on full notional.
    # Here: entry cost included in pnl1 and pnl2. Remove one full entry cost.
    L = _sig_leverage(market)
    fee = max(0.0, _sig_fee_pct(market))
    slip = max(0.0, float(_SIG_SLIPPAGE_PCT))
    entry_cost = (fee + slip) * L  # full notional, once
    return float(pnl1 + pnl2 + entry_cost)

def _sig_net_pnl_tp1_then_be(*, market: str, side: str, entry: float, tp1: float, part: float) -> float:
    """Net PnL for partial TP1 then close remainder at BE (entry)."""
    p = max(0.0, min(1.0, float(part)))
    pnl1 = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=tp1, part_entry_to_close=p)
    pnl2 = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=entry, part_entry_to_close=(1.0 - p))
    # Fix double-counted entry cost (same idea as _sig_net_pnl_two_targets)
    L = _sig_leverage(market)
    fee = max(0.0, _sig_fee_pct(market))
    slip = max(0.0, float(_SIG_SLIPPAGE_PCT))
    entry_cost = (fee + slip) * L
    return float(pnl1 + pnl2 + entry_cost)

def _model_partial_pct() -> float:
    try:
        p = float(_SIG_TP1_PARTIAL_PCT)
    except Exception:
        p = 50.0
    return max(0.0, min(100.0, p)) / 100.0

async def signal_outcome_loop() -> None:
    """Close signal tracks by price: WIN/LOSS/TP1/BE with strict BE logic.

    Strict BE:
      - BE can happen only AFTER TP1.
      - After TP1, BE triggers when price crosses entry +/- buffer
        AND stays beyond it for BE_CONFIRM_SEC seconds (anti-wick confirmation).
    """
    import datetime as _dt
    logger.info("Starting signal_outcome_loop interval=%ss buffer_pct=%s confirm_sec=%s", _SIG_WATCH_INTERVAL_SEC, _BE_BUFFER_PCT, _BE_CONFIRM_SEC)
    while True:
        try:
            rows = await db_store.list_open_signal_tracks(limit=1000)
            if not rows:
                await asyncio.sleep(_SIG_WATCH_INTERVAL_SEC)
                continue

            now = _dt.datetime.now(_dt.timezone.utc)
            part = _model_partial_pct()

            for t in rows:
                try:
                    sid = int(t.get("signal_id") or 0)
                    market = str(t.get("market") or "SPOT").upper()
                    symbol = str(t.get("symbol") or "").upper()
                    side = str(t.get("side") or "LONG").upper()
                    status = str(t.get("status") or "ACTIVE").upper()

                    entry = float(t.get("entry") or 0.0)
                    tp1 = float(t.get("tp1") or 0.0) if t.get("tp1") is not None else 0.0
                    tp2 = float(t.get("tp2") or 0.0) if t.get("tp2") is not None else 0.0
                    sl  = float(t.get("sl") or 0.0)  if t.get("sl")  is not None else 0.0

                    if sid <= 0 or entry <= 0 or not symbol:
                        continue

                    px, _src = await _fetch_signal_price(symbol, market=market)
                    if px <= 0:
                        continue

                    # Prefer TPC: if tp2 missing but tp1 exists, treat tp1 as final target
                    eff_tp2 = tp2 if (tp2 > 0 and (tp1 <= 0 or abs(tp2 - tp1) > 1e-12)) else 0.0
                    eff_tp1 = tp1 if tp1 > 0 else 0.0

                    # ACTIVE stage
                    if status == "ACTIVE":
                        # WIN by TP2 (or TP1 if no TP2)
                        if eff_tp2 > 0 and _hit_tp(side, px, eff_tp2):
                            pnl = _sig_net_pnl_two_targets(market=market, side=side, entry=entry, tp1=eff_tp1, tp2=eff_tp2, part=part) if (eff_tp1 > 0 and eff_tp2 > 0) else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=eff_tp2, part_entry_to_close=1.0)
                            await db_store.close_signal_track(signal_id=sid, status="WIN", pnl_total_pct=float(pnl))
                            continue
                        if eff_tp2 <= 0 and eff_tp1 > 0 and _hit_tp(side, px, eff_tp1):
                            # single-target win at TP1
                            pnl = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=eff_tp1, part_entry_to_close=1.0)
                            await db_store.close_signal_track(signal_id=sid, status="WIN", pnl_total_pct=float(pnl))
                            continue
                        # LOSS by SL (only before TP1 in strict mode)
                        if sl > 0 and _hit_sl(side, px, sl):
                            pnl = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=sl, part_entry_to_close=1.0)
                            await db_store.close_signal_track(signal_id=sid, status="LOSS", pnl_total_pct=float(pnl))
                            continue
                        # TP1 hit -> arm BE
                        if eff_tp1 > 0 and _hit_tp(side, px, eff_tp1):
                            await db_store.mark_signal_tp1(signal_id=sid, be_price=float(entry))
                            continue

                    # TP1 stage (BE armed)
                    if status == "TP1":
                        # WIN by TP2 if exists
                        if eff_tp2 > 0 and _hit_tp(side, px, eff_tp2):
                            pnl = _sig_net_pnl_two_targets(market=market, side=side, entry=entry, tp1=eff_tp1, tp2=eff_tp2, part=part) if eff_tp1 > 0 else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=eff_tp2, part_entry_to_close=1.0)
                            await db_store.close_signal_track(signal_id=sid, status="WIN", pnl_total_pct=float(pnl))
                            continue

                        if not _be_is_armed_sig(side, px, eff_tp1, eff_tp2):
                            # Not armed yet: don't allow BE close; reset pending BE timer if any.
                            if t.get('be_crossed_at'):
                                await db_store.clear_signal_be_crossed(signal_id=sid)
                            continue

                        # Strict BE confirmation
                        trigger = entry * (1.0 - _BE_BUFFER_PCT) if side == "LONG" else entry * (1.0 + _BE_BUFFER_PCT)
                        crossed = bool(px <= trigger) if side == "LONG" else bool(px >= trigger)

                        crossed_at = t.get("be_crossed_at")
                        crossed_at_dt = _parse_iso_dt(crossed_at) if isinstance(crossed_at, str) else crossed_at
                        if crossed and crossed_at_dt is None:
                            await db_store.mark_signal_be_crossed(signal_id=sid)
                            continue
                        if not crossed and crossed_at_dt is not None:
                            # reset if price went back (anti-wick)
                            await db_store.clear_signal_be_crossed(signal_id=sid)
                            continue
                        if crossed and crossed_at_dt is not None and _BE_CONFIRM_SEC > 0:
                            if (now - crossed_at_dt).total_seconds() >= _BE_CONFIRM_SEC:
                                pnl = _sig_net_pnl_tp1_then_be(market=market, side=side, entry=entry, tp1=eff_tp1, part=part) if eff_tp1 > 0 else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=entry, part_entry_to_close=1.0)
                                await db_store.close_signal_track(signal_id=sid, status="BE", pnl_total_pct=float(pnl))
                                continue
                        if crossed and crossed_at_dt is not None and _BE_CONFIRM_SEC == 0:
                            pnl = _sig_net_pnl_tp1_then_be(market=market, side=side, entry=entry, tp1=eff_tp1, part=part) if eff_tp1 > 0 else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=entry, part_entry_to_close=1.0)
                            await db_store.close_signal_track(signal_id=sid, status="BE", pnl_total_pct=float(pnl))
                            continue

                except Exception:
                    # keep loop alive
                    logger.exception("signal_outcome_loop: item error")
                    continue

        except Exception:
            logger.exception("signal_outcome_loop error")

        await asyncio.sleep(_SIG_WATCH_INTERVAL_SEC)


async def main() -> None:
    import time
    
    logger.info("Bot starting; TZ=%s", TZ_NAME)
    await init_db()
    load_langs()

    # Refresh global Auto-trade pause/maintenance flags in background
    asyncio.create_task(_autotrade_bot_global_loop())
    await _refresh_autotrade_bot_global_once()

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
            """CORS middleware + request-level error logging."""
            # Preflight
            if request.method == "OPTIONS":
                resp = web.Response(status=204)
            else:
                try:
                    resp = await handler(request)
                except Exception:
                    logger.exception("HTTP handler error: %s %s", request.method, request.path_qs)
                    raise
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

        # ---------------- AUTO-TRADE GLOBAL SETTINGS (pause/maintenance) ----------------

        async def autotrade_get_settings(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            st = await db_store.get_autotrade_bot_settings()
            return web.json_response({
                "pause_autotrade": bool(st.get("pause_autotrade")),
                "maintenance_mode": bool(st.get("maintenance_mode")),
                "updated_at": (st.get("updated_at").isoformat() if st.get("updated_at") else None),
            })

        async def autotrade_save_settings(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            data = await request.json()
            pause = bool(data.get("pause_autotrade", data.get("pauseAutotrade", False)))
            maint = bool(data.get("maintenance_mode", data.get("maintenanceMode", False)))
            await db_store.set_autotrade_bot_settings(pause_autotrade=pause, maintenance_mode=maint)
            # refresh in-memory cache ASAP
            AUTOTRADE_BOT_GLOBAL["pause_autotrade"] = pause
            AUTOTRADE_BOT_GLOBAL["maintenance_mode"] = maint
            return web.json_response({"ok": True})

        # ---------------- BOT ADMIN (legacy subscriptions: expires_at + sub_*/notify_*) ----------------

        async def bot_list_users(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            q = (request.query.get("query") or request.query.get("q") or "").strip()
            flt = (request.query.get("filter") or "all").strip().lower()

            async with pool.acquire() as conn:
                where = []
                args = []
                if q:
                    where.append("CAST(telegram_id AS TEXT) ILIKE $%d" % (len(args) + 1))
                    args.append(f"%{q}%")
                if flt == "active":
                    where.append("COALESCE(is_blocked,FALSE)=FALSE")
                    where.append("(expires_at IS NULL OR expires_at > now())")
                elif flt == "expired":
                    where.append("COALESCE(is_blocked,FALSE)=FALSE")
                    where.append("expires_at IS NOT NULL")
                    where.append("expires_at <= now()")
                elif flt == "blocked":
                    where.append("COALESCE(is_blocked,FALSE)=TRUE")
                where_sql = ("WHERE " + " AND ".join(where)) if where else ""

                try:
                    rows = await conn.fetch(
                        f"""
                        SELECT telegram_id, expires_at, COALESCE(is_blocked,FALSE) AS is_blocked,
                               COALESCE(sub_spreads,FALSE) AS sub_spreads,
                               COALESCE(sub_basis,FALSE) AS sub_basis,
                               COALESCE(sub_dex_cex, COALESCE(sub_dexcex, FALSE)) AS sub_dex_cex,
                               COALESCE(notify_spreads, FALSE) AS notify_spreads,
                               COALESCE(notify_basis, FALSE) AS notify_basis,
                               COALESCE(notify_dex_cex, COALESCE(notify_dexcex, FALSE)) AS notify_dex_cex
                        FROM users
                        {where_sql}
                        ORDER BY telegram_id DESC
                        LIMIT 500
                        """,
                        *args,
                    )
                except Exception:
                    rows = await conn.fetch(
                        f"""
                        SELECT telegram_id, expires_at, COALESCE(is_blocked,FALSE) AS is_blocked
                        FROM users
                        {where_sql}
                        ORDER BY telegram_id DESC
                        LIMIT 500
                        """,
                        *args,
                    )

            items = []
            for r in rows:
                items.append({
                    "telegram_id": int(r.get("telegram_id") or 0),
                    "expires_at": (r.get("expires_at").isoformat() if r.get("expires_at") else None),
                    "is_blocked": bool(r.get("is_blocked") or False),
                    "sub_spreads": bool(r.get("sub_spreads") or False),
                    "sub_basis": bool(r.get("sub_basis") or False),
                    "sub_dex_cex": bool(r.get("sub_dex_cex") or False),
                    "notify_spreads": bool(r.get("notify_spreads") or False),
                    "notify_basis": bool(r.get("notify_basis") or False),
                    "notify_dex_cex": bool(r.get("notify_dex_cex") or False),
                })
            return web.json_response({"ok": True, "items": items})

        async def bot_create_user(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            data = await request.json()
            tid = int(data.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            expires = _parse_iso_dt(data.get("expires_at"))
            sub_spreads = bool(data.get("sub_spreads", True))
            sub_basis = bool(data.get("sub_basis", True))
            sub_dex = bool(data.get("sub_dex_cex", data.get("sub_dexcex", True)))
            notify_spreads = bool(data.get("notify_spreads", sub_spreads))
            notify_basis = bool(data.get("notify_basis", sub_basis))
            notify_dex = bool(data.get("notify_dex_cex", data.get("notify_dexcex", sub_dex)))

            await ensure_user(tid)
            async with pool.acquire() as conn:
                try:
                    await conn.execute(
                        """
                        UPDATE users
                           SET expires_at=$2::timestamptz,
                               sub_spreads=$3,
                               sub_basis=$4,
                               sub_dex_cex=$5,
                               notify_spreads=$6,
                               notify_basis=$7,
                               notify_dex_cex=$8
                         WHERE telegram_id=$1
                        """,
                        tid, expires, sub_spreads, sub_basis, sub_dex, notify_spreads, notify_basis, notify_dex
                    )
                except Exception:
                    await conn.execute(
                        "UPDATE users SET expires_at=$2::timestamptz WHERE telegram_id=$1",
                        tid, expires
                    )
            return web.json_response({"ok": True})

        async def bot_patch_user(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            data = await request.json()
            data = data or {}
            await ensure_user(tid)

            expires = _parse_iso_dt(data.get("expires_at")) if ("expires_at" in data) else None
            is_blocked = data.get("is_blocked")

            sub_spreads = data.get("sub_spreads")
            sub_basis = data.get("sub_basis")
            sub_dex = data.get("sub_dex_cex", data.get("sub_dexcex"))
            notify_spreads = data.get("notify_spreads")
            notify_basis = data.get("notify_basis")
            notify_dex = data.get("notify_dex_cex", data.get("notify_dexcex"))

            async with pool.acquire() as conn:
                if expires is not None:
                    await conn.execute("UPDATE users SET expires_at=$2::timestamptz WHERE telegram_id=$1", tid, expires)
                if is_blocked is not None:
                    await conn.execute("UPDATE users SET is_blocked=$2 WHERE telegram_id=$1", tid, bool(is_blocked))
                try:
                    sets = []
                    args = [tid]

                    def add_set(col, val):
                        nonlocal sets, args
                        sets.append(f"{col}=$%d" % (len(args) + 1))
                        args.append(bool(val))

                    if sub_spreads is not None:
                        add_set("sub_spreads", sub_spreads)
                    if sub_basis is not None:
                        add_set("sub_basis", sub_basis)
                    if sub_dex is not None:
                        add_set("sub_dex_cex", sub_dex)
                    if notify_spreads is not None:
                        add_set("notify_spreads", notify_spreads)
                    if notify_basis is not None:
                        add_set("notify_basis", notify_basis)
                    if notify_dex is not None:
                        add_set("notify_dex_cex", notify_dex)

                    if sets:
                        await conn.execute("UPDATE users SET " + ", ".join(sets) + " WHERE telegram_id=$1", *args)
                except Exception:
                    pass

            return web.json_response({"ok": True})

        async def bot_user_action(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            act = (request.match_info.get("action") or "").strip().lower()
            if not tid or not act:
                return web.json_response({"ok": False, "error": "bad request"}, status=400)
            try:
                data = await request.json()
            except Exception:
                data = {}

            await ensure_user(tid)
            async with pool.acquire() as conn:
                if act == "extend":
                    days = int((data or {}).get("days") or 30)
                    await conn.execute(
                        "UPDATE users SET expires_at = COALESCE(expires_at, now()) + make_interval(days => $2) WHERE telegram_id=$1",
                        tid, days
                    )
                elif act == "block":
                    await conn.execute("UPDATE users SET is_blocked=TRUE WHERE telegram_id=$1", tid)
                elif act == "unblock":
                    await conn.execute("UPDATE users SET is_blocked=FALSE WHERE telegram_id=$1", tid)
                elif act == "delete":
                    await conn.execute("DELETE FROM users WHERE telegram_id=$1", tid)
                else:
                    return web.json_response({"ok": False, "error": "unknown action"}, status=400)

            return web.json_response({"ok": True})

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
                    SELECT telegram_id, signal_enabled, signal_expires_at, COALESCE(is_blocked,FALSE) AS is_blocked,
                           COALESCE(autotrade_enabled,FALSE) AS autotrade_enabled, autotrade_expires_at,
                           COALESCE(autotrade_stop_after_close,FALSE) AS autotrade_stop_after_close
                    FROM users
                    {where_sql}
                    ORDER BY telegram_id DESC
                    LIMIT 500
                    """,
                    *args,
                )
            out = []
            for r in rows:
                stop_flag = bool(r.get("autotrade_stop_after_close", False))
                enabled_flag = bool(r.get("autotrade_enabled", False))
                at_state = "STOP" if stop_flag else ("ON" if enabled_flag else "OFF")
                out.append({
                    "telegram_id": int(r["telegram_id"]),
                    "signal_enabled": bool(r["signal_enabled"]),
                    "signal_expires_at": (r["signal_expires_at"].isoformat() if r["signal_expires_at"] else None),
                    "is_blocked": bool(r["is_blocked"]),
                    "autotrade_enabled": bool(r.get("autotrade_enabled", False)),
                    "autotrade_stop_after_close": stop_flag,
                    "autotrade_state": at_state,
                    "autotrade_expires_at": (r["autotrade_expires_at"].isoformat() if r.get("autotrade_expires_at") else None),
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

            now_utc = dt.datetime.now(dt.timezone.utc)

            # Rolling windows (ideal for dashboard):
            #   - day   = last 24h
            #   - week  = last 7 days
            #   - month = last 30 days
            #
            # This avoids edge-cases at week/month boundaries (e.g., 1st day of month showing 0).
            ranges = {
                "day": (now_utc - dt.timedelta(hours=24), now_utc),
                "week": (now_utc - dt.timedelta(days=7), now_utc),
                "month": (now_utc - dt.timedelta(days=30), now_utc),
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
                    b = await db_store.signal_perf_bucket_global(market, since=since, until=until)
                    trades = int(b.get('trades') or 0)
                    wins = int(b.get('wins') or 0)      # TP2 hits (WIN)
                    losses = int(b.get('losses') or 0)  # SL hits (LOSS)
                    be = int(b.get('be') or 0)          # BE hits
                    tp1 = int(b.get('tp1_hits') or 0)   # TP1 hits (partial)
                    closes = int(b.get('closes') or 0)  # manual CLOSE
                    manual_close = max(0, closes)
                    # 'Signals closed' in dashboard should mean ALL closed outcomes
                    manual = max(0, trades)
                    sum_pnl = float(b.get('sum_pnl_pct') or 0.0)
                    avg_pnl = (sum_pnl / trades) if trades else 0.0
                    out[k] = {
                        "trades": trades,
                        "tp2": wins,
                        "sl": losses,
                        "be": be,
                        "tp1": tp1,
                        "manual": manual,  # back-compat: used by some dashboards as "Signals closed"
                        "closed": trades,
                        "manual_close": manual_close,
                        "sum_pnl_pct": sum_pnl,
                        "avg_pnl_pct": avg_pnl,
                    }
                return out

            perf = {
                "spot": await _mk('SPOT'),
                "futures": await _mk('FUTURES'),
            }

            # Closed positions totals (derived from perf buckets: trades == closed outcomes)
            closed: dict = {}
            try:
                for k in ("day","week","month"):
                    spot_closed = int(((perf.get("spot") or {}).get(k) or {}).get("trades") or 0)
                    fut_closed = int(((perf.get("futures") or {}).get(k) or {}).get("trades") or 0)
                    total_closed = spot_closed + fut_closed
                    closed[k] = total_closed  # legacy
                    closed[f"{k}_total"] = total_closed
                    closed[f"{k}_spot"] = spot_closed
                    closed[f"{k}_futures"] = fut_closed
            except Exception:
                pass


            return web.json_response({
                "ok": True,
                "signals": signals,
                "closed": closed,
                "perf": perf,
            })

        async def save_user(request: web.Request) -> web.Response:
            """Create/update Signal + Auto-trade access for a user.

            Expected JSON:
              telegram_id (required)
              signal_enabled (optional bool)
              signal_expires_at (optional ISO)
              add_days (optional int)  # +days for SIGNAL
              autotrade_enabled (optional bool)
              autotrade_expires_at (optional ISO)
              autotrade_add_days (optional int)  # +days for AUTO-TRADE
              is_blocked (optional bool)
            """
            if not _check_basic(request):
                return _unauthorized()
            data = await request.json()
            tid = int(data.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)

            signal_enabled = bool(data.get("signal_enabled", True))
            signal_expires_raw = data.get("signal_expires_at")
            signal_expires = _parse_iso_dt(signal_expires_raw)
            if signal_expires_raw and (signal_expires is None):
                return web.json_response({"ok": False, "error": "bad signal_expires_at"}, status=400)
            signal_add_days = int(data.get("add_days") or 0)

            autotrade_enabled = bool(data.get("autotrade_enabled", False))
            autotrade_expires_raw = data.get("autotrade_expires_at")
            autotrade_expires = _parse_iso_dt(autotrade_expires_raw)
            if autotrade_expires_raw and (autotrade_expires is None):
                return web.json_response({"ok": False, "error": "bad autotrade_expires_at"}, status=400)
            autotrade_add_days = int(data.get("autotrade_add_days") or 0)

            is_blocked = data.get("is_blocked")

            await ensure_user(tid)
            async with pool.acquire() as conn:
                # SIGNAL
                if signal_add_days:
                    await conn.execute(
                        """UPDATE users
                               SET signal_enabled = TRUE,
                                   signal_expires_at = COALESCE(signal_expires_at, now()) + make_interval(days => $2)
                             WHERE telegram_id=$1""",
                        tid, signal_add_days,
                    )
                else:
                    if signal_expires is not None:
                        await conn.execute(
                            """UPDATE users
                                   SET signal_enabled=$2,
                                       signal_expires_at=$3::timestamptz
                                 WHERE telegram_id=$1""",
                            tid, signal_enabled, signal_expires,
                        )
                    else:
                        await conn.execute(
                            """UPDATE users
                                   SET signal_enabled=$2
                                 WHERE telegram_id=$1""",
                            tid, signal_enabled,
                        )

                # AUTO-TRADE
                if autotrade_add_days:
                    await conn.execute(
                        """UPDATE users
                               SET autotrade_enabled = TRUE,
                                   autotrade_stop_after_close = FALSE,
                                   autotrade_expires_at = COALESCE(autotrade_expires_at, now()) + make_interval(days => $2)
                             WHERE telegram_id=$1""",
                        tid, autotrade_add_days,
                    )
                else:
                    # Admin OFF logic:
                    #  - if no OPEN deals -> OFF immediately
                    #  - if there are OPEN deals -> STOP (no new positions, wait for closes, then finalize -> OFF)
                    open_n = 0
                    try:
                        open_n = int(await conn.fetchval(
                            "SELECT COUNT(1) FROM autotrade_positions WHERE user_id=$1 AND status='OPEN'",
                            tid,
                        ) or 0)
                    except Exception:
                        open_n = 0

                    if not bool(autotrade_enabled):
                        if open_n > 0:
                            # STOP
                            if autotrade_expires is not None:
                                await conn.execute(
                                    """UPDATE users
                                           SET autotrade_enabled=TRUE,
                                               autotrade_stop_after_close=TRUE,
                                               autotrade_expires_at=$2::timestamptz
                                         WHERE telegram_id=$1""",
                                    tid, autotrade_expires,
                                )
                            else:
                                await conn.execute(
                                    """UPDATE users
                                           SET autotrade_enabled=TRUE,
                                               autotrade_stop_after_close=TRUE
                                         WHERE telegram_id=$1""",
                                    tid,
                                )
                        else:
                            # OFF
                            if autotrade_expires is not None:
                                await conn.execute(
                                    """UPDATE users
                                           SET autotrade_enabled=FALSE,
                                               autotrade_stop_after_close=FALSE,
                                               autotrade_expires_at=$2::timestamptz
                                         WHERE telegram_id=$1""",
                                    tid, autotrade_expires,
                                )
                            else:
                                await conn.execute(
                                    """UPDATE users
                                           SET autotrade_enabled=FALSE,
                                               autotrade_stop_after_close=FALSE
                                         WHERE telegram_id=$1""",
                                    tid,
                                )
                    else:
                        # ON
                        if autotrade_expires is not None:
                            await conn.execute(
                                """UPDATE users
                                       SET autotrade_enabled=TRUE,
                                           autotrade_stop_after_close=FALSE,
                                           autotrade_expires_at=$2::timestamptz
                                     WHERE telegram_id=$1""",
                                tid, autotrade_expires,
                            )
                        else:
                            await conn.execute(
                                """UPDATE users
                                       SET autotrade_enabled=TRUE,
                                           autotrade_stop_after_close=FALSE
                                     WHERE telegram_id=$1""",
                                tid,
                            )

                if is_blocked is not None:
                    try:
                        await conn.execute("UPDATE users SET is_blocked=$2 WHERE telegram_id=$1", tid, bool(is_blocked))
                    except Exception:
                        pass

            return web.json_response({"ok": True})
        async def patch_user(request: web.Request) -> web.Response:
            """Compat endpoint: PATCH /api/infra/admin/signal/users/{telegram_id}
            Supports SIGNAL + AUTO-TRADE fields."""
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            data = await request.json()
            data = data or {}
            data["telegram_id"] = tid

            # Reuse the same logic as save_user by performing the updates here
            signal_enabled = bool(data.get("signal_enabled", True))
            signal_expires_raw = data.get("signal_expires_at")
            signal_expires = _parse_iso_dt(signal_expires_raw)
            if signal_expires_raw and (signal_expires is None):
                return web.json_response({"ok": False, "error": "bad signal_expires_at"}, status=400)
            signal_add_days = int(data.get("add_days") or 0)

            autotrade_enabled = bool(data.get("autotrade_enabled", False))
            autotrade_expires_raw = data.get("autotrade_expires_at")
            autotrade_expires = _parse_iso_dt(autotrade_expires_raw)
            if autotrade_expires_raw and (autotrade_expires is None):
                return web.json_response({"ok": False, "error": "bad autotrade_expires_at"}, status=400)
            autotrade_add_days = int(data.get("autotrade_add_days") or 0)

            is_blocked = data.get("is_blocked")

            await ensure_user(tid)
            async with pool.acquire() as conn:
                if signal_add_days:
                    await conn.execute(
                        """UPDATE users
                               SET signal_enabled = TRUE,
                                   signal_expires_at = COALESCE(signal_expires_at, now()) + make_interval(days => $2)
                             WHERE telegram_id=$1""",
                        tid, signal_add_days,
                    )
                else:
                    if signal_expires is not None:
                        await conn.execute(
                            """UPDATE users
                                   SET signal_enabled=$2,
                                       signal_expires_at=$3::timestamptz
                                 WHERE telegram_id=$1""",
                            tid, signal_enabled, signal_expires,
                        )
                    else:
                        await conn.execute(
                            "UPDATE users SET signal_enabled=$2 WHERE telegram_id=$1",
                            tid, signal_enabled,
                        )

                if autotrade_add_days:
                    await conn.execute(
                        """UPDATE users
                               SET autotrade_enabled = TRUE,
                                   autotrade_stop_after_close = FALSE,
                                   autotrade_expires_at = COALESCE(autotrade_expires_at, now()) + make_interval(days => $2)
                             WHERE telegram_id=$1""",
                        tid, autotrade_add_days,
                    )
                else:
                    # same STOP/OFF logic as save_user
                    open_n = 0
                    try:
                        open_n = int(await conn.fetchval(
                            "SELECT COUNT(1) FROM autotrade_positions WHERE user_id=$1 AND status='OPEN'",
                            tid,
                        ) or 0)
                    except Exception:
                        open_n = 0

                    if not bool(autotrade_enabled):
                        if open_n > 0:
                            if autotrade_expires is not None:
                                await conn.execute(
                                    """UPDATE users
                                           SET autotrade_enabled=TRUE,
                                               autotrade_stop_after_close=TRUE,
                                               autotrade_expires_at=$2::timestamptz
                                         WHERE telegram_id=$1""",
                                    tid, autotrade_expires,
                                )
                            else:
                                await conn.execute(
                                    """UPDATE users
                                           SET autotrade_enabled=TRUE,
                                               autotrade_stop_after_close=TRUE
                                         WHERE telegram_id=$1""",
                                    tid,
                                )
                        else:
                            if autotrade_expires is not None:
                                await conn.execute(
                                    """UPDATE users
                                           SET autotrade_enabled=FALSE,
                                               autotrade_stop_after_close=FALSE,
                                               autotrade_expires_at=$2::timestamptz
                                         WHERE telegram_id=$1""",
                                    tid, autotrade_expires,
                                )
                            else:
                                await conn.execute(
                                    """UPDATE users
                                           SET autotrade_enabled=FALSE,
                                               autotrade_stop_after_close=FALSE
                                         WHERE telegram_id=$1""",
                                    tid,
                                )
                    else:
                        if autotrade_expires is not None:
                            await conn.execute(
                                """UPDATE users
                                       SET autotrade_enabled=TRUE,
                                           autotrade_stop_after_close=FALSE,
                                           autotrade_expires_at=$2::timestamptz
                                     WHERE telegram_id=$1""",
                                tid, autotrade_expires,
                            )
                        else:
                            await conn.execute(
                                """UPDATE users
                                       SET autotrade_enabled=TRUE,
                                           autotrade_stop_after_close=FALSE
                                     WHERE telegram_id=$1""",
                                tid,
                            )

                if is_blocked is not None:
                    try:
                        await conn.execute("UPDATE users SET is_blocked=$2 WHERE telegram_id=$1", tid, bool(is_blocked))
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


        # -------- Signal bot: messages & settings (admin) --------

        async def signal_get_settings(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            def _iso(v):
                try:
                    return v.isoformat() if v else None
                except Exception:
                    return None
            try:
                st = await db_store.get_signal_bot_settings()
            except Exception:
                st = {"pause_signals": False, "maintenance_mode": False, "updated_at": None}
            try:
                at = await db_store.get_autotrade_bot_settings()
            except Exception:
                at = {"pause_autotrade": False, "maintenance_mode": False, "updated_at": None}
            return web.json_response({
                "ok": True,
                "pause_signals": bool(st.get("pause_signals")),
                "maintenance_mode": bool(st.get("maintenance_mode")),
                "updated_at": _iso(st.get("updated_at")),
                # Auto-trade global toggles (admin)
                "pause_autotrade": bool(at.get("pause_autotrade")),
                "autotrade_maintenance_mode": bool(at.get("maintenance_mode")),
                "autotrade_updated_at": _iso(at.get("updated_at")),
                "support_username": (st.get("support_username") or SUPPORT_USERNAME),
            })
        async def signal_save_settings(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            data = await request.json()
            pause_signals = bool(data.get("pause_signals"))
            maintenance_mode = bool(data.get("maintenance_mode"))
            # Support username (admin contact). Accept several key variants from UI.
            support_username = (
                data.get("support_username")
                or data.get("SUPPORT_USERNAME")
                or data.get("supportUsername")
                or data.get("support")
                or ""
            )
            if support_username is None:
                support_username = ""
            support_username = str(support_username).strip()
            # Normalize: store without leading '@'
            if support_username.startswith("@"):
                support_username = support_username[1:].strip()
            if not support_username:
                support_username = None

            pause_autotrade = bool(data.get("pause_autotrade"))
            autotrade_maintenance_mode = bool(data.get("autotrade_maintenance_mode"))

            await db_store.set_signal_bot_settings(
                pause_signals=pause_signals,
                maintenance_mode=maintenance_mode,
                support_username=support_username,
            )
            # Apply immediately without restart
            if support_username is not None:
                global SUPPORT_USERNAME
                SUPPORT_USERNAME = str(support_username).lstrip("@").strip()

            await db_store.set_autotrade_bot_settings(
                pause_autotrade=pause_autotrade,
                maintenance_mode=autotrade_maintenance_mode,
            )
            return web.json_response({"ok": True})

        async def signal_broadcast_text(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            data = await request.json()
            text = str(data.get("text") or "").strip()
            if not text:
                return web.json_response({"ok": False, "error": "text required"}, status=400)
            uids = await get_broadcast_user_ids()
            total = len(uids)
            sent = 0
            for uid in uids:
                try:
                    await safe_send(int(uid), text)
                    sent += 1
                except Exception:
                    pass
            return web.json_response({"ok": True, "sent": sent, "total": total})

        async def signal_send_text(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            data = await request.json()
            text = str(data.get("text") or "").strip()
            if not text:
                return web.json_response({"ok": False, "error": "text required"}, status=400)
            try:
                await safe_send(tid, text)
            except Exception as e:
                return web.json_response({"ok": False, "error": str(e)}, status=500)
            return web.json_response({"ok": True})


        # Routes
        app.router.add_route("GET", "/health", health)
        app.router.add_route("GET", "/api/infra/admin/bot/users", bot_list_users)
        app.router.add_route("POST", "/api/infra/admin/bot/users", bot_create_user)
        app.router.add_route("PATCH", "/api/infra/admin/bot/users/{telegram_id}", bot_patch_user)
        app.router.add_route("POST", "/api/infra/admin/bot/users/{telegram_id}/{action}", bot_user_action)
        app.router.add_route("GET", "/api/infra/admin/signal/settings", signal_get_settings)
        app.router.add_route("POST", "/api/infra/admin/signal/settings", signal_save_settings)
        app.router.add_route("GET", "/api/infra/admin/autotrade/settings", autotrade_get_settings)
        app.router.add_route("POST", "/api/infra/admin/autotrade/settings", autotrade_save_settings)
        app.router.add_route("POST", "/api/infra/admin/signal/broadcast", signal_broadcast_text)
        app.router.add_route("POST", "/api/infra/admin/signal/send/{telegram_id}", signal_send_text)
        app.router.add_route("GET", "/api/infra/admin/signal/stats", signal_stats)
        app.router.add_route("GET", "/api/infra/admin/signal/users", list_users)
        app.router.add_route("POST", "/api/infra/admin/signal/users", save_user)
        app.router.add_route("PATCH", "/api/infra/admin/signal/users/{telegram_id}", patch_user)
        app.router.add_route("POST", "/api/infra/admin/signal/users/{telegram_id}/block", block_user)
        app.router.add_route("POST", "/api/infra/admin/signal/users/{telegram_id}/unblock", unblock_user)
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

    # --- Single-instance guard (prevents Telegram getUpdates conflict) ---
    # If multiple containers start simultaneously, only one should run polling & loops.
    async def _try_acquire_singleton_lock() -> bool:
        try:
            async with pool.acquire() as conn:
                v = await conn.fetchval("SELECT pg_try_advisory_lock(8313103750)")
                return bool(v)
        except Exception:
            # If lock query fails, do not block startup.
            return True

    if not await _try_acquire_singleton_lock():
        logger.warning("Another bot instance holds the polling lock; running HTTP server only (no polling/loops)")
        await asyncio.Event().wait()

    logger.info("Starting track_loop")
    if hasattr(backend, "track_loop"):
        asyncio.create_task(backend.track_loop(bot))
    else:
        logger.warning("Backend has no track_loop; skipping")
    logger.info("Starting scanner_loop (15m/1h/4h) interval=%ss top_n=%s", os.getenv('SCAN_INTERVAL_SECONDS',''), os.getenv('TOP_N',''))
    asyncio.create_task(backend.scanner_loop(broadcast_signal, broadcast_macro_alert))
    # ⚡ MID TREND scanner (5m/30m/1h) - optional, does not affect the main scanner
    mid_enabled = os.getenv('MID_SCANNER_ENABLED', '1').strip().lower() not in ('0','false','no','off')
    if mid_enabled:
        if hasattr(backend, 'scanner_loop_mid'):
            logger.info("Starting MID scanner_loop (5m/30m/1h) interval=%ss top_n=%s", os.getenv('MID_SCAN_INTERVAL_SECONDS',''), os.getenv('MID_TOP_N',''))
            asyncio.create_task(backend.scanner_loop_mid(broadcast_signal, broadcast_macro_alert))
        else:
            logger.warning('MID_SCANNER_ENABLED=1 but Backend has no scanner_loop_mid; skipping')

    logger.info("Starting signal_outcome_loop")
    asyncio.create_task(signal_outcome_loop())

    # Auto-trade manager (SL/TP/BE) - runs in background.
    asyncio.create_task(autotrade_manager_loop(notify_api_error=_notify_autotrade_api_error))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
