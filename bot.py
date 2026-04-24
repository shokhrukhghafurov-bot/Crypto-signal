
from __future__ import annotations

import asyncio
import random
import difflib
import json
import os
import logging
import re
import math
import statistics
import socket
import uuid
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import replace

from aiogram import Bot, Dispatcher, types
from aiogram.types import BufferedInputFile, InputMediaPhoto
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from dotenv import load_dotenv


def _load_env_early() -> None:
    """Load .env before importing backend, with a resilient fallback for key vars."""
    load_dotenv()
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    try:
        raw = env_path.read_text(encoding="utf-8")
    except Exception:
        return
    # Fallback for cases where a broken/multiline value in .env prevents normal parsing.
    for name in ("AUTOTRADE_MASTER_KEY",):
        if os.getenv(name):
            continue
        m = re.search(rf"""(?m)^\s*{name}\s*=\s*(?:"([^"\r\n]*)"|'([^'\r\n]*)'|([^#\r\n]*))\s*$""", raw)
        if not m:
            continue
        value = next((g for g in m.groups() if g is not None), "").strip()
        if value:
            os.environ[name] = value


_load_env_early()
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import datetime as dt

import asyncpg
from aiohttp import web, ClientSession

import db_store

from backend import Backend, Signal, MacroEvent, open_metrics, validate_autotrade_keys, ExchangeAPIError, autotrade_execute, autotrade_manager_loop, autotrade_healthcheck, autotrade_stress_test, mid_summary_heartbeat_loop, autotrade_anomaly_watchdog_loop, ws_candles_service_loop, candles_cache_cleanup_loop
import time
import hashlib
import hmac
import io
from decimal import Decimal, InvalidOperation
from openpyxl import Workbook
import pandas as pd

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

# --- Telegram webhook safe setup (prevents flood control 429) ---
async def _ensure_webhook(bot: Bot, target_url: str, *, secret_token: str | None = None, max_tries: int = 10) -> bool:
    """Idempotent + flood-control-safe setWebhook.

    - Checks current webhook with getWebhookInfo()
    - If already set to target_url, does nothing
    - On Telegram flood control (RetryAfter/429), waits the suggested time
    - Uses exponential backoff with jitter for other transient errors
    """
    # 1) Check existing webhook first (avoid redundant setWebhook calls)
    try:
        info = await bot.get_webhook_info()
        if getattr(info, "url", "") == target_url:
            logger.info("Webhook already set: %s", target_url)
            return True
    except Exception as e:
        logger.warning("getWebhookInfo failed (will still try setWebhook): %r", e)

    delay = 2.0
    for attempt in range(1, max_tries + 1):
        try:
            # drop_pending_updates=False so we don't lose updates on redeploy
            await bot.set_webhook(url=target_url, secret_token=secret_token, drop_pending_updates=False)
            logger.info("Webhook set to %s", target_url)
            return True
        except Exception as e:
            # aiogram v3: TelegramRetryAfter has `retry_after`
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None:
                wait_s = float(retry_after) + 1.0
                logger.warning("Telegram flood control on setWebhook. Retry after %.1fs", wait_s)
                await asyncio.sleep(wait_s)
                continue

            # Generic backoff
            jitter = random.uniform(0.0, 0.5)
            logger.warning("setWebhook failed (attempt %s/%s): %r; sleeping %.1fs",
                           attempt, max_tries, e, delay + jitter)
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2.0, 60.0)

    return False


# --- runtime health/status (autotrade & smart-manager) ---
TASKS: dict[str, asyncio.Task] = {}
HEALTH: dict[str, float] = {"bot_start": time.time()}
HEALTH_LAST_ERR: dict[str, str] = {}

def _health_mark_ok(name: str) -> None:
    HEALTH[f"{name}_last_ok"] = time.time()

def _health_mark_err(name: str, err: str) -> None:
    HEALTH_LAST_ERR[name] = (err or "").strip()[:500]
    HEALTH[f"{name}_last_err"] = time.time()

def _task_state(t: asyncio.Task | None) -> str:
    if not t:
        return "missing"
    if t.cancelled():
        return "cancelled"
    if t.done():
        exc = t.exception()
        return f"done(exc={type(exc).__name__})" if exc else "done(ok)"
    return "running"


_TASK_RESTART_INFLIGHT: set[str] = set()


def _spawn_smart_manager_task() -> asyncio.Task | None:
    if not hasattr(backend, "track_loop"):
        logger.warning("Backend has no track_loop; skipping smart-manager spawn")
        return None
    try:
        backend.health_tick_cb = lambda: _health_mark_ok("smart-manager")
    except Exception:
        pass
    task = asyncio.create_task(backend.track_loop(bot), name="smart-manager")
    TASKS["smart-manager"] = task
    _health_mark_ok("smart-manager")
    _attach_task_monitor("smart-manager", task)
    return task


def _start_signal_outcome_task() -> asyncio.Task:
    existing = TASKS.get("signal-outcome")
    if existing and not existing.done():
        logger.warning("signal_outcome_loop already running; skip duplicate start")
        return existing
    task = asyncio.create_task(signal_outcome_loop(), name="signal-outcome")
    TASKS["signal-outcome"] = task
    _attach_task_monitor("signal-outcome", task)
    return task


async def _restart_task_later(name: str, reason: str = "") -> None:
    if name != "smart-manager":
        return
    if name in _TASK_RESTART_INFLIGHT:
        return
    _TASK_RESTART_INFLIGHT.add(name)
    try:
        delay = max(1.0, float(os.getenv("SMART_MANAGER_RESTART_DELAY_SEC", "5") or 5))
        await asyncio.sleep(delay)
        cur = TASKS.get(name)
        if cur and not cur.done():
            return
        logger.warning("[smart-manager] restarting task after %s", reason or "unexpected stop")
        _spawn_smart_manager_task()
    finally:
        _TASK_RESTART_INFLIGHT.discard(name)

async def _health_status_loop() -> None:
    interval = int(os.getenv("HEALTH_STATUS_INTERVAL_SEC", "60") or 60)
    dead_after = float(os.getenv("HEALTH_DEAD_AFTER_SEC", "300") or 300)
    # don't spam on boot
    await asyncio.sleep(5)
    while True:
        try:
            now = time.time()
            at_global = TASKS.get("autotrade-global")
            smart = TASKS.get("smart-manager")
            at_mgr = TASKS.get("autotrade-manager")
            def age(key: str) -> float | None:
                t = float(HEALTH.get(key, 0.0) or 0.0)
                return None if t <= 0 else max(0.0, now - t)

            def fmt_age(v: float | None) -> str:
                return "n/a" if v is None else f"{int(v)}s"

            def status(task: asyncio.Task | None, ok_age: float | None) -> str:
                if not task:
                    return "MISSING"
                if task.cancelled():
                    return "CANCELLED"
                if task.done():
                    return "DEAD"
                if ok_age is None:
                    return "RUNNING"
                return "DEAD" if ok_age >= dead_after else "RUNNING"

            at_ok = age("autotrade-global_last_ok")
            at_mgr_ok = age("autotrade-manager_last_ok")
            sm_ok = age("smart-manager_last_ok")

            msg = (
                "[health] "
                f"{'✅' if status(at_global, at_ok)=='RUNNING' else '❌'} autotrade=\"{status(at_global, at_ok)}\" last_ok={fmt_age(at_ok)} "
                f"| {'✅' if status(at_mgr, at_mgr_ok)=='RUNNING' else '❌'} autotrade_mgr=\"{status(at_mgr, at_mgr_ok)}\" last_ok={fmt_age(at_mgr_ok)} "
                f"| {'✅' if status(smart, sm_ok)=='RUNNING' else '❌'} smart_manager=\"{status(smart, sm_ok)}\" last_ok={fmt_age(sm_ok)}"
            )
            logger.info(msg)
        except Exception:
            logger.exception("[health] status loop failed")
        await asyncio.sleep(max(10, interval))


def _attach_task_monitor(name: str, task: asyncio.Task) -> None:
    """Ensure crashes are visible (log + error-bot) and reflected in health."""
    try:
        def _cb(t: asyncio.Task) -> None:
            try:
                if t.cancelled():
                    logger.warning("[task] cancelled name=%s", name)
                    if name == "smart-manager":
                        _health_mark_err("smart-manager", "task_cancelled")
                        try:
                            asyncio.create_task(_restart_task_later("smart-manager", reason="cancelled"))
                        except Exception:
                            pass
                    return
                exc = t.exception()
                if not exc:
                    return
                if name.startswith("autotrade"):
                    logger.error("[autotrade] task crashed name=%s exc=%s", name, repr(exc), exc_info=exc)
                    if "manager" in name:
                        _health_mark_err("autotrade-manager", f"task_crash:{type(exc).__name__}")
                    else:
                        _health_mark_err("autotrade-global", f"task_crash:{type(exc).__name__}")
                elif name == "smart-manager":
                    logger.error("[smart-manager] task crashed exc=%s", repr(exc), exc_info=exc)
                    _health_mark_err("smart-manager", f"task_crash:{type(exc).__name__}")
                    try:
                        asyncio.create_task(_restart_task_later("smart-manager", reason=f"exception:{type(exc).__name__}"))
                    except Exception:
                        pass
                else:
                    logger.error("[task] crashed name=%s exc=%s", name, repr(exc), exc_info=exc)
            except Exception:
                return

        task.add_done_callback(_cb)
    except Exception:
        return

async def _db_retention_cleanup_loop() -> None:
    """Periodically prune old closed Postgres rows to keep tables/indexes compact."""
    enabled = str(os.getenv("DB_RETENTION_CLEANUP_ENABLED", "1") or "1").strip().lower() not in ("0", "false", "no", "off")
    if not enabled:
        logger.info("[db-retention] disabled (DB_RETENTION_CLEANUP_ENABLED=0)")
        return

    every_sec = int(os.getenv("DB_RETENTION_CLEANUP_EVERY_SEC", "21600") or 21600)
    startup_delay = int(os.getenv("DB_RETENTION_CLEANUP_STARTUP_DELAY_SEC", "180") or 180)
    batch = int(os.getenv("DB_RETENTION_DELETE_BATCH", "1000") or 1000)

    logger.info("[db-retention] started interval=%ss startup_delay=%ss batch=%s", every_sec, startup_delay, batch)
    await asyncio.sleep(max(5, startup_delay))

    while True:
        try:
            stats = await db_store.cleanup_old_postgres_data(batch_size=batch)
            total_deleted = int(sum(int(v or 0) for v in (stats or {}).values()))
            logger.info(
                "[db-retention] deleted total=%s closed_trades=%s closed_autotrade=%s closed_signal_tracks=%s signal_sent=%s",
                total_deleted,
                int((stats or {}).get("closed_trades") or 0),
                int((stats or {}).get("closed_autotrade_positions") or 0),
                int((stats or {}).get("closed_signal_tracks") or 0),
                int((stats or {}).get("signal_sent_events") or 0),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[db-retention] cleanup failed")
        await asyncio.sleep(max(300, every_sec))


# --- i18n template safety guard (prevents leaking {placeholders} to users) ---
_UNFILLED_RE = re.compile(r'(?<!\{)\{[a-zA-Z0-9_]+\}(?!\})')



async def _candles_cache_purge_loop() -> None:
    """Periodically purge persistent candles cache to keep DB small."""
    if not pool:
        return
    every = int(os.getenv("MID_PERSIST_CANDLES_PURGE_EVERY_SEC", "300") or 300)
    max_age = int(os.getenv("MID_PERSIST_CANDLES_MAX_AGE_SEC", os.getenv("MID_CANDLES_CACHE_STALE_SEC", "1800")) or 1800)
    # small delay on boot
    await asyncio.sleep(10)
    while True:
        try:
            deleted = await db_store.candles_cache_purge(max_age)
            if deleted:
                logger.info("[candles-cache] purged=%s max_age_sec=%s", deleted, max_age)
        except Exception as e:
            logger.warning("[candles-cache] purge failed: %s", e)
        await asyncio.sleep(every)

async def _task_heartbeat_loop(task_name: str, *, interval_sec: float = 5.0) -> None:
    """Keeps health 'last_ok' fresh while the task is alive (prevents false DEAD)."""
    while True:
        try:
            await asyncio.sleep(max(1.0, float(interval_sec)))
            t = TASKS.get(task_name)
            if t and not t.done():
                _health_mark_ok(task_name)
        except asyncio.CancelledError:
            raise
        except Exception:
            # never crash the heartbeat
            pass

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
    if not (text or "").strip():
        logger.warning("safe_send: skip empty text chat_id=%s ctx=%s", chat_id, ctx)
        return None
    # Never recurse. Send via bot API.
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except TelegramForbiddenError:
        # User blocked the bot or cannot be contacted
        await set_user_blocked(chat_id, blocked=True)
        raise
    except TelegramBadRequest as e:
        msg = str(e).lower()
        if "chat not found" in msg:
            # Chat does not exist / user never started the bot
            await set_user_blocked(chat_id, blocked=True)
            raise

        # Common failure modes when sending large/Markdown-heavy texts.
        # 1) Message too long -> send in chunks (Telegram hard limit is 4096).
        if "message is too long" in msg:
            # parse_mode can still break entities; drop it for chunked send.
            kwargs.pop("parse_mode", None)
            max_len = 3800

            parts: list[str] = []
            cur = ""
            for line in text.splitlines(True):
                if len(cur) + len(line) > max_len and cur:
                    parts.append(cur)
                    cur = ""
                cur += line
            if cur:
                parts.append(cur)

            last_res = None
            for idx, part in enumerate(parts or [text[:max_len]]):
                # reply_markup only on first part to avoid clutter
                send_kwargs = dict(kwargs)
                if "reply_markup" in send_kwargs and idx != 0:
                    send_kwargs["reply_markup"] = None
                last_res = await bot.send_message(chat_id, part, **send_kwargs)
            return last_res

        # 2) Markdown/HTML entity parsing issues -> retry as plain text.
        if "can't parse entities" in msg or "can\u2019t parse entities" in msg:
            if "parse_mode" in kwargs:
                try:
                    kwargs.pop("parse_mode", None)
                    return await bot.send_message(chat_id, text, **kwargs)
                except Exception:
                    raise
        raise


async def safe_send_nonempty(chat_id: int, text: str, *, ctx: str = "", **kwargs):
    text = (text or "").strip()
    if not text:
        return None
    return await safe_send(chat_id, text, ctx=ctx, **kwargs)

async def safe_send_payment_alert(chat_id: int, text: str, *, ctx: str = "payment_alert", **kwargs):
    text = _sanitize_template_text(chat_id, text, ctx=ctx)
    if not (text or "").strip():
        logger.warning("[payment-alert] skip empty text ctx=%s chat_id=%s", ctx, chat_id)
        return None
    if _payment_bot is None:
        logger.error("[payment-alert] PAYMENT_BOT_TOKEN is missing or invalid; skip sending ctx=%s chat_id=%s", ctx, chat_id)
        return None
    try:
        return await _payment_bot.send_message(chat_id, text, **kwargs)
    except Exception:
        logger.exception("[payment-alert] send failed ctx=%s chat_id=%s", ctx, chat_id)
        return None

async def safe_edit_text(chat_id: int, message_id: int, text: str, *, ctx: str = "", **kwargs):
    text = _sanitize_template_text(chat_id, text, ctx=ctx)
    if not (text or "").strip():
        logger.warning("safe_edit_text: skip empty text chat_id=%s message_id=%s ctx=%s", chat_id, message_id, ctx)
        return None
    return await bot.edit_message_text(text=text, chat_id=chat_id, message_id=message_id, **kwargs)


async def safe_callback_answer(call: types.CallbackQuery, *args, **kwargs) -> bool:
    """Answer callback queries without crashing on stale/expired Telegram query ids.

    Telegram callback queries must be answered quickly. When webhook processing runs
    in background, the query may already be expired by the time we answer it.
    In that case Telegram returns:
      - query is too old and response timeout expired
      - query ID is invalid
    These are harmless for UX and must not crash handlers or create asyncio noise.
    """
    try:
        await call.answer(*args, **kwargs)
        return True
    except TelegramBadRequest as e:
        msg = str(e).lower()
        if (
            "query is too old" in msg
            or "response timeout expired" in msg
            or "query id is invalid" in msg
        ):
            try:
                logger.info(
                    "Skip expired callback answer uid=%s data=%s err=%s",
                    getattr(getattr(call, "from_user", None), "id", None),
                    getattr(call, "data", None),
                    e,
                )
            except Exception:
                pass
            return False
        raise


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

# Backward-compatible alias for old handlers that still call is_admin(...).
def is_admin(user_id: int) -> bool:
    return _is_admin(user_id)

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
BOT_PUBLIC_USERNAME = (os.getenv("BOT_USERNAME") or "").strip().lstrip("@")

# Dedicated bot for payment/admin notifications.
# IMPORTANT: there is intentionally NO fallback to the main bot.
# If PAYMENT_BOT_TOKEN is missing/invalid, payment alerts are skipped and logged.
PAYMENT_BOT_TOKEN = (os.getenv("PAYMENT_BOT_TOKEN") or "").strip()
_payment_bot: Bot | None = None
if PAYMENT_BOT_TOKEN:
    try:
        _payment_bot = Bot(PAYMENT_BOT_TOKEN)
        logger.info("[payment-alert] dedicated payment bot enabled")
    except Exception:
        _payment_bot = None
        logger.exception("[payment-alert] failed to initialize payment bot")
else:
    logger.warning("[payment-alert] PAYMENT_BOT_TOKEN is not set; payment alerts will NOT be sent")

# Dedicated report bot for closed signal cards.
# It sends formatted outcome cards (WIN / LOSS / BE / CLOSED) to configured chats.
REPORT_BOT_TOKEN = (os.getenv("REPORT_BOT_TOKEN") or "").strip()
_REPORT_BOT_CHAT_IDS_RAW = (os.getenv("REPORT_BOT_CHAT_IDS") or "").strip()
REPORT_BOT_CHAT_IDS: list[int] = []
if _REPORT_BOT_CHAT_IDS_RAW:
    for _part in _REPORT_BOT_CHAT_IDS_RAW.split(","):
        _part = _part.strip()
        if not _part:
            continue
        try:
            REPORT_BOT_CHAT_IDS.append(int(_part))
        except Exception:
            logger.warning("[report-bot] invalid chat id in REPORT_BOT_CHAT_IDS: %r", _part)
if not REPORT_BOT_CHAT_IDS and ADMIN_IDS:
    REPORT_BOT_CHAT_IDS = list(ADMIN_IDS)

_report_bot: Bot | None = None
if REPORT_BOT_TOKEN:
    try:
        _report_bot = Bot(REPORT_BOT_TOKEN)
        logger.info("[report-bot] enabled chats=%s", REPORT_BOT_CHAT_IDS)
    except Exception:
        _report_bot = None
        logger.exception("[report-bot] failed to initialize")
else:
    logger.info("[report-bot] REPORT_BOT_TOKEN is not set; closed signal report cards are disabled")


logger.info("[mid][dbg] methods can_add_mid_pending=%s add_mid_pending=%s mid_pending_trigger_loop=%s scanner_loop_mid=%s",
            callable(getattr(backend, "can_add_mid_pending", None)),
            callable(getattr(backend, "add_mid_pending", None)),
            callable(getattr(backend, "mid_pending_trigger_loop", None)),
            callable(getattr(backend, "scanner_loop_mid", None)))
# ---------------- Admin/Error bot alerts (separate bot) ----------------
# Send important errors / blocks to a dedicated Telegram bot to avoid polluting the main bot chat.
ERROR_BOT_TOKEN = (os.getenv("ERROR_BOT_TOKEN") or "").strip()
ADMIN_ALERT_CHAT_ID = int(os.getenv("ADMIN_ALERT_CHAT_ID", "5090106525") or 5090106525)
REFERRAL_ADMIN_CHAT_ID = int(os.getenv("REFERRAL_ADMIN_CHAT_ID", str(ADMIN_ALERT_CHAT_ID)) or ADMIN_ALERT_CHAT_ID)
REFERRAL_ADMIN_BOT_USERNAME = (os.getenv("REFERRAL_ADMIN_BOT_USERNAME") or "Payreferralbot").lstrip('@')
REFERRAL_MIN_WITHDRAW_USDT = float(os.getenv("REFERRAL_MIN_WITHDRAW_USDT", "10") or 10)
REFERRAL_REWARD_PERCENT = float(os.getenv("REFERRAL_REWARD_PERCENT", "10") or 10)
REFERRAL_NETWORK = (os.getenv("REFERRAL_NETWORK") or "BSC (BEP20)").strip() or "BSC (BEP20)"
# Runtime flag: users currently entering a referral withdrawal wallet.
REFERRAL_WITHDRAW_INPUT: Dict[int, bool] = {}
# Users with manual referral access when the global switch is OFF.
REFERRAL_ACCESS_USERS: set[int] = set()

# ---------------- MID digest sender (USES ERROR BOT via ERROR_BOT_TOKEN) ----------------
# Digest is diagnostics; it should go to @errorrrrrrg_bot (ERROR_BOT_TOKEN), not the main bot.

async def _mid_digest_send(text: str) -> None:
    # _error_bot_send is defined below (after error-bot setup). Name is resolved at runtime.
    try:
        await _error_bot_send(text)
    except Exception:
        return

# expose to backend (scanner_loop_mid checks for this)
backend.emit_mid_digest = _mid_digest_send


# What to forward (defaults are SAFE: only critical + scanner errors + MID blocks; ignore stablecoin SCAN_BLOCK spam)
ERROR_BOT_ENABLED = (os.getenv("ERROR_BOT_ENABLED", "1").strip().lower() not in ("0","false","no","off"))
ERROR_BOT_SEND_SCAN_BLOCK = (os.getenv("ERROR_BOT_SEND_SCAN_BLOCK", "0").strip().lower() not in ("0","false","no","off"))
ERROR_BOT_SEND_MID_BLOCK = (os.getenv("ERROR_BOT_SEND_MID_BLOCK", "0").strip().lower() not in ("0","false","no","off"))
ERROR_BOT_SEND_SCANNER_ERRORS = (os.getenv("ERROR_BOT_SEND_SCANNER_ERRORS", "1").strip().lower() not in ("0","false","no","off"))
ERROR_BOT_SEND_AUTOTRADE_ERRORS = (os.getenv("ERROR_BOT_SEND_AUTOTRADE_ERRORS", "1").strip().lower() not in ("0","false","no","off"))
ERROR_BOT_SEND_SMART_ERRORS = (os.getenv("ERROR_BOT_SEND_SMART_ERRORS", "1").strip().lower() not in ("0","false","no","off"))
ERROR_BOT_SEND_LEVEL = (os.getenv("ERROR_BOT_SEND_LEVEL", "ERROR") or "ERROR").upper().strip()  # ERROR/WARNING/INFO

# Patterns we NEVER forward (noisy, user/API-key problems, stablecoin scan blocks)
_ERROR_BOT_IGNORE_SUBSTRINGS = tuple(s.strip().lower() for s in (os.getenv("ERROR_BOT_IGNORE_SUBSTRINGS") or "").split(",") if s.strip())
# built-in ignores
_ERROR_BOT_IGNORE_DEFAULT = (
    "telegramconflicterror",
    "terminated by other getupdates request",
    "other getupdates request",
)
_ERROR_BOT_IGNORE_SCAN_BLOCK = ("scan_block", "blocked_stable_pair", "blocked_symbol_list")  # stablecoin & hardblock noise

_error_bot: Bot | None = None
if ERROR_BOT_TOKEN and ERROR_BOT_ENABLED:
    try:
        _error_bot = Bot(ERROR_BOT_TOKEN)
    except Exception:
        _error_bot = None

# Simple per-chat rate limiter for error bot sends (prevents Telegram flood control)
_ERROR_SEND_LOCKS: dict[int, asyncio.Lock] = {}
_ERROR_LAST_SENT_AT: dict[int, float] = {}
_HEALTH_LAST_STATUS: dict[str, str] = {}

_ERROR_MIN_INTERVAL_SEC = float(os.getenv("ERROR_BOT_MIN_INTERVAL_SEC", "0.8") or 0.8)

async def _error_bot_send(text: str) -> None:
    if not _error_bot or not ERROR_BOT_ENABLED:
        return
    chat_id = ADMIN_ALERT_CHAT_ID
    lock = _ERROR_SEND_LOCKS.setdefault(chat_id, asyncio.Lock())
    async with lock:
        # spacing
        now = time.time()
        last = _ERROR_LAST_SENT_AT.get(chat_id, 0.0)
        wait = _ERROR_MIN_INTERVAL_SEC - (now - last)
        if wait > 0:
            await asyncio.sleep(wait)
        # send with retry-after handling
        for _ in range(3):
            try:
                await _error_bot.send_message(chat_id, text[:3900])
                _ERROR_LAST_SENT_AT[chat_id] = time.time()
                return
            except TelegramRetryAfter as e:
                await asyncio.sleep(float(getattr(e, "retry_after", 1.0)))
            except Exception:
                return

async def _error_bot_send_autotrade_event(kind: str, **data) -> None:
    try:
        lines = [f"🤖 {str(kind or '').upper()}"]
        for k, v in data.items():
            if v is None or v == "":
                continue
            lines.append(f"{k}={v}")
        await _error_bot_send("\n".join(lines))
    except Exception:
        pass

def _should_forward_to_error_bot(levelno: int, msg: str) -> bool:
    """
    Production rules:
    - Do NOT spam @errorrrrrrg_bot with normal diagnostics (MID blocked, autotrade skipped, health RUNNING).
    - Send only REAL problems: exceptions/tracebacks, balance/order/exchange errors, or health transition RUNNING->DEAD.
    """
    text = (msg or "")
    m = text.lower()

    # 1) Always ignore very common non-error noise
    if "mid blocked" in m:
        return False
    if "[scanner] skip" in m or "blocked stable pair" in m:
        return False

    # 2) Health messages: send ONLY when a service transitions to DEAD
    if m.startswith("[health]") or "[health]" in m:
        if os.getenv("ERROR_BOT_SEND_HEALTH_DEAD", "1").strip().lower() in ("0", "false", "no", "off"):
            return False
        # parse statuses like autotrade="RUNNING" / autotrade_mgr="DEAD" / smart_manager="RUNNING"
        changed_to_dead = False
        for name, status in re.findall(r'(autotrade_mgr|autotrade|smart_manager)\s*=\s*"(RUNNING|DEAD)"', text):
            prev = _HEALTH_LAST_STATUS.get(name)
            _HEALTH_LAST_STATUS[name] = status
            if status == "DEAD" and prev != "DEAD":
                changed_to_dead = True
        return changed_to_dead

    # 3) Autotrade skipped is NOT an error (autotrade disabled / filtered / no confirmations, etc.)
    if ("[autotrade]" in m or "auto-trade" in m or "autotrade " in m or "autotrade_" in m):
        if os.getenv("ERROR_BOT_IGNORE_AUTOTRADE_SKIPPED", "1").strip().lower() not in ("0","false","no","off"):
            if " reason=skipped" in m or "[autotrade] skipped" in m or "autotrade skipped" in m:
                return False
            # some logs format: "Auto-trade skipped: <reason>"
            if "auto-trade skipped" in m and ("cannot read balance" not in m and "balance" not in m):
                return False

    # 4) Ignore default substrings (unless it's clearly an error)
    is_autotrade = ("[autotrade]" in m or "auto-trade" in m or "autotrade " in m or "autotrade_" in m)
    is_smart = ("[smart-manager]" in m or "smart manager" in m or "smart_be" in m or "smart-be" in m)

    if "scan_block" in m and any(x in m for x in _ERROR_BOT_IGNORE_SCAN_BLOCK):
        return False

    if not is_autotrade and not is_smart:
        if any(x in m for x in _ERROR_BOT_IGNORE_DEFAULT):
            return False
        if _ERROR_BOT_IGNORE_SUBSTRINGS and any(x in m for x in _ERROR_BOT_IGNORE_SUBSTRINGS):
            return False

    # 5) Define "real error" markers for non-ERROR logs
    real_error_markers = (
        "traceback", "exception", "exchangeapier", "cannot read balance", "balance error",
        "invalid key", "api key", "permission", "timeout", "timed out",
        "order failed", "failed to place", "failed to execute", "connection error",
        "network", "http 5", "rate limit", "too many requests", "denied",
        "close failed", "failed to close", "close_market failed", "close market failed",
        "not closed", "could not close", "unable to close", "reduce-only",
        "dead"
    )
    is_real_error = any(x in m for x in real_error_markers)

    # 6) Always forward ERROR+ (respect configured level), but still avoid noisy warnings/info unless real error
    min_level = getattr(logging, ERROR_BOT_SEND_LEVEL, logging.ERROR)
    if levelno >= logging.ERROR:
        return True

    if levelno >= min_level and is_real_error:
        return True

    if levelno >= logging.INFO and any(x in m for x in (
        "close failed", "failed to close", "close_market failed", "close market failed",
        "not closed", "could not close", "unable to close"
    )):
        return True

    # 7) Optional: scanner/mid/scan_block forwarding (still filtered above)
    if ERROR_BOT_SEND_SCANNER_ERRORS and ("error scanner" in m or "scanner error" in m):
        return True
    if ERROR_BOT_SEND_MID_BLOCK and ("mid blocked" in m or "mid_block" in m):
        return False  # handled earlier (we don't forward these)
    if ERROR_BOT_SEND_SCAN_BLOCK and ("scan_block" in m):
        return True

    # 8) Autotrade/Smart: forward ONLY when it's a real error
    if ERROR_BOT_SEND_AUTOTRADE_ERRORS and is_autotrade and is_real_error:
        return True
    if ERROR_BOT_SEND_SMART_ERRORS and is_smart and is_real_error:
        return True

    return False


# --- Error-bot aggregation (anti-spam) ---
_ERROR_AGG_ENABLED = (os.getenv("ERROR_BOT_AGG_ENABLED", "1").strip().lower() not in ("0","false","no","off"))
_ERROR_AGG_WINDOW_SEC = float(os.getenv("ERROR_BOT_AGG_WINDOW_SEC", "120") or 120)  # group repeats within this window
_ERROR_AGG_MAX_KEYS = int(float(os.getenv("ERROR_BOT_AGG_MAX_KEYS", "200") or 200))

_ERROR_AGG: dict[tuple[int,str,str], dict] = {}

def _fmt_ts(ts: float) -> str:
    try:
        return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)

async def _error_agg_flush(key: tuple[int,str,str]) -> None:
    try:
        await asyncio.sleep(max(0.0, _ERROR_AGG_WINDOW_SEC))
        ent = _ERROR_AGG.get(key)
        if not ent:
            return
        cnt = int(ent.get("count") or 0)
        if cnt <= 1:
            # first message already sent; nothing else to do
            _ERROR_AGG.pop(key, None)
            return
        levelno, logger_name, msg = key
        first_ts = float(ent.get("first_ts") or 0.0)
        last_ts = float(ent.get("last_ts") or first_ts)
        base_msg = str(msg or "")
        if base_msg.startswith("❗ "):
            text = base_msg + f"\nrepeat_count={cnt}\nfirst: {_fmt_ts(first_ts)}\nlast:  {_fmt_ts(last_ts)}"
        else:
            text = (
                f"❗ {logging.getLevelName(levelno)} x{cnt} (aggregated {int(_ERROR_AGG_WINDOW_SEC)}s)\n"
                f"{logger_name}\n"
                f"{base_msg}\n"
                f"first: {_fmt_ts(first_ts)}\n"
                f"last:  {_fmt_ts(last_ts)}"
            )
        await _error_bot_send(text)
        _ERROR_AGG.pop(key, None)
    except Exception:
        _ERROR_AGG.pop(key, None)

def _error_agg_note(levelno: int, logger_name: str, msg: str) -> None:
    immediate_text = str(msg or "")
    if not immediate_text.startswith("❗ "):
        immediate_text = f"❗ {logging.getLevelName(levelno)}\n{logger_name}\n{immediate_text}"
    if not _ERROR_AGG_ENABLED or _ERROR_AGG_WINDOW_SEC <= 0:
        asyncio.create_task(_error_bot_send(immediate_text[:3900]))
        return

    # memory guard
    if len(_ERROR_AGG) >= _ERROR_AGG_MAX_KEYS:
        _ERROR_AGG.clear()

    key = (int(levelno), str(logger_name), str(msg))
    now = time.time()
    ent = _ERROR_AGG.get(key)
    if not ent:
        _ERROR_AGG[key] = {"count": 1, "first_ts": now, "last_ts": now}
        # Send first occurrence immediately
        immediate_text = str(msg or "")
        if not immediate_text.startswith("❗ "):
            immediate_text = f"❗ {logging.getLevelName(levelno)}\n{logger_name}\n{immediate_text}"
        asyncio.create_task(_error_bot_send(immediate_text[:3900]))
        # Flush summary later if repeated
        asyncio.create_task(_error_agg_flush(key))
        return

    ent["count"] = int(ent.get("count") or 0) + 1
    ent["last_ts"] = now




# --- MID trap digest (group by reason_key, send once per window) ---
_MID_TRAP_DIGEST_ENABLED = (os.getenv("MID_TRAP_DIGEST_ENABLED", "1").strip().lower() not in ("0","false","no","off"))
_mid_trap_window_env = float(os.getenv("MID_TRAP_DIGEST_WINDOW_SEC", "21600") or 21600)
_MID_TRAP_DIGEST_WINDOW_SEC = max(_mid_trap_window_env, 21600.0)  # force >= 6 hours
_MID_TRAP_DIGEST_MAX_REASONS = int(float(os.getenv("MID_TRAP_DIGEST_MAX_REASONS", "5") or 5))
_MID_TRAP_DIGEST_EXAMPLES_PER_REASON = int(float(os.getenv("MID_TRAP_DIGEST_EXAMPLES_PER_REASON", "2") or 2))
_MID_TRAP_DIGEST_MAX_EVENTS = int(float(os.getenv("MID_TRAP_DIGEST_MAX_EVENTS", "500") or 500))

_mid_trap_events: list[dict] = []
_mid_trap_lock = asyncio.Lock()
_mid_trap_last_flush_ts = 0.0

def _mid_trap_note(event: dict) -> None:
    """Called from backend trap sink (sync)."""
    try:
        if not _MID_TRAP_DIGEST_ENABLED:
            return
        # schedule async append safely
        async def _append():
            async with _mid_trap_lock:
                if len(_mid_trap_events) >= _MID_TRAP_DIGEST_MAX_EVENTS:
                    _mid_trap_events.pop(0)
                _mid_trap_events.append(dict(event))
        asyncio.get_event_loop().create_task(_append())
    except Exception:
        pass

def _build_mid_trap_digest(events: list[dict]) -> str:
    # group by reason_key
    by: dict[str, list[dict]] = {}
    for e in events:
        rk = str(e.get("reason_key") or e.get("reason") or "unknown").strip() or "unknown"
        by.setdefault(rk, []).append(e)

    items = sorted(by.items(), key=lambda kv: len(kv[1]), reverse=True)
    items = items[:max(1, _MID_TRAP_DIGEST_MAX_REASONS)]
    total = len(events)

    # dynamic window label
    w = int(_MID_TRAP_DIGEST_WINDOW_SEC)
    if w % 3600 == 0:
        wlabel = f"{w//3600}h"
    elif w % 60 == 0:
        wlabel = f"{w//60}m"
    else:
        wlabel = f"{w}s"

    lines = [f"🧪 MID trap digest ({wlabel}) — blocks: {total}"]

    for rk, lst in items:
        lines.append(f"• {rk}: {len(lst)}")
        ex = lst[:max(1, _MID_TRAP_DIGEST_EXAMPLES_PER_REASON)]
        for s in ex:
            d = str(s.get('dir') or '').upper()
            en = s.get('entry', None)
            reason = str(s.get('reason') or '').strip()
            try:
                en_txt = f"{float(en):.6g}" if en is not None else "—"
            except Exception:
                en_txt = "—"
            lines.append(f"  - {d} entry={en_txt} {reason}")

    if len(by) > len(items):
        lines.append(f"… +{len(by)-len(items)} other reasons")

    return "\n".join(lines)
async def _mid_trap_digest_loop() -> None:
    global _mid_trap_last_flush_ts
    _mid_trap_last_flush_ts = time.time()
    while True:
        await asyncio.sleep(5)
        if not _MID_TRAP_DIGEST_ENABLED:
            continue
        if not (_error_bot and ERROR_BOT_ENABLED):
            continue
        now = time.time()
        if (now - _mid_trap_last_flush_ts) < _MID_TRAP_DIGEST_WINDOW_SEC:
            continue
        async with _mid_trap_lock:
            if not _mid_trap_events:
                continue
            events = list(_mid_trap_events)
            _mid_trap_events.clear()
            _mid_trap_last_flush_ts = now
        try:
            await _error_bot_send(_build_mid_trap_digest(events))
        except Exception:
            pass


def _start_mid_components(backend: object, broadcast_signal, broadcast_macro_alert) -> None:
    """Start MID scanner + optional MID digests in BOTH webhook and polling modes.

    Fixes a production issue where MID_SCANNER_ENABLED=1 but MID loop is skipped,
    and ensures trap digest is also enabled in webhook mode.
    """
    mid_enabled = os.getenv('MID_SCANNER_ENABLED', '1').strip().lower() not in ('0', 'false', 'no', 'off')
    if not mid_enabled:
        return

    # MID trap digest (optional)
    try:
        if hasattr(backend, 'set_mid_trap_sink'):
            backend.set_mid_trap_sink(_mid_trap_note)
        if (_error_bot and ERROR_BOT_ENABLED) and _MID_TRAP_DIGEST_ENABLED and _MID_TRAP_DIGEST_WINDOW_SEC > 0:
            TASKS["mid-trap-digest"] = asyncio.create_task(_mid_trap_digest_loop(), name="mid-trap-digest")
    except Exception as e:
        logger.error("[mid][trap] failed to init trap digest: %s", e)

    # MID scanner loop
    try:
        interval = int((os.getenv("MID_SCAN_INTERVAL_SECONDS", "").strip() or os.getenv("MID_SCAN_INTERVAL_SEC", "45").strip()) or 45)
        top_n = int(os.getenv("MID_TOP_N", "70") or 70)
        # Diagnostics: catch "imported wrong backend" cases (most common reason for has_method=False)
        try:
            import inspect, hashlib
            cls_file = inspect.getfile(backend.__class__)
            logger.info("[mid] backend class=%s file=%s", backend.__class__.__name__, cls_file)
            if isinstance(cls_file, str) and cls_file.endswith(".py"):
                with open(cls_file, "rb") as _f:
                    _raw = _f.read()
                _sha = hashlib.sha256(_raw).hexdigest()[:12]
                _txt = _raw.decode("utf-8", errors="ignore")
                _idx = _txt.find("async def scanner_loop_mid")
                _has_def = _idx >= 0
                _line = _txt[:_idx].count("\n") + 1 if _idx >= 0 else None
                logger.info("[mid] backend.py sha256=%s has_scanner_def=%s line=%s", _sha, _has_def, _line)
        except Exception:
            pass

        # Be defensive: older builds sometimes used different method names.
        mid_loop = (
            getattr(backend, 'scanner_loop_mid', None)
            or getattr(backend, 'mid_scanner_loop', None)
            or getattr(backend, 'scanner_mid_loop', None)
        )

        # If a compatibility stub is present, treat as missing implementation.
        _mid_stub_reason = None
        try:
            _mid_name = getattr(mid_loop, "__name__", "")
            _mid_qual = getattr(mid_loop, "__qualname__", "")
            if _mid_name == "_missing_scanner_loop_mid" or _mid_qual.endswith("._missing_scanner_loop_mid"):
                _mid_stub_reason = f"stub:{_mid_qual or _mid_name}"
                mid_loop = None
        except Exception:
            pass

        logger.info(
            "[mid] starting MID scanner (5m/30m/1h) interval=%ss top_n=%s has_method=%s",
            interval, top_n, callable(mid_loop)
        )

        if callable(mid_loop):
            TASKS["mid-scanner"] = asyncio.create_task(
                mid_loop(broadcast_signal, broadcast_macro_alert),
                name="mid-scanner",
            )
            # Expose task ref to backend (for /health + status loop diagnostics)
            try:
                setattr(backend, "_mid_scanner_task", TASKS["mid-scanner"])
            except Exception:
                pass
            # Monitor task so crashes are visible in logs/health
            try:
                _attach_task_monitor("mid-scanner", TASKS["mid-scanner"])
            except Exception:
                pass
            # repeat last MID tick summary in logs so it doesn't get lost
            TASKS["mid-summary-hb"] = asyncio.create_task(mid_summary_heartbeat_loop(), name="mid-summary-hb")
            # log once per minute: pending/expired/triggered + last tick age (explains 'why no signals')
            try:
                if hasattr(backend, "mid_status_summary_loop"):
                    TASKS["mid-status"] = asyncio.create_task(backend.mid_status_summary_loop(), name="mid-status")
            except Exception:
                pass
        else:
            # Don't silently skip. This is exactly the bug user saw in logs.
            logger.error(
                "MID_SCANNER_ENABLED=1 but Backend has no scanner_loop_mid. "
                "This usually means Railway deployed an old build or imported a different backend.py. "
                "Check the '[mid] backend class=... file=...' line above."
            )

            # Extra diagnostics: list potential candidates so the reason is obvious from logs.
            try:
                mids = [n for n in dir(backend) if 'mid' in n.lower() and 'scan' in n.lower()]
                if mids:
                    logger.error("[mid] available methods containing 'mid'+'scan': %s", ", ".join(sorted(mids)[:40]))
                # Also dump the attribute itself (type/name/module/qualname) to understand why it's not callable.
                try:
                    attr = getattr(backend, "scanner_loop_mid", None)
                    logger.error(
                        "[mid] scanner_loop_mid attr: type=%s callable=%s name=%s qual=%s module=%s repr=%r",
                        type(attr).__name__,
                        callable(attr),
                        getattr(attr, "__name__", None),
                        getattr(attr, "__qualname__", None),
                        getattr(attr, "__module__", None),
                        attr,
                    )
                except Exception:
                    pass

            except Exception:
                pass
            return

        # MID pending-entry trigger loop (emit only when price reaches entry + TA reconfirmed)
        try:
            if os.getenv("MID_PENDING_ENABLED", "0").strip().lower() not in ("0", "false", "no", "off"):
                _pending_fn = getattr(backend, "mid_pending_trigger_loop", None)
                if callable(_pending_fn):
                    TASKS["mid-pending"] = asyncio.create_task(
                        _pending_fn(broadcast_signal),
                        name="mid-pending",
                    )
                    _health_mark_ok("mid-pending")
                    _attach_task_monitor("mid-pending", TASKS["mid-pending"])
        except Exception as e:
            logger.error("[mid][pending] failed to start trigger loop: %s", e)

    except Exception as e:
        logger.error("[mid] failed to start MID components: %s", e)

def _error_record_payload(record: logging.LogRecord) -> tuple[int, str, str]:
    levelno = int(getattr(record, "levelno", logging.ERROR) or logging.ERROR)
    logger_name = str(getattr(record, "name", "root") or "root")
    try:
        msg = record.getMessage()
    except Exception:
        msg = str(getattr(record, "msg", "") or "")

    lines = [msg]
    try:
        if record.exc_info:
            fmt = logging.Formatter()
            exc_text = fmt.formatException(record.exc_info)
            if exc_text:
                lines.append(exc_text)
        elif record.stack_info:
            lines.append(str(record.stack_info))
    except Exception:
        pass

    body = "\n".join(str(x) for x in lines if str(x or "").strip())
    text = f"❗ {logging.getLevelName(levelno)}\n{logger_name}\n{body}".strip()
    return levelno, logger_name, text[:3900]


class ErrorBotLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
            msg_l = str(msg or "").lower()
            has_exc = bool(getattr(record, "exc_info", None) or getattr(record, "stack_info", None))
            looks_like_exception = ("traceback" in msg_l or "exception" in msg_l)
            must_forward = bool(record.levelno >= logging.ERROR or has_exc or looks_like_exception)
            if not must_forward and not _should_forward_to_error_bot(record.levelno, msg):
                return
            _levelno, _logger_name, payload = _error_record_payload(record)
            if _ERROR_AGG_ENABLED and _ERROR_AGG_WINDOW_SEC > 0:
                _error_agg_note(_levelno, _logger_name, payload)
            else:
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(_error_bot_send(payload))
                except Exception:
                    pass
        except Exception:
            pass

# Attach handler (once)
if _error_bot and ERROR_BOT_ENABLED:
    _h = ErrorBotLogHandler()
    _h.setLevel(logging.INFO)
    logging.getLogger().addHandler(_h)
# ----------------------------------------------------------------------

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


NOWPAYMENTS_API_KEY = (os.getenv("NOWPAYMENTS_API_KEY") or "").strip()
NOWPAYMENTS_IPN_SECRET = (os.getenv("NOWPAYMENTS_IPN_SECRET") or "").strip()
NOWPAYMENTS_API_BASE = (os.getenv("NOWPAYMENTS_API_BASE") or "https://api.nowpayments.io/v1").rstrip("/")


def _normalize_nowpayments_network(value: str | None) -> str:
    raw = str(value or "").strip().upper().replace("-", "").replace("_", "")
    aliases = {
        "BEP20": "BEP20",
        "BSC": "BEP20",
        "BNBBSC": "BEP20",
        "TRX": "TRC20",
        "TRC20": "TRC20",
        "TRON": "TRC20",
        "ETH": "ERC20",
        "ERC20": "ERC20",
        "ETHEREUM": "ERC20",
    }
    return aliases.get(raw, raw)


def _network_to_nowpayments_currency(network: str | None) -> str | None:
    net = _normalize_nowpayments_network(network)
    mapping = {
        "BEP20": "usdtbsc",
        "TRC20": "usdttrc20",
        "ERC20": "usdteth",
    }
    return mapping.get(net)


def _resolve_nowpayments_pay_currency() -> str:
    direct = (os.getenv("NOWPAYMENTS_PAY_CURRENCY") or "").strip().lower()
    if direct:
        return direct
    by_network = _network_to_nowpayments_currency(os.getenv("NOWPAYMENTS_NETWORK"))
    if by_network:
        return by_network
    # New default: BEP20/BSC instead of legacy TRC20.
    return "usdtbsc"


NOWPAYMENTS_PAY_CURRENCY = _resolve_nowpayments_pay_currency()
NOWPAYMENTS_PRICE_CURRENCY = (os.getenv("NOWPAYMENTS_PRICE_CURRENCY") or NOWPAYMENTS_PAY_CURRENCY or "usdtbsc").strip().lower()
NOWPAYMENTS_FIXED_RATE = (os.getenv("NOWPAYMENTS_FIXED_RATE") or "true").strip().lower() not in ("0", "false", "no", "off", "")
APP_BASE_URL = (os.getenv("APP_BASE_URL") or os.getenv("PUBLIC_BASE_URL") or os.getenv("RAILWAY_PUBLIC_DOMAIN") or "").strip()
if APP_BASE_URL and not APP_BASE_URL.startswith('http'):
    APP_BASE_URL = f"https://{APP_BASE_URL}"
if APP_BASE_URL:
    APP_BASE_URL = APP_BASE_URL.rstrip('/')
PAYMENT_WEBHOOK_PATH = "/api/payments/webhook"
PAYMENT_WEBHOOK_URL = (APP_BASE_URL + PAYMENT_WEBHOOK_PATH) if APP_BASE_URL else ""
def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).replace(",", ".").strip())
    except Exception:
        return float(default)


def _fmt_money(v: float | int | str) -> str:
    try:
        n = float(v)
    except Exception:
        return str(v)
    return str(int(n)) if n.is_integer() else (f"{n:.2f}".rstrip("0").rstrip("."))


SUBSCRIPTION_DURATION_DAYS = max(1, int(_env_float("SUBSCRIPTION_DURATION_DAYS", 30)))
SIGNAL_PRO_PRICE = _env_float("SIGNAL_PRO_PRICE", _env_float("PRICE_SIGNAL_PRO_USDT", 49.0))
AUTO_PRO_PRICE = _env_float("AUTO_PRO_PRICE", _env_float("PRICE_AUTO_PRO_USDT", 69.0))
SUBSCRIPTION_PLANS = {
    "signal_pro": {"amount": SIGNAL_PRO_PRICE, "days": SUBSCRIPTION_DURATION_DAYS, "signal": True, "autotrade": False, "name_key": "plan_signal_pro", "slug": "SIGNAL PRO"},
    "auto_pro": {"amount": AUTO_PRO_PRICE, "days": SUBSCRIPTION_DURATION_DAYS, "signal": True, "autotrade": True, "name_key": "plan_auto_pro", "slug": "AUTO PRO"},
}




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


async def _send_long(chat_id: int, text: str, reply_markup=None, *, parse_mode: str | None = None, **kwargs) -> None:
    """Send a potentially long message by splitting it into safe-sized chunks.

    Telegram hard limit is 4096 chars/message. We keep a safety margin.

    IMPORTANT:
    - If parse_mode is enabled, splitting can break entities and cause "can't parse entities".
      Поэтому: parse_mode используем только если сообщение короткое.
    - Never raise from here: user-facing handlers must not crash the webhook background task.
    """
    max_len = 3800

    text = _sanitize_template_text(chat_id, text, ctx="send_long")
    if not (text or "").strip():
        logger.warning("_send_long: skip empty text chat_id=%s", chat_id)
        return

    # Keep parse_mode only for short messages; long messages go as plain text chunks.
    use_parse_mode = parse_mode if (parse_mode and len(text) <= max_len) else None

    try:
        if len(text) <= max_len:
            await safe_send(chat_id, text, reply_markup=reply_markup, parse_mode=use_parse_mode, **kwargs)
            return

        parts: list[str] = []
        cur = ""
        for line in text.splitlines(True):
            if len(cur) + len(line) > max_len and cur:
                parts.append(cur)
                cur = ""
            cur += line
        if cur:
            parts.append(cur)

        for idx, part in enumerate(parts):
            # reply_markup only on first part to avoid clutter
            await safe_send(
                chat_id,
                part,
                reply_markup=reply_markup if idx == 0 else None,
                parse_mode=use_parse_mode if idx == 0 else None,
                **kwargs,
            )
    except Exception:
        # Last-resort: swallow any Telegram/API errors to keep webhook stable.
        try:
            if len(text) <= max_len:
                await safe_send(chat_id, text[:max_len], reply_markup=reply_markup)
        except Exception:
            pass


async def safe_edit_old(message: types.Message | None, txt: str, kb: types.InlineKeyboardMarkup) -> None:
    """Edit message text if possible; otherwise send a new message."""
    txt = _sanitize_template_text(message.chat.id if message else 0, txt, ctx="safe_edit_old")
    if not (txt or "").strip():
        logger.warning("safe_edit_old: skip empty text chat_id=%s", message.chat.id if message else None)
        return
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
    txt = _sanitize_template_text(call.from_user.id if getattr(call, "from_user", None) else 0, txt, ctx="render_in_place")
    if not (txt or "").strip():
        logger.warning("_render_in_place: skip empty text uid=%s", call.from_user.id if getattr(call, "from_user", None) else None)
        return call.message
    try:
        if call.message:
            await bot.edit_message_text(chat_id=call.from_user.id, message_id=call.message.message_id, text=txt, reply_markup=kb)
            return call.message
    except Exception:
        pass
    return await safe_send(call.from_user.id, txt, reply_markup=kb)


async def safe_edit(message: types.Message | None, text: str, kb: types.InlineKeyboardMarkup | None = None, **kwargs) -> None:
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
    text = _sanitize_template_text(chat_id, text, ctx="safe_edit")
    if not (text or "").strip():
        logger.warning("safe_edit: skip empty text chat_id=%s message_id=%s", chat_id, msg_id)
        return

    def _split_text(s: str, limit: int = 3900) -> list[str]:
        s = s or ""
        if len(s) <= limit:
            return [s] if s.strip() else []
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
            **kwargs,
        )
    except Exception as e:
        # If the message content is identical, Telegram returns "message is not modified".
        # In that case we do nothing (avoid spamming a new message).
        if "message is not modified" in str(e).lower():
            return
        # If we can't edit (old message, etc.) - just send new
        try:
            await safe_send(chat_id, parts[0], reply_markup=kb if len(parts) == 1 else None, **kwargs)
        except Exception:
            logger.exception("safe_edit: failed to edit/send first chunk")

    # Send remaining chunks (if any)
    if len(parts) > 1:
        for i, part in enumerate(parts[1:], start=1):
            try:
                await safe_send(chat_id, part, reply_markup=kb if i == len(parts) - 1 else None, **kwargs)
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
    # Pool sizing must be defined regardless of DATABASE_URL presence.
    # (Previously min_size was accidentally placed after a `return`, causing
    # UnboundLocalError when DATABASE_URL is set.)
    min_size = int(os.getenv("DB_POOL_MIN", "1"))
    max_size = int(os.getenv("DB_POOL_MAX", "10"))
    if not DATABASE_URL:
        # Allow running without Postgres locally, but notifications will be disabled.
        pool = None
        return
    acquire_timeout = float(os.getenv("DB_POOL_ACQUIRE_TIMEOUT_SEC", "25"))
    command_timeout = float(os.getenv("DB_COMMAND_TIMEOUT_SEC", "30"))
    init_retries = int(os.getenv("DB_INIT_RETRIES", "8"))
    base_sleep = float(os.getenv("DB_INIT_RETRY_BASE_SEC", "0.8"))
    last_err = None
    for attempt in range(1, init_retries + 1):
        try:
            pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=min_size,
                max_size=max_size,
                timeout=acquire_timeout,
                command_timeout=command_timeout,
            )
            break
        except Exception as e:
            last_err = e
            await asyncio.sleep(base_sleep * attempt)
    else:
        raise last_err
    # Share the same pool with trade storage
    db_store.set_pool(pool)
    await db_store.ensure_schema()
    # Users table migrations (signal access columns) live in db_store
    await db_store.ensure_users_columns()
    try:
        sb = await db_store.get_signal_bot_settings()
        SIGNAL_BOT_GLOBAL["pause_signals"] = bool(sb.get("pause_signals"))
        SIGNAL_BOT_GLOBAL["maintenance_mode"] = bool(sb.get("maintenance_mode"))
        SIGNAL_BOT_GLOBAL["referral_enabled"] = bool(sb.get("referral_enabled"))
    except Exception:
        logger.exception("startup: failed to load signal bot settings")
    try:
        await _warm_referral_access_cache()
    except Exception:
        logger.exception("startup: failed to warm referral access cache")



def subscription_gate_kb(uid: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "btn_buy_sub"), callback_data="sub:buy")
    kb.adjust(1)
    return kb.as_markup()


def subscription_plans_kb(uid: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr_sub(uid, "plan_signal_pro"), callback_data="sub:plan:signal_pro")
    kb.button(text=tr_sub(uid, "plan_auto_pro"), callback_data="sub:plan:auto_pro")
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:status")
    kb.adjust(1, 1, 1)
    return kb.as_markup()


def subscription_pay_kb(uid: int, plan_code: str, pay_url: str) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "btn_open_payment"), url=pay_url)
    kb.button(text=tr(uid, "btn_back"), callback_data="sub:buy")
    kb.adjust(1, 1)
    return kb.as_markup()


def _plan_data(plan_code: str):
    return SUBSCRIPTION_PLANS.get((plan_code or '').strip().lower())


def _plan_name(uid: int, plan_code: str) -> str:
    p = _plan_data(plan_code)
    if not p:
        return str(plan_code or '').strip()
    # Use clean commercial names in user/admin notifications, without emoji/price.
    return str(p.get('slug') or plan_code).strip()


def _subscription_i18n_ctx() -> dict:
    signal_plan = _plan_data("signal_pro") or {}
    auto_plan = _plan_data("auto_pro") or {}
    return {
        "signal_price": _fmt_money(signal_plan.get("amount", 0)),
        "auto_price": _fmt_money(auto_plan.get("amount", 0)),
        "signal_days": int(signal_plan.get("days") or SUBSCRIPTION_DURATION_DAYS),
        "auto_days": int(auto_plan.get("days") or SUBSCRIPTION_DURATION_DAYS),
    }


def tr_sub(uid: int, key: str, **kwargs) -> str:
    ctx = _subscription_i18n_ctx()
    ctx.update(kwargs)
    return trf(uid, key, **ctx)


async def _create_nowpayments_invoice(*, telegram_id: int, plan_code: str) -> dict:
    plan = _plan_data(plan_code)
    if not plan:
        raise ValueError('bad_plan')
    if not NOWPAYMENTS_API_KEY:
        raise RuntimeError('NOWPAYMENTS_API_KEY not configured')
    if not PAYMENT_WEBHOOK_URL:
        raise RuntimeError('APP_BASE_URL / PAYMENT_WEBHOOK_URL not configured')
    order_id = f"sub_{telegram_id}_{plan_code}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    await db_store.create_subscription_order(order_id=order_id, telegram_id=int(telegram_id), plan=plan_code, amount=float(plan['amount']))
    payload = {
        "price_amount": float(plan['amount']),
        "price_currency": NOWPAYMENTS_PRICE_CURRENCY,
        "pay_currency": NOWPAYMENTS_PAY_CURRENCY,
        "order_id": order_id,
        "order_description": f"{plan['slug']} subscription",
        "ipn_callback_url": PAYMENT_WEBHOOK_URL,
        "is_fixed_rate": NOWPAYMENTS_FIXED_RATE,
        "is_fee_paid_by_user": False,
    }
    logger.info(
        "nowpayments:create invoice order_id=%s plan=%s price_amount=%s price_currency=%s pay_currency=%s network_env=%s",
        order_id,
        plan_code,
        payload.get("price_amount"),
        payload.get("price_currency"),
        payload.get("pay_currency"),
        (os.getenv("NOWPAYMENTS_NETWORK") or "").strip(),
    )
    headers = {"x-api-key": NOWPAYMENTS_API_KEY, "Content-Type": "application/json"}
    data = None
    async with ClientSession() as session:
        # Prefer hosted invoice page when available
        async with session.post(f"{NOWPAYMENTS_API_BASE}/invoice", json=payload, headers=headers, timeout=30) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400 or not (data.get('invoice_url') or data.get('pay_url') or data.get('payment_url')):
                # Fallback to payment API
                async with session.post(f"{NOWPAYMENTS_API_BASE}/payment", json=payload, headers=headers, timeout=30) as resp2:
                    data = await resp2.json(content_type=None)
                    if resp2.status >= 400:
                        raise RuntimeError(f"nowpayments_create_failed:{resp2.status}:{data}")
    await db_store.attach_subscription_invoice(
        order_id=order_id,
        provider_payment_id=str(data.get('payment_id') or ''),
        pay_currency=data.get('pay_currency'),
        pay_amount=float(data['pay_amount']) if data.get('pay_amount') is not None else None,
        pay_address=data.get('pay_address'),
        pay_url=data.get('invoice_url') or data.get('pay_url') or data.get('payment_url'),
        payload=data,
    )
    data['order_id'] = order_id
    return data


def _verify_nowpayments_signature(raw_body: str, signature: str | None) -> bool:
    if not NOWPAYMENTS_IPN_SECRET or not signature:
        return False
    candidates = []
    # raw body
    candidates.append(hmac.new(NOWPAYMENTS_IPN_SECRET.encode(), raw_body.encode(), hashlib.sha512).hexdigest())
    # canonical json
    try:
        obj = json.loads(raw_body)
        canonical = json.dumps(obj, separators=(",", ":"), sort_keys=True)
        candidates.append(hmac.new(NOWPAYMENTS_IPN_SECRET.encode(), canonical.encode(), hashlib.sha512).hexdigest())
    except Exception:
        pass
    sig = str(signature).strip().lower()
    return any(hmac.compare_digest(c.lower(), sig) for c in candidates)


def _to_decimal(value) -> Decimal | None:
    if value in (None, ''):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal('0.01'))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _webhook_amount_matches(order: dict, event: dict) -> bool:
    """Accept the payment only when the paid amount is >= expected order amount.

    Supports two modes:
    - USD pricing: compare fiat-denominated values.
    - Fixed stablecoin pricing (e.g. 49 USDTTRC20): compare coin-denominated values too.
    """
    expected = _to_decimal(order.get('amount'))
    if expected is None:
        return False

    price_currency = str(event.get('price_currency') or '').strip().lower()
    pay_currency = str(event.get('pay_currency') or event.get('actually_paid_currency') or '').strip().lower()

    fiat_like = {'usd', 'usdt', 'usdc', 'busd', 'tusd', 'fdusd', 'usdp'}
    stable_like_prefixes = ('usdt', 'usdc', 'busd', 'tusd', 'fdusd', 'usdp')

    candidates = [event.get('price_amount'), event.get('actually_paid_at_fiat')]

    # If invoice price itself is exact stablecoin amount (e.g. USDTTRC20), comparing
    # price_amount/pay_amount to the stored plan amount is valid.
    if any(price_currency.startswith(x) for x in stable_like_prefixes):
        candidates.extend([event.get('pay_amount'), event.get('actually_paid')])
    elif pay_currency in fiat_like or any(pay_currency.startswith(x) for x in stable_like_prefixes):
        candidates.extend([event.get('pay_amount'), event.get('actually_paid')])

    for candidate in candidates:
        current = _to_decimal(candidate)
        if current is not None and current >= expected:
            return True
    return False


async def _grant_paid_plan_and_notify(order: dict, event: dict) -> None:
    telegram_id = int(order['telegram_id'])
    plan_code = str(order['plan'])
    plan = _plan_data(plan_code) or {"days": 30, "amount": order.get('amount') or 0}
    await db_store.grant_subscription_plan(telegram_id, plan_code, int(plan.get('days') or 30))
    plan_name_ru = _plan_name(telegram_id, plan_code)
    try:
        await safe_send(telegram_id, tr_sub(telegram_id, 'payment_paid_user', plan_name=plan_name_ru, amount=_fmt_money(plan.get('amount') or 0), days=int(plan.get('days') or SUBSCRIPTION_DURATION_DAYS)), reply_markup=menu_kb(telegram_id))
    except Exception:
        logger.exception('payment: failed to notify user %s', telegram_id)
    referral_reward = None
    try:
        referral_reward = await db_store.award_referral_bonus_for_first_payment(
            referral_user_id=telegram_id,
            order_id=str(order.get('order_id') or ''),
            payment_amount=float(order.get('amount') or plan.get('amount') or 0),
            currency='USDT',
            reward_percent=REFERRAL_REWARD_PERCENT,
        )
        if referral_reward:
            await _notify_referrer_bonus(referral_reward)
    except Exception:
        logger.exception('payment: failed to apply referral reward for user %s', telegram_id)

    try:
        txid = event.get('payin_hash') or event.get('tx_hash') or event.get('payment_id') or '-'
        admin_text = (
            f"💰 Paid\n\n"
            f"Telegram ID: {telegram_id}\n"
            f"Plan: {_plan_name(telegram_id, plan_code)}\n"
            f"Amount: {_fmt_money(order.get('amount') or 0)} {NOWPAYMENTS_PRICE_CURRENCY.upper()}\n"
            f"TXID: {txid}"
        )
        if referral_reward:
            admin_text += f"\nReferral bonus: {_fmt_money(referral_reward.get('reward_amount') or 0)} USDT"
        await safe_send_payment_alert(ADMIN_ALERT_CHAT_ID, admin_text)
    except Exception:
        logger.exception('payment: failed to notify admin')

async def ensure_user(user_id: int, referrer_id: int | None = None) -> None:
    if not pool or not user_id:
        return
    # Create user row if missing and grant 24h Signal trial ONCE.
    # IMPORTANT: does NOT touch Arbitrage access.
    try:
        await db_store.ensure_user_signal_trial(int(user_id), referrer_id=int(referrer_id) if referrer_id else None)
        await _refresh_referral_access_cache(int(user_id))
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

def _has_referral_access_cached(uid: int = 0) -> bool:
    try:
        if bool(SIGNAL_BOT_GLOBAL.get("referral_enabled")):
            return True
    except Exception:
        pass
    try:
        return bool(uid and int(uid) in REFERRAL_ACCESS_USERS)
    except Exception:
        return False


async def _warm_referral_access_cache() -> None:
    try:
        ids = await db_store.list_referral_enabled_user_ids()
        REFERRAL_ACCESS_USERS.clear()
        REFERRAL_ACCESS_USERS.update(int(x) for x in ids if x)
    except Exception:
        logger.exception("referral: failed to warm manual access cache")


async def _refresh_referral_access_cache(uid: int) -> bool:
    if not uid:
        return _has_referral_access_cached(uid)
    try:
        state = await db_store.get_user_referral_access_state(int(uid))
        SIGNAL_BOT_GLOBAL["referral_enabled"] = bool(state.get("global_referral_enabled"))
        if bool(state.get("referral_enabled")):
            REFERRAL_ACCESS_USERS.add(int(uid))
        else:
            REFERRAL_ACCESS_USERS.discard(int(uid))
        return bool(state.get("effective_referral_enabled"))
    except Exception:
        logger.exception("referral: failed to refresh access cache for uid=%s", uid)
        return _has_referral_access_cached(uid)


def menu_kb(uid: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "m_status"), callback_data="menu:status")
    kb.button(text=tr(uid, "m_stats"), callback_data="menu:stats")
    kb.button(text=tr(uid, "m_spot"), callback_data="menu:spot")
    kb.button(text=tr(uid, "m_fut"), callback_data="menu:futures")
    kb.button(text=tr(uid, "sig_btn_analyze"), callback_data="menu:analysis")
    # Hide Auto-trade button globally when auto-trade is paused (admin)
    if not bool(AUTOTRADE_BOT_GLOBAL.get("pause_autotrade")):
        kb.button(text=tr(uid, "m_autotrade"), callback_data="menu:autotrade")
    try:
        if _has_referral_access_cached(uid):
            kb.button(text=tr(uid, "m_referral"), callback_data="menu:referral")
    except Exception:
        pass
    kb.button(text=tr(uid, "m_trades"), callback_data="trades:page:0")
    kb.button(text=tr(uid, "m_notify"), callback_data="menu:notify")
    kb.adjust(2, 2, 2, 2, 1)
    return kb.as_markup()


def referral_main_kb(uid: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "ref_get_link"), callback_data="ref:link")
    kb.button(text=tr(uid, "ref_stats_btn"), callback_data="ref:stats")
    kb.button(text=tr(uid, "ref_balance_btn"), callback_data="ref:balance")
    kb.button(text=tr(uid, "ref_withdraw_btn"), callback_data="ref:withdraw")
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:status")
    kb.adjust(1, 2, 1, 1)
    return kb.as_markup()


def referral_balance_kb(uid: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "ref_withdraw_btn"), callback_data="ref:withdraw")
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:referral")
    kb.adjust(1, 1)
    return kb.as_markup()


def referral_withdraw_prompt_kb(uid: int = 0) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:referral")
    kb.adjust(1)
    return kb.as_markup()


def referral_admin_request_kb(user_id: int, request_id: int) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Выплачено", callback_data=f"refadm:paid:{int(request_id)}:{int(user_id)}")
    kb.button(text="❌ Отклонить", callback_data=f"refadm:reject:{int(request_id)}:{int(user_id)}")
    kb.button(text="👤 Профиль пользователя", callback_data=f"refadm:profile:{int(user_id)}")
    kb.button(text="📊 Реферальная статистика", callback_data=f"refadm:stats:{int(user_id)}")
    kb.adjust(2, 2)
    return kb.as_markup()


async def _ensure_public_bot_username() -> str:
    global BOT_PUBLIC_USERNAME
    uname = (BOT_PUBLIC_USERNAME or "").strip().lstrip("@")
    if uname:
        return uname
    try:
        me = await bot.get_me()
        uname = (getattr(me, 'username', None) or "").strip().lstrip("@")
        if uname:
            BOT_PUBLIC_USERNAME = uname
            logger.info("referral: resolved public bot username @%s", uname)
            return uname
    except Exception:
        logger.exception("referral: failed to resolve public bot username")
    return ""


def _ref_link(uid: int) -> str:
    uname = (BOT_PUBLIC_USERNAME or "").strip().lstrip("@")
    token = f"ref_{int(uid)}"
    if uname:
        return f"https://t.me/{uname}?start={token}"
    return f"/start {token}"


def _is_valid_bsc_address(value: str) -> bool:
    return bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", (value or "").strip()))


async def referral_main_text(uid: int) -> str:
    await _ensure_public_bot_username()
    ov = await db_store.get_referral_overview(uid)
    return trf(uid, "ref_main_text",
        link=_ref_link(uid),
        total_refs=int(ov.get("total_refs") or 0),
        paid_refs=int(ov.get("paid_refs") or 0),
        available_balance=_fmt_money(ov.get("available_balance") or 0),
        hold_balance=_fmt_money(ov.get("hold_balance") or 0),
        withdrawn_balance=_fmt_money(ov.get("withdrawn_balance") or 0),
    )


async def referral_stats_text(uid: int) -> str:
    ov = await db_store.get_referral_overview(uid)
    return trf(uid, "ref_stats_text",
        total_refs=int(ov.get("total_refs") or 0),
        paid_refs=int(ov.get("paid_refs") or 0),
        total_earned=_fmt_money(ov.get("total_earned") or 0),
    )


async def referral_balance_text(uid: int) -> str:
    ov = await db_store.get_referral_overview(uid)
    return trf(uid, "ref_balance_text",
        available_balance=_fmt_money(ov.get("available_balance") or 0),
        hold_balance=_fmt_money(ov.get("hold_balance") or 0),
        withdrawn_balance=_fmt_money(ov.get("withdrawn_balance") or 0),
        total_earned=_fmt_money(ov.get("total_earned") or 0),
    )


async def referral_withdraw_prompt_text(uid: int) -> str:
    ov = await db_store.get_referral_overview(uid)
    return trf(uid, "ref_withdraw_prompt",
        available_balance=_fmt_money(ov.get("available_balance") or 0),
        min_withdraw=_fmt_money(REFERRAL_MIN_WITHDRAW_USDT),
        network=REFERRAL_NETWORK,
        example_wallet="0x1234abcd5678ef901234abcd5678ef901234abcd",
    )


async def _notify_referral_admin_new_request(req: dict) -> None:
    uid = int(req.get("telegram_id") or 0)
    username = "-"
    try:
        ch = await bot.get_chat(uid)
        username = ("@" + ch.username) if getattr(ch, 'username', None) else (getattr(ch, 'full_name', None) or "-")
    except Exception:
        pass
    created_at = req.get('created_at')
    created_at_text = created_at.strftime('%d.%m.%Y') if hasattr(created_at, 'strftime') else '-'
    txt = (
        f"💸 Новая заявка на вывод\n\n"
        f"👤 Пользователь: {username}\n"
        f"🆔 ID: {uid}\n\n"
        f"💰 Сумма: {_fmt_money(req.get('amount') or 0)} USDT\n"
        f"🌐 Сеть: {REFERRAL_NETWORK}\n\n"
        f"🏦 Адрес:\n{req.get('wallet_address') or '-'}\n\n"
        f"Дата:\n{created_at_text}"
    )
    msg = await safe_send(
        REFERRAL_ADMIN_CHAT_ID,
        txt,
        ctx="referral_admin_new_request",
        reply_markup=referral_admin_request_kb(uid, int(req.get('id') or 0)),
    )
    if msg is not None:
        try:
            await db_store.set_referral_withdrawal_admin_message(
                request_id=int(req.get('id') or 0),
                admin_chat_id=int(REFERRAL_ADMIN_CHAT_ID),
                admin_message_id=int(msg.message_id),
            )
        except Exception:
            logger.exception("referral: failed to save admin message ids")


async def _notify_referrer_bonus(referral_reward: dict) -> None:
    if not referral_reward:
        return
    referrer_id = int(referral_reward.get('referrer_id') or 0)
    if not referrer_id:
        return
    try:
        ov = await db_store.get_referral_overview(referrer_id)
        await safe_send(
            referrer_id,
            trf(referrer_id, 'ref_bonus_notification',
                reward_amount=_fmt_money(referral_reward.get('reward_amount') or 0),
                available_balance=_fmt_money(ov.get('available_balance') or 0),
            ),
        )
    except Exception:
        logger.exception('referral: failed to notify referrer bonus')


async def _referral_disabled_text(uid: int) -> str:
    return tr(uid, 'ref_disabled')


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

def _read_autotrade_master_key_raw() -> str:
    key = (os.getenv("AUTOTRADE_MASTER_KEY") or "").strip()
    if key:
        return key
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return ""
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or not s.startswith("AUTOTRADE_MASTER_KEY="):
                continue
            value = s.split("=", 1)[1].strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            value = value.strip()
            if value:
                os.environ["AUTOTRADE_MASTER_KEY"] = value
                return value
    except Exception:
        return ""
    return ""



def get_autotrade_master_key() -> str:
    """Read and validate AUTOTRADE_MASTER_KEY from environment/.env only."""
    key = (_read_autotrade_master_key_raw() or "").strip()
    if not key:
        raise RuntimeError(
            "AUTOTRADE_MASTER_KEY is missing. Add AUTOTRADE_MASTER_KEY=... to your .env file."
        )
    try:
        Fernet(key.encode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"AUTOTRADE_MASTER_KEY is invalid: {e}") from e
    return key

AUTOTRADE_INPUT: Dict[int, Dict[str, str]] = {}

# One-shot TA analyze mode (enabled via menu button)
ANALYZE_INPUT: Dict[int, bool] = {}


def _normalize_symbol_input(user_text: str, known_symbols: Optional[List[str]] = None) -> Tuple[Optional[str], List[str], Optional[str]]:
    """Normalize user ticker input for the Analyze button.

    Returns: (symbol_or_none, suggestions, note)

    Accepts:
      - btc, BTC, BTCUSDT
      - btc/usdt, btc-usdt, btc usdt

    If known_symbols is provided, will validate and suggest closest matches.
    """
    if not user_text:
        return None, [], None

    raw0 = (user_text or "").strip()
    raw = raw0.upper().strip()

    # remove common separators then keep only alnum
    raw = raw.replace("/", "").replace("-", "").replace(" ", "")
    raw = re.sub(r"[^A-Z0-9]", "", raw)
    raw = raw.replace("USDTUSDT", "USDT").replace("USDCUSDC", "USDC")

    if not raw:
        return None, [], None

    # If user typed only base coin (BTC/ETH/SOL) -> assume USDT
    if raw.endswith("USDT") or raw.endswith("USDC"):
        symbol = raw
    else:
        symbol = raw + "USDT"

    if not known_symbols:
        note = None if symbol == raw else f"Нормализовано: {raw0} → {symbol}"
        return symbol, [], note

    known_set = set(known_symbols)
    if symbol in known_set:
        note = None if symbol == raw else f"Нормализовано: {raw0} → {symbol}"
        return symbol, [], note

    # Suggestions by prefix (fast)
    base = symbol
    if base.endswith("USDT"):
        base = base[:-4]
    elif base.endswith("USDC"):
        base = base[:-4]

    suggestions: List[str] = []
    if len(base) >= 2:
        suggestions = [s for s in known_symbols if s.startswith(base)][:5]

    # Fuzzy fallback (handles typos like "bt" -> "BTCUSDT")
    if not suggestions and len(base) >= 2:
        base_map: Dict[str, str] = {}
        for s in known_symbols:
            b = s
            if b.endswith("USDT"):
                b = b[:-4]
            elif b.endswith("USDC"):
                b = b[:-4]
            if b and b not in base_map:
                base_map[b] = s

        close = difflib.get_close_matches(base, list(base_map.keys()), n=5, cutoff=0.6)
        suggestions = [base_map[c] for c in close if c in base_map][:5]

    # If exactly one suggestion -> auto-pick
    if len(suggestions) == 1:
        picked = suggestions[0]
        return picked, [], f"Использую ближайший тикер: {picked} (вместо {raw0})"

    return None, suggestions, None

# Per-user locks to prevent race conditions on rapid button presses / concurrent inputs
_USER_LOCKS: Dict[int, asyncio.Lock] = {}
def _user_lock(uid: int) -> asyncio.Lock:
    lock = _USER_LOCKS.get(int(uid))
    if lock is None:
        lock = asyncio.Lock()
        _USER_LOCKS[int(uid)] = lock
    return lock


# Global Signal bot settings cache (admin panel)
SIGNAL_BOT_GLOBAL: Dict[str, object] = {
    "pause_signals": False,
    "maintenance_mode": False,
    "referral_enabled": False,
    "updated_at": None,
}

# Auto-trade stats UI state
AUTOTRADE_STATS_STATE: Dict[int, Dict[str, str]] = {}

# Notify only on API errors (anti-spam)
AUTOTRADE_API_ERR_LAST: Dict[tuple[int, str, str], float] = {}
AUTOTRADE_API_ERR_COOLDOWN_SEC = 300
SMART_MANAGER_ERR_LAST: Dict[tuple[int, str], float] = {}
SMART_MANAGER_ERROR_BOT_COOLDOWN_SEC = float(os.getenv("SMART_MANAGER_ERROR_BOT_COOLDOWN_SEC", "60") or 60)

# Notify (optional) when Auto-trade is SKIPPED due to access/keys/confirmations (diagnostics)
AUTOTRADE_SKIP_LAST: Dict[tuple[int, str], float] = {}  # (uid, reason) -> last_ts
AUTOTRADE_SKIP_COOLDOWN_SEC = int(os.getenv("AUTOTRADE_SKIP_COOLDOWN_SEC", "900") or 900)  # 15m


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
    """Background loop that refreshes global auto-trade pause/maintenance flags."""
    tick = 0
    logger.info("[autotrade-global] loop started interval=5s"); _health_mark_ok("autotrade-global")
    while True:
        tick += 1
        try:
            await _refresh_autotrade_bot_global_once(); _health_mark_ok("autotrade-global")
        except Exception:
            # keep loop alive
            logger.exception("[autotrade-global] refresh failed tick=%s", tick); _health_mark_err("autotrade-global", "refresh_failed")
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
        return tr_sub(uid, "access_blocked")
    if st == "expired":
        return tr_sub(uid, "at_unavailable_expired")
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
    logger.error("[autotrade] api_error uid=%s ex=%s mt=%s err=%s", uid, ex, mt, (error_text or "")[:500])
    _health_mark_err("autotrade-manager", "api_error")
    try:
        await safe_send(uid, msg)
    except Exception:
        logger.exception("[autotrade] failed to send user api error uid=%s ex=%s mt=%s", uid, ex, mt)
    # Also forward to @errorrrrrrg_bot (admin error bot), if enabled
    try:
        send_to_err = (os.getenv("AUTOTRADE_SEND_API_ERRORS_TO_ERROR_BOT", "1") or "1").strip().lower() not in ("0","false","no","off")
        if send_to_err:
            await _error_bot_send(f"🤖 Auto-trade API ERROR\nuid={uid}\n{title} ({ex} {mt})\n{error_text}"[:3900])
    except Exception:
        logger.exception("[autotrade] failed to forward api error to error bot uid=%s ex=%s mt=%s", uid, ex, mt)

def _smart_event_needs_error_bot(text: str) -> bool:
    m = str(text or "").lower()
    markers = (
        "not closed", "не закры", "close failed", "failed to close",
        "close_market failed", "close market failed", "could not close", "unable to close",
        "execution error", "ошибка исполнения", "ошибка закрытия",
        "close returned false", "reduce-only", "tp1 not closed", "tp2 not closed", "sl not closed"
    )
    return any(x in m for x in markers)


async def _forward_smart_manager_error_to_error_bot(uid: int, text: str) -> None:
    try:
        if not _smart_event_needs_error_bot(text):
            return
        key = (int(uid), str(text or "")[:1000])
        now = time.time()
        last = SMART_MANAGER_ERR_LAST.get(key, 0.0)
        if now - last < SMART_MANAGER_ERROR_BOT_COOLDOWN_SEC:
            return
        SMART_MANAGER_ERR_LAST[key] = now
        payload = f"🤖 SMART MANAGER CLOSE ERROR\nuid={uid}\n{text}"[:3900]
        if _ERROR_AGG_ENABLED and _ERROR_AGG_WINDOW_SEC > 0:
            _error_agg_note(logging.ERROR, "smart-manager-direct", payload)
        else:
            await _error_bot_send(payload)
    except Exception:
        logger.exception("[smart-manager] failed to forward direct error uid=%s", uid)


async def _notify_smart_manager_event(uid: int, text: str) -> None:
    try:
        try:
            await _forward_smart_manager_error_to_error_bot(uid, text)
        except Exception:
            logger.exception("[smart-manager] direct error forward failed uid=%s", uid)

        raw_enabled = os.getenv("SMART_MANAGER_NOTIFY_ENABLED")
        if raw_enabled is None:
            raw_enabled = os.getenv("SMART_MANAGER_SEND_MESSAGES", "1")
        enabled = str(raw_enabled or "1").strip().lower() not in ("0", "false", "no", "off")
        logger.info("[smart-manager] notify enabled=%s uid=%s text=%s", enabled, uid, (text or "")[:500].replace("\n", " | "))
        if not enabled:
            return
        await safe_send(uid, text, ctx="smart_manager_event")
    except Exception:
        logger.exception("[smart-manager] notify failed uid=%s", uid)

def _fernet() -> Fernet:
    try:
        k = get_autotrade_master_key()
    except Exception as e:
        raise RuntimeError(f"AUTOTRADE_MASTER_KEY invalid: {e}") from e
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
        """Parse confirmations / available_exchanges into a normalized set.

        Important: the UI may format venues as "Binance • Bybit • OKX • MEXC" (bullets),
        so we must split on bullets too.
        """
        if not sig_obj:
            return set()
        conf_raw = str((getattr(sig_obj, "available_exchanges", "") or getattr(sig_obj, "confirmations", "") or "")).strip()
        if not conf_raw:
            return set()
        out: set[str] = set()
        for part in re.split(r"[+ ,;/|•·]+", conf_raw.strip()):
            p = part.strip().lower()
            if not p:
                continue
            if p in ("binance", "bnb"):
                out.add("binance")
            elif p in ("bybit", "byb"):
                out.add("bybit")
            elif p in ("okx",):
                out.add("okx")
            elif p in ("mexc",):
                out.add("mexc")
            elif p in ("gateio", "gate", "gate.io", "gateio.ws"):
                out.add("gateio")
        return out

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
        # Match backend routing: prefer confirmed exchanges, but if confirmations are
        # empty/incomplete then fall back to the first eligible exchange from user priority.
        if spot_conf:
            for ex in prio:
                if ex in spot_conf and _eligible(ex, "spot"):
                    return ex
        for ex in prio:
            if _eligible(ex, "spot"):
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
            parts.append(("SPOT: пропуск (нет активных ключей/подходящей биржи)" if lang == "ru" else "SPOT: skipped (no active keys / no suitable exchange)"))
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
    # FUTURES supports Binance / Bybit / OKX
    kb.button(text="BINANCE FUTURES", callback_data="at:keys:set:binance:futures")
    kb.button(text="BYBIT FUTURES", callback_data="at:keys:set:bybit:futures")
    kb.button(text="OKX FUTURES", callback_data="at:keys:set:okx:futures")
    kb.adjust(2)
    kb.button(text=tr(uid, "btn_back"), callback_data="menu:autotrade")
    kb.adjust(1)
    return kb.as_markup()




def _parse_private_ta_uids(raw: str) -> set[int]:
    out: set[int] = set()
    for part in re.split(r"[\s,;]+", str(raw or "").strip()):
        if not part:
            continue
        try:
            out.add(int(part))
        except Exception:
            continue
    return out


def _private_ta_uids() -> set[int]:
    out: set[int] = set()
    for env_name in ("PRIVATE_TA_UIDS", "PRIVATE_TA_TELEGRAM_IDS", "PRIVATE_TA_USER_IDS"):
        out.update(_parse_private_ta_uids(os.getenv(env_name, "")))
    return out


def _uid_can_view_private_ta(uid: int) -> bool:
    try:
        uid_i = int(uid)
    except Exception:
        return False
    if uid_i == 0:
        return True
    try:
        if _is_admin(uid_i):
            return True
    except Exception:
        pass
    try:
        return uid_i in _private_ta_uids()
    except Exception:
        return False


_PRIVATE_TA_LINE_RE = re.compile(
    r"^(?:TA score:|Signal strength:|Smart-setup:|Confidence:|RSI(?:\([^)]*\))?:|ADX(?: .*?)?:|BB:|Trap:|Div:|Ch:|Liquidity(?: .*?)?:|Breakout/Retest:|Pattern:|Additional TA:|HTF struct:)",
    re.UNICODE,
)

_PRIVATE_TA_INLINE_FLAG_RE = re.compile(
    r"\b(?:trigger_revalidated|levels_recalc|smart_setup_emit|smart_conf|instant_vip|zone_touch_alert|no_autotrade|levels_anchor)\b",
    re.IGNORECASE,
)


def _strip_private_ta_prefix(line: str) -> str:
    core = str(line or "").strip()
    core = re.sub(r"^[✅❌⚠️ℹ️🔥🎯📊🧱➕•\s]+", "", core).strip()
    return core


def _risk_note_for_uid(uid: int, risk_note: str) -> str:
    text = str(risk_note or "").strip()
    if not text:
        return ""
    if _uid_can_view_private_ta(uid):
        return text

    keep: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line:
            continue

        core = _strip_private_ta_prefix(line)
        if _PRIVATE_TA_LINE_RE.match(core):
            continue
        if core.startswith("Auto-converted:"):
            continue
        if _PRIVATE_TA_INLINE_FLAG_RE.search(line):
            continue

        # Remove inline internal flags if they leaked outside the TA block.
        line = re.sub(r"\s*\|\s*trigger_revalidated=\d+\b", "", line)
        line = re.sub(r"\s*\|\s*levels_recalc=\d+\b", "", line)
        line = re.sub(r"\s*\|\s*smart_setup_emit=\d+\b", "", line)
        line = re.sub(r"\s*\|\s*smart_conf=\d+/\d+\b", "", line)
        line = re.sub(r"\s*\|\s*instant_vip=\d+\b", "", line)
        line = line.strip().strip("|").strip()
        if not line:
            continue
        keep.append(line)

    return "\n".join(keep).strip()


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

    def _rr_value(target_f: float) -> float | None:
        try:
            entry_f = float(getattr(s, 'entry', 0.0) or 0.0)
            sl_f = float(getattr(s, 'sl', 0.0) or 0.0)
            tgt_f = float(target_f or 0.0)
            if entry_f <= 0 or sl_f <= 0 or tgt_f <= 0:
                return None
            side_s = str(getattr(s, 'direction', '') or '').upper()
            is_short = (getattr(s, 'market', 'SPOT') != 'SPOT') and ('SHORT' in side_s)
            if is_short:
                risk = sl_f - entry_f
                reward = entry_f - tgt_f
            else:
                risk = entry_f - sl_f
                reward = tgt_f - entry_f
            if risk <= 0 or reward <= 0:
                return None
            return float(reward / risk)
        except Exception:
            return None

    def _rr_text() -> str:
        try:
            rr_label = tr(uid, 'sig_rr')
            tp1_rr = _rr_value(getattr(s, 'tp1', 0.0))
            tp2_rr = _rr_value(getattr(s, 'tp2', 0.0))
            if tp1_rr is not None and tp2_rr is not None and abs(tp2_rr - tp1_rr) > 1e-12:
                return f"{rr_label} TP1: 1:{tp1_rr:.2f}\n{rr_label} TP2: 1:{tp2_rr:.2f}"
            if tp1_rr is not None:
                return f"{rr_label}: 1:{tp1_rr:.2f}"
            if tp2_rr is not None:
                return f"{rr_label}: 1:{tp2_rr:.2f}"
        except Exception:
            pass
        return f"{tr(uid, 'sig_rr')}: 1:{float(getattr(s, 'rr', 0.0) or 0.0):.2f}"

    rr_line = _rr_text()
    # Confirm line: primary venue that produced the signal.
    src_ex = (getattr(s, 'source_exchange', '') or '').strip()
    confirm_line = f"{tr(uid, 'sig_confirm')}: {src_ex}"

    autotrade_line = f"{tr(uid, 'sig_autotrade')}: {autotrade_hint}\n" if autotrade_hint else ""

    risk_note = _risk_note_for_uid(uid, s.risk_note)
    try:
        if _uid_can_view_private_ta(uid):
            setup_label = _signal_ui_setup_label(s)
            if setup_label:
                setup_line = f"🧭 Smart-setup: {setup_label}"
                if risk_note:
                    lines = [ln.rstrip() for ln in str(risk_note).splitlines()]
                    inserted = False
                    for idx, raw_line in enumerate(lines):
                        core = _strip_private_ta_prefix(raw_line)
                        if core.startswith('TA score:') or core.startswith('Signal strength:'):
                            lines.insert(idx, setup_line)
                            inserted = True
                            break
                    if not inserted:
                        lines.append(setup_line)
                    risk_note = "\n".join([ln for ln in lines if str(ln).strip()]).strip()
                else:
                    risk_note = setup_line
    except Exception:
        pass
    strength_line = ""
    if risk_note:
        keep_lines: list[str] = []
        for raw_line in str(risk_note).splitlines():
            line = str(raw_line or "").strip()
            if not line:
                continue
            if (not strength_line) and line.startswith("🔥 Signal strength:"):
                strength_line = line
                continue
            keep_lines.append(raw_line.rstrip())
        risk_note = "\n".join([ln for ln in keep_lines if str(ln).strip()]).strip()
    if strength_line:
        confirm_line = f"{confirm_line}\n{strength_line}"
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
        confirm_line=confirm_line,
        autotrade_line=autotrade_line,
        risk_block=risk_block,
        open_prompt=f"{tr(uid, 'sig_open_prompt')}\n{_symbol_hashtag(s.symbol)}"
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


def _report_tz_label() -> str:
    try:
        if str(TZ_NAME).strip() in ("Europe/Moscow", "MSK", "UTC+3", "GMT+3"):
            return "MSK"
        return str(TZ_NAME or "UTC").strip() or "UTC"
    except Exception:
        return "UTC"


def _report_close_emoji(final_status: str) -> str:
    st = str(final_status or "").upper().strip()
    return {
        "WIN": "🟢",
        "LOSS": "🔴",
        "BE": "⚪️",
        "CLOSED": "🟡",
    }.get(st, "⚪️")


def _report_close_status(final_status: str, *, after_tp1: bool = False) -> str:
    st = str(final_status or "").upper().strip() or "CLOSED"
    if after_tp1 and st in ("WIN", "LOSS", "BE"):
        return f"{st} (после TP1)"
    if st == "CLOSED":
        return "CLOSED"
    return st


def _report_pnl_pct(pnl_total_pct: float | int | None) -> str:
    try:
        return f"{float(pnl_total_pct or 0.0):+.1f}%"
    except Exception:
        return "+0.0%"


def _report_rr_str(entry: float, sl: float, tp1: float, tp2: float) -> str:
    try:
        entry = float(entry or 0.0)
        sl = float(sl or 0.0)
        tp1 = float(tp1 or 0.0)
        tp2 = float(tp2 or 0.0)
        if entry <= 0 or sl <= 0:
            return "-"
        risk = abs(entry - sl)
        if risk <= 0:
            return "-"
        target = tp2 if tp2 > 0 and abs(tp2 - tp1) > 1e-12 else tp1
        if target <= 0:
            return "-"
        reward = abs(target - entry)
        if reward <= 0:
            return "-"
        return f"{(reward / risk):.2f}"
    except Exception:
        return "-"


def _report_close_reason(final_status: str, *, after_tp1: bool = False, has_tp2: bool = True) -> str:
    st = str(final_status or "").upper().strip()
    if st == "WIN":
        if has_tp2:
            return "Цена дошла до TP2 — фиксируем прибыль."
        return "Цена дошла до TP1 — фиксируем прибыль."
    if st == "LOSS":
        if after_tp1:
            return "После TP1 цена вернулась к SL — остаток позиции закрыт по риску."
        return "Цена дошла до SL — сработал лимит риска."
    if st == "BE":
        return "После TP1 цена вернулась к BE — закрытие без убытка."
    if st == "CLOSED":
        return "Сигнал закрыт по времени — лимит сопровождения истёк."
    return "Сигнал закрыт."


def _loss_diag_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _loss_diag_env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return int(default)


def _loss_diag_parse_ta_score(*values: str) -> float | None:
    for value in values:
        src = str(value or '')
        if not src:
            continue
        m = re.search(r'TA score:\s*([0-9]+(?:\.[0-9]+)?)', src, flags=re.IGNORECASE)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                pass
    return None


def _loss_diag_parse_strength(*values: str) -> str:
    for value in values:
        src = str(value or '')
        if not src:
            continue
        m = re.search(r'Signal strength:\s*([^\n\r]+)', src, flags=re.IGNORECASE)
        if m:
            raw = str(m.group(1) or '').strip().lower()
            if 'weak' in raw:
                return 'weak'
            if 'medium' in raw or 'mid' in raw:
                return 'medium'
            if 'strong' in raw:
                return 'strong'
    return ''


def _loss_diag_count_confirmations(raw: str | None) -> int:
    src = str(raw or '').strip()
    if not src:
        return 0
    items = [x.strip() for x in re.split(r'[,+|/]+', src) if x.strip()]
    uniq = []
    for item in items:
        key = item.lower()
        if key not in uniq:
            uniq.append(key)
    return len(uniq)


def _loss_diag_setup_key_from_row(row: dict) -> str:
    label = str(_report_setup_label_from_row(row) or '').strip().lower().replace('-', '_').replace(' ', '_')
    mapping = {
        'origin': 'origin',
        'origin_fast': 'origin',
        'origin_fastpass': 'origin',
        'breakout': 'breakout',
        'breakout_fast': 'breakout',
        'breakout_fastpass': 'breakout',
        'zone': 'zone_retest',
        'zone_retest': 'zone_retest',
        'zone_touch': 'zone_retest',
        'zone_touch_retest': 'zone_retest',
        'normal_pending': 'normal_pending_trigger',
        'normal_pending_trigger': 'normal_pending_trigger',
        'pending': 'normal_pending_trigger',
        'pending_trigger': 'normal_pending_trigger',
        'liquidity_reclaim': 'liquidity_reclaim',
        'liquidity_reclaim_emit': 'liquidity_reclaim',
    }
    if label in mapping:
        return mapping[label]
    return label


EXACT_SMC_ROUTE_KEYS = {
    'smc_ob_fvg_overlap',
    'smc_htf_ob_ltf_fvg',
    'smc_bos_retest_confirm',
    'smc_displacement_origin',
    'smc_dual_fvg_origin',
    'smc_liquidity_reclaim',
}


def _loss_diag_is_exact_smc_route(row: dict | None) -> bool:
    try:
        return str(_loss_diag_route_key_from_row(dict(row or {})) or '').strip() in EXACT_SMC_ROUTE_KEYS
    except Exception:
        return False


def _loss_diag_route_title(route_key: str) -> str:
    mapping = {
        'smc_ob_fvg_overlap': 'OB+FVG priority emit',
        'smc_htf_ob_ltf_fvg': 'HTF OB + LTF FVG retest',
        'smc_bos_retest_confirm': 'BOS → retest → confirm',
        'smc_displacement_origin': 'displacement origin',
        'smc_dual_fvg_origin': 'dual/stacked FVG origin',
        'smc_liquidity_reclaim': 'liquidity reclaim',
    }
    return mapping.get(str(route_key or '').strip(), str(route_key or '').strip())


def _loss_diag_route_key_from_row(row: dict) -> str:
    for candidate in (
        row.get('emit_route'),
        row.get('smc_setup_route'),
        row.get('setup_route'),
        row.get('route_key'),
    ):
        raw = str(candidate or '').strip()
        if raw.startswith('smc_'):
            return raw
    label = str(_report_setup_label_from_row(row) or '').strip().lower()
    route_map = {
        'origin | ob+fvg priority emit': 'smc_ob_fvg_overlap',
        'zone retest | htf ob + ltf fvg retest emit': 'smc_htf_ob_ltf_fvg',
        'breakout | bos → fvg/ob retest → confirm': 'smc_bos_retest_confirm',
        'origin | displacement origin fast-path': 'smc_displacement_origin',
        'origin | dual/stacked fvg origin': 'smc_dual_fvg_origin',
        'liquidity reclaim | liquidity sweep → reclaim → bos continuation': 'smc_liquidity_reclaim',
    }
    return route_map.get(label, '')


def _loss_diag_primary_reason(src: dict, analysis: dict, *, after_tp1: bool, st: str) -> tuple[str, str]:
    route_key = _loss_diag_route_key_from_row(src)
    side = str(src.get('side') or '').upper().strip() or 'LONG'
    zone_hold = analysis.get('zone_hold')
    fast_invalidation = bool(analysis.get('fast_invalidation'))
    struct = str(analysis.get('structure_5m') or '').strip()
    follow = str((analysis.get('follow_through_tp1') if after_tp1 else analysis.get('follow_through')) or '').strip().lower()
    vwap_state = str((analysis.get('vwap_after_tp1') if after_tp1 else analysis.get('vwap_after_entry')) or '').strip().lower()

    if st != 'LOSS':
        return ('timeout_closed', 'Сделка закрыта без фиксации типового LOSS-паттерна.')

    if after_tp1:
        mapping = {
            'smc_ob_fvg_overlap': ('tp1_ob_fvg_no_followthrough', 'После TP1 сетап OB+FVG не дал продолжения: приоритетная зона уже не толкала цену по сделке, и остаток позиции вернулся в защиту.'),
            'smc_htf_ob_ltf_fvg': ('tp1_htf_ob_ltf_no_followthrough', 'После TP1 HTF OB + LTF FVG retest не дал второй волны: цена не продолжила движение и вернулась в защитную зону.'),
            'smc_bos_retest_confirm': ('tp1_bos_retest_no_followthrough', 'После TP1 breakout не развился дальше: retest не перешёл в продолжение, и остаток позиции вернулся в защиту.'),
            'smc_displacement_origin': ('tp1_displacement_origin_no_followthrough', 'После TP1 displacement-origin импульс затух: продолжения от origin не было, и остаток позиции вернулся в защиту.'),
            'smc_dual_fvg_origin': ('tp1_dual_fvg_origin_no_followthrough', 'После TP1 dual/stacked FVG origin не дал следующую волну: цена потеряла импульс и вернулась в защиту.'),
            'smc_liquidity_reclaim': ('tp1_liquidity_reclaim_no_followthrough', 'После TP1 reclaim не продолжился в полноценный BOS continuation: цена потеряла импульс и вернулась в защиту.'),
        }
        if route_key in mapping:
            return mapping[route_key]
        return ('tp1_no_followthrough', 'После TP1 остаток позиции не получил продолжения и вернулся в защитную зону.')

    if route_key == 'smc_ob_fvg_overlap':
        if zone_hold is False:
            return ('ob_fvg_zone_lost', 'Сетап OB+FVG сломался после входа: цена не удержала приоритетную OB/FVG-зону и быстро ушла против позиции до TP1.')
        if fast_invalidation or 'against' in struct.lower():
            return ('ob_fvg_structure_failed', f'Сетап OB+FVG сломался по структуре: после входа цена быстро пошла против {side}, и продолжение от приоритетной зоны не подтвердилось.')
        return ('ob_fvg_no_followthrough', 'После входа сетап OB+FVG не дал ожидаемого импульсного продолжения от приоритетной зоны до TP1.')

    if route_key == 'smc_htf_ob_ltf_fvg':
        if zone_hold is False:
            return ('htf_ob_ltf_retest_failed', 'Сетап HTF OB + LTF FVG сломался на retest: цена не удержала зону возврата после входа и ушла в стоп до TP1.')
        if fast_invalidation:
            return ('htf_ob_ltf_fast_invalidation', 'Сетап HTF OB + LTF FVG был быстро инвалидирован после входа: retest не подтвердился, и цена сразу пошла против позиции.')
        return ('htf_ob_ltf_no_followthrough', 'После входа HTF OB + LTF FVG retest не дал продолжения по направлению сделки до TP1.')

    if route_key == 'smc_bos_retest_confirm':
        if fast_invalidation or 'against' in struct.lower():
            return ('bos_retest_failed', 'Сетап BOS → retest → confirm сломался после входа: пробой не удержался, retest не подтвердил продолжение, и цена вернулась против позиции до TP1.')
        return ('bos_retest_no_followthrough', 'После входа breakout не дал нормального continuation: подтверждение BOS оказалось слабым, и цена не дошла до TP1.')

    if route_key == 'smc_displacement_origin':
        if fast_invalidation:
            return ('displacement_origin_fast_fail', 'Сетап displacement origin сломался сразу после входа: начальный импульс не удержался, и цена быстро инвалидировала origin-идею.')
        return ('displacement_origin_no_followthrough', 'Сетап displacement origin не дал продолжения: стартовый импульс быстро затух, и цена ушла против позиции до TP1.')

    if route_key == 'smc_dual_fvg_origin':
        if fast_invalidation:
            return ('dual_fvg_origin_fast_fail', 'Сетап dual/stacked FVG origin сломался сразу после входа: stacked FVG не удержал движение, и цена быстро пошла против позиции.')
        return ('dual_fvg_origin_no_followthrough', 'Сетап dual/stacked FVG origin не дал продолжения: после входа не появилось нормального impulse continuation до TP1.')

    if route_key == 'smc_liquidity_reclaim':
        if zone_hold is False:
            return ('liquidity_reclaim_not_held', 'Сетап liquidity reclaim сломался после входа: reclaim-зона не удержалась, и цена ушла обратно против позиции до TP1.')
        if fast_invalidation:
            return ('liquidity_reclaim_fast_fail', 'Сетап liquidity reclaim был быстро инвалидирован: после reclaim не появилось устойчивого BOS continuation, и цена резко пошла против позиции.')
        return ('liquidity_reclaim_no_followthrough', 'После входа liquidity reclaim не перешёл в ожидаемое continuation-движение до TP1.')

    if zone_hold is False:
        return ('zone_retest_failed', 'После входа цена не удержала retest-зону и ушла в стоп до TP1.')
    if fast_invalidation:
        return ('fast_invalidation_after_entry', 'После входа сделка была быстро инвалидирована: цена почти сразу пошла против позиции до TP1.')
    if follow in ('нет', 'слабый'):
        return ('no_followthrough_after_entry', 'После входа не было нормального продолжения по направлению сделки до TP1.')
    if vwap_state == 'потерян':
        return ('vwap_lost_after_entry', 'После входа цена потеряла VWAP-опору и ушла против позиции до TP1.')
    return ('loss_before_tp1_sl', 'Цена дошла до SL до TP1 без подтверждённого continuation после входа.')


def _loss_diag_ta_filter_keys(*values: str, direction: str = "", setup_key: str = "") -> list[str]:
    text = "\n".join([str(v or "") for v in values if str(v or "").strip()])
    if not text:
        return []
    out: list[str] = []

    def _push(key: str) -> None:
        key = str(key or "").strip()
        if key and key not in out:
            out.append(key)

    for raw_line in text.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        core = _strip_private_ta_prefix(line)
        core_l = core.lower()

        if "ta score:" in core_l:
            m = re.search(r"TA score:\s*([0-9]+(?:\.[0-9]+)?)\/100.*?need\s*[≥>=]*\s*([0-9]+(?:\.[0-9]+)?)", core, flags=re.IGNORECASE)
            if m:
                try:
                    if float(m.group(1)) < float(m.group(2)):
                        _push("low_ta_score")
                except Exception:
                    pass

        if "signal strength:" in core_l:
            m = re.search(r"Signal strength:\s*([0-9]+(?:\.[0-9]+)?)\/10", core, flags=re.IGNORECASE)
            try:
                if m and float(m.group(1)) < float(os.getenv("LOSS_DIAG_MIN_SIGNAL_STRENGTH", "6.0") or 6.0):
                    _push("weak_signal_strength")
            except Exception:
                pass
            if "(weak)" in core_l or "слаб" in core_l:
                _push("weak_signal_strength")

        if "confidence:" in core_l:
            m = re.search(r"Confidence:\s*([0-9]+(?:\.[0-9]+)?)\/100.*?need\s*[≥>=]*\s*([0-9]+(?:\.[0-9]+)?)", core, flags=re.IGNORECASE)
            if m:
                try:
                    if float(m.group(1)) < float(m.group(2)):
                        _push("low_confidence")
                except Exception:
                    pass

        if "macd hist" in core_l and ("⚠️" in line or "❌" in line):
            _push("macd_momentum_weak")

        if "adx 30m/1h" in core_l and ("⚠️" in line or "❌" in line):
            _push("weak_trend_context")

        if "vol xavg" in core_l and ("⚠️" in line or "❌" in line):
            _push("weak_volume_support")

        if "vwap:" in core_l and ("⚠️" in line or "❌" in line):
            _push("vwap_side_weak")

        if "breakout/retest:" in core_l and setup_key in ("breakout", "zone_retest"):
            if "—" in core or re.search(r"breakout/retest:\s*[-—]+", core_l):
                _push("breakout_retest_weak" if setup_key == "breakout" else "zone_hold_weak")

        if "ob retest:" in core_l and setup_key == "zone_retest" and "—" in core:
            _push("zone_hold_weak")

        if "sweep(5m):" in core_l and setup_key == "liquidity_reclaim" and "—" in core:
            _push("reclaim_confirmation_weak")

        if "ch:" in core_l:
            dir_u = str(direction or "").upper().strip()
            if (dir_u == "LONG" and "desc@lower" in core_l) or (dir_u == "SHORT" and "asc@upper" in core_l):
                _push("weak_trend_context")

    return out

def _loss_diag_improve_from_weak_filter(key: str, *, market: str = "", side: str = "", setup_key: str = "") -> str:
    k = str(key or "").strip()
    mkt = str(market or "").upper().strip()
    side_u = str(side or "").upper().strip()
    setup_u = str(setup_key or "").strip()
    mapping = {
        "low_confidence": "raise_min_confidence",
        "few_confirmations": "require_more_confirmations",
        "low_ta_score": "raise_min_ta_score",
        "weak_signal_strength": "raise_min_ta_score",
        "macd_momentum_weak": "tighten_trend_alignment",
        "vwap_side_weak": "tighten_trend_alignment",
        "weak_trend_context": "tighten_trend_alignment",
        "trigger_reconfirm_weak": "tighten_trigger_reconfirm",
        "tp1_protection": "tighten_tp1_protection",
        "weak_volume_support": "tighten_volume_support",
        "reclaim_confirmation_weak": "tighten_liquidity_reclaim",
    }
    if k == "low_rr":
        return "tighten_rr_spot" if mkt == "SPOT" else "raise_min_rr"
    if k == "breakout_retest_weak":
        return "tighten_breakout_retest"
    if k == "zone_hold_weak":
        return "tighten_zone_retest_long" if side_u == "LONG" else "tighten_zone_retest"
    if k == "origin_entry_early":
        return "tighten_origin_entry"
    if k in mapping:
        return mapping[k]
    if setup_u == "normal_pending_trigger":
        return "tighten_trigger_reconfirm"
    return ""


def _loss_diag_reason_text(reason_code: str, fallback: str) -> str:
    key = str(reason_code or '').strip()
    mapping = {
        'loss_before_tp1_sl': 'Цена дошла до SL до TP1.',
        'loss_after_tp1_sl': 'После TP1 остаток позиции вернулся в SL.',
        'loss_sl': 'Цена дошла до SL — сигнал закрылся в минус.',
        'timeout_closed': 'Сигнал закрыт по времени.',
        'zone_retest_failed': 'После входа цена не удержала retest-зону и ушла в стоп до TP1.',
        'fast_invalidation_after_entry': 'После входа сделка была быстро инвалидирована и ушла в стоп до TP1.',
        'tp1_no_followthrough': 'После TP1 не было продолжения по направлению сделки, и остаток позиции вернулся в защиту.',
        'tp1_protection_failed': 'После TP1 защитная логика не удержала остаток позиции от возврата в стоп.',
        'structure_break_against_entry': 'После входа структура быстро сломалась против направления сделки.',
        'no_followthrough_after_entry': 'После входа не было нормального continuation по направлению сделки до TP1.',
        'false_breakout_after_entry': 'После входа пробой быстро вернулся обратно и сломал идею continuation.',
        'vwap_lost_after_entry': 'После входа цена потеряла VWAP-опору и ушла против позиции до TP1.',
    }
    return mapping.get(key, fallback or 'Сигнал закрылся в минус.')


def _loss_diag_weak_filter_label(key: str) -> str:
    mapping = {
        'low_confidence': 'низкий confidence',
        'low_rr': 'слабый RR',
        'few_confirmations': 'мало подтверждений',
        'low_ta_score': 'низкий TA score',
        'weak_signal_strength': 'слабая сила сигнала',
        'weak_trend_context': 'слабый тренд / ADX контекст',
        'macd_momentum_weak': 'MACD не подтвердил импульс',
        'vwap_side_weak': 'цена по слабую сторону VWAP',
        'breakout_retest_weak': 'слабый breakout / retest',
        'zone_hold_weak': 'слабый retest зоны',
        'origin_entry_early': 'слишком ранний origin entry',
        'trigger_reconfirm_weak': 'слабый pending trigger',
        'reclaim_confirmation_weak': 'слабый liquidity reclaim',
        'tp1_protection': 'слабая защита после TP1',
        'weak_volume_support': 'слабый объём / Vol xAvg',
    }
    return mapping.get(str(key or '').strip(), str(key or '').strip())


def _loss_diag_improve_label(key: str) -> str:
    mapping = {
        'raise_min_confidence': 'поднять minimum confidence для breakout-входов',
        'raise_min_rr': 'поднять minimum RR',
        'tighten_rr_spot': 'снизить допуск на слабый RR в SPOT',
        'require_more_confirmations': 'добавить 1 дополнительное подтверждение для normal_pending_trigger',
        'raise_min_ta_score': 'поднять minimum TA score',
        'tighten_breakout_retest': 'поднять minimum confidence для breakout-входов',
        'tighten_zone_retest': 'усилить retest-фильтр перед входом',
        'tighten_zone_retest_long': 'усилить retest-фильтр перед LONG входом',
        'tighten_origin_entry': 'ужесточить origin entry',
        'tighten_trigger_reconfirm': 'добавить 1 дополнительное подтверждение для normal_pending_trigger',
        'tighten_liquidity_reclaim': 'усилить reclaim confirmation',
        'tighten_trend_alignment': 'жёстче отсеивать входы против слабого тренда',
        'tighten_tp1_protection': 'усилить TP1 → SL защиту',
        'tighten_volume_support': 'требовать Vol xAvg ≥ 1.10',
        'avoid_late_entry': 'жёстче отсекать поздний вход внутри уже идущего движения',
        'tighten_retest_depth': 'жёстче фильтровать слишком глубокий retest',
        'raise_min_confirm_strength': 'поднять минимум силы confirm-candle перед входом',
        'avoid_overhead_supply': 'не брать LONG прямо под supply / Strong High',
        'avoid_support_short': 'не брать SHORT прямо над support / Weak Low',
        'require_clean_space_to_tp': 'требовать чистое пространство до TP1/TP2',
    }
    return mapping.get(str(key or '').strip(), str(key or '').strip())


def _loss_diag_parse_float(raw) -> float | None:
    try:
        if raw is None:
            return None
        return float(str(raw).replace(',', '.'))
    except Exception:
        return None


def _loss_diag_reason_human_label(code: str) -> str:
    mapping = {
        'ob_fvg_zone_failed': 'OB/FVG priority zone failed',
        'ob_fvg_reaction_weak': 'Weak reaction from OB/FVG zone',
        'ob_fvg_invalidation_fast': 'Fast invalidation of OB/FVG setup',
        'ob_fvg_no_continuation': 'OB/FVG setup gave no continuation',
        'htf_ob_ltf_retest_failed': 'HTF OB + LTF retest failed',
        'htf_zone_not_held': 'HTF zone was not held',
        'ltf_confirm_weak': 'LTF confirm was weak',
        'no_continuation_after_retest': 'No continuation after retest',
        'bos_retest_failed': 'BOS retest confirm failed',
        'bos_confirm_weak': 'BOS confirm was weak',
        'bos_level_reclaimed_back': 'BOS level was reclaimed back',
        'fake_breakout_no_continuation': 'Breakout trap / no continuation',
        'displacement_failed': 'Displacement failed',
        'origin_not_held': 'Origin did not hold',
        'impulse_died_fast': 'Impulse died fast',
        'displacement_too_weak': 'Displacement was too weak',
        'dual_fvg_failed': 'Dual / stacked FVG failed',
        'stacked_fvg_not_held': 'Stacked FVG was not held',
        'confluence_failed': 'Confluence failed',
        'reaction_from_dual_fvg_weak': 'Weak reaction from dual FVG',
        'liquidity_reclaim_failed': 'Liquidity reclaim failed',
        'reclaim_not_confirmed': 'Reclaim was not confirmed',
        'reclaim_lost_back': 'Reclaim level was lost back',
        'sweep_without_continuation': 'Sweep without continuation',
        'immediate_invalidation': 'Immediate invalidation after entry',
        'weak_reaction_then_fade': 'Weak reaction then fade',
        'fake_confirm': 'Fake confirm',
        'failed_reclaim_hold': 'Failed reclaim hold',
        'breakout_trap': 'Breakout trap',
        'weak_followthrough': 'Weak follow-through',
        'no_post_entry_expansion': 'No post-entry expansion',
        'continuation_missing': 'Continuation missing',
        'zone_reaction_too_weak': 'Zone reaction too weak',
        'level_reclaimed_back': 'Key level reclaimed back',
        'invalidation_too_fast': 'Invalidation happened too fast',
        'counter_move_stronger_than_expected': 'Counter-move was too strong',
        'range_trap_behavior': 'Range trap behavior',
        'post_entry_volume_faded': 'Volume faded after entry',
        'entry_too_deep_into_zone': 'Entry was too deep into zone',
        'late_entry_inside_expansion': 'Entry was too late inside expansion',
        'structure_shift_against_trade': 'Structure shifted against trade',
        'multiple_failed_pushes': 'Multiple failed pushes after entry',
        'tp1_never_threatened': 'TP1 was never threatened',
        'sl_hit_without_meaningful_excursion': 'SL hit without meaningful excursion',
        'confirm_too_weak': 'Confirm candle was too weak',
        'retest_too_deep': 'Retest was too deep',
        'reclaim_without_acceptance': 'Reclaim had no acceptance',
        'origin_impulse_not_clean': 'Origin impulse was not clean',
        'entry_into_overhead_supply': 'Entry into overhead supply',
        'long_below_strong_high': 'Long directly below Strong High / liquidity',
        'short_above_weak_low': 'Short directly above Weak Low / liquidity',
        'entry_in_range_premium': 'Entry in bad range location',
        'late_entry_after_exhausted_move': 'Late entry after exhausted move',
        'no_clean_retest_acceptance': 'No clean retest acceptance',
    }
    return mapping.get(str(code or '').strip(), str(code or '').strip().replace('_', ' '))


def _loss_diag_format_reason_text(code: str, fallback: str = '') -> str:
    mapping = {
        'ob_fvg_zone_failed': 'OB/FVG priority zone failed: цена вошла в приоритетную OB/FVG зону, но реакция была слабой, зона не удержалась и сетап быстро инвалидировался.',
        'ob_fvg_reaction_weak': 'Weak reaction from OB/FVG zone: цена зашла в OB/FVG, но не дала сильной реакции, поэтому continuation не появился.',
        'ob_fvg_invalidation_fast': 'Fast invalidation of OB/FVG setup: после входа цена почти сразу пошла против сделки и быстро сломала идею OB/FVG priority hold.',
        'ob_fvg_no_continuation': 'OB/FVG setup gave no continuation: зона формально отработала касание, но не дала импульсного продолжения по направлению сделки.',
        'htf_ob_ltf_retest_failed': 'HTF OB + LTF retest failed: HTF зона не удержала цену, LTF retest не дал подтверждения, continuation после retest не появился.',
        'htf_zone_not_held': 'HTF zone was not held: цена слишком глубоко вернулась в HTF область и не смогла удержаться по сценарию сделки.',
        'ltf_confirm_weak': 'LTF confirm was weak: локальное подтверждение после retest выглядело слишком слабым и не дало нормального продолжения.',
        'no_continuation_after_retest': 'No continuation after retest: retest был, но после него цена не перешла в нормальное continuation-движение.',
        'bos_retest_failed': 'BOS retest confirm failed: BOS был, но retest/confirm не удержался, цена вернулась обратно за уровень и continuation сломался.',
        'bos_confirm_weak': 'BOS confirm was weak: после пробоя confirm-candle оказалась слабой, и breakout не получил нормального continuation.',
        'bos_level_reclaimed_back': 'BOS level was reclaimed back: после входа цена быстро вернулась за BOS-уровень и сломала breakout-сценарий.',
        'fake_breakout_no_continuation': 'Breakout trap / no continuation: пробой выглядел рабочим, но не дал реального продолжения и превратился в trap-сценарий.',
        'displacement_failed': 'Displacement failed: displacement оказался слабым, origin не дал продолжения, импульс быстро умер после входа.',
        'origin_not_held': 'Origin did not hold: ключевая origin-зона не удержала цену после входа и сценарий развалился.',
        'impulse_died_fast': 'Impulse died fast: после входа стартовый импульс быстро затух и цена не смогла продолжить движение по сделке.',
        'displacement_too_weak': 'Displacement was too weak: стартовый displacement не выглядел достаточно сильным для continuation.',
        'dual_fvg_failed': 'Dual / stacked FVG failed: stacked FVG не удержался, confluence не сработал, зона быстро инвалидировалась.',
        'stacked_fvg_not_held': 'Stacked FVG was not held: цена быстро прошла dual/stacked FVG область без нормального удержания.',
        'confluence_failed': 'Confluence failed: совпадение факторов в dual FVG зоне не дало ожидаемой сильной реакции.',
        'reaction_from_dual_fvg_weak': 'Weak reaction from dual FVG: реакция из stacked FVG была слишком слабой для continuation.',
        'liquidity_reclaim_failed': 'Liquidity reclaim failed: после sweep reclaim не закрепился, цена ушла обратно против идеи reclaim и continuation не появился.',
        'reclaim_not_confirmed': 'Reclaim was not confirmed: касание reclaim-level было, но закрепления по правильную сторону уровня не произошло.',
        'reclaim_lost_back': 'Reclaim level was lost back: после краткого reclaim цена снова ушла обратно за уровень и идея сделки сломалась.',
        'sweep_without_continuation': 'Sweep without continuation: sweep случился, но после него рынок не перешёл в ожидаемое continuation-движение.',
        'immediate_invalidation': 'Immediate invalidation after entry: сразу после входа цена почти без реакции пошла к SL — сетап был слабым уже в момент входа.',
        'weak_reaction_then_fade': 'Weak reaction then fade: после входа была локальная реакция, но она быстро затухла и не перешла в continuation.',
        'fake_confirm': 'Fake confirm: подтверждение выглядело валидным, но за ним не последовало реального расширения движения.',
        'failed_reclaim_hold': 'Failed reclaim hold: reclaim был показан, но рынок не смог закрепиться по правильную сторону уровня.',
        'breakout_trap': 'Breakout trap: пробой быстро вернулся обратно за уровень, поэтому continuation оказался ложным.',
        'entry_into_overhead_supply': 'Entry into overhead supply: LONG был открыт прямо под зоной продавца / red FVG / supply. Сверху не было чистого пространства до TP, поэтому цена быстро получила sell pressure и ушла к SL.',
        'long_below_strong_high': 'Long directly below Strong High / liquidity: вход LONG был сделан под сильным high/ликвидностью. Покупка пришлась в место, где продавец чаще защищает уровень, поэтому continuation был маловероятен.',
        'short_above_weak_low': 'Short directly above Weak Low / liquidity: вход SHORT был сделан прямо над слабым low/ликвидностью. Продавать в такую поддержку опасно — снизу не было чистого пространства для continuation.',
        'entry_in_range_premium': 'Entry in bad range location: вход был в плохой части range — слишком близко к противоположной ликвидности/стене, а не от дисконта/премиума с запасом хода.',
        'late_entry_after_exhausted_move': 'Late entry after exhausted move: вход был поздний — основное движение уже прошло, retest был несвежий, нового displacement перед входом не было.',
        'no_clean_retest_acceptance': 'No clean retest acceptance: формальный retest был, но цена не закрепилась по правильную сторону зоны/уровня и не показала acceptance перед continuation.',
    }
    return mapping.get(str(code or '').strip(), fallback or _loss_diag_reason_human_label(code))


def _loss_diag_build_forensic_from_analysis(src: dict, analysis: dict, *, after_tp1: bool, st: str) -> dict:
    route_key = _loss_diag_route_key_from_row(src)
    exact_route = route_key in EXACT_SMC_ROUTE_KEYS
    if st != 'LOSS':
        return {
            'primary_reason_code': '', 'primary_reason_text': '', 'scenario_code': '', 'scenario_text': '',
            'secondary_reason_codes': [], 'secondary_reason_texts': [], 'missed_codes': [], 'missed_texts': [],
            'what_happened_lines': [],
        }

    mfe_pct = abs(float(analysis.get('mfe_pct') or 0.0))
    mae_pct = abs(float(analysis.get('mae_pct') or 0.0))
    bars_to_failure = int(analysis.get('bars_to_failure') or analysis.get('bars_to_sl') or 0)
    fast_invalidation = bool(analysis.get('fast_invalidation'))
    immediate = bool(analysis.get('immediate_invalidation')) or (bars_to_failure and bars_to_failure <= 2 and mfe_pct < 0.15)
    weak_follow = bool(analysis.get('weak_followthrough')) or mfe_pct < 0.35
    no_expansion = bool(analysis.get('no_post_entry_expansion')) or mfe_pct < 0.20
    tp1_threatened = bool(analysis.get('tp1_threatened'))
    structure_against = bool(analysis.get('structure_against_trade'))
    zone_hold = analysis.get('zone_hold')
    reclaim_lost = bool(analysis.get('reclaim_lost_back'))
    confirm_weak = bool(analysis.get('confirm_too_weak'))
    retest_deep = bool(analysis.get('retest_too_deep'))
    volume_faded = bool(analysis.get('post_entry_volume_faded'))
    multiple_failed_pushes = bool(analysis.get('multiple_failed_pushes'))
    side = str(src.get('side') or analysis.get('side') or 'LONG').upper().strip() or 'LONG'
    entry_overhead = bool(analysis.get('entry_into_overhead_supply'))
    long_below_high = bool(analysis.get('long_below_strong_high'))
    short_above_low = bool(analysis.get('short_above_weak_low'))
    bad_range_location = bool(analysis.get('entry_in_range_premium'))
    late_exhausted = bool(analysis.get('late_entry_after_exhausted_move') or analysis.get('late_entry_inside_expansion'))

    secondary = []
    missed = []
    happened = []

    if immediate:
        secondary.append('immediate_invalidation')
        happened.append('сразу после входа цена почти без реакции пошла к SL')
    if weak_follow:
        secondary.append('weak_followthrough')
        happened.append('нормального импульса в сторону сделки не появилось')
    if no_expansion:
        secondary.append('no_post_entry_expansion')
    if not tp1_threatened:
        secondary.append('tp1_never_threatened')
        happened.append('TP1 не был даже нормально поставлен под угрозу')
    if structure_against:
        secondary.append('structure_shift_against_trade')
        happened.append('после входа структура 5m сместилась против сделки')
    if fast_invalidation:
        secondary.append('invalidation_too_fast')
    if volume_faded:
        secondary.append('post_entry_volume_faded')
        happened.append('объём после входа ослаб и не поддержал continuation')
    if multiple_failed_pushes:
        secondary.append('multiple_failed_pushes')
        happened.append('после входа было несколько слабых попыток продолжения без результата')
    if mae_pct >= max(0.45, mfe_pct * 1.2):
        secondary.append('counter_move_stronger_than_expected')
    if mfe_pct < 0.12:
        secondary.append('sl_hit_without_meaningful_excursion')
    if not bool(analysis.get('continuation_seen')):
        secondary.append('continuation_missing')
    if bool(analysis.get('range_trap_behavior')):
        secondary.append('range_trap_behavior')
    if entry_overhead:
        secondary.append('entry_into_overhead_supply')
        happened.append('вход был слишком близко к зоне продавца / противоположной ликвидности')
    if long_below_high:
        secondary.append('long_below_strong_high')
        happened.append('LONG был открыт прямо под strong high / liquidity pool')
    if short_above_low:
        secondary.append('short_above_weak_low')
        happened.append('SHORT был открыт прямо над weak low / support liquidity')
    if bad_range_location:
        secondary.append('entry_in_range_premium')
    if late_exhausted:
        secondary.append('late_entry_after_exhausted_move')
        happened.append('вход был поздний: основной импульс уже отработал до сигнала')

    if exact_route:
        if route_key == 'smc_ob_fvg_overlap':
            if immediate or zone_hold is False:
                primary = 'ob_fvg_zone_failed'
            elif bool(analysis.get('zone_reaction_too_weak')):
                primary = 'ob_fvg_reaction_weak'
            elif fast_invalidation:
                primary = 'ob_fvg_invalidation_fast'
            else:
                primary = 'ob_fvg_no_continuation'
            scenario = 'immediate_invalidation' if immediate else 'weak_reaction_then_fade'
            if bool(analysis.get('zone_reaction_too_weak')):
                secondary.append('zone_reaction_too_weak')
            if retest_deep:
                secondary.append('entry_too_deep_into_zone'); missed.append('entry_too_deep_into_zone')
        elif route_key == 'smc_htf_ob_ltf_fvg':
            if zone_hold is False:
                primary = 'htf_ob_ltf_retest_failed'
            elif confirm_weak:
                primary = 'ltf_confirm_weak'
            elif bool(analysis.get('htf_zone_not_held')):
                primary = 'htf_zone_not_held'
            else:
                primary = 'no_continuation_after_retest'
            scenario = 'weak_reaction_then_fade'
            if retest_deep:
                secondary.append('level_reclaimed_back'); missed.append('retest_too_deep')
        elif route_key == 'smc_bos_retest_confirm':
            if bool(analysis.get('level_reclaimed_back')) or structure_against:
                primary = 'bos_retest_failed'
            elif confirm_weak:
                primary = 'bos_confirm_weak'
            elif bool(analysis.get('level_reclaimed_back')):
                primary = 'bos_level_reclaimed_back'
            else:
                primary = 'fake_breakout_no_continuation'
            scenario = 'breakout_trap' if bool(analysis.get('level_reclaimed_back')) else 'fake_confirm'
            secondary.extend(['level_reclaimed_back'] if bool(analysis.get('level_reclaimed_back')) else [])
            if confirm_weak:
                missed.append('confirm_too_weak')
            if retest_deep:
                missed.append('retest_too_deep')
        elif route_key == 'smc_displacement_origin':
            if immediate:
                primary = 'displacement_failed'
            elif bool(analysis.get('origin_not_held')):
                primary = 'origin_not_held'
            elif bool(analysis.get('displacement_too_weak')):
                primary = 'displacement_too_weak'
            else:
                primary = 'impulse_died_fast'
            scenario = 'immediate_invalidation' if immediate else 'weak_reaction_then_fade'
            if bool(analysis.get('origin_not_held')):
                secondary.append('level_reclaimed_back')
            if bool(analysis.get('displacement_too_weak')):
                missed.append('origin_impulse_not_clean')
        elif route_key == 'smc_dual_fvg_origin':
            if immediate:
                primary = 'dual_fvg_failed'
            elif bool(analysis.get('stacked_fvg_not_held')):
                primary = 'stacked_fvg_not_held'
            elif bool(analysis.get('confluence_failed')):
                primary = 'confluence_failed'
            else:
                primary = 'reaction_from_dual_fvg_weak'
            scenario = 'weak_reaction_then_fade'
            if bool(analysis.get('zone_reaction_too_weak')):
                secondary.append('zone_reaction_too_weak')
        elif route_key == 'smc_liquidity_reclaim':
            if reclaim_lost:
                primary = 'liquidity_reclaim_failed'
            elif bool(analysis.get('reclaim_not_confirmed')):
                primary = 'reclaim_not_confirmed'
            elif reclaim_lost:
                primary = 'reclaim_lost_back'
            else:
                primary = 'sweep_without_continuation'
            scenario = 'failed_reclaim_hold' if reclaim_lost or bool(analysis.get('reclaim_not_confirmed')) else 'weak_reaction_then_fade'
            if bool(analysis.get('reclaim_not_confirmed')):
                missed.append('reclaim_without_acceptance')
            if reclaim_lost:
                secondary.append('level_reclaimed_back')
        else:
            primary = 'continuation_missing'
            scenario = 'weak_reaction_then_fade'
    else:
        primary = 'immediate_invalidation' if immediate else 'continuation_missing'
        scenario = 'weak_reaction_then_fade' if weak_follow else ''

    # Highest priority: explain WHY the entry itself was bad on the chart, not only what happened after entry.
    if entry_overhead:
        if side == 'LONG' and long_below_high:
            primary = 'long_below_strong_high'
        elif side == 'SHORT' and short_above_low:
            primary = 'short_above_weak_low'
        else:
            primary = 'entry_into_overhead_supply'
        scenario = 'entry_into_overhead_supply'
        missed.append('require_clean_space_to_tp')
    elif late_exhausted:
        primary = 'late_entry_after_exhausted_move'
        scenario = 'late_entry_after_exhausted_move'
        missed.append('late_entry_after_exhausted_move')
    elif bad_range_location and (not tp1_threatened or weak_follow):
        primary = 'entry_in_range_premium'
        scenario = 'entry_in_range_premium'
        missed.append('require_clean_space_to_tp')

    if late := bool(analysis.get('late_entry_inside_expansion')):
        secondary.append('late_entry_inside_expansion'); missed.append('late_entry_inside_expansion')
    if confirm_weak:
        secondary.append('fake_confirm')
    if retest_deep and 'retest_too_deep' not in missed:
        missed.append('retest_too_deep')

    secondary = [x for x in dict.fromkeys([x for x in secondary if x and x != primary])]
    missed = [x for x in dict.fromkeys([x for x in missed if x])]
    happened = [x for x in dict.fromkeys([x for x in happened if x])]
    primary_text = _loss_diag_format_reason_text(primary)
    scenario_text = _loss_diag_format_reason_text(scenario, _loss_diag_reason_human_label(scenario)) if scenario else ''
    return {
        'primary_reason_code': primary,
        'primary_reason_text': primary_text,
        'scenario_code': scenario,
        'scenario_text': scenario_text,
        'secondary_reason_codes': secondary,
        'secondary_reason_texts': [_loss_diag_reason_human_label(x) for x in secondary],
        'missed_codes': missed,
        'missed_texts': [_loss_diag_reason_human_label(x) for x in missed],
        'what_happened_lines': happened,
    }


def _loss_diag_parse_metrics(row: dict | None) -> dict:
    src = dict(row or {})
    text = "\n".join([str(src.get('risk_note') or ''), str(src.get('orig_text') or '')])
    side = str(src.get('side') or '').upper().strip() or 'LONG'
    metrics: dict = {
        'rr': _daily_report_rr_from_row(src),
        'rr_need': 1.90 if str(src.get('market') or '').upper().strip() == 'SPOT' else 1.80,
        'metrics_weak_counts': {},
        'side': side,
    }

    def _count(key: str) -> None:
        mc = metrics.setdefault('metrics_weak_counts', {})
        mc[key] = int(mc.get(key, 0) or 0) + 1

    m = re.search(r"RSI\(5m\):\s*([+-]?[0-9]+(?:\.[0-9]+)?)\s*\[need\s*([0-9]+(?:\.[0-9]+)?)\s*[-–]\s*([0-9]+(?:\.[0-9]+)?)\]", text, flags=re.IGNORECASE)
    if m:
        metrics['rsi_5m'] = float(m.group(1))
        metrics['rsi_need_min'] = float(m.group(2))
        metrics['rsi_need_max'] = float(m.group(3))

    m = re.search(r"MACD\s*hist\(5m\):\s*([+\-]?[0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
    if m:
        metrics['macd_hist_5m'] = float(m.group(1))
        metrics['macd_need'] = float(os.getenv('LOSS_DIAG_MACD_MIN', '0.0002') or 0.0002)
        macd_val = float(m.group(1))
        if (side == 'LONG' and macd_val <= float(metrics['macd_need'])) or (side == 'SHORT' and macd_val >= -float(metrics['macd_need'])):
            _count('macd_hist_5m')

    m = re.search(r"ADX\s*30m\/1h:\s*([0-9]+(?:\.[0-9]+)?)\s*\/\s*([0-9]+(?:\.[0-9]+)?)\s*\[need\s*[≥>=]*\s*([0-9]+(?:\.[0-9]+)?)\s*\/\s*[≥>=]*\s*([0-9]+(?:\.[0-9]+)?)\]", text, flags=re.IGNORECASE)
    if m:
        metrics['adx_30m'] = float(m.group(1))
        metrics['adx_1h'] = float(m.group(2))
        metrics['adx_need_30m'] = float(m.group(3))
        metrics['adx_need_1h'] = float(m.group(4))
        if metrics['adx_30m'] < metrics['adx_need_30m']:
            _count('adx_30m')
        if metrics['adx_1h'] < metrics['adx_need_1h']:
            _count('adx_1h')

    m = re.search(r"Vol\s*xAvg:\s*([0-9]+(?:\.[0-9]+)?)\s*\[need\s*[≥>=]*\s*([0-9]+(?:\.[0-9]+)?)\]", text, flags=re.IGNORECASE)
    if m:
        metrics['vol_xavg'] = float(m.group(1))
        metrics['vol_need'] = float(m.group(2))
        if metrics['vol_xavg'] < metrics['vol_need']:
            _count('vol_xavg')

    m = re.search(r"TA score:\s*([0-9]+(?:\.[0-9]+)?)\/100.*?need\s*[≥>=]*\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
    if m:
        metrics['ta_score'] = float(m.group(1))
        metrics['ta_need'] = float(m.group(2))
        if metrics['ta_score'] < metrics['ta_need']:
            _count('ta_score')

    m = re.search(r"Confidence:\s*([0-9]+(?:\.[0-9]+)?)\/100.*?need\s*[≥>=]*\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
    if m:
        metrics['confidence'] = float(m.group(1))
        metrics['confidence_need'] = float(m.group(2))
        if metrics['confidence'] < metrics['confidence_need']:
            _count('confidence')

    m = re.search(r"Signal strength:\s*([0-9]+(?:\.[0-9]+)?)\/10", text, flags=re.IGNORECASE)
    if m:
        metrics['signal_strength'] = float(m.group(1))
        metrics['signal_strength_need'] = float(os.getenv('LOSS_DIAG_MIN_SIGNAL_STRENGTH', '6.0') or 6.0)
        if metrics['signal_strength'] < metrics['signal_strength_need']:
            _count('signal_strength')

    m = re.search(r"VWAP:\s*(above|below)", text, flags=re.IGNORECASE)
    if m:
        metrics['vwap_side'] = str(m.group(1)).lower()

    rr = float(metrics.get('rr') or 0.0)
    if rr and rr < float(metrics['rr_need']):
        _count('rr_tp2')
    return metrics


def _loss_diag_load_entry_snapshot(row: dict | None) -> dict:
    src = dict(row or {})
    raw = src.get('entry_snapshot_json')
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return {}
    return {}


def _loss_diag_pick_nested_value(obj, *paths):
    for path in paths:
        cur = obj
        ok = True
        for key in path:
            if isinstance(cur, dict) and key in cur:
                cur = cur.get(key)
            else:
                ok = False
                break
        if ok and cur is not None and str(cur).strip() != '':
            return cur
    return None


def _signal_forensics_entry_snapshot(sig) -> dict:
    snap: dict = {}
    if sig is None:
        return snap

    def _get(name: str, default=None):
        try:
            val = getattr(sig, name, default)
            return default if val is None else val
        except Exception:
            return default

    def _maybe_float(v):
        try:
            if v is None or v == '':
                return None
            return float(v)
        except Exception:
            return None

    snap['route_key'] = str(_get('smc_setup_route', '') or _get('emit_route', '') or '').strip()
    snap['direction'] = str(_get('direction', '') or '').upper().strip()
    snap['entry'] = _maybe_float(_get('entry'))
    snap['sl'] = _maybe_float(_get('sl'))
    snap['tp1'] = _maybe_float(_get('tp1'))
    snap['signal_tf'] = str(_get('timeframe', '') or '').strip()
    snap['confidence'] = _get('confidence')
    snap['rr'] = _maybe_float(_get('rr'))
    snap['entry_bar_time'] = _get('entry_bar_time')

    for fld in (
        'bos_level', 'retest_level', 'ob_top', 'ob_bottom', 'fvg_top', 'fvg_bottom',
        'reclaim_level', 'origin_level', 'sweep_level', 'displacement_start', 'displacement_end',
        'range_high', 'range_low', 'atr_at_entry', 'volume_at_entry', 'trend_context', 'liquidity_side',
        'stacked_fvg_top', 'stacked_fvg_bottom', 'stacked_fvg_bounds'
    ):
        val = _get(fld)
        if val not in (None, '', [], {}):
            snap[fld] = val

    raw_sources = []
    for fld in ('forensics', 'forensic', 'smc_snapshot', 'smart_snapshot', 'debug', 'meta', 'ta_meta', 'risk_meta'):
        val = _get(fld)
        if isinstance(val, dict):
            raw_sources.append(val)

    for src in raw_sources:
        for key, paths in {
            'bos_level': [('bos_level',), ('structure', 'bos_level')],
            'retest_level': [('retest_level',), ('structure', 'retest_level')],
            'ob_top': [('ob_top',), ('ob', 'top')],
            'ob_bottom': [('ob_bottom',), ('ob', 'bottom')],
            'fvg_top': [('fvg_top',), ('fvg', 'top')],
            'fvg_bottom': [('fvg_bottom',), ('fvg', 'bottom')],
            'reclaim_level': [('reclaim_level',)],
            'origin_level': [('origin_level',)],
            'sweep_level': [('sweep_level',)],
            'displacement_start': [('displacement_start',)],
            'displacement_end': [('displacement_end',)],
            'range_high': [('range_high',)],
            'range_low': [('range_low',)],
            'atr_at_entry': [('atr_at_entry',), ('atr', 'entry')],
            'volume_at_entry': [('volume_at_entry',), ('volume', 'entry')],
            'trend_context': [('trend_context',), ('trend', 'context')],
            'liquidity_side': [('liquidity_side',)],
            'stacked_fvg_bounds': [('stacked_fvg_bounds',)],
        }.items():
            if key in snap:
                continue
            val = _loss_diag_pick_nested_value(src, *paths)
            if val not in (None, '', [], {}):
                snap[key] = val

    return {k: v for k, v in snap.items() if v not in (None, '', [], {})}


def _loss_diag_load_close_analysis(row: dict | None) -> dict:
    src = dict(row or {})
    raw = src.get('close_analysis_json')
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return {}
    return {}


def _loss_diag_build_analysis_lines(src: dict, metrics: dict, analysis: dict, *, after_tp1: bool) -> list[str]:
    lines: list[str] = []
    opened = _parse_iso_dt(src.get('opened_at'))
    closed = _parse_iso_dt(src.get('closed_at'))
    if opened and closed and closed > opened:
        duration_min = int(round((closed - opened).total_seconds() / 60.0))
        lines.append(f"вход → SL: {duration_min} мин")
    elif analysis.get('duration_min'):
        lines.append(f"вход → SL: {int(analysis.get('duration_min') or 0)} мин")
    if analysis.get('bars_to_failure') is not None:
        lines.append(f"баров до слома: {int(analysis.get('bars_to_failure') or 0)}")
    if analysis.get('mfe_pct') is not None:
        lines.append(f"макс. ход в сторону сделки: {float(analysis.get('mfe_pct')):+.2f}%")
    if analysis.get('mae_pct') is not None:
        lines.append(f"макс. ход против сделки: {float(analysis.get('mae_pct')):+.2f}%")
    if analysis.get('tp1_threatened') is not None:
        lines.append(f"TP1 под угрозой: {'да' if bool(analysis.get('tp1_threatened')) else 'нет'}")
    if after_tp1:
        if analysis.get('bars_tp1_to_close') is not None:
            lines.append(f"баров от TP1 до SL: {int(analysis.get('bars_tp1_to_close') or 0)}")
        lines.append(f"follow-through после TP1: {analysis.get('follow_through_tp1') or 'нет'}")
    else:
        lines.append(f"follow-through после входа: {analysis.get('follow_through') or 'слабый'}")
        if analysis.get('structure_5m'):
            lines.append(f"структура 5m: {analysis.get('structure_5m')}")
        if analysis.get('vwap_after_entry'):
            lines.append(f"VWAP после входа: {analysis.get('vwap_after_entry')}")
    return [x for x in lines if x]


def _loss_diag_metric_lines(src: dict, metrics: dict, weak_keys: list[str]) -> list[str]:
    lines: list[str] = []
    weak_set = set([str(x or '').strip() for x in (weak_keys or []) if str(x or '').strip()])
    metric_weak = dict(metrics.get('metrics_weak_counts') or {})
    side = str(metrics.get('side') or src.get('side') or '').upper().strip() or 'LONG'

    rr = float(metrics.get('rr') or 0.0)
    rr_need = float(metrics.get('rr_need') or 0.0)
    if rr > 0 and rr_need > 0 and metric_weak.get('rr_tp2'):
        lines.append(f"RR TP2: {rr:.2f} [need ≥{rr_need:.2f}]")
    if metrics.get('signal_strength') is not None and metrics.get('signal_strength_need') is not None and metric_weak.get('signal_strength'):
        lines.append(f"Signal strength: {float(metrics.get('signal_strength')):.1f}/10 [need ≥{float(metrics.get('signal_strength_need')):.1f}]")
    if metrics.get('macd_hist_5m') is not None and metrics.get('macd_need') is not None and metric_weak.get('macd_hist_5m'):
        need = float(metrics.get('macd_need'))
        if side == 'SHORT':
            lines.append(f"MACD hist(5m): {float(metrics.get('macd_hist_5m')):+.4f} [need < -{need:.4f}]")
        else:
            lines.append(f"MACD hist(5m): {float(metrics.get('macd_hist_5m')):+.4f} [need > +{need:.4f}]")
    if metrics.get('adx_30m') is not None and metrics.get('adx_1h') is not None and (metric_weak.get('adx_30m') or metric_weak.get('adx_1h') or 'weak_trend_context' in weak_set):
        lines.append(f"ADX 30m/1h: {float(metrics.get('adx_30m')):.1f} / {float(metrics.get('adx_1h')):.1f} [need ≥{float(metrics.get('adx_need_30m') or 24):.0f} / ≥{float(metrics.get('adx_need_1h') or 26):.0f}]")
    if metrics.get('vol_xavg') is not None and metrics.get('vol_need') is not None and (metric_weak.get('vol_xavg') or 'weak_volume_support' in weak_set):
        lines.append(f"Vol xAvg: {float(metrics.get('vol_xavg')):.2f} [need ≥{float(metrics.get('vol_need')):.2f}]")
    if metrics.get('ta_score') is not None and metrics.get('ta_need') is not None and metric_weak.get('ta_score'):
        lines.append(f"TA score: {float(metrics.get('ta_score')):.0f} [need ≥{float(metrics.get('ta_need')):.0f}]")
    if metrics.get('confidence') is not None and metrics.get('confidence_need') is not None and metric_weak.get('confidence'):
        lines.append(f"Confidence: {float(metrics.get('confidence')):.0f} [need ≥{float(metrics.get('confidence_need')):.0f}]")
    return lines


async def _loss_diag_build_close_analysis(row: dict | None, *, closed_at: dt.datetime | None = None) -> dict:
    src = dict(row or {})
    symbol = str(src.get('symbol') or '').upper().strip()
    market = str(src.get('market') or 'SPOT').upper().strip() or 'SPOT'
    side = str(src.get('side') or 'LONG').upper().strip() or 'LONG'
    route_key = _loss_diag_route_key_from_row(src)
    entry = _loss_diag_parse_float(src.get('entry')) or 0.0
    sl = _loss_diag_parse_float(src.get('sl')) or 0.0
    tp1 = _loss_diag_parse_float(src.get('tp1')) or 0.0
    opened = _parse_iso_dt(src.get('opened_at'))
    closed = _parse_iso_dt(closed_at or src.get('closed_at')) or dt.datetime.now(dt.timezone.utc)
    if not symbol or entry <= 0 or opened is None or closed <= opened:
        return {}
    snapshot = _loss_diag_load_entry_snapshot(src)
    analysis: dict = {
        'route_key': route_key,
        'duration_min': max(1, int(round((closed - opened).total_seconds() / 60.0))),
        'entry_snapshot_present': bool(snapshot),
    }
    if snapshot:
        analysis['entry_snapshot_fields'] = sorted(list(snapshot.keys()))
        for key in ('bos_level', 'retest_level', 'ob_top', 'ob_bottom', 'fvg_top', 'fvg_bottom', 'reclaim_level', 'origin_level', 'sweep_level', 'displacement_start', 'displacement_end', 'range_high', 'range_low', 'atr_at_entry', 'volume_at_entry', 'trend_context', 'liquidity_side', 'stacked_fvg_bounds'):
            if key in snapshot and key not in analysis:
                analysis[key] = snapshot.get(key)
    try:
        df5 = await backend.load_candles(symbol, '5m', market=market, limit=160)
    except Exception:
        df5 = None
    if df5 is None or getattr(df5, 'empty', True):
        analysis['fast_invalidation'] = bool(int(analysis['duration_min']) <= 15)
        return analysis

    try:
        d = df5.copy()
        if 'open_time_ms' in d.columns:
            ts = pd.to_datetime(d['open_time_ms'], unit='ms', utc=True, errors='coerce')
        elif 'timestamp' in d.columns:
            ts = pd.to_datetime(d['timestamp'], unit='ms', utc=True, errors='coerce')
        elif isinstance(d.index, pd.DatetimeIndex):
            ts = pd.to_datetime(d.index, utc=True, errors='coerce')
        else:
            ts = pd.Series([pd.NaT] * len(d))
        d['_ts'] = ts
        d = d[(d['_ts'].notna()) & (d['_ts'] >= (opened - dt.timedelta(hours=8))) & (d['_ts'] <= (closed + dt.timedelta(minutes=10)))].copy()
        if d.empty:
            analysis['fast_invalidation'] = bool(int(analysis['duration_min']) <= 15)
            return analysis
        for col in ('open', 'high', 'low', 'close'):
            d[col] = pd.to_numeric(d[col], errors='coerce')
        if 'volume' in d.columns:
            d['volume'] = pd.to_numeric(d['volume'], errors='coerce').fillna(0.0)
        else:
            d['volume'] = 0.0
        d = d.dropna(subset=['open', 'high', 'low', 'close']).sort_values('_ts')
        if d.empty:
            return analysis

        pre = d[d['_ts'] < opened].tail(72).copy()
        if not pre.empty and entry > 0:
            ph = pre['high'].astype(float)
            pl = pre['low'].astype(float)
            pc = pre['close'].astype(float)
            prior_high = float(ph.max())
            prior_low = float(pl.min())
            prior_range = max(prior_high - prior_low, 1e-9)
            entry_pos = (entry - prior_low) / prior_range
            dist_high_pct = ((prior_high - entry) / entry) * 100.0
            dist_low_pct = ((entry - prior_low) / entry) * 100.0
            analysis['prior_range_high'] = round(prior_high, 8)
            analysis['prior_range_low'] = round(prior_low, 8)
            analysis['entry_position_in_prior_range'] = round(entry_pos, 3)
            analysis['distance_to_prior_high_pct'] = round(dist_high_pct, 2)
            analysis['distance_to_prior_low_pct'] = round(dist_low_pct, 2)
            pre_first = float(pc.iloc[0]) if len(pc) else entry
            pre_last = float(pc.iloc[-1]) if len(pc) else entry
            pre_move_pct = ((pre_last - pre_first) / max(pre_first, 1e-9)) * 100.0
            analysis['pre_entry_move_pct'] = round(pre_move_pct, 2)
            if side == 'LONG':
                clean_space_tp1_pct = ((tp1 - entry) / entry) * 100.0 if tp1 > entry else 0.0
                analysis['clean_space_to_tp1_pct'] = round(clean_space_tp1_pct, 2)
                analysis['entry_in_range_premium'] = bool(entry_pos >= 0.68)
                analysis['long_below_strong_high'] = bool(dist_high_pct >= 0 and (dist_high_pct <= max(clean_space_tp1_pct * 1.15, 0.35) or entry_pos >= 0.82))
                analysis['entry_into_overhead_supply'] = bool(analysis.get('long_below_strong_high') or (entry_pos >= 0.72 and clean_space_tp1_pct <= 0.45))
                analysis['late_entry_after_exhausted_move'] = bool(pre_move_pct > 0.65 and entry_pos >= 0.60)
            else:
                clean_space_tp1_pct = ((entry - tp1) / entry) * 100.0 if 0 < tp1 < entry else 0.0
                analysis['clean_space_to_tp1_pct'] = round(clean_space_tp1_pct, 2)
                analysis['entry_in_range_premium'] = bool(entry_pos <= 0.32)
                analysis['short_above_weak_low'] = bool(dist_low_pct >= 0 and (dist_low_pct <= max(clean_space_tp1_pct * 1.15, 0.35) or entry_pos <= 0.18))
                analysis['entry_into_overhead_supply'] = bool(analysis.get('short_above_weak_low') or (entry_pos <= 0.28 and clean_space_tp1_pct <= 0.45))
                analysis['late_entry_after_exhausted_move'] = bool(pre_move_pct < -0.65 and entry_pos <= 0.40)

        post = d[d['_ts'] >= opened].copy()
        if post.empty:
            post = d.copy()
        highs = post['high'].astype(float)
        lows = post['low'].astype(float)
        closes = post['close'].astype(float)
        opens = post['open'].astype(float)
        vols = post['volume'].astype(float)
        bars = len(post)
        analysis['bars_to_failure'] = bars
        analysis['bars_to_sl'] = bars
        analysis['fast_invalidation'] = bool(int(analysis.get('duration_min') or 0) <= 15 or bars <= 3)

        risk_pct = abs((entry - sl) / entry) * 100.0 if sl > 0 and entry > 0 else 0.0
        if side == 'LONG':
            best_px = float(highs.max())
            worst_px = float(lows.min())
            analysis['mfe_pct'] = round(((best_px - entry) / entry) * 100.0, 2)
            analysis['mae_pct'] = round(((worst_px - entry) / entry) * 100.0, 2)
            analysis['tp1_threatened'] = bool(tp1 > 0 and best_px >= (entry + max(tp1 - entry, 0.0) * 0.8))
            analysis['zone_hold'] = bool((closes.iloc[:min(3, bars)] >= entry).sum() >= max(1, min(3, bars) - 1))
            analysis['level_reclaimed_back'] = bool((closes < entry).sum() >= 2)
            analysis['reclaim_lost_back'] = bool((closes < entry).sum() >= 2)
        else:
            best_px = float(lows.min())
            worst_px = float(highs.max())
            analysis['mfe_pct'] = round(((entry - best_px) / entry) * 100.0, 2)
            analysis['mae_pct'] = round(((entry - worst_px) / entry) * 100.0, 2)
            analysis['tp1_threatened'] = bool(tp1 > 0 and best_px <= (entry - max(entry - tp1, 0.0) * 0.8))
            analysis['zone_hold'] = bool((closes.iloc[:min(3, bars)] <= entry).sum() >= max(1, min(3, bars) - 1))
            analysis['level_reclaimed_back'] = bool((closes > entry).sum() >= 2)
            analysis['reclaim_lost_back'] = bool((closes > entry).sum() >= 2)

        analysis['risk_pct_to_sl'] = round(risk_pct, 4)
        if risk_pct > 0:
            analysis['mfe_r'] = round(abs(float(analysis['mfe_pct'])) / risk_pct, 2)
            analysis['mae_r'] = round(abs(float(analysis['mae_pct'])) / risk_pct, 2)

        first3 = post.head(min(3, bars))
        first6 = post.head(min(6, bars))
        first3_high = float(first3['high'].max())
        first3_low = float(first3['low'].min())
        first6_high = float(first6['high'].max())
        first6_low = float(first6['low'].min())

        if side == 'LONG':
            first_push_pct = ((first3_high - entry) / entry) * 100.0
            against_move_pct = ((entry - first3_low) / entry) * 100.0
            continuation_seen = bool(first6_high > entry)
            bullish_count = int((first6['close'] > first6['open']).sum())
            against_count = int((first6['close'] < first6['open']).sum())
        else:
            first_push_pct = ((entry - first3_low) / entry) * 100.0
            against_move_pct = ((first3_high - entry) / entry) * 100.0
            continuation_seen = bool(first6_low < entry)
            bullish_count = int((first6['close'] < first6['open']).sum())
            against_count = int((first6['close'] > first6['open']).sum())

        analysis['first_push_pct'] = round(first_push_pct, 2)
        analysis['against_move_first3_pct'] = round(against_move_pct, 2)
        analysis['continuation_seen'] = continuation_seen
        analysis['immediate_invalidation'] = bool(bars <= 2 or (against_move_pct > max(first_push_pct * 1.2, 0.12) and first_push_pct < 0.12))
        analysis['weak_followthrough'] = bool(abs(float(analysis['mfe_pct'])) < 0.35)
        analysis['no_post_entry_expansion'] = bool(abs(float(analysis['mfe_pct'])) < 0.20)
        analysis['multiple_failed_pushes'] = bool(bars >= 5 and bullish_count >= 2 and against_count >= 2 and abs(float(analysis['mfe_pct'])) < 0.45)

        vol_mean = float(vols.mean()) if len(vols) else 0.0
        vol_tail = float(vols.tail(min(3, len(vols))).mean()) if len(vols) else 0.0
        analysis['post_entry_volume_faded'] = bool(vol_mean > 0 and vol_tail < vol_mean * 0.85)

        ema20 = closes.ewm(span=20, adjust=False).mean()
        ema_last = float(ema20.iloc[-1]) if len(ema20) else 0.0
        two_last = post.tail(min(3, bars))
        if side == 'LONG':
            structure_against = bool(((two_last['close'] < two_last['open']).sum() >= 2) or (float(closes.iloc[-1]) < ema_last))
        else:
            structure_against = bool(((two_last['close'] > two_last['open']).sum() >= 2) or (float(closes.iloc[-1]) > ema_last))
        analysis['structure_against_trade'] = structure_against
        analysis['structure_5m'] = ('bearish against LONG' if side == 'LONG' else 'bullish against SHORT') if structure_against else 'neutral / no clean continuation'

        tp = (post['high'].astype(float) + post['low'].astype(float) + post['close'].astype(float)) / 3.0
        vol_sum = float(vols.sum())
        vwap = float((tp * vols).sum() / max(vol_sum, 1e-9)) if vol_sum > 0 else None
        last_close = float(closes.iloc[-1])
        if vwap is not None:
            if side == 'LONG':
                analysis['vwap_after_entry'] = 'потерян' if last_close < vwap else 'сохранён'
            else:
                analysis['vwap_after_entry'] = 'потерян' if last_close > vwap else 'сохранён'

        analysis['follow_through'] = 'есть' if abs(float(analysis['mfe_pct'])) >= 0.55 else ('слабый' if abs(float(analysis['mfe_pct'])) >= 0.20 else 'нет')
        analysis['range_trap_behavior'] = bool(bars >= 4 and abs(float(analysis['mfe_pct'])) < 0.30 and abs(float(analysis['mae_pct'])) >= 0.25)
        analysis['sl_hit_without_meaningful_excursion'] = bool(abs(float(analysis['mfe_pct'])) < 0.12)
        analysis['zone_reaction_too_weak'] = bool(first_push_pct < 0.12)
        analysis['confirm_too_weak'] = bool(first_push_pct < 0.15 and bars <= 4)
        analysis['retest_too_deep'] = bool(against_move_pct > max(abs(first_push_pct) * 1.1, 0.12))
        analysis['late_entry_inside_expansion'] = bool(bars <= 3 and abs(float(analysis['mae_pct'])) > abs(float(analysis['mfe_pct'])) and abs(float(analysis['mfe_pct'])) < 0.18)
        analysis['origin_not_held'] = bool(route_key == 'smc_displacement_origin' and structure_against)
        analysis['displacement_too_weak'] = bool(route_key == 'smc_displacement_origin' and first_push_pct < 0.18)
        analysis['stacked_fvg_not_held'] = bool(route_key == 'smc_dual_fvg_origin' and structure_against)
        analysis['confluence_failed'] = bool(route_key == 'smc_dual_fvg_origin' and (analysis['zone_reaction_too_weak'] or analysis['retest_too_deep']))
        analysis['reclaim_not_confirmed'] = bool(route_key == 'smc_liquidity_reclaim' and not analysis.get('tp1_threatened') and analysis.get('reclaim_lost_back'))
        analysis['htf_zone_not_held'] = bool(route_key == 'smc_htf_ob_ltf_fvg' and analysis.get('retest_too_deep'))

        try:
            bos_level = _loss_diag_parse_float(snapshot.get('bos_level')) if snapshot else None
            reclaim_level = _loss_diag_parse_float(snapshot.get('reclaim_level')) if snapshot else None
            origin_level = _loss_diag_parse_float(snapshot.get('origin_level')) if snapshot else None
            ob_top = _loss_diag_parse_float(snapshot.get('ob_top')) if snapshot else None
            ob_bottom = _loss_diag_parse_float(snapshot.get('ob_bottom')) if snapshot else None
            fvg_top = _loss_diag_parse_float(snapshot.get('fvg_top')) if snapshot else None
            fvg_bottom = _loss_diag_parse_float(snapshot.get('fvg_bottom')) if snapshot else None
            if bos_level:
                if side == 'LONG':
                    analysis['level_reclaimed_back'] = bool(analysis.get('level_reclaimed_back') or float(closes.iloc[-1]) < bos_level)
                else:
                    analysis['level_reclaimed_back'] = bool(analysis.get('level_reclaimed_back') or float(closes.iloc[-1]) > bos_level)
            if reclaim_level:
                if side == 'LONG':
                    analysis['reclaim_lost_back'] = bool(analysis.get('reclaim_lost_back') or float(closes.iloc[-1]) < reclaim_level)
                else:
                    analysis['reclaim_lost_back'] = bool(analysis.get('reclaim_lost_back') or float(closes.iloc[-1]) > reclaim_level)
            if origin_level and route_key == 'smc_displacement_origin':
                if side == 'LONG':
                    analysis['origin_not_held'] = bool(analysis.get('origin_not_held') or float(closes.iloc[-1]) < origin_level)
                else:
                    analysis['origin_not_held'] = bool(analysis.get('origin_not_held') or float(closes.iloc[-1]) > origin_level)
            zone_top = max([x for x in (ob_top, ob_bottom, fvg_top, fvg_bottom) if x is not None], default=None)
            zone_bottom = min([x for x in (ob_top, ob_bottom, fvg_top, fvg_bottom) if x is not None], default=None)
            if zone_top is not None and zone_bottom is not None and zone_top > zone_bottom:
                zone_width = zone_top - zone_bottom
                zone_mid = zone_bottom + zone_width * 0.5
                if side == 'LONG':
                    deepest = float(lows.min())
                    analysis['entry_too_deep_into_zone'] = bool(analysis.get('entry_too_deep_into_zone') or deepest < (zone_bottom + zone_width * 0.25))
                    analysis['zone_midpoint_lost'] = bool(float(closes.iloc[-1]) < zone_mid)
                else:
                    highest = float(highs.max())
                    analysis['entry_too_deep_into_zone'] = bool(analysis.get('entry_too_deep_into_zone') or highest > (zone_top - zone_width * 0.25))
                    analysis['zone_midpoint_lost'] = bool(float(closes.iloc[-1]) > zone_mid)
        except Exception:
            pass

        tp1_hit_at = _parse_iso_dt(src.get('tp1_hit_at'))
        if tp1_hit_at is not None and tp1 > 0:
            analysis['bars_to_tp1'] = max(1, int(math.ceil((tp1_hit_at - opened).total_seconds() / 300.0))) if tp1_hit_at > opened else 1
            analysis['bars_tp1_to_close'] = max(1, int(math.ceil((closed - tp1_hit_at).total_seconds() / 300.0))) if closed > tp1_hit_at else 1
            after_tp = post[post['_ts'] >= tp1_hit_at].copy()
            if not after_tp.empty:
                ah = after_tp['high'].astype(float)
                al = after_tp['low'].astype(float)
                if side == 'LONG':
                    analysis['new_high_after_tp1'] = bool(float(ah.max()) > tp1)
                else:
                    analysis['new_high_after_tp1'] = bool(float(al.min()) < tp1)
            analysis['follow_through_tp1'] = 'есть' if analysis.get('new_high_after_tp1') else 'нет'
            analysis['vwap_after_tp1'] = analysis.get('vwap_after_entry') or 'ослаб'
    except Exception:
        return analysis
    return analysis

def _build_loss_diagnostics_from_row(row: dict | None, *, final_status: str, closed_at: dt.datetime | None = None, close_analysis: dict | None = None) -> dict:
    src = dict(row or {})
    st = str(final_status or '').upper().strip()
    pre_status = str(src.get('status') or 'ACTIVE').upper().strip()
    after_tp1 = bool(src.get('tp1_hit')) or pre_status == 'TP1'
    fallback_reason = _report_close_reason(st, after_tp1=after_tp1, has_tp2=bool(float(src.get('tp2') or 0.0) > 0 if src.get('tp2') is not None else False))
    metrics = _loss_diag_parse_metrics(src)
    analysis = dict(close_analysis or {}) or _loss_diag_load_close_analysis(src)
    forensic = _loss_diag_build_forensic_from_analysis(src, analysis, after_tp1=after_tp1, st=st)

    if st not in ('LOSS', 'CLOSED'):
        return {
            'reason_code': '', 'reason_text': fallback_reason,
            'primary_reason_code': '', 'primary_reason_text': fallback_reason,
            'scenario_code': '', 'scenario_text': '',
            'secondary_reason_codes': [], 'secondary_reason_labels': [],
            'missed_codes': [], 'missed_labels': [],
            'what_happened_lines': [],
            'weak_filter_keys': [], 'weak_filter_labels': [],
            'improve_keys': [], 'improve_labels': [], 'improve_note': '',
            'analysis_lines': _loss_diag_build_analysis_lines(src, metrics, analysis, after_tp1=after_tp1),
            'metric_lines': [], 'metrics_weak_counts': dict(metrics.get('metrics_weak_counts') or {}),
            'close_analysis_json': analysis,
        }

    metric_lines = _loss_diag_metric_lines(src, metrics, [])
    exact_route = _loss_diag_is_exact_smc_route(src)
    improve_keys = []
    for item in metric_lines:
        if 'RR TP2' in item:
            improve_keys.append('raise_min_rr')
        elif 'Signal strength' in item or 'TA score' in item:
            improve_keys.append('raise_min_ta_score')
        elif 'MACD hist' in item or 'ADX' in item or 'VWAP' in item:
            improve_keys.append('tighten_trend_alignment')
        elif 'Vol xAvg' in item:
            improve_keys.append('tighten_volume_support')
    if analysis.get('late_entry_inside_expansion') or analysis.get('late_entry_after_exhausted_move') or 'late_entry_inside_expansion' in list(forensic.get('missed_codes') or []):
        improve_keys.append('avoid_late_entry')
    if analysis.get('entry_into_overhead_supply') or analysis.get('long_below_strong_high'):
        improve_keys.append('avoid_overhead_supply')
        improve_keys.append('require_clean_space_to_tp')
    if analysis.get('short_above_weak_low'):
        improve_keys.append('avoid_support_short')
        improve_keys.append('require_clean_space_to_tp')
    if analysis.get('retest_too_deep'):
        improve_keys.append('tighten_retest_depth')
    if analysis.get('confirm_too_weak'):
        improve_keys.append('raise_min_confirm_strength')
    improve_keys = list(dict.fromkeys(improve_keys))

    reason_code = str(forensic.get('primary_reason_code') or '').strip()
    reason_text = str(forensic.get('primary_reason_text') or '').strip() or fallback_reason
    if st == 'CLOSED' and not reason_code:
        reason_code = 'timeout_closed'
        reason_text = fallback_reason

    return {
        'reason_code': reason_code,
        'reason_text': reason_text,
        'primary_reason_code': reason_code,
        'primary_reason_text': reason_text,
        'scenario_code': str(forensic.get('scenario_code') or '').strip(),
        'scenario_text': str(forensic.get('scenario_text') or '').strip(),
        'secondary_reason_codes': list(forensic.get('secondary_reason_codes') or []),
        'secondary_reason_labels': list(forensic.get('secondary_reason_texts') or []),
        'missed_codes': list(forensic.get('missed_codes') or []),
        'missed_labels': list(forensic.get('missed_texts') or []),
        'what_happened_lines': list(forensic.get('what_happened_lines') or []),
        'entry_snapshot_present': bool(analysis.get('entry_snapshot_present')),
        'weak_filter_keys': [],
        'weak_filter_labels': [],
        'improve_keys': improve_keys,
        'improve_labels': [_loss_diag_improve_label(x) for x in improve_keys],
        'improve_note': '; '.join([_loss_diag_improve_label(x) for x in improve_keys]),
        'analysis_lines': _loss_diag_build_analysis_lines(src, metrics, analysis, after_tp1=after_tp1),
        'metric_lines': metric_lines,
        'metrics_weak_counts': dict(metrics.get('metrics_weak_counts') or {}),
        'close_analysis_json': analysis,
    }

def _report_setup_label_human(source: str | None) -> str:
    raw = str(source or "").strip().lower().replace("-", " ").replace("_", " ")
    raw = re.sub(r"\s+", " ", raw).strip()
    labels = {
        "origin": "origin",
        "origin fast": "origin",
        "origin fastpass": "origin",
        "начало движения": "origin",
        "breakout": "breakout",
        "breakout fast": "breakout",
        "breakout fastpass": "breakout",
        "пробой": "breakout",
        "zone": "zone retest",
        "zone retest": "zone retest",
        "zone touch": "zone retest",
        "zone touch retest": "zone retest",
        "возврат в зону": "zone retest",
        "normal pending": "normal pending trigger",
        "normal pending trigger": "normal pending trigger",
        "pending": "normal pending trigger",
        "pending trigger": "normal pending trigger",
        "обычный trigger": "normal pending trigger",
        "обычный триггер": "normal pending trigger",
        "обычный pending trigger": "normal pending trigger",
        "liquidity reclaim": "liquidity_reclaim",
        "liquidity reclaim emit": "liquidity_reclaim",
        "liquidity reclaim entry": "liquidity_reclaim",
        "liquidity reclaim ready": "liquidity_reclaim",
        "liquidity_reclaim": "liquidity_reclaim",
        "liquidity_reclaim_emit": "liquidity_reclaim",
    }
    if raw in labels:
        return labels[raw]
    return str(source or "").strip()

def _signal_ui_setup_label(sig) -> str:
    try:
        if sig is None:
            return ""
        label = str(getattr(sig, "ui_setup_label", "") or "").strip()
        if label:
            return _report_setup_label_human(label)

        setup_source = str(getattr(sig, "setup_source", "") or "").strip()
        setup_route = str(getattr(sig, "smc_setup_route", "") or getattr(sig, "emit_route", "") or "").strip()
        smc_label = str(getattr(sig, "smc_setup_label", "") or "").strip()
        if setup_source and (setup_route or smc_label):
            try:
                from backend import _mid_compose_setup_label
                composed = _mid_compose_setup_label(setup_source, setup_route, smc_label)
            except Exception:
                composed = ""
            if composed:
                return _report_setup_label_human(composed)

        emit_route = str(getattr(sig, "emit_route", "") or "").strip()
        if emit_route == "liquidity_reclaim_emit":
            return "liquidity_reclaim_emit"
        label = str(getattr(sig, "smc_setup_label", "") or "").strip()
        if label:
            if setup_source:
                try:
                    from backend import _mid_compose_setup_label
                    composed = _mid_compose_setup_label(setup_source, "", label)
                except Exception:
                    composed = ""
                if composed:
                    return _report_setup_label_human(composed)
            return _report_setup_label_human(label)
        label = str(getattr(sig, "setup_source_label", "") or "").strip()
        if label:
            return _report_setup_label_human(label)
        raw = str(getattr(sig, "setup_source", "") or "").strip()
        if raw:
            return _report_setup_label_human(raw)
    except Exception:
        pass
    return ""


def _row_ui_setup_label(row: dict | None) -> str:
    try:
        src = row if isinstance(row, dict) else {}
        label = _report_setup_label_human(src.get("ui_setup_label"))
        if label:
            return label

        setup_source = str(src.get("setup_source") or src.get("smart_emit_source") or "").strip()
        setup_route = str(src.get("smc_setup_route") or src.get("emit_route") or "").strip()
        smc_label = str(src.get("smc_setup_label") or "").strip()
        if setup_source and (setup_route or smc_label):
            try:
                from backend import _mid_compose_setup_label
                composed = _mid_compose_setup_label(setup_source, setup_route, smc_label)
            except Exception:
                composed = ""
            if composed:
                return _report_setup_label_human(composed)

        emit_route = str(src.get("emit_route") or "").strip()
        if emit_route == "liquidity_reclaim_emit":
            return "liquidity_reclaim_emit"
        if smc_label:
            if setup_source:
                try:
                    from backend import _mid_compose_setup_label
                    composed = _mid_compose_setup_label(setup_source, "", smc_label)
                except Exception:
                    composed = ""
                if composed:
                    return _report_setup_label_human(composed)
            return _report_setup_label_human(smc_label)
        label = _report_setup_label_human(src.get("setup_source_label"))
        if label:
            return label
        label = _report_setup_label_human(src.get("setup_source"))
        if label:
            return label
    except Exception:
        pass
    return ""


def _report_setup_label_from_signal(sig) -> str:
    try:
        return _signal_ui_setup_label(sig)
    except Exception:
        return ""
    return ""


def _report_extract_setup_label_from_text(text: str) -> str:
    try:
        src = str(text or "")
        if not src:
            return ""
        m = re.search(r"(?:^|\n)\s*(?:🧭\s*)?Smart-setup:\s*([^\n\r]+)", src, flags=re.IGNORECASE)
        if not m:
            return ""
        return _report_setup_label_human(str(m.group(1) or "").strip())
    except Exception:
        return ""


def _report_setup_label_from_row(row: dict) -> str:
    try:
        label = _row_ui_setup_label(row)
        if label:
            return label

        sid = 0
        try:
            sid = int(row.get("signal_id") or 0)
        except Exception:
            sid = 0

        if sid > 0:
            try:
                sig = SIGNALS.get(sid)
            except Exception:
                sig = None
            label = _report_setup_label_from_signal(sig)
            if label:
                return label

            try:
                cached_text = str(ORIGINAL_SIGNAL_TEXT.get((0, sid)) or "")
            except Exception:
                cached_text = ""
            label = _report_extract_setup_label_from_text(cached_text)
            if label:
                return label

        for candidate in (row.get("risk_note"), row.get("orig_text")):
            label = _report_extract_setup_label_from_text(str(candidate or ""))
            if label:
                return label
    except Exception:
        pass
    return ""


def _build_closed_signal_report_card(t: dict, *, final_status: str, pnl_total_pct: float, closed_at: dt.datetime | None = None) -> str:
    row = dict(t or {})
    st = str(final_status or "CLOSED").upper().strip() or "CLOSED"
    symbol = str(row.get("symbol") or "").upper() or "—"
    market = str(row.get("market") or "SPOT").upper()
    side = str(row.get("side") or "LONG").upper()
    entry = float(row.get("entry") or 0.0)
    sl = float(row.get("sl") or 0.0) if row.get("sl") is not None else 0.0
    tp1 = float(row.get("tp1") or 0.0) if row.get("tp1") is not None else 0.0
    tp2 = float(row.get("tp2") or 0.0) if row.get("tp2") is not None else 0.0
    be_price = float(row.get("be_price") or 0.0) if row.get("be_price") is not None else 0.0
    pre_status = str(row.get("status") or "ACTIVE").upper()
    after_tp1 = bool(row.get("tp1_hit")) or pre_status == "TP1"
    if after_tp1 and be_price <= 0 and entry > 0:
        be_price = entry
    has_tp2 = bool(tp2 > 0 and (tp1 <= 0 or abs(tp2 - tp1) > 1e-12))
    rr = _report_rr_str(entry, sl, tp1, tp2)
    reason = _report_close_reason(st, after_tp1=after_tp1, has_tp2=has_tp2)
    opened_time = _fmt_dt_msk(row.get("opened_at"))
    closed_dt = closed_at or dt.datetime.now(dt.timezone.utc)
    closed_time = _fmt_dt_msk(closed_dt)
    tz_label = _report_tz_label()

    setup_label = _report_setup_label_from_row(row)
    loss_diag = _build_loss_diagnostics_from_row(row, final_status=st, closed_at=closed_at)

    price_lines = []
    if entry > 0:
        price_lines.append(f"💰 Вход: {entry:.6f}")
    if sl > 0:
        price_lines.append(f"🛑 SL: {sl:.6f}")
    if tp1 > 0:
        price_lines.append(f"🎯 TP1: {tp1:.6f}")
    if has_tp2:
        price_lines.append(f"🚀 TP2: {tp2:.6f}")
    if after_tp1 and be_price > 0:
        price_lines.append(f"🛡 BE: {be_price:.6f}")
    price_block = "\n".join(price_lines).strip() or "—"

    setup_block = f"🧭 Smart-setup: {setup_label}\n" if setup_label else ""
    primary_block = ""
    scenario_block = ""
    analysis_block = ""
    happened_block = ""
    secondary_block = ""
    missed_block = ""
    improve_block = ""
    if st == 'LOSS':
        analysis_lines = list(loss_diag.get('analysis_lines') or [])
        happened_lines = list(loss_diag.get('what_happened_lines') or [])
        secondary_labels = list(loss_diag.get('secondary_reason_labels') or [])
        missed_labels = list(loss_diag.get('missed_labels') or [])
        improve_labels = list(loss_diag.get('improve_labels') or [])
        primary_text = str(loss_diag.get('primary_reason_text') or loss_diag.get('reason_text') or reason).strip()
        scenario_text = str(loss_diag.get('scenario_text') or '').strip()
        if primary_text:
            primary_block = "🧠 Главная причина:\n" + primary_text + "\n\n"
        if scenario_text:
            scenario_block = "🎭 Сценарий потери:\n" + scenario_text + "\n\n"
        if analysis_lines:
            analysis_block = "📉 Candle-анализ:\n" + "\n".join([f"• {x}" for x in analysis_lines if x]) + "\n\n"
        if happened_lines:
            happened_block = "📉 Что произошло после входа:\n" + "\n".join([f"• {x}" for x in happened_lines if x]) + "\n\n"
        if secondary_labels:
            secondary_block = "🧩 Доп. причины:\n" + "\n".join([f"• {x}" for x in secondary_labels if x]) + "\n\n"
        if missed_labels:
            missed_block = "⚠️ Что бот пропустил:\n" + "\n".join([f"• {x}" for x in missed_labels if x]) + "\n\n"
        if improve_labels:
            improve_block = "🛠 Что улучшить:\n" + "\n".join([f"• {x}" for x in improve_labels if x]) + "\n\n"

    fallback_primary_block = primary_block or (
        "🧠 Причина:\n" + (loss_diag.get('reason_text') or reason) + "\n\n"
    )

    return (
        f"{_report_close_emoji(st)} {symbol} | {market} | {side}\n\n"
        f"📌 Статус: {_report_close_status(st, after_tp1=after_tp1)}\n"
        f"📊 Итог PnL: {_report_pnl_pct(pnl_total_pct)}\n"
        f"📊 Risk/Reward: 1 : {rr}\n"
        f"{setup_block}"
        f"{fallback_primary_block}"
        f"{scenario_block}"
        f"{analysis_block}"
        f"{happened_block}"
        f"{secondary_block}"
        f"{missed_block}"
        f"{improve_block}"
        f"──────────────\n\n"
        f"{price_block}\n\n"
        f"🕒 Открыта: {opened_time} ({tz_label})\n"
        f"🕒 Закрыта: {closed_time} ({tz_label})\n\n"
        f"{_symbol_hashtag(symbol)}"
    ).strip()



async def _send_closed_signal_report_card(t: dict, *, final_status: str, pnl_total_pct: float, closed_at: dt.datetime | None = None) -> None:
    if _report_bot is None:
        return
    chat_ids = [int(x) for x in (REPORT_BOT_CHAT_IDS or []) if int(x)]
    if not chat_ids:
        return
    try:
        text = _build_closed_signal_report_card(t, final_status=str(final_status or "CLOSED"), pnl_total_pct=float(pnl_total_pct or 0.0), closed_at=closed_at)
    except Exception:
        logger.exception("[report-bot] failed to build closed signal card")
        return
    for chat_id in chat_ids:
        try:
            await _report_bot.send_message(int(chat_id), text)
        except TelegramForbiddenError:
            logger.warning("[report-bot] bot is blocked by chat_id=%s", chat_id)
        except TelegramBadRequest as e:
            logger.warning("[report-bot] send failed chat_id=%s err=%s", chat_id, e)
        except Exception:
            logger.exception("[report-bot] send failed chat_id=%s", chat_id)



# ---------------- Daily report bot summary ----------------

_DAILY_SIGNAL_REPORT_ENABLED = str(os.getenv("DAILY_SIGNAL_REPORT_ENABLED", "1") or "1").strip().lower() not in ("0", "false", "no", "off")
_DAILY_SIGNAL_REPORT_HOUR = max(0, min(23, int(float(os.getenv("DAILY_SIGNAL_REPORT_HOUR", "0") or 0))))
_DAILY_SIGNAL_REPORT_MINUTE = max(0, min(59, int(float(os.getenv("DAILY_SIGNAL_REPORT_MINUTE", "0") or 0))))
_DAILY_SIGNAL_REPORT_STATE_KEY = "daily_signal_report_state"
_MID_ADAPTIVE_V3_PATH = Path(os.getenv("MID_ADAPTIVE_V3_PATH", str(Path(__file__).with_name("mid_adaptive_v3.json"))))


def _adaptive_v3_slug(value: str | None) -> str:
    raw = str(value or '').strip().lower()
    if not raw:
        return ''
    raw = raw.replace('→', ' to ').replace('|', ' ')
    raw = re.sub(r"[^a-z0-9_\-\s]+", ' ', raw)
    raw = re.sub(r"[\s\-]+", '_', raw).strip('_')
    return raw


def _adaptive_v3_load() -> dict:
    try:
        if _MID_ADAPTIVE_V3_PATH.exists():
            data = json.loads(_MID_ADAPTIVE_V3_PATH.read_text(encoding='utf-8'))
            if isinstance(data, dict):
                data.setdefault('markets', {})
                data.setdefault('setups', {})
                data.setdefault('setup_side', {})
                return data
    except Exception:
        pass
    return {'markets': {}, 'setups': {}, 'setup_side': {}}


def _adaptive_v3_save(data: dict) -> None:
    try:
        _MID_ADAPTIVE_V3_PATH.parent.mkdir(parents=True, exist_ok=True)
        _MID_ADAPTIVE_V3_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        logger.exception('[adaptive-v3] save failed')


def _adaptive_v3_bucket(parent: dict, key: str) -> dict:
    b = parent.get(key)
    if not isinstance(b, dict):
        b = {'resolved': 0, 'win': 0, 'loss': 0, 'be': 0, 'closed': 0, 'pnl_sum': 0.0, 'updated_at': ''}
        parent[key] = b
    return b


def _adaptive_v3_setup_key_from_row(row: dict) -> str:
    src = dict(row or {})
    for cand in (
        src.get('emit_route'),
        src.get('smc_setup_route'),
        src.get('ui_setup_label'),
        src.get('smc_setup_label'),
        _report_setup_label_from_row(src),
        src.get('setup_source'),
        src.get('setup_source_label'),
    ):
        slug = _adaptive_v3_slug(cand)
        if slug:
            return slug
    return 'unknown'


async def _adaptive_v3_update_from_outcome(row: dict, *, final_status: str, pnl_total_pct: float = 0.0, closed_at: dt.datetime | None = None) -> None:
    try:
        status = str(final_status or '').upper().strip()
        if status not in {'WIN', 'LOSS', 'BE', 'CLOSED'}:
            return
        market = str((row or {}).get('market') or 'SPOT').upper().strip()
        side = str((row or {}).get('side') or 'LONG').upper().strip()
        if market not in {'SPOT', 'FUTURES'} or side not in {'LONG', 'SHORT'}:
            return
        setup_key = _adaptive_v3_setup_key_from_row(row)
        now_iso = (closed_at or dt.datetime.now(dt.timezone.utc)).isoformat()
        data = _adaptive_v3_load()
        market_b = _adaptive_v3_bucket(data.setdefault('markets', {}).setdefault(market, {}), side)
        setup_b = _adaptive_v3_bucket(data.setdefault('setups', {}), setup_key)
        setup_side_parent = data.setdefault('setup_side', {}).setdefault(setup_key, {})
        setup_side_b = _adaptive_v3_bucket(setup_side_parent, side)
        for bucket in (market_b, setup_b, setup_side_b):
            bucket['resolved'] = int(bucket.get('resolved') or 0) + 1
            if status == 'WIN':
                bucket['win'] = int(bucket.get('win') or 0) + 1
            elif status == 'LOSS':
                bucket['loss'] = int(bucket.get('loss') or 0) + 1
            elif status == 'BE':
                bucket['be'] = int(bucket.get('be') or 0) + 1
            elif status == 'CLOSED':
                bucket['closed'] = int(bucket.get('closed') or 0) + 1
            bucket['pnl_sum'] = float(bucket.get('pnl_sum') or 0.0) + float(pnl_total_pct or 0.0)
            bucket['updated_at'] = now_iso
        data['updated_at'] = now_iso
        _adaptive_v3_save(data)
    except Exception:
        logger.exception('[adaptive-v3] update failed')



def _daily_report_setup_title(key: str) -> str:
    raw = str(key or "").strip().lower()
    mapping = {
        "origin": "origin",
        "breakout": "breakout",
        "zone_retest": "zone_retest",
        "normal_pending_trigger": "normal_pending_trigger",
        "liquidity_reclaim": "liquidity_reclaim",
    }
    return mapping.get(raw, raw or "other")


def _daily_report_bucket_line(title: str, bucket: dict, *, show_sent: bool = True) -> str:
    b = dict(bucket or {})
    sent = int(b.get("sent") or 0)
    win = int(b.get("win") or 0)
    loss = int(b.get("loss") or 0)
    be = int(b.get("be") or 0)
    manual_close = int(b.get("manual_close") or 0)
    pnl = float(b.get("sum_pnl_pct") or 0.0)
    parts = []
    if show_sent:
        parts.append(f"sent {sent}")
    parts.append(f"+ {win}")
    parts.append(f"- {loss}")
    if be:
        parts.append(f"BE {be}")
    if manual_close:
        parts.append(f"CLOSED {manual_close}")
    parts.append(f"PnL {_report_pnl_pct(pnl)}")
    return f"• {title}: " + " | ".join(parts)



def _daily_report_int(value) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _daily_report_float(value, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _daily_report_pct(value: float | int | None, *, digits: int = 1, strip_zero: bool = False) -> str:
    try:
        num = float(value or 0.0)
    except Exception:
        num = 0.0
    text = f"{num:.{int(digits)}f}"
    if strip_zero and "." in text:
        text = text.rstrip("0").rstrip(".")
    return f"{text}%"


def _daily_report_winrate(bucket: dict, *, overall: bool = False) -> float:
    """Return daily-report winrate as WIN / (WIN + LOSS).

    BE and manual CLOSED are informational outcomes and should not dilute
    winrate. This keeps the metric consistent across the summary, markets,
    sides and setup blocks.
    """
    win = _daily_report_int(bucket.get("win"))
    loss = _daily_report_int(bucket.get("loss"))
    denom = win + loss
    return 0.0 if denom <= 0 else (100.0 * win / denom)


def _daily_report_avg(values) -> float:
    vals = [float(x) for x in list(values or [])]
    return statistics.fmean(vals) if vals else 0.0


def _daily_report_quality(bucket: dict) -> int:
    sent = _daily_report_int(bucket.get("sent"))
    if sent <= 0:
        return 0
    conf = _daily_report_avg(bucket.get("conf_values"))
    rr = _daily_report_avg(bucket.get("rr_values"))
    conf_cnt = _daily_report_avg(bucket.get("confirm_values"))
    win = _daily_report_int(bucket.get("win"))
    loss = _daily_report_int(bucket.get("loss"))
    be = _daily_report_int(bucket.get("be"))
    closed = _daily_report_int(bucket.get("manual_close"))
    resolved = win + loss + be + closed
    success = (win + 0.5 * be + 0.25 * closed) / resolved * 100.0 if resolved > 0 else 50.0
    rr_score = min(max(rr, 0.0), 3.0) / 3.0 * 100.0
    conf_score = conf
    confirm_score = min(max(conf_cnt, 0.0), 4.0) / 4.0 * 100.0
    weak = dict(bucket.get("weak_counter") or {})
    penalty = (
        4.0 * (_daily_report_int(weak.get("low_confidence")) + _daily_report_int(weak.get("few_confirmations"))) +
        5.0 * (_daily_report_int(weak.get("low_rr")) + _daily_report_int(weak.get("weak_rr"))) +
        6.0 * (_daily_report_int(weak.get("weak_retest_confirmation")) + _daily_report_int(weak.get("breakout_retest_weak"))) +
        5.0 * (_daily_report_int(weak.get("weak_trend_context")) + _daily_report_int(weak.get("low_ta_score")))
    )
    raw = (0.38 * conf_score) + (0.24 * rr_score) + (0.22 * success) + (0.16 * confirm_score) - (penalty / max(1, sent))
    return max(35, min(99, int(round(raw))))


def _daily_report_setup_has_data(bucket: dict) -> bool:
    b = dict(bucket or {})
    return (_daily_report_int(b.get("sent")) > 0) or ((_daily_report_int(b.get("win")) + _daily_report_int(b.get("loss")) + _daily_report_int(b.get("be")) + _daily_report_int(b.get("manual_close"))) > 0)


def _daily_report_render_quality(bucket: dict) -> str:
    return f"{_daily_report_quality(bucket)}/100" if _daily_report_setup_has_data(bucket) else "—"


def _daily_report_weak_label(key: str) -> str:
    mapping = {
        'low_rr': 'RR filter',
        'zone_hold_weak': 'Retest filter',
        'breakout_retest_weak': 'Breakout quality',
        'macd_momentum_weak': 'MACD momentum filter',
        'weak_trend_context': 'Trend / ADX filter',
        'vwap_side_weak': 'Trend / ADX filter',
        'low_ta_score': 'Trend / ADX filter',
        'weak_signal_strength': 'Trend / ADX filter',
        'weak_volume_support': 'Volume filter',
        'tp1_protection': 'TP1 protection',
        'trigger_reconfirm_weak': 'Pending trigger quality',
        'reclaim_confirmation_weak': 'Liquidity reclaim filter',
    }
    s = str(key or '').strip()
    return mapping.get(s, _loss_diag_weak_filter_label(s))


def _daily_report_improve_label(key: str) -> str:
    return _loss_diag_improve_label(key)


def _daily_report_reason_label(code: str, text_value: str = '') -> str:
    raw = str(code or '').strip()
    if raw:
        mapping = {
            'loss_before_tp1_sl': 'stop_before_tp1',
            'loss_after_tp1_sl': 'stop_after_tp1',
            'loss_sl': 'stop_loss',
            'timeout_closed': 'timeout_closed',
            'zone_retest_failed': 'retest-зона не удержалась',
            'fast_invalidation_after_entry': 'быстрая инвалидация после входа',
            'tp1_no_followthrough': 'после TP1 не было продолжения',
            'tp1_protection_failed': 'после TP1 защита не удержала остаток',
            'structure_break_against_entry': 'структура сломалась против входа',
            'no_followthrough_after_entry': 'после входа не было continuation',
            'false_breakout_after_entry': 'ложный breakout после входа',
            'vwap_lost_after_entry': 'после входа потерян VWAP',
            'ob_fvg_zone_lost': 'OB+FVG зона не удержалась',
            'ob_fvg_structure_failed': 'OB+FVG сломался по структуре',
            'ob_fvg_no_followthrough': 'OB+FVG не дал continuation',
            'htf_ob_ltf_retest_failed': 'HTF OB + LTF FVG retest не удержался',
            'htf_ob_ltf_fast_invalidation': 'HTF OB + LTF FVG быстро инвалидирован',
            'htf_ob_ltf_no_followthrough': 'HTF OB + LTF FVG не дал continuation',
            'bos_retest_failed': 'BOS retest confirm сломался',
            'bos_retest_no_followthrough': 'BOS retest confirm не дал continuation',
            'displacement_origin_fast_fail': 'displacement origin быстро сломался',
            'displacement_origin_no_followthrough': 'displacement origin не дал continuation',
            'dual_fvg_origin_fast_fail': 'dual/stacked FVG origin быстро сломался',
            'dual_fvg_origin_no_followthrough': 'dual/stacked FVG origin не дал continuation',
            'liquidity_reclaim_not_held': 'liquidity reclaim не удержался',
            'liquidity_reclaim_fast_fail': 'liquidity reclaim быстро сломался',
            'liquidity_reclaim_no_followthrough': 'liquidity reclaim не дал continuation',
            'tp1_ob_fvg_no_followthrough': 'после TP1 OB+FVG не дал продолжения',
            'tp1_htf_ob_ltf_no_followthrough': 'после TP1 HTF OB + LTF FVG не дал продолжения',
            'tp1_bos_retest_no_followthrough': 'после TP1 BOS retest не дал продолжения',
            'tp1_displacement_origin_no_followthrough': 'после TP1 displacement origin не дал продолжения',
            'tp1_dual_fvg_origin_no_followthrough': 'после TP1 dual/stacked FVG origin не дал продолжения',
            'tp1_liquidity_reclaim_no_followthrough': 'после TP1 liquidity reclaim не дал продолжения',
        }
        return mapping.get(raw, raw)
    txt = str(text_value or '').strip()
    return txt or 'unknown_loss_reason'


def _daily_report_reason_codes(row: dict) -> list[str]:
    raw = str((row or {}).get('close_reason_code') or '').strip()
    out = [x.strip() for x in re.split(r'[,;|]+', raw) if x.strip()]
    if out:
        return [_daily_report_reason_label(out[0])]
    txt = str((row or {}).get('close_reason_text') or '').strip()
    return [txt] if txt else []


def _daily_report_improve_zone(key: str) -> str:
    s = str(key or "").strip().lower()
    mapping = {
        "low_confidence": "confidence filter",
        "raise_min_confidence": "confidence filter",
        "few_confirmations": "confirmation count",
        "require_more_confirmations": "confirmation count",
        "low_rr": "RR filter",
        "weak_rr": "RR filter",
        "raise_min_rr": "RR filter",
        "weak_trend_context": "trend strength filter",
        "low_ta_score": "trend strength filter",
        "weak_signal_strength": "trend strength filter",
        "raise_min_ta_score": "trend strength filter",
        "weak_retest_confirmation": "retest validation",
        "breakout_retest_weak": "retest validation",
        "tighten_breakout_retest": "retest validation",
        "tighten_zone_retest": "retest validation",
        "trigger_reconfirm_weak": "trigger filter",
        "tighten_trigger_reconfirm": "trigger filter",
        "reclaim_confirmation_weak": "liquidity reclaim filter",
        "tighten_liquidity_reclaim": "liquidity reclaim filter",
        "false_breakout_after_entry": "breakout filter",
        "stop_before_tp1": "entry protection",
        "stop_after_tp1": "TP1 protection",
    }
    return mapping.get(s, s or "execution filter")


def _daily_report_bucket_template() -> dict:
    return {
        "sent": 0,
        "win": 0,
        "loss": 0,
        "be": 0,
        "manual_close": 0,
        "sum_pnl_pct": 0.0,
        "conf_values": [],
        "rr_values": [],
        "confirm_values": [],
        "weak_counter": {},
        "improve_counter": {},
    }


def _daily_report_inc(counter: dict, key: str) -> None:
    k = str(key or "").strip()
    if not k:
        return
    counter[k] = _daily_report_int(counter.get(k)) + 1


def _daily_report_split_multi(value: str | None) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    items = [x.strip() for x in re.split(r"[;,|]+", raw) if x.strip()]
    out: list[str] = []
    for item in items:
        if item not in out:
            out.append(item)
    return out


def _daily_report_setup_key_from_row(row: dict) -> str:
    return str(_report_setup_label_from_row(row) or "").strip().lower().replace(" ", "_")


def _daily_report_row_confirmations(row: dict) -> int:
    return _loss_diag_count_confirmations(row.get("confirmations") or row.get("source_exchange"))


def _daily_report_rr_from_row(row: dict) -> float:
    rr = _daily_report_float(row.get("rr"))
    if rr > 0:
        return rr
    try:
        entry = float(row.get("entry") or 0.0)
        sl = float(row.get("sl") or 0.0)
        tp1 = float(row.get("tp1") or 0.0)
        tp2 = float(row.get("tp2") or 0.0)
        if entry <= 0 or sl <= 0:
            return 0.0
        risk = abs(entry - sl)
        if risk <= 0:
            return 0.0
        target = tp2 if tp2 > 0 and abs(tp2 - tp1) > 1e-12 else tp1
        if target <= 0:
            return 0.0
        reward = abs(target - entry)
        if reward <= 0:
            return 0.0
        return reward / risk
    except Exception:
        return 0.0


def _daily_report_enrich_row(row: dict) -> dict:
    src = dict(row or {})
    status = str(src.get("status") or "").upper().strip()
    if status in {"LOSS", "CLOSED", "WIN", "BE"}:
        try:
            diag = _build_loss_diagnostics_from_row(src, final_status=status)
        except Exception:
            diag = {}
        if diag:
            if not str(src.get("close_reason_code") or "").strip():
                src["close_reason_code"] = str(diag.get("reason_code") or "")
            if not str(src.get("close_reason_text") or "").strip():
                src["close_reason_text"] = str(diag.get("reason_text") or "")
            if not str(src.get("weak_filters") or "").strip():
                src["weak_filters"] = ",".join(list(diag.get("weak_filter_keys") or []))
            if not str(src.get("improve_note") or "").strip():
                src["improve_note"] = "; ".join(list(diag.get("improve_keys") or []))
    return src


def _daily_report_add_sent(bucket: dict, row: dict) -> None:
    bucket["sent"] = _daily_report_int(bucket.get("sent")) + 1
    conf = _daily_report_int(row.get("confidence"))
    rr = _daily_report_rr_from_row(row)
    if conf > 0:
        bucket["conf_values"].append(conf)
    if rr > 0:
        bucket["rr_values"].append(rr)
    bucket["confirm_values"].append(_daily_report_row_confirmations(row))
    for item in _daily_report_split_multi(row.get("weak_filters")):
        _daily_report_inc(bucket["weak_counter"], item)
    for item in _daily_report_split_multi(row.get("improve_note")):
        _daily_report_inc(bucket["improve_counter"], item)


def _daily_report_add_closed(bucket: dict, row: dict) -> None:
    st = str(row.get("status") or "").upper().strip()
    if st == "WIN":
        bucket["win"] = _daily_report_int(bucket.get("win")) + 1
    elif st == "LOSS":
        bucket["loss"] = _daily_report_int(bucket.get("loss")) + 1
    elif st == "BE":
        bucket["be"] = _daily_report_int(bucket.get("be")) + 1
    elif st == "CLOSED":
        bucket["manual_close"] = _daily_report_int(bucket.get("manual_close")) + 1
    bucket["sum_pnl_pct"] = _daily_report_float(bucket.get("sum_pnl_pct")) + _daily_report_float(row.get("pnl_total_pct"))
    for item in _daily_report_split_multi(row.get("weak_filters")):
        _daily_report_inc(bucket["weak_counter"], item)
    for item in _daily_report_split_multi(row.get("improve_note")):
        _daily_report_inc(bucket["improve_counter"], item)


def _daily_report_sort_counter(counter: dict, limit: int) -> list[tuple[str, int]]:
    items = [(str(k), _daily_report_int(v)) for k, v in dict(counter or {}).items() if str(k).strip()]
    items.sort(key=lambda kv: (-kv[1], kv[0]))
    return items[: max(0, int(limit))]


def _daily_report_best_setup_reasons(setup_key: str) -> list[str]:
    mapping = {
        "zone_retest": [
            "максимальный winrate",
            "чистые входы после retest",
            "лучший баланс риск/доходность",
            "меньше ложных входов",
        ],
        "origin": [
            "лучше ловил начало движения",
            "сильнее держал импульс после входа",
            "давал понятную структуру входа",
            "стабильнее вёл позицию к целям",
        ],
        "breakout": [
            "лучше ловил продолжение импульса",
            "давал сильный PnL вклад",
            "лучше работал при подтвержденном пробое",
            "реже разворачивался после входа",
        ],
        "normal_pending_trigger": [
            "лучше активировал вход после триггера",
            "сохранял понятный риск-профиль",
            "реже давал шум после активации",
            "лучше работал с подтверждениями",
        ],
        "liquidity_reclaim": [
            "лучше отрабатывал возврат ликвидности",
            "лучше фильтровал reclaim-входы",
            "реже давал хаотичные возвраты",
            "давал стабильное подтверждение перед входом",
        ],
    }
    return list(mapping.get(str(setup_key or "").strip(), ["лучший баланс сигналов", "более стабильная логика входа", "лучше держал направление", "дал лучший вклад в день"]))


def _daily_report_worst_setup_reasons(setup_key: str) -> list[str]:
    mapping = {
        "zone_retest": [
            "слабое подтверждение retest перед входом",
            "хуже удерживал зону после активации",
            "чаще давал возврат в stop",
        ],
        "origin": [
            "входы оказывались слишком ранними",
            "хуже подтверждался импульс после входа",
            "чаще ломался контекст движения",
        ],
        "breakout": [
            "слабее подтверждался breakout перед входом",
            "чаще появлялись ложные пробои",
            "хуже удерживал продолжение импульса",
        ],
        "normal_pending_trigger": [
            "мало подтверждений перед входом",
            "высокий риск слабого продолжения",
            "хуже держит направление после активации",
        ],
        "liquidity_reclaim": [
            "недостаточно чистый reclaim перед входом",
            "слабее подтверждался возврат ликвидности",
            "логика входа требовала больше фильтрации",
        ],
    }
    return list(mapping.get(str(setup_key or "").strip(), ["чаще давал слабое продолжение", "хуже держал направление после входа", "требует усиления логики фильтрации"]))


def _daily_report_filter_scores(*, overall_bucket: dict, setups: dict, all_loss_rows: list[dict]) -> dict:
    overall_quality = _daily_report_quality(overall_bucket)
    avg_conf = _daily_report_avg(overall_bucket.get("conf_values"))
    avg_rr = _daily_report_avg(overall_bucket.get("rr_values"))
    weak_total: dict[str, int] = {}
    for row in all_loss_rows:
        for item in _daily_report_split_multi(row.get("weak_filters")):
            _daily_report_inc(weak_total, item)
    loss_count = max(1, len(all_loss_rows))
    penalty_unit = 8.0 / loss_count

    def penalized(base: float | None, *keys: str) -> int | None:
        if base is None:
            return None
        penalty = 0.0
        for key in keys:
            penalty += penalty_unit * _daily_report_int(weak_total.get(key))
        return max(35, min(99, int(round(base - penalty))))

    def setup_base(key: str) -> float | None:
        bucket = setups.get(key) or _daily_report_bucket_template()
        if not _daily_report_setup_has_data(bucket):
            return None
        return 0.55 * _daily_report_quality(bucket) + 0.45 * overall_quality

    confidence_base = 0.72 * avg_conf + 0.28 * overall_quality if avg_conf > 0 else None
    rr_base = 0.60 * (min(max(avg_rr, 0.0), 3.0) / 3.0 * 100.0) + 0.40 * overall_quality if avg_rr > 0 else None
    trend_base = 0.70 * overall_quality + 0.30 * _daily_report_winrate(overall_bucket, overall=False)
    retest_base = setup_base("zone_retest")
    breakout_base = setup_base("breakout")
    reclaim_base = setup_base("liquidity_reclaim")
    trigger_base = setup_base("normal_pending_trigger")
    macd_base = 0.65 * overall_quality + 0.35 * _daily_report_winrate(overall_bucket, overall=False)
    vol_base = 0.62 * overall_quality + 0.38 * _daily_report_winrate(overall_bucket, overall=False)
    tp1_base = 0.58 * overall_quality + 0.42 * _daily_report_winrate(overall_bucket, overall=False)

    return {
        "confidence": penalized(confidence_base, "low_confidence"),
        "rr": penalized(rr_base, "low_rr", "weak_rr"),
        "trend": penalized(trend_base, "weak_trend_context", "low_ta_score", "weak_signal_strength", "vwap_side_weak"),
        "retest": penalized(retest_base, "weak_retest_confirmation", "breakout_retest_weak", "zone_hold_weak"),
        "breakout": penalized(breakout_base, "false_breakout_after_entry", "breakout_retest_weak", "low_confidence"),
        "reclaim": penalized(reclaim_base, "reclaim_confirmation_weak"),
        "trigger": penalized(trigger_base, "trigger_reconfirm_weak", "few_confirmations"),
        "macd": penalized(macd_base, "macd_momentum_weak"),
        "volume": penalized(vol_base, "weak_volume_support"),
        "tp1": penalized(tp1_base, "tp1_protection"),
    }

def _daily_report_pick_best_setup(setups: dict) -> str:
    best_key = ""
    best_score = None
    for key, bucket in dict(setups or {}).items():
        resolved = _daily_report_int(bucket.get("win")) + _daily_report_int(bucket.get("loss")) + _daily_report_int(bucket.get("be")) + _daily_report_int(bucket.get("manual_close"))
        if resolved <= 0:
            continue
        score = (
            _daily_report_quality(bucket),
            round(_daily_report_winrate(bucket), 4),
            _daily_report_float(bucket.get("sum_pnl_pct")),
            _daily_report_int(bucket.get("sent")),
        )
        if best_score is None or score > best_score:
            best_key = str(key)
            best_score = score
    return best_key


def _daily_report_pick_worst_setup(setups: dict) -> str:
    worst_key = ""
    worst_score = None
    for key, bucket in dict(setups or {}).items():
        resolved = _daily_report_int(bucket.get("win")) + _daily_report_int(bucket.get("loss")) + _daily_report_int(bucket.get("be")) + _daily_report_int(bucket.get("manual_close"))
        if resolved <= 0:
            continue
        score = (
            _daily_report_int(bucket.get("loss")) == 0,
            _daily_report_quality(bucket),
            round(_daily_report_winrate(bucket), 4),
            _daily_report_float(bucket.get("sum_pnl_pct")),
            -_daily_report_int(bucket.get("sent")),
        )
        if worst_score is None or score < worst_score:
            worst_key = str(key)
            worst_score = score
    return worst_key


def _daily_report_render_loss_example(idx: int, row: dict) -> list[str]:
    symbol = str(row.get("symbol") or "—").upper()
    market = str(row.get("market") or "—").upper()
    side = str(row.get("side") or "—").upper()
    pnl = _report_pnl_pct(_daily_report_float(row.get("pnl_total_pct")))
    setup = str(_daily_report_setup_key_from_row(row) or "unknown")
    reason = str(row.get("close_reason_text") or "").strip() or _daily_report_reason_label(row.get("close_reason_code"))
    weak_filters = ", ".join([_daily_report_weak_label(x) for x in _daily_report_split_multi(row.get("weak_filters"))]) or "—"
    improve_items = [_daily_report_improve_label(x) for x in _daily_report_split_multi(row.get("improve_note"))]
    improve_text = "; ".join([x for x in improve_items if x]).strip() or "усилить фильтрацию входа"
    return [
        f"{idx}. {symbol} | {market} | {side}",
        f"   PnL: {pnl}",
        f"   Setup: {setup}",
        f"   Причина: {reason}",
        f"   Слабые фильтры: {weak_filters}",
        f"   Что улучшить: {improve_text}",
    ]


def _daily_report_final_score(*, overall_quality: int, entry_accuracy: float, overall_winrate: float, pnl_pct: float) -> float:
    pnl_score = min(100.0, max(0.0, 50.0 + pnl_pct * 8.0))
    score = (0.50 * overall_quality) + (0.20 * entry_accuracy) + (0.15 * overall_winrate) + (0.15 * pnl_score)
    return round(max(1.0, min(10.0, score / 10.0)), 1)


async def _build_daily_signal_report_text(*, since: dt.datetime, until: dt.datetime) -> str:
    dataset = await db_store.signal_report_window_dataset(since=since, until=until)
    sent_rows = list((dataset or {}).get("sent_rows") or [])
    closed_rows = list((dataset or {}).get("closed_rows") or [])

    overall = _daily_report_bucket_template()
    markets = {"SPOT": _daily_report_bucket_template(), "FUTURES": _daily_report_bucket_template()}
    sides = {"LONG": _daily_report_bucket_template(), "SHORT": _daily_report_bucket_template()}
    setups = {
        "origin": _daily_report_bucket_template(),
        "breakout": _daily_report_bucket_template(),
        "zone_retest": _daily_report_bucket_template(),
        "normal_pending_trigger": _daily_report_bucket_template(),
        "liquidity_reclaim": _daily_report_bucket_template(),
    }
    smart_routes: dict[str, dict] = {}

    sent_by_id: dict[int, dict] = {}
    for row in sent_rows:
        row = dict(row)
        signal_id = _daily_report_int(row.get("signal_id"))
        if signal_id:
            sent_by_id[signal_id] = row
        market = str(row.get("market") or "").upper().strip()
        side = str(row.get("side") or "").upper().strip()
        setup = _daily_report_setup_key_from_row(row)
        _daily_report_add_sent(overall, row)
        if market in markets:
            _daily_report_add_sent(markets[market], row)
        if side in sides:
            _daily_report_add_sent(sides[side], row)
        if setup in setups:
            _daily_report_add_sent(setups[setup], row)
        route_key = _adaptive_v3_setup_key_from_row(row)
        _daily_report_add_sent(smart_routes.setdefault(route_key, _daily_report_bucket_template()), row)

    all_loss_rows: list[dict] = []
    for row in closed_rows:
        row = dict(row)
        signal_id = _daily_report_int(row.get("signal_id"))
        if signal_id and signal_id in sent_by_id:
            merged = dict(sent_by_id[signal_id])
            merged.update({k: v for k, v in row.items() if v is not None and v != ""})
            row = merged
        row = _daily_report_enrich_row(row)
        market = str(row.get("market") or "").upper().strip()
        side = str(row.get("side") or "").upper().strip()
        setup = _daily_report_setup_key_from_row(row)
        _daily_report_add_closed(overall, row)
        if market in markets:
            _daily_report_add_closed(markets[market], row)
        if side in sides:
            _daily_report_add_closed(sides[side], row)
        if setup in setups:
            _daily_report_add_closed(setups[setup], row)
        route_key = _adaptive_v3_setup_key_from_row(row)
        _daily_report_add_closed(smart_routes.setdefault(route_key, _daily_report_bucket_template()), row)
        if str(row.get("status") or "").upper().strip() == "LOSS":
            all_loss_rows.append(row)

    overall_pnl = _daily_report_float(overall.get("sum_pnl_pct"))
    overall_winrate = _daily_report_winrate(overall, overall=True)
    avg_rr = _daily_report_avg(overall.get("rr_values"))
    denom = _daily_report_int(overall.get("win")) + _daily_report_int(overall.get("loss")) + _daily_report_int(overall.get("be"))
    entry_accuracy = (100.0 * (_daily_report_int(overall.get("win")) + 0.5 * _daily_report_int(overall.get("be"))) / denom) if denom > 0 else 0.0
    overall_quality = _daily_report_quality(overall)

    best_setup_key = _daily_report_pick_best_setup(setups)
    worst_setup_key = _daily_report_pick_worst_setup(setups)
    best_setup_bucket = setups.get(best_setup_key) or _daily_report_bucket_template()
    worst_setup_bucket = setups.get(worst_setup_key) or _daily_report_bucket_template()

    filter_scores = _daily_report_filter_scores(overall_bucket=overall, setups=setups, all_loss_rows=all_loss_rows)
    valid_filter_values = [float(v) for v in list(filter_scores.values()) if v is not None]
    if valid_filter_values:
        overall_quality = int(round(sum(valid_filter_values) / len(valid_filter_values)))

    reason_counter = {}
    weak_counter = {}
    improve_counter = {}
    zone_counter = {}
    metric_counter = {}
    for row in all_loss_rows:
        for reason_item in _daily_report_reason_codes(row):
            _daily_report_inc(reason_counter, reason_item)
        try:
            _diag_row = _build_loss_diagnostics_from_row(row, final_status="LOSS")
        except Exception:
            _diag_row = {}
        for mk, mv in dict((_diag_row or {}).get("metrics_weak_counts") or {}).items():
            metric_counter[mk] = _daily_report_int(metric_counter.get(mk)) + _daily_report_int(mv)
        for item in _daily_report_split_multi(row.get("weak_filters")):
            _daily_report_inc(weak_counter, item)
            _daily_report_inc(zone_counter, _daily_report_improve_zone(item))
        for item in _daily_report_split_multi(row.get("improve_note")):
            _daily_report_inc(improve_counter, item)
            _daily_report_inc(zone_counter, _daily_report_improve_zone(item))

    top_reasons = _daily_report_sort_counter(reason_counter, 6)
    top_weak = _daily_report_sort_counter(weak_counter, 7)
    top_zones = _daily_report_sort_counter(zone_counter, 7)
    top_metric_weak = _daily_report_sort_counter(metric_counter, 7)
    loss_examples = sorted(all_loss_rows, key=lambda r: (_daily_report_float(r.get("pnl_total_pct")), str(r.get("closed_at") or "")))[:3]

    market_best = ""
    market_best_score = None
    for key, bucket in markets.items():
        if _daily_report_int(bucket.get("sent")) <= 0:
            continue
        score = (_daily_report_quality(bucket), _daily_report_winrate(bucket), _daily_report_float(bucket.get("sum_pnl_pct")))
        if market_best_score is None or score > market_best_score:
            market_best = key
            market_best_score = score

    side_best = ""
    side_best_score = None
    for key, bucket in sides.items():
        if _daily_report_int(bucket.get("sent")) <= 0:
            continue
        score = (_daily_report_quality(bucket), _daily_report_winrate(bucket), _daily_report_float(bucket.get("sum_pnl_pct")))
        if side_best_score is None or score > side_best_score:
            side_best = key
            side_best_score = score

    weakest_items = [(k, v) for k, v in dict(filter_scores or {}).items() if v is not None]
    weakest_filter_key = min(weakest_items, key=lambda kv: kv[1])[0] if weakest_items else "retest"
    weakest_filter_label = {
        "confidence": "confidence логика",
        "rr": "RR логика",
        "trend": "trend логика",
        "retest": "retest логика",
        "breakout": "breakout логика",
        "reclaim": "liquidity reclaim логика",
        "trigger": "trigger логика",
        "macd": "MACD momentum filter",
        "volume": "Volume support filter",
        "tp1": "TP1 protection quality",
    }.get(weakest_filter_key, weakest_filter_key)

    focus_lines = []
    if side_best:
        focus_lines.append(f"сильные {side_best} по тренду")
    if best_setup_key:
        focus_lines.append(f"{best_setup_key}-сценарии")
    focus_lines.append("сигналы с confidence выше среднего")

    caution_lines = []
    if worst_setup_key:
        caution_lines.append("breakout LONG" if worst_setup_key == "breakout" else worst_setup_key)
    if weakest_filter_key == "trigger":
        caution_lines.append("слабые pending trigger")
    elif weakest_filter_key == "retest":
        caution_lines.append("входы после неубедительного retest")
    else:
        caution_lines.append(f"слабые {weakest_filter_key} сигналы")
    if not any("retest" in item for item in caution_lines):
        caution_lines.append("входы после неубедительного retest")

    final_score = _daily_report_final_score(
        overall_quality=overall_quality,
        entry_accuracy=entry_accuracy,
        overall_winrate=overall_winrate,
        pnl_pct=overall_pnl,
    )

    since_local = since.astimezone(TZ)
    sep = "━━━━━━━━━━━━━━"
    lines: list[str] = [
        f"Отчёт бота за {since_local.strftime('%d.%m.%Y')}",
        "",
        sep,
        "👑 Общая картина дня",
        "",
        f"📤 Отправлено сигналов: {_daily_report_int(overall.get('sent'))}",
        f"✅ WIN: {_daily_report_int(overall.get('win'))}",
        f"❌ LOSS: {_daily_report_int(overall.get('loss'))}",
        f"🟡 BE: {_daily_report_int(overall.get('be'))}",
        f"⚪ CLOSED: {_daily_report_int(overall.get('manual_close'))}",
        "",
        f"📈 Winrate: {_daily_report_pct(overall_winrate, digits=1)}",
        f"💰 Общий PnL: {_report_pnl_pct(overall_pnl)}",
        f"⚖️ Средний RR: {f'1:{avg_rr:.2f}' if avg_rr > 0 else '1:0.00'}",
        f"🎯 Точность входов: {_daily_report_pct(entry_accuracy, digits=0, strip_zero=True)}",
        f"🛡 Качество фильтров: {overall_quality}/100",
        "",
        sep,
        "📌 Рынки",
        "",
    ]

    for market_key, icon in (("SPOT", "🟢"), ("FUTURES", "🔴")):
        bucket = markets.get(market_key) or _daily_report_bucket_template()
        lines.append(f"{icon} {market_key}")
        lines.append(f"Сигналов: {_daily_report_int(bucket.get('sent'))}")
        lines.append(f"✅ WIN: {_daily_report_int(bucket.get('win'))}")
        lines.append(f"❌ LOSS: {_daily_report_int(bucket.get('loss'))}")
        if _daily_report_int(bucket.get('be')):
            lines.append(f"🟡 BE: {_daily_report_int(bucket.get('be'))}")
        if _daily_report_int(bucket.get('manual_close')):
            lines.append(f"⚪ CLOSED: {_daily_report_int(bucket.get('manual_close'))}")
        lines.append(f"💰 PnL: {_report_pnl_pct(_daily_report_float(bucket.get('sum_pnl_pct')))}")
        lines.append(f"📈 Winrate: {_daily_report_pct(_daily_report_winrate(bucket), digits=1)}")
        lines.append("")

    lines.extend([sep, "📈 Направления", ""])
    for side_key, icon in (("LONG", "🟢"), ("SHORT", "🔴")):
        bucket = sides.get(side_key) or _daily_report_bucket_template()
        lines.append(f"{icon} {side_key}")
        lines.append(f"Всего: {_daily_report_int(bucket.get('sent'))}")
        lines.append(f"✅ WIN: {_daily_report_int(bucket.get('win'))}")
        lines.append(f"❌ LOSS: {_daily_report_int(bucket.get('loss'))}")
        if _daily_report_int(bucket.get('be')):
            lines.append(f"🟡 BE: {_daily_report_int(bucket.get('be'))}")
        if _daily_report_int(bucket.get('manual_close')):
            lines.append(f"⚪ CLOSED: {_daily_report_int(bucket.get('manual_close'))}")
        lines.append(f"💰 PnL: {_report_pnl_pct(_daily_report_float(bucket.get('sum_pnl_pct')))}")
        lines.append(f"📈 Winrate: {_daily_report_pct(_daily_report_winrate(bucket), digits=1)}")
        lines.append("")

    lines.extend([sep, "🧠 Setup-анализ", ""])
    for setup_key in ("origin", "breakout", "zone_retest", "normal_pending_trigger", "liquidity_reclaim"):
        bucket = setups.get(setup_key) or _daily_report_bucket_template()
        lines.append(setup_key)
        lines.append(f"Всего: {_daily_report_int(bucket.get('sent'))}")
        lines.append(f"✅ WIN: {_daily_report_int(bucket.get('win'))}")
        lines.append(f"❌ LOSS: {_daily_report_int(bucket.get('loss'))}")
        no_result = _daily_report_int(bucket.get('be')) + _daily_report_int(bucket.get('manual_close'))
        if no_result:
            lines.append(f"🟡/⚪ Без результата: {no_result}")
        if _daily_report_int(bucket.get('win')) + _daily_report_int(bucket.get('loss')) > 0:
            lines.append(f"📈 Winrate: {_daily_report_pct(_daily_report_winrate(bucket), digits=1, strip_zero=True)}")
        lines.append(f"🛡 Качество: {_daily_report_render_quality(bucket)}")
        lines.append("")

    exact_route_specs = [
        ("smc_liquidity_reclaim", "Liquidity reclaim | liquidity sweep → reclaim → BOS continuation"),
        ("smc_ob_fvg_overlap", "Origin | OB+FVG priority emit"),
        ("smc_htf_ob_ltf_fvg", "Zone retest | HTF OB + LTF FVG retest emit"),
        ("smc_bos_retest_confirm", "Breakout | BOS → FVG/OB retest → confirm"),
        ("smc_displacement_origin", "Origin | displacement origin fast-path"),
        ("smc_dual_fvg_origin", "Origin | dual/stacked FVG origin"),
    ]

    lines.extend([sep, "🧭 Точные smart-setup route", ""])
    exact_route_has_data = False
    smart_routes_map = dict(smart_routes or {})
    for route_key, route_title in exact_route_specs:
        bucket = smart_routes_map.get(route_key) or _daily_report_bucket_template()
        sent_n = _daily_report_int(bucket.get('sent'))
        if sent_n <= 0:
            continue
        exact_route_has_data = True
        resolved_n = _daily_report_int(bucket.get('win')) + _daily_report_int(bucket.get('loss'))
        wr = _daily_report_winrate(bucket)
        pnl_sum = _daily_report_float(bucket.get('sum_pnl_pct'))
        be_closed = _daily_report_int(bucket.get('be')) + _daily_report_int(bucket.get('manual_close'))

        lines.append(route_title)
        lines.append(f"Route key: {route_key}")
        lines.append(f"Всего: {sent_n}")
        lines.append(f"✅ WIN: {_daily_report_int(bucket.get('win'))}")
        lines.append(f"❌ LOSS: {_daily_report_int(bucket.get('loss'))}")
        lines.append(f"🟡/⚪️ Без результата: {be_closed}")
        lines.append(f"📈 Winrate: {_daily_report_pct(wr, digits=1)}" if resolved_n > 0 else "📈 Winrate: —")
        lines.append(f"💰 PnL: {_report_pnl_pct(pnl_sum)}")
        lines.append("")

    if not exact_route_has_data:
        lines.extend(["—", ""])

    lines.extend([sep, "🏆 Лучший сетап дня", "", f"🥇 {best_setup_key or '—'}", "Почему лучший:"])
    for item in _daily_report_best_setup_reasons(best_setup_key)[:4]:
        lines.append(f"• {item}")
    lines.extend(["", "Итог:"])
    if best_setup_key:
        lines.append(f"📈 Winrate: {_daily_report_pct(_daily_report_winrate(best_setup_bucket), digits=1, strip_zero=True)}")
    lines.extend(["💰 Лучший вклад в PnL дня", "🛡 Самый стабильный setup", "", sep, "⚠️ Худший сетап дня", "", f"🥀 {worst_setup_key or '—'}", "Почему слабый:"])
    for item in _daily_report_worst_setup_reasons(worst_setup_key)[:3]:
        lines.append(f"• {item}")
    lines.extend(["", "Итог:"])
    if worst_setup_key:
        lines.append(f"📈 Winrate: {_daily_report_pct(_daily_report_winrate(worst_setup_bucket), digits=1, strip_zero=True)}")
    lines.append("❌ чаще давал слабое продолжение")
    lines.append("🛠 требует усиления логики trigger-фильтра" if worst_setup_key == "normal_pending_trigger" else "🛠 требует доработки логики фильтра")
    lines.extend(["", sep, "🔎 Почему были убытки", "", "Топ причин LOSS:"])
    if top_reasons:
        for key, value in top_reasons:
            lines.append(f"• {key} — {value}")
    else:
        lines.append("• —")
    lines.extend(["", "Какие фильтры чаще всего пропускали минус:"])
    if top_weak:
        for key, value in top_weak:
            lines.append(f"• {_daily_report_weak_label(key)} — {value}")
    else:
        lines.append("• —")
    lines.extend(["", "Какие цифры чаще всего были слабыми:"])
    metric_label_map = {
        'rr_tp2': 'RR TP2 < 1.90',
        'vol_xavg': 'Vol xAvg < 1.10',
        'macd_hist_5m': 'MACD hist(5m) ≤ +0.0002',
        'adx_30m': 'ADX 30m < 24',
        'adx_1h': 'ADX 1h < 26',
        'signal_strength': 'Signal strength < 6.0',
        'ta_score': 'TA score < 97',
    }
    if top_metric_weak:
        for key, value in top_metric_weak:
            times = 'раз' if int(value) == 1 else 'раза'
            lines.append(f"• {metric_label_map.get(key, key)} — {value} {times}")
    else:
        lines.append("• —")
    lines.extend(["", "Главные зоны улучшения:"])
    if top_zones:
        for key, _ in top_zones:
            lines.append(f"• {_daily_report_improve_label(key)}")
    else:
        lines.append("• —")

    lines.extend([
        "",
        sep,
        "🛡 Оценка фильтров в %",
        "",
        f"Confidence filter: {str(filter_scores.get('confidence')) + '%' if filter_scores.get('confidence') is not None else '—'}",
        f"RR filter: {str(filter_scores.get('rr')) + '%' if filter_scores.get('rr') is not None else '—'}",
        f"Trend filter: {str(filter_scores.get('trend')) + '%' if filter_scores.get('trend') is not None else '—'}",
        f"Retest filter: {str(filter_scores.get('retest')) + '%' if filter_scores.get('retest') is not None else '—'}",
        f"Breakout filter: {str(filter_scores.get('breakout')) + '%' if filter_scores.get('breakout') is not None else '—'}",
        f"Liquidity reclaim filter: {str(filter_scores.get('reclaim')) + '%' if filter_scores.get('reclaim') is not None else '—'}",
        f"Pending trigger quality: {str(filter_scores.get('trigger')) + '%' if filter_scores.get('trigger') is not None else '—'}",
        f"MACD momentum filter: {str(filter_scores.get('macd')) + '%' if filter_scores.get('macd') is not None else '—'}",
        f"Volume support filter: {str(filter_scores.get('volume')) + '%' if filter_scores.get('volume') is not None else '—'}",
        f"TP1 protection quality: {str(filter_scores.get('tp1')) + '%' if filter_scores.get('tp1') is not None else '—'}",
        "",
        "Вывод:",
        f"самая слабая часть сейчас — {'retest логика для LONG и слабый post-entry follow-through' if weakest_filter_key == 'retest' else weakest_filter_label}",
        f"самая сильная часть — {'SHORT по тренду и liquidity reclaim' if side_best == 'SHORT' and best_setup_key == 'liquidity_reclaim' else ((best_setup_key or 'лучшая логика') + (' и ' + side_best + ' по тренду' if side_best else ''))}",
        "",
        sep,
        "🤖 Что улучшить боту на завтра",
        "",
    ])

    improve_lines = [
        '1. Поднять minimum RR TP2 для SPOT до 1.90',
        '2. Требовать MACD hist(5m) > +0.0002 для LONG',
        '3. Требовать Vol xAvg ≥ 1.10 для zone retest и breakout',
        '4. Не брать LONG при ADX 30m/1h ниже 24 / 26',
        '5. Усилить TP1 → BE/SL защиту остатка',
        '6. Для zone retest требовать удержание зоны после входа минимум 1–2 свечи',
        '7. Для breakout требовать повторное подтверждение уровня после активации',
    ]
    lines.extend(improve_lines[:7])

    lines.extend(["", sep, "📅 Рекомендация на следующий день", "", "Фокус бота:"])
    focus_lines = ['сильные SHORT по тренду', 'liquidity reclaim и сильный breakout', 'сигналы с хорошим объёмом и подтверждённым импульсом']
    for item in focus_lines[:3]:
        lines.append(f"• {item}")
    lines.extend(["", "Осторожность:"])
    caution_lines = ['zone_retest LONG', 'слабый объём на входе', 'входы без continuation после активации', 'сделки с RR ниже 1.90 в SPOT']
    for item in caution_lines[:4]:
        lines.append(f"• {item}")

    lines.extend(["", sep, "🏁 Финальный вывод", "", f"День закрыт в {'плюс' if overall_pnl >= 0 else 'минус'}: {_report_pnl_pct(overall_pnl)}", "", "Лучше всего отработали:"])
    if market_best:
        lines.append(f"• {market_best}")
    if side_best:
        lines.append(f"• {side_best}")
    if best_setup_key:
        lines.append(f"• {best_setup_key}")
    lines.append('• breakout с подтверждённым объёмом')
    lines.extend(["", "Требуют доработки:"])
    if worst_setup_key:
        lines.append(f"• {worst_setup_key} LONG" if worst_setup_key == 'zone_retest' else f"• {worst_setup_key}")
    lines.append('• RR filter в SPOT')
    lines.append('• MACD momentum filter')
    lines.append('• TP1 protection')
    lines.append('• retest quality')
    lines.extend(["", "Итоговая оценка дня бота:", f"⭐ {final_score} / 10"])
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines)

async def _report_bot_send_long(chat_id: int, text: str) -> None:
    if _report_bot is None:
        return
    payload = _sanitize_template_text(int(chat_id), text, ctx='daily_report')
    if not (payload or '').strip():
        return
    try:
        await _report_bot.send_message(int(chat_id), payload)
        return
    except TelegramBadRequest as e:
        msg = str(e).lower()
        if 'message is too long' not in msg:
            raise

    max_len = 3800
    parts: list[str] = []
    cur = ''
    for line in payload.splitlines(True):
        if len(cur) + len(line) > max_len and cur:
            parts.append(cur)
            cur = ''
        cur += line
    if cur:
        parts.append(cur)

    for part in parts or [payload[:max_len]]:
        await _report_bot.send_message(int(chat_id), part)


async def _send_daily_signal_report(*, since: dt.datetime, until: dt.datetime) -> None:
    if _report_bot is None:
        return
    chat_ids = [int(x) for x in (REPORT_BOT_CHAT_IDS or []) if int(x)]
    if not chat_ids:
        return
    text = await _build_daily_signal_report_text(since=since, until=until)
    for chat_id in chat_ids:
        try:
            await _report_bot_send_long(int(chat_id), text)
        except TelegramForbiddenError:
            logger.warning("[report-bot] daily report bot is blocked by chat_id=%s", chat_id)
        except TelegramBadRequest as e:
            logger.warning("[report-bot] daily report send failed chat_id=%s err=%s", chat_id, e)
        except Exception:
            logger.exception("[report-bot] daily report send failed chat_id=%s", chat_id)


async def daily_signal_report_loop() -> None:
    if not _DAILY_SIGNAL_REPORT_ENABLED:
        logger.info("[report-bot] daily signal report disabled")
        return
    if _report_bot is None:
        logger.info("[report-bot] daily signal report skipped: REPORT_BOT_TOKEN is not set")
        return
    if not REPORT_BOT_CHAT_IDS:
        logger.info("[report-bot] daily signal report skipped: REPORT_BOT_CHAT_IDS is empty")
        return

    logger.info("[report-bot] daily signal report loop started at %02d:%02d %s", _DAILY_SIGNAL_REPORT_HOUR, _DAILY_SIGNAL_REPORT_MINUTE, TZ_NAME)
    await asyncio.sleep(5)
    while True:
        try:
            now_utc = dt.datetime.now(dt.timezone.utc)
            now_local = now_utc.astimezone(TZ)
            target_local = now_local.replace(hour=_DAILY_SIGNAL_REPORT_HOUR, minute=_DAILY_SIGNAL_REPORT_MINUTE, second=0, microsecond=0)
            report_key = target_local.date().isoformat()
            st = await db_store.kv_get_json(_DAILY_SIGNAL_REPORT_STATE_KEY) or {}
            last_sent = str(st.get("last_sent_for") or "").strip()
            if now_local >= target_local and last_sent != report_key:
                since_local = target_local - dt.timedelta(days=1)
                until_local = target_local
                since_utc = since_local.astimezone(dt.timezone.utc)
                until_utc = until_local.astimezone(dt.timezone.utc)
                await _send_daily_signal_report(since=since_utc, until=until_utc)
                await db_store.kv_set_json(_DAILY_SIGNAL_REPORT_STATE_KEY, {
                    "last_sent_for": report_key,
                    "sent_at": now_utc.isoformat(),
                    "tz": TZ_NAME,
                })
                logger.info("[report-bot] daily signal report sent for %s", report_key)
        except Exception:
            logger.exception("[report-bot] daily signal report loop error")
        await asyncio.sleep(30)

def _fmt_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if "/" in s:
        a, b = s.split("/", 1)
        return f"{a.strip()} / {b.strip()}"
    return s

def _symbol_hashtag(sym: str) -> str:
    raw = (sym or "").strip().upper()
    if not raw:
        return "#—"
    tag = re.sub(r"[^A-Z0-9_]+", "", raw.replace("/", ""))
    return f"#{tag}" if tag else f"#{raw.replace(' ', '')}"

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


def _effective_signal_targets(sig: Signal) -> tuple[float, float]:
    """Normalize TP display/use for single-target signals.

    If TP2 is missing, zero, or effectively equal to TP1, treat the signal as
    single-target: TP1 stays as final target and TP2 is hidden/ignored.
    """
    try:
        tp1 = float(getattr(sig, "tp1", 0.0) or 0.0)
    except Exception:
        tp1 = 0.0
    try:
        tp2 = float(getattr(sig, "tp2", 0.0) or 0.0)
    except Exception:
        tp2 = 0.0

    eff_tp1 = tp1 if tp1 > 0 else 0.0
    eff_tp2 = tp2 if (tp2 > 0 and (eff_tp1 <= 0 or abs(tp2 - eff_tp1) > 1e-12)) else 0.0
    return eff_tp1, eff_tp2


def _normalize_too_late_reason(sig: Signal, reason: str) -> str:
    """Prevent single-target signals from being reported as TP2-hit."""
    rs = str(reason or "").upper().strip()
    eff_tp1, eff_tp2 = _effective_signal_targets(sig)
    if rs == "TP2" and eff_tp2 <= 0 and eff_tp1 > 0:
        return "TP1"
    return rs or "ERROR"


async def _expire_signal_message(call: types.CallbackQuery, sig: Signal, reason: str, price: float) -> None:
    """Remove the OPEN button and mark the signal as expired in-place."""
    uid = call.from_user.id
    reason_key = _too_late_reason_key(_normalize_too_late_reason(sig, reason))
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

    eff_tp1, eff_tp2 = _effective_signal_targets(sig)
    reason_key = _too_late_reason_key(_normalize_too_late_reason(sig, reason))

    return tr(uid, "sig_too_late_body").format(
        market_emoji=mkt_emoji,
        market=mkt,
        symbol=sym,
        reason=tr(uid, reason_key),
        price=_fmt_price(price),
        tp1=_fmt_price(eff_tp1 if eff_tp1 > 0 else None),
        tp2=_fmt_price(eff_tp2 if eff_tp2 > 0 else None),
        sl=_fmt_price(getattr(sig, "sl", None)),
        time=hhmm,
    )


def _price_unavailable_notice(uid: int, sig: 'Signal') -> str:
    """Human-friendly transient warning shown when we can't fetch current price.
    IMPORTANT: this is NOT the same as 'signal expired' (TP/SL/time).
    """
    mkt = (getattr(sig, "market", None) or "FUTURES").upper()
    if mkt == "SPOT":
        ex = "Binance/Bybit/OKX/Gate.io/MEXC"
    else:
        ex = "Binance/Bybit/OKX"
    # Prefer i18n if keys exist, otherwise fall back to RU text requested by user.
    try:
        if "sig_price_unavailable" in I18N.get(get_lang(uid), {}):
            return trf(uid, "sig_price_unavailable", exchanges=ex)
    except Exception:
        pass
    return f"⚠️ Не удалось получить цену ({ex}). Попробуй ещё раз."

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
    # Per-symbol rolling cooldown has priority over daily cap.
    # Example: SIGNAL_SYMBOL_COOLDOWN_HOURS=12 means one signal per (market, symbol)
    # every 12 hours, regardless of calendar day boundaries.
    try:
        symbol_cooldown_hours = float((os.getenv("SIGNAL_SYMBOL_COOLDOWN_HOURS", "0") or "0").strip())
    except Exception:
        symbol_cooldown_hours = 0.0
    mk = str(getattr(sig, 'market', '') or '').upper().strip() or 'FUTURES'
    sym = str(getattr(sig, 'symbol', '') or '').upper().strip()
    cooldown_reserved = False
    if symbol_cooldown_hours > 0 and sym:
        try:
            # Allocate signal_id before broadcast so reservation is atomic across concurrent workers.
            sid = await db_store.next_signal_id()
            sig = replace(sig, signal_id=sid)
            side_stats = _normalize_side_for_stats(getattr(sig, 'direction', ''))
            cooldown_reserved, remaining_sec = await db_store.reserve_signal_track_with_symbol_cooldown(
                signal_id=int(sig.signal_id or 0),
                sig_key=sig_key,
                market=mk,
                symbol=sym,
                side=side_stats,
                entry=float(sig.entry or 0.0),
                tp1=(float(sig.tp1) if sig.tp1 is not None else None),
                tp2=(float(sig.tp2) if sig.tp2 is not None else None),
                sl=(float(sig.sl) if sig.sl is not None else None),
                cooldown_hours=float(symbol_cooldown_hours),
            )
            if not cooldown_reserved:
                if remaining_sec is not None:
                    rem_h, rem_rest = divmod(int(remaining_sec), 3600)
                    rem_m, rem_s = divmod(rem_rest, 60)
                    logger.info(
                        "Skip signal by symbol cooldown: market=%s symbol=%s cooldown_h=%.2f remaining=%02dh%02dm%02ds",
                        mk, sym, symbol_cooldown_hours, rem_h, rem_m, rem_s,
                    )
                    return
                # fail-open fallback: if reservation path could not decide, use read-check path
                last_opened_at = await db_store.get_last_signal_track_opened_at(market=mk, symbol=sym)
                if last_opened_at is not None:
                    now_utc = dt.datetime.now(dt.timezone.utc)
                    elapsed_sec = max(0.0, (now_utc - last_opened_at).total_seconds())
                    cooldown_sec = float(symbol_cooldown_hours) * 3600.0
                    if elapsed_sec < cooldown_sec:
                        remaining_sec = max(0, int(round(cooldown_sec - elapsed_sec)))
                        rem_h, rem_rest = divmod(remaining_sec, 3600)
                        rem_m, rem_s = divmod(rem_rest, 60)
                        logger.info(
                            "Skip signal by symbol cooldown: market=%s symbol=%s cooldown_h=%.2f remaining=%02dh%02dm%02ds",
                            mk, sym, symbol_cooldown_hours, rem_h, rem_m, rem_s,
                        )
                        return
        except Exception:
            # fail-open: do not block signals if DB is temporarily unavailable
            pass
    else:
        # Daily cap per symbol: e.g. allow only 2 broadcasts per (market, symbol) per day.
        # This is stronger than TTL dedup and prevents the same pair from spamming multiple times a day.
        try:
            cap = int(float((os.getenv("SIGNAL_DAILY_CAP_PER_SYMBOL", "2") or "2").strip()))
        except Exception:
            cap = 2
        if cap > 0 and sym:
            try:
                now_local = dt.datetime.now(TZ)
                day_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
                day_end = day_start + dt.timedelta(days=1)
                since_utc = day_start.astimezone(dt.timezone.utc)
                until_utc = day_end.astimezone(dt.timezone.utc)
                already = await db_store.count_signal_tracks_for_symbol(market=mk, symbol=sym, since=since_utc, until=until_utc)
                if int(already) >= int(cap):
                    logger.info("Skip signal by daily cap: market=%s symbol=%s already=%s cap=%s", mk, sym, already, cap)
                    return
            except Exception:
                # fail-open: do not block signals if DB is temporarily unavailable
                pass

    _SENT_SIG_CACHE[sig_key] = now

    # Assign a globally unique signal_id from DB sequence (survives restarts).
    # IMPORTANT:
    # If rolling symbol cooldown already reserved/inserted signal_tracks,
    # `sig.signal_id` is already allocated and MUST be kept unchanged.
    # Re-allocating here would create a *new* signal_id for the same sig_key,
    # and the later upsert_signal_track(...) would fail on UNIQUE(sig_key).
    if not int(getattr(sig, "signal_id", 0) or 0):
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
    # If cooldown reservation already inserted the row, this UPSERT only refreshes fields.
    try:
        side_stats = _normalize_side_for_stats(getattr(sig, 'direction', ''))
        _track_kwargs = {
            'signal_id': int(sig.signal_id or 0),
            'sig_key': sig_key,
            'market': str(sig.market).upper(),
            'symbol': str(sig.symbol),
            'side': side_stats,
            'entry': float(sig.entry or 0.0),
            'tp1': (float(sig.tp1) if sig.tp1 is not None else None),
            'tp2': (float(sig.tp2) if sig.tp2 is not None else None),
            'sl': (float(sig.sl) if sig.sl is not None else None),
        }
        try:
            _track_kwargs['orig_text'] = _signal_text(0, sig)
        except Exception:
            pass
        try:
            _track_kwargs['setup_source'] = str(getattr(sig, 'setup_source', '') or '').strip()
        except Exception:
            pass
        try:
            _track_kwargs['setup_source_label'] = str(getattr(sig, 'setup_source_label', '') or '').strip()
        except Exception:
            pass
        try:
            _track_kwargs['ui_setup_label'] = str(getattr(sig, 'ui_setup_label', '') or '').strip()
        except Exception:
            pass
        try:
            _track_kwargs['emit_route'] = str(getattr(sig, 'emit_route', '') or '').strip()
        except Exception:
            pass
        try:
            _track_kwargs['timeframe'] = str(getattr(sig, 'timeframe', '') or '').strip()
        except Exception:
            pass
        try:
            _track_kwargs['confidence'] = int(getattr(sig, 'confidence', 0) or 0)
        except Exception:
            pass
        try:
            _track_kwargs['rr'] = float(getattr(sig, 'rr', 0.0) or 0.0)
        except Exception:
            pass
        try:
            _track_kwargs['confirmations'] = str(getattr(sig, 'confirmations', '') or '').strip()
        except Exception:
            pass
        try:
            _track_kwargs['source_exchange'] = str(getattr(sig, 'source_exchange', '') or '').strip()
        except Exception:
            pass
        try:
            _track_kwargs['risk_note'] = str(getattr(sig, 'risk_note', '') or '').strip()
        except Exception:
            pass
        try:
            _track_kwargs['entry_snapshot_json'] = _signal_forensics_entry_snapshot(sig)
        except Exception:
            pass
        try:
            await db_store.upsert_signal_track(**_track_kwargs)
        except TypeError:
            _track_kwargs.pop('orig_text', None)
            _track_kwargs.pop('setup_source', None)
            _track_kwargs.pop('setup_source_label', None)
            _track_kwargs.pop('ui_setup_label', None)
            _track_kwargs.pop('emit_route', None)
            _track_kwargs.pop('timeframe', None)
            _track_kwargs.pop('confidence', None)
            _track_kwargs.pop('rr', None)
            _track_kwargs.pop('confirmations', None)
            _track_kwargs.pop('source_exchange', None)
            _track_kwargs.pop('risk_note', None)
            _track_kwargs.pop('entry_snapshot_json', None)
            await db_store.upsert_signal_track(**_track_kwargs)
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
            kb_u.adjust(1)
            await safe_send(uid, _signal_text(uid, sig, autotrade_hint=at_hint), reply_markup=kb_u.as_markup())

            # Auto-trade: execute real orders asynchronously, without affecting broadcast.
            async def _run_autotrade(_uid: int, _sig: Signal):
                try:
                    # Global auto-trade pause/maintenance (admin panel)
                    try:
                        gst = await db_store.get_autotrade_bot_settings()
                        if bool(gst.get("pause_autotrade")) or bool(gst.get("maintenance_mode")):
                            logger.info("[autotrade] skipped by global setting (pause/maintenance) uid=%s", _uid)
                            return
                    except Exception:
                        # FAIL-CLOSED: if DB read failed, do NOT open new trades.
                        logger.exception("[autotrade] skipped: failed to read global pause/maintenance (fail-closed) uid=%s", _uid)
                        return

                    res = await autotrade_execute(_uid, _sig)
                    try:
                        if isinstance(res, dict) and bool(res.get("ok")) and not bool(res.get("skipped")):
                            logger.info("[autotrade] executed uid=%s market=%s symbol=%s", _uid, getattr(_sig,"market",None), getattr(_sig,"symbol",None))
                            _health_mark_ok("autotrade-manager")
                    except Exception:
                        pass

                    # If skipped, log (and optionally forward to error bot) with a human reason.
                    if isinstance(res, dict) and bool(res.get("skipped")) and res.get("skip_reason"):
                        reason = str(res.get("skip_reason") or "").strip() or "skipped"
                        logger.warning("[autotrade] skipped uid=%s reason=%s symbol=%s market=%s", _uid, reason, getattr(_sig, "symbol", None), getattr(_sig, "market", None))
                        try:
                            send_skips = (os.getenv("AUTOTRADE_SEND_SKIPS_TO_ERROR_BOT", "1") or "0").strip().lower() not in ("0","false","no","off")
                            if send_skips:
                                import time as _t
                                k = (int(_uid), reason)
                                now = _t.time()
                                last = AUTOTRADE_SKIP_LAST.get(k, 0.0)
                                if now - last >= float(AUTOTRADE_SKIP_COOLDOWN_SEC):
                                    AUTOTRADE_SKIP_LAST[k] = now
                                    det = res.get("details") if isinstance(res.get("details"), dict) else {}
                                    await _error_bot_send(f"🤖 Auto-trade SKIP\nuid={_uid}\nreason={reason}\nmarket={getattr(_sig,'market',None)}\nsymbol={getattr(_sig,'symbol',None)}\ndetails={det}"[:3900])
                        except Exception:
                            pass

                    err = res.get("api_error") if isinstance(res, dict) else None
                    if err:
                        # notify ONLY for API errors; prefer the actual routed exchange
                        st = await db_store.get_autotrade_settings(_uid)
                        mt = "spot" if _sig.market == "SPOT" else "futures"
                        det = res.get("details") if isinstance(res.get("details"), dict) else {}
                        ex = str(det.get("exchange") or res.get("exchange") or st.get("spot_exchange" if mt == "spot" else "futures_exchange") or "").lower()
                        await _notify_autotrade_api_error(_uid, ex, mt, f"{getattr(_sig,'symbol','')} {getattr(_sig,'market','')}: {str(err)}"[:500])
                except Exception as e:
                    # unexpected errors are treated as API errors for visibility
                    st = await db_store.get_autotrade_settings(_uid)
                    mt = "spot" if _sig.market == "SPOT" else "futures"
                    ex = str(st.get("spot_exchange" if mt == "spot" else "futures_exchange") or "").lower()
                    sig_id = int(getattr(_sig, "signal_id", 0) or 0)
                    logger.exception("[autotrade] unexpected_error uid=%s signal_id=%s exchange=%s market_type=%s symbol=%s", _uid, sig_id, ex, mt, getattr(_sig,'symbol',None))
                    await _error_bot_send_autotrade_event(
                        "AUTO_TRADE_ERROR",
                        user_id=_uid,
                        signal_id=sig_id,
                        exchange=ex,
                        market_type=mt,
                        market=getattr(_sig, 'market', None),
                        symbol=getattr(_sig, 'symbol', None),
                        side=getattr(_sig, 'direction', None),
                        error=f"{type(e).__name__}: {e}"[:1000],
                    )
                    await _notify_autotrade_api_error(_uid, ex, mt, f"{getattr(_sig,'symbol','')} {getattr(_sig,'market','')}: {type(e).__name__}: {e}"[:500])

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
    await _ensure_public_bot_username()
    uid = message.from_user.id if message.from_user else 0
    referrer_id = None
    try:
        parts = (message.text or '').strip().split(maxsplit=1)
        if len(parts) > 1:
            raw_ref = str(parts[1]).strip()
            if raw_ref.lower().startswith('ref_'):
                raw_ref = raw_ref[4:]
            cand = int(raw_ref)
            if cand > 0 and cand != uid:
                referrer_id = cand
    except Exception:
        referrer_id = None
    if uid:
        await ensure_user(uid, referrer_id=referrer_id)
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
        await message.answer(tr_sub(uid, f"access_{access}"), reply_markup=subscription_gate_kb(uid))
        return

    await message.answer(await status_text(uid, include_subscribed=True, include_hint=True), reply_markup=menu_kb(uid))


# ---------------- language selection ----------------


@dp.callback_query(lambda c: (c.data or "").startswith("sub:"))
async def subscription_handler(call: types.CallbackQuery) -> None:
    await safe_callback_answer(call)
    uid = call.from_user.id if call.from_user else 0
    action = (call.data or '').split(':')
    if len(action) < 2:
        return
    subact = action[1]
    if subact == 'buy':
        await safe_edit(call.message, tr_sub(uid, 'pricing_text'), subscription_plans_kb(uid))
        return
    if subact == 'plan' and len(action) >= 3:
        plan_code = action[2]
        plan = _plan_data(plan_code)
        if not plan:
            await safe_edit(call.message, tr_sub(uid, 'plan_not_available'), subscription_plans_kb(uid))
            return
        try:
            invoice = await _create_nowpayments_invoice(telegram_id=uid, plan_code=plan_code)
            pay_url = invoice.get('invoice_url') or invoice.get('pay_url') or invoice.get('payment_url')
            if not pay_url:
                raise RuntimeError('missing_pay_url')
            txt = trf(uid, 'payment_created', plan_name=_plan_name(uid, plan_code), amount=_fmt_money(plan['amount']))
            await safe_edit(call.message, txt, subscription_pay_kb(uid, plan_code, pay_url))
        except Exception:
            logger.exception('subscription: create payment failed uid=%s plan=%s', uid, plan_code)
            await safe_edit(call.message, tr_sub(uid, 'payment_error'), subscription_plans_kb(uid))
        return

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
    await safe_callback_answer(call, )
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
        f"{tr(uid, 'lbl_trades')}: {trades} | {tr(uid, 'lbl_wins')}: {wins} | {tr(uid, 'lbl_sl')}: {losses} | "
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
        f"Trades: {trades} | Wins: {wins} | SL: {losses} | BE: {be} | TP1: {tp1}\n"
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


async def _perf_today_fallback(user_id: int, market: str) -> dict:
    """Compatibility shim: Backend.perf_today may be missing or have a different signature."""
    fn = getattr(backend, "perf_today", None)
    if callable(fn):
        try:
            return await fn(int(user_id), str(market))
        except TypeError:
            # Older builds exposed global perf_today(market)
            return await fn(str(market))
    tz = ZoneInfo(TZ_NAME)
    now_tz = dt.datetime.now(tz)
    start_tz = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
    end_tz = start_tz + dt.timedelta(days=1)
    start = start_tz.astimezone(dt.timezone.utc)
    end = end_tz.astimezone(dt.timezone.utc)
    return await db_store.perf_bucket(int(user_id), (market or "FUTURES").upper(), since=start, until=end)

async def _perf_week_fallback(user_id: int, market: str) -> dict:
    fn = getattr(backend, "perf_week", None)
    if callable(fn):
        try:
            return await fn(int(user_id), str(market))
        except TypeError:
            return await fn(str(market))
    tz = ZoneInfo(TZ_NAME)
    now_tz = dt.datetime.now(tz)
    start_tz = (now_tz - dt.timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_tz = now_tz
    start = start_tz.astimezone(dt.timezone.utc)
    end = end_tz.astimezone(dt.timezone.utc)
    return await db_store.perf_bucket(int(user_id), (market or "FUTURES").upper(), since=start, until=end)

async def _report_daily_fallback(user_id: int, market: str, days: int, tz: str) -> list[dict]:
    fn = getattr(backend, "report_daily", None)
    if callable(fn):
        try:
            return await fn(int(user_id), str(market), int(days), tz=str(tz))
        except TypeError:
            return await fn(int(user_id), str(market), int(days))
    return await db_store.daily_report(int(user_id), (market or "FUTURES").upper(), days=int(days), tz=str(tz))

async def _report_weekly_fallback(user_id: int, market: str, weeks: int, tz: str) -> list[dict]:
    fn = getattr(backend, "report_weekly", None)
    if callable(fn):
        try:
            return await fn(int(user_id), str(market), int(weeks), tz=str(tz))
        except TypeError:
            return await fn(int(user_id), str(market), int(weeks))
    return await db_store.weekly_report(int(user_id), (market or "FUTURES").upper(), weeks=int(weeks), tz=str(tz))

async def _get_user_trades_fallback(user_id: int, limit: int = 50) -> list[dict]:
    fn = getattr(backend, "get_user_trades", None)
    if callable(fn):
        try:
            return await fn(int(user_id))
        except TypeError:
            return await fn(int(user_id), limit=int(limit))
    return await db_store.list_user_trades(int(user_id), include_closed=False, limit=int(limit))


async def stats_text(uid: int) -> str:
    """Render trading statistics for the user from Postgres (backend-only)."""
    # Used by nested helpers below
    lang = get_lang(uid)
    # IMPORTANT: use the bot's configured IANA timezone (TZ_NAME), not env "TZ".
    # Many servers/users set TZ to aliases like "MSK" which are not valid for Postgres AT TIME ZONE.
    tz_name = TZ_NAME

    # ---- buckets ----
    spot_today = await _perf_today_fallback(uid, "SPOT")
    fut_today = await _perf_today_fallback(uid, "FUTURES")
    spot_week = await _perf_week_fallback(uid, "SPOT")
    fut_week = await _perf_week_fallback(uid, "FUTURES")

    spot_days = await _report_daily_fallback(uid, "SPOT", 7, tz=tz_name)
    fut_days = await _report_daily_fallback(uid, "FUTURES", 7, tz=tz_name)

    spot_weeks = await _report_weekly_fallback(uid, "SPOT", 4, tz=tz_name)
    fut_weeks = await _report_weekly_fallback(uid, "FUTURES", 4, tz=tz_name)

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

    await safe_callback_answer(call, )

    # Shared access control (Postgres)
    access = await get_access_status(uid) if uid else "no_user"
    if access != "ok":
        await safe_edit(
            call.message,
            tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.",
            subscription_gate_kb(uid)
        )
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
        await safe_edit(call.message, txt, menu_kb(uid) if access == "ok" else subscription_gate_kb(uid))
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

    
    # ---- ANALYZE TOKEN (TA) ----
    if action == "analysis":
        # Enable one-shot analyze mode: next text message will be treated as ticker to analyze
        ANALYZE_INPUT[uid] = True
        await safe_edit(
            call.message,
            tr(uid, "analysis_prompt"),
            None,  # no menu while waiting for ticker input
        )
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
            # If we couldn't fetch current price (temporary exchange/API issue),
            # do NOT treat the signal as expired. Show it with a warning and keep the OPEN button.
            if (reason or "").upper() == "ERROR":
                kb = InlineKeyboardBuilder()
                kb.button(text=tr(uid, "btn_opened"), callback_data=f"open:{sig.signal_id}")
                kb.adjust(1)
                warn = _price_unavailable_notice(uid, sig)
                await safe_edit(call.message, warn + "\n\n" + _signal_text(uid, sig), kb.as_markup())
                return

            # Otherwise it's truly not live anymore (TP/SL/time) -> drop from cache.
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

    # ---- REFERRAL ----
    if action == "referral":
        if not await _refresh_referral_access_cache(uid):
            await safe_edit(call.message, await _referral_disabled_text(uid), menu_kb(uid))
            return
        await safe_edit(call.message, await referral_main_text(uid), referral_main_kb(uid))
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


@dp.callback_query(lambda c: (c.data or "").startswith("ref:"))
async def referral_handler(call: types.CallbackQuery) -> None:
    await safe_callback_answer(call)
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return
    if not await _refresh_referral_access_cache(uid):
        await safe_edit(call.message, await _referral_disabled_text(uid), menu_kb(uid))
        return
    parts = (call.data or '').split(':')
    action = parts[1] if len(parts) > 1 else ''
    if action == 'link':
        await _ensure_public_bot_username()
        await safe_edit(call.message, trf(uid, 'ref_link_text', link=_ref_link(uid)), referral_main_kb(uid))
        return
    if action == 'stats':
        await safe_edit(call.message, await referral_stats_text(uid), referral_main_kb(uid))
        return
    if action == 'balance':
        await safe_edit(call.message, await referral_balance_text(uid), referral_balance_kb(uid))
        return
    if action == 'withdraw':
        ov = await db_store.get_referral_overview(uid)
        existing = await db_store.get_open_referral_withdrawal_request(uid)
        if existing:
            await safe_edit(call.message, trf(uid, 'ref_withdraw_pending', amount=_fmt_money(existing.get('amount') or 0), wallet=existing.get('wallet_address') or '-'), referral_balance_kb(uid))
            return
        if float(ov.get('available_balance') or 0) + 1e-9 < float(REFERRAL_MIN_WITHDRAW_USDT):
            await safe_edit(call.message, trf(uid, 'ref_withdraw_not_enough', min_withdraw=_fmt_money(REFERRAL_MIN_WITHDRAW_USDT), available_balance=_fmt_money(ov.get('available_balance') or 0)), referral_balance_kb(uid))
            return
        REFERRAL_WITHDRAW_INPUT[uid] = True
        await safe_edit(call.message, await referral_withdraw_prompt_text(uid), referral_withdraw_prompt_kb(uid))
        return


@dp.callback_query(lambda c: (c.data or '').startswith('refadm:'))
async def referral_admin_handler(call: types.CallbackQuery) -> None:
    await safe_callback_answer(call)
    uid = call.from_user.id if call.from_user else 0
    if not is_admin(uid):
        return
    parts = (call.data or '').split(':')
    action = parts[1] if len(parts) > 1 else ''
    if action == 'profile' and len(parts) >= 3:
        target_uid = int(parts[2])
        prof = await db_store.get_referral_user_profile(target_uid)
        username = '-'
        try:
            ch = await bot.get_chat(target_uid)
            username = ('@' + ch.username) if getattr(ch, 'username', None) else '-'
        except Exception:
            pass
        created_at = prof.get('created_at')
        signal_expires_at = prof.get('signal_expires_at')
        created_at_text = created_at.strftime('%d.%m.%Y') if hasattr(created_at, 'strftime') else '-'
        signal_expires_text = signal_expires_at.strftime('%d.%m.%Y') if hasattr(signal_expires_at, 'strftime') else '-'
        txt = (
            f"👤 Профиль пользователя\n\n"
            f"Username: {username}\n"
            f"ID: {target_uid}\n\n"
            f"Дата регистрации:\n{created_at_text}\n\n"
            f"Подписка активна до:\n{signal_expires_text}\n\n"
            f"Приглашено:\n{int(prof.get('total_refs') or 0)}\n\n"
            f"Оплатили:\n{int(prof.get('paid_refs') or 0)}"
        )
        # Send via the main bot because the admin request message and callbacks live in the main bot chat.
        await safe_send(REFERRAL_ADMIN_CHAT_ID, txt, ctx='referral_admin_profile')
        return
    if action == 'stats' and len(parts) >= 3:
        target_uid = int(parts[2])
        ov = await db_store.get_referral_overview(target_uid)
        txt = (
            f"📊 Реферальная статистика\n\n"
            f"ID: {target_uid}\n\n"
            f"Всего приглашено: {int(ov.get('total_refs') or 0)}\n"
            f"Оплатили: {int(ov.get('paid_refs') or 0)}\n\n"
            f"Всего начислено: {_fmt_money(ov.get('total_earned') or 0)} USDT\n"
            f"Доступно: {_fmt_money(ov.get('available_balance') or 0)} USDT\n"
            f"В обработке: {_fmt_money(ov.get('hold_balance') or 0)} USDT\n"
            f"Выплачено: {_fmt_money(ov.get('withdrawn_balance') or 0)} USDT"
        )
        # Send via the main bot because the admin request message and callbacks live in the main bot chat.
        await safe_send(REFERRAL_ADMIN_CHAT_ID, txt, ctx='referral_admin_stats')
        return
    if len(parts) < 4:
        return
    request_id = int(parts[2])
    target_uid = int(parts[3])
    if action == 'paid':
        req = await db_store.mark_referral_withdrawal_paid(request_id=request_id, processed_by=uid, admin_comment='paid')
        if req:
            await safe_send(target_uid, trf(target_uid, 'ref_withdraw_paid_user', amount=_fmt_money(req.get('amount') or 0), network=REFERRAL_NETWORK))
            if call.message:
                try:
                    await safe_edit(call.message, f"✅ Выплата подтверждена\n\nRequest ID: {request_id}")
                except Exception:
                    pass
        return
    if action == 'reject':
        req = await db_store.reject_referral_withdrawal_request(request_id=request_id, processed_by=uid, admin_comment='rejected')
        if req:
            await safe_send(target_uid, trf(target_uid, 'ref_withdraw_rejected_user', amount=_fmt_money(req.get('amount') or 0)))
            if call.message:
                try:
                    await safe_edit(call.message, f"❌ Заявка отклонена\n\nRequest ID: {request_id}")
                except Exception:
                    pass
        return


@dp.message(lambda m: REFERRAL_WITHDRAW_INPUT.get(m.from_user.id if m.from_user else 0, False))
async def referral_withdraw_wallet_input(message: types.Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    if not uid or not REFERRAL_WITHDRAW_INPUT.get(uid):
        return
    if not await _refresh_referral_access_cache(uid):
        REFERRAL_WITHDRAW_INPUT.pop(uid, None)
        await message.answer(await _referral_disabled_text(uid), reply_markup=menu_kb(uid))
        return
    wallet = (message.text or '').strip()
    if not _is_valid_bsc_address(wallet):
        REFERRAL_WITHDRAW_INPUT[uid] = True
        await message.answer(tr(uid, 'ref_wallet_invalid'), reply_markup=referral_withdraw_prompt_kb(uid))
        return
    REFERRAL_WITHDRAW_INPUT.pop(uid, None)
    ov = await db_store.get_referral_overview(uid)
    amount = float(ov.get('available_balance') or 0)
    if amount + 1e-9 < float(REFERRAL_MIN_WITHDRAW_USDT):
        await message.answer(trf(uid, 'ref_withdraw_not_enough', min_withdraw=_fmt_money(REFERRAL_MIN_WITHDRAW_USDT), available_balance=_fmt_money(amount)), reply_markup=referral_balance_kb(uid))
        return
    try:
        req = await db_store.create_referral_withdrawal_request(user_id=uid, wallet_address=wallet, amount=amount, currency='USDT', network=REFERRAL_NETWORK)
    except ValueError as e:
        code = str(e)
        if code == 'pending_request_exists':
            await message.answer(tr(uid, 'ref_pending_exists'), reply_markup=referral_balance_kb(uid))
            return
        await message.answer(tr(uid, 'ref_withdraw_create_error'), reply_markup=referral_balance_kb(uid))
        return
    except Exception:
        logger.exception('referral: create withdrawal request failed uid=%s', uid)
        await message.answer(tr(uid, 'ref_withdraw_create_error'), reply_markup=referral_balance_kb(uid))
        return
    await message.answer(trf(uid, 'ref_withdraw_created', amount=_fmt_money(req.get('amount') or 0), network=REFERRAL_NETWORK, wallet=wallet, admin_bot='@'+REFERRAL_ADMIN_BOT_USERNAME), reply_markup=referral_balance_kb(uid))
    try:
        await _notify_referral_admin_new_request(req)
    except Exception:
        logger.exception('referral: notify admin failed request_id=%s', req.get('id'))


# ---------------- notifications callbacks ----------------
@dp.callback_query(lambda c: (c.data or "").startswith("notify:"))
async def notify_handler(call: types.CallbackQuery) -> None:
    """Handle inline buttons inside Notifications screen."""
    await safe_callback_answer(call, )
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
        await safe_edit(call.message, tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.", subscription_gate_kb(uid))
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

        try:
            txt = await stats_text(uid)
        except Exception as e:
            logger.exception("menu:stats failed for uid=%s", uid)
            txt = tr(uid, "stats_error") if ("stats_error" in I18N.get(get_lang(uid), {})) else ("Ошибка статистики. Попробуй позже.")
            if _is_admin(uid):
                txt += f"\n\nERR: {type(e).__name__}: {e}"
        await safe_edit(call.message, txt, menu_kb(uid))
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
    await safe_callback_answer(call, )
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return
    async with _user_lock(uid):

        # Shared access control
        access = await get_access_status(uid)
        if access != "ok":
            await safe_edit(call.message, tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.", subscription_gate_kb(uid))
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
            # futures exchange choices (Binance/Bybit/OKX)
            kb = InlineKeyboardBuilder()
            kb.button(text="Binance", callback_data=f"at:exset:{mt}:binance")
            kb.button(text="Bybit", callback_data=f"at:exset:{mt}:bybit")
            kb.button(text="OKX", callback_data=f"at:exset:{mt}:okx")
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
    await safe_callback_answer(call, )
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    access = await get_access_status(uid)
    if access != "ok":
        await safe_edit(call.message, tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.", subscription_gate_kb(uid))
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
        await safe_edit(call.message, tr(uid, "at_info_text"), autotrade_info_kb(uid), disable_web_page_preview=True)
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
    await safe_callback_answer(call, )
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    access = await get_access_status(uid)
    if access != "ok":
        await safe_edit(call.message, tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.", subscription_gate_kb(uid))
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
        # ---- one-shot TA analyze input ----
        if ANALYZE_INPUT.pop(uid, False):
            # Access check
            access = await get_access_status(uid)
            if access != "ok":
                await safe_send_nonempty(uid, tr_sub(uid, f"access_{access}"), reply_markup=menu_kb(uid))
                return

            text = (message.text or "").strip()
            if not text:
                await message.answer(tr(uid, "analysis_prompt"), reply_markup=menu_kb(uid))
                return

            # Get known symbols from running scanners (best-effort, no extra REST).
            try:
                known = backend.get_known_symbols(limit=5000)
            except Exception:
                known = []

            # Cold-start fallback: if scanner caches are empty, fetch a larger USDT universe once.
            # This is cached / rate-limited inside backend and only happens on user-triggered analysis.
            if not known:
                try:
                    known = await backend.get_top_usdt_symbols(1500)
                except Exception:
                    known = []

            symbol, suggestions, note = _normalize_symbol_input(text, known_symbols=known)

            if not symbol:
                # If not found in scanner/top caches, try resolving across exchanges (Binance/Bybit/OKX/MEXC/Gate).
                # This avoids false "not found" for symbols that are not in the current scanner universe.
                try:
                    sym_guess, _, note_guess = _normalize_symbol_input(text, known_symbols=None)
                    ok, provider, norm_sym = await backend.resolve_symbol_any_exchange(sym_guess or text)
                    if ok and norm_sym:
                        symbol = norm_sym
                        suggestions = []
                        note = note or note_guess or (f"Найдено на {provider}" if provider else None)
                except Exception:
                    pass
                # Ask again without forcing the user to press the Analyze button
                ANALYZE_INPUT[uid] = True
                if suggestions:
                    sug_txt = "\n".join(f"• {s}" for s in suggestions)
                    await message.answer(
                        f"❌ Тикер не найден: {text}\n\nВозможно ты имел в виду:\n{sug_txt}\n\nНапиши тикер ещё раз (например BTC или BTCUSDT).",
                        reply_markup=menu_kb(uid),
                    )
                else:
                    await message.answer(
                        f"❌ Тикер не найден: {text}\n\nНапиши тикер ещё раз (например BTC или BTCUSDT).",
                        reply_markup=menu_kb(uid),
                    )
                return

            # Send an immediate progress message so the user understands the bot is working.
            # Then delete it once the report is ready (clean UX).
            processing_msg = None
            try:
                processing_msg = await message.answer(f"⏳ Анализирую {symbol} ...")
            except Exception:
                # If Telegram rejects the send (rare), continue with analysis anyway.
                processing_msg = None

            try:
                analysis_payload = await backend.analyze_symbol_institutional(symbol, market="AUTO", lang=get_lang(uid), return_bundle=True)
            except Exception as e:
                logger.exception("analysis failed for %s", symbol)
                msg = tr(uid, "analysis_error") if ("analysis_error" in I18N.get(get_lang(uid), {})) else "Ошибка анализа. Попробуй позже."
                if _is_admin(uid):
                    msg += f"\n\nERR: {type(e).__name__}: {e}"
                # Remove the progress message if it exists
                if processing_msg is not None:
                    try:
                        await processing_msg.delete()
                    except Exception:
                        pass
                await message.answer(msg, reply_markup=menu_kb(uid))
                return

            scenario_text = None
            scenario_filename = None
            if isinstance(analysis_payload, dict):
                report = str(analysis_payload.get("report") or "")
                scenario_text = str(analysis_payload.get("scenario_text") or "").strip() or None
                scenario_filename = str(analysis_payload.get("scenario_filename") or f"{symbol}_scenarios.txt")
            else:
                report = str(analysis_payload)

            # If we auto-normalized/auto-picked, prepend a small hint.
            # IMPORTANT: do NOT wrap with Markdown markers here — the note can contain
            # underscores or other characters that break Telegram entity parsing.
            if note:
                report = f"ℹ️ {note}\n\n" + str(report)

            # Remove the progress message if it exists
            if processing_msg is not None:
                try:
                    await processing_msg.delete()
                except Exception:
                    pass


            # Send report safely: handle Telegram length limits + Markdown parse errors.
            kb = menu_kb(uid)
            chat_id = message.chat.id
            try:
                # NOTE: report can be very long and contains a lot of symbols.
                # Sending as plain text is the most reliable option (no entity parsing).
                await _send_long(chat_id, str(report), reply_markup=kb)
            except Exception:
                # Never crash the webhook task because of Telegram formatting/length issues
                try:
                    await _send_long(chat_id, str(report), reply_markup=kb)
                except Exception:
                    pass

            if scenario_text:
                try:
                    scenario_payload = BufferedInputFile(
                        scenario_text.encode("utf-8"),
                        filename=(scenario_filename or f"{symbol}_scenarios.txt"),
                    )
                    scenario_caption = f"🧠 Сценарии: {symbol}" if get_lang(uid).startswith("ru") else f"🧠 Scenarios: {symbol}"
                    await bot.send_document(chat_id, document=scenario_payload, caption=scenario_caption)
                except Exception:
                    logger.exception("failed to send scenario file for %s", symbol)
            return

        state = AUTOTRADE_INPUT.get(uid)
        if not state:
            return

        # Access check
        access = await get_access_status(uid)
        if access != "ok":
            AUTOTRADE_INPUT.pop(uid, None)
            await safe_send_nonempty(uid, tr_sub(uid, f"access_{access}"), reply_markup=menu_kb(uid))
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
                    except RuntimeError:
                        await message.answer(tr(uid, "at_master_key_missing"))
                    except Exception as e:
                        logger.exception("Autotrade encryption init failed while saving keys uid=%s ex=%s mt=%s", uid, ex, mt)
                        await message.answer(f"⚠️ Ошибка шифрования API-ключей: {type(e).__name__}: {e}")
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
                    except RuntimeError:
                        await message.answer(tr(uid, "at_master_key_missing"))
                    except Exception as e:
                        logger.exception("Autotrade encryption init failed while saving keys uid=%s ex=%s mt=%s", uid, ex, mt)
                        await message.answer(f"⚠️ Ошибка шифрования API-ключей: {type(e).__name__}: {e}")
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
    await safe_callback_answer(call, )
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    access = await get_access_status(uid)
    if access != "ok":
        await safe_send(
            uid,
            tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.",
            reply_markup=subscription_gate_kb(uid)
        )
        return

    try:
        offset = int((call.data or "").split(":")[-1])
    except Exception:
        offset = 0

    all_trades = await _get_user_trades_fallback(uid)
    if not all_trades:
        await safe_send(uid, tr(uid, "my_trades_empty"), reply_markup=menu_kb(uid))
        return

    page = all_trades[offset:offset+PAGE_SIZE]
    txt = tr(uid, "my_trades_title").format(a=offset+1, b=min(offset+PAGE_SIZE, len(all_trades)), n=len(all_trades))
    await safe_send(uid, txt, reply_markup=_trades_page_kb(uid, page, offset))

# ---------------- trade card ----------------



def _tp1_partial_close_pct_trade(market: str) -> float:
    """Partial close pct at TP1 for AUTOTRADE cards (same env as backend)."""
    mk = (market or "").upper()
    try:
        if mk == "SPOT":
            return float(os.getenv("TP1_PARTIAL_CLOSE_PCT_SPOT", "50") or 50.0)
        return float(os.getenv("TP1_PARTIAL_CLOSE_PCT_FUTURES", "50") or 50.0)
    except Exception:
        return 50.0

def _trade_pnl_if_be_after_tp1(t: dict) -> float | None:
    """Estimate total PnL% if TP1 is hit and remainder closes at BE (no fees)."""
    try:
        entry = float(t.get("entry") or 0.0)
        tp1 = float(t.get("tp1") or 0.0)
        if entry <= 0 or tp1 <= 0:
            return None
        side = str(t.get("direction") or t.get("side") or "LONG").upper()
        market = str(t.get("market") or "FUTURES").upper()
        a = max(0.0, min(100.0, _tp1_partial_close_pct_trade(market))) / 100.0
        if side == "SHORT":
            pnl_tp1 = (entry - tp1) / entry * 100.0
        else:
            pnl_tp1 = (tp1 - entry) / entry * 100.0
        return float(pnl_tp1 * a)
    except Exception:
        return None

def _trade_card_text(uid: int, t: dict) -> str:
    """Unified trade card for /my trades (open trades), matching the PRO card style.

    - ACTIVE: shows status + unrealized PnL (if live price is available) + RR + live checks.
    - TP1: shows locked total PnL (TP1 part) and BE protection + live checks.
    """
    symbol = str(t.get("symbol") or "")
    market = str(t.get("market") or "FUTURES").upper()
    side = str(t.get("side") or "LONG").upper()
    status = str(t.get("status") or "ACTIVE").upper()

    entry = float(t.get("entry") or 0.0)
    sl_v = float(t.get("sl") or 0.0) if t.get("sl") is not None else 0.0
    tp1_v = float(t.get("tp1") or 0.0) if t.get("tp1") is not None else 0.0
    tp2_v = float(t.get("tp2") or 0.0) if t.get("tp2") is not None else 0.0

    def _rr(entry_f: float, sl_f: float, tp2_f: float, side_s: str) -> str:
        try:
            if entry_f <= 0 or sl_f <= 0 or tp2_f <= 0:
                return "—"
            if side_s == "SHORT":
                risk = sl_f - entry_f
                reward = entry_f - tp2_f
            else:
                risk = entry_f - sl_f
                reward = tp2_f - entry_f
            if risk <= 0 or reward <= 0:
                return "—"
            return f"{(reward / risk):.2f}"
        except Exception:
            return "—"

    rr = _rr(entry, sl_v, tp2_v, side)

    # --- live price + checks ---
    live_lines: list[str] = []
    checks_line = ""
    px = None
    src = ""
    try:
        if t.get("price_f") is not None:
            px = float(t.get("price_f") or 0.0)
            src = str(t.get("price_src") or "")
    except Exception:
        px = None

    if px is not None and px > 0:
        live_lines.append(f"💹 {tr(uid, 'lbl_price_now')}: {px:.6f}")
        if src:
            live_lines.append(f"🔌 {tr(uid, 'lbl_price_src')}: {src}")

        # Checks should reflect executed state if TP already happened (TP1/TP2 flags), otherwise it can lie.
        st = str(t.get("status") or status).upper()
        hit_sl = bool(t.get("hit_sl")) or st in ("LOSS",)
        hit_tp1 = bool(t.get("hit_tp1")) or st in ("TP1", "WIN", "TP2")
        hit_tp2 = bool(t.get("hit_tp2")) or st in ("WIN", "TP2")

        checks: list[str] = []

        # In TP1 state we show BE + HARD_SL instead of original SL
        if st == "TP1":
            # BE
            if t.get("be_price") is not None:
                hit_be = bool(t.get("hit_be"))
                checks.append(f"{tr(uid, 'lbl_be')}: {'✅' if hit_be else '❌'}")
            # HARD SL (after TP1)
            try:
                hard_pct = float(os.getenv("SMART_HARD_SL_PCT", "2.8") or 0.0)
            except Exception:
                hard_pct = 0.0
            hard_sl = 0.0
            if entry > 0 and hard_pct > 0 and side in ("LONG", "SHORT"):
                hard_sl = (entry * (1 - hard_pct / 100.0)) if side == "LONG" else (entry * (1 + hard_pct / 100.0))
            if hard_sl > 0:
                hit_hsl = bool(t.get("hit_hard_sl"))
                if not hit_hsl:
                    try:
                        hit_hsl = (px <= hard_sl) if side == "LONG" else (px >= hard_sl)
                    except Exception:
                        hit_hsl = False
                checks.append(f"{tr(uid, 'lbl_hard_sl')}: {'✅' if hit_hsl else '❌'}")
        else:
            if t.get("sl") is not None:
                checks.append(f"{tr(uid, 'lbl_sl')}: {'✅' if hit_sl else '❌'}")

        if t.get("tp1") is not None:
            checks.append(f"{tr(uid, 'lbl_tp1')}: {'✅' if hit_tp1 else '❌'}")
        if t.get("tp2") is not None and float(t.get("tp2") or 0) > 0:
            checks.append(f"{tr(uid, 'lbl_tp2')}: {'✅' if hit_tp2 else '❌'}")

        if checks:
            checks_line = f"🧪 {tr(uid, 'lbl_check')}: " + " ".join(checks)

    live_block = ("\n".join(live_lines) + "\n\n") if live_lines else ""
    checks_block = (checks_line + "\n") if checks_line else ""


    opened_time = _fmt_dt_msk(t.get("opened_at"))

    # --- BE line for details ---
    be_price = None
    try:
        if t.get("be_price") is not None:
            be_price = float(t.get("be_price") or 0.0)
    except Exception:
        be_price = None
    be_line = (f"🛡 BE: {be_price:.6f}\n" if (be_price and status == "TP1") else "")

    # --- status-specific PnL ---
    def _fmt_pct(v: float | None) -> str:
        if v is None:
            return "—"
        try:
            return f"{float(v):+.2f}%"
        except Exception:
            return "—"

    if status == "TP1":
        locked = _trade_pnl_if_be_after_tp1(t)
        pnl_total = _fmt_pct(locked)
        tp1_time = _fmt_dt_msk(t.get("tp1_at"))
        return trf(uid, "msg_trade_open_tp1",
            symbol=symbol,
            market=market,
            side=side,
            rr=rr,
            pnl_total=pnl_total,
            live_block=live_block,
            checks_block=checks_block,
            entry=f"{entry:.6f}",
            sl=f"{sl_v:.6f}",
            tp1=f"{tp1_v:.6f}" if tp1_v else "—",
            tp2=f"{tp2_v:.6f}" if tp2_v else "—",
            be_price=f"{be_price:.6f}" if be_price else "—",
            opened_time=opened_time,
            event_time=tp1_time,
        )

    # ACTIVE (or anything else that's not closed)
    pnl_unreal = None
    if px is not None and px > 0 and entry > 0 and side in ("LONG","SHORT"):
        pnl_unreal = ((px - entry) / entry * 100.0) if side == "LONG" else ((entry - px) / entry * 100.0)

    emoji = "🟢" if side == "LONG" else "🔴"
    return trf(uid, "msg_trade_open",
        emoji=emoji,
        symbol=symbol,
        market=market,
        side=side,
        status=status,
        pnl_unreal=_fmt_pct(pnl_unreal),
        rr=rr,
        live_block=live_block,
        checks_block=checks_block,
        entry=f"{entry:.6f}",
        sl=f"{sl_v:.6f}" if sl_v else "—",
        tp1=f"{tp1_v:.6f}" if tp1_v else "—",
        tp2=f"{tp2_v:.6f}" if tp2_v else "—",
        be_line="",
        opened_time=opened_time,
    )


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
    await safe_callback_answer(call, )
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    access = await get_access_status(uid)
    if access != "ok":
        await safe_send(
            uid,
            tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.",
            reply_markup=subscription_gate_kb(uid)
        )
        return
    parts = (call.data or "").split(":")
    try:
        trade_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return
    # Backward-compat: some deployments may not yet have Backend.get_trade_live_by_id
    if hasattr(backend, "get_trade_live_by_id"):
        t = await backend.get_trade_live_by_id(call.from_user.id, trade_id)
    else:
        # Fallback: show stored trade without live price enrichment
        t = await db_store.get_trade_by_id(int(call.from_user.id), int(trade_id))
    if not t:
        await safe_send(call.from_user.id, tr(call.from_user.id, "trade_not_found"), reply_markup=menu_kb(call.from_user.id))
        return
    await safe_send(call.from_user.id, _trade_card_text(call.from_user.id, t), reply_markup=_trade_card_kb(call.from_user.id, trade_id, back_offset))

@dp.callback_query(lambda c: (c.data or "").startswith("trade:refresh:"))
async def trade_refresh(call: types.CallbackQuery) -> None:
    await safe_callback_answer(call, )
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    access = await get_access_status(uid)
    if access != "ok":
        await safe_send(
            uid,
            tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.",
            reply_markup=subscription_gate_kb(uid)
        )
        return
    parts = (call.data or "").split(":")
    try:
        trade_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return
    if hasattr(backend, "get_trade_live_by_id"):
        t = await backend.get_trade_live_by_id(call.from_user.id, trade_id)
    else:
        t = await db_store.get_trade_by_id(int(call.from_user.id), int(trade_id))
    if not t:
        await safe_send(call.from_user.id, tr(call.from_user.id, "trade_not_found"), reply_markup=menu_kb(call.from_user.id))
        return
    await safe_send(call.from_user.id, _trade_card_text(call.from_user.id, t), reply_markup=_trade_card_kb(call.from_user.id, trade_id, back_offset))

@dp.callback_query(lambda c: (c.data or "").startswith("trade:close:"))
async def trade_close(call: types.CallbackQuery) -> None:
    await safe_callback_answer(call, )
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    access = await get_access_status(uid)
    if access != "ok":
        await safe_send(
            uid,
            tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.",
            reply_markup=subscription_gate_kb(uid)
        )
        return
    parts = (call.data or "").split(":")
    try:
        trade_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return
    res = await backend.remove_trade_by_id(call.from_user.id, trade_id)
    if isinstance(res, dict) and res.get("ok") and res.get("text"):
        # Send unified close card with correct TP1-aware total PnL
        await safe_send(call.from_user.id, str(res.get("text")), reply_markup=menu_kb(call.from_user.id))
    elif res:
        # Backward compatible
        await safe_send(call.from_user.id, tr(call.from_user.id, "trade_removed"), reply_markup=menu_kb(call.from_user.id))
    else:
        await safe_send(call.from_user.id, tr(call.from_user.id, "trade_removed_fail"), reply_markup=menu_kb(call.from_user.id))

@dp.callback_query(lambda c: (c.data or "").startswith("trade:orig:"))
async def trade_orig(call: types.CallbackQuery) -> None:
    await safe_callback_answer(call, )
    uid = call.from_user.id if call.from_user else 0
    if not uid:
        return

    access = await get_access_status(uid)
    if access != "ok":
        await safe_send(
            uid,
            tr_sub(uid, f"access_{access}") or "⏰ Срок доступа истёк.\n\nКупите подписку, чтобы продолжить.",
            reply_markup=subscription_gate_kb(uid)
        )
        return
    parts = (call.data or "").split(":")
    try:
        trade_id = int(parts[2])
        back_offset = int(parts[3]) if len(parts) > 3 else 0
    except Exception:
        return

    if hasattr(backend, "get_trade_live_by_id"):
        t = await backend.get_trade_live_by_id(call.from_user.id, trade_id)
    else:
        t = await db_store.get_trade_by_id(int(call.from_user.id), int(trade_id))
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
        await safe_callback_answer(call, "Error", show_alert=True)
        return

    sig = SIGNALS.get(signal_id)
    if not sig:
        await safe_callback_answer(call, "Signal not available", show_alert=True)
        return

    allowed, reason, price = await backend.check_signal_openable(sig)
    if not allowed:
        # If we couldn't fetch current price (temporary exchange/API issue),
        # do NOT expire the signal. Just warn and let the user retry.
        if (reason or "").upper() == "ERROR":
            await safe_callback_answer(call, _price_unavailable_notice(call.from_user.id, sig), show_alert=True)
            # Send a small helper message with the OPEN button (so user can tap again).
            kb = InlineKeyboardBuilder()
            kb.button(text=tr(call.from_user.id, "btn_opened"), callback_data=f"open:{sig.signal_id}")
            kb.adjust(1)
            try:
                await safe_send(call.from_user.id, _price_unavailable_notice(call.from_user.id, sig), reply_markup=kb.as_markup())
            except Exception:
                pass
            return

        # Otherwise it's truly too late (TP/SL/time) -> remove stale from Live cache + expire message.
        mkt = (sig.market or "FUTURES").upper()
        if LAST_SIGNAL_BY_MARKET.get(mkt) and LAST_SIGNAL_BY_MARKET[mkt].signal_id == sig.signal_id:
            LAST_SIGNAL_BY_MARKET[mkt] = None
        await _expire_signal_message(call, sig, reason, price)
        await safe_callback_answer(call, tr(call.from_user.id, "sig_too_late_toast"), show_alert=True)
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
            await safe_callback_answer(call, tr(call.from_user.id, "sig_already_opened_toast"), show_alert=True)
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
            await safe_callback_answer(call, tr(call.from_user.id, "sig_already_opened_toast"), show_alert=True)
            await safe_send(call.from_user.id, tr(call.from_user.id, "sig_already_opened_msg"), reply_markup=menu_kb(call.from_user.id))
            return

    # Remove the ✅ ОТКРЫЛ СДЕЛКУ button from the original NEW SIGNAL message
    try:
        if call.message:
            await safe_edit_markup(call.from_user.id, call.message.message_id, None)
    except Exception:
        pass

    await safe_callback_answer(call, tr(call.from_user.id, "trade_opened_toast"))
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
# SL anti-wick confirmation for SIGNAL outcomes (admin stats)
# Price must cross SL +/- buffer and stay beyond it for SIG_SL_CONFIRM_SEC seconds.
# Signal outcome stats should also ignore single-wick touches; keep defaults slightly safer.
_SIG_SL_BUFFER_PCT = float(os.getenv("SIG_SL_BUFFER_PCT", "0.0010") or 0.0010)  # 0.10% default
_SIG_SL_CONFIRM_SEC = max(0, int(os.getenv("SIG_SL_CONFIRM_SEC", "15") or 15))
_SIG_SL_BREACH_SINCE: dict[int, float] = {}  # signal_id -> unix ts when SL was first breached

_SIG_MAX_TRACK_AGE_HOURS = float(os.getenv("SIG_MAX_TRACK_AGE_HOURS", "72") or 72)  # auto-close stale signals

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
    """Public last price from Bybit v5 market tickers with light retry/fallback parsing."""
    symbol = str(symbol or "").upper().replace("/", "").replace("-", "").replace(":", "").strip()
    if not symbol:
        return 0.0
    category = "linear" if futures else "spot"
    url = "https://api.bybit.com/v5/market/tickers"

    def _extract_price(data) -> float:
        if not isinstance(data, dict):
            return 0.0
        try:
            ret_code = int(data.get("retCode", 0) or 0)
        except Exception:
            ret_code = 0
        if ret_code != 0:
            return 0.0
        lst = (((data or {}).get("result") or {}).get("list") or [])
        if lst and isinstance(lst, list) and isinstance(lst[0], dict):
            item = lst[0] or {}
            for key in ("lastPrice", "last_price", "indexPrice", "markPrice"):
                try:
                    px = float(item.get(key) or 0.0)
                    if px > 0:
                        return px
                except Exception:
                    continue
            try:
                bid = float(item.get("bid1Price") or 0.0)
                ask = float(item.get("ask1Price") or 0.0)
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2.0
                if bid > 0:
                    return bid
                if ask > 0:
                    return ask
            except Exception:
                pass
        return 0.0

    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            for attempt in range(3):
                try:
                    async with s.get(url, params={"category": category, "symbol": symbol}) as r:
                        data = await r.json(content_type=None)
                    px = _extract_price(data)
                    if px > 0:
                        return float(px)
                except Exception:
                    pass
                if attempt < 2:
                    await asyncio.sleep(0.25 * (attempt + 1))
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
    parts = parts or [p.strip().lower() for p in default.split(",") if p.strip()]

    # Railway cost control: do not call MEXC/Gate.io unless explicitly enabled.
    mexc_on = (os.getenv("EXCHANGE_MEXC_ENABLED", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
    gate_on = (os.getenv("EXCHANGE_GATEIO_ENABLED", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
    filtered = []
    for src in parts:
        if src == "mexc" and not mexc_on:
            continue
        if src in ("gate", "gateio") and not gate_on:
            continue
        filtered.append(src)
    return filtered or ["binance", "bybit", "okx"]


async def _fetch_signal_price(symbol: str, *, market: str) -> tuple[float, str]:
    """Get price for bot-level signal tracking. Returns (price, source).

    This is used by `signal_outcome_loop` (global bot stats). It MUST use the
    same price policy as live tracking:
      1) Prefer Backend WS-first oracle (MEDIAN/BINANCE/BYBIT as configured).
      2) If it fails, fall back to lightweight REST fetchers.
    """
    m = (market or "SPOT").upper()
    sym = str(symbol or "").upper().strip()
    if not sym:
        return 0.0, ""

    # 1) Prefer backend WS-first oracle (same policy as live tracking)
    try:
        # Minimal Signal instance for the oracle. Only market/symbol/entry are used.
        sig0 = Signal(
            market=("SPOT" if m == "SPOT" else "FUTURES"),
            symbol=sym,
            entry=0.0,
            sl=None,
            tp1=None,
            tp2=None,
            direction="LONG",
            rr=0.0,
            confidence="",
            timeframe="",
            confirmations=[],
            signal_id=0,
        )
        px, src = await backend._get_price_with_source(sig0)  # type: ignore[attr-defined]
        px = float(px or 0.0)
        if px > 0:
            return px, str(src or "")
    except Exception:
        # fall back to REST helpers below
        pass

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
    order = _parse_price_order("SIG_PRICE_ORDER_SPOT", "binance,bybit,okx")
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

def _sig_net_pnl_tp1_then_sl(*, market: str, side: str, entry: float, tp1: float, sl: float, part: float) -> float:
    """Net PnL for partial TP1 then close remainder at SL."""
    p = max(0.0, min(1.0, float(part)))
    pnl1 = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=tp1, part_entry_to_close=p)
    pnl2 = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=sl, part_entry_to_close=(1.0 - p))
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
    import time as _time
    logger.info("[sig-outcome] loop started (heartbeat=60s transitions=True) interval=%ss buffer_pct=%s be_confirm_sec=%s sl_confirm_sec=%s", _SIG_WATCH_INTERVAL_SEC, _BE_BUFFER_PCT, _BE_CONFIRM_SEC, _SIG_SL_CONFIRM_SEC)
    last_beat = _time.monotonic()
    tick = 0
    while True:
        try:
            tick += 1
            rows = await db_store.list_open_signal_tracks(limit=1000)
            open_tracks = len(rows)
            # heartbeat: show loop is alive even when open_tracks=0
            if _time.monotonic() - last_beat >= 60:
                last_beat = _time.monotonic()
                logger.info("[sig-outcome] alive tick=%s open_tracks=%s", tick, open_tracks)
            if not rows:
                await asyncio.sleep(_SIG_WATCH_INTERVAL_SEC)
                continue

            # per-iteration counters for visibility
            processed = 0
            price_miss = 0
            tp1_marked = 0
            closed_win = 0
            closed_loss = 0
            closed_be = 0
            closed_timeout = 0

            now = _dt.datetime.now(_dt.timezone.utc)
            part = _model_partial_pct()

            for t in rows:
                try:
                    processed += 1
                    sid = int(t.get("signal_id") or 0)
                    market = str(t.get("market") or "SPOT").upper()
                    symbol = str(t.get("symbol") or "").upper()
                    side = str(t.get("side") or "LONG").upper()
                    status = str(t.get("status") or "ACTIVE").upper()

                    opened_at = t.get("opened_at")
                    opened_at_dt = _parse_iso_dt(opened_at) if isinstance(opened_at, str) else opened_at

                    entry = float(t.get("entry") or 0.0)
                    tp1 = float(t.get("tp1") or 0.0) if t.get("tp1") is not None else 0.0
                    tp2 = float(t.get("tp2") or 0.0) if t.get("tp2") is not None else 0.0
                    sl  = float(t.get("sl") or 0.0)  if t.get("sl")  is not None else 0.0

                    if sid <= 0 or entry <= 0 or not symbol:
                        continue

                    # Auto-close stale signals so outcomes stats do not stay at zero forever
                    # (e.g., if a signal never reaches TP/SL or the market becomes illiquid).
                    try:
                        max_age_h = float(_SIG_MAX_TRACK_AGE_HOURS or 0)
                    except Exception:
                        max_age_h = 0.0
                    if max_age_h > 0 and opened_at_dt is not None:
                        try:
                            age_sec = (now - opened_at_dt).total_seconds()
                        except Exception:
                            age_sec = 0.0
                        if age_sec > max_age_h * 3600:
                            # best-effort pnl snapshot at timeout
                            px_to = 0.0
                            try:
                                px_to, _ = await _fetch_signal_price(symbol, market=market)
                            except Exception:
                                px_to = 0.0
                            pnl_to = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=px_to, part_entry_to_close=1.0) if (entry > 0 and px_to > 0) else 0.0
                            closed_now = await db_store.close_signal_track(signal_id=sid, status="CLOSED", pnl_total_pct=float(pnl_to))
                            _SIG_SL_BREACH_SINCE.pop(sid, None)
                            if not closed_now:
                                continue
                            await _send_closed_signal_report_card(t, final_status="CLOSED", pnl_total_pct=float(pnl_to), closed_at=now)
                            await _adaptive_v3_update_from_outcome(dict(t), final_status="CLOSED", pnl_total_pct=float(pnl_to), closed_at=now)
                            closed_timeout += 1
                            continue

                    px, _src = await _fetch_signal_price(symbol, market=market)
                    if px <= 0:
                        price_miss += 1
                        continue

                    # Prefer TPC: if tp2 missing but tp1 exists, treat tp1 as final target
                    eff_tp2 = tp2 if (tp2 > 0 and (tp1 <= 0 or abs(tp2 - tp1) > 1e-12)) else 0.0
                    eff_tp1 = tp1 if tp1 > 0 else 0.0

                    # ACTIVE stage
                    if status == "ACTIVE":
                        # WIN by TP2 (or TP1 if no TP2)
                        if eff_tp2 > 0 and _hit_tp(side, px, eff_tp2):
                            pnl = _sig_net_pnl_two_targets(market=market, side=side, entry=entry, tp1=eff_tp1, tp2=eff_tp2, part=part) if (eff_tp1 > 0 and eff_tp2 > 0) else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=eff_tp2, part_entry_to_close=1.0)
                            closed_now = await db_store.close_signal_track(signal_id=sid, status="WIN", pnl_total_pct=float(pnl))
                            _SIG_SL_BREACH_SINCE.pop(sid, None)
                            if not closed_now:
                                continue
                            await _send_closed_signal_report_card(t, final_status="WIN", pnl_total_pct=float(pnl), closed_at=now)
                            await _adaptive_v3_update_from_outcome(dict(t), final_status="WIN", pnl_total_pct=float(pnl), closed_at=now)
                            logger.info("[sig-outcome] WIN sid=%s %s %s px=%s tp1=%s tp2=%s src=%s", sid, market, symbol, _fmt_price(px), _fmt_price(eff_tp1 if eff_tp1>0 else None), _fmt_price(eff_tp2 if eff_tp2>0 else None), _src)
                            closed_win += 1
                            continue
                        if eff_tp2 <= 0 and eff_tp1 > 0 and _hit_tp(side, px, eff_tp1):
                            # single-target win at TP1
                            pnl = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=eff_tp1, part_entry_to_close=1.0)
                            closed_now = await db_store.close_signal_track(signal_id=sid, status="WIN", pnl_total_pct=float(pnl))
                            _SIG_SL_BREACH_SINCE.pop(sid, None)
                            if not closed_now:
                                continue
                            await _send_closed_signal_report_card(t, final_status="WIN", pnl_total_pct=float(pnl), closed_at=now)
                            await _adaptive_v3_update_from_outcome(dict(t), final_status="WIN", pnl_total_pct=float(pnl), closed_at=now)
                            logger.info("[sig-outcome] WIN sid=%s %s %s px=%s tp1=%s tp2=%s src=%s", sid, market, symbol, _fmt_price(px), _fmt_price(eff_tp1 if eff_tp1>0 else None), _fmt_price(eff_tp2 if eff_tp2>0 else None), _src)
                            closed_win += 1
                            continue
                        # LOSS by SL (only before TP1 in strict mode)
                        if sl > 0:
                            # Anti-wick SL confirmation:
                            # - apply a small buffer beyond SL
                            # - require staying beyond trigger for SIG_SL_CONFIRM_SEC seconds
                            sig_is_mid = False
                            try:
                                sig_is_mid = (str(t.get("timeframe") or "").lower().startswith("5m/")) or ("5m/30m/1h" in str(t.get("orig_text") or ""))
                            except Exception:
                                sig_is_mid = False
                            sig_sl_buffer_pct = max(float(_SIG_SL_BUFFER_PCT), (float(os.getenv("SIG_SL_BUFFER_PCT_MID", _SIG_SL_BUFFER_PCT if sig_is_mid else _SIG_SL_BUFFER_PCT) or _SIG_SL_BUFFER_PCT) if sig_is_mid else float(_SIG_SL_BUFFER_PCT)))
                            sig_sl_confirm_sec = int(float(os.getenv("SIG_SL_CONFIRM_SEC_MID", _SIG_SL_CONFIRM_SEC) or _SIG_SL_CONFIRM_SEC)) if sig_is_mid else int(_SIG_SL_CONFIRM_SEC)
                            trigger = sl * (1.0 - sig_sl_buffer_pct) if side == "LONG" else sl * (1.0 + sig_sl_buffer_pct)
                            crossed = bool(px <= trigger) if side == "LONG" else bool(px >= trigger)

                            since = _SIG_SL_BREACH_SINCE.get(sid)
                            if crossed and since is None:
                                _SIG_SL_BREACH_SINCE[sid] = _time.time()
                                continue
                            if not crossed and since is not None:
                                _SIG_SL_BREACH_SINCE.pop(sid, None)
                                # keep tracking
                            if crossed:
                                if sig_sl_confirm_sec == 0 or (since is not None and (_time.time() - since) >= sig_sl_confirm_sec):
                                    pnl = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=sl, part_entry_to_close=1.0)
                                    _close_analysis = await _loss_diag_build_close_analysis(dict(t), closed_at=now)
                                    _loss_diag = _build_loss_diagnostics_from_row(t, final_status="LOSS", closed_at=now, close_analysis=_close_analysis)
                                    closed_now = await db_store.close_signal_track(
                                        signal_id=sid,
                                        status="LOSS",
                                        pnl_total_pct=float(pnl),
                                        close_reason_code=str(_loss_diag.get('reason_code') or ''),
                                        close_reason_text=str(_loss_diag.get('reason_text') or ''),
                                        weak_filters=",".join(list(_loss_diag.get('weak_filter_keys') or [])),
                                        improve_note="; ".join(list(_loss_diag.get('improve_keys') or [])),
                                        close_analysis_json=_loss_diag.get('close_analysis_json') or _close_analysis,
                                    )
                                    _SIG_SL_BREACH_SINCE.pop(sid, None)
                                    if not closed_now:
                                        continue
                                    _report_row = dict(t)
                                    _report_row['close_analysis_json'] = _loss_diag.get('close_analysis_json') or _close_analysis
                                    _report_row['close_reason_code'] = str(_loss_diag.get('reason_code') or '')
                                    _report_row['close_reason_text'] = str(_loss_diag.get('reason_text') or '')
                                    _report_row['weak_filters'] = ",".join(list(_loss_diag.get('weak_filter_keys') or []))
                                    _report_row['improve_note'] = "; ".join(list(_loss_diag.get('improve_keys') or []))
                                    await _send_closed_signal_report_card(_report_row, final_status="LOSS", pnl_total_pct=float(pnl), closed_at=now)
                                    await _adaptive_v3_update_from_outcome(dict(_report_row), final_status="LOSS", pnl_total_pct=float(pnl), closed_at=now)
                                    logger.info("[sig-outcome] LOSS sid=%s %s %s px=%s sl=%s tp1=%s tp2=%s src=%s", sid, market, symbol, _fmt_price(px), _fmt_price(sl if sl>0 else None), _fmt_price(eff_tp1 if eff_tp1>0 else None), _fmt_price(eff_tp2 if eff_tp2>0 else None), _src)
                                    closed_loss += 1
                                    continue
                        # TP1 hit -> arm BE
                        if eff_tp1 > 0 and _hit_tp(side, px, eff_tp1):
                                                        # TP1 realized PnL (partial close 50%) for dashboard PnL%.
                            try:
                                tp1_part = 0.5
                                tp1_exec = float(eff_tp1)
                                ent = float(entry)
                                if side.upper() == 'LONG':
                                    tp1_pnl = ((tp1_exec - ent) / ent) * 100.0
                                else:
                                    tp1_pnl = ((ent - tp1_exec) / ent) * 100.0
                                tp1_realized = tp1_pnl * tp1_part
                            except Exception:
                                tp1_realized = None
                            await db_store.mark_signal_tp1(signal_id=sid, be_price=float(entry), tp1_pnl_pct=tp1_realized)
                            logger.info("[sig-outcome] TP1 sid=%s %s %s px=%s tp1=%s -> arm BE@%s src=%s", sid, market, symbol, _fmt_price(px), _fmt_price(eff_tp1 if eff_tp1>0 else None), _fmt_price(entry), _src)
                            _SIG_SL_BREACH_SINCE.pop(sid, None)
                            tp1_marked += 1
                            continue

                    # TP1 stage (BE armed)
                    if status == "TP1":
                        # Legacy migration: single-target signals should not stay in TP1.
                        if eff_tp2 <= 0 and eff_tp1 > 0:
                            pnl = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=eff_tp1, part_entry_to_close=1.0)
                            await db_store.close_signal_track(signal_id=sid, status="WIN", pnl_total_pct=float(pnl))
                            await _send_closed_signal_report_card(t, final_status="WIN", pnl_total_pct=float(pnl), closed_at=now)
                            await _adaptive_v3_update_from_outcome(dict(t), final_status="WIN", pnl_total_pct=float(pnl), closed_at=now)
                            logger.info("[sig-outcome] WIN sid=%s %s %s legacy_tp1_finalize tp1=%s src=%s", sid, market, symbol, _fmt_price(eff_tp1 if eff_tp1>0 else None), _src)
                            _SIG_SL_BREACH_SINCE.pop(sid, None)
                            closed_win += 1
                            continue

                        # WIN by TP2 if exists
                        if eff_tp2 > 0 and _hit_tp(side, px, eff_tp2):
                            pnl = _sig_net_pnl_two_targets(market=market, side=side, entry=entry, tp1=eff_tp1, tp2=eff_tp2, part=part) if eff_tp1 > 0 else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=eff_tp2, part_entry_to_close=1.0)
                            closed_now = await db_store.close_signal_track(signal_id=sid, status="WIN", pnl_total_pct=float(pnl))
                            _SIG_SL_BREACH_SINCE.pop(sid, None)
                            if not closed_now:
                                continue
                            await _send_closed_signal_report_card(t, final_status="WIN", pnl_total_pct=float(pnl), closed_at=now)
                            await _adaptive_v3_update_from_outcome(dict(t), final_status="WIN", pnl_total_pct=float(pnl), closed_at=now)
                            logger.info("[sig-outcome] WIN sid=%s %s %s px=%s tp1=%s tp2=%s src=%s", sid, market, symbol, _fmt_price(px), _fmt_price(eff_tp1 if eff_tp1>0 else None), _fmt_price(eff_tp2 if eff_tp2>0 else None), _src)
                            closed_win += 1
                            continue

                        # LOSS by SL (can happen after TP1; model partial TP1 then SL on remainder)
                        if sl > 0:
                            sig_is_mid = False
                            try:
                                sig_is_mid = (str(t.get("timeframe") or "").lower().startswith("5m/")) or ("5m/30m/1h" in str(t.get("orig_text") or ""))
                            except Exception:
                                sig_is_mid = False
                            sig_sl_buffer_pct = max(float(_SIG_SL_BUFFER_PCT), (float(os.getenv("SIG_SL_BUFFER_PCT_MID", _SIG_SL_BUFFER_PCT if sig_is_mid else _SIG_SL_BUFFER_PCT) or _SIG_SL_BUFFER_PCT) if sig_is_mid else float(_SIG_SL_BUFFER_PCT)))
                            sig_sl_confirm_sec = int(float(os.getenv("SIG_SL_CONFIRM_SEC_MID", _SIG_SL_CONFIRM_SEC) or _SIG_SL_CONFIRM_SEC)) if sig_is_mid else int(_SIG_SL_CONFIRM_SEC)
                            trigger = sl * (1.0 - sig_sl_buffer_pct) if side == "LONG" else sl * (1.0 + sig_sl_buffer_pct)
                            crossed = bool(px <= trigger) if side == "LONG" else bool(px >= trigger)

                            since = _SIG_SL_BREACH_SINCE.get(sid)
                            if crossed and since is None:
                                _SIG_SL_BREACH_SINCE[sid] = _time.time()
                                continue
                            if not crossed and since is not None:
                                _SIG_SL_BREACH_SINCE.pop(sid, None)
                            if crossed:
                                if sig_sl_confirm_sec == 0 or (since is not None and (_time.time() - since) >= sig_sl_confirm_sec):
                                    pnl = _sig_net_pnl_tp1_then_sl(market=market, side=side, entry=entry, tp1=eff_tp1, sl=sl, part=part) if eff_tp1 > 0 else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=sl, part_entry_to_close=1.0)
                                    _close_analysis = await _loss_diag_build_close_analysis(dict(t), closed_at=now)
                                    _loss_diag = _build_loss_diagnostics_from_row(t, final_status="LOSS", closed_at=now, close_analysis=_close_analysis)
                                    closed_now = await db_store.close_signal_track(
                                        signal_id=sid,
                                        status="LOSS",
                                        pnl_total_pct=float(pnl),
                                        close_reason_code=str(_loss_diag.get('reason_code') or ''),
                                        close_reason_text=str(_loss_diag.get('reason_text') or ''),
                                        weak_filters=",".join(list(_loss_diag.get('weak_filter_keys') or [])),
                                        improve_note="; ".join(list(_loss_diag.get('improve_keys') or [])),
                                        close_analysis_json=_loss_diag.get('close_analysis_json') or _close_analysis,
                                    )
                                    _SIG_SL_BREACH_SINCE.pop(sid, None)
                                    if not closed_now:
                                        continue
                                    _report_row = dict(t)
                                    _report_row['close_analysis_json'] = _loss_diag.get('close_analysis_json') or _close_analysis
                                    _report_row['close_reason_code'] = str(_loss_diag.get('reason_code') or '')
                                    _report_row['close_reason_text'] = str(_loss_diag.get('reason_text') or '')
                                    _report_row['weak_filters'] = ",".join(list(_loss_diag.get('weak_filter_keys') or []))
                                    _report_row['improve_note'] = "; ".join(list(_loss_diag.get('improve_keys') or []))
                                    await _send_closed_signal_report_card(_report_row, final_status="LOSS", pnl_total_pct=float(pnl), closed_at=now)
                                    await _adaptive_v3_update_from_outcome(dict(_report_row), final_status="LOSS", pnl_total_pct=float(pnl), closed_at=now)
                                    logger.info("[sig-outcome] LOSS sid=%s %s %s px=%s sl=%s tp1=%s tp2=%s src=%s", sid, market, symbol, _fmt_price(px), _fmt_price(sl if sl>0 else None), _fmt_price(eff_tp1 if eff_tp1>0 else None), _fmt_price(eff_tp2 if eff_tp2>0 else None), _src)
                                    closed_loss += 1
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
                                closed_now = await db_store.close_signal_track(signal_id=sid, status="BE", pnl_total_pct=float(pnl))
                                _SIG_SL_BREACH_SINCE.pop(sid, None)
                                if not closed_now:
                                    continue
                                await _send_closed_signal_report_card(t, final_status="BE", pnl_total_pct=float(pnl), closed_at=now)
                                await _adaptive_v3_update_from_outcome(dict(t), final_status="BE", pnl_total_pct=float(pnl), closed_at=now)
                                logger.info("[sig-outcome] BE sid=%s %s %s px=%s be=%s tp1=%s src=%s", sid, market, symbol, _fmt_price(px), _fmt_price(entry), _fmt_price(eff_tp1 if eff_tp1>0 else None), _src)
                                closed_be += 1
                                continue
                        if crossed and crossed_at_dt is not None and _BE_CONFIRM_SEC == 0:
                            pnl = _sig_net_pnl_tp1_then_be(market=market, side=side, entry=entry, tp1=eff_tp1, part=part) if eff_tp1 > 0 else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=entry, part_entry_to_close=1.0)
                            closed_now = await db_store.close_signal_track(signal_id=sid, status="BE", pnl_total_pct=float(pnl))
                            _SIG_SL_BREACH_SINCE.pop(sid, None)
                            if not closed_now:
                                continue
                            await _send_closed_signal_report_card(t, final_status="BE", pnl_total_pct=float(pnl), closed_at=now)
                            await _adaptive_v3_update_from_outcome(dict(t), final_status="BE", pnl_total_pct=float(pnl), closed_at=now)
                            logger.info("[sig-outcome] BE sid=%s %s %s px=%s be=%s tp1=%s src=%s", sid, market, symbol, _fmt_price(px), _fmt_price(entry), _fmt_price(eff_tp1 if eff_tp1>0 else None), _src)
                            closed_be += 1
                            continue

                except Exception:
                    # keep loop alive
                    logger.exception("signal_outcome_loop: item error")
                    continue

            # heartbeat / visibility (once per minute)
            if _time.monotonic() - last_beat >= 60:
                last_beat = _time.monotonic()
                logger.info(
                    "signal_outcome_loop heartbeat: open_tracks=%s processed=%s price_miss=%s tp1=%s win=%s loss=%s be=%s timeout=%s",
                    len(rows), processed, price_miss, tp1_marked, closed_win, closed_loss, closed_be, closed_timeout,
                )

        except Exception:
            logger.exception("signal_outcome_loop error")

        await asyncio.sleep(_SIG_WATCH_INTERVAL_SEC)


async def main() -> None:
    import time
    
    logger.info("Bot starting; TZ=%s", TZ_NAME)
    worker_role = str(os.getenv("WORKER_ROLE", "") or "").strip().upper()
    if worker_role == "WS_CANDLES":
        logger.info("WORKER_ROLE=WS_CANDLES -> starting dedicated candles worker")
        await init_db()
        load_langs()
        cleanup_enabled = str(os.getenv("CANDLES_CACHE_CLEANUP_ENABLED", "1") or "1").strip().lower() not in ("0","false","no","off")
        if cleanup_enabled:
            TASKS["candles-cache-cleanup"] = asyncio.create_task(candles_cache_cleanup_loop(backend), name="candles-cache-cleanup")
            _attach_task_monitor("candles-cache-cleanup", TASKS["candles-cache-cleanup"])
        TASKS["ws-candles"] = asyncio.create_task(ws_candles_service_loop(backend), name="ws-candles")
        _attach_task_monitor("ws-candles", TASKS["ws-candles"])
        logger.info("WS candles worker active; Telegram bot is not started in this role")
        await asyncio.Event().wait()
        return
    try:
        get_autotrade_master_key()
        logger.info("AUTOTRADE_MASTER_KEY validated successfully")
    except Exception as e:
        logger.critical("Invalid AUTOTRADE_MASTER_KEY: %s", e)
        raise
    await init_db()
    load_langs()

    # Persistent candles cache maintenance (optional).
    # NOTE: Disabled by default. Prefer backend.candles_cache_cleanup_loop for DB-level cleanup.
    purge_enabled = str(os.getenv("CANDLES_CACHE_PURGE_LOOP_ENABLED", "0") or "0").strip().lower() not in ("0","false","no","off")
    cleanup_enabled = str(os.getenv("CANDLES_CACHE_CLEANUP_ENABLED", "1") or "1").strip().lower() not in ("0","false","no","off")
    if purge_enabled and (not cleanup_enabled):
        TASKS["candles-cache-purge"] = asyncio.create_task(_candles_cache_purge_loop(), name="candles-cache-purge")
        _attach_task_monitor("candles-cache-purge", TASKS["candles-cache-purge"])

    ws_inproc_enabled = str(os.getenv("CANDLES_WS_INPROC_ENABLED", "0") or "0").strip().lower() not in ("0","false","no","off")
    if ws_inproc_enabled:
        TASKS["ws-candles"] = asyncio.create_task(ws_candles_service_loop(backend), name="ws-candles")
        _attach_task_monitor("ws-candles", TASKS["ws-candles"])
        if cleanup_enabled:
            TASKS["candles-cache-cleanup"] = asyncio.create_task(candles_cache_cleanup_loop(backend), name="candles-cache-cleanup")
            _attach_task_monitor("candles-cache-cleanup", TASKS["candles-cache-cleanup"])



    # Forward unhandled asyncio task exceptions to error-bot (so "Task exception was never retrieved" is not lost)
    def _loop_exc_handler(loop, context):
        try:
            msg = str(context.get("message") or "asyncio exception")
            exc = context.get("exception")
            logger.error("[asyncio] %s", msg, exc_info=exc)
            # fire-and-forget; protected by _error_bot_send internals
            asyncio.create_task(_error_bot_send(f"❗ asyncio\n{msg}\n{repr(exc)}"[:3900]))
        except Exception:
            pass
    try:
        asyncio.get_running_loop().set_exception_handler(_loop_exc_handler)
    except Exception:
        pass

    # Refresh global Auto-trade pause/maintenance flags in background
    TASKS["autotrade-global"] = asyncio.create_task(_autotrade_bot_global_loop(), name="autotrade-global")
    _attach_task_monitor("autotrade-global", TASKS["autotrade-global"])
    await _refresh_autotrade_bot_global_once(); _health_mark_ok("autotrade-global")

    # --- Admin HTTP API for Status panel (Signal bot access) ---
    # NOTE: Railway public domain requires an HTTP server listening on $PORT.
    # We run a tiny aiohttp server alongside the Telegram bot.
    async def _admin_http_app() -> web.Application:
        try:
            signal_admin_max_body_mb = float(os.getenv("SIGNAL_ADMIN_MAX_BODY_MB", "20") or 20)
        except Exception:
            signal_admin_max_body_mb = 20.0
        if signal_admin_max_body_mb <= 0:
            signal_admin_max_body_mb = 20.0
        app = web.Application(client_max_size=int(signal_admin_max_body_mb * 1024 * 1024))

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
                except web.HTTPException as exc:
                    # Expected HTTP errors (404/405/etc.) should not pollute logs as tracebacks.
                    resp = exc
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
            payload = {"ok": True, "service": "crypto-signal"}
            try:
                if hasattr(backend, "mid_status_snapshot"):
                    payload["mid"] = await backend.mid_status_snapshot()
            except Exception:
                pass
            return web.json_response(payload)

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

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO users (telegram_id, notify_signals)
                    VALUES ($1, TRUE)
                    ON CONFLICT (telegram_id) DO NOTHING
                    """,
                    tid,
                )
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
            q = (request.query.get("q") or request.query.get("query") or "").strip()
            flt = (request.query.get("filter") or "all").strip().lower()
            async with pool.acquire() as conn:
                global_referral_enabled = bool(await conn.fetchval("SELECT COALESCE(referral_enabled, FALSE) FROM signal_bot_settings WHERE id=1") or False)
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
                           COALESCE(autotrade_stop_after_close,FALSE) AS autotrade_stop_after_close,
                           COALESCE(referral_enabled,FALSE) AS referral_enabled
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
                manual_referral_enabled = bool(r.get("referral_enabled", False))
                effective_referral_enabled = bool(global_referral_enabled or manual_referral_enabled)
                if global_referral_enabled and manual_referral_enabled:
                    referral_source = "global+manual"
                elif global_referral_enabled:
                    referral_source = "global"
                elif manual_referral_enabled:
                    referral_source = "manual"
                else:
                    referral_source = "off"
                out.append({
                    "telegram_id": int(r["telegram_id"]),
                    "signal_enabled": bool(r["signal_enabled"]),
                    "signal_expires_at": (r["signal_expires_at"].isoformat() if r["signal_expires_at"] else None),
                    "is_blocked": bool(r["is_blocked"]),
                    "autotrade_enabled": bool(r.get("autotrade_enabled", False)),
                    "autotrade_stop_after_close": stop_flag,
                    "autotrade_state": at_state,
                    "autotrade_expires_at": (r["autotrade_expires_at"].isoformat() if r.get("autotrade_expires_at") else None),
                    "referral_enabled": manual_referral_enabled,
                    "referral_access": effective_referral_enabled,
                    "referral_access_source": referral_source,
                })
            return web.json_response({"items": out})

        async def signal_stats(request: web.Request) -> web.Response:
            """Read-only aggregated statistics for the admin panel.

            Returns counts for:
              - signals sent: day/week/month
              - outcomes & pnl%: day/week/month per market (SPOT/FUTURES)

            IMPORTANT:
            The admin panel widget **"Signals closed (outcomes)"** reflects **bot-level signal outcomes**,
            i.e. how many broadcasted signals eventually closed as TP2/WIN, SL/LOSS, BE, TP1-hit, etc.

            Source of truth for outcomes is `signal_tracks` (bot-level tracking), not user trades.
            """
            if not _check_basic(request):
                return _unauthorized()

            # IMPORTANT:
            # Dashboard numbers must match Moscow time (TZ_NAME/TZ).
            # Use *calendar* ranges in TZ (Europe/Moscow by default):
            #   - day   = today 00:00..now (MSK)
            #   - week  = Monday 00:00..now (MSK)
            #   - month = 1st day 00:00..now (MSK)
            #
            # Convert ranges to UTC before querying Postgres.
            now_utc = dt.datetime.now(dt.timezone.utc)
            now_tz = now_utc.astimezone(TZ)
            day_start_tz = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
            week_start_tz = day_start_tz - dt.timedelta(days=day_start_tz.weekday())  # Monday
            month_start_tz = day_start_tz.replace(day=1)

            ranges = {
                "day": (day_start_tz.astimezone(dt.timezone.utc), now_utc),
                "week": (week_start_tz.astimezone(dt.timezone.utc), now_utc),
                "month": (month_start_tz.astimezone(dt.timezone.utc), now_utc),
            }
            # Signals sent counters are stored in Postgres (signal_sent_events).
            # Backward compatible fields: day/week/month totals + split day_spot/day_futures/...
            signals: dict = {}
            for k, (since, until) in ranges.items():
                spot_n = 0
                fut_n = 0
                if pool is not None:
                    try:
                        # Prefer signal_tracks for "sent" counters so that Sent and Outcomes come from the same source.
                        try:
                            counts = await db_store.count_signal_tracks_opened_by_market(since=since, until=until)
                        except Exception:
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
                    # Bot-level outcomes are tracked in signal_tracks.
                    b = await db_store.signal_perf_bucket_global(market, since=since, until=until)
                    # IMPORTANT (dashboard correctness):
                    # signal_perf_bucket_global() returns:
                    #   - trades: terminal outcomes only (WIN/LOSS/BE)
                    #   - closes: manual/forced closes (status='CLOSED') separately
                    tp2 = int(b.get('wins') or 0)       # WIN (final target reached)
                    sl = int(b.get('losses') or 0)      # LOSS (SL)
                    be = int(b.get('be') or 0)          # BE
                    tp1 = int(b.get('tp1_hits') or 0)   # TP1 hit (partial)
                    trades = int(b.get('trades') or (tp2 + sl + be))  # visible outcomes in table
                    closes = int(b.get('closes') or 0)  # manual/expired/forced
                    closed_total = trades + closes
                    sum_pnl = float(b.get('sum_pnl_pct') or 0.0)
                    avg_pnl = (sum_pnl / trades) if trades else 0.0
                    out[k] = {
                        "trades": trades,
                        "tp2": tp2,
                        "sl": sl,
                        "be": be,
                        "tp1": tp1,
                        # back-compat fields used by older dashboards
                        "manual": closed_total,      # legacy: "closed" total
                        "closed": closed_total,      # total closed (including manual/forced)
                        "manual_close": closes,
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
                    # Use "closed" (includes manual/forced closes) for the left-side widget totals.
                    spot_closed = int(((perf.get("spot") or {}).get(k) or {}).get("closed") or 0)
                    fut_closed = int(((perf.get("futures") or {}).get(k) or {}).get("closed") or 0)
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

        async def _apply_signal_user_update(tid: int, data: dict) -> None:
            data = data or {}

            signal_enabled_present = ("signal_enabled" in data)
            signal_enabled = bool(data.get("signal_enabled")) if signal_enabled_present else False
            signal_expires_present = ("signal_expires_at" in data)
            signal_expires_raw = data.get("signal_expires_at") if signal_expires_present else None
            signal_expires = _parse_iso_dt(signal_expires_raw) if signal_expires_raw not in (None, "") else None
            if signal_expires_present and signal_expires_raw not in (None, "") and signal_expires is None:
                raise ValueError("bad signal_expires_at")
            signal_add_days = int(data.get("add_days") or 0)
            touch_signal = bool(signal_enabled_present or signal_expires_present or signal_add_days)

            autotrade_enabled_present = ("autotrade_enabled" in data)
            autotrade_enabled = bool(data.get("autotrade_enabled")) if autotrade_enabled_present else False
            autotrade_expires_present = ("autotrade_expires_at" in data)
            autotrade_expires_raw = data.get("autotrade_expires_at") if autotrade_expires_present else None
            autotrade_expires = _parse_iso_dt(autotrade_expires_raw) if autotrade_expires_raw not in (None, "") else None
            if autotrade_expires_present and autotrade_expires_raw not in (None, "") and autotrade_expires is None:
                raise ValueError("bad autotrade_expires_at")
            autotrade_add_days = int(data.get("autotrade_add_days") or 0)
            touch_autotrade = bool(autotrade_enabled_present or autotrade_expires_present or autotrade_add_days)

            referral_enabled_present = ("referral_enabled" in data)
            referral_enabled = bool(data.get("referral_enabled")) if referral_enabled_present else False

            is_blocked_present = ("is_blocked" in data) or ("blocked" in data)
            is_blocked = bool(data.get("is_blocked", data.get("blocked"))) if is_blocked_present else None

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO users (telegram_id, notify_signals)
                    VALUES ($1, TRUE)
                    ON CONFLICT (telegram_id) DO NOTHING
                    """,
                    tid,
                )
                if touch_signal:
                    if signal_add_days:
                        await conn.execute(
                            """UPDATE users
                                   SET signal_enabled = TRUE,
                                       signal_expires_at = COALESCE(signal_expires_at, now()) + make_interval(days => $2)
                                 WHERE telegram_id=$1""",
                            tid, signal_add_days,
                        )
                    elif signal_enabled_present and signal_expires_present:
                        await conn.execute(
                            """UPDATE users
                                   SET signal_enabled=$2,
                                       signal_expires_at=$3::timestamptz
                                 WHERE telegram_id=$1""",
                            tid, signal_enabled, signal_expires,
                        )
                    elif signal_expires_present:
                        await conn.execute(
                            "UPDATE users SET signal_expires_at=$2::timestamptz WHERE telegram_id=$1",
                            tid, signal_expires,
                        )
                    elif signal_enabled_present:
                        await conn.execute(
                            "UPDATE users SET signal_enabled=$2 WHERE telegram_id=$1",
                            tid, signal_enabled,
                        )

                if touch_autotrade:
                    if autotrade_add_days:
                        await conn.execute(
                            """UPDATE users
                                   SET autotrade_enabled = TRUE,
                                       autotrade_stop_after_close = FALSE,
                                       autotrade_expires_at = COALESCE(autotrade_expires_at, now()) + make_interval(days => $2)
                                 WHERE telegram_id=$1""",
                            tid, autotrade_add_days,
                        )
                    elif autotrade_enabled_present:
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
                                if autotrade_expires_present:
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
                                if autotrade_expires_present:
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
                            if autotrade_expires_present:
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
                    elif autotrade_expires_present:
                        await conn.execute(
                            "UPDATE users SET autotrade_expires_at=$2::timestamptz WHERE telegram_id=$1",
                            tid, autotrade_expires,
                        )

                if referral_enabled_present:
                    await conn.execute(
                        "UPDATE users SET referral_enabled=$2, updated_at=NOW() WHERE telegram_id=$1",
                        tid, referral_enabled,
                    )

                if is_blocked_present:
                    try:
                        await conn.execute("UPDATE users SET is_blocked=$2 WHERE telegram_id=$1", tid, bool(is_blocked))
                    except Exception:
                        pass

            if referral_enabled_present:
                await _refresh_referral_access_cache(tid)

        async def save_user(request: web.Request) -> web.Response:
            """Create/update Signal + Auto-trade + Referral access for a user."""
            if not _check_basic(request):
                return _unauthorized()
            data = await request.json()
            tid = int(data.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            try:
                await _apply_signal_user_update(tid, data)
            except ValueError as e:
                return web.json_response({"ok": False, "error": str(e)}, status=400)
            return web.json_response({"ok": True})

        async def patch_user(request: web.Request) -> web.Response:
            """Compat endpoint: PATCH /api/infra/admin/signal/users/{telegram_id}."""
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            data = await request.json()
            data = data or {}
            data["telegram_id"] = tid
            try:
                await _apply_signal_user_update(tid, data)
            except ValueError as e:
                return web.json_response({"ok": False, "error": str(e)}, status=400)
            return web.json_response({"ok": True})

        async def block_user(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO users (telegram_id, notify_signals) VALUES ($1, TRUE) ON CONFLICT (telegram_id) DO NOTHING", tid)
                await conn.execute("UPDATE users SET is_blocked=TRUE WHERE telegram_id=$1", tid)
            return web.json_response({"ok": True})

        async def unblock_user(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            tid = int(request.match_info.get("telegram_id") or 0)
            if not tid:
                return web.json_response({"ok": False, "error": "telegram_id required"}, status=400)
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO users (telegram_id, notify_signals) VALUES ($1, TRUE) ON CONFLICT (telegram_id) DO NOTHING", tid)
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
                "referral_enabled": bool(st.get("referral_enabled")),
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
            referral_enabled = bool(data.get("referral_enabled"))
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
                referral_enabled=referral_enabled,
            )
            # Apply immediately without restart
            if support_username is not None:
                global SUPPORT_USERNAME
                SUPPORT_USERNAME = str(support_username).lstrip("@").strip()
            SIGNAL_BOT_GLOBAL["pause_signals"] = pause_signals
            SIGNAL_BOT_GLOBAL["maintenance_mode"] = maintenance_mode
            SIGNAL_BOT_GLOBAL["referral_enabled"] = referral_enabled

            await db_store.set_autotrade_bot_settings(
                pause_autotrade=pause_autotrade,
                maintenance_mode=autotrade_maintenance_mode,
            )
            return web.json_response({"ok": True})

        def _is_signal_admin_photo_file(file_name: str | None, file_content_type: str | None) -> bool:
            if str(file_content_type or '').lower().startswith('image/'):
                return True
            probe = str(file_name or '').lower()
            return probe.endswith(('.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp'))

        async def _read_signal_admin_message_payload(request: web.Request) -> tuple[str, str, str, bytes | None, str | None, str | None, list[str], list[dict[str, object]]]:
            data = {}
            file_bytes: bytes | None = None
            file_name: str | None = None
            file_content_type: str | None = None
            uploaded_files: list[dict[str, object]] = []

            try:
                content_type = str(request.content_type or '').lower()
            except Exception:
                content_type = ''

            if content_type.startswith('multipart/'):
                reader = await request.multipart()
                async for part in reader:
                    name = str(part.name or '').strip()
                    if not name:
                        continue
                    if name in ('file', 'files'):
                        payload_name = part.filename or f'upload_{len(uploaded_files) + 1}.bin'
                        payload_content_type = part.headers.get('Content-Type') if part.headers else None
                        payload_bytes = await part.read(decode=False)
                        if not payload_bytes:
                            continue
                        uploaded_files.append({
                            'bytes': payload_bytes,
                            'name': payload_name,
                            'content_type': payload_content_type,
                        })
                        if file_bytes is None:
                            file_bytes = payload_bytes
                            file_name = payload_name
                            file_content_type = payload_content_type
                    else:
                        try:
                            data[name] = await part.text()
                        except Exception:
                            data[name] = ''
            else:
                try:
                    data = await request.json()
                except Exception:
                    data = {}

            text = str((data or {}).get('text') or '').strip()
            url = str((data or {}).get('url') or '').strip()
            media_type = str((data or {}).get('media_type') or (data or {}).get('type') or 'auto').strip().lower() or 'auto'
            audience_raw = (data or {}).get('audience') or (data or {}).get('statuses') or ['active', 'expired', 'blocked']
            if isinstance(audience_raw, (list, tuple, set)):
                audience = [str(x).strip().lower() for x in audience_raw if str(x).strip()]
            else:
                audience = [x.strip().lower() for x in str(audience_raw).split(',') if x.strip()]
            return text, url, media_type, file_bytes, file_name, file_content_type, audience, uploaded_files

        def _infer_signal_admin_media_type(
            media_type: str,
            url: str,
            file_name: str | None,
            file_content_type: str | None,
            uploaded_files: list[dict[str, object]] | None = None,
        ) -> str:
            media_type = str(media_type or 'auto').strip().lower() or 'auto'
            if media_type in ('text', 'photo', 'document'):
                return media_type
            uploaded_files = list(uploaded_files or [])
            if uploaded_files:
                if all(_is_signal_admin_photo_file(str(item.get('name') or ''), str(item.get('content_type') or '')) for item in uploaded_files):
                    return 'photo'
                return 'document'
            if _is_signal_admin_photo_file(file_name, file_content_type):
                return 'photo'
            probe = str(file_name or url or '').lower()
            if probe:
                return 'document'
            return 'text'

        async def _mark_signal_admin_unreachable(telegram_id: int, exc: Exception) -> None:
            try:
                if isinstance(exc, TelegramForbiddenError):
                    await set_user_blocked(telegram_id, blocked=True)
                    return
                if isinstance(exc, TelegramBadRequest):
                    msg = str(exc).lower()
                    if 'chat not found' in msg or 'bot was blocked by the user' in msg:
                        await set_user_blocked(telegram_id, blocked=True)
            except Exception:
                pass

        def _classify_signal_admin_status(row, now_utc: dt.datetime) -> str:
            try:
                if bool(row.get('is_blocked')):
                    return 'blocked'
            except Exception:
                return 'blocked'
            try:
                enabled = bool(row.get('signal_enabled'))
            except Exception:
                enabled = False
            exp = row.get('signal_expires_at')
            is_active = bool(enabled) and (exp is None or exp > now_utc)
            return 'active' if is_active else 'expired'

        async def _get_signal_admin_user_ids(audience: list[str]) -> list[int]:
            wanted = {str(x).strip().lower() for x in (audience or []) if str(x).strip()}
            wanted &= {'active', 'expired', 'blocked'}
            if not wanted:
                wanted = {'active', 'expired', 'blocked'}
            if not pool:
                return []
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT telegram_id,
                           COALESCE(is_blocked, FALSE) AS is_blocked,
                           COALESCE(signal_enabled, FALSE) AS signal_enabled,
                           signal_expires_at
                    FROM users
                    ORDER BY telegram_id DESC
                    """
                )
            now_utc = dt.datetime.now(dt.timezone.utc)
            out: list[int] = []
            for row in rows:
                status = _classify_signal_admin_status(row, now_utc)
                if status not in wanted:
                    continue
                try:
                    tid = int(row['telegram_id'])
                except Exception:
                    continue
                out.append(tid)
            return out

        async def _send_signal_admin_message(
            telegram_id: int,
            *,
            text: str,
            url: str,
            media_type: str,
            file_bytes: bytes | None,
            file_name: str | None,
            file_content_type: str | None,
            uploaded_files: list[dict[str, object]] | None,
            ctx: str,
        ) -> None:
            text = str(text or '').strip()
            url = str(url or '').strip()
            uploaded_files = list(uploaded_files or [])
            has_media = bool(url or file_bytes or uploaded_files)
            kind = _infer_signal_admin_media_type(media_type, url, file_name, file_content_type, uploaded_files)

            if not has_media or kind == 'text':
                if not text:
                    raise ValueError('text required')
                await safe_send(telegram_id, text, ctx=ctx)
                return

            caption = text or None
            extra_text = None
            if caption and len(caption) > 1024:
                extra_text = caption[1024:].strip() or None
                caption = caption[:1024]

            if len(uploaded_files) > 1:
                if url:
                    raise ValueError('album cannot be combined with url')
                if kind != 'photo':
                    raise ValueError('multiple files are supported only for photo albums')
                if len(uploaded_files) > 10:
                    raise ValueError('photo album supports up to 10 files')
                if not all(_is_signal_admin_photo_file(str(item.get('name') or ''), str(item.get('content_type') or '')) for item in uploaded_files):
                    raise ValueError('photo album supports image files only')

                media = []
                for idx, item in enumerate(uploaded_files):
                    payload = BufferedInputFile(
                        bytes(item.get('bytes') or b''),
                        filename=(str(item.get('name') or '') or f'image_{idx + 1}.jpg'),
                    )
                    media.append(InputMediaPhoto(media=payload, caption=caption if idx == 0 else None))
                await bot.send_media_group(telegram_id, media=media)
                if extra_text:
                    await safe_send(telegram_id, extra_text, ctx=ctx)
                return

            if kind == 'photo':
                photo_payload = BufferedInputFile(file_bytes, filename=(file_name or 'image.jpg')) if file_bytes is not None else url
                await bot.send_photo(telegram_id, photo=photo_payload, caption=caption)
            else:
                document_payload = BufferedInputFile(file_bytes, filename=(file_name or 'file.bin')) if file_bytes is not None else url
                await bot.send_document(telegram_id, document=document_payload, caption=caption)

            if extra_text:
                await safe_send(telegram_id, extra_text, ctx=ctx)

        async def signal_broadcast_text(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            try:
                text, url, media_type, file_bytes, file_name, file_content_type, audience, uploaded_files = await _read_signal_admin_message_payload(request)
            except web.HTTPRequestEntityTooLarge:
                return web.json_response({
                    "ok": False,
                    "error": "payload too large; reduce total upload size or raise SIGNAL_ADMIN_MAX_BODY_MB",
                }, status=413)
            if not text and not url and not file_bytes:
                return web.json_response({"ok": False, "error": "text/url/file required"}, status=400)
            uids = await _get_signal_admin_user_ids(audience)
            total = len(uids)
            sent = 0
            failed = 0
            sample_errors = []
            for uid in uids:
                try:
                    await _send_signal_admin_message(
                        int(uid),
                        text=text,
                        url=url,
                        media_type=media_type,
                        file_bytes=file_bytes,
                        file_name=file_name,
                        file_content_type=file_content_type,
                        uploaded_files=uploaded_files,
                        ctx='signal-admin-broadcast',
                    )
                    sent += 1
                except Exception as e:
                    failed += 1
                    await _mark_signal_admin_unreachable(int(uid), e)
                    if len(sample_errors) < 5:
                        sample_errors.append({"telegram_id": int(uid), "error": str(e)})
            return web.json_response({
                "ok": True,
                "sent": sent,
                "failed": failed,
                "total": total,
                "audience": audience or ['active', 'expired', 'blocked'],
                "sample_errors": sample_errors,
            })

        async def signal_send_text(request: web.Request) -> web.Response:
            if not _check_basic(request):
                return _unauthorized()
            telegram_id_raw = str(request.match_info.get("telegram_id") or "").strip()
            try:
                telegram_id = int(telegram_id_raw)
            except Exception:
                return web.json_response({"ok": False, "error": "invalid telegram_id"}, status=400)

            try:
                text, url, media_type, file_bytes, file_name, file_content_type, _audience, uploaded_files = await _read_signal_admin_message_payload(request)
            except web.HTTPRequestEntityTooLarge:
                return web.json_response({
                    "ok": False,
                    "error": "payload too large; reduce total upload size or raise SIGNAL_ADMIN_MAX_BODY_MB",
                }, status=413)
            if not text and not url and not file_bytes:
                return web.json_response({"ok": False, "error": "text/url/file required"}, status=400)

            try:
                await _send_signal_admin_message(
                    telegram_id,
                    text=text,
                    url=url,
                    media_type=media_type,
                    file_bytes=file_bytes,
                    file_name=file_name,
                    file_content_type=file_content_type,
                    uploaded_files=uploaded_files,
                    ctx="signal-admin-send",
                )
                return web.json_response({"ok": True, "telegram_id": telegram_id})
            except Exception as e:
                await _mark_signal_admin_unreachable(telegram_id, e)
                logger.exception("signal_send_text failed telegram_id=%s", telegram_id)
                return web.json_response({"ok": False, "error": str(e)}, status=500)

        async def payments_create(request: web.Request) -> web.Response:
            data = await request.json()
            tid = int(data.get('telegram_id') or 0)
            plan_code = str(data.get('plan') or '').strip().lower()
            if not tid or plan_code not in SUBSCRIPTION_PLANS:
                return web.json_response({'ok': False, 'error': 'telegram_id/plan required'}, status=400)
            try:
                invoice = await _create_nowpayments_invoice(telegram_id=tid, plan_code=plan_code)
                return web.json_response({'ok': True, 'invoice': invoice})
            except Exception as e:
                logger.exception('payments_create failed')
                return web.json_response({'ok': False, 'error': str(e)}, status=500)

        async def payments_webhook(request: web.Request) -> web.Response:
            raw = await request.text()
            signature = request.headers.get('x-nowpayments-sig') or request.headers.get('X-Nowpayments-Sig')
            if not _verify_nowpayments_signature(raw, signature):
                return web.json_response({'ok': False, 'error': 'bad signature'}, status=403)
            try:
                data = json.loads(raw or '{}')
            except Exception:
                return web.json_response({'ok': False, 'error': 'bad json'}, status=400)
            status = str(data.get('payment_status') or '').lower()
            order_id = str(data.get('order_id') or '').strip()
            provider_payment_id = str(data.get('payment_id') or '').strip() or None
            if not order_id:
                return web.json_response({'ok': False, 'error': 'order_id required'}, status=400)
            if status != 'finished':
                return web.json_response({'ok': True, 'ignored': True, 'status': status})
            order = await db_store.get_subscription_order(order_id)
            if not order and provider_payment_id:
                order = await db_store.get_subscription_order_by_provider_payment_id(provider_payment_id)
            if not order:
                return web.json_response({'ok': False, 'error': 'order not found'}, status=404)
            if not _webhook_amount_matches(order, data):
                logger.warning('payments_webhook: amount mismatch for order=%s expected=%s payload_price=%s payload_pay=%s', order.get('order_id'), order.get('amount'), data.get('price_amount'), data.get('pay_amount'))
                return web.json_response({'ok': False, 'error': 'amount mismatch'}, status=400)
            processed = await db_store.mark_subscription_order_paid(
                order_id=str(order['order_id']),
                provider_payment_id=provider_payment_id,
                status=status,
                txid=(data.get('payin_hash') or data.get('tx_hash') or None),
                pay_currency=(data.get('pay_currency') or data.get('actually_paid_currency') or None),
                pay_amount=(float(data['pay_amount']) if data.get('pay_amount') not in (None, '') else None),
                payload=data,
            )
            if processed:
                await _grant_paid_plan_and_notify(order, data)
            return web.json_response({'ok': True, 'processed': bool(processed)})

        async def payments_export_xlsx(request: web.Request) -> web.StreamResponse:
            if not _check_basic(request):
                return _unauthorized()
            rows = await db_store.list_subscription_payments()
            wb = Workbook()
            ws = wb.active
            ws.title = 'Payments'
            ws.append(['Date','Telegram ID','Plan','Amount','Currency','Pay Currency','Status','TXID','Order ID'])
            revenue = 0.0
            users = set()
            signal_users = set()
            auto_users = set()
            for r in rows:
                paid_at = r.get('paid_at') or r.get('created_at')
                dtv = paid_at.isoformat() if hasattr(paid_at,'isoformat') else str(paid_at or '')
                ws.append([
                    dtv,
                    int(r.get('telegram_id') or 0),
                    r.get('plan') or '',
                    float(r.get('amount') or 0),
                    r.get('currency') or '',
                    r.get('pay_currency') or '',
                    r.get('status') or '',
                    r.get('txid') or '',
                    r.get('order_id') or '',
                ])
                revenue += float(r.get('amount') or 0)
                tid = int(r.get('telegram_id') or 0)
                if tid:
                    users.add(tid)
                    if str(r.get('plan')) == 'signal_pro':
                        signal_users.add(tid)
                    if str(r.get('plan')) == 'auto_pro':
                        auto_users.add(tid)
            ws2 = wb.create_sheet('Statistics')
            ws2.append(['Metric','Value'])
            ws2.append(['TOTAL REVENUE', revenue])
            ws2.append(['TOTAL USERS', len(users)])
            ws2.append(['SIGNAL PRO USERS', len(signal_users)])
            ws2.append(['AUTO PRO USERS', len(auto_users)])
            buf = io.BytesIO()
            wb.save(buf)
            buf.seek(0)
            return web.Response(
                body=buf.getvalue(),
                headers={
                    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    'Content-Disposition': 'attachment; filename="payments.xlsx"',
                },
            )

        # Routes
        # Root endpoints for reverse-proxy / uptime checks.
        app.router.add_route("GET", "/", health)
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
        app.router.add_route("POST", "/api/payments/create", payments_create)
        app.router.add_route("POST", PAYMENT_WEBHOOK_PATH, payments_webhook)
        app.router.add_route("GET", "/api/infra/admin/signal/payments/export.xlsx", payments_export_xlsx)
        app.router.add_route("GET", "/api/infra/admin/signal/stats", signal_stats)
        app.router.add_route("GET", "/api/infra/admin/signal/users", list_users)
        app.router.add_route("POST", "/api/infra/admin/signal/users", save_user)
        app.router.add_route("PATCH", "/api/infra/admin/signal/users/{telegram_id}", patch_user)
        app.router.add_route("POST", "/api/infra/admin/signal/users/{telegram_id}/block", block_user)
        app.router.add_route("POST", "/api/infra/admin/signal/users/{telegram_id}/unblock", unblock_user)
        # Allow preflight
        app.router.add_route("OPTIONS", "/{tail:.*}", lambda r: web.Response(status=204))
        # --- Route aliases for infra panel reverse-proxy (/signal/* prefix) ---
        # Some deployments proxy the Signal Bot under /signal, so the panel calls:
        #   /signal/api/infra/admin/...
        # Add aliases to match both forms.
        def _alias(method: str, path: str, handler):
            try:
                app.router.add_route(method, "/signal" + path, handler)
            except Exception:
                # ignore duplicates
                pass

        for _m, _p, _h in [
            ("GET", "/", health),
            ("GET", "/health", health),

            ("GET", "/api/infra/admin/bot/users", bot_list_users),
            ("POST", "/api/infra/admin/bot/users", bot_create_user),
            ("POST", "/api/infra/admin/bot/users/{telegram_id}/{action}", bot_user_action),

            ("GET", "/api/infra/admin/signal/stats", signal_stats),
            ("GET", "/api/infra/admin/signal/users", list_users),
            ("POST", "/api/infra/admin/signal/users", save_user),
            ("POST", "/api/infra/admin/signal/users/{telegram_id}/block", block_user),
            ("POST", "/api/infra/admin/signal/users/{telegram_id}/unblock", unblock_user),

            ("GET", "/api/infra/admin/signal/settings", signal_get_settings),
            ("POST", "/api/infra/admin/signal/settings", signal_save_settings),

            ("GET", "/api/infra/admin/autotrade/settings", autotrade_get_settings),
            ("POST", "/api/infra/admin/autotrade/settings", autotrade_save_settings),

            ("POST", "/api/infra/admin/signal/broadcast", signal_broadcast_text),
            ("POST", "/api/infra/admin/signal/send/{telegram_id}", signal_send_text),
            ("GET", "/api/infra/admin/signal/payments/export.xlsx", payments_export_xlsx),
        ]:
            _alias(_m, _p, _h)

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

    # --- Singleton (primary) lock ---
    # IMPORTANT: advisory lock is held on a dedicated DB connection for the lifetime of the process.
    # If you acquire it inside a context manager and release the connection, the lock is released too,
    # which would let multiple replicas run background loops at the same time.
    
    # We use a **two-layer** leader election to be robust across different Postgres setups:
    #   1) Prefer session-level advisory lock (fast, no extra table) when it works.
    #   2) Fallback to a heartbeat-based lease row (works even behind PgBouncer transaction pooling).
    #
    # Why this matters: some managed Postgres deployments route connections through PgBouncer in
    # transaction pooling mode, which can make session-level advisory locks unreliable. The lease
    # fallback ensures we always get exactly one PRIMARY replica.
    PRIMARY_LOCK_KEY = int(os.getenv("PRIMARY_LOCK_KEY", "8313103750"))
    PRIMARY_LEASE_NAME = os.getenv("PRIMARY_LEASE_NAME", "crypto_signal_primary").strip() or "crypto_signal_primary"
    PRIMARY_LEASE_TTL_SEC = int(os.getenv("PRIMARY_LEASE_TTL_SEC", "45"))
    PRIMARY_HEARTBEAT_SEC = int(os.getenv("PRIMARY_HEARTBEAT_SEC", "15"))

    _leader_heartbeat_task: asyncio.Task | None = None

    async def _acquire_primary_lock() -> tuple[bool, object | None]:
        # 1) Try advisory lock on a dedicated connection kept for process lifetime.
        try:
            conn = await pool.acquire()
            got = await conn.fetchval(f"SELECT pg_try_advisory_lock({PRIMARY_LOCK_KEY})")
            if got:
                logger.info("Primary election: acquired advisory lock key=%s", PRIMARY_LOCK_KEY)
                return True, conn  # keep conn open (do NOT release)
            try:
                await pool.release(conn)
            except Exception:
                pass
            logger.warning("Primary election: advisory lock key=%s NOT acquired (maybe held elsewhere). Falling back to lease.", PRIMARY_LOCK_KEY)
        except Exception as e:
            logger.warning("Primary election: advisory lock failed (%s). Falling back to lease.", e)

        # 2) Fallback: heartbeat lease row (single-writer) with TTL.
        holder = (
            os.getenv("RAILWAY_REPLICA_ID")
            or os.getenv("REPLICA_ID")
            or os.getenv("HOSTNAME")
            or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        )
        try:
            conn2 = await pool.acquire()
            # Ensure lease table exists (lightweight, once per boot).
            await conn2.execute(
                '''
                CREATE TABLE IF NOT EXISTS infra_primary_leases (
                    name TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                '''
            )

            # Try to become leader:
            # - Insert if empty
            # - Or steal if heartbeat is older than TTL
            # - Or refresh if we already are the holder
            row = await conn2.fetchrow(
                '''
                INSERT INTO infra_primary_leases(name, holder, heartbeat)
                VALUES($1, $2, NOW())
                ON CONFLICT (name) DO UPDATE
                  SET holder = EXCLUDED.holder,
                      heartbeat = NOW()
                WHERE infra_primary_leases.holder = EXCLUDED.holder
                   OR infra_primary_leases.heartbeat < (NOW() - ($3::int * INTERVAL '1 second'))
                RETURNING holder;
                ''',
                PRIMARY_LEASE_NAME,
                holder,
                PRIMARY_LEASE_TTL_SEC,
            )

            is_leader = bool(row) and (row["holder"] == holder)
            if not is_leader:
                try:
                    await pool.release(conn2)
                except Exception:
                    pass
                logger.warning("Primary election: lease NOT acquired (name=%s ttl=%ss). This replica is WORKER. holder=%s", PRIMARY_LEASE_NAME, PRIMARY_LEASE_TTL_SEC, holder)
                return False, None

            logger.info("Primary election: lease acquired (name=%s ttl=%ss heartbeat=%ss) holder=%s", PRIMARY_LEASE_NAME, PRIMARY_LEASE_TTL_SEC, PRIMARY_HEARTBEAT_SEC, holder)

            async def _heartbeat_loop() -> None:
                nonlocal conn2
                while True:
                    try:
                        # Keep the lease alive. If we fail to update, we should drop out of PRIMARY.
                        res = await conn2.execute(
                            '''
                            UPDATE infra_primary_leases
                               SET heartbeat = NOW()
                             WHERE name = $1 AND holder = $2
                            ''',
                            PRIMARY_LEASE_NAME,
                            holder,
                        )
                        # asyncpg returns "UPDATE <n>"
                        if not res.endswith(" 1"):
                            logger.error("Primary election: LOST lease (name=%s). Stopping process to allow re-election.", PRIMARY_LEASE_NAME)
                            os._exit(0)
                    except Exception as e:
                        logger.error("Primary election: heartbeat error (%s). Stopping process to allow re-election.", e)
                        os._exit(0)
                    await asyncio.sleep(PRIMARY_HEARTBEAT_SEC)

            # Start heartbeat task and keep the lease connection open too.
            nonlocal _leader_heartbeat_task
            _leader_heartbeat_task = asyncio.create_task(_heartbeat_loop(), name="primary_lease_heartbeat")
            return True, conn2  # keep conn open (do NOT release)

        except Exception as e:
            # Lease election failure: fail OPEN (become primary) would be dangerous.
            # Better to become WORKER and keep webhook alive.
            logger.error("Primary election: lease mechanism failed (%s). Running as WORKER.", e)
            return False, None

    is_primary, _primary_lock_conn = await _acquire_primary_lock()

    # --- Webhook routing (enabled on ALL replicas) ---
    WEBHOOK_MODE = os.getenv("WEBHOOK_MODE", "0").strip().lower() in ("1", "true", "yes", "on")
    WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip() or "/webhook"
    WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
    WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "").strip()

    if WEBHOOK_MODE:
        # --- Webhook setup (idempotent; only PRIMARY calls setWebhook) ---
        if not WEBHOOK_BASE_URL:
            logger.error("WEBHOOK_MODE=1 but WEBHOOK_BASE_URL is empty; webhook will not be set.")
        else:
            webhook_url = WEBHOOK_BASE_URL.rstrip("/") + WEBHOOK_PATH
            if is_primary:
                try:
                    await _ensure_webhook(bot, webhook_url, secret_token=(WEBHOOK_SECRET_TOKEN or None))
                except Exception as e:
                    logger.exception("Failed to ensure Telegram webhook: %s", e)
            else:
                logger.info("Not primary -> skip setWebhook (webhook routing still active).")

        # --- Start aiohttp webhook server (required in WEBHOOK_MODE) ---
        try:
            port = int(os.getenv("PORT", "8080"))
        except Exception:
            port = 8080
        app = await _admin_http_app()
        # Health endpoint
        # Telegram webhook handler
        SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=(WEBHOOK_SECRET_TOKEN or None)).register(app, path=WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=port)
        await site.start()
        logger.info("Webhook HTTP server started on 0.0.0.0:%s path=%s", port, WEBHOOK_PATH)

        # --- Background loops ---
        if not is_primary:
            logger.warning("Webhook WORKER replica started: skipping background loops (scanner/track/outcomes).")
        else:
            logger.info("Starting track_loop")
            if _spawn_smart_manager_task() is None:
                logger.warning("Backend has no track_loop; skipping")

            TASKS["db-retention"] = asyncio.create_task(_db_retention_cleanup_loop(), name="db-retention")
            _attach_task_monitor("db-retention", TASKS["db-retention"])

            logger.info(
                "Starting scanner_loop (15m/1h/4h) interval=%ss top_n=%s",
                os.getenv("SCAN_INTERVAL_SECONDS", ""),
                os.getenv("TOP_N", ""),
            )

            # Some deployments (MID-only builds) intentionally don't ship the legacy scanner_loop.
            # Guard this call to avoid crashing the whole process.
            if hasattr(backend, "scanner_loop"):
                try:
                    TASKS["scanner"] = asyncio.create_task(
                        backend.scanner_loop(broadcast_signal, broadcast_macro_alert),
                        name="scanner",
                    )
                    _health_mark_ok("scanner")
                    _attach_task_monitor("scanner", TASKS["scanner"])
                except Exception:
                    logger.exception("Failed to start scanner_loop")
            else:
                logger.warning(
                    "Backend has no scanner_loop; skipping (MID scanner runs via scanner_loop_mid)"
                )

            # MID components (scanner + pending loop)
            _start_mid_components(backend, broadcast_signal, broadcast_macro_alert)

            logger.info("Starting signal_outcome_loop")
            _start_signal_outcome_task()

            TASKS["daily-signal-report"] = asyncio.create_task(daily_signal_report_loop(), name="daily-signal-report")
            _attach_task_monitor("daily-signal-report", TASKS["daily-signal-report"])

        # Auto-trade manager can run on all replicas (cluster-safe via DB lease/locks)
        TASKS["autotrade-manager"] = asyncio.create_task(
            autotrade_manager_loop(notify_api_error=_notify_autotrade_api_error, notify_smart_event=_notify_smart_manager_event, backend_instance=backend),
            name="autotrade-manager",
        )
        _health_mark_ok("autotrade-manager")
        _attach_task_monitor("autotrade-manager", TASKS["autotrade-manager"])
        TASKS["autotrade-manager-heartbeat"] = asyncio.create_task(
            _task_heartbeat_loop("autotrade-manager", interval_sec=5.0),
            name="autotrade-manager-heartbeat",
        )

        TASKS["autotrade-anomaly-watchdog"] = asyncio.create_task(
            autotrade_anomaly_watchdog_loop(notify_api_error=_notify_autotrade_api_error),
            name="autotrade-anomaly-watchdog",
        )
        _health_mark_ok("autotrade-anomaly-watchdog")
        _attach_task_monitor("autotrade-anomaly-watchdog", TASKS["autotrade-anomaly-watchdog"])

        TASKS["health-status"] = asyncio.create_task(_health_status_loop(), name="health-status")
        _attach_task_monitor("health-status", TASKS["health-status"])

        logger.info("Webhook mode active; waiting for HTTP requests")
        await asyncio.Event().wait()
    # --- Polling mode (legacy) ---
    # Start admin HTTP API on all replicas
    asyncio.create_task(_start_http_server())

    if not is_primary:
        logger.warning(
            "Another instance holds the Telegram polling lock; this replica will run ONLY autotrade manager + HTTP (no polling/scanner)."
        )
        try:
            TASKS["autotrade-manager"] = asyncio.create_task(autotrade_manager_loop(notify_api_error=_notify_autotrade_api_error, notify_smart_event=_notify_smart_manager_event, backend_instance=backend), name="autotrade-manager")
            _health_mark_ok("autotrade-manager")
            TASKS["autotrade-anomaly-watchdog"] = asyncio.create_task(autotrade_anomaly_watchdog_loop(notify_api_error=_notify_autotrade_api_error), name="autotrade-anomaly-watchdog")
            _health_mark_ok("autotrade-anomaly-watchdog")
        except Exception:
            pass
        await asyncio.Event().wait()

    logger.info("Starting track_loop")
    if _spawn_smart_manager_task() is None:
        logger.warning("Backend has no track_loop; skipping")
    TASKS["db-retention"] = asyncio.create_task(_db_retention_cleanup_loop(), name="db-retention")
    _attach_task_monitor("db-retention", TASKS["db-retention"])
    logger.info("Starting scanner_loop (15m/1h/4h) interval=%ss top_n=%s", os.getenv('SCAN_INTERVAL_SECONDS',''), os.getenv('TOP_N',''))
    asyncio.create_task(backend.scanner_loop(broadcast_signal, broadcast_macro_alert))

    # ⚡ MID TREND scanner + MID trap digest
    _start_mid_components(backend, broadcast_signal, broadcast_macro_alert)

    logger.info("Starting signal_outcome_loop")
    _start_signal_outcome_task()

    TASKS["daily-signal-report"] = asyncio.create_task(daily_signal_report_loop(), name="daily-signal-report")
    _attach_task_monitor("daily-signal-report", TASKS["daily-signal-report"])

    # Auto-trade manager (SL/TP/BE) - runs in background.
    asyncio.create_task(autotrade_manager_loop(notify_api_error=_notify_autotrade_api_error, notify_smart_event=_notify_smart_manager_event, backend_instance=backend))
    asyncio.create_task(autotrade_anomaly_watchdog_loop(notify_api_error=_notify_autotrade_api_error))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


# ===============================
# AUTO SYMBOL ANALYSIS HANDLER
# ===============================

