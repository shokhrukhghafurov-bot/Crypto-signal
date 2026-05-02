
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
import aiohttp
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


def _env_on(name: str, default: str = "1") -> bool:
    return str(os.getenv(name, default) or default).strip().lower() not in ("0", "false", "no", "off")

HEALTH: dict[str, float] = {"bot_start": time.time()}
HEALTH_LAST_ERR: dict[str, str] = {}

def _health_mark_ok(name: str) -> None:
    HEALTH[f"{name}_last_ok"] = time.time()

def _health_mark_err(name: str, err: str) -> None:
    HEALTH_LAST_ERR[name] = (err or "").strip()[:500]
    HEALTH[f"{name}_last_err"] = time.time()

def _cost_saver_mode() -> bool:
    return str(os.getenv("COST_SAVER_MODE", "0") or "0").strip().lower() in ("1", "true", "yes", "on")

def _cost_default(on_value: str = "1", off_value: str = "0") -> str:
    return off_value if _cost_saver_mode() else on_value

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
    smart_default = _cost_default("1", "0")
    if not _env_on("SMART_MANAGER_ENABLED", smart_default):
        logger.info("SMART_MANAGER_ENABLED=0 -> smart-manager/track_loop disabled")
        return None
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


def _start_signal_outcome_task() -> asyncio.Task | None:
    # The outcome loop is the source of closed-signal report cards. Keep it ON
    # by default even in COST_SAVER_MODE; disable only with SIGNAL_OUTCOME_LOOP_ENABLED=0.
    if not _env_on("SIGNAL_OUTCOME_LOOP_ENABLED", "1"):
        logger.info("SIGNAL_OUTCOME_LOOP_ENABLED=0 -> signal_outcome_loop disabled")
        return None
    existing = TASKS.get("signal-outcome")
    if existing and not existing.done():
        logger.warning("signal_outcome_loop already running; skip duplicate start")
        return existing
    task = asyncio.create_task(signal_outcome_loop(), name="signal-outcome")
    TASKS["signal-outcome"] = task
    _attach_task_monitor("signal-outcome", task)
    return task


async def _restart_task_later(name: str, reason: str = "") -> None:
    """Restart critical background tasks after an unexpected stop.

    Railway redeploys / transient exceptions can kill a background task while the
    bot process keeps serving HTTP. The closed-position report depends on
    signal-outcome + report-retry, so both are restarted automatically.
    """
    restartable = {"smart-manager", "signal-outcome", "closed-signal-report-retry", "daily-signal-report"}
    if name not in restartable:
        return
    if name in _TASK_RESTART_INFLIGHT:
        return
    _TASK_RESTART_INFLIGHT.add(name)
    try:
        delay_default = "5"
        delay = max(1.0, float(os.getenv(f"{name.upper().replace('-', '_')}_RESTART_DELAY_SEC", delay_default) or delay_default))
        await asyncio.sleep(delay)
        cur = TASKS.get(name)
        if cur and not cur.done():
            return

        logger.warning("[task] restarting name=%s after %s", name, reason or "unexpected stop")
        if name == "smart-manager":
            _spawn_smart_manager_task()
        elif name == "signal-outcome":
            _start_signal_outcome_task()
        elif name == "closed-signal-report-retry":
            if _env_on("CLOSED_SIGNAL_REPORT_RETRY_ENABLED", "1"):
                task = asyncio.create_task(closed_signal_report_retry_loop(), name="closed-signal-report-retry")
                TASKS["closed-signal-report-retry"] = task
                _attach_task_monitor("closed-signal-report-retry", task)
        elif name == "daily-signal-report":
            if _env_on("DAILY_SIGNAL_REPORT_ENABLED", "1"):
                task = asyncio.create_task(daily_signal_report_loop(), name="daily-signal-report")
                TASKS["daily-signal-report"] = task
                _attach_task_monitor("daily-signal-report", task)
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
            sig_outcome = TASKS.get("signal-outcome")
            report_retry = TASKS.get("closed-signal-report-retry")
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
            sig_ok = age("signal-outcome_last_ok")
            rep_ok = age("closed-signal-report-retry_last_ok")

            msg = (
                "[health] "
                f"{'✅' if status(at_global, at_ok)=='RUNNING' else '❌'} autotrade=\"{status(at_global, at_ok)}\" last_ok={fmt_age(at_ok)} "
                f"| {'✅' if status(at_mgr, at_mgr_ok)=='RUNNING' else '❌'} autotrade_mgr=\"{status(at_mgr, at_mgr_ok)}\" last_ok={fmt_age(at_mgr_ok)} "
                f"| {'✅' if status(smart, sm_ok)=='RUNNING' else '❌'} smart_manager=\"{status(smart, sm_ok)}\" last_ok={fmt_age(sm_ok)} "
                f"| {'✅' if status(sig_outcome, sig_ok)=='RUNNING' else '❌'} signal_outcome=\"{status(sig_outcome, sig_ok)}\" last_ok={fmt_age(sig_ok)} "
                f"| {'✅' if status(report_retry, rep_ok)=='RUNNING' else '❌'} report_retry=\"{status(report_retry, rep_ok)}\" last_ok={fmt_age(rep_ok)}"
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
                    if name in ("smart-manager", "signal-outcome", "closed-signal-report-retry", "daily-signal-report"):
                        _health_mark_err(name, "task_cancelled")
                        try:
                            asyncio.create_task(_restart_task_later(name, reason="cancelled"))
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
                    if name in ("signal-outcome", "closed-signal-report-retry", "daily-signal-report"):
                        _health_mark_err(name, f"task_crash:{type(exc).__name__}")
                        try:
                            asyncio.create_task(_restart_task_later(name, reason=f"exception:{type(exc).__name__}"))
                        except Exception:
                            pass
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

# Dedicated report sender for closed signal cards.
# It sends formatted outcome cards (WIN / LOSS / BE / CLOSED) only after a
# signal track reaches a final state.
REPORT_BOT_TOKEN = (os.getenv("REPORT_BOT_TOKEN") or "").strip()


def _parse_report_chat_ids() -> list[int]:
    """Read report chat ids from all supported env aliases.

    Old deployments used different variable names. Supporting aliases prevents
    the report card from silently disappearing when only REPORT_CHAT_ID or
    SIGNAL_REPORT_CHAT_ID is set in Railway.
    """
    names = (
        "REPORT_BOT_CHAT_IDS",
        "REPORT_BOT_CHAT_ID",
        "REPORT_CHAT_IDS",
        "REPORT_CHAT_ID",
        "SIGNAL_REPORT_CHAT_IDS",
        "SIGNAL_REPORT_CHAT_ID",
        "CLOSED_SIGNAL_REPORT_CHAT_IDS",
        "CLOSED_SIGNAL_REPORT_CHAT_ID",
    )
    out: list[int] = []
    seen: set[int] = set()
    for name in names:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            continue
        for part in raw.replace(";", ",").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                cid = int(part)
            except Exception:
                logger.warning("[report-bot] invalid chat id in %s: %r", name, part)
                continue
            if cid not in seen:
                out.append(cid)
                seen.add(cid)
    if not out and ADMIN_IDS:
        # Safe default: send reports to admins when a separate report chat is not configured.
        for cid in ADMIN_IDS:
            try:
                cid_i = int(cid)
            except Exception:
                continue
            if cid_i not in seen:
                out.append(cid_i)
                seen.add(cid_i)
    return out


REPORT_BOT_CHAT_IDS: list[int] = _parse_report_chat_ids()
REPORT_BOT_USE_MAIN_FALLBACK = _env_on("REPORT_BOT_USE_MAIN_FALLBACK", "1")

_report_bot: Bot | None = None
_report_bot_is_main = False
if REPORT_BOT_TOKEN:
    try:
        _report_bot = Bot(REPORT_BOT_TOKEN)
        logger.info("[report-bot] dedicated bot enabled chats=%s", REPORT_BOT_CHAT_IDS)
    except Exception:
        _report_bot = None
        logger.exception("[report-bot] failed to initialize dedicated report bot")
        if REPORT_BOT_USE_MAIN_FALLBACK:
            _report_bot = bot
            _report_bot_is_main = True
            logger.warning("[report-bot] fallback to main BOT_TOKEN after dedicated report bot init failed chats=%s", REPORT_BOT_CHAT_IDS)
elif REPORT_BOT_USE_MAIN_FALLBACK:
    # Do not drop cards just because a separate REPORT_BOT_TOKEN was not set.
    # The main bot can send the same closed-position report to REPORT_* chats/admins.
    _report_bot = bot
    _report_bot_is_main = True
    logger.warning("[report-bot] REPORT_BOT_TOKEN is not set; using main BOT_TOKEN for closed signal report cards chats=%s", REPORT_BOT_CHAT_IDS)
else:
    logger.info("[report-bot] REPORT_BOT_TOKEN is not set and fallback disabled; closed signal report cards are disabled")


async def _report_send_message(chat_id: int, text: str) -> None:
    """Send a report message with main-bot fallback.

    Some deployments set REPORT_BOT_TOKEN to a wrong bot or forget to add that
    bot to the report chat. In that case, do not lose the final card if the main
    bot can send it.
    """
    if _report_bot is None:
        raise RuntimeError("report bot is not configured")
    try:
        await _report_bot.send_message(int(chat_id), text)
        return
    except TelegramRetryAfter:
        raise
    except Exception as primary_err:
        if REPORT_BOT_USE_MAIN_FALLBACK and not _report_bot_is_main:
            try:
                await bot.send_message(int(chat_id), text)
                logger.warning("[report-bot] sent via main BOT_TOKEN fallback chat_id=%s after primary error=%s", chat_id, type(primary_err).__name__)
                return
            except Exception as fallback_err:
                raise fallback_err from primary_err
        raise


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
    mid_enabled = os.getenv('MID_SCANNER_ENABLED', _cost_default('1', '0')).strip().lower() not in ('0', 'false', 'no', 'off')
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
    interval_sec = max(10.0, float(os.getenv("AUTOTRADE_GLOBAL_REFRESH_SEC", "60") or 60))
    logger.info("[autotrade-global] loop started interval=%ss", interval_sec); _health_mark_ok("autotrade-global")
    while True:
        tick += 1
        try:
            await _refresh_autotrade_bot_global_once(); _health_mark_ok("autotrade-global")
        except Exception:
            # keep loop alive
            logger.exception("[autotrade-global] refresh failed tick=%s", tick); _health_mark_err("autotrade-global", "refresh_failed")
        await asyncio.sleep(interval_sec)


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


def _report_close_emoji(final_status: str, *, after_tp1: bool = False, pnl_total_pct: float | int | None = None) -> str:
    st = str(final_status or "").upper().strip()
    if after_tp1:
        # TP1-hit trades are not ordinary red LOSS cards. The title emoji follows
        # the total realised result: green for positive partial win, white for flat,
        # yellow when the remainder after TP1 still made the total negative.
        try:
            pnl = float(pnl_total_pct or 0.0)
        except Exception:
            pnl = 0.0
        if pnl > 0.0001:
            return "🟢"
        if pnl < -0.0001:
            return "🟡"
        return "⚪️"
    return {
        "WIN": "🟢",
        "LOSS": "🔴",
        "BE": "⚪️",
        "CLOSED": "🟡",
    }.get(st, "⚪️")


def _report_close_status(final_status: str, *, after_tp1: bool = False, pnl_total_pct: float | int | None = None) -> str:
    st = str(final_status or "").upper().strip() or "CLOSED"
    if after_tp1:
        try:
            pnl = float(pnl_total_pct or 0.0)
        except Exception:
            pnl = 0.0
        if st == "WIN":
            return "TP1 HIT → TP2 WIN"
        if st == "BE":
            return "TP1 HIT → BE remainder"
        if st == "LOSS":
            if pnl > 0.0001:
                return "TP1 HIT → BE/SL remainder"
            if pnl < -0.0001:
                return "TP1 HIT → SL remainder"
            return "TP1 HIT → BE remainder"
    if st == "CLOSED":
        return "CLOSED"
    return st


def _report_pnl_pct(pnl_total_pct: float | int | None) -> str:
    try:
        return f"{float(pnl_total_pct or 0.0):+.1f}%"
    except Exception:
        return "+0.0%"


def _report_rr_str(entry: float, sl: float, tp1: float, tp2: float) -> str:
    """Closed-card RR.

    The report card prints TP1 on every LOSS card, so RR must be calculated to TP1
    by default. Otherwise the card can show 1:2.40 while the printed TP1 is only
    around 1:1.40. Set LOSS_CARD_RR_TARGET=TP2 only if you intentionally want
    final-target RR in closed reports.
    """
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
        rr_target = str(os.getenv('LOSS_CARD_RR_TARGET', 'TP1') or 'TP1').strip().upper()
        target = tp2 if rr_target == 'TP2' and tp2 > 0 and abs(tp2 - tp1) > 1e-12 else tp1
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
            return "TP1 был взят, затем остаток вернулся к BE/SL — это не LOSS до первой цели."
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
    snap = {}
    try:
        snap = _loss_diag_load_entry_snapshot(row)
    except Exception:
        snap = {}
    for candidate in (
        row.get('emit_route'),
        row.get('smc_setup_route'),
        row.get('setup_route'),
        row.get('route_key'),
        (snap.get('route_key') if isinstance(snap, dict) else None),
        (snap.get('smc_setup_route') if isinstance(snap, dict) else None),
        (snap.get('emit_route') if isinstance(snap, dict) else None),
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
        'avoid_support_short': 'не шортить прямо в strong low / demand',
        'require_clean_space_to_tp': 'требовать чистое пространство до TP1/TP2',
        'require_bullish_reclaim': 'для LONG требовать bullish reclaim перед входом',
        'avoid_failed_demand_long': 'не покупать demand после bearish BOS без reclaim',
        'avoid_overhead_bearish_fvg': 'не брать LONG, если сверху stacked bearish FVG',
        'avoid_failed_supply_short': 'не шортить supply после bullish BOS без bearish reclaim',
        'require_bearish_reclaim': 'для SHORT требовать bearish reclaim перед входом',
        'avoid_underlying_bullish_fvg': 'не брать SHORT, если снизу stacked bullish FVG / demand',
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
        'long_failed_demand_bearish_structure': 'Long into bearish BOS / failed demand',
        'failed_demand_scenario': 'Failed demand under bearish structure',
        'failed_demand_reaction': 'Failed demand reaction',
        'long_against_bearish_structure': 'Long against bearish structure',
        'overhead_bearish_fvg': 'Overhead bearish FVG / supply',
        'short_failed_supply_bullish_structure': 'Short into bullish BOS / failed supply',
        'failed_supply_scenario': 'Failed supply under bullish structure',
        'failed_supply_reaction': 'Failed supply reaction',
        'short_against_bullish_structure': 'Short against bullish structure',
        'underlying_bullish_fvg': 'Underlying bullish FVG / demand',
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
        'entry_into_supply': 'Entry into supply / seller zone',
        'entry_into_demand': 'Entry into demand / buyer zone',
        'entry_near_opposite_zone': 'Entry too close to opposite reaction zone',
        'long_below_strong_high': 'Long entered directly into supply / Strong High',
        'short_above_weak_low': 'Short entered directly into demand / Strong Low',
        'short_into_demand_scenario': 'Late SHORT into buyer zone',
        'long_into_supply_scenario': 'Late LONG into seller zone',
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
        'long_failed_demand_bearish_structure': 'Long into bearish BOS / failed demand: LONG был открыт после bearish BOS/CHoCH, рядом с уже ослабленным demand. Покупатель не сделал reclaim/continuation, поэтому зона быстро сломалась.',
        'failed_demand_scenario': 'LONG был открыт после того, как рынок уже перешёл в bearish-структуру. Цена находилась рядом с buyer zone, но сверху уже давили bearish FVG / supply зоны. Покупатель не смог сделать reclaim и continuation, поэтому demand быстро сломался.',
        'failed_demand_reaction': 'Failed demand reaction: buyer zone не дала нормальной реакции после входа и быстро была продавлена.',
        'long_against_bearish_structure': 'Long against bearish structure: LONG был открыт против активной bearish-структуры без подтверждённого bullish reclaim.',
        'overhead_bearish_fvg': 'Overhead bearish FVG / supply: над входом оставались seller imbalance / FVG зоны, которые давили на LONG.',
        'short_failed_supply_bullish_structure': 'Short into bullish BOS / failed supply: SHORT был открыт после bullish BOS/CHoCH рядом с уже ослабленной supply. Продавец не сделал bearish reclaim/continuation, поэтому seller zone быстро сломалась.',
        'failed_supply_scenario': 'SHORT был открыт после того, как рынок уже перешёл в bullish-структуру. Цена находилась рядом с seller zone, но снизу уже давили bullish FVG / demand зоны. Продавец не смог сделать bearish reclaim и continuation, поэтому supply быстро сломался.',
        'failed_supply_reaction': 'Failed supply reaction: seller zone не дала нормальной реакции после входа и быстро была выкуплена.',
        'short_against_bullish_structure': 'Short against bullish structure: SHORT был открыт против активной bullish-структуры без подтверждённого bearish reclaim.',
        'underlying_bullish_fvg': 'Underlying bullish FVG / demand: под входом оставались buyer imbalance / FVG зоны, которые поддерживали цену против SHORT.',
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
        'entry_into_supply': 'Entry into supply: LONG был открыт прямо под seller zone / Strong High. Сверху не было чистого пространства до TP, поэтому вероятность rejection вниз была высокой.',
        'entry_into_demand': 'Entry into demand: SHORT был открыт прямо над buyer zone / Strong Low. Снизу не было чистого пространства до TP, поэтому вероятность bounce вверх была высокой.',
        'entry_near_opposite_zone': 'Entry too close to opposite reaction zone: вход был рядом с зоной, откуда цена могла сразу отреагировать против сделки.',
        'long_below_strong_high': 'Long entered directly into supply: LONG был открыт слишком высоко, прямо под seller zone / Strong High. Цена уже находилась в зоне реакции продавцов, поэтому upside был ограничен, а вероятность rejection вниз была высокой.',
        'short_above_weak_low': 'Short entered directly into demand: SHORT был открыт слишком низко, прямо над buyer zone / Strong Low. Цена уже находилась в зоне реакции покупателей, поэтому downside был ограничен, а вероятность bounce вверх была высокой.',
        'short_into_demand_scenario': 'SHORT был открыт слишком низко, прямо над buyer zone и Strong/Weak Low. Цена уже находилась в зоне реакции покупателей, поэтому downside был ограничен, а вероятность bounce вверх была высокой.',
        'long_into_supply_scenario': 'LONG был открыт слишком высоко, прямо под seller zone и Strong High. Цена уже находилась в зоне реакции продавцов, поэтому upside был ограничен, а вероятность rejection вниз была высокой.',
        'entry_in_range_premium': 'Entry in bad range location: вход был в плохой части range — слишком близко к противоположной ликвидности/стене, а не от дисконта/премиума с запасом хода.',
        'late_entry_after_exhausted_move': 'Late entry after exhausted move: вход был поздний — основное движение уже прошло, retest был несвежий, нового displacement перед входом не было.',
        'no_clean_retest_acceptance': 'No clean retest acceptance: формальный retest был, но цена не закрепилась по правильную сторону зоны/уровня и не показала acceptance перед continuation.',
    }
    return mapping.get(str(code or '').strip(), fallback or _loss_diag_reason_human_label(code))


def _loss_diag_chart_visible_lines(analysis: dict, side: str) -> list[str]:
    """Human chart facts for LOSS report. These lines explain what was visible on the chart at entry."""
    side_u = str(side or '').upper().strip()
    lines: list[str] = []
    if side_u == 'LONG' and bool(analysis.get('long_failed_demand_bearish_structure')):
        lines.extend([
            'LONG открыт после bearish BOS / CHoCH вниз',
            'сверху seller pressure / resistance zones',
            'структура уже делала lower high / lower low',
            'вход был слишком низко, но без подтверждения reclaim',
            'цена стояла под sell pressure',
            'demand/support снизу был уже слабый',
            'RR выглядел формально нормальный, но location был плохой',
            'это не чистый breakout, а попытка купить слабый demand против структуры',
        ])
        if analysis.get('bearish_context_before_entry'):
            lines.append('перед входом уже был bearish displacement / BOS вниз')
            lines.append('bullish reclaim перед LONG не появился')
        if analysis.get('demand_near_tfs'):
            try:
                lines.append('buyer zone / demand был рядом на TF: ' + '/'.join([str(x) for x in list(analysis.get('demand_near_tfs') or [])]))
            except Exception:
                pass
        if analysis.get('supply_near_tfs'):
            try:
                lines.append('overhead bearish FVG / supply виден на TF: ' + '/'.join([str(x) for x in list(analysis.get('supply_near_tfs') or [])]))
            except Exception:
                pass
    if side_u == 'SHORT' and bool(analysis.get('short_failed_supply_bullish_structure')):
        lines.extend([
            'SHORT открыт после bullish BOS / CHoCH вверх',
            'снизу buyer pressure / demand/support zones',
            'структура уже делала higher low / higher high',
            'вход был рядом с supply, но без подтверждения bearish reclaim',
            'цена стояла под buyer pressure',
            'supply/resistance сверху был уже слабый',
            'RR выглядел формально нормальный, но location был плохой',
            'это не чистый SHORT, а попытка шортить слабую supply против bullish-структуры',
        ])
        if analysis.get('bullish_context_before_entry'):
            lines.append('перед входом уже был bullish reclaim / impulse вверх')
            lines.append('bearish reclaim перед SHORT не появился')
        if analysis.get('supply_near_tfs'):
            try:
                lines.append('seller zone / supply был рядом на TF: ' + '/'.join([str(x) for x in list(analysis.get('supply_near_tfs') or [])]))
            except Exception:
                pass
        if analysis.get('demand_near_tfs'):
            try:
                lines.append('underlying bullish FVG / demand виден на TF: ' + '/'.join([str(x) for x in list(analysis.get('demand_near_tfs') or [])]))
            except Exception:
                pass
    if side_u == 'SHORT' and bool(analysis.get('short_above_weak_low')):
        lines.extend([
            'SHORT открыт низко — рядом с buyer zone/support/Strong Low',
            'снизу уже была зона реакции покупателей',
            'downside для SHORT был маленький',
            'цена уже пришла в discount area',
            'sell continuation был поздний',
            'вход сделан слишком низко в range',
        ])
        if analysis.get('demand_near_tfs'):
            try:
                lines.append('buyer zone / demand подтверждена на TF: ' + '/'.join([str(x) for x in list(analysis.get('demand_near_tfs') or [])]))
            except Exception:
                pass
        if analysis.get('entry_position_in_prior_range') is not None:
            try:
                tf = str(analysis.get('entry_position_tf') or 'range')
                pos = float(analysis.get('entry_position_in_prior_range'))
                lines.append(f'позиция SHORT в {tf} range: {pos:.2f} — низкая часть range')
            except Exception:
                pass
        try:
            cs = float(analysis.get('clean_space_to_tp1_pct') or 0.0)
            if cs > 0 and cs <= 0.45:
                lines.append('чистого пространства до TP1 почти не было')
        except Exception:
            pass
    elif side_u == 'LONG' and bool(analysis.get('long_below_strong_high')):
        lines.extend([
            'LONG открыт высоко — рядом с seller zone/resistance/Strong High',
            'сверху уже была зона реакции продавцов',
            'upside для LONG был маленький',
            'цена уже пришла в premium area',
            'buy continuation был поздний',
            'вход сделан слишком высоко в range',
        ])
        if analysis.get('supply_near_tfs'):
            try:
                lines.append('seller zone / supply подтверждена на TF: ' + '/'.join([str(x) for x in list(analysis.get('supply_near_tfs') or [])]))
            except Exception:
                pass
        if analysis.get('entry_position_in_prior_range') is not None:
            try:
                tf = str(analysis.get('entry_position_tf') or 'range')
                pos = float(analysis.get('entry_position_in_prior_range'))
                lines.append(f'позиция LONG в {tf} range: {pos:.2f} — верхняя часть range')
            except Exception:
                pass
        try:
            cs = float(analysis.get('clean_space_to_tp1_pct') or 0.0)
            if cs > 0 and cs <= 0.45:
                lines.append('чистого пространства до TP1 почти не было')
        except Exception:
            pass
    elif bool(analysis.get('late_entry_after_exhausted_move')):
        lines.extend([
            'вход произошёл после уже отработанного импульса',
            'нового fresh displacement перед входом не было',
            'сделка была открыта поздно внутри уже идущего движения',
        ])
    elif bool(analysis.get('entry_in_range_premium')):
        lines.extend([
            'вход был в плохой части range',
            'до противоположной liquidity/зоны оставалось мало места',
            'RR location был слабый',
        ])
    return list(dict.fromkeys([x for x in lines if x]))


def _loss_diag_build_forensic_from_analysis(src: dict, analysis: dict, *, after_tp1: bool, st: str) -> dict:
    route_key = _loss_diag_route_key_from_row(src)
    exact_route = route_key in EXACT_SMC_ROUTE_KEYS
    if st != 'LOSS':
        return {
            'primary_reason_code': '', 'primary_reason_text': '', 'scenario_code': '', 'scenario_text': '',
            'secondary_reason_codes': [], 'secondary_reason_texts': [], 'missed_codes': [], 'missed_texts': [],
            'what_happened_lines': [],
        }

    try:
        _loss_diag_apply_directional_location_flags(analysis, side=str(src.get('side') or analysis.get('side') or ''))
    except Exception:
        pass

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
    long_below_high = bool(analysis.get('long_below_strong_high') or analysis.get('entry_into_supply'))
    short_above_low = bool(analysis.get('short_above_weak_low') or analysis.get('entry_into_demand'))
    entry_overhead = bool((side == 'LONG') and (analysis.get('entry_into_overhead_supply') or analysis.get('entry_into_supply') or long_below_high))
    entry_opposite_zone = bool(analysis.get('entry_near_opposite_zone') or long_below_high or short_above_low)
    bad_range_location = bool(analysis.get('entry_in_range_premium'))
    try:
        entry_pos_for_priority = float(analysis.get('entry_position_in_prior_range') or analysis.get('entry_position_in_range') or 0.5)
    except Exception:
        entry_pos_for_priority = 0.5
    # If LONG had a red FVG/supply directly above and entry was not in the low/demand
    # part of the range, the visible chart reason is LONG into supply, not failed demand.
    supply_dominates_long = bool(side == 'LONG' and entry_overhead and entry_pos_for_priority >= 0.48)
    late_exhausted = bool(analysis.get('late_entry_after_exhausted_move') or analysis.get('late_entry_inside_expansion'))
    try:
        clean_space_to_tp = float(analysis.get('clean_space_to_tp1_pct') or 0.0)
    except Exception:
        clean_space_to_tp = 0.0

    # Fallback forensic inference:
    # even if explicit flags were missed, if TP1 space is tiny + no continuation,
    # treat it as bad entry location near opposite liquidity (real chart reason).
    if side == 'LONG' and (not long_below_high) and clean_space_to_tp > 0 and clean_space_to_tp <= _loss_card_location_tp_space_pct() and weak_follow and (not tp1_threatened):
        long_below_high = True
        entry_overhead = True
        entry_opposite_zone = True
        analysis['long_below_strong_high'] = True
        analysis['entry_into_supply'] = True
        analysis['entry_into_overhead_supply'] = True
        analysis['entry_near_opposite_zone'] = True
    if side == 'SHORT' and (not short_above_low) and clean_space_to_tp > 0 and clean_space_to_tp <= _loss_card_location_tp_space_pct() and weak_follow and (not tp1_threatened):
        short_above_low = True
        entry_opposite_zone = True
        analysis['short_above_weak_low'] = True
        analysis['entry_into_demand'] = True
        analysis['entry_near_opposite_zone'] = True

    failed_demand_long = bool(analysis.get('long_failed_demand_bearish_structure'))
    if side == 'LONG' and not failed_demand_long:
        failed_demand_long = bool(
            (analysis.get('long_demand_near_entry') or analysis.get('demand_near_tfs'))
            and (structure_against or analysis.get('level_reclaimed_back') or analysis.get('reclaim_lost_back'))
            and (weak_follow or no_expansion or analysis.get('zone_reaction_too_weak'))
            and (not tp1_threatened)
        )
    if failed_demand_long:
        analysis['long_failed_demand_bearish_structure'] = True
        analysis['failed_demand_reaction'] = True
        analysis['long_against_bearish_structure'] = True

    failed_supply_short = bool(analysis.get('short_failed_supply_bullish_structure'))
    if side == 'SHORT' and not failed_supply_short:
        failed_supply_short = bool(
            (analysis.get('short_supply_near_entry') or analysis.get('supply_near_tfs'))
            and (structure_against or analysis.get('level_reclaimed_back') or analysis.get('reclaim_lost_back'))
            and (weak_follow or no_expansion or analysis.get('zone_reaction_too_weak'))
            and (not tp1_threatened)
        )
    if failed_supply_short:
        analysis['short_failed_supply_bullish_structure'] = True
        analysis['failed_supply_reaction'] = True
        analysis['short_against_bullish_structure'] = True

    secondary = []
    missed = []
    happened = []
    chart_visible = _loss_diag_chart_visible_lines(analysis, side)

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
        happened.append('SHORT был открыт прямо над weak low / demand liquidity')
    if entry_opposite_zone and not (entry_overhead or long_below_high or short_above_low):
        secondary.append('entry_near_opposite_zone')
        happened.append('вход был слишком близко к противоположной зоне реакции')
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
    if side == 'LONG' and failed_demand_long and not supply_dominates_long:
        primary = 'long_failed_demand_bearish_structure'
        scenario = 'failed_demand_scenario'
        missed.extend(['require_bullish_reclaim', 'avoid_failed_demand_long'])
        if bool(analysis.get('overhead_bearish_fvg') or analysis.get('supply_near_tfs')):
            missed.append('avoid_overhead_bearish_fvg')
        happened = [
            'цена не смогла закрепиться выше зоны входа',
            'buyer zone не удержала',
            'после входа не было displacement вверх',
            'продавец продолжил sell-side движение',
            'SL был выбит почти сразу',
        ] + happened
        secondary.extend([
            'long_against_bearish_structure',
            'failed_demand_reaction',
            'no_post_entry_expansion',
            'weak_followthrough',
            'tp1_never_threatened',
            'continuation_missing',
        ])
        if bool(analysis.get('overhead_bearish_fvg') or analysis.get('supply_near_tfs')):
            secondary.append('overhead_bearish_fvg')
    elif side == 'SHORT' and failed_supply_short:
        primary = 'short_failed_supply_bullish_structure'
        scenario = 'failed_supply_scenario'
        missed.extend(['require_bearish_reclaim', 'avoid_failed_supply_short'])
        if bool(analysis.get('underlying_bullish_fvg') or analysis.get('demand_near_tfs')):
            missed.append('avoid_underlying_bullish_fvg')
        happened = [
            'цена не смогла продолжить sell-side движение',
            'seller zone не удержала',
            'после входа не было displacement вниз',
            'покупатель продолжил buy-side движение',
            'SL был выбит почти сразу',
        ] + happened
        secondary.extend([
            'short_against_bullish_structure',
            'failed_supply_reaction',
            'no_post_entry_expansion',
            'weak_followthrough',
            'tp1_never_threatened',
            'continuation_missing',
        ])
        if bool(analysis.get('underlying_bullish_fvg') or analysis.get('demand_near_tfs')):
            secondary.append('underlying_bullish_fvg')
    elif side == 'SHORT' and (short_above_low or (entry_overhead and bad_range_location)):
        primary = 'short_above_weak_low'
        scenario = 'short_into_demand_scenario'
        missed.append('require_clean_space_to_tp')
        happened = [
            'цена не смогла продолжить sell-side движение',
            'buyer zone сразу начала удерживать цену',
            'продавец не получил displacement вниз',
            'произошёл bounce против позиции',
        ] + happened
        secondary.extend(['late_entry_after_exhausted_move', 'weak_followthrough', 'no_post_entry_expansion', 'tp1_never_threatened'])
    elif side == 'LONG' and (long_below_high or entry_overhead):
        primary = 'long_below_strong_high'
        scenario = 'long_into_supply_scenario'
        missed.append('require_clean_space_to_tp')
        happened = [
            'цена не смогла продолжить buy-side движение',
            'seller zone сразу начала удерживать цену',
            'покупатель не получил displacement вверх',
            'произошёл rejection против позиции',
        ] + happened
        secondary.extend(['late_entry_after_exhausted_move', 'weak_followthrough', 'no_post_entry_expansion', 'tp1_never_threatened'])
    elif entry_overhead:
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
        'chart_visible_lines': chart_visible,
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
def _loss_diag_tf_minutes(tf: str) -> int:
    s = str(tf or '').strip().lower()
    if s.endswith('m'):
        try:
            return max(1, int(float(s[:-1] or 0)))
        except Exception:
            return 5
    if s.endswith('h'):
        try:
            return max(1, int(float(s[:-1] or 0) * 60))
        except Exception:
            return 60
    return 5


def _loss_diag_snapshot_tfs() -> list[str]:
    raw = str(os.getenv('LOSS_DIAG_TIMEFRAMES', '5m,15m,30m,1h') or '5m,15m,30m,1h')
    out: list[str] = []
    for part in re.split(r"[,;\s]+", raw):
        tf = str(part or '').strip().lower()
        if tf in {'5m', '15m', '30m', '1h'} and tf not in out:
            out.append(tf)
    return out or ['5m', '15m', '30m', '1h']


def _loss_diag_norm_ohlcv_df(df) -> pd.DataFrame:
    """Return normalized OHLCV dataframe with _ts/open/high/low/close/volume.

    The bot uses this for forensic LOSS analysis. It reads candles, not PNG screenshots.
    """
    if df is None or getattr(df, 'empty', True):
        return pd.DataFrame()
    try:
        d = df.copy()
    except Exception:
        return pd.DataFrame()
    try:
        if 'open_time_ms' in d.columns:
            ts = pd.to_datetime(d['open_time_ms'], unit='ms', utc=True, errors='coerce')
        elif 'timestamp' in d.columns:
            ts = pd.to_datetime(d['timestamp'], unit='ms', utc=True, errors='coerce')
        elif 'time' in d.columns:
            ts = pd.to_datetime(d['time'], utc=True, errors='coerce')
        elif isinstance(d.index, pd.DatetimeIndex):
            ts = pd.to_datetime(d.index, utc=True, errors='coerce')
        else:
            ts = pd.Series([pd.NaT] * len(d))
        d['_ts'] = ts
        for col in ('open', 'high', 'low', 'close'):
            if col not in d.columns:
                return pd.DataFrame()
            d[col] = pd.to_numeric(d[col], errors='coerce')
        if 'volume' in d.columns:
            d['volume'] = pd.to_numeric(d['volume'], errors='coerce').fillna(0.0)
        else:
            d['volume'] = 0.0
        d = d[d['_ts'].notna()].dropna(subset=['open', 'high', 'low', 'close']).sort_values('_ts').reset_index(drop=True)
        return d
    except Exception:
        return pd.DataFrame()


def _loss_diag_find_fvg_zones(d: pd.DataFrame, *, tf: str = '', max_scan: int = 90) -> list[dict]:
    zones: list[dict] = []
    if d is None or getattr(d, 'empty', True) or len(d) < 3:
        return zones
    try:
        x = d.tail(max(3, int(max_scan))).reset_index(drop=True)
        for i in range(2, len(x)):
            h2 = float(x.loc[i - 2, 'high'])
            l2 = float(x.loc[i - 2, 'low'])
            hi = float(x.loc[i, 'high'])
            lo = float(x.loc[i, 'low'])
            ts = x.loc[i, '_ts'] if '_ts' in x.columns else None
            # Bullish FVG: demand / buyer imbalance below current price.
            if lo > h2:
                zones.append({
                    'kind': 'bullish_fvg', 'side': 'demand', 'tf': str(tf or ''),
                    'bottom': round(h2, 8), 'top': round(lo, 8),
                    'age_bars': int(len(x) - 1 - i), 'time': str(ts) if ts is not None else '',
                })
            # Bearish FVG: supply / seller imbalance above current price.
            if hi < l2:
                zones.append({
                    'kind': 'bearish_fvg', 'side': 'supply', 'tf': str(tf or ''),
                    'bottom': round(hi, 8), 'top': round(l2, 8),
                    'age_bars': int(len(x) - 1 - i), 'time': str(ts) if ts is not None else '',
                })
    except Exception:
        return zones
    return zones


def _loss_diag_pick_near_zone(zones: list[dict], *, entry: float, zone_side: str, max_dist_pct: float, target: float = 0.0) -> dict:
    if not zones or entry <= 0:
        return {}
    want = str(zone_side or '').strip().lower()
    best = None
    best_dist = 10**9
    target_f = 0.0
    try:
        target_f = float(target or 0.0)
    except Exception:
        target_f = 0.0
    for z in zones:
        try:
            if str(z.get('side') or '').lower() != want:
                continue
            bottom = float(z.get('bottom'))
            top = float(z.get('top'))
            if top < bottom:
                bottom, top = top, bottom

            relation = ''
            blocks_target = False
            # Important for LOSS cards: a zone can be correct reason even when
            # entry is not inside it. If it sits between entry and TP1, it blocks
            # the path (LONG into overhead red FVG / SHORT into underlying green FVG).
            if target_f > 0 and abs(target_f - entry) > 1e-12:
                lo_path, hi_path = (entry, target_f) if entry <= target_f else (target_f, entry)
                if max(lo_path, bottom) <= min(hi_path, top):
                    blocks_target = True

            if bottom <= entry <= top:
                dist_pct = 0.0
                relation = 'inside'
            elif want == 'demand' and entry > top:
                dist_pct = ((entry - top) / entry) * 100.0
                relation = 'above'
            elif want == 'supply' and entry < bottom:
                dist_pct = ((bottom - entry) / entry) * 100.0
                relation = 'below'
            elif blocks_target:
                dist_pct = 0.0
                relation = 'blocks_target'
            else:
                continue

            limit = float(max_dist_pct)
            if blocks_target:
                limit = max(limit, 0.85)
                relation = 'blocks_target' if relation not in ('inside',) else 'inside_blocks_target'
            if dist_pct <= limit and dist_pct < best_dist:
                best_dist = dist_pct
                best = dict(z)
                best['distance_pct'] = round(dist_pct, 3)
                best['relation'] = relation
                if blocks_target:
                    best['blocks_target'] = True
        except Exception:
            continue
    return best or {}


def _loss_diag_build_tf_snapshot_from_df(df, *, entry: float, side: str, tf: str, opened_at: dt.datetime | None = None, tp1: float = 0.0) -> dict:
    d = _loss_diag_norm_ohlcv_df(df)
    if d.empty or entry <= 0:
        return {}
    try:
        tfm = _loss_diag_tf_minutes(tf)
        if opened_at is not None:
            # Backfill must be an entry snapshot, not a close-time snapshot.
            # For 5m/15m/30m/1h candles fetched after the trade closed, the candle
            # that contains opened_at already has future high/low/close values.
            # Use only candles that were fully closed before the entry time. If an
            # exchange response is too sparse, fall back to candle-open <= entry.
            cutoff = opened_at + dt.timedelta(seconds=5)
            closed_before_entry = d[(d['_ts'] + dt.timedelta(minutes=tfm)) <= cutoff].copy()
            if closed_before_entry.empty:
                closed_before_entry = d[d['_ts'] <= cutoff].copy()
            d = closed_before_entry
            if d.empty:
                return {}
        lookback = 96 if str(tf).lower() == '5m' else 72
        pre = d.tail(lookback).copy()
        highs = pre['high'].astype(float)
        lows = pre['low'].astype(float)
        closes = pre['close'].astype(float)
        range_high = float(highs.max())
        range_low = float(lows.min())
        rng = max(range_high - range_low, 1e-12)
        entry_f = float(entry)
        target_f = float(tp1 or 0.0)
        side_u = str(side or '').upper().strip()
        entry_pos = (entry_f - range_low) / rng
        dist_high_pct = ((range_high - entry_f) / entry_f) * 100.0
        dist_low_pct = ((entry_f - range_low) / entry_f) * 100.0
        max_dist_pct = 0.32 if tfm <= 5 else (0.42 if tfm <= 15 else 0.55)
        zones = _loss_diag_find_fvg_zones(pre, tf=tf, max_scan=min(len(pre), 90))
        demand = _loss_diag_pick_near_zone(zones, entry=entry_f, zone_side='demand', max_dist_pct=max_dist_pct, target=target_f)
        supply = _loss_diag_pick_near_zone(zones, entry=entry_f, zone_side='supply', max_dist_pct=max_dist_pct, target=target_f)

        # FVG is the best signal, but it is not the only visible blocker.
        # Many LOSS cards were falling back to “Breakout trap / no continuation”
        # because the pre-entry candle snapshot had a recent swing high/low between
        # entry and TP1, but no strict 3-candle FVG. For the report card this is
        # still a chart-location problem: LONG has overhead supply/resistance before
        # TP1; SHORT has underlying demand/support before TP1.
        path_block_zone: dict = {}
        path_block_enabled = str(os.getenv('LOSS_DIAG_PATH_BLOCK_ENABLED', '1')).strip().lower() not in ('0', 'false', 'no', 'off')
        try:
            path_max_tp_space_pct = float(str(os.getenv('LOSS_DIAG_PATH_BLOCK_MAX_TP_SPACE_PCT', '1.35') or '1.35').replace(',', '.'))
        except Exception:
            path_max_tp_space_pct = 1.35
        try:
            path_buffer_pct = float(str(os.getenv('LOSS_DIAG_PATH_BLOCK_BUFFER_PCT', '0.08') or '0.08').replace(',', '.'))
        except Exception:
            path_buffer_pct = 0.08
        try:
            zone_band_pct = float(str(os.getenv('LOSS_DIAG_PATH_BLOCK_ZONE_BAND_PCT', '0.06') or '0.06').replace(',', '.'))
        except Exception:
            zone_band_pct = 0.06
        clean_space_pct = 0.0
        if path_block_enabled and target_f > 0 and abs(target_f - entry_f) > 1e-12:
            if side_u == 'LONG' and target_f > entry_f:
                clean_space_pct = ((target_f - entry_f) / entry_f) * 100.0
                if clean_space_pct <= max(0.20, path_max_tp_space_pct):
                    upper_limit = target_f + entry_f * (path_buffer_pct / 100.0)
                    path_highs = pre[(pre['high'].astype(float) >= entry_f) & (pre['high'].astype(float) <= upper_limit)].copy()
                    if not path_highs.empty:
                        # First obstacle on the path to TP1, not necessarily the absolute high.
                        obstacle = float(path_highs['high'].astype(float).min())
                        rows = path_highs[path_highs['high'].astype(float) == obstacle]
                        idx = int(rows.index[-1]) if not rows.empty else int(path_highs.index[-1])
                        age = int(max(0, len(pre) - 1 - list(pre.index).index(idx))) if idx in list(pre.index) else 0
                        band = max(entry_f * (zone_band_pct / 100.0), (target_f - entry_f) * 0.10)
                        bottom = max(entry_f, obstacle - band)
                        top = max(bottom, obstacle)
                        path_block_zone = {
                            'kind': 'recent_swing_high_supply', 'side': 'supply', 'tf': str(tf or ''),
                            'bottom': round(bottom, 8), 'top': round(top, 8),
                            'age_bars': age, 'distance_pct': round(max(0.0, ((bottom - entry_f) / entry_f) * 100.0), 3),
                            'relation': 'blocks_target', 'blocks_target': True,
                            'source': 'range_high_between_entry_and_tp1',
                        }
                        if not supply:
                            supply = dict(path_block_zone)
            elif side_u == 'SHORT' and target_f < entry_f:
                clean_space_pct = ((entry_f - target_f) / entry_f) * 100.0
                if clean_space_pct <= max(0.20, path_max_tp_space_pct):
                    lower_limit = target_f - entry_f * (path_buffer_pct / 100.0)
                    path_lows = pre[(pre['low'].astype(float) <= entry_f) & (pre['low'].astype(float) >= lower_limit)].copy()
                    if not path_lows.empty:
                        # First obstacle on the path to TP1 for SHORT.
                        obstacle = float(path_lows['low'].astype(float).max())
                        rows = path_lows[path_lows['low'].astype(float) == obstacle]
                        idx = int(rows.index[-1]) if not rows.empty else int(path_lows.index[-1])
                        age = int(max(0, len(pre) - 1 - list(pre.index).index(idx))) if idx in list(pre.index) else 0
                        band = max(entry_f * (zone_band_pct / 100.0), (entry_f - target_f) * 0.10)
                        top = min(entry_f, obstacle + band)
                        bottom = min(top, obstacle)
                        path_block_zone = {
                            'kind': 'recent_swing_low_demand', 'side': 'demand', 'tf': str(tf or ''),
                            'bottom': round(bottom, 8), 'top': round(top, 8),
                            'age_bars': age, 'distance_pct': round(max(0.0, ((entry_f - top) / entry_f) * 100.0), 3),
                            'relation': 'blocks_target', 'blocks_target': True,
                            'source': 'range_low_between_entry_and_tp1',
                        }
                        if not demand:
                            demand = dict(path_block_zone)

        strong_low_near = bool(0 <= dist_low_pct <= max(max_dist_pct, 0.38) or entry_pos <= 0.20 or (side_u == 'SHORT' and bool(path_block_zone) and path_block_zone.get('side') == 'demand'))
        strong_high_near = bool(0 <= dist_high_pct <= max(max_dist_pct, 0.38) or entry_pos >= 0.80 or (side_u == 'LONG' and bool(path_block_zone) and path_block_zone.get('side') == 'supply'))
        pre_move_pct = 0.0
        if len(closes) >= 2:
            pre_move_pct = ((float(closes.iloc[-1]) - float(closes.iloc[0])) / max(float(closes.iloc[0]), 1e-12)) * 100.0
        return {
            'tf': str(tf),
            'bars': int(len(pre)),
            'range_high': round(range_high, 8),
            'range_low': round(range_low, 8),
            'entry_position_in_range': round(entry_pos, 3),
            'distance_to_range_high_pct': round(dist_high_pct, 3),
            'distance_to_range_low_pct': round(dist_low_pct, 3),
            'clean_space_to_tp1_pct': round(clean_space_pct, 3) if clean_space_pct > 0 else 0.0,
            'pre_entry_move_pct': round(pre_move_pct, 3),
            'demand_zone': demand,
            'supply_zone': supply,
            'path_block_zone': path_block_zone,
            'range_high_blocks_tp1': bool(side_u == 'LONG' and path_block_zone and path_block_zone.get('side') == 'supply'),
            'range_low_blocks_tp1': bool(side_u == 'SHORT' and path_block_zone and path_block_zone.get('side') == 'demand'),
            'entry_near_demand': bool(demand or strong_low_near),
            'entry_near_supply': bool(supply or strong_high_near),
            'entry_near_strong_low': strong_low_near,
            'entry_near_strong_high': strong_high_near,
            'fvg_zones_count': int(len(zones)),
        }
    except Exception:
        return {}


def _loss_diag_enrich_entry_snapshot_from_tfs(snapshot: dict, *, side: str = '') -> dict:
    """Add a stable root summary to entry_snapshot_json from all configured TF snapshots.

    Detailed candle facts stay under tf_snapshots[5m/15m/30m/1h].
    Root fields make DB inspection and close-analysis deterministic for every
    base setup and every SMC route.
    """
    if not isinstance(snapshot, dict):
        return {}
    tfs = snapshot.get('tf_snapshots')
    if not isinstance(tfs, dict) or not tfs:
        return snapshot
    side_u = str(side or snapshot.get('direction') or '').upper().strip()
    ordered = [tf for tf in ('5m', '15m', '30m', '1h') if isinstance(tfs.get(tf), dict) and tfs.get(tf)]
    if ordered:
        snapshot['loss_diag_timeframes'] = ordered
    preferred_tf = None
    for name in ('15m', '30m', '1h', '5m'):
        if name in ordered:
            preferred_tf = name
            break
    base = tfs.get(preferred_tf or (ordered[0] if ordered else ''), {}) if ordered else {}
    if isinstance(base, dict) and base:
        for dst, src_key in {
            'range_high': 'range_high',
            'range_low': 'range_low',
            'entry_position_in_range': 'entry_position_in_range',
            'distance_to_range_high_pct': 'distance_to_range_high_pct',
            'distance_to_range_low_pct': 'distance_to_range_low_pct',
            'pre_entry_move_pct': 'pre_entry_move_pct',
        }.items():
            if snapshot.get(dst) in (None, '', [], {}):
                val = base.get(src_key)
                if val not in (None, '', [], {}):
                    snapshot[dst] = val
        snapshot.setdefault('preferred_snapshot_tf', preferred_tf or '')

    all_fvgs = []
    demand_hits = []
    supply_hits = []
    for tf in ordered:
        one = tfs.get(tf)
        if not isinstance(one, dict):
            continue
        dz = one.get('demand_zone') if isinstance(one.get('demand_zone'), dict) else {}
        sz = one.get('supply_zone') if isinstance(one.get('supply_zone'), dict) else {}
        if dz:
            demand_hits.append(tf)
            all_fvgs.append(dict(dz, tf=tf))
        elif one.get('entry_near_demand'):
            demand_hits.append(tf)
        if sz:
            supply_hits.append(tf)
            all_fvgs.append(dict(sz, tf=tf))
        elif one.get('entry_near_supply'):
            supply_hits.append(tf)
    if demand_hits:
        snapshot['demand_near_entry_tfs'] = list(dict.fromkeys(demand_hits))
        if snapshot.get('nearest_demand_zone') in (None, '', [], {}):
            for z in all_fvgs:
                if str(z.get('side') or '').lower() == 'demand':
                    snapshot['nearest_demand_zone'] = z
                    break
    if supply_hits:
        snapshot['supply_near_entry_tfs'] = list(dict.fromkeys(supply_hits))
        if snapshot.get('nearest_supply_zone') in (None, '', [], {}):
            for z in all_fvgs:
                if str(z.get('side') or '').lower() == 'supply':
                    snapshot['nearest_supply_zone'] = z
                    break
    if all_fvgs:
        snapshot['fvg_zones_near_entry'] = all_fvgs[:12]

    # Conservative candidates for DB/debug. Explicit SMC levels from Signal/meta are not overwritten.
    try:
        rh = _loss_diag_parse_float(snapshot.get('range_high'))
        rl = _loss_diag_parse_float(snapshot.get('range_low'))
        if rh is not None and rl is not None and rh > rl:
            if side_u == 'LONG':
                snapshot.setdefault('bos_level_candidate', rh)
                snapshot.setdefault('reclaim_level_candidate', rh)
                snapshot.setdefault('sweep_level_candidate', rl)
                snapshot.setdefault('origin_level_candidate', rl)
            elif side_u == 'SHORT':
                snapshot.setdefault('bos_level_candidate', rl)
                snapshot.setdefault('reclaim_level_candidate', rl)
                snapshot.setdefault('sweep_level_candidate', rh)
                snapshot.setdefault('origin_level_candidate', rh)
            else:
                snapshot.setdefault('bos_level_candidate', rh)
                snapshot.setdefault('sweep_level_candidate', rl)
    except Exception:
        pass
    return snapshot




def _loss_diag_apply_directional_location_flags(analysis: dict, *, side: str) -> None:
    """Infer the real chart location for LOSS cards from candle context.

    This is a safety net for trades where the close reason is a generic SL/no-follow-through,
    but the chart clearly shows the entry was against a nearby demand/supply zone or after
    an opposite reclaim. It is intentionally conservative and only runs when TP1 was not
    properly threatened and follow-through was weak.
    """
    if not isinstance(analysis, dict):
        return
    side_u = str(side or analysis.get('side') or '').upper().strip()
    if side_u not in ('LONG', 'SHORT'):
        return

    def _f(key: str, default: float = 0.0) -> float:
        try:
            val = analysis.get(key)
            if val is None or val == '':
                return default
            return float(val)
        except Exception:
            return default

    def _b(key: str) -> bool:
        val = analysis.get(key)
        if isinstance(val, str):
            return val.strip().lower() in ('1', 'true', 'yes', 'on', 'да', 'есть')
        return bool(val)

    tp1_known = 'tp1_threatened' in analysis
    tp1_threatened = bool(analysis.get('tp1_threatened'))
    weak_after_entry = bool(
        _b('weak_followthrough')
        or _b('no_post_entry_expansion')
        or _b('zone_reaction_too_weak')
        or analysis.get('continuation_seen') is False
        or (tp1_known and not tp1_threatened)
    )
    if not weak_after_entry:
        return

    clean_space = _f('clean_space_to_tp1_pct')
    entry_pos = _f('entry_position_in_prior_range', _f('entry_position_in_range', 0.5))
    local6 = _f('local_pre_move_6_pct')
    local12 = _f('local_pre_move_12_pct')
    local24 = _f('local_pre_move_24_pct')
    pre_move = _f('pre_entry_move_pct')
    close_vs_ema = str(analysis.get('pre_close_vs_ema20') or '').lower().strip()

    bullish_context = bool(
        local6 >= 0.12
        or local12 >= 0.22
        or local24 >= 0.35
        or pre_move >= 0.45
        or close_vs_ema == 'above'
        or str(analysis.get('structure_5m') or '').lower().startswith('bullish')
        or _b('structure_against_trade')
        or _b('level_reclaimed_back')
        or _b('reclaim_lost_back')
    )
    bearish_context = bool(
        local6 <= -0.12
        or local12 <= -0.22
        or local24 <= -0.35
        or pre_move <= -0.45
        or close_vs_ema == 'below'
        or str(analysis.get('structure_5m') or '').lower().startswith('bearish')
        or _b('structure_against_trade')
        or _b('level_reclaimed_back')
        or _b('reclaim_lost_back')
    )

    if side_u == 'SHORT':
        demand_near = bool(
            analysis.get('demand_near_tfs')
            or _b('underlying_bullish_fvg')
            or _b('entry_into_demand')
            or _b('short_above_weak_low')
        )
        supply_near = bool(analysis.get('supply_near_tfs') or _b('short_supply_near_entry'))
        lowish = bool(entry_pos <= 0.28)
        if (not tp1_threatened) and 0 < clean_space <= 0.65 and not (demand_near or lowish or bullish_context):
            analysis['tp1_too_close_no_clean_space'] = True
            analysis['location_override_reason'] = 'short_tiny_tp1_space_no_confirm'
        if (not tp1_threatened) and (demand_near or lowish or bullish_context):
            if demand_near or lowish:
                analysis['short_above_weak_low'] = True
                analysis['entry_into_demand'] = True
                analysis['entry_near_opposite_zone'] = True
                analysis['entry_in_range_discount'] = True
                analysis['underlying_bullish_fvg'] = bool(analysis.get('underlying_bullish_fvg') or demand_near)
            if bullish_context:
                analysis['bullish_context_before_entry'] = True
                analysis['bearish_reclaim_missing'] = True
                analysis['short_against_bullish_structure'] = True
        if (not tp1_threatened) and bullish_context and supply_near and not demand_near and entry_pos >= 0.45:
            analysis['short_failed_supply_bullish_structure'] = True
            analysis['failed_supply_reaction'] = True
            analysis['short_against_bullish_structure'] = True
        return

    supply_near = bool(
        analysis.get('supply_near_tfs')
        or _b('overhead_bearish_fvg')
        or _b('entry_into_supply')
        or _b('entry_into_overhead_supply')
        or _b('long_below_strong_high')
    )
    demand_near = bool(analysis.get('demand_near_tfs') or _b('long_demand_near_entry'))
    highish = bool(entry_pos >= 0.72)
    if (not tp1_threatened) and 0 < clean_space <= 0.65 and not (supply_near or highish or bearish_context):
        analysis['tp1_too_close_no_clean_space'] = True
        analysis['location_override_reason'] = 'long_tiny_tp1_space_no_confirm'
    if (not tp1_threatened) and (supply_near or highish or bearish_context):
        if supply_near or highish:
            analysis['long_below_strong_high'] = True
            analysis['entry_into_supply'] = True
            analysis['entry_into_overhead_supply'] = True
            analysis['entry_near_opposite_zone'] = True
            analysis['entry_in_range_premium'] = True
            analysis['overhead_bearish_fvg'] = bool(analysis.get('overhead_bearish_fvg') or supply_near)
        if bearish_context:
            analysis['bearish_context_before_entry'] = True
            analysis['reclaim_missing'] = True
            analysis['long_against_bearish_structure'] = True
    if (not tp1_threatened) and bearish_context and demand_near and not supply_near and entry_pos <= 0.55:
        analysis['long_failed_demand_bearish_structure'] = True
        analysis['failed_demand_reaction'] = True
        analysis['long_against_bearish_structure'] = True

def _loss_diag_apply_snapshot_context(analysis: dict, snapshot: dict, *, side: str, entry: float, tp1: float, sl: float) -> None:
    """Merge entry_snapshot_json into close analysis and set chart-location flags.

    Priority is location first: if SHORT is opened into demand / near Strong Low, the card must
    explain that, not only generic breakout trap. Same mirror logic for LONG under supply.
    """
    if not isinstance(analysis, dict) or entry <= 0:
        return
    side_u = str(side or '').upper().strip()
    tfs = snapshot.get('tf_snapshots') if isinstance(snapshot, dict) else None
    if not isinstance(tfs, dict):
        tfs = {}
    checked = [tf for tf in ('5m', '15m', '30m', '1h') if isinstance(tfs.get(tf), dict) and tfs.get(tf)]
    if checked:
        analysis['tf_checked'] = checked
    try:
        if tp1 > 0:
            if side_u == 'SHORT' and tp1 < entry:
                analysis['clean_space_to_tp1_pct'] = round(((entry - tp1) / entry) * 100.0, 3)
            elif side_u == 'LONG' and tp1 > entry:
                analysis['clean_space_to_tp1_pct'] = round(((tp1 - entry) / entry) * 100.0, 3)
    except Exception:
        pass

    demand_hits = []
    supply_hits = []
    demand_zone_candidates = []
    supply_zone_candidates = []
    entry_positions = []
    pre_moves = []
    for tf, one in (tfs or {}).items():
        if not isinstance(one, dict):
            continue
        try:
            ep = one.get('entry_position_in_range')
            if ep is not None:
                entry_positions.append((str(tf), float(ep)))
            pm = one.get('pre_entry_move_pct')
            if pm is not None:
                pre_moves.append(float(pm))
            dz = one.get('demand_zone') if isinstance(one.get('demand_zone'), dict) else {}
            sz = one.get('supply_zone') if isinstance(one.get('supply_zone'), dict) else {}
            if one.get('entry_near_demand') or dz:
                demand_hits.append(str(tf))
                if dz:
                    demand_zone_candidates.append(dict(dz, tf=str(tf)))
            if one.get('entry_near_supply') or sz:
                supply_hits.append(str(tf))
                if sz:
                    supply_zone_candidates.append(dict(sz, tf=str(tf)))
        except Exception:
            continue

    if entry_positions and analysis.get('entry_position_in_prior_range') is None:
        preferred = None
        for name in ('15m', '30m', '1h', '5m'):
            preferred = next((x for x in entry_positions if x[0] == name), None)
            if preferred:
                break
        if preferred:
            analysis['entry_position_in_prior_range'] = round(float(preferred[1]), 3)
            analysis['entry_position_tf'] = preferred[0]

    try:
        cs = float(analysis.get('clean_space_to_tp1_pct') or 0.0)
    except Exception:
        cs = 0.0

    def _zone_distance_key(z):
        try:
            return (0 if bool(z.get('blocks_target')) else 1, float(z.get('distance_pct') or 999.0), int(z.get('age_bars') or 9999))
        except Exception:
            return (9, 999.0, 9999)

    if supply_zone_candidates:
        try:
            supply_zone_candidates = sorted(supply_zone_candidates, key=_zone_distance_key)
            nearest_supply = dict(supply_zone_candidates[0])
            analysis['nearest_supply_zone'] = nearest_supply
            analysis['supply_zone_relation'] = str(nearest_supply.get('relation') or '')
            analysis['supply_zone_blocks_tp1'] = bool(nearest_supply.get('blocks_target'))
            analysis['supply_zone_distance_pct'] = nearest_supply.get('distance_pct')
        except Exception:
            pass
    if demand_zone_candidates:
        try:
            demand_zone_candidates = sorted(demand_zone_candidates, key=_zone_distance_key)
            nearest_demand = dict(demand_zone_candidates[0])
            analysis['nearest_demand_zone'] = nearest_demand
            analysis['demand_zone_relation'] = str(nearest_demand.get('relation') or '')
            analysis['demand_zone_blocks_tp1'] = bool(nearest_demand.get('blocks_target'))
            analysis['demand_zone_distance_pct'] = nearest_demand.get('distance_pct')
        except Exception:
            pass

    if side_u == 'SHORT':
        if supply_hits:
            analysis['short_supply_near_entry'] = True
            analysis['supply_near_tfs'] = list(dict.fromkeys(supply_hits))
        if demand_hits:
            analysis['demand_near_tfs'] = list(dict.fromkeys(demand_hits))
            analysis['underlying_bullish_fvg'] = True
            analysis['short_above_weak_low'] = True
            analysis['entry_into_demand'] = True
            analysis['entry_near_opposite_zone'] = True
            analysis['entry_in_range_discount'] = True
        elif entry_positions:
            lowish = any(ep <= 0.35 for _, ep in entry_positions)
            if lowish and (cs <= 0.55 or bool(analysis.get('weak_followthrough'))):
                analysis['short_above_weak_low'] = True
                analysis['entry_into_demand'] = True
                analysis['entry_near_opposite_zone'] = True
                analysis['entry_in_range_discount'] = True
        if pre_moves and min(pre_moves) < -0.45:
            analysis['late_entry_after_exhausted_move'] = True

        # Failed-supply SHORT pattern: sell from supply after market already shifted bullish.
        # This is the SHORT mirror of ANIMEUSDT-type failed-demand LONG.
        bullish_after_entry = bool(analysis.get('structure_against_trade') or analysis.get('level_reclaimed_back') or analysis.get('reclaim_lost_back'))
        weak_after_entry = bool(analysis.get('weak_followthrough') or analysis.get('no_post_entry_expansion') or analysis.get('zone_reaction_too_weak') or (analysis.get('tp1_threatened') is False))
        highish_entry = False
        try:
            highish_entry = any(float(ep) >= 0.55 for _, ep in entry_positions)
        except Exception:
            highish_entry = False
        if bullish_after_entry and weak_after_entry and (supply_hits or highish_entry):
            analysis['short_failed_supply_bullish_structure'] = True
            analysis['failed_supply_reaction'] = True
            analysis['short_against_bullish_structure'] = True
            analysis['short_supply_near_entry'] = bool(analysis.get('short_supply_near_entry') or supply_hits or highish_entry)
            if demand_hits:
                analysis['underlying_bullish_fvg'] = True
    elif side_u == 'LONG':
        if demand_hits:
            analysis['long_demand_near_entry'] = True
            analysis['demand_near_tfs'] = list(dict.fromkeys(demand_hits))
        pre_bearish = bool(pre_moves and min(pre_moves) < -0.45)
        pre_bullish = bool(pre_moves and max(pre_moves) > 0.45)
        if supply_hits:
            analysis['supply_near_tfs'] = list(dict.fromkeys(supply_hits))
            analysis['overhead_bearish_fvg'] = True
            analysis['long_below_strong_high'] = True
            analysis['entry_into_supply'] = True
            analysis['entry_into_overhead_supply'] = True
            analysis['entry_near_opposite_zone'] = True
            if pre_bearish and not pre_bullish:
                analysis['bearish_context_before_entry'] = True
                analysis['long_against_bearish_structure'] = True
                analysis['long_bearish_continuation_under_supply'] = True
                analysis['reclaim_missing'] = True
            else:
                analysis['entry_in_range_premium'] = True
        elif entry_positions:
            highish = any(ep >= 0.65 for _, ep in entry_positions)
            if highish and (cs <= 0.55 or bool(analysis.get('weak_followthrough'))):
                analysis['long_below_strong_high'] = True
                analysis['entry_into_supply'] = True
                analysis['entry_into_overhead_supply'] = True
                analysis['entry_near_opposite_zone'] = True
                analysis['entry_in_range_premium'] = True
        if pre_moves and max(pre_moves) > 0.45:
            analysis['late_entry_after_exhausted_move'] = True
        if pre_moves and min(pre_moves) < -0.45:
            analysis['bearish_context_before_entry'] = True
            if analysis.get('overhead_bearish_fvg') or analysis.get('supply_near_tfs'):
                analysis['long_against_bearish_structure'] = True
                analysis['long_bearish_continuation_under_supply'] = True
                analysis['reclaim_missing'] = True

        # Failed-demand LONG pattern (ANIMEUSDT-type): buy from demand after market
        # already shifted bearish. This is different from a simple breakout trap.
        # It needs a dedicated card reason: bearish BOS/CHoCH + weak/failed demand + no reclaim.
        bearish_after_entry = bool(analysis.get('structure_against_trade') or analysis.get('level_reclaimed_back') or analysis.get('reclaim_lost_back'))
        weak_after_entry = bool(analysis.get('weak_followthrough') or analysis.get('no_post_entry_expansion') or analysis.get('zone_reaction_too_weak') or (analysis.get('tp1_threatened') is False))
        lowish_entry = False
        try:
            lowish_entry = any(float(ep) <= 0.45 for _, ep in entry_positions)
        except Exception:
            lowish_entry = False
        supply_blocks_long = bool(supply_hits and not lowish_entry)
        if bearish_after_entry and weak_after_entry and (demand_hits or lowish_entry) and not supply_blocks_long:
            analysis['long_failed_demand_bearish_structure'] = True
            analysis['failed_demand_reaction'] = True
            analysis['long_against_bearish_structure'] = True
            analysis['long_demand_near_entry'] = bool(analysis.get('long_demand_near_entry') or demand_hits or lowish_entry)
            if supply_hits:
                analysis['overhead_bearish_fvg'] = True


def _loss_diag_apply_fast_context_flags(analysis: dict, *, side: str) -> None:
    """Directional fallback for very fast SL losses.

    If a trade closes by SL before TP1 and candle/snapshot context is thin,
    use the last pre-entry candles + post-entry failure metrics to avoid a
    generic "Breakout trap" card. This does not read screenshots; it derives
    the scenario from OHLCV context saved in close_analysis_json.
    """
    if not isinstance(analysis, dict):
        return
    side_u = str(side or analysis.get('side') or '').upper().strip()
    if side_u not in ('LONG', 'SHORT'):
        return

    def _f(key: str, default: float = 0.0) -> float:
        try:
            val = analysis.get(key)
            if val is None or val == '':
                return default
            return float(val)
        except Exception:
            return default

    def _i(key: str, default: int = 0) -> int:
        try:
            val = analysis.get(key)
            if val is None or val == '':
                return default
            return int(float(val))
        except Exception:
            return default

    duration_min = _i('duration_min')
    bars_to_failure = _i('bars_to_failure') or _i('bars_to_sl')
    fast_loss = bool(
        analysis.get('fast_invalidation')
        or analysis.get('immediate_invalidation')
        or (duration_min > 0 and duration_min <= 12)
        or (bars_to_failure > 0 and bars_to_failure <= 3)
    )
    tp1_threatened = bool(analysis.get('tp1_threatened'))
    weak_after_entry = bool(
        analysis.get('weak_followthrough')
        or analysis.get('no_post_entry_expansion')
        or analysis.get('zone_reaction_too_weak')
        or not analysis.get('continuation_seen')
        or not tp1_threatened
    )
    if not (fast_loss and weak_after_entry and not tp1_threatened):
        return

    local6 = _f('local_pre_move_6_pct')
    local12 = _f('local_pre_move_12_pct')
    local24 = _f('local_pre_move_24_pct')
    pre_move = _f('pre_entry_move_pct')
    bullish12 = _i('pre_bullish_candles_12')
    bearish12 = _i('pre_bearish_candles_12')
    close_vs_ema = str(analysis.get('pre_close_vs_ema20') or '').lower().strip()
    entry_pos = _f('entry_position_in_prior_range', _f('entry_position_in_range', 0.5))

    # LONG mirror: quick SL after bearish context means failed demand / no reclaim.
    bearish_context = bool(
        local6 <= -0.18
        or local12 <= -0.30
        or local24 <= -0.45
        or pre_move <= -0.65
        or bearish12 >= bullish12 + 2
        or close_vs_ema == 'below'
        or str(analysis.get('structure_5m') or '').lower().startswith('bearish')
        or analysis.get('structure_against_trade')
    )
    if side_u == 'LONG' and bearish_context:
        analysis['long_failed_demand_bearish_structure'] = True
        analysis['failed_demand_reaction'] = True
        analysis['long_against_bearish_structure'] = True
        analysis['bearish_context_before_entry'] = True
        analysis['reclaim_missing'] = True
        analysis['entry_snapshot_inferred_from_fast_loss'] = True
        if entry_pos <= 0.55 or analysis.get('demand_near_tfs') or analysis.get('long_demand_near_entry'):
            analysis['long_demand_near_entry'] = True
        if analysis.get('supply_near_tfs') or local6 < 0 or local12 < 0:
            analysis['overhead_bearish_fvg'] = bool(analysis.get('overhead_bearish_fvg') or analysis.get('supply_near_tfs'))
        return

    # SHORT mirror: quick SL after bullish reclaim/impulse means failed supply / short into buyer pressure.
    bullish_context = bool(
        local6 >= 0.18
        or local12 >= 0.30
        or local24 >= 0.45
        or pre_move >= 0.65
        or bullish12 >= bearish12 + 2
        or close_vs_ema == 'above'
        or str(analysis.get('structure_5m') or '').lower().startswith('bullish')
        or analysis.get('structure_against_trade')
    )
    if side_u == 'SHORT' and bullish_context:
        analysis['short_failed_supply_bullish_structure'] = True
        analysis['failed_supply_reaction'] = True
        analysis['short_against_bullish_structure'] = True
        analysis['bullish_context_before_entry'] = True
        analysis['bearish_reclaim_missing'] = True
        analysis['entry_snapshot_inferred_from_fast_loss'] = True
        if entry_pos >= 0.35 or analysis.get('supply_near_tfs') or analysis.get('short_supply_near_entry'):
            analysis['short_supply_near_entry'] = True
        if analysis.get('demand_near_tfs') or local6 > 0 or local12 > 0:
            analysis['underlying_bullish_fvg'] = bool(analysis.get('underlying_bullish_fvg') or analysis.get('demand_near_tfs'))
        return


async def _signal_forensics_entry_snapshot_async(sig) -> dict:
    """Build entry_snapshot_json using candles from 5m/15m/30m/1h at signal emit time."""
    snap = _signal_forensics_entry_snapshot(sig)
    try:
        symbol = str(getattr(sig, 'symbol', '') or '').upper().strip()
        market = str(getattr(sig, 'market', 'FUTURES') or 'FUTURES').upper().strip()
        side = str(getattr(sig, 'direction', '') or '').upper().strip()
        entry = _loss_diag_parse_float(getattr(sig, 'entry', None)) or 0.0
        if not symbol or entry <= 0:
            return snap
        tf_snaps: dict = {}
        for tf in _loss_diag_snapshot_tfs():
            try:
                limit = 220 if tf == '5m' else 180
                df = await backend.load_candles(symbol, tf, market=market, limit=limit)
                one = _loss_diag_build_tf_snapshot_from_df(df, entry=entry, side=side, tf=tf, opened_at=None, tp1=(_loss_diag_parse_float(getattr(sig, 'tp1', None)) or 0.0))
                if one:
                    tf_snaps[tf] = one
            except Exception:
                continue
        if tf_snaps:
            snap['tf_snapshots'] = tf_snaps
            snap['loss_diag_timeframes'] = list(tf_snaps.keys())
            snap = _loss_diag_enrich_entry_snapshot_from_tfs(snap, side=side)
    except Exception:
        pass
    return snap


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
    if analysis.get('tf_checked'):
        try:
            lines.append("проверены TF: " + "/".join([str(x) for x in list(analysis.get('tf_checked') or [])]))
        except Exception:
            pass
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
        # Backfill/refresh multi-timeframe entry context for older rows that were
        # created before entry_snapshot_json had 5m/15m/30m/1h candle snapshots.
        # Old snapshots can exist but still be "thin": they may contain only range
        # numbers and no supply/demand/path-block location. Refresh those too, so
        # the report does not fall back to a generic Breakout-trap reason.
        if not isinstance(snapshot, dict):
            snapshot = {}
        existing_tf_snaps = snapshot.get('tf_snapshots') if isinstance(snapshot.get('tf_snapshots'), dict) else {}

        def _tf_snaps_have_location(snaps) -> bool:
            if not isinstance(snaps, dict) or not snaps:
                return False
            side_u = str(side or '').upper().strip()
            for _one in snaps.values():
                if not isinstance(_one, dict):
                    continue
                if _one.get('demand_zone') or _one.get('supply_zone') or _one.get('path_block_zone'):
                    return True
                if _one.get('range_high_blocks_tp1') or _one.get('range_low_blocks_tp1'):
                    return True
                if side_u == 'LONG' and _one.get('entry_near_supply'):
                    return True
                if side_u == 'SHORT' and _one.get('entry_near_demand'):
                    return True
            return False

        if not _tf_snaps_have_location(existing_tf_snaps):
            tf_snaps = {}
            for _tf in _loss_diag_snapshot_tfs():
                try:
                    _limit = 220 if _tf == '5m' else 180
                    _df = await backend.load_candles(symbol, _tf, market=market, limit=_limit)
                    _one = _loss_diag_build_tf_snapshot_from_df(_df, entry=entry, side=side, tf=_tf, opened_at=opened, tp1=tp1)
                    if _one:
                        tf_snaps[_tf] = _one
                except Exception:
                    continue
            if tf_snaps:
                snapshot['tf_snapshots'] = tf_snaps
                snapshot['loss_diag_timeframes'] = list(tf_snaps.keys())
                snapshot = _loss_diag_enrich_entry_snapshot_from_tfs(snapshot, side=side)
                analysis['entry_snapshot_present'] = True
        else:
            snapshot = _loss_diag_enrich_entry_snapshot_from_tfs(snapshot, side=side)
        _loss_diag_apply_snapshot_context(analysis, snapshot, side=side, entry=entry, tp1=tp1, sl=sl)
        _loss_diag_apply_directional_location_flags(analysis, side=side)
    except Exception:
        pass
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
        # Keep a buffer before opened_at because fast SL losses can open and close inside
        # the same 5m candle. Without this, post-entry analysis may be empty and the
        # LOSS card falls back to a generic reason.
        _post_tf_minutes = 5
        d = d[(d['_ts'].notna()) & (d['_ts'] >= (opened - dt.timedelta(hours=8))) & (d['_ts'] <= (closed + dt.timedelta(minutes=10 + _post_tf_minutes)))].copy()
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
            # Local pre-entry direction is more important than the whole 6h window
            # for very fast SL losses (2-7 minutes). It detects bullish reclaim
            # before SHORT and bearish breakdown before LONG.
            for _bars in (6, 12, 24):
                if len(pc) >= _bars:
                    _first = float(pc.tail(_bars).iloc[0])
                    _last = float(pc.tail(_bars).iloc[-1])
                    analysis[f'local_pre_move_{_bars}_pct'] = round(((_last - _first) / max(_first, 1e-9)) * 100.0, 2)
            try:
                _last12 = pre.tail(min(12, len(pre))).copy()
                if not _last12.empty:
                    analysis['pre_bullish_candles_12'] = int((_last12['close'].astype(float) > _last12['open'].astype(float)).sum())
                    analysis['pre_bearish_candles_12'] = int((_last12['close'].astype(float) < _last12['open'].astype(float)).sum())
                _ema20_pre = pc.ewm(span=20, adjust=False).mean()
                if len(_ema20_pre):
                    analysis['pre_close_vs_ema20'] = 'above' if float(pc.iloc[-1]) >= float(_ema20_pre.iloc[-1]) else 'below'
            except Exception:
                pass
            if side == 'LONG':
                clean_space_tp1_pct = ((tp1 - entry) / entry) * 100.0 if tp1 > entry else 0.0
                analysis['clean_space_to_tp1_pct'] = round(clean_space_tp1_pct, 2)
                analysis['entry_in_range_premium'] = bool(analysis.get('entry_in_range_premium') or entry_pos >= 0.68)
                analysis['long_below_strong_high'] = bool(analysis.get('long_below_strong_high') or (dist_high_pct >= 0 and (dist_high_pct <= max(clean_space_tp1_pct * 1.15, 0.35) or entry_pos >= 0.82)))
                analysis['entry_into_supply'] = bool(analysis.get('entry_into_supply') or analysis.get('long_below_strong_high') or (entry_pos >= 0.72 and clean_space_tp1_pct <= 0.45))
                analysis['entry_into_overhead_supply'] = bool(analysis.get('entry_into_overhead_supply') or analysis.get('entry_into_supply'))
                analysis['entry_near_opposite_zone'] = bool(analysis.get('entry_near_opposite_zone') or analysis.get('entry_into_supply'))
                analysis['late_entry_after_exhausted_move'] = bool(analysis.get('late_entry_after_exhausted_move') or (pre_move_pct > 0.65 and entry_pos >= 0.60))
            else:
                clean_space_tp1_pct = ((entry - tp1) / entry) * 100.0 if 0 < tp1 < entry else 0.0
                analysis['clean_space_to_tp1_pct'] = round(clean_space_tp1_pct, 2)
                analysis['entry_in_range_premium'] = bool(analysis.get('entry_in_range_premium') or entry_pos <= 0.32)
                analysis['short_above_weak_low'] = bool(analysis.get('short_above_weak_low') or (dist_low_pct >= 0 and (dist_low_pct <= max(clean_space_tp1_pct * 1.15, 0.35) or entry_pos <= 0.18)))
                analysis['entry_into_demand'] = bool(analysis.get('entry_into_demand') or analysis.get('short_above_weak_low') or (entry_pos <= 0.28 and clean_space_tp1_pct <= 0.45))
                analysis['entry_near_opposite_zone'] = bool(analysis.get('entry_near_opposite_zone') or analysis.get('entry_into_demand'))
                analysis['late_entry_after_exhausted_move'] = bool(analysis.get('late_entry_after_exhausted_move') or (pre_move_pct < -0.65 and entry_pos <= 0.40))

        # Include the candle that contains opened_at. For a 2-7 minute SL, the
        # whole trade often happens inside one 5m candle; using only candles with
        # open_time >= opened_at drops the important failure candle.
        post = d[d['_ts'] >= (opened - dt.timedelta(minutes=_post_tf_minutes))].copy()
        if post.empty:
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
        try:
            _loss_diag_apply_fast_context_flags(analysis, side=side)
        except Exception:
            pass
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

        try:
            _loss_diag_apply_snapshot_context(analysis, snapshot, side=side, entry=entry, tp1=tp1, sl=sl)
            _loss_diag_apply_directional_location_flags(analysis, side=side)
            _loss_diag_apply_fast_context_flags(analysis, side=side)
            _loss_diag_apply_directional_location_flags(analysis, side=side)
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
            'what_happened_lines': [], 'chart_visible_lines': [],
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
    if analysis.get('entry_into_overhead_supply') or analysis.get('entry_into_supply') or analysis.get('long_below_strong_high'):
        improve_keys.append('avoid_overhead_supply')
        improve_keys.append('require_clean_space_to_tp')
    if analysis.get('short_above_weak_low') or analysis.get('entry_into_demand'):
        improve_keys.append('avoid_support_short')
        improve_keys.append('avoid_late_entry')
        improve_keys.append('require_clean_space_to_tp')
    if analysis.get('retest_too_deep'):
        improve_keys.append('tighten_retest_depth')
    if analysis.get('confirm_too_weak'):
        improve_keys.append('raise_min_confirm_strength')
    if analysis.get('long_failed_demand_bearish_structure') or analysis.get('failed_demand_reaction'):
        improve_keys.append('avoid_failed_demand_long')
        improve_keys.append('require_bullish_reclaim')
        improve_keys.append('tighten_trend_alignment')
        if analysis.get('overhead_bearish_fvg') or analysis.get('supply_near_tfs'):
            improve_keys.append('avoid_overhead_bearish_fvg')
    if analysis.get('short_failed_supply_bullish_structure') or analysis.get('failed_supply_reaction'):
        improve_keys.append('avoid_failed_supply_short')
        improve_keys.append('require_bearish_reclaim')
        improve_keys.append('tighten_trend_alignment')
        if analysis.get('underlying_bullish_fvg') or analysis.get('demand_near_tfs'):
            improve_keys.append('avoid_underlying_bullish_fvg')
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
        'chart_visible_lines': list(forensic.get('chart_visible_lines') or []),
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
        "normal pending": "Structure pending trigger",
        "normal pending trigger": "Structure pending trigger",
        "pending": "Structure pending trigger",
        "pending trigger": "Structure pending trigger",
        "обычный trigger": "Structure pending trigger",
        "обычный триггер": "Structure pending trigger",
        "обычный pending trigger": "Structure pending trigger",
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
        # Accept both the old signal text label and the new Russian report-card label.
        # This is used for legacy rows where setup_source/ui_setup_label columns are empty,
        # but the original signal text still contains the setup line.
        m = re.search(
            r"(?:^|\n)\s*(?:🧭\s*)?(?:Smart[-\s]?setup|Setup|Сетап(?:\s+сигнала)?):\s*([^\n\r]+)",
            src,
            flags=re.IGNORECASE,
        )
        if not m:
            return ""
        return _report_setup_label_human(str(m.group(1) or "").strip())
    except Exception:
        return ""

def _report_setup_label_from_row(row: dict) -> str:
    try:
        label = _row_ui_setup_label(row)
        if label:
            return _report_setup_label_sanitize(row, label)

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
                return _report_setup_label_sanitize(row, label)

            try:
                cached_text = str(ORIGINAL_SIGNAL_TEXT.get((0, sid)) or "")
            except Exception:
                cached_text = ""
            label = _report_extract_setup_label_from_text(cached_text)
            if label:
                return _report_setup_label_sanitize(row, label)

        for candidate in (row.get("risk_note"), row.get("orig_text")):
            label = _report_extract_setup_label_from_text(str(candidate or ""))
            if label:
                return _report_setup_label_sanitize(row, label)
    except Exception:
        pass
    return ""


def _loss_card_short_reason_title(code: str, text: str = '') -> str:
    """Short one-line title for LOSS card main reason."""
    code = str(code or '').strip()
    mapping = {
        'short_above_weak_low': 'Short entered directly into demand',
        'entry_into_demand': 'Short entered directly into demand',
        'long_below_strong_high': 'Long entered directly into supply',
        'entry_into_supply': 'Long entered directly into supply',
        'entry_into_overhead_supply': 'Long entered directly into overhead supply',
        'late_entry_after_exhausted_move': 'Late entry after exhausted move',
        'entry_in_range_premium': 'Poor RR location inside range',
        'continuation_missing': 'Weak continuation after entry',
        'immediate_invalidation': 'Immediate invalidation after entry',
        'long_failed_demand_bearish_structure': 'Long into bearish BOS / failed demand',
        'failed_demand_reaction': 'Long into bearish BOS / failed demand',
        'short_failed_supply_bullish_structure': 'Short into bullish BOS / failed supply',
        'failed_supply_reaction': 'Short into bullish BOS / failed supply',
    }
    if code in mapping:
        return mapping[code]
    raw = str(text or '').strip()
    if ':' in raw:
        raw = raw.split(':', 1)[0].strip()
    return raw or _loss_diag_reason_human_label(code)


def _loss_card_location_tp_space_pct() -> float:
    """Max TP1 clean-space (%) that still counts as blocked by nearby opposite zone."""
    try:
        raw = os.getenv('LOSS_CARD_LOCATION_TP_SPACE_PCT', '1.20')
        val = float(str(raw or '1.20').replace(',', '.'))
        return max(0.15, min(2.0, val))
    except Exception:
        return 1.20



def _loss_card_tiny_space_only(analysis: dict) -> bool:
    """True when old/new analysis only knows that TP1 space was tiny.

    Tiny TP1 distance is not proof that LONG entered supply or SHORT entered
    demand. Older versions used to turn tiny space into entry_into_supply /
    entry_into_demand, which made cards say "near range high/low" even when the
    chart was a neutral retest or a move that simply pulled back before TP1.
    """
    if not isinstance(analysis, dict):
        return False
    reason = str(analysis.get('location_override_reason') or '').strip().lower()
    if reason in {
        'long_into_supply_from_tiny_tp1_space',
        'short_into_demand_from_tiny_tp1_space',
        'tiny_tp1_space_no_confirm',
        'long_tiny_tp1_space_no_confirm',
        'short_tiny_tp1_space_no_confirm',
    }:
        return True
    if analysis.get('tp1_too_close_no_clean_space'):
        return True
    return False



def _loss_card_has_real_entry_position(analysis: dict) -> bool:
    """Return True only when entry-position was actually measured, not the 0.50 default."""
    if not isinstance(analysis, dict):
        return False
    if analysis.get('entry_position_in_prior_range') not in (None, ''):
        return True
    if analysis.get('entry_position_in_range') not in (None, ''):
        return True
    return False


def _loss_card_entry_position_line(analysis: dict, *, prefix: str = 'позиция входа в range') -> str:
    """Safe range-position line for cards.

    Old cards printed `0.50` even when no real snapshot existed. That made the card
    look exact while it was only the default. Now we print the range position only
    when the value exists in analysis and the TF is known/derived.
    """
    if not _loss_card_has_real_entry_position(analysis):
        return ''
    try:
        pos = float(analysis.get('entry_position_in_prior_range') if analysis.get('entry_position_in_prior_range') not in (None, '') else analysis.get('entry_position_in_range'))
        if not math.isfinite(pos):
            return ''
        tf = str(analysis.get('entry_position_tf') or analysis.get('preferred_snapshot_tf') or '').strip()
        suffix = f' ({tf})' if tf else ''
        return f'{prefix}: {pos:.2f}{suffix}'
    except Exception:
        return ''




def _loss_card_normalize_visible_position_lines(lines: list[str], analysis: dict) -> list[str]:
    """Remove fake/default range-position lines and replace them with a measured one."""
    out = []
    for x in list(lines or []):
        sx = str(x or '').strip()
        if not sx:
            continue
        if sx.lower().startswith('позиция входа'):
            continue
        out.append(sx)
    pos_line = _loss_card_entry_position_line(analysis)
    if pos_line:
        out.append(pos_line)
    return list(dict.fromkeys(out))


def _loss_card_normalize_analysis_lines(lines: list[str]) -> list[str]:
    """Compact repeated LOSS-card analysis facts.

    Ranked engine can collect the same fact from several detectors (for example
    "fresh bullish displacement отсутствовал" and "bullish displacement отсутствовал").
    Keep the stronger/fresher version once so the Telegram card does not look
    templated or duplicated.
    """
    out: list[str] = []
    seen: set[str] = set()
    buckets: set[str] = set()
    for item in list(lines or []):
        text = str(item or '').strip()
        if not text:
            continue
        low = text.lower()
        key = re.sub(r'\s+', ' ', low)
        if key in seen:
            continue

        bucket = ''
        if 'acceptance' in low and ('отсутств' in low or 'не подтверд' in low):
            bucket = 'no_acceptance'
        elif 'fresh bullish displacement' in low and 'отсутств' in low:
            bucket = 'no_fresh_bullish_displacement'
        elif 'bullish displacement' in low and 'отсутств' in low:
            bucket = 'no_fresh_bullish_displacement'
        elif 'fresh bearish displacement' in low and 'отсутств' in low:
            bucket = 'no_fresh_bearish_displacement'
        elif 'bearish displacement' in low and 'отсутств' in low:
            bucket = 'no_fresh_bearish_displacement'
        elif 'follow-through' in low and ('слаб' in low or 'отсутств' in low):
            bucket = 'weak_followthrough'
        elif 'tp1 не был нормально поставлен под угрозу' in low:
            bucket = 'tp1_not_threatened'
        elif 'entry → tp1' in low or 'entry->tp1' in low or 'entry to tp1' in low:
            bucket = 'entry_tp1_distance'
        elif 'rr до tp1' in low or 'risk/reward до tp1' in low:
            bucket = 'entry_tp1_distance'

        if bucket and bucket in buckets:
            continue
        seen.add(key)
        if bucket:
            buckets.add(bucket)
        out.append(text)
    return out


def _loss_card_real_rr_to_tp1(entry: float, sl: float, tp1: float) -> str:
    """RR used in closed reports must match the printed TP1 line."""
    try:
        entry = float(entry or 0.0)
        sl = float(sl or 0.0)
        tp1 = float(tp1 or 0.0)
        risk = abs(entry - sl)
        reward = abs(tp1 - entry)
        if entry <= 0 or sl <= 0 or tp1 <= 0 or risk <= 0 or reward <= 0:
            return '-'
        return f'{(reward / risk):.2f}'
    except Exception:
        return '-'


def _loss_card_route_label(route: str | None) -> str:
    route = str(route or '').strip()
    labels = {
        'smc_ob_fvg_overlap': 'OB+FVG priority emit',
        'smc_htf_ob_ltf_fvg': 'HTF OB + LTF FVG retest emit',
        'smc_bos_retest_confirm': 'BOS → FVG/OB retest → confirm',
        'smc_displacement_origin': 'Displacement origin fast-path',
        'smc_dual_fvg_origin': 'Dual/stacked FVG origin',
        'smc_liquidity_reclaim': 'Liquidity sweep → reclaim → BOS continuation',
    }
    return labels.get(route, '')


def _report_setup_label_is_generic(label: str | None) -> bool:
    raw = str(label or '').strip().lower().replace('_', ' ')
    raw = re.sub(r'\s+', ' ', raw)
    return raw in {'normal pending trigger', 'normal pending', 'pending trigger', 'pending', 'обычный trigger', 'обычный триггер'}


def _report_setup_label_sanitize(row: dict, label: str) -> str:
    """Avoid showing the useless `normal pending trigger` label when a route is known."""
    try:
        src = row if isinstance(row, dict) else {}
        if not _report_setup_label_is_generic(label):
            return str(label or '').strip()
        route = str(src.get('smc_setup_route') or src.get('emit_route') or '').strip()
        if not route:
            ca = src.get('close_analysis_json')
            if isinstance(ca, str):
                try:
                    ca = json.loads(ca)
                except Exception:
                    ca = {}
            if isinstance(ca, dict):
                route = str(ca.get('route_key') or ca.get('smc_setup_route') or ca.get('emit_route') or '').strip()
        route_label = _loss_card_route_label(route)
        if route_label:
            return route_label
        return 'Structure pending trigger'
    except Exception:
        return str(label or '').strip()


def _loss_card_apply_price_location_override(src: dict, analysis: dict, *, side: str) -> None:
    """Prefer the visible chart-location reason over a generic breakout trap."""
    if not isinstance(analysis, dict):
        return
    side_u = str(side or src.get('side') or analysis.get('side') or '').upper().strip()
    if side_u not in ('LONG', 'SHORT'):
        return

    def _f(obj, key: str, default: float = 0.0) -> float:
        try:
            val = obj.get(key) if isinstance(obj, dict) else None
            if val is None or val == '':
                return default
            return float(str(val).replace(',', '.'))
        except Exception:
            return default

    entry = _f(src, 'entry') or _f(analysis, 'entry')
    tp1 = _f(src, 'tp1') or _f(analysis, 'tp1')
    if entry <= 0 or tp1 <= 0:
        return

    clean_space = _f(analysis, 'clean_space_to_tp1_pct')
    if clean_space <= 0:
        if side_u == 'LONG' and tp1 > entry:
            clean_space = ((tp1 - entry) / entry) * 100.0
        elif side_u == 'SHORT' and 0 < tp1 < entry:
            clean_space = ((entry - tp1) / entry) * 100.0
        if clean_space > 0:
            analysis['clean_space_to_tp1_pct'] = round(clean_space, 3)

    try:
        duration_min = int(float(analysis.get('duration_min') or 0))
    except Exception:
        duration_min = 0
    tp1_known = 'tp1_threatened' in analysis
    tp1_threatened = bool(analysis.get('tp1_threatened'))
    weak_or_fast = bool(
        analysis.get('weak_followthrough')
        or analysis.get('no_post_entry_expansion')
        or analysis.get('zone_reaction_too_weak')
        or analysis.get('fast_invalidation')
        or analysis.get('immediate_invalidation')
        or analysis.get('continuation_seen') is False
        or (tp1_known and not tp1_threatened)
        or (duration_min and duration_min <= 20)
    )
    if not weak_or_fast:
        return

    max_space = _loss_card_location_tp_space_pct()
    no_clean_space = bool(0 < clean_space <= max_space and (not tp1_threatened))

    try:
        entry_pos = float(analysis.get('entry_position_in_prior_range') or analysis.get('entry_position_in_range') or 0.5)
    except Exception:
        entry_pos = 0.5

    if side_u == 'LONG':
        supply_near = bool(
            analysis.get('supply_near_tfs')
            or analysis.get('overhead_bearish_fvg')
            or analysis.get('entry_into_supply')
            or analysis.get('entry_into_overhead_supply')
            or analysis.get('long_below_strong_high')
        )
        bullish_pre = False
        bearish_pre = False
        for key in ('local_pre_move_6_pct', 'local_pre_move_12_pct', 'local_pre_move_24_pct', 'pre_entry_move_pct'):
            try:
                v = float(analysis.get(key) or 0.0)
                if v >= (0.12 if key.startswith('local') else 0.45):
                    bullish_pre = True
                if v <= (-0.12 if key.startswith('local') else -0.45):
                    bearish_pre = True
            except Exception:
                pass
        highish = bool(entry_pos >= 0.72)
        if no_clean_space and not (supply_near or highish or bearish_pre):
            analysis['tp1_too_close_no_clean_space'] = True
            analysis['location_override_reason'] = 'long_tiny_tp1_space_no_confirm'
        if supply_near or highish or (bearish_pre and not bullish_pre):
            if supply_near or highish:
                analysis['long_below_strong_high'] = True
                analysis['entry_into_supply'] = True
                analysis['entry_into_overhead_supply'] = True
                analysis['entry_near_opposite_zone'] = True
                analysis['entry_in_range_premium'] = True
                analysis['overhead_bearish_fvg'] = bool(analysis.get('overhead_bearish_fvg') or supply_near)
            if bearish_pre and not bullish_pre:
                # Exact case: LONG is not only “into supply”; it is a buy
                # against bearish structure while resistance/supply is overhead.
                analysis['bearish_context_before_entry'] = True
                analysis['long_against_bearish_structure'] = True
                analysis['long_bearish_continuation_under_supply'] = True
                analysis['reclaim_missing'] = True
                analysis['location_override_reason'] = 'long_against_bearish_structure_under_supply'
        return

    demand_near = bool(
        analysis.get('demand_near_tfs')
        or analysis.get('underlying_bullish_fvg')
        or analysis.get('entry_into_demand')
        or analysis.get('short_above_weak_low')
    )
    bearish_pre = False
    for key in ('local_pre_move_6_pct', 'local_pre_move_12_pct', 'local_pre_move_24_pct', 'pre_entry_move_pct'):
        try:
            if float(analysis.get(key) or 0.0) <= (-0.12 if key.startswith('local') else -0.45):
                bearish_pre = True
                break
        except Exception:
            pass
    lowish = bool(entry_pos <= 0.28)
    if no_clean_space and not (demand_near or lowish or bearish_pre):
        analysis['tp1_too_close_no_clean_space'] = True
        analysis['location_override_reason'] = 'short_tiny_tp1_space_no_confirm'
    if demand_near or lowish or bearish_pre:
        if demand_near or lowish:
            analysis['short_above_weak_low'] = True
            analysis['entry_into_demand'] = True
            analysis['entry_near_opposite_zone'] = True
            analysis['entry_in_range_discount'] = True
            analysis['underlying_bullish_fvg'] = bool(analysis.get('underlying_bullish_fvg') or demand_near)
        if bearish_pre:
            analysis['late_entry_after_exhausted_move'] = True


def _report_price_precision(values) -> int:
    """Choose enough decimals so small-cap coin levels are not rounded equal."""
    nums = []
    for v in values or []:
        try:
            fv = float(v)
            if math.isfinite(fv) and fv > 0:
                nums.append(fv)
        except Exception:
            continue
    if not nums:
        return 6
    mn = min(abs(x) for x in nums if x > 0)
    if mn >= 1000:
        prec = 2
    elif mn >= 100:
        prec = 3
    elif mn >= 10:
        prec = 4
    elif mn >= 1:
        prec = 5
    elif mn >= 0.1:
        prec = 6
    elif mn >= 0.01:
        prec = 7
    elif mn >= 0.001:
        prec = 8
    elif mn >= 0.0001:
        prec = 9
    else:
        prec = 10

    diffs = []
    for i, a in enumerate(nums):
        for b in nums[i + 1:]:
            d = abs(a - b)
            if d > 0:
                diffs.append(d)
    if diffs:
        try:
            prec = max(prec, int(math.ceil(-math.log10(min(diffs)))) + 2)
        except Exception:
            pass
    try:
        cap = int(float(os.getenv('LOSS_CARD_PRICE_MAX_DECIMALS', '12') or 12))
    except Exception:
        cap = 12
    return max(2, min(max(6, cap), prec))


def _fmt_report_price(value, precision: int) -> str:
    try:
        fv = float(value)
        if not math.isfinite(fv) or fv <= 0:
            return '—'
        precision = int(max(2, min(14, precision)))
        return f"{fv:.{precision}f}"
    except Exception:
        return str(value) if value is not None else '—'


def _loss_card_float(analysis: dict, key: str, default: float = 0.0) -> float:
    try:
        val = analysis.get(key) if isinstance(analysis, dict) else None
        if val is None or val == '':
            return default
        return float(str(val).replace(',', '.'))
    except Exception:
        return default


def _loss_card_bool(analysis: dict, key: str) -> bool:
    val = analysis.get(key) if isinstance(analysis, dict) else None
    if isinstance(val, str):
        return val.strip().lower() in ('1', 'true', 'yes', 'on', 'да', 'есть')
    return bool(val)



def _loss_card_zone_is_fvg(zone: dict, side: str = '') -> bool:
    """True only when the saved pre-entry snapshot contains a real FVG zone.

    Some snapshots store the kind as ``bearish_fvg``/``bullish_fvg``, others as
    ``fvg_bearish``, ``red_fvg``, ``supply_fvg`` or simply ``fvg`` with a
    separate side/type/source field. Keep the detector conservative, but support
    all these saved formats so a real FVG does not fall back to generic
    resistance/support wording.
    """
    if not isinstance(zone, dict) or not zone:
        return False
    kind = str(zone.get('kind') or zone.get('type') or zone.get('zone_type') or '').lower()
    zside = str(zone.get('side') or side or '').lower()
    source = str(zone.get('source') or zone.get('name') or zone.get('label') or '').lower()
    text = f"{kind} {zside} {source}"
    if 'fvg' not in text and 'fair_value_gap' not in text and 'fair value gap' not in text:
        return False
    if zside == 'supply':
        return bool(any(x in text for x in ('bearish', 'red', 'supply', 'seller', 'fvg')))
    if zside == 'demand':
        return bool(any(x in text for x in ('bullish', 'green', 'demand', 'buyer', 'fvg')))
    return True


def _loss_card_zone_desc(zone: dict, fallback: str) -> str:
    if not isinstance(zone, dict) or not zone:
        return fallback

    kind = str(zone.get('kind') or zone.get('type') or zone.get('zone_type') or '').lower()
    side = str(zone.get('side') or '').lower()
    source = str(zone.get('source') or zone.get('name') or zone.get('label') or '').lower()
    ztext = f"{kind} {side} {source}"

    # Do not call a simple range high/low blocker "FVG".
    # It is a real chart blocker, but the card must say resistance/support, not red/green FVG.
    if 'recent_swing_high' in kind or 'range_high' in source:
        base = 'recent swing high / resistance'
    elif 'recent_swing_low' in kind or 'range_low' in source:
        base = 'recent swing low / support'
    elif ('fvg' in ztext or 'fair_value_gap' in ztext or 'fair value gap' in ztext) and (side == 'supply' or any(x in ztext for x in ('bearish', 'red', 'seller'))):
        base = 'red FVG / seller zone'
    elif ('fvg' in ztext or 'fair_value_gap' in ztext or 'fair value gap' in ztext) and (side == 'demand' or any(x in ztext for x in ('bullish', 'green', 'buyer'))):
        base = 'green FVG / buyer zone'
    elif side == 'supply':
        base = 'seller reaction / resistance zone'
    elif side == 'demand':
        base = 'buyer reaction / demand zone'
    else:
        base = fallback

    tf = str(zone.get('tf') or '').strip()
    rel = str(zone.get('relation') or '').strip()
    dist = zone.get('distance_pct')
    parts = [base]
    if tf:
        parts.append(f'TF {tf}')
    if rel in ('blocks_target', 'inside_blocks_target'):
        parts.append('между входом и TP1')
    elif rel == 'inside':
        parts.append('вход внутри зоны')
    elif dist not in (None, ''):
        try:
            parts.append(f'дистанция {float(dist):.2f}%')
        except Exception:
            pass
    return ' / '.join(parts)


def _loss_card_real_zone_context(analysis: dict, side: str) -> dict:
    """Return pre-entry zone/resistance/support context without inventing FVGs.

    Boolean flags like entry_into_supply/entry_into_demand may be inferred from price
    location. They are useful, but they are NOT proof that a red/green FVG existed
    before entry. This helper separates actual saved zone/path blocker from inferred
    price-location flags so LOSS cards do not lie about what is visible.
    """
    analysis = analysis if isinstance(analysis, dict) else {}
    side_l = str(side or '').lower().strip()
    if side_l not in ('supply', 'demand'):
        return {'zone': {}, 'tfs': [], 'has_zone': False, 'is_fvg': False, 'is_path_block': False, 'blocks_tp1': False, 'desc': 'reaction zone', 'fvg_word': 'FVG'}

    zone_key = 'nearest_supply_zone' if side_l == 'supply' else 'nearest_demand_zone'
    tfs_key = 'supply_near_tfs' if side_l == 'supply' else 'demand_near_tfs'
    blocks_key = 'supply_zone_blocks_tp1' if side_l == 'supply' else 'demand_zone_blocks_tp1'
    relation_key = 'supply_zone_relation' if side_l == 'supply' else 'demand_zone_relation'

    zone = analysis.get(zone_key) if isinstance(analysis.get(zone_key), dict) else {}
    tfs = [str(x) for x in list(analysis.get(tfs_key) or []) if str(x).strip()]
    relation = str(analysis.get(relation_key) or '').strip()

    def _truthy(v):
        if isinstance(v, str):
            return v.strip().lower() in ('1', 'true', 'yes', 'on', 'да', 'есть')
        return bool(v)

    if side_l == 'supply':
        fvg_aliases = (
            'overhead_bearish_fvg', 'bearish_fvg_above_entry', 'red_fvg_above_entry',
            'supply_fvg_above_entry', 'nearest_bearish_fvg', 'nearest_supply_fvg',
            'fvg_above_entry', 'fvg_overhead', 'tp1_behind_bearish_fvg',
            'supply_fvg_blocks_tp1', 'bearish_fvg_blocks_tp1',
        )
        block_aliases = (
            'supply_zone_blocks_tp1', 'supply_fvg_blocks_tp1', 'bearish_fvg_blocks_tp1',
            'tp1_behind_bearish_fvg', 'tp1_behind_supply_fvg',
        )
    else:
        fvg_aliases = (
            'underlying_bullish_fvg', 'bullish_fvg_below_entry', 'green_fvg_below_entry',
            'demand_fvg_below_entry', 'nearest_bullish_fvg', 'nearest_demand_fvg',
            'fvg_below_entry', 'fvg_under_entry', 'tp1_behind_bullish_fvg',
            'demand_fvg_blocks_tp1', 'bullish_fvg_blocks_tp1',
        )
        block_aliases = (
            'demand_zone_blocks_tp1', 'demand_fvg_blocks_tp1', 'bullish_fvg_blocks_tp1',
            'tp1_behind_bullish_fvg', 'tp1_behind_demand_fvg',
        )

    alias_fvg = bool(any(_truthy(analysis.get(k)) for k in fvg_aliases))
    alias_blocks = bool(any(_truthy(analysis.get(k)) for k in block_aliases))
    tfs_has_fvg = bool(any(('fvg' in x.lower() or 'fair value gap' in x.lower()) for x in tfs))
    blocks = bool(analysis.get(blocks_key) or alias_blocks or relation in ('blocks_target', 'inside_blocks_target'))

    kind = str(zone.get('kind') or zone.get('type') or zone.get('zone_type') or '').lower() if zone else ''
    source = str(zone.get('source') or zone.get('name') or zone.get('label') or '').lower() if zone else ''
    is_fvg = bool(_loss_card_zone_is_fvg(zone, side_l) or alias_fvg or tfs_has_fvg)
    is_path = bool('recent_swing' in kind or 'range_' in source or 'path' in source)
    has_zone = bool(zone or tfs or blocks or alias_fvg)

    if side_l == 'supply':
        if is_fvg:
            base = 'red FVG / seller zone'
            fvg_word = 'red FVG / supply'
        elif is_path:
            base = 'recent swing high / resistance'
            fvg_word = 'resistance / supply'
        elif has_zone:
            base = 'seller reaction / resistance zone'
            fvg_word = 'supply / resistance'
        else:
            base = 'local high / resistance'
            fvg_word = 'local high / resistance'
    else:
        if is_fvg:
            base = 'green FVG / buyer zone'
            fvg_word = 'green FVG / demand'
        elif is_path:
            base = 'recent swing low / support'
            fvg_word = 'support / demand'
        elif has_zone:
            base = 'buyer reaction / demand zone'
            fvg_word = 'demand / support'
        else:
            base = 'local low / support'
            fvg_word = 'local low / support'

    return {
        'zone': zone,
        'tfs': tfs,
        'has_zone': has_zone,
        'is_fvg': is_fvg,
        'is_path_block': is_path,
        'blocks_tp1': blocks,
        'desc': _loss_card_zone_desc(zone, base) if zone else base,
        'fvg_word': fvg_word,
    }



def _loss_card_exact_location_variant(side: str, analysis: dict, duration_min: int | None = None) -> dict:
    """Pick the exact chart-visible LOSS reason instead of one generic label.

    The wording is conservative: it says "red/green FVG" only when a real FVG was
    present in the saved pre-entry snapshot. If the blocker was a swing high/low or
    just bad range location, the card says resistance/support/local high/low instead.
    """
    side_u = str(side or '').upper().strip()
    analysis = dict(analysis or {})
    cs = _loss_card_float(analysis, 'clean_space_to_tp1_pct')
    entry_pos = _loss_card_float(analysis, 'entry_position_in_prior_range', _loss_card_float(analysis, 'entry_position_in_range', 0.5))
    local6 = _loss_card_float(analysis, 'local_pre_move_6_pct')
    local12 = _loss_card_float(analysis, 'local_pre_move_12_pct')
    pre_move = _loss_card_float(analysis, 'pre_entry_move_pct')
    immediate = bool(_loss_card_bool(analysis, 'immediate_invalidation') or _loss_card_bool(analysis, 'fast_invalidation') or (duration_min is not None and duration_min <= 8))
    no_tp_space = bool(0 < cs <= _loss_card_location_tp_space_pct())

    if side_u == 'LONG':
        ctx = _loss_card_real_zone_context(analysis, 'supply')
        zone_desc = str(ctx.get('desc') or 'local high / resistance')
        fvg_word = str(ctx.get('fvg_word') or 'local high / resistance')
        blocks_tp1 = bool(ctx.get('blocks_tp1'))
        late_pump = bool(_loss_card_bool(analysis, 'late_entry_after_exhausted_move') or local6 >= 0.12 or local12 >= 0.22 or pre_move >= 0.45)
        bearish_context = bool(
            _loss_card_bool(analysis, 'bearish_context_before_entry')
            or _loss_card_bool(analysis, 'long_against_bearish_structure')
            or _loss_card_bool(analysis, 'long_bearish_continuation_under_supply')
            or _loss_card_bool(analysis, 'reclaim_missing')
            or local6 <= -0.12
            or local12 <= -0.22
            or pre_move <= -0.45
            or str(analysis.get('structure_5m') or '').lower().startswith('bearish')
            or str(analysis.get('trend_context') or '').lower().startswith('bearish')
        )
        strong_high = bool(_loss_card_bool(analysis, 'long_below_strong_high') or entry_pos >= 0.72)
        # Keep real supply/resistance separate from a synthetic "entry is high" flag.
        # A high entry can be a poor location, but it is not proof that a red FVG existed.
        has_real_zone = bool(ctx.get('has_zone'))
        has_supply_context = bool(has_real_zone or _loss_card_bool(analysis, 'overhead_bearish_fvg') or blocks_tp1)

        if immediate and has_supply_context:
            if has_real_zone:
                primary = 'Immediate rejection from overhead supply'
                scenario = f'LONG вошёл прямо перед {fvg_word}. Реакция продавца появилась почти сразу, поэтому покупатель не успел построить continuation и цена ушла к SL.'
                visible0 = f'над входом была {zone_desc}'
                secondary_zone = 'Long into supply'
                improve0 = 'не входить прямо перед seller reaction zone'
            else:
                primary = 'Immediate rejection near local high'
                scenario = 'LONG был открыт рядом с локальным high / resistance после уже сделанного движения вверх. Покупатель не получил clean continuation, цена сразу дала rejection и ушла к SL.'
                visible0 = 'вход был рядом с локальным high / resistance, а не из discount'
                secondary_zone = 'Long near local high'
                improve0 = 'не входить в LONG прямо возле локального high без clean breakout'
            return {
                'pattern': 'immediate_long_rejection',
                'primary_text': primary,
                'scenario_text': scenario,
                'analysis_add': ['immediate rejection after entry', 'bullish follow-through отсутствовал'],
                'visible': [
                    visible0,
                    'первая реакция после входа была вниз',
                    'bullish continuation не закрепился',
                    'SL был выбит без нормального движения к TP1',
                ],
                'secondary': ['Immediate rejection', secondary_zone, 'Weak follow-through', 'SL hit without meaningful excursion'],
                'improve': [improve0, 'ждать закрепление выше уровня/зоны', 'после быстрого rejection считать setup invalid'],
            }

        if bearish_context and not late_pump:
            zone_line = f'LONG открыт под {zone_desc}' if has_real_zone else 'LONG открыт против bearish context рядом с resistance/high'
            return {
                'pattern': 'long_against_bearish_structure_under_resistance',
                'primary_text': 'Long against bearish structure / failed reclaim',
                'scenario_text': f'LONG был открыт не из чистого bullish continuation: структура была bearish или reclaim не закрепился. Цена не смогла закрепиться выше {fvg_word}, поэтому покупатель не получил продолжение и движение ушло к SL.',
                'analysis_add': ['bearish structure перед входом', 'bullish reclaim отсутствовал', 'upside expansion отсутствовал'],
                'visible': [
                    'перед входом структура уже была bearish / продавец контролировал движение',
                    zone_line,
                    'после входа не было clean bullish reclaim',
                    'это был не clean breakout continuation, а покупка против seller pressure',
                ],
                'secondary': ['Long against bearish structure', 'Failed reclaim', 'Weak upside continuation', 'Bearish continuation resumed', 'Poor RR location'],
                'improve': ['не брать LONG против bearish 5m/15m без reclaim', 'ждать закрепление выше resistance/supply', 'требовать clean space до TP1'],
            }

        if late_pump and has_supply_context and (strong_high or no_tp_space or blocks_tp1):
            visible_zone = f'сверху находился {zone_desc}' if has_real_zone else 'сверху был local high / resistance'
            return {
                'pattern': 'late_long_after_pump_into_supply_resistance',
                'primary_text': 'Late LONG after pump into supply/resistance',
                'scenario_text': f'LONG был открыт после уже отработанного buy-side impulse прямо под {fvg_word}. Покупатель уже был уставший, а путь к TP1 проходил через resistance/supply, поэтому вместо fresh bullish continuation цена дала rejection/pullback к SL.',
                'analysis_add': ['вход после уже отработанного pump', 'TP1/путь вверх был заблокирован resistance/supply', 'fresh bullish displacement после входа отсутствовал'],
                'visible': [
                    'перед входом уже был buy-side impulse',
                    'LONG открыт поздно в premium/верхней части range',
                    visible_zone,
                    'между входом и TP1 не было чистого upside-space',
                    'покупатель не получил нового displacement после входа',
                ],
                'secondary': ['Late long after pump', 'Long into supply/resistance', 'Buyer exhaustion', 'No clean space to TP1', 'Weak upside continuation'],
                'improve': ['после сильного pump не лонговать прямо под high/resistance', 'ждать discount re-entry ниже', 'входить только после acceptance выше resistance', 'если TP1 упирается в supply — цель ставить раньше или пропускать'],
            }

        if blocks_tp1 or (no_tp_space and bool(ctx.get('has_zone'))):
            return {
                'pattern': 'tp1_blocked_by_overhead_zone',
                'primary_text': 'TP1 blocked by overhead supply/resistance',
                'scenario_text': f'LONG был открыт под {fvg_word}, а TP1 находился прямо в зоне продавца/сопротивления или сразу за ней. Формальный RR был, но чистого пространства до TP1 не было.',
                'analysis_add': ['TP1 был заблокирован overhead zone', 'upside expansion отсутствовал'],
                'visible': [
                    f'TP1 упирался в {zone_desc}',
                    'между входом и TP1 не было clean space',
                    'seller reaction zone стояла на пути LONG',
                    'после входа не появился clean bullish displacement',
                ],
                'secondary': ['TP1 blocked by supply/resistance', 'No clean space to TP1', 'Weak upside continuation', 'Poor RR location'],
                'improve': ['не брать LONG, если TP1 внутри/сразу за resistance/supply', 'требовать чистое пространство до TP1', 'ждать вход ниже от discount / demand'],
            }

        if late_pump and (strong_high or no_tp_space):
            visible_zone = f'сверху находился {zone_desc}' if has_real_zone else 'сверху был локальный high / resistance'
            return {
                'pattern': 'late_long_after_exhausted_pump',
                'primary_text': 'Late LONG after exhausted pump near high',
                'scenario_text': 'LONG был открыт после уже отработанного buy-side impulse. Цена пришла в premium/верх range, поэтому продолжение вверх было слабым и быстро начался откат/rejection.',
                'analysis_add': ['вход после уже отработанного pump', 'fresh bullish displacement после входа отсутствовал'],
                'visible': [
                    'перед входом уже был buy-side impulse',
                    'LONG открыт в premium/верхней части range, а не из discount',
                    visible_zone,
                    'покупатель не получил нового displacement после входа',
                ],
                'secondary': ['Late entry', 'Buy-side exhaustion', 'Weak upside continuation', 'No post-entry expansion'],
                'improve': ['не покупать после вертикального pump возле high/resistance', 'ждать pullback ниже', 'требовать новый bullish displacement после retest'],
            }

        return {
            'pattern': 'long_poor_location_near_high',
            'primary_text': 'Long near local high / poor RR location',
            'scenario_text': 'LONG был открыт высоко в локальном range. До TP1 оставалось мало чистого пространства, а покупатель не показал нового displacement после входа, поэтому цена быстро вернулась против позиции.',
            'analysis_add': ['вход высоко в range', 'upside expansion отсутствовал'],
            'visible': [
                'LONG открыт в верхней части локального range',
                'сверху был local high / resistance, а не обязательно red FVG',
                'upside для LONG был ограничен',
                'после входа не было clean bullish displacement',
            ],
            'secondary': ['Poor RR location', 'Weak upside continuation', 'No clean space to TP1', 'Buy-side exhaustion'],
            'improve': ['не лонговать высоко в range без clean breakout', 'требовать чистое пространство до TP1', 'ждать discount re-entry ниже'],
        }

    if side_u == 'SHORT':
        ctx = _loss_card_real_zone_context(analysis, 'demand')
        zone_desc = str(ctx.get('desc') or 'local low / support')
        fvg_word = str(ctx.get('fvg_word') or 'local low / support')
        blocks_tp1 = bool(ctx.get('blocks_tp1'))
        late_dump = bool(_loss_card_bool(analysis, 'late_entry_after_exhausted_move') or local6 <= -0.12 or local12 <= -0.22 or pre_move <= -0.45)
        bullish_context = bool(
            _loss_card_bool(analysis, 'bullish_context_before_entry')
            or _loss_card_bool(analysis, 'short_against_bullish_structure')
            or _loss_card_bool(analysis, 'short_bullish_continuation_over_demand')
            or _loss_card_bool(analysis, 'bearish_reclaim_missing')
            or local6 >= 0.12
            or local12 >= 0.22
            or pre_move >= 0.45
            or str(analysis.get('structure_5m') or '').lower().startswith('bullish')
            or str(analysis.get('trend_context') or '').lower().startswith('bullish')
        )
        strong_low = bool(_loss_card_bool(analysis, 'short_above_weak_low') or entry_pos <= 0.28)
        # Keep real demand/support separate from a synthetic "entry is low" flag.
        # A low entry can be a late/discount SHORT, but it is not proof that a green FVG existed.
        has_real_zone = bool(ctx.get('has_zone'))
        has_demand_context = bool(has_real_zone or _loss_card_bool(analysis, 'underlying_bullish_fvg') or blocks_tp1)

        if immediate and has_demand_context:
            if has_real_zone:
                primary = 'Immediate bounce from underlying demand'
                scenario = f'SHORT вошёл прямо перед {fvg_word}. Реакция покупателя появилась почти сразу, поэтому продавец не успел построить continuation и цена ушла к SL.'
                visible0 = f'под входом была {zone_desc}'
                secondary_zone = 'Short into demand'
                improve0 = 'не входить прямо перед buyer reaction zone'
            else:
                primary = 'Immediate bounce near local low'
                scenario = 'SHORT был открыт рядом с локальным low / support после уже сделанного движения вниз. Продавец не получил clean continuation, цена сразу дала bounce и ушла к SL.'
                visible0 = 'вход был рядом с локальным low / support, а не из premium'
                secondary_zone = 'Short near local low'
                improve0 = 'не входить в SHORT прямо возле локального low без clean breakdown'
            return {
                'pattern': 'immediate_short_bounce',
                'primary_text': primary,
                'scenario_text': scenario,
                'analysis_add': ['immediate bounce after entry', 'bearish follow-through отсутствовал'],
                'visible': [
                    visible0,
                    'первая реакция после входа была вверх',
                    'bearish continuation не закрепился',
                    'SL был выбит без нормального движения к TP1',
                ],
                'secondary': ['Immediate bounce', secondary_zone, 'Weak follow-through', 'SL hit without meaningful excursion'],
                'improve': [improve0, 'ждать закрепление ниже уровня/зоны', 'после быстрого bounce считать setup invalid'],
            }

        if bullish_context and not late_dump:
            zone_line = f'SHORT открыт над {zone_desc}' if has_real_zone else 'SHORT открыт против bullish context рядом с support/low'
            return {
                'pattern': 'short_against_bullish_structure_over_support',
                'primary_text': 'Short against bullish structure / failed bearish reclaim',
                'scenario_text': f'SHORT был открыт не из чистого bearish continuation: структура была bullish или bearish reclaim не закрепился. Цена не смогла закрепиться ниже {fvg_word}, поэтому продавец не получил продолжение и движение ушло к SL.',
                'analysis_add': ['bullish structure перед входом', 'bearish reclaim отсутствовал', 'downside expansion отсутствовал'],
                'visible': [
                    'перед входом структура уже была bullish / покупатель контролировал движение',
                    zone_line,
                    'после входа не было clean bearish reclaim',
                    'это был не clean breakdown continuation, а SHORT против buyer pressure',
                ],
                'secondary': ['Short against bullish structure', 'Failed bearish reclaim', 'Weak downside continuation', 'Bullish continuation resumed', 'Poor RR location'],
                'improve': ['не брать SHORT против bullish 5m/15m без bearish reclaim', 'ждать закрепление ниже demand/support', 'требовать clean space до TP1'],
            }

        if late_dump and has_demand_context and (strong_low or no_tp_space or blocks_tp1):
            visible_zone = f'снизу находился {zone_desc}' if has_real_zone else 'снизу был local low / support'
            return {
                'pattern': 'late_short_after_dump_into_demand_support',
                'primary_text': 'Late SHORT after dump into demand/support',
                'scenario_text': f'SHORT был открыт после уже отработанного sell-side impulse прямо над {fvg_word}. Продавец уже был уставший, а путь к TP1 проходил через support/demand, поэтому вместо fresh bearish continuation цена дала bounce/reclaim к SL.',
                'analysis_add': ['вход после уже отработанного dump', 'TP1/путь вниз был заблокирован demand/support', 'fresh bearish displacement после входа отсутствовал'],
                'visible': [
                    'перед входом уже был sell-side impulse',
                    'SHORT открыт поздно в discount/нижней части range',
                    visible_zone,
                    'между входом и TP1 не было чистого downside-space',
                    'продавец не получил нового displacement после входа',
                ],
                'secondary': ['Late short after dump', 'Short into demand/support', 'Seller exhaustion', 'No clean space to TP1', 'Weak downside continuation'],
                'improve': ['после сильного dump не шортить прямо над low/support', 'ждать premium re-entry выше', 'входить только после breakdown ниже support', 'если TP1 упирается в demand — цель ставить раньше или пропускать'],
            }

        if blocks_tp1 or (no_tp_space and bool(ctx.get('has_zone'))):
            return {
                'pattern': 'tp1_blocked_by_underlying_zone',
                'primary_text': 'TP1 blocked by underlying demand/support',
                'scenario_text': f'SHORT был открыт над {fvg_word}, а TP1 находился прямо в зоне покупателя/поддержки или сразу за ней. Формальный RR был, но sell-side путь был заблокирован.',
                'analysis_add': ['TP1 был заблокирован underlying zone', 'downside expansion отсутствовал'],
                'visible': [
                    f'TP1 упирался в {zone_desc}',
                    'между входом и TP1 не было clean downside space',
                    'buyer reaction zone стояла на пути SHORT',
                    'после входа не появился clean bearish displacement',
                ],
                'secondary': ['TP1 blocked by demand/support', 'No clean space to TP1', 'Weak downside continuation', 'Poor RR location'],
                'improve': ['не брать SHORT, если TP1 внутри/сразу за demand/support', 'требовать чистое пространство до TP1', 'ждать вход выше от premium / supply'],
            }

        if late_dump and (strong_low or no_tp_space):
            visible_zone = f'снизу находился {zone_desc}' if has_real_zone else 'снизу был локальный low / support'
            return {
                'pattern': 'late_short_after_exhausted_dump',
                'primary_text': 'Late SHORT after exhausted dump near low',
                'scenario_text': 'SHORT был открыт после уже отработанного sell-side impulse. Цена пришла в discount/низ range, поэтому продолжение вниз было слабым и быстро начался bounce.',
                'analysis_add': ['вход после уже отработанного dump', 'fresh bearish displacement после входа отсутствовал'],
                'visible': [
                    'перед входом уже был sell-side impulse',
                    'SHORT открыт в discount/нижней части range, а не из premium',
                    visible_zone,
                    'продавец не получил нового displacement после входа',
                ],
                'secondary': ['Late entry', 'Sell-side exhaustion', 'Weak downside continuation', 'No post-entry expansion'],
                'improve': ['не шортить после вертикального dump возле low/support', 'ждать pullback выше', 'требовать новый bearish displacement после retest'],
            }

        return {
            'pattern': 'short_poor_location_near_low',
            'primary_text': 'Short near local low / poor RR location',
            'scenario_text': 'SHORT был открыт низко в локальном range. До TP1 оставалось мало чистого пространства, а продавец не показал нового displacement после входа, поэтому цена быстро дала bounce против позиции.',
            'analysis_add': ['вход низко в range', 'downside expansion отсутствовал'],
            'visible': [
                'SHORT открыт в нижней части локального range',
                'снизу был local low / support, а не обязательно green FVG',
                'downside для SHORT был ограничен',
                'после входа не было clean bearish displacement',
            ],
            'secondary': ['Poor RR location', 'Weak downside continuation', 'No clean space to TP1', 'Sell-side exhaustion'],
            'improve': ['не шортить низко в range без clean breakdown', 'требовать чистое пространство до TP1', 'ждать premium re-entry выше'],
        }
    return {}




def _loss_card_graph_visible_override_payload(src: dict, analysis: dict, *, side: str, duration_min: int | None = None) -> dict:
    """Final anti-template guard for LOSS cards.

    If diagnostics still lead to a generic "Weak confirm" card, use the actual
    candle context around entry: pre-entry impulse, entry position in range,
    clean space to TP1, and TP1 threat. The card should describe the visible
    chart reason, not only the fact that continuation was weak.
    """
    src = dict(src or {})
    analysis = dict(analysis or {})
    side_u = str(side or src.get('side') or analysis.get('side') or '').upper().strip()
    if side_u not in ('LONG', 'SHORT'):
        return {}

    def f(k: str, default: float = 0.0) -> float:
        try:
            v = analysis.get(k)
            return default if v in (None, '') else float(str(v).replace(',', '.'))
        except Exception:
            return default

    def b(k: str) -> bool:
        v = analysis.get(k)
        if isinstance(v, str):
            return v.strip().lower() in ('1', 'true', 'yes', 'on', 'да', 'есть')
        return bool(v)

    def pct_line(label: str, value: float, threshold: float = 0.01) -> str:
        return f'{label}: {value:+.2f}%' if abs(value) >= threshold else ''

    try:
        entry = float(str(src.get('entry') or analysis.get('entry') or 0).replace(',', '.'))
        tp1 = float(str(src.get('tp1') or analysis.get('tp1') or 0).replace(',', '.'))
    except Exception:
        entry = 0.0
        tp1 = 0.0

    cs = f('clean_space_to_tp1_pct')
    if cs <= 0 and entry > 0 and tp1 > 0:
        if side_u == 'LONG' and tp1 > entry:
            cs = ((tp1 - entry) / entry) * 100.0
        elif side_u == 'SHORT' and 0 < tp1 < entry:
            cs = ((entry - tp1) / entry) * 100.0

    pos = f('entry_position_in_prior_range', f('entry_position_in_range', 0.5))
    l6 = f('local_pre_move_6_pct')
    l12 = f('local_pre_move_12_pct')
    l24 = f('local_pre_move_24_pct')
    premove = f('pre_entry_move_pct')
    mfe = abs(f('mfe_pct'))
    mae = abs(f('mae_pct'))
    first_push = abs(f('first_push_pct'))
    against_first = abs(f('against_move_first3_pct'))
    no_tp = ('tp1_threatened' in analysis and not b('tp1_threatened')) or b('weak_followthrough') or b('no_post_entry_expansion') or mfe < 0.35
    tight_space_pct = _loss_diag_env_float('LOSS_CARD_GRAPH_CONTEXT_TP_SPACE_PCT', 1.20)
    tight_space = bool(0 < cs <= max(tight_space_pct, _loss_card_location_tp_space_pct()))
    bullish_pre = bool(l6 >= 0.12 or l12 >= 0.22 or l24 >= 0.35 or premove >= 0.45 or b('bullish_context_before_entry') or str(analysis.get('structure_5m') or '').lower().startswith('bullish'))
    bearish_pre = bool(l6 <= -0.12 or l12 <= -0.22 or l24 <= -0.35 or premove <= -0.45 or b('bearish_context_before_entry') or str(analysis.get('structure_5m') or '').lower().startswith('bearish'))
    supply_seen = bool(analysis.get('supply_near_tfs') or b('overhead_bearish_fvg') or b('entry_into_supply') or b('entry_into_overhead_supply') or b('long_below_strong_high'))
    demand_seen = bool(analysis.get('demand_near_tfs') or b('underlying_bullish_fvg') or b('entry_into_demand') or b('short_above_weak_low'))

    # V5: separate a real saved FVG/demand/supply zone from synthetic
    # price-location flags. Flags like entry_into_demand can be created only
    # because the entry was low/tight; they must not make the card say
    # "green FVG" when the snapshot did not save an actual green FVG.
    try:
        _real_demand_ctx = _loss_card_real_zone_context(analysis, 'demand')
        _real_supply_ctx = _loss_card_real_zone_context(analysis, 'supply')
        real_demand_seen = bool(_real_demand_ctx.get('has_zone'))
        real_supply_seen = bool(_real_supply_ctx.get('has_zone'))
        real_demand_is_fvg = bool(_real_demand_ctx.get('is_fvg'))
        real_supply_is_fvg = bool(_real_supply_ctx.get('is_fvg'))
    except Exception:
        real_demand_seen = bool(analysis.get('demand_near_tfs'))
        real_supply_seen = bool(analysis.get('supply_near_tfs'))
        real_demand_is_fvg = False
        real_supply_is_fvg = False

    try:
        fast_pullback_min = int(float(os.getenv('LOSS_CARD_SHORT_AFTER_DUMP_PULLBACK_MIN', '12') or 12))
    except Exception:
        fast_pullback_min = 12
    fast_sl_pullback = bool(duration_min is not None and 0 < int(duration_min) <= max(3, min(30, fast_pullback_min)))

    common_analysis = []
    if duration_min is not None and duration_min > 0:
        common_analysis.append(f'вход → SL: {int(duration_min)} мин')
    for item in (
        pct_line('локальный ход перед входом 30м', l6),
        pct_line('локальный ход перед входом 60м', l12),
        (f'clean space до TP1: около {cs:.2f}%' if cs > 0 else ''),
        (f'макс. ход в сторону сделки: около {mfe:.2f}%' if mfe > 0 else ''),
        (f'макс. ход против сделки: около {mae:.2f}%' if mae > 0 else ''),
    ):
        if item:
            common_analysis.append(item)

    if side_u == 'LONG':
        if (bullish_pre or bearish_pre or supply_seen or pos >= 0.62 or tight_space) and no_tp:
            if bullish_pre and (supply_seen or pos >= 0.62 or tight_space):
                primary = 'Late LONG after exhausted pump / upside blocked'
                scenario = ('LONG был открыт после уже прошедшего buy-side импульса. Цена пришла в верхнюю часть локального range / под resistance-supply, '
                            'а до TP1 не было чистого пространства. После входа покупатель не дал новую волну, поэтому цена ушла в откат к SL.')
                visible = [
                    'перед входом уже был импульс вверх, fresh displacement после входа не появился',
                    'LONG открыт поздно, ближе к local high / resistance, чем к discount',
                    'TP1 стоял над зоной, куда цена уже пришла уставшей',
                    'после входа свечи не закрепились выше entry/retest уровня',
                ]
                secondary = ['Late entry', 'Buy-side exhaustion', 'Upside blocked by resistance/supply', 'No post-entry expansion']
                improve = ['не покупать после уже отработанного pump', 'ждать pullback ниже / discount entry', 'требовать новый bullish displacement после retest']
            elif bearish_pre:
                primary = 'LONG against bearish pressure / failed reclaim'
                scenario = ('LONG был открыт против свежего sell-side давления. Перед входом рынок не показал устойчивый bullish reclaim, '
                            'поэтому покупка была слабой и цена быстро вернулась к SL.')
                visible = [
                    'перед входом уже было bearish давление / возврат под уровень',
                    'bullish reclaim перед LONG не закрепился',
                    'покупатель не смог удержать entry/retest уровень',
                    'TP1 не был нормально поставлен под угрозу',
                ]
                secondary = ['Long against bearish pressure', 'Failed bullish reclaim', 'No bullish displacement', 'Weak follow-through']
                improve = ['не брать LONG без закрепления выше уровня', 'ждать bullish reclaim и follow-through', 'фильтровать LONG против bearish 5m/15m контекста']
            else:
                primary = 'TP1 blocked by local resistance / tight upside space'
                scenario = ('LONG был открыт с ограниченным пространством до TP1. Путь вверх упирался в local high / resistance, '
                            'поэтому даже формальный confirm не имел нормального места для continuation.')
                visible = [
                    'между входом и TP1 было мало чистого пространства',
                    'ближайший local high / resistance блокировал движение вверх',
                    'после входа не появился clean bullish displacement',
                    'SL стоял внутри обычного pullback/retest-движения',
                ]
                secondary = ['TP1 blocked by local resistance', 'No clean space to TP1', 'Weak upside follow-through', 'No retest acceptance']
                improve = ['не брать LONG, если TP1 слишком близко к local high/resistance', 'требовать clean space до TP1', 'ждать acceptance выше уровня']
            if real_supply_seen:
                if real_supply_is_fvg:
                    visible.insert(1, 'сверху была реальная seller zone / red FVG / resistance')
                else:
                    visible.insert(1, 'сверху была реальная supply/resistance zone')
            if pos > 0:
                visible.append(f'позиция входа в prior range: {pos:.2f}')
            return {
                'primary_text': primary,
                'scenario_text': scenario,
                'analysis_lines': list(dict.fromkeys(common_analysis or ['bullish continuation после входа отсутствовал'])),
                'happened_lines': list(dict.fromkeys([
                    'покупатель не смог дать continuation после входа',
                    'цена не закрепилась выше entry/retest зоны',
                    'TP1 не был нормально поставлен под угрозу',
                    'SL был достигнут после rejection/отката',
                ])),
                'visible_lines': list(dict.fromkeys(visible)),
                'secondary_labels': secondary,
                'improve_labels': improve,
            }

    else:
        if (bullish_pre or bearish_pre or demand_seen or pos <= 0.42 or tight_space) and no_tp:
            # V5: WLD/FLOW-type losses. The old branch called every tight
            # SHORT "opened into demand". On the chart these are often better
            # described as premature SHORT after an already completed dump:
            # the direction can be right later, but the first normal pullback
            # hits a tight SL before fresh bearish continuation appears.
            if bearish_pre and (fast_sl_pullback or tight_space or pos <= 0.55 or demand_seen):
                primary = 'SL too tight / stopped by normal pullback before continuation'
                scenario = ('SHORT был открыт после уже прошедшего sell-side импульса, но не из premium re-entry. '
                            'SL оказался внутри обычного pullback/retest-движения: цена сначала дала bounce/reclaim к SL, '
                            'а нового clean bearish displacement сразу после входа не было.')
                visible = [
                    'перед входом уже был sell-side impulse / движение вниз',
                    'SHORT открыт после отработанного dump, не после нового bearish displacement',
                    'SL стоял слишком близко и попал в обычный pullback/retest',
                    'TP1 был рядом с local low/support, поэтому первое движение вниз не имело большого clean space',
                    'сначала появился bounce/reclaim против SHORT',
                ]
                secondary = ['SL too tight', 'Stopped by normal pullback', 'No fresh bearish displacement', 'Tight TP1/SL space']
                improve = ['после сильного dump ждать pullback выше / premium re-entry', 'ставить SHORT только после нового bearish displacement', 'не ставить SL внутри обычного retest/bounce']
            elif bullish_pre and (demand_seen or pos <= 0.55 or tight_space):
                primary = 'SHORT against fresh bullish impulse / no bearish reclaim'
                scenario = ('SHORT был открыт во время активного восстановления вверх, до подтверждённой реакции продавца. '
                            'Цена уже сделала buy-side reclaim/impulse, bearish reclaim не закрепился, поэтому покупатель выбил SL.')
                visible = [
                    'перед входом уже был fresh bullish impulse / reclaim вверх',
                    'bearish reclaim перед SHORT не подтвердился',
                    'SHORT открыт против активного buy-side momentum',
                    'после входа не было clean bearish displacement',
                ]
                secondary = ['Short against bullish impulse', 'No bearish reclaim', 'Failed supply confirmation', 'Weak downside follow-through']
                improve = ['ждать свечной rejection от supply перед SHORT', 'требовать закрытие ниже локальной demand/support', 'не шортить fresh bullish impulse без подтверждения разворота']
            elif demand_seen or pos <= 0.42 or tight_space:
                primary = 'SHORT opened too low / downside space was blocked'
                scenario = ('SHORT был открыт слишком низко: путь к TP1 был рядом с local low / support / buyer reaction area. '
                            'Downside был ограничен, поэтому цена дала bounce и дошла до SL раньше, чем появилась нормальная sell-side continuation.')
                visible = [
                    'SHORT открыт рядом с local low / support',
                    'снизу уже была область реакции покупателей / local support',
                    'downside до TP1 был ограничен',
                    'после входа не появился clean bearish displacement',
                ]
                secondary = ['Short too low', 'No clean space to TP1', 'Buyer bounce against entry', 'Weak downside follow-through']
                improve = ['не шортить в discount / возле local low', 'ждать premium re-entry выше', 'требовать clean space до TP1']
            else:
                primary = 'Late SHORT after exhausted dump near low'
                scenario = ('SHORT был открыт после уже отработанного sell-side импульса. Цена пришла в нижнюю часть range, '
                            'продавец был уставший, поэтому продолжение до TP1 затухло и появился bounce к SL.')
                visible = [
                    'перед входом уже прошёл sell-side impulse',
                    'SHORT открыт поздно в нижней части локального range',
                    'после входа не было нового fresh bearish displacement',
                    'TP1 не был нормально поставлен под угрозу',
                ]
                secondary = ['Late entry', 'Sell-side exhaustion', 'Weak follow-through', 'No post-entry expansion']
                improve = ['не шортить после уже отработанного dump', 'ждать pullback выше', 'входить только после нового bearish displacement']
            if real_demand_seen:
                if real_demand_is_fvg:
                    visible.insert(1, 'снизу была реальная buyer zone / green FVG / demand')
                else:
                    visible.insert(1, 'снизу была реальная demand/support zone')
            if pos > 0:
                visible.append(f'позиция входа в prior range: {pos:.2f}')
            return {
                'primary_text': primary,
                'scenario_text': scenario,
                'analysis_lines': list(dict.fromkeys(common_analysis or ['bearish continuation после входа отсутствовал'])),
                'happened_lines': list(dict.fromkeys([
                    'продавец не смог дать continuation после входа',
                    'цена не закрепилась ниже entry/retest зоны',
                    'TP1 не был нормально поставлен под угрозу',
                    'SL был достигнут после bounce/reclaim вверх',
                ])),
                'visible_lines': list(dict.fromkeys(visible)),
                'secondary_labels': secondary,
                'improve_labels': improve,
            }
    return {}

def _loss_card_non_template_context_payload(src: dict, analysis: dict, *, side: str, code: str = '', duration_min: int | None = None) -> dict:
    """Replace generic/template LOSS reason with candle/snapshot based chart facts.

    This function is deliberately allowed to override many exact-route failure labels
    (for example OB+FVG fast invalidation) when the stored candles show a clearer
    location reason such as late SHORT after dump, TP1 blocked by demand, or LONG
    near range high. That keeps LOSS cards from showing one repeated template.
    """
    src = dict(src or {})
    analysis = dict(analysis or {})
    side_u = str(side or src.get('side') or analysis.get('side') or '').upper().strip() or 'LONG'
    code = str(code or '').strip()

    override_codes = {
        '', 'fake_breakout_no_continuation', 'breakout_trap', 'bos_retest_no_followthrough',
        'bos_retest_failed', 'bos_confirm_weak', 'bos_level_reclaimed_back',
        'no_continuation_after_retest', 'continuation_missing', 'no_followthrough_after_entry',
        'loss_before_tp1_sl', 'weak_reaction_then_fade', 'fake_confirm',
        'ob_fvg_zone_failed', 'ob_fvg_reaction_weak', 'ob_fvg_invalidation_fast',
        'ob_fvg_no_continuation', 'ob_fvg_no_followthrough',
        'htf_ob_ltf_retest_failed', 'htf_zone_not_held', 'ltf_confirm_weak',
        'htf_ob_ltf_no_followthrough',
        'displacement_failed', 'origin_not_held', 'impulse_died_fast',
        'dual_fvg_failed', 'stacked_fvg_not_held', 'reaction_from_dual_fvg_weak',
        'liquidity_reclaim_failed', 'reclaim_not_confirmed', 'sweep_without_continuation',
        'short_above_weak_low', 'entry_into_demand', 'long_below_strong_high',
        'entry_into_supply', 'entry_into_overhead_supply', 'entry_near_opposite_zone',
        'late_entry_after_exhausted_move', 'entry_in_range_premium',
    }
    raw = str(analysis.get('primary_reason_text') or analysis.get('reason_text') or '').lower()
    if code not in override_codes and 'breakout trap' not in raw and 'no continuation' not in raw and 'invalidation' not in raw:
        return {}

    def f(k, d=0.0):
        try:
            v = analysis.get(k)
            return d if v in (None, '') else float(str(v).replace(',', '.'))
        except Exception:
            return d

    def b(k):
        v = analysis.get(k)
        return v.strip().lower() in ('1', 'true', 'yes', 'on', 'да', 'есть') if isinstance(v, str) else bool(v)

    def tfs(name):
        return [str(x) for x in list(analysis.get(name) or []) if str(x).strip()]

    def out(primary, scenario, analysis_lines, happened, visible, secondary, improve):
        return {
            'primary_text': primary,
            'scenario_text': scenario,
            'analysis_lines': [x for x in analysis_lines if str(x).strip()],
            'happened_lines': [x for x in happened if str(x).strip()],
            'visible_lines': [x for x in visible if str(x).strip()],
            'secondary_labels': [x for x in secondary if str(x).strip()],
            'improve_labels': [x for x in improve if str(x).strip()],
        }

    cs = f('clean_space_to_tp1_pct')
    if cs <= 0:
        try:
            e = float(src.get('entry') or 0)
            t = float(src.get('tp1') or 0)
            if side_u == 'LONG' and e > 0 and t > e:
                cs = ((t - e) / e) * 100.0
            elif side_u == 'SHORT' and e > 0 and 0 < t < e:
                cs = ((e - t) / e) * 100.0
            if cs > 0:
                analysis['clean_space_to_tp1_pct'] = round(cs, 3)
        except Exception:
            pass

    pos = f('entry_position_in_prior_range', f('entry_position_in_range', 0.5))
    l6, l12, l24, premove = f('local_pre_move_6_pct'), f('local_pre_move_12_pct'), f('local_pre_move_24_pct'), f('pre_entry_move_pct')
    no_tp = not bool(analysis.get('tp1_threatened'))
    # V3: stop FET/COMP/SPELL-type losses from falling into the repeated
    # "Weak confirm / no retest acceptance" template. For crypto 1:1.7 setups,
    # clean-space to TP1 around 0.6-0.9% is already tight and should be treated
    # as a chart-location problem.
    graph_tp_space_limit = max(
        _loss_card_location_tp_space_pct(),
        _loss_diag_env_float('LOSS_CARD_GRAPH_CONTEXT_TP_SPACE_PCT', 1.20),
        1.20,
    )
    no_space = bool(0 < cs <= graph_tp_space_limit and no_tp)
    level_back = bool(b('level_reclaimed_back') or b('reclaim_lost_back'))
    weak = bool(b('weak_followthrough') or b('no_post_entry_expansion') or b('zone_reaction_too_weak') or no_tp or abs(f('mfe_pct')) < 0.35)
    tiny_space_only = _loss_card_tiny_space_only(analysis)
    try:
        fast_graph_loss = bool(duration_min is not None and 0 < int(duration_min) <= int(float(os.getenv('LOSS_CARD_FAST_SL_MIN', '15') or 15)))
    except Exception:
        fast_graph_loss = bool(duration_min is not None and 0 < int(duration_min) <= 15)

    if side_u == 'LONG':
        supply_ctx = _loss_card_real_zone_context(analysis, 'supply')
        demand_ctx = _loss_card_real_zone_context(analysis, 'demand')
        stf, dtf = tfs('supply_near_tfs'), tfs('demand_near_tfs')
        supply = bool(supply_ctx.get('has_zone') or b('supply_zone_blocks_tp1') or ((not tiny_space_only) and (b('overhead_bearish_fvg') or b('entry_into_supply') or b('entry_into_overhead_supply') or b('long_below_strong_high'))))
        demand = bool(demand_ctx.get('has_zone') or b('long_demand_near_entry'))
        bearish = bool(b('bearish_context_before_entry') or b('long_against_bearish_structure') or level_back or l6 <= -0.12 or l12 <= -0.22 or l24 <= -0.35 or premove <= -0.45 or str(analysis.get('structure_5m') or '').lower().startswith('bearish'))
        late_pump = bool(l6 >= 0.12 or l12 >= 0.22 or l24 >= 0.35 or premove >= 0.45 or b('late_entry_after_exhausted_move'))
        zd = str(supply_ctx.get('desc') or 'local high / resistance')
        zw = str(supply_ctx.get('fvg_word') or 'local high / resistance')

        if late_pump and supply and no_space:
            vis = ['перед входом уже прошёл buy-side impulse', f'LONG открыт поздно под {zd}', 'между входом и целью не было чистого upside-space', 'после входа не появился fresh bullish displacement']
            if stf:
                vis.append('overhead zone видна на TF: ' + '/'.join(stf))
            if cs > 0:
                vis.append(f'чистое пространство до TP1: около {cs:.2f}%')
            return out(
                'Late LONG after pump into supply/resistance',
                f'LONG был открыт после уже отработанного импульса вверх прямо под {zw}. Цена пришла в premium/верх range, а путь к TP1 упирался в supply/resistance, поэтому покупатель не получил fresh continuation и сделка ушла к SL.',
                ['вход после уже отработанного pump', 'TP1 был заблокирован overhead zone', 'fresh bullish displacement после входа отсутствовал'],
                ['покупатель не смог дать новую волну после входа', 'верхняя зона удержала движение', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после rejection/отката'],
                vis,
                ['Late long after pump', 'Long into supply/resistance', 'No clean space to TP1', 'Buyer exhaustion'],
                ['после сильного pump ждать discount re-entry ниже', 'не брать LONG прямо под supply/resistance', 'требовать acceptance выше resistance перед входом'],
            )

        if supply and no_space and bool(supply_ctx.get('has_zone')):
            vis = [f'TP1/путь к TP1 упирался в {zd}', 'между входом и целью не было чистого пространства', f'{zw} был на пути LONG', 'после входа не появился clean bullish displacement']
            if stf:
                vis.append('overhead zone подтверждена на TF: ' + '/'.join(stf))
            if cs > 0:
                vis.append(f'чистое пространство до TP1: около {cs:.2f}%')
            return out(
                'TP1 blocked by overhead supply/resistance',
                f'LONG был открыт под {zw}. TP1 находился прямо в зоне продавца/сопротивления или сразу за ней, поэтому формально RR был, но реального clean space вверх не было.',
                ['TP1 был заблокирован overhead zone', 'bullish expansion после входа отсутствовал'],
                ['цена не смогла пройти seller reaction zone', 'покупатель не получил continuation до TP1', 'верхняя зона удержала движение', 'SL был достигнут после rejection/затухания'],
                vis,
                ['TP1 blocked by supply/resistance', 'No clean space to TP1', 'Weak upside continuation'],
                ['не брать LONG, если TP1 внутри/сразу за resistance/supply', 'требовать clean space до TP1', 'ждать вход ниже от discount / demand'],
            )

        if supply and (bearish or level_back):
            vis = ['перед входом структура была bearish или reclaim не закрепился', f'LONG открыт под {zd}', 'после входа не было закрепления выше resistance/supply', 'это не clean breakout continuation, а покупка под sell pressure']
            if stf:
                vis.append('overhead zone видна на TF: ' + '/'.join(stf))
            return out(
                'Long under supply/resistance after failed reclaim',
                f'LONG был открыт под активной зоной продавца/сопротивления после слабого или неподтверждённого reclaim. Цена не смогла закрепиться выше {zw}, поэтому buy-side continuation не появился и движение развернулось к SL.',
                ['bullish reclaim выше resistance/supply отсутствовал', 'upside expansion отсутствовал'],
                ['цена не смогла закрепиться выше зоны входа', 'seller pressure удержал движение сверху', 'покупатель не получил displacement вверх', 'sell-side движение продолжилось к SL'],
                vis,
                ['Failed reclaim', 'Overhead supply/resistance', 'Weak upside continuation'],
                ['ждать закрытие выше resistance/supply перед LONG', 'не брать LONG под sell pressure при bearish 5m/15m', 'требовать bullish displacement после retest'],
            )

        if late_pump and (supply or pos >= 0.72):
            vis = ['перед входом уже прошёл buy-side impulse', 'LONG открыт поздно в верхней части локального range', 'после входа не было нового fresh bullish displacement', f'позиция входа в range: {pos:.2f}']
            if supply:
                vis.insert(2, f'сверху находился {zd}')
            return out(
                'Late LONG after exhausted pump near high',
                'LONG был открыт после уже отработанного импульса вверх. Цена пришла в premium/верх range, поэтому покупатель уже был уставший, а продолжение до TP1 быстро затухло.',
                ['вход после уже отработанного pump', 'fresh bullish displacement после входа отсутствовал'],
                ['первичный импульс вверх уже был израсходован до входа', 'после входа покупатель не смог дать новую волну', 'цена перешла в откат против позиции', 'SL был достигнут без нормального движения к TP1'],
                vis,
                ['Late entry', 'Buy-side exhaustion', 'Weak follow-through', 'No post-entry expansion'],
                ['не покупать после вертикального pump', 'ждать pullback ниже', 'входить только после нового bullish displacement'],
            )

        if no_space and not supply and not bearish and pos < 0.72 and (late_pump or fast_graph_loss):
            # POL/API3-type losses: even when the snapshot did not serialize a
            # strict red FVG, a quick LOSS after an upward push is not only
            # "TP1 blocked". The visible cause is failed acceptance under
            # the nearest local resistance, usually after an already completed
            # pump/breakout attempt.
            cs_line = f'чистое пространство до TP1 было около {cs:.2f}%' if cs > 0 else 'чистого пространства до TP1 было мало'
            primary = 'Late LONG after pump / no acceptance at resistance' if late_pump else 'LONG under local resistance / no acceptance after retest'
            scenario = (
                'LONG был открыт после движения вверх под ближайшим local high/resistance. '
                'Главная проблема не только в расстоянии до TP1: после входа не появилось acceptance выше entry/retest зоны, '
                'покупатель не дал fresh bullish displacement, и цена быстро ушла в rejection/pullback к SL.'
            )
            return out(
                primary,
                scenario,
                [cs_line, 'acceptance выше entry/retest зоны отсутствовал', 'fresh bullish displacement после входа отсутствовал'],
                ['покупатель не смог дать новую волну после входа', 'цена не закрепилась выше entry/retest зоны', 'local resistance удержал движение сверху', 'SL был достигнут после rejection/pullback'],
                [f'позиция входа в range: {pos:.2f}', 'LONG был не из discount, а под ближайшим local high/resistance', 'TP1 стоял рядом/за seller reaction area', 'после входа нет clean bullish displacement', 'первая реакция быстро пошла против LONG'],
                ['No retest acceptance', 'Late/weak LONG location', 'Local resistance rejection', 'No fresh bullish displacement'],
                ['после pump ждать pullback ниже/discount re-entry', 'не брать LONG без acceptance выше retest/resistance', 'требовать fresh bullish displacement после входа', 'если TP1 упирается в local resistance — брать цель раньше или пропускать'],
            )

        if no_space and not supply and not bearish and pos < 0.72:
            # Do not label this as a generic template/premature entry only.
            # On real charts (GALA/BTC-style losses) the important fact is
            # usually that TP1 was too close and the path to TP1 was blocked by
            # local resistance even when no strict pre-entry FVG object was saved.
            cs_line = f'чистое пространство до TP1 было около {cs:.2f}%' if cs > 0 else 'чистого пространства до TP1 было мало'
            return out(
                'TP1 blocked by local resistance / tight upside space',
                'LONG был открыт с маленьким расстоянием до TP1. Между входом и целью не было clean upside-space: ближайший local high / resistance мог остановить движение, а обычный pullback/retest выбил SL до нормального continuation.',
                [cs_line, 'bullish acceptance после входа не подтвердился'],
                ['первый откат после входа оказался сильнее допустимого SL', 'покупатель не дал сразу нового displacement вверх', 'TP1 не был нормально поставлен под угрозу', 'сделка закрылась по SL до нормального continuation'],
                [f'позиция входа в range: {pos:.2f}', 'путь к TP1 был слишком короткий и упирался в local high / resistance', 'SL находился внутри обычного pullback/retest-движения', 'после входа не было clean bullish displacement'],
                ['TP1 blocked by local resistance', 'No clean space to TP1', 'Tight TP1/SL space', 'No retest acceptance'],
                ['не брать LONG, если TP1 слишком близко к local high/resistance', 'требовать clean space до TP1', 'ждать закрытие/acceptance выше retest-зоны', 'не ставить SL внутри активного pullback'],
            )

        if bearish and demand and pos <= 0.50:
            dd = str(demand_ctx.get('desc') or 'demand/support')
            vis = ['LONG открыт возле demand/support, но структура уже была bearish', 'buyer zone не дала сильной реакции', 'bullish reclaim перед входом не подтвердился']
            if dtf:
                vis.append('demand/support рядом на TF: ' + '/'.join(dtf))
            if stf:
                vis.append('overhead resistance/supply также видна на TF: ' + '/'.join(stf))
            return out(
                'Long into bearish structure / failed demand',
                f'LONG был открыт от {dd}, но рынок уже показывал bearish structure. Demand не дал reclaim и сильной реакции, поэтому покупатель не удержал зону и цена ушла к SL.',
                ['demand reaction была слабой', 'bullish reclaim не появился'],
                ['buyer zone не удержала цену', 'после входа не было displacement вверх', 'продавец продолжил sell-side движение', 'TP1 не был поставлен под угрозу'],
                vis,
                ['Failed demand reaction', 'Long against bearish structure', 'No bullish displacement', 'Weak follow-through'],
                ['не брать LONG от demand без bullish reclaim', 'ждать закрытие выше локальной resistance/supply', 'фильтровать LONG против bearish 5m/15m структуры'],
            )

        if level_back:
            return out(
                'BOS/retest level was reclaimed back',
                'Пробой/ретест выглядел валидно, но после входа цена быстро вернулась обратно под уровень. Acceptance выше уровня не было, поэтому continuation не подтвердился.',
                ['BOS/retest level был потерян после входа', 'acceptance выше уровня не было'],
                ['цена вернулась обратно за уровень входа', 'покупатель не смог удержать retest', 'после входа не было clean bullish displacement', 'SL был достигнут до TP1'],
                ['retest не закрепился по правильную сторону уровня', 'после входа цена быстро вернулась в старый range', 'breakout не получил follow-through', 'TP1 не был нормально поставлен под угрозу'],
                ['Level reclaimed back', 'No retest acceptance', 'Weak confirmation', 'Continuation missing'],
                ['ждать 1–2 закрытия выше BOS-level', 'требовать retest acceptance', 'не входить на первом слабом confirm'],
            )

        if pos >= 0.72:
            return out(
                'Poor LONG location near range high',
                'LONG был открыт в верхней части локального range, где upside был ограничен. Вход находился слишком близко к local high / resistance, поэтому RR location был слабый.',
                ['вход в верхней части range', 'чистого пространства до TP1 было мало'],
                ['покупатель не смог расширить движение вверх', 'цена быстро потеряла momentum', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после отката'],
                [f'позиция входа в range: {pos:.2f}', 'вход был ближе к range high, чем к discount', 'upside до TP1 был ограничен', 'после входа не было clean bullish displacement'],
                ['Poor RR location', 'Weak upside continuation', 'No clean space to TP1', 'Buy-side exhaustion'],
                ['не брать LONG в premium без сильного reclaim', 'ждать discount re-entry', 'поднимать фильтр clean space до TP1'],
            )

    else:
        demand_ctx = _loss_card_real_zone_context(analysis, 'demand')
        supply_ctx = _loss_card_real_zone_context(analysis, 'supply')
        dtf, stf = tfs('demand_near_tfs'), tfs('supply_near_tfs')
        demand = bool(demand_ctx.get('has_zone') or b('demand_zone_blocks_tp1') or ((not tiny_space_only) and (b('underlying_bullish_fvg') or b('entry_into_demand') or b('short_above_weak_low'))))
        supply = bool(supply_ctx.get('has_zone') or b('short_supply_near_entry'))
        bullish = bool(b('bullish_context_before_entry') or b('short_against_bullish_structure') or level_back or l6 >= 0.12 or l12 >= 0.22 or l24 >= 0.35 or premove >= 0.45 or str(analysis.get('structure_5m') or '').lower().startswith('bullish'))
        late_dump = bool(l6 <= -0.12 or l12 <= -0.22 or l24 <= -0.35 or premove <= -0.45 or b('late_entry_after_exhausted_move'))
        zd = str(demand_ctx.get('desc') or 'local low / support')
        zw = str(demand_ctx.get('fvg_word') or 'local low / support')

        if late_dump and demand and no_space:
            vis = ['перед входом уже прошёл sell-side impulse', f'SHORT открыт поздно над {zd}', 'между входом и целью не было чистого downside-space', 'после входа не появился fresh bearish displacement']
            if dtf:
                vis.append('underlying zone видна на TF: ' + '/'.join(dtf))
            if cs > 0:
                vis.append(f'чистое пространство до TP1: около {cs:.2f}%')
            return out(
                'Late SHORT after dump into demand/support',
                f'SHORT был открыт после уже отработанного импульса вниз прямо над {zw}. Цена пришла в discount/низ range, а путь к TP1 упирался в demand/support, поэтому продавец не получил fresh continuation и сделка ушла к SL.',
                ['вход после уже отработанного dump', 'TP1 был заблокирован underlying zone', 'fresh bearish displacement после входа отсутствовал'],
                ['продавец не смог дать новую волну после входа', 'нижняя зона удержала движение', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после bounce/reclaim'],
                vis,
                ['Late short after dump', 'Short into demand/support', 'No clean space to TP1', 'Seller exhaustion'],
                ['после сильного dump ждать premium re-entry выше', 'не брать SHORT прямо над demand/support', 'требовать breakdown ниже support перед входом'],
            )

        if demand and no_space and bool(demand_ctx.get('has_zone')):
            vis = [f'TP1/путь к TP1 упирался в {zd}', 'между входом и целью не было чистого downside-space', f'{zw} был на пути SHORT', 'после входа не появился clean bearish displacement']
            if dtf:
                vis.append('underlying zone подтверждена на TF: ' + '/'.join(dtf))
            if cs > 0:
                vis.append(f'чистое пространство до TP1: около {cs:.2f}%')
            return out(
                'TP1 blocked by underlying demand/support',
                f'SHORT был открыт над {zw}. TP1 находился прямо в зоне покупателя/поддержки или сразу за ней, поэтому sell-side путь был заблокирован.',
                ['TP1 был заблокирован underlying zone', 'downside expansion отсутствовал'],
                ['цена не смогла продавить buyer reaction zone', 'продавец не получил continuation до TP1', 'нижняя зона удержала движение', 'SL был достигнут после bounce/затухания'],
                vis,
                ['TP1 blocked by demand/support', 'No clean space to TP1', 'Weak downside continuation'],
                ['не брать SHORT, если TP1 внутри/сразу за demand/support', 'требовать clean space до TP1', 'ждать вход выше от premium / supply'],
            )

        if demand and (bullish or level_back):
            vis = ['перед входом структура была bullish или bearish reclaim не закрепился', f'SHORT открыт над {zd}', 'после входа не было закрепления ниже demand/support', 'это не clean breakdown continuation, а SHORT против buyer pressure']
            if dtf:
                vis.append('underlying zone видна на TF: ' + '/'.join(dtf))
            return out(
                'Short over demand/support after failed bearish reclaim',
                f'SHORT был открыт над активной зоной покупателя/поддержки после слабого или неподтверждённого bearish reclaim. Цена не смогла закрепиться ниже {zw}, поэтому sell-side continuation не появился и движение развернулось к SL.',
                ['bearish reclaim ниже demand/support отсутствовал', 'downside expansion отсутствовал'],
                ['цена не смогла закрепиться ниже зоны входа', 'buyer reaction zone удержала движение снизу', 'продавец не получил displacement вниз', 'buy-side движение продолжилось к SL'],
                vis,
                ['Failed bearish reclaim', 'Underlying demand/support', 'Short over demand', 'Weak downside continuation'],
                ['ждать закрытие ниже demand/support перед SHORT', 'не брать SHORT над demand при bullish 5m/15m', 'требовать bearish displacement после retest'],
            )

        if late_dump and (demand or pos <= 0.28):
            vis = ['перед входом уже прошёл sell-side impulse', 'SHORT открыт поздно в нижней части локального range', 'после входа не было нового fresh bearish displacement', f'позиция входа в range: {pos:.2f}']
            if demand:
                vis.insert(2, f'снизу находился {zd}')
            return out(
                'Late SHORT after exhausted dump near low',
                'SHORT был открыт после уже отработанного импульса вниз. Цена пришла в discount/низ range, поэтому продавец уже был уставший, а продолжение до TP1 быстро затухло.',
                ['вход после уже отработанного dump', 'fresh bearish displacement после входа отсутствовал'],
                ['первичный импульс вниз уже был израсходован до входа', 'после входа продавец не смог дать новую волну', 'цена перешла в bounce против позиции', 'SL был достигнут без нормального движения к TP1'],
                vis,
                ['Late entry', 'Sell-side exhaustion', 'Weak follow-through', 'No post-entry expansion'],
                ['не шортить после вертикального dump', 'ждать pullback выше', 'входить только после нового bearish displacement'],
            )

        if bullish and not demand:
            sd = str(supply_ctx.get('desc') or 'supply/resistance')
            vis = ['перед входом уже был fresh bullish impulse / reclaim', 'bearish reclaim перед SHORT не подтвердился', 'SHORT был открыт против активного buy-side momentum']
            if supply:
                vis.insert(1, f'цена шла к {sd}, но rejection от supply ещё не подтвердился')
            if stf:
                vis.append('supply/resistance рядом на TF: ' + '/'.join(stf))
            return out(
                'Short against fresh bullish impulse / no bearish reclaim',
                'SHORT был открыт во время активного восстановления вверх, до подтверждённой реакции продавца. Цена ещё не дала bearish reclaim/displacement, поэтому сначала продолжила buy-side движение и выбила SL.',
                ['bullish impulse перед входом', 'bearish reclaim отсутствовал', 'downside expansion после входа не появился'],
                ['покупатель продолжил движение вверх после входа', 'продавец не смог сразу развернуть цену вниз', 'SL был выбит до нормального bearish continuation'],
                vis,
                ['Short against bullish impulse', 'No bearish reclaim', 'Failed supply confirmation', 'Weak downside follow-through'],
                ['ждать свечной rejection от supply перед SHORT', 'требовать закрытие ниже локальной demand/support', 'не шортить fresh bullish impulse без подтверждения разворота'],
            )

        if bullish and supply:
            sd = str(supply_ctx.get('desc') or 'supply/resistance')
            vis = ['SHORT открыт возле supply/resistance, но структура уже была bullish', 'seller zone не дала сильной реакции', 'bearish reclaim перед входом не подтвердился']
            if stf:
                vis.append('supply/resistance рядом на TF: ' + '/'.join(stf))
            if dtf:
                vis.append('underlying demand/support также видна на TF: ' + '/'.join(dtf))
            return out(
                'Short into bullish structure / failed supply',
                f'SHORT был открыт от {sd}, но рынок уже показывал bullish structure. Supply не дала bearish reclaim и сильной реакции, поэтому продавец не удержал зону и цена ушла к SL.',
                ['supply reaction была слабой', 'bearish reclaim не появился'],
                ['seller zone не удержала цену', 'после входа не было displacement вниз', 'покупатель продолжил buy-side движение', 'TP1 не был поставлен под угрозу'],
                vis,
                ['Failed supply reaction', 'Short against bullish structure', 'No bearish displacement', 'Weak follow-through'],
                ['не брать SHORT от supply без bearish reclaim', 'ждать закрытие ниже локальной demand/support', 'фильтровать SHORT против bullish 5m/15m структуры'],
            )

        if level_back:
            return out(
                'BOS/retest level was reclaimed back',
                'Пробой/ретест выглядел валидно, но после входа цена быстро вернулась обратно выше уровня. Acceptance ниже уровня не было, поэтому bearish continuation не подтвердился.',
                ['BOS/retest level был потерян после входа', 'acceptance ниже уровня не было'],
                ['цена вернулась обратно за уровень входа', 'продавец не смог удержать retest', 'после входа не было clean bearish displacement', 'SL был достигнут до TP1'],
                ['retest не закрепился по правильную сторону уровня', 'после входа цена быстро вернулась в старый range', 'breakdown не получил follow-through', 'TP1 не был нормально поставлен под угрозу'],
                ['Level reclaimed back', 'No retest acceptance', 'Weak confirmation', 'Continuation missing'],
                ['ждать 1–2 закрытия ниже BOS-level', 'требовать retest acceptance', 'не входить на первом слабом confirm'],
            )

        if no_space and not demand and not bullish and pos > 0.28:
            # Mirror rule for SHORT: when TP1 is very close and SL is hit after
            # a bounce, the real chart reason is tight downside space / local
            # support blocking TP1. Do not print the misleading line
            # "нет признака возле range low" because the support/demand reaction
            # is often visible even if no strict FVG zone was serialized.
            cs_line = f'чистое пространство до TP1 было около {cs:.2f}%' if cs > 0 else 'чистого пространства до TP1 было мало'
            return out(
                'TP1 blocked by local support / tight downside space',
                'SHORT был открыт с маленьким расстоянием до TP1. Путь к TP1 упирался в local low / support / demand, поэтому продавец не получил clean downside-space: цена дала bounce и выбила SL до нормального continuation.',
                [cs_line, 'bearish acceptance после входа не подтвердился'],
                ['первый bounce после входа оказался сильнее допустимого SL', 'продавец не дал сразу нового displacement вниз', 'TP1 не был нормально поставлен под угрозу', 'сделка закрылась по SL до нормального continuation'],
                [f'позиция входа в range: {pos:.2f}', 'путь к TP1 был слишком короткий и упирался в local low / support', 'SL находился внутри обычного bounce/retest-движения', 'после входа не было clean bearish displacement'],
                ['TP1 blocked by local support', 'No clean space to TP1', 'Tight TP1/SL space', 'No retest acceptance'],
                ['не брать SHORT, если TP1 слишком близко к local low/support', 'требовать clean space до TP1', 'ждать закрытие/acceptance ниже retest-зоны', 'не ставить SL внутри активного bounce'],
            )

        if pos <= 0.28:
            return out(
                'Poor SHORT location near range low',
                'SHORT был открыт в нижней части локального range, где downside был ограничен. Вход находился слишком близко к local low / support, поэтому RR location был слабый.',
                ['вход в нижней части range', 'чистого пространства до TP1 было мало'],
                ['продавец не смог расширить движение вниз', 'цена быстро потеряла sell-side momentum', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после bounce'],
                [f'позиция входа в range: {pos:.2f}', 'вход был ближе к range low, чем к premium', 'downside до TP1 был ограничен', 'после входа не было clean bearish displacement'],
                ['Poor RR location', 'Weak downside continuation', 'No clean space to TP1', 'Sell-side exhaustion'],
                ['не брать SHORT в discount без сильного reclaim', 'ждать premium re-entry', 'поднимать фильтр clean space до TP1'],
            )

    graph_override = _loss_card_graph_visible_override_payload(src, analysis, side=side_u, duration_min=duration_min)
    if graph_override:
        return graph_override

    if weak:
        # V3: last-resort fallback must still describe chart/location. The old
        # `Weak bullish/bearish confirm / no retest acceptance` was true but too
        # generic and made different losses look identical.
        # For distant TP1, `cs` is geometric distance, not proof that the
        # path was clean. Tight targets keep the old clean-space wording.
        wide_target_hint = bool(cs >= 1.50)
        cs_line = (f'расстояние entry → TP1: около {cs:.2f}%' if wide_target_hint else f'clean space до TP1: около {cs:.2f}%') if cs > 0 else 'clean space до TP1 не подтверждён свечами'
        pos_line = f'позиция входа в range: {pos:.2f}' if pos > 0 else ''
        if side_u == 'LONG':
            if wide_target_hint:
                return out(
                    'Late LONG after pump / TP1 too far behind resistance',
                    'LONG был открыт после уже выполненного buy-side движения. TP1 был далеко по проценту/RR, но путь к нему проходил через overhead resistance/supply, поэтому большое расстояние до TP1 не означало clean path. После входа не было acceptance и fresh bullish displacement, цена ушла в pullback/rejection к SL.',
                    [cs_line, pos_line, 'fresh bullish displacement после входа отсутствовал'],
                    ['покупатель не смог закрепиться выше entry/retest зоны', 'после входа не появился новый bullish expansion', 'TP1 был слишком далеко за reaction area', 'SL был достигнут после pullback/rejection'],
                    ['перед входом уже был bounce/pump вверх', 'LONG открыт после роста, а не из сильного discount', 'на пути к TP1 были local high / resistance зоны', 'большой RR не равен чистому пути до TP1'],
                    ['Late long after pump', 'TP1 too far behind resistance', 'No retest acceptance', 'No fresh bullish displacement'],
                    ['после pump ждать discount re-entry ниже', 'для LONG требовать acceptance выше ближайшего resistance', 'не ставить TP1 далеко за несколькими overhead зонами'],
                )
            return out(
                'LONG under local resistance / no acceptance after retest',
                'LONG был открыт без нормального acceptance выше entry/retest зоны. Путь вверх упирался в local high / resistance или вход был после уже отработанного импульса, поэтому покупатель не смог дать fresh continuation и цена ушла к SL.',
                [cs_line, pos_line, 'fresh bullish displacement после входа отсутствовал'],
                ['покупатель не смог расширить движение вверх', 'цена не закрепилась выше entry/retest зоны', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после отката/rejection'],
                ['вход был не из сильного discount, а ближе к resistance или после уже отработанного движения', 'после входа не появился clean bullish displacement', 'движение быстро перешло против LONG'],
                ['No retest acceptance', 'Late/weak LONG location', 'No post-entry expansion', 'TP1 was never threatened'],
                ['не брать LONG без acceptance после retest', 'ждать pullback ниже / discount entry', 'требовать fresh bullish displacement после retest'],
            )
        return out(
            'SHORT had no clean downside space / bearish reclaim missing',
            'SHORT был открыт без нормального clean downside-space или без подтверждённого bearish reclaim. Даже если confirm формально был, снизу находился local low / demand либо рынок шёл fresh bullish impulse. Поэтому продавец не смог дать continuation и цена ушла к SL.',
            [cs_line, pos_line, 'fresh bearish displacement после входа отсутствовал'],
            ['продавец не смог расширить движение вниз', 'цена не закрепилась ниже entry/retest зоны', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после bounce/reclaim вверх'],
            ['вход был не из сильного premium или был против buy-side momentum', 'снизу было мало свободного пространства до TP1 / buyer zone могла держать цену', 'после входа не появился clean bearish displacement', 'движение быстро перешло против SHORT'],
            ['TP1 near demand/support reaction area', 'Bearish reclaim missing', 'No post-entry expansion', 'TP1 was never threatened'],
            ['не брать SHORT без clean space до TP1', 'ждать premium re-entry выше', 'требовать fresh bearish displacement/reclaim после retest'],
        )
    return {}



def _loss_card_after_tp1_payload(src: dict, analysis: dict, *, side: str, duration_min: int | None = None) -> dict:
    """Special LOSS card for TP1-hit then BE/SL on remainder.

    After TP1 the card must not say that TP1 was never threatened. The mistake is
    usually no continuation to TP2 / reversal after the first target.
    """
    side_u = str(side or src.get('side') or analysis.get('side') or '').upper().strip() or 'LONG'
    dline = f"вход → закрытие остатка: {duration_min} мин" if duration_min is not None and duration_min > 0 else ''
    if side_u == 'SHORT':
        direction = 'sell-side'
        reversal = 'bounce вверх'
        move = 'после TP1 продавец не смог продолжить движение к TP2'
        visible = [
            'TP1 был достигнут, значит это не обычный LOSS до первой цели',
            'после TP1 не появилось нового bearish displacement',
            'цена дала bounce против SHORT и вернулась к BE/SL по остатку',
            'продолжение к TP2 не подтвердилось',
        ]
        improve = [
            'после TP1 быстрее защищать остаток в BE',
            'часть позиции закрывать на TP1, если нет нового bearish continuation',
            'не держать остаток, если после TP1 появляется быстрый bounce/reclaim',
        ]
    else:
        direction = 'buy-side'
        reversal = 'rejection вниз'
        move = 'после TP1 покупатель не смог продолжить движение к TP2'
        visible = [
            'TP1 был достигнут, значит это не обычный LOSS до первой цели',
            'после TP1 не появилось нового bullish displacement',
            'цена дала rejection против LONG и вернулась к BE/SL по остатку',
            'продолжение к TP2 не подтвердилось',
        ]
        improve = [
            'после TP1 быстрее защищать остаток в BE',
            'часть позиции закрывать на TP1, если нет нового bullish continuation',
            'не держать остаток, если после TP1 появляется быстрый rejection/reclaim',
        ]
    analysis_lines = [x for x in [dline, 'TP1 был достигнут', f'{direction} continuation к TP2 не подтвердился', f'после TP1 появился {reversal}'] if x]
    return {
        'primary_text': 'TP1 hit, then no TP2 continuation / BE-SL remainder',
        'scenario_text': f'Сделка сначала дошла до TP1 — это не обычный LOSS до первой цели. Но после TP1 продолжения к TP2 не было: {move}, цена развернулась против позиции и закрыла остаток по BE/SL.',
        'analysis_lines': analysis_lines,
        'what_happened_lines': [
            'первая цель была взята',
            'после TP1 импульс в сторону сделки затух',
            'остаток позиции не получил continuation к TP2',
            'закрытие произошло после возврата цены против позиции',
        ],
        'chart_visible_lines': visible,
        'secondary_reason_labels': ['After TP1 reversal', 'No TP2 continuation', 'Momentum faded after TP1', 'BE/SL on remainder'],
        'improve_labels': improve,
    }



def _loss_card_ranked_reason_payload(src: dict, analysis: dict, *, side: str, duration_min: int | None = None) -> dict:
    """Rank many candle/snapshot based LOSS reasons and print only proven facts.

    v8 reason engine: the card can see many possible failure modes at once
    (location, SL, TP path, confirmation, structure, momentum, FVG/OB, liquidity,
    timing, and HTF context).  Each detector adds a scored candidate only when the
    required data/flag is present.  The highest score becomes the main reason and
    the next strongest facts become additional reasons.
    """
    if str(os.getenv('LOSS_CARD_RANKED_REASON_ENABLED', '1')).strip().lower() in ('0', 'false', 'no', 'off'):
        return {}
    src = dict(src or {})
    analysis = dict(analysis or {})
    side_u = str(side or src.get('side') or analysis.get('side') or '').upper().strip()
    if side_u not in ('LONG', 'SHORT'):
        return {}

    def f(k: str, default: float = 0.0) -> float:
        try:
            v = analysis.get(k)
            return default if v in (None, '') else float(str(v).replace(',', '.'))
        except Exception:
            return default

    def fs(obj: dict, k: str, default: float = 0.0) -> float:
        try:
            v = obj.get(k) if isinstance(obj, dict) else None
            return default if v in (None, '') else float(str(v).replace(',', '.'))
        except Exception:
            return default

    def b(k: str) -> bool:
        v = analysis.get(k)
        if isinstance(v, str):
            return v.strip().lower() in ('1', 'true', 'yes', 'on', 'да', 'есть')
        return bool(v)

    def sget(*keys: str) -> str:
        for k in keys:
            v = analysis.get(k)
            if v not in (None, '', [], {}):
                return str(v).strip()
            v = src.get(k)
            if v not in (None, '', [], {}):
                return str(v).strip()
        return ''

    def contains_any(text: str, words: tuple[str, ...]) -> bool:
        t = str(text or '').lower()
        return any(w in t for w in words)

    entry = fs(src, 'entry') or f('entry')
    sl = fs(src, 'sl') or f('sl')
    tp1 = fs(src, 'tp1') or f('tp1')
    if entry <= 0 or sl <= 0:
        return {}

    cs = f('clean_space_to_tp1_pct')
    if cs <= 0 and tp1 > 0:
        if side_u == 'LONG' and tp1 > entry:
            cs = ((tp1 - entry) / entry) * 100.0
        elif side_u == 'SHORT' and tp1 < entry:
            cs = ((entry - tp1) / entry) * 100.0
    risk_pct = f('risk_pct_to_sl')
    if risk_pct <= 0:
        risk_pct = abs((sl - entry) / entry) * 100.0
    reward_pct = 0.0
    rr_to_tp1 = 0.0
    if tp1 > 0 and entry > 0:
        reward_pct = abs((tp1 - entry) / entry) * 100.0
        try:
            rr_to_tp1 = abs(tp1 - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0.0
        except Exception:
            rr_to_tp1 = 0.0
    try:
        wide_tp1_pct = float(str(os.getenv('LOSS_CARD_WIDE_TP1_SPACE_PCT', '1.50') or '1.50').replace(',', '.'))
    except Exception:
        wide_tp1_pct = 1.50
    try:
        wide_tp1_rr = float(str(os.getenv('LOSS_CARD_WIDE_TP1_RR', '2.50') or '2.50').replace(',', '.'))
    except Exception:
        wide_tp1_rr = 2.50
    wide_tp_target = bool((cs > 0 and cs >= wide_tp1_pct) or (rr_to_tp1 > 0 and rr_to_tp1 >= wide_tp1_rr))
    try:
        tight_sl_pct = float(str(os.getenv('LOSS_CARD_TIGHT_SL_PCT', '0.38') or '0.38').replace(',', '.'))
    except Exception:
        tight_sl_pct = 0.38
    try:
        tight_space_pct = float(str(os.getenv('LOSS_CARD_TIGHT_TP1_SPACE_PCT', '0.75') or '0.75').replace(',', '.'))
    except Exception:
        tight_space_pct = 0.75
    try:
        tight_tp1_blocked_pct = float(str(os.getenv('LOSS_CARD_TIGHT_TP1_BLOCKED_PCT', '0.60') or '0.60').replace(',', '.'))
    except Exception:
        tight_tp1_blocked_pct = 0.60
    try:
        exhausted_move_pct = float(str(os.getenv('LOSS_CARD_EXHAUSTION_MOVE_PCT', '0.45') or '0.45').replace(',', '.'))
    except Exception:
        exhausted_move_pct = 0.45
    try:
        fast_sl_min = int(float(os.getenv('LOSS_CARD_FAST_SL_MIN', '15') or '15'))
    except Exception:
        fast_sl_min = 15

    pos = f('entry_position_in_prior_range', f('entry_position_in_range', 0.5))
    pos_known = _loss_card_has_real_entry_position(analysis)
    l6, l12, l24, premove = f('local_pre_move_6_pct'), f('local_pre_move_12_pct'), f('local_pre_move_24_pct'), f('pre_entry_move_pct')
    first_push_known = 'first_push_pct' in analysis
    against_known = 'against_move_first3_pct' in analysis
    mfe_known = 'mfe_pct' in analysis
    mae_known = 'mae_pct' in analysis
    first_push = abs(f('first_push_pct'))
    against = abs(f('against_move_first3_pct'))
    mfe = abs(f('mfe_pct'))
    mae = abs(f('mae_pct'))
    mfe_r = f('mfe_r')
    mae_r = f('mae_r')
    bars = int(f('bars_to_failure') or f('bars_to_sl') or 0)

    tp1_known = 'tp1_threatened' in analysis
    tight_space = bool(0 < cs <= tight_space_pct)
    fast = bool(b('fast_invalidation') or b('immediate_invalidation') or (duration_min is not None and duration_min <= fast_sl_min) or (bars > 0 and bars <= 3))
    # When old rows do not store tp1_threatened, a LOSS with tight TP1 path
    # and quick invalidation should still be treated as "TP1 was not properly
    # threatened". Otherwise the ranked engine returns {} and the card falls
    # back to the generic "TP1 blocked by local resistance" template.
    infer_tp1_not_threatened = str(os.getenv('LOSS_CARD_INFER_TP1_NOT_THREATENED', '1')).strip().lower() not in ('0', 'false', 'no', 'off')
    no_tp = bool(
        (tp1_known and not b('tp1_threatened'))
        or (infer_tp1_not_threatened and (not tp1_known) and (duration_min is not None and duration_min > 0))
        or ((not tp1_known) and (tight_space or fast or b('weak_followthrough') or b('no_post_entry_expansion')))
        or b('tp1_too_close_no_clean_space')
        or b('weak_followthrough')
        or b('no_post_entry_expansion')
        or (mfe > 0 and mfe < 0.35)
    )
    # V19: separate "TP1 not hit" from "TP1 was never threatened".
    # Some trades move 55-80% of the way to TP1 and then reject back to SL.
    # Those are near-miss / exhaustion failures, not "no meaningful excursion".
    try:
        tp1_near_miss_min = float(str(os.getenv('LOSS_CARD_TP1_NEAR_MISS_PROGRESS', '0.55') or '0.55').replace(',', '.'))
    except Exception:
        tp1_near_miss_min = 0.55
    try:
        tp1_meaningful_mfe_r_min = float(str(os.getenv('LOSS_CARD_TP1_MEANINGFUL_MFE_R', '0.75') or '0.75').replace(',', '.'))
    except Exception:
        tp1_meaningful_mfe_r_min = 0.75
    tp1_progress_frac = 0.0
    if cs > 0 and mfe > 0:
        tp1_progress_frac = max(0.0, min(2.0, mfe / max(cs, 1e-9)))
    elif cs > 0 and first_push > 0:
        tp1_progress_frac = max(0.0, min(2.0, first_push / max(cs, 1e-9)))
    tp1_near_miss = bool(
        no_tp
        and (
            tp1_progress_frac >= tp1_near_miss_min
            or (mfe_r > 0 and mfe_r >= tp1_meaningful_mfe_r_min)
            or b('tp1_near_miss')
            or b('tp1_almost_hit')
        )
    )

    sl_tight = bool(risk_pct > 0 and (risk_pct <= tight_sl_pct or (cs > 0 and risk_pct <= cs * 0.88)))
    normal_pullback = bool(sl_tight and no_tp and (against >= max(first_push * 1.05, risk_pct * 0.55, 0.08) or fast or b('retest_too_deep')))
    meaningful_excursion_missing = bool((b('sl_hit_without_meaningful_excursion') or (mfe > 0 and mfe < 0.12) or (mfe_r > 0 and mfe_r < 0.45)) and not tp1_near_miss)
    immediate_counter = bool(b('immediate_invalidation') or against >= max(first_push * 1.15, risk_pct * 0.75, 0.12))
    weak_confirm = bool(b('confirm_too_weak') or b('zone_reaction_too_weak') or (first_push_known and first_push < 0.15))
    weak_follow = bool(b('weak_followthrough') or b('no_post_entry_expansion') or (no_tp and not tp1_near_miss) or (mfe_known and mfe < 0.35))
    range_trap = bool(b('range_trap_behavior') or b('multiple_failed_pushes'))
    volume_faded = bool(b('post_entry_volume_faded'))

    demand_ctx = _loss_card_real_zone_context(analysis, 'demand')
    supply_ctx = _loss_card_real_zone_context(analysis, 'supply')
    # v9.1 hotfix: these FVG booleans must live inside this ranked-reason
    # function. In v9 they were only defined in the older forensic helper, so
    # closed-card generation crashed with NameError on real_supply_is_fvg.
    real_demand_is_fvg = bool(demand_ctx.get('is_fvg'))
    real_supply_is_fvg = bool(supply_ctx.get('is_fvg'))
    demand_seen = bool(demand_ctx.get('has_zone') or b('entry_into_demand') or b('short_above_weak_low') or analysis.get('demand_near_tfs') or b('underlying_bullish_fvg'))
    supply_seen = bool(supply_ctx.get('has_zone') or b('entry_into_supply') or b('entry_into_overhead_supply') or b('long_below_strong_high') or analysis.get('supply_near_tfs') or b('overhead_bearish_fvg'))
    demand_blocks = bool(demand_ctx.get('blocks_tp1') or b('demand_zone_blocks_tp1') or b('range_low_blocks_tp1'))
    supply_blocks = bool(supply_ctx.get('blocks_tp1') or b('supply_zone_blocks_tp1') or b('range_high_blocks_tp1'))

    structure_text = ' '.join([sget('structure_5m'), sget('structure_15m'), sget('structure_30m'), sget('htf_structure'), sget('market_structure')]).lower()
    bearish_ctx = bool(b('bearish_context_before_entry') or b('long_against_bearish_structure') or b('reclaim_missing') or contains_any(structure_text, ('bearish', 'lower high', 'lower low', 'sell')) or l6 <= -0.12 or l12 <= -0.22 or premove <= -exhausted_move_pct)
    bullish_ctx = bool(b('bullish_context_before_entry') or b('short_against_bullish_structure') or b('bearish_reclaim_missing') or contains_any(structure_text, ('bullish', 'higher high', 'higher low', 'buy')) or l6 >= 0.12 or l12 >= 0.22 or premove >= exhausted_move_pct)
    late_long = bool(b('late_entry_after_exhausted_move') or b('late_entry_inside_expansion') or l6 >= 0.12 or l12 >= 0.22 or l24 >= 0.35 or premove >= exhausted_move_pct)
    late_short = bool(b('late_entry_after_exhausted_move') or b('late_entry_inside_expansion') or l6 <= -0.12 or l12 <= -0.22 or l24 <= -0.35 or premove <= -exhausted_move_pct)
    try:
        vertical_pump_6_pct = float(str(os.getenv('LOSS_CARD_VERTICAL_PUMP_6_PCT', '0.55') or '0.55').replace(',', '.'))
    except Exception:
        vertical_pump_6_pct = 0.55
    try:
        vertical_pump_12_pct = float(str(os.getenv('LOSS_CARD_VERTICAL_PUMP_12_PCT', '0.85') or '0.85').replace(',', '.'))
    except Exception:
        vertical_pump_12_pct = 0.85
    try:
        vertical_pump_24_pct = float(str(os.getenv('LOSS_CARD_VERTICAL_PUMP_24_PCT', '1.20') or '1.20').replace(',', '.'))
    except Exception:
        vertical_pump_24_pct = 1.20
    try:
        vertical_pump_min_cs_pct = float(str(os.getenv('LOSS_CARD_VERTICAL_PUMP_MIN_TP_SPACE_PCT', '0.55') or '0.55').replace(',', '.'))
    except Exception:
        vertical_pump_min_cs_pct = 0.55
    try:
        vertical_pump_min_risk_pct = float(str(os.getenv('LOSS_CARD_VERTICAL_PUMP_MIN_RISK_PCT', '0.40') or '0.40').replace(',', '.'))
    except Exception:
        vertical_pump_min_risk_pct = 0.40
    vertical_long_pump = bool(
        side_u == 'LONG'
        and (
            b('vertical_pump_before_entry')
            or b('entry_after_vertical_pump')
            or b('strong_buy_side_impulse_before_entry')
            or l6 >= vertical_pump_6_pct
            or l12 >= vertical_pump_12_pct
            or l24 >= vertical_pump_24_pct
            or premove >= max(exhausted_move_pct, vertical_pump_12_pct)
        )
    )
    highish = bool(pos_known and pos >= 0.68)
    lowish = bool(pos_known and pos <= 0.32)
    midrange = bool(pos_known and 0.42 <= pos <= 0.58)

    real_demand_seen = bool(demand_ctx.get('has_zone'))
    real_supply_seen = bool(supply_ctx.get('has_zone'))
    # These booleans mean the opposite zone is really visible or path-blocking.
    # They deliberately exclude synthetic entry_into_* flags created only from
    # TP1 distance or default 0.50 range position.
    short_demand_block_evidence = bool(real_demand_seen or demand_blocks or (lowish and (tight_space or no_tp)))
    long_supply_block_evidence = bool(real_supply_seen or supply_blocks or (highish and (tight_space or no_tp)))

    # A LOSS can be location-based even when the saved snapshot did not mark
    # a textbook FVG/OB. If TP1 is not threatened and price is already near the
    # wrong edge of the range, the card must print the chart-visible cause
    # (late/low/high entry + blocked path), not a repeated generic template.
    short_low_path_block = bool(side_u == 'SHORT' and no_tp and (short_demand_block_evidence or (tight_space and pos_known and pos <= 0.45)))
    long_high_path_block = bool(side_u == 'LONG' and no_tp and (long_supply_block_evidence or (tight_space and pos_known and pos >= 0.55)))
    late_or_low_short = bool(late_short or (short_low_path_block and (weak_follow or normal_pullback or sl_tight or fast)))
    late_or_high_long = bool(late_long or (long_high_path_block and (weak_follow or normal_pullback or sl_tight or fast)))

    # Define both path-description lines before the side-specific reason blocks.
    # Some candidates are intentionally added as symmetric LONG/SHORT detectors
    # in the same branch; Python evaluates the candidate arguments before the
    # evidence flag, so branch-local definitions can still raise UnboundLocalError.
    demand_desc = str(demand_ctx.get('desc') or 'local low / support')
    supply_desc = str(supply_ctx.get('desc') or 'local high / resistance')
    tp1_demand_path_line = f'TP1 находился рядом/за demand/support reaction area ({demand_desc})'
    tp1_supply_path_line = f'TP1 находился рядом/за supply/resistance reaction area ({supply_desc})'

    route_key = sget('route_key', 'smart_setup_code', 'setup_code', 'setup_key')
    setup_label_raw = sget('setup_label', 'setup', 'smart_setup')
    route_text = (route_key + ' ' + setup_label_raw).lower()
    ob_fvg_route = contains_any(route_text, ('ob', 'fvg', 'htf_ob', 'zone retest'))
    origin_route = contains_any(route_text, ('origin', 'displacement origin'))
    dual_fvg_route = contains_any(route_text, ('dual', 'stacked'))
    reclaim_route = contains_any(route_text, ('reclaim', 'liquidity'))
    breakout_route = contains_any(route_text, ('bos', 'breakout'))
    structure_pending_route = contains_any(route_text, ('structure pending', 'pending trigger', 'pending'))

    cs_line = (f'расстояние entry → TP1: около {cs:.2f}%' if wide_tp_target else f'clean space до TP1: около {cs:.2f}%') if cs > 0 else 'clean space до TP1 не подтверждён свечами'
    risk_line = f'расстояние entry → SL: около {risk_pct:.2f}%' if risk_pct > 0 else ''
    mfe_line = f'макс. ход в сторону сделки: около {mfe:.2f}%' if mfe > 0 else ''
    against_line = f'первое движение против входа: около {against:.2f}%' if against > 0 else ''
    dline = f'вход → SL: {duration_min} мин' if duration_min is not None and duration_min > 0 else ''
    pos_line = _loss_card_entry_position_line(analysis)
    base_analysis = [x for x in [dline, cs_line, risk_line, mfe_line, against_line] if x]

    candidates: list[tuple[float, str, dict]] = []

    def any_flag(*keys: str) -> bool:
        return any(b(k) for k in keys)

    def any_text(*words: str) -> bool:
        joined = ' '.join([
            sget('loss_reason'), sget('close_reason'), sget('reason'), sget('debug_reason'),
            sget('ta_summary'), sget('snapshot_reason'), structure_text, route_text,
        ]).lower()
        return any(str(w).lower() in joined for w in words if str(w).strip())

    def add(score: float, key: str, *, primary: str, scenario: str, analysis_add: list[str] | None = None,
            happened: list[str] | None = None, visible: list[str] | None = None,
            secondary: list[str] | None = None, improve: list[str] | None = None,
            evidence: bool = True):
        if not evidence or score <= 0:
            return
        payload = {
            'primary_text': primary,
            'scenario_text': scenario,
            'analysis_lines': [x for x in (base_analysis + list(analysis_add or [])) if str(x).strip()],
            'happened_lines': [str(x).strip() for x in list(happened or []) if str(x).strip()],
            'visible_lines': [str(x).strip() for x in list(visible or []) if str(x).strip()],
            'secondary_labels': [str(x).strip() for x in list(secondary or []) if str(x).strip()],
            'improve_labels': [str(x).strip() for x in list(improve or []) if str(x).strip()],
        }
        candidates.append((float(score), key, payload))

    # Shared timing/SL/confirmation detectors.
    # IMPORTANT: FAST_SL is only a context flag. It must not become the main
    # Telegram reason when the chart gives a real cause (location, structure,
    # market reversal, weak confirm, TP path, OB/FVG failure, etc.). The final
    # selector below will prefer the best real chart reason over generic
    # fast/tight-SL labels and merge FAST_SL only as an additional fact.
    direction_word = 'bearish' if side_u == 'SHORT' else 'bullish'
    opp_pullback_word = 'bounce/reclaim' if side_u == 'SHORT' else 'pullback/rejection'
    add(2.4 + (0.6 if fast else 0) + (0.4 if meaningful_excursion_missing else 0), 'fast_sl_no_excursion',
        primary='SL hit fast before any meaningful excursion',
        scenario='Цена почти сразу пошла против входа: нормального хода в сторону сделки не было, TP1 не был поставлен под угрозу, поэтому SL сработал как быстрый invalidation.',
        analysis_add=['SL был достигнут быстро', 'ход в сторону сделки был слишком маленький'],
        happened=['после входа не появилось нормальное continuation-движение', 'цена быстро ушла против позиции', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут без полезного excursion'],
        visible=[f'{side_u} не получил clean {direction_word} continuation после входа', 'первое движение против входа было сильнее движения в сторону сделки', 'свечи после входа не подтвердили acceptance', pos_line],
        secondary=['Fast invalidation', 'No meaningful excursion', 'TP1 was not threatened'],
        improve=['не открывать сделку, если confirmation не даёт immediate follow-through', 'после быстрого SL считать setup trap и ждать новый retest'],
        evidence=bool(fast and no_tp and meaningful_excursion_missing))
    add(2.8 + (0.7 if normal_pullback else 0), 'sl_inside_normal_pullback',
        primary='SL was inside normal pullback / retest noise',
        scenario=f'SL стоял не за реальной invalidation-зоной, а внутри обычного {opp_pullback_word} после импульса. Цена сделала нормальный откат/ретест и выбила SL раньше, чем появился continuation.',
        analysis_add=['SL стоял внутри обычного pullback/retest-шума'],
        happened=['первый откат против входа оказался сильнее SL', 'структура не успела подтвердить continuation', 'SL был достигнут раньше нормального продолжения'],
        visible=['SL находился слишком близко к entry относительно шума свечей', 'после входа цена сделала обычный retest/pullback', 'уровень invalidation был дальше, чем фактический SL', pos_line],
        secondary=['SL too tight', 'SL inside pullback', 'Retest noise hit SL'],
        improve=['ставить SL за структурной invalidation-зоной', 'не ставить SL внутри первой зоны pullback после импульса'],
        evidence=bool(normal_pullback and (risk_pct <= max(tight_sl_pct, 0.45) or any_flag('sl_too_tight','sl_inside_pullback','sl_inside_normal_pullback'))))
    add(3.8 + (2 if weak_confirm else 0), 'weak_confirm_no_displacement',
        primary=f'Weak {direction_word} confirm / no displacement after entry',
        scenario='Сигнал имел формальное подтверждение, но после входа не появилось сильной displacement-свечи и acceptance. Цена не показала продолжение и ушла к SL.',
        analysis_add=[f'{direction_word} displacement после входа отсутствовал', 'follow-through был слабый'],
        happened=['confirm-candle не получила follow-through', 'цена не закрепилась в сторону сделки', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после слабой реакции'],
        visible=[f'нет clean {direction_word} displacement после входа', 'acceptance после retest не подтвердился', 'первые свечи после входа были слабыми/смешанными', pos_line],
        secondary=['Weak confirm candle', 'No displacement', 'No retest acceptance'],
        improve=['требовать сильный close в сторону сделки после retest', 'не входить только по касанию зоны без follow-through'],
        evidence=bool(weak_confirm and weak_follow and no_tp))
    add(3.5 + (2 if tight_space else 0), 'tp1_too_close_no_clean_space',
        primary='TP1 path had no clean space',
        scenario='До TP1 было мало чистого пространства. Цена должна была сразу пробить ближнюю реакционную область, но continuation не появился, поэтому сделка закрылась по SL.',
        analysis_add=['пространство до TP1 было маленькое или проходило через reaction area'],
        happened=['цена не смогла быстро пройти путь до TP1', 'ближняя реакционная зона остановила движение', 'TP1 не был нормально поставлен под угрозу'],
        visible=['путь от entry до TP1 проходил через ближнюю reaction area', 'TP1 стоял за ближней зоной реакции рынка', 'RR был формально нормальный, но путь к TP1 был плохой', pos_line],
        secondary=['No clean space to TP1', 'TP1 behind reaction area', 'Poor target path'],
        improve=['требовать свободный путь до TP1', 'не брать вход, если TP1 сразу упирается в support/resistance'],
        evidence=bool(tight_space and no_tp))
    add(3.4 + (2 if range_trap else 0), 'range_chop_no_direction',
        primary='Entry inside chop/range / no directional expansion',
        scenario='Вход оказался внутри локального chop/range. После входа рынок не дал directional expansion, несколько свечей гасили друг друга, и цена ушла к SL.',
        analysis_add=['range/chop behavior после входа', 'нет чистого directional expansion'],
        happened=['рынок не выбрал направление в сторону сделки', 'первые push-и были слабыми', 'цена вернулась против entry'],
        visible=['свечи после входа выглядят как chop/range, а не continuation', 'нет уверенного закрепления за retest-зоной', pos_line],
        secondary=['Range trap', 'Chop after entry', 'Multiple failed pushes'],
        improve=['избегать входа в середине локального range', 'ждать выход из chop и подтверждённый retest'],
        evidence=range_trap or (midrange and weak_follow and no_tp))
    add(3.0 + (1.5 if volume_faded else 0), 'volume_momentum_faded',
        primary='Momentum/volume faded after entry',
        scenario='После входа импульс начал затухать: follow-through не появился, объём/движение ослабли, и цена ушла против позиции.',
        analysis_add=['momentum после входа ослаб', 'volume/follow-through не поддержали сделку'],
        happened=['свечи не продолжили движение в сторону сделки', 'последующие попытки были слабее', 'рынок вернулся к SL'],
        visible=['после входа нет расширения тела свечей в сторону сделки', 'импульс затухает вместо continuation', pos_line],
        secondary=['Momentum faded', 'Weak follow-through', 'Volume faded'],
        improve=['не держать setup, если первые свечи после входа теряют momentum', 'требовать импульс/объём после confirm'],

        evidence=bool(volume_faded and weak_follow and no_tp))

    # v9 full reason catalog detectors.  These keep every common LOSS reason
    # available as an explicit candidate, but still require a matching flag,
    # metric, setup text, structure text, or zone context before printing it.
    # Rule: no evidence -> no text in Telegram card.
    is_short = side_u == 'SHORT'
    side_word = 'SHORT' if is_short else 'LONG'
    opp_zone_word = 'demand/support' if is_short else 'supply/resistance'
    opp_reaction_word = 'buyer reaction / bounce' if is_short else 'seller reaction / rejection'
    direction_loss_word = 'downside' if is_short else 'upside'
    confirm_word = 'bearish' if is_short else 'bullish'
    strong_bad_context = bullish_ctx if is_short else bearish_ctx
    near_bad_zone = demand_seen if is_short else supply_seen
    bad_zone_blocks = demand_blocks if is_short else supply_blocks
    bad_zone_desc = str((demand_ctx if is_short else supply_ctx).get('desc') or opp_zone_word)
    too_bad_location = lowish if is_short else highish
    late_after_extended = late_short if is_short else late_long
    move_name = 'dump' if is_short else 'pump'
    premium_discount_bad = (lowish if is_short else highish)

    # v9.1 explicit reason keys requested for card precision.
    # These reasons used to appear only as secondary labels/text.  They are now
    # first-class candidates, so the card can select them as the main reason when
    # the snapshot clearly proves that exact failure mode.
    add(6.4 + (1.0 if weak_confirm else 0) + (0.6 if fast else 0), 'no_acceptance_after_retest',
        primary=f'{side_word} no acceptance after retest',
        scenario=f'{side_word} получил формальный trigger/retest, но цена не закрепилась в сторону сделки. После входа не было {confirm_word} acceptance выше/ниже entry-zone, поэтому retest не подтвердился и цена ушла к SL.',
        analysis_add=['acceptance после retest отсутствовал', f'fresh {confirm_word} displacement после входа отсутствовал'],
        happened=['цена не закрепилась в сторону сделки после retest', 'confirm не получил follow-through', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после возврата против entry'],
        visible=['после retest нет 1–2 strong close в сторону сделки', f'нет clean {confirm_word} displacement после входа', 'entry/retest зона не получила acceptance', pos_line],
        secondary=['No acceptance after retest', 'Retest failed', 'No clean displacement'],
        improve=['после retest ждать 1–2 close в сторону сделки', 'не входить по одному касанию зоны без acceptance', 'если первая свеча после entry слабая — пропускать setup'],
        evidence=bool(any_flag('no_acceptance_after_retest','no_retest_acceptance','retest_acceptance_missing','reclaim_without_acceptance','no_clean_retest_acceptance') or (weak_follow and no_tp and (weak_confirm or any_text('no acceptance','acceptance missing','no retest acceptance')))))
    add(5.9 + (0.8 if fast else 0), 'no_post_entry_expansion',
        primary=f'{side_word} no post-entry expansion',
        scenario=f'После входа не появилась fresh {confirm_word} expansion candle. Сделка стояла без прогресса, импульс не расширился в сторону TP1, и цена вернулась к SL.',
        analysis_add=[f'fresh {confirm_word} expansion candle после entry отсутствовала', 'post-entry expansion отсутствовал'],
        happened=['после входа не появилось расширения движения', 'первые свечи не подтвердили continuation', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после слабого follow-through'],
        visible=[f'нет fresh {confirm_word} expansion после entry', 'свечи после входа слабые/смешанные', pos_line],
        secondary=['No post-entry expansion', 'No fresh expansion candle', 'Weak continuation'],
        improve=['требовать fresh expansion candle после trigger', 'если entry не получает continuation в первые свечи — не держать идею как сильный setup'],
        evidence=bool(any_flag('no_post_entry_expansion','no_fresh_expansion_candle','post_entry_expansion_missing') or (weak_follow and no_tp and not normal_pullback)))
    # V20: WBTC-type precision.  When TP1 is geometrically close (for
    # example 0.4-0.6%) but it sits above the nearest local high/resistance,
    # the real chart reason is not just a generic "no acceptance".  The
    # target path itself is bad: price must break a seller reaction area before
    # TP1 can be reached.  This should outrank the generic
    # `long_under_resistance_no_acceptance` card for tight TP1 losses.
    tight_tp1_resistance_path = bool(
        side_u == 'LONG'
        and no_tp
        and weak_follow
        and not wide_tp_target
        and not tp1_near_miss
        and 0 < cs <= max(tight_space_pct, tight_tp1_blocked_pct)
        and (supply_seen or supply_blocks or highish or ob_fvg_route or tight_space)
        and not bearish_ctx
    )
    add(11.9 + (0.8 if supply_seen else 0.0) + (0.6 if supply_blocks else 0.0) + (0.4 if highish else 0.0), 'tp1_blocked_by_resistance_tight_path_long',
        primary='TP1 blocked by local resistance / no clean upside path',
        scenario=f'LONG был открыт под ближайшим local high / resistance ({supply_desc}). До TP1 было всего около {cs:.2f}%, но эта цель стояла за seller reaction area: для TP1 цене сначала нужно было закрепиться выше resistance. Acceptance выше entry/retest зоны и fresh bullish displacement после входа не появились, поэтому движение превратилось в rejection/pullback к SL.',
        analysis_add=[cs_line, risk_line, 'TP1 стоял за ближайшим local resistance / seller reaction area', 'acceptance выше entry/retest зоны отсутствовал', 'fresh bullish displacement после входа отсутствовал', mfe_line],
        happened=['покупатель не смог принять цену выше local resistance', 'TP1 был заблокирован ближайшей seller reaction area', 'после entry не появился новый bullish expansion', 'SL был достигнут после rejection/отката'],
        visible=[f'над входом была {supply_desc}', 'между entry и TP1 не было чистого пути: цель стояла за resistance', 'LONG открыт под/рядом с local high / seller reaction area', 'после входа нет clean bullish displacement', 'цена не удержала reclaim/entry-зону', pos_line],
        secondary=['TP1 blocked by resistance', 'No clean upside path', 'No acceptance after retest', 'Weak bullish follow-through', 'Seller rejection'],
        improve=['не брать LONG, если TP1 сразу за local resistance', 'ждать 1–2 close выше seller reaction area', 'ставить TP1 до первой сильной seller zone или пропускать сделку', 'если после retest нет displacement — пропускать вход'],
        evidence=tight_tp1_resistance_path)

    # V21: BOME-type precision.  A fast LONG opened at the top of a
    # near-vertical pump must not be explained as a generic retest/acceptance
    # failure.  The visible issue is post-pump exhaustion: entry was close to
    # the local high after an extended move, TP1 was above the fresh high, and
    # the first reaction after entry was a fast rejection to SL.
    try:
        post_pump_fast_min_risk_pct = float(str(os.getenv('LOSS_CARD_POST_PUMP_FAST_MIN_RISK_PCT', '0.60') or '0.60').replace(',', '.'))
    except Exception:
        post_pump_fast_min_risk_pct = 0.60
    try:
        post_pump_fast_min_tp_space_pct = float(str(os.getenv('LOSS_CARD_POST_PUMP_FAST_MIN_TP_SPACE_PCT', '0.90') or '0.90').replace(',', '.'))
    except Exception:
        post_pump_fast_min_tp_space_pct = 0.90
    post_pump_fast_long_rejection = bool(
        side_u == 'LONG'
        and no_tp
        and fast
        and weak_follow
        and not tp1_near_miss
        and (late_long or vertical_long_pump or bullish_ctx or structure_pending_route)
        and (cs <= 0 or cs >= post_pump_fast_min_tp_space_pct)
        and (risk_pct <= 0 or risk_pct >= post_pump_fast_min_risk_pct)
        and not bearish_ctx
    )
    add(15.6 + (0.8 if vertical_long_pump else 0.0) + (0.5 if structure_pending_route else 0.0) + (0.4 if highish else 0.0), 'post_pump_fast_long_rejection_bome',
        primary='Late LONG at post-pump high / buyer exhaustion fast rejection',
        scenario='LONG был открыт уже после сильного buy-side pump, фактически в верхней части свежего движения. TP1 стоял выше свежего high, но перед входом не было нормального discount pullback/retest и нового acceptance. Покупатель был истощён: вместо fresh bullish continuation первая реакция быстро превратилась в rejection/pullback к SL.',
        analysis_add=['entry был после сильного buy-side pump / extended move', 'вход был ближе к post-pump high, а не из discount', 'TP1 требовал продолжения выше свежего high', 'acceptance/fresh bullish displacement после входа отсутствовал'],
        happened=['покупатель не дал новую волну вверх после входа', 'после entry быстро появился rejection/pullback', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут как fast invalidation после exhaustion'],
        visible=['перед входом уже прошёл почти безоткатный рост / pump', 'LONG открыт на вершине свежего impulse, а не после глубокого pullback', 'рядом сверху был fresh local high / seller reaction после pump', 'после входа нет clean bullish expansion', 'первая реакция быстро пошла против LONG', pos_line],
        secondary=['Late long after pump', 'Post-pump high entry', 'Buyer exhaustion', 'Fast rejection', 'No discount re-entry', 'No fresh bullish displacement'],
        improve=['после сильного pump ждать pullback ниже / discount re-entry', 'не брать LONG на свежем high без 1–2 close выше high', 'для Structure pending trigger требовать реальный retest, а не вход в конце extension', 'если TP1 стоит выше fresh high после pump — ждать acceptance выше high или пропускать'],
        evidence=post_pump_fast_long_rejection)

    if side_u == 'LONG':
        add(13.0 + (0.9 if late_long else 0.0) + (0.6 if vertical_long_pump else 0.0), 'long_near_tp1_then_rejected_after_pump',
            primary='LONG pushed near TP1, then rejected / no acceptance above high',
            scenario='LONG сначала дал движение в сторону TP1 и почти поставил цель под угрозу, но выше локального high/resistance не появилось acceptance. После near-miss покупатель потерял momentum, цена дала rejection/pullback и ушла к SL. Это не "TP1 was never threatened" — TP1 был близко, но continuation выше high не подтвердился.',
            analysis_add=[cs_line, mfe_line, 'TP1 был близко, но не взят', 'acceptance выше local high/resistance отсутствовал', 'fresh bullish expansion после near-miss отсутствовал'],
            happened=['первый push пошёл в сторону TP1', 'выше локального high не появилось закрепление', 'после near-miss цена развернулась против LONG', 'SL был достигнут после rejection/pullback'],
            visible=['после entry был ход вверх в сторону TP1', 'цена не приняла уровень выше локального high/resistance', 'после near-miss появился seller rejection', 'TP1 был близко, но не подтверждён continuation', pos_line],
            secondary=['TP1 near-miss', 'No acceptance above high', 'Momentum faded after near TP1', 'Seller rejection after push'],
            improve=['после near-miss к TP1 быстрее защищать позицию/часть выхода', 'для продолжения требовать close выше local high', 'не держать LONG, если после подхода к TP1 появляется rejection'],
            evidence=bool(tp1_near_miss and (late_long or vertical_long_pump or supply_seen or highish or b('no_acceptance_after_retest') or b('no_post_entry_expansion'))))
    elif side_u == 'SHORT':
        add(13.0 + (0.9 if late_short else 0.0), 'short_near_tp1_then_bounced_after_dump',
            primary='SHORT pushed near TP1, then bounced / no acceptance below low',
            scenario='SHORT сначала дал движение в сторону TP1 и почти поставил цель под угрозу, но ниже локального low/support не появилось acceptance. После near-miss продавец потерял momentum, цена дала bounce/reclaim и ушла к SL. Это не "TP1 was never threatened" — TP1 был близко, но continuation ниже low не подтвердился.',
            analysis_add=[cs_line, mfe_line, 'TP1 был близко, но не взят', 'acceptance ниже local low/support отсутствовал', 'fresh bearish expansion после near-miss отсутствовал'],
            happened=['первый push пошёл в сторону TP1', 'ниже локального low не появилось закрепление', 'после near-miss цена развернулась против SHORT', 'SL был достигнут после bounce/reclaim'],
            visible=['после entry был ход вниз в сторону TP1', 'цена не приняла уровень ниже local low/support', 'после near-miss появилась buyer reaction', 'TP1 был близко, но не подтверждён continuation', pos_line],
            secondary=['TP1 near-miss', 'No acceptance below low', 'Momentum faded after near TP1', 'Buyer bounce after push'],
            improve=['после near-miss к TP1 быстрее защищать позицию/часть выхода', 'для продолжения требовать close ниже local low', 'не держать SHORT, если после подхода к TP1 появляется bounce'],
            evidence=bool(tp1_near_miss and (late_short or demand_seen or lowish or b('no_acceptance_after_retest') or b('no_post_entry_expansion'))))

    add(5.6 + (0.6 if meaningful_excursion_missing else 0), 'tp1_was_never_threatened',
        primary='TP1 was never threatened',
        scenario='Цена после входа не приблизилась к TP1 достаточно близко и не поставила первую цель под реальную угрозу. Это означает, что setup не получил нужного continuation, а LOSS произошёл до нормального развития идеи.',
        analysis_add=['TP1 не был нормально поставлен под угрозу', mfe_line],
        happened=['движение в сторону сделки было слабым', 'рынок не дошёл до зоны TP1', 'после слабого excursion цена развернулась к SL'],
        visible=['TP1 не был протестирован перед SL', f'{side_u} не получил достаточного хода в сторону сделки', pos_line],
        secondary=['TP1 was never threatened', 'No meaningful excursion', 'Continuation missing'],
        improve=['не засчитывать формальный confirm без реального движения к TP1', 'добавить фильтр MFE/R после entry', 'требовать первый push в сторону сделки'],
        evidence=bool((any_flag('tp1_was_never_threatened','tp1_never_threatened') and not tp1_near_miss) or (no_tp and meaningful_excursion_missing)))
    add(3.6 + (0.8 if sl_tight else 0), 'sl_too_tight',
        primary='SL too tight for normal market noise',
        scenario='SL был поставлен слишком близко к entry относительно обычного свечного шума. Даже без полной invalidation рынок мог сделать нормальный pullback/bounce и выбить такой SL.',
        analysis_add=[risk_line, 'SL был слишком близко к entry для такого шума'],
        happened=['обычный откат против входа оказался больше SL', 'структурная invalidation была дальше фактического SL', 'SL был достигнут до подтверждённого continuation'],
        visible=['SL расположен внутри ближнего свечного шума', 'после входа был обычный retest/pullback, а не чистая structural invalidation', pos_line],
        secondary=['SL too tight', 'SL inside noise', 'Structural invalidation farther away'],
        improve=['ставить SL за structural invalidation', 'не ставить SL только за слабый micro-level', 'если нормальный SL ломает RR — пропускать сделку'],
        evidence=bool(any_flag('sl_too_tight','tight_sl') or (sl_tight and no_tp)))
    add(4.2 + (0.8 if normal_pullback else 0), 'sl_inside_normal_bounce',
        primary='SL inside normal bounce after SHORT',
        scenario='SHORT был открыт, но SL стоял внутри обычного bounce/retest после снижения. Цена сделала нормальный откат вверх против SHORT и выбила SL до подтверждённого bearish continuation.',
        analysis_add=[risk_line, 'SL стоял внутри normal bounce/retest после SHORT'],
        happened=['после входа появился обычный bounce против SHORT', 'bounce оказался сильнее SL', 'bearish continuation не успел подтвердиться', 'SL был достигнут до нового sell-side impulse'],
        visible=['SL находился внутри обычного bounce/retest-движения', 'SHORT не получил clean bearish displacement после entry', pos_line],
        secondary=['SL inside normal bounce', 'SL too tight', 'Bearish continuation missing'],
        improve=['для SHORT ставить SL за structural invalidation выше bounce-зоны', 'не шортить, если первый bounce легко выбивает SL', 'если нормальный SL ломает RR — пропускать сделку'],
        evidence=bool(is_short and (any_flag('sl_inside_normal_bounce','sl_inside_bounce') or normal_pullback)))
    add(6.1 + (1.0 if bad_zone_blocks else 0) + (0.6 if normal_pullback else 0), 'short_above_local_support',
        primary='SHORT above local support / no acceptance below entry',
        scenario=f'SHORT был открыт прямо над local support / buyer reaction area ({bad_zone_desc}). Цена не закрепилась ниже entry/retest зоны, продавец не получил fresh bearish displacement, и support дал bounce к SL.',
        analysis_add=['SHORT был над local support / buyer reaction area', 'acceptance ниже entry/retest зоны отсутствовал'],
        happened=['продавец не смог продавить local support', 'появился buyer bounce против SHORT', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после reclaim/bounce вверх'],
        visible=['снизу была local support / buyer reaction area', 'SHORT открыт слишком близко над support', 'после entry нет clean bearish displacement', pos_line],
        secondary=['SHORT above local support', 'No acceptance below entry', 'Buyer bounce'],
        improve=['не шортить прямо над local support', 'ждать premium re-entry выше', 'входить только после close ниже support/buyer reaction area'],
        evidence=bool(is_short and (any_flag('short_above_local_support','short_above_support') or (no_tp and (demand_seen or demand_blocks or lowish)))))
    add(6.0 + (1.0 if (not is_short and bad_zone_blocks) else 0), 'tp1_blocked_by_resistance',
        primary='TP1 blocked by local resistance',
        scenario='LONG имел формальный RR, но путь к TP1 проходил через ближайший local resistance / seller reaction area. Покупатель не закрепился выше этой зоны, поэтому TP1 не был нормально поставлен под угрозу и цена ушла к SL.',
        analysis_add=[cs_line, 'TP1 был заблокирован local resistance / seller reaction area'],
        happened=['покупатель не смог пробить ближний resistance', 'seller reaction появилась раньше continuation', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после rejection/отката'],
        visible=['между entry и TP1 была local resistance / seller reaction area', 'TP1 стоял рядом/за зоной реакции продавца', 'после входа нет clean bullish displacement', pos_line],
        secondary=['TP1 blocked by resistance', 'No clean upside path', 'Seller reaction before TP1'],
        improve=['не брать LONG, если TP1 сразу за resistance', 'ждать acceptance выше resistance', 'ставить TP1 до первой сильной seller reaction area или пропускать вход'],
        evidence=bool((not is_short) and (any_flag('tp1_blocked_by_resistance','tp1_blocked_by_local_resistance','resistance_blocks_tp1','supply_zone_blocks_tp1') or (no_tp and (supply_blocks or (tight_space and supply_seen))))))
    add(6.0 + (1.0 if (is_short and bad_zone_blocks) else 0), 'tp1_blocked_by_support',
        primary='TP1 blocked by local support',
        scenario='SHORT имел формальный RR, но путь к TP1 проходил через ближайший local support / buyer reaction area. Продавец не закрепился ниже этой зоны, поэтому TP1 не был нормально поставлен под угрозу и цена дала bounce к SL.',
        analysis_add=[cs_line, 'TP1 был заблокирован local support / buyer reaction area'],
        happened=['продавец не смог пробить ближний support', 'buyer reaction появилась раньше continuation', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после bounce/reclaim вверх'],
        visible=['между entry и TP1 была local support / buyer reaction area', 'TP1 стоял рядом/за зоной реакции покупателя', 'после входа нет clean bearish displacement', pos_line],
        secondary=['TP1 blocked by support', 'No clean downside path', 'Buyer reaction before TP1'],
        improve=['не брать SHORT, если TP1 сразу за support', 'ждать acceptance ниже support', 'ставить TP1 до первой сильной buyer reaction area или пропускать вход'],
        evidence=bool(is_short and (any_flag('tp1_blocked_by_support','tp1_blocked_by_local_support','support_blocks_tp1','demand_zone_blocks_tp1') or (no_tp and (demand_blocks or (tight_space and demand_seen))))))
    add(5.7 + (0.8 if weak_confirm else 0), 'follow_through_failed',
        primary=f'{side_word} follow-through failed after confirm',
        scenario=f'Confirm был формально принят, но следующего {confirm_word} follow-through не появилось. Первые свечи после entry не продолжили движение, поэтому setup потерял momentum и ушёл к SL.',
        analysis_add=['follow-through после входа был слабый/отсутствовал', f'{confirm_word} continuation не подтвердился'],
        happened=['confirm-candle не получила продолжение', 'цена не закрепилась в сторону сделки', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после failure follow-through'],
        visible=[f'после confirm нет clean {confirm_word} continuation', 'первые свечи после entry не расширили движение', pos_line],
        secondary=['Follow-through failed', 'Weak confirm continuation', 'No fresh displacement'],
        improve=['после confirm требовать продолжение следующей свечой', 'не входить, если confirm есть только формально, без follow-through'],
        evidence=bool(any_flag('follow_through_failed','followthrough_failed','weak_followthrough') or (weak_follow and no_tp)))

    # Market / BTC context. These flags are optional; if backend/snapshot provides
    # them, a fast SL can be explained as a real market reversal instead of a
    # generic "SL too tight". Numeric keys are tolerated with several names so
    # older snapshots do not break.
    btc_move_pct = f('btc_move_after_entry_pct', f('btc_first_move_pct', f('btc_move_first3_pct', 0.0)))
    market_move_pct = f('market_move_after_entry_pct', f('market_first_move_pct', f('market_move_first3_pct', 0.0)))
    try:
        market_reversal_min_pct = float(str(os.getenv('LOSS_CARD_MARKET_REVERSAL_MIN_PCT', '0.18') or '0.18').replace(',', '.'))
    except Exception:
        market_reversal_min_pct = 0.18
    if side_u == 'SHORT':
        market_against = bool(
            any_flag('market_reversal_against_position','btc_reversal_against_position','btc_bounce_against_short','broad_market_bounce')
            or any_text('btc bounce', 'market bounce', 'market reversal', 'btc reversal')
            or btc_move_pct >= market_reversal_min_pct
            or market_move_pct >= market_reversal_min_pct
        )
        market_primary = 'Market/BTC bounce invalidated SHORT'
        market_scenario = 'После входа рынок дал быстрый bounce против SHORT. Bearish continuation не появился, общий market/BTC impulse поднял цену обратно к entry/SL, поэтому сделка быстро закрылась по SL.'
        market_secondary = ['Market reversal', 'BTC/market bounce against SHORT', 'No bearish continuation']
        market_visible = ['BTC/общий рынок двигался против SHORT после входа', 'после входа не появился clean bearish displacement', pos_line]
        market_improve = ['не брать SHORT, если BTC/общий рынок уже даёт bounce против сделки', 'добавить фильтр market context перед входом']
    else:
        market_against = bool(
            any_flag('market_reversal_against_position','btc_reversal_against_position','btc_dump_against_long','broad_market_dump')
            or any_text('btc dump', 'market dump', 'market reversal', 'btc reversal')
            or btc_move_pct <= -market_reversal_min_pct
            or market_move_pct <= -market_reversal_min_pct
        )
        market_primary = 'Market/BTC dump invalidated LONG'
        market_scenario = 'После входа рынок дал быстрый dump против LONG. Bullish continuation не появился, общий market/BTC impulse продавил цену обратно к entry/SL, поэтому сделка быстро закрылась по SL.'
        market_secondary = ['Market reversal', 'BTC/market dump against LONG', 'No bullish continuation']
        market_visible = ['BTC/общий рынок двигался против LONG после входа', 'после входа не появился clean bullish displacement', pos_line]
        market_improve = ['не брать LONG, если BTC/общий рынок уже давит вниз против сделки', 'добавить фильтр market context перед входом']

    add(8.0 + (1.2 if fast else 0) + (0.8 if immediate_counter else 0), 'market_reversal_against_position',
        primary=market_primary,
        scenario=market_scenario,
        analysis_add=['market/BTC context был против позиции', 'после входа движение рынка пошло против сделки'],
        happened=['после входа market/BTC impulse пошёл против позиции', 'сетап не получил continuation', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут из-за рыночного разворота'],
        visible=market_visible,
        secondary=market_secondary,
        improve=market_improve,
        evidence=bool(market_against and no_tp))

    # 1) LOCATION / место входа
    add(5.2, 'entry_inside_range_middle',
        primary=f'{side_word} opened inside range middle / chop zone',
        scenario='Вход был не из сильной premium/discount зоны, а ближе к середине локального диапазона. В середине range цена часто пилит обе стороны, поэтому setup не получил directional expansion и ушёл к SL.',
        analysis_add=['entry находился внутри середины локального range'],
        happened=['после входа не появилось уверенное continuation-движение', 'рынок остался в chop/range', 'SL был достигнут после возврата против позиции'],
        visible=['вход не был от края диапазона', 'цена торговалась внутри локального range', pos_line],
        secondary=['Entry inside range middle', 'Chop/range location', 'No directional edge'],
        improve=['не входить в середине range', 'ждать вход от premium/discount края или подтверждённый breakout + retest'],
        evidence=bool(midrange and weak_follow and no_tp))
    add(5.3, 'entry_near_local_extreme',
        primary=f'{side_word} opened near local extreme',
        scenario=f'Вход был слишком близко к локальному extreme. Для {side_word} путь до продолжения был ограничен, а вероятность обратной реакции стала высокой.',
        analysis_add=['entry был рядом с local high/low'],
        happened=['цена быстро дала реакцию против позиции', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после обратного движения'],
        visible=['entry расположен рядом с локальным high/low', f'рядом была зона {opp_reaction_word}', pos_line],
        secondary=['Entry near local high/low', 'Poor entry location', 'Reaction zone nearby'],
        improve=['не входить прямо возле local high/low после уже прошедшего импульса', 'ждать ретест глубже в сторону лучшей цены'],
        evidence=bool(any_flag('entry_near_local_extreme','entry_near_local_high_low','entry_near_local_low','entry_near_local_high') or (too_bad_location and no_tp)))
    add(5.5, 'entry_after_extended_move',
        primary=f'Late entry after extended {move_name}',
        scenario=f'{side_word} был открыт после уже выполненного сильного движения. Momentum к моменту входа был частично потрачен, поэтому вместо continuation появилась обратная реакция и SL.',
        analysis_add=['entry был после extended move'],
        happened=['после входа не появился новый fresh expansion', 'движение против позиции появилось быстрее continuation', 'SL был достигнут после exhaustion/reaction'],
        visible=[f'перед входом уже был extended {move_name}', 'вход был поздним относительно импульса', pos_line],
        secondary=['Entry after extended move', 'Momentum already spent', 'Late entry'],
        improve=['после сильного импульса ждать pullback/retest с лучшей ценой', 'не входить в конце расширения'],
        evidence=bool(late_after_extended and no_tp))
    add(5.0, 'entry_too_far_from_fvg_ob',
        primary='Entry too far from FVG/OB origin zone',
        scenario='Вход был слишком далеко от реальной FVG/OB зоны. Цена уже ушла от зоны, поэтому risk был хуже, SL оказался ближе к шуму, а continuation не подтвердился.',
        analysis_add=['entry был далеко от FVG/OB зоны'],
        happened=['цена не получила сильную реакцию прямо от зоны', 'после входа не появился clean displacement', 'SL был достигнут после отката к зоне/против входа'],
        visible=['entry не был рядом с origin/FVG/OB', 'между зоной и entry уже был пройденный импульс', pos_line],
        secondary=['Entry too far from FVG/OB', 'Bad zone distance', 'Late zone entry'],
        improve=['входить ближе к FVG/OB origin', 'не брать сигнал, если цена уже далеко ушла от зоны'],
        evidence=bool(any_flag('entry_too_far_from_fvg_ob','entry_far_from_zone','late_zone_entry') or (ob_fvg_route and late_after_extended and weak_follow)))
    add(5.3, 'bad_premium_discount_location',
        primary='Bad premium/discount location',
        scenario=f'Location был плохой для {side_word}: сделка открыта в зоне, где ожидание continuation слабее, а обратная реакция сильнее. Из-за этого цена быстро пошла к SL.',
        analysis_add=['premium/discount location был слабый'],
        happened=['рынок не дал продолжение из этой зоны', 'появилась реакция против позиции', 'SL был достигнут до нормального TP1 threat'],
        visible=[f'{side_word} открыт в плохой premium/discount зоне', pos_line],
        secondary=['Bad premium/discount location', 'Poor location', 'Opposite reaction risk'],
        improve=['SHORT искать выше в premium, LONG искать ниже в discount', 'не входить, если location уже даёт плохую цену'],
        evidence=bool(premium_discount_bad and no_tp))

    # 2) SL / проблема стопа
    add(5.4, 'sl_inside_wick_zone',
        primary='SL inside wick rejection zone',
        scenario='SL стоял внутри зоны фитилей/снятия ликвидности. Обычный wick sweep против entry был достаточен, чтобы выбить стоп, хотя структурная invalidation была дальше.',
        analysis_add=['SL попал внутрь wick zone'],
        happened=['цена сделала wick/sweep против входа', 'стоп был выбит фитилём/шумом', 'после этого setup мог ещё не быть структурно сломан'],
        visible=['SL расположен внутри зоны предыдущих фитилей', 'рядом была wick/liquidity область', pos_line],
        secondary=['SL inside wick zone', 'Wick noise hit SL', 'Stop placed in liquidity'],
        improve=['ставить SL за wick cluster / liquidity sweep зоной', 'не ставить SL внутри области частых фитилей'],
        evidence=bool(any_flag('sl_inside_wick_zone','sl_in_wick_zone','wick_zone_hit_sl','sl_inside_wicks')))
    add(5.6, 'sl_before_structural_invalidation',
        primary='SL before real structural invalidation',
        scenario='SL был поставлен до реальной structural invalidation. Цена выбила короткий стоп обычным откатом, но структура сделки формально ломалась дальше.',
        analysis_add=['SL был до structural invalidation'],
        happened=['обычный откат дошёл до SL', 'уровень реальной invalidation был дальше', 'сделка закрылась раньше структурного слома'],
        visible=['SL не стоял за structural high/low', 'между SL и настоящей invalidation-зоной оставался запас', pos_line],
        secondary=['SL before structural invalidation', 'Invalidation level farther', 'Premature SL'],
        improve=['SL ставить за реальной structural invalidation', 'если RR ломается после структурного SL — пропускать сделку'],
        evidence=bool(any_flag('sl_before_structural_invalidation','sl_not_behind_structure','structural_sl_farther') or (normal_pullback and any_text('structural invalidation'))))
    add(5.1, 'sl_too_close_to_fvg_ob_edge',
        primary='SL too close to FVG/OB edge',
        scenario='SL стоял слишком близко к краю FVG/OB зоны. Обычный retest/mitigation зашёл чуть глубже зоны и выбил стоп до подтверждения continuation.',
        analysis_add=['SL был слишком близко к FVG/OB edge'],
        happened=['цена сделала глубокий retest зоны', 'стоп был выбит внутри нормальной mitigation области', 'continuation не успел подтвердиться'],
        visible=['SL почти на границе FVG/OB', 'зона могла дать более глубокий retest', pos_line],
        secondary=['SL too close to FVG/OB edge', 'Deep mitigation hit SL', 'Zone edge SL'],
        improve=['для OB/FVG ставить SL за полной зоной/структурой', 'не ставить SL прямо на edge зоны'],
        evidence=bool(any_flag('sl_too_close_to_fvg_ob_edge','sl_close_to_zone_edge','sl_inside_zone_edge') or (ob_fvg_route and normal_pullback)))
    add(4.9, 'sl_weak_micro_level_only',
        primary='SL protected only by weak micro level',
        scenario='SL был спрятан только за слабым micro-level, а не за сильной структурой. Такой уровень легко снимается обычным retest/sweep, поэтому цена дошла до SL.',
        analysis_add=['SL защищал только weak micro level'],
        happened=['micro-level не удержал цену', 'рынок снял локальный уровень', 'SL был достигнут без сильного structural break'],
        visible=['SL был за маленьким локальным уровнем', 'сильной HTF/LTF invalidation за SL не было', pos_line],
        secondary=['SL above/below weak micro level only', 'Weak SL protection', 'Micro level sweep'],
        improve=['не ставить SL только за микрофитилем', 'использовать structural SL или пропускать плохой RR'],
        evidence=bool(any_flag('sl_weak_micro_level_only','weak_micro_level_sl','sl_above_weak_micro_level','sl_below_weak_micro_level')))
    add(5.2, 'sl_inside_liquidity_sweep_zone',
        primary='SL placed inside liquidity sweep zone',
        scenario='SL оказался в зоне, где рынок часто снимает ликвидность. Цена сделала sweep против позиции, забрала стоп и не дала нормального TP1 threat.',
        analysis_add=['SL был внутри liquidity sweep зоны'],
        happened=['цена сняла ликвидность возле SL', 'после sweep стоп был выбит', 'до TP1 движение не дошло'],
        visible=['SL стоял в очевидной liquidity area', 'рядом были equal highs/lows или wick cluster', pos_line],
        secondary=['SL inside liquidity sweep zone', 'Stop hunt area', 'Liquidity near SL'],
        improve=['выносить SL за liquidity pool', 'не ставить SL там, где очевидно лежат стопы'],
        evidence=bool(any_flag('sl_inside_liquidity_sweep_zone','sl_in_liquidity_pool','liquidity_near_sl','stop_hunt_zone_sl')))

    # 3) TP / проблема цели
    add(5.6, 'tp1_behind_local_sr',
        primary=f'TP1 behind local {opp_zone_word}',
        scenario=f'Путь к TP1 проходил через локальную {opp_zone_word} зону. Цена не смогла пройти эту реакционную область сразу, поэтому TP1 не был поставлен под угрозу и сделка ушла в SL.',
        analysis_add=[f'TP1 был за local {opp_zone_word}'],
        happened=['ближний уровень остановил continuation', 'TP1 не был нормально поставлен под угрозу', 'цена развернулась к SL'],
        visible=[f'между entry и TP1 была {bad_zone_desc}', 'TP1 стоял за реакционной зоной', pos_line],
        secondary=['TP1 behind local support/resistance', 'Blocked TP path', 'Reaction zone before TP1'],
        improve=['TP1 ставить перед сильной встречной зоной', 'пропускать вход, если до TP1 нет свободного пути'],
        evidence=bool(bad_zone_blocks or any_flag('tp1_behind_local_sr','tp1_behind_support_resistance','tp1_blocked_by_local_level')))
    add(5.4, 'tp1_inside_demand_supply_reaction',
        primary='TP1 inside demand/supply reaction area',
        scenario='TP1 оказался внутри зоны, где рынок уже давал реакцию. Вместо чистого достижения цели цена получила встречный ответ и пошла к SL.',
        analysis_add=['TP1 находился внутри reaction zone'],
        happened=['цена не прошла реакционную область до TP1', 'встречная сторона быстро появилась', 'SL был достигнут после rejection/bounce'],
        visible=['TP1 расположен внутри demand/supply реакции', 'цель не была в чистом пространстве', pos_line],
        secondary=['TP1 inside demand/supply reaction', 'Bad TP placement', 'Target inside reaction zone'],
        improve=['ставить TP1 до reaction zone', 'не считать RR хорошим, если TP внутри зоны реакции'],
        evidence=bool(any_flag('tp1_inside_reaction_zone','tp1_inside_demand_supply','tp1_in_demand','tp1_in_supply') or (tight_space and bad_zone_blocks)))
    add(5.0, 'tp1_requires_breaking_fresh_structure',
        primary='TP1 required breaking fresh structure',
        scenario='Чтобы дойти до TP1, цена должна была сразу сломать свежую встречную структуру. Без сильного displacement это маловероятно, поэтому движение остановилось раньше и SL был выбит.',
        analysis_add=['TP1 требовал пробоя свежей структуры'],
        happened=['fresh structure не была пробита', 'continuation не получил силы', 'цена вернулась к SL'],
        visible=['на пути к TP1 стояла свежая структура', 'для TP1 нужен был дополнительный BOS/impulse', pos_line],
        secondary=['TP1 requires breaking fresh structure', 'Fresh structure blocked target', 'Weak target path'],
        improve=['не ставить TP1 за свежей структурой без clear displacement', 'ждать пробоя структуры до входа или брать цель раньше'],
        evidence=bool(any_flag('tp1_requires_breaking_fresh_structure','tp1_requires_new_bos','fresh_structure_blocks_tp1')))
    add(4.8, 'tp1_after_liquidity_pocket_filled',
        primary='TP1 after liquidity pocket already filled',
        scenario='Ликвидность по направлению сделки уже была частично забрана до входа. TP1 стоял после уже заполненного liquidity pocket, поэтому цене не хватило магнита для continuation.',
        analysis_add=['liquidity pocket перед TP1 уже был заполнен'],
        happened=['после входа не осталось сильного liquidity magnet до TP1', 'движение быстро затухло', 'цена ушла к SL'],
        visible=['ликвидность по направлению уже была снята до entry', 'TP1 стоял после заполненной liquidity pocket', pos_line],
        secondary=['TP1 after liquidity pocket filled', 'No liquidity magnet to TP1', 'Target after filled pocket'],
        improve=['проверять, осталась ли ликвидность до TP1', 'не брать target, если liquidity pocket уже заполнен'],
        evidence=bool(any_flag('tp1_after_liquidity_pocket_filled','liquidity_pocket_filled_before_tp1','tp1_after_filled_liquidity')))

    # 4) CONFIRM / подтверждение
    add(4.9, 'confirm_long_opposite_wick',
        primary='Confirm candle had long opposite wick',
        scenario='Confirm candle выглядел формально валидно, но имел длинный фитиль против направления сделки. Это показывало встречную реакцию ещё до входа, поэтому continuation быстро провалился.',
        analysis_add=['confirm candle имел длинный opposite wick'],
        happened=['после входа wick-pressure продолжился', 'acceptance не подтвердился', 'SL был достигнут после реакции против позиции'],
        visible=['confirm candle закрылась неуверенно', 'длинный фитиль показал rejection/absorption', pos_line],
        secondary=['Confirm candle has long opposite wick', 'Weak close quality', 'Rejection on confirm'],
        improve=['не принимать confirm candle с большим opposite wick', 'требовать clean body close в сторону сделки'],
        evidence=bool(any_flag('confirm_opposite_wick','confirm_long_opposite_wick','confirm_wick_against_trade')))
    add(5.0, 'confirm_closed_back_inside_zone',
        primary='Confirm candle closed back inside zone',
        scenario='Confirm candle закрылась обратно внутри retest/FVG/OB зоны. Это не acceptance, а слабое подтверждение; после входа цена не продолжила движение и ушла к SL.',
        analysis_add=['confirm закрылась обратно внутри зоны'],
        happened=['acceptance за зоной не было', 'цена осталась внутри retest area', 'SL был достигнут после провала follow-through'],
        visible=['закрытие confirm candle не вышло чисто за зону', 'entry был без уверенного acceptance', pos_line],
        secondary=['Confirm closed back inside zone', 'No acceptance after retest', 'Weak confirm close'],
        improve=['требовать close за зоной/уровнем', 'не входить, если confirm candle закрывается обратно внутри зоны'],
        evidence=bool(any_flag('confirm_closed_back_inside_zone','confirm_close_inside_zone','confirm_back_inside_retest_zone')))
    add(5.1, 'entry_triggered_without_continuation',
        primary='Entry triggered without continuation',
        scenario='Trigger сработал, но сразу после него не появилось продолжение по направлению сделки. Без post-trigger displacement цена вернулась против entry и дошла до SL.',
        analysis_add=['entry trigger был без continuation'],
        happened=['trigger сработал формально', 'следующие свечи не подтвердили направление', 'цена вернулась к SL'],
        visible=['после trigger нет clean continuation candle', 'TP1 не был нормально поставлен под угрозу', pos_line],
        secondary=['Entry triggered without continuation', 'No post-trigger expansion', 'Follow-through failed'],
        improve=['после trigger требовать immediate continuation', 'отменять/не брать сигнал при слабой post-trigger реакции'],
        evidence=bool(any_flag('entry_triggered_without_continuation','trigger_without_continuation') or (weak_follow and no_tp and not fast)))

    # 5) STRUCTURE / структура
    add(5.3, 'no_real_bos',
        primary='No real BOS before entry',
        scenario='Вход был построен как continuation, но реального BOS/структурного слома до входа не было. Без подтверждённой структуры рынок быстро вернулся против позиции.',
        analysis_add=['реальный BOS перед входом не подтверждён'],
        happened=['структура не дала подтверждение направления', 'retest оказался слабым', 'SL был достигнут после возврата в range'],
        visible=['нет чистого BOS перед входом', 'уровень не был уверенно сломан и принят рынком', pos_line],
        secondary=['No real BOS', 'Structure not confirmed', 'Weak continuation base'],
        improve=['требовать реальный BOS с close и retest', 'не входить по формальному пробою без структуры'],
        evidence=bool(any_flag('no_real_bos','bos_missing','fake_bos') or any_text('no real bos')))
    add(5.1, 'bos_was_weak',
        primary='BOS was weak / low-quality break',
        scenario='BOS был слабым: пробой не дал сильного close/displacement и не закрепился. Поэтому retest не получил continuation и цена ушла к SL.',
        analysis_add=['BOS был слабый'],
        happened=['пробой не дал follow-through', 'retest не подтвердился', 'SL был достигнут после возврата обратно'],
        visible=['BOS без сильного тела свечи', 'после BOS нет clean expansion', pos_line],
        secondary=['BOS was weak', 'Weak break quality', 'No displacement after BOS'],
        improve=['фильтровать weak BOS', 'требовать displacement и acceptance после BOS'],
        evidence=bool(any_flag('weak_bos','bos_was_weak','low_quality_bos')))
    add(5.2, 'choch_failed',
        primary='CHoCH failed / structure shift was not accepted',
        scenario='CHoCH был формальным, но рынок не принял смену структуры. Цена вернулась обратно в старую структуру и выбила SL.',
        analysis_add=['CHoCH не подтвердился acceptance'],
        happened=['после CHoCH не было continuation', 'цена вернулась против нового направления', 'SL был достигнут после failed shift'],
        visible=['CHoCH не получил закрепления', 'structure shift был быстро отменён', pos_line],
        secondary=['CHoCH failed', 'Failed structure shift', 'No acceptance after CHoCH'],
        improve=['после CHoCH ждать retest + continuation', 'не входить только на первом shift без acceptance'],
        evidence=bool(any_flag('choch_failed','failed_choch','choch_not_accepted')))
    add(5.0, 'lower_high_higher_low_not_confirmed',
        primary='Lower high / higher low not confirmed',
        scenario='Ожидаемая continuation-структура не подтвердила новый LH/HL. Без подтверждённого swing рынок потерял направление и пошёл к SL.',
        analysis_add=['LH/HL confirmation отсутствовал'],
        happened=['структурный swing не подтвердился', 'цена не продолжила направление сделки', 'SL был достигнут после возврата'],
        visible=['нет подтверждённого lower high / higher low', 'структура осталась неопределённой', pos_line],
        secondary=['Lower high / higher low not confirmed', 'Swing not confirmed', 'Structure uncertain'],
        improve=['ждать подтверждение LH для SHORT или HL для LONG', 'не входить до подтверждённого swing'],
        evidence=bool(any_flag('lh_hl_not_confirmed','lower_high_not_confirmed','higher_low_not_confirmed')))
    add(5.4, 'htf_structure_against_trade',
        primary=f'{side_word} against HTF structure',
        scenario='Младший сигнал был против 15m/30m/HTF структуры. Без поддержки старшего таймфрейма движение не получило continuation, и цена вернулась к SL.',
        analysis_add=['HTF structure была против сделки'],
        happened=['LTF setup не получил поддержку HTF', 'старшая структура потянула цену против входа', 'SL был достигнут после HTF pressure'],
        visible=['15m/30m context не подтверждал сделку', 'сделка была против старшей структуры', pos_line],
        secondary=['HTF structure against trade', 'LTF signal against HTF', 'No HTF confirmation'],
        improve=['фильтровать сделки против явной HTF структуры', 'требовать neutral/aligned HTF context'],
        evidence=bool(any_flag('htf_structure_against_trade','ltf_against_htf','against_htf_structure') or (strong_bad_context and contains_any(structure_text, ('15m', '30m', 'htf')) and no_tp)))

    # 5.5) Extended multi-setup reason catalog.
    # Every detector is evidence-gated. This keeps the card scalable for many
    # setups without falling back to one repeated template.
    add(4.9, 'entry_chased_breakout_candle',
        primary=f'{side_word} chased the breakout candle',
        scenario='Entry был сделан после уже расширенной breakout-свечи. Вместо свежего продолжения рынок дал откат к зоне пробоя, и позиция ушла к SL.',
        analysis_add=['entry был после расширенной breakout candle'],
        happened=['после breakout не появилось второй continuation-свечи', 'цена вернулась к breakout/retest зоне', 'SL был достигнут после отката'],
        visible=['вход был после уже пройденного impulse', 'следующая свеча не продолжила движение', pos_line],
        secondary=['Chased breakout candle', 'Late trigger', 'No second expansion'],
        improve=['не входить после уже растянутой свечи', 'ждать retest и новый close в сторону сделки'],
        evidence=bool(any_flag('entry_chased_breakout_candle','chased_breakout','late_breakout_chase') or any_text('chased breakout')))
    add(4.8, 'entry_on_wick_not_close',
        primary='Entry triggered on wick, not candle close',
        scenario='Trigger был получен по фитилю, но candle close не подтвердил acceptance. После этого цена вернулась против сделки и дошла до SL.',
        analysis_add=['entry был по wick-trigger без close confirmation'],
        happened=['wick дал ложное срабатывание', 'закрытие свечи не подтвердило направление', 'SL был достигнут после возврата'],
        visible=['сигнал появился на фитиле', 'close не закрепился за уровнем/зоной', pos_line],
        secondary=['Wick trigger', 'No close confirmation', 'Weak acceptance'],
        improve=['требовать candle close, а не wick-touch', 'не входить по одному проколу зоны'],
        evidence=bool(any_flag('entry_on_wick_not_close','wick_trigger_only','trigger_on_wick_only')))
    add(4.9, 'entry_before_retest_completed',
        primary='Entry before retest completed',
        scenario='Retest ещё не завершился: цена не показала acceptance и новый impulse после возврата в зону. Вход оказался ранним и был выбит SL.',
        analysis_add=['retest не был завершён перед entry'],
        happened=['цена продолжила ретест глубже после входа', 'confirmation не закрепился', 'SL был достигнут раньше continuation'],
        visible=['entry был внутри незавершённого retest', 'нет close/acceptance после retest', pos_line],
        secondary=['Premature retest entry', 'Retest not completed', 'No acceptance'],
        improve=['ждать завершения retest и close в сторону сделки', 'не входить в середине возврата к зоне'],
        evidence=bool(any_flag('entry_before_retest_completed','premature_retest_entry','retest_not_completed')))
    add(4.8, 'entry_between_zones_no_edge',
        primary='Entry between zones / no clear edge',
        scenario='Entry был между двумя зонами без сильного edge. Рынок не получил чистого направления, поэтому цена быстро вернулась к SL.',
        analysis_add=['entry был между demand/supply зонами'],
        happened=['ни buyer, ни seller edge не подтвердился', 'цена осталась в reaction area', 'SL был достигнут после chop/reversal'],
        visible=['entry расположен между ближайшими зонами', 'нет чистой зоны-источника для входа', pos_line],
        secondary=['No clear edge', 'Between zones', 'Poor location'],
        improve=['брать вход только от понятной origin-zone', 'пропускать середину между зонами'],
        evidence=bool(any_flag('entry_between_zones','entry_between_zones_no_edge','no_clear_edge')))
    add(4.7, 'entry_after_liquidity_already_taken',
        primary='Entry after liquidity was already taken',
        scenario='Ликвидность по направлению сделки уже была снята до входа. После этого не осталось сильного magnet к TP1, и движение развернулось к SL.',
        analysis_add=['liquidity по направлению уже была taken до entry'],
        happened=['цель ликвидности уже была отработана', 'continuation не получил топлива', 'SL был достигнут после возврата'],
        visible=['перед entry уже был sweep/liquidity grab по направлению', 'после sweep нет нового displacement', pos_line],
        secondary=['Liquidity already taken', 'No liquidity magnet', 'Late after sweep'],
        improve=['не входить после уже снятой liquidity без нового setup', 'проверять, осталась ли цель ликвидности до TP1'],
        evidence=bool(any_flag('liquidity_already_taken','entry_after_liquidity_taken','liquidity_taken_before_entry')))
    add(4.7, 'entry_against_session_high_low',
        primary=f'{side_word} into session high/low reaction',
        scenario='Entry был открыт прямо у session high/low reaction. Эта зона дала обратную реакцию раньше continuation, поэтому сделка ушла к SL.',
        analysis_add=['session high/low reaction была рядом с entry'],
        happened=['session level удержал цену', 'реакция против входа появилась раньше continuation', 'SL был достигнут после возврата'],
        visible=['рядом с entry был high/low текущей сессии', 'уровень дал реакцию против сделки', pos_line],
        secondary=['Session high/low reaction', 'Poor session location', 'Level rejection'],
        improve=['не входить прямо в session high/low', 'ждать acceptance за session level'],
        evidence=bool(any_flag('session_high_low_reaction','entry_against_session_high_low','session_level_rejected')))
    add(4.8, 'sl_inside_atr_noise',
        primary='SL inside ATR/noise range',
        scenario='SL стоял внутри обычного ATR/шумового диапазона инструмента. Цена сделала нормальный шумовой откат и выбила SL без настоящей structural invalidation.',
        analysis_add=['SL находился внутри ATR/noise range'],
        happened=['обычный шум рынка оказался больше SL', 'структурная invalidation не была подтверждена', 'SL был достигнут на noise move'],
        visible=['SL слишком близко к entry относительно волатильности', 'цена не сломала структуру, а сделала normal noise', pos_line],
        secondary=['SL inside ATR noise', 'Stop too close to volatility', 'Noise hit SL'],
        improve=['учитывать ATR при SL', 'ставить SL за structure, а не за минимальный шум'],
        evidence=bool(any_flag('sl_inside_atr_noise','sl_below_atr_noise','atr_noise_hit_sl')))
    add(4.7, 'sl_at_liquidity_level',
        primary='SL placed at obvious liquidity level',
        scenario='SL был поставлен на очевидный liquidity level. Цена сняла этот уровень стопов и только потом могла продолжить структуру.',
        analysis_add=['SL стоял на очевидном liquidity level'],
        happened=['рынок снял близкую ликвидность', 'SL был достигнут на stop run', 'после снятия liquidity direction мог измениться'],
        visible=['SL совпадал с obvious high/low/equal level', 'уровень был видимым для stop hunt', pos_line],
        secondary=['SL at liquidity level', 'Stop hunt risk', 'Obvious stop placement'],
        improve=['не ставить SL прямо на obvious equal high/low', 'добавлять buffer за liquidity zone'],
        evidence=bool(any_flag('sl_at_liquidity_level','sl_on_equal_high_low','obvious_sl_liquidity')))
    add(4.7, 'sl_inside_fvg_body',
        primary='SL inside FVG/OB body',
        scenario='SL стоял внутри самой FVG/OB зоны, а не за её invalidation. Нормальный тест зоны выбил SL до подтверждения continuation.',
        analysis_add=['SL находился внутри FVG/OB body'],
        happened=['цена протестировала глубину зоны', 'SL был внутри зоны retest', 'continuation не успел подтвердиться'],
        visible=['SL не был за краем FVG/OB', 'обычный retest зоны дошёл до SL', pos_line],
        secondary=['SL inside FVG/OB', 'Stop before zone invalidation', 'Retest depth hit SL'],
        improve=['ставить SL за дальним краем FVG/OB + buffer', 'не считать midpoint зоны invalidation'],
        evidence=bool(any_flag('sl_inside_fvg_body','sl_inside_ob_body','sl_inside_zone_body')))
    add(4.8, 'sl_before_last_swing',
        primary='SL before real swing invalidation',
        scenario='SL был поставлен до последнего swing invalidation. Цена сделала нормальный возврат к swing-зоне и выбила SL раньше, чем setup реально сломался.',
        analysis_add=['SL был перед real swing invalidation'],
        happened=['цена вернулась к последнему swing', 'SL был достигнут до реального structural break', 'setup не получил время на continuation'],
        visible=['invalidation swing находился дальше фактического SL', 'SL стоял внутри структуры', pos_line],
        secondary=['SL before swing invalidation', 'Structural stop too tight', 'Pullback hit SL'],
        improve=['ставить SL за last swing invalidation', 'если RR ломается после структурного SL — не брать сделку'],
        evidence=bool(any_flag('sl_before_last_swing','sl_before_swing_invalidation','sl_before_structural_swing')))
    add(4.7, 'tp1_at_equal_high_low_liquidity',
        primary='TP1 at equal high/low liquidity reaction',
        scenario='TP1 стоял за/в районе equal high/low liquidity. Перед достижением цели рынок получил reaction и вернулся к SL.',
        analysis_add=['TP1 был у equal high/low liquidity'],
        happened=['цена не смогла пройти equal level cleanly', 'liquidity reaction остановила движение', 'SL был достигнут после возврата'],
        visible=['TP1 расположен за obvious equal high/low', 'путь к цели проходил через liquidity reaction', pos_line],
        secondary=['TP1 at equal high/low', 'Liquidity reaction blocked TP1', 'Poor target path'],
        improve=['ставить TP1 до equal high/low reaction', 'не прятать TP1 за obvious liquidity без displacement'],
        evidence=bool(any_flag('tp1_at_equal_high_low','tp1_behind_equal_high_low','equal_level_blocks_tp1')))
    add(4.7, 'tp1_at_vwap_reaction',
        primary='TP1 blocked by VWAP/mean reaction',
        scenario='Путь к TP1 упирался в VWAP/mean reaction. Цена не смогла пройти среднюю/динамическую зону и вернулась против позиции.',
        analysis_add=['TP1 был за VWAP/mean reaction'],
        happened=['VWAP/mean zone остановила continuation', 'цена не закрепилась за динамическим уровнем', 'SL был достигнут после возврата'],
        visible=['между entry и TP1 была VWAP/mean reaction', 'цена не приняла уровень', pos_line],
        secondary=['VWAP blocked TP1', 'Mean reaction', 'Dynamic resistance/support'],
        improve=['не ставить TP1 за VWAP без acceptance', 'требовать close за VWAP перед входом'],
        evidence=bool(any_flag('tp1_blocked_by_vwap','vwap_blocks_tp1','tp1_at_vwap_reaction')))
    add(4.8, 'tp1_beyond_htf_level',
        primary='TP1 beyond HTF level without acceptance',
        scenario='TP1 находился за HTF уровнем. Без acceptance над/под этим уровнем сделка не имела чистого пути до цели и вернулась к SL.',
        analysis_add=['TP1 был за HTF level без acceptance'],
        happened=['HTF level остановил движение', 'acceptance за уровнем не появился', 'SL был достигнут после rejection/reclaim'],
        visible=['на пути к TP1 был 15m/30m/HTF level', 'цена не закрепилась за HTF level', pos_line],
        secondary=['TP1 beyond HTF level', 'HTF level blocked path', 'No HTF acceptance'],
        improve=['ставить TP1 до HTF level', 'требовать HTF acceptance перед целью за уровнем'],
        evidence=bool(any_flag('tp1_beyond_htf_level','htf_level_blocks_tp1','tp1_behind_htf_level')))
    add(4.9, 'confirm_low_volume',
        primary='Confirm candle had weak volume',
        scenario='Confirm candle был без достаточного объёма. Формальный trigger появился, но реального participation не было, поэтому continuation не развился.',
        analysis_add=['confirm candle был на слабом volume'],
        happened=['volume не подтвердил направление', 'следующие свечи не дали expansion', 'SL был достигнут после слабого follow-through'],
        visible=['confirm candle без volume expansion', 'нет участия рынка после trigger', pos_line],
        secondary=['Low-volume confirm', 'Weak participation', 'No volume follow-through'],
        improve=['требовать volume expansion на confirm', 'не брать retest confirm без participation'],
        evidence=bool(any_flag('confirm_low_volume','weak_volume_confirm','no_volume_on_confirm')))
    add(4.8, 'confirm_small_body',
        primary='Confirm candle body too small',
        scenario='Confirm candle имел маленькое тело и не показал dominance стороны сделки. После такого слабого close continuation не подтвердился.',
        analysis_add=['confirm candle имел small body'],
        happened=['body close был слабым', 'acceptance не подтвердился', 'SL был достигнут после обратной реакции'],
        visible=['confirm candle закрылась маленьким телом', 'нет сильного close в сторону сделки', pos_line],
        secondary=['Small confirm body', 'Weak close', 'No dominance'],
        improve=['требовать body close больше минимального threshold', 'не принимать doji/small body как confirm'],
        evidence=bool(any_flag('confirm_small_body','weak_body_confirm','doji_confirm')))
    add(4.8, 'confirm_against_vwap',
        primary='Confirm was against VWAP side',
        scenario='Confirm появился по слабую сторону VWAP/mean. Без закрепления за динамическим уровнем entry не получил continuation и закрылся по SL.',
        analysis_add=['confirm был по слабую сторону VWAP'],
        happened=['цена не приняла VWAP side', 'VWAP/mean вернул цену против entry', 'SL был достигнут после rejection/reclaim'],
        visible=['entry/confirm был по неправильную сторону VWAP', 'нет close за VWAP в сторону сделки', pos_line],
        secondary=['Wrong VWAP side', 'No VWAP acceptance', 'Mean rejection'],
        improve=['для LONG требовать close выше VWAP, для SHORT ниже VWAP', 'не входить против VWAP без сильного displacement'],
        evidence=bool(any_flag('confirm_against_vwap','wrong_vwap_side','vwap_side_weak')))
    add(4.9, 'no_two_candle_acceptance',
        primary='No 1–2 candle acceptance after retest',
        scenario='После retest не появилось 1–2 свечей acceptance в сторону сделки. Без этого setup остался только touch/trigger и был выбит SL.',
        analysis_add=['1–2 candle acceptance после retest отсутствовал'],
        happened=['первые свечи после входа не удержали направление', 'цена вернулась в entry zone', 'SL был достигнут после слабого retest'],
        visible=['нет 1–2 close в сторону сделки после retest', 'entry zone не была принята рынком', pos_line],
        secondary=['No candle acceptance', 'Retest not accepted', 'Weak post-entry hold'],
        improve=['требовать 1–2 close после retest', 'отменять вход, если первая свеча сразу возвращается в зону'],
        evidence=bool(any_flag('no_two_candle_acceptance','no_1_2_close_acceptance','no_candle_acceptance')))
    add(4.9, 'internal_structure_broke_against',
        primary='Internal structure broke against trade',
        scenario='После входа внутренняя структура сломалась против сделки. Это отменило continuation-идею раньше TP1 и привело к SL.',
        analysis_add=['internal structure сломалась против сделки'],
        happened=['micro BOS/CHoCH пошёл против entry', 'continuation invalidated', 'SL был достигнут после structure shift'],
        visible=['внутри entry-zone появился structure shift против позиции', 'нет нового swing в сторону сделки', pos_line],
        secondary=['Internal structure broke against', 'Micro CHoCH against trade', 'Continuation invalidated'],
        improve=['после entry отслеживать micro BOS против сделки', 'не держать setup после internal break against'],
        evidence=bool(any_flag('internal_structure_broke_against','micro_choch_against','micro_bos_against')))
    add(4.8, 'equal_high_low_magnet_against',
        primary='Equal high/low magnet pulled price against trade',
        scenario='Рядом была очевидная equal high/low ликвидность против позиции. Рынок сначала потянулся к ней, и SL был достигнут до continuation.',
        analysis_add=['equal high/low liquidity была против entry'],
        happened=['цена пошла к ближайшей ликвидности против позиции', 'SL попал в этот liquidity move', 'TP1 не был взят'],
        visible=['рядом с SL была obvious equal liquidity', 'рынок имел magnet против сделки', pos_line],
        secondary=['Opposite liquidity magnet', 'Equal highs/lows near SL', 'Liquidity draw against entry'],
        improve=['проверять liquidity draw до входа', 'не ставить SL перед очевидной равной ликвидностью'],
        evidence=bool(any_flag('equal_high_low_magnet_against','opposite_liquidity_magnet','liquidity_draw_against')))
    add(4.7, 'range_high_low_sweep_reversal',
        primary='Range high/low sweep reversal',
        scenario='Entry попал в sweep границы range. После снятия ликвидности цена вернулась обратно в диапазон и дошла до SL.',
        analysis_add=['был sweep range high/low перед reversal'],
        happened=['граница range была снята', 'цена вернулась обратно в range', 'SL был достигнут после sweep reversal'],
        visible=['виден sweep high/low диапазона', 'закрепления за границей range не было', pos_line],
        secondary=['Range sweep reversal', 'Failed range breakout', 'Liquidity sweep trap'],
        improve=['после sweep ждать reclaim/acceptance', 'не входить на первом проколе range high/low'],
        evidence=bool(any_flag('range_high_low_sweep_reversal','range_sweep_reversal','failed_range_sweep')))
    add(4.8, 'absorption_against_entry',
        primary='Absorption against entry',
        scenario='В зоне входа появилась absorption против позиции: свечи не проходили дальше, встречная сторона поглощала market orders, после чего цена пошла к SL.',
        analysis_add=['absorption против entry была заметна'],
        happened=['движение в сторону сделки поглощалось', 'continuation не развился', 'SL был достигнут после absorption/reversal'],
        visible=['много фитилей/малые тела в зоне входа', 'нет расширения после попытки continuation', pos_line],
        secondary=['Absorption against entry', 'Orderflow absorbed', 'No clean expansion'],
        improve=['не входить при absorption против направления', 'требовать clean break после absorption zone'],
        evidence=bool(any_flag('absorption_against_entry','seller_absorption_for_long','buyer_absorption_for_short')))
    add(4.7, 'volatility_compression_no_breakout',
        primary='Volatility compression without breakout',
        scenario='После входа была compression, но breakout в сторону сделки не появился. Цена сжалась, потеряла momentum и вышла к SL.',
        analysis_add=['volatility compression не дала breakout'],
        happened=['свечи сжались после entry', 'directional breakout не появился', 'SL был достигнут после выхода против позиции'],
        visible=['после входа narrow candles / compression', 'нет expansion в сторону сделки', pos_line],
        secondary=['Volatility compression', 'No breakout from compression', 'Momentum stalled'],
        improve=['после compression ждать breakout-close', 'не держать entry без expansion из сжатия'],
        evidence=bool(any_flag('volatility_compression_no_breakout','compression_no_breakout','narrow_range_after_entry')))
    add(4.7, 'divergence_against_trade',
        primary=f'Divergence against {side_word}',
        scenario='Перед/после entry появился divergence против направления сделки. Momentum не подтвердил price move, поэтому continuation провалился.',
        analysis_add=['divergence был против сделки'],
        happened=['price move не подтвердился momentum', 'reversal pressure усилился', 'SL был достигнут после divergence reaction'],
        visible=['видна divergence / momentum mismatch', 'цена не получила подтверждение индикаторов/импульса', pos_line],
        secondary=['Divergence against trade', 'Momentum mismatch', 'Weak continuation'],
        improve=['не брать continuation против явной divergence', 'требовать снятие divergence новым impulse'],
        evidence=bool(any_flag('divergence_against_trade','bearish_divergence_for_long','bullish_divergence_for_short')))
    add(4.6, 'low_liquidity_session_chop',
        primary='Low-liquidity session chop',
        scenario='Сигнал попал в низколиквидный chop. Без достаточного участия рынка свечи дали шум против входа и выбили SL.',
        analysis_add=['low-liquidity/chop session context'],
        happened=['рынок двигался шумно без clean flow', 'entry не получил participation', 'SL был достигнут в chop'],
        visible=['свечи мелкие/рваные, много шума', 'volume/participation слабые', pos_line],
        secondary=['Low liquidity', 'Session chop', 'No participation'],
        improve=['ужесточать фильтр сессии/volume', 'пропускать low-liquidity chop'],
        evidence=bool(any_flag('low_liquidity_session','asia_session_chop','low_liquidity_chop')))
    add(4.9, 'news_spike_volatility',
        primary='News/spike volatility invalidated setup',
        scenario='Цена попала в spike-volatility режим. В таком движении normal retest/SL логика часто ломается, и сделка была выбита до стабильного continuation.',
        analysis_add=['была news/spike volatility'],
        happened=['волатильность резко выросла', 'обычный SL/retest был сломан', 'SL был достигнут на spike move'],
        visible=['резкие свечи и volume spike вокруг entry', 'движение стало нестабильным', pos_line],
        secondary=['Spike volatility', 'News-like move', 'Unstable execution context'],
        improve=['не входить во время spike-volatility', 'после spike ждать стабилизации и новый setup'],
        evidence=bool(any_flag('news_spike_volatility','spike_volatility','abnormal_volatility')))
    add(4.8, 'post_pump_cooling_phase',
        primary='Post-impulse cooling phase',
        scenario='После сильного impulse рынок перешёл в cooling phase. Entry ожидал continuation, но цена начала переваривать движение и ушла к SL.',
        analysis_add=['рынок был в cooling phase после impulse'],
        happened=['после импульса momentum начал остывать', 'новый impulse не появился', 'SL был достигнут после pullback/chop'],
        visible=['после сильного движения началась консолидация/откат', 'нет fresh expansion после entry', pos_line],
        secondary=['Post-impulse cooling', 'Momentum cooling', 'No fresh continuation'],
        improve=['после сильного impulse ждать pullback и новый trigger', 'не входить в cooling phase без expansion'],
        evidence=bool(any_flag('post_pump_cooling_phase','post_dump_cooling_phase','momentum_cooling') or (late_after_extended and weak_follow and no_tp and not (near_bad_zone and bad_zone_blocks))))
    add(4.8, 'opposite_fvg_stack_near_entry',
        primary='Opposite FVG stack near entry',
        scenario='Возле entry была stacked FVG зона противоположной стороны. Она создала сильную reaction area, и цена не смогла продолжить движение к TP1.',
        analysis_add=['opposite stacked FVG был рядом с entry'],
        happened=['stacked FVG остановил движение', 'continuation не прошёл через reaction area', 'SL был достигнут после возврата'],
        visible=['рядом с entry несколько FVG противоположной стороны', 'TP path проходил через stacked imbalance', pos_line],
        secondary=['Opposite FVG stack', 'Stacked imbalance blocked path', 'Strong reaction area'],
        improve=['не входить против stacked FVG', 'ждать mitigation/acceptance за FVG stack'],
        evidence=bool(any_flag('opposite_fvg_stack_near_entry','stacked_opposite_fvg','opposite_stacked_fvg')))
    add(4.7, 'zone_overlap_opposing_ob',
        primary='Entry zone overlapped opposing OB/FVG',
        scenario='Зона входа пересекалась со встречной OB/FVG. Из-за конфликта зон реакция была слабой, и цена ушла к SL.',
        analysis_add=['entry zone overlapped opposing OB/FVG'],
        happened=['зоны давали конфликтующий сигнал', 'reaction от entry-zone была слабой', 'SL был достигнут после opposing reaction'],
        visible=['entry zone находится внутри/рядом со встречной зоной', 'нет чистого zone-to-zone пространства', pos_line],
        secondary=['Overlapping opposing zone', 'Confluence conflict', 'Weak zone edge'],
        improve=['не брать зоны, которые overlap со встречным OB/FVG', 'требовать чистый gap между зонами'],
        evidence=bool(any_flag('zone_overlap_opposing_ob','entry_zone_overlaps_opposing_zone','opposing_ob_overlap')))
    add(4.7, 'zone_midpoint_lost',
        primary='Zone midpoint was lost after entry',
        scenario='После entry цена потеряла midpoint входной зоны. Это показало слабость реакции и отменило continuation до TP1.',
        analysis_add=['midpoint зоны был потерян после entry'],
        happened=['цена не удержала середину зоны', 'zone reaction ослабла', 'SL был достигнут после потери midpoint'],
        visible=['price action вернулся за midpoint FVG/OB', 'zone hold не подтвердился', pos_line],
        secondary=['Zone midpoint lost', 'Weak zone hold', 'No acceptance'],
        improve=['отменять setup при потере midpoint зоны', 'не ждать TP1 после слабого zone hold'],
        evidence=bool(any_flag('zone_midpoint_lost','fvg_midpoint_lost','ob_midpoint_lost')))
    add(4.6, 'correlation_against_signal',
        primary='Correlation/sector moved against signal',
        scenario='Корреляционный рынок/сектор двигался против сделки. Даже если локальный setup выглядел валидно, широкий контекст не поддержал continuation.',
        analysis_add=['correlation/sector context был против сделки'],
        happened=['сектор/корреляция потянули цену против entry', 'локальный сигнал не получил поддержки', 'SL был достигнут после broad pressure'],
        visible=['BTC/market/sector context не подтверждал направление', 'локальный entry был против broader flow', pos_line],
        secondary=['Correlation against signal', 'Sector weakness/strength against entry', 'Broad market pressure'],
        improve=['фильтровать сигналы против BTC/sector flow', 'требовать alignment с market context'],
        evidence=bool(any_flag('correlation_against_signal','sector_against_signal','btc_correlation_against')))

    # 6) MOMENTUM / импульс
    add(5.2, 'counter_impulse_fast',
        primary='Counter impulse appeared fast after entry',
        scenario='После входа быстро появился импульс против позиции. Это отменило идею continuation и показало, что встречная сторона сильнее в моменте.',
        analysis_add=['counter impulse появился быстро после entry'],
        happened=['первая сильная свеча пошла против входа', 'continuation не подтвердился', 'SL был достигнут на counter impulse'],
        visible=['быстрое движение против entry', 'свеча против позиции сильнее first push в сторону сделки', pos_line],
        secondary=['Counter impulse appeared fast', 'Immediate opposite pressure', 'Momentum against trade'],
        improve=['не держать setup, если первая сильная свеча после входа против позиции', 'требовать first impulse в сторону сделки'],
        evidence=bool(any_flag('counter_impulse_fast','fast_counter_impulse') or immediate_counter))
    add(5.0, 'volume_spike_exhaustion',
        primary='Volume spike exhaustion before entry',
        scenario='Перед входом был volume/momentum spike, который больше похож на exhaustion, чем на начало continuation. После spike движение затухло и цена пошла к SL.',
        analysis_add=['перед входом был volume spike exhaustion'],
        happened=['после spike не было нового expansion', 'momentum затух', 'SL был достигнут после обратной реакции'],
        visible=['большая импульсная свеча/volume spike перед входом', 'после неё нет продолжения', pos_line],
        secondary=['Volume spike exhaustion', 'Momentum spent', 'No follow-through after spike'],
        improve=['не входить сразу после exhaustion spike', 'ждать нормальный pullback и новый impulse'],
        evidence=bool(any_flag('volume_spike_exhaustion','exhaustion_volume_spike','climax_volume')))
    add(4.9, 'no_fresh_expansion_candle',
        primary='No fresh expansion candle after entry',
        scenario='После входа не появилась новая expansion-свеча в сторону сделки. Без fresh expansion setup остался слабым и цена дошла до SL.',
        analysis_add=['fresh expansion candle после entry отсутствовала'],
        happened=['свечи после входа были слабыми', 'directional momentum не появился', 'SL был достигнут после слабого follow-through'],
        visible=['нет новой большой свечи в сторону сделки после entry', 'тела свечей не расширяются по направлению', pos_line],
        secondary=['No fresh expansion candle', 'No post-entry expansion', 'Weak momentum'],
        improve=['требовать fresh expansion после confirm', 'не брать сделки без новой impulse candle'],
        evidence=bool(any_flag('no_fresh_expansion_candle','no_post_entry_expansion') or (weak_follow and no_tp)))

    # 7) FVG / OB качество зоны
    add(4.8, 'fvg_too_small',
        primary='FVG too small / weak imbalance',
        scenario='FVG была слишком маленькой или слабой, поэтому зона не дала качественную реакцию. Entry получил слабый continuation и ушёл к SL.',
        analysis_add=['FVG была маленькая/слабая'],
        happened=['FVG не дала сильный retest response', 'цена не продолжила направление', 'SL был достигнут после слабой реакции зоны'],
        visible=['FVG выглядит маленькой относительно шума свечей', 'imbalance недостаточно сильный', pos_line],
        secondary=['FVG too small', 'Weak imbalance', 'Low-quality FVG'],
        improve=['фильтровать слишком маленькие FVG', 'брать только FVG с нормальным displacement'],
        evidence=bool(any_flag('fvg_too_small','small_fvg','weak_fvg') or (ob_fvg_route and any_text('fvg too small'))))
    add(5.0, 'fvg_already_mitigated',
        primary='FVG already mitigated before entry',
        scenario='FVG уже была mitigated до входа. Повторный вход от такой зоны слабее, поэтому реакция не удержала цену и SL был достигнут.',
        analysis_add=['FVG уже была mitigated до entry'],
        happened=['зона уже отработала раньше', 'повторная реакция была слабой', 'цена ушла против сделки'],
        visible=['FVG была протестирована/закрыта до входа', 'entry был по уже использованной зоне', pos_line],
        secondary=['FVG already mitigated', 'Used imbalance', 'Weak repeat reaction'],
        improve=['не использовать уже mitigated FVG как свежую', 'снижать score повторно протестированных зон'],
        evidence=bool(any_flag('fvg_already_mitigated','fvg_mitigated_before_entry','used_fvg')))
    add(5.0, 'fvg_filled_before_entry',
        primary='FVG filled before entry',
        scenario='FVG была заполнена до входа, поэтому imbalance уже не давал сильного преимущества. После entry цена не получила continuation и дошла до SL.',
        analysis_add=['FVG была заполнена до entry'],
        happened=['imbalance уже был закрыт', 'зона не дала свежей реакции', 'SL был достигнут после слабого continuation'],
        visible=['FVG уже filled перед входом', 'нет свежего imbalance на момент entry', pos_line],
        secondary=['FVG filled before entry', 'No fresh imbalance', 'Filled FVG'],
        improve=['не входить по заполненной FVG', 'требовать fresh imbalance на момент entry'],
        evidence=bool(any_flag('fvg_filled_before_entry','fvg_filled','imbalance_filled_before_entry')))
    add(4.9, 'ob_already_tested',
        primary='OB already tested / not fresh',
        scenario='OB уже был протестирован до входа. Свежесть зоны потеряна, поэтому реакция оказалась слабой и цена дошла до SL.',
        analysis_add=['OB уже был tested до entry'],
        happened=['повторный retest OB не дал сильной реакции', 'цена не удержала зону', 'SL был достигнут'],
        visible=['OB уже имел предыдущий тест', 'entry был не от свежей зоны', pos_line],
        secondary=['OB already tested', 'Not fresh OB', 'Weak repeat OB reaction'],
        improve=['снижать score уже протестированных OB', 'предпочитать fresh/origin OB'],
        evidence=bool(any_flag('ob_already_tested','ob_tested_before_entry','used_ob')))
    add(4.9, 'ob_weak_not_origin',
        primary='OB weak / not true origin',
        scenario='OB не был настоящей origin-зоной displacement. Такая зона слабее удерживает цену, поэтому entry не получил continuation и SL сработал.',
        analysis_add=['OB был weak / not origin'],
        happened=['OB не дал сильной реакции', 'цена не продолжила направление', 'SL был достигнут после слабого zone hold'],
        visible=['OB не является origin сильного displacement', 'реакция от OB слабая', pos_line],
        secondary=['OB weak / not origin', 'Weak OB quality', 'No origin displacement'],
        improve=['использовать только OB у origin displacement', 'фильтровать слабые OB без импульса'],
        evidence=bool(any_flag('ob_weak_not_origin','weak_ob','ob_not_origin')))
    add(5.1, 'zone_too_close_to_opposing_zone',
        primary='Zone too close to opposing zone',
        scenario='Входная зона находилась слишком близко к встречной demand/supply зоне. Пространства для continuation было мало, поэтому цена быстро получила обратную реакцию и ушла к SL.',
        analysis_add=['entry zone была слишком близко к opposing zone'],
        happened=['встречная зона быстро остановила движение', 'TP1 не был нормально поставлен под угрозу', 'цена вернулась к SL'],
        visible=['между entry zone и opposing zone мало расстояния', 'clean space отсутствовал', pos_line],
        secondary=['Zone too close to opposing zone', 'No clean zone-to-zone space', 'Opposing zone nearby'],
        improve=['требовать расстояние между зоной входа и встречной зоной', 'не брать OB/FVG прямо перед opposing zone'],
        evidence=bool(any_flag('zone_too_close_to_opposing_zone','opposing_zone_too_close') or (ob_fvg_route and bad_zone_blocks and tight_space)))
    add(4.8, 'zone_inside_chop_range',
        primary='FVG/OB zone inside chop/range',
        scenario='Зона входа находилась внутри chop/range, а не в чистой импульсной структуре. Поэтому реакция была слабой, и цена пошла к SL.',
        analysis_add=['FVG/OB зона была внутри chop/range'],
        happened=['range не дал directional expansion', 'зона не удержала price action', 'SL был достигнут после возврата в range'],
        visible=['OB/FVG расположена внутри локального range', 'свечи вокруг зоны выглядят choppy', pos_line],
        secondary=['Zone inside chop/range', 'Low-quality zone context', 'Range zone failed'],
        improve=['не брать зоны внутри chop', 'ждать выход из range или зону у края структуры'],
        evidence=bool(any_flag('zone_inside_chop_range','zone_inside_range','fvg_ob_inside_chop') or (ob_fvg_route and range_trap)))

    # 8) LIQUIDITY
    add(5.0, 'entry_after_liquidity_already_taken',
        primary='Entry after liquidity already taken',
        scenario='Ликвидность по направлению сделки уже была снята до входа. После этого у цены стало меньше причины продолжать движение, поэтому momentum затух и SL был достигнут.',
        analysis_add=['entry был после уже снятой ликвидности'],
        happened=['после снятия ликвидности continuation не появился', 'цена дала обратную реакцию', 'SL был достигнут'],
        visible=['liquidity target уже был взят перед entry', 'после entry нет нового liquidity magnet', pos_line],
        secondary=['Entry after liquidity already taken', 'Liquidity already swept', 'No fresh liquidity magnet'],
        improve=['не входить после уже снятого liquidity target без нового setup', 'искать следующий liquidity target до TP1'],
        evidence=bool(any_flag('entry_after_liquidity_taken','liquidity_already_taken_before_entry','liquidity_already_swept')))
    add(5.1, 'equal_highs_lows_magnet_against_trade',
        primary='Equal highs/lows magnet was against trade',
        scenario='Рядом была ликвидность equal highs/lows против направления сделки. Рынок потянулся к этому магниту, поэтому позиция быстро ушла к SL.',
        analysis_add=['equal highs/lows liquidity magnet был против сделки'],
        happened=['цена потянулась к встречной ликвидности', 'continuation по сделке не подтвердился', 'SL был достигнут после liquidity draw'],
        visible=['рядом видны equal highs/lows против позиции', 'они стали более близким магнитом, чем TP1', pos_line],
        secondary=['Equal highs/lows magnet against trade', 'Opposite liquidity draw', 'Liquidity magnet to SL'],
        improve=['проверять equal highs/lows против позиции перед входом', 'не входить, если ближайший liquidity magnet находится за SL'],
        evidence=bool(any_flag('equal_highs_lows_magnet_against_trade','equal_highs_against_trade','equal_lows_against_trade')))
    add(5.3, 'stop_hunt_sweep_then_reclaim',
        primary='Stop hunt / sweep then reclaim against entry',
        scenario='Рынок сделал sweep/stop-hunt и затем reclaim против позиции. Это отменило continuation-сценарий и привело к SL.',
        analysis_add=['sweep then reclaim появился против entry'],
        happened=['снятие ликвидности не дало продолжения по сделке', 'после sweep цена вернулась против входа', 'SL был достигнут после reclaim'],
        visible=['виден sweep уровня и быстрый reclaim', 'reclaim был против направления сделки', pos_line],
        secondary=['Stop hunt / sweep then reclaim', 'Reclaim against entry', 'Liquidity trap'],
        improve=['после sweep ждать подтверждение направления', 'не входить, если reclaim быстро идёт против setup'],
        evidence=bool(any_flag('stop_hunt_sweep_then_reclaim','sweep_then_reclaim_against_entry','liquidity_trap') or (reclaim_route and any_flag('reclaim_lost_back','reclaim_not_confirmed'))))
    add(5.0, 'untaken_liquidity_opposite_side',
        primary='Untaken liquidity on opposite side pulled price to SL',
        scenario='С противоположной стороны оставалась не снятая ликвидность. Она стала сильнее как магнит, чем TP1, и цена пошла за ней против позиции.',
        analysis_add=['untaken opposite liquidity была рядом'],
        happened=['рынок потянулся к не снятой ликвидности против entry', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после liquidity draw'],
        visible=['за SL/против позиции была видимая ликвидность', 'по направлению TP1 магнит был слабее', pos_line],
        secondary=['Untaken liquidity on opposite side', 'Liquidity draw against entry', 'Opposite liquidity magnet'],
        improve=['не входить, если ближайшая не снятая ликвидность находится против сделки', 'сначала ждать sweep этой ликвидности или новый reclaim'],
        evidence=bool(any_flag('untaken_liquidity_opposite_side','opposite_liquidity_untaken','liquidity_draw_against_trade')))
    add(5.0, 'internal_liquidity_pulled_against_entry',
        primary='Internal liquidity pulled price against entry',
        scenario='Внутри локального range оставалась внутренняя ликвидность против entry. Цена сначала пошла за этой ликвидностью и выбила SL.',
        analysis_add=['internal liquidity тянула цену против entry'],
        happened=['цена пошла за внутренней ликвидностью', 'continuation по сделке не подтвердился', 'SL был достигнут до TP1'],
        visible=['внутри range была ближняя liquidity pocket против позиции', 'entry был до её снятия', pos_line],
        secondary=['Internal liquidity pulled price against entry', 'Internal liquidity draw', 'Range liquidity against trade'],
        improve=['перед входом проверять internal liquidity внутри range', 'не входить, пока ближняя liquidity против позиции не снята'],
        evidence=bool(any_flag('internal_liquidity_pulled_against_entry','internal_liquidity_against_trade','range_liquidity_against_entry')))

    # 9) TIME / скорость сделки
    add(4.9, 'trade_stayed_too_long_without_progress',
        primary='Trade stayed too long without progress',
        scenario='После входа сделка долго не показывала прогресс в сторону TP1. Чем дольше нет continuation, тем выше риск возврата к SL — именно это и произошло.',
        analysis_add=['сделка долго стояла без прогресса'],
        happened=['TP1 не был поставлен под угрозу в нормальное время', 'momentum постепенно затух', 'цена вернулась к SL'],
        visible=['несколько свечей после entry не дали продвижения к TP1', 'рынок начал сжиматься/возвращаться против позиции', pos_line],
        secondary=['Trade stayed too long without progress', 'No time-based follow-through', 'Stalled trade'],
        improve=['добавить time-stop/soft exit, если нет прогресса', 'не держать setup после нескольких слабых свечей'],
        evidence=bool(any_flag('trade_stayed_too_long_without_progress','time_stop_needed','no_progress_too_long') or ((duration_min or 0) >= 45 and no_tp and weak_follow)))
    add(5.2, 'immediate_rejection_after_entry',
        primary='Immediate rejection after entry',
        scenario='Сразу после входа появилась реакция против позиции. Это показало, что entry попал в зону встречной силы, и цена быстро дошла до SL.',
        analysis_add=['immediate rejection появился после entry'],
        happened=['первая реакция после входа была против сделки', 'continuation не подтвердился', 'SL был достигнут после rejection/bounce'],
        visible=['свечи сразу после entry показывают rejection', 'первое сильное движение было против позиции', pos_line],
        secondary=['Immediate rejection after entry', 'Fast opposite reaction', 'Entry into reaction zone'],
        improve=['не входить, если первая свеча после trigger сразу rejected', 'ждать нового подтверждения после rejection'],
        evidence=bool(any_flag('immediate_rejection_after_entry','immediate_rejection') or (fast and immediate_counter)))

    # 10) MULTI-TF
    add(5.4, 'htf_demand_supply_blocked_trade',
        primary=f'HTF {opp_zone_word} blocked the trade',
        scenario=f'На старшем таймфрейме рядом была встречная зона {opp_zone_word}. LTF вход упёрся в HTF блок, поэтому continuation не прошёл и SL был достигнут.',
        analysis_add=[f'HTF {opp_zone_word} блокировал путь сделки'],
        happened=['LTF setup упёрся в HTF зону', 'цена дала реакцию от старшего уровня', 'TP1 не был нормально поставлен под угрозу'],
        visible=[f'рядом была HTF {bad_zone_desc}', 'LTF entry был слишком близко к старшей встречной зоне', pos_line],
        secondary=['HTF demand/supply blocked trade', 'HTF opposing zone nearby', 'LTF into HTF block'],
        improve=['проверять HTF demand/supply перед входом', 'не входить LTF setup прямо в HTF встречную зону'],
        evidence=bool(any_flag('htf_demand_supply_blocked_trade','htf_opposing_zone_blocked','htf_zone_blocks_tp1') or (contains_any(structure_text, ('htf', '15m', '30m')) and bad_zone_blocks)))
    add(5.0, 'ltf_signal_inside_htf_range',
        primary='LTF signal inside HTF range',
        scenario='Сигнал на младшем таймфрейме появился внутри старшего range. Внутри HTF range часто нет чистого continuation, поэтому цена вернулась к SL.',
        analysis_add=['LTF signal был внутри HTF range'],
        happened=['HTF range не дал clean expansion', 'младший trigger не получил продолжение', 'SL был достигнут после range reaction'],
        visible=['на HTF цена была внутри диапазона', 'LTF setup не был у сильного HTF края', pos_line],
        secondary=['LTF signal inside HTF range', 'HTF range chop', 'No HTF expansion'],
        improve=['внутри HTF range брать только края диапазона', 'ждать breakout/retest HTF range'],
        evidence=bool(any_flag('ltf_signal_inside_htf_range','inside_htf_range','htf_range_chop') or (midrange and contains_any(structure_text, ('htf range', '15m range', '30m range')))))
    add(5.1, 'htf_opposite_fvg_ob_nearby',
        primary='HTF FVG/OB opposite direction nearby',
        scenario='Рядом находилась HTF FVG/OB зона против направления сделки. Эта старшая зона дала встречную реакцию и заблокировала путь к TP1.',
        analysis_add=['HTF opposite FVG/OB была рядом'],
        happened=['цена отреагировала на HTF opposing zone', 'LTF continuation не прошёл', 'SL был достигнут после реакции старшей зоны'],
        visible=['рядом есть HTF FVG/OB против позиции', 'entry был слишком близко к старшей opposing zone', pos_line],
        secondary=['HTF FVG/OB opposite direction nearby', 'Opposing HTF imbalance', 'HTF OB/FVG reaction'],
        improve=['не входить LTF setup прямо перед HTF opposite FVG/OB', 'цель TP1 ставить до HTF opposing zone'],
        evidence=bool(any_flag('htf_opposite_fvg_ob_nearby','htf_opposing_fvg_ob','opposite_htf_ob_fvg_nearby')))

    if side_u == 'SHORT':
        demand_desc = str(demand_ctx.get('desc') or 'local low / support')
        supply_desc = str(supply_ctx.get('desc') or 'supply / resistance')
        tp1_demand_path_line = f'TP1 находился рядом/за demand/support reaction area ({demand_desc})'

        # v11 precision: do not explain every fast SHORT loss as only
        # "near support" or generic "TP1 was never threatened". If price is
        # reclaiming from bullish FVG/support and the short has no bearish
        # acceptance, the primary cause is anti-momentum entry into buyer pressure.
        short_bullish_reclaim_evidence = bool(
            no_tp and weak_follow
            and (
                bullish_ctx
                or b('underlying_bullish_fvg')
                or b('fresh_bullish_impulse')
                or b('bullish_reclaim_before_entry')
                or (structure_pending_route and (demand_seen or demand_blocks or rr_to_tp1 >= 1.80 or cs >= 0.95))
            )
            and (fast or normal_pullback or sl_tight or b('short_against_bullish_structure') or b('underlying_bullish_fvg'))
        )
        add(14.4 + (1.0 if fast else 0) + (0.6 if normal_pullback else 0) + (0.4 if structure_pending_route else 0), 'short_against_fresh_bullish_impulse_no_acceptance',
            primary='SHORT against fresh bullish reclaim / no bearish acceptance',
            scenario='SHORT был открыт против активного buy-side impulse/reclaim и рядом с buyer-support/FVG pressure. Продавец не получил acceptance ниже entry/retest зоны и fresh bearish displacement не появился, поэтому рынок продолжил bounce/reclaim вверх и выбил SL.',
            analysis_add=['перед входом был buy-side impulse / bullish reclaim', 'acceptance ниже entry/retest зоны отсутствовал', 'fresh bearish displacement после входа отсутствовал'],
            happened=['продавец не смог закрепить цену ниже entry/retest зоны', 'покупатель продолжил buy-side движение против SHORT', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после bounce/reclaim вверх'],
            visible=['перед входом был fresh bullish impulse / reclaim', 'SHORT был взят без подтверждённого bearish acceptance', 'снизу оставалась buyer reaction / bullish FVG pressure', 'после входа нет clean bearish displacement', pos_line],
            secondary=['Short against bullish reclaim', 'No bearish acceptance', 'Buyer pressure continued', 'Bullish FVG/support below'],
            improve=['не шортить fresh bullish reclaim без bearish acceptance', 'ждать 1–2 close ниже entry/support/FVG', 'для SHORT требовать новый bearish displacement после retest'],
            evidence=short_bullish_reclaim_evidence)

        # v11 precision for Structure pending trigger shorts: a very quick loss
        # with TP1 far below is not just "TP1 was never threatened". It means
        # the pending trigger fired against buyer pressure / FVG stack without
        # bearish acceptance, so TP1 was placed beyond support.
        add(13.9 + (1.0 if fast else 0) + (0.6 if rr_to_tp1 >= 2.0 else 0), 'structure_short_trigger_failed_below_buyer_support',
            primary='SHORT trigger failed / TP1 below buyer support',
            scenario='Structure SHORT trigger сработал, но bearish acceptance не подтвердился. Под входом оставалась buyer reaction / bullish FVG-support зона, а TP1 стоял ниже этой области. Поэтому цена не дала продолжение вниз, быстро сделала bounce/reclaim и дошла до SL.',
            analysis_add=['structure SHORT trigger был без bearish acceptance', 'TP1 находился ниже buyer-support / bullish FVG reaction area', 'fresh bearish displacement после entry отсутствовал'],
            happened=['trigger сработал формально, но продавец не получил continuation', 'цена не закрепилась ниже support/FVG pressure', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после bounce/reclaim'],
            visible=['SHORT был открыт до подтверждённого breakdown ниже buyer-support', 'под entry оставалась bullish FVG/support pressure', 'TP1 был ниже ближайшей зоны покупателей', 'после entry нет clean bearish displacement', pos_line],
            secondary=['Structure trigger failed', 'No bearish acceptance', 'TP1 below support/FVG', 'Buyer reclaim against short'],
            improve=['для Structure pending SHORT ждать close ниже support/FVG', 'не ставить TP1 ниже buyer-zone без breakdown', 'если trigger сразу не даёт bearish displacement — пропускать вход'],
            evidence=bool(structure_pending_route and no_tp and weak_follow and (fast or sl_tight or normal_pullback) and (rr_to_tp1 >= 1.8 or cs >= 0.95) and (demand_seen or demand_blocks or bullish_ctx or b('underlying_bullish_fvg'))))

        # v10 precision: for wide-RR SHORT losses, "TP1 was never threatened" is
        # true but too generic.  When TP1 is far below and the entry comes after a
        # sell-side extension / weak reclaim, the main cause is a late short with
        # TP1 placed behind support/demand, not merely missing MFE.
        add(12.8 + (1.0 if fast else 0) + (0.8 if normal_pullback else 0), 'late_short_wide_tp1_behind_support_no_acceptance',
            primary='Late SHORT after dump / TP1 too far below support',
            scenario='SHORT был открыт после sell-side extension или возле нижней части реакции рынка. TP1 стоял далеко ниже и проходил через support/demand reaction area, поэтому большой RR не означал clean downside path. Продавец не дал fresh bearish continuation, цена сделала bounce/reclaim и дошла до SL.',
            analysis_add=['TP1 был далеко ниже entry / за support reaction area', 'acceptance ниже entry/retest зоны отсутствовал', 'fresh bearish displacement после входа отсутствовал'],
            happened=['продавец не получил продолжение после входа', 'цена не закрепилась ниже entry/retest зоны', 'TP1 был слишком далеко за buyer reaction/support', 'SL был достигнут после bounce/reclaim вверх'],
            visible=['перед входом sell-side идея уже была частично отработана', 'ниже entry находилась support/buyer reaction area', 'TP1 был далеко ниже, за ближайшими уровнями реакции покупателей', 'после входа нет clean bearish displacement', pos_line],
            secondary=['Late short after dump', 'TP1 too far behind support', 'No bearish acceptance', 'Buyer bounce/reclaim'],
            improve=['после dump ждать premium re-entry выше', 'не ставить TP1 далеко за support без подтверждённого breakdown', 'для SHORT требовать close ниже support и fresh bearish displacement'],
            evidence=bool(wide_tp_target and no_tp and weak_follow and (late_short or normal_pullback or fast or lowish or demand_seen or bullish_ctx)))

        # Location / momentum
        add(9.1 + (1.2 if fast else 0) + (1 if normal_pullback else 0) + (0.8 if tight_space else 0), 'late_short_after_dump_no_premium_reentry',
            primary='Late SHORT after dump / no premium re-entry',
            scenario='SHORT был открыт после уже выполненного sell-side движения, но не из premium re-entry. Продавец уже был уставший, fresh bearish displacement после входа не появился, поэтому обычный bounce/reclaim дошёл до SL раньше, чем цена смогла продолжить движение к TP1.',
            analysis_add=['entry был после sell-side impulse/dump', 'premium re-entry перед SHORT не было', 'fresh bearish displacement после входа отсутствовал'],
            happened=['первичный dump уже был отработан до входа', 'продавец не смог сразу дать новую волну вниз', 'цена дала нормальный bounce/reclaim против SHORT', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут до continuation'],
            visible=['перед входом уже прошёл sell-side impulse', 'SHORT открыт поздно после движения вниз, а не от premium/supply', 'снизу мог быть local low/support, но реальная demand/FVG-зона не подтверждена', 'после входа нет clean bearish displacement', pos_line],
            secondary=['Late short after dump', 'No premium re-entry', 'Seller exhaustion', 'No fresh bearish displacement', 'Normal pullback to SL'],
            improve=['после сильного dump ждать pullback выше / premium re-entry', 'не шортить сразу внизу после импульса', 'входить только после нового bearish displacement или breakdown ниже low'],
            evidence=bool(side_u == 'SHORT' and no_tp and weak_follow and late_short and not (real_demand_seen or demand_blocks)))
        add(9.6 + (2 if fast else 0) + (1 if demand_blocks else 0) + (1 if tight_space else 0) + (1 if normal_pullback else 0) + (1 if lowish else 0), 'short_after_dump_into_demand_exhaustion',
            primary='SHORT opened after extended dump into demand / seller exhaustion bounce',
            scenario=f'SHORT был открыт после уже выполненного sell-side движения прямо над зоной покупателя: {demand_desc}. Продавец был истощён, путь к TP1 проходил через demand/support reaction area, поэтому вместо fresh bearish continuation цена дала bounce/reclaim и дошла до SL.',
            analysis_add=['entry был после extended dump / sell-side impulse', tp1_demand_path_line, 'fresh bearish displacement после входа отсутствовал'],
            happened=['продавец не дал нового continuation после входа', 'demand/support зона дала buyer bounce', 'цена не закрепилась ниже entry/retest зоны', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после bounce/reclaim вверх'],
            visible=['перед входом уже прошёл сильный dump', f'снизу была {demand_desc}', 'SHORT открыт поздно возле post-dump low / buyer reaction area', 'после входа нет clean bearish displacement', pos_line],
            secondary=['Late short after dump', 'Short into demand', 'Seller exhaustion', 'Support bounce', 'No bearish follow-through'],
            improve=['после сильного dump не шортить прямо над demand/support', 'ждать premium re-entry выше', 'входить только после clean breakdown ниже support', 'если до TP1 стоит buyer reaction area — брать цель раньше или пропускать'],
            evidence=bool(late_or_low_short and (demand_seen or demand_blocks or lowish or tight_space) and (no_tp or fast or weak_follow)))
        add(7.0 + (2 if normal_pullback else 0) + (1 if tight_space else 0) + (1 if lowish else 0) + (1 if no_tp else 0), 'late_short_after_dump_exhaustion',
            primary='Late SHORT after dump / seller exhaustion bounce',
            scenario='SHORT был открыт после уже выполненного sell-side движения. К моменту входа продавец был истощён, снизу появилась реакция покупателя, а fresh bearish continuation не появился. Цена дала bounce/reclaim и дошла до SL.',
            analysis_add=['перед входом уже был sell-side impulse/dump', 'после входа не появился fresh bearish displacement'],
            happened=['продавец не дал нового continuation после входа', 'цена не закрепилась ниже entry/retest зоны', 'появился buyer bounce/reclaim против SHORT', 'TP1 не был нормально поставлен под угрозу'],
            visible=['перед входом уже прошёл сильный dump', 'SHORT открыт поздно, ближе к post-dump low', 'снизу появилась buyer reaction area', 'SL попал в нормальный bounce/retest после падения', pos_line],
            secondary=['Late entry after dump', 'Seller exhaustion', 'Buyer bounce from low', 'No fresh bearish displacement'],
            improve=['после сильного dump ждать premium re-entry выше', 'не шортить прямо возле post-dump low', 'требовать новый bearish displacement после bounce/retest'],
            evidence=bool(late_or_low_short and (normal_pullback or no_tp or lowish or tight_space)))
        add(8.4 + (2 if fast else 0) + (1 if normal_pullback else 0) + (1 if tight_space else 0) + (1 if demand_blocks else 0), 'short_into_demand_tight_sl_fast',
            primary='SHORT near support + SL inside normal bounce',
            scenario=f'SHORT был открыт низко рядом с support/buyer reaction area: {demand_desc}. Downside до TP1 был ограничен, а SL стоял внутри обычного bounce/retest. Поэтому цена дала реакцию покупателя и выбила SL до нормального bearish continuation.',
            analysis_add=['SL был слишком близко для такого bounce/retest', 'downside до TP1 был ограничен demand/support зоной'],
            happened=['продавец не смог сразу пробить ближний support/demand', 'обычный bounce/retest оказался сильнее SL', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут до fresh bearish continuation'],
            visible=[f'снизу была {demand_desc}', 'SHORT открыт рядом с local low / support', 'SL находился внутри нормального bounce/retest-движения', 'TP1 был рядом/за реакционной зоной, поэтому путь не был чистым', 'после входа не появился clean bearish displacement', pos_line],
            secondary=['Short near support', 'SL too tight', 'SL inside normal bounce', 'TP1 blocked by support', 'Buyer bounce reaction'],
            improve=['не шортить прямо над local low/support', 'SL ставить за structural invalidation, а не внутри bounce', 'ждать premium re-entry выше', 'если RR ломается после нормального SL — пропускать сделку'],
            evidence=bool(fast and no_tp and (demand_seen or demand_blocks or lowish or tight_space) and (risk_pct <= max(tight_sl_pct, 0.45) or normal_pullback or tight_space)))
        short_demand_primary = 'SHORT opened too low near demand/support / downside path blocked' if lowish else 'SHORT opened near demand/support / downside path blocked'
        short_demand_scenario_prefix = 'слишком низко, рядом' if lowish else 'рядом'
        add(6.5 + (2 if demand_blocks else 0) + (1 if tight_space else 0) + (1 if lowish else 0), 'short_into_demand_support',
            primary=short_demand_primary,
            scenario=f'SHORT был открыт {short_demand_scenario_prefix} с зоной покупателя: {demand_desc}. Downside до TP1 был ограничен demand/support, поэтому продавец не смог расширить движение вниз и цена дала bounce/reclaim к SL.',
            analysis_add=['downside до TP1 был ограничен demand/support зоной'],
            happened=['продавец не смог пробить ближний support/demand', 'цена дала реакцию покупателя снизу', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после bounce/reclaim вверх'],
            visible=[f'снизу была {demand_desc}', 'SHORT открыт рядом с local low / support', 'TP1 был рядом/за реакционной зоной, поэтому путь не был чистым', 'после входа не появился clean bearish displacement', pos_line],
            secondary=['Short too low', 'Short into demand/support', 'TP1 blocked by support', 'TP1 near demand/support reaction area'],
            improve=['не шортить прямо над local low/support', 'ждать premium re-entry выше', 'проверять demand/support на пути к TP1'],
            evidence=bool(short_demand_block_evidence))
        add(5.7 + (1.5 if lowish else 0), 'short_discount_bad_location',
            primary='SHORT opened in discount / too close to range low',
            scenario='SHORT был открыт в нижней части локального диапазона. В таком месте downside уже ограничен, а вероятность bounce/reclaim против входа высокая.',
            analysis_add=['entry был в нижней части prior range'],
            happened=['цена не дала продолжение вниз из discount', 'покупатель быстро вернул цену вверх', 'SL был достигнут после bounce'],
            visible=['вход расположен ближе к range low, а не в premium', 'для SHORT location был слабый', 'снизу оставалось мало пространства до реакции покупателей', pos_line],
            secondary=['Bad discount location for SHORT', 'Near range low', 'Buyer reaction risk'],
            improve=['для SHORT искать premium-зону выше', 'не открывать short в нижней трети диапазона'],
            evidence=lowish and no_tp)
        add(6.0 + (2 if bullish_ctx else 0) + (1 if supply_seen else 0), 'short_against_bullish_structure_failed_supply',
            primary='SHORT against bullish structure / failed supply reaction',
            scenario=f'SHORT был открыт от seller/supply идеи ({supply_desc}), но рынок уже показывал bullish reclaim или buy-side pressure. Bearish acceptance не закрепился, поэтому покупатель выбил SL.',
            analysis_add=['bullish context / reclaim перед SHORT', 'bearish displacement после входа отсутствовал'],
            happened=['seller zone не дала сильной реакции', 'цена не закрепилась ниже entry/retest зоны', 'после входа не было clean bearish displacement', 'SL был достигнут после buy-side reclaim'],
            visible=['перед входом был buy-side impulse / bullish context', 'SHORT был взят без подтверждённого bearish reclaim', 'снизу сохранялся buyer pressure / demand', 'TP1 не был нормально поставлен под угрозу', pos_line],
            secondary=['Short against bullish structure', 'Failed supply reaction', 'No bearish reclaim'],
            improve=['не брать SHORT против bullish 5m/15m без bearish reclaim', 'ждать закрытие ниже demand/support', 'требовать новый bearish displacement после retest'],
            evidence=bool(bullish_ctx and not demand_blocks and (weak_follow or no_tp)))
        # Structure / setup-specific
        add(5.0 + (1.5 if breakout_route else 0), 'breakdown_trap_reclaimed',
            primary='Breakdown trap / level reclaimed back',
            scenario='Пробой вниз не получил продолжения: после входа цена вернулась обратно выше уровня/entry, из-за этого SHORT превратился в breakdown trap и закрылся по SL.',
            analysis_add=['breakdown был reclaimed обратно', 'bearish continuation не подтвердился'],
            happened=['цена не удержалась ниже пробитой зоны', 'reclaim вверх отменил short-идею', 'SL был достигнут после возврата выше entry/level'],
            visible=['после breakdown цена вернулась обратно в range/выше уровня', 'нет закрепления ниже retest-зоны', pos_line],
            secondary=['Breakdown trap', 'Level reclaimed', 'No bearish acceptance'],
            improve=['после BOS вниз ждать закрепление ниже уровня', 'не входить, если reclaim появляется сразу после trigger'],
            evidence=bool((b('level_reclaimed_back') or b('reclaim_lost_back')) and breakout_route and no_tp))
        add(4.8 + (1.5 if reclaim_route else 0), 'liquidity_reclaim_failed_short',
            primary='Liquidity reclaim failed for SHORT',
            scenario='Логика liquidity reclaim не подтвердилась: цена не удержала bearish reclaim и вернулась против позиции. Поэтому продавец не получил continuation, а SL был выбит.',
            analysis_add=['liquidity reclaim не подтвердился'],
            happened=['reclaim/снятие ликвидности не дало продолжения вниз', 'цена быстро вернулась выше entry/level', 'SL был достигнут после обратного reclaim'],
            visible=['после sweep/reclaim нет bearish acceptance', 'цена вернулась против SHORT', pos_line],
            secondary=['Liquidity reclaim failed', 'Sweep without continuation', 'Reclaim lost back'],
            improve=['для liquidity reclaim ждать close и continuation ниже reclaim-level', 'не шортить, если цена быстро возвращается выше reclaim-level'],
            evidence=bool((b('reclaim_not_confirmed') or b('reclaim_lost_back')) and reclaim_route))
        add(4.9 + (1.5 if ob_fvg_route else 0), 'short_ob_fvg_zone_failed',
            primary='OB/FVG seller zone failed to hold price',
            scenario='SHORT был построен от OB/FVG зоны, но реакция продавца была слабой. Цена не удержалась ниже зоны/середины зоны и пошла к SL.',
            analysis_add=['OB/FVG reaction была слабой', 'zone hold не подтвердился'],
            happened=['seller zone не дала сильного rejection', 'цена вернулась в зону/выше midpoint', 'continuation вниз не появился'],
            visible=[f'{supply_desc} не дала clean rejection', 'price action после входа не удержался ниже seller zone', pos_line],
            secondary=['OB/FVG reaction failed', 'Zone not held', 'Weak seller rejection'],
            improve=['не входить по OB/FVG без сильной реакции', 'требовать rejection candle и continuation от зоны'],
            evidence=bool(ob_fvg_route and (b('zone_midpoint_lost') or b('entry_too_deep_into_zone') or b('htf_zone_not_held') or weak_confirm) and no_tp))
        add(4.7 + (1.5 if origin_route else 0), 'short_origin_not_held',
            primary='Displacement origin failed / origin was not held',
            scenario='Origin setup не удержал уровень: после входа цена вернулась против short-идеи и сломала origin/validation area. Поэтому setup стал invalid.',
            analysis_add=['origin level не удержался', 'displacement после origin был слабый'],
            happened=['origin-зона не дала continuation', 'цена вернулась против входа', 'SL был достигнут после провала origin'],
            visible=['displacement origin не удержал price action', 'нет нового bearish expansion от origin', pos_line],
            secondary=['Origin not held', 'Displacement too weak', 'Origin setup failed'],
            improve=['для origin setup требовать сильный displacement от origin', 'не входить, если цена сразу возвращается в origin-зону'],
            evidence=bool(origin_route and (b('origin_not_held') or b('displacement_too_weak') or weak_confirm)))
        add(4.7 + (1.5 if dual_fvg_route else 0), 'short_stacked_fvg_failed',
            primary='Stacked/Dual FVG confluence failed',
            scenario='Stacked/Dual FVG confluence не удержала цену. Вместо continuation вниз цена вернулась против входа, поэтому SHORT закрылся по SL.',
            analysis_add=['stacked FVG confluence не удержал цену'],
            happened=['FVG stack не дал сильной реакции продавца', 'цена вернулась против short', 'SL был достигнут после слабого continuation'],
            visible=['dual/stacked FVG не дал чистого rejection', 'цена не удержалась ниже FVG confluence', pos_line],
            secondary=['Stacked FVG failed', 'Confluence failed', 'Weak FVG reaction'],
            improve=['не брать dual FVG, если первая реакция слабая', 'требовать continuation candle после FVG retest'],
            evidence=bool(dual_fvg_route and (b('stacked_fvg_not_held') or b('confluence_failed') or weak_confirm)))
        # HTF / liquidity magnets / target path
        add(4.6, 'htf_bullish_context_against_short',
            primary='5m SHORT was against 15m/30m bullish context',
            scenario='Младший SHORT был открыт против старшего bullish context. На HTF не было подтверждённого bearish shift, поэтому покупательская структура быстро вернула цену к SL.',
            analysis_add=['HTF context был против SHORT'],
            happened=['5m signal не получил поддержку от 15m/30m', 'цена вернулась в сторону HTF контекста', 'SL был достигнут после buy-side pressure'],
            visible=['15m/30m context не подтверждал bearish continuation', 'SHORT был против старшей структуры', pos_line],
            secondary=['HTF bullish context', 'LTF signal against HTF', 'No HTF bearish shift'],
            improve=['для SHORT требовать хотя бы neutral/bearish 15m/30m', 'не брать 5m short против явного HTF buy-side pressure'],
            evidence=bool(contains_any(structure_text, ('15m bullish', '30m bullish', 'htf bullish')) and no_tp))
    else:
        supply_desc = str(supply_ctx.get('desc') or 'local high / resistance')
        demand_desc = str(demand_ctx.get('desc') or 'demand / support')
        tp1_supply_path_line = f'TP1 находился рядом/за supply/resistance reaction area ({supply_desc})'
        tp1_distance_line = f'расстояние entry → TP1 было около {cs:.2f}% / RR до TP1 около 1:{rr_to_tp1:.2f}' if cs > 0 and rr_to_tp1 > 0 else (f'расстояние entry → TP1 было около {cs:.2f}%' if cs > 0 else 'TP1 был далеко за ближайшими reaction levels')

        # V12: BB-type fix. Sometimes the LONG direction is valid and the
        # market continues after the stop. In that case the main reason must be
        # the stop placement, not an automatic "late long / no acceptance" label.
        # Only use this when there is visible bullish support/FVG below entry and
        # no real overhead supply/resistance blocking TP1.
        bullish_idea_continued_after_stop = bool(any_flag(
            'bullish_idea_continued_after_stop', 'long_continued_after_sl',
            'idea_continued_after_stop', 'setup_worked_after_sl',
            'price_reached_tp1_after_sl', 'tp1_hit_after_sl', 'tp1_reached_after_sl'
        ))

        # V18: do not call every tight LONG stop under resistance an "SL inside
        # normal pullback".  Generic pre-close diagnostics such as
        # sl_inside_normal_pullback can be true for any close stop, including
        # cases where price keeps moving against the trade after SL.  We only
        # allow that wording as a MAIN reason when there is real after-SL proof
        # (TP1/reclaim/idea continued) or when the stop was very fast and the
        # pullback/retest is obvious.  Long slow losses like GALA/POL/ROSE stay
        # focused on resistance/no acceptance, not on an unproven SL diagnosis.
        try:
            sl_pullback_confirm_max_min = int(float(str(os.getenv('LOSS_CARD_SL_PULLBACK_CONFIRM_MAX_MIN', '30') or '30').replace(',', '.')))
        except Exception:
            sl_pullback_confirm_max_min = 30
        long_after_sl_reclaim_confirmed = bool(
            bullish_idea_continued_after_stop
            or any_flag(
                'price_reclaimed_after_sl', 'reclaim_after_sl', 'bounce_after_sl',
                'long_reclaimed_after_sl', 'price_reached_tp1_after_sl',
                'tp1_hit_after_sl', 'tp1_reached_after_sl', 'setup_worked_after_sl',
                'idea_continued_after_stop', 'long_continued_after_sl',
            )
        )
        long_generic_sl_pullback_flag = bool(any_flag(
            'sl_inside_normal_pullback', 'sl_inside_pullback',
            'sl_inside_retest', 'sl_inside_pullback_retest',
            'stop_inside_normal_pullback', 'normal_pullback_to_sl'
        ))
        long_sl_pullback_confirmed = bool(
            long_after_sl_reclaim_confirmed
            or (fast and normal_pullback)
            or (
                long_generic_sl_pullback_flag
                and normal_pullback
                and duration_min is not None
                and duration_min <= sl_pullback_confirm_max_min
            )
        )
        valid_long_but_bad_sl = bool(
            side_u == 'LONG'
            and no_tp
            and long_sl_pullback_confirmed
            and (normal_pullback or sl_tight or long_generic_sl_pullback_flag or any_flag('sl_too_tight'))
            and (real_demand_seen or demand_seen or bullish_ctx or late_long)
            and not (real_supply_seen or supply_blocks or highish or bearish_ctx)
            and (cs <= 0 or cs <= float(str(os.getenv('LOSS_CARD_VALID_LONG_MAX_TP1_PCT', '2.20') or '2.20').replace(',', '.')))
            and (rr_to_tp1 <= 0 or rr_to_tp1 <= float(str(os.getenv('LOSS_CARD_VALID_LONG_MAX_RR', '2.20') or '2.20').replace(',', '.')))
        )
        add(11.8 + (1.6 if bullish_idea_continued_after_stop else 0.0) + (0.8 if real_demand_seen else 0.0), 'long_valid_idea_sl_too_tight',
            primary='SL too tight inside normal pullback / bullish idea continued after stop',
            scenario='LONG-направление было рабочим, но SL стоял слишком близко к entry и попал внутрь обычного pullback/retest. Цена выбила стоп до структурной invalidation, а сама bullish idea оставалась валидной: под входом была buyer-support / bullish FVG зона, и TP1 был достижимым без явной overhead supply-блокировки.',
            analysis_add=[tp1_distance_line if wide_tp_target else cs_line, risk_line, 'SL стоял внутри normal pullback/retest, а не за structural invalidation', 'под entry была bullish FVG / buyer-support зона'],
            happened=['рынок сделал обычный pullback/retest против LONG', 'SL был достигнут раньше структурной invalidation', 'после стопа long-идея не была полностью сломана', 'TP1 был достижимым, проблема была в стопе, а не в направлении'],
            visible=['общая структура перед входом была bullish / buy-side impulse', 'под entry были bullish FVG / demand зоны', 'нет подтверждённой сильной seller-zone прямо над entry', 'SL стоял внутри обычного отката после импульса', 'после выбивания SL цена могла продолжить рост, значит direction был не главный дефект', pos_line],
            secondary=['SL too tight', 'SL inside normal pullback', 'Bullish idea continued after stop', 'Structural invalidation was lower', 'TP1 was reachable'],
            improve=['ставить SL за structural invalidation ниже buyer-support/FVG зоны', 'не ставить SL внутри первого pullback после impulse', 'если нормальный structural SL ломает RR — пропускать сделку', 'для valid LONG не менять причину на late entry, если после SL идея продолжила работать'],
            evidence=valid_long_but_bad_sl)

        # V16: KMNO-type precision. If TP1 is very far and sits behind an
        # overhead bearish FVG/supply stack, do not call the trade only "late
        # after pump". The exact problem is target-path quality: a wide RR was
        # created mostly by a tight SL while the path to TP1 crossed multiple
        # seller reaction / FVG zones and acceptance above the stack never came.
        overhead_fvg_stack = bool(
            real_supply_is_fvg
            or b('overhead_bearish_fvg')
            or b('bearish_fvg_above_entry')
            or b('red_fvg_above_entry')
            or b('supply_fvg_above_entry')
            or b('nearest_bearish_fvg')
            or b('nearest_supply_fvg')
            or b('tp1_behind_bearish_fvg')
            or b('tp1_behind_supply_fvg')
            or b('supply_fvg_blocks_tp1')
            or b('bearish_fvg_blocks_tp1')
            or ('fvg' in str(supply_ctx.get('tfs') or '').lower())
        )
        wide_overhead_stack = bool(
            side_u == 'LONG'
            and no_tp
            and wide_tp_target
            and not tight_space
            and (weak_follow or normal_pullback or sl_tight)
            and (overhead_fvg_stack or supply_seen or supply_blocks or ob_fvg_route or highish)
            and (cs >= max(wide_tp1_pct, 1.20) or rr_to_tp1 >= max(wide_tp1_rr, 2.50))
        )
        stack_name = 'bearish FVG / supply stack' if overhead_fvg_stack else 'overhead supply/resistance stack'
        stack_primary = 'LONG into overhead bearish FVG stack / TP1 too far behind supply' if overhead_fvg_stack else 'LONG under overhead supply stack / TP1 too far behind resistance'
        add(14.9 + (0.8 if overhead_fvg_stack else 0.0) + (0.5 if rr_to_tp1 >= 4.0 else 0.0) + (0.4 if sl_tight else 0.0), 'long_tp1_far_behind_overhead_fvg_stack',
            primary=stack_primary,
            scenario=f'LONG был открыт после bounce/retest, но TP1 стоял далеко выше — за {stack_name}. Большой RR получился из-за близкого SL, а не из-за чистого пути: цена не получила acceptance выше entry/overhead zone, fresh bullish displacement не появился, поэтому рынок откатил к SL раньше, чем смог пройти к TP1.',
            analysis_add=[tp1_distance_line, f'TP1 находился за {stack_name}', 'acceptance выше entry / overhead zone отсутствовал', 'fresh bullish displacement после входа отсутствовал', 'большой RR не означал clean path до TP1'],
            happened=['покупатель не смог закрепиться выше ближайшей seller reaction / FVG зоны', 'TP1 был слишком далеко за overhead reaction stack', 'после входа не появился новый bullish expansion', 'SL был достигнут после pullback/rejection до подтверждённого continuation'],
            visible=[f'над входом был {stack_name}', 'TP1 стоял выше ближайших seller reaction/FVG зон, а не перед ними', 'путь к TP1 проходил через несколько overhead reaction levels', 'SL был близким относительно ширины цели, поэтому RR выглядел большим', 'после входа цена не дала clean bullish displacement', pos_line],
            secondary=['TP1 too far behind overhead FVG/supply', 'No acceptance above overhead zone', 'Tight SL inflated RR', 'No fresh bullish displacement', 'Seller reaction stack above entry'],
            improve=['не ставить TP1 за несколькими overhead FVG/supply зонами без acceptance', 'если RR большой только из-за tight SL — проверять реальный clean path', 'для LONG ждать close выше seller reaction stack или брать цель до него', 'SL ставить за structural invalidation, а не делать цель нереально далёкой'],
            evidence=wide_overhead_stack)

        # V15: VIRTUAL-type fix. A LONG after a near-vertical buy-side leg
        # should not be reduced to the generic "under resistance" label. If the
        # move into entry was extended and SL sits inside the first pullback, make
        # the pump + pullback context the main reason.
        inferred_vertical_long_pump = bool(
            side_u == 'LONG'
            and no_tp
            and not real_supply_is_fvg
            and (
                vertical_long_pump
                or (ob_fvg_route and tight_space and risk_pct >= vertical_pump_min_risk_pct and cs >= vertical_pump_min_cs_pct and duration_min is not None and duration_min >= 18)
            )
            and (normal_pullback or sl_tight or weak_follow)
            and (supply_seen or supply_blocks or ob_fvg_route or highish)
        )
        vertical_long_sl_confirmed = bool(long_sl_pullback_confirmed and (normal_pullback or sl_tight or long_generic_sl_pullback_flag))
        vertical_long_primary = (
            'Late LONG after vertical pump / SL inside normal pullback'
            if vertical_long_sl_confirmed
            else 'Late LONG after vertical pump / no acceptance above resistance'
        )
        vertical_long_scenario = (
            'LONG был открыт после сильного почти вертикального buy-side impulse, уже высоко относительно последнего движения и под/рядом с local high / seller reaction area. После входа не появилось acceptance выше entry и fresh bullish displacement, цена сделала обычный pullback/retest после pump, а SL стоял внутри этого шума. Поэтому стоп выбило до подтверждённого continuation.'
            if vertical_long_sl_confirmed
            else 'LONG был открыт после сильного почти вертикального buy-side impulse, уже высоко относительно последнего движения и под/рядом с local high / seller reaction area. После входа не появилось acceptance выше entry и fresh bullish displacement, поэтому цена ушла к SL без подтверждённого continuation.'
        )
        vertical_long_analysis = ['перед входом был extended / vertical buy-side pump', 'acceptance выше entry/local high отсутствовал', 'fresh bullish displacement после входа отсутствовал']
        vertical_long_happened = ['после входа покупатель не дал сразу новую волну вверх', 'цена не закрепилась выше entry/local high']
        vertical_long_visible = ['перед входом был сильный buy-side impulse / pump', 'LONG был открыт после роста, а не из discount', 'над/рядом с entry оставалась local high / seller reaction area', 'после входа нет clean bullish displacement', pos_line]
        vertical_long_secondary = ['Late long after vertical pump', 'No acceptance above entry', 'No fresh bullish displacement', 'Entry after extended move']
        vertical_long_improve = ['после vertical pump ждать pullback ниже / discount re-entry', 'не лонговать сразу под local high без acceptance', 'если после retest нет fresh bullish displacement — пропускать вход']
        if vertical_long_sl_confirmed:
            vertical_long_analysis.insert(2, 'SL стоял внутри normal pullback/retest после pump')
            vertical_long_happened.extend(['рынок сделал normal pullback после pump', 'SL был достигнут раньше подтверждённого continuation'])
            vertical_long_visible.insert(3, 'SL находился внутри обычного pullback после импульса')
            vertical_long_secondary.insert(1, 'SL inside normal pullback')
            vertical_long_improve.insert(2, 'SL ставить за structural invalidation ниже buyer-support/FVG зоны')
        add(13.2 + (0.8 if vertical_long_pump else 0.0) + (0.6 if normal_pullback else 0.0) + (0.4 if supply_seen else 0.0), 'late_long_after_vertical_pump_sl_inside_pullback',
            primary=vertical_long_primary,
            scenario=vertical_long_scenario,
            analysis_add=vertical_long_analysis,
            happened=vertical_long_happened,
            visible=vertical_long_visible,
            secondary=vertical_long_secondary,
            improve=vertical_long_improve,
            evidence=inferred_vertical_long_pump)

        # V15/V17: 0G/ROSE-type precision. When the problem is truly both
        # overhead local resistance and a confirmed stop inside a normal retest,
        # print the combined cause. If the stop was merely close but price kept
        # moving against the trade after SL, keep the main reason as resistance /
        # no acceptance instead of overstating the SL problem. Real red FVG keeps
        # its own stronger FVG-specific reason above.
        inferred_resistance_plus_bad_sl = bool(
            side_u == 'LONG'
            and no_tp
            and not real_supply_is_fvg
            and not inferred_vertical_long_pump
            and (tight_space or supply_seen or supply_blocks or highish or ob_fvg_route)
            and long_sl_pullback_confirmed
            and (normal_pullback or sl_tight or any_flag('sl_inside_normal_pullback', 'sl_inside_pullback', 'sl_inside_retest'))
            and weak_follow
        )
        add(11.4 + (0.6 if normal_pullback else 0.0) + (0.4 if supply_seen else 0.0), 'long_under_resistance_sl_inside_pullback',
            primary='LONG under local resistance + SL inside normal pullback',
            scenario=f'LONG был открыт под ближайшим local resistance / seller reaction area ({supply_desc}), но SL одновременно стоял слишком близко и попал внутрь обычного pullback/retest. Покупатель не дал acceptance выше entry, цена откатила в нормальную зону ретеста и выбила SL до подтверждённого continuation.',
            analysis_add=['acceptance выше entry/retest зоны отсутствовал', 'SL стоял внутри normal pullback/retest', 'fresh bullish displacement после входа отсутствовал', tp1_supply_path_line],
            happened=['покупатель не смог закрепиться выше local resistance', 'рынок сделал обычный pullback/retest против LONG', 'SL был достигнут раньше structural invalidation', 'TP1 не был нормально поставлен под угрозу'],
            visible=[f'над входом была {supply_desc}', 'LONG открыт под/рядом с local high / seller reaction area', 'SL находился внутри нормального pullback/retest-движения', 'после входа нет clean bullish displacement', 'цена не удержала entry/retest-зону', pos_line],
            secondary=['Long under local resistance', 'SL inside normal pullback', 'No acceptance above entry', 'Weak bullish follow-through', 'TP1 blocked by resistance'],
            improve=['не брать LONG под resistance без закрепления выше', 'SL ставить за structural invalidation, а не внутри первого pullback', 'ждать 1–2 close выше seller reaction area', 'если после retest нет displacement — пропускать вход'],
            evidence=inferred_resistance_plus_bad_sl)

        add(10.2 + (1.0 if late_long else 0) + (0.8 if (supply_seen or supply_blocks) else 0) + (0.6 if normal_pullback else 0), 'long_tp1_far_behind_resistance_no_acceptance',
            primary='LONG into local resistance / TP1 too far behind supply',
            scenario='LONG был открыт после bounce/reclaim, но прямо под ближайшим local high / resistance. TP1 стоял далеко выше и проходил через несколько overhead reaction levels, поэтому большой RR не означал clean path. Цена не закрепилась выше resistance, fresh bullish displacement не появился, и pullback/rejection дошёл до SL.',
            analysis_add=[tp1_distance_line, 'acceptance выше local resistance отсутствовал', 'fresh bullish displacement после входа отсутствовал'],
            happened=['покупатель не смог закрепиться выше ближайшего resistance', 'TP1 был слишком далеко за overhead reaction area', 'после входа не появился новый bullish expansion', 'SL был достигнут после pullback/rejection'],
            visible=['перед входом уже был bounce/reclaim вверх', f'над входом находился {supply_desc}', 'TP1 был далеко выше, за ближайшими resistance/supply реакциями', 'путь к TP1 не был clean, хотя расстояние до цели было большим', 'после входа цена не дала acceptance выше local high/resistance', pos_line],
            secondary=['TP1 too far behind resistance', 'No acceptance above resistance', 'Late/weak LONG location', 'No fresh bullish displacement', 'Seller reaction from local high'],
            improve=['не ставить TP1 за несколькими overhead supply/resistance зонами', 'для LONG ждать acceptance выше ближайшего resistance', 'после bounce брать цель до resistance или ждать discount re-entry ниже', 'не считать большой RR чистым путём до TP1'],
            evidence=bool(wide_tp_target and no_tp and weak_follow and (late_long or supply_seen or supply_blocks or highish or normal_pullback or (duration_min is not None and duration_min >= 20))))

        late_long_wide_sl_confirmed = bool(long_sl_pullback_confirmed and (normal_pullback or sl_tight or long_generic_sl_pullback_flag))
        late_long_wide_primary = (
            'Late LONG after pump / no acceptance + SL inside pullback'
            if late_long_wide_sl_confirmed
            else 'Late LONG after pump / no acceptance above resistance'
        )
        late_long_wide_scenario = (
            'LONG был открыт после сильного роста, когда цена уже находилась высоко относительно последнего impulse и под/рядом с seller reaction area. TP1 мог быть далеко по проценту/RR, но вход был не из discount: рынок не дал acceptance выше resistance, затем сделал обычный pullback/retest, а SL стоял внутри этого шума. Поэтому сделку выбило по SL до подтверждённого continuation.'
            if late_long_wide_sl_confirmed
            else 'LONG был открыт после сильного роста, когда цена уже находилась высоко относительно последнего impulse и под/рядом с seller reaction area. TP1 мог быть далеко по проценту/RR, но путь к цели не был чистым: рынок не дал acceptance выше resistance и fresh bullish displacement после входа не появился. Поэтому цена ушла к SL без подтверждённого continuation.'
        )
        late_long_wide_analysis = [tp1_distance_line, 'acceptance выше local resistance отсутствовал', 'fresh bullish displacement после входа отсутствовал']
        late_long_wide_happened = ['после входа покупатель не дал сразу новую волну вверх', 'цена не закрепилась выше local resistance / entry-zone', 'TP1 не был взят, хотя цель могла быть далеко']
        late_long_wide_visible = ['перед входом уже прошёл buy-side impulse / pump', 'LONG был открыт после роста, а не из discount', 'над входом оставалась seller reaction / resistance area', 'большое расстояние до TP1 не означает clean path', pos_line]
        late_long_wide_secondary = ['Late long after pump', 'No acceptance above resistance', 'No fresh bullish displacement', 'TP1 not reached']
        late_long_wide_improve = ['после сильного pump ждать pullback ниже / discount re-entry', 'лонговать выше resistance только после acceptance/закрепления', 'не считать большой RR чистым путём до TP1 без acceptance']
        if late_long_wide_sl_confirmed:
            late_long_wide_analysis.insert(2, 'SL стоял внутри normal pullback/retest после pump')
            late_long_wide_happened.insert(2, 'цена сделала normal pullback/retest против LONG')
            late_long_wide_happened.insert(3, 'SL был достигнут раньше подтверждённого continuation')
            late_long_wide_visible.insert(3, 'SL находился внутри обычного pullback после импульса')
            late_long_wide_secondary.insert(2, 'SL inside normal pullback')
            late_long_wide_improve.insert(2, 'ставить SL за structural invalidation, а не внутри первого pullback')
        add(10.4 + (1.2 if wide_tp_target else 0) + (1.0 if normal_pullback else 0) + (0.7 if late_long else 0), 'late_long_after_pump_sl_inside_pullback_wide_tp',
            primary=late_long_wide_primary,
            scenario=late_long_wide_scenario,
            analysis_add=late_long_wide_analysis,
            happened=late_long_wide_happened,
            visible=late_long_wide_visible,
            secondary=late_long_wide_secondary,
            improve=late_long_wide_improve,
            evidence=bool(wide_tp_target and no_tp and (late_long or bullish_ctx or normal_pullback or supply_seen or highish or wide_tp_target) and (weak_follow or normal_pullback or duration_min is not None)))

        # V7 exact FVG/seller-zone detector. If the pre-entry snapshot shows
        # a real overhead red FVG / supply zone, make that the main reason instead
        # of the broader "local resistance" wording. This fixes ASTER-type cards:
        # LONG was not just under resistance; it was opened into/under a bearish FVG
        # and price never accepted above entry before SL.
        real_bearish_fvg_block = bool(side_u == 'LONG' and (real_supply_is_fvg or b('overhead_bearish_fvg')) and no_tp and (weak_follow or tight_space or supply_blocks or normal_pullback))
        add(12.6 + (1.0 if supply_blocks else 0) + (0.8 if tight_space else 0) + (0.6 if normal_pullback else 0), 'long_into_bearish_fvg_no_acceptance',
            primary='LONG into bearish FVG / local resistance, no acceptance above entry',
            scenario=f'LONG был открыт прямо под/в районе bearish FVG / seller reaction area ({supply_desc}). Цена не смогла закрепиться выше entry/retest зоны, fresh bullish displacement не появился, а TP1 находился за ближайшей зоной продавца. Поэтому после слабого стояния под зоной движение ушло к SL.',
            analysis_add=['bearish FVG / seller reaction area была над entry', 'acceptance выше entry/retest зоны отсутствовал', 'fresh bullish displacement после входа отсутствовал', tp1_supply_path_line],
            happened=['покупатель не смог принять цену выше bearish FVG / resistance', 'seller reaction удержала движение сверху', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после rejection/pullback'],
            visible=[f'над входом была {supply_desc}', 'LONG был открыт под/в районе bearish FVG / seller reaction area', 'TP1 стоял за ближайшей зоной продавца, а не перед свободным пространством', 'после входа нет clean bullish displacement', 'цена не удержала reclaim/entry-зону', pos_line],
            secondary=['Bearish FVG above entry', 'No acceptance above entry', 'Local resistance held', 'Seller rejection', 'No fresh bullish displacement'],
            improve=['не брать LONG прямо под bearish FVG / seller reaction area', 'ждать 1–2 close выше FVG/resistance', 'если после retest нет fresh bullish displacement — пропускать вход', 'TP1 ставить до ближайшей seller zone или не брать сделку'],
            evidence=real_bearish_fvg_block)

        # Symmetric exact detector for SHORT near a real green FVG / buyer zone.
        real_bullish_fvg_block = bool(side_u == 'SHORT' and (real_demand_is_fvg or b('underlying_bullish_fvg')) and no_tp and (weak_follow or tight_space or demand_blocks or normal_pullback))
        add(12.6 + (1.0 if demand_blocks else 0) + (0.8 if tight_space else 0) + (0.6 if normal_pullback else 0), 'short_into_bullish_fvg_no_acceptance',
            primary='SHORT into bullish FVG / local support, no acceptance below entry',
            scenario=f'SHORT был открыт прямо над/в районе bullish FVG / buyer reaction area ({demand_desc}). Цена не смогла закрепиться ниже entry/retest зоны, fresh bearish displacement не появился, а TP1 находился за ближайшей зоной покупателя. Поэтому после слабого стояния над зоной цена дала bounce/reclaim к SL.',
            analysis_add=['bullish FVG / buyer reaction area была под entry', 'acceptance ниже entry/retest зоны отсутствовал', 'fresh bearish displacement после входа отсутствовал', tp1_demand_path_line],
            happened=['продавец не смог принять цену ниже bullish FVG / support', 'buyer reaction удержала движение снизу', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после bounce/reclaim'],
            visible=[f'под входом была {demand_desc}', 'SHORT был открыт над/в районе bullish FVG / buyer reaction area', 'TP1 стоял за ближайшей зоной покупателя, а не перед свободным пространством', 'после входа нет clean bearish displacement', 'цена не удержала breakdown/entry-зону', pos_line],
            secondary=['Bullish FVG below entry', 'No acceptance below entry', 'Local support held', 'Buyer bounce', 'No fresh bearish displacement'],
            improve=['не брать SHORT прямо над bullish FVG / buyer reaction area', 'ждать 1–2 close ниже FVG/support', 'если после retest нет fresh bearish displacement — пропускать вход', 'TP1 ставить до ближайшей buyer zone или не брать сделку'],
            evidence=real_bullish_fvg_block)

        resistance_acceptance_primary = 'Late LONG after pump / no acceptance at resistance' if late_long else 'LONG under local resistance / no acceptance after retest'
        add(9.4 + (1.0 if tight_space else 0) + (0.8 if normal_pullback else 0) + (0.6 if supply_seen else 0), 'long_under_resistance_no_acceptance',
            primary=resistance_acceptance_primary,
            scenario=f'LONG был открыт под ближайшим resistance/seller reaction area ({supply_desc}). После входа цена не закрепилась выше entry/retest зоны, покупатель не дал fresh bullish displacement, поэтому движение быстро превратилось в rejection/pullback к SL.',
            analysis_add=['acceptance выше entry/retest зоны отсутствовал', 'fresh bullish displacement после входа отсутствовал'],
            happened=['покупатель не смог закрепиться выше локального resistance', 'seller reaction появилась раньше continuation', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после rejection/отката'],
            visible=[f'над входом была {supply_desc}', 'LONG открыт под local high / seller reaction area', 'после входа нет clean bullish displacement', 'цена не удержала reclaim/entry-зону', pos_line],
            secondary=['No acceptance after retest', 'Local resistance held', 'Weak bullish follow-through', 'Seller rejection'],
            improve=['не брать LONG под resistance без закрепления выше', 'ждать 1–2 close выше seller reaction area', 'если после retest нет displacement — пропускать вход'],
            evidence=bool((not wide_tp_target) and no_tp and weak_follow and (tight_space or supply_seen or supply_blocks or highish or normal_pullback or ob_fvg_route) and not bearish_ctx))

        add(9.0 + (1.5 if fast else 0) + (1 if normal_pullback else 0) + (0.8 if supply_seen else 0), 'fast_failed_long_after_pump_no_acceptance',
            primary='Late LONG after pump / no acceptance after retest',
            scenario='LONG был открыт после уже выполненного buy-side движения. После входа не появилось acceptance выше entry/retest зоны и fresh bullish displacement. Цена быстро дала rejection/pullback к SL, поэтому проблема была не только в SL, а в позднем входе без подтверждённого continuation.',
            analysis_add=['entry был после buy-side impulse/pump', 'acceptance выше retest/entry зоны отсутствовал', 'fresh bullish displacement после входа отсутствовал'],
            happened=['покупатель не смог сразу дать новую волну вверх', 'цена не закрепилась выше entry/retest зоны', 'появился rejection/pullback против LONG', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут до continuation'],
            visible=['перед входом уже прошёл buy-side impulse', 'LONG открыт после движения вверх, а не из discount', 'после входа нет clean bullish displacement', 'первая реакция быстро пошла против LONG', pos_line],
            secondary=['Late long after pump', 'No retest acceptance', 'Buyer exhaustion', 'No fresh bullish displacement', 'Fast rejection'],
            improve=['после сильного pump ждать pullback ниже / discount re-entry', 'не брать LONG без acceptance выше entry/retest зоны', 'требовать fresh bullish displacement после retest'],
            evidence=bool(side_u == 'LONG' and no_tp and fast and weak_follow and (late_long or bullish_ctx or supply_seen or cs <= 1.45)))
        add(9.6 + (2 if fast else 0) + (1 if supply_blocks else 0) + (1 if tight_space else 0) + (1 if normal_pullback else 0) + (1 if highish else 0), 'long_after_pump_into_resistance_exhaustion',
            primary='LONG opened after extended pump into resistance / buyer exhaustion',
            scenario=f'LONG был открыт после уже выполненного buy-side движения прямо под зоной продавца: {supply_desc}. Покупатель был истощён, путь к TP1 проходил через supply/resistance reaction area, поэтому вместо fresh bullish continuation цена дала rejection/pullback и дошла до SL.',
            analysis_add=['entry был после extended pump / buy-side impulse', tp1_supply_path_line, 'fresh bullish displacement после входа отсутствовал'],
            happened=['покупатель не дал нового continuation после входа', 'supply/resistance зона дала seller rejection', 'цена не закрепилась выше entry/retest зоны', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после rejection/отката'],
            visible=['перед входом уже прошёл сильный pump', f'сверху была {supply_desc}', 'LONG открыт поздно возле post-pump high / seller reaction area', 'после входа нет clean bullish displacement', pos_line],
            secondary=['Late long after pump', 'Long into resistance', 'Buyer exhaustion', 'Seller rejection', 'No bullish follow-through'],
            improve=['после сильного pump не лонговать прямо под high/resistance', 'ждать discount re-entry ниже', 'входить только после нового acceptance выше resistance', 'если до TP1 стоит seller reaction area — брать цель раньше или пропускать'],
            evidence=bool(late_or_high_long and (supply_seen or supply_blocks or highish or tight_space) and (no_tp or fast or weak_follow)))
        add(7.0 + (2 if normal_pullback else 0) + (1 if tight_space else 0) + (1 if highish else 0) + (1 if no_tp else 0), 'late_long_after_pump_exhaustion',
            primary='Late LONG after pump / buyer exhaustion pullback',
            scenario='LONG был открыт после уже выполненного buy-side движения. К моменту входа покупатель был истощён, сверху появилась реакция продавца, а fresh bullish continuation не появился. Цена дала pullback/rejection и дошла до SL.',
            analysis_add=['перед входом уже был buy-side impulse/pump', 'после входа не появился fresh bullish displacement'],
            happened=['покупатель не дал нового continuation после входа', 'цена не закрепилась выше entry/retest зоны', 'появился seller rejection против LONG', 'TP1 не был нормально поставлен под угрозу'],
            visible=['перед входом уже прошёл сильный pump', 'LONG открыт поздно, ближе к post-pump high', 'сверху появилась seller reaction area', 'SL попал в нормальный pullback/retest после роста', pos_line],
            secondary=['Late entry after pump', 'Buyer exhaustion', 'Seller rejection from high', 'No fresh bullish displacement'],
            improve=['после сильного pump ждать discount re-entry ниже', 'не лонговать прямо возле post-pump high', 'требовать новый bullish displacement после pullback/retest'],
            evidence=bool(late_or_high_long and (normal_pullback or no_tp or highish or tight_space)))
        add(8.4 + (2 if fast else 0) + (1 if normal_pullback else 0) + (1 if tight_space else 0) + (1 if supply_blocks else 0), 'long_into_supply_tight_sl_fast',
            primary='LONG under resistance + SL inside normal pullback',
            scenario=f'LONG был открыт под/рядом с resistance/seller reaction area: {supply_desc}. Upside до TP1 был ограничен, а SL стоял внутри обычного pullback/retest. Поэтому цена дала откат/rejection и выбила SL до нормального bullish continuation.',
            analysis_add=['SL был слишком близко для такого pullback/retest', 'upside до TP1 был ограничен supply/resistance зоной'],
            happened=['покупатель не смог сразу пробить ближний resistance/supply', 'обычный pullback/retest оказался сильнее SL', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут до fresh bullish continuation'],
            visible=[f'сверху была {supply_desc}', 'LONG открыт рядом с local high / resistance', 'SL находился внутри нормального pullback/retest-движения', 'TP1 был рядом/за реакционной зоной, поэтому путь не был чистым', 'после входа не появился clean bullish displacement', pos_line],
            secondary=['Long under resistance', 'SL too tight', 'SL inside normal pullback', 'TP1 blocked by resistance', 'Seller rejection reaction'],
            improve=['не лонговать прямо под local high/resistance', 'SL ставить за structural invalidation, а не внутри pullback', 'ждать discount re-entry ниже', 'если RR ломается после нормального SL — пропускать сделку'],
            evidence=bool(fast and no_tp and (supply_seen or supply_blocks or highish) and (risk_pct <= max(tight_sl_pct, 0.45) or normal_pullback or tight_space)))
        long_supply_primary = 'LONG opened too high near supply/resistance / upside path blocked' if highish else 'LONG opened into nearby supply/resistance / upside path blocked'
        long_supply_scenario_prefix = 'слишком высоко, рядом' if highish else 'рядом'
        add(6.5 + (2 if supply_blocks else 0) + (1 if tight_space else 0) + (1 if highish else 0), 'long_into_supply_resistance',
            primary=long_supply_primary,
            scenario=f'LONG был открыт {long_supply_scenario_prefix} с зоной продавца: {supply_desc}. Upside до TP1 был ограничен supply/resistance, поэтому покупатель не смог расширить движение вверх и цена дала rejection/pullback к SL.',
            analysis_add=['upside до TP1 был ограничен supply/resistance зоной'],
            happened=['покупатель не смог пробить ближний resistance/supply', 'цена дала реакцию продавца сверху', 'TP1 не был нормально поставлен под угрозу', 'SL был достигнут после rejection/отката'],
            visible=[f'сверху была {supply_desc}', 'LONG открыт рядом с local high / resistance', 'TP1 был рядом/за реакционной зоной, поэтому путь не был чистым', 'после входа не появился clean bullish displacement', pos_line],
            secondary=['Long too high', 'Long into supply/resistance', 'TP1 blocked by resistance', 'TP1 near supply/resistance reaction area'],
            improve=['не лонговать прямо под local high/resistance', 'ждать discount re-entry ниже', 'проверять supply/resistance на пути к TP1'],
            evidence=bool(long_supply_block_evidence))
        add(5.7 + (1.5 if highish else 0), 'long_premium_bad_location',
            primary='LONG opened in premium / too close to range high',
            scenario='LONG был открыт в верхней части локального диапазона. В таком месте upside уже ограничен, а вероятность rejection/pullback против входа высокая.',
            analysis_add=['entry был в верхней части prior range'],
            happened=['цена не дала продолжение вверх из premium', 'продавец быстро вернул цену вниз', 'SL был достигнут после rejection'],
            visible=['вход расположен ближе к range high, а не в discount', 'для LONG location был слабый', 'сверху оставалось мало пространства до реакции продавцов', pos_line],
            secondary=['Bad premium location for LONG', 'Near range high', 'Seller reaction risk'],
            improve=['для LONG искать discount-зону ниже', 'не открывать long в верхней трети диапазона'],
            evidence=highish and no_tp)
        add(6.0 + (2 if bearish_ctx else 0) + (1 if demand_seen else 0), 'long_against_bearish_structure_failed_demand',
            primary='LONG against bearish structure / failed demand reaction',
            scenario=f'LONG был открыт от buyer/demand идеи ({demand_desc}), но рынок уже показывал bearish breakdown или sell-side pressure. Bullish acceptance не закрепился, поэтому продавец выбил SL.',
            analysis_add=['bearish context / breakdown перед LONG', 'bullish displacement после входа отсутствовал'],
            happened=['buyer zone не дала сильной реакции', 'цена не закрепилась выше entry/retest зоны', 'после входа не было clean bullish displacement', 'SL был достигнут после sell-side pressure'],
            visible=['перед входом был sell-side impulse / bearish context', 'LONG был взят без подтверждённого bullish reclaim', 'сверху сохранялся seller pressure / supply', 'TP1 не был нормально поставлен под угрозу', pos_line],
            secondary=['Long against bearish structure', 'Failed demand reaction', 'No bullish reclaim'],
            improve=['не брать LONG против bearish 5m/15m без bullish reclaim', 'ждать закрытие выше supply/resistance', 'требовать новый bullish displacement после retest'],
            evidence=bool(bearish_ctx and not supply_blocks and (weak_follow or no_tp)))
        add(5.0 + (1.5 if breakout_route else 0), 'breakout_trap_rejected',
            primary='Breakout trap / level lost back',
            scenario='Пробой вверх не получил продолжения: после входа цена вернулась обратно ниже уровня/entry, из-за этого LONG превратился в breakout trap и закрылся по SL.',
            analysis_add=['breakout был lost back', 'bullish continuation не подтвердился'],
            happened=['цена не удержалась выше пробитой зоны', 'возврат вниз отменил long-идею', 'SL был достигнут после потери уровня'],
            visible=['после breakout цена вернулась обратно в range/ниже уровня', 'нет закрепления выше retest-зоны', pos_line],
            secondary=['Breakout trap', 'Level lost back', 'No bullish acceptance'],
            improve=['после BOS вверх ждать закрепление выше уровня', 'не входить, если потеря уровня появляется сразу после trigger'],
            evidence=bool((b('level_reclaimed_back') or b('reclaim_lost_back')) and breakout_route and no_tp))
        add(4.8 + (1.5 if reclaim_route else 0), 'liquidity_reclaim_failed_long',
            primary='Liquidity reclaim failed for LONG',
            scenario='Логика liquidity reclaim не подтвердилась: цена не удержала bullish reclaim и вернулась против позиции. Поэтому покупатель не получил continuation, а SL был выбит.',
            analysis_add=['liquidity reclaim не подтвердился'],
            happened=['reclaim/снятие ликвидности не дало продолжения вверх', 'цена быстро вернулась ниже entry/level', 'SL был достигнут после обратного reclaim'],
            visible=['после sweep/reclaim нет bullish acceptance', 'цена вернулась против LONG', pos_line],
            secondary=['Liquidity reclaim failed', 'Sweep without continuation', 'Reclaim lost back'],
            improve=['для liquidity reclaim ждать close и continuation выше reclaim-level', 'не лонговать, если цена быстро возвращается ниже reclaim-level'],
            evidence=bool((b('reclaim_not_confirmed') or b('reclaim_lost_back')) and reclaim_route))
        add(4.9 + (1.5 if ob_fvg_route else 0), 'long_ob_fvg_zone_failed',
            primary='OB/FVG buyer zone failed to hold price',
            scenario='LONG был построен от OB/FVG зоны, но реакция покупателя была слабой. Цена не удержалась выше зоны/середины зоны и пошла к SL.',
            analysis_add=['OB/FVG reaction была слабой', 'zone hold не подтвердился'],
            happened=['buyer zone не дала сильного reclaim', 'цена вернулась в зону/ниже midpoint', 'continuation вверх не появился'],
            visible=[f'{demand_desc} не дала clean reclaim', 'price action после входа не удержался выше buyer zone', pos_line],
            secondary=['OB/FVG reaction failed', 'Zone not held', 'Weak buyer reclaim'],
            improve=['не входить по OB/FVG без сильной реакции', 'требовать reclaim candle и continuation от зоны'],
            evidence=bool(ob_fvg_route and (b('zone_midpoint_lost') or b('entry_too_deep_into_zone') or b('htf_zone_not_held') or weak_confirm) and no_tp))
        add(4.7 + (1.5 if origin_route else 0), 'long_origin_not_held',
            primary='Displacement origin failed / origin was not held',
            scenario='Origin setup не удержал уровень: после входа цена вернулась против long-идеи и сломала origin/validation area. Поэтому setup стал invalid.',
            analysis_add=['origin level не удержался', 'displacement после origin был слабый'],
            happened=['origin-зона не дала continuation', 'цена вернулась против входа', 'SL был достигнут после провала origin'],
            visible=['displacement origin не удержал price action', 'нет нового bullish expansion от origin', pos_line],
            secondary=['Origin not held', 'Displacement too weak', 'Origin setup failed'],
            improve=['для origin setup требовать сильный displacement от origin', 'не входить, если цена сразу возвращается в origin-зону'],
            evidence=bool(origin_route and (b('origin_not_held') or b('displacement_too_weak') or weak_confirm)))
        add(4.7 + (1.5 if dual_fvg_route else 0), 'long_stacked_fvg_failed',
            primary='Stacked/Dual FVG confluence failed',
            scenario='Stacked/Dual FVG confluence не удержала цену. Вместо continuation вверх цена вернулась против входа, поэтому LONG закрылся по SL.',
            analysis_add=['stacked FVG confluence не удержал цену'],
            happened=['FVG stack не дал сильной реакции покупателя', 'цена вернулась против long', 'SL был достигнут после слабого continuation'],
            visible=['dual/stacked FVG не дал чистого reclaim', 'цена не удержалась выше FVG confluence', pos_line],
            secondary=['Stacked FVG failed', 'Confluence failed', 'Weak FVG reaction'],
            improve=['не брать dual FVG, если первая реакция слабая', 'требовать continuation candle после FVG retest'],
            evidence=bool(dual_fvg_route and (b('stacked_fvg_not_held') or b('confluence_failed') or weak_confirm)))
        add(4.6, 'htf_bearish_context_against_long',
            primary='5m LONG was against 15m/30m bearish context',
            scenario='Младший LONG был открыт против старшего bearish context. На HTF не было подтверждённого bullish shift, поэтому продавецкая структура быстро вернула цену к SL.',
            analysis_add=['HTF context был против LONG'],
            happened=['5m signal не получил поддержку от 15m/30m', 'цена вернулась в сторону HTF контекста', 'SL был достигнут после sell-side pressure'],
            visible=['15m/30m context не подтверждал bullish continuation', 'LONG был против старшей структуры', pos_line],
            secondary=['HTF bearish context', 'LTF signal against HTF', 'No HTF bullish shift'],
            improve=['для LONG требовать хотя бы neutral/bullish 15m/30m', 'не брать 5m long против явного HTF sell-side pressure'],
            evidence=bool(contains_any(structure_text, ('15m bearish', '30m bearish', 'htf bearish')) and no_tp))

    if not candidates:
        return {}

    # Prefer the real chart/market cause over generic timing/SL labels.
    # Fast SL is a symptom. It can explain urgency, but the main card reason
    # should be location/structure/market/confirmation/TP path whenever such
    # evidence exists.
    location_keys = ('late_', 'short_into', 'long_into', 'discount', 'premium', 'against_', 'htf_', 'break', 'liquidity', 'ob_fvg', 'origin', 'stacked', 'entry_', 'bad_', 'tp1_', 'weak_', 'range_', 'volume_', 'counter_', 'market_')
    generic_timing_keys = {'fast_sl_no_excursion', 'sl_inside_normal_pullback', 'sl_too_tight', 'sl_inside_normal_bounce'}
    boosted = []
    for score, key, payload in candidates:
        if key.startswith(location_keys):
            score += 0.35
        if key in generic_timing_keys:
            score -= 1.50
        boosted.append((score, key, payload))
    boosted.sort(key=lambda x: x[0], reverse=True)

    real_candidates = [(score, key, payload) for score, key, payload in boosted if key not in generic_timing_keys]
    if real_candidates:
        best_score, best_key, best_payload = real_candidates[0]
    else:
        best_score, best_key, best_payload = boosted[0]
    best = dict(best_payload)

    if fast and best_key not in generic_timing_keys:
        fast_label = 'Fast SL / quick invalidation'
        best['secondary_labels'] = list(dict.fromkeys(list(best.get('secondary_labels') or []) + [fast_label]))
        if dline and dline not in list(best.get('analysis_lines') or []):
            best['analysis_lines'] = [dline] + list(best.get('analysis_lines') or [])

    # Merge evidence from next strongest distinct causes. Keep main reason clean and
    # only show additional labels, not full paragraphs, so the Telegram card stays readable.
    extra_secondary: list[str] = []
    extra_analysis: list[str] = []
    for score, key, payload in boosted[1:7]:
        if score < 3.5:
            continue
        for item in list(payload.get('secondary_labels') or [])[:2]:
            if item and item not in extra_secondary:
                extra_secondary.append(item)
        for item in list(payload.get('analysis_lines') or [])[-2:]:
            if item and item not in extra_analysis and item not in best.get('analysis_lines', []):
                # Keep only compact numeric/fact lines, not repeated prose.
                if any(token in item for token in ('%', 'мин', 'отсутствовал', 'ограничен', 'слаб')):
                    extra_analysis.append(item)
    if extra_secondary:
        best['secondary_labels'] = list(dict.fromkeys(list(best.get('secondary_labels') or []) + extra_secondary))[:10]
    if extra_analysis:
        best['analysis_lines'] = list(dict.fromkeys(list(best.get('analysis_lines') or []) + extra_analysis))[:8]

    for fld in ('analysis_lines', 'happened_lines', 'visible_lines', 'secondary_labels', 'improve_labels'):
        best[fld] = [x for x in list(dict.fromkeys([str(v).strip() for v in list(best.get(fld) or []) if str(v).strip()]))]
        if fld == 'analysis_lines':
            best[fld] = _loss_card_normalize_analysis_lines(best[fld])[:8]
        if fld in ('happened_lines', 'visible_lines'):
            best[fld] = best[fld][:8]
        if fld == 'secondary_labels':
            best[fld] = best[fld][:8]
        if fld == 'improve_labels':
            best[fld] = best[fld][:6]
    best['visible_lines'] = _loss_card_normalize_visible_position_lines(best.get('visible_lines') or [], analysis)
    return best

def _loss_card_forensic_payload(src: dict, loss_diag: dict, *, after_tp1: bool = False) -> dict:
    """Build human forensic sections exactly for Telegram LOSS report card."""
    src = dict(src or {})
    loss_diag = dict(loss_diag or {})
    analysis = dict(loss_diag.get('close_analysis_json') or {})
    side = str(src.get('side') or analysis.get('side') or '').upper().strip() or 'LONG'
    try:
        _loss_card_apply_price_location_override(src, analysis, side=side)
    except Exception:
        pass
    code = str(loss_diag.get('primary_reason_code') or loss_diag.get('reason_code') or '').strip()
    tiny_space_only = _loss_card_tiny_space_only(analysis)
    if (not tiny_space_only) and side == 'LONG' and bool(analysis.get('entry_into_supply') or analysis.get('entry_into_overhead_supply') or analysis.get('long_below_strong_high')):
        code = 'long_below_strong_high'
    elif (not tiny_space_only) and side == 'SHORT' and bool(analysis.get('entry_into_demand') or analysis.get('short_above_weak_low')):
        code = 'short_above_weak_low'
    primary_text = _loss_card_short_reason_title(code, str(loss_diag.get('primary_reason_text') or loss_diag.get('reason_text') or ''))

    short_demand_raw = side == 'SHORT' and (code in {'short_above_weak_low', 'entry_into_demand'} or ((not tiny_space_only) and bool(analysis.get('short_above_weak_low') or analysis.get('entry_into_demand') or analysis.get('demand_near_tfs'))))
    short_failed_supply_raw = side == 'SHORT' and (code in {'short_failed_supply_bullish_structure', 'failed_supply_reaction'} or bool(analysis.get('short_failed_supply_bullish_structure') or analysis.get('failed_supply_reaction')))
    demand_dominates = bool(short_demand_raw and not analysis.get('supply_near_tfs'))
    short_demand = bool(short_demand_raw and (demand_dominates or not short_failed_supply_raw))
    short_failed_supply = bool(short_failed_supply_raw and not short_demand)
    long_supply = side == 'LONG' and (code in {'long_below_strong_high', 'entry_into_supply', 'entry_into_overhead_supply'} or ((not tiny_space_only) and bool(analysis.get('long_below_strong_high') or analysis.get('entry_into_supply') or analysis.get('entry_into_overhead_supply') or analysis.get('supply_near_tfs') or analysis.get('overhead_bearish_fvg'))))
    try:
        _entry_pos_payload = float(analysis.get('entry_position_in_prior_range') or analysis.get('entry_position_in_range') or 0.5)
    except Exception:
        _entry_pos_payload = 0.5
    _supply_dominates_payload = bool(long_supply and _entry_pos_payload >= 0.48)
    long_failed_demand = side == 'LONG' and not _supply_dominates_payload and (code in {'long_failed_demand_bearish_structure', 'failed_demand_reaction'} or bool(analysis.get('long_failed_demand_bearish_structure') or analysis.get('failed_demand_reaction')))

    duration_min = None
    try:
        opened = _parse_iso_dt(src.get('opened_at'))
        closed = _parse_iso_dt(src.get('closed_at'))
        if opened and closed and closed > opened:
            duration_min = int(round((closed - opened).total_seconds() / 60.0))
    except Exception:
        duration_min = None
    if duration_min is None:
        try:
            if analysis.get('duration_min') is not None:
                duration_min = int(float(analysis.get('duration_min') or 0))
        except Exception:
            duration_min = None

    if after_tp1:
        return _loss_card_after_tp1_payload(src, analysis, side=side, duration_min=duration_min)

    analysis_lines = []
    if duration_min is not None and duration_min > 0:
        analysis_lines.append(f"вход → SL: {duration_min} мин")

    if short_demand:
        variant = _loss_card_exact_location_variant(side, analysis, duration_min)
        scenario_text = str(variant.get('scenario_text') or '').strip()
        analysis_lines.extend([str(x) for x in list(variant.get('analysis_add') or []) if str(x).strip()])
        if duration_min is not None and duration_min > 12:
            happened_lines = [
                'цена не смогла продолжить sell-side движение',
                'buyer zone удержала движение снизу',
                'продавец не получил displacement вниз',
                'произошёл bounce против позиции',
            ]
        else:
            happened_lines = [
                'цена не смогла продолжить sell-side движение',
                'buyer zone сразу начала удерживать цену',
                'продавец не получил displacement вниз',
                'произошёл bounce против позиции',
            ]
        visible_lines = [str(x) for x in list(variant.get('visible') or []) if str(x).strip()]
        if analysis.get('demand_near_tfs'):
            try:
                visible_lines.append('underlying demand / green FVG виден на TF: ' + '/'.join([str(x) for x in list(analysis.get('demand_near_tfs') or [])]))
            except Exception:
                pass
        try:
            cs = float(analysis.get('clean_space_to_tp1_pct') or 0.0)
            if cs > 0:
                visible_lines.append(f'расстояние до TP1 было около {cs:.2f}% и упиралось в demand')
        except Exception:
            pass
        secondary_labels = [str(x) for x in list(variant.get('secondary') or []) if str(x).strip()]
        improve_labels = [str(x) for x in list(variant.get('improve') or []) if str(x).strip()]
        primary_text = str(variant.get('primary_text') or 'Short entered directly into demand')
    elif short_failed_supply:
        scenario_text = (
            'SHORT был открыт после того, как рынок уже перешёл в bullish-структуру. '
            'Цена находилась рядом с seller zone, но снизу уже давили bullish FVG / demand зоны. '
            'Продавец не смог сделать bearish reclaim и continuation, поэтому supply быстро сломался.'
        )
        analysis_lines.extend([
            'bearish follow-through отсутствовал',
            'supply reaction не появилась',
            'цена сразу ушла против позиции',
            'TP1 не был нормально поставлен под угрозу',
        ])
        happened_lines = [
            'цена не смогла закрепиться ниже зоны входа',
            'seller zone не удержала',
            'после входа не было displacement вниз',
            'покупатель продолжил buy-side движение',
            'SL был выбит почти сразу',
        ]
        visible_lines = [
            'SHORT открыт после bullish BOS / CHoCH вверх',
            'снизу buyer pressure / demand/support zones',
            'структура уже делала higher low / higher high',
            'вход был рядом с supply, но без подтверждения bearish reclaim',
            'цена стояла под buyer pressure',
            'supply/resistance сверху был уже слабый',
            'RR выглядел формально нормальный, но location был плохой',
            'это не чистый SHORT, а попытка шортить слабую supply против bullish-структуры',
        ]
        if analysis.get('bullish_context_before_entry'):
            visible_lines.insert(0, 'перед входом уже был bullish reclaim / impulse вверх')
            visible_lines.insert(1, 'bearish reclaim перед SHORT не появился')
        secondary_labels = [
            'Short against bullish structure',
            'Failed supply reaction',
            'No bearish displacement',
            'Underlying bullish FVG',
            'Weak follow-through',
            'TP1 was never threatened',
            'Invalidation happened too fast',
            'Continuation missing',
        ]
        improve_labels = [
            'не брать SHORT после bullish BOS без bearish reclaim',
            'требовать закрытие ниже локального FVG/demand',
            'ждать bearish displacement после supply',
            'не шортить supply, если снизу stacked bullish FVG',
            'для SHORT требовать structure_15m/30m bearish',
            'если SL выбивается за 1–3 минуты — считать setup trap/catching falling knife',
        ]
        primary_text = 'Short into bullish BOS / failed supply'
    elif long_failed_demand:
        scenario_text = (
            'LONG был открыт после того, как рынок уже перешёл в bearish-структуру. '
            'Цена находилась рядом с buyer zone, но сверху уже давили bearish FVG / supply зоны. '
            'Покупатель не смог сделать reclaim и continuation, поэтому demand быстро сломался.'
        )
        analysis_lines.extend([
            'bullish follow-through отсутствовал',
            'demand reaction не появилась',
            'цена сразу ушла против позиции',
            'TP1 не был нормально поставлен под угрозу',
        ])
        happened_lines = [
            'цена не смогла закрепиться выше зоны входа',
            'buyer zone не удержала',
            'после входа не было displacement вверх',
            'продавец продолжил sell-side движение',
            'SL был выбит почти сразу',
        ]
        visible_lines = [
            'LONG открыт после bearish BOS / CHoCH вниз',
            'сверху seller pressure / resistance zones',
            'структура уже делала lower high / lower low',
            'вход был слишком низко, но без подтверждения reclaim',
            'цена стояла под sell pressure',
            'demand/support снизу был уже слабый',
            'RR выглядел формально нормальный, но location был плохой',
            'это не чистый breakout, а попытка купить слабый demand против структуры',
        ]
        if analysis.get('bearish_context_before_entry'):
            visible_lines.insert(0, 'перед входом уже был bearish displacement / BOS вниз')
            visible_lines.insert(1, 'bullish reclaim перед LONG не появился')
        secondary_labels = [
            'Long against bearish structure',
            'Failed demand reaction',
            'No bullish displacement',
            'Overhead bearish FVG',
            'Weak follow-through',
            'TP1 was never threatened',
            'Invalidation happened too fast',
            'Continuation missing',
        ]
        improve_labels = [
            'не брать LONG после bearish BOS без reclaim',
            'требовать закрытие выше локального FVG/supply',
            'ждать bullish displacement после demand',
            'не покупать demand, если сверху stacked bearish FVG',
            'для LONG требовать structure_15m/30m bullish',
            'если SL выбивается за 1–3 минуты — считать setup trap/catching falling knife',
        ]
        primary_text = 'Long into bearish BOS / failed demand'
    elif long_supply:
        variant = _loss_card_exact_location_variant(side, analysis, duration_min)
        scenario_text = str(variant.get('scenario_text') or '').strip()
        analysis_lines.extend([str(x) for x in list(variant.get('analysis_add') or []) if str(x).strip()])
        if duration_min is not None and duration_min > 12:
            happened_lines = [
                'цена не смогла продолжить buy-side движение',
                'seller zone удержала движение сверху',
                'покупатель не получил displacement вверх',
                'произошёл rejection против позиции',
            ]
        else:
            happened_lines = [
                'цена не смогла продолжить buy-side движение',
                'seller zone сразу начала удерживать цену',
                'покупатель не получил displacement вверх',
                'произошёл rejection против позиции',
            ]
        visible_lines = [str(x) for x in list(variant.get('visible') or []) if str(x).strip()]
        if analysis.get('supply_near_tfs'):
            try:
                visible_lines.append('overhead supply / red FVG виден на TF: ' + '/'.join([str(x) for x in list(analysis.get('supply_near_tfs') or [])]))
            except Exception:
                pass
        try:
            cs = float(analysis.get('clean_space_to_tp1_pct') or 0.0)
            if cs > 0:
                visible_lines.append(f'расстояние до TP1 было около {cs:.2f}% и упиралось в supply')
        except Exception:
            pass
        secondary_labels = [str(x) for x in list(variant.get('secondary') or []) if str(x).strip()]
        improve_labels = [str(x) for x in list(variant.get('improve') or []) if str(x).strip()]
        primary_text = str(variant.get('primary_text') or 'Long entered directly into supply')
    else:
        scenario_text = str(loss_diag.get('scenario_text') or '').strip()
        analysis_lines.extend([str(x) for x in list(loss_diag.get('analysis_lines') or []) if str(x).strip()])
        happened_lines = [str(x) for x in list(loss_diag.get('what_happened_lines') or []) if str(x).strip()]
        visible_lines = [str(x) for x in list(loss_diag.get('chart_visible_lines') or []) if str(x).strip()]
        secondary_labels = [str(x) for x in list(loss_diag.get('secondary_reason_labels') or []) if str(x).strip()]
        improve_labels = [str(x) for x in list(loss_diag.get('improve_labels') or []) if str(x).strip()]

    contextual_payload = _loss_card_non_template_context_payload(src, analysis, side=side, code=code, duration_min=duration_min)
    if contextual_payload:
        primary_text = str(contextual_payload.get('primary_text') or primary_text).strip()
        scenario_text = str(contextual_payload.get('scenario_text') or scenario_text).strip()
        analysis_lines = [str(x) for x in list(contextual_payload.get('analysis_lines') or []) if str(x).strip()]
        happened_lines = [str(x) for x in list(contextual_payload.get('happened_lines') or []) if str(x).strip()]
        visible_lines = [str(x) for x in list(contextual_payload.get('visible_lines') or []) if str(x).strip()]
        secondary_labels = [str(x) for x in list(contextual_payload.get('secondary_labels') or []) if str(x).strip()]
        improve_labels = [str(x) for x in list(contextual_payload.get('improve_labels') or []) if str(x).strip()]

    ranked_payload = _loss_card_ranked_reason_payload(src, analysis, side=side, duration_min=duration_min)
    if ranked_payload:
        primary_text = str(ranked_payload.get('primary_text') or primary_text).strip()
        scenario_text = str(ranked_payload.get('scenario_text') or scenario_text).strip()
        analysis_lines = [str(x) for x in list(ranked_payload.get('analysis_lines') or []) if str(x).strip()]
        happened_lines = [str(x) for x in list(ranked_payload.get('happened_lines') or []) if str(x).strip()]
        visible_lines = [str(x) for x in list(ranked_payload.get('visible_lines') or []) if str(x).strip()]
        secondary_labels = [str(x) for x in list(ranked_payload.get('secondary_labels') or []) if str(x).strip()]
        improve_labels = [str(x) for x in list(ranked_payload.get('improve_labels') or []) if str(x).strip()]

    if not scenario_text:
        scenario_text = str(loss_diag.get('primary_reason_text') or loss_diag.get('reason_text') or '').strip()
    if not happened_lines:
        happened_lines = ['после входа не появилось нормальное continuation-движение', 'цена ушла против позиции до SL']
    if not analysis_lines:
        analysis_lines = [str(x) for x in list(loss_diag.get('analysis_lines') or []) if str(x).strip()]
    if not visible_lines:
        visible_lines = [str(x) for x in list(loss_diag.get('chart_visible_lines') or []) if str(x).strip()]
    if not visible_lines:
        if side == 'SHORT':
            visible_lines = [
                'SHORT не получил clean sell-side continuation после входа',
                'цена быстро вернулась против направления сделки',
                'bearish displacement после входа не появился',
                'TP1 не был нормально поставлен под угрозу',
            ]
        else:
            visible_lines = [
                'LONG не получил clean buy-side continuation после входа',
                'цена быстро вернулась против направления сделки',
                'bullish displacement после входа не появился',
                'TP1 не был нормально поставлен под угрозу',
            ]
    if not secondary_labels:
        secondary_labels = [str(x) for x in list(loss_diag.get('secondary_reason_labels') or []) if str(x).strip()]
    if not improve_labels:
        improve_labels = [str(x) for x in list(loss_diag.get('improve_labels') or []) if str(x).strip()]

    visible_lines = _loss_card_normalize_visible_position_lines(visible_lines, analysis)

    return {
        'primary_text': primary_text,
        'scenario_text': scenario_text,
        'analysis_lines': _loss_card_normalize_analysis_lines(list(dict.fromkeys([x for x in analysis_lines if str(x).strip()]))),
        'what_happened_lines': list(dict.fromkeys([x for x in happened_lines if str(x).strip()])),
        'chart_visible_lines': list(dict.fromkeys([x for x in visible_lines if str(x).strip()])),
        'secondary_reason_labels': list(dict.fromkeys([x for x in secondary_labels if str(x).strip()])),
        'improve_labels': list(dict.fromkeys([x for x in improve_labels if str(x).strip()])),
    }

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

    price_values_for_precision = [entry, sl, tp1]
    if has_tp2 and (st != 'LOSS' or after_tp1):
        price_values_for_precision.append(tp2)
    if after_tp1 and be_price > 0:
        price_values_for_precision.append(be_price)
    price_precision = _report_price_precision(price_values_for_precision)
    price_lines = []
    if entry > 0:
        price_lines.append(f"💰 Вход: {_fmt_report_price(entry, price_precision)}")
    if sl > 0:
        if after_tp1 and be_price > 0:
            price_lines.append(f"🛑 SL исходный: {_fmt_report_price(sl, price_precision)}")
        else:
            price_lines.append(f"🛑 SL: {_fmt_report_price(sl, price_precision)}")
    if tp1 > 0:
        price_lines.append(f"🎯 TP1: {_fmt_report_price(tp1, price_precision)}")
    if has_tp2 and (st != 'LOSS' or after_tp1):
        price_lines.append(f"🚀 TP2: {_fmt_report_price(tp2, price_precision)}")
    if after_tp1 and be_price > 0:
        price_lines.append(f"🛡 Активный SL/BE: {_fmt_report_price(be_price, price_precision)}")
    price_block = "\n".join(price_lines).strip() or "—"

    # Show the setup on LOSS cards by default too.
    # Set LOSS_CARD_SHOW_SETUP=0 only if you intentionally want to hide it.
    _show_loss_setup = str(os.getenv('LOSS_CARD_SHOW_SETUP', '1')).strip().lower() not in ('0', 'false', 'no', 'off')
    setup_block = f"🧭 Smart-setup: {setup_label}\n" if (setup_label and (st != 'LOSS' or _show_loss_setup)) else ""
    primary_block = ""
    scenario_block = ""
    analysis_block = ""
    happened_block = ""
    visible_block = ""
    secondary_block = ""
    missed_block = ""
    improve_block = ""
    if st == 'LOSS':
        card_payload = _loss_card_forensic_payload(row, loss_diag, after_tp1=after_tp1)
        analysis_lines = list(card_payload.get('analysis_lines') or [])
        happened_lines = list(card_payload.get('what_happened_lines') or [])
        visible_lines = list(card_payload.get('chart_visible_lines') or [])
        secondary_labels = list(card_payload.get('secondary_reason_labels') or [])
        improve_labels = list(card_payload.get('improve_labels') or [])
        primary_text = str(card_payload.get('primary_text') or reason).strip()
        scenario_text = str(card_payload.get('scenario_text') or '').strip()
        if primary_text:
            primary_block = "🧠 Главная причина:\n" + primary_text + "\n\n"
        if scenario_text:
            scenario_title = "🎭 Сценарий закрытия остатка:" if after_tp1 else "🎭 Сценарий потери:"
            scenario_block = scenario_title + "\n" + scenario_text + "\n\n"
        if analysis_lines:
            analysis_block = "📉 Candle-анализ:\n" + "\n".join([f"• {x}" for x in analysis_lines if x]) + "\n\n"
        if happened_lines:
            happened_title = "📉 Что произошло после TP1:" if after_tp1 else "📉 Что произошло после входа:"
            happened_block = happened_title + "\n" + "\n".join([f"• {x}" for x in happened_lines if x]) + "\n\n"
        if visible_lines:
            visible_block = "👁 Что видно на графике:\n" + "\n".join([f"• {x}" for x in visible_lines if x]) + "\n\n"
        if secondary_labels:
            secondary_block = "🧩 Доп. причины:\n" + "\n".join([f"• {x}" for x in secondary_labels if x]) + "\n\n"
        if str(os.getenv('LOSS_CARD_SHOW_MISSED', '0')).lower() in ('1','true','yes','on'):
            missed_labels = list(loss_diag.get('missed_labels') or [])
            if missed_labels:
                missed_block = "⚠️ Что бот пропустил:\n" + "\n".join([f"• {x}" for x in missed_labels if x]) + "\n\n"
        if improve_labels:
            improve_block = "🛠 Что улучшить:\n" + "\n".join([f"• {x}" for x in improve_labels if x]) + "\n\n"

    fallback_primary_block = primary_block or (
        "🧠 Причина:\n" + (loss_diag.get('reason_text') or reason) + "\n\n"
    )

    return (
        f"{_report_close_emoji(st, after_tp1=after_tp1, pnl_total_pct=pnl_total_pct)} {symbol} | {market} | {side}\n\n"
        f"📌 Статус: {_report_close_status(st, after_tp1=after_tp1, pnl_total_pct=pnl_total_pct)}\n"
        f"📊 Итог PnL: {_report_pnl_pct(pnl_total_pct)}\n"
        f"📊 Risk/Reward до TP1: 1 : {rr}\n"
        f"{setup_block}"
        f"{fallback_primary_block}"
        f"{scenario_block}"
        f"{analysis_block}"
        f"{happened_block}"
        f"{visible_block}"
        f"{secondary_block}"
        f"{missed_block}"
        f"{improve_block}"
        f"──────────────────\n\n"
        f"{price_block}\n\n"
        f"🕒 Открыта: {opened_time} ({tz_label})\n"
        f"🕒 Закрыта: {closed_time} ({tz_label})\n\n"
        f"{_symbol_hashtag(symbol)}"
    ).strip()


def _loss_diag_close_analysis_is_thin(value) -> bool:
    """True when close_analysis_json lacks real chart-location context.

    Generic post-entry metrics such as mfe_pct / mae_pct / tp1_threatened are not
    enough. Those metrics tell that the trade failed, but not *where* it failed on
    the chart. Older rows with only those fields produced repeated generic cards:
    "Breakout trap / no continuation". Treat them as thin so the sender rebuilds
    close_analysis_json from candle snapshots and can detect supply/demand blockers.
    """
    try:
        a = dict(value or {})
    except Exception:
        return True
    if not a:
        return True

    location_keys = (
        'supply_near_tfs', 'demand_near_tfs',
        'nearest_supply_zone', 'nearest_demand_zone',
        'supply_zone_relation', 'demand_zone_relation',
        'supply_zone_blocks_tp1', 'demand_zone_blocks_tp1',
        'entry_into_supply', 'entry_into_overhead_supply', 'entry_into_demand',
        'long_below_strong_high', 'short_above_weak_low',
        'entry_near_opposite_zone', 'overhead_bearish_fvg', 'underlying_bullish_fvg',
        'long_failed_demand_bearish_structure', 'failed_demand_reaction',
        'short_failed_supply_bullish_structure', 'failed_supply_reaction',
        'long_bearish_continuation_under_supply', 'short_against_bullish_structure',
        'range_high_blocks_tp1', 'range_low_blocks_tp1',
        'location_override_reason',
        'entry_position_in_prior_range', 'entry_position_in_range',
        'local_pre_move_6_pct', 'local_pre_move_12_pct', 'local_pre_move_24_pct',
        'pre_entry_move_pct', 'mfe_pct', 'mae_pct', 'first_push_pct', 'against_move_first3_pct',
    )
    return not any(a.get(k) not in (None, '', [], {}, False) for k in location_keys)


async def _send_closed_signal_report_card(t: dict, *, final_status: str, pnl_total_pct: float, closed_at: dt.datetime | None = None) -> bool:
    """Send one final outcome card and persist the send status.

    Returns True when at least one configured report chat received the card.
    The function is intentionally called only from final close paths and from
    the retry loop for final rows, so TP1/intermediate updates are not sent here.
    """
    row_for_report = dict(t or {})
    try:
        signal_id = int(row_for_report.get('signal_id') or 0)
    except Exception:
        signal_id = 0

    if _report_bot is None:
        logger.warning("[report-bot] not configured; final report skipped signal_id=%s status=%s", signal_id, final_status)
        return False
    try:
        chat_ids = [int(x) for x in (REPORT_BOT_CHAT_IDS or []) if int(x)]
    except Exception:
        chat_ids = []
    if not chat_ids:
        logger.warning("[report-bot] no REPORT_* chat ids/admin ids configured; final report skipped signal_id=%s status=%s", signal_id, final_status)
        return False

    try:
        # Safety net: every LOSS card must have candle-based close_analysis_json.
        # Some manual/legacy close paths can call this sender without precomputed
        # analysis, which previously produced the same generic Breakout trap text.
        if str(final_status or '').upper().strip() == 'LOSS':
            try:
                _always_rebuild = str(os.getenv('LOSS_CARD_ALWAYS_REBUILD_ANALYSIS', '1')).strip().lower() not in ('0', 'false', 'no', 'off')
                if _always_rebuild or _loss_diag_close_analysis_is_thin(row_for_report.get('close_analysis_json')):
                    _rebuilt_analysis = await _loss_diag_build_close_analysis(row_for_report, closed_at=closed_at)
                    if _rebuilt_analysis:
                        row_for_report['close_analysis_json'] = _rebuilt_analysis
            except Exception:
                logger.exception('[report-bot] close analysis backfill failed')
        text = _build_closed_signal_report_card(row_for_report, final_status=str(final_status or "CLOSED"), pnl_total_pct=float(pnl_total_pct or 0.0), closed_at=closed_at)
    except Exception as e:
        logger.exception("[report-bot] failed to build closed signal card")
        if signal_id > 0:
            try:
                await db_store.mark_signal_report_failed(signal_id=signal_id, error=f"build_failed: {type(e).__name__}: {e}")
            except Exception:
                pass
        return False

    sent_any = False
    errors: list[str] = []
    for chat_id in chat_ids:
        try:
            await _report_send_message(int(chat_id), text)
            sent_any = True
        except TelegramRetryAfter as e:
            try:
                wait_s = float(getattr(e, 'retry_after', 1) or 1) + 0.5
                await asyncio.sleep(min(wait_s, 10.0))
                await _report_send_message(int(chat_id), text)
                sent_any = True
            except Exception as e2:
                msg = f"retry_after_failed chat_id={chat_id}: {type(e2).__name__}: {e2}"
                errors.append(msg)
                logger.warning("[report-bot] %s", msg)
        except TelegramForbiddenError as e:
            msg = f"bot blocked/no access chat_id={chat_id}: {e}"
            errors.append(msg)
            logger.warning("[report-bot] %s", msg)
        except TelegramBadRequest as e:
            msg = f"bad request chat_id={chat_id}: {e}"
            errors.append(msg)
            logger.warning("[report-bot] %s", msg)
        except Exception as e:
            msg = f"send failed chat_id={chat_id}: {type(e).__name__}: {e}"
            errors.append(msg)
            logger.exception("[report-bot] send failed chat_id=%s", chat_id)

    if signal_id > 0:
        try:
            if sent_any:
                await db_store.mark_signal_report_sent(signal_id=signal_id)
            else:
                await db_store.mark_signal_report_failed(signal_id=signal_id, error="; ".join(errors) or "send_failed")
        except Exception:
            logger.exception("[report-bot] failed to persist report send state signal_id=%s", signal_id)

    if sent_any:
        logger.info("[report-bot] final card sent signal_id=%s status=%s chats=%s", signal_id, final_status, chat_ids)
    return bool(sent_any)


_CLOSED_SIGNAL_REPORT_RETRY_ENABLED = _env_on("CLOSED_SIGNAL_REPORT_RETRY_ENABLED", "1")
_CLOSED_SIGNAL_REPORT_RETRY_SEC = max(15, int(float(os.getenv("CLOSED_SIGNAL_REPORT_RETRY_SEC", "60") or 60)))
_CLOSED_SIGNAL_REPORT_RETRY_LIMIT = max(1, int(float(os.getenv("CLOSED_SIGNAL_REPORT_RETRY_LIMIT", "10") or 10)))
# Retry a full week by default. If Railway slept or REPORT_* env was fixed later,
# rows from the same day must still be delivered, not silently ignored after 6h.
_CLOSED_SIGNAL_REPORT_RECENT_MIN = max(0, int(float(os.getenv("CLOSED_SIGNAL_REPORT_RECENT_MIN", "10080") or 10080)))


async def closed_signal_report_retry_loop() -> None:
    """Retry final report cards that closed but were not delivered.

    This fixes missed cards after Railway restart, temporary Telegram errors, or
    a late REPORT_BOT_TOKEN / REPORT_CHAT_ID configuration. It only reads rows
    already in final states, so it never sends reports for open positions.
    """
    if not _CLOSED_SIGNAL_REPORT_RETRY_ENABLED:
        logger.info("[report-bot] retry loop disabled")
        return
    logger.info("[report-bot] retry loop started interval=%ss limit=%s recent_min=%s", _CLOSED_SIGNAL_REPORT_RETRY_SEC, _CLOSED_SIGNAL_REPORT_RETRY_LIMIT, _CLOSED_SIGNAL_REPORT_RECENT_MIN)
    while True:
        try:
            _health_mark_ok("closed-signal-report-retry")
            if _report_bot is None or not REPORT_BOT_CHAT_IDS:
                await asyncio.sleep(_CLOSED_SIGNAL_REPORT_RETRY_SEC)
                continue
            rows = await db_store.list_unsent_signal_reports(limit=_CLOSED_SIGNAL_REPORT_RETRY_LIMIT, recent_minutes=_CLOSED_SIGNAL_REPORT_RECENT_MIN)
            for row in rows or []:
                st = str(row.get('status') or 'CLOSED').upper().strip() or 'CLOSED'
                pnl = float(row.get('pnl_total_pct') or 0.0)
                closed_dt = row.get('closed_at')
                if isinstance(closed_dt, str):
                    closed_dt = _parse_iso_dt(closed_dt)
                await _send_closed_signal_report_card(dict(row), final_status=st, pnl_total_pct=pnl, closed_at=closed_dt)
                await asyncio.sleep(0.25)
        except Exception:
            logger.exception("[report-bot] retry loop error")
        await asyncio.sleep(_CLOSED_SIGNAL_REPORT_RETRY_SEC)


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
        await _report_send_message(int(chat_id), payload)
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
        await _report_send_message(int(chat_id), part)


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
        fv = float(v)
        if not math.isfinite(fv):
            return "—"
        precision = _report_price_precision([fv])
        return f"{fv:.{precision}f}".rstrip("0").rstrip(".")
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
            _track_kwargs['entry_snapshot_json'] = await _signal_forensics_entry_snapshot_async(sig)
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

            if _env_on("AUTOTRADE_SIGNAL_EXEC_ENABLED", os.getenv("AUTOTRADE_ENABLED", "0")):
                asyncio.create_task(_run_autotrade(uid, sig))
            else:
                # AUTO-TRADE is disabled: do not spawn per-user executor tasks and do not spam skipped logs.
                pass
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

# Candle catch-up closes signals that hit TP/SL while Railway/app was sleeping or
# while REST price polling temporarily missed the exact touch.
_SIG_CANDLE_CATCHUP_ENABLED = _env_on("SIGNAL_CANDLE_CATCHUP_ENABLED", "1")
_SIG_CANDLE_CATCHUP_INTERVAL_SEC = max(60, int(float(os.getenv("SIGNAL_CANDLE_CATCHUP_INTERVAL_SEC", "300") or 300)))
_SIG_CANDLE_CATCHUP_MAX_CANDLES = max(50, min(1500, int(float(os.getenv("SIGNAL_CANDLE_CATCHUP_MAX_CANDLES", "1000") or 1000))))
_SIG_CANDLE_CATCHUP_LAST: dict[int, float] = {}

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


async def _fetch_mexc_futures_price(symbol: str) -> float:
    """Public last price from MEXC USDT-M futures/contract."""
    raw = str(symbol or "").upper().replace("/", "").replace("-", "").replace(":", "").strip()
    if not raw:
        return 0.0
    contract = raw
    if raw.endswith("USDT") and len(raw) > 4:
        contract = f"{raw[:-4]}_USDT"
    url = "https://contract.mexc.com/api/v1/contract/ticker"
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, params={"symbol": contract}) as r:
                data = await r.json(content_type=None)
        payload = (data or {}).get("data") if isinstance(data, dict) else None
        if isinstance(payload, list) and payload:
            payload = payload[0]
        if isinstance(payload, dict):
            for key in ("lastPrice", "last_price", "fairPrice", "indexPrice"):
                try:
                    px = float(payload.get(key) or 0.0)
                    if px > 0:
                        return px
                except Exception:
                    continue
    except Exception:
        pass
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


async def _fetch_gateio_futures_price(symbol: str) -> float:
    """Public last price from Gate.io USDT futures."""
    contract = _gate_pair(symbol)
    if not contract:
        return 0.0
    url = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, params={"contract": contract}) as r:
                data = await r.json(content_type=None)
        if isinstance(data, list) and data:
            item = data[0] or {}
            for key in ("last", "mark_price", "index_price"):
                try:
                    px = float(item.get(key) or 0.0)
                    if px > 0:
                        return px
                except Exception:
                    continue
    except Exception:
        pass
    return 0.0


def _parse_price_order(env_name: str, default: str) -> list[str]:
    raw = (os.getenv(env_name, default) or default).strip()
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    return parts or [p.strip().lower() for p in default.split(",") if p.strip()]


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

        # Fallback order-based. Include MEXC/Gate futures so newer alts still close
        # and report even when Binance/Bybit/OKX do not list the contract.
        order = _parse_price_order("SIG_PRICE_ORDER_FUTURES", "binance,bybit,okx,mexc,gateio")
        for src in order:
            if src == "binance":
                px = await _safe(_fetch_binance_price(sym, futures=True))
            elif src == "bybit":
                px = await _safe(_fetch_bybit_price(sym, futures=True))
            elif src == "okx":
                px = await _safe(_fetch_okx_price(sym, futures=True))
            elif src == "mexc":
                px = await _safe(_fetch_mexc_futures_price(sym))
            elif src in ("gate", "gateio"):
                px = await _safe(_fetch_gateio_futures_price(sym))
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


def _sig_tf_minutes(tf: str) -> int:
    s = str(tf or "1m").strip().lower()
    m = re.match(r"^(\d+)\s*m$", s)
    if m:
        return max(1, int(m.group(1)))
    m = re.match(r"^(\d+)\s*h$", s)
    if m:
        return max(1, int(m.group(1)) * 60)
    return 1


def _sig_candle_dt(value) -> dt.datetime | None:
    try:
        if value is None:
            return None
        if isinstance(value, dt.datetime):
            return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
        if isinstance(value, (int, float)):
            x = float(value)
            if x > 10_000_000_000:  # ms
                x = x / 1000.0
            return dt.datetime.fromtimestamp(x, tz=dt.timezone.utc)
        s = str(value).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # numeric string timestamp
        if re.fullmatch(r"\d+(?:\.\d+)?", s):
            return _sig_candle_dt(float(s))
        d = dt.datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def _sig_candle_contract(symbol: str, sep: str = "_") -> str:
    raw = str(symbol or "").upper().replace("/", "").replace("-", "").replace("_", "").replace(":", "").strip()
    if raw.endswith("USDT") and len(raw) > 4:
        return f"{raw[:-4]}{sep}USDT"
    return raw


def _sig_norm_candle_rows(rows: list[dict] | None) -> list[dict]:
    out: list[dict] = []
    for r in rows or []:
        try:
            o = float(r.get("open"))
            h = float(r.get("high"))
            l = float(r.get("low"))
            c = float(r.get("close"))
            if not (h > 0 and l > 0 and o > 0 and c > 0):
                continue
            ts = _sig_candle_dt(r.get("ts") or r.get("time") or r.get("timestamp"))
            out.append({"ts": ts, "open": o, "high": h, "low": l, "close": c})
        except Exception:
            continue
    out.sort(key=lambda x: x.get("ts") or dt.datetime.min.replace(tzinfo=dt.timezone.utc))
    return out


async def _fetch_binance_candles_simple(symbol: str, *, futures: bool, interval: str, limit: int) -> list[dict]:
    sym = re.sub(r"[^A-Z0-9]", "", str(symbol or "").upper())
    if not sym:
        return []
    url = "https://fapi.binance.com/fapi/v1/klines" if futures else "https://api.binance.com/api/v3/klines"
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, params={"symbol": sym, "interval": interval, "limit": int(limit)}) as r:
                data = await r.json(content_type=None)
        rows = []
        for it in data or []:
            rows.append({"ts": it[0], "open": it[1], "high": it[2], "low": it[3], "close": it[4]})
        return _sig_norm_candle_rows(rows)
    except Exception:
        return []


async def _fetch_bybit_candles_simple(symbol: str, *, futures: bool, interval: str, limit: int) -> list[dict]:
    sym = re.sub(r"[^A-Z0-9]", "", str(symbol or "").upper())
    if not sym:
        return []
    iv = str(_sig_tf_minutes(interval))
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(
                "https://api.bybit.com/v5/market/kline",
                params={"category": ("linear" if futures else "spot"), "symbol": sym, "interval": iv, "limit": int(limit)},
            ) as r:
                data = await r.json(content_type=None)
        arr = (((data or {}).get("result") or {}).get("list") or [])
        rows = []
        for it in arr or []:
            # [start, open, high, low, close, volume, turnover]
            if not isinstance(it, (list, tuple)) or len(it) < 5:
                continue
            rows.append({"ts": it[0], "open": it[1], "high": it[2], "low": it[3], "close": it[4]})
        return _sig_norm_candle_rows(rows)
    except Exception:
        return []


async def _fetch_okx_candles_simple(symbol: str, *, futures: bool, interval: str, limit: int) -> list[dict]:
    inst = _okx_inst(symbol, futures=futures)
    if not inst:
        return []
    bar = f"{_sig_tf_minutes(interval)}m"
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get("https://www.okx.com/api/v5/market/candles", params={"instId": inst, "bar": bar, "limit": int(limit)}) as r:
                data = await r.json(content_type=None)
        arr = (data or {}).get("data") or []
        rows = []
        for it in arr or []:
            # [ts, o, h, l, c, ...]
            if not isinstance(it, (list, tuple)) or len(it) < 5:
                continue
            rows.append({"ts": it[0], "open": it[1], "high": it[2], "low": it[3], "close": it[4]})
        return _sig_norm_candle_rows(rows)
    except Exception:
        return []


async def _fetch_mexc_candles_simple(symbol: str, *, futures: bool, interval: str, limit: int) -> list[dict]:
    raw = re.sub(r"[^A-Z0-9]", "", str(symbol or "").upper())
    if not raw:
        return []
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            if futures:
                contract = _sig_candle_contract(raw, "_")
                iv_map = {1: "Min1", 3: "Min3", 5: "Min5", 15: "Min15", 30: "Min30", 60: "Min60"}
                iv = iv_map.get(_sig_tf_minutes(interval), "Min1")
                async with s.get(f"https://contract.mexc.com/api/v1/contract/kline/{contract}", params={"interval": iv, "limit": int(limit)}) as r:
                    data = await r.json(content_type=None)
                payload = (data or {}).get("data") if isinstance(data, dict) else None
                rows = []
                if isinstance(payload, dict):
                    times = payload.get("time") or []
                    opens = payload.get("open") or []
                    highs = payload.get("high") or []
                    lows = payload.get("low") or []
                    closes = payload.get("close") or []
                    for i in range(min(len(times), len(opens), len(highs), len(lows), len(closes))):
                        rows.append({"ts": times[i], "open": opens[i], "high": highs[i], "low": lows[i], "close": closes[i]})
                return _sig_norm_candle_rows(rows)
            else:
                async with s.get("https://api.mexc.com/api/v3/klines", params={"symbol": raw, "interval": interval, "limit": int(limit)}) as r:
                    data = await r.json(content_type=None)
                rows = []
                for it in data or []:
                    if not isinstance(it, (list, tuple)) or len(it) < 5:
                        continue
                    rows.append({"ts": it[0], "open": it[1], "high": it[2], "low": it[3], "close": it[4]})
                return _sig_norm_candle_rows(rows)
    except Exception:
        return []


async def _fetch_gateio_candles_simple(symbol: str, *, futures: bool, interval: str, limit: int) -> list[dict]:
    contract = _sig_candle_contract(symbol, "_")
    if not contract:
        return []
    iv = f"{_sig_tf_minutes(interval)}m"
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            if futures:
                url = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
                params = {"contract": contract, "interval": iv, "limit": int(limit)}
            else:
                url = "https://api.gateio.ws/api/v4/spot/candlesticks"
                params = {"currency_pair": contract, "interval": iv, "limit": int(limit)}
            async with s.get(url, params=params) as r:
                data = await r.json(content_type=None)
        rows = []
        for it in data or []:
            if isinstance(it, dict):
                rows.append({
                    "ts": it.get("t") or it.get("time"),
                    "open": it.get("o") or it.get("open"),
                    "high": it.get("h") or it.get("high"),
                    "low": it.get("l") or it.get("low"),
                    "close": it.get("c") or it.get("close"),
                })
            elif isinstance(it, (list, tuple)) and len(it) >= 6:
                # Gate spot: [time, volume, close, high, low, open]
                rows.append({"ts": it[0], "open": it[5], "high": it[3], "low": it[4], "close": it[2]})
        return _sig_norm_candle_rows(rows)
    except Exception:
        return []


async def _fetch_signal_candles(symbol: str, *, market: str, interval: str, limit: int) -> tuple[list[dict], str]:
    """Best-effort OHLC candles for outcome catch-up."""
    sym = str(symbol or "").upper().strip()
    m = (market or "SPOT").upper().strip()
    futures = m == "FUTURES"
    limit = max(10, min(int(limit or 200), _SIG_CANDLE_CATCHUP_MAX_CANDLES))

    # Try the backend candle router first because it already has caching/health.
    try:
        if hasattr(backend, "load_candles"):
            df = await backend.load_candles(sym, interval, m, limit=limit)  # type: ignore[attr-defined]
            if df is not None and not getattr(df, "empty", True):
                rows = []
                for idx, r in df.tail(limit).iterrows():
                    ts_val = None
                    try:
                        ts_val = r.get("time") or r.get("timestamp") or r.get("ts") or r.get("open_time")
                    except Exception:
                        ts_val = None
                    if ts_val is None:
                        try:
                            if isinstance(idx, (dt.datetime, pd.Timestamp)):
                                ts_val = idx
                        except Exception:
                            ts_val = None
                    rows.append({
                        "ts": ts_val,
                        "open": r.get("open"),
                        "high": r.get("high"),
                        "low": r.get("low"),
                        "close": r.get("close"),
                    })
                out = _sig_norm_candle_rows(rows)
                # For catch-up we need timestamps; otherwise candles from before
                # the signal could falsely close a new signal.
                if out and any(c.get("ts") for c in out):
                    return out, "backend.load_candles"
    except Exception:
        pass

    default_order = "binance,bybit,okx,mexc,gateio" if futures else "binance,bybit,okx,mexc,gateio"
    order = _parse_price_order("SIG_CANDLE_ORDER_FUTURES" if futures else "SIG_CANDLE_ORDER_SPOT", default_order)
    for src in order:
        if src == "binance":
            rows = await _fetch_binance_candles_simple(sym, futures=futures, interval=interval, limit=limit)
        elif src == "bybit":
            rows = await _fetch_bybit_candles_simple(sym, futures=futures, interval=interval, limit=limit)
        elif src == "okx":
            rows = await _fetch_okx_candles_simple(sym, futures=futures, interval=interval, limit=limit)
        elif src == "mexc":
            rows = await _fetch_mexc_candles_simple(sym, futures=futures, interval=interval, limit=limit)
        elif src in ("gate", "gateio"):
            rows = await _fetch_gateio_candles_simple(sym, futures=futures, interval=interval, limit=limit)
        else:
            rows = []
        if rows:
            return rows, src
    return [], ""


def _sig_catchup_params(opened_at_dt: dt.datetime | None, now: dt.datetime) -> tuple[str, int]:
    try:
        age_min = int(max(1, (now - opened_at_dt).total_seconds() / 60.0)) if opened_at_dt else 240
    except Exception:
        age_min = 240
    if age_min <= (_SIG_CANDLE_CATCHUP_MAX_CANDLES - 10):
        return "1m", min(_SIG_CANDLE_CATCHUP_MAX_CANDLES, max(50, age_min + 10))
    return "5m", min(_SIG_CANDLE_CATCHUP_MAX_CANDLES, max(50, int(age_min / 5) + 10))


def _sig_candle_touched(side: str, candle: dict, level: float, *, kind: str) -> bool:
    if level <= 0:
        return False
    high = float(candle.get("high") or 0.0)
    low = float(candle.get("low") or 0.0)
    side_u = str(side or "LONG").upper()
    if kind == "tp":
        return high >= level if side_u == "LONG" else low <= level
    # sl/be are adverse direction
    return low <= level if side_u == "LONG" else high >= level


def _sig_choose_same_candle(side: str, candle: dict, favorable_level: float, adverse_level: float) -> str:
    """Return 'fav' or 'adv' for a candle that touched both.

    Exact intrabar order is unknown from OHLC. Use close direction; if unclear,
    use the level closer to open. This only affects catch-up after missed polling.
    """
    try:
        o = float(candle.get("open") or 0.0)
        c = float(candle.get("close") or 0.0)
        side_u = str(side or "LONG").upper()
        if side_u == "LONG":
            if c >= o:
                return "fav"
            if c < o:
                return "adv"
            return "fav" if abs(favorable_level - o) <= abs(o - adverse_level) else "adv"
        else:
            if c <= o:
                return "fav"
            if c > o:
                return "adv"
            return "fav" if abs(o - favorable_level) <= abs(adverse_level - o) else "adv"
    except Exception:
        return "adv"


async def _detect_signal_candle_catchup(t: dict, *, now: dt.datetime) -> dict | None:
    """Detect a missed final outcome using OHLC since the signal opened.

    This is a fallback for sleeps/restarts/API misses. It only returns final
    statuses or TP1 arming; reports still go out only after final close.
    """
    if not _SIG_CANDLE_CATCHUP_ENABLED:
        return None
    try:
        sid = int(t.get("signal_id") or 0)
    except Exception:
        sid = 0
    if sid <= 0:
        return None

    now_mono = time.monotonic()
    last = float(_SIG_CANDLE_CATCHUP_LAST.get(sid, 0.0) or 0.0)
    if last and (now_mono - last) < _SIG_CANDLE_CATCHUP_INTERVAL_SEC:
        return None
    _SIG_CANDLE_CATCHUP_LAST[sid] = now_mono

    market = str(t.get("market") or "SPOT").upper()
    symbol = str(t.get("symbol") or "").upper()
    side = str(t.get("side") or "LONG").upper()
    status = str(t.get("status") or "ACTIVE").upper()
    opened_at_dt = _parse_iso_dt(t.get("opened_at"))
    tf, limit = _sig_catchup_params(opened_at_dt, now)
    candles, src = await _fetch_signal_candles(symbol, market=market, interval=tf, limit=limit)
    if not candles:
        return None

    tf_minutes = _sig_tf_minutes(tf)
    start_dt = opened_at_dt
    if status == "TP1":
        start_dt = _parse_iso_dt(t.get("tp1_hit_at")) or _parse_iso_dt(t.get("be_armed_at")) or opened_at_dt
    if start_dt:
        # Use only candles that opened after the signal/TP1 moment. Including the
        # previous candle can create false closes from a wick that happened before
        # the signal existed.
        start_floor = start_dt - dt.timedelta(seconds=5)
        candles = [c for c in candles if not c.get("ts") or c["ts"] >= start_floor]
    if not candles:
        return None

    entry = float(t.get("entry") or 0.0)
    tp1 = float(t.get("tp1") or 0.0) if t.get("tp1") is not None else 0.0
    tp2 = float(t.get("tp2") or 0.0) if t.get("tp2") is not None else 0.0
    sl = float(t.get("sl") or 0.0) if t.get("sl") is not None else 0.0
    eff_tp2 = tp2 if (tp2 > 0 and (tp1 <= 0 or abs(tp2 - tp1) > 1e-12)) else 0.0
    eff_tp1 = tp1 if tp1 > 0 else 0.0

    # Use the same anti-wick buffer for SL catch-up as live polling.
    sl_trigger = 0.0
    if sl > 0:
        sl_trigger = sl * (1.0 - _SIG_SL_BUFFER_PCT) if side == "LONG" else sl * (1.0 + _SIG_SL_BUFFER_PCT)
    be_trigger = entry * (1.0 - _BE_BUFFER_PCT) if side == "LONG" else entry * (1.0 + _BE_BUFFER_PCT)

    virtual_tp1 = bool(t.get("tp1_hit")) or status == "TP1"
    tp1_seen_at = _parse_iso_dt(t.get("tp1_hit_at")) or _parse_iso_dt(t.get("be_armed_at"))
    for candle in candles:
        cts = candle.get("ts")
        event_time = (cts + dt.timedelta(minutes=tf_minutes)) if isinstance(cts, dt.datetime) else now

        if not virtual_tp1:
            tp2_hit = bool(eff_tp2 > 0 and _sig_candle_touched(side, candle, eff_tp2, kind="tp"))
            tp1_hit = bool(eff_tp1 > 0 and _sig_candle_touched(side, candle, eff_tp1, kind="tp"))
            sl_hit = bool(sl_trigger > 0 and _sig_candle_touched(side, candle, sl_trigger, kind="sl"))

            # Single target: TP1 is final if there is no TP2.
            if eff_tp2 <= 0 and tp1_hit:
                if sl_hit:
                    choose = _sig_choose_same_candle(side, candle, eff_tp1, sl_trigger)
                    if choose == "adv":
                        return {"status": "LOSS", "closed_at": event_time, "source": src, "tp1_before": False}
                return {"status": "WIN", "closed_at": event_time, "source": src, "target": "tp1", "tp1_before": False}

            if tp2_hit or sl_hit:
                if tp2_hit and sl_hit:
                    choose = _sig_choose_same_candle(side, candle, eff_tp2, sl_trigger)
                    return {"status": ("WIN" if choose == "fav" else "LOSS"), "closed_at": event_time, "source": src, "target": "tp2", "tp1_before": bool(choose == "fav" and eff_tp1 > 0)}
                if tp2_hit:
                    return {"status": "WIN", "closed_at": event_time, "source": src, "target": "tp2", "tp1_before": bool(eff_tp1 > 0)}
                return {"status": "LOSS", "closed_at": event_time, "source": src, "tp1_before": False}

            if tp1_hit:
                virtual_tp1 = True
                tp1_seen_at = event_time
                continue

        # TP1 armed stage
        tp2_hit = bool(eff_tp2 > 0 and _sig_candle_touched(side, candle, eff_tp2, kind="tp"))
        sl_hit = bool(sl_trigger > 0 and _sig_candle_touched(side, candle, sl_trigger, kind="sl"))
        be_hit = bool(entry > 0 and _sig_candle_touched(side, candle, be_trigger, kind="sl"))

        if tp2_hit or sl_hit:
            if tp2_hit and sl_hit:
                choose = _sig_choose_same_candle(side, candle, eff_tp2, sl_trigger)
                return {"status": ("WIN" if choose == "fav" else "LOSS"), "closed_at": event_time, "source": src, "target": "tp2", "tp1_before": True}
            if tp2_hit:
                return {"status": "WIN", "closed_at": event_time, "source": src, "target": "tp2", "tp1_before": True}
            return {"status": "LOSS", "closed_at": event_time, "source": src, "tp1_before": True}

        if be_hit:
            return {"status": "BE", "closed_at": event_time, "source": src, "tp1_before": True}

    if status == "ACTIVE" and virtual_tp1:
        return {"status": "TP1", "closed_at": tp1_seen_at or now, "source": src, "tp1_before": True}
    return None



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
            _health_mark_ok("signal-outcome")
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

                    # Effective targets are needed by both candle catch-up and live price polling.
                    eff_tp2 = tp2 if (tp2 > 0 and (tp1 <= 0 or abs(tp2 - tp1) > 1e-12)) else 0.0
                    eff_tp1 = tp1 if tp1 > 0 else 0.0

                    if sid <= 0 or entry <= 0 or not symbol:
                        continue

                    # Catch-up by OHLC candles. This is the safety net for Railway/app sleep:
                    # if TP/SL happened while the loop was not polling current price, close
                    # the signal and send the final report card after wake/redeploy.
                    try:
                        _catch = await _detect_signal_candle_catchup(t, now=now)
                    except Exception:
                        logger.exception("[sig-outcome] candle catch-up failed sid=%s %s %s", sid, market, symbol)
                        _catch = None
                    if _catch:
                        cst = str(_catch.get("status") or "").upper().strip()
                        catch_closed_at = _catch.get("closed_at") if isinstance(_catch.get("closed_at"), dt.datetime) else now
                        catch_src = str(_catch.get("source") or "candles")
                        catch_tp1_before = bool(_catch.get("tp1_before")) or status == "TP1" or bool(t.get("tp1_hit"))

                        if cst == "TP1" and status == "ACTIVE":
                            try:
                                tp1_part = _model_partial_pct()
                                tp1_exec = float(eff_tp1 if 'eff_tp1' in locals() else (tp1 or 0.0))
                                ent = float(entry)
                                if side.upper() == "LONG":
                                    tp1_pnl = ((tp1_exec - ent) / ent) * 100.0
                                else:
                                    tp1_pnl = ((ent - tp1_exec) / ent) * 100.0
                                tp1_realized = tp1_pnl * tp1_part
                            except Exception:
                                tp1_realized = None
                            await db_store.mark_signal_tp1(signal_id=sid, be_price=float(entry), tp1_pnl_pct=tp1_realized)
                            _SIG_SL_BREACH_SINCE.pop(sid, None)
                            logger.info("[sig-outcome][catchup] TP1 sid=%s %s %s src=%s", sid, market, symbol, catch_src)
                            tp1_marked += 1
                            continue

                        if cst in ("WIN", "LOSS", "BE"):
                            # Compute the same model PnL used by the live polling path.
                            target = str(_catch.get("target") or "").lower()
                            if cst == "WIN":
                                if target == "tp2" and eff_tp2 > 0:
                                    pnl = _sig_net_pnl_two_targets(market=market, side=side, entry=entry, tp1=eff_tp1, tp2=eff_tp2, part=part) if (eff_tp1 > 0 and eff_tp2 > 0) else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=eff_tp2, part_entry_to_close=1.0)
                                else:
                                    close_px = eff_tp1 if eff_tp1 > 0 else (eff_tp2 if eff_tp2 > 0 else entry)
                                    pnl = _sig_net_pnl_pct(market=market, side=side, entry=entry, close=close_px, part_entry_to_close=1.0)
                                closed_now = await db_store.close_signal_track(signal_id=sid, status="WIN", pnl_total_pct=float(pnl), closed_at=catch_closed_at)
                                _SIG_SL_BREACH_SINCE.pop(sid, None)
                                if not closed_now:
                                    continue
                                _report_row = dict(t)
                                if catch_tp1_before:
                                    _report_row["tp1_hit"] = True
                                    _report_row["status"] = "TP1"
                                await _send_closed_signal_report_card(_report_row, final_status="WIN", pnl_total_pct=float(pnl), closed_at=catch_closed_at)
                                await _adaptive_v3_update_from_outcome(dict(_report_row), final_status="WIN", pnl_total_pct=float(pnl), closed_at=catch_closed_at)
                                logger.info("[sig-outcome][catchup] WIN sid=%s %s %s src=%s", sid, market, symbol, catch_src)
                                closed_win += 1
                                continue

                            if cst == "LOSS":
                                pnl = _sig_net_pnl_tp1_then_sl(market=market, side=side, entry=entry, tp1=eff_tp1, sl=sl, part=part) if (catch_tp1_before and eff_tp1 > 0) else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=sl, part_entry_to_close=1.0)
                                _base_row = dict(t)
                                if catch_tp1_before:
                                    _base_row["tp1_hit"] = True
                                    _base_row["status"] = "TP1"
                                _close_analysis = await _loss_diag_build_close_analysis(_base_row, closed_at=catch_closed_at)
                                _loss_diag = _build_loss_diagnostics_from_row(_base_row, final_status="LOSS", closed_at=catch_closed_at, close_analysis=_close_analysis)
                                closed_now = await db_store.close_signal_track(
                                    signal_id=sid,
                                    status="LOSS",
                                    pnl_total_pct=float(pnl),
                                    close_reason_code=str(_loss_diag.get("reason_code") or ""),
                                    close_reason_text=str(_loss_diag.get("reason_text") or ""),
                                    weak_filters=",".join(list(_loss_diag.get("weak_filter_keys") or [])),
                                    improve_note="; ".join(list(_loss_diag.get("improve_keys") or [])),
                                    close_analysis_json=_loss_diag.get("close_analysis_json") or _close_analysis,
                                    closed_at=catch_closed_at,
                                )
                                _SIG_SL_BREACH_SINCE.pop(sid, None)
                                if not closed_now:
                                    continue
                                _report_row = dict(_base_row)
                                _report_row["close_analysis_json"] = _loss_diag.get("close_analysis_json") or _close_analysis
                                _report_row["close_reason_code"] = str(_loss_diag.get("reason_code") or "")
                                _report_row["close_reason_text"] = str(_loss_diag.get("reason_text") or "")
                                _report_row["weak_filters"] = ",".join(list(_loss_diag.get("weak_filter_keys") or []))
                                _report_row["improve_note"] = "; ".join(list(_loss_diag.get("improve_keys") or []))
                                await _send_closed_signal_report_card(_report_row, final_status="LOSS", pnl_total_pct=float(pnl), closed_at=catch_closed_at)
                                await _adaptive_v3_update_from_outcome(dict(_report_row), final_status="LOSS", pnl_total_pct=float(pnl), closed_at=catch_closed_at)
                                logger.info("[sig-outcome][catchup] LOSS sid=%s %s %s src=%s", sid, market, symbol, catch_src)
                                closed_loss += 1
                                continue

                            if cst == "BE":
                                pnl = _sig_net_pnl_tp1_then_be(market=market, side=side, entry=entry, tp1=eff_tp1, part=part) if eff_tp1 > 0 else _sig_net_pnl_pct(market=market, side=side, entry=entry, close=entry, part_entry_to_close=1.0)
                                closed_now = await db_store.close_signal_track(signal_id=sid, status="BE", pnl_total_pct=float(pnl), closed_at=catch_closed_at)
                                _SIG_SL_BREACH_SINCE.pop(sid, None)
                                if not closed_now:
                                    continue
                                _report_row = dict(t)
                                _report_row["tp1_hit"] = True
                                _report_row["status"] = "TP1"
                                await _send_closed_signal_report_card(_report_row, final_status="BE", pnl_total_pct=float(pnl), closed_at=catch_closed_at)
                                await _adaptive_v3_update_from_outcome(dict(_report_row), final_status="BE", pnl_total_pct=float(pnl), closed_at=catch_closed_at)
                                logger.info("[sig-outcome][catchup] BE sid=%s %s %s src=%s", sid, market, symbol, catch_src)
                                closed_be += 1
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
                            closed_now = await db_store.close_signal_track(signal_id=sid, status="WIN", pnl_total_pct=float(pnl))
                            if not closed_now:
                                continue
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

    # Refresh global Auto-trade pause/maintenance flags in background.
    # Low-load mode: when AUTO-TRADE is disabled, do not keep a DB polling loop alive.
    if _env_on("AUTOTRADE_GLOBAL_LOOP_ENABLED", os.getenv("AUTOTRADE_ENABLED", "0")):
        TASKS["autotrade-global"] = asyncio.create_task(_autotrade_bot_global_loop(), name="autotrade-global")
        _attach_task_monitor("autotrade-global", TASKS["autotrade-global"])
        await _refresh_autotrade_bot_global_once(); _health_mark_ok("autotrade-global")
    else:
        AUTOTRADE_BOT_GLOBAL["pause_autotrade"] = True
        AUTOTRADE_BOT_GLOBAL["maintenance_mode"] = True
        logger.info("AUTOTRADE_GLOBAL_LOOP_ENABLED=0/AUTOTRADE_ENABLED=0 -> autotrade global loop disabled")

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

            cache_ttl = float(os.getenv("ADMIN_STATS_CACHE_TTL_SEC", "30") or 30)
            cache_key = "signal_stats"
            try:
                now_cache = time.time()
                cache = getattr(signal_stats, "_cache", {})
                if cache_ttl > 0 and cache_key in cache:
                    ts_cached, data_cached = cache[cache_key]
                    if (now_cache - float(ts_cached)) < cache_ttl:
                        return web.json_response(data_cached)
            except Exception:
                cache = {}

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


            data = {
                "ok": True,
                "signals": signals,
                "closed": closed,
                "perf": perf,
            }
            try:
                cache = getattr(signal_stats, "_cache", {})
                cache[cache_key] = (time.time(), data)
                setattr(signal_stats, "_cache", cache)
            except Exception:
                pass
            return web.json_response(data)

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

            main_scanner_enabled = os.getenv("MAIN_SCANNER_ENABLED", os.getenv("SCANNER_ENABLED", _cost_default("1", "0"))).strip().lower() not in ("0", "false", "no", "off")
            main_top_n = os.getenv("MAIN_TOP_N", os.getenv("TOP_N", ""))
            logger.info(
                "Starting scanner_loop check enabled=%s interval=%ss top_n=%s",
                main_scanner_enabled,
                os.getenv("SCAN_INTERVAL_SECONDS", ""),
                main_top_n,
            )

            # Some deployments (MID-only builds) intentionally don't ship the legacy scanner_loop.
            # Guard this call to avoid crashing the whole process.
            if main_scanner_enabled and hasattr(backend, "scanner_loop"):
                try:
                    TASKS["scanner"] = asyncio.create_task(
                        backend.scanner_loop(broadcast_signal, broadcast_macro_alert),
                        name="scanner",
                    )
                    _health_mark_ok("scanner")
                    _attach_task_monitor("scanner", TASKS["scanner"])
                except Exception:
                    logger.exception("Failed to start scanner_loop")
            elif not main_scanner_enabled:
                logger.info("MAIN_SCANNER_ENABLED=0 -> legacy main scanner disabled")
            else:
                logger.warning(
                    "Backend has no scanner_loop; skipping (MID scanner runs via scanner_loop_mid)"
                )

            # MID components (scanner + pending loop)
            _start_mid_components(backend, broadcast_signal, broadcast_macro_alert)

            logger.info("Starting signal_outcome_loop")
            _start_signal_outcome_task()
            if _CLOSED_SIGNAL_REPORT_RETRY_ENABLED:
                TASKS["closed-signal-report-retry"] = asyncio.create_task(closed_signal_report_retry_loop(), name="closed-signal-report-retry")
                _attach_task_monitor("closed-signal-report-retry", TASKS["closed-signal-report-retry"])

            TASKS["daily-signal-report"] = asyncio.create_task(daily_signal_report_loop(), name="daily-signal-report")
            _attach_task_monitor("daily-signal-report", TASKS["daily-signal-report"])

        # Auto-trade manager: available, but low-load. It sleeps long when there are no open positions.
        autotrade_mgr_enabled = os.getenv("AUTOTRADE_MANAGER_ENABLED", os.getenv("AUTOTRADE_ENABLED", "0")).strip().lower() not in ("0", "false", "no", "off")
        if autotrade_mgr_enabled:
            TASKS["autotrade-manager"] = asyncio.create_task(
                autotrade_manager_loop(notify_api_error=_notify_autotrade_api_error, notify_smart_event=_notify_smart_manager_event, backend_instance=backend),
                name="autotrade-manager",
            )
            _health_mark_ok("autotrade-manager")
            _attach_task_monitor("autotrade-manager", TASKS["autotrade-manager"])
            TASKS["autotrade-manager-heartbeat"] = asyncio.create_task(
                _task_heartbeat_loop("autotrade-manager", interval_sec=float(os.getenv("AUTOTRADE_MANAGER_HEARTBEAT_SEC", "60") or 60)),
                name="autotrade-manager-heartbeat",
            )
        else:
            logger.info("AUTOTRADE_MANAGER_ENABLED=0 -> autotrade manager disabled")

        if os.getenv("AUTOTRADE_DIAG_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on"):
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
            if os.getenv("AUTOTRADE_MANAGER_ENABLED", os.getenv("AUTOTRADE_ENABLED", "0")).strip().lower() not in ("0", "false", "no", "off"):
                TASKS["autotrade-manager"] = asyncio.create_task(autotrade_manager_loop(notify_api_error=_notify_autotrade_api_error, notify_smart_event=_notify_smart_manager_event, backend_instance=backend), name="autotrade-manager")
                _health_mark_ok("autotrade-manager")
            if os.getenv("AUTOTRADE_DIAG_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on"):
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
    main_scanner_enabled = os.getenv("MAIN_SCANNER_ENABLED", os.getenv("SCANNER_ENABLED", _cost_default("1", "0"))).strip().lower() not in ("0", "false", "no", "off")
    logger.info("Starting scanner_loop check enabled=%s interval=%ss top_n=%s", main_scanner_enabled, os.getenv('SCAN_INTERVAL_SECONDS',''), os.getenv('MAIN_TOP_N', os.getenv('TOP_N','')))
    if main_scanner_enabled and hasattr(backend, "scanner_loop"):
        TASKS["scanner"] = asyncio.create_task(backend.scanner_loop(broadcast_signal, broadcast_macro_alert), name="scanner")
        _attach_task_monitor("scanner", TASKS["scanner"])
    else:
        logger.info("MAIN_SCANNER_ENABLED=0 or scanner_loop missing -> legacy main scanner disabled")

    # ⚡ MID TREND scanner + MID trap digest
    _start_mid_components(backend, broadcast_signal, broadcast_macro_alert)

    logger.info("Starting signal_outcome_loop")
    _start_signal_outcome_task()
    if _CLOSED_SIGNAL_REPORT_RETRY_ENABLED:
        TASKS["closed-signal-report-retry"] = asyncio.create_task(closed_signal_report_retry_loop(), name="closed-signal-report-retry")
        _attach_task_monitor("closed-signal-report-retry", TASKS["closed-signal-report-retry"])

    TASKS["daily-signal-report"] = asyncio.create_task(daily_signal_report_loop(), name="daily-signal-report")
    _attach_task_monitor("daily-signal-report", TASKS["daily-signal-report"])

    # Auto-trade manager (SL/TP/BE) - runs in background.
    if os.getenv("AUTOTRADE_MANAGER_ENABLED", os.getenv("AUTOTRADE_ENABLED", "0")).strip().lower() not in ("0", "false", "no", "off"):
        TASKS["autotrade-manager"] = asyncio.create_task(autotrade_manager_loop(notify_api_error=_notify_autotrade_api_error, notify_smart_event=_notify_smart_manager_event, backend_instance=backend), name="autotrade-manager")
        _attach_task_monitor("autotrade-manager", TASKS["autotrade-manager"])
    if os.getenv("AUTOTRADE_DIAG_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on"):
        TASKS["autotrade-anomaly-watchdog"] = asyncio.create_task(autotrade_anomaly_watchdog_loop(notify_api_error=_notify_autotrade_api_error), name="autotrade-anomaly-watchdog")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


# ===============================
# AUTO SYMBOL ANALYSIS HANDLER
# ===============================

