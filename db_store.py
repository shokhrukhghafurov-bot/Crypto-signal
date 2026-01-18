from __future__ import annotations

import asyncpg
import datetime as dt
import logging
from typing import Any, Dict, List, Optional, Tuple

_pool: Optional[asyncpg.Pool] = None

logger = logging.getLogger("db_store")

def set_pool(pool: asyncpg.Pool) -> None:
    global _pool
    _pool = pool

def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized. Call set_pool() first.")
    return _pool

async def ensure_schema() -> None:
    """
    Creates tables needed for persistent trades/statistics.
    Uses IDENTITY (auto increment) columns.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        # Persistent sequence for bot callback signal_id (survives restarts)
        await conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS signal_seq START 1;
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          user_id BIGINT NOT NULL,
          signal_id BIGINT NOT NULL, -- signal id used in bot callbacks (in-memory signal sequence)
          market TEXT NOT NULL CHECK (market IN ('SPOT','FUTURES')),
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          entry NUMERIC(18,8) NOT NULL,
          tp1   NUMERIC(18,8),
          tp2   NUMERIC(18,8),
          sl    NUMERIC(18,8),
          status TEXT NOT NULL CHECK (status IN ('ACTIVE','TP1','BE','WIN','LOSS','CLOSED')),
          tp1_hit BOOLEAN NOT NULL DEFAULT FALSE,
          be_price NUMERIC(18,8),
          opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          closed_at TIMESTAMPTZ,
          pnl_total_pct NUMERIC(10,4),
          orig_text TEXT NOT NULL
        );
        """)
        
        # --- schema compat: allow many users to open same signal ---
        # Older DBs could have UNIQUE(signal_id) which blocks other users.
        await conn.execute("""
        DO $$
        DECLARE r RECORD;
        BEGIN
          -- Drop UNIQUE constraints that are only on signal_id (legacy)
          FOR r IN
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'trades'::regclass
              AND contype = 'u'
              AND pg_get_constraintdef(oid) ILIKE '%(signal_id)%'
              AND pg_get_constraintdef(oid) NOT ILIKE '%user_id%'
          LOOP
            EXECUTE format('ALTER TABLE trades DROP CONSTRAINT IF EXISTS %I;', r.conname);
          END LOOP;

          -- Drop UNIQUE indexes that are only on signal_id (legacy)
          FOR r IN
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'trades'
              AND indexdef ILIKE '%unique%'
              AND indexdef ILIKE '%(signal_id)%'
              AND indexdef NOT ILIKE '%user_id%'
          LOOP
            EXECUTE format('DROP INDEX IF EXISTS %I;', r.indexname);
          END LOOP;
        END $$;
        """)

        # Ensure the correct uniqueness: each user can open the same signal independently.
        await conn.execute("""
        -- Drop legacy full unique index (prevents reopening after close)
        DROP INDEX IF EXISTS uq_trades_user_signal;

        -- Allow history: unique only for ACTIVE states (user can't have 2 active trades for same signal)
        CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_user_signal_active
        ON trades (user_id, signal_id)
        WHERE status IN ('ACTIVE','TP1');
""")

        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_user_status
        ON trades (user_id, status);
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_status_opened
        ON trades (status, opened_at DESC);
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_events (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          trade_id BIGINT NOT NULL REFERENCES trades(id) ON DELETE CASCADE,
          event_type TEXT NOT NULL CHECK (event_type IN ('OPEN','TP1','BE','WIN','LOSS','CLOSE')),
          price NUMERIC(18,8),
          pnl_pct NUMERIC(10,4),
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trade_events_trade_time
        ON trade_events (trade_id, created_at DESC);
        """)


        # --- signals sent counters (persistent, for admin dashboard) ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_sent_events (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          sig_key TEXT NOT NULL,
          market TEXT NOT NULL CHECK (market IN ('SPOT','FUTURES')),
          signal_id BIGINT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (sig_key)
        );
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_sent_events_time
        ON signal_sent_events (created_at DESC);
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_sent_events_market_time
        ON signal_sent_events (market, created_at DESC);
        """)


async def ensure_users_columns() -> None:
    """Best-effort schema migration for users table.

    We keep it here (db_store) so bot.py stays clean and all DB migrations
    live in one place.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        # users table is created/managed elsewhere (backend). We only add missing columns.
        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS notify_signals BOOLEAN NOT NULL DEFAULT TRUE;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add notify_signals")

        # --- Signal bot access (separate from Arbitrage access) ---
        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS signal_enabled BOOLEAN NOT NULL DEFAULT FALSE;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add signal_enabled")

        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS signal_expires_at TIMESTAMPTZ;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add signal_expires_at")

        # Helpful index; ignore failure on managed DBs.
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);")
        except Exception:
            logger.exception("ensure_users_columns: failed to create idx_users_telegram_id")


async def ensure_user_signal_trial(user_id: int) -> None:
    """Create user row if missing and grant 24h Signal trial ONCE.

    IMPORTANT: does NOT touch Arbitrage access.
    """
    if not user_id:
        return
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (telegram_id, notify_signals, signal_enabled, signal_expires_at)
            VALUES ($1, TRUE, TRUE, (NOW() AT TIME ZONE 'UTC') + INTERVAL '24 hours')
            ON CONFLICT (telegram_id) DO NOTHING;
            """,
            int(user_id),
        )

async def next_signal_id() -> int:
    """Return a globally unique signal_id for bot callbacks (Postgres sequence)."""
    pool = get_pool()
    async with pool.acquire() as conn:
        v = await conn.fetchval("SELECT nextval('signal_seq');")
        return int(v) if v is not None else int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)

async def open_trade_once(
    *,
    user_id: int,
    signal_id: int,
    market: str,
    symbol: str,
    side: str,
    entry: float,
    tp1: float | None,
    tp2: float | None,
    sl: float | None,
    orig_text: str,
) -> Tuple[bool, Optional[int]]:
    """
    Inserts a new trade if not already opened (unique user_id+signal_id).
    Returns (inserted, trade_db_id).
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        try:
            async def _insert(with_signal_id: int) -> Optional[int]:
                r = await conn.fetchrow(
                    """
                    INSERT INTO trades (user_id, signal_id, market, symbol, side, entry, tp1, tp2, sl, status, orig_text)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'ACTIVE',$10)
                    ON CONFLICT (user_id, signal_id) WHERE status IN ('ACTIVE','TP1') DO NOTHING
                    RETURNING id;
                    """,
                    int(user_id), int(with_signal_id), market, symbol, side, float(entry),
                    (float(tp1) if tp1 is not None else None),
                    (float(tp2) if tp2 is not None else None),
                    (float(sl) if sl is not None else None),
                    orig_text or "",
                )
                return int(r["id"]) if (r and r.get("id") is not None) else None

            # 1) Try normal insert (works on correct schema with partial unique index)
            trade_id = await _insert(int(signal_id))
            if trade_id is not None:
                await conn.execute(
                    "INSERT INTO trade_events (trade_id, event_type) VALUES ($1,'OPEN');",
                    trade_id,
                )
                return True, trade_id

            # 2) If insert was blocked, check why.
            # On legacy DBs there may be a FULL UNIQUE(user_id, signal_id) which blocks reopening even after WIN/LOSS.
            existing = await conn.fetchrow(
                """
                SELECT id, market, status, closed_at
                FROM trades
                WHERE user_id=$1 AND signal_id=$2
                ORDER BY opened_at DESC
                LIMIT 1;
                """,
                int(user_id), int(signal_id),
            )
            if existing:
                ex_market = str(existing.get("market") or "").upper()
                st = str(existing.get("status") or "").upper()
                closed_at = existing.get("closed_at")
                # Truly active -> treat as already opened
                if ex_market == str(market or "").upper() and st in ("ACTIVE", "TP1") and closed_at is None:
                    return False, None

            # 3) Reopen fallback: generate a new callback-safe signal_id and insert.
            # This keeps the user experience correct even if the DB still has a legacy UNIQUE constraint.
            new_sid_row = await conn.fetchrow("SELECT nextval('signal_seq') AS sid;")
            new_sid = int(new_sid_row["sid"]) if new_sid_row and new_sid_row.get("sid") is not None else int(signal_id)
            trade_id = await _insert(new_sid)
            if trade_id is not None:
                await conn.execute(
                    "INSERT INTO trade_events (trade_id, event_type) VALUES ($1,'OPEN');",
                    trade_id,
                )
                return True, trade_id

            return False, None
        except Exception:
            # let caller log
            raise

async def list_user_trades(user_id: int, *, include_closed: bool = True, limit: int = 50) -> List[Dict[str, Any]]:
    pool = get_pool()
    where = "user_id=$1"
    if not include_closed:
        where += " AND status IN ('ACTIVE','TP1')"
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, user_id, signal_id, market, symbol, side, entry, tp1, tp2, sl,
                   status, tp1_hit, be_price, opened_at, closed_at, pnl_total_pct, orig_text
            FROM trades
            WHERE {where}
            ORDER BY opened_at DESC
            LIMIT {int(limit)};
            """,
            int(user_id),
        )
        return [dict(r) for r in rows]

async def get_trade_by_user_signal(user_id: int, signal_id: int) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, user_id, signal_id, market, symbol, side, entry, tp1, tp2, sl,
                   status, tp1_hit, be_price, opened_at, closed_at, pnl_total_pct, orig_text
            FROM trades
            WHERE user_id=$1 AND signal_id=$2
            """,
            int(user_id), int(signal_id)
        )
        return dict(row) if row else None


async def get_trade_by_id(user_id: int, trade_id: int) -> Optional[Dict[str, Any]]:
    """Fetch a trade by its DB id, ensuring it belongs to the user."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, user_id, signal_id, market, symbol, side, entry, tp1, tp2, sl,
                   status, tp1_hit, be_price, opened_at, closed_at, pnl_total_pct, orig_text
            FROM trades
            WHERE user_id=$1 AND id=$2
            """,
            int(user_id), int(trade_id)
        )
        return dict(row) if row else None

async def list_active_trades(limit: int = 500) -> List[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, user_id, signal_id, market, symbol, side, entry, tp1, tp2, sl,
                   status, tp1_hit, be_price, opened_at, closed_at, pnl_total_pct, orig_text
            FROM trades
            WHERE status IN ('ACTIVE','TP1')
            ORDER BY opened_at DESC
            LIMIT $1
            """,
            int(limit),
        )
        return [dict(r) for r in rows]

async def set_tp1(trade_id: int, *, be_price: float, price: float | None = None, pnl_pct: float | None = None) -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE trades
            SET status='TP1', tp1_hit=TRUE, be_price=$2
            WHERE id=$1;
            """,
            int(trade_id), float(be_price)
        )
        await conn.execute(
            "INSERT INTO trade_events (trade_id, event_type, price, pnl_pct) VALUES ($1,'TP1',$2,$3);",
            int(trade_id),
            (float(price) if price is not None else None),
            (float(pnl_pct) if pnl_pct is not None else None),
        )

async def close_trade(trade_id: int, *, status: str, price: float | None = None, pnl_total_pct: float | None = None) -> None:
    """
    status: BE / WIN / LOSS / CLOSED
    """
    pool = get_pool()
    ev = "CLOSE" if status == "CLOSED" else status
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE trades
            SET status=$2, closed_at=NOW(), pnl_total_pct=$3
            WHERE id=$1;
            """,
            int(trade_id), status, (float(pnl_total_pct) if pnl_total_pct is not None else None),
        )
        await conn.execute(
            "INSERT INTO trade_events (trade_id, event_type, price, pnl_pct) VALUES ($1,$2,$3,$4);",
            int(trade_id),
            ev,
            (float(price) if price is not None else None),
            (float(pnl_total_pct) if pnl_total_pct is not None else None),
        )

async def perf_bucket(user_id: int, market: str, *, since: dt.datetime, until: dt.datetime) -> Dict[str, Any]:
    """
    Aggregates performance in [since, until) using trade_events.

    Events-based stats are more robust than reading the final trades rows,
    because TP1 should be counted even if a trade is later closed manually,
    and close results are best represented by the final close event.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            WITH ev AS (
              SELECT e.trade_id, e.event_type, e.pnl_pct
              FROM trade_events e
              JOIN trades t ON t.id = e.trade_id
              WHERE t.user_id=$1 AND t.market=$2
                AND e.created_at >= $3 AND e.created_at < $4
            )
            SELECT
              SUM(CASE WHEN event_type IN ('WIN','LOSS','BE','CLOSE') THEN 1 ELSE 0 END)::int AS trades,
              SUM(CASE WHEN event_type='WIN' THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN event_type='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN event_type='BE' THEN 1 ELSE 0 END)::int AS be,
              COUNT(DISTINCT CASE WHEN event_type='TP1' THEN trade_id END)::int AS tp1_hits,
              COALESCE(SUM(CASE WHEN event_type IN ('WIN','LOSS','BE','CLOSE') THEN COALESCE(pnl_pct,0) ELSE 0 END), 0)::float AS sum_pnl_pct
            FROM ev;
            """,
            int(user_id),
            market,
            since,
            until,
        )
        base = {"trades":0,"wins":0,"losses":0,"be":0,"tp1_hits":0,"sum_pnl_pct":0.0}
        if not row:
            return base
        d = dict(row)
        # asyncpg may return None for SUMs when no rows
        return {
            "trades": int(d.get("trades") or 0),
            "wins": int(d.get("wins") or 0),
            "losses": int(d.get("losses") or 0),
            "be": int(d.get("be") or 0),
            "tp1_hits": int(d.get("tp1_hits") or 0),
            "sum_pnl_pct": float(d.get("sum_pnl_pct") or 0.0),
        }


async def perf_bucket_global(market: str, *, since: dt.datetime, until: dt.datetime) -> Dict[str, Any]:
    """Global performance aggregation (all users) in [since, until) using trade_events.

    This mirrors perf_bucket(), but without filtering by user_id.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            WITH ev AS (
              SELECT e.trade_id, e.event_type, e.pnl_pct
              FROM trade_events e
              JOIN trades t ON t.id = e.trade_id
              WHERE t.market=$1
                AND e.created_at >= $2 AND e.created_at < $3
            )
            SELECT
              SUM(CASE WHEN event_type IN ('WIN','LOSS','BE','CLOSE') THEN 1 ELSE 0 END)::int AS trades,
              SUM(CASE WHEN event_type='WIN' THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN event_type='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN event_type='BE' THEN 1 ELSE 0 END)::int AS be,
              SUM(CASE WHEN event_type='CLOSE' THEN 1 ELSE 0 END)::int AS closes,
              COUNT(DISTINCT CASE WHEN event_type='TP1' THEN trade_id END)::int AS tp1_hits,
              COALESCE(SUM(CASE WHEN event_type IN ('WIN','LOSS','BE','CLOSE') THEN COALESCE(pnl_pct,0) ELSE 0 END), 0)::float AS sum_pnl_pct
            FROM ev;
            """,
            market,
            since,
            until,
        )
        base = {"trades":0,"wins":0,"losses":0,"be":0,"closes":0,"tp1_hits":0,"sum_pnl_pct":0.0}
        if not row:
            return base
        d = dict(row)
        return {
            "trades": int(d.get("trades") or 0),
            "wins": int(d.get("wins") or 0),
            "losses": int(d.get("losses") or 0),
            "be": int(d.get("be") or 0),
            "closes": int(d.get("closes") or 0),
            "tp1_hits": int(d.get("tp1_hits") or 0),
            "sum_pnl_pct": float(d.get("sum_pnl_pct") or 0.0),
        }

async def daily_report(user_id: int, market: str, *, days: int, tz: str = "UTC") -> List[dict]:
    """
    Returns per-day buckets for the last N days using trade_events.
    Each item: {"day":"YYYY-MM-DD","trades":..,"wins":..,"losses":..,"be":..,"tp1_hits":..,"sum_pnl_pct":..}
    Day boundaries are computed in the given timezone.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              (e.created_at AT TIME ZONE $4)::date AS day,
              SUM(CASE WHEN e.event_type IN ('WIN','LOSS','BE','CLOSE') THEN 1 ELSE 0 END)::int AS trades,
              SUM(CASE WHEN e.event_type='WIN' THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN e.event_type='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN e.event_type='BE' THEN 1 ELSE 0 END)::int AS be,
              COUNT(DISTINCT CASE WHEN e.event_type='TP1' THEN e.trade_id END)::int AS tp1_hits,
              COALESCE(SUM(CASE WHEN e.event_type IN ('WIN','LOSS','BE','CLOSE') THEN COALESCE(e.pnl_pct,0) ELSE 0 END), 0)::float AS sum_pnl_pct
            FROM trade_events e
            JOIN trades t ON t.id = e.trade_id
            WHERE t.user_id=$1 AND t.market=$2
              AND e.created_at >= (NOW() - ($3::int || ' days')::interval)
            GROUP BY day
            ORDER BY day ASC;
            """,
            int(user_id),
            market,
            int(days),
            str(tz),
        )
    return [dict(r) for r in rows]


async def weekly_report(user_id: int, market: str, *, weeks: int, tz: str = "UTC") -> List[dict]:
    """
    Returns per-week buckets (ISO week label) for the last N weeks using trade_events.
    Each item: {"week":"2026-W03","trades":..,"wins":..,"losses":..,"be":..,"tp1_hits":..,"sum_pnl_pct":..}
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              to_char(date_trunc('week', (e.created_at AT TIME ZONE $4)), 'IYYY-"W"IW') AS week,
              SUM(CASE WHEN e.event_type IN ('WIN','LOSS','BE','CLOSE') THEN 1 ELSE 0 END)::int AS trades,
              SUM(CASE WHEN e.event_type='WIN' THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN e.event_type='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN e.event_type='BE' THEN 1 ELSE 0 END)::int AS be,
              COUNT(DISTINCT CASE WHEN e.event_type='TP1' THEN e.trade_id END)::int AS tp1_hits,
              COALESCE(SUM(CASE WHEN e.event_type IN ('WIN','LOSS','BE','CLOSE') THEN COALESCE(e.pnl_pct,0) ELSE 0 END), 0)::float AS sum_pnl_pct
            FROM trade_events e
            JOIN trades t ON t.id = e.trade_id
            WHERE t.user_id=$1 AND t.market=$2
              AND e.created_at >= (NOW() - ($3::int || ' weeks')::interval)
            GROUP BY week
            ORDER BY week ASC;
            """,
            int(user_id),
            market,
            int(weeks),
            str(tz),
        )
    return [dict(r) for r in rows]


# ---------------- signals sent counters ----------------

async def record_signal_sent(*, sig_key: str, market: str, signal_id: int | None = None) -> bool:
    """Persist an event that a signal was broadcast (dedup by sig_key).

    Returns True if inserted, False if duplicate or failed.
    """
    if not sig_key:
        return False
    market = str(market or '').upper()
    if market not in ('SPOT','FUTURES'):
        return False
    pool = get_pool()
    async with pool.acquire() as conn:
        try:
            r = await conn.fetchrow(
                """
                INSERT INTO signal_sent_events (sig_key, market, signal_id)
                VALUES ($1,$2,$3)
                ON CONFLICT (sig_key) DO NOTHING
                RETURNING id;
                """,
                sig_key, market, (int(signal_id) if signal_id is not None else None),
            )
            return bool(r)
        except Exception:
            logger.exception('record_signal_sent failed')
            return False


async def count_signal_sent_by_market(*, since: dt.datetime, until: dt.datetime) -> Dict[str, int]:
    """Count broadcasted signals in [since, until) grouped by market.

    Returns dict like {'SPOT': n, 'FUTURES': m}. Missing keys mean 0.
    """
    pool = get_pool()
    out: Dict[str, int] = {'SPOT': 0, 'FUTURES': 0}
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT market, COUNT(*)::BIGINT AS n
                FROM signal_sent_events
                WHERE created_at >= $1 AND created_at < $2
                GROUP BY market;
                """,
                since, until,
            )
            for r in rows or []:
                mk = str(r.get('market') or '').upper()
                if mk in out:
                    out[mk] = int(r.get('n') or 0)
        except Exception:
            logger.exception('count_signal_sent_by_market failed')
    return out
