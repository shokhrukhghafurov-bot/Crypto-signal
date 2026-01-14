from __future__ import annotations

import asyncpg
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

_pool: Optional[asyncpg.Pool] = None

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
await conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_user_signal
        ON trades (user_id, signal_id);
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
            row = await conn.fetchrow(
                """
                INSERT INTO trades (user_id, signal_id, market, symbol, side, entry, tp1, tp2, sl, status, orig_text)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'ACTIVE',$10)
                ON CONFLICT (user_id, signal_id) DO NOTHING
                RETURNING id;
                """,
                int(user_id), int(signal_id), market, symbol, side, float(entry),
                (float(tp1) if tp1 is not None else None),
                (float(tp2) if tp2 is not None else None),
                (float(sl) if sl is not None else None),
                orig_text or ""
            )
            if row and row["id"] is not None:
                trade_id = int(row["id"])
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
