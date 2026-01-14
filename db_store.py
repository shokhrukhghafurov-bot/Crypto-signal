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

async def perf_bucket(market: str, *, since: dt.datetime, until: dt.datetime) -> Dict[str, Any]:
    """
    Aggregates CLOSED trades in [since, until).
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              COUNT(*)::int AS trades,
              SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN status='BE' THEN 1 ELSE 0 END)::int AS be,
              SUM(CASE WHEN status='TP1' THEN 1 ELSE 0 END)::int AS tp1_hits,
              COALESCE(SUM(CASE WHEN pnl_total_pct IS NULL THEN 0 ELSE pnl_total_pct END), 0)::float AS sum_pnl_pct
            FROM trades
            WHERE market=$1
              AND status IN ('WIN','LOSS','BE','CLOSED')
              AND closed_at >= $2 AND closed_at < $3;
            """,
            market,
            since,
            until,
        )
        return dict(row) if row else {"trades":0,"wins":0,"losses":0,"be":0,"tp1_hits":0,"sum_pnl_pct":0.0}

async def daily_report(market: str, *, days: int, tz: str = "UTC") -> List[str]:
    """
    Returns lines like YYYY-MM-DD: trades=... winrate=... pnl=...
    """
    # We do day bucketing in SQL using date_trunc in UTC; tz conversion on DB side can be added later if needed.
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              (closed_at AT TIME ZONE 'UTC')::date AS day,
              COUNT(*)::int AS trades,
              SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END)::int AS wins,
              COALESCE(SUM(CASE WHEN pnl_total_pct IS NULL THEN 0 ELSE pnl_total_pct END), 0)::float AS pnl
            FROM trades
            WHERE market=$1
              AND status IN ('WIN','LOSS','BE','CLOSED')
              AND closed_at >= (NOW() - ($2::int || ' days')::interval)
            GROUP BY day
            ORDER BY day ASC;
            """,
            market,
            int(days),
        )
        # Build full range with empty days
        today = dt.datetime.utcnow().date()
        by_day = {r["day"].isoformat(): dict(r) for r in rows if r.get("day")}
        out: List[str] = []
        for i in range(days-1, -1, -1):
            d = (today - dt.timedelta(days=i)).isoformat()
            r = by_day.get(d)
            if not r:
                out.append(f"{d}: trades=0 winrate=0.0% pnl=+0.00%")
                continue
            trades = int(r["trades"])
            wins = int(r["wins"])
            pnl = float(r["pnl"])
            wr = (wins / trades * 100.0) if trades else 0.0
            out.append(f"{d}: trades={trades} winrate={wr:.1f}% pnl={pnl:+.2f}%")
        return out
