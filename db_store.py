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

        # --- Signal bot global settings (single row) ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_bot_settings (
          id INT PRIMARY KEY CHECK (id = 1),
          pause_signals BOOLEAN NOT NULL DEFAULT FALSE,
          maintenance_mode BOOLEAN NOT NULL DEFAULT FALSE,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        await conn.execute("""
        INSERT INTO signal_bot_settings(id) VALUES (1)
        ON CONFLICT (id) DO NOTHING;
        """)
        # Ensure support_username column exists
        await conn.execute("""
        ALTER TABLE signal_bot_settings
        ADD COLUMN IF NOT EXISTS support_username TEXT;
        """)

        


        # --- Auto-trade global settings (single row) ---
        await conn.execute("""
CREATE TABLE IF NOT EXISTS autotrade_bot_settings (
  id INT PRIMARY KEY CHECK (id = 1),
  pause_autotrade BOOLEAN NOT NULL DEFAULT FALSE,
  maintenance_mode BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
""")
        await conn.execute("""
        INSERT INTO autotrade_bot_settings(id) VALUES (1)
ON CONFLICT (id) DO NOTHING;
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

        # ------------------------
        # Auto-trade (separate from manual tracking)
        # ------------------------
        # Settings are per user and split by market (spot/futures).
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS autotrade_settings (
          user_id BIGINT PRIMARY KEY,
          spot_enabled BOOLEAN NOT NULL DEFAULT FALSE,
          futures_enabled BOOLEAN NOT NULL DEFAULT FALSE,

          spot_exchange TEXT NOT NULL DEFAULT 'binance' CHECK (spot_exchange IN ('binance','bybit')),
          futures_exchange TEXT NOT NULL DEFAULT 'binance' CHECK (futures_exchange IN ('binance','bybit')),

          -- Amounts are validated both in bot UI and DB. 0 means "not set".
          spot_amount_per_trade NUMERIC(18,8) NOT NULL DEFAULT 0
            CHECK (spot_amount_per_trade = 0 OR spot_amount_per_trade >= 10),
          futures_margin_per_trade NUMERIC(18,8) NOT NULL DEFAULT 0
            CHECK (futures_margin_per_trade = 0 OR futures_margin_per_trade >= 5),
          futures_leverage INT NOT NULL DEFAULT 1,
          futures_cap NUMERIC(18,8) NOT NULL DEFAULT 0,

          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        # Add constraints for existing installations (CREATE TABLE IF NOT EXISTS won't update old schema).
        await conn.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_autotrade_spot_min') THEN
            ALTER TABLE autotrade_settings
              ADD CONSTRAINT ck_autotrade_spot_min
              CHECK (spot_amount_per_trade = 0 OR spot_amount_per_trade >= 10);
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_autotrade_futures_min') THEN
            ALTER TABLE autotrade_settings
              ADD CONSTRAINT ck_autotrade_futures_min
              CHECK (futures_margin_per_trade = 0 OR futures_margin_per_trade >= 5);
          END IF;
        END $$;
        """)

        # Exchange keys are stored encrypted (ciphertext only).
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS autotrade_keys (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          user_id BIGINT NOT NULL,
          exchange TEXT NOT NULL CHECK (exchange IN ('binance','bybit')),
          market_type TEXT NOT NULL CHECK (market_type IN ('spot','futures')),
          api_key_enc TEXT,
          api_secret_enc TEXT,
          passphrase_enc TEXT,
          is_active BOOLEAN NOT NULL DEFAULT FALSE,
          last_ok_at TIMESTAMPTZ,
          last_error TEXT,
          last_error_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (user_id, exchange, market_type)
        );
        """)

        # Stores allocated amounts for open autotrade positions (used to count "used" caps).
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS autotrade_positions (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          user_id BIGINT NOT NULL,
          signal_id BIGINT,
          exchange TEXT NOT NULL CHECK (exchange IN ('binance','bybit')),
          market_type TEXT NOT NULL CHECK (market_type IN ('spot','futures')),
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          allocated_usdt NUMERIC(18,8) NOT NULL DEFAULT 0,
          pnl_usdt NUMERIC(18,8),
          roi_percent NUMERIC(18,8),
          status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','CLOSED','ERROR')),
          api_order_ref TEXT,
          opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          closed_at TIMESTAMPTZ,
          UNIQUE (user_id, signal_id, exchange, market_type)
        );
        """)

        # Backward-compatible migrations for existing DBs
        try:
            await conn.execute("ALTER TABLE autotrade_positions ADD COLUMN IF NOT EXISTS pnl_usdt NUMERIC(18,8);")
            await conn.execute("ALTER TABLE autotrade_positions ADD COLUMN IF NOT EXISTS roi_percent NUMERIC(18,8);")
        except Exception:
            pass

        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_autotrade_positions_user_open
        ON autotrade_positions (user_id, market_type, status);
        """)

        # Prevent duplicate OPEN positions per symbol (race-condition safe)
        # NOTE: partial unique index; Postgres supports this.
        try:
            await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_autotrade_positions_user_symbol_open
            ON autotrade_positions (user_id, market_type, symbol)
            WHERE status='OPEN';
            """)
        except Exception:
            # Some managed DBs may restrict CREATE INDEX; ignore.
            pass


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


        # --- Auto-trade access (managed from Signal admin panel) ---
        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS autotrade_enabled BOOLEAN NOT NULL DEFAULT FALSE;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add autotrade_enabled")

        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS autotrade_expires_at TIMESTAMPTZ;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add autotrade_expires_at")

        # STOP mode for auto-trade: when admin disables auto-trade while the user
        # still has OPEN autotrade positions, we enter a "STOP" state.
        # In STOP: new positions are not opened, existing ones are managed until closed.
        # When all positions close, bot flips the user to OFF automatically.
        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS autotrade_stop_after_close BOOLEAN NOT NULL DEFAULT FALSE;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add autotrade_stop_after_close")

        # Helpful index; ignore failure on managed DBs.
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);")
        except Exception:
            logger.exception("ensure_users_columns: failed to create idx_users_telegram_id")


async def get_autotrade_access(user_id: int) -> Dict[str, Any]:
    """Return per-user admin access flags for auto-trade.

    Fields:
      - is_blocked
      - autotrade_enabled
      - autotrade_expires_at
      - autotrade_stop_after_close (STOP mode)
    """
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire() as conn:
        r = await conn.fetchrow(
            """
            SELECT
              telegram_id,
              COALESCE(is_blocked,FALSE) AS is_blocked,
              COALESCE(autotrade_enabled,FALSE) AS autotrade_enabled,
              autotrade_expires_at,
              COALESCE(autotrade_stop_after_close,FALSE) AS autotrade_stop_after_close
            FROM users
            WHERE telegram_id=$1
            """,
            uid,
        )
        return dict(r) if r else {
            "telegram_id": uid,
            "is_blocked": False,
            "autotrade_enabled": False,
            "autotrade_expires_at": None,
            "autotrade_stop_after_close": False,
        }


async def count_open_autotrade_positions(user_id: int) -> int:
    """Count OPEN autotrade positions for the user."""
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            SELECT COUNT(1)
            FROM autotrade_positions
            WHERE user_id=$1 AND status='OPEN'
            """,
            uid,
        )
        try:
            return int(n or 0)
        except Exception:
            return 0


async def finalize_autotrade_disable(user_id: int) -> None:
    """Finalize STOP -> OFF when all positions are closed."""
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE users
            SET autotrade_enabled=FALSE,
                autotrade_stop_after_close=FALSE
            WHERE telegram_id=$1
            """,
            uid,
        )


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
            # Prevent duplicate open trades per symbol+market for the same user.
            # If an ACTIVE/TP1 trade exists for this symbol, do not open a new one.
            existing_sym = await conn.fetchval(
                """
                SELECT id
                FROM trades
                WHERE user_id=$1
                  AND UPPER(market)=UPPER($2)
                  AND UPPER(symbol)=UPPER($3)
                  AND status IN ('ACTIVE','TP1')
                  AND closed_at IS NULL
                ORDER BY opened_at DESC
                LIMIT 1;
                """,
                int(user_id), str(market or ''), str(symbol or '')
            )
            if existing_sym is not None:
                return False, None

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
        # IMPORTANT:
        # We must count outcomes per *trade*, not per *event row*.
        # Some setups can emit multiple CLOSE events for the same trade_id.
        # If we simply SUM() event rows, TRADES/PNL% become incorrect.
        row = await conn.fetchrow(
            """
            WITH last_outcome AS (
              SELECT DISTINCT ON (e.trade_id)
                e.trade_id,
                e.event_type,
                COALESCE(e.pnl_pct, 0) AS pnl_pct
              FROM trade_events e
              JOIN trades t ON t.id = e.trade_id
              WHERE t.user_id=$1 AND t.market=$2
                AND e.created_at >= $3 AND e.created_at < $4
                AND e.event_type IN ('WIN','LOSS','BE','CLOSE')
              ORDER BY e.trade_id, e.created_at DESC
            ),
            tp1 AS (
              SELECT COUNT(DISTINCT e.trade_id)::int AS tp1_hits
              FROM trade_events e
              JOIN trades t ON t.id = e.trade_id
              WHERE t.user_id=$1 AND t.market=$2
                AND e.created_at >= $3 AND e.created_at < $4
                AND e.event_type = 'TP1'
            )
            SELECT
              COUNT(*)::int AS trades,
              SUM(CASE WHEN event_type='WIN' THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN event_type='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN event_type='BE' THEN 1 ELSE 0 END)::int AS be,
              (SELECT tp1_hits FROM tp1)::int AS tp1_hits,
              COALESCE(SUM(pnl_pct), 0)::float AS sum_pnl_pct
            FROM last_outcome;
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
        # IMPORTANT:
        # Count outcomes per trade_id (last outcome within the period), not per event row.
        row = await conn.fetchrow(
            """
            WITH last_outcome AS (
              SELECT DISTINCT ON (e.trade_id)
                e.trade_id,
                e.event_type,
                COALESCE(e.pnl_pct, 0) AS pnl_pct
              FROM trade_events e
              JOIN trades t ON t.id = e.trade_id
              WHERE t.market=$1
                AND e.created_at >= $2 AND e.created_at < $3
                AND e.event_type IN ('WIN','LOSS','BE','CLOSE')
              ORDER BY e.trade_id, e.created_at DESC
            ),
            tp1 AS (
              SELECT COUNT(DISTINCT e.trade_id)::int AS tp1_hits
              FROM trade_events e
              JOIN trades t ON t.id = e.trade_id
              WHERE t.market=$1
                AND e.created_at >= $2 AND e.created_at < $3
                AND e.event_type = 'TP1'
            )
            SELECT
              COUNT(*)::int AS trades,
              SUM(CASE WHEN event_type='WIN' THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN event_type='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN event_type='BE' THEN 1 ELSE 0 END)::int AS be,
              SUM(CASE WHEN event_type='CLOSE' THEN 1 ELSE 0 END)::int AS closes,
              (SELECT tp1_hits FROM tp1)::int AS tp1_hits,
              COALESCE(SUM(pnl_pct), 0)::float AS sum_pnl_pct
            FROM last_outcome;
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


# ---------------- Signal bot settings (global) ----------------

async def get_signal_bot_settings() -> Dict[str, Any]:
    """Get signal bot global settings."""
    pool = get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "SELECT pause_signals, maintenance_mode, updated_at, support_username FROM signal_bot_settings WHERE id=1"
            )
            if not row:
                return {"pause_signals": False, "maintenance_mode": False, "updated_at": None}
            return {
                "pause_signals": bool(row.get("pause_signals")),
                "maintenance_mode": bool(row.get("maintenance_mode")),
                "updated_at": row.get("updated_at"),
            }
        except Exception:
            logger.exception("get_signal_bot_settings failed")
            return {"pause_signals": False, "maintenance_mode": False, "updated_at": None}


async def set_signal_bot_settings(*, pause_signals: bool, maintenance_mode: bool, support_username: str | None = None) -> None:
    """Persist signal bot settings."""
    pool = get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO signal_bot_settings(id, pause_signals, maintenance_mode, support_username, updated_at)
                VALUES (1, $1, $2, $3, NOW())
                ON CONFLICT (id)
                DO UPDATE SET
                    pause_signals = EXCLUDED.pause_signals,
                    maintenance_mode = EXCLUDED.maintenance_mode,
                    support_username = EXCLUDED.support_username,
                    updated_at = NOW();
                """,
                bool(pause_signals),
                bool(maintenance_mode),
                support_username,
            )
        except Exception:
            logger.exception(
                "set_signal_bot_settings failed (pause_signals=%s maintenance_mode=%s support_username=%r)",
                pause_signals, maintenance_mode, support_username
            )
            raise

# ---------------- Auto-trade global settings (admin) ----------------

async def get_autotrade_bot_settings() -> Dict[str, Any]:
    """Get auto-trade global settings."""
    pool = get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "SELECT pause_autotrade, maintenance_mode, updated_at FROM autotrade_bot_settings WHERE id=1"
            )
            if not row:
                return {"pause_autotrade": False, "maintenance_mode": False, "updated_at": None}
            return {
                "pause_autotrade": bool(row.get("pause_autotrade")),
                "maintenance_mode": bool(row.get("maintenance_mode")),
                "updated_at": row.get("updated_at"),
            }
        except Exception:
            logger.exception("get_autotrade_bot_settings failed")
            return {"pause_autotrade": False, "maintenance_mode": False, "updated_at": None}


async def set_autotrade_bot_settings(*, pause_autotrade: bool, maintenance_mode: bool) -> None:
    """Persist auto-trade global settings."""
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO autotrade_bot_settings(id, pause_autotrade, maintenance_mode, updated_at)
            VALUES (1, $1, $2, NOW())
            ON CONFLICT (id)
            DO UPDATE SET pause_autotrade=EXCLUDED.pause_autotrade, maintenance_mode=EXCLUDED.maintenance_mode, updated_at=NOW();
            """,
            bool(pause_autotrade), bool(maintenance_mode),
        )



# ---------------- Auto-trade settings / keys ----------------

async def get_autotrade_settings(user_id: int) -> Dict[str, Any]:
    """Return autotrade settings for the user (with defaults)."""
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT user_id, spot_enabled, futures_enabled,
                   spot_exchange, futures_exchange,
                   spot_amount_per_trade, futures_margin_per_trade,
                   futures_leverage, futures_cap
            FROM autotrade_settings
            WHERE user_id=$1
            """,
            uid,
        )
        if not row:
            # Ensure row exists with defaults
            await conn.execute(
                """
                INSERT INTO autotrade_settings(user_id) VALUES ($1)
                ON CONFLICT (user_id) DO NOTHING;
                """,
                uid,
            )
            return {
                "user_id": uid,
                "spot_enabled": False,
                "futures_enabled": False,
                "spot_exchange": "binance",
                "futures_exchange": "binance",
                "spot_amount_per_trade": 0.0,
                "futures_margin_per_trade": 0.0,
                "futures_leverage": 1,
                "futures_cap": 0.0,
            }
        return {
            "user_id": uid,
            "spot_enabled": bool(row.get("spot_enabled")),
            "futures_enabled": bool(row.get("futures_enabled")),
            "spot_exchange": str(row.get("spot_exchange") or "binance"),
            "futures_exchange": str(row.get("futures_exchange") or "binance"),
            "spot_amount_per_trade": float(row.get("spot_amount_per_trade") or 0.0),
            "futures_margin_per_trade": float(row.get("futures_margin_per_trade") or 0.0),
            "futures_leverage": int(row.get("futures_leverage") or 1),
            "futures_cap": float(row.get("futures_cap") or 0.0),
        }


async def set_autotrade_toggle(user_id: int, market_type: str, enabled: bool) -> None:
    pool = get_pool()
    uid = int(user_id)
    m = (market_type or "").lower().strip()
    col = "spot_enabled" if m == "spot" else "futures_enabled"
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO autotrade_settings(user_id, {col}, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET {col}=EXCLUDED.{col}, updated_at=NOW();
            """,
            uid,
            bool(enabled),
        )


async def toggle_autotrade_toggle(user_id: int, market_type: str) -> bool:
    """Atomically toggle spot/futures enabled flag.

    Prevents race conditions on rapid button presses by avoiding read-then-write in app code.
    Returns the new value.
    """
    pool = get_pool()
    uid = int(user_id)
    m = (market_type or "").lower().strip()
    if m not in ("spot", "futures"):
        m = "spot"
    col = "spot_enabled" if m == "spot" else "futures_enabled"
    async with pool.acquire() as conn:
        new_val = await conn.fetchval(
            f"""
            INSERT INTO autotrade_settings(user_id, {col}, updated_at)
            VALUES ($1, TRUE, NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET {col} = NOT COALESCE(autotrade_settings.{col}, FALSE),
                  updated_at = NOW()
            RETURNING {col};
            """,
            uid,
        )
    return bool(new_val)

async def set_autotrade_exchange(user_id: int, market_type: str, exchange: str) -> None:
    pool = get_pool()
    uid = int(user_id)
    m = (market_type or "").lower().strip()
    ex = (exchange or "binance").lower().strip()
    if ex not in ("binance", "bybit"):
        ex = "binance"
    col = "spot_exchange" if m == "spot" else "futures_exchange"
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO autotrade_settings(user_id, {col}, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET {col}=EXCLUDED.{col}, updated_at=NOW();
            """,
            uid,
            ex,
        )


async def set_autotrade_amount(user_id: int, market_type: str, amount_usdt: float) -> None:
    pool = get_pool()
    uid = int(user_id)
    m = (market_type or "").lower().strip()
    col = "spot_amount_per_trade" if m == "spot" else "futures_margin_per_trade"
    amt = float(amount_usdt or 0.0)
    if amt < 0:
        amt = 0.0
    # Hard minimums (also enforced in DB schema):
    # - SPOT amount per trade >= 10 USDT (or 0 when unset)
    # - FUTURES margin per trade >= 5 USDT (or 0 when unset)
    if m == "spot" and 0 < amt < 10:
        raise ValueError("SPOT amount per trade must be >= 10")
    if m != "spot" and 0 < amt < 5:
        raise ValueError("FUTURES margin per trade must be >= 5")
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO autotrade_settings(user_id, {col}, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET {col}=EXCLUDED.{col}, updated_at=NOW();
            """,
            uid,
            amt,
        )


async def set_autotrade_futures_leverage(user_id: int, leverage: int) -> None:
    pool = get_pool()
    uid = int(user_id)
    lev = int(leverage or 1)
    if lev < 1:
        lev = 1
    if lev > 125:
        lev = 125
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO autotrade_settings(user_id, futures_leverage, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET futures_leverage=EXCLUDED.futures_leverage, updated_at=NOW();
            """,
            uid,
            lev,
        )


async def set_autotrade_futures_cap(user_id: int, cap_usdt: float) -> None:
    pool = get_pool()
    uid = int(user_id)
    cap = float(cap_usdt or 0.0)
    if cap < 0:
        cap = 0.0
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO autotrade_settings(user_id, futures_cap, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET futures_cap=EXCLUDED.futures_cap, updated_at=NOW();
            """,
            uid,
            cap,
        )


async def get_autotrade_keys_status(user_id: int) -> List[Dict[str, Any]]:
    """Return list with statuses for all stored keys."""
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT exchange, market_type, is_active, last_ok_at, last_error, last_error_at
            FROM autotrade_keys
            WHERE user_id=$1
            ORDER BY exchange, market_type;
            """,
            uid,
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "exchange": str(r.get("exchange")),
                    "market_type": str(r.get("market_type")),
                    "is_active": bool(r.get("is_active")),
                    "last_ok_at": r.get("last_ok_at"),
                    "last_error": r.get("last_error"),
                    "last_error_at": r.get("last_error_at"),
                }
            )
        return out


async def get_autotrade_keys_row(*, user_id: int, exchange: str, market_type: str) -> Optional[Dict[str, Any]]:
    """Return encrypted key row for (user_id, exchange, market_type).

    NOTE: returns ciphertext only. Decryption must happen outside db_store.
    """
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    if ex not in ("binance", "bybit"):
        ex = "binance"
    if mt not in ("spot", "futures"):
        mt = "spot"
    async with pool.acquire() as conn:
        r = await conn.fetchrow(
            """
            SELECT user_id, exchange, market_type, api_key_enc, api_secret_enc, passphrase_enc,
                   is_active, last_ok_at, last_error, last_error_at
            FROM autotrade_keys
            WHERE user_id=$1 AND exchange=$2 AND market_type=$3
            """,
            uid,
            ex,
            mt,
        )
        return dict(r) if r else None


async def create_autotrade_position(
    *,
    user_id: int,
    signal_id: Optional[int],
    exchange: str,
    market_type: str,
    symbol: str,
    side: str,
    allocated_usdt: float,
    api_order_ref: Optional[str] = None,
) -> None:
    """Insert or update autotrade position row as OPEN."""
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    sym = str(symbol or "").upper().strip()
    side = str(side or "").upper().strip()
    if ex not in ("binance", "bybit"):
        ex = "binance"
    if mt not in ("spot", "futures"):
        mt = "spot"
    if not sym:
        sym = "UNKNOWN"
    if side not in ("BUY", "SELL"):
        side = "BUY"
    alloc = float(allocated_usdt or 0.0)
    async with pool.acquire() as conn:
        # Prevent duplicates: if there is already an OPEN position for this symbol, do nothing.
        exists = await conn.fetchval(
            """
            SELECT 1 FROM autotrade_positions
            WHERE user_id=$1 AND market_type=$2 AND symbol=$3 AND status='OPEN'
            LIMIT 1;
            """,
            uid, mt, sym,
        )
        if exists:
            return

        await conn.execute(
            """
            INSERT INTO autotrade_positions(user_id, signal_id, exchange, market_type, symbol, side, allocated_usdt, status, api_order_ref, opened_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,'OPEN',$8,NOW())
            ON CONFLICT (user_id, signal_id, exchange, market_type)
            DO UPDATE SET allocated_usdt=EXCLUDED.allocated_usdt,
                          status='OPEN',
                          api_order_ref=EXCLUDED.api_order_ref,
                          opened_at=NOW(),
                          closed_at=NULL;
            """,
            uid,
            signal_id,
            ex,
            mt,
            sym,
            side,
            alloc,
            api_order_ref,
        )


async def close_autotrade_position(
    *,
    user_id: int,
    signal_id: Optional[int],
    exchange: str,
    market_type: str,
    status: str = "CLOSED",
    pnl_usdt: Optional[float] = None,
    roi_percent: Optional[float] = None,
) -> None:
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    st = (status or "CLOSED").upper().strip()
    if st not in ("CLOSED", "ERROR"):
        st = "CLOSED"
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE autotrade_positions
            SET status=$5,
                closed_at=NOW(),
                pnl_usdt=COALESCE($6, pnl_usdt),
                roi_percent=COALESCE($7, roi_percent)
            WHERE user_id=$1 AND signal_id=$2 AND exchange=$3 AND market_type=$4;
            """,
            uid,
            signal_id,
            ex,
            mt,
            st,
            pnl_usdt,
            roi_percent,
        )


async def list_open_autotrade_positions(*, limit: int = 200) -> List[Dict[str, Any]]:
    pool = get_pool()
    lim = int(limit or 200)
    if lim < 1:
        lim = 1
    if lim > 2000:
        lim = 2000
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, user_id, signal_id, exchange, market_type, symbol, side, allocated_usdt, api_order_ref, opened_at
            FROM autotrade_positions
            WHERE status='OPEN'
            ORDER BY opened_at ASC
            LIMIT $1;
            """,
            lim,
        )
        return [dict(r) for r in rows]


async def update_autotrade_order_ref(*, row_id: int, api_order_ref: str) -> None:
    pool = get_pool()
    rid = int(row_id)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE autotrade_positions
            SET api_order_ref=$2
            WHERE id=$1;
            """,
            rid,
            api_order_ref,
        )


async def upsert_autotrade_keys(
    *,
    user_id: int,
    exchange: str,
    market_type: str,
    api_key_enc: str | None,
    api_secret_enc: str | None,
    passphrase_enc: str | None = None,
    is_active: bool = False,
    last_error: str | None = None,
) -> None:
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    if ex not in ("binance", "bybit"):
        ex = "binance"
    if mt not in ("spot", "futures"):
        mt = "spot"
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO autotrade_keys(user_id, exchange, market_type, api_key_enc, api_secret_enc, passphrase_enc,
                                      is_active, last_ok_at, last_error, last_error_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7, CASE WHEN $7 AND $8::TEXT IS NULL THEN NOW() ELSE NULL END, $8::TEXT, CASE WHEN $8::TEXT IS NULL THEN NULL ELSE NOW() END, NOW())
            ON CONFLICT (user_id, exchange, market_type)
            DO UPDATE SET api_key_enc=EXCLUDED.api_key_enc,
                          api_secret_enc=EXCLUDED.api_secret_enc,
                          passphrase_enc=EXCLUDED.passphrase_enc,
                          is_active=EXCLUDED.is_active,
                          last_ok_at=CASE WHEN EXCLUDED.is_active AND EXCLUDED.last_error IS NULL THEN NOW() ELSE autotrade_keys.last_ok_at END,
                          last_error=EXCLUDED.last_error,
                          last_error_at=EXCLUDED.last_error_at,
                          updated_at=NOW();
            """,
            uid,
            ex,
            mt,
            api_key_enc,
            api_secret_enc,
            passphrase_enc,
            bool(is_active),
            last_error,
        )


async def mark_autotrade_key_error(
    *,
    user_id: int,
    exchange: str,
    market_type: str,
    error: str,
    deactivate: bool = False,
) -> None:
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    async with pool.acquire() as conn:
        # Only deactivate keys on *credential/permission* failures. Network/rate-limit/order-parameter
        # errors should not permanently disable a key.
        if bool(deactivate):
            await conn.execute(
                """
                INSERT INTO autotrade_keys(user_id, exchange, market_type, is_active, last_error, last_error_at, updated_at)
                VALUES ($1,$2,$3,FALSE,$4,NOW(),NOW())
                ON CONFLICT (user_id, exchange, market_type)
                DO UPDATE SET is_active=FALSE, last_error=$4, last_error_at=NOW(), updated_at=NOW();
                """,
                uid,
                ex,
                mt,
                (error or "API error")[:500],
            )
        else:
            await conn.execute(
                """
                INSERT INTO autotrade_keys(user_id, exchange, market_type, is_active, last_error, last_error_at, updated_at)
                VALUES ($1,$2,$3,TRUE,$4,NOW(),NOW())
                ON CONFLICT (user_id, exchange, market_type)
                DO UPDATE SET last_error=$4, last_error_at=NOW(), updated_at=NOW();
                """,
                uid,
                ex,
                mt,
                (error or "API error")[:500],
            )

async def mark_autotrade_key_ok(
    *,
    user_id: int,
    exchange: str,
    market_type: str,
) -> None:
    """Mark stored keys as verified/working.

    Sets last_ok_at=NOW(), clears last_error/last_error_at, keeps is_active=TRUE.
    """
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    if ex not in ("binance", "bybit"):
        ex = "binance"
    if mt not in ("spot", "futures"):
        mt = "spot"
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO autotrade_keys(user_id, exchange, market_type, is_active, last_ok_at, last_error, last_error_at, updated_at)
            VALUES ($1,$2,$3,TRUE,NOW(),NULL,NULL,NOW())
            ON CONFLICT (user_id, exchange, market_type)
            DO UPDATE SET is_active=TRUE,
                          last_ok_at=NOW(),
                          last_error=NULL,
                          last_error_at=NULL,
                          updated_at=NOW();
            """,
            uid,
            ex,
            mt,
        )

async def get_autotrade_used_usdt(user_id: int, market_type: str) -> float:
    pool = get_pool()
    uid = int(user_id)
    mt = (market_type or "spot").lower().strip()
    async with pool.acquire() as conn:
        v = await conn.fetchval(
            """
            SELECT COALESCE(SUM(allocated_usdt), 0)
            FROM autotrade_positions
            WHERE user_id=$1 AND market_type=$2 AND status='OPEN'
            """,
            uid,
            mt,
        )
        try:
            return float(v or 0.0)
        except Exception:
            return 0.0


async def get_autotrade_stats(
    *,
    user_id: int,
    market_type: str = "all",  # 'spot' | 'futures' | 'all'
    period: str = "today",     # 'today' | 'week' | 'month'
) -> Dict[str, Any]:
    """Return Auto-trade stats for the period.

    Counting rules:
    - opened: positions opened within the period
    - closed (+/-): positions CLOSED within the period based on pnl_usdt sign
    - active: current OPEN positions (not period-bound)
    - pnl: sum of pnl_usdt for CLOSED positions within the period
    - roi_percent: (sum pnl) / (sum allocated_usdt of closed) * 100
    """
    pool = get_pool()
    uid = int(user_id)
    mt = (market_type or "all").lower().strip()
    if mt not in ("spot", "futures", "all"):
        mt = "all"
    pr = (period or "today").lower().strip()
    if pr not in ("today", "week", "month"):
        pr = "today"
    start_expr = {
        "today": "date_trunc('day', NOW())",
        "week": "date_trunc('week', NOW())",
        "month": "date_trunc('month', NOW())",
    }[pr]

    mt_where = "" if mt == "all" else "AND market_type=$2"

    async with pool.acquire() as conn:
        # opened
        if mt == "all":
            opened = await conn.fetchval(
                f"""
                SELECT COUNT(*)
                FROM autotrade_positions
                WHERE user_id=$1 AND opened_at >= {start_expr}
                """,
                uid,
            )
        else:
            opened = await conn.fetchval(
                f"""
                SELECT COUNT(*)
                FROM autotrade_positions
                WHERE user_id=$1 {mt_where} AND opened_at >= {start_expr}
                """,
                uid,
                mt,
            )

        # closed + / - and pnl + invested for ROI
        if mt == "all":
            row = await conn.fetchrow(
                f"""
                SELECT
                  COALESCE(SUM(CASE WHEN status='CLOSED' AND closed_at >= {start_expr} AND COALESCE(pnl_usdt,0) > 0 THEN 1 ELSE 0 END),0) AS closed_plus,
                  COALESCE(SUM(CASE WHEN status='CLOSED' AND closed_at >= {start_expr} AND COALESCE(pnl_usdt,0) < 0 THEN 1 ELSE 0 END),0) AS closed_minus,
                  COALESCE(SUM(CASE WHEN status='CLOSED' AND closed_at >= {start_expr} THEN COALESCE(pnl_usdt,0) ELSE 0 END),0) AS pnl_total,
                  COALESCE(SUM(CASE WHEN status='CLOSED' AND closed_at >= {start_expr} THEN COALESCE(allocated_usdt,0) ELSE 0 END),0) AS invested_closed
                FROM autotrade_positions
                WHERE user_id=$1;
                """,
                uid,
            )
        else:
            row = await conn.fetchrow(
                f"""
                SELECT
                  COALESCE(SUM(CASE WHEN status='CLOSED' AND closed_at >= {start_expr} AND COALESCE(pnl_usdt,0) > 0 THEN 1 ELSE 0 END),0) AS closed_plus,
                  COALESCE(SUM(CASE WHEN status='CLOSED' AND closed_at >= {start_expr} AND COALESCE(pnl_usdt,0) < 0 THEN 1 ELSE 0 END),0) AS closed_minus,
                  COALESCE(SUM(CASE WHEN status='CLOSED' AND closed_at >= {start_expr} THEN COALESCE(pnl_usdt,0) ELSE 0 END),0) AS pnl_total,
                  COALESCE(SUM(CASE WHEN status='CLOSED' AND closed_at >= {start_expr} THEN COALESCE(allocated_usdt,0) ELSE 0 END),0) AS invested_closed
                FROM autotrade_positions
                WHERE user_id=$1 AND market_type=$2;
                """,
                uid,
                mt,
            )

        # active (current)
        if mt == "all":
            active = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM autotrade_positions
                WHERE user_id=$1 AND status='OPEN'
                """,
                uid,
            )
        else:
            active = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM autotrade_positions
                WHERE user_id=$1 AND market_type=$2 AND status='OPEN'
                """,
                uid,
                mt,
            )

    closed_plus = int(row["closed_plus"] or 0)
    closed_minus = int(row["closed_minus"] or 0)
    pnl_total = float(row["pnl_total"] or 0.0)
    invested_closed = float(row["invested_closed"] or 0.0)
    roi = (pnl_total / invested_closed * 100.0) if invested_closed > 0 else 0.0

    return {
        "opened": int(opened or 0),
        "closed_plus": closed_plus,
        "closed_minus": closed_minus,
        "active": int(active or 0),
        "pnl_total": pnl_total,
        "roi_percent": roi,
        "invested_closed": invested_closed,
    }
