
from __future__ import annotations

import asyncpg
import datetime as dt
import logging
import hashlib
import os
import json
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
def utcnow():
    return dt.datetime.now(dt.timezone.utc)

_pool: Optional[asyncpg.Pool] = None

logger = logging.getLogger("db_store")

def set_pool(pool: asyncpg.Pool) -> None:
    global _pool
    _pool = pool

def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized. Call set_pool() first.")
    return _pool

def _db_acquire_timeout() -> float:
    try:
        return float(os.getenv("DB_POOL_ACQUIRE_TIMEOUT_SEC", "25"))
    except Exception:
        return 25.0


def _autotrade_period_start(period: str) -> dt.datetime:
    """Return UTC start timestamp for the requested auto-trade stats bucket.

    Use the bot/app timezone first (TZ_NAME), then legacy TZ, then UTC.
    This keeps the stats card aligned with the UI labels like "Сегодня" / "Неделя".
    """
    pr = (period or "today").lower().strip()
    if pr not in ("today", "week", "month"):
        pr = "today"
    tz_name = (os.getenv("TZ_NAME") or os.getenv("TZ") or "UTC").strip() or "UTC"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = dt.timezone.utc
    now_local = dt.datetime.now(tz)
    if pr == "today":
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    elif pr == "week":
        start_local = (now_local - dt.timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_local = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start_local.astimezone(dt.timezone.utc)


async def ensure_users_table() -> None:
    """Create users table for fresh installations (new Postgres).

    Some deployments used a shared users table created by another service.
    When we move to a dedicated Postgres for this bot, we must create it here.
    """
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
          telegram_id BIGINT PRIMARY KEY,
          is_blocked BOOLEAN NOT NULL DEFAULT FALSE,

          notify_signals BOOLEAN NOT NULL DEFAULT TRUE,

          -- Signal bot access flags
          signal_enabled BOOLEAN NOT NULL DEFAULT FALSE,
          signal_expires_at TIMESTAMPTZ,

          -- Legacy shared access (compat)
          expires_at TIMESTAMPTZ,

          -- Auto-trade access (admin-managed)
          autotrade_enabled BOOLEAN NOT NULL DEFAULT FALSE,
          autotrade_expires_at TIMESTAMPTZ,
          autotrade_stop_after_close BOOLEAN NOT NULL DEFAULT FALSE,

          -- Referral access (manual override when global flag is OFF)
          referral_enabled BOOLEAN NOT NULL DEFAULT FALSE,

          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);")
        except Exception:
            pass

async def ensure_schema() -> None:
    """
    Creates tables needed for persistent trades/statistics.
    Uses IDENTITY (auto increment) columns.
    """
    pool = get_pool()
    await ensure_users_table()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:

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
          exchange TEXT NOT NULL DEFAULT 'manual',
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          entry NUMERIC(18,8) NOT NULL,
          tp1   NUMERIC(18,8),
          tp2   NUMERIC(18,8),
          sl    NUMERIC(18,8),
          status TEXT NOT NULL CHECK (status IN ('ACTIVE','TP1','BE','WIN','LOSS','CLOSED')),
          tp1_hit BOOLEAN NOT NULL DEFAULT FALSE,
          be_price NUMERIC(18,8),
          linked_position_id BIGINT,
          managed_by TEXT,
          opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          closed_at TIMESTAMPTZ,
          pnl_total_pct NUMERIC(10,4),
          tp1_pnl_pct NUMERIC(10,4),
          orig_text TEXT NOT NULL
        );
        """)

        try:
            await conn.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS exchange TEXT NOT NULL DEFAULT 'manual';")
            await conn.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS linked_position_id BIGINT;")
            await conn.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS managed_by TEXT;")
            await conn.execute("UPDATE trades SET exchange='manual' WHERE exchange IS NULL OR exchange='';")
        except Exception:
            pass

        # --- Signal bot global settings (single row) ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_bot_settings (
          id INT PRIMARY KEY CHECK (id = 1),
          pause_signals BOOLEAN NOT NULL DEFAULT FALSE,
          maintenance_mode BOOLEAN NOT NULL DEFAULT FALSE,
          referral_enabled BOOLEAN NOT NULL DEFAULT FALSE,
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

        await conn.execute("""
        ALTER TABLE signal_bot_settings
        ADD COLUMN IF NOT EXISTS referral_enabled BOOLEAN NOT NULL DEFAULT FALSE;
        """)

        # --- Generic KV store for small persistent JSON state (e.g., MID autotune) ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS kv_store (
          key TEXT PRIMARY KEY,
          value JSONB NOT NULL DEFAULT '{}'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        # ensure_kv_store_jsonb: migrate older TEXT schema to JSONB (best-effort)
        try:
            col = await conn.fetchrow(
                """
                SELECT data_type, udt_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'kv_store'
                  AND column_name = 'value'
                """
            )
            udt = (col.get('udt_name') if col else None)
            if udt and str(udt).lower() != 'jsonb':
                # Try cast existing values to jsonb; if fails, keep TEXT but kv_set_json will handle it.
                await conn.execute(
                    """
                    ALTER TABLE kv_store
                    ALTER COLUMN value TYPE JSONB
                    USING CASE
                        WHEN value IS NULL OR value='' THEN '{}'::jsonb
                        ELSE value::jsonb
                    END;
                    """
                )
        except Exception:
            pass

        


        
        # --- Persistent candles cache (binary, not JSON) ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS candles_cache (
          key TEXT PRIMARY KEY,
          payload BYTEA NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_candles_cache_updated ON candles_cache(updated_at);")
        except Exception:
            pass



        # --- Referral system ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS referral_rewards (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          referrer_id BIGINT NOT NULL,
          referral_user_id BIGINT NOT NULL UNIQUE,
          order_id TEXT,
          payment_amount NUMERIC(18,8) NOT NULL DEFAULT 0,
          reward_percent NUMERIC(8,4) NOT NULL DEFAULT 10.0,
          reward_amount NUMERIC(18,8) NOT NULL DEFAULT 0,
          currency TEXT NOT NULL DEFAULT 'USDT',
          status TEXT NOT NULL DEFAULT 'available',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          paid_at TIMESTAMPTZ
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS referral_withdrawal_requests (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          telegram_id BIGINT NOT NULL,
          amount NUMERIC(18,8) NOT NULL,
          currency TEXT NOT NULL DEFAULT 'USDT',
          network TEXT NOT NULL DEFAULT 'BSC',
          wallet_address TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          admin_chat_id BIGINT,
          admin_message_id BIGINT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          processed_at TIMESTAMPTZ,
          processed_by BIGINT,
          admin_comment TEXT
        );
        """)
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_referral_rewards_referrer ON referral_rewards(referrer_id, created_at DESC);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_referral_withdrawals_user ON referral_withdrawal_requests(telegram_id, created_at DESC);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_referral_withdrawals_status ON referral_withdrawal_requests(status, created_at DESC);")
        except Exception:
            pass

        # --- Subscription payments (NOWPayments) ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS subscription_orders (
          order_id TEXT PRIMARY KEY,
          telegram_id BIGINT NOT NULL,
          plan TEXT NOT NULL,
          amount NUMERIC(18,8) NOT NULL,
          currency TEXT NOT NULL DEFAULT 'USD',
          provider TEXT NOT NULL DEFAULT 'nowpayments',
          status TEXT NOT NULL DEFAULT 'pending',
          provider_payment_id TEXT,
          pay_currency TEXT,
          pay_amount NUMERIC(18,8),
          pay_address TEXT,
          pay_url TEXT,
          payload JSONB NOT NULL DEFAULT '{}'::jsonb,
          txid TEXT,
          paid_at TIMESTAMPTZ,
          processed_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS subscription_payments (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          order_id TEXT NOT NULL REFERENCES subscription_orders(order_id) ON DELETE CASCADE,
          telegram_id BIGINT NOT NULL,
          plan TEXT NOT NULL,
          amount NUMERIC(18,8) NOT NULL,
          currency TEXT NOT NULL DEFAULT 'USD',
          pay_currency TEXT,
          pay_amount NUMERIC(18,8),
          provider TEXT NOT NULL DEFAULT 'nowpayments',
          provider_payment_id TEXT,
          txid TEXT,
          status TEXT NOT NULL,
          payload JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          paid_at TIMESTAMPTZ
        );
        """)
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_subscription_orders_tid ON subscription_orders(telegram_id, created_at DESC);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_subscription_orders_status ON subscription_orders(status, created_at DESC);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_subscription_payments_tid ON subscription_payments(telegram_id, created_at DESC);")
        except Exception:
            pass
        try:
            await conn.execute("CREATE OR REPLACE VIEW orders AS SELECT * FROM subscription_orders;")
            await conn.execute("CREATE OR REPLACE VIEW payments AS SELECT * FROM subscription_payments;")
        except Exception:
            pass

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
        DROP INDEX IF EXISTS uq_trades_user_signal_active;

        -- Allow one ACTIVE trade per user+signal+exchange. This lets the same signal run
        -- independently on multiple exchanges while still preventing duplicate active rows
        -- on the same venue.
        CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_user_signal_exchange_active
        ON trades (user_id, signal_id, exchange)
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

          spot_exchange_priority TEXT NOT NULL DEFAULT 'binance,bybit,okx,mexc,gateio',

          spot_exchange TEXT NOT NULL DEFAULT 'binance' CHECK (spot_exchange IN ('binance','bybit','okx','mexc','gateio')),
          futures_exchange TEXT NOT NULL DEFAULT 'binance' CHECK (futures_exchange IN ('binance','bybit','okx','mexc','gateio')),

          -- Amounts are validated both in bot UI and DB. 0 means "not set".
          spot_amount_per_trade NUMERIC(18,8) NOT NULL DEFAULT 0
            CHECK (spot_amount_per_trade = 0 OR spot_amount_per_trade >= 15),
          futures_margin_per_trade NUMERIC(18,8) NOT NULL DEFAULT 0
            CHECK (futures_margin_per_trade = 0 OR futures_margin_per_trade >= 10),
          futures_leverage INT NOT NULL DEFAULT 1,
          futures_cap NUMERIC(18,8) NOT NULL DEFAULT 0,

          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        # Add constraints for existing installations (CREATE TABLE IF NOT EXISTS won't update old schema).
        # Ensure spot_exchange_priority column exists (priority order for SPOT auto-trade)
        try:
            await conn.execute("""
            ALTER TABLE autotrade_settings
              ADD COLUMN IF NOT EXISTS spot_exchange_priority TEXT NOT NULL DEFAULT 'binance,bybit,okx,mexc,gateio';
            """)
        except Exception:
            pass

        # Persisted Smart Risk Manager state (effective futures cap) — survives restarts.
        # These columns are optional and added for existing installations.
        try:
            await conn.execute("""
            ALTER TABLE autotrade_settings
              ADD COLUMN IF NOT EXISTS effective_futures_cap NUMERIC(18,8),
              ADD COLUMN IF NOT EXISTS effective_futures_cap_updated_at TIMESTAMPTZ,
              ADD COLUMN IF NOT EXISTS effective_futures_cap_notify_at TIMESTAMPTZ;
            """)
        except Exception:
            pass

        # Add constraints for existing installations (CREATE TABLE IF NOT EXISTS won't update old schema).
        await conn.execute("""DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_autotrade_spot_min') THEN
            ALTER TABLE autotrade_settings
              ADD CONSTRAINT ck_autotrade_spot_min
              CHECK (spot_amount_per_trade = 0 OR spot_amount_per_trade >= 15);
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_autotrade_futures_min') THEN
            ALTER TABLE autotrade_settings
              ADD CONSTRAINT ck_autotrade_futures_min
              CHECK (futures_margin_per_trade = 0 OR futures_margin_per_trade >= 10);
          END IF;
        END $$;
        """)

        # Exchange keys are stored encrypted (ciphertext only).
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS autotrade_keys (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          user_id BIGINT NOT NULL,
          exchange TEXT NOT NULL CHECK (exchange IN ('binance','bybit','okx','mexc','gateio')),
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

        # --- widen exchange CHECK constraints for spot (migrations) ---
        # Older installations may have CHECK(exchange IN ('binance','bybit')) which blocks new exchanges.
        # Attempt to drop and recreate standard check constraints (best-effort).
        try:
            await conn.execute("ALTER TABLE autotrade_keys DROP CONSTRAINT IF EXISTS autotrade_keys_exchange_check;")
        except Exception:
            pass
        try:
            await conn.execute("ALTER TABLE autotrade_keys ADD CONSTRAINT autotrade_keys_exchange_check CHECK (exchange IN ('binance','bybit','okx','mexc','gateio'));")
        except Exception:
            pass

        try:
            await conn.execute("ALTER TABLE autotrade_positions DROP CONSTRAINT IF EXISTS autotrade_positions_exchange_check;")
        except Exception:
            pass
        try:
            await conn.execute("ALTER TABLE autotrade_positions ADD CONSTRAINT autotrade_positions_exchange_check CHECK (exchange IN ('binance','bybit','okx','mexc','gateio'));")
        except Exception:
            pass


        try:
            await conn.execute("ALTER TABLE autotrade_positions ADD COLUMN IF NOT EXISTS meta JSONB NOT NULL DEFAULT '{}'::jsonb;")
        except Exception:
            pass

        # Optional: speed up meta queries (best-effort)
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_autotrade_positions_meta_gin ON autotrade_positions USING GIN (meta);")
        except Exception:
            pass

        # --- cluster manager lease/lock columns (for multi-replica autotrade manager) ---
        try:
            await conn.execute("ALTER TABLE autotrade_positions ADD COLUMN IF NOT EXISTS mgr_owner TEXT;")
        except Exception:
            pass
        try:
            await conn.execute("ALTER TABLE autotrade_positions ADD COLUMN IF NOT EXISTS mgr_lock_until TIMESTAMPTZ;")
        except Exception:
            pass
        try:
            await conn.execute("ALTER TABLE autotrade_positions ADD COLUMN IF NOT EXISTS mgr_lock_acquired_at TIMESTAMPTZ;")
        except Exception:
            pass


        try:
            await conn.execute("ALTER TABLE autotrade_settings DROP CONSTRAINT IF EXISTS autotrade_settings_spot_exchange_check;")
        except Exception:
            pass
        try:
            await conn.execute("ALTER TABLE autotrade_settings ADD CONSTRAINT autotrade_settings_spot_exchange_check CHECK (spot_exchange IN ('binance','bybit','okx','mexc','gateio'));")
        except Exception:
            pass

        # Stores allocated amounts for open autotrade positions (used to count "used" caps).
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS autotrade_positions (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          user_id BIGINT NOT NULL,
          signal_id BIGINT,
          exchange TEXT NOT NULL CHECK (exchange IN ('binance','bybit','okx','mexc','gateio')),
          market_type TEXT NOT NULL CHECK (market_type IN ('spot','futures')),
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          allocated_usdt NUMERIC(18,8) NOT NULL DEFAULT 0,
          pnl_usdt NUMERIC(18,8),
          roi_percent NUMERIC(18,8),
          status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','CLOSED','ERROR')),
          api_order_ref TEXT,
          meta JSONB NOT NULL DEFAULT '{}'::jsonb,
          linked_trade_id BIGINT,
          mgr_owner TEXT,
          mgr_lock_until TIMESTAMPTZ,
          mgr_lock_acquired_at TIMESTAMPTZ,
          opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          closed_at TIMESTAMPTZ
        );
        """)

        # Backward-compatible migrations for existing DBs
        try:
            await conn.execute("ALTER TABLE autotrade_positions ADD COLUMN IF NOT EXISTS pnl_usdt NUMERIC(18,8);")
            await conn.execute("ALTER TABLE autotrade_positions ADD COLUMN IF NOT EXISTS roi_percent NUMERIC(18,8);")
            await conn.execute("ALTER TABLE autotrade_positions ADD COLUMN IF NOT EXISTS linked_trade_id BIGINT;")
        except Exception:
            pass

        # Legacy builds used a full UNIQUE(user_id, signal_id, exchange, market_type),
        # which prevented keeping historical rows and could reopen/update the wrong position.
        # The manager now relies on OPEN-only uniqueness per concrete venue+symbol+side.
        try:
            await conn.execute("""
            DO $$
            DECLARE
              c RECORD;
            BEGIN
              FOR c IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid='autotrade_positions'::regclass
                  AND contype='u'
                  AND pg_get_constraintdef(oid) ILIKE '%(user_id, signal_id, exchange, market_type)%'
              LOOP
                EXECUTE format('ALTER TABLE autotrade_positions DROP CONSTRAINT IF EXISTS %I;', c.conname);
              END LOOP;
            END $$;
            """)
        except Exception:
            pass

        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_autotrade_positions_user_open
        ON autotrade_positions (user_id, market_type, status);
        """)

        
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_autotrade_positions_mgr_lock
        ON autotrade_positions (status, mgr_lock_until);
        """)
# Prevent duplicate OPEN positions per symbol (race-condition safe)
        # NOTE: partial unique index; Postgres supports this.
        try:
            await conn.execute("DROP INDEX IF EXISTS idx_autotrade_positions_user_symbol_open;")
            await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_autotrade_positions_user_exchange_symbol_side_open
            ON autotrade_positions (user_id, exchange, market_type, symbol, side)
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


        # --- bot-level signal outcome tracking (independent from users) ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_tracks (
          signal_id BIGINT PRIMARY KEY,
          sig_key TEXT UNIQUE,
          market TEXT NOT NULL CHECK (market IN ('SPOT','FUTURES')),
          symbol TEXT NOT NULL,
          side TEXT NOT NULL CHECK (side IN ('LONG','SHORT')),
          entry NUMERIC(18,8) NOT NULL,
          tp1   NUMERIC(18,8),
          tp2   NUMERIC(18,8),
          sl    NUMERIC(18,8),
          status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','TP1','BE','WIN','LOSS','CLOSED')),
          tp1_hit BOOLEAN NOT NULL DEFAULT FALSE,
          tp1_hit_at TIMESTAMPTZ,
          be_price NUMERIC(18,8),
          be_armed_at TIMESTAMPTZ,
          be_crossed_at TIMESTAMPTZ,
          opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          closed_at TIMESTAMPTZ,
          pnl_total_pct NUMERIC(10,4),
          orig_text TEXT,
          setup_source TEXT,
          setup_source_label TEXT,
          ui_setup_label TEXT,
          emit_route TEXT,
          timeframe TEXT,
          confidence INT,
          rr NUMERIC(10,4),
          confirmations TEXT,
          source_exchange TEXT,
          risk_note TEXT,
          close_reason_code TEXT,
          close_reason_text TEXT,
          weak_filters TEXT,
          improve_note TEXT
        );
        """)

        # Backward-compatible migrations for existing DBs
        try:
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS tp1_hit_at TIMESTAMPTZ;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS be_armed_at TIMESTAMPTZ;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS be_crossed_at TIMESTAMPTZ;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS pnl_total_pct NUMERIC(10,4);")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS tp1_pnl_pct NUMERIC(10,4);")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS orig_text TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS setup_source TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS setup_source_label TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS ui_setup_label TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS emit_route TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS timeframe TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS confidence INT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS rr NUMERIC(10,4);")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS confirmations TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS source_exchange TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS risk_note TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS close_reason_code TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS close_reason_text TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS weak_filters TEXT;")
            await conn.execute("ALTER TABLE signal_tracks ADD COLUMN IF NOT EXISTS improve_note TEXT;")
        except Exception:
            pass

        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_tracks_market_status ON signal_tracks(market, status);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_tracks_closed_at ON signal_tracks(closed_at);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_tracks_tp1_hit_at ON signal_tracks(tp1_hit_at);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_tracks_market_symbol_opened_at ON signal_tracks(market, symbol, opened_at DESC);")
        except Exception:
            pass


async def ensure_users_columns() -> None:
    """Best-effort schema migration for users table.

    We keep it here (db_store) so bot.py stays clean and all DB migrations
    live in one place.
    """
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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

        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS referrer_id BIGINT;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add referrer_id")

        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS referral_enabled BOOLEAN NOT NULL DEFAULT FALSE;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add referral_enabled")

        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS ref_available_balance NUMERIC(18,8) NOT NULL DEFAULT 0;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add ref_available_balance")

        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS ref_hold_balance NUMERIC(18,8) NOT NULL DEFAULT 0;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add ref_hold_balance")

        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS ref_withdrawn_balance NUMERIC(18,8) NOT NULL DEFAULT 0;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add ref_withdrawn_balance")

        try:
            await conn.execute(
                """
                ALTER TABLE users
                  ADD COLUMN IF NOT EXISTS ref_total_earned NUMERIC(18,8) NOT NULL DEFAULT 0;
                """
            )
        except Exception:
            logger.exception("ensure_users_columns: failed to add ref_total_earned")

        # Helpful indexes; ignore failure on managed DBs.
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);")
        except Exception:
            logger.exception("ensure_users_columns: failed to create idx_users_telegram_id")
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_referral_enabled ON users(referral_enabled) WHERE referral_enabled = TRUE;")
        except Exception:
            logger.exception("ensure_users_columns: failed to create idx_users_referral_enabled")


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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE users
            SET autotrade_enabled=FALSE,
                autotrade_stop_after_close=FALSE
            WHERE telegram_id=$1
            """,
            uid,
        )


async def ensure_user_signal_trial(user_id: int, referrer_id: int | None = None) -> None:
    """Create user row if missing and grant 24h Signal trial ONCE.

    IMPORTANT: does NOT touch Arbitrage access.
    """
    if not user_id:
        return

    uid = int(user_id)
    clean_referrer_id = int(referrer_id) if referrer_id else None
    if clean_referrer_id == uid:
        clean_referrer_id = None

    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            INSERT INTO users (
                telegram_id,
                notify_signals,
                signal_enabled,
                signal_expires_at,
                autotrade_enabled,
                autotrade_expires_at,
                referrer_id
            )
            VALUES (
                $1,
                TRUE,
                TRUE,
                (NOW() AT TIME ZONE 'UTC') + INTERVAL '24 hours',
                TRUE,
                (NOW() AT TIME ZONE 'UTC') + INTERVAL '24 hours',
                $2
            )
            ON CONFLICT (telegram_id) DO NOTHING;
            """,
            uid, clean_referrer_id,
        )

async def next_signal_id() -> int:
    """Return a globally unique signal_id for bot callbacks (Postgres sequence)."""
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    exchange: str | None = None,
    linked_position_id: int | None = None,
    managed_by: str | None = None,
) -> Tuple[bool, Optional[int]]:
    """
    Inserts a new trade if not already opened (unique user_id+signal_id).
    Returns (inserted, trade_db_id).
    """
    pool = get_pool()
    ex_name = str(exchange or 'manual').lower().strip() or 'manual'
    linked_pos = int(linked_position_id) if linked_position_id is not None else None
    mgr_name = str(managed_by or '').strip() or None
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
                  AND LOWER(COALESCE(exchange,'manual'))=LOWER($4)
                  AND UPPER(side)=UPPER($5)
                  AND status IN ('ACTIVE','TP1')
                  AND closed_at IS NULL
                ORDER BY opened_at DESC
                LIMIT 1;
                """,
                int(user_id), str(market or ''), str(symbol or ''), ex_name, str(side or '')
            )
            if existing_sym is not None:
                return False, None

            async def _insert(with_signal_id: int) -> Optional[int]:
                r = await conn.fetchrow(
                    """
                    INSERT INTO trades (user_id, signal_id, market, exchange, symbol, side, entry, tp1, tp2, sl, status, orig_text, linked_position_id, managed_by)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'ACTIVE',$11,$12,$13)
                    ON CONFLICT (user_id, signal_id, exchange) WHERE status IN ('ACTIVE','TP1') DO NOTHING
                    RETURNING id;
                    """,
                    int(user_id), int(with_signal_id), market, ex_name, symbol, side, float(entry),
                    (float(tp1) if tp1 is not None else None),
                    (float(tp2) if tp2 is not None else None),
                    (float(sl) if sl is not None else None),
                    orig_text or "",
                    linked_pos,
                    mgr_name,
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
                SELECT id, market, exchange, status, closed_at, symbol
                FROM trades
                WHERE user_id=$1 AND signal_id=$2 AND LOWER(COALESCE(exchange,'manual'))=LOWER($3)
                ORDER BY opened_at DESC
                LIMIT 1;
                """,
                int(user_id), int(signal_id), ex_name,
            )
            if existing:
                ex_market = str(existing.get("market") or "").upper()
                ex_exchange = str(existing.get("exchange") or "manual").lower()
                st = str(existing.get("status") or "").upper()
                closed_at = existing.get("closed_at")
                ex_symbol = str(existing.get('symbol') or '').upper()
                # Truly active -> treat as already opened
                if (
                    ex_market == str(market or "").upper()
                    and ex_exchange == ex_name
                    and ex_symbol == str(symbol or "").upper()
                    and str(existing.get("side") or "").upper() == str(side or "").upper()
                    and st in ("ACTIVE", "TP1")
                    and closed_at is None
                ):
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        rows = await conn.fetch(
            f"""
            SELECT t.id, t.user_id, t.signal_id, t.market, t.exchange, t.symbol, t.side, t.entry, t.tp1, t.tp2, t.sl,
                   t.status, t.tp1_hit, t.be_price, t.linked_position_id, t.managed_by, t.opened_at, t.closed_at, t.pnl_total_pct, t.orig_text,
                   e_tp1.tp1_at
            FROM trades t
            LEFT JOIN LATERAL (
              SELECT e.created_at AS tp1_at
              FROM trade_events e
              WHERE e.trade_id=t.id AND e.event_type='TP1'
              ORDER BY e.created_at DESC
              LIMIT 1
            ) e_tp1 ON TRUE
            WHERE {where.replace('user_id=$1', 't.user_id=$1')}
            ORDER BY opened_at DESC
            LIMIT {int(limit)};
            """,
            int(user_id),
        )
        return [dict(r) for r in rows]

async def get_trade_by_user_signal(user_id: int, signal_id: int, *, exchange: str | None = None) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            SELECT t.id, t.user_id, t.signal_id, t.market, t.exchange, t.symbol, t.side, t.entry, t.tp1, t.tp2, t.sl,
                   t.status, t.tp1_hit, t.be_price, t.linked_position_id, t.managed_by, t.opened_at, t.closed_at, t.pnl_total_pct, t.orig_text,
                   e_tp1.tp1_at
            FROM trades t
            LEFT JOIN LATERAL (
              SELECT e.created_at AS tp1_at
              FROM trade_events e
              WHERE e.trade_id=t.id AND e.event_type='TP1'
              ORDER BY e.created_at DESC
              LIMIT 1
            ) e_tp1 ON TRUE
            WHERE t.user_id=$1 AND t.signal_id=$2
              AND ($3::TEXT IS NULL OR LOWER(COALESCE(t.exchange,'manual'))=LOWER($3))
            ORDER BY t.opened_at DESC
            LIMIT 1
            """,
            int(user_id), int(signal_id), (str(exchange).lower() if exchange else None)
        )
        return dict(row) if row else None


async def get_trade_by_id(user_id: int, trade_id: int) -> Optional[Dict[str, Any]]:
    """Fetch a trade by its DB id, ensuring it belongs to the user."""
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            SELECT t.id, t.user_id, t.signal_id, t.market, t.exchange, t.symbol, t.side, t.entry, t.tp1, t.tp2, t.sl,
                   t.status, t.tp1_hit, t.be_price, t.linked_position_id, t.managed_by, t.opened_at, t.closed_at, t.pnl_total_pct, t.orig_text,
                   e_tp1.tp1_at
            FROM trades t
            LEFT JOIN LATERAL (
              SELECT e.created_at AS tp1_at
              FROM trade_events e
              WHERE e.trade_id=t.id AND e.event_type='TP1'
              ORDER BY e.created_at DESC
              LIMIT 1
            ) e_tp1 ON TRUE
            WHERE t.user_id=$1 AND t.id=$2
            """,
            int(user_id), int(trade_id)
        )
        return dict(row) if row else None

async def list_active_trades(limit: int = 500) -> List[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        rows = await conn.fetch(
            """
            SELECT t.id, t.user_id, t.signal_id, t.market, t.exchange, t.symbol, t.side, t.entry, t.tp1, t.tp2, t.sl,
                   t.status, t.tp1_hit, t.be_price, t.linked_position_id, t.managed_by, t.opened_at, t.closed_at, t.pnl_total_pct, t.orig_text,
                   e_tp1.tp1_at
            FROM trades t
            LEFT JOIN LATERAL (
              SELECT e.created_at AS tp1_at
              FROM trade_events e
              WHERE e.trade_id=t.id AND e.event_type='TP1'
              ORDER BY e.created_at DESC
              LIMIT 1
            ) e_tp1 ON TRUE
            WHERE t.status IN ('ACTIVE','TP1')
            ORDER BY t.opened_at DESC
            LIMIT $1
            """,
            int(limit),
        )
        return [dict(r) for r in rows]


async def update_trade_autotrade_link(*, trade_id: int, position_id: int | None = None, exchange: str | None = None, managed_by: str | None = None) -> None:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE trades
            SET linked_position_id=COALESCE($2, linked_position_id),
                exchange=COALESCE($3, exchange),
                managed_by=COALESCE($4, managed_by)
            WHERE id=$1;
            """,
            int(trade_id),
            (int(position_id) if position_id is not None else None),
            (str(exchange).lower() if exchange else None),
            (str(managed_by) if managed_by else None),
        )


async def get_autotrade_position_by_id(row_id: int) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            SELECT id, user_id, signal_id, exchange, market_type, symbol, side, allocated_usdt, pnl_usdt, roi_percent,
                   status, api_order_ref, meta, linked_trade_id, opened_at, closed_at
            FROM autotrade_positions
            WHERE id=$1
            LIMIT 1;
            """,
            int(row_id),
        )
        return dict(row) if row else None


async def find_autotrade_position_for_trade(
    *,
    user_id: int,
    signal_id: int,
    market: str,
    symbol: str,
    exchange: str | None = None,
    side: str | None = None,
    trade_id: int | None = None,
    include_closed: bool = True,
) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    mt = 'futures' if str(market or '').upper() == 'FUTURES' else 'spot'
    ex = str(exchange).lower() if exchange else None
    pos_side = str(side or '').upper().strip() or None
    if pos_side in ('LONG', 'BUY'):
        pos_side = 'BUY'
    elif pos_side in ('SHORT', 'SELL'):
        pos_side = 'SELL'
    linked_trade_id = int(trade_id) if trade_id is not None else None
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            SELECT id, user_id, signal_id, exchange, market_type, symbol, side, allocated_usdt, pnl_usdt, roi_percent,
                   status, api_order_ref, meta, linked_trade_id, opened_at, closed_at
            FROM autotrade_positions
            WHERE user_id=$1
              AND (
                    ($6::BIGINT IS NOT NULL AND linked_trade_id=$6)
                    OR (
                        signal_id=$2
                        AND market_type=$3
                        AND UPPER(symbol)=UPPER($4)
                        AND ($5::TEXT IS NULL OR LOWER(exchange)=LOWER($5))
                        AND ($7::TEXT IS NULL OR UPPER(side)=UPPER($7))
                    )
                  )
              AND ($8::BOOL OR status='OPEN')
            ORDER BY
              CASE WHEN ($6::BIGINT IS NOT NULL AND linked_trade_id=$6) THEN 0 ELSE 1 END,
              CASE WHEN status='OPEN' THEN 0 ELSE 1 END,
              opened_at DESC,
              id DESC
            LIMIT 1;
            """,
            int(user_id), int(signal_id), mt, str(symbol or ''), ex, linked_trade_id, pos_side, bool(include_closed),
        )
        return dict(row) if row else None


async def update_autotrade_trade_link(*, row_id: int, trade_id: int | None = None) -> None:
    pool = get_pool()
    rid = int(row_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE autotrade_positions
            SET linked_trade_id=COALESCE($2, linked_trade_id)
            WHERE id=$1;
            """,
            rid,
            (int(trade_id) if trade_id is not None else None),
        )


async def set_tp1(trade_id: int, *, be_price: float, price: float | None = None, pnl_pct: float | None = None) -> None:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE trades
            SET status='TP1', tp1_hit=TRUE, be_price=$2,
                pnl_total_pct=COALESCE($3, pnl_total_pct)
            WHERE id=$1;
            """,
            int(trade_id), float(be_price), (float(pnl_pct) if pnl_pct is not None else None)
        )
        await conn.execute(
            "INSERT INTO trade_events (trade_id, event_type, price, pnl_pct) VALUES ($1,'TP1',$2,$3);",
            int(trade_id),
            (float(price) if price is not None else None),
            (float(pnl_pct) if pnl_pct is not None else None),
        )


async def set_trade_be_price(trade_id: int, *, be_price: float) -> None:
    """Update BE price for an ACTIVE/TP1 trade (no status change)."""
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE trades
            SET be_price=$2
            WHERE id=$1;
            """,
            int(trade_id), float(be_price)
        )

async def close_trade(trade_id: int, *, status: str, price: float | None = None, pnl_total_pct: float | None = None) -> None:
    """
    status: BE / WIN / LOSS / CLOSED

    NOTE: Some strategies may call close_trade(..., status="HARD_SL") as an emergency stop.
    For database consistency and statistics ("Signals closed / outcomes"), HARD_SL is recorded as LOSS.
    """
    pool = get_pool()
    st = (status or "").upper().strip()

    # Normalize: HARD_SL should be counted as SL/LOSS everywhere.
    if st == "HARD_SL":
        st = "LOSS"

    ev = "CLOSE" if st == "CLOSED" else st
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE trades
            SET status=$2, closed_at=NOW(), pnl_total_pct=$3
            WHERE id=$1;
            """,
            int(trade_id), st, (float(pnl_total_pct) if pnl_total_pct is not None else None),
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
              COALESCE(SUM(CASE WHEN event_type='WIN' THEN ABS(COALESCE(pnl_pct,0)) WHEN event_type='LOSS' THEN -ABS(COALESCE(pnl_pct,0)) WHEN event_type='BE' THEN 0 ELSE COALESCE(pnl_pct,0) END), 0)::float AS sum_pnl_pct
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
              COALESCE(SUM(CASE WHEN event_type='WIN' THEN ABS(COALESCE(pnl_pct,0)) WHEN event_type='LOSS' THEN -ABS(COALESCE(pnl_pct,0)) WHEN event_type='BE' THEN 0 ELSE COALESCE(pnl_pct,0) END), 0)::float AS sum_pnl_pct
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        rows = await conn.fetch(
            """
            SELECT
              (e.created_at AT TIME ZONE $4)::date AS day,
              SUM(CASE WHEN e.event_type IN ('WIN','LOSS','BE','CLOSE') THEN 1 ELSE 0 END)::int AS trades,
              SUM(CASE WHEN e.event_type='WIN' THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN e.event_type='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN e.event_type='BE' THEN 1 ELSE 0 END)::int AS be,
              COUNT(DISTINCT CASE WHEN e.event_type='TP1' THEN e.trade_id END)::int AS tp1_hits,
              COALESCE(SUM(CASE WHEN e.event_type='WIN' THEN ABS(COALESCE(e.pnl_pct,0)) WHEN e.event_type='LOSS' THEN -ABS(COALESCE(e.pnl_pct,0)) WHEN e.event_type='BE' THEN 0 WHEN e.event_type='CLOSE' THEN COALESCE(e.pnl_pct,0) ELSE 0 END), 0)::float AS sum_pnl_pct
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        rows = await conn.fetch(
            """
            SELECT
              to_char(date_trunc('week', (e.created_at AT TIME ZONE $4)), 'IYYY-"W"IW') AS week,
              SUM(CASE WHEN e.event_type IN ('WIN','LOSS','BE','CLOSE') THEN 1 ELSE 0 END)::int AS trades,
              SUM(CASE WHEN e.event_type='WIN' THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN e.event_type='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN e.event_type='BE' THEN 1 ELSE 0 END)::int AS be,
              COUNT(DISTINCT CASE WHEN e.event_type='TP1' THEN e.trade_id END)::int AS tp1_hits,
              COALESCE(SUM(CASE WHEN e.event_type='WIN' THEN ABS(COALESCE(e.pnl_pct,0)) WHEN e.event_type='LOSS' THEN -ABS(COALESCE(e.pnl_pct,0)) WHEN e.event_type='BE' THEN 0 WHEN e.event_type='CLOSE' THEN COALESCE(e.pnl_pct,0) ELSE 0 END), 0)::float AS sum_pnl_pct
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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


async def count_signal_tracks_for_symbol(*, market: str, symbol: str, since: dt.datetime, until: dt.datetime) -> int:
    """Count bot-level signals opened for a (market, symbol) in [since, until).

    Uses signal_tracks.opened_at (created when the bot broadcasts a signal).
    """
    market = str(market or '').upper().strip()
    symbol = str(symbol or '').upper().strip()
    if market not in ('SPOT', 'FUTURES') or not symbol:
        return 0
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            r = await conn.fetchval(
                """
                SELECT COUNT(*)::BIGINT
                FROM signal_tracks
                WHERE market=$1 AND symbol=$2
                  AND opened_at >= $3 AND opened_at < $4;
                """,
                market, symbol, since, until,
            )
            return int(r or 0)
        except Exception:
            logger.exception('count_signal_tracks_for_symbol failed')
            return 0


async def get_last_signal_track_opened_at(*, market: str, symbol: str) -> Optional[dt.datetime]:
    """Return the latest bot-level broadcast time for a (market, symbol).

    Used for rolling per-symbol cooldowns such as "only one signal per 12 hours".
    Returns UTC-aware datetime or None.
    """
    market = str(market or '').upper().strip()
    symbol = str(symbol or '').upper().strip()
    if market not in ('SPOT', 'FUTURES') or not symbol:
        return None
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            row = await conn.fetchrow(
                """
                SELECT MAX(opened_at) AS last_opened_at
                FROM signal_tracks
                WHERE market=$1 AND symbol=$2;
                """,
                market, symbol,
            )
            last_opened_at = row.get('last_opened_at') if row else None
            if last_opened_at is None:
                return None
            if isinstance(last_opened_at, dt.datetime) and last_opened_at.tzinfo is None:
                return last_opened_at.replace(tzinfo=dt.timezone.utc)
            return last_opened_at
        except Exception:
            logger.exception('get_last_signal_track_opened_at failed')
            return None



def _symbol_cooldown_lock_key(*, market: str, symbol: str) -> int:
    raw = f"{str(market or '').upper().strip()}|{str(symbol or '').upper().strip()}".encode('utf-8')
    return int.from_bytes(hashlib.sha1(raw).digest()[:8], "big", signed=False) & 0x7FFFFFFFFFFFFFFF


async def reserve_signal_track_with_symbol_cooldown(
    *,
    signal_id: int,
    sig_key: str,
    market: str,
    symbol: str,
    side: str,
    entry: float,
    tp1: float | None,
    tp2: float | None,
    sl: float | None,
    cooldown_hours: float,
) -> Tuple[bool, Optional[int]]:
    # Atomically reserve a signal_track row under rolling per-symbol cooldown.
    # Returns (reserved, remaining_seconds). Uses a transaction-scoped advisory
    # lock per (market, symbol) to avoid duplicate sends across concurrent workers.
    if not signal_id:
        return False, None
    market = str(market or "").upper().strip()
    symbol = str(symbol or "").upper().strip()
    side = str(side or "").upper().strip()
    if market not in ("SPOT", "FUTURES") or not symbol:
        return False, None
    if side not in ("LONG", "SHORT"):
        side = "LONG"
    try:
        cooldown_hours = float(cooldown_hours or 0.0)
    except Exception:
        cooldown_hours = 0.0
    if cooldown_hours <= 0:
        return False, None
    cooldown_sec = max(0.0, cooldown_hours * 3600.0)
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            async with conn.transaction():
                await conn.execute('SELECT pg_advisory_xact_lock($1::bigint);', _symbol_cooldown_lock_key(market=market, symbol=symbol))
                row = await conn.fetchrow(
                    """
                    SELECT opened_at
                    FROM signal_tracks
                    WHERE market=$1 AND symbol=$2
                    ORDER BY opened_at DESC
                    LIMIT 1;
                    """,
                    market, symbol,
                )
                last_opened_at = row.get('opened_at') if row else None
                if last_opened_at is not None:
                    if isinstance(last_opened_at, dt.datetime) and last_opened_at.tzinfo is None:
                        last_opened_at = last_opened_at.replace(tzinfo=dt.timezone.utc)
                    elapsed_sec = max(0.0, (utcnow() - last_opened_at).total_seconds())
                    if elapsed_sec < cooldown_sec:
                        remaining_sec = max(0, int(round(cooldown_sec - elapsed_sec)))
                        return False, remaining_sec
                inserted = await _upsert_signal_track_conn(
                    conn,
                    signal_id=int(signal_id),
                    sig_key=str(sig_key or ""),
                    market=market,
                    symbol=symbol,
                    side=side,
                    entry=float(entry or 0.0),
                    tp1=(float(tp1) if tp1 is not None else None),
                    tp2=(float(tp2) if tp2 is not None else None),
                    sl=(float(sl) if sl is not None else None),
                )
                if not inserted:
                    logger.info(
                        "reserve_signal_track_with_symbol_cooldown dedup by sig_key: market=%s symbol=%s signal_id=%s",
                        market, symbol, int(signal_id),
                    )
                return True, None
        except Exception:
            logger.exception('reserve_signal_track_with_symbol_cooldown failed')
            return False, None

async def count_signal_tracks_opened_by_market(*, since: dt.datetime, until: dt.datetime) -> Dict[str, int]:
    """Count bot-level signal tracks opened in [since, until) grouped by market.

    This is the most consistent definition of "signals sent" for the dashboard,
    because outcomes are computed from the same signal_tracks table.
    """
    # In some startup/error scenarios the pool might not be initialized yet.
    try:
        pool = get_pool()
    except Exception:
        return {'SPOT': 0, 'FUTURES': 0}

    out: Dict[str, int] = {'SPOT': 0, 'FUTURES': 0}
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT market, COUNT(*)::BIGINT AS n
                FROM signal_tracks
                WHERE opened_at IS NOT NULL
                  AND opened_at >= $1 AND opened_at < $2
                GROUP BY market;
                """,
                since, until,
            )
            for r in rows or []:
                mk = str(r.get('market') or '').upper()
                if mk in out:
                    out[mk] = int(r.get('n') or 0)
        except Exception:
            # If the table doesn't exist yet or DB is unavailable, fail gracefully.
            logger.exception('count_signal_tracks_opened_by_market failed')
    return out




# ---------------- bot-level signal outcome tracking ----------------

async def upsert_signal_track(
    *,
    signal_id: int,
    sig_key: str,
    market: str,
    symbol: str,
    side: str,
    entry: float,
    tp1: float | None,
    tp2: float | None,
    sl: float | None,
    orig_text: str | None = None,
    setup_source: str | None = None,
    setup_source_label: str | None = None,
    ui_setup_label: str | None = None,
    emit_route: str | None = None,
    timeframe: str | None = None,
    confidence: int | None = None,
    rr: float | None = None,
    confirmations: str | None = None,
    source_exchange: str | None = None,
    risk_note: str | None = None,
) -> None:
    """Create/update a bot-level signal tracking row.

    This is independent from user trades. Used for signal performance stats.
    """
    if not signal_id:
        return
    market = str(market or "").upper()
    side = str(side or "").upper()
    if market not in ("SPOT", "FUTURES"):
        market = "SPOT"
    if side not in ("LONG", "SHORT"):
        side = "LONG"
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        inserted = await _upsert_signal_track_conn(
            conn,
            signal_id=int(signal_id),
            sig_key=str(sig_key or ""),
            market=market,
            symbol=str(symbol or "").upper(),
            side=side,
            entry=float(entry or 0.0),
            tp1=(float(tp1) if tp1 is not None else None),
            tp2=(float(tp2) if tp2 is not None else None),
            sl=(float(sl) if sl is not None else None),
            orig_text=(str(orig_text or "") if orig_text is not None else None),
            setup_source=(str(setup_source or "") if setup_source is not None else None),
            setup_source_label=(str(setup_source_label or "") if setup_source_label is not None else None),
            ui_setup_label=(str(ui_setup_label or "") if ui_setup_label is not None else None),
            emit_route=(str(emit_route or "") if emit_route is not None else None),
            timeframe=(str(timeframe or "") if timeframe is not None else None),
            confidence=(int(confidence) if confidence is not None else None),
            rr=(float(rr) if rr is not None else None),
            confirmations=(str(confirmations or "") if confirmations is not None else None),
            source_exchange=(str(source_exchange or "") if source_exchange is not None else None),
            risk_note=(str(risk_note or "") if risk_note is not None else None),
        )
        if not inserted:
            logger.info(
                "upsert_signal_track dedup by sig_key: market=%s symbol=%s signal_id=%s",
                market, str(symbol or "").upper(), int(signal_id),
            )


async def _upsert_signal_track_conn(
    conn,
    *,
    signal_id: int,
    sig_key: str,
    market: str,
    symbol: str,
    side: str,
    entry: float,
    tp1: float | None,
    tp2: float | None,
    sl: float | None,
    orig_text: str | None = None,
    setup_source: str | None = None,
    setup_source_label: str | None = None,
    ui_setup_label: str | None = None,
    emit_route: str | None = None,
    timeframe: str | None = None,
    confidence: int | None = None,
    rr: float | None = None,
    confirmations: str | None = None,
    source_exchange: str | None = None,
    risk_note: str | None = None,
) -> bool:
    """Insert/update signal_tracks safely.

    Returns True on normal insert/update.
    Returns False when an existing UNIQUE(sig_key) row was reused as dedup.
    """
    try:
        await conn.execute(
            """
            INSERT INTO signal_tracks (
              signal_id, sig_key, market, symbol, side, entry, tp1, tp2, sl,
              status, opened_at, updated_at, orig_text, setup_source, setup_source_label, ui_setup_label, emit_route,
              timeframe, confidence, rr, confirmations, source_exchange, risk_note
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'ACTIVE', NOW(), NOW(), $10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
            ON CONFLICT (signal_id)
            DO UPDATE SET
              sig_key=EXCLUDED.sig_key,
              market=EXCLUDED.market,
              symbol=EXCLUDED.symbol,
              side=EXCLUDED.side,
              entry=EXCLUDED.entry,
              tp1=EXCLUDED.tp1,
              tp2=EXCLUDED.tp2,
              sl=EXCLUDED.sl,
              orig_text=COALESCE(EXCLUDED.orig_text, signal_tracks.orig_text),
              setup_source=COALESCE(NULLIF(EXCLUDED.setup_source, ''), signal_tracks.setup_source),
              setup_source_label=COALESCE(NULLIF(EXCLUDED.setup_source_label, ''), signal_tracks.setup_source_label),
              ui_setup_label=COALESCE(NULLIF(EXCLUDED.ui_setup_label, ''), signal_tracks.ui_setup_label),
              emit_route=COALESCE(NULLIF(EXCLUDED.emit_route, ''), signal_tracks.emit_route),
              timeframe=COALESCE(NULLIF(EXCLUDED.timeframe, ''), signal_tracks.timeframe),
              confidence=COALESCE(EXCLUDED.confidence, signal_tracks.confidence),
              rr=COALESCE(EXCLUDED.rr, signal_tracks.rr),
              confirmations=COALESCE(NULLIF(EXCLUDED.confirmations, ''), signal_tracks.confirmations),
              source_exchange=COALESCE(NULLIF(EXCLUDED.source_exchange, ''), signal_tracks.source_exchange),
              risk_note=COALESCE(NULLIF(EXCLUDED.risk_note, ''), signal_tracks.risk_note),
              updated_at=NOW();
            """,
            int(signal_id),
            str(sig_key or ""),
            market,
            symbol,
            side,
            float(entry or 0.0),
            (float(tp1) if tp1 is not None else None),
            (float(tp2) if tp2 is not None else None),
            (float(sl) if sl is not None else None),
            (str(orig_text) if orig_text is not None else None),
            (str(setup_source) if setup_source is not None else None),
            (str(setup_source_label) if setup_source_label is not None else None),
            (str(ui_setup_label) if ui_setup_label is not None else None),
            (str(emit_route) if emit_route is not None else None),
            (str(timeframe) if timeframe is not None else None),
            (int(confidence) if confidence is not None else None),
            (float(rr) if rr is not None else None),
            (str(confirmations) if confirmations is not None else None),
            (str(source_exchange) if source_exchange is not None else None),
            (str(risk_note) if risk_note is not None else None),
        )
        return True
    except asyncpg.UniqueViolationError:
        # Existing row with the same sig_key: treat it as dedup instead of crashing.
        # Keep opened_at / status history intact; refresh only market fields.
        await conn.execute(
            """
            UPDATE signal_tracks
            SET market=$3,
                symbol=$4,
                side=$5,
                entry=$6,
                tp1=$7,
                tp2=$8,
                sl=$9,
                orig_text=COALESCE($10, orig_text),
                setup_source=COALESCE(NULLIF($11, ''), setup_source),
                setup_source_label=COALESCE(NULLIF($12, ''), setup_source_label),
                ui_setup_label=COALESCE(NULLIF($13, ''), ui_setup_label),
                emit_route=COALESCE(NULLIF($14, ''), emit_route),
                timeframe=COALESCE(NULLIF($15, ''), timeframe),
                confidence=COALESCE($16, confidence),
                rr=COALESCE($17, rr),
                confirmations=COALESCE(NULLIF($18, ''), confirmations),
                source_exchange=COALESCE(NULLIF($19, ''), source_exchange),
                risk_note=COALESCE(NULLIF($20, ''), risk_note),
                updated_at=NOW()
            WHERE sig_key=$2;
            """,
            int(signal_id),
            str(sig_key or ""),
            market,
            symbol,
            side,
            float(entry or 0.0),
            (float(tp1) if tp1 is not None else None),
            (float(tp2) if tp2 is not None else None),
            (float(sl) if sl is not None else None),
            (str(orig_text) if orig_text is not None else None),
            (str(setup_source) if setup_source is not None else None),
            (str(setup_source_label) if setup_source_label is not None else None),
            (str(ui_setup_label) if ui_setup_label is not None else None),
            (str(emit_route) if emit_route is not None else None),
            (str(timeframe) if timeframe is not None else None),
            (int(confidence) if confidence is not None else None),
            (float(rr) if rr is not None else None),
            (str(confirmations) if confirmations is not None else None),
            (str(source_exchange) if source_exchange is not None else None),
            (str(risk_note) if risk_note is not None else None),
        )
        return False


async def list_open_signal_tracks(*, limit: int = 500) -> List[dict]:
    """Return ACTIVE/TP1 bot-level signal tracks."""
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        rows = await conn.fetch(
            """
            SELECT *
            FROM signal_tracks
            WHERE status IN ('ACTIVE','TP1')
            ORDER BY opened_at ASC
            LIMIT $1;
            """,
            int(limit),
        )
    return [dict(r) for r in rows]


async def mark_signal_tp1(*, signal_id: int, be_price: float | None = None, tp1_pnl_pct: float | None = None) -> None:
    pool = get_pool()
    # TP1 is always a favorable partial outcome. Normalize to non-negative so
    # old/bad calculations do not poison dashboard PnL.
    norm_tp1_pnl = None
    if tp1_pnl_pct is not None:
        try:
            norm_tp1_pnl = abs(float(tp1_pnl_pct))
        except Exception:
            norm_tp1_pnl = None
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE signal_tracks
            SET status='TP1',
                tp1_hit=TRUE,
                tp1_hit_at=COALESCE(tp1_hit_at, NOW()),
                be_price=COALESCE($2, be_price),
                tp1_pnl_pct=COALESCE($3, tp1_pnl_pct),
                be_armed_at=COALESCE(be_armed_at, NOW()),
                updated_at=NOW()
            WHERE signal_id=$1 AND status='ACTIVE';
            """,
            int(signal_id),
            (float(be_price) if be_price is not None else None),
            norm_tp1_pnl,
        )


async def mark_signal_be_crossed(*, signal_id: int) -> None:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE signal_tracks
            SET be_crossed_at=COALESCE(be_crossed_at, NOW()),
                updated_at=NOW()
            WHERE signal_id=$1 AND status='TP1';
            """,
            int(signal_id),
        )


async def clear_signal_be_crossed(*, signal_id: int) -> None:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE signal_tracks
            SET be_crossed_at=NULL,
                updated_at=NOW()
            WHERE signal_id=$1 AND status='TP1';
            """,
            int(signal_id),
        )


async def close_signal_track(
    *,
    signal_id: int,
    status: str,
    pnl_total_pct: float | None = None,
    close_reason_code: str | None = None,
    close_reason_text: str | None = None,
    weak_filters: str | None = None,
    improve_note: str | None = None,
) -> bool:
    """Close an ACTIVE/TP1 signal track exactly once and store pnl.

    Returns True only when this call transitioned the row from ACTIVE/TP1 to a final
    state. A False return means the track was already closed elsewhere, which keeps
    outcome loops idempotent across concurrent workers/restarts.

    Normalization matters for the admin dashboard:
      - WIN must never contribute negative PnL
      - LOSS must never contribute positive PnL
      - BE should be 0
    This also self-heals if an upstream calculator produced the wrong sign.
    """
    st = str(status or "").upper()
    if st not in ("WIN", "LOSS", "BE", "CLOSED"):
        st = "CLOSED"

    norm_pnl = None
    if pnl_total_pct is not None:
        try:
            raw = float(pnl_total_pct)
            if st == "WIN":
                norm_pnl = abs(raw)
            elif st == "LOSS":
                norm_pnl = -abs(raw)
            elif st == "BE":
                norm_pnl = 0.0
            else:
                norm_pnl = raw
        except Exception:
            norm_pnl = None

    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            UPDATE signal_tracks
            SET status=$2,
                closed_at=NOW(),
                updated_at=NOW(),
                pnl_total_pct=$3,
                close_reason_code=COALESCE($4, close_reason_code),
                close_reason_text=COALESCE($5, close_reason_text),
                weak_filters=COALESCE($6, weak_filters),
                improve_note=COALESCE($7, improve_note)
            WHERE signal_id=$1
              AND status IN ('ACTIVE','TP1')
            RETURNING signal_id;
            """,
            int(signal_id),
            st,
            norm_pnl,
            (str(close_reason_code or "") or None),
            (str(close_reason_text or "") or None),
            (str(weak_filters or "") or None),
            (str(improve_note or "") or None),
        )
    return bool(row)


async def signal_perf_bucket_global(market: str, *, since: dt.datetime, until: dt.datetime) -> Dict[str, Any]:
    """Aggregate bot-level signal outcomes.

    Dashboard expectation: compare **signals sent** vs **signals closed** for the *same cohort*.

    Many systems bucket outcomes by `closed_at` ("what closed today"). But your admin UI is labeled
    like "Signals sent" vs "Signals closed (outcomes)", so the natural expectation is:

      "From the signals we SENT in this period, how many of them have already closed and with what result."

    Therefore default bucketing is **by closed_at** ("what closed in this period").

    Switch to cohort behavior via env:
        SIGNAL_STATS_BUCKET_BY=opened

    Allowed values: closed | opened
    """
    market = str(market or "").upper()
    if market not in ("SPOT", "FUTURES"):
        market = "SPOT"

    bucket_by = (os.getenv("SIGNAL_STATS_BUCKET_BY", "closed") or "closed").strip().lower()
    if bucket_by not in ("opened", "closed"):
        bucket_by = "closed"

    # If DB not ready, be safe for dashboard.
    try:
        pool = get_pool()
    except Exception:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "be": 0,
            "closes": 0,
            "tp1_hits": 0,
            "sum_pnl_pct": 0.0,
        }

    # IMPORTANT (dashboard correctness):
    # The admin UI table shows outcomes as TP2/SL/BE/TP1 and does NOT have a separate column for manual CLOSE.
    # Previously we included status='CLOSED' inside "trades", which made TRADES bigger than TP2+SL+BE (+TP1).
    # To keep the widget consistent, we define:
    #   - trades = terminal outcomes only (WIN/LOSS/BE)
    #   - closes = manual/forced closes (status='CLOSED') returned separately
    if bucket_by == "opened":
        # Cohort: signals opened in [since,until), count terminal outcomes regardless of when they closed.
        where_outcome = "opened_at >= $2 AND opened_at < $3 AND status IN ('WIN','LOSS','BE') AND closed_at IS NOT NULL"
        where_close = "opened_at >= $2 AND opened_at < $3 AND status='CLOSED' AND closed_at IS NOT NULL"
        where_tp1 = "opened_at >= $2 AND opened_at < $3 AND tp1_hit=TRUE"
    else:
        # Legacy: count outcomes that closed within [since,until)
        where_outcome = "status IN ('WIN','LOSS','BE') AND closed_at IS NOT NULL AND closed_at >= $2 AND closed_at < $3"
        where_close = "status='CLOSED' AND closed_at IS NOT NULL AND closed_at >= $2 AND closed_at < $3"
        where_tp1 = "tp1_hit=TRUE AND tp1_hit_at IS NOT NULL AND tp1_hit_at >= $2 AND tp1_hit_at < $3"

    q = f"""
            SELECT
              COUNT(*) FILTER (WHERE {where_outcome})::int AS trades,
              COUNT(*) FILTER (WHERE {where_outcome} AND status='WIN')::int AS wins,
              COUNT(*) FILTER (WHERE {where_outcome} AND status='LOSS')::int AS losses,
              COUNT(*) FILTER (WHERE {where_outcome} AND status='BE')::int AS be,
              COUNT(*) FILTER (WHERE {where_close})::int AS closes,
              COUNT(*) FILTER (WHERE {where_tp1})::int AS tp1_hits,
              (
                COALESCE(SUM(CASE
                    WHEN {where_outcome} AND status='WIN' THEN GREATEST(0, ABS(COALESCE(pnl_total_pct,0)) - ABS(COALESCE(tp1_pnl_pct,0)))
                    WHEN {where_outcome} AND status='LOSS' THEN -GREATEST(0, ABS(COALESCE(pnl_total_pct,0)) - ABS(COALESCE(tp1_pnl_pct,0)))
                    WHEN {where_outcome} AND status='BE' THEN 0
                    ELSE 0
                  END),0)
                + COALESCE(SUM(CASE
                    WHEN {where_tp1} THEN ABS(COALESCE(tp1_pnl_pct,0))
                    ELSE 0
                  END),0)
              )::float AS sum_pnl_pct
            FROM signal_tracks
            WHERE market=$1;
            """

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(q, market, since, until)

    if not row:
        return {"trades": 0, "wins": 0, "losses": 0, "be": 0, "closes": 0, "tp1_hits": 0, "sum_pnl_pct": 0.0}
    return {
        "trades": int(row.get("trades") or 0),
        "wins": int(row.get("wins") or 0),
        "losses": int(row.get("losses") or 0),
        "be": int(row.get("be") or 0),
        "closes": int(row.get("closes") or 0),
        "tp1_hits": int(row.get("tp1_hits") or 0),
        "sum_pnl_pct": float(row.get("sum_pnl_pct") or 0.0),
    }





# ----------------------
# Daily signal report helpers
# ----------------------

_SIGNAL_REPORT_SETUP_KEYS = (
    "origin",
    "breakout",
    "zone_retest",
    "normal_pending_trigger",
    "liquidity_reclaim",
)


def _signal_report_extract_setup_from_text(text: str | None) -> str:
    try:
        src = str(text or "")
        if not src:
            return ""
        m = re.search(r"(?:^|\n)\s*(?:🧭\s*)?Smart-setup:\s*([^\n\r]+)", src, flags=re.IGNORECASE)
        if not m:
            return ""
        return str(m.group(1) or "").strip()
    except Exception:
        return ""


def _signal_report_normalize_setup_label(*, ui_setup_label: str | None = None, emit_route: str | None = None, setup_source_label: str | None = None, setup_source: str | None = None, orig_text: str | None = None) -> str:
    raw = ""
    for candidate in (ui_setup_label, emit_route, setup_source_label, setup_source, _signal_report_extract_setup_from_text(orig_text)):
        try:
            c = str(candidate or "").strip()
        except Exception:
            c = ""
        if c:
            raw = c
            break
    s = str(raw or "").strip().lower().replace('-', '_').replace(' ', '_')
    aliases = {
        "origin": "origin",
        "начало_движения": "origin",
        "breakout": "breakout",
        "пробой": "breakout",
        "zone": "zone_retest",
        "zone_retest": "zone_retest",
        "zone_touch": "zone_retest",
        "zone_touch_retest": "zone_retest",
        "возврат_в_зону": "zone_retest",
        "normal_pending_trigger": "normal_pending_trigger",
        "normal_pending": "normal_pending_trigger",
        "pending": "normal_pending_trigger",
        "pending_trigger": "normal_pending_trigger",
        "обычный_trigger": "normal_pending_trigger",
        "обычный_триггер": "normal_pending_trigger",
        "обычный_pending_trigger": "normal_pending_trigger",
        "liquidity_reclaim": "liquidity_reclaim",
        "liquidity_reclaim_emit": "liquidity_reclaim",
        "liquidity_reclaim_entry": "liquidity_reclaim",
        "liquidity_reclaim_ready": "liquidity_reclaim",
    }
    return aliases.get(s, s if s in _SIGNAL_REPORT_SETUP_KEYS else "")


def _signal_report_empty_bucket() -> Dict[str, Any]:
    return {
        "sent": 0,
        "closed": 0,
        "win": 0,
        "loss": 0,
        "be": 0,
        "manual_close": 0,
        "sum_pnl_pct": 0.0,
    }


def _signal_report_bucket_add_sent(bucket: Dict[str, Any]) -> None:
    bucket["sent"] = int(bucket.get("sent") or 0) + 1


def _signal_report_bucket_add_close(bucket: Dict[str, Any], status: str, pnl_total_pct: float | None) -> None:
    st = str(status or "").upper().strip()
    bucket["closed"] = int(bucket.get("closed") or 0) + 1
    if st == "WIN":
        bucket["win"] = int(bucket.get("win") or 0) + 1
    elif st == "LOSS":
        bucket["loss"] = int(bucket.get("loss") or 0) + 1
    elif st == "BE":
        bucket["be"] = int(bucket.get("be") or 0) + 1
    elif st == "CLOSED":
        bucket["manual_close"] = int(bucket.get("manual_close") or 0) + 1
    try:
        bucket["sum_pnl_pct"] = float(bucket.get("sum_pnl_pct") or 0.0) + float(pnl_total_pct or 0.0)
    except Exception:
        pass


async def signal_report_window_summary(*, since: dt.datetime, until: dt.datetime) -> Dict[str, Any]:
    """Aggregate a daily signal report for the given time window.

    Sent counters use signal_tracks.opened_at in [since, until).
    Outcome counters are calculated for the SAME sent cohort, not for everything
    that happened to close in the window.
    """
    try:
        pool = get_pool()
    except Exception:
        return {
            "overall": _signal_report_empty_bucket(),
            "markets": {"SPOT": _signal_report_empty_bucket(), "FUTURES": _signal_report_empty_bucket()},
            "sides": {"LONG": _signal_report_empty_bucket(), "SHORT": _signal_report_empty_bucket()},
            "setups": {k: _signal_report_empty_bucket() for k in _SIGNAL_REPORT_SETUP_KEYS},
        }

    sent_rows: List[dict] = []
    closed_rows: List[dict] = []
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT signal_id, market, side, orig_text, setup_source, setup_source_label, ui_setup_label, emit_route
                FROM signal_tracks
                WHERE opened_at IS NOT NULL
                  AND opened_at >= $1 AND opened_at < $2;
                """,
                since, until,
            )
            sent_rows = [dict(r) for r in (rows or [])]
        except Exception:
            logger.exception('signal_report_window_summary sent_rows failed')
        signal_ids = [int(r.get("signal_id") or 0) for r in sent_rows if int(r.get("signal_id") or 0) > 0]
        try:
            if signal_ids:
                rows = await conn.fetch(
                    """
                    SELECT market, side, status, pnl_total_pct, orig_text, setup_source, setup_source_label, ui_setup_label, emit_route
                    FROM signal_tracks
                    WHERE signal_id = ANY($1::BIGINT[])
                      AND closed_at IS NOT NULL
                      AND status IN ('WIN','LOSS','BE','CLOSED');
                    """,
                    signal_ids,
                )
                closed_rows = [dict(r) for r in (rows or [])]
        except Exception:
            logger.exception('signal_report_window_summary closed_rows failed')

    out = {
        "overall": _signal_report_empty_bucket(),
        "markets": {"SPOT": _signal_report_empty_bucket(), "FUTURES": _signal_report_empty_bucket()},
        "sides": {"LONG": _signal_report_empty_bucket(), "SHORT": _signal_report_empty_bucket()},
        "setups": {k: _signal_report_empty_bucket() for k in _SIGNAL_REPORT_SETUP_KEYS},
    }

    for row in sent_rows:
        market = str(row.get("market") or "").upper().strip()
        side = str(row.get("side") or "").upper().strip()
        setup = _signal_report_normalize_setup_label(
            ui_setup_label=row.get("ui_setup_label"),
            emit_route=row.get("emit_route"),
            setup_source_label=row.get("setup_source_label"),
            setup_source=row.get("setup_source"),
            orig_text=row.get("orig_text"),
        )
        _signal_report_bucket_add_sent(out["overall"])
        if market in out["markets"]:
            _signal_report_bucket_add_sent(out["markets"][market])
        if side in out["sides"]:
            _signal_report_bucket_add_sent(out["sides"][side])
        if setup in out["setups"]:
            _signal_report_bucket_add_sent(out["setups"][setup])

    for row in closed_rows:
        market = str(row.get("market") or "").upper().strip()
        side = str(row.get("side") or "").upper().strip()
        status = str(row.get("status") or "").upper().strip()
        pnl_total_pct = row.get("pnl_total_pct")
        setup = _signal_report_normalize_setup_label(
            ui_setup_label=row.get("ui_setup_label"),
            emit_route=row.get("emit_route"),
            setup_source_label=row.get("setup_source_label"),
            setup_source=row.get("setup_source"),
            orig_text=row.get("orig_text"),
        )
        _signal_report_bucket_add_close(out["overall"], status, pnl_total_pct)
        if market in out["markets"]:
            _signal_report_bucket_add_close(out["markets"][market], status, pnl_total_pct)
        if side in out["sides"]:
            _signal_report_bucket_add_close(out["sides"][side], status, pnl_total_pct)
        if setup in out["setups"]:
            _signal_report_bucket_add_close(out["setups"][setup], status, pnl_total_pct)

    return out


async def signal_report_window_dataset(*, since: dt.datetime, until: dt.datetime) -> Dict[str, Any]:
    """Return raw rows needed to build the verbose daily report.

    Report logic for the signal bot:
      - include signals SENT in the current window;
      - also include carry-over signals from previous days if they CLOSED in the
        current window.

    This matches the desired operator workflow: if yesterday's signal did not
    close yesterday, it should fall into the next daily report once it closes.

    To keep counters coherent we return:
      - sent_rows = current-window sent rows + carry-over rows that closed now;
      - closed_rows = rows that actually closed in the current window.

    `sent_rows` therefore means "signals included in this report", not strictly
    "opened today only".
    """
    try:
        pool = get_pool()
    except Exception:
        return {"sent_rows": [], "closed_rows": []}

    sent_rows: List[dict] = []
    closed_rows: List[dict] = []
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        sent_by_id: Dict[int, dict] = {}
        try:
            rows = await conn.fetch(
                """
                SELECT signal_id, market, symbol, side, entry, tp1, tp2, sl,
                       opened_at, orig_text, setup_source, setup_source_label, ui_setup_label, emit_route,
                       timeframe, confidence, rr, confirmations, source_exchange, risk_note,
                       close_reason_code, close_reason_text, weak_filters, improve_note
                FROM signal_tracks
                WHERE opened_at IS NOT NULL
                  AND opened_at >= $1 AND opened_at < $2
                ORDER BY opened_at ASC, signal_id ASC;
                """,
                since, until,
            )
            sent_rows = [dict(r) for r in (rows or [])]
            for row in sent_rows:
                sid = int(row.get('signal_id') or 0)
                if sid > 0 and sid not in sent_by_id:
                    sent_by_id[sid] = dict(row)
        except Exception:
            logger.exception('signal_report_window_dataset sent_rows failed')

        try:
            rows = await conn.fetch(
                """
                SELECT signal_id, market, symbol, side, entry, tp1, tp2, sl,
                       status, tp1_hit, pnl_total_pct, opened_at, closed_at,
                       orig_text, setup_source, setup_source_label, ui_setup_label, emit_route,
                       timeframe, confidence, rr, confirmations, source_exchange, risk_note,
                       close_reason_code, close_reason_text, weak_filters, improve_note
                FROM signal_tracks
                WHERE closed_at IS NOT NULL
                  AND closed_at >= $1 AND closed_at < $2
                  AND status IN ('WIN','LOSS','BE','CLOSED')
                ORDER BY closed_at ASC NULLS LAST, signal_id ASC;
                """,
                since, until,
            )
            closed_rows = [dict(r) for r in (rows or [])]
        except Exception:
            logger.exception('signal_report_window_dataset closed_rows failed')

        if closed_rows:
            for row in closed_rows:
                sid = int(row.get('signal_id') or 0)
                if sid <= 0 or sid in sent_by_id:
                    continue
                # carry-over signal from an earlier day that closed in this window:
                # include it in the current report base dataset.
                sent_like = dict(row)
                sent_like.pop('status', None)
                sent_like.pop('tp1_hit', None)
                sent_like.pop('pnl_total_pct', None)
                sent_like.pop('closed_at', None)
                sent_by_id[sid] = sent_like
                sent_rows.append(sent_like)

    sent_rows.sort(key=lambda r: (str(r.get('opened_at') or ''), int(r.get('signal_id') or 0)))
    closed_rows.sort(key=lambda r: (str(r.get('closed_at') or ''), int(r.get('signal_id') or 0)))
    return {
        "sent_rows": sent_rows,
        "closed_rows": closed_rows,
    }



def _signal_loss_diag_empty() -> Dict[str, Any]:
    return {
        "reasons": {},
        "weak_filters": {},
        "improvements": {},
        "examples": [],
    }


def _signal_loss_diag_inc(counter: Dict[str, int], key: str) -> None:
    k = str(key or "").strip()
    if not k:
        return
    counter[k] = int(counter.get(k) or 0) + 1


def _signal_loss_diag_split(text_value: str | None) -> List[str]:
    raw = str(text_value or "")
    if not raw:
        return []
    parts = re.split(r"[,;|]+", raw)
    out: List[str] = []
    for part in parts:
        item = str(part or "").strip()
        if item and item not in out:
            out.append(item)
    return out


async def signal_loss_diagnostics_window(*, since: dt.datetime, until: dt.datetime, limit: int = 5) -> Dict[str, Any]:
    try:
        pool = get_pool()
    except Exception:
        return _signal_loss_diag_empty()

    out = _signal_loss_diag_empty()
    rows: List[dict] = []
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            fetched = await conn.fetch(
                """
                SELECT signal_id, symbol, market, side, pnl_total_pct, closed_at,
                       status, tp1_hit, setup_source, setup_source_label, ui_setup_label, emit_route,
                       close_reason_code, close_reason_text, weak_filters, improve_note
                FROM signal_tracks
                WHERE closed_at IS NOT NULL
                  AND closed_at >= $1 AND closed_at < $2
                  AND status = 'LOSS'
                ORDER BY closed_at DESC;
                """,
                since, until,
            )
            rows = [dict(r) for r in (fetched or [])]
        except Exception:
            logger.exception('signal_loss_diagnostics_window failed')
            return out

    for idx, row in enumerate(rows):
        reason_code = str(row.get('close_reason_code') or '').strip() or 'loss_sl'
        reason_text = str(row.get('close_reason_text') or '').strip()
        improve_note = str(row.get('improve_note') or '').strip()
        weak_filters = _signal_loss_diag_split(row.get('weak_filters'))

        _signal_loss_diag_inc(out['reasons'], reason_code)
        for item in weak_filters:
            _signal_loss_diag_inc(out['weak_filters'], item)
        for item in _signal_loss_diag_split(improve_note):
            _signal_loss_diag_inc(out['improvements'], item)

        if idx < max(0, int(limit or 0)):
            out['examples'].append({
                'signal_id': int(row.get('signal_id') or 0),
                'symbol': str(row.get('symbol') or '').upper(),
                'market': str(row.get('market') or '').upper(),
                'side': str(row.get('side') or '').upper(),
                'pnl_total_pct': float(row.get('pnl_total_pct') or 0.0),
                'closed_at': row.get('closed_at'),
                'reason_code': reason_code,
                'reason_text': reason_text,
                'weak_filters': weak_filters,
                'improve_note': improve_note,
                'setup': _signal_report_normalize_setup_label(
                    ui_setup_label=row.get('ui_setup_label'),
                    emit_route=row.get('emit_route'),
                    setup_source_label=row.get('setup_source_label'),
                    setup_source=row.get('setup_source'),
                    orig_text=None,
                ),
            })

    return out

async def trade_perf_bucket_global(market: str, *, since: dt.datetime, until: dt.datetime) -> Dict[str, Any]:
    """Aggregate AUTO-TRADE outcomes in [since, until) across ALL users.

    This is what the admin dashboard "Signals closed (outcomes)" expects: it reflects
    actual executed trades (trade_events + trades), not theoretical signal tracks.

    We take the *last* terminal outcome per trade (WIN/LOSS/BE/CLOSE) within the window.
    TP1 is counted separately as DISTINCT trades that had a TP1 event in the window.
    """
    market = str(market or "").upper()
    if market not in ("SPOT", "FUTURES"):
        market = "SPOT"

    # If DB not ready, be safe for dashboard.
    try:
        pool = get_pool()
    except Exception:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "be": 0,
            "closes": 0,
            "tp1_hits": 0,
            "sum_pnl_pct": 0.0,
        }

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            WITH last_outcome AS (
              SELECT DISTINCT ON (e.trade_id)
                     e.trade_id,
                     e.event_type,
                     e.pnl_pct,
                     e.created_at
                FROM trade_events e
                JOIN trades t ON t.id = e.trade_id
               WHERE t.market = $1
                 AND e.event_type IN ('WIN','LOSS','BE','CLOSE')
                 AND e.created_at >= $2 AND e.created_at < $3
               ORDER BY e.trade_id, e.created_at DESC
            ),
            tp1 AS (
              SELECT COUNT(DISTINCT e.trade_id)::int AS tp1_hits
                FROM trade_events e
                JOIN trades t ON t.id = e.trade_id
               WHERE t.market = $1
                 AND e.event_type = 'TP1'
                 AND e.created_at >= $2 AND e.created_at < $3
            )
            SELECT
              COUNT(*)::int AS trades,
              SUM(CASE WHEN event_type='WIN'  THEN 1 ELSE 0 END)::int AS wins,
              SUM(CASE WHEN event_type='LOSS' THEN 1 ELSE 0 END)::int AS losses,
              SUM(CASE WHEN event_type='BE'   THEN 1 ELSE 0 END)::int AS be,
              SUM(CASE WHEN event_type='CLOSE' THEN 1 ELSE 0 END)::int AS closes,
              (SELECT tp1_hits FROM tp1)::int AS tp1_hits,
              COALESCE(SUM(CASE WHEN event_type='WIN' THEN ABS(COALESCE(pnl_pct,0)) WHEN event_type='LOSS' THEN -ABS(COALESCE(pnl_pct,0)) WHEN event_type='BE' THEN 0 ELSE COALESCE(pnl_pct,0) END), 0)::float AS sum_pnl_pct
            FROM last_outcome;
            """,
            market,
            since,
            until,
        )

    if not row:
        return {"trades": 0, "wins": 0, "losses": 0, "be": 0, "closes": 0, "tp1_hits": 0, "sum_pnl_pct": 0.0}

    return {
        "trades": int(row.get("trades") or 0),
        "wins": int(row.get("wins") or 0),
        "losses": int(row.get("losses") or 0),
        "be": int(row.get("be") or 0),
        "closes": int(row.get("closes") or 0),
        "tp1_hits": int(row.get("tp1_hits") or 0),
        "sum_pnl_pct": float(row.get("sum_pnl_pct") or 0.0),
    }

# ---------------- Signal bot settings (global) ----------------

async def get_signal_bot_settings() -> Dict[str, Any]:
    """Get signal bot global settings."""
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            row = await conn.fetchrow(
                "SELECT pause_signals, maintenance_mode, referral_enabled, updated_at, support_username FROM signal_bot_settings WHERE id=1"
            )
            if not row:
                return {"pause_signals": False, "maintenance_mode": False, "referral_enabled": False, "updated_at": None, "support_username": None}
            return {
                "pause_signals": bool(row.get("pause_signals")),
                "maintenance_mode": bool(row.get("maintenance_mode")),
                "referral_enabled": bool(row.get("referral_enabled")),
                "updated_at": row.get("updated_at"),
                "support_username": row.get("support_username"),
            }
        except Exception:
            logger.exception("get_signal_bot_settings failed")
            return {"pause_signals": False, "maintenance_mode": False, "referral_enabled": False, "updated_at": None, "support_username": None}


async def set_signal_bot_settings(*, pause_signals: bool, maintenance_mode: bool, support_username: str | None = None, referral_enabled: bool = False) -> None:
    """Persist signal bot settings."""
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            await conn.execute(
                """
                INSERT INTO signal_bot_settings(id, pause_signals, maintenance_mode, support_username, referral_enabled, updated_at)
                VALUES (1, $1, $2, $3, $4, NOW())
                ON CONFLICT (id)
                DO UPDATE SET
                    pause_signals = EXCLUDED.pause_signals,
                    maintenance_mode = EXCLUDED.maintenance_mode,
                    support_username = EXCLUDED.support_username,
                    referral_enabled = EXCLUDED.referral_enabled,
                    updated_at = NOW();
                """,
                bool(pause_signals),
                bool(maintenance_mode),
                support_username,
                bool(referral_enabled),
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            row = await conn.fetchrow(
                "SELECT pause_autotrade, maintenance_mode, updated_at FROM autotrade_bot_settings WHERE id=1"
            )
            if not row:
                return {"pause_autotrade": False, "maintenance_mode": False, "updated_at": None}
            return {
                "pause_autotrade": bool(row.get("pause_autotrade")),
                "maintenance_mode": bool(row.get("maintenance_mode")),
                "referral_enabled": bool(row.get("referral_enabled")),
                "updated_at": row.get("updated_at"),
            }
        except Exception:
            logger.exception("get_autotrade_bot_settings failed")
            return {"pause_autotrade": False, "maintenance_mode": False, "updated_at": None}


async def set_autotrade_bot_settings(*, pause_autotrade: bool, maintenance_mode: bool) -> None:
    """Persist auto-trade global settings."""
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            SELECT user_id, spot_enabled, futures_enabled,
                   spot_exchange_priority,
                   spot_exchange, futures_exchange,
                   spot_amount_per_trade, futures_margin_per_trade,
                   futures_leverage, futures_cap,
                   effective_futures_cap, effective_futures_cap_notify_at
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
                "spot_exchange_priority": "binance,bybit,okx,mexc,gateio",
                "spot_exchange": "binance",
                "futures_exchange": "binance",
                "spot_amount_per_trade": 0.0,
                "futures_margin_per_trade": 0.0,
                "futures_leverage": 1,
                "futures_cap": 0.0,
                "effective_futures_cap": None,
                "effective_futures_cap_notify_at": None,
            }
        return {
            "user_id": uid,
            "spot_enabled": bool(row.get("spot_enabled")),
            "futures_enabled": bool(row.get("futures_enabled")),
            "spot_exchange_priority": str(row.get("spot_exchange_priority") or "binance,bybit,okx,mexc,gateio"),
            "spot_exchange": str(row.get("spot_exchange") or "binance"),
            "futures_exchange": str(row.get("futures_exchange") or "binance"),
            "spot_amount_per_trade": float(row.get("spot_amount_per_trade") or 0.0),
            "futures_margin_per_trade": float(row.get("futures_margin_per_trade") or 0.0),
            "futures_leverage": int(row.get("futures_leverage") or 1),
            "futures_cap": float(row.get("futures_cap") or 0.0),
            "effective_futures_cap": (float(row.get("effective_futures_cap")) if row.get("effective_futures_cap") is not None else None),
            "effective_futures_cap_notify_at": row.get("effective_futures_cap_notify_at"),
        }



async def update_effective_futures_cap_state(user_id: int, new_effective_cap: float) -> Dict[str, Any]:
    """Persist Smart Risk Manager effective futures cap and return previous state.

    Returns:
      { "prev_cap": float|None, "prev_notify_at": datetime|None }
    """
    pool = get_pool()
    uid = int(user_id)
    cap = float(new_effective_cap or 0.0)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        # Ensure the row exists
        await conn.execute(
            """
            INSERT INTO autotrade_settings(user_id) VALUES ($1)
            ON CONFLICT (user_id) DO NOTHING;
            """,
            uid,
        )
        row = await conn.fetchrow(
            """
            WITH prev AS (
              SELECT effective_futures_cap, effective_futures_cap_notify_at
              FROM autotrade_settings
              WHERE user_id=$1
            ),
            upd AS (
              UPDATE autotrade_settings
              SET effective_futures_cap=$2,
                  effective_futures_cap_updated_at=NOW(),
                  updated_at=NOW()
              WHERE user_id=$1
              RETURNING 1
            )
            SELECT prev.effective_futures_cap AS prev_cap,
                   prev.effective_futures_cap_notify_at AS prev_notify_at
            FROM prev;
            """,
            uid,
            cap,
        )
        return {
            "prev_cap": (float(row.get("prev_cap")) if row and row.get("prev_cap") is not None else None),
            "prev_notify_at": (row.get("prev_notify_at") if row else None),
        }


async def set_effective_futures_cap_notify_now(user_id: int) -> None:
    """Mark that we have notified the user about effective cap decrease (cooldown marker)."""
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        # Ensure the row exists
        await conn.execute(
            """
            INSERT INTO autotrade_settings(user_id) VALUES ($1)
            ON CONFLICT (user_id) DO NOTHING;
            """,
            uid,
        )
        await conn.execute(
            """
            UPDATE autotrade_settings
            SET effective_futures_cap_notify_at=NOW(),
                updated_at=NOW()
            WHERE user_id=$1;
            """,
            uid,
        )

async def set_autotrade_toggle(user_id: int, market_type: str, enabled: bool) -> None:
    pool = get_pool()
    uid = int(user_id)
    m = (market_type or "").lower().strip()
    col = "spot_enabled" if m == "spot" else "futures_enabled"
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    if m == "futures":
        # Futures keys are supported for Binance/Bybit/OKX
        if ex not in ("binance", "bybit", "okx"):
            ex = "binance"
    else:
        # Spot keys can be stored for multiple exchanges
        if ex not in ("binance", "bybit", "okx", "mexc", "gateio"):
            ex = "binance"
    col = "spot_exchange" if m == "spot" else "futures_exchange"
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    if m == "spot" and 0 < amt < 15:
        raise ValueError("SPOT amount per trade must be >= 15")
    if m != "spot" and 0 < amt < 10:
        raise ValueError("FUTURES margin per trade must be >= 10")
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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



async def set_spot_exchange_priority(user_id: int, priority: list[str] | str) -> None:
    """Set SPOT exchange priority order for auto-trade.

    Stores a comma-separated list in autotrade_settings.spot_exchange_priority.
    Also updates autotrade_settings.spot_exchange to the first item for UI compatibility.
    """
    pool = get_pool()
    uid = int(user_id)
    # Normalize input
    if isinstance(priority, str):
        items = [p.strip().lower() for p in priority.split(",") if p.strip()]
    else:
        items = [str(p).strip().lower() for p in (priority or []) if str(p).strip()]
    allowed = ["binance", "bybit", "okx", "mexc", "gateio"]
    # keep allowed, unique, preserve order
    out = []
    for x in items:
        if x in allowed and x not in out:
            out.append(x)
    # append missing allowed at end
    for x in allowed:
        if x not in out:
            out.append(x)
    csv = ",".join(out)
    first = out[0] if out else "binance"

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute("INSERT INTO autotrade_settings(user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING;", uid)
        await conn.execute(
            """
            UPDATE autotrade_settings
            SET spot_exchange_priority=$2,
                spot_exchange=$3,
                updated_at=NOW()
            WHERE user_id=$1
            """,
            uid,
            csv,
            first,
        )

async def get_spot_exchange_priority(user_id: int) -> list[str]:
    """Get SPOT exchange priority order."""
    st = await get_autotrade_settings(int(user_id))
    csv = str(st.get("spot_exchange_priority") or "binance,bybit,okx,mexc,gateio")
    items = [p.strip().lower() for p in csv.split(",") if p.strip()]
    allowed = ["binance", "bybit", "okx", "mexc", "gateio"]
    out = [x for x in items if x in allowed]
    for x in allowed:
        if x not in out:
            out.append(x)
    return out


async def get_autotrade_keys_status(user_id: int) -> List[Dict[str, Any]]:
    """Return list with statuses for all stored keys."""
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    if mt == "futures":
        if ex not in ("binance", "bybit", "okx"):
            ex = "binance"
    else:
        if ex not in ("binance", "bybit", "okx", "mexc", "gateio"):
            ex = "binance"
    if mt not in ("spot", "futures"):
        mt = "spot"
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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
    journal_orig_text: Optional[str] = None,
) -> Optional[int]:
    """Insert or refresh an OPEN autotrade position row.

    OPEN uniqueness is per concrete venue+market+symbol+side, while closed rows stay
    in history as separate records. A linked shadow trade is created/updated so the
    manager controls the real position and track_loop mirrors the journal state.
    """
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    sym = str(symbol or "").upper().strip()
    side = str(side or "").upper().strip()
    if mt == "futures":
        if ex not in ("binance", "bybit", "okx"):
            ex = "binance"
    else:
        if ex not in ("binance", "bybit", "okx", "mexc", "gateio"):
            ex = "binance"
    if mt not in ("spot", "futures"):
        mt = "spot"
    if not sym:
        sym = "UNKNOWN"
    if side not in ("BUY", "SELL"):
        side = "BUY"
    alloc = float(allocated_usdt or 0.0)
    pos_id: Optional[int] = None
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            SELECT id, signal_id
            FROM autotrade_positions
            WHERE user_id=$1
              AND exchange=$2
              AND market_type=$3
              AND symbol=$4
              AND side=$5
              AND status='OPEN'
            ORDER BY opened_at DESC, id DESC
            LIMIT 1;
            """,
            uid, ex, mt, sym, side,
        )
        if row:
            pos_id = int(row.get("id") or 0) or None
            try:
                existing_signal_id = int(row.get("signal_id") or 0)
                incoming_signal_id = int(signal_id or 0)
            except Exception:
                existing_signal_id = 0
                incoming_signal_id = 0
            if pos_id and existing_signal_id not in (0, incoming_signal_id):
                return pos_id
            await conn.execute(
                """
                UPDATE autotrade_positions
                SET signal_id=COALESCE(signal_id, $2),
                    allocated_usdt=$3,
                    api_order_ref=COALESCE($4, api_order_ref),
                    status='OPEN',
                    closed_at=NULL
                WHERE id=$1;
                """,
                int(pos_id),
                (int(signal_id) if signal_id is not None else None),
                alloc,
                api_order_ref,
            )
        else:
            row2 = await conn.fetchrow(
                """
                INSERT INTO autotrade_positions(
                    user_id, signal_id, exchange, market_type, symbol, side,
                    allocated_usdt, status, api_order_ref, opened_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,'OPEN',$8,NOW())
                ON CONFLICT (user_id, exchange, market_type, symbol, side) WHERE status='OPEN'
                DO NOTHING
                RETURNING id;
                """,
                uid,
                (int(signal_id) if signal_id is not None else None),
                ex,
                mt,
                sym,
                side,
                alloc,
                api_order_ref,
            )
            if row2 and row2.get("id") is not None:
                pos_id = int(row2["id"])
            else:
                row3 = await conn.fetchrow(
                    """
                    SELECT id, signal_id
                    FROM autotrade_positions
                    WHERE user_id=$1
                      AND exchange=$2
                      AND market_type=$3
                      AND symbol=$4
                      AND side=$5
                      AND status='OPEN'
                    ORDER BY opened_at DESC, id DESC
                    LIMIT 1;
                    """,
                    uid, ex, mt, sym, side,
                )
                if row3 and row3.get("id") is not None:
                    pos_id = int(row3["id"])
                    try:
                        existing_signal_id = int(row3.get("signal_id") or 0)
                        incoming_signal_id = int(signal_id or 0)
                    except Exception:
                        existing_signal_id = 0
                        incoming_signal_id = 0
                    if existing_signal_id in (0, incoming_signal_id):
                        await conn.execute(
                            """
                            UPDATE autotrade_positions
                            SET signal_id=COALESCE(signal_id, $2),
                                allocated_usdt=$3,
                                api_order_ref=COALESCE($4, api_order_ref),
                                status='OPEN',
                                closed_at=NULL
                            WHERE id=$1;
                            """,
                            int(pos_id),
                            (int(signal_id) if signal_id is not None else None),
                            alloc,
                            api_order_ref,
                        )
                    else:
                        return pos_id

    if pos_id and signal_id:
        try:
            ref = {}
            if isinstance(api_order_ref, dict):
                ref = dict(api_order_ref)
            elif api_order_ref:
                ref = json.loads(str(api_order_ref) or "{}") or {}
        except Exception:
            ref = {}

        def _num(v):
            try:
                if v is None or v == "":
                    return None
                return float(v)
            except Exception:
                return None

        market = "FUTURES" if mt == "futures" else "SPOT"
        trade_side = "LONG" if side in ("BUY", "LONG") else "SHORT"
        entry = _num(ref.get("entry_price"))
        if entry is None:
            entry = _num(ref.get("entry")) or 0.0
        tp1 = _num(ref.get("tp1"))
        tp2 = _num(ref.get("tp2"))
        sl = _num(ref.get("sl"))
        orig_text = str(journal_orig_text or f"[AUTOTRADE {ex.upper()} {mt.upper()}]")
        trade_id = None
        try:
            inserted, trade_id = await open_trade_once(
                user_id=uid,
                signal_id=int(signal_id),
                market=market,
                exchange=ex,
                symbol=sym,
                side=trade_side,
                entry=float(entry or 0.0),
                tp1=tp1,
                tp2=tp2,
                sl=sl,
                orig_text=orig_text,
                linked_position_id=int(pos_id),
                managed_by="autotrade_manager",
            )
            if (not inserted) or (trade_id is None):
                tr = await get_trade_by_user_signal(uid, int(signal_id), exchange=ex)
                trade_id = int(tr.get("id") or 0) if tr else None
        except Exception:
            trade_id = None
        if trade_id:
            try:
                await update_trade_autotrade_link(trade_id=int(trade_id), position_id=int(pos_id), exchange=ex, managed_by="autotrade_manager")
            except Exception:
                pass
            try:
                await update_autotrade_trade_link(row_id=int(pos_id), trade_id=int(trade_id))
            except Exception:
                pass
    return pos_id


async def reserve_autotrade_position(
    *,
    user_id: int,
    signal_id: Optional[int],
    exchange: str,
    market_type: str,
    symbol: str,
    side: str,
    allocated_usdt: float,
    reservation_ref: str,
) -> bool:
    """Atomically reserve an OPEN position row *before* placing orders.

    Returns True if reservation inserted, False if the concrete venue+market+symbol+side
    is already reserved/open for the user.
    """
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    sym = (symbol or "").upper().strip()
    side = (side or "BUY").upper().strip()
    alloc = float(allocated_usdt or 0.0)

    if mt == "futures":
        if ex not in ("binance", "bybit", "okx"):
            ex = "binance"
    else:
        if ex not in ("binance", "bybit", "okx", "mexc", "gateio"):
            ex = "binance"
    if mt not in ("spot", "futures"):
        mt = "spot"
    if not sym:
        sym = "UNKNOWN"
    if side not in ("BUY", "SELL"):
        side = "BUY"

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO autotrade_positions(
                user_id, signal_id, exchange, market_type, symbol, side,
                allocated_usdt, status, api_order_ref, opened_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,'OPEN',$8,NOW())
            ON CONFLICT (user_id, exchange, market_type, symbol, side) WHERE status='OPEN'
            DO NOTHING
            RETURNING id;
            """,
            uid,
            (int(signal_id) if signal_id is not None else None),
            ex,
            mt,
            sym,
            side,
            alloc,
            reservation_ref,
        )
        return bool(row and row.get("id") is not None)


async def close_autotrade_position(
    *,
    user_id: int,
    signal_id: Optional[int],
    exchange: str,
    market_type: str,
    symbol: str | None = None,
    side: str | None = None,
    status: str = "CLOSED",
    pnl_usdt: Optional[float] = None,
    roi_percent: Optional[float] = None,
    row_id: int | None = None,
) -> None:
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    sym = str(symbol or "").upper().strip() or None
    pos_side = str(side or "").upper().strip() or None
    if pos_side in ("LONG", "BUY"):
        pos_side = "BUY"
    elif pos_side in ("SHORT", "SELL"):
        pos_side = "SELL"
    else:
        pos_side = None
    st = (status or "CLOSED").upper().strip()
    rid = int(row_id) if row_id is not None else None
    if st not in ("CLOSED", "ERROR"):
        st = "CLOSED"
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        if rid is not None and rid > 0:
            await conn.execute(
                """
                UPDATE autotrade_positions
                SET status=$2,
                    closed_at=NOW(),
                    mgr_owner=NULL,
                    mgr_lock_until=NULL,
                    mgr_lock_acquired_at=NULL,
                    pnl_usdt=COALESCE($3, pnl_usdt),
                    roi_percent=COALESCE($4, roi_percent)
                WHERE id=$1 AND status='OPEN';
                """,
                rid,
                st,
                pnl_usdt,
                roi_percent,
            )
            return

        await conn.execute(
            """
            WITH pick AS (
                SELECT id
                FROM autotrade_positions
                WHERE user_id=$1
                  AND ($2::BIGINT IS NULL OR signal_id=$2)
                  AND exchange=$3
                  AND market_type=$4
                  AND ($5::TEXT IS NULL OR UPPER(symbol)=UPPER($5))
                  AND ($6::TEXT IS NULL OR UPPER(side)=UPPER($6))
                  AND status='OPEN'
                ORDER BY opened_at DESC, id DESC
                LIMIT 1
            )
            UPDATE autotrade_positions ap
            SET status=$7,
                closed_at=NOW(),
                mgr_owner=NULL,
                mgr_lock_until=NULL,
                mgr_lock_acquired_at=NULL,
                pnl_usdt=COALESCE($8, ap.pnl_usdt),
                roi_percent=COALESCE($9, ap.roi_percent)
            FROM pick
            WHERE ap.id=pick.id;
            """,
            uid,
            (int(signal_id) if signal_id is not None else None),
            ex,
            mt,
            sym,
            pos_side,
            st,
            pnl_usdt,
            roi_percent,
        )


async def cleanup_autotrade_reservation(
    *,
    user_id: int,
    market_type: str,
    symbol: str,
    reservation_ref: str,
) -> bool:
    """Delete a reserved/OPEN autotrade position created for dry-run/testing.

    Returns True if something was deleted.
    """
    pool = get_pool()
    uid = int(user_id)
    mt = str(market_type or "spot").lower()
    sym = str(symbol or "").upper().strip()
    ref = str(reservation_ref or "").strip()
    if not ref or not sym:
        return False

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        res = await conn.execute(
            """
            DELETE FROM autotrade_positions
            WHERE user_id=$1
              AND market_type=$2
              AND symbol=$3
              AND api_order_ref=$4
              AND status='OPEN'
            """,
            uid, mt, sym, ref,
        )
        # asyncpg returns like 'DELETE 1'
        try:
            n = int(str(res).split()[-1])
        except Exception:
            n = 0
        return n > 0


async def autotrade_health_snapshot(*, stale_minutes: int = 10, sample_limit: int = 10) -> dict:
    """DB-only health snapshot for autotrade."""
    pool = get_pool()
    sm = int(stale_minutes or 10)
    lim = int(sample_limit or 10)
    if lim < 0:
        lim = 0
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        open_cnt = await conn.fetchval(
            """SELECT COUNT(*) FROM autotrade_positions WHERE status='OPEN'"""
        )
        stale_rows = await conn.fetch(
            """
            SELECT id, user_id, exchange, market_type, symbol, side, api_order_ref, opened_at
            FROM autotrade_positions
            WHERE status='OPEN'
              AND api_order_ref LIKE 'RESERVED:%'
              AND opened_at < (NOW() - ($1::int * INTERVAL '1 minute'))
            ORDER BY opened_at ASC
            LIMIT $2
            """,
            sm, lim,
        )
        stale_cnt = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM autotrade_positions
            WHERE status='OPEN'
              AND api_order_ref LIKE 'RESERVED:%'
              AND opened_at < (NOW() - ($1::int * INTERVAL '1 minute'))
            """,
            sm,
        )
    stale = []
    for r in stale_rows or []:
        stale.append(dict(r))
    return {
        "open_positions": int(open_cnt or 0),
        "stale_reserved_positions": int(stale_cnt or 0),
        "stale_samples": stale,
    }

async def list_open_autotrade_positions(*, limit: int = 200) -> List[Dict[str, Any]]:
    pool = get_pool()
    lim = int(limit or 200)
    if lim < 1:
        lim = 1
    if lim > 2000:
        lim = 2000
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        rows = await conn.fetch(
            """
            SELECT id, user_id, signal_id, exchange, market_type, symbol, side, allocated_usdt, api_order_ref, meta, opened_at
            FROM autotrade_positions
            WHERE status='OPEN'
            ORDER BY opened_at ASC
            LIMIT $1;
            """,
            lim,
        )
        return [dict(r) for r in rows]

async def claim_open_autotrade_positions(*, owner: str, limit: int = 200, lease_sec: int = 90) -> List[Dict[str, Any]]:
    """Claim a batch of OPEN autotrade_positions for this manager worker.

    Uses a DB-level row lock (FOR UPDATE SKIP LOCKED) + lease fields (mgr_owner/mgr_lock_until)
    so multiple replicas can safely run autotrade_manager_loop without double-processing.

    Lease expires automatically if a worker dies.
    """
    pool = get_pool()
    lim = int(limit or 200)
    if lim < 1:
        lim = 1
    if lim > 2000:
        lim = 2000
    lease = int(lease_sec or 90)
    if lease < 5:
        lease = 5
    if lease > 3600:
        lease = 3600
    own = str(owner or "").strip()
    if not own:
        own = "worker"

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        # One statement, protected by row locks.
        rows = await conn.fetch(
            """
            WITH picked AS (
                SELECT id
                FROM autotrade_positions
                WHERE status='OPEN'
                  AND (mgr_lock_until IS NULL OR mgr_lock_until < NOW() OR mgr_owner=$1)
                ORDER BY opened_at ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE autotrade_positions p
            SET mgr_owner=$1,
                mgr_lock_until=NOW() + ($3::int * INTERVAL '1 second'),
                mgr_lock_acquired_at=COALESCE(p.mgr_lock_acquired_at, NOW())
            FROM picked
            WHERE p.id=picked.id
            RETURNING p.id, p.user_id, p.signal_id, p.exchange, p.market_type, p.symbol, p.side,
                      p.allocated_usdt, p.api_order_ref, p.meta, p.opened_at;
            """,
            own,
            lim,
            lease,
        )
        return [dict(r) for r in rows]


async def release_autotrade_position_lock(*, row_id: int, owner: str) -> None:
    """Release manager lease early (optional)."""
    pool = get_pool()
    rid = int(row_id)
    own = str(owner or "").strip()
    if not own:
        own = "worker"
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE autotrade_positions
            SET mgr_lock_until=NULL,
                mgr_owner=NULL,
                mgr_lock_acquired_at=NULL
            WHERE id=$1 AND (mgr_owner=$2 OR mgr_owner IS NULL);
            """,
            rid,
            own,
        )


async def touch_autotrade_position_lock(*, row_id: int, owner: str, lease_sec: int = 90) -> None:
    """Extend lease for a row already owned by this worker."""
    pool = get_pool()
    rid = int(row_id)
    lease = int(lease_sec or 90)
    if lease < 5:
        lease = 5
    if lease > 3600:
        lease = 3600
    own = str(owner or "").strip()
    if not own:
        own = "worker"
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE autotrade_positions
            SET mgr_lock_until=NOW() + ($3::int * INTERVAL '1 second'),
                mgr_owner=$2,
                mgr_lock_acquired_at=COALESCE(mgr_lock_acquired_at, NOW())
            WHERE id=$1 AND status='OPEN' AND (mgr_owner=$2 OR mgr_owner IS NULL OR mgr_lock_until < NOW());
            """,
            rid,
            own,
            lease,
        )



async def update_autotrade_order_ref(*, row_id: int, api_order_ref: str) -> None:
    pool = get_pool()
    rid = int(row_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE autotrade_positions
            SET api_order_ref=$2
            WHERE id=$1;
            """,
            rid,
            api_order_ref,
        )
async def update_autotrade_position_meta(*, row_id: int, meta: Any, replace: bool = False) -> None:
    """Update autotrade_positions.meta (JSONB).

    - If replace=True: meta is fully replaced.
    - If replace=False (default): meta is merged (meta = meta || patch).

    meta can be:
      - dict (will be json-dumped)
      - JSON string
    """
    pool = get_pool()
    rid = int(row_id)

    patch_json = meta
    try:
        if isinstance(meta, dict):
            patch_json = json.dumps(meta, ensure_ascii=False)
        elif meta is None:
            patch_json = "{}"
        else:
            patch_json = str(meta)
    except Exception:
        patch_json = "{}"

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        if replace:
            await conn.execute(
                """
                UPDATE autotrade_positions
                SET meta=$2::jsonb
                WHERE id=$1;
                """,
                rid,
                patch_json,
            )
        else:
            await conn.execute(
                """
                UPDATE autotrade_positions
                SET meta = COALESCE(meta, '{}'::jsonb) || ($2::jsonb)
                WHERE id=$1;
                """,
                rid,
                patch_json,
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
    if mt == "futures":
        # Futures keys are supported for Binance/Bybit/OKX
        if ex not in ("binance", "bybit", "okx"):
            ex = "binance"
    else:
        # Spot keys can be stored for multiple exchanges
        if ex not in ("binance", "bybit", "okx", "mexc", "gateio"):
            ex = "binance"
    if mt not in ("spot", "futures"):
        mt = "spot"
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            INSERT INTO autotrade_keys(user_id, exchange, market_type, api_key_enc, api_secret_enc, passphrase_enc,
                                      is_active, last_ok_at, last_error, last_error_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7, CASE WHEN $7 THEN NOW() ELSE NULL END, $8::TEXT, CASE WHEN $8::TEXT IS NULL THEN NULL ELSE NOW() END, NOW())
            ON CONFLICT (user_id, exchange, market_type)
            DO UPDATE SET api_key_enc=EXCLUDED.api_key_enc,
                          api_secret_enc=EXCLUDED.api_secret_enc,
                          passphrase_enc=EXCLUDED.passphrase_enc,
                          is_active=EXCLUDED.is_active,
                          last_ok_at=EXCLUDED.last_ok_at,
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
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
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


async def disable_autotrade_key(*, user_id: int, exchange: str, market_type: str) -> None:
    """User-initiated disconnect: mark a stored key as inactive (keeps ciphertext for future re-enable)."""
    pool = get_pool()
    uid = int(user_id)
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    if mt == "futures" and ex not in ("binance", "bybit", "okx"):
        ex = "binance"
    if mt == "spot" and ex not in ("binance", "bybit", "okx", "mexc", "gateio"):
        ex = "binance"
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            INSERT INTO autotrade_keys(user_id, exchange, market_type, is_active, last_error, last_error_at, updated_at)
            VALUES ($1,$2,$3,FALSE,'disabled_by_user',NOW(),NOW())
            ON CONFLICT (user_id, exchange, market_type)
            DO UPDATE SET is_active=FALSE, last_error='disabled_by_user', last_error_at=NOW(), updated_at=NOW();
            """,
            uid,
            ex,
            mt,
        )

def _autotrade_cap_lock_key(*, user_id: int, market_type: str) -> int:
    raw = f"autotrade_cap|{int(user_id)}|{str(market_type or '').lower().strip()}".encode("utf-8")
    return int.from_bytes(hashlib.sha1(raw).digest()[:8], "big", signed=False) & 0x7FFFFFFFFFFFFFFF


async def acquire_autotrade_cap_lock(conn: asyncpg.Connection, *, user_id: int, market_type: str) -> int:
    """Acquire a session-level advisory lock for per-user auto-trade cap checks.

    This serializes concurrent futures opens across replicas so cap/position checks
    see fresh DB state before the next order is sent.
    Returns the lock key for later release.
    """
    key = _autotrade_cap_lock_key(user_id=int(user_id), market_type=str(market_type or "spot"))
    await conn.execute("SELECT pg_advisory_lock($1::bigint);", key)
    return key


async def release_autotrade_cap_lock(conn: asyncpg.Connection, lock_key: int) -> None:
    try:
        await conn.execute("SELECT pg_advisory_unlock($1::bigint);", int(lock_key))
    except Exception:
        pass


async def get_autotrade_used_usdt(user_id: int, market_type: str, *, conn: asyncpg.Connection | None = None) -> float:
    pool = get_pool()
    uid = int(user_id)
    mt = (market_type or "spot").lower().strip()

    async def _fetch(_conn: asyncpg.Connection) -> float:
        v = await _conn.fetchval(
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

    if conn is not None:
        return await _fetch(conn)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn2:
        return await _fetch(conn2)


async def get_autotrade_open_positions_count(user_id: int, market_type: str, *, conn: asyncpg.Connection | None = None) -> int:
    pool = get_pool()
    uid = int(user_id)
    mt = (market_type or "spot").lower().strip()

    async def _fetch(_conn: asyncpg.Connection) -> int:
        v = await _conn.fetchval(
            """
            SELECT COUNT(*)
            FROM autotrade_positions
            WHERE user_id=$1 AND market_type=$2 AND status='OPEN'
            """,
            uid,
            mt,
        )
        try:
            return int(v or 0)
        except Exception:
            return 0

    if conn is not None:
        return await _fetch(conn)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn2:
        return await _fetch(conn2)


async def get_autotrade_stats(
    *,
    user_id: int,
    market_type: str = "all",  # 'spot' | 'futures' | 'all'
    period: str = "today",     # 'today' | 'week' | 'month'
) -> Dict[str, Any]:
    """Return Auto-trade stats for the period.

    Notes:
    - period boundaries are computed in the bot/app timezone and converted to UTC;
      this keeps "Сегодня/Неделя/Месяц" aligned with the Telegram UI.
    - some closes (especially smart-manager virtual exits) may temporarily miss top-level
      pnl_usdt/roi_percent in ``autotrade_positions``. We therefore backfill the stats view
      from linked trades and, when needed, approximate PnL from persisted api_order_ref/meta.
    - ``active`` must reflect the real current OPEN count only.
      Do not derive it from period cohort math like ``opened - closed_total`` because
      that produces false positives when the selected period contains historical opens
      and closes that do not match the current live state.
    """
    pool = get_pool()
    uid = int(user_id)
    mt = (market_type or "all").lower().strip()
    if mt not in ("spot", "futures", "all"):
        mt = "all"
    pr = (period or "today").lower().strip()
    if pr not in ("today", "week", "month"):
        pr = "today"

    since = _autotrade_period_start(pr)
    fee_rate_spot = max(0.0, float(os.getenv("FEE_RATE_SPOT", "0.001") or 0.001))
    fee_rate_futures = max(0.0, float(os.getenv("FEE_RATE_FUTURES", "0.001") or 0.001))

    def _as_float(v: Any, default: float = 0.0) -> float:
        try:
            if v is None or v == "":
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    def _safe_json_obj(v: Any) -> Dict[str, Any]:
        try:
            if isinstance(v, dict):
                return dict(v)
            if isinstance(v, str) and v.strip():
                obj = json.loads(v)
                return dict(obj) if isinstance(obj, dict) else {}
        except Exception:
            return {}
        return {}

    def _boolish(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        return s in ("1", "true", "yes", "y", "on")

    def _approx_from_ref(row: Dict[str, Any]) -> tuple[float | None, float | None]:
        ref = _safe_json_obj(row.get("api_order_ref"))
        meta = _safe_json_obj(row.get("meta"))

        entry = _as_float(ref.get("entry_price") or ref.get("entry"), 0.0)
        qty = _as_float(ref.get("qty"), 0.0)
        if entry <= 0 or qty <= 0:
            return None, None

        reason = str(meta.get("closed_reason") or ref.get("closed_reason") or "").upper().strip()
        tp1 = _as_float(ref.get("tp1"), 0.0)
        tp2 = _as_float(ref.get("tp2"), 0.0)
        sl = _as_float(ref.get("sl"), 0.0)
        be_price = _as_float(meta.get("be_price") or ref.get("be_price"), 0.0)
        tp1_hit = _boolish(meta.get("tp1_hit") if ("tp1_hit" in meta) else ref.get("tp1_hit"))

        exit_price = 0.0
        if reason == "TP2":
            exit_price = tp2
        elif reason in ("FULL_TP1", "FULL_TP1_NO_TP2"):
            exit_price = tp1
        elif reason == "BE":
            exit_price = be_price if be_price > 0 else entry
        elif reason == "SL":
            exit_price = be_price if (tp1_hit and be_price > 0) else sl
        if exit_price <= 0:
            return None, None

        side = str(row.get("side") or ref.get("side") or "BUY").upper().strip()
        if side in ("SELL", "SHORT"):
            pnl_gross = (entry - exit_price) * qty
        else:
            pnl_gross = (exit_price - entry) * qty

        fee_rate = fee_rate_futures if str(row.get("market_type") or "spot").lower() == "futures" else fee_rate_spot
        fees_est = fee_rate * ((entry * qty) + (exit_price * qty))
        realized_pnl = _as_float(ref.get("realized_pnl_usdt"), 0.0)
        realized_fee = _as_float(ref.get("realized_fee_usdt"), 0.0)
        pnl_net = float(pnl_gross) - float(fees_est) + float(realized_pnl) - float(realized_fee)

        alloc = _as_float(row.get("allocated_usdt"), 0.0)
        roi = (pnl_net / alloc * 100.0) if alloc > 0 else None
        return float(pnl_net), (float(roi) if roi is not None else None)

    if mt == "all":
        opened_q = """
            SELECT COUNT(*)
            FROM autotrade_positions
            WHERE user_id=$1 AND opened_at >= $2
        """
        opened_args = (uid, since)
        active_q = """
            SELECT COUNT(*)
            FROM autotrade_positions
            WHERE user_id=$1 AND status='OPEN'
        """
        active_args = (uid,)
        closed_q = """
            SELECT
                ap.id,
                ap.market_type,
                ap.side,
                ap.allocated_usdt,
                ap.pnl_usdt,
                ap.roi_percent,
                ap.api_order_ref,
                ap.meta,
                tr.status AS trade_status,
                tr.pnl_total_pct AS trade_pnl_total_pct
            FROM autotrade_positions ap
            LEFT JOIN LATERAL (
                SELECT t.status, t.pnl_total_pct, t.closed_at, t.id
                FROM trades t
                WHERE t.user_id = ap.user_id
                  AND (
                        (ap.linked_trade_id IS NOT NULL AND t.id = ap.linked_trade_id)
                        OR (t.linked_position_id = ap.id)
                        OR (
                            ap.signal_id IS NOT NULL
                            AND t.signal_id = ap.signal_id
                            AND UPPER(t.market) = CASE WHEN ap.market_type = 'futures' THEN 'FUTURES' ELSE 'SPOT' END
                            AND UPPER(t.symbol) = UPPER(ap.symbol)
                            AND (
                                (UPPER(ap.side) = 'BUY' AND UPPER(t.side) IN ('BUY', 'LONG'))
                                OR (UPPER(ap.side) = 'SELL' AND UPPER(t.side) IN ('SELL', 'SHORT'))
                            )
                            AND t.opened_at BETWEEN (ap.opened_at - INTERVAL '12 hours') AND (COALESCE(ap.closed_at, ap.opened_at) + INTERVAL '12 hours')
                        )
                  )
                ORDER BY
                    CASE
                        WHEN ap.linked_trade_id IS NOT NULL AND t.id = ap.linked_trade_id THEN 0
                        WHEN t.linked_position_id = ap.id THEN 1
                        ELSE 2
                    END,
                    CASE WHEN t.closed_at IS NULL THEN 1 ELSE 0 END,
                    t.closed_at DESC NULLS LAST,
                    t.id DESC
                LIMIT 1
            ) tr ON TRUE
            WHERE ap.user_id=$1
              AND ap.status='CLOSED'
              AND ap.closed_at IS NOT NULL
              AND ap.closed_at >= $2
        """
        closed_args = (uid, since)
    else:
        opened_q = """
            SELECT COUNT(*)
            FROM autotrade_positions
            WHERE user_id=$1 AND market_type=$2 AND opened_at >= $3
        """
        opened_args = (uid, mt, since)
        active_q = """
            SELECT COUNT(*)
            FROM autotrade_positions
            WHERE user_id=$1 AND market_type=$2 AND status='OPEN'
        """
        active_args = (uid, mt)
        closed_q = """
            SELECT
                ap.id,
                ap.market_type,
                ap.side,
                ap.allocated_usdt,
                ap.pnl_usdt,
                ap.roi_percent,
                ap.api_order_ref,
                ap.meta,
                tr.status AS trade_status,
                tr.pnl_total_pct AS trade_pnl_total_pct
            FROM autotrade_positions ap
            LEFT JOIN LATERAL (
                SELECT t.status, t.pnl_total_pct, t.closed_at, t.id
                FROM trades t
                WHERE t.user_id = ap.user_id
                  AND (
                        (ap.linked_trade_id IS NOT NULL AND t.id = ap.linked_trade_id)
                        OR (t.linked_position_id = ap.id)
                        OR (
                            ap.signal_id IS NOT NULL
                            AND t.signal_id = ap.signal_id
                            AND UPPER(t.market) = CASE WHEN ap.market_type = 'futures' THEN 'FUTURES' ELSE 'SPOT' END
                            AND UPPER(t.symbol) = UPPER(ap.symbol)
                            AND (
                                (UPPER(ap.side) = 'BUY' AND UPPER(t.side) IN ('BUY', 'LONG'))
                                OR (UPPER(ap.side) = 'SELL' AND UPPER(t.side) IN ('SELL', 'SHORT'))
                            )
                            AND t.opened_at BETWEEN (ap.opened_at - INTERVAL '12 hours') AND (COALESCE(ap.closed_at, ap.opened_at) + INTERVAL '12 hours')
                        )
                  )
                ORDER BY
                    CASE
                        WHEN ap.linked_trade_id IS NOT NULL AND t.id = ap.linked_trade_id THEN 0
                        WHEN t.linked_position_id = ap.id THEN 1
                        ELSE 2
                    END,
                    CASE WHEN t.closed_at IS NULL THEN 1 ELSE 0 END,
                    t.closed_at DESC NULLS LAST,
                    t.id DESC
                LIMIT 1
            ) tr ON TRUE
            WHERE ap.user_id=$1
              AND ap.market_type=$2
              AND ap.status='CLOSED'
              AND ap.closed_at IS NOT NULL
              AND ap.closed_at >= $3
        """
        closed_args = (uid, mt, since)

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        opened = await conn.fetchval(opened_q, *opened_args)
        active = await conn.fetchval(active_q, *active_args)
        closed_rows = await conn.fetch(closed_q, *closed_args)

    closed_total = 0
    closed_plus = 0
    closed_minus = 0
    pnl_total = 0.0
    invested_closed = 0.0

    for rr in closed_rows or []:
        row = dict(rr)
        closed_total += 1
        alloc = _as_float(row.get("allocated_usdt"), 0.0)
        invested_closed += alloc

        pnl_val = None
        roi_val = None

        if row.get("pnl_usdt") is not None:
            pnl_val = _as_float(row.get("pnl_usdt"), 0.0)
        if row.get("roi_percent") is not None:
            roi_val = _as_float(row.get("roi_percent"), 0.0)
            if pnl_val is None and alloc > 0:
                pnl_val = alloc * roi_val / 100.0

        if pnl_val is None and row.get("trade_pnl_total_pct") is not None:
            roi_val = _as_float(row.get("trade_pnl_total_pct"), 0.0)
            if alloc > 0:
                pnl_val = alloc * roi_val / 100.0

        if pnl_val is None:
            approx_pnl, approx_roi = _approx_from_ref(row)
            if approx_pnl is not None:
                pnl_val = float(approx_pnl)
                roi_val = float(approx_roi) if approx_roi is not None else roi_val

        if pnl_val is not None:
            pnl_total += float(pnl_val)

        trade_status = str(row.get("trade_status") or "").upper().strip()
        meta = _safe_json_obj(row.get("meta"))
        ref = _safe_json_obj(row.get("api_order_ref"))
        close_reason = str(meta.get("closed_reason") or ref.get("closed_reason") or "").upper().strip()
        tp1_hit = _boolish(meta.get("tp1_hit") if ("tp1_hit" in meta) else ref.get("tp1_hit"))

        sign = 0
        if pnl_val is not None:
            if pnl_val > 0:
                sign = 1
            elif pnl_val < 0:
                sign = -1
        elif roi_val is not None:
            if roi_val > 0:
                sign = 1
            elif roi_val < 0:
                sign = -1
        elif trade_status == "WIN":
            sign = 1
        elif trade_status == "LOSS":
            sign = -1
        elif close_reason in ("TP2", "FULL_TP1", "FULL_TP1_NO_TP2"):
            sign = 1
        elif close_reason == "SL" and not tp1_hit:
            sign = -1

        if sign > 0:
            closed_plus += 1
        elif sign < 0:
            closed_minus += 1

    opened_i = max(int(opened or 0), 0)
    active_i = max(int(active or 0), 0)

    # IMPORTANT:
    # 'Активных сейчас' is a live-state metric, not a period-derived estimate.
    # The old fallback `opened - closed_total` looked reasonable on paper, but it
    # overcounted when the selected period contained historical opens/closes that
    # no longer matched the current OPEN rows. That is exactly how users could see
    # 'Активных сейчас: 3' while in reality there were 0 active positions.
    #
    # If a deployment ever needs the legacy behavior for debugging, it can be
    # explicitly re-enabled via env, but the safe default is OFF.
    if active_i <= 0:
        try:
            legacy_fallback = str(os.getenv("AUTOTRADE_STATS_ACTIVE_RESIDUAL_FALLBACK", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
        except Exception:
            legacy_fallback = False
        if legacy_fallback and opened_i > closed_total:
            active_i = max(opened_i - closed_total, 0)

    roi = (pnl_total / invested_closed * 100.0) if invested_closed > 0 else 0.0

    return {
        "opened": opened_i,
        "closed_plus": closed_plus,
        "closed_minus": closed_minus,
        "active": active_i,
        "pnl_total": float(pnl_total),
        "roi_percent": float(roi),
        "invested_closed": float(invested_closed),
        "closed_total": int(closed_total),
        "period_since": since,
    }


async def count_closed_autotrade_positions(*, user_id: int, market_type: str = "futures", last_n: int = 5) -> int:
    """Return count of CLOSED autotrade positions for user (up to last_n most recent).
    Used for UI gating: after 5 closed deals we allow higher minimum effective cap.
    """
    pool = get_pool()
    uid = int(user_id)
    mt = (market_type or "futures").lower().strip()
    n = int(last_n or 0)
    if n <= 0:
        n = 5
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT 1
                FROM autotrade_positions
                WHERE user_id=$1 AND market_type=$2 AND status='CLOSED'
                ORDER BY closed_at DESC NULLS LAST, id DESC
                LIMIT $3
                """,
                uid, mt, n
            )
            return int(len(rows))
        except Exception:
            return 0


# ----------------------
# Generic KV store (JSONB)
# ----------------------

async def kv_get_json(key: str) -> Optional[dict]:
    """Return JSON object for a key from Postgres KV store.

    Works with both JSONB and TEXT (JSON string) schemas.
    Returns None if key not found or value is not a JSON object.
    """
    import json

    pool = get_pool()
    k = str(key or "").strip()
    if not k:
        return None
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow("SELECT value FROM kv_store WHERE key=$1", k)
        if not row:
            return None
        v = row.get("value")
        if isinstance(v, dict):
            return v
        if isinstance(v, str) and v:
            try:
                obj = json.loads(v)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None
        return None


async def kv_set_json(key: str, value: dict) -> None:
    """Upsert JSON value for a key into Postgres KV store.

    NOTE: We always serialize to a JSON string to be compatible with older schemas
    where kv_store.value might have been created as TEXT.
    """
    import json

    pool = get_pool()
    k = str(key or "").strip()
    if not k:
        return
    try:
        payload = json.dumps(value if isinstance(value, dict) else {"value": value}, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        payload = "{}"

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        # We support multiple historical schemas:
        # 1) kv_store("key" TEXT PRIMARY KEY, "value" JSONB, updated_at TIMESTAMPTZ)
        # 2) kv_store("key" TEXT PRIMARY KEY, "value" TEXT,  updated_at TIMESTAMPTZ)
        # 3) same as above but WITHOUT updated_at column
        #
        # Using quoted identifiers makes this work even if the schema used reserved names.
        queries = [
            (
                """
                INSERT INTO kv_store("key", "value", updated_at)
                VALUES ($1, $2::jsonb, NOW())
                ON CONFLICT ("key")
                DO UPDATE SET "value"=EXCLUDED."value", updated_at=NOW()
                """,
                (k, payload),
            ),
            (
                """
                INSERT INTO kv_store("key", "value")
                VALUES ($1, $2::jsonb)
                ON CONFLICT ("key")
                DO UPDATE SET "value"=EXCLUDED."value"
                """,
                (k, payload),
            ),
            (
                """
                INSERT INTO kv_store("key", "value", updated_at)
                VALUES ($1, $2::text, NOW())
                ON CONFLICT ("key")
                DO UPDATE SET "value"=EXCLUDED."value", updated_at=NOW()
                """,
                (k, payload),
            ),
            (
                """
                INSERT INTO kv_store("key", "value")
                VALUES ($1, $2::text)
                ON CONFLICT ("key")
                DO UPDATE SET "value"=EXCLUDED."value"
                """,
                (k, payload),
            ),
        ]

        last_err: Exception | None = None
        for q, args in queries:
            try:
                await conn.execute(q, *args)
                return
            except Exception as e:
                last_err = e
                continue
        if last_err:
            raise last_err


import zlib
import pickle

import pickle

def _cc_key(ex_name: str, market: str, symbol: str, tf: str, limit: int) -> str:
    return f"candles:{ex_name}:{market}:{symbol}:{tf}:{int(limit)}"

def _cc_pack_df(df) -> bytes:
    # Store binary (pickle+zlib) to avoid JSON and reduce size
    payload = {
        "columns": list(df.columns),
        "values": df.values.tolist(),
        "index": df.index.tolist() if hasattr(df.index, "tolist") else list(df.index),
    }
    raw = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    return zlib.compress(raw, level=6)

def _cc_unpack_df(blob: bytes):
    raw = zlib.decompress(blob)
    payload = pickle.loads(raw)
    import pandas as pd
    df = pd.DataFrame(payload["values"], columns=payload["columns"])
    try:
        df.index = payload.get("index", df.index)
    except Exception:
        pass
    return df

async def candles_cache_get(ex_name: str, market: str, symbol: str, tf: str, limit: int):
    pool = get_pool()
    if not pool:
        return None, None
    key = _cc_key(ex_name, market, symbol, tf, limit)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow("SELECT payload, updated_at FROM candles_cache WHERE key=$1", key)
        return None if not row else (row["payload"], row["updated_at"])

async def candles_cache_set(ex_name: str, market: str, symbol: str, tf: str, limit: int, payload: bytes) -> None:
    pool = get_pool()
    if not pool:
        return
    key = _cc_key(ex_name, market, symbol, tf, limit)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            "INSERT INTO candles_cache(key,payload,updated_at) VALUES ($1,$2,NOW()) "
            "ON CONFLICT (key) DO UPDATE SET payload=EXCLUDED.payload, updated_at=NOW()",
            key, payload
        )

async def candles_cache_purge(max_age_sec: int) -> int:
    """Delete candles cache rows older than max_age_sec. Returns number of deleted rows."""
    pool = get_pool()
    if not pool:
        return 0
    max_age_sec = int(max_age_sec or 0)
    if max_age_sec <= 0:
        return 0
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        res = await conn.execute(
            "DELETE FROM candles_cache WHERE updated_at < (NOW() - ($1::int * INTERVAL '1 second'))",
            max_age_sec
        )
    # asyncpg returns like 'DELETE 12'
    try:
        return int(str(res).split()[-1])
    except Exception:
        return 0


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return int(default)


async def _delete_in_chunks(conn: asyncpg.Connection, *, sql: str, older_than_days: int, batch_size: int) -> int:
    """Run a chunked DELETE CTE until no more rows match."""
    total = 0
    days = int(older_than_days or 0)
    batch = int(batch_size or 0)
    if days <= 0 or batch <= 0:
        return 0
    while True:
        n = await conn.fetchval(sql, days, batch)
        n = int(n or 0)
        total += n
        if n < batch:
            break
    return total


async def cleanup_old_postgres_data(*, batch_size: int | None = None) -> Dict[str, int]:
    """Delete old closed rows from high-churn Postgres tables.

    Safe defaults keep recent history while pruning rows that only bloat storage/indexes.
    Financial/audit tables are intentionally NOT touched.

    Env knobs:
      DB_RETENTION_DELETE_BATCH                (default 1000)
      DB_RETENTION_CLOSED_TRADES_DAYS          (default 60)
      DB_RETENTION_CLOSED_AUTOTRADE_DAYS       (default 60)
      DB_RETENTION_SIGNAL_TRACKS_DAYS          (default 60)
      DB_RETENTION_SIGNAL_SENT_DAYS            (default 60)
      DB_RETENTION_ANALYZE_ENABLED             (default 1)
    """
    pool = get_pool()
    batch = int(batch_size or _env_int("DB_RETENTION_DELETE_BATCH", 1000) or 1000)
    batch = max(50, min(batch, 10000))

    closed_trades_days = max(0, _env_int("DB_RETENTION_CLOSED_TRADES_DAYS", 60))
    closed_autotrade_days = max(0, _env_int("DB_RETENTION_CLOSED_AUTOTRADE_DAYS", 60))
    signal_tracks_days = max(0, _env_int("DB_RETENTION_SIGNAL_TRACKS_DAYS", 60))
    signal_sent_days = max(0, _env_int("DB_RETENTION_SIGNAL_SENT_DAYS", 60))
    analyze_enabled = str(os.getenv("DB_RETENTION_ANALYZE_ENABLED", "1") or "1").strip().lower() not in ("0", "false", "no", "off")

    totals: Dict[str, int] = {
        "closed_trades": 0,
        "closed_autotrade_positions": 0,
        "closed_signal_tracks": 0,
        "signal_sent_events": 0,
    }
    changed_tables: set[str] = set()

    sql_closed_trades = """
        WITH victim AS (
            SELECT id
            FROM trades
            WHERE closed_at IS NOT NULL
              AND status IN ('BE','WIN','LOSS','CLOSED')
              AND closed_at < (NOW() - ($1::int * INTERVAL '1 day'))
            ORDER BY closed_at ASC, id ASC
            LIMIT $2
        ), del AS (
            DELETE FROM trades t
            USING victim v
            WHERE t.id = v.id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM del;
    """
    sql_closed_autotrade = """
        WITH victim AS (
            SELECT id
            FROM autotrade_positions
            WHERE status IN ('CLOSED','ERROR')
              AND closed_at IS NOT NULL
              AND closed_at < (NOW() - ($1::int * INTERVAL '1 day'))
            ORDER BY closed_at ASC, id ASC
            LIMIT $2
        ), del AS (
            DELETE FROM autotrade_positions p
            USING victim v
            WHERE p.id = v.id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM del;
    """
    sql_closed_signal_tracks = """
        WITH victim AS (
            SELECT signal_id
            FROM signal_tracks
            WHERE status NOT IN ('ACTIVE','TP1')
              AND closed_at IS NOT NULL
              AND closed_at < (NOW() - ($1::int * INTERVAL '1 day'))
            ORDER BY closed_at ASC, signal_id ASC
            LIMIT $2
        ), del AS (
            DELETE FROM signal_tracks s
            USING victim v
            WHERE s.signal_id = v.signal_id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM del;
    """
    sql_signal_sent = """
        WITH victim AS (
            SELECT id
            FROM signal_sent_events
            WHERE created_at < (NOW() - ($1::int * INTERVAL '1 day'))
            ORDER BY created_at ASC, id ASC
            LIMIT $2
        ), del AS (
            DELETE FROM signal_sent_events s
            USING victim v
            WHERE s.id = v.id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM del;
    """

    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        if closed_trades_days > 0:
            totals["closed_trades"] = await _delete_in_chunks(
                conn,
                sql=sql_closed_trades,
                older_than_days=closed_trades_days,
                batch_size=batch,
            )
            if totals["closed_trades"] > 0:
                changed_tables.update({"trades", "trade_events"})

        if closed_autotrade_days > 0:
            totals["closed_autotrade_positions"] = await _delete_in_chunks(
                conn,
                sql=sql_closed_autotrade,
                older_than_days=closed_autotrade_days,
                batch_size=batch,
            )
            if totals["closed_autotrade_positions"] > 0:
                changed_tables.add("autotrade_positions")

        if signal_tracks_days > 0:
            totals["closed_signal_tracks"] = await _delete_in_chunks(
                conn,
                sql=sql_closed_signal_tracks,
                older_than_days=signal_tracks_days,
                batch_size=batch,
            )
            if totals["closed_signal_tracks"] > 0:
                changed_tables.add("signal_tracks")

        if signal_sent_days > 0:
            totals["signal_sent_events"] = await _delete_in_chunks(
                conn,
                sql=sql_signal_sent,
                older_than_days=signal_sent_days,
                batch_size=batch,
            )
            if totals["signal_sent_events"] > 0:
                changed_tables.add("signal_sent_events")

        if analyze_enabled and changed_tables:
            for table_name in sorted(changed_tables):
                try:
                    await conn.execute(f"ANALYZE {table_name};")
                except Exception:
                    logger.debug("cleanup_old_postgres_data: ANALYZE failed for %s", table_name, exc_info=True)

    return totals


# ========================
# Subscription payments
# ========================
async def create_subscription_order(*, order_id: str, telegram_id: int, plan: str, amount: float, currency: str = 'USD', provider: str = 'nowpayments') -> None:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            INSERT INTO subscription_orders(order_id, telegram_id, plan, amount, currency, provider, status)
            VALUES ($1,$2,$3,$4,$5,$6,'pending')
            ON CONFLICT (order_id) DO UPDATE
              SET telegram_id=EXCLUDED.telegram_id,
                  plan=EXCLUDED.plan,
                  amount=EXCLUDED.amount,
                  currency=EXCLUDED.currency,
                  provider=EXCLUDED.provider,
                  updated_at=NOW();
            """,
            str(order_id), int(telegram_id), str(plan), float(amount), str(currency), str(provider),
        )

async def attach_subscription_invoice(*, order_id: str, provider_payment_id: str | None = None, pay_currency: str | None = None, pay_amount: float | None = None, pay_address: str | None = None, pay_url: str | None = None, payload: dict | None = None) -> None:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            UPDATE subscription_orders
               SET provider_payment_id=COALESCE($2, provider_payment_id),
                   pay_currency=COALESCE($3, pay_currency),
                   pay_amount=COALESCE($4, pay_amount),
                   pay_address=COALESCE($5, pay_address),
                   pay_url=COALESCE($6, pay_url),
                   payload=COALESCE($7::jsonb, payload),
                   updated_at=NOW()
             WHERE order_id=$1
            """,
            str(order_id),
            (str(provider_payment_id) if provider_payment_id else None),
            (str(pay_currency) if pay_currency else None),
            (float(pay_amount) if pay_amount is not None else None),
            (str(pay_address) if pay_address else None),
            (str(pay_url) if pay_url else None),
            (json.dumps(payload or {}, ensure_ascii=False) if payload is not None else None),
        )

async def get_subscription_order(order_id: str) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow("SELECT * FROM subscription_orders WHERE order_id=$1", str(order_id))
        return dict(row) if row else None

async def get_subscription_order_by_provider_payment_id(provider_payment_id: str) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow("SELECT * FROM subscription_orders WHERE provider_payment_id=$1", str(provider_payment_id))
        return dict(row) if row else None

async def mark_subscription_order_paid(*, order_id: str, provider_payment_id: str | None = None, status: str = 'finished', txid: str | None = None, pay_currency: str | None = None, pay_amount: float | None = None, payload: dict | None = None) -> bool:
    """Returns True if this order was processed now, False if already processed or missing."""
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        async with conn.transaction():
            row = await conn.fetchrow("SELECT * FROM subscription_orders WHERE order_id=$1 FOR UPDATE", str(order_id))
            if not row:
                return False
            if row.get('processed_at') is not None:
                return False
            await conn.execute(
                """
                UPDATE subscription_orders
                   SET provider_payment_id=COALESCE($2, provider_payment_id),
                       status=$3,
                       txid=COALESCE($4, txid),
                       pay_currency=COALESCE($5, pay_currency),
                       pay_amount=COALESCE($6, pay_amount),
                       payload=COALESCE($7::jsonb, payload),
                       paid_at=NOW(),
                       processed_at=NOW(),
                       updated_at=NOW()
                 WHERE order_id=$1
                """,
                str(order_id),
                (str(provider_payment_id) if provider_payment_id else None),
                str(status),
                (str(txid) if txid else None),
                (str(pay_currency) if pay_currency else None),
                (float(pay_amount) if pay_amount is not None else None),
                (json.dumps(payload or {}, ensure_ascii=False) if payload is not None else None),
            )
            rr = await conn.fetchrow("SELECT telegram_id, plan, amount, currency FROM subscription_orders WHERE order_id=$1", str(order_id))
            if rr:
                await conn.execute(
                    """
                    INSERT INTO subscription_payments(order_id, telegram_id, plan, amount, currency, pay_currency, pay_amount, provider, provider_payment_id, txid, status, payload, paid_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,'nowpayments',$8,$9,$10,$11::jsonb,NOW())
                    """,
                    str(order_id), int(rr['telegram_id']), str(rr['plan']), float(rr['amount']), str(rr['currency']),
                    (str(pay_currency) if pay_currency else None), (float(pay_amount) if pay_amount is not None else None),
                    (str(provider_payment_id) if provider_payment_id else None), (str(txid) if txid else None), str(status),
                    json.dumps(payload or {}, ensure_ascii=False),
                )
            return True

async def grant_subscription_plan(telegram_id: int, plan: str, days: int = 30) -> Dict[str, Any]:
    """Grant SIGNAL PRO / AUTO PRO access. Returns resulting flags."""
    plan_norm = (plan or '').strip().lower()
    signal_days = int(days or 30)
    autotrade_days = int(days or 30)
    enable_autotrade = plan_norm == 'auto_pro'
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO users(telegram_id, notify_signals, signal_enabled, autotrade_enabled, created_at, updated_at)
                VALUES ($1, TRUE, FALSE, FALSE, NOW(), NOW())
                ON CONFLICT (telegram_id) DO NOTHING
                """,
                int(telegram_id),
            )
            await conn.execute(
                """
                UPDATE users
                   SET signal_enabled=TRUE,
                       signal_expires_at=GREATEST(COALESCE(signal_expires_at, NOW()), NOW()) + make_interval(days => $2),
                       autotrade_enabled=CASE WHEN $3 THEN TRUE ELSE FALSE END,
                       autotrade_stop_after_close=FALSE,
                       autotrade_expires_at=CASE WHEN $3 THEN GREATEST(COALESCE(autotrade_expires_at, NOW()), NOW()) + make_interval(days => $4) ELSE NULL END,
                       is_blocked=FALSE,
                       updated_at=NOW()
                 WHERE telegram_id=$1
                """,
                int(telegram_id), signal_days, bool(enable_autotrade), autotrade_days,
            )
            row = await conn.fetchrow("SELECT telegram_id, signal_enabled, signal_expires_at, autotrade_enabled, autotrade_expires_at FROM users WHERE telegram_id=$1", int(telegram_id))
            return dict(row) if row else {"telegram_id": int(telegram_id)}

async def grant_access(telegram_id: int, plan: str, days: int = 30) -> Dict[str, Any]:
    """Compatibility alias for the subscription spec."""
    return await grant_subscription_plan(telegram_id=telegram_id, plan=plan, days=days)

async def list_subscription_payments(*, date_from: Optional[str] = None, date_to: Optional[str] = None) -> List[Dict[str, Any]]:
    pool = get_pool()
    where=[]; args=[]
    if date_from:
        args.append(date_from)
        where.append(f"created_at >= ${len(args)}::timestamptz")
    if date_to:
        args.append(date_to)
        where.append(f"created_at < ${len(args)}::timestamptz")
    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''
    q = f"SELECT * FROM subscription_payments {where_sql} ORDER BY created_at DESC"
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        rows = await conn.fetch(q, *args)
    return [dict(r) for r in rows]


# ---------------- Referral system ----------------

async def get_referral_overview(user_id: int) -> Dict[str, Any]:
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        user = await conn.fetchrow(
            """
            SELECT telegram_id, referrer_id,
                   COALESCE(referral_enabled, FALSE) AS referral_enabled,
                   COALESCE(ref_available_balance, 0) AS ref_available_balance,
                   COALESCE(ref_hold_balance, 0) AS ref_hold_balance,
                   COALESCE(ref_withdrawn_balance, 0) AS ref_withdrawn_balance,
                   COALESCE(ref_total_earned, 0) AS ref_total_earned
            FROM users
            WHERE telegram_id=$1
            """,
            uid,
        )
        global_referral_enabled = await conn.fetchval(
            "SELECT COALESCE(referral_enabled, FALSE) FROM signal_bot_settings WHERE id=1"
        )
        invited = await conn.fetchval("SELECT COUNT(1) FROM users WHERE referrer_id=$1", uid)
        paid = await conn.fetchval("SELECT COUNT(1) FROM referral_rewards WHERE referrer_id=$1", uid)
        manual_referral_enabled = bool((user.get("referral_enabled") if user else False) or False)
        return {
            "user_id": uid,
            "referrer_id": int(user.get("referrer_id")) if user and user.get("referrer_id") is not None else None,
            "referral_enabled": manual_referral_enabled,
            "global_referral_enabled": bool(global_referral_enabled or False),
            "effective_referral_enabled": bool(bool(global_referral_enabled or False) or manual_referral_enabled),
            "total_refs": int(invited or 0),
            "paid_refs": int(paid or 0),
            "available_balance": float((user.get("ref_available_balance") if user else 0) or 0),
            "hold_balance": float((user.get("ref_hold_balance") if user else 0) or 0),
            "withdrawn_balance": float((user.get("ref_withdrawn_balance") if user else 0) or 0),
            "total_earned": float((user.get("ref_total_earned") if user else 0) or 0),
        }


async def get_referral_user_profile(user_id: int) -> Dict[str, Any]:
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            SELECT telegram_id, created_at, signal_expires_at, referrer_id,
                   COALESCE(referral_enabled, FALSE) AS referral_enabled
            FROM users WHERE telegram_id=$1
            """, uid
        )
        ov = await get_referral_overview(uid)
        out = dict(ov)
        out.update({
            "created_at": row.get("created_at") if row else None,
            "signal_expires_at": row.get("signal_expires_at") if row else None,
            "referral_enabled": bool(row.get("referral_enabled") if row else False),
        })
        return out


async def list_referral_enabled_user_ids() -> List[int]:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        rows = await conn.fetch(
            """
            SELECT telegram_id
            FROM users
            WHERE COALESCE(referral_enabled, FALSE) = TRUE
            ORDER BY telegram_id
            """
        )
    return [int(r.get("telegram_id")) for r in rows if r and r.get("telegram_id") is not None]


async def get_user_referral_access_state(user_id: int) -> Dict[str, Any]:
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COALESCE((SELECT referral_enabled FROM signal_bot_settings WHERE id = 1), FALSE) AS global_referral_enabled,
                COALESCE((SELECT referral_enabled FROM users WHERE telegram_id = $1), FALSE) AS referral_enabled
            """,
            uid,
        )
    global_enabled = bool((row.get("global_referral_enabled") if row else False) or False)
    user_enabled = bool((row.get("referral_enabled") if row else False) or False)
    return {
        "user_id": uid,
        "global_referral_enabled": global_enabled,
        "referral_enabled": user_enabled,
        "effective_referral_enabled": bool(global_enabled or user_enabled),
    }


async def set_user_referral_enabled(user_id: int, enabled: bool) -> Dict[str, Any]:
    pool = get_pool()
    uid = int(user_id)
    flag = bool(enabled)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            """
            INSERT INTO users (telegram_id, referral_enabled)
            VALUES ($1, $2)
            ON CONFLICT (telegram_id) DO UPDATE
            SET referral_enabled = EXCLUDED.referral_enabled,
                updated_at = NOW()
            """,
            uid,
            flag,
        )
    return await get_user_referral_access_state(uid)


async def award_referral_bonus_for_first_payment(*, referral_user_id: int, order_id: str | None, payment_amount: float, currency: str = 'USDT', reward_percent: float = 10.0) -> Optional[Dict[str, Any]]:
    """Award a referral bonus only for the invited user's first successful payment.

    We guard this in two ways:
    1) only one reward row can exist per invited user;
    2) the invited user must have exactly one successful subscription payment recorded.

    The second check keeps the behavior aligned with the product spec even if this
    function is called from a later payment flow or retried after historical data changes.
    """
    pool = get_pool()
    uid = int(referral_user_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        async with conn.transaction():
            user = await conn.fetchrow(
                """
                SELECT u.telegram_id,
                       u.referrer_id,
                       COALESCE((SELECT referral_enabled FROM users WHERE telegram_id = u.referrer_id), FALSE) AS referrer_referral_enabled,
                       COALESCE((SELECT referral_enabled FROM signal_bot_settings WHERE id = 1), FALSE) AS global_referral_enabled
                FROM users u
                WHERE u.telegram_id=$1
                FOR UPDATE OF u
                """,
                uid,
            )
            if not user:
                return None
            referrer_id = user.get('referrer_id')
            if referrer_id is None or int(referrer_id) == uid:
                return None
            if not (bool(user.get('global_referral_enabled')) or bool(user.get('referrer_referral_enabled'))):
                return None

            existing = await conn.fetchrow("SELECT id, reward_amount FROM referral_rewards WHERE referral_user_id=$1", uid)
            if existing:
                return None

            paid_count = await conn.fetchval(
                """
                SELECT COUNT(1)
                FROM subscription_payments
                WHERE telegram_id=$1
                  AND COALESCE(status, 'finished') = 'finished'
                """,
                uid,
            )
            if int(paid_count or 0) != 1:
                return None

            reward_amount = round(float(payment_amount or 0) * (float(reward_percent or 10.0) / 100.0), 8)
            if reward_amount <= 0:
                return None
            rec = await conn.fetchrow(
                """
                INSERT INTO referral_rewards(referrer_id, referral_user_id, order_id, payment_amount, reward_percent, reward_amount, currency, status, created_at, available_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,'available',NOW(),NOW())
                RETURNING id, referrer_id, referral_user_id, reward_amount, payment_amount, currency, created_at
                """,
                int(referrer_id), uid, str(order_id) if order_id else None, float(payment_amount or 0), float(reward_percent or 10.0), reward_amount, str(currency or 'USDT').upper(),
            )
            await conn.execute(
                """
                UPDATE users
                SET ref_available_balance = COALESCE(ref_available_balance,0) + $2,
                    ref_total_earned = COALESCE(ref_total_earned,0) + $2,
                    updated_at = NOW()
                WHERE telegram_id=$1
                """,
                int(referrer_id), reward_amount,
            )
            return dict(rec) if rec else None


async def get_open_referral_withdrawal_request(user_id: int) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow(
            """
            SELECT * FROM referral_withdrawal_requests
            WHERE telegram_id=$1 AND status='pending'
            ORDER BY created_at DESC
            LIMIT 1
            """, uid
        )
        return dict(row) if row else None


async def create_referral_withdrawal_request(*, user_id: int, wallet_address: str, amount: float, currency: str = 'USDT', network: str = 'BSC (BEP20)') -> Dict[str, Any]:
    pool = get_pool()
    uid = int(user_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT id FROM referral_withdrawal_requests WHERE telegram_id=$1 AND status='pending' ORDER BY created_at DESC LIMIT 1 FOR UPDATE", uid
            )
            if existing:
                raise ValueError('pending_request_exists')
            row = await conn.fetchrow(
                "SELECT COALESCE(ref_available_balance,0) AS available, COALESCE(ref_hold_balance,0) AS hold FROM users WHERE telegram_id=$1 FOR UPDATE", uid
            )
            if not row:
                raise ValueError('user_not_found')
            available = float(row.get('available') or 0)
            amt = round(float(amount or 0), 8)
            if amt <= 0 or available + 1e-9 < amt:
                raise ValueError('insufficient_balance')
            await conn.execute(
                """
                UPDATE users
                SET ref_available_balance = COALESCE(ref_available_balance,0) - $2,
                    ref_hold_balance = COALESCE(ref_hold_balance,0) + $2,
                    updated_at = NOW()
                WHERE telegram_id=$1
                """, uid, amt
            )
            network_value = str(network or 'BSC (BEP20)').strip() or 'BSC (BEP20)'
            req = await conn.fetchrow(
                """
                INSERT INTO referral_withdrawal_requests(telegram_id, amount, currency, network, wallet_address, status, created_at)
                VALUES ($1,$2,$3,$4,$5,'pending',NOW())
                RETURNING *
                """, uid, amt, str(currency or 'USDT').upper(), network_value, str(wallet_address).strip()
            )
            return dict(req)


async def set_referral_withdrawal_admin_message(*, request_id: int, admin_chat_id: int, admin_message_id: int) -> None:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        await conn.execute(
            "UPDATE referral_withdrawal_requests SET admin_chat_id=$2, admin_message_id=$3 WHERE id=$1",
            int(request_id), int(admin_chat_id), int(admin_message_id),
        )


async def get_referral_withdrawal_request(request_id: int) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        row = await conn.fetchrow("SELECT * FROM referral_withdrawal_requests WHERE id=$1", int(request_id))
        return dict(row) if row else None


async def mark_referral_withdrawal_paid(*, request_id: int, processed_by: int | None = None, admin_comment: str | None = None) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    rid = int(request_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        async with conn.transaction():
            req = await conn.fetchrow("SELECT * FROM referral_withdrawal_requests WHERE id=$1 FOR UPDATE", rid)
            if not req or str(req.get('status')) != 'pending':
                return dict(req) if req else None
            amt = float(req.get('amount') or 0)
            uid = int(req.get('telegram_id'))
            await conn.execute(
                """
                UPDATE users
                SET ref_hold_balance = GREATEST(COALESCE(ref_hold_balance,0) - $2, 0),
                    ref_withdrawn_balance = COALESCE(ref_withdrawn_balance,0) + $2,
                    updated_at = NOW()
                WHERE telegram_id=$1
                """, uid, amt
            )
            row = await conn.fetchrow(
                """
                UPDATE referral_withdrawal_requests
                SET status='paid', processed_at=NOW(), processed_by=$2, admin_comment=$3
                WHERE id=$1
                RETURNING *
                """, rid, int(processed_by) if processed_by else None, admin_comment
            )
            return dict(row) if row else None


async def reject_referral_withdrawal_request(*, request_id: int, processed_by: int | None = None, admin_comment: str | None = None) -> Optional[Dict[str, Any]]:
    pool = get_pool()
    rid = int(request_id)
    async with pool.acquire(timeout=_db_acquire_timeout()) as conn:
        async with conn.transaction():
            req = await conn.fetchrow("SELECT * FROM referral_withdrawal_requests WHERE id=$1 FOR UPDATE", rid)
            if not req or str(req.get('status')) != 'pending':
                return dict(req) if req else None
            amt = float(req.get('amount') or 0)
            uid = int(req.get('telegram_id'))
            await conn.execute(
                """
                UPDATE users
                SET ref_hold_balance = GREATEST(COALESCE(ref_hold_balance,0) - $2, 0),
                    ref_available_balance = COALESCE(ref_available_balance,0) + $2,
                    updated_at = NOW()
                WHERE telegram_id=$1
                """, uid, amt
            )
            row = await conn.fetchrow(
                """
                UPDATE referral_withdrawal_requests
                SET status='rejected', processed_at=NOW(), processed_by=$2, admin_comment=$3
                WHERE id=$1
                RETURNING *
                """, rid, int(processed_by) if processed_by else None, admin_comment
            )
            return dict(row) if row else None
