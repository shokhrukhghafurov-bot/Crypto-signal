import csv
import hashlib
import io
import ipaddress
import json
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from config import settings


def _normalize_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip().replace(',', '.')
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _normalize_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    if isinstance(value, str):
        raw = value.strip().replace(',', '.')
        if not raw:
            return None
        try:
            return int(round(float(raw)))
        except ValueError:
            return None
    return None


def _normalize_optional_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        normalized = raw.replace('Z', '+00:00')
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


VIRTUAL_LOCATION_CODES = {"auto-fastest", "auto-reserve"}
VIRTUAL_LOCATION_POOL_SIZE = max(1, int(getattr(settings, 'VIRTUAL_LOCATION_POOL_SIZE', 3) or 3))
VIRTUAL_LOCATION_FRESH_CHECK_MINUTES = 15
VIRTUAL_LOCATION_REUSE_PING_DRIFT_MS = 120
VIRTUAL_LOCATION_LOAD_IMBALANCE_THRESHOLD = max(0, int(getattr(settings, 'VIRTUAL_LOCATION_LOAD_IMBALANCE_THRESHOLD', 0) or 0))
VIRTUAL_LOCATION_MAX_PING_BALANCE_DRIFT_MS = max(60, int(getattr(settings, 'VIRTUAL_LOCATION_MAX_PING_BALANCE_DRIFT_MS', 180) or 180))
VIRTUAL_LOCATION_MAX_USERS_PER_SERVER = max(0, int(getattr(settings, 'VIRTUAL_LOCATION_MAX_USERS_PER_SERVER', 0) or 0))

SCHEMA_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL UNIQUE,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    language TEXT NOT NULL DEFAULT 'ru',
    status TEXT NOT NULL DEFAULT 'active',
    device_limit_override INTEGER,
    subscription_token TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS plans (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name_ru TEXT NOT NULL,
    name_en TEXT NOT NULL,
    price_rub NUMERIC(10,2) NOT NULL,
    duration_days INTEGER NOT NULL,
    device_limit INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    source_env_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    plan_id INTEGER NOT NULL REFERENCES plans(id),
    starts_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS devices (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,
    device_name TEXT,
    device_fingerprint TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, device_fingerprint)
);

CREATE TABLE IF NOT EXISTS device_subscription_tokens (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id INTEGER REFERENCES devices(id) ON DELETE SET NULL,
    token TEXT NOT NULL UNIQUE,
    device_fingerprint TEXT,
    platform TEXT,
    device_name TEXT,
    client TEXT,
    client_id TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name_ru TEXT NOT NULL,
    name_en TEXT NOT NULL,
    country_code TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_recommended BOOLEAN NOT NULL DEFAULT FALSE,
    is_reserve BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'online',
    sort_order INTEGER NOT NULL DEFAULT 100,
    download_mbps DOUBLE PRECISION,
    upload_mbps DOUBLE PRECISION,
    ping_ms INTEGER,
    speed_checked_at TIMESTAMPTZ,
    vpn_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    location_source TEXT NOT NULL DEFAULT 'catalog',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payments (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    plan_id INTEGER NOT NULL REFERENCES plans(id),
    provider TEXT NOT NULL,
    method TEXT,
    amount NUMERIC(10,2) NOT NULL,
    currency TEXT NOT NULL DEFAULT 'RUB',
    status TEXT NOT NULL DEFAULT 'created',
    external_payment_id TEXT UNIQUE,
    checkout_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    paid_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS auth_codes (
    code TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admin_notes (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    admin_name TEXT NOT NULL,
    note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS manual_extensions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    days_added INTEGER NOT NULL,
    reason TEXT,
    admin_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_notifications (
    id BIGSERIAL PRIMARY KEY,
    unique_key TEXT NOT NULL UNIQUE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    last_error TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bot_error_log (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    context TEXT,
    error_message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS vpn_runtime_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_location_credentials (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    location_code TEXT NOT NULL,
    uuid TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, location_code)
);

CREATE TABLE IF NOT EXISTS user_device_location_credentials (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    location_code TEXT NOT NULL,
    uuid TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(device_id, location_code)
);

CREATE TABLE IF NOT EXISTS virtual_location_assignments (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    virtual_code TEXT NOT NULL,
    concrete_code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, virtual_code)
);

CREATE TABLE IF NOT EXISTS vpn_location_sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_fingerprint TEXT NOT NULL,
    location_code TEXT NOT NULL,
    client TEXT,
    platform TEXT,
    device_name TEXT,
    subscription_token TEXT,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, device_fingerprint, location_code)
);
"""

MIGRATION_SQL = [
    # users
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS language TEXT DEFAULT 'ru'",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS device_limit_override INTEGER",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS subscription_token TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",

    # plans
    "ALTER TABLE plans ADD COLUMN IF NOT EXISTS device_limit INTEGER DEFAULT 2",
    "ALTER TABLE plans ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE",
    "ALTER TABLE plans ADD COLUMN IF NOT EXISTS source_env_key TEXT",
    "ALTER TABLE plans ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE plans ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",

    # subscriptions
    "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'",
    "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",

    # devices
    "ALTER TABLE devices ADD COLUMN IF NOT EXISTS device_name TEXT",
    "ALTER TABLE devices ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE",
    "ALTER TABLE devices ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE devices ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",

    # per-device subscription tokens: one import link = one device slot
    "CREATE TABLE IF NOT EXISTS device_subscription_tokens (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, device_id INTEGER REFERENCES devices(id) ON DELETE SET NULL, token TEXT NOT NULL UNIQUE, device_fingerprint TEXT, platform TEXT, device_name TEXT, client TEXT, client_id TEXT, is_active BOOLEAN NOT NULL DEFAULT TRUE, first_seen_at TIMESTAMPTZ, last_seen_at TIMESTAMPTZ, expires_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS device_id INTEGER REFERENCES devices(id) ON DELETE SET NULL",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS device_fingerprint TEXT",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS platform TEXT",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS device_name TEXT",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS client TEXT",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS client_id TEXT",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE device_subscription_tokens ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",

    # locations
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS country_code TEXT",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS is_recommended BOOLEAN DEFAULT FALSE",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS is_reserve BOOLEAN DEFAULT FALSE",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'online'",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS sort_order INTEGER DEFAULT 100",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS download_mbps DOUBLE PRECISION",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS upload_mbps DOUBLE PRECISION",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS ping_ms INTEGER",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS speed_checked_at TIMESTAMPTZ",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS vpn_payload JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS location_source TEXT DEFAULT 'catalog'",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE locations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",

    # payments
    "ALTER TABLE payments ADD COLUMN IF NOT EXISTS method TEXT",
    "ALTER TABLE payments ADD COLUMN IF NOT EXISTS currency TEXT DEFAULT 'RUB'",
    "ALTER TABLE payments ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'created'",
    "ALTER TABLE payments ADD COLUMN IF NOT EXISTS external_payment_id TEXT",
    "ALTER TABLE payments ADD COLUMN IF NOT EXISTS checkout_url TEXT",
    "ALTER TABLE payments ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE payments ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ",

    # notifications/errors
    "ALTER TABLE bot_notifications ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE bot_notifications ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE bot_notifications ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ",
    "ALTER TABLE bot_notifications ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ",
    "ALTER TABLE bot_notifications ADD COLUMN IF NOT EXISTS last_error TEXT",
    "ALTER TABLE bot_notifications ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE bot_notifications ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ",
    "ALTER TABLE bot_error_log ADD COLUMN IF NOT EXISTS context TEXT",
    "ALTER TABLE bot_error_log ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",

    # runtime settings
    "ALTER TABLE vpn_runtime_settings ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE vpn_runtime_settings ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE vpn_runtime_settings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",

    # indexes compatible with old databases
    "CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_subscription_token ON users(subscription_token) WHERE subscription_token IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id ON subscriptions(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON subscriptions(status)",
    "CREATE INDEX IF NOT EXISTS idx_devices_user_id ON devices(user_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_device_subscription_tokens_token ON device_subscription_tokens(token)",
    "CREATE INDEX IF NOT EXISTS idx_device_subscription_tokens_user_id ON device_subscription_tokens(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_device_subscription_tokens_device_id ON device_subscription_tokens(device_id)",
    "CREATE INDEX IF NOT EXISTS idx_device_subscription_tokens_fingerprint ON device_subscription_tokens(user_id, device_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status)",
    "CREATE INDEX IF NOT EXISTS idx_bot_notifications_unsent ON bot_notifications(sent_at, failed_at, next_retry_at, created_at)",
    "CREATE TABLE IF NOT EXISTS user_location_credentials (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, location_code TEXT NOT NULL, uuid TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(user_id, location_code))",
    "CREATE INDEX IF NOT EXISTS idx_user_location_credentials_user_id ON user_location_credentials(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_user_location_credentials_location_code ON user_location_credentials(location_code)",
    "CREATE TABLE IF NOT EXISTS user_device_location_credentials (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE, location_code TEXT NOT NULL, uuid TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(device_id, location_code))",
    "CREATE INDEX IF NOT EXISTS idx_user_device_location_credentials_user_device ON user_device_location_credentials(user_id, device_id)",
    "CREATE INDEX IF NOT EXISTS idx_user_device_location_credentials_location_code ON user_device_location_credentials(location_code)",
    "CREATE INDEX IF NOT EXISTS idx_virtual_location_assignments_virtual_code ON virtual_location_assignments(virtual_code)",
    "CREATE INDEX IF NOT EXISTS idx_virtual_location_assignments_concrete_code ON virtual_location_assignments(concrete_code)",
    "CREATE TABLE IF NOT EXISTS vpn_location_sessions (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, device_fingerprint TEXT NOT NULL, location_code TEXT NOT NULL, client TEXT, platform TEXT, device_name TEXT, subscription_token TEXT, last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(user_id, device_fingerprint, location_code))",
    "ALTER TABLE vpn_location_sessions ADD COLUMN IF NOT EXISTS client TEXT",
    "ALTER TABLE vpn_location_sessions ADD COLUMN IF NOT EXISTS platform TEXT",
    "ALTER TABLE vpn_location_sessions ADD COLUMN IF NOT EXISTS device_name TEXT",
    "ALTER TABLE vpn_location_sessions ADD COLUMN IF NOT EXISTS subscription_token TEXT",
    "ALTER TABLE vpn_location_sessions ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE vpn_location_sessions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE vpn_location_sessions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
    "CREATE INDEX IF NOT EXISTS idx_vpn_location_sessions_location_seen ON vpn_location_sessions(location_code, last_seen_at)",
    "CREATE INDEX IF NOT EXISTS idx_vpn_location_sessions_user_device ON vpn_location_sessions(user_id, device_fingerprint)",
]

POST_MIGRATION_SQL = [
    "UPDATE users SET language = 'ru' WHERE language IS NULL OR language = ''",
    "UPDATE users SET status = 'active' WHERE status IS NULL OR status = ''",
    "UPDATE users SET device_limit_override = NULL WHERE device_limit_override IS NOT NULL AND device_limit_override <= 0",
    "UPDATE device_subscription_tokens SET is_active = TRUE WHERE is_active IS NULL",
    "UPDATE device_subscription_tokens SET created_at = NOW() WHERE created_at IS NULL",
    "UPDATE device_subscription_tokens SET updated_at = NOW() WHERE updated_at IS NULL",
    f"UPDATE plans SET device_limit = {int(settings.VPN_DEFAULT_DEVICE_LIMIT)} WHERE device_limit IS NULL OR device_limit <= 0",
    "UPDATE plans SET is_active = TRUE WHERE is_active IS NULL",
    "UPDATE plans SET source_env_key = code WHERE source_env_key IS NULL OR source_env_key = ''",
    "UPDATE subscriptions SET status = 'active' WHERE status IS NULL OR status = ''",
    "UPDATE locations SET status = 'online' WHERE status IS NULL OR status = ''",
    "UPDATE locations SET sort_order = 100 WHERE sort_order IS NULL",
    "UPDATE locations SET is_recommended = FALSE WHERE is_recommended IS NULL",
    "UPDATE locations SET is_reserve = FALSE WHERE is_reserve IS NULL",
    "UPDATE locations SET vpn_payload = '{}'::jsonb WHERE vpn_payload IS NULL",
    "UPDATE locations SET is_deleted = FALSE WHERE is_deleted IS NULL",
    "UPDATE locations SET location_source = 'catalog' WHERE location_source IS NULL OR location_source = ''",
    "UPDATE payments SET currency = 'RUB' WHERE currency IS NULL OR currency = ''",
    "UPDATE payments SET status = 'created' WHERE status IS NULL OR status = ''",
    "UPDATE bot_notifications SET payload = '{}'::jsonb WHERE payload IS NULL",
    "INSERT INTO vpn_runtime_settings (id, payload) VALUES (1, '{}'::jsonb) ON CONFLICT (id) DO NOTHING",
    "CREATE TABLE IF NOT EXISTS user_location_credentials (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, location_code TEXT NOT NULL, uuid TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(user_id, location_code))",
    "UPDATE user_location_credentials SET status = 'active' WHERE status IS NULL OR status = ''",
    "CREATE TABLE IF NOT EXISTS user_device_location_credentials (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE, location_code TEXT NOT NULL, uuid TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(device_id, location_code))",
    "UPDATE user_device_location_credentials SET status = 'active' WHERE status IS NULL OR status = ''",
    "CREATE TABLE IF NOT EXISTS virtual_location_assignments (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, virtual_code TEXT NOT NULL, concrete_code TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(user_id, virtual_code))",
    "CREATE TABLE IF NOT EXISTS vpn_location_sessions (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, device_fingerprint TEXT NOT NULL, location_code TEXT NOT NULL, client TEXT, platform TEXT, device_name TEXT, subscription_token TEXT, last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(user_id, device_fingerprint, location_code))",
    "UPDATE locations SET name_ru = 'Авто | Самый быстрый' WHERE code = 'auto-fastest' AND (name_ru IS NULL OR BTRIM(name_ru) = '' OR name_ru IN ('Авто | Самый быстрый', '★ Авто | Самый быстрый'))",
    "UPDATE locations SET name_en = 'Auto | Fastest' WHERE code = 'auto-fastest' AND (name_en IS NULL OR BTRIM(name_en) = '' OR name_en IN ('Auto | Fastest', '★ Auto | Fastest'))",
    "UPDATE locations SET name_ru = 'Авто | Самый быстрый резерв' WHERE code = 'auto-reserve' AND (name_ru IS NULL OR BTRIM(name_ru) = '')",
    "UPDATE locations SET name_en = 'Auto | Fastest Reserve' WHERE code = 'auto-reserve' AND (name_en IS NULL OR BTRIM(name_en) = '')",
]

SERIAL_SEQUENCE_TARGETS = (
    ("users", "id"),
    ("plans", "id"),
    ("subscriptions", "id"),
    ("devices", "id"),
    ("device_subscription_tokens", "id"),
    ("locations", "id"),
    ("user_location_credentials", "id"),
    ("user_device_location_credentials", "id"),
    ("admin_notes", "id"),
    ("manual_extensions", "id"),
    ("bot_notifications", "id"),
    ("bot_error_log", "id"),
)



def _run_schema_migrations(cur: psycopg.Cursor) -> None:
    for statement in MIGRATION_SQL:
        cur.execute(statement)
    for statement in POST_MIGRATION_SQL:
        cur.execute(statement)


def _table_has_column(cur: psycopg.Cursor, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = CURRENT_SCHEMA()
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table_name, column_name),
    )
    return bool(cur.fetchone())


def _resync_serial_sequence(cur: psycopg.Cursor, table_name: str, column_name: str = "id") -> None:
    if not _table_has_column(cur, table_name, column_name):
        return
    cur.execute("SELECT pg_get_serial_sequence(%s, %s) AS sequence_name", (table_name, column_name))
    row = cur.fetchone()
    if not row:
        return
    sequence_name = row.get("sequence_name")
    if not sequence_name:
        return
    cur.execute(
        sql.SQL("SELECT COALESCE(MAX({column}), 0) AS max_id FROM {table}").format(
            column=sql.Identifier(column_name),
            table=sql.Identifier(table_name),
        )
    )
    max_id_row = cur.fetchone()
    max_id = int((max_id_row or {}).get("max_id") or 0)
    next_value = max(1, max_id + 1)
    cur.execute("SELECT setval(%s, %s, false)", (sequence_name, next_value))


def _resync_serial_sequences(cur: psycopg.Cursor) -> None:
    for table_name, column_name in SERIAL_SEQUENCE_TARGETS:
        _resync_serial_sequence(cur, table_name, column_name)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def db() -> psycopg.Connection:
    return psycopg.connect(settings.DATABASE_URL, row_factory=dict_row)


def bootstrap() -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_TABLES_SQL)
            _run_schema_migrations(cur)
            _resync_serial_sequences(cur)
            _cleanup_tracking_alias_duplicates(cur)
        conn.commit()
    apply_runtime_settings_overrides()
    sync_plans_from_env()
    sync_locations_catalog()


def sync_plans_from_env() -> None:
    source_aliases = _canonical_plan_sources()
    canonical_sources = tuple(source_aliases.keys())
    obsolete_sources = tuple({alias for values in source_aliases.values() for alias in values if alias not in canonical_sources})
    with db() as conn:
        with conn.cursor() as cur:
            for plan in settings.plan_definitions():
                aliases = tuple(source_aliases.get(plan["source_env_key"], [plan["source_env_key"]]))
                cur.execute(
                    "SELECT id FROM plans WHERE source_env_key = ANY(%s) ORDER BY updated_at DESC NULLS LAST, id DESC LIMIT 1",
                    (list(aliases),),
                )
                existing = cur.fetchone()
                if existing:
                    cur.execute(
                        """
                        UPDATE plans
                        SET code = %(code)s,
                            name_ru = %(name_ru)s,
                            name_en = %(name_en)s,
                            price_rub = %(price_rub)s,
                            duration_days = %(duration_days)s,
                            device_limit = %(device_limit)s,
                            is_active = %(is_active)s,
                            source_env_key = %(source_env_key)s,
                            updated_at = NOW()
                        WHERE id = %(id)s
                        """,
                        {**plan, "id": existing["id"]},
                    )
                    cur.execute(
                        "UPDATE plans SET is_active = FALSE, updated_at = NOW() WHERE source_env_key = ANY(%s) AND id <> %s",
                        (list(aliases), existing["id"]),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO plans (code, name_ru, name_en, price_rub, duration_days, device_limit, is_active, source_env_key)
                        VALUES (%(code)s, %(name_ru)s, %(name_en)s, %(price_rub)s, %(duration_days)s, %(device_limit)s, %(is_active)s, %(source_env_key)s)
                        ON CONFLICT (code) DO UPDATE SET
                            name_ru = EXCLUDED.name_ru,
                            name_en = EXCLUDED.name_en,
                            price_rub = EXCLUDED.price_rub,
                            duration_days = EXCLUDED.duration_days,
                            device_limit = EXCLUDED.device_limit,
                            is_active = EXCLUDED.is_active,
                            source_env_key = EXCLUDED.source_env_key,
                            updated_at = NOW()
                        """,
                        plan,
                    )
            if obsolete_sources:
                cur.execute(
                    "UPDATE plans SET is_active = FALSE, updated_at = NOW() WHERE source_env_key = ANY(%s)",
                    (list(obsolete_sources),),
                )
        conn.commit()


def _coerce_runtime_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _coerce_runtime_int(value: Any, default: int, minimum: int = 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return max(minimum, int(default))
    return max(minimum, parsed)


def _coerce_runtime_str(value: Any, default: str) -> str:
    text_value = str(value or "").strip()
    return text_value or default


def _coerce_runtime_languages(value: Any, default: List[str]) -> List[str]:
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    elif isinstance(value, list):
        items = [str(item).strip() for item in value]
    else:
        items = list(default or [])
    normalized = [item for item in items if item]
    return normalized or list(default or ["ru", "en"])


def _canonical_plan_sources() -> Dict[str, List[str]]:
    return {
        "PLAN_DAILY": ["PLAN_DAILY"],
        "PLAN_MONTHLY_1": ["PLAN_MONTHLY_1", "PLAN_MONTHLY", "PLAN_DEVICE_1"],
        "PLAN_MONTHLY_2": ["PLAN_MONTHLY_2", "PLAN_DEVICE_2"],
        "PLAN_MONTHLY_3": ["PLAN_MONTHLY_3", "PLAN_DEVICE_3"],
    }


def _runtime_plan_slot(plan: Dict[str, Any]) -> Optional[str]:
    source_key = str(plan.get("source_env_key") or "").strip().upper()
    legacy_map = {
        "PLAN_DAILY": "daily",
        "PLAN_MONTHLY": "monthly_1",
        "PLAN_DEVICE_1": "monthly_1",
        "PLAN_DEVICE_2": "monthly_2",
        "PLAN_DEVICE_3": "monthly_3",
        "PLAN_MONTHLY_1": "monthly_1",
        "PLAN_MONTHLY_2": "monthly_2",
        "PLAN_MONTHLY_3": "monthly_3",
    }
    if source_key in legacy_map:
        return legacy_map[source_key]
    slot = str(plan.get("slot") or "").strip().lower()
    return slot or None


def _current_runtime_plan_payloads() -> List[Dict[str, Any]]:
    order = {"daily": 0, "monthly_1": 1, "monthly_2": 2, "monthly_3": 3}
    picked: Dict[str, Dict[str, Any]] = {}
    canonical_sources = set(_canonical_plan_sources().keys())
    for item in get_all_plans():
        slot = _runtime_plan_slot(item)
        if slot not in {"daily", "monthly_1", "monthly_2", "monthly_3"}:
            continue
        row = {
            "slot": slot,
            "code": str(item.get("code") or "").strip() or slot,
            "name_ru": str(item.get("name_ru") or "").strip() or slot,
            "name_en": str(item.get("name_en") or item.get("name_ru") or "").strip() or slot,
            "price_rub": _coerce_runtime_int(item.get("price_rub"), 0, minimum=0),
            "duration_days": _coerce_runtime_int(item.get("duration_days"), 1, minimum=1),
            "device_limit": _coerce_runtime_int(item.get("device_limit"), settings.VPN_MAX_DEVICES_PER_ACCOUNT, minimum=1),
            "is_active": bool(item.get("is_active", True)),
            "_source_env_key": str(item.get("source_env_key") or "").strip().upper(),
        }
        current = picked.get(slot)
        if current is None:
            picked[slot] = row
            continue
        row_rank = (1 if row["is_active"] else 0, 1 if row["_source_env_key"] in canonical_sources else 0)
        current_rank = (1 if current["is_active"] else 0, 1 if current["_source_env_key"] in canonical_sources else 0)
        if row_rank > current_rank:
            picked[slot] = row
    plans = []
    for slot in ("daily", "monthly_1", "monthly_2", "monthly_3"):
        row = picked.get(slot)
        if not row:
            continue
        row.pop("_source_env_key", None)
        plans.append(row)
    plans.sort(key=lambda item: order.get(item["slot"], 999))
    return plans


def _normalize_client_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    return "v2raytun" if mode == "v2raytun" else "hiddify"


def _client_store_urls(mode: Optional[str] = None) -> Dict[str, str]:
    selected = _normalize_client_mode(mode if mode is not None else getattr(settings, "VPN_CLIENT_MODE", "hiddify"))
    if selected == "v2raytun":
        mobile = {
            "android_app_url": str(getattr(settings, "V2RAYTUN_ANDROID_APP_URL", "") or "").strip(),
            "ios_app_url": str(getattr(settings, "V2RAYTUN_IOS_APP_URL", "") or "").strip(),
            "android_app_package": str(getattr(settings, "V2RAYTUN_ANDROID_APP_PACKAGE", "") or "").strip(),
        }
    else:
        mobile = {
            "android_app_url": str(getattr(settings, "HIDDIFY_ANDROID_APP_URL", getattr(settings, "ANDROID_APP_URL", "")) or "").strip(),
            "ios_app_url": str(getattr(settings, "HIDDIFY_IOS_APP_URL", getattr(settings, "IOS_APP_URL", "")) or "").strip(),
            "android_app_package": str(getattr(settings, "HIDDIFY_ANDROID_APP_PACKAGE", getattr(settings, "ANDROID_APP_PACKAGE", "")) or "").strip(),
        }
    return {
        **mobile,
        "windows_app_url": str(getattr(settings, "HAPP_WINDOWS_APP_URL", getattr(settings, "WINDOWS_APP_URL", "")) or "").strip(),
        "macos_app_url": str(getattr(settings, "HAPP_MACOS_APP_URL", getattr(settings, "MACOS_APP_URL", "")) or "").strip(),
    }


def _active_client_name(mode: Optional[str] = None) -> str:
    return "v2RayTun" if _normalize_client_mode(mode) == "v2raytun" else "Hiddify"


def _desktop_client_name() -> str:
    return "Happ"


def get_runtime_settings_payload() -> Dict[str, Any]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM vpn_runtime_settings WHERE id = 1")
            row = cur.fetchone()
    payload = row.get("payload") if row else {}
    return dict(payload or {})


def apply_runtime_settings_overrides(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    data = dict(payload or get_runtime_settings_payload() or {})
    settings.APP_NAME = _coerce_runtime_str(data.get("app_name"), settings.APP_NAME)
    settings.VPN_CLIENT_MODE = _normalize_client_mode(data.get("client_mode") or getattr(settings, "VPN_CLIENT_MODE", "hiddify"))
    settings.APP_ENV = _coerce_runtime_str(data.get("app_env"), settings.APP_ENV)
    settings.APP_LANGS = _coerce_runtime_languages(data.get("languages"), list(settings.APP_LANGS or ["ru", "en"]))
    settings.BOT_NAME = _coerce_runtime_str(data.get("bot_name"), settings.BOT_NAME)
    settings.BOT_USERNAME = _coerce_runtime_str(data.get("bot_username"), settings.BOT_USERNAME)
    settings.SUPPORT_TELEGRAM_URL = _coerce_runtime_str(data.get("support_telegram_url"), settings.SUPPORT_TELEGRAM_URL)
    settings.PAYMENTS_ENABLED = _coerce_runtime_bool(data.get("payments_enabled"), settings.PAYMENTS_ENABLED)
    settings.VPN_MAINTENANCE_MODE = _coerce_runtime_bool(data.get("maintenance_mode"), settings.VPN_MAINTENANCE_MODE)
    settings.VPN_NEW_ACTIVATIONS_ENABLED = _coerce_runtime_bool(data.get("new_activations_enabled"), settings.VPN_NEW_ACTIVATIONS_ENABLED)
    max_devices = _coerce_runtime_int(data.get("max_devices_per_account"), settings.VPN_MAX_DEVICES_PER_ACCOUNT, minimum=1)
    plan_limits = [
        _as_positive_int(item.get("device_limit"))
        for item in (data.get("plans") or [])
        if isinstance(item, dict)
    ]
    max_plan_limit = max([limit for limit in plan_limits if limit is not None], default=1)
    effective_max_devices = max(max_devices, max_plan_limit)
    default_device_limit = _coerce_runtime_int(data.get("device_limit"), effective_max_devices, minimum=1)
    settings.VPN_MAX_DEVICES_PER_ACCOUNT = effective_max_devices
    settings.VPN_DEFAULT_DEVICE_LIMIT = min(default_device_limit, effective_max_devices)
    settings.VPN_ACCESS_MODE = "free" if str(data.get("access_mode") or getattr(settings, "VPN_ACCESS_MODE", "paid")).strip().lower() == "free" else "paid"
    settings.VPN_FREE_MODE_DEVICE_LIMIT = min(
        _coerce_runtime_int(data.get("free_mode_device_limit"), settings.VPN_DEFAULT_DEVICE_LIMIT, minimum=1),
        settings.VPN_MAX_DEVICES_PER_ACCOUNT,
    )
    settings.VPN_PAID_GRACE_HOURS = _coerce_runtime_int(data.get("paid_grace_hours"), getattr(settings, "VPN_PAID_GRACE_HOURS", 24), minimum=1)
    settings.VPN_SETTINGS_EDITABLE = True

    plans = data.get("plans") if isinstance(data.get("plans"), list) else _current_runtime_plan_payloads()
    plan_by_slot = {
        slot: item
        for item in plans
        if isinstance(item, dict) and (slot := _runtime_plan_slot(item)) in {"daily", "monthly_1", "monthly_2", "monthly_3"}
    }

    slot_meta = {
        "daily": ("PLAN_DAILY", "PLAN_DAILY_CODE", "PLAN_DAILY_NAME_RU", "PLAN_DAILY_NAME_EN", "PLAN_DAILY_PRICE_RUB", "PLAN_DAILY_DURATION_DAYS", "PLAN_DAILY_DEVICE_LIMIT", "PLAN_DAILY_ENABLED"),
        "monthly_1": ("PLAN_MONTHLY_1", "PLAN_MONTHLY_1_CODE", "PLAN_MONTHLY_1_NAME_RU", "PLAN_MONTHLY_1_NAME_EN", "PLAN_MONTHLY_1_PRICE_RUB", "PLAN_MONTHLY_1_DURATION_DAYS", "PLAN_MONTHLY_1_DEVICE_LIMIT", "PLAN_MONTHLY_1_ENABLED"),
        "monthly_2": ("PLAN_MONTHLY_2", "PLAN_MONTHLY_2_CODE", "PLAN_MONTHLY_2_NAME_RU", "PLAN_MONTHLY_2_NAME_EN", "PLAN_MONTHLY_2_PRICE_RUB", "PLAN_MONTHLY_2_DURATION_DAYS", "PLAN_MONTHLY_2_DEVICE_LIMIT", "PLAN_MONTHLY_2_ENABLED"),
        "monthly_3": ("PLAN_MONTHLY_3", "PLAN_MONTHLY_3_CODE", "PLAN_MONTHLY_3_NAME_RU", "PLAN_MONTHLY_3_NAME_EN", "PLAN_MONTHLY_3_PRICE_RUB", "PLAN_MONTHLY_3_DURATION_DAYS", "PLAN_MONTHLY_3_DEVICE_LIMIT", "PLAN_MONTHLY_3_ENABLED"),
    }
    for slot, (_source_key, code_attr, name_ru_attr, name_en_attr, price_attr, duration_attr, limit_attr, active_attr) in slot_meta.items():
        plan = plan_by_slot.get(slot)
        if not plan:
            continue
        setattr(settings, code_attr, _coerce_runtime_str(plan.get("code"), getattr(settings, code_attr)))
        setattr(settings, name_ru_attr, _coerce_runtime_str(plan.get("name_ru"), getattr(settings, name_ru_attr)))
        setattr(settings, name_en_attr, _coerce_runtime_str(plan.get("name_en"), getattr(settings, name_en_attr)))
        setattr(settings, price_attr, _coerce_runtime_int(plan.get("price_rub"), getattr(settings, price_attr), minimum=0))
        fixed_duration = 1 if slot == "daily" else 30
        fixed_limit = 1 if slot in {"daily", "monthly_1"} else (2 if slot == "monthly_2" else 3)
        setattr(settings, duration_attr, fixed_duration)
        setattr(settings, limit_attr, fixed_limit)
        setattr(settings, active_attr, _coerce_runtime_bool(plan.get("is_active"), getattr(settings, active_attr)))

    return data


def save_runtime_settings_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    previous = get_runtime_settings_payload()
    prev_mode = "free" if str(previous.get("access_mode") or getattr(settings, "VPN_ACCESS_MODE", "paid")).strip().lower() == "free" else "paid"
    next_mode = "free" if str(payload.get("access_mode") or previous.get("access_mode") or getattr(settings, "VPN_ACCESS_MODE", "paid")).strip().lower() == "free" else "paid"
    now_iso = datetime.now(timezone.utc).isoformat()
    normalized = {
        "app_name": _coerce_runtime_str(payload.get("app_name"), settings.APP_NAME),
        "client_mode": _normalize_client_mode(payload.get("client_mode") or getattr(settings, "VPN_CLIENT_MODE", "hiddify")),
        "app_env": _coerce_runtime_str(payload.get("app_env"), settings.APP_ENV),
        "languages": _coerce_runtime_languages(payload.get("languages"), list(settings.APP_LANGS or ["ru", "en"])),
        "bot_name": _coerce_runtime_str(payload.get("bot_name"), settings.BOT_NAME),
        "bot_username": _coerce_runtime_str(payload.get("bot_username"), settings.BOT_USERNAME),
        "support_telegram_url": _coerce_runtime_str(payload.get("support_telegram_url"), settings.SUPPORT_TELEGRAM_URL),
        "payments_enabled": _coerce_runtime_bool(payload.get("payments_enabled"), settings.PAYMENTS_ENABLED),
        "maintenance_mode": _coerce_runtime_bool(payload.get("maintenance_mode"), settings.VPN_MAINTENANCE_MODE),
        "new_activations_enabled": _coerce_runtime_bool(payload.get("new_activations_enabled"), settings.VPN_NEW_ACTIVATIONS_ENABLED),
        "access_mode": next_mode,
        "free_mode_device_limit": _coerce_runtime_int(payload.get("free_mode_device_limit"), previous.get("free_mode_device_limit") or getattr(settings, "VPN_FREE_MODE_DEVICE_LIMIT", settings.VPN_DEFAULT_DEVICE_LIMIT), minimum=1),
        "paid_grace_hours": _coerce_runtime_int(payload.get("paid_grace_hours"), previous.get("paid_grace_hours") or getattr(settings, "VPN_PAID_GRACE_HOURS", 24), minimum=1),
    }
    max_devices = _coerce_runtime_int(payload.get("max_devices_per_account"), settings.VPN_MAX_DEVICES_PER_ACCOUNT, minimum=1)
    normalized["max_devices_per_account"] = max_devices
    normalized["device_limit"] = min(
        _coerce_runtime_int(payload.get("device_limit"), max_devices, minimum=1),
        max_devices,
    )

    plans_input = payload.get("plans") if isinstance(payload.get("plans"), list) else _current_runtime_plan_payloads()
    normalized_plans: List[Dict[str, Any]] = []
    plan_bases = {
        "daily": (settings.PLAN_DAILY_CODE, settings.PLAN_DAILY_NAME_RU, settings.PLAN_DAILY_NAME_EN, settings.PLAN_DAILY_PRICE_RUB, settings.PLAN_DAILY_DURATION_DAYS, settings.PLAN_DAILY_DEVICE_LIMIT, settings.PLAN_DAILY_ENABLED),
        "monthly_1": (settings.PLAN_MONTHLY_1_CODE, settings.PLAN_MONTHLY_1_NAME_RU, settings.PLAN_MONTHLY_1_NAME_EN, settings.PLAN_MONTHLY_1_PRICE_RUB, settings.PLAN_MONTHLY_1_DURATION_DAYS, settings.PLAN_MONTHLY_1_DEVICE_LIMIT, settings.PLAN_MONTHLY_1_ENABLED),
        "monthly_2": (settings.PLAN_MONTHLY_2_CODE, settings.PLAN_MONTHLY_2_NAME_RU, settings.PLAN_MONTHLY_2_NAME_EN, settings.PLAN_MONTHLY_2_PRICE_RUB, settings.PLAN_MONTHLY_2_DURATION_DAYS, settings.PLAN_MONTHLY_2_DEVICE_LIMIT, settings.PLAN_MONTHLY_2_ENABLED),
        "monthly_3": (settings.PLAN_MONTHLY_3_CODE, settings.PLAN_MONTHLY_3_NAME_RU, settings.PLAN_MONTHLY_3_NAME_EN, settings.PLAN_MONTHLY_3_PRICE_RUB, settings.PLAN_MONTHLY_3_DURATION_DAYS, settings.PLAN_MONTHLY_3_DEVICE_LIMIT, settings.PLAN_MONTHLY_3_ENABLED),
    }
    for slot in ("daily", "monthly_1", "monthly_2", "monthly_3"):
        raw_plan = next((item for item in plans_input if isinstance(item, dict) and _runtime_plan_slot(item) == slot), None) or {}
        base_code, base_name_ru, base_name_en, base_price, base_duration, base_limit, base_active = plan_bases[slot]
        name_ru = _coerce_runtime_str(raw_plan.get("name_ru"), base_name_ru)
        name_en = _coerce_runtime_str(raw_plan.get("name_en"), raw_plan.get("name_ru") or base_name_en)
        fixed_duration = 1 if slot == "daily" else 30
        fixed_limit = 1 if slot in {"daily", "monthly_1"} else (2 if slot == "monthly_2" else 3)
        normalized_plans.append({
            "slot": slot,
            "code": _coerce_runtime_str(raw_plan.get("code"), base_code),
            "name_ru": name_ru,
            "name_en": name_en,
            "price_rub": _coerce_runtime_int(raw_plan.get("price_rub"), base_price, minimum=0),
            "duration_days": fixed_duration,
            "device_limit": min(fixed_limit, max_devices),
            "is_active": _coerce_runtime_bool(raw_plan.get("is_active"), base_active),
        })
    normalized["plans"] = normalized_plans

    # Preserve admin-managed 3X-UI server registry across normal VPN settings saves.
    # This lets operators add new 3X-UI servers from the admin panel without
    # changing code or Railway ENV every time a new node is created.
    if isinstance(payload.get("xui_servers"), list):
        normalized["xui_servers"] = payload.get("xui_servers")
    elif isinstance(previous.get("xui_servers"), list):
        normalized["xui_servers"] = previous.get("xui_servers")

    if prev_mode == "free" and next_mode == "paid":
        normalized["paid_grace_started_at"] = now_iso
    elif next_mode == "free":
        normalized["paid_grace_started_at"] = None
    else:
        normalized["paid_grace_started_at"] = previous.get("paid_grace_started_at")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO vpn_runtime_settings (id, payload, updated_at)
                VALUES (1, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
                """,
                (Jsonb(normalized),),
            )
        conn.commit()
    apply_runtime_settings_overrides(normalized)
    sync_plans_from_env()
    return normalized


def _parse_locations_json(raw_json: str) -> List[Dict[str, Any]]:
    try:
        raw_locations = json.loads(raw_json or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(raw_locations, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw_locations, start=1):
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or "").strip()
        name_ru = str(item.get("name_ru") or "").strip()
        name_en = str(item.get("name_en") or name_ru).strip()
        if not code or not name_ru or not name_en:
            continue
        country_code = item.get("country_code")
        if country_code is not None:
            country_code = str(country_code).strip().upper() or None
        normalized.append(
            {
                "code": code,
                "name_ru": name_ru,
                "name_en": name_en,
                "country_code": country_code,
                "is_active": bool(item.get("is_active", True)),
                "is_recommended": bool(item.get("is_recommended", False)),
                "is_reserve": bool(item.get("is_reserve", False)),
                "status": str(item.get("status") or "online").strip() or "online",
                "sort_order": int(item.get("sort_order") or idx * 10),
                "vpn_payload": item.get("vpn_payload") if isinstance(item.get("vpn_payload"), dict) else {},
                "is_deleted": bool(item.get("is_deleted", False)),
                "location_source": str(item.get("location_source") or "catalog").strip() or "catalog",
            }
        )
    return normalized



def _load_default_locations() -> List[Dict[str, Any]]:
    builtin_locations = _parse_locations_json(settings.DEFAULT_LOCATIONS_JSON)
    if settings.DEFAULT_LOCATIONS_ENV_OVERRIDE_ENABLED:
        env_locations = _parse_locations_json(settings.DEFAULT_LOCATIONS_ENV_JSON)
        if env_locations:
            return env_locations
    return builtin_locations


def _parse_json_object_if_possible(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        candidate = raw.strip()
        if not candidate:
            return None
        try:
            value = json.loads(candidate)
        except Exception:
            return None
        if isinstance(value, dict):
            return value
    return None


def _extract_dns_servers_from_xray(payload: Dict[str, Any]) -> List[str]:
    dns = payload.get("dns")
    if not isinstance(dns, dict):
        return []
    servers = dns.get("servers")
    if not isinstance(servers, list):
        return []
    result: List[str] = []
    for item in servers:
        if isinstance(item, str) and item.strip():
            result.append(item.strip())
        elif isinstance(item, dict):
            address = str(item.get("address") or "").strip()
            if address:
                result.append(address)
    return result


def _convert_raw_xray_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    outbounds = payload.get("outbounds")
    if not isinstance(outbounds, list):
        return {}

    proxy = None
    for outbound in outbounds:
        if not isinstance(outbound, dict):
            continue
        protocol = str(outbound.get("protocol") or "").strip().lower()
        if protocol == "vless":
            proxy = outbound
            if str(outbound.get("tag") or "").strip().lower() == "proxy":
                break
    if not isinstance(proxy, dict):
        return {}

    settings_payload = proxy.get("settings")
    stream_settings = proxy.get("streamSettings") if isinstance(proxy.get("streamSettings"), dict) else {}
    vnext = settings_payload.get("vnext") if isinstance(settings_payload, dict) else None
    if not isinstance(vnext, list) or not vnext:
        return {}
    upstream = vnext[0] if isinstance(vnext[0], dict) else {}
    users = upstream.get("users") if isinstance(upstream, dict) else None
    if not isinstance(users, list) or not users:
        return {}
    user = users[0] if isinstance(users[0], dict) else {}

    grpc_settings = stream_settings.get("grpcSettings") if isinstance(stream_settings.get("grpcSettings"), dict) else {}
    ws_settings = stream_settings.get("wsSettings") if isinstance(stream_settings.get("wsSettings"), dict) else {}
    xhttp_settings = stream_settings.get("xhttpSettings") if isinstance(stream_settings.get("xhttpSettings"), dict) else {}
    reality_settings = stream_settings.get("realitySettings") if isinstance(stream_settings.get("realitySettings"), dict) else {}
    tls_settings = stream_settings.get("tlsSettings") if isinstance(stream_settings.get("tlsSettings"), dict) else {}

    security = str(stream_settings.get("security") or proxy.get("security") or "reality").strip() or "reality"
    server_name = str(
        reality_settings.get("serverName")
        or tls_settings.get("serverName")
        or proxy.get("sni")
        or ""
    ).strip()

    converted: Dict[str, Any] = {
        "protocol": "vless",
        "engine": "xray",
        "server": str(upstream.get("address") or "").strip(),
        "port": upstream.get("port"),
        "uuid": str(user.get("id") or "").strip(),
        "transport": str(stream_settings.get("network") or proxy.get("network") or "tcp").strip() or "tcp",
        "network": str(stream_settings.get("network") or proxy.get("network") or "tcp").strip() or "tcp",
        "security": security,
        "flow": str(user.get("flow") or "").strip() or None,
        "sni": server_name or None,
        "server_name": server_name or None,
        "service_name": str(grpc_settings.get("serviceName") or "").strip() or None,
        "mode": str(xhttp_settings.get("mode") or "").strip() or None,
        "public_key": str(reality_settings.get("publicKey") or "").strip() or None,
        "short_id": str(reality_settings.get("shortId") or "").strip() or None,
        "fingerprint": str(reality_settings.get("fingerprint") or tls_settings.get("fingerprint") or "").strip() or None,
        "allow_insecure": bool(tls_settings.get("allowInsecure") or proxy.get("allowInsecure") or False),
        "host": None,
        "path": None,
        "packet_encoding": "xudp",
        "domain_resolver": "dns-remote",
        "connect_mode": "tun",
        "full_tunnel": True,
        "raw_xray_config": json.dumps(payload, ensure_ascii=False),
        "rawXrayConfig": json.dumps(payload, ensure_ascii=False),
    }

    if converted["transport"] in {"ws", "websocket"}:
        converted["host"] = str(ws_settings.get("headers", {}).get("Host") or "").strip() or None
        converted["path"] = str(ws_settings.get("path") or "/").strip() or "/"
    elif converted["transport"] == "xhttp":
        converted["host"] = str(xhttp_settings.get("host") or "").strip() or None
        converted["path"] = str(xhttp_settings.get("path") or "/").strip() or "/"

    dns_servers = _extract_dns_servers_from_xray(payload)
    if dns_servers:
        converted["dns_servers"] = dns_servers
        converted["dnsServers"] = dns_servers

    alpn = tls_settings.get("alpn")
    if isinstance(alpn, list):
        normalized_alpn = [str(item).strip() for item in alpn if str(item).strip()]
        if normalized_alpn:
            converted["alpn"] = normalized_alpn

    return {key: value for key, value in converted.items() if value is not None and value != ""}


def _antiblock_location_code(payload: Dict[str, Any]) -> str:
    code = str(payload.get("location_code") or payload.get("code") or payload.get("locationCode") or "").strip().lower()
    if code:
        return code
    remark = str(payload.get("remark") or payload.get("display_name") or "").strip().lower()
    if "lte" in remark:
        return "ru-lte"
    if "fast / international" in remark or "international" in remark:
        return "intl-fast"
    return ""


def _is_antiblock_location_code(code: str) -> bool:
    normalized = str(code or "").strip().lower()
    return normalized.startswith("ru-lte") or normalized.startswith("intl-fast")


def _antiblock_sni_pool_for_code(code: str) -> List[str]:
    normalized = str(code or "").strip().lower()
    if normalized.startswith("ru-lte"):
        pool = list(getattr(settings, "VLESS_ANTI_BLOCK_SNI_POOL_RU_LTE", []) or [])
    elif normalized.startswith("intl-fast"):
        pool = list(getattr(settings, "VLESS_ANTI_BLOCK_SNI_POOL_INTL", []) or [])
    else:
        pool = []
    if not pool:
        pool = list(getattr(settings, "VLESS_ANTI_BLOCK_SNI_POOL", []) or [])
    cleaned = [str(item or "").strip() for item in pool if str(item or "").strip()]
    return cleaned


def _choose_antiblock_sni(payload: Dict[str, Any], code: str) -> str:
    pool = _antiblock_sni_pool_for_code(code)
    seed_parts = [
        code,
        str(payload.get("server") or "").strip(),
        str(payload.get("public_key") or payload.get("publicKey") or "").strip(),
        str(payload.get("short_id") or payload.get("shortId") or "").strip(),
        str(payload.get("uuid") or "").strip(),
        str(payload.get("remark") or payload.get("display_name") or "").strip(),
    ]
    seed = "|".join(seed_parts) or code or "antiblock"
    idx = int(hashlib.sha256(seed.encode("utf-8")).hexdigest(), 16) % len(pool)
    return pool[idx]


def _apply_anti_block_profile(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload or {})
    if not normalized:
        return {}
    code = _antiblock_location_code(normalized)
    if not _is_antiblock_location_code(code):
        return normalized

    normalized.setdefault("engine", "xray")
    normalized.setdefault("protocol", "vless")
    transport = str(normalized.get("transport") or normalized.get("network") or "tcp").strip().lower() or "tcp"
    normalized["transport"] = transport
    normalized["network"] = transport

    explicit_sni = str(normalized.get("server_name") or normalized.get("sni") or "").strip()
    selected_sni = explicit_sni or _choose_antiblock_sni(normalized, code)
    normalized["server_name"] = selected_sni
    normalized["sni"] = selected_sni
    normalized.setdefault("fingerprint", "chrome")
    normalized.setdefault("mtu", 1400)
    normalized["domain_resolver"] = str(normalized.get("domain_resolver") or normalized.get("domainResolver") or "dns-remote").strip() or "dns-remote"
    normalized["packet_encoding"] = str(normalized.get("packet_encoding") or normalized.get("packetEncoding") or "xudp").strip() or "xudp"
    normalized["connect_mode"] = str(normalized.get("connect_mode") or normalized.get("connectMode") or "tun").strip() or "tun"
    normalized["full_tunnel"] = _coerce_runtime_bool(normalized.get("full_tunnel", normalized.get("fullTunnel", True)), True)
    normalized["dns_servers"] = [str(item or "").strip() for item in (normalized.get("dns_servers") or normalized.get("dnsServers") or ["1.1.1.1", "8.8.8.8"]) if str(item or "").strip()] or ["1.1.1.1", "8.8.8.8"]
    normalized["dnsServers"] = list(normalized["dns_servers"])

    has_reality_material = bool(str(normalized.get("public_key") or normalized.get("publicKey") or "").strip())
    security = str(normalized.get("security") or "").strip().lower()
    if has_reality_material or security in {"", "tls", "reality"}:
        normalized["security"] = "reality"

    if transport == "tcp":
        normalized["flow"] = "xtls-rprx-vision"
        normalized.pop("service_name", None)
        normalized.pop("serviceName", None)
        normalized.pop("path", None)
        normalized.pop("mode", None)
    else:
        if str(normalized.get("flow") or "").strip().lower() == "xtls-rprx-vision":
            normalized.pop("flow", None)
        if transport == "grpc":
            normalized.setdefault("service_name", "grpc")
            normalized["mode"] = "gun"
        elif transport in {"ws", "websocket"}:
            normalized["path"] = str(normalized.get("path") or "/").strip() or "/"
            normalized["host"] = str(normalized.get("host") or selected_sni).strip() or selected_sni
            normalized["security"] = "tls" if not has_reality_material else "reality"
        elif transport == "xhttp":
            normalized["path"] = str(normalized.get("path") or "/").strip() or "/"
            normalized["mode"] = str(normalized.get("mode") or "auto").strip() or "auto"
            normalized["host"] = str(normalized.get("host") or selected_sni).strip() or selected_sni
            normalized["security"] = "tls" if not has_reality_material else "reality"

    if code.startswith("ru-lte"):
        normalized["anti_block_profile"] = "lte"
        normalized["route_mode"] = str(normalized.get("route_mode") or "split").strip() or "split"
        normalized["direct_ru"] = _coerce_runtime_bool(normalized.get("direct_ru", True), True)
        normalized["direct_domains"] = [str(item or "").strip() for item in (normalized.get("direct_domains") or [".ru", ".su", ".xn--p1ai"]) if str(item or "").strip()] or [".ru", ".su", ".xn--p1ai"]
        normalized["full_tunnel"] = False
    else:
        normalized["anti_block_profile"] = "global"
    normalized["location_code"] = normalized.get("location_code") or code
    normalized["locationCode"] = normalized.get("locationCode") or normalized["location_code"]
    normalized["domainResolver"] = normalized["domain_resolver"]
    normalized["packetEncoding"] = normalized["packet_encoding"]
    normalized["connectMode"] = normalized["connect_mode"]
    return normalized


def _payload_direct_ru_enabled(payload: Dict[str, Any]) -> bool:
    # Respect explicit admin switch first. route_mode=split should not silently
    # re-enable RU direct if admin turned direct_ru off for troubleshooting.
    if "direct_ru" in payload:
        value = payload.get("direct_ru")
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
    route_mode = str(payload.get("route_mode") or "").strip().lower()
    if route_mode == "split":
        return True
    code = str(payload.get("location_code") or payload.get("locationCode") or payload.get("resolved_location_code") or "").strip().lower()
    return code.startswith("ru-lte")


def _is_ip_literal(value: Any) -> bool:
    try:
        ipaddress.ip_address(str(value or "").strip())
        return True
    except ValueError:
        return False


def _default_direct_domains(payload: Dict[str, Any]) -> List[str]:
    values = payload.get("direct_domains") or [".ru", ".su", ".xn--p1ai"]
    result = [str(item or "").strip() for item in values if str(item or "").strip()]
    return result or [".ru", ".su", ".xn--p1ai"]


def _default_xray_private_ip_rules() -> List[str]:
    return ["geoip:private", "geoip:loopback"]


def _xray_direct_domain_rules(payload: Dict[str, Any]) -> List[str]:
    rules: List[str] = []
    for suffix in _default_direct_domains(payload):
        cleaned = str(suffix or "").strip()
        if not cleaned:
            continue
        if cleaned.startswith("."):
            cleaned = cleaned[1:]
        if cleaned:
            rules.append(f"domain:{cleaned}")
    return rules


def _xray_force_proxy_domain_rules() -> List[str]:
    """Domains that must stay behind VPN even in RU split/LTE mode.

    With `geoip:ru -> direct`, some clients can resolve Telegram/YouTube/CDN
    endpoints to Russian or nearby cache IPs. If the direct RU rule is matched
    first, Telegram can stop opening or YouTube can show "video unavailable /
    skipped" while the VLESS tunnel itself is healthy. Put these proxy rules
    before RU-direct rules.
    """
    return [
        "geosite:telegram",
        "domain:telegram.org",
        "domain:t.me",
        "domain:tdesktop.com",
        "domain:telegra.ph",
        "geosite:youtube",
        "domain:youtube.com",
        "domain:youtu.be",
        "domain:googlevideo.com",
        "domain:ytimg.com",
        "domain:ggpht.com",
        "domain:googleusercontent.com",
        "domain:youtubei.googleapis.com",
        "geosite:instagram",
        "domain:instagram.com",
        "domain:cdninstagram.com",
        "domain:fbcdn.net",
    ]


def _xray_force_proxy_ip_rules() -> List[str]:
    # Avoid geoip:telegram here: not every client bundle ships that geosite/geoip entry.
    # Domain/SNI/sniffing rules above are safer and match the known-good config style.
    return []


def _ru_lte_geoip_direct_enabled() -> bool:
    return bool(getattr(settings, "RU_LTE_GEOIP_DIRECT_ENABLED", False))


def _ru_lte_geosite_direct_enabled() -> bool:
    return bool(getattr(settings, "RU_LTE_GEOSITE_DIRECT_ENABLED", False))


def _extract_proxy_outbound_from_raw(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = payload.get("raw_xray_config") or payload.get("rawXrayConfig")
    parsed = _parse_json_object_if_possible(raw)
    if not isinstance(parsed, dict):
        return {}
    outbounds = parsed.get("outbounds")
    if not isinstance(outbounds, list):
        return {}
    for outbound in outbounds:
        if isinstance(outbound, dict) and str(outbound.get("tag") or "").strip().lower() == "proxy":
            return dict(outbound)
    for outbound in outbounds:
        if isinstance(outbound, dict) and str(outbound.get("protocol") or "").strip().lower() == "vless":
            return dict(outbound)
    return {}


def _build_canonical_xray_stream_settings(payload: Dict[str, Any], *, existing_proxy: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    transport = str(payload.get("transport") or payload.get("network") or "tcp").strip().lower() or "tcp"
    network = "ws" if transport == "websocket" else transport
    security = str(payload.get("security") or "reality").strip().lower() or "reality"
    server_name = str(payload.get("server_name") or payload.get("sni") or payload.get("host") or "").strip()
    existing_stream = existing_proxy.get("streamSettings") if isinstance(existing_proxy, dict) and isinstance(existing_proxy.get("streamSettings"), dict) else {}
    stream_settings: Dict[str, Any] = {"network": network}
    if security == "reality":
        reality = existing_stream.get("realitySettings") if isinstance(existing_stream.get("realitySettings"), dict) else {}
        stream_settings["security"] = "reality"
        stream_settings["realitySettings"] = {
            "show": bool(reality.get("show", False)),
            "serverName": server_name,
            "fingerprint": str(payload.get("fingerprint") or reality.get("fingerprint") or "chrome").strip() or "chrome",
            "publicKey": str(payload.get("public_key") or payload.get("publicKey") or reality.get("publicKey") or "").strip(),
            "shortId": str(payload.get("short_id") or payload.get("shortId") or reality.get("shortId") or "").strip(),
            "spiderX": str(payload.get("spider_x") or payload.get("spiderX") or reality.get("spiderX") or "").strip(),
        }
        if not stream_settings["realitySettings"]["spiderX"]:
            if network in {"grpc", "xhttp", "ws"}:
                stream_settings["realitySettings"]["spiderX"] = "/"
            else:
                fallback_path = str(payload.get("path") or "/").strip() or "/"
                stream_settings["realitySettings"]["spiderX"] = fallback_path if fallback_path.startswith("/") else f"/{fallback_path.lstrip('/')}"
    elif security == "tls":
        tls = existing_stream.get("tlsSettings") if isinstance(existing_stream.get("tlsSettings"), dict) else {}
        stream_settings["security"] = "tls"
        tls_settings: Dict[str, Any] = {
            "serverName": server_name,
            "allowInsecure": bool(payload.get("allow_insecure") or payload.get("allowInsecure") or tls.get("allowInsecure") or False),
        }
        alpn = [str(item or "").strip() for item in (payload.get("alpn") or tls.get("alpn") or []) if str(item or "").strip()]
        if alpn:
            tls_settings["alpn"] = alpn
        stream_settings["tlsSettings"] = tls_settings
    else:
        stream_settings["security"] = "none"

    if network == "grpc":
        grpc_existing = existing_stream.get("grpcSettings") if isinstance(existing_stream.get("grpcSettings"), dict) else {}
        grpc_settings: Dict[str, Any] = {
            "serviceName": str(payload.get("service_name") or payload.get("serviceName") or grpc_existing.get("serviceName") or "").strip(),
            "multiMode": bool(grpc_existing.get("multiMode", False)),
        }
        authority = str(payload.get("authority") or grpc_existing.get("authority") or "").strip()
        if authority:
            grpc_settings["authority"] = authority
        stream_settings["grpcSettings"] = grpc_settings
    elif network == "ws":
        ws_existing = existing_stream.get("wsSettings") if isinstance(existing_stream.get("wsSettings"), dict) else {}
        ws_settings: Dict[str, Any] = {
            "path": str(payload.get("path") or ws_existing.get("path") or "/").strip() or "/",
        }
        host = str(payload.get("host") or ws_existing.get("headers", {}).get("Host") or server_name or "").strip()
        if host:
            ws_settings["headers"] = {"Host": host}
        stream_settings["wsSettings"] = ws_settings
    elif network == "xhttp":
        xhttp_existing = existing_stream.get("xhttpSettings") if isinstance(existing_stream.get("xhttpSettings"), dict) else {}
        xhttp_settings: Dict[str, Any] = {
            "path": str(payload.get("path") or xhttp_existing.get("path") or "/").strip() or "/",
            "mode": str(payload.get("mode") or xhttp_existing.get("mode") or "auto").strip() or "auto",
        }
        host = str(payload.get("host") or xhttp_existing.get("host") or server_name or "").strip()
        if host:
            xhttp_settings["host"] = host
        for key, value in xhttp_existing.items():
            if key not in xhttp_settings and value not in (None, "", [], {}):
                xhttp_settings[key] = value
        stream_settings["xhttpSettings"] = xhttp_settings
    return stream_settings


def _payload_endpoint_identity(payload: Dict[str, Any]) -> str:
    normalized = dict(payload or {})
    parts = [
        str(normalized.get("server") or "").strip().lower(),
        str(normalized.get("port") or normalized.get("server_port") or "").strip(),
        str(normalized.get("uuid") or normalized.get("id") or "").strip().lower(),
        str(normalized.get("public_key") or normalized.get("publicKey") or "").strip(),
        str(normalized.get("short_id") or normalized.get("shortId") or "").strip(),
        str(normalized.get("server_name") or normalized.get("sni") or "").strip().lower(),
        str(normalized.get("transport") or normalized.get("network") or "").strip().lower(),
        str(normalized.get("path") or "").strip(),
        str(normalized.get("service_name") or normalized.get("serviceName") or "").strip(),
        str(normalized.get("host") or "").strip().lower(),
        str(normalized.get("mode") or "").strip().lower(),
        str(normalized.get("security") or "").strip().lower(),
        str(normalized.get("flow") or "").strip(),
    ]
    return "|".join(parts)


def _probe_debug_identity(debug: Dict[str, Any]) -> str:
    if not isinstance(debug, dict):
        return ""
    parts = [
        str(debug.get("server") or "").strip().lower(),
        str(debug.get("port") or "").strip(),
        str(debug.get("uuid") or debug.get("id") or "").strip().lower(),
        str(debug.get("public_key") or debug.get("publicKey") or "").strip(),
        str(debug.get("short_id") or debug.get("shortId") or "").strip(),
        str(debug.get("server_name") or debug.get("sni") or "").strip().lower(),
        str(debug.get("transport") or debug.get("network") or "").strip().lower(),
        str(debug.get("path") or "").strip(),
        str(debug.get("service_name") or debug.get("serviceName") or "").strip(),
        str(debug.get("host") or "").strip().lower(),
        str(debug.get("mode") or "").strip().lower(),
        str(debug.get("security") or "").strip().lower(),
        str(debug.get("flow") or "").strip(),
    ]
    return "|".join(parts)


def _invalidate_stale_live_probe(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload or {})
    probe = normalized.get("_last_live_probe")
    if not isinstance(probe, dict):
        return normalized
    payload_identity = _payload_endpoint_identity(normalized)
    if not payload_identity:
        return normalized
    probe_identity = _probe_debug_identity(probe.get("debug") if isinstance(probe.get("debug"), dict) else {})
    if not probe_identity:
        return normalized
    if payload_identity == probe_identity:
        return normalized
    normalized.pop("_last_live_probe", None)
    return normalized


def _build_canonical_raw_xray_config(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    protocol = str(payload.get("protocol") or "vless").strip().lower() or "vless"
    server = str(payload.get("server") or "").strip()
    uuid = str(payload.get("uuid") or payload.get("id") or "").strip()
    try:
        port = int(payload.get("port") or payload.get("server_port") or 0)
    except (TypeError, ValueError):
        port = 0
    if protocol != "vless" or not server or port <= 0 or not uuid:
        return None

    existing_proxy = _extract_proxy_outbound_from_raw(payload)
    transport = str(payload.get("transport") or payload.get("network") or "tcp").strip().lower() or "tcp"
    user: Dict[str, Any] = {
        "id": uuid,
        "encryption": str(payload.get("encryption") or "none").strip() or "none",
    }
    flow = str(payload.get("flow") or "").strip()
    if flow:
        user["flow"] = flow

    dns_servers = [str(item or "").strip() for item in (payload.get("dns_servers") or payload.get("dnsServers") or ["1.1.1.1", "8.8.8.8"]) if str(item or "").strip()]
    if not dns_servers:
        dns_servers = ["1.1.1.1", "8.8.8.8"]

    first_direct_rule: Dict[str, Any]
    if _is_ip_literal(server):
        first_direct_rule = {"type": "field", "ip": [server], "outboundTag": "direct"}
    else:
        first_direct_rule = {"type": "field", "domain": [f"domain:{server}"], "outboundTag": "direct"}

    routing_rules: List[Dict[str, Any]] = [
        first_direct_rule,
        {"type": "field", "ip": _default_xray_private_ip_rules(), "outboundTag": "direct"},
        {"type": "field", "domain": ["domain:localhost"], "outboundTag": "direct"},
        {"type": "field", "protocol": ["bittorrent"], "outboundTag": "block"},
    ]
    if _payload_direct_ru_enabled(payload):
        # Order is critical. The known-good RU LTE config does NOT use broad geoip:ru direct.
        # Broad geoip direct can catch Telegram/YouTube/Googlevideo cache IPs and bypass VPN.
        routing_rules.insert(1, {"type": "field", "domain": _xray_force_proxy_domain_rules(), "outboundTag": "proxy"})
        force_proxy_ips = _xray_force_proxy_ip_rules()
        insert_at = 2
        if force_proxy_ips:
            routing_rules.insert(insert_at, {"type": "field", "ip": force_proxy_ips, "outboundTag": "proxy"})
            insert_at += 1
        if _ru_lte_geoip_direct_enabled():
            routing_rules.insert(insert_at, {"type": "field", "ip": ["geoip:ru"], "outboundTag": "direct"})
            insert_at += 1
        domain_rules = []
        if _ru_lte_geosite_direct_enabled():
            domain_rules.append("geosite:ru")
        domain_rules.extend(_xray_direct_domain_rules(payload))
        if domain_rules:
            routing_rules.insert(insert_at, {"type": "field", "domain": domain_rules, "outboundTag": "direct"})

    config: Dict[str, Any] = {
        "dns": {"queryStrategy": "UseIP", "servers": dns_servers},
        "inbounds": [
            {
                "listen": "127.0.0.1",
                "port": 10808,
                "protocol": "socks",
                "settings": {"auth": "noauth", "udp": True},
                "sniffing": {"enabled": True, "destOverride": ["http", "tls", "quic"], "routeOnly": False},
                "tag": "socks",
            },
            {
                "listen": "127.0.0.1",
                "port": 10809,
                "protocol": "http",
                "settings": {"allowTransparent": False},
                "sniffing": {"enabled": True, "destOverride": ["http", "tls", "quic"], "routeOnly": False},
                "tag": "http",
            },
            {
                "listen": "::1",
                "port": 1080,
                "protocol": "socks",
                "settings": {"udp": True},
                "tag": "socks-internal",
            },
        ],
        "meta": None,
        "outbounds": [
            {
                "protocol": protocol,
                "tag": "proxy",
                "settings": {
                    "vnext": [{
                        "address": server,
                        "port": port,
                        "users": [user],
                    }]
                },
                "streamSettings": _build_canonical_xray_stream_settings(payload, existing_proxy=existing_proxy),
            },
            {"protocol": "freedom", "tag": "direct", "settings": {"domainStrategy": "AsIs"}},
            {"protocol": "blackhole", "tag": "block"},
        ],
        "remarks": str(payload.get("remark") or payload.get("display_name") or payload.get("location_code") or "").strip(),
        "routing": {
            "domainMatcher": "hybrid",
            "domainStrategy": "IPIfNonMatch",
            "rules": routing_rules,
        },
    }
    return config


def _canonicalize_payload_metadata(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload or {})
    if not normalized:
        return {}

    canonical_code = str(
        normalized.get("location_code")
        or normalized.get("locationCode")
        or normalized.get("code")
        or normalized.get("resolved_location_code")
        or ""
    ).strip()
    if canonical_code:
        normalized["location_code"] = canonical_code
        normalized["locationCode"] = canonical_code
        normalized["resolved_location_code"] = canonical_code

    canonical_country = str(
        normalized.get("country_code")
        or normalized.get("resolved_country_code")
        or ""
    ).strip().upper()
    if canonical_country:
        normalized["country_code"] = canonical_country
        normalized["resolved_country_code"] = canonical_country

    canonical_name = str(normalized.get("remark") or normalized.get("display_name") or "").strip()
    if canonical_name:
        normalized["display_name"] = canonical_name
        normalized["remark"] = canonical_name

    canonical_server_name = str(normalized.get("server_name") or normalized.get("sni") or "").strip()
    if canonical_server_name:
        normalized["server_name"] = canonical_server_name
        normalized["sni"] = canonical_server_name

    canonical_public_key = str(normalized.get("public_key") or normalized.get("publicKey") or "").strip()
    if canonical_public_key:
        normalized["public_key"] = canonical_public_key
        normalized["publicKey"] = canonical_public_key

    canonical_short_id = str(normalized.get("short_id") or normalized.get("shortId") or "").strip()
    if canonical_short_id:
        normalized["short_id"] = canonical_short_id
        normalized["shortId"] = canonical_short_id

    rebuilt_raw = _build_canonical_raw_xray_config(normalized)
    if rebuilt_raw:
        raw_text = json.dumps(rebuilt_raw, ensure_ascii=False)
        normalized["raw_xray_config"] = raw_text
        normalized["rawXrayConfig"] = raw_text

    if isinstance(normalized.get("direct_domains"), list):
        normalized["direct_domains"] = [str(item or "").strip() for item in normalized.get("direct_domains") if str(item or "").strip()]

    return normalized


def _prepare_exact_admin_vpn_payload(
    payload: Dict[str, Any],
    *,
    canonical_code: str = "",
    canonical_country: str = "",
    canonical_name: str = "",
) -> Dict[str, Any]:
    """Prepare an admin-edited vpn_payload for exact storage.

    Exact admin save means: store the JSON/textarea + visible modal fields as the
    source of truth. Do not rebuild raw_xray_config, do not merge old DB values,
    and do not force credential_mode/access_mode to another mode.
    """
    full_payload = dict(payload or {})

    def _sync_pair(primary: str, alias: str) -> None:
        primary_has = primary in full_payload and full_payload.get(primary) not in (None, "")
        alias_has = alias in full_payload and full_payload.get(alias) not in (None, "")
        chosen = full_payload.get(primary) if primary_has else (full_payload.get(alias) if alias_has else None)
        if chosen not in (None, ""):
            full_payload[primary] = chosen
            full_payload[alias] = chosen

    for primary, alias in (
        ("managed_by", "managedBy"),
        ("xui_server_key", "xuiServerKey"),
        ("xui_inbound_id", "xuiInboundId"),
        ("credential_mode", "credentialMode"),
        ("access_mode", "accessMode"),
        ("uuid_mode", "uuidMode"),
        ("server_name", "sni"),
        ("public_key", "publicKey"),
        ("short_id", "shortId"),
        ("domain_resolver", "domainResolver"),
        ("packet_encoding", "packetEncoding"),
        ("location_code", "locationCode"),
    ):
        _sync_pair(primary, alias)

    xui_server_key = str(full_payload.get("xui_server_key") or full_payload.get("xuiServerKey") or "").strip()
    xui_inbound_id = full_payload.get("xui_inbound_id") if full_payload.get("xui_inbound_id") is not None else full_payload.get("xuiInboundId")
    managed_raw_original = str(full_payload.get("managed_by") or full_payload.get("managedBy") or "").strip()
    managed_raw = managed_raw_original.lower()
    # Exact admin save: the dropdown value is source of truth. Do not force
    # managed_by=3x-ui only because xui_server_key/inbound exists. This fixes
    # the modal reopening with the old/forced value after Save location.
    if managed_raw_original:
        if managed_raw in {"3x-ui", "3xui", "xui", "x-ui", "three-x-ui"}:
            full_payload["managed_by"] = "3x-ui"
            full_payload["managedBy"] = "3x-ui"
        else:
            full_payload["managed_by"] = managed_raw_original
            full_payload["managedBy"] = managed_raw_original
    if xui_server_key:
        full_payload["xui_server_key"] = xui_server_key
        full_payload["xuiServerKey"] = xui_server_key
    if str(xui_inbound_id or "").strip():
        try:
            inbound_int = int(float(str(xui_inbound_id).strip()))
        except (TypeError, ValueError):
            inbound_int = xui_inbound_id
        full_payload["xui_inbound_id"] = inbound_int
        full_payload["xuiInboundId"] = inbound_int

    code = str(canonical_code or full_payload.get("location_code") or full_payload.get("locationCode") or full_payload.get("resolved_location_code") or "").strip()
    country = str(canonical_country or full_payload.get("country_code") or full_payload.get("resolved_country_code") or "").strip().upper()
    name = str(canonical_name or full_payload.get("remark") or full_payload.get("display_name") or "").strip()
    if code:
        full_payload["location_code"] = code
        full_payload["locationCode"] = code
        full_payload["resolved_location_code"] = code
    if country:
        full_payload["country_code"] = country
        full_payload["resolved_country_code"] = country
    if name and not full_payload.get("remark"):
        full_payload["remark"] = name
    if name and not full_payload.get("display_name"):
        full_payload["display_name"] = name

    return full_payload


def _apply_admin_mobile_defaults(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload or {})
    if not normalized:
        return {}

    raw_xray_payload = None
    raw_xray = normalized.get("raw_xray_config") or normalized.get("rawXrayConfig")
    parsed_raw_xray = _parse_json_object_if_possible(raw_xray)
    if isinstance(parsed_raw_xray, dict):
        raw_xray_payload = parsed_raw_xray
    elif isinstance(normalized.get("outbounds"), list) and isinstance(normalized.get("inbounds"), list):
        # Admin may paste a full raw Xray JSON directly into vpn_payload.
        raw_xray_payload = dict(normalized)
        normalized.setdefault("raw_xray_config", json.dumps(raw_xray_payload, ensure_ascii=False))
        normalized.setdefault("rawXrayConfig", normalized.get("raw_xray_config"))

    if raw_xray_payload:
        converted = _convert_raw_xray_payload(raw_xray_payload)
        for key, value in converted.items():
            if normalized.get(key) in (None, "", [], {}):
                normalized[key] = value

    engine = str(normalized.get("engine") or "").strip().lower()
    if not engine:
        normalized["engine"] = "xray" if normalized.get("raw_xray_config") or normalized.get("rawXrayConfig") else "nekobox"
    elif engine == "xray-core":
        normalized["engine"] = "xray"

    normalized.setdefault("protocol", "vless")
    normalized.setdefault("transport", normalized.get("network") or "tcp")
    normalized.setdefault("network", normalized.get("transport") or "tcp")
    normalized.setdefault("security", "reality")
    normalized.setdefault("mtu", 1400)
    normalized.setdefault("domain_resolver", "dns-remote")
    normalized.setdefault("packet_encoding", "xudp")
    normalized.setdefault("connect_mode", "tun")
    normalized.setdefault("full_tunnel", True)

    dns_servers = normalized.get("dns_servers") or normalized.get("dnsServers")
    if not dns_servers:
        normalized["dns_servers"] = ["1.1.1.1", "8.8.8.8"]
        normalized["dnsServers"] = ["1.1.1.1", "8.8.8.8"]
    return _apply_anti_block_profile(normalized)


def _normalize_vpn_payload_keys(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload or {})
    if not normalized:
        return {}

    def _text_value(key: str) -> str:
        return str(normalized.get(key) or "").strip()

    def _sync_alias(primary_key: str, alias_key: str) -> None:
        primary_value = _text_value(primary_key)
        alias_value = _text_value(alias_key)
        chosen = primary_value or alias_value
        if chosen:
            normalized[primary_key] = chosen
            normalized[alias_key] = chosen

    if "server_port" in normalized and "port" not in normalized:
        normalized["port"] = normalized.get("server_port")
    if "id" in normalized and "uuid" not in normalized:
        normalized["uuid"] = normalized.get("id")
    if "network" in normalized and "transport" not in normalized:
        normalized["transport"] = normalized.get("network")
    if "transport" in normalized and "network" not in normalized:
        normalized["network"] = normalized.get("transport")
    _sync_alias("server_name", "sni")
    _sync_alias("service_name", "serviceName")
    _sync_alias("public_key", "publicKey")
    _sync_alias("short_id", "shortId")
    if "dnsServers" in normalized and "dns_servers" not in normalized:
        normalized["dns_servers"] = normalized.get("dnsServers")
    if "dns_servers" in normalized and "dnsServers" not in normalized:
        normalized["dnsServers"] = normalized.get("dns_servers")
    if "allowInsecure" in normalized and "allow_insecure" not in normalized:
        normalized["allow_insecure"] = normalized.get("allowInsecure")
    if "allow_insecure" in normalized and "allowInsecure" not in normalized:
        normalized["allowInsecure"] = normalized.get("allow_insecure")
    _sync_alias("domain_resolver", "domainResolver")
    _sync_alias("packet_encoding", "packetEncoding")
    if "rawSingBoxConfig" in normalized and "raw_sing_box_config" not in normalized:
        normalized["raw_sing_box_config"] = normalized.get("rawSingBoxConfig")
    if "raw_sing_box_config" in normalized and "rawSingBoxConfig" not in normalized:
        normalized["rawSingBoxConfig"] = normalized.get("raw_sing_box_config")
    if "rawXrayConfig" in normalized and "raw_xray_config" not in normalized:
        normalized["raw_xray_config"] = normalized.get("rawXrayConfig")
    if "raw_xray_config" in normalized and "rawXrayConfig" not in normalized:
        normalized["rawXrayConfig"] = normalized.get("raw_xray_config")

    # Admin UI can send camelCase aliases and can also have XUI fields filled
    # while managed_by dropdown is still manual/shared. Do not drop those fields:
    # any non-empty xui_inbound_id or non-default xui_server_key means this is a
    # 3X-UI-managed template and must be saved as such.
    if "managedBy" in normalized and "managed_by" not in normalized:
        normalized["managed_by"] = normalized.get("managedBy")
    if "managed_by" in normalized and "managedBy" not in normalized:
        normalized["managedBy"] = normalized.get("managed_by")
    if "xuiServerKey" in normalized and "xui_server_key" not in normalized:
        normalized["xui_server_key"] = normalized.get("xuiServerKey")
    if "xui_server_key" in normalized and "xuiServerKey" not in normalized:
        normalized["xuiServerKey"] = normalized.get("xui_server_key")
    if "xuiInboundId" in normalized and "xui_inbound_id" not in normalized:
        normalized["xui_inbound_id"] = normalized.get("xuiInboundId")
    if "xui_inbound_id" in normalized and "xuiInboundId" not in normalized:
        normalized["xuiInboundId"] = normalized.get("xui_inbound_id")
    if "credentialMode" in normalized and "credential_mode" not in normalized:
        normalized["credential_mode"] = normalized.get("credentialMode")
    if "credential_mode" in normalized and "credentialMode" not in normalized:
        normalized["credentialMode"] = normalized.get("credential_mode")
    if "accessMode" in normalized and "access_mode" not in normalized:
        normalized["access_mode"] = normalized.get("accessMode")
    if "access_mode" in normalized and "accessMode" not in normalized:
        normalized["accessMode"] = normalized.get("access_mode")
    if "uuidMode" in normalized and "uuid_mode" not in normalized:
        normalized["uuid_mode"] = normalized.get("uuidMode")
    if "uuid_mode" in normalized and "uuidMode" not in normalized:
        normalized["uuidMode"] = normalized.get("uuid_mode")

    xui_server_key = str(normalized.get("xui_server_key") or normalized.get("xuiServerKey") or "").strip()
    xui_inbound_id = normalized.get("xui_inbound_id") if normalized.get("xui_inbound_id") is not None else normalized.get("xuiInboundId")
    has_xui_identity = bool(str(xui_inbound_id or "").strip()) or bool(xui_server_key and xui_server_key != "default")
    managed_by = str(normalized.get("managed_by") or normalized.get("managedBy") or "").strip().lower()
    if has_xui_identity or managed_by in {"3x-ui", "3xui", "xui", "x-ui", "three-x-ui"}:
        normalized["managed_by"] = "3x-ui"
        normalized["managedBy"] = "3x-ui"
        normalized["xui_server_key"] = xui_server_key or "default"
        normalized["xuiServerKey"] = normalized["xui_server_key"]
        if str(xui_inbound_id or "").strip():
            try:
                inbound_int = int(float(str(xui_inbound_id).strip()))
            except (TypeError, ValueError):
                inbound_int = xui_inbound_id
            normalized["xui_inbound_id"] = inbound_int
            normalized["xuiInboundId"] = inbound_int
        if str(normalized.get("credential_mode") or normalized.get("credentialMode") or "").strip().lower() in {"", "per_user", "owned_per_user", "device", "device_uuid", "per_device"}:
            normalized["credential_mode"] = "per_device"
            normalized["credentialMode"] = "per_device"
            normalized["uuid_mode"] = "per_device"
            normalized["uuidMode"] = "per_device"
    normalized = _apply_anti_block_profile(_apply_admin_mobile_defaults(normalized))
    normalized = _invalidate_stale_live_probe(normalized)
    return _canonicalize_payload_metadata(normalized)


def _normalize_location_access_mode(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {
        "owned_per_user",
        "per_user",
        "per_device",
        "owned",
        "template",
        "template_per_user",
        "user_uuid",
        "device_uuid",
        "user-specific",
        "user_specific",
        "device-specific",
        "device_specific",
    }:
        return "owned_per_user"
    return "external_static"


def _extract_location_access_mode(row: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None) -> str:
    sources = []
    if isinstance(payload, dict):
        sources.extend([payload.get("access_mode"), payload.get("credential_mode")])
    if isinstance(row, dict):
        sources.extend([row.get("access_mode")])
        row_payload = row.get("vpn_payload")
        if isinstance(row_payload, dict):
            sources.extend([row_payload.get("access_mode"), row_payload.get("credential_mode")])
    for candidate in sources:
        normalized = _normalize_location_access_mode(candidate)
        if str(candidate or "").strip():
            return normalized
    return "external_static"


def _location_is_3xui_managed(payload: Optional[Dict[str, Any]]) -> bool:
    data = payload or {}
    managed_by = str(
        data.get("managed_by")
        or data.get("managedBy")
        or data.get("provider")
        or data.get("runtime_provider")
        or ""
    ).strip().lower()
    if managed_by in {"3x-ui", "3xui", "xui", "x-ui", "three-x-ui"}:
        return True
    return bool(data.get("xui_inbound_id") or data.get("xuiInboundId"))


def _apply_location_access_mode(payload: Dict[str, Any], access_mode: str) -> Dict[str, Any]:
    normalized = dict(payload or {})
    mode = _normalize_location_access_mode(access_mode)
    normalized["access_mode"] = mode

    requested_credential_mode = str(
        normalized.get("credential_mode")
        or normalized.get("credentialMode")
        or ""
    ).strip().lower()
    requested_uuid_mode = str(
        normalized.get("uuid_mode")
        or normalized.get("uuidMode")
        or ""
    ).strip().lower()

    if mode == "owned_per_user":
        # 3X-UI locations must stay per_device. The previous implementation
        # rewrote every owned_per_user location to credential_mode=per_user,
        # so the admin dropdown became blank and per-device UUID sync was not
        # obvious/safe in the UI. Preserve/force per_device for managed 3X-UI
        # templates and device-UUID locations.
        if (
            requested_credential_mode == "per_device"
            or requested_uuid_mode == "per_device"
            or _location_is_3xui_managed(normalized)
        ):
            normalized["credential_mode"] = "per_device"
            normalized["uuid_mode"] = "per_device"
        else:
            normalized["credential_mode"] = "per_user"
    else:
        normalized["credential_mode"] = "static"
        if requested_uuid_mode != "per_device":
            normalized["uuid_mode"] = "static"
    return normalized


def _location_requires_user_credential(row: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None) -> bool:
    return _extract_location_access_mode(row=row, payload=payload) == "owned_per_user"


def _placeholder_like_value(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    lowered = text.lower()
    placeholder_prefixes = ("paste_", "your_", "replace_", "todo", "changeme")
    if lowered.startswith(placeholder_prefixes):
        return True
    if "example.com" in lowered or "example.net" in lowered or "example.org" in lowered:
        return True
    if lowered in {"ru_provider_host", "uz_provider_host", "ru_provider_user", "uz_provider_user", "ru_provider_pass", "uz_provider_pass"}:
        return True
    return False


def _config_is_complete(payload: Dict[str, Any]) -> bool:
    normalized = _apply_admin_mobile_defaults(_normalize_vpn_payload_keys(payload))
    server = str(normalized.get("server") or "").strip()
    uuid = str(normalized.get("uuid") or "").strip()
    security = str(normalized.get("security") or "reality").strip().lower() or "reality"
    transport = str(normalized.get("transport") or normalized.get("network") or "tcp").strip().lower() or "tcp"
    sni = str(normalized.get("server_name") or normalized.get("sni") or "").strip()
    public_key = str(normalized.get("public_key") or normalized.get("publicKey") or "").strip()
    short_id = str(normalized.get("short_id") or normalized.get("shortId") or "").strip()
    service_name = str(normalized.get("service_name") or normalized.get("serviceName") or "").strip()
    path = str(normalized.get("path") or "").strip()
    dns_servers = normalized.get("dns_servers") or normalized.get("dnsServers") or []
    requires_runtime_uuid = _location_requires_user_credential(payload=normalized)
    try:
        port = int(normalized.get("port") or 0)
    except (TypeError, ValueError):
        port = 0

    if _placeholder_like_value(server) or port <= 0:
        return False
    if not requires_runtime_uuid and _placeholder_like_value(uuid):
        return False
    if not isinstance(dns_servers, list) or not [str(item).strip() for item in dns_servers if str(item).strip() and not _placeholder_like_value(item)]:
        return False
    if security == "reality":
        if _placeholder_like_value(public_key) or _placeholder_like_value(sni):
            return False
    if transport == "grpc" and _placeholder_like_value(service_name):
        return False
    if transport in {"ws", "websocket"} and _placeholder_like_value(path):
        return False
    return True


def _compose_vpn_payload_for_location(row: Dict[str, Any], *, requested_location_code: Optional[str] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    default_payload = settings.default_vpn_payload()
    if default_payload:
        payload.update(default_payload)
    overrides = settings.location_vpn_payloads().get(str(row.get("code") or "").strip())
    if overrides:
        payload.update(overrides)
    stored = row.get("vpn_payload")
    if isinstance(stored, dict) and stored:
        payload.update(stored)

    payload = _apply_admin_mobile_defaults(_normalize_vpn_payload_keys(payload))
    if not payload:
        return {}

    payload = _apply_location_access_mode(payload, _extract_location_access_mode(row, payload))
    canonical_code = str(requested_location_code or row.get("code") or payload.get("location_code") or payload.get("locationCode") or "").strip()
    canonical_name = str(row.get("name_en") or row.get("name_ru") or canonical_code or payload.get("remark") or payload.get("display_name") or "").strip()
    canonical_country = str(row.get("country_code") or payload.get("country_code") or payload.get("resolved_country_code") or "").strip().upper()
    if canonical_code:
        payload["location_code"] = canonical_code
        payload["locationCode"] = canonical_code
        payload["resolved_location_code"] = str(row.get("code") or canonical_code).strip()
    if canonical_country:
        payload["country_code"] = canonical_country
        payload["resolved_country_code"] = canonical_country
    if canonical_name:
        payload["remark"] = canonical_name
        payload["display_name"] = canonical_name
    payload = _invalidate_stale_live_probe(payload)
    return _canonicalize_payload_metadata(payload)


def _get_user_location_credential(cur: psycopg.Cursor, user_id: int, location_code: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM user_location_credentials
        WHERE user_id = %s AND location_code = %s
        LIMIT 1
        """,
        (int(user_id), str(location_code or "").strip()),
    )
    row = cur.fetchone()
    return dict(row) if row else None



def ensure_user_location_credential(user_id: int, location_code: str) -> Dict[str, Any]:
    resolved_code = str(location_code or "").strip()
    if not resolved_code:
        raise ValueError("location_code is required for per-user credential")
    with db() as conn:
        with conn.cursor() as cur:
            existing = _get_user_location_credential(cur, user_id, resolved_code)
            if existing:
                if str(existing.get("status") or "").strip().lower() != "active":
                    cur.execute(
                        """
                        UPDATE user_location_credentials
                        SET status = 'active', updated_at = NOW()
                        WHERE id = %s
                        RETURNING *
                        """,
                        (existing["id"],),
                    )
                    row = cur.fetchone()
                    conn.commit()
                    return dict(row) if row else existing
                return existing
            generated_uuid = str(uuid4())
            cur.execute(
                """
                INSERT INTO user_location_credentials (user_id, location_code, uuid, status)
                VALUES (%s, %s, %s, 'active')
                ON CONFLICT (user_id, location_code) DO UPDATE
                    SET updated_at = NOW()
                RETURNING *
                """,
                (int(user_id), resolved_code, generated_uuid),
            )
            row = cur.fetchone()
        conn.commit()
    if not row:
        raise ValueError("Failed to create per-user credential")
    return dict(row)




def _get_user_device_location_credential(cur: psycopg.Cursor, user_id: int, device_id: int, location_code: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM user_device_location_credentials
        WHERE user_id = %s AND device_id = %s AND location_code = %s
        LIMIT 1
        """,
        (int(user_id), int(device_id), str(location_code or "").strip()),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def ensure_user_device_location_credential(user_id: int, device_id: int, location_code: str) -> Dict[str, Any]:
    resolved_code = str(location_code or "").strip()
    if not resolved_code:
        raise ValueError("location_code is required for per-device credential")
    if int(device_id or 0) <= 0:
        return ensure_user_location_credential(user_id, resolved_code)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, is_active FROM devices WHERE id = %s AND user_id = %s LIMIT 1",
                (int(device_id), int(user_id)),
            )
            device_row = cur.fetchone()
            if not device_row or not bool(device_row.get("is_active")):
                raise PermissionError("Device is removed")
            existing = _get_user_device_location_credential(cur, user_id, device_id, resolved_code)
            if existing:
                if str(existing.get("status") or "").strip().lower() != "active":
                    cur.execute(
                        """
                        UPDATE user_device_location_credentials
                        SET status = 'active', updated_at = NOW()
                        WHERE id = %s
                        RETURNING *
                        """,
                        (existing["id"],),
                    )
                    row = cur.fetchone()
                    conn.commit()
                    return dict(row) if row else existing
                return existing
            generated_uuid = str(uuid4())
            cur.execute(
                """
                INSERT INTO user_device_location_credentials (user_id, device_id, location_code, uuid, status)
                VALUES (%s, %s, %s, %s, 'active')
                ON CONFLICT (device_id, location_code) DO UPDATE
                    SET updated_at = NOW()
                RETURNING *
                """,
                (int(user_id), int(device_id), resolved_code, generated_uuid),
            )
            row = cur.fetchone()
        conn.commit()
    if not row:
        raise ValueError("Failed to create per-device credential")
    return dict(row)


def revoke_device_location_credentials(user_id: int, device_id: int) -> None:
    if int(device_id or 0) <= 0:
        return
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE user_device_location_credentials
                SET status = 'revoked', updated_at = NOW()
                WHERE user_id = %s AND device_id = %s AND status <> 'revoked'
                """,
                (int(user_id), int(device_id)),
            )
        conn.commit()


def build_user_vpn_payload_for_location(user_id: int, row: Dict[str, Any], *, requested_location_code: Optional[str] = None, device_id: Optional[int] = None) -> Dict[str, Any]:
    payload = _compose_vpn_payload_for_location(row, requested_location_code=requested_location_code)
    if not payload:
        return {}
    force_per_device_uuid = bool(getattr(settings, "DEVICE_UUID_REQUIRED_FOR_SUBSCRIPTION", True)) and int(device_id or 0) > 0
    if not force_per_device_uuid and not _location_requires_user_credential(row=row, payload=payload):
        return payload
    resolved_location_code = str(row.get("code") or requested_location_code or "").strip()
    if int(device_id or 0) > 0:
        credential = ensure_user_device_location_credential(int(user_id), int(device_id or 0), resolved_location_code)
    else:
        credential = ensure_user_location_credential(int(user_id), resolved_location_code)
    payload = dict(payload)
    template_uuid = str(payload.get("uuid") or "").strip()
    payload["uuid"] = str(credential.get("uuid") or "").strip()
    payload["id"] = payload["uuid"]
    payload["credential_status"] = str(credential.get("status") or "active").strip() or "active"
    payload["credential_location_code"] = resolved_location_code
    if int(device_id or 0) > 0:
        payload["credential_mode"] = "per_device"
        payload["access_mode"] = "owned_per_user"
        payload["credential_device_id"] = int(device_id or 0)
    if template_uuid and template_uuid != payload["uuid"]:
        payload["template_uuid"] = template_uuid

    # Rebuild rawXrayConfig after replacing the template UUID with the runtime
    # per-device/per-user UUID. Some clients and diagnostics prefer the raw JSON
    # over the top-level VLESS fields; keeping both in sync prevents "half-old,
    # half-new" profiles after personal UUID migration.
    payload = _canonicalize_payload_metadata(payload)
    return payload


def _location_speed_rank(row: Dict[str, Any]) -> tuple:
    download = _normalize_optional_float(row.get("download_mbps")) or 0.0
    upload = _normalize_optional_float(row.get("upload_mbps")) or 0.0
    ping = _normalize_optional_int(row.get("ping_ms"))
    has_speed = 1 if (download > 0 or upload > 0 or (ping is not None and ping > 0)) else 0
    ping_score = 0 if ping is None else max(0, 10000 - ping)
    recommended = 1 if bool(row.get("is_recommended")) else 0
    reserve = 1 if bool(row.get("is_reserve")) else 0
    return (has_speed, download, upload, ping_score, recommended, -reserve, -(int(row.get("sort_order") or 9999)))


def _is_lte_location(row: Dict[str, Any]) -> bool:
    code = str(row.get("code") or "").strip().lower()
    if code.startswith("ru-lte") or code.startswith("russia-lte"):
        return True
    name_parts = [row.get("name_ru"), row.get("name_en")]
    joined = " ".join(str(part or "").strip().lower() for part in name_parts if str(part or "").strip())
    return "lte" in joined


def _speed_metrics_present(row: Dict[str, Any]) -> bool:
    return bool(
        (_normalize_optional_float(row.get("download_mbps")) or 0) > 0
        or (_normalize_optional_float(row.get("upload_mbps")) or 0) > 0
        or (_normalize_optional_int(row.get("ping_ms")) or 0) > 0
    )


def _speed_metrics_fresh(row: Dict[str, Any], *, max_age_minutes: int = VIRTUAL_LOCATION_FRESH_CHECK_MINUTES) -> bool:
    checked_at = _normalize_optional_timestamp(row.get("speed_checked_at"))
    if checked_at is None:
        return False
    age = datetime.now(timezone.utc) - checked_at.astimezone(timezone.utc)
    return age <= timedelta(minutes=max(1, int(max_age_minutes or 1)))


def _speed_ping_present(row: Dict[str, Any]) -> bool:
    ping = _normalize_optional_int(row.get("ping_ms"))
    return ping is not None and ping > 0


def _speed_ping_fresh(row: Dict[str, Any], *, max_age_minutes: int = VIRTUAL_LOCATION_FRESH_CHECK_MINUTES) -> bool:
    return _speed_ping_present(row) and _speed_metrics_fresh(row, max_age_minutes=max_age_minutes)


def _virtual_ping_sort_key(row: Dict[str, Any]) -> tuple:
    ping = _normalize_optional_int(row.get("ping_ms"))
    ping_rank = ping if ping is not None and ping > 0 else 10**9
    reserve_rank = 1 if bool(row.get("is_reserve")) else 0
    recommended_rank = 0 if bool(row.get("is_recommended")) else 1
    sort_order = int(row.get("sort_order") or 9999)
    name_rank = str(row.get("name_en") or row.get("name_ru") or row.get("code") or "").strip().lower()
    download = _normalize_optional_float(row.get("download_mbps")) or 0.0
    upload = _normalize_optional_float(row.get("upload_mbps")) or 0.0
    return (ping_rank, reserve_rank, recommended_rank, -download, -upload, sort_order, name_rank)


def _location_has_fresh_live_signal(row: Dict[str, Any], *, max_age_minutes: int = VIRTUAL_LOCATION_FRESH_CHECK_MINUTES) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status not in {"online", "reserve"}:
        return False
    payload = _compose_vpn_payload_for_location(dict(row))
    if not (payload and _config_is_complete(payload)):
        return False
    return _speed_ping_fresh(row, max_age_minutes=max_age_minutes)


def _location_payload_ready_for_publish(row: Dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status not in {"online", "reserve"}:
        return False
    payload = _compose_vpn_payload_for_location(dict(row))
    return bool(payload) and _config_is_complete(payload)


def _virtual_live_and_usable(row: Dict[str, Any]) -> bool:
    row_code = str(row.get("code") or "").strip()
    if not row_code or row_code in {"auto-fastest", "auto-reserve"}:
        return False
    return _location_has_fresh_live_signal(row)


def _virtual_payload_ready_and_usable(row: Dict[str, Any]) -> bool:
    row_code = str(row.get("code") or "").strip()
    if not row_code or row_code in {"auto-fastest", "auto-reserve"}:
        return False
    return _location_payload_ready_for_publish(row)


def _virtual_selection_tiers(rows: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    usable_live = [row for row in rows if _virtual_live_and_usable(row)]
    usable_ready = [row for row in rows if _virtual_payload_ready_and_usable(row)]
    fresh_ping = sorted(
        [row for row in usable_live if _speed_ping_fresh(row)],
        key=_virtual_ping_sort_key,
    )
    measured_ping = sorted(
        [row for row in usable_live if _speed_ping_present(row)],
        key=_virtual_ping_sort_key,
    )
    fresh_measured = sorted(
        [row for row in usable_live if _speed_metrics_present(row) and _speed_metrics_fresh(row)],
        key=_location_speed_rank,
        reverse=True,
    )
    all_live = sorted(usable_live, key=_location_speed_rank, reverse=True)
    tiers: List[List[Dict[str, Any]]] = [
        _dedupe_location_rows(fresh_ping),
        _dedupe_location_rows(measured_ping),
        _dedupe_location_rows(fresh_measured),
        _dedupe_location_rows(all_live),
    ]
    if bool(getattr(settings, "AUTO_VIRTUAL_ALLOW_READY_FALLBACK", False)):
        ready_with_ping = sorted(
            [row for row in usable_ready if _speed_ping_present(row)],
            key=_virtual_ping_sort_key,
        )
        ready_any = sorted(usable_ready, key=_location_speed_rank, reverse=True)
        tiers.extend([
            _dedupe_location_rows(ready_with_ping),
            _dedupe_location_rows(ready_any),
        ])
    return tiers


def _dedupe_location_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        code = str(row.get("code") or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        unique.append(row)
    return unique


def get_user_virtual_location_assignment(user_id: int, virtual_code: str) -> Optional[str]:
    code = str(virtual_code or "").strip()
    if not code:
        return None
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT concrete_code FROM virtual_location_assignments WHERE user_id = %s AND virtual_code = %s LIMIT 1",
                (user_id, code),
            )
            row = cur.fetchone()
    if not row:
        return None
    return str(row.get("concrete_code") or "").strip() or None


def upsert_user_virtual_location_assignment(user_id: int, virtual_code: str, concrete_code: str) -> None:
    virtual_clean = str(virtual_code or "").strip()
    concrete_clean = str(concrete_code or "").strip()
    if not virtual_clean or not concrete_clean:
        return
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO virtual_location_assignments (user_id, virtual_code, concrete_code)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, virtual_code) DO UPDATE SET
                    concrete_code = EXCLUDED.concrete_code,
                    updated_at = NOW()
                """,
                (user_id, virtual_clean, concrete_clean),
            )
        conn.commit()


def get_virtual_location_assignment_counts(concrete_codes: List[str]) -> Dict[str, int]:
    clean_codes = [str(code or "").strip() for code in concrete_codes if str(code or "").strip()]
    if not clean_codes:
        return {}
    placeholders = ", ".join(["%s"] * len(clean_codes))
    query = f"""
        SELECT vla.concrete_code, COUNT(DISTINCT vla.user_id) AS assigned_users
        FROM virtual_location_assignments vla
        JOIN users u ON u.id = vla.user_id
        JOIN subscriptions s ON s.user_id = vla.user_id
        WHERE vla.concrete_code IN ({placeholders})
          AND COALESCE(u.status, 'active') <> 'blocked'
          AND s.starts_at <= NOW()
          AND s.expires_at >= NOW()
        GROUP BY vla.concrete_code
    """
    counts: Dict[str, int] = {}
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(clean_codes))
            for row in cur.fetchall():
                counts[str(row.get("concrete_code") or "").strip()] = int(row.get("assigned_users") or 0)
    return counts




def record_vpn_location_session(*, user_id: int, device_fingerprint: str, location_codes: List[str], subscription_token: str = "", client: str = "", platform: str = "", device_name: str = "") -> None:
    """Record a recent real VPN subscription refresh for CONNECTED NOW."""
    uid = int(user_id or 0)
    fp = str(device_fingerprint or "").strip()
    codes = sorted({str(code or "").strip() for code in (location_codes or []) if str(code or "").strip()})
    if uid <= 0 or not fp or not codes:
        return
    with db() as conn:
        with conn.cursor() as cur:
            for code in codes:
                cur.execute(
                    """
                    INSERT INTO vpn_location_sessions (user_id, device_fingerprint, location_code, client, platform, device_name, subscription_token, last_seen_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (user_id, device_fingerprint, location_code) DO UPDATE SET
                        client = EXCLUDED.client, platform = EXCLUDED.platform, device_name = EXCLUDED.device_name,
                        subscription_token = EXCLUDED.subscription_token, last_seen_at = NOW(), updated_at = NOW()
                    """,
                    (uid, fp, code, str(client or "").strip()[:80], str(platform or "").strip()[:80], str(device_name or "").strip()[:160], str(subscription_token or "").strip()[:256]),
                )
            cur.execute(
                """
                DELETE FROM vpn_location_sessions
                WHERE user_id = %s AND device_fingerprint = %s AND NOT (location_code = ANY(%s))
                """,
                (uid, fp, codes),
            )
        conn.commit()


def get_location_connected_user_counts(location_codes: Optional[List[str]] = None, *, online_window_minutes: Optional[int] = None) -> Dict[str, int]:
    """Return honest real-time CONNECTED users per VPN location.

    Counted only when a non-blocked user with active access refreshed /sub/{token}
    from a real VPN client recently. Default online window: 30 minutes
    (env/settings VPN_CONNECTED_ONLINE_MINUTES).
    """
    runtime = get_runtime_settings_payload()
    access_mode = str(runtime.get("access_mode") or getattr(settings, "VPN_ACCESS_MODE", "paid") or "paid").strip().lower()
    free_mode = access_mode == "free"
    try:
        window_minutes = int(online_window_minutes or getattr(settings, "VPN_CONNECTED_ONLINE_MINUTES", 30) or 30)
    except Exception:
        window_minutes = 30
    window_minutes = max(1, min(window_minutes, 1440))

    clean_codes = [str(code or "").strip() for code in (location_codes or []) if str(code or "").strip()]
    params: List[Any] = [window_minutes]
    code_filter_sql = ""
    if clean_codes:
        placeholders = ", ".join(["%s"] * len(clean_codes))
        code_filter_sql = f"AND vls.location_code IN ({placeholders})"
        params.extend(clean_codes)

    access_sql = "TRUE" if free_mode else """
            EXISTS (
                SELECT 1 FROM subscriptions s
                WHERE s.user_id = u.id
                  AND COALESCE(s.status, 'active') = 'active'
                  AND s.starts_at <= NOW()
                  AND s.expires_at >= NOW()
            )
        """

    query = f"""
        SELECT vls.location_code AS code, COUNT(DISTINCT vls.user_id) AS connected_users
        FROM vpn_location_sessions vls
        JOIN users u ON u.id = vls.user_id
        WHERE COALESCE(u.status, 'active') <> 'blocked'
          AND {access_sql}
          AND vls.last_seen_at >= NOW() - (%s::int * INTERVAL '1 minute')
          {code_filter_sql}
        GROUP BY vls.location_code
    """

    counts: Dict[str, int] = {code: 0 for code in clean_codes}
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            for row in cur.fetchall():
                code = str(row.get("code") or "").strip()
                if code:
                    counts[code] = int(row.get("connected_users") or 0)
    return counts

def _virtual_role_order(code: str) -> List[bool]:
    # False -> primary/manual/LTE rows, True -> reserve rows.
    # Auto | Fastest should strongly prefer primary rows across *all* health
    # tiers before it ever falls back to reserve. Auto | Fastest Reserve should
    # do the opposite.
    if code == "auto-reserve":
        return [True, False]
    return [False, True]


def _virtual_location_candidates(code: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if code not in {"auto-fastest", "auto-reserve"}:
        return []

    tiers = _virtual_selection_tiers(rows)
    ordered: List[Dict[str, Any]] = []
    for want_reserve in _virtual_role_order(code):
        for tier in tiers:
            for row in tier:
                if bool(row.get("is_reserve")) == want_reserve:
                    ordered.append(row)
    return _dedupe_location_rows(ordered)


def _virtual_strict_candidate_pool(code: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tiers = _virtual_selection_tiers(rows)
    for want_reserve in _virtual_role_order(code):
        for tier in tiers:
            scoped = [row for row in tier if bool(row.get("is_reserve")) == want_reserve]
            if scoped:
                # Hard rule: do not mix weaker tiers into a stronger tier, and do
                # not let reserve rows win for Auto | Fastest while there is at
                # least one usable primary row in any stronger or weaker tier.
                return list(scoped[: max(1, VIRTUAL_LOCATION_POOL_SIZE)])
    return []


def _virtual_role_preferred_pool(code: str, pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not pool:
        return []

    non_reserve = [row for row in pool if not bool(row.get("is_reserve"))]
    reserve = [row for row in pool if bool(row.get("is_reserve"))]

    # User expectation:
    # - Auto | Fastest should first try real/manual/LTE locations and only fall
    #   back to reserve rows when there is no better primary candidate.
    # - Auto | Fastest Reserve should do the opposite and stay in the reserve
    #   pool whenever at least one usable reserve candidate exists.
    if code == "auto-fastest" and non_reserve:
        return list(non_reserve)
    if code == "auto-reserve" and reserve:
        return list(reserve)
    return list(pool)


def _virtual_sibling_code(code: str) -> Optional[str]:
    if code == "auto-fastest":
        return "auto-reserve"
    if code == "auto-reserve":
        return "auto-fastest"
    return None


def _virtual_row_health_band(row: Dict[str, Any]) -> int:
    if _speed_ping_fresh(row):
        return 0
    if _speed_ping_present(row):
        return 1
    if _speed_metrics_present(row) and _speed_metrics_fresh(row):
        return 2
    if _speed_metrics_present(row):
        return 3
    return 4


def _virtual_pool_best_quality(pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not pool:
        return []
    best_band = min(_virtual_row_health_band(row) for row in pool)
    filtered = [row for row in pool if _virtual_row_health_band(row) == best_band]
    ping_rows: List[Dict[str, Any]] = []
    for row in filtered:
        ping = _normalize_optional_int(row.get("ping_ms"))
        if ping is not None and ping > 0:
            ping_rows.append(row)
    if ping_rows:
        best_ping = min(_normalize_optional_int(row.get("ping_ms")) or 0 for row in ping_rows)
        max_drift = max(60, int(VIRTUAL_LOCATION_MAX_PING_BALANCE_DRIFT_MS or 180))
        near_best = [
            row for row in ping_rows
            if (_normalize_optional_int(row.get("ping_ms")) or 10**9) <= best_ping + max_drift
        ]
        if near_best:
            return near_best
        return ping_rows
    return filtered


def _prefer_primary_virtual_pool(pool: List[Dict[str, Any]], loads: Dict[str, int]) -> List[Dict[str, Any]]:
    quality_pool = _virtual_pool_best_quality(pool)
    if len(quality_pool) <= 3:
        return list(quality_pool)

    primary = list(quality_pool[:3])
    overflow = list(quality_pool[3:])
    if not overflow:
        return primary

    def row_load(row: Dict[str, Any]) -> int:
        return int(loads.get(str(row.get("code") or "").strip(), 0))

    best_primary_load = min(row_load(row) for row in primary)
    best_overflow_load = min(row_load(row) for row in overflow)
    if best_overflow_load + 1 < best_primary_load:
        return overflow
    return primary


def _virtual_row_load(row: Dict[str, Any], loads: Dict[str, int]) -> int:
    return int(loads.get(str(row.get("code") or "").strip(), 0))


def _virtual_row_not_overloaded(row: Dict[str, Any], loads: Dict[str, int], *, threshold: int = VIRTUAL_LOCATION_LOAD_IMBALANCE_THRESHOLD) -> bool:
    if not loads:
        return True
    min_load = min(int(value or 0) for value in loads.values()) if loads else 0
    return _virtual_row_load(row, loads) <= min_load + max(0, int(threshold or 0))


def _pick_balanced_virtual_candidate(pool: List[Dict[str, Any]], loads: Dict[str, int]) -> Optional[Dict[str, Any]]:
    if not pool:
        return None

    max_users = max(0, int(VIRTUAL_LOCATION_MAX_USERS_PER_SERVER or 0))
    available_pool = list(pool)
    if max_users > 0:
        not_full = [row for row in pool if _virtual_row_load(row, loads) < max_users]
        if not_full:
            available_pool = not_full

    threshold = max(0, int(VIRTUAL_LOCATION_LOAD_IMBALANCE_THRESHOLD or 0))
    min_load = min(_virtual_row_load(row, loads) for row in available_pool) if available_pool else 0
    for row in available_pool:
        if _virtual_row_load(row, loads) <= min_load + threshold:
            return row
    return min(
        enumerate(available_pool or pool),
        key=lambda item: (_virtual_row_load(item[1], loads), item[0]),
    )[1]


def reset_virtual_location_assignments_for_concrete_code(concrete_code: str) -> int:
    clean_code = str(concrete_code or '').strip()
    if not clean_code or clean_code in VIRTUAL_LOCATION_CODES:
        return 0
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM virtual_location_assignments WHERE concrete_code = %s', (clean_code,))
            deleted = int(cur.rowcount or 0)
        conn.commit()
    return deleted


def _virtual_assignment_reusable(assigned_row: Dict[str, Any], best_row: Optional[Dict[str, Any]], loads: Dict[str, int]) -> bool:
    # Sticky assignment rule:
    # keep the current concrete location as long as it is still a valid
    # auto-candidate. Do not reshuffle existing users just because another node
    # is slightly faster or less loaded. Reassignment should happen only when
    # the current node is no longer eligible for the virtual pool.
    return assigned_row is not None


def _pick_virtual_location(code: str, user_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    rows = list_locations(active_only=True)
    if not rows:
        return None

    ranked_candidates = _virtual_location_candidates(code, rows)
    if not ranked_candidates:
        return None

    # Main rule:
    # take only the top-N live candidates in speed order and balance new users
    # across that ordered pool. Existing users keep their current assignment as
    # long as it remains in the valid candidate pool.
    pool = ranked_candidates[: max(1, VIRTUAL_LOCATION_POOL_SIZE)]
    if not pool:
        return None
    if not user_id:
        return pool[0]

    sibling_code = _virtual_sibling_code(code)
    sibling_assigned_code = get_user_virtual_location_assignment(user_id, sibling_code) if sibling_code else None
    if sibling_assigned_code:
        sibling_filtered_pool = [
            row for row in pool
            if str(row.get("code") or "").strip() != sibling_assigned_code
        ]
        if sibling_filtered_pool:
            pool = sibling_filtered_pool
        if not pool:
            return None

    loads = get_virtual_location_assignment_counts([str(row.get("code") or "").strip() for row in pool])

    assigned_code = get_user_virtual_location_assignment(user_id, code)
    if assigned_code:
        sibling_collision = bool(sibling_assigned_code and assigned_code == sibling_assigned_code)
        if not sibling_collision:
            assigned_row = next((row for row in pool if str(row.get("code") or "").strip() == assigned_code), None)
            if assigned_row is not None and _virtual_assignment_reusable(assigned_row, pool[0] if pool else None, loads):
                return assigned_row

    selected = _pick_balanced_virtual_candidate(pool, loads) or pool[0]
    selected_code = str(selected.get("code") or "").strip()
    if selected_code:
        upsert_user_virtual_location_assignment(user_id, code, selected_code)
    return selected

def sync_locations_catalog() -> None:
    locations = _load_default_locations()
    if not locations:
        return

    default_codes = [item["code"] for item in locations]
    with db() as conn:
        with conn.cursor() as cur:
            for item in locations:
                data = dict(item)
                data["location_source"] = "catalog"
                data["is_deleted"] = bool(data.get("is_deleted", False))
                if not data.get("vpn_payload"):
                    data["vpn_payload"] = _compose_vpn_payload_for_location(data)
                data["download_mbps"] = _normalize_optional_float(data.get("download_mbps"))
                data["upload_mbps"] = _normalize_optional_float(data.get("upload_mbps"))
                data["ping_ms"] = _normalize_optional_int(data.get("ping_ms"))
                data["speed_checked_at"] = _normalize_optional_timestamp(data.get("speed_checked_at"))
                data["vpn_payload"] = Jsonb(data.get("vpn_payload") or {})
                cur.execute(
                    """
                    INSERT INTO locations (code, name_ru, name_en, country_code, is_active, is_recommended, is_reserve, status, sort_order, download_mbps, upload_mbps, ping_ms, speed_checked_at, vpn_payload, is_deleted, location_source)
                    VALUES (%(code)s, %(name_ru)s, %(name_en)s, %(country_code)s, %(is_active)s, %(is_recommended)s, %(is_reserve)s, %(status)s, %(sort_order)s, %(download_mbps)s, %(upload_mbps)s, %(ping_ms)s, %(speed_checked_at)s, %(vpn_payload)s, %(is_deleted)s, %(location_source)s)
                    ON CONFLICT (code) DO UPDATE SET
                        -- Preserve admin-edited catalog rows across restarts.
                        -- Bootstrap should only backfill missing defaults, not overwrite
                        -- mutable values changed from the admin panel.
                        name_ru = CASE
                            WHEN COALESCE(NULLIF(BTRIM(locations.name_ru), ''), NULL) IS NULL THEN EXCLUDED.name_ru
                            ELSE locations.name_ru
                        END,
                        name_en = CASE
                            WHEN COALESCE(NULLIF(BTRIM(locations.name_en), ''), NULL) IS NULL THEN EXCLUDED.name_en
                            ELSE locations.name_en
                        END,
                        country_code = CASE
                            WHEN COALESCE(NULLIF(BTRIM(locations.country_code), ''), NULL) IS NULL THEN EXCLUDED.country_code
                            ELSE locations.country_code
                        END,
                        is_active = locations.is_active,
                        is_recommended = locations.is_recommended,
                        is_reserve = locations.is_reserve,
                        status = CASE
                            WHEN COALESCE(NULLIF(BTRIM(locations.status), ''), NULL) IS NULL THEN EXCLUDED.status
                            ELSE locations.status
                        END,
                        sort_order = CASE
                            WHEN locations.sort_order IS NULL THEN EXCLUDED.sort_order
                            ELSE locations.sort_order
                        END,
                        download_mbps = CASE
                            WHEN locations.download_mbps IS NULL THEN EXCLUDED.download_mbps
                            ELSE locations.download_mbps
                        END,
                        upload_mbps = CASE
                            WHEN locations.upload_mbps IS NULL THEN EXCLUDED.upload_mbps
                            ELSE locations.upload_mbps
                        END,
                        ping_ms = CASE
                            WHEN locations.ping_ms IS NULL THEN EXCLUDED.ping_ms
                            ELSE locations.ping_ms
                        END,
                        speed_checked_at = CASE
                            WHEN locations.speed_checked_at IS NULL THEN EXCLUDED.speed_checked_at
                            ELSE locations.speed_checked_at
                        END,
                        vpn_payload = CASE
                            WHEN locations.vpn_payload = '{}'::jsonb THEN EXCLUDED.vpn_payload
                            ELSE locations.vpn_payload
                        END,
                        is_deleted = locations.is_deleted,
                        location_source = CASE
                            WHEN COALESCE(NULLIF(BTRIM(locations.location_source), ''), 'catalog') = 'admin' THEN 'admin'
                            ELSE 'catalog'
                        END,
                        updated_at = NOW()
                    """,
                    data,
                )

            placeholders = ", ".join(["%s"] * len(default_codes))
            cur.execute(
                f"""
                UPDATE locations
                SET is_active = FALSE,
                    is_recommended = FALSE,
                    is_reserve = FALSE,
                    status = CASE WHEN status = 'online' THEN 'offline' ELSE status END,
                    updated_at = NOW()
                WHERE is_deleted = FALSE
                  AND location_source = 'catalog'
                  AND code NOT IN ({placeholders})
                """,
                tuple(default_codes),
            )
        conn.commit()


def _normalize_location_source(value: Any) -> str:
    normalized = str(value or "catalog").strip().lower()
    return "admin" if normalized == "admin" else "catalog"


def _as_positive_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _normalize_device_limit_override(value: Any) -> Optional[int]:
    parsed = _as_positive_int(value)
    if parsed is None:
        return None
    # V2: per-user override must bypass the global default ceiling.
    # Global max remains a default/plan cap for regular users,
    # but selected users may explicitly receive a higher manual limit.
    return parsed


def _resolve_effective_device_limit(plan_limit: Any = None, user_override: Any = None) -> int:
    override_limit = _normalize_device_limit_override(user_override)
    if override_limit is not None:
        return override_limit
    plan_value = _as_positive_int(plan_limit)
    if plan_value is not None:
        return plan_value
    default_limit = _as_positive_int(settings.VPN_DEFAULT_DEVICE_LIMIT) or 1
    return default_limit



def _runtime_access_mode_payload(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    data = dict(payload or get_runtime_settings_payload() or {})
    access_mode = "free" if str(data.get("access_mode") or getattr(settings, "VPN_ACCESS_MODE", "paid")).strip().lower() == "free" else "paid"
    free_mode_device_limit = min(
        _coerce_runtime_int(data.get("free_mode_device_limit"), getattr(settings, "VPN_FREE_MODE_DEVICE_LIMIT", settings.VPN_DEFAULT_DEVICE_LIMIT), minimum=1),
        max(1, int(getattr(settings, "VPN_MAX_DEVICES_PER_ACCOUNT", settings.VPN_DEFAULT_DEVICE_LIMIT) or settings.VPN_DEFAULT_DEVICE_LIMIT or 1)),
    )
    paid_grace_hours = _coerce_runtime_int(data.get("paid_grace_hours"), getattr(settings, "VPN_PAID_GRACE_HOURS", 24), minimum=1)
    grace_started_at = _normalize_optional_timestamp(data.get("paid_grace_started_at"))
    return {
        "access_mode": access_mode,
        "free_mode_device_limit": free_mode_device_limit,
        "paid_grace_hours": paid_grace_hours,
        "paid_grace_started_at": grace_started_at,
    }


def _user_has_paid_grace(user: Optional[Dict[str, Any]], access_payload: Dict[str, Any]) -> bool:
    if not user or access_payload.get("access_mode") != "paid":
        return False
    grace_started_at = access_payload.get("paid_grace_started_at")
    if not grace_started_at:
        return False
    user_created_at = _normalize_optional_timestamp((user or {}).get("created_at"))
    if not user_created_at or user_created_at > grace_started_at:
        return False
    grace_hours = max(1, int(access_payload.get("paid_grace_hours") or 24))
    grace_until = grace_started_at + timedelta(hours=grace_hours)
    return datetime.now(timezone.utc) <= grace_until


def _resolve_user_access_state(
    user: Optional[Dict[str, Any]],
    latest_subscription: Optional[Dict[str, Any]],
    *,
    access_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = _runtime_access_mode_payload(access_payload)
    if not user or str((user or {}).get("status") or "").strip().lower() == "blocked":
        return {
            "is_active": False,
            "access_source": "blocked",
            "show_buy_button": payload.get("access_mode") != "free",
            "grace_active": False,
            "free_mode_active": payload.get("access_mode") == "free",
            "device_limit": 0,
            "access_mode": payload.get("access_mode"),
            "free_mode_device_limit": payload.get("free_mode_device_limit"),
            "paid_grace_hours": payload.get("paid_grace_hours"),
            "paid_grace_started_at": payload.get("paid_grace_started_at"),
        }
    paid_limit = _resolve_effective_device_limit((latest_subscription or {}).get("device_limit"), (user or {}).get("device_limit_override"))
    if latest_subscription and str(latest_subscription.get("status") or "").strip().lower() == "active":
        return {
            "is_active": True,
            "access_source": "paid_subscription",
            "show_buy_button": payload.get("access_mode") != "free",
            "grace_active": False,
            "free_mode_active": payload.get("access_mode") == "free",
            "device_limit": paid_limit,
            "access_mode": payload.get("access_mode"),
            "free_mode_device_limit": payload.get("free_mode_device_limit"),
            "paid_grace_hours": payload.get("paid_grace_hours"),
            "paid_grace_started_at": payload.get("paid_grace_started_at"),
        }
    grace_active = _user_has_paid_grace(user, payload)
    if payload.get("access_mode") == "free":
        return {
            "is_active": True,
            "access_source": "free_mode",
            "show_buy_button": False,
            "grace_active": False,
            "free_mode_active": True,
            "device_limit": _resolve_effective_device_limit(payload.get("free_mode_device_limit"), (user or {}).get("device_limit_override")),
            "access_mode": payload.get("access_mode"),
            "free_mode_device_limit": payload.get("free_mode_device_limit"),
            "paid_grace_hours": payload.get("paid_grace_hours"),
            "paid_grace_started_at": payload.get("paid_grace_started_at"),
        }
    if grace_active:
        return {
            "is_active": True,
            "access_source": "paid_grace",
            "show_buy_button": True,
            "grace_active": True,
            "free_mode_active": False,
            "device_limit": _resolve_effective_device_limit(payload.get("free_mode_device_limit"), (user or {}).get("device_limit_override")),
            "access_mode": payload.get("access_mode"),
            "free_mode_device_limit": payload.get("free_mode_device_limit"),
            "paid_grace_hours": payload.get("paid_grace_hours"),
            "paid_grace_started_at": payload.get("paid_grace_started_at"),
        }
    return {
        "is_active": False,
        "access_source": "inactive",
        "show_buy_button": True,
        "grace_active": False,
        "free_mode_active": False,
        "device_limit": paid_limit,
        "access_mode": payload.get("access_mode"),
        "free_mode_device_limit": payload.get("free_mode_device_limit"),
        "paid_grace_hours": payload.get("paid_grace_hours"),
        "paid_grace_started_at": payload.get("paid_grace_started_at"),
    }


def _normalize_user(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "id": row["id"],
        "telegram_id": row["telegram_id"],
        "username": row.get("username"),
        "first_name": row.get("first_name"),
        "last_name": row.get("last_name"),
        "language": row.get("language") or "ru",
        "status": row.get("status") or "active",
        "device_limit_override": _normalize_device_limit_override(row.get("device_limit_override")),
        "subscription_token": row.get("subscription_token"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def _generate_subscription_token() -> str:
    return secrets.token_urlsafe(32)


def ensure_user_subscription_token(user_id: int) -> str:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT subscription_token FROM users WHERE id = %s FOR UPDATE", (user_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("User not found")
            existing = str((row or {}).get("subscription_token") or "").strip()
            if existing:
                conn.commit()
                return existing

            for _ in range(5):
                token = _generate_subscription_token()
                cur.execute(
                    "UPDATE users SET subscription_token = %s, updated_at = NOW() WHERE id = %s RETURNING subscription_token",
                    (token, user_id),
                )
                updated = cur.fetchone()
                if updated and updated.get("subscription_token"):
                    conn.commit()
                    return str(updated["subscription_token"])
            raise RuntimeError("Failed to generate unique subscription token")


def get_user_by_active_auth_code(code: str) -> Optional[Dict[str, Any]]:
    normalized = (code or "").strip()
    if not normalized:
        return None
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.*
                FROM auth_codes ac
                JOIN users u ON u.id = ac.user_id
                WHERE ac.code = %s
                  AND ac.used_at IS NULL
                  AND ac.expires_at > NOW()
                LIMIT 1
                """,
                (normalized,),
            )
            row = cur.fetchone()
    return _normalize_user(row) if row else None


def get_user_by_subscription_token(subscription_token: str) -> Optional[Dict[str, Any]]:
    token = str(subscription_token or "").strip()
    if not token:
        return None
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE subscription_token = %s", (token,))
            return _normalize_user(cur.fetchone())


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            return _normalize_user(cur.fetchone())


def get_user_by_telegram_id(telegram_id: int) -> Optional[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
            return _normalize_user(cur.fetchone())


def upsert_telegram_user(payload: Dict[str, Any]) -> Dict[str, Any]:
    telegram_id = int(payload["telegram_id"])
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (telegram_id, username, first_name, last_name, language, status)
                VALUES (%s, %s, %s, %s, %s, COALESCE(%s, 'active'))
                ON CONFLICT (telegram_id) DO UPDATE SET
                    username = COALESCE(EXCLUDED.username, users.username),
                    first_name = COALESCE(EXCLUDED.first_name, users.first_name),
                    last_name = COALESCE(EXCLUDED.last_name, users.last_name),
                    language = CASE
                        WHEN users.language IS NULL OR users.language = '' THEN COALESCE(EXCLUDED.language, 'ru')
                        ELSE users.language
                    END,
                    updated_at = NOW()
                RETURNING *
                """,
                (
                    telegram_id,
                    payload.get("username"),
                    payload.get("first_name"),
                    payload.get("last_name"),
                    payload.get("language") or "ru",
                    payload.get("status"),
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return _normalize_user(row)


def set_user_language(user_id: int, language: str) -> Dict[str, Any]:
    language = "en" if language == "en" else "ru"
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET language = %s, updated_at = NOW() WHERE id = %s RETURNING *", (language, user_id))
            row = cur.fetchone()
        conn.commit()
    return _normalize_user(row)


def set_user_status_by_telegram(telegram_id: int, status: str, admin_name: str, note: str) -> Dict[str, Any]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET status = %s, updated_at = NOW() WHERE telegram_id = %s RETURNING *", (status, telegram_id))
            row = cur.fetchone()
            if not row:
                raise ValueError("User not found")
            cur.execute(
                "INSERT INTO admin_notes (user_id, admin_name, note) VALUES (%s, %s, %s)",
                (row["id"], admin_name, note),
            )
        conn.commit()
    return _normalize_user(row)


def get_active_plans() -> List[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM plans WHERE is_active = TRUE ORDER BY duration_days ASC, price_rub ASC")
            return [dict(row) for row in cur.fetchall()]


def get_all_plans() -> List[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM plans ORDER BY duration_days ASC, price_rub ASC")
            return [dict(row) for row in cur.fetchall()]


def get_plan_by_code(code: str) -> Optional[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM plans WHERE code = %s", (code,))
            row = cur.fetchone()
            return dict(row) if row else None


def refresh_subscription_statuses(user_id: Optional[int] = None) -> None:
    query = """
        UPDATE subscriptions
        SET status = CASE
            WHEN starts_at <= NOW() AND expires_at >= NOW() THEN 'active'
            WHEN starts_at > NOW() THEN 'pending'
            ELSE 'expired'
        END,
        updated_at = NOW()
    """
    args: Tuple[Any, ...] = tuple()
    if user_id is not None:
        query += " WHERE user_id = %s"
        args = (user_id,)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, args)
        conn.commit()


def _subscription_select(where_sql: str) -> str:
    return f"""
        SELECT s.*, p.code AS plan_code, p.name_ru AS plan_name_ru, p.name_en AS plan_name_en,
               p.name_ru, p.name_en, p.price_rub, p.duration_days, p.device_limit
        FROM subscriptions s
        JOIN plans p ON p.id = s.plan_id
        {where_sql}
        ORDER BY s.expires_at DESC
        LIMIT 1
    """



def get_current_subscription(user_id: int) -> Optional[Dict[str, Any]]:
    refresh_subscription_statuses(user_id)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(_subscription_select("WHERE s.user_id = %s AND s.status = 'active'"), (user_id,))
            row = cur.fetchone()
            return dict(row) if row else None



def _generate_device_subscription_token() -> str:
    return "dt_" + secrets.token_urlsafe(32).rstrip("=")


def _generate_subscription_client_id() -> str:
    return "cid-" + secrets.token_urlsafe(18).rstrip("=").replace("/", "-").replace(".", "-")[:36]


def _normalize_subscription_client_id(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    cleaned = ''.join(ch for ch in raw if ch.isalnum() or ch in {'-', '_', '.', ':', '@'})
    return cleaned[:120]


def _pending_device_fingerprint_for_token(token: str) -> str:
    normalized = str(token or "").strip()
    digest = hashlib.sha256(f"pending-device-token:{normalized}".encode("utf-8")).hexdigest()
    return f"pending:{digest}"


def _is_pending_device_fingerprint(value: Any) -> bool:
    return str(value or "").strip().lower().startswith("pending:")


def _pending_slot_ttl_hours() -> int:
    try:
        value = int(getattr(settings, "DEVICE_PENDING_SLOT_TTL_HOURS", 24) or 0)
    except Exception:
        value = 24
    return max(0, value)


def cleanup_expired_pending_device_slots(user_id: Optional[int] = None) -> int:
    """Release old pending device slots that never completed first import.

    A pending slot is created when the bot issues a one-device /sub/dt_... URL.
    It must count against the plan limit immediately, but it should not stay
    forever if the child/second phone never imports the subscription.
    """
    ttl_hours = _pending_slot_ttl_hours()
    if ttl_hours <= 0:
        return 0
    uid = int(user_id or 0) or None
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH expired AS (
                    SELECT d.id
                    FROM devices d
                    LEFT JOIN LATERAL (
                        SELECT dst.id, dst.created_at, dst.expires_at
                        FROM device_subscription_tokens dst
                        WHERE dst.user_id = d.user_id
                          AND dst.device_id = d.id
                          AND dst.is_active = TRUE
                        ORDER BY dst.created_at DESC, dst.id DESC
                        LIMIT 1
                    ) dst ON TRUE
                    WHERE d.is_active = TRUE
                      AND COALESCE(d.device_fingerprint, '') LIKE 'pending:%%'
                      AND (%s::integer IS NULL OR d.user_id = %s::integer)
                      AND (
                            (dst.expires_at IS NOT NULL AND dst.expires_at <= NOW())
                         OR (COALESCE(dst.created_at, d.created_at) <= NOW() - (%s * INTERVAL '1 hour'))
                      )
                )
                UPDATE devices d
                SET is_active = FALSE, last_seen_at = NOW()
                WHERE d.id IN (SELECT id FROM expired)
                RETURNING d.id
                """,
                (uid, uid, ttl_hours),
            )
            expired_ids = [int(row["id"]) for row in cur.fetchall()]
            if expired_ids:
                cur.execute(
                    """
                    UPDATE device_subscription_tokens
                    SET is_active = FALSE, updated_at = NOW()
                    WHERE device_id = ANY(%s)
                       OR (COALESCE(device_fingerprint, '') LIKE 'pending:%%'
                           AND (%s::integer IS NULL OR user_id = %s::integer)
                           AND (
                                (expires_at IS NOT NULL AND expires_at <= NOW())
                             OR (created_at <= NOW() - (%s * INTERVAL '1 hour'))
                           ))
                    """,
                    (expired_ids, uid, uid, ttl_hours),
                )
                cur.execute(
                    """
                    UPDATE user_device_location_credentials
                    SET status = 'revoked', updated_at = NOW()
                    WHERE device_id = ANY(%s) AND status <> 'revoked'
                    """,
                    (expired_ids,),
                )
        conn.commit()
    return len(expired_ids)


def _decorate_device_row(row: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(row or {})
    is_pending = _is_pending_device_fingerprint(item.get("device_fingerprint"))
    item["is_pending"] = bool(is_pending)
    if is_pending:
        item["device_status"] = "pending"
    elif bool(item.get("is_active", True)):
        item["device_status"] = "active"
    else:
        item["device_status"] = "inactive"
    return item


def create_device_subscription_token_for_user(
    user_id: int,
    *,
    platform: str = "",
    device_name: str = "",
    client: str = "",
    ttl_hours: Optional[int] = None,
) -> Dict[str, Any]:
    """Create a one-device subscription token and reserve one device slot immediately.

    The bot/open-app flow uses the user subscription token only as a bridge. This
    function creates an active pending device row first, then creates dt_... bound
    to that pending row. On the first real /sub/dt_... request the pending
    fingerprint is replaced by the real subscription-client fingerprint.

    Result: pressing "Connect/Add device" consumes exactly one slot, the token is
    not user-wide, and deleting the device disables that token too.
    """
    user = get_user_by_id(int(user_id))
    if not user:
        raise ValueError("User not found")
    if user.get("status") == "blocked":
        raise PermissionError("User is blocked")
    cleanup_expired_pending_device_slots(int(user_id))
    access_view = get_user_subscription_view(int(user_id))
    if not bool(access_view.get("is_active")):
        raise PermissionError("Active subscription required")
    device_limit = max(1, int(access_view.get("device_limit") or 1))
    expires_at = None
    if ttl_hours is not None and int(ttl_hours or 0) > 0:
        expires_at = now_utc() + timedelta(hours=int(ttl_hours))

    normalized_platform = str(platform or "client").strip()[:80] or "client"
    normalized_device_name = str(device_name or "VPN client").strip()[:160] or "VPN client"
    normalized_client = str(client or "").strip()[:80]

    # Reuse an existing unbound pending token for the same platform/client. This
    # prevents repeated clicks on “Open connection” from consuming all slots.
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dst.*, d.id AS pending_device_id
                FROM device_subscription_tokens dst
                JOIN devices d ON d.id = dst.device_id AND d.user_id = dst.user_id
                WHERE dst.user_id = %s
                  AND dst.is_active = TRUE
                  AND d.is_active = TRUE
                  AND COALESCE(dst.device_fingerprint, '') LIKE 'pending:%%'
                  AND COALESCE(dst.platform, '') = %s
                  AND COALESCE(dst.client, '') = %s
                  AND (dst.expires_at IS NULL OR dst.expires_at > NOW())
                ORDER BY dst.created_at DESC
                LIMIT 1
                """,
                (int(user_id), normalized_platform, normalized_client),
            )
            pending = cur.fetchone()
            if pending:
                return dict(pending)

    devices_used = int(access_view.get("devices_used") or 0)
    if devices_used >= device_limit:
        raise PermissionError(f"Device limit reached ({devices_used}/{device_limit})")

    for _ in range(6):
        token = _generate_device_subscription_token()
        client_id = _generate_subscription_client_id()
        pending_fingerprint = _pending_device_fingerprint_for_token(token)
        try:
            with db() as conn:
                with conn.cursor() as cur:
                    # Re-check inside the transaction so quick repeated button taps
                    # cannot reserve more active slots than the plan allows.
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM devices WHERE user_id = %s AND is_active = TRUE",
                        (int(user_id),),
                    )
                    count_row = cur.fetchone() or {"cnt": 0}
                    current_used = int(count_row.get("cnt") or 0)
                    if current_used >= device_limit:
                        raise PermissionError(f"Device limit reached ({current_used}/{device_limit})")
                    cur.execute(
                        """
                        INSERT INTO devices (user_id, platform, device_name, device_fingerprint)
                        VALUES (%s, %s, %s, %s)
                        RETURNING *
                        """,
                        (int(user_id), normalized_platform, normalized_device_name, pending_fingerprint),
                    )
                    device_row = cur.fetchone()
                    cur.execute(
                        """
                        INSERT INTO device_subscription_tokens
                            (user_id, device_id, token, device_fingerprint, platform, device_name, client, client_id, expires_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                        """,
                        (
                            int(user_id),
                            int(device_row["id"]),
                            token,
                            pending_fingerprint,
                            normalized_platform,
                            normalized_device_name,
                            normalized_client,
                            client_id,
                            expires_at,
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
                result = dict(row)
                result["device"] = dict(device_row)
                return result
        except psycopg.errors.UniqueViolation:
            continue
    raise ValueError("Failed to create device subscription token")


def get_device_subscription_token(subscription_token: str) -> Optional[Dict[str, Any]]:
    token = str(subscription_token or "").strip()
    if not token:
        return None
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dst.*, u.telegram_id, u.username, u.first_name, u.last_name, u.language, u.status AS user_status,
                       u.device_limit_override, u.subscription_token AS user_subscription_token
                FROM device_subscription_tokens dst
                JOIN users u ON u.id = dst.user_id
                WHERE dst.token = %s
                LIMIT 1
                """,
                (token,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def user_from_device_subscription_token(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "id": row.get("user_id"),
        "telegram_id": row.get("telegram_id"),
        "username": row.get("username"),
        "first_name": row.get("first_name"),
        "last_name": row.get("last_name"),
        "language": row.get("language") or "ru",
        "status": row.get("user_status") or "active",
        "device_limit_override": row.get("device_limit_override"),
        "subscription_token": row.get("user_subscription_token"),
    }


def bind_device_subscription_token(
    subscription_token: str,
    *,
    platform: str,
    device_name: str,
    device_fingerprint: str,
    client: str = "",
    client_id: str = "",
) -> Dict[str, Any]:
    token = str(subscription_token or "").strip()
    fingerprint = str(device_fingerprint or "").strip()
    request_client_id = _normalize_subscription_client_id(client_id)
    if not token or not fingerprint:
        return {"allowed": False, "reason": "device_fingerprint_required", "detail": "Device fingerprint is required"}

    normalized_platform = str(platform or "client").strip()[:80] or "client"
    normalized_device_name = str(device_name or "VPN client").strip()[:160] or "VPN client"
    normalized_client = str(client or "").strip()[:80]

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dst.*, u.status AS user_status
                FROM device_subscription_tokens dst
                JOIN users u ON u.id = dst.user_id
                WHERE dst.token = %s
                FOR UPDATE
                """,
                (token,),
            )
            raw_token_row = cur.fetchone()
            if not raw_token_row:
                return {"allowed": False, "reason": "token_not_found", "detail": "Device subscription token not found"}
            token_row = dict(raw_token_row)
            user_id = int(token_row.get("user_id") or 0)
            token_id = int(token_row.get("id") or 0)
            token_device_id = int(token_row.get("device_id") or 0)

            if not bool(token_row.get("is_active")):
                return {"allowed": False, "reason": "token_revoked", "detail": "Device subscription token is revoked", "user_id": user_id}
            expires_at = token_row.get("expires_at")
            if expires_at:
                exp = expires_at if isinstance(expires_at, datetime) else _normalize_optional_timestamp(expires_at)
                if exp and exp.astimezone(timezone.utc) < now_utc():
                    cur.execute("UPDATE device_subscription_tokens SET is_active = FALSE, updated_at = NOW() WHERE id = %s", (token_id,))
                    conn.commit()
                    return {"allowed": False, "reason": "token_expired", "detail": "Device subscription token expired", "user_id": user_id}
            if str(token_row.get("user_status") or "").strip().lower() == "blocked":
                return {"allowed": False, "reason": "user_blocked", "detail": "Access blocked", "user_id": user_id}

            expected_client_id = _normalize_subscription_client_id(token_row.get("client_id"))
            if expected_client_id and request_client_id != expected_client_id:
                return {
                    "allowed": False,
                    "reason": "token_bound_to_other_device",
                    "detail": "This device token must be used with its original subcid/client_id",
                    "user_id": user_id,
                    "known_device": False,
                }

            bound_fp = str(token_row.get("device_fingerprint") or "").strip()
            pending_bound = _is_pending_device_fingerprint(bound_fp)

            if token_device_id > 0:
                cur.execute("SELECT * FROM devices WHERE user_id = %s AND id = %s LIMIT 1", (user_id, token_device_id))
                linked_device = cur.fetchone()
                if linked_device and not bool(linked_device.get("is_active")):
                    cur.execute("UPDATE device_subscription_tokens SET is_active = FALSE, updated_at = NOW() WHERE id = %s", (token_id,))
                    conn.commit()
                    return {"allowed": False, "reason": "device_removed", "detail": "Device was removed", "user_id": user_id, "known_device": False}

            if bound_fp and not pending_bound and bound_fp != fingerprint:
                return {
                    "allowed": False,
                    "reason": "token_bound_to_other_device",
                    "detail": "This subscription token is already bound to another device",
                    "user_id": user_id,
                    "known_device": False,
                }

            if bound_fp == fingerprint and not pending_bound:
                cur.execute(
                    """
                    SELECT * FROM devices
                    WHERE user_id = %s AND device_fingerprint = %s
                    ORDER BY id DESC LIMIT 1
                    """,
                    (user_id, fingerprint),
                )
                device_row = cur.fetchone()
                if device_row and not bool(device_row.get("is_active")):
                    cur.execute("UPDATE device_subscription_tokens SET is_active = FALSE, updated_at = NOW() WHERE id = %s", (token_id,))
                    conn.commit()
                    return {"allowed": False, "reason": "device_removed", "detail": "Device was removed", "user_id": user_id, "known_device": False}
                cur.execute(
                    """
                    UPDATE device_subscription_tokens
                    SET platform = COALESCE(NULLIF(%s, ''), platform),
                        device_name = COALESCE(NULLIF(%s, ''), device_name),
                        client = COALESCE(NULLIF(%s, ''), client),
                        client_id = COALESCE(NULLIF(%s, ''), client_id),
                        last_seen_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (normalized_platform, normalized_device_name, normalized_client, request_client_id, token_id),
                )
                updated = cur.fetchone()
                if device_row:
                    cur.execute(
                        "UPDATE devices SET platform = %s, device_name = %s, last_seen_at = NOW() WHERE id = %s",
                        (normalized_platform, normalized_device_name, device_row["id"]),
                    )
                conn.commit()
                return {"allowed": True, "reason": "ok", "detail": "OK", "user_id": user_id, "known_device": True, "device_token": dict(updated or token_row), "device": dict(device_row) if device_row else None}

            # New dt_ token: replace the pending fingerprint by the real one on
            # the already-reserved device slot. If the same real device already
            # exists, reuse it and release the pending slot so one phone does not
            # accidentally consume two slots.
            cur.execute(
                """
                SELECT * FROM devices
                WHERE user_id = %s AND device_fingerprint = %s AND is_active = TRUE
                ORDER BY id DESC LIMIT 1
                """,
                (user_id, fingerprint),
            )
            existing_real_device = cur.fetchone()
            if existing_real_device:
                real_device_id = int(existing_real_device["id"])
                if token_device_id and token_device_id != real_device_id:
                    cur.execute("UPDATE devices SET is_active = FALSE, last_seen_at = NOW() WHERE user_id = %s AND id = %s", (user_id, token_device_id))
                cur.execute(
                    """
                    UPDATE device_subscription_tokens
                    SET device_id = %s,
                        device_fingerprint = %s,
                        platform = %s,
                        device_name = %s,
                        client = %s,
                        client_id = COALESCE(NULLIF(%s, ''), client_id),
                        first_seen_at = COALESCE(first_seen_at, NOW()),
                        last_seen_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (real_device_id, fingerprint, normalized_platform, normalized_device_name, normalized_client, request_client_id, token_id),
                )
                updated = cur.fetchone()
                cur.execute(
                    "UPDATE devices SET platform = %s, device_name = %s, last_seen_at = NOW() WHERE id = %s",
                    (normalized_platform, normalized_device_name, real_device_id),
                )
                conn.commit()
                return {"allowed": True, "reason": "ok", "detail": "OK", "user_id": user_id, "known_device": True, "device": dict(existing_real_device), "device_token": dict(updated)}

            if token_device_id <= 0:
                # Legacy token row without a reserved slot: keep compatibility but
                # still enforce the plan limit before creating a device.
                try:
                    conn.commit()
                    device = _upsert_device_record(
                        user_id,
                        normalized_platform,
                        normalized_device_name,
                        fingerprint,
                        enforce_limit=True,
                    )
                except PermissionError as exc:
                    view = get_user_subscription_view(user_id)
                    return {
                        "allowed": False,
                        "reason": "device_limit_reached" if "limit" in str(exc).lower() else "device_bind_failed",
                        "detail": str(exc),
                        "user_id": user_id,
                        "devices_used": int(view.get("devices_used") or 0),
                        "device_limit": int(view.get("device_limit") or 0),
                        "known_device": False,
                    }
                except Exception as exc:
                    return {"allowed": False, "reason": "device_bind_failed", "detail": str(exc), "user_id": user_id, "known_device": False}
                with db() as conn2:
                    with conn2.cursor() as cur2:
                        cur2.execute(
                            """
                            UPDATE device_subscription_tokens
                            SET device_id = %s, device_fingerprint = %s, platform = %s, device_name = %s,
                                client = %s, client_id = COALESCE(NULLIF(%s, ''), client_id),
                                first_seen_at = COALESCE(first_seen_at, NOW()), last_seen_at = NOW(), updated_at = NOW()
                            WHERE token = %s AND (device_fingerprint IS NULL OR device_fingerprint = '')
                            RETURNING *
                            """,
                            (int(device["id"]), fingerprint, normalized_platform, normalized_device_name, normalized_client, request_client_id, token),
                        )
                        updated = cur2.fetchone()
                    conn2.commit()
                if not updated:
                    return {"allowed": False, "reason": "token_bind_race", "detail": "Token was bound by another request", "user_id": user_id, "known_device": False}
                return {"allowed": True, "reason": "ok", "detail": "OK", "user_id": user_id, "known_device": False, "device": device, "device_token": dict(updated)}

            # Normal new pending slot path.
            try:
                cur.execute(
                    """
                    UPDATE devices
                    SET device_fingerprint = %s,
                        platform = %s,
                        device_name = %s,
                        is_active = TRUE,
                        last_seen_at = NOW()
                    WHERE user_id = %s AND id = %s AND is_active = TRUE
                    RETURNING *
                    """,
                    (fingerprint, normalized_platform, normalized_device_name, user_id, token_device_id),
                )
                device = cur.fetchone()
            except psycopg.errors.UniqueViolation:
                conn.rollback()
                return {"allowed": False, "reason": "token_bind_race", "detail": "Device fingerprint was already bound", "user_id": user_id, "known_device": False}
            if not device:
                conn.commit()
                return {"allowed": False, "reason": "device_removed", "detail": "Device was removed", "user_id": user_id, "known_device": False}
            cur.execute(
                """
                UPDATE device_subscription_tokens
                SET device_id = %s,
                    device_fingerprint = %s,
                    platform = %s,
                    device_name = %s,
                    client = %s,
                    client_id = COALESCE(NULLIF(%s, ''), client_id),
                    first_seen_at = COALESCE(first_seen_at, NOW()),
                    last_seen_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s AND (device_fingerprint = %s OR device_fingerprint IS NULL OR device_fingerprint = '')
                RETURNING *
                """,
                (int(device["id"]), fingerprint, normalized_platform, normalized_device_name, normalized_client, request_client_id, token_id, bound_fp),
            )
            updated = cur.fetchone()
            if not updated:
                conn.commit()
                return {"allowed": False, "reason": "token_bind_race", "detail": "Token was bound by another request", "user_id": user_id, "known_device": False}
            conn.commit()
            return {"allowed": True, "reason": "ok", "detail": "OK", "user_id": user_id, "known_device": False, "device": dict(device), "device_token": dict(updated)}


def touch_device_subscription_token(subscription_token: str, device_fingerprint: str) -> None:
    token = str(subscription_token or "").strip()
    fingerprint = str(device_fingerprint or "").strip()
    if not token or not fingerprint:
        return
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE device_subscription_tokens
                SET last_seen_at = NOW(), updated_at = NOW()
                WHERE token = %s AND device_fingerprint = %s AND is_active = TRUE
                """,
                (token, fingerprint),
            )
        conn.commit()

def get_subscription_device_gate_by_token(subscription_token: str, device_fingerprint: str) -> Optional[Dict[str, Any]]:
    token = str(subscription_token or "").strip()
    fingerprint = str(device_fingerprint or "").strip()
    if not token:
        return None
    user = get_user_by_subscription_token(token)
    if not user:
        return None
    removed_device = None
    if fingerprint:
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM devices WHERE user_id = %s AND device_fingerprint = %s ORDER BY id DESC LIMIT 1",
                    (int(user["id"]), fingerprint),
                )
                removed_device = cur.fetchone()
        if removed_device and not bool(removed_device.get("is_active")):
            devices = get_user_devices(int(user["id"]))
            subscription = get_current_subscription(int(user["id"]))
            access_state = _resolve_user_access_state(user, subscription)
            allowed_limit = int(access_state.get("device_limit") or 0)
            return {
                "allowed": False,
                "reason": "device_removed",
                "detail": "Device was removed",
                "user_id": int(user["id"]),
                "devices_used": len(devices),
                "device_limit": allowed_limit,
                "known_device": False,
            }
    if user.get("status") == "blocked":
        return {
            "allowed": False,
            "reason": "user_blocked",
            "detail": "Access blocked",
            "user_id": int(user["id"]),
            "devices_used": 0,
            "device_limit": 0,
            "known_device": False,
        }
    subscription = get_current_subscription(int(user["id"]))
    access_state = _resolve_user_access_state(user, subscription)
    if not access_state.get("is_active"):
        return {
            "allowed": False,
            "reason": "subscription_inactive",
            "detail": "Active subscription required",
            "user_id": int(user["id"]),
            "devices_used": 0,
            "device_limit": 0,
            "known_device": False,
        }
    allowed_limit = int(access_state.get("device_limit") or 0)
    devices = get_user_devices(int(user["id"]))
    known_device = bool(fingerprint) and any(str(item.get("device_fingerprint") or "") == fingerprint for item in devices)
    return {
        "allowed": known_device or len(devices) < allowed_limit,
        "reason": "ok" if (known_device or len(devices) < allowed_limit) else "device_limit_reached",
        "detail": "OK" if (known_device or len(devices) < allowed_limit) else f"Device limit reached ({len(devices)}/{allowed_limit})",
        "user_id": int(user["id"]),
        "devices_used": len(devices),
        "device_limit": allowed_limit,
        "known_device": known_device,
    }


def get_latest_subscription(user_id: int) -> Optional[Dict[str, Any]]:
    refresh_subscription_statuses(user_id)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(_subscription_select("WHERE s.user_id = %s"), (user_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def get_user_devices(user_id: int) -> List[Dict[str, Any]]:
    cleanup_expired_pending_device_slots(int(user_id))
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    d.*,
                    pending_token.created_at AS pending_created_at,
                    pending_token.expires_at AS pending_expires_at
                FROM devices d
                LEFT JOIN LATERAL (
                    SELECT dst.created_at, dst.expires_at
                    FROM device_subscription_tokens dst
                    WHERE dst.user_id = d.user_id
                      AND dst.device_id = d.id
                      AND dst.is_active = TRUE
                    ORDER BY dst.created_at DESC, dst.id DESC
                    LIMIT 1
                ) pending_token ON TRUE
                WHERE d.user_id = %s AND d.is_active = TRUE
                ORDER BY
                    CASE WHEN COALESCE(d.device_fingerprint, '') LIKE 'pending:%%' THEN 1 ELSE 0 END DESC,
                    d.last_seen_at DESC,
                    d.created_at DESC
                """,
                (int(user_id),),
            )
            return [_decorate_device_row(dict(row)) for row in cur.fetchall()]


def _get_user_subscription_view_with_conn(conn: psycopg.Connection, user_id: int) -> Dict[str, Any]:
    refresh_subscription_statuses(user_id)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        user_row = cur.fetchone()
        user = _normalize_user(user_row)

        cur.execute(_subscription_select("WHERE s.user_id = %s AND s.status = 'active'"), (user_id,))
        subscription_row = cur.fetchone()
        subscription = dict(subscription_row) if subscription_row else None

        latest = subscription
        if latest is None:
            cur.execute(_subscription_select("WHERE s.user_id = %s"), (user_id,))
            latest_row = cur.fetchone()
            latest = dict(latest_row) if latest_row else None

        # Pending slots are cleaned/decorated by get_user_devices(). They count
        # as used until imported or deleted/expired.
        devices = get_user_devices(int(user_id))

    access_state = _resolve_user_access_state(user, subscription)
    subscription_token = ensure_user_subscription_token(user_id) if user and access_state.get("is_active") else None
    return {
        "subscription": latest,
        "is_active": bool(access_state.get("is_active")),
        "devices": devices,
        "devices_used": len(devices),
        "device_limit": int(access_state.get("device_limit") or 0),
        "device_limit_override": (user or {}).get("device_limit_override"),
        "subscription_token": subscription_token,
        "access_mode": access_state.get("access_mode"),
        "access_source": access_state.get("access_source"),
        "free_mode_active": bool(access_state.get("free_mode_active")),
        "grace_active": bool(access_state.get("grace_active")),
        "show_buy_button": bool(access_state.get("show_buy_button")),
        "free_mode_device_limit": int(access_state.get("free_mode_device_limit") or 0),
        "paid_grace_hours": int(access_state.get("paid_grace_hours") or 0),
        "paid_grace_started_at": access_state.get("paid_grace_started_at"),
    }


def get_user_subscription_view(user_id: int) -> Dict[str, Any]:
    with db() as conn:
        return _get_user_subscription_view_with_conn(conn, user_id)


def _device_platform_family(platform: Any) -> str:
    normalized = str(platform or "").strip().lower()
    if normalized in {"android", "ios"}:
        return "mobile"
    if normalized in {"windows", "macos", "linux"}:
        return "desktop"
    if normalized == "client":
        return "generic"
    return normalized or "generic"



def _is_subscription_tracking_fingerprint(value: Any) -> bool:
    raw = str(value or "").strip().lower()
    return bool(raw) and bool(re.fullmatch(r"[0-9a-f]{64}", raw))



def _prefer_device_candidate(items: List[Dict[str, Any]], platform: str, device_name: str) -> Optional[Dict[str, Any]]:
    if not items:
        return None
    normalized_platform = str(platform or "").strip().lower()
    normalized_name = str(device_name or "").strip()

    def sort_key(item: Dict[str, Any]) -> tuple:
        item_platform = str(item.get("platform") or "").strip().lower()
        item_name = str(item.get("device_name") or "").strip()
        seen_at = _normalize_optional_timestamp(item.get("last_seen_at")) or _normalize_optional_timestamp(item.get("created_at")) or datetime.fromtimestamp(0, tz=timezone.utc)
        fingerprint = str(item.get("device_fingerprint") or "").strip()
        return (
            1 if item_platform == normalized_platform else 0,
            1 if item_name == normalized_name else 0,
            1 if not _is_subscription_tracking_fingerprint(fingerprint) else 0,
            1 if fingerprint else 0,
            seen_at,
            int(item.get("id") or 0),
        )

    return max(items, key=sort_key)



def _select_tracking_alias_match_device(existing: List[Dict[str, Any]], platform: str, device_name: str, incoming_fingerprint: str) -> Optional[Dict[str, Any]]:
    if not _is_subscription_tracking_fingerprint(incoming_fingerprint):
        return None

    normalized_platform = str(platform or "").strip().lower()
    normalized_name = str(device_name or "").strip()
    platform_family = _device_platform_family(normalized_platform)
    client_family = _device_client_family(normalized_name)

    exact_matches = [
        item for item in existing
        if str(item.get("platform") or "").strip().lower() == normalized_platform
        and str(item.get("device_name") or "").strip() == normalized_name
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]

    strong_exact_matches = [
        item for item in exact_matches
        if not _is_subscription_tracking_fingerprint(item.get("device_fingerprint"))
    ]
    if len(strong_exact_matches) == 1:
        return strong_exact_matches[0]

    same_platform = [
        item for item in existing
        if str(item.get("platform") or "").strip().lower() == normalized_platform
    ]
    strong_same_platform = [
        item for item in same_platform
        if not _is_subscription_tracking_fingerprint(item.get("device_fingerprint"))
    ]

    if platform_family == "desktop":
        if len(same_platform) == 1:
            return same_platform[0]
        if len(strong_same_platform) == 1:
            return strong_same_platform[0]
    else:
        if len(strong_same_platform) == 1 and len(same_platform) == 1:
            return strong_same_platform[0]

    same_family = []
    for item in existing:
        item_platform = str(item.get("platform") or "").strip().lower()
        item_name = str(item.get("device_name") or "").strip()
        item_family = _device_platform_family(item_platform)
        item_client_family = _device_client_family(item_name)
        compatible_family = item_family == platform_family
        compatible_client = client_family == item_client_family or "generic" in {client_family, item_client_family}
        if compatible_family and compatible_client:
            same_family.append(item)
    if len(same_family) == 1:
        return same_family[0]

    if len(existing) == 1:
        only_item = existing[0]
        only_platform = str(only_item.get("platform") or "").strip().lower()
        only_name = str(only_item.get("device_name") or "").strip()
        only_family = _device_platform_family(only_platform)
        only_client_family = _device_client_family(only_name)
        compatible_family = only_family == platform_family or "generic" in {only_family, platform_family}
        compatible_client = client_family == only_client_family or "generic" in {client_family, only_client_family}
        if compatible_family and compatible_client:
            return only_item

    return None



def _device_client_family(device_name: Any) -> str:
    normalized = str(device_name or "").strip().lower()
    if "v2raytun" in normalized:
        return "v2raytun"
    if "hiddify" in normalized:
        return "hiddify"
    if "happ" in normalized:
        return "happ"
    if "nekobox" in normalized:
        return "nekobox"
    if "nekoray" in normalized:
        return "nekoray"
    if "sing-box" in normalized or "singbox" in normalized:
        return "sing-box"
    if "vpn client" in normalized:
        return "generic"
    return normalized or "generic"



def _device_slot_matches_target(item: Dict[str, Any], platform: str, device_name: str) -> bool:
    normalized_platform = str(platform or '').strip().lower()
    normalized_name = str(device_name or '').strip()
    platform_family = _device_platform_family(normalized_platform)
    client_family = _device_client_family(normalized_name)

    item_platform = str(item.get('platform') or '').strip().lower()
    item_name = str(item.get('device_name') or '').strip()
    item_platform_family = _device_platform_family(item_platform)
    item_client_family = _device_client_family(item_name)

    compatible_platform = item_platform == normalized_platform or item_platform_family == platform_family
    compatible_client = item_client_family == client_family or 'generic' in {item_client_family, client_family}
    return compatible_platform and compatible_client


def _collapse_duplicate_device_slots(user_id: int, platform: str, device_name: str) -> None:
    active_devices = get_user_devices(user_id)
    matching = [item for item in active_devices if _device_slot_matches_target(item, platform, device_name)]
    if len(matching) <= 1:
        return

    normalized_platform = str(platform or '').strip().lower()
    normalized_name = str(device_name or '').strip()

    def sort_key(item: Dict[str, Any]) -> tuple:
        item_platform = str(item.get('platform') or '').strip().lower()
        item_name = str(item.get('device_name') or '').strip()
        seen_at = _normalize_optional_timestamp(item.get('last_seen_at')) or _normalize_optional_timestamp(item.get('created_at')) or datetime.fromtimestamp(0, tz=timezone.utc)
        return (
            1 if item_platform == normalized_platform else 0,
            1 if item_name == normalized_name else 0,
            1 if str(item.get('device_fingerprint') or '').strip() else 0,
            seen_at,
            int(item.get('id') or 0),
        )

    keep = max(matching, key=sort_key)
    drop_ids = [int(item.get('id') or 0) for item in matching if int(item.get('id') or 0) and int(item.get('id') or 0) != int(keep.get('id') or 0)]
    if not drop_ids:
        return
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'UPDATE devices SET is_active = FALSE, last_seen_at = NOW() WHERE user_id = %s AND id = ANY(%s)',
                (user_id, drop_ids),
            )
        conn.commit()


def _select_soft_match_device(existing: List[Dict[str, Any]], platform: str, device_name: str, incoming_fingerprint: str = "") -> Optional[Dict[str, Any]]:
    return _select_tracking_alias_match_device(existing, platform, device_name, incoming_fingerprint)



def _cleanup_tracking_alias_duplicates(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        SELECT DISTINCT user_id
        FROM devices
        WHERE is_active = TRUE
        """
    )
    user_ids = [int(row["user_id"]) for row in cur.fetchall()]
    for user_id in user_ids:
        cur.execute(
            """
            SELECT *
            FROM devices
            WHERE user_id = %s AND is_active = TRUE
            ORDER BY last_seen_at DESC, created_at DESC, id DESC
            """,
            (user_id,),
        )
        devices = [dict(row) for row in cur.fetchall()]
        if len(devices) <= 1:
            continue

        drop_ids: List[int] = []
        desktop_platform_groups: Dict[str, List[Dict[str, Any]]] = {}
        for item in devices:
            platform = str(item.get("platform") or "").strip().lower()
            if _device_platform_family(platform) == "desktop":
                desktop_platform_groups.setdefault(platform, []).append(item)

        for platform, group in desktop_platform_groups.items():
            strong = [item for item in group if not _is_subscription_tracking_fingerprint(item.get("device_fingerprint"))]
            tracking = [item for item in group if _is_subscription_tracking_fingerprint(item.get("device_fingerprint"))]
            if not tracking:
                continue
            if len(strong) == 1:
                keep = _prefer_device_candidate([strong[0]] + tracking, platform, str(strong[0].get("device_name") or ""))
                for item in group:
                    item_id = int(item.get("id") or 0)
                    if item_id and keep and item_id != int(keep.get("id") or 0):
                        drop_ids.append(item_id)
                continue
            if not strong and len(group) > 1:
                by_client: Dict[str, List[Dict[str, Any]]] = {}
                for item in group:
                    by_client.setdefault(_device_client_family(item.get("device_name")), []).append(item)
                if len(by_client) > 1:
                    keep = _prefer_device_candidate(group, platform, str(group[0].get("device_name") or ""))
                    for item in group:
                        item_id = int(item.get("id") or 0)
                        if item_id and keep and item_id != int(keep.get("id") or 0):
                            drop_ids.append(item_id)

        if drop_ids:
            unique_drop_ids = sorted(set(drop_ids))
            cur.execute(
                "UPDATE devices SET is_active = FALSE, last_seen_at = NOW() WHERE user_id = %s AND id = ANY(%s)",
                (user_id, unique_drop_ids),
            )


def _upsert_device_record(
    user_id: int,
    platform: str,
    device_name: str,
    device_fingerprint: str,
    *,
    enforce_limit: bool = True,
) -> Optional[Dict[str, Any]]:
    user = get_user_by_id(user_id)
    if not user:
        raise ValueError("User not found")
    if user["status"] == "blocked":
        raise PermissionError("User is blocked")

    access_view = get_user_subscription_view(user_id)
    if not bool(access_view.get("is_active")):
        raise PermissionError("Active subscription required")
    if enforce_limit and not settings.VPN_NEW_ACTIVATIONS_ENABLED:
        raise PermissionError("New activations are disabled")
    allowed_limit = max(1, int(access_view.get("device_limit") or 0))
    if not enforce_limit:
        _collapse_duplicate_device_slots(user_id, platform, device_name)
    existing = get_user_devices(user_id)
    for item in existing:
        if item["device_fingerprint"] == device_fingerprint:
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE devices
                        SET platform = %s, device_name = %s, is_active = TRUE, last_seen_at = NOW()
                        WHERE id = %s
                        RETURNING *
                        """,
                        (platform, device_name, item["id"]),
                    )
                    row = cur.fetchone()
                conn.commit()
            return dict(row) if row else None

    alias_match = None
    if not enforce_limit:
        alias_match = _select_tracking_alias_match_device(existing, platform, device_name, device_fingerprint)
        if alias_match:
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE devices
                        SET device_fingerprint = %s,
                            platform = %s,
                            device_name = %s,
                            is_active = TRUE,
                            last_seen_at = NOW()
                        WHERE id = %s
                        RETURNING *
                        """,
                        (device_fingerprint, platform, device_name, alias_match["id"]),
                    )
                    row = cur.fetchone()
                conn.commit()
            return dict(row) if row else None

    if len(existing) >= allowed_limit:
        if not enforce_limit:
            matched_item = _select_soft_match_device(existing, platform, device_name, device_fingerprint)
            if matched_item:
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE devices
                            SET device_fingerprint = %s,
                                platform = %s,
                                device_name = %s,
                                is_active = TRUE,
                                last_seen_at = NOW()
                            WHERE id = %s
                            RETURNING *
                            """,
                            (device_fingerprint, platform, device_name, matched_item["id"]),
                        )
                        row = cur.fetchone()
                    conn.commit()
                return dict(row) if row else None
        if enforce_limit:
            raise PermissionError(f"Device limit reached ({allowed_limit})")
        return None
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO devices (user_id, platform, device_name, device_fingerprint)
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (user_id, platform, device_name, device_fingerprint),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else None



def register_device(user_id: int, platform: str, device_name: str, device_fingerprint: str) -> Dict[str, Any]:
    item = _upsert_device_record(user_id, platform, device_name, device_fingerprint, enforce_limit=True)
    if not item:
        raise PermissionError("Device registration failed")
    return item



def touch_subscription_device_by_token(subscription_token: str, platform: str, device_name: str, device_fingerprint: str) -> Optional[Dict[str, Any]]:
    token = str(subscription_token or "").strip()
    if not token or not device_fingerprint:
        return None
    user = get_user_by_subscription_token(token)
    if not user:
        return None
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM devices WHERE user_id = %s AND device_fingerprint = %s ORDER BY id DESC LIMIT 1",
                (int(user["id"]), str(device_fingerprint or "").strip()),
            )
            existing = cur.fetchone()
    if existing and not bool(existing.get("is_active")):
        return None
    try:
        return _upsert_device_record(
            int(user["id"]),
            str(platform or "client").strip() or "client",
            str(device_name or "VPN client").strip() or "VPN client",
            str(device_fingerprint or "").strip(),
            enforce_limit=False,
        )
    except Exception:
        return None


def delete_device(user_id: int, device_id: int) -> Optional[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE devices
                SET is_active = FALSE,
                    last_seen_at = NOW()
                WHERE user_id = %s AND id = %s AND is_active = TRUE
                RETURNING *
                """,
                (int(user_id), int(device_id)),
            )
            row = cur.fetchone()
            if row:
                fingerprint = str(row.get("device_fingerprint") or "").strip()
                cur.execute(
                    """
                    UPDATE device_subscription_tokens
                    SET is_active = FALSE, updated_at = NOW()
                    WHERE user_id = %s
                      AND (
                            device_id = %s
                         OR (device_fingerprint IS NOT NULL AND device_fingerprint <> '' AND device_fingerprint = %s)
                      )
                    """,
                    (int(user_id), int(device_id), fingerprint),
                )
                cur.execute(
                    """
                    UPDATE user_device_location_credentials
                    SET status = 'revoked', updated_at = NOW()
                    WHERE user_id = %s AND device_id = %s AND status <> 'revoked'
                    """,
                    (int(user_id), int(device_id)),
                )
                cur.execute(
                    """
                    DELETE FROM vpn_location_sessions
                    WHERE user_id = %s AND device_fingerprint = %s
                    """,
                    (int(user_id), fingerprint),
                )
        conn.commit()
    if row:
        enqueue_notification(
            user_id=int(user_id),
            event_type="device_removed",
            unique_key=f"device_removed:{row['id']}:{int(datetime.now(timezone.utc).timestamp())}",
            payload={
                "platform": row.get("platform"),
                "device_name": row.get("device_name"),
                "device_id": row.get("id"),
                "uuid_scope": "per_device",
            },
        )
        return dict(row)
    return None
def list_locations(active_only: bool = True) -> List[Dict[str, Any]]:
    query = "SELECT * FROM locations WHERE is_deleted = FALSE"
    if active_only:
        query += " AND is_active = TRUE"
    query += " ORDER BY sort_order ASC, id ASC"
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [dict(row) for row in cur.fetchall()]


def _normalize_location_mutation_payload(payload: Dict[str, Any], *, default_status: str = "offline") -> Dict[str, Any]:
    data = dict(payload or {})
    save_exact_vpn_payload = bool(data.pop("save_exact_vpn_payload", False))
    data["code"] = str(data.get("code") or "").strip()
    data["name_ru"] = str(data.get("name_ru") or "").strip()
    data["name_en"] = str(data.get("name_en") or data.get("name_ru") or "").strip()
    country_code = data.get("country_code")
    data["country_code"] = str(country_code).strip().upper() or None if country_code is not None else None
    data["status"] = str(data.get("status") or default_status).strip().lower() or default_status
    if data["status"] not in {"online", "offline", "reserve"}:
        data["status"] = default_status
    try:
        data["sort_order"] = int(data.get("sort_order") or 100)
    except (TypeError, ValueError):
        data["sort_order"] = 100
    data["is_active"] = bool(data.get("is_active", True))
    data["is_recommended"] = bool(data.get("is_recommended", False))
    data["is_reserve"] = bool(data.get("is_reserve", False))
    data["download_mbps"] = _normalize_optional_float(data.get("download_mbps"))
    data["upload_mbps"] = _normalize_optional_float(data.get("upload_mbps"))
    data["ping_ms"] = _normalize_optional_int(data.get("ping_ms"))
    data["speed_checked_at"] = _normalize_optional_timestamp(data.get("speed_checked_at"))

    # Fold visible 3X-UI modal controls into vpn_payload for both create and edit.
    # This is important for exact admin saves: the DB payload must store the same
    # Managed by / XUI server key / inbound / credential mode that the admin sees.
    if isinstance(data.get("vpn_payload"), dict):
        folded_payload = dict(data.get("vpn_payload") or {})
    else:
        folded_payload = {}
    for source_key, snake_key, camel_key in (
        ("managed_by", "managed_by", "managedBy"),
        ("xui_server_key", "xui_server_key", "xuiServerKey"),
        ("xui_inbound_id", "xui_inbound_id", "xuiInboundId"),
        ("credential_mode", "credential_mode", "credentialMode"),
        ("access_mode", "access_mode", "accessMode"),
    ):
        if source_key in data and data.get(source_key) is not None:
            folded_payload[snake_key] = data.get(source_key)
            folded_payload[camel_key] = data.get(source_key)
    if folded_payload:
        data["vpn_payload"] = folded_payload

    if save_exact_vpn_payload:
        data["vpn_payload"] = _prepare_exact_admin_vpn_payload(
            data.get("vpn_payload") or {},
            canonical_code=data["code"],
            canonical_country=data.get("country_code") or "",
            canonical_name=data.get("name_en") or "",
        )
        return data

    vpn_payload = _apply_admin_mobile_defaults(_normalize_vpn_payload_keys(data.get("vpn_payload") or {}))
    access_mode = _extract_location_access_mode(data, vpn_payload)
    vpn_payload = _apply_location_access_mode(vpn_payload, access_mode)
    if data["code"]:
        vpn_payload["location_code"] = data["code"]
        vpn_payload["locationCode"] = data["code"]
        vpn_payload.setdefault("resolved_location_code", data["code"])
    if data.get("country_code"):
        vpn_payload["country_code"] = data["country_code"]
        vpn_payload["resolved_country_code"] = data["country_code"]
    if data["name_en"]:
        vpn_payload["remark"] = data["name_en"]
        vpn_payload["display_name"] = data["name_en"]
    data["vpn_payload"] = _canonicalize_payload_metadata(vpn_payload)
    return data



def _insert_or_upsert_location(cur: psycopg.Cursor, data: Dict[str, Any]) -> Dict[str, Any]:
    db_data = dict(data)
    db_data["vpn_payload"] = Jsonb(db_data.get("vpn_payload") or {})
    cur.execute(
        """
        INSERT INTO locations (code, name_ru, name_en, country_code, is_active, is_recommended, is_reserve, status, sort_order, download_mbps, upload_mbps, ping_ms, speed_checked_at, vpn_payload, is_deleted, location_source)
        VALUES (%(code)s, %(name_ru)s, %(name_en)s, %(country_code)s, %(is_active)s, %(is_recommended)s, %(is_reserve)s, %(status)s, %(sort_order)s, %(download_mbps)s, %(upload_mbps)s, %(ping_ms)s, %(speed_checked_at)s, %(vpn_payload)s, %(is_deleted)s, %(location_source)s)
        ON CONFLICT (code) DO UPDATE SET
            name_ru = EXCLUDED.name_ru,
            name_en = EXCLUDED.name_en,
            country_code = EXCLUDED.country_code,
            is_active = EXCLUDED.is_active,
            is_recommended = EXCLUDED.is_recommended,
            is_reserve = EXCLUDED.is_reserve,
            status = EXCLUDED.status,
            sort_order = EXCLUDED.sort_order,
            download_mbps = EXCLUDED.download_mbps,
            upload_mbps = EXCLUDED.upload_mbps,
            ping_ms = EXCLUDED.ping_ms,
            speed_checked_at = EXCLUDED.speed_checked_at,
            vpn_payload = EXCLUDED.vpn_payload,
            is_deleted = FALSE,
            location_source = EXCLUDED.location_source,
            updated_at = NOW()
        RETURNING *
        """,
        db_data,
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("Location was not saved")
    return dict(row)



def _is_locations_sequence_conflict(exc: Exception) -> bool:
    text = str(exc).lower()
    constraint_name = str(getattr(getattr(exc, "diag", None), "constraint_name", "") or "").lower()
    return (
        "duplicate key value violates unique constraint" in text
        and ("locations_pkey" in text or constraint_name == "locations_pkey")
    )



def create_location(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = _normalize_location_mutation_payload(payload)
    if not data["code"]:
        raise ValueError("code is required")
    if not data["name_ru"]:
        raise ValueError("name_ru is required")
    if not data["name_en"]:
        raise ValueError("name_en is required")
    data["is_deleted"] = False
    data["location_source"] = _normalize_location_source(data.get("location_source") or "admin")
    with db() as conn:
        with conn.cursor() as cur:
            try:
                _resync_serial_sequence(cur, "locations")
                row = _insert_or_upsert_location(cur, data)
            except psycopg.Error as exc:
                conn.rollback()
                if not _is_locations_sequence_conflict(exc):
                    raise
                with conn.cursor() as retry_cur:
                    _resync_serial_sequence(retry_cur, "locations")
                    row = _insert_or_upsert_location(retry_cur, data)
        conn.commit()
    return row


def patch_location(location_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_payload = dict(payload or {})
    save_exact_vpn_payload = bool(normalized_payload.pop("save_exact_vpn_payload", False))

    # Safety net for older/admin UI variants that send XUI controls at the top level.
    # Fold them into vpn_payload before validation so the DB always stores what the
    # admin actually changed in the modal.
    if isinstance(normalized_payload.get("vpn_payload"), dict):
        top_payload = dict(normalized_payload.get("vpn_payload") or {})
    else:
        top_payload = {}
    for source_key, snake_key, camel_key in (
        ("managed_by", "managed_by", "managedBy"),
        ("xui_server_key", "xui_server_key", "xuiServerKey"),
        ("xui_inbound_id", "xui_inbound_id", "xuiInboundId"),
        ("credential_mode", "credential_mode", "credentialMode"),
        ("access_mode", "access_mode", "accessMode"),
    ):
        if source_key in normalized_payload and normalized_payload.get(source_key) is not None:
            top_payload[snake_key] = normalized_payload.get(source_key)
            top_payload[camel_key] = normalized_payload.get(source_key)
    if top_payload:
        normalized_payload["vpn_payload"] = top_payload

    access_mode_requested = "access_mode" in normalized_payload

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM locations WHERE id = %s AND is_deleted = FALSE LIMIT 1", (location_id,))
            existing = cur.fetchone()
            if not existing:
                raise ValueError("Location not found")
            existing_row = dict(existing)

            if access_mode_requested and not save_exact_vpn_payload:
                current_vpn_payload = dict(existing_row.get("vpn_payload") or {})
                if isinstance(normalized_payload.get("vpn_payload"), dict):
                    current_vpn_payload.update(normalized_payload.get("vpn_payload") or {})
                merged_mode = _extract_location_access_mode({"access_mode": normalized_payload.get("access_mode")}, current_vpn_payload)
                normalized_payload["vpn_payload"] = _apply_location_access_mode(current_vpn_payload, merged_mode)

            updates: List[str] = []
            values: List[Any] = []
            allowed = {"code", "name_ru", "name_en", "country_code", "is_active", "is_recommended", "is_reserve", "status", "sort_order", "download_mbps", "upload_mbps", "ping_ms", "speed_checked_at", "vpn_payload"}
            for key, value in normalized_payload.items():
                if key not in allowed:
                    continue
                if key == "code":
                    value = str(value or "").strip()
                    if not value:
                        raise ValueError("code is required")
                elif key in {"name_ru", "name_en"}:
                    value = str(value or "").strip()
                elif key == "country_code":
                    value = str(value).strip().upper() or None if value is not None else None
                elif key == "status":
                    value = str(value or "offline").strip() or "offline"
                elif key == "sort_order":
                    try:
                        value = int(value)
                    except (TypeError, ValueError):
                        value = 100
                elif key in {"download_mbps", "upload_mbps"}:
                    value = _normalize_optional_float(value)
                elif key == "ping_ms":
                    value = _normalize_optional_int(value)
                elif key == "speed_checked_at":
                    value = _normalize_optional_timestamp(value)
                elif key == "vpn_payload":
                    # Admin edits the textarea as a FULL config. In exact mode, store the
                    # JSON that came from the admin panel without rebuilding raw_xray_config
                    # or re-merging old defaults. This fixes the visible problem where the
                    # modal was changed, but reopening the location showed the old manual/default
                    # JSON again.
                    if isinstance(value, dict):
                        full_payload = dict(value or {})
                    else:
                        full_payload = {}

                    if save_exact_vpn_payload:
                        # Keep common snake/camel aliases in sync, but do not regenerate the
                        # whole config. That lets admin changes to raw_xray_config, code, xui_*,
                        # credential_mode, access_mode and custom fields persist exactly.
                        full_payload = _prepare_exact_admin_vpn_payload(
                            full_payload,
                            canonical_code=str(normalized_payload.get("code") or existing_row.get("code") or "").strip(),
                            canonical_country=str(normalized_payload.get("country_code") or existing_row.get("country_code") or "").strip().upper(),
                            canonical_name=str(normalized_payload.get("name_en") or existing_row.get("name_en") or existing_row.get("name_ru") or "").strip(),
                        )
                        value = Jsonb(full_payload)
                    else:
                        full_payload = dict(_apply_admin_mobile_defaults(_normalize_vpn_payload_keys(full_payload or {})))
                        full_payload = _apply_location_access_mode(full_payload, _extract_location_access_mode(normalized_payload, full_payload))
                        canonical_code = str(normalized_payload.get("code") or full_payload.get("location_code") or full_payload.get("locationCode") or existing_row.get("code") or "").strip()
                        canonical_name = str(normalized_payload.get("name_en") or full_payload.get("remark") or full_payload.get("display_name") or existing_row.get("name_en") or existing_row.get("name_ru") or canonical_code or "").strip()
                        canonical_country = str(normalized_payload.get("country_code") or full_payload.get("country_code") or full_payload.get("resolved_country_code") or existing_row.get("country_code") or "").strip().upper()
                        if canonical_code:
                            full_payload["location_code"] = canonical_code
                            full_payload["locationCode"] = canonical_code
                            full_payload["resolved_location_code"] = canonical_code
                        if canonical_name:
                            full_payload["remark"] = canonical_name
                            full_payload["display_name"] = canonical_name
                        if canonical_country:
                            full_payload["country_code"] = canonical_country
                            full_payload["resolved_country_code"] = canonical_country
                        value = Jsonb(_canonicalize_payload_metadata(full_payload))
                updates.append(f"{key} = %s")
                values.append(value)

            if not updates:
                raise ValueError("No valid fields to update")

            values.append(location_id)
            query = f"UPDATE locations SET {', '.join(updates)}, updated_at = NOW() WHERE id = %s AND is_deleted = FALSE RETURNING *"
            try:
                cur.execute(query, tuple(values))
            except psycopg.errors.UniqueViolation as exc:
                conn.rollback()
                raise ValueError("Location code already exists") from exc
            row = cur.fetchone()
            if not row:
                raise ValueError("Location not found")
        conn.commit()
    return dict(row)


def delete_location(location_id: int) -> Dict[str, Any]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE locations
                SET is_deleted = TRUE,
                    is_active = FALSE,
                    is_recommended = FALSE,
                    is_reserve = FALSE,
                    status = CASE WHEN status = 'online' THEN 'offline' ELSE status END,
                    updated_at = NOW()
                WHERE id = %s AND is_deleted = FALSE
                RETURNING *
                """,
                (location_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("Location not found")
        conn.commit()
    return dict(row)


def get_vpn_config_for_user(user_id: int, location_code: str) -> Dict[str, Any]:
    subscription = get_current_subscription(user_id)
    if not subscription:
        raise PermissionError("Active subscription required")

    if location_code in {"auto-fastest", "auto-reserve"}:
        row = _pick_virtual_location(location_code, user_id=user_id)
        if not row:
            raise ValueError("No active VLESS node is available for auto selection")
    else:
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM locations WHERE code = %s AND is_active = TRUE AND is_deleted = FALSE AND status IN ('online', 'reserve') LIMIT 1",
                    (location_code,),
                )
                row = cur.fetchone()
    if not row:
        raise ValueError("Location not found")
    if location_code not in {"auto-fastest", "auto-reserve"}:
        current_row = dict(row)
        allow_ready_fallback = bool(getattr(settings, "USER_CONFIG_ALLOW_READY_FALLBACK", False))
        if allow_ready_fallback:
            if not _location_has_fresh_live_signal(current_row) and not _location_payload_ready_for_publish(current_row):
                raise ValueError("Location is offline or its VLESS config is incomplete")
        elif not _location_has_fresh_live_signal(current_row):
            raise ValueError("Location is offline or has not passed a fresh live tunnel check")

    payload = build_user_vpn_payload_for_location(int(user_id), dict(row), requested_location_code=location_code)
    if not payload:
        raise ValueError("VLESS config is not configured for this location")
    if not _config_is_complete(payload):
        raise ValueError("VLESS config is incomplete for this location")
    return payload

def create_payment_record(
    user_id: int,
    plan_id: int,
    provider: str,
    method: str,
    amount: float,
    currency: str,
    status: str,
    external_payment_id: Optional[str] = None,
    checkout_url: Optional[str] = None,
) -> Dict[str, Any]:
    payment_id = str(uuid4())
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (id, user_id, plan_id, provider, method, amount, currency, status, external_payment_id, checkout_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (payment_id, user_id, plan_id, provider, method, amount, currency, status, external_payment_id, checkout_url),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row)


def update_payment(payment_id: str, **fields: Any) -> Dict[str, Any]:
    if not fields:
        raise ValueError("No fields to update")
    updates = []
    values = []
    for key, value in fields.items():
        updates.append(f"{key} = %s")
        values.append(value)
    if fields.get("status") == "paid":
        updates.append("paid_at = COALESCE(paid_at, NOW())")
    values.append(payment_id)
    query = f"UPDATE payments SET {', '.join(updates)} WHERE id = %s RETURNING *"
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(values))
            row = cur.fetchone()
            if not row:
                raise ValueError("Payment not found")
        conn.commit()
    return dict(row)


def get_payment_for_user(payment_id: str, user_id: int) -> Optional[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pay.*, p.code AS plan_code, p.name_ru AS plan_name_ru, p.name_en AS plan_name_en,
                       p.duration_days, p.device_limit
                FROM payments pay
                JOIN plans p ON p.id = pay.plan_id
                WHERE pay.id = %s AND pay.user_id = %s
                """,
                (payment_id, user_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def get_payment_by_internal_or_external(payment_ref: str) -> Optional[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM payments WHERE id = %s OR external_payment_id = %s LIMIT 1",
                (payment_ref, payment_ref),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def activate_payment_and_extend_subscription(payment_id: str) -> Dict[str, Any]:
    payment = get_payment_by_internal_or_external(payment_id)
    if not payment:
        raise ValueError("Payment not found")
    if payment.get("status") == "paid":
        return payment
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM plans WHERE id = %s", (payment["plan_id"],))
            plan = cur.fetchone()
            cur.execute("SELECT * FROM users WHERE id = %s", (payment["user_id"],))
            user = cur.fetchone()
            if not plan or not user:
                raise ValueError("Plan or user not found")
            if user["status"] == "blocked":
                raise PermissionError("Blocked user cannot be activated")
            cur.execute(
                "UPDATE payments SET status = 'paid', paid_at = COALESCE(paid_at, NOW()) WHERE id = %s RETURNING *",
                (payment["id"],),
            )
            payment = dict(cur.fetchone())
            cur.execute(
                "SELECT * FROM subscriptions WHERE user_id = %s ORDER BY expires_at DESC LIMIT 1",
                (payment["user_id"],),
            )
            latest = cur.fetchone()
            start_at = now_utc()
            if latest and latest["expires_at"] and latest["expires_at"] > start_at:
                start_at = latest["expires_at"]
            expires_at = start_at + timedelta(days=int(plan["duration_days"]))
            cur.execute(
                """
                INSERT INTO subscriptions (user_id, plan_id, starts_at, expires_at, status)
                VALUES (%s, %s, %s, %s, 'active')
                RETURNING *
                """,
                (payment["user_id"], plan["id"], start_at, expires_at),
            )
            subscription = cur.fetchone()
        conn.commit()
    refresh_subscription_statuses(payment["user_id"])
    user_info = _normalize_user(user) or {}
    subscription_token = ensure_user_subscription_token(payment["user_id"])
    enqueue_notification(
        user_id=payment["user_id"],
        event_type="payment_paid",
        unique_key=f"payment_paid:{payment['id']}",
        payload={
            "payment_id": payment["id"],
            "plan_code": plan["code"],
            "plan_name_ru": plan["name_ru"],
            "plan_name_en": plan["name_en"],
            "duration_days": int(plan["duration_days"]),
            "device_limit": _resolve_effective_device_limit(plan["device_limit"], user_info.get("device_limit_override")),
            "expires_at": subscription["expires_at"].isoformat(),
            "subscription_token": subscription_token,
        },
    )
    return payment


def list_admin_users(search: str = "", status_filter: str = "all") -> Dict[str, Any]:
    refresh_subscription_statuses()
    clauses = []
    values: List[Any] = []
    if search:
        clauses.append("(CAST(u.telegram_id AS TEXT) ILIKE %s OR COALESCE(u.username, '') ILIKE %s)")
        values.extend([f"%{search}%", f"%{search}%"])
    if status_filter == "blocked":
        clauses.append("u.status = 'blocked'")
    elif status_filter == "active":
        clauses.append("u.status = 'active' AND EXISTS (SELECT 1 FROM subscriptions s WHERE s.user_id = u.id AND s.status = 'active')")
    elif status_filter == "expired":
        clauses.append("u.status <> 'blocked' AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.user_id = u.id AND s.status = 'active')")
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    u.id AS user_id,
                    u.telegram_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.language,
                    u.status,
                    u.device_limit_override,
                    s.id AS subscription_id,
                    s.expires_at,
                    p.code AS plan_code,
                    p.name_ru AS plan_name_ru,
                    p.name_en AS plan_name_en,
                    p.device_limit,
                    COALESCE((SELECT COUNT(*) FROM devices d WHERE d.user_id = u.id AND d.is_active = TRUE), 0) AS devices_used
                FROM users u
                LEFT JOIN LATERAL (
                    SELECT * FROM subscriptions s1
                    WHERE s1.user_id = u.id
                    ORDER BY s1.expires_at DESC
                    LIMIT 1
                ) s ON TRUE
                LEFT JOIN plans p ON p.id = s.plan_id
                {where_sql}
                ORDER BY u.created_at DESC
                """,
                tuple(values),
            )
            items = []
            for row in cur.fetchall():
                item = dict(row)
                item["device_limit_override"] = _normalize_device_limit_override(item.get("device_limit_override"))
                item["device_limit"] = _resolve_effective_device_limit(item.get("device_limit"), item.get("device_limit_override"))
                items.append(item)
            cur.execute("SELECT COUNT(*) AS total FROM users")
            total = cur.fetchone()["total"]
            cur.execute("SELECT COUNT(*) AS total FROM users WHERE status = 'blocked'")
            blocked = cur.fetchone()["total"]
            cur.execute("SELECT COUNT(DISTINCT user_id) AS total FROM subscriptions WHERE status = 'active'")
            active = cur.fetchone()["total"]
    summary = {
        "total_users": total,
        "blocked_users": blocked,
        "active_subscriptions": active,
        "expired_or_no_subscription": max(total - blocked - active, 0),
    }
    return {"items": items, "summary": summary}


def admin_create_or_update_user(payload: Dict[str, Any], admin_name: str) -> Dict[str, Any]:
    plan = get_plan_by_code(payload["plan_code"])
    if not plan:
        raise ValueError("Plan not found")
    user = upsert_telegram_user(payload)
    if "device_limit_override" in payload:
        user = set_user_device_limit_override_by_telegram(int(user["telegram_id"]), payload.get("device_limit_override"), admin_name)["user"]
    refresh_subscription_statuses(user["id"])
    expires_at = payload.get("expires_at")
    if expires_at:
        if isinstance(expires_at, str):
            expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        else:
            expires_dt = expires_at
        starts_at = now_utc()
        if expires_dt <= starts_at:
            starts_at = expires_dt - timedelta(days=int(plan["duration_days"]))
    else:
        starts_at = now_utc()
        expires_dt = starts_at + timedelta(days=int(plan["duration_days"]))
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM subscriptions WHERE user_id = %s AND status = 'active' ORDER BY expires_at DESC, id DESC LIMIT 1",
                (user["id"],),
            )
            current = cur.fetchone()
            if not current:
                cur.execute(
                    "SELECT * FROM subscriptions WHERE user_id = %s ORDER BY expires_at DESC, id DESC LIMIT 1",
                    (user["id"],),
                )
                current = cur.fetchone()
            if current:
                cur.execute(
                    """
                    UPDATE subscriptions
                    SET plan_id = %s, starts_at = %s, expires_at = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (plan["id"], starts_at, expires_dt, current["id"]),
                )
                note = f"Manual access updated for plan {plan['code']}"
            else:
                cur.execute(
                    "INSERT INTO subscriptions (user_id, plan_id, starts_at, expires_at, status) VALUES (%s, %s, %s, %s, 'active')",
                    (user["id"], plan["id"], starts_at, expires_dt),
                )
                note = f"Manual access issued for plan {plan['code']}"
            cur.execute(
                "INSERT INTO admin_notes (user_id, admin_name, note) VALUES (%s, %s, %s)",
                (user["id"], admin_name, note),
            )
        conn.commit()
    refresh_subscription_statuses(user["id"])
    return get_user_snapshot_by_telegram(int(user["telegram_id"]))


def get_user_snapshot_by_telegram(telegram_id: int) -> Dict[str, Any]:
    user = get_user_by_telegram_id(telegram_id)
    if not user:
        raise ValueError("User not found")
    view = get_user_subscription_view(user["id"])
    return {
        "user": user,
        "subscription": view["subscription"],
        "devices": view["devices"],
        "devices_used": view["devices_used"],
        "device_limit": view["device_limit"],
        "device_limit_override": view.get("device_limit_override"),
    }


def extend_user_subscription_by_telegram(telegram_id: int, days_added: int, reason: str, admin_name: str) -> Dict[str, Any]:
    user = get_user_by_telegram_id(telegram_id)
    if not user:
        raise ValueError("User not found")
    if days_added <= 0:
        raise ValueError("days_added must be positive")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM subscriptions WHERE user_id = %s ORDER BY expires_at DESC LIMIT 1", (user["id"],))
            sub = cur.fetchone()
            if not sub:
                plan = next((item for item in get_all_plans() if bool(item.get("is_active", True))), None) or get_all_plans()[0]
                starts_at = now_utc()
                expires_at = starts_at + timedelta(days=days_added)
                cur.execute(
                    "INSERT INTO subscriptions (user_id, plan_id, starts_at, expires_at, status) VALUES (%s, %s, %s, %s, 'active')",
                    (user["id"], plan["id"], starts_at, expires_at),
                )
            else:
                base = sub["expires_at"] if sub["expires_at"] > now_utc() else now_utc()
                new_expiry = base + timedelta(days=days_added)
                cur.execute(
                    "UPDATE subscriptions SET expires_at = %s, status = 'active', updated_at = NOW() WHERE id = %s",
                    (new_expiry, sub["id"]),
                )
            cur.execute(
                "INSERT INTO manual_extensions (user_id, days_added, reason, admin_name) VALUES (%s, %s, %s, %s)",
                (user["id"], days_added, reason, admin_name),
            )
        conn.commit()
    refresh_subscription_statuses(user["id"])
    return get_user_snapshot_by_telegram(telegram_id)


def set_user_device_limit_override_by_telegram(telegram_id: int, device_limit_override: Optional[int], admin_name: str) -> Dict[str, Any]:
    user = get_user_by_telegram_id(telegram_id)
    if not user:
        raise ValueError("User not found")
    normalized_override = _normalize_device_limit_override(device_limit_override)
    note = (
        f"Device limit override set to {normalized_override}"
        if normalized_override is not None
        else "Device limit override cleared (plan default)"
    )
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET device_limit_override = %s, updated_at = NOW() WHERE telegram_id = %s RETURNING *",
                (normalized_override, telegram_id),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("User not found")
            cur.execute(
                "INSERT INTO admin_notes (user_id, admin_name, note) VALUES (%s, %s, %s)",
                (row["id"], admin_name, note),
            )
        conn.commit()
    return get_user_snapshot_by_telegram(telegram_id)


def list_active_device_location_credentials(*, user_id: Optional[int] = None, device_id: Optional[int] = None) -> List[Dict[str, Any]]:
    query = """
        SELECT c.*, d.platform, d.device_name, d.device_fingerprint, u.telegram_id
        FROM user_device_location_credentials c
        JOIN devices d ON d.id = c.device_id AND d.user_id = c.user_id
        JOIN users u ON u.id = c.user_id
        WHERE c.status = 'active' AND d.is_active = TRUE
    """
    args: List[Any] = []
    if user_id is not None:
        query += " AND c.user_id = %s"
        args.append(int(user_id))
    if device_id is not None:
        query += " AND c.device_id = %s"
        args.append(int(device_id))
    query += " ORDER BY c.location_code ASC, c.user_id ASC, c.device_id ASC"
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(args))
            return [dict(row) for row in cur.fetchall()]


def reset_user_devices_by_telegram(telegram_id: int, admin_name: str) -> Dict[str, Any]:
    user = get_user_by_telegram_id(telegram_id)
    if not user:
        raise ValueError("User not found")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM devices WHERE user_id = %s AND is_active = TRUE", (user["id"],))
            devices = [dict(row) for row in cur.fetchall()]
            cur.execute(
                "UPDATE device_subscription_tokens SET is_active = FALSE, updated_at = NOW() WHERE user_id = %s",
                (user["id"],),
            )
            cur.execute(
                "UPDATE user_device_location_credentials SET status = 'revoked', updated_at = NOW() WHERE user_id = %s",
                (user["id"],),
            )
            cur.execute("DELETE FROM vpn_location_sessions WHERE user_id = %s", (user["id"],))
            cur.execute("DELETE FROM devices WHERE user_id = %s", (user["id"],))
            cur.execute(
                "INSERT INTO admin_notes (user_id, admin_name, note) VALUES (%s, %s, %s)",
                (user["id"], admin_name, "Devices reset from admin panel"),
            )
        conn.commit()
    if devices:
        enqueue_notification(
            user_id=user["id"],
            event_type="device_removed",
            unique_key=f"device_reset:{user['id']}:{int(now_utc().timestamp())}",
            payload={"count": len(devices), "device_name": "all", "platform": "all"},
        )
    return get_user_snapshot_by_telegram(telegram_id)


def list_payments(status_filter: str = "all") -> List[Dict[str, Any]]:
    query = """
        SELECT pay.*, u.telegram_id, u.username, p.code AS plan_code, p.name_ru AS plan_name_ru, p.name_en AS plan_name_en
        FROM payments pay
        JOIN users u ON u.id = pay.user_id
        JOIN plans p ON p.id = pay.plan_id
    """
    values: Tuple[Any, ...] = tuple()
    if status_filter != "all":
        query += " WHERE pay.status = %s"
        values = (status_filter,)
    query += " ORDER BY pay.created_at DESC"
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
            return [dict(row) for row in cur.fetchall()]


def export_payments_csv(status_filter: str = "all") -> str:
    rows = list_payments(status_filter)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["payment_id", "telegram_id", "username", "plan_code", "amount", "currency", "status", "external_payment_id", "created_at", "paid_at"])
    for row in rows:
        writer.writerow([
            row.get("id"),
            row.get("telegram_id"),
            row.get("username"),
            row.get("plan_code"),
            row.get("amount"),
            row.get("currency"),
            row.get("status"),
            row.get("external_payment_id"),
            row.get("created_at"),
            row.get("paid_at"),
        ])
    return output.getvalue()


def issue_auth_code(user_id: int, ttl_minutes: Optional[int] = None, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ttl = max(1, int(ttl_minutes or settings.AUTH_CODE_TTL_MINUTES or 5))
    code = secrets.token_urlsafe(18)
    expires_at = now_utc() + timedelta(minutes=ttl)
    payload = meta or {}
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auth_codes
                SET used_at = NOW()
                WHERE user_id = %s AND used_at IS NULL
                """,
                (user_id,),
            )
            cur.execute(
                """
                INSERT INTO auth_codes (code, user_id, expires_at, meta)
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (code, user_id, expires_at, Jsonb(payload)),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row)


def consume_auth_code(code: str) -> Optional[Dict[str, Any]]:
    normalized = (code or "").strip()
    if not normalized:
        return None
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM auth_codes
                WHERE code = %s
                FOR UPDATE
                """,
                (normalized,),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None
            if row.get("used_at") is not None or row.get("expires_at") <= now_utc():
                conn.rollback()
                return None
            cur.execute(
                "UPDATE auth_codes SET used_at = NOW() WHERE code = %s",
                (normalized,),
            )
            cur.execute(
                "SELECT * FROM users WHERE id = %s",
                (row["user_id"],),
            )
            user = cur.fetchone()
        conn.commit()
    return _normalize_user(user)


def settings_snapshot() -> Dict[str, Any]:
    plans = _current_runtime_plan_payloads()
    urls = _client_store_urls()
    client_mode = _normalize_client_mode(getattr(settings, "VPN_CLIENT_MODE", "hiddify"))
    return {
        "app_name": settings.APP_NAME,
        "client_mode": client_mode,
        "client_name": _active_client_name(client_mode),
        "mobile_client_mode": client_mode,
        "mobile_client_name": _active_client_name(client_mode),
        "desktop_client_mode": "happ",
        "desktop_client_name": _desktop_client_name(),
        "app_env": settings.APP_ENV,
        "languages": settings.APP_LANGS,
        "device_limit": settings.VPN_DEFAULT_DEVICE_LIMIT,
        "max_devices_per_account": settings.VPN_MAX_DEVICES_PER_ACCOUNT,
        "maintenance_mode": settings.VPN_MAINTENANCE_MODE,
        "new_activations_enabled": settings.VPN_NEW_ACTIVATIONS_ENABLED,
        "access_mode": getattr(settings, "VPN_ACCESS_MODE", "paid"),
        "free_mode_device_limit": getattr(settings, "VPN_FREE_MODE_DEVICE_LIMIT", settings.VPN_DEFAULT_DEVICE_LIMIT),
        "paid_grace_hours": getattr(settings, "VPN_PAID_GRACE_HOURS", 24),
        "paid_grace_started_at": (get_runtime_settings_payload() or {}).get("paid_grace_started_at"),
        "payments_enabled": settings.PAYMENTS_ENABLED,
        "payments_provider": settings.PAYMENTS_PROVIDER,
        "support_telegram_url": settings.SUPPORT_TELEGRAM_URL,
        "support_faq_ru": settings.SUPPORT_FAQ_RU,
        "support_faq_en": settings.SUPPORT_FAQ_EN,
        "bot_name": settings.BOT_NAME,
        "bot_username": settings.BOT_USERNAME,
        "open_app_url": settings.OPEN_APP_URL,
        "android_app_url": urls["android_app_url"],
        "ios_app_url": urls["ios_app_url"],
        "windows_app_url": urls["windows_app_url"],
        "macos_app_url": urls["macos_app_url"],
        "android_app_package": urls["android_app_package"],
        "settings_editable": True,
        "locations_catalog_source": "env_override" if settings.DEFAULT_LOCATIONS_ENV_OVERRIDE_ENABLED else "builtin_mvp",
        "plans": plans,
    }


def enqueue_notification(user_id: int, event_type: str, unique_key: str, payload: Dict[str, Any]) -> bool:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_notifications (unique_key, user_id, event_type, payload)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (unique_key) DO NOTHING
                RETURNING id
                """,
                (unique_key, user_id, event_type, Jsonb(payload or {})),
            )
            row = cur.fetchone()
        conn.commit()
    return bool(row)


def purge_stale_subscription_notifications() -> None:
    warning_hours = max(1, int(getattr(settings, "SUBSCRIPTION_WARNING_HOURS", 12) or 12))
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM bot_notifications n
                WHERE n.sent_at IS NULL
                  AND n.event_type = 'subscription_expiring'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM (
                          SELECT DISTINCT ON (s.user_id) s.id, s.user_id, s.expires_at
                          FROM subscriptions s
                          WHERE s.status = 'active' AND s.expires_at > NOW()
                          ORDER BY s.user_id, s.expires_at DESC, s.id DESC
                      ) latest
                      WHERE latest.user_id = n.user_id
                        AND latest.expires_at <= NOW() + INTERVAL '1 day'
                        AND latest.id = CASE
                            WHEN COALESCE(n.payload->>'subscription_id', '') ~ '^[0-9]+$'
                                THEN (n.payload->>'subscription_id')::BIGINT
                            ELSE NULL
                        END
                  )
                """
            )
            cur.execute(
                f"""
                DELETE FROM bot_notifications n
                WHERE n.sent_at IS NULL
                  AND n.event_type = 'subscription_expiring_12h'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM (
                          SELECT DISTINCT ON (s.user_id) s.id, s.user_id, s.expires_at
                          FROM subscriptions s
                          WHERE s.status = 'active' AND s.expires_at > NOW()
                          ORDER BY s.user_id, s.expires_at DESC, s.id DESC
                      ) latest
                      WHERE latest.user_id = n.user_id
                        AND latest.expires_at <= NOW() + INTERVAL '{warning_hours} hour'
                        AND latest.id = CASE
                            WHEN COALESCE(n.payload->>'subscription_id', '') ~ '^[0-9]+$'
                                THEN (n.payload->>'subscription_id')::BIGINT
                            ELSE NULL
                        END
                  )
                """
            )
            cur.execute(
                """
                DELETE FROM bot_notifications n
                WHERE n.sent_at IS NULL
                  AND n.event_type = 'subscription_expired'
                  AND EXISTS (
                      SELECT 1
                      FROM subscriptions s
                      WHERE s.user_id = n.user_id
                        AND s.status = 'active'
                  )
                """
            )
        conn.commit()



def enqueue_subscription_notifications() -> None:
    refresh_subscription_statuses()
    purge_stale_subscription_notifications()
    warning_hours = max(1, int(getattr(settings, "SUBSCRIPTION_WARNING_HOURS", 12) or 12))
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT latest.id, latest.user_id, latest.expires_at, latest.plan_code, latest.name_ru, latest.name_en, latest.duration_days, latest.device_limit
                FROM (
                    SELECT DISTINCT ON (s.user_id)
                        s.id, s.user_id, s.expires_at, p.code AS plan_code, p.name_ru, p.name_en, p.duration_days, p.device_limit
                    FROM subscriptions s
                    JOIN plans p ON p.id = s.plan_id
                    WHERE s.status = 'active' AND s.expires_at > NOW()
                    ORDER BY s.user_id, s.expires_at DESC, s.id DESC
                ) latest
                WHERE latest.expires_at <= NOW() + INTERVAL '1 day'
                """
            )
            expiring = [dict(row) for row in cur.fetchall()]
            cur.execute(
                f"""
                SELECT latest.id, latest.user_id, latest.expires_at, latest.plan_code, latest.name_ru, latest.name_en, latest.duration_days, latest.device_limit
                FROM (
                    SELECT DISTINCT ON (s.user_id)
                        s.id, s.user_id, s.expires_at, p.code AS plan_code, p.name_ru, p.name_en, p.duration_days, p.device_limit
                    FROM subscriptions s
                    JOIN plans p ON p.id = s.plan_id
                    WHERE s.status = 'active' AND s.expires_at > NOW()
                    ORDER BY s.user_id, s.expires_at DESC, s.id DESC
                ) latest
                WHERE latest.expires_at <= NOW() + INTERVAL '{warning_hours} hour'
                """
            )
            expiring_critical = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT DISTINCT ON (s.user_id)
                    s.id, s.user_id, s.expires_at, p.code AS plan_code, p.name_ru, p.name_en, p.duration_days, p.device_limit
                FROM subscriptions s
                JOIN plans p ON p.id = s.plan_id
                WHERE s.status = 'expired'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM subscriptions s2
                      WHERE s2.user_id = s.user_id
                        AND s2.status = 'active'
                  )
                ORDER BY s.user_id, s.expires_at DESC
                LIMIT 500
                """
            )
            expired = [dict(row) for row in cur.fetchall()]
    for row in expiring:
        user = get_user_by_id(int(row["user_id"])) or {}
        enqueue_notification(
            user_id=row["user_id"],
            event_type="subscription_expiring",
            unique_key=f"subscription_expiring:{row['id']}",
            payload={
                "subscription_id": row["id"],
                "expires_at": row["expires_at"].isoformat(),
                "plan_code": row["plan_code"],
                "plan_name_ru": row["name_ru"],
                "plan_name_en": row["name_en"],
                "duration_days": int(row["duration_days"]),
                "device_limit": _resolve_effective_device_limit(row["device_limit"], user.get("device_limit_override")),
            },
        )
    for row in expiring_critical:
        user = get_user_by_id(int(row["user_id"])) or {}
        enqueue_notification(
            user_id=row["user_id"],
            event_type="subscription_expiring_12h",
            unique_key=f"subscription_expiring_12h:{row['id']}",
            payload={
                "subscription_id": row["id"],
                "expires_at": row["expires_at"].isoformat(),
                "plan_code": row["plan_code"],
                "plan_name_ru": row["name_ru"],
                "plan_name_en": row["name_en"],
                "duration_days": int(row["duration_days"]),
                "device_limit": _resolve_effective_device_limit(row["device_limit"], user.get("device_limit_override")),
                "warning_hours": warning_hours,
            },
        )
    for row in expired:
        user = get_user_by_id(int(row["user_id"])) or {}
        enqueue_notification(
            user_id=row["user_id"],
            event_type="subscription_expired",
            unique_key=f"subscription_expired:{row['id']}",
            payload={
                "subscription_id": row["id"],
                "expires_at": row["expires_at"].isoformat(),
                "plan_code": row["plan_code"],
                "plan_name_ru": row["name_ru"],
                "plan_name_en": row["name_en"],
                "duration_days": int(row["duration_days"]),
                "device_limit": _resolve_effective_device_limit(row["device_limit"], user.get("device_limit_override")),
            },
        )



def list_pending_notifications(limit: int = 100) -> List[Dict[str, Any]]:
    purge_stale_subscription_notifications()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT n.*, u.telegram_id, u.language, u.status AS user_status
                FROM bot_notifications n
                JOIN users u ON u.id = n.user_id
                WHERE n.sent_at IS NULL
                  AND n.failed_at IS NULL
                  AND (n.next_retry_at IS NULL OR n.next_retry_at <= NOW())
                ORDER BY n.created_at ASC
                LIMIT %s
                """,
                (limit,),
            )
            rows = [dict(row) for row in cur.fetchall()]
    return rows


def mark_notification_sent(notification_id: int) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE bot_notifications SET sent_at = NOW(), last_error = NULL, next_retry_at = NULL WHERE id = %s", (notification_id,))
        conn.commit()


def mark_notification_retry(notification_id: int, error_message: str, retry_after_seconds: int = 300) -> None:
    retry_delay = max(30, int(retry_after_seconds or 300))
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_notifications
                SET attempt_count = COALESCE(attempt_count, 0) + 1,
                    last_error = %s,
                    next_retry_at = NOW() + (%s * INTERVAL '1 second')
                WHERE id = %s
                """,
                (str(error_message or "")[:4000], retry_delay, notification_id),
            )
        conn.commit()


def mark_notification_failed(notification_id: int, error_message: str) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_notifications
                SET attempt_count = COALESCE(attempt_count, 0) + 1,
                    last_error = %s,
                    failed_at = NOW(),
                    next_retry_at = NULL
                WHERE id = %s
                """,
                (str(error_message or "")[:4000], notification_id),
            )
        conn.commit()


def record_bot_error(source: str, context: str, error_message: str) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bot_error_log (source, context, error_message) VALUES (%s, %s, %s)",
                (source, context, error_message[:4000]),
            )
        conn.commit()


def list_bot_errors(limit: int = 50) -> List[Dict[str, Any]]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bot_error_log ORDER BY created_at DESC LIMIT %s", (limit,))
            return [dict(row) for row in cur.fetchall()]


def list_broadcast_targets(statuses: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    statuses = statuses or ["active"]
    statuses = [item.strip().lower() for item in statuses if item]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.id, u.telegram_id, u.language, u.status,
                       s.expires_at
                FROM users u
                LEFT JOIN LATERAL (
                    SELECT * FROM subscriptions s1
                    WHERE s1.user_id = u.id
                    ORDER BY s1.expires_at DESC
                    LIMIT 1
                ) s ON TRUE
                ORDER BY u.created_at DESC
                """
            )
            rows = [dict(row) for row in cur.fetchall()]
    result = []
    now = now_utc()
    for row in rows:
        if row["status"] == "blocked":
            user_bucket = "blocked"
        elif row.get("expires_at") and row["expires_at"] >= now:
            user_bucket = "active"
        else:
            user_bucket = "expired"
        if user_bucket in statuses:
            result.append(row)
    return result
