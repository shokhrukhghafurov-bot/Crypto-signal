from __future__ import annotations

import aiosqlite
from pathlib import Path
from typing import Any, Optional

DB_PATH = Path("data.sqlite3")

SCHEMA = """PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  market TEXT NOT NULL,
  direction TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  entry REAL NOT NULL,
  sl REAL NOT NULL,
  tp1 REAL NOT NULL,
  tp2 REAL NOT NULL,
  rr REAL NOT NULL,
  confidence INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  signal_id INTEGER NOT NULL,
  opened_at TEXT NOT NULL,
  status TEXT NOT NULL,
  tp1_hit INTEGER NOT NULL DEFAULT 0,
  tp1_close_pct INTEGER NOT NULL DEFAULT 50,
  sl_moved_to_be INTEGER NOT NULL DEFAULT 0,
  closed_at TEXT,
  close_reason TEXT,
  UNIQUE(user_id, signal_id)
);

CREATE INDEX IF NOT EXISTS idx_user_positions_status ON user_positions(status);
CREATE INDEX IF NOT EXISTS idx_user_positions_signal ON user_positions(signal_id);
"""

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.executescript(SCHEMA)
        await db.commit()

async def fetchone(query: str, params: tuple[Any, ...] = ()) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cur:
            return await cur.fetchone()

async def fetchall(query: str, params: tuple[Any, ...] = ()) -> list[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cur:
            return await cur.fetchall()

async def execute(query: str, params: tuple[Any, ...] = ()) -> None:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute(query, params)
        await db.commit()

async def execute_returning_id(query: str, params: tuple[Any, ...] = ()) -> int:
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        cur = await db.execute(query, params)
        await db.commit()
        return int(cur.lastrowid)
