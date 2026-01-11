from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _get_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return default
    return int(val)

def _get_int_list(name: str) -> list[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return out

@dataclass(frozen=True)
class Settings:
    bot_token: str
    vip_channel_id: int
    admin_ids: list[int]
    track_interval_seconds: int
    tp1_partial_close_pct: int

def load_settings() -> Settings:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is missing. Put it in .env")

    vip_channel_id = _get_int("VIP_CHANNEL_ID", 0)
    if vip_channel_id == 0:
        raise RuntimeError("VIP_CHANNEL_ID is missing. Put numeric channel id in .env")

    admin_ids = _get_int_list("ADMIN_IDS")
    track_interval_seconds = _get_int("TRACK_INTERVAL_SECONDS", 3)
    tp1_partial_close_pct = _get_int("TP1_PARTIAL_CLOSE_PCT", 50)
    tp1_partial_close_pct = max(0, min(100, tp1_partial_close_pct))

    return Settings(
        bot_token=bot_token,
        vip_channel_id=vip_channel_id,
        admin_ids=admin_ids,
        track_interval_seconds=track_interval_seconds,
        tp1_partial_close_pct=tp1_partial_close_pct,
    )
