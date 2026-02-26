from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from pathlib import Path
import os
import re
import time
import contextvars

# =======================
# SAFE ATR PICKER FOR SL
# =======================
import math

def _safe_last_atr(df):
    if df is None or getattr(df, "empty", True):
        return None
    if "atr" not in df.columns:
        return None
    try:
        v = float(df["atr"].iloc[-1])
        if not math.isfinite(v) or v <= 0:
            return None
        return v
    except Exception:
        return None

def pick_atr_for_sl(df5, df1, df4, min_1h_bars=60):
    pref = (os.getenv("ANALYSIS_SL_ATR_TF", "1h") or "1h").strip().lower()

    df1_ok = df1 if (df1 is not None and not df1.empty and len(df1) >= min_1h_bars) else None

    atr5 = _safe_last_atr(df5)
    atr1 = _safe_last_atr(df1_ok)
    atr4 = _safe_last_atr(df4)

    if pref in ("5m","5") and atr5 is not None:
        return atr5, "5m"
    if pref in ("1h","60m","1") and atr1 is not None:
        return atr1, "1h"
    if pref in ("4h","240m","4") and atr4 is not None:
        return atr4, "4h"

    if atr1 is not None:
        return atr1, "1h"
    if atr5 is not None:
        return atr5, "5m"
    if atr4 is not None:
        return atr4, "4h"

    return 0.0, "none"




# =======================
# TEST MODE (diagnostics)
# =======================
# Enable with: TEST_MODE=1
# Purpose: temporarily relax MID filters to validate that signal emission pipeline works end-to-end.
_TEST_MODE = (os.getenv("TEST_MODE", "0") or "").strip().lower() in ("1","true","yes","on")
_TEST_MODE_SCOPE = (os.getenv("TEST_MODE_SCOPE", "MID") or "MID").strip().upper()

if _TEST_MODE and _TEST_MODE_SCOPE in ("MID","ALL"):
    # We set os.environ overrides BEFORE the rest of the module reads env vars into globals.
    _overrides = {
        # --- MID alignment / HTF guards ---
        "MID_TF_ALIGN_MIN": "0",
        "MID_REQUIRE_30M_TREND": "0",
        "MID_REQUIRE_STRUCTURE_ALIGN": "0",
        "MID_REQUIRE_VWAP_ALIGN": "0",
        "MID_GUARD_HTF_ALIGN": "0",
        "MID_GUARD_VWAP": "0",
        "MID_GUARD_TREND_ADX_MIN": "0",
        # --- MID ADX / RR / score ---
        "MID_MIN_ADX_1H": "0",
        "MID_MIN_ADX_30M": "0",
        "MID_HTF_STRONG_ADX_MIN": "0",
        "MID_MIN_RR": "0.5",
        "MID_MIN_RR_IF_ADX_GT_25": "0.5",
        "MID_RR_DISCOUNT": "1.0",
        "MID_MIN_SCORE_FUTURES": "90",
        "MID_MIN_SCORE_SPOT": "90",
        "MID_MIN_CONFIDENCE": "0",
        "MID_MIN_VOL_X": "0",
        # --- MID regime / filters ---
        "MID_NEWS_FILTER": "1",
        "MID_MACRO_FILTER": "1",
        "MID_RANGE_POS_FILTER": "0",
        "MID_TOP_FILTERS": "0",
        "MID_TRAP_FILTERS": "1",
        "MID_ALLOW_RANGE": "1",
        "MID_ALLOW_COUNTERTREND_WITH_5M_REVERSAL": "1",
        # Optional: avoid "no repeat" during test
        "MID_SYMBOL_COOLDOWN_MIN": os.getenv("MID_SYMBOL_COOLDOWN_MIN", "0") or "0",
    }
    for k, v in _overrides.items():
        os.environ[k] = str(v)

# Enable verbose price source / fallback logging
PRICE_DEBUG = os.getenv('PRICE_DEBUG', '0').strip().lower() in ('1','true','yes','on')

# =======================
# MID CANDLES "LIGHT MODE"
# =======================
# Enable with: MID_CANDLES_LIGHT_MODE=1
# - Binace-first routing for candles
# - Faster REST timeout to avoid long stalls, with immediate fallback to BYBIT/OKX (and MEXC/GATEIO for SPOT)
MID_CANDLES_LIGHT_MODE = (os.getenv("MID_CANDLES_LIGHT_MODE", "0") or "").strip().lower() in ("1","true","yes","on")
MID_CANDLES_BINANCE_FIRST = (os.getenv("MID_CANDLES_BINANCE_FIRST", "0") or "").strip().lower() in ("1","true","yes","on")
MID_LIGHT_HTTP_TIMEOUT_SEC = float(os.getenv("MID_LIGHT_HTTP_TIMEOUT_SEC", "2.5") or "2.5")


# --- MID extra entry filters (anti-late-entry / RSI / VWAP distance / climax) ---
MID_LATE_ENTRY_ATR_MAX = float(os.getenv("MID_LATE_ENTRY_ATR_MAX", "2.2"))   # (close - recent_low)/ATR_30m
MID_RSI_LONG_MAX = float(os.getenv("MID_RSI_LONG_MAX", "62"))               # RSI(5m) for LONG must be <
MID_RSI_SHORT_MIN = float(os.getenv("MID_RSI_SHORT_MIN", "48"))             # RSI(5m) for SHORT must be >
MID_VWAP_DIST_ATR_MAX = float(os.getenv("MID_VWAP_DIST_ATR_MAX", "1.8"))     # abs(close - vwap)/ATR_30m
MID_CLIMAX_VOL_X = float(os.getenv("MID_CLIMAX_VOL_X", "2.5"))              # last_vol / avg_vol
MID_CLIMAX_BODY_ATR = float(os.getenv("MID_CLIMAX_BODY_ATR", "1.2"))         # abs(close-open)/ATR_30m
MID_CLIMAX_COOLDOWN_BARS = int(os.getenv("MID_CLIMAX_COOLDOWN_BARS", "1"))   # block next N bars after climax


# --- MID ULTRA SAFE (reduce SL dramatically; fewer but higher-quality signals) ---
MID_ULTRA_SAFE = os.getenv("MID_ULTRA_SAFE", "0").strip().lower() in ("1","true","yes","on")
# Strong-trend countertrend blocker (uses 30m ADX)
MID_HTF_ALIGN_GUARD = os.getenv("MID_HTF_ALIGN_GUARD", "1").strip().lower() not in ("0","false","no","off")
MID_HTF_STRONG_ADX_MIN = float(os.getenv("MID_HTF_STRONG_ADX_MIN", "28"))   # ADX(30m) >= -> treat as strong trend
# Allow countertrend only if 5m shows reversal structure (BOS + candle + swing)
MID_ALLOW_COUNTERTREND_WITH_5M_REVERSAL = os.getenv("MID_ALLOW_COUNTERTREND_WITH_5M_REVERSAL", "1").strip().lower() not in ("0","false","no","off")

# More strict late-entry defaults when ULTRA SAFE is enabled (applied in code)
MID_ULTRA_LATE_ENTRY_ATR_MAX = float(os.getenv("MID_ULTRA_LATE_ENTRY_ATR_MAX", "1.6"))
MID_ULTRA_VWAP_DIST_ATR_MAX = float(os.getenv("MID_ULTRA_VWAP_DIST_ATR_MAX", "1.2"))
MID_ULTRA_RSI_LONG_MAX = float(os.getenv("MID_ULTRA_RSI_LONG_MAX", "58"))
MID_ULTRA_RSI_SHORT_MIN = float(os.getenv("MID_ULTRA_RSI_SHORT_MIN", "52"))

# Additional ULTRA SAFE tightening knobs (optional)
MID_ULTRA_RSI_LONG_MIN = float(os.getenv("MID_ULTRA_RSI_LONG_MIN", "48"))   # RSI(5m) for LONG must be >=
MID_ULTRA_RSI_SHORT_MAX = float(os.getenv("MID_ULTRA_RSI_SHORT_MAX", "58")) # RSI(5m) for SHORT must be <=
MID_ULTRA_CONFIRM_CANDLE_BODY_ATR_MIN = float(os.getenv("MID_ULTRA_CONFIRM_CANDLE_BODY_ATR_MIN", "0.15"))

# Near-extremes blocker (avoid LONG right under resistance / SHORT right above support)
MID_BLOCK_NEAR_EXTREMES = os.getenv("MID_BLOCK_NEAR_EXTREMES", "1").strip().lower() not in ("0","false","no","off")
MID_NEAR_EXTREME_ATR_MIN = float(os.getenv("MID_NEAR_EXTREME_ATR_MIN", "0.35"))   # dist to recent extreme in ATR(30m)
MID_ULTRA_NEAR_EXTREME_ATR_MIN = float(os.getenv("MID_ULTRA_NEAR_EXTREME_ATR_MIN", "0.55"))
# --- MID anti-bounce / RSI window / BB bounce guard ---
MID_ANTI_BOUNCE_ENABLED = os.getenv("MID_ANTI_BOUNCE_ENABLED", "1").strip().lower() not in ("0","false","no","off")
MID_ANTI_BOUNCE_ATR_MAX = float(os.getenv("MID_ANTI_BOUNCE_ATR_MAX", "1.8"))     # SHORT: (close - recent_low)/ATR_30m; LONG: (recent_high - close)/ATR_30m
MID_RSI_LONG_MIN = float(os.getenv("MID_RSI_LONG_MIN", "45"))                    # RSI(5m) for LONG must be >
MID_RSI_SHORT_MAX = float(os.getenv("MID_RSI_SHORT_MAX", "64"))                  # RSI(5m) for SHORT must be <
MID_BLOCK_BB_BOUNCE = os.getenv("MID_BLOCK_BB_BOUNCE", "1").strip().lower() not in ("0","false","no","off")

# Confirmation candle guard (reduces fast SL right after entry):
# - Require direction-aligned last 5m candle (green for LONG, red for SHORT)
# - Require minimal candle body vs ATR(30m) to avoid chop/noise entries
MID_CONFIRM_CANDLE_ENABLED = os.getenv("MID_CONFIRM_CANDLE_ENABLED", "1").strip().lower() not in ("0","false","no","off")
MID_CONFIRM_CANDLE_REQUIRE_DIRECTION = os.getenv("MID_CONFIRM_CANDLE_REQUIRE_DIRECTION", "1").strip().lower() not in ("0","false","no","off")
MID_CONFIRM_CANDLE_BODY_ATR_MIN = float(os.getenv("MID_CONFIRM_CANDLE_BODY_ATR_MIN", "0.10"))  # abs(close-open)/ATR_30m
# Require price to be on the correct side of VWAP(30m) (LONG above, SHORT below)
MID_REQUIRE_VWAP_SIDE = os.getenv("MID_REQUIRE_VWAP_SIDE", "1").strip().lower() not in ("0","false","no","off")
# --- MID trend/structure guards (anti-countertrend) ---
# Block LONG when micro structure is bearish (reduces "bounce then SL" cases)
MID_BLOCK_LONG_BOS_DOWN = os.getenv("MID_BLOCK_LONG_BOS_DOWN", "1").strip().lower() not in ("0","false","no","off")
MID_BLOCK_LONG_BELOW_EMA20_5M = os.getenv("MID_BLOCK_LONG_BELOW_EMA20_5M", "1").strip().lower() not in ("0","false","no","off")
MID_BLOCK_LONG_2_RED_CANDLES = os.getenv("MID_BLOCK_LONG_2_RED_CANDLES", "1").strip().lower() not in ("0","false","no","off")
MID_BLOCK_LONG_LOWER_HIGHS = os.getenv("MID_BLOCK_LONG_LOWER_HIGHS", "1").strip().lower() not in ("0","false","no","off")
MID_LOWER_HIGHS_LOOKBACK = int(os.getenv("MID_LOWER_HIGHS_LOOKBACK", "4") or 4)  # count of last highs to compare
MID_BLOCK_SHORT_BOS_UP = os.getenv("MID_BLOCK_SHORT_BOS_UP", "1").strip().lower() not in ("0","false","no","off")
MID_BLOCK_SHORT_ABOVE_EMA20_5M = os.getenv("MID_BLOCK_SHORT_ABOVE_EMA20_5M", "1").strip().lower() not in ("0","false","no","off")
MID_BLOCK_SHORT_2_GREEN_CANDLES = os.getenv("MID_BLOCK_SHORT_2_GREEN_CANDLES", "1").strip().lower() not in ("0","false","no","off")
MID_BLOCK_SHORT_HIGHER_LOWS = os.getenv("MID_BLOCK_SHORT_HIGHER_LOWS", "1").strip().lower() not in ("0","false","no","off")
# --- MID microstructure robustness ---
MID_BOS_RECENT_BARS = int(os.getenv("MID_BOS_RECENT_BARS", "6") or 6)  # consider BOS within last N bars
MID_HL_TOL_PCT = float(os.getenv("MID_HL_TOL_PCT", "0.001") or 0.001)  # allow equal-ish highs/lows (0.1%)
MID_REQUIRE_5M_CONFIRM = os.getenv("MID_REQUIRE_5M_CONFIRM", "1").strip().lower() not in ("0","false","no","off")
MID_REQUIRE_LIQ_SWEEP = os.getenv("MID_REQUIRE_LIQ_SWEEP", "0").strip().lower() not in ("0","false","no","off")
MID_LIQ_SWEEP_BONUS = float(os.getenv("MID_LIQ_SWEEP_BONUS", "4") or 4.0)
MID_FAIL_CLOSED = os.getenv("MID_FAIL_CLOSED", "1").strip().lower() not in ("0","false","no","off")
MID_HIGHER_LOWS_LOOKBACK = int(os.getenv("MID_HIGHER_LOWS_LOOKBACK", str(MID_LOWER_HIGHS_LOOKBACK)) or MID_LOWER_HIGHS_LOOKBACK)



import random
import re
import time
import contextvars


import datetime as dt
import math
import statistics
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List, Any

### MID AUTO-TUNE (TP2 hit-rate control)
_MID_AUTOTUNE_ENABLED = os.getenv("MID_AUTOTUNE_ENABLED", "0").strip().lower() not in ("0","false","no","off")
_MID_AUTOTUNE_TARGET = float(os.getenv("MID_AUTOTUNE_TARGET", "0.70") or 0.70)
_MID_AUTOTUNE_TOL = float(os.getenv("MID_AUTOTUNE_TOL", "0.03") or 0.03)
_MID_AUTOTUNE_ALPHA = float(os.getenv("MID_AUTOTUNE_ALPHA", "0.10") or 0.10)  # EMA speed (~20 trades memory)
_MID_AUTOTUNE_MIN_TRADES = int(os.getenv("MID_AUTOTUNE_MIN_TRADES", "20") or 20)
_MID_AUTOTUNE_EVERY_N = int(os.getenv("MID_AUTOTUNE_EVERY_N", "5") or 5)
_MID_AUTOTUNE_K_ATR = float(os.getenv("MID_AUTOTUNE_K_ATR", "0.25") or 0.25)
_MID_AUTOTUNE_K_RR = float(os.getenv("MID_AUTOTUNE_K_RR", "0.06") or 0.06)

_MID_AUTOTUNE_ATR_MIN_SPOT = float(os.getenv("MID_AUTOTUNE_ATR_MIN_SPOT", "1.8") or 1.8)
_MID_AUTOTUNE_ATR_MAX_SPOT = float(os.getenv("MID_AUTOTUNE_ATR_MAX_SPOT", "3.0") or 3.0)
_MID_AUTOTUNE_ATR_MIN_FUT = float(os.getenv("MID_AUTOTUNE_ATR_MIN_FUT", "1.6") or 1.6)
_MID_AUTOTUNE_ATR_MAX_FUT = float(os.getenv("MID_AUTOTUNE_ATR_MAX_FUT", "2.6") or 2.6)

_MID_AUTOTUNE_DISC_MIN = float(os.getenv("MID_AUTOTUNE_DISC_MIN", "0.78") or 0.78)
_MID_AUTOTUNE_DISC_MAX = float(os.getenv("MID_AUTOTUNE_DISC_MAX", "0.94") or 0.94)

_MID_AUTOTUNE_PATH = Path(os.getenv("MID_AUTOTUNE_PATH", str(Path(__file__).with_name("mid_autotune.json"))))

# In-memory cache for MID autotune state.
# Kept sync-friendly because scanner code uses it from non-async contexts.
_MID_AUTOTUNE_CACHE: dict = {}


def _parse_seconds_env(name: str, default: float) -> float:
    """Parse seconds-like environment values (supports optional trailing 's')."""
    raw = os.getenv(name, "").strip()
    if raw == "":
        return float(default)
    if raw.lower().endswith("s"):
        raw = raw[:-1].strip()
    try:
        return float(raw)
    except Exception:
        return float(default)

def _clamp(v: float, lo: float, hi: float) -> float:
    try:
        return float(max(lo, min(hi, float(v))))
    except Exception:
        return float(lo)

def _mid_autotune_load() -> dict:
    """Load autotune state.

    Since v2, state is per-market:
      { "spot": {...}, "futures": {...} }
    For backward compatibility, a legacy flat dict is migrated into both buckets.
    """
    global _MID_AUTOTUNE_CACHE
    # Fast path: already loaded into memory.
    if isinstance(_MID_AUTOTUNE_CACHE, dict) and (_MID_AUTOTUNE_CACHE.get("spot") or _MID_AUTOTUNE_CACHE.get("futures")):
        return _MID_AUTOTUNE_CACHE

    st: Optional[dict] = None
    try:
        if _MID_AUTOTUNE_PATH.exists():
            st = json.loads(_MID_AUTOTUNE_PATH.read_text(encoding="utf-8"))
    except Exception:
        st = None

    if isinstance(st, dict):
        # v2 format
        if "spot" in st or "futures" in st:
            if "spot" not in st:
                st["spot"] = {"p_ema": None, "n": 0, "since": 0, "params": {}}
            if "futures" not in st:
                st["futures"] = {"p_ema": None, "n": 0, "since": 0, "params": {}}
            _MID_AUTOTUNE_CACHE = st
            return _MID_AUTOTUNE_CACHE

        # legacy flat format
        if any(k in st for k in ("p_ema", "n", "since", "params")):
            legacy = {
                "p_ema": st.get("p_ema", None),
                "n": int(st.get("n") or 0),
                "since": int(st.get("since") or 0),
                "params": dict(st.get("params") or {}),
            }
            _MID_AUTOTUNE_CACHE = {"spot": dict(legacy), "futures": dict(legacy)}
            return _MID_AUTOTUNE_CACHE

    _MID_AUTOTUNE_CACHE = {
        "spot": {"p_ema": None, "n": 0, "since": 0, "params": {}},
        "futures": {"p_ema": None, "n": 0, "since": 0, "params": {}},
    }
    return _MID_AUTOTUNE_CACHE

def _mid_autotune_save(state: dict) -> None:
    try:
        _MID_AUTOTUNE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _MID_AUTOTUNE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def _mid_autotune_bucket(st: dict, market: str) -> dict:
    m = (market or "").strip().lower()
    if m.startswith("fut"):
        key = "futures"
    else:
        key = "spot"
    b = st.get(key)
    if not isinstance(b, dict):
        b = {"p_ema": None, "n": 0, "since": 0, "params": {}}
        st[key] = b
    # normalize fields
    b.setdefault("p_ema", None)
    b.setdefault("n", 0)
    b.setdefault("since", 0)
    if "params" not in b or not isinstance(b.get("params"), dict):
        b["params"] = {}
    return b

def _mid_autotune_get_param(name: str, default: float, market: str = "spot") -> float:
    # tuned value overrides env if enabled
    if not _MID_AUTOTUNE_ENABLED:
        return float(os.getenv(name, str(default)) or default)
    st = _mid_autotune_load()
    b = _mid_autotune_bucket(st, market)
    try:
        pv = b.get("params", {}).get(name, None)
        if pv is not None:
            return float(pv)
    except Exception:
        pass
    return float(os.getenv(name, str(default)) or default)

def _mid_autotune_is_mid_trade(orig_text: str, timeframe: str) -> bool:
    try:
        if timeframe in ("5m","30m","1h"):
            return True
        t = (orig_text or "").upper()
        return (" MID" in t) or ("[MID]" in t) or ("TF:5M" in t) or ("TF: 5M" in t)
    except Exception:
        return False

async def _mid_autotune_update_on_close(*, market: str, orig_text: str, timeframe: str, tp2_present: bool, hit_tp2: bool) -> None:
    global _MID_AUTOTUNE_CACHE
    if not _MID_AUTOTUNE_ENABLED:
        return
    if not tp2_present:
        return
    if not _mid_autotune_is_mid_trade(orig_text, timeframe):
        return

    # Prefer Postgres KV state if available, but keep an in-memory cache for sync scanner calls.
    st = _mid_autotune_load()
    try:
        import db_store as _db_store
        if _db_store.get_pool() is not None:
            kv = await _db_store.kv_get_json("mid_autotune")
            if isinstance(kv, dict):
                st = kv
                # refresh cache
                _MID_AUTOTUNE_CACHE = st
    except Exception:
        pass
    b = _mid_autotune_bucket(st, market)
    n = int(b.get("n") or 0)
    since = int(b.get("since") or 0)

    # EMA update
    p_prev = b.get("p_ema", None)
    x = 1.0 if hit_tp2 else 0.0
    if p_prev is None:
        p = x
    else:
        p = (1.0 - _MID_AUTOTUNE_ALPHA) * float(p_prev) + _MID_AUTOTUNE_ALPHA * x

    n += 1
    since += 1
    # BUGFIX: p_ema is per-market bucket, not at the root.
    b["p_ema"] = float(p)
    b["n"] = n
    b["since"] = since

    # Refresh cache for sync readers.
    _MID_AUTOTUNE_CACHE = st

    # tune periodically
    if n < _MID_AUTOTUNE_MIN_TRADES or since < _MID_AUTOTUNE_EVERY_N:
        _mid_autotune_save(st)
        try:
            import db_store as _db_store
            if _db_store.get_pool() is not None:
                await _db_store.kv_set_json("mid_autotune", st)
        except Exception:
            pass
        return

    target = _MID_AUTOTUNE_TARGET
    e = target - float(p)
    if abs(e) < _MID_AUTOTUNE_TOL:
        b["since"] = 0
        _mid_autotune_save(st)
        try:
            import db_store as _db_store
            if _db_store.get_pool() is not None:
                await _db_store.kv_set_json("mid_autotune", st)
        except Exception:
            pass
        return

    params = dict(b.get("params") or {})

    # Defaults if not set yet
    disc = float(params.get("MID_RR_DISCOUNT", os.getenv("MID_RR_DISCOUNT", "0.86") or 0.86))
    max_tp2_atr = float(params.get("MID_MAX_TP2_ATR", os.getenv("MID_MAX_TP2_ATR", "2.2") or 2.2))

    mkt = (market or "FUTURES").upper()
    if mkt == "SPOT":
        atr_min, atr_max = _MID_AUTOTUNE_ATR_MIN_SPOT, _MID_AUTOTUNE_ATR_MAX_SPOT
    else:
        atr_min, atr_max = _MID_AUTOTUNE_ATR_MIN_FUT, _MID_AUTOTUNE_ATR_MAX_FUT

    # Main lever: TP2 ATR cap (multiplicative)
    max_tp2_atr = _clamp(max_tp2_atr * math.exp(_MID_AUTOTUNE_K_ATR * e), atr_min, atr_max)
    # Secondary lever: RR discount (additive)
    disc = _clamp(disc + _MID_AUTOTUNE_K_RR * e, _MID_AUTOTUNE_DISC_MIN, _MID_AUTOTUNE_DISC_MAX)

    params["MID_MAX_TP2_ATR"] = float(max_tp2_atr)
    params["MID_RR_DISCOUNT"] = float(disc)

    # Optional: tighten ADX thresholds if still missing target and already at minimum greediness
    try:
        min_adx_1h = float(params.get("MID_MIN_ADX_1H", os.getenv("MID_MIN_ADX_1H", "22") or 22))
        min_adx_30m = float(params.get("MID_MIN_ADX_30M", os.getenv("MID_MIN_ADX_30M", "20") or 20))
    except Exception:
        min_adx_1h, min_adx_30m = 22.0, 20.0

    if float(p) < (target - 0.05) and max_tp2_atr <= (atr_min + 0.05) and disc <= (_MID_AUTOTUNE_DISC_MIN + 0.01):
        min_adx_1h = _clamp(min_adx_1h + 1.0, 0.0, 60.0)
        min_adx_30m = _clamp(min_adx_30m + 1.0, 0.0, 60.0)
        params["MID_MIN_ADX_1H"] = float(min_adx_1h)
        params["MID_MIN_ADX_30M"] = float(min_adx_30m)

    b["params"] = params
    b["since"] = 0
    _MID_AUTOTUNE_CACHE = st
    _mid_autotune_save(st)
    try:
        import db_store as _db_store
        if _db_store.get_pool() is not None:
            await _db_store.kv_set_json("mid_autotune", st)
    except Exception:
        pass


import aiohttp
import hmac
import hashlib
import uuid
import base64
import urllib.parse
import numpy as np
import pandas as pd


def _mid_norm_ohlcv(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Normalize OHLCV columns to lower-case open/high/low/close/(volume).
    Returns cleaned df or None if schema is unusable.
    """
    try:
        if df is None:
            return None
        if not hasattr(df, "empty"):
            return None
        if df.empty:
            # Empty frame is not a schema error; just return it.
            return df.copy()
        d = df.copy()
        # normalize column names
        cols = list(d.columns)
        lower_map = {c: str(c).strip().lower() for c in cols}
        d.rename(columns=lower_map, inplace=True)
        # common aliases
        alias = {
            "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume",
            "vol": "volume", "qty": "volume", "quote_volume": "volume",
            "openprice": "open", "highprice": "high", "lowprice": "low", "closeprice": "close",
        }
        for k, v in alias.items():
            if k in d.columns and v not in d.columns:
                d.rename(columns={k: v}, inplace=True)
        # some feeds use 'timestamp'/'time' for open_time_ms (optional)
        if "open_time_ms" not in d.columns:
            for k in ("open_time", "timestamp", "time", "t"):
                if k in d.columns:
                    try:
                        d["open_time_ms"] = d[k].astype("int64")
                        break
                    except Exception:
                        pass
        # validate required columns
        for req in ("open","high","low","close"):
            if req not in d.columns:
                return None
        # cast numeric
        for req in ("open","high","low","close"):
            d[req] = d[req].astype(float)
        if "volume" in d.columns:
            try:
                d["volume"] = d["volume"].astype(float)
            except Exception:
                pass
        return d
    except Exception:
        return None




import websockets
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from ta.volume import OnBalanceVolumeIndicator, MFIIndicator
from zoneinfo import ZoneInfo

import logging

import db_store

from cryptography.fernet import Fernet

logger = logging.getLogger("crypto-signal")

# --- Top symbols cache & provider cooldown (prevents REST storms / bans) ---
TOP_SYMBOLS_TTL_SEC = int(os.getenv("TOP_SYMBOLS_TTL_SEC", "300") or "300")  # cache top list for N seconds
TOP_PROVIDER_COOLDOWN_SEC = int(os.getenv("TOP_PROVIDER_COOLDOWN_SEC", "90") or "90")
BINANCE_BAN_COOLDOWN_SEC = int(os.getenv("BINANCE_BAN_COOLDOWN_SEC", "900") or "900")
BYBIT_RATE_LIMIT_COOLDOWN_SEC = int(os.getenv("BYBIT_RATE_LIMIT_COOLDOWN_SEC", "120") or "120")

_TOP_SYMBOLS_CACHE: dict[str, Any] = {"expires_mono": 0.0, "symbols": [], "provider": ""}
_TOP_PROVIDER_COOLDOWN_UNTIL: dict[str, float] = {}  # provider -> monotonic timestamp

def _top_cache_get(n: int) -> list[str]:
    try:
        if TOP_SYMBOLS_TTL_SEC <= 0:
            return []
        if float(_TOP_SYMBOLS_CACHE.get("expires_mono") or 0.0) > time.monotonic():
            syms = list(_TOP_SYMBOLS_CACHE.get("symbols") or [])
            if n <= 0:
                return syms
            return syms[:n]
    except Exception:
        pass
    return []

def _top_cache_set(symbols: list[str], provider: str) -> None:
    try:
        if TOP_SYMBOLS_TTL_SEC <= 0:
            return
        _TOP_SYMBOLS_CACHE["symbols"] = list(symbols or [])
        _TOP_SYMBOLS_CACHE["provider"] = str(provider or "")
        _TOP_SYMBOLS_CACHE["expires_mono"] = time.monotonic() + float(TOP_SYMBOLS_TTL_SEC)
    except Exception:
        pass

def _top_set_provider_cooldown(provider: str, seconds: int) -> None:
    try:
        if seconds <= 0:
            return
        _TOP_PROVIDER_COOLDOWN_UNTIL[str(provider)] = time.monotonic() + float(seconds)
    except Exception:
        pass

def _top_provider_is_cooled(provider: str) -> bool:
    try:
        return float(_TOP_PROVIDER_COOLDOWN_UNTIL.get(str(provider)) or 0.0) > time.monotonic()
    except Exception:
        return False

def _top_cooldown_for_error(provider: str, err: Exception) -> int:
    """Decide cooldown based on error string (handles Binance IP ban & Bybit rate limit)."""
    try:
        s = str(err)
    except Exception:
        s = ""
    p = str(provider or "").upper()

    # Binance IP ban / request-weight ban: HTTP 418 and JSON code -1003 are typical
    if p == "BINANCE":
        if ("HTTP 418" in s) or ("-1003" in s) or ("IP banned" in s) or ("Way too much request weight" in s):
            return BINANCE_BAN_COOLDOWN_SEC
        # Too many requests / rate limit
        if ("HTTP 429" in s) or ("too many requests" in s.lower()):
            return max(60, TOP_PROVIDER_COOLDOWN_SEC)

    # Bybit rate limit
    if p == "BYBIT":
        if ("retCode=10006" in s) or ("Too many visits" in s):
            return BYBIT_RATE_LIMIT_COOLDOWN_SEC
        if ("HTTP 429" in s) or ("too many" in s.lower()):
            return max(30, TOP_PROVIDER_COOLDOWN_SEC)

    # Generic provider cooldown
    return TOP_PROVIDER_COOLDOWN_SEC


# --- MID summary heartbeat (repeat last MID tick summary so it doesn't get lost in Railway logs) ---
MID_LAST_SUMMARY = None
MID_LAST_SUMMARY_TS = 0.0

def _mid_set_last_summary(summary: str) -> None:
    global MID_LAST_SUMMARY, MID_LAST_SUMMARY_TS
    MID_LAST_SUMMARY = summary
    MID_LAST_SUMMARY_TS = time.time()

async def mid_summary_heartbeat_loop() -> None:
    """Periodically re-log the last MID 'tick done' summary for visibility in log streams."""
    try:
        every = int(os.getenv("MID_SUMMARY_EVERY_SEC", "0") or "0")
    except Exception:
        every = 0
    if every <= 0:
        return
    logger.info("[mid][summary] heartbeat enabled every=%ss", every)
    while True:
        await asyncio.sleep(every)
        if MID_LAST_SUMMARY:
            age = time.time() - float(MID_LAST_SUMMARY_TS or time.time())
            logger.info("[mid][summary] %s (age=%.0fs)", MID_LAST_SUMMARY, age)



# --- MID hard-block counter (for honest [mid][summary] metrics) ---
# Counts cases where evaluate_on_exchange_mid_v2() rejected a candidate via _mid_block_reason(...).
_MID_HARD_BLOCK_TOTAL = 0

def _mid_inc_hard_block(n: int = 1) -> None:
    global _MID_HARD_BLOCK_TOTAL
    try:
        _MID_HARD_BLOCK_TOTAL += int(n)
    except Exception:
        pass

def _mid_hard_block_total() -> int:
    try:
        return int(_MID_HARD_BLOCK_TOTAL)
    except Exception:
        return 0

# --- MID hard-block breakdown (per-tick) ---
# We keep counts of *which* _mid_block_reason(...) caused a hard block.
# Reset each MID tick by setting _MID_HARD_BLOCK_TICK_TOKEN to a new value.
_MID_HARD_BLOCK_TICK_TOKEN = None  # type: ignore
_MID_HARD_BLOCK_BY_KEY = defaultdict(int)  # key -> count
_MID_HARD_BLOCK_SAMPLES = defaultdict(list)  # key -> [sample strings]

def _mid_hardblock_key(reason: str) -> str:
    """Normalize a raw block reason into a compact bucket key."""
    try:
        r = (reason or "").strip()
        if not r:
            return "unknown"
        head = r.split()[0]  # first token
        # common forms: 'late_entry_atr=1.23', 'anti_bounce_short ...', 'rsi_short=..', 'below_ema20_5m ...'
        key = head.split("=", 1)[0].strip()
        if key.startswith("anti_bounce_"):
            return "anti_bounce"
        if key.startswith("rsi_"):
            return "rsi"
        if key in ("below_ema20_5m", "above_ema20_5m"):
            return "ema20_5m"
        if key.startswith("near_recent_"):
            return "near_recent_extreme"
        if key.startswith("mid_"):
            return "mid_error"
        return key
    except Exception:
        return "unknown"

def _mid_hardblock_reset_tick() -> None:
    global _MID_HARD_BLOCK_TICK_TOKEN, _MID_HARD_BLOCK_BY_KEY, _MID_HARD_BLOCK_SAMPLES
    try:
        _MID_HARD_BLOCK_TICK_TOKEN = str(uuid.uuid4())
        _MID_HARD_BLOCK_BY_KEY = defaultdict(int)
        _MID_HARD_BLOCK_SAMPLES = defaultdict(list)
    except Exception:
        _MID_HARD_BLOCK_TICK_TOKEN = "1"
        _MID_HARD_BLOCK_BY_KEY = defaultdict(int)
        _MID_HARD_BLOCK_SAMPLES = defaultdict(list)

def _mid_hardblock_track(reason: str, symbol: str | None = None) -> None:
    """Track per-tick hard-block buckets (safe to call from anywhere)."""
    try:
        if not _MID_HARD_BLOCK_TICK_TOKEN:
            return
        k = _mid_hardblock_key(str(reason))
        _MID_HARD_BLOCK_BY_KEY[k] += 1
        # keep few samples for log context
        try:
            maxs = int(os.getenv("MID_HARDBLOCK_SAMPLES", "2") or "2")
        except Exception:
            maxs = 2
        if maxs > 0:
            arr = _MID_HARD_BLOCK_SAMPLES[k]
            if len(arr) < maxs:
                if symbol:
                    arr.append(f"{symbol}:{reason}")
                else:
                    arr.append(str(reason))
    except Exception:
        pass

def _mid_hardblock_dump(max_keys: int = 6) -> str:
    """Return compact breakdown string like: late_entry_atr=40, vwap_dist_atr=25, anti_bounce=18"""
    try:
        items = sorted([(k, int(v)) for k, v in _MID_HARD_BLOCK_BY_KEY.items()], key=lambda x: (-x[1], x[0]))
        items = items[:max(1, int(max_keys))]
        return ", ".join([f"{k}={v}" for k, v in items])
    except Exception:
        return ""

# --- MID trap digest sink (aggregates "MID blocked (trap)" into a periodic digest) ---

# Trap log dedup (avoid spamming the same message every second)
_MID_TRAP_LAST_LOG: Dict[str, float] = {}

# Per-tick log suppression (avoid repeating [mid][block]/[mid][trap] many times for same symbol)
_MID_TICK_CTX = contextvars.ContextVar('MID_TICK_CTX', default=None)


_MID_ADAPT_CTX = contextvars.ContextVar('MID_ADAPT_CTX', default=None)

def _mid_adapt_reset_tick() -> None:
    """Reset adaptive regime stats for the current MID tick."""
    try:
        _MID_ADAPT_CTX.set({'n': 0, 'sum_atrp': 0.0, 'sum_t': 0.0, 'seen': set()})
    except Exception:
        pass

def _mid_adapt_track(atrp: float, t: float, symbol: str | None = None) -> None:
    """Track per-symbol ATR% + normalized regime parameter (t in [0..1]).
    If symbol is provided, counts each symbol only once per tick (even if evaluated on many venues).
    """
    try:
        ctx = _MID_ADAPT_CTX.get()
        if not isinstance(ctx, dict):
            return
        sym = (str(symbol or '').strip().upper()) if symbol is not None else ''
        if sym:
            seen = ctx.get('seen')
            if not isinstance(seen, set):
                seen = set()
                ctx['seen'] = seen
            if sym in seen:
                return
            seen.add(sym)
        ctx['n'] = int(ctx.get('n') or 0) + 1
        ctx['sum_atrp'] = float(ctx.get('sum_atrp') or 0.0) + float(atrp or 0.0)
        ctx['sum_t'] = float(ctx.get('sum_t') or 0.0) + float(t or 0.0)
    except Exception:
        return


def _mid_adapt_regime_from_t(t: float) -> str:
    try:
        tt = float(t)
    except Exception:
        tt = 0.5
    if tt <= 0.33:
        return "LOW"
    if tt >= 0.66:
        return "HIGH"
    return "MID"

def _mid_adapt_snapshot() -> Optional[Tuple[str, float, float, int]]:
    """Return (regime, avg_atrp, avg_t, n) for current tick, or None."""
    try:
        ctx = _MID_ADAPT_CTX.get()
        if not isinstance(ctx, dict):
            return None
        n = int(ctx.get('n') or 0)
        if n <= 0:
            return None
        avg_atrp = float(ctx.get('sum_atrp') or 0.0) / float(n)
        avg_t = float(ctx.get('sum_t') or 0.0) / float(n)
        regime = _mid_adapt_regime_from_t(avg_t)
        return (regime, float(avg_atrp), float(avg_t), n)
    except Exception:
        return None


def _mid_trap_log_cooldown_sec() -> float:
    """Cooldown (seconds) for MID log de-duplication.

    Priority:
      1) MID_LOG_DEDUP_SEC (new, applies to ALL MID log types: trap/block/emit)
      2) MID_TRAP_LOG_COOLDOWN_SEC (legacy, kept for backward compatibility)
    """
    try:
        v = (os.getenv("MID_LOG_DEDUP_SEC") or "").strip()
        if v:
            return float(v)
        return float(os.getenv("MID_TRAP_LOG_COOLDOWN_SEC", "10") or 10)
    except Exception:
        return 10.0



def _mid_trap_should_log(key: str) -> bool:
    try:
        now = time.time()
        cd = max(0.0, float(_mid_trap_log_cooldown_sec()))
        if cd <= 0:
            return True
        last = _MID_TRAP_LAST_LOG.get(key, 0.0)
        if now - last < cd:
            return False
        _MID_TRAP_LAST_LOG[key] = now
        # light purge
        if len(_MID_TRAP_LAST_LOG) > 2000:
            cutoff = now - max(cd, 30.0)
            for k, ts in list(_MID_TRAP_LAST_LOG.items())[:500]:
                if ts < cutoff:
                    _MID_TRAP_LAST_LOG.pop(k, None)
        return True
    except Exception:
        return True
# Bot can register a sink callback that receives structured trap events.
_MID_TRAP_SINK = None  # type: ignore
def set_mid_trap_sink(cb):
    """Register a callable that receives trap events: cb(event: dict)."""
    global _MID_TRAP_SINK
    _MID_TRAP_SINK = cb

def _emit_mid_trap_event(event: dict) -> None:
    try:
        cb = _MID_TRAP_SINK
        if cb:
            cb(event)
    except Exception:
        pass

def _mid_trap_reason_key(reason: str) -> str:
    try:
        r = (reason or "").strip()
        if not r:
            return "unknown"
        return r.split()[0].strip()
    except Exception:
        return "unknown"

# ------------------ Stablecoin pair blocking (scanner + autotrade) ------------------
# Blocks stable-vs-stable pairs like USDCUSDT / DAIUSDT / USD1USDT, etc.
# Configure:
#   BLOCK_STABLECOIN_PAIRS=1 (default)
#   STABLECOINS=USDT,USDC,DAI,BUSD,TUSD,FDUSD,USDP,PYUSD,FRAX,EURC,USD1
#   BLOCKED_SYMBOLS=USDCUSDT,BUSDUSDT,DAIUSDT,USD1USDT  (optional additional hard blocks)

def _env_csv_set(name: str, default_csv: str) -> set[str]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        raw = default_csv
    return {p.strip().upper() for p in raw.split(",") if p and p.strip()}

_BLOCK_STABLECOIN_PAIRS = (os.getenv("BLOCK_STABLECOIN_PAIRS", "1").strip().lower() not in ("0","false","no","off"))

_STABLECOINS = _env_csv_set(
    "STABLECOINS",
    "USDT,USDC,DAI,BUSD,TUSD,FDUSD,USDP,PYUSD,FRAX,EURC,USD1"
)

_BLOCKED_SYMBOLS = _env_csv_set(
    "BLOCKED_SYMBOLS",
    ""
)

# Sort by length to match e.g. USD1 before USD
_STABLECOINS_SORTED = sorted(_STABLECOINS, key=len, reverse=True)

def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").upper().strip()
    s = s.replace("-", "").replace("_", "").replace("/", "")
    return s

def _split_base_quote(symbol: str) -> tuple[str, str]:
    """Split a concatenated symbol into (base, quote) when quote matches a known stablecoin."""
    s = _normalize_symbol(symbol)
    for q in _STABLECOINS_SORTED:
        if s.endswith(q) and len(s) > len(q):
            return (s[:-len(q)], q)
    return (s, "")

def is_blocked_symbol(symbol: str) -> bool:
    s = _normalize_symbol(symbol)
    if s in _BLOCKED_SYMBOLS:
        return True
    if not _BLOCK_STABLECOIN_PAIRS:
        return False
    base, quote = _split_base_quote(s)
    if quote and base in _STABLECOINS:
        return True
    return False



# ------------------ Auto-trade exchange API helpers ------------------

class ExchangeAPIError(RuntimeError):
    """Raised when an exchange API call fails (network / auth / rate limit / etc.)."""


def _extract_signal_id(sig: object) -> int:
    """Extract a stable, non-null signal id from a signal object."""
    try:
        v = getattr(sig, "signal_id", None)
        if v is None:
            v = getattr(sig, "id", None)
        v = int(v)
        return v if v > 0 else 0
    except Exception:
        return 0


def _should_deactivate_key(err_text: str) -> bool:
    """Return True only for credential/permission/IP-whitelist failures."""
    t = (err_text or "").lower()
    needles = (
        "invalid api-key",
        "api-key invalid",
        "invalid api key",
        "invalid key",
        "invalid signature",
        "signature for this request is not valid",
        "signature mismatch",
        "permission denied",
        "not authorized",
        "no permission",
        "apikey permission",
        "trade permission",
        "ip",
        "whitelist",
    )
    return any(n in t for n in needles)


# ------------------ Smart FUTURES Cap (effective cap) ------------------
# In-memory cache: last effective cap per user (resets on restart).
_LAST_EFFECTIVE_FUT_CAP: dict[int, float] = {}
_LAST_EFFECTIVE_FUT_CAP_NOTIFY_AT: dict[int, float] = {}

def _smartcap_env_float(name: str, default: float) -> float:
    try:
        v = (os.getenv(name) or "").strip()
        return float(v) if v else float(default)
    except Exception:
        return float(default)

def _smartcap_env_int(name: str, default: int) -> int:
    try:
        v = (os.getenv(name) or "").strip()
        return int(v) if v else int(default)
    except Exception:
        return int(default)

_SMARTCAP_STEP_USDT = max(1.0, _smartcap_env_float("EFF_CAP_STEP_USDT", 10.0))
_SMARTCAP_COOLDOWN_SEC = max(60, _smartcap_env_int("EFF_CAP_NOTIFY_COOLDOWN_SEC", 1800))  # 30 min
_SMARTCAP_DECREASE_MIN_USDT = max(0.0, _smartcap_env_float("EFF_CAP_DECREASE_MIN_USDT", 10.0))
_SMARTCAP_BASE_PCT = min(0.95, max(0.1, _smartcap_env_float("EFF_CAP_BASE_PCT", 0.65)))  # base cap = balance * 0.65
_SMARTCAP_HARD_MAX_PCT = min(0.98, max(0.3, _smartcap_env_float("EFF_CAP_HARD_MAX_PCT", 0.85)))  # never use >85% of balance

# ------------------ Virtual SL/TP mode (price-watched) ------------------
# When enabled, the bot does NOT place TP/SL orders on the exchange.
# It opens an entry MARKET order and then manages TP/SL/BE virtually by watching price and sending MARKET closes.
_VIRTUAL_SLTP_ALL = (os.getenv("VIRTUAL_SLTP_ALL", "1").strip().lower() not in ("0","false","no","off"))

# When enabled, PRO management is applied even when TP/SL orders were placed on the exchange.
# The manager will cancel existing TP/SL child orders (when supported) and manage the position like virtual mode.
_SMART_PRO_CONTROL_REAL = (os.getenv("SMART_PRO_CONTROL_REAL", "1").strip().lower() not in ("0","false","no","off"))


# Break-even fee buffer (percent). Example: 0.05 means +0.05% for LONG, -0.05% for SHORT.
_BE_FEE_BUFFER_PCT = float(os.getenv("BE_FEE_BUFFER_PCT", "0.05") or 0.05)

# Stop-loss confirmation (seconds) and buffer (percent).
# SL_CONFIRM_SEC: require price to remain beyond SL for N seconds before closing.
# SL_BUFFER_PCT: extra buffer around SL in percent (0.03 means 0.03%).
_SL_CONFIRM_SEC = int(float(os.getenv("SL_CONFIRM_SEC", "0") or 0))
_SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0") or 0.0)



# =======================
# Smart Trade Manager PRO — ENV (SAFE DEFAULTS)
# These globals MUST exist because some parts of the code (e.g., track_loop)
# reference SMART_* names directly. If an env var is missing or malformed,
# we fall back to safe defaults (no NameError, no crash).
# =======================

def _smartpro_env_float(name: str, default: float) -> float:
    try:
        v = (os.getenv(name) or "").strip()
        return float(v) if v else float(default)
    except Exception:
        return float(default)

def _smartpro_env_int(name: str, default: int) -> int:
    try:
        v = (os.getenv(name) or "").strip()
        return int(float(v)) if v else int(default)
    except Exception:
        return int(default)

def _smartpro_env_bool(name: str, default: bool) -> bool:
    try:
        v = (os.getenv(name) or "").strip().lower()
        if not v:
            return bool(default)
        return v not in ("0", "false", "no", "off")
    except Exception:
        return bool(default)


def _is_mid_tf(tf: str) -> bool:
    tf = (tf or "").strip().lower()
    # MID timeframe usually looks like "5m/30m/1h"
    return tf.startswith("5m/") or "5m/30m/1h" in tf

def _is_mid_signal(sig: "Signal") -> bool:
    try:
        return _is_mid_tf(getattr(sig, "timeframe", "") or "")
    except Exception:
        return False

def _env_float_mid(base: str, default: float, is_mid: bool) -> float:
    """Read env BASE or BASE_MID if is_mid. BASE_MID wins when present."""
    if is_mid:
        v = os.getenv(f"{base}_MID")
        if v is not None and str(v).strip() != "":
            try:
                return float(v)
            except Exception:
                pass
    v = os.getenv(base)
    if v is not None and str(v).strip() != "":
        try:
            return float(v)
        except Exception:
            return float(default)
    return float(default)

def _env_int_mid(base: str, default: int, is_mid: bool) -> int:
    if is_mid:
        v = os.getenv(f"{base}_MID")
        if v is not None and str(v).strip() != "":
            try:
                return int(float(v))
            except Exception:
                pass
    v = os.getenv(base)
    if v is not None and str(v).strip() != "":
        try:
            return int(float(v))
        except Exception:
            return int(default)
    return int(default)

# --- TP1 / TP2 decision engine ---
SMART_TP2_PROB_STRONG = _smartpro_env_float("SMART_TP2_PROB_STRONG", 0.72)
SMART_TP2_PROB_MED = _smartpro_env_float("SMART_TP2_PROB_MED", 0.45)
SMART_TP1_PARTIAL_PCT = _smartpro_env_float("SMART_TP1_PARTIAL_PCT", 0.50)
SMART_FORCE_FULL_TP1_IF_NO_TP2 = _smartpro_env_bool("SMART_FORCE_FULL_TP1_IF_NO_TP2", True)

# --- Early-exit / momentum ---
SMART_MOMENTUM_WINDOW_SEC = _smartpro_env_int("SMART_MOMENTUM_WINDOW_SEC", 30)
SMART_EARLY_EXIT_MOM_NEG_PCT = _smartpro_env_float("SMART_EARLY_EXIT_MOM_NEG_PCT", 0.16)
SMART_EARLY_EXIT_CONSEC_NEG = _smartpro_env_int("SMART_EARLY_EXIT_CONSEC_NEG", 2)
SMART_EARLY_EXIT_MIN_GAIN_PCT = _smartpro_env_float("SMART_EARLY_EXIT_MIN_GAIN_PCT", 0.12)

# --- Dynamic BE ---
SMART_BE_DELAY_SEC = _smartpro_env_int("SMART_BE_DELAY_SEC", 25)
SMART_BE_MIN_PCT = _smartpro_env_float("SMART_BE_MIN_PCT", 0.06)
SMART_BE_MAX_PCT = _smartpro_env_float("SMART_BE_MAX_PCT", 0.45)
SMART_BE_VOL_MULT = _smartpro_env_float("SMART_BE_VOL_MULT", 0.35)
SMART_BE_ARM_PCT_TO_TP2 = _smartpro_env_float("SMART_BE_ARM_PCT_TO_TP2", 0.15)

# --- Peak -> reversal exit ---
SMART_PEAK_MIN_GAIN_PCT = _smartpro_env_float("SMART_PEAK_MIN_GAIN_PCT", 0.42)
SMART_REVERSAL_EXIT_PCT = _smartpro_env_float("SMART_REVERSAL_EXIT_PCT", 0.32)

# --- Safety controls ---
SMART_ARM_SL_AFTER_PCT = _smartpro_env_float("SMART_ARM_SL_AFTER_PCT", 0.22)
SMART_HARD_SL_PCT = _smartpro_env_float("SMART_HARD_SL_PCT", 2.80)

# --- Generic loop tick / debounce (if referenced) ---
SMART_TICK = _smartpro_env_float("SMART_TICK", 1.0)

# Normalize ranges to avoid accidental misconfig
if SMART_TP1_PARTIAL_PCT < 0.0:
    SMART_TP1_PARTIAL_PCT = 0.0
if SMART_TP1_PARTIAL_PCT > 1.0:
    SMART_TP1_PARTIAL_PCT = 1.0
if SMART_BE_MAX_PCT < SMART_BE_MIN_PCT:
    SMART_BE_MAX_PCT = SMART_BE_MIN_PCT

def _be_with_fee_buffer(entry_price: float, *, direction: str) -> float:
    """Return BE price adjusted by fee buffer."""
    try:
        ep = float(entry_price or 0.0)
        if ep <= 0:
            return 0.0
        buf = max(0.0, float(_BE_FEE_BUFFER_PCT)) / 100.0
        d = (direction or "").upper().strip()
        if d == "SHORT":
            return ep * (1.0 - buf)
        return ep * (1.0 + buf)
    except Exception:
        return float(entry_price or 0.0)

def _smartcap_floor_step(x: float, step: float) -> float:
    try:
        x = float(x)
        step = float(step)
        if step <= 0:
            return max(0.0, x)
        return max(0.0, math.floor(x / step) * step)
    except Exception:
        return 0.0

def _smartcap_multiplier(winrate: float | None) -> float:
    try:
        if winrate is None:
            return 1.0
        wr = float(winrate)
    except Exception:
        return 1.0
    if wr < 40.0:
        return 0.6
    if wr < 50.0:
        return 0.8
    if wr < 60.0:
        return 1.0
    if wr < 70.0:
        return 1.1
    return 1.25

def _calc_effective_futures_cap(*, balance_usdt: float, ui_cap_usdt: float, winrate: float | None) -> float:
    """Compute effective futures cap (USDT margin limit) using balance + winrate.

    - base = balance * _SMARTCAP_BASE_PCT
    - multiply by winrate bucket
    - rounded DOWN to _SMARTCAP_STEP_USDT
    - capped by ui_cap (if >0)
    - hard-capped by balance * _SMARTCAP_HARD_MAX_PCT (rounded down)
    """
    bal = max(0.0, float(balance_usdt or 0.0))
    ui = max(0.0, float(ui_cap_usdt or 0.0))
    base = bal * _SMARTCAP_BASE_PCT
    eff = base * _smartcap_multiplier(winrate)
    eff = _smartcap_floor_step(eff, _SMARTCAP_STEP_USDT)
    hard = _smartcap_floor_step(bal * _SMARTCAP_HARD_MAX_PCT, _SMARTCAP_STEP_USDT)
    if hard > 0:
        eff = min(eff, hard)
    if ui > 0:
        eff = min(eff, ui)
    return float(max(0.0, eff))



async def _binance_signed_request(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    path: str,
    method: str,
    api_key: str,
    api_secret: str,
    params: dict | None = None,
) -> dict:
    """Minimal Binance signed request helper (HMAC SHA256 query signature)."""
    params = dict(params or {})
    params.setdefault("timestamp", str(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)))
    query = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    url = f"{base_url}{path}?{query}&signature={sig}"
    headers = {"X-MBX-APIKEY": api_key}
    try:
        async with session.request(method.upper(), url, headers=headers) as r:
            data = await r.json(content_type=None)
            if r.status != 200:
                raise ExchangeAPIError(f"Binance API HTTP {r.status}: {data}")
            return data if isinstance(data, dict) else {"data": data}
    except ExchangeAPIError:
        raise
    except Exception as e:
        raise ExchangeAPIError(f"Binance API error: {e}")


async def _bybit_signed_request(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    path: str,
    method: str,
    api_key: str,
    api_secret: str,
    params: dict | None = None,
) -> dict:
    """Bybit V5 signing helper (HMAC SHA256). We use GET for validation."""
    params = dict(params or {})
    ts = str(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000))
    recv = "5000"
    query = urllib.parse.urlencode(params, doseq=True)
    pre = ts + api_key + recv + query
    sig = hmac.new(api_secret.encode("utf-8"), pre.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": sig,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv,
    }
    url = f"{base_url}{path}"
    if query:
        url += f"?{query}"
    try:
        async with session.request(method.upper(), url, headers=headers) as r:
            data = await r.json(content_type=None)
            if r.status != 200:
                raise ExchangeAPIError(f"Bybit API HTTP {r.status}: {data}")
            if isinstance(data, dict) and int(data.get("retCode", 0) or 0) != 0:
                raise ExchangeAPIError(f"Bybit API retCode {data.get('retCode')}: {data.get('retMsg')}")
            return data if isinstance(data, dict) else {"data": data}
    except ExchangeAPIError:
        raise
    except Exception as e:
        raise ExchangeAPIError(f"Bybit API error: {e}")


async def _bybit_v5_request(
    session: aiohttp.ClientSession,
    *,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: dict | None = None,
    json_body: dict | None = None,
    base_url: str = "https://api.bybit.com",
) -> dict:
    """Bybit V5 request helper for GET/POST with proper signing.

    Signature: HMAC_SHA256(secret, timestamp + apiKey + recvWindow + payload)
      - payload is queryString for GET
      - payload is JSON string for POST
    """
    params = dict(params or {})
    ts = str(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000))
    recv = "5000"
    method_u = method.upper()
    query = urllib.parse.urlencode(params, doseq=True)
    body_str = ""
    if json_body is not None:
        body_str = json.dumps(json_body, separators=(",", ":"), ensure_ascii=False)
    payload = query if method_u == "GET" else body_str
    pre = ts + api_key + recv + payload
    sig = hmac.new(api_secret.encode("utf-8"), pre.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": sig,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv,
        "Content-Type": "application/json",
    }
    url = f"{base_url}{path}"
    if query and method_u == "GET":
        url += f"?{query}"
    try:
        async with session.request(method_u, url, headers=headers, data=(body_str if method_u != "GET" else None)) as r:
            data = await r.json(content_type=None)
            if r.status != 200:
                raise ExchangeAPIError(f"Bybit API HTTP {r.status}: {data}")
            if isinstance(data, dict) and int(data.get("retCode", 0) or 0) != 0:
                raise ExchangeAPIError(f"Bybit API retCode {data.get('retCode')}: {data.get('retMsg')}")
            return data if isinstance(data, dict) else {"data": data}
    except ExchangeAPIError:
        raise
    except Exception as e:
        raise ExchangeAPIError(f"Bybit API error: {e}")



# ------------------ OKX / MEXC / Gate.io signed request helpers (SPOT auto-trade) ------------------

def _okx_inst(symbol: str) -> str:
    # OKX uses BASE-QUOTE
    s = symbol.upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}-USDT"
    return s

def _gate_pair(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}_USDT"
    return s

async def _okx_signed_request(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    path: str,
    method: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    params: dict | None = None,
    json_body: dict | None = None,
) -> dict:
    ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    method_u = method.upper()
    query = ""
    if params:
        query = urllib.parse.urlencode(params, doseq=True)
    body_str = ""
    if json_body is not None:
        body_str = json.dumps(json_body, separators=(",", ":"), ensure_ascii=False)
    req_path = path + (("?" + query) if (query and method_u == "GET") else "")
    prehash = ts + method_u + req_path + (body_str if method_u != "GET" else "")
    digest = hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    sign = base64.b64encode(digest).decode("utf-8")
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    url = base_url + req_path if method_u == "GET" else (base_url + path + (("?" + query) if query else ""))
    async with session.request(method_u, url, headers=headers, data=(body_str if method_u != "GET" else None)) as r:
        data = await r.json(content_type=None)
        if r.status != 200:
            raise ExchangeAPIError(f"OKX API HTTP {r.status}: {data}")
        # OKX returns {"code":"0",...} on success
        if isinstance(data, dict) and str(data.get("code")) not in ("0", "200", "OK", ""):
            raise ExchangeAPIError(f"OKX API code {data.get('code')}: {data.get('msg')}")
        return data if isinstance(data, dict) else {"data": data}

async def _mexc_signed_request(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    path: str,
    method: str,
    api_key: str,
    api_secret: str,
    params: dict | None = None,
) -> dict:
    params = dict(params or {})
    params.setdefault("timestamp", str(int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)))
    query = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    url = f"{base_url}{path}?{query}&signature={sig}"
    headers = {"X-MEXC-APIKEY": api_key}
    async with session.request(method.upper(), url, headers=headers) as r:
        data = await r.json(content_type=None)
        if r.status != 200:
            raise ExchangeAPIError(f"MEXC API HTTP {r.status}: {data}")
        return data if isinstance(data, dict) else {"data": data}

async def _gateio_signed_request(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    path: str,
    method: str,
    api_key: str,
    api_secret: str,
    params: dict | None = None,
    json_body: dict | None = None,
) -> dict:
    # Gate.io v4 signing: https://www.gate.io/docs/developers/apiv4/en/#authentication
    method_u = method.upper()
    query = urllib.parse.urlencode(params or {}, doseq=True)
    body_str = ""
    if json_body is not None:
        body_str = json.dumps(json_body, separators=(",", ":"), ensure_ascii=False)
    ts = str(int(dt.datetime.now(dt.timezone.utc).timestamp()))
    hashed_payload = hashlib.sha512(body_str.encode("utf-8")).hexdigest()
    sign_str = "\n".join([method_u, path, query, hashed_payload, ts])
    sig = hmac.new(api_secret.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha512).hexdigest()
    headers = {"KEY": api_key, "SIGN": sig, "Timestamp": ts, "Content-Type": "application/json"}
    url = f"{base_url}{path}"
    if query:
        url += "?" + query
    async with session.request(method_u, url, headers=headers, data=(body_str if json_body is not None else None)) as r:
        data = await r.json(content_type=None)
        if r.status >= 300:
            raise ExchangeAPIError(f"Gate.io API HTTP {r.status}: {data}")
        return data if isinstance(data, dict) else {"data": data}

async def _okx_spot_market_buy(*, api_key: str, api_secret: str, passphrase: str, symbol: str, quote_usdt: float) -> dict:
    inst = _okx_inst(symbol)
    body = {"instId": inst, "tdMode": "cash", "side": "buy", "ordType": "market", "sz": str(float(quote_usdt))}
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _okx_signed_request(s, base_url="https://www.okx.com", path="/api/v5/trade/order", method="POST",
                                         api_key=api_key, api_secret=api_secret, passphrase=passphrase, json_body=body)

async def _okx_spot_market_sell(*, api_key: str, api_secret: str, passphrase: str, symbol: str, base_qty: float) -> dict:
    inst = _okx_inst(symbol)
    body = {"instId": inst, "tdMode": "cash", "side": "sell", "ordType": "market", "sz": str(float(base_qty))}
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _okx_signed_request(s, base_url="https://www.okx.com", path="/api/v5/trade/order", method="POST",
                                         api_key=api_key, api_secret=api_secret, passphrase=passphrase, json_body=body)

async def _mexc_spot_market_buy(*, api_key: str, api_secret: str, symbol: str, quote_usdt: float) -> dict:
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _mexc_signed_request(s, base_url="https://api.mexc.com", path="/api/v3/order", method="POST",
                                          api_key=api_key, api_secret=api_secret,
                                          params={"symbol": symbol.upper(), "side": "BUY", "type": "MARKET", "quoteOrderQty": str(float(quote_usdt))})

async def _mexc_spot_market_sell(*, api_key: str, api_secret: str, symbol: str, base_qty: float) -> dict:
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _mexc_signed_request(s, base_url="https://api.mexc.com", path="/api/v3/order", method="POST",
                                          api_key=api_key, api_secret=api_secret,
                                          params={"symbol": symbol.upper(), "side": "SELL", "type": "MARKET", "quantity": str(float(base_qty))})

async def _gateio_spot_market_buy(*, api_key: str, api_secret: str, symbol: str, quote_usdt: float) -> dict:
    pair = _gate_pair(symbol)
    # Gate.io market buy uses amount in quote currency in some accounts; we attempt quote-based "amount" with "account": "spot"
    body = {"currency_pair": pair, "type": "market", "side": "buy", "account": "spot", "amount": str(float(quote_usdt))}
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _gateio_signed_request(s, base_url="https://api.gateio.ws", path="/api/v4/spot/orders", method="POST",
                                            api_key=api_key, api_secret=api_secret, json_body=body)

async def _gateio_spot_market_sell(*, api_key: str, api_secret: str, symbol: str, base_qty: float) -> dict:
    pair = _gate_pair(symbol)
    body = {"currency_pair": pair, "type": "market", "side": "sell", "account": "spot", "amount": str(float(base_qty))}
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _gateio_signed_request(s, base_url="https://api.gateio.ws", path="/api/v4/spot/orders", method="POST",
                                            api_key=api_key, api_secret=api_secret, json_body=body)

async def _okx_public_price(symbol: str) -> float:
    inst = _okx_inst(symbol)
    data = await _http_json("GET", "https://www.okx.com/api/v5/market/ticker", params={"instId": inst}, timeout_s=8)
    lst = (data.get("data") or [])
    if lst and isinstance(lst, list) and isinstance(lst[0], dict):
        return float(lst[0].get("last") or 0.0)
    return 0.0

async def _mexc_public_price(symbol: str) -> float:
    data = await _http_json("GET", "https://api.mexc.com/api/v3/ticker/price", params={"symbol": symbol.upper()}, timeout_s=8)
    return float(data.get("price") or 0.0)

async def _gateio_public_price(symbol: str) -> float:
    pair = _gate_pair(symbol)
    data = await _http_json("GET", "https://api.gateio.ws/api/v4/spot/tickers", params={"currency_pair": pair}, timeout_s=8)
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return float(data[0].get("last") or 0.0)
    return 0.0


async def validate_autotrade_keys(
    *,
    exchange: str,
    market_type: str,
    api_key: str,
    api_secret: str,
    passphrase: str | None = None,
) -> dict:
    """Validate keys for READ + TRADE permissions (best-effort, no real orders).

    Returns:
      {"ok": bool, "read_ok": bool, "trade_ok": bool, "error": str|None}
    """
    ex = (exchange or "binance").lower().strip()
    mt = (market_type or "spot").lower().strip()
    if ex not in ("binance", "bybit", "okx", "mexc", "gateio"):
        return {"ok": False, "read_ok": False, "trade_ok": False, "error": "unsupported_exchange"}
    if mt not in ("spot", "futures"):
        mt = "spot"

    api_key = (api_key or "").strip()
    api_secret = (api_secret or "").strip()
    if not api_key or not api_secret:
        return {"ok": False, "read_ok": False, "trade_ok": False, "error": "missing_keys"}

    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            # ---------------- Binance ----------------
            if ex == "binance":
                if mt == "spot":
                    acc = await _binance_signed_request(
                        s,
                        base_url="https://api.binance.com",
                        path="/api/v3/account",
                        method="GET",
                        api_key=api_key,
                        api_secret=api_secret,
                        params={"recvWindow": "5000"},
                    )
                    perms = acc.get("permissions")
                    perms_set: set[str] = set()
                    if isinstance(perms, (list, tuple)):
                        perms_set = {str(x).upper() for x in perms}
                    elif isinstance(perms, str):
                        perms_set = {p.strip().upper() for p in perms.split(",") if p.strip()}

                    if perms_set:
                        trade_ok = ("SPOT" in perms_set) or ("MARGIN" in perms_set) or ("TRADE" in perms_set)
                    else:
                        can_trade = acc.get("canTrade")
                        trade_ok = True if (can_trade is None) else bool(can_trade)

                    if not trade_ok:
                        return {"ok": True, "read_ok": True, "trade_ok": False, "error": "trade_permission_missing"}
                    return {"ok": True, "read_ok": True, "trade_ok": True, "error": None}

                # futures
                acc = await _binance_signed_request(
                    s,
                    base_url="https://fapi.binance.com",
                    path="/fapi/v2/account",
                    method="GET",
                    api_key=api_key,
                    api_secret=api_secret,
                    params={"recvWindow": "5000"},
                )
                can_trade = acc.get("canTrade")
                if can_trade is not None and not bool(can_trade):
                    return {"ok": True, "read_ok": True, "trade_ok": False, "error": "trade_permission_missing"}
                return {"ok": True, "read_ok": True, "trade_ok": True, "error": None}

            # ---------------- Bybit ----------------
            if ex == "bybit":
                data = await _bybit_signed_request(
                    s,
                    base_url="https://api.bybit.com",
                    path="/v5/user/query-api",
                    method="GET",
                    api_key=api_key,
                    api_secret=api_secret,
                    params={},
                )
                res = data.get("result") or {}
                if isinstance(res, dict):
                    ro = res.get("readOnly")
                    if ro is not None and int(ro) == 1:
                        return {"ok": True, "read_ok": True, "trade_ok": False, "error": "trade_permission_missing"}
                    perm = res.get("permissions") or res.get("permission")
                    if isinstance(perm, dict):
                        trade_flag = perm.get("Trade") or perm.get("trade") or perm.get("SpotTrade") or perm.get("ContractTrade")
                        if trade_flag is not None and not bool(trade_flag):
                            return {"ok": True, "read_ok": True, "trade_ok": False, "error": "trade_permission_missing"}
                return {"ok": True, "read_ok": True, "trade_ok": True, "error": None}

            # ---------------- OKX ----------------
            if ex == "okx":
                if not passphrase:
                    return {"ok": False, "read_ok": False, "trade_ok": False, "error": "missing_passphrase"}
                await _okx_signed_request(
                    s,
                    base_url="https://www.okx.com",
                    path="/api/v5/account/balance",
                    method="GET",
                    api_key=api_key,
                    api_secret=api_secret,
                    passphrase=passphrase,
                    params={"ccy": "USDT"},
                )
                return {"ok": True, "read_ok": True, "trade_ok": True, "error": None}

            # ---------------- MEXC ----------------
            if ex == "mexc":
                await _mexc_signed_request(
                    s,
                    base_url="https://api.mexc.com",
                    path="/api/v3/account",
                    method="GET",
                    api_key=api_key,
                    api_secret=api_secret,
                    params={},
                )
                return {"ok": True, "read_ok": True, "trade_ok": True, "error": None}

            # ---------------- Gate.io ----------------
            if ex == "gateio":
                await _gateio_signed_request(
                    s,
                    base_url="https://api.gateio.ws",
                    path="/api/v4/spot/accounts",
                    method="GET",
                    api_key=api_key,
                    api_secret=api_secret,
                    params={},
                )
                return {"ok": True, "read_ok": True, "trade_ok": True, "error": None}

        return {"ok": False, "read_ok": False, "trade_ok": False, "error": "unsupported_exchange"}

    except ExchangeAPIError as e:
        return {"ok": False, "read_ok": False, "trade_ok": False, "error": str(e)}


def _autotrade_fernet() -> Fernet:
    k = (os.getenv("AUTOTRADE_MASTER_KEY") or "").strip()
    if not k:
        raise RuntimeError("AUTOTRADE_MASTER_KEY env is missing")
    return Fernet(k.encode("utf-8"))


def _decrypt_token(token: str | None) -> str:
    if not token:
        return ""
    f = _autotrade_fernet()
    return f.decrypt(token.encode("utf-8")).decode("utf-8")


async def _http_json(method: str, url: str, *, params: dict | None = None, json_body: dict | None = None, headers: dict | None = None, timeout_s: int = 10) -> dict:
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.request(method.upper(), url, params=params, json=json_body, headers=headers) as r:
            data = await r.json(content_type=None)
            if r.status != 200:
                raise ExchangeAPIError(f"HTTP {r.status} {url}: {data}")
            return data if isinstance(data, dict) else {"data": data}


def _round_step(qty: float, step: float) -> float:
    """Round quantity down to the nearest step."""
    if step <= 0:
        return float(qty)
    return math.floor(float(qty) / float(step)) * float(step)


def _round_tick(price: float, tick: float) -> float:
    """Round price down to the nearest tick."""
    if tick <= 0:
        return float(price)
    return math.floor(float(price) / float(tick)) * float(tick)


_BINANCE_INFO_CACHE: dict[str, dict] = {}


async def _binance_exchange_info(*, futures: bool) -> dict:
    key = "futures" if futures else "spot"
    if key in _BINANCE_INFO_CACHE and _BINANCE_INFO_CACHE[key].get("_ts", 0) > time.time() - 3600:
        return _BINANCE_INFO_CACHE[key]
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo" if futures else "https://api.binance.com/api/v3/exchangeInfo"
    data = await _http_json("GET", url, timeout_s=10)
    data["_ts"] = time.time()
    _BINANCE_INFO_CACHE[key] = data
    return data


def _binance_symbol_filters(info: dict, symbol: str) -> tuple[float, float, float, float]:
    """Return (qty_step, min_qty, tick_size, min_notional) for symbol. Best-effort."""
    sym = symbol.upper()
    for s in info.get("symbols", []) or []:
        if str(s.get("symbol")).upper() == sym:
            step = 0.0
            min_qty = 0.0
            tick = 0.0
            min_notional = 0.0
            for f in s.get("filters", []) or []:
                if f.get("filterType") == "LOT_SIZE":
                    step = float(f.get("stepSize") or 0.0)
                    min_qty = float(f.get("minQty") or 0.0)
                elif f.get("filterType") == "PRICE_FILTER":
                    tick = float(f.get("tickSize") or 0.0)
                elif f.get("filterType") in ("MIN_NOTIONAL", "NOTIONAL"):
                    # spot often uses MIN_NOTIONAL; some endpoints expose NOTIONAL
                    mn = f.get("minNotional")
                    if mn is None:
                        mn = f.get("notional")
                    try:
                        min_notional = float(mn or 0.0)
                    except Exception:
                        pass
            return step, min_qty, tick, min_notional
    return 0.0, 0.0, 0.0, 0.0


async def _binance_price(symbol: str, *, futures: bool) -> float:
    sym = symbol.upper()
    url = "https://fapi.binance.com/fapi/v1/ticker/price" if futures else "https://api.binance.com/api/v3/ticker/price"
    data = await _http_json("GET", url, params={"symbol": sym}, timeout_s=8)
    return float(data.get("price") or 0.0)



# ------------------ Generic order sizing helpers ------------------

_RL_LOG_TS: dict[str, float] = {}

def _log_rate_limited(key: str, message: str, *, every_s: int = 60, level: str = "info") -> None:
    """Log at most once per `every_s` seconds per key."""
    try:
        now = time.time()
        last = _RL_LOG_TS.get(key, 0.0)
        if now - last < float(every_s):
            return
        _RL_LOG_TS[key] = now
        if level == "debug":
            logger.debug(message)
        elif level == "warning":
            logger.warning(message)
        elif level == "error":
            logger.error(message)
        else:
            logger.info(message)
    except Exception:
        # Logging must never break trading loop
        pass


_OKX_FILTER_CACHE: dict[str, dict] = {}
async def _okx_instrument_filters(*, inst_type: str, symbol: str) -> tuple[float, float, float]:
    """Return (qty_step, min_qty, tick) for OKX instruments (best-effort)."""
    key = f"{inst_type}:{symbol.upper()}"
    cached = _OKX_FILTER_CACHE.get(key)
    if cached and cached.get("_ts", 0) > time.time() - 3600:
        return float(cached.get("qty_step") or 0.0), float(cached.get("min_qty") or 0.0), float(cached.get("tick") or 0.0)

    inst = _okx_inst(symbol)
    url = "https://www.okx.com/api/v5/public/instruments"
    data = await _http_json("GET", url, params={"instType": inst_type, "instId": inst}, timeout_s=10)
    lst = ((data.get("data") or []) if isinstance(data, dict) else [])
    qty_step = 0.0
    min_qty = 0.0
    tick = 0.0
    if lst and isinstance(lst, list) and isinstance(lst[0], dict):
        it = lst[0]
        # OKX uses lotSz / minSz / tickSz strings
        try:
            qty_step = float(it.get("lotSz") or 0.0)
        except Exception:
            qty_step = 0.0
        try:
            min_qty = float(it.get("minSz") or 0.0)
        except Exception:
            min_qty = 0.0
        try:
            tick = float(it.get("tickSz") or 0.0)
        except Exception:
            tick = 0.0

    _OKX_FILTER_CACHE[key] = {"_ts": time.time(), "qty_step": qty_step, "min_qty": min_qty, "tick": tick}
    return qty_step, min_qty, tick


_MEXC_FILTER_CACHE: dict[str, dict] = {}
async def _mexc_symbol_filters(symbol: str) -> tuple[float, float, float, float]:
    """Return (qty_step, min_qty, tick_size, min_notional) for MEXC spot symbol (best-effort)."""
    sym = symbol.upper()
    cached = _MEXC_FILTER_CACHE.get(sym)
    if cached and cached.get("_ts", 0) > time.time() - 3600:
        return float(cached.get("qty_step") or 0.0), float(cached.get("min_qty") or 0.0), float(cached.get("tick") or 0.0), float(cached.get("min_notional") or 0.0)

    url = "https://api.mexc.com/api/v3/exchangeInfo"
    data = await _http_json("GET", url, params={"symbol": sym}, timeout_s=10)
    step = 0.0
    min_qty = 0.0
    tick = 0.0
    min_notional = 0.0
    for s in data.get("symbols", []) or []:
        if str(s.get("symbol") or "").upper() != sym:
            continue
        for f in s.get("filters", []) or []:
            ft = f.get("filterType")
            if ft == "LOT_SIZE":
                try:
                    step = float(f.get("stepSize") or 0.0)
                except Exception:
                    step = 0.0
                try:
                    min_qty = float(f.get("minQty") or 0.0)
                except Exception:
                    min_qty = 0.0
            elif ft == "PRICE_FILTER":
                try:
                    tick = float(f.get("tickSize") or 0.0)
                except Exception:
                    tick = 0.0
            elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                mn = f.get("minNotional")
                if mn is None:
                    mn = f.get("notional")
                try:
                    min_notional = float(mn or 0.0)
                except Exception:
                    min_notional = 0.0
        break

    _MEXC_FILTER_CACHE[sym] = {"_ts": time.time(), "qty_step": step, "min_qty": min_qty, "tick": tick, "min_notional": min_notional}
    return step, min_qty, tick, min_notional


_GATE_FILTER_CACHE: dict[str, dict] = {}
async def _gate_symbol_filters(symbol: str) -> tuple[float, float, float, float]:
    """Return (qty_step, min_qty, tick, min_notional) for Gate.io spot symbol (best-effort)."""
    pair = _gate_pair(symbol)
    cached = _GATE_FILTER_CACHE.get(pair)
    if cached and cached.get("_ts", 0) > time.time() - 3600:
        return float(cached.get("qty_step") or 0.0), float(cached.get("min_qty") or 0.0), float(cached.get("tick") or 0.0), float(cached.get("min_notional") or 0.0)

    url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
    data = await _http_json("GET", url, params={"currency_pair": pair}, timeout_s=10)
    # Gate returns either a dict (single) or list
    it = None
    if isinstance(data, dict):
        it = data
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        it = data[0]

    qty_step = 0.0
    min_qty = 0.0
    tick = 0.0
    min_notional = 0.0
    if isinstance(it, dict):
        # amount_precision defines qty step = 10^-precision
        try:
            prec = int(it.get("amount_precision") or 0)
            if prec > 0:
                qty_step = 10 ** (-prec)
        except Exception:
            qty_step = 0.0
        try:
            min_qty = float(it.get("min_base_amount") or 0.0)
        except Exception:
            min_qty = 0.0
        try:
            min_notional = float(it.get("min_quote_amount") or 0.0)
        except Exception:
            min_notional = 0.0
        try:
            # tick size isn't always provided; leave 0 if absent
            tick = float(it.get("price_precision") or 0.0)
        except Exception:
            tick = 0.0

    _GATE_FILTER_CACHE[pair] = {"_ts": time.time(), "qty_step": qty_step, "min_qty": min_qty, "tick": tick, "min_notional": min_notional}
    return qty_step, min_qty, tick, min_notional


async def _normalize_close_qty(*, ex: str, mt: str, symbol: str, qty: float, px: float) -> float:
    """Best-effort: round down by step, and ensure >= min qty and >= min notional floor (spot 15 USDT, futures 10 USDT)."""
    q = max(0.0, float(qty))
    if q <= 0 or float(px or 0.0) <= 0:
        return 0.0

    min_usdt_floor = 15.0 if mt == "spot" else 10.0
    qty_step = 0.0
    min_qty = 0.0
    min_notional_ex = 0.0

    try:
        if ex == "binance":
            info = await _binance_exchange_info(futures=(mt == "futures"))
            qty_step, min_qty, _tick, mn = _binance_symbol_filters(info, symbol)
            min_notional_ex = float(mn or 0.0)
        elif ex == "bybit":
            qty_step, min_qty, _tick = await _bybit_instrument_filters(category=("linear" if mt == "futures" else "spot"), symbol=symbol)
        elif ex == "okx":
            # OKX uses instrument metadata; most bots here use spot only
            qty_step, min_qty, _tick = await _okx_instrument_filters(inst_type=("SWAP" if mt == "futures" else "SPOT"), symbol=symbol)
        elif ex == "mexc":
            qty_step, min_qty, _tick, mn = await _mexc_symbol_filters(symbol)
            min_notional_ex = float(mn or 0.0)
        else:
            qty_step, min_qty, _tick, mn = await _gate_symbol_filters(symbol)
            min_notional_ex = float(mn or 0.0)
    except Exception as e:
        _log_rate_limited(f"norm_filters:{ex}:{mt}:{symbol}", f"[SMART] filters fetch failed {ex}/{mt} {symbol}: {e}", every_s=300, level="debug")

    if qty_step and qty_step > 0:
        q = _round_step(q, qty_step)

    if min_qty and min_qty > 0 and q < min_qty:
        return 0.0

    min_notional = max(float(min_usdt_floor), float(min_notional_ex or 0.0))
    if min_notional > 0 and (q * float(px)) < min_notional:
        return 0.0

    return float(q)


# ------------------ Bybit trading helpers (V5, unified API) ------------------

async def _bybit_price(symbol: str, *, category: str) -> float:
    sym = symbol.upper()
    data = await _http_json(
        "GET",
        "https://api.bybit.com/v5/market/tickers",
        params={"category": category, "symbol": sym},
        timeout_s=10,
    )
    lst = (data.get("result") or {}).get("list") or []
    if lst and isinstance(lst, list) and isinstance(lst[0], dict):
        return float(lst[0].get("lastPrice") or 0.0)
    return 0.0


_BYBIT_INFO_CACHE: dict[str, dict] = {}


async def _bybit_instrument_filters(*, category: str, symbol: str) -> tuple[float, float, float]:
    """Return (qty_step, min_qty, tick_size) for a Bybit V5 instrument."""
    key = f"{category}:{symbol.upper()}"
    cached = _BYBIT_INFO_CACHE.get(key)
    if cached and cached.get("_ts", 0) > time.time() - 3600:
        return float(cached.get("qty_step") or 0.0), float(cached.get("min_qty") or 0.0), float(cached.get("tick") or 0.0)

    data = await _http_json(
        "GET",
        "https://api.bybit.com/v5/market/instruments-info",
        params={"category": category, "symbol": symbol.upper()},
        timeout_s=10,
    )
    lst = ((data.get("result") or {}).get("list") or [])
    qty_step = 0.0
    min_qty = 0.0
    tick = 0.0
    if lst and isinstance(lst, list) and isinstance(lst[0], dict):
        it = lst[0]
        lot = it.get("lotSizeFilter") or {}
        pf = it.get("priceFilter") or {}
        try:
            qty_step = float(lot.get("qtyStep") or 0.0)
        except Exception:
            qty_step = 0.0
        try:
            min_qty = float(lot.get("minOrderQty") or 0.0)
        except Exception:
            min_qty = 0.0
        try:
            tick = float(pf.get("tickSize") or 0.0)
        except Exception:
            tick = 0.0

    _BYBIT_INFO_CACHE[key] = {"_ts": time.time(), "qty_step": qty_step, "min_qty": min_qty, "tick": tick}
    return qty_step, min_qty, tick


async def _bybit_available_usdt(api_key: str, api_secret: str) -> float:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        data = await _bybit_v5_request(
            s,
            method="GET",
            path="/v5/account/wallet-balance",
            api_key=api_key,
            api_secret=api_secret,
            params={"accountType": "UNIFIED", "coin": "USDT"},
        )
        res = (data.get("result") or {})
        lst = res.get("list") or []
        if not lst:
            return 0.0
        coin = None
        # bybit nests coin list
        cl = lst[0].get("coin") if isinstance(lst[0], dict) else None
        if isinstance(cl, list) and cl:
            coin = cl[0]
        if isinstance(coin, dict):
            for k in ("availableToWithdraw", "walletBalance", "equity"):
                if coin.get(k) is not None:
                    try:
                        return float(coin.get(k) or 0.0)
                    except Exception:
                        pass
        # fallback total
        try:
            return float(lst[0].get("totalAvailableBalance") or 0.0)
        except Exception:
            return 0.0


async def _bybit_order_create(
    *,
    api_key: str,
    api_secret: str,
    category: str,
    symbol: str,
    side: str,
    order_type: str,
    qty: float,
    price: float | None = None,
    reduce_only: bool | None = None,
    trigger_price: float | None = None,
    close_on_trigger: bool | None = None,
) -> dict:
    body: dict[str, Any] = {
        "category": category,
        "symbol": symbol.upper(),
        "side": "Buy" if side.upper() in ("BUY", "LONG") else "Sell",
        "orderType": order_type,
        "qty": str(float(qty)),
        "timeInForce": "GTC",
    }
    if price is not None:
        body["price"] = str(float(price))
    if reduce_only is not None:
        body["reduceOnly"] = bool(reduce_only)
    if trigger_price is not None:
        body["triggerPrice"] = str(float(trigger_price))
    if close_on_trigger is not None:
        body["closeOnTrigger"] = bool(close_on_trigger)
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _bybit_v5_request(
            s,
            method="POST",
            path="/v5/order/create",
            api_key=api_key,
            api_secret=api_secret,
            json_body=body,
        )


async def _bybit_order_status(*, api_key: str, api_secret: str, category: str, symbol: str, order_id: str) -> dict:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _bybit_v5_request(
            s,
            method="GET",
            path="/v5/order/realtime",
            api_key=api_key,
            api_secret=api_secret,
            params={"category": category, "symbol": symbol.upper(), "orderId": str(order_id)},
        )


async def _bybit_cancel_order(*, api_key: str, api_secret: str, category: str, symbol: str, order_id: str) -> None:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        await _bybit_v5_request(
            s,
            method="POST",
            path="/v5/order/cancel",
            api_key=api_key,
            api_secret=api_secret,
            json_body={"category": category, "symbol": symbol.upper(), "orderId": str(order_id)},
        )


async def _binance_spot_free_usdt(api_key: str, api_secret: str) -> float:
    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        acc = await _binance_signed_request(
            s,
            base_url="https://api.binance.com",
            path="/api/v3/account",
            method="GET",
            api_key=api_key,
            api_secret=api_secret,
            params={"recvWindow": "5000"},
        )
        for b in acc.get("balances", []) or []:
            if str(b.get("asset")) == "USDT":
                return float(b.get("free") or 0.0)
    return 0.0


async def _binance_futures_available_margin(api_key: str, api_secret: str) -> float:
    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        acc = await _binance_signed_request(
            s,
            base_url="https://fapi.binance.com",
            path="/fapi/v2/account",
            method="GET",
            api_key=api_key,
            api_secret=api_secret,
            params={"recvWindow": "5000"},
        )
        # availableBalance is the best signal for free margin
        try:
            return float(acc.get("availableBalance") or 0.0)
        except Exception:
            return 0.0


async def _binance_spot_market_buy_quote(
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    quote_usdt: float,
) -> dict:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _binance_signed_request(
            s,
            base_url="https://api.binance.com",
            path="/api/v3/order",
            method="POST",
            api_key=api_key,
            api_secret=api_secret,
            params={
                "symbol": symbol.upper(),
                "side": "BUY",
                "type": "MARKET",
                "quoteOrderQty": f"{quote_usdt:.8f}",
                "newOrderRespType": "FULL",
                "recvWindow": "5000",
            },
        )


async def _binance_spot_market_sell_base(
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    qty: float,
) -> dict:
    """Emergency spot close (SELL MARKET by base quantity)."""
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _binance_signed_request(
            s,
            base_url="https://api.binance.com",
            path="/api/v3/order",
            method="POST",
            api_key=api_key,
            api_secret=api_secret,
            params={
                "symbol": symbol.upper(),
                "side": "SELL",
                "type": "MARKET",
                "quantity": f"{float(qty):.8f}",
                "newOrderRespType": "FULL",
                "recvWindow": "5000",
            },
        )


async def _binance_spot_limit_sell(
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    qty: float,
    price: float,
) -> dict:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _binance_signed_request(
            s,
            base_url="https://api.binance.com",
            path="/api/v3/order",
            method="POST",
            api_key=api_key,
            api_secret=api_secret,
            params={
                "symbol": symbol.upper(),
                "side": "SELL",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": f"{qty:.8f}",
                "price": f"{price:.8f}",
                "recvWindow": "5000",
            },
        )


async def _binance_spot_stop_loss_limit_sell(
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    qty: float,
    stop_price: float,
) -> dict:
    # For STOP_LOSS_LIMIT, Binance requires both stopPrice and price.
    limit_price = stop_price * 0.999
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _binance_signed_request(
            s,
            base_url="https://api.binance.com",
            path="/api/v3/order",
            method="POST",
            api_key=api_key,
            api_secret=api_secret,
            params={
                "symbol": symbol.upper(),
                "side": "SELL",
                "type": "STOP_LOSS_LIMIT",
                "timeInForce": "GTC",
                "quantity": f"{qty:.8f}",
                "price": f"{limit_price:.8f}",
                "stopPrice": f"{stop_price:.8f}",
                "recvWindow": "5000",
            },
        )


async def _binance_futures_set_leverage(*, api_key: str, api_secret: str, symbol: str, leverage: int) -> None:
    lev = int(leverage or 1)
    if lev < 1:
        lev = 1
    if lev > 125:
        lev = 125
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        await _binance_signed_request(
            s,
            base_url="https://fapi.binance.com",
            path="/fapi/v1/leverage",
            method="POST",
            api_key=api_key,
            api_secret=api_secret,
            params={"symbol": symbol.upper(), "leverage": str(lev), "recvWindow": "5000"},
        )


async def _binance_futures_market_open(
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    side: str,
    qty: float,
) -> dict:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _binance_signed_request(
            s,
            base_url="https://fapi.binance.com",
            path="/fapi/v1/order",
            method="POST",
            api_key=api_key,
            api_secret=api_secret,
            params={
                "symbol": symbol.upper(),
                "side": side.upper(),
                "type": "MARKET",
                "quantity": f"{qty:.8f}",
                "recvWindow": "5000",
            },
        )


async def _binance_futures_reduce_limit(
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    side: str,
    qty: float,
    price: float,
) -> dict:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _binance_signed_request(
            s,
            base_url="https://fapi.binance.com",
            path="/fapi/v1/order",
            method="POST",
            api_key=api_key,
            api_secret=api_secret,
            params={
                "symbol": symbol.upper(),
                "side": side.upper(),
                "type": "LIMIT",
                "timeInForce": "GTC",
                "reduceOnly": "true",
                "quantity": f"{qty:.8f}",
                "price": f"{price:.8f}",
                "recvWindow": "5000",
            },
        )


async def _binance_futures_reduce_market(
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    side: str,
    qty: float,
) -> dict:
    """Reduce-only MARKET order (emergency close / partial close)."""
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _binance_signed_request(
            s,
            base_url="https://fapi.binance.com",
            path="/fapi/v1/order",
            method="POST",
            api_key=api_key,
            api_secret=api_secret,
            params={
                "symbol": symbol.upper(),
                "side": side.upper(),
                "type": "MARKET",
                "reduceOnly": "true",
                "quantity": f"{float(qty):.8f}",
                "recvWindow": "5000",
            },
        )


async def _binance_futures_stop_market_close_all(
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    side: str,
    stop_price: float,
) -> dict:
    # side is the close side (opposite to entry): SELL closes long, BUY closes short.
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _binance_signed_request(
            s,
            base_url="https://fapi.binance.com",
            path="/fapi/v1/order",
            method="POST",
            api_key=api_key,
            api_secret=api_secret,
            params={
                "symbol": symbol.upper(),
                "side": side.upper(),
                "type": "STOP_MARKET",
                "stopPrice": f"{stop_price:.8f}",
                "closePosition": "true",
                "recvWindow": "5000",
            },
        )


async def _binance_order_status(
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    order_id: int,
    futures: bool,
) -> dict:
    timeout = aiohttp.ClientTimeout(total=10)
    base_url = "https://fapi.binance.com" if futures else "https://api.binance.com"
    path = "/fapi/v1/order" if futures else "/api/v3/order"
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await _binance_signed_request(
            s,
            base_url=base_url,
            path=path,
            method="GET",
            api_key=api_key,
            api_secret=api_secret,
            params={"symbol": symbol.upper(), "orderId": str(int(order_id)), "recvWindow": "5000"},
        )


async def _binance_cancel_order(*, api_key: str, api_secret: str, symbol: str, order_id: int, futures: bool) -> None:
    timeout = aiohttp.ClientTimeout(total=10)
    base_url = "https://fapi.binance.com" if futures else "https://api.binance.com"
    path = "/fapi/v1/order" if futures else "/api/v3/order"
    async with aiohttp.ClientSession(timeout=timeout) as s:
        await _binance_signed_request(
            s,
            base_url=base_url,
            path=path,
            method="DELETE",
            api_key=api_key,
            api_secret=api_secret,
            params={"symbol": symbol.upper(), "orderId": str(int(order_id)), "recvWindow": "5000"},
        )


async def autotrade_execute(user_id: int, sig: "Signal") -> dict:
    """Execute real trading orders for a signal for a single user.

    Returns dict:
      {"ok": bool, "skipped": bool, "api_error": str|None}
    """
    uid = int(user_id)
    market = (getattr(sig, "market", "") or "").upper()
    if market not in ("SPOT", "FUTURES"):
        return {"ok": False, "skipped": True, "api_error": None, "skip_reason": "bad_market", "details": {"market": market}}

    def _skip(reason: str, **details) -> dict:
        return {"ok": False, "skipped": True, "api_error": None, "skip_reason": reason, "details": details or {}}

    def _fail(api_error: str, **details) -> dict:
        return {"ok": False, "skipped": False, "api_error": api_error, "details": details or {}}


    # Hard block: stable-vs-stable pairs (fail-safe)
    sym = (getattr(sig, "symbol", "") or "").upper().strip()
    if is_blocked_symbol(sym):
        logger.warning("[autotrade] blocked stable pair skip: %s", sym)
        return _skip("blocked_symbol", symbol=sym)

    st = await db_store.get_autotrade_settings(uid)
    mt = "spot" if market == "SPOT" else "futures"


    # Extract TA extras (ATR 5m) from confirmations JSON for smarter manager (trailing, near-TP2).
    def _sig_ta_float(name: str) -> float:
        try:
            raw = getattr(sig, "confirmations", "") or ""
            if not raw:
                return 0.0
            data = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(data, dict):
                # Some deployments store {"ta": {...}}; others store TA dict directly.
                ta = data.get("ta") if isinstance(data.get("ta"), dict) else data
                v = ta.get(name, 0.0) if isinstance(ta, dict) else 0.0
                return float(v or 0.0)
        except Exception:
            return 0.0
        return 0.0

    atr5_sig = _sig_ta_float("atr5")
    atr5_pct_sig = _sig_ta_float("atr5_pct")
    atr_pct_sig = _sig_ta_float("atr_pct")
    # --- Admin gate (like SIGNAL): global per-user Auto-trade allow/deny + expiry ---
    # This is independent from per-market toggles in autotrade_settings.
    # If disabled/expired/blocked -> skip silently.
    try:
        acc = await db_store.get_autotrade_access(uid)
        if bool(acc.get("is_blocked")):
            return _skip("skipped")
        if not bool(acc.get("autotrade_enabled")):
            return _skip("skipped")
        if bool(acc.get("autotrade_stop_after_close")):
            return _skip("skipped")
        exp = acc.get("autotrade_expires_at")
        if exp is not None:
            import datetime as _dt
            now = _dt.datetime.now(_dt.timezone.utc)
            try:
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=_dt.timezone.utc)
                if exp <= now:
                    return _skip("skipped")
            except Exception:
                return _skip("skipped")
    except Exception:
        # best-effort: if access columns not ready, default to allow
        pass

    enabled = bool(st.get("spot_enabled")) if mt == "spot" else bool(st.get("futures_enabled"))
    if not enabled:
        return _skip("skipped")

    
    exchange = str(st.get("spot_exchange" if mt == "spot" else "futures_exchange") or "binance").lower().strip()

    # SPOT: choose exchange based on user's priority intersecting with signal confirmations.
    # Fallback order is user-defined (1->2->3...), and we ONLY trade on exchanges that confirmed the signal.
    if mt == "spot":
        # Parse confirmations like "BYBIT+OKX" to {"bybit","okx"}
        conf_raw = str((getattr(sig, "available_exchanges", "") or getattr(sig, "confirmations", "") or "")).strip()
        conf_set: set[str] = set()
        # confirmations may arrive in UI formats like "Binance • Bybit • OKX".
        # include common separators (bullets, middle dot) to avoid false empty conf_set.
        for part in re.split(r"[+ ,;/|•·]+", conf_raw.strip()):
            p = part.strip().lower()
            if not p:
                continue
            # normalize common labels
            if p in ("binance", "bnb"):
                conf_set.add("binance")
            elif p in ("bybit", "byb"):
                conf_set.add("bybit")
            elif p in ("okx",):
                conf_set.add("okx")
            elif p in ("mexc",):
                conf_set.add("mexc")
            elif p in ("gateio", "gate", "gate.io", "gateio.ws"):
                conf_set.add("gateio")

        # Require at least one confirmation exchange; otherwise skip auto-trade
        if not conf_set:
            return {
                "ok": False,
                "skipped": True,
                "skip_reason": "no_confirmations",
                "details": {"confirmations_raw": conf_raw},
                "api_error": None,
            }


        # Priority list from DB (comma-separated)
        pr_csv = str(st.get("spot_exchange_priority") or "binance,bybit,okx,mexc,gateio")
        pr = [x.strip().lower() for x in pr_csv.split(",") if x.strip()]
        allowed = ["binance", "bybit", "okx", "mexc", "gateio"]
        pr2: list[str] = []
        for x in pr:
            if x in allowed and x not in pr2:
                pr2.append(x)
        for x in allowed:
            if x not in pr2:
                pr2.append(x)

        chosen = None
        # iterate user priority, but only those that confirmed signal
        for ex in pr2:
            if conf_set and ex not in conf_set:
                continue
            row = await db_store.get_autotrade_keys_row(user_id=uid, exchange=ex, market_type="spot")
            if not row or not bool(row.get("is_active")):
                continue
            # require both key and secret
            if not (row.get("api_key_enc") and row.get("api_secret_enc")):
                continue
            if ex == "okx" and not row.get("passphrase_enc"):
                # okx needs passphrase
                continue
            chosen = ex
            break

        if not chosen:
            return _skip("no_exchange_available", market=market)
        exchange = chosen

    # FUTURES: choose exchange based on where the futures contract actually exists
    # (BINANCE/BYBIT/OKX). If the signal provides a list of executable futures venues
    # in available_exchanges/confirmations, we enforce it. Otherwise we allow the
    # user-selected exchange as a best-effort fallback.
    if mt == "futures":
        fut_allowed = ["binance", "bybit", "okx"]

        # Parse venues like "BINANCE+OKX" -> {"binance","okx"}
        conf_raw = str((getattr(sig, "available_exchanges", "") or getattr(sig, "confirmations", "") or "")).strip()
        conf_set: set[str] = set()
        for part in re.split(r"[+ ,;/|•·]+", conf_raw.strip()):
            p = part.strip().lower()
            if not p:
                continue
            if p in ("binance", "bnb"):
                conf_set.add("binance")
            elif p in ("bybit", "byb"):
                conf_set.add("bybit")
            elif p in ("okx",):
                conf_set.add("okx")

        # Helper: does user have active futures keys for exchange?
        async def _has_fut_keys(ex: str) -> bool:
            try:
                row = await db_store.get_autotrade_keys_row(user_id=uid, exchange=ex, market_type="futures")
                if not row or not bool(row.get("is_active")):
                    return False
                if not (row.get("api_key_enc") and row.get("api_secret_enc")):
                    return False
                if ex == "okx" and not row.get("passphrase_enc"):
                    return False
                return True
            except Exception:
                return False

        # Normalize invalid selection
        if exchange not in fut_allowed:
            exchange = "binance"

        # Choose futures exchange by priority:
        # - user must have active futures keys
        # - futures contract must exist on that exchange (prevents 'Symbol Is Invalid')
        # - if signal provides confirmations/available_exchanges, enforce that subset
        async def _fut_contract_exists(ex: str, sym: str) -> bool:
            ex = (ex or "").lower()
            sym = (sym or "").upper().replace("/", "")
            try:
                if ex == "binance":
                    info = await _binance_exchange_info(futures=True)
                    for s in (info.get("symbols", []) or []):
                        if str(s.get("symbol") or "").upper() == sym:
                            stt = str(s.get("status") or "").upper()
                            return (stt in ("TRADING", "1")) or (stt == "")
                    return False
                if ex == "bybit":
                    data = await _http_json(
                        "GET",
                        "https://api.bybit.com/v5/market/instruments-info",
                        params={"category": "linear", "symbol": sym},
                        timeout_s=10,
                    )
                    lst = ((data.get("result") or {}).get("list") or [])
                    return bool(lst)
                if ex == "okx":
                    await _okx_instrument_filters(inst_type="SWAP", symbol=sym)
                    return True
            except Exception:
                return False
            return False

        candidates = [exchange] + [x for x in fut_allowed if x != exchange]
        if conf_set:
            candidates = [x for x in candidates if x in conf_set]

        chosen = None
        for ex in candidates:
            if not (await _has_fut_keys(ex)):
                continue
            if not (await _fut_contract_exists(ex, symbol)):
                continue
            chosen = ex
            break
        if not chosen:
            return _skip("no_exchange_available", market=market)
        exchange = chosen

    # amounts
    spot_amt = float(st.get("spot_amount_per_trade") or 0.0)
    fut_margin = float(st.get("futures_margin_per_trade") or 0.0)
    fut_cap = float(st.get("futures_cap") or 0.0)
    fut_lev = int(st.get("futures_leverage") or 1)

    need_usdt = spot_amt if mt == "spot" else fut_margin
    # MID positions are typically noisier; optionally reduce allocation via SMART_MID_ALLOC_MULT
    try:
        if _is_mid_signal(sig):
            mult = float(os.getenv("SMART_MID_ALLOC_MULT", "1.0") or 1.0)
            if mult > 0:
                need_usdt *= mult
    except Exception:
        pass

    # Hard minimums (validated in bot UI and DB, but re-checked here for safety)
    if mt == "spot" and 0 < need_usdt < 15:
        return _skip("skipped")
    if mt == "futures" and 0 < need_usdt < 10:
        return _skip("skipped")
    if need_usdt <= 0:
        return _skip("skipped")

    # fetch keys
    row = await db_store.get_autotrade_keys_row(user_id=uid, exchange=exchange, market_type=mt)
    if not row or not bool(row.get("is_active")):
        return _skip("skipped")
    try:
        api_key = _decrypt_token(row.get("api_key_enc"))
        api_secret = _decrypt_token(row.get("api_secret_enc"))
    except Exception as e:
        await db_store.mark_autotrade_key_error(
            user_id=uid,
            exchange=exchange,
            market_type=mt,
            error=f"decrypt_error: {e}",
            deactivate=True,
        )
        return {"ok": False, "skipped": True, "api_error": f"decrypt_error"}

    symbol = str(getattr(sig, "symbol", "") or "").upper().replace("/", "")
    if not symbol:
        return _skip("skipped")

    direction = str(getattr(sig, "direction", "LONG") or "LONG").upper()
    entry = float(getattr(sig, "entry", 0.0) or 0.0)
    sl = float(getattr(sig, "sl", 0.0) or 0.0)
    tp1 = float(getattr(sig, "tp1", 0.0) or 0.0)
    tp2 = float(getattr(sig, "tp2", 0.0) or 0.0)

    try:
        if exchange == "bybit":
            category = "spot" if mt == "spot" else "linear"
            # Futures cap is user-defined (by margin). Spot cap is implicit (wallet balance).
            free = await _bybit_available_usdt(api_key, api_secret)
            if free < need_usdt:
                return _skip("insufficient_balance", free_usdt=float(free), need_usdt=float(need_usdt), exchange=exchange, market=market)

            cap_decreased = False
            prev_eff_val = None
            winrate = None
            eff_cap = float(fut_cap or 0.0)

            if mt == "futures":
                used = await db_store.get_autotrade_used_usdt(uid, "futures")

                # Smart effective cap uses CURRENT available balance + winrate.
                try:
                    w = await db_store.get_autotrade_winrate(uid, market_type="futures", limit=20)
                    if int(w.get("n") or 0) >= 5:
                        winrate = float(w.get("winrate") or 0.0)
                except Exception:
                    winrate = None

                eff_cap = _calc_effective_futures_cap(balance_usdt=float(free), ui_cap_usdt=float(fut_cap), winrate=winrate)

                prev_eff = _LAST_EFFECTIVE_FUT_CAP.get(uid)
                _LAST_EFFECTIVE_FUT_CAP[uid] = float(eff_cap)
                if prev_eff is not None:
                    prev_eff_val = float(prev_eff)
                    if float(eff_cap) + 1e-9 < float(prev_eff):
                        dec = float(prev_eff) - float(eff_cap)
                        now_ts = time.time()
                        last_ts = float(_LAST_EFFECTIVE_FUT_CAP_NOTIFY_AT.get(uid) or 0.0)
                        if dec >= _SMARTCAP_DECREASE_MIN_USDT and (now_ts - last_ts) >= _SMARTCAP_COOLDOWN_SEC:
                            cap_decreased = True
                            _LAST_EFFECTIVE_FUT_CAP_NOTIFY_AT[uid] = now_ts

                if eff_cap > 0 and used + need_usdt > eff_cap:
                    return {
                        "ok": False, "skipped": True, "api_error": None,
                        "cap_ui": float(fut_cap), "cap_effective": float(eff_cap),
                        "cap_decreased": cap_decreased, "cap_prev_effective": prev_eff_val,
                        "winrate": (float(winrate) if winrate is not None else None),
                    }

            direction_local = direction
            if mt == "spot":
                # Spot: only long/buy is supported.
                side = "BUY"
            else:
                side = "BUY" if direction_local == "LONG" else "SELL"
            close_side = "SELL" if side == "BUY" else "BUY"

            px = await _bybit_price(symbol, category=category)
            if px <= 0:
                raise ExchangeAPIError("Bybit price=0")

            qty_step, min_qty, tick = await _bybit_instrument_filters(category=category, symbol=symbol)

            # Spot qty in base; Futures qty in contracts (approx via notional/price)
            if mt == "spot":
                qty = max(0.0, need_usdt / px)
            else:
                qty = max(0.0, (need_usdt * float(fut_lev)) / px)
            if qty_step > 0:
                qty = _round_step(qty, qty_step)
            if min_qty > 0 and qty < min_qty:
                raise ExchangeAPIError(f"Bybit qty<minQty after rounding: {qty} < {min_qty}")

            entry_res = await _bybit_order_create(
                api_key=api_key,
                api_secret=api_secret,
                category=category,
                symbol=symbol,
                side=side,
                order_type="Market",
                qty=qty,
                reduce_only=False if mt == "futures" else None,
            )
            order_id = ((entry_res.get("result") or {}).get("orderId") or (entry_res.get("result") or {}).get("orderId"))

            has_tp2 = tp2 > 0 and abs(tp2 - tp1) > 1e-12
            qty1 = _round_step(qty * (0.5 if has_tp2 else 1.0), qty_step)
            qty2 = _round_step(qty - qty1, qty_step) if has_tp2 else 0.0
            if has_tp2 and min_qty > 0 and qty2 < min_qty:
                has_tp2 = False
                qty1 = qty
                qty2 = 0.0

            # --- VIRTUAL mode: do not place SL/TP orders on exchange ---
            if _VIRTUAL_SLTP_ALL:
                entry_p = float(entry or 0.0)
                ref = {
                "signal_timeframe": getattr(sig, "timeframe", None),
                "signal_kind": ("MID" if _is_mid_signal(sig) else "MAIN"),
                    "virtual": True,
                    "exchange": "bybit",
                    "signal_timeframe": getattr(sig, "timeframe", None),
                    "signal_kind": ("MID" if _is_mid_signal(sig) else "MAIN"),
                    "market_type": mt,
                    "symbol": symbol,
                "atr_pct": float(atr_pct_sig) if mt == "futures" else 0.0,
                "atr5": float(atr5_sig) if mt == "futures" else 0.0,
                "atr5_pct": float(atr5_pct_sig) if mt == "futures" else 0.0,
                    "direction": direction,
                    "side": side,
                    "close_side": ("SELL" if str(side).upper() == "BUY" else "BUY"),
                    "entry_price": entry_p,
                    "qty": float(qty),
                    "tp1": float(tp1 or 0.0),
                    "tp2": float(tp2 or 0.0),
                    "sl": float(sl or 0.0),
                    "tp1_hit": False,
                    "be_moved": False,
                    "be_price": 0.0,
                }
                sig_id = _extract_signal_id(sig)
                if sig_id <= 0:
                    raise ExchangeAPIError("missing signal_id")
                await db_store.create_autotrade_position(
                    user_id=uid,
                    signal_id=sig_id,
                    exchange="bybit",
                    market_type=mt,
                    symbol=symbol,
                    side=side,
                    allocated_usdt=need_usdt,
                    api_order_ref=json.dumps(ref),
                )
                return {"ok": True, "skipped": False, "api_error": None}


            # Place SL first; if SL fails, immediately close to avoid unprotected exposure.
            try:
                sl_res = await _bybit_order_create(
                    api_key=api_key,
                    api_secret=api_secret,
                    category=category,
                    symbol=symbol,
                    side=close_side,
                    order_type="Market",
                    qty=qty,
                    reduce_only=(True if mt == "futures" else None),
                    trigger_price=_round_tick(sl, tick),
                    close_on_trigger=(True if mt == "futures" else None),
                )
            except Exception:
                try:
                    await _bybit_order_create(
                        api_key=api_key,
                        api_secret=api_secret,
                        category=category,
                        symbol=symbol,
                        side=close_side,
                        order_type="Market",
                        qty=qty,
                        reduce_only=(True if mt == "futures" else None),
                    )
                except Exception:
                    pass
                raise

            # TP(s) as limit reduce-only for futures; spot normal sell. If TP1 fails, close to avoid half-configured strategy.
            try:
                tp1_res = await _bybit_order_create(
                    api_key=api_key,
                    api_secret=api_secret,
                    category=category,
                    symbol=symbol,
                    side=close_side,
                    order_type="Limit",
                    qty=qty1,
                    price=_round_tick(tp1, tick),
                    reduce_only=(True if mt == "futures" else None),
                )
            except Exception:
                try:
                    await _bybit_cancel_order(api_key=api_key, api_secret=api_secret, category=category, symbol=symbol, order_id=str((sl_res.get("result") or {}).get("orderId") or ""))
                except Exception:
                    pass
                try:
                    await _bybit_order_create(
                        api_key=api_key,
                        api_secret=api_secret,
                        category=category,
                        symbol=symbol,
                        side=close_side,
                        order_type="Market",
                        qty=qty,
                        reduce_only=(True if mt == "futures" else None),
                    )
                except Exception:
                    pass
                raise

            tp2_res = None
            if has_tp2 and qty2 > 0:
                try:
                    tp2_res = await _bybit_order_create(
                        api_key=api_key,
                        api_secret=api_secret,
                        category=category,
                        symbol=symbol,
                        side=close_side,
                        order_type="Limit",
                        qty=qty2,
                        price=_round_tick(tp2, tick),
                        reduce_only=(True if mt == "futures" else None),
                    )
                except Exception:
                    tp2_res = None

            def _rid(x: dict | None) -> str | None:
                if not isinstance(x, dict):
                    return None
                return str((x.get("result") or {}).get("orderId") or "") or None

            ref = {
                "exchange": "bybit",
                "market_type": mt,
                "category": category,
                "symbol": symbol,
                "atr_pct": float(atr_pct_sig) if mt == "futures" else 0.0,
                "atr5": float(atr5_sig) if mt == "futures" else 0.0,
                "atr5_pct": float(atr5_pct_sig) if mt == "futures" else 0.0,
                "side": side,
                "close_side": close_side,
                "entry_order_id": str(order_id) if order_id else None,
                "sl_order_id": _rid(sl_res),
                "tp1_order_id": _rid(tp1_res),
                "tp2_order_id": _rid(tp2_res),
                "entry_price": float(entry or 0.0),
                "be_price": float(entry or 0.0),
                "be_moved": False,
                "qty": qty,
                "tp1": tp1,
                "tp2": tp2,
                "sl": sl,
            }

            sig_id = _extract_signal_id(sig)
            if sig_id <= 0:
                raise ExchangeAPIError("missing signal_id")
            await db_store.create_autotrade_position(
                user_id=uid,
                signal_id=sig_id,
                exchange="bybit",
                market_type=mt,
                symbol=symbol,
                side=side,
                allocated_usdt=need_usdt,
                api_order_ref=json.dumps(ref),
            )
            return {"ok": True, "skipped": False, "api_error": None,
                    **({"cap_ui": float(fut_cap), "cap_effective": float(eff_cap), "cap_decreased": bool(cap_decreased), "cap_prev_effective": prev_eff_val, "winrate": (float(winrate) if winrate is not None else None)} if mt == "futures" else {})
                    }

        
        # -------- OKX / MEXC / Gate.io (SPOT) --------
        if exchange in ("okx", "mexc", "gateio"):
            if mt != "spot":
                return _skip("skipped")

            # Futures cap is irrelevant for SPOT; spot cap is wallet balance.
            symbol = _normalize_symbol(getattr(sig, "symbol", "") or "")
            if not symbol:
                raise ExchangeAPIError("missing symbol")

            # Use public price to estimate base quantity for virtual SL/TP tracking
            if exchange == "okx":
                px = await _okx_public_price(symbol)
            elif exchange == "mexc":
                px = await _mexc_public_price(symbol)
            else:
                px = await _gateio_public_price(symbol)

            if not px or px <= 0:
                raise ExchangeAPIError("price_unavailable")

            est_qty = (need_usdt / float(px)) if need_usdt > 0 else 0.0
            if est_qty <= 0:
                raise ExchangeAPIError("qty_estimate_zero")

            # Execute market entry
            if direction == "SHORT":
                # Spot short is not supported in this bot (requires margin). Skip safely.
                return _skip("skipped")

            if exchange == "okx":
                passphrase = _decrypt_token(row.get("passphrase_enc"))
                entry_order = await _okx_spot_market_buy(api_key=api_key, api_secret=api_secret, passphrase=passphrase, symbol=symbol, quote_usdt=need_usdt)
            elif exchange == "mexc":
                entry_order = await _mexc_spot_market_buy(api_key=api_key, api_secret=api_secret, symbol=symbol, quote_usdt=need_usdt)
            else:
                entry_order = await _gateio_spot_market_buy(api_key=api_key, api_secret=api_secret, symbol=symbol, quote_usdt=need_usdt)

            # Store a virtual manager ref (virtual SL/TP + BE after TP1)
            ref = {
                "exchange": exchange,
                "market_type": "spot",
                "symbol": symbol,
                "atr_pct": float(atr_pct_sig) if mt == "futures" else 0.0,
                "atr5": float(atr5_sig) if mt == "futures" else 0.0,
                "atr5_pct": float(atr5_pct_sig) if mt == "futures" else 0.0,
                "side": "BUY",
                "close_side": "SELL",
                "virtual": True,
                "entry_price": float(px),
                "be_price": float(px),
                "be_moved": False,
                "tp1_hit": False,
                "qty": float(est_qty),
                "tp1": float(tp1 or 0.0),
                "tp2": float(tp2 or 0.0),
                "sl": float(sl or 0.0),
            }

            sig_id = _extract_signal_id(sig)
            if sig_id <= 0:
                raise ExchangeAPIError("missing signal_id")
            await db_store.create_autotrade_position(
                user_id=uid,
                signal_id=sig_id,
                exchange=exchange,
                market_type="spot",
                symbol=symbol,
                side="BUY",
                allocated_usdt=need_usdt,
                api_order_ref=json.dumps(ref),
            )
            return {"ok": True, "skipped": False, "api_error": None}

        # Safety guard: never fall through into Binance execution for other exchanges
        if exchange != "binance":
            return _skip("skipped")

# -------- Binance --------
        if mt == "spot":
            free = await _binance_spot_free_usdt(api_key, api_secret)
            if free < need_usdt:
                return _skip("insufficient_balance", free_usdt=float(free), need_usdt=float(need_usdt), exchange=exchange, market=market)

            info = await _binance_exchange_info(futures=False)
            qty_step, min_qty, tick, min_notional = _binance_symbol_filters(info, symbol)

            # Entry: market buy by quote amount (USDT)
            entry_res = await _binance_spot_market_buy_quote(
                api_key=api_key,
                api_secret=api_secret,
                symbol=symbol,
                quote_usdt=need_usdt,
            )

            exec_qty = float(entry_res.get("executedQty") or 0.0)
            if exec_qty <= 0:
                # fallback: attempt to infer from fills
                q = 0.0
                for f in entry_res.get("fills", []) or []:
                    q += float(f.get("qty") or 0.0)
                exec_qty = q
            if exec_qty <= 0:
                raise ExchangeAPIError(f"Binance SPOT entry executedQty=0: {entry_res}")

            # Split for TP1/TP2 (respect LOT_SIZE)
            has_tp2 = tp2 > 0 and abs(tp2 - tp1) > 1e-12
            # Partial close % is configurable via env (TP1_PARTIAL_CLOSE_PCT_SPOT)
            p = max(0.0, min(100.0, float(_tp1_partial_close_pct('SPOT'))))
            a = p / 100.0
            qty1 = exec_qty * (a if has_tp2 else 1.0)
            qty2 = (exec_qty - qty1) if has_tp2 else 0.0
            qty1 = _round_step(qty1, qty_step)
            qty2 = _round_step(qty2, qty_step)
            exec_qty = _round_step(exec_qty, qty_step)
            if exec_qty < min_qty:
                raise ExchangeAPIError(f"Binance SPOT qty<minQty after rounding: {exec_qty} < {min_qty}")
            if has_tp2 and qty2 < min_qty:
                # If the second leg becomes too small, do a single TP.
                has_tp2 = False
                qty1 = exec_qty
                qty2 = 0.0

            # --- VIRTUAL mode: do not place SL/TP orders on exchange ---
            if _VIRTUAL_SLTP_ALL:
                # Use actual executed qty from entry as the managed base qty.
                entry_p = float(entry or 0.0)
                ref = {
                    "virtual": True,
                    "exchange": "binance",
                    "market_type": "spot",
                    "symbol": symbol,
                "atr_pct": float(atr_pct_sig) if mt == "futures" else 0.0,
                "atr5": float(atr5_sig) if mt == "futures" else 0.0,
                "atr5_pct": float(atr5_pct_sig) if mt == "futures" else 0.0,
                    "direction": direction,
                    "side": "BUY",
                    "close_side": "SELL",
                    "entry_price": entry_p,
                    "qty": float(exec_qty),
                    "tp1": float(tp1 or 0.0),
                    "tp2": float(tp2 or 0.0),
                    "sl": float(sl or 0.0),
                    "tp1_hit": False,
                    "be_moved": False,
                    "be_price": 0.0,
                }
                sig_id = _extract_signal_id(sig)
                if sig_id <= 0:
                    raise ExchangeAPIError("missing signal_id")
                await db_store.create_autotrade_position(
                    user_id=uid,
                    signal_id=sig_id,
                    exchange="binance",
                    market_type="spot",
                    symbol=symbol,
                    side="BUY",
                    allocated_usdt=need_usdt,
                    api_order_ref=json.dumps(ref),
                )
                return {"ok": True, "skipped": False, "api_error": None}


            # Place SL first; if SL fails, immediately close (sell) to avoid an unprotected position.
            try:
                sl_res = await _binance_spot_stop_loss_limit_sell(
                    api_key=api_key,
                    api_secret=api_secret,
                    symbol=symbol,
                    qty=exec_qty,
                    stop_price=_round_tick(sl, tick),
                )
            except Exception as e:
                # Emergency close
                try:
                    await _binance_spot_market_sell_base(api_key=api_key, api_secret=api_secret, symbol=symbol, qty=exec_qty)
                except Exception:
                    pass
                raise

            # Place TP(s). If TP1 fails, close to avoid keeping a half-configured strategy.
            try:
                tp1_res = await _binance_spot_limit_sell(api_key=api_key, api_secret=api_secret, symbol=symbol, qty=qty1, price=_round_tick(tp1, tick))
            except Exception:
                try:
                    await _binance_cancel_order(api_key=api_key, api_secret=api_secret, symbol=symbol, order_id=int(sl_res.get("orderId") or 0), futures=False)
                except Exception:
                    pass
                try:
                    await _binance_spot_market_sell_base(api_key=api_key, api_secret=api_secret, symbol=symbol, qty=exec_qty)
                except Exception:
                    pass
                raise
            tp2_res = None
            if has_tp2 and qty2 > 0:
                tp2_res = await _binance_spot_limit_sell(api_key=api_key, api_secret=api_secret, symbol=symbol, qty=qty2, price=_round_tick(tp2, tick))

            ref = {
                "exchange": "binance",
                "market_type": "spot",
                "symbol": symbol,
                "atr_pct": float(atr_pct_sig) if mt == "futures" else 0.0,
                "atr5": float(atr5_sig) if mt == "futures" else 0.0,
                "atr5_pct": float(atr5_pct_sig) if mt == "futures" else 0.0,
                "side": "BUY",
                "entry_order_id": entry_res.get("orderId"),
                "sl_order_id": sl_res.get("orderId"),
                "tp1_order_id": tp1_res.get("orderId"),
                "tp2_order_id": (tp2_res.get("orderId") if isinstance(tp2_res, dict) else None),
                "entry_price": float(entry or 0.0),
                "be_price": float(entry or 0.0),
                "be_moved": False,
                "qty": exec_qty,
                "tp1": tp1,
                "tp2": tp2,
                "sl": sl,
            }

            sig_id = _extract_signal_id(sig)
            if sig_id <= 0:
                raise ExchangeAPIError("missing signal_id")
            await db_store.create_autotrade_position(
                user_id=uid,
                signal_id=sig_id,
                exchange="binance",
                market_type="spot",
                symbol=symbol,
                side="BUY",
                allocated_usdt=need_usdt,
                api_order_ref=json.dumps(ref),
            )
            return {"ok": True, "skipped": False, "api_error": None}

        # futures
        used = await db_store.get_autotrade_used_usdt(uid, "futures")
        if fut_cap > 0 and used + need_usdt > fut_cap:
            return _skip("skipped")

        avail = await _binance_futures_available_margin(api_key, api_secret)
        if avail < need_usdt:
            return _skip("skipped")

        await _binance_futures_set_leverage(api_key=api_key, api_secret=api_secret, symbol=symbol, leverage=fut_lev)

        # compute qty from margin*leverage/price
        px = await _binance_price(symbol, futures=True)
        if px <= 0:
            raise ExchangeAPIError("Binance futures price=0")
        raw_qty = (need_usdt * float(fut_lev)) / px
        info = await _binance_exchange_info(futures=True)
        step, min_qty, tick, _mn = _binance_symbol_filters(info, symbol)
        qty = _round_step(raw_qty, step) if step > 0 else raw_qty
        if min_qty > 0 and qty < min_qty:
            qty = min_qty

        side = "BUY" if direction == "LONG" else "SELL"
        close_side = "SELL" if side == "BUY" else "BUY"
        entry_res = await _binance_futures_market_open(api_key=api_key, api_secret=api_secret, symbol=symbol, side=side, qty=qty)

        # --- VIRTUAL mode: do not place SL/TP orders on exchange ---
        if _VIRTUAL_SLTP_ALL:
            entry_p = float(entry or 0.0)
            ref = {
                "virtual": True,
                "exchange": "binance",
                "market_type": "futures",
                "symbol": symbol,
                "atr_pct": float(atr_pct_sig) if mt == "futures" else 0.0,
                "atr5": float(atr5_sig) if mt == "futures" else 0.0,
                "atr5_pct": float(atr5_pct_sig) if mt == "futures" else 0.0,
                "direction": direction,
                "side": side.upper(),
                "close_side": ("SELL" if side.upper() == "BUY" else "BUY"),
                "entry_price": entry_p,
                "qty": float(qty),
                "tp1": float(tp1 or 0.0),
                "tp2": float(tp2 or 0.0),
                "sl": float(sl or 0.0),
                "tp1_hit": False,
                "be_moved": False,
                "be_price": 0.0,
            }
            sig_id = _extract_signal_id(sig)
            if sig_id <= 0:
                raise ExchangeAPIError("missing signal_id")
            await db_store.create_autotrade_position(
                user_id=uid,
                signal_id=sig_id,
                exchange="binance",
                market_type="futures",
                symbol=symbol,
                side=side,
                allocated_usdt=need_usdt,
                api_order_ref=json.dumps(ref),
            )
            return {"ok": True, "skipped": False, "api_error": None}


        # Place SL first; if SL fails, immediately close position to avoid unprotected exposure.
        try:
            sl_res = await _binance_futures_stop_market_close_all(
                api_key=api_key,
                api_secret=api_secret,
                symbol=symbol,
                side=close_side,
                stop_price=_round_tick(sl, tick),
            )
        except Exception:
            try:
                await _binance_futures_reduce_market(api_key=api_key, api_secret=api_secret, symbol=symbol, side=close_side, qty=qty)
            except Exception:
                pass
            raise

        # TP reduce-only limits (respect LOT_SIZE)
        has_tp2 = tp2 > 0 and abs(tp2 - tp1) > 1e-12
        qty1 = _round_step(qty * (0.5 if has_tp2 else 1.0), step)
        qty2 = _round_step(qty - qty1, step) if has_tp2 else 0.0
        if min_qty > 0 and qty1 < min_qty:
            # Not enough size for strategy
            raise ExchangeAPIError(f"Binance FUTURES qty<minQty after rounding: {qty1} < {min_qty}")
        if has_tp2 and min_qty > 0 and qty2 < min_qty:
            has_tp2 = False
            qty1 = qty
            qty2 = 0.0
        try:
            tp1_res = await _binance_futures_reduce_limit(
                api_key=api_key,
                api_secret=api_secret,
                symbol=symbol,
                side=close_side,
                qty=qty1,
                price=_round_tick(tp1, tick),
            )
        except Exception:
            # Cancel SL and close to avoid half-configured strategy
            try:
                await _binance_cancel_order(api_key=api_key, api_secret=api_secret, symbol=symbol, order_id=int(sl_res.get("orderId") or 0), futures=True)
            except Exception:
                pass
            try:
                await _binance_futures_reduce_market(api_key=api_key, api_secret=api_secret, symbol=symbol, side=close_side, qty=qty)
            except Exception:
                pass
            raise
        tp2_res = None
        if has_tp2 and qty2 > 0:
            try:
                tp2_res = await _binance_futures_reduce_limit(api_key=api_key, api_secret=api_secret, symbol=symbol, side=close_side, qty=qty2, price=_round_tick(tp2, tick))
            except Exception:
                tp2_res = None

        ref = {
            "exchange": "binance",
            "market_type": "futures",
            "symbol": symbol,
                "atr_pct": float(atr_pct_sig) if mt == "futures" else 0.0,
                "atr5": float(atr5_sig) if mt == "futures" else 0.0,
                "atr5_pct": float(atr5_pct_sig) if mt == "futures" else 0.0,
            "side": side,
            "close_side": close_side,
            "entry_order_id": entry_res.get("orderId"),
            "sl_order_id": sl_res.get("orderId"),
            "tp1_order_id": tp1_res.get("orderId"),
            "tp2_order_id": (tp2_res.get("orderId") if isinstance(tp2_res, dict) else None),
            "entry_price": float(entry or 0.0),
            "be_price": float(entry or 0.0),
            "be_moved": False,
            "qty": qty,
            "tp1": tp1,
            "tp2": tp2,
            "sl": sl,
        }

        sig_id = _extract_signal_id(sig)
        if sig_id <= 0:
            raise ExchangeAPIError("missing signal_id")
        await db_store.create_autotrade_position(
            user_id=uid,
            signal_id=sig_id,
            exchange="binance",
            market_type="futures",
            symbol=symbol,
            side=side,
            allocated_usdt=need_usdt,
            api_order_ref=json.dumps(ref),
        )
        return {"ok": True, "skipped": False, "api_error": None}

    except ExchangeAPIError as e:
        err = str(e)
        await db_store.mark_autotrade_key_error(
            user_id=uid,
            exchange=exchange,
            market_type=mt,
            error=err,
            deactivate=_should_deactivate_key(err),
        )
        return _fail(err, exchange=exchange, market=market)
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        await db_store.mark_autotrade_key_error(
            user_id=uid,
            exchange=exchange,
            market_type=mt,
            error=err,
            deactivate=_should_deactivate_key(err),
        )
        return {"ok": False, "skipped": False, "api_error": err}


# ------------------ Auto-trade soft reconcile (manual close sync) ------------------
# If a user closes a position manually on the exchange, SL/TP orders may never be FILLED,
# and the DB row in autotrade_positions can stay OPEN forever. We periodically reconcile
# OPEN rows against real balances/positions and close them best-effort.
_LAST_RECONCILE_TS: dict[int, float] = {}
_SMART_TICK_TS: dict[int, float] = {}  # pos_id -> last log ts
_RECONCILE_COOLDOWN_SEC = max(10, int(os.getenv("AUTOTRADE_RECONCILE_SEC", "60") or "60"))
_DUST_PCT = float(os.getenv("AUTOTRADE_DUST_PCT", "0.01") or "0.01")   # 1% of original qty
_DUST_MIN = float(os.getenv("AUTOTRADE_DUST_MIN", "1e-8") or "1e-8")

def _base_asset(symbol: str) -> str:
    s = (symbol or "").upper().strip()
    for q in ("USDT","USDC","BUSD","USD"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)]
    return s

async def _spot_base_balance(*, ex: str, api_key: str, api_secret: str, passphrase: str | None, symbol: str) -> float:
    base = _base_asset(symbol)
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        ex = (ex or "").lower().strip()
        if ex == "binance":
            data = await _binance_signed_request(
                s, base_url="https://api.binance.com", path="/api/v3/account", method="GET",
                api_key=api_key, api_secret=api_secret, params={"recvWindow":"5000"},
            )
            bals = data.get("balances") or []
            if isinstance(bals, list):
                for b in bals:
                    if isinstance(b, dict) and str(b.get("asset") or "").upper() == base:
                        free = float(b.get("free") or 0.0)
                        locked = float(b.get("locked") or 0.0)
                        return float(free + locked)
            return 0.0

        if ex == "bybit":
            # V5 unified wallet-balance; best-effort.
            data = await _bybit_v5_request(
                s, method="GET", path="/v5/account/wallet-balance",
                api_key=api_key, api_secret=api_secret,
                params={"accountType":"UNIFIED", "coin": base},
            )
            res = (data.get("result") or {})
            lst = res.get("list") or []
            if isinstance(lst, list) and lst:
                cl = (lst[0].get("coin") or [])
                if isinstance(cl, list) and cl:
                    coin = cl[0]
                    if isinstance(coin, dict):
                        free = float(coin.get("availableToWithdraw") or coin.get("walletBalance") or 0.0)
                        # no separate "locked" field exposed consistently; walletBalance is OK for our dust check
                        return float(free)
            return 0.0

        if ex == "okx":
            data = await _okx_signed_request(
                s, base_url="https://www.okx.com", path="/api/v5/account/balance", method="GET",
                api_key=api_key, api_secret=api_secret, passphrase=(passphrase or ""),
                params={"ccy": base},
            )
            dl = (data.get("data") or [])
            if isinstance(dl, list) and dl:
                details = (dl[0].get("details") or [])
                if isinstance(details, list) and details:
                    d0 = details[0]
                    if isinstance(d0, dict):
                        cash = float(d0.get("cashBal") or 0.0)
                        frozen = float(d0.get("frozenBal") or 0.0)
                        return float(cash + frozen)
            return 0.0

        if ex == "mexc":
            data = await _mexc_signed_request(
                s, base_url="https://api.mexc.com", path="/api/v3/account", method="GET",
                api_key=api_key, api_secret=api_secret, params={},
            )
            bals = data.get("balances") or []
            if isinstance(bals, list):
                for b in bals:
                    if isinstance(b, dict) and str(b.get("asset") or "").upper() == base:
                        free = float(b.get("free") or 0.0)
                        locked = float(b.get("locked") or 0.0)
                        return float(free + locked)
            return 0.0

        if ex == "gateio":
            data = await _gateio_signed_request(
                s, base_url="https://api.gateio.ws", path="/api/v4/spot/accounts", method="GET",
                api_key=api_key, api_secret=api_secret, params={},
            )
            if isinstance(data, list):
                for a in data:
                    if isinstance(a, dict) and str(a.get("currency") or "").upper() == base:
                        avail = float(a.get("available") or 0.0)
                        locked = float(a.get("locked") or 0.0)
                        return float(avail + locked)
            return 0.0

    return 0.0

async def _futures_position_size(*, ex: str, api_key: str, api_secret: str, symbol: str, category: str = "linear") -> float:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        ex = (ex or "").lower().strip()
        if ex == "binance":
            data = await _binance_signed_request(
                s, base_url="https://fapi.binance.com", path="/fapi/v2/positionRisk", method="GET",
                api_key=api_key, api_secret=api_secret, params={"symbol": symbol, "recvWindow":"5000"},
            )
            # can be list
            if isinstance(data, list) and data:
                d0 = data[0]
            else:
                d0 = data.get("data") if isinstance(data, dict) else None
            if isinstance(d0, dict):
                return float(d0.get("positionAmt") or 0.0)
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, dict) and str(d.get("symbol") or "").upper() == symbol:
                        return float(d.get("positionAmt") or 0.0)
            return 0.0

        if ex == "bybit":
            data = await _bybit_v5_request(
                s, method="GET", path="/v5/position/list",
                api_key=api_key, api_secret=api_secret,
                params={"category": category, "symbol": symbol},
            )
            res = data.get("result") or {}
            lst = res.get("list") or []
            if isinstance(lst, list) and lst:
                d0 = lst[0]
                if isinstance(d0, dict):
                    return float(d0.get("size") or 0.0)
            return 0.0
    return 0.0

async def autotrade_manager_loop(*, notify_api_error) -> None:
    """Background loop to manage SL/TP/BE for real orders.

    notify_api_error(user_id:int, text:str) will be called only on API errors.
    """

    import socket as _socket
    _wid_base = os.getenv("AUTOTRADE_MANAGER_ID") or os.getenv("RAILWAY_REPLICA_ID") or os.getenv("HOSTNAME") or _socket.gethostname()
    manager_worker_id = f"{_wid_base}:{os.getpid()}"
    manager_batch = int(os.getenv("AUTOTRADE_MANAGER_BATCH", "300") or 300)
    manager_lease_sec = int(os.getenv("AUTOTRADE_MANAGER_LEASE_SEC", "120") or 120)

    def _as_float(x, default: float = 0.0) -> float:
        try:
            if x is None:
                return float(default)
            return float(str(x))
        except Exception:
            return float(default)

    def _binance_avg_price(order: dict) -> float:
        if not isinstance(order, dict):
            return 0.0
        ap = _as_float(order.get("avgPrice"), 0.0)
        if ap > 0:
            return ap
        exec_qty = _as_float(order.get("executedQty"), 0.0)
        cqq = _as_float(order.get("cummulativeQuoteQty"), 0.0)
        return (cqq / exec_qty) if exec_qty > 0 else 0.0

    def _bybit_avg_price(resp: dict) -> float:
        # resp is full response from _bybit_order_status
        try:
            lst = ((resp.get("result") or {}).get("list") or [])
            o = lst[0] if (lst and isinstance(lst[0], dict)) else {}
        except Exception:
            o = {}
        ap = _as_float(o.get("avgPrice"), 0.0)
        if ap > 0:
            return ap
        qty = _as_float(o.get("cumExecQty"), 0.0)
        val = _as_float(o.get("cumExecValue"), 0.0)
        return (val / qty) if qty > 0 else 0.0

    def _calc_autotrade_pnl_from_orders(*, ref: dict, entry_order: dict, exit_order: dict, exchange: str, market_type: str, allocated_usdt: float) -> tuple[float | None, float | None]:
        """Compute net PnL using entry/exit average fills + modeled fees.

        Improvements vs legacy:
        - uses executed qty from the *exit* order when available
        - subtracts modeled fees (FEE_RATE_SPOT/FEE_RATE_FUTURES) for entry+exit notionals
        - adds any previously accumulated realized pnl from partial closes (e.g., TP1) stored in ref

        Still an approximation (we do not fetch per-fill commissions/funding to keep API usage light).
        """
        try:
            side = str(ref.get("side") or "BUY").upper()
            qty_ref = _as_float(ref.get("qty"), 0.0)
            if qty_ref <= 0:
                return None, None

            # Extract executed qty from the exit order if possible (more accurate for partial closes)
            def _binance_exec_qty(order: dict) -> float:
                if not isinstance(order, dict):
                    return 0.0
                return _as_float(order.get("executedQty"), 0.0)

            def _bybit_exec_qty(resp: dict) -> float:
                try:
                    lst = ((resp.get("result") or {}).get("list") or [])
                    o = lst[0] if (lst and isinstance(lst[0], dict)) else {}
                except Exception:
                    o = {}
                return _as_float(o.get("cumExecQty"), 0.0)

            if exchange == "binance":
                entry_p = _binance_avg_price(entry_order)
                exit_p = _binance_avg_price(exit_order)
                qty_exit = _binance_exec_qty(exit_order)
            else:
                entry_p = _bybit_avg_price(entry_order)
                exit_p = _bybit_avg_price(exit_order)
                qty_exit = _bybit_exec_qty(exit_order)

            if entry_p <= 0 or exit_p <= 0:
                return None, None

            qty_used = qty_exit if qty_exit > 0 else qty_ref
            qty_used = max(0.0, min(float(qty_used), float(qty_ref) if qty_ref > 0 else float(qty_used)))
            if qty_used <= 0:
                return None, None

            # BUY = long, SELL = short (for futures); for spot we assume BUY then SELL
            if side == "BUY":
                pnl_gross = (exit_p - entry_p) * qty_used
            else:
                pnl_gross = (entry_p - exit_p) * qty_used

            # Modeled fees (keep light: no per-fill fees API calls)
            fr = float(FEE_RATE_SPOT if str(market_type).lower() == "spot" else FEE_RATE_FUTURES)
            fees_est = fr * ((entry_p * qty_used) + (exit_p * qty_used))

            # Add previously realized pnl/fees from earlier partial closes (e.g. TP1)
            realized_pnl = _as_float(ref.get("realized_pnl_usdt"), 0.0)
            realized_fee = _as_float(ref.get("realized_fee_usdt"), 0.0)

            pnl_net = float(pnl_gross) - float(fees_est) + float(realized_pnl) - float(realized_fee)

            alloc = float(allocated_usdt or 0.0)
            roi = (pnl_net / alloc * 100.0) if alloc > 0 else None
            return float(pnl_net), (float(roi) if roi is not None else None)
        except Exception:
            return None, None

   

    # One-time fast self-heal (at startup): populate meta.ref from api_order_ref for all OPEN positions.
    async def _meta_selfheal_once() -> None:
        try:
            rows0 = await db_store.list_open_autotrade_positions(limit=500)
        except Exception:
            return
        for rr in rows0 or []:
            try:
                ref0 = json.loads(rr.get("api_order_ref") or "{}")
            except Exception:
                continue

            meta0 = {}
            try:
                mr = rr.get("meta")
                if isinstance(mr, str):
                    meta0 = json.loads(mr or "{}") or {}
                elif isinstance(mr, dict):
                    meta0 = dict(mr)
            except Exception:
                meta0 = {}

            try:
                if meta0.get("ref"):
                    continue
                src_ref = ""
                for k in (
                    "ref",
                    "order_ref",
                    "client_order_ref",
                    "clientOrderId",
                    "client_order_id",
                    "orderLinkId",
                    "order_link_id",
                ):
                    v = ref0.get(k)
                    if v is not None and str(v).strip():
                        src_ref = str(v).strip()
                        break
                if src_ref:
                    # merge-patch only (keep any existing meta fields)
                    await db_store.update_autotrade_position_meta(row_id=int(rr.get("id") or 0), meta={"ref": src_ref}, replace=False)
            except Exception:
                pass

    try:
        await _meta_selfheal_once()
    except Exception:
        pass

    # Best-effort; never crash.
    while True:
        try:
            rows = await db_store.claim_open_autotrade_positions(owner=manager_worker_id, limit=manager_batch, lease_sec=manager_lease_sec)
            for r in rows:
                rid = int((r or {}).get('id') or 0)
                # Refresh lease so other replicas won't steal it mid-processing.
                try:
                    if rid > 0:
                        await db_store.touch_autotrade_position_lock(row_id=rid, owner=manager_worker_id, lease_sec=manager_lease_sec)
                except Exception:
                    pass

                try:
                    ref = json.loads(r.get("api_order_ref") or "{}")
                    tf = str(ref.get("signal_timeframe") or "")
                    kind = str(ref.get("signal_kind") or "")
                    is_mid = (kind.upper() == "MID") or _is_mid_tf(tf)
                except Exception:
                    continue

                # --- Meta (persistent per-position extra state) ---
                meta = {}
                try:
                    meta_raw = r.get("meta")
                    if isinstance(meta_raw, str):
                        meta = json.loads(meta_raw or "{}") or {}
                    elif isinstance(meta_raw, dict):
                        meta = dict(meta_raw)
                    else:
                        meta = {}
                except Exception:
                    meta = {}

                # --- Self-heal: if meta.ref is empty, copy from api_order_ref (slow migration without manual scripts) ---
                try:
                    if not (meta.get("ref") or ""):
                        src_ref = ""
                        for k in ("ref", "order_ref", "client_order_ref", "clientOrderId", "client_order_id", "orderLinkId", "order_link_id"):
                            v = ref.get(k)
                            if v is not None and str(v).strip():
                                src_ref = str(v).strip()
                                break
                        if src_ref:
                            meta["ref"] = src_ref
                            try:
                                await db_store.update_autotrade_position_meta(row_id=int(r.get("id") or 0), meta=meta, replace=False)
                            except Exception:
                                pass
                except Exception:
                    pass

                # Keep in-memory copy (used later in this loop)
                r["meta"] = meta

                ex = str(ref.get("exchange") or r.get("exchange") or "").lower()
                if ex not in ("binance", "bybit", "okx", "mexc", "gateio"):
                    continue

                uid = int(r.get("user_id"))
                mt = str(ref.get("market_type") or r.get("market_type") or "").lower()
                futures = (mt == "futures")

                # --- SMART_TICK (Railway visibility): proves manager loop is running ---
                try:
                    pos_id_tick = int(r.get("id") or 0)
                except Exception:
                    pos_id_tick = 0
                now_tick = time.time()
                last_tick = _SMART_TICK_TS.get(pos_id_tick, 0.0) if pos_id_tick else 0.0
                if (not pos_id_tick) or (now_tick - last_tick >= 60.0):
                    if pos_id_tick:
                        _SMART_TICK_TS[pos_id_tick] = now_tick
                    sym_tick = str(ref.get("symbol") or r.get("symbol") or "")
                    side_tick = str(ref.get("side") or r.get("side") or "")
                    logger.info(
                        "SMART_TICK uid=%s market=%s ex=%s sym=%s side=%s virtual=%s",
                        uid, mt, ex, sym_tick, side_tick, bool(ref.get("virtual"))
                    )
                symbol = str(ref.get("symbol") or r.get("symbol") or "").upper()

                # Load keys
                row = await db_store.get_autotrade_keys_row(user_id=uid, exchange=ex, market_type=mt)
                if not row or not bool(row.get("is_active")):
                    continue
                try:
                    api_key = _decrypt_token(row.get("api_key_enc"))
                    api_secret = _decrypt_token(row.get("api_secret_enc"))
                except Exception:
                    continue

                # --- Soft reconcile: detect manual close and close stale OPEN rows ---
                # If user closed manually on exchange, SL/TP may never be FILLED -> DB can remain OPEN forever.
                # We periodically verify that the position/holding still exists on the exchange.
                try:
                    pos_id = int(r.get("id") or 0)
                except Exception:
                    pos_id = 0
                now_ts = time.time()
                last_ts = _LAST_RECONCILE_TS.get(pos_id, 0.0)
                if pos_id and (now_ts - last_ts) >= _RECONCILE_COOLDOWN_SEC:
                    _LAST_RECONCILE_TS[pos_id] = now_ts
                    try:
                        # Determine if position is still open on exchange
                        is_closed = False
                        if mt == "spot":
                            passphrase = ""
                            if ex == "okx":
                                try:
                                    passphrase = _decrypt_token(row.get("passphrase_enc"))
                                except Exception:
                                    passphrase = ""
                            bal = await _spot_base_balance(ex=ex, api_key=api_key, api_secret=api_secret,
                                                           passphrase=(passphrase or None), symbol=symbol)
                            ref_qty = _as_float(ref.get("qty"), 0.0)
                            eps = max(_DUST_MIN, abs(ref_qty) * _DUST_PCT)
                            if bal <= eps:
                                is_closed = True
                        else:
                            # futures (Binance/Bybit only in this codebase)
                            size = await _futures_position_size(ex=ex, api_key=api_key, api_secret=api_secret, symbol=symbol,
                                                                category=str(ref.get("category") or "linear"))
                            if abs(float(size or 0.0)) <= _DUST_MIN:
                                is_closed = True

                        if is_closed:
                            # Best-effort: cancel any known child orders to avoid stray fills after manual close.
                            # (Some exchanges allow reduce-only orders to remain; spot leftovers are especially risky.)
                            try:
                                if ex in ("binance", "bybit"):
                                    cat2 = str(ref.get("category") or ("linear" if mt == "futures" else "spot"))
                                    for oid in [ref.get("tp1_order_id"), ref.get("tp2_order_id"), ref.get("sl_order_id")]:
                                        if not oid:
                                            continue
                                        try:
                                            if ex == "binance":
                                                await _binance_cancel_order(
                                                    api_key=api_key,
                                                    api_secret=api_secret,
                                                    symbol=symbol,
                                                    order_id=int(oid),
                                                    futures=(mt == "futures"),
                                                )
                                            else:
                                                await _bybit_cancel_order(
                                                    api_key=api_key,
                                                    api_secret=api_secret,
                                                    category=cat2,
                                                    symbol=symbol,
                                                    order_id=str(oid),
                                                )
                                        except Exception:
                                            pass
                            except Exception:
                                pass

                            # Persist a reason so UI/debug can distinguish manual closes from TP/SL.
                            try:
                                if pos_id:
                                    await db_store.update_autotrade_position_meta(
                                        row_id=int(pos_id),
                                        meta={"closed_reason": "MANUAL_CLOSE", "closed_by_user": True},
                                        replace=False,
                                    )
                            except Exception:
                                pass

                            # Best-effort PnL: use current price at detection time.
                            entry = _as_float(ref.get("entry"), _as_float(r.get("entry"), 0.0))
                            qty = _as_float(ref.get("qty"), 0.0)
                            side = str(ref.get("side") or r.get("side") or "").upper()
                            allocated_usdt = _as_float(r.get("allocated_usdt"), 0.0)
                            px = 0.0
                            try:
                                if ex == "binance":
                                    px = await _binance_price(symbol, futures=(mt == "futures"))
                                elif ex == "bybit":
                                    px = await _bybit_price(symbol, category=("linear" if mt == "futures" else "spot"))
                                elif ex == "okx":
                                    px = await _okx_public_price(symbol)
                                elif ex == "mexc":
                                    px = await _mexc_public_price(symbol)
                                else:
                                    px = await _gateio_public_price(symbol)
                            except Exception:
                                px = 0.0
                            pnl_usdt = None
                            roi_percent = None
                            try:
                                if entry and qty and px:
                                    if mt == "futures":
                                        # futures: BUY=LONG, SELL=SHORT
                                        if side in ("SELL", "SHORT"):
                                            pnl = (entry - px) * qty
                                        else:
                                            pnl = (px - entry) * qty
                                    else:
                                        # spot: only long
                                        pnl = (px - entry) * qty
                                    pnl_usdt = float(pnl)
                                    if allocated_usdt > 0:
                                        roi_percent = float(pnl_usdt) / float(allocated_usdt) * 100.0
                            except Exception:
                                pnl_usdt = None
                                roi_percent = None

                            await db_store.close_autotrade_position(
                                user_id=uid,
                                signal_id=r.get("signal_id"),
                                exchange=ex,
                                market_type=mt,
                                status="CLOSED",
                                pnl_usdt=pnl_usdt,
                                roi_percent=roi_percent,
                            )
                            # respect stop_after_close flag
                            try:
                                acc2 = await db_store.get_autotrade_access(uid)
                                if bool(acc2.get("autotrade_stop_after_close")):
                                    if await db_store.count_open_autotrade_positions(uid) <= 0:
                                        await db_store.finalize_autotrade_disable(uid)
                            except Exception:
                                pass
                            continue
                    except Exception:
                        # Never crash the manager loop because of reconcile.
                        logger.exception("Soft reconcile error for pos_id=%s", pos_id)
                tp1_id = ref.get("tp1_order_id")
                tp2_id = ref.get("tp2_order_id")
                sl_id = ref.get("sl_order_id")
                be_moved = bool(ref.get("be_moved"))
                side_str = 'LONG' if str(ref.get('side','BUY')).upper() in ('BUY','LONG') else 'SHORT'
                if side_str != 'LONG':
                    # Spot manager supports LONG only (no short spot). Mark as ERROR to avoid wrong closes.
                    await db_store.close_autotrade_position(user_id=uid, signal_id=r.get('signal_id'), exchange=ex, market_type=mt, status='ERROR')
                    continue
                be_price = float(ref.get("be_price") or 0.0)
                close_side = str(ref.get("close_side") or ("SELL" if str(ref.get("side")) == "BUY" else "BUY")).upper()

                
                # -------- Virtual management (ALL exchanges / SPOT+FUTURES) --------
                # If ref["virtual"] is True, we manage by watching price and sending MARKET closes.
                if bool(ref.get("virtual")) or _SMART_PRO_CONTROL_REAL:

                    # If this position was opened with exchange TP/SL orders (virtual==False) but PRO-control is enabled,
                    # cancel child orders once to avoid double-execution and then manage it with the same PRO rules as virtual mode.
                    if (not bool(ref.get("virtual"))) and _SMART_PRO_CONTROL_REAL and (not bool(ref.get("pro_real_init"))):
                        try:
                            if ex == "binance":
                                for oid in [ref.get("tp1_order_id"), ref.get("tp2_order_id"), ref.get("sl_order_id")]:
                                    try:
                                        if oid:
                                            await _binance_cancel_order(api_key=api_key, api_secret=api_secret, symbol=symbol, order_id=int(oid), futures=(mt=="futures"))
                                    except Exception:
                                        pass
                            elif ex == "bybit":
                                cat = ("linear" if mt == "futures" else "spot")
                                for oid in [ref.get("tp1_order_id"), ref.get("tp2_order_id"), ref.get("sl_order_id")]:
                                    try:
                                        if oid:
                                            await _bybit_cancel_order(api_key=api_key, api_secret=api_secret, category=cat, symbol=symbol, order_id=str(oid))
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                        # Clear stored child order ids so legacy branch won't touch them.
                        ref["tp1_order_id"] = None
                        ref["tp2_order_id"] = None
                        ref["sl_order_id"] = None
                        ref["be_moved"] = False
                        ref["pro_real_init"] = True
                        dirty = True
                    try:
                        # --- price (decision) ---
                        # For futures, prefer a robust decision price to reduce noise:
                        # median of available sources (Binance Futures + Bybit Linear + optional public OKX),
                        # falling back to the trade exchange source if only one is available.
                        if mt == "futures":
                            srcs = []
                            try:
                                data_b = await _http_json("GET", "https://fapi.binance.com/fapi/v1/ticker/price", params={"symbol": symbol.upper()}, timeout_s=8)
                                px_b = float((data_b or {}).get("price") or 0.0)
                                if px_b > 0:
                                    srcs.append(px_b)
                            except Exception:
                                pass
                            try:
                                px_y = await _bybit_price(symbol, category="linear")
                                if float(px_y or 0.0) > 0:
                                    srcs.append(float(px_y))
                            except Exception:
                                pass
                            try:
                                # OKX public last (best-effort)
                                px_o = await _okx_public_price(symbol)
                                if float(px_o or 0.0) > 0:
                                    srcs.append(float(px_o))
                            except Exception:
                                pass
                            if len(srcs) >= 2:
                                px = float(statistics.median(srcs))
                                ref["px_decision_src"] = "MEDIAN"
                                dirty = True
                            elif len(srcs) == 1:
                                px = float(srcs[0])
                                ref["px_decision_src"] = "SINGLE"
                                dirty = True
                            else:
                                # fall back to per-exchange source
                                if ex == "binance":
                                    data = await _http_json("GET", "https://fapi.binance.com/fapi/v1/ticker/price", params={"symbol": symbol.upper()}, timeout_s=8)
                                    px = float((data or {}).get("price") or 0.0)
                                elif ex == "bybit":
                                    px = await _bybit_price(symbol, category="linear")
                                elif ex == "okx":
                                    px = await _okx_public_price(symbol)
                                elif ex == "mexc":
                                    px = await _mexc_public_price(symbol)
                                else:
                                    px = await _gateio_public_price(symbol)
                        else:
                            # spot: keep per-exchange source
                            if ex == "binance":
                                data = await _http_json("GET", "https://api.binance.com/api/v3/ticker/price", params={"symbol": symbol.upper()}, timeout_s=8)
                                px = float((data or {}).get("price") or 0.0)
                            elif ex == "bybit":
                                px = await _bybit_price(symbol, category="spot")
                            elif ex == "okx":
                                px = await _okx_public_price(symbol)
                            elif ex == "mexc":
                                px = await _mexc_public_price(symbol)
                            else:
                                px = await _gateio_public_price(symbol)
                    except Exception:
                        px = 0.0

                    if not px or px <= 0:
                        continue

                    entry_p = float(ref.get("entry_price") or 0.0)
                    qty = float(ref.get("qty") or 0.0)
                    tp1 = float(ref.get("tp1") or 0.0)
                    tp2 = float(ref.get("tp2") or 0.0)
                    sl = float(ref.get("sl") or 0.0)
                    tp1_hit = bool(ref.get("tp1_hit"))
                    be_moved = bool(ref.get("be_moved"))
                    direction = str(ref.get("direction") or ("LONG" if str(ref.get("side")).upper() in ("BUY","LONG") else "SHORT")).upper()
                    # If BE is active, be_price is stored; otherwise computed at TP1.
                    be_price = float(ref.get("be_price") or 0.0)

                    if qty <= 0:
                        await db_store.close_autotrade_position(
                            user_id=uid, signal_id=r.get("signal_id"), exchange=ex, market_type=mt, status="CLOSED"
                        )
                        continue

                    async def _close_market(q: float) -> None:
                        q2 = max(0.0, float(q))
                        if q2 <= 0:
                            return
                        # Normalize qty for exchange filters + minimal notional floor (spot 15 USDT, futures 10 USDT).
                        q2 = await _normalize_close_qty(ex=ex, mt=mt, symbol=symbol, qty=q2, px=px)
                        if q2 <= 0:
                            _log_rate_limited(
                                f"smart_skip_small:{uid}:{ex}:{mt}:{symbol}",
                                f"[SMART] skip close: qty too small for min notional/filters {ex}/{mt} {symbol} px={px}",
                                every_s=120,
                                level="debug",
                            )
                            # Emergency fallback: try close-all stop-market for Binance Futures (closePosition=true)
                            # using a trigger slightly beyond current px so it fires immediately.
                            try:
                                if mt == "futures" and ex == "binance":
                                    sp = float(px) * (0.999 if direction == "LONG" else 1.001)
                                    await _binance_futures_stop_market_close_all(
                                        api_key=api_key,
                                        api_secret=api_secret,
                                        symbol=symbol,
                                        side=("SELL" if direction == "LONG" else "BUY"),
                                        stop_price=sp,
                                        reduce_only=True,
                                    )
                            except Exception:
                                pass
                            return
                        # Spot short is not supported in this bot; virtual logic assumes LONG for spot.
                        if mt == "spot":
                            if ex == "binance":
                                await _binance_spot_market_sell_base(api_key=api_key, api_secret=api_secret, symbol=symbol, qty=q2)
                            elif ex == "bybit":
                                await _bybit_order_create(
                                    api_key=api_key,
                                    api_secret=api_secret,
                                    category="spot",
                                    symbol=symbol,
                                    side="Sell",
                                    order_type="Market",
                                    qty=q2,
                                )
                            elif ex == "okx":
                                await _okx_spot_market_sell(api_key=api_key, api_secret=api_secret, passphrase=passphrase or "", symbol=symbol, base_qty=q2)
                            elif ex == "mexc":
                                await _mexc_spot_market_sell(api_key=api_key, api_secret=api_secret, symbol=symbol, base_qty=q2)
                            else:
                                await _gateio_spot_market_sell(api_key=api_key, api_secret=api_secret, symbol=symbol, base_qty=q2)
                            return

                        # Futures
                        close_side = "SELL" if direction == "LONG" else "BUY"
                        if ex == "binance":
                            await _binance_futures_reduce_market(api_key=api_key, api_secret=api_secret, symbol=symbol, side=close_side, qty=q2)
                        elif ex == "bybit":
                            await _bybit_order_create(
                                api_key=api_key,
                                api_secret=api_secret,
                                category="linear",
                                symbol=symbol,
                                side=("Sell" if close_side == "SELL" else "Buy"),
                                order_type="Market",
                                qty=q2,
                                reduce_only=True,
                            )
                        else:
                            # Other futures exchanges aren't supported in this bot; close silently
                            raise ExchangeAPIError("unsupported futures exchange in virtual mode")

                    def _hit_sl() -> bool:
                        if sl <= 0:
                            return False
                        return (px <= sl) if direction == "LONG" else (px >= sl)

                    def _hit_tp(level: float) -> bool:
                        if level <= 0:
                            return False
                        return (px >= level) if direction == "LONG" else (px <= level)

                    def _hit_be() -> bool:
                        if be_price <= 0:
                            return False
                        return (px <= be_price) if direction == "LONG" else (px >= be_price)

                    # --- SMART virtual manager (no immediate SL/TP; momentum-aware TP; delayed BE; reversal exit) ---
                    now_ts = time.time()

                    # Tunables (env)
                    ARM_SL_AFTER_PCT = float(os.getenv("SMART_ARM_SL_AFTER_PCT", "0.22") or 0.22)          # arm normal SL after +X% move in favor
                    HARD_SL_PCT      = float(os.getenv("SMART_HARD_SL_PCT", "2.80") or 2.80)               # emergency stop while SL is not armed

                    # TP1 -> TP2 probability decision
                    TP2_PROB_STRONG  = float(os.getenv("SMART_TP2_PROB_STRONG", "0.72") or 0.72)           # hold to TP2 if prob >= strong
                    TP2_PROB_MED     = float(os.getenv("SMART_TP2_PROB_MED", "0.45") or 0.45)              # partial at TP1 if prob >= med
                    TP1_PARTIAL_PCT  = float(os.getenv("SMART_TP1_PARTIAL_PCT", "0.50") or 0.50)           # partial close size at TP1 (weak/medium)
                    FORCE_FULL_TP1_NO_TP2 = (os.getenv("SMART_FORCE_FULL_TP1_IF_NO_TP2", "1").strip().lower() not in ("0","false","no","off"))

                    # Momentum / structure
                    MOM_WINDOW_SEC   = float(os.getenv("SMART_MOMENTUM_WINDOW_SEC", "30") or 30)           # anchor update window
                    EARLY_EXIT_NEG_PCT= _env_float_mid("SMART_EARLY_EXIT_MOM_NEG_PCT", 0.16, is_mid)    # adverse move threshold vs anchor
                    EARLY_EXIT_CONSEC= _env_int_mid("SMART_EARLY_EXIT_CONSEC_NEG", 2, is_mid)       # consecutive adverse hits
                    EARLY_EXIT_MIN_GAIN= _env_float_mid("SMART_EARLY_EXIT_MIN_GAIN_PCT", 0.12, is_mid)  # only early-exit if already in profit

                    # Dynamic BE
                    BE_DELAY_SEC     = _env_float_mid("SMART_BE_DELAY_SEC", 25.0, is_mid)                  # delay before arming BE after partial TP1
                    BE_MIN_PCT       = _env_float_mid("SMART_BE_MIN_PCT", 0.06, is_mid)
                    BE_MAX_PCT       = float(os.getenv("SMART_BE_MAX_PCT", "0.45") or 0.45)
                    BE_VOL_MULT      = float(os.getenv("SMART_BE_VOL_MULT", "0.35") or 0.35)
                    BE_ARM_PCT_TO_TP2= float(os.getenv("SMART_BE_ARM_PCT_TO_TP2", "0.15") or 0.15)         # arm BE only after this progress to TP2

                    # Peak -> reversal
                    PEAK_MIN_GAIN_PCT= float(os.getenv("SMART_PEAK_MIN_GAIN_PCT", "0.42") or 0.42)
                    REV_EXIT_PCT     = float(os.getenv("SMART_REVERSAL_EXIT_PCT", "0.32") or 0.32)

                    # Persistent state in ref
                    state = str(ref.get("sm_state") or "INIT").upper()
                    armed_sl = bool(ref.get("armed_sl"))
                    tp1_seen = bool(ref.get("tp1_seen"))
                    tp1_partial = bool(ref.get("tp1_partial"))
                    tp1_ts = float(ref.get("tp1_ts") or 0.0)
                    be_pending = bool(ref.get("be_pending"))
                    last_px = float(ref.get("last_px") or px)
                    last_px_ts = float(ref.get("last_px_ts") or now_ts)
                    best_px = float(ref.get("best_px") or px)
                    hard_sl = float(ref.get("hard_sl") or 0.0)

                    dirty = False

                    # Initialize hard emergency SL once (while SL is not armed).
                    if hard_sl <= 0 and entry_p > 0:
                        if direction == "LONG":
                            hard_sl = entry_p * (1.0 - (max(0.2, HARD_SL_PCT) / 100.0))
                        else:
                            hard_sl = entry_p * (1.0 + (max(0.2, HARD_SL_PCT) / 100.0))
                        ref["hard_sl"] = float(hard_sl)
                        dirty = True

                    # Update best price (peak) in favorable direction
                    if direction == "LONG":
                        if px > best_px:
                            best_px = px
                            ref["best_px"] = float(best_px)
                            dirty = True
                    else:
                        if px < best_px:
                            best_px = px
                            ref["best_px"] = float(best_px)
                            dirty = True

                    # --- OPTIONAL trailing stop after TP1 (uses ATR(5m) if available) ---
                    TRAIL_AFTER_TP1 = (os.getenv("SMART_TRAIL_AFTER_TP1", "0").strip().lower() not in ("0","false","no","off"))
                    TRAIL_ATR_MULT  = float(os.getenv("SMART_TRAIL_ATR_MULT", "1.30") or 1.30)
                    TRAIL_CONFIRM_SEC = float(os.getenv("SMART_TRAIL_CONFIRM_SEC", "2") or 2)
                    # Prefer ATR(5m) from signal; fallback to ATR% (30m) stored on ref (if present)
                    atr5_ref = float(ref.get("atr5") or 0.0)
                    if atr5_ref <= 0 and entry_p > 0:
                        try:
                            atrp = float(ref.get("atr_pct") or 0.0)
                            atr5_ref = (atrp / 100.0) * entry_p if atrp > 0 else 0.0
                        except Exception:
                            atr5_ref = 0.0

                    if TRAIL_AFTER_TP1 and atr5_ref > 0 and bool(ref.get("tp1_seen")):
                        # activate only after TP1 decision (partial or hold); protect profits on pullback
                        if direction == "LONG":
                            trail_px = float(best_px) - (max(0.5, TRAIL_ATR_MULT) * float(atr5_ref))
                        else:
                            trail_px = float(best_px) + (max(0.5, TRAIL_ATR_MULT) * float(atr5_ref))
                        if trail_px > 0:
                            ref["trail_px"] = float(trail_px)
                            # track how long price stayed beyond trail
                            t_first = float(ref.get("trail_first_ts") or 0.0)
                            hit_trail = (px <= trail_px) if direction == "LONG" else (px >= trail_px)
                            if hit_trail:
                                if t_first <= 0:
                                    ref["trail_first_ts"] = float(now_ts)
                                    dirty = True
                                elif (now_ts - t_first) >= TRAIL_CONFIRM_SEC:
                                    # close remaining qty (market reduce-only)
                                    try:
                                        await _close_market(qty)
                                    except Exception as e:
                                        _log_rate_limited(f"smart_trail_close_err:{uid}:{ex}:{mt}:{symbol}", f"[SMART] trail close failed {ex}/{mt} {symbol}: {e}", every_s=30, level="warning")
                                    ref["sm_state"] = "TRAIL_EXIT"
                                    dirty = True
                            else:
                                if t_first != 0.0:
                                    ref["trail_first_ts"] = 0.0
                                    dirty = True

                    # Update momentum anchor price every MOM_WINDOW_SEC
                    if (now_ts - last_px_ts) >= max(5.0, MOM_WINDOW_SEC):
                        ref["last_px"] = float(px)
                        ref["last_px_ts"] = float(now_ts)
                        last_px = float(px)
                        last_px_ts = float(now_ts)
                        dirty = True

                    # Arm normal SL only after a small move in favor (so we don't "arm into a dump")
                    # For FUTURES we treat SL as structure: arm immediately if SL is provided.
                    # (Noise handling is done via confirmation in SMART STRUCTURAL SL below.)
                    if (not armed_sl) and float(sl or 0.0) > 0:
                        armed_sl = True
                        ref["armed_sl"] = True
                        ref["sm_state"] = "PROTECT"
                        dirty = True
                    # Reversal exit (close near the top/bottom when retrace starts)
                    if entry_p > 0 and best_px > 0:
                        if direction == "LONG":
                            gain_pct = (best_px / entry_p - 1.0) * 100.0
                            retr_pct = (1.0 - (px / best_px)) * 100.0 if best_px > 0 else 0.0
                            if gain_pct >= PEAK_MIN_GAIN_PCT and retr_pct >= REV_EXIT_PCT:
                                try:
                                    await _close_market(qty)
                                except Exception as e:
                                    _log_rate_limited(f"smart_close_err:{uid}:{ex}:{mt}:{symbol}", f"[SMART] close_market failed {ex}/{mt} {symbol}: {e}", every_s=60, level="info")
                                await db_store.close_autotrade_position(
                                    user_id=uid, signal_id=r.get("signal_id"), exchange=ex, market_type=mt, status="CLOSED"
                                )
                                continue
                        else:
                            gain_pct = (1.0 - (best_px / entry_p)) * 100.0
                            retr_pct = ((px / best_px) - 1.0) * 100.0 if best_px > 0 else 0.0
                            if gain_pct >= PEAK_MIN_GAIN_PCT and retr_pct >= REV_EXIT_PCT:
                                try:
                                    await _close_market(qty)
                                except Exception as e:
                                    _log_rate_limited(f"smart_close_err:{uid}:{ex}:{mt}:{symbol}", f"[SMART] close_market failed {ex}/{mt} {symbol}: {e}", every_s=60, level="info")
                                await db_store.close_autotrade_position(
                                    user_id=uid, signal_id=r.get("signal_id"), exchange=ex, market_type=mt, status="CLOSED"
                                )
                                continue

                    # Effective SL: before arming -> HARD SL only; after arming -> original SL (fallback to HARD if missing)

                    # --- Early-exit (structure break): consecutive adverse momentum hits while in profit ---
                    neg_count = int(ref.get("neg_count") or 0)
                    # adverse move vs anchor
                    adv_pct = 0.0
                    if last_px > 0:
                        if direction == "LONG":
                            adv_pct = max(0.0, (1.0 - (px / last_px)) * 100.0)
                        else:
                            adv_pct = max(0.0, (px / last_px - 1.0) * 100.0)

                    if adv_pct >= max(0.0, EARLY_EXIT_NEG_PCT):
                        neg_count += 1
                    else:
                        neg_count = 0

                    ref["neg_count"] = int(neg_count)
                    dirty = True

                    # only exit early if already in profit (protect wins)
                    gain_now_pct = 0.0
                    if entry_p > 0:
                        if direction == "LONG":
                            gain_now_pct = (px / entry_p - 1.0) * 100.0
                        else:
                            gain_now_pct = (1.0 - (px / entry_p)) * 100.0

                    if neg_count >= max(1, int(EARLY_EXIT_CONSEC)) and gain_now_pct >= float(EARLY_EXIT_MIN_GAIN):
                        try:
                            await _close_market(qty)
                        except Exception as e:
                            _log_rate_limited(f"smart_close_err:{uid}:{ex}:{mt}:{symbol}", f"[SMART] early-exit close failed {ex}/{mt} {symbol}: {e}", every_s=60, level="info")
                        await db_store.close_autotrade_position(
                            user_id=uid, signal_id=r.get("signal_id"), exchange=ex, market_type=mt, status="CLOSED"
                        )
                        continue
                    # --- SMART STRUCTURAL SL ---
                    # Goals:
                    # 1) If structure SL is broken -> close (decisive)
                    # 2) Avoid one-tick / one-wick false SL (confirmation)
                    # 3) If price quickly reclaims above SL (noise sweep) -> do NOT close
                    # 4) Keep HARD SL as emergency fallback
                    SL_CONFIRM_LAST = int(os.getenv("SMART_SL_CONFIRM_LAST", "3") or 3)
                    SL_CONFIRM_HITS = int(os.getenv("SMART_SL_CONFIRM_HITS", "2") or 2)
                    SL_CONFIRM_SEC  = float(os.getenv("SMART_SL_CONFIRM_SEC", "3") or 3)
                    SL_DEEP_PCT     = float(os.getenv("SMART_SL_DEEP_PCT", "0.15") or 0.15)

                    # Noise-sweep protection: allow quick reclaim above SL within grace window
                    SL_GRACE_SEC    = float(os.getenv("SMART_SL_GRACE_SEC", "6") or 6)
                    SL_GRACE_BASE   = float(os.getenv("SMART_SL_GRACE_BASE", "0") or 0)
                    SL_GRACE_ATR_K  = float(os.getenv("SMART_SL_GRACE_ATR_K", "0") or 0)
                    SL_USE_5M_CLOSE = (os.getenv("SMART_SL_USE_5M_CLOSE", "0").strip().lower() not in ("0","false","no","off"))
                    # Adaptive grace: grace = max(SMART_SL_GRACE_SEC, SMART_SL_GRACE_BASE + SMART_SL_GRACE_ATR_K * ATR%)
                    try:
                        atrp = float(ref.get("atr_pct") or ref.get("atr_pct_30m") or 0.0)
                    except Exception:
                        atrp = 0.0
                    try:
                        SL_GRACE_SEC = max(float(SL_GRACE_SEC), float(SL_GRACE_BASE) + float(SL_GRACE_ATR_K) * max(0.0, float(atrp)))
                    except Exception:
                        pass
                    SL_RECLAIM_PCT  = float(os.getenv("SMART_SL_RECLAIM_PCT", "0.04") or 0.04)  # reclaim buffer around SL
                    SL_BOUNCE_PCT   = float(os.getenv("SMART_SL_BOUNCE_PCT", "0.10") or 0.10)    # bounce from breach extreme

                    sl_struct = float(sl or 0.0)
                    sl_hard   = float(hard_sl or 0.0)

                    # Emergency HARD SL always active
                    def _hit_hard() -> bool:
                        if sl_hard <= 0:
                            return False
                        return (px <= sl_hard) if direction == "LONG" else (px >= sl_hard)

                    # Structural SL hit (raw)
                    def _hit_struct() -> bool:
                        if sl_struct <= 0:
                            return False
                        return (px <= sl_struct) if direction == "LONG" else (px >= sl_struct)

                    def _reclaimed() -> bool:
                        if sl_struct <= 0:
                            return False
                        # Price back inside with a tiny buffer to avoid flicker around SL
                        if direction == "LONG":
                            return px >= sl_struct * (1.0 + SL_RECLAIM_PCT / 100.0)
                        else:
                            return px <= sl_struct * (1.0 - SL_RECLAIM_PCT / 100.0)

                    def _deep_breach() -> bool:
                        if sl_struct <= 0 or entry_p <= 0:
                            return False
                        if direction == "LONG":
                            d = (sl_struct - px)
                        else:
                            d = (px - sl_struct)
                        if d <= 0:
                            return False
                        return (d / entry_p) * 100.0 >= SL_DEEP_PCT

                    # Update breach tracking state
                    sl_hits = int(ref.get("sl_hits") or 0)
                    sl_first_ts = float(ref.get("sl_first_ts") or 0.0)
                    sl_extreme = float(ref.get("sl_extreme") or 0.0)  # lowest px for LONG breach, highest for SHORT breach

                    crossed = _hit_struct()

                    if crossed:
                        if sl_first_ts <= 0:
                            sl_first_ts = now_ts
                            # init extreme
                            sl_extreme = px
                        # update extreme
                        if direction == "LONG":
                            sl_extreme = min(sl_extreme, px) if sl_extreme else px
                        else:
                            sl_extreme = max(sl_extreme, px) if sl_extreme else px
                        sl_hits += 1
                        ref["sl_hits"] = int(sl_hits)
                        ref["sl_first_ts"] = float(sl_first_ts)
                        ref["sl_extreme"] = float(sl_extreme)
                        dirty = True
                    else:
                        # If we reclaimed quickly after a sweep, reset immediately
                        if sl_hits != 0 or sl_first_ts != 0.0 or sl_extreme != 0.0:
                            ref["sl_hits"] = 0
                            ref["sl_first_ts"] = 0.0
                            ref["sl_extreme"] = 0.0
                            dirty = True

                    # Decide close:
                    # - HARD SL: immediate (failsafe)
                    # - Deep breach: immediate
                    # - Otherwise: require confirmation AND no quick reclaim/bounce within grace window
                    sl_close = False
                    if _hit_hard():
                        sl_close = True
                    elif crossed and _deep_breach():
                        sl_close = True
                    elif crossed:
                        # Grace: if price reclaims above SL quickly -> do not close
                        age = (now_ts - sl_first_ts) if sl_first_ts > 0 else 0.0

                        def _allow_struct_close() -> bool:
                            if not SL_USE_5M_CLOSE or sl_first_ts <= 0:
                                return True
                            try:
                                next5m = (math.floor(float(sl_first_ts) / 300.0) + 1.0) * 300.0
                            except Exception:
                                return True
                            # wait for the next 5m boundary (proxy for a candle close confirmation)
                            return float(now_ts) >= float(next5m)

                        # bounce check: if price bounced enough from extreme AND reclaimed -> reset (noise sweep)
                        bounced = False
                        if sl_extreme and entry_p > 0:
                            if direction == "LONG":
                                # bounce up from lowest point
                                bounced = ((px - sl_extreme) / entry_p) * 100.0 >= SL_BOUNCE_PCT
                            else:
                                bounced = ((sl_extreme - px) / entry_p) * 100.0 >= SL_BOUNCE_PCT

                        if age <= SL_GRACE_SEC and (_reclaimed() or bounced):
                            ref["sl_hits"] = 0
                            ref["sl_first_ts"] = 0.0
                            ref["sl_extreme"] = 0.0
                            dirty = True
                            sl_close = False
                        else:
                            # Confirm by hits or time-under (after grace window starts to matter)
                            if sl_hits >= SL_CONFIRM_HITS:
                                # require that we are past the minimal confirm sec OR past grace sec
                                if age >= min(SL_CONFIRM_SEC, SL_GRACE_SEC):
                                    sl_close = _allow_struct_close()
                            elif sl_first_ts > 0 and age >= SL_CONFIRM_SEC and age >= SL_GRACE_SEC:
                                sl_close = _allow_struct_close()

                    # --- SL (smart structural) ---
                    if sl_close:

                        try:
                            await _close_market(qty)
                        except Exception as e:
                            _log_rate_limited(f"smart_close_err:{uid}:{ex}:{mt}:{symbol}", f"[SMART] close_market failed {ex}/{mt} {symbol}: {e}", every_s=60, level="info")
                        await db_store.close_autotrade_position(
                            user_id=uid, signal_id=r.get("signal_id"), exchange=ex, market_type=mt, status="CLOSED"
                        )
                        continue

                    # --- TP2: if reached, close 100% ---
                    if tp2 > 0 and _hit_tp(tp2):
                        try:
                            await _close_market(qty)
                        except Exception as e:
                            _log_rate_limited(f"smart_close_err:{uid}:{ex}:{mt}:{symbol}", f"[SMART] close_market failed {ex}/{mt} {symbol}: {e}", every_s=60, level="info")
                        await db_store.close_autotrade_position(
                            user_id=uid, signal_id=r.get("signal_id"), exchange=ex, market_type=mt, status="CLOSED"
                        )
                        continue

                    # --- TP1: probability engine (TP2 vs take profit now) ---
                    if (tp1 > 0) and (not tp1_seen) and _hit_tp(tp1):
                        # Momentum vs anchor (%)
                        mom_fav_pct = 0.0
                        mom_adv_pct = 0.0
                        if last_px > 0:
                            if direction == "LONG":
                                mom_fav_pct = max(0.0, (px / last_px - 1.0) * 100.0)
                                mom_adv_pct = max(0.0, (1.0 - (px / last_px)) * 100.0)
                            else:
                                mom_fav_pct = max(0.0, (1.0 - (px / last_px)) * 100.0)
                                mom_adv_pct = max(0.0, (px / last_px - 1.0) * 100.0)

                        # Progress to TP2 (0..1) and distance remaining (%)
                        prob_tp2 = 0.0
                        if tp2 > 0 and entry_p > 0 and tp2 != entry_p:
                            if direction == "LONG":
                                prog = (px - entry_p) / (tp2 - entry_p)
                                dist = max(0.0, (tp2 - px) / max(1e-9, px)) * 100.0
                            else:
                                prog = (entry_p - px) / (entry_p - tp2)
                                dist = max(0.0, (px - tp2) / max(1e-9, px)) * 100.0
                            prog = max(0.0, min(1.0, float(prog)))
                            # If momentum is strong relative to remaining distance, higher probability.
                            mom_factor = 0.0
                            if dist > 0:
                                mom_factor = max(0.0, min(1.0, mom_fav_pct / max(0.05, dist)))
                            # Simple, stable blend (no ML, no magic)
                            prob_tp2 = max(0.0, min(1.0, 0.55 * prog + 0.45 * mom_factor))

                        ref["tp1_seen"] = True
                        tp1_seen = True
                        ref["tp2_prob"] = float(prob_tp2)
                        dirty = True

                        # Decision
                        if (tp2 <= 0) and FORCE_FULL_TP1_NO_TP2:
                            mode = "FULL_TP1_NO_TP2"
                        elif prob_tp2 >= TP2_PROB_STRONG:
                            mode = "HOLD_TO_TP2"
                        elif prob_tp2 >= TP2_PROB_MED:
                            mode = "PARTIAL_TP1"
                        else:
                            mode = "FULL_TP1"

                        ref["tp1_mode"] = mode
                        dirty = True

                        if mode == "HOLD_TO_TP2":
                            # Strong conditions: skip TP1 close, aim for TP2 (close 100%)
                            ref["tp1_hit"] = True
                            dirty = True
                        elif mode == "PARTIAL_TP1":
                            q_close = max(0.0, qty * max(0.05, min(0.95, TP1_PARTIAL_PCT)))
                            q_rem = max(0.0, qty - q_close)
                            if q_close > 0:
                                try:
                                    await _close_market(q_close)
                                except Exception as e:
                                    _log_rate_limited(f"smart_close_err:{uid}:{ex}:{mt}:{symbol}", f"[SMART] close market failed {ex}/{mt} {symbol}: {e}", every_s=60, level="info")
                            ref["qty"] = float(q_rem)
                            qty = float(q_rem)
                            ref["tp1_hit"] = True
                            ref["tp1_partial"] = True
                            ref["tp1_ts"] = float(now_ts)
                            ref["be_pending"] = True
                            dirty = True
                        else:
                            # FULL_TP1 / FULL_TP1_NO_TP2
                            try:
                                await _close_market(qty)
                            except Exception as e:
                                _log_rate_limited(f"smart_close_err:{uid}:{ex}:{mt}:{symbol}", f"[SMART] close market failed {ex}/{mt} {symbol}: {e}", every_s=60, level="info")
                            await db_store.close_autotrade_position(
                                user_id=uid, signal_id=r.get("signal_id"), exchange=ex, market_type=mt, status="CLOSED"
                            )
                            continue
# --- Arm BE only AFTER partial TP1 and after a short delay + confirmation
                    if bool(ref.get("tp1_partial")) and bool(ref.get("be_pending")) and (not be_moved) and entry_p > 0:
                        if (now_ts - float(ref.get("tp1_ts") or 0.0)) >= max(0.0, BE_DELAY_SEC):
                            # Confirm: still not below entry (for long) / not above entry (for short)
                            ok_confirm = (px >= entry_p) if direction == "LONG" else (px <= entry_p)
                            if ok_confirm:
                                # Arm BE only if enough progress to TP2 (optional)
                                ok_prog = True
                                if tp2 > 0 and entry_p > 0 and tp2 != entry_p and BE_ARM_PCT_TO_TP2 > 0:
                                    if direction == "LONG":
                                        prog2 = (px - entry_p) / (tp2 - entry_p)
                                    else:
                                        prog2 = (entry_p - px) / (entry_p - tp2)
                                    ok_prog = float(prog2) >= float(BE_ARM_PCT_TO_TP2)

                                if ok_prog:
                                    # Dynamic BE buffer: min + vol_mult*vol, clamped
                                    vol_pct = 0.0
                                    if last_px > 0:
                                        vol_pct = abs(px / last_px - 1.0) * 100.0
                                    buf_pct = max(float(BE_MIN_PCT), min(float(BE_MAX_PCT), float(BE_MIN_PCT) + float(BE_VOL_MULT) * float(vol_pct)))

                                    be_fee = float(_be_with_fee_buffer(entry_p, direction=direction) or 0.0)
                                    if direction == "LONG":
                                        be_dyn = float(entry_p) * (1.0 + (buf_pct / 100.0))
                                        be_price = max(be_fee, be_dyn)
                                    else:
                                        be_dyn = float(entry_p) * (1.0 - (buf_pct / 100.0))
                                        be_price = min(be_fee if be_fee > 0 else be_dyn, be_dyn)

                                    ref["be_price"] = float(be_price)
                                    ref["be_moved"] = True
                                    ref["be_pending"] = False
                                    be_moved = True
                                    dirty = True

                    # --- BE hit: close remaining ---
                    if bool(ref.get("be_moved")) and _hit_be():
                        try:
                            await _close_market(qty)
                        except Exception as e:
                            _log_rate_limited(f"smart_close_err:{uid}:{ex}:{mt}:{symbol}", f"[SMART] close_market failed {ex}/{mt} {symbol}: {e}", every_s=60, level="info")
                        await db_store.close_autotrade_position(
                            user_id=uid, signal_id=r.get("signal_id"), exchange=ex, market_type=mt, status="CLOSED"
                        )
                        continue

                    # Persist updated ref if changed
                    if dirty:
                        try:
                            await db_store.update_autotrade_order_ref(row_id=int(r.get("id") or 0), api_order_ref=json.dumps(ref))
                        except Exception:
                            pass

                    continue
# If TP1 filled and BE not moved: cancel SL and place new SL at entry (BE)
                if tp1_id and (not be_moved) and be_price > 0:
                    if ex == "binance":
                        try:
                            o = await _binance_order_status(api_key=api_key, api_secret=api_secret, symbol=symbol, order_id=int(tp1_id), futures=futures)
                            tp1_filled = (str(o.get("status")) == "FILLED")
                        except ExchangeAPIError as e:
                            try:
                                notify_api_error(uid, f"⚠️ Auto-trade ERROR ({ex} {mt})\n{str(e)[:200]}")
                            except Exception:
                                pass
                            continue
                    else:
                        category = str(ref.get("category") or ("spot" if mt == "spot" else "linear"))
                        try:
                            o = await _bybit_order_status(api_key=api_key, api_secret=api_secret, category=category, symbol=symbol, order_id=str(tp1_id))
                            lst = ((o.get("result") or {}).get("list") or [])
                            st = (lst[0].get("orderStatus") if (lst and isinstance(lst[0], dict)) else None)
                            tp1_filled = (str(st) == "Filled")
                        except ExchangeAPIError as e:
                            try:
                                notify_api_error(uid, f"⚠️ Auto-trade ERROR ({ex} {mt})\n{str(e)[:200]}")
                            except Exception:
                                pass
                            continue
                    if tp1_filled:
                        try:
                            if sl_id:
                                if ex == "binance":
                                    await _binance_cancel_order(api_key=api_key, api_secret=api_secret, symbol=symbol, order_id=int(sl_id), futures=futures)
                                else:
                                    category = str(ref.get("category") or ("spot" if mt == "spot" else "linear"))
                                    await _bybit_cancel_order(api_key=api_key, api_secret=api_secret, category=category, symbol=symbol, order_id=str(sl_id))
                        except Exception:
                            pass

                        # Accumulate realized PnL on partial close (TP1) to improve final PnL accuracy.
                        # Keep this light: reuse TP1 status response ("o") and optionally fetch entry order once.
                        try:
                            if not bool(ref.get("tp1_realized_done")):
                                # Determine executed qty and avg price for TP1
                                if ex == "binance":
                                    tp1_qty_exec = _as_float(o.get("executedQty"), 0.0)
                                    tp1_avg = _binance_avg_price(o)
                                else:
                                    try:
                                        lst0 = ((o.get("result") or {}).get("list") or [])
                                        od0 = lst0[0] if (lst0 and isinstance(lst0[0], dict)) else {}
                                    except Exception:
                                        od0 = {}
                                    tp1_qty_exec = _as_float(od0.get("cumExecQty"), 0.0)
                                    tp1_avg = _bybit_avg_price(o)

                                # Get entry avg price (best-effort)
                                entry_avg = _as_float(ref.get("entry_avg_price"), 0.0)
                                if entry_avg <= 0:
                                    entry_id = ref.get("entry_order_id")
                                    if entry_id:
                                        if ex == "binance":
                                            eo = await _binance_order_status(api_key=api_key, api_secret=api_secret, symbol=symbol, order_id=int(entry_id), futures=futures)
                                            entry_avg = _binance_avg_price(eo)
                                        else:
                                            category = str(ref.get("category") or ("spot" if mt == "spot" else "linear"))
                                            eo = await _bybit_order_status(api_key=api_key, api_secret=api_secret, category=category, symbol=symbol, order_id=str(entry_id))
                                            entry_avg = _bybit_avg_price(eo)
                                if entry_avg <= 0:
                                    entry_avg = _as_float(ref.get("entry_price"), 0.0)

                                if tp1_qty_exec > 0 and tp1_avg > 0 and entry_avg > 0:
                                    side_local = str(ref.get("side") or "BUY").upper()
                                    if side_local == "BUY":
                                        pnl_gross = (tp1_avg - entry_avg) * tp1_qty_exec
                                    else:
                                        pnl_gross = (entry_avg - tp1_avg) * tp1_qty_exec

                                    fr_local = float(FEE_RATE_SPOT if str(mt).lower() == "spot" else FEE_RATE_FUTURES)
                                    fees_est = fr_local * ((entry_avg * tp1_qty_exec) + (tp1_avg * tp1_qty_exec))

                                    ref["realized_pnl_usdt"] = float(_as_float(ref.get("realized_pnl_usdt"), 0.0) + pnl_gross)
                                    ref["realized_fee_usdt"] = float(_as_float(ref.get("realized_fee_usdt"), 0.0) + fees_est)
                                    ref["realized_qty"] = float(_as_float(ref.get("realized_qty"), 0.0) + tp1_qty_exec)
                                    ref["entry_avg_price"] = float(entry_avg)
                                    ref["tp1_realized_done"] = True
                        except Exception:
                            # Best-effort only; do not block management.
                            pass

                        # Place new SL at BE
                        if ex == "binance":
                            if futures:
                                new_sl = await _binance_futures_stop_market_close_all(api_key=api_key, api_secret=api_secret, symbol=symbol, side=close_side, stop_price=be_price)
                            else:
                                qty = float(ref.get("qty") or 0.0)
                                new_sl = await _binance_spot_stop_loss_limit_sell(api_key=api_key, api_secret=api_secret, symbol=symbol, qty=qty, stop_price=be_price)
                        else:
                            category = str(ref.get("category") or ("spot" if mt == "spot" else "linear"))
                            qty = float(ref.get("qty") or 0.0)
                            new_sl = await _bybit_order_create(
                                api_key=api_key,
                                api_secret=api_secret,
                                category=category,
                                symbol=symbol,
                                side=close_side,
                                order_type="Market",
                                qty=qty,
                                trigger_price=be_price,
                                reduce_only=(True if mt == "futures" else None),
                                close_on_trigger=(True if mt == "futures" else None),
                            )

                        if ex == "binance":
                            ref["sl_order_id"] = new_sl.get("orderId")
                        else:
                            ref["sl_order_id"] = str(((new_sl.get("result") or {}).get("orderId") or "")) or None
                        ref["be_moved"] = True
                        await db_store.update_autotrade_order_ref(row_id=int(r.get("id")), api_order_ref=json.dumps(ref))

                # Close detection: SL filled OR (TP2 filled if exists else TP1 filled)
                # If SL FILLED -> closed
                if sl_id:
                    if ex == "binance":
                        try:
                            o_sl = await _binance_order_status(api_key=api_key, api_secret=api_secret, symbol=symbol, order_id=int(sl_id), futures=futures)
                            sl_filled = (str(o_sl.get("status")) == "FILLED")
                        except ExchangeAPIError as e:
                            try:
                                notify_api_error(uid, f"⚠️ Auto-trade ERROR ({ex} {mt})\n{str(e)[:200]}")
                            except Exception:
                                pass
                            continue
                    else:
                        category = str(ref.get("category") or ("spot" if mt == "spot" else "linear"))
                        try:
                            o_sl = await _bybit_order_status(api_key=api_key, api_secret=api_secret, category=category, symbol=symbol, order_id=str(sl_id))
                            lst = ((o_sl.get("result") or {}).get("list") or [])
                            stx = (lst[0].get("orderStatus") if (lst and isinstance(lst[0], dict)) else None)
                            sl_filled = (str(stx) == "Filled")
                        except ExchangeAPIError as e:
                            try:
                                notify_api_error(uid, f"⚠️ Auto-trade ERROR ({ex} {mt})\n{str(e)[:200]}")
                            except Exception:
                                pass
                            continue
                    if sl_filled:
                        pnl_usdt = None
                        roi_percent = None
                        try:
                            entry_id = ref.get("entry_order_id")
                            entry_order = None
                            if entry_id:
                                if ex == "binance":
                                    entry_order = await _binance_order_status(
                                        api_key=api_key,
                                        api_secret=api_secret,
                                        symbol=symbol,
                                        order_id=int(entry_id),
                                        futures=futures,
                                    )
                                else:
                                    category = str(ref.get("category") or ("spot" if mt == "spot" else "linear"))
                                    entry_order = await _bybit_order_status(
                                        api_key=api_key,
                                        api_secret=api_secret,
                                        category=category,
                                        symbol=symbol,
                                        order_id=str(entry_id),
                                    )
                            if entry_order:
                                pnl_usdt, roi_percent = _calc_autotrade_pnl_from_orders(
                                    ref=ref,
                                    entry_order=entry_order,
                                    exit_order=o_sl,
                                    exchange=ex,
                                    market_type=mt,
                                    allocated_usdt=float(r.get("allocated_usdt") or 0.0),
                                )
                        except Exception:
                            pnl_usdt = None
                            roi_percent = None
                        await db_store.close_autotrade_position(
                            user_id=uid,
                            signal_id=r.get("signal_id"),
                            exchange=ex,
                            market_type=mt,
                            status="CLOSED",
                            pnl_usdt=pnl_usdt,
                            roi_percent=roi_percent,
)
                        try:
                            acc2 = await db_store.get_autotrade_access(uid)
                            if bool(acc2.get("autotrade_stop_after_close")):
                                if await db_store.count_open_autotrade_positions(uid) <= 0:
                                    await db_store.finalize_autotrade_disable(uid)
                        except Exception:
                            pass
                        continue

                target_id = tp2_id or tp1_id
                if target_id:
                    if ex == "binance":
                        try:
                            o_tp = await _binance_order_status(api_key=api_key, api_secret=api_secret, symbol=symbol, order_id=int(target_id), futures=futures)
                            tp_filled = (str(o_tp.get("status")) == "FILLED")
                        except ExchangeAPIError as e:
                            try:
                                notify_api_error(uid, f"⚠️ Auto-trade ERROR ({ex} {mt})\n{str(e)[:200]}")
                            except Exception:
                                pass
                            continue
                    else:
                        category = str(ref.get("category") or ("spot" if mt == "spot" else "linear"))
                        try:
                            o_tp = await _bybit_order_status(api_key=api_key, api_secret=api_secret, category=category, symbol=symbol, order_id=str(target_id))
                            lst = ((o_tp.get("result") or {}).get("list") or [])
                            stx = (lst[0].get("orderStatus") if (lst and isinstance(lst[0], dict)) else None)
                            tp_filled = (str(stx) == "Filled")
                        except ExchangeAPIError as e:
                            try:
                                notify_api_error(uid, f"⚠️ Auto-trade ERROR ({ex} {mt})\n{str(e)[:200]}")
                            except Exception:
                                pass
                            continue
                    if tp_filled:
                        pnl_usdt = None
                        roi_percent = None
                        try:
                            entry_id = ref.get("entry_order_id")
                            entry_order = None
                            if entry_id:
                                if ex == "binance":
                                    entry_order = await _binance_order_status(
                                        api_key=api_key,
                                        api_secret=api_secret,
                                        symbol=symbol,
                                        order_id=int(entry_id),
                                        futures=futures,
                                    )
                                else:
                                    category = str(ref.get("category") or ("spot" if mt == "spot" else "linear"))
                                    entry_order = await _bybit_order_status(
                                        api_key=api_key,
                                        api_secret=api_secret,
                                        category=category,
                                        symbol=symbol,
                                        order_id=str(entry_id),
                                    )
                            if entry_order:
                                pnl_usdt, roi_percent = _calc_autotrade_pnl_from_orders(
                                    ref=ref,
                                    entry_order=entry_order,
                                    exit_order=o_tp,
                                    exchange=ex,
                                    market_type=mt,
                                    allocated_usdt=float(r.get("allocated_usdt") or 0.0),
                                )
                        except Exception:
                            pnl_usdt = None
                            roi_percent = None
                        await db_store.close_autotrade_position(
                            user_id=uid,
                            signal_id=r.get("signal_id"),
                            exchange=ex,
                            market_type=mt,
                            status="CLOSED",
                            pnl_usdt=pnl_usdt,
                            roi_percent=roi_percent,
                        )
                        try:
                            acc2 = await db_store.get_autotrade_access(uid)
                            if bool(acc2.get("autotrade_stop_after_close")):
                                if await db_store.count_open_autotrade_positions(uid) <= 0:
                                    await db_store.finalize_autotrade_disable(uid)
                        except Exception:
                            pass

        except ExchangeAPIError as e:
            # global API issue; no user id here
            logger.warning("Auto-trade manager API error: %s", e)
        except Exception:
            logger.exception("Auto-trade manager loop error")

        await asyncio.sleep(10)


# --- i18n template safety guard (prevents leaking {placeholders} to users) ---
_UNFILLED_RE = re.compile(r'(?<!\{)\{[a-zA-Z0-9_]+\}(?!\})')

def _sanitize_template_text(uid: int, text: str, ctx: str = "") -> str:
    if not text:
        return text
    hits = _UNFILLED_RE.findall(text)
    if hits:
        logger.error("Unfilled i18n placeholders for uid=%s ctx=%s hits=%s text=%r", uid, ctx, sorted(set(hits)), text)
        text = _UNFILLED_RE.sub("", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

async def safe_send(bot, chat_id: int, text: str, *, ctx: str = "", **kwargs):
    text = _sanitize_template_text(chat_id, text, ctx=ctx)
    # Never recurse. Send via bot API.
    return await bot.send_message(chat_id, text, **kwargs)



# ------------------ ENV helpers ------------------
def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default
def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "yes", "y", "on")


# --- i18n helpers (loaded from i18n.json; no hardcoded auto-close texts) ---
I18N_FILE = Path(__file__).with_name("i18n.json")
LANG_FILE = Path("langs.json")

_I18N_CACHE: dict = {}
_I18N_MTIME: float = 0.0
_LANG_CACHE: dict[int, str] = {}
_LANG_CACHE_MTIME: float = 0.0

def _load_i18n() -> dict:
    global _I18N_CACHE, _I18N_MTIME
    try:
        if I18N_FILE.exists():
            mt = I18N_FILE.stat().st_mtime
            if _I18N_CACHE and mt == _I18N_MTIME:
                return _I18N_CACHE
            data = json.loads(I18N_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "ru" in data and "en" in data:
                _I18N_CACHE = data
                _I18N_MTIME = mt
                return data
    except Exception:
        pass
    if not _I18N_CACHE:
        _I18N_CACHE = {"ru": {}, "en": {}}
    return _I18N_CACHE

def _load_langs_if_needed() -> None:
    global _LANG_CACHE, _LANG_CACHE_MTIME
    try:
        if not LANG_FILE.exists():
            _LANG_CACHE = {}
            _LANG_CACHE_MTIME = 0.0
            return
        mt = LANG_FILE.stat().st_mtime
        if mt == _LANG_CACHE_MTIME:
            return
        raw = json.loads(LANG_FILE.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            tmp: dict[int, str] = {}
            for k, v in raw.items():
                try:
                    uid = int(k)
                except Exception:
                    continue
                vv = (v or "").lower().strip()
                tmp[uid] = "en" if vv == "en" else "ru"
            _LANG_CACHE = tmp
            _LANG_CACHE_MTIME = mt
    except Exception:
        pass

def _get_lang(uid: int) -> str:
    _load_langs_if_needed()
    return _LANG_CACHE.get(int(uid), "ru")

def _tr(uid: int, key: str) -> str:
    lang = _get_lang(uid)
    i18n = _load_i18n()
    d = i18n.get(lang, {})
    if key in d:
        return d.get(key, key)
    return i18n.get("en", {}).get(key, key)

class _SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"

def _trf(uid: int, key: str, **kwargs) -> str:
    tmpl = _tr(uid, key)
    try:
        return str(tmpl).format_map(_SafeDict(**kwargs))
    except Exception:
        return str(tmpl)

def _market_label(uid: int, market: str) -> str:
    return _tr(uid, "lbl_spot") if str(market).upper() == "SPOT" else _tr(uid, "lbl_futures")
# --- /i18n helpers ---


# ------------------ backend-only metrics helpers ------------------

def calc_profit_pct(entry: float, tp: float, direction: str) -> float:
    """Estimated profit percent from entry to tp for LONG/SHORT."""
    try:
        entry = float(entry)
        tp = float(tp)
        if entry == 0:
            return 0.0
        side = str(direction or "LONG").upper().strip()
        if side == "SHORT":
            return (entry - tp) / entry * 100.0
        return (tp - entry) / entry * 100.0
    except Exception:
        return 0.0

def default_futures_leverage() -> str:
    v = os.getenv("FUTURES_LEVERAGE_DEFAULT", "5").strip()
    return v or "5"

def open_metrics(sig: "Signal") -> dict:
    """Return placeholders for OPEN templates, computed only here (backend-only)."""
    side = (getattr(sig, "direction", "") or "LONG").upper().strip()
    mkt = (getattr(sig, "market", "") or "FUTURES").upper().strip()
    # prefer TP2 for estimate; fallback to TP1
    tp = getattr(sig, "tp2", 0.0) or getattr(sig, "tp1", 0.0) or 0.0
    entry = getattr(sig, "entry", 0.0) or 0.0
    profit = calc_profit_pct(entry, tp, side)
    return {
        "side": side,
        "market": mkt,
        "profit": round(float(profit), 1),
        "lev": default_futures_leverage(),
        "rr": round(float(getattr(sig, "rr", 0.0) or 0.0), 1),
    }



def _price_debug_block(uid: int, *, price: float, source: str, side: str, sl: float | None, tp1: float | None, tp2: float | None, sl_label_key: str | None = None) -> str:
    """Human-readable debug block for a trade.

    Important: all visible labels must come from i18n (see i18n.json keys).
    """
    try:
        price_f = float(price)
    except Exception:
        return ""

    side_u = (side or "LONG").upper()

    def hit_tp(lvl: float) -> bool:
        return price_f >= lvl if side_u == "LONG" else price_f <= lvl

    def hit_sl(lvl: float) -> bool:
        return price_f <= lvl if side_u == "LONG" else price_f >= lvl

    lines = [
        f"💹 {_tr(uid, 'lbl_price_now')}: {price_f:.6f}",
        f"🔌 {_tr(uid, 'lbl_price_src')}: {source}",
    ]

    checks: list[str] = []

    # SL / BE label override (e.g. after TP1 we may want to show BE instead of SL)
    sl_key = (sl_label_key or "lbl_sl").strip() if isinstance(sl_label_key, str) else "lbl_sl"
    sl_lbl = _tr(uid, sl_key)

    if sl is not None and float(sl) > 0:
        lvl = float(sl)
        checks.append(f"{sl_lbl}: {lvl:.6f} {'✅' if hit_sl(lvl) else '❌'}")

    if tp1 is not None and float(tp1) > 0:
        lvl = float(tp1)
        checks.append(f"{_tr(uid, 'lbl_tp1')}: {lvl:.6f} {'✅' if hit_tp(lvl) else '❌'}")

    if tp2 is not None and float(tp2) > 0:
        lvl = float(tp2)
        checks.append(f"{_tr(uid, 'lbl_tp2')}: {lvl:.6f} {'✅' if hit_tp(lvl) else '❌'}")

    if checks:
        lines.append(f"🧪 {_tr(uid, 'lbl_check')}:")
        lines.extend(["• " + c for c in checks])

    return "\n".join(lines)


from zoneinfo import ZoneInfo
MSK = ZoneInfo("Europe/Moscow")

def fmt_dt_msk(d):
    if not d:
        return "—"
    import datetime as dt
    if isinstance(d, dt.datetime):
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(MSK).strftime("%d.%m.%Y %H:%M")
    return "—"

def fmt_pnl_pct(p: float) -> str:
    try:
        p = float(p)
        sign = "+" if p > 0 else ""
        return f"{sign}{p:.1f}%"
    except Exception:
        return "0.0%"

TOP_N = _env_int("TOP_N", 50)
# TOP_N controls how many USDT symbols to scan. Set TOP_N=0 to scan ALL USDT pairs.
SCAN_INTERVAL_SECONDS = max(30, _env_int("SCAN_INTERVAL_SECONDS", 150))
CONFIDENCE_MIN = max(0, min(100, _env_int("CONFIDENCE_MIN", 80)))
# --- TA Quality mode (strict/medium) ---
SIGNAL_MODE = (os.getenv("SIGNAL_MODE", "").strip().lower() or "strict")
if SIGNAL_MODE not in ("strict", "medium"):
    SIGNAL_MODE = "strict"

# Per-market minimum TA score (0..100). Can be overridden via env.
_def_spot = 78 if SIGNAL_MODE == "strict" else 65
_def_fut = 74 if SIGNAL_MODE == "strict" else 62
TA_MIN_SCORE_SPOT = max(0, min(100, _env_int("TA_MIN_SCORE_SPOT", _def_spot)))
TA_MIN_SCORE_FUTURES = max(0, min(100, _env_int("TA_MIN_SCORE_FUTURES", _def_fut)))

# Trend strength filter (ADX)
TA_MIN_ADX = max(0.0, _env_float("TA_MIN_ADX", 22.0 if SIGNAL_MODE == "strict" else 17.0))

# Volume confirmation: require volume >= SMA(volume,20) * X
TA_MIN_VOL_X = max(0.5, _env_float("TA_MIN_VOL_X", 1.25 if SIGNAL_MODE == "strict" else 1.05))

# MACD histogram threshold (momentum). For LONG require hist >= +X, for SHORT <= -X
TA_MIN_MACD_H = max(0.0, _env_float("TA_MIN_MACD_H", 0.0 if SIGNAL_MODE == "strict" else 0.0))

# Bollinger rule: if 1, require breakout/cross of midline in direction of trend on the last candle
TA_BB_REQUIRE_BREAKOUT = _env_bool("TA_BB_REQUIRE_BREAKOUT", True if SIGNAL_MODE == "strict" else False)

# RSI guard rails
TA_RSI_MAX_LONG = max(1.0, _env_float("TA_RSI_MAX_LONG", 68.0 if SIGNAL_MODE == "strict" else 72.0))
TA_RSI_MIN_SHORT = max(0.0, _env_float("TA_RSI_MIN_SHORT", 32.0 if SIGNAL_MODE == "strict" else 28.0))

# Multi-timeframe confirmation: if 1, require 4h and 1h trend alignment (recommended).
# If 0, allow 4h trend only (more signals).
TA_REQUIRE_1H_TREND = _env_bool("TA_REQUIRE_1H_TREND", True if SIGNAL_MODE == "strict" else False)

COOLDOWN_MINUTES = max(1, _env_int("COOLDOWN_MINUTES", 180))

USE_REAL_PRICE = _env_bool("USE_REAL_PRICE", True)
# Price source selection per market: BINANCE / BYBIT / MEDIAN
SPOT_PRICE_SOURCE = (os.getenv("SPOT_PRICE_SOURCE", "MEDIAN").strip().upper() or "MEDIAN")
FUTURES_PRICE_SOURCE = (os.getenv("FUTURES_PRICE_SOURCE", "MEDIAN").strip().upper() or "MEDIAN")
TRACK_INTERVAL_SECONDS = max(1, _env_int("TRACK_INTERVAL_SECONDS", 3))
# Backward-compatible default partial close percent (legacy)
TP1_PARTIAL_CLOSE_PCT = max(0, min(100, _env_int("TP1_PARTIAL_CLOSE_PCT", 50)))

# New per-market settings
TP1_PARTIAL_CLOSE_PCT_SPOT = max(0, min(100, _env_int("TP1_PARTIAL_CLOSE_PCT_SPOT", TP1_PARTIAL_CLOSE_PCT)))
TP1_PARTIAL_CLOSE_PCT_FUTURES = max(0, min(100, _env_int("TP1_PARTIAL_CLOSE_PCT_FUTURES", TP1_PARTIAL_CLOSE_PCT)))

BE_AFTER_TP1_SPOT = _env_bool("BE_AFTER_TP1_SPOT", True)
BE_AFTER_TP1_FUTURES = _env_bool("BE_AFTER_TP1_FUTURES", True)

# Fee-protected BE: exit remaining position at entry +/- fee buffer so BE is not negative after fees (model)
BE_FEE_BUFFER = _env_bool("BE_FEE_BUFFER", True)
FEE_RATE_SPOT = max(0.0, _env_float("FEE_RATE_SPOT", 0.001))        # e.g. 0.001 = 0.10%
FEE_RATE_FUTURES = max(0.0, _env_float("FEE_RATE_FUTURES", 0.001))  # e.g. 0.001 = 0.10%
FEE_BUFFER_MULTIPLIER = max(0.0, _env_float("FEE_BUFFER_MULTIPLIER", 2.0))  # 2 = entry+exit fees
ATR_MULT_SL = max(0.5, _env_float("ATR_MULT_SL", 1.5))
TP1_R = max(0.5, _env_float("TP1_R", 1.5))
TP2_R = max(1.0, _env_float("TP2_R", 3.0))
# --- Upgrades: Adaptive TP2 + delayed BE arming (optional) ---
ADAPTIVE_TP2 = _env_bool("ADAPTIVE_TP2", False)
ADAPTIVE_TP2_ADX_STRONG = _env_float("ADAPTIVE_TP2_ADX_STRONG", 30.0)
ADAPTIVE_TP2_ADX_MED = _env_float("ADAPTIVE_TP2_ADX_MED", 25.0)
ADAPTIVE_TP2_R_STRONG = _env_float("ADAPTIVE_TP2_R_STRONG", 2.8)
ADAPTIVE_TP2_R_MED = _env_float("ADAPTIVE_TP2_R_MED", 2.45)
ADAPTIVE_TP2_R_WEAK = _env_float("ADAPTIVE_TP2_R_WEAK", 2.2)

# When >0, BE (SL->BE) becomes "armed" only after price moves from TP1 towards TP2 by this fraction.
# Helps TP2 by reducing BE whipsaws after TP1. 0 = immediate BE (legacy behavior).
BE_ARM_PCT_TO_TP2 = max(0.0, min(1.0, _env_float("BE_ARM_PCT_TO_TP2", 0.0)))

# News filter
NEWS_FILTER = _env_bool("NEWS_FILTER", False)
CRYPTOPANIC_TOKEN = os.getenv("CRYPTOPANIC_TOKEN", "").strip()
CRYPTOPANIC_PUBLIC = _env_bool("CRYPTOPANIC_PUBLIC", False)
CRYPTOPANIC_REGIONS = os.getenv("CRYPTOPANIC_REGIONS", "en").strip() or "en"
CRYPTOPANIC_KIND = os.getenv("CRYPTOPANIC_KIND", "news").strip() or "news"
NEWS_LOOKBACK_MIN = max(5, _env_int("NEWS_LOOKBACK_MIN", 60))
NEWS_ACTION = os.getenv("NEWS_ACTION", "FUTURES_OFF").strip().upper()  # FUTURES_OFF / PAUSE_ALL

# Macro filter
MACRO_FILTER = _env_bool("MACRO_FILTER", False)

# Orderbook filter (optional)
ORDERBOOK_FILTER = _env_bool("ORDERBOOK_FILTER", False)
# ------------------ Orderbook filter (FUTURES only recommended) ------------------
# Backward compatible alias: ORDERBOOK_FILTER
USE_ORDERBOOK = _env_bool("USE_ORDERBOOK", ORDERBOOK_FILTER)
ORDERBOOK_EXCHANGES = [x.strip().lower() for x in _env_str("ORDERBOOK_EXCHANGES", "binance,bybit").split(",") if x.strip()]
ORDERBOOK_FUTURES_ONLY = _env_bool("ORDERBOOK_FUTURES_ONLY", True)

# Strict defaults (can be overridden via ENV)
ORDERBOOK_LEVELS = max(5, _env_int("ORDERBOOK_LEVELS", 20))
ORDERBOOK_IMBALANCE_MIN = max(1.0, _env_float("ORDERBOOK_IMBALANCE_MIN", 1.6))  # bids/asks ratio
ORDERBOOK_WALL_RATIO = max(1.0, _env_float("ORDERBOOK_WALL_RATIO", 4.0))  # max_level / avg_level
ORDERBOOK_WALL_NEAR_PCT = max(0.05, _env_float("ORDERBOOK_WALL_NEAR_PCT", 0.5))  # consider walls within X% from mid
ORDERBOOK_MAX_SPREAD_PCT = max(0.0, _env_float("ORDERBOOK_MAX_SPREAD_PCT", 0.08))  # block if spread too wide
MACRO_ACTION = os.getenv("MACRO_ACTION", "FUTURES_OFF").strip().upper()  # FUTURES_OFF / PAUSE_ALL
BLACKOUT_BEFORE_MIN = max(0, _env_int("BLACKOUT_BEFORE_MIN", 30))
BLACKOUT_AFTER_MIN = max(0, _env_int("BLACKOUT_AFTER_MIN", 45))
_tz_raw = (os.getenv("TZ_NAME", "Europe/Berlin") or "").strip()
# Normalize common non-IANA aliases (ZoneInfo requires IANA names)
_TZ_ALIASES = {
    "MSK": "Europe/Moscow",
    "MOSCOW": "Europe/Moscow",
    "EUROPE/MOSCOW": "Europe/Moscow",
    "UTC+3": "Europe/Moscow",
    "UTC+03:00": "Europe/Moscow",
    "GMT+3": "Europe/Moscow",
    "GMT+03:00": "Europe/Moscow",
}

TZ_NAME = _TZ_ALIASES.get(_tz_raw.upper(), _tz_raw) or "Europe/Berlin"

# If TZ_NAME is still invalid, fall back to a safe default
try:
    ZoneInfo(TZ_NAME)
except Exception:
    TZ_NAME = "Europe/Moscow" if TZ_NAME.upper() in _TZ_ALIASES else "UTC"

FOMC_DECISION_HOUR_ET = max(0, min(23, _env_int("FOMC_DECISION_HOUR_ET", 14)))
FOMC_DECISION_MINUTE_ET = max(0, min(59, _env_int("FOMC_DECISION_MINUTE_ET", 0)))

# ------------------ Models ------------------
def _mid_block_reason(symbol: str, side: str, close: float, o: float, recent_low: float, recent_high: float,
                      atr_30m: float, rsi_5m: float, vwap: float, bb_pos: str | None,
                      ema20_5m: float | None, bos_down_5m: bool, two_red_5m: bool, lower_highs_5m: bool,
                      bos_up_5m: bool, two_green_5m: bool, higher_lows_5m: bool,
                      last_vol: float, avg_vol: float, last_body: float, climax_recent_bars: int,
                      htf_dir_1h: str | None = None, htf_dir_30m: str | None = None, adx_30m: float | None = None) -> str | None:
    """Return human-readable MID BLOCKED reason (for logging + error-bot), or None if allowed."""
    try:
        # ULTRA SAFE: tighten filters dynamically for near-zero SL preference
        late_entry_max = MID_ULTRA_LATE_ENTRY_ATR_MAX if MID_ULTRA_SAFE else MID_LATE_ENTRY_ATR_MAX
        vwap_dist_max = MID_ULTRA_VWAP_DIST_ATR_MAX if MID_ULTRA_SAFE else MID_VWAP_DIST_ATR_MAX
        rsi_long_max = MID_ULTRA_RSI_LONG_MAX if MID_ULTRA_SAFE else MID_RSI_LONG_MAX
        rsi_long_min = MID_ULTRA_RSI_LONG_MIN if MID_ULTRA_SAFE else MID_RSI_LONG_MIN
        rsi_short_min = MID_ULTRA_RSI_SHORT_MIN if MID_ULTRA_SAFE else MID_RSI_SHORT_MIN
        rsi_short_max = MID_ULTRA_RSI_SHORT_MAX if MID_ULTRA_SAFE else MID_RSI_SHORT_MAX
        confirm_body_min = MID_ULTRA_CONFIRM_CANDLE_BODY_ATR_MIN if MID_ULTRA_SAFE else MID_CONFIRM_CANDLE_BODY_ATR_MIN

        # --- Adaptive MID filter thresholds by ATR% regime (optional) ---
        try:
            if os.getenv("MID_ADAPTIVE_FILTERS", "0").strip().lower() in ("1","true","yes","on"):
                atrp = (float(atr_30m) / float(close) * 100.0) if (atr_30m and close) else 0.0
                low = float(os.getenv("MID_ADAPT_ATR_PCT_LOW", "2.0") or 2.0)
                high = float(os.getenv("MID_ADAPT_ATR_PCT_HIGH", "6.0") or 6.0)
                if high <= low:
                    t = 0.5
                else:
                    t = (atrp - low) / (high - low)
                if t < 0.0:
                    t = 0.0
                if t > 1.0:
                    t = 1.0

                try:
                    _mid_adapt_track(float(atrp or 0.0), float(t or 0.0), symbol)
                except Exception:
                    pass

                # When ATR% is LOW (t≈0) -> loosen filters a bit (more emitted in quiet markets).
                # When ATR% is HIGH (t≈1) -> tighten filters (avoid late/dirty entries in volatile markets).
                late_tight = float(os.getenv("MID_ADAPT_LATE_TIGHTEN_MIN", "0.90") or 0.90)
                late_wide  = float(os.getenv("MID_ADAPT_LATE_WIDEN_MAX", "1.15") or 1.15)
                vwap_tight = float(os.getenv("MID_ADAPT_VWAP_TIGHTEN_MIN", "0.90") or 0.90)
                vwap_wide  = float(os.getenv("MID_ADAPT_VWAP_WIDEN_MAX", "1.20") or 1.20)
                bounce_tight = float(os.getenv("MID_ADAPT_BOUNCE_TIGHTEN_MIN", "0.90") or 0.90)
                bounce_wide  = float(os.getenv("MID_ADAPT_BOUNCE_WIDEN_MAX", "1.30") or 1.30)

                near_strict = float(os.getenv("MID_ADAPT_NEAR_STRICT_MAX", "1.15") or 1.15)  # low ATR% => stricter (bigger)
                near_loose  = float(os.getenv("MID_ADAPT_NEAR_LOOSE_MIN", "0.70") or 0.70)    # high ATR% => looser (smaller)
                vol_strict  = float(os.getenv("MID_ADAPT_VOL_STRICT_MAX", "1.10") or 1.10)   # low ATR% => stricter (bigger)
                vol_loose   = float(os.getenv("MID_ADAPT_VOL_LOOSE_MIN", "0.95") or 0.95)    # high ATR% => looser (smaller)

                late_scale = late_wide + t * (late_tight - late_wide)
                vwap_scale = vwap_wide + t * (vwap_tight - vwap_wide)
                bounce_scale = bounce_wide + t * (bounce_tight - bounce_wide)

                # Apply to thresholds
                late_entry_max *= late_scale
                vwap_dist_max *= vwap_scale

                # Near-extreme threshold: bigger => more blocking (stricter) when ATR% high
                # Interpolate multiplier from near_loose (t=0) to near_strict (t=1)
                near_mult = near_loose + t * (near_strict - near_loose)

                # Min volume threshold: slightly stricter when ATR% high
                vol_mult = vol_loose + t * (vol_strict - vol_loose)

                # Save for later usage in this function (locals)
                _mid_adapt_near_mult = near_mult
                _mid_adapt_vol_mult = vol_mult
                _mid_adapt_bounce_scale = bounce_scale
            else:
                _mid_adapt_near_mult = 1.0
                _mid_adapt_vol_mult = 1.0
                _mid_adapt_bounce_scale = 1.0
        except Exception:
            _mid_adapt_near_mult = 1.0
            _mid_adapt_vol_mult = 1.0
            _mid_adapt_bounce_scale = 1.0

        if atr_30m and atr_30m > 0:
            if side.upper() == "LONG":
                move_from_low = (close - recent_low) / atr_30m
                if move_from_low > late_entry_max:
                    return f"late_entry_atr={move_from_low:.2f} > {late_entry_max:g}"
            else:  # SHORT
                move_from_high = (recent_high - close) / atr_30m
                if move_from_high > late_entry_max:
                    return f"late_entry_atr={move_from_high:.2f} > {late_entry_max:g}"


            # --- Near-extremes: avoid LONG right under resistance / SHORT right above support ---
            try:
                if MID_BLOCK_NEAR_EXTREMES and atr_30m and atr_30m > 0:
                    thr = (MID_ULTRA_NEAR_EXTREME_ATR_MIN if MID_ULTRA_SAFE else MID_NEAR_EXTREME_ATR_MIN) * (_mid_adapt_near_mult if '_mid_adapt_near_mult' in locals() else 1.0)
                    if side.upper() == "LONG":
                        dist_to_high = (recent_high - close) / atr_30m
                        if dist_to_high >= 0 and dist_to_high < thr:
                            return f"near_recent_high dist_atr={dist_to_high:.2f} < {thr:g}"
                    else:
                        dist_to_low = (close - recent_low) / atr_30m
                        if dist_to_low >= 0 and dist_to_low < thr:
                            return f"near_recent_low dist_atr={dist_to_low:.2f} < {thr:g}"
            except Exception:
                if MID_FAIL_CLOSED:
                    return "mid_near_extreme_error"
                pass
            # --- Anti-bounce: block SHORT after sharp rebound from recent low (and vice versa for LONG) ---
            if MID_ANTI_BOUNCE_ENABLED and atr_30m and atr_30m > 0:
                # Prefer side-specific MAX if set; fall back to MID_ANTI_BOUNCE_ATR_MAX
                bounce_atr_max = float(globals().get("MID_ANTI_BOUNCE_ATR_MAX", _env_float("MID_ANTI_BOUNCE_ATR_MAX", 1.8)))
                _ab_short = _env_float("MID_ANTI_BOUNCE_SHORT_MAX", 0.0)
                _ab_long  = _env_float("MID_ANTI_BOUNCE_LONG_MAX", 0.0)
                bounce_short_max = float(_ab_short) if _ab_short and _ab_short > 0 else bounce_atr_max
                bounce_long_max  = float(_ab_long)  if _ab_long  and _ab_long  > 0 else bounce_atr_max
                bounce_scale = _mid_adapt_bounce_scale if '_mid_adapt_bounce_scale' in locals() else 1.0
                bounce_short_max *= bounce_scale
                bounce_long_max *= bounce_scale

                if side.upper() == "SHORT":
                    dist_low_atr = (close - recent_low) / atr_30m
                    if dist_low_atr > bounce_short_max:
                        return f"anti_bounce_short dist_low_atr={dist_low_atr:.2f} > {bounce_short_max:g}"
                else:  # LONG
                    dist_high_atr = (recent_high - close) / atr_30m
                    if dist_high_atr > bounce_long_max:
                        return f"anti_bounce_long dist_high_atr={dist_high_atr:.2f} > {bounce_long_max:g}"



            # --- HTF alignment guard (ULTRA SAFE): block countertrend entries in strong trends ---
            try:
                if MID_HTF_ALIGN_GUARD and (adx_30m is not None) and (htf_dir_1h or htf_dir_30m):
                    strong = False
                    try:
                        strong = float(adx_30m) >= float(MID_HTF_STRONG_ADX_MIN)
                    except Exception:
                        strong = False

                    if strong:
                        h1 = (htf_dir_1h or "").upper()
                        h30 = (htf_dir_30m or "").upper()
                        s = side.upper()

                        # determine if this is countertrend vs HTF bias (both tf agree)
                        if h1 in ("LONG","SHORT") and h30 in ("LONG","SHORT") and h1 == h30 and s != h1:
                            # allow countertrend only if 5m shows clear reversal structure
                            if MID_ALLOW_COUNTERTREND_WITH_5M_REVERSAL:
                                if s == "SHORT":
                                    # need bearish reversal on 5m: BOS down + red candles + lower highs
                                    if not (bos_down_5m and two_red_5m and lower_highs_5m):
                                        return f"htf_align_block strong_adx={float(adx_30m):.1f} htf={h1} need_5m_reversal"
                                else:  # LONG
                                    if not (bos_up_5m and two_green_5m and higher_lows_5m):
                                        return f"htf_align_block strong_adx={float(adx_30m):.1f} htf={h1} need_5m_reversal"
                            else:
                                return f"htf_align_block strong_adx={float(adx_30m):.1f} htf={h1}"
            except Exception:
                pass

            # --- BB bounce guard (mirror): avoid entries away from BB extremes ---
            # For quality entries (less SL), we prefer:
            #   LONG  only near/below lower band (bb_pos == '↓low')
            #   SHORT only near/above upper band (bb_pos == '↑high')
            # Everything inside the bands is treated as "bounce zone" and blocked.
            if MID_BLOCK_BB_BOUNCE and bb_pos:
                b = str(bb_pos)
                if side.upper() == "SHORT":
                    if b in ("mid→high", "low→mid", "↓low"):
                        return f"bb_bounce_zone={bb_pos}"
                else:  # LONG
                    if b in ("mid→high", "low→mid", "↑high"):
                        return f"bb_bounce_zone={bb_pos}"

            dist = abs(close - vwap) / atr_30m if vwap is not None else 0.0
            if dist > vwap_dist_max:
                return f"vwap_dist_atr={dist:.2f} > {vwap_dist_max:g}"

            body_atr = (abs(close - o) / atr_30m) if o is not None else 0.0
            if avg_vol and avg_vol > 0:
                vol_x = last_vol / avg_vol
            else:
                vol_x = 0.0

            try:
                min_vol = float(MID_MIN_VOL_X) * (_mid_adapt_vol_mult if '_mid_adapt_vol_mult' in locals() else 1.0)
                if min_vol and vol_x < min_vol:
                    return f"vol_x={vol_x:.2f} < {min_vol:g}"
            except Exception:
                pass
            # "climax": big vol + big body; block next N bars
            if climax_recent_bars > 0 or (vol_x > MID_CLIMAX_VOL_X and body_atr > MID_CLIMAX_BODY_ATR):
                return f"climax vol_x={vol_x:.2f} > {MID_CLIMAX_VOL_X:g} and body_atr={body_atr:.2f} > {MID_CLIMAX_BODY_ATR:g}"

        # RSI filter (works even if ATR missing)
        if rsi_5m is not None:
            if side.upper() == "LONG":
                if rsi_5m >= rsi_long_max:
                    return f"rsi_long={rsi_5m:.1f} >= {rsi_long_max:g}"
                if rsi_5m < rsi_long_min:
                    return f"rsi_long={rsi_5m:.1f} < {rsi_long_min:g}"
            else:  # SHORT
                if rsi_5m <= rsi_short_min:
                    return f"rsi_short={rsi_5m:.1f} <= {rsi_short_min:g}"
                if rsi_5m >= rsi_short_max:
                    return f"rsi_short={rsi_5m:.1f} >= {rsi_short_max:g}"

        # --- Extra quality guards (to reduce immediate SL after entry) ---
        try:
            if MID_REQUIRE_VWAP_SIDE and vwap is not None:
                if side.upper() == "LONG" and close < float(vwap):
                    return f"vwap_side_long close<{float(vwap):.6g}"
                if side.upper() == "SHORT" and close > float(vwap):
                    return f"vwap_side_short close>{float(vwap):.6g}"
        except Exception:
            pass

        try:
            if MID_CONFIRM_CANDLE_ENABLED:
                # Direction-aligned candle
                if MID_CONFIRM_CANDLE_REQUIRE_DIRECTION and o is not None:
                    if side.upper() == "LONG" and close <= float(o):
                        return "confirm_candle_long close<=open"
                    if side.upper() == "SHORT" and close >= float(o):
                        return "confirm_candle_short close>=open"
                # Minimal body vs ATR to avoid chop
                if atr_30m and atr_30m > 0 and o is not None:
                    body_atr2 = abs(float(close) - float(o)) / float(atr_30m)
                    if body_atr2 < confirm_body_min:
                        return f"small_body_atr={body_atr2:.2f} < {confirm_body_min:g}"
        except Exception:
            pass


        # --- Anti-countertrend guards (5m structure) ---
        try:
            if side.upper() == "LONG":
                if MID_BLOCK_LONG_BOS_DOWN and bos_down_5m:
                    return "bos_down_5m"
                if MID_BLOCK_LONG_BELOW_EMA20_5M and (ema20_5m is not None) and close < float(ema20_5m):
                    return f"below_ema20_5m close<{float(ema20_5m):.6g}"
                if MID_BLOCK_LONG_2_RED_CANDLES and two_red_5m:
                    return "two_red_candles_5m"
                if MID_BLOCK_LONG_LOWER_HIGHS and lower_highs_5m:
                    return "lower_highs_5m"
            elif side.upper() == "SHORT":
                if MID_BLOCK_SHORT_BOS_UP and bos_up_5m:
                    return "bos_up_5m"
                if MID_BLOCK_SHORT_ABOVE_EMA20_5M and (ema20_5m is not None) and close > float(ema20_5m):
                    return f"above_ema20_5m close>{float(ema20_5m):.6g}"
                if MID_BLOCK_SHORT_2_GREEN_CANDLES and two_green_5m:
                    return "two_green_candles_5m"
                if MID_BLOCK_SHORT_HIGHER_LOWS and higher_lows_5m:
                    return "higher_lows_5m"
        except Exception:
            pass
        # --- Require 5m confirmation (prevents SHORT into bullish microtrend / LONG into bearish microtrend) ---
        try:

            if MID_REQUIRE_5M_CONFIRM:
                s = side.upper()
                if MID_ULTRA_SAFE:
                    # ULTRA SAFE: require real continuation/reversal structure, not just EMA touch
                    if s == "SHORT":
                        score = 0
                        if bos_down_5m: score += 1
                        if lower_highs_5m: score += 1
                        if two_red_5m: score += 1
                        if score < 2:
                            return "no_bear_5m_structure"
                        if (ema20_5m is not None) and not (close < float(ema20_5m)):
                            return "short_above_ema20_5m"
                    elif s == "LONG":
                        score = 0
                        if bos_up_5m: score += 1
                        if higher_lows_5m: score += 1
                        if two_green_5m: score += 1
                        if score < 2:
                            return "no_bull_5m_structure"
                        if (ema20_5m is not None) and not (close > float(ema20_5m)):
                            return "long_below_ema20_5m"
                else:
                    if s == "SHORT":
                        bear = bool(bos_down_5m) or bool(two_red_5m) or bool(lower_highs_5m) or ((ema20_5m is not None) and (close < float(ema20_5m)))
                        if not bear:
                            return "no_bear_5m_confirm"
                    elif s == "LONG":
                        bull = bool(bos_up_5m) or bool(two_green_5m) or bool(higher_lows_5m) or ((ema20_5m is not None) and (close > float(ema20_5m)))
                        if not bull:
                            return "no_bull_5m_confirm"
        except Exception:
            # if confirmation calc fails, respect fail-closed flag
            if MID_FAIL_CLOSED:
                return "mid_5m_confirm_error"
            pass


    except Exception:
        # if filter computation fails, respect fail-closed flag
        return "mid_filter_error" if MID_FAIL_CLOSED else None
    return None
@dataclass(frozen=True)
class Signal:
    # NOTE: All fields have defaults to avoid dataclass ordering issues
    # (non-default argument follows default argument) if earlier fields
    # are given defaults in some deployments.
    signal_id: int = 0
    market: str = ""
    symbol: str = ""
    direction: str = ""
    timeframe: str = ""
    entry: float = 0.0
    sl: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    rr: float = 0.0
    confidence: int = 0
    confirmations: str = ""
    # Exchange metadata (used for display/routing)
    source_exchange: str = ""
    available_exchanges: str = ""
    risk_note: str = ""
    ts: float = 0.0

@dataclass

class UserTrade:
    user_id: int
    signal: Signal
    tp1_hit: bool = False
    sl_moved_to_be: bool = False
    be_price: float = 0.0
    active: bool = True
    result: str = "ACTIVE"  # ACTIVE / TP1 / WIN / LOSS / BE / CLOSED
    last_price: float = 0.0
    opened_ts: float = 0.0


# ------------------ Trade performance stats (spot/futures) ------------------
TRADE_STATS_FILE = Path("trade_stats.json")

# structure:
# {
#   "spot": {"days": {"YYYY-MM-DD": {...}}, "weeks": {"YYYY-WNN": {...}}},
#   "futures": {...}
# }
def _empty_bucket() -> Dict[str, float]:
    return {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "be": 0,
        "tp1_hits": 0,
        "sum_pnl_pct": 0.0,
    }

def _safe_pct(x: float) -> float:
    if not math.isfinite(x):
        return 0.0
    return float(x)

def _market_key(market: str) -> str:
    m = (market or "FUTURES").strip().upper()
    return "spot" if m == "SPOT" else "futures"


def _tp1_partial_close_pct(market: str) -> float:
    mk = _market_key(market)
    return float(TP1_PARTIAL_CLOSE_PCT_SPOT if mk == "spot" else TP1_PARTIAL_CLOSE_PCT_FUTURES)


# Backward-compat alias (older code referenced _partial_close_pct)
def _partial_close_pct(market: str) -> float:
    return _tp1_partial_close_pct(market)


# Backward-compat alias (older code referenced _be_enabled)
def _be_enabled(market: str) -> bool:
    return _be_after_tp1(market)


def _be_after_tp1(market: str) -> bool:
    mk = _market_key(market)
    return bool(BE_AFTER_TP1_SPOT if mk == "spot" else BE_AFTER_TP1_FUTURES)

def _fee_rate(market: str) -> float:
    mk = _market_key(market)
    return float(FEE_RATE_SPOT if mk == "spot" else FEE_RATE_FUTURES)

def _be_exit_price(entry: float, side: str, market: str) -> float:
    """Return modeled BE exit price that covers fees on remaining position."""
    entry = float(entry)
    if not BE_FEE_BUFFER:
        return entry
    fr = max(0.0, _fee_rate(market))
    buf = max(0.0, float(FEE_BUFFER_MULTIPLIER)) * fr
    side = (side or "LONG").upper()
    # IMPORTANT:
    # BE after TP1 is used as a *protective stop* on the remaining position.
    # Therefore the BE level must be on the "risk" side of the current price:
    #   - LONG: stop triggers when price falls back -> BE below entry
    #   - SHORT: stop triggers when price rises back -> BE above entry
    # Otherwise (e.g., SHORT with BE below entry) the condition (price >= BE)
    # would be true immediately and the trade would auto-close right after TP1.
    if side == "SHORT":
        return entry * (1.0 + buf)
    return entry * (1.0 - buf)


def _day_key_tz(ts: float) -> str:
    d = dt.datetime.fromtimestamp(ts, tz=ZoneInfo(TZ_NAME)).date()
    return d.isoformat()

def _week_key_tz(ts: float) -> str:
    d = dt.datetime.fromtimestamp(ts, tz=ZoneInfo(TZ_NAME)).date()
    y, w, _ = d.isocalendar()
    return f"{y}-W{int(w):02d}"

def _calc_effective_pnl_pct(trade: "UserTrade", close_price: float, close_reason: str) -> float:
    s = trade.signal
    entry = float(s.entry or 0.0)
    if entry <= 0:
        return 0.0

    side = (s.direction or "LONG").upper()
    def pnl_pct_for(price: float) -> float:
        price = float(price)
        if side == "SHORT":
            return (entry - price) / entry * 100.0
        return (price - entry) / entry * 100.0

    # No TP1 hit => full position closes at close_price
    if not trade.tp1_hit:
        return _safe_pct(pnl_pct_for(close_price))

    # TP1 hit => partial close at TP1, rest closes at BE or TP2 (or manual)
    p = max(0.0, min(100.0, float(_tp1_partial_close_pct(s.market))))
    a = p / 100.0
    tp1_price = float(s.tp1 or close_price)

    pnl_tp1 = pnl_pct_for(tp1_price) * a

    # Remaining portion:
    if close_reason == "WIN":
        # rest closes at TP2 (if provided), else close_price
        tp2_price = float(s.tp2 or close_price)
        pnl_rest = pnl_pct_for(tp2_price) * (1.0 - a)
    elif close_reason == "BE":
        pnl_rest = 0.0
    else:
        # manual close / unknown: use close_price for remaining
        pnl_rest = pnl_pct_for(close_price) * (1.0 - a)

    return _safe_pct(pnl_tp1 + pnl_rest)

def _calc_rr_str(entry: float, sl: float, tp1: float, tp2: float) -> str:
    """Return Risk/Reward as 'X.XX' where reward uses TP2 if present else TP1."""
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
        target = tp2 if tp2 > 0 else tp1
        if target <= 0:
            return "-"
        reward = abs(target - entry)
        rr = reward / risk if risk > 0 else 0.0
        if rr <= 0:
            return "-"
        return f"{rr:.2f}"
    except Exception:
        return "-"


# ------------------ Human-friendly close reasons (i18n) ------------------

def _human_close_reason(uid: int, code: str | None, **kv: Any) -> str:
    """Return a short, user-friendly reason string using i18n.

    `code` is a stable internal identifier (e.g. 'TP2_REACHED').
    We map codes to i18n keys, and keep optional numeric details in `kv`.
    """
    if not code:
        return ""
    c = str(code).upper().strip()

    # Map internal codes to i18n keys (keep keys stable)
    key_map = {
        "TP2_REACHED": "reason_tp2_reached",
        "SL_REACHED": "reason_sl_reached",
        "SL_AFTER_TP1": "reason_sl_after_tp1",
        "TP1_PARTIAL": "reason_tp1_partial",
        "TP1_HOLD": "reason_tp1_hold_to_tp2",
        "SMART_BE": "reason_smart_be",
        "SMART_HARD_SL": "reason_smart_hard_sl",
        "TRAIL_EXIT": "reason_trail_exit",
        "REVERSAL_EXIT": "reason_reversal_exit",
        "EARLY_EXIT_MOM": "reason_early_exit_momentum",
        "STRUCTURE_BREAK": "reason_structure_break",
        "TRAP_DETECTED": "reason_trap_detected",
        "VOLUME_DROPPED": "reason_volume_dropped",
        "BOS_AGAINST": "reason_bos_against",
    }

    k = key_map.get(c)
    if not k:
        # fallback: show code as-is (still better than nothing)
        return str(code)
    try:
        return _trf(uid, k, **kv)
    except Exception:
        try:
            return _trf(uid, k)
        except Exception:
            return str(code)

def _lvl_line(label: str, value: float) -> str:
    try:
        v = float(value or 0.0)
        if v > 0:
            return f"{label}: {v:.6f}\n"
    except Exception:
        pass
    return ""



def _bump_stats(store: dict, market: str, ts: float, close_reason: str, pnl_pct: float, tp1_hit: bool) -> None:
    mk = _market_key(market)
    dayk = _day_key_tz(ts)
    weekk = _week_key_tz(ts)

    mroot = store.setdefault(mk, {"days": {}, "weeks": {}})
    days = mroot.setdefault("days", {})
    weeks = mroot.setdefault("weeks", {})

    def get_bucket(root: dict, k: str) -> dict:
        b = root.get(k)
        if not isinstance(b, dict):
            b = _empty_bucket()
            root[k] = b
        # ensure keys
        for kk, vv in _empty_bucket().items():
            if kk not in b:
                b[kk] = vv
        return b

    bd = get_bucket(days, dayk)
    bw = get_bucket(weeks, weekk)

    def apply(bucket: dict) -> None:
        bucket["trades"] = int(bucket.get("trades", 0)) + 1
        # wins/losses based on pnl sign (если WIN но ушёл в минус — засчитываем как LOSS)
        if pnl_pct > 0:
            bucket["wins"] = int(bucket.get("wins", 0)) + 1
        elif pnl_pct < 0:
            bucket["losses"] = int(bucket.get("losses", 0)) + 1
        else:
            bucket["be"] = int(bucket.get("be", 0)) + 1

        if tp1_hit:
            bucket["tp1_hits"] = int(bucket.get("tp1_hits", 0)) + 1

        bucket["sum_pnl_pct"] = float(bucket.get("sum_pnl_pct", 0.0)) + float(pnl_pct)

    apply(bd)
    apply(bw)

@dataclass(frozen=True)

class MacroEvent:
    name: str
    type: str
    start_ts_utc: float

# ------------------ News risk filter (CryptoPanic) ------------------
class NewsFilter:
    # Use Developer v2 endpoint (matches API Reference in CryptoPanic Developers panel)
    BASE_URL = "https://cryptopanic.com/api/developer/v2/posts/"

    def __init__(self) -> None:
        self._last_block: tuple[str, float] | None = None
        self._cache: Dict[str, Tuple[float, str]] = {}
        self._cache_ttl = 60
        # prevent hanging network calls
        self._timeout = aiohttp.ClientTimeout(total=8)

    def enabled(self) -> bool:
        return NEWS_FILTER and bool(CRYPTOPANIC_TOKEN)

    def base_coin(self, symbol: str) -> str:
        # Accept formats: BTCUSDT, BTC/USDT, BTC-USDT, etc.
        s = str(symbol).upper().replace("/", "").replace("-", "").strip()
        for q in ("USDT", "USDC", "BUSD", "USD", "FDUSD", "TUSD", "DAI", "EUR", "BTC", "ETH"):
            if s.endswith(q) and len(s) > len(q):
                return s[:-len(q)]
        return s

    async def action_for_symbol(self, session: aiohttp.ClientSession, symbol: str) -> str:
        if not self.enabled():
            return "ALLOW"

        coin = self.base_coin(symbol)
        key = f"{coin}:{NEWS_ACTION}:{NEWS_LOOKBACK_MIN}"
        now = time.time()
        cached = self._cache.get(key)
        if cached and (now - cached[0]) < self._cache_ttl:
            return cached[1]

        action = "ALLOW"
        try:
            params = {
                "auth_token": CRYPTOPANIC_TOKEN,
                "currencies": coin,
                "filter": "important",
                # reduce noise; can be overridden via env if desired
                "kind": CRYPTOPANIC_KIND,
                "regions": CRYPTOPANIC_REGIONS,
            }
            if CRYPTOPANIC_PUBLIC:
                params["public"] = "true"

            async with session.get(self.BASE_URL, params=params, timeout=self._timeout) as r:
                if r.status != 200:
                    action = "ALLOW"
                else:
                    data = await r.json()
                    posts = (data or {}).get("results", []) or []
                    cutoff = now - (NEWS_LOOKBACK_MIN * 60)
                    recent = False
                    recent_post = None
                    for p in posts[:20]:
                        published_at = p.get("published_at") or p.get("created_at")
                        if not published_at:
                            continue
                        try:
                            ts = pd.to_datetime(published_at.replace("Z", "+00:00")).timestamp()
                        except Exception:
                            continue
                        if ts >= cutoff:
                            recent = True
                            recent_post = p
                            break
                    if recent:
                        action = NEWS_ACTION if NEWS_ACTION in ("FUTURES_OFF", "PAUSE_ALL") else "FUTURES_OFF"
                        # remember last block for status (reason, until_ts)
                        reason = coin
                        try:
                            if recent_post:
                                title = (recent_post.get("title") or recent_post.get("slug") or "").strip()
                                src = None
                                if isinstance(recent_post.get("source"), dict):
                                    src = (recent_post["source"].get("title") or recent_post["source"].get("name"))
                                if not src:
                                    src = (recent_post.get("domain") or recent_post.get("source") or "")
                                src = (src or "").strip()
                                # CryptoPanic 'important' filter is used; keep it in text for clarity
                                parts = []
                                if src:
                                    parts.append(src)
                                parts.append("Important")
                                if title:
                                    parts.append(title)
                                reason = "CryptoPanic — " + " / ".join(parts)
                        except Exception:
                            pass
                        self._last_block = (reason, now + NEWS_LOOKBACK_MIN * 60)
        except Exception:
            action = "ALLOW"

        self._cache[key] = (now, action)
        return action

# ------------------ Macro calendar (AUTO fetch) ------------------

    def last_block_info(self) -> tuple[str, float] | None:
        """Return (reason, until_ts) for the last time news filter triggered."""
        return self._last_block

class MacroCalendar:
    BLS_URL = "https://www.bls.gov/schedule/news_release/current_year.asp"
    FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

    def __init__(self) -> None:
        self._events: List[MacroEvent] = []
        self._last_fetch_ts: float = 0.0
        self._fetch_ttl: float = 6 * 60 * 60
        self._notified: Dict[str, float] = {}
        self._notify_cooldown = 6 * 60 * 60

    def enabled(self) -> bool:
        return MACRO_FILTER

    async def ensure_loaded(self, session: aiohttp.ClientSession) -> None:
        if not self.enabled():
            self._events = []
            return
        now = time.time()
        if self._events and (now - self._last_fetch_ts) < self._fetch_ttl:
            return
        await self.fetch(session)

    async def fetch(self, session: aiohttp.ClientSession) -> None:
        events: List[MacroEvent] = []

        # BLS schedule
        try:
            async with session.get(self.BLS_URL) as r:
                r.raise_for_status()
                html = await r.text()

            tables = pd.read_html(html)
            for t in tables:
                cols = [c.strip().lower() for c in t.columns.astype(str).tolist()]
                if not ("release" in cols and "date" in cols):
                    continue
                t2 = t.copy()
                t2.columns = cols
                for _, row in t2.iterrows():
                    release = str(row.get("release", "")).strip()
                    date_str = str(row.get("date", "")).strip()
                    time_str = str(row.get("time", "")).strip()
                    if not release or not date_str or not time_str:
                        continue

                    rel_l = release.lower()
                    if "consumer price index" in rel_l:
                        name = "CPI (US Inflation)"
                        etype = "CPI"
                    elif "employment situation" in rel_l:
                        name = "NFP (US Jobs)"
                        etype = "NFP"
                    else:
                        continue

                    try:
                        dt_et = pd.to_datetime(f"{date_str} {time_str}")
                    except Exception:
                        continue
                    dt_et = dt_et.tz_localize(ZoneInfo("America/New_York"), ambiguous="NaT", nonexistent="shift_forward")
                    dt_utc = dt_et.tz_convert(ZoneInfo("UTC")).timestamp()
                    events.append(MacroEvent(name=name, type=etype, start_ts_utc=float(dt_utc)))
                break
        except Exception:
            pass

        # FOMC calendar (best-effort parse)
        try:
            async with session.get(self.FOMC_URL) as r:
                r.raise_for_status()
                txt = await r.text()

            year = pd.Timestamp.utcnow().year
            months = ["January","February","March","April","May","June","July","August","September","October","November","December"]
            for m in months:
                pattern = rf"{m}\\s*\\n\\s*(\\d{{1,2}})\\s*[-–]\\s*(\\d{{1,2}})"
                m1 = re.search(pattern, txt, flags=re.IGNORECASE)
                if not m1:
                    continue
                d2 = int(m1.group(2))
                try:
                    dt_et = pd.Timestamp(year=year, month=months.index(m)+1, day=d2, hour=FOMC_DECISION_HOUR_ET, minute=FOMC_DECISION_MINUTE_ET, tz="America/New_York")
                except Exception:
                    continue
                events.append(MacroEvent(name="FOMC Rate Decision", type="FED", start_ts_utc=float(dt_et.tz_convert("UTC").timestamp())))
        except Exception:
            pass

        now = time.time()
        filtered = [e for e in events if (now - 2*24*3600) <= e.start_ts_utc <= (now + 370*24*3600)]
        filtered.sort(key=lambda e: e.start_ts_utc)
        self._events = filtered
        self._last_fetch_ts = time.time()

    def blackout_window(self, ev: MacroEvent) -> Tuple[float, float]:
        start = ev.start_ts_utc - (BLACKOUT_BEFORE_MIN * 60)
        end = ev.start_ts_utc + (BLACKOUT_AFTER_MIN * 60)
        return start, end

    def current_action(self) -> Tuple[str, Optional[MacroEvent], Optional[Tuple[float, float]]]:
        if not self.enabled():
            return "ALLOW", None, None
        now = time.time()
        for ev in self._events:
            w0, w1 = self.blackout_window(ev)
            if w0 <= now <= w1:
                act = MACRO_ACTION if MACRO_ACTION in ("FUTURES_OFF", "PAUSE_ALL") else "FUTURES_OFF"
                return act, ev, (w0, w1)
        return "ALLOW", None, None

    def status(self) -> tuple[str, MacroEvent | None, tuple[float, float] | None]:
        """Return (action, event, (w0,w1)) for current moment."""
        return self.current_action()

    def next_event(self) -> Optional[Tuple[MacroEvent, Tuple[float, float]]]:
        if not self.enabled():
            return None
        now = time.time()
        for ev in self._events:
            w0, w1 = self.blackout_window(ev)
            if now < w0:
                return ev, (w0, w1)
        return None

    def should_notify(self, ev: MacroEvent) -> bool:
        key = f"{ev.type}:{ev.start_ts_utc}"
        now = time.time()
        last = self._notified.get(key, 0.0)
        if (now - last) >= self._notify_cooldown:
            self._notified[key] = now
            return True
        return False

# ------------------ Price feed (WS, Binance) ------------------
class PriceFeed:
    """Lightweight WS price feed with per-exchange streams.

    Stores latest trade price and timestamp for (exchange, market, symbol).
    Backwards compatible: if exchange is omitted, defaults to BINANCE.
    """

    def __init__(self) -> None:
        self._prices: Dict[Tuple[str, str, str], float] = {}
        self._sources: Dict[Tuple[str, str, str], str] = {}
        self._ts: Dict[Tuple[str, str, str], float] = {}
        self._tasks: Dict[Tuple[str, str, str], asyncio.Task] = {}

    def _key(self, ex: str, market: str, symbol: str) -> Tuple[str, str, str]:
        return (str(ex or "binance").lower().strip(), market.upper(), symbol.upper())

    def get_latest(self, market: str, symbol: str, *, max_age_sec: float | None = None, ex: str = "binance") -> Optional[float]:
        key = self._key(ex, market, symbol)
        p = self._prices.get(key)
        if p is None:
            return None
        if max_age_sec is not None:
            ts = self._ts.get(key)
            if ts is None or (time.time() - ts) > float(max_age_sec):
                return None
        return float(p)

    def get_latest_source(self, market: str, symbol: str, *, ex: str = "binance") -> Optional[str]:
        return self._sources.get(self._key(ex, market, symbol))

    def _set_latest(self, market: str, symbol: str, price: float, *, source: str = "WS", ex: str = "binance") -> None:
        key = self._key(ex, market, symbol)
        self._prices[key] = float(price)
        self._sources[key] = str(source or "WS")
        self._ts[key] = time.time()

    async def ensure_stream(self, market: str, symbol: str, ex: str = "binance") -> None:
        key = self._key(ex, market, symbol)
        t = self._tasks.get(key)
        if t and not t.done():
            return
        self._tasks[key] = asyncio.create_task(self._run_stream(ex, market, symbol))

    # ---- WS helpers ----
    def _okx_inst(self, market: str, symbol: str) -> str:
        sym = (symbol or "").upper()
        # BTCUSDT -> BTC-USDT
        if sym.endswith("USDT"):
            base, quote = sym[:-4], "USDT"
        elif sym.endswith("USDC"):
            base, quote = sym[:-4], "USDC"
        else:
            # naive split: last 3/4 as quote. fallback: raw
            base, quote = sym[:-3], sym[-3:]
            if len(sym) >= 4 and sym[-4:] in ("USDT", "USDC"):
                base, quote = sym[:-4], sym[-4:]
        inst = f"{base}-{quote}"
        m = (market or "FUTURES").upper()
        if m != "SPOT":
            # OKX perpetual swap for USDT/USDC pairs is commonly *-SWAP
            inst = inst + "-SWAP"
        return inst

    async def _run_stream(self, ex: str, market: str, symbol: str) -> None:
        ex0 = str(ex or "binance").lower().strip()
        m = (market or "FUTURES").upper()
        sym_u = (symbol or "").upper()
        sym_l = sym_u.lower()

        backoff = 1
        while True:
            try:
                if ex0 == "binance":
                    url = f"wss://stream.binance.com:9443/ws/{sym_l}@trade" if m == "SPOT" else f"wss://fstream.binance.com/ws/{sym_l}@trade"
                    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                        backoff = 1
                        async for msg in ws:
                            data = json.loads(msg)
                            p = data.get("p")
                            if p is not None:
                                self._set_latest(m, sym_u, float(p), source="WS", ex="binance")

                elif ex0 == "bybit":
                    # Bybit v5 public streams. linear for futures, spot for spot.
                    url = "wss://stream.bybit.com/v5/public/spot" if m == "SPOT" else "wss://stream.bybit.com/v5/public/linear"
                    sub = {"op": "subscribe", "args": [f"publicTrade.{sym_u}"]}
                    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                        await ws.send(json.dumps(sub))
                        backoff = 1
                        async for msg in ws:
                            data = json.loads(msg)
                            # v5: {"topic":"publicTrade.BTCUSDT","data":[{"p":"..."}]}
                            arr = data.get("data")
                            if isinstance(arr, list) and arr:
                                p = arr[0].get("p") or arr[0].get("price")
                                if p is not None:
                                    self._set_latest(m, sym_u, float(p), source="WS", ex="bybit")

                elif ex0 == "okx":
                    url = "wss://ws.okx.com:8443/ws/v5/public"
                    inst = self._okx_inst(m, sym_u)
                    sub = {"op": "subscribe", "args": [{"channel": "trades", "instId": inst}]}
                    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                        await ws.send(json.dumps(sub))
                        backoff = 1
                        async for msg in ws:
                            data = json.loads(msg)
                            # {"arg":{"channel":"trades"...},"data":[{"px":"..."}]}
                            arr = data.get("data")
                            if isinstance(arr, list) and arr:
                                p = arr[0].get("px") or arr[0].get("price")
                                if p is not None:
                                    self._set_latest(m, sym_u, float(p), source="WS", ex="okx")

                elif ex0 == "gateio":
                    # Gate.io WS v4. Spot-only used in this project.
                    if m != "SPOT":
                        raise Exception("gateio ws only spot")
                    url = "wss://api.gateio.ws/ws/v4/"
                    sub = {"time": int(time.time()), "channel": "spot.trades", "event": "subscribe", "payload": [sym_u]}
                    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                        await ws.send(json.dumps(sub))
                        backoff = 1
                        async for msg in ws:
                            data = json.loads(msg)
                            res = data.get("result")
                            if isinstance(res, dict):
                                p = res.get("price")
                                if p is not None:
                                    self._set_latest(m, sym_u, float(p), source="WS", ex="gateio")
                            elif isinstance(res, list) and res:
                                # sometimes result is list of trades
                                p = res[0].get("price") if isinstance(res[0], dict) else None
                                if p is not None:
                                    self._set_latest(m, sym_u, float(p), source="WS", ex="gateio")

                elif ex0 == "mexc":
                    # MEXC public WS (spot). Futures not used here.
                    if m != "SPOT":
                        raise Exception("mexc ws only spot")
                    url = "wss://wbs.mexc.com/ws"
                    # channel: "spot@public.deals.v3.api@{symbol}"
                    sub = {"method": "SUBSCRIPTION", "params": [f"spot@public.deals.v3.api@{sym_u}"]}
                    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                        await ws.send(json.dumps(sub))
                        backoff = 1
                        async for msg in ws:
                            data = json.loads(msg)
                            d = data.get("data") or {}
                            deals = d.get("deals") if isinstance(d, dict) else None
                            if isinstance(deals, list) and deals:
                                p = deals[0].get("p") or deals[0].get("price")
                                if p is not None:
                                    self._set_latest(m, sym_u, float(p), source="WS", ex="mexc")
                else:
                    raise Exception(f"unknown exchange ws: {ex0}")

            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def mock_price(self, market: str, symbol: str, base: float | None = None) -> float:
        key = ("mock", market.upper(), symbol.upper())
        seed = (float(base) if base is not None and base > 0 else None)
        price = self._prices.get(key, seed if seed is not None else random.uniform(100, 50000))
        price *= random.uniform(0.996, 1.004)
        self._prices[key] = price
        self._sources[key] = "MOCK"
        return price

# ------------------ Price caching + REST limiting ------------------
class _RestLimiter:
    """Very lightweight per-exchange limiter to avoid REST storms/429.

    It enforces:
      - max concurrent in-flight REST requests per exchange
      - a minimum delay between request starts per exchange
    """

    def __init__(self) -> None:
        self._sem: dict[str, asyncio.Semaphore] = {}
        self._lock: dict[str, asyncio.Lock] = {}
        self._next_ts: dict[str, float] = {}

    def _cfg(self, ex: str) -> tuple[int, float]:
        exu = (ex or "").upper()
        mc = int(os.getenv(f"REST_MAX_CONCURRENT_{exu}", os.getenv("REST_MAX_CONCURRENT", "3")) or 3)
        if mc < 1:
            mc = 1
        if mc > 50:
            mc = 50
        mi_ms = float(os.getenv(f"REST_MIN_INTERVAL_MS_{exu}", os.getenv("REST_MIN_INTERVAL_MS", "150")) or 150.0)
        if mi_ms < 0:
            mi_ms = 0.0
        if mi_ms > 5000:
            mi_ms = 5000.0
        return mc, mi_ms / 1000.0

    async def run(self, ex: str, fn):
        exn = (ex or "").lower().strip() or "generic"
        mc, min_interval = self._cfg(exn)
        sem = self._sem.get(exn)
        if sem is None or sem._value + len(getattr(sem, "_waiters", []) or []) != mc:
            sem = asyncio.Semaphore(mc)
            self._sem[exn] = sem
        lk = self._lock.setdefault(exn, asyncio.Lock())
        async with sem:
            if min_interval > 0:
                async with lk:
                    now = time.time()
                    nxt = float(self._next_ts.get(exn, 0.0) or 0.0)
                    if nxt > now:
                        await asyncio.sleep(nxt - now)
                    self._next_ts[exn] = time.time() + min_interval
            return await fn()


class _AsyncPriceCache:
    """Cache for (exchange, market, symbol) -> (price, source, ts) with anti-storm in-flight dedupe."""

    def __init__(self, ttl_sec: float = 2.0) -> None:
        self.ttl_sec = float(ttl_sec)
        self._data: dict[tuple[str, str, str], tuple[float, str, float]] = {}
        # Negative cache for transient "no price" situations (timeouts/429/symbol not found).
        # Keeps the bot from hammering the same key and prevents noisy asyncio warnings.
        self._neg: dict[tuple[str, str, str], tuple[str, float]] = {}
        self._neg_ttl_sec = max(0.5, min(5.0, self.ttl_sec))
        self._inflight: dict[tuple[str, str, str], asyncio.Future] = {}
        self._mu = asyncio.Lock()

    def get(self, ex: str, market: str, symbol: str) -> tuple[float | None, str] | None:
        key = (ex.lower(), market.upper(), symbol.upper())
        # Negative cache hit
        nv = self._neg.get(key)
        if nv:
            nsrc, nts = nv
            if (time.time() - float(nts)) <= self._neg_ttl_sec:
                # caller expects (price, src); price=None indicates "not available"
                return None, str(nsrc)
            else:
                self._neg.pop(key, None)
        v = self._data.get(key)
        if not v:
            return None
        p, src, ts = v
        if (time.time() - float(ts)) <= self.ttl_sec:
            return float(p), str(src)
        return None

    async def get_or_fetch(self, ex: str, market: str, symbol: str, fetch_coro):
        key = (ex.lower(), market.upper(), symbol.upper())
        # Fast path: cache hit
        v = self.get(ex, market, symbol)
        if v is not None:
            return v

        async with self._mu:
            # Re-check under lock
            v = self.get(ex, market, symbol)
            if v is not None:
                return v
            fut = self._inflight.get(key)
            if fut is None or fut.done():
                fut = asyncio.get_running_loop().create_future()
                # If some caller launches work and later drops it (create_task / cancellation),
                # asyncio may warn "Future exception was never retrieved". Ensure we always
                # mark exceptions as retrieved.
                def _consume_future_exception(_f: asyncio.Future):
                    try:
                        _ = _f.exception()
                    except Exception:
                        pass

                fut.add_done_callback(_consume_future_exception)
                self._inflight[key] = fut
                owner = True
            else:
                owner = False

        if not owner:
            return await fut

        try:
            p, src = await fetch_coro()
            async with self._mu:
                now = time.time()
                if p is None:
                    # Negative cache instead of raising: this is a normal transient state.
                    self._neg[key] = (str(src), now)
                    if not fut.done():
                        fut.set_result((None, str(src)))
                    self._inflight.pop(key, None)
                    return None, str(src)

                self._data[key] = (float(p), str(src), now)
                # Clear negative cache on success
                self._neg.pop(key, None)
                if not fut.done():
                    fut.set_result((float(p), str(src)))
                self._inflight.pop(key, None)
            return float(p), str(src)
        except Exception as e:
            async with self._mu:
                if not fut.done():
                    fut.set_exception(e)
                self._inflight.pop(key, None)
            raise

# ------------------ Exchange data (candles) ------------------
class MultiExchangeData:
    BINANCE_SPOT = "https://api.binance.com"
    BINANCE_FUTURES = "https://fapi.binance.com"
    BYBIT = "https://api.bybit.com"
    OKX = "https://www.okx.com"
    MEXC = "https://api.mexc.com"
    GATEIO = "https://api.gateio.ws/api/v4"

    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None

        # --- Candle/market caching (stability + less API spam) ---
        self._candles_cache: dict[tuple, tuple[float, pd.DataFrame]] = {}  # key -> (expires_mono, df)
        self._candles_inflight: dict[tuple, asyncio.Future] = {}  # key -> Future[df]
        self._markets_cache: dict[str, tuple[float, set[str]]] = {}  # exchange -> (expires_mono, set(symbols))
        self._unsupported_until: dict[tuple, float] = {}  # (exchange, market, symbol, interval) -> expires_mono

        # Limits / timeouts
        self._http_concurrency = int(os.getenv("HTTP_CONCURRENCY", "25") or "25")
        self._http_req_timeout_sec = float(os.getenv("HTTP_REQ_TIMEOUT_SEC", "6") or "6")
        if MID_CANDLES_LIGHT_MODE:
            # keep REST fast in light mode; fallback to other exchanges instead of waiting long
            try:
                self._http_req_timeout_sec = float(min(self._http_req_timeout_sec, MID_LIGHT_HTTP_TIMEOUT_SEC))
            except Exception:
                self._http_req_timeout_sec = self._http_req_timeout_sec
        self._http_sem = asyncio.Semaphore(max(1, self._http_concurrency))

        # Feature flags
        self._market_avail_check = (os.getenv("MID_MARKET_AVAIL_CHECK", "1") or "1").strip().lower() in ("1","true","yes","on")
        self._markets_cache_ttl = int(os.getenv("MID_MARKETS_CACHE_TTL_SEC", "21600") or "21600")  # 6h
        self._unsupported_ttl = int(os.getenv("MID_UNSUPPORTED_SYMBOL_TTL_SEC", "21600") or "21600")  # 6h

        # Candle cache counters (for [mid][summary])
        self._candle_cache_hit = 0
        self._candle_cache_miss = 0
        self._candle_inflight_wait = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    def candle_counters_snapshot(self) -> tuple[int,int,int]:
        """Return (hit, miss, inflight_wait) counters."""
        try:
            return (int(self._candle_cache_hit), int(self._candle_cache_miss), int(self._candle_inflight_wait))
        except Exception:
            return (0,0,0)


    def _norm_top_symbol(self, s: str) -> str:
        s = (s or "").upper().strip()
        s = s.replace("-", "").replace("_", "").replace("/", "")
        return s

    def _is_ok_top_symbol(self, sym: str) -> bool:
        if not sym.endswith("USDT"):
            return False
        if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            return False
        return True

    async def _top_from_binance(self) -> list[tuple[float, str]]:
        assert self.session is not None
        url = f"{self.BINANCE_SPOT}/api/v3/ticker/24hr"
        data = await self._get_json(url)
        out: list[tuple[float, str]] = []
        for t in data or []:
            sym = self._norm_top_symbol(t.get("symbol", ""))
            if not self._is_ok_top_symbol(sym):
                continue
            try:
                qv = float(t.get("quoteVolume", "0") or 0)
            except Exception:
                qv = 0.0
            out.append((qv, sym))
        return out

    async def _top_from_mexc(self) -> list[tuple[float, str]]:
        assert self.session is not None
        url = f"{self.MEXC}/api/v3/ticker/24hr"
        data = await self._get_json(url)
        out: list[tuple[float, str]] = []
        for t in data or []:
            sym = self._norm_top_symbol(t.get("symbol", ""))
            if not self._is_ok_top_symbol(sym):
                continue
            try:
                # MEXC uses quoteVolume too
                qv = float(t.get("quoteVolume", "0") or 0)
            except Exception:
                qv = 0.0
            out.append((qv, sym))
        return out

    async def _top_from_bybit(self) -> list[tuple[float, str]]:
        # Bybit v5 spot tickers
        url = f"{self.BYBIT}/v5/market/tickers"
        data = await self._get_json(url, params={"category": "spot"})
        lst = (((data or {}).get("result") or {}).get("list") or []) if isinstance(data, dict) else []
        out: list[tuple[float, str]] = []
        for t in lst:
            sym = self._norm_top_symbol(t.get("symbol", ""))
            if not self._is_ok_top_symbol(sym):
                continue
            # turnover24h is quote turnover (in USDT for USDT pairs)
            qv = 0.0
            for k in ("turnover24h", "quoteVolume", "volume24h"):
                try:
                    if t.get(k) is not None:
                        qv = float(t.get(k) or 0.0)
                        break
                except Exception:
                    continue
            out.append((qv, sym))
        return out

    async def _top_from_okx(self) -> list[tuple[float, str]]:
        url = f"{self.OKX}/api/v5/market/tickers"
        data = await self._get_json(url, params={"instType": "SPOT"})
        lst = (data.get("data") or []) if isinstance(data, dict) else []
        out: list[tuple[float, str]] = []
        for t in lst:
            sym = self._norm_top_symbol(t.get("instId", ""))
            if not self._is_ok_top_symbol(sym):
                continue
            qv = 0.0
            # volCcy24h is quote volume
            for k in ("volCcy24h", "volCcy", "vol24h"):
                try:
                    if t.get(k) is not None:
                        qv = float(t.get(k) or 0.0)
                        break
                except Exception:
                    continue
            out.append((qv, sym))
        return out

    async def _top_from_gateio(self) -> list[tuple[float, str]]:
        url = f"{self.GATEIO}/spot/tickers"
        data = await self._get_json(url)
        out: list[tuple[float, str]] = []
        for t in data or []:
            sym = self._norm_top_symbol(t.get("currency_pair", ""))
            if not self._is_ok_top_symbol(sym):
                continue
            qv = 0.0
            # quote_volume is quote volume; some accounts use "quote_volume" / "quote_volume_24h"
            for k in ("quote_volume", "quote_volume_24h", "base_volume", "base_volume_24h"):
                try:
                    if t.get(k) is not None:
                        qv = float(t.get(k) or 0.0)
                        break
                except Exception:
                    continue
            out.append((qv, sym))
        return out

    async def get_top_usdt_symbols(self, n: int) -> List[str]:
        """Return Binance-style symbols (e.g. BTCUSDT) sorted by quote turnover.
    
        IMPORTANT: Binance may be unreachable (DNS/ban/region). We fall back to other exchanges.
        This method also uses a short TTL cache + provider cooldown to prevent REST storms.
        """
        assert self.session is not None
    
        # Fast path: return cached list (shared module-level cache) to avoid hitting REST every tick
        cached = _top_cache_get(n)
        if cached:
            try:
                self.last_top_provider = str(_TOP_SYMBOLS_CACHE.get("provider") or "CACHE")
            except Exception:
                pass
            return cached
    
        providers = [
            ("BINANCE", self._top_from_binance),
            ("BYBIT", self._top_from_bybit),
            ("OKX", self._top_from_okx),
            ("MEXC", self._top_from_mexc),
            ("GATEIO", self._top_from_gateio),
        ]
    
        last_err: Exception | None = None
        items: list[tuple[float, str]] = []
    
        for name, fn in providers:
            # Skip provider during cooldown (ban/rate limit backoff)
            if _top_provider_is_cooled(name):
                continue
            try:
                items = await fn()
                if items:
                    try:
                        self.last_top_provider = str(name)
                    except Exception:
                        pass
                    break
            except Exception as e:
                last_err = e
                # Set cooldown so we don't hammer the same provider again next tick
                try:
                    cd = int(_top_cooldown_for_error(name, e))
                    _top_set_provider_cooldown(name, cd)
                except Exception:
                    pass
                logger.warning("get_top_usdt_symbols: %s failed: %s", name, e)
                continue
    
        if not items:
            if last_err is not None:
                raise last_err
            return []
    
        items.sort(reverse=True, key=lambda x: float(x[0] or 0.0))
        syms = [sym for _, sym in items if sym]
    
        # Deduplicate while preserving order
        seen: set[str] = set()
        dedup: list[str] = []
        for s in syms:
            if s in seen:
                continue
            seen.add(s)
            dedup.append(s)
    
        # Save to TTL cache (shared across scanner ticks)
        try:
            _top_cache_set(dedup, getattr(self, "last_top_provider", ""))
        except Exception:
            pass
    
        if n <= 0:
            return dedup
        return dedup[:n]
    
    async def _get_json(self, url: str, params: Optional[Dict[str, str]] = None) -> Any:
        """HTTP GET JSON with concurrency limit + per-request timeout.

        Many exchanges (especially Binance) return HTTP 400 with a JSON error payload.
        aiohttp's raise_for_status() would hide that payload, making `no_candles` impossible
        to debug. We always read the body and include a short snippet in the exception.
        """
        assert self.session is not None
        async with self._http_sem:
            async def _do():
                try:
                    async with self.session.get(url, params=params) as r:
                        body_txt = await r.text()
                        if r.status >= 400:
                            snippet = (body_txt or "")[:300].replace("\n", " ").replace("\r", " ")
                            raise ExchangeAPIError(f"HTTP {r.status} url={str(r.url)} body={snippet}")

                        if not body_txt:
                            return None
                        try:
                            return json.loads(body_txt)
                        except Exception:
                            return None
                except asyncio.TimeoutError as e:
                    raise ExchangeAPIError(f"HTTP timeout url={url}") from e
                except (aiohttp.ClientError, OSError, ConnectionResetError) as e:
                    raise ExchangeAPIError(f"HTTP error url={url} err={type(e).__name__}: {e}") from e

            return await asyncio.wait_for(_do(), timeout=self._http_req_timeout_sec)




    def _interval_cache_ttl(self, interval: str) -> float:
        """TTL for candle cache, seconds."""
        s = (interval or "").strip().lower()
        if s == "5m":
            return float(os.getenv("CANDLE_CACHE_TTL_5M_SEC", "15") or "15")
        if s == "15m":
            return float(os.getenv("CANDLE_CACHE_TTL_15M_SEC", "30") or "30")
        if s == "30m":
            return float(os.getenv("CANDLE_CACHE_TTL_30M_SEC", "90") or "90")
        if s in ("1h","60m"):
            return float(os.getenv("CANDLE_CACHE_TTL_1H_SEC", "180") or "180")
        if s in ("4h","240m"):
            return float(os.getenv("CANDLE_CACHE_TTL_4H_SEC", "600") or "600")
        return float(os.getenv("CANDLE_CACHE_TTL_DEFAULT_SEC", "30") or "30")

    def _is_unsupported_cached(self, exchange: str, market: str, symbol: str, interval: str) -> bool:
        key = (exchange.upper(), (market or "").upper(), symbol.upper(), (interval or "").lower())
        until = self._unsupported_until.get(key)
        if until is None:
            return False
        if time.monotonic() >= until:
            self._unsupported_until.pop(key, None)
            return False
        return True

    def _mark_unsupported(self, exchange: str, market: str, symbol: str, interval: str) -> None:
        key = (exchange.upper(), (market or "").upper(), symbol.upper(), (interval or "").lower())
        self._unsupported_until[key] = time.monotonic() + float(self._unsupported_ttl)

    async def _get_gateio_markets(self) -> set[str]:
        now = time.monotonic()
        cached = self._markets_cache.get("GATEIO")
        if cached and now < cached[0]:
            return cached[1]
        url = f"{self.GATEIO}/spot/currency_pairs"
        try:
            raw = await self._get_json(url, params=None)
            s: set[str] = set()
            if isinstance(raw, list):
                for r in raw:
                    if isinstance(r, dict):
                        pid = (r.get("id") or r.get("currency_pair") or "").strip()
                        if pid:
                            s.add(pid.upper())
            self._markets_cache["GATEIO"] = (now + float(self._markets_cache_ttl), s)
            return s
        except Exception:
            if cached:
                return cached[1]
            return set()

    async def _get_mexc_markets(self) -> set[str]:
        now = time.monotonic()
        cached = self._markets_cache.get("MEXC")
        if cached and now < cached[0]:
            return cached[1]
        url = f"{self.MEXC}/api/v3/exchangeInfo"
        try:
            raw = await self._get_json(url, params=None)
            s: set[str] = set()
            if isinstance(raw, dict):
                for r in (raw.get("symbols") or []):
                    if isinstance(r, dict):
                        sym = (r.get("symbol") or "").strip()
                        if sym:
                            s.add(sym.upper())
            self._markets_cache["MEXC"] = (now + float(self._markets_cache_ttl), s)
            return s
        except Exception:
            if cached:
                return cached[1]
            return set()


    async def _get_binance_futures_markets(self) -> set[str]:
        """Cached set of BINANCE USDT-M futures symbols (canonical like BTCUSDT)."""
        now = time.monotonic()
        cached = self._markets_cache.get("BINANCE_FUTURES")
        if cached and now < cached[0]:
            return cached[1]
        url = f"{self.BINANCE_FUTURES}/fapi/v1/exchangeInfo"
        try:
            raw = await self._get_json(url, params=None)
            s: set[str] = set()
            if isinstance(raw, dict):
                for r in (raw.get("symbols") or []):
                    if isinstance(r, dict):
                        sym = (r.get("symbol") or "").strip()
                        if not sym:
                            continue
                        stt = str(r.get("status") or "").upper()
                        if stt and stt not in ("TRADING", "1"):
                            continue
                        s.add(sym.upper())
            self._markets_cache["BINANCE_FUTURES"] = (now + float(self._markets_cache_ttl), s)
            return s
        except Exception:
            if cached:
                return cached[1]
            return set()

    async def _get_binance_spot_markets(self) -> set[str]:
        """Cached set of BINANCE spot symbols (canonical like BTCUSDT)."""
        now = time.monotonic()
        cached = self._markets_cache.get("BINANCE_SPOT")
        if cached and now < cached[0]:
            return cached[1]
        url = f"{self.BINANCE_SPOT}/api/v3/exchangeInfo"
        try:
            raw = await self._get_json(url, params=None)
            s: set[str] = set()
            if isinstance(raw, dict):
                for r in (raw.get("symbols") or []):
                    if isinstance(r, dict):
                        sym = (r.get("symbol") or "").strip()
                        if not sym:
                            continue
                        stt = str(r.get("status") or "").upper()
                        if stt and stt not in ("TRADING", "1"):
                            continue
                        s.add(sym.upper())
            self._markets_cache["BINANCE_SPOT"] = (now + float(self._markets_cache_ttl), s)
            return s
        except Exception:
            if cached:
                return cached[1]
            return set()

    async def _get_bybit_markets(self, *, category: str) -> set[str]:
        """Cached set of BYBIT symbols for category 'linear' (futures) or 'spot'."""
        cat = (category or "spot").strip().lower()
        key_name = f"BYBIT_{cat.upper()}"
        now = time.monotonic()
        cached = self._markets_cache.get(key_name)
        if cached and now < cached[0]:
            return cached[1]
        url = f"{self.BYBIT}/v5/market/instruments-info"
        s: set[str] = set()
        try:
            cursor = None
            # Safety cap: avoid infinite pagination loops if API misbehaves
            for _ in range(0, 8):
                params = {"category": cat, "limit": "1000"}
                if cursor:
                    params["cursor"] = cursor
                raw = await self._get_json(url, params=params)
                if isinstance(raw, dict) and str(raw.get("retCode", "0")) not in ("0", ""):
                    raise ExchangeAPIError(f"BYBIT instruments-info retCode={raw.get('retCode')} retMsg={raw.get('retMsg')}")
                lst = ((raw.get("result") or {}).get("list") or []) if isinstance(raw, dict) else []
                for r in lst:
                    if isinstance(r, dict):
                        sym = (r.get("symbol") or "").strip()
                        if sym:
                            s.add(sym.upper())
                nxt = ((raw.get("result") or {}).get("nextPageCursor") or "") if isinstance(raw, dict) else ""
                if not nxt or nxt == cursor:
                    break
                cursor = nxt
            self._markets_cache[key_name] = (now + float(self._markets_cache_ttl), s)
            return s
        except Exception:
            if cached:
                return cached[1]
            return set()

    async def _get_okx_markets(self, *, inst_type: str) -> set[str]:
        """Cached set of OKX instIds normalized to canonical BASEQUOTE (BTCUSDT)."""
        it = (inst_type or "SPOT").strip().upper()
        key_name = f"OKX_{it}"
        now = time.monotonic()
        cached = self._markets_cache.get(key_name)
        if cached and now < cached[0]:
            return cached[1]
        url = f"{self.OKX}/api/v5/public/instruments"
        s: set[str] = set()
        try:
            raw = await self._get_json(url, params={"instType": it})
            if isinstance(raw, dict) and str(raw.get("code", "0")) not in ("0", ""):
                raise ExchangeAPIError(f"OKX instruments code={raw.get('code')} msg={raw.get('msg')}")
            lst = (raw.get("data") or []) if isinstance(raw, dict) else []
            for r in lst:
                if not isinstance(r, dict):
                    continue
                inst = (r.get("instId") or "").strip().upper()
                if not inst:
                    continue
                # Spot: BTC-USDT  | Swap: BTC-USDT-SWAP
                if it == 'SWAP' and not inst.endswith('-SWAP'):
                    continue
                if "-USDT" in inst:
                    base = inst.split("-USDT", 1)[0].replace("-", "").replace("_", "")
                    if base:
                        s.add(f"{base}USDT")
            self._markets_cache[key_name] = (now + float(self._markets_cache_ttl), s)
            return s
        except Exception:
            if cached:
                return cached[1]
            return set()

    async def _market_supported_ex(self, exchange: str, market: str, symbol: str) -> bool:
        """True if symbol exists on the exchange for the given market (SPOT/FUTURES)."""
        if not self._market_avail_check:
            return True
        ex = (exchange or "").upper().strip()
        mkt = (market or "SPOT").upper().strip()
        sym = (symbol or "").upper().strip()
        if not sym:
            return False

        # Gate/MEXC: already supported via old helper (spot-only)
        if ex in ("GATEIO", "MEXC") and mkt == "SPOT":
            return await self._market_supported(ex, sym)

        if ex == "BINANCE" and mkt == "FUTURES":
            markets = await self._get_binance_futures_markets()
            sym_req = self._binance_futures_symbol_alias(sym)
            return (sym in markets) or (sym_req in markets)

        if ex == "BINANCE" and mkt == "SPOT":
            markets = await self._get_binance_spot_markets()
            return sym in markets

        if ex == "BYBIT":
            cat = "linear" if mkt == "FUTURES" else "spot"
            markets = await self._get_bybit_markets(category=cat)
            return sym in markets

        if ex == "OKX":
            it = "SWAP" if mkt == "FUTURES" else "SPOT"
            markets = await self._get_okx_markets(inst_type=it)
            return sym in markets

        return True

    async def _market_supported(self, exchange: str, symbol: str) -> bool:
        if not self._market_avail_check:
            return True
        ex = (exchange or "").upper()
        sym = symbol.upper()
        if ex == "GATEIO":
            pair = _gate_pair(sym).upper()
            markets = await self._get_gateio_markets()
            return pair in markets
        if ex == "MEXC":
            markets = await self._get_mexc_markets()
            return sym in markets
        return True

    async def _klines_cached(self, *, cache_key: tuple, interval: str, fetch_coro):
        now = time.monotonic()
        cached = self._candles_cache.get(cache_key)
        if cached and now < cached[0]:
            try:
                self._candle_cache_hit += 1
            except Exception:
                pass
            return cached[1]

        inflight = self._candles_inflight.get(cache_key)
        if inflight is not None and not inflight.done():
            try:
                self._candle_inflight_wait += 1
            except Exception:
                pass
            return await inflight

        try:
            self._candle_cache_miss += 1
        except Exception:
            pass

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        # Prevent "Future exception was never retrieved" warnings if a fetch fails
        def _consume_exc(_fut: asyncio.Future):
            try:
                if not _fut.cancelled():
                    _ = _fut.exception()
            except Exception:
                pass
        fut.add_done_callback(_consume_exc)

        self._candles_inflight[cache_key] = fut
        try:
            df = await fetch_coro()
            ttl = self._interval_cache_ttl(interval)
            self._candles_cache[cache_key] = (time.monotonic() + ttl, df)
            if not fut.done():
                fut.set_result(df)
            dfn = _mid_norm_ohlcv(df)
            if dfn is None:
                _mid_diag_add(symb, ex_name, market, tf, 'bad_schema', 'cache')
                return pd.DataFrame()
            return dfn
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            cur = self._candles_inflight.get(cache_key)
            if cur is fut:
                self._candles_inflight.pop(cache_key, None)

    def _df_from_ohlcv(self, rows: List[List[Any]], order: str) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()

        if order == "binance":
            df = pd.DataFrame(rows, columns=[
                "open_time","open","high","low","close","volume",
                "close_time","quote_volume","n_trades","taker_base","taker_quote","ignore"
            ])
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            for col in ("open","high","low","close","volume","quote_volume"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
        elif order == "bybit":
            df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume","turnover"])
            df["open_time"] = pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="ms")
            for col in ("open","high","low","close","volume","turnover"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume","vol_ccy","vol_quote","confirm"])
            df["open_time"] = pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="ms")
            for col in ("open","high","low","close","volume","vol_ccy","vol_quote"):
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["open","high","low","close"]).sort_values("open_time").reset_index(drop=True)
        return df

    async def klines_binance(self, symbol: str, interval: str, limit: int = 250, market: str = 'SPOT') -> pd.DataFrame:
        sym = (symbol or '').upper().strip()
        iv = (interval or '').strip().lower()
        mkt = (market or 'SPOT').upper().strip()
        # Binance USDT-M futures has some contracts with a different symbol name
        # than spot (e.g. PEPE futures is typically 1000PEPEUSDT).
        sym_req = self._binance_futures_symbol_alias(sym) if mkt == 'FUTURES' else sym
        cache_key = ('BINANCE', mkt, sym_req, iv, int(limit))
        # Fast-skip unsupported symbols/markets (prevents BINANCE invalid symbol storms)
        symbol_key = sym_req
        if self._is_unsupported_cached("BINANCE", mkt, symbol_key, iv):
            return pd.DataFrame()
        try:
            if not await self._market_supported_ex("BINANCE", mkt, sym):
                # Mark unsupported for this market and optionally fall back (futures -> spot)
                self._mark_unsupported("BINANCE", mkt, symbol_key, iv)
                if mkt == 'FUTURES' and (os.getenv('BINANCE_FUTURES_FALLBACK_SPOT', '1') or '1').strip().lower() in ('1','true','yes','on'):
                    return await self.klines_binance(sym, interval, limit=limit, market='SPOT')
                return pd.DataFrame()
        except Exception:
            # If availability check fails, fall back to request (legacy behavior)
            pass


        async def _fetch():
            # Binance may return HTTP 200 with JSON error payload; validate type
            if mkt == 'FUTURES':
                url = f"{self.BINANCE_FUTURES}/fapi/v1/klines"
            else:
                url = f"{self.BINANCE_SPOT}/api/v3/klines"
            params = {'symbol': sym_req, 'interval': iv, 'limit': str(limit)}
            raw = await self._get_json(url, params=params)
            if isinstance(raw, dict) and 'code' in raw:
                raise ExchangeAPIError(f"BINANCE {mkt} code={raw.get('code')} msg={raw.get('msg')}")
            if not isinstance(raw, list):
                raise ExchangeAPIError(f"BINANCE {mkt} bad_payload type={type(raw).__name__}")
            return self._df_from_ohlcv(raw, 'binance')

        return await self._klines_cached(cache_key=cache_key, interval=iv, fetch_coro=_fetch)

    def _binance_futures_symbol_alias(self, sym: str) -> str:
        """Map spot-like symbols to Binance USDT-M futures symbol when they differ.

        Env override:
          BINANCE_FUTURES_SYMBOL_MAP="PEPEUSDT=1000PEPEUSDT,FLOKIUSDT=1000FLOKIUSDT"

        Defaults include only a few common cases to avoid surprising remaps.
        """
        try:
            if not hasattr(self, '_binance_fut_sym_map_cache'):
                m = {
                    'PEPEUSDT': '1000PEPEUSDT',
                    'FLOKIUSDT': '1000FLOKIUSDT',
                    'SHIBUSDT': '1000SHIBUSDT',
                }
                raw = (os.getenv('BINANCE_FUTURES_SYMBOL_MAP', '') or '').strip()
                if raw:
                    for part in raw.split(','):
                        part = part.strip()
                        if not part or '=' not in part:
                            continue
                        k, v = part.split('=', 1)
                        k = (k or '').upper().strip()
                        v = (v or '').upper().strip()
                        if k and v:
                            m[k] = v
                self._binance_fut_sym_map_cache = m
            return self._binance_fut_sym_map_cache.get(sym, sym)
        except Exception:
            return sym

    
    async def depth_binance(self, symbol: str, limit: int = 20) -> Optional[dict]:
        """Fetch top-of-book depth from Binance Spot. Returns None on errors.
        When ORDERBOOK_FILTER is enabled, you can use this to compute imbalance.
        """
        url = f"{self.BINANCE_SPOT}/api/v3/depth"
        params = {"symbol": symbol, "limit": str(limit)}
        try:
            data = await self._get_json(url, params=params)
            return data if isinstance(data, dict) else None
        except Exception:
            return None
    async def klines_bybit(self, symbol: str, interval: str, limit: int = 200, market: str = 'SPOT') -> pd.DataFrame:
        sym = (symbol or '').upper().strip()
        iv = (interval or '').strip().lower()
        mkt = (market or 'SPOT').upper().strip()
        cache_key = ('BYBIT', mkt, sym, iv, int(limit))

        # Fast-skip unsupported symbols/markets (prevents BYBIT empty/no_candles storms)
        if self._is_unsupported_cached("BYBIT", mkt, sym, iv):
            return pd.DataFrame()
        try:
            if not await self._market_supported_ex("BYBIT", mkt, sym):
                self._mark_unsupported("BYBIT", mkt, sym, iv)
                return pd.DataFrame()
        except Exception:
            # If availability check fails, fall back to request (legacy behavior)
            pass

        async def _fetch():
            interval_map = {'5m':'5', '15m':'15', '30m':'30', '1h':'60', '4h':'240'}
            itv = interval_map.get(iv, '15')
            url = f"{self.BYBIT}/v5/market/kline"
            category = 'linear' if mkt == 'FUTURES' else 'spot'
            params = {'category': category, 'symbol': sym, 'interval': itv, 'limit': str(limit)}
            data = await self._get_json(url, params=params)
            if not isinstance(data, dict):
                raise ExchangeAPIError(f"BYBIT {mkt} bad_payload type={type(data).__name__}")
            if isinstance(data, dict) and str(data.get('retCode', '0')) not in ('0', ''):
                raise ExchangeAPIError(f"BYBIT {mkt} retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
            rows = (data or {}).get('result', {}).get('list', []) or []
            rows = list(reversed(rows))
            return self._df_from_ohlcv(rows, 'bybit')

        return await self._klines_cached(cache_key=cache_key, interval=iv, fetch_coro=_fetch)

    def okx_inst(self, symbol: str) -> str:
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT"
        return symbol

    async def klines_okx(self, symbol: str, interval: str, limit: int = 200, market: str = 'SPOT') -> pd.DataFrame:
        sym = (symbol or '').upper().strip()
        iv = (interval or '').strip().lower()
        mkt = (market or 'SPOT').upper().strip()
        cache_key = ('OKX', mkt, sym, iv, int(limit))

        if self._is_unsupported_cached("OKX", mkt, sym, iv):
            return pd.DataFrame()
        try:
            if not await self._market_supported_ex("OKX", mkt, sym):
                self._mark_unsupported("OKX", mkt, sym, iv)
                return pd.DataFrame()
        except Exception:
            pass

        async def _fetch():
            bar_map = {'5m':'5m', '15m':'15m', '30m':'30m', '1h':'1H', '4h':'4H'}
            bar = bar_map.get(iv, '15m')
            if mkt == 'FUTURES':
                inst = self.okx_inst(sym) + '-SWAP'
            else:
                inst = self.okx_inst(sym)
            url = f"{self.OKX}/api/v5/market/candles"
            params = {'instId': inst, 'bar': bar, 'limit': str(limit)}
            data = await self._get_json(url, params=params)
            if not isinstance(data, dict):
                raise ExchangeAPIError(f"OKX {mkt} bad_payload type={type(data).__name__}")
            if isinstance(data, dict) and str(data.get('code', '0')) not in ('0', ''):
                raise ExchangeAPIError(f"OKX {mkt} code={data.get('code')} msg={data.get('msg')}")
            rows = (data or {}).get('data', []) or []
            rows = list(reversed(rows))
            return self._df_from_ohlcv(rows, 'okx')

        return await self._klines_cached(cache_key=cache_key, interval=iv, fetch_coro=_fetch)


    async def klines_mexc(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        """Fetch klines from MEXC Spot (Binance-compatible /api/v3/klines)."""
        sym = (symbol or "").upper().strip()
        iv = (interval or "").strip().lower()
        market = "SPOT"

        if self._is_unsupported_cached("MEXC", market, sym, iv):
            return pd.DataFrame()

        if not await self._market_supported("MEXC", sym):
            self._mark_unsupported("MEXC", market, sym, iv)
            return pd.DataFrame()

        cache_key = ("MEXC", market, sym, iv, int(limit))

        def _mexc_interval(x: str) -> str:
            x = (x or "").strip().lower()
            if x.endswith("h"):
                try:
                    return f"{int(x[:-1]) * 60}m"
                except Exception:
                    return x
            return x

        async def _fetch():
            url = f"{self.MEXC}/api/v3/klines"
            params = {"symbol": sym, "interval": _mexc_interval(iv), "limit": str(limit)}
            try:
                raw = await self._get_json(url, params=params)
            except aiohttp.ClientResponseError as e:
                if getattr(e, "status", None) in (400, 404):
                    self._mark_unsupported("MEXC", market, sym, iv)
                    return pd.DataFrame()
                raise

            rows = raw if isinstance(raw, list) else []
            norm: list[list] = []
            for r in rows:
                if not isinstance(r, (list, tuple)) or len(r) < 6:
                    continue
                open_t = r[0]
                o = r[1]
                h = r[2] if len(r) > 2 else None
                l = r[3] if len(r) > 3 else None
                c = r[4] if len(r) > 4 else None
                v = r[5] if len(r) > 5 else None
                close_t = r[6] if len(r) > 6 else None
                quote_v = r[7] if len(r) > 7 else None
                norm.append([open_t, o, h, l, c, v, close_t, quote_v, 0, 0, 0, 0])
            return self._df_from_ohlcv(norm, "binance")

        return await self._klines_cached(cache_key=cache_key, interval=iv, fetch_coro=_fetch)


    async def klines_gateio(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        """Fetch klines from Gate.io Spot API v4 /spot/candlesticks."""
        sym = (symbol or "").upper().strip()
        iv = (interval or "").strip().lower()
        market = "SPOT"

        if self._is_unsupported_cached("GATEIO", market, sym, iv):
            return pd.DataFrame()

        if not await self._market_supported("GATEIO", sym):
            self._mark_unsupported("GATEIO", market, sym, iv)
            return pd.DataFrame()

        cache_key = ("GATEIO", market, sym, iv, int(limit))

        async def _fetch():
            pair = _gate_pair(sym)
            url = f"{self.GATEIO}/spot/candlesticks"
            params = {"currency_pair": pair, "interval": iv, "limit": str(limit)}
            try:
                raw = await self._get_json(url, params=params)
            except aiohttp.ClientResponseError as e:
                if getattr(e, "status", None) in (400, 404):
                    self._mark_unsupported("GATEIO", market, sym, iv)
                    return pd.DataFrame()
                raise

            rows = raw if isinstance(raw, list) else []
            norm: list[list] = []

            def _interval_ms(s: str) -> int:
                s = (s or "").strip().lower()
                try:
                    if s.endswith("m"):
                        return int(float(s[:-1]) * 60_000)
                    if s.endswith("h"):
                        return int(float(s[:-1]) * 3_600_000)
                    if s.endswith("d"):
                        return int(float(s[:-1]) * 86_400_000)
                except Exception:
                    return 0
                return 0

            step_ms = _interval_ms(iv)

            for r in rows:
                if not isinstance(r, (list, tuple)) or len(r) < 6:
                    continue
                t_s, v, c, h, l, o = r[0], r[1], r[2], r[3], r[4], r[5]
                try:
                    t_ms = int(float(t_s) * 1000.0)
                except Exception:
                    continue
                close_ms = t_ms + step_ms if step_ms > 0 else t_ms
                norm.append([t_ms, o, h, l, c, v, close_ms, 0, 0, 0, 0, 0])

            return self._df_from_ohlcv(norm, "binance")

        return await self._klines_cached(cache_key=cache_key, interval=iv, fetch_coro=_fetch)


# ------------------ Indicators / engine ------------------
def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add core indicators used by the engine.
    Expected columns: open, high, low, close, (optional) volume.
    """
    df = df.copy()
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    # Trend
    try:
        df["ema20"] = EMAIndicator(close, window=20).ema_indicator()
        df["ema50"] = EMAIndicator(close, window=50).ema_indicator()
        df["ema200"] = EMAIndicator(close, window=200).ema_indicator()
    except Exception:
        df["ema20"] = np.nan
        df["ema50"] = np.nan
        df["ema200"] = np.nan

    # Momentum
    try:
        df["rsi"] = RSIIndicator(close, window=14).rsi()
    except Exception:
        df["rsi"] = np.nan

    try:
        macd = MACD(close, window_slow=26, window_fast=12, window_sign=9)
        df["macd"] = macd.macd()
        df["macd_signal"] = macd.macd_signal()
        df["macd_hist"] = macd.macd_diff()
    except Exception:
        df["macd"] = np.nan
        df["macd_signal"] = np.nan
        df["macd_hist"] = np.nan

    # Trend strength
    try:
        df["adx"] = ADXIndicator(high, low, close, window=14).adx()
    except Exception:
        df["adx"] = np.nan

    # Volatility
    try:
        df["atr"] = AverageTrueRange(high, low, close, window=14).average_true_range()
    except Exception:
        df["atr"] = np.nan


    # ATR percent (volatility in % of price)
    try:
        _c = close.replace(0, np.nan)
        df["atr_pct"] = (df["atr"].astype(float) / _c.astype(float)) * 100.0
    except Exception:
        df["atr_pct"] = np.nan
    # Bollinger Bands
    try:
        bb = BollingerBands(close, window=20, window_dev=2)
        df["bb_mavg"] = bb.bollinger_mavg()
        df["bb_high"] = bb.bollinger_hband()
        df["bb_low"] = bb.bollinger_lband()
    except Exception:
        df["bb_mavg"] = np.nan
        df["bb_high"] = np.nan
        df["bb_low"] = np.nan

    # Volume-based (optional)
    if "volume" in df.columns:
        vol = df["volume"].astype(float).fillna(0.0)
        df["vol_sma20"] = vol.rolling(window=20, min_periods=5).mean()

        try:
            df["obv"] = OnBalanceVolumeIndicator(close, vol).on_balance_volume()
        except Exception:
            df["obv"] = np.nan

        try:
            df["mfi"] = MFIIndicator(high, low, close, vol, window=14).money_flow_index()
        except Exception:
            df["mfi"] = np.nan

        # VWAP (cumulative)
        try:
            tp = (high + low + close) / 3.0
            pv = tp * vol
            cum_vol = vol.cumsum().replace(0, np.nan)
            df["vwap"] = pv.cumsum() / cum_vol
        except Exception:
            df["vwap"] = np.nan
    else:
        df["vol_sma20"] = np.nan
        df["obv"] = np.nan
        df["mfi"] = np.nan
        df["vwap"] = np.nan

    return df


def _trend_dir(df: pd.DataFrame) -> Optional[str]:
    row = df.iloc[-1]
    if pd.isna(row["ema50"]) or pd.isna(row["ema200"]):
        return None
    return "LONG" if row["ema50"] > row["ema200"] else ("SHORT" if row["ema50"] < row["ema200"] else None)

def _trigger_15m(direction: str, df15i: pd.DataFrame) -> bool:
    """
    Entry trigger on 15m timeframe with configurable strict/medium thresholds via ENV.
    """
    if len(df15i) < 25:
        return False
    last = df15i.iloc[-1]
    prev = df15i.iloc[-2]

    # Required indicators
    req = ("rsi", "macd", "macd_signal", "macd_hist", "bb_mavg", "bb_high", "bb_low", "vol_sma20", "mfi", "adx")
    if any(pd.isna(last.get(x, np.nan)) for x in req) or pd.isna(prev.get("macd_hist", np.nan)):
        return False

    close = float(last["close"])
    prev_close = float(prev["close"])

    # ADX on 15m should show at least some structure (helps reduce chop)
    adx15 = float(last.get("adx", np.nan))
    if not np.isnan(adx15) and adx15 < max(10.0, TA_MIN_ADX - 6.0):
        return False

    # MACD momentum confirmation + optional histogram magnitude
    macd_hist = float(last["macd_hist"])
    prev_hist = float(prev["macd_hist"])
    if direction == "LONG":
        macd_ok = (last["macd"] > last["macd_signal"]) and (macd_hist > prev_hist) and (macd_hist >= TA_MIN_MACD_H)
    else:
        macd_ok = (last["macd"] < last["macd_signal"]) and (macd_hist < prev_hist) and (macd_hist <= -TA_MIN_MACD_H)
    if not macd_ok:
        return False

    # RSI cross 50 with guard rails (configurable)
    rsi_last = float(last["rsi"])
    rsi_prev = float(prev["rsi"])
    if direction == "LONG":
        rsi_ok = (rsi_prev < 50) and (rsi_last >= 50) and (rsi_last <= TA_RSI_MAX_LONG)
        if float(last["mfi"]) >= 80:
            return False
    else:
        rsi_ok = (rsi_prev > 50) and (rsi_last <= 50) and (rsi_last >= TA_RSI_MIN_SHORT)
        if float(last["mfi"]) <= 20:
            return False
    if not rsi_ok:
        return False

    # Bollinger filter
    bb_mid = float(last["bb_mavg"])
    bb_high = float(last["bb_high"])
    bb_low = float(last["bb_low"])

    if direction == "LONG":
        # avoid extremes: stay below upper band unless breakout is required
        if TA_BB_REQUIRE_BREAKOUT:
            # require cross above midline on the last candle
            if not (prev_close <= bb_mid and close > bb_mid):
                return False
        else:
            if not (close > bb_mid and close < bb_high):
                return False
    else:
        if TA_BB_REQUIRE_BREAKOUT:
            if not (prev_close >= bb_mid and close < bb_mid):
                return False
        else:
            if not (close < bb_mid and close > bb_low):
                return False

    # Volume activity: require above-average volume
    vol = float(last.get("volume", 0.0) or 0.0)
    vol_sma = float(last["vol_sma20"])
    if vol_sma > 0 and vol < vol_sma * TA_MIN_VOL_X:
        return False

    return True



# ------------------ MID anti top/bottom-trap filters (1H structure) ------------------
# Goal: reduce SL frequency by blocking MID entries that occur too close to local extremes
# or right after liquidity sweeps / exhaustion impulses.
#
# Applies to BOTH LONG and SHORT, and to BOTH FUTURES and SPOT MID signals.
#
# Env toggles (all optional):
#   MID_TRAP_FILTERS=1            enable/disable all filters
#   MID_TRAP_ATR_MULT=1.2         distance-to-extreme threshold in ATR(1h)
#   MID_TRAP_LOOKBACK=48          candles for local high/low on 1h
#   MID_TRAP_WICK_RATIO=0.60      wick/body ratio threshold
#   MID_TRAP_WICK_LAST=3          how many last 1h candles to inspect
#   MID_TRAP_WICK_HITS=2          require >= hits among last candles
#   MID_TRAP_IMPULSE_N=6          consecutive candles threshold
#   MID_TRAP_WEAK_LAST=3          last candles to detect "pierce & fail"
#   MID_TRAP_WEAK_BASE=24         base window for previous extreme (excluding last candles)
#   MID_TRAP_PIERCE_PCT=0.0       minimal pierce percent (0.1 = 0.1%)
def _mid_trap_env_bool(name: str, default: bool) -> bool:
    try:
        v = (os.getenv(name) or "").strip().lower()
        if not v:
            return bool(default)
        return v not in ("0","false","no","off")
    except Exception:
        return bool(default)

def _mid_trap_env_int(name: str, default: int) -> int:
    try:
        v = (os.getenv(name) or "").strip()
        return int(float(v)) if v else int(default)
    except Exception:
        return int(default)

def _mid_trap_env_float(name: str, default: float) -> float:
    try:
        v = (os.getenv(name) or "").strip()
        return float(v) if v else float(default)
    except Exception:
        return float(default)

def _mid_atr_1h(df1hi: pd.DataFrame) -> float:
    try:
        last1h = df1hi.iloc[-1]
        atr = float(last1h.get("atr", np.nan))
        if not np.isnan(atr) and atr > 0:
            return float(atr)
    except Exception:
        pass
    # Fallback compute ATR-ish from true range
    try:
        hi = df1hi["high"].astype(float)
        lo = df1hi["low"].astype(float)
        cl = df1hi["close"].astype(float)
        prev = cl.shift(1)
        tr = np.maximum(hi - lo, np.maximum((hi - prev).abs(), (lo - prev).abs()))
        atr = float(tr.rolling(14, min_periods=8).mean().iloc[-1])
        if not np.isnan(atr) and atr > 0:
            return float(atr)
    except Exception:
        pass
    return 0.0

def _mid_wick_ratio(candle: pd.Series, *, direction: str) -> float:
    try:
        o = float(candle.get("open", np.nan))
        c = float(candle.get("close", np.nan))
        h = float(candle.get("high", np.nan))
        l = float(candle.get("low", np.nan))
        if any(np.isnan(x) for x in (o,c,h,l)):
            return 0.0
        body = abs(c - o)
        eps = max(1e-12, body)
        d = (direction or "").upper().strip()
        if d == "SHORT":
            wick = min(o, c) - l  # lower wick
        else:
            wick = h - max(o, c)  # upper wick
        if wick <= 0:
            return 0.0
        return float(wick / eps)
    except Exception:
        return 0.0

def _mid_consecutive_candles(df1hi: pd.DataFrame, *, direction: str, n: int) -> bool:
    try:
        if n <= 1:
            return False
        tail = df1hi.tail(n)
        if len(tail) < n:
            return False
        d = (direction or "").upper().strip()
        if d == "SHORT":
            return bool((tail["close"].astype(float) < tail["open"].astype(float)).all())
        return bool((tail["close"].astype(float) > tail["open"].astype(float)).all())
    except Exception:
        return False

def _mid_weak_extreme(df1hi: pd.DataFrame, *, direction: str, last_n: int, base_n: int, pierce_pct: float) -> bool:
    """Weak High/Low: price pierces previous extreme but fails to hold on closes."""
    try:
        if df1hi is None or df1hi.empty:
            return False
        last_n = max(1, int(last_n))
        base_n = max(last_n + 3, int(base_n))
        if len(df1hi) < (base_n + last_n):
            # best-effort: use what we have, but keep meaning
            base = df1hi.iloc[:-last_n] if len(df1hi) > last_n else df1hi
            tail = df1hi.tail(last_n)
        else:
            base = df1hi.iloc[-(base_n + last_n):-last_n]
            tail = df1hi.tail(last_n)
        d = (direction or "").upper().strip()
        pp = max(0.0, float(pierce_pct)) / 100.0
        if d == "SHORT":
            prev_low = float(base["low"].astype(float).min())
            if np.isnan(prev_low) or prev_low <= 0:
                return False
            # pierce below prev_low but close back above
            for _, r in tail.iterrows():
                lo = float(r.get("low", np.nan))
                cl = float(r.get("close", np.nan))
                if np.isnan(lo) or np.isnan(cl):
                    continue
                if lo < prev_low * (1.0 - pp) and cl > prev_low:
                    return True
            return False
        else:
            prev_high = float(base["high"].astype(float).max())
            if np.isnan(prev_high) or prev_high <= 0:
                return False
            for _, r in tail.iterrows():
                hi = float(r.get("high", np.nan))
                cl = float(r.get("close", np.nan))
                if np.isnan(hi) or np.isnan(cl):
                    continue
                if hi > prev_high * (1.0 + pp) and cl < prev_high:
                    return True
            return False
    except Exception:
        return False

def _mid_structure_trap_ok(*, direction: str, entry: float, df1hi: pd.DataFrame) -> tuple[bool, str]:
    enabled = _mid_trap_env_bool("MID_TRAP_FILTERS", True)
    if not enabled:
        return (True, "")

    d = (direction or "").upper().strip()
    if d not in ("LONG","SHORT"):
        return (True, "")

    atr_mult = max(0.1, _mid_trap_env_float("MID_TRAP_ATR_MULT", 1.2))
    lookback = max(12, _mid_trap_env_int("MID_TRAP_LOOKBACK", 48))
    wick_ratio_th = max(0.1, _mid_trap_env_float("MID_TRAP_WICK_RATIO", 0.60))
    wick_last = max(1, _mid_trap_env_int("MID_TRAP_WICK_LAST", 3))
    wick_hits_req = max(1, _mid_trap_env_int("MID_TRAP_WICK_HITS", 2))
    impulse_n = max(3, _mid_trap_env_int("MID_TRAP_IMPULSE_N", 6))
    weak_last = max(1, _mid_trap_env_int("MID_TRAP_WEAK_LAST", 3))
    weak_base = max(12, _mid_trap_env_int("MID_TRAP_WEAK_BASE", 24))
    pierce_pct = max(0.0, _mid_trap_env_float("MID_TRAP_PIERCE_PCT", 0.0))

    try:
        ep = float(entry)
        if np.isnan(ep) or ep <= 0:
            return (True, "")
    except Exception:
        return (True, "")

    if df1hi is None or df1hi.empty or len(df1hi) < 20:
        require_1h = _mid_trap_env_bool("MID_TRAP_REQUIRE_1H_DATA", True)
        if require_1h:
            return (False, "no_1h_data")
        return (True, "")

    atr1h = _mid_atr_1h(df1hi)
    if atr1h <= 0:
        return (True, "")

    # Filter 1: distance to local 1H extreme
    try:
        w = df1hi.tail(lookback)
        if d == "LONG":
            loc_high = float(w["high"].astype(float).max())
            dist = float(loc_high - ep)
            if dist <= atr_mult * atr1h:
                return (False, f"near_1h_high dist={dist:.6g} atr={atr1h:.6g} entry={entry:.6g}")
        else:
            loc_low = float(w["low"].astype(float).min())
            dist = float(ep - loc_low)
            if dist <= atr_mult * atr1h:
                return (False, f"near_1h_low dist={dist:.6g} atr={atr1h:.6g} entry={entry:.6g}")
    except Exception:
        pass

    # Filter 2: wick liquidity (upper for LONG / lower for SHORT)
    try:
        tail = df1hi.tail(wick_last)
        hits = 0
        for _, r in tail.iterrows():
            if _mid_wick_ratio(r, direction=d) > wick_ratio_th:
                hits += 1
        if hits >= wick_hits_req:
            return (False, f"wick_liquidity hits={hits}/{wick_last} entry={entry:.6g}")
    except Exception:
        pass

    # Filter 3: post-impulse exhaustion (N same-color candles)
    if _mid_consecutive_candles(df1hi, direction=d, n=impulse_n):
        return (False, f"post_impulse_{impulse_n} entry={entry:.6g}")

    # Filter 4: weak high/low (pierce & fail)
    if _mid_weak_extreme(df1hi, direction=d, last_n=weak_last, base_n=weak_base, pierce_pct=pierce_pct):
        return (False, f"weak_extreme_pierce_fail entry={entry:.6g}")

    return (True, "")



# ================== MID institutional TA engines (Structure/Regime/Sweep/BO-RT/OB) ==================

def _mid_bool_env(name: str, default: str = "0") -> bool:
    try:
        v = os.getenv(name, default)
        return str(v).strip().lower() not in ("0", "false", "no", "off", "")
    except Exception:
        return False


def _mid_market_regime(df30i: pd.DataFrame, df1hi: pd.DataFrame, *, entry: float, atr30: float, adx30: float | None) -> str:
    """Classify MID regime using 30m as primary with 1h confirmation.
    Returns one of: TRENDING, RANGING, EXPANSION, COMPRESSION.
    """
    try:
        if df30i is None or df30i.empty:
            return "—"
        adx = float(adx30) if (adx30 is not None and adx30 == adx30) else float(df30i.iloc[-1].get("adx", float("nan")))
        atrp = (float(atr30) / float(entry) * 100.0) if (entry and atr30) else 0.0

        # EMA distance and its change (compression vs expansion proxy)
        ema50 = df30i["ema50"].astype(float)
        ema200 = df30i["ema200"].astype(float)
        dist = (ema50 - ema200).abs() / df30i["close"].astype(float).replace(0, np.nan)
        dist_now = float(dist.iloc[-1]) if len(dist) else 0.0
        dist_prev = float(dist.iloc[-6]) if len(dist) >= 7 else dist_now
        dist_chg = dist_now - dist_prev

        # ATR slope (expansion/compression proxy)
        atrs = df30i.get("atr", None)
        atr_now = float(atr30)
        atr_sma = float("nan")
        if atrs is not None:
            try:
                atr_sma = float(pd.Series(atrs).astype(float).rolling(20).mean().iloc[-1])
            except Exception:
                atr_sma = float("nan")

        atr_rising = (not np.isnan(atr_sma)) and atr_now > atr_sma * 1.05
        atr_falling = (not np.isnan(atr_sma)) and atr_now < atr_sma * 0.95

        # Thresholds (tunable)
        adx_tr = float(os.getenv("MID_REGIME_ADX_TREND", "25") or 25)
        adx_rg = float(os.getenv("MID_REGIME_ADX_RANGE", "18") or 18)

        # Decide
        if (not np.isnan(adx)) and adx >= adx_tr and dist_chg >= 0:
            return "TRENDING"
        if (not np.isnan(adx)) and adx <= adx_rg and dist_now <= float(os.getenv("MID_REGIME_EMA_DIST_MAX", "0.010") or 0.010):
            # If volatility expands while ADX low, treat as EXPANSION (breakout risk)
            if atr_rising or (atrp >= float(os.getenv("MID_REGIME_ATR_PCT_EXP", "2.2") or 2.2) and dist_chg > 0):
                return "EXPANSION"
            return "RANGING"
        if atr_rising and dist_chg > 0:
            return "EXPANSION"
        if atr_falling and dist_chg < 0:
            return "COMPRESSION"
        # Fallback: trending-ish if EMA distance is meaningful
        if dist_now >= float(os.getenv("MID_REGIME_EMA_DIST_TREND", "0.015") or 0.015) and (not np.isnan(adx)) and adx >= 20:
            return "TRENDING"
        return "—"
    except Exception:
        return "—"


def _mid_structure_hhhl(df: pd.DataFrame, *, lookback: int = 220, pivot: int = 3) -> str:
    """Return HH-HL / LH-LL / RANGE / — based on pivot highs/lows on 1h (or 30m)."""
    try:
        if df is None or df.empty or len(df) < 60:
            return "—"
        d = df.tail(lookback).copy()
        highs = d["high"].astype(float).values
        lows = d["low"].astype(float).values

        ph = _pivot_points(highs, left=pivot, right=pivot, mode="high")
        pl = _pivot_points(lows, left=pivot, right=pivot, mode="low")
        if len(ph) < 2 or len(pl) < 2:
            return "—"
        (_, h1), (_, h2) = ph[-2], ph[-1]
        (_, l1), (_, l2) = pl[-2], pl[-1]

        # Tiny tolerance to treat near-equals as range
        tol = float(os.getenv("MID_STRUCTURE_TOL_PCT", "0.001") or 0.001)  # 0.1%
        hh = h2 > h1 * (1.0 + tol)
        lh = h2 < h1 * (1.0 - tol)
        hl = l2 > l1 * (1.0 + tol)
        ll = l2 < l1 * (1.0 - tol)

        if hh and hl:
            return "HH-HL"
        if lh and ll:
            return "LH-LL"

        # Range if both roughly equal
        if (abs(h2 - h1) <= abs(h1) * tol * 2) and (abs(l2 - l1) <= abs(l1) * tol * 2):
            return "RANGE"
        return "—"
    except Exception:
        return "—"


def _mid_liquidity_sweep_5m(df5i: pd.DataFrame, *, lookback: int = 20) -> Tuple[bool, bool, str]:
    """Detect simple stop-sweep on 5m vs recent extremes.
    Returns (sweep_long, sweep_short, txt).
    """
    try:
        if df5i is None or df5i.empty or len(df5i) < lookback + 3:
            return (False, False, "—")
        d = df5i.tail(lookback + 2).copy()
        # Use prior extremes excluding last bar
        prev = d.iloc[:-1]
        last = d.iloc[-1]
        prev_low = float(prev["low"].astype(float).min())
        prev_high = float(prev["high"].astype(float).max())
        low = float(last.get("low", np.nan))
        high = float(last.get("high", np.nan))
        close = float(last.get("close", np.nan))
        if np.isnan(low) or np.isnan(high) or np.isnan(close):
            return (False, False, "—")

        # Tolerance to avoid micro-wicks
        atr5 = float(last.get("atr", np.nan))
        tol = max((atr5 if (atr5 == atr5 and atr5 > 0) else 0.0) * 0.10, close * float(os.getenv("MID_SWEEP_TOL_PCT", "0.0008") or 0.0008))
        sweep_long = (low < prev_low - tol) and (close > prev_low + tol * 0.2)
        sweep_short = (high > prev_high + tol) and (close < prev_high - tol * 0.2)

        txt = "SWEEP↑" if sweep_long else ("SWEEP↓" if sweep_short else "—")
        return (bool(sweep_long), bool(sweep_short), txt)
    except Exception:
        return (False, False, "—")


def _mid_breakout_retest_trigger(df5i: pd.DataFrame, *, direction: str, support: float | None, resistance: float | None, atr5: float) -> Tuple[bool, str]:
    """Detect breakout and (optional) retest on 5m using provided S/R levels.
    Returns (triggered, label).
    """
    try:
        if df5i is None or df5i.empty or len(df5i) < 10:
            return (False, "—")
        diru = str(direction or "").upper()
        d = df5i.tail(int(os.getenv("MID_BORT_LOOKBACK_5M", "36") or 36)).copy()
        highs = d["high"].astype(float).values
        lows = d["low"].astype(float).values
        closes = d["close"].astype(float).values

        tol = max(float(atr5) * 0.25 if (atr5 and atr5 > 0) else 0.0, float(closes[-1]) * float(os.getenv("MID_BORT_TOL_PCT", "0.0015") or 0.0015))
        retest_bars = int(os.getenv("MID_RETEST_MAX_BARS_5M", "8") or 8)

        sup = float(support) if (support is not None and support == support) else float("nan")
        res = float(resistance) if (resistance is not None and resistance == resistance) else float("nan")

        bo_up = None
        if not np.isnan(res) and res > 0:
            for i in range(1, len(closes)):
                if closes[i - 1] <= res and closes[i] > res:
                    bo_up = i
                    break
        bo_dn = None
        if not np.isnan(sup) and sup > 0:
            for i in range(1, len(closes)):
                if closes[i - 1] >= sup and closes[i] < sup:
                    bo_dn = i
                    break

        def _has_retest(start_idx: int, lvl: float, side: str) -> bool:
            jmax = min(len(closes), start_idx + 1 + max(1, retest_bars))
            for j in range(start_idx + 1, jmax):
                if side == "UP":
                    if (lows[j] <= lvl + tol) and (closes[j] > lvl):
                        return True
                else:
                    if (highs[j] >= lvl - tol) and (closes[j] < lvl):
                        return True
            return False

        if diru == "LONG" and bo_up is not None:
            return (True, "BO↑+RT" if _has_retest(bo_up, res, "UP") else "BO↑")
        if diru == "SHORT" and bo_dn is not None:
            return (True, "BO↓+RT" if _has_retest(bo_dn, sup, "DN") else "BO↓")
        return (False, "—")
    except Exception:
        return (False, "—")


def _mid_order_block(df30i: pd.DataFrame, *, direction: str, atr30: float) -> Tuple[Optional[Tuple[float, float]], bool]:
    """Detect a simple order block on 30m and whether current 5m price is retesting it.
    Returns (zone(low,high), valid_found). Zone is in absolute price.
    """
    try:
        if df30i is None or df30i.empty or len(df30i) < 60:
            return (None, False)
        diru = str(direction or "").upper()
        d = df30i.tail(int(os.getenv("MID_OB_LOOKBACK_30M", "160") or 160)).copy()
        atr_mult = float(os.getenv("MID_OB_ATR_IMPULSE_MULT", "1.2") or 1.2)
        # Impulse definition: body >= atr_mult * ATR(30m)
        atr = float(atr30)
        if atr <= 0 or np.isnan(atr):
            return (None, False)

        o = d["open"].astype(float).values
        h = d["high"].astype(float).values
        l = d["low"].astype(float).values
        c = d["close"].astype(float).values

        # Search from most recent backwards: last opposite candle before an impulse candle
        zone = None
        for i in range(len(d) - 3, 2, -1):
            body_next = abs(c[i + 1] - o[i + 1])
            if body_next < atr_mult * atr:
                continue
            # direction impulse
            imp_up = c[i + 1] > o[i + 1]
            imp_dn = c[i + 1] < o[i + 1]
            # candidate candle i: opposite color
            bear_i = c[i] < o[i]
            bull_i = c[i] > o[i]

            if diru == "LONG" and imp_up and bear_i:
                lo = min(l[i], c[i], o[i])
                hi = max(o[i], c[i])  # body top (more conservative)
                zone = (float(lo), float(hi))
                break
            if diru == "SHORT" and imp_dn and bull_i:
                hi = max(h[i], c[i], o[i])
                lo = min(o[i], c[i])  # body bottom
                zone = (float(lo), float(hi))
                break

        return (zone, bool(zone is not None))
    except Exception:
        return (None, False)

# ================== END MID institutional TA engines ==================
def evaluate_on_exchange_mid(df5: pd.DataFrame, df30: pd.DataFrame, df1h: pd.DataFrame, symbol: str = "", diag: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """MID analysis: 5m (trigger) / 30m (mid) / 1h (trend).

    Produces a result dict compatible with scanner_loop_mid and a rich TA block (like MAIN),
    but tuned for MID timeframes.
    """
    # Ensure diagnostics always have something meaningful (prevents other{unknown=...}).
    # We'll overwrite these defaults as we learn better stage/reason.
    try:
        if diag is not None:
            diag.setdefault("fail_stage", "other")
            diag.setdefault("fail_reason", "unknown")
    except Exception:
        pass

    if df5 is None or df30 is None or df1h is None:
        try:
            if diag is not None:
                diag["fail_stage"] = "other"
                diag["fail_reason"] = "missing_df"
        except Exception:
            pass
        return None
    if df5.empty or df30.empty or df1h.empty:
        try:
            if diag is not None:
                diag["fail_stage"] = "other"
                diag["fail_reason"] = "empty_df"
        except Exception:
            pass
        return None

    def _fail(stage: str, reason: str = "") -> None:
        try:
            if diag is None:
                return
            diag["fail_stage"] = str(stage)
            # Always write a reason so summary never falls back to unknown unless we truly have nothing.
            r = (str(reason).strip() if reason is not None else "")
            if not r:
                r = "unknown" if str(stage) == "other" else str(stage)
            diag["fail_reason"] = r
        except Exception:
            return

    try:
        df5i = _add_indicators(df5)
        df30i = _add_indicators(df30)
        df1hi = _add_indicators(df1h)
    except Exception:
        _fail("other", "indicators_error")
        return None

    # Directions (trend=1h, mid=30m)
    dir_trend = _trend_dir(df1hi)
    dir_mid = _trend_dir(df30i)

    last5 = df5i.iloc[-1]
    last30 = df30i.iloc[-1]
    last1h = df1hi.iloc[-1]

    entry = float(last5.get("close", np.nan))
    if np.isnan(entry) or entry <= 0:
        _fail("other", "bad_entry")
        return None

    # ATR from 30m (prefer indicator, fallback to true-range)
    atr30 = float(last30.get("atr", np.nan))
    if np.isnan(atr30) or atr30 <= 0:
        try:
            hi = df30i["high"].astype(float)
            lo = df30i["low"].astype(float)
            cl = df30i["close"].astype(float)
            prev = cl.shift(1)
            tr = np.maximum(hi - lo, np.maximum((hi - prev).abs(), (lo - prev).abs()))
            atr30 = float(tr.rolling(14).mean().iloc[-1])
        except Exception:
            _fail("other", "atr30_calc_error")
            return None
    if np.isnan(atr30) or atr30 <= 0:
        _fail("other", "bad_atr30")
        return None

    # Trend strength
    adx30 = float(last30.get("adx", np.nan))
    adx1h = float(last1h.get("adx", np.nan))
    if np.isnan(adx30):
        adx30 = float("nan")
    if np.isnan(adx1h):
        adx1h = float("nan")

    atr_pct = (atr30 / entry) * 100.0

    # Market hint for MID (used by autotune params / guards)
    try:
        market = choose_market(float(adx30) if not np.isnan(adx30) else float('nan'), float(atr_pct))
    except Exception:
        market = "FUTURES"


    # Momentum override for HTF bias (prevents countertrend signals during strong trend reversals)
    try:
        def _momentum_override(df: pd.DataFrame, base_dir: str | None, adx_val: float) -> str | None:
            row = df.iloc[-1]
            close = float(row.get("close", np.nan))
            ema20 = float(row.get("ema20", np.nan))
            ema50 = float(row.get("ema50", np.nan))
            ema200 = float(row.get("ema200", np.nan))
            if np.isnan(close) or np.isnan(ema20) or np.isnan(ema50) or np.isnan(ema200):
                return base_dir
            if not np.isnan(adx_val) and float(adx_val) >= 25.0:
                if close > ema200 and ema20 > ema50:
                    return "LONG"
                if close < ema200 and ema20 < ema50:
                    return "SHORT"
            return base_dir

        dir_trend = _momentum_override(df1hi, dir_trend, adx1h)
        dir_mid = _momentum_override(df30i, dir_mid, adx30)
    except Exception:
        pass

    if dir_trend is None:
        _fail("other", "no_trend_dir")
        return None
    if dir_mid is None:
        dir_mid = dir_trend




    # ATR on 5m (for smart trailing / near-TP2 logic in autotrade manager)
    atr5 = float(last5.get("atr", np.nan))
    if np.isnan(atr5) or atr5 <= 0:
        try:
            hi = df5i["high"].astype(float)
            lo = df5i["low"].astype(float)
            cl = df5i["close"].astype(float)
            prev = cl.shift(1)
            tr = np.maximum(hi - lo, np.maximum((hi - prev).abs(), (lo - prev).abs()))
            atr5 = float(tr.rolling(14).mean().iloc[-1])
        except Exception:
            atr5 = 0.0
    if np.isnan(atr5) or atr5 <= 0:
        atr5 = 0.0
    atr5_pct = (atr5 / entry) * 100.0 if entry > 0 else 0.0

    # MID structural trap filters (apply to LONG/SHORT, SPOT/FUTURES)
    ok_trap, trap_reason = _mid_structure_trap_ok(direction=dir_trend, entry=entry, df1hi=df1hi)
    if not ok_trap:
        try:
            
            _trap_msg = f"{symbol}|{dir_trend}|{_mid_trap_reason_key(str(trap_reason))}"
            _ctx = _MID_TICK_CTX.get()
            _tick_key = f"{symbol}|{dir_trend}|trap"
            if _ctx is not None and _tick_key in _ctx.get('trap', set()):
                _ctx['suppress_trap'] = int(_ctx.get('suppress_trap', 0)) + 1
            else:
                if _ctx is not None:
                    _ctx.setdefault('trap', set()).add(_tick_key)
                if _mid_trap_should_log(_trap_msg):
                    logger.info("[mid][trap] %s dir=%s reason=%s entry=%.6g", symbol, dir_trend, trap_reason, float(entry))

            _emit_mid_trap_event({"dir": str(dir_trend), "reason": str(trap_reason), "reason_key": _mid_trap_reason_key(str(trap_reason)), "entry": float(entry)})
        except Exception:
            pass
        _stage = "trap"
        try:
            _tr = str(trap_reason)
            if "post_impulse" in _tr:
                _stage = "impulse"
            elif ("weak_extreme" in _tr) or ("near_" in _tr) or ("extreme" in _tr):
                _stage = "extreme"
        except Exception:
            pass
        _fail(_stage, str(trap_reason))
        return None


    # Quality gate: avoid choppy markets (helps TP2 hit-rate)
    try:
        allow_range = os.getenv("MID_ALLOW_RANGE", "0").strip().lower() not in ("0","false","no","off")
        min_adx_30m = float(_mid_autotune_get_param("MID_MIN_ADX_30M", float(os.getenv("MID_MIN_ADX_30M", "0") or 0), market))
        min_adx_1h = float(_mid_autotune_get_param("MID_MIN_ADX_1H", float(os.getenv("MID_MIN_ADX_1H", "0") or 0), market))
        if not allow_range and min_adx_30m > 0 and min_adx_1h > 0:
            if (not np.isnan(adx30)) and (not np.isnan(adx1h)) and (adx30 < min_adx_30m) and (adx1h < min_adx_1h):
                _fail("trend", "adx_choppy")
                return None
    except Exception:
        pass

    # ------------------ MID institutional engines (optional) ------------------
    mid_regime = "—"
    struct_hhhl_1h = "—"
    sweep_long = sweep_short = False
    sweep_txt = "—"
    bo_rt_trigger = False
    bo_rt_label = "—"
    ob_zone = None  # (low, high)
    ob_found = False
    ob_retest = False

    try:
        use_regime = _mid_bool_env("MID_USE_REGIME", "1")
        use_structure = _mid_bool_env("MID_USE_STRUCTURE_ENGINE", "1")
        use_sweep = _mid_bool_env("MID_USE_LIQUIDITY_SWEEP", "1")
        use_bort = _mid_bool_env("MID_USE_BREAKOUT_RETEST", "1")
        use_ob = _mid_bool_env("MID_USE_ORDER_BLOCK", "1")
        require_trigger = _mid_bool_env("MID_REQUIRE_TRIGGER", "1")

        if use_regime:
            mid_regime = _mid_market_regime(df30i, df1hi, entry=float(entry), atr30=float(atr30), adx30=(float(adx30) if not np.isnan(adx30) else None))

        if use_structure:
            struct_hhhl_1h = _mid_structure_hhhl(df1hi, lookback=int(os.getenv("MID_STRUCTURE_LOOKBACK_1H", "220") or 220), pivot=int(os.getenv("MID_STRUCTURE_PIVOT", "3") or 3))
            # Hard block if structure contradicts direction
            if str(dir_trend).upper() == "LONG" and struct_hhhl_1h == "LH-LL":
                _fail("structure", "structure_mismatch")
                return None
            if str(dir_trend).upper() == "SHORT" and struct_hhhl_1h == "HH-HL":
                _fail("structure", "structure_mismatch")
                return None

        # Liquidity sweep trigger on 5m
        if use_sweep:
            lb = int(os.getenv("MID_SWEEP_LOOKBACK_5M", "20") or 20)
            sweep_long, sweep_short, sweep_txt = _mid_liquidity_sweep_5m(df5i, lookback=lb)

        # Breakout+Retest trigger (levels from 30m, fallback to 1h)
        sup_lvl = res_lvl = None
        try:
            sup_lvl, res_lvl = _nearest_levels(df30i, lookback=int(os.getenv("MID_BORT_LEVEL_LOOKBACK_30M", "180") or 180), swing=int(os.getenv("MID_BORT_LEVEL_SWING", "3") or 3))
            if sup_lvl is None and res_lvl is None:
                sup_lvl, res_lvl = _nearest_levels(df1hi, lookback=240, swing=3)
        except Exception:
            sup_lvl = res_lvl = None

        if use_bort:
            bo_rt_trigger, bo_rt_label = _mid_breakout_retest_trigger(df5i, direction=str(dir_trend), support=sup_lvl, resistance=res_lvl, atr5=float(atr5))

        # Order block on 30m + retest on 5m
        if use_ob:
            ob_zone, ob_found = _mid_order_block(df30i, direction=str(dir_trend), atr30=float(atr30))
            if ob_found and ob_zone and df5i is not None and not df5i.empty:
                try:
                    zlo, zhi = float(ob_zone[0]), float(ob_zone[1])
                    if zhi < zlo:
                        zlo, zhi = zhi, zlo
                    last5r = df5i.iloc[-1]
                    lo5 = float(last5r.get("low", np.nan))
                    hi5 = float(last5r.get("high", np.nan))
                    tol = max(float(atr5) * 0.25 if (atr5 and atr5 > 0) else 0.0, float(entry) * float(os.getenv("MID_OB_TOL_PCT", "0.0015") or 0.0015))
                    if (not np.isnan(lo5)) and (not np.isnan(hi5)):
                        ob_retest = (hi5 >= zlo - tol) and (lo5 <= zhi + tol)
                except Exception:
                    ob_retest = False

        # Regime gating: in ranges, ignore pure breakouts; prefer sweep/OB retests.
        if use_regime and mid_regime == "RANGING":
            if bo_rt_trigger and (not (sweep_long or sweep_short) and (not ob_retest)):
                # prevent range fake breakouts
                _fail("structure", "regime_range_no_breakout")
                return None

        # Require at least one execution trigger for MID (institutional quality)
        if require_trigger:
            trig_ok = False
            if str(dir_trend).upper() == "LONG":
                trig_ok = bool(sweep_long) or bool(bo_rt_trigger) or bool(ob_retest)
            else:
                trig_ok = bool(sweep_short) or bool(bo_rt_trigger) or bool(ob_retest)
            if not trig_ok:
                _fail("other", "no_trigger")
                return None
    except Exception:
        # Never block if engine fails unexpectedly.
        pass


    # --- Dynamic TP2/RR for MID ---

    def _tp2_r_mid(adx_1h: float, adx_30m: float, atrp: float) -> float:
        """TP2_R for MID, synchronized with MAIN adaptive TP2_R.

        - Uses MAIN's _adaptive_tp2_r(adx1h) (which already respects ADAPTIVE_TP2 + thresholds).
        - Applies a small MID discount (MID_RR_DISCOUNT, default 0.92) so MID stays slightly less greedy.
        - You can disable sync with MID_RR_SYNC_WITH_MAIN=0 (then falls back to MID_TP2_R).
        """
        try:
            sync = (os.getenv("MID_RR_SYNC_WITH_MAIN", "1").strip().lower() not in ("0","false","no","off"))
            if not sync:
                # keep legacy fixed RR for MID
                return float(os.getenv("MID_TP2_R", str(TP2_R)) or TP2_R)
            base = float(_adaptive_tp2_r(adx_1h))
            disc = float(_mid_autotune_get_param("MID_RR_DISCOUNT", 0.92, market))
            # clamp to sane range
            if disc < 0.70:
                disc = 0.70
            if disc > 1.00:
                disc = 1.00
            r = base * disc
            # keep within practical bounds
            if r < 1.20:
                r = 1.20
            if r > 3.50:
                r = 3.50
            return float(r)
        except Exception:
            try:
                return float(_adaptive_tp2_r(adx_1h))
            except Exception:
                return float(os.getenv("MID_TP2_R", str(TP2_R)) or TP2_R)

        # Base by trend strength
        if a1 >= 40:
            r = 3.0
        elif a1 >= 30:
            r = 2.6
        elif a1 >= 22:
            r = 2.2
        elif a1 >= 16:
            r = 1.8
        else:
            r = 1.5

        # Midframe confirmation
        if a30 >= 28:
            r += 0.2
        elif a30 <= 12:
            r -= 0.1

        # Volatility sanity: too low -> reduce; too high -> reduce
        try:
            v = float(atrp)
        except Exception:
            v = 0.0
        if v < 0.2:
            r -= 0.2
        elif v > 5.0:
            r -= 0.3
        elif v > 3.5:
            r -= 0.15

        return float(max(1.3, min(3.2, r)))

    tp2_r = _tp2_r_mid(adx1h, adx30, atr_pct)
    sl, tp1, tp2, rr = _build_levels(dir_trend, entry, atr30, tp2_r=tp2_r)
    # --- Adaptive MID SL/TP scaling by ATR%% (optional) ---
    try:
        if os.getenv("MID_ADAPTIVE_SLTP", "0").strip().lower() in ("1","true","yes","on"):
            # Use ATR% on 30m as market volatility regime
            atrp = float(atr_pct) if not np.isnan(atr_pct) else 0.0
            low = float(os.getenv("MID_ADAPT_ATR_PCT_LOW", "2.0") or 2.0)
            high = float(os.getenv("MID_ADAPT_ATR_PCT_HIGH", "6.0") or 6.0)
            sl_tight = float(os.getenv("MID_ADAPT_SL_TIGHTEN_MIN", "0.95") or 0.95)
            sl_wide = float(os.getenv("MID_ADAPT_SL_WIDEN_MAX", "1.35") or 1.35)
            tp_tight = float(os.getenv("MID_ADAPT_TP_TIGHTEN_MIN", "0.98") or 0.98)
            tp_wide = float(os.getenv("MID_ADAPT_TP_WIDEN_MAX", "1.25") or 1.25)
    
            if high <= low:
                t = 0.5
            else:
                t = (atrp - low) / (high - low)
            if t < 0.0:
                t = 0.0
            if t > 1.0:
                t = 1.0
    
            sl_scale = sl_tight + t * (sl_wide - sl_tight)
            tp_scale = tp_tight + t * (tp_wide - tp_tight)
    
            # Scale distances from entry while preserving direction
            r_sl = abs(entry - sl) * sl_scale
            r_tp1 = abs(tp1 - entry) * tp_scale
            r_tp2 = abs(tp2 - entry) * tp_scale
            if str(dir_trend).upper() == "LONG":
                sl = entry - r_sl
                tp1 = entry + r_tp1
                tp2 = entry + r_tp2
            else:
                sl = entry + r_sl
                tp1 = entry - r_tp1
                tp2 = entry - r_tp2
            rr = abs(tp2 - entry) / max(1e-12, abs(entry - sl))
    except Exception:
        pass
    # Hard cap: keep TP2 within MID_MAX_TP2_ATR * ATR to improve hit-rate
    try:
        max_tp2_atr = float(_mid_autotune_get_param("MID_MAX_TP2_ATR", float(os.getenv("MID_MAX_TP2_ATR", "2.2") or 2.2), market))
        if max_tp2_atr > 0 and atr30 and tp2:
            max_dist = abs(float(atr30)) * float(max_tp2_atr)
            cur_dist = abs(float(tp2) - float(entry))
            if max_dist > 0 and cur_dist > max_dist:
                tp2 = float(entry) + (max_dist if dir_trend == "LONG" else -max_dist)
                # recompute rr with adjusted tp2
                rr = abs(float(tp2) - float(entry)) / max(1e-12, abs(float(entry) - float(sl)))
    except Exception:
        pass

    # --- TA extras (MAIN-like) ---
    # RSI/MACD on 5m
    rsi5 = float(last5.get("rsi", np.nan))
    macd_hist5 = float(last5.get("macd_hist", np.nan))

    # Bollinger Bands on 5m (20)
    bb_low = bb_mid = bb_high = float("nan")
    bb_pos = "—"
    try:
        cl = df5i["close"].astype(float)
        bb_mid = float(cl.rolling(20).mean().iloc[-1])
        bb_std = float(cl.rolling(20).std(ddof=0).iloc[-1])
        bb_low = bb_mid - 2.0 * bb_std
        bb_high = bb_mid + 2.0 * bb_std
        bb_pos = _bb_position(entry, bb_low, bb_mid, bb_high)
    except Exception:
        pass
    bb_str = bb_pos

    # Relative volume on 5m
    vol_rel = float("nan")
    try:
        vol = float(df5i.iloc[-1].get("volume", np.nan))
        vol_sma = float(df5i["volume"].astype(float).rolling(20).mean().iloc[-1])
        vol_rel = (vol / vol_sma) if (vol_sma and not np.isnan(vol_sma) and vol_sma > 0) else np.nan
    except Exception:
        vol_rel = float("nan")

    # VWAP on 30m (typical price * vol)
    vwap_val = float("nan")
    vwap_txt = "—"
    try:
        d = df30i.tail(80).copy()
        vol = d["volume"].astype(float)
        tp = (d["high"].astype(float) + d["low"].astype(float) + d["close"].astype(float)) / 3.0
        denom = float(vol.sum())
        if denom > 0:
            vwap_val = float((tp * vol).sum() / denom)
            if entry >= vwap_val:
                vwap_txt = f"{vwap_val:.6g} (above)"
            else:
                vwap_txt = f"{vwap_val:.6g} (below)"
    except Exception:
        pass

    # ------------------ MID hard filters (reduce SL hits) ------------------
    # NOTE: We keep the thresholds centralized in globals (MID_*), and produce
    # a human-readable reason for logs / error-bot digest.
    try:
        late_lookback = int(float(os.getenv("MID_LATE_ENTRY_LOOKBACK", "48") or 48))
        if late_lookback < 10:
            late_lookback = 48

        w = df5i.tail(late_lookback) if len(df5i) >= late_lookback else df5i
        recent_low = float(w["low"].astype(float).min())
        recent_high = float(w["high"].astype(float).max())

        o5 = float(last5.get("open", np.nan))
        c5 = float(last5.get("close", np.nan))
        last_body = abs(c5 - o5) if (not np.isnan(o5) and not np.isnan(c5)) else 0.0

        last_vol = float(last5.get("volume", np.nan))
        avg_vol = float(df5i["volume"].astype(float).rolling(20).mean().iloc[-1]) if "volume" in df5i.columns and len(df5i) >= 20 else float("nan")

        # Count "climax" bars in the last MID_CLIMAX_COOLDOWN_BARS bars (excluding the current one)
        climax_recent_bars = 0
        try:
            n = int(MID_CLIMAX_COOLDOWN_BARS) if isinstance(MID_CLIMAX_COOLDOWN_BARS, int) else int(float(MID_CLIMAX_COOLDOWN_BARS))
            n = max(0, n)
            if n > 0 and atr30 > 0 and "volume" in df5i.columns:
                tail = df5i.tail(n + 1).iloc[:-1]  # previous N bars
                if not tail.empty:
                    v_sma = tail["volume"].astype(float).rolling(20).mean()
                    for i in range(len(tail)):
                        vv = float(tail.iloc[i].get("volume", 0.0))
                        av = float(v_sma.iloc[i]) if (i < len(v_sma) and not np.isnan(v_sma.iloc[i])) else float("nan")
                        oo = float(tail.iloc[i].get("open", np.nan))
                        cc = float(tail.iloc[i].get("close", np.nan))
                        if np.isnan(oo) or np.isnan(cc) or np.isnan(av) or av <= 0:
                            continue
                        vol_x = vv / av
                        body_atr = abs(cc - oo) / atr30 if atr30 > 0 else 0.0
                        if vol_x > MID_CLIMAX_VOL_X and body_atr > MID_CLIMAX_BODY_ATR:
                            climax_recent_bars += 1
        except Exception:
            climax_recent_bars = 0

        # BB position (5m) for entry-bounce filter (used only for blocking, not required for output)
        bb_pos_for_block = "—"
        try:
            cl = df5i["close"].astype(float)
            bb_mid0 = float(cl.rolling(20).mean().iloc[-1])
            bb_std0 = float(cl.rolling(20).std(ddof=0).iloc[-1])
            bb_low0 = bb_mid0 - 2.0 * bb_std0
            bb_high0 = bb_mid0 + 2.0 * bb_std0
            bb_pos_for_block = _bb_position(float(entry), float(bb_low0), float(bb_mid0), float(bb_high0))
        except Exception:
            bb_pos_for_block = "—"

        
        # --- 5m structure signals (used only for MID anti-countertrend blocking) ---
        ema20_5m = None
        bos_down_5m = False
        bos_up_5m = False
        two_red_5m = False
        two_green_5m = False
        lower_highs_5m = False
        higher_lows_5m = False
        try:
            op = df5i["open"].astype(float)
            hi = df5i["high"].astype(float)
            lo = df5i["low"].astype(float)
            cl = df5i["close"].astype(float)

            # EMA20 on 5m
            if len(cl) >= 25:
                ema20_5m = float(cl.ewm(span=20, adjust=False).mean().iloc[-1])

            # Two red candles in a row (close < open)
            if len(cl) >= 2 and len(op) >= 2:
                two_red_5m = (float(cl.iloc[-1]) < float(op.iloc[-1])) and (float(cl.iloc[-2]) < float(op.iloc[-2]))
                two_green_5m = (float(cl.iloc[-1]) > float(op.iloc[-1])) and (float(cl.iloc[-2]) > float(op.iloc[-2]))

            # BOS heuristic: consider breaks within last MID_BOS_RECENT_BARS bars (more robust than last-bar only)
            lb = 20
            k = max(2, int(MID_BOS_RECENT_BARS))
            if len(lo) >= lb + k + 1:
                # previous extremes excluding the last k bars
                prev_low = float(lo.iloc[-(lb+k+1):-(k)].min())
                prev_high = float(hi.iloc[-(lb+k+1):-(k)].max())
                bos_down_5m = False
                bos_up_5m = False
                # scan last k bars for a decisive close beyond the previous extreme
                for j in range(-k, 0):
                    try:
                        _low = float(lo.iloc[j])
                        _high = float(hi.iloc[j])
                        _close = float(cl.iloc[j])
                        if (_low < prev_low) and (_close < prev_low):
                            bos_down_5m = True
                        if (_high > prev_high) and (_close > prev_high):
                            bos_up_5m = True
                    except Exception:
                        continue
            
            # Lower highs / higher lows with small tolerance to avoid false negatives on equal-ish pivots
            tol = max(0.0, float(MID_HL_TOL_PCT))
            nlh = max(3, int(MID_LOWER_HIGHS_LOOKBACK))
            if len(hi) >= nlh:
                hh = [float(x) for x in hi.iloc[-nlh:].tolist()]
                lower_highs_5m = all(hh[i] <= hh[i-1] * (1.0 + tol) for i in range(1, len(hh)))
            nhl = max(3, int(MID_HIGHER_LOWS_LOOKBACK))
            if len(lo) >= nhl:
                ll = [float(x) for x in lo.iloc[-nhl:].tolist()]
                higher_lows_5m = all(ll[i] >= ll[i-1] * (1.0 - tol) for i in range(1, len(ll)))
        except Exception:
            pass

        reason = _mid_block_reason(
            symbol=symbol,
            side=str(dir_trend),
            close=float(entry),
            o=o5 if not np.isnan(o5) else float(entry),
            recent_low=recent_low,
            recent_high=recent_high,
            atr_30m=float(atr30),
            rsi_5m=float(last5.get("rsi", np.nan)),
            vwap=float(vwap_val) if not np.isnan(vwap_val) else float(entry),
            bb_pos=bb_pos_for_block,
            ema20_5m=ema20_5m,
            bos_down_5m=bool(bos_down_5m),
            two_red_5m=bool(two_red_5m),
            lower_highs_5m=bool(lower_highs_5m),
            bos_up_5m=bool(bos_up_5m),
            two_green_5m=bool(two_green_5m),
            higher_lows_5m=bool(higher_lows_5m),
            last_vol=last_vol if not np.isnan(last_vol) else 0.0,
            avg_vol=avg_vol if not np.isnan(avg_vol) else 0.0,
            last_body=float(last_body),
            climax_recent_bars=int(climax_recent_bars),
            htf_dir_1h=str(dir_trend),
            htf_dir_30m=str(dir_mid),
            adx_30m=float(adx30) if not np.isnan(adx30) else None,
        )
        if reason:
            try:
                _mid_inc_hard_block(1)
                _mid_hardblock_track(str(reason), symbol)
                _k = f"block|{symbol}|{dir_trend}|{_mid_trap_reason_key(str(reason))}"
                _ctx = _MID_TICK_CTX.get()
                _tick_key = f"{symbol}|{dir_trend}|block"
                if _ctx is not None and _tick_key in _ctx.get('block', set()):
                    _ctx['suppress_block'] = int(_ctx.get('suppress_block', 0)) + 1
                else:
                    if _ctx is not None:
                        _ctx.setdefault('block', set()).add(_tick_key)
                    if _mid_trap_should_log(_k):
                        logger.info("[mid][block] %s dir=%s reason=%s entry=%.6g", symbol, dir_trend, reason, float(entry))
                # Reuse MID trap sink/digest to show why MID hard-filters blocked entries
                _emit_mid_trap_event({
                    "dir": str(dir_trend),
                    # Keep the raw reason so the digest can bucket by the *actual* filter
                    # (anti_bounce_short / rsi_short / bb_bounce_zone / etc.)
                    "reason": str(reason),
                    "reason_key": _mid_trap_reason_key(str(reason)),
                    "entry": float(entry),
                })
            except Exception:
                pass
            # IMPORTANT: surface the concrete block reason in MID no-signal summary.
            # Without this, the caller sees stage==other but reason missing -> other{unknown=...}.
            try:
                _fail("other", str(reason))
            except Exception:
                pass
            return None
    except Exception:
        # If filters fail, do not block signal.
        pass

# Pattern on 5m
    pattern, pat_bias = _candle_pattern(df5i)

    # RSI divergence on 30m (more stable than 5m)
    rsi_div = _rsi_divergence(df30i)

    # Channel + structure on 1h
    channel, _, _ = _linreg_channel(df1hi)
    mstruct_raw = _market_structure(df1hi)
    if mstruct_raw in ("HH-HL", "LH-LL"):
        mstruct = "TREND"
    elif mstruct_raw == "RANGE":
        mstruct = "RANGE"
    else:
        mstruct = "—"

    support, resistance = _nearest_levels(df1hi)

    # Breakout / Retest detection (simple but robust)
    # We use 1h swing S/R and confirm on recent 5m candles.
    breakout_retest = "—"
    try:
        sup_lvl = float(support) if support is not None else float("nan")
        res_lvl = float(resistance) if resistance is not None else float("nan")
        try:
            tol = max(float(atr5) * 0.25, float(entry) * 0.0015)  # ~0.15% or 0.25 ATR(5m)
        except Exception:
            tol = float(entry) * 0.0015

        d5 = df5i.tail(36).copy()  # last ~3h
        if (not d5.empty) and len(d5) >= 10:
            highs5 = d5["high"].astype(float).values
            lows5 = d5["low"].astype(float).values
            closes5 = d5["close"].astype(float).values

            # Breakout up: close above resistance after being below/at it
            bo_up_idx = None
            if (not np.isnan(res_lvl)) and res_lvl > 0:
                for i in range(1, len(closes5)):
                    if closes5[i - 1] <= res_lvl and closes5[i] > res_lvl:
                        bo_up_idx = i
                        break

            # Breakout down: close below support after being above/at it
            bo_dn_idx = None
            if (not np.isnan(sup_lvl)) and sup_lvl > 0:
                for i in range(1, len(closes5)):
                    if closes5[i - 1] >= sup_lvl and closes5[i] < sup_lvl:
                        bo_dn_idx = i
                        break

            # Retest logic (after breakout)
            ret_up = False
            if bo_up_idx is not None:
                for j in range(bo_up_idx + 1, len(closes5)):
                    if (lows5[j] <= res_lvl + tol) and (closes5[j] > res_lvl):
                        ret_up = True
                        break

            ret_dn = False
            if bo_dn_idx is not None:
                for j in range(bo_dn_idx + 1, len(closes5)):
                    if (highs5[j] >= sup_lvl - tol) and (closes5[j] < sup_lvl):
                        ret_dn = True
                        break

            # Prefer breakout in the same direction as the MID signal
            if str(dir_trend).upper() == "LONG":
                if bo_up_idx is not None and ret_up:
                    breakout_retest = "BO↑ + Retest"
                elif bo_up_idx is not None:
                    breakout_retest = "BO↑"
                elif bo_dn_idx is not None and ret_dn:
                    breakout_retest = "BO↓ + Retest"
                elif bo_dn_idx is not None:
                    breakout_retest = "BO↓"
            else:
                if bo_dn_idx is not None and ret_dn:
                    breakout_retest = "BO↓ + Retest"
                elif bo_dn_idx is not None:
                    breakout_retest = "BO↓"
                elif bo_up_idx is not None and ret_up:
                    breakout_retest = "BO↑ + Retest"
                elif bo_up_idx is not None:
                    breakout_retest = "BO↑"
    except Exception:
        breakout_retest = "—"

    # Range position filter (only when market structure is RANGE)
    range_pos = None
    range_pos_txt = "—"
    try:
        sup = float(support) if support is not None else float("nan")
        res = float(resistance) if resistance is not None else float("nan")
        if (not np.isnan(sup)) and (not np.isnan(res)) and res > sup:
            range_pos = (float(entry) - sup) / (res - sup)
            # keep in [0..1] for safety
            if range_pos < 0:
                range_pos = 0.0
            if range_pos > 1:
                range_pos = 1.0
            range_pos_txt = f"{range_pos:.2f}"
    except Exception:
        range_pos = None
        range_pos_txt = "—"

    try:
        range_pos_filter = os.getenv("MID_RANGE_POS_FILTER", "0").strip().lower() not in ("0","false","no","off")
        long_max = float(os.getenv("MID_RANGE_POS_LONG_MAX", "0.75") or 0.75)
        short_min = float(os.getenv("MID_RANGE_POS_SHORT_MIN", "0.25") or 0.25)
        if range_pos_filter and (mstruct == "RANGE") and (range_pos is not None):
            if (dir_trend == "LONG") and (range_pos > long_max):
                _fail("structure", "range_pos_long")
                return None
            if (dir_trend == "SHORT") and (range_pos < short_min):
                _fail("structure", "range_pos_short")
                return None
    except Exception:
        pass


    # Score / confidence: use unified TA score to avoid constant 100s
    ta_score = _ta_score(
        direction=dir_trend,
        adx1=adx30,
        adx4=adx1h,
        rsi15=rsi5,
        macd_hist15=macd_hist5,
        vol_rel=vol_rel,
        bb_pos=bb_pos,
        rsi_div=rsi_div,
        channel=channel,
        mstruct=mstruct,
        pat_bias=pat_bias,
        atr_pct=atr_pct,
)
    confidence = ta_score

    ta: Dict[str, Any] = {
        "direction": dir_trend,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "rr": rr,
        "confidence": confidence,

        # fields used by MID filters/formatter
        "dir1": dir_mid,
        "dir4": dir_trend,
        "adx1": adx30,
        "adx4": adx1h,
        "atr_pct": atr_pct,

        "atr5": atr5,
        "atr5_pct": atr5_pct,
        # MID TA block fields
        "rsi": rsi5,
        "macd_hist": macd_hist5,
        "bb": bb_str,
        "rel_vol": vol_rel if (not np.isnan(vol_rel)) else 0.0,
        "vwap": vwap_txt,
        "vwap_val": vwap_val if (not np.isnan(vwap_val)) else 0.0,
        "pattern": pattern,
        "support": support,
        "resistance": resistance,
        "breakout_retest": breakout_retest,
        "regime": mid_regime,
        "structure_hhhl_1h": struct_hhhl_1h,
        "sweep": sweep_txt,
        "bo_rt": bo_rt_label,
        "ob_zone": ob_zone,
        "ob_retest": bool(ob_retest),
        "channel": channel,
        "mstruct": mstruct,
        "range_pos": range_pos_txt,
        "range_pos_val": (range_pos if range_pos is not None else 0.0),
        "rsi_div": rsi_div,
        "ta_score": ta_score,
    }
    ta["ta_block"] = _fmt_ta_block_mid(ta)
    return ta
def choose_market(adx1_max: float, atr_pct_max: float) -> str:
    return "FUTURES" if (not np.isnan(adx1_max)) and adx1_max >= 28 and atr_pct_max >= 0.8 else "SPOT"

# ------------------ Backend ------------------

    async def orderbook_binance_futures(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """Binance USDT-M futures orderbook."""
        assert self.session is not None
        url = f"{self.BINANCE_FUTURES}/fapi/v1/depth"
        params = {"symbol": symbol, "limit": str(limit)}
        return await self._get_json(url, params=params)

    async def orderbook_bybit_linear(self, symbol: str, limit: int = 50) -> Dict[str, Any]:
        """Bybit linear futures orderbook."""
        assert self.session is not None
        url = f"{self.BYBIT}/v5/market/orderbook"
        params = {"category": "linear", "symbol": symbol, "limit": str(limit)}
        return await self._get_json(url, params=params)


# ------------------ Orderbook filter helpers ------------------
def _orderbook_metrics(bids: List[List[Any]], asks: List[List[Any]], *, levels: int) -> Dict[str, float]:
    """Return imbalance (bids/asks notional), spread_pct, bid_wall_ratio, ask_wall_ratio, bid_wall_near, ask_wall_near."""
    def _take(side: List[List[Any]]) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        for row in side[:levels]:
            try:
                p = float(row[0])
                q = float(row[1])
                out.append((p, q))
            except Exception:
                continue
        return out

    b = _take(bids)
    a = _take(asks)
    if not b or not a:
        return {"imbalance": 1.0, "spread_pct": 999.0, "bid_wall_ratio": 0.0, "ask_wall_ratio": 0.0, "mid": 0.0}

    best_bid = b[0][0]
    best_ask = a[0][0]
    mid = (best_bid + best_ask) / 2.0 if (best_bid > 0 and best_ask > 0) else max(best_bid, best_ask)
    spread_pct = ((best_ask - best_bid) / mid * 100.0) if mid > 0 else 999.0

    bid_not = [p * q for p, q in b]
    ask_not = [p * q for p, q in a]
    sum_b = float(sum(bid_not))
    sum_a = float(sum(ask_not))
    imbalance = (sum_b / sum_a) if sum_a > 0 else 999.0

    bid_avg = (sum_b / len(bid_not)) if bid_not else 0.0
    ask_avg = (sum_a / len(ask_not)) if ask_not else 0.0
    bid_wall_ratio = (max(bid_not) / bid_avg) if bid_avg > 0 else 0.0
    ask_wall_ratio = (max(ask_not) / ask_avg) if ask_avg > 0 else 0.0

    return {
        "imbalance": float(imbalance),
        "spread_pct": float(spread_pct),
        "bid_wall_ratio": float(bid_wall_ratio),
        "ask_wall_ratio": float(ask_wall_ratio),
        "mid": float(mid),
    }

def _be_is_armed(*, side: str, price: float, tp1: float | None, tp2: float | None) -> bool:
    """If BE_ARM_PCT_TO_TP2>0, arm BE only after price moves from TP1 towards TP2 by that fraction."""
    try:
        pct = float(BE_ARM_PCT_TO_TP2)
    except Exception:
        pct = 0.0
    if pct <= 0:
        return True
    try:
        tp1f = float(tp1 or 0.0)
        tp2f = float(tp2 or 0.0)
        px = float(price)
        if tp1f <= 0 or tp2f <= 0 or abs(tp2f - tp1f) < 1e-12:
            return True
        arm_lvl = tp1f + pct * (tp2f - tp1f)  # works for LONG and SHORT (tp2<tp1)
        s = (side or "LONG").upper()
        return (px >= arm_lvl) if s == "LONG" else (px <= arm_lvl)
    except Exception:
        return True
def _build_levels(direction: str, entry: float, atr: float, *, tp2_r: float | None = None) -> Tuple[float, float, float, float]:
    R = max(1e-9, ATR_MULT_SL * atr)
    if direction == "LONG":
        sl = entry - R
        tp1 = entry + TP1_R * R
        tp2 = entry + (float(tp2_r) if tp2_r is not None else TP2_R) * R
    else:
        sl = entry + R
        tp1 = entry - TP1_R * R
        tp2 = entry - (float(tp2_r) if tp2_r is not None else TP2_R) * R
    rr = abs(tp2 - entry) / abs(entry - sl)
    return sl, tp1, tp2, rr

def _candle_pattern(df: pd.DataFrame) -> Tuple[str, int]:
    """
    Very lightweight candlestick pattern detection on the last 2 candles.
    Returns (pattern_name, bias) where bias: +1 bullish, -1 bearish, 0 neutral.
    """
    try:
        if df is None or len(df) < 3:
            return ("—", 0)
        c1 = df.iloc[-2]
        c2 = df.iloc[-1]
        o1,h1,l1,cl1 = float(c1["open"]), float(c1["high"]), float(c1["low"]), float(c1["close"])
        o2,h2,l2,cl2 = float(c2["open"]), float(c2["high"]), float(c2["low"]), float(c2["close"])

        body1 = abs(cl1 - o1)
        body2 = abs(cl2 - o2)
        rng2 = max(1e-12, h2 - l2)

        # Doji
        if body2 <= rng2 * 0.1:
            return ("Doji", 0)

        # Engulfing
        bull_engulf = (cl1 < o1) and (cl2 > o2) and (o2 <= cl1) and (cl2 >= o1)
        bear_engulf = (cl1 > o1) and (cl2 < o2) and (o2 >= cl1) and (cl2 <= o1)
        if bull_engulf:
            return ("Bullish Engulfing", +1)
        if bear_engulf:
            return ("Bearish Engulfing", -1)

        # Hammer / Shooting Star (simple)
        upper_w = h2 - max(o2, cl2)
        lower_w = min(o2, cl2) - l2
        if body2 <= rng2 * 0.35:
            if lower_w >= body2 * 1.8 and upper_w <= body2 * 0.7:
                return ("Hammer", +1)
            if upper_w >= body2 * 1.8 and lower_w <= body2 * 0.7:
                return ("Shooting Star", -1)

        # Inside bar
        if (h2 <= h1) and (l2 >= l1):
            return ("Inside Bar", 0)

        return ("—", 0)
    except Exception:
        return ("—", 0)


def _nearest_levels(df: pd.DataFrame, lookback: int = 180, swing: int = 3) -> Tuple[Optional[float], Optional[float]]:
    """
    Finds nearest swing support/resistance levels using simple local extrema on 'high'/'low'.
    Returns (support, resistance) relative to the last close.
    """
    try:
        if df is None or df.empty or len(df) < (swing * 2 + 5):
            return (None, None)
        d = df.tail(max(lookback, swing * 2 + 5)).copy()
        last_close = float(d.iloc[-1]["close"])

        highs = d["high"].astype(float).values
        lows = d["low"].astype(float).values

        swing_highs = []
        swing_lows = []
        n = len(d)

        for i in range(swing, n - swing):
            h = highs[i]
            l = lows[i]
            if h == max(highs[i - swing:i + swing + 1]):
                swing_highs.append(h)
            if l == min(lows[i - swing:i + swing + 1]):
                swing_lows.append(l)

        # nearest below and above price
        support = None
        resistance = None
        below = [x for x in swing_lows if x <= last_close]
        above = [x for x in swing_highs if x >= last_close]
        if below:
            support = max(below)
        if above:
            resistance = min(above)

        return (support, resistance)
    except Exception:
        return (None, None)


def _pivot_points(series: np.ndarray, *, left: int = 3, right: int = 3, mode: str = "high") -> List[Tuple[int, float]]:
    """Return pivot highs/lows as (index, value) using simple local extrema."""
    pts: List[Tuple[int, float]] = []
    n = len(series)
    if n < left + right + 3:
        return pts
    for i in range(left, n - right):
        window = series[i - left:i + right + 1]
        v = series[i]
        if mode == "high":
            if v == np.max(window):
                pts.append((i, float(v)))
        else:
            if v == np.min(window):
                pts.append((i, float(v)))
    return pts


def _rsi_divergence(df: pd.DataFrame, *, lookback: int = 120, pivot: int = 3) -> str:
    """Detect basic bullish/bearish RSI divergence on recent pivots. Returns 'BULL', 'BEAR' or '—'."""
    try:
        if df is None or df.empty or len(df) < 40:
            return "—"
        d = df.tail(lookback).copy()
        if "rsi" not in d.columns:
            return "—"
        closes = d["close"].astype(float).values
        rsi = d["rsi"].astype(float).values
        # pivots on price
        ph = _pivot_points(closes, left=pivot, right=pivot, mode="high")
        pl = _pivot_points(closes, left=pivot, right=pivot, mode="low")
        # need last two pivots of each kind
        if len(ph) >= 2:
            (i1, p1), (i2, p2) = ph[-2], ph[-1]
            if i1 < i2 and p2 > p1 and (rsi[i2] < rsi[i1]):
                return "BEAR"
        if len(pl) >= 2:
            (i1, p1), (i2, p2) = pl[-2], pl[-1]
            if i1 < i2 and p2 < p1 and (rsi[i2] > rsi[i1]):
                return "BULL"
        return "—"
    except Exception:
        return "—"


def _linreg_channel(df: pd.DataFrame, *, window: int = 120, k: float = 2.0) -> Tuple[str, float, float]:
    """Linear regression channel on close. Returns (label, pos, slope). label like 'desc@upper'."""
    try:
        if df is None or df.empty or len(df) < max(60, window // 2):
            return ("—", float("nan"), float("nan"))
        d = df.tail(window).copy()
        y = d["close"].astype(float).values
        x = np.arange(len(y), dtype=float)
        # y = a*x + b
        a, b = np.polyfit(x, y, 1)
        y_hat = a * x + b
        resid = y - y_hat
        sd = float(np.std(resid))
        last = float(y[-1])
        mid = float(y_hat[-1])
        upper = mid + k * sd
        lower = mid - k * sd
        width = max(1e-12, upper - lower)
        pos = (last - lower) / width  # 0..1 typically
        # trend label
        if abs(a) < 1e-6:
            trend = "flat"
        else:
            trend = "asc" if a > 0 else "desc"
        if pos >= 0.8:
            where = "upper"
        elif pos <= 0.2:
            where = "lower"
        else:
            where = "mid"
        return (f"{trend}@{where}", pos, float(a))
    except Exception:
        return ("—", float("nan"), float("nan"))


def _market_structure(df: pd.DataFrame, *, lookback: int = 180, swing: int = 3) -> str:
    """Minimal market-structure label from swing highs/lows: 'HH-HL', 'LH-LL', or '—'."""
    try:
        if df is None or df.empty or len(df) < (swing * 2 + 10):
            return "—"
        d = df.tail(lookback).copy()
        highs = d["high"].astype(float).values
        lows = d["low"].astype(float).values
        sh = _pivot_points(highs, left=swing, right=swing, mode="high")
        sl = _pivot_points(lows, left=swing, right=swing, mode="low")
        if len(sh) < 2 or len(sl) < 2:
            return "—"
        (_, h1), (_, h2) = sh[-2], sh[-1]
        (_, l1), (_, l2) = sl[-2], sl[-1]
        hh = h2 > h1
        hl = l2 > l1
        lh = h2 < h1
        ll = l2 < l1
        if hh and hl:
            return "HH-HL"
        if lh and ll:
            return "LH-LL"
        # mixed / range
        return "RANGE"
    except Exception:
        return "—"


def _bb_position(close: float, bb_low: float, bb_mid: float, bb_high: float) -> str:
    try:
        if any(np.isnan(x) for x in [close, bb_low, bb_mid, bb_high]):
            return "—"
        if close >= bb_high:
            return "↑high"
        if close <= bb_low:
            return "↓low"
        if close >= bb_mid:
            return "mid→high"
        return "low→mid"
    except Exception:
        return "—"


def _ta_score(*,
              direction: str,
              adx1: float,
              adx4: float,
              rsi15: float,
              macd_hist15: float,
              vol_rel: float,
              bb_pos: str,
              rsi_div: str,
              channel: str,
              mstruct: str,
              pat_bias: int,
              atr_pct: float) -> int:
    """Compute 0..100 TA score (used as 'confidence' too).

    This is shared by MAIN and MID. We intentionally avoid saturating to 100 too easily:
    the raw additive score is passed through a smooth squashing function.
    """
    score = 50.0

    # Trend strength (multi-timeframe)
    if not np.isnan(adx4):
        score += min(22.0, max(0.0, (adx4 - 15.0) * 1.1))
    if not np.isnan(adx1):
        score += min(18.0, max(0.0, (adx1 - 15.0) * 0.9))

    # RSI sanity (prefer balanced momentum for entries)
    if not np.isnan(rsi15):
        if 42.0 <= rsi15 <= 58.0:
            score += 10.0
        elif 38.0 <= rsi15 <= 62.0:
            score += 6.0
        else:
            score -= 3.0

    # MACD histogram bias (small effect, do not dominate)
    if not np.isnan(macd_hist15):
        if direction == "LONG" and macd_hist15 > 0:
            score += 4.0
        elif direction == "SHORT" and macd_hist15 < 0:
            score += 4.0
        else:
            score -= 2.0

    # Relative volume (if missing/zero, apply a small penalty)
    if vol_rel is None or (isinstance(vol_rel, float) and np.isnan(vol_rel)) or vol_rel <= 0:
        score -= 5.0
    else:
        if vol_rel >= 1.6:
            score += 6.0
        elif vol_rel >= 1.2:
            score += 3.0

    # Bollinger position: being at extremes is better for mean-reversion entries,
    # but for trend-following we just treat it as a mild confirmation.
    if bb_pos in ("↑high", "↓low"):
        score += 3.0
    elif bb_pos in ("mid→high", "low→mid"):
        score += 1.0

    # Divergence / channel / structure
    if rsi_div and rsi_div != "—":
        if (direction == "LONG" and "bull" in str(rsi_div).lower()) or (direction == "SHORT" and "bear" in str(rsi_div).lower()):
            score += 4.0
        else:
            score -= 2.0

    if channel and channel != "—":
        score += 2.0

    if mstruct == "TREND":
        score += 4.0
    elif mstruct == "RANGE":
        score -= 4.0

    # Pattern bias
    if (direction == "LONG" and pat_bias > 0) or (direction == "SHORT" and pat_bias < 0):
        score += 4.0
    elif pat_bias == 0:
        score -= 2.0

    # ATR sanity (too wild reduces score)
    if not np.isnan(atr_pct):
        if atr_pct > 6.0:
            score -= 8.0
        elif atr_pct > 4.0:
            score -= 4.0
        elif atr_pct < 0.15:
            score -= 4.0

    # Smooth squashing so 100/100 is rare.
    raw = score
    score = 50.0 + 50.0 * math.tanh((raw - 50.0) / 35.0)

    return int(max(0, min(100, round(score))))
def _fmt_ta_block(ta: Dict[str, Any]) -> str:
    """
    Format TA snapshot for Telegram/UX. Keep it short but informative.
    """
    try:
        if not ta:
            return ""
        score = ta.get("score", "—")
        rsi = ta.get("rsi15", "—")
        macd_h = ta.get("macd_hist15", "—")
        adx1 = ta.get("adx1", "—")
        adx4 = ta.get("adx4", "—")
        atrp = ta.get("atr_pct", "—")
        bb = ta.get("bb_pos", "—")
        volr = ta.get("vol_rel", "—")
        vwap = ta.get("vwap_bias", "—")
        pat = ta.get("pattern", "—")
        div = ta.get("rsi_div", "—")
        ch = ta.get("channel", "—")
        ms = ta.get("mstruct", "—")
        sup = ta.get("support", None)
        res = ta.get("resistance", None)

        def fnum(x, fmt="{:.2f}"):
            try:
                if x is None or (isinstance(x, float) and np.isnan(x)):
                    return "—"
                return fmt.format(float(x))
            except Exception:
                return "—"

        def fint(x):
            try:
                if x is None or (isinstance(x, float) and np.isnan(x)):
                    return "—"
                return str(int(round(float(x))))
            except Exception:
                return "—"

        lines = [
            f"📊 TA score: {fint(score)}/100 | Mode: {SIGNAL_MODE}",
            f"RSI: {fnum(rsi, '{:.1f}')} | MACD hist: {fnum(macd_h, '{:.4f}')}",
            f"ADX 1h/4h: {fnum(adx1, '{:.1f}')}/{fnum(adx4, '{:.1f}')} | ATR%: {fnum(atrp, '{:.2f}')}",
            f"BB: {bb} | Vol xAvg: {fnum(volr, '{:.2f}')} | VWAP: {vwap}",
        ]
        trap_ok = bool(ta.get("trap_ok", True))
        trap_reason = str(ta.get("trap_reason", "") or "")
        trap_line = "🧱 Trap: OK" if trap_ok else ("🧱 Trap: BLOCKED (" + trap_reason + ")" if trap_reason else "🧱 Trap: BLOCKED")
        lines.append(trap_line)
        # extra context (keep concise)
        ctx = []
        if div and div != "—":
            ctx.append(f"Div: {div}")
        if ch and ch != "—":
            ctx.append(f"Ch: {ch}")
        if ms and ms != "—":
            ctx.append(f"PA: {ms}")
        if ctx:
            lines.append(" | ".join(ctx))
        extra = []
        if pat and pat != "—":
            extra.append(f"Pattern: {pat}")
        if sup is not None:
            extra.append(f"Support: {fnum(sup, '{:.6g}')}")
        if res is not None:
            extra.append(f"Resistance: {fnum(res, '{:.6g}')}")
        if extra:
            lines.append(" | ".join(extra))
        return "\n".join(lines).strip()
    except Exception:
        return ""

def _fmt_ta_block_mid(ta: Dict[str, Any], mode: str = "") -> str:
    """TA block for ⚡ MID scanner (5m/30m/1h)."""
    try:
        if not ta:
            return ""
        score = ta.get("confidence", ta.get("ta_score", 0))
        mode_txt = (mode or "").strip() or os.getenv("MID_SIGNAL_MODE", "") or os.getenv("SIGNAL_MODE", "")
        mode_txt = (mode_txt or "strict").strip().lower()
        rsi = float(ta.get("rsi", 0.0) or 0.0)
        macd_hist = float(ta.get("macd_hist", 0.0) or 0.0)
        adx_30m = float(ta.get("adx1", 0.0) or 0.0)
        adx_1h = float(ta.get("adx4", 0.0) or 0.0)
        atr_pct = float(ta.get("atr_pct", 0.0) or 0.0)
        bb = ta.get("bb", "—")
        volx = float(ta.get("rel_vol", 0.0) or 0.0)
        vwap = ta.get("vwap", "—")
        ch = ta.get("channel", "—")
        pa = ta.get("mstruct", ta.get("structure", "—"))
        pattern = ta.get("pattern", "—")
        sup = ta.get("support", "—")
        res = ta.get("resistance", "—")
        br = ta.get("breakout_retest", "—")
        eq_hi = ta.get("eq_hi", None)
        eq_lo = ta.get("eq_lo", None)
        sweep = ta.get("sweep", "")
        obtxt = "YES" if bool(ta.get("ob_retest", False)) else "—"
        lines = [
            f"📊 TA score: {int(round(float(score))):d}/100 | Mode: {mode_txt}",
            f"RSI(5m): {rsi:.1f} | MACD hist(5m): {macd_hist:.4f}",
            f"ADX 30m/1h: {adx_30m:.1f}/{adx_1h:.1f} | ATR% (30m): {atr_pct:.2f}",
            f"BB: {bb} | Vol xAvg: {volx:.2f} | VWAP: {vwap}",
            f"Ch: {ch} | PA: {pa} | Regime: {ta.get('regime', '—')}",
            f"Liquidity: EQH: {(_fmt_int_space(eq_hi) if eq_hi is not None else '—')} | EQL: {(_fmt_int_space(eq_lo) if eq_lo is not None else '—')} | Sweep(5m): {sweep or '—'}",
            f"Breakout/Retest: {br} | OB retest: {obtxt}",
            f"Pattern: {pattern} | Support: {sup} | Resistance: {res}",
        ]
        trap_ok = bool(ta.get("trap_ok", True))
        trap_reason = str(ta.get("trap_reason", "") or "")
        trap_line = "🧱 Trap: OK" if trap_ok else ("🧱 Trap: BLOCKED (" + trap_reason + ")" if trap_reason else "🧱 Trap: BLOCKED")
        # Put right after VWAP line
        try:
            lines.insert(4, trap_line)
        except Exception:
            lines.append(trap_line)
        return "\n".join(lines)
    except Exception:
        return ""


def _confidence(adx4: float, adx1: float, rsi15: float, atr_pct: float) -> int:
    score = 0
    score += 0 if np.isnan(adx4) else int(min(25, max(0, (adx4 - 15) * 1.25)))
    score += 0 if np.isnan(adx1) else int(min(25, max(0, (adx1 - 15) * 1.25)))
    if not np.isnan(rsi15):
        score += 20 if 40 <= rsi15 <= 60 else (15 if 35 <= rsi15 <= 65 else 8)
    score += 20 if 0.3 <= atr_pct <= 3.0 else (14 if 0.2 <= atr_pct <= 4.0 else 8)
    score += 10
    return int(max(0, min(100, score)))

def evaluate_on_exchange(df15: pd.DataFrame, df1h: pd.DataFrame, df4h: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Compute direction + levels + TA snapshot for one exchange using 15m/1h/4h OHLCV."""
    if df15.empty or df1h.empty or df4h.empty:
        return None

    df15i = _add_indicators(df15)
    df1hi = _add_indicators(df1h)
    df4hi = _add_indicators(df4h)

    dir4 = _trend_dir(df4hi)
    dir1 = _trend_dir(df1hi)

    # Multi-timeframe confirmation (configurable)
    if dir4 is None:
        return None
    if TA_REQUIRE_1H_TREND:
        if dir1 is None or dir4 != dir1:
            return None
    else:
        if dir1 is None:
            dir1 = dir4

    last15 = df15i.iloc[-1]
    last1h = df1hi.iloc[-1]
    last4h = df4hi.iloc[-1]
    adx1 = float(last1h.get("adx", np.nan))

    entry = float(last15["close"])
    # --- Anti-trap structure filters (avoid tops/bottoms) ---
    trap_ok, trap_reason = _mid_structure_trap_ok(direction=str(dir4).upper(), entry=entry, df1hi=df1hi)
    if not bool(trap_ok):
        return None

    atr = float(last1h.get("atr", np.nan))
    if np.isnan(atr) or atr <= 0:
        return None

    atr_pct = (atr / entry) * 100.0 if entry > 0 else 0.0
    tp2_r = _adaptive_tp2_r(adx1)
    sl, tp1, tp2, rr = _build_levels(dir1, entry, atr, tp2_r=tp2_r)

    # Snapshot values
    rsi15 = float(last15.get("rsi", np.nan))
    macd_hist15 = float(last15.get("macd_hist", np.nan))
    adx4 = float(last4h.get("adx", np.nan))

    bb_low = float(last15.get("bb_low", np.nan))
    bb_mid = float(last15.get("bb_mavg", np.nan))
    bb_high = float(last15.get("bb_high", np.nan))

    vol = float(last15.get("volume", 0.0) or 0.0)
    vol_sma = float(last15.get("vol_sma20", np.nan))
    vol_rel = (vol / vol_sma) if (vol_sma and not np.isnan(vol_sma) and vol_sma > 0) else np.nan

    # Pattern + S/R
    pattern, pat_bias = _candle_pattern(df15i)
    support, resistance = _nearest_levels(df1hi, lookback=180, swing=3)

    # Divergence / channels / minimal market structure (algorithmic)
    rsi_div = _rsi_divergence(df15i, lookback=140, pivot=3)
    channel, ch_pos, ch_slope = _linreg_channel(df1hi, window=140, k=2.0)
    mstruct = _market_structure(df1hi, lookback=200, swing=3)
    bb_pos = _bb_position(entry, bb_low, bb_mid, bb_high)

    # Strict-ish trigger on 15m (uses ENV thresholds)
    if not _trigger_15m(dir1, df15i):
        return None

    score = _ta_score(
        direction=dir1,
        adx1=adx1,
        adx4=adx4,
        rsi15=rsi15,
        macd_hist15=macd_hist15,
        vol_rel=vol_rel,
        bb_pos=bb_pos,
        rsi_div=rsi_div,
        channel=channel,
        mstruct=mstruct,
        pat_bias=pat_bias,
        atr_pct=atr_pct,
    )
    # keep legacy name 'confidence' used by filters, but make it the same as TA score
    confidence = int(score)

    ta = {
        "direction": dir1,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "rr": rr,
        "score": score,
        "confidence": confidence,
        "trap_ok": bool(trap_ok),
        "trap_reason": str(trap_reason or ""),
        "blocked": (not bool(trap_ok)),
        "atr_pct": atr_pct,
        "adx1": adx1,
        "adx4": adx4,
        "rsi15": rsi15,
        "macd_hist15": macd_hist15,
        "bb_low": bb_low,
        "bb_mid": bb_mid,
        "bb_high": bb_high,
        "bb_pos": bb_pos,
        "vol_rel": vol_rel,
        "pattern": pattern,
        "rsi_div": rsi_div,
        "channel": channel,
        "mstruct": mstruct,
        "support": support,
        "resistance": resistance,
    }
    return ta

def evaluate_on_exchange_mid_v2(df5: pd.DataFrame, df30: pd.DataFrame, df1h: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """MID analysis: 5m (trigger) / 30m (mid) / 1h (trend).

    Produces a result dict compatible with scanner_loop_mid and a rich TA block (like MAIN),
    but tuned for MID timeframes.
    """
    if df5 is None or df30 is None or df1h is None:
        return None
    if df5.empty or df30.empty or df1h.empty:
        return None

    try:
        df5i = _add_indicators(df5)
        df30i = _add_indicators(df30)
        df1hi = _add_indicators(df1h)
    except Exception:
        return None

    # Directions (trend=1h, mid=30m)
    dir_trend = _trend_dir(df1hi)
    dir_mid = _trend_dir(df30i)
    if dir_trend is None:
        return None
    if dir_mid is None:
        dir_mid = dir_trend

    last5 = df5i.iloc[-1]
    last30 = df30i.iloc[-1]
    last1h = df1hi.iloc[-1]

    entry = float(last5.get("close", np.nan))
    if np.isnan(entry) or entry <= 0:
        return None

    # ATR from 30m (prefer indicator, fallback to true-range)
    atr30 = float(last30.get("atr", np.nan))
    if np.isnan(atr30) or atr30 <= 0:
        try:
            hi = df30i["high"].astype(float)
            lo = df30i["low"].astype(float)
            cl = df30i["close"].astype(float)
            prev = cl.shift(1)
            tr = np.maximum(hi - lo, np.maximum((hi - prev).abs(), (lo - prev).abs()))
            atr30 = float(tr.rolling(14).mean().iloc[-1])
        except Exception:
            return None
    if np.isnan(atr30) or atr30 <= 0:
        return None

    # Trend strength
    adx30 = float(last30.get("adx", np.nan))
    adx1h = float(last1h.get("adx", np.nan))
    if np.isnan(adx30):
        adx30 = float("nan")
    if np.isnan(adx1h):
        adx1h = float("nan")

    atr_pct = (atr30 / entry) * 100.0

    # --- Dynamic TP2/RR for MID ---
    def _tp2_r_mid(adx_1h: float, adx_30m: float, atrp: float) -> float:
        """Adaptive TP2_R for MID. Keeps targets realistic in weak trends and allows larger targets in strong trends."""
        a1 = 0.0 if np.isnan(adx_1h) else float(adx_1h)
        a30 = 0.0 if np.isnan(adx_30m) else float(adx_30m)

        # Base by trend strength
        if a1 >= 40:
            r = 3.0
        elif a1 >= 30:
            r = 2.6
        elif a1 >= 22:
            r = 2.2
        elif a1 >= 16:
            r = 1.8
        else:
            r = 1.5

        # Midframe confirmation
        if a30 >= 28:
            r += 0.2
        elif a30 <= 12:
            r -= 0.1

        # Volatility sanity: too low -> reduce; too high -> reduce
        try:
            v = float(atrp)
        except Exception:
            v = 0.0
        if v < 0.2:
            r -= 0.2
        elif v > 5.0:
            r -= 0.3
        elif v > 3.5:
            r -= 0.15

        return float(max(1.3, min(3.2, r)))

    tp2_r = _tp2_r_mid(adx1h, adx30, atr_pct)
    sl, tp1, tp2, rr = _build_levels(dir_trend, entry, atr30, tp2_r=tp2_r)
    # --- Anti-trap structure filters (apply to MID and MAIN candidates) ---
    trap_ok, trap_reason = _mid_structure_trap_ok(direction=str(dir_trend).upper(), entry=entry, df1hi=df1hi)
    if not bool(trap_ok):
        # Block obvious top/bottom traps. Also emit a structured event so bot can build a digest.
        try:
            if os.getenv("MID_INTERNAL_TRAP_LOG","0").strip().lower() not in ("0","false","no","off"):
                _k = f"trap|{symbol}|{str(dir_trend).upper()}|{_mid_trap_reason_key(str(trap_reason))}"
                if _mid_trap_should_log(_k):
                    logger.info('[mid][trap] %s dir=%s reason=%s entry=%.6g', symbol, str(dir_trend).upper(), str(trap_reason), float(entry))
            _emit_mid_trap_event({
                'dir': str(dir_trend).upper(),
                'reason': str(trap_reason),
                'reason_key': _mid_trap_reason_key(str(trap_reason)),
                'entry': float(entry),
            })
        except Exception:
            pass
        return None


    # --- TA extras (MAIN-like) ---
    # RSI/MACD on 5m
    rsi5 = float(last5.get("rsi", np.nan))
    macd_hist5 = float(last5.get("macd_hist", np.nan))

    # Bollinger Bands on 5m (20)
    bb_low = bb_mid = bb_high = float("nan")
    bb_pos = "—"
    try:
        cl = df5i["close"].astype(float)
        bb_mid = float(cl.rolling(20).mean().iloc[-1])
        bb_std = float(cl.rolling(20).std(ddof=0).iloc[-1])
        bb_low = bb_mid - 2.0 * bb_std
        bb_high = bb_mid + 2.0 * bb_std
        bb_pos = _bb_position(entry, bb_low, bb_mid, bb_high)
    except Exception:
        pass
    bb_str = bb_pos

    # Relative volume on 5m
    vol_rel = float("nan")
    try:
        vol = float(df5i.iloc[-1].get("volume", np.nan))
        vol_sma = float(df5i["volume"].astype(float).rolling(20).mean().iloc[-1])
        vol_rel = (vol / vol_sma) if (vol_sma and not np.isnan(vol_sma) and vol_sma > 0) else np.nan
    except Exception:
        vol_rel = float("nan")

    # VWAP on 30m (typical price * vol)
    vwap_val = float("nan")
    vwap_txt = "—"
    try:
        d = df30i.tail(80).copy()
        vol = d["volume"].astype(float)
        tp = (d["high"].astype(float) + d["low"].astype(float) + d["close"].astype(float)) / 3.0
        denom = float(vol.sum())
        if denom > 0:
            vwap_val = float((tp * vol).sum() / denom)
            if entry >= vwap_val:
                vwap_txt = f"{vwap_val:.6g} (above)"
            else:
                vwap_txt = f"{vwap_val:.6g} (below)"
    except Exception:
        pass

    # Pattern on 5m
    pattern, pat_bias = _candle_pattern(df5i)

    # RSI divergence on 30m (more stable than 5m)
    rsi_div = _rsi_divergence(df30i)

    # Channel + structure on 1h
    channel, _, _ = _linreg_channel(df1hi)
    mstruct_raw = _market_structure(df1hi)
    if mstruct_raw in ("HH-HL", "LH-LL"):
        mstruct = "TREND"
    elif mstruct_raw == "RANGE":
        mstruct = "RANGE"
    else:
        mstruct = "—"

    support, resistance = _nearest_levels(df1hi)

    # ---------- Liquidity zones + sweep (SMC-lite) ----------
    eq_hi = None
    eq_lo = None
    sweep_long = False
    sweep_short = False
    sweep_txt = "—"
    try:
        # Tolerance based on 30m ATR% (fallback to 0.2%)
        atr_abs_30m = abs(float(entry)) * (abs(float(atr_pct)) / 100.0) if (entry and atr_pct == atr_pct) else (abs(float(entry)) * 0.002)
        tol_liq = max(atr_abs_30m * 0.4, abs(float(entry)) * 0.001)

        # Swing points on 1h (for stable EQ levels)
        def _swing_points(_df: pd.DataFrame, left: int = 3, right: int = 3):
            try:
                if _df is None or getattr(_df, "empty", True) or len(_df) < (left + right + 20):
                    return [], []
                highs = _df["high"].astype(float).values
                lows = _df["low"].astype(float).values
                sh, sl = [], []
                for i in range(left, len(_df) - right):
                    h = highs[i]
                    l = lows[i]
                    if h == max(highs[i-left:i+right+1]):
                        sh.append((i, float(h)))
                    if l == min(lows[i-left:i+right+1]):
                        sl.append((i, float(l)))
                return sh, sl
            except Exception:
                return [], []

        sh, sl = _swing_points(df1hi, left=3, right=3)
        if len(sh) >= 2 and abs(sh[-1][1] - sh[-2][1]) <= tol_liq:
            eq_hi = (sh[-1][1] + sh[-2][1]) / 2.0
        if len(sl) >= 2 and abs(sl[-1][1] - sl[-2][1]) <= tol_liq:
            eq_lo = (sl[-1][1] + sl[-2][1]) / 2.0

        # Sweep detection on last ~30 candles 5m: wick through EQ + close back inside
        recent5 = df5i.tail(30) if (df5i is not None and not getattr(df5i, "empty", True)) else None
        if recent5 is not None and len(recent5) >= 5:
            close_last = float(recent5["close"].astype(float).iloc[-1])
            if eq_lo is not None:
                low_min = float(recent5["low"].astype(float).min())
                sweep_long = (low_min < float(eq_lo) - tol_liq * 0.15) and (close_last > float(eq_lo) + tol_liq * 0.05)
            if eq_hi is not None:
                high_max = float(recent5["high"].astype(float).max())
                sweep_short = (high_max > float(eq_hi) + tol_liq * 0.15) and (close_last < float(eq_hi) - tol_liq * 0.05)

        if dir_trend == "LONG":
            sweep_txt = "SWEEP↑" if sweep_long else "—"
        else:
            sweep_txt = "SWEEP↓" if sweep_short else "—"
    except Exception:
        pass


    # Score / confidence: use unified TA score to avoid constant 100s
    ta_score = _ta_score(
        direction=dir_trend,
        adx1=adx30,
        adx4=adx1h,
        rsi15=rsi5,
        macd_hist15=macd_hist5,
        vol_rel=vol_rel,
        bb_pos=bb_pos,
        rsi_div=rsi_div,
        channel=channel,
        mstruct=mstruct,
        pat_bias=pat_bias,
        atr_pct=atr_pct,
    )

    # Liquidity sweep bonus (aligns with direction) — boosts quality, doesn't force entry by default
    try:
        if (dir_trend == "LONG" and sweep_long) or (dir_trend == "SHORT" and sweep_short):
            ta_score = min(100, float(ta_score) + float(MID_LIQ_SWEEP_BONUS))
    except Exception:
        pass

    confidence = ta_score

    ta: Dict[str, Any] = {
        "direction": dir_trend,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "rr": rr,
        "confidence": confidence,
        "trap_ok": bool(trap_ok),
        "trap_reason": str(trap_reason or ""),
        "blocked": (not bool(trap_ok)),

        # fields used by MID filters/formatter
        "dir1": dir_mid,
        "dir4": dir_trend,
        "adx1": adx30,
        "adx4": adx1h,
        "atr_pct": atr_pct,

        # MID TA block fields
        "rsi": rsi5,
        "macd_hist": macd_hist5,
        "bb": bb_str,
        "rel_vol": vol_rel if (not np.isnan(vol_rel)) else 0.0,
        "vwap": vwap_txt,
        "vwap_val": (float(vwap_val) if (not np.isnan(vwap_val)) else 0.0),
        "pattern": pattern,
        "support": support,
        "resistance": resistance,
        "channel": channel,
        "mstruct": mstruct,
        "eq_hi": eq_hi,
        "eq_lo": eq_lo,
        "sweep_long": bool(sweep_long),
        "sweep_short": bool(sweep_short),
        "sweep": sweep_txt,
        "rsi_div": rsi_div,
        "ta_score": ta_score,
    }
    ta["ta_block"] = _fmt_ta_block_mid(ta)
    return ta
def choose_market(adx1_max: float, atr_pct_max: float) -> str:
    return "FUTURES" if (not np.isnan(adx1_max)) and adx1_max >= 28 and atr_pct_max >= 0.8 else "SPOT"

# ------------------ Backend ------------------

    async def orderbook_binance_futures(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """Binance USDT-M futures orderbook."""
        assert self.session is not None
        url = f"{self.BINANCE_FUTURES}/fapi/v1/depth"
        params = {"symbol": symbol, "limit": str(limit)}
        return await self._get_json(url, params=params)

    async def orderbook_bybit_linear(self, symbol: str, limit: int = 50) -> Dict[str, Any]:
        """Bybit linear futures orderbook."""
        assert self.session is not None
        url = f"{self.BYBIT}/v5/market/orderbook"
        params = {"category": "linear", "symbol": symbol, "limit": str(limit)}
        return await self._get_json(url, params=params)


# ------------------ Orderbook filter helpers ------------------
def _orderbook_metrics(bids: List[List[Any]], asks: List[List[Any]], *, levels: int) -> Dict[str, float]:
    """Return imbalance (bids/asks notional), spread_pct, bid_wall_ratio, ask_wall_ratio, bid_wall_near, ask_wall_near."""
    def _take(side: List[List[Any]]) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        for row in side[:levels]:
            try:
                p = float(row[0])
                q = float(row[1])
                out.append((p, q))
            except Exception:
                continue
        return out

    b = _take(bids)
    a = _take(asks)
    if not b or not a:
        return {"imbalance": 1.0, "spread_pct": 999.0, "bid_wall_ratio": 0.0, "ask_wall_ratio": 0.0, "mid": 0.0}

    best_bid = b[0][0]
    best_ask = a[0][0]
    mid = (best_bid + best_ask) / 2.0 if (best_bid > 0 and best_ask > 0) else max(best_bid, best_ask)
    spread_pct = ((best_ask - best_bid) / mid * 100.0) if mid > 0 else 999.0

    bid_not = [p * q for p, q in b]
    ask_not = [p * q for p, q in a]
    sum_b = float(sum(bid_not))
    sum_a = float(sum(ask_not))
    imbalance = (sum_b / sum_a) if sum_a > 0 else 999.0

    bid_avg = (sum_b / len(bid_not)) if bid_not else 0.0
    ask_avg = (sum_a / len(ask_not)) if ask_not else 0.0
    bid_wall_ratio = (max(bid_not) / bid_avg) if bid_avg > 0 else 0.0
    ask_wall_ratio = (max(ask_not) / ask_avg) if ask_avg > 0 else 0.0

    return {
        "imbalance": float(imbalance),
        "spread_pct": float(spread_pct),
        "bid_wall_ratio": float(bid_wall_ratio),
        "ask_wall_ratio": float(ask_wall_ratio),
        "mid": float(mid),
    }

def _orderbook_has_near_wall(side: List[List[Any]], *, mid: float, direction: str, near_pct: float, wall_ratio: float, levels: int) -> bool:
    """Detect an unusually large level close to current price."""
    # side: bids for SHORT-wall, asks for LONG-wall
    vals: List[Tuple[float, float]] = []
    for row in side[:levels]:
        try:
            p = float(row[0]); q = float(row[1])
            if p <= 0 or q <= 0:
                continue
            vals.append((p, p*q))
        except Exception:
            continue
    if not vals or mid <= 0:
        return False
    avg = sum(v for _, v in vals) / len(vals)
    if avg <= 0:
        return False
    # wall candidates near mid
    if direction == "LONG":
        # ask wall above mid within near_pct
        near = [(p, v) for p, v in vals if p >= mid and (p - mid) / mid * 100.0 <= near_pct]
    else:
        # bid wall below mid within near_pct
        near = [(p, v) for p, v in vals if p <= mid and (mid - p) / mid * 100.0 <= near_pct]
    if not near:
        return False
    max_near = max(v for _, v in near)
    return (max_near / avg) >= wall_ratio

async def orderbook_filter(api: "MultiExchangeData", exchange: str, symbol: str, direction: str) -> Tuple[bool, str]:
    """Return (allow, summary) for FUTURES orderbook confirmation."""
    exchange_l = exchange.strip().lower()
    try:
        if exchange_l == "binance":
            raw = await api.orderbook_binance_futures(symbol, limit=max(50, ORDERBOOK_LEVELS))
            bids = raw.get("bids", []) or []
            asks = raw.get("asks", []) or []
        elif exchange_l == "bybit":
            raw = await api.orderbook_bybit_linear(symbol, limit=max(50, ORDERBOOK_LEVELS))
            res = (raw.get("result", {}) or {})
            bids = res.get("b", []) or []
            asks = res.get("a", []) or []
        else:
            return (True, f"OB {exchange}: skip")
    except Exception:
        return (True, f"OB {exchange}: err")

    m = _orderbook_metrics(bids, asks, levels=ORDERBOOK_LEVELS)
    imb = m["imbalance"]
    spread = m["spread_pct"]
    mid = m["mid"]

    # quick blocks
    if spread > ORDERBOOK_MAX_SPREAD_PCT:
        return (False, f"OB {exchange}: spread {spread:.3f}%")

    # near wall blocks
    ask_wall_near = _orderbook_has_near_wall(asks, mid=mid, direction="LONG", near_pct=ORDERBOOK_WALL_NEAR_PCT, wall_ratio=ORDERBOOK_WALL_RATIO, levels=ORDERBOOK_LEVELS)
    bid_wall_near = _orderbook_has_near_wall(bids, mid=mid, direction="SHORT", near_pct=ORDERBOOK_WALL_NEAR_PCT, wall_ratio=ORDERBOOK_WALL_RATIO, levels=ORDERBOOK_LEVELS)

    if direction == "LONG":
        if imb < ORDERBOOK_IMBALANCE_MIN:
            return (False, f"OB {exchange}: imb {imb:.2f}x")
        if ask_wall_near:
            return (False, f"OB {exchange}: ask-wall")
    else:
        # for SHORT we need asks/bids >= min  => bids/asks <= 1/min
        if imb > (1.0 / ORDERBOOK_IMBALANCE_MIN):
            return (False, f"OB {exchange}: imb {imb:.2f}x")
        if bid_wall_near:
            return (False, f"OB {exchange}: bid-wall")

    return (True, f"OB {exchange}: ok imb {imb:.2f}x spr {spread:.3f}%")



class PriceUnavailableError(Exception):
    """Raised when all price sources failed for a symbol/market."""
    pass

class Backend:
    def __init__(self) -> None:
        self.feed = PriceFeed()
        self.news = NewsFilter()
        self.macro = MacroCalendar()

        self._rest_limiter = _RestLimiter()
        self._price_cache = _AsyncPriceCache(ttl_sec=float(os.getenv("PRICE_CACHE_TTL_SEC", "2") or 2))
        self._ws_max_age_sec = float(os.getenv("PRICE_WS_MAX_AGE_SEC", "2") or 2)
        # Trades are stored in PostgreSQL (db_store)
        self._last_signal_ts: Dict[str, float] = {}
        self._signal_seq = 1

        # Track when price fetching started failing per trade_id (used for forced CLOSE)
        self._price_fail_since: Dict[int, float] = {}

        # Track first moment when price breached SL per trade_id (for SL confirmation).
        self._sl_breach_since: Dict[int, float] = {}


        self.last_signal: Optional[Signal] = None
        self.last_spot_signal: Optional[Signal] = None
        self.last_futures_signal: Optional[Signal] = None
        self.scanned_symbols_last: int = 0
        self.last_news_action: str = "ALLOW"
        self.last_macro_action: str = "ALLOW"
        self.scanner_running: bool = True
        self.last_scan_ts: float = 0.0
        # Cache last successful TOP symbols so scanner can continue when one exchange is temporarily down.
        self._symbols_cache: list[str] = []
        self._symbols_cache_ts: float = 0.0
        self.trade_stats: dict = {}
        self._load_trade_stats()

        # Optional heartbeat callback (set by bot.py) to report Smart Manager liveness.
        # Called best-effort once per track_loop cycle.
        self.health_tick_cb = None



    def set_mid_trap_sink(self, cb) -> None:
        """Register sink for MID trap events (used by bot to build periodic digest (>=6h))."""
        try:
            set_mid_trap_sink(cb)
        except Exception:
            pass


    # ---------------- TA / UI helpers ----------------
    def get_known_symbols(self, *, limit: int = 5000, quotes: tuple[str, ...] = ("USDT", "USDC")) -> list[str]:
        """Best-effort symbol universe for UI helpers (Analyze button, suggestions).

        We intentionally avoid doing extra REST calls here.
        We reuse symbol pools that are already maintained by running scanners.

        Returns sanitized, de-duplicated symbols like BTCUSDT.
        """
        import re

        def _extract(v) -> list[str]:
            try:
                if not v:
                    return []
                if isinstance(v, (list, tuple)):
                    # Some caches store (ts, provider, symbols)
                    if v and isinstance(v[-1], list):
                        return list(v[-1])
                    if v and isinstance(v[0], str):
                        return list(v)
                if isinstance(v, dict):
                    cand = v.get("symbols") or v.get("data") or v.get("value") or []
                    if isinstance(cand, (list, tuple)):
                        return list(cand)
                return []
            except Exception:
                return []

        def _sanitize(symbols: list[str]) -> list[str]:
            out: list[str] = []
            for s in symbols or []:
                s2 = re.sub(r"[^A-Z0-9]", "", (s or "").upper())
                if not any(s2.endswith(q) for q in quotes):
                    continue
                if len(s2) < 6 or len(s2) > 30:
                    continue
                out.append(s2)
            # de-dupe keep order
            return list(dict.fromkeys(out))

        pools: list[str] = []
        for attr in ("_mid_symbols_cache", "mid_symbols_cache", "_symbols_cache", "symbols_cache"):
            pools.extend(_extract(getattr(self, attr, None)))

        pools = _sanitize(pools)
        if limit and len(pools) > limit:
            pools = pools[: max(1, int(limit))]
        return pools


    def next_signal_id(self) -> int:
        sid = self._signal_seq
        self._signal_seq += 1
        return sid

    def can_emit(self, symbol: str) -> bool:
        ts = self._last_signal_ts.get(symbol.upper(), 0.0)
        return (time.time() - ts) >= (COOLDOWN_MINUTES * 60)

    def mark_emitted(self, symbol: str) -> None:
        self._last_signal_ts[symbol.upper()] = time.time()

    async def open_trade(self, user_id: int, signal: Signal, orig_text: str) -> bool:
        """Persist trade in Postgres. Returns False if already opened."""
        inserted, _tid = await db_store.open_trade_once(
            user_id=int(user_id),
            signal_id=int(signal.signal_id),
            market=(signal.market or "FUTURES").upper(),
            symbol=signal.symbol,
            side=(signal.direction or "LONG").upper(),
            entry=float(signal.entry or 0.0),
            tp1=float(signal.tp1) if signal.tp1 is not None else None,
            tp2=float(signal.tp2) if signal.tp2 is not None else None,
            sl=float(signal.sl) if signal.sl is not None else None,
            orig_text=orig_text or "",
        )
        return bool(inserted)


    async def remove_trade(self, user_id: int, signal_id: int) -> bool:
        row = await db_store.get_trade_by_user_signal(int(user_id), int(signal_id))
        if not row:
            return False
        trade_id = int(row["id"])
        # manual close: use current price + proper TP1-aware PnL
        s = Signal(
            symbol=str(row.get("symbol") or ""),
            market=str(row.get("market") or "FUTURES").upper(),
            direction=str(row.get("side") or "LONG").upper(),
            entry=float(row.get("entry") or 0.0),
            tp1=float(row.get("tp1")) if row.get("tp1") is not None else None,
            tp2=float(row.get("tp2")) if row.get("tp2") is not None else None,
            sl=float(row.get("sl")) if row.get("sl") is not None else None,
        )
        try:
            close_price, _src = await self._get_price_with_source(s)
            close_price = float(close_price)
        except Exception:
            close_price = float(row.get("entry") or 0.0)

        trade_ctx = UserTrade(user_id=int(user_id), signal=s, tp1_hit=bool(row.get("tp1_hit")))
        pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(close_price), close_reason="CLOSED")
        try:
            await db_store.close_trade(trade_id, status="CLOSED", price=float(close_price), pnl_total_pct=float(pnl))
            self._sl_breach_since.pop(trade_id, None)
            try:
                if hasattr(self, "_tp1_peak_px"):
                    self._tp1_peak_px.pop(trade_id, None)
                if hasattr(self, "_be_skip_until"):
                    self._be_skip_until.pop(trade_id, None)
            except Exception:
                pass
        except Exception:
            # best effort
            await db_store.close_trade(trade_id, status="CLOSED")
            self._sl_breach_since.pop(trade_id, None)
            try:
                if hasattr(self, "_tp1_peak_px"):
                    self._tp1_peak_px.pop(trade_id, None)
                if hasattr(self, "_be_skip_until"):
                    self._be_skip_until.pop(trade_id, None)
            except Exception:
                pass
        return True


    # ---------------- trade stats helpers ----------------
    def _load_trade_stats(self) -> None:
        self.trade_stats = {"spot": {"days": {}, "weeks": {}}, "futures": {"days": {}, "weeks": {}}}
        if TRADE_STATS_FILE.exists():
            try:
                data = json.loads(TRADE_STATS_FILE.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    self.trade_stats.update(data)
            except Exception:
                logger.exception("scanner_loop exception")

    def _save_trade_stats(self) -> None:
        try:
            TRADE_STATS_FILE.write_text(json.dumps(self.trade_stats, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        except Exception:
            pass

    def record_trade_close(self, trade: UserTrade, close_reason: str, close_price: float, close_ts: float | None = None) -> None:
        ts = float(close_ts if close_ts is not None else time.time())
        pnl_pct = _calc_effective_pnl_pct(trade, float(close_price), close_reason)
        _bump_stats(self.trade_stats, trade.signal.market, ts, close_reason, pnl_pct, bool(trade.tp1_hit))
        self._save_trade_stats()

    def _bucket(self, market: str, period: str, key: str) -> dict:
        mk = _market_key(market)
        root = (self.trade_stats or {}).get(mk, {})
        per = root.get(period, {}) if isinstance(root, dict) else {}
        b = per.get(key, {}) if isinstance(per, dict) else {}
        if not isinstance(b, dict):
            b = _empty_bucket()
        # normalize keys
        base = _empty_bucket()
        for k, v in base.items():
            if k not in b:
                b[k] = v
        return b

    async def perf_today(self, user_id: int, market: str) -> dict:
        tz = ZoneInfo(TZ_NAME)
        now_tz = dt.datetime.now(tz)
        start_tz = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
        end_tz = start_tz + dt.timedelta(days=1)
        start = start_tz.astimezone(dt.timezone.utc)
        end = end_tz.astimezone(dt.timezone.utc)
        return await db_store.perf_bucket(int(user_id), (market or "FUTURES").upper(), since=start, until=end)

    async def perf_week(self, user_id: int, market: str) -> dict:
        """Last 7 days rolling window in TZ_NAME (inclusive of today)."""
        tz = ZoneInfo(TZ_NAME)
        now_tz = dt.datetime.now(tz)
        # start at local midnight 6 days ago => 7 calendar days including today
        start_tz = (now_tz - dt.timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_tz = now_tz
        start = start_tz.astimezone(dt.timezone.utc)
        end = end_tz.astimezone(dt.timezone.utc)
        return await db_store.perf_bucket(int(user_id), (market or "FUTURES").upper(), since=start, until=end)

    async def report_daily(self, user_id: int, market: str, days: int = 7, tz: str = "UTC") -> list[dict]:
        return await db_store.daily_report(int(user_id), (market or "FUTURES").upper(), days=int(days), tz=str(tz))

    async def report_weekly(self, user_id: int, market: str, weeks: int = 4, tz: str = "UTC") -> list[dict]:
        return await db_store.weekly_report(int(user_id), (market or "FUTURES").upper(), weeks=int(weeks), tz=str(tz))

    async def get_trade(self, user_id: int, signal_id: int) -> Optional[dict]:
        return await db_store.get_trade_by_user_signal(int(user_id), int(signal_id))

    async def get_trade_live(self, user_id: int, signal_id: int) -> Optional[dict]:
        """Return trade row with live price + price source + hit checks."""
        t = await db_store.get_trade_by_user_signal(int(user_id), int(signal_id))
        if not t:
            return None
        try:
            market = (t.get('market') or 'FUTURES').upper()
            symbol = str(t.get('symbol') or '')
            side = (t.get('side') or 'LONG').upper()
            s = Signal(
                signal_id=int(t.get('signal_id') or 0),
                market=market,
                symbol=symbol,
                direction=side,
                timeframe='',
                entry=float(t.get('entry') or 0.0),
                sl=float(t.get('sl') or 0.0),
                tp1=float(t.get('tp1') or 0.0),
                tp2=float(t.get('tp2') or 0.0),
                rr=0.0,
                confidence=0,
                confirmations='',
                risk_note='',
                ts=float(dt.datetime.now(dt.timezone.utc).timestamp()),
            )
            price, src = await self._get_price_with_source(s)
            price_f = float(price)
            def hit_tp(lvl: float) -> bool:
                return price_f >= lvl if side == 'LONG' else price_f <= lvl
            def hit_sl(lvl: float) -> bool:
                return price_f <= lvl if side == 'LONG' else price_f >= lvl
            sl = float(t.get('sl') or 0.0) if t.get('sl') is not None else 0.0
            tp1 = float(t.get('tp1') or 0.0) if t.get('tp1') is not None else 0.0
            tp2 = float(t.get('tp2') or 0.0) if t.get('tp2') is not None else 0.0
            out = dict(t)
            out['price_f'] = price_f
            out['price_src'] = src
            out['hit_sl'] = bool(sl and hit_sl(float(sl)))
            out['hit_tp1'] = bool(tp1 and hit_tp(float(tp1)))
            out['hit_tp2'] = bool(tp2 and hit_tp(float(tp2)))
            return out
        except Exception:
            return dict(t)


    async def get_trade_live_by_id(self, user_id: int, trade_id: int) -> Optional[dict]:
        """Return trade row (by DB id) with live price + price source + hit checks."""
        t = await db_store.get_trade_by_id(int(user_id), int(trade_id))
        if not t:
            return None
        try:
            market = (t.get('market') or 'FUTURES').upper()
            symbol = str(t.get('symbol') or '')
            side = (t.get('side') or 'LONG').upper()
            s = Signal(
                signal_id=int(t.get('signal_id') or 0),
                market=market,
                symbol=symbol,
                direction=side,
                timeframe='',
                entry=float(t.get('entry') or 0.0),
                sl=float(t.get('sl') or 0.0),
                tp1=float(t.get('tp1') or 0.0),
                tp2=float(t.get('tp2') or 0.0),
                rr=0.0,
                confidence=0,
                confirmations='',
                risk_note='',
                ts=float(dt.datetime.now(dt.timezone.utc).timestamp()),
            )
            price, src = await self._get_price_with_source(s)
            price_f = float(price)
            def hit_tp(lvl: float) -> bool:
                return price_f >= lvl if side == 'LONG' else price_f <= lvl
            def hit_sl(lvl: float) -> bool:
                return price_f <= lvl if side == 'LONG' else price_f >= lvl
            sl = float(t.get('sl') or 0.0) if t.get('sl') is not None else 0.0
            tp1 = float(t.get('tp1') or 0.0) if t.get('tp1') is not None else 0.0
            tp2 = float(t.get('tp2') or 0.0) if t.get('tp2') is not None else 0.0
            out = dict(t)
            out['price_f'] = price_f
            out['price_src'] = src
            out['hit_sl'] = bool(sl and hit_sl(float(sl)))
            out['hit_tp1'] = bool(tp1 and hit_tp(float(tp1)))
            out['hit_tp2'] = bool(tp2 and hit_tp(float(tp2)))
            return out
        except Exception:
            return dict(t)

    async def remove_trade_by_id(self, user_id: int, trade_id: int) -> dict | None:
        """Manual close from UI button.

        IMPORTANT:
        - Must use *current* price (not entry)
        - Must compute total PnL correctly even if TP1 partial happened
        - Returns a ready-to-send unified card text (or None on failure)
        """
        row = await db_store.get_trade_by_id(int(user_id), int(trade_id))
        if not row:
            return None

        # Build a minimal Signal-like object for price + pnl calculation
        s = Signal(
            symbol=str(row.get("symbol") or ""),
            market=str(row.get("market") or "FUTURES").upper(),
            direction=str(row.get("side") or "LONG").upper(),
            entry=float(row.get("entry") or 0.0),
            tp1=float(row.get("tp1")) if row.get("tp1") is not None else None,
            tp2=float(row.get("tp2")) if row.get("tp2") is not None else None,
            sl=float(row.get("sl")) if row.get("sl") is not None else None,
        )

        try:
            close_px, close_src = await self._get_price_with_source(s)
            close_px = float(close_px)
        except Exception:
            # Best-effort fallback: use entry if price is unavailable
            close_px, close_src = float(row.get("entry") or 0.0), ""

        tp1_hit = bool(row.get("tp1_hit"))
        trade_ctx = UserTrade(user_id=int(user_id), signal=s, tp1_hit=tp1_hit)
        pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(close_px), close_reason="CLOSED")

        try:
            await db_store.close_trade(int(trade_id), status="CLOSED", price=float(close_px), pnl_total_pct=float(pnl))
        except Exception:
            # If DB fails, still return a message so user sees what happened.
            try:
                await db_store.close_trade(int(trade_id), status="CLOSED")
            except Exception:
                pass

        # Build unified card (same style as auto close cards)
        import datetime as _dt
        now_utc = _dt.datetime.now(_dt.timezone.utc)
        rr = _calc_rr_str(float(getattr(s, "entry", 0.0) or 0.0), float(getattr(s, "sl", 0.0) or 0.0), float(getattr(s, "tp1", 0.0) or 0.0), float(getattr(s, "tp2", 0.0) or 0.0))
        after_tp1 = _trf(int(user_id), "after_tp1_suffix") if tp1_hit else ""
        close_agent = _trf(int(user_id), "close_agent_manual")

        # BUGFIX: tp2_disp must be defined for the i18n template.
        # Some signals/trades don't have TP2 (or it's 0), so we use "-".
        tp2_disp = "-"
        try:
            _tp2v = float(getattr(s, "tp2", 0.0) or 0.0)
            tp2_disp = (f"{_tp2v:.6f}" if _tp2v > 0 else "-")
        except Exception:
            tp2_disp = "-"

        # Status string should explicitly mention manual close
        status_txt = _trf(int(user_id), "status_closed_manual")

        # Optional: show price source for transparency (PRO users love this)
        reason_line = ""
        try:
            if close_src:
                _r = _trf(int(user_id), "reason_manual_close", source=str(close_src))
                reason_line = (_trf(int(user_id), "lbl_reason", reason=_r) + "\n") if _r else ""
        except Exception:
            reason_line = ""

        emoji = "⚪️"
        be_price = float(row.get("be_price") or 0.0) if row.get("be_price") is not None else 0.0
        be_line = (f"🛡 BE: {float(be_price):.6f}\n" if (be_price and float(be_price) > 0) else "")
        txt = _trf(int(user_id),
            "msg_manual_close",
            symbol=s.symbol,
            market=str(getattr(s, "market", "FUTURES") or "FUTURES").upper(),
            emoji=emoji,
            side=str(getattr(s, "direction", "LONG") or "LONG").upper(),
            after_tp1=after_tp1,
            rr=rr,
            close_agent=close_agent,
            reason_line=reason_line,
            entry=f"{float(getattr(s,'entry',0.0) or 0.0):.6f}",
            sl=f"{float(getattr(s,'sl',0.0) or 0.0):.6f}",
            tp1=f"{float(getattr(s,'tp1',0.0) or 0.0):.6f}",
            tp2=tp2_disp,
            be_line=be_line,
            pnl_total=fmt_pnl_pct(float(pnl)),
            opened_time=fmt_dt_msk(row.get("opened_at")),
            closed_time=fmt_dt_msk(now_utc),
            status=status_txt,
        )

        return {"ok": True, "text": txt, "pnl_total_pct": float(pnl), "close_price": float(close_px)}

    async def get_user_trades(self, user_id: int) -> list[dict]:
        return await db_store.list_user_trades(int(user_id), include_closed=False, limit=50)

    def get_next_macro(self) -> Optional[Tuple[MacroEvent, Tuple[float, float]]]:
        return self.macro.next_event()

    def get_macro_status(self) -> dict:
        act, ev, win = self.macro.status()
        until_ts = None
        reason = None
        window = None
        if act != "ALLOW" and ev and win:
            w0, w1 = win
            until_ts = w1
            window = (w0, w1)
            reason = getattr(ev, "name", None) or getattr(ev, "title", None) or getattr(ev, "type", None)
        return {"action": act, "event": ev, "window": window, "until_ts": until_ts, "reason": reason}

    def get_news_status(self) -> dict:
        act = getattr(self, "last_news_action", "ALLOW")
        info = self.news.last_block_info() if hasattr(self, "news") else None
        reason = None
        until_ts = None
        if info:
            coin, until = info
            reason = coin
            until_ts = until
        return {"action": act, "reason": reason, "until_ts": until_ts}

    async def _price_cached(self, ex: str, market: str, symbol: str, fetch_fn, *, src_label: str) -> tuple[float | None, str]:
        """(cache -> fetch) helper for a single exchange."""
        try:
            c = self._price_cache.get(ex, market, symbol)
            if c is not None:
                p, src = c
                if p is None:
                    return None, str(src)
                return float(p), str(src)
        except Exception:
            pass

        async def _do():
            p = await fetch_fn()
            return (p, src_label)

        try:
            p, src = await self._price_cache.get_or_fetch(ex, market, symbol, _do)
            if p is None:
                return None, str(src)
            return float(p), str(src)
        except Exception:
            return None, ex.upper()

    async def _fetch_rest_price(self, market: str, symbol: str) -> float | None:
        """Binance REST fallback price (spot/futures)."""
        m = (market or "FUTURES").upper()
        sym = (symbol or "").upper()
        if not sym:
            return None
        url = "https://api.binance.com/api/v3/ticker/price" if m == "SPOT" else "https://fapi.binance.com/fapi/v1/ticker/price"

        async def _do_req():
            try:
                timeout = aiohttp.ClientTimeout(total=6)
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get(url, params={"symbol": sym}) as r:
                        if r.status != 200:
                            return None
                        data = await r.json()
                p = data.get("price") if isinstance(data, dict) else None
                return float(p) if p is not None else None
            except Exception:
                return None

        try:
            return await self._rest_limiter.run("binance", _do_req)
        except Exception:
            return None


    async def _fetch_bybit_price(self, market: str, symbol: str) -> float | None:
        """Bybit REST price (spot/futures). Uses V5 tickers.

        market: SPOT -> category=spot
                FUTURES -> category=linear (USDT perpetual)
        """
        mkt = (market or "FUTURES").upper()
        sym = (symbol or "").upper().replace("/", "")
        if not sym:
            return None
        category = "spot" if mkt == "SPOT" else "linear"
        url = "https://api.bybit.com/v5/market/tickers"

        async def _do_req():
            try:
                timeout = aiohttp.ClientTimeout(total=6)
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get(url, params={"category": category, "symbol": sym}) as r:
                        if r.status != 200:
                            return None
                        data = await r.json()
                if not isinstance(data, dict):
                    return None
                if int(data.get("retCode", 0) or 0) != 0:
                    return None
                res = data.get("result") or {}
                lst = (res.get("list") or []) if isinstance(res, dict) else []
                if not lst:
                    return None
                item = lst[0] if isinstance(lst, list) else None
                if not isinstance(item, dict):
                    return None
                p = item.get("lastPrice") or item.get("indexPrice") or item.get("markPrice")
                return float(p) if p is not None else None
            except Exception:
                return None

        try:
            return await self._rest_limiter.run("bybit", _do_req)
        except Exception:
            return None


    async def _fetch_okx_price(self, market: str, symbol: str) -> float | None:
        """OKX REST price (spot/futures).

        market: SPOT -> instId=BASE-USDT
                FUTURES -> instId=BASE-USDT-SWAP (USDT perpetual)
        """
        mkt = (market or "FUTURES").upper()
        sym = (symbol or "").upper().replace("/", "")
        if not sym:
            return None
        try:
            base = sym[:-4] if sym.endswith("USDT") else sym
            inst = f"{base}-USDT" if mkt == "SPOT" else f"{base}-USDT-SWAP"
        except Exception:
            return None

        url = "https://www.okx.com/api/v5/market/ticker"

        async def _do_req():
            try:
                timeout = aiohttp.ClientTimeout(total=6)
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get(url, params={"instId": inst}) as r:
                        if r.status != 200:
                            return None
                        data = await r.json()
                if not isinstance(data, dict):
                    return None
                arr = data.get("data") or []
                if not arr or not isinstance(arr, list):
                    return None
                item = arr[0] if arr else None
                if not isinstance(item, dict):
                    return None
                p = item.get("last") or item.get("lastPrice")
                return float(p) if p is not None else None
            except Exception:
                return None

        try:
            return await self._rest_limiter.run("okx", _do_req)
        except Exception:
            return None


    async def _get_price_with_source(self, signal: Signal) -> tuple[float, str]:
        """Return (price, source) for tracking.

        Required policy:
          1) Try **WS only** in priority order.
          2) If **all WS** fail, then try **REST/public** in the same order.

        Selection is controlled via env:
          SPOT_PRICE_SOURCE=BINANCE|BYBIT|MEDIAN
          FUTURES_PRICE_SOURCE=BINANCE|BYBIT|MEDIAN

        Note: if all sources fail, raises PriceUnavailableError.
        """
        market = (signal.market or "FUTURES").upper()
        base = float(getattr(signal, "entry", 0) or 0) or None

        # Mock mode: keep prices around entry to avoid nonsense.
        if not USE_REAL_PRICE:
            return float(self.feed.mock_price(market, signal.symbol, base=base)), "MOCK"

        mode = (SPOT_PRICE_SOURCE if market == "SPOT" else FUTURES_PRICE_SOURCE).upper().strip() or "MEDIAN"
        if mode not in ("BINANCE", "BYBIT", "MEDIAN"):
            mode = "MEDIAN"

        def _pdbg(msg: str):
            if PRICE_DEBUG:
                try:
                    logger.info(f"[price] {market} {signal.symbol}: {msg}")
                except Exception:
                    pass

        def _is_reasonable(p: float | None) -> bool:
            """Sanity filter to avoid using bad ticks (e.g., 0.0).

            Permissive, but rejects:
              - non-finite / <= 0
              - if entry is known: values wildly off-scale (10x away)
            """
            if p is None:
                return False
            try:
                fp = float(p)
            except Exception:
                return False
            if not math.isfinite(fp) or fp <= 0:
                return False
            if base is not None and base > 0:
                lo = base * 0.10
                hi = base * 10.0
                if fp < lo or fp > hi:
                    return False
            return True

        # --- WS-only helpers (NO REST FALLBACK INSIDE) ---
        async def _binance_ws_only() -> tuple[float | None, str]:
            try:
                await self.feed.ensure_stream(market, signal.symbol, ex="binance")
                latest = self.feed.get_latest(market, signal.symbol, max_age_sec=self._ws_max_age_sec)
                if latest is not None and float(latest) > 0:
                    src = self.feed.get_latest_source(market, signal.symbol) or "BINANCE_WS"
                    # Normalize feed sources
                    if src == "WS":
                        src = "BINANCE_WS"
                    elif src == "REST":
                        # In WS-only phase we must not accept REST-derived values
                        return None, "BINANCE_WS_MISS"
                    if _is_reasonable(float(latest)):
                        _pdbg(f"BINANCE WS hit p={latest} src={src}")
                        return float(latest), "BINANCE_WS"
            except Exception:
                pass

            # cache: accept only WS-derived cached values
            try:
                c = self._price_cache.get("binance", market, signal.symbol)
                if c is not None and c[0] is not None and str(c[1]).upper().endswith("_WS") and _is_reasonable(c[0]):
                    return float(c[0]), str(c[1])
            except Exception:
                pass

            return None, "BINANCE_WS_MISS"

        async def _bybit_ws_only() -> tuple[float | None, str]:
            try:
                await self.feed.ensure_stream(market, signal.symbol, ex="bybit")
                latest = self.feed.get_latest(market, signal.symbol, max_age_sec=self._ws_max_age_sec, ex="bybit")
                if latest is not None and float(latest) > 0 and _is_reasonable(float(latest)):
                    _pdbg(f"BYBIT WS hit p={latest}")
                    return float(latest), "BYBIT_WS"
            except Exception:
                pass
            try:
                c = self._price_cache.get("bybit", market, signal.symbol)
                if c is not None and c[0] is not None and str(c[1]).upper().endswith("_WS") and _is_reasonable(c[0]):
                    return float(c[0]), str(c[1])
            except Exception:
                pass
            return None, "BYBIT_WS_MISS"

        async def _okx_ws_only() -> tuple[float | None, str]:
            try:
                await self.feed.ensure_stream(market, signal.symbol, ex="okx")
                latest = self.feed.get_latest(market, signal.symbol, max_age_sec=self._ws_max_age_sec, ex="okx")
                if latest is not None and float(latest) > 0 and _is_reasonable(float(latest)):
                    _pdbg(f"OKX WS hit p={latest}")
                    return float(latest), "OKX_WS"
            except Exception:
                pass
            try:
                c = self._price_cache.get("okx", market, signal.symbol)
                if c is not None and c[0] is not None and str(c[1]).upper().endswith("_WS") and _is_reasonable(c[0]):
                    return float(c[0]), str(c[1])
            except Exception:
                pass
            return None, "OKX_WS_MISS"

        async def _gateio_ws_only() -> tuple[float | None, str]:
            if market != "SPOT":
                return None, "GATEIO_WS_MISS"
            try:
                await self.feed.ensure_stream(market, signal.symbol, ex="gateio")
                latest = self.feed.get_latest(market, signal.symbol, max_age_sec=self._ws_max_age_sec, ex="gateio")
                if latest is not None and float(latest) > 0 and _is_reasonable(float(latest)):
                    _pdbg(f"GATEIO WS hit p={latest}")
                    return float(latest), "GATEIO_WS"
            except Exception:
                pass
            try:
                c = self._price_cache.get("gateio", market, signal.symbol)
                if c is not None and c[0] is not None and str(c[1]).upper().endswith("_WS") and _is_reasonable(c[0]):
                    return float(c[0]), str(c[1])
            except Exception:
                pass
            return None, "GATEIO_WS_MISS"

        async def _mexc_ws_only() -> tuple[float | None, str]:
            if market != "SPOT":
                return None, "MEXC_WS_MISS"
            try:
                await self.feed.ensure_stream(market, signal.symbol, ex="mexc")
                latest = self.feed.get_latest(market, signal.symbol, max_age_sec=self._ws_max_age_sec, ex="mexc")
                if latest is not None and float(latest) > 0 and _is_reasonable(float(latest)):
                    _pdbg(f"MEXC WS hit p={latest}")
                    return float(latest), "MEXC_WS"
            except Exception:
                pass
            try:
                c = self._price_cache.get("mexc", market, signal.symbol)
                if c is not None and c[0] is not None and str(c[1]).upper().endswith("_WS") and _is_reasonable(c[0]):
                    return float(c[0]), str(c[1])
            except Exception:
                pass
            return None, "MEXC_WS_MISS"

        # --- REST-only helpers (NO WS INSIDE) ---
        async def _binance_rest_only() -> tuple[float | None, str]:
            rest = await self._fetch_rest_price(market, signal.symbol)
            if _is_reasonable(rest):
                try:
                    async with self._price_cache._mu:
                        self._price_cache._data[("binance", market.upper(), signal.symbol.upper())] = (float(rest), "BINANCE_REST", time.time())
                except Exception:
                    pass
                try:
                    self.feed._set_latest(market, signal.symbol, float(rest), source="REST")
                except Exception:
                    pass
                return float(rest), "BINANCE_REST"
            return None, "BINANCE_REST_MISS"

        async def _bybit_rest_only() -> tuple[float | None, str]:
            async def _fetch():
                return await self._fetch_bybit_price(market, signal.symbol)
            p, _src = await self._price_cached("bybit", market, signal.symbol, _fetch, src_label="BYBIT_REST")
            if _is_reasonable(p):
                return float(p), "BYBIT_REST"
            return None, "BYBIT_REST_MISS"

        async def _okx_rest_only() -> tuple[float | None, str]:
            async def _fetch():
                return await self._fetch_okx_price(market, signal.symbol)
            p, _src = await self._price_cached("okx", market, signal.symbol, _fetch, src_label="OKX_REST")
            if _is_reasonable(p):
                return float(p), "OKX_REST"
            return None, "OKX_REST_MISS"

        async def _gateio_rest_only() -> tuple[float | None, str]:
            if market != "SPOT":
                return None, "GATEIO_REST_MISS"

            async def _fetch():
                async def _do():
                    try:
                        return await _gateio_public_price(signal.symbol)
                    except Exception:
                        return None
                return await self._rest_limiter.run("gateio", _do)

            p, _src = await self._price_cached("gateio", market, signal.symbol, _fetch, src_label="GATEIO_PUBLIC")
            if _is_reasonable(p):
                return float(p), "GATEIO_PUBLIC"
            return None, "GATEIO_REST_MISS"

        async def _mexc_rest_only() -> tuple[float | None, str]:
            if market != "SPOT":
                return None, "MEXC_REST_MISS"

            async def _fetch():
                async def _do():
                    try:
                        return await _mexc_public_price(signal.symbol)
                    except Exception:
                        return None
                return await self._rest_limiter.run("mexc", _do)

            p, _src = await self._price_cached("mexc", market, signal.symbol, _fetch, src_label="MEXC_PUBLIC")
            if _is_reasonable(p):
                return float(p), "MEXC_PUBLIC"
            return None, "MEXC_REST_MISS"

        spot_chain = ["BINANCE", "BYBIT", "OKX", "GATEIO", "MEXC"]
        fut_chain = ["BINANCE", "BYBIT", "OKX"]
        chain = spot_chain if market == "SPOT" else fut_chain

        def _rotate(chain0: list[str], first: str) -> list[str]:
            first = (first or "").upper()
            if first in chain0:
                i = chain0.index(first)
                return chain0[i:] + chain0[:i]
            return chain0

        # Rotate only for single-source modes; MEDIAN is handled specially.
        if mode == "BINANCE":
            chain = _rotate(chain, "BINANCE")
        elif mode == "BYBIT":
            chain = _rotate(chain, "BYBIT")

        async def _ws_only(name: str) -> tuple[float | None, str]:
            n = name.upper()
            if n == "BINANCE":
                return await _binance_ws_only()
            if n == "BYBIT":
                return await _bybit_ws_only()
            if n == "OKX":
                return await _okx_ws_only()
            if n == "GATEIO":
                return await _gateio_ws_only()
            if n == "MEXC":
                return await _mexc_ws_only()
            return None, f"{n}_WS_MISS"

        async def _rest_only(name: str) -> tuple[float | None, str]:
            n = name.upper()
            if n == "BINANCE":
                return await _binance_rest_only()
            if n == "BYBIT":
                return await _bybit_rest_only()
            if n == "OKX":
                return await _okx_rest_only()
            if n == "GATEIO":
                return await _gateio_rest_only()
            if n == "MEXC":
                return await _mexc_rest_only()
            return None, f"{n}_REST_MISS"

        # --- PHASE A: WS-only (priority order) ---
        if mode == "MEDIAN":
            # MEDIAN over available WS sources (best-effort). Prefer major venues first.
            _pdbg("mode=MEDIAN; PHASE A WS-only: try BINANCE_WS/BYBIT_WS/OKX_WS for median")
            ws_prices: list[tuple[str, float]] = []
            for _nm, _fn in (("BINANCE", _binance_ws_only), ("BYBIT", _bybit_ws_only), ("OKX", _okx_ws_only)):
                try:
                    p, _ = await _fn()
                except Exception:
                    p = None
                if _is_reasonable(p):
                    ws_prices.append((_nm, float(p)))
            if len(ws_prices) >= 2:
                try:
                    med = float(statistics.median([p for _, p in ws_prices]))
                    return med, "MEDIAN(" + "+".join([f"{nm}_WS" for nm, _ in ws_prices]) + ")"
                except Exception:
                    pass
            if ws_prices:
                nm, p = ws_prices[0]
                return float(p), f"{nm}_WS"

        _pdbg(f"PHASE A WS-only order={'+'.join(chain)} mode={mode}")
        for exname in chain:
            p, src = await _ws_only(exname)
            _pdbg(f"WS {exname} -> p={p} src={src}")
            if _is_reasonable(p):
                _pdbg(f"SELECTED WS {exname} src={src} p={p}")
                return float(p), str(src)

        # --- PHASE B: REST/public fallback (only if ALL WS failed) ---
        if mode == "MEDIAN":
            _pdbg("mode=MEDIAN; PHASE B REST-only: try BINANCE_REST and BYBIT_REST for median")
            b, _bsrc = await _binance_rest_only()
            y, _ysrc = await _bybit_rest_only()
            if _is_reasonable(b) and _is_reasonable(y):
                try:
                    med = float(statistics.median([float(b), float(y)]))
                    return med, "MEDIAN(BINANCE_REST+BYBIT_REST)"
                except Exception:
                    pass
            if _is_reasonable(b):
                return float(b), "BINANCE_REST"
            if _is_reasonable(y):
                return float(y), "BYBIT_REST"

        _pdbg(f"PHASE B REST-only order={'+'.join(chain)} mode={mode}")
        for exname in chain:
            p, src = await _rest_only(exname)
            _pdbg(f"REST {exname} -> p={p} src={src}")
            if _is_reasonable(p):
                _pdbg(f"SELECTED REST {exname} src={src} p={p}")
                return float(p), str(src)

        raise PriceUnavailableError(f"price unavailable market={market} symbol={signal.symbol} mode={mode}")



    async def _get_price(self, signal: Signal) -> float:
        price, _src = await self._get_price_with_source(signal)
        return float(price)

    async def check_signal_openable(self, signal: Signal) -> tuple[bool, str, float]:
        """Return (allowed, reason_code, current_price).

        reason_code is one of: OK, TP2, TP1, SL, TIME, ERROR
        """
        try:
            now = time.time()
            ttl = int(os.getenv("SIGNAL_OPEN_TTL_SECONDS", "1800"))  # 30 min default
            sig_ts = float(getattr(signal, "ts", 0) or 0)
            if sig_ts and ttl > 0 and (now - sig_ts) > ttl:
                price = await self._get_price(signal)
                return False, "TIME", float(price)

            price = float(await self._get_price(signal))

            direction = (getattr(signal, "direction", None) or getattr(signal, "side", None) or "").upper()
            is_short = "SHORT" in direction
            is_long = not is_short

            tp1 = getattr(signal, "tp1", None)
            tp2 = getattr(signal, "tp2", None)
            sl = getattr(signal, "sl", None)

            tp1_f = float(tp1) if tp1 is not None else None
            tp2_f = float(tp2) if tp2 is not None else None
            sl_f = float(sl) if sl is not None else None

            if is_long:
                if tp2_f is not None and price >= tp2_f:
                    return False, "TP2", price
                if tp1_f is not None and price >= tp1_f:
                    return False, "TP1", price
                if sl_f is not None and price <= sl_f:
                    return False, "SL", price
            else:
                if tp2_f is not None and price <= tp2_f:
                    return False, "TP2", price
                if tp1_f is not None and price <= tp1_f:
                    return False, "TP1", price
                if sl_f is not None and price >= sl_f:
                    return False, "SL", price

            return True, "OK", price
        except Exception:
            try:
                price = float(await self._get_price(signal))
            except Exception:
                price = 0.0
            return False, "ERROR", price

    async def track_loop(self, bot) -> None:
        """Main tracker loop. Reads ACTIVE/TP1 trades from PostgreSQL and updates their status."""
        while True:
            # heartbeat for health log (best effort)
            try:
                cb = getattr(self, "health_tick_cb", None)
                if callable(cb):
                    cb()
            except Exception:
                pass
            try:
                rows = await db_store.list_active_trades(limit=500)
            except Exception:
                logger.exception("track_loop: failed to load active trades from DB")
                rows = []

            for row in rows:
                try:
                    trade_id = int(row["id"])
                    uid = int(row["user_id"])
                    market = (row.get("market") or "FUTURES").upper()
                    symbol = str(row.get("symbol") or "")
                    side = str(row.get("side") or "LONG").upper()

                    s = Signal(
                        signal_id=int(row.get("signal_id") or 0),
                        market=market,
                        symbol=symbol,
                        direction=side,
                        timeframe="",
                        entry=float(row.get("entry") or 0.0),
                        sl=float(row.get("sl") or 0.0),
                        tp1=float(row.get("tp1") or 0.0),
                        tp2=float(row.get("tp2") or 0.0),
                        rr=0.0,
                        confidence=0,
                        confirmations="",
                        risk_note="",
                        ts=float(dt.datetime.now(dt.timezone.utc).timestamp()),
                    )

                    # Pre-format TP2 for templates (prevents UnboundLocalError in close messages)
                    try:
                        _tp2v = float(getattr(s, 'tp2', 0.0) or 0.0)
                    except Exception:
                        _tp2v = 0.0
                    tp2_disp = (f"{_tp2v:.6f}" if _tp2v > 0 else "-")

                    try:
                        price_f, price_src = await self._get_price_with_source(s)
                        price_f = float(price_f)
                        # clear failure tracker on success
                        self._price_fail_since.pop(trade_id, None)
                    except PriceUnavailableError as e:
                        now_ts = time.time()
                        first = self._price_fail_since.get(trade_id)
                        if first is None:
                            self._price_fail_since[trade_id] = now_ts
                            first = now_ts
                        close_after_min = float(os.getenv("PRICE_FAIL_CLOSE_MINUTES", "10") or 10)
                        # skip this tick; try again next loop
                        if close_after_min <= 0 or (now_ts - first) < (close_after_min * 60.0):
                            logger.warning("price unavailable: skip tick trade_id=%s %s %s err=%s", trade_id, market, symbol, str(e)[:200])
                            continue

                        # Forced close after N minutes without price
                        minutes = int(close_after_min)
                        txt = _trf(uid, 'msg_price_unavailable_close', minutes=minutes, symbol=symbol, market=market)
                        if txt == 'msg_price_unavailable_close':
                            # fallback if i18n key missing
                            txt = f"⚠️ Price unavailable for {minutes} min. Force-closing trade.\n\n{symbol} | {market}"
                        try:
                            await safe_send(bot, uid, txt, ctx="msg_price_unavailable_close")
                        except Exception:
                            pass
                        # Use entry as best-effort close price
                        close_px = float(s.entry or 0.0)
                        await db_store.close_trade(trade_id, status="CLOSED", price=close_px, pnl_total_pct=float(row.get("pnl_total_pct") or 0.0) if row.get("pnl_total_pct") is not None else 0.0)
                        self._sl_breach_since.pop(trade_id, None)
                        try:
                            if hasattr(self, "_tp1_peak_px"):
                                self._tp1_peak_px.pop(trade_id, None)
                            if hasattr(self, "_be_skip_until"):
                                self._be_skip_until.pop(trade_id, None)
                        except Exception:
                            pass
                        self._price_fail_since.pop(trade_id, None)
                        continue

                    def hit_tp(lvl: float) -> bool:
                        return price_f >= lvl if side == "LONG" else price_f <= lvl

                    def hit_sl(lvl: float) -> bool:
                        return price_f <= lvl if side == "LONG" else price_f >= lvl

                    status = str(row.get("status") or "ACTIVE").upper()
                    tp1_hit = bool(row.get("tp1_hit"))
                    be_price = float(row.get("be_price") or 0.0) if row.get("be_price") is not None else 0.0

                    # After TP1 we move protection SL to BE (entry +/- fee buffer).
                    # The debug block should reflect the *active* protective level, not the original signal SL.
                    be_armed = _be_is_armed(side=side, price=price_f, tp1=(float(s.tp1) if s.tp1 else None), tp2=(float(s.tp2) if s.tp2 else None))
                    effective_sl = (be_price if (tp1_hit and _be_enabled(market) and be_price > 0 and be_armed) else (float(s.sl) if s.sl else None))
                    dbg = ""
                    try:
                        _pd = (os.getenv("PRICE_DEBUG", "0") or "").strip().lower()
                        _price_dbg_on = _pd not in ("", "0", "false", "no", "off")
                    except Exception:
                        _price_dbg_on = False
                    if _price_dbg_on:
                        dbg = _price_debug_block(
                            uid,
                            price=price_f,
                            source=price_src,
                            side=side,
                            sl=effective_sl,
                            tp1=(float(s.tp1) if s.tp1 else None),
                            tp2=(float(s.tp2) if s.tp2 else None),
                            sl_label_key=('lbl_be' if (tp1_hit and be_armed) else 'lbl_sl'),
                        )

                    # 1) Before TP1: TP2 (gap) -> WIN
                    if not tp1_hit and s.tp2 and hit_tp(float(s.tp2)):
                        trade_ctx = UserTrade(user_id=uid, signal=s, tp1_hit=tp1_hit)
                        pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(s.tp2), close_reason="WIN")
                        import datetime as _dt
                        now_utc = _dt.datetime.now(_dt.timezone.utc)
                        rr = _calc_rr_str(float(getattr(s,'entry',0.0) or 0.0), float(getattr(s,'sl',0.0) or 0.0), float(getattr(s,'tp1',0.0) or 0.0), float(getattr(s,'tp2',0.0) or 0.0))
                        after_tp1 = _trf(uid, "after_tp1_suffix") if tp1_hit else ""
                        close_agent = _trf(uid, "close_agent_smart_manager")
                        be_line = (f"🛡 BE: {float(be_price):.6f}\n" if (be_price and float(be_price)>0) else "")
                        _r = _human_close_reason(uid, "TP2_REACHED")
                        reason_line = (_trf(uid, "lbl_reason", reason=_r) + "\n") if _r else ""
                        emoji = "🟢"
                        txt = _trf(uid, "msg_auto_win",
                            symbol=s.symbol,
                            market=market,
                            emoji=emoji,
                            side=str(getattr(s,'direction','') or '').upper() or "LONG",
                            after_tp1=after_tp1,
                            rr=rr,
                            close_agent=close_agent,
                            reason_line=reason_line,
                            entry=f"{float(getattr(s,'entry',0.0) or 0.0):.6f}",
                            sl=f"{float(getattr(s,'sl',0.0) or 0.0):.6f}",
                            tp1=f"{float(getattr(s,'tp1',0.0) or 0.0):.6f}",
                            tp2=tp2_disp,
                            be_line=be_line,
                            pnl_total=fmt_pnl_pct(float(pnl)),
                            opened_time=fmt_dt_msk(row.get("opened_at")),
                            closed_time=fmt_dt_msk(now_utc),
                            status="WIN",
                        )
                        if dbg:
                            txt += "\n\n" + dbg
                        await safe_send(bot, uid, txt, ctx="msg_auto_win")
                        await db_store.close_trade(trade_id, status="WIN", price=float(s.tp2), pnl_total_pct=float(pnl))
                        self._sl_breach_since.pop(trade_id, None)
                        try:
                            if hasattr(self, "_tp1_peak_px"):
                                self._tp1_peak_px.pop(trade_id, None)
                            if hasattr(self, "_be_skip_until"):
                                self._be_skip_until.pop(trade_id, None)
                        except Exception:
                            pass
                        try:
                            await _mid_autotune_update_on_close(market=market, orig_text=str(row.get('orig_text') or ''), timeframe=str(getattr(s,'timeframe','') or ''), tp2_present=bool(s.tp2), hit_tp2=True)
                        except Exception:
                            pass
                        continue

                    # 2) Before TP1: SL -> LOSS
                    if not tp1_hit and s.sl:
                        sl_lvl = float(s.sl)
                        buf = (_SL_BUFFER_PCT / 100.0)
                        breached = (price_f <= sl_lvl * (1 - buf)) if side == "LONG" else (price_f >= sl_lvl * (1 + buf))
                        if breached and _SL_CONFIRM_SEC > 0:
                            t0 = self._sl_breach_since.get(trade_id)
                            if t0 is None:
                                self._sl_breach_since[trade_id] = time.time()
                                continue
                            if (time.time() - t0) < _SL_CONFIRM_SEC:
                                continue
                        else:
                            self._sl_breach_since.pop(trade_id, None)
                            try:
                                if hasattr(self, "_tp1_peak_px"):
                                    self._tp1_peak_px.pop(trade_id, None)
                                if hasattr(self, "_be_skip_until"):
                                    self._be_skip_until.pop(trade_id, None)
                            except Exception:
                                pass
                    else:
                        breached = False

                    if not tp1_hit and s.sl and breached:
                        trade_ctx = UserTrade(user_id=uid, signal=s, tp1_hit=tp1_hit)
                        pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(s.sl), close_reason="LOSS")
                        import datetime as _dt
                        now_utc = _dt.datetime.now(_dt.timezone.utc)
                        rr = _calc_rr_str(float(getattr(s,'entry',0.0) or 0.0), float(getattr(s,'sl',0.0) or 0.0), float(getattr(s,'tp1',0.0) or 0.0), float(getattr(s,'tp2',0.0) or 0.0))
                        after_tp1 = ""
                        close_agent = _trf(uid, "close_agent_smart_manager")
                        be_line = ""
                        _r = _human_close_reason(uid, "SL_REACHED")
                        reason_line = (_trf(uid, "lbl_reason", reason=_r) + "\n") if _r else ""
                        emoji = "🔴"
                        txt = _trf(uid, "msg_auto_loss",
                            symbol=s.symbol,
                            market=market,
                            emoji=emoji,
                            side=str(getattr(s,'direction','') or '').upper() or "LONG",
                            after_tp1=after_tp1,
                            rr=rr,
                            close_agent=close_agent,
                            reason_line=reason_line,
                            entry=f"{float(getattr(s,'entry',0.0) or 0.0):.6f}",
                            tp1=f"{float(getattr(s,'tp1',0.0) or 0.0):.6f}",
                            tp2=tp2_disp,
                            be_line=be_line,
                            pnl_total=fmt_pnl_pct(float(pnl)),
                            sl=f"{float(s.sl):.6f}",
                            opened_time=fmt_dt_msk(row.get("opened_at")),
                            closed_time=fmt_dt_msk(now_utc),
                            status="LOSS",
                        )
                        if dbg:
                            txt += "\n\n" + dbg
                        await safe_send(bot, uid, txt, ctx="msg_auto_loss")
                        await db_store.close_trade(trade_id, status="LOSS", price=float(s.sl), pnl_total_pct=float(pnl))
                        self._sl_breach_since.pop(trade_id, None)
                        try:
                            if hasattr(self, "_tp1_peak_px"):
                                self._tp1_peak_px.pop(trade_id, None)
                            if hasattr(self, "_be_skip_until"):
                                self._be_skip_until.pop(trade_id, None)
                        except Exception:
                            pass
                        try:
                            await _mid_autotune_update_on_close(market=market, orig_text=str(row.get('orig_text') or ''), timeframe=str(getattr(s,'timeframe','') or ''), tp2_present=bool(s.tp2), hit_tp2=False)
                        except Exception:
                            pass
                        continue

                    # 3) TP1 -> partial close + BE
                    if not tp1_hit and s.tp1 and hit_tp(float(s.tp1)):
                        be_px = _be_exit_price(s.entry, side, market)
                        tp2_val = float(getattr(s,'tp2',0.0) or 0.0)
                        tp2_disp = (f"{tp2_val:.6f}" if tp2_val > 0 else "-")
                        sl_ok = "✅" if (float(getattr(s,'sl',0.0) or 0.0) > 0 and hit_sl(float(getattr(s,'sl',0.0) or 0.0))) else "❌"
                        tp1_ok = "✅"
                        tp2_ok = "✅" if (tp2_val > 0 and hit_tp(float(tp2_val))) else "❌"
                        import datetime as _dt
                        now_utc = _dt.datetime.now(_dt.timezone.utc)
                        trade_ctx_tp1 = UserTrade(user_id=uid, signal=s, tp1_hit=True)
                        pnl = _calc_effective_pnl_pct(trade_ctx_tp1, close_price=float(be_px), close_reason="BE")
                        rr = _calc_rr_str(float(getattr(s,'entry',0.0) or 0.0), float(getattr(s,'sl',0.0) or 0.0), float(getattr(s,'tp1',0.0) or 0.0), float(getattr(s,'tp2',0.0) or 0.0))
                        close_agent = _trf(uid, "close_agent_smart_manager")
                        _pct = int(_partial_close_pct(market))
                        reason = _human_close_reason(uid, "TP1_PARTIAL", closed_pct=_pct)
                        reason_line = (_trf(uid, "lbl_reason", reason=reason) + "\n") if reason else ""
                        txt = _trf(uid, "msg_auto_tp1",
                            symbol=s.symbol,
                            market=market,
                            side=str(getattr(s,'direction','') or '').upper() or "LONG",
                            rr=rr,
                            close_agent=close_agent,
                            reason_line=reason_line,
                            entry=f"{float(getattr(s,'entry',0.0) or 0.0):.6f}",
                            sl=f"{float(getattr(s,'sl',0.0) or 0.0):.6f}",
                            tp1=f"{float(getattr(s,'tp1',0.0) or 0.0):.6f}",
                            tp2=tp2_disp,
                            pnl_total=fmt_pnl_pct(float(pnl)),
                            closed_pct=int(_partial_close_pct(market)),
                            be_price=f"{float(be_px):.6f}",
                            price_now=f"{float(price_f):.6f}",
                            price_source=str(price_src),
                            sl_ok=sl_ok,
                            tp1_ok=tp1_ok,
                            tp2_ok=tp2_ok,
                            opened_time=fmt_dt_msk(row.get("opened_at")),
                            event_time=fmt_dt_msk(now_utc),
                            status="TP1",
                        )
                        if dbg:
                            txt += "\n\n" + dbg
                        await safe_send(bot, uid, txt, ctx="msg_auto_tp1")
                        # TP1 event: store FIXED PnL (partial TP1 close) so cards show correct 'зафиксировано'
                        _p = max(0.0, min(100.0, float(_tp1_partial_close_pct(s.market))))
                        _has_tp2 = bool(getattr(s, 'tp2', 0.0) or 0.0)
                        _full_tp1 = float(calc_profit_pct(s.entry, float(s.tp1), side))
                        _fixed_tp1 = (_full_tp1 * (_p/100.0)) if _has_tp2 else _full_tp1
                        await db_store.set_tp1(trade_id, be_price=float(be_px), price=float(s.tp1), pnl_pct=float(_fixed_tp1))
                        continue

                    # 3) After TP1: emergency hard SL (always) + BE logic (optional)
                    if tp1_hit:
                        entry_p = float(s.entry or 0.0)
                        be_lvl = be_price if be_price else _be_exit_price(entry_p, side, market)

                        # Emergency hard SL (always active after TP1): prevents deep drawdown if price collapses,
                        # even if BE is being held/disabled.
                        try:
                            hard_pct = float(os.getenv("SMART_HARD_SL_PCT", "2.8") or 0.0)
                        except Exception:
                            hard_pct = 0.0
                        hard_sl = 0.0
                        if entry_p > 0 and hard_pct > 0:
                            hard_sl = (entry_p * (1 - hard_pct / 100.0)) if side == "LONG" else (entry_p * (1 + hard_pct / 100.0))

                        if hard_sl > 0 and hit_sl(float(hard_sl)):
                            import datetime as _dt
                            now_utc = _dt.datetime.now(_dt.timezone.utc)
                            trade_ctx = UserTrade(user_id=uid, signal=s, tp1_hit=tp1_hit)
                            pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(hard_sl), close_reason="HARD_SL")
                            rr = _calc_rr_str(float(getattr(s,'entry',0.0) or 0.0), float(getattr(s,'sl',0.0) or 0.0), float(getattr(s,'tp1',0.0) or 0.0), float(getattr(s,'tp2',0.0) or 0.0))
                            after_tp1 = _trf(uid, "after_tp1_suffix")
                            close_agent = _trf(uid, "close_agent_smart_manager")
                            reason = _human_close_reason(uid, "SMART_HARD_SL", hard_pct=f"{float(hard_pct):.2f}")
                            # Optional tech details via SMART_REASON_TECH_DETAILS=1
                            tech = ""
                            try:
                                if os.getenv("SMART_REASON_TECH_DETAILS", "0").strip() not in ("0","false","no","off"):
                                    tech = " " + _trf(uid, "smart_hard_sl_reason", hard_pct=f"{float(hard_pct):.2f}")
                            except Exception:
                                tech = ""
                            reason = (str(reason) + str(tech)).strip()
                            reason_line = (_trf(uid, "lbl_reason", reason=reason) + "\n") if reason else ""
                            be_line = f"🛡 BE: {float(be_lvl):.6f}\n"
                            emoji = "🟠"
                            txt = _trf(uid, "msg_auto_loss",
                                symbol=s.symbol,
                                market=market,
                                emoji=emoji,
                                side=str(getattr(s,'direction','') or '').upper() or "LONG",
                                after_tp1=after_tp1,
                                rr=rr,
                                close_agent=close_agent,
                                reason_line=reason_line,
                                entry=f"{float(getattr(s,'entry',0.0) or 0.0):.6f}",
                                tp1=f"{float(getattr(s,'tp1',0.0) or 0.0):.6f}",
                                tp2=tp2_disp,
                                be_line=be_line,
                                pnl_total=fmt_pnl_pct(float(pnl)),
                                sl=f"{float(hard_sl):.6f}",
                                opened_time=fmt_dt_msk(row.get("opened_at")),
                                closed_time=fmt_dt_msk(now_utc),
                                status="HARD_SL",
                            )
                            if dbg:
                                txt += "\n\n" + dbg
                            await safe_send(bot, uid, txt, ctx="msg_auto_loss")
                            # pnl already computed above

                            await db_store.close_trade(trade_id, status="HARD_SL", price=float(hard_sl), pnl_total_pct=float(pnl))
                            self._sl_breach_since.pop(trade_id, None)
                            try:
                                if hasattr(self, "_tp1_peak_px"):
                                    self._tp1_peak_px.pop(trade_id, None)
                                if hasattr(self, "_be_skip_until"):
                                    self._be_skip_until.pop(trade_id, None)
                            except Exception:
                                pass
                            try:
                                await _mid_autotune_update_on_close(market=market, orig_text=str(row.get('orig_text') or ''), timeframe=str(getattr(s,'timeframe','') or ''), tp2_present=bool(s.tp2), hit_tp2=False)
                            except Exception:
                                pass
                            continue

                        # BE logic is optional and controlled by env flags, but HARD SL above is always active.
                        if _be_enabled(market):
                            # Track best favorable excursion after TP1 to estimate "chance to still reach TP2".
                            if not hasattr(self, "_tp1_peak_px"):
                                self._tp1_peak_px = {}
                            peak = float(self._tp1_peak_px.get(trade_id, 0.0) or 0.0)
                            if side == "LONG":
                                peak = max(peak, float(price_f))
                            else:
                                peak = float(price_f) if peak <= 0 else min(peak, float(price_f))
                            self._tp1_peak_px[trade_id] = float(peak)

                            # Optional cooldown when we decide to "hold despite BE touch" to avoid flip-flopping.
                            if not hasattr(self, "_be_skip_until"):
                                self._be_skip_until = {}
                            now_ts = time.time()
                            if now_ts < float(self._be_skip_until.get(trade_id, 0.0) or 0.0):
                                # During cooldown we do not close at BE (but HARD SL is still active above).
                                pass
                            else:
                                be_armed_now = _be_is_armed(side=side, price=price_f, tp1=getattr(s,'tp1',None), tp2=getattr(s,'tp2',None))
                                if be_armed_now and hit_sl(float(be_lvl)):
                                    # Estimate probability to still reach TP2 based on progress (peak vs TP2) and pullback from peak.
                                    prob_tp2 = 0.0
                                    tp2_lvl = float(getattr(s, "tp2", 0.0) or 0.0)
                                    if tp2_lvl > 0 and entry_p > 0 and tp2_lvl != entry_p:
                                        if side == "LONG":
                                            prog = (peak - entry_p) / (tp2_lvl - entry_p)
                                            pullback = ((peak - price_f) / max(1e-9, peak)) * 100.0 if peak > 0 else 0.0
                                        else:
                                            prog = (entry_p - peak) / (entry_p - tp2_lvl)
                                            pullback = ((price_f - peak) / max(1e-9, peak)) * 100.0 if peak > 0 else 0.0
                                        prog = max(0.0, min(1.0, float(prog)))
                                        try:
                                            pb_cap = float(os.getenv("SMART_BE_PULLBACK_PCT", "1.2") or 1.2)
                                        except Exception:
                                            pb_cap = 1.2
                                        pb_score = max(0.0, min(1.0, 1.0 - (max(0.0, float(pullback)) / max(0.05, float(pb_cap)))))
                                        prob_tp2 = max(0.0, min(1.0, 0.70 * prog + 0.30 * pb_score))

                                    try:
                                        hold_prob = float(os.getenv("SMART_BE_HOLD_PROB", "0.70") or 0.70)
                                    except Exception:
                                        hold_prob = 0.70
                                    try:
                                        skip_sec = float(os.getenv("SMART_BE_SKIP_COOLDOWN_SEC", "45") or 45)
                                    except Exception:
                                        skip_sec = 45.0

                                    if prob_tp2 >= hold_prob:
                                        # Strong chance: keep position alive, ignore this BE touch for a while.
                                        self._be_skip_until[trade_id] = float(now_ts + max(1.0, float(skip_sec)))
                                    else:
                                        import datetime as _dt
                                        now_utc = _dt.datetime.now(_dt.timezone.utc)
                                        # Build Smart BE close reason
                                        _prog = float(prog) if 'prog' in locals() else 0.0
                                        _pullback = float(pullback) if 'pullback' in locals() else 0.0
                                        # pnl already computed above

                                        rr = _calc_rr_str(float(getattr(s,'entry',0.0) or 0.0), float(getattr(s,'sl',0.0) or 0.0), float(getattr(s,'tp1',0.0) or 0.0), float(getattr(s,'tp2',0.0) or 0.0))
                                        after_tp1 = _trf(uid, "after_tp1_suffix")
                                        close_agent = _trf(uid, "close_agent_smart_be")
                                        # Human-friendly reason + optional technical details (for admins)
                                        reason = _human_close_reason(uid, "SMART_BE")
                                        # You can enable technical details via ENV SMART_REASON_TECH_DETAILS=1
                                        tech = ""
                                        try:
                                            if os.getenv("SMART_REASON_TECH_DETAILS", "0").strip() not in ("0","false","no","off"):
                                                tech = " " + _trf(uid, "smart_be_reason", prob=f"{float(prob_tp2):.2f}", hold=f"{float(hold_prob):.2f}", prog=f"{_prog:.2f}", pullback=f"{_pullback:.2f}")
                                        except Exception:
                                            tech = ""
                                        reason = (str(reason) + str(tech)).strip()
                                        reason_line = (_trf(uid, "lbl_reason", reason=reason) + "\n") if reason else ""
                                        be_line = f"🛡 BE: {float(be_lvl):.6f}\n"
                                        emoji = "🟡"
                                        txt = _trf(uid, "msg_auto_be",
                                            symbol=s.symbol,
                                            market=market,
                                            emoji=emoji,
                                            side=str(getattr(s,'direction','') or '').upper() or "LONG",
                                            after_tp1=after_tp1,
                                            rr=rr,
                                            close_agent=close_agent,
                                            reason_line=reason_line,
                                            entry=f"{float(getattr(s,'entry',0.0) or 0.0):.6f}",
                                            sl=f"{float(getattr(s,'sl',0.0) or 0.0):.6f}",
                                            tp1=f"{float(getattr(s,'tp1',0.0) or 0.0):.6f}",
                                            tp2=tp2_disp,
                                            be_line=be_line,
                                            pnl_total=fmt_pnl_pct(float(pnl)),
                                            be_price=f"{float(be_lvl):.6f}",
                                            opened_time=fmt_dt_msk(row.get("opened_at")),
                                            closed_time=fmt_dt_msk(now_utc),
                                            status="BE",
                                        )
                                        if dbg:
                                            txt += "\n\n" + dbg
                                        await safe_send(bot, uid, txt, ctx="msg_auto_be")
                                        trade_ctx = UserTrade(user_id=uid, signal=s, tp1_hit=tp1_hit)
                                        pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(be_lvl), close_reason="BE")
                                        await db_store.close_trade(trade_id, status="BE", price=float(be_lvl), pnl_total_pct=float(pnl))
                                        self._sl_breach_since.pop(trade_id, None)
                                        try:
                                            if hasattr(self, "_tp1_peak_px"):
                                                self._tp1_peak_px.pop(trade_id, None)
                                            if hasattr(self, "_be_skip_until"):
                                                self._be_skip_until.pop(trade_id, None)
                                        except Exception:
                                            pass
                                        try:
                                            await _mid_autotune_update_on_close(market=market, orig_text=str(row.get('orig_text') or ''), timeframe=str(getattr(s,'timeframe','') or ''), tp2_present=bool(s.tp2), hit_tp2=False)
                                        except Exception:
                                            pass
                                        continue

                    # 4) TP2 -> WIN

                    if s.tp2 and hit_tp(float(s.tp2)):
                        trade_ctx = UserTrade(user_id=uid, signal=s, tp1_hit=tp1_hit)
                        pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(s.tp2), close_reason="WIN")
                        import datetime as _dt
                        now_utc = _dt.datetime.now(_dt.timezone.utc)
                        rr = _calc_rr_str(float(getattr(s,'entry',0.0) or 0.0), float(getattr(s,'sl',0.0) or 0.0), float(getattr(s,'tp1',0.0) or 0.0), float(getattr(s,'tp2',0.0) or 0.0))
                        after_tp1 = _trf(uid, "after_tp1_suffix") if tp1_hit else ""
                        close_agent = _trf(uid, "close_agent_smart_manager")
                        be_line = (f"🛡 BE: {float(be_price):.6f}\n" if (be_price and float(be_price)>0) else "")
                        _r = _human_close_reason(uid, "TP2_REACHED")
                        reason_line = (_trf(uid, "lbl_reason", reason=_r) + "\n") if _r else ""
                        emoji = "🟢"
                        txt = _trf(uid, "msg_auto_win",
                            symbol=s.symbol,
                            market=market,
                            emoji=emoji,
                            side=str(getattr(s,'direction','') or '').upper() or "LONG",
                            after_tp1=after_tp1,
                            rr=rr,
                            close_agent=close_agent,
                            reason_line=reason_line,
                            entry=f"{float(getattr(s,'entry',0.0) or 0.0):.6f}",
                            sl=f"{float(getattr(s,'sl',0.0) or 0.0):.6f}",
                            tp1=f"{float(getattr(s,'tp1',0.0) or 0.0):.6f}",
                            tp2=tp2_disp,
                            be_line=be_line,
                            pnl_total=fmt_pnl_pct(float(pnl)),
                            opened_time=fmt_dt_msk(row.get("opened_at")),
                            closed_time=fmt_dt_msk(now_utc),
                            status="WIN",
                        )
                        if dbg:
                            txt += "\n\n" + dbg
                        await safe_send(bot, uid, txt, ctx="msg_auto_win")
                        await db_store.close_trade(trade_id, status="WIN", price=float(s.tp2), pnl_total_pct=float(pnl))
                        self._sl_breach_since.pop(trade_id, None)
                        try:
                            if hasattr(self, "_tp1_peak_px"):
                                self._tp1_peak_px.pop(trade_id, None)
                            if hasattr(self, "_be_skip_until"):
                                self._be_skip_until.pop(trade_id, None)
                        except Exception:
                            pass
                        continue

                except Exception:
                    logger.exception("track_loop: error while tracking trade row=%r", row)

            await asyncio.sleep(TRACK_INTERVAL_SECONDS)

    async def scanner_loop(self, emit_signal_cb, emit_macro_alert_cb) -> None:
        while True:
            start = time.time()
            try:
                _mid_hardblock_reset_tick()
            except Exception:
                pass
            logger.info("SCAN tick start top_n=%s interval=%ss news_filter=%s macro_filter=%s", TOP_N, SCAN_INTERVAL_SECONDS, bool(NEWS_FILTER and CRYPTOPANIC_TOKEN), bool(MACRO_FILTER))
            logger.info("[scanner] tick start TOP_N=%s interval=%ss", TOP_N, SCAN_INTERVAL_SECONDS)
            self.last_scan_ts = start
            try:
                async with MultiExchangeData() as api:
                    await self.macro.ensure_loaded(api.session)  # type: ignore[arg-type]

                    try:
                        symbols = await api.get_top_usdt_symbols(TOP_N * 3)  # FIX: fetch extra for pre-filter
                        # FIX: remove blocked/stable symbols BEFORE TOP_N slicing
                        symbols = [s for s in symbols if not is_blocked_symbol(s)]
                        symbols = symbols[:TOP_N]
                        if symbols:
                            self._symbols_cache = list(symbols)
                            self._symbols_cache_ts = time.time()
                    except Exception as e:
                        # If Binance (or other primary) is unreachable (DNS/ban), fall back to cached list.
                        if self._symbols_cache:
                            logger.warning("[scanner] get_top_usdt_symbols failed (%s); using cached symbols (%s)", e, len(self._symbols_cache))
                            symbols = list(self._symbols_cache)
                        else:
                            raise
                    self.scanned_symbols_last = len(symbols)
                    logger.info("[main][scanner] symbols loaded: %s (TOP_N=%s)", self.scanned_symbols_last, TOP_N)

                    # reject stats (per tick) — used only for logging/debug; never crash if missing
                    _rej_counts = defaultdict(int)
                    _rej_reasons_by_sym = defaultdict(list)  # sym -> [base_reason...]

                    def _rej_add(_sym: str, _reason: str) -> None:
                        """Collect reject reason (for later summary breakdown)."""
                        try:
                            _r = str(_reason or "").strip() or "unknown"
                            _rej_counts[_r] += 1

                            # Normalize to base reason names expected in summary
                            _map = {
                                "score": "score_low",
                                "rr": "rr_low",
                                "confidence": "confidence_low",
                            }
                            _base = _map.get(_r, _r)
                            # Keep it short/clean
                            if len(_base) > 64:
                                _base = _base[:64]
                            _rej_reasons_by_sym[str(_sym)].append(_base)
                        except Exception:
                            pass

                    mac_act, mac_ev, mac_win = self.macro.current_action()
                    self.last_macro_action = mac_act
                    if MACRO_FILTER:
                        logger.info("[scanner] macro action=%s next=%s window=%s", mac_act, getattr(mac_ev, "name", None) if mac_ev else None, mac_win)

                    if mac_act != "ALLOW" and mac_ev and mac_win and self.macro.should_notify(mac_ev):
                        logger.info("[macro] alert: action=%s event=%s window=%s", mac_act, getattr(mac_ev, "name", None), mac_win)
                        await emit_macro_alert_cb(mac_act, mac_ev, mac_win, TZ_NAME)

                    for sym in symbols:
                        if not self.can_emit(sym):
                            continue
                        if is_blocked_symbol(sym):
                            logger.info("[scanner] skip blocked stable pair: %s", sym)
                            continue
                        if mac_act == "PAUSE_ALL":
                            continue

                        # News action
                        try:
                            if self.news.enabled():
                                news_act = await self.news.action_for_symbol(api.session, sym)  # type: ignore[arg-type]
                            else:
                                news_act = "ALLOW"
                        except Exception:
                            news_act = "ALLOW"
                        self.last_news_action = news_act
                        if news_act == "PAUSE_ALL":
                            continue

                        async def fetch_exchange(name: str):
                            try:
                                if name == "BINANCE":
                                    df15 = await api.klines_binance(sym, "15m", 250)
                                    df1h = await api.klines_binance(sym, "1h", 250)
                                    df4h = await api.klines_binance(sym, "4h", 250)
                                elif name == "BYBIT":
                                    df15 = await api.klines_bybit(sym, "15m", 200)
                                    df1h = await api.klines_bybit(sym, "1h", 200)
                                    df4h = await api.klines_bybit(sym, "4h", 200)
                                elif name == "OKX":
                                    df15 = await api.klines_okx(sym, "15m", 200)
                                    df1h = await api.klines_okx(sym, "1h", 200)
                                    df4h = await api.klines_okx(sym, "4h", 200)
                                elif name == "GATEIO":
                                    df15 = await api.klines_gateio(sym, "15m", 200)
                                    df1h = await api.klines_gateio(sym, "1h", 200)
                                    df4h = await api.klines_gateio(sym, "4h", 200)
                                else:  # MEXC
                                    df15 = await api.klines_mexc(sym, "15m", 200)
                                    df1h = await api.klines_mexc(sym, "1h", 200)
                                    df4h = await api.klines_mexc(sym, "4h", 200)
                                res = evaluate_on_exchange(df15, df1h, df4h)
                                return name, res
                            except Exception:
                                return name, None

                        # Exchanges to scan (independent from ORDERBOOK_EXCHANGES)
                        _scan_ex = (os.getenv('SCANNER_EXCHANGES','BINANCE,BYBIT,OKX,GATEIO,MEXC') or '').strip()
                        scan_exchanges = [x.strip().upper() for x in _scan_ex.split(',') if x.strip()]
                        if not scan_exchanges:
                            scan_exchanges = ['BINANCE','BYBIT','OKX','MEXC','GATEIO']

                        # --- Choose best exchange candidate (MAIN scanner) ---
                        results = await asyncio.gather(*[fetch_exchange(x) for x in scan_exchanges])
                        best_name = None
                        best_r = None
                        best_score = -1.0
                        for _name, _res in results:
                            if not _res:
                                continue
                            try:
                                _c = float(_res.get("confidence", 0) or 0)
                            except Exception:
                                _c = 0.0
                            if _c > best_score:
                                best_score = _c
                                best_name = _name
                                best_r = _res

                        if not best_r or not best_name:
                            _rej_add(sym, "no_candidate")
                            continue

                        best_dir = str(best_r.get("direction") or "").upper()
                        entry = float(best_r.get("entry") or 0.0)
                        sl = float(best_r.get("sl") or 0.0)
                        tp1 = float(best_r.get("tp1") or 0.0)
                        tp2 = float(best_r.get("tp2") or 0.0)
                        rr = float(best_r.get("rr") or 0.0)
                        conf = int(round(float(best_r.get("confidence", 0) or 0)))

                        # Market: choose FUTURES only for stronger trend+volatility, otherwise SPOT
                        try:
                            market = choose_market(float(best_r.get("adx1", 0) or 0), float(best_r.get("atr_pct", 0) or 0))
                        except Exception:
                            market = "SPOT"

                        # Risk/TA notes (human-readable)
                        risk_note = _fmt_ta_block(best_r)

                        # SPOT+SHORT is not executable -> convert to FUTURES unless Futures are forced off by news/macro
                        if market == "SPOT" and best_dir == "SHORT":
                            if news_act == "FUTURES_OFF" or mac_act == "FUTURES_OFF":
                                _rej_add(sym, "futures_forced_off")
                                continue
                            market = "FUTURES"
                            risk_note = (risk_note + "\n" if risk_note else "") + "ℹ️ Auto-converted: SPOT SHORT → FUTURES"

                        risk_notes = [risk_note] if risk_note else []
                        async def _pair_exists(ex: str) -> bool:
                            try:
                                exu = (ex or "").upper().strip()

                                # FUTURES confirmations must be executable futures markets only
                                if market == "FUTURES":
                                    if exu == "BINANCE":
                                        p = await self._fetch_rest_price("FUTURES", sym)
                                    elif exu == "BYBIT":
                                        p = await self._fetch_bybit_price("FUTURES", sym)
                                    elif exu == "OKX":
                                        p = await self._fetch_okx_price("FUTURES", sym)
                                    else:
                                        return False
                                    return bool(p and float(p) > 0)

                                # SPOT confirmations (supports all 5 exchanges)
                                if exu == "BINANCE":
                                    p = await self._fetch_rest_price("SPOT", sym)
                                elif exu == "BYBIT":
                                    p = await self._fetch_bybit_price("SPOT", sym)
                                elif exu == "OKX":
                                    p = await self._fetch_okx_price("SPOT", sym)
                                elif exu == "MEXC":
                                    p = await _mexc_public_price(sym)
                                else:  # GATEIO
                                    p = await _gateio_public_price(sym)
                                return bool(p and float(p) > 0)
                            except Exception:
                                return False

                        
                        # --- HARD BLOCK: FUTURES signal must have a real futures contract on executable venues ---
                        # If choose_market() selected FUTURES but the symbol has no futures instrument on
                        # Binance/Bybit/OKX, we skip emitting this signal. (No auto-downgrade to SPOT.)
                        if market == "FUTURES":
                            try:
                                _fut_ok = await asyncio.gather(_pair_exists("BINANCE"), _pair_exists("BYBIT"), _pair_exists("OKX"))
                                if not any(bool(x) for x in _fut_ok):
                                    logger.info("[scanner] skip %s FUTURES: no contract on BINANCE/BYBIT/OKX (best_source=%s)", sym, best_name)
                                    _rej_add(sym, "no_futures_contract")
                                    continue
                            except Exception as _e:
                                logger.info("[scanner] skip %s FUTURES: futures existence check failed: %s", sym, _e)
                                _rej_add(sym, "futures_existence_check_failed")
                                continue

# Exchanges where the instrument exists for the given market.
                        # FUTURES: only executable futures venues (Binance/Bybit/OKX)
                        # SPOT: full supported set (Binance/Bybit/OKX/GateIO/MEXC)
                        _ex_order = ["BINANCE", "BYBIT", "OKX"] if market == "FUTURES" else ["BINANCE", "BYBIT", "OKX", "GATEIO", "MEXC"]
                        _oks = await asyncio.gather(*[_pair_exists(x) for x in _ex_order])
                        _pair_exchanges = [x for x, ok in zip(_ex_order, _oks) if ok]
                        if not _pair_exchanges:
                            _pair_exchanges = [best_name]
                        conf_names = "+".join(_pair_exchanges)


                        sid = self.next_signal_id()
                        sig = Signal(
                            signal_id=sid,
                            market=market,
                            symbol=sym,
                            direction=best_dir,
                            timeframe="15m/1h/4h",
                            entry=entry,
                            sl=sl,
                            tp1=tp1,
                            tp2=tp2,
                            rr=rr,
                            confidence=conf,
                            confirmations=conf_names,
                            source_exchange=best_name,
                            available_exchanges=conf_names,
                            risk_note="\\n".join(risk_notes).strip(),
                            ts=time.time(),
                        )

                        self.mark_emitted(sym)
                        self.last_signal = sig
                        if sig.market == "SPOT":
                            self.last_spot_signal = sig
                        else:
                            self.last_futures_signal = sig

                        logger.info("[scan][emit] %s market=%s dir=%s conf=%s rr=%.2f notes=%s", sig.symbol, sig.market, sig.direction, sig.confidence, float(sig.rr), (sig.risk_note or "-")[:120])
                        try:
                            logger.info("[mid][emit] %s market=%s dir=%s conf=%s rr=%.2f exch=%s notes=%s entry=%.6g",
                                        sym, sig.market, str(sig.direction).upper(), int(sig.confidence), float(sig.rr), str(sig.source_exchange), str(sig.risk_note or ""), float(sig.entry))
                        except Exception:
                            pass
                        await emit_signal_cb(sig)
                        _mid_emitted += 1
                        await asyncio.sleep(2)

            except Exception:
                logger.exception("scanner_loop error")

            elapsed = time.time() - start
            logger.info("SCAN tick done scanned=%s elapsed=%.1fs last_news=%s last_macro=%s", int(getattr(self, "scanned_symbols_last", 0) or 0), elapsed, getattr(self, "last_news_action", "?"), getattr(self, "last_macro_action", "?"))
            await asyncio.sleep(max(5, SCAN_INTERVAL_SECONDS - elapsed))


# ------------------ Auto-trade diagnostics (admin) ------------------


    def can_emit_mid(self, symbol: str) -> bool:
        cooldown_min = int(os.getenv("MID_COOLDOWN_MINUTES", "180"))
        m = getattr(self, "_last_emit_mid", None)
        if m is None:
            self._last_emit_mid = {}
            m = self._last_emit_mid
        ts = m.get(symbol)
        return ts is None or (time.time() - float(ts)) >= cooldown_min * 60

    def mark_emitted_mid(self, symbol: str) -> None:
        m = getattr(self, "_last_emit_mid", None)
        if m is None:
            self._last_emit_mid = {}
            m = self._last_emit_mid
        m[symbol] = time.time()


    # ---------------- MID trap/blocked digest (anti-spam analytics) ----------------
    def _mid_reason_key(self, reason: str) -> str:
        r = (reason or "").strip()
        if not r:
            return "unknown"
        # take token before first space; also strip trailing punctuation
        key = r.split()[0].strip()
        return key or "unknown"

    def _mid_digest_add(self, stats: dict, symbol: str, direction: str, entry, reason: str) -> None:
        key = self._mid_reason_key(reason)
        ent = stats.setdefault(key, {"count": 0, "LONG": 0, "SHORT": 0, "examples": []})
        ent["count"] += 1
        d = (direction or "").upper()
        if d in ("LONG", "SHORT"):
            ent[d] += 1

        # keep a few compact examples: "FILUSDT SHORT entry=0.02341 late_entry atr>2.2"
        max_ex = int(os.getenv("MID_TRAP_DIGEST_EXAMPLES", "2") or 2)
        if max_ex <= 0 or len(ent["examples"]) >= max_ex:
            return

        sym = (symbol or "").strip() or "—"
        try:
            en_txt = f"{float(entry):.6g}" if entry is not None else "—"
        except Exception:
            en_txt = "—"
        r = (reason or "").strip()
        ent["examples"].append(f"{sym} {d} entry={en_txt} {r}".strip())


    async def _mid_digest_maybe_send(self, stats: dict, last_sent_at: float) -> float:
        period = int(os.getenv("MID_TRAP_DIGEST_SEC", "21600") or 21600)
        period = max(period, 21600)  # force >= 6 hours
        if period <= 0:
            return last_sent_at
        now = time.time()
        if (now - last_sent_at) < period:
            return last_sent_at

        total = sum(int(v.get("count", 0) or 0) for v in stats.values())
        if total <= 0:
            return now  # reset window even if empty

        top_n = int(os.getenv("MID_TRAP_DIGEST_TOP", "5") or 5)
        # sort by count desc
        items = sorted(stats.items(), key=lambda kv: int(kv[1].get("count", 0) or 0), reverse=True)[:max(1, top_n)]

        # pretty header like "6h" (or more)
        if period % 60 == 0:
            mins = period // 60
            win = f"{mins}m"
        else:
            win = f"{period}s"

        lines = [f"🧪 MID trap digest ({win}) — blocks: {total}", ""]
        for k, v in items:
            cnt = int(v.get("count", 0) or 0)
            lines.append(f"• {k}: {cnt}")
            exs = v.get("examples") or []
            for ex in exs:
                lines.append(f"  - {ex}")
            lines.append("")

        if len(stats) > len(items):
            lines.append(f"… +{len(stats)-len(items)} other reasons")

        text = "\n".join(lines).strip()
        try:
            emit = getattr(self, "emit_mid_digest", None)
            if callable(emit):
                await emit(text)
        except Exception:
            pass
        stats.clear()
        return now



    async def scanner_loop_mid(self, emit_signal_cb, emit_macro_alert_cb) -> None:
        tf_trigger, tf_mid, tf_trend = "5m", "30m", "1h"

        # --- MID candles market selection ---
        # IMPORTANT (production hardening):
        # User requested: "свечи берем только из SPOT и эти свечи для FUTURES тоже используем".
        # We force MID candles to be fetched from SPOT only.
        # (Signals can still be FUTURES — we just use SPOT klines for TA.)
        market_mode = (os.getenv('MID_CANDLES_MARKET_MODE', 'AUTO') or 'AUTO').upper().strip()
        allow_fut = str(os.getenv('MID_ALLOW_FUTURES', '0') or '0').strip().lower() not in ('0','false','no','off')
        if market_mode == 'AUTO':
            markets_try = ['FUTURES','SPOT'] if allow_fut else ['SPOT']
        elif market_mode in ('FUTURES','SWAP','PERP','PERPS'):
            markets_try = ['FUTURES']
        else:
            markets_try = ['SPOT']
        # default/primary market used for prefetch
        market_mid = markets_try[0] if markets_try else 'SPOT'

        # Hard timeout for a whole MID tick (prevents 200s+ overruns).
        # 0 = disabled.
        mid_tick_timeout_sec = _parse_seconds_env('MID_TICK_TIMEOUT_SEC', 0.0)

        # Soft timeout per symbol (skips symbols whose data fetch is too slow).
        # 0 = disabled.
        mid_symbol_timeout_sec = _parse_seconds_env('MID_SYMBOL_TIMEOUT_SEC', 8.0)
        mid_tick_budget_sec = _parse_seconds_env('MID_TICK_BUDGET_SEC', 35.0)

        while True:
            interval_sec = int(os.getenv("MID_SCAN_INTERVAL_SECONDS", "45") or 45)
            async def _mid_tick_body():
                start_total = time.time()
                start = start_total
                start_scan = start_total
                prefetch_elapsed = 0.0
                # --- Candles diagnostics (per tick) ---
                _mid_diag_enabled = str(os.getenv('MID_CANDLES_LOG_DIAG', os.getenv('MID_CANDLES_LOG_FAIL', '1')) or '1').strip().lower() not in ('0','false','no','off')
                _mid_diag_max = int(os.getenv('MID_CANDLES_DIAG_MAX', '200') or 200)
                _mid_diag_lines = 0
                _mid_candles_diag = defaultdict(list)  # (symbol, tf) -> ["EX:MARKET:STATUS(:reason)", ...]
                _mid_candles_diag_seen = defaultdict(set)  # (symbol, tf) -> {rec,...} (dedupe per tick)

                _mid_log_candles_short = os.getenv("MID_LOG_CANDLES_SHORT", "1").strip().lower() in ("1","true","yes","on")
                _mid_candles_short_seen = set()  # {(symbol, tf)} per tick
                # Prefill logs are very noisy in production. Default OFF.
                # Enable explicitly via MID_LOG_PREFILL=1 when debugging.
                _mid_log_prefill = os.getenv("MID_LOG_PREFILL", "0").strip().lower() in ("1","true","yes","on")
                # If you still want to see successful prefill lines, enable MID_LOG_PREFILL_OK=1.
                _mid_log_prefill_ok = os.getenv("MID_LOG_PREFILL_OK", "0").strip().lower() in ("1","true","yes","on")

                def _mid_diag_add(symb: str, ex_name: str, market: str, tf: str, status: str, reason: str = '') -> None:
                    nonlocal _mid_diag_lines
                    try:
                        key = (symb, tf)
                        rec = f"{ex_name}:{(market or 'SPOT').upper().strip()}:{status}" + (f":{reason}" if reason else '')
                        # Dedupe per tick: prevents storms like unsupported_cached repeated many times in diag.
                        if rec in _mid_candles_diag_seen[key]:
                            return
                        _mid_candles_diag_seen[key].add(rec)
                        _mid_candles_diag[key].append(rec)
                        # Optional: log once per symbol/tf when candles are present but too short for indicators.
                        try:
                            if status == "partial" and _mid_log_candles_short and os.getenv("MID_LOG_CANDLES_SHORT_EARLY", "0").strip().lower() in ("1","true","yes","on"):
                                k2 = (symb, tf)
                                if k2 not in _mid_candles_short_seen:
                                    _mid_candles_short_seen.add(k2)
                                    logger.info("[mid][candles_short] symbol=%s tf=%s ex=%s market=%s %s", symb, tf, ex_name, (market or "SPOT"), reason)
                        except Exception:
                            pass
                        if _mid_diag_enabled and _mid_diag_lines < _mid_diag_max and status != 'OK':
                            # Collect diagnostics; actual logging happens once per symbol when candles are finally unavailable.
                            _mid_diag_lines += 1
                    except Exception:
                        pass

                if os.getenv("MID_SCANNER_ENABLED", "1").strip().lower() in ("0","false","no"):
                    await asyncio.sleep(10)
                    return

                # Tick-local log guard
                _MID_TICK_CTX.set({
                    'block': set(),
                    'trap': set(),
                    'candles_missing': set(),
                    'candles_missing_sym': set(),
                    'prefill_skip': set(),
                    'prefill_err': set(),
                    'prefill_empty': set(),
                    'suppress_block': 0,
                    'suppress_trap': 0,
                })
                try:
                    _mid_adapt_reset_tick()
                except Exception:
                    pass


                interval = interval_sec
                top_n = int(os.getenv("MID_TOP_N", "50"))
                top_n_symbols = int(os.getenv("MID_TOP_N_SYMBOLS", str(top_n)) or top_n)
                # MID_TOP_N_SYMBOLS defines the *universe* size to load (e.g. 200).
                # MID_TOP_N defines how many of that universe to actually scan each tick (e.g. 70).
                if top_n_symbols < top_n:
                    top_n_symbols = top_n

                # --- MID trap digest state (persists across ticks) ---
                if not hasattr(self, "_mid_trap_digest_stats"):
                    self._mid_trap_digest_stats = {}
                if not hasattr(self, "_mid_trap_digest_last_sent"):
                    self._mid_trap_digest_last_sent = time.time()

                mode = (os.getenv("MID_SIGNAL_MODE","").strip().lower()
                        or os.getenv("SIGNAL_MODE","").strip().lower()
                        or "strict")
                if os.getenv("MID_STRICT","0").strip() in ("1","true","yes"):
                    mode = "strict"

                # 1:1 thresholds with MAIN scanner by default
                use_main = os.getenv("MID_USE_MAIN_THRESHOLDS","1").strip().lower() not in ("0","false","no")
                if use_main:
                    min_score_spot = int(globals().get("TA_MIN_SCORE_SPOT", 78))
                    min_score_fut = int(globals().get("TA_MIN_SCORE_FUTURES", 74))
                else:
                    min_score_spot = int(os.getenv("MID_MIN_SCORE_SPOT","76"))
                    min_score_fut = int(os.getenv("MID_MIN_SCORE_FUTURES","72"))
                min_rr = float(os.getenv("MID_MIN_RR","2.0"))

                min_adx_30m = float(os.getenv("MID_MIN_ADX_30M","0") or "0")
                min_adx_1h = float(os.getenv("MID_MIN_ADX_1H","0") or "0")
                min_atr_pct = float(os.getenv("MID_MIN_ATR_PCT","0") or "0")
                # HARD MID TP2-first extra filters
                mid_min_vol_x = float(os.getenv("MID_MIN_VOL_X", "0") or "0")
                mid_require_vwap_bias = os.getenv("MID_REQUIRE_VWAP_BIAS", "1").strip().lower() not in ("0","false","no","off")
                mid_min_vwap_dist_atr = float(os.getenv("MID_MIN_VWAP_DIST_ATR", "0") or "0")

                require_align = os.getenv("MID_REQUIRE_30M_TREND","1").strip().lower() not in ("0","false","no")
                structure_align_soft = os.getenv("MID_STRUCTURE_ALIGN_SOFT","0").strip().lower() not in ("0","false","no")
                structure_align_penalty = float(os.getenv("MID_STRUCTURE_ALIGN_PENALTY","8") or 8)
                allow_futures = os.getenv("MID_ALLOW_FUTURES","1").strip().lower() not in ("0","false","no")

                tp_policy = (os.getenv("MID_TP_POLICY","R") or "R").strip().upper()
                tp1_r = float(os.getenv("MID_TP1_R","1.2"))
                tp2_r = float(os.getenv("MID_TP2_R","2.8"))

                # Exchanges to scan (independent from ORDERBOOK_EXCHANGES)
                # NOTE: MID candles routing can restrict this universe further via MID_CANDLES_SOURCES / MID_ENABLE_SECONDARY_EXCHANGES
                _scan_ex = (os.getenv('SCANNER_EXCHANGES','BINANCE,BYBIT,OKX,GATEIO,MEXC') or '').strip()
                scan_exchanges = [x.strip().upper() for x in _scan_ex.split(',') if x.strip()]
                if not scan_exchanges:
                    scan_exchanges = ['BINANCE','BYBIT','OKX','MEXC','GATEIO']

                # MID candle sources preference.
                # If MID_CANDLES_SOURCES is not provided, we default to the same universe as SCANNER_EXCHANGES
                # (so "сканер по все биржи" also means candles routing can use all those exchanges).
                # Secondary exchanges (GATEIO/MEXC) are still gated by MID_ENABLE_SECONDARY_EXCHANGES.
                _mid_sources_env = (os.getenv('MID_CANDLES_SOURCES', '') or '').strip()
                _mid_sources = _mid_sources_env if _mid_sources_env else ','.join(scan_exchanges)
                mid_candles_sources = [x.strip().upper() for x in _mid_sources.split(',') if x.strip()]
                if not mid_candles_sources:
                    mid_candles_sources = list(scan_exchanges)

                # --- MID candles sources (Variant A: multi-exchange) ---
                # We allow pulling candles from all supported adapters, and decide per-symbol dynamically.
                # This is required for tokens that exist on GateIO/MEXC but not on Binance.
                allowed_candle_exchanges = ['BINANCE','BYBIT','OKX','MEXC','GATEIO']

                mid_force_all = (os.getenv('MID_CANDLES_ALL_EXCHANGES', '1') or '1').strip().lower() in ('1','true','yes','on')
                enable_secondary = (os.getenv('MID_ENABLE_SECONDARY_EXCHANGES','1') or '1').strip().lower() in ('1','true','yes','on')

                if mid_force_all:
                    # Don't require candles sources to be present in SCANNER_EXCHANGES:
                    # SCANNER_EXCHANGES controls symbol universe; candles sources control where we fetch klines.
                    mid_candles_sources = [x for x in mid_candles_sources if x in allowed_candle_exchanges]
                    if not mid_candles_sources:
                        mid_candles_sources = allowed_candle_exchanges[:]
                else:
                    if not enable_secondary:
                        # hard-disable secondary unless allowed
                        mid_candles_sources = [x for x in mid_candles_sources if x in ('BINANCE','BYBIT','OKX')] or ['BINANCE','BYBIT','OKX']
                    # Ensure candle sources are within scan_exchanges (legacy behavior)
                    mid_candles_sources = [x for x in mid_candles_sources if x in scan_exchanges] or ['BINANCE','BYBIT','OKX']

                # Always keep BINANCE as a fallback candle source unless explicitly disabled.
                # NOTE: Many symbols are missing on BYBIT/OKX SPOT; without BINANCE fallback MID may produce массовый no_candles/emitted=0.
                disable_binance = (os.getenv('MID_DISABLE_BINANCE_CANDLES','0') or '0').strip().lower() in ('1','true','yes','on')
                force_binance = (os.getenv('MID_FORCE_BINANCE_CANDLES','1') or '1').strip().lower() in ('1','true','yes','on')
                if (not disable_binance) and force_binance:
                    if 'BINANCE' not in mid_candles_sources:
                        mid_candles_sources = ['BINANCE'] + [x for x in mid_candles_sources if x != 'BINANCE']

                # --- MID candles routing: stable primary (hash) BINANCE/BYBIT + fallback + smart cache ---
                _primary_ex = (os.getenv("MID_PRIMARY_EXCHANGES", "BINANCE,BYBIT") or "").strip()
                mid_primary_exchanges = [x.strip().upper() for x in _primary_ex.split(",") if x.strip()]

                # Universe for MID candles routing:
                # Use MID_CANDLES_SOURCES (already filtered by MID_ENABLE_SECONDARY_EXCHANGES and SCANNER_EXCHANGES),
                # NOT the full scan_exchanges list. This prevents wasting time on exchanges that are not intended
                # to be used for candles (e.g., MEXC/GATEIO futures returning empty).
                mid_universe = list(mid_candles_sources or ["BINANCE","BYBIT","OKX"])

                # Ensure at least 2 primaries; fall back to first two in universe
                if len(mid_primary_exchanges) < 2:
                    mid_primary_exchanges = mid_universe[:2] if len(mid_universe) >= 2 else ["BINANCE", "BYBIT"]

                # Keep only those available in mid_universe
                mid_primary_exchanges = [x for x in mid_primary_exchanges if x in mid_universe] or (mid_universe[:2] if len(mid_universe) >= 2 else ["BINANCE","BYBIT"])

                # Fallback exchanges are the rest of the MID universe
                mid_fallback_exchanges = [x for x in mid_universe if x not in mid_primary_exchanges]
                mid_primary_mode = (os.getenv("MID_PRIMARY_MODE", "hash") or "hash").strip().lower()
                mid_candles_retry = int(os.getenv("MID_CANDLES_RETRY", "1") or "1")
                mid_cache_ttl_5m = int(os.getenv("MID_CANDLES_CACHE_TTL_5M", os.getenv("MID_CANDLES_CACHE_TTL_SEC", "60")) or "60")
                mid_cache_ttl_30m = int(os.getenv("MID_CANDLES_CACHE_TTL_30M", os.getenv("MID_CANDLES_CACHE_TTL_SEC", "180")) or "180")
                mid_cache_ttl_1h = int(os.getenv("MID_CANDLES_CACHE_TTL_1H", os.getenv("MID_CANDLES_CACHE_TTL_SEC", "300")) or "300")
                mid_cache_stale_sec = int(os.getenv("MID_CANDLES_CACHE_STALE_SEC", "900") or "900")

                # persistent candles cache (shared across ticks)
                if not hasattr(self, "_mid_candles_cache"):
                    self._mid_candles_cache = {}  # type: ignore[attr-defined]
                if not hasattr(self, "_mid_candles_inflight"):
                    self._mid_candles_inflight = {}  # type: ignore[attr-defined]
                if not hasattr(self, "_mid_klines_sem"):
                    try:
                        self._mid_klines_sem = asyncio.Semaphore(int(os.getenv("MID_KLINES_CONCURRENCY", "15") or "15"))  # type: ignore[attr-defined]
                    except Exception:
                        self._mid_klines_sem = asyncio.Semaphore(int(os.getenv('MID_KLINES_CONCURRENCY', '45') or 45))  # type: ignore[attr-defined]

                _mid_candles_cache = self._mid_candles_cache  # type: ignore[attr-defined]
                _mid_candles_inflight = self._mid_candles_inflight  # type: ignore[attr-defined]
                _mid_klines_sem = self._mid_klines_sem  # type: ignore[attr-defined]


                def _mid_cache_ttl(tf: str) -> int:
                    t = (tf or "").lower()
                    if t in ("5m", "5min", "5"):
                        return mid_cache_ttl_5m
                    if t in ("30m", "30min", "30"):
                        return mid_cache_ttl_30m
                    return mid_cache_ttl_1h


                def _mid_tf_stale_sec(tf: str, *, kind: str = "generic") -> float:
                    """Return max allowed candle age (seconds) for a timeframe.

                    Priority (highest -> lowest):
                      - MID_REST_CANDLES_MAX_AGE_SEC / MID_PERSIST_CANDLES_MAX_AGE_SEC (explicit per-source override)
                      - MID_CANDLES_CACHE_STALE_SEC (legacy global override)
                      - TF-aware defaults (env-overridable):
                          5m  -> MID_TF_STALE_5M_SEC  (default 1200)
                          30m -> MID_TF_STALE_30M_SEC (default 5400)
                          1h  -> MID_TF_STALE_1H_SEC  (default 7200)
                    """
                    try:
                        t = (tf or "").strip().lower()
                        # Explicit per-source override (keeps backward compatibility)
                        if kind == "rest":
                            v = (os.getenv("MID_REST_CANDLES_MAX_AGE_SEC") or "").strip()
                            if v:
                                return float(v)
                        if kind == "persist":
                            v = (os.getenv("MID_PERSIST_CANDLES_MAX_AGE_SEC") or "").strip()
                            if v:
                                return float(v)

                        # Legacy global override
                        v = (os.getenv("MID_CANDLES_CACHE_STALE_SEC") or "").strip()
                        if v:
                            return float(v)

                        # TF-aware defaults (can be overridden)
                        if t in ("5m", "5min", "5"):
                            v = (os.getenv("MID_TF_STALE_5M_SEC") or os.getenv("MID_STALE_5M_SEC") or "").strip()
                            return float(v) if v else 1200.0
                        if t in ("30m", "30min", "30"):
                            v = (os.getenv("MID_TF_STALE_30M_SEC") or os.getenv("MID_STALE_30M_SEC") or "").strip()
                            return float(v) if v else 5400.0
                        # default: 1h+
                        v = (os.getenv("MID_TF_STALE_1H_SEC") or os.getenv("MID_STALE_1H_SEC") or "").strip()
                        return float(v) if v else 7200.0
                    except Exception:
                        # safest fallback
                        return 1800.0

                def _mid_primary_for_symbol(symb: str) -> str:
                    if MID_CANDLES_LIGHT_MODE or MID_CANDLES_BINANCE_FIRST:
                        return "BINANCE"
                    # Prefer the exchange that produced the symbol universe (scanner), to avoid routing
                    # a symbol to an exchange where it doesn't exist -> candles_unavailable storms.
                    try:
                        _pref_map = getattr(self, "_mid_symbol_pref_exchange", None)
                        if isinstance(_pref_map, dict):
                            _px = _pref_map.get(symb)
                            if _px:
                                return str(_px).upper()
                    except Exception:
                        pass

                    # Fallback: stable routing hash(symbol) -> primary exchange from MID_PRIMARY_EXCHANGES
                    if not mid_primary_exchanges:
                        return "BINANCE"
                    if mid_primary_mode not in ("hash", "round_robin"):
                        # unknown mode -> hash
                        pass
                    try:
                        h = int.from_bytes(hashlib.sha1(symb.encode("utf-8")).digest()[:4], "big")
                    except Exception:
                        h = sum(ord(c) for c in symb)
                    return mid_primary_exchanges[h % len(mid_primary_exchanges)]
                # --- MID candles diagnostics (optional) ---
                _mid_candles_log_fail = int(os.getenv("MID_CANDLES_LOG_FAIL", "1") or "1")
                _mid_candles_log_samples = int(os.getenv("MID_CANDLES_LOG_FAIL_SAMPLES", "3") or "3")
                _mid_candles_fail = defaultdict(int)  # (exchange, tf) -> count
                _mid_candles_fail_samples = defaultdict(list)  # (exchange, tf) -> [err...]
                _mid_candles_log_empty = int(os.getenv('MID_CANDLES_LOG_EMPTY', '1') or '1')
                _mid_candles_log_empty_samples = int(os.getenv('MID_CANDLES_LOG_EMPTY_SAMPLES', '5') or '5')
                _mid_candles_empty = 0
                _mid_candles_empty_reasons = defaultdict(int)
                _mid_candles_empty_samples = defaultdict(list)
                _mid_candles_fallback = 0


                def _mid_fmt_missing_wait(tf: str, need: int, got: int) -> str:
                    """Return ' missing=X (~Y needed)' for partial candle history diagnostics."""
                    try:
                        if not need:
                            return ''
                        missing = int(need) - int(got or 0)
                        if missing <= 0:
                            return ''
                        tf_l = (tf or '').lower().strip()
                        sec_per = 0
                        # supports like 5m, 30m, 1h, 4h, 1d
                        if tf_l.endswith('m') and tf_l[:-1].isdigit():
                            sec_per = int(tf_l[:-1]) * 60
                        elif tf_l.endswith('h') and tf_l[:-1].isdigit():
                            sec_per = int(tf_l[:-1]) * 3600
                        elif tf_l.endswith('d') and tf_l[:-1].isdigit():
                            sec_per = int(tf_l[:-1]) * 86400
                        if sec_per <= 0:
                            return f" missing={missing}"
                        total = missing * sec_per
                        days = total // 86400
                        rem = total % 86400
                        hours = rem // 3600
                        rem = rem % 3600
                        mins = rem // 60
                        if days > 0:
                            approx = f"{days}d{hours:02d}h"
                        elif hours > 0:
                            approx = f"{hours}h{mins:02d}m"
                        else:
                            approx = f"{mins}m"
                        return f" missing={missing} (~{approx} needed)"
                    except Exception:
                        return ''


                async def _mid_fetch_klines_rest(ex_name: str, symb: str, tf: str, limit: int, market: str) -> Optional[pd.DataFrame]:


                    nonlocal _mid_candles_net_fail, _mid_candles_unsupported, _mid_candles_partial
                    try:
                        # Global MID kline concurrency guard (prevents HTTP overload -> timeouts -> no_candles)
                        async with _mid_klines_sem:
                            mkt = (market or 'SPOT').upper().strip()
                            # PRODUCTION: optionally force SPOT candles to avoid FUTURES-only instrument gaps
                            if os.getenv('MID_CANDLES_FORCE_SPOT', '0').strip().lower() not in ('0','false','no','off'):
                                mkt = 'SPOT'

                            # PRODUCTION: drop obviously-bad symbols early (saves REST/WS limits and prevents no_candles storms)
                            _sym = (symb or '').strip().upper()
                            if (not _sym.isascii()) or (not _sym.endswith('USDT')) or (len(_sym) < 6) or (not re.match(r'^[A-Z0-9]{2,20}USDT$', _sym)):
                                # Mark as unsupported to avoid repeated REST/WS attempts for clearly invalid symbols.
                                try:
                                    api._mark_unsupported(ex_name, mkt, _sym, tf)
                                except Exception:
                                    pass
                                _mid_diag_add(_sym, ex_name, mkt, tf, 'invalid_symbol_format')
                                _mid_candles_unsupported += 1
                                return None

                            def _mid_rest_is_fresh(df: Optional[pd.DataFrame]) -> bool:
                                """Validate REST candles freshness. Reject stale REST to avoid using old data."""
                                try:
                                    if df is None or getattr(df, 'empty', True):
                                        return False
                                    max_age = float(_mid_tf_stale_sec(tf, kind="rest"))
                                    now_ms = int(time.time() * 1000)
                                    if "close_time_ms" in df.columns:
                                        last_ms = int(df["close_time_ms"].iloc[-1])
                                    elif "close_time" in df.columns:
                                        last_ms = int(df["close_time"].iloc[-1])
                                    elif "open_time_ms" in df.columns:
                                        # approximate close from open + tf
                                        last_ms = int(df["open_time_ms"].iloc[-1])
                                    elif "open_time" in df.columns:
                                        last_ms = int(df["open_time"].iloc[-1])
                                    else:
                                        return True  # cannot verify -> don't block
                                    return (now_ms - last_ms) <= int(max_age * 1000)
                                except Exception:
                                    return True


                            # Fast-skip pairs we already know are unsupported on this exchange/market/TF
                            try:
                                if api._is_unsupported_cached(ex_name, mkt, _sym, tf):
                                    _mid_diag_add(symb, ex_name, mkt, tf, 'unsupported_cached')
                                    return pd.DataFrame()
                            except Exception:
                                pass
                            if ex_name in ('GATEIO','MEXC') and mkt == 'FUTURES':
                                # Futures candles are not implemented for these adapters. Mark unsupported and skip.
                                try:
                                    api._mark_unsupported(ex_name, mkt, symb, tf)
                                except Exception:
                                    pass
                                _mid_diag_add(symb, ex_name, mkt, tf, 'unsupported_market')
                                return None
                            # Availability check (cached): skip symbols that don't exist on this exchange/market
                            try:
                                if not await api._market_supported_ex(ex_name, mkt, _sym):
                                    try:
                                        api._mark_unsupported(ex_name, mkt, _sym, tf)
                                    except Exception:
                                        pass
                                    _mid_diag_add(_sym, ex_name, mkt, tf, 'unsupported_pair')
                                    return pd.DataFrame()
                            except Exception:
                                # If availability check fails, fall back to request
                                pass

                            if ex_name == "BINANCE":
                                # Avoid noisy HTTP 400 (Invalid symbol) on futures by checking exchangeInfo first.
                                try:
                                    if mkt == 'FUTURES':
                                        info = api._binance_exchange_info(futures=True)
                                        if info and isinstance(info, dict):
                                            syms = {s.get('symbol') for s in (info.get('symbols') or []) if isinstance(s, dict)}
                                            symb_req = api._binance_futures_symbol_alias((symb or '').upper().strip())
                                            if (symb not in syms) and (symb_req not in syms):
                                                try:
                                                    api._mark_unsupported(ex_name, mkt, symb, tf)
                                                except Exception:
                                                    pass
                                                _mid_diag_add(symb, ex_name, mkt, tf, 'unsupported_pair')
                                                return pd.DataFrame()
                                except Exception:
                                    # If exchangeInfo fails, fall back to request (old behavior)
                                    pass
                                df = await api.klines_binance(symb, tf, limit, market=market)
                                if df is not None and not getattr(df, 'empty', True) and not _mid_rest_is_fresh(df):
                                    _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'stale_rest')
                                    return pd.DataFrame()
                                dfn = _mid_norm_ohlcv(df)
                                if dfn is None:
                                    _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'bad_schema', 'rest')
                                    return pd.DataFrame()
                                return dfn
                            if ex_name == "BYBIT":
                                df = await api.klines_bybit(symb, tf, limit, market=market)
                                if df is not None and not getattr(df, 'empty', True) and not _mid_rest_is_fresh(df):
                                    _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'stale_rest')
                                    return pd.DataFrame()
                                dfn = _mid_norm_ohlcv(df)
                                if dfn is None:
                                    _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'bad_schema', 'rest')
                                    return pd.DataFrame()
                                return dfn
                            if ex_name == "OKX":
                                df = await api.klines_okx(symb, tf, limit, market=market)
                                if df is not None and not getattr(df, 'empty', True) and not _mid_rest_is_fresh(df):
                                    _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'stale_rest')
                                    return pd.DataFrame()
                                dfn = _mid_norm_ohlcv(df)
                                if dfn is None:
                                    _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'bad_schema', 'rest')
                                    return pd.DataFrame()
                                return dfn
                            if ex_name == "GATEIO":
                                df = await api.klines_gateio(symb, tf, limit)
                                if df is not None and not getattr(df, 'empty', True) and not _mid_rest_is_fresh(df):
                                    _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'stale_rest')
                                    return pd.DataFrame()
                                dfn = _mid_norm_ohlcv(df)
                                if dfn is None:
                                    _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'bad_schema', 'rest')
                                    return pd.DataFrame()
                                return dfn
                            # default MEXC
                            df = await api.klines_mexc(symb, tf, limit)
                            if df is not None and not getattr(df, 'empty', True) and not _mid_rest_is_fresh(df):
                                _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'stale_rest')
                                return pd.DataFrame()
                            dfn = _mid_norm_ohlcv(df)
                            if dfn is None:
                                _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'bad_schema', 'rest')
                                return pd.DataFrame()
                            return dfn

                    except Exception as e:
                        # classify candle failures
                        is_pair_not_found = False
                        try:
                            msg = str(e)
                            umsg = msg.upper()
                            # Pair/instrument does not exist on this exchange/market
                            if isinstance(e, ExchangeAPIError):
                                if ('INVALID SYMBOL' in umsg) or ('-1121' in umsg) or ('SYMBOL IS INVALID' in umsg) or ('NOT SUPPORTED SYMBOL' in umsg) or ('NOT SUPPORTED SYMBOLS' in umsg) or ('INSTRUMENT ID' in umsg and 'DOESN\'T EXIST' in umsg) or ('CODE=51001' in umsg):
                                    is_pair_not_found = True
                            # Some wrappers may not be ExchangeAPIError but still include these markers
                            if (not is_pair_not_found) and (('SYMBOL IS INVALID' in umsg) or ('NOT SUPPORTED SYMBOL' in umsg) or ('NOT SUPPORTED SYMBOLS' in umsg) or ('INVALID SYMBOL' in umsg) or ('CODE=51001' in umsg) or ("DOESN'T EXIST" in umsg and 'INSTRUMENT' in umsg)):
                                is_pair_not_found = True

                            if is_pair_not_found:
                                _mid_candles_unsupported += 1
                                try:
                                    api._mark_unsupported(ex_name, (market or 'SPOT'), symb, tf)
                                except Exception:
                                    pass
                            else:
                                # HTTP 400/404 often means unsupported interval/symbol too
                                if isinstance(e, ExchangeAPIError) and ('HTTP 400' in msg or 'HTTP 404' in msg):
                                    _mid_candles_unsupported += 1
                                    try:
                                        api._mark_unsupported(ex_name, (market or 'SPOT'), symb, tf)
                                    except Exception:
                                        pass
                                else:
                                    _mid_candles_net_fail += 1
                                    _mid_diag_add(symb, ex_name, (market or 'SPOT'), tf, 'fail', f"{type(e).__name__}: {e}")
                        except Exception:
                            pass
                        # swallow but count (optional)
                        try:
                            if _mid_candles_log_fail:
                                _mid_candles_fail[(ex_name, tf)] += 1
                                lst = _mid_candles_fail_samples[(ex_name, tf)]
                                if len(lst) < _mid_candles_log_samples:
                                    lst.append(f"{type(e).__name__}: {e}")
                        except Exception:
                            pass
                        return None

                
                
                async def _mid_fetch_klines_wsdb_only(ex_name: str, symb: str, tf: str, limit: int, market: str) -> Optional[pd.DataFrame]:
                                    """Return candles using only in-memory cache + persistent DB cache.
                                    NO REST, NO retries. Used to build higher TF from WS/DB 5m without accidentally triggering REST storms.
                                    """
                                    nonlocal _mid_db_hit, _mid_candles_cache
                                    try:
                                        _sn = (symb or '').strip().upper()
                                        _sn = _sn.replace('-', '').replace('_', '').replace(':', '').replace('/', '')
                                        for _suf in ('SWAP','PERP','PERPETUAL','FUT','FUTURES'):
                                            if _sn.endswith(_suf):
                                                _sn = _sn[: -len(_suf)]
                                                break
                                        symb_n = _sn
                                        key = (ex_name, (market or 'SPOT').upper().strip(), symb_n, tf, int(limit))
                                        now = time.time()
                                        ttl = _mid_cache_ttl(tf)
                                        cached = _mid_candles_cache.get(key)
                                        if cached:
                                            ts, df = cached
                                            if (now - ts) <= ttl and df is not None and not getattr(df, 'empty', True):
                                                _mid_db_hit += 1
                                                _mid_diag_ok(symb_n, ex_name, market, tf, 'cache', df)
                                                dfn = _mid_norm_ohlcv(df)
                                                if dfn is None:
                                                    _mid_diag_add(symb_n, ex_name, market, tf, 'bad_schema', 'cache')
                                                    return pd.DataFrame()
                                                return dfn

                                        persist_enabled = str(os.getenv("MID_PERSIST_CANDLES", "1") or "1").strip().lower() not in ("0","false","no","off")
                                        if persist_enabled and tf in ("5m","30m","1h"):
                                            try:
                                                max_age = int(float(_mid_tf_stale_sec(tf, kind="persist")))
                                                row = await db_store.candles_cache_get(ex_name, (market or 'SPOT').upper().strip(), symb_n, tf, int(limit))
                                                if (not row):
                                                    try:
                                                        ws_limit = int(os.getenv("CANDLES_WS_LIMIT", "250") or 250)
                                                    except Exception:
                                                        ws_limit = 250
                                                    if int(limit) != int(ws_limit):
                                                        row = await db_store.candles_cache_get(ex_name, (market or 'SPOT').upper().strip(), symb_n, tf, int(ws_limit))
                                                if row:
                                                    blob, updated_at = row
                                                    if updated_at is not None:
                                                        age = (dt.datetime.now(dt.timezone.utc) - updated_at).total_seconds()
                                                    else:
                                                        age = 0.0
                                                    if age <= max_age:
                                                        dfp = db_store._cc_unpack_df(blob)
                                                        try:
                                                            if dfp is not None and not dfp.empty and int(limit) > 0 and len(dfp) > int(limit):
                                                                dfp = dfp.tail(int(limit)).copy()
                                                        except Exception:
                                                            pass
                                                        if dfp is not None and not dfp.empty:
                                                            _mid_db_hit += 1
                                                            _mid_candles_cache[key] = (now, dfp)
                                                            _mid_diag_ok(symb_n, ex_name, market, tf, 'persist', dfp)
                                                            dfp_n = _mid_norm_ohlcv(dfp)
                                                            if dfp_n is None:
                                                                _mid_diag_add(symb_n, ex_name, market, tf, 'bad_schema', 'persist')
                                                                return pd.DataFrame()
                                                            return dfp_n
                                                    else:
                                                        _mid_diag_add(symb_n, ex_name, market, tf, 'stale_persist')
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass
                                    return None

                async def _mid_fetch_klines_cached(ex_name: str, symb: str, tf: str, limit: int, market: str) -> Optional[pd.DataFrame]:

                    nonlocal _mid_candles_net_fail, _mid_candles_unsupported, _mid_candles_partial, _mid_db_hit, _mid_rest_refill, _mid_candles_cache, _mid_candles_empty, _mid_candles_empty_reasons, _mid_candles_empty_samples, _mid_candles_fallback
                    # Normalize symbol consistently for cache + unsupported markers
                    _sn = (symb or '').strip().upper()
                    _sn = _sn.replace('-', '').replace('_', '').replace(':', '').replace('/', '')
                    for _suf in ('SWAP','PERP','PERPETUAL','FUT','FUTURES'):
                        if _sn.endswith(_suf):
                            _sn = _sn[: -len(_suf)]
                            break
                    symb = _sn
                    key = (ex_name, (market or 'SPOT').upper().strip(), symb, tf, int(limit))
                    mkt0 = (market or 'SPOT').upper().strip()
                    # Fast skip: known unsupported (avoid repeated empty/unsupported storms)
                    try:
                        if api._is_unsupported_cached(ex_name, mkt0, symb, tf):
                            _mid_candles_unsupported += 1
                            _mid_diag_add(symb, ex_name, mkt0, tf, 'unsupported_cached', 'pre')
                            return None
                    except Exception:
                        pass
                    # Fast skip: market/symbol not supported according to cached instruments
                    # (prevents FUTURES empty responses when the contract doesn't exist)
                    try:
                        if getattr(api, "_market_avail_check", False):
                            if not _market_supported_ex(ex_name, mkt0, symb):
                                try:
                                    api._mark_unsupported(ex_name, mkt0, symb, tf)
                                except Exception:
                                    pass
                                _mid_diag_add(symb, ex_name, mkt0, tf, 'unsupported_pair', 'pre')
                                _mid_candles_unsupported += 1
                                return None
                    except Exception:
                        pass
                    now = time.time()
                    ttl = _mid_cache_ttl(tf)
                    cached = _mid_candles_cache.get(key)
                    if cached:
                        ts, df = cached
                        if (now - ts) <= ttl and df is not None and not df.empty:
                            # IMPORTANT: treat short cache (got < need) as missing on cold start.
                            # This allows REST-prefill / REST fallback to fill history after restart.
                            try:
                                need_bars = int(_mid_need_bars(tf) or 0)
                            except Exception:
                                need_bars = 0
                            got_bars = 0
                            try:
                                got_bars = int(len(df))
                            except Exception:
                                got_bars = 0
                            if need_bars and got_bars < need_bars:
                                # keep diag as partial, but do NOT return; fall through to persist/prefill/REST
                                _mid_diag_ok(symb, ex_name, market, tf, 'cache', df)
                            else:
                                _mid_db_hit += 1
                                _mid_diag_ok(symb, ex_name, market, tf, 'cache', df)
                                return df
                    
                    # Persistent candles cache (Postgres) to survive restarts.
                    persist_enabled = str(os.getenv("MID_PERSIST_CANDLES", "1") or "1").strip().lower() not in ("0","false","no","off")
                    if persist_enabled and tf in ("5m","30m","1h"):
                        try:
                            max_age = int(float(_mid_tf_stale_sec(tf, kind="persist")))
                            row = await db_store.candles_cache_get(ex_name, (market or 'SPOT').upper().strip(), symb, tf, int(limit))
                            if (not row):
                                # WS-candles service usually persists with a single fixed limit (default 250).
                                # If MID asks for a different limit (e.g. 200/120), try the WS limit as fallback
                                # to maximize cache hits and avoid REST storms.
                                try:
                                    ws_limit = int(os.getenv("CANDLES_WS_LIMIT", "250") or 250)
                                except Exception:
                                    ws_limit = 250
                                if int(limit) != int(ws_limit):
                                    row = await db_store.candles_cache_get(ex_name, (market or 'SPOT').upper().strip(), symb, tf, int(ws_limit))
                            if row:
                                blob, updated_at = row
                                if updated_at is not None:
                                    age = (dt.datetime.now(dt.timezone.utc) - updated_at).total_seconds()
                                else:
                                    age = 0.0
                                if age <= max_age:
                                    dfp = db_store._cc_unpack_df(blob)
                                    try:
                                        if dfp is not None and not dfp.empty and int(limit) > 0 and len(dfp) > int(limit):
                                            dfp = dfp.tail(int(limit)).copy()
                                    except Exception:
                                        pass
                                    if dfp is not None and not dfp.empty:
                                        # IMPORTANT: treat short persist cache (got < need) as missing.
                                        # This prevents cold-start got=1 from blocking signals for hours.
                                        dfp_n = _mid_norm_ohlcv(dfp)
                                        if dfp_n is None:
                                            _mid_diag_add(symb, ex_name, market, tf, 'bad_schema', 'persist')
                                            return pd.DataFrame()
                                        try:
                                            need_bars = int(_mid_need_bars(tf) or 0)
                                        except Exception:
                                            need_bars = 0
                                        got_bars = 0
                                        try:
                                            got_bars = int(len(dfp_n))
                                        except Exception:
                                            got_bars = 0
                                        if need_bars and got_bars < need_bars:
                                            # record diag + cache, but fall through to prefill/REST
                                            _mid_db_hit += 1
                                            _mid_candles_cache[key] = (now, dfp_n)
                                            _mid_diag_ok(symb, ex_name, market, tf, 'persist', dfp_n)
                                        else:
                                            _mid_db_hit += 1
                                            _mid_candles_cache[key] = (now, dfp_n)
                                            _mid_diag_ok(symb, ex_name, market, tf, 'persist', dfp_n)
                                            return dfp_n
                        except Exception:
                            pass

                    # --- REST prefill for cold start (fills history after restart) ---
                    # WS is great for live updates but after restart may have got=1.
                    # When enabled, if we don't have enough bars in cache/DB, we do a one-time REST prefill
                    # (with per-symbol/TF cooldown) and persist the result to Postgres candles_cache.
                    prefill_enabled = str(os.getenv("MID_PREFILL_FROM_REST", "0") or "0").strip().lower() not in ("0","false","no","off")
                    prefill_only_if_missing = str(os.getenv("MID_PREFILL_ONLY_IF_MISSING", "1") or "1").strip().lower() not in ("0","false","no","off")
                    try:
                        prefill_cooldown = float(os.getenv("MID_PREFILL_COOLDOWN_SEC", "900") or 900)
                    except Exception:
                        prefill_cooldown = 900.0
                    try:
                        prefill_timeout = float(os.getenv("MID_PREFILL_TIMEOUT_SEC", "3.5") or 3.5)
                    except Exception:
                        prefill_timeout = 3.5

                    def _prefill_limit_for_tf(tf_: str) -> int:
                        ttf = (tf_ or "").strip().lower()
                        try:
                            if ttf in ("5m","5min","5"):
                                return int(os.getenv("MID_PREFILL_LIMIT_5M", "300") or 300)
                            if ttf in ("30m","30min","30"):
                                return int(os.getenv("MID_PREFILL_LIMIT_30M", "200") or 200)
                            return int(os.getenv("MID_PREFILL_LIMIT_1H", "150") or 150)
                        except Exception:
                            return 300 if ttf.startswith("5") else (200 if ttf.startswith("30") else 150)

                    # prefill guard state
                    if prefill_enabled and tf in ("5m","30m","1h"):
                        try:
                            # If we already have enough bars in WS/DB cache -> skip prefill.
                            need = _mid_need_bars(tf) if prefill_only_if_missing else 0
                            if need and int(limit) > 0 and int(limit) >= need:
                                # We don't know got here because cache lookup already failed, so treat as missing.
                                pass
                            if not hasattr(self, "_mid_prefill_last"):
                                self._mid_prefill_last = {}  # type: ignore[attr-defined]
                            _pl = self._mid_prefill_last  # type: ignore[attr-defined]
                            _pkey = (ex_name, (market or "SPOT").upper().strip(), symb, tf)
                            last_ts = float(_pl.get(_pkey, 0.0) or 0.0)
                            # If we have a recent successful prefill for this (market,symbol,tf),
                            # don't hit REST again even if we rotate exchanges.
                            if not hasattr(self, "_mid_prefill_last_ok"):
                                self._mid_prefill_last_ok = {}  # type: ignore[attr-defined]
                            _pl_ok = self._mid_prefill_last_ok  # type: ignore[attr-defined]
                            _okey = ((market or "SPOT").upper().strip(), symb, tf)
                            last_ok = float(_pl_ok.get(_okey, 0.0) or 0.0)
                            if (time.time() - last_ok) < float(prefill_cooldown):
                                last_ts = time.time()
                                try:
                                    if _mid_log_prefill:
                                        _left = float(prefill_cooldown) - (time.time() - last_ok)
                                        try:
                                            _ctx = _MID_TICK_CTX.get()
                                            _ps = _ctx.get('prefill_skip') if isinstance(_ctx, dict) else None
                                            _k = (symb, tf, (market or "SPOT").upper().strip())
                                            if isinstance(_ps, set) and _k in _ps:
                                                pass
                                            else:
                                                if isinstance(_ps, set):
                                                    _ps.add(_k)
                                                # Skip logs are noisy; keep them on DEBUG when explicitly enabled.
                                                logger.debug("[mid][prefill_skip] symbol=%s tf=%s market=%s reason=cooldown left=%.1fs", symb, tf, (market or "SPOT"), max(0.0, _left))
                                        except Exception:
                                            logger.debug("[mid][prefill_skip] symbol=%s tf=%s market=%s reason=cooldown left=%.1fs", symb, tf, (market or "SPOT"), max(0.0, _left))
                                except Exception:
                                    pass

                            if (time.time() - last_ts) >= float(prefill_cooldown):
                                _pl[_pkey] = float(time.time())
                                pf_limit = _prefill_limit_for_tf(tf)
                                # ensure prefill limit at least what caller asked
                                try:
                                    pf_limit = int(max(int(pf_limit), int(limit)))
                                except Exception:
                                    pf_limit = int(pf_limit)
                                # Do REST prefill (single venue) with a short timeout.
                                try:
                                    try:
                                        if _mid_log_prefill:
                                            logger.debug("[mid][prefill_try] symbol=%s tf=%s market=%s ex=%s limit=%s pf_limit=%s timeout=%ss", symb, tf, (market or "SPOT"), ex_name, limit, pf_limit, prefill_timeout)
                                    except Exception:
                                        pass
                                    df_pf = await asyncio.wait_for(
                                        _mid_fetch_klines_rest(ex_name, symb, tf, int(pf_limit), market),
                                        timeout=float(prefill_timeout),
                                    )
                                except Exception as _e_pf:
                                    # If REST prefill fails, this is important: candles may be unavailable.
                                    # Log once per (market,symbol,tf) per tick.
                                    df_pf = None
                                    try:
                                        _ctx = _MID_TICK_CTX.get()
                                        _pe = _ctx.get('prefill_err') if isinstance(_ctx, dict) else None
                                        _k = ((market or "SPOT").upper().strip(), symb, tf, ex_name)
                                        if isinstance(_pe, set) and _k in _pe:
                                            pass
                                        else:
                                            if isinstance(_pe, set):
                                                _pe.add(_k)
                                            logger.warning(
                                                "[mid][prefill_error] symbol=%s tf=%s market=%s ex=%s err=%s",
                                                symb, tf, (market or "SPOT"), ex_name, str(_e_pf),
                                            )
                                    except Exception:
                                        logger.warning(
                                            "[mid][prefill_error] symbol=%s tf=%s market=%s ex=%s",
                                            symb, tf, (market or "SPOT"), ex_name,
                                        )
                                if df_pf is not None and not getattr(df_pf, "empty", True):
                                    # normalize once, then persist
                                    df_pf_n = _mid_norm_ohlcv(df_pf)
                                    if df_pf_n is not None and not getattr(df_pf_n, "empty", True):
                                        got_pf = 0
                                        try:
                                            got_pf = int(len(df_pf_n))
                                        except Exception:
                                            got_pf = 0
                                        # Persist under multiple common limits to maximize reuse (prefill_limit + WS limit + requested limit).
                                        try:
                                            if persist_enabled:
                                                try:
                                                    ws_limit = int(os.getenv("CANDLES_WS_LIMIT", "250") or 250)
                                                except Exception:
                                                    ws_limit = 250
                                                    ws_limit = 250
                                                if int(ws_limit) != int(pf_limit) and got_pf:
                                                    df_ws = df_pf_n.tail(int(ws_limit)).copy() if got_pf > int(ws_limit) else df_pf_n
                                                    blob_ws = db_store._cc_pack_df(df_ws)
                                                    await db_store.candles_cache_set(ex_name, (market or 'SPOT').upper().strip(), symb, tf, int(ws_limit), blob_ws)
                                                if int(limit) != int(pf_limit) and int(limit) > 0 and got_pf:
                                                    df_req = df_pf_n.tail(int(limit)).copy() if got_pf > int(limit) else df_pf_n
                                                    blob_req = db_store._cc_pack_df(df_req)
                                                    await db_store.candles_cache_set(ex_name, (market or 'SPOT').upper().strip(), symb, tf, int(limit), blob_req)
                                        except Exception:
                                            pass
                                        # Cache in memory for this request
                                        try:
                                            df_ret = df_pf_n
                                            if int(limit) > 0 and got_pf and got_pf > int(limit):
                                                df_ret = df_pf_n.tail(int(limit)).copy()
                                            _mid_candles_cache[key] = (now, df_ret)
                                        except Exception:
                                            df_ret = df_pf_n
                                        _mid_rest_refill += 1
                                        # Successful prefill is normal; keep it silent by default.
                                        if _mid_log_prefill and _mid_log_prefill_ok:
                                            try:
                                                logger.debug(
                                                    "[mid][prefill_ok] symbol=%s tf=%s market=%s ex=%s got=%s limit=%s pf_limit=%s",
                                                    symb, tf, (market or "SPOT"), ex_name, got_pf, limit, pf_limit,
                                                )
                                            except Exception:
                                                pass
                                        _mid_diag_ok(symb, ex_name, market, tf, "rest_prefill", df_pf_n)
                                        try:
                                            if hasattr(self, "_mid_prefill_last_ok"):
                                                self._mid_prefill_last_ok[((market or "SPOT").upper().strip(), symb, tf)] = float(time.time())  # type: ignore[attr-defined]
                                        except Exception:
                                            pass
                                        return df_ret
                                else:
                                    # REST returned empty/no data -> that's a real problem; log once per tick.
                                    try:
                                        _ctx = _MID_TICK_CTX.get()
                                        _pe0 = _ctx.get('prefill_empty') if isinstance(_ctx, dict) else None
                                        _k0 = ((market or "SPOT").upper().strip(), symb, tf, ex_name)
                                        if isinstance(_pe0, set) and _k0 in _pe0:
                                            pass
                                        else:
                                            if isinstance(_pe0, set):
                                                _pe0.add(_k0)
                                            logger.warning(
                                                "[mid][prefill_empty] symbol=%s tf=%s market=%s ex=%s pf_limit=%s",
                                                symb, tf, (market or "SPOT"), ex_name, pf_limit,
                                            )
                                    except Exception:
                                        pass
                        except Exception:
                            pass



                    # If TF is 30m/1h and we have 5m candles (WS/DB), build higher TF from 5m to avoid REST.
                    if tf in ("30m", "1h"):
                        try:
                            tf_minutes = 30 if tf == "30m" else 60
                            # Get 5m candles (WS/DB-first; REST only if no cache)
                            df5 = await _mid_fetch_klines_wsdb_only(ex_name, symb, "5m", int(max(limit, 120)), market)
                            if df5 is not None and not getattr(df5, "empty", True):
                                d5 = df5.copy()
                                try:
                                    if "open_time" in d5.columns and "open_time_ms" not in d5.columns:
                                        d5["open_time_ms"] = d5["open_time"].astype("int64")
                                    if "close_time" in d5.columns and "close_time_ms" not in d5.columns:
                                        d5["close_time_ms"] = d5["close_time"].astype("int64")
                                    d5["open_time_ms"] = d5["open_time_ms"].astype("int64")
                                    if "close_time_ms" in d5.columns:
                                        d5["close_time_ms"] = d5["close_time_ms"].astype("int64")
                                except Exception:
                                    pass

                                bucket_ms = tf_minutes * 60_000
                                ot = d5["open_time_ms"].astype("int64")
                                b = (ot // bucket_ms) * bucket_ms
                                d5 = d5.assign(_bucket=b)

                                g = d5.groupby("_bucket", sort=True)

                                out = g.agg(
                                    open_time_ms=("open_time_ms", "min"),
                                    open=("open", "first"),
                                    high=("high", "max"),
                                    low=("low", "min"),
                                    close=("close", "last"),
                                    volume=("volume", "sum"),
                                ).reset_index(drop=True)

                                try:
                                    if "close_time_ms" in d5.columns:
                                        out["close_time_ms"] = g["close_time_ms"].max().values
                                    else:
                                        out["close_time_ms"] = out["open_time_ms"].astype("int64") + bucket_ms
                                except Exception:
                                    out["close_time_ms"] = out["open_time_ms"].astype("int64") + bucket_ms

                                # add extra cols for packed DF compatibility
                                for _c, _v in (
                                    ("quote_volume", 0.0),
                                    ("trades", 0),
                                    ("taker_base_volume", 0.0),
                                    ("taker_quote_volume", 0.0),
                                    ("ignore", 0),
                                ):
                                    if _c not in out.columns:
                                        out[_c] = _v

                                # freshness check
                                try:
                                    max_age = float(os.getenv("MID_AGG_FROM_5M_MAX_AGE_SEC", "1800") or 1800)
                                    now_ms = int(time.time() * 1000)
                                    last_close = int(out["close_time_ms"].iloc[-1])
                                    allowed = max(max_age, float(tf_minutes) * 60.0 * 2.0)
                                    if (now_ms - last_close) <= int(allowed * 1000):
                                        if int(limit) > 0 and len(out) > int(limit):
                                            out = out.tail(int(limit)).copy()
                                        _mid_candles_cache[key] = (now, out)
                                        _mid_diag_add(symb, ex_name, market, tf, "OK", "agg_from_5m")
                                        try:
                                            if persist_enabled and tf in ("30m", "1h"):
                                                blob = db_store._cc_pack_df(out)
                                                await db_store.candles_cache_set(ex_name, (market or 'SPOT').upper().strip(), symb, tf, int(limit), blob)
                                        except Exception:
                                            pass
                                        return out
                                    else:
                                        _mid_diag_add(symb, ex_name, market, tf, "stale_agg_from_5m")
                                except Exception:
                                    pass
                        except Exception:
                            pass

                    # --- REST fetch with adaptive limit (250 -> 200 -> 120) that works even when mid_candles_retry=0 ---
                    last = None
                    base_limit = int(limit)
                    limits_to_try = []
                    try:
                        if base_limit > 0:
                            limits_to_try.append(base_limit)
                        if base_limit > 200:
                            limits_to_try.append(200)
                        if base_limit > 120:
                            limits_to_try.append(120)
                        _seen = set()
                        limits_to_try = [x for x in limits_to_try if (x not in _seen and not _seen.add(x))]
                    except Exception:
                        limits_to_try = [int(limit)]
                    
                    for _i in range(max(0, mid_candles_retry) + 1):
                        for _lim in limits_to_try:
                            limit = int(_lim)
                            last = await _mid_fetch_klines_rest(ex_name, symb, tf, limit, market)
                    
                            # If response is empty (no exception), count as EMPTY + optional adaptive behaviors
                            if last is None or getattr(last, 'empty', False):
                    
                                # Bidirectional market fallback (FUTURES <-> SPOT) when requested market yields empty/None/unsupported.
                                try:
                                    mkt_req = (market or 'SPOT').upper().strip()
                                    fb_global = str(os.getenv('MID_CANDLES_MARKET_FALLBACK', '1') or '1').strip().lower() not in ('0','false','no','off')
                                    if fb_global and mkt_req in ('FUTURES', 'SPOT'):
                                        if mkt_req == 'FUTURES':
                                            fb_on = str(os.getenv('MID_CANDLES_FUTURES_FALLBACK_SPOT', '1') or '1').strip().lower() not in ('0','false','no','off')
                                            alt = 'SPOT' if fb_on else None
                                        else:
                                            fb_on = str(os.getenv('MID_CANDLES_SPOT_FALLBACK_FUTURES', '1') or '1').strip().lower() not in ('0','false','no','off')
                                            alt = 'FUTURES' if fb_on else None
                                        if alt:
                                            alt_df = await _mid_fetch_klines_rest(ex_name, symb, tf, int(limit), alt)
                                            if alt_df is not None and not getattr(alt_df, 'empty', True):
                                                # Cache write-through under BOTH markets to suppress repeated empties.
                                                key_alt = (ex_name, alt, symb, tf, int(limit))
                                                _mid_candles_cache[key_alt] = (now, alt_df)
                                                _mid_candles_cache[key] = (now, alt_df)
                                                # write-through to persistent cache
                                                try:
                                                    persist_enabled = str(os.getenv('MID_PERSIST_CANDLES', '1') or '1').strip().lower() not in ('0','false','no','off')
                                                    if persist_enabled and tf in ('5m','30m','1h'):
                                                        blob = db_store._cc_pack_df(alt_df)
                                                        await db_store.candles_cache_set(ex_name, alt, symb, tf, int(limit), blob)
                                                        await db_store.candles_cache_set(ex_name, mkt_req, symb, tf, int(limit), blob)
                                                except Exception:
                                                    pass
                                                _mid_candles_fallback += 1
                                                _mid_diag_add(symb, ex_name, mkt_req, tf, 'OK', f"fallback_{alt.lower()}")
                                                return alt_df
                                except Exception:
                                    pass
                    
                                # If we got an empty dataframe, it often means the instrument doesn't exist for this market (esp. OKX SWAP).
                                # Re-check market availability and convert empty->unsupported_pair to avoid repeated empty storms.
                                try:
                                    mkt_now = (market or 'SPOT').upper().strip()
                                    if await api._market_supported_ex(ex_name, mkt_now, symb) is False:
                                        try:
                                            api._mark_unsupported(ex_name, mkt_now, symb, tf)
                                        except Exception:
                                            pass
                                        _mid_diag_add(symb, ex_name, mkt_now, tf, 'unsupported_pair')
                                        _mid_candles_unsupported += 1
                                        return None
                                except Exception:
                                    pass
                    
                                # If REST returned empty on the *smallest* limit for BYBIT/OKX FUTURES, treat as unsupported to stop storms.
                                try:
                                    mkt_now2 = (market or 'SPOT').upper().strip()
                                    empty_as_unsup = str(os.getenv('MID_EMPTY_FUTURES_AS_UNSUPPORTED', '1') or '1').strip().lower() not in ('0','false','no','off')
                                    is_last_lim = (int(limit) <= 120) or (limit == limits_to_try[-1])
                                    is_last_try = (_i >= max(0, mid_candles_retry))
                                    if empty_as_unsup and mkt_now2 == 'FUTURES' and ex_name in ('BYBIT','OKX') and is_last_lim and is_last_try:
                                        try:
                                            api._mark_unsupported(ex_name, mkt_now2, symb, tf)
                                        except Exception:
                                            pass
                                        _mid_diag_add(symb, ex_name, mkt_now2, tf, 'unsupported_pair', 'empty_futures')
                                        _mid_candles_unsupported += 1
                                        return None
                                except Exception:
                                    pass
                    
                                
                                # If REST returned empty repeatedly (any exchange/market), optionally treat it as unsupported
                                # to avoid repeated empty storms and let other venues win quickly.
                                try:
                                    mkt_now2 = (market or 'SPOT').upper().strip()
                                    empty_as_unsup_all = str(os.getenv('MID_EMPTY_AS_UNSUPPORTED', '1') or '1').strip().lower() not in ('0','false','no','off')
                                    min_lim = int(os.getenv('MID_EMPTY_AS_UNSUPPORTED_MIN_LIMIT', '120') or 120)
                                    is_small_lim = (int(limit) <= min_lim) or (limit == limits_to_try[0])
                                    is_last_try = (_i >= max(0, mid_candles_retry))
                                    if empty_as_unsup_all and is_small_lim and is_last_try:
                                        try:
                                            api._mark_unsupported(ex_name, mkt_now2, symb, tf)
                                        except Exception:
                                            pass
                                        _mid_diag_add(symb, ex_name, mkt_now2, tf, 'unsupported_pair', 'empty')
                                        _mid_candles_unsupported += 1
                                        return None
                                except Exception:
                                    pass
# If it was explicitly marked unsupported (e.g. market not implemented),
                                # don't count it as an "empty candles" sample.
                                if api._is_unsupported_cached(ex_name, (market or 'SPOT').upper().strip(), symb, tf):
                                    _mid_candles_unsupported += 1
                                    return None

                                # Treat persistent "empty" as unsupported_pair (most often: contract doesn't exist).
                                # This makes MID immediately try other markets/exchanges instead of accumulating empty storms.
                                try:
                                    api._mark_unsupported(ex_name, (market or 'SPOT').upper().strip(), symb, tf)
                                except Exception:
                                    pass
                                _mid_diag_add(symb, ex_name, (market or 'SPOT').upper().strip(), tf, 'unsupported_pair', 'empty')
                                _mid_candles_unsupported += 1
                                return None
                    
                            # got non-empty candles
                            break
                        else:
                            # all limits empty for this retry iteration
                            last = __import__('pandas').DataFrame()
                    
                        if last is not None and not getattr(last, 'empty', True):
                            break
                    
                        # tiny backoff reduces rate-limit bursts and improves success on flaky networks
                        if _i < max(0, mid_candles_retry):
                            try:
                                await __import__('asyncio').sleep(0.15 * (_i + 1))
                            except Exception:
                                pass
                    
                    if last is not None and not getattr(last, 'empty', True):
                        # Accept REST candles only if newest candle is reasonably fresh (prevents stale REST poisoning).
                        try:
                            rest_max_age = float(os.getenv('MID_REST_CANDLES_MAX_AGE_SEC', '1800') or 1800)
                            tf_sec = 300.0 if tf == '5m' else (1800.0 if tf == '30m' else (3600.0 if tf == '1h' else 0.0))
                            allowed = max(rest_max_age, tf_sec * 2.0) if tf_sec > 0 else rest_max_age
                            now_ms = int(__import__('time').time() * 1000)
                            if 'close_time_ms' in last.columns:
                                _last_close = int(float(last['close_time_ms'].iloc[-1]))
                            elif 'close_time' in last.columns:
                                _last_close = int(float(last['close_time'].iloc[-1]))
                            else:
                                _last_close = 0
                            if _last_close > 0 and (now_ms - _last_close) > int(allowed * 1000):
                                _mid_diag_add(symb, ex_name, market, tf, 'stale_rest')
                                last = __import__('pandas').DataFrame()
                        except Exception:
                            pass
                    
                    if last is not None and not getattr(last, 'empty', True):
                        _mid_candles_cache[key] = (now, last)
                        # write-through to persistent cache
                        try:
                            persist_enabled = str(os.getenv('MID_PERSIST_CANDLES', '1') or '1').strip().lower() not in ('0','false','no','off')
                            if persist_enabled and tf in ('5m','30m','1h'):
                                blob = db_store._cc_pack_df(last)
                                await db_store.candles_cache_set(ex_name, (market or 'SPOT').upper().strip(), symb, tf, int(limit), blob)
                        except Exception:
                            pass
                        _mid_rest_refill += 1
                        _mid_diag_ok(symb, ex_name, market, tf, 'rest', last)
                        return last
                    
                    # fallback to stale cache if available and not too old
                    if cached:
                        ts, df = cached
                        if (now - ts) <= float(_mid_tf_stale_sec(tf, kind="cache")) and df is not None and not df.empty:
                            return df
                    return None

                try:
                    async with MultiExchangeData() as api:
                        await self.macro.ensure_loaded(api.session)  # type: ignore[arg-type]
                        # Same best-effort symbols loading as MAIN scanner.
                        # --- MID symbols pool (independent from MAIN scanner cache) ---
                        if not hasattr(self, "_mid_symbols_cache"):
                            self._mid_symbols_cache = []
                            self._mid_symbols_cache_ts = 0.0
                        try:
                            symbols_pool = await api.get_top_usdt_symbols(top_n_symbols)
                            if symbols_pool:
                                # Normalize symbols across exchanges (OKX uses BTC-USDT / BTC-USDT-SWAP, Gate may use BTC_USDT).
                                # Internally we use canonical BASEQUOTE like BTCUSDT.
                                def _mid_norm_symbol(s: str) -> str:
                                    u = (s or "").upper().strip()
                                    # Remove common separators
                                    u = u.replace("-", "").replace("_", "").replace(":", "")
                                    # Strip derivative suffixes
                                    for suf in ("SWAP", "PERP", "FUT", "FUTURES"):
                                        if u.endswith(suf):
                                            u = u[: -len(suf)]
                                            break
                                    return u

                                symbols_pool = [_mid_norm_symbol(x) for x in symbols_pool if x]
                                # Production hardening: drop obviously invalid symbols early (prevents invalid_symbol_format/no_candles storms)
                                _sym_ok = re.compile(r'^[A-Z0-9]{2,20}USDT$')
                                symbols_pool = [x for x in symbols_pool if x and x.isascii() and x.endswith('USDT') and _sym_ok.match(x)]
                                # de-duplicate (some exchanges/pools can return duplicates)
                                symbols_pool = list(dict.fromkeys(symbols_pool))
                                self._mid_symbols_cache = list(symbols_pool)
                                self._mid_symbols_cache_ts = time.time()
                                # remember which exchange provided this universe (so MID prefers correct candles exchange)
                                try:
                                    _pref_ex = getattr(api, 'last_top_provider', None)
                                    if _pref_ex:
                                        self._mid_symbols_cache_provider = str(_pref_ex)
                                        self._mid_symbol_pref_exchange = {s: str(_pref_ex) for s in symbols_pool}
                                except Exception:
                                    pass
                        except Exception as e:
                            if getattr(self, "_mid_symbols_cache", None):
                                logger.warning("[mid] get_top_usdt_symbols failed (%s); using cached symbols (%s)", e, len(self._mid_symbols_cache))
                                symbols_pool = list(self._mid_symbols_cache)
                                try:
                                    _pref_ex = getattr(self, '_mid_symbols_cache_provider', None)
                                    if _pref_ex:
                                        self._mid_symbol_pref_exchange = {s: str(_pref_ex) for s in symbols_pool}
                                except Exception:
                                    pass
                            else:
                                raise
                        # Scan only first MID_TOP_N symbols from the loaded universe.
                        # FIX: remove blocked/stable symbols BEFORE slicing to TOP_N
                        _filtered_pool = [s for s in symbols_pool if not is_blocked_symbol(s)]
                        symbols = list(_filtered_pool[:max(0, int(top_n))])
                        # ensure unique symbols per tick (preserve order)
                        if symbols:
                            symbols = list(dict.fromkeys(symbols))
                        # Prefilter symbols that are not available on ANY configured MID candle source.
                        # This removes candles_unavailable storms like FXSUSDT and reduces wasted HTTP calls.
                        try:
                            prefilter = (os.getenv("MID_PREFILTER_UNSUPPORTED_SYMBOLS", "1") or "1").strip().lower() in ("1","true","yes","on")
                        except Exception:
                            prefilter = True
                        if prefilter and symbols:
                            try:
                                _kept = []
                                for _s in symbols:
                                    if is_blocked_symbol(_s):
                                        _rej_add(_s, "blocked_symbol")
                                        continue
                                    _ok = False
                                    # Try both markets because later we may fall back FUTURES->SPOT
                                    for _mkt in ("FUTURES","SPOT"):
                                        for _ex in (mid_universe or ["BINANCE","BYBIT","OKX"]):
                                            try:
                                                if _market_supported_ex(_ex, _mkt, _s):
                                                    _ok = True
                                                    break
                                            except Exception:
                                                continue
                                        if _ok:
                                            break
                                    if _ok:
                                        _kept.append(_s)
                                    else:
                                        # remember for routing diagnostics
                                        _rej_add(_s, "no_market_any")
                                symbols = _kept
                            except Exception:
                                pass
                        # --- MID tick counters / diagnostics ---
                        _mid_scanned = len(symbols)
                        _mid_pool = len(symbols_pool)
                        _mid_emitted = 0
                        _mid_skip_blocked = 0
                        _mid_skip_cooldown = 0
                        _mid_skip_macro = 0
                        _mid_skip_news = 0
                        _mid_skip_trap = 0
                        _mid_f_align = 0
                        _mid_f_score = 0
                        _mid_f_rr = 0
                        _mid_f_adx = 0
                        _mid_f_atr = 0
                        _mid_f_futoff = 0
                        _mid_candles_net_fail = 0
                        _mid_no_signal = 0
                        _mid_no_signal_reasons = {"trap":0,"structure":0,"trend":0,"extreme":0,"impulse":0,"other":0}
                        _mid_no_signal_stage_by_sym = {}
                        _mid_no_signal_stages_by_sym = {}
                        _mid_no_signal_detail_reasons_other = {}  # base_reason -> count (stage==other only)
                        _mid_no_signal_reasons_by_sym = {}      # sym -> [fail_reason...] per venue
                        _rej_reasons_added = set()  # ensure _rej_reasons_by_sym merged once per sym
  # sym -> [stage per venue with OK candles]
                        _mid_no_signal_stage_mode = str(os.getenv("MID_NO_SIGNAL_STAGE_MODE","best")).strip().lower()
                        if _mid_no_signal_stage_mode not in ("best","majority"):
                            _mid_no_signal_stage_mode = "best"

                        _mid_candles_unsupported = 0
                        _mid_candles_partial = 0
                        _mid_db_hit = 0
                        _mid_rest_refill = 0
                        _c0 = api.candle_counters_snapshot()
                        _mid_hard_block0 = _mid_hard_block_total()
                        try:
                            _mid_adapt_reset_tick()
                        except Exception:
                            pass
                        _mid_hard_block = 0

                        # --- MID reject digest (log-only): explains "scanned=N but counters don't add up" ---
                        # MID reject digest (enabled by default; set MID_REJECT_DIGEST=0 to disable)
                        _rej_enabled = os.getenv("MID_REJECT_DIGEST", "1").strip().lower() in ("1","true","yes","on")
                        _rej_max_reasons = int(os.getenv("MID_REJECT_DIGEST_MAX_REASONS", "12") or 12)
                        _rej_examples = int(os.getenv("MID_REJECT_DIGEST_EXAMPLES", "3") or 3)
                        _rej_seen = set()
                        _rej_counts = {}
                        _rej_examples_map = {}
                        _rej_full = os.getenv("MID_REJECT_DIGEST_FULL", "0").strip().lower() in ("1","true","yes","on")
                        _rej_full_max = int(os.getenv("MID_REJECT_DIGEST_FULL_MAX", str(_mid_scanned)) or _mid_scanned)
                        _rej_reason_by_sym = {}  # sym -> reason
                        # Keep a list of normalized reasons too, so 'other={...}' in [mid][summary]
                        # can show a breakdown like: other=43{score_low=6,rr_low=3,...}
                        _rej_reasons_by_sym = defaultdict(list)  # sym -> [base_reason...]

                        def _rej_add(_sym: str, _reason: str):
                            """Record exactly one terminal reason per symbol per tick."""
                            if not _rej_enabled:
                                return
                            try:
                                if _sym in _rej_seen:
                                    return
                                _rej_seen.add(_sym)
                                _rej_reason_by_sym[_sym] = _reason
                                # Also collect normalized base reason for per-symbol aggregation.
                                try:
                                    _r = str(_reason or '').strip() or 'unknown'
                                    _base = _r.split()[0] if _r else 'unknown'
                                    for _sep in ('=', ':'):
                                        if _sep in _base:
                                            _base = _base.split(_sep, 1)[0]
                                    _base = _base.strip() or 'unknown'
                                    _map = {'score': 'score_low', 'rr': 'rr_low', 'confidence': 'confidence_low'}
                                    _base = _map.get(_base, _base)
                                    if len(_base) > 64:
                                        _base = _base[:64]
                                    _rej_reasons_by_sym[str(_sym)].append(_base)
                                except Exception:
                                    pass
                                _rej_counts[_reason] = int(_rej_counts.get(_reason, 0) or 0) + 1
                                lst = _rej_examples_map.get(_reason)
                                if lst is None:
                                    lst = []
                                    _rej_examples_map[_reason] = lst
                                if len(lst) < _rej_examples:
                                    lst.append(_sym)
                            except Exception:
                                return
                        logger.info("[mid] tick start TOP_N=%s interval=%ss pool=%s scanned=%s (MID_TOP_N_SYMBOLS=%s)", top_n, interval, _mid_pool, _mid_scanned, top_n_symbols)
                        logger.info("[mid][scanner] symbols loaded: %s (pool=%s)", _mid_scanned, _mid_pool)
                        mac_act, mac_ev, mac_win = self.macro.current_action()
                        self.last_macro_action = mac_act
                        if MACRO_FILTER:
                            logger.info("[mid] macro action=%s next=%s window=%s", mac_act, getattr(mac_ev, "name", None) if mac_ev else None, mac_win)
                            if mac_act != "ALLOW" and mac_ev and mac_win and self.macro.should_notify(mac_ev):
                                logger.info("[mid][macro] alert: action=%s event=%s window=%s", mac_act, getattr(mac_ev, "name", None), mac_win)
                                await emit_macro_alert_cb(mac_act, mac_ev, mac_win, TZ_NAME)


                        # --- Architecture C: prefetch candles for all symbols (primary exchange) ---
                        mid_prefetch = os.getenv("MID_PREFETCH_CANDLES", "1").strip().lower() in ("1","true","yes","on")
                        if mid_prefetch and symbols:
                            try:
                                prefetch_limit = int(os.getenv("MID_PREFETCH_LIMIT", "250") or 250)
                                prefetch_timeout = float(os.getenv("MID_PREFETCH_TIMEOUT_SEC", "25") or 25)
                                groups = defaultdict(list)
                                for _s in symbols:
                                    if is_blocked_symbol(_s):
                                        continue
                                    groups[_mid_primary_for_symbol(_s)].append(_s)

                                tasks = []
                                for _ex, _syms in groups.items():
                                    for _s in _syms:
                                        tasks.append(asyncio.create_task(_mid_fetch_klines_cached(_ex, _s, tf_trigger, prefetch_limit, market_mid)))
                                        tasks.append(asyncio.create_task(_mid_fetch_klines_cached(_ex, _s, tf_mid, prefetch_limit, market_mid)))
                                        tasks.append(asyncio.create_task(_mid_fetch_klines_cached(_ex, _s, tf_trend, prefetch_limit, market_mid)))

                                if tasks:
                                    if prefetch_timeout and prefetch_timeout > 0:
                                        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=prefetch_timeout)
                                    else:
                                        await asyncio.gather(*tasks, return_exceptions=True)
                            except Exception as _e:
                                logger.warning("[mid] candles prefetch failed: %s", _e)

                        # Exclude prefetch time from per-symbol budget (budget applies to scan phase)
                        start_scan = time.time()
                        prefetch_elapsed = float(start_scan - start_total)
                        for sym in symbols:
                            # Hard budget per MID tick (prevents very long ticks during exchange issues)
                            if mid_tick_budget_sec and mid_tick_budget_sec > 0:
                                if (time.time() - start_scan) > float(mid_tick_budget_sec):
                                    # Mark remaining symbols as budget-exceeded (so [mid][reject] stays honest)
                                    try:
                                        for _s in symbols[symbols.index(sym):]:
                                            _rej_add(_s, 'tick_budget')
                                    except Exception:
                                        pass
                                    break
                            if is_blocked_symbol(sym):
                                _mid_skip_blocked += 1
                                _rej_add(sym, "blocked_symbol")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, "", None, "blocked_symbol")
                                except Exception:
                                    pass
                                continue
                            if not self.can_emit_mid(sym):
                                _mid_skip_cooldown += 1
                                _rej_add(sym, "cooldown")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, "", None, "cooldown")
                                except Exception:
                                    pass
                                continue

                            if MACRO_FILTER and mac_act == "PAUSE_ALL":
                                _mid_skip_macro += 1
                                _rej_add(sym, "macro_pause_all")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, "", None, "macro_pause_all")
                                except Exception:
                                    pass
                                continue
                            news_act = "OK"
                            if NEWS_FILTER and CRYPTOPANIC_TOKEN:
                                news_act = await self.news.action_for_symbol(api.session, sym)
                                if news_act == "PAUSE_ALL":
                                    _mid_skip_news += 1
                                    _rej_add(sym, "news_pause_all")
                                    try:
                                        self._mid_digest_add(self._mid_trap_digest_stats, sym, "", None, "news_pause_all")
                                    except Exception:
                                        pass
                                    continue

                            supporters = []  # kept for compatibility with later counters (not used for multi-exchange scoring)
                            chosen_name = None
                            chosen_market = None
                            # --- MID candles length requirements (diagnostics) ---
                            # If candles are present but too short for indicators, MID will behave like "no_candles".
                            # Log explicit need/got per TF to distinguish "didn't have time to collect" vs "bad data".
                            def _mid_need_bars(tf: str) -> int:
                                t = (tf or "").strip().lower()
                                try:
                                    if t in ("5m", "5min", "5"):
                                        return int(os.getenv("MID_NEED_5M_BARS", os.getenv("MID_NEED_BARS_5M", "60")) or 60)
                                    if t in ("30m", "30min", "30"):
                                        return int(os.getenv("MID_NEED_30M_BARS", os.getenv("MID_NEED_BARS_30M", "200")) or 200)
                                    # default: 1h+
                                    return int(os.getenv("MID_NEED_1H_BARS", os.getenv("MID_NEED_BARS_1H", "200")) or 200)
                                except Exception:
                                    return 0

                            
                            def _mid_diag_ok(symb: str, ex_name: str, market: str, tf: str, source: str, df: Optional[pd.DataFrame]) -> None:
                                """Add an OK/partial diag record including got/need bars.
                                If got < need -> mark as partial with need/got/missing (~time needed).
                                """
                                try:
                                    need = _mid_need_bars(tf)
                                    got = int(len(df)) if df is not None else 0
                                    if need and got < need:
                                        _mid_diag_add(symb, ex_name, market, tf, "partial",
                                                     f"need_{tf}_bars={need} got={got}" + _mid_fmt_missing_wait(tf, need, got))
                                    else:
                                        # Keep the existing OK semantics but enrich with bars count for clarity.
                                        if got:
                                            _mid_diag_add(symb, ex_name, market, tf, "OK", f"{source}(got={got})")
                                        else:
                                            _mid_diag_add(symb, ex_name, market, tf, "OK", source)
                                except Exception:
                                    _mid_diag_add(symb, ex_name, market, tf, "OK", source)

                            def _mid_len(df: Optional[pd.DataFrame]) -> int:
                                try:
                                    return int(len(df)) if df is not None else 0
                                except Exception:
                                    return 0

                            chosen_r: Optional[Dict[str, Any]] = None
                            found_ok_candles: bool = False  # candles were present and had enough bars on at least one venue

                            async def _choose_exchange_mid():
                                nonlocal chosen_name, chosen_market, chosen_r, found_ok_candles
                                primary = _mid_primary_for_symbol(sym)
                                # try primary first, then the other primary (if any), then fallbacks
                                if MID_CANDLES_LIGHT_MODE or MID_CANDLES_BINANCE_FIRST:
                                    # "binance-first" fast path: try BINANCE first, then BYBIT/OKX, then SPOT fallbacks
                                    _light_primary = ["BINANCE", "BYBIT", "OKX"]
                                    try_order = []
                                    for _x in _light_primary:
                                        if _x not in try_order:
                                            try_order.append(_x)
                                    for _x in (mid_primary_exchanges or []):
                                        if _x not in try_order:
                                            try_order.append(_x)
                                    for _x in (mid_fallback_exchanges or []):
                                        if _x not in try_order:
                                            try_order.append(_x)
                                else:
                                    try_order = [primary] + [x for x in mid_primary_exchanges if x != primary] + [x for x in mid_fallback_exchanges if x != primary]

                                # Try markets in order (AUTO: FUTURES->SPOT, or configured order)
                                for mkt in markets_try:
                                    mkt_u = (mkt or 'SPOT').upper().strip()
                                    if mkt_u == 'FUTURES':
                                        names = [x for x in try_order if x in ('BINANCE','BYBIT','OKX')]
                                    else:
                                        names = try_order
                                    for name in names:
                                        try:
                                            # Fetch trigger TF first (cuts 3x HTTP load on misses -> fewer timeouts/no_candles)
                                            a = await _mid_fetch_klines_cached(name, sym, tf_trigger, 250, mkt)
                                            if a is None or a.empty:
                                                continue

                                            b, c = await asyncio.gather(
                                                _mid_fetch_klines_cached(name, sym, tf_mid, 250, mkt),
                                                _mid_fetch_klines_cached(name, sym, tf_trend, 250, mkt),
                                                return_exceptions=True,
                                            )
                                            if isinstance(b, Exception):
                                                b = None
                                            if isinstance(c, Exception):
                                                c = None

                                            # Require true TFs for MID (no silent TF substitution).
                                            if b is None or getattr(b, 'empty', True):
                                                continue
                                            if c is None or getattr(c, 'empty', True):
                                                continue

                                            # Length sanity: distinguish "empty" vs "too few bars".
                                            n5_need = _mid_need_bars(tf_trigger)
                                            n30_need = _mid_need_bars(tf_mid)
                                            n1h_need = _mid_need_bars(tf_trend)
                                            n5_got = _mid_len(a)
                                            n30_got = _mid_len(b)
                                            n1h_got = _mid_len(c)
                                            if n5_need and n5_got < n5_need:
                                                _mid_diag_add(sym, name, mkt_u, tf_trigger, "partial", f"need_{tf_trigger}_bars={n5_need} got={n5_got}" + _mid_fmt_missing_wait(tf_trigger, n5_need, n5_got))
                                                _mid_candles_partial += 1
                                                continue
                                            if n30_need and n30_got < n30_need:
                                                _mid_diag_add(sym, name, mkt_u, tf_mid, "partial", f"need_{tf_mid}_bars={n30_need} got={n30_got}" + _mid_fmt_missing_wait(tf_mid, n30_need, n30_got))
                                                _mid_candles_partial += 1
                                                continue
                                            if n1h_need and n1h_got < n1h_need:
                                                _mid_diag_add(sym, name, mkt_u, tf_trend, "partial", f"need_{tf_trend}_bars={n1h_need} got={n1h_got}" + _mid_fmt_missing_wait(tf_trend, n1h_need, n1h_got))
                                                _mid_candles_partial += 1
                                                continue

                                            # Candles are available (non-empty + enough bars) on this venue.
                                            found_ok_candles = True
                                            _diag = {}
                                            r = evaluate_on_exchange_mid(a, b, c, symbol=sym, diag=_diag)
                                            if not r:
                                                try:
                                                    _st = str(_diag.get("fail_stage") or "other")
                                                    if _st not in _mid_no_signal_reasons:
                                                        _st = "other"
                                                    # Collect stage per venue; we'll decide final stage when rejecting as no_signal.
                                                    _mid_no_signal_stage_by_sym[sym] = _st  # last (debug)
                                                    _mid_no_signal_stages_by_sym.setdefault(sym, []).append(_st)
                                                    # Collect detailed fail reason (best-effort).
                                                    _rr = str(_diag.get("fail_reason") or ("unknown" if str(_st or '')=='other' else _st) or "other")
                                                    _rr = _rr.strip() or "other"
                                                    if len(_rr) > 64:
                                                        _rr = _rr[:64]
                                                    _mid_no_signal_reasons_by_sym.setdefault(sym, []).append(_rr)
                                                    # Merge per-symbol reject reasons collected by _rej_add (score_low/rr_low/etc)
                                                    try:
                                                        if sym not in _rej_reasons_added and '_rej_reasons_by_sym' in locals():
                                                            for _r0 in list(_rej_reasons_by_sym.get(sym) or []):
                                                                if _r0:
                                                                    _mid_no_signal_reasons_by_sym.setdefault(sym, []).append(str(_r0))
                                                            _rej_reasons_added.add(sym)
                                                    except Exception:
                                                        pass
                                                except Exception:
                                                    pass
                                            if r:
                                                chosen_name = name
                                                chosen_market = mkt
                                                chosen_r = r
                                                return
                                        except Exception:
                                            continue

# Soft timeout per symbol (prevents a single symbol/exchange from stalling the whole MID tick)
                            if mid_symbol_timeout_sec and mid_symbol_timeout_sec > 0:
                                try:
                                    await asyncio.wait_for(_choose_exchange_mid(), timeout=float(mid_symbol_timeout_sec))
                                except asyncio.TimeoutError:
                                    _rej_add(sym, f"symbol_timeout_{float(mid_symbol_timeout_sec):.0f}s")
                                    try:
                                        self._mid_digest_add(self._mid_trap_digest_stats, sym, '', None, 'symbol_timeout')
                                    except Exception:
                                        pass
                                    continue
                            else:
                                await _choose_exchange_mid()

                            if not chosen_r:
                                if found_ok_candles:
                                    # Candles were OK but no signal on any venue.
                                    _rej_add(sym, "no_signal")
                                    try:
                                        _mid_no_signal += 1
                                        stages = list(_mid_no_signal_stages_by_sym.get(sym) or [])
                                        if stages:
                                            if _mid_no_signal_stage_mode == "majority":
                                                # pick most frequent stage; tie -> earliest (keeps primary-order preference)
                                                counts = {}
                                                for s in stages:
                                                    counts[s] = counts.get(s, 0) + 1
                                                best_s = None
                                                best_n = -1
                                                for s in stages:
                                                    n = counts.get(s, 0)
                                                    if n > best_n:
                                                        best_n = n
                                                        best_s = s
                                                _st = best_s or stages[0]
                                            else:
                                                # "best": first venue in priority order that had OK candles
                                                _st = stages[0]
                                        else:
                                            _st = str(_mid_no_signal_stage_by_sym.get(sym) or "other")
                                        if _st not in _mid_no_signal_reasons:
                                            _st = "other"
                                        _mid_no_signal_reasons[_st] = int(_mid_no_signal_reasons.get(_st,0) or 0) + 1
                                        try:
                                            _rs = list(_mid_no_signal_reasons_by_sym.get(sym) or [])
                                            if _rs:
                                                if _mid_no_signal_stage_mode == 'majority':
                                                    counts2 = {}
                                                    for r0 in _rs:
                                                        counts2[r0] = counts2.get(r0, 0) + 1
                                                    best_r = None
                                                    best_n2 = -1
                                                    for r0 in _rs:
                                                        n2 = counts2.get(r0, 0)
                                                        if n2 > best_n2:
                                                            best_n2 = n2
                                                            best_r = r0
                                                    _rbest = best_r or _rs[0]
                                                else:
                                                    _rbest = _rs[0]
                                            else:
                                                _rbest = ("unknown" if str(_st or '')=='other' else str(_st or 'other'))
                                            # Count detailed no-signal reasons only for the 'other' bucket,
                                            # and normalize to a base reason name (e.g. "rsi_long=37.7" -> "rsi_long",
                                            # "near_1h_low dist=..." -> "near_1h_low").
                                            if str(_st or '') == 'other' and _rbest:
                                                _key = str(_rbest).strip()
                                                # base token (first word)
                                                _key = _key.split()[0] if _key else _key
                                                # strip value parts
                                                for _sep in ('=', ':'):
                                                    if _sep in _key:
                                                        _key = _key.split(_sep, 1)[0]
                                                _key = _key.strip()
                                                if _key and _key != 'other':
                                                    _mid_no_signal_detail_reasons_other[_key] = int(_mid_no_signal_detail_reasons_other.get(_key, 0) or 0) + 1
                                        except Exception:
                                            pass

                                    except Exception:
                                        pass
                                    continue
                                _rej_add(sym, "candles_unavailable")

                                # Log once per symbol per tick when candles are finally unavailable (prevents spam).
                                try:
                                    _ctx = _MID_TICK_CTX.get()
                                    if isinstance(_ctx, dict):
                                        _cms = _ctx.get('candles_missing_sym')
                                        if isinstance(_cms, set) and sym in _cms:
                                            pass
                                        else:
                                            if isinstance(_cms, set):
                                                _cms.add(sym)
                                            # build condensed diagnostic summary for this symbol
                                            parts = []
                                            try:
                                                for (s_key, tf_key), recs in _mid_candles_diag.items():
                                                    if s_key == sym and recs:
                                                        # keep only last few records per TF to cap size
                                                        tail = recs[-6:]
                                                        parts.append(f"{tf_key}=" + ",".join(tail))
                                            except Exception:
                                                pass
                                            diag = " | ".join(parts)
                                            if len(diag) > 700:
                                                diag = diag[:700] + "…"
                                            if diag:
                                                logger.info(f"[mid][candles_unavailable] symbol={sym} diag={diag}")
                                except Exception:
                                    pass
                                continue

                            best_name, best_r = chosen_name, chosen_r
                            base_r = best_r
                            # --- Anti-trap filters: skip candidates that look like tops/bottoms ---
                            if base_r.get("trap_ok") is False or base_r.get("blocked") is True:
                                _mid_skip_trap += 1

                                _rej_add(sym, str(base_r.get("trap_reason") or base_r.get("block_reason") or "trap_block"))

                                # collect digest stats (DO NOT forward to error-bot)
                                _r = (base_r.get("trap_reason") or base_r.get("block_reason") or "")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, str(base_r.get("direction","")), base_r.get("entry"), str(_r))
                                except Exception:
                                    pass
                                logger.info("[mid][trap] %s %s blocked=%s reason=%s src_best=%s", sym, str(base_r.get("direction","")).upper(), base_r.get("blocked"), base_r.get("trap_reason",""), best_name)
                                continue
                            if require_align and str(base_r.get("dir1","")).upper() != str(base_r.get("dir4","")).upper():
                                _mid_f_align += 1
                                if not structure_align_soft:
                                    _rej_add(sym, "align_mismatch")
                                    try:
                                        self._mid_digest_add(self._mid_trap_digest_stats, sym, str(base_r.get("direction","")), base_r.get("entry"), "align_mismatch")
                                    except Exception:
                                        pass
                                    continue
                                else:
                                    # Soft alignment: don't reject, but apply confidence penalty so only strong setups pass.
                                    try:
                                        self._mid_digest_add(self._mid_trap_digest_stats, sym, str(base_r.get("direction","")), base_r.get("entry"),
                                                           f"align_soft_penalty={structure_align_penalty:g}")
                                    except Exception:
                                        pass
                                    # Mark penalty to be applied after we read conf
                                    base_r["__align_soft_penalty"] = float(structure_align_penalty)

                            conf = float(base_r.get("confidence",0) or 0)
                            align_pen = float(base_r.get("__align_soft_penalty", 0) or 0)
                            if align_pen:
                                conf = max(0.0, conf - align_pen)
                            rr = float(base_r.get("rr",0) or 0)
                            adx30 = float(base_r.get("adx1",0) or 0)
                            adx1h = float(base_r.get("adx4",0) or 0)
                            atrp = float(base_r.get("atr_pct",0) or 0)

                            if min_adx_30m and adx30 < min_adx_30m:
                                _mid_f_adx += 1
                                _rej_add(sym, "adx_30m")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, str(base_r.get("direction","")), base_r.get("entry"), f"adx30<{min_adx_30m:g} adx={adx30:g}")
                                except Exception:
                                    pass
                                continue
                            if min_adx_1h and adx1h < min_adx_1h:
                                _mid_f_adx += 1
                                _rej_add(sym, "adx_1h")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, str(base_r.get("direction","")), base_r.get("entry"), f"adx1h<{min_adx_1h:g} adx={adx1h:g}")
                                except Exception:
                                    pass
                                continue
                            if min_atr_pct and atrp < min_atr_pct:
                                _mid_f_atr += 1
                                _rej_add(sym, "atr_pct")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, str(base_r.get("direction","")), base_r.get("entry"), f"atr_pct<{min_atr_pct:g} atr%={atrp:g}")
                                except Exception:
                                    pass
                                continue

                            market = choose_market(adx30, atrp)
                            if not allow_futures:
                                market = "SPOT"
                            if market == "FUTURES" and (news_act == "FUTURES_OFF" or mac_act == "FUTURES_OFF"):
                                _mid_f_futoff += 1
                                market = "SPOT"

                            min_conf = min_score_fut if market == "FUTURES" else min_score_spot
                            if conf < float(min_conf):
                                _mid_f_score += 1
                                _rej_add(sym, "score")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, str(base_r.get("direction","")), base_r.get("entry"), f"score<{float(min_conf):g} score={conf:g}")
                                except Exception:
                                    pass
                                continue
                            if rr < float(min_rr):
                                _mid_f_rr += 1
                                _rej_add(sym, "rr")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, str(base_r.get("direction","")), base_r.get("entry"), f"rr<{float(min_rr):g} rr={rr:g}")
                                except Exception:
                                    pass
                                continue

                            direction = str(base_r.get("direction","")).upper()
                            entry = float(base_r["entry"]); sl = float(base_r["sl"])
                            tp1 = float(base_r["tp1"]); tp2 = float(base_r["tp2"])
                            # --- HARD MID filters (TP2-first) ---
                            # 1) Volume must be real, иначе TP2 часто не добивает
                            volx = float(base_r.get("rel_vol", 0.0) or 0.0)
                            if mid_min_vol_x and volx < mid_min_vol_x:
                                _mid_f_score += 1
                                _rej_add(sym, "vol_x")
                                try:
                                    self._mid_digest_add(self._mid_trap_digest_stats, sym, str(base_r.get("direction","")), base_r.get("entry"), f"vol_x<{mid_min_vol_x:g} vol_x={volx:g}")
                                except Exception:
                                    pass
                                continue

                            # 2) Must be on the correct side of VWAP + far enough from VWAP (avoid chop)
                            vwap_val_num = float(base_r.get("vwap_val", 0.0) or 0.0)
                            atr30 = abs(entry) * (abs(atrp) / 100.0) if entry > 0 else 0.0
                            if vwap_val_num > 0 and atr30 > 0:
                                if mid_require_vwap_bias:
                                    if direction == "SHORT" and not (entry < vwap_val_num):
                                        _mid_f_align += 1
                                        _rej_add(sym, "vwap_bias")
                                        try:
                                            self._mid_digest_add(self._mid_trap_digest_stats, sym, direction, entry, f"vwap_bias_short entry>=vwap vwap={vwap_val_num:g}")
                                        except Exception:
                                            pass
                                        continue
                                    if direction == "LONG" and not (entry > vwap_val_num):
                                        _mid_f_align += 1
                                        _rej_add(sym, "vwap_bias")
                                        try:
                                            self._mid_digest_add(self._mid_trap_digest_stats, sym, direction, entry, f"vwap_bias_long entry<=vwap vwap={vwap_val_num:g}")
                                        except Exception:
                                            pass
                                        continue
                                if mid_min_vwap_dist_atr > 0:
                                    if abs(entry - vwap_val_num) < (atr30 * mid_min_vwap_dist_atr):
                                        _mid_f_atr += 1
                                        _rej_add(sym, "vwap_too_close")
                                        try:
                                            dist = abs(entry - vwap_val_num) / atr30 if atr30 else 0.0
                                            self._mid_digest_add(self._mid_trap_digest_stats, sym, direction, entry, f"vwap_far {dist:.2g}atr")
                                        except Exception:
                                            pass
                                        continue



                            # 2b) Optional: require liquidity sweep in direction (SMC confirmation)
                            if MID_REQUIRE_LIQ_SWEEP:
                                try:
                                    if direction == "LONG" and not bool(base_r.get("sweep_long", False)):
                                        _mid_f_score += 1
                                        _rej_add(sym, "liq_sweep_missing")
                                        try:
                                            self._mid_digest_add(self._mid_trap_digest_stats, sym, direction, entry, "liq_sweep_missing_long")
                                        except Exception:
                                            pass
                                        continue
                                    if direction == "SHORT" and not bool(base_r.get("sweep_short", False)):
                                        _mid_f_score += 1
                                        _rej_add(sym, "liq_sweep_missing")
                                        try:
                                            self._mid_digest_add(self._mid_trap_digest_stats, sym, direction, entry, "liq_sweep_missing_short")
                                        except Exception:
                                            pass
                                        continue
                                except Exception:
                                    pass
                            if tp_policy == "R":
                                risk = abs(entry-sl)
                                if risk <= 0:
                                    continue
                                if direction == "LONG":
                                    tp1 = entry + risk*tp1_r; tp2 = entry + risk*tp2_r
                                else:
                                    tp1 = entry - risk*tp1_r; tp2 = entry - risk*tp2_r

                        
                            # Policy: SPOT + SHORT is confusing (spot has no short). Auto-convert to FUTURES.
                            risk_note = _fmt_ta_block_mid(base_r, mode)
                            if market == "SPOT" and str(direction).upper() == "SHORT":
                                fut_forced_off = (not allow_futures) or (news_act == "FUTURES_OFF") or (mac_act == "FUTURES_OFF")
                                if fut_forced_off:
                                    _mid_f_futoff += 1
                                    _rej_add(sym, "futures_forced_off")
                                    continue
                                market = "FUTURES"
                                if risk_note:
                                    risk_note = risk_note + "\n" + "ℹ️ Auto-converted: SPOT SHORT → FUTURES"
                                else:
                                    risk_note = "ℹ️ Auto-converted: SPOT SHORT → FUTURES"

                            # Exchanges where the symbol exists (used for display and SPOT autotrade routing).
                            async def _pair_exists(ex: str) -> bool:
                                try:
                                    exu = (ex or '').upper().strip()
                                    if market == 'FUTURES':
                                        if exu == 'BINANCE':
                                            p = await self._fetch_rest_price('FUTURES', sym)
                                        elif exu == 'BYBIT':
                                            p = await self._fetch_bybit_price('FUTURES', sym)
                                        elif exu == 'OKX':
                                            p = await self._fetch_okx_price('FUTURES', sym)
                                        else:
                                            return False
                                        return bool(p and float(p) > 0)
                                    # SPOT
                                    if exu == 'BINANCE':
                                        p = await self._fetch_rest_price('SPOT', sym)
                                    elif exu == 'BYBIT':
                                        p = await self._fetch_bybit_price('SPOT', sym)
                                    elif exu == 'OKX':
                                        p = await self._fetch_okx_price('SPOT', sym)
                                    elif exu == 'MEXC':
                                        p = await _mexc_public_price(sym)
                                    else:  # GATEIO
                                        p = await _gateio_public_price(sym)
                                    return bool(p and float(p) > 0)
                                except Exception:
                                    return False

                        
                            # --- HARD BLOCK: FUTURES signal must have a real futures contract on executable venues ---
                            # If choose_market() selected FUTURES but the symbol has no futures instrument on
                            # Binance/Bybit/OKX, we skip emitting this signal. (No auto-downgrade to SPOT.)
                            if market == "FUTURES":
                                try:
                                    _fut_ok = await asyncio.gather(_pair_exists("BINANCE"), _pair_exists("BYBIT"), _pair_exists("OKX"))
                                    if not any(bool(x) for x in _fut_ok):
                                        logger.info("[scanner] skip %s FUTURES: no contract on BINANCE/BYBIT/OKX (best_source=%s)", sym, best_name)
                                        _rej_add(sym, "no_futures_contract")
                                        continue
                                except Exception as _e:
                                    logger.info("[scanner] skip %s FUTURES: futures existence check failed: %s", sym, _e)
                                    _rej_add(sym, "futures_existence_check_failed")
                                    continue

    # Exchanges where the instrument exists for the given market.
                            _ex_order = ['BINANCE', 'BYBIT', 'OKX'] if (market == 'FUTURES' or not enable_secondary) else ['BINANCE', 'BYBIT', 'OKX', 'GATEIO', 'MEXC']
                            _oks = await asyncio.gather(*[_pair_exists(x) for x in _ex_order])
                            _pair_exchanges = [x for x, ok in zip(_ex_order, _oks) if ok]
                            if not _pair_exchanges:
                                _pair_exchanges = [best_name]
                            conf_names = '+'.join(_pair_exchanges)
                            sig = Signal(
                            signal_id=self.next_signal_id(),
                            market=market,
                            symbol=sym,
                            direction=direction,
                            timeframe=f"{tf_trigger}/{tf_mid}/{tf_trend}",
                            entry=entry, sl=sl, tp1=float(tp1), tp2=float(tp2),
                            rr=float(tp2_r if tp_policy=="R" else rr),
                            confidence=int(round(conf)),
                            confirmations=conf_names,
                            source_exchange=best_name,
                            available_exchanges=conf_names,
                            risk_note=risk_note,
                            ts=time.time(),
                            )
                            self.mark_emitted_mid(sym)
                            self.last_signal = sig
                            if sig.market == "SPOT":
                                self.last_spot_signal = sig
                            else:
                                self.last_futures_signal = sig
                            await emit_signal_cb(sig)
                            _mid_emitted += 1
                            await asyncio.sleep(2)
                            _rej_add(sym, "emitted")
                except Exception:
                    logger.exception("[mid] scanner_loop_mid error")


                # --- send digest every MID_TRAP_DIGEST_SEC ---
                try:
                    self._mid_trap_digest_last_sent = await self._mid_digest_maybe_send(
                        self._mid_trap_digest_stats, float(getattr(self, "_mid_trap_digest_last_sent", time.time()))
                    )
                except Exception:
                    pass

                elapsed = time.time() - start
                try:
                    _mid_hard_block = max(0, _mid_hard_block_total() - int(_mid_hard_block0 or 0))
                    # no_candles should reflect final outcome (candles_unavailable), not intermediate misses during fallback.
                    # Otherwise a symbol that eventually succeeds still inflates no_candles.
                    _no_candles_final = 0
                    try:
                        _no_candles_final = int(_rej_counts.get('candles_unavailable', 0) or 0)
                    except Exception:
                        _no_candles_final = 0
                    # Expand no_signal 'other' into aggregated fail reasons (best-effort, safe)
                    _ns_other_details = ''
                    try:
                        _other_n = int(_mid_no_signal_reasons.get('other', 0) or 0)
                        if _other_n > 0:
                            # 0 => show all (default). >0 => show top-N + +rest.
                            try:
                                topn = int(float(os.getenv('MID_NO_SIGNAL_DETAIL_TOPN', '0') or 0))
                            except Exception:
                                topn = 0
                    
                            items = []
                            if isinstance(_mid_no_signal_detail_reasons_other, dict) and _mid_no_signal_detail_reasons_other:
                                items = sorted(_mid_no_signal_detail_reasons_other.items(),
                                               key=lambda kv: (-int(kv[1] or 0), str(kv[0])))
                    
                            if not items:
                                _ns_other_details = '{untracked=%s}' % int(_other_n)
                            else:
                                items = [(str(k), int(v or 0)) for k, v in items if v and str(k).strip() and str(k).strip() != 'other']
                                if items:
                                    if topn and topn > 0:
                                        shown = items[:topn]
                                        rest = sum(v for _, v in items[topn:])
                                    else:
                                        shown = items
                                        rest = 0
                                    _ns_other_details = '{' + ','.join([f'{k}={v}' for k, v in shown])
                                    if rest > 0:
                                        _ns_other_details += f',+{rest}'
                                    _ns_other_details += '}'
                    except Exception as e:
                        # Never break the tick due to summary formatting.
                        _ns_other_details = ''
                    
                    # Build and log summary safely (avoid syntax/formatting issues)
                    try:
                        summary = (
                            f"tick done scanned={_mid_scanned} emitted={_mid_emitted} blocked={_mid_skip_blocked} "
                            f"hardblock={_mid_hard_block} no_signal={int(_mid_no_signal)} "
                            f"[trap={int(_mid_no_signal_reasons.get('trap',0) or 0)},"
                            f"structure={int(_mid_no_signal_reasons.get('structure',0) or 0)},"
                            f"trend={int(_mid_no_signal_reasons.get('trend',0) or 0)},"
                            f"extreme={int(_mid_no_signal_reasons.get('extreme',0) or 0)},"
                            f"impulse={int(_mid_no_signal_reasons.get('impulse',0) or 0)},"
                            f"other={int(_mid_no_signal_reasons.get('other',0) or 0)}{_ns_other_details}] "
                            f"cooldown={_mid_skip_cooldown} macro={_mid_skip_macro} news={_mid_skip_news} "
                            f"align={_mid_f_align} score={_mid_f_score} rr={_mid_f_rr} adx={_mid_f_adx} atr={_mid_f_atr} futoff={_mid_f_futoff} "
                            f"no_candles={_no_candles_final} candles_net_fail={_mid_candles_net_fail} candles_unsupported={_mid_candles_unsupported} "
                            f"candles_partial={_mid_candles_partial} candles_empty={_mid_candles_empty} candles_fallback={_mid_candles_fallback} "
                            f"cache_hit={max(0, (api.candle_counters_snapshot()[0]-_c0[0]) if 'api' in locals() else 0)} "
                            f"cache_miss={max(0, (api.candle_counters_snapshot()[1]-_c0[1]) if 'api' in locals() else 0)} "
                            f"inflight_wait={max(0, (api.candle_counters_snapshot()[2]-_c0[2]) if 'api' in locals() else 0)} "
                            f"prefetch={float(prefetch_elapsed):.1f}s elapsed={float(elapsed):.1f}s total={float(time.time() - start_total):.1f}s"
                        )
                        logger.info("[mid][summary] %s", summary)

                        # Adaptive regime snapshot (avg across evaluated symbols for this tick)
                        try:
                            _snap = _mid_adapt_snapshot()
                            if _snap:
                                _reg, _atrp_avg, _t_avg, _n = _snap
                                logger.info("[mid][adaptive] regime=%s atrp=%.1f%% t=%.2f n=%s", _reg, float(_atrp_avg), float(_t_avg), int(_n))
                        except Exception:
                            pass
                    except Exception as e:
                        summary = f"tick done (summary_error={type(e).__name__}) scanned={_mid_scanned}"
                        logger.warning("[mid][summary_error] %s", e)
                        logger.info("[mid][summary] %s", summary)
                    try:
                        _tot = int(_mid_db_hit) + int(_mid_rest_refill)
                        if _tot > 0:
                            _hit_pct = int(round(100.0 * float(_mid_db_hit) / float(_tot)))
                            _miss_pct = max(0, 100 - _hit_pct)
                        else:
                            _hit_pct = 100
                            _miss_pct = 0
                        logger.info("[mid] cache_hit=%s%% cache_miss=%s%% (db_hit=%s rest_refill=%s)", _hit_pct, _miss_pct, _mid_db_hit, _mid_rest_refill)
                    except Exception:
                        pass


                    # Optional: hardblock breakdown (top buckets)
                    try:
                        if int(os.getenv("MID_HARDBLOCK_LOG", "1") or "1"):
                            hb = _mid_hardblock_dump(int(os.getenv("MID_HARDBLOCK_TOP", "6") or "6"))
                            if hb:
                                logger.info("[mid][hardblock] %s", hb)
                    except Exception:
                        pass

                    # Per-tick breakdown of where scanned symbols went.
                    if not _rej_enabled:
                        logger.info("[mid][reject] disabled (set MID_REJECT_DIGEST=1 to enable)")
                    else:
                        try:
                            accounted = len(_rej_seen)
                            missing = max(0, int(_mid_scanned) - int(accounted))
                            if missing:
                                _rej_counts["untracked"] = int(_rej_counts.get("untracked", 0) or 0) + int(missing)
                            top = sorted([(k, int(v)) for k, v in _rej_counts.items()], key=lambda x: (-x[1], x[0]))[:max(1, _rej_max_reasons)]
                            parts = []
                            for k, v in top:
                                ex = _rej_examples_map.get(k) or []
                                exs = ",".join(ex) if ex else ""
                                parts.append(f"{k}={v}" + (f" [{exs}]" if exs else ""))
                            # candles empty breakdown (top-N)
                            # Helps debug real "no candles" cases when HTTP doesn't error.
                            # IMPORTANT: do NOT spam this when candles were eventually fetched from another venue.
                            # Log it only when at least one symbol ended up with candles_unavailable in this tick.
                            try:
                                if int(_rej_counts.get('candles_unavailable', 0) or 0) > 0 and _mid_candles_empty_reasons:
                                    top_empty = sorted(_mid_candles_empty_reasons.items(), key=lambda kv: -int(kv[1] or 0))[:6]
                                    top_s = ', '.join([f'{k}={int(v)}' for k, v in top_empty])
                                    logger.info('[mid][candles_empty_top] total=%s top=%s', int(_mid_candles_empty), top_s)
                                    if _mid_candles_log_empty:
                                        for k, _v in top_empty:
                                            samp = _mid_candles_empty_samples.get(k) or []
                                            if samp:
                                                logger.info('[mid][candles_empty_samples] %s :: %s', k, ','.join(samp[:max(0, int(_mid_candles_log_empty_samples))]))
                            except Exception:
                                pass

                            logger.info("[mid][reject] scanned=%s accounted=%s missing=%s :: %s", int(_mid_scanned), int(accounted), int(missing), " | ".join(parts))

                            if _mid_candles_log_fail and _mid_candles_fail:
                                try:
                                    # top offenders
                                    items = sorted(_mid_candles_fail.items(), key=lambda kv: kv[1], reverse=True)[:6]
                                    parts2 = []
                                    for (ex, tf), cnt in items:
                                        samp = _mid_candles_fail_samples.get((ex, tf)) or []
                                        s = "; ".join(samp)
                                        parts2.append(f"{ex}:{tf}={cnt}" + (f" ({s})" if s else ""))
                                    logger.warning("[mid][candles_fail] %s", " | ".join(parts2))
                                except Exception:
                                    pass
                            # Optional: full per-symbol dump (can be verbose). Shows one reason per scanned symbol.
                            if _rej_full:
                                try:
                                    maxn = max(0, int(_rej_full_max))
                                    pairs = []
                                    for s in symbols[:maxn]:
                                        r = _rej_reason_by_sym.get(s) or "untracked"
                                        pairs.append(f"{s}:{r}")
                                    # chunk to avoid extremely long single lines
                                    chunk = 20
                                    for i in range(0, len(pairs), chunk):
                                        logger.info("[mid][reject_full] %s", " | ".join(pairs[i:i+chunk]))
                                except Exception:
                                    pass
                        except Exception:
                            pass
                    try:
                        _mid_set_last_summary(summary)
                    except Exception:
                        pass
                except Exception:
                    pass
                return elapsed
            tick_elapsed = None
            try:
                if mid_tick_timeout_sec and mid_tick_timeout_sec > 0:
                    tick_elapsed = await asyncio.wait_for(_mid_tick_body(), timeout=mid_tick_timeout_sec)
                else:
                    tick_elapsed = await _mid_tick_body()
            except asyncio.TimeoutError:
                summary = f"tick TIMEOUT after {mid_tick_timeout_sec:.1f}s (TOP_N={int(os.getenv('MID_TOP_N','50') or 50)} MID_TOP_N_SYMBOLS={int(os.getenv('MID_TOP_N_SYMBOLS', os.getenv('MID_TOP_N','50')) or (os.getenv('MID_TOP_N','50') or 50))})"
                logger.warning("[mid][summary] %s; skipped (set MID_TICK_TIMEOUT_SEC=0 to disable)", summary)
                try:
                    _mid_set_last_summary(summary)
                except Exception:
                    pass
                tick_elapsed = None
            spent = float(tick_elapsed if tick_elapsed is not None else (mid_tick_timeout_sec or 0.0))
            sleep_for = max(1, (interval_sec if interval_sec > 0 else 1) - int(spent))
            await asyncio.sleep(sleep_for)
async def autotrade_healthcheck() -> dict:
    """DB-only health snapshot for autotrade. Never places orders."""
    try:
        stale = int(os.getenv("AUTOTRADE_STALE_MINUTES", "10") or 10)
    except Exception:
        stale = 10
    try:
        return await db_store.autotrade_health_snapshot(stale_minutes=stale)
    except Exception as e:
        _log_rate_limited("autotrade_healthcheck", f"autotrade_healthcheck failed: {e}", every_s=60, level="warning")
        return {"ok": False, "error": str(e)}

async def autotrade_stress_test(*, user_id: int, symbol: str, market_type: str = "spot", n: int = 50) -> dict:
    """SAFE placeholder.

    This build keeps production stable: stress-test is disabled here to avoid accidental trading.
    """
    return {"ok": False, "error": "stress_test_disabled_in_production"}



# =========================
# WS Candles Aggregator (SPOT + FUTURES) with DB store + REST fallback support
# =========================
#
# Design:
# - Dedicated service (1 replica) subscribes to 5m kline/candles via WS for BINANCE/BYBIT/OKX
#   for both SPOT and FUTURES markets.
# - Aggregates 30m and 1h candles locally from closed 5m candles to reduce WS subscriptions.
# - Persists latest candle history to Postgres via db_store.candles_cache_set(), using the same
#   packed DF format expected by MID persistent candles cache.
#
# Usage (recommended separate Railway service):
#   WORKER_ROLE=WS_CANDLES
#   CANDLES_WS_EXCHANGES=binance,bybit,okx
#   CANDLES_WS_MARKETS=SPOT,FUTURES
#   CANDLES_WS_SYMBOLS_SOURCE=TOP_N        # or STATIC
#   CANDLES_WS_SYMBOLS_STATIC=BTCUSDT,ETHUSDT
#   CANDLES_WS_LIMIT=250
#   CANDLES_WS_DB_WRITE_EVERY_SEC=3
#
# In bot.py (or your main), start:
#   if os.getenv("WORKER_ROLE") == "WS_CANDLES":
#       await ws_candles_service_loop(backend)
#
# Notes:
# - If WS misses or reconnects, MID will still REST-refill on cache-miss. This service just makes
#   cache hits common and reduces REST load.
#
from collections import defaultdict, deque

class _WSCandlesAggregator:
    def __init__(self, *, exchanges: list[str], markets: list[str], symbols: list[str], limit: int = 250) -> None:
        self.exchanges = [str(x).lower().strip() for x in (exchanges or []) if str(x).strip()]
        self.markets = [str(x).upper().strip() for x in (markets or []) if str(x).strip()]
        self.symbols = [str(s).upper().strip() for s in (symbols or []) if str(s).strip()]
        self.limit = int(limit or 250)
        # buffers: (ex, market, symbol, tf) -> deque[dict]
        self._buf5 = defaultdict(lambda: deque(maxlen=self.limit))
        self._agg30 = defaultdict(lambda: deque(maxlen=self.limit))
        self._agg1h = defaultdict(lambda: deque(maxlen=self.limit))
        # partial accumulators for 30m/1h: (ex,market,symbol,tf)-> list[dict]
        self._acc30 = defaultdict(list)
        self._acc1h = defaultdict(list)
        # last write time per key to avoid excessive DB writes
        self._last_write = defaultdict(float)

    def _okx_inst(self, market: str, symbol: str) -> str:
        sym = (symbol or "").upper()
        if sym.endswith("USDT"):
            base, quote = sym[:-4], "USDT"
        elif sym.endswith("USDC"):
            base, quote = sym[:-4], "USDC"
        else:
            base, quote = sym[:-3], sym[-3:]
            if len(sym) >= 4 and sym[-4:] in ("USDT","USDC"):
                base, quote = sym[:-4], sym[-4:]
        inst = f"{base}-{quote}"
        if (market or "SPOT").upper() != "SPOT":
            inst = inst + "-SWAP"
        return inst

    def _tf_bucket_start(self, open_ms: int, tf_minutes: int) -> int:
        # align to tf boundary in ms
        tf_ms = tf_minutes * 60_000
        return int((open_ms // tf_ms) * tf_ms)

    def _append_5m(self, ex: str, market: str, symbol: str, c: dict) -> None:
        key = (ex, market, symbol)
        self._buf5[key].append(c)

        # build 30m + 1h from closed 5m candles
        try:
            o_ms = int(c["open_time_ms"])
        except Exception:
            return

        # 30m accumulator
        b30 = self._tf_bucket_start(o_ms, 30)
        acc30_key = (ex, market, symbol, b30)
        self._acc30[acc30_key].append(c)
        if len(self._acc30[acc30_key]) >= 6:
            grp = self._acc30.pop(acc30_key)
            self._agg30[key].append(self._agg_from_group(grp, tf="30m"))

        # 1h accumulator
        b1h = self._tf_bucket_start(o_ms, 60)
        acc1h_key = (ex, market, symbol, b1h)
        self._acc1h[acc1h_key].append(c)
        if len(self._acc1h[acc1h_key]) >= 12:
            grp = self._acc1h.pop(acc1h_key)
            self._agg1h[key].append(self._agg_from_group(grp, tf="1h"))

    def _agg_from_group(self, grp: list[dict], tf: str) -> dict:
        # grp are 5m candles (dicts), assume sorted by open_time
        grp = sorted(grp, key=lambda x: int(x.get("open_time_ms", 0)))
        o = grp[0]
        l = grp[-1]
        high = max(float(x["high"]) for x in grp)
        low = min(float(x["low"]) for x in grp)
        vol = sum(float(x.get("volume", 0.0)) for x in grp)
        return dict(
            open_time_ms=int(o["open_time_ms"]),
            close_time_ms=int(l.get("close_time_ms") or (int(l["open_time_ms"]) + 5*60_000)),
            open=float(o["open"]),
            high=float(high),
            low=float(low),
            close=float(l["close"]),
            volume=float(vol),
            tf=tf,
        )

    def _df_from_buf(self, buf: deque) -> 'pd.DataFrame':
        if not buf:
            return pd.DataFrame()
        rows = []
        for x in buf:
            rows.append([
                int(x["open_time_ms"]),
                float(x["open"]),
                float(x["high"]),
                float(x["low"]),
                float(x["close"]),
                float(x.get("volume", 0.0)),
                int(x.get("close_time_ms") or (int(x["open_time_ms"]) + 5*60_000)),
                0.0, 0, 0.0, 0.0, 0
            ])
        # match binance-like columns; indicators need open_time/open/high/low/close/volume
        df = pd.DataFrame(rows, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","quote_volume","n_trades","taker_base","taker_quote","ignore"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        for col in ("open","high","low","close","volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    async def _maybe_write_db(self, ex: str, market: str, symbol: str, tf: str, df: 'pd.DataFrame') -> None:
        try:
            import db_store  # local module
        except Exception:
            return
        if df is None or df.empty:
            return
        write_every = float(os.getenv("CANDLES_WS_WRITE_EVERY_SEC", os.getenv("CANDLES_WS_DB_WRITE_EVERY_SEC", "3")) or 3)
        key = (ex, market, symbol, tf)
        now = time.time()
        if (now - float(self._last_write.get(key, 0.0))) < write_every:
            return
        self._last_write[key] = now
        try:
            blob = db_store._cc_pack_df(df)
            await db_store.candles_cache_set(ex.upper(), market.upper(), symbol.upper(), tf, int(self.limit), blob)
        except Exception:
            # do not crash on DB failures
            return

    async def _flush_symbol(self, ex: str, market: str, symbol: str) -> None:
        key = (ex, market, symbol)
        # 5m
        df5 = self._df_from_buf(self._buf5.get(key) or deque())
        await self._maybe_write_db(ex, market, symbol, "5m", df5)
        # 30m
        df30 = self._df_from_buf(self._agg30.get(key) or deque())
        await self._maybe_write_db(ex, market, symbol, "30m", df30)
        # 1h
        df1h = self._df_from_buf(self._agg1h.get(key) or deque())
        await self._maybe_write_db(ex, market, symbol, "1h", df1h)

    async def _binance_loop(self, market: str) -> None:
        # One connection per market, 5m streams for all symbols
        import websockets
        m = (market or "SPOT").upper()
        base = "wss://stream.binance.com:9443" if m == "SPOT" else "wss://fstream.binance.com"
        streams = "/stream?streams=" + "/".join([f"{s.lower()}@kline_5m" for s in self.symbols])
        url = base + streams
        backoff = 1
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=2048) as ws:
                    backoff = 1
                    async for msg in ws:
                        try:
                            j = json.loads(msg)
                            data = j.get("data") or {}
                            k = data.get("k") or {}
                            if not k:
                                continue
                            sym = (k.get("s") or "").upper()
                            is_closed = bool(k.get("x"))
                            if not is_closed:
                                continue
                            o_ms = int(k.get("t"))
                            c_ms = int(k.get("T"))
                            c = dict(
                                open_time_ms=o_ms,
                                close_time_ms=c_ms,
                                open=float(k.get("o")),
                                high=float(k.get("h")),
                                low=float(k.get("l")),
                                close=float(k.get("c")),
                                volume=float(k.get("v") or 0.0),
                                tf="5m",
                            )
                            self._append_5m("binance", m, sym, c)
                            await self._flush_symbol("binance", m, sym)
                        except Exception:
                            continue
            except Exception:
                await asyncio.sleep(min(30, backoff))
                backoff = min(30, backoff * 2)

    async def _bybit_loop(self, market: str) -> None:
        # Public v5 websockets; subscribe to 5m kline topics
        import websockets
        m = (market or "SPOT").upper()
        url = "wss://stream.bybit.com/v5/public/spot" if m == "SPOT" else "wss://stream.bybit.com/v5/public/linear"
        sub = {"op": "subscribe", "args": [f"kline.5.{s.upper()}" for s in self.symbols]}
        backoff = 1
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=4096) as ws:
                    backoff = 1
                    try:
                        await ws.send(json.dumps(sub))
                    except Exception:
                        pass
                    async for msg in ws:
                        try:
                            j = json.loads(msg)
                            topic = str(j.get("topic") or "")
                            if not topic.startswith("kline."):
                                continue
                            data = j.get("data")
                            if not data:
                                continue
                            # data can be dict or list; normalize to list
                            if isinstance(data, dict):
                                data = [data]
                            for k in data:
                                # bybit kline fields: start/end/open/high/low/close/volume, confirm
                                is_closed = bool(k.get("confirm") or k.get("is_confirm"))
                                if not is_closed:
                                    continue
                                sym = (k.get("symbol") or "").upper()
                                o_ms = int(k.get("start") or 0)
                                c_ms = int(k.get("end") or (o_ms + 5*60_000))
                                c = dict(
                                    open_time_ms=o_ms,
                                    close_time_ms=c_ms,
                                    open=float(k.get("open")),
                                    high=float(k.get("high")),
                                    low=float(k.get("low")),
                                    close=float(k.get("close")),
                                    volume=float(k.get("volume") or 0.0),
                                    tf="5m",
                                )
                                self._append_5m("bybit", m, sym, c)
                                await self._flush_symbol("bybit", m, sym)
                        except Exception:
                            continue
            except Exception:
                await asyncio.sleep(min(30, backoff))
                backoff = min(30, backoff * 2)

    async def _okx_loop(self, market: str) -> None:
        import websockets
        m = (market or "SPOT").upper()
        url = "wss://ws.okx.com:8443/ws/v5/public"
        args = [{"channel":"candle5m","instId": self._okx_inst(m, s)} for s in self.symbols]
        sub = {"op":"subscribe","args": args}
        backoff = 1
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=4096) as ws:
                    backoff = 1
                    try:
                        await ws.send(json.dumps(sub))
                    except Exception:
                        pass
                    async for msg in ws:
                        try:
                            j = json.loads(msg)
                            if j.get("event"):
                                continue
                            arg = j.get("arg") or {}
                            ch = str(arg.get("channel") or "")
                            if ch != "candle5m":
                                continue
                            inst = str(arg.get("instId") or "")
                            data = j.get("data") or []
                            if not data:
                                continue
                            # OKX candle array: [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
                            row = data[0]
                            if len(row) < 6:
                                continue
                            ts = int(row[0])
                            o,h,l,c0,vol = row[1],row[2],row[3],row[4],row[5]
                            # inst -> symbol
                            sym = inst.replace("-SWAP","").replace("-","")
                            cdict = dict(
                                open_time_ms=ts,
                                close_time_ms=ts + 5*60_000,
                                open=float(o), high=float(h), low=float(l), close=float(c0),
                                volume=float(vol or 0.0),
                                tf="5m",
                            )
                            self._append_5m("okx", m, sym, cdict)
                            await self._flush_symbol("okx", m, sym)
                        except Exception:
                            continue
            except Exception:
                await asyncio.sleep(min(30, backoff))
                backoff = min(30, backoff * 2)

    async def run(self) -> None:
        tasks = []
        for ex in self.exchanges:
            for m in self.markets:
                if ex == "binance":
                    tasks.append(asyncio.create_task(self._binance_loop(m)))
                elif ex == "bybit":
                    tasks.append(asyncio.create_task(self._bybit_loop(m)))
                elif ex == "okx":
                    tasks.append(asyncio.create_task(self._okx_loop(m)))
        if not tasks:
            return
        await asyncio.gather(*tasks)

async def ws_candles_service_loop(backend: 'Backend') -> None:
    """Run WS candles aggregator and persist candles to DB cache for SPOT and FUTURES.

    Important: the scanner symbol list is often empty at process start (it is loaded by loops),
    so this service waits until symbols become available, and then keeps them refreshed
    (by restarting subscriptions when the symbol set changes).
    """
    import asyncio
    import logging
    import os
    import re
    import time

    logger = logging.getLogger("crypto-signal")

    exchanges = [x.strip().lower() for x in str(os.getenv("CANDLES_WS_EXCHANGES", "binance,bybit,okx") or "").split(",") if x.strip()]
    _m_raw = os.getenv("CANDLES_WS_MARKET", os.getenv("CANDLES_WS_MARKETS", "SPOT,FUTURES") or "SPOT,FUTURES")
    markets = [x.strip().upper() for x in str(_m_raw or "").split(",") if x.strip()]
    limit = int(os.getenv("CANDLES_WS_LIMIT", "250") or 250)
    resub_sec = int(os.getenv("CANDLES_WS_RESUB_CHECK_SEC", os.getenv("CANDLES_WS_RESUB_CHECK", "30")) or 30)

    src = str(os.getenv("CANDLES_WS_SYMBOLS_SOURCE", "BOTH_POOLS") or "BOTH_POOLS").upper().strip()
    # BOTH_POOLS = MID pool + main scanner pool; MID_POOL = MID scanner pool; TOP_N = backend.load_top_symbols; STATIC = env list
    if src not in ("BOTH_POOLS", "MID_POOL", "TOP_N", "STATIC"):
        src = "BOTH_POOLS"

    def _sanitize_symbols(symbols: list[str]) -> list[str]:
        clean: list[str] = []
        for s in symbols or []:
            s2 = re.sub(r"[^A-Z0-9]", "", (s or "").upper())
            if not s2.endswith(("USDT", "USDC")):
                continue
            if len(s2) < 6 or len(s2) > 30:
                continue
            clean.append(s2)
        # de-dupe keep order
        return list(dict.fromkeys(clean))

    def _extract_cache_symbols(v) -> list[str]:
        """Extract symbols list from various cache shapes used in Backend."""
        try:
            if not v:
                return []
            if isinstance(v, (list, tuple)):
                if v and isinstance(v[-1], list):
                    return list(v[-1])
                if v and isinstance(v[0], str):
                    return list(v)
            if isinstance(v, dict):
                cand = v.get("symbols") or v.get("data") or v.get("value") or []
                if isinstance(cand, (list, tuple)):
                    return list(cand)
            return []
        except Exception:
            return []


    async def _get_symbols() -> list[str]:
        # 1) STATIC
        if src == "STATIC":
            raw = str(os.getenv("CANDLES_WS_SYMBOLS_STATIC", "") or "")
            return _sanitize_symbols([s.strip() for s in raw.split(",") if s.strip()])

        # 2) Pools from running scanners (best: no extra REST)
        if src in ("BOTH_POOLS", "MID_POOL"):
            try:
                mid_max = int(os.getenv("CANDLES_WS_TOP_N_MID", os.getenv("MID_TOP_N_SYMBOLS", "150")) or 150)
            except Exception:
                mid_max = 150
            try:
                main_max = int(os.getenv("CANDLES_WS_TOP_N_MAIN", os.getenv("TOP_N", "10")) or 10)
            except Exception:
                main_max = 10

            mid_syms: list[str] = []
            main_syms: list[str] = []

            # MID pool (scanner_loop_mid)
            for attr in ("_mid_symbols_cache", "mid_symbols_cache"):
                mid_syms = _extract_cache_symbols(getattr(backend, attr, None))
                if mid_syms:
                    break

            # MAIN pool (scanner_loop)
            if src == "BOTH_POOLS":
                for attr in ("_symbols_cache", "symbols_cache"):
                    main_syms = _extract_cache_symbols(getattr(backend, attr, None))
                    if main_syms:
                        break

            mid_syms = _sanitize_symbols(list(mid_syms)[: max(0, mid_max)])
            main_syms = _sanitize_symbols(list(main_syms)[: max(0, main_max)])

            if mid_syms or main_syms:
                out: list[str] = []
                seen: set[str] = set()
                for s in (mid_syms + main_syms):
                    if s not in seen:
                        out.append(s)
                        seen.add(s)
                return out

        # 3) TOP_N via backend (fallback)
        try:
            top_n = int(os.getenv("CANDLES_WS_TOP_N", os.getenv("MID_TOP_N_SYMBOLS", os.getenv("TOP_N", "100"))) or 100)
        except Exception:
            top_n = 100
        try:
            syms = await backend.load_top_symbols(top_n=top_n)  # type: ignore
            return _sanitize_symbols(list(syms or []))
        except Exception:
            raw = str(os.getenv("CANDLES_WS_SYMBOLS_STATIC", "") or "")
            return _sanitize_symbols([s.strip() for s in raw.split(",") if s.strip()])

    logger.info("[ws-candles] service starting exchanges=%s markets=%s limit=%s src=%s resub=%ss",
                exchanges, markets, limit, src, resub_sec)

    current: list[str] = []
    agg_task: asyncio.Task | None = None
    last_wait_log = 0.0

    while True:
        try:
            symbols = await _get_symbols()
            if not symbols:
                now = time.time()
                if now - last_wait_log > 30:
                    logger.info("[ws-candles] waiting for symbols... (scanner pools not ready yet)")
                    last_wait_log = now
                await asyncio.sleep(5)
                continue

            # Restart aggregator when symbol set changes or when task died
            need_restart = (symbols != current) or (agg_task is None) or (agg_task.done())

            if agg_task is not None and agg_task.done():
                try:
                    exc = agg_task.exception()
                    if exc:
                        logger.warning("[ws-candles] aggregator crashed: %s", exc)
                except Exception:
                    pass
                agg_task = None
                current = []

            if need_restart:
                # Stop previous (if running)
                if agg_task is not None and (not agg_task.done()):
                    try:
                        agg_task.cancel()
                        await asyncio.wait_for(agg_task, timeout=10)
                    except Exception:
                        pass
                    agg_task = None

                current = symbols
                logger.info("[ws-candles] (re)starting exchanges=%s markets=%s symbols=%s limit=%s",
                            exchanges, markets, len(current), limit)
                agg = _WSCandlesAggregator(exchanges=exchanges, markets=markets, symbols=current, limit=limit)
                agg_task = asyncio.create_task(agg.run(), name="ws-candles-aggregator")

            await asyncio.sleep(max(10, resub_sec))
        except asyncio.CancelledError:
            if agg_task is not None:
                agg_task.cancel()
            raise
        except Exception:
            logger.exception("[ws-candles] service loop error")
            await asyncio.sleep(5)

# ============================================
# Candles cache cleanup loop
# ============================================

async def candles_cache_cleanup_loop(backend: 'Backend') -> None:
    """Periodically purge stale candles_cache rows (keys not updated recently).

    candles_cache stores ONE row per key (ex/market/symbol/tf/limit). Over time, keys for delisted
    symbols or disabled markets can remain. This loop deletes rows whose updated_at is older than
    a configured threshold.

    Env:
      CANDLES_CACHE_CLEANUP_EVERY_SEC (default 14400 = 4h)
      CANDLES_CACHE_PURGE_MAX_AGE_SEC (default 86400 = 24h)
      CANDLES_CACHE_CLEANUP_ENABLED (default 1)
    """
    import asyncio
    import logging
    import os

    log = logging.getLogger('crypto-signal')

    enabled = str(os.getenv('CANDLES_CACHE_CLEANUP_ENABLED', '1') or '1').strip().lower() not in ('0','false','no','off')
    if not enabled:
        log.info('[candles-cleanup] disabled (CANDLES_CACHE_CLEANUP_ENABLED=0)')
        return

    every_sec = int(os.getenv('CANDLES_CACHE_CLEANUP_EVERY_SEC', '14400') or 14400)
    max_age_sec = int(os.getenv('CANDLES_CACHE_PURGE_MAX_AGE_SEC', '86400') or 86400)

    log.info('[candles-cleanup] started interval=%ss max_age=%ss', every_sec, max_age_sec)

    while True:
        try:
            deleted = 0
            try:
                deleted = await db_store.candles_cache_purge(max_age_sec)
            except Exception:
                # db_store may not be ready yet; retry next cycle
                log.exception('[candles-cleanup] purge failed')
            else:
                log.info('[candles-cleanup] deleted=%s stale keys (older than %ss)', deleted, max_age_sec)
        except Exception:
            log.exception('[candles-cleanup] unexpected error')

        await asyncio.sleep(max(60, every_sec))


# ===============================
# INSTITUTIONAL TA ENGINE (AUTO)
# ===============================

# --- Institutional token analysis (multi-timeframe) ---
_I18N_CACHE = {"data": None, "mtime": 0.0}

def _load_i18n_dict() -> dict:
    """Loads i18n.json next to this file. Cached by mtime."""
    import os, json, time
    try:
        p = os.path.join(os.path.dirname(__file__), "i18n.json")
        st = os.stat(p)
        if _I18N_CACHE["data"] is None or float(st.st_mtime) != float(_I18N_CACHE["mtime"]):
            _I18N_CACHE["data"] = json.loads(open(p, "r", encoding="utf-8").read())
            _I18N_CACHE["mtime"] = float(st.st_mtime)
        return _I18N_CACHE["data"] or {}
    except Exception:
        return {}

def _tr_i18n(lang: str, key: str, **kwargs) -> str:
    d = _load_i18n_dict()
    lang = (lang or "ru").lower()
    if lang not in d:
        lang = "ru"
    s = None
    try:
        s = d.get(lang, {}).get(key)
        if s is None:
            # fallback to EN, then key
            s = d.get("en", {}).get(key, key)
        if kwargs:
            return str(s).format(**kwargs)
        return str(s)
    except Exception:
        try:
            return str(s) if s is not None else key
        except Exception:
            return key

def _fmt_int_space(x) -> str:
    """Human-friendly price/level formatter.

    Previously this formatted everything as an integer, which made low-priced
    assets (e.g., PEPE at 0.0000...) display as 0. We keep the function name for
    compatibility but format adaptively.
    """
    try:
        v = float(x)
        if not (v == v) or v in (float("inf"), float("-inf")):
            return "—"

        av = abs(v)
        if av >= 1000:
            s = f"{v:,.0f}"
        elif av >= 1:
            s = f"{v:,.2f}"
        elif av >= 0.01:
            s = f"{v:.4f}"
        elif av >= 0.0001:
            s = f"{v:.6f}"
        else:
            s = f"{v:.8f}"

        return s.replace(",", " ")
    except Exception:
        return "—"

def _fmt_float(x, nd=2) -> str:
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return "—"


async def analyze_symbol_institutional(self, symbol: str, market: str = "FUTURES", lang: str = "ru") -> str:
    """
    Institutional-style TA analysis.
    Output format is Telegram Markdown and follows the required template.
    Uses 5m/1h/4h candles + indicators from _add_indicators().
    Includes lightweight "pro" extras: structure, swing levels, trendlines, channel, breakout/retest, liquidity, regime.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return "⚠️ empty symbol"
    # allow entering BTC or BTCUSDT
    if not sym.endswith("USDT") and sym.isalnum() and len(sym) <= 12:
        sym = sym + "USDT"
    mkt = (market or "FUTURES").strip().upper()
    lang = (lang or "ru").lower()

    def _df_valid(df) -> bool:
        """Strict validation to prevent fake 0-valued analysis when candles fetch fails."""
        try:
            if df is None or getattr(df, "empty", True):
                return False
            if len(df) < 60:
                return False
            # Guard against placeholder frames (all zeros)
            for col in ("close", "high", "low"):
                if col in df.columns:
                    try:
                        mx = float(df[col].astype(float).max())
                    except Exception:
                        return False
                    if mx <= 0:
                        return False
            if "close" in df.columns:
                try:
                    if float(df["close"].astype(float).iloc[-1]) <= 0:
                        return False
                except Exception:
                    return False
            return True
        except Exception:
            return False

    # --------- AUTO market selection (SPOT priority -> FUTURES fallback) ---------
    if mkt == "AUTO":
        candidates: list[tuple[str, str]] = [("SPOT", sym), ("FUTURES", sym)]

        # Meme-contract alias: futures may use 1000x contract tickers (e.g., 1000PEPEUSDT)
        if sym.endswith("USDT"):
            base = sym[:-4]
            if base in ("PEPE",):
                candidates.append(("FUTURES", f"1000{base}USDT"))

        chosen_mkt = None
        chosen_sym = None
        df5 = None
        df1 = None
        df4 = None

        for cmkt, csym in candidates:
            try:
                t5 = await self.load_candles(csym, "5m", cmkt)
            except Exception:
                t5 = None
            if _df_valid(t5):
                chosen_mkt, chosen_sym = cmkt, csym
                df5 = t5
                # load higher TFs best-effort
                try:
                    df1 = await self.load_candles(csym, "1h", cmkt)
                except Exception:
                    df1 = None
                try:
                    df4 = await self.load_candles(csym, "4h", cmkt)
                except Exception:
                    df4 = None
                break

        if chosen_mkt is None or chosen_sym is None or not _df_valid(df5):
            hint = ""
            if sym.endswith("USDT") and sym[:-4] == "PEPE":
                hint = "\n\nПодсказка: для PEPE фьючерса часто используется 1000PEPEUSDT"
            return f"❌ Нет данных для {sym} (AUTO).{hint}"

        # apply chosen
        mkt = chosen_mkt
        sym = chosen_sym

    else:
        # ---------- load candles ----------
        df5 = await self.load_candles(sym, "5m", mkt)
        df1 = await self.load_candles(sym, "1h", mkt)
        df4 = await self.load_candles(sym, "4h", mkt)

    # Validate candles (avoid fake 0-valued analysis)
    if not _df_valid(df5):
        hint = ""
        if sym.endswith("USDT") and sym[:-4] == "PEPE":
            hint = "\n\nПодсказка: для PEPE фьючерса часто используется 1000PEPEUSDT"
        return f"❌ Нет свечей/цены для {sym} ({mkt}).{hint}"

    # ---------- indicators ----------
    df5 = _add_indicators(df5)
    if df1 is not None and not getattr(df1, "empty", True) and len(df1) >= 60:
        df1 = _add_indicators(df1)
    if df4 is not None and not getattr(df4, "empty", True) and len(df4) >= 60:
        df4 = _add_indicators(df4)

    price = float(df5["close"].astype(float).iloc[-1])

    # Pick base df for "structure/pro": prefer 1h, else 5m
    df_struct = df1 if (df1 is not None and not getattr(df1, "empty", True) and len(df1) >= 120) else df5
    df_struct_w = df_struct.tail(240).reset_index(drop=True)

    # ---------- levels (support/resistance + pivots) ----------
    try:
        d1w = df1.tail(120) if (df1 is not None and not getattr(df1, "empty", True) and len(df1) >= 120) else df5.tail(240)
        support1 = float(d1w["low"].astype(float).min())
        resistance1 = float(d1w["high"].astype(float).max())
        last_close_1h = float(d1w["close"].astype(float).iloc[-1])
    except Exception:
        d1w = df5.tail(240)
        support1 = float(d1w["low"].astype(float).min())
        resistance1 = float(d1w["high"].astype(float).max())
        last_close_1h = float(d1w["close"].astype(float).iloc[-1])

    rng = max(1e-9, (resistance1 - support1))
    support2 = support1 - rng * 0.5
    resistance2 = resistance1 + rng * 0.5
    pivot = (support1 + resistance1) / 2.0

    # Classic pivot points (using last ~24 1h candles, fallback to 5m window)
    try:
        piv_src = (df1.tail(24) if (df1 is not None and not getattr(df1, "empty", True) and len(df1) >= 24) else df5.tail(288))
        H = float(piv_src["high"].astype(float).max())
        L = float(piv_src["low"].astype(float).min())
        C = float(piv_src["close"].astype(float).iloc[-2]) if len(piv_src) >= 2 else float(piv_src["close"].astype(float).iloc[-1])
        P = (H + L + C) / 3.0
        R1 = 2 * P - L
        S1 = 2 * P - H
        R2 = P + (H - L)
        S2 = P - (H - L)
    except Exception:
        P = pivot
        R1, R2 = resistance1, resistance2
        S1, S2 = support1, support2

    # ---------- indicators (5m) ----------
    rsi = float(df5["rsi"].iloc[-1]) if "rsi" in df5.columns else 50.0
    macd = float(df5["macd_hist"].iloc[-1]) if "macd_hist" in df5.columns else 0.0
    adx5 = float(df5["adx"].iloc[-1]) if "adx" in df5.columns else 0.0
    atr_pct = float(df5["atr_pct"].iloc[-1]) if "atr_pct" in df5.columns else 0.0
    # ATR for SL using ENV-configurable timeframe
    atr_abs, atr_tf = pick_atr_for_sl(df5, df1, df4)

    # volume relative (current volume / avg prev 40)
    vol_rel = 1.0
    try:
        if "volume" in df5.columns:
            v = df5["volume"].astype(float)
            if len(v) >= 50:
                base = float(v.tail(41).head(40).mean())
                cur = float(v.iloc[-1])
                if base > 0:
                    vol_rel = cur / base
    except Exception:
        vol_rel = 1.0

    # Volume trend + spikes
    vol_trend = "—"
    vol_spike = "—"
    try:
        if "volume" in df5.columns and len(df5) >= 60:
            v = df5["volume"].astype(float).tail(60)
            ma_short = float(v.tail(10).mean())
            ma_long = float(v.tail(40).mean())
            if ma_long > 0:
                ratio = ma_short / ma_long
                if ratio >= 1.15:
                    vol_trend = "↑"
                elif ratio <= 0.87:
                    vol_trend = "↓"
                else:
                    vol_trend = "→"
            # spike
            spike_thr = float(v.tail(40).mean()) * 1.8 if float(v.tail(40).mean()) > 0 else 0
            if spike_thr > 0 and float(v.iloc[-1]) >= spike_thr:
                vol_spike = "SPIKE"
    except Exception:
        pass

    # ---------- trend labels (multi-timeframe) ----------
    def _trend_label(df):
        try:
            ema50 = float(df["ema50"].iloc[-1])
            ema200 = float(df["ema200"].iloc[-1])
            adx = float(df["adx"].iloc[-1])
        except Exception:
            return _tr_i18n(lang, "analysis_trend_pullback")
        if ema50 > ema200 and adx >= 25:
            return _tr_i18n(lang, "analysis_trend_strong_up")
        if ema50 > ema200:
            return _tr_i18n(lang, "analysis_trend_up")
        if ema50 < ema200 and adx >= 25:
            return _tr_i18n(lang, "analysis_trend_strong_down")
        return _tr_i18n(lang, "analysis_trend_pullback")

    trend4 = _trend_label(df4) if (df4 is not None and not getattr(df4, "empty", True)) else "—"
    trend1 = _trend_label(df1) if (df1 is not None and not getattr(df1, "empty", True)) else "—"
    trend5 = _trend_label(df5)

    # ---------- swings / structure ----------
    def _swing_points(_df: pd.DataFrame, left: int = 3, right: int = 3):
        """Return (swing_highs, swing_lows) as lists of tuples (idx, price)."""
        try:
            if _df is None or getattr(_df, "empty", True) or len(_df) < (left + right + 10):
                return [], []
            highs = _df["high"].astype(float).values
            lows = _df["low"].astype(float).values
            sh, sl = [], []
            for i in range(left, len(_df) - right):
                h = highs[i]
                l = lows[i]
                if h == max(highs[i-left:i+right+1]):
                    sh.append((i, float(h)))
                if l == min(lows[i-left:i+right+1]):
                    sl.append((i, float(l)))
            return sh, sl
        except Exception:
            return [], []

    def _structure_label(_sh, _sl) -> str:
        """HH/HL vs LH/LL using last 2 swings."""
        try:
            if len(_sh) < 2 or len(_sl) < 2:
                return "RANGE"
            h1, h2 = _sh[-2][1], _sh[-1][1]
            l1, l2 = _sl[-2][1], _sl[-1][1]
            if h2 > h1 and l2 > l1:
                return "HH/HL"
            if h2 < h1 and l2 < l1:
                return "LH/LL"
            return "RANGE"
        except Exception:
            return "RANGE"

    sh, sl = _swing_points(df_struct_w, left=3, right=3)
    struct_lbl = _structure_label(sh, sl)

    swing_hi = sh[-1][1] if len(sh) else resistance1
    swing_lo = sl[-1][1] if len(sl) else support1

    # ---------- trendlines + channel (swing-based, fallback to regression) ----------
    def _line_from_two(p1, p2, x):
        try:
            (x1, y1), (x2, y2) = p1, p2
            if x2 == x1:
                return float(y2)
            m = (y2 - y1) / (x2 - x1)
            return float(y1 + m * (x - x1))
        except Exception:
            return float("nan")

    tl_support = None
    tl_resist = None
    chan_type = None
    chan_support = support1
    chan_resist = resistance1
    chan_mid = pivot

    try:
        last_x = len(df_struct_w) - 1

        # Trendlines from last 2 swing lows/highs
        if len(sl) >= 2:
            tl_support = _line_from_two(sl[-2], sl[-1], last_x)
        if len(sh) >= 2:
            tl_resist = _line_from_two(sh[-2], sh[-1], last_x)

        # 1) Prefer swing-based channel when both trendlines exist and form a sane band
        if (tl_support is not None and tl_support == tl_support) and (tl_resist is not None and tl_resist == tl_resist):
            if tl_support < tl_resist:
                chan_support = float(tl_support)
                chan_resist = float(tl_resist)
                chan_mid = (chan_support + chan_resist) / 2.0

                # type from slope of support line (last 2 swing lows)
                try:
                    (x1, y1), (x2, y2) = sl[-2], sl[-1]
                    slope = (y2 - y1) / max(1e-9, (x2 - x1))
                except Exception:
                    slope = 0.0

                if slope > 0:
                    chan_type = "Восходящий канал" if lang.startswith("ru") else "Ascending channel"
                elif slope < 0:
                    chan_type = "Нисходящий канал" if lang.startswith("ru") else "Descending channel"
                else:
                    chan_type = "Боковой канал" if lang.startswith("ru") else "Range channel"

        # 2) Fallback: regression channel on closes (useful when swings are sparse)
        if chan_type is None:
            y = df_struct_w["close"].astype(float).values
            x = np.arange(len(y), dtype=float)
            if len(y) >= 80:
                x_mean = float(x.mean())
                y_mean = float(y.mean())
                denom = float(((x - x_mean) ** 2).sum())
                slope = float((((x - x_mean) * (y - y_mean)).sum()) / denom) if denom > 0 else 0.0
                intercept = y_mean - slope * x_mean
                y_hat = intercept + slope * x
                resid = y - y_hat
                band = float(np.std(resid)) * 2.0  # ~2 sigma
                ch_mid = float(y_hat[-1])
                ch_up = float(ch_mid + band)
                ch_dn = float(ch_mid - band)

                if ch_dn < ch_up and abs(ch_mid) > 0:
                    chan_support = ch_dn
                    chan_resist = ch_up
                    chan_mid = ch_mid

                if slope > 0:
                    chan_type = "Восходящий канал" if lang.startswith("ru") else "Ascending channel"
                elif slope < 0:
                    chan_type = "Нисходящий канал" if lang.startswith("ru") else "Descending channel"
                else:
                    chan_type = "Боковой канал" if lang.startswith("ru") else "Range channel"
            else:
                chan_type = "Канал" if lang.startswith("ru") else "Channel"
                chan_mid = (chan_support + chan_resist) / 2.0
    except Exception:
        chan_support = support1
        chan_resist = resistance1
        chan_mid = pivot
        chan_type = "Канал" if lang.startswith("ru") else "Channel"

    # Price position inside channel (near support / mid / resistance)
    chan_pos = "—"
    try:
        tol_pos = max(atr_abs * 0.6, price * 0.001)
        if abs(price - chan_support) <= tol_pos:
            chan_pos = "у поддержки" if lang.startswith("ru") else "near support"
        elif abs(price - chan_resist) <= tol_pos:
            chan_pos = "у сопротивления" if lang.startswith("ru") else "near resistance"
        elif abs(price - chan_mid) <= tol_pos:
            chan_pos = "у середины" if lang.startswith("ru") else "near midpoint"
        else:
            chan_pos = "внутри канала" if lang.startswith("ru") else "inside channel"
    except Exception:
        pass

# ---------- breakout / retest ----------
    brk = "—"
    try:
        last_close = float(df5["close"].astype(float).iloc[-1])
        tol = max(atr_abs * 0.6, (last_close * 0.001))  # ~0.1% or 0.6 ATR
        if last_close > max(resistance1, chan_resist) + tol:
            brk = "Пробой ↑" if lang.startswith("ru") else "Breakout ↑"
        elif last_close < min(support1, chan_support) - tol:
            brk = "Пробой ↓" if lang.startswith("ru") else "Breakout ↓"
        else:
            if abs(last_close - resistance1) <= tol:
                brk = "Ретест R1" if lang.startswith("ru") else "Retest R1"
            elif abs(last_close - support1) <= tol:
                brk = "Ретест S1" if lang.startswith("ru") else "Retest S1"
            elif abs(last_close - chan_resist) <= tol:
                brk = "Ретест канала" if lang.startswith("ru") else "Channel retest"
            elif abs(last_close - chan_support) <= tol:
                brk = "Ретест канала" if lang.startswith("ru") else "Channel retest"
    except Exception:
        pass

    # ---------- liquidity zones (equal highs/lows) ----------
    liq = "—"
    try:
        atr_abs_s = float(df_struct_w["atr"].iloc[-1]) if "atr" in df_struct_w.columns and df_struct_w["atr"].iloc[-1] == df_struct_w["atr"].iloc[-1] else atr_abs
        tol2 = max(atr_abs_s * 0.4, price * 0.001)
        if len(sh) >= 2 and abs(sh[-1][1] - sh[-2][1]) <= tol2:
            lvl = (sh[-1][1] + sh[-2][1]) / 2.0
            liq = ("Равные хаи" if lang.startswith("ru") else "Equal Highs") + f" ~{_fmt_int_space(lvl)}"
        elif len(sl) >= 2 and abs(sl[-1][1] - sl[-2][1]) <= tol2:
            lvl = (sl[-1][1] + sl[-2][1]) / 2.0
            liq = ("Равные лои" if lang.startswith("ru") else "Equal Lows") + f" ~{_fmt_int_space(lvl)}"
    except Exception:
        pass



    # ---------- SMC-lite: BOS / CHOCH (based on swing breaks) ----------
    smc_event = "—"
    try:
        last_close = float(df5["close"].astype(float).iloc[-1])
        tol_smc = max(atr_abs * 0.35, last_close * 0.0008)  # ~0.08% or 0.35 ATR
        # latest swing levels from structure TF
        last_swing_hi = float(swing_hi)
        last_swing_lo = float(swing_lo)

        # BOS: break in trend direction
        if last_close > last_swing_hi + tol_smc:
            smc_event = "BOS ↑" if not lang.startswith("ru") else "BOS вверх"
            # If structure was bearish -> this is CHOCH
            if struct_lbl == "LH/LL":
                smc_event = "CHOCH ↑" if not lang.startswith("ru") else "CHOCH вверх"
        elif last_close < last_swing_lo - tol_smc:
            smc_event = "BOS ↓" if not lang.startswith("ru") else "BOS вниз"
            if struct_lbl == "HH/HL":
                smc_event = "CHOCH ↓" if not lang.startswith("ru") else "CHOCH вниз"
    except Exception:
        pass

    # ---------- FVG (Fair Value Gap) ----------
    fvg_txt = "—"
    try:
        _df = df_struct_w.tail(220).reset_index(drop=True)
        if len(_df) >= 20:
            h = _df["high"].astype(float).values
            l = _df["low"].astype(float).values

            last_zone = None  # (kind, z_lo, z_hi, idx)
            for i in range(2, len(_df)):
                # bullish FVG: current low > high 2 bars back
                if l[i] > h[i-2]:
                    z_lo, z_hi = float(h[i-2]), float(l[i])
                    last_zone = ("BULL", z_lo, z_hi, i)
                # bearish FVG: current high < low 2 bars back
                elif h[i] < l[i-2]:
                    z_lo, z_hi = float(h[i]), float(l[i-2])
                    last_zone = ("BEAR", z_lo, z_hi, i)

            if last_zone is not None:
                kind, z_lo, z_hi, idx = last_zone
                # active/unfilled heuristic
                if kind == "BULL":
                    filled = (l[idx+1:] <= z_lo).any() if idx+1 < len(_df) else False
                else:
                    filled = (h[idx+1:] >= z_hi).any() if idx+1 < len(_df) else False

                side = ("Bullish" if kind=="BULL" else "Bearish") if not lang.startswith("ru") else ("Бычья" if kind=="BULL" else "Медвежья")
                st = ("active" if not filled else "filled") if not lang.startswith("ru") else ("активна" if not filled else "заполнена")
                fvg_txt = f"{side} FVG [{_fmt_int_space(z_lo)}–{_fmt_int_space(z_hi)}] ({st})"
    except Exception:
        pass

    # ---------- momentum extras (divergence + slope) ----------
    div = "—"
    try:
        if "rsi" in df5.columns and len(df5) >= 80:
            p = df5["close"].astype(float).tail(60).values
            r = df5["rsi"].astype(float).tail(60).values
            # compare last swing-like points (simple)
            p1, p2 = float(p[-40]), float(p[-1])
            r1, r2 = float(r[-40]), float(r[-1])
            if p2 > p1 and r2 < r1:
                div = "Bearish divergence" if not lang.startswith("ru") else "Медвежья дивергенция"
            elif p2 < p1 and r2 > r1:
                div = "Bullish divergence" if not lang.startswith("ru") else "Бычья дивергенция"
    except Exception:
        pass

    slope_ema = "—"
    try:
        if "ema20" in df5.columns and len(df5) >= 30:
            e = df5["ema20"].astype(float).tail(20).values
            slope = float(e[-1] - e[0])
            if abs(slope) < (atr_abs * 0.2):
                slope_ema = "flat"
            elif slope > 0:
                slope_ema = "up"
            else:
                slope_ema = "down"
    except Exception:
        pass

    # ---------- patterns (lightweight heuristics) ----------
    pattern = "—"
    try:
        if len(sh) >= 3 and len(sl) >= 3:
            # Double top/bottom
            tol_p = max(atr_abs * 0.6, price * 0.001)
            if abs(sh[-1][1] - sh[-2][1]) <= tol_p and sh[-1][1] > sl[-1][1]:
                pattern = "Double Top" if not lang.startswith("ru") else "Двойная вершина"
            elif abs(sl[-1][1] - sl[-2][1]) <= tol_p and sl[-1][1] < sh[-1][1]:
                pattern = "Double Bottom" if not lang.startswith("ru") else "Двойное дно"
            else:
                # Triangle: lower highs + higher lows over last swings
                if sh[-1][1] < sh[-2][1] and sl[-1][1] > sl[-2][1]:
                    pattern = "Triangle" if not lang.startswith("ru") else "Треугольник"
    except Exception:
        pass

    # ---------- smart money zones (simplified order block) ----------
    smz = "—"
    try:
        df_ob = df1 if (df1 is not None and not getattr(df1, "empty", True) and len(df1) >= 80) else df5
        w = df_ob.tail(120).reset_index(drop=True)
        c = w["close"].astype(float).values
        o = w["open"].astype(float).values
        h = w["high"].astype(float).values
        l = w["low"].astype(float).values
        # find recent impulse: candle range vs ATR
        atr_ob = float(w["atr"].iloc[-1]) if "atr" in w.columns else 0.0
        thr = max(atr_ob * 1.2, price * 0.002)
        idx = None
        dir_up = None
        for i in range(len(w)-3, 10, -1):
            if abs(c[i] - o[i]) >= thr:
                idx = i
                dir_up = (c[i] > o[i])
                break
        if idx is not None:
            # order block: last opposite candle before impulse
            ob_idx = None
            if dir_up:
                for j in range(idx-1, max(idx-12, 0), -1):
                    if c[j] < o[j]:  # bearish candle
                        ob_idx = j
                        break
            else:
                for j in range(idx-1, max(idx-12, 0), -1):
                    if c[j] > o[j]:  # bullish candle
                        ob_idx = j
                        break
            if ob_idx is not None:
                z_hi = max(o[ob_idx], c[ob_idx])
                z_lo = min(o[ob_idx], c[ob_idx])
                smz = ("OB " + ("Demand" if dir_up else "Supply")) if not lang.startswith("ru") else ("OB " + ("Спрос" if dir_up else "Предложение"))
                smz += f" [{_fmt_int_space(z_lo)}–{_fmt_int_space(z_hi)}]"
    except Exception:
        pass

    # ---------- market regime ----------
    regime = "—"
    try:
        adx_s = float(df_struct_w["adx"].iloc[-1]) if "adx" in df_struct_w.columns else adx5
        atrp_s = float(df_struct_w["atr_pct"].iloc[-1]) if "atr_pct" in df_struct_w.columns else atr_pct
        # expansion/contraction: compare last atr% to avg
        atrp_avg = float(df_struct_w["atr_pct"].astype(float).tail(80).mean()) if "atr_pct" in df_struct_w.columns and len(df_struct_w) >= 80 else atrp_s
        expansion = (atrp_s > atrp_avg * 1.25) if atrp_avg > 0 else False
        if expansion:
            regime = "EXPANSION"
        elif adx_s >= 25 and struct_lbl in ("HH/HL", "LH/LL"):
            regime = "TRENDING"
        elif adx_s < 18 and struct_lbl == "RANGE":
            regime = "RANGING"
        else:
            regime = "CHOPPY"
    except Exception:
        regime = "—"



    # ---------- volatility analysis (ATR% expansion/contraction) ----------
    vola_state = "—"
    vola_risk = "—"
    try:
        atrp_series = df_struct_w["atr_pct"].astype(float) if (df_struct_w is not None and "atr_pct" in df_struct_w.columns) else None
        if atrp_series is not None and len(atrp_series) >= 60:
            cur = float(atrp_series.iloc[-1])
            avg40 = float(atrp_series.tail(40).mean())
            avg10 = float(atrp_series.tail(10).mean())
            if avg40 > 0:
                if avg10 > avg40 * 1.15:
                    vola_state = "volatility rising" if not lang.startswith("ru") else "волатильность растёт"
                    vola_risk = "unstable / breakout risk" if not lang.startswith("ru") else "рынок нестабильный / риск breakout"
                elif avg10 < avg40 * 0.88:
                    vola_state = "volatility falling" if not lang.startswith("ru") else "волатильность падает"
                    vola_risk = "compression / wait" if not lang.startswith("ru") else "сжатие / ждать импульс"
                else:
                    vola_state = "volatility stable" if not lang.startswith("ru") else "волатильность стабильна"
                    vola_risk = "normal" if not lang.startswith("ru") else "нормально"
    except Exception:
        pass

    # ---------- volume analysis (accumulation / distribution) ----------
    vol_phase = "neutral" if not lang.startswith("ru") else "нейтрально"
    try:
        if "volume" in df5.columns and len(df5) >= 60:
            # price change vs volume trend
            delta = float(df5["close"].astype(float).iloc[-1] - df5["close"].astype(float).iloc[-20])
            if vol_trend == "↑" and vol_rel >= 1.2:
                if delta > 0:
                    vol_phase = "accumulation" if not lang.startswith("ru") else "аккумуляция"
                elif delta < 0:
                    vol_phase = "distribution" if not lang.startswith("ru") else "дистрибуция"
    except Exception:
        pass

    # ---------- probability engine (enhanced) ----------
    long_score = 0.0
    short_score = 0.0

    # EMA trend
    try:
        if float(df5["ema50"].iloc[-1]) > float(df5["ema200"].iloc[-1]):
            long_score += 3
        else:
            short_score += 3
    except Exception:
        pass

    # RSI
    if rsi > 55:
        long_score += 2
    elif rsi < 45:
        short_score += 2

    # MACD
    if macd > 0:
        long_score += 1
    else:
        short_score += 1

    # ADX -> confidence, not direction (adds to both)
    if adx5 >= 25:
        long_score += 1
        short_score += 1

    # Structure bias
    if struct_lbl == "HH/HL":
        long_score += 1
    elif struct_lbl == "LH/LL":
        short_score += 1

    # Breakout bias
    if "↑" in brk:
        long_score += 1
    elif "↓" in brk:
        short_score += 1

    total = long_score + short_score
    long_p = int(round(100.0 * long_score / total)) if total > 0 else 50
    long_p = max(0, min(100, long_p))
    short_p = 100 - long_p

    bias = "LONG" if long_p > short_p else "SHORT"
    diff = abs(long_p - short_p)
    if lang.startswith("ru"):
        confidence = "ВЫСОКАЯ" if diff >= 30 else ("СРЕДНЯЯ" if diff >= 15 else "НИЗКАЯ")
    else:
        confidence = "HIGH" if diff >= 30 else ("MEDIUM" if diff >= 15 else "LOW")

    # ---------- trading plan (ATR + structure aware) ----------
    entry = price
    sl_pad = max(atr_abs * 0.6, entry * 0.001)  # 0.1% or 0.6 ATR

    if bias == "LONG":
        base_sl = min(support1, float(swing_lo), float(chan_support))
        sl = base_sl - sl_pad

        base_tp1 = min(resistance1, float(chan_resist))
        base_tp2 = max(resistance2, float(chan_resist))
        tp1 = base_tp1
        tp2 = base_tp2

        rr = (tp2 - entry) / max(1e-9, (entry - sl))
    else:
        base_sl = max(resistance1, float(swing_hi), float(chan_resist))
        sl = base_sl + sl_pad

        base_tp1 = max(support1, float(chan_support))
        base_tp2 = min(support2, float(chan_support))
        tp1 = base_tp1
        tp2 = base_tp2

        rr = (entry - tp2) / max(1e-9, (sl - entry))

    # ---------- formatting ----------
    line = "━━━━━━━━━━━━━━━━━━━"
    price_s = _fmt_int_space(price)

    s1_s = _fmt_int_space(support1)
    s2_s = _fmt_int_space(support2)
    r1_s = _fmt_int_space(resistance1)
    r2_s = _fmt_int_space(resistance2)
    piv_s = _fmt_int_space(pivot)

    P_s = _fmt_int_space(P)
    R1_s = _fmt_int_space(R1)
    R2_s = _fmt_int_space(R2)
    S1_s = _fmt_int_space(S1)
    S2_s = _fmt_int_space(S2)

    entry_s = _fmt_int_space(entry)
    sl_s = _fmt_int_space(sl)
    tp1_s = _fmt_int_space(tp1)
    tp2_s = _fmt_int_space(tp2)

    chan_type_txt = (chan_type or (_tr_i18n(lang, "analysis_channel_type") if "analysis_channel_type" in I18N.get(lang, {}) else ("Channel" if not lang.startswith("ru") else "Канал")))
    chan_s_s = _fmt_int_space(chan_support)
    chan_m_s = _fmt_int_space(chan_mid)
    chan_r_s = _fmt_int_space(chan_resist)

    tl_s_txt = _fmt_int_space(tl_support) if (tl_support is not None and tl_support == tl_support) else "—"
    tl_r_txt = _fmt_int_space(tl_resist) if (tl_resist is not None and tl_resist == tl_resist) else "—"

    rsi_i = int(round(rsi)) if rsi == rsi else 0
    adx_i = int(round(adx5)) if adx5 == adx5 else 0
    atr_s = _fmt_float(atr_pct, 1)
    vol_s = _fmt_float(vol_rel, 2)

    macd_label = "🟢 bullish" if macd > 0 else "🔴 bearish"
    vol_status = _tr_i18n(lang, "analysis_volume_status_acc") if float(vol_rel) >= 1.2 else _tr_i18n(lang, "analysis_volume_status_neu")

    # ---------- report (RU template) ----------
    if lang.startswith("ru"):
        # Better labels for market
        mkt_ru = "ФЬЮЧЕРСЫ" if mkt == "FUTURES" else "СПОТ"

        # Regime strength (use structure TF ADX when possible)
        try:
            adx_reg = float(df_struct_w["adx"].iloc[-1]) if "adx" in df_struct_w.columns else adx5
        except Exception:
            adx_reg = adx5
        adx_reg_i = int(round(float(adx_reg))) if adx_reg == adx_reg else adx_i
        trend_strength = "Высокая" if adx_reg_i >= 30 else ("Средняя" if adx_reg_i >= 22 else "Низкая")

        # Volatility text mapping
        vola_txt = "Стабильна"
        if isinstance(vola_state, str):
            vs = vola_state.lower()
            if "раст" in vs or "rising" in vs:
                vola_txt = "Растёт"
            elif "пад" in vs or "fall" in vs:
                vola_txt = "Падает"
        risk_txt = "Нормальный"
        if isinstance(vola_risk, str):
            vr = vola_risk.lower()
            if "breakout" in vr or "проб" in vr or "нестабил" in vr:
                risk_txt = "Высокий"
            elif "сжат" in vr or "compression" in vr:
                risk_txt = "Низкий"
        compression_txt = "Да" if (isinstance(vola_risk, str) and ("сжат" in vola_risk.lower() or "compression" in vola_risk.lower())) else "Нет"

        # Momentum text mapping
        rsi_mood = "бычий импульс" if rsi >= 55 else ("медвежий импульс" if rsi <= 45 else "нейтрально")
        macd_mood = "Бычий импульс ↑" if macd > 0 else "Медвежий импульс ↓"
        slope_txt = "Вверх ↗" if slope_ema == "up" else ("Вниз ↘" if slope_ema == "down" else "Флет →")
        div_txt = div if div not in ("—", "-", None) else "Не обнаружена"

        # Volume text mapping
        vol_rel_txt = f"{_fmt_float(vol_rel,2)}x" if vol_rel == vol_rel else "—"
        vol_tr_txt = "Растёт ↑" if vol_trend == "up" else ("Падает ↓" if vol_trend == "down" else "Флет →")
        spike_txt = "Да" if (isinstance(vol_spike, str) and ("spike" in vol_spike.lower() or "всплеск" in vol_spike.lower() or "⚡" in vol_spike)) else "Нет"

        # Breakout/retest status for the header line
        status_txt = ("Нет" if str(lang).startswith("ru") else "None")
        try:
            b = (brk or "").lower()
            if "ретест" in b or "retest" in b:
                if "s1" in b or "support" in b or "поддерж" in b:
                    status_txt = "Retest поддержки (бычий сигнал)"
                elif "r1" in b or "resist" in b or "сопрот" in b:
                    status_txt = "Retest сопротивления (медвежий сигнал)"
                else:
                    status_txt = "Retest уровня"
            elif "пробой" in b or "breakout" in b:
                status_txt = "Пробой (импульс)"
        except Exception:
            pass

        # Liquidity zones: try to output both Equal Highs and Equal Lows

        # Liquidity zones: detect Equal Highs/Lows as clusters of swing points (not only last 2)
        eq_hi = None
        eq_lo = None
        eq_hi_touches = 0
        eq_lo_touches = 0
        eq_hi_strength = 0
        eq_lo_strength = 0
        eq_hi_dist_pct = None
        eq_lo_dist_pct = None
        eq_hi_dist_atr = None
        eq_lo_dist_atr = None
        try:
            atr_abs_s = float(df_struct_w["atr"].iloc[-1]) if "atr" in df_struct_w.columns and df_struct_w["atr"].iloc[-1] == df_struct_w["atr"].iloc[-1] else atr_abs
            tol_atr = float(os.getenv("LIQ_EQ_TOL_ATR_MULT", "0.35") or 0.35)
            tol_pct = float(os.getenv("LIQ_EQ_TOL_PCT", "0.0010") or 0.0010)  # 0.10%
            tol2 = max(atr_abs_s * tol_atr, price * tol_pct)

            max_swings = int(os.getenv("LIQ_EQ_MAX_SWINGS", "12") or 12)
            min_sep_pct = float(os.getenv("LIQ_EQ_MIN_SEP_PCT", "0.0002") or 0.0002)  # avoid classifying "near price" as liquidity

            def _median(vals: list[float]) -> float:
                s = sorted(vals)
                return float(s[len(s)//2])

            def _best_eq(swings: list[tuple[int, float]] | None, side: str):
                # side: "high" (liquidity above) or "low" (liquidity below)
                if not swings:
                    return None
                pts = swings[-max_swings:]
                clusters: list[dict] = []
                for ix, v in pts:
                    v = float(v)
                    placed = False
                    for c in clusters:
                        if abs(v - float(c["level"])) <= tol2:
                            c["vals"].append(v)
                            c["idxs"].append(int(ix))
                            c["level"] = _median(c["vals"])
                            placed = True
                            break
                    if not placed:
                        clusters.append({"vals": [v], "idxs": [int(ix)], "level": v})

                # filter by side relative to current price (we care about resting liquidity away from price)
                if side == "high":
                    cand = [c for c in clusters if float(c["level"]) >= price * (1.0 + min_sep_pct)]
                else:
                    cand = [c for c in clusters if float(c["level"]) <= price * (1.0 - min_sep_pct)]

                if not cand:
                    return None

                # best = most touches, then most recent touch
                cand.sort(key=lambda c: (len(c["vals"]), max(c["idxs"])), reverse=True)
                c = cand[0]
                level = float(c["level"])
                touches = int(len(c["vals"]))
                last_touch = int(max(c["idxs"]))
                spread = float(max(c["vals"]) - min(c["vals"])) if touches >= 2 else 0.0

                # strength (1..5): touches + recency - dispersion
                strength = min(5, max(1, touches))
                try:
                    age = (len(df_struct_w) - 1) - last_touch
                    if age <= int(os.getenv("LIQ_EQ_RECENT_BONUS_BARS", "60") or 60):
                        strength = min(5, strength + 1)
                except Exception:
                    pass
                if spread > tol2 * float(os.getenv("LIQ_EQ_SPREAD_PENALTY", "0.8") or 0.8):
                    strength = max(1, strength - 1)

                dist = abs(level - price)
                dist_pct = (dist / price * 100.0) if price > 0 else None
                dist_atr = (dist / atr_abs_s) if atr_abs_s and atr_abs_s > 0 else None
                return (level, touches, strength, dist_pct, dist_atr)

            hi = _best_eq(sh, "high")
            if hi:
                eq_hi, eq_hi_touches, eq_hi_strength, eq_hi_dist_pct, eq_hi_dist_atr = hi

            lo = _best_eq(sl, "low")
            if lo:
                eq_lo, eq_lo_touches, eq_lo_strength, eq_lo_dist_pct, eq_lo_dist_atr = lo

        except Exception:
            pass

        def _liq_line(prefix: str, level: float | None, dist_pct: float | None, dist_atr: float | None, strength: int, touches: int, side: str) -> str:
            if level is None:
                return f"{prefix}: Не обнаружены"
            try:
                dp = f"{dist_pct:.2f}%" if dist_pct is not None and dist_pct == dist_pct else "n/a"
            except Exception:
                dp = "n/a"
            try:
                da = f"{dist_atr:.2f} ATR" if dist_atr is not None and dist_atr == dist_atr else "n/a"
            except Exception:
                da = "n/a"
            arrow = "↑" if side == "high" else "↓"
            return f"{prefix}: {_fmt_int_space(level)} {arrow} | dist: {dp} ({da}) | strength: {int(strength)}/5 | touches: {int(touches)}"

        eq_hi_line = _liq_line("Equal Highs (ликвидность сверху)", eq_hi, eq_hi_dist_pct, eq_hi_dist_atr, eq_hi_strength, eq_hi_touches, "high")
        eq_lo_line = _liq_line("Equal Lows (ликвидность снизу)", eq_lo, eq_lo_dist_pct, eq_lo_dist_atr, eq_lo_strength, eq_lo_touches, "low")
        # Order block parsing: "OB Спрос [lo–hi]" -> lo/hi
        ob_side = "—"
        ob_lo = None
        ob_hi = None
        try:
            if isinstance(smz, str) and "[" in smz and "]" in smz:
                ob_side = "спроса" if ("спрос" in smz.lower() or "demand" in smz.lower()) else "предложения"
                inside = smz.split("[", 1)[1].split("]", 1)[0]
                inside = inside.replace("–", "-").replace("—", "-")
                a, b = inside.split("-", 1)
                ob_lo = float(str(a).replace(" ", ""))
                ob_hi = float(str(b).replace(" ", ""))
        except Exception:
            pass

        # FVG parsing: "Бычья FVG [lo–hi] (активна)" -> lo/hi
        fvg_side = None
        fvg_lo = None
        fvg_hi = None
        fvg_active = None
        try:
            if isinstance(fvg_txt, str) and "FVG" in fvg_txt and "[" in fvg_txt and "]" in fvg_txt:
                fvg_side = "Бычий" if "Быч" in fvg_txt else ("Медвежий" if "Медв" in fvg_txt else None)
                inside = fvg_txt.split("[", 1)[1].split("]", 1)[0]
                inside = inside.replace("–", "-").replace("—", "-")
                a, b = inside.split("-", 1)
                fvg_lo = float(str(a).replace(" ", ""))
                fvg_hi = float(str(b).replace(" ", ""))
                fvg_active = ("актив" in fvg_txt.lower() or "active" in fvg_txt.lower())
        except Exception:
            pass

        # Trading plan extras
        # Entry Zone (идеально для институционального входа) строим НЕ от ATR вокруг цены,
        # а от зон спроса/предложения: Order Block + FVG + liquidity sweep + channel/support.
        # После зоны обязательно показываем Trigger (подтверждение) и Invalidation (отмена).
        try:
            entry_lo = None
            entry_hi = None
            entry_kind = "—"
            entry_confluence = 0
            entry_notes = []

            cs = float(chan_support) if chan_support is not None else None
            cm = float(chan_mid) if chan_mid is not None else None
            cr = float(chan_resist) if chan_resist is not None else None
            s1n = float(support1) if support1 is not None else None
            s2n = float(support2) if support2 is not None else None
            r1n = float(resistance1) if resistance1 is not None else None
            r2n = float(resistance2) if resistance2 is not None else None

            # базовые допуски "рядом" (для confluence)
            tol = max(atr_abs * 0.6, price * 0.002)

            # fallback ширина зоны, чтобы отчёт никогда не ломался
            w_fallback = max(atr_abs * 0.25, price * 0.001)

            # liquidity sweep detection (последние ~30 свечей 5м)
            sweep_long = False
            sweep_short = False
            try:
                recent = df5.tail(30)
                if eq_lo is not None and "low" in recent.columns and "close" in recent.columns:
                    low_min = float(recent["low"].astype(float).min())
                    close_last = float(recent["close"].astype(float).iloc[-1])
                    sweep_long = (low_min < float(eq_lo) - tol * 0.15) and (close_last > float(eq_lo) + tol * 0.05)
                if eq_hi is not None and "high" in recent.columns and "close" in recent.columns:
                    high_max = float(recent["high"].astype(float).max())
                    close_last = float(recent["close"].astype(float).iloc[-1])
                    sweep_short = (high_max > float(eq_hi) + tol * 0.15) and (close_last < float(eq_hi) - tol * 0.05)
            except Exception:
                pass


            def _overlap(a_lo, a_hi, b_lo, b_hi):
                lo = max(a_lo, b_lo)
                hi = min(a_hi, b_hi)
                return (lo, hi) if hi > lo else (None, None)

            def _near(x, lo, hi, tol_):
                if x is None:
                    return False
                return (lo - tol_) <= x <= (hi + tol_)

            def _clamp_zone(zlo, zhi):
                zlo = max(0.0, float(zlo))
                zhi = max(zlo, float(zhi))
                return zlo, zhi

            # --- LONG: Demand zones ---
            if bias == "LONG":
                ob_ok = (ob_lo is not None and ob_hi is not None and ("спрос" in str(ob_side).lower()))
                fvg_ok = (fvg_lo is not None and fvg_hi is not None and fvg_active is True and (str(fvg_side).lower().startswith("быч")))
                ch_ok = (cs is not None and cm is not None and cm > cs)

                # 1) лучший вариант: OB + FVG overlap (узкая зона)
                if ob_ok and fvg_ok:
                    o_lo, o_hi = _overlap(float(ob_lo), float(ob_hi), float(fvg_lo), float(fvg_hi))
                    if o_lo is not None:
                        entry_lo, entry_hi = _clamp_zone(o_lo, o_hi)
                        entry_kind = "OB+FVG"
                        entry_notes.append("Order Block + FVG")
                    else:
                        # если не пересекаются — берём OB как базу (он сильнее)
                        entry_lo, entry_hi = _clamp_zone(float(ob_lo), float(ob_hi))
                        entry_kind = "OB"
                        entry_notes.append("Order Block")

                elif ob_ok:
                    entry_lo, entry_hi = _clamp_zone(float(ob_lo), float(ob_hi))
                    entry_kind = "OB"
                    entry_notes.append("Order Block")

                elif fvg_ok:
                    entry_lo, entry_hi = _clamp_zone(float(fvg_lo), float(fvg_hi))
                    entry_kind = "FVG"
                    entry_notes.append("FVG")

                elif ch_ok:
                    # зона от поддержки канала к середине (консервативно)
                    base = cs
                    # если Support1 выше поддержки канала — используем его (лучше)
                    if s1n is not None:
                        base = max(base, s1n)
                    w = max(w_fallback, (cm - base) * 0.35)
                    entry_lo, entry_hi = _clamp_zone(base, base + w)
                    entry_kind = "CHANNEL"
                    entry_notes.append("Support/Channel")

                # fallback: ATR band (если ничего нет)
                if entry_lo is None or entry_hi is None or not (entry_hi > entry_lo):
                    entry_lo, entry_hi = _clamp_zone(price - atr_abs * 0.8, price - atr_abs * 0.1)
                    entry_kind = "ATR"
                    entry_notes.append("Fallback ATR")

                # --- confluence scoring (0..5) ---
                # OB присутствует (сильнейший фактор)
                if ob_ok and _near((float(ob_lo)+float(ob_hi))/2.0, entry_lo, entry_hi, tol):
                    entry_confluence += 2
                # FVG рядом/внутри
                if fvg_ok and _near((float(fvg_lo)+float(fvg_hi))/2.0, entry_lo, entry_hi, tol):
                    entry_confluence += 1
                # Support канала рядом с низом зоны
                if cs is not None and abs(cs - entry_lo) <= tol:
                    entry_confluence += 1
                # Liquidity (Equal Lows) рядом
                if _near(eq_lo, entry_lo, entry_hi, tol):
                    entry_confluence += 1
                # Pivot/S1 внутри/рядом
                try:
                    if _near(float(S1), entry_lo, entry_hi, tol) or _near(float(pivot), entry_lo, entry_hi, tol):
                        entry_confluence += 1
                except Exception:
                    pass
                entry_confluence = min(5, max(0, int(entry_confluence)))

                # --- Trigger & Invalidation for LONG ---
                # Обязательное подтверждение: Liquidity sweep снизу (Equal Lows) + возврат выше уровня
                entry_sweep_txt = "Liquidity sweep (5м): " + ("Да" if sweep_long else "Нет")
                trig_parts = []
                trig_parts.append("Liquidity sweep снизу (Equal Lows) + reclaim")
                trig_parts.append("BOS 5м вверх (закрытие выше локального swing-high)")
                trig_parts.append(f"reclaim зоны (закрытие 5м выше {_fmt_int_space(entry_hi)})")
                trig_parts.append("объём > 1.2x")
                if not sweep_long:
                    entry_trigger_txt = "Триггер входа: СНАЧАЛА " + trig_parts[0] + " (сейчас: Нет), затем " + " + ".join(trig_parts[1:])
                else:
                    entry_trigger_txt = "Триггер входа: " + " + ".join(trig_parts)

                inv_lvl = max(0.0, entry_lo - max(atr_abs * 0.15, price * 0.0008))
                entry_inval_txt = f"Инвалидация: закрепление 5м ниже {_fmt_int_space(inv_lvl)}"

                # Подгоняем SL под инвалидацию (SL должен быть ниже инвалидации)
                try:
                    sl = min(float(sl), inv_lvl)
                except Exception:
                    pass

            # --- SHORT: Supply zones ---
            else:
                ob_ok = (ob_lo is not None and ob_hi is not None and ("предлож" in str(ob_side).lower()))
                fvg_ok = (fvg_lo is not None and fvg_hi is not None and fvg_active is True and (str(fvg_side).lower().startswith("медв")))
                ch_ok = (cr is not None and cm is not None and cr > cm)

                if ob_ok and fvg_ok:
                    o_lo, o_hi = _overlap(float(ob_lo), float(ob_hi), float(fvg_lo), float(fvg_hi))
                    if o_lo is not None:
                        entry_lo, entry_hi = _clamp_zone(o_lo, o_hi)
                        entry_kind = "OB+FVG"
                        entry_notes.append("Order Block + FVG")
                    else:
                        entry_lo, entry_hi = _clamp_zone(float(ob_lo), float(ob_hi))
                        entry_kind = "OB"
                        entry_notes.append("Order Block")

                elif ob_ok:
                    entry_lo, entry_hi = _clamp_zone(float(ob_lo), float(ob_hi))
                    entry_kind = "OB"
                    entry_notes.append("Order Block")

                elif fvg_ok:
                    entry_lo, entry_hi = _clamp_zone(float(fvg_lo), float(fvg_hi))
                    entry_kind = "FVG"
                    entry_notes.append("FVG")

                elif ch_ok:
                    top = cr
                    if r1n is not None:
                        top = min(top, r1n)
                    w = max(w_fallback, (top - cm) * 0.35)
                    entry_lo, entry_hi = _clamp_zone(top - w, top)
                    entry_kind = "CHANNEL"
                    entry_notes.append("Resistance/Channel")

                if entry_lo is None or entry_hi is None or not (entry_hi > entry_lo):
                    entry_lo, entry_hi = _clamp_zone(price + atr_abs * 0.1, price + atr_abs * 0.8)
                    entry_kind = "ATR"
                    entry_notes.append("Fallback ATR")

                # confluence scoring
                if ob_ok and _near((float(ob_lo)+float(ob_hi))/2.0, entry_lo, entry_hi, tol):
                    entry_confluence += 2
                if fvg_ok and _near((float(fvg_lo)+float(fvg_hi))/2.0, entry_lo, entry_hi, tol):
                    entry_confluence += 1
                if cr is not None and abs(cr - entry_hi) <= tol:
                    entry_confluence += 1
                if _near(eq_hi, entry_lo, entry_hi, tol):
                    entry_confluence += 1
                try:
                    if _near(float(R1), entry_lo, entry_hi, tol) or _near(float(pivot), entry_lo, entry_hi, tol):
                        entry_confluence += 1
                except Exception:
                    pass
                entry_confluence = min(5, max(0, int(entry_confluence)))

                entry_sweep_txt = "Liquidity sweep (5м): " + ("Да" if sweep_short else "Нет")
                trig_parts = []
                trig_parts.append("Liquidity sweep сверху (Equal Highs) + reject")
                trig_parts.append("BOS 5м вниз (закрытие ниже локального swing-low)")
                trig_parts.append(f"reject зоны (закрытие 5м ниже {_fmt_int_space(entry_lo)})")
                trig_parts.append("объём > 1.2x")
                if not sweep_short:
                    entry_trigger_txt = "Триггер входа: СНАЧАЛА " + trig_parts[0] + " (сейчас: Нет), затем " + " + ".join(trig_parts[1:])
                else:
                    entry_trigger_txt = "Триггер входа: " + " + ".join(trig_parts)

                inv_lvl = entry_hi + max(atr_abs * 0.15, price * 0.0008)
                entry_inval_txt = f"Инвалидация: закрепление 5м выше {_fmt_int_space(inv_lvl)}"

                try:
                    sl = max(float(sl), inv_lvl)
                except Exception:
                    pass

        except Exception:
            entry_lo, entry_hi = price, price
            entry_kind = "—"
            entry_confluence = 0
            entry_notes = []
            entry_sweep_txt = "Liquidity sweep (5м): —"
            entry_trigger_txt = "Триггер входа: —"
            entry_inval_txt = "Инвалидация: —"


        entry_zone_txt = f"{_fmt_int_space(entry_lo)} – {_fmt_int_space(entry_hi)}"
        entry_quality_txt = f"Качество зоны (confluence): {entry_confluence}/5"
        entry_kind_txt = f"Тип зоны: {entry_kind}" if entry_kind and entry_kind != "—" else "Тип зоны: —"
        entry_notes_txt = ("Основание: " + ", ".join(entry_notes)) if entry_notes else "Основание: —"
        # ---------- ENTRY STATUS (WAIT / READY / CONFIRMED / INVALID) ----------
        # Meaning:
        #   WAIT      -> price is not in entry zone yet (monitor only)
        #   READY     -> price is inside entry zone (watch for confirmation)
        #   CONFIRMED -> sweep + BOS + reclaim + volume confirmation present
        #   INVALID   -> invalidation level breached (setup is broken)
        entry_status = "WAIT"
        try:
            p_now = float(price)
            lo = float(entry_lo)
            hi = float(entry_hi)
            inv = float(inv_lvl) if inv_lvl is not None else None

            last5_close = None
            try:
                if df5 is not None and (not getattr(df5, "empty", True)) and "close" in df5.columns:
                    last5_close = float(df5["close"].astype(float).iloc[-1])
            except Exception:
                last5_close = None

            # Setup invalid?
            if inv is not None:
                if str(bias).upper() == "LONG" and p_now <= inv:
                    entry_status = "INVALID"
                elif str(bias).upper() == "SHORT" and p_now >= inv:
                    entry_status = "INVALID"

            if entry_status != "INVALID":
                in_zone = (p_now >= min(lo, hi) and p_now <= max(lo, hi))
                if in_zone:
                    entry_status = "READY"
                else:
                    entry_status = "WAIT"

                # Confirmation check (only meaningful when not invalid)
                bos_up = False
                bos_dn = False
                try:
                    se = (smc_event or "")
                    se_l = str(se).lower()
                    if "bos" in se_l:
                        bos_up = ("↑" in str(se)) or ("вверх" in se_l) or ("up" in se_l)
                        bos_dn = ("↓" in str(se)) or ("вниз" in se_l) or ("down" in se_l)
                except Exception:
                    pass

                reclaim_ok = False
                try:
                    if last5_close is not None:
                        if str(bias).upper() == "LONG":
                            reclaim_ok = last5_close >= hi
                        else:
                            reclaim_ok = last5_close <= lo
                except Exception:
                    pass

                vol_ok = False
                try:
                    vol_ok = float(vol_rel) >= 1.2
                except Exception:
                    vol_ok = False

                sweep_ok = False
                try:
                    if str(bias).upper() == "LONG":
                        sweep_ok = bool(sweep_long)
                    else:
                        sweep_ok = bool(sweep_short)
                except Exception:
                    sweep_ok = False

                bos_ok = bos_up if str(bias).upper() == "LONG" else bos_dn

                if sweep_ok and bos_ok and reclaim_ok and vol_ok:
                    entry_status = "CONFIRMED"
        except Exception:
            entry_status = "WAIT"

        def _entry_status_word(lang: str, st: str) -> str:
            st = (st or "WAIT").upper()
            # Prefer i18n keys (so RU/EN are consistent)
            key = {
                "WAIT": "entry_status_wait",
                "READY": "entry_status_ready",
                "CONFIRMED": "entry_status_confirmed",
                "INVALID": "entry_status_invalid",
            }.get(st, "entry_status_wait")
            try:
                return _tr_i18n(lang, key)
            except Exception:
                if str(lang).startswith("ru"):
                    return {"WAIT":"WAIT (ждать)","READY":"READY (в зоне)","CONFIRMED":"CONFIRMED (вход)","INVALID":"INVALID (сломано)"}.get(st, "WAIT")
                return st

        entry_status_txt = f"{_tr_i18n(lang, 'entry_status_label')}: {_entry_status_word(lang, entry_status)}"
# RR to TP1/TP2
        rr1 = None
        rr2 = None
        try:
            risk = max(1e-9, abs(price - sl))
            rr1 = abs(tp1 - price) / risk
            rr2 = abs(tp2 - price) / risk
        except Exception:
            pass
        rr1_txt = _fmt_float(rr1, 1) if rr1 is not None else "—"
        rr2_txt = _fmt_float(rr2, 1) if rr2 is not None else _fmt_float(rr, 1)

        # Bias text
        bias_txt = "LONG 📈" if bias == "LONG" else "SHORT 📉"
        conf_txt = "Высокая" if confidence in ("HIGH", "Высокая") else ("Средняя" if confidence in ("MED", "Средняя") else "Низкая")

        # Conclusion
        concl = []
        concl.append(f"Рынок находится в фазе {regime} с {('сильным' if trend_strength=='Высокая' else 'умеренным')} трендом.")
        if struct_lbl in ("HH/HL", "LH/LL"):
            concl.append(f"Структура {struct_lbl} подтверждает текущий приоритет: {bias}.")
        if isinstance(smz, str) and smz not in ("—", "-"):
            concl.append("Order Block подтверждает интерес крупного участника.")
        if isinstance(fvg_txt, str) and "FVG" in fvg_txt and ("актив" in fvg_txt.lower() or "active" in fvg_txt.lower()):
            concl.append("Активный FVG усиливает сценарий продолжения.")
        if float(vol_rel) >= 1.2:
            concl.append("Объём выше среднего поддерживает движение.")

        report_lines = [
            f"📊 {sym} ({mkt_ru})",
            f"💰 Цена: {price_s} USDT",
            "",
            line,
            "🧭 Структура рынка (разные таймфреймы)",
            "",
            f"Глобальный тренд (4ч): {trend4}",
            f"Среднесрочный тренд (1ч): {trend1}",
            f"Локальный тренд (5м): {trend5}",
            "",
            f"🧩 Рыночная структура: {struct_lbl} (бычья структура)" if struct_lbl == "HH/HL" else f"🧩 Рыночная структура: {struct_lbl}",
            "",
            line,
            "🧬 Рыночный режим",
            "",
            f"Режим: {regime} 🚀 (фаза расширения)" if regime == "EXPANSION" else f"Режим: {regime}",
            f"Сила тренда: {trend_strength} (ADX: {adx_reg_i})",
            "",
            "🌪️ Анализ волатильности",
            "",
            f"Волатильность: {vola_txt}",
            f"Риск пробоя: {risk_txt}",
            f"Сжатие: {compression_txt}",
            "",
            line,
            "⚡ Анализ импульса",
            "",
            f"RSI: {rsi_i} ({rsi_mood})",
            f"MACD: {macd_mood}",
            f"Наклон EMA20: {slope_txt}",
            f"Дивергенция RSI: {div_txt}",
            "",
            line,
            "📦 Анализ объёма",
            "",
            f"Относительный объём: {vol_rel_txt} (выше среднего)" if float(vol_rel) >= 1.2 else f"Относительный объём: {vol_rel_txt}",
            f"Тренд объёма: {vol_tr_txt}",
            f"Всплеск объёма: {spike_txt}",
            f"Фаза объёма: {vol_phase}",
            "",
            line,
            "🎯 Ключевые уровни",
            "",
            "🔴 Сопротивление:",
            f"• {r1_s}",
            f"• {r2_s}",
            "",
            "🟢 Поддержка:",
            f"• {s1_s}",
            f"• {s2_s}",
            "",
            f"Pivot (центр диапазона): {piv_s}",
            "",
            "Pivot Points (24ч):",
            f"Pivot: {P_s}",
            f"R1: {R1_s}",
            f"R2: {R2_s}",
            f"S1: {S1_s}",
            f"S2: {S2_s}",
            "",
            line,
            "📐 Трендовые линии и канал",
            "",
            f"Тип канала: {chan_type_txt} 📈" if "восход" in str(chan_type_txt).lower() else f"Тип канала: {chan_type_txt}",
            "",
            f"Поддержка канала: {chan_s_s}",
            f"Средняя линия канала: {chan_m_s}",
            f"Сопротивление канала: {chan_r_s}",
            "",
            "Трендовые линии:",
            f"Линия поддержки: {tl_s_txt}",
            f"Линия сопротивления: {tl_r_txt}",
            "",
            line,
            "🚦 Пробой и ликвидность",
            "",
            f"Статус: {status_txt}",
            "",
            "💧 Зоны ликвидности:",
                        eq_hi_line,
                        eq_lo_line,
            entry_sweep_txt,
            "",
            line,
            "🏦 Smart Money анализ (SMC)",
            "",
            (f"Order Block (зона {ob_side}): {_fmt_int_space(ob_lo)} – {_fmt_int_space(ob_hi)}" if (ob_lo is not None and ob_hi is not None) else ("Order Block: —" if smz in ("—", "-") else f"Order Block: {smz}")),
            "",
            "События структуры:",
            ("BOS: Бычий пробой структуры ↑" if (isinstance(smc_event, str) and "bos" in smc_event.lower() and ("вверх" in smc_event.lower() or "↑" in smc_event)) else
             "BOS: Медвежий пробой структуры ↓" if (isinstance(smc_event, str) and "bos" in smc_event.lower()) else "BOS: —"),
            ("CHOCH: Обнаружен" if (isinstance(smc_event, str) and "choch" in smc_event.lower()) else "CHOCH: Не обнаружен"),
            "",
            "FVG (Fair Value Gap):",
            (f"{fvg_side} FVG: {_fmt_int_space(fvg_lo)} – {_fmt_int_space(fvg_hi)} ({'активен' if fvg_active else 'заполнен'})" if (fvg_side and fvg_lo is not None and fvg_hi is not None and fvg_active is not None) else (f"{fvg_txt}" if fvg_txt not in ("—", "-") else "—")),
            "",
            line,
            "📊 Вероятностный анализ",
            "",
            f"Вероятность роста (LONG): {long_p}%",
            f"Вероятность падения (SHORT): {short_p}%",
            "",
            f"Приоритет: {bias_txt}",
            f"Уверенность: {conf_txt}",
            "",
            line,
            "🧠 Сценарии движения",
            "",
            "📈 Bullish сценарий:",
            f"Если цена удержит {s1_s} → движение к {r1_s}",
            f"Пробой {r1_s} → продолжение к {r2_s}",
            "",
            "📉 Bearish сценарий:",
            f"Если цена потеряет {s1_s} → снижение к {s2_s}",
            "",
            line,
            "📍 Торговый план (Institutional)",
            "",
            entry_status_txt,
            f"Зона входа: {entry_zone_txt}",
            f"{entry_kind_txt}",
            f"{entry_quality_txt}",
            f"{entry_notes_txt}",
            f"{entry_trigger_txt}",
            f"{entry_inval_txt}",
            "",
            "Stop Loss (на основе ATR + структуры):",
            f"SL: {sl_s}",
            "",
            "Take Profit цели:",
            f"TP1: {tp1_s}",
            f"TP2: {tp2_s}",
            "",
            "Risk/Reward:",
            f"TP1 RR: {rr1_txt}",
            f"TP2 RR: {rr2_txt}",
            "",
            line,
            "🧠 Итоговый институциональный вывод",
            "",
            "\n".join(concl) if concl else "—",
            "",
            f"Основной сценарий: {'продолжение роста 📈' if bias=='LONG' else 'снижение 📉'}",
            f"Предпочтение: {bias}",
            "",
            f"🛡️ SL рассчитан по ATR ({atr_tf})",
        ]
    else:
        # ---------- report (legacy EN/other) ----------
        report_lines = [
            f"📊 **{sym} ({mkt})**",
            _tr_i18n(lang, "analysis_price_label", price=price_s) if "analysis_price_label" in I18N.get(lang, {}) else f"💰 **Цена:** {price_s} USDT",
            "",
            line,
            "",
            _tr_i18n(lang, "analysis_market_structure_title") if "analysis_market_structure_title" in I18N.get(lang, {}) else "## 🧭 Market structure",
            "",
            _tr_i18n(lang, "analysis_global_trend", t=trend4) if "analysis_global_trend" in I18N.get(lang, {}) else f"**Trend (4h):** {trend4}",
            _tr_i18n(lang, "analysis_mid_trend", t=trend1) if "analysis_mid_trend" in I18N.get(lang, {}) else f"**Trend (1h):** {trend1}",
            _tr_i18n(lang, "analysis_local_trend", t=trend5) if "analysis_local_trend" in I18N.get(lang, {}) else f"**Trend (5m):** {trend5}",
            "",
            f"Regime: **{regime}** | Structure: **{struct_lbl}**",
            f"Volatility: **{vola_state}** | {vola_risk}",
            f"Volume phase: **{vol_phase}**",
            "",
            line,
            "",
            "## 📐 Channel",
            "",
            f"Type: **{chan_type_txt}**",
            f"Support: **{chan_s_s}**",
            f"Mid: **{chan_m_s}**",
            f"Resistance: **{chan_r_s}**",
            f"Status: {chan_pos}",
            "",
            f"Trendlines: S={tl_s_txt} | R={tl_r_txt}",
            f"Breakout/Retest: {brk} | Liquidity: {liq}",
            f"SMC: {smz} | {smc_event}",
            f"FVG: {fvg_txt}",
            "",
            line,
            "",
            "## 🎯 Levels",
            "",
            f"R1: {r1_s}",
            f"R2: {r2_s}",
            f"S1: {s1_s}",
            f"S2: {s2_s}",
            f"Pivot: {piv_s}",
            f"Pivot points: P={P_s} R1={R1_s} R2={R2_s} S1={S1_s} S2={S2_s}",
            "",
            line,
            "",
            "## ⚡ Momentum",
            "",
            f"RSI: {rsi_i} | MACD: {macd_label} | ADX: {adx_i}",
            f"Divergence: {div} | EMA20 slope: {slope_ema}",
            f"ATR%: {atr_s}% | SL ATR TF: {atr_tf}",
            f"Volume ratio: {vol_s} ({vol_status}) | Volume trend: {vol_trend} | {vol_spike}",
            "",
            line,
            "",
            "## 📊 Probability",
            "",
            f"LONG: {long_p}% | SHORT: {short_p}%",
            f"Bias: **{bias}** | Confidence: **{confidence}**",
            "",
            line,
            "",
            "## 🧾 Trading Plan",
            "",
            f"Side: {bias}",
            f"Entry: {entry_s}",
            f"SL: {sl_s}",
            f"TP1: {tp1_s}",
            f"TP2: {tp2_s}",
            f"RR (to TP2): {_fmt_float(rr,1)}",
            "",
            line,
            "⚠️ Not financial advice.",
        ]

    report = "\n".join([x for x in report_lines if x is not None])
    return report




# =========================
# Institutional TA bindings
# =========================

async def _backend_load_candles(self: "Backend", symbol: str, tf: str, market: str = "FUTURES", limit: int | None = None) -> pd.DataFrame:
    """Load OHLCV candles for symbol/tf/market using existing MultiExchangeData adapters.
    Returns normalized df with columns: open/high/low/close/(volume).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return pd.DataFrame()
    tf = (tf or "5m").strip().lower()
    mkt = (market or "FUTURES").strip().upper()
    if limit is None:
        limit = 300 if tf in ("1h","4h") else 500

    # Prefer exchanges depending on market
    if mkt == "FUTURES":
        ex_order = ["BYBIT", "BINANCE", "OKX"]
    else:
        ex_order = ["BINANCE", "OKX", "BYBIT", "GATEIO", "MEXC"]

    async with MultiExchangeData() as api:
        for ex in ex_order:
            try:
                if ex == "BINANCE":
                    df = await api.klines_binance(sym, tf, limit=limit, market=mkt)
                elif ex == "BYBIT":
                    df = await api.klines_bybit(sym, tf, limit=limit, market=mkt)
                elif ex == "OKX":
                    df = await api.klines_okx(sym, tf, limit=limit, market=mkt)
                elif ex == "GATEIO":
                    if mkt == "FUTURES":
                        continue
                    df = await api.klines_gateio(sym, tf, limit=limit)
                else:  # MEXC
                    if mkt == "FUTURES":
                        continue
                    df = await api.klines_mexc(sym, tf, limit=limit)

                dfn = _mid_norm_ohlcv(df)
                if dfn is None or getattr(dfn, "empty", True):
                    continue
                # Ensure volume exists
                if "volume" not in dfn.columns:
                    dfn["volume"] = np.nan
                return dfn.reset_index(drop=True)
            except Exception:
                continue
    return pd.DataFrame()

# Bind methods to Backend (non-invasive, avoids moving large blocks inside class)
try:
    Backend.load_candles = _backend_load_candles  # type: ignore[attr-defined]
except Exception:
    pass

try:
    Backend.analyze_symbol_institutional = analyze_symbol_institutional  # type: ignore[attr-defined]
except Exception:
    pass
