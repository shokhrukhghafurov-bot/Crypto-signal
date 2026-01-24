\
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import os
import random
import re
import time
import datetime as dt
import math
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List, Any

import aiohttp
import hmac
import hashlib
import base64
import urllib.parse
import numpy as np
import pandas as pd
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
        return {"ok": False, "skipped": True, "api_error": None}

    st = await db_store.get_autotrade_settings(uid)
    mt = "spot" if market == "SPOT" else "futures"

    # --- Admin gate (like SIGNAL): global per-user Auto-trade allow/deny + expiry ---
    # This is independent from per-market toggles in autotrade_settings.
    # If disabled/expired/blocked -> skip silently.
    try:
        acc = await db_store.get_autotrade_access(uid)
        if bool(acc.get("is_blocked")):
            return {"ok": False, "skipped": True, "api_error": None}
        if not bool(acc.get("autotrade_enabled")):
            return {"ok": False, "skipped": True, "api_error": None}
        if bool(acc.get("autotrade_stop_after_close")):
            return {"ok": False, "skipped": True, "api_error": None}
        exp = acc.get("autotrade_expires_at")
        if exp is not None:
            import datetime as _dt
            now = _dt.datetime.now(_dt.timezone.utc)
            try:
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=_dt.timezone.utc)
                if exp <= now:
                    return {"ok": False, "skipped": True, "api_error": None}
            except Exception:
                return {"ok": False, "skipped": True, "api_error": None}
    except Exception:
        # best-effort: if access columns not ready, default to allow
        pass

    enabled = bool(st.get("spot_enabled")) if mt == "spot" else bool(st.get("futures_enabled"))
    if not enabled:
        return {"ok": False, "skipped": True, "api_error": None}

    
    exchange = str(st.get("spot_exchange" if mt == "spot" else "futures_exchange") or "binance").lower().strip()

    # SPOT: choose exchange based on user's priority intersecting with signal confirmations.
    # Fallback order is user-defined (1->2->3...), and we ONLY trade on exchanges that confirmed the signal.
    if mt == "spot":
        # Parse confirmations like "BYBIT+OKX" to {"bybit","okx"}
        conf_raw = str(getattr(sig, "confirmations", "") or "")
        conf_set: set[str] = set()
        for part in re.split(r"[+ ,;/|]+", conf_raw.strip()):
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
            return {"ok": True, "skipped": True, "api_error": None}
        exchange = chosen

    # FUTURES: only Binance/Bybit supported
    if mt == "futures":
        if exchange not in ("binance", "bybit"):
            exchange = "binance"

    # amounts
    spot_amt = float(st.get("spot_amount_per_trade") or 0.0)
    fut_margin = float(st.get("futures_margin_per_trade") or 0.0)
    fut_cap = float(st.get("futures_cap") or 0.0)
    fut_lev = int(st.get("futures_leverage") or 1)

    need_usdt = spot_amt if mt == "spot" else fut_margin
    # Hard minimums (validated in bot UI and DB, but re-checked here for safety)
    if mt == "spot" and 0 < need_usdt < 15:
        return {"ok": False, "skipped": True, "api_error": None}
    if mt == "futures" and 0 < need_usdt < 10:
        return {"ok": False, "skipped": True, "api_error": None}
    if need_usdt <= 0:
        return {"ok": False, "skipped": True, "api_error": None}

    # fetch keys
    row = await db_store.get_autotrade_keys_row(user_id=uid, exchange=exchange, market_type=mt)
    if not row or not bool(row.get("is_active")):
        return {"ok": False, "skipped": True, "api_error": None}
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
        return {"ok": False, "skipped": True, "api_error": None}

    direction = str(getattr(sig, "direction", "LONG") or "LONG").upper()
    entry = float(getattr(sig, "entry", 0.0) or 0.0)
    sl = float(getattr(sig, "sl", 0.0) or 0.0)
    tp1 = float(getattr(sig, "tp1", 0.0) or 0.0)
    tp2 = float(getattr(sig, "tp2", 0.0) or 0.0)

    try:
        if exchange == "bybit":
            category = "spot" if mt == "spot" else "linear"
            # Futures cap is user-defined (by margin). Spot cap is implicit (wallet balance).
            if mt == "futures":
                used = await db_store.get_autotrade_used_usdt(uid, "futures")
                if fut_cap > 0 and used + need_usdt > fut_cap:
                    return {"ok": False, "skipped": True, "api_error": None}
            free = await _bybit_available_usdt(api_key, api_secret)
            if free < need_usdt:
                return {"ok": False, "skipped": True, "api_error": None}

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
            return {"ok": True, "skipped": False, "api_error": None}

        # -------- Binance --------
        if mt == "spot":
            free = await _binance_spot_free_usdt(api_key, api_secret)
            if free < need_usdt:
                return {"ok": False, "skipped": True, "api_error": None}

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
            return {"ok": False, "skipped": True, "api_error": None}

        avail = await _binance_futures_available_margin(api_key, api_secret)
        if avail < need_usdt:
            return {"ok": False, "skipped": True, "api_error": None}

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
        return {"ok": False, "skipped": False, "api_error": err}
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


async def autotrade_manager_loop(*, notify_api_error) -> None:
    """Background loop to manage SL/TP/BE for real orders.

    notify_api_error(user_id:int, text:str) will be called only on API errors.
    """

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
        """Compute approximate pnl using entry/exit average fill prices.

        Note: This is an approximation. For partial closes, it accounts only the final closing order.
        """
        try:
            side = str(ref.get("side") or "BUY").upper()
            qty = _as_float(ref.get("qty"), 0.0)
            if qty <= 0:
                return None, None

            if exchange == "binance":
                entry_p = _binance_avg_price(entry_order)
                exit_p = _binance_avg_price(exit_order)
            else:
                entry_p = _bybit_avg_price(entry_order)
                exit_p = _bybit_avg_price(exit_order)

            if entry_p <= 0 or exit_p <= 0:
                return None, None

            # BUY = long, SELL = short (for futures); for spot we assume BUY then SELL
            if side == "BUY":
                pnl = (exit_p - entry_p) * qty
            else:
                pnl = (entry_p - exit_p) * qty

            alloc = float(allocated_usdt or 0.0)
            roi = (pnl / alloc * 100.0) if alloc > 0 else None
            return float(pnl), (float(roi) if roi is not None else None)
        except Exception:
            return None, None

    # Best-effort; never crash.
    while True:
        try:
            rows = await db_store.list_open_autotrade_positions(limit=500)
            for r in rows:
                try:
                    ref = json.loads(r.get("api_order_ref") or "{}")
                except Exception:
                    continue
                ex = str(ref.get("exchange") or r.get("exchange") or "").lower()
                if ex not in ("binance", "bybit"):
                    continue

                uid = int(r.get("user_id"))
                mt = str(ref.get("market_type") or r.get("market_type") or "").lower()
                futures = (mt == "futures")
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

                tp1_id = ref.get("tp1_order_id")
                tp2_id = ref.get("tp2_order_id")
                sl_id = ref.get("sl_order_id")
                be_moved = bool(ref.get("be_moved"))
                be_price = float(ref.get("be_price") or 0.0)
                close_side = str(ref.get("close_side") or ("SELL" if str(ref.get("side")) == "BUY" else "BUY")).upper()

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
    return i18n.get(lang, i18n.get("en", {})).get(key, key)

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



def _price_debug_block(uid: int, *, price: float, source: str, side: str, sl: float | None, tp1: float | None, tp2: float | None) -> str:
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
    checks = []
    if sl is not None and float(sl) > 0:
        lvl = float(sl)
        checks.append(f"SL: {lvl:.6f} {'✅' if hit_sl(lvl) else '❌'}")
    if tp1 is not None and float(tp1) > 0:
        lvl = float(tp1)
        checks.append(f"{_tr(uid, 'lbl_tp1')}: {lvl:.6f} {'✅' if hit_tp(lvl) else '❌'}")
    if tp2 is not None and float(tp2) > 0:
        lvl = float(tp2)
        checks.append(f"TP2: {lvl:.6f} {'✅' if hit_tp(lvl) else '❌'}")
    if checks:
        lines.append(f"🧪 {_tr(uid, 'lbl_check')}:" )
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
@dataclass(frozen=True)
class Signal:
    signal_id: int
    market: str
    symbol: str
    direction: str
    timeframe: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr: float
    confidence: int
    confirmations: str
    risk_note: str
    ts: float

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
    def __init__(self) -> None:
        self._prices: Dict[Tuple[str, str], float] = {}
        self._sources: Dict[Tuple[str, str], str] = {}
        self._tasks: Dict[Tuple[str, str], asyncio.Task] = {}

    def get_latest(self, market: str, symbol: str) -> Optional[float]:
        return self._prices.get((market.upper(), symbol.upper()))

    def get_latest_source(self, market: str, symbol: str) -> Optional[str]:
        return self._sources.get((market.upper(), symbol.upper()))

    def _set_latest(self, market: str, symbol: str, price: float, source: str = "WS") -> None:
        key = (market.upper(), symbol.upper())
        self._prices[key] = float(price)
        self._sources[key] = str(source or "WS")

    async def ensure_stream(self, market: str, symbol: str) -> None:
        key = (market.upper(), symbol.upper())
        t = self._tasks.get(key)
        if t and not t.done():
            return
        self._tasks[key] = asyncio.create_task(self._run_stream(market, symbol))

    async def _run_stream(self, market: str, symbol: str) -> None:
        m = market.upper()
        sym = symbol.lower()
        url = f"wss://stream.binance.com:9443/ws/{sym}@trade" if m == "SPOT" else f"wss://fstream.binance.com/ws/{sym}@trade"
        backoff = 1
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    backoff = 1
                    async for msg in ws:
                        data = json.loads(msg)
                        p = data.get("p")
                        if p is not None:
                            self._set_latest(m, symbol, float(p), source="WS")
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def mock_price(self, market: str, symbol: str, base: float | None = None) -> float:
        key = ("MOCK:" + market.upper(), symbol.upper())
        seed = (float(base) if base is not None and base > 0 else None)
        price = self._prices.get(key, seed if seed is not None else random.uniform(100, 50000))
        price *= random.uniform(0.996, 1.004)
        self._prices[key] = price
        self._sources[key] = "MOCK"
        return price

# ------------------ Exchange data (candles) ------------------
class MultiExchangeData:
    BINANCE_SPOT = "https://api.binance.com"
    BINANCE_FUTURES = "https://fapi.binance.com"
    BYBIT = "https://api.bybit.com"
    OKX = "https://www.okx.com"

    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def get_top_usdt_symbols(self, n: int) -> List[str]:
        assert self.session is not None
        url = f"{self.BINANCE_SPOT}/api/v3/ticker/24hr"
        async with self.session.get(url) as r:
            r.raise_for_status()
            data = await r.json()

        items = []
        for t in data:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
                continue
            try:
                qv = float(t.get("quoteVolume", "0") or 0)
            except Exception:
                qv = 0.0
            items.append((qv, sym))
        items.sort(reverse=True, key=lambda x: x[0])
        if n <= 0:
            return [sym for _, sym in items]
        return [sym for _, sym in items[:n]]

    async def _get_json(self, url: str, params: Optional[Dict[str, str]] = None) -> Any:
        assert self.session is not None
        async with self.session.get(url, params=params) as r:
            r.raise_for_status()
            return await r.json()

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

    async def klines_binance(self, symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
        url = f"{self.BINANCE_SPOT}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": str(limit)}
        raw = await self._get_json(url, params=params)
        return self._df_from_ohlcv(raw, "binance")

    
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
    async def klines_bybit(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        interval_map = {"15m":"15", "1h":"60", "4h":"240"}
        itv = interval_map.get(interval, "15")
        url = f"{self.BYBIT}/v5/market/kline"
        params = {"category": "spot", "symbol": symbol, "interval": itv, "limit": str(limit)}
        data = await self._get_json(url, params=params)
        rows = (data or {}).get("result", {}).get("list", []) or []
        rows = list(reversed(rows))
        return self._df_from_ohlcv(rows, "bybit")

    def okx_inst(self, symbol: str) -> str:
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT"
        return symbol

    async def klines_okx(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        bar_map = {"15m":"15m", "1h":"1H", "4h":"4H"}
        bar = bar_map.get(interval, "15m")
        inst = self.okx_inst(symbol)
        url = f"{self.OKX}/api/v5/market/candles"
        params = {"instId": inst, "bar": bar, "limit": str(limit)}
        data = await self._get_json(url, params=params)
        rows = (data or {}).get("data", []) or []
        rows = list(reversed(rows))
        return self._df_from_ohlcv(rows, "okx")

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
        df["ema50"] = EMAIndicator(close, window=50).ema_indicator()
        df["ema200"] = EMAIndicator(close, window=200).ema_indicator()
    except Exception:
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


def _build_levels(direction: str, entry: float, atr: float) -> Tuple[float, float, float, float]:
    R = max(1e-9, ATR_MULT_SL * atr)
    if direction == "LONG":
        sl = entry - R
        tp1 = entry + TP1_R * R
        tp2 = entry + TP2_R * R
    else:
        sl = entry + R
        tp1 = entry - TP1_R * R
        tp2 = entry - TP2_R * R
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
    """Compute 0..100 TA score (used as 'confidence' too). Keep deterministic and simple."""
    score = 50.0
    # trend strength
    if not np.isnan(adx4):
        score += min(18.0, max(0.0, (adx4 - 15.0) * 1.2))
    if not np.isnan(adx1):
        score += min(12.0, max(0.0, (adx1 - 15.0) * 0.9))
    # volume
    if not np.isnan(vol_rel):
        score += min(10.0, max(0.0, (vol_rel - 1.0) * 8.0))
    # macd impulse
    if not np.isnan(macd_hist15):
        if direction == "LONG":
            score += 6.0 if macd_hist15 >= 0 else 0.0
        else:
            score += 6.0 if macd_hist15 <= 0 else 0.0
    # rsi sanity
    if not np.isnan(rsi15):
        if direction == "LONG":
            score += 6.0 if 45 <= rsi15 <= TA_RSI_MAX_LONG else 2.0
        else:
            score += 6.0 if TA_RSI_MIN_SHORT <= rsi15 <= 55 else 2.0
    # divergence bonus
    if (direction == "LONG" and rsi_div == "BULL") or (direction == "SHORT" and rsi_div == "BEAR"):
        score += 8.0
    # channel bonus
    if isinstance(channel, str) and channel != "—":
        if direction == "LONG" and ("asc@lower" in channel or "flat@lower" in channel):
            score += 4.0
        if direction == "SHORT" and ("desc@upper" in channel or "flat@upper" in channel):
            score += 4.0
    # market structure
    if (direction == "LONG" and mstruct == "HH-HL") or (direction == "SHORT" and mstruct == "LH-LL"):
        score += 7.0
    elif mstruct == "RANGE":
        score -= 4.0
    # pattern
    if (direction == "LONG" and pat_bias > 0) or (direction == "SHORT" and pat_bias < 0):
        score += 3.0
    # atr sanity (too wild reduces score)
    if not np.isnan(atr_pct):
        if atr_pct > 6.0:
            score -= 6.0
        elif atr_pct > 4.0:
            score -= 3.0
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

    entry = float(last15["close"])
    atr = float(last1h.get("atr", np.nan))
    if np.isnan(atr) or atr <= 0:
        return None

    atr_pct = (atr / entry) * 100.0 if entry > 0 else 0.0
    sl, tp1, tp2, rr = _build_levels(dir1, entry, atr)

    # Snapshot values
    rsi15 = float(last15.get("rsi", np.nan))
    macd_hist15 = float(last15.get("macd_hist", np.nan))
    adx4 = float(last4h.get("adx", np.nan))
    adx1 = float(last1h.get("adx", np.nan))

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
    ta["ta_block"] = _fmt_ta_block(ta)
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
        # Trades are stored in PostgreSQL (db_store)
        self._last_signal_ts: Dict[str, float] = {}
        self._signal_seq = 1

        # Track when price fetching started failing per trade_id (used for forced CLOSE)
        self._price_fail_since: Dict[int, float] = {}

        self.last_signal: Optional[Signal] = None
        self.last_spot_signal: Optional[Signal] = None
        self.last_futures_signal: Optional[Signal] = None
        self.scanned_symbols_last: int = 0
        self.last_news_action: str = "ALLOW"
        self.last_macro_action: str = "ALLOW"
        self.scanner_running: bool = True
        self.last_scan_ts: float = 0.0
        self.trade_stats: dict = {}
        self._load_trade_stats()


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
        # manual close (keep last known price if present)
        close_price = float(row.get("entry") or 0.0)
        try:
            await db_store.close_trade(trade_id, status="CLOSED", price=close_price, pnl_total_pct=float(row.get("pnl_total_pct") or 0.0))
        except Exception:
            # best effort
            await db_store.close_trade(trade_id, status="CLOSED")
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

    async def remove_trade_by_id(self, user_id: int, trade_id: int) -> bool:
        row = await db_store.get_trade_by_id(int(user_id), int(trade_id))
        if not row:
            return False
        # manual close (keep last known price if present)
        close_price = float(row.get('entry') or 0.0)
        try:
            await db_store.close_trade(int(trade_id), status='CLOSED', price=close_price, pnl_total_pct=float(row.get('pnl_total_pct') or 0.0))
        except Exception:
            await db_store.close_trade(int(trade_id), status='CLOSED')
        return True

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
    async def _fetch_rest_price(self, market: str, symbol: str) -> float | None:
        """Binance REST fallback price (spot/futures)."""
        m = (market or "FUTURES").upper()
        sym = (symbol or "").upper()
        if not sym:
            return None
        url = "https://api.binance.com/api/v3/ticker/price" if m == "SPOT" else "https://fapi.binance.com/fapi/v1/ticker/price"
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
            if sym.endswith("USDT"):
                base = sym[:-4]
            else:
                base = sym
            inst = f"{base}-USDT" if mkt == "SPOT" else f"{base}-USDT-SWAP"
            url = "https://www.okx.com/api/v5/market/ticker"
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


    async def _get_price_with_source(self, signal: Signal) -> tuple[float, str]:
        """Return (price, source) for tracking.

        Sources:
          - BINANCE_WS / BINANCE_REST
          - BYBIT_REST
          - MEDIAN(BINANCE+BYBIT) / BINANCE_ONLY / BYBIT_ONLY
          - OKX_REST

        Note: if all sources fail, raises PriceUnavailableError.

        Selection is controlled via env:
          SPOT_PRICE_SOURCE=BINANCE|BYBIT|MEDIAN
          FUTURES_PRICE_SOURCE=BINANCE|BYBIT|MEDIAN
        """
        market = (signal.market or "FUTURES").upper()
        base = float(getattr(signal, "entry", 0) or 0) or None

        # Mock mode: keep prices around entry to avoid nonsense.
        if not USE_REAL_PRICE:
            return float(self.feed.mock_price(market, signal.symbol, base=base)), "MOCK"

        mode = (SPOT_PRICE_SOURCE if market == "SPOT" else FUTURES_PRICE_SOURCE).upper().strip() or "MEDIAN"
        if mode not in ("BINANCE", "BYBIT", "MEDIAN"):
            mode = "MEDIAN"

        def _is_reasonable(p: float | None) -> bool:
            """Sanity filter to avoid using bad ticks (e.g., 0.0) that can
            accidentally distort MEDIAN logic and trigger false TP/SL hits.

            We keep it intentionally permissive, but:
              - reject non-finite / <= 0
              - if entry is known, reject values that are wildly off-scale
                (e.g., 10x away), which usually indicates a wrong market/symbol.
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

        async def get_binance() -> tuple[float | None, str]:
            # websocket first, REST fallback
            try:
                await self.feed.ensure_stream(market, signal.symbol)
                latest = self.feed.get_latest(market, signal.symbol)
                if latest is not None and float(latest) > 0:
                    src = self.feed.get_latest_source(market, signal.symbol) or "BINANCE_WS"
                    if src == "WS":
                        src = "BINANCE_WS"
                    elif src == "REST":
                        src = "BINANCE_REST"
                    # Guard against rare cases where a stale/invalid cached tick
                    # (e.g., 0.0) could slip through.
                    if _is_reasonable(float(latest)):
                        return float(latest), str(src)
            except Exception:
                pass
            rest = await self._fetch_rest_price(market, signal.symbol)
            if _is_reasonable(rest):
                try:
                    self.feed._set_latest(market, signal.symbol, float(rest), source="REST")
                except Exception:
                    pass
                return float(rest), "BINANCE_REST"
            return None, "BINANCE"

        async def get_bybit() -> tuple[float | None, str]:
            p = await self._fetch_bybit_price(market, signal.symbol)
            if _is_reasonable(p):
                return float(p), "BYBIT_REST"
            return None, "BYBIT"

        async def get_okx() -> tuple[float | None, str]:
            p = await self._fetch_okx_price(market, signal.symbol)
            if _is_reasonable(p):
                return float(p), "OKX_REST"
            return None, "OKX"

        if mode == "BINANCE":
            b, bsrc = await get_binance()
            if b is not None:
                return float(b), bsrc
            y, ysrc = await get_bybit()
            if y is not None:
                return float(y), ysrc
            o, osrc = await get_okx()
            if o is not None:
                return float(o), osrc
        elif mode == "BYBIT":
            y, ysrc = await get_bybit()
            if y is not None:
                return float(y), ysrc
            o, osrc = await get_okx()
            if o is not None:
                return float(o), osrc
            b, bsrc = await get_binance()
            if b is not None:
                return float(b), bsrc
        else:  # MEDIAN
            b, bsrc = await get_binance()
            y, ysrc = await get_bybit()

            b_ok = _is_reasonable(b)
            y_ok = _is_reasonable(y)

            if b_ok and y_ok:
                fb = float(b)
                fy = float(y)
                # If one exchange suddenly returns a very different price
                # (or we somehow got a stale tick), don't average it —
                # it can create a fake mid-price and trigger TP/SL.
                try:
                    div = abs(fb - fy) / max(1e-12, min(fb, fy))
                except Exception:
                    div = 0.0

                max_div = float(os.getenv("PRICE_MAX_DIVERGENCE_PCT", "0.05") or 0.05)
                if max_div < 0:
                    max_div = 0.0

                if div > max_div:
                    if base is not None and base > 0:
                        # choose the one closer to entry
                        if abs(fb - base) <= abs(fy - base):
                            return fb, "BINANCE_ONLY(DIVERGE)"
                        return fy, "BYBIT_ONLY(DIVERGE)"
                    # default preference: Binance
                    return fb, "BINANCE_ONLY(DIVERGE)"

                price = float(np.median([fb, fy]))
                return price, "MEDIAN(BINANCE+BYBIT)"
            if b_ok:
                return float(b), "BINANCE_ONLY"
            if y_ok:
                return float(y), "BYBIT_ONLY"


            o, osrc = await get_okx()
            if _is_reasonable(o):
                return float(o), "OKX_ONLY"

        # All sources failed -> let caller decide (skip tick / forced CLOSE)
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
                    effective_sl = (be_price if (tp1_hit and _be_enabled(market) and be_price > 0) else (float(s.sl) if s.sl else None))
                    dbg = _price_debug_block(
                        uid,
                        price=price_f,
                        source=price_src,
                        side=side,
                        sl=effective_sl,
                        tp1=(float(s.tp1) if s.tp1 else None),
                        tp2=(float(s.tp2) if s.tp2 else None),
                    )
                    # 1) Before TP1: TP2 (gap) -> WIN
                    if not tp1_hit and s.tp2 and hit_tp(float(s.tp2)):
                        trade_ctx = UserTrade(user_id=uid, signal=s, tp1_hit=tp1_hit)
                        pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(s.tp2), close_reason="WIN")
                        import datetime as _dt
                        now_utc = _dt.datetime.now(_dt.timezone.utc)
                        txt = _trf(uid, "msg_auto_win",
                            symbol=s.symbol,
                            market=market,
                            pnl_total=fmt_pnl_pct(float(pnl)),
                            opened_time=fmt_dt_msk(row.get("opened_at")),
                            closed_time=fmt_dt_msk(now_utc),
                            status="WIN",
                        )
                        if dbg:
                            txt += "\n\n" + dbg
                        await safe_send(bot, uid, txt, ctx="msg_auto_win")
                        await db_store.close_trade(trade_id, status="WIN", price=float(s.tp2), pnl_total_pct=float(pnl))
                        continue

                    # 2) Before TP1: SL -> LOSS
                    if not tp1_hit and s.sl and hit_sl(float(s.sl)):
                        trade_ctx = UserTrade(user_id=uid, signal=s, tp1_hit=tp1_hit)
                        pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(s.sl), close_reason="LOSS")
                        import datetime as _dt
                        now_utc = _dt.datetime.now(_dt.timezone.utc)
                        txt = _trf(uid, "msg_auto_loss",
                            symbol=s.symbol,
                            market=market,
                            sl=f"{float(s.sl):.6f}",
                            opened_time=fmt_dt_msk(row.get("opened_at")),
                            closed_time=fmt_dt_msk(now_utc),
                            status="LOSS",
                        )
                        if dbg:
                            txt += "\n\n" + dbg
                        await safe_send(bot, uid, txt, ctx="msg_auto_loss")
                        await db_store.close_trade(trade_id, status="LOSS", price=float(s.sl), pnl_total_pct=float(pnl))
                        continue

                    # 3) TP1 -> partial close + BE
                    if not tp1_hit and s.tp1 and hit_tp(float(s.tp1)):
                        be_px = _be_exit_price(s.entry, side, market)
                        import datetime as _dt
                        now_utc = _dt.datetime.now(_dt.timezone.utc)
                        txt = _trf(uid, "msg_auto_tp1",
                            symbol=s.symbol,
                            market=market,
                            closed_pct=int(_partial_close_pct(market)),
                            be_price=f"{float(be_px):.6f}",
                            opened_time=fmt_dt_msk(row.get("opened_at")),
                            event_time=fmt_dt_msk(now_utc),
                            status="TP1",
                        )
                        if dbg:
                            txt += "\n\n" + dbg
                        await safe_send(bot, uid, txt, ctx="msg_auto_tp1")
                        await db_store.set_tp1(trade_id, be_price=float(be_px), price=float(s.tp1), pnl_pct=float(calc_profit_pct(s.entry, float(s.tp1), side)))
                        continue

                    # 3) After TP1: BE close
                    if tp1_hit and _be_enabled(market):
                        be_lvl = be_price if be_price else _be_exit_price(s.entry, side, market)
                        if hit_sl(float(be_lvl)):
                            import datetime as _dt
                            now_utc = _dt.datetime.now(_dt.timezone.utc)
                            txt = _trf(uid, "msg_auto_be",
                                symbol=s.symbol,
                                market=market,
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
                            continue

                    # 4) TP2 -> WIN
                    if s.tp2 and hit_tp(float(s.tp2)):
                        trade_ctx = UserTrade(user_id=uid, signal=s, tp1_hit=tp1_hit)
                        pnl = _calc_effective_pnl_pct(trade_ctx, close_price=float(s.tp2), close_reason="WIN")
                        import datetime as _dt
                        now_utc = _dt.datetime.now(_dt.timezone.utc)
                        txt = _trf(uid, "msg_auto_win",
                            symbol=s.symbol,
                            market=market,
                            pnl_total=fmt_pnl_pct(float(pnl)),
                            opened_time=fmt_dt_msk(row.get("opened_at")),
                            closed_time=fmt_dt_msk(now_utc),
                            status="WIN",
                        )
                        if dbg:
                            txt += "\n\n" + dbg
                        await safe_send(bot, uid, txt, ctx="msg_auto_win")
                        await db_store.close_trade(trade_id, status="WIN", price=float(s.tp2), pnl_total_pct=float(pnl))
                        continue

                except Exception:
                    logger.exception("track_loop: error while tracking trade row=%r", row)

            await asyncio.sleep(TRACK_INTERVAL_SECONDS)

    async def scanner_loop(self, emit_signal_cb, emit_macro_alert_cb) -> None:
        while True:
            start = time.time()
            logger.info("SCAN tick start top_n=%s interval=%ss news_filter=%s macro_filter=%s", TOP_N, SCAN_INTERVAL_SECONDS, bool(NEWS_FILTER and CRYPTOPANIC_TOKEN), bool(MACRO_FILTER))
            logger.info("[scanner] tick start TOP_N=%s interval=%ss", TOP_N, SCAN_INTERVAL_SECONDS)
            self.last_scan_ts = start
            try:
                async with MultiExchangeData() as api:
                    await self.macro.ensure_loaded(api.session)  # type: ignore[arg-type]

                    symbols = await api.get_top_usdt_symbols(TOP_N)
                    self.scanned_symbols_last = len(symbols)
                    logger.info("[scanner] symbols loaded: %s", self.scanned_symbols_last)

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
                                else:
                                    df15 = await api.klines_okx(sym, "15m", 200)
                                    df1h = await api.klines_okx(sym, "1h", 200)
                                    df4h = await api.klines_okx(sym, "4h", 200)
                                res = evaluate_on_exchange(df15, df1h, df4h)
                                return name, res
                            except Exception:
                                return name, None

                        results = await asyncio.gather(fetch_exchange("BINANCE"), fetch_exchange("BYBIT"), fetch_exchange("OKX"))
                        good = [(name, r) for (name, r) in results if r is not None]
                        if len(good) < 2:
                            continue

                        dirs: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
                        for name, r in good:
                            dirs.setdefault(r["direction"], []).append((name, r))
                        best_dir, supporters = max(dirs.items(), key=lambda kv: len(kv[1]))
                        if len(supporters) < 2:
                            continue

                        entry = float(np.median([s[1]["entry"] for s in supporters]))
                        sl = float(np.median([s[1]["sl"] for s in supporters]))
                        tp1 = float(np.median([s[1]["tp1"] for s in supporters]))
                        tp2 = float(np.median([s[1]["tp2"] for s in supporters]))
                        rr = float(np.median([s[1]["rr"] for s in supporters]))
                        conf = int(np.median([s[1]["confidence"] for s in supporters]))
                        
                        market = choose_market(float(np.nanmax([s[1]["adx1"] for s in supporters])),
                                              float(np.nanmax([s[1]["atr_pct"] for s in supporters])))
                        
                        min_conf = TA_MIN_SCORE_FUTURES if market == "FUTURES" else TA_MIN_SCORE_SPOT

# Orderbook confirmation (FUTURES only, enabled via ENV)
                        if USE_ORDERBOOK and (not ORDERBOOK_FUTURES_ONLY or market == "FUTURES"):
                            ob_allows: List[bool] = []
                            ob_notes: List[str] = []
                            for ex in [s[0] for s in supporters]:
                                if ex.strip().lower() not in ORDERBOOK_EXCHANGES:
                                    continue
                                ok, note = await orderbook_filter(api, ex, sym, best_dir)
                                ob_allows.append(bool(ok))
                                ob_notes.append(note)
                            # If we checked at least one exchange and none allowed -> block signal
                            if ob_allows and not any(ob_allows):
                                # keep a short reason in logs
                                logger.info("[orderbook] block %s %s %s notes=%s", sym, market, best_dir, "; ".join(ob_notes)[:200])
                                continue
                        
                        if conf < min_conf or rr < 2.0:
                            continue
                        
                        risk_notes = []

                        # Add TA summary (real indicators) to the signal card
                        try:
                            best_supporter = max(supporters, key=lambda s: int(s[1].get("confidence", 0)))
                            ta_block = str(best_supporter[1].get("ta_block", "") or "").strip()
                            if ta_block:
                                risk_notes.append(ta_block)
                        except Exception:
                            pass


                        if news_act == "FUTURES_OFF" and market == "FUTURES":
                            market = "SPOT"
                            risk_notes.append("⚠️ Futures paused due to news")
                        if mac_act == "FUTURES_OFF" and market == "FUTURES":
                            market = "SPOT"
                            risk_notes.append("⚠️ Futures signals are temporarily disabled (macro)")

                        # Policy: SPOT + SHORT is confusing for most users. Default behavior:
                        # auto-convert SPOT SHORT -> FUTURES SHORT (keeps Entry/SL/TP intact).
                        # Exception: if futures signals are currently forced OFF (news/macro), we cannot convert,
                        # so we skip such signals to avoid sending a non-executable SPOT SHORT.
                        if market == "SPOT" and str(best_dir).upper() == "SHORT":
                            fut_forced_off = any(
                                ("Futures paused" in n) or ("Futures signals are temporarily disabled" in n)
                                for n in risk_notes
                            )
                            if fut_forced_off:
                                logger.info(
                                    "[signal] skip %s %s %s reason=spot_short_but_futures_forced_off",
                                    sym,
                                    market,
                                    best_dir,
                                )
                                continue

                            market = "FUTURES"
                            risk_notes.append("ℹ️ Auto-converted: SPOT SHORT → FUTURES")
                            logger.info(
                                "[signal] convert %s SPOT/SHORT -> FUTURES/SHORT",
                                sym,
                            )

                        conf_names = "+".join([s[0] for s in supporters])

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
                            risk_note="\\n".join(risk_notes).strip(),
                            ts=time.time(),
                        )

                        self.mark_emitted(sym)
                        self.last_signal = sig
                        if sig.market == "SPOT":
                            self.last_spot_signal = sig
                        else:
                            self.last_futures_signal = sig

                        logger.info("[signal] emit %s %s %s conf=%s rr=%.2f notes=%s", sig.symbol, sig.market, sig.direction, sig.confidence, sig.rr, (sig.risk_note or "-")[:120])
                        logger.info("SIGNAL %s %s %s conf=%s rr=%.2f notes=%s", sig.market, sig.symbol, sig.direction, sig.confidence, sig.rr, (sig.risk_note or "-"))
                        logger.info("SIGNAL found %s %s %s conf=%s rr=%.2f exch=%s", sig.market, sig.symbol, sig.direction, sig.confidence, float(sig.rr), sig.confirmations)
                        await emit_signal_cb(sig)
                        await asyncio.sleep(2)

            except Exception:
                logger.exception("scanner_loop error")

            elapsed = time.time() - start
            logger.info("SCAN tick done scanned=%s elapsed=%.1fs last_news=%s last_macro=%s", int(getattr(self, "scanned_symbols_last", 0) or 0), elapsed, getattr(self, "last_news_action", "?"), getattr(self, "last_macro_action", "?"))
            await asyncio.sleep(max(5, SCAN_INTERVAL_SECONDS - elapsed))
