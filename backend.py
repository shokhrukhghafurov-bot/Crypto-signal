from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, unquote, quote
import base64
import hashlib
import ipaddress
import html
import json
from decimal import Decimal, ROUND_HALF_UP
import re
import secrets
from uuid import uuid4
import time
import threading
import socket
import shutil
import subprocess
import tempfile
from pathlib import Path
import logging
import xml.etree.ElementTree as ET


import jwt
import requests
import psycopg
from psycopg.rows import dict_row
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBasic, HTTPBasicCredentials, HTTPBearer
from pydantic import BaseModel, Field

from config import settings
from db_store import (
    activate_payment_and_extend_subscription,
    admin_create_or_update_user,
    bootstrap,
    create_location,
    create_payment_record,
    delete_device,
    delete_location,
    export_payments_csv,
    _compose_vpn_payload_for_location,
    _normalize_vpn_payload_keys,
    _config_is_complete,
    build_user_vpn_payload_for_location,
    _pick_virtual_location,
    get_active_plans,
    get_payment_by_internal_or_external,
    get_payment_for_user,
    get_plan_by_code,
    get_vpn_config_for_user,
    get_user_by_id,
    get_user_by_subscription_token,
    get_user_by_active_auth_code,
    get_user_by_telegram_id,
    get_user_snapshot_by_telegram,
    get_user_subscription_view,
    get_subscription_device_gate_by_token,
    ensure_user_subscription_token,
    issue_auth_code,
    consume_auth_code,
    list_admin_users,
    list_bot_errors,
    list_broadcast_targets,
    list_locations,
    list_payments,
    patch_location,
    reset_virtual_location_assignments_for_concrete_code,
    record_bot_error,
    refresh_subscription_statuses,
    register_device,
    touch_subscription_device_by_token,
    reset_user_devices_by_telegram,
    set_user_device_limit_override_by_telegram,
    set_user_language,
    set_user_status_by_telegram,
    settings_snapshot,
    save_runtime_settings_payload,
    sync_plans_from_env,
    upsert_telegram_user,
    update_payment,
    extend_user_subscription_by_telegram,
    enqueue_notification,
    _get_user_subscription_view_with_conn,
)


app = FastAPI(title=f"{settings.APP_NAME} VPN API")
logger = logging.getLogger("inet.vpn")

security = HTTPBearer(auto_error=False)
basic_security = HTTPBasic()


class TelegramAuthIn(BaseModel):
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    language: str = "ru"


class CodeAuthIn(BaseModel):
    code: str
    telegram_id: Optional[int] = None
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    language: str = "ru"


class IssueCodeIn(BaseModel):
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    language: str = "ru"


class RefreshIn(BaseModel):
    refresh_token: Optional[str] = None


class LanguageIn(BaseModel):
    language: str = "ru"


class DeviceRegisterIn(BaseModel):
    platform: str
    device_name: str
    device_fingerprint: str


class PaymentCreateIn(BaseModel):
    plan_code: str
    method: str = "telegram"


class AdminUserUpsertIn(BaseModel):
    telegram_id: int
    plan_code: str
    expires_at: Optional[datetime] = None
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    language: str = "ru"
    device_limit_override: Optional[int] = Field(default=None, ge=1)


class AdminUserPatchIn(BaseModel):
    device_limit_override: Optional[int] = None


class ExtendIn(BaseModel):
    days_added: Optional[int] = Field(default=None, gt=0)
    days: Optional[int] = Field(default=None, gt=0)
    reason: str = "manual extension"

    def normalized_days(self) -> int:
        return int(self.days_added or self.days or 30)


class LocationIn(BaseModel):
    code: str
    name_ru: str
    name_en: str
    country_code: Optional[str] = None
    is_active: bool = True
    is_recommended: bool = False
    is_reserve: bool = False
    active: Optional[bool] = None
    recommended: Optional[bool] = None
    reserve: Optional[bool] = None
    status: str = "online"
    sort_order: int = 100
    download_mbps: Optional[float] = None
    upload_mbps: Optional[float] = None
    ping_ms: Optional[int] = None
    speed_checked_at: Optional[str] = None
    access_mode: Optional[str] = None
    vpn_payload: Dict[str, Any] = Field(default_factory=dict)


class LocationPatchIn(BaseModel):
    name_ru: Optional[str] = None
    name_en: Optional[str] = None
    country_code: Optional[str] = None
    is_active: Optional[bool] = None
    is_recommended: Optional[bool] = None
    is_reserve: Optional[bool] = None
    active: Optional[bool] = None
    recommended: Optional[bool] = None
    reserve: Optional[bool] = None
    status: Optional[str] = None
    sort_order: Optional[int] = None
    download_mbps: Optional[float] = None
    upload_mbps: Optional[float] = None
    ping_ms: Optional[int] = None
    speed_checked_at: Optional[str] = None
    access_mode: Optional[str] = None
    vpn_payload: Optional[Dict[str, Any]] = None


class BroadcastIn(BaseModel):
    text: str = ""
    statuses: List[str] = Field(default_factory=lambda: ["active"])


class TestSendIn(BaseModel):
    telegram_id: int
    text: str


class VpnClientEventIn(BaseModel):
    platform: str
    stage: str = "runtime"
    status: Optional[str] = None
    location_code: Optional[str] = None
    error_message: Optional[str] = None
    details: Optional[str] = None


class AdminPlanSettingsIn(BaseModel):
    slot: str
    code: str
    name_ru: str
    name_en: Optional[str] = None
    price_rub: int = Field(default=0, ge=0)
    duration_days: int = Field(default=1, ge=1)
    device_limit: int = Field(default=1, ge=1)
    is_active: bool = True


class AdminVpnSettingsIn(BaseModel):
    app_name: str
    client_mode: str = "hiddify"
    app_env: str = "production"
    languages: List[str] = Field(default_factory=lambda: ["ru", "en"])
    bot_name: str
    bot_username: str
    support_telegram_url: str
    payments_enabled: bool = False
    maintenance_mode: bool = False
    new_activations_enabled: bool = True
    max_devices_per_account: int = Field(default=settings.VPN_MAX_DEVICES_PER_ACCOUNT, ge=1)
    device_limit: Optional[int] = Field(default=None, ge=1)
    plans: List[AdminPlanSettingsIn] = Field(default_factory=list)


RU_LTE_RESERVE_LOCATION_CODES: Tuple[str, ...] = ("ru-lte-reserve-1", "ru-lte-reserve-2", "ru-lte-reserve-3")
BLACK_RESERVE_LOCATION_CODES: Tuple[str, ...] = ("intl-fast-reserve-1", "intl-fast-reserve-2", "intl-fast-reserve-3")
RU_LTE_LOCATION_CODES: Tuple[str, ...] = ("ru-lte",) + RU_LTE_RESERVE_LOCATION_CODES
BLACK_LOCATION_CODES: Tuple[str, ...] = ("intl-fast",) + BLACK_RESERVE_LOCATION_CODES
PUBLIC_STABLE_LOCATION_CODES: Tuple[str, ...] = RU_LTE_LOCATION_CODES + BLACK_LOCATION_CODES
PUBLIC_VIRTUAL_LOCATION_CODES: Tuple[str, ...] = ("auto-fastest", "auto-reserve")
_DEAD_CANDIDATE_CACHE: Dict[str, Dict[str, float]] = {"ru_lte": {}, "black": {}}
_DEAD_CANDIDATE_LOCK = threading.Lock()
_PROJECT_ROOT = Path(__file__).resolve().parent


def _vpn_configs_raw_base() -> str:
    return str(getattr(settings, "VPN_CONFIGS_REPO_RAW_BASE", "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/main") or "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/main").strip().rstrip("/")


def _remote_source_aliases() -> Dict[str, str]:
    base = _vpn_configs_raw_base()
    return {
        "Vless-Reality-White-Lists-Rus-Mobile.txt": f"{base}/Vless-Reality-White-Lists-Rus-Mobile.txt",
        "Vless-Reality-White-Lists-Rus-Mobile-2.txt": f"{base}/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
        "WHITE-CIDR-RU-checked.txt": f"{base}/WHITE-CIDR-RU-checked.txt",
        "WHITE-CIDR-RU-all.txt": f"{base}/WHITE-CIDR-RU-all.txt",
        "BLACK_VLESS_RUS_mobile.txt": f"{base}/BLACK_VLESS_RUS_mobile.txt",
        "BLACK_VLESS_RUS.txt": f"{base}/BLACK_VLESS_RUS.txt",
    }


def _local_source_fallbacks() -> Dict[str, Path]:
    return {
        "Vless-Reality-White-Lists-Rus-Mobile.txt": _PROJECT_ROOT / "sources" / "ru_lte" / "Vless-Reality-White-Lists-Rus-Mobile.txt",
        "Vless-Reality-White-Lists-Rus-Mobile-2.txt": _PROJECT_ROOT / "sources" / "ru_lte" / "Vless-Reality-White-Lists-Rus-Mobile-2.txt",
        "WHITE-CIDR-RU-checked.txt": _PROJECT_ROOT / "sources" / "ru_lte" / "WHITE-CIDR-RU-checked.txt",
        "WHITE-CIDR-RU-all.txt": _PROJECT_ROOT / "sources" / "ru_lte" / "WHITE-CIDR-RU-all.txt",
        "BLACK_VLESS_RUS_mobile.txt": _PROJECT_ROOT / "sources" / "black" / "BLACK_VLESS_RUS_mobile.txt",
        "BLACK_VLESS_RUS.txt": _PROJECT_ROOT / "sources" / "black" / "BLACK_VLESS_RUS.txt",
    }


def _canonical_source_name(source: str) -> str:
    raw = str(source or "").strip()
    return Path(urlsplit(raw).path or raw).name


def _canonical_remote_source_url(source: str) -> Optional[str]:
    raw = str(source or "").strip()
    if not raw:
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    return _remote_source_aliases().get(_canonical_source_name(raw))


def _fresh_source_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return raw
    parts = list(urlsplit(raw))
    query_items = parse_qsl(parts[3], keep_blank_values=True)
    query_items = [(key, value) for key, value in query_items if key != "_ts"]
    query_items.append(("_ts", str(int(time.time()))))
    parts[3] = urlencode(query_items)
    return urlunsplit(parts)


def _resolve_local_source_path(source: str) -> Path:
    raw = str(source or "").strip()
    path = Path(raw)
    if path.is_absolute():
        return path
    if path.exists():
        return path.resolve()
    return (_PROJECT_ROOT / path).resolve()


def _read_text_from_source(source: str) -> str:
    raw = str(source or "").strip()
    if not raw:
        return ""
    fallback_path = _local_source_fallbacks().get(_canonical_source_name(raw))
    remote_url = _canonical_remote_source_url(raw)
    request_headers = {
        "Accept": "text/plain, text/html;q=0.9, */*;q=0.8",
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
        "User-Agent": "inet-vpn-refresh/1.0",
    }

    if remote_url:
        try:
            response = requests.get(_fresh_source_url(remote_url), timeout=20, headers=request_headers)
            response.raise_for_status()
            return response.text
        except Exception:
            if raw.startswith(("http://", "https://")) and fallback_path and fallback_path.is_file():
                return fallback_path.read_text(encoding="utf-8")
            if not raw.startswith(("http://", "https://")):
                path = _resolve_local_source_path(raw)
                if path.is_file():
                    return path.read_text(encoding="utf-8")
                if fallback_path and fallback_path.is_file():
                    return fallback_path.read_text(encoding="utf-8")
            raise

    path = _resolve_local_source_path(raw)
    if path.is_file():
        return path.read_text(encoding="utf-8")
    if fallback_path and fallback_path.is_file():
        return fallback_path.read_text(encoding="utf-8")
    raise FileNotFoundError(f"Source file not found: {raw}")


def _decode_vless_name(fragment: str) -> str:
    return unquote(fragment or "").strip()


def _parse_vless_subscription_line(line: str) -> Optional[Dict[str, Any]]:
    raw = str(line or "").strip()
    if not raw.startswith("vless://"):
        return None
    base, _, fragment = raw.partition("#")
    parsed = urlsplit(base)
    if parsed.scheme.lower() != "vless":
        return None
    uuid = unquote(parsed.username or "").strip()
    server = (parsed.hostname or "").strip()
    try:
        port = int(parsed.port or 0)
    except (TypeError, ValueError):
        port = 0
    if not uuid or not server or port <= 0:
        return None
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    transport = (query.get("type") or query.get("transport") or query.get("network") or "tcp").strip() or "tcp"
    payload: Dict[str, Any] = {
        "engine": "xray",
        "protocol": "vless",
        "server": server,
        "port": port,
        "uuid": uuid,
        "transport": transport,
        "network": transport,
        "security": (query.get("security") or "reality").strip() or "reality",
        "flow": (query.get("flow") or "").strip(),
        "sni": (query.get("sni") or query.get("serverName") or query.get("host") or "").strip(),
        "server_name": (query.get("sni") or query.get("serverName") or query.get("host") or "").strip(),
        "public_key": (query.get("pbk") or query.get("public_key") or query.get("publicKey") or "").strip(),
        "short_id": (query.get("sid") or query.get("short_id") or query.get("shortId") or "").strip(),
        "fingerprint": (query.get("fp") or query.get("fingerprint") or "chrome").strip() or "chrome",
        "service_name": (query.get("serviceName") or query.get("service_name") or "").strip(),
        "path": (query.get("path") or "").strip(),
        "spider_x": (query.get("spx") or query.get("spiderX") or query.get("spider_x") or "").strip(),
        "packet_encoding": (query.get("packetEncoding") or query.get("packet_encoding") or query.get("packet-encoding") or "xudp").strip() or "xudp",
        "remark": _decode_vless_name(fragment),
        "dns_servers": ["1.1.1.1", "8.8.8.8"],
        "connect_mode": (query.get("connectMode") or query.get("connect_mode") or "tun").strip() or "tun",
        "full_tunnel": str(query.get("fullTunnel") or query.get("full_tunnel") or "1").strip().lower() not in {"0", "false", "no", "off"},
    }
    if query.get("mode"):
        payload["mode"] = query.get("mode")
    if query.get("host"):
        payload["host"] = query.get("host")
    if query.get("alpn"):
        payload["alpn"] = [item.strip() for item in str(query.get("alpn") or "").split(",") if item.strip()]
    return _normalize_vpn_payload_keys(payload)


def _transport_allowed(payload: Dict[str, Any], allowed_values: List[str]) -> bool:
    allowed = {item.strip().lower() for item in (allowed_values or []) if str(item or "").strip()}
    transport = str(payload.get("transport") or payload.get("network") or "tcp").strip().lower()
    return transport in allowed if allowed else True


def _payload_transport(payload: Dict[str, Any]) -> str:
    return str(payload.get("transport") or payload.get("network") or "tcp").strip().lower() or "tcp"


def _payload_security(payload: Dict[str, Any]) -> str:
    return str(payload.get("security") or "").strip().lower()


def _xray_reality_spider_x(payload: Dict[str, Any], transport: str) -> Optional[str]:
    explicit = str(payload.get("spider_x") or payload.get("spiderX") or payload.get("spx") or "").strip()
    if explicit:
        return explicit if explicit.startswith("/") else f"/{explicit.lstrip('/')}"
    # For grpc/http transports, path/serviceName metadata must not be copied into spiderX.
    if transport in {"grpc", "xhttp", "httpupgrade", "ws", "websocket"}:
        return "/"
    fallback = str(payload.get("path") or "/").strip() or "/"
    return fallback if fallback.startswith("/") else f"/{fallback.lstrip('/')}"


def _probe_runtime_uuid(payload: Dict[str, Any]) -> str:
    candidates: List[Any] = [
        payload.get("uuid"),
        payload.get("probe_uuid"),
        payload.get("live_probe_uuid"),
        payload.get("probeUuid"),
        payload.get("liveProbeUuid"),
    ]
    nested_probe = payload.get("_probe")
    if isinstance(nested_probe, dict):
        candidates.extend([
            nested_probe.get("uuid"),
            nested_probe.get("probe_uuid"),
            nested_probe.get("live_probe_uuid"),
        ])
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _payload_probe_ready(payload: Dict[str, Any]) -> bool:
    return bool(_probe_runtime_uuid(payload))


def _payload_for_real_probe(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload or {})
    runtime_uuid = _probe_runtime_uuid(normalized)
    if runtime_uuid:
        normalized["uuid"] = runtime_uuid
    return normalized


def _candidate_identity_key(payload: Dict[str, Any]) -> str:
    return "|".join([
        str(payload.get("server") or "").strip().lower(),
        str(payload.get("port") or "").strip(),
        str(payload.get("uuid") or "").strip().lower(),
        str(payload.get("public_key") or payload.get("publicKey") or "").strip(),
        str(payload.get("short_id") or payload.get("shortId") or "").strip(),
        str(payload.get("server_name") or payload.get("sni") or "").strip().lower(),
        str(payload.get("path") or "").strip(),
        str(payload.get("service_name") or "").strip(),
    ])


def _candidate_server_key(payload: Dict[str, Any]) -> str:
    return "|".join([
        str(payload.get("server") or "").strip().lower(),
        str(payload.get("port") or "").strip(),
    ])


def _candidate_sni_key(payload: Dict[str, Any]) -> str:
    return str(payload.get("server_name") or payload.get("sni") or "").strip().lower()


def _dead_candidate_cooldown_seconds(pool: str) -> float:
    if pool == "black":
        return max(5, int(settings.BLACK_DEAD_COOLDOWN_MINUTES or 30)) * 60.0
    return max(5, int(settings.RU_LTE_DEAD_COOLDOWN_MINUTES or 45)) * 60.0


def _cleanup_dead_candidate_cache(pool: str) -> None:
    now = time.time()
    with _DEAD_CANDIDATE_LOCK:
        bucket = _DEAD_CANDIDATE_CACHE.setdefault(pool, {})
        expired = [key for key, expires_at in bucket.items() if expires_at <= now]
        for key in expired:
            bucket.pop(key, None)


def _is_candidate_recently_dead(pool: str, payload: Dict[str, Any]) -> bool:
    _cleanup_dead_candidate_cache(pool)
    identity = _candidate_identity_key(payload)
    server_key = _candidate_server_key(payload)
    with _DEAD_CANDIDATE_LOCK:
        bucket = _DEAD_CANDIDATE_CACHE.setdefault(pool, {})
        return bool(bucket.get(identity) or bucket.get(f"server:{server_key}"))


def _mark_candidate_dead(pool: str, payload: Dict[str, Any]) -> None:
    identity = _candidate_identity_key(payload)
    server_key = _candidate_server_key(payload)
    expires_at = time.time() + _dead_candidate_cooldown_seconds(pool)
    with _DEAD_CANDIDATE_LOCK:
        bucket = _DEAD_CANDIDATE_CACHE.setdefault(pool, {})
        if identity:
            bucket[identity] = expires_at
        if server_key:
            bucket[f"server:{server_key}"] = expires_at


def _candidate_quality_reasons(payload: Dict[str, Any], *, pool: str) -> List[str]:
    reasons: List[str] = []
    probe_payload = _payload_for_real_probe(payload)
    protocol = str(probe_payload.get("protocol") or "vless").strip().lower()
    transport = _payload_transport(probe_payload)
    security = _payload_security(probe_payload)
    server_name = str(probe_payload.get("server_name") or probe_payload.get("sni") or "").strip()
    public_key = str(probe_payload.get("public_key") or probe_payload.get("publicKey") or "").strip()
    short_id = str(probe_payload.get("short_id") or probe_payload.get("shortId") or "").strip()
    flow = str(probe_payload.get("flow") or "").strip().lower()
    service_name = str(probe_payload.get("service_name") or "").strip()
    path = str(probe_payload.get("path") or "").strip()

    if protocol != "vless":
        reasons.append("protocol")
    if not str(probe_payload.get("server") or "").strip():
        reasons.append("server")
    if int(probe_payload.get("port") or 0) <= 0:
        reasons.append("port")
    if not _payload_probe_ready(probe_payload):
        reasons.append("uuid")

    allowed = settings.BLACK_ALLOWED_TRANSPORTS if pool == "black" else settings.RU_LTE_ALLOWED_TRANSPORTS
    if not _transport_allowed(payload, allowed or []):
        reasons.append("transport_not_allowed")

    if pool == "ru_lte" and security != "reality":
        reasons.append("security_not_reality")
    if pool == "black" and security not in {"reality", "tls"}:
        reasons.append("security_unsupported")

    if security in {"reality", "tls"} and not server_name:
        reasons.append("missing_sni")
    if security == "reality" and not public_key:
        reasons.append("missing_public_key")
    if security == "reality" and transport == "tcp" and flow != "xtls-rprx-vision":
        reasons.append("tcp_missing_vision_flow")
    if transport == "grpc" and not service_name:
        reasons.append("grpc_missing_service_name")
    if transport in {"ws", "websocket"} and not path:
        reasons.append("ws_missing_path")
    return reasons


def _is_candidate_strong(payload: Dict[str, Any], *, pool: str) -> bool:
    return not _candidate_quality_reasons(payload, pool=pool)


def _candidate_sort_key(item: Dict[str, Any]) -> Tuple[int, int, str]:
    return (int(item.get("_score") or 0), int(item.get("_source_priority") or 0), str(item.get("remark") or ""))


def _select_diverse_candidates(candidates: List[Dict[str, Any]], *, max_candidates: int) -> List[Dict[str, Any]]:
    chosen: List[Dict[str, Any]] = []
    used_servers: set[str] = set()
    used_sni: set[str] = set()

    for candidate in candidates:
        server_key = _candidate_server_key(candidate)
        sni_key = _candidate_sni_key(candidate)
        if server_key and server_key in used_servers:
            continue
        if sni_key and sni_key in used_sni:
            continue
        chosen.append(candidate)
        if server_key:
            used_servers.add(server_key)
        if sni_key:
            used_sni.add(sni_key)
        if len(chosen) >= max_candidates:
            return chosen

    for candidate in candidates:
        identity = _candidate_identity_key(candidate)
        if any(_candidate_identity_key(item) == identity for item in chosen):
            continue
        chosen.append(candidate)
        if len(chosen) >= max_candidates:
            return chosen
    return chosen


def _probe_candidate_with_timeout(payload: Dict[str, Any], timeout_seconds: float) -> Dict[str, Any]:
    server = str(payload.get("server") or "").strip()
    port = int(payload.get("port") or 0)
    if not server or port <= 0:
        return {"ok": False, "latency_ms": None, "error": "missing_server_or_port"}

    started = time.perf_counter()
    try:
        addr_infos = socket.getaddrinfo(server, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        return {"ok": False, "latency_ms": None, "error": f"dns:{exc}"}

    timeout = max(1.0, float(timeout_seconds or 4))
    last_error = None
    for family, socktype, proto, _canonname, sockaddr in addr_infos[:4]:
        sock = None
        try:
            sock = socket.socket(family, socktype, proto)
            sock.settimeout(timeout)
            sock.connect(sockaddr)
            latency_ms = int((time.perf_counter() - started) * 1000)
            return {"ok": True, "latency_ms": latency_ms, "error": None}
        except OSError as exc:
            last_error = str(exc)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
    return {"ok": False, "latency_ms": None, "error": last_error or "connect_failed"}


def _probe_binary_status() -> Dict[str, Any]:
    runner_name, runner_bin = _resolve_probe_runner()
    xray_raw = str(getattr(settings, "VPN_REAL_PROBE_XRAY_BIN", "xray") or "xray").strip() or "xray"
    singbox_raw = str(getattr(settings, "VPN_REAL_PROBE_SINGBOX_BIN", "sing-box") or "sing-box").strip() or "sing-box"
    xray_path = shutil.which(xray_raw) or (xray_raw if xray_raw.startswith("/") and Path(xray_raw).is_file() else None)
    singbox_path = shutil.which(singbox_raw) or (singbox_raw if singbox_raw.startswith("/") and Path(singbox_raw).is_file() else None)
    resolved_path = runner_bin if runner_bin and Path(runner_bin).is_file() else None

    version = ""
    if resolved_path:
        try:
            output = subprocess.check_output([resolved_path, "version"], stderr=subprocess.STDOUT, timeout=8)
            version = (output.decode(errors="ignore").splitlines() or [""])[0].strip()
        except Exception as exc:
            version = f"version_check_failed:{exc}"

    return {
        "runner": runner_name,
        "resolved_path": resolved_path,
        "version": version,
        "xray_path": xray_path,
        "singbox_path": singbox_path,
    }


def _log_probe_binary_status() -> None:
    info = _probe_binary_status()
    logging.info(
        "[vpn][probe-runtime] runner=%s resolved=%s xray=%s singbox=%s version=%s",
        info.get("runner") or "-",
        info.get("resolved_path") or "missing",
        info.get("xray_path") or "missing",
        info.get("singbox_path") or "missing",
        info.get("version") or "-",
    )


def _resolve_probe_runner() -> Tuple[str, Optional[str]]:
    runner = str(getattr(settings, "VPN_REAL_PROBE_RUNNER", "auto") or "auto").strip().lower() or "auto"
    if runner not in {"xray", "sing-box", "singbox", "auto"}:
        runner = "auto"

    def _resolve_path(raw_value: str, default_name: str) -> Optional[str]:
        raw = str(raw_value or default_name).strip() or default_name
        if raw.startswith("/"):
            probe_path = Path(raw)
            return str(probe_path) if probe_path.is_file() else None
        return shutil.which(raw)

    xray_bin = _resolve_path(getattr(settings, "VPN_REAL_PROBE_XRAY_BIN", "xray"), "xray")
    singbox_bin = _resolve_path(getattr(settings, "VPN_REAL_PROBE_SINGBOX_BIN", "sing-box"), "sing-box")

    if runner == "auto":
        if xray_bin:
            return ("xray", xray_bin)
        if singbox_bin:
            return ("singbox", singbox_bin)
        return ("xray", None)
    if runner in {"sing-box", "singbox"}:
        return ("singbox", singbox_bin)
    return ("xray", xray_bin)


def _pick_free_local_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def _wait_for_local_socks_port(port: int, timeout_sec: float) -> bool:
    deadline = time.time() + max(1.0, timeout_sec)
    while time.time() < deadline:
        sock = None
        try:
            sock = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            return True
        except OSError:
            time.sleep(0.1)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
    return False


def _build_xray_ru_lte_probe_config(payload: Dict[str, Any], socks_port: int) -> Dict[str, Any]:
    transport = _payload_transport(payload)
    security = _payload_security(payload)
    server_name = str(payload.get("server_name") or payload.get("sni") or payload.get("host") or "").strip()
    user: Dict[str, Any] = {
        "id": _probe_runtime_uuid(payload),
        "encryption": str(payload.get("encryption") or "none").strip() or "none",
    }
    flow = str(payload.get("flow") or "").strip()
    if flow:
        user["flow"] = flow

    stream_settings: Dict[str, Any] = {
        "network": transport if transport not in {"websocket"} else "ws",
    }
    if security == "reality":
        stream_settings["security"] = "reality"
        stream_settings["realitySettings"] = {
            "show": False,
            "serverName": server_name,
            "fingerprint": str(payload.get("fingerprint") or "chrome").strip() or "chrome",
            "publicKey": str(payload.get("public_key") or payload.get("publicKey") or "").strip(),
            "shortId": str(payload.get("short_id") or payload.get("shortId") or "").strip(),
            "spiderX": _xray_reality_spider_x(payload, transport) or "/",
        }
    elif security == "tls":
        stream_settings["security"] = "tls"
        tls_settings: Dict[str, Any] = {
            "serverName": server_name,
            "allowInsecure": bool(payload.get("allow_insecure") or payload.get("allowInsecure") or False),
        }
        alpn = [str(item or "").strip() for item in (payload.get("alpn") or []) if str(item or "").strip()]
        if alpn:
            tls_settings["alpn"] = alpn
        stream_settings["tlsSettings"] = tls_settings
    else:
        stream_settings["security"] = "none"

    if transport == "grpc":
        stream_settings["grpcSettings"] = {
            "serviceName": str(payload.get("service_name") or payload.get("serviceName") or "").strip(),
            "multiMode": False,
        }
    elif transport in {"ws", "websocket"}:
        ws_settings: Dict[str, Any] = {
            "path": str(payload.get("path") or "/").strip() or "/",
        }
        host = str(payload.get("host") or server_name or "").strip()
        if host:
            ws_settings["headers"] = {"Host": host}
        stream_settings["wsSettings"] = ws_settings
    elif transport == "xhttp":
        xhttp_settings: Dict[str, Any] = {
            "path": str(payload.get("path") or "/").strip() or "/",
            "mode": str(payload.get("mode") or "auto").strip() or "auto",
        }
        host = str(payload.get("host") or server_name or "").strip()
        if host:
            xhttp_settings["host"] = host
        extra = payload.get("xhttp_settings")
        if isinstance(extra, dict):
            for key, value in extra.items():
                if key not in xhttp_settings and value not in (None, "", [], {}):
                    xhttp_settings[key] = value
        stream_settings["xhttpSettings"] = xhttp_settings

    dns_servers = [str(item or "").strip() for item in (payload.get("dns_servers") or ["1.1.1.1", "8.8.8.8"]) if str(item or "").strip()]
    if not dns_servers:
        dns_servers = ["1.1.1.1", "8.8.8.8"]

    return {
        "log": {"loglevel": "warning"},
        "dns": {
            "servers": dns_servers,
            "queryStrategy": "UseIPv4",
        },
        "inbounds": [
            {
                "listen": "127.0.0.1",
                "port": int(socks_port),
                "protocol": "socks",
                "settings": {
                    "auth": "noauth",
                    "udp": True,
                },
                "sniffing": {
                    "enabled": False,
                },
            }
        ],
        "outbounds": [
            {
                "tag": "proxy",
                "protocol": str(payload.get("protocol") or "vless").strip() or "vless",
                "settings": {
                    "vnext": [
                        {
                            "address": str(payload.get("server") or "").strip(),
                            "port": int(payload.get("port") or 0),
                            "users": [user],
                        }
                    ]
                },
                "streamSettings": stream_settings,
            }
        ],
        "routing": {
            "domainStrategy": "AsIs",
        },
    }


def _build_singbox_ru_lte_probe_config(payload: Dict[str, Any], socks_port: int) -> Dict[str, Any]:
    transport = _payload_transport(payload)
    security = _payload_security(payload)
    server_name = str(payload.get("server_name") or payload.get("sni") or "").strip()
    outbound: Dict[str, Any] = {
        "type": str(payload.get("protocol") or "vless").strip() or "vless",
        "tag": "proxy",
        "server": str(payload.get("server") or "").strip(),
        "server_port": int(payload.get("port") or 0),
        "uuid": _probe_runtime_uuid(payload),
        "packet_encoding": str(payload.get("packet_encoding") or payload.get("packetEncoding") or "xudp").strip() or "xudp",
    }
    if security in {"tls", "reality"}:
        outbound["tls"] = {
            "enabled": True,
            "server_name": server_name,
            "utls": {
                "enabled": True,
                "fingerprint": str(payload.get("fingerprint") or "chrome").strip() or "chrome",
            },
        }
        if security == "reality":
            outbound["tls"]["reality"] = {
                "enabled": True,
                "public_key": str(payload.get("public_key") or payload.get("publicKey") or "").strip(),
                "short_id": str(payload.get("short_id") or payload.get("shortId") or "").strip(),
            }
        alpn = [str(item or "").strip() for item in (payload.get("alpn") or []) if str(item or "").strip()]
        if alpn:
            outbound["tls"]["alpn"] = alpn
        if security == "tls":
            outbound["tls"]["insecure"] = bool(payload.get("allow_insecure") or payload.get("allowInsecure") or False)
    flow = str(payload.get("flow") or "").strip()
    if flow:
        outbound["flow"] = flow
    if transport == "grpc":
        outbound["transport"] = {
            "type": "grpc",
            "service_name": str(payload.get("service_name") or payload.get("serviceName") or "").strip(),
        }
    elif transport in {"ws", "websocket"}:
        transport_payload: Dict[str, Any] = {
            "type": "ws",
            "path": str(payload.get("path") or "/").strip() or "/",
        }
        host = str(payload.get("host") or "").strip()
        if host:
            transport_payload["headers"] = {"Host": host}
        outbound["transport"] = transport_payload
    elif transport == "xhttp":
        transport_payload = {
            "type": "httpupgrade",
            "path": str(payload.get("path") or "/").strip() or "/",
            "host": [str(payload.get("host") or server_name or "").strip()] if str(payload.get("host") or server_name or "").strip() else [],
        }
        outbound["transport"] = transport_payload

    dns_remote_addr = str((payload.get("dns_servers") or ["1.1.1.1"])[0] or "1.1.1.1").strip() or "1.1.1.1"
    dns_local_addr = str((payload.get("dns_servers") or ["1.1.1.1", "8.8.8.8"])[-1] or "8.8.8.8").strip() or "8.8.8.8"
    return {
        "log": {"level": "error"},
        "dns": {
            "servers": [
                {"tag": "dns-remote", "address": dns_remote_addr, "detour": "proxy"},
                {"tag": "dns-direct", "address": dns_local_addr},
            ],
            "rules": [
                {"outbound": ["proxy"], "server": "dns-remote"},
            ],
            "final": "dns-remote",
            "strategy": "ipv4_only",
        },
        "inbounds": [
            {
                "type": "socks",
                "tag": "local-socks",
                "listen": "127.0.0.1",
                "listen_port": int(socks_port),
            }
        ],
        "outbounds": [
            outbound,
            {"type": "direct", "tag": "direct"},
            {"type": "block", "tag": "block"},
        ],
        "route": {
            "auto_detect_interface": True,
            "final": "proxy",
        },
    }


def _run_curl_through_socks(url: str, socks_port: int) -> Dict[str, Any]:
    connect_timeout = max(2, int(getattr(settings, "VPN_REAL_PROBE_CONNECT_TIMEOUT_SEC", 6) or 6))
    max_time = max(connect_timeout + 1, int(getattr(settings, "VPN_REAL_PROBE_MAX_TIME_SEC", 12) or 12))
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--location",
        "--output",
        "/dev/null",
        "--write-out",
        "%{http_code} %{time_connect} %{time_starttransfer} %{time_total}",
        "--socks5-hostname",
        f"127.0.0.1:{int(socks_port)}",
        "--connect-timeout",
        str(connect_timeout),
        "--max-time",
        str(max_time),
        url,
    ]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=max_time + 2)
    if completed.returncode != 0:
        return {"ok": False, "error": (completed.stderr or completed.stdout or f"curl_exit_{completed.returncode}").strip()}
    parts = str(completed.stdout or "").strip().split()
    if len(parts) != 4:
        return {"ok": False, "error": "curl_bad_metrics"}
    http_code, _time_connect, _time_ttfb, time_total = parts
    try:
        latency_ms = int(float(time_total) * 1000)
    except (TypeError, ValueError):
        latency_ms = None
    if str(http_code) == "000":
        return {"ok": False, "error": "http_000", "latency_ms": latency_ms}
    return {"ok": True, "latency_ms": latency_ms, "http_code": http_code}


def _probe_target_label(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "probe"
    try:
        host = urlsplit(raw).hostname or raw
    except Exception:
        host = raw
    if "youtube" in host:
        return "youtube"
    if "instagram" in host:
        return "instagram"
    if "telegram" in host:
        return "telegram"
    if host.endswith("vk.com") or host == "vk.com":
        return "vk"
    if host.endswith("ya.ru") or host == "ya.ru" or host.endswith("yandex.ru"):
        return "ya"
    return host


def _append_probe_url(targets: List[Tuple[str, str]], seen: set[str], value: Any) -> None:
    text = str(value or "").strip()
    if not text:
        return
    if text.startswith(("http://", "https://")):
        url = text
    else:
        host = text.split("/")[0].strip()
        if not host or ":" in host or " " in host:
            return
        url = f"https://{host}/"
    if url in seen:
        return
    seen.add(url)
    targets.append((_probe_target_label(url), url))


def _pool_probe_urls(pool: str) -> List[str]:
    if pool == "ru_lte":
        urls = list(getattr(settings, "RU_LTE_REAL_PROBE_URLS", []) or [])
        if urls:
            return [str(item or "").strip() for item in urls if str(item or "").strip()]
        fallback_urls = list(getattr(settings, "VPN_REAL_PROBE_RU_EXTRA_URLS", []) or [])
        return [str(item or "").strip() for item in fallback_urls if str(item or "").strip()]
    if pool == "black":
        urls = list(getattr(settings, "VPN_REAL_PROBE_URLS", []) or [])
        return [str(item or "").strip() for item in urls if str(item or "").strip()]
    urls = list(getattr(settings, "VPN_REAL_PROBE_URLS", []) or [])
    return [str(item or "").strip() for item in urls if str(item or "").strip()]


def _pool_real_probe_required(pool: str) -> bool:
    if pool == "ru_lte":
        return bool(getattr(settings, "RU_LTE_REAL_PROBE_REQUIRED", True))
    if pool == "black":
        return bool(getattr(settings, "BLACK_REAL_PROBE_REQUIRED", True))
    return bool(getattr(settings, "VPN_REAL_PROBE_REQUIRED", False))


def _pool_probe_min_success(pool: str, total_targets: int) -> int:
    total = max(1, int(total_targets or 0))
    if pool == "ru_lte":
        raw = int(getattr(settings, "RU_LTE_REAL_PROBE_MIN_SUCCESS", 1) or 1)
    elif pool == "black":
        raw = int(getattr(settings, "BLACK_REAL_PROBE_MIN_SUCCESS", getattr(settings, "VPN_REAL_PROBE_MIN_SUCCESS", 2)) or 1)
    else:
        raw = int(getattr(settings, "VPN_REAL_PROBE_MIN_SUCCESS", 2) or 1)
        # Generic internet checks often include 3 public targets (YouTube/Telegram/Instagram).
        # Keep them resilient to one flaky endpoint instead of requiring a perfect 3/3 pass.
        if total >= 3:
            raw = min(raw, 2)
    return min(max(1, raw), total)


def _candidate_probe_targets(payload: Dict[str, Any], *, pool: str = "generic") -> List[Tuple[str, str]]:
    targets: List[Tuple[str, str]] = []
    seen: set[str] = set()

    pool_urls = _pool_probe_urls(pool)

    # RU LTE is a special white-list/mobile pool. Probe only explicit RU/mobile
    # targets instead of requiring global services like YouTube/Instagram.
    if pool == "ru_lte":
        for item in pool_urls:
            _append_probe_url(targets, seen, item)
        return targets

    # For regular VPN live checks prefer only the explicit probe URL pool
    # (for example YouTube / Telegram / Instagram). Do not probe the node host,
    # SNI/server_name, or payload host here: for REALITY/gRPC these values are
    # transport metadata and are not reliable health-check targets.
    for item in pool_urls:
        _append_probe_url(targets, seen, item)
    if targets:
        return targets

    # Safety fallback only when the explicit pool is empty/misconfigured.
    for value in (
        payload.get("host"),
        payload.get("server_name") or payload.get("sni"),
        payload.get("server"),
    ):
        _append_probe_url(targets, seen, value)
    return targets


def _probe_candidate_via_real_tunnel(payload: Dict[str, Any], *, pool: str = "generic") -> Dict[str, Any]:
    probe_payload = _payload_for_real_probe(payload)
    if not _payload_probe_ready(probe_payload):
        return {"ok": False, "latency_ms": None, "error": "probe_uuid_missing", "method": "probe_setup"}

    runner_name, runner_bin = _resolve_probe_runner()
    transport = _payload_transport(probe_payload)
    if transport == "xhttp" and runner_name == "singbox":
        xray_bin = shutil.which(str(getattr(settings, "VPN_REAL_PROBE_XRAY_BIN", "xray") or "xray").strip() or "xray")
        if xray_bin:
            runner_name, runner_bin = "xray", xray_bin
    if not runner_bin:
        return {"ok": False, "latency_ms": None, "error": f"{runner_name}_binary_missing", "method": f"{runner_name}_real"}

    targets = _candidate_probe_targets(probe_payload, pool=pool)
    if not targets:
        return {"ok": False, "latency_ms": None, "error": "probe_urls_missing", "method": f"{runner_name}_real"}

    required_successes = _pool_probe_min_success(pool, len(targets))
    socks_port = _pick_free_local_port()
    warmup_ms = max(0, int(getattr(settings, "VPN_REAL_PROBE_WARMUP_MS", 1200) or 1200))
    startup_timeout = max(2.0, float(getattr(settings, "VPN_REAL_PROBE_CONNECT_TIMEOUT_SEC", 6) or 6))
    process: Optional[subprocess.Popen[str]] = None
    stdout_tail = ""
    successes: List[Tuple[str, str, Optional[int]]] = []
    failures: List[str] = []
    with tempfile.TemporaryDirectory(prefix=f"inet_{pool}_probe_") as temp_dir:
        config_path = Path(temp_dir) / "config.json"
        if runner_name == "singbox":
            config_payload = _build_singbox_ru_lte_probe_config(probe_payload, socks_port)
            command = [runner_bin, "run", "-c", str(config_path)]
        else:
            config_payload = _build_xray_ru_lte_probe_config(probe_payload, socks_port)
            command = [runner_bin, "-config", str(config_path)]
        config_path.write_text(json.dumps(config_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            if not _wait_for_local_socks_port(socks_port, startup_timeout):
                if process.poll() is not None:
                    stdout_tail = (process.stdout.read() if process.stdout else "").strip()[-700:]
                    return {"ok": False, "latency_ms": None, "error": f"{runner_name}_exit:{process.returncode}:{stdout_tail or 'startup_failed'}", "method": f"{runner_name}_real"}
                return {"ok": False, "latency_ms": None, "error": "socks_not_ready", "method": f"{runner_name}_real"}
            if warmup_ms > 0:
                time.sleep(warmup_ms / 1000.0)
            for label, url in targets:
                result = _run_curl_through_socks(url, socks_port)
                if result.get("ok"):
                    successes.append((label, url, result.get("latency_ms")))
                    if len({item[0] for item in successes}) >= required_successes:
                        latencies = [int(item[2]) for item in successes if item[2] is not None]
                        latency_ms = int(sum(latencies) / len(latencies)) if latencies else None
                        primary_url = successes[0][1] if successes else None
                        return {
                            "ok": True,
                            "latency_ms": latency_ms,
                            "method": f"{runner_name}_real",
                            "probe_url": primary_url,
                            "probe_labels_ok": [item[0] for item in successes],
                            "probe_urls_ok": [item[1] for item in successes],
                        }
                else:
                    failures.append(f"{label}:{result.get('error') or 'probe_failed'}")
            return {
                "ok": False,
                "latency_ms": None,
                "error": f"real_probe_not_enough_success:{len({item[0] for item in successes})}/{required_successes}"
                         + (f"; {' | '.join(failures[:4])}" if failures else ""),
                "method": f"{runner_name}_real",
                "probe_labels_ok": [item[0] for item in successes],
                "probe_urls_ok": [item[1] for item in successes],
            }
        except subprocess.TimeoutExpired:
            return {"ok": False, "latency_ms": None, "error": "real_probe_timeout", "method": f"{runner_name}_real"}
        except Exception as exc:
            return {"ok": False, "latency_ms": None, "error": f"real_probe_exception:{exc}", "method": f"{runner_name}_real"}
        finally:
            if process is not None:
                try:
                    process.terminate()
                    process.wait(timeout=2)
                except Exception:
                    try:
                        process.kill()
                    except Exception:
                        pass
                    try:
                        process.wait(timeout=1)
                    except Exception:
                        pass


def _ru_lte_transport_bonus(payload: Dict[str, Any]) -> int:
    transport = str(payload.get("transport") or payload.get("network") or "tcp").strip().lower()
    if transport == "xhttp":
        return 60
    if transport == "grpc":
        return 50
    if transport in {"ws", "websocket"}:
        return 40
    if transport == "tcp":
        return -25
    return -50


def _ru_lte_source_priority(source: str) -> int:
    raw = str(source or "").strip().lower()
    if raw.endswith("/vless-reality-white-lists-rus-mobile.txt") or raw.endswith("vless-reality-white-lists-rus-mobile.txt"):
        return 40
    if raw.endswith("/vless-reality-white-lists-rus-mobile-2.txt") or raw.endswith("vless-reality-white-lists-rus-mobile-2.txt"):
        return 30
    if raw.endswith("/white-cidr-ru-checked.txt") or raw.endswith("white-cidr-ru-checked.txt"):
        return 20
    if raw.endswith("/white-cidr-ru-all.txt") or raw.endswith("white-cidr-ru-all.txt"):
        return 10
    if raw.endswith("/white-sni-ru-all.txt") or raw.endswith("white-sni-ru-all.txt"):
        return 0
    return 0


def _ru_lte_transport_allowed(payload: Dict[str, Any]) -> bool:
    allowed = {item.strip().lower() for item in (settings.RU_LTE_ALLOWED_TRANSPORTS or ["grpc", "tcp", "ws", "xhttp"]) if str(item or "").strip()}
    transport = str(payload.get("transport") or payload.get("network") or "tcp").strip().lower()
    return transport in allowed


def _black_source_priority(source: str) -> int:
    raw = str(source or "").strip().lower()
    if raw.endswith('/black_vless_rus_mobile.txt') or raw.endswith('black_vless_rus_mobile.txt'):
        return 40
    if raw.endswith('/black_vless_rus.txt') or raw.endswith('black_vless_rus.txt'):
        return 30
    if raw.endswith('/black_ss+all_rus.txt') or raw.endswith('black_ss+all_rus.txt'):
        return 10
    return 0


def _black_transport_allowed(payload: Dict[str, Any]) -> bool:
    allowed = {item.strip().lower() for item in (settings.BLACK_ALLOWED_TRANSPORTS or ["grpc", "tcp", "ws", "xhttp"]) if str(item or "").strip()}
    transport = str(payload.get("transport") or payload.get("network") or "tcp").strip().lower()
    return transport in allowed if allowed else True


def _generic_probe_candidate(payload: Dict[str, Any], *, pool: str, tcp_timeout: float) -> Dict[str, Any]:
    require_real_probe = _pool_real_probe_required(pool)
    allow_tcp_fallback_on_runner_error = bool(getattr(settings, "VPN_REAL_PROBE_ALLOW_TCP_FALLBACK_ON_RUNNER_ERROR", False))
    real_probe_error = ""
    if bool(getattr(settings, "VPN_REAL_PROBE_ENABLED", True)):
        real_probe = _probe_candidate_via_real_tunnel(payload, pool=pool)
        if real_probe.get("ok"):
            return real_probe
        real_probe_error = str(real_probe.get("error") or "").strip().lower()
        if require_real_probe:
            runner_error_prefixes = (
                "xray_binary_missing",
                "singbox_binary_missing",
                "socks_not_ready",
                "real_probe_timeout",
            )
            if allow_tcp_fallback_on_runner_error and real_probe_error.startswith(runner_error_prefixes):
                logger.warning("[vpn][probe] pool=%s strict_real_probe=1 runner_error=%s tcp_fallback_allowed=1", pool, real_probe_error or "-")
            else:
                return real_probe
    elif require_real_probe:
        return {"ok": False, "latency_ms": None, "error": "real_probe_disabled", "method": "real_probe_disabled"}

    fallback_probe = _probe_candidate_with_timeout(payload, float(tcp_timeout or 4))
    fallback_probe["method"] = "tcp_fallback"
    if real_probe_error:
        fallback_probe["real_probe_error"] = real_probe_error
    return fallback_probe


def _black_probe_candidate(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _generic_probe_candidate(payload, pool="black", tcp_timeout=float(settings.BLACK_CONNECT_TIMEOUT_SEC or 4))


def _ru_lte_probe_candidate(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _generic_probe_candidate(payload, pool="ru_lte", tcp_timeout=float(settings.RU_LTE_CONNECT_TIMEOUT_SEC or 4))

def _ru_lte_payload_score(payload: Dict[str, Any]) -> int:
    score = 0
    transport = _payload_transport(payload)
    security = _payload_security(payload)
    score += _ru_lte_transport_bonus(payload)
    if security == "reality":
        score += 35
    elif security == "tls":
        score += 10
    if str(payload.get("flow") or "").strip().lower() == "xtls-rprx-vision":
        score += 20
    elif transport == "tcp":
        score -= 35
    remark = str(payload.get("remark") or "").lower()
    if "cidr" in remark:
        score += 20
    if "mobile" in remark or "lte" in remark:
        score += 15
    if payload.get("server_name") or payload.get("sni"):
        score += 10
    if payload.get("public_key"):
        score += 10
    if payload.get("short_id"):
        score += 8
    if transport == "grpc" and payload.get("service_name"):
        score += 12
    if transport in {"ws", "websocket"} and payload.get("path"):
        score += 10
    quality_penalty = len(_candidate_quality_reasons(payload, pool="black" if security == "tls" else "ru_lte")) * 20
    score -= quality_penalty
    latency = payload.get("_latency_ms")
    if isinstance(latency, int):
        if latency <= 120:
            score += 40
        elif latency <= 200:
            score += 28
        elif latency <= 350:
            score += 15
        elif latency <= 600:
            score += 5
        else:
            score -= 25
    if payload.get("_probe_ok"):
        score += 50
    return score


def _patch_location_by_code(code: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for row in list_locations(active_only=False):
        if str(row.get("code") or "") == code:
            return patch_location(int(row.get("id")), updates)

    default_names = {
        "ru-lte": ("Россия LTE", "Russia LTE", False),
        "ru-lte-reserve-1": ("Россия LTE | Резерв 1", "Russia LTE | Reserve 1", True),
        "ru-lte-reserve-2": ("Россия LTE | Резерв 2", "Russia LTE | Reserve 2", True),
        "ru-lte-reserve-3": ("Россия LTE | Резерв 3", "Russia LTE | Reserve 3", True),
        "intl-fast": ("Fast / International", "Fast / International", False),
        "intl-fast-reserve-1": ("Fast / International | Reserve 1", "Fast / International | Reserve 1", True),
        "intl-fast-reserve-2": ("Fast / International | Reserve 2", "Fast / International | Reserve 2", True),
        "intl-fast-reserve-3": ("Fast / International | Reserve 3", "Fast / International | Reserve 3", True),
    }
    if code not in default_names:
        return None

    name_ru, name_en, is_reserve = default_names[code]
    base_payload = dict(updates.get("vpn_payload") or {})
    if base_payload:
        base_payload.setdefault("location_code", code)
        base_payload.setdefault("remark", name_en)

    payload = {
        "code": code,
        "name_ru": str(updates.get("name_ru") or name_ru),
        "name_en": str(updates.get("name_en") or name_en),
        "country_code": updates.get("country_code", "RU" if code.startswith("ru-lte") else None),
        "is_active": bool(updates.get("is_active", True)),
        "is_recommended": bool(updates.get("is_recommended", code == "ru-lte")),
        "is_reserve": bool(updates.get("is_reserve", is_reserve)),
        "status": str(updates.get("status") or "offline"),
        "sort_order": int(updates.get("sort_order") or {"ru-lte": 30, "ru-lte-reserve-1": 31, "ru-lte-reserve-2": 32, "ru-lte-reserve-3": 33, "intl-fast": 80, "intl-fast-reserve-1": 81, "intl-fast-reserve-2": 82, "intl-fast-reserve-3": 83}.get(code, 100)),
        "vpn_payload": base_payload,
        "location_source": str(updates.get("location_source") or "catalog"),
    }
    return create_location(payload)


def refresh_ru_lte_locations() -> Dict[str, Any]:
    sources = [item for item in (settings.RU_LTE_SOURCE_URLS or []) if str(item or "").strip()]
    candidates: List[Dict[str, Any]] = []
    seen: set[str] = set()
    source_stats: List[Dict[str, Any]] = []
    raw_parsed_total = 0
    complete_candidates_total = 0
    duplicate_skipped_total = 0
    quality_rejected_total = 0
    cooldown_skipped_total = 0

    for source in sources:
        stat = {
            "source": source,
            "lines": 0,
            "parsed": 0,
            "complete": 0,
            "accepted": 0,
            "duplicates": 0,
            "quality_rejected": 0,
            "cooldown_skipped": 0,
            "error": None,
        }
        try:
            content = _read_text_from_source(source)
            for line in content.splitlines():
                stat["lines"] += 1
                payload = _parse_vless_subscription_line(line)
                if not payload:
                    continue
                stat["parsed"] += 1
                raw_parsed_total += 1
                normalized = _normalize_vpn_payload_keys(payload)
                if not _config_is_complete(normalized):
                    continue
                stat["complete"] += 1
                complete_candidates_total += 1
                reasons = _candidate_quality_reasons(normalized, pool="ru_lte")
                if reasons:
                    stat["quality_rejected"] += 1
                    quality_rejected_total += 1
                    continue
                key = _candidate_identity_key(normalized)
                if key in seen:
                    stat["duplicates"] += 1
                    duplicate_skipped_total += 1
                    continue
                if _is_candidate_recently_dead("ru_lte", normalized):
                    stat["cooldown_skipped"] += 1
                    cooldown_skipped_total += 1
                    continue
                seen.add(key)
                normalized["_source"] = source
                normalized["_source_priority"] = _ru_lte_source_priority(source)
                normalized["_score"] = _ru_lte_payload_score(normalized)
                candidates.append(normalized)
                stat["accepted"] += 1
        except Exception as exc:
            stat["error"] = str(exc)
        source_stats.append(stat)

    candidates.sort(key=_candidate_sort_key, reverse=True)
    test_limit = max(1, int(settings.RU_LTE_TEST_LIMIT or 40))
    tested: List[Dict[str, Any]] = []
    probed_total = 0
    probe_errors = 0
    for candidate in candidates[:test_limit]:
        probed_total += 1
        probe = _ru_lte_probe_candidate(candidate)
        if not probe.get("ok"):
            probe_errors += 1
            _mark_candidate_dead("ru_lte", candidate)
            continue
        normalized = dict(candidate)
        normalized["_probe_ok"] = True
        normalized["_latency_ms"] = int(probe.get("latency_ms") or 0)
        normalized["_probe_method"] = str(probe.get("method") or "")
        normalized["_probe_url"] = str(probe.get("probe_url") or "")
        normalized["_score"] = _ru_lte_payload_score(normalized)
        tested.append(normalized)

    tested.sort(key=lambda item: (int(item.get("_latency_ms") or 999999), -int(item.get("_source_priority") or 0), -int(item.get("_score") or 0), str(item.get("remark") or "")))
    max_candidates = min(len(RU_LTE_RESERVE_LOCATION_CODES), max(1, int(settings.RU_LTE_MAX_CANDIDATES or len(RU_LTE_RESERVE_LOCATION_CODES))))
    top = _select_diverse_candidates(tested, max_candidates=max_candidates)[:max_candidates]

    existing_by_code = {str(row.get("code") or ""): row for row in list_locations(active_only=False)}
    selected_identities = {_candidate_identity_key(item) for item in top}
    if len(top) < max_candidates:
        for code in RU_LTE_RESERVE_LOCATION_CODES:
            existing = existing_by_code.get(code) or {}
            existing_payload = _compose_vpn_payload_for_location(dict(existing)) if existing else None
            if not existing_payload or not _config_is_complete(existing_payload):
                continue
            existing_payload = _normalize_vpn_payload_keys(existing_payload)
            if _candidate_identity_key(existing_payload) in selected_identities:
                continue
            if not _is_candidate_strong(existing_payload, pool="ru_lte"):
                continue
            if _is_candidate_recently_dead("ru_lte", existing_payload):
                continue
            probe = _ru_lte_probe_candidate(existing_payload)
            if not probe.get("ok"):
                _mark_candidate_dead("ru_lte", existing_payload)
                continue
            existing_payload["_probe_ok"] = True
            existing_payload["_latency_ms"] = int(probe.get("latency_ms") or existing.get("ping_ms") or 0)
            existing_payload["_probe_method"] = str(probe.get("method") or "")
            existing_payload["_probe_url"] = str(probe.get("probe_url") or "")
            existing_payload["_source_priority"] = -1
            existing_payload["_score"] = _ru_lte_payload_score(existing_payload)
            top.append(existing_payload)
            selected_identities.add(_candidate_identity_key(existing_payload))
            if len(top) >= max_candidates:
                break
        top = _select_diverse_candidates(top, max_candidates=max_candidates)[:max_candidates]

    assigned: List[Dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    remarks = {
        "ru-lte-reserve-1": "Russia LTE | Reserve 1",
        "ru-lte-reserve-2": "Russia LTE | Reserve 2",
        "ru-lte-reserve-3": "Russia LTE | Reserve 3",
    }

    for idx, code in enumerate(RU_LTE_RESERVE_LOCATION_CODES):
        payload = dict(top[idx]) if idx < len(top) else {}
        if payload:
            for key in ["_score", "_source", "_probe_ok"]:
                payload.pop(key, None)
            probe_method = str(payload.pop("_probe_method", "") or "")
            probe_url = str(payload.pop("_probe_url", "") or "")
            latency_ms = int(payload.pop("_latency_ms", 0) or 0)
            payload["location_code"] = code
            payload["remark"] = remarks.get(code, code)
            updates = {
                "vpn_payload": payload,
                "status": "online",
                "is_active": True,
                "is_recommended": False,
                "is_reserve": True,
                "ping_ms": latency_ms if latency_ms > 0 else None,
                "speed_checked_at": now_iso,
                "location_source": "github",
            }
            row = _patch_location_by_code(code, updates)
            assigned.append({
                "code": code,
                "server": payload.get("server"),
                "transport": payload.get("transport"),
                "remark": payload.get("remark"),
                "latency_ms": latency_ms,
                "probe_method": probe_method,
                "probe_url": probe_url,
                "updated": bool(row),
                "kept_old": idx >= len(tested),
            })
        else:
            existing = existing_by_code.get(code) or {}
            existing_payload = _compose_vpn_payload_for_location(dict(existing)) if existing else None
            keep_old = False
            if existing_payload and _config_is_complete(existing_payload):
                existing_payload = _normalize_vpn_payload_keys(existing_payload)
                if _is_candidate_strong(existing_payload, pool="ru_lte") and not _is_candidate_recently_dead("ru_lte", existing_payload):
                    probe = _ru_lte_probe_candidate(existing_payload)
                    if probe.get("ok"):
                        _patch_location_by_code(code, {
                            "status": "online",
                            "is_active": True,
                            "is_recommended": False,
                            "is_reserve": True,
                            "ping_ms": int(probe.get("latency_ms") or existing.get("ping_ms") or 0) or None,
                            "speed_checked_at": now_iso,
                            "location_source": str(existing.get("location_source") or "github"),
                        })
                        assigned.append({
                            "code": code,
                            "server": existing_payload.get("server"),
                            "transport": existing_payload.get("transport"),
                            "remark": remarks.get(code, code),
                            "latency_ms": int(probe.get("latency_ms") or existing.get("ping_ms") or 0) or None,
                            "probe_method": probe.get("method"),
                            "probe_url": probe.get("probe_url"),
                            "updated": True,
                            "kept_old": True,
                        })
                        keep_old = True
                    else:
                        _mark_candidate_dead("ru_lte", existing_payload)
                else:
                    _mark_candidate_dead("ru_lte", existing_payload)
            if not keep_old:
                row = _patch_location_by_code(code, {
                    "status": "offline",
                    "is_active": False,
                    "is_recommended": False,
                    "is_reserve": True,
                    "ping_ms": None,
                    "speed_checked_at": now_iso,
                    "location_source": "github",
                })
                assigned.append({"code": code, "server": None, "transport": None, "remark": None, "latency_ms": None, "updated": bool(row), "kept_old": False})

    selected_live = [item for item in assigned if item.get("server")]
    reused_existing_total = len([item for item in selected_live if item.get("kept_old")])
    effective_candidates_total = max(len(tested), len(selected_live))
    source_errors = [item for item in source_stats if item.get("error")]
    source_errors_total = len(source_errors)
    sources_ok_total = len(source_stats) - source_errors_total
    display_candidates_total = raw_parsed_total if raw_parsed_total > 0 else effective_candidates_total
    summary_live_total = max(len(tested), len(selected_live))
    refresh_summary = (
        f"parsed {raw_parsed_total} | live {summary_live_total} | selected {len(selected_live)}"
    )
    if reused_existing_total:
        refresh_summary += f" | reused {reused_existing_total}"
    if source_errors_total:
        refresh_summary += f" | source_errors {source_errors_total}"

    return {
        "ok": bool(top),
        "sources": source_stats,
        # Admin banner field: show parsed count when available, otherwise fall back
        # to the effective/reused live pool so the UI never shows confusing 0|3.
        "candidates_total": display_candidates_total,
        "raw_parsed_total": raw_parsed_total,
        "parsed_candidates_total": raw_parsed_total,
        "complete_candidates_total": complete_candidates_total,
        "unique_candidates_total": len(candidates),
        "effective_candidates_total": effective_candidates_total,
        "quality_rejected_total": quality_rejected_total,
        "duplicate_skipped_total": duplicate_skipped_total,
        "cooldown_skipped_total": cooldown_skipped_total,
        "tested_total": probed_total,
        "live_total": len(tested),
        "reused_existing_total": reused_existing_total,
        "probe_errors": probe_errors,
        "source_errors_total": source_errors_total,
        "sources_total": len(source_stats),
        "sources_ok_total": sources_ok_total,
        "source_errors": [{"source": item.get("source"), "error": item.get("error")} for item in source_errors],
        "real_probe_enabled": bool(getattr(settings, "RU_LTE_REAL_PROBE_ENABLED", True)),
        "real_probe_required": bool(getattr(settings, "RU_LTE_REAL_PROBE_REQUIRED", False)),
        "real_probe_runner": str(getattr(settings, "RU_LTE_REAL_PROBE_RUNNER", "xray") or "xray"),
        "selected": assigned,
        "selected_live_total": len(selected_live),
        "refresh_summary": refresh_summary,
        "auto_refresh_enabled": bool(settings.RU_LTE_AUTO_REFRESH_ENABLED),
        "auto_refresh_minutes": max(1, int(settings.RU_LTE_AUTO_REFRESH_MINUTES or 30)),
        "repo_raw_base": _vpn_configs_raw_base(),
    }


def refresh_black_locations() -> Dict[str, Any]:
    sources = [item for item in (settings.BLACK_SOURCE_URLS or []) if str(item or "").strip()]
    candidates: List[Dict[str, Any]] = []
    seen: set[str] = set()
    source_stats: List[Dict[str, Any]] = []
    raw_parsed_total = 0
    complete_candidates_total = 0
    duplicate_skipped_total = 0
    quality_rejected_total = 0
    cooldown_skipped_total = 0

    for source in sources:
        stat = {
            "source": source,
            "lines": 0,
            "parsed": 0,
            "complete": 0,
            "accepted": 0,
            "duplicates": 0,
            "quality_rejected": 0,
            "cooldown_skipped": 0,
            "error": None,
        }
        try:
            content = _read_text_from_source(source)
            for line in content.splitlines():
                stat["lines"] += 1
                payload = _parse_vless_subscription_line(line)
                if not payload:
                    continue
                stat["parsed"] += 1
                raw_parsed_total += 1
                normalized = _normalize_vpn_payload_keys(payload)
                if not _config_is_complete(normalized):
                    continue
                stat["complete"] += 1
                complete_candidates_total += 1
                reasons = _candidate_quality_reasons(normalized, pool="black")
                if reasons:
                    stat["quality_rejected"] += 1
                    quality_rejected_total += 1
                    continue
                key = _candidate_identity_key(normalized)
                if key in seen:
                    stat["duplicates"] += 1
                    duplicate_skipped_total += 1
                    continue
                if _is_candidate_recently_dead("black", normalized):
                    stat["cooldown_skipped"] += 1
                    cooldown_skipped_total += 1
                    continue
                seen.add(key)
                normalized["_source"] = source
                normalized["_source_priority"] = _black_source_priority(source)
                normalized["_score"] = _ru_lte_payload_score(normalized)
                candidates.append(normalized)
                stat["accepted"] += 1
        except Exception as exc:
            stat["error"] = str(exc)
        source_stats.append(stat)

    candidates.sort(key=_candidate_sort_key, reverse=True)
    test_limit = max(1, int(settings.BLACK_TEST_LIMIT or 40))
    tested: List[Dict[str, Any]] = []
    probed_total = 0
    probe_errors = 0
    max_candidates = min(len(BLACK_RESERVE_LOCATION_CODES), max(1, int(settings.BLACK_MAX_CANDIDATES or len(BLACK_RESERVE_LOCATION_CODES))))
    probe_budget_seconds = max(8.0, min(float(settings.BLACK_AUTO_REFRESH_TIMEOUT_SEC or 600), 25.0))
    probe_started = time.perf_counter()
    for candidate in candidates[:test_limit]:
        if tested and len(tested) >= max_candidates and (time.perf_counter() - probe_started) >= 2.0:
            break
        if (time.perf_counter() - probe_started) >= probe_budget_seconds:
            break
        probed_total += 1
        probe = _black_probe_candidate(candidate)
        if not probe.get("ok"):
            probe_errors += 1
            _mark_candidate_dead("black", candidate)
            continue
        normalized = dict(candidate)
        normalized["_probe_ok"] = True
        normalized["_latency_ms"] = int(probe.get("latency_ms") or 0)
        normalized["_score"] = _ru_lte_payload_score(normalized)
        tested.append(normalized)

    tested.sort(key=lambda item: (int(item.get("_latency_ms") or 999999), -int(item.get("_source_priority") or 0), -int(item.get("_score") or 0), str(item.get("remark") or "")))
    top = _select_diverse_candidates(tested, max_candidates=max_candidates)[:max_candidates]

    existing_by_code = {str(row.get("code") or ""): row for row in list_locations(active_only=False)}
    selected_identities = {_candidate_identity_key(item) for item in top}
    if len(top) < max_candidates:
        for code in BLACK_RESERVE_LOCATION_CODES:
            existing = existing_by_code.get(code) or {}
            existing_payload = _compose_vpn_payload_for_location(dict(existing)) if existing else None
            if not existing_payload or not _config_is_complete(existing_payload):
                continue
            existing_payload = _normalize_vpn_payload_keys(existing_payload)
            if _candidate_identity_key(existing_payload) in selected_identities:
                continue
            if not _is_candidate_strong(existing_payload, pool="black"):
                continue
            if _is_candidate_recently_dead("black", existing_payload):
                continue
            probe = _black_probe_candidate(existing_payload)
            if not probe.get("ok"):
                _mark_candidate_dead("black", existing_payload)
                continue
            existing_payload["_probe_ok"] = True
            existing_payload["_latency_ms"] = int(probe.get("latency_ms") or existing.get("ping_ms") or 0)
            existing_payload["_source_priority"] = -1
            existing_payload["_score"] = _ru_lte_payload_score(existing_payload)
            top.append(existing_payload)
            selected_identities.add(_candidate_identity_key(existing_payload))
            if len(top) >= max_candidates:
                break
        top = _select_diverse_candidates(top, max_candidates=max_candidates)[:max_candidates]

    assigned: List[Dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    remarks = {
        "intl-fast-reserve-1": "Fast / International | Reserve 1",
        "intl-fast-reserve-2": "Fast / International | Reserve 2",
        "intl-fast-reserve-3": "Fast / International | Reserve 3",
    }

    for idx, code in enumerate(BLACK_RESERVE_LOCATION_CODES):
        payload = dict(top[idx]) if idx < len(top) else {}
        if payload:
            for key in ["_score", "_source", "_probe_ok"]:
                payload.pop(key, None)
            latency_ms = int(payload.pop("_latency_ms", 0) or 0)
            payload["location_code"] = code
            payload["remark"] = remarks.get(code, code)
            updates = {
                "name_ru": remarks.get(code, code),
                "name_en": remarks.get(code, code),
                "country_code": None,
                "vpn_payload": payload,
                "status": "online",
                "is_active": True,
                "is_recommended": False,
                "is_reserve": True,
                "ping_ms": latency_ms if latency_ms > 0 else None,
                "speed_checked_at": now_iso,
                "location_source": "github",
            }
            row = _patch_location_by_code(code, updates)
            assigned.append({
                "code": code,
                "server": payload.get("server"),
                "transport": payload.get("transport"),
                "security": payload.get("security"),
                "remark": payload.get("remark"),
                "latency_ms": latency_ms,
                "updated": bool(row),
                "kept_old": idx >= len(tested),
            })
        else:
            existing = existing_by_code.get(code) or {}
            existing_payload = _compose_vpn_payload_for_location(dict(existing)) if existing else None
            keep_old = False
            if existing_payload and _config_is_complete(existing_payload):
                existing_payload = _normalize_vpn_payload_keys(existing_payload)
                if _is_candidate_strong(existing_payload, pool="black") and not _is_candidate_recently_dead("black", existing_payload):
                    probe = _black_probe_candidate(existing_payload)
                    if probe.get("ok"):
                        _patch_location_by_code(code, {
                            "name_ru": remarks.get(code, code),
                            "name_en": remarks.get(code, code),
                            "country_code": None,
                            "status": "online",
                            "is_active": True,
                            "is_recommended": False,
                            "is_reserve": True,
                            "ping_ms": int(probe.get("latency_ms") or existing.get("ping_ms") or 0) or None,
                            "speed_checked_at": now_iso,
                            "location_source": str(existing.get("location_source") or "github"),
                        })
                        assigned.append({
                            "code": code,
                            "server": existing_payload.get("server"),
                            "transport": existing_payload.get("transport"),
                            "security": existing_payload.get("security"),
                            "remark": remarks.get(code, code),
                            "latency_ms": int(probe.get("latency_ms") or existing.get("ping_ms") or 0) or None,
                            "updated": True,
                            "kept_old": True,
                        })
                        keep_old = True
                    else:
                        _mark_candidate_dead("black", existing_payload)
                else:
                    _mark_candidate_dead("black", existing_payload)
            if not keep_old:
                row = _patch_location_by_code(code, {
                    "name_ru": remarks.get(code, code),
                    "name_en": remarks.get(code, code),
                    "country_code": None,
                    "status": "offline",
                    "is_active": False,
                    "is_recommended": False,
                    "is_reserve": True,
                    "ping_ms": None,
                    "speed_checked_at": now_iso,
                    "location_source": "github",
                })
                assigned.append({"code": code, "server": None, "transport": None, "security": None, "remark": None, "latency_ms": None, "updated": bool(row), "kept_old": False})

    selected_live = [item for item in assigned if item.get("server")]
    reused_existing_total = len([item for item in selected_live if item.get("kept_old")])
    effective_candidates_total = max(len(tested), len(selected_live))
    source_errors = [item for item in source_stats if item.get("error")]
    source_errors_total = len(source_errors)
    sources_ok_total = len(source_stats) - source_errors_total
    display_candidates_total = raw_parsed_total if raw_parsed_total > 0 else effective_candidates_total
    summary_live_total = max(len(tested), len(selected_live))
    refresh_summary = (
        f"parsed {raw_parsed_total} | live {summary_live_total} | selected {len(selected_live)}"
    )
    if reused_existing_total:
        refresh_summary += f" | reused {reused_existing_total}"
    if source_errors_total:
        refresh_summary += f" | source_errors {source_errors_total}"

    return {
        "ok": bool(top),
        "sources": source_stats,
        # Admin banner field: show parsed count when available, otherwise fall back
        # to the effective/reused live pool so the UI never shows confusing 0|3.
        "candidates_total": display_candidates_total,
        "raw_parsed_total": raw_parsed_total,
        "parsed_candidates_total": raw_parsed_total,
        "complete_candidates_total": complete_candidates_total,
        "unique_candidates_total": len(candidates),
        "effective_candidates_total": effective_candidates_total,
        "quality_rejected_total": quality_rejected_total,
        "duplicate_skipped_total": duplicate_skipped_total,
        "cooldown_skipped_total": cooldown_skipped_total,
        "tested_total": probed_total,
        "live_total": len(tested),
        "reused_existing_total": reused_existing_total,
        "probe_errors": probe_errors,
        "source_errors_total": source_errors_total,
        "sources_total": len(source_stats),
        "sources_ok_total": sources_ok_total,
        "source_errors": [{"source": item.get("source"), "error": item.get("error")} for item in source_errors],
        "selected": assigned,
        "selected_live_total": len(selected_live),
        "refresh_summary": refresh_summary,
        "auto_refresh_enabled": bool(settings.BLACK_AUTO_REFRESH_ENABLED),
        "auto_refresh_minutes": max(1, int(settings.BLACK_AUTO_REFRESH_MINUTES or 30)),
        "repo_raw_base": _vpn_configs_raw_base(),
    }




def require_admin(credentials: HTTPBasicCredentials = Depends(basic_security)) -> str:
    valid_user = _safe_compare_secret(credentials.username, settings.ADMIN_BASIC_USER)
    valid_pass = _safe_compare_secret(credentials.password, settings.ADMIN_BASIC_PASS)
    if not (valid_user and valid_pass):
        raise HTTPException(status_code=401, detail="Invalid admin credentials", headers={"WWW-Authenticate": "Basic"})
    return credentials.username


@app.get("/api/infra/admin/vpn/debug/probe-runtime")
def admin_probe_runtime(credentials: HTTPBasicCredentials = Depends(require_admin)) -> Dict[str, Any]:
    return {"ok": True, **_probe_binary_status()}


def _run_startup_background_tasks() -> None:
    logger.info("[vpn][startup] background_tasks_started=1")
    if settings.RU_LTE_REFRESH_ON_STARTUP:
        try:
            _run_ru_lte_refresh_safe(source="startup")
        except Exception as exc:
            logger.exception("[vpn][startup] ru_lte_refresh_failed: %s", exc)
            _set_ru_lte_refresh_error(exc)
    if settings.BLACK_REFRESH_ON_STARTUP:
        try:
            _run_black_refresh_safe(source="startup")
        except Exception as exc:
            logger.exception("[vpn][startup] black_refresh_failed: %s", exc)
            _set_black_refresh_error(exc)
    if bool(getattr(settings, "VPN_LIVE_CHECK_ON_STARTUP", True)):
        try:
            _run_vpn_live_check_safe(source="startup", active_only=bool(getattr(settings, "VPN_LIVE_CHECK_ACTIVE_ONLY", True)))
        except Exception as exc:
            logger.exception("[vpn][startup] live_check_failed: %s", exc)
            _set_vpn_live_check_error(exc)
    logger.info("[vpn][startup] background_tasks_finished=1")


@app.on_event("startup")
def on_startup() -> None:
    bootstrap()
    _log_probe_binary_status()
    _start_ru_lte_auto_refresh_loop()
    _start_black_auto_refresh_loop()
    _start_vpn_live_check_auto_loop()
    thread = threading.Thread(target=_run_startup_background_tasks, name="vpn-startup-tasks", daemon=True)
    thread.start()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.CORS_ORIGINS == ["*"] else settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=500)

_LOCATIONS_CACHE_TTL_SEC = 5
_locations_cache: Dict[str, Any] = {"expires_at": 0.0, "items": None}
_ru_lte_refresh_state: Dict[str, Any] = {"last_success_at": None, "last_error": None, "last_error_at": None}
_black_refresh_state: Dict[str, Any] = {"last_success_at": None, "last_error": None, "last_error_at": None}
_vpn_live_check_state: Dict[str, Any] = {
    "last_success_at": None,
    "last_error": None,
    "last_error_at": None,
    "last_started_at": None,
    "last_finished_at": None,
    "last_source": None,
    "last_summary": None,
}
_vpn_live_check_lock = threading.Lock()


def _json_no_cache_headers(*, etag_seed: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    }
    if etag_seed is not None:
        headers["ETag"] = f'W/"{hashlib.sha256(etag_seed.encode("utf-8")).hexdigest()}"'
    return headers


def _runtime_config_version() -> str:
    parts: List[str] = []
    for row in list_locations(active_only=False):
        code = str(row.get("code") or "").strip()
        updated_at = str(row.get("updated_at") or "").strip()
        status = str(row.get("status") or "").strip().lower()
        is_active = "1" if bool(row.get("is_active")) else "0"
        is_deleted = "1" if bool(row.get("is_deleted")) else "0"
        payload = _compose_vpn_payload_for_location(dict(row)) or {}
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        parts.append("|".join([code, updated_at, status, is_active, is_deleted, payload_json]))
    digest = hashlib.sha256("\n".join(sorted(parts)).encode("utf-8")).hexdigest()
    return digest


def _config_version_payload() -> Dict[str, Any]:
    return {
        "ok": True,
        "version": _runtime_config_version(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "locations_cache_ttl_sec": int(_LOCATIONS_CACHE_TTL_SEC),
        "supports": {
            "sync": True,
            "vpn_config": True,
            "subscription": True,
        },
    }


def _invalidate_locations_cache() -> None:
    _locations_cache["items"] = None
    _locations_cache["expires_at"] = 0.0


def _set_ru_lte_refresh_success() -> None:
    _ru_lte_refresh_state["last_success_at"] = datetime.now(timezone.utc).isoformat()
    _ru_lte_refresh_state["last_error"] = None
    _ru_lte_refresh_state["last_error_at"] = None


def _set_ru_lte_refresh_error(exc: Exception) -> None:
    _ru_lte_refresh_state["last_error"] = str(exc)
    _ru_lte_refresh_state["last_error_at"] = datetime.now(timezone.utc).isoformat()


def _run_ru_lte_refresh_safe(*, source: str = "manual") -> Dict[str, Any]:
    result = refresh_ru_lte_locations()
    result["refresh_source"] = source
    _invalidate_locations_cache()
    _set_ru_lte_refresh_success()
    return result


def _start_ru_lte_auto_refresh_loop() -> None:
    if not settings.RU_LTE_AUTO_REFRESH_ENABLED:
        return
    interval_seconds = max(60, int(settings.RU_LTE_AUTO_REFRESH_MINUTES or 30) * 60)
    retry_seconds = min(90, max(20, int(settings.RU_LTE_CONNECT_TIMEOUT_SEC or 4) * 10))
    initial_delay = interval_seconds if settings.RU_LTE_REFRESH_ON_STARTUP else 0

    def worker() -> None:
        if initial_delay > 0:
            time.sleep(initial_delay)
        while True:
            started = time.monotonic()
            degraded = False
            try:
                result = _run_ru_lte_refresh_safe(source="auto")
                degraded = bool(result.get("selected_live_total", 0) < max(1, int(settings.RU_LTE_MAX_CANDIDATES or 4)))
            except Exception as exc:
                _set_ru_lte_refresh_error(exc)
                degraded = True
            elapsed = time.monotonic() - started
            sleep_for = retry_seconds if degraded else interval_seconds
            time.sleep(max(5.0, sleep_for - elapsed))

    thread = threading.Thread(target=worker, name="ru-lte-auto-refresh", daemon=True)
    thread.start()


def _set_black_refresh_success() -> None:
    _black_refresh_state["last_success_at"] = datetime.now(timezone.utc).isoformat()
    _black_refresh_state["last_error"] = None
    _black_refresh_state["last_error_at"] = None


def _set_black_refresh_error(exc: Exception) -> None:
    _black_refresh_state["last_error"] = str(exc)
    _black_refresh_state["last_error_at"] = datetime.now(timezone.utc).isoformat()


def _run_black_refresh_safe(*, source: str = "manual") -> Dict[str, Any]:
    result = refresh_black_locations()
    result["refresh_source"] = source
    _invalidate_locations_cache()
    _set_black_refresh_success()
    return result


def _start_black_auto_refresh_loop() -> None:
    if not settings.BLACK_AUTO_REFRESH_ENABLED:
        return
    interval_seconds = max(60, int(settings.BLACK_AUTO_REFRESH_MINUTES or 30) * 60)
    retry_seconds = min(120, max(30, int(settings.BLACK_CONNECT_TIMEOUT_SEC or 4) * 12))
    initial_delay = interval_seconds if settings.BLACK_REFRESH_ON_STARTUP else 0

    def worker() -> None:
        if initial_delay > 0:
            time.sleep(initial_delay)
        while True:
            started = time.monotonic()
            degraded = False
            try:
                result = _run_black_refresh_safe(source="auto")
                degraded = bool(result.get("selected_live_total", 0) < max(1, int(settings.BLACK_MAX_CANDIDATES or 4)))
            except Exception as exc:
                _set_black_refresh_error(exc)
                degraded = True
            elapsed = time.monotonic() - started
            sleep_for = retry_seconds if degraded else interval_seconds
            time.sleep(max(5.0, sleep_for - elapsed))

    thread = threading.Thread(target=worker, name="black-auto-refresh", daemon=True)
    thread.start()


def _set_vpn_live_check_success(summary: Dict[str, Any], *, source: str) -> None:
    _vpn_live_check_state["last_success_at"] = datetime.now(timezone.utc).isoformat()
    _vpn_live_check_state["last_error"] = None
    _vpn_live_check_state["last_error_at"] = None
    _vpn_live_check_state["last_finished_at"] = datetime.now(timezone.utc).isoformat()
    _vpn_live_check_state["last_source"] = source
    _vpn_live_check_state["last_summary"] = dict(summary)


def _set_vpn_live_check_error(exc: Exception) -> None:
    _vpn_live_check_state["last_error"] = str(exc)
    _vpn_live_check_state["last_error_at"] = datetime.now(timezone.utc).isoformat()
    _vpn_live_check_state["last_finished_at"] = datetime.now(timezone.utc).isoformat()


def _run_vpn_live_checks(*, source: str = "manual", active_only: bool = True) -> Dict[str, Any]:
    rows = list_locations(active_only=False)
    results: List[Dict[str, Any]] = []
    tested_rows = 0
    for row in rows:
        code = str(row.get("code") or "").strip()
        if code in PUBLIC_VIRTUAL_LOCATION_CODES:
            logger.info("[vpn][probe] source=%s code=%s result=skipped reason=virtual_location", source, code or "unknown")
            continue
        if active_only and not bool(row.get("is_active")):
            logger.info("[vpn][probe] source=%s code=%s result=skipped reason=inactive", source, code or "unknown")
            continue
        tested_rows += 1
        results.append(_run_location_speed_test(row, source=source))
    ok_count = sum(1 for item in results if item.get("status") == "ok")
    error_count = sum(1 for item in results if item.get("status") == "error")
    skipped_count = sum(1 for item in results if item.get("status") == "skipped")
    summary = {
        "ok": True,
        "source": source,
        "active_only": bool(active_only),
        "tested": tested_rows,
        "updated": ok_count,
        "errors": error_count,
        "skipped": skipped_count,
        "items": results,
    }
    _invalidate_locations_cache()
    logger.info(
        "[vpn][probe][summary] source=%s tested=%s ok=%s errors=%s skipped=%s active_only=%s",
        source,
        tested_rows,
        ok_count,
        error_count,
        skipped_count,
        int(bool(active_only)),
    )
    return summary


def _run_vpn_live_check_safe(*, source: str = "manual", active_only: bool = True) -> Dict[str, Any]:
    if not _vpn_live_check_lock.acquire(blocking=False):
        snapshot = dict(_vpn_live_check_state.get("last_summary") or {})
        snapshot.update({"ok": True, "busy": True, "source": source, "active_only": bool(active_only)})
        return snapshot
    _vpn_live_check_state["last_started_at"] = datetime.now(timezone.utc).isoformat()
    try:
        summary = _run_vpn_live_checks(source=source, active_only=active_only)
        _set_vpn_live_check_success({k: v for k, v in summary.items() if k != "items"}, source=source)
        return summary
    except Exception as exc:
        _set_vpn_live_check_error(exc)
        raise
    finally:
        _vpn_live_check_lock.release()


def _start_vpn_live_check_auto_loop() -> None:
    if not bool(getattr(settings, "VPN_LIVE_CHECK_AUTO_ENABLED", True)):
        return
    interval_seconds = max(60, int(getattr(settings, "VPN_LIVE_CHECK_AUTO_MINUTES", 3) or 3) * 60)
    retry_seconds = max(20, int(getattr(settings, "VPN_LIVE_CHECK_RETRY_SECONDS", 45) or 45))
    initial_delay = interval_seconds if bool(getattr(settings, "VPN_LIVE_CHECK_ON_STARTUP", True)) else 15
    active_only = bool(getattr(settings, "VPN_LIVE_CHECK_ACTIVE_ONLY", True))

    def worker() -> None:
        if initial_delay > 0:
            time.sleep(initial_delay)
        while True:
            started = time.monotonic()
            degraded = False
            try:
                summary = _run_vpn_live_check_safe(source="auto", active_only=active_only)
                degraded = bool(summary.get("errors")) or bool(summary.get("busy"))
            except Exception:
                degraded = True
            elapsed = time.monotonic() - started
            sleep_for = retry_seconds if degraded else interval_seconds
            time.sleep(max(5.0, sleep_for - elapsed))

    thread = threading.Thread(target=worker, name="vpn-live-check-auto", daemon=True)
    thread.start()


def _location_status_is_online(row: Dict[str, Any]) -> bool:
    return str(row.get("status") or "").strip().lower() in {"online", "reserve"}


def _public_location_code_allowed(code: str) -> bool:
    clean = str(code or "").strip()
    return bool(clean)


def _row_has_ready_payload(row: Dict[str, Any]) -> bool:
    payload = _compose_vpn_payload_for_location(dict(row))
    return bool(payload) and _config_is_complete(payload)


def _public_concrete_location_allowed(row: Dict[str, Any]) -> bool:
    code = str(row.get("code") or "").strip()
    if not code or code in PUBLIC_VIRTUAL_LOCATION_CODES:
        return False
    if not _location_status_is_online(row):
        return False
    return _row_has_ready_payload(row)


def _public_location_sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, int, int, str]:
    code = str(row.get("code") or "").strip()
    is_virtual = code in PUBLIC_VIRTUAL_LOCATION_CODES
    is_recommended = bool(row.get("is_recommended"))
    sort_order = int(row.get("sort_order") or 9999)
    stable_priority = PUBLIC_STABLE_LOCATION_CODES.index(code) if code in PUBLIC_STABLE_LOCATION_CODES else 1000
    reserve_rank = 1 if bool(row.get("is_reserve")) else 0
    display_name = str(row.get("name_en") or row.get("name_ru") or code or "").strip().lower()
    return (
        0 if is_virtual else 1,
        0 if is_recommended else 1,
        sort_order,
        stable_priority,
        reserve_rank,
        display_name,
    )


def _public_location_rows() -> List[Dict[str, Any]]:
    rows = list_locations(active_only=True)
    items: List[Dict[str, Any]] = []
    for row in rows:
        code = str(row.get("code") or "").strip()
        if not _public_location_code_allowed(code):
            continue
        if code in PUBLIC_VIRTUAL_LOCATION_CODES:
            resolved = _pick_virtual_location(code)
            if resolved is not None:
                items.append(dict(row))
            continue
        if not _public_concrete_location_allowed(row):
            continue
        items.append(dict(row))
    items.sort(key=_public_location_sort_key)
    return items


def _public_locations_health_snapshot() -> Dict[str, Any]:
    rows = _public_location_rows()
    online_codes = [str(row.get("code") or "").strip() for row in rows if str(row.get("code") or "").strip() in PUBLIC_STABLE_LOCATION_CODES]
    online_set = set(online_codes)
    ru_online = [code for code in RU_LTE_LOCATION_CODES if code in online_set]
    intl_online = [code for code in BLACK_LOCATION_CODES if code in online_set]
    return {
        "public_total": len(rows),
        "ru_lte_online": ru_online,
        "intl_online": intl_online,
        "healthy": bool(ru_online or intl_online),
        "degraded": not (RU_LTE_LOCATION_CODES[0] in online_set and BLACK_LOCATION_CODES[0] in online_set),
    }


def _cached_locations_payload() -> List[Dict[str, Any]]:
    now = time.monotonic()
    cached_items = _locations_cache.get("items")
    expires_at = float(_locations_cache.get("expires_at") or 0.0)
    if cached_items is not None and expires_at > now:
        return cached_items
    items = [serialize_location(row) for row in _public_location_rows()]
    _locations_cache["items"] = items
    _locations_cache["expires_at"] = now + _LOCATIONS_CACHE_TTL_SEC
    return items



def _safe_compare_secret(left: Optional[str], right: Optional[str]) -> bool:
    left_bytes = (left or "").encode("utf-8")
    right_bytes = (right or "").encode("utf-8")
    return secrets.compare_digest(left_bytes, right_bytes)


def issue_token(user_id: int, *, expires_delta: timedelta, token_type: str = "access") -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "typ": token_type,
        "iat": now,
        "exp": now + expires_delta,
        "jti": uuid4().hex,
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm="HS256")



def issue_access_token(user_id: int) -> str:
    return issue_token(
        user_id,
        expires_delta=timedelta(minutes=max(1, int(settings.AUTH_ACCESS_TOKEN_MINUTES or 60))),
        token_type="access",
    )



def issue_refresh_token(user_id: int) -> str:
    return issue_token(
        user_id,
        expires_delta=timedelta(days=max(1, int(settings.AUTH_REFRESH_TOKEN_DAYS or 90))),
        token_type="refresh",
    )



def _decode_token(raw_token: str, *, expected_type: Optional[str] = None) -> Dict[str, Any]:
    try:
        payload = jwt.decode(raw_token, settings.JWT_SECRET, algorithms=["HS256"])
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc
    token_type = str(payload.get("typ") or "")
    if expected_type and token_type != expected_type:
        raise HTTPException(status_code=401, detail=f"Invalid token type: expected {expected_type}")
    return payload



def _issue_auth_payload(user: Dict[str, Any], *, is_new: bool = False) -> Dict[str, Any]:
    user_id = int(user["id"])
    refresh_subscription_statuses(user_id)
    fresh_user = get_user_by_id(user_id) or user
    access_token = issue_access_token(user_id)
    refresh_token = issue_refresh_token(user_id)
    view = get_user_subscription_view(user_id)
    return {
        "ok": True,
        "token": access_token,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "user": fresh_user,
        "subscription": view.get("subscription"),
        "language": fresh_user.get("language") or "ru",
        "is_new": is_new,
    }



def _fallback_code_user_payload(payload: CodeAuthIn) -> Dict[str, Any]:
    suffix = str(payload.code or settings.AUTH_DEV_LOGIN_CODE)[-6:]
    telegram_id = payload.telegram_id or int(f"900{suffix}")
    language = "en" if payload.language == "en" else "ru"
    return {
        "telegram_id": telegram_id,
        "username": payload.username or f"inet_dev_{suffix}",
        "first_name": payload.first_name or "INET",
        "last_name": payload.last_name or "Dev",
        "language": language,
    }



def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    payload = _decode_token(credentials.credentials, expected_type="access")
    user_id = int(payload["sub"])
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user



def require_code_issuer(x_auth_code_secret: Optional[str] = Header(default=None)) -> bool:
    configured_secret = (settings.AUTH_CODE_ISSUER_SECRET or "").strip()
    if not configured_secret:
        return True
    if _safe_compare_secret(x_auth_code_secret, configured_secret):
        return True
    raise HTTPException(status_code=401, detail="Invalid code issuer secret")



def _flag_emoji(country_code: Optional[str]) -> str:
    if not country_code or len(country_code) != 2:
        return ""
    code = country_code.upper()
    if not code.isalpha():
        return ""
    return "".join(chr(127397 + ord(char)) for char in code)



def _location_row_by_code(code: str) -> Optional[Dict[str, Any]]:
    normalized_code = str(code or "").strip().lower()
    if not normalized_code:
        return None
    for item in list_locations(active_only=False):
        item_code = str(item.get("code") or "").strip().lower()
        if item_code == normalized_code:
            return dict(item)
    return None


def _resolved_location_code_for_row(row: Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> str:
    normalized = _normalize_vpn_payload_keys(payload or {}) if payload else {}
    row_code = str(row.get("code") or "").strip()
    if row_code in PUBLIC_VIRTUAL_LOCATION_CODES:
        return str(normalized.get("resolved_location_code") or normalized.get("location_code") or row_code).strip()
    return str(normalized.get("resolved_location_code") or normalized.get("location_code") or row_code).strip()


def _resolved_country_code_for_row(row: Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> str:
    normalized = _normalize_vpn_payload_keys(payload or {}) if payload else {}
    country_code = str(
        normalized.get("resolved_country_code")
        or normalized.get("country_code")
        or row.get("country_code")
        or ""
    ).strip()
    if country_code:
        return country_code.upper()
    effective_code = _resolved_location_code_for_row(row, normalized)
    if effective_code:
        matched_row = _location_row_by_code(effective_code)
        matched_country_code = str((matched_row or {}).get("country_code") or "").strip()
        if matched_country_code:
            return matched_country_code.upper()
    return ""


def _subscription_country_flag(row: Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> str:
    country_code = _resolved_country_code_for_row(row, payload)
    return _flag_emoji(country_code) if country_code else ""


def _strip_leading_location_tokens(value: str) -> str:
    cleaned = str(value or "").strip()
    while True:
        updated = cleaned
        updated = re.sub(r"^(?:★|⭐)\s+", "", updated).strip()
        updated = re.sub(r"^[\U0001F1E6-\U0001F1FF]{2}\s*", "", updated).strip()
        updated = re.sub(r"^(?:📶|🌍|🌎|🌏|🌐|🏁|⚡)\s*", "", updated).strip()
        updated = re.sub(r"^(?:[A-Z]{2}|[A-Z][a-z])\s+(?=[A-Z])", "", updated).strip()
        if updated == cleaned:
            return updated
        cleaned = updated


def _subscription_icon_for_row(row: Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> str:
    effective_code = _resolved_location_code_for_row(row, payload).lower()
    country_flag = _subscription_country_flag(row, payload)
    if effective_code.startswith("ru-lte"):
        return country_flag or "🇷🇺"
    if effective_code.startswith("intl-fast"):
        return "🌍"
    return country_flag


def _decorate_location_text(base_name: str, effective_code: str) -> str:
    text_value = str(base_name or "").strip()
    if not text_value:
        return "VLESS"
    if "→" in text_value:
        left, right = text_value.split("→", 1)
        right_clean = _strip_leading_location_tokens(right)
        if effective_code.lower().startswith("ru-lte") and not right_clean.startswith("📶"):
            right_clean = f"📶 {right_clean}".strip()
        return f"{left.strip()} → {right_clean}".strip()
    text_value = _strip_leading_location_tokens(text_value)
    if effective_code.lower().startswith("ru-lte") and not text_value.startswith("📶"):
        return f"📶 {text_value}".strip()
    return text_value


def _subscription_target_name_for_row(row: Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> str:
    normalized = _normalize_vpn_payload_keys(payload or {}) if payload else {}
    base_name = str(
        normalized.get("remark")
        or normalized.get("display_name")
        or row.get("name_en")
        or row.get("name_ru")
        or row.get("code")
        or "VLESS"
    ).strip() or "VLESS"
    effective_code = _resolved_location_code_for_row(row, normalized)
    display_text = _decorate_location_text(base_name, effective_code)
    icon = _subscription_icon_for_row(row, normalized)
    if icon:
        return f"{icon} {display_text}".strip()
    return display_text




def _location_meta(row: Dict[str, Any]) -> Dict[str, Any]:
    code = str(row.get("code") or "").strip()
    lowered = code.lower()
    if code.startswith("auto-"):
        return {
            "type": "virtual",
            "section_key": "system",
            "section_name_ru": "Системные",
            "section_name_en": "System",
            "icon": "💎",
        }
    if lowered.startswith("ru-lte"):
        return {
            "type": "mobile",
            "section_key": "mobile",
            "section_name_ru": "Мобильные",
            "section_name_en": "Mobile",
            "icon": _subscription_icon_for_row(row),
        }
    if lowered.startswith("intl-fast"):
        return {
            "type": "node",
            "section_key": "countries",
            "section_name_ru": "Основные страны",
            "section_name_en": "Main countries",
            "icon": "🌍",
        }
    return {
        "type": "node",
        "section_key": "countries",
        "section_name_ru": "Основные страны",
        "section_name_en": "Main countries",
        "icon": _subscription_icon_for_row(row),
    }



def _diagnostic_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()



def _diagnostic_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return None



def _diagnostic_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0



def _diagnostic_placeholder(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    lowered = text.lower()
    if lowered.startswith(("paste_", "your_", "replace_", "todo", "changeme")):
        return True
    if "example.com" in lowered or "example.net" in lowered or "example.org" in lowered:
        return True
    if lowered in {"ru_provider_host", "uz_provider_host", "ru_provider_user", "uz_provider_user", "ru_provider_pass", "uz_provider_pass"}:
        return True
    return False


def _diagnostic_string_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []



def _diagnostic_access_mode(payload: Dict[str, Any]) -> str:
    text = str((payload or {}).get("access_mode") or (payload or {}).get("credential_mode") or "").strip().lower()
    if text in {"owned_per_user", "per_user", "owned", "template", "template_per_user", "user_uuid", "user-specific", "user_specific"}:
        return "owned_per_user"
    return "external_static"



def _build_tun_platform_diagnostics(payload: Dict[str, Any], platform_label: str) -> Dict[str, Any]:
    issues: List[str] = []
    fixes: List[str] = []

    server = _diagnostic_text(payload.get("server"))
    port = _diagnostic_int(payload.get("port"))
    uuid = _diagnostic_text(payload.get("uuid"))
    security = _diagnostic_text(payload.get("security") or "reality").lower() or "reality"
    transport = _diagnostic_text(payload.get("transport") or payload.get("network") or "tcp").lower() or "tcp"
    sni = _diagnostic_text(payload.get("server_name") or payload.get("sni"))
    public_key = _diagnostic_text(payload.get("public_key") or payload.get("publicKey"))
    short_id = _diagnostic_text(payload.get("short_id") or payload.get("shortId"))
    service_name = _diagnostic_text(payload.get("service_name") or payload.get("serviceName"))
    path = _diagnostic_text(payload.get("path"))
    connect_mode = _diagnostic_text(payload.get("connect_mode") or "tun").lower() or "tun"
    full_tunnel = _diagnostic_bool(payload.get("full_tunnel"))
    dns_servers = _diagnostic_string_list(payload.get("dns_servers") or payload.get("dnsServers"))
    access_mode = _diagnostic_access_mode(payload)
    requires_runtime_uuid = access_mode == "owned_per_user"

    if not payload:
        issues.append("vpn_payload is empty")
        fixes.append("Open Edit payload and fill server, port, and Reality fields.")

    if not server or _diagnostic_placeholder(server):
        issues.append("server is missing or still contains a placeholder")
    if port <= 0:
        issues.append("port is missing")
    if requires_runtime_uuid:
        if not uuid or _diagnostic_placeholder(uuid):
            fixes.append("Template mode is enabled: the backend will inject a per-user UUID at выдаче конфигурации.")
            fixes.append("For live tunnel probe add probe_uuid (or live_probe_uuid) for this server template.")
    elif not uuid or _diagnostic_placeholder(uuid):
        issues.append("uuid is missing or still contains a placeholder")

    base_required_issues = {"server is missing or still contains a placeholder", "port is missing"}
    if not requires_runtime_uuid:
        base_required_issues.add("uuid is missing or still contains a placeholder")
    if any(issue in base_required_issues for issue in issues):
        if requires_runtime_uuid:
            fixes.append("Set real server/port values in effective payload. UUID comes from per-user credential mode.")
        else:
            fixes.append("Set real server/port/uuid values in effective payload.")

    if security == "reality":
        if not public_key or _diagnostic_placeholder(public_key):
            issues.append("Reality public_key is missing or still contains a placeholder")
        if not sni or _diagnostic_placeholder(sni):
            issues.append("Reality server_name / sni is missing or still contains a placeholder")
        if any(issue.startswith("Reality ") for issue in issues):
            fixes.append("For Reality fill real public_key and server_name/sni values.")

    if transport in {"grpc"} and (not service_name or _diagnostic_placeholder(service_name)):
        issues.append("gRPC service_name is missing or still contains a placeholder")
        fixes.append("Set service_name for gRPC transport.")

    if transport in {"ws", "websocket"} and (not path or _diagnostic_placeholder(path)):
        issues.append("WebSocket path is missing or still contains a placeholder")
        fixes.append("Set path for WebSocket transport.")

    if connect_mode != "tun":
        issues.append(f"connect_mode must be tun, got {connect_mode or 'empty'}")
        fixes.append("Set connect_mode=tun.")

    if full_tunnel is False:
        issues.append("full_tunnel is disabled")
        fixes.append("Set full_tunnel=true for full-device routing.")

    if not dns_servers or any(_diagnostic_placeholder(item) for item in dns_servers):
        issues.append("dns_servers is empty or still contains placeholders")
        fixes.append("Add dns_servers, for example 1.1.1.1 and 8.8.8.8.")

    platform_key = str(platform_label or "").strip().lower()
    if platform_key == "windows":
        fixes.append("Run the VPN client as Administrator before enabling TUN on Windows.")
    elif platform_key == "macos":
        fixes.append("Allow VPN / Network Extension permissions in macOS System Settings if TUN does not start.")

    fatal_prefixes = [
        "vpn_payload is empty",
        "server is missing or still contains a placeholder",
        "port is missing",
        "Reality public_key is missing or still contains a placeholder",
        "Reality server_name / sni is missing or still contains a placeholder",
        "connect_mode must be tun",
    ]
    if not requires_runtime_uuid:
        fatal_prefixes.append("uuid is missing or still contains a placeholder")
    fatal_prefixes = tuple(fatal_prefixes)
    fatal_issues = [issue for issue in issues if issue.startswith(fatal_prefixes)]

    if fatal_issues:
        status = "error"
        label = f"{platform_label}: payload incomplete"
    elif issues:
        status = "warning"
        label = f"{platform_label}: check payload"
    else:
        status = "ready"
        label = f"{platform_label}: ready"

    if not issues and not _config_is_complete(payload):
        status = "error"
        label = f"{platform_label}: payload incomplete"
        issues.append("effective payload is incomplete")
        fixes.append("Fill required fields in effective payload.")

    unique_fixes = list(dict.fromkeys([item for item in fixes if item]))
    return {
        "status": status,
        "label": label,
        "issues": issues,
        "fixes": unique_fixes,
    }



def build_location_tun_diagnostics(row: Dict[str, Any], *, resolved_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    code = _diagnostic_text(row.get("code"))
    preview_row = dict(row)
    preview_payload = dict(resolved_payload) if isinstance(resolved_payload, dict) else {}
    preview_target_code = code
    preview_target_name = _diagnostic_text(row.get("name_en") or row.get("name_ru") or code)
    is_virtual = code in PUBLIC_VIRTUAL_LOCATION_CODES

    if is_virtual and not preview_payload:
        picked = _pick_virtual_location(code)
        if picked is not None:
            preview_row = dict(picked)
            preview_target_code = _diagnostic_text(preview_row.get("code")) or code
            preview_target_name = _diagnostic_text(preview_row.get("name_en") or preview_row.get("name_ru") or preview_target_code)
            preview_payload = _compose_vpn_payload_for_location(preview_row, requested_location_code=code or None)
            if preview_payload:
                virtual_name = _diagnostic_text(row.get("name_en") or row.get("name_ru") or code) or code or "VLESS"
                resolved_name = preview_target_name or preview_target_code or "VLESS"
                preview_payload = dict(preview_payload)
                preview_payload["remark"] = f"{virtual_name} → {resolved_name}"
                preview_payload["display_name"] = preview_payload["remark"]
        else:
            issue = "no auto candidate available"
            fix = "Bring at least one online concrete location with a complete payload. A fresh live check is preferred, but virtual auto routing can also fall back to payload-ready online nodes."
            platform_items = {}
            for platform_label in ("Android", "iOS", "Windows", "macOS"):
                platform_items[platform_label.lower()] = {
                    "status": "error",
                    "label": f"{platform_label}: no candidate",
                    "issues": [issue],
                    "fixes": [fix],
                }
            return {
                "summary_status": "error",
                "summary_text": issue,
                "issues": [issue],
                "fixes": [fix],
                "android": platform_items["android"],
                "ios": platform_items["ios"],
                "windows": platform_items["windows"],
                "macos": platform_items["macos"],
                "preview_payload": {},
                "preview_target_code": None,
                "preview_target_name": None,
                "preview_is_virtual": True,
            }

    payload = preview_payload if preview_payload else _compose_vpn_payload_for_location(dict(row))
    android = _build_tun_platform_diagnostics(payload, "Android")
    ios = _build_tun_platform_diagnostics(payload, "iOS")
    windows = _build_tun_platform_diagnostics(payload, "Windows")
    macos = _build_tun_platform_diagnostics(payload, "macOS")

    live_state = _row_live_probe_state(preview_row)
    for platform_item, platform_label in ((android, "Android"), (ios, "iOS"), (windows, "Windows"), (macos, "macOS")):
        if live_state.get("status") != "ready" and platform_item.get("status") == "ready":
            platform_item["status"] = live_state.get("status") or "warning"
            platform_item["label"] = f"{platform_label}: {live_state.get('label') or 'check required'}"
            platform_item["issues"] = list(dict.fromkeys([live_state.get("text")] + platform_item.get("issues", [])))
            platform_item["fixes"] = list(dict.fromkeys(platform_item.get("fixes", []) + ([live_state.get("fix")] if live_state.get("fix") else [])))
        elif live_state.get("status") == "ready" and platform_item.get("status") == "ready":
            platform_item["label"] = f"{platform_label}: {live_state.get('label') or 'live ok'}"

    platform_items = (android, ios, windows, macos)

    summary_status = "ready"
    if any(item["status"] == "error" for item in platform_items):
        summary_status = "error"
    elif any(item["status"] == "warning" for item in platform_items):
        summary_status = "warning"

    issues = list(dict.fromkeys(
        android.get("issues", [])
        + ios.get("issues", [])
        + windows.get("issues", [])
        + macos.get("issues", [])
    ))
    fixes = list(dict.fromkeys(
        android.get("fixes", [])
        + ios.get("fixes", [])
        + windows.get("fixes", [])
        + macos.get("fixes", [])
    ))
    checked_at = live_state.get("checked_at")
    checked_text = checked_at.astimezone(timezone.utc).isoformat() if isinstance(checked_at, datetime) else None
    if summary_status == "ready":
        if is_virtual and preview_target_name and preview_target_code and preview_target_code != code:
            summary_text = f"Virtual auto route is live. Admin preview resolves to {preview_target_name}."
        else:
            summary_text = live_state.get("text") or "Real tunnel check passed."
    else:
        summary_text = live_state.get("text") or "; ".join(issues[:4]) or "TUN payload needs attention."

    return {
        "summary_status": summary_status,
        "summary_text": summary_text,
        "issues": issues,
        "fixes": fixes,
        "android": android,
        "ios": ios,
        "windows": windows,
        "macos": macos,
        "live_status": live_state.get("status"),
        "live_text": live_state.get("text"),
        "live_reason": live_state.get("reason"),
        "live_publishable": bool(live_state.get("publishable")),
        "live_ping_ms": live_state.get("ping_ms"),
        "live_checked_at": checked_text,
        "live_probe": dict((payload if isinstance(payload, dict) else {}).get("_last_live_probe") or {}),
        "preview_payload": payload if isinstance(payload, dict) else {},
        "preview_target_code": preview_target_code,
        "preview_target_name": preview_target_name,
        "preview_target_status": preview_row.get("status"),
        "preview_download_mbps": preview_row.get("download_mbps"),
        "preview_upload_mbps": preview_row.get("upload_mbps"),
        "preview_ping_ms": preview_row.get("ping_ms"),
        "preview_speed_checked_at": preview_row.get("speed_checked_at"),
        "preview_is_virtual": is_virtual,
    }


def _location_error_created_at(row: Dict[str, Any]) -> str:
    for key in ("updated_at", "created_at"):
        value = row.get(key)
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        text = _diagnostic_text(value)
        if text:
            return text
    return datetime.now(timezone.utc).isoformat()



def build_location_tun_error_items() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in list_locations(active_only=False):
        diagnostics = build_location_tun_diagnostics(row)
        if diagnostics.get("summary_status") == "ready":
            continue
        code = _diagnostic_text(row.get("code")) or "unknown"
        issues = diagnostics.get("issues", [])
        fixes = diagnostics.get("fixes", [])
        message_parts = []
        if issues:
            message_parts.append("Issues: " + " | ".join(issues[:4]))
        if fixes:
            message_parts.append("Fix: " + " | ".join(fixes[:3]))
        items.append({
            "created_at": _location_error_created_at(row),
            "source": f"vpn-location:{code}",
            "context": diagnostics.get("summary_status") or "error",
            "error_message": " || ".join(message_parts) or (diagnostics.get("summary_text") or "TUN payload needs attention."),
        })
    return items



def serialize_location(row: Dict[str, Any], *, include_payload: bool = False) -> Dict[str, Any]:
    item = dict(row)
    raw_vpn_payload = item.pop("vpn_payload", None)
    normalized_payload = dict(raw_vpn_payload) if isinstance(raw_vpn_payload, dict) else {}
    meta = _location_meta(item)
    item.update(meta)
    effective_code = _resolved_location_code_for_row(item, normalized_payload)
    if str(effective_code or "").lower().startswith("ru-lte"):
        item["display_name_ru"] = _decorate_location_text(str(item.get("name_ru") or item.get("name_en") or ""), effective_code)
        item["display_name_en"] = _decorate_location_text(str(item.get("name_en") or item.get("name_ru") or ""), effective_code)
    elif str(effective_code or "").lower().startswith("intl-fast"):
        item["display_name_ru"] = _strip_leading_location_tokens(str(item.get("name_ru") or item.get("name_en") or ""))
        item["display_name_en"] = _strip_leading_location_tokens(str(item.get("name_en") or item.get("name_ru") or ""))
    else:
        item["display_name_ru"] = f'{meta["icon"]} {item.get("name_ru")}'.strip() if meta.get("icon") else item.get("name_ru")
        item["display_name_en"] = f'{meta["icon"]} {item.get("name_en")}'.strip() if meta.get("icon") else item.get("name_en")
    item["name"] = item.get("display_name_ru") or item.get("name_ru") or item.get("name_en")
    item["recommended"] = bool(item.get("is_recommended"))
    item["reserve"] = bool(item.get("is_reserve"))
    item["location_source"] = str(item.get("location_source") or "catalog")
    diagnostics = build_location_tun_diagnostics(row)
    resolved_payload = diagnostics.get("preview_payload") if isinstance(diagnostics.get("preview_payload"), dict) else {}
    item["has_vpn_payload"] = bool(normalized_payload) or bool(resolved_payload)
    item["vpn_payload_complete"] = bool(resolved_payload) and _config_is_complete(resolved_payload)
    item["resolved_target_code"] = diagnostics.get("preview_target_code")
    item["resolved_target_name"] = diagnostics.get("preview_target_name")
    if str(item.get("code") or "").strip() in PUBLIC_VIRTUAL_LOCATION_CODES and diagnostics.get("preview_target_code"):
        item["status"] = diagnostics.get("preview_target_status") or item.get("status")
        item["download_mbps"] = diagnostics.get("preview_download_mbps")
        item["upload_mbps"] = diagnostics.get("preview_upload_mbps")
        item["ping_ms"] = diagnostics.get("preview_ping_ms")
        item["speed_checked_at"] = diagnostics.get("preview_speed_checked_at")
    item["live_status"] = diagnostics.get("live_status")
    item["live_text"] = diagnostics.get("live_text")
    item["live_reason"] = diagnostics.get("live_reason")
    item["live_publishable"] = bool(diagnostics.get("live_publishable"))
    item["live_checked_at"] = diagnostics.get("live_checked_at")
    item["live_ping_ms"] = diagnostics.get("live_ping_ms")
    item["live_probe"] = diagnostics.get("live_probe") or {}
    if include_payload:
        item["vpn_payload"] = normalized_payload
        item["resolved_vpn_payload"] = resolved_payload
        item["tun_diagnostics"] = diagnostics
    return item



def _normalize_admin_location_input(data: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(data or {})
    alias_pairs = (
        ("active", "is_active"),
        ("recommended", "is_recommended"),
        ("reserve", "is_reserve"),
    )
    for alias_key, canonical_key in alias_pairs:
        if canonical_key not in normalized and alias_key in normalized:
            normalized[canonical_key] = normalized.get(alias_key)
    return normalized


def _bot_public_url() -> str:
    bot_username = (settings.BOT_USERNAME or "").strip().lstrip("@")
    if bot_username:
        return f"https://t.me/{bot_username}"
    return settings.SUPPORT_TELEGRAM_URL


def _bot_profile_title_label() -> str:
    bot_username = (settings.BOT_USERNAME or "").strip().lstrip("@")
    if bot_username:
        return f"t.me/{bot_username}"
    bot_url = str(_bot_public_url() or "").strip()
    if bot_url:
        return bot_url.replace("https://", "").replace("http://", "")
    return "bot"


def _selected_client_mode() -> str:
    mode = str(getattr(settings, "VPN_CLIENT_MODE", "hiddify") or "").strip().lower()
    return "v2raytun" if mode == "v2raytun" else "hiddify"


def _selected_client_name() -> str:
    return "v2RayTun" if _selected_client_mode() == "v2raytun" else "Hiddify"


def _normalize_target_platform(platform: Optional[str]) -> str:
    raw = str(platform or "").strip().lower()
    if raw in {"android"}:
        return "android"
    if raw in {"ios", "iphone", "ipad"}:
        return "ios"
    if raw in {"windows", "win"}:
        return "windows"
    if raw in {"macos", "mac", "osx", "darwin"}:
        return "macos"
    if raw == "linux":
        return "linux"
    return "client"


def _client_mode_for_platform(platform: Optional[str]) -> str:
    normalized = _normalize_target_platform(platform)
    if normalized in {"windows", "macos"}:
        return "happ"
    return _selected_client_mode()


def _client_name_for_platform(platform: Optional[str]) -> str:
    mode = _client_mode_for_platform(platform)
    if mode == "happ":
        return "Happ"
    return "v2RayTun" if mode == "v2raytun" else "Hiddify"


def _selected_android_app_package(platform: Optional[str] = None) -> str:
    mode = _client_mode_for_platform(platform or "android")
    if mode == "v2raytun":
        return str(getattr(settings, "V2RAYTUN_ANDROID_APP_PACKAGE", "") or "").strip()
    return str(getattr(settings, "HIDDIFY_ANDROID_APP_PACKAGE", getattr(settings, "ANDROID_APP_PACKAGE", "")) or "").strip()


def _selected_platform_store_url(platform: str) -> str:
    key = _normalize_target_platform(platform)
    mode = _client_mode_for_platform(key)
    if key == "windows":
        return str(getattr(settings, "HAPP_WINDOWS_APP_URL", getattr(settings, "WINDOWS_APP_URL", "")) or "").strip()
    if key == "macos":
        return str(getattr(settings, "HAPP_MACOS_APP_URL", getattr(settings, "MACOS_APP_URL", "")) or "").strip()
    if mode == "v2raytun":
        if key == "android":
            return str(getattr(settings, "V2RAYTUN_ANDROID_APP_URL", "") or "").strip()
        if key in {"ios"}:
            return str(getattr(settings, "V2RAYTUN_IOS_APP_URL", "") or "").strip()
    if key == "android":
        return str(getattr(settings, "HIDDIFY_ANDROID_APP_URL", getattr(settings, "ANDROID_APP_URL", "")) or "").strip()
    if key in {"ios"}:
        return str(getattr(settings, "HIDDIFY_IOS_APP_URL", getattr(settings, "IOS_APP_URL", "")) or "").strip()
    return str(getattr(settings, "HAPP_WINDOWS_APP_URL", getattr(settings, "WINDOWS_APP_URL", "")) or "").strip()


def _build_native_import_url(subscription_url: Optional[str], platform: Optional[str] = None) -> str:
    clean_url = str(subscription_url or "").strip()
    if not clean_url:
        return ""
    mode = _client_mode_for_platform(platform)
    if mode == "happ":
        return ""
    if mode == "v2raytun":
        return f"v2raytun://import/{quote(clean_url, safe=':/?&=%#')}"
    import_name = str(getattr(settings, "HIDDIFY_IMPORT_NAME", "") or f"{settings.APP_NAME} Subscription").strip()
    return f"hiddify://import/{quote(clean_url, safe=':/?&=%')}#{quote(import_name, safe='')}"


def _subscription_public_url_from_base(base_url: str, token: Optional[str]) -> Optional[str]:
    base = str(base_url or "").strip().rstrip("/")
    clean_token = str(token or "").strip()
    if not base or not clean_token:
        return None
    return f"{base}/sub/{quote(clean_token, safe='')}"


def _resolve_subscription_token(*, token: Optional[str] = None, code: Optional[str] = None) -> Optional[str]:
    clean_token = str(token or "").strip()
    if clean_token:
        return clean_token
    clean_code = str(code or "").strip()
    if not clean_code:
        return None
    user = get_user_by_active_auth_code(clean_code)
    if not user:
        return None
    return ensure_user_subscription_token(int(user["id"]))


def _request_forwarded_proto(request: Optional[Request]) -> str:
    if request is None:
        return ""
    raw_cf_visitor = str(request.headers.get("cf-visitor") or "").strip()
    if raw_cf_visitor:
        try:
            parsed_cf_visitor = json.loads(raw_cf_visitor)
            cf_scheme = str(parsed_cf_visitor.get("scheme") or "").strip().lower()
            if cf_scheme in {"http", "https"}:
                return cf_scheme
        except Exception:
            pass
    for header_name in ("x-forwarded-proto", "x-forwarded-protocol", "x-scheme"):
        raw_value = str(request.headers.get(header_name) or "").strip()
        if not raw_value:
            continue
        first_part = raw_value.split(",", 1)[0].strip().lower()
        if first_part in {"http", "https"}:
            return first_part
    scheme = str(getattr(getattr(request, "url", None), "scheme", "") or "").strip().lower()
    return scheme if scheme in {"http", "https"} else ""



def _request_forwarded_host(request: Optional[Request]) -> str:
    if request is None:
        return ""
    raw_forwarded_host = str(request.headers.get("x-forwarded-host") or "").strip()
    if raw_forwarded_host:
        return raw_forwarded_host.split(",", 1)[0].strip()
    host_header = str(request.headers.get("host") or "").strip()
    if host_header:
        return host_header
    try:
        return str(getattr(request.url, "netloc", "") or "").strip()
    except Exception:
        return ""



def _request_external_base_url(request: Optional[Request]) -> str:
    configured_base = str(getattr(settings, "BACKEND_BASE_URL", "") or "").strip().rstrip("/")
    if configured_base:
        return configured_base
    if request is None:
        return ""
    scheme = _request_forwarded_proto(request) or "https"
    host = _request_forwarded_host(request)
    if not host:
        return ""
    return f"{scheme}://{host}"



def _request_is_https(request: Optional[Request]) -> bool:
    return _request_forwarded_proto(request) == "https"



def _subscription_public_url(request: Optional[Request] = None, token: Optional[str] = None, code: Optional[str] = None) -> Optional[str]:
    clean_token = _resolve_subscription_token(token=token, code=code)
    if not clean_token:
        return None
    external_base = _request_external_base_url(request)
    if external_base:
        return _subscription_public_url_from_base(external_base, clean_token)
    return _subscription_public_url_from_base(settings.BACKEND_BASE_URL, clean_token)


def _build_native_open_app_url(request: Optional[Request] = None, *, code: Optional[str] = None, token: Optional[str] = None, lang: Optional[str] = None, platform: Optional[str] = None) -> str:
    del lang
    subscription_url = _subscription_public_url(request, token=token, code=code)
    return _build_native_import_url(subscription_url, platform=platform)


def _build_open_app_bridge_url(request: Request, *, code: Optional[str] = None, token: Optional[str] = None, lang: Optional[str] = None, platform: Optional[str] = None) -> str:
    bridge = (settings.OPEN_APP_BRIDGE_URL or "").strip()
    if bridge:
        parts = urlsplit(bridge)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        if code:
            query["code"] = code
        elif token:
            query["token"] = token
        if lang:
            query["lang"] = lang
        if platform:
            query["platform"] = _normalize_target_platform(platform)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))

    base = _request_external_base_url(request)
    query = {}
    if code:
        query["code"] = code
    elif token:
        query["token"] = token
    if lang:
        query["lang"] = lang
    if platform:
        query["platform"] = _normalize_target_platform(platform)
    return f"{base}/open-app?{urlencode(query)}" if query else f"{base}/open-app"


def _detect_android_app_package(platform: Optional[str] = None) -> str:
    explicit = _selected_android_app_package(platform=platform)
    if explicit:
        return explicit
    parsed = urlsplit(_selected_platform_store_url("android"))
    if parsed.scheme and parsed.netloc:
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        package_name = str(query.get("id") or "").strip()
        if package_name:
            return package_name
    return ""


def _build_android_intent_url(native_url: str) -> str:
    parsed_native = urlsplit(native_url)
    if not parsed_native.scheme or parsed_native.scheme in {"http", "https"}:
        return ""
    android_package = _detect_android_app_package()
    if not android_package:
        return ""
    intent_path = parsed_native.path or ""
    if parsed_native.netloc:
        intent_path = f"//{parsed_native.netloc}{intent_path}"
    intent_query = f"?{parsed_native.query}" if parsed_native.query else ""
    intent_fragment = f"#{parsed_native.fragment}" if parsed_native.fragment else ""
    return (
        f"intent:{intent_path}{intent_query}{intent_fragment}"
        f"#Intent;scheme={parsed_native.scheme};package={android_package};end"
    )


def _subscription_access_context(token: str) -> Optional[Dict[str, Any]]:
    provided = str(token or "").strip()
    if not provided:
        return None
    expected = str(settings.SUBSCRIPTION_TOKEN or "").strip()
    if settings.LEGACY_GLOBAL_SUBSCRIPTION_TOKEN_ENABLED and expected and secrets.compare_digest(provided, expected):
        return {"kind": "global", "user": None}
    user = get_user_by_subscription_token(provided)
    if user:
        return {"kind": "user", "user": user}
    return None



def _subscription_token_is_active(token: Optional[str]) -> bool:
    access = _subscription_access_context(str(token or "").strip())
    if not access:
        return False
    if access["kind"] != "user":
        return True
    user = access.get("user")
    if not user or user.get("status") == "blocked":
        return False
    view = get_user_subscription_view(int(user["id"]))
    return bool(view.get("is_active") and view.get("subscription"))



def _sanitize_tracking_value(value: Optional[str], *, max_len: int = 160) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    cleaned = ''.join(ch for ch in raw if ch.isalnum() or ch in {'-', '_', '.', ':', '@'})
    return cleaned[:max_len]



def _detect_device_platform_and_name(request: Request) -> Tuple[str, str]:
    user_agent = str(request.headers.get("user-agent") or "").strip()
    ua = user_agent.lower()
    sec_platform = str(request.headers.get("sec-ch-ua-platform") or "").strip().strip('"').lower()
    platform = "client"
    if "android" in ua or sec_platform == "android":
        platform = "android"
    elif any(token in ua for token in ("iphone", "ipad", "ios")) or sec_platform in {"ios", "iphone", "ipad"}:
        platform = "ios"
    elif "windows" in ua:
        platform = "windows"
    elif any(token in ua for token in ("mac os", "macos", "darwin")):
        platform = "macos"
    elif "linux" in ua:
        platform = "linux"

    client_name = "VPN client"
    if "hiddify" in ua:
        client_name = "Hiddify"
    elif "v2raytun" in ua:
        client_name = "v2RayTun"
    elif "happ" in ua:
        client_name = "Happ"
    elif "nekobox" in ua:
        client_name = "NekoBox"
    elif "nekoray" in ua:
        client_name = "NekoRay"
    elif "sing-box" in ua or "singbox" in ua:
        client_name = "sing-box"

    platform_title = {
        "android": "Android",
        "ios": "iOS",
        "windows": "Windows",
        "macos": "macOS",
        "linux": "Linux",
    }.get(platform)
    device_name = f"{client_name} {platform_title}" if platform_title else client_name
    return platform, device_name



def _device_platform_family(platform: Optional[str]) -> str:
    normalized = str(platform or "").strip().lower()
    if normalized in {"android", "ios"}:
        return "mobile"
    if normalized in {"windows", "macos", "linux"}:
        return "desktop"
    if normalized == "client":
        return "generic"
    return normalized or "generic"



def _device_client_family(device_name: Optional[str]) -> str:
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



def _is_public_ip_address(value: Optional[str]) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    try:
        parsed = ipaddress.ip_address(raw)
    except ValueError:
        return False
    if parsed.is_private or parsed.is_loopback or parsed.is_link_local or parsed.is_reserved or parsed.is_multicast or parsed.is_unspecified:
        return False
    return True



def _client_ip_from_request(request: Request) -> str:
    for header_name in ("cf-connecting-ip", "x-real-ip", "x-forwarded-for"):
        raw = str(request.headers.get(header_name) or "").strip()
        if not raw:
            continue
        candidate = raw.split(",", 1)[0].strip()
        if _is_public_ip_address(candidate):
            return candidate
    client = getattr(request, "client", None)
    host = str(getattr(client, "host", None) or "").strip()
    if _is_public_ip_address(host):
        return host
    return ""



def _normalize_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    raw = str(value).strip().replace(',', '.')
    if not raw:
        return None
    try:
        return int(round(float(raw)))
    except ValueError:
        return None


def _normalize_optional_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    raw = str(value).strip()
    if not raw:
        return None
    normalized = raw.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _live_probe_max_age_minutes() -> int:
    return max(1, int(getattr(settings, 'SUBSCRIPTION_STRICT_FRESH_PING_MINUTES', 20) or 20))


def _row_live_probe_state(row: Dict[str, Any]) -> Dict[str, Any]:
    if not _row_has_ready_payload(row):
        return {
            'status': 'error',
            'label': 'payload incomplete',
            'text': 'vpn payload is incomplete',
            'reason': 'payload_incomplete',
            'publishable': False,
            'fresh': False,
            'ping_ms': None,
            'checked_at': None,
            'age_minutes': None,
            'fix': 'Fill server, port, uuid and transport-specific fields, then run a live tunnel probe again.',
        }
    if not _location_status_is_online(row):
        return {
            'status': 'error',
            'label': 'offline',
            'text': 'location is marked offline',
            'reason': 'offline',
            'publishable': False,
            'fresh': False,
            'ping_ms': _normalize_optional_int(row.get('ping_ms')),
            'checked_at': _normalize_optional_timestamp(row.get('speed_checked_at')),
            'age_minutes': None,
            'fix': 'Bring the node back online and rerun the live tunnel probe.',
        }
    payload = _compose_vpn_payload_for_location(dict(row)) or dict(row.get('vpn_payload') or {})
    last_probe = payload.get('_last_live_probe') if isinstance(payload, dict) else {}
    if not isinstance(last_probe, dict):
        last_probe = {}
    probe_method = str(last_probe.get('method') or '').strip().lower()
    used_tcp_fallback = probe_method == 'tcp_fallback'
    ping_ms = _normalize_optional_int(row.get('ping_ms'))
    checked_at = _normalize_optional_timestamp(row.get('speed_checked_at'))
    if checked_at is None:
        return {
            'status': 'warning',
            'label': 'no connectivity check',
            'text': 'no connectivity check has been recorded yet',
            'reason': 'never_checked',
            'publishable': False,
            'fresh': False,
            'ping_ms': ping_ms,
            'checked_at': None,
            'age_minutes': None,
            'fix': 'Run Speed-test / live validation so the backend can verify the node. By default the backend now requires a real tunnel probe and will not publish nodes that only passed a TCP reachability check.',
        }
    age = datetime.now(timezone.utc) - checked_at.astimezone(timezone.utc)
    age_minutes = int(max(0, age.total_seconds()) // 60)
    max_age_minutes = _live_probe_max_age_minutes()
    failed_label = 'tcp check failed' if used_tcp_fallback else 'live check failed'
    failed_text = 'the last tcp reachability check failed' if used_tcp_fallback else 'the last live tunnel check failed'
    stale_text = f"tcp reachability check is stale ({age_minutes} min old)" if used_tcp_fallback else f"live tunnel check is stale ({age_minutes} min old)"
    ok_label = 'tcp reachable' if used_tcp_fallback else 'live ok'
    ok_text = f'tcp reachability check passed ({ping_ms} ms)' if used_tcp_fallback else f'real tunnel check passed ({ping_ms} ms)'
    ok_reason = 'tcp_ok' if used_tcp_fallback else 'live_ok'
    failed_fix = 'Replace the node or payload and rerun the connectivity check.' if used_tcp_fallback else 'Replace the node or payload and rerun the live tunnel probe.'
    stale_fix = 'Refresh the location and rerun the connectivity check before publishing it to clients.' if used_tcp_fallback else 'Refresh the location and rerun the live tunnel probe before publishing it to clients.'
    if ping_ms is None or ping_ms <= 0:
        return {
            'status': 'error',
            'label': failed_label,
            'text': failed_text,
            'reason': 'last_probe_failed',
            'publishable': False,
            'fresh': False,
            'ping_ms': ping_ms,
            'checked_at': checked_at,
            'age_minutes': age_minutes,
            'fix': failed_fix,
        }
    if age > timedelta(minutes=max_age_minutes):
        return {
            'status': 'warning',
            'label': 'connectivity check stale' if used_tcp_fallback else 'live check stale',
            'text': stale_text,
            'reason': 'stale_probe',
            'publishable': False,
            'fresh': False,
            'ping_ms': ping_ms,
            'checked_at': checked_at,
            'age_minutes': age_minutes,
            'fix': stale_fix,
        }
    return {
        'status': 'ready',
        'label': ok_label,
        'text': ok_text,
        'reason': ok_reason,
        'publishable': True,
        'fresh': True,
        'ping_ms': ping_ms,
        'checked_at': checked_at,
        'age_minutes': age_minutes,
        'fix': '',
    }


def _subscription_browser_preview_request(request: Request) -> bool:
    if not bool(getattr(settings, 'SUBSCRIPTION_BROWSER_PREVIEW_NO_DEVICE_TRACK', True)):
        return False
    client_hint = str(request.query_params.get('client') or '').strip().lower()
    if client_hint in {'hiddify', 'v2raytun', 'happ', 'nekobox', 'nekoray', 'sing-box', 'singbox'}:
        return False
    if _subscription_client_id_from_request(request):
        return False
    ua = str(request.headers.get('user-agent') or '').strip().lower()
    if not ua:
        return False
    known_vpn_clients = ('hiddify', 'v2raytun', 'happ', 'nekobox', 'nekoray', 'sing-box', 'singbox')
    if any(marker in ua for marker in known_vpn_clients):
        return False
    browser_markers = ('mozilla/', 'chrome/', 'safari/', 'firefox/', 'edg/', 'opr/', 'opera/')
    return any(marker in ua for marker in browser_markers)


def _subscription_row_has_fresh_live_signal(row: Dict[str, Any]) -> bool:
    if not _location_status_is_online(row):
        return False
    if not _row_has_ready_payload(row):
        return False
    ping_ms = _normalize_optional_int(row.get('ping_ms'))
    if ping_ms is None or ping_ms <= 0:
        return False
    checked_at = _normalize_optional_timestamp(row.get('speed_checked_at'))
    if checked_at is None:
        return False
    max_age_minutes = max(1, int(getattr(settings, 'SUBSCRIPTION_STRICT_FRESH_PING_MINUTES', 20) or 20))
    age = datetime.now(timezone.utc) - checked_at.astimezone(timezone.utc)
    return age <= timedelta(minutes=max_age_minutes)


def _subscription_row_allowed_for_publish(row: Dict[str, Any], *, user_id: Optional[int], strict_health: bool) -> bool:
    code = str(row.get('code') or '').strip()
    if not code:
        return False
    if code in PUBLIC_VIRTUAL_LOCATION_CODES:
        picked = _pick_virtual_location(code, user_id=user_id)
        if not picked:
            return False
        return _subscription_row_has_fresh_live_signal(dict(picked)) if strict_health else _public_concrete_location_allowed(dict(picked))
    if strict_health:
        return _subscription_row_has_fresh_live_signal(row)
    return _public_concrete_location_allowed(row)


def _subscription_client_id_from_request(request: Request) -> str:
    raw_value = (
        request.query_params.get("client_id")
        or request.query_params.get("subcid")
        or request.headers.get("x-client-id")
        or request.cookies.get("inet_sub_cid")
    )
    if not raw_value:
        legacy_cid = str(request.query_params.get("cid") or "").strip()
        if legacy_cid.startswith("cid-"):
            raw_value = legacy_cid
    return _sanitize_tracking_value(raw_value, max_len=120)



def _subscription_fingerprint_source(request: Request, token: str, client_id: Optional[str] = None) -> Optional[Tuple[str, str]]:
    normalized_client_id = _sanitize_tracking_value(client_id, max_len=120)
    if normalized_client_id:
        return (f"cid:{normalized_client_id}", "client_id")
    user_agent = str(request.headers.get("user-agent") or "").strip()
    accept_language = str(request.headers.get("accept-language") or "").strip()
    sec_ch_ua = str(request.headers.get("sec-ch-ua") or "").strip()
    sec_platform = str(request.headers.get("sec-ch-ua-platform") or "").strip()
    sec_mobile = str(request.headers.get("sec-ch-ua-mobile") or "").strip()
    client_ip = _client_ip_from_request(request)
    parts = [user_agent, accept_language, sec_ch_ua, sec_platform, sec_mobile]
    normalized_parts = [part.strip() for part in parts if part and part.strip()]
    if client_ip:
        normalized_parts.append(client_ip)
    if not normalized_parts:
        return None
    return ("fallback:" + "|".join(normalized_parts), "fallback")



def _build_subscription_device_fingerprint(request: Request, token: str, client_id: Optional[str] = None) -> Optional[str]:
    fingerprint_source = _subscription_fingerprint_source(request, token, client_id)
    if not fingerprint_source:
        return None
    source, _ = fingerprint_source
    return hashlib.sha256(f"sub-device:v3:{token}:{source}".encode("utf-8")).hexdigest()



def _subscription_cookie_value(request: Request, token: str) -> str:
    current = _subscription_client_id_from_request(request)
    if current:
        return current
    ua = str(request.headers.get("user-agent") or "").strip()
    platform, _ = _detect_device_platform_and_name(request)
    seed = f"{token}|{platform}|{ua}|{uuid4().hex}"
    return "cid-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]



def _subscription_soft_gate_allow(request: Request, access: Optional[Dict[str, Any]], gate: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not gate or gate.get("allowed"):
        return gate
    if not access or access.get("kind") != "user":
        return gate
    client_hint = str(request.query_params.get("client") or "").strip().lower()
    user_agent_hint = str(request.headers.get("user-agent") or "").strip().lower()
    has_explicit_client_id = bool(_subscription_client_id_from_request(request))
    allow_soft_match_with_client_id = (
        client_hint in {"v2raytun", "hiddify"}
        or "v2raytun" in user_agent_hint
        or "hiddify" in user_agent_hint
    )
    if has_explicit_client_id and not allow_soft_match_with_client_id:
        return gate
    used = int(gate.get("devices_used") or 0)
    limit = int(gate.get("device_limit") or 0)
    if limit <= 0 or used < limit:
        return gate
    platform, device_name = _detect_device_platform_and_name(request)
    user = access.get("user") or {}
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        return gate
    try:
        view = get_user_subscription_view(user_id)
    except Exception:
        return gate
    devices = list(view.get("devices") or [])
    normalized_platform = str(platform or "").strip().lower()
    normalized_name = str(device_name or "").strip()
    platform_family = _device_platform_family(normalized_platform)
    client_family = _device_client_family(normalized_name)

    exact_matches = [
        item
        for item in devices
        if str(item.get("platform") or "").strip().lower() == normalized_platform
        and str(item.get("device_name") or "").strip() == normalized_name
    ]
    if len(exact_matches) == 1:
        relaxed = dict(gate)
        relaxed["allowed"] = True
        relaxed["known_device"] = True
        relaxed["reason"] = "platform_device_soft_match"
        relaxed["detail"] = f"Subscription refresh allowed for existing {normalized_name or normalized_platform or 'device'} slot"
        return relaxed

    same_platform_matches = [
        item
        for item in devices
        if str(item.get("platform") or "").strip().lower() == normalized_platform
    ]
    if platform_family == "desktop" and len(same_platform_matches) == 1:
        relaxed = dict(gate)
        relaxed["allowed"] = True
        relaxed["known_device"] = True
        relaxed["reason"] = "platform_soft_match"
        relaxed["detail"] = f"Subscription refresh allowed for existing {normalized_platform or 'device'} slot"
        return relaxed

    same_family_matches = []
    for item in devices:
        item_platform = str(item.get("platform") or "").strip().lower()
        item_name = str(item.get("device_name") or "").strip()
        item_family = _device_platform_family(item_platform)
        item_client_family = _device_client_family(item_name)
        same_family = item_family == platform_family
        compatible_client = client_family == item_client_family or "generic" in {client_family, item_client_family}
        if same_family and compatible_client:
            same_family_matches.append(item)
    if platform_family != "mobile" and len(same_family_matches) == 1:
        relaxed = dict(gate)
        relaxed["allowed"] = True
        relaxed["known_device"] = True
        relaxed["reason"] = "platform_family_soft_match"
        relaxed["detail"] = f"Subscription refresh allowed for existing {platform_family or normalized_platform or 'device'} slot"
        return relaxed

    if len(devices) == 1:
        only_item = devices[0]
        only_platform = str(only_item.get("platform") or "").strip().lower()
        only_name = str(only_item.get("device_name") or "").strip()
        only_family = _device_platform_family(only_platform)
        only_client_family = _device_client_family(only_name)
        compatible_family = only_family == platform_family or "generic" in {only_family, platform_family}
        compatible_client = client_family == only_client_family or "generic" in {client_family, only_client_family}
        if compatible_family and compatible_client:
            relaxed = dict(gate)
            relaxed["allowed"] = True
            relaxed["known_device"] = True
            relaxed["reason"] = "single_slot_soft_match"
            relaxed["detail"] = "Subscription refresh allowed for existing single device slot"
            return relaxed

    return gate



def _track_subscription_device_access(request: Request, token: str, access: Optional[Dict[str, Any]]) -> None:
    if not access or access.get("kind") != "user":
        return
    user = access.get("user") or {}
    if not user or user.get("status") == "blocked":
        return
    client_id = _subscription_client_id_from_request(request)
    fingerprint = _build_subscription_device_fingerprint(request, token, client_id)
    if not fingerprint:
        return
    platform, device_name = _detect_device_platform_and_name(request)
    try:
        touch_subscription_device_by_token(token, platform=platform, device_name=device_name, device_fingerprint=fingerprint)
    except Exception:
        pass



def _subscription_device_gate(request: Request, token: str, access: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not access or access.get("kind") != "user":
        return None
    user = access.get("user") or {}
    if not user or user.get("status") == "blocked":
        return None
    client_id = _subscription_client_id_from_request(request)
    fingerprint = _build_subscription_device_fingerprint(request, token, client_id)
    if not fingerprint:
        return None
    try:
        gate = get_subscription_device_gate_by_token(token, fingerprint)
    except Exception:
        return None
    return _subscription_soft_gate_allow(request, access, gate)



def _subscription_remark_for_row(row: Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> str:
    normalized = _normalize_vpn_payload_keys(payload or {}) if payload else {}
    base_name = _subscription_target_name_for_row(row, normalized)
    if bool(row.get("is_recommended")) and " ★ " not in base_name and not base_name.startswith(("★ ", "⭐ ")):
        icon = _subscription_icon_for_row(row, normalized)
        if icon and base_name.startswith(f"{icon} "):
            return f"{icon} ★ {base_name[len(icon):].strip()}"
        return f"★ {base_name}"
    return base_name


def _build_vless_subscription_line(payload: Dict[str, Any], *, fallback_name: str = "VLESS") -> str:
    normalized = _normalize_vpn_payload_keys(payload)
    if not normalized or not _config_is_complete(normalized):
        raise ValueError("VLESS payload is incomplete")

    uuid = str(normalized.get("uuid") or "").strip()
    server = str(normalized.get("server") or "").strip()
    port = int(normalized.get("port") or 443)
    remark = str(normalized.get("remark") or normalized.get("display_name") or fallback_name).strip() or fallback_name

    query: Dict[str, Any] = {
        "type": str(normalized.get("transport") or normalized.get("network") or "tcp").strip() or "tcp",
        "security": str(normalized.get("security") or "reality").strip() or "reality",
        "encryption": str(normalized.get("encryption") or "none").strip() or "none",
    }
    transport = str(normalized.get("transport") or normalized.get("network") or "tcp").strip().lower() or "tcp"
    optional_keys = {
        "flow": normalized.get("flow"),
        "sni": normalized.get("sni") or normalized.get("server_name"),
        "host": normalized.get("host"),
        "path": normalized.get("path"),
        "serviceName": normalized.get("service_name") or normalized.get("serviceName"),
        "mode": normalized.get("mode") or ("gun" if transport == "grpc" else ("auto" if transport == "xhttp" else None)),
        "pbk": normalized.get("public_key") or normalized.get("publicKey"),
        "sid": normalized.get("short_id") or normalized.get("shortId"),
        "fp": normalized.get("fingerprint"),
        "alpn": ",".join(str(item).strip() for item in (normalized.get("alpn") or []) if str(item).strip()),
        "packetEncoding": normalized.get("packet_encoding") or normalized.get("packetEncoding"),
        "connectMode": normalized.get("connect_mode") or normalized.get("connectMode"),
        "fullTunnel": "1" if bool(normalized.get("full_tunnel", True)) else "0",
    }
    for key, value in optional_keys.items():
        text = str(value or "").strip()
        if text:
            query[key] = text

    return f"vless://{quote(uuid, safe='')}@{server}:{port}?{urlencode(query)}#{quote(remark, safe='')}"



def _hiddify_subscription_transport_allowed(payload: Dict[str, Any]) -> bool:
    allowed = {item.strip().lower() for item in (settings.HIDDIFY_SUBSCRIPTION_ALLOWED_TRANSPORTS or ["grpc", "tcp", "ws"]) if str(item or "").strip()}
    transport = str(payload.get("transport") or payload.get("network") or "tcp").strip().lower()
    return transport in allowed if allowed else True


def _subscription_payload_and_fallback_name(row: Dict[str, Any], *, user_id: Optional[int] = None) -> Tuple[Optional[Dict[str, Any]], str]:
    code = str(row.get("code") or "").strip()
    resolved_row = dict(row)
    if code in PUBLIC_VIRTUAL_LOCATION_CODES:
        picked = _pick_virtual_location(code, user_id=user_id)
        if not picked:
            return None, str(row.get("name_en") or row.get("name_ru") or code or "VLESS")
        resolved_row = dict(picked)

    if user_id is not None:
        payload = build_user_vpn_payload_for_location(int(user_id), resolved_row, requested_location_code=code or None)
    else:
        payload = _compose_vpn_payload_for_location(resolved_row, requested_location_code=code or None)
    if not payload:
        return None, str(row.get("name_en") or row.get("name_ru") or code or "VLESS")

    payload_for_subscription = dict(payload)
    if code in PUBLIC_VIRTUAL_LOCATION_CODES:
        virtual_name = str(row.get("name_en") or row.get("name_ru") or code or "VLESS").strip() or "VLESS"
        resolved_name = _subscription_target_name_for_row(resolved_row, payload_for_subscription)
        virtual_remark = f"{virtual_name} → {resolved_name}"
        payload_for_subscription["remark"] = virtual_remark
        payload_for_subscription["display_name"] = virtual_remark
        decorated_virtual_remark = _subscription_remark_for_row(row, payload_for_subscription)
        payload_for_subscription["remark"] = decorated_virtual_remark
        payload_for_subscription["display_name"] = decorated_virtual_remark
    else:
        payload_for_subscription["remark"] = _subscription_remark_for_row(row, payload_for_subscription)
        payload_for_subscription["display_name"] = payload_for_subscription["remark"]

    fallback_name = _subscription_remark_for_row(row, payload_for_subscription)
    return payload_for_subscription, fallback_name


def _subscription_location_rows(user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    public_rows = _public_location_rows()

    def collect(*, strict_health: bool) -> List[Dict[str, Any]]:
        published: List[Dict[str, Any]] = []
        for row in public_rows:
            base_row = dict(row)
            if not _subscription_row_allowed_for_publish(base_row, user_id=user_id, strict_health=strict_health):
                continue
            payload, _ = _subscription_payload_and_fallback_name(base_row, user_id=user_id)
            if payload and _hiddify_subscription_transport_allowed(payload):
                published.append(base_row)
        return published

    strict_rows = collect(strict_health=True)
    if strict_rows:
        return strict_rows
    if bool(getattr(settings, "SUBSCRIPTION_ALLOW_READY_FALLBACK", False)):
        return collect(strict_health=False)
    logger.warning("[sub][publish] strict_live_only=1 fallback_ready_disabled=1 published=0 user_id=%s", user_id if user_id is not None else "-")
    return []


def _hiddify_profile_update_interval_header_value() -> str:
    raw_value = getattr(settings, "HIDDIFY_PROFILE_UPDATE_INTERVAL_HOURS", 1.0)
    try:
        numeric = float(raw_value)
    except (TypeError, ValueError):
        numeric = 1.0
    if numeric <= 0:
        numeric = 1.0
    normalized_hours = int(numeric + 0.5)
    if normalized_hours < 1:
        normalized_hours = 1
    return str(normalized_hours)


def _subscription_cache_buster_url(request: Optional[Request], token: str, content_version: str) -> Optional[str]:
    base_url = _subscription_public_url(request, token=token)
    if not base_url:
        return None
    version = str(content_version or "").strip()
    if not version:
        return base_url
    parts = urlsplit(base_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["v"] = version[:16]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _subscription_inline_comment_headers(
    *,
    profile_title: str,
    update_interval_hours: str,
    subscription_userinfo: str,
    support_url: str,
    profile_web_page_url: str,
    moved_permanently_to_url: Optional[str],
    content_version: str,
) -> List[str]:
    title_b64 = base64.b64encode(profile_title.encode("utf-8")).decode("ascii")
    lines: List[str] = [
        f"#profile-title: base64:{title_b64}",
        f"#profile-update-interval: {update_interval_hours}",
        f"#subscription-userinfo: {subscription_userinfo}",
    ]
    if support_url:
        lines.append(f"#support-url: {support_url}")
    if profile_web_page_url:
        lines.append(f"#profile-web-page-url: {profile_web_page_url}")
    if moved_permanently_to_url:
        lines.append(f"#moved-permanently-to: {moved_permanently_to_url}")
    version = str(content_version or "").strip()
    if version:
        lines.append(f"#x-subscription-version: {version[:16]}")
    return lines[:10]


def _http_date_from_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def _subscription_inactive_response(request: Request, token: str, *, head_only: bool = False, expires_ts: Optional[int] = None) -> Response:
    bot_public_url = str(_bot_public_url() or "").strip()
    support_url = bot_public_url or str(getattr(settings, "SUPPORT_TELEGRAM_URL", "") or "").strip()
    fallback_web_url = str(getattr(settings, "APP_BASE_URL", "") or getattr(settings, "ADMIN_PANEL_BASE_URL", "") or "").strip()
    profile_web_page_url = support_url or fallback_web_url
    expired_ts = int(expires_ts or (time.time() - 300))
    profile_title = f"{settings.APP_NAME} · subscription expired"
    message_lines = [
        "# subscription-state: expired",
        "# access: inactive",
        "# message: Subscription expired. Buy a new subscription in the Telegram bot.",
        "# message-ru: Подписка истекла. Купите новую подписку в Telegram-боте.",
    ]
    if support_url:
        message_lines.append(f"# buy-subscription-url: {support_url}")
    update_interval_hours = _hiddify_profile_update_interval_header_value()
    content = "\n".join(message_lines) + "\n"
    content_version = hashlib.sha256(content.encode("utf-8")).hexdigest()
    moved_permanently_to_url = _subscription_cache_buster_url(request, token, content_version)
    inline_headers = _subscription_inline_comment_headers(
        profile_title=profile_title,
        update_interval_hours=update_interval_hours,
        subscription_userinfo=f"upload=0; download=0; total=0; expire={expired_ts}",
        support_url=support_url,
        profile_web_page_url=profile_web_page_url,
        moved_permanently_to_url=moved_permanently_to_url,
        content_version=content_version,
    )
    client_hint = str(request.query_params.get("client") or "").strip().lower()
    user_agent_hint = str(request.headers.get("user-agent") or "").strip().lower()
    suppress_inline_headers = client_hint == "v2raytun" or "v2raytun" in user_agent_hint
    rendered_content = content if suppress_inline_headers else "\n".join(inline_headers + message_lines) + "\n"
    response_etag = hashlib.sha256(rendered_content.encode("utf-8")).hexdigest()
    now_utc = datetime.now(timezone.utc)
    headers = {
        "Content-Disposition": 'inline; filename="inet-subscription.txt"',
        "Profile-Title": quote(f"base64:{base64.b64encode(profile_title.encode('utf-8')).decode('ascii')}", safe=':='),
        "Subscription-Userinfo": f"upload=0; download=0; total=0; expire={expired_ts}",
        "Cache-Control": "private, no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0, s-maxage=0",
        "CDN-Cache-Control": "no-store, no-cache, max-age=0",
        "Cloudflare-CDN-Cache-Control": "no-store, no-cache, max-age=0",
        "Surrogate-Control": "no-store",
        "Pragma": "no-cache",
        "Expires": "0",
        "ETag": f'W/"{response_etag}"',
        "Last-Modified": _http_date_from_datetime(now_utc) or "",
        "Vary": "*",
        "X-Accel-Expires": "0",
        "profile-update-interval": update_interval_hours,
        "support-url": support_url,
        "profile-web-page-url": profile_web_page_url,
        "moved-permanently-to": moved_permanently_to_url or "",
        "x-hiddify-source": "subscription",
        "x-subscription-version": content_version,
        "x-subscription-generated-at": now_utc.isoformat(),
        "x-subscription-state": "expired",
    }
    return Response(status_code=200, headers=headers) if head_only else Response(content=rendered_content, media_type="text/plain; charset=utf-8", headers=headers)


def _subscription_response(request: Request, token: str, *, head_only: bool = False) -> Response:
    access = _subscription_access_context(token)
    if not access:
        raise HTTPException(status_code=404, detail="Subscription not found")

    expires_ts = int(time.time()) + 86400 * 3650
    profile_title = f"{settings.APP_NAME} · {_bot_profile_title_label()}"
    subscription_user_id: Optional[int] = None
    if access["kind"] == "user":
        user = access["user"]
        if not user or user.get("status") == "blocked":
            raise HTTPException(status_code=404, detail="Subscription not found")
        view = get_user_subscription_view(int(user["id"]))
        subscription = view.get("subscription") or {}
        expires_at = subscription.get("expires_at")
        if isinstance(expires_at, datetime):
            expires_ts = int(expires_at.timestamp())
        elif isinstance(expires_at, str):
            try:
                expires_ts = int(datetime.fromisoformat(expires_at.replace("Z", "+00:00")).timestamp())
            except Exception:
                pass
        if not view.get("is_active") or not subscription:
            return _subscription_inactive_response(request, token, head_only=head_only, expires_ts=expires_ts)
        profile_title = f"{settings.APP_NAME} · {_bot_profile_title_label()}"
        subscription_user_id = int(user["id"])

        is_browser_preview = _subscription_browser_preview_request(request)
        if not is_browser_preview:
            gate = _subscription_device_gate(request, token, access)
            if gate and not gate.get("allowed"):
                used = int(gate.get("devices_used") or 0)
                limit = int(gate.get("device_limit") or 0)
                content = (
                    f"Device limit reached ({used}/{limit}). Remove one device in the bot or admin panel and try again.\n"
                    f"Лимит устройств исчерпан ({used}/{limit}). Удалите одно устройство в боте или админке и попробуйте снова.\n"
                )
                return Response(content=content, status_code=403, media_type="text/plain; charset=utf-8")

    if not head_only and not _subscription_browser_preview_request(request):
        _track_subscription_device_access(request, token, access)

    rows = _subscription_location_rows(subscription_user_id)
    lines: List[str] = []
    for row in rows:
        payload_for_subscription, fallback_name = _subscription_payload_and_fallback_name(dict(row), user_id=subscription_user_id)
        if not payload_for_subscription:
            continue
        try:
            lines.append(_build_vless_subscription_line(payload_for_subscription, fallback_name=fallback_name))
        except Exception:
            continue

    if not lines:
        raise HTTPException(status_code=503, detail="No ready VLESS locations found for subscription")

    content = "\n".join(lines) + "\n"
    content_version = hashlib.sha256(content.encode("utf-8")).hexdigest()
    last_modified_dt: Optional[datetime] = None
    for row in rows:
        raw_updated = row.get("updated_at")
        parsed: Optional[datetime] = None
        if isinstance(raw_updated, datetime):
            parsed = raw_updated
        elif isinstance(raw_updated, str):
            try:
                parsed = datetime.fromisoformat(raw_updated.replace("Z", "+00:00"))
            except Exception:
                parsed = None
        if parsed is not None:
            if last_modified_dt is None or parsed > last_modified_dt:
                last_modified_dt = parsed
    if last_modified_dt is None:
        last_modified_dt = datetime.now(timezone.utc)
    bot_public_url = str(_bot_public_url() or "").strip()
    profile_web_page_url = bot_public_url or str(getattr(settings, "ADMIN_PANEL_BASE_URL", "") or getattr(settings, "APP_BASE_URL", "") or "").strip()
    support_url = bot_public_url or str(getattr(settings, "SUPPORT_TELEGRAM_URL", "") or "").strip()
    update_interval_hours = _hiddify_profile_update_interval_header_value()
    subscription_userinfo = f"upload=0; download=0; total=0; expire={expires_ts}"
    moved_permanently_to_url = _subscription_cache_buster_url(request, token, content_version)
    inline_headers = _subscription_inline_comment_headers(
        profile_title=profile_title,
        update_interval_hours=update_interval_hours,
        subscription_userinfo=subscription_userinfo,
        support_url=support_url,
        profile_web_page_url=profile_web_page_url,
        moved_permanently_to_url=moved_permanently_to_url,
        content_version=content_version,
    )
    client_hint = str(request.query_params.get("client") or "").strip().lower()
    user_agent_hint = str(request.headers.get("user-agent") or "").strip().lower()
    suppress_inline_headers = client_hint == "v2raytun" or "v2raytun" in user_agent_hint
    content = "\n".join(lines) + "\n" if suppress_inline_headers else "\n".join(inline_headers + lines) + "\n"
    response_etag = hashlib.sha256(content.encode("utf-8")).hexdigest()
    headers = {
        "Content-Disposition": 'inline; filename="inet-subscription.txt"',
        "Profile-Title": quote(f"base64:{base64.b64encode(profile_title.encode('utf-8')).decode('ascii')}", safe=':='),
        "Subscription-Userinfo": subscription_userinfo,
        "Cache-Control": "private, no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0, s-maxage=0",
        "CDN-Cache-Control": "no-store, no-cache, max-age=0",
        "Cloudflare-CDN-Cache-Control": "no-store, no-cache, max-age=0",
        "Surrogate-Control": "no-store",
        "Pragma": "no-cache",
        "Expires": "0",
        "ETag": f'W/"{response_etag}"',
        "Last-Modified": _http_date_from_datetime(last_modified_dt) or "",
        "Vary": "*",
        "X-Accel-Expires": "0",
        "profile-update-interval": update_interval_hours,
        "support-url": support_url,
        "profile-web-page-url": profile_web_page_url,
        "moved-permanently-to": moved_permanently_to_url or "",
        "x-hiddify-source": "subscription",
        "x-subscription-version": content_version,
        "x-subscription-generated-at": datetime.now(timezone.utc).isoformat(),
    }
    response = Response(status_code=200, headers=headers) if head_only else Response(content=content, media_type="text/plain; charset=utf-8", headers=headers)
    try:
        response.set_cookie(
            key="inet_sub_cid",
            value=_subscription_cookie_value(request, token),
            max_age=31536000,
            httponly=False,
            samesite="lax",
            secure=_request_is_https(request),
        )
    except Exception:
        pass
    return response


@app.get("/sub/{token}")
def public_subscription(request: Request, token: str, cid: Optional[str] = Query(default=None)) -> Response:
    del cid
    return _subscription_response(request, token, head_only=False)


@app.head("/sub/{token}")
def public_subscription_head(request: Request, token: str, cid: Optional[str] = Query(default=None)) -> Response:
    del cid
    return _subscription_response(request, token, head_only=True)


@app.get("/sub")
def public_subscription_hint() -> Dict[str, Any]:
    return {
        "ok": True,
        "message": "Use /sub/<personal_subscription_token>",
    }


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/open-app", response_class=HTMLResponse, name="open_app_bridge")
def open_app_bridge(
    request: Request,
    code: Optional[str] = Query(default=None),
    token: Optional[str] = Query(default=None),
    lang: str = Query(default="ru"),
    platform: Optional[str] = Query(default=None),
) -> HTMLResponse:
    norm_lang = "en" if lang == "en" else "ru"
    detected_platform, _ = _detect_device_platform_and_name(request)
    target_platform = _normalize_target_platform(platform or detected_platform)
    client_mode = _client_mode_for_platform(target_platform)
    client_name = _client_name_for_platform(target_platform)
    resolved_token = _resolve_subscription_token(token=token, code=code)
    active_token = resolved_token if _subscription_token_is_active(resolved_token) else None
    subscription_client_id = _subscription_cookie_value(request, active_token) if active_token else ""
    subscription_url = _subscription_public_url(request, token=active_token) if active_token else None
    tracked_subscription_url = None
    if subscription_url and subscription_client_id:
        try:
            parts = urlsplit(subscription_url)
            query = dict(parse_qsl(parts.query, keep_blank_values=True))
            query["client_id"] = subscription_client_id
            if client_mode:
                query["client"] = client_mode
            tracked_subscription_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
        except Exception:
            tracked_subscription_url = subscription_url
    elif subscription_url:
        tracked_subscription_url = subscription_url
    native_url = _build_native_import_url(tracked_subscription_url, platform=target_platform) if tracked_subscription_url else ""
    supports_native_launch = bool(native_url)
    bot_url = _bot_public_url()
    page_text = {
        "ru": {
            "title": f"Подключение через {client_name}",
            "headline": f"Подключение через {client_name}",
            "body_auto": f"{client_name} должен открыться автоматически и импортировать вашу персональную подписку. Если этого не произошло, используйте кнопки ниже.",
            "body_manual": f"Для {client_name} на этой платформе используйте ручной импорт: скопируйте персональную ссылку ниже и добавьте её в приложение как Subscription / URL. Токен уже зашит в ссылку.",
            "invalid_link": "Ссылка недействительна или срок доступа истёк. Вернитесь в бота и купите подписку.",
            "open_button": f"Открыть в {client_name}",
            "copy_button": f"Скопировать ссылку для {client_name}",
            "android_button": f"Скачать {_client_name_for_platform('android')} для Android",
            "ios_button": f"Скачать {_client_name_for_platform('ios')} для iPhone / iPad",
            "windows_button": "Скачать Happ для Windows",
            "macos_button": "Скачать Happ для macOS",
            "copy_label": "Персональная ссылка подписки",
            "copy_done": "Ссылка подписки скопирована.",
            "copy_failed": "Не удалось скопировать автоматически. Скопируйте ссылку ниже вручную.",
            "bot_button": "Вернуться в бота",
        },
        "en": {
            "title": f"Connect with {client_name}",
            "headline": f"Connect with {client_name}",
            "body_auto": f"{client_name} should open automatically and import your personal subscription. If it does not, use the buttons below.",
            "body_manual": f"On this platform, use manual import for {client_name}: copy the personal subscription link below and add it inside the app as Subscription / URL. The token is already embedded in the link.",
            "invalid_link": "This link is invalid or access has expired. Return to the bot and buy a subscription.",
            "open_button": f"Open in {client_name}",
            "copy_button": f"Copy link for {client_name}",
            "android_button": f"Download {_client_name_for_platform('android')} for Android",
            "ios_button": f"Download {_client_name_for_platform('ios')} for iPhone / iPad",
            "windows_button": "Download Happ for Windows",
            "macos_button": "Download Happ for macOS",
            "copy_label": "Personal subscription link",
            "copy_done": "Subscription link copied.",
            "copy_failed": "Could not copy automatically. Copy the link below manually.",
            "bot_button": "Back to bot",
        },
    }[norm_lang]
    display_subscription_url = tracked_subscription_url or subscription_url
    copy_block = ""
    if display_subscription_url:
        copy_block = f'<div class="code"><div class="label">{html.escape(page_text["copy_label"])}</div><code id="subscription-link-value">{html.escape(display_subscription_url)}</code></div>'
    native_url_attr = html.escape(native_url or display_subscription_url or "", quote=True)
    native_url_js = json.dumps(native_url)
    subscription_url_js = json.dumps(display_subscription_url or "")
    client_mode_js = json.dumps(client_mode)
    supports_native_launch_js = json.dumps(supports_native_launch)
    copy_done_js = json.dumps(page_text["copy_done"])
    copy_failed_js = json.dumps(page_text["copy_failed"])
    import_name_js = json.dumps(str(getattr(settings, "HIDDIFY_IMPORT_NAME", "") or f"{settings.APP_NAME} Subscription").strip())
    android_intent_url = _build_android_intent_url(native_url)
    android_intent_url_js = json.dumps(android_intent_url)
    selected_android_url = _selected_platform_store_url("android")
    selected_ios_url = _selected_platform_store_url("ios")
    selected_windows_url = _selected_platform_store_url("windows")
    selected_macos_url = _selected_platform_store_url("macos")
    android_url = html.escape(selected_android_url, quote=True)
    android_url_js = json.dumps(selected_android_url)
    ios_url = html.escape(selected_ios_url, quote=True)
    ios_url_js = json.dumps(selected_ios_url)
    windows_url = html.escape(selected_windows_url, quote=True)
    macos_url = html.escape(selected_macos_url, quote=True)
    bot_url_attr = html.escape(bot_url, quote=True)
    title = html.escape(page_text["title"])
    headline = html.escape(page_text["headline"])
    body_value = (page_text["body_auto"] if supports_native_launch else page_text["body_manual"]) if subscription_url else page_text["invalid_link"]
    body = html.escape(body_value)
    open_button = html.escape(page_text["open_button"] if supports_native_launch else page_text["copy_button"])
    android_button = html.escape(page_text["android_button"])
    ios_button = html.escape(page_text["ios_button"])
    windows_button = html.escape(page_text["windows_button"])
    macos_button = html.escape(page_text["macos_button"])
    bot_button = html.escape(page_text["bot_button"])
    html_page = f"""<!doctype html>
<html lang="{norm_lang}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{ color-scheme: dark; }}
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0b1220; color: #f3f4f6; }}
    .wrap {{ max-width: 560px; margin: 0 auto; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; box-sizing: border-box; }}
    .card {{ width: 100%; background: #111827; border: 1px solid #1f2937; border-radius: 24px; padding: 24px; box-sizing: border-box; box-shadow: 0 10px 30px rgba(0,0,0,.35); }}
    h1 {{ margin: 0 0 12px; font-size: 28px; line-height: 1.2; }}
    p {{ margin: 0 0 20px; color: #cbd5e1; font-size: 16px; line-height: 1.5; }}
    .actions {{ display: grid; gap: 12px; }}
    .btn {{ display: block; text-align: center; text-decoration: none; padding: 14px 16px; border-radius: 14px; font-weight: 700; }}
    .btn-primary {{ background: #22c55e; color: #04130a; }}
    .btn-secondary {{ background: #1f2937; color: #f3f4f6; }}
    .code {{ margin: 0 0 20px; padding: 16px; border-radius: 16px; background: #0f172a; border: 1px solid #1e293b; }}
    .label {{ font-size: 13px; color: #94a3b8; margin-bottom: 8px; }}
    code {{ display: block; font-size: 15px; font-weight: 700; word-break: break-all; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>{headline}</h1>
      <p>{body}</p>
      {copy_block}
      <div class="actions">
        <a class="btn btn-primary" href="{native_url_attr}">{open_button}</a>
        <a class="btn btn-secondary" href="{android_url}">{android_button}</a>
        <a class="btn btn-secondary" href="{ios_url}">{ios_button}</a>
        <a class="btn btn-secondary" href="{windows_url}">{windows_button}</a>
        <a class="btn btn-secondary" href="{macos_url}">{macos_button}</a>
        <a class="btn btn-secondary" href="{bot_url_attr}">{bot_button}</a>
      </div>
    </div>
  </div>
  <script>
    (function () {{
      const initialNativeUrl = {native_url_js};
      const baseSubscriptionUrl = {subscription_url_js};
      const clientMode = {client_mode_js};
      const supportsNativeLaunch = {supports_native_launch_js};
      const importName = {import_name_js};
      const copyDoneText = {copy_done_js};
      const copyFailedText = {copy_failed_js};
      const androidStoreUrl = {android_url_js};
      const iosStoreUrl = {ios_url_js};
      const primaryButton = document.querySelector('.btn-primary');
      const buttonDefaultText = primaryButton ? primaryButton.textContent : '';
      const subscriptionCode = document.getElementById('subscription-link-value');
      const userAgent = navigator.userAgent || '';
      const isAndroid = /Android/i.test(userAgent);
      const isIOS = /iPhone|iPad|iPod/i.test(userAgent);
      let hiddenAt = 0;

      const markHidden = function () {{
        hiddenAt = Date.now();
      }};
      document.addEventListener('visibilitychange', function () {{
        if (document.visibilityState === 'hidden') {{
          markHidden();
        }}
      }});
      window.addEventListener('pagehide', markHidden);
      window.addEventListener('blur', markHidden);

      const getClientId = function () {{
        const key = 'inet-subscription-client-id';
        try {{
          if (baseSubscriptionUrl) {{
            const parsed = new URL(baseSubscriptionUrl);
            const fromUrl = parsed.searchParams.get('client_id');
            if (fromUrl) {{
              try {{ window.localStorage.setItem(key, fromUrl); }} catch (storageError) {{}}
              return fromUrl;
            }}
          }}
        }} catch (error) {{}}
        try {{
          const cookieMatch = document.cookie.match(/(?:^|; )inet_sub_cid=([^;]+)/);
          if (cookieMatch && cookieMatch[1]) {{
            const fromCookie = decodeURIComponent(cookieMatch[1]);
            if (fromCookie) {{
              try {{ window.localStorage.setItem(key, fromCookie); }} catch (storageError) {{}}
              return fromCookie;
            }}
          }}
        }} catch (error) {{}}
        try {{
          let existing = window.localStorage.getItem(key);
          if (existing) return existing;
          const generated = 'cid-' + Math.random().toString(36).slice(2) + Date.now().toString(36);
          window.localStorage.setItem(key, generated);
          return generated;
        }} catch (error) {{
          return 'cid-' + Date.now().toString(36);
        }}
      }};

      const buildTrackedSubscriptionUrl = function () {{
        if (!baseSubscriptionUrl) return '';
        try {{
          const url = new URL(baseSubscriptionUrl);
          url.searchParams.set('client_id', getClientId());
          return url.toString();
        }} catch (error) {{
          return baseSubscriptionUrl;
        }}
      }};

      const buildNativeUrl = function () {{
        const trackedSubscriptionUrl = buildTrackedSubscriptionUrl();
        if (!trackedSubscriptionUrl) return initialNativeUrl || '';
        if (clientMode === 'happ') {{
          return '';
        }}
        if (clientMode === 'v2raytun') {{
          return 'v2raytun://import/' + encodeURIComponent(trackedSubscriptionUrl);
        }}
        return 'hiddify://import/' + encodeURIComponent(trackedSubscriptionUrl) + '#' + encodeURIComponent(importName || 'Subscription');
      }};

      const currentNativeUrl = function () {{
        return buildNativeUrl() || initialNativeUrl || '';
      }};

      const currentAndroidIntentUrl = function () {{
        const nativeUrl = currentNativeUrl();
        if (!nativeUrl || !isAndroid) return '';
        const packageId = {json.dumps(_detect_android_app_package(platform=target_platform))};
        if (!packageId) return '';
        try {{
          const parsed = new URL(nativeUrl);
          const path = (parsed.host ? '//' + parsed.host : '') + (parsed.pathname || '');
          const query = parsed.search || '';
          const hash = parsed.hash || '';
          return 'intent:' + path + query + hash + '#Intent;scheme=' + parsed.protocol.replace(':', '') + ';package=' + packageId + ';end';
        }} catch (error) {{
          return '';
        }}
      }};

      const launchByAnchor = function (url) {{
        if (!url) return;
        const link = document.createElement('a');
        link.href = url;
        link.style.display = 'none';
        document.body.appendChild(link);
        link.click();
        window.setTimeout(function () {{
          link.remove();
        }}, 250);
      }};

      const copySubscriptionLink = async function () {{
        const trackedSubscriptionUrl = buildTrackedSubscriptionUrl();
        if (!trackedSubscriptionUrl) return false;
        try {{
          if (navigator.clipboard && navigator.clipboard.writeText) {{
            await navigator.clipboard.writeText(trackedSubscriptionUrl);
          }} else {{
            const temp = document.createElement('textarea');
            temp.value = trackedSubscriptionUrl;
            temp.setAttribute('readonly', 'readonly');
            temp.style.position = 'fixed';
            temp.style.opacity = '0';
            document.body.appendChild(temp);
            temp.focus();
            temp.select();
            document.execCommand('copy');
            temp.remove();
          }}
          if (primaryButton) primaryButton.textContent = copyDoneText;
          window.setTimeout(function () {{
            if (primaryButton) primaryButton.textContent = buttonDefaultText;
          }}, 1600);
          return true;
        }} catch (error) {{
          if (primaryButton) primaryButton.textContent = copyFailedText;
          window.setTimeout(function () {{
            if (primaryButton) primaryButton.textContent = buttonDefaultText;
          }}, 1800);
          return false;
        }}
      }};

      const openStoreIfNeeded = function () {{
        if (Date.now() - hiddenAt < 1200) {{
          return;
        }}
        const storeUrl = isAndroid ? androidStoreUrl : (isIOS ? iosStoreUrl : '');
        if (storeUrl) {{
          window.location.href = storeUrl;
        }}
      }};

      const tryOpen = function () {{
        if (!supportsNativeLaunch) {{
          copySubscriptionLink();
          return;
        }}
        const nativeUrl = currentNativeUrl();
        if (!nativeUrl) return;
        if (primaryButton) {{
          primaryButton.href = nativeUrl;
        }}
        const androidIntentUrl = currentAndroidIntentUrl();
        if (isAndroid && androidIntentUrl) {{
          launchByAnchor(androidIntentUrl);
          window.setTimeout(function () {{
            if (Date.now() - hiddenAt < 1200) {{
              return;
            }}
            launchByAnchor(nativeUrl);
          }}, 250);
          window.setTimeout(openStoreIfNeeded, 1800);
          return;
        }}

        launchByAnchor(nativeUrl);
        window.setTimeout(function () {{
          if (Date.now() - hiddenAt < 1200) {{
            return;
          }}
          window.location.href = nativeUrl;
        }}, 180);
        window.setTimeout(openStoreIfNeeded, 1800);
      }};

      if (subscriptionCode) {{
        subscriptionCode.textContent = buildTrackedSubscriptionUrl() || subscriptionCode.textContent || '';
      }}
      if (primaryButton) {{
        primaryButton.href = currentNativeUrl() || '#';
      }}
      if (supportsNativeLaunch) {{
        window.setTimeout(tryOpen, 80);
      }}
      primaryButton?.addEventListener('click', function (event) {{
        event.preventDefault();
        tryOpen();
      }});
    }})();
  </script>
</body>
</html>"""
    response = HTMLResponse(content=html_page)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    if active_token and subscription_client_id:
        try:
            response.set_cookie(
                key="inet_sub_cid",
                value=subscription_client_id,
                max_age=31536000,
                httponly=False,
                samesite="lax",
                secure=_request_is_https(request),
            )
        except Exception:
            pass
    return response


@app.get("/robots.txt", include_in_schema=False)
def robots_txt() -> Response:
    response = Response(content="User-agent: *\nAllow: /\n", media_type="text/plain")
    response.headers["Cache-Control"] = "public, max-age=3600"
    return response

@app.get("/health")
def health() -> Dict[str, Any]:
    locations = _public_locations_health_snapshot()
    return {
        "ok": bool(locations.get("healthy")),
        "service": settings.APP_NAME,
        "locations": locations,
        "ru_lte_refresh": dict(_ru_lte_refresh_state),
        "black_refresh": dict(_black_refresh_state),
        "locations_cache_ttl_sec": int(_LOCATIONS_CACHE_TTL_SEC),
    }


@app.post("/auth/telegram")
def auth_telegram(payload: TelegramAuthIn) -> Dict[str, Any]:
    existing = get_user_by_telegram_id(payload.telegram_id)
    user = upsert_telegram_user(payload.model_dump())
    return _issue_auth_payload(user, is_new=existing is None)


@app.post("/auth/code/issue")
def auth_code_issue(request: Request, payload: IssueCodeIn, _: bool = Depends(require_code_issuer)) -> Dict[str, Any]:
    existing = get_user_by_telegram_id(payload.telegram_id)
    user = upsert_telegram_user(payload.model_dump())
    refresh_subscription_statuses(int(user["id"]))
    fresh_user = get_user_by_id(int(user["id"])) or user
    issued = issue_auth_code(
        int(fresh_user["id"]),
        ttl_minutes=settings.AUTH_CODE_TTL_MINUTES,
        meta={
            "telegram_id": payload.telegram_id,
            "language": payload.language,
        },
    )
    code = str(issued["code"])
    deep_link = _build_open_app_bridge_url(
        request,
        code=code,
        lang=fresh_user.get("language") or payload.language or "ru",
    )
    return {
        "ok": True,
        "code": code,
        "deep_link": deep_link,
        "expires_at": issued["expires_at"].isoformat(),
        "user": fresh_user,
        "is_new": existing is None,
    }


@app.post("/auth/code")
def auth_code(payload: CodeAuthIn) -> Dict[str, Any]:
    normalized_code = (payload.code or "").strip()
    if settings.AUTH_ALLOW_DEV_CODE and normalized_code == settings.AUTH_DEV_LOGIN_CODE:
        user_payload = _fallback_code_user_payload(payload)
        existing = get_user_by_telegram_id(int(user_payload["telegram_id"]))
        user = upsert_telegram_user(user_payload)
        return _issue_auth_payload(user, is_new=existing is None)

    user = consume_auth_code(normalized_code)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired code")

    requested_language = "en" if payload.language == "en" else "ru"
    if user.get("language") != requested_language:
        user = set_user_language(int(user["id"]), requested_language)
    return _issue_auth_payload(user, is_new=False)


@app.post("/auth/refresh")
def auth_refresh(
    payload: RefreshIn,
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> Dict[str, Any]:
    raw_token = (payload.refresh_token or "").strip()
    if not raw_token and credentials and credentials.credentials:
        raw_token = credentials.credentials
    if not raw_token:
        raise HTTPException(status_code=401, detail="Missing refresh token")
    decoded = _decode_token(raw_token, expected_type="refresh")
    user_id = int(decoded["sub"])
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return _issue_auth_payload(user, is_new=False)


@app.get("/auth/me")
def auth_me(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    view = get_user_subscription_view(user["id"])
    return {"ok": True, "user": user, **view, "language": user.get("language") or "ru"}


@app.post("/auth/logout")
def auth_logout(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    return {"ok": True, "logged_out": True, "user_id": user["id"]}


@app.patch("/users/me/language")
def patch_language(payload: LanguageIn, user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    updated = set_user_language(user["id"], payload.language)
    return {"ok": True, "user": updated}


@app.get("/app/config")
def app_config() -> Dict[str, Any]:
    return {
        "app_name": settings.APP_NAME,
        "support_url": settings.SUPPORT_TELEGRAM_URL,
        "bot_url": _bot_public_url(),
        "maintenance_mode": settings.VPN_MAINTENANCE_MODE,
        "payments_enabled": settings.PAYMENTS_ENABLED,
        "payment_commission_percent": max(float(getattr(settings, "PAYMENTS_COMMISSION_PERCENT", 0.0) or 0.0), 0.0),
        "client_mode": _selected_client_mode(),
        "client_name": _selected_client_name(),
        "mobile_client_mode": _selected_client_mode(),
        "mobile_client_name": _selected_client_name(),
        "desktop_client_mode": "happ",
        "desktop_client_name": "Happ",
        "android_app_url": _selected_platform_store_url("android"),
        "ios_app_url": _selected_platform_store_url("ios"),
        "windows_app_url": _selected_platform_store_url("windows"),
        "macos_app_url": _selected_platform_store_url("macos"),
        "device_limit_default": settings.VPN_DEFAULT_DEVICE_LIMIT,
        "feature_flags": {
            "auth_refresh": True,
            "auth_logout": True,
            "maintenance_mode": settings.VPN_MAINTENANCE_MODE,
            "payments_enabled": settings.PAYMENTS_ENABLED,
            "new_activations_enabled": settings.VPN_NEW_ACTIVATIONS_ENABLED,
            "settings_editable": True,
        },
    }


@app.get("/plans")
def plans() -> Dict[str, Any]:
    sync_plans_from_env()
    return {"ok": True, "items": get_active_plans()}


@app.get("/subscriptions/me")
def subscription_me(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    with psycopg.connect(settings.DATABASE_URL, row_factory=dict_row) as conn:
        view = _get_user_subscription_view_with_conn(conn, user["id"])
    if view.get("is_active") and view.get("subscription"):
        subscription_token = view.get("subscription_token") or ensure_user_subscription_token(int(user["id"]))
        subscription_url = _subscription_public_url_from_base(settings.BACKEND_BASE_URL, subscription_token)
    else:
        subscription_token = None
        subscription_url = None
    return {"ok": True, **view, "subscription_token": subscription_token, "subscription_url": subscription_url}


@app.get("/devices")
def devices(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    with psycopg.connect(settings.DATABASE_URL, row_factory=dict_row) as conn:
        view = _get_user_subscription_view_with_conn(conn, user["id"])
    return {"ok": True, "items": view["devices"], "devices_used": view["devices_used"], "device_limit": view["device_limit"]}


@app.post("/devices/register")
def devices_register(payload: DeviceRegisterIn, user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    try:
        item = register_device(user["id"], payload.platform, payload.device_name, payload.device_fingerprint)
        return {"ok": True, "device": item}
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/devices/{device_id}")
def devices_delete(device_id: int, user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    item = delete_device(user["id"], device_id)
    if not item:
        raise HTTPException(status_code=404, detail="Device not found")
    return {"ok": True, "device": item}


@app.get("/config/version")
def config_version() -> JSONResponse:
    payload = _config_version_payload()
    version = str(payload.get("version") or "")
    return JSONResponse(content=payload, headers=_json_no_cache_headers(etag_seed=version))


@app.get("/sync")
def sync_state(user: Dict[str, Any] = Depends(get_current_user)) -> JSONResponse:
    items = _cached_locations_payload()
    payload = {
        **_config_version_payload(),
        "user_id": int(user["id"]),
        "items": items,
    }
    version = str(payload.get("version") or "")
    return JSONResponse(content=payload, headers=_json_no_cache_headers(etag_seed=version))


@app.get("/locations")
def locations() -> JSONResponse:
    items = _cached_locations_payload()
    seed = json.dumps(items, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return JSONResponse(content={"ok": True, "items": items}, headers=_json_no_cache_headers(etag_seed=seed))


@app.get("/locations/status")
def locations_status() -> JSONResponse:
    items = [{"code": row["code"], "status": row["status"], "is_active": row["is_active"]} for row in _cached_locations_payload()]
    seed = json.dumps(items, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return JSONResponse(content={"ok": True, "items": items}, headers=_json_no_cache_headers(etag_seed=seed))


@app.get("/vpn/config/{location_code}")
def vpn_config(location_code: str, user: Dict[str, Any] = Depends(get_current_user)) -> JSONResponse:
    try:
        config = get_vpn_config_for_user(user["id"], location_code)
        seed = json.dumps(config, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return JSONResponse(content={"ok": True, "config": config}, headers=_json_no_cache_headers(etag_seed=seed))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/support/faq")
def support_faq(lang: str = Query(default="ru")) -> Dict[str, Any]:
    norm = "en" if lang == "en" else "ru"
    return {
        "ok": True,
        "lang": norm,
        "support_url": settings.SUPPORT_TELEGRAM_URL,
        "faq": settings.SUPPORT_FAQ_EN if norm == "en" else settings.SUPPORT_FAQ_RU,
    }



def _decimal_money(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _payment_amount_with_commission(amount_rub: float) -> Decimal:
    amount = _decimal_money(amount_rub)
    commission = max(Decimal(str(getattr(settings, "PAYMENTS_COMMISSION_PERCENT", 0.0) or 0.0)), Decimal("0"))
    if commission <= 0:
        return amount
    factor = (Decimal("100") + commission) / Decimal("100")
    return (amount * factor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _money_str(value: Any) -> str:
    return format(_decimal_money(value), ".2f")


def _robokassa_hash_value(*parts: Any) -> str:
    algorithm = str(getattr(settings, "ROBOKASSA_HASH_ALGORITHM", "md5") or "md5").strip().lower()
    base = ":".join(str(part) for part in parts)
    if algorithm in {"md5", "md-5"}:
        return hashlib.md5(base.encode("utf-8")).hexdigest()
    if algorithm in {"sha1", "sha-1"}:
        return hashlib.sha1(base.encode("utf-8")).hexdigest()
    if algorithm in {"sha256", "sha-256"}:
        return hashlib.sha256(base.encode("utf-8")).hexdigest()
    if algorithm in {"sha384", "sha-384"}:
        return hashlib.sha384(base.encode("utf-8")).hexdigest()
    if algorithm in {"sha512", "sha-512"}:
        return hashlib.sha512(base.encode("utf-8")).hexdigest()
    raise ValueError(f"Unsupported ROBOKASSA_HASH_ALGORITHM: {algorithm}")


def _robokassa_invoice_id() -> str:
    return f"{int(time.time() * 1000)}{secrets.randbelow(900) + 100}"


def _robokassa_result_url() -> str:
    explicit = str(getattr(settings, "ROBOKASSA_RESULT_URL", "") or "").strip()
    if explicit:
        return explicit
    base = str(getattr(settings, "APP_BASE_URL", "") or getattr(settings, "BACKEND_BASE_URL", "") or "").strip().rstrip("/")
    return f"{base}/payments/webhook/robokassa" if base else ""


def _robokassa_success_url() -> str:
    explicit = str(getattr(settings, "ROBOKASSA_SUCCESS_URL", "") or "").strip()
    if explicit:
        return explicit
    base = str(getattr(settings, "APP_BASE_URL", "") or getattr(settings, "BACKEND_BASE_URL", "") or "").strip().rstrip("/")
    return f"{base}/payments/success/robokassa" if base else ""


def _robokassa_fail_url() -> str:
    explicit = str(getattr(settings, "ROBOKASSA_FAIL_URL", "") or "").strip()
    if explicit:
        return explicit
    base = str(getattr(settings, "APP_BASE_URL", "") or getattr(settings, "BACKEND_BASE_URL", "") or "").strip().rstrip("/")
    return f"{base}/payments/fail/robokassa" if base else ""


def _create_robokassa_payment(local_payment_id: str, amount_rub: float, description: str) -> Dict[str, Any]:
    _ = local_payment_id
    invoice_id = _robokassa_invoice_id()
    out_sum = _money_str(amount_rub)
    success_url = _robokassa_success_url()
    fail_url = _robokassa_fail_url()
    signature = _robokassa_hash_value(
        settings.ROBOKASSA_MERCHANT_LOGIN,
        out_sum,
        invoice_id,
        success_url,
        "GET",
        fail_url,
        "GET",
        settings.ROBOKASSA_PASSWORD1,
    )
    params = {
        "MerchantLogin": settings.ROBOKASSA_MERCHANT_LOGIN,
        "OutSum": out_sum,
        "InvId": invoice_id,
        "Description": str(description or "").strip()[:100],
        "Culture": "en" if str(settings.ROBOKASSA_CULTURE or "ru").strip().lower() == "en" else "ru",
        "Encoding": "utf-8",
        "SignatureValue": signature,
        "SuccessUrl2": success_url,
        "SuccessUrl2Method": "GET",
        "FailUrl2": fail_url,
        "FailUrl2Method": "GET",
    }
    inc_curr_label = str(getattr(settings, "ROBOKASSA_INCCURRLABEL", "") or "").strip()
    if inc_curr_label:
        params["IncCurrLabel"] = inc_curr_label
    if bool(getattr(settings, "ROBOKASSA_IS_TEST", False)):
        params["IsTest"] = "1"
    checkout_url = f"{str(settings.ROBOKASSA_PAYMENT_URL or '').strip()}?{urlencode(params)}"
    return {
        "id": invoice_id,
        "checkout_url": checkout_url,
        "amount": out_sum,
    }


def _robokassa_validate_signature(out_sum: str, inv_id: str, signature_value: str, password: str) -> bool:
    if not out_sum or not inv_id or not signature_value or not password:
        return False
    expected = _robokassa_hash_value(out_sum, inv_id, password)
    return secrets.compare_digest(expected.lower(), str(signature_value).strip().lower())


def _robokassa_sync_payment_status(payment: Dict[str, Any]) -> Dict[str, Any]:
    if not payment or payment.get("provider") != "robokassa":
        return payment
    if payment.get("status") == "paid":
        return payment
    if bool(getattr(settings, "ROBOKASSA_IS_TEST", False)):
        return payment
    invoice_id = str(payment.get("external_payment_id") or "").strip()
    if not invoice_id or not settings.ROBOKASSA_MERCHANT_LOGIN or not settings.ROBOKASSA_PASSWORD2:
        return payment
    signature = _robokassa_hash_value(settings.ROBOKASSA_MERCHANT_LOGIN, invoice_id, settings.ROBOKASSA_PASSWORD2)
    try:
        response = requests.get(
            "https://auth.robokassa.ru/Merchant/WebService/Service.asmx/OpStateExt",
            params={
                "MerchantLogin": settings.ROBOKASSA_MERCHANT_LOGIN,
                "InvoiceID": invoice_id,
                "Signature": signature,
            },
            timeout=20,
        )
        response.raise_for_status()
        root = ET.fromstring(response.text)
        namespace = {"rk": root.tag.split("}")[0].strip("{")} if "}" in root.tag else {}
        code_node = root.find(".//rk:State/rk:Code", namespace) if namespace else root.find(".//State/Code")
        state_code = int((code_node.text or "0").strip()) if code_node is not None and (code_node.text or "").strip() else 0
        if state_code == 100:
            return activate_payment_and_extend_subscription(payment["id"])
        if state_code in {10, 60}:
            return update_payment(payment["id"], status="cancelled")
        if state_code in {5, 20, 50, 80} and payment.get("status") != "pending":
            return update_payment(payment["id"], status="pending")
    except Exception:
        logger.exception("Robokassa status sync failed for payment %s", payment.get("id"))
    return get_payment_by_internal_or_external(payment.get("id")) or payment


def _create_yookassa_payment(local_payment_id: str, amount_rub: float, description: str) -> Dict[str, Any]:
    response = requests.post(
        "https://api.yookassa.ru/v3/payments",
        headers={"Idempotence-Key": str(uuid4())},
        auth=(settings.YOOKASSA_SHOP_ID, settings.YOOKASSA_SECRET_KEY),
        json={
            "amount": {"value": f"{float(amount_rub):.2f}", "currency": "RUB"},
            "capture": True,
            "confirmation": {"type": "redirect", "return_url": settings.YOOKASSA_RETURN_URL},
            "description": description,
            "metadata": {"local_payment_id": local_payment_id, "app_name": settings.APP_NAME},
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


@app.post("/payments/create")
def payments_create(payload: PaymentCreateIn, user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    if settings.VPN_MAINTENANCE_MODE:
        raise HTTPException(status_code=503, detail="Maintenance mode is enabled")
    if user["status"] == "blocked":
        raise HTTPException(status_code=403, detail="Blocked user cannot create payment")
    plan = get_plan_by_code(payload.plan_code)
    if not plan or not plan["is_active"]:
        raise HTTPException(status_code=404, detail="Plan not found or inactive")
    final_amount = _payment_amount_with_commission(float(plan["price_rub"]))
    payment = create_payment_record(
        user_id=user["id"],
        plan_id=plan["id"],
        provider=settings.PAYMENTS_PROVIDER,
        method=payload.method,
        amount=float(final_amount),
        currency="RUB",
        status="disabled" if not settings.PAYMENTS_ENABLED else "created",
    )
    if not settings.PAYMENTS_ENABLED:
        return {
            "ok": True,
            "payments_enabled": False,
            "message": "Payment module is ready, but PAYMENTS_ENABLED=false now.",
            "payment": payment,
            "plan": plan,
        }
    if settings.PAYMENTS_PROVIDER == "yookassa":
        if not settings.YOOKASSA_SHOP_ID or not settings.YOOKASSA_SECRET_KEY:
            raise HTTPException(status_code=500, detail="YooKassa credentials are missing")
        try:
            remote = _create_yookassa_payment(payment["id"], float(final_amount), f"{settings.APP_NAME} {plan['name_ru']}")
            payment = update_payment(
                payment["id"],
                external_payment_id=remote.get("id"),
                checkout_url=((remote.get("confirmation") or {}).get("confirmation_url")),
                status=remote.get("status", "pending"),
            )
        except requests.RequestException as exc:
            update_payment(payment["id"], status="error")
            raise HTTPException(status_code=502, detail=f"YooKassa create payment failed: {exc}") from exc
    elif settings.PAYMENTS_PROVIDER == "robokassa":
        if not settings.ROBOKASSA_MERCHANT_LOGIN or not settings.ROBOKASSA_PASSWORD1 or not settings.ROBOKASSA_PASSWORD2:
            raise HTTPException(status_code=500, detail="Robokassa credentials are missing")
        try:
            remote = _create_robokassa_payment(payment["id"], float(final_amount), f"{settings.APP_NAME} {plan['name_ru']}")
            payment = update_payment(
                payment["id"],
                external_payment_id=remote.get("id"),
                checkout_url=remote.get("checkout_url"),
                status="pending",
            )
        except Exception as exc:
            update_payment(payment["id"], status="error")
            raise HTTPException(status_code=502, detail=f"Robokassa create payment failed: {exc}") from exc
    else:
        raise HTTPException(status_code=500, detail=f"Unsupported payments provider: {settings.PAYMENTS_PROVIDER}")
    return {"ok": True, "payments_enabled": True, "payment": payment, "plan": plan}


@app.get("/payments/{payment_id}")
def payments_get(payment_id: str, user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    payment = get_payment_for_user(payment_id, user["id"])
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    payment = _robokassa_sync_payment_status(payment)
    payment = get_payment_for_user(payment_id, user["id"]) or payment
    return {"ok": True, "payment": payment}


@app.post("/payments/webhook/yookassa")
def payments_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    obj = payload.get("object") or {}
    remote_payment_id = obj.get("id") or ((obj.get("metadata") or {}).get("local_payment_id"))
    if not remote_payment_id:
        return {"ok": True, "ignored": True}
    payment = get_payment_by_internal_or_external(remote_payment_id)
    if not payment:
        return {"ok": True, "ignored": True}
    status_value = obj.get("status") or payload.get("event") or payment["status"]
    if status_value in {"succeeded", "paid"}:
        activate_payment_and_extend_subscription(payment["id"])
    elif status_value in {"canceled", "cancelled"}:
        update_payment(payment["id"], status="cancelled")
        enqueue_notification(
            user_id=payment['user_id'],
            event_type='payment_failed',
            unique_key=f'payment_failed:{payment["id"]}',
            payload={'payment_id': payment['id']},
        )
    else:
        update_payment(payment["id"], status=status_value)
    return {"ok": True}


async def _robokassa_request_params(request: Request) -> Dict[str, str]:
    data: Dict[str, Any] = dict(request.query_params)
    if request.method.upper() == "POST":
        with contextlib.suppress(Exception):
            form = await request.form()
            data.update(dict(form))
    return {str(key): str(value) for key, value in data.items()}


@app.api_route("/payments/webhook/robokassa", methods=["GET", "POST"])
async def payments_webhook_robokassa(request: Request) -> Response:
    params = await _robokassa_request_params(request)
    out_sum = str(params.get("OutSum") or "").strip()
    inv_id = str(params.get("InvId") or params.get("InvoiceID") or "").strip()
    signature = str(params.get("SignatureValue") or "").strip()
    if not _robokassa_validate_signature(out_sum, inv_id, signature, settings.ROBOKASSA_PASSWORD2):
        return Response(content="bad sign", status_code=400, media_type="text/plain")
    payment = get_payment_by_internal_or_external(inv_id)
    if not payment:
        return Response(content=f"OK{inv_id}", media_type="text/plain")
    expected_amount = _money_str(payment.get("amount") or 0)
    received_amount = _money_str(out_sum or 0)
    if expected_amount != received_amount:
        logger.warning("Robokassa amount mismatch for payment %s: expected=%s got=%s", payment.get("id"), expected_amount, received_amount)
        return Response(content="bad sum", status_code=400, media_type="text/plain")
    activate_payment_and_extend_subscription(payment["id"])
    return Response(content=f"OK{inv_id}", media_type="text/plain")


@app.api_route("/payments/success/robokassa", methods=["GET", "POST"])
async def payments_success_robokassa(request: Request) -> HTMLResponse:
    params = await _robokassa_request_params(request)
    out_sum = str(params.get("OutSum") or "").strip()
    inv_id = str(params.get("InvId") or params.get("InvoiceID") or "").strip()
    signature = str(params.get("SignatureValue") or "").strip()
    paid = False
    if _robokassa_validate_signature(out_sum, inv_id, signature, settings.ROBOKASSA_PASSWORD1):
        payment = get_payment_by_internal_or_external(inv_id)
        if payment:
            expected_amount = _money_str(payment.get("amount") or 0)
            received_amount = _money_str(out_sum or 0)
            if expected_amount == received_amount:
                activate_payment_and_extend_subscription(payment["id"])
                paid = True
    title = "Оплата получена" if paid else "Платёж обрабатывается"
    body = "Подписка активируется автоматически. Вернитесь в Telegram-бот и нажмите «Проверить оплату»." if paid else "Банк или платёжная система ещё обрабатывает платёж. Вернитесь в Telegram-бот и нажмите «Проверить оплату» через несколько минут."
    return HTMLResponse(f"<!doctype html><html lang='ru'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>{title}</title></head><body style='font-family:Arial,sans-serif;padding:24px;max-width:720px;margin:0 auto;'><h2>{title}</h2><p>{body}</p></body></html>")


@app.api_route("/payments/fail/robokassa", methods=["GET", "POST"])
async def payments_fail_robokassa(request: Request) -> HTMLResponse:
    params = await _robokassa_request_params(request)
    inv_id = str(params.get("InvId") or params.get("InvoiceID") or "").strip()
    payment = get_payment_by_internal_or_external(inv_id) if inv_id else None
    if payment and payment.get("status") not in {"paid", "cancelled"}:
        update_payment(payment["id"], status="cancelled")
    return HTMLResponse("<!doctype html><html lang='ru'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Оплата не завершена</title></head><body style='font-family:Arial,sans-serif;padding:24px;max-width:720px;margin:0 auto;'><h2>Оплата не завершена</h2><p>Вы можете вернуться в Telegram-бот и попробовать оплатить снова.</p></body></html>")


@app.get("/api/infra/admin/vpn/users")
def admin_users(
    search: str = Query(default=""),
    query_text: str = Query(default="", alias="query"),
    status_filter: str = Query(default="all", alias="status"),
    filter_text: str = Query(default="", alias="filter"),
    admin_name: str = Depends(require_admin),
) -> Dict[str, Any]:
    _ = admin_name
    effective_search = query_text or search
    effective_status = filter_text or status_filter or "all"
    data = list_admin_users(search=effective_search, status_filter=effective_status)
    return {"ok": True, **data}


@app.post("/api/infra/admin/vpn/users")
def admin_users_create(payload: AdminUserUpsertIn, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    try:
        item = admin_create_or_update_user(payload.model_dump(exclude_none=True), admin_name)
        return {"ok": True, "item": item}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/infra/admin/vpn/users/{telegram_id}")
def admin_user_patch(telegram_id: int, payload: AdminUserPatchIn, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    data = payload.model_dump(exclude_unset=True)
    if "device_limit_override" not in data:
        raise HTTPException(status_code=400, detail="No changes provided")
    try:
        item = set_user_device_limit_override_by_telegram(telegram_id, data.get("device_limit_override"), admin_name)
        return {"ok": True, "item": item}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.patch("/api/infra/admin/vpn/users/{telegram_id}/block")
def admin_user_block(telegram_id: int, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    try:
        user = set_user_status_by_telegram(telegram_id, "blocked", admin_name, "User blocked from VPN admin")
        return {"ok": True, "user": user}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.patch("/api/infra/admin/vpn/users/{telegram_id}/unblock")
def admin_user_unblock(telegram_id: int, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    try:
        user = set_user_status_by_telegram(telegram_id, "active", admin_name, "User unblocked from VPN admin")
        return {"ok": True, "user": user}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/infra/admin/vpn/users/{telegram_id}/extend")
def admin_user_extend(telegram_id: int, payload: ExtendIn, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    try:
        item = extend_user_subscription_by_telegram(telegram_id, payload.normalized_days(), payload.reason, admin_name)
        return {"ok": True, "item": item}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/infra/admin/vpn/users/{telegram_id}/reset-devices")
def admin_user_reset_devices(telegram_id: int, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    try:
        item = reset_user_devices_by_telegram(telegram_id, admin_name)
        return {"ok": True, "item": item}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/infra/admin/vpn/payments")
def admin_payments(status_filter: str = Query(default="all", alias="status"), admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    return {"ok": True, "items": list_payments(status_filter=status_filter)}


@app.get("/api/infra/admin/vpn/payments/export.csv")
def admin_payments_export(status_filter: str = Query(default="all", alias="status"), admin_name: str = Depends(require_admin)) -> Response:
    _ = admin_name
    content = export_payments_csv(status_filter=status_filter)
    return Response(content=content, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=vpn-payments.csv"})


def _safe_speed_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not (number >= 0):
        return None
    return round(number, 2)


def _safe_speed_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    if number < 0:
        return None
    return number


def _location_speed_test_url(row: Dict[str, Any]) -> Optional[str]:
    payload = _compose_vpn_payload_for_location(dict(row))
    candidates = [
        payload.get("speed_test_url"),
        payload.get("speedtest_url"),
        payload.get("speed_url"),
        payload.get("metrics_url"),
    ]
    for value in candidates:
        text = str(value or "").strip()
        if text.lower().startswith(("http://", "https://")):
            return text

    server = str(payload.get("speed_test_host") or payload.get("server") or "").strip()
    if not server or "://" in server:
        return None
    scheme = str(payload.get("speed_test_scheme") or "https").strip() or "https"
    path = str(payload.get("speed_test_path") or "/speed").strip() or "/speed"
    if not path.startswith("/"):
        path = "/" + path
    try:
        port = int(payload.get("speed_test_port") or 0)
    except (TypeError, ValueError):
        port = 0
    base = f"{scheme}://{server}"
    if port > 0 and not ((scheme == "https" and port == 443) or (scheme == "http" and port == 80)):
        base += f":{port}"
    return base + path


def _extract_speed_metrics(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("Speed test response must be a JSON object")

    source = data
    for key in ("data", "result", "speed", "metrics"):
        nested = source.get(key)
        if isinstance(nested, dict):
            source = nested
            break

    download = _safe_speed_float(
        source.get("download_mbps")
        or source.get("downloadMbps")
        or source.get("download")
        or data.get("download_mbps")
        or data.get("downloadMbps")
        or data.get("download")
    )
    upload = _safe_speed_float(
        source.get("upload_mbps")
        or source.get("uploadMbps")
        or source.get("upload")
        or data.get("upload_mbps")
        or data.get("uploadMbps")
        or data.get("upload")
    )
    ping = _safe_speed_int(
        source.get("ping_ms")
        or source.get("pingMs")
        or source.get("ping")
        or data.get("ping_ms")
        or data.get("pingMs")
        or data.get("ping")
    )
    checked_at = (
        source.get("checked_at")
        or source.get("checkedAt")
        or source.get("timestamp")
        or data.get("checked_at")
        or data.get("checkedAt")
        or data.get("timestamp")
    )

    if download is None and upload is None and ping is None:
        raise ValueError("Response has no speed fields")

    if checked_at:
        checked_text = str(checked_at).strip()
        try:
            checked_iso = datetime.fromisoformat(checked_text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except ValueError:
            checked_iso = datetime.now(timezone.utc).isoformat()
    else:
        checked_iso = datetime.now(timezone.utc).isoformat()

    return {
        "download_mbps": download,
        "upload_mbps": upload,
        "ping_ms": ping,
        "speed_checked_at": checked_iso,
    }


def _probe_pool_for_location_code(code: str) -> str:
    normalized = str(code or "").strip().lower()
    if normalized.startswith("ru-lte"):
        return "ru_lte"
    if normalized.startswith("intl-fast"):
        return "black"
    return "generic"


def _probe_payload_debug_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_vpn_payload_keys(payload or {}) if isinstance(payload, dict) else {}
    public_key = str(normalized.get("public_key") or normalized.get("publicKey") or "").strip()
    return {
        "server": str(normalized.get("server") or "").strip(),
        "port": _normalize_optional_int(normalized.get("port")),
        "transport": _payload_transport(normalized),
        "security": _payload_security(normalized),
        "server_name": str(normalized.get("server_name") or normalized.get("sni") or "").strip(),
        "public_key_len": len(public_key),
        "short_id": str(normalized.get("short_id") or normalized.get("shortId") or "").strip(),
        "flow": str(normalized.get("flow") or "").strip(),
        "service_name": str(normalized.get("service_name") or normalized.get("serviceName") or "").strip(),
        "path": str(normalized.get("path") or "").strip(),
        "host": str(normalized.get("host") or "").strip(),
    }


def _probe_debug_summary(fields: Dict[str, Any]) -> str:
    return (
        f"server={fields.get('server') or '-'} "
        f"port={fields.get('port') if fields.get('port') is not None else '-'} "
        f"transport={fields.get('transport') or '-'} "
        f"security={fields.get('security') or '-'} "
        f"server_name={fields.get('server_name') or '-'} "
        f"public_key_len={fields.get('public_key_len') or 0} "
        f"short_id={fields.get('short_id') or '-'} "
        f"flow={fields.get('flow') or '-'} "
        f"service_name={fields.get('service_name') or '-'} "
        f"path={fields.get('path') or '-'} "
        f"host={fields.get('host') or '-'}"
    )


def _log_location_probe_result(row: Dict[str, Any], probe: Dict[str, Any], *, source: str = "manual") -> None:
    code = str(row.get("code") or "").strip() or "unknown"
    method = str(probe.get("method") or "")
    reason = str(probe.get("error") or "")
    real_probe_error = str(probe.get("real_probe_error") or "")
    latency_ms = probe.get("latency_ms")
    labels = ",".join(str(item) for item in (probe.get("probe_labels_ok") or []))
    urls = ",".join(str(item) for item in (probe.get("probe_urls_ok") or []))
    payload = _compose_vpn_payload_for_location(dict(row)) or dict(row.get("vpn_payload") or {})
    debug_fields = _probe_payload_debug_fields(payload)
    probe_url = str(probe.get("probe_url") or "")
    debug_summary = _probe_debug_summary(debug_fields)
    if probe.get("ok"):
        logger.info(
            "[vpn][probe] source=%s code=%s result=ok method=%s latency_ms=%s labels=%s probe_url=%s urls=%s %s",
            source,
            code,
            method,
            latency_ms,
            labels or "-",
            probe_url or "-",
            urls or "-",
            debug_summary,
        )
    else:
        logger.warning(
            "[vpn][probe] source=%s code=%s result=error method=%s reason=%s real_probe_error=%s labels=%s probe_url=%s urls=%s %s",
            source,
            code,
            method,
            reason or "unknown",
            real_probe_error or "-",
            labels or "-",
            probe_url or "-",
            urls or "-",
            debug_summary,
        )


def _store_location_probe_result(row: Dict[str, Any], probe: Dict[str, Any], *, source: str = "manual") -> Dict[str, Any]:
    checked_iso = datetime.now(timezone.utc).isoformat()
    payload = dict(row.get("vpn_payload") or {})
    effective_payload = _compose_vpn_payload_for_location(dict(row)) or payload
    probe_ok = bool(probe.get("ok"))
    payload["_last_live_probe"] = {
        "ok": probe_ok,
        "at": checked_iso,
        "source": str(source or "manual"),
        "method": str(probe.get("method") or ""),
        "probe_url": str(probe.get("probe_url") or ""),
        "probe_urls_ok": list(probe.get("probe_urls_ok") or []),
        "probe_labels_ok": list(probe.get("probe_labels_ok") or []),
        "error": str(probe.get("error") or ""),
        "real_probe_error": str(probe.get("real_probe_error") or ""),
        "debug": _probe_payload_debug_fields(effective_payload),
    }
    patch: Dict[str, Any] = {
        "ping_ms": int(probe.get("latency_ms") or 0) or None if probe_ok else None,
        "speed_checked_at": checked_iso,
        "vpn_payload": payload,
    }
    status = str(row.get("status") or "").strip().lower()
    if probe_ok:
        patch["status"] = "reserve" if status == "reserve" else "online"
    item = patch_location(int(row.get("id")), patch)
    return item


def _run_location_speed_test(row: Dict[str, Any], *, source: str = "manual") -> Dict[str, Any]:
    code = str(row.get("code") or "")
    is_active = bool(row.get("is_active"))
    result: Dict[str, Any] = {
        "id": row.get("id"),
        "code": code,
        "name_ru": row.get("name_ru"),
        "status": "skipped",
    }

    if code in {"auto-fastest", "auto-reserve"}:
        result["reason"] = "virtual_location"
        return result
    if not is_active:
        result["reason"] = "inactive"
        return result

    payload = _compose_vpn_payload_for_location(dict(row))
    if not payload or not _config_is_complete(payload):
        result["reason"] = "payload_incomplete"
        return result

    pool = _probe_pool_for_location_code(code)
    tcp_timeout = float(settings.RU_LTE_CONNECT_TIMEOUT_SEC or 4) if pool == "ru_lte" else float(settings.BLACK_CONNECT_TIMEOUT_SEC or 4)
    probe = _generic_probe_candidate(payload, pool=pool, tcp_timeout=tcp_timeout)
    _log_location_probe_result(row, probe, source=source)
    previous_status = str(row.get("status") or "")
    previous_ping_ms = _normalize_optional_int(row.get("ping_ms"))
    item = _store_location_probe_result(row, probe, source=source)
    result.update({
        "status": "ok" if probe.get("ok") else "error",
        "source": source,
        "debug_fields": _probe_payload_debug_fields(payload),
        "method": probe.get("method"),
        "probe_url": probe.get("probe_url"),
        "probe_urls_ok": probe.get("probe_urls_ok") or [],
        "probe_labels_ok": probe.get("probe_labels_ok") or [],
        "reason": probe.get("error"),
        "real_probe_error": probe.get("real_probe_error"),
        "before": {
            "status": previous_status,
            "ping_ms": previous_ping_ms,
        },
        "after": {
            "status": item.get("status"),
            "ping_ms": item.get("ping_ms"),
            "live_publishable": bool(_row_live_probe_state(item).get("publishable")),
            "live_reason": _row_live_probe_state(item).get("reason"),
        },
        "metrics": {
            "ping_ms": item.get("ping_ms"),
            "speed_checked_at": (
                item.get("speed_checked_at").astimezone(timezone.utc).isoformat()
                if isinstance(item.get("speed_checked_at"), datetime)
                else item.get("speed_checked_at")
            ),
        },
    })
    return result


@app.post("/api/infra/admin/vpn/locations/speed-test")
def admin_locations_speed_test(admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    return _run_vpn_live_check_safe(source="manual", active_only=bool(getattr(settings, "VPN_LIVE_CHECK_ACTIVE_ONLY", True)))

@app.post("/api/infra/admin/vpn/locations/refresh-ru-lte")
def admin_refresh_ru_lte(admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    return _run_ru_lte_refresh_safe(source="manual")


@app.post("/api/infra/admin/vpn/locations/refresh-black")
def admin_refresh_black(admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    return _run_black_refresh_safe(source="manual")


@app.get("/api/infra/admin/vpn/locations")
def admin_locations(admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    return {
        "ok": True,
        "items": [serialize_location(row, include_payload=True) for row in list_locations(active_only=False)],
        "ru_lte_refresh": dict(_ru_lte_refresh_state),
        "ru_lte_auto_refresh_enabled": bool(settings.RU_LTE_AUTO_REFRESH_ENABLED),
        "ru_lte_auto_refresh_minutes": max(1, int(settings.RU_LTE_AUTO_REFRESH_MINUTES or 30)),
        "black_refresh": dict(_black_refresh_state),
        "black_auto_refresh_enabled": bool(settings.BLACK_AUTO_REFRESH_ENABLED),
        "black_auto_refresh_minutes": max(1, int(settings.BLACK_AUTO_REFRESH_MINUTES or 30)),
        "vpn_live_check": dict(_vpn_live_check_state),
        "vpn_live_check_on_startup": bool(getattr(settings, "VPN_LIVE_CHECK_ON_STARTUP", True)),
        "vpn_live_check_auto_enabled": bool(getattr(settings, "VPN_LIVE_CHECK_AUTO_ENABLED", True)),
        "vpn_live_check_auto_minutes": max(1, int(getattr(settings, "VPN_LIVE_CHECK_AUTO_MINUTES", 3) or 3)),
        "vpn_live_check_active_only": bool(getattr(settings, "VPN_LIVE_CHECK_ACTIVE_ONLY", True)),
    }


@app.post("/api/infra/admin/vpn/locations")
def admin_locations_create(payload: LocationIn, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    try:
        item = create_location(_normalize_admin_location_input(payload.model_dump(exclude_none=True)))
        return {"ok": True, "item": serialize_location(item, include_payload=True)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except psycopg.Error as exc:
        detail = str(exc).strip() or "Database error while saving location"
        raise HTTPException(status_code=500, detail=detail) from exc


@app.patch("/api/infra/admin/vpn/locations/{location_id}")
def admin_locations_patch(location_id: int, payload: LocationPatchIn, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    data = _normalize_admin_location_input({key: value for key, value in payload.model_dump().items() if value is not None})
    try:
        item = patch_location(location_id, data)
        reset_assignments = 0
        code = str(item.get("code") or "").strip()
        if code and code not in PUBLIC_VIRTUAL_LOCATION_CODES and any(key in data for key in {"is_active", "status", "vpn_payload", "is_reserve"}):
            reset_assignments = reset_virtual_location_assignments_for_concrete_code(code)
        return {"ok": True, "item": serialize_location(item, include_payload=True), "reset_virtual_assignments": reset_assignments}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/infra/admin/vpn/locations/{location_id}")
def admin_locations_delete(location_id: int, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    try:
        item = delete_location(location_id)
        code = str(item.get("code") or "").strip()
        reset_assignments = reset_virtual_location_assignments_for_concrete_code(code) if code and code not in PUBLIC_VIRTUAL_LOCATION_CODES else 0
        return {"ok": True, "item": serialize_location(item, include_payload=True), "reset_virtual_assignments": reset_assignments}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/infra/admin/vpn/settings")
def admin_settings(admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    sync_plans_from_env()
    return {"ok": True, "settings": settings_snapshot()}


@app.post("/api/infra/admin/vpn/settings")
def admin_settings_save(payload: AdminVpnSettingsIn, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    save_runtime_settings_payload(payload.model_dump(exclude_none=True))
    return {
        "ok": True,
        "message": "Settings saved to database and applied immediately.",
        "settings": settings_snapshot(),
    }



def _telegram_api_url(method: str) -> str:
    if not settings.BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured")
    return f"https://api.telegram.org/bot{settings.BOT_TOKEN}/{method}"



def telegram_send_message(telegram_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"chat_id": int(telegram_id), "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    response = requests.post(_telegram_api_url("sendMessage"), json=payload, timeout=20)
    response.raise_for_status()
    return response.json()


@app.post("/api/infra/admin/vpn/test-send")
def admin_test_send(payload: TestSendIn, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    try:
        telegram_send_message(payload.telegram_id, payload.text)
        return {"ok": True}
    except Exception as exc:
        record_bot_error("vpn-admin-test", str(payload.telegram_id), str(exc))
        raise HTTPException(status_code=502, detail=f"Telegram send failed: {exc}") from exc


@app.post("/api/infra/admin/vpn/broadcast")
def admin_broadcast(payload: BroadcastIn, admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    text = payload.text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text is required")
    sent = 0
    failed = 0
    for target in list_broadcast_targets(payload.statuses or ["active"]):
        try:
            telegram_send_message(int(target["telegram_id"]), text)
            sent += 1
        except Exception as exc:
            failed += 1
            record_bot_error("vpn-broadcast", str(target.get("telegram_id")), str(exc))
    return {"ok": True, "sent": sent, "failed": failed, "total": sent + failed}


@app.post("/vpn/client-events")
def vpn_client_events(payload: VpnClientEventIn, user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    platform = _diagnostic_text(payload.platform).lower() or "unknown"
    stage = _diagnostic_text(payload.stage).lower() or "runtime"
    status = _diagnostic_text(payload.status).lower()
    location_code = _diagnostic_text(payload.location_code)
    user_label = str(user.get("telegram_id") or user.get("id") or "unknown")
    context_parts = [f"user={user_label}"]
    if location_code:
        context_parts.append(f"location={location_code}")
    if status:
        context_parts.append(f"status={status}")
    message = _diagnostic_text(payload.error_message) or _diagnostic_text(payload.details) or f"Client VPN event: {stage}"
    record_bot_error(f"vpn-client-{platform}-{stage}", " | ".join(context_parts), message)
    return {"ok": True}


@app.get("/api/infra/admin/vpn/errors")
def admin_errors(limit: int = Query(default=50, ge=1, le=500), admin_name: str = Depends(require_admin)) -> Dict[str, Any]:
    _ = admin_name
    items = list_bot_errors(limit=limit) + build_location_tun_error_items()
    items.sort(key=lambda item: _diagnostic_text(item.get("created_at")) or "", reverse=True)
    return {"ok": True, "items": items[:limit]}
