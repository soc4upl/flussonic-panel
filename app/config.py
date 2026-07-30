from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any


@dataclass(frozen=True)
class FlussonicServer:
    id: str
    name: str
    url: str
    username: str
    password: str
    primary: bool = False
    enabled: bool = True
    verify_tls: bool = True
    node_exporter_url: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "FlussonicServer":
        required = ("id", "name", "url", "username", "password")
        missing = [key for key in required if not raw.get(key)]
        if missing:
            raise ValueError(f"Flussonic server is missing: {', '.join(missing)}")
        return cls(
            id=str(raw["id"]),
            name=str(raw["name"]),
            url=str(raw["url"]).rstrip("/"),
            username=str(raw["username"]),
            password=str(raw["password"]),
            primary=bool(raw.get("primary", False)),
            enabled=bool(raw.get("enabled", True)),
            verify_tls=bool(raw.get("verify_tls", True)),
            node_exporter_url=(str(raw.get("node_exporter_url") or "").strip().rstrip("/") or None),
        )


@dataclass(frozen=True)
class Settings:
    panel_title: str
    admin_user: str
    admin_password: str
    secret_key: str
    cookie_secure: bool
    request_timeout: float
    servers: tuple[FlussonicServer, ...]
    server_store_path: str
    monitor_poll_seconds: float
    session_request_timeout: float
    sessions_path: str
    monitor_max_pages: int
    uptime_reset_gap: int
    uptime_prune_after: int
    database_path: str
    monitor_history_seconds: int
    monitor_retention_days: int
    source_check_enabled: bool
    source_check_seconds: int
    source_check_timeout: float
    source_check_concurrency: int
    source_check_all_servers: bool
    server_load_enabled: bool
    server_load_poll_seconds: int
    server_network_poll_seconds: float
    server_load_history_seconds: int
    server_load_cpu_warning: float
    server_load_memory_warning: float
    server_load_disk_warning: float
    runtime_metrics_paths: tuple[str, ...]

    @property
    def enabled_servers(self) -> tuple[FlussonicServer, ...]:
        return tuple(server for server in self.servers if server.enabled)

    @property
    def primary_server(self) -> FlussonicServer:
        enabled = self.enabled_servers
        if not enabled:
            raise RuntimeError("No enabled Flussonic servers configured")
        return next((server for server in enabled if server.primary), enabled[0])


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    raw_servers = os.getenv("FLUSSONIC_SERVERS_JSON", "[]")
    try:
        parsed = json.loads(raw_servers)
    except json.JSONDecodeError as exc:
        raise RuntimeError("FLUSSONIC_SERVERS_JSON contains invalid JSON") from exc
    if not isinstance(parsed, list):
        raise RuntimeError("FLUSSONIC_SERVERS_JSON must be a JSON array")

    servers = tuple(FlussonicServer.from_dict(item) for item in parsed)
    ids = [server.id for server in servers]
    if len(ids) != len(set(ids)):
        raise RuntimeError("Flussonic server IDs must be unique")

    return Settings(
        panel_title=os.getenv("PANEL_TITLE", "Cyrius Stream Control"),
        admin_user=os.getenv("PANEL_ADMIN_USER", "admin"),
        admin_password=os.getenv("PANEL_ADMIN_PASSWORD", "change-me"),
        secret_key=os.getenv("PANEL_SECRET_KEY", "change-this-secret-key"),
        cookie_secure=_as_bool(os.getenv("PANEL_COOKIE_SECURE"), False),
        request_timeout=float(os.getenv("FLUSSONIC_REQUEST_TIMEOUT", "12")),
        servers=servers,
        server_store_path=os.getenv("FLUSSONIC_SERVER_STORE", "/app/data/servers.json"),
        monitor_poll_seconds=max(30.0, float(os.getenv("MONITOR_POLL_SECONDS", "30"))),
        session_request_timeout=max(1.0, float(os.getenv("FLUSSONIC_SESSION_TIMEOUT", os.getenv("FLUSSONIC_REQUEST_TIMEOUT", "12")))),
        sessions_path=os.getenv("FLUSSONIC_SESSIONS_PATH", "/streamer/api/v3/sessions"),
        monitor_max_pages=max(1, int(os.getenv("MONITOR_MAX_PAGES", "1000"))),
        uptime_reset_gap=max(1, int(os.getenv("UPTIME_RESET_GAP", "15"))),
        uptime_prune_after=max(10, int(os.getenv("UPTIME_PRUNE_AFTER", "600"))),
        database_path=os.getenv("PANEL_DATABASE", "/app/data/panel.db"),
        monitor_history_seconds=max(30, int(os.getenv("MONITOR_HISTORY_SECONDS", "60"))),
        monitor_retention_days=max(1, int(os.getenv("MONITOR_RETENTION_DAYS", "30"))),
        source_check_enabled=_as_bool(os.getenv("SOURCE_CHECK_ENABLED"), True),
        source_check_seconds=max(60, int(os.getenv("SOURCE_CHECK_SECONDS", "300"))),
        source_check_timeout=max(2.0, float(os.getenv("SOURCE_CHECK_TIMEOUT", "8"))),
        source_check_concurrency=max(1, min(50, int(os.getenv("SOURCE_CHECK_CONCURRENCY", "8")))),
        source_check_all_servers=_as_bool(os.getenv("SOURCE_CHECK_ALL_SERVERS"), False),
        server_load_enabled=_as_bool(os.getenv("SERVER_LOAD_ENABLED"), True),
        server_load_poll_seconds=max(30, int(os.getenv("SERVER_LOAD_POLL_SECONDS", "60"))),
        server_network_poll_seconds=max(1.0, min(10.0, float(os.getenv("SERVER_NETWORK_POLL_SECONDS", "1")))),
        server_load_history_seconds=max(30, int(os.getenv("SERVER_LOAD_HISTORY_SECONDS", "60"))),
        server_load_cpu_warning=max(1.0, min(100.0, float(os.getenv("SERVER_LOAD_CPU_WARNING", "85")))),
        server_load_memory_warning=max(1.0, min(100.0, float(os.getenv("SERVER_LOAD_MEMORY_WARNING", "85")))),
        server_load_disk_warning=max(1.0, min(100.0, float(os.getenv("SERVER_LOAD_DISK_WARNING", "90")))),
        runtime_metrics_paths=tuple(
            item.strip() for item in os.getenv(
                "FLUSSONIC_RUNTIME_METRICS_PATHS",
                "/streamer/api-v4/runtime/metrics,/streamer/api/v3/runtime/metrics,/runtime/metrics",
            ).split(",") if item.strip()
        ),
    )
