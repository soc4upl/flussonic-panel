from __future__ import annotations

import asyncio
import copy
import time
from collections import defaultdict
from contextlib import suppress
from typing import Any
from urllib.parse import urljoin

import httpx

from .config import FlussonicServer, Settings
from .server_store import ServerStore
from .datastore import DataStore

SESSION_LIST_KEYS = ("sessions", "items", "result", "data")
NEXT_KEYS = ("next", "next_page", "next_cursor", "cursor_next")
UNKNOWN = "Unknown"


def _pick_first(data: dict[str, Any], keys: tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return default


def _extract_field(data: dict[str, Any], keys: tuple[str, ...], default: str = UNKNOWN) -> str:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return str(value)
    return default


def _get_sessions_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = _pick_first(payload, SESSION_LIST_KEYS, default=[])
    else:
        items = []
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _get_next(payload: Any) -> Any:
    return _pick_first(payload, NEXT_KEYS) if isinstance(payload, dict) else None


def format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days, remaining = divmod(seconds, 86400)
    hours, remaining = divmod(remaining, 3600)
    minutes, seconds = divmod(remaining, 60)
    if days:
        return f"{days}d {hours:02}:{minutes:02}:{seconds:02}"
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def parse_sessions(sessions: list[dict[str, Any]], server: FlussonicServer) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for session in sessions:
        rows.append(
            {
                "server_id": server.id,
                "server": server.name,
                "channel": _extract_field(session, ("name", "stream", "channel", "stream_name")),
                "login": _extract_field(session, ("user_id", "login", "username", "user", "account")),
                "ip": _extract_field(session, ("ip", "client_ip", "remote_ip", "addr", "address")),
            }
        )
    return rows


def build_summary(rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    totals_map: dict[str, dict[str, Any]] = {}
    per_server_map: dict[tuple[str, str], dict[str, Any]] = {}

    for row in rows:
        login = row["login"]
        server = row["server"]
        ip = row["ip"]

        total = totals_map.setdefault(
            login,
            {"login": login, "sessions": 0, "ips": set(), "servers": set()},
        )
        total["sessions"] += 1
        if ip not in (UNKNOWN, "None", ""):
            total["ips"].add(ip)
        total["servers"].add(server)

        key = (login, server)
        item = per_server_map.setdefault(
            key,
            {"login": login, "server": server, "sessions": 0, "ips": set()},
        )
        item["sessions"] += 1
        if ip not in (UNKNOWN, "None", ""):
            item["ips"].add(ip)

    totals = [
        {
            "login": item["login"],
            "sessions": item["sessions"],
            "unique_ips": len(item["ips"]),
            "server_count": len(item["servers"]),
            "servers": ", ".join(sorted(item["servers"])),
        }
        for item in totals_map.values()
    ]
    totals.sort(key=lambda item: (-item["sessions"], item["login"].casefold()))

    per_server = [
        {
            "login": item["login"],
            "server": item["server"],
            "sessions": item["sessions"],
            "unique_ips": len(item["ips"]),
            "ips": ", ".join(sorted(item["ips"])) or "-",
        }
        for item in per_server_map.values()
    ]
    per_server.sort(key=lambda item: (-item["sessions"], item["login"].casefold(), item["server"].casefold()))
    return totals, per_server


class SessionsMonitor:
    def __init__(self, settings: Settings, server_store: ServerStore, data_store: DataStore | None = None):
        self.settings = settings
        self.server_store = server_store
        self.data_store = data_store
        self._task: asyncio.Task[None] | None = None
        self._snapshot: dict[str, Any] = self._empty_snapshot()
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._stream_uptime: dict[tuple[str, str], dict[str, int | str]] = {}
        self._collect_lock = asyncio.Lock()

    def _empty_snapshot(self) -> dict[str, Any]:
        return {
            "ts": int(time.time()),
            "poll_seconds": self.settings.monitor_poll_seconds,
            "metrics": {
                "sessions": 0,
                "logins": 0,
                "unique_ips": 0,
                "active_streams": 0,
            },
            "server_counts": [],
            "totals": [],
            "per_server": [],
            "sessions": [],
            "streams_uptime": [],
            "errors": [],
        }

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._poll_loop(), name="flussonic-sessions-monitor")

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    def snapshot(self) -> dict[str, Any]:
        return copy.deepcopy(self._snapshot)

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=3)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)

    async def refresh_now(self) -> dict[str, Any]:
        async with self._collect_lock:
            payload = await self._collect()
            self._snapshot = payload
            self._publish(payload)
        return self.snapshot()

    def _publish(self, payload: dict[str, Any]) -> None:
        for queue in tuple(self._subscribers):
            if queue.full():
                with suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            with suppress(asyncio.QueueFull):
                queue.put_nowait(payload)

    async def _poll_loop(self) -> None:
        while True:
            started = time.monotonic()
            try:
                async with self._collect_lock:
                    payload = await self._collect()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # monitor must stay alive even after an unexpected response
                payload = self._empty_snapshot()
                payload["errors"] = [f"Ошибка мониторинга: {exc}"]
            self._snapshot = payload
            if self.data_store:
                self.data_store.save_monitor_point(payload, self.settings.monitor_history_seconds)
                self.data_store.prune_monitor(int(time.time()) - self.settings.monitor_retention_days * 86400)
            self._publish(payload)
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.25, self.settings.monitor_poll_seconds - elapsed))

    async def _collect(self) -> dict[str, Any]:
        servers = list(self.server_store.enabled())
        now_ts = int(time.time())
        if not servers:
            payload = self._empty_snapshot()
            payload["ts"] = now_ts
            payload["errors"] = ["Нет включённых Flussonic-серверов"]
            return payload

        results = await asyncio.gather(
            *(self._fetch_server(server) for server in servers),
            return_exceptions=True,
        )
        rows: list[dict[str, str]] = []
        errors: list[str] = []
        server_counts: list[dict[str, Any]] = []

        for server, result in zip(servers, results):
            if isinstance(result, Exception):
                message = str(result)
                errors.append(f"{server.name}: {message}")
                server_counts.append(
                    {
                        "server_id": server.id,
                        "server": server.name,
                        "sessions": 0,
                        "online": False,
                        "error": message,
                    }
                )
                continue
            rows.extend(result)
            server_counts.append(
                {
                    "server_id": server.id,
                    "server": server.name,
                    "sessions": len(result),
                    "online": True,
                    "error": None,
                }
            )

        totals, per_server = build_summary(rows)
        streams_uptime = self._build_streams_uptime(rows, now_ts)
        unique_ips = {
            row["ip"]
            for row in rows
            if row["ip"] not in (UNKNOWN, "None", "")
        }

        return {
            "ts": now_ts,
            "poll_seconds": self.settings.monitor_poll_seconds,
            "metrics": {
                "sessions": len(rows),
                "logins": len({row["login"] for row in rows}),
                "unique_ips": len(unique_ips),
                "active_streams": len(streams_uptime),
            },
            "server_counts": server_counts,
            "totals": totals,
            "per_server": per_server,
            "sessions": rows,
            "streams_uptime": streams_uptime,
            "errors": errors,
        }

    async def _fetch_server(self, server: FlussonicServer) -> list[dict[str, str]]:
        sessions = await self._fetch_all_sessions(server)
        return parse_sessions(sessions, server)

    async def _fetch_all_sessions(self, server: FlussonicServer) -> list[dict[str, Any]]:
        all_sessions: list[dict[str, Any]] = []
        next_ref: Any = None
        seen_next: set[str] = set()
        base_url = f"{server.url}{self.settings.sessions_path}"

        async with httpx.AsyncClient(
            auth=httpx.BasicAuth(server.username, server.password),
            timeout=self.settings.session_request_timeout,
            verify=server.verify_tls,
            follow_redirects=True,
        ) as client:
            for _ in range(self.settings.monitor_max_pages):
                params: dict[str, Any] | None = None
                current_url = base_url
                if next_ref:
                    if isinstance(next_ref, str) and next_ref.startswith(("http://", "https://")):
                        current_url = next_ref
                    elif isinstance(next_ref, str) and next_ref.startswith("/"):
                        current_url = urljoin(server.url + "/", next_ref.lstrip("/"))
                    else:
                        params = {"cursor": next_ref}

                response = await client.get(current_url, params=params)
                try:
                    body: Any = response.json() if response.content else {}
                except ValueError:
                    body = response.text[:1000]
                if response.status_code >= 400:
                    detail = body.get("error") if isinstance(body, dict) else body
                    raise RuntimeError(f"HTTP {response.status_code}: {detail or response.reason_phrase}")

                all_sessions.extend(_get_sessions_list(body))
                next_ref = _get_next(body)
                if not next_ref:
                    return all_sessions

                marker = str(next_ref)
                if marker in seen_next:
                    raise RuntimeError("Обнаружен цикл пагинации sessions API")
                seen_next.add(marker)

        raise RuntimeError(f"Превышен лимит пагинации ({self.settings.monitor_max_pages} страниц)")

    def _build_streams_uptime(self, rows: list[dict[str, str]], now_ts: int) -> list[dict[str, Any]]:
        current_counts: dict[tuple[str, str], int] = defaultdict(int)
        names: dict[tuple[str, str], str] = {}
        for row in rows:
            key = (row["server_id"], row["channel"])
            current_counts[key] += 1
            names[key] = row["server"]

        for key in current_counts:
            previous = self._stream_uptime.get(key)
            if not previous:
                self._stream_uptime[key] = {
                    "first_seen": now_ts,
                    "last_seen": now_ts,
                    "server": names[key],
                }
            else:
                if now_ts - int(previous["last_seen"]) > self.settings.uptime_reset_gap:
                    previous["first_seen"] = now_ts
                previous["last_seen"] = now_ts
                previous["server"] = names[key]

        for key, value in list(self._stream_uptime.items()):
            if now_ts - int(value["last_seen"]) > self.settings.uptime_prune_after:
                del self._stream_uptime[key]

        streams: list[dict[str, Any]] = []
        for (server_id, channel), count in current_counts.items():
            value = self._stream_uptime[(server_id, channel)]
            uptime_sec = now_ts - int(value["first_seen"])
            streams.append(
                {
                    "server_id": server_id,
                    "server": str(value["server"]),
                    "channel": channel,
                    "sessions": count,
                    "first_seen": int(value["first_seen"]),
                    "uptime_sec": uptime_sec,
                    "uptime": format_duration(uptime_sec),
                }
            )

        streams.sort(key=lambda item: (-item["uptime_sec"], -item["sessions"], item["channel"].casefold()))
        return streams
