from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any

from .datastore import DataStore
from .flussonic import FlussonicClient
from .notifications import NotificationService
from .server_store import ServerStore


BAD_STATUSES = {"error", "offline", "failed", "failure", "dead"}
UNKNOWN_STATUSES = {"", "unknown", "none", "n/a"}


class StreamStatusMonitor:
    """Lightweight Flussonic runtime-status monitor.

    This monitor never opens/probes input URLs and never invokes ffprobe. It only
    reads the normal Flussonic streams API and reacts to explicit runtime states
    such as ``error``/``offline``. Ondemand ``waiting`` is therefore normal.
    """

    def __init__(
        self,
        servers: ServerStore,
        client: FlussonicClient,
        store: DataStore,
        notifications: NotificationService,
    ):
        self.servers = servers
        self.client = client
        self.store = store
        self.notifications = notifications
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._bad_counts: dict[tuple[str, str], int] = {}
        self._alerted: set[tuple[str, str]] = set()
        self._last_status: dict[tuple[str, str], str] = {}

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._poll_loop(), name="flussonic-stream-status-monitor")

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    @staticmethod
    def _is_bad(status: Any) -> bool:
        value = str(status or "").strip().lower()
        if value in BAD_STATUSES:
            return True
        return value.startswith("error") or value.startswith("offline") or value.startswith("failed")

    def _should_watch(self, server_id: str, stream_name: str) -> bool:
        # In mirror mode every configured copy is meaningful. In hybrid mode an
        # Assigned stream is monitored only on the selected target CDN so an old
        # extra copy cannot generate a misleading alert.
        if not bool(self.store.get_setting("placement_enabled", False)):
            return True
        placement = self.store.placement(stream_name)
        if not placement or placement.get("mode") != "assigned":
            return True
        return placement.get("primary_server_id") == server_id

    async def refresh_now(self) -> dict[str, Any]:
        async with self._lock:
            return await self._collect()

    async def _collect(self) -> dict[str, Any]:
        config = self.notifications.status_monitor_settings()
        confirmations = int(config["confirmations"])
        checked = 0
        bad = 0
        sent = 0
        recovered = 0
        errors: list[str] = []
        seen: set[tuple[str, str]] = set()

        async def fetch(server):
            try:
                return server, await self.client.list_streams(server), None
            except Exception as exc:  # server availability has its own alert path
                return server, [], str(exc)

        results = await asyncio.gather(*(fetch(server) for server in self.servers.enabled()))
        for server, streams, error in results:
            if error:
                errors.append(f"{server.name}: {error}")
                continue
            for stream in streams:
                name = str(stream.get("name") or "").strip()
                if not name or not self._should_watch(server.id, name):
                    continue
                key = (server.id, name)
                seen.add(key)
                if bool(stream.get("disabled")):
                    self._bad_counts.pop(key, None)
                    self._alerted.discard(key)
                    self._last_status[key] = "disabled"
                    continue

                status = str(stream.get("status") or "unknown").strip().lower()
                checked += 1
                previous = self._last_status.get(key)
                self._last_status[key] = status

                if self._is_bad(status):
                    bad += 1
                    count = self._bad_counts.get(key, 0) + 1
                    self._bad_counts[key] = count
                    if count >= confirmations and key not in self._alerted:
                        self._alerted.add(key)
                        title = str(stream.get("title") or "").strip()
                        label = f"{name} · {title}" if title and title != name else name
                        delivered = await self.notifications.send(
                            "stream_error",
                            "\n".join(
                                [
                                    "🔴 Поток OFFLINE / ERROR",
                                    f"Поток: {label}",
                                    f"Сервер: {server.name}",
                                    f"Статус: {status}",
                                    f"Подтверждений: {count}/{confirmations}",
                                ]
                            ),
                            "critical",
                        )
                        sent += int(bool(delivered))
                else:
                    self._bad_counts[key] = 0
                    # unknown is not enough evidence that an earlier failure is
                    # fixed. waiting, on the other hand, is a valid ondemand state.
                    if key in self._alerted and status not in UNKNOWN_STATUSES:
                        self._alerted.discard(key)
                        title = str(stream.get("title") or "").strip()
                        label = f"{name} · {title}" if title and title != name else name
                        delivered = await self.notifications.send(
                            "stream_recovered",
                            "\n".join(
                                [
                                    "🟢 Поток восстановлен",
                                    f"Поток: {label}",
                                    f"Сервер: {server.name}",
                                    f"Статус: {status}",
                                    f"Было: {previous or 'error'}",
                                ]
                            ),
                            "info",
                        )
                        recovered += int(bool(delivered))

        # Forget streams that no longer belong to the monitor. Missing Assigned
        # streams are handled by migration verification; they must not be confused
        # with explicit Flussonic runtime status=error/offline.
        for key in list(self._last_status):
            if key not in seen:
                self._last_status.pop(key, None)
                self._bad_counts.pop(key, None)
                self._alerted.discard(key)

        return {
            "ok": not errors,
            "checked": checked,
            "bad": bad,
            "alerts_delivered": sent,
            "recoveries_delivered": recovered,
            "errors": errors,
            "poll_seconds": config["poll_seconds"],
            "confirmations": confirmations,
        }

    async def _poll_loop(self) -> None:
        while True:
            config = self.notifications.status_monitor_settings()
            if config["active"]:
                try:
                    await self.refresh_now()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A background notification monitor must never stop the panel.
                    pass
                await asyncio.sleep(float(config["poll_seconds"]))
            else:
                await asyncio.sleep(5.0)
