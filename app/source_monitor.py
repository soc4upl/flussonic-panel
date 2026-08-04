from __future__ import annotations

import asyncio
import ssl
import time
from contextlib import suppress
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

from .config import Settings, FlussonicServer
from .datastore import DataStore
from .flussonic import FlussonicClient
from .notifications import NotificationService
from .server_store import ServerStore


class SourceMonitor:
    def __init__(self, settings: Settings, servers: ServerStore, client: FlussonicClient, store: DataStore, notifications: NotificationService):
        self.settings, self.servers, self.client, self.store, self.notifications = settings, servers, client, store, notifications
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._server_state: dict[str, bool] = {}
        limits = httpx.Limits(max_connections=max(20, settings.source_check_concurrency * 2), max_keepalive_connections=20)
        headers = {"User-Agent": "Cyrius-Source-Check/5.9.4"}
        self._http = {
            True: httpx.AsyncClient(timeout=settings.source_check_timeout, verify=True, follow_redirects=True, limits=limits, headers=headers),
            False: httpx.AsyncClient(timeout=settings.source_check_timeout, verify=False, follow_redirects=True, limits=limits, headers=headers),
        }

    async def start(self):
        """Source probes are deliberately manual-only.

        Kept as a lifecycle hook for compatibility, but it must never create a
        scheduler or touch any source until an authenticated API request calls
        :meth:`run_all`.
        """
        self._task = None

    async def stop(self):
        await asyncio.gather(*(client.aclose() for client in self._http.values()))

    @staticmethod
    def _probe_url(url: str) -> str | None:
        parts = urlsplit(url)
        scheme = parts.scheme.lower()
        mapping = {
            "http": "http",
            "https": "https",
            "hls": "http",
            "hlss": "https",
            "tshttp": "http",
            "tshttps": "https",
        }
        target_scheme = mapping.get(scheme)
        if not target_scheme:
            return None
        return urlunsplit((target_scheme, parts.netloc, parts.path, parts.query, parts.fragment))

    @staticmethod
    def _kind(url: str) -> str:
        return urlsplit(url).scheme.lower()

    async def _probe_http(self, server: FlussonicServer, url: str, probe_url: str) -> dict[str, Any]:
        scheme = self._kind(url)
        started = time.monotonic()
        client = self._http[bool(server.verify_tls)]
        request_headers = {"Range": "bytes=0-8191"} if scheme in {"hls", "hlss", "http", "https"} else {}
        async with client.stream("GET", probe_url, headers=request_headers) as response:
            sample = bytearray()
            async for chunk in response.aiter_bytes():
                sample.extend(chunk)
                if len(sample) >= 8192:
                    break
        latency = round((time.monotonic() - started) * 1000)
        if response.status_code >= 400:
            return {"state": "failed", "status_code": response.status_code, "latency_ms": latency, "detail": response.reason_phrase}
        if scheme in {"hls", "hlss"}:
            text = bytes(sample).decode("utf-8", errors="ignore").lstrip("\ufeff\r\n \t")
            if not text.startswith("#EXTM3U"):
                return {"state": "failed", "status_code": response.status_code, "latency_ms": latency, "detail": "Ответ не похож на HLS playlist (#EXTM3U не найден)"}
            return {"state": "ok", "status_code": response.status_code, "latency_ms": latency, "detail": "HLS playlist доступен"}
        if scheme in {"tshttp", "tshttps"}:
            if not sample:
                return {"state": "failed", "status_code": response.status_code, "latency_ms": latency, "detail": "HTTP MPEG-TS не передал данные"}
            sync = sample[0] == 0x47 or (len(sample) > 188 and sample[188] == 0x47)
            return {"state": "ok", "status_code": response.status_code, "latency_ms": latency, "detail": "MPEG-TS данные получены" if sync else "HTTP-источник передаёт данные; TS sync byte не подтверждён"}
        return {"state": "ok", "status_code": response.status_code, "latency_ms": latency, "detail": "HTTP-источник доступен"}

    async def _probe_m4f(self, server: FlussonicServer, url: str) -> dict[str, Any]:
        parts = urlsplit(url)
        host = parts.hostname
        if not host:
            return {"state": "failed", "detail": "В M4F URL отсутствует host"}
        secure = parts.scheme.lower() == "m4fs"
        port = parts.port or (443 if secure else 80)
        ssl_context: ssl.SSLContext | bool | None = None
        if secure:
            if server.verify_tls:
                ssl_context = ssl.create_default_context()
            else:
                ssl_context = ssl._create_unverified_context()
        started = time.monotonic()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=ssl_context, server_hostname=host if secure else None),
                timeout=self.settings.source_check_timeout,
            )
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()
            return {
                "state": "reachable",
                "latency_ms": round((time.monotonic() - started) * 1000),
                "detail": "M4F TCP-порт доступен; наличие медиаданных подтверждает состояние потока во Flussonic",
            }
        except Exception as exc:
            return {"state": "failed", "latency_ms": round((time.monotonic() - started) * 1000), "detail": str(exc)[:500]}

    async def _probe(self, server: FlussonicServer, stream_name: str, url: str, semaphore: asyncio.Semaphore) -> dict[str, Any]:
        now = int(time.time())
        base = {"server_id": server.id, "server_name": server.name, "stream_name": stream_name, "input_url": url, "checked_at": now}
        scheme = self._kind(url)
        async with semaphore:
            try:
                if scheme in {"m4f", "m4fs"}:
                    return base | await self._probe_m4f(server, url)
                probe_url = self._probe_url(url)
                if probe_url:
                    return base | await self._probe_http(server, url, probe_url)
                return base | {"state": "unsupported", "detail": "Протокол проверяется через диагностику состояния Flussonic"}
            except Exception as exc:
                return base | {"state": "failed", "detail": str(exc)[:500]}

    async def run_all(self, stream_name: str | None = None, server_id: str | None = None) -> dict[str, Any]:
        async with self._lock:
            semaphore = asyncio.Semaphore(self.settings.source_check_concurrency)
            tasks = []
            errors = []
            removed_stale = 0
            enabled = list(self.servers.enabled())
            primary = self.servers.primary()
            if server_id:
                selected = self.servers.get(server_id)
                source_servers = [selected] if selected and selected.enabled else []
            else:
                source_servers = enabled if self.settings.source_check_all_servers else ([primary] if primary else [])

            # Cheap server health check for every node; source probes are primary-only by default.
            for server in enabled:
                status = await self.client.server_status(server)
                online = bool(status.get("online"))
                if not online and self._server_state.get(server.id, True):
                    await self.notifications.send("server_offline", f"🔴 Flussonic недоступен\nСервер: {server.name}\nОшибка: {status.get('error')}")
                self._server_state[server.id] = online
                if not online:
                    errors.append(f"{server.name}: {status.get('error')}")

            for server in source_servers:
                if not self._server_state.get(server.id, False):
                    continue
                try:
                    if stream_name:
                        raw = await self.client.get_stream(server, stream_name)
                        configs = {stream_name: self.client.disk_config(raw)}
                    else:
                        configs = await self.client.list_stream_configs(server)
                except Exception as exc:
                    errors.append(f"{server.name}: {exc}")
                    continue
                current_sources: dict[str, set[str]] = {}
                for name, config in configs.items():
                    source_urls = {
                        str(inp["url"])
                        for inp in (config.get("inputs", []) if isinstance(config.get("inputs"), list) else [])
                        if isinstance(inp, dict) and inp.get("url")
                    }
                    current_sources[str(name)] = source_urls
                    if bool(config.get("disabled")):
                        continue
                    for url in source_urls:
                        tasks.append(self._probe(server, str(name), url, semaphore))

                # The server configuration was fetched successfully, so cached
                # checks absent from the live config are now safe to remove.
                removed_stale += self.store.reconcile_source_checks(
                    server_id=server.id,
                    current_sources=current_sources,
                    stream_name=stream_name,
                )

            results = await asyncio.gather(*tasks) if tasks else []
            failed = 0
            for item in results:
                old = self.store.upsert_source_check(item)
                if item["state"] == "failed":
                    failed += 1
                    if not old or old.get("state") != "failed":
                        safe_url = item["input_url"].split("?", 1)[0] + ("?…" if "?" in item["input_url"] else "")
                        await self.notifications.send("source_failed", f"⚠️ Источник недоступен\nСервер: {item['server_name']}\nПоток: {item['stream_name']}\nInput: {safe_url}\nОшибка: {item.get('detail') or item.get('status_code')}")
            return {"ok": not errors, "checked": len(results), "failed": failed, "removed_stale": removed_stale, "errors": errors, "items": results}
