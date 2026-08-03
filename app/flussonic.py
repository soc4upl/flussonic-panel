from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import quote

import httpx

from .config import FlussonicServer, Settings


class FlussonicError(RuntimeError):
    pass


@dataclass
class ServerOperationResult:
    server_id: str
    server_name: str
    ok: bool
    status_code: int | None = None
    data: Any | None = None
    error: str | None = None
    elapsed_ms: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "server_id": self.server_id,
            "server_name": self.server_name,
            "ok": self.ok,
            "status_code": self.status_code,
            "data": self.data,
            "error": self.error,
            "elapsed_ms": self.elapsed_ms,
        }


class FlussonicClient:
    """Async Flussonic client with persistent connection pools.

    A separate pool is kept for TLS verification on/off because httpx configures
    certificate verification at client level. Authentication stays per request.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        timeout = httpx.Timeout(settings.request_timeout)
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=30, keepalive_expiry=30.0)
        common = {
            "timeout": timeout,
            "follow_redirects": True,
            "limits": limits,
            "headers": {"Accept": "application/json", "User-Agent": "Cyrius-Stream-Control/5.9.3"},
        }
        self._clients = {
            True: httpx.AsyncClient(verify=True, **common),
            False: httpx.AsyncClient(verify=False, **common),
        }

    async def aclose(self) -> None:
        await asyncio.gather(*(client.aclose() for client in self._clients.values()))

    @staticmethod
    def _auth(server: FlussonicServer) -> httpx.BasicAuth:
        return httpx.BasicAuth(server.username, server.password)

    async def _request(
        self,
        server: FlussonicServer,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{server.url}{path}"
        pooled_client = self._clients[bool(server.verify_tls)]
        try:
            response = await pooled_client.request(
                method,
                url,
                auth=self._auth(server),
                json=json_body,
                params=params,
            )
        except httpx.HTTPError as exc:
            raise FlussonicError(f"Connection error: {exc}") from exc

        try:
            body = response.json() if response.content else None
        except ValueError:
            body = response.text[:1000]

        if response.status_code >= 400:
            detail = body.get("error") if isinstance(body, dict) else body
            raise FlussonicError(f"HTTP {response.status_code}: {detail or response.reason_phrase}")
        return body

    async def request_text(self, server: FlussonicServer, path: str) -> str:
        url = f"{server.url}{path}"
        pooled_client = self._clients[bool(server.verify_tls)]
        try:
            response = await pooled_client.get(
                url,
                auth=self._auth(server),
                headers={"Accept": "text/plain, application/openmetrics-text, */*"},
            )
        except httpx.HTTPError as exc:
            raise FlussonicError(f"Connection error: {exc}") from exc
        if response.status_code >= 400:
            detail = response.text[:500].strip() or response.reason_phrase
            raise FlussonicError(f"HTTP {response.status_code}: {detail}")
        return response.text

    async def request_external_text(self, url: str, *, verify_tls: bool = True, timeout: float | None = None) -> str:
        """Fetch a Prometheus/OpenMetrics endpoint without Flussonic authentication."""
        pooled_client = self._clients[bool(verify_tls)]
        request_options: dict[str, Any] = {}
        if timeout is not None:
            request_options["timeout"] = timeout
        try:
            response = await pooled_client.get(
                url,
                headers={"Accept": "text/plain, application/openmetrics-text, */*"},
                **request_options,
            )
        except httpx.HTTPError as exc:
            raise FlussonicError(f"Connection error: {exc}") from exc
        if response.status_code >= 400:
            detail = response.text[:500].strip() or response.reason_phrase
            raise FlussonicError(f"HTTP {response.status_code}: {detail}")
        return response.text

    async def runtime_metrics(self, server: FlussonicServer) -> tuple[str, str]:
        errors: list[str] = []
        for path in self.settings.runtime_metrics_paths:
            try:
                text = await self.request_text(server, path)
                if text.strip():
                    return path, text
                errors.append(f"{path}: empty response")
            except FlussonicError as exc:
                errors.append(f"{path}: {exc}")
        raise FlussonicError("Runtime metrics unavailable: " + "; ".join(errors))

    async def aggregate_stream_stats(self, server: FlussonicServer) -> dict[str, Any]:
        body = await self._request(
            server,
            "GET",
            "/streamer/api/v3/streams",
            params={"limit": 10000, "select": "name,stats"},
        )
        totals = {
            "streams": 0,
            "running_streams": 0,
            "alive_streams": 0,
            "cpu_units": 0.0,
            "ram_bytes": 0.0,
            "input_bandwidth_bps": 0.0,
            "output_bandwidth_bps": 0.0,
            "transcoder_overloaded": 0,
        }
        for item in self.extract_streams(body):
            stats = item.get("stats") if isinstance(item.get("stats"), dict) else {}
            totals["streams"] += 1
            totals["running_streams"] += int(bool(stats.get("running")))
            totals["alive_streams"] += int(bool(stats.get("alive")))
            totals["cpu_units"] += float(stats.get("cpu_units") or 0)
            totals["ram_bytes"] += float(stats.get("ram_bytes") or 0)
            totals["input_bandwidth_bps"] += float(stats.get("inputs_bandwidth") or 0)
            totals["output_bandwidth_bps"] += float(stats.get("output_bandwidth") or 0)
            totals["transcoder_overloaded"] += int(bool(stats.get("transcoder_overloaded")))
        return totals

    async def server_status(self, server: FlussonicServer) -> dict[str, Any]:
        started = asyncio.get_running_loop().time()
        try:
            body = await self._request(
                server,
                "GET",
                "/streamer/api/v3/streams",
                params={"limit": 1},
            )
            latency_ms = round((asyncio.get_running_loop().time() - started) * 1000)
            return {
                "id": server.id,
                "name": server.name,
                "url": server.url,
                "primary": server.primary,
                "online": True,
                "latency_ms": latency_ms,
                "error": None,
                "stream_count_hint": self._count_hint(body),
            }
        except FlussonicError as exc:
            latency_ms = round((asyncio.get_running_loop().time() - started) * 1000)
            return {
                "id": server.id,
                "name": server.name,
                "url": server.url,
                "primary": server.primary,
                "online": False,
                "latency_ms": latency_ms,
                "error": str(exc),
                "stream_count_hint": None,
            }

    @staticmethod
    def _count_hint(body: Any) -> int | None:
        if isinstance(body, list):
            return len(body)
        if isinstance(body, dict):
            for key in ("total", "count"):
                if isinstance(body.get(key), int):
                    return body[key]
            for key in ("streams", "items", "results", "data"):
                if isinstance(body.get(key), list):
                    return len(body[key])
        return None

    @staticmethod
    def extract_streams(body: Any) -> list[dict[str, Any]]:
        if isinstance(body, list):
            items = body
        elif isinstance(body, dict):
            items = None
            for key in ("streams", "items", "results", "data"):
                if isinstance(body.get(key), list):
                    items = body[key]
                    break
            if items is None and body.get("name"):
                items = [body]
            if items is None:
                items = []
        else:
            items = []
        return [item for item in items if isinstance(item, dict) and item.get("name")]

    @staticmethod
    def disk_config(stream: dict[str, Any]) -> dict[str, Any]:
        config = stream.get("config_on_disk")
        return config if isinstance(config, dict) else stream

    @classmethod
    def normalize_stream(cls, stream: dict[str, Any]) -> dict[str, Any]:
        config = cls.disk_config(stream)
        runtime_inputs = stream.get("inputs") if isinstance(stream.get("inputs"), list) else []
        config_inputs = config.get("inputs") if isinstance(config.get("inputs"), list) else runtime_inputs
        stats = stream.get("stats") if isinstance(stream.get("stats"), dict) else {}
        return {
            "name": config.get("name") or stream.get("name"),
            "title": config.get("title") or stream.get("title") or "",
            "provider": config.get("provider") or stream.get("provider") or "",
            "position": config.get("position", stream.get("position", 0)) or 0,
            "static": bool(config.get("static", stream.get("static", False))),
            "disabled": bool(config.get("disabled", stream.get("disabled", False))),
            "inputs": [
                {"url": item.get("url", "")}
                for item in config_inputs
                if isinstance(item, dict) and item.get("url")
            ],
            "on_play": (config.get("on_play") or stream.get("on_play") or {}).get("url")
            if isinstance(config.get("on_play") or stream.get("on_play"), dict)
            else None,
            "status": stats.get("status", "unknown"),
            "running": bool(stats.get("running", False)),
            "alive": bool(stats.get("alive", False)),
            "named_by": stream.get("named_by"),
        }

    async def list_streams(self, server: FlussonicServer) -> list[dict[str, Any]]:
        body = await self._request(
            server,
            "GET",
            "/streamer/api/v3/streams",
            params={"limit": 10000},
        )
        streams = [self.normalize_stream(item) for item in self.extract_streams(body)]
        return sorted(streams, key=lambda item: (item.get("position", 0), item["name"].lower()))

    async def list_stream_configs(self, server: FlussonicServer) -> dict[str, dict[str, Any]]:
        body = await self._request(server, "GET", "/streamer/api/v3/streams", params={"limit": 10000, "select": "name,config_on_disk"})
        result: dict[str, dict[str, Any]] = {}
        for item in self.extract_streams(body):
            config = dict(self.disk_config(item))
            name = config.get("name") or item.get("name")
            if name:
                result[str(name)] = config
        return result

    async def get_stream(self, server: FlussonicServer, name: str) -> dict[str, Any]:
        encoded_name = quote(name, safe="/")
        body = await self._request(server, "GET", f"/streamer/api/v3/streams/{encoded_name}")
        if not isinstance(body, dict):
            raise FlussonicError("Unexpected stream response")
        return body

    async def put_stream(self, server: FlussonicServer, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        encoded_name = quote(name, safe="/")
        body = await self._request(
            server,
            "PUT",
            f"/streamer/api/v3/streams/{encoded_name}",
            json_body=payload,
        )
        return body if isinstance(body, dict) else {"response": body}

    async def delete_stream(self, server: FlussonicServer, name: str) -> Any:
        encoded_name = quote(name, safe="/")
        return await self._request(server, "DELETE", f"/streamer/api/v3/streams/{encoded_name}")

    @classmethod
    def canonical_config(cls, config: dict[str, Any] | None) -> dict[str, Any]:
        """Normalize old/new Flussonic representations for meaningful comparison.

        Position and runtime/service fields are intentionally ignored: they do not
        change stream playback and frequently differ after imports or reordering.
        """
        if not isinstance(config, dict):
            return {}
        ignored = {
            "stats", "cluster_key", "named_by", "position", "runtime",
            "last_error", "source_error", "effective", "config_on_disk",
        }

        def clean(value: Any, key: str = "") -> Any:
            if isinstance(value, dict):
                result = {}
                for child_key, child_value in value.items():
                    if child_key in ignored:
                        continue
                    normalized = clean(child_value, child_key)
                    if normalized not in (None, "", [], {}):
                        result[child_key] = normalized
                return result
            if isinstance(value, list):
                result = [clean(item, key) for item in value]
                return [item for item in result if item not in (None, "", [], {})]
            if key == "on_play" and isinstance(value, str):
                return {"url": value}
            return value

        normalized = clean(dict(config))
        on_play = normalized.get("on_play")
        if isinstance(on_play, str):
            normalized["on_play"] = {"url": on_play}
        inputs = normalized.get("inputs")
        if isinstance(inputs, list):
            fixed = []
            for item in inputs:
                if isinstance(item, str):
                    fixed.append({"url": item})
                elif isinstance(item, dict):
                    fixed.append(item)
            normalized["inputs"] = fixed
        return normalized

    @classmethod
    def config_hash(cls, config: dict[str, Any] | None) -> str:
        encoded = json.dumps(cls.canonical_config(config), sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()[:12]

    @classmethod
    def config_diff(cls, base: dict[str, Any] | None, other: dict[str, Any] | None) -> list[str]:
        left, right = cls.canonical_config(base), cls.canonical_config(other)
        keys = sorted(set(left) | set(right))
        return [key for key in keys if left.get(key) != right.get(key)]

    @staticmethod
    def select_servers(
        all_servers: Iterable[FlussonicServer], target_ids: list[str] | None
    ) -> list[FlussonicServer]:
        enabled = [server for server in all_servers if server.enabled]
        if target_ids is None:
            return enabled
        requested = set(target_ids)
        selected = [server for server in enabled if server.id in requested]
        missing = requested - {server.id for server in selected}
        if missing:
            raise FlussonicError(f"Unknown or disabled server IDs: {', '.join(sorted(missing))}")
        return selected

    async def run_many(self, servers: list[FlussonicServer], operation) -> list[dict[str, Any]]:
        async def one(server: FlussonicServer) -> ServerOperationResult:
            started = asyncio.get_running_loop().time()
            try:
                data = await operation(server)
                elapsed_ms = round((asyncio.get_running_loop().time() - started) * 1000)
                if isinstance(data, dict) and (data.get("name") or data.get("config_on_disk")):
                    data = {**data, "normalized_stream": self.normalize_stream(data)}
                return ServerOperationResult(server.id, server.name, True, 200, data=data, elapsed_ms=elapsed_ms)
            except FlussonicError as exc:
                elapsed_ms = round((asyncio.get_running_loop().time() - started) * 1000)
                return ServerOperationResult(server.id, server.name, False, error=str(exc), elapsed_ms=elapsed_ms)

        results = await asyncio.gather(*(one(server) for server in servers))
        return [result.as_dict() for result in results]
