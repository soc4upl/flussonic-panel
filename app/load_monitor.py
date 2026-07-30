from __future__ import annotations

import asyncio
import copy
import math
import re
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .config import FlussonicServer, Settings
from .datastore import DataStore
from .flussonic import FlussonicClient, FlussonicError
from .monitor import SessionsMonitor
from .notifications import NotificationService
from .server_store import ServerStore

_SAMPLE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+"
    r"([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?|NaN|[+-]?Inf)(?:\s+\d+)?$"
)
_LABEL_RE = re.compile(r'(\w+)="((?:\\.|[^"\\])*)"')
_PSEUDO_FS = {"tmpfs", "devtmpfs", "proc", "sysfs", "overlay", "squashfs", "cgroup", "cgroup2", "tracefs"}
_VIRTUAL_NET_PREFIXES = ("veth", "docker", "br-", "virbr", "cni", "flannel", "cali", "ifb")


@dataclass
class Sample:
    name: str
    labels: dict[str, str]
    value: float


def parse_prometheus(text: str) -> list[Sample]:
    samples: list[Sample] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE_RE.match(line)
        if not match:
            continue
        raw_value = match.group(3)
        try:
            value = float(raw_value)
        except ValueError:
            continue
        if not math.isfinite(value):
            continue
        labels: dict[str, str] = {}
        for key, raw in _LABEL_RE.findall(match.group(2) or ""):
            labels[key] = bytes(raw, "utf-8").decode("unicode_escape")
        samples.append(Sample(match.group(1), labels, value))
    return samples


def _lname(sample: Sample) -> str:
    return sample.name.lower()


def _first_value(samples: list[Sample], exact: tuple[str, ...] = (), contains: tuple[str, ...] = ()) -> float | None:
    exact_set = {item.lower() for item in exact}
    for sample in samples:
        name = _lname(sample)
        if name in exact_set:
            return sample.value
    for token in contains:
        token = token.lower()
        for sample in samples:
            name = _lname(sample)
            if token in name and not name.endswith("_total") and "seconds_total" not in name:
                return sample.value
    return None


def _sum_values(samples: list[Sample], names: tuple[str, ...], *, exclude_loopback: bool = False) -> float | None:
    wanted = {name.lower() for name in names}
    values = []
    for sample in samples:
        if _lname(sample) not in wanted:
            continue
        device = sample.labels.get("device") or sample.labels.get("interface") or ""
        if exclude_loopback and device in {"lo", "loopback"}:
            continue
        values.append(sample.value)
    return sum(values) if values else None




def effective_node_exporter_url(server: FlussonicServer) -> str:
    """Return configured Node Exporter URL or infer http://HOST:9100/metrics."""
    raw = (server.node_exporter_url or "").strip().rstrip("/")
    if raw:
        parsed = urlsplit(raw)
        path = parsed.path or ""
        if path in {"", "/"}:
            path = "/metrics"
        return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment))
    parsed = urlsplit(server.url)
    hostname = parsed.hostname
    if not hostname:
        return ""
    host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    return f"http://{host}:9100/metrics"


def network_only_exporter_url(url: str) -> str:
    """Ask node_exporter for the netdev collector only when supported."""
    parsed = urlsplit(url)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if not any(key == "collect[]" for key, _ in query):
        query.append(("collect[]", "netdev"))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query, doseq=True), parsed.fragment))


def _network_counters(samples: list[Sample]) -> dict[str, dict[str, float]]:
    counters: dict[str, dict[str, float]] = {}
    rx_names = {"node_network_receive_bytes_total", "network_receive_bytes_total", "system_network_receive_bytes_total"}
    tx_names = {"node_network_transmit_bytes_total", "network_transmit_bytes_total", "system_network_transmit_bytes_total"}
    up_names = {"node_network_up", "network_up"}
    for sample in samples:
        name = _lname(sample)
        device = sample.labels.get("device") or sample.labels.get("interface") or ""
        if not device or device in {"lo", "loopback"}:
            continue
        row = counters.setdefault(device, {
            "rx_total_bytes": 0.0, "tx_total_bytes": 0.0, "up": 1.0,
            "virtual": float(device.startswith(_VIRTUAL_NET_PREFIXES)),
        })
        if name in rx_names:
            row["rx_total_bytes"] += sample.value
        elif name in tx_names:
            row["tx_total_bytes"] += sample.value
        elif name in up_names:
            row["up"] = sample.value
    return counters


def _percent(value: float | None, name_hint: str = "") -> float | None:
    if value is None:
        return None
    if value <= 1.0 and any(token in name_hint for token in ("ratio", "utilization", "usage")):
        value *= 100.0
    return max(0.0, min(100.0, value))


def _metric_by_tokens(samples: list[Sample], include: tuple[str, ...], exclude: tuple[str, ...] = ()) -> Sample | None:
    for sample in samples:
        name = _lname(sample)
        if all(token in name for token in include) and not any(token in name for token in exclude):
            return sample
    return None


def _filesystem_usage(samples: list[Sample]) -> tuple[float | None, float | None, float | None]:
    sizes: dict[tuple[str, str], float] = {}
    available: dict[tuple[str, str], float] = {}
    for sample in samples:
        name = _lname(sample)
        mount = sample.labels.get("mountpoint") or sample.labels.get("path") or sample.labels.get("device") or "default"
        fstype = (sample.labels.get("fstype") or "").lower()
        if fstype in _PSEUDO_FS or mount.startswith(("/proc", "/sys", "/run")):
            continue
        key = (mount, sample.labels.get("device", ""))
        if name in {"node_filesystem_size_bytes", "filesystem_size_bytes", "system_disk_total_bytes"} or ("filesystem" in name and "size_bytes" in name):
            sizes[key] = max(sizes.get(key, 0.0), sample.value)
        elif name in {"node_filesystem_avail_bytes", "node_filesystem_free_bytes", "filesystem_avail_bytes", "system_disk_free_bytes"} or ("filesystem" in name and ("avail_bytes" in name or "free_bytes" in name)):
            available[key] = max(available.get(key, 0.0), sample.value)
    if not sizes:
        total = _first_value(samples, contains=("disk_total_bytes", "filesystem_total_bytes"))
        free = _first_value(samples, contains=("disk_free_bytes", "filesystem_free_bytes", "disk_available_bytes"))
        if total and free is not None:
            used = max(0.0, total - free)
            return total, used, 100.0 * used / total if total else None
        return None, None, None
    total = 0.0
    free = 0.0
    seen_mounts: set[str] = set()
    for key, size in sizes.items():
        mount = key[0]
        if mount in seen_mounts:
            continue
        seen_mounts.add(mount)
        total += size
        free += min(size, available.get(key, 0.0))
    used = max(0.0, total - free)
    return total, used, 100.0 * used / total if total else None


class ServerLoadMonitor:
    def __init__(
        self,
        settings: Settings,
        server_store: ServerStore,
        client: FlussonicClient,
        data_store: DataStore,
        notifications: NotificationService,
        sessions_monitor: SessionsMonitor,
    ):
        self.settings = settings
        self.server_store = server_store
        self.client = client
        self.data_store = data_store
        self.notifications = notifications
        self.sessions_monitor = sessions_monitor
        self._task: asyncio.Task[None] | None = None
        self._network_task: asyncio.Task[None] | None = None
        self._snapshot = self._empty_snapshot()
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._collect_lock = asyncio.Lock()
        self._previous: dict[str, dict[str, float]] = {}
        self._network_previous: dict[str, dict[str, float]] = {}
        self._network_latest: dict[str, dict[str, Any]] = {}
        self._alert_state: dict[tuple[str, str], bool] = {}

    def _empty_snapshot(self) -> dict[str, Any]:
        return {
            "ts": int(time.time()),
            "enabled": self.settings.server_load_enabled,
            "poll_seconds": self.settings.server_load_poll_seconds,
            "network_poll_seconds": self.settings.server_network_poll_seconds,
            "thresholds": {
                "cpu": self.settings.server_load_cpu_warning,
                "memory": self.settings.server_load_memory_warning,
                "disk": self.settings.server_load_disk_warning,
            },
            "summary": {"servers": 0, "online": 0, "warnings": 0, "avg_cpu_percent": None, "avg_memory_percent": None, "network_rx_bps": 0, "network_tx_bps": 0, "network_bps": 0},
            "servers": [],
            "errors": [],
        }

    async def start(self) -> None:
        if not self.settings.server_load_enabled:
            return
        if not self._task or self._task.done():
            self._task = asyncio.create_task(self._poll_loop(), name="flussonic-server-load-monitor")
        if not self._network_task or self._network_task.done():
            self._network_task = asyncio.create_task(self._network_poll_loop(), name="flussonic-network-live-monitor")

    async def stop(self) -> None:
        tasks = [task for task in (self._task, self._network_task) if task]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._task = None
        self._network_task = None

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
            self._persist(payload)
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
            except Exception as exc:
                payload = self._empty_snapshot()
                payload["errors"] = [f"Ошибка мониторинга нагрузки: {exc}"]
            self._snapshot = payload
            self._persist(payload)
            self._publish(payload)
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.5, self.settings.server_load_poll_seconds - elapsed))

    async def _network_poll_loop(self) -> None:
        # The first sample establishes a counter baseline. The second and later
        # samples produce real RX/TX rates without polling the heavy Flussonic API.
        while True:
            started = time.monotonic()
            try:
                updates, errors = await self._collect_network_live()
                if updates:
                    async with self._collect_lock:
                        payload = self._merge_network_updates(self._snapshot, updates, errors)
                        self._snapshot = payload
                    self._publish(payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Keep the last known values. Full load polling continues separately.
                pass
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.1, self.settings.server_network_poll_seconds - elapsed))

    async def _collect_network_live(self) -> tuple[list[dict[str, Any]], list[str]]:
        servers = list(self.server_store.enabled())
        if not servers:
            return [], []
        results = await asyncio.gather(*(self._fetch_network_live(server) for server in servers), return_exceptions=True)
        updates: list[dict[str, Any]] = []
        errors: list[str] = []
        for server, result in zip(servers, results):
            if isinstance(result, Exception):
                errors.append(f"{server.name}: {result}")
                continue
            updates.append(result)
            self._network_latest[server.id] = result
        return updates, errors

    async def _fetch_network_live(self, server: FlussonicServer) -> dict[str, Any]:
        exporter_url = effective_node_exporter_url(server)
        if not exporter_url:
            raise FlussonicError("Node Exporter URL is not configured")
        fast_url = network_only_exporter_url(exporter_url)
        try:
            text = await self.client.request_external_text(
                fast_url, verify_tls=server.verify_tls, timeout=max(2.0, self.settings.server_network_poll_seconds * 2.5)
            )
            samples = parse_prometheus(text)
            if not _network_counters(samples):
                raise FlussonicError("netdev collector returned no network counters")
        except Exception:
            # Older node_exporter versions may not support collect[]=netdev.
            text = await self.client.request_external_text(
                exporter_url, verify_tls=server.verify_tls, timeout=max(2.0, self.settings.server_network_poll_seconds * 2.5)
            )
            samples = parse_prometheus(text)
            if not _network_counters(samples):
                raise FlussonicError("Node Exporter returned no network counters")
        return self._network_rates(server, samples, time.time())

    def _network_rates(self, server: FlussonicServer, samples: list[Sample], now: float) -> dict[str, Any]:
        network = _network_counters(samples)
        previous = self._network_previous.get(server.id) or {}
        elapsed = now - float(previous.get("ts") or now)
        current: dict[str, float] = {"ts": now}
        interfaces: list[dict[str, Any]] = []
        aggregate_rx = 0.0
        aggregate_tx = 0.0
        aggregate_rx_total = 0.0
        aggregate_tx_total = 0.0
        aggregate_found = False
        for device, counters in network.items():
            rx_key = f"rx:{device}"
            tx_key = f"tx:{device}"
            rx_rate = tx_rate = None
            if elapsed > 0 and rx_key in previous:
                delta = counters["rx_total_bytes"] - previous[rx_key]
                if delta >= 0:
                    rx_rate = delta * 8.0 / elapsed
            if elapsed > 0 and tx_key in previous:
                delta = counters["tx_total_bytes"] - previous[tx_key]
                if delta >= 0:
                    tx_rate = delta * 8.0 / elapsed
            current[rx_key] = counters["rx_total_bytes"]
            current[tx_key] = counters["tx_total_bytes"]
            virtual = bool(counters.get("virtual"))
            up = bool(counters.get("up", 1))
            interfaces.append({
                "device": device, "up": up, "virtual": virtual,
                "rx_bps": rx_rate, "tx_bps": tx_rate,
                "rx_total_bytes": counters["rx_total_bytes"],
                "tx_total_bytes": counters["tx_total_bytes"],
            })
            if not virtual and up:
                aggregate_found = True
                aggregate_rx += float(rx_rate or 0)
                aggregate_tx += float(tx_rate or 0)
                aggregate_rx_total += counters["rx_total_bytes"]
                aggregate_tx_total += counters["tx_total_bytes"]
        if not aggregate_found:
            aggregate_rx = sum(float(item.get("rx_bps") or 0) for item in interfaces)
            aggregate_tx = sum(float(item.get("tx_bps") or 0) for item in interfaces)
            aggregate_rx_total = sum(float(item.get("rx_total_bytes") or 0) for item in interfaces)
            aggregate_tx_total = sum(float(item.get("tx_total_bytes") or 0) for item in interfaces)
        interfaces.sort(key=lambda item: float(item.get("rx_bps") or 0) + float(item.get("tx_bps") or 0), reverse=True)
        self._network_previous[server.id] = current
        return {
            "server_id": server.id, "server": server.name, "url": server.url,
            "network_rx_bps": aggregate_rx if elapsed > 0 else None,
            "network_tx_bps": aggregate_tx if elapsed > 0 else None,
            "network_rx_total_bytes": aggregate_rx_total,
            "network_tx_total_bytes": aggregate_tx_total,
            "network_interfaces": interfaces,
            "network_ts": int(now),
        }

    def _merge_network_updates(
        self, payload: dict[str, Any], updates: list[dict[str, Any]], errors: list[str] | None = None
    ) -> dict[str, Any]:
        merged = copy.deepcopy(payload or self._empty_snapshot())
        by_id = {str(item.get("server_id")): item for item in merged.get("servers") or []}
        configured = {server.id: server for server in self.server_store.enabled()}
        for update in updates:
            server_id = str(update.get("server_id") or "")
            item = by_id.get(server_id)
            if item is None:
                server = configured.get(server_id)
                item = {
                    "server_id": server_id, "server": update.get("server") or (server.name if server else server_id),
                    "url": update.get("url") or (server.url if server else ""), "online": True,
                    "state": "warming", "source": "node_exporter", "limited": False,
                    "cpu_percent": None, "memory_percent": None, "disk_percent": None, "sessions": 0,
                }
                merged.setdefault("servers", []).append(item)
                by_id[server_id] = item
            item.update({key: value for key, value in update.items() if key.startswith("network_")})
            item["network_live"] = True
        merged["ts"] = int(time.time())
        merged["network_ts"] = max((int(item.get("network_ts") or 0) for item in updates), default=merged.get("network_ts"))
        merged["network_poll_seconds"] = self.settings.server_network_poll_seconds
        merged["network_errors"] = errors or []
        servers = merged.get("servers") or []
        summary = dict(merged.get("summary") or {})
        summary["servers"] = len(servers)
        summary["online"] = sum(bool(item.get("online")) for item in servers)
        summary["network_rx_bps"] = sum(float(item.get("network_rx_bps") or 0) for item in servers)
        summary["network_tx_bps"] = sum(float(item.get("network_tx_bps") or 0) for item in servers)
        summary["network_bps"] = summary["network_rx_bps"] + summary["network_tx_bps"]
        merged["summary"] = summary
        return merged

    def _apply_latest_network(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._network_latest:
            return payload
        return self._merge_network_updates(payload, list(self._network_latest.values()), [])

    def _persist(self, payload: dict[str, Any]) -> None:
        self.data_store.save_server_load(payload, self.settings.server_load_history_seconds)
        self.data_store.prune_server_load(int(time.time()) - self.settings.monitor_retention_days * 86400)

    async def _collect(self) -> dict[str, Any]:
        servers = list(self.server_store.enabled())
        payload = self._empty_snapshot()
        payload["ts"] = int(time.time())
        if not servers:
            payload["errors"] = ["Нет включённых Flussonic-серверов"]
            return payload
        results = await asyncio.gather(*(self._fetch_server(server) for server in servers), return_exceptions=True)
        session_counts = {
            item.get("server_id"): int(item.get("sessions") or 0)
            for item in (self.sessions_monitor.snapshot().get("server_counts") or [])
        }
        items: list[dict[str, Any]] = []
        errors: list[str] = []
        for server, result in zip(servers, results):
            if isinstance(result, Exception):
                message = str(result)
                errors.append(f"{server.name}: {message}")
                items.append({
                    "server_id": server.id, "server": server.name, "url": server.url, "online": False,
                    "state": "offline", "source": "unavailable", "error": message, "sessions": session_counts.get(server.id, 0),
                })
                continue
            result["sessions"] = session_counts.get(server.id, 0)
            items.append(result)
            await self._check_alerts(result)
        cpus = [float(item["cpu_percent"]) for item in items if item.get("online") and item.get("cpu_percent") is not None]
        memories = [float(item["memory_percent"]) for item in items if item.get("online") and item.get("memory_percent") is not None]
        warnings = sum(item.get("state") in {"warning", "critical"} for item in items)
        payload["servers"] = items
        payload["errors"] = errors
        payload["summary"] = {
            "servers": len(items),
            "online": sum(bool(item.get("online")) for item in items),
            "warnings": warnings,
            "avg_cpu_percent": round(sum(cpus) / len(cpus), 1) if cpus else None,
            "avg_memory_percent": round(sum(memories) / len(memories), 1) if memories else None,
            "network_rx_bps": sum(float(item.get("network_rx_bps") or 0) for item in items),
            "network_tx_bps": sum(float(item.get("network_tx_bps") or 0) for item in items),
            "network_bps": sum(float(item.get("network_rx_bps") or 0) + float(item.get("network_tx_bps") or 0) for item in items),
        }
        return self._apply_latest_network(payload)

    async def _fetch_server(self, server: FlussonicServer) -> dict[str, Any]:
        now = time.time()
        started = time.monotonic()
        errors: list[str] = []
        samples: list[Sample] = []
        metrics_path: str | None = None
        source = "node_exporter"

        exporter_url = effective_node_exporter_url(server)
        if exporter_url:
            try:
                text = await self.client.request_external_text(exporter_url, verify_tls=server.verify_tls)
                samples = parse_prometheus(text)
                if not any(sample.name.startswith("node_") for sample in samples):
                    raise FlussonicError("Node Exporter response contains no node_* metrics")
                metrics_path = exporter_url
            except Exception as exc:
                errors.append(f"Node Exporter {exporter_url}: {exc}")
                samples = []

        if not samples:
            source = "runtime_metrics"
            try:
                runtime_path, text = await self.client.runtime_metrics(server)
                samples = parse_prometheus(text)
                if not samples:
                    raise FlussonicError("metrics response contains no numeric samples")
                metrics_path = runtime_path
            except Exception as exc:
                errors.append(str(exc))
                samples = []

        if samples:
            item = self._from_metrics(server, samples, now, metrics_path or source, source=source)
        else:
            fallback = await self.client.aggregate_stream_stats(server)
            item = self._from_fallback(server, fallback, "; ".join(errors) or None)
        item["node_exporter_url"] = exporter_url or None
        item["latency_ms"] = round((time.monotonic() - started) * 1000)
        item["ts"] = int(now)
        return item

    async def test_node_exporter(self, server: FlussonicServer) -> dict[str, Any]:
        url = effective_node_exporter_url(server)
        if not url:
            return {"ok": False, "url": None, "error": "Не удалось определить URL Node Exporter"}
        started = time.monotonic()
        try:
            text = await self.client.request_external_text(url, verify_tls=server.verify_tls)
            samples = parse_prometheus(text)
            node_samples = sum(sample.name.startswith("node_") for sample in samples)
            if not node_samples:
                raise FlussonicError("В ответе нет метрик node_*")
            return {"ok": True, "url": url, "samples": node_samples, "latency_ms": round((time.monotonic() - started) * 1000)}
        except Exception as exc:
            return {"ok": False, "url": url, "error": str(exc), "latency_ms": round((time.monotonic() - started) * 1000)}

    def _from_metrics(self, server: FlussonicServer, samples: list[Sample], now: float, path: str, *, source: str = "runtime_metrics") -> dict[str, Any]:
        previous = self._previous.get(server.id) or {}
        cpu_count = _first_value(samples, exact=("machine_cpu_cores", "system_cpu_count", "node_cpu_count", "process_cpu_count"))
        node_cpu = [sample for sample in samples if _lname(sample) == "node_cpu_seconds_total"]
        if cpu_count is None and node_cpu:
            cpu_count = float(len({sample.labels.get("cpu") for sample in node_cpu if sample.labels.get("cpu") is not None}) or 1)

        direct_cpu_sample = _metric_by_tokens(samples, ("cpu", "percent"), ("temperature",)) or _metric_by_tokens(samples, ("cpu", "utilization"), ("seconds", "total"))
        cpu_percent = _percent(direct_cpu_sample.value, _lname(direct_cpu_sample)) if direct_cpu_sample else None
        cpu_source = "host"

        current: dict[str, float] = {"ts": now}
        if node_cpu:
            total = sum(sample.value for sample in node_cpu)
            idle = sum(sample.value for sample in node_cpu if sample.labels.get("mode") in {"idle", "iowait"})
            current.update({"node_cpu_total": total, "node_cpu_idle": idle})
            if previous.get("node_cpu_total") is not None:
                delta_total = total - previous["node_cpu_total"]
                delta_idle = idle - previous.get("node_cpu_idle", idle)
                if delta_total > 0:
                    cpu_percent = max(0.0, min(100.0, 100.0 * (1.0 - delta_idle / delta_total)))
        process_cpu = _first_value(samples, exact=("process_cpu_seconds_total", "erlang_vm_cpu_seconds_total", "beam_cpu_seconds_total"))
        process_cpu_percent = None
        if process_cpu is not None:
            current["process_cpu_seconds"] = process_cpu
            elapsed = now - previous.get("ts", now)
            delta = process_cpu - previous.get("process_cpu_seconds", process_cpu)
            if elapsed > 0 and delta >= 0:
                process_cpu_percent = max(0.0, 100.0 * delta / elapsed / max(cpu_count or 1.0, 1.0))
                if cpu_percent is None:
                    cpu_percent = min(100.0, process_cpu_percent)
                    cpu_source = "process"

        total_memory = _first_value(samples, exact=("node_memory_MemTotal_bytes", "system_memory_total_bytes", "memory_total_bytes"), contains=("memory_total_bytes", "mem_total_bytes"))
        available_memory = _first_value(samples, exact=("node_memory_MemAvailable_bytes", "node_memory_MemFree_bytes", "system_memory_available_bytes", "memory_available_bytes"), contains=("memory_available_bytes", "memory_free_bytes", "mem_available_bytes"))
        used_memory = _first_value(samples, exact=("system_memory_used_bytes", "memory_used_bytes"), contains=("memory_used_bytes",))
        if total_memory and available_memory is not None:
            used_memory = max(0.0, total_memory - available_memory)
        memory_percent = 100.0 * used_memory / total_memory if total_memory and used_memory is not None else None
        process_memory = (
            _first_value(samples, exact=("erlang_vm_memory_total_bytes", "beam_memory_total_bytes"), contains=("erlang_vm_memory_total_bytes", "beam_memory_total_bytes"))
            if source == "node_exporter"
            else _first_value(samples, exact=("process_resident_memory_bytes", "erlang_vm_memory_total_bytes", "beam_memory_total_bytes"), contains=("resident_memory_bytes", "vm_memory_total_bytes"))
        )

        network = _network_counters(samples)
        aggregate_network = [item for item in network.values() if not bool(item.get("virtual")) and bool(item.get("up", 1))]
        if not aggregate_network:
            aggregate_network = list(network.values())
        rx_total = sum(item["rx_total_bytes"] for item in aggregate_network) if aggregate_network else None
        tx_total = sum(item["tx_total_bytes"] for item in aggregate_network) if aggregate_network else None
        network_rx_bps = network_tx_bps = None
        elapsed = now - previous.get("ts", now)
        interfaces: list[dict[str, Any]] = []
        for device, counters in network.items():
            rx_key = f"network_rx_total:{device}"
            tx_key = f"network_tx_total:{device}"
            rx_rate = tx_rate = None
            if elapsed > 0 and previous.get(rx_key) is not None:
                rx_rate = max(0.0, (counters["rx_total_bytes"] - previous[rx_key]) * 8.0 / elapsed)
            if elapsed > 0 and previous.get(tx_key) is not None:
                tx_rate = max(0.0, (counters["tx_total_bytes"] - previous[tx_key]) * 8.0 / elapsed)
            current[rx_key] = counters["rx_total_bytes"]
            current[tx_key] = counters["tx_total_bytes"]
            interfaces.append({
                "device": device,
                "up": bool(counters.get("up", 1)),
                "virtual": bool(counters.get("virtual")),
                "rx_bps": rx_rate,
                "tx_bps": tx_rate,
                "rx_total_bytes": counters["rx_total_bytes"],
                "tx_total_bytes": counters["tx_total_bytes"],
            })
        interfaces.sort(key=lambda item: float(item.get("rx_bps") or 0) + float(item.get("tx_bps") or 0), reverse=True)
        if rx_total is not None:
            current["network_rx_total"] = rx_total
            if elapsed > 0 and previous.get("network_rx_total") is not None:
                network_rx_bps = max(0.0, (rx_total - previous["network_rx_total"]) * 8.0 / elapsed)
        if tx_total is not None:
            current["network_tx_total"] = tx_total
            if elapsed > 0 and previous.get("network_tx_total") is not None:
                network_tx_bps = max(0.0, (tx_total - previous["network_tx_total"]) * 8.0 / elapsed)

        disk_total, disk_used, disk_percent = _filesystem_usage(samples)
        load1 = _first_value(samples, exact=("node_load1", "system_load1", "load1"), contains=("load_average_1",))
        load5 = _first_value(samples, exact=("node_load5", "system_load5", "load5"), contains=("load_average_5",))
        load15 = _first_value(samples, exact=("node_load15", "system_load15", "load15"), contains=("load_average_15",))
        boot_time = _first_value(samples, exact=("node_boot_time_seconds", "system_boot_time_seconds"))
        process_start = _first_value(samples, exact=("process_start_time_seconds", "flussonic_start_time_seconds"))
        start_time = boot_time or process_start
        uptime_seconds = max(0, int(now - start_time)) if start_time and start_time <= now else None
        temperature = _first_value(samples, exact=("node_hwmon_temp_celsius",), contains=("temperature_celsius", "cpu_temperature"))

        self._previous[server.id] = current
        state = self._state(cpu_percent, memory_percent, disk_percent)
        return {
            "server_id": server.id, "server": server.name, "url": server.url, "online": True,
            "state": state, "source": source, "metrics_path": path, "limited": False,
            "cpu_percent": round(cpu_percent, 1) if cpu_percent is not None else None,
            "cpu_source": cpu_source, "cpu_count": round(cpu_count, 1) if cpu_count else None,
            "process_cpu_percent": round(process_cpu_percent, 1) if process_cpu_percent is not None else None,
            "memory_total_bytes": total_memory, "memory_used_bytes": used_memory,
            "memory_percent": round(memory_percent, 1) if memory_percent is not None else None,
            "process_memory_bytes": process_memory,
            "disk_total_bytes": disk_total, "disk_used_bytes": disk_used,
            "disk_percent": round(disk_percent, 1) if disk_percent is not None else None,
            "network_rx_bps": network_rx_bps, "network_tx_bps": network_tx_bps,
            "network_rx_total_bytes": rx_total, "network_tx_total_bytes": tx_total,
            "network_interfaces": interfaces,
            "load1": load1, "load5": load5, "load15": load15,
            "uptime_seconds": uptime_seconds, "temperature_celsius": temperature,
            "error": None,
        }

    def _from_fallback(self, server: FlussonicServer, stats: dict[str, Any], metrics_error: str | None) -> dict[str, Any]:
        state = "warning" if int(stats.get("transcoder_overloaded") or 0) else "limited"
        return {
            "server_id": server.id, "server": server.name, "url": server.url, "online": True,
            "state": state, "source": "stream_stats", "metrics_path": None, "limited": True,
            "cpu_percent": None, "memory_percent": None, "disk_percent": None,
            "network_rx_bps": float(stats.get("input_bandwidth_bps") or 0),
            "network_tx_bps": float(stats.get("output_bandwidth_bps") or 0),
            "network_rx_total_bytes": None, "network_tx_total_bytes": None, "network_interfaces": [],
            "process_memory_bytes": float(stats.get("ram_bytes") or 0),
            "cpu_units": float(stats.get("cpu_units") or 0),
            "streams": int(stats.get("streams") or 0),
            "running_streams": int(stats.get("running_streams") or 0),
            "alive_streams": int(stats.get("alive_streams") or 0),
            "transcoder_overloaded": int(stats.get("transcoder_overloaded") or 0),
            "error": metrics_error,
        }

    def _state(self, cpu: float | None, memory: float | None, disk: float | None) -> str:
        values = [
            (cpu, self.settings.server_load_cpu_warning),
            (memory, self.settings.server_load_memory_warning),
            (disk, self.settings.server_load_disk_warning),
        ]
        ratios = [value / threshold for value, threshold in values if value is not None and threshold > 0]
        if any(ratio >= 1.12 for ratio in ratios):
            return "critical"
        if any(ratio >= 1.0 for ratio in ratios):
            return "warning"
        return "ok"

    async def _check_alerts(self, item: dict[str, Any]) -> None:
        checks = (
            ("cpu", item.get("cpu_percent"), self.settings.server_load_cpu_warning, "CPU"),
            ("memory", item.get("memory_percent"), self.settings.server_load_memory_warning, "RAM"),
            ("disk", item.get("disk_percent"), self.settings.server_load_disk_warning, "Диск"),
        )
        for key, raw_value, threshold, label in checks:
            if raw_value is None:
                continue
            value = float(raw_value)
            state_key = (str(item["server_id"]), key)
            active = self._alert_state.get(state_key, False)
            if value >= threshold and not active:
                self._alert_state[state_key] = True
                await self.notifications.send(
                    "server_load",
                    f"⚠️ {item['server']}: {label} {value:.1f}% (порог {threshold:.0f}%)",
                )
            elif value < max(0.0, threshold - 7.0) and active:
                self._alert_state[state_key] = False
