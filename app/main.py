from __future__ import annotations

import asyncio
import hmac
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .auth import COOKIE_NAME, Session, create_session_token, require_session
from .config import FlussonicServer, get_settings
from .datastore import DataStore
from .flussonic import FlussonicClient, FlussonicError
from .monitor import SessionsMonitor
from .load_monitor import ServerLoadMonitor
from .notifications import NotificationService
from .source_monitor import SourceMonitor
from .server_store import ServerStore, ServerStoreError
from .cluster_store import ClusterStore, ClusterStoreError
from .m3u_import import parse_m3u
from .placement import build_batch_plan
from .models import (
    InputsUpdate, LoginRequest, ReorderRequest, StreamCreate, StreamPatch, SyncRequest,
    ServerCreate, ServerUpdate, ServerTestRequest, BulkOperationRequest,
    NotificationSettingsUpdate, NotificationTestRequest, SourceActionRequest,
    ClusterSettingsUpdate, PlacementSettingsUpdate, PlacementAssignmentUpdate,
    PlacementBulkUpdate, PlacementApplyRequest, PlacementPlanRequest, PlacementTargetPickRequest, PlacementMigrationRequest, M3UImportRequest, StreamModeBulkRequest,
    StreamStateBulkRequest, ChangePreviewRequest,
)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
settings = get_settings()
client = FlussonicClient(settings)
server_store = ServerStore(Path(settings.server_store_path), settings.secret_key, settings.servers)
cluster_store = ClusterStore(Path(settings.cluster_store_path), settings.secret_key)
data_store = DataStore(Path(settings.database_path))
notifications = NotificationService(data_store, settings.secret_key)
monitor = SessionsMonitor(settings, server_store, data_store)
source_monitor = SourceMonitor(settings, server_store, client, data_store, notifications)
load_monitor = ServerLoadMonitor(settings, server_store, client, data_store, notifications, monitor)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await monitor.start(); await source_monitor.start(); await load_monitor.start()
    try: yield
    finally:
        await load_monitor.stop(); await source_monitor.stop(); await monitor.stop(); await client.aclose()


app = FastAPI(title=settings.panel_title, version="5.13.1", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def placement_enabled() -> bool:
    return bool(data_store.get_setting("placement_enabled", False))


def placement_info(name: str) -> dict[str, Any]:
    saved = data_store.placement(name) or {"stream_name": name, "mode": "mirror", "primary_server_id": None}
    configured_mode = saved.get("mode") or "mirror"
    effective_mode = configured_mode if placement_enabled() else "mirror"
    return {**saved, "configured_mode": configured_mode, "effective_mode": effective_mode, "enabled": placement_enabled()}


def placement_targets(name: str, servers: list[FlussonicServer] | None = None) -> list[FlussonicServer]:
    enabled = list(servers or server_store.enabled())
    info = placement_info(name)
    if info["effective_mode"] == "assigned":
        target = next((server for server in enabled if server.id == info.get("primary_server_id")), None)
        return [target] if target else []
    return enabled


def placement_public(name: str) -> dict[str, Any]:
    info = placement_info(name)
    server = server_store.get(info.get("primary_server_id")) if info.get("primary_server_id") else None
    return {
        "enabled": info["enabled"],
        "configured_mode": info["configured_mode"],
        "effective_mode": info["effective_mode"],
        "primary_server_id": info.get("primary_server_id"),
        "primary_server_name": server.name if server else None,
    }


def get_server(server_id: str | None) -> FlussonicServer:
    server = server_store.get(server_id) if server_id else server_store.primary()
    if server is None: raise HTTPException(status_code=404 if server_id else 409, detail="Сервер не найден" if server_id else "Сначала добавьте и включите Flussonic-сервер")
    if not server.enabled: raise HTTPException(status_code=409, detail="Сервер отключён")
    return server


def public_server(server: FlussonicServer) -> dict[str, Any]:
    return {"id": server.id, "name": server.name, "url": server.url, "username": server.username, "primary": server.primary, "enabled": server.enabled, "verify_tls": server.verify_tls, "node_exporter_url": server.node_exporter_url, "password_set": bool(server.password)}


def operation_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    succeeded = [r for r in results if r["ok"]]; failed = [r for r in results if not r["ok"]]
    return {"ok": not failed, "partial": bool(succeeded and failed), "success_count": len(succeeded), "failure_count": len(failed), "max_elapsed_ms": max((r.get("elapsed_ms") or 0 for r in results), default=0), "results": results}


def _change_value(value: Any) -> Any:
    if isinstance(value, dict):
        if set(value) == {"url"}:
            return value.get("url")
        return {k: _change_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_change_value(v) for v in value]
    return value


def _config_diff(current: dict[str, Any] | None, desired: dict[str, Any] | None) -> list[dict[str, Any]]:
    if current is None and desired is None:
        return []
    if desired is None:
        return [{"field": "stream", "before": "существует", "after": "будет удалён"}]
    if current is None:
        return [{"field": "stream", "before": "отсутствует", "after": "будет создан"}]
    left = client.canonical_config(current)
    right = client.canonical_config(desired)
    keys = sorted(set(left) | set(right))
    changes = []
    for key in keys:
        before = left.get(key)
        after = right.get(key)
        if before != after:
            changes.append({"field": key, "before": _change_value(before), "after": _change_value(after)})
    return changes


def _audit_matches_stream(item: dict[str, Any], name: str) -> bool:
    if item.get("entity_id") == name:
        return True
    details = item.get("details") or {}
    for result in details.get("results") or []:
        if isinstance(result, dict) and result.get("stream_name") == name:
            return True
    for result in details.get("items") or []:
        if isinstance(result, dict) and result.get("stream_name") == name:
            return True
    return False


async def backup_stream(server: FlussonicServer, name: str, actor: str, action: str) -> int | None:
    try:
        raw = await client.get_stream(server, name)
    except FlussonicError:
        return None
    config = dict(client.disk_config(raw))
    return data_store.backup(actor=actor, action=action, stream_name=name, server_id=server.id, server_name=server.name, config=config, config_hash=client.config_hash(config))


async def mutate_with_backup(targets: list[FlussonicServer], name: str, actor: str, action: str, operation) -> list[dict[str, Any]]:
    async def one(server: FlussonicServer):
        await backup_stream(server, name, actor, action)
        return await operation(server)
    results = await client.run_many(targets, one)
    summary = operation_summary(results)
    data_store.audit(actor=actor, action=action, entity_type="stream", entity_id=name, server_id=None, status="success" if summary["ok"] else "partial" if summary["partial"] else "failed", summary=f"{action}: {name}", details=summary)
    return results


@app.get("/", include_in_schema=False)
async def index() -> FileResponse: return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-store"})

@app.get("/api/health")
async def health() -> dict[str, Any]: return {"ok": True, "version": "5.13.1", "configured_servers": len(server_store.all()), "enabled_servers": len(server_store.enabled()), "server_load_monitor": settings.server_load_enabled, "node_exporter": True, "network_realtime": True, "cluster": True, "placement": True, "source_checks": "manual"}

@app.post("/api/auth/login")
async def login(payload: LoginRequest, response: Response) -> dict[str, Any]:
    if not (hmac.compare_digest(payload.username, settings.admin_user) and hmac.compare_digest(payload.password, settings.admin_password)):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Неверный логин или пароль")
    response.set_cookie(COOKIE_NAME, create_session_token(payload.username), httponly=True, samesite="lax", secure=settings.cookie_secure, max_age=43200, path="/")
    data_store.audit(actor=payload.username, action="login", entity_type="auth", entity_id=payload.username, server_id=None, status="success", summary="Вход в панель")
    return {"ok": True, "username": payload.username}

@app.post("/api/auth/logout")
async def logout(response: Response, session: Session = Depends(require_session)) -> dict[str, bool]:
    response.delete_cookie(COOKIE_NAME, path="/"); data_store.audit(actor=session.username, action="logout", entity_type="auth", entity_id=session.username, server_id=None, status="success", summary="Выход из панели"); return {"ok": True}

@app.get("/api/auth/me")
async def me(session: Session = Depends(require_session)) -> dict[str, Any]: return {"username": session.username, "title": settings.panel_title}

@app.get("/api/monitor/snapshot")
async def monitor_snapshot(refresh: bool = Query(False), _: Session = Depends(require_session)) -> dict[str, Any]: return await monitor.refresh_now() if refresh else monitor.snapshot()

@app.get("/api/monitor/history")
async def monitor_history(range: str = Query("24h"), _: Session = Depends(require_session)) -> dict[str, Any]:
    seconds = {"1h": 3600, "24h": 86400, "7d": 604800, "30d": 2592000}.get(range, 86400)
    return {"range": range, "items": data_store.monitor_history(int(time.time()) - seconds)}

@app.get("/api/monitor/stream")
async def monitor_stream(_: Session = Depends(require_session)) -> StreamingResponse:
    queue = monitor.subscribe()
    async def events():
        try:
            yield f"data: {json.dumps(monitor.snapshot(), ensure_ascii=False, separators=(',', ':'))}\n\n"
            while True:
                try: yield f"data: {json.dumps(await asyncio.wait_for(queue.get(), 20), ensure_ascii=False, separators=(',', ':'))}\n\n"
                except asyncio.TimeoutError: yield ": keep-alive\n\n"
        finally: monitor.unsubscribe(queue)
    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no","Connection":"keep-alive"})

@app.get("/api/load/snapshot")
async def load_snapshot(refresh: bool = Query(False), _: Session = Depends(require_session)) -> dict[str, Any]:
    return await load_monitor.refresh_now() if refresh else load_monitor.snapshot()

@app.get("/api/load/history")
async def load_history(range: str = Query("24h"), server_id: str | None = Query(None), _: Session = Depends(require_session)) -> dict[str, Any]:
    seconds = {"1h": 3600, "24h": 86400, "7d": 604800, "30d": 2592000}.get(range, 86400)
    return {"range": range, "server_id": server_id, "items": data_store.server_load_history(int(time.time()) - seconds, server_id)}

@app.get("/api/load/stream")
async def load_stream(_: Session = Depends(require_session)) -> StreamingResponse:
    queue = load_monitor.subscribe()
    async def events():
        try:
            yield f"data: {json.dumps(load_monitor.snapshot(), ensure_ascii=False, separators=(',', ':'))}\n\n"
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), 20)
                    yield f"data: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            load_monitor.unsubscribe(queue)
    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no","Connection":"keep-alive"})

@app.get("/api/servers")
async def servers(_: Session = Depends(require_session)) -> dict[str, Any]:
    async def status_for(server):
        base = public_server(server)
        return {**base, **(await client.server_status(server) if server.enabled else {"online":False,"latency_ms":None,"error":"Сервер отключён","stream_count_hint":None})}
    items = await asyncio.gather(*(status_for(s) for s in server_store.all()))
    primary = server_store.primary(); return {"primary_id": primary.id if primary else None, "items": items}

@app.post("/api/servers")
async def create_server(payload: ServerCreate, session: Session = Depends(require_session)) -> dict[str, Any]:
    try: server = server_store.create(payload.model_dump(exclude_none=True))
    except (ServerStoreError, ValueError) as exc: raise HTTPException(400, str(exc)) from exc
    data_store.audit(actor=session.username, action="server_create", entity_type="server", entity_id=server.id, server_id=server.id, status="success", summary=f"Добавлен сервер {server.name}")
    return {"ok": True, "server": public_server(server)}

@app.put("/api/servers/{server_id}")
async def update_server(server_id: str, payload: ServerUpdate, session: Session = Depends(require_session)) -> dict[str, Any]:
    try: server = server_store.update(server_id, payload.model_dump(exclude_none=True))
    except (ServerStoreError, ValueError) as exc: raise HTTPException(400, str(exc)) from exc
    data_store.audit(actor=session.username, action="server_update", entity_type="server", entity_id=server.id, server_id=server.id, status="success", summary=f"Изменён сервер {server.name}")
    return {"ok": True, "server": public_server(server)}

@app.delete("/api/servers/{server_id}")
async def remove_server(server_id: str, session: Session = Depends(require_session)) -> dict[str, Any]:
    try: server_store.delete(server_id)
    except ServerStoreError as exc: raise HTTPException(404, str(exc)) from exc
    data_store.audit(actor=session.username, action="server_delete", entity_type="server", entity_id=server_id, server_id=server_id, status="success", summary=f"Удалён сервер {server_id}")
    return {"ok": True}

@app.post("/api/servers/{server_id}/test")
async def test_saved_server(server_id: str, _: Session = Depends(require_session)) -> dict[str, Any]:
    server = server_store.get(server_id)
    if not server: raise HTTPException(404, "Сервер не найден")
    flussonic_result, exporter_result = await asyncio.gather(
        client.server_status(server), load_monitor.test_node_exporter(server)
    )
    return {"ok": flussonic_result["online"], **flussonic_result, "node_exporter": exporter_result}

@app.post("/api/servers/{server_id}/primary")
async def make_primary(server_id: str, session: Session = Depends(require_session)) -> dict[str, Any]:
    try: server = server_store.set_primary(server_id)
    except ServerStoreError as exc: raise HTTPException(400, str(exc)) from exc
    data_store.audit(actor=session.username, action="server_primary", entity_type="server", entity_id=server.id, server_id=server.id, status="success", summary=f"{server.name} назначен основным")
    return {"ok": True, "server": public_server(server)}

@app.post("/api/servers/test")
async def test_server(payload: ServerTestRequest, _: Session = Depends(require_session)) -> dict[str, Any]:
    temp = FlussonicServer(
        id="connection-test", name="Проверка", url=payload.url, username=payload.username,
        password=payload.password, verify_tls=payload.verify_tls, node_exporter_url=payload.node_exporter_url,
    )
    flussonic_result, exporter_result = await asyncio.gather(
        client.server_status(temp), load_monitor.test_node_exporter(temp)
    )
    return {"ok": flussonic_result["online"], **flussonic_result, "node_exporter": exporter_result}


def _parse_bitrate_limit(value: str | None) -> float | None:
    if not value:
        return None
    raw = value.strip().upper()
    multiplier = 1.0
    if raw.endswith("K"):
        multiplier, raw = 1_000.0, raw[:-1]
    elif raw.endswith("M"):
        multiplier, raw = 1_000_000.0, raw[:-1]
    elif raw.endswith("G"):
        multiplier, raw = 1_000_000_000.0, raw[:-1]
    try:
        return float(raw) * multiplier
    except ValueError:
        return None


def _cluster_config_text(config: dict[str, Any]) -> str:
    key = str(config.get("cluster_key") or "CHANGE_ME")
    peers = config.get("peers") or []
    lines = [f"cluster_key {key};", "", "# Remote sources:"]
    for peer in peers:
        lines.extend([f"peer {peer['host']} {{", "}"])
    lines.extend(["", "# Balancer:", f"balancer {config.get('balancer_name') or 'lb01'} {{", f"  mode {config.get('mode') or 'clients'};"])
    for peer in peers:
        suffix = f" max_bitrate={peer.get('max_bitrate')}" if peer.get("max_bitrate") else ""
        lines.append(f"  server {peer['host']}{suffix};")
    lines.append("}")
    return "\n".join(lines) + "\n"


@app.get("/api/cluster/settings")
async def cluster_settings(_: Session = Depends(require_session)) -> dict[str, Any]:
    return {"settings": cluster_store.get(), "servers": [public_server(server) for server in server_store.all()]}


@app.put("/api/cluster/settings")
async def save_cluster_settings(payload: ClusterSettingsUpdate, session: Session = Depends(require_session)) -> dict[str, Any]:
    known = {server.id for server in server_store.all()}
    if payload.balancer_server_id and payload.balancer_server_id not in known:
        raise HTTPException(400, "Balancer-сервер не найден")
    unknown = [peer.server_id for peer in payload.peers if peer.server_id not in known]
    if unknown:
        raise HTTPException(400, f"Неизвестные peer server_id: {', '.join(unknown)}")
    current = cluster_store.get()
    if payload.enabled and not (payload.cluster_key or current.get("has_cluster_key")):
        raise HTTPException(400, "Укажите cluster_key")
    try:
        saved = cluster_store.update(payload.model_dump(exclude_none=True))
    except ClusterStoreError as exc:
        raise HTTPException(400, str(exc)) from exc
    data_store.audit(actor=session.username, action="cluster_settings", entity_type="cluster", entity_id=payload.balancer_name, server_id=payload.balancer_server_id, status="success", summary=f"Сохранены настройки Cluster: {len(payload.peers)} peer")
    return {"ok": True, "settings": saved}


@app.get("/api/cluster/config")
async def cluster_config(_: Session = Depends(require_session)) -> dict[str, Any]:
    config = cluster_store.get(include_secret=True)
    return {"config": _cluster_config_text(config), "has_cluster_key": bool(config.get("cluster_key"))}


@app.get("/api/cluster/overview")
async def cluster_overview(refresh: bool = Query(False), _: Session = Depends(require_session)) -> dict[str, Any]:
    config = cluster_store.get()
    load = await load_monitor.refresh_now() if refresh else load_monitor.snapshot()
    load_by_id = {str(item.get("server_id")): item for item in (load.get("servers") or [])}
    session_by_id = {str(item.get("server_id")): item for item in (monitor.snapshot().get("server_counts") or [])}
    rows = []
    total_clients = total_streams = 0
    total_output = 0.0
    for peer in config.get("peers") or []:
        server = server_store.get(str(peer.get("server_id") or ""))
        metrics = load_by_id.get(str(peer.get("server_id") or ""), {})
        sessions = session_by_id.get(str(peer.get("server_id") or ""), {})
        clients = int(sessions.get("sessions") or metrics.get("sessions") or 0)
        streams = int(metrics.get("streams") or metrics.get("alive_streams") or 0)
        output_bps = float(metrics.get("output_bandwidth_bps") or 0)
        mode = config.get("mode") or "clients"
        if mode == "bitrate":
            load_value = output_bps / 1000.0
            load_unit = "kbps"
        elif mode == "usage":
            limit = _parse_bitrate_limit(peer.get("max_bitrate"))
            load_value = 100.0 * output_bps / limit if limit else None
            load_unit = "%"
        elif mode == "streams":
            load_value = float(streams)
            load_unit = "streams"
        else:
            load_value = float(clients)
            load_unit = "clients"
        rows.append({
            "server_id": peer.get("server_id"), "host": peer.get("host"),
            "server_name": server.name if server else peer.get("server_id"),
            "online": bool(metrics.get("online")) and bool(sessions.get("online", True)),
            "cpu_percent": metrics.get("cpu_percent"), "memory_percent": metrics.get("memory_percent"),
            "clients": clients, "streams": streams, "output_bitrate_kbps": round(output_bps / 1000.0, 1),
            "load": round(load_value, 1) if load_value is not None else None, "load_unit": load_unit,
            "load1": metrics.get("load1"), "uptime_seconds": metrics.get("uptime_seconds"),
            "state": metrics.get("state") or ("offline" if not metrics.get("online") else "limited"),
            "source": metrics.get("source"), "error": metrics.get("error"),
        })
        total_clients += clients; total_streams += streams; total_output += output_bps
    balancer_id = config.get("balancer_server_id")
    balancer_metrics = load_by_id.get(str(balancer_id or ""), {})
    loads = [float(row["load"]) for row in rows if row.get("load") is not None]
    return {
        "ts": int(time.time()), "poll_seconds": settings.cluster_poll_seconds,
        "settings": config,
        "balancer": {
            "server_id": balancer_id,
            "server_name": server_store.get(str(balancer_id)).name if balancer_id and server_store.get(str(balancer_id)) else None,
            "online": bool(balancer_metrics.get("online")), "state": balancer_metrics.get("state"),
        },
        "summary": {"nodes": len(rows), "online": sum(bool(row["online"]) for row in rows), "clients": total_clients, "streams": total_streams, "output_bitrate_kbps": round(total_output / 1000.0, 1), "average_load": round(sum(loads) / len(loads), 1) if loads else None},
        "nodes": rows,
    }



@app.get("/api/channel-card/{name:path}")
async def channel_card(name: str, server_id: str | None = None, _: Session = Depends(require_session)) -> dict[str, Any]:
    servers = list(server_store.enabled())
    placement = placement_public(name)
    preferred_id = placement.get("primary_server_id") if placement.get("effective_mode") == "assigned" else (server_id or (server_store.primary().id if server_store.primary() else None))

    async def inspect(server: FlussonicServer) -> dict[str, Any]:
        try:
            raw = await client.get_stream(server, name)
            stream = client.normalize_stream(raw)
            stats = raw.get("stats") if isinstance(raw.get("stats"), dict) else {}
            return {
                "server_id": server.id, "server_name": server.name, "online": True, "present": True,
                "stream": stream, "config": client.disk_config(raw),
                "clients": int(stats.get("playback_opened_sessions", stats.get("playback_total_sessions", 0)) or 0),
                "input_bitrate": int(stats.get("inputs_bandwidth", 0) or 0),
                "output_bitrate": int(stats.get("output_bandwidth", 0) or 0),
                "status": stats.get("status") or stream.get("status") or "unknown",
            }
        except Exception as exc:
            return {"server_id": server.id, "server_name": server.name, "online": True, "present": False, "error": str(exc), "clients": 0, "input_bitrate": 0, "output_bitrate": 0}

    nodes = list(await asyncio.gather(*(inspect(server) for server in servers)))
    reference = next((item for item in nodes if item["server_id"] == preferred_id and item.get("present")), None) or next((item for item in nodes if item.get("present")), None)
    audit = [item for item in data_store.audit_items(500) if _audit_matches_stream(item, name)][:20]
    backups = data_store.backups(name, 10)
    source_checks = data_store.source_checks(stream_name=name)
    expected_ids = {server.id for server in placement_targets(name, servers)}
    for node in nodes:
        node["expected"] = node["server_id"] in expected_ids
        node["role"] = "primary" if node["server_id"] == placement.get("primary_server_id") and placement.get("effective_mode") == "assigned" else ("mirror" if node["server_id"] in expected_ids else "extra")
    return {
        "name": name, "placement": placement, "reference_server_id": reference.get("server_id") if reference else None,
        "stream": reference.get("stream") if reference else None, "config": reference.get("config") if reference else None,
        "summary": {
            "present": sum(bool(item.get("present")) for item in nodes),
            "expected": len(expected_ids),
            "clients": sum(int(item.get("clients") or 0) for item in nodes),
            "input_bitrate": sum(int(item.get("input_bitrate") or 0) for item in nodes),
            "output_bitrate": sum(int(item.get("output_bitrate") or 0) for item in nodes),
        },
        "servers": nodes, "source_checks": source_checks[:20], "backups": backups, "audit": audit,
    }


@app.post("/api/changes/dry-run")
async def changes_dry_run(payload: ChangePreviewRequest, _: Session = Depends(require_session)) -> dict[str, Any]:
    enabled = list(server_store.enabled())
    try:
        selected_targets = client.select_servers(enabled, payload.target_ids) if payload.target_ids is not None else enabled
    except FlussonicError as exc:
        raise HTTPException(400, str(exc)) from exc
    source = get_server(payload.source_id) if payload.operation == "sync" else None
    semaphore = asyncio.Semaphore(10)
    source_cache: dict[str, dict[str, Any]] = {}

    async def desired_for(name: str, server: FlussonicServer) -> dict[str, Any]:
        async with semaphore:
            try:
                raw = await client.get_stream(server, name)
                current = dict(client.disk_config(raw))
                desired: dict[str, Any] | None = dict(current)
                note = None
                if payload.operation == "delete":
                    desired = None
                elif payload.operation == "add_input":
                    inputs = list(current.get("inputs") or [])
                    value = str(payload.value or "").strip()
                    if value and not any(isinstance(item, dict) and item.get("url") == value for item in inputs):
                        inputs.append({"url": value})
                    desired["inputs"] = inputs
                elif payload.operation == "set_provider":
                    desired["provider"] = str(payload.value or "")
                elif payload.operation == "set_on_play":
                    desired["on_play"] = {"url": str(payload.value)} if payload.value else None
                elif payload.operation == "set_static":
                    desired["static"] = bool(payload.value)
                elif payload.operation == "set_disabled":
                    desired["disabled"] = bool(payload.value)
                elif payload.operation == "sync":
                    if source is None:
                        raise RuntimeError("Не выбран исходный сервер")
                    if name not in source_cache:
                        source_raw = await client.get_stream(source, name)
                        source_cfg = dict(client.disk_config(source_raw)); source_cfg.pop("name", None)
                        source_cache[name] = source_cfg
                    desired = dict(source_cache[name])
                    note = f"источник: {source.name}"
                changes = _config_diff(current, desired)
                return {
                    "stream_name": name, "server_id": server.id, "server_name": server.name, "ok": True,
                    "changed": bool(changes), "operation": payload.operation, "changes": changes, "note": note,
                }
            except Exception as exc:
                return {"stream_name": name, "server_id": server.id, "server_name": server.name, "ok": False, "changed": False, "operation": payload.operation, "changes": [], "error": str(exc)}

    jobs = []
    for name in payload.names:
        targets = placement_targets(name, enabled) if payload.target_ids is None and payload.operation in {"set_static", "set_disabled"} else selected_targets
        if payload.operation == "sync" and placement_enabled():
            targets = placement_targets(name, enabled)
        for server in targets:
            if source and server.id == source.id:
                continue
            jobs.append(desired_for(name, server))
    items = list(await asyncio.gather(*jobs)) if jobs else []
    changed = [item for item in items if item.get("ok") and item.get("changed")]
    unchanged = [item for item in items if item.get("ok") and not item.get("changed")]
    failed = [item for item in items if not item.get("ok")]
    return {
        "ok": not failed, "operation": payload.operation, "stream_count": len(payload.names),
        "target_count": len(items), "change_count": len(changed), "unchanged_count": len(unchanged), "failure_count": len(failed),
        "items": items,
    }


def _aggregate_stream_rows(rows: list[tuple[FlussonicServer, list[dict[str, Any]]]]) -> list[dict[str, Any]]:
    """Collapse duplicate channel names from multiple CDN into one row for the ВСЕ view."""
    grouped: dict[str, list[tuple[FlussonicServer, dict[str, Any]]]] = {}
    for server, items in rows:
        for item in items:
            name = str(item.get("name") or "")
            if not name:
                continue
            grouped.setdefault(name, []).append((server, item))

    primary = server_store.primary()
    result: list[dict[str, Any]] = []
    for name, nodes in grouped.items():
        placement = placement_public(name)
        preferred_id = placement.get("primary_server_id") if placement.get("effective_mode") == "assigned" else (primary.id if primary else None)
        reference_server, reference_item = next(((server, item) for server, item in nodes if server.id == preferred_id), nodes[0])
        item = dict(reference_item)
        server_ids = [server.id for server, _ in nodes]
        server_names = [server.name for server, _ in nodes]
        alive_nodes = [node for _, node in nodes if node.get("alive")]
        waiting_nodes = [node for _, node in nodes if node.get("status") == "waiting"]
        item.update({
            "alive": bool(alive_nodes),
            "running": any(bool(node.get("running")) for _, node in nodes),
            "status": (alive_nodes[0].get("status") if alive_nodes else ("waiting" if waiting_nodes else reference_item.get("status", "unknown"))),
            "placement": placement,
            "server_ids": server_ids,
            "server_names": server_names,
            "server_count": len(server_ids),
            "reference_server_id": reference_server.id,
            "reference_server_name": reference_server.name,
        })
        result.append(item)
    return sorted(result, key=lambda item: (item.get("position", 0), str(item.get("name", "")).lower()))


@app.get("/api/streams")
async def streams(server_id: str | None = None, search: str | None = None, _: Session = Depends(require_session)) -> dict[str, Any]:
    if server_id == "all":
        servers = server_store.enabled()
        if not servers:
            raise HTTPException(409, "Сначала добавьте и включите Flussonic-сервер")

        async def load_one(server: FlussonicServer) -> tuple[FlussonicServer, list[dict[str, Any]] | None, str | None]:
            try:
                return server, await client.list_streams(server), None
            except FlussonicError as exc:
                return server, None, str(exc)

        loaded = await asyncio.gather(*(load_one(server) for server in servers))
        rows = [(server, items) for server, items, error in loaded if items is not None]
        errors = [{"server_id": server.id, "server_name": server.name, "error": error} for server, items, error in loaded if error]
        if not rows:
            detail = "; ".join(f"{item['server_name']}: {item['error']}" for item in errors) or "Нет доступных CDN"
            raise HTTPException(502, detail)
        items = _aggregate_stream_rows(rows)
        if search:
            needle = search.casefold()
            items = [item for item in items if needle in str(item.get("name", "")).casefold() or needle in str(item.get("title", "")).casefold() or needle in str(item.get("provider", "")).casefold() or any(needle in str(inp.get("url", "")).casefold() for inp in item.get("inputs", []))]
        return {
            "server": {"id": "all", "name": "ВСЕ"},
            "stats": {
                "total": len(items),
                "alive": sum(bool(item.get("alive")) for item in items),
                "running": sum(bool(item.get("running")) for item in items),
                "waiting": sum(item.get("status") == "waiting" for item in items),
            },
            "items": items,
            "errors": errors,
            "placement_enabled": placement_enabled(),
        }

    server = get_server(server_id)
    try:
        items = await client.list_streams(server)
    except FlussonicError as exc:
        raise HTTPException(502, str(exc)) from exc
    if search:
        n = search.casefold()
        items = [x for x in items if n in x["name"].casefold() or n in x.get("title", "").casefold() or n in x.get("provider", "").casefold() or any(n in i["url"].casefold() for i in x.get("inputs", []))]
    items = [dict(item, placement=placement_public(item["name"]), server_ids=[server.id], server_names=[server.name], server_count=1, reference_server_id=server.id, reference_server_name=server.name) for item in items]
    return {"server": {"id": server.id, "name": server.name}, "stats": {"total": len(items), "alive": sum(x["alive"] for x in items), "running": sum(x["running"] for x in items), "waiting": sum(x["status"] == "waiting" for x in items)}, "items": items, "errors": [], "placement_enabled": placement_enabled()}

@app.get("/api/streams/{name:path}")
async def stream_detail(name: str, server_id: str | None = None, _: Session = Depends(require_session)) -> dict[str, Any]:
    server=get_server(server_id)
    try: raw=await client.get_stream(server,name)
    except FlussonicError as exc: raise HTTPException(502,str(exc)) from exc
    return {"server":{"id":server.id,"name":server.name},"stream":dict(client.normalize_stream(raw),placement=placement_public(name)),"config_on_disk":client.disk_config(raw),"placement":placement_public(name)}

@app.get("/api/diagnostics/{name:path}")
async def diagnostics(name: str, server_id: str | None = None, _: Session = Depends(require_session)) -> dict[str, Any]:
    server=get_server(server_id)
    try: raw=await client.get_stream(server,name)
    except FlussonicError as exc: raise HTTPException(502,str(exc)) from exc
    stats=raw.get("stats") if isinstance(raw.get("stats"),dict) else {}; normalized=client.normalize_stream(raw)
    media=raw.get("media_info") or raw.get("input_media_info") or stats.get("media_info") or {}
    runtime_inputs=raw.get("inputs") if isinstance(raw.get("inputs"),list) else []
    active=raw.get("active_input") or stats.get("active_input") or stats.get("input_id")
    if active is None and normalized["alive"] and runtime_inputs: active=runtime_inputs[0].get("url")
    return {"server":public_server(server),"stream":normalized,"status":{"status":stats.get("status"),"alive":stats.get("alive"),"running":stats.get("running"),"coder_error":stats.get("coder_error"),"inputs_bandwidth":stats.get("inputs_bandwidth"),"output_bandwidth":stats.get("output_bandwidth"),"ram_bytes":stats.get("ram_bytes"),"cpu_units":stats.get("cpu_units"),"playback_sessions":stats.get("playback_opened_sessions",stats.get("playback_total_sessions")),"active_input":active},"media_info":media,"runtime_inputs":runtime_inputs,"config":client.disk_config(raw),"raw_stats":stats}

@app.get("/api/preview/{name:path}")
async def preview(name: str, server_id: str | None = None, _: Session = Depends(require_session)) -> dict[str, Any]:
    server=get_server(server_id); encoded=quote(name,safe="/")
    return {"server":{"id":server.id,"name":server.name},"embed_url":f"{server.url}/{encoded}/embed.html?realtime=true","hls_url":f"{server.url}/{encoded}/index.m3u8","ll_hls_url":f"{server.url}/{encoded}/index.ll.m3u8"}

@app.post("/api/streams")
async def create_stream(payload: StreamCreate, session: Session = Depends(require_session)) -> dict[str, Any]:
    if payload.placement_mode is not None:
        if payload.placement_mode == "assigned" and not server_store.get(payload.placement_server_id): raise HTTPException(400,"Назначенный сервер не найден")
        data_store.set_placement(stream_name=payload.name, mode=payload.placement_mode, primary_server_id=payload.placement_server_id, actor=session.username)
    desired = placement_targets(payload.name) if payload.placement_mode is not None else []
    try: targets=desired or client.select_servers(server_store.enabled(),payload.target_ids)
    except FlussonicError as exc: raise HTTPException(400,str(exc)) from exc
    body={"name":payload.name,"title":payload.title,"provider":payload.provider,"static":payload.static,"inputs":[i.model_dump() for i in payload.inputs]}
    if payload.position is not None: body["position"]=payload.position
    if payload.on_play: body["on_play"]={"url":payload.on_play}
    results=await client.run_many(targets,lambda s:client.put_stream(s,payload.name,body)); summary=operation_summary(results)
    data_store.audit(actor=session.username,action="stream_create",entity_type="stream",entity_id=payload.name,server_id=None,status="success" if summary["ok"] else "partial",summary=f"Создан поток {payload.name}",details=summary)
    return summary

@app.post("/api/import/m3u/preview")
async def preview_m3u_import(payload: M3UImportRequest, _: Session = Depends(require_session)) -> dict[str, Any]:
    parsed = parse_m3u(payload.content)
    if not parsed["items"]:
        raise HTTPException(400, "В M3U не найдено ни одной пары #EXTINF + URL")
    if payload.placement_mode == "assigned":
        target = server_store.get(payload.placement_server_id)
        if target is None or not target.enabled:
            raise HTTPException(400, "Назначенный сервер не найден или отключён")
        targets = [target]
    else:
        targets = list(server_store.enabled())
    if not targets:
        raise HTTPException(409, "Нет включённых Flussonic-серверов")

    existing: set[str] = set()
    inventory_errors: list[dict[str, str]] = []
    async def inventory(server: FlussonicServer):
        try:
            return server, await client.list_stream_configs(server), None
        except Exception as exc:
            return server, {}, str(exc)
    inventories = await asyncio.gather(*(inventory(server) for server in targets))
    for server, configs, error in inventories:
        existing.update(str(name) for name in configs)
        if error:
            inventory_errors.append({"server_id": server.id, "server_name": server.name, "error": error})
    return {**parsed, "items": [{**item, "existing": item["name"] in existing} for item in parsed["items"]], "existing": sum(item["name"] in existing for item in parsed["items"]), "inventory_errors": inventory_errors}


@app.post("/api/import/m3u/apply")
async def apply_m3u_import(payload: M3UImportRequest, session: Session = Depends(require_session)) -> dict[str, Any]:
    parsed = parse_m3u(payload.content)
    items = parsed["items"]
    if not items:
        raise HTTPException(400, "В M3U не найдено ни одной пары #EXTINF + URL")
    if payload.placement_mode == "assigned":
        target = server_store.get(payload.placement_server_id)
        if target is None or not target.enabled:
            raise HTTPException(400, "Назначенный сервер не найден или отключён")
        targets = [target]
    else:
        targets = list(server_store.enabled())
    if not targets:
        raise HTTPException(409, "Нет включённых Flussonic-серверов")

    existing: set[str] = set()
    inventory_errors: list[dict[str, str]] = []
    async def inventory(server: FlussonicServer):
        try:
            return server, await client.list_stream_configs(server), None
        except Exception as exc:
            return server, {}, str(exc)
    inventories = await asyncio.gather(*(inventory(server) for server in targets))
    for server, configs, error in inventories:
        existing.update(str(name) for name in configs)
        if error:
            inventory_errors.append({"server_id": server.id, "server_name": server.name, "error": error})

    semaphore = asyncio.Semaphore(2)
    async def create_one(item: dict[str, str]) -> dict[str, Any]:
        name = item["name"]
        if name in existing and not payload.overwrite_existing:
            return {**item, "ok": True, "status": "skipped", "detail": "Поток уже существует; пропущен без изменений", "results": []}
        body: dict[str, Any] = {
            "name": name,
            "title": item["title"],
            "provider": payload.provider,
            "static": payload.static,
            "inputs": [{"url": item["url"]}],
        }
        if payload.on_play:
            body["on_play"] = {"url": payload.on_play}
        async with semaphore:
            results = await client.run_many(targets, lambda server: client.put_stream(server, name, body))
        summary = operation_summary(results)
        if payload.placement_mode == "assigned" and summary["success_count"]:
            data_store.set_placement(stream_name=name, mode="assigned", primary_server_id=payload.placement_server_id, actor=session.username)
        return {**item, "ok": summary["ok"], "status": "created" if summary["ok"] else "partial" if summary["partial"] else "failed", "results": results}

    results = await asyncio.gather(*(create_one(item) for item in items))
    created = sum(item["status"] == "created" for item in results)
    skipped = sum(item["status"] == "skipped" for item in results)
    failed = sum(item["status"] in {"failed", "partial"} for item in results)
    summary = {
        "ok": failed == 0,
        "partial": created > 0 and failed > 0,
        "parsed": len(items),
        "created": created,
        "skipped": skipped,
        "failed": failed,
        "warnings": parsed["warnings"],
        "inventory_errors": inventory_errors,
        "items": results,
    }
    data_store.audit(actor=session.username, action="m3u_import", entity_type="stream", entity_id=f"{len(items)} streams", server_id=payload.placement_server_id if payload.placement_mode == "assigned" else None, status="success" if summary["ok"] else "partial" if created else "failed", summary=f"M3U импорт: создано {created}, пропущено {skipped}, ошибок {failed}", details=summary)
    return summary


@app.patch("/api/streams/{name:path}")
async def patch_stream(name: str,payload:StreamPatch,session:Session=Depends(require_session))->dict[str,Any]:
    if payload.placement_mode is not None:
        if payload.placement_mode == "assigned" and not server_store.get(payload.placement_server_id): raise HTTPException(400,"Назначенный сервер не найден")
        data_store.set_placement(stream_name=name, mode=payload.placement_mode, primary_server_id=payload.placement_server_id, actor=session.username)
    desired = placement_targets(name) if payload.placement_mode is not None else []
    try: targets=desired or client.select_servers(server_store.enabled(),payload.target_ids)
    except FlussonicError as exc: raise HTTPException(400,str(exc)) from exc
    body=payload.model_dump(exclude={"target_ids","placement_mode","placement_server_id"},exclude_none=True)
    if "on_play" in body: body["on_play"]={"url":body["on_play"]} if body["on_play"] else None
    if "inputs" in body: body["inputs"]=[i.model_dump() for i in payload.inputs or []]
    if not body: raise HTTPException(400,"Нет полей для изменения")
    results=await mutate_with_backup(targets,name,session.username,"stream_update",lambda s:client.put_stream(s,name,body)); return operation_summary(results)

@app.put("/api/streams/{name:path}/inputs")
async def update_inputs(name:str,payload:InputsUpdate,session:Session=Depends(require_session))->dict[str,Any]:
    try: targets=client.select_servers(server_store.enabled(),payload.target_ids)
    except FlussonicError as exc: raise HTTPException(400,str(exc)) from exc
    results=await mutate_with_backup(targets,name,session.username,"inputs_update",lambda s:client.put_stream(s,name,{"inputs":[i.model_dump() for i in payload.inputs]})); return operation_summary(results)

@app.delete("/api/streams/{name:path}")
async def delete_stream(name:str,target_id:list[str]|None=Query(None),session:Session=Depends(require_session))->dict[str,Any]:
    try: targets=client.select_servers(server_store.enabled(),target_id)
    except FlussonicError as exc: raise HTTPException(400,str(exc)) from exc
    results=await mutate_with_backup(targets,name,session.username,"stream_delete",lambda s:client.delete_stream(s,name)); return operation_summary(results)

@app.put("/api/streams/reorder/batch")
async def reorder_streams(payload:ReorderRequest,session:Session=Depends(require_session))->dict[str,Any]:
    try: targets=client.select_servers(server_store.enabled(),payload.target_ids)
    except FlussonicError as exc: raise HTTPException(400,str(exc)) from exc
    async def on_server(server):
        sem=asyncio.Semaphore(8)
        async def upd(item):
            async with sem: await backup_stream(server,item.name,session.username,"stream_reorder"); await client.put_stream(server,item.name,{"position":item.position})
        await asyncio.gather(*(upd(i) for i in payload.streams)); return {"updated":len(payload.streams)}
    results=await client.run_many(targets,on_server); summary=operation_summary(results); data_store.audit(actor=session.username,action="stream_reorder",entity_type="stream",entity_id="batch",server_id=None,status="success" if summary["ok"] else "partial",summary=f"Изменён порядок {len(payload.streams)} потоков",details=summary); return summary

@app.get("/api/compare/{name:path}")
async def compare_stream(name:str,_:Session=Depends(require_session))->dict[str,Any]:
    servers=list(server_store.enabled()); primary=server_store.primary()
    async def fetch(server):
        try:
            raw=await client.get_stream(server,name); config=client.disk_config(raw)
            return server,config,None
        except FlussonicError as exc:
            return server,None,str(exc)
    loaded=await asyncio.gather(*(fetch(server) for server in servers))
    configs={server.id:config for server,config,error in loaded if config}
    errors={server.id:error for server,config,error in loaded if error}
    placement=placement_public(name); expected_ids={server.id for server in placement_targets(name,servers)}
    preferred=placement.get("primary_server_id") if placement["effective_mode"]=="assigned" else (primary.id if primary else None)
    reference_id=preferred if configs.get(preferred) else next((server.id for server in servers if configs.get(server.id)),None)
    reference=configs.get(reference_id); reference_hash=client.config_hash(reference) if reference else None
    items=[]
    for server in servers:
        config=configs.get(server.id); present=bool(config); expected=server.id in expected_ids; matches=bool(config and reference and client.config_hash(config)==reference_hash)
        if errors.get(server.id) and expected: state="error"
        elif expected and not present: state="missing"
        elif expected and matches: state="ok"
        elif expected and present: state="different"
        elif not expected and present: state="extra"
        else: state="ignored"
        items.append({"server_id":server.id,"server_name":server.name,"ok":not bool(errors.get(server.id)),"present":present,"expected":expected,"state":state,"hash":client.config_hash(config) if config else None,"config":config,"differences":client.config_diff(reference,config) if config and reference else [],"error":errors.get(server.id)})
    return {"in_sync":bool(reference) and all(item["state"] in {"ok","ignored"} for item in items),"placement":placement,"reference_server_id":reference_id,"items":items}

@app.get("/api/placement/settings")
async def get_placement_settings(_:Session=Depends(require_session))->dict[str,Any]:
    return {"enabled":placement_enabled(),"mode":"hybrid" if placement_enabled() else "mirror","saved_assignments":len(data_store.placements())}

@app.put("/api/placement/settings")
async def save_placement_settings(payload:PlacementSettingsUpdate,session:Session=Depends(require_session))->dict[str,Any]:
    data_store.set_setting("placement_enabled",payload.enabled)
    data_store.audit(actor=session.username,action="placement_mode",entity_type="settings",entity_id="placement",server_id=None,status="success",summary="Включён гибридный режим размещения" if payload.enabled else "Включён зеркальный режим")
    return {"ok":True,"enabled":payload.enabled,"mode":"hybrid" if payload.enabled else "mirror"}

@app.get("/api/placement/overview")
async def placement_overview(_:Session=Depends(require_session))->dict[str,Any]:
    sync=await build_sync_overview(False)
    assigned=sum(1 for item in sync["items"] if item["placement"]["configured_mode"]=="assigned")
    counts={server.id:0 for server in server_store.enabled()}
    for item in sync["items"]:
        server_id=item["placement"].get("primary_server_id")
        if item["placement"]["configured_mode"]=="assigned" and server_id in counts: counts[server_id]+=1
    server_counts=[{"server_id":server.id,"server_name":server.name,"assigned":counts.get(server.id,0),"capacity":100} for server in server_store.enabled()]
    return {"enabled":placement_enabled(),"items":sync["items"],"server_counts":server_counts,"summary":{"total":len(sync["items"]),"assigned":assigned,"mirror":len(sync["items"])-assigned,"drift":sum(1 for item in sync["items"] if not item["in_sync"])}}

@app.put("/api/placement/stream/{name:path}")
async def save_placement(name:str,payload:PlacementAssignmentUpdate,session:Session=Depends(require_session))->dict[str,Any]:
    if payload.mode=="assigned" and not server_store.get(payload.server_id): raise HTTPException(400,"Сервер назначения не найден")
    item=data_store.set_placement(stream_name=name,mode=payload.mode,primary_server_id=payload.server_id,actor=session.username)
    data_store.audit(actor=session.username,action="placement_assign",entity_type="stream",entity_id=name,server_id=payload.server_id,status="success",summary=f"Размещение {name}: {payload.mode}",details=item)
    return {"ok":True,"placement":placement_public(name)}

@app.put("/api/placement/bulk/assign")
async def save_placement_bulk(payload:PlacementBulkUpdate,session:Session=Depends(require_session))->dict[str,Any]:
    if payload.mode=="assigned" and not server_store.get(payload.server_id): raise HTTPException(400,"Сервер назначения не найден")
    items=data_store.set_placements(stream_names=payload.names,mode=payload.mode,primary_server_id=payload.server_id,actor=session.username)
    data_store.audit(actor=session.username,action="placement_bulk_assign",entity_type="stream",entity_id=f"{len(payload.names)} streams",server_id=payload.server_id,status="success",summary=f"Назначено размещение для {len(payload.names)} потоков",details={"mode":payload.mode,"server_id":payload.server_id,"names":payload.names})
    return {"ok":True,"updated":len(items)}

@app.post("/api/placement/plan")
async def placement_plan(payload: PlacementPlanRequest, _: Session = Depends(require_session)) -> dict[str, Any]:
    servers = list(server_store.enabled())
    if payload.server_ids:
        requested = set(payload.server_ids)
        known = {server.id for server in servers}
        missing = requested - known
        if missing:
            raise HTTPException(400, f"Неизвестные или отключённые CDN: {', '.join(sorted(missing))}")
        servers = [server for server in servers if server.id in requested]
    if not servers:
        raise HTTPException(409, "Нет включённых CDN для планирования")

    primary = server_store.primary()
    if primary is None:
        raise HTTPException(409, "Не назначен основной сервер для чтения списка каналов")
    try:
        configs = await client.list_stream_configs(primary)
    except Exception as exc:
        raise HTTPException(502, f"Не удалось прочитать список каналов с {primary.name}: {exc}") from exc

    def sort_key(name: str) -> tuple[int, str]:
        cfg = configs.get(name) or {}
        try:
            position = int(cfg.get("position") or 1_000_000_000)
        except (TypeError, ValueError):
            position = 1_000_000_000
        return position, name.casefold()

    candidates = [
        name for name in sorted(configs, key=sort_key)
        if (data_store.placement(name) or {}).get("mode", "mirror") == "mirror"
    ]
    placements = data_store.placements()
    rows = []
    for server in servers:
        assigned = sum(
            1 for item in placements.values()
            if item.get("mode") == "assigned" and item.get("primary_server_id") == server.id
        )
        rows.append({"server_id": server.id, "server_name": server.name, "assigned": assigned})
    plan = build_batch_plan(candidates, rows, batch_size=payload.batch_size)
    return {"ok": True, "primary_server_id": primary.id, **plan}


@app.post("/api/placement/pick")
async def placement_pick(payload: PlacementTargetPickRequest, _: Session = Depends(require_session)) -> dict[str, Any]:
    """Select the next N streams that are still in mirror mode for one target CDN.

    This is intentionally non-destructive: it only returns a deterministic selection.
    The actual move still goes through migration Dry Run and explicit apply.
    """
    target = server_store.get(payload.server_id)
    if target is None or not target.enabled:
        raise HTTPException(400, "Целевой CDN не найден или отключён")

    primary = server_store.primary()
    if primary is None:
        raise HTTPException(409, "Не назначен основной сервер для чтения списка каналов")
    try:
        configs = await client.list_stream_configs(primary)
    except Exception as exc:
        raise HTTPException(502, f"Не удалось прочитать список каналов с {primary.name}: {exc}") from exc

    def sort_key(name: str) -> tuple[int, str]:
        cfg = configs.get(name) or {}
        try:
            position = int(cfg.get("position") or 1_000_000_000)
        except (TypeError, ValueError):
            position = 1_000_000_000
        return position, name.casefold()

    candidates = [
        name for name in sorted(configs, key=sort_key)
        if (data_store.placement(name) or {}).get("mode", "mirror") == "mirror"
    ]
    names = candidates[:payload.count]
    assigned_before = sum(
        1 for item in data_store.placements().values()
        if item.get("mode") == "assigned" and item.get("primary_server_id") == target.id
    )
    return {
        "ok": True,
        "server_id": target.id,
        "server_name": target.name,
        "requested_count": payload.count,
        "candidate_count": len(candidates),
        "selected_count": len(names),
        "remaining_after": max(0, len(candidates) - len(names)),
        "assigned_before": assigned_before,
        "assigned_after_if_applied": assigned_before + len(names),
        "names": names,
    }


async def _placement_config_maps(servers: list[FlussonicServer]) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, str]]:
    async def load(server: FlussonicServer):
        try:
            return server, await client.list_stream_configs(server), None
        except Exception as exc:
            return server, {}, str(exc)
    loaded = await asyncio.gather(*(load(server) for server in servers))
    maps = {server.id: configs for server, configs, _ in loaded}
    errors = {server.id: str(error) for server, _, error in loaded if error}
    return maps, errors


def _migration_reference(name: str, target: FlussonicServer, servers: list[FlussonicServer], maps: dict[str, dict[str, dict[str, Any]]]) -> dict[str, Any] | None:
    target_cfg = maps.get(target.id, {}).get(name)
    if target_cfg:
        return target_cfg
    primary = server_store.primary()
    if primary and maps.get(primary.id, {}).get(name):
        return maps[primary.id][name]
    return next((maps.get(server.id, {}).get(name) for server in servers if maps.get(server.id, {}).get(name)), None)


@app.post("/api/placement/migrate/dry-run")
async def placement_migration_dry_run(payload: PlacementMigrationRequest, _: Session = Depends(require_session)) -> dict[str, Any]:
    target = server_store.get(payload.server_id)
    if target is None or not target.enabled:
        raise HTTPException(400, "Целевой CDN не найден или отключён")
    servers = list(server_store.enabled())
    maps, errors = await _placement_config_maps(servers)
    cluster = cluster_store.get()
    peer_ids = {str(item.get("server_id")) for item in cluster.get("peers") or []}
    peer_ready = bool(cluster.get("enabled")) and all(server.id in peer_ids for server in servers)
    warnings: list[str] = []
    if not peer_ready:
        warnings.append("В настройках панели не все включённые CDN отмечены как peer. После удаления копий проверьте открытие каналов через ваш обычный LB URL.")
    if errors:
        warnings.append("Часть CDN недоступна для чтения. Миграция будет заблокирована, пока панель не сможет проверить все серверы.")

    items: list[dict[str, Any]] = []
    for name in payload.names:
        present = [server for server in servers if maps.get(server.id, {}).get(name)]
        reference = _migration_reference(name, target, servers, maps)
        item_errors = [f"{next((s.name for s in servers if s.id == sid), sid)}: {error}" for sid, error in errors.items()]
        if reference is None:
            item_errors.append("Конфигурация потока не найдена ни на одном CDN")
        target_current = maps.get(target.id, {}).get(name)
        target_same = bool(reference and target_current and client.config_hash(target_current) == client.config_hash(reference))
        extras = [server for server in present if server.id != target.id]
        placement = placement_public(name)
        before_placement = placement.get("primary_server_name") if placement.get("configured_mode") == "assigned" else "Зеркало"
        changes = [
            {"field": "Размещение", "before": before_placement, "after": target.name},
            {"field": f"{target.name}: конфигурация", "before": "совпадает" if target_same else ("другая" if target_current else "нет"), "after": "проверена и готова"},
            {"field": "Копии на CDN", "before": [server.name for server in present] or ["нигде"], "after": [target.name]},
        ]
        note = "Удалятся после проверки target: " + ", ".join(server.name for server in extras) if extras else "Лишних копий нет"
        items.append({
            "stream_name": name,
            "server_id": target.id,
            "server_name": target.name,
            "ok": not item_errors,
            "changed": bool(not target_same or extras or before_placement != target.name),
            "operation": "placement_migrate",
            "changes": changes,
            "note": note,
            "error": "; ".join(item_errors) if item_errors else None,
            "target_write": not target_same,
            "delete_count": len(extras),
            "delete_servers": [server.name for server in extras],
        })

    failures = [item for item in items if not item["ok"]]
    changed = [item for item in items if item["ok"] and item["changed"]]
    return {
        "ok": not failures,
        "operation": "placement_migrate",
        "stream_count": len(payload.names),
        "target_count": sum(1 + int(item.get("delete_count") or 0) for item in items),
        "change_count": len(changed),
        "unchanged_count": sum(1 for item in items if item["ok"] and not item["changed"]),
        "failure_count": len(failures),
        "target_server_id": target.id,
        "target_server_name": target.name,
        "peer_ready": peer_ready,
        "warnings": warnings,
        "items": items,
    }


@app.post("/api/placement/migrate")
async def placement_migrate(payload: PlacementMigrationRequest, session: Session = Depends(require_session)) -> dict[str, Any]:
    if not placement_enabled():
        raise HTTPException(409, "Сначала включите гибридное распределение")
    target = server_store.get(payload.server_id)
    if target is None or not target.enabled:
        raise HTTPException(400, "Целевой CDN не найден или отключён")
    servers = list(server_store.enabled())
    maps, errors = await _placement_config_maps(servers)
    if errors:
        detail = "; ".join(f"{next((s.name for s in servers if s.id == sid), sid)}: {error}" for sid, error in errors.items())
        raise HTTPException(502, f"Миграция остановлена: не удалось проверить все CDN. {detail}")

    snapshot = {"streams": {}}
    for name in payload.names:
        snapshot["streams"][name] = {
            "placement": data_store.placement(name),
            "servers": {
                server.id: {
                    "server_name": server.name,
                    "present": bool(maps.get(server.id, {}).get(name)),
                    "config": maps.get(server.id, {}).get(name),
                } for server in servers
            },
        }
    migration_id = data_store.create_migration_batch(
        actor=session.username, target_server_id=target.id, target_server_name=target.name, snapshot=snapshot
    )

    stream_sem = asyncio.Semaphore(3)

    async def process(name: str) -> dict[str, Any]:
        async with stream_sem:
            reference = _migration_reference(name, target, servers, maps)
            if reference is None:
                return {"stream_name": name, "ok": False, "stage": "reference", "error": "Конфигурация потока не найдена"}
            body = dict(reference)
            body.pop("name", None)
            expected_hash = client.config_hash(reference)
            actions: list[dict[str, Any]] = []
            try:
                target_current = maps.get(target.id, {}).get(name)
                if not target_current or client.config_hash(target_current) != expected_hash:
                    if target_current:
                        await backup_stream(target, name, session.username, "migration_target_update")
                    await client.put_stream(target, name, body)
                    actions.append({"action": "target_write", "server_id": target.id, "server_name": target.name})
                check = await client.get_stream(target, name)
                actual = client.disk_config(check)
                if client.config_hash(actual) != expected_hash:
                    raise FlussonicError("Целевой CDN вернул другую конфигурацию после записи")
                actions.append({"action": "target_verified", "server_id": target.id, "server_name": target.name})

                # Target is already proven healthy at config level.  Persist desired
                # placement before removing extras so a partial delete cannot make the
                # panel treat the stream as a mirror and recreate removed copies.
                placement = data_store.set_placement(
                    stream_name=name,
                    mode="assigned",
                    primary_server_id=target.id,
                    actor=session.username,
                )
                actions.append({"action": "placement_committed", "server_id": target.id, "server_name": target.name})

                extras = [server for server in servers if server.id != target.id and maps.get(server.id, {}).get(name)]
                for server in extras:
                    await backup_stream(server, name, session.username, "migration_remove_extra")
                    await client.delete_stream(server, name)
                    actions.append({"action": "extra_deleted", "server_id": server.id, "server_name": server.name})

                final_check = await client.get_stream(target, name)
                if client.config_hash(client.disk_config(final_check)) != expected_hash:
                    raise FlussonicError("Финальная проверка целевого CDN не прошла")
                data_store.audit(
                    actor=session.username,
                    action="placement_migrate_stream",
                    entity_type="stream",
                    entity_id=name,
                    server_id=target.id,
                    status="success",
                    summary=f"Поток {name} перенесён на {target.name}",
                    details={"target": target.id, "actions": actions, "placement": placement},
                )
                return {"stream_name": name, "ok": True, "stage": "done", "target_server_id": target.id, "target_server_name": target.name, "actions": actions}
            except Exception as exc:
                data_store.audit(
                    actor=session.username,
                    action="placement_migrate_stream",
                    entity_type="stream",
                    entity_id=name,
                    server_id=target.id,
                    status="failed",
                    summary=f"Не удалось перенести {name} на {target.name}",
                    details={"target": target.id, "actions": actions, "error": str(exc)},
                )
                return {"stream_name": name, "ok": False, "stage": "apply", "target_server_id": target.id, "target_server_name": target.name, "actions": actions, "error": str(exc)}

    results = list(await asyncio.gather(*(process(name) for name in payload.names)))
    summary = operation_summary(results)
    summary.update({"target_server_id": target.id, "target_server_name": target.name, "stream_count": len(payload.names), "migration_id": migration_id})
    batch_status = "success" if summary["ok"] else "partial" if summary["success_count"] else "failed"
    data_store.finish_migration_batch(migration_id, status=batch_status, result=summary)
    data_store.audit(
        actor=session.username,
        action="placement_migrate_batch",
        entity_type="stream",
        entity_id=f"{len(payload.names)} streams",
        server_id=target.id,
        status="success" if summary["ok"] else "partial" if summary["success_count"] else "failed",
        summary=f"Миграция на {target.name}: {summary['success_count']} успешно, {summary['failure_count']} ошибок",
        details=summary,
    )
    return summary


@app.get("/api/placement/migrations")
async def placement_migrations(limit: int = Query(default=50, ge=1, le=200), _: Session = Depends(require_session)) -> dict[str, Any]:
    return {"items": data_store.migration_batches(limit=limit)}


def _migration_snapshot_reference(before: dict[str, Any], preferred_server_id: str | None) -> dict[str, Any] | None:
    servers = before.get("servers") or {}
    if preferred_server_id:
        preferred = servers.get(preferred_server_id) or {}
        if preferred.get("present") and preferred.get("config"):
            return preferred.get("config")
    for snap in servers.values():
        if snap.get("present") and snap.get("config"):
            return snap.get("config")
    return None


@app.post("/api/placement/migrations/{migration_id}/verify")
async def placement_migration_verify(migration_id: int, _: Session = Depends(require_session)) -> dict[str, Any]:
    batch = data_store.migration_batch(migration_id)
    if batch is None:
        raise HTTPException(404, "Миграция не найдена")
    if batch.get("rolled_back_at"):
        raise HTTPException(409, "Эта миграция уже откатана; проверяйте текущее размещение в разделе Размещение")

    snapshot = batch.get("snapshot", {}).get("streams") or {}
    if not snapshot:
        raise HTTPException(409, "У этой миграции нет снимка для постпроверки")

    all_servers = {server.id: server for server in server_store.all()}
    snapshot_ids = {sid for item in snapshot.values() for sid in (item.get("servers") or {})}
    current_enabled_ids = {server.id for server in server_store.enabled()}
    relevant_ids = snapshot_ids | current_enabled_ids | {str(batch.get("target_server_id") or "")}
    relevant_ids.discard("")
    relevant = [all_servers[sid] for sid in relevant_ids if sid in all_servers and all_servers[sid].enabled]
    maps, read_errors = await _placement_config_maps(relevant)

    target_id = str(batch.get("target_server_id") or "")
    target_name = str(batch.get("target_server_name") or target_id)
    items: list[dict[str, Any]] = []
    server_checks = 0

    for name, before in snapshot.items():
        expected_reference = _migration_snapshot_reference(before, target_id)
        expected_hash = client.config_hash(expected_reference) if expected_reference else None
        placement = data_store.placement(name)
        placement_ok = bool(placement and placement.get("mode") == "assigned" and placement.get("primary_server_id") == target_id)
        server_states: list[dict[str, Any]] = []
        extras: list[str] = []
        unreadable: list[str] = []
        target_present = False
        target_hash_ok = False if expected_hash else True

        for server_id in sorted(relevant_ids, key=lambda sid: (all_servers.get(sid).name.casefold() if all_servers.get(sid) else sid.casefold())):
            server = all_servers.get(server_id)
            server_name = server.name if server else (before.get("servers", {}).get(server_id, {}).get("server_name") or server_id)
            expected = server_id == target_id
            server_checks += 1
            if server is None or not server.enabled:
                unreadable.append(server_name)
                server_states.append({"server_id": server_id, "server_name": server_name, "expected": expected, "present": None, "state": "unavailable", "label": "сервер отсутствует/отключён"})
                continue
            if server_id in read_errors:
                unreadable.append(server_name)
                server_states.append({"server_id": server_id, "server_name": server_name, "expected": expected, "present": None, "state": "unavailable", "label": read_errors[server_id]})
                continue
            current = maps.get(server_id, {}).get(name)
            present = current is not None
            if expected:
                target_present = present
                if present and expected_hash:
                    target_hash_ok = client.config_hash(current) == expected_hash
                state = "ok" if present else "missing"
                label = "назначен · есть" if present else "назначен · отсутствует"
            elif present:
                extras.append(server_name)
                state = "extra"
                label = "лишняя копия"
            else:
                state = "ok"
                label = "нет · правильно"
            server_states.append({"server_id": server_id, "server_name": server_name, "expected": expected, "present": present, "state": state, "label": label})

        critical_reasons: list[str] = []
        warning_reasons: list[str] = []
        if not placement_ok:
            critical_reasons.append("Размещение в панели не зафиксировано на целевом CDN")
        if not target_present:
            critical_reasons.append(f"Поток отсутствует на {target_name}")
        elif expected_hash and not target_hash_ok:
            critical_reasons.append(f"Конфигурация на {target_name} отличается от снимка до миграции")
        if extras:
            warning_reasons.append("Остались лишние копии: " + ", ".join(extras))
        if unreadable:
            warning_reasons.append("Не удалось проверить: " + ", ".join(unreadable))
        if critical_reasons:
            status = "critical"
        elif warning_reasons:
            status = "warning"
        else:
            status = "ok"

        items.append({
            "stream_name": name,
            "target_server_id": target_id,
            "target_server_name": target_name,
            "status": status,
            "placement_ok": placement_ok,
            "target_present": target_present,
            "target_config_ok": target_hash_ok,
            "extras": extras,
            "unreadable": unreadable,
            "reasons": critical_reasons + warning_reasons,
            "servers": server_states,
        })

    ok_count = sum(1 for item in items if item["status"] == "ok")
    warning_count = sum(1 for item in items if item["status"] == "warning")
    critical_count = sum(1 for item in items if item["status"] == "critical")
    return {
        "ok": critical_count == 0 and warning_count == 0,
        "migration_id": migration_id,
        "target_server_id": target_id,
        "target_server_name": target_name,
        "stream_count": len(items),
        "server_checks": server_checks,
        "ok_count": ok_count,
        "warning_count": warning_count,
        "critical_count": critical_count,
        "checked_at": int(time.time()),
        "items": items,
    }


def _rollback_preview(batch: dict[str, Any], maps: dict[str, dict[str, dict[str, Any]]], servers_by_id: dict[str, FlussonicServer]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for name, before in (batch.get("snapshot", {}).get("streams") or {}).items():
        changes: list[dict[str, Any]] = []
        errors: list[str] = []
        desired_servers = before.get("servers") or {}
        touched = 0
        for server_id, snap in desired_servers.items():
            server = servers_by_id.get(server_id)
            if server is None or not server.enabled:
                errors.append(f"Сервер {snap.get('server_name') or server_id} отсутствует или отключён")
                continue
            current = maps.get(server_id, {}).get(name)
            desired = snap.get("config") if snap.get("present") else None
            if desired is not None:
                if current is None or client.config_hash(current) != client.config_hash(desired):
                    changes.append({"field": f"{server.name}: поток", "before": "нет" if current is None else "изменён", "after": "восстановить конфигурацию"})
                    touched += 1
            elif current is not None:
                changes.append({"field": f"{server.name}: поток", "before": "есть", "after": "удалить (до миграции отсутствовал)"})
                touched += 1
        current_placement = data_store.placement(name)
        original_placement = before.get("placement")
        cur_mode = (current_placement or {}).get("mode", "mirror")
        cur_server = (current_placement or {}).get("primary_server_id")
        old_mode = (original_placement or {}).get("mode", "mirror")
        old_server = (original_placement or {}).get("primary_server_id")
        if (cur_mode, cur_server) != (old_mode, old_server) or (current_placement is None) != (original_placement is None):
            changes.append({"field": "Размещение", "before": f"{cur_mode}:{cur_server or '-'}", "after": f"{old_mode}:{old_server or '-'}"})
        items.append({
            "stream_name": name, "server_id": batch.get("target_server_id"), "server_name": batch.get("target_server_name"),
            "ok": not errors, "changed": bool(changes), "operation": "placement_rollback", "changes": changes,
            "note": f"Будет восстановлено состояние до миграции #{batch['id']}", "error": "; ".join(errors) if errors else None,
            "target_count": max(1, touched),
        })
    failures = [item for item in items if not item["ok"]]
    return {
        "ok": not failures, "operation": "placement_rollback", "migration_id": batch["id"],
        "stream_count": len(items), "target_count": sum(item.get("target_count", 1) for item in items),
        "change_count": sum(1 for item in items if item["ok"] and item["changed"]),
        "unchanged_count": sum(1 for item in items if item["ok"] and not item["changed"]),
        "failure_count": len(failures), "warnings": ["Откат восстановит конфигурации и размещение, сохранённые непосредственно перед этой миграцией."],
        "items": items,
    }


@app.post("/api/placement/migrations/{migration_id}/rollback/dry-run")
async def placement_rollback_dry_run(migration_id: int, _: Session = Depends(require_session)) -> dict[str, Any]:
    batch = data_store.migration_batch(migration_id)
    if batch is None:
        raise HTTPException(404, "Миграция не найдена")
    if batch.get("rolled_back_at"):
        raise HTTPException(409, "Эта миграция уже полностью откатана")
    snapshot_servers = {sid for item in (batch.get("snapshot", {}).get("streams") or {}).values() for sid in (item.get("servers") or {})}
    servers_by_id = {server.id: server for server in server_store.all()}
    relevant = [servers_by_id[sid] for sid in snapshot_servers if sid in servers_by_id and servers_by_id[sid].enabled]
    maps, errors = await _placement_config_maps(relevant)
    missing = snapshot_servers - {server.id for server in relevant}
    if errors or missing:
        details = [f"{sid}: {err}" for sid, err in errors.items()] + [f"{sid}: сервер отсутствует/отключён" for sid in sorted(missing)]
        raise HTTPException(502, "Откат остановлен: " + "; ".join(details))
    return _rollback_preview(batch, maps, servers_by_id)


@app.post("/api/placement/migrations/{migration_id}/rollback")
async def placement_rollback(migration_id: int, session: Session = Depends(require_session)) -> dict[str, Any]:
    batch = data_store.migration_batch(migration_id)
    if batch is None:
        raise HTTPException(404, "Миграция не найдена")
    if batch.get("rolled_back_at"):
        raise HTTPException(409, "Эта миграция уже полностью откатана")
    snapshot = batch.get("snapshot", {}).get("streams") or {}
    snapshot_servers = {sid for item in snapshot.values() for sid in (item.get("servers") or {})}
    servers_by_id = {server.id: server for server in server_store.all()}
    relevant = [servers_by_id[sid] for sid in snapshot_servers if sid in servers_by_id and servers_by_id[sid].enabled]
    maps, errors = await _placement_config_maps(relevant)
    missing = snapshot_servers - {server.id for server in relevant}
    if errors or missing:
        details = [f"{sid}: {err}" for sid, err in errors.items()] + [f"{sid}: сервер отсутствует/отключён" for sid in sorted(missing)]
        raise HTTPException(502, "Откат остановлен: " + "; ".join(details))
    sem = asyncio.Semaphore(3)

    async def restore(name: str, before: dict[str, Any]) -> dict[str, Any]:
        async with sem:
            actions: list[dict[str, Any]] = []
            try:
                for server_id, snap in (before.get("servers") or {}).items():
                    server = servers_by_id[server_id]
                    current = maps.get(server_id, {}).get(name)
                    desired = snap.get("config") if snap.get("present") else None
                    if desired is not None:
                        if current is None or client.config_hash(current) != client.config_hash(desired):
                            if current is not None:
                                await backup_stream(server, name, session.username, "migration_rollback_overwrite")
                            body = dict(desired); body.pop("name", None)
                            await client.put_stream(server, name, body)
                            check = client.disk_config(await client.get_stream(server, name))
                            if client.config_hash(check) != client.config_hash(desired):
                                raise FlussonicError(f"{server.name}: конфигурация после восстановления не совпадает")
                            actions.append({"action": "restored", "server_id": server.id, "server_name": server.name})
                    elif current is not None:
                        await backup_stream(server, name, session.username, "migration_rollback_remove")
                        await client.delete_stream(server, name)
                        actions.append({"action": "removed", "server_id": server.id, "server_name": server.name})
                original = before.get("placement")
                if original is None:
                    data_store.delete_placement(name)
                    actions.append({"action": "placement_removed"})
                else:
                    data_store.set_placement(stream_name=name, mode=original.get("mode") or "mirror", primary_server_id=original.get("primary_server_id"), actor=session.username)
                    actions.append({"action": "placement_restored", "mode": original.get("mode") or "mirror", "server_id": original.get("primary_server_id")})
                data_store.audit(actor=session.username, action="placement_rollback_stream", entity_type="stream", entity_id=name, server_id=batch.get("target_server_id"), status="success", summary=f"{name} восстановлен из миграции #{migration_id}", details={"migration_id": migration_id, "actions": actions})
                return {"stream_name": name, "ok": True, "actions": actions}
            except Exception as exc:
                data_store.audit(actor=session.username, action="placement_rollback_stream", entity_type="stream", entity_id=name, server_id=batch.get("target_server_id"), status="failed", summary=f"Ошибка отката {name} из миграции #{migration_id}", details={"migration_id": migration_id, "actions": actions, "error": str(exc)})
                return {"stream_name": name, "ok": False, "actions": actions, "error": str(exc)}

    results = list(await asyncio.gather(*(restore(name, before) for name, before in snapshot.items())))
    summary = operation_summary(results)
    summary.update({"migration_id": migration_id, "stream_count": len(snapshot)})
    data_store.mark_migration_rollback(migration_id, actor=session.username, result=summary, complete=summary["ok"])
    data_store.audit(actor=session.username, action="placement_rollback_batch", entity_type="migration", entity_id=str(migration_id), server_id=batch.get("target_server_id"), status="success" if summary["ok"] else "partial" if summary["success_count"] else "failed", summary=f"Откат миграции #{migration_id}: {summary['success_count']} успешно, {summary['failure_count']} ошибок", details=summary)
    return summary


@app.post("/api/placement/apply")
async def apply_placement(payload:PlacementApplyRequest,session:Session=Depends(require_session))->dict[str,Any]:
    servers=list(server_store.enabled())
    async def load(server):
        try:return server,await client.list_stream_configs(server),None
        except Exception as exc:return server,{},str(exc)
    loaded=await asyncio.gather(*(load(server) for server in servers))
    maps={server.id:configs for server,configs,error in loaded}
    server_errors={server.id:error for server,configs,error in loaded if error}
    operation_sem=asyncio.Semaphore(8)

    async def put_verified(server:FlussonicServer,name:str,body:dict[str,Any],expected_hash:str)->dict[str,Any]:
        async with operation_sem:
            if server_errors.get(server.id):
                return {"server_id":server.id,"server_name":server.name,"stream_name":name,"action":"put","ok":False,"error":server_errors[server.id]}
            try:
                if maps.get(server.id,{}).get(name): await backup_stream(server,name,session.username,"placement_apply")
                data=await client.put_stream(server,name,body)
                check=await client.get_stream(server,name); actual=client.disk_config(check)
                if client.config_hash(actual)!=expected_hash: raise FlussonicError("Проверка конфигурации после записи не прошла")
                return {"server_id":server.id,"server_name":server.name,"stream_name":name,"action":"put","ok":True,"data":data}
            except Exception as exc:
                return {"server_id":server.id,"server_name":server.name,"stream_name":name,"action":"put","ok":False,"error":str(exc)}

    async def delete_extra(server:FlussonicServer,name:str)->dict[str,Any]:
        async with operation_sem:
            if server_errors.get(server.id):
                return {"server_id":server.id,"server_name":server.name,"stream_name":name,"action":"delete","ok":False,"error":server_errors[server.id]}
            try:
                await backup_stream(server,name,session.username,"placement_remove_extra")
                data=await client.delete_stream(server,name)
                return {"server_id":server.id,"server_name":server.name,"stream_name":name,"action":"delete","ok":True,"data":data}
            except Exception as exc:
                return {"server_id":server.id,"server_name":server.name,"stream_name":name,"action":"delete","ok":False,"error":str(exc)}

    async def process_stream(name:str)->list[dict[str,Any]]:
        expected={server.id for server in placement_targets(name,servers)}
        placement=placement_public(name)
        preferred=placement.get("primary_server_id") if placement["effective_mode"]=="assigned" else (server_store.primary().id if server_store.primary() else None)
        reference=maps.get(preferred,{}).get(name) if preferred else None
        if reference is None:
            reference=next((maps.get(server.id,{}).get(name) for server in servers if maps.get(server.id,{}).get(name)),None)
        if reference is None:
            return [{"stream_name":name,"action":"source","ok":False,"error":"Конфигурация потока не найдена"}]
        body=dict(reference); body.pop("name",None); expected_hash=client.config_hash(reference)
        puts=[put_verified(server,name,body,expected_hash) for server in servers if server.id in expected and (not maps.get(server.id,{}).get(name) or client.config_hash(maps[server.id][name])!=expected_hash)]
        stream_results=list(await asyncio.gather(*puts)) if puts else []
        if any(not item["ok"] for item in stream_results):
            stream_results.append({"stream_name":name,"action":"delete_skipped","ok":False,"error":"Лишние копии не удалены: целевой CDN не прошёл проверку"})
            return stream_results
        if payload.remove_extras:
            deletes=[delete_extra(server,name) for server in servers if server.id not in expected and maps.get(server.id,{}).get(name)]
            if deletes: stream_results.extend(await asyncio.gather(*deletes))
        if not stream_results:
            stream_results.append({"stream_name":name,"action":"noop","ok":True,"detail":"Размещение уже соответствует выбранной модели"})
        return stream_results

    grouped=await asyncio.gather(*(process_stream(name) for name in payload.names))
    results=[item for group in grouped for item in group]
    summary={"ok":all(item.get("ok") for item in results),"success_count":sum(bool(item.get("ok")) for item in results),"failure_count":sum(not bool(item.get("ok")) for item in results),"results":results,"remove_extras":payload.remove_extras}
    data_store.audit(actor=session.username,action="placement_apply",entity_type="stream",entity_id=f"{len(payload.names)} streams",server_id=None,status="success" if summary["ok"] else "partial",summary=f"Применено размещение для {len(payload.names)} потоков",details=summary)
    return summary

async def build_sync_overview(send_notification:bool=True)->dict[str,Any]:
    servers=list(server_store.enabled()); primary=server_store.primary()
    if not primary: return {"items":[],"primary_id":None,"placement_enabled":placement_enabled()}
    async def load(server):
        try: return server, await client.list_stream_configs(server), None
        except Exception as exc: return server, {}, str(exc)
    loaded=await asyncio.gather(*(load(s) for s in servers))
    maps={server.id:configs for server,configs,error in loaded}
    errors={server.id:error for server,configs,error in loaded if error}
    assigned_names=set(data_store.placements())
    names=sorted(set().union(assigned_names, *(set(configs) for configs in maps.values())))
    items=[]
    for name in names:
        placement=placement_public(name)
        expected_ids={server.id for server in placement_targets(name,servers)}
        preferred_id=placement.get("primary_server_id") if placement["effective_mode"]=="assigned" else primary.id
        reference_id=preferred_id if maps.get(preferred_id,{}).get(name) else next((server.id for server in servers if maps.get(server.id,{}).get(name)), None)
        reference=maps.get(reference_id,{}).get(name) if reference_id else None
        reference_hash=client.config_hash(reference) if reference else None
        states=[]
        for server in servers:
            cfg=maps.get(server.id,{}).get(name); present=bool(cfg); expected=server.id in expected_ids
            current_hash=client.config_hash(cfg) if cfg else None
            matches=bool(cfg and reference and current_hash==reference_hash)
            differences=client.config_diff(reference,cfg) if cfg and reference else (["missing"] if expected and reference and not cfg else [])
            if errors.get(server.id): state="error"
            elif expected and not present: state="missing"
            elif expected and matches: state="ok"
            elif expected and present: state="different"
            elif not expected and present: state="extra"
            else: state="ignored"
            states.append({"server_id":server.id,"server_name":server.name,"expected":expected,"present":present,"hash":current_hash,"matches_primary":matches,"matches_reference":matches,"differences":differences,"state":state,"error":errors.get(server.id)})
        in_sync=bool(reference) and all(item["state"] in {"ok","ignored"} for item in states)
        items.append({"name":name,"primary_present":bool(maps.get(primary.id,{}).get(name)),"reference_server_id":reference_id,"in_sync":in_sync,"placement":placement,"states":states})
    drift_count=sum(1 for item in items if not item["in_sync"])
    if drift_count and send_notification:
        mode_text="гибридном режиме" if placement_enabled() else f"зеркальном режиме от {primary.name}"
        await notifications.send("sync_drift", f"⚠️ Рассинхронизация Flussonic: {drift_count} потоков отличаются в {mode_text}")
    return {"primary_id":primary.id,"primary_name":primary.name,"placement_enabled":placement_enabled(),"items":items,"errors":errors}


@app.get("/api/sync/overview")
async def sync_overview(_:Session=Depends(require_session))->dict[str,Any]:
    return await build_sync_overview(True)

@app.post("/api/sync/{name:path}")
async def sync_stream(name:str,payload:SyncRequest,session:Session=Depends(require_session))->dict[str,Any]:
    candidates=[]
    assigned=placement_public(name)
    if assigned["effective_mode"]=="assigned" and assigned.get("primary_server_id"):
        candidate=server_store.get(assigned["primary_server_id"]); candidates += [candidate] if candidate else []
    requested=server_store.get(payload.source_id) if payload.source_id else None
    primary=server_store.primary()
    candidates += [item for item in (requested,primary,*server_store.enabled()) if item and item not in candidates]
    source=None; raw=None
    for candidate in candidates:
        try: raw=await client.get_stream(candidate,name); source=candidate; break
        except FlussonicError: continue
    if not source or raw is None: raise HTTPException(400,"Поток не найден ни на одном доступном сервере")
    config=dict(client.disk_config(raw)); config.pop("name",None)
    targets=placement_targets(name) if placement_enabled() else client.select_servers(server_store.enabled(),payload.target_ids)
    targets=[server for server in targets if server.id!=source.id]
    results=await mutate_with_backup(targets,name,session.username,"stream_sync",lambda server:client.put_stream(server,name,config))
    summary=operation_summary(results); summary["source"]={"id":source.id,"name":source.name}; summary["placement"]=assigned; return summary

@app.put("/api/stream-mode")
async def set_stream_mode(payload: StreamModeBulkRequest, session: Session = Depends(require_session)) -> dict[str, Any]:
    """Switch selected streams between ondemand and static on their effective placement targets.

    Missing streams are never created by this operation: each target is read first,
    backed up, and only then patched with the static flag. This is important while
    migrating from mirrored to assigned placement.
    """
    semaphore = asyncio.Semaphore(8)

    async def one(name: str, server: FlussonicServer) -> dict[str, Any]:
        async with semaphore:
            try:
                raw = await client.get_stream(server, name)
                config = dict(client.disk_config(raw))
                current_static = bool(config.get("static", raw.get("static", False)))
                if current_static == payload.static:
                    return {
                        "server_id": server.id,
                        "server_name": server.name,
                        "stream_name": name,
                        "ok": True,
                        "static": payload.static,
                        "changed": False,
                        "detail": "Режим уже установлен",
                    }
                data_store.backup(
                    actor=session.username,
                    action="stream_mode",
                    stream_name=name,
                    server_id=server.id,
                    server_name=server.name,
                    config=config,
                    config_hash=client.config_hash(config),
                )
                data = await client.put_stream(server, name, {"static": payload.static})
                return {
                    "server_id": server.id,
                    "server_name": server.name,
                    "stream_name": name,
                    "ok": True,
                    "static": payload.static,
                    "changed": True,
                    "data": data,
                }
            except Exception as exc:
                return {
                    "server_id": server.id,
                    "server_name": server.name,
                    "stream_name": name,
                    "ok": False,
                    "static": payload.static,
                    "error": str(exc),
                }

    jobs = []
    no_targets = []
    for name in payload.names:
        targets = placement_targets(name)
        if not targets:
            no_targets.append({
                "stream_name": name,
                "ok": False,
                "static": payload.static,
                "error": "Для потока не найден целевой CDN",
            })
            continue
        jobs.extend(one(name, server) for server in targets)

    results = list(await asyncio.gather(*jobs)) if jobs else []
    results.extend(no_targets)
    succeeded = [item for item in results if item["ok"]]
    failed = [item for item in results if not item["ok"]]
    summary = {
        "ok": not failed,
        "partial": bool(succeeded and failed),
        "success_count": len(succeeded),
        "failure_count": len(failed),
        "changed_count": sum(bool(item.get("changed")) for item in succeeded),
        "unchanged_count": sum(not bool(item.get("changed")) for item in succeeded),
        "stream_count": len(payload.names),
        "static": payload.static,
        "mode": "static" if payload.static else "ondemand",
        "results": results,
    }
    data_store.audit(
        actor=session.username,
        action="stream_mode_static" if payload.static else "stream_mode_ondemand",
        entity_type="stream",
        entity_id=f"{len(payload.names)} streams",
        server_id=None,
        status="success" if summary["ok"] else "partial" if summary["partial"] else "failed",
        summary=f"Режим {'static' if payload.static else 'ondemand'}: {len(payload.names)} потоков",
        details=summary,
    )
    return summary


@app.put("/api/stream-state")
async def set_stream_state(payload: StreamStateBulkRequest, session: Session = Depends(require_session)) -> dict[str, Any]:
    """Temporarily disable or re-enable streams without changing their configuration.

    The operation follows the effective placement model: mirrored streams are changed
    on every enabled CDN, assigned streams only on their primary CDN. Existing static/
    ondemand mode, inputs and other stream settings are preserved.
    """
    semaphore = asyncio.Semaphore(8)

    async def one(name: str, server: FlussonicServer) -> dict[str, Any]:
        async with semaphore:
            try:
                raw = await client.get_stream(server, name)
                config = dict(client.disk_config(raw))
                current_disabled = bool(config.get("disabled", raw.get("disabled", False)))
                if current_disabled == payload.disabled:
                    return {
                        "server_id": server.id,
                        "server_name": server.name,
                        "stream_name": name,
                        "ok": True,
                        "disabled": payload.disabled,
                        "changed": False,
                        "detail": "Состояние уже установлено",
                    }

                data_store.backup(
                    actor=session.username,
                    action="stream_disable" if payload.disabled else "stream_enable",
                    stream_name=name,
                    server_id=server.id,
                    server_name=server.name,
                    config=config,
                    config_hash=client.config_hash(config),
                )
                data = await client.put_stream(server, name, {"disabled": payload.disabled})
                if payload.disabled:
                    data_store.mark_source_checks(
                        server_id=server.id,
                        stream_name=name,
                        state="disabled",
                        detail="Поток временно отключён через панель",
                    )
                else:
                    # Enabling a stream must not start a hidden source probe. The cached
                    # disabled result is removed; a fresh check remains manual-only.
                    data_store.delete_source_checks(server_id=server.id, stream_name=name)
                return {
                    "server_id": server.id,
                    "server_name": server.name,
                    "stream_name": name,
                    "ok": True,
                    "disabled": payload.disabled,
                    "changed": True,
                    "data": data,
                }
            except Exception as exc:
                return {
                    "server_id": server.id,
                    "server_name": server.name,
                    "stream_name": name,
                    "ok": False,
                    "disabled": payload.disabled,
                    "error": str(exc),
                }

    jobs = []
    no_targets = []
    for name in payload.names:
        targets = placement_targets(name)
        if not targets:
            no_targets.append({
                "stream_name": name,
                "ok": False,
                "disabled": payload.disabled,
                "error": "Для потока не найден целевой CDN",
            })
            continue
        jobs.extend(one(name, server) for server in targets)

    results = list(await asyncio.gather(*jobs)) if jobs else []
    results.extend(no_targets)
    succeeded = [item for item in results if item["ok"]]
    failed = [item for item in results if not item["ok"]]
    summary = {
        "ok": not failed,
        "partial": bool(succeeded and failed),
        "success_count": len(succeeded),
        "failure_count": len(failed),
        "changed_count": sum(bool(item.get("changed")) for item in succeeded),
        "unchanged_count": sum(not bool(item.get("changed")) for item in succeeded),
        "stream_count": len(payload.names),
        "disabled": payload.disabled,
        "state": "disabled" if payload.disabled else "enabled",
        "results": results,
    }
    data_store.audit(
        actor=session.username,
        action="stream_disable" if payload.disabled else "stream_enable",
        entity_type="stream",
        entity_id=f"{len(payload.names)} streams",
        server_id=None,
        status="success" if summary["ok"] else "partial" if summary["partial"] else "failed",
        summary=f"{'Временно отключено' if payload.disabled else 'Включено'}: {len(payload.names)} потоков",
        details=summary,
    )
    return summary


@app.post("/api/bulk")
async def bulk(payload:BulkOperationRequest,session:Session=Depends(require_session))->dict[str,Any]:
    try: targets=client.select_servers(server_store.enabled(),payload.target_ids)
    except FlussonicError as exc: raise HTTPException(400,str(exc)) from exc
    source=get_server(payload.source_id) if payload.operation=="sync" else None; semaphore=asyncio.Semaphore(12)
    async def one(server,name):
        async with semaphore:
            try:
                await backup_stream(server,name,session.username,f"bulk_{payload.operation}")
                if payload.operation=="delete": data=await client.delete_stream(server,name)
                elif payload.operation=="sync":
                    raw=await client.get_stream(source,name); cfg=dict(client.disk_config(raw)); cfg.pop("name",None); data=await client.put_stream(server,name,cfg)
                else:
                    raw=await client.get_stream(server,name); cfg=dict(client.disk_config(raw)); body={}
                    if payload.operation=="add_input":
                        inputs=list(cfg.get("inputs") or []); value=str(payload.value or "").strip()
                        if value and not any(i.get("url")==value for i in inputs): inputs.append({"url":value})
                        body={"inputs":inputs}
                    elif payload.operation=="set_provider": body={"provider":str(payload.value or "")}
                    elif payload.operation=="set_on_play": body={"on_play":{"url":str(payload.value)}} if payload.value else {"on_play":None}
                    elif payload.operation=="set_static": body={"static":bool(payload.value)}
                    data=await client.put_stream(server,name,body)
                return {"server_id":server.id,"server_name":server.name,"stream_name":name,"ok":True,"data":data}
            except Exception as exc: return {"server_id":server.id,"server_name":server.name,"stream_name":name,"ok":False,"error":str(exc)}
    if payload.operation=="sync" and placement_enabled():
        jobs=[one(server,name) for name in payload.names for server in placement_targets(name) if not (source and server.id==source.id)]
    else:
        jobs=[one(server,name) for server in targets for name in payload.names if not (source and server.id==source.id)]
    results=await asyncio.gather(*jobs); summary={"ok":all(r["ok"] for r in results),"success_count":sum(r["ok"] for r in results),"failure_count":sum(not r["ok"] for r in results),"results":results}
    data_store.audit(actor=session.username,action=f"bulk_{payload.operation}",entity_type="stream",entity_id=f"{len(payload.names)} streams",server_id=None,status="success" if summary["ok"] else "partial",summary=f"Массовая операция {payload.operation}: {len(payload.names)} потоков",details=summary)
    return summary

@app.get("/api/backups")
async def backups(stream_name:str|None=None,limit:int=Query(200,ge=1,le=1000),_:Session=Depends(require_session))->dict[str,Any]: return {"items":data_store.backups(stream_name,limit)}

@app.post("/api/backups/{backup_id}/restore")
async def restore_backup(backup_id:int,session:Session=Depends(require_session))->dict[str,Any]:
    item=data_store.backup_by_id(backup_id)
    if not item: raise HTTPException(404,"Резервная копия не найдена")
    server=get_server(item["server_id"]); await backup_stream(server,item["stream_name"],session.username,"before_restore"); cfg=dict(item["config"]); cfg.pop("name",None)
    try: result=await client.put_stream(server,item["stream_name"],cfg)
    except FlussonicError as exc: raise HTTPException(502,str(exc)) from exc
    data_store.audit(actor=session.username,action="backup_restore",entity_type="stream",entity_id=item["stream_name"],server_id=server.id,status="success",summary=f"Восстановлена версия #{backup_id} потока {item['stream_name']}")
    return {"ok":True,"stream":client.normalize_stream(result)}

@app.get("/api/audit")
async def audit(limit:int=Query(300,ge=1,le=2000),action:str|None=None,_:Session=Depends(require_session))->dict[str,Any]: return {"items":data_store.audit_items(limit,action)}

@app.get("/api/source-checks")
async def source_checks(stream_name:str|None=None,_:Session=Depends(require_session))->dict[str,Any]:
    return {
        "enabled": True,
        "mode": "manual",
        "background": False,
        "interval": None,
        "items": data_store.source_checks(stream_name=stream_name),
    }

@app.post("/api/source-checks/run")
async def run_source_checks(stream_name:str|None=Query(None),server_id:str|None=Query(None),session:Session=Depends(require_session))->dict[str,Any]:
    result=await source_monitor.run_all(stream_name,server_id); data_store.audit(actor=session.username,action="source_check",entity_type="stream",entity_id=stream_name or "all",server_id=server_id,status="success" if result["ok"] else "partial",summary=f"Проверка источников: {result['checked']}",details=result); return result


@app.post("/api/source-checks/action")
async def source_action(payload:SourceActionRequest,session:Session=Depends(require_session))->dict[str,Any]:
    server=get_server(payload.server_id)
    name=payload.stream_name
    try:
        raw=await client.get_stream(server,name)
        config=dict(client.disk_config(raw))
    except FlussonicError as exc:
        raise HTTPException(502,str(exc)) from exc

    await backup_stream(server,name,session.username,f"source_{payload.action}")
    inputs=[dict(item) for item in config.get("inputs",[]) if isinstance(item,dict) and item.get("url")]
    body:dict[str,Any]
    summary_text=""

    if payload.action=="disable_stream":
        body={"disabled":True}
        summary_text=f"Временно отключён поток {name}"
    elif payload.action=="enable_stream":
        body={"disabled":False}
        summary_text=f"Включён поток {name}"
    elif payload.action=="add_input":
        new_url=str(payload.new_input or "").strip()
        if any(item.get("url")==new_url for item in inputs):
            raise HTTPException(409,"Такой input уже существует")
        inputs.append({"url":new_url})
        body={"inputs":inputs}
        summary_text=f"Добавлен input в {name}"
    elif payload.action=="remove_input":
        if len(inputs)<=1:
            raise HTTPException(409,"Нельзя удалить последний input. Сначала добавьте резервный или отключите поток")
        updated=[item for item in inputs if item.get("url")!=payload.input_url]
        if len(updated)==len(inputs):
            raise HTTPException(404,"Input не найден в конфигурации потока")
        body={"inputs":updated}
        summary_text=f"Удалён input из {name}"
    else:
        current=next((item for item in inputs if item.get("url")==payload.input_url),None)
        if current is None:
            raise HTTPException(404,"Input не найден в конфигурации потока")
        body={"inputs":[current]+[item for item in inputs if item.get("url")!=payload.input_url]}
        summary_text=f"Input поднят в основной для {name}"

    try:
        result=await client.put_stream(server,name,body)
    except FlussonicError as exc:
        data_store.audit(actor=session.username,action=f"source_{payload.action}",entity_type="stream",entity_id=name,server_id=server.id,status="failed",summary=f"Ошибка действия с источником {name}",details={"error":str(exc)})
        raise HTTPException(502,str(exc)) from exc

    check_result=None
    if payload.action=="disable_stream":
        data_store.mark_source_checks(server_id=server.id,stream_name=name,state="disabled",detail="Поток временно отключён через панель")
    else:
        # Configuration actions must not trigger hidden network probes. Remove
        # cached results so the operator can explicitly run a fresh check.
        data_store.delete_source_checks(server_id=server.id,stream_name=name)
    data_store.audit(actor=session.username,action=f"source_{payload.action}",entity_type="stream",entity_id=name,server_id=server.id,status="success",summary=summary_text,details={"input_url":payload.input_url,"new_input":payload.new_input})
    return {"ok":True,"action":payload.action,"server":{"id":server.id,"name":server.name},"stream":client.normalize_stream(result),"check":check_result}

@app.get("/api/notifications/settings")
async def notification_settings(_:Session=Depends(require_session))->dict[str,Any]: return notifications.public_settings()

@app.put("/api/notifications/settings")
async def save_notification_settings(payload:NotificationSettingsUpdate,session:Session=Depends(require_session))->dict[str,Any]:
    result=notifications.save_settings(payload.model_dump()); data_store.audit(actor=session.username,action="notification_settings",entity_type="settings",entity_id="notifications",server_id=None,status="success",summary="Изменены настройки уведомлений"); return result

@app.post("/api/notifications/test")
async def test_notification(payload:NotificationTestRequest,_:Session=Depends(require_session))->dict[str,Any]: return {"ok":await notifications.send("test",payload.message,"info",force=True)}

@app.get("/api/notifications")
async def notification_log(_:Session=Depends(require_session))->dict[str,Any]: return {"items":data_store.notification_items()}
