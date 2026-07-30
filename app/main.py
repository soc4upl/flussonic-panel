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
from .models import (
    InputsUpdate, LoginRequest, ReorderRequest, StreamCreate, StreamPatch, SyncRequest,
    ServerCreate, ServerUpdate, ServerTestRequest, BulkOperationRequest,
    NotificationSettingsUpdate, NotificationTestRequest, SourceActionRequest,
)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
settings = get_settings()
client = FlussonicClient(settings)
server_store = ServerStore(Path(settings.server_store_path), settings.secret_key, settings.servers)
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


app = FastAPI(title=settings.panel_title, version="5.7.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


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
async def health() -> dict[str, Any]: return {"ok": True, "version": "5.7.0", "configured_servers": len(server_store.all()), "enabled_servers": len(server_store.enabled()), "server_load_monitor": settings.server_load_enabled, "node_exporter": True, "network_realtime": True}

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

@app.get("/api/streams")
async def streams(server_id: str | None = None, search: str | None = None, _: Session = Depends(require_session)) -> dict[str, Any]:
    server=get_server(server_id)
    try: items=await client.list_streams(server)
    except FlussonicError as exc: raise HTTPException(502,str(exc)) from exc
    if search:
        n=search.casefold(); items=[x for x in items if n in x["name"].casefold() or n in x.get("title","").casefold() or n in x.get("provider","").casefold() or any(n in i["url"].casefold() for i in x.get("inputs",[]))]
    return {"server":{"id":server.id,"name":server.name},"stats":{"total":len(items),"alive":sum(x["alive"] for x in items),"running":sum(x["running"] for x in items),"waiting":sum(x["status"]=="waiting" for x in items)},"items":items}

@app.get("/api/streams/{name:path}")
async def stream_detail(name: str, server_id: str | None = None, _: Session = Depends(require_session)) -> dict[str, Any]:
    server=get_server(server_id)
    try: raw=await client.get_stream(server,name)
    except FlussonicError as exc: raise HTTPException(502,str(exc)) from exc
    return {"server":{"id":server.id,"name":server.name},"stream":client.normalize_stream(raw),"config_on_disk":client.disk_config(raw)}

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
    try: targets=client.select_servers(server_store.enabled(),payload.target_ids)
    except FlussonicError as exc: raise HTTPException(400,str(exc)) from exc
    body={"name":payload.name,"title":payload.title,"provider":payload.provider,"static":payload.static,"inputs":[i.model_dump() for i in payload.inputs]}
    if payload.position is not None: body["position"]=payload.position
    if payload.on_play: body["on_play"]={"url":payload.on_play}
    results=await client.run_many(targets,lambda s:client.put_stream(s,payload.name,body)); summary=operation_summary(results)
    data_store.audit(actor=session.username,action="stream_create",entity_type="stream",entity_id=payload.name,server_id=None,status="success" if summary["ok"] else "partial",summary=f"Создан поток {payload.name}",details=summary)
    return summary

@app.patch("/api/streams/{name:path}")
async def patch_stream(name: str,payload:StreamPatch,session:Session=Depends(require_session))->dict[str,Any]:
    try: targets=client.select_servers(server_store.enabled(),payload.target_ids)
    except FlussonicError as exc: raise HTTPException(400,str(exc)) from exc
    body=payload.model_dump(exclude={"target_ids"},exclude_none=True)
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
    async def fetch(server):
        try:
            raw=await client.get_stream(server,name); config=client.disk_config(raw); return {"server_id":server.id,"server_name":server.name,"ok":True,"hash":client.config_hash(config),"config":config,"error":None}
        except FlussonicError as exc: return {"server_id":server.id,"server_name":server.name,"ok":False,"hash":None,"config":None,"error":str(exc)}
    items=await asyncio.gather(*(fetch(s) for s in server_store.enabled())); hashes={i["hash"] for i in items if i["ok"]}; return {"in_sync":len(hashes)<=1 and all(i["ok"] for i in items),"items":items}

@app.get("/api/sync/overview")
async def sync_overview(_:Session=Depends(require_session))->dict[str,Any]:
    servers=list(server_store.enabled()); primary=server_store.primary()
    if not primary: return {"items":[],"primary_id":None}
    async def load(server):
        try: return server, await client.list_stream_configs(server), None
        except Exception as exc: return server, {}, str(exc)
    loaded=await asyncio.gather(*(load(s) for s in servers)); maps={s.id:m for s,m,e in loaded}; errors={s.id:e for s,m,e in loaded if e}; names=sorted(set().union(*(set(m) for m in maps.values())))
    items=[]
    for name in names:
        base=maps.get(primary.id,{}).get(name); base_hash=client.config_hash(base) if base else None; states=[]
        for s in servers:
            cfg=maps[s.id].get(name); h=client.config_hash(cfg) if cfg else None
            states.append({"server_id":s.id,"server_name":s.name,"present":bool(cfg),"hash":h,"matches_primary":bool(cfg and base and h==base_hash),"error":errors.get(s.id)})
        items.append({"name":name,"primary_present":bool(base),"in_sync":bool(base) and all(x["matches_primary"] for x in states),"states":states})
    drift_count=sum(1 for item in items if not item["in_sync"])
    if drift_count:
        await notifications.send("sync_drift", f"⚠️ Рассинхронизация Flussonic: {drift_count} потоков отличаются от основного сервера {primary.name}")
    return {"primary_id":primary.id,"primary_name":primary.name,"items":items,"errors":errors}

@app.post("/api/sync/{name:path}")
async def sync_stream(name:str,payload:SyncRequest,session:Session=Depends(require_session))->dict[str,Any]:
    source=get_server(payload.source_id)
    try:
        raw=await client.get_stream(source,name); config=dict(client.disk_config(raw)); config.pop("name",None); targets=[s for s in client.select_servers(server_store.enabled(),payload.target_ids) if s.id!=source.id]
    except FlussonicError as exc: raise HTTPException(400,str(exc)) from exc
    results=await mutate_with_backup(targets,name,session.username,"stream_sync",lambda s:client.put_stream(s,name,config)); summary=operation_summary(results); summary["source"]={"id":source.id,"name":source.name}; return summary

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
    jobs=[one(s,n) for s in targets for n in payload.names if not (source and s.id==source.id)]; results=await asyncio.gather(*jobs); summary={"ok":all(r["ok"] for r in results),"success_count":sum(r["ok"] for r in results),"failure_count":sum(not r["ok"] for r in results),"results":results}
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
async def source_checks(stream_name:str|None=None,_:Session=Depends(require_session))->dict[str,Any]: return {"enabled":settings.source_check_enabled,"interval":settings.source_check_seconds,"items":data_store.source_checks(stream_name=stream_name)}

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
        data_store.delete_source_checks(server_id=server.id,stream_name=name)
        check_result=await source_monitor.run_all(name,server.id)
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
