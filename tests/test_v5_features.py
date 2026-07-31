from pathlib import Path

from app.datastore import DataStore
from app.models import BulkOperationRequest


def test_backup_audit_history_and_source_checks(tmp_path: Path):
    store = DataStore(tmp_path / "panel.db")
    backup_id = store.backup(actor="admin", action="stream_update", stream_name="demo", server_id="cdn1", server_name="CDN 1", config={"name":"demo","inputs":[{"url":"hls://example/live"}]}, config_hash="abc")
    assert store.backup_by_id(backup_id)["config"]["name"] == "demo"
    store.audit(actor="admin", action="stream_update", entity_type="stream", entity_id="demo", server_id=None, status="success", summary="changed")
    assert store.audit_items()[0]["summary"] == "changed"
    store.save_monitor_point({"ts": 1000, "metrics": {"sessions": 3, "logins": 2, "unique_ips": 2, "active_streams": 1}, "server_counts": []}, min_gap=60)
    assert store.monitor_history(0)[0]["sessions"] == 3
    old = store.upsert_source_check({"server_id":"cdn1","server_name":"CDN 1","stream_name":"demo","input_url":"hls://example/live","checked_at":1000,"state":"ok","latency_ms":10})
    assert old is None
    old = store.upsert_source_check({"server_id":"cdn1","server_name":"CDN 1","stream_name":"demo","input_url":"hls://example/live","checked_at":1001,"state":"failed","latency_ms":20,"detail":"timeout"})
    assert old["state"] == "ok"


def test_bulk_model_boolean_and_operations():
    request = BulkOperationRequest(names=["a"], operation="set_static", value=True, target_ids=["cdn1"])
    assert request.value is True


def test_source_probe_url_supports_flussonic_http_schemes():
    from app.source_monitor import SourceMonitor
    assert SourceMonitor._probe_url("hlss://example.com/live/index.m3u8?token=abc") == "https://example.com/live/index.m3u8?token=abc"
    assert SourceMonitor._probe_url("hls://example.com/live/index.m3u8") == "http://example.com/live/index.m3u8"
    assert SourceMonitor._probe_url("tshttp://example.com:88/play/a0ey") == "http://example.com:88/play/a0ey"
    assert SourceMonitor._probe_url("tshttps://example.com/play/a0ey") == "https://example.com/play/a0ey"
    assert SourceMonitor._probe_url("m4f://example.com/channel") is None


def test_source_action_model():
    from app.models import SourceActionRequest
    request = SourceActionRequest(server_id="cdn-2", stream_name="sport", input_url="m4f://host/stream", action="promote_input")
    assert request.action == "promote_input"
    add = SourceActionRequest(server_id="cdn-2", stream_name="sport", action="add_input", new_input="tshttp://host/live")
    assert add.new_input.startswith("tshttp://")


def test_reconcile_source_checks_removes_deleted_inputs_only(tmp_path: Path):
    store = DataStore(tmp_path / "panel.db")
    common = {"server_id": "cdn1", "server_name": "CDN 1", "checked_at": 1000, "state": "failed"}
    store.upsert_source_check(common | {"stream_name": "sport", "input_url": "hls://old/input.m3u8"})
    store.upsert_source_check(common | {"stream_name": "sport", "input_url": "hls://current/input.m3u8"})
    store.upsert_source_check(common | {"stream_name": "news", "input_url": "hls://news/input.m3u8"})
    store.upsert_source_check(common | {"server_id": "cdn2", "server_name": "CDN 2", "stream_name": "sport", "input_url": "hls://old/input.m3u8"})

    removed = store.reconcile_source_checks(
        server_id="cdn1",
        current_sources={"sport": {"hls://current/input.m3u8"}},
        stream_name="sport",
    )

    assert removed == 1
    rows = store.source_checks()
    keys = {(row["server_id"], row["stream_name"], row["input_url"]) for row in rows}
    assert ("cdn1", "sport", "hls://old/input.m3u8") not in keys
    assert ("cdn1", "sport", "hls://current/input.m3u8") in keys
    assert ("cdn1", "news", "hls://news/input.m3u8") in keys
    assert ("cdn2", "sport", "hls://old/input.m3u8") in keys


def test_reconcile_full_server_removes_deleted_stream_records(tmp_path: Path):
    store = DataStore(tmp_path / "panel.db")
    common = {"server_id": "cdn1", "server_name": "CDN 1", "checked_at": 1000, "state": "ok"}
    store.upsert_source_check(common | {"stream_name": "deleted_stream", "input_url": "hls://old"})
    store.upsert_source_check(common | {"stream_name": "live_stream", "input_url": "hls://live"})

    removed = store.reconcile_source_checks(
        server_id="cdn1",
        current_sources={"live_stream": {"hls://live"}},
    )

    assert removed == 1
    assert [row["stream_name"] for row in store.source_checks()] == ["live_stream"]


def test_source_monitor_start_is_manual_only(tmp_path):
    import asyncio
    from types import SimpleNamespace
    from app.source_monitor import SourceMonitor

    settings = SimpleNamespace(source_check_concurrency=1, source_check_timeout=2)
    monitor = SourceMonitor(settings, None, None, None, None)
    asyncio.run(monitor.start())
    assert monitor._task is None
    asyncio.run(monitor.stop())
