from pathlib import Path

from app.datastore import DataStore
from app.main import app


def test_migration_snapshot_roundtrip(tmp_path: Path):
    store = DataStore(tmp_path / "panel.db")
    snapshot = {
        "streams": {
            "NTV": {
                "placement": None,
                "servers": {
                    "cdn1": {"server_name": "CDN 1", "present": True, "config": {"name": "NTV", "inputs": [{"url": "hls://example.invalid/ntv.m3u8"}]}},
                    "cdn2": {"server_name": "CDN 2", "present": True, "config": {"name": "NTV", "inputs": [{"url": "hls://example.invalid/ntv.m3u8"}]}},
                },
            }
        }
    }
    batch_id = store.create_migration_batch(actor="admin", target_server_id="cdn1", target_server_name="CDN 1", snapshot=snapshot)
    batch = store.migration_batch(batch_id)
    assert batch is not None
    assert batch["snapshot"]["streams"]["NTV"]["servers"]["cdn2"]["present"] is True
    store.finish_migration_batch(batch_id, status="success", result={"ok": True, "success_count": 1})
    assert store.migration_batches()[0]["status"] == "success"
    store.mark_migration_rollback(batch_id, actor="admin", result={"ok": True}, complete=True)
    assert store.migration_batch(batch_id)["rolled_back_by"] == "admin"


def test_rollback_routes_registered():
    routes = {(route.path, tuple(sorted(getattr(route, "methods", None) or []))) for route in app.routes}
    assert ("/api/placement/migrations", ("GET",)) in routes
    assert ("/api/placement/migrations/{migration_id}/rollback/dry-run", ("POST",)) in routes
    assert ("/api/placement/migrations/{migration_id}/rollback", ("POST",)) in routes
