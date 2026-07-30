from pathlib import Path

from app.server_store import ServerStore


def test_server_store_encrypts_and_persists(tmp_path: Path):
    path = tmp_path / "servers.json"
    store = ServerStore(path, "test-secret", [])
    server = store.create(
        {
            "name": "CDN 1",
            "url": "http://localhost:8022",
            "username": "panel_api",
            "password": "secret-password",
            "primary": True,
            "node_exporter_url": "http://localhost:9100/metrics",
        }
    )

    assert "secret-password" not in path.read_text(encoding="utf-8")

    restored = ServerStore(path, "test-secret", [])
    assert restored.get(server.id).password == "secret-password"
    assert restored.primary().id == server.id
    assert restored.get(server.id).node_exporter_url == "http://localhost:9100/metrics"
