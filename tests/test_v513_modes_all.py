from pathlib import Path

import app.main as main
from app.config import FlussonicServer


def _server(server_id: str, name: str, primary: bool = False) -> FlussonicServer:
    return FlussonicServer(
        id=server_id,
        name=name,
        url=f"http://{server_id}.invalid",
        username="u",
        password="p",
        primary=primary,
    )


def test_all_streams_collapses_duplicate_names(monkeypatch):
    cdn1 = _server("cdn1", "CDN 1", True)
    cdn2 = _server("cdn2", "CDN 2")
    monkeypatch.setattr(main.server_store, "primary", lambda: cdn1)
    monkeypatch.setattr(main, "placement_public", lambda name: {
        "enabled": False,
        "configured_mode": "mirror",
        "effective_mode": "mirror",
        "primary_server_id": None,
        "primary_server_name": None,
    })
    rows = [
        (cdn1, [{"name": "NTV", "title": "NTV 1", "provider": "P", "position": 1, "inputs": [], "alive": False, "running": False, "status": "waiting"}]),
        (cdn2, [
            {"name": "NTV", "title": "NTV 2", "provider": "P", "position": 1, "inputs": [], "alive": True, "running": True, "status": "running"},
            {"name": "CNN", "title": "CNN", "provider": "P", "position": 2, "inputs": [], "alive": False, "running": False, "status": "waiting"},
        ]),
    ]
    items = main._aggregate_stream_rows(rows)
    assert [item["name"] for item in items] == ["NTV", "CNN"]
    ntv = items[0]
    assert ntv["reference_server_id"] == "cdn1"
    assert ntv["server_ids"] == ["cdn1", "cdn2"]
    assert ntv["server_count"] == 2
    assert ntv["alive"] is True
    assert ntv["running"] is True


def test_all_streams_prefers_assigned_target(monkeypatch):
    cdn1 = _server("cdn1", "CDN 1", True)
    cdn3 = _server("cdn3", "CDN 3")
    monkeypatch.setattr(main.server_store, "primary", lambda: cdn1)
    monkeypatch.setattr(main, "placement_public", lambda name: {
        "enabled": True,
        "configured_mode": "assigned",
        "effective_mode": "assigned",
        "primary_server_id": "cdn3",
        "primary_server_name": "CDN 3",
    })
    items = main._aggregate_stream_rows([
        (cdn1, [{"name": "BBC", "title": "old", "position": 1, "inputs": [], "alive": False, "running": False, "status": "waiting"}]),
        (cdn3, [{"name": "BBC", "title": "target", "position": 1, "inputs": [], "alive": False, "running": False, "status": "waiting"}]),
    ])
    assert items[0]["reference_server_id"] == "cdn3"
    assert items[0]["title"] == "target"


def test_v513_ui_has_global_modes_and_all_selector():
    root = Path(__file__).resolve().parents[1]
    html = (root / "app/static/index.html").read_text()
    js = (root / "app/static/app.js").read_text()
    assert 'data-view="settings"' in html
    assert 'data-panel-mode="mirror"' in html
    assert 'data-panel-mode="hybrid"' in html
    assert 'value="all"' in js
    assert 'ВСЕ · объединённый список' in js
