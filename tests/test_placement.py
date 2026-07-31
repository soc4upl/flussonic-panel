from pathlib import Path

from app.datastore import DataStore
from app.models import PlacementAssignmentUpdate, StreamCreate


def test_placement_store_roundtrip(tmp_path: Path):
    store = DataStore(tmp_path / "panel.db")
    assert store.placement("cnn") is None
    item = store.set_placement(stream_name="cnn", mode="assigned", primary_server_id="cdn2", actor="admin")
    assert item["primary_server_id"] == "cdn2"
    assert store.placement("cnn")["mode"] == "assigned"
    store.set_placement(stream_name="cnn", mode="mirror", primary_server_id="cdn2", actor="admin")
    assert store.placement("cnn")["primary_server_id"] is None


def test_assignment_validation():
    try:
        PlacementAssignmentUpdate(mode="assigned")
        assert False, "validation must fail"
    except ValueError:
        pass
    model = StreamCreate(name="cnn", inputs=[{"url":"hls://example/live.m3u8"}], placement_mode="assigned", placement_server_id="cdn2")
    assert model.placement_server_id == "cdn2"
