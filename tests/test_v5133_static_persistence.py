from pathlib import Path


def test_stream_mode_writes_full_disk_config_and_verifies_persistence():
    source = (Path(__file__).parents[1] / "app" / "main.py").read_text()
    block = source[
        source.index('@app.put("/api/stream-mode")'):
        source.index('@app.put("/api/stream-state")')
    ]
    assert 'desired = dict(config)' in block
    assert 'desired["static"] = payload.static' in block
    assert 'body.pop("name", None)' in block
    assert 'persisted = dict(client.disk_config(check))' in block
    assert 'config_on_disk.static' in block
    assert 'client.config_hash(persisted) != client.config_hash(desired)' in block


def test_legacy_bulk_static_uses_full_disk_config_and_verifies():
    source = (Path(__file__).parents[1] / "app" / "main.py").read_text()
    block = source[
        source.index('@app.post("/api/bulk")'):
        source.index('@app.get("/api/backups")')
    ]
    assert 'desired=dict(cfg); desired["static"]=bool(payload.value)' in block
    assert 'body=dict(desired); body.pop("name",None)' in block
    assert 'config_on_disk.static не сохранился' in block
