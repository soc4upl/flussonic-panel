from pathlib import Path

from app.flussonic import FlussonicClient


def test_manual_source_check_all_uses_all_enabled_servers():
    source = (Path(__file__).parents[1] / "app" / "source_monitor.py").read_text()
    assert "source_servers = enabled" in source
    assert "source_servers = enabled if self.settings.source_check_all_servers" not in source


def test_flussonic23_missing_static_equals_explicit_true():
    missing = {"name": "demo", "inputs": [{"url": "hls://example.invalid/live.m3u8"}]}
    explicit = {"name": "demo", "inputs": [{"url": "hls://example.invalid/live.m3u8"}], "static": True}
    assert FlussonicClient.canonical_config(missing)["static"] is True
    assert FlussonicClient.config_hash(missing) == FlussonicClient.config_hash(explicit)


def test_static_persistence_checks_default_missing_as_true():
    source = (Path(__file__).parents[1] / "app" / "main.py").read_text()
    assert 'persisted.get("static", True)' in source or 'persisted.get("static",True)' in source
