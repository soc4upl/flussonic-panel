from pathlib import Path

from app.cluster_store import ClusterStore
from app.flussonic import FlussonicClient
from app.models import ClusterSettingsUpdate


def test_cluster_store_encrypts_key(tmp_path: Path):
    store = ClusterStore(tmp_path / "cluster.json", "secret")
    saved = store.update({
        "enabled": True,
        "balancer_name": "lb01",
        "mode": "clients",
        "cluster_key": "super-secret-key",
        "peers": [{"server_id": "cdn1", "host": "cdn-1.example.com", "max_bitrate": None}],
    })
    assert saved["has_cluster_key"] is True
    assert "super-secret-key" not in (tmp_path / "cluster.json").read_text()
    assert store.get(include_secret=True)["cluster_key"] == "super-secret-key"


def test_cluster_model_validates_mode_and_peers():
    payload = ClusterSettingsUpdate(
        enabled=True,
        balancer_server_id="lb",
        balancer_name="lb01",
        mode="clients",
        cluster_key="abc",
        peers=[{"server_id": "cdn1", "host": "cdn-1.example.com"}],
    )
    assert payload.mode == "clients"
    assert payload.peers[0].host == "cdn-1.example.com"


def test_sync_hash_ignores_position_and_old_input_shape():
    first = {
        "name": "demo",
        "position": 1,
        "title": "Demo",
        "inputs": [{"url": "hls://example/live.m3u8"}],
        "on_play": {"url": "auth://backend"},
    }
    second = {
        "name": "demo",
        "position": 999,
        "title": "Demo",
        "inputs": ["hls://example/live.m3u8"],
        "on_play": "auth://backend",
        "stats": {"alive": True},
    }
    assert FlussonicClient.config_hash(first) == FlussonicClient.config_hash(second)
    assert FlussonicClient.config_diff(first, second) == []
