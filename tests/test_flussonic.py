from app.flussonic import FlussonicClient


def test_normalize_prefers_config_on_disk():
    raw = {
        "name": "demo",
        "inputs": [{"url": "hls://runtime", "scte35": True}],
        "config_on_disk": {
            "name": "demo",
            "title": "Demo",
            "inputs": [{"url": "hls://disk"}],
            "position": 12,
            "static": False,
        },
        "stats": {"status": "waiting", "alive": False, "running": False},
    }
    normalized = FlussonicClient.normalize_stream(raw)
    assert normalized["inputs"] == [{"url": "hls://disk"}]
    assert normalized["position"] == 12
    assert normalized["status"] == "waiting"


def test_stream_patch_accepts_inputs_in_same_request():
    from app.models import StreamPatch

    patch = StreamPatch(
        title="Demo HD",
        inputs=[{"url": "hls://primary"}, {"url": "m4f://backup"}],
        target_ids=["cdn-1"],
    )

    assert [item.url for item in patch.inputs] == ["hls://primary", "m4f://backup"]


def test_stream_patch_rejects_duplicate_inputs():
    import pytest
    from pydantic import ValidationError
    from app.models import StreamPatch

    with pytest.raises(ValidationError):
        StreamPatch(inputs=[{"url": "hls://same"}, {"url": "hls://same"}])
