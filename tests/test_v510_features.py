import pytest
from pydantic import ValidationError

from app.main import _config_diff, app
from app.models import ChangePreviewRequest


def test_change_preview_request_supports_dry_run_operations():
    req = ChangePreviewRequest(names=["cnn", "bbc"], operation="set_disabled", value=True)
    assert req.operation == "set_disabled"
    assert req.value is True
    with pytest.raises(ValidationError):
        ChangePreviewRequest(names=["cnn"], operation="unknown")


def test_dry_run_and_channel_card_routes_are_registered():
    matches = {(route.path, tuple(sorted(getattr(route, "methods", None) or []))) for route in app.routes}
    assert ("/api/changes/dry-run", ("POST",)) in matches
    assert ("/api/channel-card/{name:path}", ("GET",)) in matches


def test_config_diff_ignores_position_but_detects_playback_changes():
    current = {
        "name": "demo",
        "position": 1,
        "provider": "A",
        "static": False,
        "inputs": [{"url": "hls://one"}],
    }
    desired = {
        "name": "demo",
        "position": 99,
        "provider": "B",
        "static": True,
        "inputs": [{"url": "hls://one"}, {"url": "hls://two"}],
    }
    changes = _config_diff(current, desired)
    fields = {item["field"] for item in changes}
    assert "position" not in fields
    assert {"provider", "static", "inputs"}.issubset(fields)


def test_delete_diff_is_explicit():
    changes = _config_diff({"name": "demo", "inputs": [{"url": "hls://one"}]}, None)
    assert changes == [{"field": "stream", "before": "существует", "after": "будет удалён"}]
