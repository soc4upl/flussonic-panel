from app.main import app
from app.models import StreamStateBulkRequest


def test_stream_state_request_accepts_disable_and_enable():
    assert StreamStateBulkRequest(names=["discovery", "cnn"], disabled=True).disabled is True
    assert StreamStateBulkRequest(names=["bbc"], disabled=False).disabled is False


def test_stream_state_endpoint_is_registered():
    matches = {(route.path, tuple(sorted(getattr(route, "methods", None) or []))) for route in app.routes}
    assert ("/api/stream-state", ("PUT",)) in matches
