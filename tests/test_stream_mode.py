from app.main import app
from app.models import M3UImportRequest, StreamModeBulkRequest


def test_stream_mode_request_accepts_bulk_static_and_ondemand():
    assert StreamModeBulkRequest(names=["discovery", "cnn"], static=True).static is True
    assert StreamModeBulkRequest(names=["bbc"], static=False).static is False


def test_m3u_import_can_create_static_streams():
    request = M3UImportRequest(content="#EXTM3U\n#EXTINF:-1,Demo\nhttp://example/live\n", static=True)
    assert request.static is True


def test_stream_mode_endpoint_is_registered():
    matches = {(route.path, tuple(sorted(getattr(route, "methods", None) or []))) for route in app.routes}
    assert ("/api/stream-mode", ("PUT",)) in matches
