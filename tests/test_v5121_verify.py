from app.main import app, _migration_snapshot_reference


def test_migration_snapshot_reference_prefers_target():
    before = {
        "servers": {
            "cdn1": {"present": True, "config": {"name": "NTV", "provider": "ONE"}},
            "cdn2": {"present": True, "config": {"name": "NTV", "provider": "TWO"}},
        }
    }
    assert _migration_snapshot_reference(before, "cdn2")["provider"] == "TWO"
    assert _migration_snapshot_reference(before, "missing")["provider"] == "ONE"


def test_post_migration_verify_route_registered():
    routes = {(route.path, tuple(sorted(getattr(route, "methods", None) or []))) for route in app.routes}
    assert ("/api/placement/migrations/{migration_id}/verify", ("POST",)) in routes
