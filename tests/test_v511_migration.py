from app.main import app
from app.models import PlacementMigrationRequest, PlacementPlanRequest
from app.placement import build_batch_plan


def test_batch_plan_fills_each_cdn_to_requested_limit():
    plan = build_batch_plan(
        [f"ch{i:03d}" for i in range(1, 251)],
        [
            {"server_id": "cdn1", "server_name": "CDN 1", "assigned": 0},
            {"server_id": "cdn2", "server_name": "CDN 2", "assigned": 20},
            {"server_id": "cdn3", "server_name": "CDN 3", "assigned": 0},
        ],
        batch_size=100,
    )
    assert [batch["planned"] for batch in plan["batches"]] == [100, 80, 70]
    assert plan["planned_count"] == 250
    assert plan["unplanned_count"] == 0
    assert plan["batches"][0]["names"][0] == "ch001"
    assert plan["batches"][1]["names"][0] == "ch101"


def test_batch_plan_reports_overflow_without_mutation():
    plan = build_batch_plan(
        ["a", "b", "c", "d"],
        [{"server_id": "cdn1", "server_name": "CDN 1", "assigned": 2}],
        batch_size=3,
    )
    assert plan["batches"][0]["names"] == ["a"]
    assert plan["unplanned"] == ["b", "c", "d"]


def test_migration_models_and_routes():
    assert PlacementPlanRequest(batch_size=100).batch_size == 100
    assert PlacementMigrationRequest(names=["NTV"], server_id="cdn1").server_id == "cdn1"
    routes = {(route.path, tuple(sorted(getattr(route, "methods", None) or []))) for route in app.routes}
    assert ("/api/placement/plan", ("POST",)) in routes
    assert ("/api/placement/migrate/dry-run", ("POST",)) in routes
    assert ("/api/placement/migrate", ("POST",)) in routes
