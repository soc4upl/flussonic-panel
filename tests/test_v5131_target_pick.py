from pydantic import ValidationError

from app.models import PlacementTargetPickRequest


def test_target_pick_accepts_230():
    payload = PlacementTargetPickRequest(count=230, server_id="cdn-5")
    assert payload.count == 230
    assert payload.server_id == "cdn-5"


def test_target_pick_rejects_more_than_migration_limit():
    try:
        PlacementTargetPickRequest(count=501, server_id="cdn-5")
    except ValidationError:
        return
    raise AssertionError("count > 500 must be rejected")
