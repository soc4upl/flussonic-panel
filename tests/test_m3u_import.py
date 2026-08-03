from app.m3u_import import parse_m3u, stream_name_from_title
from app.models import M3UImportRequest


SAMPLE = '''#EXTM3U
#EXTINF:-1 tvg-id="1502" tvg-name="TVP 1 HD" group-title="Национальные" tvg-logo="https://example/1502.png" tvg-chno="1",TVP 1 HD
http://cdn-6.cyriustv.ru:8011/sweet.php/1502?key=a1
#EXTINF:-1 tvg-id="1503" tvg-name="TVP 2 HD" group-title="Национальные",TVP 2 HD
http://cdn-6.cyriustv.ru:8011/sweet.php/1503?key=a1
#EXTINF:-1 tvg-id="3904" tvg-name="MEGA HIT HD" group-title="Фильмовые",MEGA HIT HD
http://cdn-6.cyriustv.ru:8011/sweet.php/3904?key=a1
'''


def test_m3u_parser_uses_only_name_and_url():
    result = parse_m3u(SAMPLE)
    assert result["count"] == 3
    assert result["items"][0] == {
        "name": "TVP_1_HD",
        "title": "TVP 1 HD",
        "url": "http://cdn-6.cyriustv.ru:8011/sweet.php/1502?key=a1",
    }
    assert result["items"][2]["name"] == "MEGA_HIT_HD"
    assert all("logo" not in item and "tvg" not in item for item in result["items"])


def test_duplicate_entry_is_skipped_and_slug_collision_gets_suffix():
    text = '''#EXTM3U
#EXTINF:-1,Test HD
http://one/live
#EXTINF:-1,Test HD
http://one/live
#EXTINF:-1,Test HD
http://two/live
'''
    result = parse_m3u(text)
    assert [item["name"] for item in result["items"]] == ["Test_HD", "Test_HD_2"]
    assert any("повтор" in warning for warning in result["warnings"])


def test_cyrillic_title_gets_readable_system_name():
    name = stream_name_from_title("Первый канал HD", "http://example/live")
    assert name == "Pervyy_kanal_HD"


def test_m3u_import_assignment_requires_server():
    try:
        M3UImportRequest(content=SAMPLE, placement_mode="assigned")
        assert False, "validation must fail"
    except ValueError:
        pass
    model = M3UImportRequest(content=SAMPLE, placement_mode="assigned", placement_server_id="cdn1")
    assert model.placement_server_id == "cdn1"
