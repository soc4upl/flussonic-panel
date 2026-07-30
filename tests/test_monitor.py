from app.config import FlussonicServer
from app.monitor import build_summary, format_duration, parse_sessions


def test_parse_sessions_and_summary():
    server = FlussonicServer(
        id="cdn-1",
        name="CDN 1",
        url="http://localhost:8022",
        username="api",
        password="secret",
    )
    rows = parse_sessions(
        [
            {"name": "news", "user_id": "alice", "ip": "10.0.0.1"},
            {"stream": "sport", "login": "alice", "client_ip": "10.0.0.2"},
            {"channel": "news", "username": "bob", "remote_ip": "10.0.0.1"},
        ],
        server,
    )

    totals, per_server = build_summary(rows)

    assert rows[0]["channel"] == "news"
    assert totals[0]["login"] == "alice"
    assert totals[0]["sessions"] == 2
    assert totals[0]["unique_ips"] == 2
    assert per_server[0]["server"] == "CDN 1"


def test_format_duration():
    assert format_duration(65) == "00:01:05"
    assert format_duration(90061) == "1d 01:01:01"
