from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_sources_view_has_server_selector():
    html = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="sources-server-filter"' in html
    assert 'ВСЕ серверы' in html


def test_sources_manual_run_scopes_to_selected_server():
    js = (ROOT / "app/static/app.js").read_text(encoding="utf-8")
    assert "sourceServerId: 'all'" in js
    assert "query.set('server_id', state.sourceServerId)" in js
    assert "selectedServerId === 'all' || item.server_id === selectedServerId" in js
