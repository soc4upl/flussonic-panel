from __future__ import annotations

import pytest

from app.config import FlussonicServer
from app.datastore import DataStore
from app.stream_status_monitor import StreamStatusMonitor


class FakeServers:
    def __init__(self, servers):
        self._servers = list(servers)

    def enabled(self):
        return list(self._servers)


class FakeClient:
    def __init__(self, payloads):
        self.payloads = payloads

    async def list_streams(self, server):
        value = self.payloads[server.id]
        if isinstance(value, Exception):
            raise value
        return [dict(item) for item in value]


class FakeNotifications:
    def __init__(self, confirmations=2):
        self.confirmations = confirmations
        self.sent = []

    def status_monitor_settings(self):
        return {"active": True, "poll_seconds": 30, "confirmations": self.confirmations}

    async def send(self, event_type, message, level="warning", force=False):
        self.sent.append((event_type, message, level))
        return True


def stream(name, status, *, disabled=False, title=""):
    return {
        "name": name,
        "title": title,
        "status": status,
        "disabled": disabled,
        "static": False,
    }


@pytest.mark.asyncio
async def test_stream_error_alert_is_deduplicated_and_recovery_is_sent(tmp_path):
    server = FlussonicServer(id="cdn1", name="CDN 1", url="http://cdn1", username="u", password="p")
    store = DataStore(tmp_path / "panel.db")
    client = FakeClient({"cdn1": [stream("NTV", "error", title="НТВ HD")]})
    notifications = FakeNotifications(confirmations=2)
    monitor = StreamStatusMonitor(FakeServers([server]), client, store, notifications)

    first = await monitor.refresh_now()
    assert first["bad"] == 1
    assert notifications.sent == []

    second = await monitor.refresh_now()
    assert second["bad"] == 1
    assert [item[0] for item in notifications.sent] == ["stream_error"]
    assert "CDN 1" in notifications.sent[0][1]
    assert "NTV" in notifications.sent[0][1]

    await monitor.refresh_now()
    assert [item[0] for item in notifications.sent] == ["stream_error"]

    client.payloads["cdn1"] = [stream("NTV", "running", title="НТВ HD")]
    await monitor.refresh_now()
    assert [item[0] for item in notifications.sent] == ["stream_error", "stream_recovered"]


@pytest.mark.asyncio
async def test_waiting_disabled_and_non_target_assigned_do_not_alert(tmp_path):
    cdn1 = FlussonicServer(id="cdn1", name="CDN 1", url="http://cdn1", username="u", password="p")
    cdn2 = FlussonicServer(id="cdn2", name="CDN 2", url="http://cdn2", username="u", password="p")
    store = DataStore(tmp_path / "panel.db")
    store.set_setting("placement_enabled", True)
    store.set_placement(stream_name="NTV", mode="assigned", primary_server_id="cdn1", actor="test")
    client = FakeClient({
        "cdn1": [stream("NTV", "waiting"), stream("OFF", "error", disabled=True)],
        "cdn2": [stream("NTV", "error")],
    })
    notifications = FakeNotifications(confirmations=1)
    monitor = StreamStatusMonitor(FakeServers([cdn1, cdn2]), client, store, notifications)

    result = await monitor.refresh_now()
    assert result["bad"] == 0
    assert notifications.sent == []


@pytest.mark.asyncio
async def test_mirror_mode_alerts_per_server(tmp_path):
    cdn1 = FlussonicServer(id="cdn1", name="CDN 1", url="http://cdn1", username="u", password="p")
    cdn2 = FlussonicServer(id="cdn2", name="CDN 2", url="http://cdn2", username="u", password="p")
    store = DataStore(tmp_path / "panel.db")
    store.set_setting("placement_enabled", False)
    client = FakeClient({"cdn1": [stream("NTV", "error")], "cdn2": [stream("NTV", "running")]})
    notifications = FakeNotifications(confirmations=1)
    monitor = StreamStatusMonitor(FakeServers([cdn1, cdn2]), client, store, notifications)

    result = await monitor.refresh_now()
    assert result["bad"] == 1
    assert len(notifications.sent) == 1
    assert notifications.sent[0][0] == "stream_error"
    assert "CDN 1" in notifications.sent[0][1]
