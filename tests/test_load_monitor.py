from types import SimpleNamespace

from app.config import FlussonicServer
from app.load_monitor import ServerLoadMonitor, parse_prometheus


def settings():
    return SimpleNamespace(
        server_load_enabled=True,
        server_load_poll_seconds=30,
        server_network_poll_seconds=1.0,
        server_load_cpu_warning=85.0,
        server_load_memory_warning=85.0,
        server_load_disk_warning=90.0,
        server_load_history_seconds=60,
        monitor_retention_days=30,
    )


def monitor():
    return ServerLoadMonitor(settings(), None, None, None, None, None)


def test_parse_prometheus_samples_and_labels():
    samples = parse_prometheus('node_cpu_seconds_total{cpu="0",mode="idle"} 12.5\nprocess_resident_memory_bytes 1024\n')
    assert len(samples) == 2
    assert samples[0].labels == {"cpu": "0", "mode": "idle"}
    assert samples[1].value == 1024


def test_extract_memory_disk_and_direct_cpu():
    text = '''
server_cpu_usage_percent 42
node_memory_MemTotal_bytes 1000
node_memory_MemAvailable_bytes 250
node_filesystem_size_bytes{mountpoint="/",fstype="ext4",device="/dev/sda1"} 2000
node_filesystem_avail_bytes{mountpoint="/",fstype="ext4",device="/dev/sda1"} 500
process_resident_memory_bytes 300
node_load1 1.25
'''
    server = FlussonicServer("one", "One", "http://one", "u", "p")
    item = monitor()._from_metrics(server, parse_prometheus(text), 100.0, "/runtime/metrics")
    assert item["cpu_percent"] == 42.0
    assert item["memory_percent"] == 75.0
    assert item["disk_percent"] == 75.0
    assert item["process_memory_bytes"] == 300
    assert item["state"] == "ok"


def test_cpu_and_network_are_calculated_from_counter_deltas():
    server = FlussonicServer("one", "One", "http://one", "u", "p")
    mon = monitor()
    first = '''
node_cpu_seconds_total{cpu="0",mode="idle"} 80
node_cpu_seconds_total{cpu="0",mode="user"} 20
node_network_receive_bytes_total{device="eth0"} 1000
node_network_transmit_bytes_total{device="eth0"} 2000
'''
    second = '''
node_cpu_seconds_total{cpu="0",mode="idle"} 85
node_cpu_seconds_total{cpu="0",mode="user"} 25
node_network_receive_bytes_total{device="eth0"} 2000
node_network_transmit_bytes_total{device="eth0"} 4000
'''
    mon._from_metrics(server, parse_prometheus(first), 100.0, "/runtime/metrics")
    item = mon._from_metrics(server, parse_prometheus(second), 110.0, "/runtime/metrics")
    assert item["cpu_percent"] == 50.0
    assert item["network_rx_bps"] == 800.0
    assert item["network_tx_bps"] == 1600.0


def test_node_exporter_url_is_inferred_and_can_be_overridden():
    from app.load_monitor import effective_node_exporter_url

    inferred = FlussonicServer("one", "One", "http://cdn-2.example.com:8022", "u", "p")
    explicit = FlussonicServer(
        "two", "Two", "http://cdn-2.example.com:8022", "u", "p",
        node_exporter_url="http://metrics.example.com:9200",
    )
    assert effective_node_exporter_url(inferred) == "http://cdn-2.example.com:9100/metrics"
    assert effective_node_exporter_url(explicit) == "http://metrics.example.com:9200/metrics"


def test_network_interfaces_are_calculated_individually():
    server = FlussonicServer("one", "One", "http://one", "u", "p")
    mon = monitor()
    first = '''
node_network_receive_bytes_total{device="eth0"} 1000
node_network_transmit_bytes_total{device="eth0"} 2000
node_network_receive_bytes_total{device="eth1"} 500
node_network_transmit_bytes_total{device="eth1"} 700
node_network_receive_bytes_total{device="lo"} 999999
node_network_transmit_bytes_total{device="lo"} 999999
node_network_up{device="eth0"} 1
node_network_up{device="eth1"} 0
'''
    second = '''
node_network_receive_bytes_total{device="eth0"} 3000
node_network_transmit_bytes_total{device="eth0"} 5000
node_network_receive_bytes_total{device="eth1"} 1500
node_network_transmit_bytes_total{device="eth1"} 1700
node_network_up{device="eth0"} 1
node_network_up{device="eth1"} 0
'''
    mon._from_metrics(server, parse_prometheus(first), 100.0, "node", source="node_exporter")
    item = mon._from_metrics(server, parse_prometheus(second), 110.0, "node", source="node_exporter")
    interfaces = {row["device"]: row for row in item["network_interfaces"]}
    assert set(interfaces) == {"eth0", "eth1"}
    assert interfaces["eth0"]["rx_bps"] == 1600.0
    assert interfaces["eth0"]["tx_bps"] == 2400.0
    assert interfaces["eth1"]["up"] is False
    assert item["network_rx_bps"] == 1600.0
    assert item["network_tx_bps"] == 2400.0


def test_network_only_exporter_url_adds_netdev_collector():
    from app.load_monitor import network_only_exporter_url

    assert network_only_exporter_url("http://host:9100/metrics") == (
        "http://host:9100/metrics?collect%5B%5D=netdev"
    )


def test_realtime_network_rates_use_one_second_deltas():
    server = FlussonicServer("one", "One", "http://one", "u", "p")
    mon = monitor()
    first = parse_prometheus(
        '''
node_network_receive_bytes_total{device="eth0"} 1000
node_network_transmit_bytes_total{device="eth0"} 2000
node_network_up{device="eth0"} 1
'''
    )
    second = parse_prometheus(
        '''
node_network_receive_bytes_total{device="eth0"} 2000
node_network_transmit_bytes_total{device="eth0"} 4000
node_network_up{device="eth0"} 1
'''
    )
    mon._network_rates(server, first, 100.0)
    item = mon._network_rates(server, second, 101.0)
    assert item["network_rx_bps"] == 8000.0
    assert item["network_tx_bps"] == 16000.0
    assert item["network_interfaces"][0]["rx_bps"] == 8000.0
