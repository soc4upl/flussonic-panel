from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


class DataStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.RLock()
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init(self) -> None:
        with self._lock, self._connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS backups (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts INTEGER NOT NULL,
              actor TEXT NOT NULL,
              action TEXT NOT NULL,
              stream_name TEXT NOT NULL,
              server_id TEXT NOT NULL,
              server_name TEXT NOT NULL,
              config_hash TEXT,
              config_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_backups_stream ON backups(stream_name, ts DESC);
            CREATE TABLE IF NOT EXISTS audit_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts INTEGER NOT NULL,
              actor TEXT NOT NULL,
              action TEXT NOT NULL,
              entity_type TEXT NOT NULL,
              entity_id TEXT,
              server_id TEXT,
              status TEXT NOT NULL,
              summary TEXT NOT NULL,
              details_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC);
            CREATE TABLE IF NOT EXISTS monitor_points (
              ts INTEGER PRIMARY KEY,
              sessions INTEGER NOT NULL,
              logins INTEGER NOT NULL,
              unique_ips INTEGER NOT NULL,
              active_streams INTEGER NOT NULL,
              server_counts_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS server_load_points (
              ts INTEGER NOT NULL,
              server_id TEXT NOT NULL,
              server_name TEXT NOT NULL,
              online INTEGER NOT NULL,
              state TEXT NOT NULL,
              cpu_percent REAL,
              memory_percent REAL,
              disk_percent REAL,
              network_rx_bps REAL,
              network_tx_bps REAL,
              process_memory_bytes REAL,
              sessions INTEGER NOT NULL DEFAULT 0,
              details_json TEXT NOT NULL DEFAULT '{}',
              PRIMARY KEY(ts, server_id)
            );
            CREATE INDEX IF NOT EXISTS idx_server_load_ts ON server_load_points(ts DESC);
            CREATE TABLE IF NOT EXISTS source_checks (
              server_id TEXT NOT NULL,
              server_name TEXT NOT NULL,
              stream_name TEXT NOT NULL,
              input_url TEXT NOT NULL,
              checked_at INTEGER NOT NULL,
              state TEXT NOT NULL,
              status_code INTEGER,
              latency_ms INTEGER,
              detail TEXT,
              PRIMARY KEY(server_id, stream_name, input_url)
            );
            CREATE TABLE IF NOT EXISTS settings (
              key TEXT PRIMARY KEY,
              value_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notifications (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts INTEGER NOT NULL,
              level TEXT NOT NULL,
              event_type TEXT NOT NULL,
              message TEXT NOT NULL,
              delivered INTEGER NOT NULL DEFAULT 0,
              error TEXT
            );
            CREATE TABLE IF NOT EXISTS stream_placements (
              stream_name TEXT PRIMARY KEY,
              mode TEXT NOT NULL CHECK(mode IN ('mirror','assigned')),
              primary_server_id TEXT,
              updated_at INTEGER NOT NULL,
              updated_by TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_stream_placements_server ON stream_placements(primary_server_id);
            """)

    def backup(self, *, actor: str, action: str, stream_name: str, server_id: str, server_name: str, config: dict[str, Any], config_hash: str) -> int:
        with self._lock, self._connect() as db:
            cur = db.execute(
                "INSERT INTO backups(ts,actor,action,stream_name,server_id,server_name,config_hash,config_json) VALUES(?,?,?,?,?,?,?,?)",
                (int(time.time()), actor, action, stream_name, server_id, server_name, config_hash, json.dumps(config, ensure_ascii=False, separators=(",", ":"))),
            )
            return int(cur.lastrowid)

    def backups(self, stream_name: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        sql = "SELECT * FROM backups"
        args: list[Any] = []
        if stream_name:
            sql += " WHERE stream_name=?"
            args.append(stream_name)
        sql += " ORDER BY ts DESC, id DESC LIMIT ?"
        args.append(limit)
        with self._lock, self._connect() as db:
            rows = db.execute(sql, args).fetchall()
        return [dict(row) | {"config": json.loads(row["config_json"])} for row in rows]

    def backup_by_id(self, backup_id: int) -> dict[str, Any] | None:
        with self._lock, self._connect() as db:
            row = db.execute("SELECT * FROM backups WHERE id=?", (backup_id,)).fetchone()
        return None if row is None else dict(row) | {"config": json.loads(row["config_json"])}

    def audit(self, *, actor: str, action: str, entity_type: str, entity_id: str | None, server_id: str | None, status: str, summary: str, details: dict[str, Any] | None = None) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                "INSERT INTO audit_log(ts,actor,action,entity_type,entity_id,server_id,status,summary,details_json) VALUES(?,?,?,?,?,?,?,?,?)",
                (int(time.time()), actor, action, entity_type, entity_id, server_id, status, summary, json.dumps(details or {}, ensure_ascii=False, separators=(",", ":"))),
            )

    def audit_items(self, limit: int = 300, action: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM audit_log"
        args: list[Any] = []
        if action:
            sql += " WHERE action=?"
            args.append(action)
        sql += " ORDER BY ts DESC, id DESC LIMIT ?"
        args.append(limit)
        with self._lock, self._connect() as db:
            rows = db.execute(sql, args).fetchall()
        return [dict(row) | {"details": json.loads(row["details_json"])} for row in rows]

    def save_monitor_point(self, payload: dict[str, Any], min_gap: int = 60) -> None:
        ts = int(payload.get("ts") or time.time())
        ts -= ts % min_gap
        metrics = payload.get("metrics") or {}
        with self._lock, self._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO monitor_points(ts,sessions,logins,unique_ips,active_streams,server_counts_json) VALUES(?,?,?,?,?,?)",
                (ts, int(metrics.get("sessions", 0)), int(metrics.get("logins", 0)), int(metrics.get("unique_ips", 0)), int(metrics.get("active_streams", 0)), json.dumps(payload.get("server_counts") or [], ensure_ascii=False, separators=(",", ":"))),
            )

    def monitor_history(self, since_ts: int) -> list[dict[str, Any]]:
        with self._lock, self._connect() as db:
            rows = db.execute("SELECT * FROM monitor_points WHERE ts>=? ORDER BY ts", (since_ts,)).fetchall()
        return [dict(row) | {"server_counts": json.loads(row["server_counts_json"])} for row in rows]

    def prune_monitor(self, before_ts: int) -> None:
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM monitor_points WHERE ts<?", (before_ts,))

    def save_server_load(self, payload: dict[str, Any], min_gap: int = 60) -> None:
        ts = int(payload.get("ts") or time.time())
        ts -= ts % max(1, min_gap)
        with self._lock, self._connect() as db:
            for item in payload.get("servers") or []:
                db.execute(
                    """
                    INSERT OR REPLACE INTO server_load_points(
                      ts,server_id,server_name,online,state,cpu_percent,memory_percent,disk_percent,
                      network_rx_bps,network_tx_bps,process_memory_bytes,sessions,details_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        ts, str(item.get("server_id") or ""), str(item.get("server") or ""),
                        int(bool(item.get("online"))), str(item.get("state") or "unknown"),
                        item.get("cpu_percent"), item.get("memory_percent"), item.get("disk_percent"),
                        item.get("network_rx_bps"), item.get("network_tx_bps"), item.get("process_memory_bytes"),
                        int(item.get("sessions") or 0),
                        json.dumps(item, ensure_ascii=False, separators=(",", ":")),
                    ),
                )

    def server_load_history(self, since_ts: int, server_id: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM server_load_points WHERE ts>=?"
        args: list[Any] = [since_ts]
        if server_id:
            sql += " AND server_id=?"
            args.append(server_id)
        sql += " ORDER BY ts, server_name"
        with self._lock, self._connect() as db:
            rows = db.execute(sql, args).fetchall()
        return [dict(row) | {"details": json.loads(row["details_json"])} for row in rows]

    def prune_server_load(self, before_ts: int) -> None:
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM server_load_points WHERE ts<?", (before_ts,))

    def upsert_source_check(self, item: dict[str, Any]) -> dict[str, Any] | None:
        key = (item["server_id"], item["stream_name"], item["input_url"])
        with self._lock, self._connect() as db:
            old = db.execute("SELECT * FROM source_checks WHERE server_id=? AND stream_name=? AND input_url=?", key).fetchone()
            db.execute("""
              INSERT INTO source_checks(server_id,server_name,stream_name,input_url,checked_at,state,status_code,latency_ms,detail)
              VALUES(?,?,?,?,?,?,?,?,?)
              ON CONFLICT(server_id,stream_name,input_url) DO UPDATE SET
                server_name=excluded.server_name, checked_at=excluded.checked_at, state=excluded.state,
                status_code=excluded.status_code, latency_ms=excluded.latency_ms, detail=excluded.detail
            """, (item["server_id"], item["server_name"], item["stream_name"], item["input_url"], item["checked_at"], item["state"], item.get("status_code"), item.get("latency_ms"), item.get("detail")))
        return dict(old) if old else None

    def reconcile_source_checks(self, *, server_id: str, current_sources: dict[str, set[str]], stream_name: str | None = None) -> int:
        """Delete cached checks that no longer exist in the current Flussonic config.

        Reconciliation is deliberately scoped to a server whose configuration was
        fetched successfully. This prevents a temporary API/network failure from
        erasing health-check history.
        """
        sql = "SELECT stream_name,input_url FROM source_checks WHERE server_id=?"
        args: list[Any] = [server_id]
        if stream_name is not None:
            sql += " AND stream_name=?"
            args.append(stream_name)
        with self._lock, self._connect() as db:
            rows = db.execute(sql, args).fetchall()
            stale = [
                (server_id, str(row["stream_name"]), str(row["input_url"]))
                for row in rows
                if str(row["input_url"]) not in current_sources.get(str(row["stream_name"]), set())
            ]
            if stale:
                db.executemany(
                    "DELETE FROM source_checks WHERE server_id=? AND stream_name=? AND input_url=?",
                    stale,
                )
        return len(stale)

    def mark_source_checks(self, *, server_id: str, stream_name: str, state: str, detail: str) -> None:
        with self._lock, self._connect() as db:
            db.execute("UPDATE source_checks SET checked_at=?, state=?, status_code=NULL, latency_ms=NULL, detail=? WHERE server_id=? AND stream_name=?", (int(time.time()), state, detail, server_id, stream_name))

    def delete_source_checks(self, *, server_id: str, stream_name: str, input_url: str | None = None) -> None:
        with self._lock, self._connect() as db:
            if input_url is None:
                db.execute("DELETE FROM source_checks WHERE server_id=? AND stream_name=?", (server_id, stream_name))
            else:
                db.execute("DELETE FROM source_checks WHERE server_id=? AND stream_name=? AND input_url=?", (server_id, stream_name, input_url))

    def source_checks(self, limit: int = 2000, stream_name: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM source_checks"
        args: list[Any] = []
        if stream_name:
            sql += " WHERE stream_name=?"
            args.append(stream_name)
        sql += " ORDER BY checked_at DESC LIMIT ?"
        args.append(limit)
        with self._lock, self._connect() as db:
            return [dict(row) for row in db.execute(sql, args).fetchall()]

    def set_setting(self, key: str, value: Any) -> None:
        with self._lock, self._connect() as db:
            db.execute("INSERT INTO settings(key,value_json) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json", (key, json.dumps(value, ensure_ascii=False)))

    def get_setting(self, key: str, default: Any = None) -> Any:
        with self._lock, self._connect() as db:
            row = db.execute("SELECT value_json FROM settings WHERE key=?", (key,)).fetchone()
        return default if row is None else json.loads(row["value_json"])


    def placement(self, stream_name: str) -> dict[str, Any] | None:
        with self._lock, self._connect() as db:
            row = db.execute("SELECT * FROM stream_placements WHERE stream_name=?", (stream_name,)).fetchone()
        return dict(row) if row else None

    def placements(self) -> dict[str, dict[str, Any]]:
        with self._lock, self._connect() as db:
            rows = db.execute("SELECT * FROM stream_placements ORDER BY stream_name").fetchall()
        return {str(row["stream_name"]): dict(row) for row in rows}

    def set_placement(self, *, stream_name: str, mode: str, primary_server_id: str | None, actor: str) -> dict[str, Any]:
        if mode not in {"mirror", "assigned"}:
            raise ValueError("Unsupported placement mode")
        if mode == "assigned" and not primary_server_id:
            raise ValueError("Assigned placement requires a server")
        server_id = primary_server_id if mode == "assigned" else None
        now = int(time.time())
        with self._lock, self._connect() as db:
            db.execute(
                """INSERT INTO stream_placements(stream_name,mode,primary_server_id,updated_at,updated_by)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(stream_name) DO UPDATE SET
                     mode=excluded.mode, primary_server_id=excluded.primary_server_id,
                     updated_at=excluded.updated_at, updated_by=excluded.updated_by""",
                (stream_name, mode, server_id, now, actor),
            )
        return {"stream_name": stream_name, "mode": mode, "primary_server_id": server_id, "updated_at": now, "updated_by": actor}

    def set_placements(self, *, stream_names: list[str], mode: str, primary_server_id: str | None, actor: str) -> list[dict[str, Any]]:
        return [self.set_placement(stream_name=name, mode=mode, primary_server_id=primary_server_id, actor=actor) for name in stream_names]

    def delete_placement(self, stream_name: str) -> None:
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM stream_placements WHERE stream_name=?", (stream_name,))

    def notification(self, level: str, event_type: str, message: str, delivered: bool, error: str | None = None) -> None:
        with self._lock, self._connect() as db:
            db.execute("INSERT INTO notifications(ts,level,event_type,message,delivered,error) VALUES(?,?,?,?,?,?)", (int(time.time()), level, event_type, message, int(delivered), error))

    def notification_items(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock, self._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM notifications ORDER BY ts DESC,id DESC LIMIT ?", (limit,)).fetchall()]
