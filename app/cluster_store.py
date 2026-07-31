from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken


class ClusterStoreError(RuntimeError):
    pass


class ClusterStore:
    """Encrypted persistent cluster configuration."""

    def __init__(self, path: Path, secret_key: str):
        self.path = path
        self._lock = threading.RLock()
        digest = hashlib.sha256(secret_key.encode("utf-8")).digest()
        self._fernet = Fernet(base64.urlsafe_b64encode(digest))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._save_raw(self._default())

    @staticmethod
    def _default() -> dict[str, Any]:
        return {
            "version": 1,
            "enabled": False,
            "balancer_server_id": None,
            "balancer_name": "lb01",
            "mode": "clients",
            "peers": [],
            "cluster_key_encrypted": None,
        }

    def _encrypt(self, value: str) -> str:
        return self._fernet.encrypt(value.encode("utf-8")).decode("ascii")

    def _decrypt(self, value: str | None) -> str:
        if not value:
            return ""
        try:
            return self._fernet.decrypt(value.encode("ascii")).decode("utf-8")
        except (InvalidToken, ValueError, UnicodeError) as exc:
            raise ClusterStoreError("Не удалось расшифровать cluster_key. Проверьте PANEL_SECRET_KEY.") from exc

    def _load_raw(self) -> dict[str, Any]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ClusterStoreError(f"Не удалось прочитать настройки Cluster: {exc}") from exc
        if not isinstance(data, dict):
            raise ClusterStoreError("Неверный формат настроек Cluster")
        return {**self._default(), **data}

    def _save_raw(self, data: dict[str, Any]) -> None:
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.chmod(temp, 0o600)
        os.replace(temp, self.path)
        os.chmod(self.path, 0o600)

    def get(self, *, include_secret: bool = False) -> dict[str, Any]:
        with self._lock:
            raw = self._load_raw()
            secret = self._decrypt(raw.get("cluster_key_encrypted"))
            result = {
                "enabled": bool(raw.get("enabled")),
                "balancer_server_id": raw.get("balancer_server_id"),
                "balancer_name": str(raw.get("balancer_name") or "lb01"),
                "mode": str(raw.get("mode") or "clients"),
                "peers": list(raw.get("peers") or []),
                "has_cluster_key": bool(secret),
            }
            if include_secret:
                result["cluster_key"] = secret
            return result

    def update(self, data: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            raw = self._load_raw()
            cluster_key = data.pop("cluster_key", None)
            raw.update(data)
            if cluster_key is not None and str(cluster_key).strip():
                raw["cluster_key_encrypted"] = self._encrypt(str(cluster_key).strip())
            self._save_raw(raw)
            return self.get()
