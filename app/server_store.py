from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from cryptography.fernet import Fernet, InvalidToken

from .config import FlussonicServer

SERVER_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


class ServerStoreError(RuntimeError):
    pass


class ServerStore:
    """Persistent encrypted storage for Flussonic server credentials."""

    def __init__(self, path: Path, secret_key: str, seed_servers: Iterable[FlussonicServer] = ()):
        self.path = path
        self._lock = threading.RLock()
        digest = hashlib.sha256(secret_key.encode("utf-8")).digest()
        self._fernet = Fernet(base64.urlsafe_b64encode(digest))
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if self.path.exists():
            self._servers = self._load()
        else:
            self._servers = self._normalize(list(seed_servers))
            self._save()

    def _encrypt(self, value: str) -> str:
        return self._fernet.encrypt(value.encode("utf-8")).decode("ascii")

    def _decrypt(self, value: str) -> str:
        try:
            return self._fernet.decrypt(value.encode("ascii")).decode("utf-8")
        except (InvalidToken, ValueError, UnicodeError) as exc:
            raise ServerStoreError(
                "Не удалось расшифровать пароли серверов. Проверьте PANEL_SECRET_KEY: "
                "он должен быть тем же, с которым серверы были сохранены."
            ) from exc

    def _load(self) -> list[FlussonicServer]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ServerStoreError(f"Не удалось прочитать хранилище серверов: {exc}") from exc

        items = raw.get("servers") if isinstance(raw, dict) else None
        if not isinstance(items, list):
            raise ServerStoreError("Файл серверов имеет неверный формат")

        servers: list[FlussonicServer] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            password_token = item.get("password_encrypted")
            if not isinstance(password_token, str):
                raise ServerStoreError("В хранилище отсутствует зашифрованный пароль")
            decoded = dict(item)
            decoded.pop("password_encrypted", None)
            decoded["password"] = self._decrypt(password_token)
            servers.append(FlussonicServer.from_dict(decoded))
        return self._normalize(servers)

    def _save(self) -> None:
        payload = {
            "version": 1,
            "servers": [
                {
                    "id": server.id,
                    "name": server.name,
                    "url": server.url,
                    "username": server.username,
                    "password_encrypted": self._encrypt(server.password),
                    "primary": server.primary,
                    "enabled": server.enabled,
                    "verify_tls": server.verify_tls,
                    "node_exporter_url": server.node_exporter_url,
                }
                for server in self._servers
            ],
        }
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.chmod(temp, 0o600)
            os.replace(temp, self.path)
            os.chmod(self.path, 0o600)
        except OSError as exc:
            raise ServerStoreError(f"Не удалось сохранить серверы: {exc}") from exc

    @staticmethod
    def _normalize(servers: list[FlussonicServer]) -> list[FlussonicServer]:
        seen: set[str] = set()
        normalized: list[FlussonicServer] = []
        for server in servers:
            if server.id in seen:
                raise ServerStoreError(f"Повторяющийся ID сервера: {server.id}")
            seen.add(server.id)
            normalized.append(server)

        enabled = [server for server in normalized if server.enabled]
        primary_id = next((server.id for server in enabled if server.primary), None)
        if enabled and primary_id is None:
            primary_id = enabled[0].id

        return [
            replace(server, primary=bool(server.enabled and server.id == primary_id))
            for server in normalized
        ]

    @staticmethod
    def _new_id() -> str:
        return f"srv-{uuid4().hex[:8]}"

    def all(self) -> tuple[FlussonicServer, ...]:
        with self._lock:
            return tuple(self._servers)

    def enabled(self) -> tuple[FlussonicServer, ...]:
        return tuple(server for server in self.all() if server.enabled)

    def primary(self) -> FlussonicServer | None:
        enabled = self.enabled()
        return next((server for server in enabled if server.primary), enabled[0] if enabled else None)

    def get(self, server_id: str) -> FlussonicServer | None:
        return next((server for server in self.all() if server.id == server_id), None)

    def create(self, data: dict[str, Any]) -> FlussonicServer:
        with self._lock:
            server_id = str(data.get("id") or self._new_id()).strip()
            if not SERVER_ID_RE.fullmatch(server_id):
                raise ServerStoreError("ID сервера может содержать латиницу, цифры, _, - и .")
            if any(server.id == server_id for server in self._servers):
                raise ServerStoreError("Сервер с таким ID уже существует")

            raw = dict(data)
            raw["id"] = server_id
            server = FlussonicServer.from_dict(raw)
            if server.primary:
                self._servers = [replace(item, primary=False) for item in self._servers]
            self._servers.append(server)
            self._servers = self._normalize(self._servers)
            self._save()
            return self.get(server_id)  # type: ignore[return-value]

    def update(self, server_id: str, data: dict[str, Any]) -> FlussonicServer:
        with self._lock:
            existing = self.get(server_id)
            if existing is None:
                raise ServerStoreError("Сервер не найден")

            values = {
                "id": existing.id,
                "name": data.get("name", existing.name),
                "url": data.get("url", existing.url),
                "username": data.get("username", existing.username),
                "password": data.get("password") or existing.password,
                "primary": data.get("primary", existing.primary),
                "enabled": data.get("enabled", existing.enabled),
                "verify_tls": data.get("verify_tls", existing.verify_tls),
                "node_exporter_url": data.get("node_exporter_url", existing.node_exporter_url),
            }
            updated = FlussonicServer.from_dict(values)
            if updated.primary and updated.enabled:
                self._servers = [replace(item, primary=False) for item in self._servers]

            self._servers = [updated if item.id == server_id else item for item in self._servers]
            self._servers = self._normalize(self._servers)
            self._save()
            return self.get(server_id)  # type: ignore[return-value]

    def delete(self, server_id: str) -> None:
        with self._lock:
            if self.get(server_id) is None:
                raise ServerStoreError("Сервер не найден")
            self._servers = [server for server in self._servers if server.id != server_id]
            self._servers = self._normalize(self._servers)
            self._save()

    def set_primary(self, server_id: str) -> FlussonicServer:
        with self._lock:
            server = self.get(server_id)
            if server is None:
                raise ServerStoreError("Сервер не найден")
            if not server.enabled:
                raise ServerStoreError("Сначала включите сервер")
            self._servers = [replace(item, primary=item.id == server_id) for item in self._servers]
            self._save()
            return self.get(server_id)  # type: ignore[return-value]
