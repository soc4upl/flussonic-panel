from __future__ import annotations

from typing import Any
import httpx
from cryptography.fernet import Fernet
import base64, hashlib

from .datastore import DataStore


class NotificationService:
    def __init__(self, store: DataStore, secret_key: str):
        self.store = store
        key = base64.urlsafe_b64encode(hashlib.sha256(secret_key.encode()).digest())
        self.fernet = Fernet(key)

    def public_settings(self) -> dict[str, Any]:
        raw = self.store.get_setting("notifications", {}) or {}
        return {
            "enabled": bool(raw.get("enabled", False)),
            "telegram_chat_id": raw.get("telegram_chat_id", ""),
            "telegram_token_set": bool(raw.get("telegram_bot_token_enc")),
            "webhook_url_set": bool(raw.get("webhook_url_enc")),
            "events": raw.get("events", ["source_failed", "server_offline", "sync_drift", "server_load"]),
        }

    def save_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        old = self.store.get_setting("notifications", {}) or {}
        raw = {
            "enabled": bool(payload.get("enabled", False)),
            "telegram_chat_id": str(payload.get("telegram_chat_id", "")).strip(),
            "events": payload.get("events") or ["source_failed", "server_offline", "sync_drift", "server_load"],
            "telegram_bot_token_enc": old.get("telegram_bot_token_enc"),
            "webhook_url_enc": old.get("webhook_url_enc"),
        }
        if payload.get("telegram_bot_token"):
            raw["telegram_bot_token_enc"] = self.fernet.encrypt(str(payload["telegram_bot_token"]).encode()).decode()
        if payload.get("webhook_url"):
            raw["webhook_url_enc"] = self.fernet.encrypt(str(payload["webhook_url"]).encode()).decode()
        if payload.get("clear_telegram"):
            raw["telegram_bot_token_enc"] = None
        if payload.get("clear_webhook"):
            raw["webhook_url_enc"] = None
        self.store.set_setting("notifications", raw)
        return self.public_settings()

    def _decrypt(self, value: str | None) -> str | None:
        if not value:
            return None
        try:
            return self.fernet.decrypt(value.encode()).decode()
        except Exception:
            return None

    async def send(self, event_type: str, message: str, level: str = "warning", force: bool = False) -> bool:
        raw = self.store.get_setting("notifications", {}) or {}
        if not force and (not raw.get("enabled") or event_type not in (raw.get("events") or [])):
            return False
        delivered = False
        errors: list[str] = []
        token = self._decrypt(raw.get("telegram_bot_token_enc"))
        chat_id = raw.get("telegram_chat_id")
        webhook = self._decrypt(raw.get("webhook_url_enc"))
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            if token and chat_id:
                try:
                    r = await client.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": message, "disable_web_page_preview": True})
                    r.raise_for_status(); delivered = True
                except Exception as exc:
                    errors.append(f"Telegram: {exc}")
            if webhook:
                try:
                    r = await client.post(webhook, json={"event": event_type, "level": level, "message": message})
                    r.raise_for_status(); delivered = True
                except Exception as exc:
                    errors.append(f"Webhook: {exc}")
        self.store.notification(level, event_type, message, delivered, "; ".join(errors) or None)
        return delivered
