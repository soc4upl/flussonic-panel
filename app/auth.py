from __future__ import annotations

import base64
import hashlib
import hmac
import time
from dataclasses import dataclass

from fastapi import Cookie, HTTPException, status

from .config import get_settings

COOKIE_NAME = "flussonic_panel_session"
SESSION_TTL_SECONDS = 12 * 60 * 60


@dataclass(frozen=True)
class Session:
    username: str
    expires_at: int


def _signature(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def create_session_token(username: str) -> str:
    settings = get_settings()
    expires_at = int(time.time()) + SESSION_TTL_SECONDS
    payload = f"{username}|{expires_at}".encode()
    signed = payload + b"|" + _signature(payload, settings.secret_key).encode()
    return base64.urlsafe_b64encode(signed).decode()


def parse_session_token(token: str) -> Session | None:
    settings = get_settings()
    try:
        decoded = base64.urlsafe_b64decode(token.encode()).decode()
        username, expires_raw, supplied_sig = decoded.rsplit("|", 2)
        payload = f"{username}|{expires_raw}".encode()
        expected_sig = _signature(payload, settings.secret_key)
        if not hmac.compare_digest(supplied_sig, expected_sig):
            return None
        expires_at = int(expires_raw)
        if expires_at <= int(time.time()):
            return None
        return Session(username=username, expires_at=expires_at)
    except (ValueError, UnicodeDecodeError):
        return None


def require_session(
    flussonic_panel_session: str | None = Cookie(default=None, alias=COOKIE_NAME),
) -> Session:
    if not flussonic_panel_session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    session = parse_session_token(flussonic_panel_session)
    if session is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired")
    return session
