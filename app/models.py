from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

STREAM_NAME_RE = re.compile(r"^[A-Za-z0-9_./-]+$")


class LoginRequest(BaseModel):
    username: str
    password: str


class InputItem(BaseModel):
    url: str = Field(min_length=3, max_length=4096)

    @field_validator("url")
    @classmethod
    def clean_url(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Input URL cannot be empty")
        if any(char in value for char in ("\n", "\r", "\x00")):
            raise ValueError("Input URL contains forbidden characters")
        return value


class TargetedRequest(BaseModel):
    target_ids: list[str] | None = None


class InputsUpdate(TargetedRequest):
    inputs: list[InputItem] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def unique_inputs(self) -> "InputsUpdate":
        urls = [item.url for item in self.inputs]
        if len(urls) != len(set(urls)):
            raise ValueError("Duplicate input URLs are not allowed")
        return self


class StreamCreate(TargetedRequest):
    placement_mode: str | None = None
    placement_server_id: str | None = Field(default=None, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    title: str = Field(default="", max_length=500)
    provider: str = Field(default="CYRIUSTV", max_length=255)
    on_play: str | None = Field(default="auth://NewAuthBackend1", max_length=2048)
    static: bool = False
    position: int | None = Field(default=None, ge=0)
    inputs: list[InputItem] = Field(min_length=1, max_length=50)

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        value = value.strip()
        if not STREAM_NAME_RE.fullmatch(value):
            raise ValueError("Use only letters, digits, _, -, / and . in stream name")
        return value

    @model_validator(mode="after")
    def unique_inputs(self) -> "StreamCreate":
        urls = [item.url for item in self.inputs]
        if len(urls) != len(set(urls)):
            raise ValueError("Duplicate input URLs are not allowed")
        if self.placement_mode not in {None, "mirror", "assigned"}:
            raise ValueError("placement_mode must be mirror or assigned")
        if self.placement_mode == "assigned" and not self.placement_server_id:
            raise ValueError("placement_server_id is required for assigned mode")
        return self


class StreamPatch(TargetedRequest):
    placement_mode: str | None = None
    placement_server_id: str | None = Field(default=None, max_length=64)
    title: str | None = Field(default=None, max_length=500)
    provider: str | None = Field(default=None, max_length=255)
    on_play: str | None = Field(default=None, max_length=2048)
    static: bool | None = None
    position: int | None = Field(default=None, ge=0)
    inputs: list[InputItem] | None = Field(default=None, min_length=1, max_length=50)

    @model_validator(mode="after")
    def unique_inputs(self) -> "StreamPatch":
        if self.inputs is not None:
            urls = [item.url for item in self.inputs]
            if len(urls) != len(set(urls)):
                raise ValueError("Duplicate input URLs are not allowed")
        if self.placement_mode not in {None, "mirror", "assigned"}:
            raise ValueError("placement_mode must be mirror or assigned")
        if self.placement_mode == "assigned" and not self.placement_server_id:
            raise ValueError("placement_server_id is required for assigned mode")
        return self


class ReorderItem(BaseModel):
    name: str
    position: int = Field(ge=0)


class ReorderRequest(TargetedRequest):
    streams: list[ReorderItem] = Field(min_length=1, max_length=10000)


class SyncRequest(TargetedRequest):
    source_id: str | None = None


class ApiResult(BaseModel):
    ok: bool
    data: Any | None = None
    error: str | None = None


class ServerCreate(BaseModel):
    id: str | None = Field(default=None, max_length=64)
    name: str = Field(min_length=1, max_length=120)
    url: str = Field(min_length=8, max_length=2048)
    username: str = Field(min_length=1, max_length=255)
    password: str = Field(min_length=1, max_length=1024)
    primary: bool = False
    enabled: bool = True
    verify_tls: bool = True
    node_exporter_url: str | None = Field(default=None, max_length=2048)

    @field_validator("id")
    @classmethod
    def clean_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if value and not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
            raise ValueError("Server ID may contain letters, digits, _, - and .")
        return value or None

    @field_validator("node_exporter_url")
    @classmethod
    def clean_node_exporter_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().rstrip("/")
        if not value:
            return None
        if not re.match(r"^https?://", value, flags=re.IGNORECASE):
            raise ValueError("Node Exporter URL must start with http:// or https://")
        if any(char in value for char in ("\n", "\r", "\x00")):
            raise ValueError("Node Exporter URL contains invalid characters")
        return value

    @field_validator("name", "username", "password")
    @classmethod
    def clean_server_text(cls, value: str) -> str:
        value = value.strip()
        if not value or any(char in value for char in ("\n", "\r", "\x00")):
            raise ValueError("Field contains invalid characters")
        return value

    @field_validator("url")
    @classmethod
    def clean_server_url(cls, value: str) -> str:
        value = value.strip().rstrip("/")
        if not re.match(r"^https?://", value, flags=re.IGNORECASE):
            raise ValueError("URL must start with http:// or https://")
        if any(char in value for char in ("\n", "\r", "\x00")):
            raise ValueError("URL contains invalid characters")
        return value


class ServerUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    url: str | None = Field(default=None, min_length=8, max_length=2048)
    username: str | None = Field(default=None, min_length=1, max_length=255)
    password: str | None = Field(default=None, max_length=1024)
    primary: bool | None = None
    enabled: bool | None = None
    verify_tls: bool | None = None
    node_exporter_url: str | None = Field(default=None, max_length=2048)

    @field_validator("node_exporter_url")
    @classmethod
    def clean_optional_node_exporter_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().rstrip("/")
        if not value:
            return ""
        if not re.match(r"^https?://", value, flags=re.IGNORECASE):
            raise ValueError("Node Exporter URL must start with http:// or https://")
        if any(char in value for char in ("\n", "\r", "\x00")):
            raise ValueError("Node Exporter URL contains invalid characters")
        return value

    @field_validator("name", "username")
    @classmethod
    def clean_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or any(char in value for char in ("\n", "\r", "\x00")):
            raise ValueError("Field contains invalid characters")
        return value

    @field_validator("password")
    @classmethod
    def clean_optional_password(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if any(char in value for char in ("\n", "\r", "\x00")):
            raise ValueError("Password contains invalid characters")
        return value or None

    @field_validator("url")
    @classmethod
    def clean_optional_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().rstrip("/")
        if not re.match(r"^https?://", value, flags=re.IGNORECASE):
            raise ValueError("URL must start with http:// or https://")
        return value


class ServerTestRequest(BaseModel):
    url: str = Field(min_length=8, max_length=2048)
    username: str = Field(min_length=1, max_length=255)
    password: str = Field(min_length=1, max_length=1024)
    verify_tls: bool = True
    node_exporter_url: str | None = Field(default=None, max_length=2048)

    @field_validator("node_exporter_url")
    @classmethod
    def clean_test_node_exporter_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().rstrip("/")
        if not value:
            return None
        if not re.match(r"^https?://", value, flags=re.IGNORECASE):
            raise ValueError("Node Exporter URL must start with http:// or https://")
        return value

    @field_validator("url")
    @classmethod
    def clean_test_url(cls, value: str) -> str:
        value = value.strip().rstrip("/")
        if not re.match(r"^https?://", value, flags=re.IGNORECASE):
            raise ValueError("URL must start with http:// or https://")
        return value

class BulkOperationRequest(TargetedRequest):
    names: list[str] = Field(min_length=1, max_length=500)
    operation: str
    value: str | bool | None = None
    source_id: str | None = None

    @field_validator("operation")
    @classmethod
    def valid_operation(cls, value: str) -> str:
        allowed = {"add_input", "set_provider", "set_on_play", "set_static", "delete", "sync"}
        if value not in allowed:
            raise ValueError("Unsupported bulk operation")
        return value


class ChangePreviewRequest(TargetedRequest):
    names: list[str] = Field(min_length=1, max_length=500)
    operation: str
    value: str | bool | None = None
    source_id: str | None = None

    @field_validator("operation")
    @classmethod
    def valid_change_operation(cls, value: str) -> str:
        allowed = {"add_input", "set_provider", "set_on_play", "set_static", "set_disabled", "delete", "sync"}
        if value not in allowed:
            raise ValueError("Unsupported change operation")
        return value


class StreamModeBulkRequest(BaseModel):
    names: list[str] = Field(min_length=1, max_length=500)
    static: bool


class StreamStateBulkRequest(BaseModel):
    names: list[str] = Field(min_length=1, max_length=500)
    disabled: bool


class M3UImportRequest(BaseModel):
    content: str = Field(min_length=1, max_length=2_000_000)
    placement_mode: str = "mirror"
    placement_server_id: str | None = Field(default=None, max_length=64)
    provider: str = Field(default="CYRIUSTV", max_length=255)
    on_play: str | None = Field(default="auth://NewAuthBackend1", max_length=2048)
    static: bool = False
    overwrite_existing: bool = False

    @model_validator(mode="after")
    def validate_import(self) -> "M3UImportRequest":
        if self.placement_mode not in {"mirror", "assigned"}:
            raise ValueError("placement_mode must be mirror or assigned")
        if self.placement_mode == "assigned" and not self.placement_server_id:
            raise ValueError("placement_server_id is required for assigned mode")
        return self


class SourceActionRequest(BaseModel):
    server_id: str = Field(min_length=1, max_length=64)
    stream_name: str = Field(min_length=1, max_length=255)
    input_url: str | None = Field(default=None, max_length=4096)
    action: str
    new_input: str | None = Field(default=None, max_length=4096)

    @field_validator("action")
    @classmethod
    def valid_source_action(cls, value: str) -> str:
        allowed = {"disable_stream", "enable_stream", "add_input", "remove_input", "promote_input"}
        if value not in allowed:
            raise ValueError("Unsupported source action")
        return value

    @field_validator("input_url", "new_input")
    @classmethod
    def clean_source_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or any(char in value for char in ("\n", "\r", "\x00")):
            raise ValueError("Input URL contains invalid characters")
        return value

    @model_validator(mode="after")
    def validate_action_fields(self) -> "SourceActionRequest":
        if self.action in {"remove_input", "promote_input"} and not self.input_url:
            raise ValueError("input_url is required")
        if self.action == "add_input" and not self.new_input:
            raise ValueError("new_input is required")
        return self


class NotificationSettingsUpdate(BaseModel):
    enabled: bool = False
    telegram_chat_id: str = ""
    telegram_bot_token: str | None = None
    webhook_url: str | None = None
    clear_telegram: bool = False
    clear_webhook: bool = False
    events: list[str] = Field(default_factory=lambda: ["source_failed", "server_offline", "sync_drift", "server_load"])


class NotificationTestRequest(BaseModel):
    message: str = "Тестовое уведомление Cyrius Stream Control"

class PlacementSettingsUpdate(BaseModel):
    enabled: bool = False


class PlacementAssignmentUpdate(BaseModel):
    mode: str
    server_id: str | None = Field(default=None, max_length=64)

    @model_validator(mode="after")
    def validate_assignment(self) -> "PlacementAssignmentUpdate":
        if self.mode not in {"mirror", "assigned"}:
            raise ValueError("mode must be mirror or assigned")
        if self.mode == "assigned" and not self.server_id:
            raise ValueError("server_id is required for assigned mode")
        return self


class PlacementBulkUpdate(PlacementAssignmentUpdate):
    names: list[str] = Field(min_length=1, max_length=2000)


class PlacementApplyRequest(BaseModel):
    names: list[str] = Field(min_length=1, max_length=2000)
    remove_extras: bool = False


class PlacementPlanRequest(BaseModel):
    batch_size: int = Field(default=100, ge=1, le=500)
    server_ids: list[str] | None = Field(default=None, max_length=100)


class PlacementTargetPickRequest(BaseModel):
    count: int = Field(default=100, ge=1, le=500)
    server_id: str = Field(min_length=1, max_length=64)


class PlacementMigrationRequest(BaseModel):
    names: list[str] = Field(min_length=1, max_length=500)
    server_id: str = Field(min_length=1, max_length=64)


class ClusterPeerItem(BaseModel):
    server_id: str = Field(min_length=1, max_length=64)
    host: str = Field(min_length=1, max_length=255)
    max_bitrate: str | None = Field(default=None, max_length=32)

    @field_validator("host")
    @classmethod
    def clean_cluster_host(cls, value: str) -> str:
        value = value.strip()
        if not value or any(char in value for char in ("\n", "\r", "\x00", "{", "}", ";")):
            raise ValueError("Некорректный hostname peer")
        return value

    @field_validator("max_bitrate")
    @classmethod
    def clean_max_bitrate(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            return None
        if not re.fullmatch(r"\d+(?:[KMG])?", value, flags=re.IGNORECASE):
            raise ValueError("max_bitrate: число с необязательным K, M или G")
        return value.upper()


class ClusterSettingsUpdate(BaseModel):
    enabled: bool = True
    balancer_server_id: str | None = Field(default=None, max_length=64)
    balancer_name: str = Field(default="lb01", min_length=1, max_length=64)
    mode: str = "clients"
    cluster_key: str | None = Field(default=None, max_length=255)
    peers: list[ClusterPeerItem] = Field(default_factory=list, max_length=100)

    @field_validator("balancer_name")
    @classmethod
    def clean_balancer_name(cls, value: str) -> str:
        value = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
            raise ValueError("Имя balancer может содержать латиницу, цифры, _, - и .")
        return value

    @field_validator("mode")
    @classmethod
    def clean_cluster_mode(cls, value: str) -> str:
        value = value.strip().lower()
        if value not in {"clients", "bitrate", "usage", "streams"}:
            raise ValueError("Режим должен быть clients, bitrate, usage или streams")
        return value

    @model_validator(mode="after")
    def unique_cluster_peers(self) -> "ClusterSettingsUpdate":
        server_ids = [item.server_id for item in self.peers]
        hosts = [item.host.lower() for item in self.peers]
        if len(server_ids) != len(set(server_ids)):
            raise ValueError("Один сервер выбран в Cluster несколько раз")
        if len(hosts) != len(set(hosts)):
            raise ValueError("Hostname peer повторяется")
        return self
