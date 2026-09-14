from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Optional

from mqtt import MqttConfig, PublishConfig, TlsConfig, TopicConfig


class SendMode(str, Enum):
    NORMAL = "normal"
    HISTORY_DATA = "history_data"


@dataclass(frozen=True)
class SourceConfig:
    directory: Path
    file_pattern: str = "*.parquet"
    files: tuple[Path, ...] = ()
    batch_by_subdirectory: bool = False


@dataclass(frozen=True)
class MessageConfig:
    ps_id: int
    app_key: str
    source: str
    default_data_type: str = "telemetry"
    data_type_mapping_path: Optional[Path] = None


@dataclass(frozen=True)
class RuntimeConfig:
    source: SourceConfig
    message: MessageConfig
    mqtt: MqttConfig
    send_mode: SendMode = SendMode.NORMAL

    def with_send_mode(self, send_mode: SendMode) -> "RuntimeConfig":
        return RuntimeConfig(source=self.source, message=self.message, mqtt=self.mqtt, send_mode=send_mode)

    def with_source_directory(self, directory: Path) -> "RuntimeConfig":
        return RuntimeConfig(
            source=SourceConfig(
                directory=directory.expanduser().resolve(),
                file_pattern=self.source.file_pattern,
                files=(),
                batch_by_subdirectory=False,
            ),
            message=self.message,
            mqtt=self.mqtt,
            send_mode=self.send_mode,
        )


def _read_required_string(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Config value '{key}' is required and must be a non-empty string.")
    return value


def _read_required_int(data: Mapping[str, Any], key: str) -> int:
    value = data.get(key)
    if not isinstance(value, int):
        raise ValueError(f"Config value '{key}' is required and must be an integer.")
    return value


def _build_source_config(data: Mapping[str, Any], base_dir: Path) -> SourceConfig:
    source_data = data.get("source", {})
    if not isinstance(source_data, Mapping):
        raise ValueError("Config value 'source' must be an object.")

    directory = Path(str(source_data.get("directory", "src/sourceData"))).expanduser()
    if not directory.is_absolute():
        directory = base_dir / directory
    directory = directory.resolve()

    files_data = source_data.get("files", [])
    if files_data is None:
        files_data = []
    if not isinstance(files_data, list):
        raise ValueError("Config value 'source.files' must be an array.")

    files = []
    for file_value in files_data:
        file_path = Path(str(file_value)).expanduser()
        if not file_path.is_absolute():
            file_path = directory / file_path
        files.append(file_path.resolve())

    return SourceConfig(
        directory=directory,
        file_pattern=str(source_data.get("file_pattern", "*.parquet")),
        files=tuple(files),
        batch_by_subdirectory=bool(source_data.get("batch_by_subdirectory", False)),
    )


def _resolve_optional_path(value: Any, base_dir: Path) -> Optional[Path]:
    if not value:
        return None

    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _build_message_config(data: Mapping[str, Any], base_dir: Path) -> MessageConfig:
    message_data = data.get("message", {})
    if not isinstance(message_data, Mapping):
        raise ValueError("Config value 'message' must be an object.")

    return MessageConfig(
        ps_id=_read_required_int(message_data, "ps_id"),
        app_key=_read_required_string(message_data, "app_key"),
        source=_read_required_string(message_data, "source"),
        default_data_type=str(message_data.get("default_data_type", "telemetry")),
        data_type_mapping_path=_resolve_optional_path(message_data.get("data_type_mapping_path"), base_dir),
    )


def _resolve_optional_path_string(value: Any, base_dir: Path) -> str:
    resolved_path = _resolve_optional_path(value, base_dir)
    return "" if resolved_path is None else str(resolved_path)


def _build_mqtt_config(data: Mapping[str, Any], app_key: str, base_dir: Path) -> MqttConfig:
    mqtt_data = data.get("mqtt", {})
    if not isinstance(mqtt_data, Mapping):
        raise ValueError("Config value 'mqtt' must be an object.")

    tls_data = mqtt_data.get("tls", {})
    if not isinstance(tls_data, Mapping):
        raise ValueError("Config value 'mqtt.tls' must be an object.")

    publish_data = mqtt_data.get("publish", {})
    if not isinstance(publish_data, Mapping):
        raise ValueError("Config value 'mqtt.publish' must be an object.")

    topics_data = mqtt_data.get("topics", {})
    if not isinstance(topics_data, Mapping):
        raise ValueError("Config value 'mqtt.topics' must be an object.")

    config = MqttConfig(
        app_key=app_key,
        username=_read_required_string(mqtt_data, "username"),
        password=_read_required_string(mqtt_data, "password"),
        client_id=str(mqtt_data.get("client_id", "")),
        encrypted=bool(mqtt_data.get("encrypted", False)),
        encrypted_host=str(mqtt_data.get("encrypted_host", "euinsightiot.isolarinsightportal.com")),
        encrypted_port=int(mqtt_data.get("encrypted_port", 16668)),
        plain_host=str(mqtt_data.get("plain_host", "euinsightiot.isolarinsightportal.com")),
        plain_port=int(mqtt_data.get("plain_port", 19999)),
        tls=TlsConfig(
            enabled=bool(tls_data.get("enabled", False)),
            ca_cert=_resolve_optional_path_string(tls_data.get("ca_cert"), base_dir),
            client_cert=_resolve_optional_path_string(tls_data.get("client_cert"), base_dir),
            client_key=_resolve_optional_path_string(tls_data.get("client_key"), base_dir),
            insecure_skip_verify=bool(tls_data.get("insecure_skip_verify", False)),
        ),
        publish=PublishConfig(
            qos=int(publish_data.get("qos", 1)),
            retain=bool(publish_data.get("retain", False)),
            keepalive=int(publish_data.get("keepalive", 60)),
            max_payload_bytes=int(publish_data.get("max_payload_bytes", 65536)),
            publish_retries=int(publish_data.get("publish_retries", 3)),
            publish_retry_interval_seconds=float(publish_data.get("publish_retry_interval_seconds", 1.0)),
            publish_timeout_seconds=float(publish_data.get("publish_timeout_seconds", 10.0)),
        ),
        topics=TopicConfig(
            telemetry_template=str(
                topics_data.get(
                    "telemetry_template",
                    "isolarcloud/insight/pub/rundata/5min/{app_key}/enum_0_0_0",
                )
            ),
            telesignaling_template=str(
                topics_data.get(
                    "telesignaling_template",
                    "isolarcloud/insight/pub/faultdata/{app_key}/enum_0_0_0",
                )
            ),
            history_data_template=str(
                topics_data.get(
                    "history_data_template",
                    "isolarcloud/insight/pub/rundata/history/{app_key}/enum_0_0_0",
                )
            ),
        ),
        connect_timeout_seconds=float(mqtt_data.get("connect_timeout_seconds", 10.0)),
        reconnect_min_delay_seconds=int(mqtt_data.get("reconnect_min_delay_seconds", 1)),
        reconnect_max_delay_seconds=int(mqtt_data.get("reconnect_max_delay_seconds", 30)),
        max_reconnect_attempts=int(mqtt_data.get("max_reconnect_attempts", 3)),
    )
    config.validate()
    return config


def _build_send_mode(data: Mapping[str, Any]) -> SendMode:
    try:
        return SendMode(str(data.get("send_mode", SendMode.NORMAL.value)))
    except ValueError as error:
        raise ValueError("Config value 'send_mode' must be 'normal' or 'history_data'.") from error


def load_runtime_config(config_path: Path) -> RuntimeConfig:
    resolved_config_path = config_path.expanduser().resolve()
    with resolved_config_path.open("r", encoding="utf-8") as config_file:
        data = json.load(config_file)

    if not isinstance(data, Mapping):
        raise ValueError("Runtime config must be a JSON object.")

    base_dir = resolved_config_path.parent.parent if resolved_config_path.parent.name == "config" else Path.cwd()
    source = _build_source_config(data, base_dir)
    message = _build_message_config(data, base_dir)
    mqtt = _build_mqtt_config(data, message.app_key, base_dir)
    send_mode = _build_send_mode(data)
    return RuntimeConfig(source=source, message=message, mqtt=mqtt, send_mode=send_mode)
