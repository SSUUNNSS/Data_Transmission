from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Union

from .errors import MqttConfigError


class DataType(str, Enum):
    """Supported measuring point data types"""

    TELEMETRY = "telemetry"
    TELESIGNALING = "telesignaling"

ENABLE_ENCRYPT = False
DEFAULT_ENCRYPTED_HOST = "euinsightiot.isolarinsightportal.com"
DEFAULT_ENCRYPTED_PORT = 16668
DEFAULT_PLAIN_HOST = "euinsightiot.isolarinsightportal.com"
DEFAULT_PLAIN_PORT = 19999


DEFAULT_TELEMETRY_TOPIC_TEMPLATE = "isolarcloud/insight/pub/rundata/5min/{app_key}/enum_0_0_0"
DEFAULT_TELESIGNALING_TOPIC_TEMPLATE = "isolarcloud/insight/pub/faultdata/{app_key}/enum_0_0_0"
DEFAULT_HISTORY_DATA_TOPIC_TEMPLATE = "isolarcloud/insight/pub/rundata/history/{app_key}/enum_0_0_0"

MAX_PAYLOAD_BYTES = 64 * 1024


@dataclass(frozen=True)
class TlsConfig:
    """TLS behavior for encrypted MQTT connections."""

    enabled: bool = False
    ca_cert: str = ""
    client_cert: str = ""
    client_key: str = ""
    # Temporary local integration mode. Do not use it for production hardening.
    insecure_skip_verify: bool = False

    def validate(self, tls_enabled: bool) -> None:
        if not tls_enabled:
            return
        if bool(self.client_cert) != bool(self.client_key):
            raise MqttConfigError("MQTT TLS client_cert and client_key must be configured together.")


@dataclass(frozen=True)
class PublishConfig:
    """Publish-time behavior shared by all supported MQTT topics."""

    qos: int = 1
    retain: bool = False
    keepalive: int = 60
    max_payload_bytes: int = MAX_PAYLOAD_BYTES
    publish_retries: int = 3
    publish_retry_interval_seconds: float = 1.0
    publish_timeout_seconds: float = 10.0

    def validate(self) -> None:
        if self.qos not in (0, 1):
            raise MqttConfigError("MQTT qos must be 0 or 1.")
        if self.keepalive <= 0:
            raise MqttConfigError("MQTT keepalive must be greater than 0.")
        if self.max_payload_bytes <= 0:
            raise MqttConfigError("MQTT max_payload_bytes must be greater than 0.")
        if self.publish_retries < 0:
            raise MqttConfigError("MQTT publish_retries cannot be negative.")
        if self.publish_timeout_seconds <= 0:
            raise MqttConfigError("MQTT publish_timeout_seconds must be greater than 0.")


@dataclass(frozen=True)
class TopicConfig:
    """Topic templates for supported data types."""

    telemetry_template: str = DEFAULT_TELEMETRY_TOPIC_TEMPLATE
    telesignaling_template: str = DEFAULT_TELESIGNALING_TOPIC_TEMPLATE
    history_data_template: str = DEFAULT_HISTORY_DATA_TOPIC_TEMPLATE

    def get_topic(self, data_type: Union[DataType, str], app_key: str) -> str:
        normalized_data_type = self.normalize_data_type(data_type)
        if normalized_data_type == DataType.TELEMETRY:
            return self.telemetry_template.format(app_key=app_key)
        if normalized_data_type == DataType.TELESIGNALING:
            return self.telesignaling_template.format(app_key=app_key)
        raise MqttConfigError(f"Unsupported MQTT data type: {data_type}")

    def get_history_data_topic(self, app_key: str) -> str:
        return self.history_data_template.format(app_key=app_key)

    def normalize_data_type(self, data_type: Union[DataType, str]) -> DataType:
        if isinstance(data_type, DataType):
            return data_type
        try:
            return DataType(data_type)
        except ValueError as error:
            raise MqttConfigError(f"Unsupported MQTT data type: {data_type}") from error


@dataclass(frozen=True)
class MqttConfig:
    """Top-level MQTT client configuration."""

    app_key: str
    username: str
    password: str
    client_id: str = ""
    encrypted: bool = ENABLE_ENCRYPT
    encrypted_host: str = DEFAULT_ENCRYPTED_HOST
    encrypted_port: int = DEFAULT_ENCRYPTED_PORT
    plain_host: str = DEFAULT_PLAIN_HOST
    plain_port: int = DEFAULT_PLAIN_PORT
    tls: TlsConfig = field(default_factory=TlsConfig)
    publish: PublishConfig = field(default_factory=PublishConfig)
    topics: TopicConfig = field(default_factory=TopicConfig)
    # Initial connection is bounded by timeout; paho handles later reconnect delays.
    connect_timeout_seconds: float = 10.0
    reconnect_min_delay_seconds: int = 1
    reconnect_max_delay_seconds: int = 30
    max_reconnect_attempts: int = 3

    @property
    def host(self) -> str:
        return self.encrypted_host if self.encrypted else self.plain_host

    @property
    def port(self) -> int:
        return self.encrypted_port if self.encrypted else self.plain_port

    def validate(self) -> None:
        if not self.app_key:
            raise MqttConfigError("MQTT app_key is required.")
        if not self.username:
            raise MqttConfigError("MQTT username is required.")
        if not self.password:
            raise MqttConfigError("MQTT password is required.")
        if not self.host:
            raise MqttConfigError("MQTT host is required.")
        if self.port <= 0:
            raise MqttConfigError("MQTT port must be greater than 0.")
        if self.connect_timeout_seconds <= 0:
            raise MqttConfigError("MQTT connect_timeout_seconds must be greater than 0.")
        if self.reconnect_min_delay_seconds <= 0:
            raise MqttConfigError("MQTT reconnect_min_delay_seconds must be greater than 0.")
        if self.reconnect_max_delay_seconds < self.reconnect_min_delay_seconds:
            raise MqttConfigError("MQTT reconnect_max_delay_seconds cannot be less than reconnect_min_delay_seconds.")
        if self.max_reconnect_attempts < 0:
            raise MqttConfigError("MQTT max_reconnect_attempts cannot be negative.")
        self.tls.validate(tls_enabled=self.encrypted or self.tls.enabled)
        self.publish.validate()
