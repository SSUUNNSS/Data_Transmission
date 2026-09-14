from __future__ import annotations

import json
import logging
import ssl
import time
from dataclasses import dataclass
from threading import Event
from typing import Any, Mapping, Optional, Union
from uuid import uuid4

import paho.mqtt.client as mqtt

from .config import DataType, MqttConfig
from .errors import (
    MqttConnectionError,
    MqttNonRetryableConnectionError,
    MqttPublishError,
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublishResult:
    """Result returned to callers so failed messages can be logged and skipped."""

    data_type: DataType
    topic: str
    payload_bytes: int
    attempts: int
    success: bool


def encode_json_payload(message: Mapping[str, Any]) -> bytes:
    """Encode JSON compactly so payload size checks match the MQTT bytes sent."""

    return json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def split_oversized_json_payload(message: Mapping[str, Any], max_payload_bytes: int) -> list[Mapping[str, Any]]:
    """TODO: Placeholder for the oversized JSON split protocol."""

    return []


def _is_retryable_connack(reason_code: int) -> bool:
    return reason_code == 3


def _connack_error_message(reason_code: int) -> str:
    messages = {
        1: "Connection refused: unsupported MQTT protocol version.",
        2: "Connection refused: invalid client id.",
        3: "Connection refused: server unavailable.",
        4: "Connection refused: bad username or password.",
        5: "Connection refused: not authorized.",
    }
    return messages.get(reason_code, f"MQTT connection refused with reason code {reason_code}.")


def _reason_code_to_int(reason_code: Any) -> int:
    if hasattr(reason_code, "value"):
        return int(reason_code.value)
    return int(reason_code)


class MqttPublisher:

    def __init__(self, config: MqttConfig, logger: Optional[logging.Logger] = None) -> None:
        config.validate()
        self.config = config
        self.logger = logger or LOGGER
        self._connected = Event()
        self._disconnect_requested = False
        self._connect_reason_code: Optional[int] = None
        self._initial_retryable_connack_count = 0
        self._reconnect_attempts = 0
        self._has_connected_once = False
        self._fatal_error: Optional[MqttConnectionError] = None
        self._client_id = self._build_client_id()
        self._client = self._create_client()

    def connect(self) -> None:
        self._connected.clear()
        self._disconnect_requested = False
        self._connect_reason_code = None
        self._initial_retryable_connack_count = 0
        self._fatal_error = None
        # paho-mqtt reconnection configuration after the network loop starts
        self._client.reconnect_delay_set(
            min_delay=self.config.reconnect_min_delay_seconds,
            max_delay=self.config.reconnect_max_delay_seconds,
        )

        try:
            self._client.connect_async(self.config.host, self.config.port, self.config.publish.keepalive)
            self._client.loop_start()
        except Exception as error:
            self._client.loop_stop()
            raise MqttConnectionError(f"Failed to start MQTT connection: {error}") from error

        # Keep startup deterministic for batch jobs and Lambda-style one-shot execution.
        if not self._connected.wait(timeout=self.config.connect_timeout_seconds):
            self._client.loop_stop()
            if self._fatal_error is not None:
                raise self._fatal_error
            if self._connect_reason_code is not None:
                message = _connack_error_message(self._connect_reason_code)
                raise MqttConnectionError(f"Timed out waiting for retryable MQTT connection: {message}")
            raise MqttConnectionError("Timed out waiting for MQTT CONNACK.")

        reason_code = self._connect_reason_code
        if reason_code == 0:
            return

        # Non-retryable CONNACK codes should fail fast instead of reconnecting.
        message = _connack_error_message(reason_code or -1)
        self._client.loop_stop()
        self._connected.clear()
        if not _is_retryable_connack(reason_code or -1):
            raise MqttNonRetryableConnectionError(message)
        raise MqttConnectionError(message)

    def disconnect(self) -> None:
        self._disconnect_requested = True
        self._client.loop_stop()
        self._client.disconnect()
        self._connected.clear()

    def publish(
        self,
        message: Mapping[str, Any],
        data_type: Union[DataType, str],
        topic_override: Optional[str] = None,
    ) -> PublishResult:
        if self._fatal_error is not None:
            raise self._fatal_error

        normalized_data_type = self.config.topics.normalize_data_type(data_type)
        topic = topic_override or self.resolve_topic(normalized_data_type)
        payload = encode_json_payload(message)

        if len(payload) > self.config.publish.max_payload_bytes:
            split_messages = split_oversized_json_payload(message, self.config.publish.max_payload_bytes)
            if not split_messages:
                self.logger.warning(
                    "MQTT payload exceeds max size and will be sent without splitting.",
                    extra={
                        "data_type": normalized_data_type.value,
                        "topic": topic,
                        "payload_bytes": len(payload),
                        "max_payload_bytes": self.config.publish.max_payload_bytes,
                    },
                )

        attempts = self.config.publish.publish_retries + 1
        last_error: Optional[Exception] = None

        for attempt in range(1, attempts + 1):
            try:
                info = self._client.publish(
                    topic,
                    payload,
                    qos=self.config.publish.qos,
                    retain=self.config.publish.retain,
                )
                info.wait_for_publish(timeout=self.config.publish.publish_timeout_seconds)

                # wait_for_publish can return even when the publish result code is not success.
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    raise MqttPublishError(f"MQTT publish failed with rc={info.rc}.")

                self.logger.debug(
                    "Published MQTT message.",
                    extra={
                        "data_type": normalized_data_type.value,
                        "topic": topic,
                        "payload_bytes": len(payload),
                        "attempt": attempt,
                    },
                )
                return PublishResult(normalized_data_type, topic, len(payload), attempt, True)
            except Exception as error:
                last_error = error
                self.logger.warning(
                    "MQTT publish attempt failed.",
                    extra={
                        "data_type": normalized_data_type.value,
                        "topic": topic,
                        "payload_bytes": len(payload),
                        "attempt": attempt,
                        "max_attempts": attempts,
                        "error": str(error),
                    },
                )
                if attempt < attempts:
                    time.sleep(self.config.publish.publish_retry_interval_seconds)

        self.logger.error(
            "MQTT publish failed after retries.",
            extra={
                "data_type": normalized_data_type.value,
                "topic": topic,
                "payload_bytes": len(payload),
                "attempts": attempts,
                "error": str(last_error),
            },
        )
        return PublishResult(normalized_data_type, topic, len(payload), attempts, False)

    def resolve_topic(self, data_type: Union[DataType, str]) -> str:
        return self.config.topics.get_topic(data_type, self.config.app_key)

    def _build_client_id(self) -> str:
        if self.config.client_id:
            return self.config.client_id
        return f"{self.config.app_key}-{uuid4().hex[:8]}"

    def _create_client(self) -> mqtt.Client:
        client_kwargs = {
            "client_id": self._client_id,
            "protocol": mqtt.MQTTv311,
            "callback_api_version": mqtt.CallbackAPIVersion.VERSION2,
        }

        client = mqtt.Client(**client_kwargs)
        client.username_pw_set(self.config.username, self.config.password)

        if self.config.encrypted or self.config.tls.enabled:
            ca_certs = self.config.tls.ca_cert or None
            certfile = self.config.tls.client_cert or None
            keyfile = self.config.tls.client_key or None
            if self.config.tls.insecure_skip_verify:
                # This is a temporary integration mode and should not be used for production hardening.
                client.tls_set(ca_certs=ca_certs, certfile=certfile, keyfile=keyfile, cert_reqs=ssl.CERT_NONE)
                client.tls_insecure_set(True)
            else:
                client.tls_set(ca_certs=ca_certs, certfile=certfile, keyfile=keyfile, cert_reqs=ssl.CERT_REQUIRED)

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        return client

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        reason_code_value = _reason_code_to_int(reason_code)
        self._connect_reason_code = reason_code_value

        if reason_code_value == 0:
            # A successful CONNACK releases the synchronous connect() waiter.
            self._has_connected_once = True
            self._initial_retryable_connack_count = 0
            self._reconnect_attempts = 0
            self._connected.set()
            self.logger.info(
                "Connected to MQTT broker.",
                extra={"client_id": self._client_id, "mqtt_host": self.config.host, "mqtt_port": self.config.port},
            )
            return

        if _is_retryable_connack(reason_code_value):
            self._initial_retryable_connack_count += 1

        self.logger.warning(
            "MQTT broker refused connection.",
            extra={
                "reason_code": reason_code_value,
                "error": _connack_error_message(reason_code_value),
                "retryable": _is_retryable_connack(reason_code_value),
                "retryable_attempts": self._initial_retryable_connack_count,
                "max_retryable_attempts": self.config.max_reconnect_attempts,
            },
        )
        if _is_retryable_connack(reason_code_value):
            if self._initial_retryable_connack_count >= self.config.max_reconnect_attempts:
                self._fatal_error = MqttConnectionError(_connack_error_message(reason_code_value))
                self._connected.set()
            return

        if not _is_retryable_connack(reason_code_value):
            self._connected.set()

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: Any = None,
        reason_code: Any = None,
        properties: Any = None,
    ) -> None:
        self._connected.clear()
        if reason_code is None:
            reason_code = disconnect_flags
            disconnect_flags = None

        if self._has_connected_once and not self._disconnect_requested:
            self._reconnect_attempts += 1
            if self._reconnect_attempts > self.config.max_reconnect_attempts:
                self._fatal_error = MqttConnectionError("MQTT reconnect attempts exceeded.")
                self.logger.error(
                    "MQTT reconnect attempts exceeded; stopping network loop.",
                    extra={
                        "client_id": self._client_id,
                        "reconnect_attempts": self._reconnect_attempts,
                        "max_reconnect_attempts": self.config.max_reconnect_attempts,
                    },
                )
                self._client.loop_stop()

        self.logger.info(
            "Disconnected from MQTT broker.",
            extra={
                "client_id": self._client_id,
                "reason_code": str(reason_code),
                "disconnect_flags": str(disconnect_flags),
                "reconnect_attempts": self._reconnect_attempts,
            },
        )
