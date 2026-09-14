class MqttError(Exception):
    """Base exception for MQTT publishing errors."""


class MqttConfigError(MqttError):
    """Raised when MQTT configuration is invalid."""


class MqttConnectionError(MqttError):
    """Raised when MQTT connection fails."""


class MqttNonRetryableConnectionError(MqttConnectionError):
    """Raised when CONNACK indicates a permanent connection failure."""


class MqttPublishError(MqttError):
    """Raised when a message cannot be published."""


class MqttPayloadTooLargeError(MqttError):
    """Raised when a payload exceeds the configured MQTT size limit."""
