from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Optional

from mqtt import DataType, MqttPublisher, PublishResult

from .config import RuntimeConfig, SendMode
from .message_builder import BuiltMessage, MessageBuilder
from .parquet_source import ParquetSourceReader


LOGGER = logging.getLogger(__name__)
SUCCESS_LOG_INTERVAL = 1000


@dataclass(frozen=True)
class RunSummary:
    records_read: int
    messages_built: int
    messages_published: int
    messages_failed: int
    messages_skipped: int = 0


@dataclass(frozen=True)
class BuildResult:
    batch_name: str
    source_files: list[str]
    records_read: int
    messages: list[BuiltMessage]


@dataclass(frozen=True)
class TransmissionResult:
    status: str
    next_offset: int
    records_read: int
    messages_built: int
    messages_published: int
    messages_failed: int
    messages_skipped: int


class TransmissionRunner:
    def __init__(self, config: RuntimeConfig, logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.logger = logger or LOGGER

    def build_messages(self) -> BuildResult:
        build_results = self.build_message_batches()
        records_read = sum(result.records_read for result in build_results)
        messages = [message for result in build_results for message in result.messages]
        source_files = [file_path for result in build_results for file_path in result.source_files]
        return BuildResult(
            batch_name="all",
            source_files=source_files,
            records_read=records_read,
            messages=messages,
        )

    def build_message_batches(self) -> list[BuildResult]:
        reader = ParquetSourceReader(
            self.config.source.directory,
            self.config.source.file_pattern,
            self.config.source.files,
            self.config.source.batch_by_subdirectory,
        )

        self.logger.info("Data processing started.", extra={"console": True})
        build_results = []
        total_records_read = 0
        total_messages_built = 0

        for batch in reader.list_batches():
            builder = MessageBuilder(self.config.message, self.logger)
            records_read = 0
            for record in reader.iter_batch_records(batch):
                records_read += 1
                builder.add_record(record)

            messages = builder.build_messages()
            total_records_read += records_read
            total_messages_built += len(messages)
            build_result = BuildResult(
                batch_name=batch.name,
                source_files=[str(file_path) for file_path in batch.files],
                records_read=records_read,
                messages=messages,
            )
            build_results.append(build_result)
            self.logger.info(
                "Built MQTT messages from parquet source batch.",
                extra={
                    "batch_name": batch.name,
                    "source_files": [str(file_path) for file_path in batch.files],
                    "records_read": records_read,
                    "messages_built": len(messages),
                },
            )

        self.logger.info(
            "Data processing completed: %s records read, %s MQTT messages built."
            % (total_records_read, total_messages_built),
            extra={"console": True},
        )

        return build_results

    def log_publish_success_checkpoint(
        self,
        build_result: BuildResult,
        message: BuiltMessage,
        result: PublishResult,
        published: int,
    ) -> None:
        if published % SUCCESS_LOG_INTERVAL != 0:
            return

        self.logger.info(
            "Published MQTT message checkpoint.",
            extra={
                "published_count": published,
                "success_log_interval": SUCCESS_LOG_INTERVAL,
                "batch_name": build_result.batch_name,
                "records_read": build_result.records_read,
                "messages_built": len(build_result.messages),
                "source_files": build_result.source_files,
                "data_type": result.data_type.value,
                "topic": result.topic,
                "payload_bytes": result.payload_bytes,
                "publish_attempts": result.attempts,
                "mqtt_message": message.payload,
            },
        )

    def resolve_publish_topic_override(self, message: BuiltMessage) -> Optional[str]:
        if self.config.send_mode == SendMode.NORMAL:
            return None

        if self.config.send_mode == SendMode.HISTORY_DATA:
            if message.data_type != DataType.TELEMETRY:
                return None
            return self.config.mqtt.topics.get_history_data_topic(self.config.mqtt.app_key)

        raise ValueError(f"Unsupported send mode: {self.config.send_mode.value}")

    def should_skip_message(self, message: BuiltMessage) -> bool:
        return self.config.send_mode == SendMode.HISTORY_DATA and message.data_type != DataType.TELEMETRY

    def run_resumable(
        self,
        start_offset: int = 0,
        checkpoint_messages: int = 500,
        should_pause: Optional[Callable[[], bool]] = None,
        checkpoint_callback: Optional[Callable[[TransmissionResult], None]] = None,
    ) -> TransmissionResult:
        if start_offset < 0:
            raise ValueError("start_offset must be greater than or equal to 0.")
        if checkpoint_messages <= 0:
            raise ValueError("checkpoint_messages must be greater than 0.")

        build_results = self.build_message_batches()
        records_read = sum(result.records_read for result in build_results)
        messages_built = sum(len(result.messages) for result in build_results)
        publishable = [
            (build_result, message)
            for build_result in build_results
            for message in build_result.messages
            if not self.should_skip_message(message)
        ]
        offset = min(start_offset, len(publishable))
        published = 0
        failed = 0
        skipped = messages_built - len(publishable)
        publisher = MqttPublisher(self.config.mqtt, self.logger)

        def result(status: str) -> TransmissionResult:
            return TransmissionResult(
                status=status,
                next_offset=offset,
                records_read=records_read,
                messages_built=messages_built,
                messages_published=published,
                messages_failed=failed,
                messages_skipped=skipped,
            )

        try:
            self.logger.info("MQTT connection started.", extra={"console": True})
            publisher.connect()
            self.logger.info("MQTT connection succeeded. Starting publish.", extra={"console": True})
            self.logger.info(
                "MQTT publishing in progress from offset %s.",
                offset,
                extra={"console": True},
            )
            initial_checkpoint = result("in_progress")
            if checkpoint_callback is not None:
                checkpoint_callback(initial_checkpoint)
            chunk_published = 0
            for build_result, message in publishable[offset:]:
                topic_override = self.resolve_publish_topic_override(message)
                publish_result = publisher.publish(
                    message.payload,
                    message.data_type,
                    topic_override=topic_override,
                )
                if not publish_result.success:
                    failed += 1
                    checkpoint = result("failed")
                    if checkpoint_callback is not None:
                        checkpoint_callback(checkpoint)
                    return checkpoint

                published += 1
                offset += 1
                chunk_published += 1
                if chunk_published >= checkpoint_messages:
                    checkpoint = result("in_progress")
                    if checkpoint_callback is not None:
                        checkpoint_callback(checkpoint)
                    self.logger.info(
                        "History checkpoint reached: next_offset=%s.",
                        offset,
                        extra={"console": True},
                    )
                    chunk_published = 0
                    if should_pause is not None and should_pause():
                        paused = result("paused")
                        if checkpoint_callback is not None:
                            checkpoint_callback(paused)
                        self.logger.info(
                            "History paused at next_offset=%s.",
                            offset,
                            extra={"console": True},
                        )
                        return paused

            completed = result("completed")
            if checkpoint_callback is not None:
                checkpoint_callback(completed)
            self.logger.info(
                "MQTT publishing completed: %s published, %s failed, %s skipped.",
                published,
                failed,
                skipped,
                extra={"console": True},
            )
            return completed
        except Exception:
            self.logger.error("MQTT connection or publish failed.", extra={"console": True})
            self.logger.exception("MQTT connection or publish failed.")
            raise
        finally:
            publisher.disconnect()

    def run(self, send_limit: int = 0) -> RunSummary:
        if send_limit < 0:
            raise ValueError("Send limit must be greater than or equal to 0.")

        build_results = self.build_message_batches()
        records_read = sum(result.records_read for result in build_results)
        messages_built = sum(len(result.messages) for result in build_results)
        published = 0
        failed = 0
        skipped = 0
        publisher = MqttPublisher(self.config.mqtt, self.logger)

        try:
            self.logger.info("MQTT connection started.", extra={"console": True})
            publisher.connect()
            self.logger.info("MQTT connection succeeded. Starting publish.", extra={"console": True})
            self.logger.info("MQTT publishing in progress.", extra={"console": True})
            for build_result in build_results:
                self.logger.info(
                    "Publishing MQTT message batch.",
                    extra={
                        "batch_name": build_result.batch_name,
                        "records_read": build_result.records_read,
                        "messages_built": len(build_result.messages),
                        "source_files": build_result.source_files,
                    },
                )
                for message in build_result.messages:
                    if send_limit and published + failed >= send_limit:
                        self.logger.info(
                            "MQTT send limit reached; stopping publish loop.",
                            extra={
                                "send_limit": send_limit,
                                "messages_published": published,
                                "messages_failed": failed,
                                "messages_skipped": skipped,
                            },
                        )
                        break

                    if self.should_skip_message(message):
                        skipped += 1
                        self.logger.info(
                            "Skipping MQTT message because send mode only allows telemetry.",
                            extra={
                                "send_mode": self.config.send_mode.value,
                                "batch_name": build_result.batch_name,
                                "data_type": message.data_type.value,
                                "mqtt_message": message.payload,
                            },
                        )
                        continue

                    topic_override = self.resolve_publish_topic_override(message)
                    result = publisher.publish(message.payload, message.data_type, topic_override=topic_override)
                    if result.success:
                        published += 1
                        self.log_publish_success_checkpoint(build_result, message, result, published)
                    else:
                        failed += 1
                if send_limit and published + failed >= send_limit:
                    break
            self.logger.info(
                "MQTT publishing completed: %s published, %s failed, %s skipped." % (published, failed, skipped),
                extra={"console": True},
            )
        except Exception:
            self.logger.error("MQTT connection or publish failed.", extra={"console": True})
            self.logger.exception("MQTT connection or publish failed.")
            raise
        finally:
            publisher.disconnect()

        return RunSummary(
            records_read=records_read,
            messages_built=messages_built,
            messages_published=published,
            messages_failed=failed,
            messages_skipped=skipped,
        )
