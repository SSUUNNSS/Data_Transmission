from __future__ import annotations

import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from mqtt.publisher import encode_json_payload
from transmission import SendMode, TransmissionRunner, load_runtime_config


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "local.json"
DEFAULT_LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "transmission.log"
DEFAULT_PREVIEW_OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent / "outputs" / "mqtt_preview" / "messages_preview.jsonl"
)
STANDARD_LOG_RECORD_FIELDS = set(
    logging.LogRecord(
        name="",
        level=0,
        pathname="",
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    ).__dict__
) | {"asctime", "message"}


class ConsoleStageFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return bool(
            getattr(record, "console", False)
            or record.name == "watch_folder"
        )


class ExtraJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base_message = super().format(record)
        extra = {
            key: value
            for key, value in record.__dict__.items()
            if key not in STANDARD_LOG_RECORD_FIELDS and not key.startswith("_")
        }
        if not extra:
            return base_message
        return f"{base_message} {json.dumps(extra, ensure_ascii=False, default=str, separators=(',', ':'))}"


class StationContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "station"):
            record.station = "SYSTEM"
        return True


class StationLoggerAdapter(logging.LoggerAdapter):
    def process(self, message, kwargs):
        extra = kwargs.setdefault("extra", {})
        extra.setdefault("station", self.extra["station"])
        return f"[{self.extra['station']}] {message}", kwargs


def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=100 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(ExtraJsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    file_handler.addFilter(StationContextFilter())

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.addFilter(ConsoleStageFilter())
    console_handler.addFilter(StationContextFilter())
    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transmit local parquet source data to MQTT.")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to JSON runtime config.",
    )
    parser.add_argument(
        "--log-file",
        default=str(DEFAULT_LOG_PATH),
        help="Path to full log file.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Build MQTT messages and write a preview file without connecting to MQTT.",
    )
    parser.add_argument(
        "--preview-output",
        default=str(DEFAULT_PREVIEW_OUTPUT_PATH),
        help="Path to the JSON Lines preview output file.",
    )
    parser.add_argument(
        "--preview-limit",
        type=int,
        default=100,
        help="Maximum number of messages to write in preview mode. Use 0 for no limit.",
    )
    parser.add_argument(
        "--send-mode",
        choices=[SendMode.NORMAL.value, SendMode.HISTORY_DATA.value],
        default="",
        help="Override config send mode. Use normal or history_data.",
    )
    parser.add_argument(
        "--send-limit",
        type=int,
        default=0,
        help="Maximum number of MQTT messages to send. Use 0 for no limit.",
    )
    return parser.parse_args()


def get_station_name(config_path: Path) -> str:
    station_name = config_path.stem
    if station_name.endswith("_local"):
        station_name = station_name[:-6]
    return station_name.capitalize()


def count_preview_messages(runner: TransmissionRunner, build_results) -> int:
    count = 0
    for build_result in build_results:
        for message in build_result.messages:
            if not runner.should_skip_message(message):
                count += 1
    return count


def write_preview_file(runner: TransmissionRunner, build_results, preview_output: Path, preview_limit: int) -> int:
    if preview_limit < 0:
        raise ValueError("Preview limit must be greater than or equal to 0.")

    preview_output.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    total_messages = count_preview_messages(runner, build_results)

    with preview_output.open("w", encoding="utf-8") as preview_file:
        for build_result in build_results:
            for batch_index, message in enumerate(build_result.messages, start=1):
                if runner.should_skip_message(message):
                    continue
                if preview_limit and written >= preview_limit:
                    return written

                topic = runner.resolve_publish_topic_override(message)
                if topic is None:
                    topic = runner.config.mqtt.topics.get_topic(message.data_type, runner.config.mqtt.app_key)
                record = {
                    "message_index": written + 1,
                    "message_count": total_messages,
                    "send_mode": runner.config.send_mode.value,
                    "batch_name": build_result.batch_name,
                    "batch_message_index": batch_index,
                    "batch_message_count": len(build_result.messages),
                    "source_files": build_result.source_files,
                    "data_type": message.data_type.value,
                    "topic": topic,
                    "payload_bytes": len(encode_json_payload(message.payload)),
                    "message": message.payload,
                }
                preview_file.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
                preview_file.write("\n")
                written += 1

    return written


def main() -> None:
    args = parse_args()
    setup_logging(Path(args.log_file).expanduser().resolve())
    config_path = Path(args.config).expanduser().resolve()
    logger = StationLoggerAdapter(
        logging.getLogger(__name__),
        {"station": get_station_name(config_path)},
    )
    logger.info("Config loading started.", extra={"console": True})
    config = load_runtime_config(config_path)
    if args.send_mode:
        config = config.with_send_mode(SendMode(args.send_mode))
    logger.info("Config loading completed.", extra={"console": True})

    runner = TransmissionRunner(config, logger=logger)
    if args.preview:
        build_results = runner.build_message_batches()
        total_messages = count_preview_messages(runner, build_results)
        preview_output = Path(args.preview_output).expanduser().resolve()
        logger.info("Preview file writing started.", extra={"console": True})
        preview_written = write_preview_file(runner, build_results, preview_output, args.preview_limit)
        logger.info(
            "Preview file written.",
            extra={
                "preview_output": str(preview_output),
                "preview_written": preview_written,
                "messages_built": total_messages,
                "preview_limit": args.preview_limit,
                "send_mode": config.send_mode.value,
            },
        )
        logger.info(
            "Preview completed: %s of %s messages written to %s."
            % (preview_written, total_messages, preview_output),
            extra={"console": True},
        )
        return

    summary = runner.run(send_limit=args.send_limit)
    logger.info("Transmission completed.", extra=summary.__dict__)
    logger.info(
        "Transmission completed: %(records_read)s records read, %(messages_built)s messages built, "
        "%(messages_published)s published, %(messages_failed)s failed, %(messages_skipped)s skipped."
        % summary.__dict__,
        extra={"console": True},
    )


if __name__ == "__main__":
    main()
