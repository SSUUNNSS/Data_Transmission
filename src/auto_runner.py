from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from main import StationLoggerAdapter, get_station_name, setup_logging
from transmission import (
    SendMode,
    TransmissionRunner,
    load_runtime_config,
)
from transmission.runner import TransmissionResult


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "local.json"
DEFAULT_SOURCE_ROOT = PROJECT_ROOT / "src" / "sourceData"


def get_log_path_from_config(config_arg: str) -> Path:
    config_path = Path(config_arg).expanduser().resolve()

    return (
        PROJECT_ROOT
        / "logs"
        / f"{config_path.stem}_transmission.log"
    )


def get_state_path(config_path: Path) -> Path:
    config_name = config_path.stem

    if config_name.endswith("_local"):
        config_name = config_name[:-6]

    return (
        PROJECT_ROOT
        / "state"
        / f"{config_name}_processed_files.json"
    )


def get_legacy_state_path(config_path: Path) -> Path:
    config_name = config_path.stem

    if config_name.endswith("_local"):
        config_name = config_name[:-6]

    return PROJECT_ROOT / "state" / f"{config_name}_processed_batches.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automatically send parquet batches to MQTT."
    )

    parser.add_argument(
        "--source-root",
        default=str(DEFAULT_SOURCE_ROOT),
        help="Root directory containing daily parquet batch folders.",
    )

    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to runtime config JSON.",
    )

    parser.add_argument(
        "--file",
        default="",
        help="Process exactly this parquet file.",
    )

    parser.add_argument(
        "--start-date",
        default="",
        help="Start processing from this date, e.g. 2026-08-01.",
    )

    parser.add_argument(
        "--send-mode",
        choices=[
            SendMode.NORMAL.value,
            SendMode.HISTORY_DATA.value,
        ],
        default="",
        help="Override send mode from the runtime config.",
    )

    parser.add_argument(
        "--send-limit",
        type=int,
        default=0,
        help=(
            "Maximum MQTT messages to send per daily batch. "
            "Use 0 for full sending."
        ),
    )

    return parser.parse_args()


def load_state(
    state_path: Path,
) -> dict[str, Any]:

    legacy_state_path = state_path.with_name(
        state_path.name.replace(
            "_processed_files.json",
            "_processed_batches.json",
        )
    )

    if not state_path.exists() and legacy_state_path.exists():
        state_path = legacy_state_path

    if not state_path.exists() or state_path.stat().st_size == 0:
        return {
            "processed_files": {}
        }

    try:
        with state_path.open("r", encoding="utf-8") as file:
            state = json.load(file)
    except (json.JSONDecodeError, OSError) as error:
        raise ValueError(f"State file is not valid JSON: {state_path}") from error

    if not isinstance(state, dict):
        raise ValueError(f"State file must contain a JSON object: {state_path}")

    processed_files = state.get("processed_files", {})
    if not isinstance(processed_files, dict):
        raise ValueError(f"State value 'processed_files' must be an object: {state_path}")

    state["processed_files"] = processed_files

    return state


def save_state(
    state_path: Path,
    state: dict[str, Any],
) -> None:

    state_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = state_path.with_suffix(state_path.suffix + ".tmp")

    with temp_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            state,
            file,
            ensure_ascii=False,
            indent=2,
        )
        file.flush()

    temp_path.replace(
        state_path
    )


def file_id(
    file_path: Path,
    send_mode: str | SendMode,
    mode_station_root: Path,
) -> str:
    mode_value = SendMode(send_mode).value
    relative_file = file_path.resolve().relative_to(mode_station_root.resolve())
    date_name = relative_file.parent.name
    return (
        Path(mode_value)
        / date_name
        / file_path.name
    ).as_posix()


def validate_file(
    file_path: Path,
    source_root: Path,
) -> Path:
    resolved_file = file_path.expanduser().resolve()
    resolved_root = source_root.expanduser().resolve()

    if resolved_file.suffix.lower() != ".parquet":
        raise ValueError(f"File must have a .parquet suffix: {resolved_file}")

    if not resolved_file.is_file():
        raise FileNotFoundError(f"Parquet file does not exist: {resolved_file}")

    try:
        resolved_file.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError(f"File is outside source root: {resolved_file}") from error

    return resolved_file


def discover_files(
    source_root: Path,
) -> list[Path]:
    if not source_root.exists():
        raise FileNotFoundError(
            f"Source root does not exist: {source_root}"
        )

    return sorted(
        (
            parquet_file
            for parquet_file in source_root.rglob("*.parquet")
            if parquet_file.is_file()
        ),
        key=lambda path: path.relative_to(source_root).as_posix(),
    )


def process_file(
    file_path: Path,
    source_root: Path,
    config_path: Path,
    send_mode: str,
    send_limit: int,
    state_path: Path,
    should_pause=None,
    checkpoint_messages: int = 500,
) -> bool:
    if send_limit < 0:
        raise ValueError(
            "--send-limit must be greater than or equal to 0."
        )

    file_path = validate_file(file_path, source_root)
    station_name = get_station_name(config_path)
    selected_mode = (
        SendMode(send_mode)
        if send_mode
        else load_runtime_config(config_path).send_mode
    )
    relative_id = file_id(file_path, selected_mode, source_root)
    state = load_state(state_path)
    previous = state["processed_files"].get(relative_id)

    if (
        isinstance(previous, dict)
        and previous.get("status") == "completed"
    ):
        previous_mode = previous.get("send_mode", send_mode or "config")
        print(f"[{station_name}][{previous_mode}] [SKIP] {relative_id} (already completed)")
        return True

    base_config = load_runtime_config(config_path)
    runtime_config = (
        base_config
        .with_send_mode(selected_mode)
        .with_source_directory(file_path)
    )

    logger = StationLoggerAdapter(
        logging.getLogger("transmission"),
        {"station": f"{station_name}][{selected_mode.value}"},
    )

    mode_prefix = f"[{station_name}][{selected_mode.value}]"
    print(f"{mode_prefix} Station: {station_name}")
    print(f"{mode_prefix} Config: {config_path}")
    print(f"{mode_prefix} Source root: {source_root}")
    print(f"{mode_prefix} State file: {state_path}")
    print(f"{mode_prefix} File: {file_path.name}")
    print(f"{mode_prefix} [PROCESSING] {relative_id}")

    runner = TransmissionRunner(
        runtime_config,
        logger=logger,
    )

    def save_checkpoint(result: TransmissionResult) -> None:
        state["processed_files"][relative_id] = {
            "status": result.status,
            "send_mode": selected_mode.value,
            "next_offset": result.next_offset,
            "records_read": result.records_read,
            "messages_built": result.messages_built,
            "messages_published": result.next_offset,
            "messages_failed": result.messages_failed,
            "messages_skipped": result.messages_skipped,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        save_state(state_path, state)

    try:
        if selected_mode == SendMode.HISTORY_DATA:
            previous_offset = 0
            if isinstance(previous, dict):
                previous_offset = int(previous.get("next_offset", 0))
            result = runner.run_resumable(
                start_offset=previous_offset,
                checkpoint_messages=checkpoint_messages,
                should_pause=should_pause or (lambda: False),
                checkpoint_callback=save_checkpoint,
            )
            summary = result
        else:
            summary = runner.run(send_limit=send_limit)
    except Exception as error:
        last_checkpoint = state["processed_files"].get(relative_id)
        state["processed_files"][relative_id] = {
            "status": "failed",
            "last_attempt": datetime.now(
                timezone.utc
            ).isoformat(),
            "send_mode": selected_mode.value,
            "error": str(error),
        }
        if isinstance(last_checkpoint, dict) and "next_offset" in last_checkpoint:
            state["processed_files"][relative_id]["next_offset"] = last_checkpoint["next_offset"]
        save_state(state_path, state)
        print(f"{mode_prefix} [FAILED] {relative_id}: {error}")
        return False

    if isinstance(summary, TransmissionResult) and summary.status == "paused":
        print(f"{mode_prefix} [PAUSED] {relative_id} at offset {summary.next_offset}")
        return "paused"

    completed = summary.status == "completed" if isinstance(summary, TransmissionResult) else summary.messages_failed == 0
    entry = {
        "status": "completed" if completed else "failed",
        "completed_at" if completed else "last_attempt": datetime.now(
            timezone.utc
        ).isoformat(),
        "send_mode": selected_mode.value,
        "records_read": summary.records_read,
        "messages_built": summary.messages_built,
        "messages_published": summary.messages_published,
        "messages_failed": summary.messages_failed,
        "messages_skipped": summary.messages_skipped,
    }
    if isinstance(summary, TransmissionResult):
        entry["next_offset"] = summary.next_offset
        entry["messages_published"] = summary.next_offset
    state["processed_files"][relative_id] = entry
    save_state(state_path, state)
    print(f"{mode_prefix} [{entry['status'].upper()}] {relative_id}")
    return completed


def main(args: argparse.Namespace) -> None:
    source_root = Path(args.source_root).expanduser().resolve()
    config_path = Path(args.config).expanduser().resolve()
    state_path = get_state_path(config_path)

    if args.file:
        succeeded = process_file(
            Path(args.file),
            source_root,
            config_path,
            args.send_mode,
            args.send_limit,
            state_path,
        )
        if not succeeded:
            raise SystemExit(1)
        return

    start_date = None
    if args.start_date:
        start_date = datetime.strptime(
            args.start_date,
            "%Y-%m-%d",
        ).date()

    for file_path in discover_files(source_root):
        relative_path = file_path.relative_to(source_root)
        if start_date and len(relative_path.parts) >= 2:
            try:
                file_date = datetime.strptime(
                    relative_path.parts[-2],
                    "%Y-%m-%d",
                ).date()
            except ValueError:
                file_date = None

            if file_date and file_date < start_date:
                continue

        process_file(
            file_path,
            source_root,
            config_path,
            args.send_mode,
            args.send_limit,
            state_path,
        )


if __name__ == "__main__":
    args = parse_args()

    log_path = get_log_path_from_config(
        args.config
    )

    setup_logging(log_path)

    main(args)