from __future__ import annotations

import argparse
import logging
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from watchdog.events import FileSystemEventHandler, FileMovedEvent
from watchdog.observers import Observer

from auto_runner import PROJECT_ROOT, file_id, get_state_path, load_state, process_file
from main import setup_logging
from transmission import SendMode


SOURCE_ROOT = PROJECT_ROOT / "src" / "sourceData"
STABILITY_CHECK_SECONDS = 2.0
STABLE_SIZE_CHECKS = 3
STABILITY_TIMEOUT_SECONDS = 30 * 60
HISTORY_CHECKPOINT_MESSAGES = 500

STATIONS = {
    "Karlskrona": PROJECT_ROOT / "config" / "karlskrona_local.json",
    "Falkoping": PROJECT_ROOT / "config" / "falkoping_local.json",
    "Sandviken": PROJECT_ROOT / "config" / "sandviken_local.json",
    "Vaggeryd": PROJECT_ROOT / "config" / "vaggeryd_local.json",
    "Varnamo": PROJECT_ROOT / "config" / "varnamo_local.json",
    "Vasteras": PROJECT_ROOT / "config" / "vasteras_local.json",
    "Mjolby": PROJECT_ROOT / "config" / "mjolby_local.json",
    "Katrineholm": PROJECT_ROOT / "config" / "katrineholm_local.json",
    "Varberg": PROJECT_ROOT / "config" / "varberg_local.json",
    "Test": PROJECT_ROOT / "config" / "test_local.json",
}

LOGGER = logging.getLogger("watch_folder")


@dataclass(frozen=True)
class Station:
    name: str
    global_source_root: Path
    config_path: Path

    def mode_source_root(self, mode: SendMode) -> Path:
        return self.global_source_root / mode.value / self.name


@dataclass(frozen=True)
class FileJob:
    path: Path
    mode: SendMode


class StationWorker(threading.Thread):
    def __init__(
        self,
        station: Station,
        normal_jobs: queue.Queue[Optional[FileJob]],
        history_jobs: queue.Queue[Optional[FileJob]],
        stop_event: threading.Event,
        pending_files: set[str],
        pending_lock: threading.Lock,
        checkpoint_messages: int = HISTORY_CHECKPOINT_MESSAGES,
    ) -> None:
        super().__init__(name=f"station-{station.name}", daemon=True)
        self.station = station
        self.normal_jobs = normal_jobs
        self.history_jobs = history_jobs
        self.stop_event = stop_event
        self.pending_files = pending_files
        self.pending_lock = pending_lock
        self.checkpoint_messages = checkpoint_messages

    def run(self) -> None:
        LOGGER.info("[%s] worker started", self.station.name)
        while not self.stop_event.is_set():
            job = self.next_job()
            if job is None:
                continue
            if job.path == Path():
                if job.mode == SendMode.NORMAL:
                    self.normal_jobs.task_done()
                else:
                    self.history_jobs.task_done()
                break

            key = str(job.path).casefold()
            try:
                status = self.process(job)
                if status == "paused":
                    self.history_jobs.put(job)
                else:
                    with self.pending_lock:
                        self.pending_files.discard(key)
            except Exception:
                LOGGER.exception("[%s] worker exception for %s", self.station.name, job.path)
                with self.pending_lock:
                    self.pending_files.discard(key)
            finally:
                if job.mode == SendMode.NORMAL:
                    self.normal_jobs.task_done()
                else:
                    self.history_jobs.task_done()

        LOGGER.info("[%s] worker stopped", self.station.name)

    def next_job(self) -> Optional[FileJob]:
        try:
            job = self.normal_jobs.get_nowait()
            if job is None:
                return FileJob(Path(), SendMode.NORMAL)
            return job
        except queue.Empty:
            pass
        try:
            job = self.history_jobs.get(timeout=0.5)
            if job is None:
                return FileJob(Path(), SendMode.HISTORY_DATA)
            return job
        except queue.Empty:
            return None

    def has_normal_pending(self) -> bool:
        return not self.normal_jobs.empty()

    def process(self, job: FileJob) -> str | bool:
        file_path = job.path
        mode = job.mode
        mode_source_root = self.station.mode_source_root(mode)
        LOGGER.info("[%s][%s] waiting for file stability: %s", self.station.name, mode.value, file_path)
        if not wait_until_file_is_stable(file_path):
            LOGGER.error("[%s][%s] file did not become stable: %s", self.station.name, mode.value, file_path)
            return False

        LOGGER.info("[%s][%s] processing %s", self.station.name, mode.value, file_path.name)
        return process_file(
            file_path=file_path,
            source_root=mode_source_root,
            config_path=self.station.config_path,
            send_mode=mode.value,
            send_limit=0,
            state_path=get_state_path(self.station.config_path),
            should_pause=self.has_normal_pending if mode == SendMode.HISTORY_DATA else None,
            checkpoint_messages=self.checkpoint_messages,
        )


def build_stations(source_root: Path = SOURCE_ROOT) -> dict[str, Station]:
    stations = {}
    for canonical_name, config_path in STATIONS.items():
        stations[canonical_name.casefold()] = Station(
            name=canonical_name,
            global_source_root=source_root,
            config_path=config_path,
        )
    return stations


def identify_station(
    path: Path,
    source_root: Path,
    stations: dict[str, Station],
) -> tuple[Station, SendMode, Path] | None:
    try:
        relative_path = path.resolve().relative_to(source_root.resolve())
    except ValueError:
        LOGGER.warning("[WATCHER] ignoring path outside source root: %s", path)
        return None

    if len(relative_path.parts) != 4:
        LOGGER.warning("[WATCHER] ignoring malformed parquet path: %s", relative_path)
        return None

    try:
        mode = SendMode(relative_path.parts[0].casefold())
    except ValueError:
        LOGGER.warning("[WATCHER] ignoring unsupported send mode: %s", relative_path.parts[0])
        return None

    station = stations.get(relative_path.parts[1].casefold())
    if station is None:
        LOGGER.warning("[WATCHER] unknown station: %s", relative_path.parts[1])
        return None

    try:
        datetime.strptime(relative_path.parts[2], "%Y-%m-%d")
    except ValueError:
        LOGGER.warning("[WATCHER] ignoring path with invalid date directory: %s", relative_path)
        return None

    if path.suffix.casefold() != ".parquet":
        return None
    return station, mode, path.resolve()


def wait_until_file_is_stable(
    file_path: Path,
    check_interval: float = STABILITY_CHECK_SECONDS,
    required_checks: int = STABLE_SIZE_CHECKS,
    timeout: float = STABILITY_TIMEOUT_SECONDS,
) -> bool:
    deadline = time.monotonic() + timeout
    previous_size = None
    stable_checks = 0

    while time.monotonic() < deadline:
        try:
            size = file_path.stat().st_size
            with file_path.open("rb") as file:
                file.read(1)
        except (FileNotFoundError, PermissionError, OSError):
            stable_checks = 0
            time.sleep(check_interval)
            continue

        if size == previous_size:
            stable_checks += 1
        else:
            stable_checks = 1
        previous_size = size

        if stable_checks >= required_checks:
            return True
        time.sleep(check_interval)

    return False


class ParquetEventHandler(FileSystemEventHandler):
    def __init__(
        self,
        source_root: Path,
        stations: dict[str, Station],
        station_queues: dict[str, tuple[queue.Queue[Optional[FileJob]], queue.Queue[Optional[FileJob]]]],
        pending_files: set[str],
        pending_lock: threading.Lock,
    ) -> None:
        self.source_root = source_root
        self.stations = stations
        self.station_queues = station_queues
        self.pending_files = pending_files
        self.pending_lock = pending_lock

    def on_created(self, event) -> None:
        if not event.is_directory:
            self.enqueue(Path(event.src_path))

    def on_moved(self, event: FileMovedEvent) -> None:
        if not event.is_directory:
            self.enqueue(Path(event.dest_path))

    def enqueue(self, path: Path) -> None:
        if path.suffix.casefold() != ".parquet":
            return

        identified = identify_station(path, self.source_root, self.stations)
        if identified is None:
            return
        station, mode, resolved_path = identified

        try:
            relative_id = file_id(
                resolved_path,
                mode,
                station.mode_source_root(mode),
            )
            state = load_state(get_state_path(station.config_path))
            completed = state["processed_files"].get(relative_id)
            if isinstance(completed, dict) and completed.get("status") == "completed":
                LOGGER.info("[%s] completed file event ignored: %s", station.name, relative_id)
                return
        except (OSError, ValueError) as error:
            LOGGER.error("[%s] could not inspect processed-file state: %s", station.name, error)
            return

        key = str(resolved_path).casefold()
        with self.pending_lock:
            if key in self.pending_files:
                LOGGER.info("[WATCHER] duplicate event ignored: %s", resolved_path)
                return
            self.pending_files.add(key)

        LOGGER.info(
            "[WATCHER] detected %s",
            resolved_path.relative_to(self.source_root.resolve()),
        )
        job = FileJob(resolved_path, mode)
        normal_queue, history_queue = self.station_queues[station.name.casefold()]
        (normal_queue if mode == SendMode.NORMAL else history_queue).put(job)
        LOGGER.info("[%s][%s] queued %s", station.name, mode.value, resolved_path.name)


def run_watcher(
    source_root: Path = SOURCE_ROOT,
    checkpoint_messages: int = HISTORY_CHECKPOINT_MESSAGES,
) -> None:
    source_root = source_root.resolve()
    stations = build_stations(source_root)
    station_queues = {
        key: (queue.Queue(), queue.Queue())
        for key in stations
    }
    pending_files: set[str] = set()
    pending_lock = threading.Lock()
    stop_event = threading.Event()
    workers = [
        StationWorker(
            station,
            station_queues[key][0],
            station_queues[key][1],
            stop_event,
            pending_files,
            pending_lock,
            checkpoint_messages,
        )
        for key, station in stations.items()
    ]

    for key, station in stations.items():
        try:
            state = load_state(get_state_path(station.config_path))
        except (OSError, ValueError) as error:
            LOGGER.error("[%s] could not recover state: %s", station.name, error)
            continue
        for relative_id, entry in state.get("processed_files", {}).items():
            if not isinstance(entry, dict):
                continue
            if entry.get("status") not in {"paused", "in_progress", "failed"}:
                continue
            if entry.get("send_mode") != SendMode.HISTORY_DATA.value:
                continue
            mode_relative_id = relative_id.removeprefix("history_data/")
            history_path = station.mode_source_root(SendMode.HISTORY_DATA) / mode_relative_id
            if history_path.is_file():
                with pending_lock:
                    pending_files.add(str(history_path.resolve()).casefold())
                station_queues[key][1].put(
                    FileJob(history_path.resolve(), SendMode.HISTORY_DATA)
                )
                LOGGER.info(
                    "[%s][history_data] recovered %s from offset %s",
                    station.name,
                    relative_id,
                    entry.get("next_offset", 0),
                )

    LOGGER.info("[WATCHER] starting; monitoring %s", source_root)
    for worker in workers:
        worker.start()

    observer = Observer()
    observer.schedule(
        ParquetEventHandler(source_root, stations, station_queues, pending_files, pending_lock),
        str(source_root),
        recursive=True,
    )
    observer.start()
    LOGGER.info("[WATCHER] running")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        LOGGER.info("[WATCHER] shutting down")
    finally:
        observer.stop()
        observer.join()
        stop_event.set()
        for normal_queue, history_queue in station_queues.values():
            normal_queue.put(None)
            history_queue.put(None)
        for worker in workers:
            worker.join()
        LOGGER.info("[WATCHER] stopped")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Watch sourceData and process newly completed parquet files."
    )
    parser.add_argument(
        "--source-root",
        default=str(SOURCE_ROOT),
        help="Root directory containing station directories.",
    )
    parser.add_argument(
        "--log-file",
        default=str(PROJECT_ROOT / "logs" / "watch_folder.log"),
        help="Watcher log file.",
    )
    parser.add_argument(
        "--history-checkpoint-messages",
        type=int,
        default=HISTORY_CHECKPOINT_MESSAGES,
        help="History messages per cooperative pause checkpoint.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    setup_logging(Path(args.log_file).expanduser().resolve())
    run_watcher(
        Path(args.source_root).expanduser().resolve(),
        checkpoint_messages=args.history_checkpoint_messages,
    )
