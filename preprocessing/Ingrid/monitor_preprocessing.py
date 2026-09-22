"""监控 raw_data，新文件稳定后自动生成对应日期的 Parquet。"""

from __future__ import annotations

import argparse
import logging
import queue
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from watchdog.events import FileSystemEventHandler, FileMovedEvent
from watchdog.observers import Observer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_ROOT = PROJECT_ROOT / "src" / "sourceData" / "raw_data"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "src" / "sourceData" / "normal"
DATE_FORMAT = "%Y-%m-%d"
LOGGER = logging.getLogger("monitor_preprocessing")


class RawDataHandler(FileSystemEventHandler):
    def __init__(self, source_root: Path, jobs: queue.Queue[Path], pending: set[str], lock: threading.Lock) -> None:
        self.source_root = source_root
        self.jobs = jobs
        self.pending = pending
        self.lock = lock

    def on_created(self, event) -> None:
        if not event.is_directory:
            self.enqueue(Path(event.src_path))

    def on_moved(self, event: FileMovedEvent) -> None:
        if not event.is_directory:
            self.enqueue(Path(event.dest_path))

    def enqueue(self, file_path: Path) -> None:
        station_date = get_station_date(file_path, self.source_root)
        if station_date is None:
            return
        key = str(file_path.resolve()).casefold()
        with self.lock:
            if key in self.pending:
                return
            self.pending.add(key)
        self.jobs.put(file_path.resolve())
        LOGGER.info("detected raw file: %s", file_path)


def get_station_date(file_path: Path, source_root: Path) -> tuple[str, str] | None:
    try:
        relative_path = file_path.resolve().relative_to(source_root.resolve())
    except ValueError:
        return None
    if len(relative_path.parts) != 3:
        return None
    station, date_name, file_name = relative_path.parts
    if not file_name or Path(file_name).suffix.lower() == ".parquet":
        return None
    try:
        datetime.strptime(date_name, DATE_FORMAT)
    except ValueError:
        return None
    return station, date_name


def wait_until_stable(file_path: Path, interval: float, checks: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    previous_size = None
    stable_checks = 0
    while time.monotonic() < deadline:
        try:
            size = file_path.stat().st_size
            with file_path.open("rb") as data_file:
                data_file.read(1)
        except (FileNotFoundError, PermissionError, OSError):
            stable_checks = 0
            time.sleep(interval)
            continue
        if size == previous_size:
            stable_checks += 1
        else:
            stable_checks = 1
        previous_size = size
        if stable_checks >= checks:
            return True
        time.sleep(interval)
    return False


def process_file(
    file_path: Path,
    source_root: Path,
    output_root: Path,
    stability_interval: float,
    stability_checks: int,
    stability_timeout: float,
) -> None:
    station_date = get_station_date(file_path, source_root)
    if station_date is None:
        return
    station, date_name = station_date
    if not wait_until_stable(file_path, stability_interval, stability_checks, stability_timeout):
        LOGGER.error("file did not become stable: %s", file_path)
        return

    command = [
        sys.executable,
        str(Path(__file__).resolve().with_name("export_daily_parquet.py")),
        "--source-dir",
        str(source_root / station),
        "--output-dir",
        str(output_root),
        "--only-date",
        date_name,
        "--input-file",
        str(file_path),
        "--force",
    ]
    LOGGER.info("processing %s/%s/%s", station, date_name, file_path.name)
    result = subprocess.run(command, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.returncode == 0:
        LOGGER.info("completed %s/%s: %s", station, date_name, result.stdout.strip())
    else:
        LOGGER.error("failed %s/%s: %s", station, date_name, result.stderr.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch raw_data and preprocess new files automatically.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--stability-interval", type=float, default=2.0)
    parser.add_argument("--stability-checks", type=int, default=3)
    parser.add_argument("--stability-timeout", type=float, default=30 * 60)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_root = Path(args.source_root).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    source_root.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)

    jobs: queue.Queue[Path] = queue.Queue()
    pending: set[str] = set()
    lock = threading.Lock()

    def worker() -> None:
        while True:
            file_path = jobs.get()
            try:
                process_file(
                    file_path,
                    source_root,
                    output_root,
                    args.stability_interval,
                    args.stability_checks,
                    args.stability_timeout,
                )
            except Exception:
                LOGGER.exception("preprocessing failed for %s", file_path)
            finally:
                with lock:
                    pending.discard(str(file_path).casefold())
                jobs.task_done()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    threading.Thread(target=worker, name="preprocessing-worker", daemon=True).start()
    observer = Observer()
    observer.schedule(RawDataHandler(source_root, jobs, pending, lock), str(source_root), recursive=True)
    observer.start()
    LOGGER.info("watching %s; outputting to %s", source_root, output_root)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        LOGGER.info("stopping preprocessing monitor")
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()