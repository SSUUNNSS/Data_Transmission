from __future__ import annotations

import tempfile
import threading
import time
import unittest
from queue import Queue
from pathlib import Path
from unittest.mock import patch

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from watch_folder import (  # noqa: E402
    FileJob,
    ParquetEventHandler,
    Station,
    StationWorker,
    identify_station,
    wait_until_file_is_stable,
)
from transmission import SendMode  # noqa: E402


class WatcherTests(unittest.TestCase):
    def test_mode_station_roots_use_mode_first_order(self) -> None:
        root = Path("C:/Data_Transmission_Ingrid/src/sourceData")
        station = Station("Karlskrona", root, root / "config.json")

        self.assertEqual(
            station.mode_source_root(SendMode.HISTORY_DATA),
            root / "history_data" / "Karlskrona",
        )
        self.assertEqual(
            station.mode_source_root(SendMode.NORMAL),
            root / "normal" / "Karlskrona",
        )

    def test_station_identification_rejects_unknown_and_malformed_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            station = Station("Karlskrona", root, root / "config.json")
            stations = {"karlskrona": station}

            valid = root / "normal" / "Karlskrona" / "2026-09-10" / "file.parquet"
            unknown = root / "normal" / "Unknown" / "2026-09-10" / "file.parquet"
            unknown_mode = root / "test" / "Karlskrona" / "2026-09-10" / "file.parquet"
            malformed = root / "Karlskrona" / "normal" / "2026-09-10" / "file.parquet"

            self.assertEqual(
                identify_station(valid, root, stations),
                (station, SendMode.NORMAL, valid.resolve()),
            )
            self.assertIsNone(identify_station(unknown, root, stations))
            self.assertIsNone(identify_station(unknown_mode, root, stations))
            self.assertIsNone(identify_station(malformed, root, stations))

    def test_stable_file_requires_repeated_identical_sizes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "file.parquet"
            path.write_bytes(b"data")
            self.assertTrue(
                wait_until_file_is_stable(
                    path,
                    check_interval=0.001,
                    required_checks=3,
                    timeout=1,
                )
            )

    def test_same_station_worker_is_sequential(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            station = Station("Karlskrona", root, root / "config.json")
            first = root / "first.parquet"
            second = root / "second.parquet"
            first.write_bytes(b"a")
            second.write_bytes(b"b")
            jobs = Queue()
            pending = {str(first).casefold(), str(second).casefold()}
            pending_lock = threading.Lock()
            stop_event = threading.Event()
            order = []

            def fake_process(job: FileJob) -> None:
                order.append(job.path.name)
                time.sleep(0.02)

            worker = StationWorker(station, jobs, Queue(), stop_event, pending, pending_lock)
            with patch.object(worker, "process", side_effect=fake_process):
                worker.start()
                jobs.put(FileJob(first, SendMode.NORMAL))
                jobs.put(FileJob(second, SendMode.NORMAL))
                jobs.join()
                stop_event.set()
                jobs.put(None)
                worker.join(timeout=1)

            self.assertEqual(order, ["first.parquet", "second.parquet"])

    def test_different_station_workers_can_run_in_parallel(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first_station = Station("Karlskrona", root, root / "first.json")
            second_station = Station("Falkoping", root, root / "second.json")
            first_jobs = Queue()
            second_jobs = Queue()
            pending = set()
            pending_lock = threading.Lock()
            stop_event = threading.Event()
            started = {"first": threading.Event(), "second": threading.Event()}

            first_worker = StationWorker(
                first_station,
                first_jobs,
                Queue(),
                stop_event,
                pending,
                pending_lock,
            )
            second_worker = StationWorker(
                second_station,
                second_jobs,
                Queue(),
                stop_event,
                pending,
                pending_lock,
            )

            def first_process(_: Path) -> None:
                started["first"].set()
                self.assertTrue(started["second"].wait(1))

            def second_process(_: Path) -> None:
                started["second"].set()
                self.assertTrue(started["first"].wait(1))

            with patch.object(first_worker, "process", side_effect=first_process), patch.object(
                second_worker,
                "process",
                side_effect=second_process,
            ):
                first_worker.start()
                second_worker.start()
                first_jobs.put(FileJob(root / "first.parquet", SendMode.NORMAL))
                second_jobs.put(FileJob(root / "second.parquet", SendMode.NORMAL))
                first_jobs.join()
                second_jobs.join()
                stop_event.set()
                first_jobs.put(None)
                second_jobs.put(None)
                first_worker.join(timeout=1)
                second_worker.join(timeout=1)

            self.assertTrue(started["first"].is_set())
            self.assertTrue(started["second"].is_set())

    def test_duplicate_events_are_queued_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            station = Station("Karlskrona", root, root / "config.json")
            path = root / "normal" / "Karlskrona" / "2026-09-10" / "file.parquet"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"data")
            jobs = {"karlskrona": (Queue(), Queue())}
            pending = set()
            handler = ParquetEventHandler(
                root,
                {"karlskrona": station},
                jobs,
                pending,
                threading.Lock(),
            )

            handler.enqueue(path)
            handler.enqueue(path)
            self.assertEqual(jobs["karlskrona"][0].qsize(), 1)


if __name__ == "__main__":
    unittest.main()
