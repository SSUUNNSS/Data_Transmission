from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import auto_runner  # noqa: E402


class AutoRunnerFileTests(unittest.TestCase):
    def test_mode_prefixed_file_ids_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            normal_root = root / "normal" / "Karlskrona"
            history_root = root / "history_data" / "Karlskrona"
            normal_file = normal_root / "2026-09-14" / "file.parquet"
            history_file = history_root / "2026-09-14" / "file.parquet"

            self.assertEqual(
                auto_runner.file_id(normal_file, "normal", normal_root),
                "normal/2026-09-14/file.parquet",
            )
            self.assertEqual(
                auto_runner.file_id(history_file, "history_data", history_root),
                "history_data/2026-09-14/file.parquet",
            )

    def test_completed_file_is_not_processed_again(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source_root = root / "normal" / "Karlskrona"
            file_path = source_root / "2026-09-10" / "file.parquet"
            file_path.parent.mkdir(parents=True)
            file_path.write_bytes(b"placeholder")
            config_path = root / "karlskrona_local.json"
            state_path = root / "karlskrona_processed_files.json"

            class FakeConfig:
                send_mode = auto_runner.SendMode.NORMAL

                def with_send_mode(self, mode):
                    return self

                def with_source_directory(self, directory):
                    return self

            summary = SimpleNamespace(
                messages_failed=0,
                records_read=1,
                messages_built=1,
                messages_published=1,
                messages_skipped=0,
            )

            with patch.object(auto_runner, "load_runtime_config", return_value=FakeConfig()), patch.object(
                auto_runner,
                "TransmissionRunner",
            ) as runner_class:
                runner_class.return_value.run.return_value = summary
                self.assertTrue(
                    auto_runner.process_file(
                        file_path,
                        source_root,
                        config_path,
                        "",
                        0,
                        state_path,
                    )
                )
                self.assertTrue(
                    auto_runner.process_file(
                        file_path,
                        source_root,
                        config_path,
                        "",
                        0,
                        state_path,
                    )
                )
                self.assertEqual(runner_class.return_value.run.call_count, 1)

            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                state["processed_files"]["normal/2026-09-10/file.parquet"]["status"],
                "completed",
            )

    def test_empty_state_file_is_treated_as_new_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            state_path.touch()
            self.assertEqual(auto_runner.load_state(state_path), {"processed_files": {}})


if __name__ == "__main__":
    unittest.main()
