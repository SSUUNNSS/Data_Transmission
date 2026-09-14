from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mqtt import DataType  # noqa: E402
from transmission.config import SendMode  # noqa: E402
from transmission.runner import BuildResult, TransmissionRunner  # noqa: E402
from transmission.message_builder import BuiltMessage  # noqa: E402


class FakePublisher:
    def __init__(self, config, logger):
        self.published = []

    def connect(self):
        return None

    def publish(self, payload, data_type, topic_override=None):
        self.published.append(payload)
        return SimpleNamespace(success=True)

    def disconnect(self):
        return None


class ResumableRunnerTests(unittest.TestCase):
    def test_history_pauses_at_checkpoint_and_resumes_from_offset(self) -> None:
        messages = [
            BuiltMessage(
                ts_utc=None,
                dev_char=f"device-{index}",
                data_type=DataType.TELEMETRY,
                payload={"index": index},
            )
            for index in range(4)
        ]
        config = SimpleNamespace(
            send_mode=SendMode.HISTORY_DATA,
            mqtt=SimpleNamespace(
                app_key="test-app",
                topics=SimpleNamespace(get_history_data_topic=lambda _: "history"),
            ),
        )
        runner = TransmissionRunner(config)
        runner.build_message_batches = lambda: [
            BuildResult("file", ["file.parquet"], 4, messages)
        ]
        checkpoints = []
        should_pause = lambda: True

        with patch("transmission.runner.MqttPublisher", FakePublisher):
            paused = runner.run_resumable(
                checkpoint_messages=2,
                should_pause=should_pause,
                checkpoint_callback=checkpoints.append,
            )

        self.assertEqual(paused.status, "paused")
        self.assertEqual(paused.next_offset, 2)
        self.assertEqual([checkpoint.next_offset for checkpoint in checkpoints], [0, 2, 2])

        resumed_checkpoints = []
        with patch("transmission.runner.MqttPublisher", FakePublisher):
            completed = runner.run_resumable(
                start_offset=paused.next_offset,
                checkpoint_messages=2,
                checkpoint_callback=resumed_checkpoints.append,
            )

        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.next_offset, 4)
        self.assertEqual(completed.messages_published, 2)


if __name__ == "__main__":
    unittest.main()
