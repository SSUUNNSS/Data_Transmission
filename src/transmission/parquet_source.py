from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pyarrow.parquet as pq


REQUIRED_COLUMNS = ("ts_utc", "metric", "value")


@dataclass(frozen=True)
class SourceRecord:
    file_path: Path
    row_group_index: int
    row_index: int
    ts_utc: Any
    metric: Any
    value: Any


@dataclass(frozen=True)
class SourceBatch:
    name: str
    files: list[Path]


class ParquetSourceReader:
    def __init__(
        self,
        directory: Path,
        file_pattern: str = "*.parquet",
        files: tuple[Path, ...] = (),
        batch_by_subdirectory: bool = False,
    ) -> None:
        self.directory = directory
        self.file_pattern = file_pattern
        self.files = files
        self.batch_by_subdirectory = batch_by_subdirectory

    def list_files(self) -> list[Path]:
        batches = self.list_batches()
        return [file_path for batch in batches for file_path in batch.files]

    def list_batches(self) -> list[SourceBatch]:
        if self.files:
            missing_files = [file_path for file_path in self.files if not file_path.is_file()]
            if missing_files:
                raise FileNotFoundError(f"Configured source files do not exist: {missing_files}")
            return [SourceBatch(name="configured-files", files=sorted(self.files))]

        if self.directory.is_file():
            return [SourceBatch(name=self.directory.stem, files=[self.directory])]

        if self.batch_by_subdirectory:
            batches = []
            for child_directory in sorted(path for path in self.directory.iterdir() if path.is_dir()):
                files = sorted(child_directory.glob(self.file_pattern))
                if files:
                    batches.append(SourceBatch(name=child_directory.name, files=files))
            if not batches:
                raise FileNotFoundError(
                    f"No files matching {self.file_pattern!r} were found in subdirectories of {self.directory}."
                )
            return batches

        files = sorted(self.directory.glob(self.file_pattern))
        if not files:
            raise FileNotFoundError(f"No files matching {self.file_pattern!r} were found in {self.directory}.")
        return [SourceBatch(name=self.directory.name, files=files)]

    def iter_records(self) -> Iterator[SourceRecord]:
        for batch in self.list_batches():
            yield from self.iter_batch_records(batch)

    def iter_batch_records(self, batch: SourceBatch) -> Iterator[SourceRecord]:
        for file_path in batch.files:
            parquet_file = pq.ParquetFile(file_path)
            missing_columns = [column for column in REQUIRED_COLUMNS if column not in parquet_file.schema_arrow.names]
            if missing_columns:
                raise ValueError(f"Parquet file {file_path} is missing columns: {missing_columns}")

            for row_group_index in range(parquet_file.metadata.num_row_groups):
                table = parquet_file.read_row_group(row_group_index, columns=list(REQUIRED_COLUMNS))
                rows = table.to_pylist()
                for row_index, row in enumerate(rows):
                    yield SourceRecord(
                        file_path=file_path,
                        row_group_index=row_group_index,
                        row_index=row_index,
                        ts_utc=row.get("ts_utc"),
                        metric=row.get("metric"),
                        value=row.get("value"),
                    )
