"""把 Falköping 全量数据重采样成三列干净数据，输出 CSV 与 parquet。

三列 = ts_utc / metric / value，与现有 falkoping_first5_resampled.csv 格式一致：
    - ts_utc：`YYYY-MM-DD HH:MM:SS+00:00`（空格分隔、无毫秒、0 时区）
    - metric：`测点.描述` 点分格式
    - value ：数值

重采样规则（见 FalkopingSourceReader.iter_batch_records_resampled）：
    每个 (metric, 5 分钟桶) 只保留时间最小的一条，全量 528 万行 → 约 19 万行。

用法（在项目根目录下）：
    .venv\\Scripts\\python.exe preprocessing\\Ingrid\\export_clean_data.py
"""

from __future__ import annotations

import csv
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from preprocessing_utils import iter_resampled, list_batches

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "src" / "sourceData" / "normal" / "falkoping"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "preprocessing"
CSV_PATH = OUTPUT_DIR / "falkoping_resampled_full.csv"
PARQUET_PATH = OUTPUT_DIR / "falkoping_resampled_full.parquet"


def _format_ts(ts) -> str:
    """时间戳 → `YYYY-MM-DD HH:MM:SS+00:00`（与现有 resampled csv 一致）。"""
    return ts.isoformat(sep=" ", timespec="seconds")


def _to_micros(ts) -> int:
    """时间戳 → UTC 微秒整数（parquet timestamp[us, tz=UTC]）。"""
    return int(ts.timestamp() * 1_000_000)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    batches = list_batches(SOURCE_DIR, batch_by_subdirectory=True)
    if not batches:
        raise SystemExit(f"No source files found in {SOURCE_DIR}")

    # 三列 parquet 的 schema，对齐 octopus 的 parquet（timestamp[us, tz=UTC] / string / double）。
    schema = pa.schema(
        [
            pa.field("ts_utc", pa.timestamp("us", tz="UTC")),
            pa.field("metric", pa.string()),
            pa.field("value", pa.float64()),
        ]
    )

    writer = pq.ParquetWriter(PARQUET_PATH, schema)
    ts_list: list[int] = []
    metric_list: list[str] = []
    value_list: list[float] = []
    total = 0

    CHUNK_SIZE = 200_000

    def flush() -> None:
        nonlocal ts_list, metric_list, value_list
        if not ts_list:
            return
        table = pa.table(
            {
                "ts_utc": pa.array(ts_list, type=pa.timestamp("us", tz="UTC")),
                "metric": pa.array(metric_list, type=pa.string()),
                "value": pa.array(value_list, type=pa.float64()),
            }
        )
        writer.write_table(table)
        ts_list, metric_list, value_list = [], [], []

    try:
        with CSV_PATH.open("w", encoding="utf-8", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(["ts_utc", "metric", "value"])

            for batch in batches:
                for record in iter_resampled(batch):
                    ts_str = _format_ts(record.ts_utc)
                    csv_writer.writerow([ts_str, record.metric, record.value])

                    ts_list.append(_to_micros(record.ts_utc))
                    metric_list.append(str(record.metric))
                    value_list.append(float(record.value))
                    total += 1

                    if len(ts_list) >= CHUNK_SIZE:
                        flush()
                        print(f"processed {total} rows...")

            flush()
    finally:
        writer.close()

    csv_mb = CSV_PATH.stat().st_size / (1024 * 1024)
    pq_mb = PARQUET_PATH.stat().st_size / (1024 * 1024)
    print(f"Done. {total} rows ->")
    print(f"  CSV:     {CSV_PATH} ({csv_mb:.1f} MB)")
    print(f"  Parquet: {PARQUET_PATH} ({pq_mb:.1f} MB)")


if __name__ == "__main__":
    main()
