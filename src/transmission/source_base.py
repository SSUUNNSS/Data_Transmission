"""数据源基础结构：跨站点共享的原始记录与批次定义。

所有站点的原始数据（parquet / CSV / 分号分隔文本等）经过各自
source_<站点>.py 的读取后，都归一化成统一的三列结构：
SourceRecord(ts_utc, metric, value)。

本模块只放共享的数据结构，不放任何站点特有的读取逻辑；
每个站点一个 source_<站点>.py 文件，负责把本站点的原始格式转成三列。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SourceRecord:
    """一条归一化后的原始数据记录，含来源定位信息与三个业务字段。

    ts_utc  : 时间（datetime）
    metric  : 测点名
    value   : 测值
    """

    file_path: Path
    row_group_index: int
    row_index: int
    ts_utc: Any
    metric: Any
    value: Any


@dataclass(frozen=True)
class SourceBatch:
    """一批待处理的文件（例如一个子目录下的所有数据文件）。"""

    name: str
    files: list[Path]
