"""消息构建：把原始记录（SourceRecord）聚合、映射成 MQTT 消息（BuiltMessage）。

核心思路：
1. 拆分 metric 字段，得到「完整测点名 metric_key」和「数据键 data_key」
2. 从 metric_key 的层级结构推断候选 Device Type
3. 用 data_key + 候选 Device Type 从 mapping 中精确匹配数据类型
4. 根据设备层级从 metric_key 动态生成 devChar
5. 把「同一时间 + 同一设备 + 同一类型」的记录归并为一条消息
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from mqtt import DataType

from .config import MessageConfig
from .source_base import SourceRecord


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedMetric:
    """metric 字段拆分结果：完整测点名和数据键。"""

    metric_key: str
    data_key: str


@dataclass(frozen=True)
class BuiltMessage:
    """一条构建完成的 MQTT 消息：时间、设备、类型，以及最终 payload。"""

    ts_utc: datetime
    dev_char: str
    data_type: DataType
    payload: dict[str, Any]


def parse_metric(metric: Any) -> Optional[ParsedMetric]:
    """按最后一个 '.' 拆分 Falköping metric。

    例如：
      metric = "FKP1_S1_Ba_CMU1_POS_MAX_U_BMU_MV.Position max spänning cell (#BMU)"

      rsplit(".", 1) →
        metric_key = "FKP1_S1_Ba_CMU1_POS_MAX_U_BMU_MV"
        data_key = "Position max spänning cell (#BMU)"

    data_key 用于查 data_type_mapping.json；
    metric_key 用于判断设备层级并生成 devChar；
    data_key 同时保持原样写入 MQTT payload 的 data。
    """
    if metric is None:
        return None

    metric_text = str(metric).strip()
    if "." not in metric_text:
        return None

    metric_key, data_key = metric_text.rsplit(".", 1)
    metric_key = metric_key.strip()
    data_key = data_key.strip()

    if not metric_key or not data_key:
        return None

    return ParsedMetric(metric_key=metric_key, data_key=data_key)


def infer_device_type(metric_key: str) -> Optional[str]:
    """根据 Falköping metric_key 的实际层级结构推断候选 Device Type。

    优先级：
    1. 包含 _CMU数字_           -> Rack
    2. 包含 _S数字_MV_         -> PCS
    3. 包含 _S数字_Ba/Bb_      -> BSC
    4. 至少包含 _S数字         -> LC

    S1/S2、Ba/Bb、CMU1/2/... 均动态识别，不写死。
    """
    if re.search(r"_S\d+_(?:Ba|Bb)_CMU\d+(?:_|$)", metric_key, flags=re.IGNORECASE):
        return "Rack"

    if re.search(r"_S\d+_MV(?:_|$)", metric_key, flags=re.IGNORECASE):
        return "PCS"

    if re.search(r"_S\d+_(?:Ba|Bb)(?:_|$)", metric_key, flags=re.IGNORECASE):
        return "BSC"

    if re.search(r"_S\d+(?:_|$)", metric_key, flags=re.IGNORECASE):
        return "LC"

    return None


def build_dev_char(metric_key: str, device_type: Any) -> Optional[str]:
    """根据 mapping 中的 Device Type，从原始 metric_key 动态生成 devChar。

    RACK -> 截断到 Sx_Ba/Bb_CMU数字
    PCS  -> 截断到 Sx_MV
    LC   -> 截断到 Sx
    BSC  -> 截断到 Sx_Ba/Bb

    S 编号、Ba/Bb、CMU 编号都从原始 metric 动态提取，不写死。
    """
    normalized_device_type = str(device_type).strip().upper()

    patterns = {
        "RACK": r"^(.*?_S\d+_(?:Ba|Bb)_CMU\d+)(?:_|$)",
        "PCS": r"^(.*?_S\d+_MV)(?:_|$)",
        "LC": r"^(.*?_S\d+)(?:_|$)",
        "BSC": r"^(.*?_S\d+_(?:Ba|Bb))(?:_|$)",
    }

    pattern = patterns.get(normalized_device_type)
    if pattern is None:
        return None

    match = re.match(pattern, metric_key, flags=re.IGNORECASE)
    if match is None:
        return None

    return match.group(1)


def normalize_timestamp(value: Any) -> Optional[datetime]:
    """把时间值统一为带 UTC 时区的 datetime；缺失或非 datetime 类型返回 None。"""
    if value is None:
        return None

    if isinstance(value, datetime):
        timestamp = value
    else:
        return None

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    return timestamp.astimezone(timezone.utc)


def to_unix_timestamp_seconds(timestamp: datetime) -> int:
    """把 datetime 转成 Unix 秒（整数）。"""
    return int(timestamp.timestamp())


def normalize_mapping_type(value: Any) -> Optional[DataType]:
    """把映射表里的 type 字符串归一化为 DataType 枚举；不支持的取值返回 None。"""
    normalized = str(value).strip().lower()
    if normalized == "telemetry":
        return DataType.TELEMETRY
    if normalized == "telesignaling":
        return DataType.TELESIGNALING
    return None


def load_data_type_mapping(mapping_path: Optional[Path]) -> dict[str, list[dict[str, Any]]]:
    """加载 data_type_mapping.json。"""
    if mapping_path is None:
        return {}

    with mapping_path.open("r", encoding="utf-8") as mapping_file:
        data = json.load(mapping_file)

    if not isinstance(data, dict):
        raise ValueError("Data type mapping must be a JSON object.")

    mapping: dict[str, list[dict[str, Any]]] = {}
    for metric_key, entries in data.items():
        if not isinstance(entries, list):
            raise ValueError(f"Data type mapping for '{metric_key}' must be an array.")
        normalized_entries = []
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError(f"Data type mapping entry for '{metric_key}' must be an object.")
            normalized_entries.append(entry)
        mapping[str(metric_key)] = normalized_entries
    return mapping


class MessageBuilder:
    """把原始记录聚合为 MQTT 消息。"""

    def __init__(self, config: MessageConfig, logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.logger = logger or LOGGER
        self._groups: dict[tuple[datetime, str, DataType], dict[str, Any]] = {}
        self._data_type_mapping = load_data_type_mapping(config.data_type_mapping_path)

    def add_record(self, record: SourceRecord) -> None:
        """处理一条原始记录：校验、拆分、映射、生成 devChar，然后归并。"""
        timestamp = normalize_timestamp(record.ts_utc)
        if timestamp is None:
            self.logger.warning(
                "Skipping row because ts_utc is missing or invalid.",
                extra={"file": str(record.file_path), "row_group": record.row_group_index, "row": record.row_index},
            )
            return

        parsed_metric = parse_metric(record.metric)
        if parsed_metric is None:
            self.logger.warning(
                "Skipping row because metric cannot be split.",
                extra={
                    "file": str(record.file_path),
                    "row_group": record.row_group_index,
                    "row": record.row_index,
                    "metric": str(record.metric),
                },
            )
            return

        data_key = parsed_metric.data_key

        candidate_device_type = infer_device_type(parsed_metric.metric_key)

        mapping_result = self.resolve_mapping(
            record,
            parsed_metric,
            candidate_device_type,
        )

        if mapping_result is None:
            data_type = DataType(self.config.default_data_type)
            device_type = candidate_device_type
        else:
            data_type, mapped_device_type = mapping_result
            # devChar 的设备层级优先采用 metric 本身推断出的实际结构。
            # 这样 CMU1/CMU2 等层级不会因为 mapping 中重复 data_key 而丢失。
            device_type = candidate_device_type or mapped_device_type

        if device_type:
            dev_char = build_dev_char(parsed_metric.metric_key, device_type)
        else:
            dev_char = None

        if dev_char is None:
            self.logger.warning(
                "Could not derive devChar from metric; using full metric key.",
                extra={
                    "file": str(record.file_path),
                    "row_group": record.row_group_index,
                    "row": record.row_index,
                    "metric_key": parsed_metric.metric_key,
                    "data_key": parsed_metric.data_key,
                    "candidate_device_type": str(candidate_device_type),
                },
            )
            dev_char = parsed_metric.metric_key

        # 同一「时间 + 设备 + 类型」的记录归入同一分组。
        group_key = (timestamp, dev_char, data_type)
        data = self._groups.setdefault(group_key, {})

        if data_key in data:
            self.logger.warning(
                "Duplicate metric key in aggregation group; later value overwrites earlier value.",
                extra={
                    "file": str(record.file_path),
                    "row_group": record.row_group_index,
                    "row": record.row_index,
                    "timestamp": timestamp.isoformat(),
                    "dev_char": dev_char,
                    "data_key": data_key,
                },
            )

        data[data_key] = record.value

    def build_messages(self) -> list[BuiltMessage]:
        """把聚合好的分组打包成消息列表，并按时间、设备、类型排序。"""
        messages = []
        for (timestamp, dev_char, data_type), data in self._groups.items():
            payload = {
                "timestamp": to_unix_timestamp_seconds(timestamp),
                "psId": self.config.ps_id,
                "appkey": self.config.app_key,
                "source": self.config.source,
                "devChar": dev_char,
                "data": data,
            }
            messages.append(BuiltMessage(timestamp, dev_char, data_type, payload))

        return sorted(messages, key=lambda message: (message.ts_utc, message.dev_char, message.data_type.value))

    def resolve_mapping(
        self,
        record: SourceRecord,
        parsed_metric: ParsedMetric,
        candidate_device_type: Optional[str],
    ) -> Optional[tuple[DataType, str]]:
        """用 data_key 查 mapping，并结合 metric 推断出的 Device Type 选择正确 entry。

        若同一个 data_key 对应多个 Device Type：
        - 优先匹配 candidate_device_type；
        - 若没有匹配项，但所有有效 entry 的 data type 完全一致，
          仍可安全使用这个共同的 data type。
        """
        entries = self._data_type_mapping.get(parsed_metric.data_key, [])
        if not entries:
            return None

        valid_entries: list[tuple[DataType, str]] = []

        for entry in entries:
            data_type = normalize_mapping_type(entry.get("type"))
            device_type = str(entry.get("Device Type", "")).strip()

            if data_type is None or not device_type:
                continue

            valid_entries.append((data_type, device_type))

            if (
                candidate_device_type
                and device_type.casefold() == candidate_device_type.casefold()
            ):
                return data_type, device_type

        if not valid_entries:
            return None

        # 如果没有 Device Type 精确命中，但所有 entry 都是同一种数据类型，
        # 则 data_type 仍然没有歧义。
        unique_data_types = {data_type for data_type, _ in valid_entries}
        if len(unique_data_types) == 1:
            common_data_type = valid_entries[0][0]
            fallback_device_type = candidate_device_type or valid_entries[0][1]

            self.logger.warning(
                "No mapping entry matched inferred Device Type; using common mapped data type.",
                extra={
                    "file": str(record.file_path),
                    "row_group": record.row_group_index,
                    "row": record.row_index,
                    "metric_key": parsed_metric.metric_key,
                    "data_key": parsed_metric.data_key,
                    "candidate_device_type": str(candidate_device_type),
                    "mapped_device_types": ",".join(device for _, device in valid_entries),
                    "data_type": common_data_type.value,
                },
            )
            return common_data_type, fallback_device_type

        self.logger.warning(
            "Ambiguous mapping: no Device Type match and mapped data types differ; using default data type.",
            extra={
                "file": str(record.file_path),
                "row_group": record.row_group_index,
                "row": record.row_index,
                "metric_key": parsed_metric.metric_key,
                "data_key": parsed_metric.data_key,
                "candidate_device_type": str(candidate_device_type),
            },
        )
        return None
