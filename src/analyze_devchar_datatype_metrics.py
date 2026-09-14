import csv
import json
from collections import Counter, defaultdict
from pathlib import Path


PREVIEW_FILE = Path(
    "outputs/mqtt_preview/messages_preview.jsonl"
)

OUTPUT_DIR = Path(
    "outputs/mqtt_preview/devchar_datatype_analysis"
)


def main() -> None:
    if not PREVIEW_FILE.exists():
        raise FileNotFoundError(
            f"找不到 preview 文件：{PREVIEW_FILE.resolve()}"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # key:
    # (devChar, data_type)
    #
    # value:
    # 该组合下出现过的所有唯一测点
    grouped_metrics: dict[tuple[str, str], set[str]] = defaultdict(set)

    # 每个 devChar + data_type + metric 出现次数
    metric_counts: dict[
        tuple[str, str],
        Counter[str],
    ] = defaultdict(Counter)

    # 每个 devChar + data_type 对应多少条 MQTT 消息
    message_counts: Counter[tuple[str, str]] = Counter()

    total_lines = 0
    valid_messages = 0
    invalid_lines = 0

    with PREVIEW_FILE.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            total_lines += 1
            line = line.strip()

            if not line:
                continue

            try:
                preview_record = json.loads(line)
            except json.JSONDecodeError as exc:
                invalid_lines += 1
                print(
                    f"第 {line_number} 行 JSON 解析失败，已跳过：{exc}"
                )
                continue

            data_type = str(
                preview_record.get("data_type", "unknown")
            )

            message = preview_record.get("message", {})

            if not isinstance(message, dict):
                continue

            dev_char = message.get("devChar")
            data = message.get("data", {})

            if not dev_char:
                continue

            if not isinstance(data, dict):
                continue

            dev_char = str(dev_char)
            group_key = (dev_char, data_type)

            valid_messages += 1
            message_counts[group_key] += 1

            for metric in data.keys():
                metric_name = str(metric)

                grouped_metrics[group_key].add(metric_name)
                metric_counts[group_key][metric_name] += 1

    write_summary(
        grouped_metrics=grouped_metrics,
        message_counts=message_counts,
    )

    write_detail(
        metric_counts=metric_counts,
    )

    print("\n统计完成")
    print(f"preview 文件总行数：{total_lines}")
    print(f"有效 MQTT 消息数：{valid_messages}")
    print(f"无法解析的 JSON 行数：{invalid_lines}")
    print(
        "唯一 devChar + data_type 组合数："
        f"{len(grouped_metrics)}"
    )
    print(f"输出目录：{OUTPUT_DIR.resolve()}")


def write_summary(
    grouped_metrics: dict[tuple[str, str], set[str]],
    message_counts: Counter[tuple[str, str]],
) -> None:
    """
    每个 devChar + data_type 一行，
    所有测点放在 metrics 单元格中。
    """
    output_file = OUTPUT_DIR / "devchar_datatype_summary.csv"

    with output_file.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                "devChar",
                "data_type",
                "message_count",
                "unique_metric_count",
                "metrics",
            ]
        )

        for dev_char, data_type in sorted(grouped_metrics):
            group_key = (dev_char, data_type)
            metrics = sorted(grouped_metrics[group_key])

            writer.writerow(
                [
                    dev_char,
                    data_type,
                    message_counts[group_key],
                    len(metrics),
                    "; ".join(metrics),
                ]
            )

    print(f"已生成：{output_file}")


def write_detail(
    metric_counts: dict[tuple[str, str], Counter[str]],
) -> None:
    output_file = OUTPUT_DIR / "devchar_datatype_metric_detail.csv"

    with output_file.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:
        writer = csv.writer(
            file,
            delimiter=";",
        )

        writer.writerow(
            [
                "devChar",
                "data_type",
                "metric",
                "occurrence_count",
            ]
        )

        for dev_char, data_type in sorted(metric_counts):
            group_key = (dev_char, data_type)

            for metric in sorted(metric_counts[group_key]):
                writer.writerow(
                    [
                        dev_char,
                        data_type,
                        metric,
                        metric_counts[group_key][metric],
                    ]
                )

    print(f"已生成：{output_file}")


if __name__ == "__main__":
    main()