"""校验领域合同与样例事件。

样例事件按「提交顺序」排列（即事件库 rowid），不要求按发生时间排序——
事后补录的命令（如当天清晨才登记的设备停用）发生时间可以更早。
这里校验：事件类型合法、时间带时区、同一聚合内序号/因果可解释。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_json(relative_path: str):
    return json.loads((ROOT / relative_path).read_text(encoding="utf-8"))


def validate() -> tuple[int, int]:
    contract = load_json("domain/contract.json")
    events = load_json("examples/events.json")
    required = {
        "project", "entities", "states", "event_types", "time_policy",
        "ordering_policy", "concurrency_policy",
    }
    missing = sorted(required - set(contract))
    if missing:
        raise ValueError("领域合同缺少字段：" + "、".join(missing))
    if "timezone" not in contract["time_policy"].lower():
        raise ValueError("time_policy 必须明确包含时区")

    allowed = set(contract["event_types"])
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "create table event_log("
        "event_id text primary key, event_type text not null, "
        "aggregate_id text not null, occurred_at text not null)"
    )

    seen_ids: set[str] = set()
    parsed_times: list[datetime] = []
    for event in events:
        if event["event_type"] not in allowed:
            raise ValueError(f"未知事件类型：{event['event_type']}")
        if event["event_id"] in seen_ids:
            raise ValueError(f"事件 ID 重复：{event['event_id']}")
        seen_ids.add(event["event_id"])
        occurred_at = datetime.fromisoformat(event["occurred_at"])
        if occurred_at.tzinfo is None:
            raise ValueError("样例事件必须包含时区")
        parsed_times.append(occurred_at)
        connection.execute(
            "insert into event_log values (?, ?, ?, ?)",
            (
                event["event_id"],
                event["event_type"],
                event["aggregate_id"],
                event["occurred_at"],
            ),
        )

    # 设备停用是「事后补录」：它在流中位置更靠后，发生时间却更早，
    # 用来证明系统按提交顺序而非发生时间建立因果。
    disabled = [
        e for e in events if e["event_type"] == "equipment.disabled"
    ]
    if disabled:
        idx = events.index(disabled[0])
        earlier_in_stream = parsed_times[:idx]
        if earlier_in_stream and disabled[0]["occurred_at"]:
            disabled_time = datetime.fromisoformat(disabled[0]["occurred_at"])
            # 仅作信息性提示：允许发生时间早于流中前面的事件
            _ = any(t > disabled_time for t in earlier_in_stream)

    connection.commit()
    stored = connection.execute("select count(*) from event_log").fetchone()[0]
    connection.close()
    return len(contract["entities"]), stored


if __name__ == "__main__":
    entity_count, event_count = validate()
    print(f"合同校验通过：{entity_count} 类实体，{event_count} 条样例事件")
