"""不可变事件与确定性序列化。

系统状态完全由事件流折叠得到；任何命令的结果都是「追加若干事件」。
同一段事件流重放任意次必须得到相同状态，因此：

- 事件载荷只放数据，不放运行时对象；
- JSON 序列化固定分隔符与键顺序；
- 事件 ID 由「聚合 ID + 聚合内序号」派生，重放不会产生新 ID。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

ENVELOPE_VERSION = 1

# 预约生命周期
RESERVATION_REQUESTED = "reservation.requested"
RESERVATION_REVISED = "reservation.revised"
RESERVATION_SCHEDULED = "reservation.scheduled"
RESERVATION_WAITLISTED = "reservation.waitlisted"
RESERVATION_PROMOTED = "reservation.promoted"
RESERVATION_REJECTED = "reservation.rejected"
RESERVATION_ARRIVED = "vehicle.arrived"
RESERVATION_PROCESSING = "reservation.processing"
RESERVATION_COMPLETED = "reservation.completed"
RESERVATION_CANCELLED = "reservation.cancelled"
# 人工插单：记录批准人与被延后批次
RESERVATION_OVERRIDDEN = "reservation.overridden"
# 设备停用/恢复
EQUIPMENT_DISABLED = "equipment.disabled"
EQUIPMENT_ENABLED = "equipment.enabled"
# 候补超时告警
ALARM_RAISED = "alarm.raised"
ALARM_CLEARED = "alarm.cleared"

RESERVATION_EVENTS = frozenset(
    {
        RESERVATION_REQUESTED,
        RESERVATION_REVISED,
        RESERVATION_SCHEDULED,
        RESERVATION_WAITLISTED,
        RESERVATION_PROMOTED,
        RESERVATION_REJECTED,
        RESERVATION_ARRIVED,
        RESERVATION_PROCESSING,
        RESERVATION_COMPLETED,
        RESERVATION_CANCELLED,
        RESERVATION_OVERRIDDEN,
    }
)


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    aggregate_id: str
    aggregate_type: str
    seq: int
    occurred_at: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "seq": self.seq,
            "occurred_at": self.occurred_at,
            "payload": self.payload,
        }

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            aggregate_id=data["aggregate_id"],
            aggregate_type=data["aggregate_type"],
            seq=data["seq"],
            occurred_at=data["occurred_at"],
            payload=dict(data.get("payload", {})),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Event":
        return cls.from_dict(json.loads(raw))


def make_event(
    event_type: str,
    aggregate_id: str,
    seq: int,
    occurred_at: str,
    payload: dict[str, Any],
    aggregate_type: str = "reservation",
) -> Event:
    """按聚合 ID 与聚合内序号派生确定性事件 ID。"""
    digest = hashlib.sha256(
        json.dumps(
            [aggregate_type, aggregate_id, seq, event_type, payload],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]
    return Event(
        event_id=f"{aggregate_type}:{aggregate_id}:{seq}:{digest}",
        event_type=event_type,
        aggregate_id=aggregate_id,
        aggregate_type=aggregate_type,
        seq=seq,
        occurred_at=occurred_at,
        payload=payload,
    )
