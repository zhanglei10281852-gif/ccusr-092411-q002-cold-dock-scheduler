"""事件定义。

所有状态变化都以不可变事件落库。事件载荷只使用可 JSON 序列化的类型，
时间同时保存 epoch minute（计算用）与 ISO 8601 字符串（审计用）。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

# ---- 事件类型（contract v2 以新版本追加，不修改 v1） ----
RESERVATION_REQUESTED = "reservation.requested"
RESERVATION_SCHEDULED = "reservation.scheduled"
RESERVATION_WAITLISTED = "reservation.waitlisted"
RESERVATION_REJECTED = "reservation.rejected"
RESERVATION_RESCHEDULED = "reservation.rescheduled"
RESERVATION_POSTPONED = "reservation.postponed"
RESERVATION_AMENDED = "reservation.amended"
RESERVATION_MANUAL_INSERTED = "reservation.manual_inserted"
WAITLIST_ACCEPTED = "waitlist.accepted"
VEHICLE_ARRIVED = "vehicle.arrived"
OPERATION_STARTED = "operation.started"
RESERVATION_COMPLETED = "reservation.completed"
RESERVATION_CANCELLED = "reservation.cancelled"
EQUIPMENT_DISABLED = "equipment.disabled"
EQUIPMENT_ENABLED = "equipment.enabled"
GOODS_RELEASED = "goods.released"
ALERT_RAISED = "alert.raised"
ALERT_RESOLVED = "alert.resolved"

ALL_EVENT_TYPES = frozenset(
    {
        RESERVATION_REQUESTED,
        RESERVATION_SCHEDULED,
        RESERVATION_WAITLISTED,
        RESERVATION_REJECTED,
        RESERVATION_RESCHEDULED,
        RESERVATION_POSTPONED,
        RESERVATION_AMENDED,
        RESERVATION_MANUAL_INSERTED,
        WAITLIST_ACCEPTED,
        VEHICLE_ARRIVED,
        OPERATION_STARTED,
        RESERVATION_COMPLETED,
        RESERVATION_CANCELLED,
        EQUIPMENT_DISABLED,
        EQUIPMENT_ENABLED,
        GOODS_RELEASED,
        ALERT_RAISED,
        ALERT_RESOLVED,
    }
)


@dataclass(frozen=True, slots=True)
class Event:
    seq: int
    event_id: str
    event_type: str
    aggregate_id: str
    occurred_at: int  # epoch minute
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_id": self.aggregate_id,
            "occurred_at": self.occurred_at,
            "payload": self.payload,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)

    @staticmethod
    def from_row(seq: int, event_id: str, event_type: str, aggregate_id: str,
                 occurred_at: int, payload_raw: str) -> "Event":
        return Event(
            seq=seq,
            event_id=event_id,
            event_type=event_type,
            aggregate_id=aggregate_id,
            occurred_at=occurred_at,
            payload=json.loads(payload_raw),
        )
