"""领域模型：资源、作业、排程计划、冲突。

时间在计划内部同时保存两种形态：

- ``day`` + 分钟序号（``start_minute``/``end_minute``）——排程引擎的工作坐标，
  分钟序号允许 >= 1440 以表达跨午夜作业；
- ISO 字符串（``start``/``end``）——持久化与对外展示形态，
  由 :class:`~coldchain.timeutil.TimeService` 按市场时区换算。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# 三类分钟级容量资源
DOCK = "dock"            # 月台：按同时在泊车辆数计容
PRECOOL = "precool"       # 预冷间：按在间托盘数计容
STORAGE = "storage"       # 低温库库位：按占用托盘数计容
RESOURCE_TYPES = (DOCK, PRECOOL, STORAGE)

# 作业默认经过的资源顺序
FLOW = (DOCK, PRECOOL, STORAGE)


class ReservationState(str, Enum):
    REQUESTED = "requested"
    REJECTED = "rejected"
    WAITLISTED = "waitlisted"
    SCHEDULED = "scheduled"
    ARRIVED = "arrived"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


# 已开始实体作业的车辆「保持原位」：这些状态绝不参与重新编排。
# 已到场（arrived）但尚未开始靠台作业的车辆仍可被重排，
# 只是不能早于实际到场分钟、且受最长暴露时间约束。
IMMOVABLE_STATES = frozenset(
    {ReservationState.PROCESSING, ReservationState.COMPLETED}
)


@dataclass(frozen=True)
class Operation:
    """单预约在单一资源上的一段占用（半开区间 [start, end)）。"""

    resource_type: str
    resource_id: str
    start_minute: int
    end_minute: int
    pallets: int
    start: str = ""
    end: str = ""
    locked: bool = False

    def overlaps(self, other: "Operation") -> bool:
        return (
            self.resource_type == other.resource_type
            and self.resource_id == other.resource_id
            and self.start_minute < other.end_minute
            and other.start_minute < self.end_minute
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "start_minute": self.start_minute,
            "end_minute": self.end_minute,
            "pallets": self.pallets,
            "start": self.start,
            "end": self.end,
            "locked": self.locked,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Operation":
        return cls(
            resource_type=data["resource_type"],
            resource_id=data["resource_id"],
            start_minute=data["start_minute"],
            end_minute=data["end_minute"],
            pallets=data["pallets"],
            start=data.get("start", ""),
            end=data.get("end", ""),
            locked=data.get("locked", False),
        )


@dataclass
class Plan:
    """一张预约的完整作业计划。"""

    reservation_id: str
    day: str
    operations: list[Operation] = field(default_factory=list)
    state: str = ReservationState.SCHEDULED.value
    version: int = 0

    def ops_for(self, resource_type: str) -> list[Operation]:
        return [op for op in self.operations if op.resource_type == resource_type]

    @property
    def start_minute(self) -> int | None:
        return min((op.start_minute for op in self.operations), default=None)

    @property
    def end_minute(self) -> int | None:
        return max((op.end_minute for op in self.operations), default=None)

    def lock(self) -> None:
        self.operations = [
            Operation(**{**op.to_dict(), "locked": True}) for op in self.operations
        ]

    def to_dict(self) -> dict[str, Any]:
        return {
            "reservation_id": self.reservation_id,
            "day": self.day,
            "state": self.state,
            "version": self.version,
            "operations": [op.to_dict() for op in self.operations],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Plan":
        return cls(
            reservation_id=data["reservation_id"],
            day=data["day"],
            state=data["state"],
            version=data.get("version", 0),
            operations=[Operation.from_dict(op) for op in data.get("operations", [])],
        )


@dataclass(frozen=True)
class BlockingLoad:
    """造成冲突的既有占用。"""

    reservation_id: str
    start_minute: int
    end_minute: int
    pallets: int
    locked: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "reservation_id": self.reservation_id,
            "start_minute": self.start_minute,
            "end_minute": self.end_minute,
            "pallets": self.pallets,
            "locked": self.locked,
        }


@dataclass(frozen=True)
class Conflict:
    """拒绝或改期时返回的「真正冲突的约束」。"""

    resource_type: str
    resource_id: str
    constraint: str                 # capacity | disabled | exposure | window | zone
    minute: int                     # 冲突发生的分钟坐标
    limit: int                      # 该约束允许的上限（容量/暴露时长）
    required: int = 0               # 本次申请在该分钟需要的量
    available: int = 0              # 该分钟实际剩余
    blockers: tuple[BlockingLoad, ...] = ()
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "constraint": self.constraint,
            "minute": self.minute,
            "limit": self.limit,
            "required": self.required,
            "available": self.available,
            "blockers": [b.to_dict() for b in self.blockers],
            "message": self.message,
        }
