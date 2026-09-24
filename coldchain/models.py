"""领域值对象：申报、排程方案、约束冲突、命令结果。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import timepolicy
from .events import Event


@dataclass(frozen=True, slots=True)
class Declaration:
    """承运方申报：到场区间、温区、托盘数、最长暴露时间。"""

    reservation_id: str
    carrier: str
    vehicle_id: str
    zone: str
    pallets: int
    window_start: int  # epoch minute，到场区间起（含）
    window_end: int    # 到场区间止（不含）
    max_exposure_minutes: int
    priority: int = 100  # 数值越小优先级越高；人工插单为 0

    def to_payload(self, tz) -> dict[str, Any]:
        return {
            "reservation_id": self.reservation_id,
            "carrier": self.carrier,
            "vehicle_id": self.vehicle_id,
            "zone": self.zone,
            "pallets": self.pallets,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "window_start_iso": timepolicy.iso(self.window_start, tz),
            "window_end_iso": timepolicy.iso(self.window_end, tz),
            "max_exposure_minutes": self.max_exposure_minutes,
            "priority": self.priority,
        }

    @staticmethod
    def from_payload(payload: dict[str, Any]) -> "Declaration":
        return Declaration(
            reservation_id=payload["reservation_id"],
            carrier=payload["carrier"],
            vehicle_id=payload["vehicle_id"],
            zone=payload["zone"],
            pallets=int(payload["pallets"]),
            window_start=int(payload["window_start"]),
            window_end=int(payload["window_end"]),
            max_exposure_minutes=int(payload["max_exposure_minutes"]),
            priority=int(payload.get("priority", 100)),
        )


@dataclass(frozen=True, slots=True)
class Plan:
    """一张预约的完整资源方案（门位 -> 预冷间 -> 低温库）。"""

    planned_arrival: int
    dock_id: str
    dock_start: int
    dock_end: int
    precool_id: str
    precool_start: int
    precool_end: int
    storage_id: str
    storage_at: int
    pallets: int
    zone: str

    def to_payload(self, tz) -> dict[str, Any]:
        def point(rid: str, start: int, end: int | None = None) -> dict[str, Any]:
            data = {"resource_id": rid, "start": start,
                    "start_iso": timepolicy.iso(start, tz)}
            if end is not None:
                data["end"] = end
                data["end_iso"] = timepolicy.iso(end, tz)
            return data

        return {
            "planned_arrival": self.planned_arrival,
            "planned_arrival_iso": timepolicy.iso(self.planned_arrival, tz),
            "dock": point(self.dock_id, self.dock_start, self.dock_end),
            "precool": point(self.precool_id, self.precool_start, self.precool_end),
            "storage": point(self.storage_id, self.storage_at),
            "pallets": self.pallets,
            "zone": self.zone,
        }

    @staticmethod
    def from_payload(payload: dict[str, Any]) -> "Plan":
        dock = payload["dock"]
        precool = payload["precool"]
        storage = payload["storage"]
        return Plan(
            planned_arrival=int(payload["planned_arrival"]),
            dock_id=dock["resource_id"],
            dock_start=int(dock["start"]),
            dock_end=int(dock["end"]),
            precool_id=precool["resource_id"],
            precool_start=int(precool["start"]),
            precool_end=int(precool["end"]),
            storage_id=storage["resource_id"],
            storage_at=int(storage["start"]),
            pallets=int(payload["pallets"]),
            zone=payload["zone"],
        )


@dataclass(frozen=True, slots=True)
class Violation:
    """真正阻止排程的约束冲突，可直接回给承运方。"""

    code: str                 # window_closed / dock_busy / precool_overload / ...
    resource_kind: str        # dock / precool / storage / exposure
    resource_id: str
    message: str
    at: int | None = None        # 首次突破上限的分钟
    start: int | None = None
    end: int | None = None
    capacity: int | None = None  # 该资源的分钟级上限
    load: int | None = None      # 冲突时已占用量
    required: int | None = None  # 本次申报需要的量

    def to_payload(self, tz) -> dict[str, Any]:
        data: dict[str, Any] = {
            "code": self.code,
            "resource_kind": self.resource_kind,
            "resource_id": self.resource_id,
            "message": self.message,
        }
        for key, value in (
            ("at", self.at), ("start", self.start), ("end", self.end),
            ("capacity", self.capacity), ("load", self.load),
            ("required", self.required),
        ):
            if value is not None:
                data[key] = value
                if key in ("at", "start", "end"):
                    data[f"{key}_iso"] = timepolicy.iso(value, tz)
        return data

    @staticmethod
    def from_payload(payload: dict[str, Any]) -> "Violation":
        return Violation(
            code=payload["code"],
            resource_kind=payload["resource_kind"],
            resource_id=payload["resource_id"],
            message=payload["message"],
            at=payload.get("at"),
            start=payload.get("start"),
            end=payload.get("end"),
            capacity=payload.get("capacity"),
            load=payload.get("load"),
            required=payload.get("required"),
        )


@dataclass(slots=True)
class CommandResult:
    """一次命令的确定结果。"""

    accepted: bool
    events: list[Event] = field(default_factory=list)
    violations: list[Violation] = field(default_factory=list)
    plan: Plan | None = None
    diverted_to_waitlist: bool = False
    version: int = 0

    @property
    def event_types(self) -> list[str]:
        return [e.event_type for e in self.events]
