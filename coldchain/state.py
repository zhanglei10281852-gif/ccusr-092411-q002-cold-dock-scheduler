"""事件折叠状态。

``State.fold`` 是纯函数：给定事件序列必然得到相同状态，不依赖当前时钟、
随机数或事件到达次序之外的任何东西。候补序号、告警、设备停用区间、
人工插单的批准人与被延后排期全部从事件派生，因此重启进程后原样恢复。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .events import (
    ALARM_CLEARED,
    ALARM_RAISED,
    EQUIPMENT_DISABLED,
    EQUIPMENT_ENABLED,
    RESERVATION_ARRIVED,
    RESERVATION_CANCELLED,
    RESERVATION_COMPLETED,
    RESERVATION_OVERRIDDEN,
    RESERVATION_PROCESSING,
    RESERVATION_PROMOTED,
    RESERVATION_REJECTED,
    RESERVATION_REQUESTED,    RESERVATION_REVISED,
    RESERVATION_SCHEDULED,
    RESERVATION_WAITLISTED,
    Event,
)
from .models import Plan, ReservationState


@dataclass
class DisabledInterval:
    day: str
    resource_type: str
    resource_id: str
    start_minute: int
    end_minute: int | None      # None 表示当日剩余时间全部停用
    reason: str = ""
    revoked: bool = False


@dataclass
class Alarm:
    reservation_id: str
    code: str
    message: str
    deadline_minute: int
    raised_at: str
    cleared: bool = False
    cleared_at: str = ""


@dataclass
class OverrideRecord:
    approver: str
    displaced: tuple[str, ...]
    note: str
    at: str


@dataclass
class Reservation:
    reservation_id: str
    carrier: str = ""
    vehicle_id: str = ""
    zone: str = ""
    pallets: int = 0
    max_exposure_minutes: int = 0
    day: str = ""
    window_start_minute: int = 0
    window_end_minute: int = 0
    timeout_after_minutes: int | None = None
    state: str = ReservationState.REQUESTED.value
    plan: Plan | None = None
    waitlist_seq: int | None = None
    waitlisted_at: str = ""
    waitlist_reason: dict[str, Any] | None = None
    arrived_at: str = ""
    arrived_minute: int | None = None
    processing_minute: int | None = None
    # 人工插单批准的到场分钟；车辆未到场前，重排以它为有效窗口起点
    forced_start_minute: int | None = None
    completed_at: str = ""
    cancel_reason: str = ""
    alarms: list[Alarm] = field(default_factory=list)
    overrides: list[OverrideRecord] = field(default_factory=list)
    rejection: dict[str, Any] | None = None
    version: int = 0
    created_at: str = ""

    @property
    def active_alarms(self) -> list[Alarm]:
        return [a for a in self.alarms if not a.cleared]

    @property
    def is_waitlisted(self) -> bool:
        return self.state == ReservationState.WAITLISTED.value

    @property
    def immovable(self) -> bool:
        """已开始实体作业（processing/completed）的车辆保持原位。"""
        return self.state in {
            ReservationState.PROCESSING.value,
            ReservationState.COMPLETED.value,
        }

    def exposure_window_ok(self, plan_start_minute: int) -> bool:
        """最早暴露（车辆到场）到开始作业的间隔不得超过最长暴露时间。"""
        if self.arrived_minute is None or not self.max_exposure_minutes:
            return True
        return plan_start_minute - self.arrived_minute <= self.max_exposure_minutes


@dataclass
class State:
    reservations: dict[str, Reservation] = field(default_factory=dict)
    disables: list[DisabledInterval] = field(default_factory=list)
    # 每天候补序号单调递增，仅从事件派生
    waitlist_counters: dict[str, int] = field(default_factory=dict)
    last_seq: dict[str, int] = field(default_factory=dict)

    # -- 查询 -----------------------------------------------------------

    def get(self, reservation_id: str) -> Reservation:
        if reservation_id not in self.reservations:
            raise KeyError(f"未知预约：{reservation_id}")
        return self.reservations[reservation_id]

    def by_day(self, day: str) -> list[Reservation]:
        return [r for r in self.reservations.values() if r.day == day]

    def waitlist(self, day: str) -> list[Reservation]:
        rows = [
            r for r in self.by_day(day)
            if r.state == ReservationState.WAITLISTED.value and r.waitlist_seq is not None
        ]
        return sorted(rows, key=lambda r: (r.waitlist_seq, r.reservation_id))

    def active_disables(self, day: str) -> list[DisabledInterval]:
        return [d for d in self.disables if d.day == day and not d.revoked]

    def planned(self, day: str) -> list[Reservation]:
        return [
            r for r in self.by_day(day)
            if r.plan is not None and r.state != ReservationState.CANCELLED.value
        ]

    # -- 折叠 -----------------------------------------------------------

    def apply(self, event: Event) -> None:
        self.last_seq[event.aggregate_id] = event.seq
        p = event.payload
        et = event.event_type
        rid = event.aggregate_id

        if et == RESERVATION_REQUESTED:
            self.reservations[rid] = Reservation(
                reservation_id=rid,
                carrier=p["carrier"],
                vehicle_id=p["vehicle_id"],
                zone=p["zone"],
                pallets=p["pallets"],
                max_exposure_minutes=p["max_exposure_minutes"],
                day=p["day"],
                window_start_minute=p["window_start_minute"],
                window_end_minute=p["window_end_minute"],
                timeout_after_minutes=p.get("timeout_after_minutes"),
                state=ReservationState.REQUESTED.value,
                created_at=event.occurred_at,
            )
            return

        if et == EQUIPMENT_DISABLED:
            self.disables.append(
                DisabledInterval(
                    day=p["day"],
                    resource_type=p["resource_type"],
                    resource_id=p["resource_id"],
                    start_minute=p["start_minute"],
                    end_minute=p.get("end_minute"),
                    reason=p.get("reason", ""),
                )
            )
            return

        if et == EQUIPMENT_ENABLED:
            for interval in reversed(self.disables):
                if (
                    interval.day == p["day"]
                    and interval.resource_type == p["resource_type"]
                    and interval.resource_id == p["resource_id"]
                    and not interval.revoked
                ):
                    interval.revoked = True
                    break
            return

        rec = self.reservations.get(rid)
        if rec is None:
            # 重播容错：未知聚合的剩余事件忽略而不是崩溃
            return

        if et == RESERVATION_REVISED:
            rec.window_start_minute = p["window_start_minute"]
            rec.window_end_minute = p["window_end_minute"]
            rec.max_exposure_minutes = p["max_exposure_minutes"]
            # 改期即重新申报：清除上一次拒绝与候补标记
            rec.rejection = None
            if rec.state == ReservationState.REJECTED.value:
                rec.state = ReservationState.REQUESTED.value
            rec.version += 1
        elif et in (RESERVATION_SCHEDULED, RESERVATION_PROMOTED):
            rec.plan = Plan.from_dict(p["plan"])
            # 物理事实（到场/开工/完工）永远不被计划更新降级：
            # scheduled 事件只承载重新计算出的作业安排。
            if rec.state == ReservationState.PROCESSING.value:
                pass
            elif rec.state == ReservationState.COMPLETED.value:
                pass
            elif rec.arrived_minute is not None:
                rec.state = ReservationState.ARRIVED.value
            else:
                rec.state = rec.plan.state or ReservationState.SCHEDULED.value
            if et == RESERVATION_PROMOTED:
                rec.waitlist_seq = None
                rec.waitlist_reason = None
            rec.version += 1
        elif et == RESERVATION_WAITLISTED:
            rec.state = ReservationState.WAITLISTED.value
            rec.waitlist_seq = p["seq"]
            rec.waitlisted_at = event.occurred_at
            rec.waitlist_reason = p.get("reason")
            # 候补没有已确认计划：旧排程作废，避免残留占用被误读。
            rec.plan = None
            self.waitlist_counters[rec.day] = max(
                self.waitlist_counters.get(rec.day, 0), p["seq"]
            )
        elif et == RESERVATION_REJECTED:
            # 拒绝留痕：预约进入 rejected、不参与排程；承运方改期后可重新申报。
            rec.state = ReservationState.REJECTED.value
            rec.plan = None
            rec.rejection = {"conflicts": p.get("conflicts", [])}
        elif et == RESERVATION_ARRIVED:
            rec.state = ReservationState.ARRIVED.value
            rec.arrived_at = event.occurred_at
            rec.arrived_minute = p["minute"]
            # 实际到场后以实际到场分钟为准，插单批准时间失效
            rec.forced_start_minute = None
            # 到场但未开工：计划仍可重排，不锁定
        elif et == RESERVATION_PROCESSING:
            rec.state = ReservationState.PROCESSING.value
            rec.processing_minute = p["minute"]
            if rec.plan is not None:
                rec.plan.lock()
        elif et == RESERVATION_COMPLETED:
            rec.state = ReservationState.COMPLETED.value
            rec.completed_at = event.occurred_at
            for alarm in rec.alarms:
                if not alarm.cleared:
                    alarm.cleared = True
                    alarm.cleared_at = event.occurred_at
        elif et == RESERVATION_CANCELLED:
            rec.state = ReservationState.CANCELLED.value
            rec.cancel_reason = p.get("reason", "")
        elif et == RESERVATION_OVERRIDDEN:
            rec.overrides.append(
                OverrideRecord(
                    approver=p["approver"],
                    displaced=tuple(p.get("displaced", [])),
                    note=p.get("note", ""),
                    at=event.occurred_at,
                )
            )
            if p.get("start_minute") is not None:
                rec.forced_start_minute = p["start_minute"]
            if "plan" in p:
                rec.plan = Plan.from_dict(p["plan"])
                if rec.arrived_minute is not None and rec.state not in {
                    ReservationState.PROCESSING.value,
                    ReservationState.COMPLETED.value,
                }:
                    rec.state = ReservationState.ARRIVED.value
                else:
                    rec.state = rec.plan.state
            rec.waitlist_seq = None
            rec.waitlist_reason = None
            rec.version += 1
        elif et == ALARM_RAISED:
            rec.alarms.append(
                Alarm(
                    reservation_id=rid,
                    code=p["code"],
                    message=p["message"],
                    deadline_minute=p["deadline_minute"],
                    raised_at=event.occurred_at,
                )
            )
        elif et == ALARM_CLEARED:
            code = p["code"]
            for alarm in reversed(rec.alarms):
                if alarm.code == code and not alarm.cleared:
                    alarm.cleared = True
                    alarm.cleared_at = event.occurred_at
                    break

    @classmethod
    def fold(cls, events: list[Event]) -> "State":
        # 事件必须按提交顺序（事件库 rowid）折叠；此处不再重排，
        # 以保留跨聚合命令的因果顺序。
        state = cls()
        for event in events:
            state.apply(event)
        return state
