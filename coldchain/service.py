"""应用服务：预约申报、到场、设备停用、人工插单、候补与告警。

每个命令都走同一条确定性流水线：

1. 从事件库折叠出最新状态（重启后自然恢复候补序号与告警）；
2. 在目标营业日上运行排程引擎（已开工车辆作为锁定占用，不动）；
3. 把计划差异作为事件，在一个 ``begin immediate`` 事务内原子追加；
4. 返回计划，或在拒绝/改期失败时返回真正卡住的冲突约束。

并发由事件库的乐观版本号 + SQLite 写事务裁决：两个调度员同时接受
候补时，后提交者收到 :class:`StaleVersion`，必须重新加载状态后重试，
因此任一资源上限都不可能被突破。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .capacity import Resource
from .events import (
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
    RESERVATION_REQUESTED,
    RESERVATION_REVISED,
    RESERVATION_SCHEDULED,
    RESERVATION_WAITLISTED,
    Event,
    make_event,
)
from .models import (
    RESOURCE_TYPES,
    Conflict,
    Plan,
    ReservationState,
)
from .scheduler import ForceSlot, Scheduler, SchedulingResult, StageProfile
from .state import State
from .store import EventStore, StaleVersion
from .timeutil import TimeService

ALARM_WAITLIST_TIMEOUT = "waitlist.timeout"
ALARM_EXPOSURE_TIMEOUT = "exposure.timeout"


class InvalidState(Exception):
    """命令在当前预约状态下不允许。"""


class ScheduleConflict(Exception):
    """拒绝或改期失败，携带真正冲突的约束。"""

    def __init__(self, conflicts: list[Conflict], message: str = "无法排程") -> None:
        super().__init__(message)
        self.conflicts = conflicts

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "schedule_conflict",
            "message": str(self),
            "conflicts": [c.to_dict() for c in self.conflicts],
        }


@dataclass
class CommandResult:
    status: str                      # scheduled | waitlisted
    reservation_id: str
    plan: Plan | None = None
    conflicts: list[Conflict] = field(default_factory=list)
    displaced: list[str] = field(default_factory=list)
    promoted: list[str] = field(default_factory=list)
    events: list[Event] = field(default_factory=list)
    waitlist_seq: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reservation_id": self.reservation_id,
            "plan": self.plan.to_dict() if self.plan else None,
            "conflicts": [c.to_dict() for c in self.conflicts],
            "displaced": list(self.displaced),
            "promoted": list(self.promoted),
            "waitlist_seq": self.waitlist_seq,
        }


class ColdChainService:
    def __init__(
        self,
        store: EventStore,
        resources: list[Resource] | None = None,
        time: TimeService | None = None,
        profile: StageProfile | None = None,
    ) -> None:
        self.store = store
        self.time = time or TimeService()
        self.profile = profile or StageProfile()
        if resources is not None:
            self.save_resources(resources)
        self._resources = self._load_resources()

    # -- 资源配置 -------------------------------------------------------

    def save_resources(self, resources: list[Resource]) -> None:
        for res in resources:
            if res.resource_type not in RESOURCE_TYPES:
                raise ValueError(f"未知资源类型：{res.resource_type}")
            if res.capacity <= 0:
                raise ValueError("资源容量必须为正")
        self.store.save_resources(
            [(r.resource_type, r.resource_id, r.zone, r.capacity) for r in resources]
        )
        self._resources = list(resources)

    def _load_resources(self) -> list[Resource]:
        return [
            Resource(rtype, rid, zone, cap)
            for rtype, rid, zone, cap in self.store.load_resources()
        ]

    def _scheduler(self) -> Scheduler:
        return Scheduler(self.time, list(self._resources), self.profile)

    # -- 状态加载 -------------------------------------------------------

    def load_state(self) -> State:
        return State.fold(self.store.load_events())

    def _day_minute(self, day: str, value: str) -> int:
        start, _ = self.time.day_bounds(day)
        return self.time.minutes_between(start, value)

    def _commit(
        self,
        events: list[Event],
        expected: dict[str, int] | None = None,
    ) -> list[Event]:
        """按聚合统一编号后原子追加。

        ``expected`` 是调用方加载状态时各聚合的基线最后序号；
        ``EventStore.append`` 在 ``begin immediate`` 事务内重新读取
        库内序号，若已有并发事务写入则抛 :class:`StaleVersion`，
        整批回滚——这是「两个调度员同时操作也不突破容量」的最终裁决点。

        编号严格按调用方给定的因果顺序逐聚合递增。
        """
        if not events:
            return []
        aggregates = {e.aggregate_id for e in events}
        if expected is None:
            expected = {agg: self.store.last_seq(agg) for agg in aggregates}
        baselines = dict(expected)
        numbered: list[Event] = []
        for event in events:
            baselines[event.aggregate_id] = baselines.get(
                event.aggregate_id, 0
            ) + 1
            numbered.append(
                make_event(
                    event.event_type,
                    event.aggregate_id,
                    baselines[event.aggregate_id],
                    event.occurred_at,
                    event.payload,
                    aggregate_type=event.aggregate_type,
                )
            )
        self.store.append(numbered, expected)
        return numbered

    # -- 承运申报 -------------------------------------------------------

    def request_reservation(
        self,
        reservation_id: str,
        *,
        carrier: str,
        vehicle_id: str,
        zone: str,
        pallets: int,
        day: str,
        window_start_minute: int,
        window_end_minute: int,
        max_exposure_minutes: int,
        timeout_after_minutes: int | None = None,
        occurred_at: str | None = None,
        reject_if_unavailable: bool = False,
    ) -> CommandResult:
        occurred_at = occurred_at or self.time.format(self.time.now())
        if pallets <= 0:
            raise ValueError("托盘数必须为正")
        if max_exposure_minutes <= 0:
            raise ValueError("最长暴露时间必须为正")
        if not 0 <= window_start_minute < window_end_minute <= 2 * 1440:
            raise ValueError("到场区间分钟数非法（允许跨午夜，最长 48 小时）")

        state = self.load_state()
        if reservation_id in state.reservations:
            raise InvalidState(f"预约已存在：{reservation_id}")
        baseline = dict(state.last_seq)

        requested = make_event(
            RESERVATION_REQUESTED, reservation_id, 1, occurred_at,
            {
                "carrier": carrier,
                "vehicle_id": vehicle_id,
                "zone": zone,
                "pallets": pallets,
                "day": day,
                "window_start_minute": window_start_minute,
                "window_end_minute": window_end_minute,
                "max_exposure_minutes": max_exposure_minutes,
                "timeout_after_minutes": timeout_after_minutes,
            },
        )
        state.apply(requested)
        if reject_if_unavailable:
            now_minute = self._minute_if_same_day(day, occurred_at)
            probe, _ = self._run_day(state, day, now_minute=now_minute)
            if reservation_id in probe.failed:
                rejected = make_event(
                    RESERVATION_REJECTED, reservation_id, 2, occurred_at,
                    {
                        "conflicts": [
                            c.to_dict() for c in probe.failed[reservation_id]
                        ]
                    },
                )
                self._commit(
                    [requested, rejected], expected=dict(baseline)
                )
                raise ScheduleConflict(
                    probe.failed[reservation_id],
                    "申报被拒绝：以下约束真正冲突",
                )
        return self._replan(
            state, day, target_id=reservation_id,
            occurred_at=occurred_at, seeded=[requested], baseline=baseline,
        )

    # -- 改期：失败时不写任何事件，返回真正冲突的约束 ------------------

    def reschedule(
        self,
        reservation_id: str,
        *,
        window_start_minute: int,
        window_end_minute: int,
        max_exposure_minutes: int | None = None,
        occurred_at: str | None = None,
    ) -> CommandResult:
        occurred_at = occurred_at or self.time.format(self.time.now())
        state = self.load_state()
        rec = state.get(reservation_id)
        if rec.immovable:
            raise InvalidState("已开工车辆不能改期")
        if not 0 <= window_start_minute < window_end_minute <= 2 * 1440:
            raise ValueError("到场区间分钟数非法")
        baseline = dict(state.last_seq)

        revised = make_event(
            RESERVATION_REVISED, reservation_id,
            state.last_seq.get(reservation_id, 0) + 1, occurred_at,
            {
                "window_start_minute": window_start_minute,
                "window_end_minute": window_end_minute,
                "max_exposure_minutes": (
                    max_exposure_minutes
                    if max_exposure_minutes is not None
                    else rec.max_exposure_minutes
                ),
            },
        )
        state.apply(revised)
        result, _ = self._run_day(state, rec.day)
        if reservation_id in result.failed:
            raise ScheduleConflict(
                result.failed[reservation_id],
                "改期失败：以下约束真正冲突",
            )
        return self._replan(
            state, rec.day, target_id=reservation_id,
            occurred_at=occurred_at, seeded=[revised], baseline=baseline,
        )

    # -- 到场 / 开工 / 完成 --------------------------------------------

    def arrive(
        self,
        reservation_id: str,
        at: str | None = None,
        *,
        occurred_at: str | None = None,
    ) -> CommandResult:
        at_dt = self.time.parse(at) if at else self.time.now()
        occurred_at = occurred_at or self.time.format(at_dt)
        state = self.load_state()
        rec = state.get(reservation_id)
        if rec.state not in {
            ReservationState.SCHEDULED.value,
            ReservationState.WAITLISTED.value,
            ReservationState.REQUESTED.value,
            ReservationState.ARRIVED.value,
        }:
            raise InvalidState(f"状态 {rec.state} 不允许到场登记")
        baseline = dict(state.last_seq)

        minute = max(0, self._day_minute(rec.day, self.time.format(at_dt)))
        if rec.arrived_minute is not None:
            # 重复到场登记幂等：只做一次重排
            return self._replan(
                state, rec.day, target_id=reservation_id,
                occurred_at=occurred_at, baseline=baseline,
            )
        arrived = make_event(
            RESERVATION_ARRIVED, reservation_id,
            state.last_seq.get(reservation_id, 0) + 1, occurred_at,
            {"minute": minute},
        )
        state.apply(arrived)
        return self._replan(
            state, rec.day, target_id=reservation_id,
            occurred_at=occurred_at, seeded=[arrived], baseline=baseline,
        )

    def start_processing(self, reservation_id: str, at: str | None = None) -> Event:
        at_dt = self.time.parse(at) if at else self.time.now()
        occurred_at = self.time.format(at_dt)
        state = self.load_state()
        rec = state.get(reservation_id)
        if rec.state != ReservationState.ARRIVED.value:
            raise InvalidState(f"状态 {rec.state} 不能开工")
        minute = self._day_minute(rec.day, occurred_at)
        event = make_event(
            RESERVATION_PROCESSING, reservation_id,
            state.last_seq.get(reservation_id, 0) + 1, occurred_at,
            {"minute": minute},
        )
        return self._commit(
            [event], expected={reservation_id: state.last_seq.get(reservation_id, 0)}
        )[0]

    def complete(self, reservation_id: str, at: str | None = None) -> CommandResult:
        at_dt = self.time.parse(at) if at else self.time.now()
        occurred_at = self.time.format(at_dt)
        state = self.load_state()
        rec = state.get(reservation_id)
        baseline = dict(state.last_seq)
        completed = make_event(
            RESERVATION_COMPLETED, reservation_id,
            state.last_seq.get(reservation_id, 0) + 1, occurred_at, {},
        )
        state.apply(completed)
        # 完工与「释放容量后的重排」在同一事务提交：
        # 已开工的 completed 在排程中被排除，候补可立即获得释放的容量。
        return self._replan(
            state, rec.day, target_id=None,
            occurred_at=occurred_at, seeded=[completed], baseline=baseline,
        )

    def cancel(self, reservation_id: str, reason: str = "") -> CommandResult:
        state = self.load_state()
        rec = state.get(reservation_id)
        baseline = dict(state.last_seq)
        occurred_at = self.time.format(self.time.now())
        cancelled = make_event(
            RESERVATION_CANCELLED, reservation_id,
            state.last_seq.get(reservation_id, 0) + 1, occurred_at,
            {"reason": reason},
        )
        state.apply(cancelled)
        return self._replan(
            state, rec.day, target_id=None,
            occurred_at=occurred_at, seeded=[cancelled], baseline=baseline,
        )

    # -- 设备停用 / 恢复 ------------------------------------------------

    def disable_equipment(
        self,
        day: str,
        resource_type: str,
        resource_id: str,
        start_minute: int,
        end_minute: int | None = None,
        *,
        reason: str = "",
        occurred_at: str | None = None,
    ) -> CommandResult:
        occurred_at = occurred_at or self.time.format(self.time.now())
        if (resource_type, resource_id) not in {
            (r.resource_type, r.resource_id) for r in self._resources
        }:
            raise KeyError(f"未知资源：{resource_type}/{resource_id}")
        aggregate = f"{day}:{resource_type}:{resource_id}"
        state = self.load_state()
        baseline = dict(state.last_seq)
        event = make_event(
            EQUIPMENT_DISABLED, aggregate,
            state.last_seq.get(aggregate, 0) + 1, occurred_at,
            {
                "day": day,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "start_minute": start_minute,
                "end_minute": end_minute,
                "reason": reason,
            },
            aggregate_type="equipment",
        )
        state.apply(event)
        return self._replan(
            state, day, target_id=None, occurred_at=occurred_at,
            seeded=[event], baseline=baseline,
        )

    def enable_equipment(
        self, day: str, resource_type: str, resource_id: str
    ) -> CommandResult:
        occurred_at = self.time.format(self.time.now())
        aggregate = f"{day}:{resource_type}:{resource_id}"
        state = self.load_state()
        baseline = dict(state.last_seq)
        event = make_event(
            EQUIPMENT_ENABLED, aggregate,
            state.last_seq.get(aggregate, 0) + 1, occurred_at,
            {"day": day, "resource_type": resource_type, "resource_id": resource_id},
            aggregate_type="equipment",
        )
        state.apply(event)
        return self._replan(
            state, day, target_id=None, occurred_at=occurred_at,
            seeded=[event], baseline=baseline,
        )

    # -- 人工插单：必须有批准人，记录被延后批次 ------------------------

    def manual_insert(
        self,
        reservation_id: str,
        *,
        start_minute: int,
        approver: str,
        note: str = "",
        occurred_at: str | None = None,
    ) -> CommandResult:
        if not approver or not approver.strip():
            raise ValueError("人工插单必须记录批准人")
        occurred_at = occurred_at or self.time.format(self.time.now())
        state = self.load_state()
        rec = state.get(reservation_id)
        if rec.immovable:
            raise InvalidState("该车辆已开工，无需插单")

        # 在内存状态中登记批准的到场分钟，使本次及后续重排都把它
        # 当作该预约的到场锚点（持久化在 override 事件中）。
        rec.forced_start_minute = start_minute
        force = (reservation_id, ForceSlot(start_minute, approver, note))
        now_minute = self._minute_if_same_day(rec.day, occurred_at)
        result, _ = self._run_day(
            state, rec.day, force=force, now_minute=now_minute
        )
        if reservation_id not in result.scheduled:
            raise ScheduleConflict(
                result.failed.get(reservation_id, []),
                "插单槽位与已开工车辆或硬约束冲突，无法插入",
            )

        events = self._diff_events(
            state, rec.day, result, occurred_at,
            forced=reservation_id,
            force_meta={
                "approver": approver.strip(),
                "note": note,
                "start_minute": start_minute,
            },
        )
        committed = self._commit(events, expected=dict(state.last_seq))
        return CommandResult(
            status="scheduled",
            reservation_id=reservation_id,
            plan=result.scheduled[reservation_id],
            displaced=sorted(result.displaced),
            events=committed,
        )

    # -- 候补接受：并发时由乐观锁裁决，容量绝不超限 --------------------

    def accept_waitlist(
        self,
        day: str,
        reservation_ids: list[str] | None = None,
        *,
        occurred_at: str | None = None,
    ) -> CommandResult:
        """尝试把候补（按原有候补序号）排入释放出的容量。

        排得上的 promoted，排不上的保留原序号。两个调度员并发调用时，
        其中一个事务会抛 :class:`StaleVersion`，重试后得到的新计划仍然
        满足全部资源上限。
        """
        occurred_at = occurred_at or self.time.format(self.time.now())
        state = self.load_state()
        waiting = {r.reservation_id for r in state.waitlist(day)}
        if reservation_ids is not None:
            for rid in reservation_ids:
                if rid not in waiting:
                    raise InvalidState(f"{rid} 不在 {day} 候补队列中")
        return self._replan(
            state, day, target_id=None, occurred_at=occurred_at,
            baseline=dict(state.last_seq),
        )

    # -- 超时告警：重启后继续有效（全部从事件折叠） --------------------

    def pump_alarms(
        self, day: str, now_minute: int, *, occurred_at: str | None = None
    ) -> list[Event]:
        """补提所有到期告警。同一告警代码对同一预约只提一次（幂等）。"""
        occurred_at = occurred_at or self.time.format(
            self.time.at_minute(day, now_minute)
        )
        state = self.load_state()
        pending: list[Event] = []
        for rec in sorted(state.by_day(day), key=lambda r: r.reservation_id):
            raised_codes = {a.code for a in rec.alarms}

            if (
                rec.is_waitlisted
                and rec.timeout_after_minutes
                and rec.waitlisted_at
                and ALARM_WAITLIST_TIMEOUT not in raised_codes
            ):
                # 截止时刻按绝对时间戳计算（候补可能在营业日之前建立）
                wait_dt = self.time.parse(rec.waitlisted_at)
                now_dt = self.time.at_minute(day, now_minute)
                deadline_minute = (
                    self._day_minute(day, self.time.format(wait_dt))
                    + rec.timeout_after_minutes
                )
                waited = self.time.minutes_between(wait_dt, now_dt)
                if waited >= rec.timeout_after_minutes:
                    pending.append(
                        make_event(
                            ALARM_RAISED, rec.reservation_id, 0, occurred_at,
                            {
                                "code": ALARM_WAITLIST_TIMEOUT,
                                "message": (
                                    f"候补 #{rec.waitlist_seq} 已等待 "
                                    f"{waited} 分钟，超过 "
                                    f"{rec.timeout_after_minutes} 分钟未排上"
                                ),
                                "deadline_minute": deadline_minute,
                            },
                        )
                    )

            if (
                rec.state == ReservationState.ARRIVED.value
                and rec.arrived_minute is not None
                and rec.max_exposure_minutes
                and ALARM_EXPOSURE_TIMEOUT not in raised_codes
            ):
                plan_start = (
                    rec.plan.start_minute if rec.plan is not None else None
                )
                # 已排程：看靠台分钟是否已超暴露上限；
                # 暂无可行计划（到场等待）：看当前分钟是否已超上限。
                reference = plan_start if plan_start is not None else now_minute
                if reference - rec.arrived_minute > rec.max_exposure_minutes:
                    deadline = rec.arrived_minute + rec.max_exposure_minutes
                    pending.append(
                        make_event(
                            ALARM_RAISED, rec.reservation_id, 0, occurred_at,
                            {
                                "code": ALARM_EXPOSURE_TIMEOUT,
                                "message": (
                                    f"车辆到场后已等待 {reference - rec.arrived_minute} "
                                    f"分钟，超过最长暴露 {rec.max_exposure_minutes} 分钟"
                                ),
                                "deadline_minute": deadline,
                            },
                        )
                    )
        return self._commit(pending, expected=dict(state.last_seq))

    # -- 容量审计：逐分钟核对任一资源不超上限 --------------------------

    def audit_day(self, day: str) -> dict[str, Any]:
        """把当日所有未取消作业铺到台账，返回越界点与峰值利用率。"""
        from .capacity import Ledger

        state = self.load_state()
        ledger = Ledger(self._resources)
        for interval in state.active_disables(day):
            ledger.disable(
                interval.resource_type, interval.resource_id,
                interval.start_minute, interval.end_minute,
            )
        violations: list[dict[str, Any]] = []
        for rec in sorted(state.by_day(day), key=lambda r: r.reservation_id):
            if not rec.plan or rec.state == ReservationState.CANCELLED.value:
                continue
            for op in rec.plan.operations:
                conflict = ledger.check_interval(
                    op.resource_type, op.resource_id,
                    op.start_minute, op.end_minute, op.pallets,
                )
                if conflict is not None:
                    violations.append(
                        {"reservation_id": rec.reservation_id, **conflict.to_dict()}
                    )
                ledger.add(rec.reservation_id, op)

        peaks: dict[str, Any] = {}
        for (rtype, rid), loads in ledger._usage.items():
            res = ledger.resources[(rtype, rid)]
            peak = 0
            peak_minute = 0
            for minute in range(ledger.horizon):
                used = ledger.usage_at(rtype, rid, minute)
                if used > peak:
                    peak, peak_minute = used, minute
            peaks[f"{rtype}/{rid}"] = {
                "peak": peak,
                "capacity": res.capacity,
                "peak_minute": peak_minute,
                "utilization": round(peak / res.capacity, 3),
            }
        return {
            "day": day,
            "within_limits": not violations,
            "violations": violations,
            "peaks": peaks,
        }

    # -- 核心：排程、差异、提交 ----------------------------------------

    def _run_day(
        self,
        state: State,
        day: str,
        *,
        force: tuple[str, ForceSlot] | None = None,
        now_minute: int | None = None,
    ) -> tuple[SchedulingResult, list]:
        reservations = [
            r for r in state.by_day(day)
            if r.state
            not in {
                ReservationState.CANCELLED.value,
                ReservationState.COMPLETED.value,
                ReservationState.REJECTED.value,
            }
        ]
        result = self._scheduler().plan_day(
            day, reservations,
            disables=state.active_disables(day),
            force=force,
            now_minute=now_minute,
        )
        return result, reservations

    def _minute_if_same_day(self, day: str, occurred_at: str) -> int | None:
        try:
            dt = self.time.parse(occurred_at)
            if dt.date().isoformat() == day:
                return self._day_minute(day, occurred_at)
        except (ValueError, TypeError):
            return None
        return None

    def _replan(
        self,
        state: State,
        day: str,
        *,
        target_id: str | None,
        occurred_at: str,
        seeded: list[Event] | None = None,
        baseline: dict[str, int] | None = None,
    ) -> CommandResult:
        """运行当日排程并提交差异。

        ``seeded`` 与差异事件在同一事务提交；``baseline`` 是命令加载状态时
        的聚合序号快照，作为乐观并发的期望值。
        """
        seeded = seeded or []
        if baseline is None:
            baseline = dict(state.last_seq)
        before_waiting = {
            r.reservation_id: r.waitlist_seq for r in state.waitlist(day)
        }
        now_minute = self._minute_if_same_day(day, occurred_at)
        result, _ = self._run_day(state, day, now_minute=now_minute)
        diff = self._diff_events(state, day, result, occurred_at)
        committed = self._commit(list(seeded) + diff, expected=dict(baseline))

        promoted = sorted(rid for rid in before_waiting if rid in result.scheduled)
        target_rec = state.get(target_id) if target_id else None
        target_plan = result.scheduled.get(target_id) if target_id else None
        target_conflicts = result.failed.get(target_id, []) if target_id else []
        status = "scheduled"
        waitlist_seq = None
        if target_id and target_id in result.failed:
            if target_rec is not None and target_rec.arrived_minute is not None:
                # 车辆已在场但当前确实没有可行槽位：保持到场状态，等待告警
                status = "arrived_waiting"
            else:
                status = "waitlisted"
                waitlist_seq = (
                    target_rec.waitlist_seq
                    if target_rec and target_rec.is_waitlisted
                    else self.load_state().get(target_id).waitlist_seq
                )
        return CommandResult(
            status=status,
            reservation_id=target_id or "",
            plan=target_plan,
            conflicts=target_conflicts,
            displaced=sorted(result.displaced),
            promoted=promoted,
            events=committed,
            waitlist_seq=waitlist_seq,
        )

    def _diff_events(
        self,
        state: State,
        day: str,
        result: SchedulingResult,
        occurred_at: str,
        *,
        forced: str | None = None,
        force_meta: dict[str, str] | None = None,
    ) -> list[Event]:
        events: list[Event] = []
        waitlist_counter = state.waitlist_counters.get(day, 0)
        reservations = sorted(
            (r for r in state.by_day(day)
             if r.state
             not in {
                 ReservationState.CANCELLED.value,
                 ReservationState.COMPLETED.value,
                 ReservationState.REJECTED.value,
             }),
            key=lambda r: r.reservation_id,
        )

        for rec in reservations:
            rid = rec.reservation_id
            if rid in result.scheduled:
                plan = result.scheduled[rid]
                became_promoted = rec.is_waitlisted
                if (
                    became_promoted
                    or rec.plan is None
                    or self._plan_changed(rec.plan, plan)
                ):
                    if forced == rid:
                        payload = {
                            "approver": (force_meta or {}).get("approver", ""),
                            "note": (force_meta or {}).get("note", ""),
                            "displaced": sorted(result.displaced),
                            "start_minute": (force_meta or {}).get("start_minute"),
                            "plan": plan.to_dict(),
                        }
                        events.append(
                            make_event(
                                RESERVATION_OVERRIDDEN, rid, 0,
                                occurred_at, payload,
                            )
                        )
                    else:
                        etype = (
                            RESERVATION_PROMOTED if became_promoted
                            else RESERVATION_SCHEDULED
                        )
                        events.append(
                            make_event(
                                etype, rid, 0, occurred_at,
                                {"plan": plan.to_dict()},
                            )
                        )
            elif (
                rid in result.failed
                and not rec.immovable
                and rec.arrived_minute is None
            ):
                # 未到场且排不上：进入/回到候补。已持有候补序号的
                # （候补车辆改期后仍排不上）保留原序号，绝不新发序号。
                if rec.waitlist_seq is None:
                    waitlist_counter += 1
                    seq_no = waitlist_counter
                else:
                    seq_no = rec.waitlist_seq
                if not rec.is_waitlisted:
                    events.append(
                        make_event(
                            RESERVATION_WAITLISTED, rid, 0, occurred_at,
                            {
                                "seq": seq_no,
                                "reason": {
                                    "conflicts": [
                                        c.to_dict() for c in result.failed[rid]
                                    ]
                                },
                            },
                        )
                    )

        return events

    @staticmethod
    def _plan_changed(before: Plan, after: Plan) -> bool:
        if len(before.operations) != len(after.operations):
            return True
        for a, b in zip(before.operations, after.operations):
            if (
                a.resource_type != b.resource_type
                or a.resource_id != b.resource_id
                or a.start_minute != b.start_minute
                or a.end_minute != b.end_minute
            ):
                return True
        return False
