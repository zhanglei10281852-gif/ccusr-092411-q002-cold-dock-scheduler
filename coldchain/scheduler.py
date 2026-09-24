"""确定性排程引擎。

规则要点：

- 已进场（arrived/processing/completed）的作业作为锁定占用进入台账，
  引擎绝不移动它们；只编排尚未开始的预约；
- 候选顺序固定：被强制的插单 → 候补（按候补序号）→ 其他（按申报时间、ID），
  因此同一事件流无论重放多少次，候选顺序与资源首选都完全一致；
- 资源首选按资源 ID 升序，开始时间逐分钟前探，取最早可行解；
- 排不下时返回真正卡住的约束（容量 + 占用批次、设备停用、暴露超时、
  窗口/温区），供拒绝或改期响应直接使用。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .capacity import Ledger, Resource
from .models import DOCK, FLOW, PRECOOL, STORAGE, Conflict, Operation, Plan
from .state import Reservation
from .timeutil import TimeService


@dataclass(frozen=True)
class StageProfile:
    """各阶段标准作业时长（分钟）。"""

    dock_minutes: int = 30
    precool_minutes: int = 60
    storage_minutes: int = 20

    def duration(self, resource_type: str) -> int:
        return {
            DOCK: self.dock_minutes,
            PRECOOL: self.precool_minutes,
            STORAGE: self.storage_minutes,
        }[resource_type]


@dataclass(frozen=True)
class ForceSlot:
    """人工插单的强制槽位。"""

    start_minute: int
    approver: str
    note: str = ""


@dataclass
class SchedulingResult:
    scheduled: dict[str, Plan] = field(default_factory=dict)
    failed: dict[str, list[Conflict]] = field(default_factory=dict)
    order: list[str] = field(default_factory=list)
    displaced: list[str] = field(default_factory=list)

    def conflicts_for(self, reservation_id: str) -> list[Conflict]:
        return self.failed.get(reservation_id, [])


class Scheduler:
    def __init__(
        self,
        time: TimeService,
        resources: list[Resource],
        profile: StageProfile | None = None,
        horizon: int = 2 * 1440,
    ) -> None:
        self.time = time
        self.profile = profile or StageProfile()
        self.horizon = horizon
        self.resource_catalog = resources

    # -- 主入口 ---------------------------------------------------------

    def plan_day(
        self,
        day: str,
        reservations: list[Reservation],
        disables: list | None = None,
        force: tuple[str, ForceSlot] | None = None,
        now_minute: int | None = None,
    ) -> SchedulingResult:
        ledger = Ledger(self.resource_catalog, horizon=self.horizon)
        for interval in disables or []:
            ledger.disable(
                interval.resource_type,
                interval.resource_id,
                interval.start_minute,
                interval.end_minute,
            )

        frozen: set[str] = set()
        movable: list[Reservation] = []
        result = SchedulingResult()

        for rec in reservations:
            if rec.plan is None or not rec.plan.operations:
                movable.append(rec)
                continue
            if rec.immovable:
                frozen.add(rec.reservation_id)
                for op in rec.plan.operations:
                    locked = Operation(**{**op.to_dict(), "locked": True})
                    ledger.add(rec.reservation_id, locked)
            else:
                movable.append(rec)

        ordered = self._order(movable, force)
        result.order = [r.reservation_id for r in ordered]

        forced_id = force[0] if force else None
        for rec in ordered:
            fixed_start = (
                force[1].start_minute
                if force and rec.reservation_id == forced_id
                else None
            )
            placed = self._place(ledger, day, rec, fixed_start, now_minute)
            if isinstance(placed, Plan):
                result.scheduled[rec.reservation_id] = placed
                for op in placed.operations:
                    ledger.add(rec.reservation_id, op)
            else:
                result.failed[rec.reservation_id] = placed

        if force is not None:
            forced_id, _slot = force
            previous = {
                r.reservation_id: r.plan
                for r in reservations
                if r.plan is not None and r.reservation_id not in frozen
            }
            for rid in result.order:
                if rid == forced_id or rid not in previous:
                    continue
                new_plan = result.scheduled.get(rid)
                if (
                    new_plan is None
                    or new_plan.start_minute != previous[rid].start_minute
                ):
                    result.displaced.append(rid)

        return result

    # -- 候选排序 -------------------------------------------------------

    @staticmethod
    def _order(
        movable: list[Reservation],
        force: tuple[str, ForceSlot] | None,
    ) -> list[Reservation]:
        def key(rec: Reservation) -> tuple:
            if force and rec.reservation_id == force[0]:
                rank = 0
            elif rec.plan is not None and rec.waitlist_seq is None:
                # 已排程者优先保住既有槽位（设备停用时才被迫移动）
                rank = 1
            elif rec.waitlist_seq is not None:
                # 候补按原有候补序号 FCFS，只填释放出的空位
                rank = 2
            else:
                # 新申报按申报时间
                rank = 3
            anchor = (
                rec.plan.start_minute
                if rec.plan is not None and rec.plan.start_minute is not None
                else (rec.waitlist_seq if rec.waitlist_seq is not None else 0)
            )
            return (rank, anchor, rec.created_at or "", rec.reservation_id)

        return sorted(movable, key=key)

    # -- 单预约放置 -----------------------------------------------------

    @staticmethod
    def _arrival_anchor(rec: Reservation) -> int | None:
        """实际到场分钟优先；否则取人工插单批准的到场分钟。"""
        if rec.arrived_minute is not None:
            return rec.arrived_minute
        return rec.forced_start_minute

    def _dock_bounds(
        self,
        rec: Reservation,
        fixed_start: int | None,
        now_minute: int | None,
    ) -> tuple[int, int]:
        earliest = rec.window_start_minute
        latest = rec.window_end_minute - 1  # 未到场：开始分钟必须落在申报区间内
        anchor = self._arrival_anchor(rec)
        if anchor is not None:
            earliest = max(earliest, anchor)
            # 车辆已到场（迟到/排队）或已被批准到指定槽位：
            # 申报窗口不再是上界，改由锚点 + 最长暴露时间约束。
            latest = self.horizon - 1
        # 任何靠台安排都不能落在已过去的分钟（命令时刻为下界）。
        # 命令发生在营业日之前时 now_minute 为 None，不构成限制。
        if now_minute is not None:
            earliest = max(earliest, now_minute)
        if fixed_start is not None:
            earliest = latest = fixed_start
        return max(0, earliest), min(latest, self.horizon - 1)

    def _place(
        self,
        ledger: Ledger,
        day: str,
        rec: Reservation,
        fixed_start: int | None,
        now_minute: int | None = None,
    ) -> Plan | list[Conflict]:
        durations = {stage: self.profile.duration(stage) for stage in FLOW}

        if rec.window_start_minute >= rec.window_end_minute:
            return [
                Conflict(
                    resource_type=DOCK, resource_id="-",
                    constraint="window", minute=rec.window_start_minute,
                    limit=rec.window_end_minute,
                    required=rec.window_start_minute,
                    available=rec.window_end_minute,
                    message="申报到场区间非法：结束分钟不晚于开始分钟",
                )
            ]

        dock_earliest, dock_latest = self._dock_bounds(rec, fixed_start, now_minute)
        if dock_earliest > dock_latest:
            return [
                Conflict(
                    resource_type=DOCK, resource_id="-",
                    constraint="window", minute=dock_earliest,
                    limit=dock_latest, required=dock_earliest,
                    available=dock_latest,
                    message="到场区间内不存在可开始靠台的分钟（迟到/窗口已过）",
                )
            ]

        # 暴露时间（到场 → 进入预冷间）是冷链安全硬约束：
        # - 已实际到场：从实际到场分钟起算，与靠台候选无关；
        # - 尚未到场（含人工插单）：承运方按批准的靠台时刻到场，
        #   因此每个靠台候选的暴露起点就是该候选本身。
        # - 人工插单车辆未到场时，锚点为批准分钟，与候选一致。
        anchor = self._arrival_anchor(rec)
        fixed_exposure_origin = (
            anchor if rec.arrived_minute is not None else None
        )

        conflicts: list[Conflict] = []
        for dock_start in range(dock_earliest, dock_latest + 1):
            exposure_origin = (
                fixed_exposure_origin
                if fixed_exposure_origin is not None
                else dock_start
            )
            precool_latest = (
                min(self.horizon - durations[PRECOOL],
                    exposure_origin + rec.max_exposure_minutes)
                if rec.max_exposure_minutes
                else self.horizon - durations[PRECOOL]
            )
            dock_op, dock_conflict = self._find_slot(
                ledger, day, rec, DOCK,
                dock_start, dock_start, durations[DOCK],
            )
            if dock_op is None:
                conflicts.append(dock_conflict)
                continue

            precool_op, precool_conflict = self._find_slot(
                ledger, day, rec, PRECOOL,
                dock_op.end_minute, precool_latest, durations[PRECOOL],
            )
            if precool_op is None:
                conflicts.append(precool_conflict)
                continue

            storage_op, storage_conflict = self._find_slot(
                ledger, day, rec, STORAGE,
                precool_op.end_minute,
                self.horizon - durations[STORAGE],
                durations[STORAGE],
            )
            if storage_op is None:
                conflicts.append(storage_conflict)
                continue

            return Plan(
                reservation_id=rec.reservation_id,
                day=day,
                operations=[dock_op, precool_op, storage_op],
                state="scheduled",
                version=rec.version + 1,
            )

        # 最早可靠台分钟已使暴露上限不可能满足时，明确报暴露约束
        earliest_origin = (
            fixed_exposure_origin
            if fixed_exposure_origin is not None
            else dock_earliest
        )
        if rec.max_exposure_minutes and dock_earliest + durations[DOCK] > (
            min(self.horizon, earliest_origin + rec.max_exposure_minutes)
        ):
            conflicts.append(
                Conflict(
                    resource_type=PRECOOL, resource_id="-",
                    constraint="exposure",
                    minute=earliest_origin + rec.max_exposure_minutes,
                    limit=rec.max_exposure_minutes,
                    required=dock_earliest - earliest_origin,
                    available=max(
                        0,
                        rec.max_exposure_minutes
                        - (dock_earliest - earliest_origin),
                    ),
                    message=(
                        f"最早可靠台分钟 {dock_earliest} 已使暴露时间超过上限 "
                        f"{rec.max_exposure_minutes} 分钟"
                    ),
                )
            )
        return _dedup_conflicts(conflicts)

    def _find_slot(
        self,
        ledger: Ledger,
        day: str,
        rec: Reservation,
        stage: str,
        earliest: int,
        latest: int,
        duration: int,
    ) -> tuple[Operation | None, Conflict]:
        """逐分钟前探，每分钟按资源 ID 升序选第一个可行资源。"""
        candidates = ledger.compatible(stage, rec.zone)
        if not candidates:
            return None, Conflict(
                resource_type=stage, resource_id="-", constraint="zone",
                minute=earliest, limit=0, required=rec.pallets, available=0,
                message=f"温区 {rec.zone} 没有可用的 {stage} 资源",
            )
        last_conflict: Conflict | None = None
        if earliest > latest:
            return None, Conflict(
                resource_type=stage, resource_id=candidates[0].resource_id,
                constraint="exposure" if stage == PRECOOL else "window",
                minute=max(0, earliest), limit=max(0, latest),
                required=earliest, available=max(0, latest),
                message=f"{stage} 允许开始区间为空（上游延迟或暴露上限）",
            )
        for start in range(earliest, latest + 1):
            end = start + duration
            if end > self.horizon:
                return None, Conflict(
                    resource_type=stage, resource_id="-", constraint="window",
                    minute=start, limit=self.horizon, required=end,
                    available=self.horizon,
                    message=f"{stage} 作业超出跨午夜排程边界（48 小时）",
                )
            for res in candidates:
                conflict = ledger.check_interval(
                    stage, res.resource_id, start, end, rec.pallets
                )
                if conflict is None:
                    return (
                        Operation(
                            resource_type=stage,
                            resource_id=res.resource_id,
                            start_minute=start,
                            end_minute=end,
                            pallets=rec.pallets,
                            start=self.time.format(
                                self.time.at_minute(day, start)
                            ),
                            end=self.time.format(
                                self.time.at_minute(day, end)
                            ),
                            locked=False,
                        ),
                        Conflict("", "", "", 0, 0),
                    )
                last_conflict = conflict
        return None, last_conflict if last_conflict is not None else Conflict(
            resource_type=stage, resource_id=candidates[0].resource_id,
            constraint="window", minute=earliest, limit=latest,
            required=earliest, available=latest,
            message=f"{stage} 在允许区间内没有可行槽位",
        )


def _dedup_conflicts(conflicts: list[Conflict]) -> list[Conflict]:
    """同一资源同一约束只保留最早分钟、剩余最少的那条。"""
    best: dict[tuple[str, str, str], Conflict] = {}
    for conflict in conflicts:
        if not conflict.constraint:
            continue
        key = (conflict.resource_type, conflict.resource_id, conflict.constraint)
        current = best.get(key)
        if current is None or (conflict.minute, conflict.available) < (
            current.minute,
            current.available,
        ):
            best[key] = conflict
    return sorted(
        best.values(), key=lambda c: (c.minute, c.resource_type, c.resource_id)
    )
