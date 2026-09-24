"""对外应用服务：命令入口。

每个命令都遵循同一套流程：

1. 重放事件日志得到当前状态（重启后候补序号、告警、停用区间全部还在）；
2. 调用纯函数排程引擎产出 *事件*（当前时间由调用方显式传入）；
3. 以乐观版本号原子提交，并发冲突时重新加载并重算（容量因此绝不被突破）。

幂等键相同的命令重放只返回已提交事件；排程结果只依赖事件日志与注入时间，
同一段预约事件重放任意次都产生相同排程。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from . import events as evt
from . import scheduler
from .catalog import Catalog
from .events import Event
from .models import CommandResult, Declaration, Plan, Violation
from .state import (
    ARRIVED, PROCESSING, SCHEDULED, WAITLISTED, replay,
)
from .store import ConflictError, EventStore
from .timepolicy import market_zone, to_minute


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    timezone: str = "Asia/Shanghai"
    max_conflict_retries: int = 5


class SchedulingService:
    def __init__(self, store: EventStore, catalog: Catalog,
                 config: ServiceConfig | None = None,
                 scheduler_config: scheduler.SchedulerConfig | None = None) -> None:
        self.store = store
        self.catalog = catalog
        self.config = config or ServiceConfig()
        self.tz = market_zone(self.config.timezone)
        self.scheduler_config = scheduler_config or scheduler.SchedulerConfig(
            tz=self.tz)

    # ------------------------------------------------------------ 内部设施
    def _now(self, at: Any) -> int:
        if at is None:
            return int(time.time() // 60)
        return to_minute(at, self.tz)

    def _state(self):
        all_events = self.store.load()
        return replay(all_events), all_events

    def _commit(
        self,
        plan_fn: Callable[[Any, int], tuple[list[Event], dict]],
        *,
        idem_key: str | None = None,
    ) -> tuple[list[Event], dict]:
        """加载状态 -> 生成事件 -> 乐观提交，冲突自动重算。"""
        if idem_key is not None:
            cached = self.store.lookup_idempotency(idem_key)
            if cached is not None:
                return cached, self._meta_from_events(cached)
        for attempt in range(self.config.max_conflict_retries + 1):
            state, _ = self._state()
            events, meta = plan_fn(state, state.version)
            # 统一按提交顺序重排内存事件序号（落库以这里的顺序为准）
            events = [Event(seq=state.version + i + 1, event_id=e.event_id,
                            event_type=e.event_type,
                            aggregate_id=e.aggregate_id,
                            occurred_at=e.occurred_at, payload=e.payload)
                      for i, e in enumerate(events)]
            try:
                stored = self.store.append(events, state.version, idem_key)
                return stored, meta
            except ConflictError:
                if attempt == self.config.max_conflict_retries:
                    raise
        raise RuntimeError("unreachable")

    @staticmethod
    def _meta_from_events(events: list[Event]) -> dict:
        """幂等重放时从已提交事件重建命令结果元数据。"""
        from .models import Violation
        meta: dict = {"accepted": bool(events)}
        for e in events:
            p = e.payload
            if e.event_type in (evt.RESERVATION_SCHEDULED,
                                evt.RESERVATION_MANUAL_INSERTED,
                                evt.WAITLIST_ACCEPTED):
                if "plan" in p:
                    meta["plan"] = Plan.from_payload(p["plan"])
            if e.event_type == evt.RESERVATION_REJECTED:
                meta["accepted"] = False
                meta["violations"] = [
                    Violation.from_payload(v) for v in p.get("violations", [])]
            if e.event_type == evt.RESERVATION_WAITLISTED:
                meta["diverted_to_waitlist"] = True
                meta.setdefault("violations", [
                    Violation.from_payload(v)
                    for v in p.get("violations", [])])
            if (e.event_type == evt.RESERVATION_POSTPONED
                    and not p.get("present")):
                # 改期/重排导致回到候补：容量冲突，命令未按请求成功
                meta["accepted"] = False
                meta["diverted_to_waitlist"] = True
                meta["violations"] = [
                    Violation.from_payload(v)
                    for v in p.get("violations", [])]
        return meta

    @staticmethod
    def _result(stored: list[Event], meta: dict) -> CommandResult:
        result = CommandResult(
            accepted=meta.get("accepted", bool(stored)),
            events=stored,
            violations=meta.get("violations", []),
            plan=meta.get("plan"),
            diverted_to_waitlist=meta.get("diverted_to_waitlist", False),
            version=stored[-1].seq if stored else meta.get("version", 0),
        )
        return result

    def _replan(self, state, now: int, *, fresh: dict | None = None,
                manual_rid: str | None = None, anchors: dict | None = None,
                amended: set[str] | None = None,
                restrict: set[str] | None = None,
                include_waitlist: bool = True):
        return scheduler.replan(
            state, self.catalog, now, self.scheduler_config,
            fresh=fresh, manual_rid=manual_rid, anchors=anchors,
            amended=amended, restrict=restrict,
            include_waitlist=include_waitlist)

    # ------------------------------------------------------------ 申报
    def request_reservation(self, decl: Declaration, *, now: Any = None,
                            idem_key: str | None = None) -> CommandResult:
        """承运方申报到场区间、温区、托盘数与最长暴露时间。"""
        self._validate_declaration(decl)
        at = self._now(now)

        def plan_fn(state, version):
            if state.statuses.get(decl.reservation_id) is not None:
                raise ValueError(f"预约已存在：{decl.reservation_id}")
            requested = Event(
                seq=version + 1, event_id="",
                event_type=evt.RESERVATION_REQUESTED,
                aggregate_id=decl.reservation_id, occurred_at=at,
                payload={**decl.to_payload(self.tz),
                         "idempotency_key": idem_key})
            outcome = self._replan(state, at, fresh={decl.reservation_id: decl},
                                include_waitlist=False)
            events = [requested, *outcome.events]
            rid = decl.reservation_id
            if rid in outcome.plans:
                meta = {"plan": outcome.plans[rid], "accepted": True}
            elif rid in outcome.rejected_new:
                meta = {"accepted": False, "plan": None,
                        "violations": outcome.violations_by_rid.get(rid, [])}
            else:
                vs = outcome.violations_by_rid.get(rid, [])
                meta = {"accepted": True, "plan": None,
                        "diverted_to_waitlist": True, "violations": vs}
            return events, meta

        stored, meta = self._commit(plan_fn, idem_key=idem_key)
        return self._result(stored, meta)

    def explain(self, decl: Declaration, *, at: Any = None) -> list[Violation]:
        """只探测不提交：返回在期望时间点真正冲突的约束（用于改期咨询）。"""
        self._validate_declaration(decl)
        t = self._now(at)
        state, _ = self._state()
        movable = {rid for rid, st in state.statuses.items()
                   if st == SCHEDULED}
        tl = scheduler.build_timelines(state, self.catalog, movable)
        earliest = max(t, decl.window_start)
        plan, violations = scheduler.try_place(
            decl, self.catalog, tl, earliest, decl.window_end)
        if plan is not None:
            return []
        return violations

    def amend_reservation(self, reservation_id: str, decl: Declaration, *,
                          now: Any = None) -> CommandResult:
        """承运方申请改期（改窗口/温区/托盘/暴露时间）。

        已进场或已终结的预约不可改期；容量不允许时返回真实冲突约束，
        已排定的预约因此回到候补（原序号重新排队）。
        """
        self._validate_declaration(decl)
        at = self._now(now)

        def plan_fn(state, version):
            status = state.statuses.get(reservation_id)
            if status is None:
                raise ValueError(f"预约不存在：{reservation_id}")
            if status in (ARRIVED, PROCESSING, "completed", "cancelled",
                          "rejected"):
                raise ValueError(
                    f"当前状态不能改期：{reservation_id}（{status}）")
            old_decl = state.declarations[reservation_id]
            amended = Event(
                seq=version + 1, event_id="",
                event_type=evt.RESERVATION_AMENDED,
                aggregate_id=reservation_id, occurred_at=at,
                payload={"declaration": decl.to_payload(self.tz),
                         "previous": old_decl.to_payload(self.tz),
                         "at": at})
            # 镜像新申报后重排（旧方案被整笔撤装）
            state.declarations[reservation_id] = decl
            outcome = self._replan(state, at, amended={reservation_id},
                                include_waitlist=False)
            rid = reservation_id
            if rid in outcome.plans:
                meta = {"accepted": True, "plan": outcome.plans[rid]}
            else:
                meta = {"accepted": False,
                        "violations": outcome.violations_by_rid.get(rid, [])}
            return [amended, *outcome.events], meta

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    # ------------------------------------------------------------ 到场
    def report_arrival(self, reservation_id: str, arrived_at: Any,
                       *, now: Any = None) -> CommandResult:
        """车辆实际到场（提前/迟到）。已进场车辆保持原位，只重排后续。"""
        arrival = to_minute(arrived_at, self.tz)
        at = self._now(now)

        def plan_fn(state, version):
            status = state.statuses.get(reservation_id)
            if status is None:
                raise ValueError(f"预约不存在：{reservation_id}")
            if status in ("completed", "cancelled", "rejected"):
                raise ValueError(f"预约已终结：{reservation_id}（{status}）")
            if reservation_id in state.arrived_at:
                raise ValueError(f"车辆已登记到场：{reservation_id}")

            fact = Event(
                seq=version + 1, event_id="",
                event_type=evt.VEHICLE_ARRIVED,
                aggregate_id=reservation_id, occurred_at=at,
                payload={"arrived_at": arrival,
                         "arrived_at_iso": _iso(arrival, self.tz)})
            plan = state.plans.get(reservation_id)
            # 到场不晚于计划靠台时刻：等待原档即可，保持原位
            if (status == SCHEDULED and plan is not None
                    and arrival <= plan.dock_start):
                return [fact], {"accepted": True, "plan": plan}
            # 迟到错过原档起点，或到场的是候补车辆：围绕实际到场重排
            outcome = self._replan(
                state, at, anchors={reservation_id: arrival},
                include_waitlist=False)
            meta = {"accepted": True,
                    "plan": outcome.plans.get(reservation_id),
                    "violations": outcome.violations_by_rid.get(
                        reservation_id, [])}
            return [fact, *outcome.events], meta

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    # ------------------------------------------------------------ 候补接纳
    def accept_waitlist(self, reservation_id: str, *, dispatcher: str = "",
                        now: Any = None) -> CommandResult:
        """调度员接纳候补。并发接纳由乐观锁串行化，绝不突破容量。"""
        at = self._now(now)

        def plan_fn(state, version):
            status = state.statuses.get(reservation_id)
            if status is None:
                raise ValueError(f"预约不存在：{reservation_id}")
            if status == SCHEDULED:
                # 并发落败：另一个调度员已把该候补接纳，容量判定以其提交为准
                return [], {
                    "accepted": False,
                    "violations": [Violation(
                        code="waitlist_closed",
                        resource_kind="waitlist", resource_id=reservation_id,
                        message="该候补已被其他调度员接纳")],
                    "version": version}
            if status != WAITLISTED:
                raise ValueError(
                    f"预约不在候补队列：{reservation_id}（{status}）")
            # FIFO：只能接纳到目标序号为止的前缀，不得越过更早的候补
            pos = state.waitlist_position(reservation_id) or 1
            prefix = state.waitlist[:pos]
            outcome = self._replan(state, at, restrict=set(prefix))
            rid = reservation_id
            # 严格 FIFO：前缀中第一个未排下的候补会挡住其后所有接纳
            blocked = next((w for w in prefix if w not in outcome.plans), None)
            if blocked is not None and blocked != rid:
                # 撤回目标接纳事件（其余前缀接纳保留）
                outcome.events = [
                    e for e in outcome.events
                    if not (e.aggregate_id == rid
                            and e.event_type == evt.WAITLIST_ACCEPTED)]
                outcome.plans.pop(rid, None)
                meta = {
                    "accepted": False,
                    "violations": [Violation(
                        code="waitlist_order",
                        resource_kind="waitlist", resource_id=blocked,
                        message=(f"候补序号 {state.waitlist_position(blocked)} "
                                 f"（{blocked}）尚未接纳，不能越过它接纳"
                                 f"序号 {pos}（{rid}）"),
                        required=state.waitlist_position(blocked),
                        capacity=pos - 1)]}
            elif rid in outcome.plans:
                plan_event = next(
                    (e for e in outcome.events
                     if e.aggregate_id == rid
                     and e.event_type == evt.WAITLIST_ACCEPTED), None)
                if plan_event is not None:
                    plan_event.payload["dispatcher"] = dispatcher
                meta = {"accepted": True, "plan": outcome.plans[rid]}
            else:
                meta = {"accepted": False,
                        "violations": outcome.violations_by_rid.get(rid, [])}
            return outcome.events, meta

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    # ------------------------------------------------------------ 人工插单
    def manual_insert(self, decl: Declaration, *, approver: str,
                      reason: str = "", now: Any = None) -> CommandResult:
        """批准人授权的人工插单。

        成功时记录批准人与被延后批次；容量不足时拒绝并返回真实冲突，
        人工插单同样不得突破任何资源上限。
        """
        if not approver:
            raise ValueError("人工插单必须记录批准人")
        self._validate_declaration(decl)
        at = self._now(now)
        manual_decl = Declaration(
            reservation_id=decl.reservation_id, carrier=decl.carrier,
            vehicle_id=decl.vehicle_id, zone=decl.zone,
            pallets=decl.pallets, window_start=decl.window_start,
            window_end=decl.window_end,
            max_exposure_minutes=decl.max_exposure_minutes, priority=0)

        def plan_fn(state, version):
            if state.statuses.get(decl.reservation_id) is not None:
                raise ValueError(f"预约已存在：{decl.reservation_id}")
            outcome = self._replan(
                state, at, fresh={decl.reservation_id: manual_decl},
                manual_rid=decl.reservation_id,
                include_waitlist=False)
            plan = outcome.manual_plan
            if plan is None:
                return [], {
                    "accepted": False,
                    "violations": outcome.violations_by_rid.get(
                        decl.reservation_id, []),
                    "version": version,
                }
            inserted = Event(
                seq=version + 1, event_id="",
                event_type=evt.RESERVATION_MANUAL_INSERTED,
                aggregate_id=decl.reservation_id, occurred_at=at,
                payload={
                    "declaration": manual_decl.to_payload(self.tz),
                    "plan": plan.to_payload(self.tz),
                    "approver": approver,
                    "reason": reason,
                    "postponed_batches": list(outcome.displaced),
                    "inserted_at": at,
                })
            # 插单先于其引发的延后/改期事件落日志
            return [inserted, *outcome.events], {
                "accepted": True, "plan": plan}

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    # ------------------------------------------------------------ 设备停用
    def disable_resource(self, kind: str, resource_id: str,
                         start: Any, end: Any | None = None, *,
                         reason: str = "", now: Any = None) -> CommandResult:
        """设备停用：撤下受影响的未开始作业并重排，已进场作业不动。"""
        at = self._now(now)
        s = to_minute(start, self.tz)
        e = to_minute(end, self.tz) if end is not None else None
        if end is not None and e <= s:
            raise ValueError("停用区间结束必须晚于开始")
        if not self.catalog.has_resource(kind, resource_id):
            raise ValueError(f"资源不存在：{kind}/{resource_id}")

        def plan_fn(state, version):
            disabled = Event(
                seq=version + 1, event_id="",
                event_type=evt.EQUIPMENT_DISABLED,
                aggregate_id=resource_id, occurred_at=at,
                payload={"resource_kind": kind, "resource_id": resource_id,
                         "start": s, "end": e,
                         "start_iso": _iso(s, self.tz),
                         "end_iso": _iso(e, self.tz) if e is not None else None,
                         "reason": reason})
            # 镜像停用区间，使本轮重排立刻可见
            state.downtime.setdefault(resource_id, []).append([s, e])
            outcome = self._replan(state, at, include_waitlist=False)
            return [disabled, *outcome.events], {
                "accepted": True,
                "displaced": list(outcome.displaced),
                "violations": [v for vs in outcome.violations_by_rid.values()
                               for v in vs
                               if v.resource_id == resource_id]}

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    def enable_resource(self, kind: str, resource_id: str, *,
                        start: Any = None, now: Any = None) -> CommandResult:
        """设备恢复：闭合停用区间并重排以接纳候补。"""
        at = self._now(now)
        s = to_minute(start, self.tz) if start is not None else at
        if not self.catalog.has_resource(kind, resource_id):
            raise ValueError(f"资源不存在：{kind}/{resource_id}")

        def plan_fn(state, version):
            enabled = Event(
                seq=version + 1, event_id="",
                event_type=evt.EQUIPMENT_ENABLED,
                aggregate_id=resource_id, occurred_at=at,
                payload={"resource_kind": kind, "resource_id": resource_id,
                         "start": s, "start_iso": _iso(s, self.tz)})
            # 设备提前恢复：闭合开放区间，或把覆盖该时刻的计划停用区间缩短到 s
            intervals = state.downtime.setdefault(resource_id, [])
            for interval in reversed(intervals):
                ds, de = interval
                if de is None or (ds <= s and s < de):
                    interval[1] = s
                    break
            outcome = self._replan(state, at, include_waitlist=False)
            return [enabled, *outcome.events], {"accepted": True}

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    # ------------------------------------------------------------ 作业生命周期
    def start_operation(self, reservation_id: str, *, now: Any = None
                        ) -> CommandResult:
        """车辆靠台开始作业（此后冻结，任何重排不再移动它）。"""
        at = self._now(now)

        def plan_fn(state, version):
            status = state.statuses.get(reservation_id)
            if status not in (SCHEDULED, ARRIVED):
                raise ValueError(
                    f"当前状态不能开始作业：{reservation_id}（{status}）")
            event = Event(
                seq=version + 1, event_id="",
                event_type=evt.OPERATION_STARTED,
                aggregate_id=reservation_id, occurred_at=at,
                payload={"started_at": at,
                         "started_at_iso": _iso(at, self.tz)})
            # 镜像"作业中"：该作业即刻冻结，重排不得再移动
            state.statuses[reservation_id] = PROCESSING
            if reservation_id in state.waitlist:
                state.waitlist.remove(reservation_id)
            outcome = self._replan(state, at, include_waitlist=False)
            return [event, *outcome.events], {"accepted": True}

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    def complete_reservation(self, reservation_id: str, *, now: Any = None
                             ) -> CommandResult:
        """作业完成：月台/预冷间释放（按方案时间自然释放），并重排候补。"""
        at = self._now(now)

        def plan_fn(state, version):
            status = state.statuses.get(reservation_id)
            if status not in (ARRIVED, PROCESSING):
                raise ValueError(
                    f"当前状态不能完成：{reservation_id}（{status}）")
            event = Event(
                seq=version + 1, event_id="",
                event_type=evt.RESERVATION_COMPLETED,
                aggregate_id=reservation_id, occurred_at=at,
                payload={"completed_at": at})
            # 镜像终态：月台/预冷间随之释放，库位保留到出库
            state.statuses[reservation_id] = "completed"
            if reservation_id in state.waitlist:
                state.waitlist.remove(reservation_id)
            outcome = self._replan(state, at, include_waitlist=False)
            return [event, *outcome.events], {"accepted": True}

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    def release_goods(self, reservation_id: str, *, now: Any = None
                      ) -> CommandResult:
        """货物出库：释放低温库库位。"""
        at = self._now(now)

        def plan_fn(state, version):
            if reservation_id not in state.plans:
                raise ValueError(f"预约没有入库方案：{reservation_id}")
            event = Event(
                seq=version + 1, event_id="",
                event_type=evt.GOODS_RELEASED,
                aggregate_id=reservation_id, occurred_at=at,
                payload={"released_at": at})
            state.storage_released.add(reservation_id)
            outcome = self._replan(state, at, include_waitlist=False)
            return [event, *outcome.events], {"accepted": True}

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    def cancel_reservation(self, reservation_id: str, *, reason: str = "",
                           now: Any = None) -> CommandResult:
        at = self._now(now)

        def plan_fn(state, version):
            status = state.statuses.get(reservation_id)
            if status in (None, "completed", "cancelled"):
                raise ValueError(f"预约不可取消：{reservation_id}（{status}）")
            event = Event(
                seq=version + 1, event_id="",
                event_type=evt.RESERVATION_CANCELLED,
                aggregate_id=reservation_id, occurred_at=at,
                payload={"reason": reason, "cancelled_at": at})
            # 镜像终态：撤掉该预约的全部占用后重排
            state.statuses[reservation_id] = "cancelled"
            state.plans.pop(reservation_id, None)
            if reservation_id in state.waitlist:
                state.waitlist.remove(reservation_id)
            state.waitlist_since.pop(reservation_id, None)
            outcome = self._replan(state, at, include_waitlist=False)
            return [event, *outcome.events], {"accepted": True}

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    # ------------------------------------------------------------ 巡检与告警
    def sweep(self, *, now: Any = None) -> CommandResult:
        """周期巡检：重排接纳候补；候补等待超时则产生持久告警。"""
        at = self._now(now)

        def plan_fn(state, version):
            # 先处理窗口已永久错过的候补：过期拒绝，释放候补队列
            expired: list[Event] = []
            for rid in list(state.waitlist):
                decl = state.declarations.get(rid)
                if decl is not None and decl.window_end <= at:
                    state.waitlist.remove(rid)
                    state.waitlist_since.pop(rid, None)
                    state.statuses[rid] = "rejected"
                    state.plans.pop(rid, None)
                    expired.append(Event(
                        seq=version + len(expired) + 1, event_id="",
                        event_type=evt.RESERVATION_REJECTED,
                        aggregate_id=rid, occurred_at=at,
                        payload={"reason": "window_expired",
                                 "window_end": decl.window_end,
                                 "window_end_iso": _iso(decl.window_end,
                                                        self.tz)}))
            outcome = self._replan(state, at)
            # 本轮刚被接纳的候补不再计算超时
            accepted_now = {e.aggregate_id for e in outcome.events
                            if e.event_type == evt.WAITLIST_ACCEPTED}
            timeout = self.scheduler_config.waitlist_timeout
            extra: list[Event] = []
            for rid in state.waitlist:
                if rid in accepted_now:
                    continue
                since = state.waitlist_since.get(rid, at)
                key = f"waitlist_timeout:{rid}"
                if at - since >= timeout and not state.alert_active(key):
                    extra.append(Event(
                        seq=version + len(expired)
                        + len(outcome.events) + len(extra) + 1,
                        event_id="", event_type=evt.ALERT_RAISED,
                        aggregate_id=rid, occurred_at=at,
                        payload={"key": key, "code": "waitlist_timeout",
                                 "at": at, "since": since,
                                 "waitlist_position":
                                     state.waitlist_position(rid),
                                 "message": ("候补等待已超过 "
                                             f"{timeout} 分钟")}))
            return [*expired, *outcome.events, *extra], {"accepted": True}

        stored, meta = self._commit(plan_fn)
        return self._result(stored, meta)

    # ------------------------------------------------------------ 查询
    def snapshot(self):
        """返回重放后的状态（测试与查询用）。"""
        return self._state()[0]

    # ------------------------------------------------------------ 校验
    def _validate_declaration(self, decl: Declaration) -> None:
        if not decl.reservation_id:
            raise ValueError("预约号不能为空")
        if decl.pallets <= 0:
            raise ValueError("托盘数必须为正")
        if decl.window_end <= decl.window_start:
            raise ValueError("到场区间结束必须晚于开始")
        if decl.max_exposure_minutes <= 0:
            raise ValueError("最长暴露时间必须为正")
        if not decl.zone:
            raise ValueError("温区不能为空")


def _iso(minute: int | None, tz) -> str | None:
    if minute is None:
        return None
    from .timepolicy import iso
    return iso(minute, tz)
