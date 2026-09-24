"""确定性排程引擎。

全部为纯函数：给定折叠状态、资源目录、显式注入的当前时间，输出事件与计划，
不直接写库、不读取系统时钟。

规则要点：
- 已经进场（arrived 且已有方案 / processing / completed）的作业冻结，
  扰动只重排未开始的作业；
- 搜索按"最早可行分钟 -> 资源编号升序"进行，保证同态输入同态结果；
- 拒绝或改期返回真正冲突的约束（资源、分钟、上限、负载）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import events as evt
from .catalog import Catalog
from .events import Event
from .models import Declaration, Plan, Violation
from .state import (
    ACTIVE_STATUSES, ARRIVED, PROCESSING, SCHEDULED, WAITLISTED, State,
)
from .timeline import INF, Timeline
from .timepolicy import market_zone

# 库位占用状态：从排定（按方案的入库时刻起）到出库为止
STORAGE_OCCUPYING = ACTIVE_STATUSES | {"completed"}
# 窗口结束后仍允许向后编排的最大范围（分钟）
DEFAULT_HORIZON = 1440
# 候补等待超时（分钟）
DEFAULT_WAITLIST_TIMEOUT = 30

# 结构性硬冲突：无法靠等待解决，直接拒绝
HARD_VIOLATIONS = frozenset(
    {"window_closed", "zone_unsupported", "exposure_impossible"})


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    horizon: int = DEFAULT_HORIZON
    waitlist_timeout: int = DEFAULT_WAITLIST_TIMEOUT
    tz: Any = field(default=None)  # 市场时区；由服务层注入


def _resolved(config: SchedulerConfig) -> SchedulerConfig:
    if config.tz is not None:
        return config
    return SchedulerConfig(horizon=config.horizon,
                           waitlist_timeout=config.waitlist_timeout,
                           tz=market_zone("Asia/Shanghai"))


# ---------------------------------------------------------------- 时间线构建

def build_timelines(state: State, catalog: Catalog,
                    movable: set[str]) -> dict[str, Timeline]:
    """按当前生效计划构造三类时间线；``movable`` 的旧占用全部撤装。"""
    docks = Timeline()
    precool = Timeline()
    storage = Timeline()

    for rid, plan in state.plans.items():
        status = state.statuses.get(rid)
        if status in ("rejected", "cancelled") or rid in movable:
            continue
        # 月台 / 预冷间只在卸货与预冷窗口内占用
        if status in (SCHEDULED, ARRIVED, PROCESSING):
            docks.add(plan.dock_id, plan.dock_start, plan.dock_end, 1, rid)
            precool.add(plan.precool_id, plan.precool_start,
                        plan.precool_end, plan.pallets, rid)
        # 库位自入库起持续占用，直到出库；完成卸货不释放库位
        if (status in STORAGE_OCCUPYING
                and rid not in state.storage_released):
            storage.add(plan.storage_id, plan.storage_at, INF,
                        plan.pallets, rid)

    for resource_id, intervals in state.downtime.items():
        kind = _resource_kind(catalog, resource_id)
        target = {"dock": docks, "precool": precool,
                  "storage": storage}.get(kind)
        if target is None:
            continue
        for start, end in intervals:
            target.disable(resource_id, start, end if end is not None else INF)

    return {"dock": docks, "precool": precool, "storage": storage}


def _resource_kind(catalog: Catalog, resource_id: str) -> str | None:
    if catalog.has_resource("dock", resource_id):
        return "dock"
    if catalog.has_resource("precool", resource_id):
        return "precool"
    if catalog.has_resource("storage", resource_id):
        return "storage"
    return None


# ---------------------------------------------------------------- 单点探测

def _dock_violation(t0: int, dur_d: int, catalog: Catalog,
                    tl: dict[str, Timeline]) -> Violation | None:
    breaches = []
    docks = catalog.docks()
    for d in docks:
        b = tl["dock"].evaluate(d.dock_id, t0, t0 + dur_d, 1, 1)
        if b is not None:
            breaches.append((d.dock_id, b))
    # 只要还有一个门位空闲，月台就不是真正的瓶颈
    if len(breaches) < len(docks):
        return None
    did, b = min(breaches, key=lambda x: (x[1].first_minute, x[0]))
    return Violation(
        code="equipment_down" if b.equipment_down else "dock_busy",
        resource_kind="dock", resource_id=did,
        at=b.first_minute, start=t0, end=t0 + dur_d,
        capacity=1, load=b.load, required=1,
        message=("月台停用" if b.equipment_down else "所有月台在该时段被占用")
                + f"：{did} 自分钟 {b.first_minute} 起不可用")


def _precool_violation(pre_start: int, dur_p: int, decl: Declaration,
                       catalog: Catalog,
                       tl: dict[str, Timeline]) -> Violation | None:
    rooms = sorted(catalog.precool_rooms(decl.zone), key=lambda r: r.room_id)
    if not rooms:
        return Violation(
            code="zone_unsupported", resource_kind="precool", resource_id="-",
            at=pre_start,
            message=f"没有温区 {decl.zone} 的预冷间")
    breaches = []
    for r in rooms:
        b = tl["precool"].evaluate(r.room_id, pre_start, pre_start + dur_p,
                                   decl.pallets, r.pallet_capacity)
        if b is not None:
            breaches.append((r.room_id, r.pallet_capacity, b))
    if not breaches:
        return None
    rid_, cap, b = min(
        breaches, key=lambda x: (x[2].first_minute, x[2].peak - x[1], x[0]))
    return Violation(
        code="equipment_down" if b.equipment_down else "precool_overload",
        resource_kind="precool", resource_id=rid_,
        at=b.first_minute, start=pre_start, end=pre_start + dur_p,
        capacity=cap, load=b.peak, required=decl.pallets,
        message=("预冷间停用" if b.equipment_down else "预冷间托盘超量")
                + f"：{rid_} 峰值 {b.peak}/{cap}，本单 {decl.pallets} 托")


def _storage_violation(storage_at: int, decl: Declaration, catalog: Catalog,
                       tl: dict[str, Timeline]) -> Violation | None:
    s = catalog.storage_room(decl.zone)
    if s is None:
        return Violation(
            code="zone_unsupported", resource_kind="storage", resource_id="-",
            at=storage_at, message=f"没有温区 {decl.zone} 的低温库")
    b = tl["storage"].evaluate(s.room_id, storage_at, storage_at + 1,
                               decl.pallets, s.pallet_capacity)
    if b is None:
        return None
    return Violation(
        code="equipment_down" if b.equipment_down else "storage_full",
        resource_kind="storage", resource_id=s.room_id,
        at=b.first_minute, start=storage_at,
        capacity=s.pallet_capacity,
        load=(b.load if not b.equipment_down else 0),
        required=decl.pallets,
        message=("低温库停用" if b.equipment_down else "低温库库位不足")
                + f"：{s.room_id} 需 {decl.pallets} 托，"
                  f"当前 {b.load if not b.equipment_down else 0}/"
                  f"{s.pallet_capacity}")


def violations_at(t0: int, decl: Declaration, catalog: Catalog,
                  tl: dict[str, Timeline]) -> list[Violation]:
    """在期望分钟 t0 逐资源探测，返回真正冲突的约束集合。"""
    timing = catalog.timing
    dur_d = timing.dock_duration(decl.zone)
    dur_p = timing.precool_duration(decl.zone)
    out: list[Violation] = []
    if dur_d > decl.max_exposure_minutes:
        out.append(Violation(
            code="exposure_impossible", resource_kind="exposure",
            resource_id="-", at=t0,
            message=(f"月台作业 {dur_d} 分钟已超过申报最长暴露时间 "
                     f"{decl.max_exposure_minutes} 分钟"),
            required=dur_d, capacity=decl.max_exposure_minutes))
    v = _dock_violation(t0, dur_d, catalog, tl)
    if v is not None:
        out.append(v)
    pre_start = t0 + dur_d
    v = _precool_violation(pre_start, dur_p, decl, catalog, tl)
    if v is not None:
        out.append(v)
    storage_at = pre_start + dur_p
    v = _storage_violation(storage_at, decl, catalog, tl)
    if v is not None:
        out.append(v)
    return out


def try_place(decl: Declaration, catalog: Catalog, tl: dict[str, Timeline],
              earliest: int, latest: int, *,
              inclusive: bool = False) -> tuple[Plan | None, list[Violation]]:
    """在到场区间内按分钟搜索最早可行方案。

    依次确定：到场/靠台分钟 -> 月台 -> 预冷间 -> 低温库；
    任一资源不可行则分钟级后移。全区间失败时返回最早时间点的真实冲突。

    承运方申报窗口为半开区间，``latest``（窗口结束）默认不可取；
    到场锚点以暴露时限算出的最后靠台分钟则取闭区间（``inclusive=True``）。
    """
    timing = catalog.timing
    dur_d = timing.dock_duration(decl.zone)
    dur_p = timing.precool_duration(decl.zone)
    storage_room = catalog.storage_room(decl.zone)

    last = latest if inclusive else latest - 1
    if last < earliest:
        return None, [Violation(
            code="window_closed", resource_kind="window",
            resource_id=decl.reservation_id, at=earliest,
            start=decl.window_start, end=decl.window_end,
            message="可接受区间为空（到场区间已关闭或暴露时间不足）",
            required=earliest, capacity=decl.window_end)]
    if dur_d > decl.max_exposure_minutes:
        return None, violations_at(earliest, decl, catalog, tl)
    if storage_room is None:
        return None, violations_at(earliest, decl, catalog, tl)

    rooms = sorted(catalog.precool_rooms(decl.zone), key=lambda r: r.room_id)
    docks = catalog.docks()
    for t in range(earliest, last + 1):
        pre_start = t + dur_d
        storage_at = pre_start + dur_p
        for d in docks:
            if tl["dock"].evaluate(d.dock_id, t, t + dur_d, 1, 1) is not None:
                continue
            for r in rooms:
                if tl["precool"].evaluate(
                        r.room_id, pre_start, pre_start + dur_p,
                        decl.pallets, r.pallet_capacity) is not None:
                    continue
                if tl["storage"].evaluate(
                        storage_room.room_id, storage_at, storage_at + 1,
                        decl.pallets,
                        storage_room.pallet_capacity) is not None:
                    continue
                return Plan(
                    planned_arrival=t,
                    dock_id=d.dock_id, dock_start=t, dock_end=t + dur_d,
                    precool_id=r.room_id, precool_start=pre_start,
                    precool_end=pre_start + dur_p,
                    storage_id=storage_room.room_id, storage_at=storage_at,
                    pallets=decl.pallets, zone=decl.zone), []
    return None, violations_at(earliest, decl, catalog, tl)


def commit_plan(decl: Declaration, plan: Plan, tl: dict[str, Timeline]) -> None:
    """把方案装入时间线，使同轮后续候选可见。"""
    tl["dock"].add(plan.dock_id, plan.dock_start, plan.dock_end, 1,
                   decl.reservation_id)
    tl["precool"].add(plan.precool_id, plan.precool_start, plan.precool_end,
                      plan.pallets, decl.reservation_id)
    tl["storage"].add(plan.storage_id, plan.storage_at, INF, plan.pallets,
                      decl.reservation_id)


# ---------------------------------------------------------------- 重排核心

@dataclass(slots=True)
class ReplanOutcome:
    events: list[Event]
    plans: dict[str, Plan]
    displaced: list[str]              # 被延后回候补的已排定批次（按处理顺序）
    waitlisted_new: list[str]         # 本轮首次进入候补的新申报
    rejected_new: list[str]           # 本轮因结构性冲突必须拒绝的新申报
    violations_by_rid: dict[str, list[Violation]]
    unplaced_anchors: list[str]       # 已到场但暴露时限内排不下
    manual_plan: Plan | None = None


def _order_key(state: State, rid: str, anchors: set[str],
               manual_rid: str | None) -> tuple:
    """重排处理顺序：

    0 已到场锚点（现实时间固定，最先保住）
    1 人工插单（批准的紧急单，允许挤占后续档）
    2 已排定尚未到场（按原时刻原位重放，最小化扰动）
    3 既有候补（按原候补序号，只吃剩余容量，序号重启后仍有效）
    4 本轮新申报（FIFO，不得越过既有候补）
    """
    if rid in anchors:
        group = 0
    elif rid == manual_rid:
        group = 1
    elif rid in state.plans:
        group = 2
    elif state.statuses.get(rid) == WAITLISTED:
        group = 3
    else:
        group = 4
    pos = state.waitlist_position(rid) or 0
    plan = state.plans.get(rid)
    planned = plan.planned_arrival if plan else 0
    decl = state.declarations.get(rid)
    prio = decl.priority if decl else 100
    req = state.requested_at.get(rid, 0)
    return (group, pos, planned, prio, req, rid)


def replan(state: State, catalog: Catalog, now: int,
           config: SchedulerConfig | None = None, *,
           anchors: dict[str, int] | None = None,
           fresh: dict[str, Declaration] | None = None,
           manual_rid: str | None = None,
           amended: set[str] | None = None,
           restrict: set[str] | None = None,
           include_waitlist: bool = True) -> ReplanOutcome:
    """重排所有尚未开始的作业；已进场且已定位的作业保持原位。

    anchors: 到场分钟被现实固定的预约（提前/迟到的本车，或上轮未排下的在场车）；
             其靠台时间只能落在到场后、最长暴露时间之内。
    fresh:   本轮新申报（尚无计划），含人工插单。
    manual_rid: fresh 中按人工插单处理、拥有最高优先且单独发审计事件的预约。
    amended: 本轮改期的既有预约，按新申报窗口重新搜索。
    restrict: 只让给定集合内的既有预约参与（用于候补 FIFO 前缀接纳）。
    include_waitlist: 扰动类命令传 False——候补只由显式接纳或周期巡检放行。
    """
    config = _resolved(config or SchedulerConfig())
    tz = config.tz
    anchors = dict(anchors or {})
    amended = set(amended or ())
    # 上轮到场但至今没有生效方案的车，继续作为锚点重试
    for rid, at in state.arrived_at.items():
        if (state.statuses.get(rid) == ARRIVED and rid not in state.plans
                and rid not in anchors):
            anchors[rid] = at
    fresh = dict(fresh or {})

    # 可移动：尚未到场的生效方案 + 锚点（其旧方案围绕实际到场重算）
    movable = {rid for rid, st in state.statuses.items()
               if st == SCHEDULED}
    movable |= set(anchors)
    if restrict is not None:
        # 限制轮：既有排程保持为固定占用，只放行集合内对象
        movable = (movable & restrict) | set(anchors)
    # 候补（可能无方案）参与本轮接纳；扰动命令可关闭候补放行
    waitlisted = {rid for rid, st in state.statuses.items()
                  if st == WAITLISTED}
    if not include_waitlist:
        waitlisted = set()

    tl = build_timelines(state, catalog, movable)
    candidates = movable
    if restrict is None:
        candidates |= waitlisted | set(fresh)
    else:
        # 限制轮：只放行集合内的候补/新申报（FIFO 前缀接纳）
        candidates |= (waitlisted & restrict)
        candidates |= (set(fresh) & restrict)
    order = sorted(candidates,
                   key=lambda rid: _order_key(state, rid, set(anchors),
                                              manual_rid))

    decls = dict(state.declarations)
    decls.update(fresh)

    outcome = ReplanOutcome(events=[], plans={}, displaced=[],
                            waitlisted_new=[], rejected_new=[],
                            violations_by_rid={}, unplaced_anchors=[],
                            manual_plan=None)
    base_seq = state.version
    # 严格 FIFO 门控：本轮参与的候补按序号处理，第一个排不下的候补
    # 会挡住其后所有候补（组 3 内按序号升序）
    waitlist_gate_closed = False

    def emit(etype: str, rid: str, at: int, payload: dict) -> Event:
        event = Event(seq=base_seq + len(outcome.events) + 1, event_id="",
                      event_type=etype, aggregate_id=rid,
                      occurred_at=at, payload=payload)
        outcome.events.append(event)
        return event

    for rid in order:
        decl = decls[rid]
        old_plan = state.plans.get(rid)
        is_manual = rid == manual_rid

        # 严格 FIFO：被更早候补挡住时本轮不尝试，保持原序号
        if waitlist_gate_closed and state.statuses.get(rid) == WAITLISTED:
            continue

        if rid in anchors:
            arrival = anchors[rid]
            dur_d = catalog.timing.dock_duration(decl.zone)
            # 不得早于承运方窗口起点或当前时间；必须在暴露时限内靠台
            earliest = max(arrival, now, decl.window_start)
            latest = arrival + max(0, decl.max_exposure_minutes - dur_d)
        elif old_plan is not None and rid not in amended:
            # 已排定作业：优先保住原时刻；不允许越过承运方申报窗口
            earliest = max(now, old_plan.planned_arrival)
            latest = decl.window_end
        else:
            # 改期 / 候补接纳 / 新申报：从当前时间起按申报窗口搜索
            earliest = max(now, decl.window_start)
            latest = decl.window_end

        plan, violations = try_place(
            decl, catalog, tl, earliest, latest,
            inclusive=rid in anchors)

        if plan is None:
            outcome.violations_by_rid[rid] = violations
            if rid in anchors:
                outcome.unplaced_anchors.append(rid)
                if old_plan is not None:
                    # 撤下已失效的旧档；车辆在场，状态保持 arrived，
                    # 下一轮（巡检/设备恢复）自动作为锚点重试
                    emit(evt.RESERVATION_POSTPONED, rid, now, {
                        "since": now,
                        "present": True,
                        "reason": "arrival_no_capacity",
                        "violations": [v.to_payload(tz) for v in violations],
                        "old_plan": old_plan.to_payload(tz),
                    })
                key = f"exposure:{rid}"
                if not state.alert_active(key):
                    emit(evt.ALERT_RAISED, rid, now, {
                        "key": key, "code": "exposure_wait",
                        "at": now,
                        "message": ("车辆已到场，最长暴露时间内无可用月台/"
                                    "预冷间/库位"),
                        "violations": [v.to_payload(tz) for v in violations],
                    })
                continue
            if is_manual:
                # 人工插单也不得突破容量；服务层据此拒绝并回传真实冲突
                continue
            status = state.statuses.get(rid)
            if status == WAITLISTED:
                # 仍候补，原序号保持不变；严格 FIFO 挡住其后候补
                waitlist_gate_closed = True
                continue
            if rid in fresh:
                codes = {v.code for v in violations}
                if codes & HARD_VIOLATIONS:
                    # 窗口关闭 / 温区不支持 / 暴露时间物理不可行：真正拒绝
                    outcome.rejected_new.append(rid)
                    emit(evt.RESERVATION_REJECTED, rid, now, {
                        "reason": "hard_constraint",
                        "violations": [v.to_payload(tz) for v in violations],
                    })
                else:
                    # 仅容量不足：进入候补，容量释放后按序接纳
                    outcome.waitlisted_new.append(rid)
                    emit(evt.RESERVATION_WAITLISTED, rid, now, {
                        "since": now,
                        "reason": "no_capacity_in_window",
                        "violations": [v.to_payload(tz) for v in violations],
                    })
                continue
            # 已排定但本轮被挤掉：延后回候补
            outcome.displaced.append(rid)
            emit(evt.RESERVATION_POSTPONED, rid, now, {
                "since": now,
                "reason": "replan_no_capacity",
                "violations": [v.to_payload(tz) for v in violations],
                "old_plan": old_plan.to_payload(tz) if old_plan else None,
            })
            continue

        commit_plan(decl, plan, tl)
        outcome.plans[rid] = plan

        if is_manual:
            outcome.manual_plan = plan
            continue  # manual_inserted 事件由服务层统一发出（含审计）

        if rid in anchors:
            # 车辆提前/迟到：到场事实事件由服务层发出，这里只发方案变化
            if old_plan != plan:
                emit(evt.RESERVATION_RESCHEDULED, rid, now, {
                    "plan": plan.to_payload(tz),
                    "old_plan": old_plan.to_payload(tz) if old_plan else None,
                    "reason": ("actual_arrival_"
                               + ("early" if arrival < decl.window_start
                                  else "late" if arrival > decl.window_end
                                  else "ontime")),
                    "arrived_at": arrival,
                    "at": now,
                })
            key = f"exposure:{rid}"
            if state.alert_active(key):
                emit(evt.ALERT_RESOLVED, rid, now, {"key": key, "at": now})
            continue

        if state.statuses.get(rid) == WAITLISTED:
            pos = state.waitlist_position(rid)
            emit(evt.WAITLIST_ACCEPTED, rid, now, {
                "plan": plan.to_payload(tz),
                "waitlist_position": pos,
                "accepted_at": now,
            })
            key = f"waitlist_timeout:{rid}"
            if state.alert_active(key):
                emit(evt.ALERT_RESOLVED, rid, now, {"key": key, "at": now})
            continue

        if old_plan is None:
            emit(evt.RESERVATION_SCHEDULED, rid, now,
                 {"plan": plan.to_payload(tz), "at": now})
        elif old_plan != plan:
            emit(evt.RESERVATION_RESCHEDULED, rid, now, {
                "plan": plan.to_payload(tz),
                "old_plan": old_plan.to_payload(tz),
                "reason": "replan",
                "at": now,
            })

    return outcome
