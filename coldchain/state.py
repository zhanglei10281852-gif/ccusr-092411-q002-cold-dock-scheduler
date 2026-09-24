"""调度状态：由事件日志确定性折叠得到。

重启后重放全部事件即可还原：候补序号（``waitlist`` 列表顺序）、
超时告警（``alerts``）、设备停用区间、审计记录全部来自事件，无额外状态。
"""

from __future__ import annotations

from typing import Any

from . import events as evt
from .events import Event
from .models import Declaration, Plan, Violation

# 终态 / 在场状态
COMPLETED = "completed"
CANCELLED = "cancelled"
REJECTED = "rejected"
REQUESTED = "requested"
WAITLISTED = "waitlisted"
SCHEDULED = "scheduled"
ARRIVED = "arrived"
PROCESSING = "processing"

# 已进场且已有定位的车辆保持原位；重排只动未到场的作业
ENTERED_STATUSES = frozenset({ARRIVED, PROCESSING, COMPLETED})
# 作业已开始，资源占用不可移动
FROZEN_STATUSES = frozenset({PROCESSING, COMPLETED})
ACTIVE_STATUSES = frozenset(
    {SCHEDULED, ARRIVED, PROCESSING, WAITLISTED})
# 需要参与排程/候补接纳的未决状态
PENDING_STATUSES = frozenset({REQUESTED, WAITLISTED, SCHEDULED})


class State:
    def __init__(self) -> None:
        self.version = 0
        self.declarations: dict[str, Declaration] = {}
        self.statuses: dict[str, str] = {}
        self.plans: dict[str, Plan] = {}
        self.waitlist: list[str] = []
        self.waitlist_since: dict[str, int] = {}
        self.requested_at: dict[str, int] = {}
        self.arrived_at: dict[str, int] = {}
        self.started_at: dict[str, int] = {}
        self.closed_at: dict[str, int] = {}
        self.storage_released: set[str] = set()
        # resource_id -> 停用区间列表（半开；None 终点表示启用事件闭合）
        self.downtime: dict[str, list[list[int | None]]] = {}
        self.alerts: dict[str, dict[str, Any]] = {}
        self.manual_audit: list[dict[str, Any]] = []
        self.idempotency: dict[str, dict[str, Any]] = {}
        self.plans_history: dict[str, list[Plan]] = {}

    # ---- 派生查询 ----
    def is_frozen(self, rid: str) -> bool:
        """已经进场的车辆保持原位。"""
        return self.statuses.get(rid) in ENTERED_STATUSES

    def is_active(self, rid: str) -> bool:
        return self.statuses.get(rid) in ACTIVE_STATUSES

    def waitlist_position(self, rid: str) -> int | None:
        """候补序号从 1 开始；不在候补返回 None。"""
        try:
            return self.waitlist.index(rid) + 1
        except ValueError:
            return None

    def frozen_plans(self) -> dict[str, Plan]:
        return {rid: p for rid, p in self.plans.items() if self.is_frozen(rid)}

    def open_storage_rids(self) -> set[str]:
        return {rid for rid in self.plans
                if self.statuses.get(rid) in ACTIVE_STATUSES
                and rid not in self.storage_released}

    def alert_active(self, key: str) -> bool:
        record = self.alerts.get(key)
        return bool(record and record.get("active"))
def fold(state: State, event: Event) -> State:
    """把单条事件应用到状态（纯函数式更新）。"""
    state.version = event.seq
    p = event.payload
    rid = event.aggregate_id
    et = event.event_type

    if et == evt.RESERVATION_REQUESTED:
        state.declarations[rid] = Declaration.from_payload(p)
        state.statuses[rid] = "requested"
        state.requested_at[rid] = event.occurred_at
        if p.get("idempotency_key"):
            state.idempotency[p["idempotency_key"]] = {"event_id": event.event_id}

    elif et == evt.RESERVATION_SCHEDULED:
        state.plans[rid] = Plan.from_payload(p["plan"])
        state.plans_history.setdefault(rid, []).append(state.plans[rid])
        state.statuses[rid] = SCHEDULED

    elif et == evt.RESERVATION_WAITLISTED:
        state.statuses[rid] = WAITLISTED
        # 候补意味着当前没有生效方案（历史方案仍保留在 plans_history）
        state.plans.pop(rid, None)
        if rid not in state.waitlist:
            state.waitlist.append(rid)
            state.waitlist_since[rid] = p.get("since", event.occurred_at)

    elif et == evt.RESERVATION_REJECTED:
        state.statuses[rid] = REJECTED
        state.closed_at[rid] = event.occurred_at
        state.plans.pop(rid, None)

    elif et == evt.RESERVATION_RESCHEDULED:
        state.plans[rid] = Plan.from_payload(p["plan"])
        state.plans_history.setdefault(rid, []).append(state.plans[rid])
        # 已到场车辆围绕实际到场时间改排后仍是 arrived（方案冻结，不可再移动）
        if state.statuses.get(rid) != ARRIVED:
            state.statuses[rid] = SCHEDULED
        if rid in state.waitlist:
            state.waitlist.remove(rid)
        state.waitlist_since.pop(rid, None)

    elif et == evt.RESERVATION_POSTPONED:
        if p.get("present"):
            # 车辆已实际到场但暴露时限内排不进：撤下失效旧档，保持 arrived，
            # 不进候补队列（在场车辆优先于一切候补，下轮自动重试）
            state.plans.pop(rid, None)
            if rid in state.waitlist:
                state.waitlist.remove(rid)
            state.waitlist_since.pop(rid, None)
        else:
            state.statuses[rid] = WAITLISTED
            state.plans.pop(rid, None)
            if rid not in state.waitlist:
                state.waitlist.append(rid)
                state.waitlist_since[rid] = p.get("since", event.occurred_at)

    elif et == evt.RESERVATION_AMENDED:
        state.declarations[rid] = Declaration.from_payload(p["declaration"])

    elif et == evt.RESERVATION_MANUAL_INSERTED:
        state.declarations[rid] = Declaration.from_payload(p["declaration"])
        state.plans[rid] = Plan.from_payload(p["plan"])
        state.plans_history.setdefault(rid, []).append(state.plans[rid])
        state.statuses[rid] = SCHEDULED
        state.requested_at.setdefault(rid, event.occurred_at)
        state.manual_audit.append({
            "reservation_id": rid,
            "approver": p["approver"],
            "reason": p.get("reason", ""),
            "postponed_batches": list(p.get("postponed_batches", ())),
            "at": event.occurred_at,
            "event_id": event.event_id,
        })

    elif et == evt.WAITLIST_ACCEPTED:
        state.plans[rid] = Plan.from_payload(p["plan"])
        state.plans_history.setdefault(rid, []).append(state.plans[rid])
        state.statuses[rid] = SCHEDULED
        if rid in state.waitlist:
            state.waitlist.remove(rid)
        state.waitlist_since.pop(rid, None)

    elif et == evt.VEHICLE_ARRIVED:
        state.arrived_at[rid] = p["arrived_at"]
        state.statuses[rid] = ARRIVED
        if rid in state.waitlist:
            state.waitlist.remove(rid)
        state.waitlist_since.pop(rid, None)
        if "plan" in p:
            state.plans[rid] = Plan.from_payload(p["plan"])
            state.plans_history.setdefault(rid, []).append(state.plans[rid])

    elif et == evt.OPERATION_STARTED:
        state.started_at[rid] = p["started_at"]
        state.statuses[rid] = PROCESSING
        if rid in state.waitlist:
            state.waitlist.remove(rid)

    elif et == evt.RESERVATION_COMPLETED:
        state.statuses[rid] = COMPLETED
        state.closed_at[rid] = event.occurred_at
        if rid in state.waitlist:
            state.waitlist.remove(rid)

    elif et == evt.RESERVATION_CANCELLED:
        state.statuses[rid] = CANCELLED
        state.closed_at[rid] = event.occurred_at
        if rid in state.waitlist:
            state.waitlist.remove(rid)
        state.waitlist_since.pop(rid, None)

    elif et == evt.EQUIPMENT_DISABLED:
        state.downtime.setdefault(p["resource_id"], []).append(
            [p["start"], p.get("end")])

    elif et == evt.EQUIPMENT_ENABLED:
        at_minute = p["start"]
        intervals = state.downtime.get(p["resource_id"], [])
        # 闭合开放区间；或缩短覆盖恢复时刻的计划停用区间
        for interval in reversed(intervals):
            ds, de = interval
            if de is None or (ds <= at_minute and at_minute < de):
                interval[1] = at_minute
                break

    elif et == evt.GOODS_RELEASED:
        state.storage_released.add(rid)

    elif et == evt.ALERT_RAISED:
        state.alerts[p["key"]] = {
            "active": True,
            "code": p["code"],
            "reservation_id": rid,
            "at": p["at"],
            "message": p.get("message", ""),
            "event_id": event.event_id,
        }

    elif et == evt.ALERT_RESOLVED:
        record = state.alerts.get(p["key"])
        if record:
            record["active"] = False
            record["resolved_at"] = p["at"]

    return state


def replay(events: list[Event]) -> State:
    state = State()
    for event in events:
        fold(state, event)
    return state
