"""分钟级容量台账。

每个资源在营业日坐标上维护一条长度为 ``horizon``（默认 48 小时）的
整数占用曲线。月台按车辆数计容（每段作业计 1），预冷间与低温库按
托盘数计容。设备停用以停用区间表达，停用分钟容量视为 0，并返回
``disabled`` 类型冲突，与普通容量超量区分。
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import DOCK, BlockingLoad, Conflict, Operation

DEFAULT_HORIZON = 2 * 1440

# (reservation_id, operation)
Load = tuple[str, Operation]


@dataclass(frozen=True)
class Resource:
    resource_type: str
    resource_id: str
    zone: str                 # 温区；"*" 表示任意温区可用（如月台）
    capacity: int             # 每分钟上限：月台=车位数，其余=托盘数

    @property
    def unit(self) -> str:
        return "vehicles" if self.resource_type == DOCK else "pallets"


class Ledger:
    def __init__(self, resources: list[Resource], horizon: int = DEFAULT_HORIZON) -> None:
        self.horizon = horizon
        self.resources: dict[tuple[str, str], Resource] = {}
        self._usage: dict[tuple[str, str], list[Load]] = {}
        for res in resources:
            key = (res.resource_type, res.resource_id)
            if key in self.resources:
                raise ValueError(f"资源重复定义：{key}")
            self.resources[key] = res
            self._usage[key] = []
        # 停用区间（半开，分钟）
        self.disabled: dict[tuple[str, str], list[tuple[int, int]]] = {
            key: [] for key in self.resources
        }

    # -- 资源查询 -------------------------------------------------------

    def compatible(self, resource_type: str, zone: str) -> list[Resource]:
        out = [
            res
            for (rtype, _), res in self.resources.items()
            if rtype == resource_type and (res.zone == zone or res.zone == "*")
        ]
        return sorted(out, key=lambda r: r.resource_id)

    def disable(self, resource_type: str, resource_id: str,
                start: int, end: int | None) -> None:
        key = (resource_type, resource_id)
        if key not in self.resources:
            raise KeyError(f"未知资源：{key}")
        end = self.horizon if end is None else min(end, self.horizon)
        start = max(0, start)
        if end <= start:
            return
        self.disabled[key].append((start, end))
        self.disabled[key].sort()

    def is_disabled(self, key: tuple[str, str], minute: int) -> bool:
        return any(s <= minute < e for s, e in self.disabled.get(key, ()))

    # -- 占用登记 -------------------------------------------------------

    @staticmethod
    def weight(resource_type: str, op: Operation) -> int:
        return 1 if resource_type == DOCK else op.pallets

    def add(self, reservation_id: str, op: Operation) -> None:
        key = (op.resource_type, op.resource_id)
        if key not in self.resources:
            raise KeyError(f"未知资源：{key}")
        self._usage[key].append((reservation_id, op))

    def active(self, resource_type: str, resource_id: str, minute: int) -> list[Load]:
        return [
            (rid, op)
            for rid, op in self._usage.get((resource_type, resource_id), ())
            if op.start_minute <= minute < op.end_minute
        ]

    def usage_at(self, resource_type: str, resource_id: str, minute: int) -> int:
        return sum(
            self.weight(resource_type, op)
            for _, op in self.active(resource_type, resource_id, minute)
        )

    # -- 可行性 ---------------------------------------------------------

    def check_interval(
        self,
        resource_type: str,
        resource_id: str,
        start: int,
        end: int,
        pallets: int,
    ) -> Conflict | None:
        """返回区间内最严重的一个冲突；无冲突返回 None。"""
        key = (resource_type, resource_id)
        res = self.resources.get(key)
        if res is None:
            return Conflict(
                resource_type=resource_type, resource_id=resource_id,
                constraint="zone", minute=start, limit=0, required=pallets,
                available=0, message=f"资源不存在：{resource_type}/{resource_id}",
            )
        weight = 1 if resource_type == DOCK else pallets
        if start < 0 or end > self.horizon or end <= start:
            return Conflict(
                resource_type=resource_type, resource_id=resource_id,
                constraint="window", minute=max(start, 0), limit=self.horizon,
                required=end, available=self.horizon,
                message="作业超出跨午夜排程边界（48 小时）",
            )
        worst: Conflict | None = None
        for minute in range(start, end):
            if self.is_disabled(key, minute):
                return Conflict(
                    resource_type=resource_type, resource_id=resource_id,
                    constraint="disabled", minute=minute, limit=0,
                    required=weight, available=0,
                    message=f"设备停用：{resource_type}/{resource_id} 第 {minute} 分钟不可用",
                )
            loads = self.active(resource_type, resource_id, minute)
            used = sum(self.weight(resource_type, op) for _, op in loads)
            available = res.capacity - used
            if weight > available:
                blockers = tuple(
                    BlockingLoad(
                        reservation_id=rid,
                        start_minute=op.start_minute,
                        end_minute=op.end_minute,
                        pallets=self.weight(resource_type, op),
                        locked=op.locked,
                    )
                    for rid, op in sorted(loads, key=lambda item: (item[1].start_minute, item[0]))
                )
                candidate = Conflict(
                    resource_type=resource_type, resource_id=resource_id,
                    constraint="capacity", minute=minute, limit=res.capacity,
                    required=weight + used, available=max(available, 0),
                    blockers=blockers,
                    message=(
                        f"{resource_type}/{resource_id} 第 {minute} 分钟超量："
                        f"需要 {weight + used}，上限 {res.capacity}"
                    ),
                )
                if worst is None or candidate.available < worst.available:
                    worst = candidate
        return worst

    def first_capacity_conflict(
        self,
        resource_type: str,
        zone: str,
        start: int,
        end: int,
        pallets: int,
    ) -> Conflict | None:
        """跨所有温区兼容资源，返回最先出现的容量/停用冲突。"""
        conflicts = []
        for res in self.compatible(resource_type, zone):
            found = self.check_interval(res.resource_type, res.resource_id, start, end, pallets)
            if found is not None:
                conflicts.append(found)
        if not conflicts:
            return None
        return sorted(conflicts, key=lambda c: (c.minute, c.constraint != "disabled"))[0]
