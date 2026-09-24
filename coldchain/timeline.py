"""分钟级容量时间线。

每条占用是半开区间 ``[start, end)``；离散资源（月台）容量为 1，
累积型资源（预冷间、低温库）容量为托盘数。设备停用期间容量视为 0。

冲突探测用扫描线在候选窗口内一次走完全部区段，返回 *首次* 超限的分钟、
当时负载与峰值负载，供冲突回传使用。
"""

from __future__ import annotations

from dataclasses import dataclass

INF = 10**9  # 开放式占用终点（已入库未出库）


@dataclass(frozen=True, slots=True)
class Usage:
    key: str       # 占用归属（预约号），便于整笔撤装
    start: int
    end: int
    amount: int


@dataclass(frozen=True, slots=True)
class Breach:
    first_minute: int
    load: int       # 首次超限时的负载（含本次申报）
    peak: int       # 候选窗口内的峰值负载
    equipment_down: bool = False


class Timeline:
    def __init__(self) -> None:
        self._usage: dict[str, list[Usage]] = {}
        self._downtime: dict[str, list[tuple[int, int]]] = {}

    # ---- 占用维护 ----
    def add(self, resource_id: str, start: int, end: int, amount: int,
            key: str) -> None:
        if end <= start:
            raise ValueError(f"占用区间非法：{key} {start}..{end}")
        self._usage.setdefault(resource_id, []).append(
            Usage(key, start, end, amount))

    def remove_key(self, resource_id: str, key: str) -> None:
        if resource_id in self._usage:
            self._usage[resource_id] = [
                u for u in self._usage[resource_id] if u.key != key]

    # ---- 设备停用 ----
    def disable(self, resource_id: str, start: int, end: int) -> None:
        self._downtime.setdefault(resource_id, []).append((start, end))

    def is_available(self, resource_id: str, start: int, end: int) -> int | None:
        """返回区间内首个停用分钟；完全可用返回 None。"""
        for ds, de in sorted(self._downtime.get(resource_id, ())):
            if ds < end and start < de:
                return max(ds, start)
        return None

    # ---- 查询 ----
    def load_at(self, resource_id: str, minute: int) -> int:
        return sum(u.amount for u in self._usage.get(resource_id, ())
                   if u.start <= minute < u.end)

    def evaluate(self, resource_id: str, start: int, end: int, amount: int,
                 capacity: int) -> Breach | None:
        """检查叠加 ``amount`` 后 [start, end) 是否突破 ``capacity``。

        容量超限或设备停用均返回 Breach；完全可行返回 None。
        """
        down_at = self.is_available(resource_id, start, end)
        if down_at is not None:
            return Breach(down_at, capacity, capacity, equipment_down=True)

        # 与候选窗口相交的占用，折算成窗口内的增量事件。
        events: list[tuple[int, int]] = []
        running = 0
        for u in self._usage.get(resource_id, ()):
            if u.end <= start or u.start >= end:
                continue
            # 半开区间：起点恰为窗口起点时，该分钟已计入占用
            if u.start <= start:
                running += u.amount
            else:
                events.append((u.start, u.amount))
            if start < u.end < end:
                events.append((u.end, -u.amount))
        events.sort(key=lambda e: e[0])

        first_breach: int | None = None
        breach_load = 0
        peak = running
        cursor = start
        load = running
        for point, delta in events:
            if load + amount > capacity and first_breach is None:
                first_breach = cursor
                breach_load = load + amount
            peak = max(peak, load + amount)
            load += delta
            cursor = point
        if first_breach is None and load + amount > capacity:
            first_breach = cursor
            breach_load = load + amount
        peak = max(peak, load + amount)
        if first_breach is not None:
            return Breach(first_breach, breach_load, peak)
        return None
