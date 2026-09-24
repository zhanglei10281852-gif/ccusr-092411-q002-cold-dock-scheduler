"""资源目录与作业时长策略。

三类资源：

- ``dock`` 月台门位：离散资源，同一分钟只能有一辆车占用；
- ``precool`` 预冷间：按托盘计分钟级容量，货物在间内停留固定时长；
- ``storage`` 低温库：按温区划分的库位，托盘入库后持续占用，直到出库。
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Dock:
    dock_id: str
    label: str = ""


@dataclass(frozen=True, slots=True)
class PrecoolRoom:
    room_id: str
    zone: str
    pallet_capacity: int
    label: str = ""


@dataclass(frozen=True, slots=True)
class StorageRoom:
    room_id: str
    zone: str
    pallet_capacity: int
    label: str = ""


@dataclass(frozen=True, slots=True)
class TimingPolicy:
    """各温区作业时长（分钟）；未配置温区使用默认值。"""

    dock_minutes: int = 30
    precool_minutes: int = 60
    dock_by_zone: dict[str, int] = field(default_factory=dict)
    precool_by_zone: dict[str, int] = field(default_factory=dict)

    def dock_duration(self, zone: str) -> int:
        return int(self.dock_by_zone.get(zone, self.dock_minutes))

    def precool_duration(self, zone: int | str) -> int:
        return int(self.precool_by_zone.get(zone, self.precool_minutes))


class Catalog:
    """调度用的只读资源目录。"""

    def __init__(self, timing: TimingPolicy | None = None) -> None:
        self.timing = timing or TimingPolicy()
        self._docks: dict[str, Dock] = {}
        self._precool: dict[str, PrecoolRoom] = {}
        self._storage: dict[str, StorageRoom] = {}

    # ---- 目录装配 ----
    def add_dock(self, dock_id: str, label: str = "") -> "Catalog":
        if dock_id in self._docks:
            raise ValueError(f"月台已存在：{dock_id}")
        self._docks[dock_id] = Dock(dock_id, label)
        return self

    def add_precool_room(self, room_id: str, zone: str, pallet_capacity: int,
                         label: str = "") -> "Catalog":
        if room_id in self._precool:
            raise ValueError(f"预冷间已存在：{room_id}")
        if pallet_capacity <= 0:
            raise ValueError("预冷间容量必须为正")
        self._precool[room_id] = PrecoolRoom(room_id, zone, pallet_capacity, label)
        return self

    def add_storage_room(self, room_id: str, zone: str, pallet_capacity: int,
                         label: str = "") -> "Catalog":
        if room_id in self._storage:
            raise ValueError(f"低温库已存在：{room_id}")
        if pallet_capacity <= 0:
            raise ValueError("低温库容量必须为正")
        self._storage[room_id] = StorageRoom(room_id, zone, pallet_capacity, label)
        return self

    # ---- 查询 ----
    def docks(self) -> list[Dock]:
        return [self._docks[k] for k in sorted(self._docks)]

    def precool_rooms(self, zone: str) -> list[PrecoolRoom]:
        return [r for r in self._precool.values() if r.zone == zone]

    def storage_room(self, zone: str) -> StorageRoom | None:
        rooms = [r for r in self._storage.values() if r.zone == zone]
        return sorted(rooms, key=lambda r: r.room_id)[0] if rooms else None

    def dock(self, dock_id: str) -> Dock:
        return self._docks[dock_id]

    def precool(self, room_id: str) -> PrecoolRoom:
        return self._precool[room_id]

    def storage(self, room_id: str) -> StorageRoom:
        return self._storage[room_id]

    def has_resource(self, kind: str, resource_id: str) -> bool:
        if kind == "dock":
            return resource_id in self._docks
        if kind == "precool":
            return resource_id in self._precool
        if kind == "storage":
            return resource_id in self._storage
        return False
