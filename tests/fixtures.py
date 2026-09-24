"""测试夹具：标准资源目录与时间工具。"""
from __future__ import annotations

from coldchain.catalog import Catalog, TimingPolicy
from coldchain.models import Declaration
from coldchain.service import SchedulingService
from coldchain.store import EventStore
from coldchain.timepolicy import market_zone, to_minute

TZ = market_zone("Asia/Shanghai")


def m(value: str) -> int:
    return to_minute(value, TZ)


def build_catalog() -> Catalog:
    """两门位 + 每温区一间预冷间/低温库的标准目录。

    frozen 预冷间容量 20 托，低温库 100 托；
    chilled 预冷间容量 12 托，低温库 60 托。
    月台作业 30 分钟，预冷 60 分钟。
    """
    catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
    catalog.add_dock("dock-1", "1 号月台")
    catalog.add_dock("dock-2", "2 号月台")
    catalog.add_precool_room("pc-f", "frozen", 20, "冷冻预冷间")
    catalog.add_precool_room("pc-c", "chilled", 12, "冷藏预冷间")
    catalog.add_storage_room("st-f", "frozen", 100, "冷冻库")
    catalog.add_storage_room("st-c", "chilled", 60, "冷藏库")
    return catalog


def build_service(store: EventStore | None = None,
                  catalog: Catalog | None = None) -> SchedulingService:
    return SchedulingService(store or EventStore(":memory:"),
                             catalog or build_catalog())


def declaration(rid: str, zone: str = "frozen", pallets: int = 10,
                window: tuple[str, str] = ("2026-09-23T08:00",
                                           "2026-09-23T12:00"),
                exposure: int = 120, carrier: str = "承运A",
                vehicle: str | None = None) -> Declaration:
    return Declaration(
        reservation_id=rid, carrier=carrier,
        vehicle_id=vehicle or f"veh-{rid}", zone=zone, pallets=pallets,
        window_start=m(window[0]), window_end=m(window[1]),
        max_exposure_minutes=exposure)
