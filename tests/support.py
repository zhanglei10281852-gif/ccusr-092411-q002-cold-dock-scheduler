"""测试共享夹具。"""
from __future__ import annotations

import unittest

from coldchain.capacity import Resource
from coldchain.models import DOCK, PRECOOL, STORAGE
from coldchain.scheduler import StageProfile
from coldchain.store import EventStore
from coldchain.timeutil import TimeService
from coldchain.service import ColdChainService

DAY = "2026-09-25"


def standard_resources() -> list[Resource]:
    """三个月台、两间预冷间（冻/鲜各 20 托）、两间低温库。"""
    return [
        Resource(DOCK, "dock-1", "*", 1),
        Resource(DOCK, "dock-2", "*", 1),
        Resource(DOCK, "dock-3", "*", 1),
        Resource(PRECOOL, "pc-f", "frozen", 20),
        Resource(PRECOOL, "pc-c", "chilled", 20),
        Resource(STORAGE, "st-f", "frozen", 100),
        Resource(STORAGE, "st-c", "chilled", 100),
    ]


def small_resources() -> list[Resource]:
    """两个月台、一间预冷间（20 托）、一间低温库，便于制造真实候补。"""
    return [
        Resource(DOCK, "dock-1", "*", 1),
        Resource(DOCK, "dock-2", "*", 1),
        Resource(PRECOOL, "pc-f", "frozen", 20),
        Resource(STORAGE, "st-f", "frozen", 100),
    ]


def make_service(
    path: str = ":memory:",
    resources: list[Resource] | None = None,
) -> ColdChainService:
    return ColdChainService(
        EventStore(path),
        resources if resources is not None else standard_resources(),
        TimeService("Asia/Shanghai"),
        StageProfile(),
    )


class ColdChainTestCase(unittest.TestCase):
    DAY = DAY

    def setUp(self) -> None:
        self.svc = make_service()
