"""分钟级容量台账测试。"""
from __future__ import annotations

import unittest

from coldchain.capacity import Ledger, Resource
from coldchain.models import DOCK, PRECOOL, STORAGE, Operation


class LedgerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(
            [
                Resource(DOCK, "dock-1", "*", 1),
                Resource(PRECOOL, "pc-f", "frozen", 20),
            ]
        )

    def _op(self, rtype, rid, start, end, pallets=10):
        return Operation(rtype, rid, start, end, pallets)

    def test_dock_counts_vehicles_not_pallets(self) -> None:
        # 月台容量 1：一辆 20 托的车只算 1 个车位
        self.ledger.add("r1", self._op(DOCK, "dock-1", 0, 30, 20))
        self.assertIsNone(self.ledger.check_interval(DOCK, "dock-1", 30, 60, 20))
        conflict = self.ledger.check_interval(DOCK, "dock-1", 10, 40, 1)
        self.assertEqual(conflict.constraint, "capacity")
        self.assertEqual(conflict.blockers[0].reservation_id, "r1")

    def test_precool_counts_pallets(self) -> None:
        self.ledger.add("r1", self._op(PRECOOL, "pc-f", 0, 60, 12))
        # 再加 9 托超限（12+9>20）
        conflict = self.ledger.check_interval(PRECOOL, "pc-f", 0, 60, 9)
        self.assertEqual(conflict.constraint, "capacity")
        self.assertEqual(conflict.required, 21)
        self.assertEqual(conflict.available, 8)
        # 8 托恰好放得下
        self.assertIsNone(self.ledger.check_interval(PRECOOL, "pc-f", 0, 60, 8))

    def test_half_open_intervals_touch_but_do_not_overlap(self) -> None:
        # [0,30) 与 [30,60) 首尾相接不算重叠
        self.ledger.add("r1", self._op(DOCK, "dock-1", 0, 30))
        self.assertIsNone(self.ledger.check_interval(DOCK, "dock-1", 30, 60, 1))

    def test_disabled_minute_reports_disabled_conflict(self) -> None:
        self.ledger.disable(DOCK, "dock-1", 20, 40)
        conflict = self.ledger.check_interval(DOCK, "dock-1", 10, 30, 1)
        self.assertEqual(conflict.constraint, "disabled")
        self.assertEqual(conflict.minute, 20)
        # 停用区间外仍可用
        self.assertIsNone(self.ledger.check_interval(DOCK, "dock-1", 0, 20, 1))

    def test_zone_compatibility(self) -> None:
        ledger = Ledger(
            [
                Resource(PRECOOL, "pc-f", "frozen", 20),
                Resource(PRECOOL, "pc-c", "chilled", 20),
            ]
        )
        self.assertEqual(
            [r.resource_id for r in ledger.compatible(PRECOOL, "frozen")],
            ["pc-f"],
        )

    def test_unknown_resource(self) -> None:
        conflict = self.ledger.check_interval(STORAGE, "nope", 0, 10, 1)
        self.assertEqual(conflict.constraint, "zone")


if __name__ == "__main__":
    unittest.main()
