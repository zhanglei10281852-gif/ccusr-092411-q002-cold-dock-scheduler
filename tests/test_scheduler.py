"""排程引擎测试：迟到重排、设备停用、只动未开工车辆、冲突归因。"""
from __future__ import annotations

import unittest

from coldchain.models import DOCK, PRECOOL, STORAGE
from coldchain.scheduler import ForceSlot
from coldchain.state import State
from coldchain.events import make_event, RESERVATION_REQUESTED

from tests.support import ColdChainTestCase, DAY


def seed_requested(state: State, rid: str, *, pallets=10, zone="frozen",
                   ws=480, we=600, exposure=300, created="t", seq=1):
    event = make_event(
        RESERVATION_REQUESTED, rid, seq, created,
        {
            "carrier": "c", "vehicle_id": "v", "zone": zone,
            "pallets": pallets, "day": DAY,
            "window_start_minute": ws, "window_end_minute": we,
            "max_exposure_minutes": exposure,
            "timeout_after_minutes": None,
        },
    )
    state.apply(event)
    return state.get(rid)


class SchedulerTest(ColdChainTestCase):
    def request(self, rid, **kw):
        result = self.svc.request_reservation(
            rid,
            carrier="c", vehicle_id="v",
            zone=kw.pop("zone", "frozen"),
            pallets=kw.pop("pallets", 10),
            day=DAY,
            window_start_minute=kw.pop("ws", 480),
            window_end_minute=kw.pop("we", 600),
            max_exposure_minutes=kw.pop("exposure", 300),
            occurred_at=kw.pop("created", f"c-{rid}"),
            **kw,
        )
        return result

    def test_basic_chain_uses_dock_precool_storage_in_order(self) -> None:
        r = self.request("r1")
        ops = [(o.resource_type, o.resource_id) for o in r.plan.operations]
        self.assertEqual(
            ops,
            [(DOCK, "dock-1"), (PRECOOL, "pc-f"), (STORAGE, "st-f")],
        )
        # 默认时长 30/60/20
        starts = [o.start_minute for o in r.plan.operations]
        self.assertEqual(starts, [480, 510, 570])

    def test_late_arrival_is_moved_into_next_slot(self) -> None:
        self.request("a", ws=480, we=700)
        b = self.request("b", ws=480, we=500)
        first_dock = b.plan.operations[0].start_minute
        # b 迟到 40 分钟（08:40 到，窗口 08:00–08:20 已过）
        res = self.svc.arrive("b", "2026-09-25T08:40:00+08:00")
        self.assertEqual(res.status, "scheduled")
        self.assertGreaterEqual(
            res.plan.operations[0].start_minute, 520
        )
        self.assertNotEqual(
            res.plan.operations[0].start_minute, first_dock
        )

    def test_started_vehicle_never_moves(self) -> None:
        self.request("a", ws=480, we=700)
        self.svc.arrive("a", "2026-09-25T08:00:00+08:00")
        self.svc.start_processing("a", "2026-09-25T08:00:00+08:00")
        locked = self.svc.load_state().get("a").plan.operations[0]
        # 停用 a 正在使用的 dock-1 之后时段，a 仍保持原位
        self.svc.disable_equipment(
            DAY, DOCK, "dock-1", 480, 510,
            reason="故障", occurred_at="2026-09-25T07:00+08:00",
        )
        again = self.svc.load_state().get("a").plan.operations[0]
        self.assertEqual(
            (again.resource_id, again.start_minute),
            (locked.resource_id, locked.start_minute),
        )
        self.assertTrue(again.locked)

    def test_equipment_displacement_only_moves_not_started(self) -> None:
        self.request("a", ws=480, we=700)
        self.request("b", ws=480, we=700)
        self.request("c", ws=480, we=700)
        # 三个预约分别占 dock-1/2/3
        docks = {
            rid: self.svc.load_state().get(rid).plan.operations[0].resource_id
            for rid in "abc"
        }
        self.assertEqual(set(docks.values()), {"dock-1", "dock-2", "dock-3"})
        # a 开工锁定
        self.svc.arrive("a", "2026-09-25T08:00+08:00")
        self.svc.start_processing("a", "2026-09-25T08:00+08:00")
        # 停用 dock-2、dock-3 整天 -> b/c 无法靠台，但 a 不动
        self.svc.disable_equipment(
            DAY, DOCK, "dock-2", 0, None,
            reason="检修", occurred_at="2026-09-25T07:00+08:00",
        )
        self.svc.disable_equipment(
            DAY, DOCK, "dock-3", 0, None,
            reason="检修", occurred_at="2026-09-25T07:00+08:00",
        )
        state = self.svc.load_state()
        self.assertEqual(state.get("a").plan.operations[0].resource_id, "dock-1")
        self.assertTrue(
            state.get("b").is_waitlisted or state.get("b").plan is None
            or state.get("b").plan.operations[0].resource_id != "dock-2"
        )

    def test_reschedule_failure_returns_real_conflicts(self) -> None:
        # 占满 dock-1 的 480 起整段
        self.request("big", pallets=20, ws=480, we=760, exposure=600)
        self.request("big2", pallets=20, ws=480, we=760, exposure=600)
        self.request("x", pallets=20, ws=540, we=560, exposure=600, created="c-x")
        with self.assertRaises(Exception) as ctx:
            self.svc.reschedule("x", window_start_minute=480,
                                window_end_minute=490, max_exposure_minutes=5)
        conflicts = ctx.exception.conflicts
        self.assertTrue(conflicts)
        kinds = {c.constraint for c in conflicts}
        self.assertIn("exposure", kinds)
        # 容量冲突必须带真正的占用批次
        cap = [c for c in conflicts if c.constraint == "capacity"]
        if cap:
            self.assertTrue(cap[0].blockers)
            self.assertIn(cap[0].blockers[0].reservation_id, {"big", "big2"})

    def test_cross_midnight_schedule(self) -> None:
        r = self.request("night", ws=1400, we=1430, exposure=400)
        self.assertEqual(r.status, "scheduled")
        last = r.plan.operations[-1]
        self.assertGreater(last.start_minute, 1440)
        self.assertTrue(last.end.startswith("2026-09-26"))

    def test_exposure_limit_blocks_too_long_wait(self) -> None:
        # 两个月台都被长作业占住，新车暴露上限极短，必然排不下
        self.request("h1", ws=480, we=800, exposure=800)
        self.request("h2", ws=480, we=800, exposure=800)
        tight = self.svc.request_reservation(
            "tight", carrier="c", vehicle_id="v", zone="frozen",
            pallets=10, day=DAY, window_start_minute=480,
            window_end_minute=500, max_exposure_minutes=5,
            occurred_at="c-tight",
        )
        self.assertEqual(tight.status, "waitlisted")
        kinds = {c.constraint for c in tight.conflicts}
        self.assertIn("exposure", kinds)


if __name__ == "__main__":
    unittest.main()
