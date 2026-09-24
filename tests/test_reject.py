"""拒绝与改期：必须返回真正冲突的约束，改期后可重新排程。"""
from __future__ import annotations

import unittest

from coldchain.service import ScheduleConflict

from tests.support import DAY, make_service, small_resources


class RejectRescheduleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service(resources=small_resources())
        for rid in ("occ-1", "occ-2"):
            self.svc.request_reservation(
                rid, carrier="c", vehicle_id="v", zone="frozen", pallets=10,
                day=DAY, window_start_minute=480, window_end_minute=800,
                max_exposure_minutes=800, occurred_at=f"t-{rid}",
            )

    def test_rejection_carries_real_constraints_and_blockers(self) -> None:
        with self.assertRaises(ScheduleConflict) as ctx:
            self.svc.request_reservation(
                "rj", carrier="c", vehicle_id="v", zone="frozen", pallets=20,
                day=DAY, window_start_minute=480, window_end_minute=490,
                max_exposure_minutes=10, reject_if_unavailable=True,
                occurred_at="t-rj",
            )
        conflicts = ctx.exception.conflicts
        kinds = {c.constraint for c in conflicts}
        self.assertIn("capacity", kinds)
        self.assertIn("exposure", kinds)
        cap = next(c for c in conflicts if c.constraint == "capacity")
        self.assertGreater(cap.required, cap.limit)
        blockers = {b.reservation_id for c in conflicts for b in c.blockers}
        self.assertTrue(blockers & {"occ-1", "occ-2"})
        # 拒绝留痕且不参与排程
        rec = self.svc.load_state().get("rj")
        self.assertEqual(rec.state, "rejected")
        self.assertIsNone(rec.plan)
        self.assertIsNotNone(rec.rejection)

    def test_rejected_reservation_can_be_rescheduled_later(self) -> None:
        with self.assertRaises(ScheduleConflict):
            self.svc.request_reservation(
                "rj", carrier="c", vehicle_id="v", zone="frozen", pallets=20,
                day=DAY, window_start_minute=480, window_end_minute=490,
                max_exposure_minutes=10, reject_if_unavailable=True,
                occurred_at="t-rj",
            )
        result = self.svc.reschedule(
            "rj", window_start_minute=760, window_end_minute=820,
            max_exposure_minutes=300, occurred_at="t-later",
        )
        self.assertEqual(result.status, "scheduled")
        rec = self.svc.load_state().get("rj")
        self.assertIsNone(rec.rejection)
        self.assertEqual(rec.state, "scheduled")

    def test_reschedule_failure_returns_conflicts_without_mutating(self) -> None:
        # occ-1 已排程；把它改期到容量/暴露均不可能的窗口必须失败，
        # 且不改变原计划。
        before = self.svc.load_state().get("occ-1").plan.to_dict()
        with self.assertRaises(ScheduleConflict):
            self.svc.reschedule(
                "occ-1", window_start_minute=480,
                window_end_minute=490, max_exposure_minutes=5,
                occurred_at="t-move",
            )
        after = self.svc.load_state().get("occ-1").plan.to_dict()
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
