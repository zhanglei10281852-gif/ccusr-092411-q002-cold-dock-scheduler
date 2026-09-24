"""应用服务测试：候补、插单、告警、并发裁决。"""
from __future__ import annotations

import os
import tempfile
import unittest

from coldchain.models import DOCK
from coldchain.scheduler import ForceSlot
from coldchain.service import ScheduleConflict
from coldchain.store import EventStore, StaleVersion
from coldchain.timeutil import TimeService

from tests.support import DAY, make_service, small_resources, standard_resources


def saturate(svc) -> None:
    """用两辆车占满两个月台 + 预冷间 20 托的早段（小资源集）。"""
    svc.request_reservation(
        "occ-1", carrier="c", vehicle_id="v1", zone="frozen", pallets=10,
        day=DAY, window_start_minute=480, window_end_minute=800,
        max_exposure_minutes=800, occurred_at="t0",
    )
    svc.request_reservation(
        "occ-2", carrier="c", vehicle_id="v2", zone="frozen", pallets=10,
        day=DAY, window_start_minute=480, window_end_minute=800,
        max_exposure_minutes=800, occurred_at="t1",
    )


class WaitlistTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service(resources=small_resources())

    def test_third_vehicle_waits_with_monotonic_sequence(self) -> None:
        saturate(self.svc)
        w1 = self.svc.request_reservation(
            "w1", carrier="c", vehicle_id="w", zone="frozen", pallets=20,
            day=DAY, window_start_minute=480, window_end_minute=500,
            max_exposure_minutes=60, occurred_at="t2",
        )
        w2 = self.svc.request_reservation(
            "w2", carrier="c", vehicle_id="w", zone="frozen", pallets=20,
            day=DAY, window_start_minute=480, window_end_minute=500,
            max_exposure_minutes=60, occurred_at="t3",
        )
        self.assertEqual((w1.status, w1.waitlist_seq), ("waitlisted", 1))
        self.assertEqual((w2.status, w2.waitlist_seq), ("waitlisted", 2))
        # 队列按序号返回
        queue = [r.reservation_id for r in self.svc.load_state().waitlist(DAY)]
        self.assertEqual(queue, ["w1", "w2"])

    def test_sequence_survives_restart(self) -> None:
        path = tempfile.mktemp(suffix=".db")
        try:
            svc = make_service(path)
            saturate(svc)
            svc.request_reservation(
                "w1", carrier="c", vehicle_id="w", zone="frozen", pallets=20,
                day=DAY, window_start_minute=480, window_end_minute=500,
                max_exposure_minutes=60, occurred_at="t2",
            )
            revived = make_service(path)
            self.assertEqual(revived.load_state().get("w1").waitlist_seq, 1)
            # 新候补继续使用递增序号，不会与旧序号冲突
            w2 = revived.request_reservation(
                "w2", carrier="c", vehicle_id="w", zone="frozen", pallets=20,
                day=DAY, window_start_minute=480, window_end_minute=500,
                max_exposure_minutes=60, occurred_at="t3",
            )
            self.assertEqual(w2.waitlist_seq, 2)
        finally:
            os.unlink(path)

    def test_accept_waitlist_fills_freed_capacity_in_sequence_order(self) -> None:
        # 单月台 + 10 托预冷间：串行链条占满后，第三辆只能候补
        from coldchain.capacity import Resource
        from coldchain.models import DOCK, PRECOOL, STORAGE

        tight = [
            Resource(DOCK, "dock-1", "*", 1),
            Resource(PRECOOL, "pc-f", "frozen", 10),
            Resource(STORAGE, "st-f", "frozen", 100),
        ]
        svc = make_service(resources=tight)
        # occ-1 占唯一月台 480-510
        svc.request_reservation(
            "occ-1", carrier="c", vehicle_id="v1", zone="frozen", pallets=10,
            day=DAY, window_start_minute=480, window_end_minute=520,
            max_exposure_minutes=800, occurred_at="t0",
        )
        # w1 窗口截至 09:00、暴露上限 30：月台被 occ-1 占到 510，
        # w1 即便 510 靠台，预冷要等 occ-1 570 结束，暴露 60 > 30 → 候补
        svc.request_reservation(
            "w1", carrier="c", vehicle_id="w", zone="frozen", pallets=10,
            day=DAY, window_start_minute=480, window_end_minute=540,
            max_exposure_minutes=30, occurred_at="t2",
        )
        # w2 窗口 08:00-08:30，窗口内月台被占 → 候补
        svc.request_reservation(
            "w2", carrier="c", vehicle_id="w", zone="frozen", pallets=10,
            day=DAY, window_start_minute=480, window_end_minute=510,
            max_exposure_minutes=120, occurred_at="t3",
        )
        self.svc = svc
        self.assertEqual(
            [r.reservation_id for r in svc.load_state().waitlist(DAY)],
            ["w1", "w2"],
        )
        # occ-1 取消（尚未开始），释放唯一月台与预冷占位；
        # w1 窗口仍开放、按候补序号首先提升，w2 窗口已过继续候补。
        result = svc.cancel("occ-1", reason="承运方撤单")
        self.assertIn("w1", result.promoted)
        self.assertNotIn("w2", result.promoted)
        state = svc.load_state()
        self.assertIsNone(state.get("w1").waitlist_seq)
        self.assertEqual(state.get("w2").waitlist_seq, 2)

    def test_completion_releases_capacity_and_replans(self) -> None:
        # 完工（已开工车辆结束）与取消走同一条释放+重排路径：
        # 完工后审计不应有任何越界。
        saturate(self.svc)
        self.svc.request_reservation(
            "late", carrier="c", vehicle_id="w", zone="frozen", pallets=10,
            day=DAY, window_start_minute=600, window_end_minute=700,
            max_exposure_minutes=300, occurred_at="t9",
        )
        self.svc.arrive("occ-1", "2026-09-25T08:00:00+08:00")
        self.svc.start_processing("occ-1", "2026-09-25T08:00:00+08:00")
        self.svc.complete("occ-1", "2026-09-25T09:00:00+08:00")
        audit = self.svc.audit_day(DAY)
        self.assertTrue(audit["within_limits"])


class ManualInsertTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service(resources=small_resources())
        saturate(self.svc)
        self.svc.request_reservation(
            "w1", carrier="c", vehicle_id="w", zone="frozen", pallets=20,
            day=DAY, window_start_minute=480, window_end_minute=500,
            max_exposure_minutes=800, occurred_at="t2",
        )

    def test_approver_is_required(self) -> None:
        with self.assertRaises(ValueError):
            self.svc.manual_insert("w1", start_minute=600, approver="   ")

    def test_insert_records_approver_and_displaced_batches(self) -> None:
        # 在 760（occ 作业结束后）插入，不应顶掉任何批次
        result = self.svc.manual_insert(
            "w1", start_minute=760, approver="王调度", note="紧急",
            occurred_at="2026-09-25T09:00:00+08:00",
        )
        self.assertEqual(result.status, "scheduled")
        rec = self.svc.load_state().get("w1")
        self.assertEqual(rec.overrides[0].approver, "王调度")
        self.assertEqual(rec.overrides[0].note, "紧急")
        self.assertEqual(rec.overrides[0].displaced, tuple(result.displaced))

    def test_insert_cannot_break_started_vehicle(self) -> None:
        # 两辆车都已开工，锁死两个月台与预冷间；w1 强插 480 必须被拒绝，
        # 冲突中必须指明真正占着资源的锁定批次。
        self.svc.arrive("occ-1", "2026-09-25T08:00:00+08:00")
        self.svc.start_processing("occ-1", "2026-09-25T08:00:00+08:00")
        self.svc.arrive("occ-2", "2026-09-25T08:00:00+08:00")
        self.svc.start_processing("occ-2", "2026-09-25T08:00:00+08:00")
        with self.assertRaises(ScheduleConflict) as ctx:
            self.svc.manual_insert(
                "w1", start_minute=480, approver="王调度",
                occurred_at="2026-09-25T08:05:00+08:00",
            )
        blockers = {
            b.reservation_id
            for c in ctx.exception.conflicts
            for b in c.blockers
        }
        self.assertTrue(blockers & {"occ-1", "occ-2"})


class AlarmTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service(resources=small_resources())
        saturate(self.svc)

    def test_exposure_alarm_for_arrived_vehicle_without_slot(self) -> None:
        # 单月台被一辆长作业锁死；新车到场后暴露上限极短，
        # 没有可行槽位，必须在超出上限时产生暴露告警（而不是静默候补）。
        from coldchain.capacity import Resource
        from coldchain.models import DOCK, PRECOOL, STORAGE

        tight = [
            Resource(DOCK, "dock-1", "*", 1),
            Resource(PRECOOL, "pc-f", "frozen", 100),
            Resource(STORAGE, "st-f", "frozen", 100),
        ]
        svc = make_service(resources=tight)
        svc.request_reservation(
            "hold", carrier="c", vehicle_id="v", zone="frozen", pallets=10,
            day=DAY, window_start_minute=480, window_end_minute=800,
            max_exposure_minutes=800, occurred_at="t0",
        )
        svc.arrive("hold", "2026-09-25T08:00:00+08:00")
        svc.start_processing("hold", "2026-09-25T08:00:00+08:00")
        svc.request_reservation(
            "x", carrier="c", vehicle_id="x", zone="frozen", pallets=10,
            day=DAY, window_start_minute=480, window_end_minute=800,
            max_exposure_minutes=20, occurred_at="t1",
        )
        arrived = svc.arrive("x", "2026-09-25T08:00:00+08:00")
        self.assertEqual(arrived.status, "arrived_waiting")
        # 08:30（630 分）已超过 08:00 + 20 分钟
        alarms = svc.pump_alarms(
            DAY, 540, occurred_at="2026-09-25T09:00:00+08:00"
        )
        codes = [e.payload["code"] for e in alarms if e.aggregate_id == "x"]
        self.assertIn("exposure.timeout", codes)
        # 幂等
        again = svc.pump_alarms(
            DAY, 541, occurred_at="2026-09-25T09:01:00+08:00"
        )
        self.assertFalse(
            [e for e in again if e.aggregate_id == "x"]
        )

    def test_waitlist_timeout_alarm_is_idempotent_and_persistent(self) -> None:
        path = tempfile.mktemp(suffix=".db")
        try:
            svc = make_service(path, resources=small_resources())
            saturate(svc)
            svc.request_reservation(
                "w1", carrier="c", vehicle_id="w", zone="frozen", pallets=20,
                day=DAY, window_start_minute=480, window_end_minute=500,
                max_exposure_minutes=60, timeout_after_minutes=15,
                occurred_at="2026-09-24T09:00:00+08:00",
            )
            first = svc.pump_alarms(
                DAY, 600, occurred_at="2026-09-25T10:00:00+08:00"
            )
            self.assertEqual(len(first), 1)
            # 再次扫描不重复告警
            second = svc.pump_alarms(
                DAY, 601, occurred_at="2026-09-25T10:01:00+08:00"
            )
            self.assertEqual(second, [])
            # 重启后告警仍在
            revived = make_service(path, resources=small_resources())
            codes = [a.code for a in revived.load_state().get("w1").alarms]
            self.assertEqual(codes, ["waitlist.timeout"])
        finally:
            os.unlink(path)


class ConcurrencyTest(unittest.TestCase):
    def test_two_dispatchers_cannot_both_take_last_slot(self) -> None:
        path = tempfile.mktemp(suffix=".db")
        try:
            svc = make_service(path, resources=small_resources())
            saturate(svc)
            svc.request_reservation(
                "w1", carrier="c", vehicle_id="w", zone="frozen", pallets=20,
                day=DAY, window_start_minute=480, window_end_minute=800,
                max_exposure_minutes=800, occurred_at="t2",
            )
            svc.request_reservation(
                "w2", carrier="c", vehicle_id="w", zone="frozen", pallets=20,
                day=DAY, window_start_minute=480, window_end_minute=800,
                max_exposure_minutes=800, occurred_at="t3",
            )
            # 两个调度员各自持有一份基于同一历史折叠出的状态
            dispatcher_a = make_service(path, resources=small_resources())
            dispatcher_b = make_service(path, resources=small_resources())
            state_a = dispatcher_a.load_state()
            state_b = dispatcher_b.load_state()

            def force_insert(who, state, rid):
                result, _ = who._run_day(
                    state, DAY,
                    force=(rid, ForceSlot(760, "dispatcher")),
                )
                events = who._diff_events(
                    state, DAY, result, "2026-09-25T11:00:00+08:00",
                    forced=rid,
                    force_meta={"approver": "dispatcher", "note": ""},
                )
                who._commit(events, expected=dict(state.last_seq))

            force_insert(dispatcher_a, state_a, "w1")
            with self.assertRaises(StaleVersion):
                force_insert(dispatcher_b, state_b, "w2")

            # 输家重新加载后重试：排程仍满足所有资源上限
            won = dispatcher_b.manual_insert(
                "w2", start_minute=790, approver="dispatcher-b",
                occurred_at="2026-09-25T11:05:00+08:00",
            )
            self.assertEqual(won.status, "scheduled")
            self._assert_no_over_capacity(
                make_service(path, resources=small_resources())
            )
        finally:
            os.unlink(path)

    def _assert_no_over_capacity(self, svc) -> None:
        from coldchain.capacity import Ledger

        ledger = Ledger(small_resources())
        for rec in svc.load_state().reservations.values():
            if not rec.plan or rec.state == "cancelled":
                continue
            for op in rec.plan.operations:
                conflict = ledger.check_interval(
                    op.resource_type, op.resource_id,
                    op.start_minute, op.end_minute, op.pallets,
                )
                self.assertIsNone(conflict, msg=f"{rec.reservation_id}: {conflict}")
                ledger.add(rec.reservation_id, op)


if __name__ == "__main__":
    unittest.main()
