"""确定性重放与事件库测试。"""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from coldchain.state import State
from coldchain.store import EventStore, StaleVersion

from tests.support import make_service


class ReplayDeterminismTest(unittest.TestCase):
    def _scenario(self, svc) -> None:
        day = "2026-09-25"
        svc.request_reservation(
            "a", carrier="c", vehicle_id="v1", zone="frozen", pallets=10,
            day=day, window_start_minute=480, window_end_minute=760,
            max_exposure_minutes=500, occurred_at="2026-09-24T08:00+08:00",
        )
        svc.request_reservation(
            "b", carrier="c", vehicle_id="v2", zone="frozen", pallets=10,
            day=day, window_start_minute=480, window_end_minute=760,
            max_exposure_minutes=500, occurred_at="2026-09-24T08:05+08:00",
        )
        svc.request_reservation(
            "c", carrier="c", vehicle_id="v3", zone="frozen", pallets=20,
            day=day, window_start_minute=480, window_end_minute=500,
            max_exposure_minutes=60, timeout_after_minutes=20,
            occurred_at="2026-09-24T08:10+08:00",
        )
        # b 迟到
        svc.arrive("b", "2026-09-25T08:40:00+08:00")
        # a 开工锁定
        svc.arrive("a", "2026-09-25T08:00:00+08:00")
        svc.start_processing("a", "2026-09-25T08:00:00+08:00")
        # 设备停用（事后补录，occurred_at 早于已提交的计划事件）
        svc.disable_equipment(
            day, "dock", "dock-2", 600, 660,
            reason="维修", occurred_at="2026-09-25T07:30:00+08:00",
        )
        # 超时告警
        svc.pump_alarms(day, 700, occurred_at="2026-09-25T11:40:00+08:00")
        # 人工插单
        svc.manual_insert(
            "c", start_minute=700, approver="李调度",
            occurred_at="2026-09-25T09:00:00+08:00",
        )

    def _snapshot(self, state: State) -> str:
        return json.dumps(
            {
                rid: {
                    "state": rec.state,
                    "waitlist_seq": rec.waitlist_seq,
                    "plan": rec.plan.to_dict() if rec.plan else None,
                    "alarms": [a.to_dict() if hasattr(a, "to_dict")
                               else (a.code, a.cleared) for a in rec.alarms],
                    "overrides": [
                        {"approver": o.approver, "displaced": list(o.displaced)}
                        for o in rec.overrides
                    ],
                }
                for rid, rec in sorted(state.reservations.items())
            },
            sort_keys=True, ensure_ascii=False,
        )

    def test_replay_same_stream_any_times_identical(self) -> None:
        svc = make_service()
        self._scenario(svc)
        events = svc.store.load_events()
        first = self._snapshot(State.fold(events))
        for _ in range(5):
            self.assertEqual(self._snapshot(State.fold(events)), first)

    def test_replay_after_restart_identical(self) -> None:
        path = tempfile.mktemp(suffix=".db")
        try:
            svc = make_service(path)
            self._scenario(svc)
            before = self._snapshot(State.fold(svc.store.load_events()))
            revived = make_service(path)
            after = self._snapshot(revived.load_state())
            self.assertEqual(before, after)
        finally:
            os.unlink(path)

    def test_fresh_runs_produce_byte_identical_event_stream(self) -> None:
        paths = [tempfile.mktemp(suffix=".db") for _ in range(2)]
        streams = []
        try:
            for path in paths:
                svc = make_service(path)
                self._scenario(svc)
                streams.append([e.to_json() for e in svc.store.load_events()])
            self.assertEqual(streams[0], streams[1])
        finally:
            for path in paths:
                os.unlink(path)

    def test_late_recorded_event_keeps_commit_order_semantics(self) -> None:
        # occurred_at 早于既有事件的设备停用，按提交顺序折叠后
        # 不应把已开工车辆的状态/计划改回去
        svc = make_service()
        day = "2026-09-25"
        svc.request_reservation(
            "a", carrier="c", vehicle_id="v", zone="frozen", pallets=10,
            day=day, window_start_minute=480, window_end_minute=700,
            max_exposure_minutes=500, occurred_at="2026-09-24T08:00+08:00",
        )
        svc.arrive("a", "2026-09-25T08:00:00+08:00")
        svc.start_processing("a", "2026-09-25T08:00:00+08:00")
        svc.disable_equipment(
            day, "dock", "dock-1", 0, 1440,
            reason="突发故障", occurred_at="2026-09-25T06:00:00+08:00",
        )
        rec = svc.load_state().get("a")
        self.assertEqual(rec.state, "processing")
        self.assertTrue(rec.plan.operations[0].locked)


class EventStoreTest(unittest.TestCase):
    def test_concurrent_append_uses_optimistic_version(self) -> None:
        store = EventStore(":memory:")
        from coldchain.events import make_event

        e1 = make_event("reservation.requested", "r1", 1, "t1", {"x": 1})
        store.append([e1], {"r1": 0})
        # 基于过期版本号追加必须失败
        e2 = make_event("reservation.scheduled", "r1", 2, "t2", {"x": 2})
        with self.assertRaises(StaleVersion):
            store.append([e2], {"r1": 0})
        # 使用正确版本号成功
        store.append([e2], {"r1": 1})
        self.assertEqual(store.last_seq("r1"), 2)

    def test_gap_in_sequence_rejected(self) -> None:
        store = EventStore(":memory:")
        from coldchain.events import make_event

        e = make_event("reservation.requested", "r9", 5, "t", {})
        with self.assertRaises(StaleVersion):
            store.append([e])


if __name__ == "__main__":
    unittest.main()
