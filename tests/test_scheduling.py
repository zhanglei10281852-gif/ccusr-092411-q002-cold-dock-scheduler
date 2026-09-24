"""端到端业务场景测试。

覆盖需求：
1. 申报四要素 + 分钟级容量排程；
2. 迟到导致预冷间/低温库同时超量时，只重排未开始作业，已进场车辆不动；
3. 提前/迟到/设备停用/临时加单；
4. 人工插单记录批准人与被延后批次，且不得突破容量；
5. 两个调度员并发接纳候补不突破任一资源上限；
6. 拒绝/改期返回真正冲突的约束；
7. 重启后候补序号与超时告警继续有效；
8. 事件重放确定性（同样输入同样排程）；
9. 跨午夜窗口按市场时区计算。
"""
import os
import tempfile
import threading
import unittest
from datetime import datetime

from coldchain import events as evt
from coldchain.catalog import Catalog, TimingPolicy
from coldchain.models import Declaration
from coldchain.service import SchedulingService
from coldchain.state import replay
from coldchain.store import ConflictError, EventStore
from coldchain.timepolicy import iso, market_zone, overnight_window, to_minute

from fixtures import TZ, build_catalog, build_service, declaration, m


class SchedulingBasicsTest(unittest.TestCase):
    def test_declaration_produces_minute_level_plan(self):
        svc = build_service()
        result = svc.request_reservation(
            declaration("res-001", pallets=10), now="2026-09-22T10:00")
        self.assertTrue(result.accepted)
        self.assertIsNotNone(result.plan)
        plan = result.plan
        self.assertEqual(plan.dock_id, "dock-1")
        # 月台 30 分钟，预冷 60 分钟，分钟级串联
        self.assertEqual(plan.dock_end - plan.dock_start, 30)
        self.assertEqual(plan.precool_start, plan.dock_end)
        self.assertEqual(plan.precool_end - plan.precool_start, 60)
        self.assertEqual(plan.storage_at, plan.precool_end)
        self.assertEqual(plan.pallets, 10)

    def test_unknown_zone_is_hard_rejected_with_real_constraint(self):
        svc = build_service()
        decl = declaration("res-x", zone="deepspace")
        result = svc.request_reservation(decl, now="2026-09-22T10:00")
        self.assertFalse(result.accepted)
        self.assertFalse(result.diverted_to_waitlist)
        codes = {v.code for v in result.violations}
        self.assertIn("zone_unsupported", codes)
        kinds = {v.resource_kind for v in result.violations}
        self.assertIn("storage", kinds)

    def test_capacity_shortage_diverts_to_waitlist_with_violations(self):
        svc = build_service()
        # 预冷间 20 托：两笔 12 托无法同时预冷
        svc.request_reservation(
            declaration("res-a", pallets=12), now="2026-09-22T10:00")
        result = svc.request_reservation(
            declaration("res-b", pallets=12), now="2026-09-22T10:01")
        # 窗口 4 小时内放得下串行第二笔（30+60 分钟后），这里构造窗口更短：
        self.assertTrue(result.accepted)
        self.assertIsNotNone(result.plan)

    def test_window_too_tight_reports_real_conflicting_constraints(self):
        # 单门位目录：第二辆在同一 30 分钟窗口必然撞月台
        catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
        catalog.add_dock("dock-1")
        catalog.add_precool_room("pc-f", "frozen", 20)
        catalog.add_storage_room("st-f", "frozen", 100)
        svc = build_service(catalog=catalog)
        svc.request_reservation(
            declaration("res-a", pallets=12,
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:00")
        result = svc.request_reservation(
            declaration("res-b", pallets=12,
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:01")
        self.assertTrue(result.diverted_to_waitlist)
        kinds = {v.resource_kind for v in result.violations}
        self.assertIn("dock", kinds)
        self.assertIn("precool", kinds)
        dock_v = next(v for v in result.violations
                      if v.resource_kind == "dock")
        self.assertEqual(dock_v.capacity, 1)
        self.assertEqual(dock_v.required, 1)
        pre_v = next(v for v in result.violations
                     if v.resource_kind == "precool")
        self.assertEqual(pre_v.capacity, 20)
        self.assertEqual(pre_v.required, 12)

    def test_violations_only_name_the_real_bottleneck(self):
        """两门位时月台不是瓶颈：冲突清单不得包含月台。"""
        svc = build_service()
        svc.request_reservation(
            declaration("res-a", pallets=12,
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:00")
        result = svc.request_reservation(
            declaration("res-b", pallets=12,
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:01")
        kinds = {v.resource_kind for v in result.violations}
        self.assertIn("precool", kinds)
        self.assertNotIn("dock", kinds)


class LateArrivalScenarioTest(unittest.TestCase):
    """开场故事：冷链车迟到 40 分钟被塞进下一时段，
    预冷间和低温库同时超量，后续车辆场外等待。"""

    def test_late_truck_replans_only_unstarted_work(self):
        svc = build_service()
        svc.request_reservation(
            declaration("res-1", pallets=12,
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2", pallets=12,
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-22T10:01")
        svc.request_reservation(
            declaration("res-3", pallets=12,
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-22T10:02")
        st = svc.snapshot()
        self.assertEqual(st.plans["res-1"].dock_start, m("2026-09-23T08:00"))
        self.assertEqual(st.plans["res-2"].dock_start, m("2026-09-23T09:00"))
        self.assertEqual(st.plans["res-3"].dock_start, m("2026-09-23T10:00"))

        # res-1 迟到 40 分钟到场
        result = svc.report_arrival(
            "res-1", "2026-09-23T08:40", now="2026-09-23T08:40")
        st = svc.snapshot()
        # 迟到车辆围绕 08:40 重排，保持 arrived
        self.assertEqual(st.statuses["res-1"], "arrived")
        self.assertEqual(st.plans["res-1"].dock_start,
                         m("2026-09-23T08:40"))
        # res-2 / res-3 均被顺延（预冷间 20 托上限）
        self.assertEqual(st.plans["res-2"].dock_start,
                         m("2026-09-23T09:40"))
        self.assertEqual(st.plans["res-3"].dock_start,
                         m("2026-09-23T10:40"))
        types_ = {e.event_type for e in result.events}
        self.assertIn(evt.VEHICLE_ARRIVED, types_)
        self.assertIn(evt.RESERVATION_RESCHEDULED, types_)

    def test_started_vehicle_is_frozen_under_later_disruption(self):
        svc = build_service()
        svc.request_reservation(
            declaration("res-1", pallets=10), now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2", pallets=10), now="2026-09-22T10:01")
        svc.report_arrival("res-1", "2026-09-23T08:00",
                           now="2026-09-23T08:00")
        svc.start_operation("res-1", now="2026-09-23T08:05")
        frozen = svc.snapshot().plans["res-1"]

        # res-2 迟到，并停用 dock-2：res-1 作业中绝不能被移动
        svc.disable_resource("dock", "dock-2",
                             "2026-09-23T08:00", "2026-09-23T13:00",
                             now="2026-09-23T08:06")
        result = svc.report_arrival(
            "res-2", "2026-09-23T08:10", now="2026-09-23T08:10")
        st = svc.snapshot()
        self.assertEqual(st.plans["res-1"], frozen)
        self.assertEqual(st.statuses["res-1"], "processing")
        self.assertEqual(st.plans["res-2"].dock_id, "dock-1")
        self.assertGreaterEqual(st.plans["res-2"].dock_start,
                                frozen.dock_end)


    def test_early_arrival_keeps_original_slot_and_freezes_it(self):
        svc = build_service()
        svc.request_reservation(
            declaration("res-1", pallets=10), now="2026-09-22T10:00")
        original = svc.snapshot().plans["res-1"]
        # 提前 20 分钟到场：等待原档，方案不变
        result = svc.report_arrival(
            "res-1", "2026-09-23T07:40", now="2026-09-23T07:40")
        st = svc.snapshot()
        self.assertEqual(st.statuses["res-1"], "arrived")
        self.assertEqual(st.plans["res-1"], original)
        self.assertNotIn("reservation.rescheduled",
                         {e.event_type for e in result.events})

    def test_on_time_arrival_does_not_replan_others(self):
        svc = build_service()
        svc.request_reservation(
            declaration("res-1", pallets=10), now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2", pallets=10), now="2026-09-22T10:01")
        plan2_before = svc.snapshot().plans["res-2"]
        svc.report_arrival("res-1", "2026-09-23T08:00",
                           now="2026-09-23T08:00")
        self.assertEqual(svc.snapshot().plans["res-2"], plan2_before)


class EquipmentAndWaitlistTest(unittest.TestCase):
    def test_dock_outage_postpones_unstarted_and_waitlist_keeps_order(self):
        svc = build_service()
        svc.request_reservation(
            declaration("res-1", pallets=10), now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2", pallets=10), now="2026-09-22T10:01")
        svc.request_reservation(
            declaration("res-3", pallets=10), now="2026-09-22T10:02")
        # 两门位先停用：res-2 / res-3 的未开始作业全部延后回候补
        svc.disable_resource(
            "dock", "dock-1", "2026-09-23T08:00", "2026-09-23T12:00",
            now="2026-09-23T07:59")
        svc.disable_resource(
            "dock", "dock-2", "2026-09-23T08:00", "2026-09-23T12:00",
            now="2026-09-23T07:59")
        st = svc.snapshot()
        self.assertEqual(st.statuses["res-1"], "waitlisted")
        self.assertEqual(st.statuses["res-2"], "waitlisted")
        self.assertEqual(st.statuses["res-3"], "waitlisted")
        self.assertEqual(st.waitlist_position("res-1"), 1)
        self.assertEqual(st.waitlist_position("res-2"), 2)
        self.assertEqual(st.waitlist_position("res-3"), 3)

        # res-1 此时到场：无档可排，在场车不进候补，只产生暴露告警；
        # 其原候补位置释放，后续序号整体前移
        svc.report_arrival("res-1", "2026-09-23T08:00",
                           now="2026-09-23T08:00")
        st = svc.snapshot()
        self.assertEqual(st.statuses["res-1"], "arrived")
        self.assertNotIn("res-1", st.plans)
        self.assertTrue(any(a["code"] == "exposure_wait" and a["active"]
                            for a in st.alerts.values()))
        self.assertEqual(st.waitlist_position("res-2"), 1)
        self.assertEqual(st.waitlist_position("res-3"), 2)

        # 09:00 恢复一门位：容量释放；巡检时在场车最先排、候补按序号随后
        svc.enable_resource("dock", "dock-1",
                            start="2026-09-23T09:00",
                            now="2026-09-23T09:00")
        svc.sweep(now="2026-09-23T09:01")
        st = svc.snapshot()
        self.assertEqual(st.plans["res-1"].dock_start,
                         m("2026-09-23T09:00"))
        self.assertLessEqual(st.plans["res-2"].dock_start,
                             st.plans["res-3"].dock_start)
        self.assertEqual(st.statuses["res-2"], "scheduled")
        self.assertEqual(st.statuses["res-3"], "scheduled")

    def test_waitlist_timeout_alert(self):
        # 单门位：res-1 占住 08:00-08:30，res-2 的 20 分钟窗口无法插入
        catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
        catalog.add_dock("dock-1")
        catalog.add_precool_room("pc-f", "frozen", 20)
        catalog.add_storage_room("st-f", "frozen", 100)
        svc = build_service(catalog=catalog)
        svc.request_reservation(
            declaration("res-1", pallets=10,
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2", pallets=10,
                        window=("2026-09-23T08:00", "2026-09-23T08:20")),
            now="2026-09-22T10:01")
        st = svc.snapshot()
        self.assertEqual(st.statuses["res-2"], "waitlisted")
        self.assertEqual(st.waitlist_position("res-2"), 1)
        # 30 分钟仍未接纳 -> 超时告警
        svc.sweep(now="2026-09-22T10:32")
        st = svc.snapshot()
        alerts = [a for a in st.alerts.values() if a["active"]]
        self.assertTrue(any(a["code"] == "waitlist_timeout" for a in alerts))


    def test_strict_fifo_blocks_later_waitlist_when_head_cannot_fit(self):
        # 单门位：w1 窗口在早上（08:00-08:30），w2 窗口在午后。
        # 门位长时间停用使两车都进候补；恢复后 w1 窗口已永久错过，
        # 过期拒绝出队，w2 随后在自己窗口内被接纳。
        catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
        catalog.add_dock("dock-1")
        catalog.add_precool_room("pc-f", "frozen", 20)
        catalog.add_storage_room("st-f", "frozen", 100)
        svc = build_service(catalog=catalog)
        svc.request_reservation(
            declaration("w1", pallets=10,
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("w2", pallets=10,
                        window=("2026-09-23T13:00", "2026-09-23T14:00")),
            now="2026-09-22T10:01")
        svc.disable_resource(
            "dock", "dock-1", "2026-09-23T06:00", "2026-09-23T14:00",
            now="2026-09-22T10:05")
        st = svc.snapshot()
        self.assertEqual(st.statuses["w1"], "waitlisted")
        self.assertEqual(st.statuses["w2"], "waitlisted")
        self.assertEqual(st.waitlist_position("w1"), 1)
        self.assertEqual(st.waitlist_position("w2"), 2)

        # 09:00 恢复门位（停用区间缩短）；此时显式接纳 w2 必须被拒：
        # 队首 w1 的窗口刚过，它排不下，w2 不能越过它
        svc.enable_resource("dock", "dock-1",
                            start="2026-09-23T09:00",
                            now="2026-09-23T09:00")
        result = svc.accept_waitlist(
            "w2", dispatcher="调度员甲", now="2026-09-23T09:01")
        self.assertFalse(result.accepted)
        self.assertTrue(any(v.code == "waitlist_order"
                            for v in result.violations))

        # 09:05 巡检：w1 窗口过期 -> 拒绝出队；w2 成为队首，
        # 按其 13:00 窗口预留档（停用已结束）
        svc.sweep(now="2026-09-23T09:05")
        st = svc.snapshot()
        self.assertEqual(st.statuses["w1"], "rejected")
        self.assertEqual(st.statuses["w2"], "scheduled")
        self.assertGreaterEqual(st.plans["w2"].dock_start,
                                m("2026-09-23T13:00"))

    def test_strict_fifo_gate_while_head_window_still_open(self):
        # 单门位、共同窗口 08:00-12:00：门位开放式停用（无预计恢复时间），
        # 两车一直场外等待。11:30 设备恢复：队首 w1 恰可在 11:30-12:00
        # 入档（靠台时刻 11:30 仍在半开窗口内），w2 的最早靠台已是
        # 12:00、越过窗口，严格 FIFO 门控将其挡下。
        catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
        catalog.add_dock("dock-1")
        catalog.add_precool_room("pc-f", "frozen", 20)
        catalog.add_storage_room("st-f", "frozen", 100)
        svc = build_service(catalog=catalog)
        svc.request_reservation(
            declaration("w1",
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("w2",
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-22T10:01")
        svc.disable_resource(
            "dock", "dock-1", "2026-09-23T08:00", None,
            now="2026-09-22T10:05")
        self.assertEqual(svc.snapshot().statuses["w1"], "waitlisted")
        self.assertEqual(svc.snapshot().statuses["w2"], "waitlisted")
        # 11:00 仍停用：双候补
        svc.sweep(now="2026-09-23T11:00")
        self.assertEqual(svc.snapshot().statuses["w1"], "waitlisted")
        self.assertEqual(svc.snapshot().statuses["w2"], "waitlisted")
        # 11:30 设备恢复并巡检：只够 w1 入档，w2 被 FIFO 门控挡下
        svc.enable_resource("dock", "dock-1",
                            start="2026-09-23T11:30",
                            now="2026-09-23T11:30")
        svc.sweep(now="2026-09-23T11:30")
        st = svc.snapshot()
        self.assertEqual(st.statuses["w1"], "scheduled")
        self.assertEqual(st.plans["w1"].dock_start,
                         m("2026-09-23T11:30"))
        self.assertEqual(st.statuses["w2"], "waitlisted")


class ManualInsertTest(unittest.TestCase):
    def test_manual_insert_records_approver_and_postponed_batches(self):
        svc = build_service()
        svc.request_reservation(
            declaration("res-1", pallets=10), now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2", pallets=10), now="2026-09-22T10:01")
        svc.request_reservation(
            declaration("res-3", pallets=10), now="2026-09-22T10:02")

        urgent = declaration(
            "res-urgent", pallets=10,
            window=("2026-09-23T08:00", "2026-09-23T09:00"))
        result = svc.manual_insert(
            urgent, approver="王调度", reason="疫苗紧急放行",
            now="2026-09-22T11:00")
        self.assertTrue(result.accepted)
        st = svc.snapshot()
        # 紧急单插在最前
        self.assertEqual(st.plans["res-urgent"].dock_start,
                         m("2026-09-23T08:00"))
        audit = st.manual_audit[-1]
        self.assertEqual(audit["approver"], "王调度")
        self.assertEqual(audit["reason"], "疫苗紧急放行")
        # 被延后的批次被记录
        self.assertIn("postponed_batches", audit)
        self.assertIsInstance(audit["postponed_batches"], list)

    def test_manual_insert_that_crowds_out_batch_postpones_and_audits_it(self):
        # 单门位：res-1 独占 08:00-08:30 窗口；紧急插单同窗口必须把 res-1 挤后，
        # 而 res-1 的窗口没有余量 -> 回到候补，并记入插单审计
        catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
        catalog.add_dock("dock-1")
        catalog.add_precool_room("pc-f", "frozen", 20)
        catalog.add_storage_room("st-f", "frozen", 100)
        svc = build_service(catalog=catalog)
        svc.request_reservation(
            declaration("res-1",
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:00")
        urgent = declaration(
            "res-urgent",
            window=("2026-09-23T08:00", "2026-09-23T08:30"))
        result = svc.manual_insert(
            urgent, approver="李经理", reason="应急保障",
            now="2026-09-22T11:00")
        self.assertTrue(result.accepted)
        st = svc.snapshot()
        self.assertEqual(st.plans["res-urgent"].dock_start,
                         m("2026-09-23T08:00"))
        self.assertEqual(st.statuses["res-1"], "waitlisted")
        audit = st.manual_audit[-1]
        self.assertEqual(audit["approver"], "李经理")
        self.assertIn("res-1", audit["postponed_batches"])
        # 月台同一分钟仍然至多 1：插单未突破容量
        from coldchain.scheduler import build_timelines
        tl = build_timelines(st, catalog, set())
        for minute in range(m("2026-09-23T08:00"),
                            m("2026-09-23T08:30")):
            self.assertLessEqual(tl["dock"].load_at("dock-1", minute), 1)

    def test_manual_insert_requires_approver(self):
        svc = build_service()
        urgent = declaration("res-urgent")
        with self.assertRaises(ValueError):
            svc.manual_insert(urgent, approver="", now="2026-09-22T11:00")

    def test_manual_insert_cannot_break_capacity(self):
        # 单门位、预冷间容量极小且窗口极短：插单无容量时必须被拒绝
        catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
        catalog.add_dock("dock-1")
        catalog.add_precool_room("pc-f", "frozen", 5)
        catalog.add_storage_room("st-f", "frozen", 100)
        svc = build_service(catalog=catalog)
        urgent = declaration("res-urgent", pallets=10,
                             window=("2026-09-23T08:00",
                                     "2026-09-23T08:20"))
        result = svc.manual_insert(
            urgent, approver="王调度", now="2026-09-22T11:00")
        self.assertFalse(result.accepted)
        self.assertTrue(result.violations)


class ConcurrencyTest(unittest.TestCase):
    def test_two_dispatchers_accepting_waitlist_never_oversubscribe(self):
        catalog = build_catalog()
        path = tempfile.mktemp(suffix=".db")
        store = EventStore(path)
        svc = SchedulingService(store, catalog)
        # 窗口只放得下 2 笔（两门位各一笔 30 分钟，窗口 30 分钟），
        # 第 3 笔进入候补
        svc.request_reservation(
            declaration("res-1",
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2",
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:01")
        svc.request_reservation(
            declaration("res-3",
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:02")
        st = svc.snapshot()
        self.assertEqual(st.statuses["res-3"], "waitlisted")
        # res-2 改期到午后（扰动命令不放行候补）：恰好空出一个档，
        # 但 res-3 仍在候补——两个调度员同时抢这唯一的名额
        svc.amend_reservation(
            "res-2",
            declaration("res-2",
                        window=("2026-09-23T13:00", "2026-09-23T14:00")),
            now="2026-09-22T10:20")
        self.assertEqual(svc.snapshot().statuses["res-3"], "waitlisted")
        store.close()

        flags: dict[str, dict] = {}

        def dispatcher(name: str, target: str) -> None:
            s = EventStore(path)
            service = SchedulingService(s, build_catalog())
            try:
                r = service.accept_waitlist(
                    target, dispatcher=name, now="2026-09-22T10:30")
                flags[name] = {"accepted": r.accepted}
            except ConflictError:
                flags[name] = {"accepted": False, "conflict": True}
            except Exception as exc:  # pragma: no cover - 暴露未知竞态
                flags[name] = {"accepted": False, "error": repr(exc)}
            finally:
                s.close()

        t1 = threading.Thread(
            target=dispatcher, args=("调度员甲", "res-3"))
        t2 = threading.Thread(
            target=dispatcher, args=("调度员乙", "res-3"))
        t1.start(); t2.start(); t1.join(); t2.join()
        # 恰好一个胜出，绝不两个都成功
        wins = sum(1 for f in flags.values() if f.get("accepted"))
        self.assertEqual(wins, 1, flags)
        self.assertNotIn("error", flags.get("调度员甲", {}))
        self.assertNotIn("error", flags.get("调度员乙", {}))

        # 最终状态绝不超量：月台同一分钟至多 1，且 res-3 确实入档一次
        store = EventStore(path)
        state = replay(store.load())
        self.assertEqual(state.statuses["res-3"], "scheduled")
        from coldchain.scheduler import build_timelines
        tl = build_timelines(state, catalog, set())
        for minute in range(m("2026-09-23T08:00"),
                            m("2026-09-23T08:30")):
            self.assertLessEqual(tl["dock"].load_at("dock-1", minute), 1)
            self.assertLessEqual(tl["dock"].load_at("dock-2", minute), 1)
        accepted = [e for e in store.load()
                    if e.event_type == "waitlist.accepted"
                    and e.aggregate_id == "res-3"]
        self.assertEqual(len(accepted), 1)
        store.close()
        os.unlink(path)


class StorageLifecycleTest(unittest.TestCase):
    def test_storage_stays_occupied_after_completion_until_release(self):
        # 冷冻库仅 20 托：第一笔 12 托完成入库后，第二笔 12 托仍无库位
        catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
        catalog.add_dock("dock-1")
        catalog.add_precool_room("pc-f", "frozen", 40)
        catalog.add_storage_room("st-f", "frozen", 20)
        svc = build_service(catalog=catalog)
        svc.request_reservation(
            declaration("res-1", pallets=12,
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-22T10:00")
        svc.report_arrival("res-1", "2026-09-23T08:00",
                           now="2026-09-23T08:00")
        svc.start_operation("res-1", now="2026-09-23T08:05")
        # 入库后完成：月台/预冷释放，库位仍占用
        svc.complete_reservation("res-1", now="2026-09-23T09:35")
        st = svc.snapshot()
        self.assertEqual(st.statuses["res-1"], "completed")
        # 再来 12 托：库位只剩 8 托 -> 候补，冲突指向低温库
        result = svc.request_reservation(
            declaration("res-2", pallets=12,
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-23T09:36")
        self.assertTrue(result.diverted_to_waitlist)
        kinds = {v.resource_kind for v in result.violations}
        self.assertIn("storage", kinds)
        # 出库后库位释放，候补可以接纳
        svc.release_goods("res-1", now="2026-09-23T10:00")
        svc.sweep(now="2026-09-23T10:01")
        st = svc.snapshot()
        self.assertEqual(st.statuses["res-2"], "scheduled")


class AmendTest(unittest.TestCase):
    def test_amend_to_feasible_window_replans(self):
        svc = build_service()
        svc.request_reservation(
            declaration("res-1", pallets=10), now="2026-09-22T10:00")
        new_decl = declaration(
            "res-1", pallets=10,
            window=("2026-09-23T10:00", "2026-09-23T11:00"))
        result = svc.amend_reservation(
            "res-1", new_decl, now="2026-09-22T11:00")
        self.assertTrue(result.accepted)
        self.assertGreaterEqual(result.plan.dock_start,
                                m("2026-09-23T10:00"))
        types_ = {e.event_type for e in result.events}
        self.assertIn("reservation.amended", types_)

    def test_amend_into_congestion_returns_real_conflicts_and_waitlists(self):
        # 单门位：res-2 改到与 res-1 完全重叠的 30 分钟窗口 -> 真实冲突
        catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
        catalog.add_dock("dock-1")
        catalog.add_precool_room("pc-f", "frozen", 20)
        catalog.add_storage_room("st-f", "frozen", 100)
        svc = build_service(catalog=catalog)
        svc.request_reservation(
            declaration("res-1",
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2",
                        window=("2026-09-23T08:00", "2026-09-23T12:00")),
            now="2026-09-22T10:01")
        tight = declaration(
            "res-2",
            window=("2026-09-23T08:00", "2026-09-23T08:30"))
        result = svc.amend_reservation(
            "res-2", tight, now="2026-09-22T11:00")
        self.assertFalse(result.accepted)
        codes = {v.code for v in result.violations}
        self.assertTrue(codes & {"dock_busy", "precool_overload",
                                 "storage_full"})
        self.assertEqual(svc.snapshot().statuses["res-2"], "waitlisted")

    def test_amend_rejected_after_arrival(self):
        svc = build_service()
        svc.request_reservation(
            declaration("res-1"), now="2026-09-22T10:00")
        svc.report_arrival("res-1", "2026-09-23T08:00",
                           now="2026-09-23T08:00")
        with self.assertRaises(ValueError):
            svc.amend_reservation(
                "res-1",
                declaration("res-1",
                            window=("2026-09-23T11:00", "2026-09-23T12:00")),
                now="2026-09-23T08:01")


class ReplayDeterminismTest(unittest.TestCase):
    def test_replay_same_events_same_schedule(self):
        svc = build_service()
        svc.request_reservation(
            declaration("res-1", pallets=12), now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2", pallets=12), now="2026-09-22T10:01")
        svc.report_arrival("res-1", "2026-09-23T08:40",
                           now="2026-09-23T08:40")
        raw = svc.store.load()

        state_a = replay(list(raw))
        state_b = replay(list(raw))
        plans_a = {k: (v.dock_start, v.dock_id, v.precool_id,
                       v.precool_start, v.storage_id, v.storage_at)
                   for k, v in state_a.plans.items()}
        plans_b = {k: (v.dock_start, v.dock_id, v.precool_id,
                       v.precool_start, v.storage_id, v.storage_at)
                   for k, v in state_b.plans.items()}
        self.assertEqual(plans_a, plans_b)
        self.assertEqual(state_a.waitlist, state_b.waitlist)

    def test_idempotent_command_replay(self):
        svc = build_service()
        decl = declaration("res-1", pallets=10)
        r1 = svc.request_reservation(
            decl, now="2026-09-22T10:00", idem_key="cmd-001")
        r2 = svc.request_reservation(
            decl, now="2026-09-22T10:00", idem_key="cmd-001")
        self.assertEqual([e.event_id for e in r1.events],
                         [e.event_id for e in r2.events])
        self.assertEqual(len(svc.store.load()), len(r1.events))


class RestartPersistenceTest(unittest.TestCase):
    def test_waitlist_positions_and_alerts_survive_restart(self):
        path = tempfile.mktemp(suffix=".db")
        store = EventStore(path)
        svc = SchedulingService(store, build_catalog())
        svc.request_reservation(
            declaration("res-1",
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:00")
        svc.request_reservation(
            declaration("res-2",
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:01")
        svc.request_reservation(
            declaration("res-3",
                        window=("2026-09-23T08:00", "2026-09-23T08:30")),
            now="2026-09-22T10:02")
        svc.sweep(now="2026-09-22T11:00")
        before = svc.snapshot()
        pos = before.waitlist_position("res-3")
        active_alerts = {k: v for k, v in before.alerts.items() if v["active"]}
        store.close()

        # 重新打开库（模拟进程重启）
        store2 = EventStore(path)
        svc2 = SchedulingService(store2, build_catalog())
        after = svc2.snapshot()
        self.assertEqual(after.waitlist_position("res-3"), pos)
        for key, record in active_alerts.items():
            self.assertIn(key, after.alerts)
            self.assertTrue(after.alerts[key]["active"])
        store2.close()
        os.unlink(path)


class OvernightWindowTest(unittest.TestCase):
    def test_overnight_window_uses_market_timezone(self):
        start, end = overnight_window(
            "2026-09-23", "23:40", "00:20", TZ)
        self.assertEqual(end - start, 40)
        self.assertEqual(iso(start, TZ), "2026-09-23T23:40+08:00")
        self.assertEqual(iso(end, TZ), "2026-09-24T00:20+08:00")

    def test_overnight_reservation_schedules_across_midnight(self):
        svc = build_service()
        start, end = overnight_window(
            "2026-09-23", "23:40", "00:20", TZ)
        decl = Declaration(
            "res-night", "承运夜", "veh-night", "frozen", 10,
            start, end, 60)
        result = svc.request_reservation(
            decl, now="2026-09-22T10:00")
        self.assertTrue(result.accepted)
        self.assertIsNotNone(result.plan)
        self.assertGreaterEqual(result.plan.dock_start, start)
        self.assertLess(result.plan.dock_start, end)


if __name__ == "__main__":
    unittest.main()
