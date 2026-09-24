"""原始事故复盘演示。

场景：一辆冷链车迟到 40 分钟。旧的人工做法把它临时塞进下一时段，
导致预冷间与低温库同时超量，后续两车只能在场外等待。

本演示用系统接管同一情况，展示：
迟到车自动排入最近可行槽位、已开工车辆不动、被挤的车进入候补并
保留序号、设备停用触发重排、人工插单记录批准人与被延后批次。

运行：python3 tools/demo_incident.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coldchain.capacity import Resource
from coldchain.models import DOCK, PRECOOL, STORAGE
from coldchain.service import ColdChainService, ScheduleConflict
from coldchain.store import EventStore
from coldchain.timeutil import TimeService

DAY = "2026-09-26"


def line(title: str) -> None:
    print("\n" + "=" * 64)
    print(title)
    print("=" * 64)


def show(svc: ColdChainService, *rids: str) -> None:
    state = svc.load_state()
    for rid in rids:
        rec = state.get(rid)
        if rec.plan and rec.plan.operations:
            chain = " → ".join(
                f"{op.resource_id}@{op.start}"[0:32] for op in rec.plan.operations
            )
            minutes = " → ".join(
                f"{op.resource_id}:{op.start_minute}-{op.end_minute}"
                for op in rec.plan.operations
            )
            print(f"  [{rec.state:>10}] {rid}: {minutes}")
        else:
            seq = f" 候补#{rec.waitlist_seq}" if rec.waitlist_seq else ""
            print(f"  [{rec.state:>10}] {rid}{seq}")


def main() -> None:
    resources = [
        Resource(DOCK, "dock-A", "*", 1),
        Resource(DOCK, "dock-B", "*", 1),
        Resource(PRECOOL, "pc-冷冻", "frozen", 20),
        Resource(STORAGE, "库-冷冻", "frozen", 60),
    ]
    svc = ColdChainService(
        EventStore(":memory:"), resources, TimeService("Asia/Shanghai")
    )

    line("1. 承运方申报（到场区间/温区/托盘数/最长暴露时间）")
    # 08:00-08:30 窗口，冻品，各 10 托，最长暴露 90 分钟
    for rid, ts in (("冷链车-甲", "07:30"), ("冷链车-乙", "07:31")):
        svc.request_reservation(
            rid, carrier="某物流", vehicle_id=rid, zone="frozen", pallets=10,
            day=DAY, window_start_minute=480, window_end_minute=510,
            max_exposure_minutes=90,
            timeout_after_minutes=30,
            occurred_at=f"2026-09-25T{ts}+08:00",
        )
    show(svc, "冷链车-甲", "冷链车-乙")

    line("2. 后续两车申报同一高峰，容量不足 → 进入候补（序号持久）")
    for rid, ts in (("冷链车-丙", "07:32"), ("冷链车-丁", "07:33")):
        result = svc.request_reservation(
            rid, carrier="某物流", vehicle_id=rid, zone="frozen", pallets=20,
            day=DAY, window_start_minute=480, window_end_minute=510,
            max_exposure_minutes=90, timeout_after_minutes=30,
            occurred_at=f"2026-09-25T{ts}+08:00",
        )
        print(f"  {rid}: {result.status}"
              + (f" #{result.waitlist_seq}" if result.waitlist_seq else ""))
    print("  候补队列:", [r.reservation_id for r in svc.load_state().waitlist(DAY)])

    line("3. 甲车按时到场开工 → 作业锁定，重排绝不会移动它")
    svc.arrive("冷链车-甲", f"{DAY}T08:00+08:00")
    svc.start_processing("冷链车-甲", f"{DAY}T08:00+08:00")

    line("4. 乙车迟到 40 分钟（08:40 才到）→ 系统找最近可行槽位")
    result = svc.arrive("冷链车-乙", f"{DAY}T08:40+08:00")
    print(f"  乙车：{result.status}（没有被硬塞进下一时段造成超量）")
    show(svc, "冷链车-甲", "冷链车-乙", "冷链车-丙", "冷链车-丁")

    line("5. 月台 dock-B 突发故障停用 → 只重排未开工的车，甲车不动")
    svc.disable_equipment(
        DAY, DOCK, "dock-B", 520, 600, reason="液压平台故障",
        occurred_at=f"{DAY}T08:45+08:00",
    )
    show(svc, "冷链车-甲", "冷链车-乙", "冷链车-丙", "冷链车-丁")

    line("6. 超时告警（候补等待超时，幂等且重启有效）")
    alarms = svc.pump_alarms(
        DAY, 560, occurred_at=f"{DAY}T09:20+08:00"
    )
    for event in alarms:
        print(f"  告警 {event.aggregate_id}: {event.payload['message']}")

    line("7. 人工插单：丙车走应急通道，必须记录批准人与被延后批次")
    try:
        inserted = svc.manual_insert(
            "冷链车-丙", start_minute=600, approver="值班经理 王磊",
            note="丙车货物温度临界，优先入库",
            occurred_at=f"{DAY}T09:10+08:00",
        )
        rec = svc.load_state().get("冷链车-丙")
        approval = rec.overrides[0]
        print(f"  丙车已插入，批准人：{approval.approver}")
        print(f"  被延后批次：{list(approval.displaced) or '无'}")
    except ScheduleConflict as exc:
        print("  插单被拒绝，真正冲突的约束：")
        for c in exc.conflicts:
            print(f"   - {c.resource_type}/{c.resource_id} {c.constraint} "
                  f"@minute {c.minute}: {c.message}")
    show(svc, "冷链车-甲", "冷链车-乙", "冷链车-丙", "冷链车-丁")

    line("8. 甲车完工释放容量 → 重排候补；丁车窗口已过且未到场，继续候补")
    result = svc.complete("冷链车-甲", f"{DAY}T09:30+08:00")
    print("  窗口仍开放、本次获得提升的候补：", result.promoted or "无")
    print("  审计：", svc.audit_day(DAY)["within_limits"],
          "（月台/预冷间/库位逐分钟均未超上限）")
    show(svc, "冷链车-甲", "冷链车-乙", "冷链车-丙", "冷链车-丁")

    line("9. 同一事件流重放任意次，排程完全相同")
    from coldchain.state import State
    import json

    events = svc.store.load_events()
    snapshot = json.dumps(
        [
            (r.reservation_id, r.state,
             r.plan.to_dict() if r.plan else None, r.waitlist_seq)
            for r in sorted(
                State.fold(events).reservations.values(),
                key=lambda x: x.reservation_id,
            )
        ],
        sort_keys=True, ensure_ascii=False,
    )
    same = all(
        json.dumps(
            [
                (r.reservation_id, r.state,
                 r.plan.to_dict() if r.plan else None, r.waitlist_seq)
                for r in sorted(
                    State.fold(events).reservations.values(),
                    key=lambda x: x.reservation_id,
                )
            ],
            sort_keys=True, ensure_ascii=False,
        )
        == snapshot
        for _ in range(3)
    )
    print("  三次重放结果一致：", same)


if __name__ == "__main__":
    main()
