"""开场事故场景回放：冷链车迟到 40 分钟。

运行：
    python3 tools/demo_scenario.py

纯标准库；使用内存事件库，打印每一步的排程变化与真实冲突约束。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coldchain.catalog import Catalog, TimingPolicy
from coldchain.models import Declaration
from coldchain.service import SchedulingService
from coldchain.store import EventStore
from coldchain.timepolicy import iso, market_zone, to_minute

TZ = market_zone("Asia/Shanghai")


def minute(value: str) -> int:
    return to_minute(value, TZ)


def show(svc: SchedulingService, title: str) -> None:
    st = svc.snapshot()
    print(f"\n=== {title} ===")
    for rid in sorted(st.statuses):
        status = st.statuses[rid]
        plan = st.plans.get(rid)
        pos = st.waitlist_position(rid)
        if plan:
            print(f"  {rid:10s} {status:10s} 月台 {plan.dock_id} "
                  f"{iso(plan.dock_start, TZ)}~{iso(plan.dock_end, TZ)} "
                  f"预冷 {plan.precool_id}  库 {plan.storage_id}")
        elif pos:
            print(f"  {rid:10s} {status:10s} 候补序号 {pos}（场外等待）")
        else:
            print(f"  {rid:10s} {status:10s}")


def main() -> None:
    # 两门位；冷冻预冷间仅 20 托/分钟，冷冻库 100 托
    catalog = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
    catalog.add_dock("dock-1", "1 号月台")
    catalog.add_dock("dock-2", "2 号月台")
    catalog.add_precool_room("pc-f", "frozen", 20, "冷冻预冷间")
    catalog.add_storage_room("st-f", "frozen", 100, "冷冻库")
    svc = SchedulingService(EventStore(":memory:"), catalog)

    window = ("2026-09-23T08:00", "2026-09-23T12:00")
    declarations = [
        Declaration("res-late", "承运冷链", "车-迟到", "frozen", 12,
                    minute(window[0]), minute(window[1]), 90),
        Declaration("res-a", "承运冷链", "车-A", "frozen", 12,
                    minute(window[0]), minute(window[1]), 90),
        Declaration("res-b", "承运冷链", "车-B", "frozen", 12,
                    minute(window[0]), minute(window[1]), 90),
    ]
    for i, decl in enumerate(declarations):
        result = svc.request_reservation(
            decl, now=f"2026-09-22T10:{i:02d}")
        if result.diverted_to_waitlist:
            print(f"{decl.reservation_id} 进入候补")
    show(svc, "前一日 10:00 排定计划（08:00 / 09:00 / 10:00 串行）")

    # 首车迟到 40 分钟，值班员登记实际到场
    svc.report_arrival("res-late", "2026-09-23T08:40",
                       now="2026-09-23T08:40")
    show(svc, "08:40 首车迟到 40 分钟到场（只重排未开始作业）")

    # 08:45 临时加单：18 托疫苗，窗口只到 09:30，预冷间装不下 -> 真实冲突
    urgent = Declaration("res-vax", "承运医药", "车-疫苗", "frozen", 18,
                         minute("2026-09-23T08:45"),
                         minute("2026-09-23T09:30"), 60)
    result = svc.manual_insert(urgent, approver="王值班",
                               reason="疫苗紧急放行",
                               now="2026-09-23T08:45")
    print("\n=== 08:45 人工插单（18 托，窗口 08:45~09:30）===")
    if not result.accepted:
        for v in result.violations:
            print(f"  冲突：[{v.code}] {v.resource_kind}/{v.resource_id} "
                  f"{v.message}")
    else:
        show(svc, "插单成功")

    # 候补超时告警
    svc.sweep(now="2026-09-23T08:46")
    st = svc.snapshot()
    alerts = [a for a in st.alerts.values() if a["active"]]
    if alerts:
        print("\n=== 活动告警 ===")
        for a in alerts:
            print(f"  {a['code']}: {a['message']}（{a['reservation_id']}）")

    print(f"\n事件总数：{len(svc.store.load())}（重启后重放可完整还原）")


if __name__ == "__main__":
    main()
