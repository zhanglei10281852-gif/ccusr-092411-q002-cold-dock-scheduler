# 冷库月台预约协调

冷链车迟到、月台与预冷间同时超量、后续车辆在场外等待——本项目把这套
人工协调沉淀为一个**事件溯源（event-sourced）的分钟级排程服务**。

承运方申报到场区间、温区、托盘数与最长暴露时间后，系统结合月台、预冷间、
低温库库位的分钟级容量给出计划；提前到场、迟到、设备停用、临时加单发生时，
**已开始实体作业的车辆保持原位，只重新编排尚未开始的作业**。

## 它保证什么

| 需求 | 实现位置 |
| --- | --- |
| 到场区间 / 温区 / 托盘数 / 最长暴露时间申报 | `service.request_reservation` |
| 月台（车位）、预冷间与库位（托盘）分钟级容量 | `capacity.Ledger` |
| 跨午夜窗口按市场时区计算 | `timeutil.TimeService`（营业日分钟坐标，最长 48h） |
| 迟到 / 提前到场 / 设备停用 / 临时加单重排 | `service.arrive` / `disable_equipment` / `manual_insert` |
| 已开工车辆不动，只排未开始的作业 | `state.Reservation.immovable` + `scheduler.plan_day` |
| 人工插单记录**批准人**与**被延后批次** | `reservation.overridden` 事件 |
| 两个调度员同时接受候补也不突破任一上限 | `store.EventStore` 乐观版本号 + 单事务裁决 |
| 一次拒绝 / 改期返回**真正冲突的约束** | `ScheduleConflict.conflicts`（含容量/停用/暴露/窗口/温区与占用批次） |
| 候补序号、超时告警重启后继续有效 | 全部状态由事件折叠（SQLite 持久化） |
| 同一段事件重放任意次排程相同 | 确定性折叠：提交顺序、确定性事件 ID、无墙钟/随机依赖 |

## 核心设计

- **事件溯源**：状态完全由事件流折叠（`state.State.fold`）。事件只追加、
  不修改；事件按**提交顺序（rowid）**而非发生时间折叠，因此事后补录
  （如 11:00 录入 07:00 的设备停用）不会颠倒跨聚合因果。
- **确定性排程**（`scheduler.Scheduler`）：候选顺序固定
  （插单 → 已排程 → 候补按序号 → 新申报按申报时间），资源按 ID 升序、
  开始分钟逐分钟前探取最早可行解。月台 30 分、预冷 60 分、入库 20 分
  （`StageProfile` 可调）。
- **乐观并发**：每个聚合维护单调序号，提交时在 `begin immediate`
  事务内校验期望版本；两个调度员竞争同一释放槽位时，只有一个事务成功，
  另一个收到 `StaleVersion`，重新加载后重试。
- **冲突归因**：拒绝/改期失败时返回具体资源、分钟、上限、所需量、剩余量
  以及真正占用该分钟的批次（`BlockingLoad`）。
- **暴露时间硬约束**：从实际到场（未到场按计划靠台时刻）到进入预冷间
  不得超过承运申报的最长暴露时间；到场后确实无可行槽位时保持
  `arrived` 状态并产生 `exposure.timeout` 告警，而不是静默塞进超时槽位。

## 目录

```
coldchain/
  timeutil.py    市场时区、跨午夜窗口、分钟坐标
  models.py      资源类型、作业 Operation、计划 Plan、冲突 Conflict
  events.py      不可变事件与确定性序列化/ID 派生
  state.py       事件折叠（纯函数，重放确定性的基础）
  capacity.py    分钟级容量台账与停用区间
  scheduler.py   确定性排程引擎
  store.py       SQLite 事件库（持久化、乐观并发）
  service.py     应用服务（申报/到场/停用/插单/候补/告警/审计）
domain/contract.json   领域合同（实体、状态、事件、策略）
examples/events.json   含“事后补录”的样例事件流
tools/
  validate_contract.py 离线校验合同与样例
  demo_incident.py      原始事故复盘演示
tests/                 38 个 unittest 用例
```

## 快速开始

```bash
# 编译检查
python3 -m compileall -q .

# 单元测试
python3 -m unittest discover -s tests -v

# 领域资料校验
python3 tools/validate_contract.py

# 事故复盘演示（迟到车 / 候补 / 设备停用 / 插单 / 完工释放 / 确定性重放）
python3 tools/demo_incident.py
```

仅依赖 Python 3.11+ 标准库（含 SQLite、zoneinfo），无需启动额外服务。

## 最小用法

```python
from coldchain import ColdChainService
from coldchain.capacity import Resource
from coldchain.models import DOCK, PRECOOL, STORAGE
from coldchain.store import EventStore
from coldchain.timeutil import TimeService

svc = ColdChainService(
    EventStore("coldchain.db"),   # 或 ":memory:"
    resources=[
        Resource(DOCK, "dock-1", "*", 1),
        Resource(PRECOOL, "pc-f", "frozen", 20),
        Resource(STORAGE, "st-f", "frozen", 100),
    ],
    time=TimeService("Asia/Shanghai"),
)

# 申报：2026-09-25，08:00–08:30 窗口，冻品 10 托，最长暴露 90 分钟
result = svc.request_reservation(
    "res-001", carrier="某物流", vehicle_id="沪A-123", zone="frozen",
    pallets=10, day="2026-09-25",
    window_start_minute=480, window_end_minute=510,
    max_exposure_minutes=90, timeout_after_minutes=30,
)
print(result.status)            # scheduled / waitlisted
print(result.plan.operations)   # dock → precool → storage 三段作业

# 迟到 40 分钟到场 → 排入最近可行槽位，已开工车辆不动
svc.arrive("res-001", "2026-09-25T08:40:00+08:00")

# 设备停用 → 只重排未开始的作业
svc.disable_equipment("2026-09-25", DOCK, "dock-1", 600, 660, reason="维修")

# 人工插单（必须有批准人；返回被延后批次）
svc.manual_insert("res-009", start_minute=660, approver="王磊", note="应急")

# 逐分钟审计任一资源是否越界
print(svc.audit_day("2026-09-25")["within_limits"])
```

重启进程后用同一数据库文件构造 `ColdChainService`，候补序号、告警、
插单批准记录与全部计划自动恢复；对同一事件流反复折叠得到逐字节一致的结果。
