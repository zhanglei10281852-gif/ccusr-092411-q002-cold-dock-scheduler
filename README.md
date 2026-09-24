# 冷库月台预约协调

面向冷链仓的分钟级预约排程与扰动重排服务。承运方申报到场区间、温区、托盘数与
最长暴露时间后，系统结合月台、预冷间与低温库的分钟级容量给出"门位→预冷→入库"
的完整计划；提前/迟到、设备停用、临时加单发生时，**已经进场的车辆保持原位，
只重新编排尚未开始的作业**。

## 领域规则

- **资源与容量**
  - 月台：离散门位，同一分钟互斥（容量 1）；
  - 预冷间：按托盘计的分钟级容量，货物停留固定时长；
  - 低温库：按温区划分库位，托盘自入库持续占用至出库，卸货完成不释放库位。
- **到场窗口**：半开区间 `[start, end)`；跨午夜窗口按市场时区（默认
  `Asia/Shanghai`）的当地自然日计算，朴素时间按市场时区补全。
- **扰动处理**：已到场且已有方案 / 作业中 / 已完成的车辆冻结；只撤装并重排
  尚未开始的作业，已排定作业优先保住原时刻，被挤掉的批次回候补并记录原因。
- **候补**：按进入顺序编号（FIFO），只能由调度员显式接纳或周期巡检放行；
  接纳严格按序号，不能越过更早的候补；序号与超时告警来自事件日志，重启后仍在。
- **人工插单**：必须记录批准人、原因与被延后批次；插单同样不得突破任何容量，
  无容量时拒绝并返回真实冲突。
- **并发**：每个命令以读取时的事件日志版本号乐观提交，两个调度员同时接纳
  恰好一个成功、另一个收到版本冲突并重试，资源上限绝不被突破。
- **确定性**：当前时间由命令显式注入；事件标识由内容与序号确定性派生，
  同一段事件重放任意次得到完全相同的状态与排程。

## 模块

| 模块 | 职责 |
| --- | --- |
| `coldchain/timepolicy.py` | 市场时区、epoch minute 时间轴、跨午夜窗口 |
| `coldchain/catalog.py` | 月台/预冷间/低温库目录与作业时长策略 |
| `coldchain/models.py` | 申报 `Declaration`、方案 `Plan`、冲突 `Violation` |
| `coldchain/events.py` | 全部事件类型与事件结构 |
| `coldchain/timeline.py` | 半开区间的分钟级容量时间线与精确冲突定位 |
| `coldchain/state.py` | 由事件确定性折叠出的调度状态 |
| `coldchain/scheduler.py` | 纯函数排程/重排引擎（锚点、FIFO、硬拒绝） |
| `coldchain/store.py` | SQLite 事件库（乐观锁、幂等键、WAL） |
| `coldchain/service.py` | 命令入口：申报、到场、接纳、插单、停用、改期… |

## 快速开始

```python
from coldchain.catalog import Catalog, TimingPolicy
from coldchain.models import Declaration
from coldchain.service import SchedulingService
from coldchain.store import EventStore
from coldchain.timepolicy import market_zone, to_minute

tz = market_zone("Asia/Shanghai")
cat = Catalog(TimingPolicy(dock_minutes=30, precool_minutes=60))
cat.add_dock("dock-1").add_dock("dock-2")
cat.add_precool_room("pc-f", "frozen", 20)
cat.add_storage_room("st-f", "frozen", 100)

svc = SchedulingService(EventStore("scheduler.db"), cat)
t = lambda s: to_minute(s, tz)
r = svc.request_reservation(Declaration(
    "res-1", "承运A", "车-1", "frozen", 12,
    t("2026-09-23T08:00"), t("2026-09-23T12:00"), 90),
    now="2026-09-22T10:00")
print(r.accepted, r.plan.dock_id, r.plan.dock_start)

# 迟到 40 分钟：登记实际到场，只重排未开始作业
svc.report_arrival("res-1", "2026-09-23T08:40", now="2026-09-23T08:40")
```

完整事故场景：`python3 tools/demo_scenario.py`。

## 构建

```bash
python3 -m compileall -q .
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：分钟级容量边界、迟到/提前到场、冻结、设备停用恢复、候补 FIFO 与超时告警、
人工插单审计、两调度员并发接纳、改期真实冲突、跨午夜、重启持久化与重放确定性。

## 资料校验

```bash
python3 tools/validate_contract.py
```

所有命令均在项目根目录执行，仅依赖 Python 3.11+ 标准库。
