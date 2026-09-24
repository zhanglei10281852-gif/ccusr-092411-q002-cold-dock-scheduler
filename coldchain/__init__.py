"""冷库预约协调领域服务。

模块划分：

- timeutil   市场时区与跨午夜窗口的分钟级换算
- models     资源、作业、计划、冲突等领域结构
- events     事件类型与序列化
- state      事件折叠（纯函数，重放确定性的基础）
- capacity   分钟级资源容量台账
- scheduler  确定性排程引擎
- store      SQLite 事件库（持久化、乐观并发）
- service    应用服务：承运申报、到场、插单、候补、告警
"""

from .models import Conflict, Operation, Plan
from .service import ColdChainService, InvalidState, ScheduleConflict, StaleVersion
from .store import EventStore, DuplicateEvent

__all__ = [
    "ColdChainService",
    "Conflict",
    "DuplicateEvent",
    "InvalidState",
    "Operation",
    "Plan",
    "ScheduleConflict",
    "StaleVersion",
    "EventStore",
]
