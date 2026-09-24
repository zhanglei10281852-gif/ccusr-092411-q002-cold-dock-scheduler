"""冷库预约协调领域服务。

仅依赖 Python 标准库。模块划分：

- ``timepolicy``：市场时区、分钟时间轴、跨午夜窗口
- ``catalog``：月台 / 预冷间 / 低温库目录与作业时长策略
- ``models``：申报、计划、冲突、决策等值对象
- ``events``：事件类型与事件结构
- ``timeline``：分钟级容量时间线
- ``state``：由事件折叠出的调度状态
- ``scheduler``：纯函数式排程与重排引擎
- ``store``：SQLite 事件库（并发安全、可重启）
- ``service``：对外应用服务（命令入口）
"""

from __future__ import annotations

__version__ = "1.0.0"
