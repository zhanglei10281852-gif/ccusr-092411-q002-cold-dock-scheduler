"""市场时区与分钟级时间换算。

所有排程以「市场营业日的分钟序号」为内部坐标：营业日从市场时区
00:00 起算，窗口允许跨越午夜（最大 48 小时），因此分钟序号可能大于 1440。
对外仍然使用带时区的 ISO 8601 字符串。
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

MINUTES_PER_DAY = 1440
MAX_WINDOW_MINUTES = 2 * MINUTES_PER_DAY


class TimeService:
    """以市场时区解释所有到场窗口。"""

    def __init__(self, market_tz: str = "Asia/Shanghai") -> None:
        self.tz = ZoneInfo(market_tz)

    # -- 解析与渲染 -----------------------------------------------------

    def parse(self, value: str | datetime) -> datetime:
        """解析带时区的时间；若为 naive datetime 则锚定到市场时区。"""
        if isinstance(value, datetime):
            dt = value
        else:
            dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=self.tz)
        return dt.astimezone(self.tz)

    def format(self, dt: datetime) -> str:
        return self.parse(dt).isoformat(timespec="minutes")

    def local(self, dt: datetime) -> datetime:
        return self.parse(dt)

    def day_bounds(self, day: date | str) -> tuple[datetime, datetime]:
        """某营业日 [00:00, 次日 00:00) 的市场时区边界。"""
        if isinstance(day, str):
            day = date.fromisoformat(day)
        start = datetime.combine(day, time.min, self.tz)
        return start, start + timedelta(days=1)

    # -- 分钟坐标 -------------------------------------------------------

    def minutes_between(self, start: datetime, end: datetime) -> int:
        delta = self.parse(end) - self.parse(start)
        return int(delta.total_seconds() // 60)

    def at_minute(self, day: date | str, minute: int) -> datetime:
        """营业日内第 minute 分钟对应的绝对时间（minute 可以 >= 1440）。"""
        start, _ = self.day_bounds(day)
        return start + timedelta(minutes=minute)

    def window_minutes(
        self, day: date | str, start_minute: int, end_minute: int
    ) -> tuple[datetime, datetime]:
        if end_minute <= start_minute:
            raise ValueError("窗口结束分钟必须晚于开始分钟")
        if end_minute - start_minute > MAX_WINDOW_MINUTES:
            raise ValueError("预约窗口跨度不能超过 48 小时")
        return self.at_minute(day, start_minute), self.at_minute(day, end_minute)

    def business_day_of(self, dt: str | datetime) -> date:
        """事件所属营业日：市场时区 00:00–24:00（跨午夜作业归属当日）。"""
        return self.parse(dt).date()

    def now(self) -> datetime:
        return datetime.now(self.tz)

    def epoch_minute(self, dt: datetime) -> int:
        """全局绝对分钟序号，用于跨日事件排序与持久化比较。"""
        return int(self.parse(dt).timestamp() // 60)
