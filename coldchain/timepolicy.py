"""市场时区与分钟级时间轴。

系统内部一律使用 *epoch minute*（整数）进行容量计算，避免夏令时、跨午夜等
歧义；市场时区只在解析输入、格式化输出以及界定"当地午夜"时使用。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_FALLBACK_OFFSETS = {
    "Asia/Shanghai": 8,
}


def market_zone(name: str) -> timezone | ZoneInfo:
    """返回市场时区；容器缺少 tzdata 时退回固定偏移。"""
    try:
        return ZoneInfo(name)
    except Exception:  # pragma: no cover - 取决于运行环境的 tzdata
        offset = _FALLBACK_OFFSETS.get(name, 0)
        return timezone(timedelta(hours=offset), name=name)


def parse_dt(value: datetime | str, tz: timezone | ZoneInfo) -> datetime:
    """解析 ISO 8601 字符串；朴素时间按市场时区补全。"""
    if isinstance(value, str):
        dt = datetime.fromisoformat(value)
    else:
        dt = value
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def to_minute(value: datetime | str | int, tz: timezone | ZoneInfo) -> int:
    """把时间转换为 epoch minute（向下取整到分钟）。"""
    if isinstance(value, int):
        return value
    dt = parse_dt(value, tz)
    return int(dt.timestamp() // 60)


def from_minute(minute: int, tz: timezone | ZoneInfo) -> datetime:
    return datetime.fromtimestamp(minute * 60, tz)


def iso(minute: int, tz: timezone | ZoneInfo) -> str:
    """分钟时间轴坐标 -> 带时区偏移的 ISO 8601 字符串。"""
    return from_minute(minute, tz).isoformat(timespec="minutes")


def overnight_window(
    local_date: str,
    start_clock: str,
    end_clock: str,
    tz: timezone | ZoneInfo,
) -> tuple[int, int]:
    """按市场时区构造到场窗口，允许跨午夜。

    例如 ``2026-09-23`` 的 ``23:40``–``00:20`` 是同一夜的 40 分钟窗口，
    结束时钟小于等于开始时钟时自动顺延到下一个当地自然日。
    """
    day = datetime.fromisoformat(local_date)
    sh, sm = (int(x) for x in start_clock.split(":"))
    eh, em = (int(x) for x in end_clock.split(":"))
    start = day.replace(hour=sh, minute=sm, tzinfo=tz)
    end = day.replace(hour=eh, minute=em, tzinfo=tz)
    if end <= start:
        end += timedelta(days=1)
    return to_minute(start, tz), to_minute(end, tz)
