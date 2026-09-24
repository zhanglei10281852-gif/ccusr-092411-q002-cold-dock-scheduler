"""时区与跨午夜窗口测试。"""
from __future__ import annotations

import unittest
from datetime import date

from coldchain.timeutil import TimeService


class TimeServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ts = TimeService("Asia/Shanghai")

    def test_window_across_midnight(self) -> None:
        # 1400 分 = 23:20，1500 分 = 次日 01:00
        start, end = self.ts.window_minutes("2026-09-25", 1400, 1500)
        self.assertEqual(start.isoformat(), "2026-09-25T23:20:00+08:00")
        self.assertEqual(end.isoformat(), "2026-09-26T01:00:00+08:00")

    def test_utc_input_interpreted_in_market_zone(self) -> None:
        dt = self.ts.parse("2026-09-25T16:00:00Z") # UTC = 次日 00:00 +08
        self.assertEqual(dt.isoformat(), "2026-09-26T00:00:00+08:00")
        self.assertEqual(self.ts.business_day_of(dt), date(2026, 9, 26))

    def test_minute_roundtrip(self) -> None:
        start, _ = self.ts.day_bounds("2026-09-25")
        at = self.ts.at_minute("2026-09-25", 600)
        self.assertEqual(self.ts.minutes_between(start, at), 600)

    def test_window_over_48h_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.ts.window_minutes("2026-09-25", 0, 3000)

    def test_business_day_uses_market_timezone(self) -> None:
        # 23:30 +08 仍属当日营业日
        self.assertEqual(
            self.ts.business_day_of("2026-09-25T23:30:00+08:00"),
            date(2026, 9, 25),
        )
        # 同一时刻 UTC 表达也按市场时区归日
        self.assertEqual(
            self.ts.business_day_of("2026-09-25T15:30:00Z"),
            date(2026, 9, 25),
        )


if __name__ == "__main__":
    unittest.main()
