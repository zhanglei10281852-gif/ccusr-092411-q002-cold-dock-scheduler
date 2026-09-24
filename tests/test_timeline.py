"""分钟级容量时间线测试。"""
import unittest

from coldchain.timeline import Timeline


class TimelineTest(unittest.TestCase):
    def test_discrete_resource_capacity_one(self):
        tl = Timeline()
        tl.add("d1", 0, 30, 1, "a")
        # 相邻半开区间不冲突
        self.assertIsNone(tl.evaluate("d1", 30, 60, 1, 1))
        # 重叠冲突，首次超限分钟即重叠起点
        breach = tl.evaluate("d1", 10, 40, 1, 1)
        self.assertIsNotNone(breach)
        self.assertEqual(breach.first_minute, 10)
        self.assertEqual(breach.load, 2)

    def test_pallet_capacity_and_peak(self):
        tl = Timeline()
        tl.add("pc", 0, 60, 12, "a")
        tl.add("pc", 30, 90, 5, "b")     # 峰值 17
        # 加 3 托：峰值 20，恰好不超
        self.assertIsNone(tl.evaluate("pc", 0, 90, 3, 20))
        # 加 4 托：峰值 21，首次超限在分钟 30（b 进入时）
        breach = tl.evaluate("pc", 0, 90, 4, 20)
        self.assertIsNotNone(breach)
        self.assertEqual(breach.first_minute, 30)
        self.assertEqual(breach.peak, 21)

    def test_same_minute_boundaries_are_half_open(self):
        tl = Timeline()
        tl.add("pc", 0, 30, 20, "a")
        # 恰在分钟 30 进入：a 在 30 已离开，不冲突
        self.assertIsNone(tl.evaluate("pc", 30, 60, 20, 20))
        # 恰在分钟 0 与 a 同时进入：冲突
        self.assertIsNotNone(tl.evaluate("pc", 0, 30, 1, 20))

    def test_remove_key(self):
        tl = Timeline()
        tl.add("pc", 0, 60, 20, "a")
        tl.remove_key("pc", "a")
        self.assertIsNone(tl.evaluate("pc", 0, 60, 20, 20))

    def test_downtime_makes_capacity_zero(self):
        tl = Timeline()
        tl.disable("d1", 100, 200)
        breach = tl.evaluate("d1", 0, 50, 1, 1)
        self.assertIsNone(breach)
        breach = tl.evaluate("d1", 90, 130, 1, 1)
        self.assertIsNotNone(breach)
        self.assertTrue(breach.equipment_down)
        self.assertEqual(breach.first_minute, 100)

    def test_open_ended_usage(self):
        tl = Timeline()
        tl.add("st", 10, 10**9, 8, "a")
        # 再加 2 托到容量 10：不超
        self.assertIsNone(tl.evaluate("st", 1000, 1001, 2, 10))
        # 容量只剩 1 托，申请 2 托：超限
        breach = tl.evaluate("st", 1000, 1001, 2, 9)
        self.assertIsNotNone(breach)
        self.assertEqual(breach.first_minute, 1000)
        self.assertEqual(breach.load, 10)


if __name__ == "__main__":
    unittest.main()
