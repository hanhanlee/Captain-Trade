"""
modules.n_pattern_detector 單元測試

執行：
  python -m unittest tests.test_n_pattern_detector
"""
from __future__ import annotations

import unittest
from datetime import date, timedelta

import pandas as pd

from modules.n_pattern_detector import (
    NPattern,
    find_abc_points,
    is_breakout,
)


def _ohlc_from_lows_highs(
    values: list[tuple[float, float]],
    volumes: list[float] | None = None,
) -> pd.DataFrame:
    """
    依 (low, high) 序列建構 OHLC 測試資料：
      - open/close 取 low/high 的中點，避免被當作其他訊號
      - date 從 2026-01-01 開始連續遞增
      - volumes：若提供則需與 values 等長，否則所有列填 1000
    """
    if volumes is not None and len(volumes) != len(values):
        raise ValueError("volumes 長度需與 values 相同")
    rows = []
    start = date(2026, 1, 1)
    for i, (lo, hi) in enumerate(values):
        rows.append(
            {
                "date": (start + timedelta(days=i)).isoformat(),
                "open": (lo + hi) / 2,
                "high": hi,
                "low": lo,
                "close": (lo + hi) / 2,
                "volume": volumes[i] if volumes is not None else 1000,
            }
        )
    return pd.DataFrame(rows)


def _flat_segment(low: float, high: float, n: int) -> list[tuple[float, float]]:
    """產生 n 根同一區間的 K，作為填充。"""
    return [(low, high)] * n


class FindAbcPointsTest(unittest.TestCase):
    def test_textbook_n_pattern(self):
        """標準 N 字底：明顯下跌 → A → 拉到 B → 回測到 C（不破 A）→ 等待突破。"""
        bars: list[tuple[float, float]] = []
        # 下跌段：120 → 102（跌 15%）
        for i in range(8):
            top = 120 - i * 2.4
            bars.append((top - 1.5, top))
        # A 點：96 是最低（前後各 3 根都更高）
        bars.append((101, 103))
        bars.append((100, 102))
        bars.append((99, 101))
        bars.append((96, 99))            # A
        bars.append((99, 102))
        bars.append((101, 105))
        bars.append((104, 108))
        # B 點：120 是最高
        bars.append((108, 113))
        bars.append((112, 117))
        bars.append((116, 120))          # B
        bars.append((114, 118))
        bars.append((110, 115))
        bars.append((105, 111))
        # C 點：100（>= A=96，回測 (120-96)*0.83 ≈ 80%）
        bars.append((101, 106))
        bars.append((100, 104))          # C
        bars.append((103, 107))
        bars.append((105, 110))
        bars.append((108, 113))

        df = _ohlc_from_lows_highs(bars)
        pat = find_abc_points(df, fractal_k=3)

        self.assertIsNotNone(pat, "標準 N 字底應該被偵測到")
        self.assertAlmostEqual(pat.A.price, 96.0)
        self.assertAlmostEqual(pat.B.price, 120.0)
        self.assertAlmostEqual(pat.C.price, 100.0)
        self.assertGreaterEqual(pat.C.price, pat.A.price)
        self.assertGreater(pat.B.price, pat.A.price)
        self.assertGreater(pat.b_rise_pct, 5.0)
        self.assertGreater(pat.c_retrace_pct, 30.0)

    def test_c_below_a_should_fail(self):
        """C 點跌破 A 點 → 型態失敗。"""
        bars: list[tuple[float, float]] = []
        for i in range(8):
            top = 120 - i * 2.4
            bars.append((top - 1.5, top))
        bars.append((101, 103))
        bars.append((100, 102))
        bars.append((99, 101))
        bars.append((96, 99))            # A=96
        bars.append((99, 102))
        bars.append((103, 106))
        bars.append((107, 111))
        bars.append((112, 116))
        bars.append((115, 119))
        bars.append((118, 122))          # B=122
        bars.append((116, 120))
        bars.append((110, 115))
        bars.append((100, 105))
        bars.append((92, 97))            # C=92 < A=96
        bars.append((95, 100))
        bars.append((98, 103))
        bars.append((101, 106))
        bars.append((104, 109))

        df = _ohlc_from_lows_highs(bars)
        pat = find_abc_points(df, fractal_k=3)
        self.assertIsNone(pat, "C 跌破 A 不應被視為有效 N 字底")

    def test_b_rise_too_small(self):
        """B 漲幅不足 min_b_rise_pct → 過濾掉（避免抓到鋸齒整理）。"""
        bars: list[tuple[float, float]] = []
        for i in range(8):
            top = 120 - i * 2.4
            bars.append((top - 1.5, top))
        # A=96, B 只到 99（漲 3.1%），C=97
        bars.append((101, 103))
        bars.append((100, 102))
        bars.append((99, 101))
        bars.append((96, 99))            # A
        bars.append((97, 99.5))
        bars.append((98, 99.8))
        bars.append((98.5, 99))
        bars.append((98.8, 99))
        bars.append((98.5, 99))          # B 候選 ~99
        bars.append((98, 98.5))
        bars.append((97.5, 98))
        bars.append((97, 97.8))          # C 候選
        bars.append((97.5, 98))
        bars.append((98, 98.5))
        bars.append((98.5, 99))
        bars.append((99, 99.5))

        df = _ohlc_from_lows_highs(bars)
        pat = find_abc_points(df, fractal_k=3, min_b_rise_pct=5.0)
        self.assertIsNone(pat, "B 漲幅不足 5% 不應被視為有效")

    def test_no_prior_downtrend(self):
        """A 之前若沒有下跌段（例如一路橫盤後拉一個小坑），require_prior_downtrend=True 應拒絕。"""
        bars: list[tuple[float, float]] = []
        # 橫盤
        bars += _flat_segment(99.5, 100.5, 8)
        # A 假象低點
        bars.append((99, 100))
        bars.append((98.5, 99.5))
        bars.append((98, 99))
        bars.append((96, 98))            # A
        bars.append((98, 100))
        bars.append((101, 103))
        bars.append((104, 107))
        # B
        bars.append((108, 112))
        bars.append((110, 115))
        bars.append((113, 118))          # B
        bars.append((110, 114))
        bars.append((105, 110))
        bars.append((100, 105))
        # C
        bars.append((98, 102))
        bars.append((97, 100))           # C
        bars.append((98, 102))
        bars.append((101, 105))
        bars.append((104, 108))

        df = _ohlc_from_lows_highs(bars)

        with_downtrend = find_abc_points(df, fractal_k=3, require_prior_downtrend=True)
        self.assertIsNone(with_downtrend, "缺少前置下跌段時應拒絕")

        without_downtrend = find_abc_points(df, fractal_k=3, require_prior_downtrend=False)
        self.assertIsNotNone(without_downtrend, "關閉下跌段條件時應能找到 A/B/C")

    def test_retrace_too_shallow(self):
        """C 回測太淺（B-A 中只回測 < 30%） → 過濾掉。"""
        bars: list[tuple[float, float]] = []
        for i in range(8):
            top = 120 - i * 2.4
            bars.append((top - 1.5, top))
        # A=96, B=120, C 只回到 117（回測 (120-117)/(120-96)=12.5%）
        bars.append((101, 103))
        bars.append((100, 102))
        bars.append((99, 101))
        bars.append((96, 99))            # A
        bars.append((99, 103))
        bars.append((103, 107))
        bars.append((107, 111))
        bars.append((111, 115))
        bars.append((115, 119))
        bars.append((116, 120))          # B
        bars.append((118, 119.5))
        bars.append((117.5, 119))
        bars.append((117, 118.5))        # C 候選 ~117
        bars.append((117.5, 119))
        bars.append((118, 119.5))
        bars.append((118.5, 120))

        df = _ohlc_from_lows_highs(bars)
        pat = find_abc_points(df, fractal_k=3, min_c_retrace_pct=30.0)
        self.assertIsNone(pat, "C 回測幅度不足 30% 不應視為有效")

    def test_returns_most_recent_pattern(self):
        """資料中若存在多組可能的 A/B/C，應回傳最新（最靠右）的一組。"""
        bars: list[tuple[float, float]] = []
        # 第一組：下跌 → A1=80 → B1=95 → C1=82
        for i in range(8):
            top = 100 - i * 1.8
            bars.append((top - 1, top))
        bars.append((84, 86))
        bars.append((82, 85))
        bars.append((81, 84))
        bars.append((80, 83))            # A1
        bars.append((82, 85))
        bars.append((85, 89))
        bars.append((88, 92))
        bars.append((91, 94))
        bars.append((93, 95))            # B1
        bars.append((90, 93))
        bars.append((86, 90))
        bars.append((83, 87))            # C1
        # 接著再下跌 → A2=70 → B2=90 → C2=72
        bars.append((80, 84))
        bars.append((76, 80))
        bars.append((73, 76))
        bars.append((71, 74))
        bars.append((70, 73))            # A2
        bars.append((72, 76))
        bars.append((75, 80))
        bars.append((79, 84))
        bars.append((83, 88))
        bars.append((87, 90))            # B2
        bars.append((84, 88))
        bars.append((78, 84))
        bars.append((74, 80))
        bars.append((72, 76))            # C2
        bars.append((74, 78))
        bars.append((76, 80))
        bars.append((78, 82))

        df = _ohlc_from_lows_highs(bars)
        pat = find_abc_points(df, lookback=80, fractal_k=3)
        self.assertIsNotNone(pat)
        # 最新的一組應該是 A2/B2/C2 附近，C 不應該是 C1=83
        self.assertLess(pat.C.price, 80, "應回傳較新的那組 N 字底（C ≈ 72）")

    def test_empty_or_too_short(self):
        self.assertIsNone(find_abc_points(pd.DataFrame()))
        df = _ohlc_from_lows_highs([(100, 101)] * 5)
        self.assertIsNone(find_abc_points(df, fractal_k=3))

    def test_volume_profile_matches_definition(self):
        """A→B 量增、B→C 量縮時，VolumeProfile 應標記兩個布林為 True。"""
        bars: list[tuple[float, float]] = []
        vols: list[float] = []
        # 下跌段（A 之前）量平
        for i in range(8):
            top = 120 - i * 2.4
            bars.append((top - 1.5, top))
            vols.append(1000)
        bars.append((101, 103)); vols.append(1000)
        bars.append((100, 102)); vols.append(1000)
        bars.append((99, 101));  vols.append(1000)
        bars.append((96, 99));   vols.append(1000)   # A
        # A→B 量增
        bars.append((99, 102));  vols.append(2500)
        bars.append((101, 105)); vols.append(3000)
        bars.append((104, 108)); vols.append(3500)
        bars.append((108, 113)); vols.append(4000)
        bars.append((112, 117)); vols.append(4200)
        bars.append((116, 120)); vols.append(4500)   # B
        # B→C 量縮
        bars.append((114, 118)); vols.append(2000)
        bars.append((110, 115)); vols.append(1500)
        bars.append((105, 111)); vols.append(1200)
        bars.append((101, 106)); vols.append(1000)
        bars.append((100, 104)); vols.append(900)    # C
        bars.append((103, 107)); vols.append(1000)
        bars.append((105, 110)); vols.append(1100)
        bars.append((108, 113)); vols.append(1200)

        df = _ohlc_from_lows_highs(bars, volumes=vols)
        pat = find_abc_points(df, fractal_k=3)

        self.assertIsNotNone(pat)
        self.assertIsNotNone(pat.volume)
        self.assertTrue(pat.volume.a_to_b_volume_increase, "A→B 應偵測為量增")
        self.assertTrue(pat.volume.b_to_c_volume_decrease, "B→C 應偵測為量縮")
        self.assertGreater(pat.volume.avg_a_to_b, pat.volume.avg_b_to_c)
        self.assertGreater(pat.volume.avg_a_to_b, pat.volume.pre_a_avg)

    def test_volume_profile_negative_signals(self):
        """A→B 量沒增（甚至縮）、B→C 量沒縮（反而增）→ 兩個布林為 False。"""
        bars: list[tuple[float, float]] = []
        vols: list[float] = []
        for i in range(8):
            top = 120 - i * 2.4
            bars.append((top - 1.5, top))
            vols.append(5000)        # 下跌時量大（恐慌賣壓）
        bars.append((101, 103)); vols.append(5000)
        bars.append((100, 102)); vols.append(5000)
        bars.append((99, 101));  vols.append(5000)
        bars.append((96, 99));   vols.append(5000)   # A
        # A→B 量沒增（反而較 A 之前縮）
        bars.append((99, 102));  vols.append(2000)
        bars.append((101, 105)); vols.append(2000)
        bars.append((104, 108)); vols.append(2000)
        bars.append((108, 113)); vols.append(2000)
        bars.append((112, 117)); vols.append(2000)
        bars.append((116, 120)); vols.append(2000)   # B
        # B→C 量反而增
        bars.append((114, 118)); vols.append(3500)
        bars.append((110, 115)); vols.append(4000)
        bars.append((105, 111)); vols.append(4500)
        bars.append((101, 106)); vols.append(4500)
        bars.append((100, 104)); vols.append(5000)   # C
        bars.append((103, 107)); vols.append(2000)
        bars.append((105, 110)); vols.append(2000)
        bars.append((108, 113)); vols.append(2000)

        df = _ohlc_from_lows_highs(bars, volumes=vols)
        pat = find_abc_points(df, fractal_k=3)

        self.assertIsNotNone(pat)
        self.assertIsNotNone(pat.volume)
        self.assertFalse(pat.volume.a_to_b_volume_increase)
        self.assertFalse(pat.volume.b_to_c_volume_decrease)

    def test_volume_profile_missing_column(self):
        """資料無 volume 欄位時，volume 屬性為 None 但 A/B/C 仍應正常偵測。"""
        bars: list[tuple[float, float]] = []
        for i in range(8):
            top = 120 - i * 2.4
            bars.append((top - 1.5, top))
        bars.append((101, 103))
        bars.append((100, 102))
        bars.append((99, 101))
        bars.append((96, 99))
        bars.append((99, 102))
        bars.append((101, 105))
        bars.append((104, 108))
        bars.append((108, 113))
        bars.append((112, 117))
        bars.append((116, 120))
        bars.append((114, 118))
        bars.append((110, 115))
        bars.append((105, 111))
        bars.append((101, 106))
        bars.append((100, 104))
        bars.append((103, 107))
        bars.append((105, 110))
        bars.append((108, 113))

        df = _ohlc_from_lows_highs(bars).drop(columns=["volume"])
        pat = find_abc_points(df, fractal_k=3)
        self.assertIsNotNone(pat)
        self.assertIsNone(pat.volume, "缺 volume 時應回 None，不應拋例外")

    def test_accepts_finmind_max_min_columns(self):
        """price_cache 真實欄位是 max/min（FinMind 原命名），detector 應自動對齊。"""
        bars: list[tuple[float, float]] = []
        for i in range(8):
            top = 120 - i * 2.4
            bars.append((top - 1.5, top))
        bars.append((101, 103))
        bars.append((100, 102))
        bars.append((99, 101))
        bars.append((96, 99))
        bars.append((99, 102))
        bars.append((101, 105))
        bars.append((104, 108))
        bars.append((108, 113))
        bars.append((112, 117))
        bars.append((116, 120))
        bars.append((114, 118))
        bars.append((110, 115))
        bars.append((105, 111))
        bars.append((101, 106))
        bars.append((100, 104))
        bars.append((103, 107))
        bars.append((105, 110))
        bars.append((108, 113))

        df = _ohlc_from_lows_highs(bars).rename(columns={"high": "max", "low": "min"})
        pat = find_abc_points(df, fractal_k=3)
        self.assertIsNotNone(pat, "max/min 命名應被自動接受")
        self.assertAlmostEqual(pat.A.price, 96.0)
        self.assertAlmostEqual(pat.B.price, 120.0)
        self.assertAlmostEqual(pat.C.price, 100.0)


class IsBreakoutTest(unittest.TestCase):
    def test_strict_greater_than(self):
        self.assertTrue(is_breakout(120.5, 120.0))
        self.assertFalse(is_breakout(120.0, 120.0), "等於 B 不算突破")
        self.assertFalse(is_breakout(119.99, 120.0))

    def test_handles_none_and_invalid(self):
        self.assertFalse(is_breakout(None, 120.0))
        self.assertFalse(is_breakout(120.0, None))
        self.assertFalse(is_breakout(None, None))
        self.assertFalse(is_breakout("abc", 120.0))


if __name__ == "__main__":
    unittest.main()
