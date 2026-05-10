"""
modules.n_pattern_watchlist_builder.build_from_data 單元測試（不打 DB / 不打 API）

執行：
  python -m unittest tests.test_n_pattern_watchlist_builder
"""
from __future__ import annotations

import unittest
from datetime import date, timedelta

import pandas as pd

from modules.n_pattern_watchlist_builder import build_from_data


def _make_n_pattern_df(
    *,
    end_date: date,
    pattern_offset_days: int = 0,
    last_close: float | None = None,
    volume_a_to_b: float = 3000,
    volume_b_to_c: float = 1000,
    volume_pre_a: float = 1000,
) -> pd.DataFrame:
    """
    建構一個合法 N 字底的日 K：
      bars 為固定 33 根。bar_idx 對應如下：
        0..7   下跌段（量＝volume_pre_a）
        8..11  趨近 A 點   （量＝volume_pre_a）
        12..17 A→B 拉升    （量＝volume_a_to_b）
        18..23 B→C 回測    （量＝volume_b_to_c）
        24..32 C 之後（用來決定現價）
      A=96, B=120, C=100。
    pattern_offset_days：把整段資料往前推幾天（讓 C 變舊，測 max_c_age_days 過濾）
    last_close：最後一根 close 強制覆寫，用來測 distance_to_b_pct 過濾
    """
    bars: list[tuple[float, float, float]] = []   # (low, high, volume)
    for i in range(8):
        top = 120 - i * 2.4
        bars.append((top - 1.5, top, volume_pre_a))
    bars.append((101, 103, volume_pre_a))
    bars.append((100, 102, volume_pre_a))
    bars.append((99, 101, volume_pre_a))
    bars.append((96, 99, volume_pre_a))            # A (idx=11)
    bars.append((99, 102, volume_a_to_b))
    bars.append((101, 105, volume_a_to_b))
    bars.append((104, 108, volume_a_to_b))
    bars.append((108, 113, volume_a_to_b))
    bars.append((112, 117, volume_a_to_b))
    bars.append((116, 120, volume_a_to_b))         # B (idx=17)
    bars.append((114, 118, volume_b_to_c))
    bars.append((110, 115, volume_b_to_c))
    bars.append((105, 111, volume_b_to_c))
    bars.append((101, 106, volume_b_to_c))
    bars.append((100, 104, volume_b_to_c))         # C (idx=22)
    bars.append((103, 107, volume_b_to_c))
    bars.append((105, 110, volume_b_to_c))
    bars.append((108, 113, volume_b_to_c))
    bars.append((110, 115, volume_b_to_c))
    bars.append((112, 116, volume_b_to_c))
    bars.append((113, 117, volume_b_to_c))
    bars.append((114, 118, volume_b_to_c))
    bars.append((115, 119, volume_b_to_c))
    bars.append((114, 118, volume_b_to_c))
    bars.append((114, 118, volume_b_to_c))         # 最末根

    rows = []
    n = len(bars)
    base = end_date - timedelta(days=pattern_offset_days)
    for i, (lo, hi, vol) in enumerate(bars):
        d = base - timedelta(days=(n - 1 - i))
        rows.append({
            "date": d.isoformat(),
            "open": (lo + hi) / 2,
            "high": hi,
            "low": lo,
            "close": (lo + hi) / 2,
            "volume": vol,
        })
    df = pd.DataFrame(rows)
    if last_close is not None:
        df.loc[df.index[-1], "close"] = last_close
    return df


class BuildFromDataTest(unittest.TestCase):
    def setUp(self):
        self.today = date(2026, 5, 11)
        self.stock_info = {
            "2330": {"stock_name": "台積電", "industry_category": "半導體業"},
            "2454": {"stock_name": "聯發科", "industry_category": "半導體業"},
        }

    def test_actionable_candidate_passes(self):
        """C 形成在 5 天內、現價在 B 下方 3% → 應入選。"""
        df = _make_n_pattern_df(end_date=self.today, last_close=116.5)
        candidates, stats = build_from_data(
            price_data={"2330": df},
            stock_info=self.stock_info,
            today=self.today,
        )
        self.assertEqual(stats.scanned, 1)
        self.assertEqual(stats.hits, 1)
        self.assertEqual(stats.written, 1)
        c = candidates[0]
        self.assertEqual(c["stock_id"], "2330")
        self.assertEqual(c["stock_name"], "台積電")
        self.assertEqual(c["industry"], "半導體業")
        self.assertAlmostEqual(c["b_price"], 120.0)
        self.assertAlmostEqual(c["a_price"], 96.0)
        # distance = (120 - 116.5) / 120 * 100 ≈ 2.92
        self.assertAlmostEqual(c["distance_to_b_pct"], 2.9166666666, places=3)
        self.assertTrue(c["vol_a_to_b_increase"])
        self.assertTrue(c["vol_b_to_c_decrease"])

    def test_skip_when_c_too_old(self):
        """C 形成日距今超過 max_c_age_days → skipped_old_c。"""
        # 把整段資料往前 30 天，C 會變成 30 天前
        df = _make_n_pattern_df(end_date=self.today, pattern_offset_days=30, last_close=116.5)
        _, stats = build_from_data(
            price_data={"2330": df},
            stock_info=self.stock_info,
            today=self.today,
            max_c_age_days=15,
        )
        self.assertEqual(stats.hits, 1)
        self.assertEqual(stats.written, 0)
        self.assertEqual(stats.skipped_old_c, 1)

    def test_skip_when_price_too_far_below_b(self):
        """現價距 B 超過 max_distance_to_b_pct → skipped_far。"""
        # 強制 last_close = 100（距 B=120 約 16.7%）
        df = _make_n_pattern_df(end_date=self.today, last_close=100.0)
        _, stats = build_from_data(
            price_data={"2330": df},
            stock_info=self.stock_info,
            today=self.today,
            max_distance_to_b_pct=8.0,
        )
        self.assertEqual(stats.hits, 1)
        self.assertEqual(stats.written, 0)
        self.assertEqual(stats.skipped_far, 1)

    def test_skip_when_already_well_above_b(self):
        """現價已超過 B 太多（max_above_b_pct）→ skipped_already_broken。"""
        df = _make_n_pattern_df(end_date=self.today, last_close=125.0)   # 高於 B=120 約 4.2%
        _, stats = build_from_data(
            price_data={"2330": df},
            stock_info=self.stock_info,
            today=self.today,
            max_above_b_pct=1.5,
        )
        self.assertEqual(stats.hits, 1)
        self.assertEqual(stats.written, 0)
        self.assertEqual(stats.skipped_already_broken, 1)

    def test_just_breaking_b_still_passes(self):
        """剛突破 B 一點點（在 max_above_b_pct 範圍內）→ 仍入選，distance 為負。"""
        df = _make_n_pattern_df(end_date=self.today, last_close=121.0)   # 高於 B=120 約 0.83%
        candidates, stats = build_from_data(
            price_data={"2330": df},
            stock_info=self.stock_info,
            today=self.today,
            max_above_b_pct=1.5,
        )
        self.assertEqual(stats.written, 1)
        self.assertLess(candidates[0]["distance_to_b_pct"], 0)

    def test_multiple_stocks_mixed(self):
        """兩檔：一檔 actionable、一檔太遠 → 各被分類正確。"""
        df_ok = _make_n_pattern_df(end_date=self.today, last_close=117.0)
        df_far = _make_n_pattern_df(end_date=self.today, last_close=100.0)
        candidates, stats = build_from_data(
            price_data={"2330": df_ok, "2454": df_far},
            stock_info=self.stock_info,
            today=self.today,
        )
        self.assertEqual(stats.scanned, 2)
        self.assertEqual(stats.hits, 2)
        self.assertEqual(stats.written, 1)
        self.assertEqual(stats.skipped_far, 1)
        self.assertEqual(candidates[0]["stock_id"], "2330")

    def test_handles_finmind_max_min_columns(self):
        """price_data 用 max/min/Trading_Volume 命名也能正常處理。"""
        df = _make_n_pattern_df(end_date=self.today, last_close=117.0)
        df = df.rename(columns={"high": "max", "low": "min", "volume": "Trading_Volume"})
        _, stats = build_from_data(
            price_data={"2330": df},
            stock_info=self.stock_info,
            today=self.today,
        )
        self.assertEqual(stats.written, 1)


if __name__ == "__main__":
    unittest.main()
