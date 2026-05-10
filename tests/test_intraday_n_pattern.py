"""
modules.intraday_n_pattern 單元測試（不打 DB / 不打 Telegram / 不打 Shioaji）

執行：
  python -m unittest tests.test_intraday_n_pattern
"""
from __future__ import annotations

import unittest
from datetime import date
from unittest import mock

from modules.intraday_n_pattern import (
    format_breakout_alert,
    run_n_pattern_check,
)


def _sample_item(**overrides) -> dict:
    base = {
        "stock_id": "3467",
        "stock_name": "台灣精材",
        "industry": "半導體業",
        "a_date": date(2026, 4, 15),
        "a_price": 40.00,
        "b_date": date(2026, 4, 25),
        "b_price": 71.00,
        "c_date": date(2026, 5, 2),
        "c_price": 56.00,
        "b_rise_pct": 77.5,
        "c_retrace_pct": 48.4,
        "vol_a_to_b_increase": True,
        "vol_b_to_c_decrease": True,
        "avg_volume_b_to_c": 1500.0,
        "last_close": 70.5,
        "distance_to_b_pct": 0.7,
        "alerted_today": False,
    }
    base.update(overrides)
    return base


class FormatBreakoutAlertTest(unittest.TestCase):
    def test_full_message_contains_all_sections(self):
        msg = format_breakout_alert(
            _sample_item(),
            price=72.00,
            total_volume=2100.0,
            now_str="10:23",
        )
        # 標題與時間
        self.assertIn("3467 台灣精材", msg)
        self.assertIn("10:23", msg)
        # 結構區
        self.assertIn("A=40.00(04-15)", msg)
        self.assertIn("B=71.00(04-25)", msg)
        self.assertIn("C=56.00(05-02)", msg)
        # 量能驗證
        self.assertIn("A→B 量增：✓", msg)
        self.assertIn("B→C 量縮：✓", msg)
        # 量倍數 = 2100/1500 = 1.40
        self.assertIn("1.40×", msg)
        # 現價與突破幅度（72-71)/71 ≈ 1.41%
        self.assertIn("72.00", msg)
        self.assertIn("+1.41%", msg)
        # 假突破提醒
        self.assertIn("留意假突破", msg)
        # 報價來源
        self.assertIn("Shioaji", msg)

    def test_volume_signals_negative_render_as_x(self):
        msg = format_breakout_alert(
            _sample_item(vol_a_to_b_increase=False, vol_b_to_c_decrease=False),
            price=72.00,
            total_volume=2100.0,
            now_str="10:23",
        )
        self.assertIn("A→B 量增：✗", msg)
        self.assertIn("B→C 量縮：✗", msg)

    def test_volume_signals_unknown_render_as_dash(self):
        msg = format_breakout_alert(
            _sample_item(vol_a_to_b_increase=None, vol_b_to_c_decrease=None),
            price=72.00,
            total_volume=2100.0,
            now_str="10:23",
        )
        self.assertIn("A→B 量增：—", msg)
        self.assertIn("B→C 量縮：—", msg)

    def test_missing_total_volume_shows_unknown(self):
        msg = format_breakout_alert(
            _sample_item(),
            price=72.00,
            total_volume=None,
            now_str="10:23",
        )
        self.assertIn("今日累積量：未知", msg)

    def test_missing_baseline_volume_shows_unknown(self):
        msg = format_breakout_alert(
            _sample_item(avg_volume_b_to_c=None),
            price=72.00,
            total_volume=2100.0,
            now_str="10:23",
        )
        self.assertIn("今日累積量：未知", msg)

    def test_just_breaking_b_by_tiny_amount(self):
        """剛突破 0.01 元也應顯示為突破。"""
        msg = format_breakout_alert(
            _sample_item(),
            price=71.01,
            total_volume=1600.0,
            now_str="09:01",
        )
        self.assertIn("71.01", msg)
        # 0.01/71 ≈ 0.014%
        self.assertIn("+0.01%", msg)


class RunNPatternCheckTest(unittest.TestCase):
    """整合測試：mock 掉 DB / Shioaji / Telegram 三個外部依賴。"""

    def test_no_active_items_returns_zero(self):
        with mock.patch("db.n_pattern_watchlist.today_active", return_value=[]):
            self.assertEqual(run_n_pattern_check(), 0)

    def test_no_shioaji_returns_zero_without_api_calls(self):
        """Shioaji 沒登入時整輪安靜跳過，不應呼叫 Telegram。"""
        item = _sample_item()
        with mock.patch("db.n_pattern_watchlist.today_active", return_value=[item]), \
             mock.patch("modules.intraday_n_pattern._get_shioaji_snapshots", return_value={}), \
             mock.patch("notifications.telegram_notify.send_stock_alert") as send_mock:
            self.assertEqual(run_n_pattern_check(), 0)
            send_mock.assert_not_called()

    def test_breakout_triggers_alert_and_marks(self):
        """價 > B 應觸發 Telegram 並標記 alerted_today。"""
        item = _sample_item()
        snaps = {"3467": {"last_price": 72.0, "total_volume": 2100.0}}
        with mock.patch("db.n_pattern_watchlist.today_active", return_value=[item]), \
             mock.patch("db.n_pattern_watchlist.mark_alerted", return_value=True) as mark_mock, \
             mock.patch("modules.intraday_n_pattern._get_shioaji_snapshots", return_value=snaps), \
             mock.patch("notifications.telegram_notify.send_stock_alert", return_value=True) as send_mock, \
             mock.patch("db.event_log.log_event"):
            sent = run_n_pattern_check()
        self.assertEqual(sent, 1)
        send_mock.assert_called_once()
        mark_mock.assert_called_once_with("3467")
        sent_msg = send_mock.call_args.args[0]
        self.assertIn("3467 台灣精材", sent_msg)
        self.assertIn("留意假突破", sent_msg)

    def test_no_breakout_does_not_alert(self):
        """價 <= B 不應觸發推播或標記。"""
        item = _sample_item(b_price=71.0)
        snaps = {"3467": {"last_price": 71.0, "total_volume": 1500.0}}   # 等於 B 不算突破
        with mock.patch("db.n_pattern_watchlist.today_active", return_value=[item]), \
             mock.patch("db.n_pattern_watchlist.mark_alerted") as mark_mock, \
             mock.patch("modules.intraday_n_pattern._get_shioaji_snapshots", return_value=snaps), \
             mock.patch("notifications.telegram_notify.send_stock_alert") as send_mock:
            sent = run_n_pattern_check()
        self.assertEqual(sent, 0)
        send_mock.assert_not_called()
        mark_mock.assert_not_called()

    def test_telegram_failure_still_marks_to_avoid_loop(self):
        """
        Telegram 推播失敗仍保留 alerted_today=True（樂觀標記模式）。
        這跟 portfolio monitor 的策略一致：避免重試期間每分鐘都重推。
        若使用者要重新觸發，需手動或等到隔日 watchlist 重建。
        """
        item = _sample_item()
        snaps = {"3467": {"last_price": 72.0, "total_volume": 2100.0}}
        with mock.patch("db.n_pattern_watchlist.today_active", return_value=[item]), \
             mock.patch("db.n_pattern_watchlist.mark_alerted") as mark_mock, \
             mock.patch("modules.intraday_n_pattern._get_shioaji_snapshots", return_value=snaps), \
             mock.patch("notifications.telegram_notify.send_stock_alert", return_value=False), \
             mock.patch("db.event_log.log_event"):
            sent = run_n_pattern_check()
        self.assertEqual(sent, 0)            # 推播失敗 sent 為 0
        mark_mock.assert_called_once()       # 但仍標記


if __name__ == "__main__":
    unittest.main()
