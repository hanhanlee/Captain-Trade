"""
煙霧測試：用 2312 金寶 5/22 模擬 09:30 盤中重判，
驗證 intraday_v5_late_qualifier 的 patch + evaluate_v5 邏輯能命中。

執行：python scripts/test_v5_late_qualifier_2312.py
"""
from __future__ import annotations

import sys
import os
from datetime import date, datetime

# 確保 stdout 走 UTF-8（Windows CP950 規避）
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Project root 加入 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.price_cache import load_prices
from modules.scanner import compute_indicators
from modules.v5_sniper import V5Params, evaluate_v5
from modules.intraday_v5_late_qualifier import (
    _patch_intraday_bar,
    _estimate_full_day_volume,
    BREAKOUT_BUFFER_PCT,
)


def _late_gate_passed(result) -> bool:
    """與 intraday_v5_late_qualifier 內部邏輯一致：盤中跳過 body_strong + gain_within_cap（手動驗）。"""
    return (
        result.ma_full_bull
        and result.red_candle
        and result.close_gt_ma5
        and result.high_gt_prev
        and result.vol_breakout
        and result.vol_floor_ok
        and result.kd_bull
        and result.macd_bull_zero_above
        and result.macd_osc_growing
    )


def main():
    sid = "2312"
    target_date = date(2026, 5, 22)
    sim_time = datetime(2026, 5, 22, 9, 30)

    print(f"=== 煙霧測試：{sid} 金寶 模擬 {sim_time.strftime('%Y-%m-%d %H:%M')} V5 補抓重判 ===\n")

    # 1) 載入截至 5/21 的 EOD（120 天）
    df_all = load_prices(sid, lookback_days=200)
    if df_all is None or df_all.empty:
        print(f"❌ 無 {sid} 的 EOD 資料")
        return 1

    df_eod = df_all[df_all["date"].dt.date < target_date].copy()
    if len(df_eod) < 60:
        print(f"❌ {sid} 截至 {target_date} 前一日 EOD 不足 60 根（只有 {len(df_eod)} 根）")
        return 1

    last_eod = df_eod.iloc[-1]
    print(f"5/21 EOD 最後一根：")
    print(f"  日期：{last_eod['date'].date()}")
    print(f"  open={last_eod['open']:.2f} max={last_eod['max']:.2f} "
          f"min={last_eod['min']:.2f} close={last_eod['close']:.2f}")
    print(f"  Trading_Volume={last_eod['Trading_Volume']:.0f}\n")

    # 2) 構造 5/22 09:30 的 snapshot（用實際 5/22 EOD 的 high/close 做最佳近似）
    #    實際 5/22 走勢：開盤約 32.50，最高 33.10，收 33.10
    #    09:30 假設已經跳空上來並逼近最高（合理假設：強勢突破票）
    snap = {
        "last_price": 33.10,
        "open": 32.50,
        "high": 33.10,
        "low": 31.00,
        # 30 分鐘成交假設 8000 張（強勢突破票早盤就常見 5000–15000 張）
        "total_volume": 8000.0,
    }
    print(f"模擬 5/22 09:30 snapshot：")
    print(f"  last_price={snap['last_price']:.2f} open={snap['open']:.2f} "
          f"high={snap['high']:.2f} low={snap['low']:.2f}")
    print(f"  total_volume={snap['total_volume']:.0f} 張（盤中累計）\n")

    # 3) 預估全日量
    est_full_day_lots, scale = _estimate_full_day_volume(snap["total_volume"], sim_time)
    print(f"預估全日量：{est_full_day_lots:.0f} 張（×{scale} scale factor）\n")

    # 4) Patch 進「今日 K 棒」
    df_patched = _patch_intraday_bar(
        df_eod, today=target_date, snap=snap,
        estimated_full_day_lots=est_full_day_lots,
    )
    if df_patched is None:
        print("❌ patch 失敗")
        return 1

    print(f"Patched df 最後一根（今日）：")
    last = df_patched.iloc[-1]
    print(f"  日期：{last['date'].date() if hasattr(last['date'], 'date') else last['date']}")
    print(f"  open={last['open']:.2f} max={last['max']:.2f} "
          f"min={last['min']:.2f} close={last['close']:.2f}")
    print(f"  Trading_Volume={last['Trading_Volume']:.0f}（=預估全日量 ×1000）\n")

    # 5) 重算指標 + evaluate_v5
    #    與 late_qualifier 一致：解禁 body_ratio_min / max_gain_pct，讓 pattern detection 跑得到
    df_with_ind = compute_indicators(df_patched)
    params = V5Params(
        body_ratio_min=0.0,
        max_gain_pct=None,
        pattern_a_squeeze_pct=4.0,
    )
    result = evaluate_v5(df_with_ind, today=target_date, params=params)

    # 6) 印每條 gate 結果
    print("=== V5 共同 gate 結果 ===")
    gate_fields = [
        "ma_full_bull", "red_candle", "body_strong", "close_gt_ma5",
        "high_gt_prev", "vol_breakout", "vol_floor_ok",
        "kd_bull", "macd_bull_zero_above", "macd_osc_growing",
        "gain_within_cap",
    ]
    for f in gate_fields:
        val = getattr(result, f, False)
        mark = "✅" if val else "❌"
        print(f"  {mark} {f}")
    print(f"\n  gate_passed = {result.gate_passed}")
    print(f"  pattern_type = {result.pattern_type}")
    print(f"  pattern_a_squeeze_ok = {result.pattern_a_squeeze_ok}")
    print(f"  pattern_a_break_10d = {result.pattern_a_break_10d}")
    print(f"  gain_pct = {result.gain_pct:.2f}% (僅供參考，late qualifier 改用 breakout buffer)")
    print()

    last_ind = df_with_ind.iloc[-1]
    print(f"指標（patched 後重算）：")
    print(f"  ma5={last_ind['ma5']:.2f} ma10={last_ind['ma10']:.2f} "
          f"ma20={last_ind['ma20']:.2f} ma60={last_ind['ma60']:.2f}")
    print(f"  macd_hist={last_ind['macd_hist']:.4f}\n")

    # 7) 最終結論（用 late_qualifier 內部規則：跳過 body_strong，手動套 breakout buffer）
    gates_ok = _late_gate_passed(result)
    pattern_ok = result.pattern_type in ("A", "B")

    # 計算 breakout_target（與 late_qualifier 同邏輯）
    df_eod_no_today = df_with_ind[df_with_ind["date"].dt.date < target_date]
    if result.pattern_type == "A":
        breakout_target = float(df_eod_no_today["max"].iloc[-10:].max())
    elif result.pattern_type == "B":
        breakout_target = float(df_eod_no_today["max"].iloc[-1])
    else:
        breakout_target = float("nan")

    price = float(df_with_ind["close"].iloc[-1])
    breakout_pct = (price - breakout_target) / breakout_target * 100 if breakout_target > 0 else 0.0
    buffer_ok = pattern_ok and price <= breakout_target * (1 + BREAKOUT_BUFFER_PCT / 100)
    late_pass = gates_ok and pattern_ok and buffer_ok

    print("=" * 60)
    print(f"late_qualifier 規則 pass = {late_pass}")
    print(f"  9 gates (排除 body_strong) = {gates_ok}")
    print(f"  pattern_type = {result.pattern_type} → pattern_ok = {pattern_ok}")
    if pattern_ok:
        print(f"  breakout_target = {breakout_target:.2f}")
        print(f"  現價 {price:.2f} = 突破點 +{breakout_pct:+.2f}% / buffer {BREAKOUT_BUFFER_PCT:.1f}% → buffer_ok = {buffer_ok}")
    if late_pass:
        print(f"✅ 通過！盤中 late qualifier 會推播 {sid} 金寶 {result.pattern_type} 型")
        return 0
    else:
        missed = []
        for f in [
            "ma_full_bull", "red_candle", "close_gt_ma5", "high_gt_prev",
            "vol_breakout", "vol_floor_ok", "kd_bull", "macd_bull_zero_above",
            "macd_osc_growing",
        ]:
            if not getattr(result, f, False):
                missed.append(f)
        if not pattern_ok:
            missed.append("pattern_type(None)")
        if pattern_ok and not buffer_ok:
            missed.append(f"breakout_buffer(過突破點+{breakout_pct:.2f}%>buffer{BREAKOUT_BUFFER_PCT:.1f}%)")
        print(f"\n❌ 未通過，缺：{', '.join(missed)}")
        if pattern_ok and not buffer_ok:
            print(f"\n  ⚠ 2312 9:30 模擬價已過突破點 +{breakout_pct:.2f}%，3% buffer 接不到（屬於極端跳空案例）")
        return 2


if __name__ == "__main__":
    sys.exit(main())
