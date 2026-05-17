"""
v5 狙擊手版 — 精準型突破與回檔選股雷達

參考：ref/stock_selection_radar.md

雙軌進場（OR 關係）：
  型態 A：均線糾結 + 突破近 10 日新高（盤整突破）
  型態 B：回檔再上（底底高 + 守月線）+ 破昨高（點火）
         pullback（預設）：近 5 日 Low > 前 6~20 日 Low；過去 5 日 Close 都 > MA20
         n_pattern（嚴謹）：呼叫 n_pattern_detector，A/B/C 點明確

共同 gate（不分型態都要過）：
  - MA5 > MA10 > MA20 > MA60（多頭排列、季線之上）
  - 收紅 K：Close > Open
  - 實體比：(Close - Open) / (High - Low) ≥ BODY_RATIO_MIN
  - Close > MA5
  - High > High[1]（破昨高）
  - Volume ≥ Volume[1] × VOL_MULT 且 Volume ≥ MIN_LOTS（張）
  - KD：K > D
  - MACD：DIF > DEA 且 DIF > 0 且 DEA > 0
  - MACD OSC > 0 且 OSC > OSC[1]
  - 今日漲幅 ≤ MAX_GAIN_PCT（避免追高）

設計原則：
  - 直接消費 modules.scanner.compute_indicators() 已產出的欄位
    （ma5/ma10/ma20/ma60/macd_hist 等），不重算
  - 型態 B 預設用 pullback 簡化規格；可切到 n_pattern_detector 嚴格模式
  - 條件極嚴，「今日 0 檔」屬正常空倉；UI 端要明示這是 feature 不是 bug
  - 盤中重跑：呼叫端負責把現價 patch 到 df 最後一筆 close（本層只認 df 內容）

對外 API：
  evaluate_v5(df, *, today=None, params=None) -> V5Result
  passes_basic_v5(result: V5Result) -> bool
  score_v5(result: V5Result) -> float

整合方式（在 modules/scanner.py 內呼叫）：
  from modules.v5_sniper import evaluate_v5
  res = evaluate_v5(df, today=scan_date)
  # 把 res 的 booleans / pattern_type / reasons 灌進 ScanSignal 對應欄位即可
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ── 預設參數（可由 UI / config 覆寫） ────────────────────────────────────
@dataclass(frozen=True)
class V5Params:
    # 共同 gate
    body_ratio_min: float = 0.60         # 實體 K / 全棒振幅
    vol_mult: float = 1.5                # 今量 ÷ 昨量 倍數門檻
    min_lots: int = 1000                 # 最低成交張數
    max_gain_pct: Optional[float] = None # 今日漲幅上限（防追高）；None = 停用此 gate
    ma_full_alignment: bool = True       # MA5>MA10>MA20>MA60

    # 型態 A：糾結突破
    pattern_a_squeeze_pct: float = 3.0   # 均線糾結度門檻（昨日）
    pattern_a_breakout_window: int = 10  # 收盤 ≥ 近 N 日最高（含今日）

    # 型態 B：回檔再上（簡化版 pullback，與 n_pattern_detector 二擇一）
    # pullback 規格：
    #   條件 1（底底高）：Lowest(Low, 5) > Lowest(Low[5], 15) — 拉回不破前低
    #   條件 2（守月線）：過去 N 日 Close 每天都 > MA20 — 回檔在月線之上
    #   條件 3、4（紅K突破MA5 / 突破前日高）已在共同 gate，不重複實作
    pattern_b_use_n_pattern: bool = False # True = 呼叫 n_pattern_detector（嚴謹）；False = pullback（守月線）
    pattern_b_recent_low_window: int = 5
    pattern_b_prior_low_window: int = 15  # 前 6~20 日 → 跳過近 5 後再取 15 根
    pattern_b_above_ma20_window: int = 5  # 過去 N 日 Close 都 > MA20

    # n_pattern_detector 透傳參數（pattern_b_use_n_pattern=True 時生效）
    n_pattern_lookback: int = 80
    n_pattern_fractal_k: int = 3
    n_pattern_min_b_rise_pct: float = 5.0
    n_pattern_min_c_retrace_pct: float = 30.0
    n_pattern_max_c_retrace_pct: float = 50.0
    n_pattern_max_c_age_days: int = 15


# ── 結果結構 ────────────────────────────────────────────────────────────
@dataclass
class V5Result:
    pattern_type: Optional[str] = None   # "A" / "B" / None
    gate_passed: bool = False            # 共同 gate 全部通過
    reasons: list[str] = field(default_factory=list)  # 觸發了哪些條件（debug / label 用）

    # 個別 gate 結果（給上層計分 / 顯示用）
    ma_full_bull: bool = False
    body_strong: bool = False
    red_candle: bool = False
    close_gt_ma5: bool = False
    high_gt_prev: bool = False
    vol_breakout: bool = False
    vol_floor_ok: bool = False
    kd_bull: bool = False
    macd_bull_zero_above: bool = False
    macd_osc_growing: bool = False
    gain_within_cap: bool = False
    gain_cap_applied: bool = False       # 是否實際啟用漲幅 gate（False 時 gain_within_cap 強制 True、不計分、不顯示標籤）
    gain_pct: float = 0.0                # 今日漲幅（%），無論 gate 是否啟用都記下，方便顯示

    # 型態 A
    pattern_a_squeeze_ok: bool = False
    pattern_a_break_10d: bool = False

    # 型態 B
    pattern_b_higher_low: bool = False
    pattern_b_above_ma20: bool = False     # pullback 模式：過去 N 日 Close 都 > MA20
    pattern_b_n_pattern_hit: bool = False  # 若改採 n_pattern_detector


# ── 主要入口 ────────────────────────────────────────────────────────────
def evaluate_v5(
    df: pd.DataFrame,
    *,
    today: Optional[date] = None,
    params: Optional[V5Params] = None,
) -> V5Result:
    """
    純函式版：對單一檔股票的日 K（已含指標欄位）做 v5 判定。

    前提：df 已經跑過 modules.scanner.compute_indicators()，
         必含 ma5/ma10/ma20/ma60/macd_hist 等欄位。
    """
    p = params or V5Params()
    result = V5Result()

    if df is None or df.empty or len(df) < 60:
        return result

    # ── 取最後兩根做比較 ──
    last = df.iloc[-1]
    prev = df.iloc[-2]

    # 必要欄位防呆
    required_cols = ("open", "high", "low", "close",
                     "ma5", "ma10", "ma20", "ma60",
                     "macd", "macd_signal", "macd_hist")
    if any(c not in df.columns for c in required_cols):
        return result
    if any(pd.isna(last.get(c)) for c in required_cols):
        return result

    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
    if vol_col is None or pd.isna(last.get(vol_col)) or pd.isna(prev.get(vol_col)):
        return result

    # ── 共同 gate ─────────────────────────────────────────────────────
    result.ma_full_bull = (
        last["ma5"] > last["ma10"] > last["ma20"] > last["ma60"]
    )

    result.red_candle = float(last["close"]) > float(last["open"])
    total_range = float(last["high"]) - float(last["low"])
    if total_range > 0:
        body = float(last["close"]) - float(last["open"])
        result.body_strong = (body / total_range) >= p.body_ratio_min

    result.close_gt_ma5 = float(last["close"]) > float(last["ma5"])
    result.high_gt_prev = float(last["high"]) > float(prev["high"])

    # Volume 單位：FinMind Trading_Volume 為「股」，1 張 = 1000 股
    vol_today_lots = float(last[vol_col]) / 1000.0
    vol_prev_lots = float(prev[vol_col]) / 1000.0
    result.vol_breakout = vol_today_lots >= vol_prev_lots * p.vol_mult
    result.vol_floor_ok = vol_today_lots >= p.min_lots

    # 漲幅上限（防追高）— 可選 gate
    if float(prev["close"]) > 0:
        result.gain_pct = (float(last["close"]) - float(prev["close"])) / float(prev["close"]) * 100.0
    if p.max_gain_pct is None:
        # 未啟用 → 視為通過，不影響 gate / score / label
        result.gain_cap_applied = False
        result.gain_within_cap = True
    else:
        result.gain_cap_applied = True
        result.gain_within_cap = result.gain_pct <= p.max_gain_pct

    # KD / MACD：依賴 compute_indicators 已產出的欄位
    # TODO: 確認 scanner.py 是否有 k_value / d_value 欄位；若無，可用 indicators.py 的 kdj() 補上
    if "k_value" in df.columns and "d_value" in df.columns and not pd.isna(last["k_value"]):
        result.kd_bull = float(last["k_value"]) > float(last["d_value"])
    else:
        result.kd_bull = True  # TODO: 缺欄位先 pass，避免誤殺；正式上線前接好

    # MACD: scanner.compute_indicators() 已塞 df["macd"]=DIF / df["macd_signal"]=DEA / df["macd_hist"]
    if "macd" in df.columns and "macd_signal" in df.columns and not pd.isna(last["macd"]):
        dif, dea = float(last["macd"]), float(last["macd_signal"])
        result.macd_bull_zero_above = (dif > dea) and (dif > 0) and (dea > 0)
    else:
        result.macd_bull_zero_above = float(last["macd_hist"]) > 0  # 退而求其次

    if not pd.isna(prev["macd_hist"]):
        osc, osc_prev = float(last["macd_hist"]), float(prev["macd_hist"])
        result.macd_osc_growing = (osc > 0) and (osc > osc_prev)

    gate_passed = (
        (result.ma_full_bull if p.ma_full_alignment else True)
        and result.red_candle
        and result.body_strong
        and result.close_gt_ma5
        and result.high_gt_prev
        and result.vol_breakout
        and result.vol_floor_ok
        and result.kd_bull
        and result.macd_bull_zero_above
        and result.macd_osc_growing
        and result.gain_within_cap
    )
    result.gate_passed = gate_passed

    if not gate_passed:
        # gate 沒過就不繼續判型態，省 n_pattern 計算成本
        _collect_reasons(result)
        return result

    # ── 型態 A：均線糾結 + 突破近 10 日新高 ───────────────────────────
    # 糾結度用「昨日」三線（與既有 v4 一致：ma_squeeze 旗標的精神）
    ma_vals_prev = [float(prev["ma5"]), float(prev["ma10"]), float(prev["ma20"])]
    if min(ma_vals_prev) > 0:
        spread_pct = (max(ma_vals_prev) - min(ma_vals_prev)) / float(prev["ma20"]) * 100
        result.pattern_a_squeeze_ok = spread_pct < p.pattern_a_squeeze_pct

    # 突破近 10 日新高（含今日）：今日 close ≥ 近 N 日內的最高 high
    window = df["high"].iloc[-p.pattern_a_breakout_window:]
    if not window.empty:
        result.pattern_a_break_10d = float(last["close"]) >= float(window.max())

    pattern_a_hit = result.pattern_a_squeeze_ok and result.pattern_a_break_10d

    # ── 型態 B：回檔再上（底底高 + 守月線） ───────────────────────────
    pattern_b_hit = False
    if p.pattern_b_use_n_pattern:
        pattern_b_hit = _pattern_b_via_n_pattern(df, today, p, result)
    else:
        pattern_b_hit = _pattern_b_via_pullback(df, p, result)

    # ── 決定 pattern_type（A 優先；同時命中也視為命中） ──────────────
    if pattern_a_hit:
        result.pattern_type = "A"
    elif pattern_b_hit:
        result.pattern_type = "B"

    _collect_reasons(result)
    return result


# ── 型態 B 兩種實作 ────────────────────────────────────────────────────
def _pattern_b_via_n_pattern(
    df: pd.DataFrame,
    today: Optional[date],
    p: V5Params,
    result: V5Result,
) -> bool:
    """重用既有 N 字底偵測器：A→B→C 形成後，今日 = D（破昨高 + 共同 gate 已過）。"""
    from modules.n_pattern_detector import find_abc_points

    try:
        pat = find_abc_points(
            df,
            lookback=p.n_pattern_lookback,
            fractal_k=p.n_pattern_fractal_k,
            min_b_rise_pct=p.n_pattern_min_b_rise_pct,
            min_c_retrace_pct=p.n_pattern_min_c_retrace_pct,
            require_prior_downtrend=False,  # v5 已有 MA 多頭排列當趨勢保護
        )
    except Exception as exc:
        logger.warning("v5 pattern B n_pattern_detector 失敗: %s", exc)
        return False

    if pat is None:
        return False
    if pat.c_retrace_pct >= p.n_pattern_max_c_retrace_pct:
        return False

    # C 不能太舊
    if today is not None and pat.C.date is not None:
        try:
            c_date = pd.to_datetime(pat.C.date).date()
            if (today - c_date).days > p.n_pattern_max_c_age_days:
                return False
        except Exception:
            pass

    result.pattern_b_n_pattern_hit = True
    return True


def _pattern_b_via_pullback(
    df: pd.DataFrame,
    p: V5Params,
    result: V5Result,
) -> bool:
    """
    型態 B 簡化版（pullback / 守月線）：
      條件 1（底底高）：近 5 日 Low 最小 > 前 6~20 日 Low 最小
      條件 2（守月線）：過去 N 日 Close 每天都 > MA20

    註：條件 3「紅K + 站上 MA5」、條件 4「突破前日高」已在 evaluate_v5 共同
        gate 涵蓋（red_candle / close_gt_ma5 / high_gt_prev），此函式不重複實作。

    盤中重跑約定：
        df 最後一筆 close 由呼叫端負責填值。EOD 為當日收盤；盤中若要即時重判，
        呼叫端應先把現價 patch 到 df.iloc[-1]["close"]，本函式不另做時間判斷。
    """
    n_recent = p.pattern_b_recent_low_window           # 5
    n_prior = p.pattern_b_prior_low_window             # 15
    n_above = p.pattern_b_above_ma20_window            # 5

    if len(df) < n_recent + n_prior + 1:
        return False

    recent_low = float(df["low"].iloc[-n_recent:].min())
    prior_low = float(df["low"].iloc[-(n_recent + n_prior):-n_recent].min())
    result.pattern_b_higher_low = recent_low > prior_low

    closes = df["close"].iloc[-n_above:]
    ma20s = df["ma20"].iloc[-n_above:]
    if closes.isna().any() or ma20s.isna().any():
        result.pattern_b_above_ma20 = False
    else:
        result.pattern_b_above_ma20 = bool((closes.values > ma20s.values).all())

    return result.pattern_b_higher_low and result.pattern_b_above_ma20


# ── 計分 / 判定 ────────────────────────────────────────────────────────
_GATE_FIELDS = (
    "ma_full_bull", "red_candle", "body_strong", "close_gt_ma5",
    "high_gt_prev", "vol_breakout", "vol_floor_ok",
    "kd_bull", "macd_bull_zero_above", "macd_osc_growing",
    "gain_within_cap",
)

_GATE_WEIGHTS = {
    "ma_full_bull": 15,
    "body_strong": 10,
    "close_gt_ma5": 5,
    "high_gt_prev": 10,
    "vol_breakout": 15,
    "vol_floor_ok": 5,
    "kd_bull": 5,
    "macd_bull_zero_above": 10,
    "macd_osc_growing": 5,
    "gain_within_cap": 5,
}

_PATTERN_BONUS = {
    "A": 20,   # 糾結突破：較罕見，加分多
    "B": 15,   # 回檔再上：常見一些
}


def passes_basic_v5(result: V5Result) -> bool:
    """v5 必要條件：共同 gate 全過，且至少命中型態 A 或 B。"""
    return result.gate_passed and result.pattern_type in ("A", "B")


def score_v5(result: V5Result) -> float:
    """
    v5 計分：共同 gate + 型態加分（A=20, B=15）
    與 v3/v4 計分區間（100~150）大致對齊，方便排序顯示。

    漲幅 gate 若未啟用（gain_cap_applied=False），即使 gain_within_cap=True 也不計分，
    避免「停用 gate 反而每檔自動 +5」造成分數膨脹、不同設定間無法比較。
    """
    base = 0
    for k, w in _GATE_WEIGHTS.items():
        if not getattr(result, k, False):
            continue
        if k == "gain_within_cap" and not result.gain_cap_applied:
            continue
        base += w
    bonus = _PATTERN_BONUS.get(result.pattern_type or "", 0)
    return round(float(base + bonus), 1)


def triggered_labels_v5(result: V5Result) -> list[str]:
    """產出顯示用標籤，給 page 的『訊號』欄使用。"""
    label_map = {
        "ma_full_bull": "MA5>10>20>60",
        "body_strong": "實體紅K≥60%",
        "close_gt_ma5": "站上MA5",
        "high_gt_prev": "破昨高",
        "vol_breakout": "量增1.5x",
        "vol_floor_ok": "量≥1000張",
        "kd_bull": "KD多頭",
        "macd_bull_zero_above": "MACD零軸上",
        "macd_osc_growing": "OSC加長",
        # gain_within_cap 標籤由下方依 gain_cap_applied 動態決定，這裡不放
        "pattern_a_squeeze_ok": "均線糾結<3%",
        "pattern_a_break_10d": "破10日新高",
        "pattern_b_higher_low": "底底高",
        "pattern_b_above_ma20": "回檔守月線",
        "pattern_b_n_pattern_hit": "N字底成形",
    }
    labels = [v for k, v in label_map.items() if getattr(result, k, False)]
    # 漲幅 gate：只有啟用時才顯示，顯示實際漲幅 % 讓使用者一眼判斷
    if result.gain_cap_applied and result.gain_within_cap:
        labels.append(f"漲幅{result.gain_pct:.1f}%≤上限")
    if result.pattern_type:
        labels.insert(0, f"型態{result.pattern_type}")
    return labels


# ── 內部 helper ───────────────────────────────────────────────────────
def _collect_reasons(result: V5Result) -> None:
    """把目前命中/未命中的 gate 條件收集到 reasons，供 debug。"""
    for f in _GATE_FIELDS:
        ok = getattr(result, f, False)
        suffix = ""
        if f == "gain_within_cap" and not result.gain_cap_applied:
            suffix = "（未啟用）"
        result.reasons.append(f"{'✓' if ok else '✗'} {f}{suffix}")


# ════════════════════════════════════════════════════════════════════════
# 盤中 builder 用：只檢查靜態 gate（EOD 已決定的條件）
# ════════════════════════════════════════════════════════════════════════
@dataclass
class V5BuilderResult:
    """
    08:50 盤前 builder 用的精簡結果。
    只裝載靜態 gate（昨日 EOD 即可判定）與 breakout_target，
    動態 gate（破昨高 / 量增 / 紅K / 漲幅 cap）留給盤中 checker。
    """
    pattern_type: Optional[str] = None       # "A" / "B"
    static_gate_passed: bool = False

    # 盤中 checker 所需欄位
    breakout_target: float = 0.0             # 今日要破的目標價（含等於）
    prev_high: float = 0.0                   # 昨日最高（破昨高用）
    prev_close: float = 0.0
    prev_volume_lots: float = 0.0            # 昨日張數
    vol_ma5_lots: float = 0.0                # 五日均量（張）

    ma5: float = 0.0
    ma10: float = 0.0
    ma20: float = 0.0
    ma60: float = 0.0
    ma_spread_pct: float = 0.0               # MA5/10/20 糾結度（昨日）

    # 靜態 gate 個別結果（debug 用）
    ma_full_bull: bool = False
    kd_bull: bool = False
    macd_bull_zero_above: bool = False
    macd_osc_growing: bool = False
    pattern_a_squeeze_ok: bool = False
    pattern_a_room_to_break: bool = False    # 昨收 < 10 日高（還沒破）
    pattern_b_hit: bool = False


def evaluate_v5_for_builder(
    df: pd.DataFrame,
    *,
    today: Optional[date] = None,
    params: Optional[V5Params] = None,
) -> Optional[V5BuilderResult]:
    """
    盤前 builder 專用：只跑靜態 gate，產出今日要監看的 breakout_target。

    與 evaluate_v5 的差異：
      - df 最後一筆 = 昨日 EOD（08:50 跑時尚未有今日 K）
      - 不檢查紅K / 實體 / 站上MA5 / 破昨高 / 量增 / 漲幅cap → 留給盤中
      - 型態 A：要求昨收尚未破 10 日高（否則今日無突破空間）
      - 型態 B：同 evaluate_v5（N-字底 / rolling 都是靜態判斷）

    回傳 None 表示靜態 gate 未過或無型態命中；否則回傳 V5BuilderResult。
    """
    p = params or V5Params()

    if df is None or df.empty or len(df) < 60:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    required_cols = ("open", "high", "low", "close",
                     "ma5", "ma10", "ma20", "ma60",
                     "macd", "macd_signal", "macd_hist")
    if any(c not in df.columns for c in required_cols):
        return None
    if any(pd.isna(last.get(c)) for c in required_cols):
        return None

    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
    if vol_col is None or pd.isna(last.get(vol_col)):
        return None

    res = V5BuilderResult()

    # ── 靜態 gate ────────────────────────────────────────────────────
    res.ma_full_bull = (
        last["ma5"] > last["ma10"] > last["ma20"] > last["ma60"]
    )

    # KD（缺欄位 fallback True，與 evaluate_v5 一致）
    if "k_value" in df.columns and "d_value" in df.columns and not pd.isna(last.get("k_value")):
        res.kd_bull = float(last["k_value"]) > float(last["d_value"])
    else:
        res.kd_bull = True

    dif, dea = float(last["macd"]), float(last["macd_signal"])
    res.macd_bull_zero_above = (dif > dea) and (dif > 0) and (dea > 0)

    if not pd.isna(prev["macd_hist"]):
        osc, osc_prev = float(last["macd_hist"]), float(prev["macd_hist"])
        res.macd_osc_growing = (osc > 0) and (osc > osc_prev)

    static_gate_passed = (
        (res.ma_full_bull if p.ma_full_alignment else True)
        and res.kd_bull
        and res.macd_bull_zero_above
        and res.macd_osc_growing
    )
    res.static_gate_passed = static_gate_passed
    if not static_gate_passed:
        return None

    # ── 共用欄位（給 checker）────────────────────────────────────────
    res.prev_high = float(last["high"])
    res.prev_close = float(last["close"])
    res.prev_volume_lots = float(last[vol_col]) / 1000.0
    if "vol_ma5" in df.columns and not pd.isna(last.get("vol_ma5")):
        # vol_ma5 已是「股」單位（scanner 算法與 Trading_Volume 同單位）
        res.vol_ma5_lots = float(last["vol_ma5"]) / 1000.0
    res.ma5 = float(last["ma5"])
    res.ma10 = float(last["ma10"])
    res.ma20 = float(last["ma20"])
    res.ma60 = float(last["ma60"])

    ma_vals = [res.ma5, res.ma10, res.ma20]
    if min(ma_vals) > 0:
        res.ma_spread_pct = round(
            (max(ma_vals) - min(ma_vals)) / res.ma20 * 100, 2
        )

    # ── 型態 A：均線糾結（昨日）+ 昨收尚未破 10 日高 ────────────────
    res.pattern_a_squeeze_ok = res.ma_spread_pct < p.pattern_a_squeeze_pct

    window_high = float(df["high"].iloc[-p.pattern_a_breakout_window:].max())
    res.pattern_a_room_to_break = res.prev_close < window_high

    pattern_a_hit = res.pattern_a_squeeze_ok and res.pattern_a_room_to_break

    # ── 型態 B：底底高 + 守月線（同 evaluate_v5，靜態） ──────────────
    # 借 V5Result 容器跑既有邏輯，避免重複實作
    proxy = V5Result()
    if p.pattern_b_use_n_pattern:
        pattern_b_hit = _pattern_b_via_n_pattern(df, today, p, proxy)
    else:
        pattern_b_hit = _pattern_b_via_pullback(df, p, proxy)
    res.pattern_b_hit = pattern_b_hit

    # ── 決定型態與 breakout_target ──────────────────────────────────
    if pattern_a_hit:
        res.pattern_type = "A"
        # A 型：今日要破近 10 日高（含等於），這是 checker 比對的目標
        res.breakout_target = window_high
    elif pattern_b_hit:
        res.pattern_type = "B"
        # B 型：N-字底成形，今日只要破昨高就點火
        res.breakout_target = res.prev_high
    else:
        return None

    return res


# ════════════════════════════════════════════════════════════════════════
# 盤中重驗：用現價當「今日 close」，檢查型態 B 守月線條件是否仍成立
# ════════════════════════════════════════════════════════════════════════
@dataclass
class PatternBIntradayCheck:
    hit: bool = False
    higher_low: bool = False        # 條件 1（用 EOD 結果）
    above_ma20: bool = False        # 條件 2（用現價重算）
    today_ma20: float = 0.0
    reason: str = ""                # 失效原因（debug / 推播略過時記錄用）


def verify_pattern_b_intraday(
    df_eod: pd.DataFrame,
    *,
    current_price: float,
    params: Optional[V5Params] = None,
) -> PatternBIntradayCheck:
    """
    盤中重驗型態 B：用即時現價當「今日 close」，檢查回檔型態是否仍成立。

    重驗範圍（與 _pattern_b_via_pullback 對應）：
      條件 1（底底高）：沿用 EOD 資料判定。
        盤中 low 受早盤殺低影響大，重驗反而誤殺；型態學上「底底高」是
        EOD 才能蓋棺的事，盤中不重判。
      條件 2（守月線）：把今日 close 換成現價，並重算今日 MA20
        （= 最近 19 日 EOD close + 現價 之平均），重新檢查過去 N 日
        每天的 close > 對應 MA20。

    為什麼不重驗條件 3、4（紅K + 站上MA5 / 破前日高）：
      - 破前日高已由 intraday checker 的 breakout_target 把關
      - 紅K / 站上MA5 在 v5 規格刻意盤中不檢查（雜訊大）

    Args:
        df_eod: 昨日為止的日 K（需含 close, low, ma20），最後一筆 = 昨日 EOD。
                來源建議：db.price_cache.load_prices(...) + scanner.compute_indicators(...)
        current_price: 今日即時現價
        params: 同 evaluate_v5；預設取 V5Params 預設值

    Returns:
        PatternBIntradayCheck — hit=False 時可從 reason 看失效原因
    """
    p = params or V5Params()
    chk = PatternBIntradayCheck()

    n_recent = p.pattern_b_recent_low_window
    n_prior = p.pattern_b_prior_low_window
    n_above = p.pattern_b_above_ma20_window

    if df_eod is None or df_eod.empty:
        chk.reason = "df_eod 為空"
        return chk
    needed = max(n_recent + n_prior + 1, 20)
    if len(df_eod) < needed:
        chk.reason = f"資料不足（需 {needed} 根，現有 {len(df_eod)} 根）"
        return chk
    for col in ("close", "low", "ma20"):
        if col not in df_eod.columns:
            chk.reason = f"缺欄位 {col}"
            return chk

    if current_price is None or pd.isna(current_price) or current_price <= 0:
        chk.reason = "current_price 無效"
        return chk

    # 條件 1：底底高（沿用 EOD）
    recent_low = float(df_eod["low"].iloc[-n_recent:].min())
    prior_low = float(df_eod["low"].iloc[-(n_recent + n_prior):-n_recent].min())
    chk.higher_low = recent_low > prior_low

    # 條件 2：守月線（今日 close = 現價，今日 MA20 重算）
    today_ma20 = (float(df_eod["close"].iloc[-19:].sum()) + float(current_price)) / 20.0
    chk.today_ma20 = round(today_ma20, 4)

    # 過去 N 日 = 「最近 (n_above - 1) 日 EOD」 + 「今日（現價）」
    eod_take = n_above - 1
    closes_eod = df_eod["close"].iloc[-eod_take:].astype(float).tolist() if eod_take > 0 else []
    ma20_eod = df_eod["ma20"].iloc[-eod_take:].astype(float).tolist() if eod_take > 0 else []
    if any(pd.isna(v) for v in ma20_eod):
        chk.reason = "EOD ma20 含 NaN"
        return chk

    closes_all = closes_eod + [float(current_price)]
    ma20_all = ma20_eod + [today_ma20]
    chk.above_ma20 = all(c > m for c, m in zip(closes_all, ma20_all))

    chk.hit = chk.higher_low and chk.above_ma20
    if not chk.hit and not chk.reason:
        if not chk.higher_low:
            chk.reason = "底底高失效（EOD 已破前低）"
        elif not chk.above_ma20:
            chk.reason = f"守月線失效（今日 MA20={today_ma20:.2f}，現價={current_price:.2f}）"
    return chk
