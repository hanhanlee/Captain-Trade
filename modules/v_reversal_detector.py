"""
搶 V 型反轉偵測器 —— 急跌 → 低檔爆量 → 止跌K →（待）過高確認 + KD 低檔金叉。

四個必備條件（對應使用者定義）：
  ① 急跌：從近 60 日收盤高回落 ≥20% 「或」近 10 日內下跌 ≥6 天（OR，涵蓋較廣）。
  ② 低檔爆量：低檔區某日量 ≥ 前 5 日均量 ×1.5（沿用 scanner「量能爆發」定義）。
  ③ 止跌K：爆量日後（含）3 日內出現 十字（實體比 ≤0.30）或紅K；取其最高點為進場觸發價。
  ④ 過高確認（進場點）：紅K 收盤/現價 > 止跌K 最高點。（由盤中 / 收盤模組確認，本模組不判）
  KD：低檔（K<30）黃金交叉（K 上穿 D）—— 必要條件。低檔於建候選時要求；金叉在④確認時檢。

本模組只做「①②③ + KD 低檔」的型態成形偵測（盤前建候選用），回傳止跌K高當觸發價。
數值沿用既有策略慣例：爆量 1.5×前5日均量、視窗 60/20 日、實體比公式同 v5、KD(9,3,3)。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

from modules.indicators import kdj


@dataclass(frozen=True)
class VReversalParams:
    # ① 急跌（OR）
    drop_lookback: int = 60          # 從近 N 日收盤高回落
    drop_pct_min: float = 20.0       # 回落 ≥ 此 %（跌深）
    down_days_window: int = 10       # 近 N 日
    down_days_min: int = 6           # 內下跌 ≥ M 天（急跌動能）
    # ② 低檔爆量
    vol_spike_mult: float = 1.5      # 今量 ÷ 前 5 日均量（沿用 scanner volume_explosion）
    low_zone_window: int = 20        # 低檔區判定視窗
    low_zone_pctl: float = 0.30      # 爆量日收盤落在近 N 日區間下緣 < 此分位
    # ③ 止跌K
    stabilize_within: int = 3        # 爆量日後（含）N 日內須出現止跌K
    doji_body_max: float = 0.30      # 十字：|close-open| / (high-low) ≤ 此值
    stabilize_fresh_days: int = 2    # 止跌K 須落在最近 N 根（不追舊訊號）
    # KD
    kd_n: int = 9
    kd_m1: int = 3
    kd_m2: int = 3
    kd_low: float = 30.0             # 低檔：K < 此值


@dataclass
class VReversalPattern:
    # 急跌
    drop_pct: float                  # 從近 60 日高的回落 %（負值）
    down_days: int                   # 近 10 日下跌天數
    sharp_by_drop: bool              # 是否因「跌深」成立
    sharp_by_days: bool              # 是否因「連跌」成立
    # 爆量
    spike_date: Optional[object]     # 爆量日
    spike_vol_ratio: float           # 爆量日 量 / 前5日均量
    # 止跌K
    stabilize_date: Optional[object] # 止跌K 日
    stabilize_is_doji: bool
    stabilize_is_red: bool
    trigger_price: float             # 止跌K 最高點 = 進場觸發價
    # KD（止跌K 當日）
    kd_k: float
    kd_d: float
    kd_low: bool                     # K < kd_low 門檻
    kd_golden_cross: bool            # 止跌K 當日是否已 K 上穿 D
    # 現況
    last_close: float
    distance_to_trigger_pct: float   # 現價距觸發價還差 %（負值 = 已突破）
    already_broken: bool             # 現價是否已 ≥ 觸發價（late）

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        for k in ("spike_date", "stabilize_date"):
            v = d.get(k)
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
            elif v is not None:
                d[k] = str(v)
        return d


def _resolve_cols(df: pd.DataFrame) -> Optional[dict]:
    """容錯 FinMind（max/min/Trading_Volume）與通用命名。缺必要欄位回 None。"""
    high = "high" if "high" in df.columns else ("max" if "max" in df.columns else None)
    low = "low" if "low" in df.columns else ("min" if "min" in df.columns else None)
    vol = "Trading_Volume" if "Trading_Volume" in df.columns else (
        "volume" if "volume" in df.columns else None)
    if high is None or low is None or "close" not in df.columns or "open" not in df.columns:
        return None
    return {"high": high, "low": low, "vol": vol, "close": "close", "open": "open"}


def _body_ratio(o: float, h: float, l: float, c: float) -> float:
    """實體比 = |close-open| / (high-low)；同 v5 定義。全幅 0 時回 0（視為十字）。"""
    rng = h - l
    if rng <= 0:
        return 0.0
    return abs(c - o) / rng


def find_v_reversal(df: pd.DataFrame, p: VReversalParams = VReversalParams()) -> Optional[VReversalPattern]:
    """偵測「①②③ + KD 低檔」是否成形（以 df 最後一根為現況）。找不到回 None。

    回傳的 trigger_price = 止跌K 最高點，供盤中 / 收盤確認「過高進場」用。
    already_broken=True 表示現價已越過觸發價（盤中補抓 / 收盤確認會處理 late）。
    """
    cols = _resolve_cols(df)
    if cols is None or len(df) < max(p.drop_lookback, p.low_zone_window) + 2:
        return None

    d = df.reset_index(drop=True)
    close = d[cols["close"]].astype(float)
    open_ = d[cols["open"]].astype(float)
    high = d[cols["high"]].astype(float)
    low = d[cols["low"]].astype(float)
    vol = d[cols["vol"]].astype(float) if cols["vol"] else None
    if vol is None or vol.isna().all():
        return None

    last = len(d) - 1
    dates = d["date"] if "date" in d.columns else pd.Series(range(len(d)))

    # ── ① 急跌（OR）──────────────────────────────────────────────
    hi_window = close.iloc[-p.drop_lookback:]
    peak = float(hi_window.max())
    last_close = float(close.iloc[last])
    drop_pct = (last_close - peak) / peak * 100 if peak > 0 else 0.0
    down_days = int((close.diff() < 0).iloc[-p.down_days_window:].sum())
    sharp_by_drop = drop_pct <= -p.drop_pct_min
    sharp_by_days = down_days >= p.down_days_min
    if not (sharp_by_drop or sharp_by_days):
        return None

    # 前 5 日均量（不含當日：與 scanner vol_ma5 一致用 rolling(5) 含當日；此處沿用含當日）
    vol_ma5 = vol.rolling(5).mean()

    # ── KD(9,3,3) ────────────────────────────────────────────────
    k_ser, d_ser, _ = kdj(d, p.kd_n, p.kd_m1, p.kd_m2)

    # 20 日低檔區間（判斷爆量日是否在低檔）
    roll_lo = low.rolling(p.low_zone_window).min()
    roll_hi = high.rolling(p.low_zone_window).max()

    def _is_low_zone(i: int) -> bool:
        lo, hi = roll_lo.iloc[i], roll_hi.iloc[i]
        if pd.isna(lo) or pd.isna(hi) or hi <= lo:
            return False
        pos = (close.iloc[i] - lo) / (hi - lo)
        return pos < p.low_zone_pctl

    def _is_spike(i: int) -> tuple[bool, float]:
        base = vol_ma5.iloc[i]
        if pd.isna(base) or base <= 0:
            return False, 0.0
        ratio = float(vol.iloc[i]) / float(base)
        return ratio >= p.vol_spike_mult, ratio

    # ── ③ 止跌K 須在最近 stabilize_fresh_days 根；往回找對應的低檔爆量日 ──
    for j in range(last, last - p.stabilize_fresh_days, -1):
        if j < 1:
            break
        o, h, l, c = float(open_.iloc[j]), float(high.iloc[j]), float(low.iloc[j]), float(close.iloc[j])
        is_doji = _body_ratio(o, h, l, c) <= p.doji_body_max
        is_red = c > o
        if not (is_doji or is_red):
            continue

        # 爆量日：止跌K 當日或其前 stabilize_within 日內，且在低檔區
        spike_idx = None
        spike_ratio = 0.0
        for i in range(j, max(j - p.stabilize_within, -1), -1):
            ok, ratio = _is_spike(i)
            if ok and _is_low_zone(i):
                spike_idx = i
                spike_ratio = ratio
                break
        if spike_idx is None:
            continue

        # KD 低檔（止跌K 當日 K < 門檻）
        kj = float(k_ser.iloc[j]) if pd.notna(k_ser.iloc[j]) else float("nan")
        dj = float(d_ser.iloc[j]) if pd.notna(d_ser.iloc[j]) else float("nan")
        if pd.isna(kj) or kj >= p.kd_low:
            continue
        golden = (
            pd.notna(k_ser.iloc[j - 1]) and pd.notna(d_ser.iloc[j - 1])
            and float(k_ser.iloc[j - 1]) <= float(d_ser.iloc[j - 1]) and kj > dj
        )

        trigger = float(high.iloc[j])
        dist = (last_close - trigger) / trigger * 100 if trigger > 0 else 0.0
        return VReversalPattern(
            drop_pct=round(drop_pct, 2),
            down_days=down_days,
            sharp_by_drop=sharp_by_drop,
            sharp_by_days=sharp_by_days,
            spike_date=dates.iloc[spike_idx],
            spike_vol_ratio=round(spike_ratio, 2),
            stabilize_date=dates.iloc[j],
            stabilize_is_doji=is_doji,
            stabilize_is_red=is_red,
            trigger_price=round(trigger, 2),
            kd_k=round(kj, 2),
            kd_d=round(dj, 2),
            kd_low=True,
            kd_golden_cross=bool(golden),
            last_close=round(last_close, 2),
            distance_to_trigger_pct=round(dist, 2),
            already_broken=last_close >= trigger,
        )

    return None
