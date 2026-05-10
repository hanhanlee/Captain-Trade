"""
N 字底型態偵測器（日線結構版）

依據 ref/N字底_明確定義.md 的標準：
  A：第一低點 → B：反彈高點 → C：第二低點（C >= A）→ D：突破點（D > B）

本模組只負責找出最近一組「已成形」的 A/B/C，
由呼叫端在盤中比對現價是否大於 B（即 D 點成立，視為 N 字底完成）。

主要 API：
  find_abc_points(df, ...) -> NPattern | None
  is_breakout(current_price, b_price) -> bool
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional

import pandas as pd


@dataclass
class SwingPoint:
    index: int       # 在傳入 DataFrame（截斷後）裡的位置，0-based 升冪
    date: str        # YYYY-MM-DD
    price: float     # A/C 用 low；B 用 high


@dataclass
class VolumeProfile:
    """A→B 與 B→C 兩階段的量能特徵（不作為過濾條件，僅作為盤中通知附註）。"""
    avg_a_to_b: float | None        # A→B 區間平均量
    avg_b_to_c: float | None        # B→C 區間平均量
    a_to_b_volume_increase: bool    # A→B 量是否大於 A 之前等長區間
    b_to_c_volume_decrease: bool    # B→C 量是否小於 A→B
    pre_a_avg: float | None         # A 之前等長區間平均量（比較基準）

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class NPattern:
    A: SwingPoint
    B: SwingPoint
    C: SwingPoint
    b_rise_pct: float           # B 相對 A 的漲幅（%）
    c_retrace_pct: float        # C 從 B 回測 (B-A) 的百分比
    volume: VolumeProfile | None = None

    def to_dict(self) -> dict:
        return {
            "A": asdict(self.A),
            "B": asdict(self.B),
            "C": asdict(self.C),
            "b_rise_pct": self.b_rise_pct,
            "c_retrace_pct": self.c_retrace_pct,
            "volume": self.volume.to_dict() if self.volume else None,
        }


def _normalize_ohlc_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    將 FinMind 原始命名（max/min/Trading_Volume）對齊為通用名（high/low/volume）。
    若對應欄位都不存在，原樣回傳，由呼叫端的 needed 檢查擋下。
    """
    rename_map: dict[str, str] = {}
    if "high" not in df.columns and "max" in df.columns:
        rename_map["max"] = "high"
    if "low" not in df.columns and "min" in df.columns:
        rename_map["min"] = "low"
    if "volume" not in df.columns and "Trading_Volume" in df.columns:
        rename_map["Trading_Volume"] = "volume"
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def _compute_volume_profile(
    df: pd.DataFrame,
    a_idx: int,
    b_idx: int,
    c_idx: int,
) -> VolumeProfile | None:
    """
    依據 A/B/C 三點計算量能特徵：
      - A→B 平均量（含端點）
      - B→C 平均量（含端點）
      - 比較基準：A 之前等長度（與 A→B 相同根數）的平均量
    任一段資料不足或缺 volume 欄位則回傳 None。
    """
    if "volume" not in df.columns:
        return None
    try:
        vols = pd.to_numeric(df["volume"], errors="coerce")
    except Exception:
        return None
    if vols.isna().all():
        return None

    a_to_b = vols.iloc[a_idx : b_idx + 1].dropna()
    b_to_c = vols.iloc[b_idx : c_idx + 1].dropna()
    if a_to_b.empty or b_to_c.empty:
        return None

    avg_ab = float(a_to_b.mean())
    avg_bc = float(b_to_c.mean())

    pre_len = b_idx - a_idx + 1
    pre_start = max(0, a_idx - pre_len)
    pre_a = vols.iloc[pre_start:a_idx].dropna()
    pre_avg: float | None = float(pre_a.mean()) if not pre_a.empty else None

    return VolumeProfile(
        avg_a_to_b=avg_ab,
        avg_b_to_c=avg_bc,
        a_to_b_volume_increase=(pre_avg is not None and avg_ab > pre_avg),
        b_to_c_volume_decrease=(avg_bc < avg_ab),
        pre_a_avg=pre_avg,
    )


def _fractal_low_indices(lows: pd.Series, k: int) -> list[int]:
    """前後各 k 根都嚴格高於它的局部低點位置。"""
    n = len(lows)
    out: list[int] = []
    for i in range(k, n - k):
        pivot = lows.iloc[i]
        left = lows.iloc[i - k : i]
        right = lows.iloc[i + 1 : i + k + 1]
        if (left > pivot).all() and (right > pivot).all():
            out.append(i)
    return out


def _fractal_high_indices(highs: pd.Series, k: int) -> list[int]:
    """前後各 k 根都嚴格低於它的局部高點位置。"""
    n = len(highs)
    out: list[int] = []
    for i in range(k, n - k):
        pivot = highs.iloc[i]
        left = highs.iloc[i - k : i]
        right = highs.iloc[i + 1 : i + k + 1]
        if (left < pivot).all() and (right < pivot).all():
            out.append(i)
    return out


def _has_prior_downtrend(
    df: pd.DataFrame,
    a_index: int,
    lookback: int,
    min_drop_pct: float,
) -> bool:
    """A 點之前 lookback 根之內，最高收盤至 A 低點的跌幅是否達門檻。"""
    if a_index <= 0:
        return False
    start = max(0, a_index - lookback)
    prior = df["close"].iloc[start:a_index]
    if prior.empty:
        return False
    peak = float(prior.max())
    a_low = float(df["low"].iloc[a_index])
    if peak <= 0:
        return False
    drop_pct = (peak - a_low) / peak * 100
    return drop_pct >= min_drop_pct


def find_abc_points(
    df: pd.DataFrame,
    *,
    lookback: int = 60,
    fractal_k: int = 3,
    min_b_rise_pct: float = 5.0,
    min_c_retrace_pct: float = 30.0,
    require_prior_downtrend: bool = True,
    prior_downtrend_lookback: int = 10,
    prior_downtrend_min_drop_pct: float = 5.0,
) -> Optional[NPattern]:
    """
    從日 K 中找出最近一組已成形的 A→B→C。

    參數：
      df: 含 date/high/low/close 欄位的 DataFrame，需按日期升冪排序
      lookback: 只看最後 N 根 K
      fractal_k: 局部高/低點偵測的單側根數（k=3 即前後各 3 根都比它高/低）
      min_b_rise_pct: B 對 A 的最小漲幅（%）
      min_c_retrace_pct: C 從 B 回測 (B-A) 的最小百分比，用來排除微幅震盪
      require_prior_downtrend: A 之前是否需要有下跌段
      prior_downtrend_lookback: 回看根數
      prior_downtrend_min_drop_pct: 下跌段最小跌幅（%）

    回傳：
      最近一組通過所有條件的 NPattern；找不到回 None。
    """
    if df is None or df.empty:
        return None

    df = _normalize_ohlc_columns(df)
    needed = {"date", "high", "low", "close"}
    if not needed.issubset(df.columns):
        return None

    df = df.tail(lookback).reset_index(drop=True)
    if len(df) < fractal_k * 4 + 1:
        return None

    lows = df["low"].astype(float)
    highs = df["high"].astype(float)

    low_idx = _fractal_low_indices(lows, fractal_k)
    high_idx = _fractal_high_indices(highs, fractal_k)
    if not low_idx or not high_idx:
        return None

    # 從最新的 fractal low 當 C 開始往回找。
    # 緊鄰 C 之前的最後一個 fractal high 即為 B；緊鄰 B 之前的最後一個 fractal low 即為 A。
    for c_idx in reversed(low_idx):
        b_candidates = [h for h in high_idx if h < c_idx]
        if not b_candidates:
            continue
        b_idx = b_candidates[-1]

        a_candidates = [l for l in low_idx if l < b_idx]
        if not a_candidates:
            continue
        a_idx = a_candidates[-1]

        a_price = float(lows.iloc[a_idx])
        b_price = float(highs.iloc[b_idx])
        c_price = float(lows.iloc[c_idx])

        if a_price <= 0 or b_price <= 0:
            continue

        # 1) C 不破 A
        if c_price < a_price:
            continue

        # 2) B 對 A 的漲幅
        b_rise = (b_price - a_price) / a_price * 100
        if b_rise < min_b_rise_pct:
            continue

        # 3) C 回測幅度（避免抓到鋸齒整理）
        ab_range = b_price - a_price
        if ab_range <= 0:
            continue
        c_retrace = (b_price - c_price) / ab_range * 100
        if c_retrace < min_c_retrace_pct:
            continue

        # 4) A 點之前要有下跌段（定義第七節：先確認前面有一段明顯下跌）
        if require_prior_downtrend and not _has_prior_downtrend(
            df,
            a_idx,
            lookback=prior_downtrend_lookback,
            min_drop_pct=prior_downtrend_min_drop_pct,
        ):
            continue

        return NPattern(
            A=SwingPoint(index=a_idx, date=str(df["date"].iloc[a_idx]), price=a_price),
            B=SwingPoint(index=b_idx, date=str(df["date"].iloc[b_idx]), price=b_price),
            C=SwingPoint(index=c_idx, date=str(df["date"].iloc[c_idx]), price=c_price),
            b_rise_pct=b_rise,
            c_retrace_pct=c_retrace,
            volume=_compute_volume_profile(df, a_idx, b_idx, c_idx),
        )

    return None


def is_breakout(current_price: float | None, b_price: float | None) -> bool:
    """D > B 即視為突破（不加緩衝，由呼叫端附上『留意假突破』提醒）。"""
    if current_price is None or b_price is None:
        return False
    try:
        return float(current_price) > float(b_price)
    except (TypeError, ValueError):
        return False
