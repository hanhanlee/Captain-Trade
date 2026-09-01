"""
搶 V 型反轉候選清單 builder（比照 n_pattern_watchlist_builder）。

盤後/盤前掃全市場日 K → find_v_reversal 找出「①②③ + KD 低檔」成形、且尚未遠離
止跌K高的股票 → 寫入 db.v_reversal_watchlist，供盤中即時突破確認使用。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import date, timedelta

import pandas as pd

from modules.v_reversal_detector import VReversalParams, find_v_reversal

logger = logging.getLogger(__name__)


@dataclass
class BuildStats:
    scanned: int = 0
    hits: int = 0
    written: int = 0
    skipped_far: int = 0            # 現價距觸發價太遠（還沒接近）
    skipped_broken_far: int = 0     # 已遠遠突破（追進效益低）
    errors: int = 0

    def as_dict(self) -> dict:
        return asdict(self)


def build_from_data(
    *,
    price_data: dict[str, pd.DataFrame],
    stock_info: dict[str, dict],
    params: VReversalParams | None = None,
    max_distance_below_pct: float = 8.0,   # 現價可在觸發價下方多少 %（越接近越好）
    max_above_pct: float = 3.0,            # 現價已突破觸發價多少 % 內仍收（剛突破）
) -> tuple[list[dict], BuildStats]:
    """純函式：給價量 + 基本資訊 → 候選清單 + 統計。"""
    p = params or VReversalParams()
    candidates: list[dict] = []
    stats = BuildStats()

    for sid, df in price_data.items():
        if df is None or df.empty or len(df) < 70:
            continue
        stats.scanned += 1
        try:
            pat = find_v_reversal(df, p)
        except Exception as exc:
            logger.warning("v_reversal detector 失敗 %s: %s", sid, exc)
            stats.errors += 1
            continue
        if pat is None:
            continue
        stats.hits += 1

        # distance: 負值=在觸發價下方(等突破)；正值=已突破
        dist = pat.distance_to_trigger_pct
        if dist < -abs(max_distance_below_pct):
            stats.skipped_far += 1
            continue
        if dist > abs(max_above_pct):
            stats.skipped_broken_far += 1
            continue

        # 前 5 日均量（張）：盤中流動性 gate 基準（股 ÷ 1000）
        vol_ma5_lots = None
        vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else (
            "volume" if "volume" in df.columns else None)
        if vol_col is not None:
            try:
                vs = pd.to_numeric(df[vol_col].iloc[-5:], errors="coerce").dropna()
                if not vs.empty:
                    vol_ma5_lots = float(vs.mean()) / 1000.0
            except Exception:
                vol_ma5_lots = None

        info = stock_info.get(sid, {}) or {}
        candidates.append({
            "stock_id": sid,
            "stock_name": info.get("stock_name"),
            "industry": info.get("industry_category"),
            "drop_pct": pat.drop_pct,
            "down_days": pat.down_days,
            "sharp_by_drop": pat.sharp_by_drop,
            "sharp_by_days": pat.sharp_by_days,
            "spike_date": _as_date(pat.spike_date),
            "spike_vol_ratio": pat.spike_vol_ratio,
            "stabilize_date": _as_date(pat.stabilize_date),
            "stabilize_is_doji": pat.stabilize_is_doji,
            "stabilize_is_red": pat.stabilize_is_red,
            "trigger_price": pat.trigger_price,
            "kd_k": pat.kd_k,
            "kd_d": pat.kd_d,
            "kd_low": pat.kd_low,
            "kd_golden_cross": pat.kd_golden_cross,
            "vol_ma5": vol_ma5_lots,
            "last_close": pat.last_close,
            "distance_to_trigger_pct": dist,
            "entry_path": "morning",
        })
        stats.written += 1

    return candidates, stats


def build_watchlist(
    *,
    industries: tuple[str, ...] | list[str] | None = None,
    params: VReversalParams | None = None,
    max_distance_below_pct: float = 8.0,
    max_above_pct: float = 3.0,
) -> dict:
    """生產入口：拉股票清單 + 日K 快取 → 過濾 → 寫 DB。回傳 stats dict。

    industries=None → 全市場（抄底反轉不限產業）；傳入 tuple 則限定產業。
    """
    from data.finmind_client import get_stock_list
    from db.price_cache import load_prices_multi
    from db import v_reversal_watchlist as wl

    info_df = get_stock_list()
    if info_df is None or info_df.empty:
        logger.warning("get_stock_list() 回空，V 反 watchlist 跳過")
        return {"scanned": 0, "hits": 0, "written": 0, "errors": 1}

    if industries:
        info_df = info_df[info_df["industry_category"].isin(tuple(industries))].copy()
    # 只掃 4 位數普通股（排除 ETF / 權證 / 特別股等）
    info_df = info_df[info_df["stock_id"].astype(str).str.fullmatch(r"\d{4}")].copy()
    sids = info_df["stock_id"].astype(str).tolist()
    if not sids:
        return {"scanned": 0, "hits": 0, "written": 0, "errors": 0}

    stock_info = {
        str(r["stock_id"]): {
            "stock_name": r.get("stock_name"),
            "industry_category": r.get("industry_category"),
        }
        for _, r in info_df.iterrows()
    }

    # detector 需近 60 日以上；抓約 90 個交易日的日曆範圍（含連假保險）
    today = date.today()
    start_date = (today - timedelta(days=140)).isoformat()
    price_data = load_prices_multi(sids, start_date=start_date)

    candidates, stats = build_from_data(
        price_data=price_data,
        stock_info=stock_info,
        params=params,
        max_distance_below_pct=max_distance_below_pct,
        max_above_pct=max_above_pct,
    )

    wl.purge_old(older_than_days=7)
    wl.purge_alert_history(older_than_days=30)
    wl.clear_today(preserve_alerted=True)
    wl.upsert_candidates(candidates, scan_date=today)

    return stats.as_dict()


def _as_date(v):
    if v is None:
        return None
    if hasattr(v, "date") and not isinstance(v, date):
        try:
            return pd.Timestamp(v).date()
        except Exception:
            return None
    return v
