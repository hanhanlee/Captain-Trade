"""
盤中 v5 狙擊手版 watchlist 建構器

每日 08:50 由排程呼叫 build_watchlist()：
  1. 篩出科技股產業（同 v3 / N 字底定義）
  2. 讀日 K 快取 → 計算指標
  3. 跑 evaluate_v5_for_builder() — 只過靜態 gate（多頭排列 / MACD / 型態 A or B）
  4. 寫入 db.v5_sniper_watchlist；紀錄 breakout_target 供盤中 checker 比對

科技股波動較大，型態 A 的均線糾結度預設放寬到 4%（v3 用 3%）。
動態 gate（破昨高 / 量增 / 紅K / 漲幅 cap）一律不在此檢查，留給盤中 checker。

純函式 build_from_data() 可在不打 DB / 不打 API 的情況下被單元測試。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import pandas as pd

from modules.scanner import compute_indicators
from modules.v5_sniper import V5Params, evaluate_v5_for_builder

logger = logging.getLogger(__name__)

TECH_INDUSTRIES: tuple[str, ...] = (
    "半導體業",
    "電子零組件業",
    "電腦及週邊設備業",
    "光電業",
    "通信網路業",
    "電子通路業",
    "資訊服務業",
    "其他電子業",
    "電子工業",
)


@dataclass
class BuildStats:
    scanned: int = 0
    pattern_a: int = 0
    pattern_b: int = 0
    written: int = 0
    skipped_no_data: int = 0
    skipped_gates_failed: int = 0   # 靜態 gate 沒過或無型態命中
    errors: int = 0

    def as_dict(self) -> dict:
        return {
            "scanned": self.scanned,
            "pattern_a": self.pattern_a,
            "pattern_b": self.pattern_b,
            "written": self.written,
            "skipped_no_data": self.skipped_no_data,
            "skipped_gates_failed": self.skipped_gates_failed,
            "errors": self.errors,
        }


def build_from_data(
    *,
    price_data: dict[str, pd.DataFrame],
    stock_info: dict[str, dict],
    pattern_a_squeeze_pct: float = 4.0,
    max_gain_pct: Optional[float] = None,
    scan_date: Optional[date] = None,
) -> tuple[list[dict], BuildStats]:
    """
    純函式版：對每檔股票跑 evaluate_v5_for_builder，產出 watchlist 候選清單。

    Args:
        price_data: {stock_id: 日 K DataFrame}（未計算指標也可，內部會跑 compute_indicators）
        stock_info: {stock_id: {stock_name, industry_category}}
        pattern_a_squeeze_pct: 型態 A 的糾結度門檻，科技股預設 4%
        max_gain_pct: 漲幅 cap（會寫入 DB 供盤中 checker 用，None = 不啟用）
        scan_date: 給 n_pattern_detector 判斷 C 點是否太舊用；預設今日

    回傳 (candidates, stats)。
    每筆 candidate 已含 db.v5_sniper_watchlist.upsert_candidates() 所需欄位。
    """
    candidates: list[dict] = []
    stats = BuildStats()
    sd = scan_date or date.today()

    params = V5Params(
        pattern_a_squeeze_pct=pattern_a_squeeze_pct,
        max_gain_pct=max_gain_pct,
    )

    for sid, df in price_data.items():
        if df is None or df.empty or len(df) < 60:
            stats.skipped_no_data += 1
            continue
        stats.scanned += 1
        try:
            df_ind = compute_indicators(df)
        except Exception as exc:
            logger.warning("compute_indicators 失敗 %s: %s", sid, exc)
            stats.errors += 1
            continue

        try:
            res = evaluate_v5_for_builder(df_ind, today=sd, params=params)
        except Exception as exc:
            logger.warning("evaluate_v5_for_builder 失敗 %s: %s", sid, exc)
            stats.errors += 1
            continue

        if res is None:
            stats.skipped_gates_failed += 1
            continue

        info = stock_info.get(sid, {}) or {}
        candidates.append({
            "stock_id": sid,
            "stock_name": info.get("stock_name"),
            "industry": info.get("industry_category"),
            "pattern_type": res.pattern_type,
            "breakout_target": round(res.breakout_target, 2),
            "prev_high": round(res.prev_high, 2),
            "prev_close": round(res.prev_close, 2),
            "prev_volume": round(res.prev_volume_lots, 0),  # 已轉「張」
            "vol_ma5": round(res.vol_ma5_lots, 0) if res.vol_ma5_lots else None,
            "ma5": round(res.ma5, 2),
            "ma10": round(res.ma10, 2),
            "ma20": round(res.ma20, 2),
            "ma60": round(res.ma60, 2),
            "ma_spread_pct": res.ma_spread_pct,
            "max_gain_pct": max_gain_pct,
        })
        if res.pattern_type == "A":
            stats.pattern_a += 1
        elif res.pattern_type == "B":
            stats.pattern_b += 1
        stats.written += 1

    return candidates, stats


def build_watchlist(
    *,
    industries: tuple[str, ...] | list[str] | None = None,
    pattern_a_squeeze_pct: float = 4.0,
    max_gain_pct: Optional[float] = None,
    lookback_days: int = 120,
) -> dict:
    """
    生產用入口：拉科技股日 K → 過濾 → 寫入 DB。
    回傳 stats dict 供排程記錄 / Telegram 摘要使用。

    Args:
        industries: 產業範圍，預設 TECH_INDUSTRIES
        pattern_a_squeeze_pct: 型態 A 糾結度門檻（%）
        max_gain_pct: 漲幅 cap（會存入每筆 row，盤中 checker 可讀取）
        lookback_days: 取多少天日 K；n_pattern_detector 預設看 80 根，
                       加上 MA60 + 緩衝，預設 120 天足夠
    """
    from data.finmind_client import get_stock_list
    from db.price_cache import load_prices_multi
    from db import v5_sniper_watchlist as wl

    inds = tuple(industries) if industries is not None else TECH_INDUSTRIES

    info_df = get_stock_list()
    if info_df is None or info_df.empty:
        logger.warning("get_stock_list() 回空，v5 watchlist 跳過")
        return {"scanned": 0, "written": 0, "errors": 1}

    info_df = info_df[info_df["industry_category"].isin(inds)].copy()
    sids = info_df["stock_id"].astype(str).tolist()
    if not sids:
        logger.warning("無符合產業的股票，v5 watchlist 跳過")
        return {"scanned": 0, "written": 0, "errors": 0}

    stock_info = {
        str(r["stock_id"]): {
            "stock_name": r.get("stock_name"),
            "industry_category": r.get("industry_category"),
        }
        for _, r in info_df.iterrows()
    }

    today = date.today()
    # MA60 + n_pattern 80 根 lookback，給足緩衝
    start_date = (today - timedelta(days=int(lookback_days * 1.4) + 10)).isoformat()
    price_data = load_prices_multi(sids, start_date=start_date)

    candidates, stats = build_from_data(
        price_data=price_data,
        stock_info=stock_info,
        pattern_a_squeeze_pct=pattern_a_squeeze_pct,
        max_gain_pct=max_gain_pct,
        scan_date=today,
    )

    wl.purge_old(older_than_days=7)
    wl.clear_today()
    wl.upsert_candidates(candidates, scan_date=today)

    logger.info(
        "v5 watchlist 建立完成：scanned=%d, pattern_a=%d, pattern_b=%d, written=%d",
        stats.scanned, stats.pattern_a, stats.pattern_b, stats.written,
    )
    return stats.as_dict()
