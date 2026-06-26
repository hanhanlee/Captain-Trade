"""
盤中 V3 三線齊穿 watchlist 建構器

每日 08:50 由排程呼叫 build_watchlist()：
  1. 篩出科技股產業（同 N 字底定義）
  2. 讀日 K 快取 → 計算指標
  3. 過濾條件（昨收 < MA5 且 < MA10 且 < MA20，即三線全線以下 + 三線糾結度 < 3%）
  4. 寫入 db.v3_breakout_watchlist

純函式 build_from_data() 可在不打 DB / 不打 API 的情況下被單元測試。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from modules.scanner import compute_indicators

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
    written: int = 0
    skipped_no_vol: int = 0          # 無成交量欄位
    skipped_not_below_ma: int = 0    # 昨收未在三線全線以下（已突破或部分突破）
    skipped_no_squeeze: int = 0      # 三線未糾結（spread >= 3%）
    errors: int = 0

    def as_dict(self) -> dict:
        return {
            "scanned": self.scanned,
            "written": self.written,
            "skipped_not_below_ma": self.skipped_not_below_ma,
            "skipped_no_squeeze": self.skipped_no_squeeze,
            "skipped_no_vol": self.skipped_no_vol,
            "errors": self.errors,
        }


def build_from_data(
    *,
    price_data: dict[str, pd.DataFrame],
    stock_info: dict[str, dict],
    ma_squeeze_threshold: float = 3.0,
) -> tuple[list[dict], BuildStats]:
    """
    純函式版：掃描所有股票，找出「昨收在三線全線以下 + 三線糾結」的候選。

    昨收必須 < MA5 且 < MA10 且 < MA20，確保盤中突破是真正「從三線下齊穿到三線上」，
    而非昨日已部分站上、盤中只補站最後一條線。

    回傳 (candidates, stats)。
    candidates 每筆含：stock_id, stock_name, industry, ma5, ma10, ma20,
                       vol_ma5, last_close, ma_spread_pct
    """
    candidates: list[dict] = []
    stats = BuildStats()

    for sid, df in price_data.items():
        if df is None or df.empty or len(df) < 30:
            continue
        stats.scanned += 1
        try:
            df = compute_indicators(df)
        except Exception as exc:
            logger.warning("compute_indicators 失敗 %s: %s", sid, exc)
            stats.errors += 1
            continue

        vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
        if vol_col is None or df[vol_col].isna().all():
            stats.skipped_no_vol += 1
            continue

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # 所有 MA 均需有效
        for col in ("ma5", "ma10", "ma20"):
            if pd.isna(latest.get(col)) or pd.isna(prev.get(col)):
                stats.errors += 1
                break
        else:
            pass

        if any(pd.isna(latest.get(c)) or pd.isna(prev.get(c))
               for c in ("ma5", "ma10", "ma20")):
            stats.errors += 1
            continue

        # 條件 1：昨收必須在三線全線以下（等待突破而非已突破）
        # 建表在 08:50，DB 裡最新一筆即昨日收盤
        last_close = float(latest["close"])
        ma5 = float(latest["ma5"])
        ma10 = float(latest["ma10"])
        ma20 = float(latest["ma20"])

        if not (last_close < ma5 and last_close < ma10 and last_close < ma20):
            stats.skipped_not_below_ma += 1
            continue

        # 條件 2：三線糾結度 < threshold（用昨日，即最後一筆）
        spread_pct = (max(ma5, ma10, ma20) - min(ma5, ma10, ma20)) / ma20 * 100
        if spread_pct >= ma_squeeze_threshold:
            stats.skipped_no_squeeze += 1
            continue

        # 前五日均量（vol_ma5 已在 compute_indicators 計算）
        vol_ma5_val = latest.get("vol_ma5")
        if pd.isna(vol_ma5_val) or float(vol_ma5_val) <= 0:
            stats.skipped_no_vol += 1
            continue

        info = stock_info.get(sid, {}) or {}
        candidates.append({
            "stock_id": sid,
            "stock_name": info.get("stock_name"),
            "industry": info.get("industry_category"),
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            # Shioaji 盤中 total_volume 單位為「張」，故 vol_ma5 也存「張」對齊 V5
            "vol_ma5": round(float(vol_ma5_val) / 1000, 0),
            "last_close": last_close,
            "ma_spread_pct": round(spread_pct, 2),
        })
        stats.written += 1

    return candidates, stats


def build_watchlist(
    *,
    industries: tuple[str, ...] | list[str] | None = None,
    ma_squeeze_threshold: float = 3.0,
    lookback_days: int = 40,
) -> dict:
    """
    生產用入口：拉科技股日 K → 過濾 → 寫入 DB。
    回傳 stats dict 供排程記錄 / Telegram 摘要使用。
    """
    from data.finmind_client import get_stock_list
    from db.price_cache import load_prices_multi
    from db import v3_breakout_watchlist as wl

    inds = tuple(industries) if industries is not None else TECH_INDUSTRIES

    info_df = get_stock_list()
    if info_df is None or info_df.empty:
        logger.warning("get_stock_list() 回空，V3 watchlist 跳過")
        return {"scanned": 0, "written": 0, "errors": 1}

    info_df = info_df[info_df["industry_category"].isin(inds)].copy()
    sids = info_df["stock_id"].astype(str).tolist()
    if not sids:
        logger.warning("無符合產業的股票，V3 watchlist 跳過")
        return {"scanned": 0, "written": 0, "errors": 0}

    stock_info = {
        str(r["stock_id"]): {
            "stock_name": r.get("stock_name"),
            "industry_category": r.get("industry_category"),
        }
        for _, r in info_df.iterrows()
    }

    today = date.today()
    # MA20 需要 20 個交易日，加保險緩衝
    start_date = (today - timedelta(days=int(lookback_days * 1.4) + 10)).isoformat()
    price_data = load_prices_multi(sids, start_date=start_date)

    candidates, stats = build_from_data(
        price_data=price_data,
        stock_info=stock_info,
        ma_squeeze_threshold=ma_squeeze_threshold,
    )

    wl.purge_old(older_than_days=7)
    # 保留盤中已推播的列：當天重建（手動或服務重啟在 misfire grace 內二次觸發）
    # 不要把已推播的票洗掉、也不要把 alerted 旗標重置成未推播。
    wl.clear_today(preserve_alerted=True)
    wl.upsert_candidates(candidates, scan_date=today)

    return stats.as_dict()
