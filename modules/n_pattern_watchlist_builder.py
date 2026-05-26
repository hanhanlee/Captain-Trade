"""
盤中 N 字底 watchlist 建構器

執行流程（每日 8:50 前由排程呼叫 build_watchlist）：
  1. 從 stock_info_cache 篩出指定產業（預設科技股）
  2. 每檔讀日 K 快取 → 跑 n_pattern_detector
  3. 套用「actionable」過濾：
       - C 形成日距今不超過 max_c_age_days
       - 現價距 B 還在 max_distance_to_b_pct 以內（含已突破當天）
  4. 寫入 db.n_pattern_watchlist

純函式 build_from_data() 可在不打 DB / 不打 API 的情況下被單元測試。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from modules.n_pattern_detector import find_abc_points

logger = logging.getLogger(__name__)


# 上市櫃可視為「科技股」的產業分類（FinMind TaiwanStockInfo 命名）
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
    scanned: int = 0       # 進入偵測的股票數
    hits: int = 0          # detector 找到 A/B/C 的數量
    written: int = 0       # 通過 actionable 過濾、寫入 DB 的數量
    skipped_old_c: int = 0 # 因 C 太舊而剔除
    skipped_far: int = 0   # 因現價距 B 太遠而剔除
    skipped_already_broken: int = 0  # 已遠遠突破 B（多單追不上）→ 剔除
    skipped_deep_retrace: int = 0    # C 回測過深（>=max_c_retrace_pct）→ 剔除
    errors: int = 0

    def as_dict(self) -> dict:
        return {
            "scanned": self.scanned,
            "hits": self.hits,
            "written": self.written,
            "skipped_old_c": self.skipped_old_c,
            "skipped_far": self.skipped_far,
            "skipped_already_broken": self.skipped_already_broken,
            "skipped_deep_retrace": self.skipped_deep_retrace,
            "errors": self.errors,
        }


def _parse_date(value) -> Optional[date]:
    """容錯：DataFrame 內 date 欄位可能是 str / datetime / pandas Timestamp。"""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def build_from_data(
    *,
    price_data: dict[str, pd.DataFrame],
    stock_info: dict[str, dict],
    today: date,
    fractal_k: int = 3,
    min_b_rise_pct: float = 5.0,
    min_c_retrace_pct: float = 30.0,
    max_c_retrace_pct: float = 50.0,
    require_prior_downtrend: bool = True,
    lookback: int = 80,
    max_c_age_days: int = 15,
    max_distance_to_b_pct: float = 8.0,
    max_above_b_pct: float = 1.5,
) -> tuple[list[dict], BuildStats]:
    """
    純函式版：給定價量資料與股票基本資訊，產生候選清單。

    參數：
      price_data: {stock_id: DataFrame(date/open/high/low/close/volume)}
                  欄位 max/min/Trading_Volume 也接受（detector 會自動對齊）
      stock_info: {stock_id: {'stock_name': str, 'industry_category': str}}
      today: 用來計算 C 點離今天幾天
      max_c_age_days: C 形成後超過這個交易日數則視為過時，剔除
      max_c_retrace_pct: C 回測超過此百分比則視為回調過深，剔除（預設 50%）
      max_distance_to_b_pct: 現價需在 B 下方這個 % 以內（即將突破才有意義）
      max_above_b_pct: 現價已超過 B 多少 % 之內仍可加入名單（剛突破第一天）
                      超過則視為已遠遠突破、追進效益低，剔除

    回傳 (candidates, stats)；candidates 即可丟給 db.n_pattern_watchlist.upsert_candidates。
    """
    candidates: list[dict] = []
    stats = BuildStats()

    for sid, df in price_data.items():
        if df is None or df.empty or len(df) < 30:
            continue
        stats.scanned += 1
        try:
            pat = find_abc_points(
                df,
                lookback=lookback,
                fractal_k=fractal_k,
                min_b_rise_pct=min_b_rise_pct,
                min_c_retrace_pct=min_c_retrace_pct,
                require_prior_downtrend=require_prior_downtrend,
            )
        except Exception as exc:
            logger.warning("detector 失敗 %s: %s", sid, exc)
            stats.errors += 1
            continue

        if pat is None:
            continue
        stats.hits += 1

        # C 回測過深過濾：回測比例太高代表反彈力道不足，勝率較低
        if pat.c_retrace_pct >= max_c_retrace_pct:
            stats.skipped_deep_retrace += 1
            continue

        c_d = _parse_date(pat.C.date)
        if c_d is None:
            stats.errors += 1
            continue

        # actionable 過濾 1：C 不能太舊
        c_age_days = (today - c_d).days
        if c_age_days > max_c_age_days:
            stats.skipped_old_c += 1
            continue

        # 取最新收盤
        # detector 會自動處理 max/min 命名，但這裡需要 close 跟 volume 還是原表的；正常 close 都在
        try:
            last_close = float(df["close"].iloc[-1])
        except Exception:
            stats.errors += 1
            continue

        b_price = pat.B.price
        if b_price <= 0:
            continue

        # actionable 過濾 2：現價距 B
        distance_pct = (b_price - last_close) / b_price * 100   # 正值=還沒到 B；負值=已過 B
        if distance_pct > max_distance_to_b_pct:
            stats.skipped_far += 1
            continue
        if distance_pct < -abs(max_above_b_pct):
            stats.skipped_already_broken += 1
            continue

        info = stock_info.get(sid, {}) or {}
        vol = pat.volume

        # 前 5 日均量（張）：盤中流動性 gate 的基準；單位「股」÷1000 = 張
        vol_ma5_lots: float | None = None
        vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else (
            "volume" if "volume" in df.columns else None
        )
        if vol_col is not None:
            try:
                vol_series = pd.to_numeric(df[vol_col].iloc[-5:], errors="coerce").dropna()
                if not vol_series.empty:
                    vol_ma5_lots = float(vol_series.mean()) / 1000.0
            except Exception:
                vol_ma5_lots = None

        candidates.append({
            "stock_id": sid,
            "stock_name": info.get("stock_name"),
            "industry": info.get("industry_category"),
            "a_date": _parse_date(pat.A.date),
            "a_price": pat.A.price,
            "b_date": _parse_date(pat.B.date),
            "b_price": pat.B.price,
            "c_date": c_d,
            "c_price": pat.C.price,
            "b_rise_pct": pat.b_rise_pct,
            "c_retrace_pct": pat.c_retrace_pct,
            "vol_a_to_b_increase": bool(vol.a_to_b_volume_increase) if vol else None,
            "vol_b_to_c_decrease": bool(vol.b_to_c_volume_decrease) if vol else None,
            "avg_volume_b_to_c": float(vol.avg_b_to_c) if vol and vol.avg_b_to_c is not None else None,
            "vol_ma5": vol_ma5_lots,
            "last_close": last_close,
            "distance_to_b_pct": distance_pct,
        })
        stats.written += 1

    return candidates, stats


def build_watchlist(
    *,
    industries: tuple[str, ...] | list[str] | None = None,
    fractal_k: int = 3,
    min_b_rise_pct: float = 5.0,
    min_c_retrace_pct: float = 30.0,
    max_c_retrace_pct: float = 50.0,
    require_prior_downtrend: bool = True,
    lookback: int = 80,
    max_c_age_days: int = 15,
    max_distance_to_b_pct: float = 8.0,
    max_above_b_pct: float = 1.5,
) -> dict:
    """
    生產用入口：拉股票清單 + 日 K 快取 → 過濾 → 寫入 DB。
    回傳 stats dict 給排程記錄 / Telegram 摘要訊息使用。
    """
    from data.finmind_client import get_stock_list
    from db.price_cache import load_prices_multi
    from db import n_pattern_watchlist as wl

    inds = tuple(industries) if industries is not None else TECH_INDUSTRIES

    info_df = get_stock_list()
    if info_df is None or info_df.empty:
        logger.warning("get_stock_list() 回空，watchlist 跳過")
        return {"scanned": 0, "hits": 0, "written": 0, "errors": 1}

    info_df = info_df[info_df["industry_category"].isin(inds)].copy()
    sids = info_df["stock_id"].astype(str).tolist()
    if not sids:
        logger.warning("無符合產業的股票，watchlist 跳過")
        return {"scanned": 0, "hits": 0, "written": 0, "errors": 0}

    stock_info = {
        str(r["stock_id"]): {
            "stock_name": r.get("stock_name"),
            "industry_category": r.get("industry_category"),
        }
        for _, r in info_df.iterrows()
    }

    # detector 只用最後 lookback 根日 K，沒必要把全部歷史拉進來。
    # 80 個交易日約等於 112 個日曆日（5/7 比例），加上連假與保險再多 18 天。
    today = date.today()
    start_date = (today - timedelta(days=int(lookback * 1.4) + 18)).isoformat()
    price_data = load_prices_multi(sids, start_date=start_date)

    candidates, stats = build_from_data(
        price_data=price_data,
        stock_info=stock_info,
        today=today,
        fractal_k=fractal_k,
        min_b_rise_pct=min_b_rise_pct,
        min_c_retrace_pct=min_c_retrace_pct,
        max_c_retrace_pct=max_c_retrace_pct,
        require_prior_downtrend=require_prior_downtrend,
        lookback=lookback,
        max_c_age_days=max_c_age_days,
        max_distance_to_b_pct=max_distance_to_b_pct,
        max_above_b_pct=max_above_b_pct,
    )

    wl.purge_old(older_than_days=7)
    wl.clear_today()
    wl.upsert_candidates(candidates, scan_date=today)

    return stats.as_dict()
