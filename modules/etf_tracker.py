"""
ETF 成分股資金流追蹤

核心邏輯：比較 ETF 最近兩次持股快照，偵測：
  - 新進持股（new_entry）：前次快照中不存在，本次出現
  - 權重增加（weight_up）：權重上升超過 threshold
  - 權重下降（weight_down）：權重下降超過 threshold
  - 遭剔除（ejected）：前次存在，本次消失

所有偵測結果都以 stock_id 為 key，供 scanner.run_scan 注入 ScanSignal。

預設追蹤 ETF（DEFAULT_TRACKED_ETFS）：
  0050、00981A、00982A、00985A、00991A、00992A、00993A、00995A
"""
from __future__ import annotations

import logging
from typing import TypedDict

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_TRACKED_ETFS: list[str] = [
    "0050",
    "00981A",
    "00982A",
    "00985A",
    "00991A",
    "00992A",
    "00993A",
    "00995A",
]

# 權重變化視為「有意義」的最小門檻（百分點）
WEIGHT_CHANGE_THRESHOLD = 0.1


class EtfStockSignal(TypedDict):
    new_entry:    bool          # 新進成分股
    weight_up:    bool          # 權重增加
    weight_down:  bool          # 權重下降
    ejected:      bool          # 遭剔除
    etfs_hold:    list[str]     # 目前仍持有此股的 ETF 清單
    etfs_new:     list[str]     # 新進此股的 ETF 清單
    etfs_up:      list[str]     # 增持此股的 ETF 清單
    etfs_down:    list[str]     # 減持此股的 ETF 清單
    etfs_ejected: list[str]     # 剔除此股的 ETF 清單
    max_weight:   float         # 在所有追蹤 ETF 中的最高當前權重 %
    weight_delta: float         # 最大單一 ETF 的權重變化（正=增、負=減）


def _empty_signal() -> EtfStockSignal:
    return EtfStockSignal(
        new_entry=False, weight_up=False, weight_down=False, ejected=False,
        etfs_hold=[], etfs_new=[], etfs_up=[], etfs_down=[], etfs_ejected=[],
        max_weight=0.0, weight_delta=0.0,
    )


def compute_etf_changes(
    holdings_df: pd.DataFrame,
    etf_id: str,
    weight_threshold: float = WEIGHT_CHANGE_THRESHOLD,
) -> dict[str, dict]:
    """
    比較 ETF 最近兩個持股快照，回傳 {hold_stock_id: change_dict}。

    change_dict 格式：
      {
        "status":   "new_entry" | "weight_up" | "weight_down" | "ejected" | "unchanged",
        "etf_id":   str,
        "prev_pct": float,  # 前次權重（遭剔除時為最後已知值，新進時為 0）
        "curr_pct": float,  # 本次權重（遭剔除時為 0）
        "delta":    float,  # curr - prev
      }

    holdings_df 需包含 date、hold_stock_id、percentage 欄位，date 為 datetime 型別。
    """
    if holdings_df.empty:
        return {}

    dates = sorted(holdings_df["date"].dropna().unique(), reverse=True)
    if len(dates) < 2:
        # 只有一個快照：全部標記為 new_entry（首次下載）
        latest = dates[0] if dates else None
        if latest is None:
            return {}
        snap = holdings_df[holdings_df["date"] == latest].set_index("hold_stock_id")["percentage"]
        return {
            sid: {"status": "new_entry", "etf_id": etf_id,
                  "prev_pct": 0.0, "curr_pct": float(pct), "delta": float(pct)}
            for sid, pct in snap.items()
        }

    curr_date, prev_date = dates[0], dates[1]
    curr = holdings_df[holdings_df["date"] == curr_date].set_index("hold_stock_id")["percentage"].to_dict()
    prev = holdings_df[holdings_df["date"] == prev_date].set_index("hold_stock_id")["percentage"].to_dict()

    all_stocks = set(curr) | set(prev)
    result: dict[str, dict] = {}

    for sid in all_stocks:
        c = float(curr.get(sid, 0.0))
        p = float(prev.get(sid, 0.0))
        delta = c - p

        if sid not in prev:
            status = "new_entry"
        elif sid not in curr:
            status = "ejected"
        elif delta >= weight_threshold:
            status = "weight_up"
        elif delta <= -weight_threshold:
            status = "weight_down"
        else:
            status = "unchanged"

        result[sid] = {
            "status":   status,
            "etf_id":   etf_id,
            "prev_pct": p,
            "curr_pct": c,
            "delta":    round(delta, 4),
        }

    return result


def get_stock_etf_signals(
    etf_ids: list[str] | None = None,
    force_refresh: bool = False,
    weight_threshold: float = WEIGHT_CHANGE_THRESHOLD,
) -> dict[str, EtfStockSignal]:
    """
    對所有指定 ETF 執行持股變化分析，回傳 {stock_id: EtfStockSignal} 字典。

    etf_ids: 要追蹤的 ETF 清單，None 時使用 DEFAULT_TRACKED_ETFS。
    force_refresh: True 時強制重新從 FinMind 抓取。

    回傳的 signals 可直接注入 run_scan(etf_signal_data=...) 參數。
    """
    from data.finmind_client import get_etf_holding
    from db.etf_cache import get_latest_two_snapshots

    if etf_ids is None:
        etf_ids = DEFAULT_TRACKED_ETFS

    # {stock_id: EtfStockSignal}
    merged: dict[str, EtfStockSignal] = {}

    for etf_id in etf_ids:
        try:
            df = get_etf_holding(etf_id, force_refresh=force_refresh)
        except Exception as exc:
            logger.warning("ETF %s 資料抓取失敗：%s", etf_id, exc)
            continue

        if df.empty:
            continue

        changes = compute_etf_changes(df, etf_id, weight_threshold=weight_threshold)

        for stock_id, chg in changes.items():
            if not stock_id:
                continue
            if stock_id not in merged:
                merged[stock_id] = _empty_signal()

            sig = merged[stock_id]
            status = chg["status"]
            curr_pct = chg["curr_pct"]
            delta    = chg["delta"]

            if status == "new_entry":
                sig["new_entry"] = True
                sig["etfs_new"].append(etf_id)
            elif status == "weight_up":
                sig["weight_up"] = True
                sig["etfs_up"].append(etf_id)
            elif status == "weight_down":
                sig["weight_down"] = True
                sig["etfs_down"].append(etf_id)
            elif status == "ejected":
                sig["ejected"] = True
                sig["etfs_ejected"].append(etf_id)

            if status in ("new_entry", "weight_up", "weight_down", "unchanged"):
                if etf_id not in sig["etfs_hold"]:
                    sig["etfs_hold"].append(etf_id)
                if curr_pct > sig["max_weight"]:
                    sig["max_weight"] = round(curr_pct, 4)

            if abs(delta) > abs(sig["weight_delta"]):
                sig["weight_delta"] = round(delta, 4)

    return merged


def build_etf_holdings_table(
    etf_ids: list[str] | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    建立「ETF 持股變化比較表」供 UI 顯示。

    欄位：etf_id, hold_stock_id, hold_stock_name,
          prev_pct, curr_pct, delta, status
    """
    from data.finmind_client import get_etf_holding

    if etf_ids is None:
        etf_ids = DEFAULT_TRACKED_ETFS

    rows = []
    for etf_id in etf_ids:
        try:
            df = get_etf_holding(etf_id, force_refresh=force_refresh)
        except Exception as exc:
            logger.warning("ETF %s 表格建立失敗：%s", etf_id, exc)
            continue

        if df.empty:
            continue

        changes = compute_etf_changes(df, etf_id)
        name_map = (
            df.sort_values("date").groupby("hold_stock_id")["hold_stock_name"].last().to_dict()
        )

        for sid, chg in changes.items():
            rows.append({
                "etf_id":         etf_id,
                "hold_stock_id":  sid,
                "hold_stock_name": name_map.get(sid, ""),
                "prev_pct":       chg["prev_pct"],
                "curr_pct":       chg["curr_pct"],
                "delta":          chg["delta"],
                "status":         chg["status"],
            })

    if not rows:
        return pd.DataFrame()

    df_out = pd.DataFrame(rows)
    order = {"new_entry": 0, "weight_up": 1, "weight_down": 2, "ejected": 3, "unchanged": 4}
    df_out["_sort"] = df_out["status"].map(order).fillna(9)
    return (
        df_out.sort_values(["_sort", "delta"], ascending=[True, False])
        .drop(columns=["_sort"])
        .reset_index(drop=True)
    )
