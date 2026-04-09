"""
交易日誌模組
計算績效統計：勝率、盈虧比、最大回撤、平均持有天數
"""
import pandas as pd
import numpy as np
from db.database import get_session
from db.models import TradeJournal
from datetime import date


def add_trade(stock_id: str, stock_name: str, action: str, price: float,
              shares: int, trade_date: date, reason: str = "",
              emotion: str = "", pnl: float = None):
    with get_session() as sess:
        t = TradeJournal(
            stock_id=stock_id,
            stock_name=stock_name,
            action=action.upper(),
            price=price,
            shares=shares,
            trade_date=trade_date,
            reason=reason,
            emotion=emotion,
            pnl=pnl,
        )
        sess.add(t)
        sess.commit()


def get_all_trades() -> pd.DataFrame:
    with get_session() as sess:
        rows = sess.query(TradeJournal).order_by(TradeJournal.trade_date.desc()).all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([{
            "id": r.id,
            "stock_id": r.stock_id,
            "stock_name": r.stock_name,
            "action": r.action,
            "price": r.price,
            "shares": r.shares,
            "trade_date": r.trade_date,
            "reason": r.reason,
            "emotion": r.emotion,
            "pnl": r.pnl,
        } for r in rows])


def get_trade(trade_id: int) -> dict | None:
    """取得單筆記錄，供編輯表單預填用。找不到回傳 None。"""
    with get_session() as sess:
        row = sess.query(TradeJournal).filter(TradeJournal.id == trade_id).first()
        if not row:
            return None
        return {
            "id": row.id,
            "stock_id": row.stock_id,
            "stock_name": row.stock_name or "",
            "action": row.action,
            "price": float(row.price),
            "shares": int(row.shares),
            "trade_date": row.trade_date,
            "reason": row.reason or "",
            "emotion": row.emotion or "",
            "pnl": float(row.pnl) if row.pnl is not None else None,
        }


def update_trade(trade_id: int, stock_id: str, stock_name: str, action: str,
                 price: float, shares: int, trade_date: date,
                 reason: str = "", emotion: str = "", pnl: float = None):
    """更新既有交易記錄。"""
    with get_session() as sess:
        row = sess.query(TradeJournal).filter(TradeJournal.id == trade_id).first()
        if not row:
            return False
        row.stock_id   = stock_id
        row.stock_name = stock_name
        row.action     = action.upper()
        row.price      = price
        row.shares     = shares
        row.trade_date = trade_date
        row.reason     = reason
        row.emotion    = emotion
        row.pnl        = pnl
        sess.commit()
        return True


def delete_trade(trade_id: int):
    with get_session() as sess:
        row = sess.query(TradeJournal).filter(TradeJournal.id == trade_id).first()
        if row:
            sess.delete(row)
            sess.commit()


def calc_performance(df: pd.DataFrame) -> dict:
    """
    計算整體績效統計，只看有 pnl 的 SELL 記錄

    Returns dict:
        total_trades, win_trades, win_rate, avg_win, avg_loss,
        profit_factor, total_pnl, best_trade, worst_trade
    """
    if df.empty:
        return {}

    sell_df = df[(df["action"] == "SELL") & df["pnl"].notna()].copy()
    if sell_df.empty:
        return {}

    wins = sell_df[sell_df["pnl"] > 0]
    losses = sell_df[sell_df["pnl"] <= 0]

    total = len(sell_df)
    win_count = len(wins)
    win_rate = win_count / total * 100 if total > 0 else 0

    avg_win = wins["pnl"].mean() if not wins.empty else 0
    avg_loss = losses["pnl"].mean() if not losses.empty else 0

    total_win = wins["pnl"].sum()
    total_loss = abs(losses["pnl"].sum())
    profit_factor = total_win / total_loss if total_loss > 0 else float("inf")

    # 期望值 = 勝率 × 平均獲利 - (1 - 勝率) × 平均虧損
    win_rate_dec = win_rate / 100
    expected_value = win_rate_dec * avg_win - (1 - win_rate_dec) * abs(avg_loss)

    # 盈虧比評級
    if profit_factor < 1:
        pf_rating = "長期必虧"
    elif profit_factor < 1.5:
        pf_rating = "僅達損益平衡"
    elif profit_factor < 2:
        pf_rating = "良好"
    else:
        pf_rating = "優秀"

    return {
        "total_trades": total,
        "win_trades": win_count,
        "loss_trades": len(losses),
        "win_rate": round(win_rate, 1),
        "avg_win": round(avg_win),
        "avg_loss": round(avg_loss),
        "profit_factor": round(profit_factor, 2),
        "pf_rating": pf_rating,
        "expected_value": round(expected_value),
        "total_pnl": round(sell_df["pnl"].sum()),
        "best_trade": round(sell_df["pnl"].max()),
        "worst_trade": round(sell_df["pnl"].min()),
    }


def calc_emotion_stats(df: pd.DataFrame) -> pd.DataFrame:
    """統計各情緒標籤的勝率，找出情緒化交易模式"""
    sell_df = df[(df["action"] == "SELL") & df["pnl"].notna() & df["emotion"].notna()]
    sell_df = sell_df[sell_df["emotion"] != ""]
    if sell_df.empty:
        return pd.DataFrame()

    result = []
    for emotion, group in sell_df.groupby("emotion"):
        wins = (group["pnl"] > 0).sum()
        result.append({
            "情緒": emotion,
            "交易次數": len(group),
            "獲利次數": wins,
            "勝率%": round(wins / len(group) * 100, 1),
            "平均損益": round(group["pnl"].mean()),
        })
    return pd.DataFrame(result).sort_values("勝率%", ascending=False)
