"""
交易日誌模組
計算績效統計：勝率、盈虧比、最大回撤、平均持有天數
"""
import pandas as pd
import numpy as np
from db.database import get_session
from db.models import TradeJournal, Portfolio
from datetime import date


def add_trade(stock_id: str, stock_name: str, action: str, price: float,
              shares: int, trade_date: date, reason: str = "",
              emotion: str = "", pnl: float = None):
    portfolio_sync = None
    with get_session() as sess:
        stock_id = (stock_id or "").strip()
        stock_name = (stock_name or "").strip()
        action = action.upper()
        t = TradeJournal(
            stock_id=stock_id,
            stock_name=stock_name,
            action=action,
            price=price,
            shares=shares,
            trade_date=trade_date,
            reason=reason,
            emotion=emotion,
            pnl=pnl,
        )
        sess.add(t)
        if action == "BUY":
            _upsert_portfolio_buy(
                sess,
                stock_id=stock_id,
                stock_name=stock_name,
                shares=int(shares),
                price=float(price),
                trade_date=trade_date,
            )
        elif action == "SELL":
            portfolio_sync = _apply_portfolio_sell(
                sess,
                stock_id=stock_id,
                shares=int(shares),
            )
        sess.commit()
    return portfolio_sync


def _upsert_portfolio_buy(sess, stock_id: str, stock_name: str, shares: int,
                          price: float, trade_date: date | None = None):
    """將買進交易併入持股監控；已持有時用加權平均成本。"""
    stock_id = (stock_id or "").strip()
    if not stock_id or shares <= 0 or price <= 0:
        return

    row = sess.query(Portfolio).filter(Portfolio.stock_id == stock_id).first()
    if row:
        old_shares = int(row.shares or 0)
        new_shares = old_shares + int(shares)
        if new_shares <= 0:
            return
        row.cost_price = (
            (float(row.cost_price or 0) * old_shares + float(price) * int(shares))
            / new_shares
        )
        row.shares = new_shares
        if stock_name and not row.stock_name:
            row.stock_name = stock_name
        if trade_date and not row.buy_date:
            row.buy_date = trade_date
        return

    sess.add(Portfolio(
        stock_id=stock_id,
        stock_name=stock_name or "",
        shares=int(shares),
        cost_price=float(price),
        buy_date=trade_date,
        notes="由交易日誌自動加入",
        note="由交易日誌自動加入",
    ))


def _apply_portfolio_sell(sess, stock_id: str, shares: int) -> dict | None:
    """將賣出交易同步扣回持股監控；賣完時刪除該持股。"""
    stock_id = (stock_id or "").strip()
    if not stock_id or shares <= 0:
        return None

    row = sess.query(Portfolio).filter(Portfolio.stock_id == stock_id).first()
    if not row:
        return None

    old_shares = int(row.shares or 0)
    new_shares = old_shares - int(shares)
    if new_shares <= 0:
        sess.delete(row)
        return {
            "stock_id": stock_id,
            "old_shares": old_shares,
            "new_shares": 0,
            "removed": True,
        }

    row.shares = new_shares
    return {
        "stock_id": stock_id,
        "old_shares": old_shares,
        "new_shares": new_shares,
        "removed": False,
    }


def _open_positions_from_trades(rows: list[TradeJournal]) -> dict:
    """用 FIFO 從交易日誌推算目前仍開放的買進部位。"""
    lots_by_stock: dict[str, list[dict]] = {}
    ordered = sorted(rows, key=lambda r: (r.trade_date or date.min, r.id or 0))

    for row in ordered:
        sid = (row.stock_id or "").strip()
        if not sid:
            continue
        lots = lots_by_stock.setdefault(sid, [])
        action = (row.action or "").upper()
        shares = int(row.shares or 0)
        price = float(row.price or 0)
        if shares <= 0:
            continue

        if action == "BUY":
            lots.append({
                "shares": shares,
                "price": price,
                "stock_name": row.stock_name or "",
                "trade_date": row.trade_date,
            })
        elif action == "SELL":
            remaining = shares
            while remaining > 0 and lots:
                if lots[0]["shares"] > remaining:
                    lots[0]["shares"] -= remaining
                    remaining = 0
                else:
                    remaining -= lots[0]["shares"]
                    lots.pop(0)

    positions = {}
    for sid, lots in lots_by_stock.items():
        total_shares = sum(l["shares"] for l in lots)
        if total_shares <= 0:
            continue
        total_cost = sum(l["shares"] * l["price"] for l in lots)
        first_lot = lots[0]
        latest_name = next((l["stock_name"] for l in reversed(lots) if l["stock_name"]), "")
        positions[sid] = {
            "stock_name": latest_name,
            "shares": total_shares,
            "cost_price": total_cost / total_shares,
            "buy_date": first_lot.get("trade_date"),
        }
    return positions


def sync_open_trades_to_portfolio() -> list[dict]:
    """
    將交易日誌推算出的仍持有部位同步到持股監控。

    對有交易日誌的股票，同步目前股數；若已無未平倉部位則移除。
    已存在的停損、停利與備註不會被覆蓋。
    回傳新增項目清單。
    """
    added = []
    with get_session() as sess:
        rows = sess.query(TradeJournal).all()
        positions = _open_positions_from_trades(rows)
        trade_stock_ids = {(r.stock_id or "").strip() for r in rows if (r.stock_id or "").strip()}

        existing_rows = sess.query(Portfolio).filter(Portfolio.stock_id.in_(trade_stock_ids)).all() if trade_stock_ids else []
        for row in existing_rows:
            sid = (row.stock_id or "").strip()
            pos = positions.get(sid)
            if not pos:
                sess.delete(row)
                continue
            row.shares = int(pos["shares"])
            if not row.stock_name and pos["stock_name"]:
                row.stock_name = pos["stock_name"]
            if not row.buy_date and pos["buy_date"]:
                row.buy_date = pos["buy_date"]

        for sid, pos in positions.items():
            exists = sess.query(Portfolio).filter(Portfolio.stock_id == sid).first()
            if exists:
                continue
            sess.add(Portfolio(
                stock_id=sid,
                stock_name=pos["stock_name"],
                shares=int(pos["shares"]),
                cost_price=float(pos["cost_price"]),
                buy_date=pos["buy_date"],
                notes="由交易日誌同步補入",
                note="由交易日誌同步補入",
            ))
            added.append({
                "stock_id": sid,
                "stock_name": pos["stock_name"],
                "shares": int(pos["shares"]),
                "cost_price": round(float(pos["cost_price"]), 2),
            })
        sess.commit()
    return added


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
