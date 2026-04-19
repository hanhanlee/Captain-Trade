"""
持股監控模組
- 即時計算未實現損益、回撤
- 判斷停損/停利/技術破壞等賣出警示
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional
from modules.indicators import sma


@dataclass
class AlertLevel:
    NONE = "none"
    INFO = "info"
    WARNING = "warning"
    DANGER = "danger"


@dataclass
class StockAlert:
    stock_id: str
    stock_name: str
    level: str          # AlertLevel
    reason: str
    current_price: float
    cost_price: float
    pnl_pct: float


def calc_holding_stats(holding: dict, df: pd.DataFrame) -> dict:
    """
    計算單一持股的即時狀態

    holding 欄位：stock_id, stock_name, shares, cost_price, stop_loss, take_profit
    df：該股日K（需含 close, Trading_Volume）

    回傳：
        close, pnl, pnl_pct, high_since_buy, drawdown_from_high,
        ma20, above_ma20, alerts
    """
    if df.empty:
        return {}

    latest = df.iloc[-1]
    close = latest["close"]
    cost = holding["cost_price"]
    shares = holding["shares"]

    pnl = (close - cost) * shares               # shares 已統一為股數
    pnl_pct = (close - cost) / cost * 100

    # 近 60 日最高點（從持有以來高點估算）
    high_since_buy = df["close"].tail(60).max()
    drawdown_from_high = (close - high_since_buy) / high_since_buy * 100

    # 技術面
    ma20_series = sma(df["close"], 20)
    ma20 = ma20_series.iloc[-1] if not ma20_series.empty else None
    above_ma20 = bool(close > ma20) if ma20 is not None and not np.isnan(ma20) else None

    alerts = _check_alerts(holding, close, pnl_pct, drawdown_from_high, above_ma20, ma20)

    return {
        "stock_id": holding["stock_id"],
        "stock_name": holding.get("stock_name", ""),
        "shares": shares,
        "cost_price": cost,
        "close": close,
        "pnl": round(pnl),
        "pnl_pct": round(pnl_pct, 2),
        "high_since_buy": high_since_buy,
        "drawdown_from_high": round(drawdown_from_high, 2),
        "ma20": round(ma20, 2) if ma20 is not None and not np.isnan(ma20) else None,
        "above_ma20": above_ma20,
        "stop_loss": holding.get("stop_loss"),
        "take_profit": holding.get("take_profit"),
        "alerts": alerts,
    }


def _check_alerts(holding: dict, close: float, pnl_pct: float,
                  drawdown: float, above_ma20: Optional[bool],
                  ma20: Optional[float]) -> list:
    alerts = []
    stop_loss = holding.get("stop_loss")
    take_profit = holding.get("take_profit")

    # 1. 跌破停損價
    if stop_loss and close <= stop_loss:
        alerts.append(StockAlert(
            stock_id=holding["stock_id"],
            stock_name=holding.get("stock_name", ""),
            level=AlertLevel.DANGER,
            reason=f"跌破停損價 {float(stop_loss):.2f} 元",
            current_price=close,
            cost_price=holding["cost_price"],
            pnl_pct=pnl_pct,
        ))

    # 2. 達到停利目標
    if take_profit and close >= take_profit:
        alerts.append(StockAlert(
            stock_id=holding["stock_id"],
            stock_name=holding.get("stock_name", ""),
            level=AlertLevel.INFO,
            reason=f"達到停利目標 {float(take_profit):.2f} 元",
            current_price=close,
            cost_price=holding["cost_price"],
            pnl_pct=pnl_pct,
        ))

    # 3. 從高點大幅回撤
    if drawdown <= -8:
        level = AlertLevel.DANGER if drawdown <= -12 else AlertLevel.WARNING
        alerts.append(StockAlert(
            stock_id=holding["stock_id"],
            stock_name=holding.get("stock_name", ""),
            level=level,
            reason=f"從高點回撤 {drawdown:.1f}%",
            current_price=close,
            cost_price=holding["cost_price"],
            pnl_pct=pnl_pct,
        ))

    # 4. 跌破 MA20
    if above_ma20 is False and ma20 is not None:
        alerts.append(StockAlert(
            stock_id=holding["stock_id"],
            stock_name=holding.get("stock_name", ""),
            level=AlertLevel.WARNING,
            reason=f"跌破 MA20（{float(ma20):.2f} 元）",
            current_price=close,
            cost_price=holding["cost_price"],
            pnl_pct=pnl_pct,
        ))

    # 5. 虧損超過 -5%（無停損設定時）
    if not stop_loss and pnl_pct <= -5:
        level = AlertLevel.DANGER if pnl_pct <= -10 else AlertLevel.WARNING
        alerts.append(StockAlert(
            stock_id=holding["stock_id"],
            stock_name=holding.get("stock_name", ""),
            level=level,
            reason=f"未實現虧損 {pnl_pct:.1f}%，建議設定停損",
            current_price=close,
            cost_price=holding["cost_price"],
            pnl_pct=pnl_pct,
        ))

    return alerts


def run_portfolio_check(holdings: list, price_data: dict) -> tuple:
    """
    批次檢查所有持股

    Returns:
        stats_list: list of dict（每檔持股的完整狀態）
        all_alerts: list of StockAlert（所有警示，依嚴重度排序）
    """
    stats_list = []
    all_alerts = []

    priority = {AlertLevel.DANGER: 0, AlertLevel.WARNING: 1, AlertLevel.INFO: 2, AlertLevel.NONE: 3}

    for h in holdings:
        sid = h["stock_id"]
        df = price_data.get(sid)
        if df is None:
            continue
        stats = calc_holding_stats(h, df)
        if stats:
            stats_list.append(stats)
            all_alerts.extend(stats.get("alerts", []))

    all_alerts.sort(key=lambda a: priority.get(a.level, 9))
    return stats_list, all_alerts
