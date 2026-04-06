"""
回測引擎

原則：
1. 進場訊號只用當日以前資料計算，下一交易日開盤進場，避免前視偏差。
2. 出場條件改為模組化開關；使用者可自由組合，任一已啟用條件觸發即出場。
3. 交易成本以台股常見股票費率估算，每筆交易固定以 1 張計算。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd

from modules.scanner import analyze_stock, compute_indicators

BUY_FEE_RATE = 0.001425
SELL_FEE_RATE = 0.001425
SELL_TAX_RATE = 0.003


@dataclass
class BacktestConfig:
    start_date: str
    end_date: str
    enable_trailing_exit: bool = True
    trailing_stop_pct: float = 8.0
    trailing_tp_activation_pct: float = 15.0
    trailing_tp_pct: float = 5.0
    enable_ma20_exit: bool = True
    enable_max_hold_exit: bool = True
    max_hold_days: int = 20
    enable_indicator_exit: bool = False
    indicator_exit_mode: str = "rsi_50"  # rsi_50 / macd_dead_cross
    min_score: float = 65.0
    warmup_days: int = 60


@dataclass
class TradeRecord:
    stock_id: str
    buy_date: date
    buy_price: float
    sell_date: date | None = None
    sell_price: float | None = None
    exit_reason: str = ""
    hold_days: int = 0
    pnl: float = 0.0
    pnl_pct: float = 0.0


@dataclass
class BacktestResult:
    config: BacktestConfig
    trades: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)

    def summary(self) -> dict:
        if not self.trades:
            return {}

        closed = [t for t in self.trades if t.sell_date is not None]
        if not closed:
            return {}

        wins = [t for t in closed if t.pnl_pct > 0]
        losses = [t for t in closed if t.pnl_pct <= 0]

        avg_win = np.mean([t.pnl_pct for t in wins]) if wins else 0.0
        avg_loss = np.mean([t.pnl_pct for t in losses]) if losses else 0.0
        profit_factor = (
            sum(t.pnl_pct for t in wins) / abs(sum(t.pnl_pct for t in losses))
            if losses and sum(t.pnl_pct for t in losses) != 0
            else float("inf")
        )

        eq = self.equity_curve
        max_dd = 0.0
        if len(eq) > 1:
            peak = eq[0]
            for value in eq:
                if value > peak:
                    peak = value
                drawdown = (value - peak) / peak * 100
                if drawdown < max_dd:
                    max_dd = drawdown

        total_return = (eq[-1] - 1.0) * 100 if eq else 0.0

        return {
            "total_trades": len(closed),
            "win_trades": len(wins),
            "loss_trades": len(losses),
            "win_rate": round(len(wins) / len(closed) * 100, 1),
            "avg_win_pct": round(avg_win, 2),
            "avg_loss_pct": round(avg_loss, 2),
            "profit_factor": round(profit_factor, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "total_return_pct": round(total_return, 2),
            "avg_hold_days": round(np.mean([t.hold_days for t in closed]), 1),
            "exit_reasons": pd.Series([t.exit_reason for t in closed]).value_counts().to_dict(),
        }


def _calc_pnl(buy_price: float, sell_price: float) -> tuple[float, float]:
    """以 1 張估算實際交易成本後的損益。"""
    buy_cost = buy_price * 1000 * (1 + BUY_FEE_RATE)
    sell_revenue = sell_price * 1000 * (1 - SELL_FEE_RATE - SELL_TAX_RATE)
    pnl = sell_revenue - buy_cost
    pnl_pct = pnl / buy_cost * 100
    return round(pnl), round(pnl_pct, 3)


def _evaluate_exit_conditions(
    stock_df: pd.DataFrame,
    today: date,
    pos: TradeRecord,
    peak_price: float,
    config: BacktestConfig,
) -> tuple[float | None, str]:
    """回傳當天是否出場，以及對應的出場原因。"""
    today_row = stock_df[stock_df["date"].dt.date == today]
    if today_row.empty:
        return None, ""

    row = today_row.iloc[0]
    high = float(row.get("max", row["close"]))
    low = float(row.get("min", row["close"]))
    close = float(row["close"])
    hold_days = (today - pos.buy_date).days

    df_to_today = stock_df[stock_df["date"].dt.date <= today]
    candidates: list[tuple[str, float]] = []

    # 順序代表同一天多條件同時成立時，交易紀錄顯示的優先原因。
    if config.enable_trailing_exit:
        tp_activated = peak_price >= pos.buy_price * (1 + config.trailing_tp_activation_pct / 100)
        if tp_activated:
            trail_pct = config.trailing_tp_pct
            trail_label = "移動停利觸發"
        else:
            trail_pct = config.trailing_stop_pct
            trail_label = "移動停損觸發"

        trail_stop = peak_price * (1 - trail_pct / 100)
        if low <= trail_stop:
            candidates.append((trail_label, trail_stop))

    if config.enable_ma20_exit and len(df_to_today) >= 20:
        ma20 = df_to_today["close"].tail(20).mean()
        if close < ma20:
            candidates.append(("跌破MA20", close))

    if config.enable_max_hold_exit and hold_days >= config.max_hold_days:
        candidates.append((f"達最大持倉{config.max_hold_days}天", close))

    if config.enable_indicator_exit and len(df_to_today) >= 35:
        indicator_df = compute_indicators(df_to_today)
        if len(indicator_df) >= 2:
            prev_row = indicator_df.iloc[-2]
            curr_row = indicator_df.iloc[-1]

            if config.indicator_exit_mode == "rsi_50":
                prev_rsi = prev_row.get("rsi14")
                curr_rsi = curr_row.get("rsi14")
                crossed_down = (
                    pd.notna(prev_rsi)
                    and pd.notna(curr_rsi)
                    and prev_rsi >= 50
                    and curr_rsi < 50
                )
                if crossed_down:
                    candidates.append(("RSI轉弱", close))

            elif config.indicator_exit_mode == "macd_dead_cross":
                prev_macd = prev_row.get("macd")
                prev_signal = prev_row.get("macd_signal")
                curr_macd = curr_row.get("macd")
                curr_signal = curr_row.get("macd_signal")
                dead_cross = (
                    pd.notna(prev_macd)
                    and pd.notna(prev_signal)
                    and pd.notna(curr_macd)
                    and pd.notna(curr_signal)
                    and prev_macd >= prev_signal
                    and curr_macd < curr_signal
                )
                if dead_cross:
                    candidates.append(("MACD死亡交叉", close))

    if candidates:
        exit_reason, sell_price = candidates[0]
        return sell_price, exit_reason
    return None, ""


def run_backtest(
    price_data: dict,
    config: BacktestConfig,
    progress_callback=None,
) -> BacktestResult:
    result = BacktestResult(config=config)

    all_dates = set()
    for df in price_data.values():
        if not df.empty:
            all_dates.update(df["date"].dt.date.tolist())

    start = pd.to_datetime(config.start_date).date()
    end = pd.to_datetime(config.end_date).date()
    trading_days = sorted(d for d in all_dates if start <= d <= end)
    if not trading_days:
        return result

    open_positions: dict[str, TradeRecord] = {}
    peak_prices: dict[str, float] = {}
    equity = 1.0

    total_days = len(trading_days)
    for day_idx, today in enumerate(trading_days):
        if progress_callback:
            progress_callback(day_idx + 1, total_days)

        day_pnl_pct = 0.0
        closed_today: list[str] = []

        for sid, pos in list(open_positions.items()):
            stock_df = price_data.get(sid)
            if stock_df is None:
                continue

            today_row = stock_df[stock_df["date"].dt.date == today]
            if today_row.empty:
                continue

            high = float(today_row.iloc[0].get("max", today_row.iloc[0]["close"]))
            peak_prices[sid] = max(peak_prices.get(sid, pos.buy_price), high)

            sell_price, exit_reason = _evaluate_exit_conditions(
                stock_df=stock_df,
                today=today,
                pos=pos,
                peak_price=peak_prices[sid],
                config=config,
            )
            if sell_price is None:
                continue

            pnl, pnl_pct = _calc_pnl(pos.buy_price, sell_price)
            pos.sell_date = today
            pos.sell_price = sell_price
            pos.exit_reason = exit_reason
            pos.hold_days = (today - pos.buy_date).days
            pos.pnl = pnl
            pos.pnl_pct = pnl_pct
            day_pnl_pct += pnl_pct
            closed_today.append(sid)

        for sid in closed_today:
            result.trades.append(open_positions.pop(sid))
            peak_prices.pop(sid, None)

        if day_idx + 1 < len(trading_days):
            next_day = trading_days[day_idx + 1]

            for sid, full_df in price_data.items():
                if sid in open_positions:
                    continue

                df_to_today = full_df[full_df["date"].dt.date <= today].copy()
                if len(df_to_today) < config.warmup_days:
                    continue

                signal = analyze_stock(df_to_today)
                if signal is None or not signal.passes_basic():
                    continue
                if signal.score() < config.min_score:
                    continue

                next_row = full_df[full_df["date"].dt.date == next_day]
                if next_row.empty:
                    continue

                buy_price = float(next_row.iloc[0].get("open", next_row.iloc[0]["close"]))
                if buy_price <= 0:
                    continue

                open_positions[sid] = TradeRecord(
                    stock_id=sid,
                    buy_date=next_day,
                    buy_price=buy_price,
                )

        if day_pnl_pct:
            equity *= 1 + day_pnl_pct / 100
        result.equity_curve.append(round(equity, 6))

    for _, pos in open_positions.items():
        result.trades.append(pos)

    return result


def compare_to_benchmark(
    result: BacktestResult,
    benchmark_df: pd.DataFrame,
    config: BacktestConfig,
) -> dict:
    if benchmark_df.empty or not result.equity_curve:
        return {}

    start = pd.to_datetime(config.start_date)
    end = pd.to_datetime(config.end_date)
    bm = benchmark_df[(benchmark_df["date"] >= start) & (benchmark_df["date"] <= end)].copy()
    bm = bm.sort_values("date")
    if bm.empty or len(bm) < 2:
        return {}

    bm_return = (bm["close"].iloc[-1] / bm["close"].iloc[0] - 1) * 100
    strategy_return = (result.equity_curve[-1] - 1) * 100
    alpha = strategy_return - bm_return

    return {
        "strategy_return_pct": round(strategy_return, 2),
        "benchmark_return_pct": round(bm_return, 2),
        "alpha_pct": round(alpha, 2),
    }
