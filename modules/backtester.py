"""
回測引擎
逐日重播歷史資料，模擬選股雷達的實際績效

核心設計原則：
  1. 零前視偏差（No Look-ahead Bias）
     每個交易日只能使用「當天收盤前」已知的資料
  2. 真實執行假設
     訊號日收盤後產生，次日開盤價買進
  3. 含交易成本
     買進：手續費 0.1425%
     賣出：手續費 0.1425% + 交易稅 0.3%

出場規則（移動式）：
  - 移動停損：持倉最高價 × (1 - trailing_stop_pct%)；最高價只升不降
  - 移動停利：獲利達 trailing_tp_activation_pct% 後，改用更緊的 trailing_tp_pct% 追蹤
  - 強制出場：持有超過 max_hold_days 天
  - 技術破壞：收盤跌破 MA20（選用）
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from datetime import date
from modules.scanner import analyze_stock, compute_indicators

# 台股交易成本
BUY_FEE_RATE = 0.001425      # 買進手續費
SELL_FEE_RATE = 0.001425     # 賣出手續費
SELL_TAX_RATE = 0.003        # 交易稅（賣出）
TOTAL_COST_RATE = BUY_FEE_RATE + SELL_FEE_RATE + SELL_TAX_RATE  # 約 0.447%


@dataclass
class BacktestConfig:
    start_date: str           # 回測開始日期，格式 "YYYY-MM-DD"
    end_date: str             # 回測結束日期
    trailing_stop_pct: float = 8.0            # 移動停損：從持倉最高價回落幾% 出場
    trailing_tp_activation_pct: float = 15.0  # 移動停利啟動門檻：獲利達此% 後緊縮追蹤
    trailing_tp_pct: float = 5.0              # 啟動後的緊縮追蹤幅度（從最高價回落幾%）
    max_hold_days: int = 20         # 最大持有天數（保底出場）
    use_ma20_exit: bool = True      # 跌破 MA20 時出場
    min_score: float = 65.0         # 只買強度分數 >= 此值的訊號
    warmup_days: int = 60           # 計算指標需要的暖身天數（不產生訊號）


@dataclass
class TradeRecord:
    stock_id: str
    buy_date: date
    buy_price: float
    sell_date: date = None
    sell_price: float = None
    exit_reason: str = ""
    hold_days: int = 0
    pnl: float = 0.0          # 含交易成本的損益（元，以1張計算）
    pnl_pct: float = 0.0      # 含交易成本的損益率


@dataclass
class BacktestResult:
    config: BacktestConfig
    trades: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)   # 每日帳戶淨值（起始=1.0）

    def summary(self) -> dict:
        if not self.trades:
            return {}
        closed = [t for t in self.trades if t.sell_date is not None]
        if not closed:
            return {}

        wins = [t for t in closed if t.pnl_pct > 0]
        losses = [t for t in closed if t.pnl_pct <= 0]

        avg_win = np.mean([t.pnl_pct for t in wins]) if wins else 0
        avg_loss = np.mean([t.pnl_pct for t in losses]) if losses else 0
        profit_factor = (
            sum(t.pnl_pct for t in wins) / abs(sum(t.pnl_pct for t in losses))
            if losses and sum(t.pnl_pct for t in losses) != 0 else float("inf")
        )

        # 最大回撤
        eq = self.equity_curve
        max_dd = 0.0
        if len(eq) > 1:
            peak = eq[0]
            for v in eq:
                if v > peak:
                    peak = v
                dd = (v - peak) / peak * 100
                if dd < max_dd:
                    max_dd = dd

        total_return = (self.equity_curve[-1] - 1.0) * 100 if self.equity_curve else 0

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


def _calc_pnl(buy_price: float, sell_price: float) -> tuple:
    """含手續費與交易稅計算損益率（以1張1000股計算）"""
    buy_cost = buy_price * 1000 * (1 + BUY_FEE_RATE)
    sell_revenue = sell_price * 1000 * (1 - SELL_FEE_RATE - SELL_TAX_RATE)
    pnl = sell_revenue - buy_cost
    pnl_pct = pnl / buy_cost * 100
    return round(pnl), round(pnl_pct, 3)


def run_backtest(
    price_data: dict,       # {stock_id: DataFrame}（全歷史）
    config: BacktestConfig,
    progress_callback=None, # callback(current, total) 供 UI 更新進度條
) -> BacktestResult:
    """
    執行回測

    price_data: 每檔股票的完整歷史日K（從快取載入）
    config: 回測參數
    """
    result = BacktestResult(config=config)

    # 建立交易日曆（取所有股票的聯集日期，只保留在範圍內的）
    all_dates = set()
    for df in price_data.values():
        if not df.empty:
            dates = df["date"].dt.date if hasattr(df["date"].iloc[0], "date") else df["date"]
            all_dates.update(dates)

    start = pd.to_datetime(config.start_date).date()
    end = pd.to_datetime(config.end_date).date()
    trading_days = sorted(d for d in all_dates if start <= d <= end)

    if not trading_days:
        return result

    # 持倉狀態 {stock_id: TradeRecord}
    open_positions: dict[str, TradeRecord] = {}
    # 追蹤各持倉的最高價（移動停損/停利基準），不放入 TradeRecord（屬於執行期狀態）
    peak_prices: dict[str, float] = {}
    equity = 1.0          # 以比例計算，起始 1.0
    daily_returns = []

    total_days = len(trading_days)

    for day_idx, today in enumerate(trading_days):
        if progress_callback:
            progress_callback(day_idx + 1, total_days)

        day_pnl_pct = 0.0
        closed_today = []

        # ── 先處理持倉出場 ──────────────────────────────
        for sid, pos in list(open_positions.items()):
            stock_df = price_data.get(sid)
            if stock_df is None:
                continue

            today_row = stock_df[stock_df["date"].dt.date == today]
            if today_row.empty:
                continue

            row = today_row.iloc[0]
            high = row.get("max", row["close"])
            low = row.get("min", row["close"])
            close = row["close"]

            hold_days = (today - pos.buy_date).days
            sell_price = None
            exit_reason = ""

            # 更新持倉最高價（用當日最高價追蹤，只升不降）
            prev_peak = peak_prices.get(sid, pos.buy_price)
            peak = max(prev_peak, high)
            peak_prices[sid] = peak

            # 判斷是否已啟動移動停利模式（獲利達啟動門檻）
            tp_activated = peak >= pos.buy_price * (1 + config.trailing_tp_activation_pct / 100)
            if tp_activated:
                trail_pct = config.trailing_tp_pct
                trail_label = "移動停利"
            else:
                trail_pct = config.trailing_stop_pct
                trail_label = "移動停損"

            # 移動停損/停利：最高價回落 trail_pct% 時出場（以當日最低價判斷觸及）
            trail_stop = peak * (1 - trail_pct / 100)
            if low <= trail_stop:
                sell_price = trail_stop
                exit_reason = trail_label

            # MA20 跌破出場（未觸及移動停損時才判斷）
            elif config.use_ma20_exit:
                df_to_today = stock_df[stock_df["date"].dt.date <= today]
                if len(df_to_today) >= 20:
                    ma20 = df_to_today["close"].tail(20).mean()
                    if close < ma20:
                        sell_price = close
                        exit_reason = "跌破MA20"

            # 強制出場（持有超過天數限制）
            if sell_price is None and hold_days >= config.max_hold_days:
                sell_price = close
                exit_reason = f"持滿{config.max_hold_days}天"

            if sell_price is not None:
                pnl, pnl_pct = _calc_pnl(pos.buy_price, sell_price)
                pos.sell_date = today
                pos.sell_price = sell_price
                pos.exit_reason = exit_reason
                pos.hold_days = hold_days
                pos.pnl = pnl
                pos.pnl_pct = pnl_pct
                day_pnl_pct += pnl_pct
                closed_today.append(sid)

        for sid in closed_today:
            result.trades.append(open_positions.pop(sid))
            peak_prices.pop(sid, None)

        # ── 掃描今日訊號（使用截至今日的資料，次日進場）──
        if day_idx + 1 < len(trading_days):
            next_day = trading_days[day_idx + 1]

            for sid, full_df in price_data.items():
                # 跳過已持倉的股票
                if sid in open_positions:
                    continue

                # 只用截至今日的資料（防止前視偏差）
                df_to_today = full_df[full_df["date"].dt.date <= today].copy()

                # 需要足夠的暖身資料
                if len(df_to_today) < config.warmup_days:
                    continue

                sig = analyze_stock(df_to_today)
                if sig is None or not sig.passes_basic():
                    continue
                if sig.score() < config.min_score:
                    continue

                # 確認次日有開盤價可以買進
                next_row = full_df[full_df["date"].dt.date == next_day]
                if next_row.empty:
                    continue

                buy_price = next_row.iloc[0].get("open", next_row.iloc[0]["close"])
                if not buy_price or buy_price <= 0:
                    continue

                open_positions[sid] = TradeRecord(
                    stock_id=sid,
                    buy_date=next_day,
                    buy_price=buy_price,
                )

        # 更新權益曲線
        equity = equity * (1 + day_pnl_pct / 100) if day_pnl_pct else equity
        result.equity_curve.append(round(equity, 6))
        daily_returns.append(day_pnl_pct)

    # 將剩餘未平倉部位加入記錄（未出場，不計入績效統計）
    for sid, pos in open_positions.items():
        result.trades.append(pos)

    return result


def compare_to_benchmark(
    result: BacktestResult,
    benchmark_df: pd.DataFrame,  # 加權指數日K
    config: BacktestConfig,
) -> dict:
    """
    計算策略相對大盤的超額報酬

    benchmark_df 需包含 date, close 欄位
    """
    if benchmark_df.empty or not result.equity_curve:
        return {}

    start = pd.to_datetime(config.start_date)
    end = pd.to_datetime(config.end_date)

    bm = benchmark_df[
        (benchmark_df["date"] >= start) &
        (benchmark_df["date"] <= end)
    ].copy().sort_values("date")

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
