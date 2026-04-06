"""
回測引擎

設計原則：
1. 進場只使用當日以前已知資料，於次交易日開盤買進，避免前視偏差。
2. 出場條件模組化；任一已啟用條件先觸發就出場。
3. 保留原有台股交易成本：買進手續費 0.1425%，賣出手續費 0.1425% + 證交稅 0.3%。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from io import StringIO

import numpy as np
import pandas as pd

from modules.indicators import atr, sma
from modules.scanner import analyze_stock, compute_indicators

BUY_FEE_RATE = 0.001425
SELL_FEE_RATE = 0.001425
SELL_TAX_RATE = 0.003


@dataclass
class BacktestConfig:
    start_date: str
    end_date: str
    enable_trailing_exit: bool = True
    atr_period: int = 14
    atr_multiplier: float = 2.5
    enable_ma20_exit: bool = True
    enable_max_hold_exit: bool = True
    max_hold_days: int = 20
    enable_indicator_exit: bool = False
    indicator_exit_mode: str = "rsi_50"  # rsi_50 / macd_dead_cross
    enable_market_filter: bool = True
    market_ma_period: int = 20
    max_bias_ratio: float = 10.0
    min_score: float = 65.0
    warmup_days: int = 60
    exclude_leveraged_etf: bool = True  # 排除槓桿/反向/期貨型 ETF（代碼末位 L/R/U）


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
class SkipRecord:
    stock_id: str
    trade_date: date
    reason: str
    score: float = 0.0
    bias_ratio: float | None = None


@dataclass
class BacktestResult:
    config: BacktestConfig
    trades: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)
    skip_logs: list = field(default_factory=list)

    def summary(self) -> dict:
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

        max_dd = 0.0
        if len(self.equity_curve) > 1:
            peak = self.equity_curve[0]
            for value in self.equity_curve:
                if value > peak:
                    peak = value
                drawdown = (value - peak) / peak * 100
                if drawdown < max_dd:
                    max_dd = drawdown

        total_return = (self.equity_curve[-1] - 1.0) * 100 if self.equity_curve else 0.0
        skip_counts = (
            pd.Series([s.reason for s in self.skip_logs]).value_counts().to_dict()
            if self.skip_logs
            else {}
        )

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
            "skip_reasons": skip_counts,
            "skip_count": len(self.skip_logs),
        }


def _calc_pnl(buy_price: float, sell_price: float) -> tuple[float, float]:
    buy_cost = buy_price * 1000 * (1 + BUY_FEE_RATE)
    sell_revenue = sell_price * 1000 * (1 - SELL_FEE_RATE - SELL_TAX_RATE)
    pnl = sell_revenue - buy_cost
    pnl_pct = pnl / buy_cost * 100
    return round(pnl), round(pnl_pct, 3)


def _prepare_price_frame(df: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    prepared = compute_indicators(df)
    prepared = prepared.copy()
    prepared["atr14"] = atr(prepared, period=config.atr_period)
    return prepared


def _prepare_market_frame(df: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    market = df.copy().sort_values("date").reset_index(drop=True)
    market["market_ma20"] = sma(market["close"], config.market_ma_period)
    market["market_ma20_slope_up"] = market["market_ma20"] > market["market_ma20"].shift(1)
    return market


class Strategy:
    def __init__(self, config: BacktestConfig, market_df: pd.DataFrame | None = None):
        self.config = config
        self.market_df = market_df

    def update_exit_signal(
        self,
        stock_df: pd.DataFrame,
        today: date,
        position: TradeRecord,
        peak_price: float,
        today_iloc: int | None = None,
    ) -> tuple[float | None, str]:
        """根據當前持倉更新出場訊號。"""
        if today_iloc is not None:
            row = stock_df.iloc[today_iloc]
        else:
            today_rows = stock_df.loc[stock_df["date"].dt.date == today]
            if today_rows.empty:
                return None, ""
            row = today_rows.iloc[0]
            today_iloc = today_rows.index[0]
        low = float(row.get("min", row["close"]))
        close = float(row["close"])
        hold_days = (today - position.buy_date).days
        candidates: list[tuple[str, float]] = []

        if self.config.enable_trailing_exit:
            atr_value = row.get("atr14")
            if pd.notna(atr_value) and float(atr_value) > 0:
                trail_stop = peak_price - float(atr_value) * self.config.atr_multiplier
                if low <= trail_stop:
                    candidates.append(("ATR移動停損觸發", trail_stop))

        if self.config.enable_ma20_exit:
            ma20 = row.get("ma20")
            if pd.notna(ma20) and close < float(ma20):
                candidates.append(("跌破MA20", close))

        if self.config.enable_max_hold_exit and hold_days >= self.config.max_hold_days:
            candidates.append((f"達最大持倉{self.config.max_hold_days}天", close))

        if self.config.enable_indicator_exit:
            idx = today_iloc
            if idx >= 1:
                prev_row = stock_df.iloc[idx - 1]
                if self.config.indicator_exit_mode == "rsi_50":
                    prev_rsi = prev_row.get("rsi14")
                    curr_rsi = row.get("rsi14")
                    crossed_down = (
                        pd.notna(prev_rsi)
                        and pd.notna(curr_rsi)
                        and float(prev_rsi) >= 50
                        and float(curr_rsi) < 50
                    )
                    if crossed_down:
                        candidates.append(("RSI轉弱", close))
                elif self.config.indicator_exit_mode == "macd_dead_cross":
                    prev_macd = prev_row.get("macd")
                    prev_signal = prev_row.get("macd_signal")
                    curr_macd = row.get("macd")
                    curr_signal = row.get("macd_signal")
                    dead_cross = (
                        pd.notna(prev_macd)
                        and pd.notna(prev_signal)
                        and pd.notna(curr_macd)
                        and pd.notna(curr_signal)
                        and float(prev_macd) >= float(prev_signal)
                        and float(curr_macd) < float(curr_signal)
                    )
                    if dead_cross:
                        candidates.append(("MACD死亡交叉", close))

        if not candidates:
            return None, ""

        exit_reason, sell_price = candidates[0]
        return sell_price, exit_reason

    def check_entry_condition(
        self,
        stock_id: str,
        stock_df_to_today: pd.DataFrame,
        trade_date: date,
        precomputed: bool = False,
    ) -> tuple[bool, str, float | None, float | None]:
        """檢查是否允許進場，回傳：是否允許、原因、分數、BIAS。"""
        signal = analyze_stock(stock_df_to_today, precomputed=precomputed)
        if signal is None or not signal.passes_basic():
            return False, "Signal Not Ready", None, None

        score = signal.score()
        if score < self.config.min_score:
            return False, "Score Too Low", score, signal.ma20_bias_ratio

        bias_ratio = signal.ma20_bias_ratio
        if pd.notna(bias_ratio) and bias_ratio > self.config.max_bias_ratio:
            return False, "[Skip] Bias Too High", score, bias_ratio

        if self.config.enable_market_filter:
            if self.market_df is None or self.market_df.empty:
                return False, "[Skip] Market Filter Unavailable", score, bias_ratio

            market_today = self.market_df.loc[self.market_df["date"].dt.date == trade_date]
            if market_today.empty:
                return False, "[Skip] Market Data Missing", score, bias_ratio

            market_row = market_today.iloc[0]
            market_ok = (
                pd.notna(market_row.get("market_ma20"))
                and float(market_row["close"]) > float(market_row["market_ma20"])
                and bool(market_row.get("market_ma20_slope_up", False))
            )
            if not market_ok:
                return False, "[Skip] Market Filter", score, bias_ratio

        return True, "OK", score, bias_ratio


def run_backtest(
    price_data: dict,
    config: BacktestConfig,
    progress_callback=None,
    market_df: pd.DataFrame | None = None,
) -> BacktestResult:
    result = BacktestResult(config=config)
    strategy = Strategy(
        config=config,
        market_df=_prepare_market_frame(market_df, config) if market_df is not None and not market_df.empty else None,
    )

    _LEVERAGED_ETF_RE = re.compile(r"^\d{5}[LRU]$", re.IGNORECASE)

    if config.exclude_leveraged_etf:
        price_data = {
            sid: df for sid, df in price_data.items()
            if not _LEVERAGED_ETF_RE.match(sid)
        }

    prepared_price_data = {
        stock_id: _prepare_price_frame(df, config)
        for stock_id, df in price_data.items()
        if not df.empty
    }

    # 預先建立每支股票的 date→iloc index mapping，避免每次全表掃描 O(n)→O(1)
    date_index_maps: dict[str, dict[date, int]] = {
        stock_id: {d: i for i, d in enumerate(df["date"].dt.date)}
        for stock_id, df in prepared_price_data.items()
    }

    all_dates: set[date] = set()
    for dmap in date_index_maps.values():
        all_dates.update(dmap.keys())

    start = pd.to_datetime(config.start_date).date()
    end = pd.to_datetime(config.end_date).date()
    trading_days = sorted(day for day in all_dates if start <= day <= end)
    if not trading_days:
        return result

    open_positions: dict[str, TradeRecord] = {}
    peak_prices: dict[str, float] = {}
    equity = 1.0

    for day_idx, today in enumerate(trading_days):
        if progress_callback:
            progress_callback(day_idx + 1, len(trading_days))

        day_pnl_pct = 0.0
        closed_today: list[str] = []

        for stock_id, position in list(open_positions.items()):
            stock_df = prepared_price_data.get(stock_id)
            if stock_df is None:
                continue

            today_iloc = date_index_maps[stock_id].get(today)
            if today_iloc is None:
                continue

            row = stock_df.iloc[today_iloc]
            day_high = float(row.get("max", row["close"]))
            peak_prices[stock_id] = max(peak_prices.get(stock_id, position.buy_price), day_high)

            sell_price, exit_reason = strategy.update_exit_signal(
                stock_df=stock_df,
                today=today,
                position=position,
                peak_price=peak_prices[stock_id],
                today_iloc=today_iloc,
            )
            if sell_price is None:
                continue

            pnl, pnl_pct = _calc_pnl(position.buy_price, sell_price)
            position.sell_date = today
            position.sell_price = sell_price
            position.exit_reason = exit_reason
            position.hold_days = (today - position.buy_date).days
            position.pnl = pnl
            position.pnl_pct = pnl_pct
            day_pnl_pct += pnl_pct
            closed_today.append(stock_id)

        for stock_id in closed_today:
            result.trades.append(open_positions.pop(stock_id))
            peak_prices.pop(stock_id, None)

        if day_idx + 1 < len(trading_days):
            next_day = trading_days[day_idx + 1]

            for stock_id, full_df in prepared_price_data.items():
                if stock_id in open_positions:
                    continue

                dmap = date_index_maps[stock_id]
                today_iloc = dmap.get(today)
                if today_iloc is None or today_iloc + 1 < config.warmup_days:
                    continue

                # 用 iloc 切片，避免布林遮罩全表掃描，也不用 copy（analyze_stock 內部不修改 df）
                df_to_today = full_df.iloc[: today_iloc + 1]

                allowed, reason, score, bias_ratio = strategy.check_entry_condition(
                    stock_id=stock_id,
                    stock_df_to_today=df_to_today,
                    trade_date=today,
                    precomputed=True,  # 指標已在 _prepare_price_frame 計算過，跳過重算
                )
                if not allowed:
                    if reason.startswith("[Skip]"):
                        result.skip_logs.append(
                            SkipRecord(
                                stock_id=stock_id,
                                trade_date=today,
                                reason=reason,
                                score=score or 0.0,
                                bias_ratio=bias_ratio,
                            )
                        )
                    continue

                next_iloc = dmap.get(next_day)
                if next_iloc is None:
                    continue

                buy_price = float(full_df.iloc[next_iloc].get("open", full_df.iloc[next_iloc]["close"]))
                if buy_price <= 0:
                    continue

                open_positions[stock_id] = TradeRecord(
                    stock_id=stock_id,
                    buy_date=next_day,
                    buy_price=buy_price,
                )

        if day_pnl_pct:
            equity *= 1 + day_pnl_pct / 100
        result.equity_curve.append(round(equity, 6))

    for _, position in open_positions.items():
        result.trades.append(position)

    return result


def generate_text_report(result: BacktestResult, config: BacktestConfig) -> str:
    """產生可供 AI 分析的純文字回測報告。"""
    buf = StringIO()
    w = buf.write

    now_str = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    w(f"# 回測策略分析報告\n")
    w(f"生成時間：{now_str}\n\n")

    # ── 一、策略配置 ─────────────────────────────────────────────
    w("## 一、策略配置\n\n")
    w(f"- 回測期間：{config.start_date} ～ {config.end_date}\n")
    w(f"- 最低強度分數：{config.min_score}\n")
    w(f"- 個股最大容許 BIAS：{config.max_bias_ratio}%\n")
    w(f"- 大盤 MA{config.market_ma_period} 濾網：{'啟用' if config.enable_market_filter else '關閉'}\n")
    w(f"- ATR 動態移動停損：{'啟用' if config.enable_trailing_exit else '關閉'}")
    if config.enable_trailing_exit:
        w(f"（期間={config.atr_period}，倍數={config.atr_multiplier}）")
    w("\n")
    w(f"- 跌破 MA20 出場：{'啟用' if config.enable_ma20_exit else '關閉'}\n")
    w(f"- 最大持倉天數出場：{'啟用' if config.enable_max_hold_exit else '關閉'}")
    if config.enable_max_hold_exit:
        w(f"（{config.max_hold_days} 天）")
    w("\n")
    w(f"- 技術指標停利：{'啟用' if config.enable_indicator_exit else '關閉'}")
    if config.enable_indicator_exit:
        w(f"（{config.indicator_exit_mode}）")
    w("\n")
    w(f"- 暖機天數：{config.warmup_days}\n\n")

    # ── 二、整體績效統計 ─────────────────────────────────────────
    summary = result.summary()
    if not summary:
        w("## 二、整體績效統計\n\n無已平倉交易。\n")
        return buf.getvalue()

    w("## 二、整體績效統計\n\n")
    w(f"| 指標 | 數值 |\n|------|------|\n")
    w(f"| 總交易次數 | {summary['total_trades']} 筆 |\n")
    w(f"| 勝率 | {summary['win_rate']}% ({summary['win_trades']} 勝 / {summary['loss_trades']} 負) |\n")
    w(f"| 獲利因子 (Profit Factor) | {summary['profit_factor']} |\n")
    w(f"| 總報酬 | {summary['total_return_pct']:+.2f}% |\n")
    w(f"| 最大回撤 | {summary['max_drawdown_pct']:.2f}% |\n")
    w(f"| 平均獲利（勝） | {summary['avg_win_pct']:+.2f}% |\n")
    w(f"| 平均虧損（敗） | {summary['avg_loss_pct']:+.2f}% |\n")
    w(f"| 獲損比 (Win/Loss Ratio) | {abs(summary['avg_win_pct'] / summary['avg_loss_pct']):.2f}x |\n"
      if summary['avg_loss_pct'] != 0 else "| 獲損比 | N/A |\n")
    w(f"| 平均持倉天數 | {summary['avg_hold_days']} 天 |\n")
    w(f"| 略過訊號數 | {summary.get('skip_count', 0)} 次 |\n\n")

    # ── 三、出場原因分析 ─────────────────────────────────────────
    closed = [t for t in result.trades if t.sell_date is not None]
    w("## 三、出場原因分析\n\n")
    exit_reasons = summary.get("exit_reasons", {})
    if exit_reasons:
        w("| 出場原因 | 次數 | 佔比 | 平均報酬 |\n|----------|------|------|----------|\n")
        total_closed = len(closed)
        for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
            trades_by_reason = [t for t in closed if t.exit_reason == reason]
            avg_pnl = np.mean([t.pnl_pct for t in trades_by_reason]) if trades_by_reason else 0
            pct = count / total_closed * 100
            w(f"| {reason} | {count} | {pct:.1f}% | {avg_pnl:+.2f}% |\n")
        w("\n")

    # ── 四、月度績效分析 ─────────────────────────────────────────
    w("## 四、月度績效分析（依賣出月份）\n\n")
    monthly: dict[str, list] = {}
    for t in closed:
        key = t.sell_date.strftime("%Y-%m")
        monthly.setdefault(key, []).append(t.pnl_pct)
    if monthly:
        w("| 月份 | 交易筆數 | 總報酬 | 勝率 |\n|------|----------|--------|------|\n")
        for ym in sorted(monthly.keys()):
            pnls = monthly[ym]
            total_r = sum(pnls)
            wr = sum(1 for p in pnls if p > 0) / len(pnls) * 100
            w(f"| {ym} | {len(pnls)} | {total_r:+.2f}% | {wr:.0f}% |\n")
        w("\n")

    # ── 五、個股表現統計（≥2 筆交易）────────────────────────────
    w("## 五、個股表現統計（至少 2 筆交易）\n\n")
    stock_trades: dict[str, list] = {}
    for t in closed:
        stock_trades.setdefault(t.stock_id, []).append(t.pnl_pct)
    multi_trades = {sid: pnls for sid, pnls in stock_trades.items() if len(pnls) >= 2}
    if multi_trades:
        rows = []
        for sid, pnls in multi_trades.items():
            rows.append((sid, len(pnls), sum(pnls), sum(1 for p in pnls if p > 0) / len(pnls) * 100))
        rows.sort(key=lambda x: -x[2])
        w("| 股票代號 | 交易次數 | 累計報酬 | 勝率 |\n|----------|----------|----------|------|\n")
        for sid, cnt, total_r, wr in rows:
            w(f"| {sid} | {cnt} | {total_r:+.2f}% | {wr:.0f}% |\n")
        w("\n")

    # ── 六、最佳 / 最差交易各 10 筆 ─────────────────────────────
    sorted_trades = sorted(closed, key=lambda t: t.pnl_pct, reverse=True)
    w("## 六、最佳交易（前 10 筆）\n\n")
    w("| 股票代號 | 買進日 | 賣出日 | 持倉天數 | 報酬 | 出場原因 |\n")
    w("|----------|--------|--------|----------|------|----------|\n")
    for t in sorted_trades[:10]:
        w(f"| {t.stock_id} | {t.buy_date} | {t.sell_date} | {t.hold_days} | {t.pnl_pct:+.2f}% | {t.exit_reason} |\n")
    w("\n")

    w("## 七、最差交易（後 10 筆）\n\n")
    w("| 股票代號 | 買進日 | 賣出日 | 持倉天數 | 報酬 | 出場原因 |\n")
    w("|----------|--------|--------|----------|------|----------|\n")
    for t in sorted_trades[-10:][::-1]:
        w(f"| {t.stock_id} | {t.buy_date} | {t.sell_date} | {t.hold_days} | {t.pnl_pct:+.2f}% | {t.exit_reason} |\n")
    w("\n")

    # ── 八、完整交易明細 ─────────────────────────────────────────
    w("## 八、完整交易明細\n\n")
    w("| 股票代號 | 買進日 | 買進價 | 賣出日 | 賣出價 | 持倉天數 | 報酬(%) | 出場原因 |\n")
    w("|----------|--------|--------|--------|--------|----------|---------|----------|\n")
    for t in sorted(closed, key=lambda x: x.buy_date):
        w(f"| {t.stock_id} | {t.buy_date} | {t.buy_price:.2f} | {t.sell_date} | {t.sell_price:.2f} | {t.hold_days} | {t.pnl_pct:+.3f}% | {t.exit_reason} |\n")
    w("\n")

    # ── 九、略過原因統計 ─────────────────────────────────────────
    skip_reasons = summary.get("skip_reasons", {})
    if skip_reasons:
        w("## 九、略過原因統計\n\n")
        w("| 原因 | 次數 |\n|------|------|\n")
        for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
            w(f"| {reason} | {count} |\n")
        w("\n")

    w("---\n*本報告由 srock 回測引擎自動生成，供策略優化參考。*\n")
    return buf.getvalue()


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
