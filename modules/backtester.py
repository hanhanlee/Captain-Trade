"""
回測引擎 v2 — 真實資金池模擬

設計原則：
1. 進場只使用當日以前已知資料，於次交易日開盤買進，避免前視偏差。
2. 出場條件模組化；任一已啟用條件先觸發就出場。
3. 真實資金池：初始資金 + 最大持倉數 + 2% 固定風險部位控管。
4. 擇優機制：同日多個進場訊號時依強度分數排序，優先買進分數最高者。
5. 每日結算真實帳戶淨值（現金 + 持倉市值），MDD 與報酬由淨值曲線計算。
6. 台股整張制：部位以 1000 股為最小單位向下取整。
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


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class BacktestConfig:
    start_date: str
    end_date: str
    # ── 資金與部位管理 ──────────────────────────────────────────────
    initial_capital: float = 1_000_000.0   # 初始資金（元）
    max_positions: int = 5                  # 最大持倉檔數
    risk_per_trade_pct: float = 2.0         # 單筆最大風險（帳戶淨值的 %）
    # ── 交易成本 ────────────────────────────────────────────────────
    buy_fee_rate: float = 0.001425
    sell_fee_rate: float = 0.001425
    sell_tax_rate: float = 0.003
    # ── 出場條件 ────────────────────────────────────────────────────
    enable_trailing_exit: bool = True
    atr_period: int = 14
    atr_multiplier: float = 2.5
    enable_ma20_exit: bool = True
    enable_max_hold_exit: bool = True
    max_hold_days: int = 20
    enable_indicator_exit: bool = False
    indicator_exit_mode: str = "rsi_50"    # rsi_50 / macd_dead_cross
    # ── 進場過濾 ────────────────────────────────────────────────────
    enable_market_filter: bool = True
    market_ma_period: int = 20
    max_bias_ratio: float = 10.0
    min_score: float = 65.0
    warmup_days: int = 60
    exclude_leveraged_etf: bool = True     # 排除代碼末位 L/R/U 的 ETF


# ──────────────────────────────────────────────────────────────────────────────
# Data classes
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    stock_id: str
    buy_date: date
    buy_price: float
    shares: int = 0                        # 持有股數（台股整張=1000股）
    cost_basis: float = 0.0               # 含手續費買入總成本（元）
    sell_date: date | None = None
    sell_price: float | None = None
    sell_revenue: float | None = None     # 含費用賣出所得（元）
    exit_reason: str = ""
    hold_days: int = 0
    pnl: float = 0.0                      # 實際損益（元）
    pnl_pct: float = 0.0                  # 報酬率（%，相對於 cost_basis）
    entry_score: float = 0.0              # 進場時強度分數（供擇優排序記錄）


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
    equity_curve: list = field(default_factory=list)   # 每日帳戶總淨值（元）
    equity_dates: list = field(default_factory=list)   # 對應日期
    skip_logs: list = field(default_factory=list)

    def summary(self) -> dict:
        closed = [t for t in self.trades if t.sell_date is not None]
        if not closed:
            return {}

        wins   = [t for t in closed if t.pnl_pct > 0]
        losses = [t for t in closed if t.pnl_pct <= 0]

        avg_win  = np.mean([t.pnl_pct for t in wins])  if wins   else 0.0
        avg_loss = np.mean([t.pnl_pct for t in losses]) if losses else 0.0

        win_pnl  = sum(t.pnl for t in wins)
        loss_pnl = abs(sum(t.pnl for t in losses))
        profit_factor = win_pnl / loss_pnl if loss_pnl > 0 else float("inf")

        # MDD 與總報酬從真實淨值曲線計算
        max_dd = 0.0
        initial = self.config.initial_capital
        if len(self.equity_curve) > 1:
            peak = self.equity_curve[0]
            for v in self.equity_curve:
                if v > peak:
                    peak = v
                dd = (v - peak) / peak * 100
                if dd < max_dd:
                    max_dd = dd

        total_return = (self.equity_curve[-1] / initial - 1) * 100 if self.equity_curve else 0.0
        final_equity = self.equity_curve[-1] if self.equity_curve else initial
        total_pnl    = final_equity - initial

        skip_counts = (
            pd.Series([s.reason for s in self.skip_logs]).value_counts().to_dict()
            if self.skip_logs else {}
        )

        return {
            "total_trades":      len(closed),
            "win_trades":        len(wins),
            "loss_trades":       len(losses),
            "win_rate":          round(len(wins) / len(closed) * 100, 1),
            "avg_win_pct":       round(avg_win, 2),
            "avg_loss_pct":      round(avg_loss, 2),
            "profit_factor":     round(profit_factor, 2),
            "max_drawdown_pct":  round(max_dd, 2),
            "total_return_pct":  round(total_return, 2),
            "total_pnl":         round(total_pnl),
            "initial_capital":   initial,
            "final_equity":      round(final_equity),
            "avg_hold_days":     round(np.mean([t.hold_days for t in closed]), 1),
            "exit_reasons":      pd.Series([t.exit_reason for t in closed]).value_counts().to_dict(),
            "skip_reasons":      skip_counts,
            "skip_count":        len(self.skip_logs),
        }


# ──────────────────────────────────────────────────────────────────────────────
# 核心計算工具
# ──────────────────────────────────────────────────────────────────────────────

def _calc_buy_cost(buy_price: float, shares: int, config: BacktestConfig) -> float:
    """含手續費的買入總成本（元）"""
    return buy_price * shares * (1 + config.buy_fee_rate)


def _calc_sell_revenue(sell_price: float, shares: int, config: BacktestConfig) -> float:
    """含手續費與交易稅的賣出所得（元）"""
    return sell_price * shares * (1 - config.sell_fee_rate - config.sell_tax_rate)


def _calc_exit(
    sell_price: float,
    position: TradeRecord,
    config: BacktestConfig,
) -> tuple[float, float, float]:
    """回傳 (pnl_ntd, pnl_pct, sell_revenue)"""
    revenue  = _calc_sell_revenue(sell_price, position.shares, config)
    pnl      = revenue - position.cost_basis
    pnl_pct  = pnl / position.cost_basis * 100 if position.cost_basis > 0 else 0.0
    return round(pnl), round(pnl_pct, 3), round(revenue)


def _calc_position_size(
    entry_price: float,
    atr_val: float,
    equity: float,
    available_cash: float,
    config: BacktestConfig,
) -> int:
    """
    2% 固定風險法部位計算（台股整張制）

    risk_per_share = ATR × ATR倍數（與移動停損一致）
    若 ATR 無效，退而以進場價 5% 為風險估算。
    最終取「風險上限」與「現金上限」兩者較小值，向下取整到整張（1000股）。
    """
    risk_budget    = equity * config.risk_per_trade_pct / 100
    risk_per_share = atr_val * config.atr_multiplier if atr_val > 0 else entry_price * 0.05

    if risk_per_share <= 0 or entry_price <= 0:
        return 0

    shares_by_risk = risk_budget / risk_per_share
    shares_by_cash = available_cash / (entry_price * (1 + config.buy_fee_rate))

    raw_shares = min(shares_by_risk, shares_by_cash)
    lots = int(raw_shares / 1000)
    return lots * 1000   # 0 代表買不起一張


def _prepare_price_frame(df: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    prepared = compute_indicators(df)
    prepared = prepared.copy()
    prepared["atr14"] = atr(prepared, period=config.atr_period)
    return prepared


def _prepare_market_frame(df: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    market = df.copy().sort_values("date").reset_index(drop=True)
    market["market_ma20"]           = sma(market["close"], config.market_ma_period)
    market["market_ma20_slope_up"]  = market["market_ma20"] > market["market_ma20"].shift(1)
    return market


# ──────────────────────────────────────────────────────────────────────────────
# Strategy（出場 + 進場條件）
# ──────────────────────────────────────────────────────────────────────────────

class Strategy:
    def __init__(self, config: BacktestConfig, market_df: pd.DataFrame | None = None):
        self.config    = config
        self.market_df = market_df

    def update_exit_signal(
        self,
        stock_df: pd.DataFrame,
        today: date,
        position: TradeRecord,
        peak_price: float,
        today_iloc: int | None = None,
    ) -> tuple[float | None, str]:
        """根據當前持倉更新出場訊號。回傳 (sell_price, exit_reason) 或 (None, '')。"""
        if today_iloc is not None:
            row = stock_df.iloc[today_iloc]
        else:
            today_rows = stock_df.loc[stock_df["date"].dt.date == today]
            if today_rows.empty:
                return None, ""
            row       = today_rows.iloc[0]
            today_iloc = today_rows.index[0]

        low       = float(row.get("min",  row["close"]))
        close     = float(row["close"])
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
                    if (pd.notna(prev_rsi) and pd.notna(curr_rsi)
                            and float(prev_rsi) >= 50 and float(curr_rsi) < 50):
                        candidates.append(("RSI轉弱", close))
                elif self.config.indicator_exit_mode == "macd_dead_cross":
                    pm, ps = prev_row.get("macd"), prev_row.get("macd_signal")
                    cm, cs = row.get("macd"),      row.get("macd_signal")
                    if (pd.notna(pm) and pd.notna(ps) and pd.notna(cm) and pd.notna(cs)
                            and float(pm) >= float(ps) and float(cm) < float(cs)):
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
        """檢查是否允許進場。回傳 (allowed, reason, score, bias_ratio)。"""
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
            mr = market_today.iloc[0]
            market_ok = (
                pd.notna(mr.get("market_ma20"))
                and float(mr["close"]) > float(mr["market_ma20"])
                and bool(mr.get("market_ma20_slope_up", False))
            )
            if not market_ok:
                return False, "[Skip] Market Filter", score, bias_ratio

        return True, "OK", score, bias_ratio


# ──────────────────────────────────────────────────────────────────────────────
# 回測主引擎
# ──────────────────────────────────────────────────────────────────────────────

def run_backtest(
    price_data: dict,
    config: BacktestConfig,
    progress_callback=None,
    market_df: pd.DataFrame | None = None,
) -> BacktestResult:
    """
    時間序列事件驅動回測。

    每個交易日依序：
      1. 執行昨日決定的進場（扣現金、轉入持倉）
      2. 評估各持倉出場訊號（賣出、回收現金）
      3. 計算當日帳戶總淨值（現金 + 持倉市值）並記錄
      4. 掃描進場候選股，依分數擇優，預約明日進場
    """
    result   = BacktestResult(config=config)
    strategy = Strategy(
        config=config,
        market_df=_prepare_market_frame(market_df, config)
                  if market_df is not None and not market_df.empty else None,
    )

    # 排除槓桿/反向/期貨 ETF
    _LEVERAGED_ETF_RE = re.compile(r"^\d{5}[LRU]$", re.IGNORECASE)
    if config.exclude_leveraged_etf:
        price_data = {sid: df for sid, df in price_data.items()
                      if not _LEVERAGED_ETF_RE.match(sid)}

    # 預算所有股票技術指標（只做一次）
    prepared: dict[str, pd.DataFrame] = {
        sid: _prepare_price_frame(df, config)
        for sid, df in price_data.items() if not df.empty
    }

    # 預建 date→iloc 映射，O(1) 日期查找
    date_maps: dict[str, dict[date, int]] = {
        sid: {d: i for i, d in enumerate(df["date"].dt.date)}
        for sid, df in prepared.items()
    }

    all_dates: set[date] = set()
    for dm in date_maps.values():
        all_dates.update(dm.keys())

    start_d = pd.to_datetime(config.start_date).date()
    end_d   = pd.to_datetime(config.end_date).date()
    trading_days = sorted(d for d in all_dates if start_d <= d <= end_d)
    if not trading_days:
        return result

    # ── 帳戶狀態 ──────────────────────────────────────────────────
    cash: float                          = config.initial_capital
    open_positions: dict[str, TradeRecord] = {}   # 已在市場中的持倉
    pending_buys:   dict[str, TradeRecord] = {}   # 今日決定、明日開盤執行
    peak_prices:    dict[str, float]       = {}

    for day_idx, today in enumerate(trading_days):
        if progress_callback:
            progress_callback(day_idx + 1, len(trading_days))

        # ── 1. 執行昨日決定的進場（扣現金）─────────────────────────
        for sid, pending in list(pending_buys.items()):
            dmap     = date_maps.get(sid, {})
            buy_iloc = dmap.get(pending.buy_date)
            if buy_iloc is None:
                # 下一交易日無資料，跳過
                result.skip_logs.append(
                    SkipRecord(sid, pending.buy_date, "[Skip] 進場日無資料", pending.entry_score)
                )
                continue
            # 確認用明日開盤價（可能與決策時預估一致，也可能有跳空）
            actual_open = float(prepared[sid].iloc[buy_iloc].get("open",
                                 prepared[sid].iloc[buy_iloc]["close"]))
            if actual_open <= 0:
                continue
            # 重新計算實際成本（開盤可能跳空）
            cost = _calc_buy_cost(actual_open, pending.shares, config)
            if cost > cash + 1:   # 允許 1 元浮點誤差
                result.skip_logs.append(
                    SkipRecord(sid, pending.buy_date, "[Skip] 進場當日資金不足", pending.entry_score)
                )
                continue
            pending.buy_price  = actual_open
            pending.cost_basis = cost
            cash              -= cost
            open_positions[sid] = pending
        pending_buys.clear()

        # ── 2. 評估出場訊號 ──────────────────────────────────────
        closed_today: list[str] = []
        for sid, pos in list(open_positions.items()):
            df      = prepared.get(sid)
            if df is None:
                continue
            t_iloc  = date_maps[sid].get(today)
            if t_iloc is None:
                continue

            row      = df.iloc[t_iloc]
            day_high = float(row.get("max", row["close"]))
            peak_prices[sid] = max(peak_prices.get(sid, pos.buy_price), day_high)

            sell_price, exit_reason = strategy.update_exit_signal(
                stock_df=df, today=today, position=pos,
                peak_price=peak_prices[sid], today_iloc=t_iloc,
            )
            if sell_price is None:
                continue

            pnl, pnl_pct, revenue = _calc_exit(sell_price, pos, config)
            pos.sell_date    = today
            pos.sell_price   = sell_price
            pos.sell_revenue = revenue
            pos.exit_reason  = exit_reason
            pos.hold_days    = (today - pos.buy_date).days
            pos.pnl          = pnl
            pos.pnl_pct      = pnl_pct
            cash            += revenue
            closed_today.append(sid)

        for sid in closed_today:
            result.trades.append(open_positions.pop(sid))
            peak_prices.pop(sid, None)

        # ── 3. 計算當日帳戶總淨值 ───────────────────────────────
        mtm = 0.0
        for sid, pos in open_positions.items():
            df     = prepared.get(sid)
            t_iloc = date_maps.get(sid, {}).get(today)
            if df is not None and t_iloc is not None:
                close_px = float(df.iloc[t_iloc]["close"])
            else:
                close_px = pos.buy_price   # 無當日資料，用成本價代替
            mtm += pos.shares * close_px

        daily_equity = cash + mtm
        result.equity_curve.append(round(daily_equity, 2))
        result.equity_dates.append(today)

        # ── 4. 掃描進場候選（明日開盤執行）─────────────────────
        available_slots = config.max_positions - len(open_positions) - len(pending_buys)
        if available_slots <= 0 or day_idx + 1 >= len(trading_days):
            continue

        next_day = trading_days[day_idx + 1]

        # 收集所有符合條件的候選股
        candidates: list[tuple[float, str, int, float]] = []
        # (score, stock_id, next_iloc, atr_val_today)

        for sid, full_df in prepared.items():
            if sid in open_positions or sid in pending_buys:
                continue

            dmap       = date_maps[sid]
            t_iloc     = dmap.get(today)
            if t_iloc is None or t_iloc + 1 < config.warmup_days:
                continue

            df_slice = full_df.iloc[: t_iloc + 1]
            allowed, reason, score, bias_ratio = strategy.check_entry_condition(
                stock_id=sid,
                stock_df_to_today=df_slice,
                trade_date=today,
                precomputed=True,
            )
            if not allowed:
                if reason.startswith("[Skip]"):
                    result.skip_logs.append(
                        SkipRecord(sid, today, reason, score or 0.0, bias_ratio)
                    )
                continue

            next_iloc = dmap.get(next_day)
            if next_iloc is None:
                continue

            atr_val = float(full_df.iloc[t_iloc].get("atr14") or 0)
            candidates.append((score or 0.0, sid, next_iloc, atr_val))

        # 依分數由高到低排序，只取可用槽位數
        candidates.sort(key=lambda x: -x[0])

        available_cash = cash   # 這一輪迴圈中逐步扣減
        for score, sid, next_iloc, atr_val in candidates[:available_slots]:
            full_df    = prepared[sid]
            next_row   = full_df.iloc[next_iloc]
            est_price  = float(next_row.get("open", next_row["close"]))
            if est_price <= 0:
                continue

            shares = _calc_position_size(
                entry_price=est_price,
                atr_val=atr_val,
                equity=daily_equity,
                available_cash=available_cash,
                config=config,
            )
            if shares <= 0:
                result.skip_logs.append(
                    SkipRecord(sid, today, "[Skip] 資金不足（無法買進一張）", score)
                )
                continue

            est_cost = _calc_buy_cost(est_price, shares, config)
            if est_cost > available_cash + 1:
                result.skip_logs.append(
                    SkipRecord(sid, today, "[Skip] 資金不足", score)
                )
                continue

            available_cash -= est_cost
            pending_buys[sid] = TradeRecord(
                stock_id    = sid,
                buy_date    = next_day,
                buy_price   = est_price,   # 實際執行時會用真實開盤價覆蓋
                shares      = shares,
                cost_basis  = est_cost,    # 實際執行時會重算
                entry_score = score,
            )

    # 未平倉部位（回測結束仍持有）
    for pos in open_positions.values():
        result.trades.append(pos)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# 報告生成
# ──────────────────────────────────────────────────────────────────────────────

def generate_text_report(result: BacktestResult, config: BacktestConfig) -> str:
    """產生可供 AI 分析的純文字回測報告。"""
    buf = StringIO()
    w   = buf.write

    now_str = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    w("# 回測策略分析報告\n")
    w(f"生成時間：{now_str}\n\n")

    # ── 一、策略配置 ──────────────────────────────────────────────
    w("## 一、策略配置\n\n")
    w(f"- 回測期間：{config.start_date} ～ {config.end_date}\n")
    _ic  = getattr(config, "initial_capital",    1_000_000)
    _mp  = getattr(config, "max_positions",      5)
    _rpt = getattr(config, "risk_per_trade_pct", 2.0)
    _bfr = getattr(config, "buy_fee_rate",  0.001425)
    _sfr = getattr(config, "sell_fee_rate", 0.001425)
    _str = getattr(config, "sell_tax_rate", 0.003)
    w(f"- 初始資金：{_ic:,.0f} 元\n")
    w(f"- 最大持倉檔數：{_mp} 檔\n")
    w(f"- 單筆最大風險：帳戶淨值 {_rpt}%\n")
    w(f"- 買進手續費：{_bfr*100:.4f}%\n")
    w(f"- 賣出手續費：{_sfr*100:.4f}%　交易稅：{_str*100:.2f}%\n")
    w(f"- 最低強度分數：{config.min_score}\n")
    w(f"- 個股最大容許 BIAS：{config.max_bias_ratio}%\n")
    w(f"- 大盤 MA{config.market_ma_period} 濾網：{'啟用' if config.enable_market_filter else '關閉'}\n")
    w(f"- ATR 動態移動停損：{'啟用' if config.enable_trailing_exit else '關閉'}"
      + (f"（期間={config.atr_period}，倍數={config.atr_multiplier}）" if config.enable_trailing_exit else "") + "\n")
    w(f"- 跌破 MA20 出場：{'啟用' if config.enable_ma20_exit else '關閉'}\n")
    w(f"- 最大持倉天數出場：{'啟用' if config.enable_max_hold_exit else '關閉'}"
      + (f"（{config.max_hold_days} 天）" if config.enable_max_hold_exit else "") + "\n")
    w(f"- 技術指標停利：{'啟用' if config.enable_indicator_exit else '關閉'}"
      + (f"（{config.indicator_exit_mode}）" if config.enable_indicator_exit else "") + "\n\n")

    # ── 二、整體績效統計 ───────────────────────────────────────────
    summary = result.summary()
    if not summary:
        w("## 二、整體績效統計\n\n無已平倉交易。\n")
        return buf.getvalue()

    w("## 二、整體績效統計\n\n")
    w("| 指標 | 數值 |\n|------|------|\n")
    w(f"| 初始資金 | {summary.get('initial_capital', 0):,.0f} 元 |\n")
    w(f"| 最終淨值 | {summary.get('final_equity', 0):,.0f} 元 |\n")
    w(f"| 總損益 | {summary.get('total_pnl', 0):+,.0f} 元 |\n")
    w(f"| 總報酬 | {summary['total_return_pct']:+.2f}% |\n")
    w(f"| 最大回撤 | {summary['max_drawdown_pct']:.2f}% |\n")
    w(f"| 總交易次數 | {summary['total_trades']} 筆 |\n")
    w(f"| 勝率 | {summary['win_rate']}% ({summary['win_trades']} 勝 / {summary['loss_trades']} 負) |\n")
    w(f"| 獲利因子 | {summary['profit_factor']} |\n")
    w(f"| 平均獲利（勝） | {summary['avg_win_pct']:+.2f}% |\n")
    w(f"| 平均虧損（敗） | {summary['avg_loss_pct']:+.2f}% |\n")
    w((f"| 獲損比 | {abs(summary['avg_win_pct'] / summary['avg_loss_pct']):.2f}x |\n"
       if summary['avg_loss_pct'] != 0 else "| 獲損比 | N/A |\n"))
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
            trades_r = [t for t in closed if t.exit_reason == reason]
            avg_pnl  = np.mean([t.pnl_pct for t in trades_r]) if trades_r else 0
            w(f"| {reason} | {count} | {count/total_closed*100:.1f}% | {avg_pnl:+.2f}% |\n")
        w("\n")

    # ── 四、月度績效分析 ─────────────────────────────────────────
    w("## 四、月度績效分析（依賣出月份）\n\n")
    monthly: dict[str, list] = {}
    for t in closed:
        monthly.setdefault(t.sell_date.strftime("%Y-%m"), []).append(t.pnl_pct)
    if monthly:
        w("| 月份 | 交易筆數 | 總報酬(%) | 勝率 |\n|------|----------|-----------|------|\n")
        for ym in sorted(monthly.keys()):
            pnls = monthly[ym]
            w(f"| {ym} | {len(pnls)} | {sum(pnls):+.2f}% | {sum(1 for p in pnls if p>0)/len(pnls)*100:.0f}% |\n")
        w("\n")

    # ── 五、個股表現統計（≥2 筆）────────────────────────────────
    w("## 五、個股表現統計（至少 2 筆交易）\n\n")
    stock_trades: dict[str, list] = {}
    for t in closed:
        stock_trades.setdefault(t.stock_id, []).append(t.pnl_pct)
    multi = {sid: ps for sid, ps in stock_trades.items() if len(ps) >= 2}
    if multi:
        rows = [(sid, len(ps), sum(ps), sum(1 for p in ps if p > 0)/len(ps)*100)
                for sid, ps in multi.items()]
        rows.sort(key=lambda x: -x[2])
        w("| 股票代號 | 交易次數 | 累計報酬(%) | 勝率 |\n|----------|----------|-------------|------|\n")
        for sid, cnt, total_r, wr in rows:
            w(f"| {sid} | {cnt} | {total_r:+.2f}% | {wr:.0f}% |\n")
        w("\n")

    # ── 六、最佳 / 最差交易各 10 筆 ─────────────────────────────
    sorted_trades = sorted(closed, key=lambda t: t.pnl_pct, reverse=True)
    for title, trades_subset in [("最佳交易（前 10 筆）", sorted_trades[:10]),
                                  ("最差交易（後 10 筆）", sorted_trades[-10:][::-1])]:
        w(f"## 六、{title}\n\n")
        w("| 股票代號 | 買進日 | 賣出日 | 持倉天數 | 股數 | 損益(元) | 報酬(%) | 出場原因 |\n")
        w("|----------|--------|--------|----------|------|----------|---------|----------|\n")
        for t in trades_subset:
            w(f"| {t.stock_id} | {t.buy_date} | {t.sell_date} | {t.hold_days} "
              f"| {getattr(t, 'shares', '-')} | {t.pnl:+,.0f} | {t.pnl_pct:+.2f}% | {t.exit_reason} |\n")
        w("\n")

    # ── 七、完整交易明細 ─────────────────────────────────────────
    w("## 七、完整交易明細\n\n")
    w("| 股票代號 | 買進日 | 買進價 | 股數 | 賣出日 | 賣出價 | 持倉天數 | 損益(元) | 報酬(%) | 出場原因 |\n")
    w("|----------|--------|--------|------|--------|--------|----------|----------|---------|----------|\n")
    for t in sorted(closed, key=lambda x: x.buy_date):
        w(f"| {t.stock_id} | {t.buy_date} | {t.buy_price:.2f} | {getattr(t, 'shares', '-')} "
          f"| {t.sell_date} | {t.sell_price:.2f} | {t.hold_days} "
          f"| {t.pnl:+,.0f} | {t.pnl_pct:+.3f}% | {t.exit_reason} |\n")
    w("\n")

    # ── 八、略過原因統計 ─────────────────────────────────────────
    skip_reasons = summary.get("skip_reasons", {})
    if skip_reasons:
        w("## 八、略過原因統計\n\n")
        w("| 原因 | 次數 |\n|------|------|\n")
        for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
            w(f"| {reason} | {count} |\n")
        w("\n")

    w("---\n*本報告由 srock 回測引擎自動生成，供策略優化參考。*\n")
    return buf.getvalue()


# ──────────────────────────────────────────────────────────────────────────────
# 大盤比較
# ──────────────────────────────────────────────────────────────────────────────

def compare_to_benchmark(
    result: BacktestResult,
    benchmark_df: pd.DataFrame,
    config: BacktestConfig,
) -> dict:
    if benchmark_df.empty or not result.equity_curve:
        return {}

    start = pd.to_datetime(config.start_date)
    end   = pd.to_datetime(config.end_date)
    bm    = benchmark_df[(benchmark_df["date"] >= start) & (benchmark_df["date"] <= end)].copy()
    bm    = bm.sort_values("date")
    if bm.empty or len(bm) < 2:
        return {}

    bm_return       = (bm["close"].iloc[-1] / bm["close"].iloc[0] - 1) * 100
    strategy_return = (result.equity_curve[-1] / config.initial_capital - 1) * 100
    alpha           = strategy_return - bm_return

    return {
        "strategy_return_pct":  round(strategy_return, 2),
        "benchmark_return_pct": round(bm_return, 2),
        "alpha_pct":            round(alpha, 2),
    }
