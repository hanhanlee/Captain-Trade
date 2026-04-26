"""
回測模組頁面
"""

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.finmind_client import get_daily_price, get_stock_list
from db.database import init_db
from db.price_cache import (
    get_all_cached_stocks,
    get_cache_summary,
    get_cached_dates,
    init_cache_table,
    load_prices,
    save_prices,
)
from modules.backtester import BacktestConfig, generate_text_report, run_backtest

st.set_page_config(page_title="回測模組", page_icon="📉", layout="wide")
init_db()
init_cache_table()

st.title("📉 回測模組")
st.markdown("使用本機快取資料驗證策略，並加入大盤濾網與動態停損。")
st.markdown("---")

tab_backtest, tab_result = st.tabs(["設定回測參數", "查看回測結果"])


def _load_market_index(start_date: str = "2019-01-01") -> pd.DataFrame:
    """讀取加權指數歷史資料，優先使用本機快取，缺少時再補抓。"""
    market_df = load_prices("TAIEX", start_date=start_date)
    if not market_df.empty:
        return market_df

    market_df = get_daily_price("TAIEX", start_date=start_date)
    if not market_df.empty:
        save_prices("TAIEX", market_df)
    return market_df


with tab_backtest:
    st.subheader("設定回測參數")

    # ── 加權指數快取狀態 ──────────────────────────────────────────
    market_earliest, market_latest = get_cached_dates("TAIEX")
    if not market_earliest:
        st.error(
            "⚠️ 加權指數（大盤濾網必要條件）尚未下載，啟用大盤濾網時無法執行回測。  \n"
            "請前往 **6 - 資料管理** → 啟動「回測重建模式」補充歷史資料。"
        )
    else:
        stale = (date.today() - date.fromisoformat(str(market_latest))).days > 7
        if stale:
            st.warning(
                f"加權指數快取已超過 7 天未更新（最新：{market_latest}）。  \n"
                "建議前往 **6 - 資料管理** → 啟動「回測重建模式」更新。"
            )
        else:
            st.success(f"加權指數快取正常（{market_earliest} ～ {market_latest}）")

    cached_stocks = get_all_cached_stocks()
    if not cached_stocks:
        st.warning(
            "目前沒有可用快取資料。  \n"
            "請前往 **6 - 資料管理** → 啟動「回測重建模式」下載歷史資料。"
        )
        st.stop()

    st.info(f"目前可回測股票數：**{len(cached_stocks)}** 檔")

    col_p1, col_p2, col_p3 = st.columns(3)
    with col_p1:
        st.markdown("#### 回測範圍")
        bt_start = st.date_input("開始日期", value=date.today() - timedelta(days=365), min_value=date(2019, 1, 1))
        bt_end = st.date_input("結束日期", value=date.today() - timedelta(days=1))
        stock_count = st.number_input(
            "回測股票數",
            min_value=10,
            max_value=len(cached_stocks),
            value=min(100, len(cached_stocks)),
            step=10,
        )
        st.markdown("#### 資金與部位管理")
        initial_capital = st.number_input(
            "初始資金（元）", min_value=100_000, max_value=100_000_000,
            value=1_000_000, step=100_000,
            help="模擬帳戶的起始資金。帳戶淨值 = 剩餘現金 + 持倉市值。",
        )
        max_positions = st.number_input(
            "最大持倉檔數", min_value=1, max_value=20, value=5, step=1,
            help="同時最多持有幾檔。達上限時新訊號自動略過。",
        )
        risk_per_trade_pct = st.slider(
            "單筆最大風險（帳戶淨值 %）", 0.5, 5.0, 2.0, 0.5,
            help="每筆交易願意承擔的最大虧損佔帳戶淨值比例（2% 固定風險法）。",
        )

    with col_p2:
        st.markdown("#### 出場策略設定")
        enable_trailing_exit = st.checkbox("啟用 ATR 動態移動停損", value=True)
        atr_multiplier = st.slider("ATR 停損倍數", 1.0, 5.0, 2.5, 0.1) if enable_trailing_exit else 2.5
        enable_ma20_exit = st.checkbox("啟用跌破均線出場", value=True)
        ma_exit_period = 20
        if enable_ma20_exit:
            ma_exit_label = st.selectbox(
                "出場均線",
                options=["MA5（5日線）", "MA10（10日線）", "MA20（月線）"],
                index=2,
                help="收盤價跌破選定均線即出場",
            )
            ma_exit_period = int(ma_exit_label.split("MA")[1].split("（")[0])
        enable_max_hold_exit = st.checkbox("啟用最大持倉天數出場", value=True)
        max_hold = st.slider("最大持倉天數", 5, 120, 20, 5) if enable_max_hold_exit else 20
        enable_indicator_exit = st.checkbox("啟用技術指標停利法", value=False)
        indicator_exit_mode = "rsi_50"
        if enable_indicator_exit:
            indicator_label = st.selectbox("技術反轉訊號", ["RSI 跌破 50", "MACD 死亡交叉"])
            indicator_exit_mode = "rsi_50" if indicator_label == "RSI 跌破 50" else "macd_dead_cross"
        st.markdown("#### 交易成本")
        buy_fee_rate  = st.number_input("買進手續費 (%)", value=0.1425, step=0.01, format="%.4f") / 100
        sell_fee_rate = st.number_input("賣出手續費 (%)", value=0.1425, step=0.01, format="%.4f") / 100
        sell_tax_rate = st.number_input("賣出交易稅 (%)", value=0.3000, step=0.01, format="%.4f") / 100

    with col_p3:
        st.markdown("#### 進場過濾器")
        min_score = st.slider("最低強度分數", 50.0, 100.0, 65.0, 1.0)
        enable_market_filter = st.checkbox("啟用大盤 MA20 濾網", value=True)
        overheat_atr_mult = st.slider(
            "過熱防護 ATR 倍數", 0.0, 6.0, 3.5, 0.5,
            help="收盤 > MA20 + N×ATR14 時跳過進場。0 = 停用。熱門電子股可調高至 4.5~5.0。"
        )
        _ma_mode_label = st.radio(
            "三線齊穿判斷模式",
            options=["嚴謹型（昨日三線全在線下）", "寬鬆型（昨日任一線在線下）"],
            index=0,
            help="嚴謹型篩出的標的轉折更乾淨；寬鬆型標的較多但力道參差不齊。",
        )
        ma_breakout_mode = "strict" if _ma_mode_label.startswith("嚴謹") else "loose"
        exclude_all_etf = st.checkbox(
            "排除所有 ETF",
            value=False,
            help="排除代碼以 0 開頭的股票（如 0050、0056、006208 等），只保留一般上市公司。",
        )
        exclude_leveraged_etf = st.checkbox(
            "排除槓桿/反向/期貨型 ETF",
            value=True,
            disabled=exclude_all_etf,
            help="排除代碼末位為 L（槓桿）、R（反向）、U（期貨）的 ETF。勾選「排除所有 ETF」後自動涵蓋。",
        )
        allow_fractional_shares = st.checkbox(
            "允許買零股",
            value=False,
            help="資金不足買一張（1000股）時，改買最多可負擔的零股股數（最少 1 股）。",
        )
        st.markdown("---")
        st.markdown("**部位計算說明**")
        st.caption("每筆進場股數 = 單筆風險預算 ÷ (ATR × ATR倍數)，不足一張則不進場。")
        st.caption(f"設定：初始 {initial_capital:,.0f} 元，最多 {max_positions} 檔，每筆風險 {risk_per_trade_pct}%")

    st.markdown("---")

    if st.button("開始回測", type="primary", use_container_width=True):
        config = BacktestConfig(
            start_date=str(bt_start),
            end_date=str(bt_end),
            initial_capital=initial_capital,
            max_positions=max_positions,
            risk_per_trade_pct=risk_per_trade_pct,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            sell_tax_rate=sell_tax_rate,
            enable_trailing_exit=enable_trailing_exit,
            atr_multiplier=atr_multiplier,
            enable_ma20_exit=enable_ma20_exit,
            ma_exit_period=ma_exit_period,
            enable_max_hold_exit=enable_max_hold_exit,
            max_hold_days=max_hold,
            enable_indicator_exit=enable_indicator_exit,
            indicator_exit_mode=indicator_exit_mode,
            enable_market_filter=enable_market_filter,
            overheat_atr_mult=overheat_atr_mult,
            min_score=min_score,
            exclude_all_etf=exclude_all_etf,
            exclude_leveraged_etf=exclude_leveraged_etf,
            allow_fractional_shares=allow_fractional_shares,
            ma_breakout_mode=ma_breakout_mode,
        )

        with st.spinner("正在讀取本機快取資料..."):
            price_data = {}
            for stock_id in cached_stocks[:stock_count]:
                df = load_prices(stock_id, start_date="2019-01-01")
                if not df.empty:
                    price_data[stock_id] = df

            market_df = _load_market_index(start_date="2019-01-01")

        if enable_market_filter and market_df.empty:
            st.error("無法取得加權指數資料，暫時無法執行大盤濾網回測。")
            st.stop()

        progress = st.progress(0)
        status = st.empty()

        def on_progress(current: int, total: int) -> None:
            progress.progress(current / total)
            status.text(f"回測進度 {current}/{total}")

        bt_result = run_backtest(
            price_data=price_data,
            config=config,
            progress_callback=on_progress,
            market_df=market_df,
        )

        progress.empty()
        status.empty()

        st.session_state["bt_result"] = bt_result
        st.session_state["bt_config"] = config
        st.session_state["bt_market_df"] = market_df

        summary = bt_result.summary()
        if summary:
            st.success(
                f"回測完成！共 {summary['total_trades']} 筆已平倉交易，"
                f"最終淨值 **{summary['final_equity']:,.0f} 元**"
                f"（{summary['total_return_pct']:+.2f}%），"
                f"略過 {summary.get('skip_count', 0)} 筆候選。"
            )
        else:
            st.warning("回測完成，但沒有產生已平倉交易。")


with tab_result:
    st.subheader("回測結果")

    if "bt_result" not in st.session_state:
        st.info("請先執行一次回測。")
        st.stop()

    bt_result = st.session_state["bt_result"]
    config = st.session_state["bt_config"]
    summary = bt_result.summary()

    if not summary:
        st.warning("目前沒有已平倉交易可供分析。")
        st.stop()

    with st.expander("回測參數"):
        indicator_label = "RSI 跌破 50" if config.indicator_exit_mode == "rsi_50" else "MACD 死亡交叉"
        ic  = getattr(config, "initial_capital",    1_000_000)
        mp  = getattr(config, "max_positions",      5)
        rpt = getattr(config, "risk_per_trade_pct", 2.0)
        bfr = getattr(config, "buy_fee_rate",  0.001425) * 100
        sfr = getattr(config, "sell_fee_rate", 0.001425) * 100
        str_ = getattr(config, "sell_tax_rate", 0.003)   * 100
        st.markdown(
            f"""
| 參數 | 值 |
|------|----|
| 回測期間 | {config.start_date} ~ {config.end_date} |
| 初始資金 | {ic:,.0f} 元 |
| 最大持倉檔數 | {mp} 檔 |
| 單筆最大風險 | {rpt}% |
| 買進手續費 | {bfr:.4f}% |
| 賣出手續費 | {sfr:.4f}%　交易稅 {str_:.2f}% |
| 最低強度分數 | {config.min_score} |
| 三線齊穿模式 | {'嚴謹型（三線全穿）' if getattr(config, 'ma_breakout_mode', 'strict') == 'strict' else '寬鬆型（任一線即可）'} |
| ATR 動態停損 | {'開啟' if config.enable_trailing_exit else '關閉'} (倍數 {config.atr_multiplier}) |
| 跌破 MA{config.ma_exit_period} 出場 | {'開啟' if config.enable_ma20_exit else '關閉'} |
| 最大持倉天數出場 | {'開啟' if config.enable_max_hold_exit else '關閉'} ({config.max_hold_days} 天) |
| 技術指標停利法 | {'開啟' if config.enable_indicator_exit else '關閉'} ({indicator_label}) |
| 大盤濾網 | {'開啟' if config.enable_market_filter else '關閉'} |
| 過熱防護 ATR 倍數 | {getattr(config, 'overheat_atr_mult', 3.5)}x |
| 排除所有 ETF | {'是' if getattr(config, 'exclude_all_etf', False) else '否'} |
| 排除槓桿/期貨 ETF | {'是' if getattr(config, 'exclude_leveraged_etf', False) else '否'} |
"""
        )

    st.markdown("### 核心績效")
    _ic = getattr(config, "initial_capital", 1_000_000)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("初始資金", f"{_ic:,.0f} 元")
    c2.metric("最終淨值", f"{summary.get('final_equity', _ic):,.0f} 元",
              delta=f"{summary['total_return_pct']:+.2f}%")
    c3.metric("總損益", f"{summary.get('total_pnl', 0):+,.0f} 元")
    c4.metric("最大回撤（淨值曲線）", f"{summary['max_drawdown_pct']:.2f}%")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("總交易數", summary["total_trades"])
    c6.metric("勝率", f"{summary['win_rate']}%", delta=f"{summary['win_trades']} 勝 / {summary['loss_trades']} 負")
    c7.metric("獲利因子", summary["profit_factor"])
    c8.metric("平均持倉天數", f"{summary['avg_hold_days']} 天")

    c9, c10, _, _ = st.columns(4)
    c9.metric("平均獲利", f"{summary['avg_win_pct']:+.2f}%")
    c10.metric("平均虧損", f"{summary['avg_loss_pct']:+.2f}%")

    st.markdown("---")
    st.markdown("### 帳戶淨值曲線")
    if bt_result.equity_curve:
        _ic = getattr(config, "initial_capital", 1_000_000)
        _eq_dates = getattr(bt_result, "equity_dates", [])
        eq_dates = _eq_dates if _eq_dates else \
                   list(pd.date_range(start=config.start_date, periods=len(bt_result.equity_curve), freq="B"))
        fig_eq = go.Figure()
        fig_eq.add_trace(go.Scatter(
            x=eq_dates,
            y=bt_result.equity_curve,
            name="帳戶淨值",
            line=dict(color="#3498db", width=2),
            fill="tozeroy",
            fillcolor="rgba(52,152,219,0.08)",
        ))
        fig_eq.add_hline(y=_ic, line_dash="dash", line_color="#95a5a6",
                         annotation_text=f"初始資金 {_ic:,.0f}")
        fig_eq.update_layout(
            height=380, margin=dict(t=30, b=10),
            yaxis_title="帳戶淨值（元）",
            yaxis_tickformat=",",
        )
        st.plotly_chart(fig_eq, use_container_width=True)

    col_exit, col_skip = st.columns(2)
    with col_exit:
        st.markdown("### 出場原因分布")
        exit_data = summary.get("exit_reasons", {})
        if exit_data:
            fig_exit = go.Figure(
                go.Pie(
                    labels=list(exit_data.keys()),
                    values=list(exit_data.values()),
                    hole=0.4,
                    textinfo="label+percent+value",
                )
            )
            fig_exit.update_layout(height=320, margin=dict(t=20, b=10))
            st.plotly_chart(fig_exit, use_container_width=True)

    with col_skip:
        st.markdown("### 略過原因分布")
        skip_data = summary.get("skip_reasons", {})
        if skip_data:
            fig_skip = go.Figure(
                go.Pie(
                    labels=list(skip_data.keys()),
                    values=list(skip_data.values()),
                    hole=0.4,
                    textinfo="label+percent+value",
                )
            )
            fig_skip.update_layout(height=320, margin=dict(t=20, b=10))
            st.plotly_chart(fig_skip, use_container_width=True)

    # 未平倉 metric 加到核心績效旁
    open_count = summary.get("open_count", 0)
    open_pnl   = summary.get("open_unrealized_pnl", 0)
    if open_count > 0:
        st.info(
            f"回測結束時仍有 **{open_count}** 檔未平倉，"
            f"估計未實現損益 **{open_pnl:+,.0f} 元**（以期末收盤 × 扣除交易成本估算，"
            f"已含於最終淨值）。"
        )

    st.markdown("---")
    st.markdown("### 交易明細")
    closed_trades = [t for t in bt_result.trades if t.sell_date is not None]
    if closed_trades:
        trades_df = pd.DataFrame(
            [
                {
                    "股票代號": t.stock_id,
                    "買進日": t.buy_date,
                    "買進價": t.buy_price,
                    "股數": getattr(t, "shares", "-"),
                    "成本(元)": round(getattr(t, "cost_basis", 0)) or "-",
                    "賣出日": t.sell_date,
                    "賣出價": t.sell_price,
                    "持倉天數": t.hold_days,
                    "損益(元)": t.pnl,
                    "報酬(%)": t.pnl_pct,
                    "出場原因": t.exit_reason,
                }
                for t in sorted(closed_trades, key=lambda x: x.buy_date)
            ]
        )
        st.dataframe(trades_df, use_container_width=True, hide_index=True, height=360)

    # ── 未平倉部位明細 ────────────────────────────────────────────
    open_trades = [t for t in bt_result.trades if t.sell_date is None]
    if open_trades:
        st.markdown("---")
        st.markdown("### 📂 未平倉部位")
        st.caption("回測結束日仍持有、尚未出場的部位。損益以期末收盤估算，已含於最終淨值中。")

        end_prices = getattr(bt_result, "open_positions_end_prices", {})
        open_rows = []
        for t in sorted(open_trades, key=lambda x: x.buy_date):
            end_px = end_prices.get(t.stock_id)
            if end_px is not None and t.cost_basis > 0:
                sell_fee = config.sell_fee_rate + config.sell_tax_rate
                est_revenue = end_px * t.shares * (1 - sell_fee)
                unreal_pnl  = round(est_revenue - t.cost_basis)
                unreal_pct  = round((est_revenue / t.cost_basis - 1) * 100, 2)
            else:
                unreal_pnl = unreal_pct = None

            open_rows.append({
                "股票代號":     t.stock_id,
                "買進日":       t.buy_date,
                "買進價":       t.buy_price,
                "股數":         t.shares,
                "成本(元)":     round(t.cost_basis) if t.cost_basis else "-",
                "期末收盤":     end_px if end_px else "-",
                "未實現損益(元)": unreal_pnl if unreal_pnl is not None else "-",
                "未實現報酬(%)": unreal_pct if unreal_pct is not None else "-",
                "持倉天數":     t.hold_days,
                "進場分數":     round(t.entry_score, 1) if t.entry_score else "-",
            })

        open_df = pd.DataFrame(open_rows)
        st.dataframe(open_df, use_container_width=True, hide_index=True,
                     height=min(400, 40 + len(open_df) * 35))

    if bt_result.skip_logs:
        st.markdown("---")
        st.markdown("### 略過交易紀錄")
        skip_df = pd.DataFrame(
            [
                {
                    "日期": s.trade_date,
                    "股票代號": s.stock_id,
                    "原因": s.reason,
                    "分數": s.score,
                    "BIAS(%)": s.bias_ratio,
                }
                for s in bt_result.skip_logs
            ]
        )
        st.dataframe(skip_df, use_container_width=True, hide_index=True, height=320)

    st.markdown("---")
    st.markdown("### 匯出與 AI 分析")

    report_text = generate_text_report(bt_result, config)
    report_filename = f"backtest_report_{config.start_date}_{config.end_date}.md"

    col_dl, col_ai = st.columns(2)

    with col_dl:
        st.caption("下載完整報告，可自行上傳至任何 AI 平台進行分析。")
        st.download_button(
            label="📄 下載分析報告 (.md)",
            data=report_text.encode("utf-8"),
            file_name=report_filename,
            mime="text/markdown",
            use_container_width=True,
        )
        with st.expander("預覽報告內容"):
            st.text(report_text[:3000] + ("\n...(截斷預覽，完整內容請下載)" if len(report_text) > 3000 else ""))

    with col_ai:
        st.caption("使用 Gemini 2.0 Flash 直接在頁面內解讀回測結果。")
        ai_btn = st.button(
            "🤖 AI 深度解讀回測結果",
            use_container_width=True,
            type="primary",
        )

    if ai_btn:
        with st.spinner("AI 正在分析數百筆交易紀錄，請稍候..."):
            from agents.reviewer import analyze_backtest_report
            ai_result = analyze_backtest_report(report_text)

        st.markdown("---")
        st.markdown("### 🤖 AI 深度解讀")
        st.markdown(ai_result)
        st.markdown(
            "*使用模型：Gemini 2.5 Flash*\n\n"
            "> ⚠️ **提示：** 此內容為 AI 自動生成，僅供參考。"
            "若需使用更進階的模型（如 Claude 3.5 Sonnet 或 Gemini 1.5 Pro）進行私密深度分析，"
            "請點擊上方「📄 下載分析報告」並自行上傳至對應平台。"
        )
