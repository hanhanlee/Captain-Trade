"""
回測模組頁面
"""

from datetime import date, timedelta
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.finmind_client import get_daily_price, get_stock_list
from db.database import init_db
from db.price_cache import (
    get_all_cached_stocks,
    get_cache_summary,
    init_cache_table,
    load_prices,
    save_prices,
)
from modules.backtester import BacktestConfig, compare_to_benchmark, run_backtest

st.set_page_config(page_title="回測模組", page_icon="📉", layout="wide")
init_db()
init_cache_table()

st.title("📉 回測模組")
st.markdown("使用本機快取資料做策略回測，避免每次都重新打 API。")
st.markdown("---")

tab_download, tab_backtest, tab_result = st.tabs(
    ["下載回測資料", "設定回測參數", "查看回測結果"]
)


with tab_download:
    st.subheader("下載與檢查歷史資料")
    st.info("回測前可先確認本機快取覆蓋範圍，必要時再補抓歷史日 K。")

    summary = get_cache_summary()
    if not summary.empty:
        col_s1, col_s2, col_s3 = st.columns(3)
        col_s1.metric("已快取股票數", f"{len(summary)} 檔")
        col_s2.metric("最早資料日", str(summary["earliest"].min()))
        col_s3.metric("最新資料日", str(summary["latest"].max()))
        with st.expander("檢視快取明細"):
            st.dataframe(summary, use_container_width=True, hide_index=True)
    else:
        st.warning("目前尚無任何回測快取資料。")

    st.markdown("---")

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        download_count = st.number_input(
            "下載股票檔數",
            min_value=20,
            max_value=600,
            value=100,
            step=20,
        )
        start_year = st.selectbox(
            "起始年度",
            options=[2019, 2020, 2021, 2022, 2023],
            index=0,
        )

    with col_d2:
        st.markdown("**API 使用估算**")
        st.markdown(f"- 本次預計抓取 **{download_count}** 檔")
        st.markdown("- 免費額度約每小時 600 次")
        st.markdown(f"- 執行後剩餘預估額度 **{600 - download_count}** 次")
        if download_count > 500:
            st.warning("這次請求量接近免費額度上限，建議分批下載。")

    if st.button("開始下載歷史資料", type="primary", use_container_width=True):
        with st.spinner("正在取得股票清單..."):
            stock_list = get_stock_list()
            if stock_list.empty:
                st.error("無法取得股票清單，請先確認 API Token。")
                st.stop()

        target_ids = stock_list["stock_id"].head(download_count).tolist()
        start_date_str = f"{start_year}-01-01"
        progress = st.progress(0)
        status = st.empty()
        success = 0
        skip = 0
        fail = 0

        cache_summary = get_cache_summary()

        for idx, stock_id in enumerate(target_ids):
            progress.progress((idx + 1) / len(target_ids))
            status.text(f"下載中 {stock_id} ({idx + 1}/{len(target_ids)})")
            try:
                if not cache_summary.empty and stock_id in cache_summary["stock_id"].values:
                    row = cache_summary.loc[cache_summary["stock_id"] == stock_id].iloc[0]
                    if str(row["earliest"]) <= start_date_str:
                        skip += 1
                        continue

                df = get_daily_price(stock_id, start_date=start_date_str)
                if df.empty:
                    fail += 1
                else:
                    save_prices(stock_id, df)
                    success += 1

                time.sleep(0.1)
            except Exception:
                fail += 1

        progress.empty()
        status.empty()
        st.success(f"下載完成，成功 {success} 檔，已覆蓋略過 {skip} 檔，失敗 {fail} 檔。")
        st.rerun()


with tab_backtest:
    st.subheader("設定回測參數")

    cached_stocks = get_all_cached_stocks()
    if not cached_stocks:
        st.warning("目前沒有任何可用快取資料，請先到上一頁籤下載。")
        st.stop()

    st.info(f"目前可回測股票數：**{len(cached_stocks)}** 檔")

    col_p1, col_p2, col_p3 = st.columns(3)

    with col_p1:
        st.markdown("#### 回測範圍")
        bt_start = st.date_input(
            "開始日期",
            value=date.today() - timedelta(days=365),
            min_value=date(2019, 1, 1),
        )
        bt_end = st.date_input(
            "結束日期",
            value=date.today() - timedelta(days=1),
        )
        stock_count = st.number_input(
            "回測股票數",
            min_value=10,
            max_value=len(cached_stocks),
            value=min(100, len(cached_stocks)),
            step=10,
        )

    with col_p2:
        st.markdown("#### 出場策略設定")
        enable_trailing_exit = st.checkbox("啟用移動停損/停利", value=True)
        if enable_trailing_exit:
            trailing_stop = st.slider("停損 (%)", 3.0, 20.0, 8.0, 0.5)
            trailing_tp_act = st.slider("停利啟動門檻 (%)", 5.0, 50.0, 15.0, 1.0)
            trailing_tp = st.slider("停利回撤 (%)", 1.0, 15.0, 5.0, 0.5)
        else:
            trailing_stop = 8.0
            trailing_tp_act = 15.0
            trailing_tp = 5.0

        enable_ma20_exit = st.checkbox("啟用跌破月線 (MA20) 出場", value=True)
        enable_max_hold_exit = st.checkbox("啟用最大持倉天數出場", value=True)
        if enable_max_hold_exit:
            max_hold = st.slider("最大持倉天數", 5, 120, 20, 5)
        else:
            max_hold = 20

        enable_indicator_exit = st.checkbox("啟用技術指標停利法", value=False)
        indicator_exit_mode = "rsi_50"
        if enable_indicator_exit:
            indicator_exit_label = st.selectbox(
                "技術反轉訊號",
                ["RSI 跌破 50", "MACD 死亡交叉"],
            )
            indicator_exit_mode = (
                "rsi_50" if indicator_exit_label == "RSI 跌破 50" else "macd_dead_cross"
            )

    with col_p3:
        st.markdown("#### 進場門檻")
        min_score = st.slider("最低強度分數", 50.0, 100.0, 65.0, 1.0)
        st.markdown("---")
        st.markdown("**參數摘要**")
        if enable_trailing_exit:
            rr = trailing_tp_act / trailing_stop
            st.markdown(f"停利啟動/停損比 = {rr:.1f} : 1")
            st.caption(
                f"停損 {trailing_stop}% ；最大漲幅達 {trailing_tp_act}% 後，改用 {trailing_tp}% 移動停利。"
            )
        else:
            st.caption("目前未啟用移動停損/停利，將僅使用其他勾選的出場條件。")

        enabled_exits = []
        if enable_trailing_exit:
            enabled_exits.append("移動停損/停利")
        if enable_ma20_exit:
            enabled_exits.append("跌破 MA20")
        if enable_max_hold_exit:
            enabled_exits.append("最大持倉天數")
        if enable_indicator_exit:
            enabled_exits.append("技術指標停利")
        st.caption("已啟用出場條件：" + ("、".join(enabled_exits) if enabled_exits else "無"))

    st.markdown("---")

    if st.button("開始回測", type="primary", use_container_width=True):
        config = BacktestConfig(
            start_date=str(bt_start),
            end_date=str(bt_end),
            enable_trailing_exit=enable_trailing_exit,
            trailing_stop_pct=trailing_stop,
            trailing_tp_activation_pct=trailing_tp_act,
            trailing_tp_pct=trailing_tp,
            enable_ma20_exit=enable_ma20_exit,
            enable_max_hold_exit=enable_max_hold_exit,
            max_hold_days=max_hold,
            enable_indicator_exit=enable_indicator_exit,
            indicator_exit_mode=indicator_exit_mode,
            min_score=min_score,
        )

        target_ids = cached_stocks[:stock_count]
        with st.spinner("正在讀取本機快取資料..."):
            price_data = {}
            for stock_id in target_ids:
                df = load_prices(stock_id, start_date="2019-01-01")
                if not df.empty:
                    price_data[stock_id] = df

        st.info(f"共載入 {len(price_data)} 檔股票，開始執行回測。")

        progress = st.progress(0)
        status = st.empty()

        def on_progress(current: int, total: int) -> None:
            progress.progress(current / total)
            status.text(f"回測進度 {current}/{total}")

        bt_result = run_backtest(price_data, config, progress_callback=on_progress)
        progress.empty()
        status.empty()

        st.session_state["bt_result"] = bt_result
        st.session_state["bt_config"] = config

        summary = bt_result.summary()
        if summary:
            st.success(f"回測完成，共產生 {summary['total_trades']} 筆已平倉交易。")
        else:
            st.warning("回測完成，但沒有產生已平倉交易，請調整進場或出場參數。")


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
        exit_mode_label = (
            "RSI 跌破 50" if config.indicator_exit_mode == "rsi_50" else "MACD 死亡交叉"
        )
        st.markdown(
            f"""
| 參數 | 值 |
|------|----|
| 回測期間 | {config.start_date} ~ {config.end_date} |
| 最低強度分數 | {config.min_score} |
| 移動停損/停利 | {'開啟' if config.enable_trailing_exit else '關閉'} |
| 停損比例 | {config.trailing_stop_pct}% |
| 停利啟動門檻 | {config.trailing_tp_activation_pct}% |
| 停利回撤比例 | {config.trailing_tp_pct}% |
| 跌破 MA20 出場 | {'開啟' if config.enable_ma20_exit else '關閉'} |
| 最大持倉天數出場 | {'開啟' if config.enable_max_hold_exit else '關閉'} |
| 最大持倉天數 | {config.max_hold_days} 天 |
| 技術指標停利法 | {'開啟' if config.enable_indicator_exit else '關閉'} |
| 技術停利訊號 | {exit_mode_label} |
"""
        )

    st.markdown("### 核心績效")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("總交易數", summary["total_trades"])
    c2.metric("勝率", f"{summary['win_rate']}%", delta=f"{summary['win_trades']} 勝 / {summary['loss_trades']} 負")
    c3.metric("獲利因子", summary["profit_factor"])
    c4.metric("總報酬", f"{summary['total_return_pct']:+.2f}%")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("平均獲利", f"{summary['avg_win_pct']:+.2f}%")
    c6.metric("平均虧損", f"{summary['avg_loss_pct']:+.2f}%")
    c7.metric("最大回撤", f"{summary['max_drawdown_pct']:.2f}%")
    c8.metric("平均持倉天數", f"{summary['avg_hold_days']} 天")

    st.markdown("---")
    st.markdown("### 資金曲線")
    if bt_result.equity_curve:
        n_days = len(bt_result.equity_curve)
        dates = pd.date_range(start=config.start_date, periods=n_days, freq="B")
        benchmark_curve = [(1 + 0.08) ** (i / 252) for i in range(n_days)]

        fig_eq = go.Figure()
        fig_eq.add_trace(
            go.Scatter(
                x=dates,
                y=[v * 100 for v in bt_result.equity_curve],
                name="策略",
                line=dict(color="#3498db", width=2),
                fill="tozeroy",
                fillcolor="rgba(52,152,219,0.08)",
            )
        )
        fig_eq.add_trace(
            go.Scatter(
                x=dates,
                y=[v * 100 for v in benchmark_curve],
                name="年化 8% 基準",
                line=dict(color="#95a5a6", width=1.5, dash="dot"),
            )
        )
        fig_eq.update_layout(height=380, margin=dict(t=30, b=10))
        st.plotly_chart(fig_eq, use_container_width=True)

    col_exit, col_stock = st.columns(2)

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

    with col_stock:
        st.markdown("### 個股累計報酬")
        closed_trades = [t for t in bt_result.trades if t.sell_date is not None]
        if closed_trades:
            stock_pnl = {}
            for trade in closed_trades:
                stock_pnl[trade.stock_id] = stock_pnl.get(trade.stock_id, 0) + trade.pnl_pct
            sp_df = pd.DataFrame(
                sorted(stock_pnl.items(), key=lambda x: x[1]),
                columns=["stock_id", "total_pnl_pct"],
            )
            colors = ["#27ae60" if v >= 0 else "#e74c3c" for v in sp_df["total_pnl_pct"]]
            fig_sp = go.Figure(
                go.Bar(
                    x=sp_df["total_pnl_pct"],
                    y=sp_df["stock_id"],
                    orientation="h",
                    marker_color=colors,
                )
            )
            fig_sp.update_layout(height=320, margin=dict(t=10, b=10), xaxis_title="累計報酬 (%)")
            st.plotly_chart(fig_sp, use_container_width=True)

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
                    "賣出日": t.sell_date,
                    "賣出價": t.sell_price,
                    "持倉天數": t.hold_days,
                    "報酬(%)": t.pnl_pct,
                    "出場原因": t.exit_reason,
                }
                for t in sorted(closed_trades, key=lambda x: x.buy_date)
            ]
        )
        st.dataframe(trades_df, use_container_width=True, hide_index=True, height=400)
