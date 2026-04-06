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
from modules.backtester import BacktestConfig, generate_text_report, run_backtest

st.set_page_config(page_title="回測模組", page_icon="📉", layout="wide")
init_db()
init_cache_table()

st.title("📉 回測模組")
st.markdown("使用本機快取資料驗證策略，並加入大盤濾網與動態停損。")
st.markdown("---")

tab_download, tab_backtest, tab_result = st.tabs(
    ["下載回測資料", "設定回測參數", "查看回測結果"]
)


def _load_market_index(start_date: str = "2019-01-01") -> pd.DataFrame:
    """讀取加權指數歷史資料，優先使用本機快取，缺少時再補抓。"""
    market_df = load_prices("TAIEX", start_date=start_date)
    if not market_df.empty:
        return market_df

    market_df = get_daily_price("TAIEX", start_date=start_date)
    if not market_df.empty:
        save_prices("TAIEX", market_df)
    return market_df


with tab_download:
    st.subheader("下載與檢查歷史資料")
    st.info("回測前可先檢查本機快取是否足夠，必要時再補抓。")

    # ── 加權指數狀態 ──────────────────────────────────────────────
    st.markdown("#### 加權指數（大盤濾網必要條件）")
    from db.price_cache import get_cached_dates
    market_earliest, market_latest = get_cached_dates("TAIEX")
    if market_earliest:
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("加權指數快取", "已下載")
        mc2.metric("最早日期", str(market_earliest))
        mc3.metric("最新日期", str(market_latest))
        from datetime import date
        stale = (date.today() - date.fromisoformat(str(market_latest))).days > 7
        mc4.metric("狀態", "需要更新" if stale else "正常", delta_color="inverse" if stale else "off")
        if stale:
            st.warning("加權指數資料超過 7 天未更新，建議重新下載。")
    else:
        st.error("⚠️ 加權指數尚未下載，大盤濾網功能無法使用！請點擊下方按鈕下載。")

    if st.button("下載加權指數歷史資料", use_container_width=True):
        with st.spinner("正在下載加權指數..."):
            mdf = get_daily_price("TAIEX", start_date="2019-01-01")
        if mdf.empty:
            st.error("下載失敗，請確認 API Token 是否有效。")
        else:
            save_prices("TAIEX", mdf)
            st.success(f"加權指數下載完成，共 {len(mdf)} 筆資料。")
            st.rerun()

    st.markdown("---")

    summary = get_cache_summary()
    if not summary.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("已快取股票數", f"{len(summary)} 檔")
        col2.metric("最早資料日", str(summary["earliest"].min()))
        col3.metric("最新資料日", str(summary["latest"].max()))
        with st.expander("檢視快取明細"):
            st.dataframe(summary, use_container_width=True, hide_index=True)
    else:
        st.warning("目前沒有任何回測快取資料。")

    st.markdown("---")

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        download_count = st.number_input("下載股票檔數", min_value=20, max_value=600, value=100, step=20)
        start_year = st.selectbox("起始年度", options=[2019, 2020, 2021, 2022, 2023], index=0)
    with col_d2:
        st.markdown("**API 使用估算**")
        st.markdown(f"- 本次預計抓取 **{download_count}** 檔")
        st.markdown("- 免費額度約每小時 600 次")
        st.markdown(f"- 執行後剩餘預估額度 **{600 - download_count}** 次")

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
        st.warning("目前沒有可用快取資料，請先到上一頁籤下載。")
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

    with col_p2:
        st.markdown("#### 出場策略設定")
        enable_trailing_exit = st.checkbox("啟用 ATR 動態移動停損", value=True)
        atr_multiplier = st.slider("ATR 停損倍數", 1.0, 5.0, 2.5, 0.1) if enable_trailing_exit else 2.5
        enable_ma20_exit = st.checkbox("啟用跌破月線 (MA20) 出場", value=True)
        enable_max_hold_exit = st.checkbox("啟用最大持倉天數出場", value=True)
        max_hold = st.slider("最大持倉天數", 5, 120, 20, 5) if enable_max_hold_exit else 20
        enable_indicator_exit = st.checkbox("啟用技術指標停利法", value=False)
        indicator_exit_mode = "rsi_50"
        if enable_indicator_exit:
            indicator_label = st.selectbox("技術反轉訊號", ["RSI 跌破 50", "MACD 死亡交叉"])
            indicator_exit_mode = "rsi_50" if indicator_label == "RSI 跌破 50" else "macd_dead_cross"

    with col_p3:
        st.markdown("#### 進場過濾器")
        min_score = st.slider("最低強度分數", 50.0, 100.0, 65.0, 1.0)
        enable_market_filter = st.checkbox("啟用大盤 MA20 濾網", value=True)
        max_bias_ratio = st.slider("個股最大容許 BIAS (%)", 0.0, 20.0, 10.0, 0.5)
        exclude_leveraged_etf = st.checkbox(
            "排除槓桿/反向/期貨型 ETF",
            value=True,
            help="排除代碼末位為 L（槓桿）、R（反向）、U（期貨）的 ETF，如 00631L、00632R、00635U。"
                 "這類商品有波動耗損與轉倉成本，不適合用趨勢策略回測。",
        )
        st.markdown("---")
        st.markdown("**過濾條件摘要**")
        st.caption("進場需同時滿足：大盤站上 MA20 且 MA20 向上、個股 BIAS 不超過上限。")
        st.caption(f"目前設定：最低分數 {min_score:.0f}，BIAS ≤ {max_bias_ratio:.1f}%")

    st.markdown("---")

    if st.button("開始回測", type="primary", use_container_width=True):
        config = BacktestConfig(
            start_date=str(bt_start),
            end_date=str(bt_end),
            enable_trailing_exit=enable_trailing_exit,
            atr_multiplier=atr_multiplier,
            enable_ma20_exit=enable_ma20_exit,
            enable_max_hold_exit=enable_max_hold_exit,
            max_hold_days=max_hold,
            enable_indicator_exit=enable_indicator_exit,
            indicator_exit_mode=indicator_exit_mode,
            enable_market_filter=enable_market_filter,
            max_bias_ratio=max_bias_ratio,
            min_score=min_score,
            exclude_leveraged_etf=exclude_leveraged_etf,
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
                f"回測完成，共產生 {summary['total_trades']} 筆已平倉交易，"
                f"略過 {summary.get('skip_count', 0)} 筆不符合進場過濾器的候選。"
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
        st.markdown(
            f"""
| 參數 | 值 |
|------|----|
| 回測期間 | {config.start_date} ~ {config.end_date} |
| 最低強度分數 | {config.min_score} |
| ATR 動態停損 | {'開啟' if config.enable_trailing_exit else '關閉'} |
| ATR 期間 | {config.atr_period} |
| ATR 停損倍數 | {config.atr_multiplier} |
| 跌破 MA20 出場 | {'開啟' if config.enable_ma20_exit else '關閉'} |
| 最大持倉天數出場 | {'開啟' if config.enable_max_hold_exit else '關閉'} |
| 最大持倉天數 | {config.max_hold_days} 天 |
| 技術指標停利法 | {'開啟' if config.enable_indicator_exit else '關閉'} |
| 技術停利訊號 | {indicator_label} |
| 大盤濾網 | {'開啟' if config.enable_market_filter else '關閉'} |
| 個股最大 BIAS | {config.max_bias_ratio}% |
| 排除槓桿/期貨 ETF | {'是' if getattr(config, 'exclude_leveraged_etf', False) else '否'} |
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
        fig_eq.update_layout(height=360, margin=dict(t=30, b=10))
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
        st.dataframe(trades_df, use_container_width=True, hide_index=True, height=320)

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
    st.markdown("### 匯出 Gemini 分析報告")
    st.caption("產生包含策略配置、績效統計、月度分析、完整交易明細的 Markdown 報告，可直接貼給 Gemini 分析。")
    report_text = generate_text_report(bt_result, config)
    report_filename = f"backtest_report_{config.start_date}_{config.end_date}.md"
    st.download_button(
        label="下載分析報告 (.md)",
        data=report_text.encode("utf-8"),
        file_name=report_filename,
        mime="text/markdown",
        use_container_width=True,
        type="primary",
    )
    with st.expander("預覽報告內容"):
        st.text(report_text[:3000] + ("\n...(截斷預覽，完整內容請下載)" if len(report_text) > 3000 else ""))
