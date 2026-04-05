"""
回測模組頁面
下載歷史資料 → 設定參數 → 執行回測 → 績效報告
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, timedelta
import time

from data.finmind_client import get_stock_list, get_daily_price
from db.price_cache import (
    init_cache_table, save_prices, load_prices,
    get_all_cached_stocks, get_cache_summary,
)
from db.database import init_db
from modules.backtester import BacktestConfig, run_backtest, compare_to_benchmark

st.set_page_config(page_title="回測模組", page_icon="🔬", layout="wide")
init_db()
init_cache_table()

st.title("🔬 回測模組")
st.markdown("驗證選股策略在歷史上的真實績效")
st.markdown("---")

tab_download, tab_backtest, tab_result = st.tabs(
    ["📥 歷史資料下載", "⚙️ 執行回測", "📊 績效報告"]
)


# ══ Tab 1：歷史資料下載 ══════════════════════════════════════
with tab_download:
    st.subheader("步驟一：下載股票歷史資料到本機")
    st.info(
        "回測使用本機快取資料，不需要每次重新呼叫 API。\n"
        "建議一次性下載後，之後只需定期更新。"
    )

    # 顯示目前快取狀況
    summary = get_cache_summary()
    if not summary.empty:
        col_s1, col_s2, col_s3 = st.columns(3)
        col_s1.metric("已快取股票數", f"{len(summary)} 檔")
        col_s2.metric("資料最早", str(summary["earliest"].min()))
        col_s3.metric("資料最新", str(summary["latest"].max()))
        with st.expander("查看快取清單"):
            st.dataframe(summary, use_container_width=True, hide_index=True)
    else:
        st.warning("本機尚無快取資料，請先下載。")

    st.markdown("---")

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        download_count = st.number_input(
            "下載幾檔（依市值排序前N名）",
            min_value=20, max_value=600, value=100, step=20,
        )
        start_year = st.selectbox(
            "資料起始年份",
            options=[2019, 2020, 2021, 2022, 2023],
            index=0,
        )
    with col_d2:
        st.markdown("**API 請求預估**")
        st.markdown(f"- 需要 **{download_count}** 次請求")
        st.markdown(f"- 免費帳號每日上限：600 次")
        remaining = 600 - download_count
        st.markdown(f"- 下載後剩餘配額：約 {remaining} 次")
        if download_count > 500:
            st.warning("超過 500 檔可能耗盡當日配額，建議分兩天執行")

    if st.button("🚀 開始下載歷史資料", type="primary", use_container_width=True):
        with st.spinner("取得股票清單..."):
            stock_list = get_stock_list()
            if stock_list.empty:
                st.error("無法取得股票清單，請確認 API Token")
                st.stop()

        target_ids = stock_list["stock_id"].head(download_count).tolist()
        start_date_str = f"{start_year}-01-01"

        prog = st.progress(0)
        status = st.empty()
        success, skip, fail = 0, 0, 0

        for i, sid in enumerate(target_ids):
            prog.progress((i + 1) / len(target_ids))
            status.text(f"下載 {sid}（{i+1}/{len(target_ids)}）")
            try:
                # 檢查是否已有快取
                earliest, latest = None, None
                cached_summary = get_cache_summary()
                if not cached_summary.empty and sid in cached_summary["stock_id"].values:
                    row = cached_summary[cached_summary["stock_id"] == sid].iloc[0]
                    earliest = str(row["earliest"])
                    latest = str(row["latest"])

                # 已有完整資料則跳過
                if earliest and earliest <= start_date_str:
                    skip += 1
                    continue

                import requests, os
                from dotenv import load_dotenv
                load_dotenv()
                resp = requests.get(
                    "https://api.finmindtrade.com/api/v4/data",
                    params={
                        "dataset": "TaiwanStockPrice",
                        "data_id": sid,
                        "start_date": start_date_str,
                        "token": os.getenv("FINMIND_TOKEN", ""),
                    },
                    timeout=30,
                )
                data = resp.json()
                if data.get("status") == 200 and data.get("data"):
                    import pandas as _pd
                    df = _pd.DataFrame(data["data"])
                    df["date"] = _pd.to_datetime(df["date"])
                    for col in ["open", "max", "min", "close", "Trading_Volume"]:
                        if col in df.columns:
                            df[col] = _pd.to_numeric(df[col], errors="coerce")
                    saved = save_prices(sid, df)
                    success += 1
                else:
                    fail += 1

                time.sleep(0.1)
            except Exception:
                fail += 1

        prog.empty()
        status.empty()
        st.success(f"下載完成：成功 {success} 檔，已有快取跳過 {skip} 檔，失敗 {fail} 檔")
        st.rerun()


# ══ Tab 2：執行回測 ══════════════════════════════════════════
with tab_backtest:
    st.subheader("步驟二：設定回測參數並執行")

    cached_stocks = get_all_cached_stocks()
    if not cached_stocks:
        st.warning("請先到「歷史資料下載」頁籤下載資料")
        st.stop()

    st.info(f"本機快取共 **{len(cached_stocks)}** 檔股票可供回測使用")

    col_p1, col_p2, col_p3 = st.columns(3)

    with col_p1:
        st.markdown("#### 時間範圍")
        bt_start = st.date_input(
            "回測開始日",
            value=date.today() - timedelta(days=365),
            min_value=date(2019, 1, 1),
        )
        bt_end = st.date_input(
            "回測結束日",
            value=date.today() - timedelta(days=1),
        )
        stock_count = st.number_input(
            "使用幾檔股票（取快取前N檔）",
            min_value=10, max_value=len(cached_stocks),
            value=min(100, len(cached_stocks)), step=10,
        )

    with col_p2:
        st.markdown("#### 出場規則（移動式）")
        trailing_stop = st.slider(
            "移動停損 %（從最高價回落幾%）", 3.0, 20.0, 8.0, 0.5,
            help="持倉後最高價一旦形成，從該高點回落此幅度即出場。比固定停損更能讓獲利奔跑。",
        )
        trailing_tp_act = st.slider(
            "移動停利啟動門檻 %（獲利達此% 後緊縮）", 5.0, 50.0, 15.0, 1.0,
            help="獲利達此水位後，追蹤幅度自動收緊，保護已累積的利潤。",
        )
        trailing_tp = st.slider(
            "啟動後緊縮追蹤 %", 1.0, 15.0, 5.0, 0.5,
            help="啟動移動停利後，從最高價只允許回落此幅度。設得越小，鎖利越積極。",
        )
        max_hold = st.slider("最大持有天數（保底出場）", 5, 60, 20, 5)
        use_ma20_exit = st.checkbox("跌破 MA20 強制出場", value=True)

    with col_p3:
        st.markdown("#### 選股條件")
        min_score = st.slider("最低強度分數門檻", 50.0, 90.0, 65.0, 5.0)
        st.markdown("---")
        st.markdown("**參數預覽**")
        rr = trailing_tp_act / trailing_stop
        st.markdown(f"啟動前 RR = {rr:.1f} : 1　{'✅ 理想' if rr >= 2 else '⚠️ 偏低' if rr >= 1.5 else '❌ 不建議'}")
        st.caption(
            f"啟動前：最高價回落 **{trailing_stop}%** 停損  \n"
            f"獲利 **{trailing_tp_act}%** 後：改追蹤回落 **{trailing_tp}%** 鎖利"
        )

    st.markdown("---")

    if st.button("▶️ 執行回測", type="primary", use_container_width=True):
        config = BacktestConfig(
            start_date=str(bt_start),
            end_date=str(bt_end),
            trailing_stop_pct=trailing_stop,
            trailing_tp_activation_pct=trailing_tp_act,
            trailing_tp_pct=trailing_tp,
            max_hold_days=max_hold,
            use_ma20_exit=use_ma20_exit,
            min_score=min_score,
        )

        target_ids = cached_stocks[:stock_count]

        with st.spinner("載入本機快取資料..."):
            price_data = {}
            for sid in target_ids:
                df = load_prices(sid, start_date="2019-01-01")
                if not df.empty:
                    price_data[sid] = df

        st.info(f"共載入 {len(price_data)} 檔股票資料，開始回測...")

        prog_bt = st.progress(0)
        status_bt = st.empty()

        def on_progress(cur, total):
            prog_bt.progress(cur / total)
            status_bt.text(f"回測進度：{cur}/{total} 個交易日")

        bt_result = run_backtest(price_data, config, progress_callback=on_progress)

        prog_bt.empty()
        status_bt.empty()

        st.session_state["bt_result"] = bt_result
        st.session_state["bt_config"] = config

        summary = bt_result.summary()
        if summary:
            st.success(f"回測完成！共產生 {summary['total_trades']} 筆交易記錄")
        else:
            st.warning("回測完成，但沒有產生任何交易，可嘗試降低最低分數門檻或擴大時間範圍")


# ══ Tab 3：績效報告 ══════════════════════════════════════════
with tab_result:
    st.subheader("回測績效報告")

    if "bt_result" not in st.session_state:
        st.info("請先到「執行回測」頁籤執行回測")
        st.stop()

    bt_result = st.session_state["bt_result"]
    config = st.session_state["bt_config"]
    summary = bt_result.summary()

    if not summary:
        st.warning("沒有足夠的交易記錄可以分析")
        st.stop()

    # ── 參數摘要 ───────────────────────────────────────────
    with st.expander("回測參數設定"):
        st.markdown(f"""
        | 參數 | 值 |
        |------|-----|
        | 回測期間 | {config.start_date} ～ {config.end_date} |
        | 移動停損 | {config.trailing_stop_pct}%（從最高價回落） |
        | 移動停利啟動 | 獲利達 {config.trailing_tp_activation_pct}% 後緊縮 |
        | 啟動後追蹤幅度 | {config.trailing_tp_pct}%（從最高價回落） |
        | 最大持有天數 | {config.max_hold_days} 天 |
        | MA20 出場 | {'是' if config.use_ma20_exit else '否'} |
        | 最低分數門檻 | {config.min_score} 分 |
        """)

    # ── 核心指標 ───────────────────────────────────────────
    st.markdown("### 核心績效指標")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("總交易次數", summary["total_trades"])
    c2.metric(
        "勝率",
        f"{summary['win_rate']}%",
        delta=f"{summary['win_trades']}勝 / {summary['loss_trades']}敗",
    )
    c3.metric(
        "盈虧比",
        summary["profit_factor"],
        delta="良好" if summary["profit_factor"] >= 1.5 else "需改善",
        delta_color="normal" if summary["profit_factor"] >= 1.5 else "inverse",
    )
    c4.metric(
        "策略總報酬",
        f"{summary['total_return_pct']:+.2f}%",
        delta_color="normal",
    )

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("平均獲利", f"{summary['avg_win_pct']:+.2f}%")
    c6.metric("平均虧損", f"{summary['avg_loss_pct']:+.2f}%")
    c7.metric("最大回撤", f"{summary['max_drawdown_pct']:.2f}%",
              delta_color="inverse")
    c8.metric("平均持有天數", f"{summary['avg_hold_days']} 天")

    # ── 策略 vs 大盤 ───────────────────────────────────────
    st.markdown("---")
    st.markdown("### 策略 vs 買進持有大盤")

    if bt_result.equity_curve:
        # 產生日期軸（使用回測天數）
        n_days = len(bt_result.equity_curve)
        dates = pd.date_range(start=config.start_date, periods=n_days, freq="B")

        # 簡單的大盤模擬（用固定年化 8% 作為基準，因 API 限制）
        bm_curve = [(1 + 0.08) ** (i / 252) for i in range(n_days)]

        fig_eq = go.Figure()
        fig_eq.add_trace(go.Scatter(
            x=dates, y=[v * 100 for v in bt_result.equity_curve],
            name="選股策略", line=dict(color="#3498db", width=2),
            fill="tozeroy", fillcolor="rgba(52,152,219,0.08)",
        ))
        fig_eq.add_trace(go.Scatter(
            x=dates, y=[v * 100 for v in bm_curve],
            name="大盤基準（年化8%）",
            line=dict(color="#95a5a6", width=1.5, dash="dot"),
        ))
        fig_eq.add_hline(y=100, line_dash="dash", line_color="gray",
                         annotation_text="起始資金")
        fig_eq.update_layout(
            height=380, template="plotly_dark",
            title="累積報酬曲線（起始值 = 100）",
            yaxis_title="報酬指數",
            margin=dict(t=40, b=10),
        )
        st.plotly_chart(fig_eq, use_container_width=True)

    # ── 出場原因分析 ───────────────────────────────────────
    col_exit, col_stock = st.columns(2)

    with col_exit:
        st.markdown("### 出場原因分布")
        exit_data = summary.get("exit_reasons", {})
        if exit_data:
            fig_exit = go.Figure(go.Pie(
                labels=list(exit_data.keys()),
                values=list(exit_data.values()),
                hole=0.4,
                textinfo="label+percent+value",
            ))
            fig_exit.update_layout(height=320, template="plotly_dark",
                                   margin=dict(t=20, b=10))
            st.plotly_chart(fig_exit, use_container_width=True)

    with col_stock:
        st.markdown("### 各股票損益分布")
        closed_trades = [t for t in bt_result.trades if t.sell_date is not None]
        if closed_trades:
            stock_pnl = {}
            for t in closed_trades:
                stock_pnl[t.stock_id] = stock_pnl.get(t.stock_id, 0) + t.pnl_pct
            sp_df = pd.DataFrame(
                sorted(stock_pnl.items(), key=lambda x: x[1]),
                columns=["stock_id", "total_pnl_pct"]
            )
            colors = ["#27ae60" if v >= 0 else "#e74c3c" for v in sp_df["total_pnl_pct"]]
            fig_sp = go.Figure(go.Bar(
                x=sp_df["total_pnl_pct"], y=sp_df["stock_id"],
                orientation="h", marker_color=colors,
            ))
            fig_sp.update_layout(height=320, template="plotly_dark",
                                 margin=dict(t=10, b=10),
                                 xaxis_title="累積損益%")
            st.plotly_chart(fig_sp, use_container_width=True)

    # ── 每筆交易明細 ───────────────────────────────────────
    st.markdown("---")
    st.markdown("### 每筆交易明細")
    closed_trades = [t for t in bt_result.trades if t.sell_date is not None]
    if closed_trades:
        trades_df = pd.DataFrame([{
            "代碼": t.stock_id,
            "買入日": t.buy_date,
            "買入價": t.buy_price,
            "賣出日": t.sell_date,
            "賣出價": t.sell_price,
            "持有天數": t.hold_days,
            "損益%": t.pnl_pct,
            "出場原因": t.exit_reason,
        } for t in sorted(closed_trades, key=lambda x: x.buy_date)])

        trades_df.index = range(1, len(trades_df) + 1)

        def color_pnl(val):
            if isinstance(val, float):
                return "color:#e74c3c" if val > 0 else "color:#27ae60" if val < 0 else ""
            return ""

        st.dataframe(
            trades_df.style.applymap(color_pnl, subset=["損益%"]),
            use_container_width=True,
            height=400,
        )

    # ── 策略診斷建議 ───────────────────────────────────────
    st.markdown("---")
    st.markdown("### 策略診斷")

    diagnoses = []
    if summary["win_rate"] < 40:
        diagnoses.append("⚠️ **勝率偏低（< 40%）**：考慮提高最低分數門檻，或加入法人買超作為必要條件")
    if summary["profit_factor"] < 1.2:
        diagnoses.append("⚠️ **盈虧比偏低（< 1.2）**：考慮縮短停損距離或放寬停利目標")
    if summary["max_drawdown_pct"] < -25:
        diagnoses.append("⚠️ **最大回撤過大（> 25%）**：考慮縮短最大持有天數或收緊停損")
    if summary["avg_hold_days"] < 3:
        diagnoses.append("ℹ️ **平均持有天數極短**：策略頻繁出場，交易成本影響較大")

    exit_reasons = summary.get("exit_reasons", {})
    trailing_stop_count = exit_reasons.get("移動停損", 0)
    total_closed = summary["total_trades"]
    if total_closed > 0 and trailing_stop_count / total_closed > 0.5:
        diagnoses.append("⚠️ **超過 50% 交易以移動停損出場**：訊號品質可能偏低，考慮提高門檻分數或放寬移動停損幅度")

    trailing_tp_count = exit_reasons.get("移動停利", 0)
    if total_closed > 0 and trailing_tp_count / total_closed > 0.4:
        diagnoses.append("✅ **移動停利比例高（> 40%）**：股票達到目標後緊縮鎖利的能力良好")

    if summary["profit_factor"] >= 1.5 and summary["win_rate"] >= 45:
        diagnoses.append("✅ **策略整體表現良好**：盈虧比與勝率都在合理範圍")

    if not diagnoses:
        diagnoses.append("✅ 策略各項指標在正常範圍內")

    for d in diagnoses:
        st.markdown(d)
