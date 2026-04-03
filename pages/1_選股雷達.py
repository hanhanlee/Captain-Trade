"""
選股雷達頁面 v2
掃描全市場，找出技術面強勢股票
新增：週線多頭共振、相對強度(RS)、產業族群分析
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import time

from data.finmind_client import get_stock_list, get_daily_price, get_institutional_investors, check_all_three_buying
from modules.scanner import run_scan, compute_indicators, sector_analysis
from modules.indicators import weekly_ma_trend

st.set_page_config(page_title="選股雷達", page_icon="🔍", layout="wide")


def render_chart(stock_id: str, df: pd.DataFrame):
    """繪製日K線圖 + 指標"""
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        vertical_spacing=0.05, row_heights=[0.6, 0.2, 0.2],
        subplot_titles=[f"{stock_id} 日K線圖", "成交量", "MACD"],
    )

    fig.add_trace(go.Candlestick(
        x=df["date"], open=df["open"], high=df["max"],
        low=df["min"], close=df["close"], name="K線",
        increasing_line_color="#e74c3c", decreasing_line_color="#27ae60",
    ), row=1, col=1)

    for ma, color in [("ma5", "#f39c12"), ("ma20", "#3498db"), ("ma60", "#9b59b6")]:
        if ma in df.columns:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df[ma], name=ma.upper(),
                line=dict(color=color, width=1.2),
            ), row=1, col=1)

    for bb_col, name in [("bb_upper", "BB上軌"), ("bb_lower", "BB下軌")]:
        if bb_col in df.columns:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df[bb_col], name=name,
                line=dict(color="rgba(52,152,219,0.4)", width=0.8, dash="dot"),
            ), row=1, col=1)

    if "Trading_Volume" in df.columns:
        bar_colors = ["#e74c3c" if c >= o else "#27ae60"
                      for c, o in zip(df["close"], df["open"])]
        fig.add_trace(go.Bar(
            x=df["date"], y=df["Trading_Volume"],
            name="成交量", marker_color=bar_colors, showlegend=False,
        ), row=2, col=1)

    if all(c in df.columns for c in ["macd", "macd_signal", "macd_hist"]):
        fig.add_trace(go.Scatter(x=df["date"], y=df["macd"], name="DIF",
                                  line=dict(color="#e74c3c", width=1)), row=3, col=1)
        fig.add_trace(go.Scatter(x=df["date"], y=df["macd_signal"], name="DEA",
                                  line=dict(color="#3498db", width=1)), row=3, col=1)
        hist_colors = ["#e74c3c" if v >= 0 else "#27ae60"
                       for v in df["macd_hist"].fillna(0)]
        fig.add_trace(go.Bar(x=df["date"], y=df["macd_hist"],
                              marker_color=hist_colors, showlegend=False), row=3, col=1)

    fig.update_layout(height=680, showlegend=True, xaxis_rangeslider_visible=False,
                      template="plotly_dark", margin=dict(t=40, b=10),
                      legend=dict(orientation="h", y=1.02))
    st.plotly_chart(fig, use_container_width=True)

    if "rsi14" in df.columns:
        latest_rsi = df["rsi14"].iloc[-1]
        rsi_color = "#e74c3c" if latest_rsi > 70 else ("#27ae60" if latest_rsi < 30 else "#f39c12")
        st.markdown(f"**RSI(14)：<span style='color:{rsi_color}'>{latest_rsi:.1f}</span>**",
                    unsafe_allow_html=True)
        rfig = go.Figure()
        rfig.add_trace(go.Scatter(x=df["date"], y=df["rsi14"], name="RSI",
                                   line=dict(color="#f39c12"), fill="tozeroy",
                                   fillcolor="rgba(243,156,18,0.1)"))
        rfig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="超買70")
        rfig.add_hline(y=50, line_dash="dot", line_color="gray")
        rfig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="超賣30")
        rfig.update_layout(height=180, template="plotly_dark",
                            margin=dict(t=10, b=10), showlegend=False)
        st.plotly_chart(rfig, use_container_width=True)


def render_weekly_chart(stock_id: str, df: pd.DataFrame):
    """繪製週K線圖 + 週MA10"""
    wt = weekly_ma_trend(df, ma_period=10)
    if not wt or "weekly_df" not in wt:
        st.caption("週線資料不足（需至少 14 週）")
        return

    weekly = wt["weekly_df"]
    high_col = "max" if "max" in weekly.columns else "high"
    low_col = "min" if "min" in weekly.columns else "low"

    from modules.indicators import sma
    weekly["wma10"] = sma(weekly["close"], 10)

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=weekly["week_end"], open=weekly["open"], high=weekly[high_col],
        low=weekly[low_col], close=weekly["close"], name="週K",
        increasing_line_color="#e74c3c", decreasing_line_color="#27ae60",
    ))
    fig.add_trace(go.Scatter(
        x=weekly["week_end"], y=weekly["wma10"], name="週MA10",
        line=dict(color="#3498db", width=2),
    ))

    status = "🟢 週線多頭" if wt["weekly_above_ma"] and wt["weekly_ma_rising"] else "🔴 週線偏弱"
    fig.update_layout(
        height=380, template="plotly_dark",
        title=f"{stock_id} 週K線圖　{status}",
        xaxis_rangeslider_visible=False,
        margin=dict(t=40, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── 側邊欄 ───────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ 掃描設定")
    min_price = st.number_input("最低股價（元）", min_value=1.0, max_value=100.0,
                                value=10.0, step=1.0)
    scan_mode = st.radio("掃描範圍",
                          ["快速測試（20 檔）", "小型掃描（100 檔）", "全市場掃描（需時較長）"],
                          index=0)
    include_institutional = st.checkbox("納入法人買賣超（較慢）", value=False)

    st.markdown("---")
    st.markdown("**流動性前置過濾**")
    vol_filter_mode = st.radio(
        "篩選方式",
        ["不過濾", "前日量前 N 名（推薦）", "日均量 ≥ N 張"],
        index=1,
        help="先過濾低流動性股票，加快掃描速度並聚焦主流股",
    )
    if vol_filter_mode == "前日量前 N 名（推薦）":
        top_volume_n = st.number_input("取前 N 名", min_value=50, max_value=500,
                                        value=100, step=50)
        min_avg_volume = 0
        st.caption("💡 鎖定當天最活躍的股票，動態追蹤市場熱點")
    elif vol_filter_mode == "日均量 ≥ N 張":
        min_avg_volume = st.number_input("最低日均量（張）", min_value=0,
                                          max_value=10000, value=1000, step=100)
        top_volume_n = 0
        st.caption("💡 日均量 > 1000 張通常對應資本額 20 億以上")
    else:
        min_avg_volume, top_volume_n = 0, 0

    st.markdown("---")
    st.markdown("**產業輪動過濾**")
    use_sector_filter = st.checkbox("只掃描近一週漲幅前 N 類股", value=False,
                                     help="自動算出哪些產業最強，只在那幾個產業裡選股")
    top_sector_n = 0
    if use_sector_filter:
        top_sector_n = st.number_input("取前 N 個產業", min_value=1, max_value=10,
                                        value=3, step=1)
        st.caption("💡 資金正在流入的產業，勝率更高")

    st.markdown("---")
    st.markdown("**v2 進階選項**")
    require_weekly = st.checkbox("必須週線多頭（更嚴格，結果更少）", value=False)
    min_rs = st.slider("最低相對強度 RS 分數", 0, 80, 0, 5,
                       help="0 = 不限制；60 以上 = 強勢股")
    st.markdown("---")
    st.caption("全市場掃描約需 30-50 分鐘")


# ── 主頁面 ───────────────────────────────────────────────────
st.title("🔍 選股雷達")
st.markdown("掃描全市場，依技術強度找出值得關注的標的")
st.markdown("---")

tab_scan, tab_sector, tab_chart = st.tabs(["📊 掃描結果", "🏭 產業族群", "📈 個股圖表"])


# ══ Tab：掃描結果 ════════════════════════════════════════════
with tab_scan:
    if st.button("🚀 開始掃描", type="primary", use_container_width=True):
        with st.spinner("載入股票清單..."):
            try:
                stock_list = get_stock_list()
            except Exception as e:
                st.error(f"無法取得股票清單：{e}")
                st.stop()

        if stock_list.empty:
            st.warning("股票清單為空，請確認 API Token")
            st.stop()

        if scan_mode.startswith("快速"):
            sample_ids = stock_list["stock_id"].head(20).tolist()
        elif scan_mode.startswith("小型"):
            sample_ids = stock_list["stock_id"].head(100).tolist()
        else:
            sample_ids = stock_list["stock_id"].tolist()

        total = len(sample_ids)
        st.info(f"準備掃描 **{total}** 檔股票...")

        prog = st.progress(0)
        status_txt = st.empty()
        price_data, inst_data = {}, {}

        for i, sid in enumerate(sample_ids):
            status_txt.text(f"下載：{sid}（{i+1}/{total}）")
            prog.progress((i + 1) / total)
            try:
                df = get_daily_price(sid, days=150)   # 多取一點供週線計算
                if not df.empty:
                    price_data[sid] = df
                if include_institutional:
                    idf = get_institutional_investors(sid, days=10)
                    if not idf.empty:
                        inst_data[sid] = check_all_three_buying(idf, days=2)
                time.sleep(0.05)
            except Exception:
                pass

        prog.empty()
        status_txt.empty()

        with st.spinner("計算指標，篩選中..."):
            result_df, sector_info = run_scan(
                price_data=price_data,
                stock_info=stock_list,
                inst_data=inst_data if include_institutional else {},
                min_price=min_price,
                min_avg_volume=min_avg_volume,
                top_volume_n=top_volume_n,
                top_sector_n=top_sector_n,
            )

        # 顯示產業輪動過濾結果
        if sector_info and top_sector_n > 0:
            top_inds = sorted(sector_info, key=lambda x: sector_info[x]["return_pct"], reverse=True)[:top_sector_n]
            ind_tags = "　".join(
                [f"**{ind}** ({sector_info[ind]['return_pct']:+.1f}%)" for ind in top_inds]
            )
            st.info(f"📊 本次鎖定產業（近 5 日漲幅前 {top_sector_n} 名）：{ind_tags}")

        # 套用 v2 進階過濾
        if not result_df.empty:
            if require_weekly:
                result_df = result_df[result_df["signals"].str.contains("週線多頭", na=False)]
            if min_rs > 0:
                result_df = result_df[result_df["rs_score"] >= min_rs]

        if result_df.empty:
            st.warning("沒有符合條件的股票，可嘗試調整篩選條件")
        else:
            st.success(f"找到 **{len(result_df)}** 檔符合條件的股票")
            st.session_state["scan_results"] = result_df
            st.session_state["price_data"] = price_data

    # ── 顯示結果 ──────────────────────────────────────────────
    if "scan_results" in st.session_state:
        result_df = st.session_state["scan_results"]

        if "scan_results" not in st.session_state or not st.session_state.get("_just_scanned"):
            st.info(f"上次掃描結果（{len(result_df)} 檔）")

        # 分數 > 100 的標記
        has_elite = (result_df["score"] > 100).any()
        if has_elite:
            n_elite = (result_df["score"] > 100).sum()
            st.success(f"⭐ 本次發現 **{n_elite}** 檔「精選強勢股」（分數 > 100，週線 + RS 雙重確認）")

        display_df = result_df.rename(columns={
            "stock_id": "代碼", "stock_name": "名稱", "industry": "產業",
            "close": "收盤", "change_pct": "漲跌%", "volume_ratio": "量比",
            "score": "強度分數", "rs_score": "RS分數", "signals": "觸發條件",
        })
        display_df.index = range(1, len(display_df) + 1)

        st.dataframe(display_df, use_container_width=True, height=500)
    else:
        st.markdown("""
        #### 篩選條件（v3）

        | 條件 | 類型 | 分數 | 說明 |
        |------|------|------|------|
        | 站上 MA20 且均線向上 | 必要 | +35 | 趨勢確認 |
        | 量能 > 均量 1.3 倍 | 必要 | +20 | 有效突破 |
        | MACD 或 RSI 50-70 | 必要 | +15 | 動能確認 |
        | 不低於布林下軌 | 必要 | +10 | 排除弱勢 |
        | 三大法人連續 2 日齊買 | 加分 | +7 | 外資＋投信＋自營商同步買超 |
        | 融資減少 | 加分 | +3 | 籌碼乾淨 |
        | 週線 MA10 多頭 | 加分 | +10 | 多週期共振 |
        | 相對強度 RS ≥ 60 | 加分 | +8 | 抗跌／領漲特性 |
        | MA5 > MA10 > MA20 | 加分 | +5 | 多頭排列 |
        | 近10日量集中在上漲日 | 加分 | +7 | 量能品質優良 |
        | 突破近60日最高收盤 | 加分 | +8 | 突破盤整 |
        """)


# ══ Tab：產業族群 ════════════════════════════════════════════
with tab_sector:
    st.subheader("🏭 產業族群分析")
    st.markdown("哪些產業目前有最多強勢股出現？資金往往集中在特定族群。")

    if "scan_results" not in st.session_state:
        st.info("請先執行掃描")
    else:
        result_df = st.session_state["scan_results"]
        sector_df = sector_analysis(result_df)

        if sector_df.empty:
            st.info("無足夠的產業資料")
        else:
            col_bar, col_table = st.columns([3, 2])

            with col_bar:
                top_sectors = sector_df.head(10)
                colors = [
                    "#e74c3c" if c >= 3 else "#f39c12" if c == 2 else "#3498db"
                    for c in top_sectors["count"]
                ]
                fig_s = go.Figure(go.Bar(
                    x=top_sectors["count"],
                    y=top_sectors["industry"],
                    orientation="h",
                    marker_color=colors,
                    text=[f"{c} 檔  均分{s}" for c, s in
                          zip(top_sectors["count"], top_sectors["avg_score"])],
                    textposition="outside",
                ))
                fig_s.update_layout(
                    height=380, template="plotly_dark",
                    title="入選強勢股數量（依產業）",
                    xaxis_title="入選檔數",
                    margin=dict(t=40, b=10, l=120),
                )
                st.plotly_chart(fig_s, use_container_width=True)

            with col_table:
                st.markdown("#### 各族群代表股")
                display_sector = sector_df.rename(columns={
                    "industry": "產業", "count": "入選檔數",
                    "avg_score": "平均分數", "top_stock": "最強個股",
                })
                st.dataframe(display_sector[["產業", "入選檔數", "平均分數", "最強個股"]],
                             use_container_width=True, hide_index=True)

            # 解讀提示
            if not sector_df.empty:
                top1 = sector_df.iloc[0]
                st.markdown("---")
                st.info(
                    f"**資金集中分析：**「{top1['industry']}」目前入選 {top1['count']} 檔強勢股，"
                    f"平均強度 {top1['avg_score']} 分，是本次掃描中資金最活躍的族群。\n\n"
                    f"操作建議：考慮從最強族群中挑選分數最高的個股優先研究。"
                )


# ══ Tab：個股圖表 ════════════════════════════════════════════
with tab_chart:
    st.subheader("個股圖表（日K + 週K）")

    col_sel, col_btn = st.columns([3, 1])
    with col_sel:
        if "scan_results" in st.session_state and "price_data" in st.session_state:
            result_df = st.session_state["scan_results"]
            opts = ["（手動輸入）"] + [
                f"{r['stock_id']} {r['stock_name']}（{r['score']}分）"
                for _, r in result_df.iterrows()
            ]
            sel = st.selectbox("從掃描結果選擇", opts)
            selected_id = sel.split(" ")[0] if sel != "（手動輸入）" else None
        else:
            selected_id = None

        manual_id = st.text_input("或直接輸入股票代碼", placeholder="2330")

    with col_btn:
        st.markdown("<br><br>", unsafe_allow_html=True)
        fetch = st.button("查詢圖表", use_container_width=True)

    target = manual_id.strip() if manual_id.strip() else (selected_id or None)

    if fetch and target:
        price_cache = st.session_state.get("price_data", {})
        if target in price_cache:
            df = compute_indicators(price_cache[target])
        else:
            with st.spinner(f"下載 {target}..."):
                try:
                    df = get_daily_price(target, days=150)
                    if df.empty:
                        st.warning("找不到此股票")
                        df = None
                    else:
                        df = compute_indicators(df)
                except Exception as e:
                    st.error(str(e))
                    df = None
        if df is not None:
            st.session_state["chart_df"] = df
            st.session_state["chart_id"] = target

    if "chart_df" in st.session_state:
        chart_id = st.session_state["chart_id"]
        chart_df = st.session_state["chart_df"]

        day_tab, week_tab = st.tabs(["日K線", "週K線"])
        with day_tab:
            render_chart(chart_id, chart_df)
        with week_tab:
            render_weekly_chart(chart_id, chart_df)
    else:
        st.info("請選擇股票或輸入代碼後點擊「查詢圖表」")
