"""
市場環境 + 排程控制頁面
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import time

from data.finmind_client import get_daily_price, get_institutional_investors
from modules.indicators import sma, rsi, macd
from notifications.line_notify import send_multicast
from notifications.telegram_notify import send_stock_alert as tg_alert

st.set_page_config(page_title="市場環境", page_icon="🌐", layout="wide")
st.title("🌐 市場環境 & 排程控制")

tab_market, tab_scheduler = st.tabs(["📈 大盤環境", "⏰ 排程推播"])


# ══ Tab：大盤環境 ═══════════════════════════════════════════
with tab_market:
    st.subheader("加權指數技術面")

    if st.button("🔄 載入大盤資料", type="primary"):
        with st.spinner("下載加權指數資料..."):
            try:
                df_taiex = get_daily_price("TAIEX", days=200)
                if df_taiex.empty:
                    st.warning("無法取得加權指數資料，FinMind 可能需要付費方案")
                else:
                    st.session_state["taiex_df"] = df_taiex
            except Exception as e:
                st.error(f"載入失敗：{e}")

    if "taiex_df" in st.session_state:
        df = st.session_state["taiex_df"].copy()
        close = df["close"]

        df["ma20"] = sma(close, 20)
        df["ma60"] = sma(close, 60)
        df["ma120"] = sma(close, 120)
        df["rsi14"] = rsi(close, 14)
        dif, dea, hist = macd(close)
        df["macd"], df["macd_signal"], df["macd_hist"] = dif, dea, hist

        latest = df.iloc[-1]

        # 多空判斷
        above_ma20 = latest["close"] > latest["ma20"] if pd.notna(latest.get("ma20")) else None
        above_ma60 = latest["close"] > latest["ma60"] if pd.notna(latest.get("ma60")) else None
        macd_bull = latest["macd"] > latest["macd_signal"] if pd.notna(latest.get("macd")) else None
        rsi_val = latest.get("rsi14")

        bull_signals = sum([bool(above_ma20), bool(above_ma60), bool(macd_bull)])
        if bull_signals >= 3:
            market_status = "🟢 多頭"
            status_color = "#27ae60"
        elif bull_signals == 2:
            market_status = "🟡 偏多震盪"
            status_color = "#f39c12"
        elif bull_signals == 1:
            market_status = "🟠 偏空震盪"
            status_color = "#e67e22"
        else:
            market_status = "🔴 空頭"
            status_color = "#e74c3c"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("市場環境", market_status)
        c2.metric("加權指數", f"{latest['close']:,.0f}", delta=None)
        c3.metric("RSI(14)", f"{rsi_val:.1f}" if pd.notna(rsi_val) else "—")
        c4.metric("站上 MA20", "是" if above_ma20 else "否",
                  delta_color="normal" if above_ma20 else "inverse")

        # 加權指數圖
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["date"], y=df["close"], name="加權指數",
                                  line=dict(color="#3498db", width=2)))
        for col, color, name in [("ma20", "#f39c12", "MA20"),
                                   ("ma60", "#e74c3c", "MA60"),
                                   ("ma120", "#9b59b6", "MA120")]:
            if col in df.columns:
                fig.add_trace(go.Scatter(x=df["date"], y=df[col], name=name,
                                          line=dict(color=color, width=1.2, dash="dot")))
        fig.update_layout(height=400, template="plotly_dark",
                          margin=dict(t=20, b=10), title="加權指數走勢")
        st.plotly_chart(fig, use_container_width=True)

        # 法人買賣超
        st.subheader("三大法人動向（台積電 2330 為例）")
        if st.button("載入法人資料"):
            with st.spinner("下載法人資料..."):
                try:
                    inst_df = get_institutional_investors("2330", days=30)
                    if not inst_df.empty:
                        st.session_state["inst_df"] = inst_df
                except Exception as e:
                    st.error(str(e))

        if "inst_df" in st.session_state:
            inst = st.session_state["inst_df"]
            fig_inst = go.Figure()
            for name_type in inst["name"].unique() if "name" in inst.columns else []:
                sub = inst[inst["name"] == name_type]
                fig_inst.add_trace(go.Bar(x=sub["date"], y=sub["net"],
                                           name=name_type))
            fig_inst.update_layout(height=300, template="plotly_dark",
                                    barmode="group", margin=dict(t=10, b=10))
            st.plotly_chart(fig_inst, use_container_width=True)
    else:
        st.info("點擊「載入大盤資料」查看市場環境判讀。")
        st.markdown("""
        #### 多空判斷邏輯

        | 訊號 | 說明 |
        |------|------|
        | 站上 MA20 | 短期趨勢偏多 |
        | 站上 MA60 | 中期趨勢偏多 |
        | MACD > Signal | 動能偏多 |

        - 3 個訊號全滿 → **多頭，可積極佈局**
        - 2 個訊號 → **偏多震盪，選擇性操作**
        - 1 個訊號 → **偏空，縮手觀望**
        - 0 個訊號 → **空頭，嚴格控制部位**
        """)


# ══ Tab：排程控制 ════════════════════════════════════════════
with tab_scheduler:
    st.subheader("手動觸發排程任務")
    st.info(
        "盤後選股、盤後持股與週報仍可用 `python scheduler/jobs.py` 常駐自動推播。\n"
        "盤中分K持股監控已改為 App 內建排程器，請到「持股監控」頁啟動。"
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**選股掃描**")
        scan_count = st.number_input("掃描檔數", min_value=20, max_value=500,
                                      value=100, step=20, key="sched_scan_count")
        top_n = st.number_input("推播前幾名", min_value=1, max_value=10,
                                 value=5, key="sched_top_n")
        if st.button("立即執行選股掃描", use_container_width=True):
            with st.spinner(f"掃描中（{scan_count} 檔）..."):
                from scheduler.jobs import job_daily_scan
                job_daily_scan(top_n=int(top_n), scan_count=int(scan_count))
            st.success("完成，請查看 LINE 訊息")

    with col2:
        st.markdown("**持股警示**")
        st.markdown("檢查所有持股並推播有警示的標的")
        if st.button("立即執行持股警示", use_container_width=True):
            with st.spinner("檢查持股..."):
                from scheduler.jobs import job_portfolio_check
                job_portfolio_check()
            st.success("完成，請查看 LINE 訊息")

    with col3:
        st.markdown("**績效週報**")
        st.markdown("推播交易日誌的累積績效摘要")
        if st.button("立即推播績效摘要", use_container_width=True):
            from scheduler.jobs import job_weekly_performance
            job_weekly_performance()
            st.success("完成，請查看 LINE 訊息")

    st.markdown("---")
    st.subheader("排程時間設定")
    st.markdown("""
    | 任務 | 時間 | 說明 |
    |------|------|------|
    | 盤後選股掃描 | 週一至週五 14:45 | 收盤後自動掃描，推播前 5 名 |
    | 盤中分K持股監控 | 週一至週五 09:00-13:30 | App 內建排程器，至「持股監控」頁啟動 |
    | 盤中持股警示快照 | 週一至週五 13:30 | 外部 scheduler 的一次性日K警示檢查 |
    | 盤後持股警示 | 週一至週五 14:35 | 盤後再次確認警示 |
    | 週績效報告 | 週五 15:10 | 當週勝率、損益統計 |

    **啟動常駐排程：**
    ```bash
    python scheduler/jobs.py
    ```
    """)

    # 自訂推播測試
    st.markdown("---")
    st.subheader("自訂 LINE 推播測試")
    test_msg = st.text_area("訊息內容", value="台股工具測試訊息", height=80)
    if st.button("發送測試訊息"):
        line_ok = send_multicast(test_msg)
        tg_ok = tg_alert(test_msg)
        if line_ok or tg_ok:
            st.success(f"推播成功！{'LINE ✓' if line_ok else ''} {'Telegram ✓' if tg_ok else ''}")
        else:
            st.error("推播失敗，請確認 LINE Token 與 Telegram Bot 設定")
