"""
交易日誌頁面
記錄每筆交易 → 統計勝率/盈虧比/情緒分析
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date

from modules.journal import (
    add_trade, get_trade, update_trade, get_all_trades, delete_trade,
    calc_performance, calc_emotion_stats, sync_open_trades_to_portfolio,
)
from db.database import init_db

st.set_page_config(page_title="交易日誌", page_icon="📔", layout="wide")
init_db()

_synced_positions = sync_open_trades_to_portfolio()
if _synced_positions:
    st.toast(
        "已將交易日誌中的未入列持股補進持股監控："
        + "、".join([p["stock_id"] for p in _synced_positions]),
        icon="✅",
    )

EMOTIONS = ["冷靜", "樂觀", "貪婪", "恐慌", "衝動", "猶豫", "FOMO"]
ACTIONS = ["BUY", "SELL"]

st.title("📔 交易日誌")
tab_stats, tab_log, tab_add = st.tabs(["📊 績效統計", "📋 交易記錄", "➕ 新增交易"])


# ══ Tab：績效統計 ═══════════════════════════════════════════
with tab_stats:
    df = get_all_trades()

    if df.empty:
        st.info("尚無交易記錄，請先在「新增交易」頁籤輸入資料。")
    else:
        perf = calc_performance(df)
        if not perf:
            st.info("尚無已平倉（SELL + 損益）的記錄，統計將在新增賣出記錄後顯示。")
        else:
            # 核心指標
            st.subheader("核心績效")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("總交易次數", perf["total_trades"],
                      delta=f"{perf['win_trades']}勝 {perf['loss_trades']}敗")
            c2.metric("勝率", f"{perf['win_rate']}%")
            c3.metric("盈虧比", perf["profit_factor"],
                      delta=perf["pf_rating"],
                      delta_color="normal")
            c4.metric("總損益", f"{perf['total_pnl']:+,.0f} 元",
                      delta_color="normal")

            c5, c6, c7, c8 = st.columns(4)
            c5.metric("期望值", f"{perf['expected_value']:+,.0f} 元",
                      delta="正期望值 ✅" if perf["expected_value"] > 0 else "負期望值 ❌",
                      delta_color="normal")
            c6.metric("平均獲利", f"{perf['avg_win']:+,.0f} 元")
            c7.metric("最佳交易", f"{perf['best_trade']:+,.0f} 元")
            c8.metric("最差交易", f"{perf['worst_trade']:+,.0f} 元")

            st.markdown("---")

            # 累積損益曲線
            sell_df = df[(df["action"] == "SELL") & df["pnl"].notna()].sort_values("trade_date")
            if not sell_df.empty:
                sell_df["cumulative_pnl"] = sell_df["pnl"].cumsum()

                st.subheader("累積損益曲線")
                fig_pnl = go.Figure()
                fig_pnl.add_trace(go.Scatter(
                    x=sell_df["trade_date"], y=sell_df["cumulative_pnl"],
                    fill="tozeroy",
                    fillcolor="rgba(52,152,219,0.15)",
                    line=dict(color="#3498db", width=2),
                    name="累積損益",
                ))
                fig_pnl.add_hline(y=0, line_dash="dash", line_color="gray")
                fig_pnl.update_layout(
                    height=300, template="plotly_dark",
                    yaxis_title="累積損益（元）",
                    margin=dict(t=20, b=20),
                )
                st.plotly_chart(fig_pnl, use_container_width=True)

            col_l, col_r = st.columns(2)

            # 各股票損益長條
            with col_l:
                st.subheader("各股票損益")
                stock_pnl = (sell_df.groupby(["stock_id", "stock_name"])["pnl"]
                             .sum().reset_index()
                             .sort_values("pnl", ascending=True))
                stock_pnl["label"] = stock_pnl["stock_id"] + " " + stock_pnl["stock_name"].fillna("")
                colors = ["#27ae60" if v >= 0 else "#e74c3c" for v in stock_pnl["pnl"]]
                fig_stock = go.Figure(go.Bar(
                    x=stock_pnl["pnl"], y=stock_pnl["label"],
                    orientation="h", marker_color=colors,
                ))
                fig_stock.update_layout(height=300, template="plotly_dark", margin=dict(t=10, b=10))
                st.plotly_chart(fig_stock, use_container_width=True)

            # 情緒分析
            with col_r:
                st.subheader("情緒 vs 勝率")
                emotion_df = calc_emotion_stats(df)
                if not emotion_df.empty:
                    fig_em = px.bar(emotion_df, x="情緒", y="勝率%", color="勝率%",
                                    color_continuous_scale="RdYlGn", range_color=[0, 100],
                                    text="勝率%", template="plotly_dark")
                    fig_em.update_layout(height=300, margin=dict(t=10, b=10), showlegend=False)
                    st.plotly_chart(fig_em, use_container_width=True)
                else:
                    st.info("尚無足夠情緒標籤資料")


# ══ Tab：交易記錄 ═══════════════════════════════════════════
with tab_log:
    df = get_all_trades()
    if df.empty:
        st.info("尚無交易記錄")
    else:
        # 篩選
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            filter_action = st.multiselect("篩選動作", ["BUY", "SELL"], default=["BUY", "SELL"])
        with col_f2:
            filter_stock = st.text_input("篩選股票代碼", placeholder="空白顯示全部")

        filtered = df[df["action"].isin(filter_action)]
        if filter_stock:
            filtered = filtered[filtered["stock_id"].str.contains(filter_stock)]

        display = filtered.rename(columns={
            "id": "ID", "stock_id": "代碼", "stock_name": "名稱", "action": "動作",
            "price": "價格", "shares": "股數", "trade_date": "日期",
            "reason": "理由", "emotion": "情緒", "pnl": "損益(元)",
        })

        st.dataframe(display, use_container_width=True, hide_index=True)

        st.markdown("---")

        # ── 編輯 ────────────────────────────────────────────
        with st.expander("✏️ 修改記錄", expanded=st.session_state.get("edit_expanded", False)):
            all_ids = df["id"].tolist()
            edit_id = st.number_input("輸入要修改的 ID", min_value=1, step=1,
                                      value=int(all_ids[0]) if all_ids else 1,
                                      key="edit_id_input")

            if st.button("載入記錄", key="load_edit_btn"):
                rec = get_trade(int(edit_id))
                if rec:
                    st.session_state["editing_record"] = rec
                    st.session_state["edit_expanded"] = True
                    st.rerun()
                else:
                    st.error(f"找不到 ID {edit_id}")

            rec = st.session_state.get("editing_record")
            if rec:
                st.caption(f"正在編輯 ID {rec['id']}")
                with st.form("edit_trade_form"):
                    er1c1, er1c2, er1c3 = st.columns(3)
                    with er1c1:
                        e_id_str   = st.text_input("股票代碼 *", value=rec["stock_id"])
                        e_name     = st.text_input("股票名稱",   value=rec["stock_name"])
                    with er1c2:
                        e_action   = st.selectbox("動作 *", ACTIONS,
                                                  index=ACTIONS.index(rec["action"]) if rec["action"] in ACTIONS else 0)
                        e_date     = st.date_input("交易日期 *", value=rec["trade_date"])
                    with er1c3:
                        e_price    = st.number_input("價格 *", min_value=0.01,
                                                     value=rec["price"], step=0.1)
                        e_shares   = st.number_input("股數 *", min_value=1,
                                                     value=rec["shares"], step=1)

                    er2c1, er2c2 = st.columns(2)
                    with er2c1:
                        e_pnl      = st.number_input("損益（元）",
                                                     value=float(rec["pnl"]) if rec["pnl"] is not None else 0.0,
                                                     step=100.0)
                        emotion_opts = [""] + EMOTIONS
                        e_emotion  = st.selectbox("情緒標記", emotion_opts,
                                                  index=emotion_opts.index(rec["emotion"]) if rec["emotion"] in emotion_opts else 0)
                    with er2c2:
                        e_reason   = st.text_area("進出場理由", value=rec["reason"], height=100)

                    save_col, cancel_col = st.columns(2)
                    with save_col:
                        save_btn = st.form_submit_button("儲存修改", type="primary", use_container_width=True)
                    with cancel_col:
                        cancel_btn = st.form_submit_button("取消", use_container_width=True)

                    if save_btn:
                        if not e_id_str.strip():
                            st.error("股票代碼不能空白")
                        else:
                            ok = update_trade(
                                trade_id=rec["id"],
                                stock_id=e_id_str.strip(),
                                stock_name=e_name.strip(),
                                action=e_action,
                                price=e_price,
                                shares=e_shares,
                                trade_date=e_date,
                                reason=e_reason,
                                emotion=e_emotion,
                                pnl=e_pnl if e_action == "SELL" and e_pnl != 0 else None,
                            )
                            if ok:
                                st.session_state.pop("editing_record", None)
                                st.session_state["edit_expanded"] = False
                                st.success(f"✅ 已更新 ID {rec['id']}")
                                st.rerun()
                            else:
                                st.error("更新失敗，請確認 ID 是否正確")

                    if cancel_btn:
                        st.session_state.pop("editing_record", None)
                        st.session_state["edit_expanded"] = False
                        st.rerun()

        # ── 刪除 ────────────────────────────────────────────
        with st.expander("🗑️ 刪除記錄"):
            all_ids = df["id"].tolist()
            del_id = st.number_input("輸入要刪除的 ID", min_value=1, step=1,
                                     value=int(all_ids[0]) if all_ids else 1,
                                     key="del_id_input")
            if st.button("確認刪除", type="secondary"):
                if int(del_id) in all_ids:
                    delete_trade(int(del_id))
                    st.success(f"已刪除 ID {del_id}")
                    st.rerun()
                else:
                    st.error("找不到此 ID")


# ══ Tab：新增交易 ═══════════════════════════════════════════
with tab_add:
    st.subheader("新增交易記錄")

    with st.form("add_trade_form", clear_on_submit=True):
        r1c1, r1c2, r1c3 = st.columns(3)
        with r1c1:
            t_id = st.text_input("股票代碼 *", placeholder="2330")
            t_name = st.text_input("股票名稱", placeholder="台積電")
        with r1c2:
            t_action = st.selectbox("動作 *", ACTIONS)
            t_date = st.date_input("交易日期 *", value=date.today())
        with r1c3:
            t_price = st.number_input("價格 *", min_value=0.01, value=100.0, step=0.1)
            t_shares = st.number_input("股數 *", min_value=1, value=1000, step=1)

        r2c1, r2c2 = st.columns(2)
        with r2c1:
            t_pnl = st.number_input("損益（賣出時填，元）", value=0.0, step=100.0)
            t_emotion = st.selectbox("情緒標記", [""] + EMOTIONS)
        with r2c2:
            t_reason = st.text_area("進出場理由", placeholder="例：突破季線放量，符合選股條件...", height=100)

        submitted = st.form_submit_button("新增記錄", type="primary", use_container_width=True)
        if submitted:
            if not t_id:
                st.error("請填入股票代碼")
            else:
                portfolio_sync = add_trade(
                    stock_id=t_id.strip(),
                    stock_name=t_name.strip(),
                    action=t_action,
                    price=t_price,
                    shares=t_shares,
                    trade_date=t_date,
                    reason=t_reason,
                    emotion=t_emotion,
                    pnl=t_pnl if t_action == "SELL" and t_pnl != 0 else None,
                )
                if t_action == "BUY":
                    st.success(f"✅ 已新增 {t_action} {t_id} {t_name} @ {t_price}，並同步加入持股監控")
                elif portfolio_sync:
                    if portfolio_sync.get("removed"):
                        st.success(f"✅ 已新增 {t_action} {t_id} {t_name} @ {t_price}，並已從持股監控移除")
                    else:
                        st.success(
                            f"✅ 已新增 {t_action} {t_id} {t_name} @ {t_price}，持股監控剩餘 "
                            f"{portfolio_sync['new_shares']} 股"
                        )
                else:
                    st.success(f"✅ 已新增 {t_action} {t_id} {t_name} @ {t_price}")
