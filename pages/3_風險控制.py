"""
風險控制引擎頁面
- 部位大小計算（固定風險法 / Kelly Criterion）
- 帳戶總曝險監控
- 最大回撤分析
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from data.finmind_client import get_stock_list

from modules.risk import (
    calc_position_fixed_risk,
    calc_position_kelly,
    calc_portfolio_exposure,
    calc_max_drawdown,
    calc_sector_exposure,
    calc_atr_trailing_stop,
)
from modules.journal import get_all_trades, calc_performance
from db.database import get_session, init_db
from db.models import Portfolio

st.set_page_config(page_title="風險控制", page_icon="🛡️", layout="wide")
init_db()

st.title("🛡️ 風險控制引擎")
st.markdown("*「先求不輸，再求獲利。保住本金才能等待機會。」*")
st.markdown("---")

tab_position, tab_exposure, tab_drawdown = st.tabs(
    ["📐 部位計算", "⚖️ 帳戶曝險", "📉 回撤分析"]
)


# ══ Tab：部位計算 ════════════════════════════════════════════
with tab_position:
    st.subheader("進場前：應該買幾張？")

    with st.sidebar:
        st.header("⚙️ 帳戶設定")
        account_size = st.number_input(
            "帳戶總資金（元）",
            min_value=100_000, max_value=100_000_000,
            value=1_000_000, step=100_000,
            format="%d",
        )
        st.markdown("---")
        st.caption("此數值會套用到所有風險計算")

    method = st.radio("計算方法", ["固定風險法（推薦）", "Kelly Criterion"],
                      horizontal=True)

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("#### 交易參數")
        entry_price = st.number_input("預計進場價（元）", min_value=1.0, value=100.0, step=0.5)
        stop_loss = st.number_input("停損價（元）", min_value=0.1,
                                     value=round(entry_price * 0.92, 1), step=0.5)
        target_price = st.number_input("目標價（元，選填）", min_value=0.0,
                                        value=round(entry_price * 1.15, 1), step=0.5)

    with col_r:
        if method.startswith("固定"):
            st.markdown("#### 風險設定")
            risk_pct = st.slider("每筆最大虧損（帳戶%）", 0.5, 5.0, 2.0, 0.5)
            st.caption(f"= 最多虧損 **{account_size * risk_pct / 100:,.0f} 元**")
        else:
            st.markdown("#### 歷史績效")
            df_trades = get_all_trades()
            perf = calc_performance(df_trades) if not df_trades.empty else {}

            default_wr = perf.get("win_rate", 55.0)
            default_win = abs(perf.get("avg_win", 15000))
            default_loss = abs(perf.get("avg_loss", 8000))

            win_rate_input = st.number_input("歷史勝率（%）", 10.0, 90.0,
                                              float(default_wr), 1.0)
            avg_win_input = st.number_input("平均獲利（元/張）", 1000.0, 200000.0,
                                             float(default_win), 1000.0)
            avg_loss_input = st.number_input("平均虧損（元/張）", 100.0, 100000.0,
                                              float(default_loss), 500.0)
            kelly_frac = st.select_slider("Kelly 使用比例（越保守越安全）",
                                           options=[0.1, 0.25, 0.5, 1.0],
                                           value=0.25,
                                           format_func=lambda x: f"{x:.0%}")

    st.markdown("---")

    if st.button("計算建議部位", type="primary", use_container_width=True):
        try:
            if method.startswith("固定"):
                result = calc_position_fixed_risk(
                    account_size=account_size,
                    risk_pct=risk_pct,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    target_price=target_price if target_price > entry_price else None,
                )
            else:
                result = calc_position_kelly(
                    account_size=account_size,
                    win_rate=win_rate_input,
                    avg_win=avg_win_input,
                    avg_loss=avg_loss_input,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    kelly_fraction=kelly_frac,
                )

            st.session_state["position_result"] = result
        except ValueError as e:
            st.error(str(e))

    if "position_result" in st.session_state:
        r = st.session_state["position_result"]

        if r.recommended_shares == 0:
            st.error(r.note)
        else:
            # 主要結果
            st.markdown("### 建議結果")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("建議張數", f"{r.recommended_shares} 張")
            c2.metric("投入金額", f"{r.total_cost:,.0f} 元",
                      delta=f"占帳戶 {r.total_cost/account_size*100:.1f}%")
            c3.metric("最大風險", f"{r.risk_amount:,.0f} 元",
                      delta=f"{r.risk_pct_of_account}% 帳戶",
                      delta_color="inverse")
            c4.metric("報酬風險比", f"{r.reward_risk_ratio:.1f} : 1",
                      delta="良好" if r.reward_risk_ratio >= 2 else "偏低",
                      delta_color="normal" if r.reward_risk_ratio >= 2 else "inverse")

            st.caption(f"計算方法：{r.method}　{r.note}")

            # 視覺化：風險/報酬長條
            if r.reward_risk_ratio > 0 and target_price > entry_price:
                fig_rr = go.Figure()
                profit = (target_price - entry_price) * r.recommended_shares * 1000
                fig_rr.add_trace(go.Bar(
                    x=["最大虧損", "預期獲利"],
                    y=[-r.risk_amount, profit],
                    marker_color=["#e74c3c", "#27ae60"],
                    text=[f"-{r.risk_amount:,.0f}", f"+{profit:,.0f}"],
                    textposition="outside",
                ))
                fig_rr.add_hline(y=0, line_color="gray")
                fig_rr.update_layout(
                    height=280, template="plotly_dark",
                    title="風險 vs 報酬（元）",
                    margin=dict(t=40, b=10), showlegend=False,
                )
                st.plotly_chart(fig_rr, use_container_width=True)

            # 各張數情境比較
            st.markdown("#### 各張數情境比較")
            scenarios = []
            for n in range(1, min(r.recommended_shares + 6, 11)):
                cost_n = n * 1000 * entry_price
                risk_n = n * 1000 * (entry_price - stop_loss)
                profit_n = n * 1000 * (target_price - entry_price) if target_price > entry_price else 0
                scenarios.append({
                    "張數": n,
                    "投入金額": f"{cost_n:,.0f}",
                    "最大虧損": f"{risk_n:,.0f}",
                    "潛在獲利": f"{profit_n:,.0f}" if profit_n else "—",
                    "風險占帳戶%": f"{risk_n/account_size*100:.2f}%",
                    "建議": "✅ 建議" if n == r.recommended_shares else "",
                })
            st.dataframe(pd.DataFrame(scenarios), use_container_width=True, hide_index=True)

    # ── ATR 移動停利計算器 ──────────────────────────────────
    st.markdown("---")
    with st.expander("📈 ATR 移動停利計算器（進場後追蹤用）", expanded=False):
        st.markdown("""
        **移動停損線** = 持有期間最高價 − N × ATR

        比固定停利更能守住大行情的獲利，同時防止被短線震盪洗出場。
        """)
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            atr_entry = st.number_input("進場成本（元）", min_value=1.0, value=100.0,
                                         step=0.5, key="atr_entry")
            atr_current = st.number_input("目前股價（元）", min_value=1.0, value=110.0,
                                           step=0.5, key="atr_current")
        with col_b:
            atr_highest = st.number_input("持有期間最高價（元）", min_value=1.0, value=115.0,
                                           step=0.5, key="atr_highest")
            atr_val = st.number_input("ATR 值（元）", min_value=0.1, value=3.0,
                                       step=0.1, key="atr_val",
                                       help="從 K 線圖指標讀取，或以近期平均波幅估算")
        with col_c:
            atr_mult = st.slider("ATR 倍數", min_value=1.0, max_value=4.0, value=2.0,
                                  step=0.5, key="atr_mult",
                                  help="2 倍為常用設定；波動大時可用 2.5~3 倍")

        if st.button("計算移動停利", key="btn_atr"):
            ts = calc_atr_trailing_stop(
                entry_price=atr_entry,
                current_price=atr_current,
                highest_price=atr_highest,
                atr_value=atr_val,
                multiplier=atr_mult,
            )
            if ts:
                status_color = {"safe": "✅", "warning": "⚠️", "triggered": "🔴"}.get(ts["status"], "")
                status_text = {"safe": "安全", "warning": "接近停損", "triggered": "已觸發出場"}.get(ts["status"], "")

                c1, c2, c3 = st.columns(3)
                c1.metric("移動停損線", f"{ts['trailing_stop']:.2f} 元",
                           delta=f"{status_color} {status_text}")
                c2.metric("已鎖定獲利", f"{ts['locked_profit_pct']:+.2f}%",
                           delta_color="normal" if ts["locked_profit_pct"] >= 0 else "inverse")
                c3.metric("距停損緩衝", f"{ts['profit_buffer_pct']:.2f}%",
                           delta_color="normal" if ts["profit_buffer_pct"] >= 2 else "inverse")

                if ts["status"] == "triggered":
                    st.error(f"🔴 股價已跌破移動停損線（{ts['trailing_stop']:.2f}），依紀律應出場")
                elif ts["status"] == "warning":
                    st.warning(f"⚠️ 距停損線僅 {ts['profit_buffer_pct']:.2f}%，密切注意")
                else:
                    st.success(f"✅ 停損線 {ts['trailing_stop']:.2f}，鎖定獲利 {ts['locked_profit_pct']:+.2f}%")

                st.caption(f"公式：{atr_highest:.2f}（最高價）− {atr_mult} × {atr_val:.2f}（ATR）= {ts['trailing_stop']:.2f}")


# ══ Tab：帳戶曝險 ════════════════════════════════════════════
with tab_exposure:
    st.subheader("目前帳戶整體風險狀況")

    if "portfolio_stats" not in st.session_state:
        st.info("請先前往「持股監控」頁面更新持股資料。")
    else:
        stats_list = st.session_state["portfolio_stats"]
        if not stats_list:
            st.info("持股清單為空")
        else:
            account_size_exp = st.number_input(
                "帳戶總資金（元）", min_value=100_000, max_value=100_000_000,
                value=1_000_000, step=100_000, format="%d", key="exp_account",
            )

            exposure = calc_portfolio_exposure(stats_list, account_size_exp)

            # 警示
            for w in exposure.get("warnings", []):
                st.warning(w)
            if not exposure.get("warnings"):
                st.success("✅ 帳戶曝險在合理範圍內")

            # 指標
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("已投入市值", f"{exposure['total_market_value']:,.0f} 元")
            c2.metric("總曝險比例", f"{exposure['exposure_pct']}%",
                      delta_color="inverse" if exposure['exposure_pct'] > 80 else "normal")
            c3.metric("剩餘現金比", f"{exposure['cash_pct']}%")
            c4.metric("總未實現損益", f"{exposure['total_pnl']:+,.0f} 元",
                      delta_color="normal")

            st.markdown("---")

            # 持股權重圓餅圖
            col_pie, col_bar = st.columns(2)

            with col_pie:
                positions = exposure["positions"]
                labels = [f"{p['stock_id']} {p['stock_name']}" for p in positions]
                labels.append("現金")
                values = [p["market_value"] for p in positions]
                values.append(max(0, account_size_exp - exposure["total_market_value"]))

                fig_pie = go.Figure(go.Pie(
                    labels=labels, values=values,
                    hole=0.4,
                    textinfo="label+percent",
                ))
                fig_pie.update_layout(
                    height=350, template="plotly_dark",
                    title="帳戶配置比例", margin=dict(t=40, b=10),
                )
                st.plotly_chart(fig_pie, use_container_width=True)

            with col_bar:
                df_pos = pd.DataFrame(positions)
                colors = ["#27ae60" if p >= 0 else "#e74c3c" for p in df_pos["pnl_pct"]]
                fig_bar = go.Figure(go.Bar(
                    x=df_pos["stock_id"] + " " + df_pos["stock_name"].fillna(""),
                    y=df_pos["pnl_pct"],
                    marker_color=colors,
                    text=[f"{v:+.1f}%" for v in df_pos["pnl_pct"]],
                    textposition="outside",
                ))
                fig_bar.add_hline(y=0, line_color="gray")
                fig_bar.update_layout(
                    height=350, template="plotly_dark",
                    title="各持股損益%", margin=dict(t=40, b=10),
                    yaxis_title="損益%",
                )
                st.plotly_chart(fig_bar, use_container_width=True)

            # ── 產業集中度分析 ──────────────────────────────
            st.markdown("---")
            st.markdown("#### 🏭 產業集中度分析")
            max_sector = st.slider("單一產業曝險上限（帳戶%）", 3.0, 15.0, 7.0, 0.5,
                                    key="max_sector_pct",
                                    help="超過此比例代表同產業風險過度集中")

            if st.button("分析產業曝險", key="btn_sector"):
                # 取得產業對應
                try:
                    stock_info_df = get_stock_list()
                    ind_map = stock_info_df.set_index("stock_id")["industry_category"].to_dict()
                except Exception:
                    ind_map = {}

                holdings_with_ind = [
                    {**s, "industry": ind_map.get(s["stock_id"], "未分類")}
                    for s in stats_list
                ]
                sector_result = calc_sector_exposure(
                    holdings_with_ind, account_size_exp, max_sector_pct=max_sector
                )
                st.session_state["sector_result"] = sector_result

            if "sector_result" in st.session_state:
                sr = st.session_state["sector_result"]
                for w in sr["warnings"]:
                    st.warning(w)
                if sr["sectors"] and not sr["warnings"]:
                    st.success("✅ 各產業曝險均在合理範圍內")

                if sr["sectors"]:
                    df_sec = pd.DataFrame(sr["sectors"])
                    fig_sec = go.Figure(go.Bar(
                        x=df_sec["industry"],
                        y=df_sec["pct_of_account"],
                        marker_color=[
                            "#e74c3c" if v > max_sector else "#27ae60"
                            for v in df_sec["pct_of_account"]
                        ],
                        text=[f"{v:.1f}%" for v in df_sec["pct_of_account"]],
                        textposition="outside",
                    ))
                    fig_sec.add_hline(y=max_sector, line_dash="dash", line_color="#f39c12",
                                       annotation_text=f"上限 {max_sector}%")
                    fig_sec.update_layout(
                        height=300, template="plotly_dark",
                        title="各產業佔帳戶比例",
                        margin=dict(t=40, b=10), showlegend=False,
                        yaxis_title="%",
                    )
                    st.plotly_chart(fig_sec, use_container_width=True)

                    df_sec_display = df_sec.rename(columns={
                        "industry": "產業", "market_value": "市值(元)",
                        "pct_of_account": "佔帳戶%", "stocks": "持股"
                    })
                    st.dataframe(df_sec_display, use_container_width=True, hide_index=True)

            st.markdown("---")
            # 風險指引
            st.markdown("""
            #### 風險配置原則

            | 指標 | 安全範圍 | 你的狀況 |
            |------|----------|----------|
            | 單一持股 | < 25% 帳戶 | 見上方圓餅圖 |
            | 同產業持股 | < 7% 帳戶 | 見產業集中度 |
            | 總曝險 | < 80% 帳戶 | {exp}% |
            | 每筆風險 | < 2% 帳戶 | 見部位計算頁 |
            """.format(exp=exposure['exposure_pct']))


# ══ Tab：回撤分析 ════════════════════════════════════════════
with tab_drawdown:
    st.subheader("帳戶最大回撤分析")

    df_trades = get_all_trades()
    sell_df = df_trades[(df_trades["action"] == "SELL") & df_trades["pnl"].notna()].sort_values("trade_date") if not df_trades.empty else pd.DataFrame()

    if sell_df.empty:
        st.info("尚無賣出記錄，完成交易後此頁將顯示回撤曲線。")
    else:
        account_init = st.number_input(
            "初始帳戶資金（元）", min_value=100_000, max_value=100_000_000,
            value=1_000_000, step=100_000, format="%d", key="dd_account",
        )

        sell_df["cumulative_pnl"] = sell_df["pnl"].cumsum()
        sell_df["equity"] = account_init + sell_df["cumulative_pnl"]

        equity_list = [account_init] + sell_df["equity"].tolist()
        dd_result = calc_max_drawdown(equity_list)

        c1, c2, c3 = st.columns(3)
        c1.metric("最大回撤", f"{dd_result['max_drawdown_pct']:.2f}%",
                  delta_color="inverse")
        c2.metric("高點淨值", f"{dd_result['peak']:,.0f} 元")
        c3.metric("最低點", f"{dd_result['trough']:,.0f} 元")

        # 回撤狀態
        if dd_result["max_drawdown_pct"] > -15:
            st.success("✅ 最大回撤在可接受範圍（< 15%）")
        elif dd_result["max_drawdown_pct"] > -25:
            st.warning("⚠️ 最大回撤偏大（15–25%），建議檢視策略")
        else:
            st.error("🔴 最大回撤超過 25%，策略需要重新評估")

        st.markdown("---")

        # 淨值曲線
        dates = [sell_df["trade_date"].iloc[0]] + sell_df["trade_date"].tolist()
        equities = equity_list[:len(dates)]

        # 計算滾動回撤
        peaks = pd.Series(equities).cummax()
        drawdowns = (pd.Series(equities) - peaks) / peaks * 100

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=list(range(len(equities))), y=equities,
            name="帳戶淨值", line=dict(color="#3498db", width=2),
            fill="tozeroy", fillcolor="rgba(52,152,219,0.08)",
        ))
        fig.add_trace(go.Scatter(
            x=list(range(len(equities))), y=list(peaks),
            name="淨值高點", line=dict(color="#f39c12", width=1, dash="dot"),
        ))
        fig.add_hline(y=account_init, line_dash="dash", line_color="gray",
                      annotation_text="初始資金")
        fig.update_layout(
            height=320, template="plotly_dark",
            title="帳戶淨值曲線", margin=dict(t=40, b=10),
            xaxis_title="交易次數",
        )
        st.plotly_chart(fig, use_container_width=True)

        # 回撤曲線
        fig_dd = go.Figure()
        fig_dd.add_trace(go.Scatter(
            x=list(range(len(drawdowns))), y=list(drawdowns),
            fill="tozeroy", fillcolor="rgba(231,76,60,0.2)",
            line=dict(color="#e74c3c", width=1.5),
            name="回撤%",
        ))
        fig_dd.add_hline(y=-15, line_dash="dash", line_color="#f39c12",
                          annotation_text="警戒 -15%")
        fig_dd.add_hline(y=-25, line_dash="dash", line_color="#e74c3c",
                          annotation_text="危險 -25%")
        fig_dd.update_layout(
            height=220, template="plotly_dark",
            title="回撤曲線（%）", margin=dict(t=40, b=10),
            yaxis_title="%",
        )
        st.plotly_chart(fig_dd, use_container_width=True)
