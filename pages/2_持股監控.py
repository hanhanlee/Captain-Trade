"""
持股監控頁面
輸入持股 → 即時損益 + 警示 + K 線圖
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time

from data.finmind_client import get_daily_price
from modules.portfolio import run_portfolio_check, AlertLevel
from db.database import get_session, init_db
from db.models import Portfolio
from notifications.line_notify import send_message

st.set_page_config(page_title="持股監控", page_icon="💼", layout="wide")
init_db()

LEVEL_COLOR = {
    AlertLevel.DANGER: "#e74c3c",
    AlertLevel.WARNING: "#f39c12",
    AlertLevel.INFO: "#3498db",
    AlertLevel.NONE: "#95a5a6",
}
LEVEL_LABEL = {
    AlertLevel.DANGER: "🔴 危險",
    AlertLevel.WARNING: "🟡 注意",
    AlertLevel.INFO: "🔵 提示",
}


# ── DB 操作 ─────────────────────────────────────────────────
def load_holdings() -> list:
    with get_session() as sess:
        rows = sess.query(Portfolio).all()
        return [
            {
                "id": r.id,
                "stock_id": r.stock_id,
                "stock_name": r.stock_name or "",
                "shares": r.shares,
                "cost_price": r.cost_price,
                "stop_loss": r.stop_loss,
                "take_profit": r.take_profit,
                "note": r.note or "",
            }
            for r in rows
        ]


def save_holding(stock_id, stock_name, shares, cost_price, stop_loss, take_profit, note):
    with get_session() as sess:
        p = Portfolio(
            stock_id=stock_id,
            stock_name=stock_name,
            shares=shares,
            cost_price=cost_price,
            stop_loss=stop_loss if stop_loss else None,
            take_profit=take_profit if take_profit else None,
            note=note,
        )
        sess.add(p)
        sess.commit()


def delete_holding(holding_id: int):
    with get_session() as sess:
        row = sess.query(Portfolio).filter(Portfolio.id == holding_id).first()
        if row:
            sess.delete(row)
            sess.commit()


def update_holding(holding_id: int, stop_loss, take_profit, note):
    with get_session() as sess:
        row = sess.query(Portfolio).filter(Portfolio.id == holding_id).first()
        if row:
            row.stop_loss = stop_loss if stop_loss else None
            row.take_profit = take_profit if take_profit else None
            row.note = note
            sess.commit()


# ── K 線圖 ───────────────────────────────────────────────────
def render_holding_chart(stock_id: str, df: pd.DataFrame, cost_price: float,
                          stop_loss: float = None, take_profit: float = None):
    from modules.indicators import sma, macd, bollinger_bands, rsi

    df = df.copy()
    df["ma5"] = sma(df["close"], 5)
    df["ma20"] = sma(df["close"], 20)
    dif, dea, hist = macd(df["close"])
    df["macd"], df["macd_signal"], df["macd_hist"] = dif, dea, hist

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        vertical_spacing=0.05, row_heights=[0.75, 0.25],
        subplot_titles=[f"{stock_id} K線", "MACD"],
    )

    fig.add_trace(go.Candlestick(
        x=df["date"], open=df["open"], high=df["max"],
        low=df["min"], close=df["close"], name="K線",
        increasing_line_color="#e74c3c", decreasing_line_color="#27ae60",
    ), row=1, col=1)

    for col, color, name in [("ma5", "#f39c12", "MA5"), ("ma20", "#3498db", "MA20")]:
        fig.add_trace(go.Scatter(x=df["date"], y=df[col], name=name,
                                  line=dict(color=color, width=1.2)), row=1, col=1)

    # 成本線
    fig.add_hline(y=cost_price, line_dash="dash", line_color="#9b59b6",
                  annotation_text=f"成本 {cost_price}", row=1, col=1)
    if stop_loss:
        fig.add_hline(y=stop_loss, line_dash="dot", line_color="#e74c3c",
                      annotation_text=f"停損 {stop_loss}", row=1, col=1)
    if take_profit:
        fig.add_hline(y=take_profit, line_dash="dot", line_color="#27ae60",
                      annotation_text=f"停利 {take_profit}", row=1, col=1)

    fig.add_trace(go.Scatter(x=df["date"], y=df["macd"], name="DIF",
                              line=dict(color="#e74c3c", width=1)), row=2, col=1)
    fig.add_trace(go.Scatter(x=df["date"], y=df["macd_signal"], name="DEA",
                              line=dict(color="#3498db", width=1)), row=2, col=1)
    hist_colors = ["#e74c3c" if v >= 0 else "#27ae60" for v in df["macd_hist"].fillna(0)]
    fig.add_trace(go.Bar(x=df["date"], y=df["macd_hist"], name="柱",
                          marker_color=hist_colors, showlegend=False), row=2, col=1)

    fig.update_layout(height=550, template="plotly_dark",
                      xaxis_rangeslider_visible=False, margin=dict(t=30, b=10))
    st.plotly_chart(fig, use_container_width=True)


# ── 頁面主體 ─────────────────────────────────────────────────
st.title("💼 持股監控")

tab_monitor, tab_manage = st.tabs(["📊 即時監控", "✏️ 管理持股"])

# ══ Tab：即時監控 ═══════════════════════════════════════════
with tab_monitor:
    holdings = load_holdings()

    if not holdings:
        st.info("尚未新增任何持股。請前往「管理持股」頁籤新增。")
        st.stop()

    col_refresh, col_notify = st.columns([2, 1])
    with col_refresh:
        refresh = st.button("🔄 更新報價", type="primary", use_container_width=True)
    with col_notify:
        notify_btn = st.button("📲 推播警示到 LINE", use_container_width=True)

    if refresh or "portfolio_stats" not in st.session_state:
        price_data = {}
        prog = st.progress(0)
        for i, h in enumerate(holdings):
            prog.progress((i + 1) / len(holdings))
            try:
                df = get_daily_price(h["stock_id"], days=90)
                if not df.empty:
                    price_data[h["stock_id"]] = df
                time.sleep(0.05)
            except Exception:
                pass
        prog.empty()

        stats_list, all_alerts = run_portfolio_check(holdings, price_data)
        st.session_state["portfolio_stats"] = stats_list
        st.session_state["portfolio_alerts"] = all_alerts
        st.session_state["portfolio_prices"] = price_data

    stats_list = st.session_state.get("portfolio_stats", [])
    all_alerts = st.session_state.get("portfolio_alerts", [])
    price_data = st.session_state.get("portfolio_prices", {})

    # 警示區塊
    danger_alerts = [a for a in all_alerts if a.level == AlertLevel.DANGER]
    warn_alerts = [a for a in all_alerts if a.level == AlertLevel.WARNING]

    if danger_alerts:
        for a in danger_alerts:
            st.error(f"🔴 **{a.stock_id} {a.stock_name}** — {a.reason}　現價 {a.current_price} 元　損益 {a.pnl_pct:+.1f}%")
    if warn_alerts:
        for a in warn_alerts:
            st.warning(f"🟡 **{a.stock_id} {a.stock_name}** — {a.reason}　現價 {a.current_price} 元　損益 {a.pnl_pct:+.1f}%")
    if not danger_alerts and not warn_alerts:
        st.success("✅ 所有持股目前無警示")

    # LINE 推播
    if notify_btn:
        if not all_alerts:
            send_message("💼 持股監控：所有持股目前無警示")
            st.toast("已推播：無警示", icon="📲")
        else:
            lines = ["💼 持股監控警示"]
            for a in all_alerts[:8]:
                emoji = "🔴" if a.level == AlertLevel.DANGER else "🟡"
                lines.append(f"\n{emoji} {a.stock_id} {a.stock_name}")
                lines.append(f"   {a.reason}")
                lines.append(f"   現價 {a.current_price} 元  損益 {a.pnl_pct:+.1f}%")
            send_message("\n".join(lines))
            st.toast("警示已推播到 LINE", icon="📲")

    st.markdown("---")

    # 持股總覽表
    if stats_list:
        total_pnl = sum(s["pnl"] for s in stats_list)
        total_cost = sum(s["cost_price"] * s["shares"] * 1000 for s in stats_list)
        total_pnl_pct = total_pnl / total_cost * 100 if total_cost else 0

        m1, m2, m3 = st.columns(3)
        m1.metric("總未實現損益", f"{total_pnl:+,.0f} 元",
                  delta=f"{total_pnl_pct:+.2f}%")
        m2.metric("持股檔數", f"{len(stats_list)} 檔")
        m3.metric("警示數", f"{len(all_alerts)} 則",
                  delta="需注意" if all_alerts else "正常",
                  delta_color="inverse" if all_alerts else "normal")

        st.markdown("---")

        rows = []
        for s in stats_list:
            alert_labels = [LEVEL_LABEL.get(a.level, "") for a in s["alerts"]]
            rows.append({
                "代碼": s["stock_id"],
                "名稱": s["stock_name"],
                "張數": s["shares"],
                "成本": s["cost_price"],
                "現價": s["close"],
                "損益(元)": s["pnl"],
                "損益%": s["pnl_pct"],
                "高點回撤%": s["drawdown_from_high"],
                "MA20": s["ma20"],
                "警示": "、".join(alert_labels) if alert_labels else "—",
            })

        df_display = pd.DataFrame(rows)

        def color_pnl(val):
            if isinstance(val, (int, float)):
                return "color:#e74c3c" if val > 0 else ("color:#27ae60" if val < 0 else "")
            return ""

        styled = df_display.style.applymap(color_pnl, subset=["損益(元)", "損益%", "高點回撤%"])
        st.dataframe(styled, use_container_width=True, hide_index=True)

        # 個股圖表
        st.markdown("---")
        st.subheader("個股圖表")
        options = [f"{s['stock_id']} {s['stock_name']}" for s in stats_list]
        selected = st.selectbox("選擇持股", options)
        if selected:
            sid = selected.split(" ")[0]
            stat = next((s for s in stats_list if s["stock_id"] == sid), None)
            df = price_data.get(sid)
            if stat and df is not None:
                render_holding_chart(
                    sid, df,
                    cost_price=stat["cost_price"],
                    stop_loss=stat.get("stop_loss"),
                    take_profit=stat.get("take_profit"),
                )


# ══ Tab：管理持股 ═══════════════════════════════════════════
with tab_manage:
    st.subheader("新增持股")

    with st.form("add_holding_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            new_id = st.text_input("股票代碼 *", placeholder="例：2330")
            new_name = st.text_input("股票名稱", placeholder="例：台積電")
        with c2:
            new_shares = st.number_input("持有張數 *", min_value=1, value=1, step=1)
            new_cost = st.number_input("成本價 *", min_value=0.01, value=100.0, step=0.1)
        with c3:
            new_sl = st.number_input("停損價（選填）", min_value=0.0, value=0.0, step=0.1)
            new_tp = st.number_input("停利價（選填）", min_value=0.0, value=0.0, step=0.1)
        new_note = st.text_input("備註（選填）")

        submitted = st.form_submit_button("新增", type="primary", use_container_width=True)
        if submitted:
            if not new_id:
                st.error("請輸入股票代碼")
            else:
                save_holding(
                    stock_id=new_id.strip(),
                    stock_name=new_name.strip(),
                    shares=new_shares,
                    cost_price=new_cost,
                    stop_loss=new_sl if new_sl > 0 else None,
                    take_profit=new_tp if new_tp > 0 else None,
                    note=new_note,
                )
                st.success(f"已新增 {new_id} {new_name}")
                st.rerun()

    st.markdown("---")
    st.subheader("現有持股")

    holdings = load_holdings()
    if not holdings:
        st.info("尚無持股紀錄")
    else:
        for h in holdings:
            with st.expander(f"{h['stock_id']} {h['stock_name']}  |  {h['shares']} 張  |  成本 {h['cost_price']} 元"):
                ec1, ec2, ec3 = st.columns(3)
                with ec1:
                    e_sl = st.number_input(f"停損價", value=float(h["stop_loss"] or 0),
                                           step=0.1, key=f"sl_{h['id']}")
                with ec2:
                    e_tp = st.number_input(f"停利價", value=float(h["take_profit"] or 0),
                                           step=0.1, key=f"tp_{h['id']}")
                with ec3:
                    e_note = st.text_input("備註", value=h["note"], key=f"note_{h['id']}")

                btn_col1, btn_col2 = st.columns(2)
                with btn_col1:
                    if st.button("儲存修改", key=f"save_{h['id']}", use_container_width=True):
                        update_holding(h["id"],
                                       stop_loss=e_sl if e_sl > 0 else None,
                                       take_profit=e_tp if e_tp > 0 else None,
                                       note=e_note)
                        st.success("已更新")
                        st.rerun()
                with btn_col2:
                    if st.button("刪除", key=f"del_{h['id']}", type="secondary",
                                  use_container_width=True):
                        delete_holding(h["id"])
                        st.rerun()
