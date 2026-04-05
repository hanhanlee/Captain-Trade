"""
持股監控頁面
輸入持股 → 即時損益 + 警示 + K 線圖
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from sqlalchemy import text

from data.finmind_client import get_daily_price
from modules.portfolio import run_portfolio_check, AlertLevel
from modules.portfolio_io import (
    STANDARD_COLUMNS,
    holdings_to_export_df,
    parse_holdings_csv,
    validate_holdings_df,
)
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
                "notes": (r.notes if hasattr(r, "notes") else None) or r.note or "",
            }
            for r in rows
        ]


def save_holding(stock_id, stock_name, shares, cost_price, stop_loss, take_profit, notes):
    with get_session() as sess:
        p = Portfolio(
            stock_id=stock_id,
            stock_name=stock_name,
            shares=shares,
            cost_price=cost_price,
            stop_loss=stop_loss if stop_loss else None,
            take_profit=take_profit if take_profit else None,
            note=notes,
            notes=notes,
        )
        sess.add(p)
        sess.commit()


def delete_holding(holding_id: int):
    with get_session() as sess:
        row = sess.query(Portfolio).filter(Portfolio.id == holding_id).first()
        if row:
            sess.delete(row)
            sess.commit()


def update_holding(holding_id: int, shares, cost_price, stop_loss, take_profit, notes):
    with get_session() as sess:
        row = sess.query(Portfolio).filter(Portfolio.id == holding_id).first()
        if row:
            row.shares = int(shares)
            row.cost_price = float(cost_price)
            row.stop_loss = stop_loss if stop_loss else None
            row.take_profit = take_profit if take_profit else None
            row.note = notes
            if hasattr(row, "notes"):
                row.notes = notes
            sess.commit()


def replace_holdings(df: pd.DataFrame):
    """以匯入結果整批覆寫 portfolio。"""
    with get_session() as sess:
        sess.execute(text("DELETE FROM portfolio"))
        for row in df.to_dict("records"):
            sess.add(Portfolio(
                stock_id=row["stock_id"],
                stock_name=row.get("stock_name", ""),
                shares=int(row["shares"]),
                cost_price=float(row["cost_price"]),
                stop_loss=float(row["stop_loss"]) if pd.notna(row["stop_loss"]) else None,
                take_profit=float(row["take_profit"]) if pd.notna(row["take_profit"]) else None,
                note=row.get("notes", "") or "",
                notes=row.get("notes", "") or "",
            ))
        sess.commit()


def append_holdings(df: pd.DataFrame):
    """將匯入結果附加到 portfolio。"""
    with get_session() as sess:
        for row in df.to_dict("records"):
            sess.add(Portfolio(
                stock_id=row["stock_id"],
                stock_name=row.get("stock_name", ""),
                shares=int(row["shares"]),
                cost_price=float(row["cost_price"]),
                stop_loss=float(row["stop_loss"]) if pd.notna(row["stop_loss"]) else None,
                take_profit=float(row["take_profit"]) if pd.notna(row["take_profit"]) else None,
                note=row.get("notes", "") or "",
                notes=row.get("notes", "") or "",
            ))
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
    else:
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
            total_cost = sum(s["cost_price"] * s["shares"] for s in stats_list)
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
                    "股數": s["shares"],
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
            new_shares = st.number_input("持有股數 *", min_value=1, value=1000, step=1)
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
                    notes=new_note,
                )
                st.success(f"已新增 {new_id} {new_name}")
                st.rerun()

    st.markdown("---")
    st.subheader("匯入 / 匯出持股")

    holdings_for_io = load_holdings()
    export_df = holdings_to_export_df(holdings_for_io)
    export_csv = export_df.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        "匯出持股 CSV",
        data=export_csv,
        file_name="portfolio_export.csv",
        mime="text/csv",
        use_container_width=True,
    )

    upload_file = st.file_uploader("匯入持股 CSV", type=["csv"])
    if upload_file is not None:
        try:
            parsed_df, parse_meta = parse_holdings_csv(upload_file.getvalue())
            st.session_state["portfolio_import_df"] = parsed_df
            st.session_state["portfolio_import_meta"] = parse_meta
            st.success(
                f"CSV 解析成功，編碼：{parse_meta['encoding']}，共 {parse_meta['row_count']} 筆。"
            )
        except Exception as e:
            st.error(f"CSV 解析失敗：{e}")

    import_df = st.session_state.get("portfolio_import_df")
    import_meta = st.session_state.get("portfolio_import_meta", {})
    if import_df is not None and not import_df.empty:
        st.info("資料已解析。請在下方表格中補齊『停損價』等設定後，點擊確認儲存。")
        if import_meta.get("mapping"):
            st.caption(f"欄位對應：{import_meta['mapping']}")

        editable_df = st.data_editor(
            import_df,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "stock_id": st.column_config.TextColumn("stock_id", required=True),
                "stock_name": st.column_config.TextColumn("stock_name"),
                "shares": st.column_config.NumberColumn("shares", min_value=1, step=1, required=True),
                "cost_price": st.column_config.NumberColumn("cost_price", min_value=0.01, step=0.01, required=True),
                "stop_loss": st.column_config.NumberColumn("stop_loss", min_value=0.0, step=0.01),
                "take_profit": st.column_config.NumberColumn("take_profit", min_value=0.0, step=0.01),
                "notes": st.column_config.TextColumn("notes"),
            },
            key="portfolio_import_editor",
        )

        act1, act2 = st.columns(2)
        with act1:
            if st.button("確認並覆寫至資料庫", type="primary", use_container_width=True):
                clean_df, errors = validate_holdings_df(pd.DataFrame(editable_df))
                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    replace_holdings(clean_df)
                    st.success(f"已覆寫資料庫，共儲存 {len(clean_df)} 筆持股。")
                    st.session_state.pop("portfolio_import_df", None)
                    st.session_state.pop("portfolio_import_meta", None)
                    st.rerun()
        with act2:
            if st.button("確認並附加至資料庫", use_container_width=True):
                clean_df, errors = validate_holdings_df(pd.DataFrame(editable_df))
                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    append_holdings(clean_df)
                    st.success(f"已附加至資料庫，共新增 {len(clean_df)} 筆持股。")
                    st.session_state.pop("portfolio_import_df", None)
                    st.session_state.pop("portfolio_import_meta", None)
                    st.rerun()

    st.markdown("---")
    st.subheader("現有持股")

    holdings = load_holdings()
    if not holdings:
        st.info("尚無持股紀錄")
    else:
        holdings_df = pd.DataFrame(holdings)[
            ["id", "stock_id", "stock_name", "shares", "cost_price", "stop_loss", "take_profit", "notes"]
        ].copy()

        st.caption("可直接修改股數、成本價、停損價、停利價與備註；股票代碼與名稱固定不允許編輯。")

        edited_holdings = st.data_editor(
            holdings_df,
            use_container_width=True,
            hide_index=True,
            disabled=["id", "stock_id", "stock_name"],
            column_config={
                "id": st.column_config.TextColumn("id", width="small"),
                "stock_id": st.column_config.TextColumn("stock_id"),
                "stock_name": st.column_config.TextColumn("stock_name"),
                "shares": st.column_config.NumberColumn("shares", min_value=1, step=1, required=True),
                "cost_price": st.column_config.NumberColumn("cost_price", min_value=0.01, step=0.01, required=True),
                "stop_loss": st.column_config.NumberColumn("stop_loss", min_value=0.0, step=0.01),
                "take_profit": st.column_config.NumberColumn("take_profit", min_value=0.0, step=0.01),
                "notes": st.column_config.TextColumn("notes"),
            },
            key="current_holdings_editor",
        )

        save_col, _ = st.columns([2, 5])
        with save_col:
            if st.button("儲存目前持股修改", type="primary", use_container_width=True):
                edited_df = pd.DataFrame(edited_holdings).copy()
                edited_df["shares"] = pd.to_numeric(edited_df["shares"], errors="coerce")
                edited_df["cost_price"] = pd.to_numeric(edited_df["cost_price"], errors="coerce")
                edited_df["stop_loss"] = pd.to_numeric(edited_df["stop_loss"], errors="coerce")
                edited_df["take_profit"] = pd.to_numeric(edited_df["take_profit"], errors="coerce")
                edited_df["notes"] = edited_df["notes"].fillna("").astype(str)

                invalid = edited_df[
                    edited_df["shares"].isna()
                    | (edited_df["shares"] <= 0)
                    | edited_df["cost_price"].isna()
                    | (edited_df["cost_price"] <= 0)
                ]
                if not invalid.empty:
                    bad_ids = "、".join(invalid["stock_id"].astype(str).tolist()[:5])
                    st.error(f"以下持股的股數或成本價不合法：{bad_ids}")
                else:
                    for row in edited_df.to_dict("records"):
                        update_holding(
                            holding_id=int(row["id"]),
                            shares=int(row["shares"]),
                            cost_price=float(row["cost_price"]),
                            stop_loss=float(row["stop_loss"]) if pd.notna(row["stop_loss"]) and row["stop_loss"] > 0 else None,
                            take_profit=float(row["take_profit"]) if pd.notna(row["take_profit"]) and row["take_profit"] > 0 else None,
                            notes=row["notes"],
                        )
                    st.success("持股資料已更新")
                    st.rerun()

        st.markdown("---")
        st.markdown("#### 刪除持股")
        for h in holdings:
            col1, col2 = st.columns([6, 2])
            with col1:
                st.write(f"{h['stock_id']} {h['stock_name']} | {h['shares']} 股 | 成本 {h['cost_price']} 元")
            with col2:
                if st.button("刪除", key=f"del_{h['id']}", type="secondary", use_container_width=True):
                    delete_holding(h["id"])
                    st.rerun()
