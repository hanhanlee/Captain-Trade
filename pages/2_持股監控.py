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

from data.data_source import DataSourceManager, FALLBACK_WARNING
from modules.portfolio import run_portfolio_check, AlertLevel, StockAlert
from modules.portfolio_io import (
    STANDARD_COLUMNS,
    holdings_to_export_df,
    parse_holdings_csv,
    validate_holdings_df,
)
from db.price_cache import load_prices
from db.database import get_session, init_db
from db.models import Portfolio
from db.settings import (
    get_intraday_monitor_scheduler_enabled,
    is_market_closed,
    set_intraday_monitor_scheduler_enabled,
)
from notifications.line_notify import send_multicast

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
def _fmt_price(value) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "-"


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
                "intraday_monitor": bool(r.intraday_monitor) if r.intraday_monitor is not None else False,
            }
            for r in rows
        ]


def _intraday_monitor_enabled_count() -> int:
    with get_session() as sess:
        return (
            sess.query(Portfolio)
            .filter(Portfolio.intraday_monitor == True)  # noqa: E712
            .count()
        )


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


def update_holding(holding_id: int, shares, cost_price, stop_loss, take_profit, notes,
                   intraday_monitor: bool = False):
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
            if hasattr(row, "intraday_monitor"):
                row.intraday_monitor = bool(intraday_monitor)
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


def collect_premium_portfolio_alerts(stats_list: list[dict], price_data: dict) -> tuple[list[StockAlert], dict[str, list[StockAlert]]]:
    """Read cache-only Premium signals for current holdings and convert them to alerts."""
    try:
        from data.finmind_client import get_cached_holding_shares, get_cached_risk_flags
        from db.broker_cache import load_broker_main_force
    except Exception:
        return [], {}

    premium_alerts: list[StockAlert] = []
    by_stock: dict[str, list[StockAlert]] = {}

    def _add_alert(stat: dict, level: str, reason: str):
        alert = StockAlert(
            stock_id=stat["stock_id"],
            stock_name=stat.get("stock_name", ""),
            level=level,
            reason=f"[Premium] {reason}",
            current_price=float(stat.get("close") or 0),
            cost_price=float(stat.get("cost_price") or 0),
            pnl_pct=float(stat.get("pnl_pct") or 0),
        )
        premium_alerts.append(alert)
        by_stock.setdefault(stat["stock_id"], []).append(alert)

    def _active_official_flags(risk_df: pd.DataFrame, latest_date: str) -> list[tuple[str, dict]]:
        if risk_df is None or risk_df.empty or "flag_type" not in risk_df.columns:
            return []
        latest_ts = pd.Timestamp(latest_date)
        active: list[tuple[str, dict]] = []
        for _, row in risk_df.iterrows():
            flag_type = str(row.get("flag_type", "") or "")
            if flag_type == "price_limit":
                continue
            detail = row.get("detail") or {}
            if not isinstance(detail, dict):
                detail = {}

            if flag_type == "disposition":
                start = pd.to_datetime(detail.get("period_start") or row.get("date"), errors="coerce")
                end = pd.to_datetime(detail.get("period_end") or row.get("date"), errors="coerce")
                if pd.notna(start) and pd.notna(end) and start <= latest_ts <= end:
                    active.append((flag_type, detail))
            elif flag_type == "suspended":
                start = pd.to_datetime(row.get("date"), errors="coerce")
                end = pd.to_datetime(detail.get("resumption_date") or row.get("date"), errors="coerce")
                if pd.notna(start) and pd.notna(end) and start <= latest_ts <= end:
                    active.append((flag_type, detail))
            elif flag_type == "treasury_shares":
                start = pd.to_datetime(
                    detail.get("plan_buyback_start_date") or detail.get("start_date") or row.get("date"),
                    errors="coerce",
                )
                end = pd.to_datetime(
                    detail.get("plan_buyback_end_date") or detail.get("end_date") or row.get("date"),
                    errors="coerce",
                )
                if pd.notna(start) and pd.notna(end) and start <= latest_ts <= end:
                    active.append((flag_type, detail))
            else:
                active.append((flag_type, detail))
        return active

    for stat in stats_list:
        sid = stat["stock_id"]
        df = price_data.get(sid)
        if df is None or df.empty or "date" not in df.columns:
            continue

        dates = pd.to_datetime(df["date"], errors="coerce").dropna()
        if dates.empty:
            continue
        latest_date = dates.max().date().isoformat()
        lookback_start = (pd.Timestamp(latest_date) - pd.Timedelta(days=180)).date().isoformat()
        risk_lookback_start = (pd.Timestamp(latest_date) - pd.Timedelta(days=60)).date().isoformat()

        try:
            risk_df = get_cached_risk_flags(stock_id=sid, start_date=risk_lookback_start, end_date=latest_date)
        except Exception:
            risk_df = pd.DataFrame()
        _FLAG_LABELS = {
            "disposition": "官方處置股",
            "suspended": "官方停止買賣",
            "shareholding_transfer": "內部人申報轉讓",
            "attention": "官方注意股",
            "treasury_shares": "實施庫藏股",
        }
        for flag_type, detail in _active_official_flags(risk_df, latest_date):
            if flag_type in _FLAG_LABELS:
                level = (
                    AlertLevel.DANGER if flag_type == "suspended"
                    else AlertLevel.INFO if flag_type == "treasury_shares"
                    else AlertLevel.WARNING
                )
                suffix = ""
                if flag_type == "disposition":
                    suffix = f"（{detail.get('measure') or detail.get('condition') or '處置期間內'}）"
                elif flag_type == "shareholding_transfer":
                    shares = detail.get("target_transfer_shares")
                    method = detail.get("transfer_methods") or detail.get("transfer_method")
                    suffix = f"（{method or '申報轉讓'}"
                    if shares not in (None, ""):
                        suffix += f"，{shares} 張"
                    suffix += "）"
                elif flag_type == "attention":
                    suffix = f"（{detail.get('reason') or '注意交易資訊'}）"
                elif flag_type == "treasury_shares":
                    shares = detail.get("plan_buyback_shares")
                    suffix = f"（計畫買回 {shares} 張）" if shares not in (None, "") else "（庫藏股執行期間）"
                _add_alert(stat, level, f"{_FLAG_LABELS[flag_type]}{suffix}")
            else:
                _add_alert(stat, AlertLevel.WARNING, f"官方風險旗標：{flag_type}")

        try:
            holding_df = get_cached_holding_shares(stock_id=sid, start_date=lookback_start, end_date=latest_date)
        except Exception:
            holding_df = pd.DataFrame()
        if holding_df is not None and not holding_df.empty:
            hdf = holding_df.copy()
            hdf["date"] = pd.to_datetime(hdf["date"], errors="coerce")
            hdf = hdf.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            if len(hdf) >= 2:
                latest = hdf.iloc[-1]
                prev = hdf.iloc[-2]

                def _delta(col: str):
                    if pd.isna(latest.get(col)) or pd.isna(prev.get(col)):
                        return None
                    return float(latest[col]) - float(prev[col])

                d400 = _delta("above_400_pct")
                d1000 = _delta("above_1000_pct")
                d10 = _delta("below_10_pct")
                if d400 is not None and d400 < 0:
                    _add_alert(stat, AlertLevel.WARNING, f"大戶(400張+)比例下降 {d400:+.2f}pp")
                if d1000 is not None and d1000 < 0:
                    _add_alert(stat, AlertLevel.WARNING, f"大戶(1000張+)比例下降 {d1000:+.2f}pp")
                if d10 is not None and d10 > 0:
                    _add_alert(stat, AlertLevel.WARNING, f"散戶(10張以下)比例上升 {d10:+.2f}pp")

        try:
            broker_dates = [
                d.date().isoformat()
                for d in pd.to_datetime(df["date"], errors="coerce").dropna().tail(30)
            ]
            broker_df = load_broker_main_force(sid, broker_dates)
        except Exception:
            broker_df = pd.DataFrame()
        if broker_df is not None and not broker_df.empty:
            bdf = broker_df.copy().sort_values("date").reset_index(drop=True)
            latest_broker = bdf.iloc[-1]
            net = pd.to_numeric(latest_broker.get("net"), errors="coerce")
            reversal = pd.to_numeric(latest_broker.get("reversal_flag"), errors="coerce")
            if pd.notna(reversal) and bool(reversal):
                _add_alert(stat, AlertLevel.WARNING, "主力反手訊號")
            elif pd.notna(net) and net < 0:
                _add_alert(stat, AlertLevel.WARNING, f"主力淨賣超 {abs(float(net)):,.0f} 張")

    priority = {AlertLevel.DANGER: 0, AlertLevel.WARNING: 1, AlertLevel.INFO: 2}
    premium_alerts.sort(key=lambda a: priority.get(a.level, 9))
    return premium_alerts, by_stock


# ── Premium 警示重算 ─────────────────────────────────────────
def _recompute_premium_alerts(
    stats_list: list[dict],
    all_alerts: list[StockAlert],
    price_data: dict,
) -> tuple[list[dict], list[StockAlert], list[StockAlert]]:
    """Refresh Premium alerts from local cache without touching price/API fetches."""
    base_alerts = [
        a for a in all_alerts
        if not str(getattr(a, "reason", "")).startswith("[Premium]")
    ]
    for stat in stats_list:
        stat["alerts"] = [
            a for a in stat.get("alerts", [])
            if not str(getattr(a, "reason", "")).startswith("[Premium]")
        ]

    premium_alerts, premium_by_stock = collect_premium_portfolio_alerts(stats_list, price_data)
    if premium_alerts:
        for stat in stats_list:
            stat["alerts"].extend(premium_by_stock.get(stat["stock_id"], []))

    all_alerts = base_alerts + premium_alerts
    priority = {AlertLevel.DANGER: 0, AlertLevel.WARNING: 1, AlertLevel.INFO: 2}
    all_alerts.sort(key=lambda a: priority.get(a.level, 9))
    return stats_list, all_alerts, premium_alerts


def render_holding_chart(stock_id: str, df: pd.DataFrame, cost_price: float,
                          stop_loss: float = None, take_profit: float = None):
    from modules.indicators import sma, macd, bollinger_bands, rsi

    df = df.copy()
    df["ma5"] = sma(df["close"], 5)
    df["ma20"] = sma(df["close"], 20)
    bb_upper, bb_mid, bb_lower = bollinger_bands(df["close"], 20, 2.0)
    df["bb_upper"] = bb_upper
    df["bb_mid"] = bb_mid
    df["bb_lower"] = bb_lower
    df["bb_bandwidth"] = ((bb_upper - bb_lower) / bb_mid * 100).round(2)
    dif, dea, hist = macd(df["close"])
    df["macd"], df["macd_signal"], df["macd_hist"] = dif, dea, hist

    latest_bw = df["bb_bandwidth"].iloc[-1] if "bb_bandwidth" in df.columns else None
    bw_text = (
        f"BB帶寬 {latest_bw:.2f}%"
        if latest_bw is not None and pd.notna(latest_bw)
        else "BB帶寬資料不足"
    )

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

    for col, name in [("bb_upper", "BB上軌"), ("bb_mid", "BB中軌"), ("bb_lower", "BB下軌")]:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df[col], name=name,
            line=dict(
                color="rgba(52,152,219,0.45)" if col != "bb_mid" else "rgba(149,165,166,0.45)",
                width=0.9,
                dash="dot",
            ),
        ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=pd.concat([df["date"], df["date"].iloc[::-1]]),
        y=pd.concat([df["bb_upper"], df["bb_lower"].iloc[::-1]]),
        fill="toself",
        fillcolor="rgba(52,152,219,0.06)",
        line=dict(width=0),
        hoverinfo="skip",
        name="BB通道",
        showlegend=False,
    ), row=1, col=1)

    # 成本線
    fig.add_hline(y=cost_price, line_dash="dash", line_color="#9b59b6",
                  annotation_text=f"成本 {_fmt_price(cost_price)}", row=1, col=1)
    if stop_loss:
        fig.add_hline(y=stop_loss, line_dash="dot", line_color="#e74c3c",
                      annotation_text=f"停損 {_fmt_price(stop_loss)}", row=1, col=1)
    if take_profit:
        fig.add_hline(y=take_profit, line_dash="dot", line_color="#27ae60",
                      annotation_text=f"停利 {_fmt_price(take_profit)}", row=1, col=1)

    fig.add_trace(go.Scatter(x=df["date"], y=df["macd"], name="DIF",
                              line=dict(color="#e74c3c", width=1)), row=2, col=1)
    fig.add_trace(go.Scatter(x=df["date"], y=df["macd_signal"], name="DEA",
                              line=dict(color="#3498db", width=1)), row=2, col=1)
    hist_colors = ["#e74c3c" if v >= 0 else "#27ae60" for v in df["macd_hist"].fillna(0)]
    fig.add_trace(go.Bar(x=df["date"], y=df["macd_hist"], name="柱",
                          marker_color=hist_colors, showlegend=False), row=2, col=1)

    fig.update_layout(height=550, template="plotly_dark",
                      title=dict(text=bw_text, x=0.02, xanchor="left"),
                      xaxis_rangeslider_visible=False, margin=dict(t=50, b=10))
    st.plotly_chart(fig, use_container_width=True)


# ── 頁面主體 ─────────────────────────────────────────────────
st.title("💼 持股監控")

tab_monitor, tab_manage = st.tabs(["📊 即時監控", "✏️ 管理持股"])

# ══ Tab：即時監控 ═══════════════════════════════════════════
with tab_monitor:
    holdings = load_holdings()
    market_closed_mode = is_market_closed()
    scheduler_enabled = get_intraday_monitor_scheduler_enabled()
    monitored_count = _intraday_monitor_enabled_count()
    try:
        from scheduler.intraday_service import (
            run_once as run_intraday_scheduler_once,
            start_intraday_scheduler,
            status as intraday_scheduler_status,
            stop_intraday_scheduler,
        )
        if scheduler_enabled:
            start_intraday_scheduler()
        else:
            stop_intraday_scheduler()
        intraday_status = intraday_scheduler_status()
    except Exception as exc:
        run_intraday_scheduler_once = None
        start_intraday_scheduler = None
        stop_intraday_scheduler = None
        intraday_status = {
            "running": False,
            "last_error": str(exc),
            "last_run_at": None,
            "last_sent_count": 0,
            "next_run_time": None,
        }

    if not holdings:
        st.info("尚未新增任何持股。請前往「管理持股」頁籤新增。")
    else:
        st.markdown("#### 盤中即時監控排程")
        sched_cols = st.columns([1.2, 1.2, 1.4, 2.2])
        with sched_cols[0]:
            st.metric("排程器", "運行中" if intraday_status.get("running") else "未啟動")
        with sched_cols[1]:
            st.metric("監控持股", f"{monitored_count} 檔")
        with sched_cols[2]:
            last_run = intraday_status.get("last_run_at")
            last_run_text = last_run.strftime("%H:%M:%S") if hasattr(last_run, "strftime") else "尚未執行"
            st.metric("上次執行", last_run_text)
        with sched_cols[3]:
            last_error = intraday_status.get("last_error")
            if last_error:
                st.error(f"排程錯誤：{last_error}")
            elif intraday_status.get("running"):
                st.caption("每分鐘讀取勾選持股的 Sponsor 分K現價，觸及均線/停損/停利時推播 LINE。")
            else:
                st.caption("啟動後不需要另外開 `python scheduler/jobs.py` 視窗。")

        ctrl1, ctrl2, ctrl3 = st.columns([1, 1, 2])
        with ctrl1:
            if not scheduler_enabled:
                if st.button("啟動盤中監控排程", type="primary", use_container_width=True):
                    set_intraday_monitor_scheduler_enabled(True)
                    if start_intraday_scheduler:
                        start_intraday_scheduler()
                    st.rerun()
            else:
                if st.button("停止盤中監控排程", type="secondary", use_container_width=True):
                    set_intraday_monitor_scheduler_enabled(False)
                    if stop_intraday_scheduler:
                        stop_intraday_scheduler()
                    st.rerun()
        with ctrl2:
            if st.button("立即測試一次", use_container_width=True):
                if run_intraday_scheduler_once:
                    sent = run_intraday_scheduler_once()
                    st.success(f"已執行一次盤中監控，推播 {sent} 則。")
                else:
                    st.error("盤中監控排程器目前無法執行。")
        with ctrl3:
            if monitored_count == 0:
                st.warning("目前沒有任何持股勾選「🔔盤中監控」，排程器即使啟動也不會抓即時分K。")
            elif not scheduler_enabled:
                st.info("已有持股勾選盤中監控，但排程器尚未啟動。")

        st.markdown("---")

        if market_closed_mode:
            st.info(
                "🏖️ 休市模式啟用中：持股監控只讀取本機快取，不會連線更新報價。",
                icon=None,
            )
        col_refresh, col_premium, col_notify = st.columns([1.4, 1.2, 1])
        with col_refresh:
            clicked_refresh = st.button(
                "🔄 更新報價" if not market_closed_mode else "🔄 重新讀取快取",
                type="primary",
                use_container_width=True,
            )
        with col_premium:
            premium_recheck_btn = st.button(
                "重檢 Premium 風險",
                use_container_width=True,
                help="只讀本機 Premium cache，不抓報價、不打 FinMind API",
            )
        with col_notify:
            notify_btn = st.button("📲 推播警示到 LINE", use_container_width=True)

        # 按「更新報價」→ 進入待確認狀態
        if clicked_refresh and not market_closed_mode:
            st.session_state["refresh_pending"] = True

        # 休市模式直接執行，不需確認
        if clicked_refresh and market_closed_mode:
            st.session_state["do_refresh"] = True

        # 顯示確認對話框
        if st.session_state.get("refresh_pending"):
            from db.price_cache import get_cached_dates
            dates = [get_cached_dates(h["stock_id"]) for h in holdings]
            latest_dates = [d[1] for d in dates if d[1] is not None]
            if latest_dates:
                oldest = min(latest_dates)
                newest = max(latest_dates)
                cache_msg = f"資料庫目前最新報價：**{newest}**（最舊持股：{oldest}）"
            else:
                cache_msg = "資料庫目前**無任何快取資料**，將全部從網路下載。"
            st.info(f"ℹ️ {cache_msg}  \n確認要從網路更新報價？")
            c_ok, c_cancel = st.columns(2)
            with c_ok:
                if st.button("✅ 確認更新", type="primary", use_container_width=True):
                    st.session_state["refresh_pending"] = False
                    st.session_state["do_refresh"] = True
                    st.rerun()
            with c_cancel:
                if st.button("❌ 取消", use_container_width=True):
                    st.session_state["refresh_pending"] = False
                    st.rerun()

        refresh = st.session_state.pop("do_refresh", False)

        if refresh or "portfolio_stats" not in st.session_state:
            price_data = {}
            failed_ids = []
            dsm = None if market_closed_mode else DataSourceManager()
            prog = st.progress(0)
            for i, h in enumerate(holdings):
                prog.progress((i + 1) / len(holdings))
                try:
                    if market_closed_mode:
                        df = load_prices(h["stock_id"], start_date="2025-01-01")
                    else:
                        df = dsm.get_price(h["stock_id"], required_days=90)
                    if not df.empty:
                        price_data[h["stock_id"]] = df
                    else:
                        failed_ids.append(h["stock_id"])
                    if not market_closed_mode:
                        time.sleep(0.05)
                except Exception:
                    failed_ids.append(h["stock_id"])
            prog.empty()

            # 若本輪一筆都抓不到，不覆蓋既有結果，避免誤顯示成「無警示」
            if price_data:
                stats_list, all_alerts = run_portfolio_check(holdings, price_data)
                stats_list, all_alerts, premium_alerts = _recompute_premium_alerts(
                    stats_list,
                    all_alerts,
                    price_data,
                )
                st.session_state["portfolio_stats"] = stats_list
                st.session_state["portfolio_alerts"] = all_alerts
                st.session_state["portfolio_premium_alerts"] = premium_alerts
                st.session_state["portfolio_prices"] = price_data
                st.session_state["portfolio_failed_ids"] = failed_ids
                st.session_state["portfolio_fallback_mode"] = dsm.fallback_mode if dsm is not None else False
            else:
                st.session_state["portfolio_failed_ids"] = [h["stock_id"] for h in holdings]

        stats_list = st.session_state.get("portfolio_stats", [])
        all_alerts = st.session_state.get("portfolio_alerts", [])
        premium_alerts = st.session_state.get("portfolio_premium_alerts", [])
        price_data = st.session_state.get("portfolio_prices", {})
        failed_ids = st.session_state.get("portfolio_failed_ids", [])
        fallback_mode = st.session_state.get("portfolio_fallback_mode", False)

        # Premium 風險重檢只讀本機 cache，避免每次 rerun 都卡住頁面。
        if premium_recheck_btn and stats_list and price_data:
            stats_list, all_alerts, premium_alerts = _recompute_premium_alerts(
                stats_list,
                all_alerts,
                price_data,
            )
            st.session_state["portfolio_stats"] = stats_list
            st.session_state["portfolio_alerts"] = all_alerts
            st.session_state["portfolio_premium_alerts"] = premium_alerts
            st.toast("Premium 風險已從本機 cache 重新檢查")

        if fallback_mode:
            st.warning(FALLBACK_WARNING)
        if failed_ids:
            st.warning(
                "以下持股本輪無法取得最新報價，已略過或沿用先前結果："
                + "、".join(failed_ids[:10])
                + (" …" if len(failed_ids) > 10 else "")
            )
        if not stats_list and holdings:
            st.error("目前無法取得任何持股報價，因此無法判斷警示。請稍後再試或先檢查資料來源。")

        # 警示區塊
        danger_alerts = [a for a in all_alerts if a.level == AlertLevel.DANGER]
        warn_alerts = [a for a in all_alerts if a.level == AlertLevel.WARNING]

        if danger_alerts:
            for a in danger_alerts:
                st.error(f"🔴 **{a.stock_id} {a.stock_name}** — {a.reason}　現價 {a.current_price:.2f} 元　損益 {a.pnl_pct:+.1f}%")
        if warn_alerts:
            for a in warn_alerts:
                st.warning(f"🟡 **{a.stock_id} {a.stock_name}** — {a.reason}　現價 {a.current_price:.2f} 元　損益 {a.pnl_pct:+.1f}%")
        if not danger_alerts and not warn_alerts:
            st.success("✅ 所有持股目前無警示")

        if premium_alerts:
            with st.expander(f"Premium 風險摘要 ({len(premium_alerts)} 則)", expanded=False):
                for a in premium_alerts:
                    label = LEVEL_LABEL.get(a.level, "Premium")
                    st.write(f"{label} **{a.stock_id} {a.stock_name}** - {a.reason}")
                st.caption(
                    "已檢查：申報轉讓、注意股、官方處置股、官方停止買賣、實施庫藏股、"
                    "大戶持股分布、主力券商反手/賣超。"
                )
        else:
            with st.expander("Premium 風險檢查範圍", expanded=False):
                st.write("已檢查：申報轉讓、注意股、官方處置股、官方停止買賣、實施庫藏股。")
                st.write("已檢查：大戶持股分布、主力券商反手/賣超。")
                st.caption("每日漲跌停價 price_limit 只是參考價，不列為風險警示。")

        # LINE 推播
        if notify_btn:
            if not all_alerts:
                ok = send_multicast("💼 持股監控：所有持股目前無警示")
                st.toast("已群播：無警示" if ok else "LINE 群播失敗", icon="📲" if ok else "❌")
            else:
                lines = ["💼 持股監控警示"]
                for a in all_alerts[:8]:
                    emoji = "🔴" if a.level == AlertLevel.DANGER else "🟡"
                    lines.append(f"\n{emoji} {a.stock_id} {a.stock_name}")
                    lines.append(f"   {a.reason}")
                    lines.append(f"   現價 {a.current_price:.2f} 元  損益 {a.pnl_pct:+.1f}%")
                ok = send_multicast("\n".join(lines))
                st.toast("警示已群播到 LINE" if ok else "LINE 群播失敗", icon="📲" if ok else "❌")

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
                    "成本": round(float(s["cost_price"]), 2),
                    "現價": round(float(s["close"]), 2),
                    "損益(元)": s["pnl"],
                    "損益%": s["pnl_pct"],
                    "高點回撤%": s["drawdown_from_high"],
                    "MA20": round(float(s["ma20"]), 2) if s["ma20"] is not None else None,
                    "警示": "、".join(alert_labels) if alert_labels else "—",
                })

            df_display = pd.DataFrame(rows)

            def color_pnl(val):
                if isinstance(val, (int, float)):
                    return "color:#e74c3c" if val > 0 else ("color:#27ae60" if val < 0 else "")
                return ""

            styled = (
                df_display.style
                .applymap(color_pnl, subset=["損益(元)", "損益%", "高點回撤%"])
                .format({
                    "成本": "{:.2f}",
                    "現價": "{:.2f}",
                    "MA20": "{:.2f}",
                    "損益%": "{:+.2f}",
                    "高點回撤%": "{:+.2f}",
                }, na_rep="-")
            )
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
            ["id", "stock_id", "stock_name", "shares", "cost_price", "stop_loss", "take_profit",
             "notes", "intraday_monitor"]
        ].copy()

        st.caption("可直接修改欄位；🔔 盤中監控：勾選後每分鐘掃描現價，觸及 MA5/10/20 或停損停利時推播 LINE。")

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
                "intraday_monitor": st.column_config.CheckboxColumn("🔔盤中監控", default=False),
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
                edited_df["intraday_monitor"] = edited_df.get("intraday_monitor", False).fillna(False).astype(bool)

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
                            intraday_monitor=bool(row.get("intraday_monitor", False)),
                        )
                    st.success("持股資料已更新")
                    st.rerun()

        st.markdown("---")
        st.markdown("#### 刪除持股")
        for h in holdings:
            col1, col2 = st.columns([6, 2])
            with col1:
                st.write(f"{h['stock_id']} {h['stock_name']} | {h['shares']} 股 | 成本 {_fmt_price(h['cost_price'])} 元")
            with col2:
                if st.button("刪除", key=f"del_{h['id']}", type="secondary", use_container_width=True):
                    delete_holding(h["id"])
                    st.rerun()
