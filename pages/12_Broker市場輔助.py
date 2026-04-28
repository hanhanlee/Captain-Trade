"""
Broker 市場輔助
- Tab 1：API 狀態（登入/登出、連線、流量）
- Tab 2：商品檔查詢（漲跌停、參考價、可當沖、融資券）
- Tab 3：盤中即時監控（持股 + 盤中監控清單的即時快照）
"""
import streamlit as st
import pandas as pd
from datetime import datetime

from db.database import get_session, init_db
from db.models import Portfolio

st.set_page_config(page_title="Broker 市場輔助", page_icon="📡", layout="wide")
init_db()


def _adapter():
    from broker.shioaji_adapter import get_adapter
    return get_adapter()


# ── 共用格式化 ────────────────────────────────────────────────────────────────

def _fmt(v, fmt=".2f", fallback="—") -> str:
    try:
        return format(float(v), fmt) if v is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _pct_color(v) -> str:
    try:
        f = float(v)
        color = "red" if f < 0 else "green" if f > 0 else "gray"
        return f'<span style="color:{color}">{f:+.2f}%</span>'
    except (TypeError, ValueError):
        return "—"


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 1 — API 狀態
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_status():
    adapter = _adapter()
    logged_in = adapter.is_logged_in()

    col_login, col_logout = st.columns([1, 1])
    with col_login:
        if st.button("🔌 登入 Shioaji", disabled=logged_in, use_container_width=True):
            with st.spinner("登入中，等待商品檔下載（最多 10 秒）…"):
                ok = adapter.login()
            if ok:
                st.success("登入成功")
                st.rerun()
            else:
                st.error("登入失敗，請檢查 SINOTRADE_APIKEY / SINOTRADE_SECRETKEY")
    with col_logout:
        if st.button("🔴 登出", disabled=not logged_in, use_container_width=True):
            adapter.logout()
            st.info("已登出")
            st.rerun()

    st.divider()

    health = adapter.health_check()

    c1, c2, c3 = st.columns(3)
    c1.metric("連線狀態", "✅ 已連線" if health.get("logged_in") else "⚫ 未連線")
    c2.metric("商品檔", "✅ 就緒" if health.get("contracts_ready") else "⚠️ 未就緒")
    c3.metric("連線數", health.get("connections", "—"))

    if health.get("logged_in") and "bytes_used" in health:
        used_mb = health["bytes_used"] / 1024 / 1024
        limit_mb = health["bytes_limit"] / 1024 / 1024
        pct = health["usage_pct"]
        st.progress(min(pct / 100, 1.0), text=f"每日流量：{used_mb:.1f} MB / {limit_mb:.0f} MB（{pct:.1f}%）")
        if pct >= 70:
            st.warning(f"流量使用率 {pct:.1f}%，請注意上限")

    if not health.get("logged_in"):
        st.info("尚未登入。登入後可使用商品檔查詢與盤中即時監控。")


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 2 — 商品檔查詢
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_contract():
    adapter = _adapter()
    if not adapter.is_logged_in():
        st.info("請先在「API 狀態」頁登入 Shioaji。")
        return

    stock_id = st.text_input("股票代碼", placeholder="例如 2330、0050", max_chars=10).strip()
    fetch_snap = st.checkbox("同時取得即時報價", value=True)

    if not stock_id:
        return

    if st.button("查詢", use_container_width=False):
        info = adapter.get_contract_info(stock_id)
        if info is None:
            st.error(f"找不到商品：{stock_id}（請確認代碼正確，或商品檔尚未就緒）")
            return

        st.subheader(f"{info['code']} {info['name']}")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("參考價（昨收）", _fmt(info["reference"]))
        col2.metric("漲停價", _fmt(info["limit_up"]))
        col3.metric("跌停價", _fmt(info["limit_down"]))
        col4.metric("交易單位", _fmt(info["unit"], ".0f", "—") + " 股")

        col5, col6, col7 = st.columns(3)
        col5.metric("可當沖", info["day_trade"])
        col6.metric("融資餘額", _fmt(info["margin_trading_balance"], ".0f", "—"))
        col7.metric("融券餘額", _fmt(info["short_selling_balance"], ".0f", "—"))

        if info.get("update_date"):
            st.caption(f"商品檔更新日：{info['update_date']}")

        if fetch_snap:
            with st.spinner("取得即時報價…"):
                snap = adapter.get_snapshot(stock_id)
            if snap:
                st.divider()
                s1, s2, s3, s4, s5 = st.columns(5)
                s1.metric("即時價", _fmt(snap["last_price"]))
                s2.metric("今日漲跌", f"{_fmt(snap['change_price'])} ({_fmt(snap['change_rate'])}%)")
                s3.metric("今日高點", _fmt(snap["high"]))
                s4.metric("今日低點", _fmt(snap["low"]))
                s5.metric("總成交量", _fmt(snap["total_volume"], ".0f"))

                d1, d2 = st.columns(2)
                dist_up = snap.get("dist_to_limit_up_pct")
                dist_down = snap.get("dist_to_limit_down_pct")
                d1.metric(
                    "距漲停",
                    f"{dist_up:.1f}%" if dist_up is not None else "—",
                    delta="⚠️ 接近漲停" if (dist_up is not None and dist_up <= 3) else None,
                    delta_color="inverse",
                )
                d2.metric(
                    "距跌停",
                    f"{dist_down:.1f}%" if dist_down is not None else "—",
                    delta="⚠️ 接近跌停" if (dist_down is not None and dist_down <= 5) else None,
                    delta_color="inverse",
                )
                st.caption(f"報價時間：{snap.get('ts', '—')}")
            else:
                st.warning("無法取得即時報價（可能尚未開盤或代碼有誤）")


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 3 — 盤中即時監控
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_intraday():
    adapter = _adapter()
    if not adapter.is_logged_in():
        st.info("請先在「API 狀態」頁登入 Shioaji。")
        return

    # 取得有啟用盤中監控的持股
    with get_session() as sess:
        rows = sess.query(Portfolio).filter(Portfolio.intraday_monitor == True).all()  # noqa: E712
        monitored = [
            {"stock_id": r.stock_id, "stock_name": r.stock_name or "", "cost_price": r.cost_price,
             "stop_loss": r.stop_loss, "take_profit": r.take_profit}
            for r in rows
        ]
        all_holdings = [{"stock_id": r.stock_id, "stock_name": r.stock_name or ""} for r in
                        sess.query(Portfolio).all()]

    # 讓使用者選擇要查的標的範圍
    scope = st.radio(
        "查詢範圍",
        ["盤中監控中的持股", "全部持股", "手動輸入代碼"],
        horizontal=True,
    )

    if scope == "盤中監控中的持股":
        target_list = monitored
    elif scope == "全部持股":
        target_list = all_holdings
    else:
        custom = st.text_input("輸入代碼（逗號分隔）", placeholder="2330,2317,0050")
        target_list = [{"stock_id": s.strip(), "stock_name": ""} for s in custom.split(",") if s.strip()]

    if not target_list:
        st.info("沒有可監控的標的。請先在「持股監控」頁新增持股並啟用盤中監控。")
        return

    col_btn, col_time = st.columns([2, 8])
    with col_btn:
        do_fetch = st.button("🔄 取得即時報價", use_container_width=True)

    if not do_fetch:
        st.caption("點擊上方按鈕取得最新報價")
        return

    stock_ids = [str(h["stock_id"]) for h in target_list]
    id_to_name = {str(h["stock_id"]): h["stock_name"] for h in target_list}
    id_to_cost = {str(h.get("stock_id", "")): h.get("cost_price") for h in monitored}
    id_to_stop = {str(h.get("stock_id", "")): h.get("stop_loss") for h in monitored}

    with st.spinner(f"批次取得 {len(stock_ids)} 檔報價…"):
        snaps = adapter.get_snapshots(stock_ids)

    if not snaps:
        st.error("無法取得報價。請確認 Shioaji 已連線且商品檔已就緒。")
        return

    col_time.caption(f"更新時間：{datetime.now().strftime('%H:%M:%S')}，共 {len(snaps)} 檔")

    rows_data = []
    for sid in stock_ids:
        snap = snaps.get(sid)
        name = snap["stock_name"] if snap and snap.get("stock_name") else id_to_name.get(sid, "")
        if snap is None:
            rows_data.append({
                "代碼": sid, "名稱": name, "即時價": None, "漲跌%": None,
                "今高": None, "今低": None, "距漲停%": None, "距跌停%": None,
                "成本": _fmt(id_to_cost.get(sid)), "停損": _fmt(id_to_stop.get(sid)),
                "狀態": "無資料",
            })
            continue

        close = snap["last_price"]
        dist_up = snap.get("dist_to_limit_up_pct")
        dist_down = snap.get("dist_to_limit_down_pct")
        cost = id_to_cost.get(sid)
        pnl_pct = round((close - cost) / cost * 100, 2) if (cost and close) else None

        status = "正常"
        stop = id_to_stop.get(sid)
        if stop and close and close <= stop:
            status = "⚠️ 觸停損"
        elif dist_up is not None and dist_up <= 3:
            status = "🔺 近漲停"
        elif dist_down is not None and dist_down <= 5:
            status = "🔻 近跌停"

        rows_data.append({
            "代碼": sid,
            "名稱": name,
            "即時價": close,
            "漲跌%": snap.get("change_rate"),
            "今高": snap.get("high"),
            "今低": snap.get("low"),
            "距漲停%": dist_up,
            "距跌停%": dist_down,
            "成本": _fmt(cost),
            "未實現%": pnl_pct,
            "停損": _fmt(stop),
            "狀態": status,
        })

    df = pd.DataFrame(rows_data)

    # 數值欄位格式化
    for col in ["即時價", "今高", "今低"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: _fmt(v))
    for col in ["漲跌%", "距漲停%", "距跌停%", "未實現%"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: f"{float(v):.2f}%" if v is not None else "—")

    # 狀態高亮
    def _highlight(row):
        if "⚠️" in str(row.get("狀態", "")):
            return ["background-color: #fff0f0"] * len(row)
        if "🔺" in str(row.get("狀態", "")):
            return ["background-color: #fff8e6"] * len(row)
        if "🔻" in str(row.get("狀態", "")):
            return ["background-color: #fff0f0"] * len(row)
        return [""] * len(row)

    st.dataframe(
        df.style.apply(_highlight, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    # 警示摘要
    alerts_rows = df[df["狀態"] != "正常"]
    if not alerts_rows.empty:
        st.warning(f"共 {len(alerts_rows)} 檔需注意：{', '.join(alerts_rows['代碼'].tolist())}")


# ═══════════════════════════════════════════════════════════════════════════════
# 主畫面
# ═══════════════════════════════════════════════════════════════════════════════

st.title("📡 Broker 市場輔助")
st.caption("Shioaji / 永豐 API — 行情輔助（唯讀，不下單）")

tab1, tab2, tab3 = st.tabs(["🔌 API 狀態", "📋 商品檔查詢", "📊 盤中即時監控"])

with tab1:
    _tab_status()

with tab2:
    _tab_contract()

with tab3:
    _tab_intraday()
