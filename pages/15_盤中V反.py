"""
盤中搶 V 型反轉頁面

  1. 服務狀態：scheduler + 今日推播計數
  2. 今日 watchlist：today_all()（含已預警 / 已收盤確認）
  3. 手動工具：立即重建 / 立即盤中檢查 / 立即收盤確認
  4. 推播歷史：v_reversal_alert* / v_reversal_close_confirm / v_reversal_watchlist_built
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from db.database import init_db
from db import v_reversal_watchlist as wl
from db.event_log import query_events

st.set_page_config(page_title="盤中搶V反轉", page_icon="⚡", layout="wide")
init_db()

st.title("⚡ 盤中搶 V 型反轉監控")
st.caption(
    "每日 08:40 掃全市場找「急跌→低檔爆量→止跌K」成形、KD 低檔的候選；9:00–13:30 每分鐘檢查"
    "『過止跌K高 + 紅K + KD 低檔金叉 + 流動性』發盤中預警，13:15 併入統整，13:40 用定案收盤發收盤確認。"
    "　⚠ 逆勢抄底、僅供觀察。"
)
st.markdown("---")

# ── 服務狀態 ──────────────────────────────────────────────
st.subheader("⚙ 服務狀態")
_pid_file = Path(__file__).resolve().parents[1] / "runtime" / "scheduler.pid"
_running = False
if _pid_file.exists():
    try:
        import psutil
        _proc = psutil.Process(int(_pid_file.read_text().strip()))
        _running = _proc.is_running() and _proc.status() != psutil.STATUS_ZOMBIE
    except Exception:
        _running = False

today_iso = date.today().isoformat()
alerts_today = query_events(event_type="v_reversal_alert", date_from=today_iso, date_to=today_iso, limit=500)
confirms_today = query_events(event_type="v_reversal_close_confirm", date_from=today_iso, date_to=today_iso, limit=500)
items_today = wl.today_all()

c1, c2, c3, c4 = st.columns(4)
c1.metric("srock scheduler", "🟢 執行中" if _running else "🔴 未啟動")
c2.metric("今日盤中預警", len(alerts_today))
c3.metric("今日收盤確認", len(confirms_today))
c4.metric("今日候選總數", len(items_today))
if not _running:
    st.warning("srock scheduler 未執行，盤中搶V反轉不會自動跑。請執行 `srock up` 或 `srock start scheduler`。")

st.markdown("---")

# ── 今日候選 ──────────────────────────────────────────────
st.subheader("📋 今日候選清單")
items = wl.today_all()
if not items:
    st.info(
        "今日尚無候選。可能：(1) 08:40 尚未跑 (2) 非交易日 (3) 全市場無符合 — 可用下方「立即重建」跑一次。"
    )
else:
    def _fd(v):
        return v.strftime("%m-%d") if hasattr(v, "strftime") else (str(v)[5:10] if v else "?")

    rows = []
    for it in items:
        sharp = []
        if it.get("drop_pct") is not None:
            sharp.append(f"{it['drop_pct']:.0f}%")
        if it.get("sharp_by_days") and it.get("down_days") is not None:
            sharp.append(f"跌{it['down_days']}天")
        rows.append({
            "預警": "✅" if it.get("alerted_today") else "⏳",
            "收盤確認": "✅" if it.get("confirmed_today") else "—",
            "代號": it["stock_id"],
            "名稱": it.get("stock_name") or "",
            "產業": it.get("industry") or "",
            "急跌": "、".join(sharp) or "—",
            "爆量": f"{it['spike_vol_ratio']:.1f}×({_fd(it.get('spike_date'))})" if it.get("spike_vol_ratio") else "—",
            "止跌K": ("紅K" if it.get("stabilize_is_red") else ("十字" if it.get("stabilize_is_doji") else "?")) + f"({_fd(it.get('stabilize_date'))})",
            "觸發價": round(float(it["trigger_price"]), 2) if it.get("trigger_price") is not None else None,
            "K": round(float(it["kd_k"]), 1) if it.get("kd_k") is not None else None,
            "D": round(float(it["kd_d"]), 1) if it.get("kd_d") is not None else None,
            "均量(張)": round(float(it["vol_ma5"]), 0) if it.get("vol_ma5") is not None else None,
            "昨收": round(float(it["last_close"]), 2) if it.get("last_close") is not None else None,
            "距觸發%": round(float(it["distance_to_trigger_pct"]), 2) if it.get("distance_to_trigger_pct") is not None else None,
        })
    df = pd.DataFrame(rows)
    cm1, cm2, cm3 = st.columns(3)
    cm1.metric("候選總數", len(df))
    cm2.metric("已預警", int((df["預警"] == "✅").sum()))
    cm3.metric("流動性達標(≥5000張)", int((df["均量(張)"].fillna(0) >= 5000).sum()))
    st.dataframe(df, use_container_width=True, height=460)
    st.caption("距觸發%：負值=現價仍在觸發價下方（等突破）；接近 0 或正值=已到/已突破。均量 < 5000 張者盤中會被流動性 gate 濾掉。")

st.markdown("---")

# ── 手動工具 ──────────────────────────────────────────────
st.subheader("🛠 手動工具")
mc1, mc2, mc3 = st.columns(3)
with mc1:
    st.markdown("**重建今日候選**")
    st.caption("全市場掃描，約數十秒～數分鐘。改參數後想立即驗證時用。")
    if st.button("立即重建", use_container_width=True):
        with st.spinner("建構中…"):
            try:
                from modules.v_reversal_watchlist_builder import build_watchlist
                stats = build_watchlist()
                st.success(f"scanned={stats.get('scanned',0)} hits={stats.get('hits',0)} written={stats.get('written',0)} errors={stats.get('errors',0)}")
                st.json(stats)
            except Exception as exc:
                st.error(f"重建失敗：{exc}")
with mc2:
    st.markdown("**立即盤中檢查**")
    st.caption("對未預警候選跑一次過高確認（需 Shioaji 登入）。")
    if st.button("立即檢查", use_container_width=True):
        with st.spinner("檢查中…"):
            try:
                from modules.intraday_v_reversal import run_v_reversal_check
                st.success(f"本輪預警 {run_v_reversal_check()} 則")
            except Exception as exc:
                st.error(f"檢查失敗：{exc}")
with mc3:
    st.markdown("**立即收盤確認**")
    st.caption("用定案收盤重驗發收盤確認（盤後 13:35 之後才有意義）。")
    if st.button("立即確認", use_container_width=True):
        with st.spinner("確認中…"):
            try:
                from modules.intraday_v_reversal import run_v_reversal_close_confirm
                st.success(f"本輪收盤確認 {run_v_reversal_close_confirm()} 則")
            except Exception as exc:
                st.error(f"確認失敗：{exc}")

st.markdown("---")

# ── 推播歷史 ──────────────────────────────────────────────
st.subheader("📜 推播歷史（近 14 天）")
d_to = date.today()
d_from = d_to - timedelta(days=14)
ev_alert = query_events(event_type="v_reversal_alert", date_from=d_from.isoformat(), date_to=d_to.isoformat(), limit=500)
ev_conf = query_events(event_type="v_reversal_close_confirm", date_from=d_from.isoformat(), date_to=d_to.isoformat(), limit=500)
ev_built = query_events(event_type="v_reversal_watchlist_built", date_from=d_from.isoformat(), date_to=d_to.isoformat(), limit=50)

tab_a, tab_b = st.tabs(["預警 / 收盤確認", "watchlist 建構"])
with tab_a:
    rows_a = []
    for e in ev_alert + ev_conf:
        try:
            p = json.loads(e.get("payload_json") or "{}")
        except Exception:
            p = {}
        rows_a.append({
            "時間": e["created_at"],
            "類型": "盤中預警" if e["event_type"] == "v_reversal_alert" else "收盤確認",
            "代號": e.get("stock_id") or "",
            "名稱": e.get("stock_name") or "",
            "價": p.get("price") or p.get("close"),
            "觸發": p.get("trigger"),
            "K": p.get("kd_k"), "D": p.get("kd_d"),
        })
    if rows_a:
        rows_a.sort(key=lambda r: r["時間"], reverse=True)
        st.dataframe(pd.DataFrame(rows_a), use_container_width=True, height=360)
    else:
        st.info("近 14 天無推播紀錄。")
with tab_b:
    rows_b = []
    for e in ev_built:
        try:
            p = json.loads(e.get("payload_json") or "{}")
        except Exception:
            p = {}
        rows_b.append({"時間": e["created_at"], "scanned": p.get("scanned"), "hits": p.get("hits"),
                       "written": p.get("written"), "skipped_far": p.get("skipped_far"),
                       "skipped_broken_far": p.get("skipped_broken_far"), "errors": p.get("errors")})
    if rows_b:
        st.dataframe(pd.DataFrame(rows_b), use_container_width=True, height=300)
    else:
        st.info("近 14 天無建構紀錄。")
