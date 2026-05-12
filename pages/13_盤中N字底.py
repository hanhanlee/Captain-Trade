"""
盤中 N 字底頁面

組件：
  1. 服務狀態：盤中排程器 + N 字底檢查最近一次結果（共用 portfolio 監控的分鐘輪）
  2. 今日 watchlist：display today_all()（含已推播）→ 表格 + 結構欄位
  3. 手動工具：立即重建 watchlist / 立即跑一次盤中檢查
  4. 突破警示歷史：從 event_log 撈 n_pattern_breakout_alert*
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from db.database import init_db
from db import n_pattern_watchlist as wl
from db.event_log import query_events

st.set_page_config(page_title="盤中 N 字底", page_icon="🎯", layout="wide")
init_db()

st.title("🎯 盤中 N 字底突破監控")
st.caption(
    "每日 08:00 自動建構科技股 N 字底候選清單，9:00–13:30 每分鐘檢查 D > B 突破並推播 Telegram。"
    "本頁可檢視當日候選與最近突破紀錄，並手動觸發重建 / 即時檢查。"
)
st.markdown("---")

# ── 服務狀態 ──────────────────────────────────────────────────────────────────
# 盤中 N 字底突破偵測現由 srock scheduler 進程（scheduler/jobs.py 的 job_intraday_monitor）
# 負責；本頁從 scheduler.pid + event_log 觀察其狀態。
st.subheader("⚙ 服務狀態")

from pathlib import Path
import os

_scheduler_pid_file = Path(__file__).resolve().parents[1] / "runtime" / "scheduler.pid"
_scheduler_running = False
if _scheduler_pid_file.exists():
    try:
        import psutil
        _pid = int(_scheduler_pid_file.read_text().strip())
        _proc = psutil.Process(_pid)
        _scheduler_running = _proc.is_running() and _proc.status() != psutil.STATUS_ZOMBIE
    except Exception:
        _scheduler_running = False

today_iso = date.today().isoformat()
events_today = query_events(
    event_type="n_pattern_breakout_alert",
    date_from=today_iso,
    date_to=today_iso,
    limit=500,
)
items_today = wl.today_all()

c1, c2, c3, c4 = st.columns(4)
c1.metric("srock scheduler", "🟢 執行中" if _scheduler_running else "🔴 未啟動")
c2.metric("今日已推播", len(events_today))
c3.metric(
    "最近一次突破",
    events_today[0]["created_at"][11:19] if events_today else "—",
)
c4.metric("今日 watchlist 總數", len(items_today))
if not _scheduler_running:
    st.warning(
        "srock scheduler 未執行，盤中 N 字底突破偵測不會自動跑。"
        "請於終端機執行 `srock up` 或 `srock start scheduler`。"
    )

st.markdown("---")

# ── 今日 watchlist ────────────────────────────────────────────────────────────
st.subheader("📋 今日候選清單")

items = wl.today_all()
if not items:
    st.info(
        "今日尚無候選紀錄。\n\n"
        "可能原因：(1) 排程器尚未在 08:00 跑過 (2) 今日非交易日 (3) 全市場無符合條件之 N 字底 — "
        "可使用下方「手動重建」按鈕立即跑一次。"
    )
else:
    rows = []
    for it in items:
        a_d = it["a_date"].strftime("%m-%d") if it.get("a_date") else "?"
        b_d = it["b_date"].strftime("%m-%d") if it.get("b_date") else "?"
        c_d = it["c_date"].strftime("%m-%d") if it.get("c_date") else "?"

        def _tick(v):
            if v is True:
                return "✓"
            if v is False:
                return "✗"
            return "—"

        dist = it.get("distance_to_b_pct")
        rows.append({
            "推播": "✅" if it.get("alerted_today") else "⏳",
            "代號": it["stock_id"],
            "名稱": it.get("stock_name") or "",
            "產業": it.get("industry") or "",
            "A": f"{it['a_price']:.2f}({a_d})" if it.get("a_price") else "?",
            "B": f"{it['b_price']:.2f}({b_d})" if it.get("b_price") else "?",
            "C": f"{it['c_price']:.2f}({c_d})" if it.get("c_price") else "?",
            "B 漲幅%": round(float(it["b_rise_pct"]), 1) if it.get("b_rise_pct") is not None else None,
            "C 回測%": round(float(it["c_retrace_pct"]), 1) if it.get("c_retrace_pct") is not None else None,
            "AB 量增": _tick(it.get("vol_a_to_b_increase")),
            "BC 量縮": _tick(it.get("vol_b_to_c_decrease")),
            "昨收": round(float(it["last_close"]), 2) if it.get("last_close") is not None else None,
            "距 B%": round(float(dist), 2) if dist is not None else None,
        })

    df = pd.DataFrame(rows)

    total = len(df)
    alerted = int((df["推播"] == "✅").sum())
    near = int((df["距 B%"].fillna(99) <= 2.0).sum())
    cm1, cm2, cm3 = st.columns(3)
    cm1.metric("候選總數", total)
    cm2.metric("已推播", alerted)
    cm3.metric("距 B 在 2% 以內", near)

    st.dataframe(df, use_container_width=True, height=460)

st.markdown("---")

# ── 手動工具 ──────────────────────────────────────────────────────────────────
st.subheader("🛠 手動工具")
mc1, mc2 = st.columns(2)

with mc1:
    st.markdown("**重建今日候選清單**")
    st.caption(
        "立即跑一次 build_watchlist（拉日 K → detector → actionable 過濾 → 寫入 DB），"
        "適用於排程錯過或想立即驗證新參數時。"
    )
    if st.button("立即重建", key="rebuild_watchlist", use_container_width=True):
        with st.spinner("建構中…可能需要 30 秒～數分鐘（依快取狀態）"):
            try:
                from modules.n_pattern_watchlist_builder import build_watchlist
                stats = build_watchlist()
                st.success(
                    f"完成：scanned={stats.get('scanned', 0)}　"
                    f"hits={stats.get('hits', 0)}　"
                    f"written={stats.get('written', 0)}　"
                    f"errors={stats.get('errors', 0)}"
                )
                with st.expander("詳細統計"):
                    st.json(stats)
                st.info("重新整理頁面即可看到新清單。")
            except Exception as exc:
                st.error(f"重建失敗：{exc}")

with mc2:
    st.markdown("**立即執行盤中突破檢查**")
    st.caption(
        "對今日 watchlist 中尚未推播的候選跑一次 Shioaji 報價 + D>B 判斷。"
        "Shioaji 未登入時整輪會安靜略過。"
    )
    if st.button("立即檢查", key="run_n_pattern_now", use_container_width=True):
        with st.spinner("檢查中…"):
            try:
                from modules.intraday_n_pattern import run_n_pattern_check
                sent = run_n_pattern_check()
                st.success(f"完成：本輪推播 {sent} 則")
            except Exception as exc:
                st.error(f"檢查失敗：{exc}")

st.markdown("---")

# ── 突破警示歷史 ──────────────────────────────────────────────────────────────
st.subheader("📜 突破警示歷史（近 14 天）")

date_to = date.today()
date_from = date_to - timedelta(days=14)

events_alert = query_events(
    event_type="n_pattern_breakout_alert",
    date_from=date_from.isoformat(),
    date_to=date_to.isoformat(),
    limit=500,
)
events_failed = query_events(
    event_type="n_pattern_breakout_alert_failed",
    date_from=date_from.isoformat(),
    date_to=date_to.isoformat(),
    limit=500,
)
events_built = query_events(
    event_type="n_pattern_watchlist_built",
    date_from=date_from.isoformat(),
    date_to=date_to.isoformat(),
    limit=50,
)

tab_alert, tab_built = st.tabs(["突破推播", "watchlist 建構"])

with tab_alert:
    rows_a = []
    for e in events_alert + events_failed:
        try:
            payload = json.loads(e.get("payload_json") or "{}")
        except Exception:
            payload = {}
        rows_a.append({
            "時間": e["created_at"],
            "結果": "✅" if e["event_type"] == "n_pattern_breakout_alert" else "❌",
            "代號": e.get("stock_id") or "",
            "名稱": e.get("stock_name") or "",
            "現價": payload.get("price"),
            "B": payload.get("b_price"),
            "今日量": payload.get("total_volume"),
            "BC 日均量": payload.get("avg_volume_b_to_c"),
            "摘要": e.get("summary") or "",
        })
    if rows_a:
        rows_a.sort(key=lambda r: r["時間"], reverse=True)
        st.dataframe(pd.DataFrame(rows_a), use_container_width=True, height=380)
    else:
        st.info("近 14 天無突破推播紀錄。")

with tab_built:
    rows_b = []
    for e in events_built:
        try:
            payload = json.loads(e.get("payload_json") or "{}")
        except Exception:
            payload = {}
        rows_b.append({
            "時間": e["created_at"],
            "scanned": payload.get("scanned"),
            "hits": payload.get("hits"),
            "written": payload.get("written"),
            "skipped_old_c": payload.get("skipped_old_c"),
            "skipped_far": payload.get("skipped_far"),
            "skipped_already_broken": payload.get("skipped_already_broken"),
            "errors": payload.get("errors"),
        })
    if rows_b:
        st.dataframe(pd.DataFrame(rows_b), use_container_width=True, height=300)
    else:
        st.info("近 14 天無 watchlist 建構紀錄（檢查 scheduler/jobs.py 是否啟動）。")
