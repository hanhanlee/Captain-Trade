"""
盤中 v5 狙擊手版頁面

組件：
  1. 服務狀態：scheduler + v5 突破檢查最近一次結果
  2. 今日 watchlist：display today_all()（含已推播）→ 表格 + 型態 + 來源（morning/late）
  3. 手動工具：立即重建 watchlist / 立即跑一次盤中檢查 / 立即跑一次 late qualifier
  4. 突破警示歷史：從 event_log 撈 v5_sniper_alert* + v5_late_qualifier_alert*
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from db.database import init_db
from db import v5_sniper_watchlist as wl
from db.event_log import query_events

st.set_page_config(page_title="盤中 v5 狙擊", page_icon="🎯", layout="wide")
init_db()

st.title("🎯 盤中 v5 狙擊手版突破監控")
st.caption(
    "**雙軌監控**：(1) 主軌道 — 每日 08:50 用昨日 EOD 建構科技股 v5 候選清單（型態 A：糾結突破 / "
    "型態 B：守月線回檔再上），9:00–13:30 每分鐘檢查突破目標價 + 量增並推播 Telegram；"
    "(2) **late qualifier 補抓軌道** — 09:15–13:25 每 5 分鐘對 ~300 檔科技股做盤中 V5 完整重判，"
    "補抓「晨間沒進 watchlist 但盤中才成形」的票，過熱濾網用「現價 ≤ 突破目標 ×1.03」（過前高 3% 內才追）。"
    "本頁可檢視當日候選（含 morning / late 來源欄）與最近突破紀錄。"
)
st.markdown("---")

# ── 服務狀態 ──────────────────────────────────────────────────────────────────
st.subheader("⚙ 服務狀態")

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
    event_type="v5_sniper_alert",
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
        "srock scheduler 未執行，盤中 v5 狙擊偵測不會自動跑。"
        "請於終端機執行 `srock up` 或 `srock start scheduler`。"
    )

st.markdown("---")

# ── 今日 watchlist ────────────────────────────────────────────────────────────
st.subheader("📋 今日候選清單")

items = wl.today_all()
if not items:
    st.info(
        "今日尚無候選紀錄。\n\n"
        "可能原因：(1) 排程器尚未在 08:50 跑過 (2) 今日非交易日 "
        "(3) 科技股無符合條件之 v5 型態（極正常，v5 設計上空倉率高）— "
        "可使用下方「手動重建」按鈕立即跑一次。"
    )
else:
    rows = []
    for it in items:
        target = it.get("breakout_target")
        prev_close = it.get("prev_close")
        dist_to_target = None
        if target and prev_close and float(prev_close) > 0:
            dist_to_target = (float(target) - float(prev_close)) / float(prev_close) * 100

        entry = it.get("entry_path") or "morning"
        rows.append({
            "推播": "✅" if it.get("alerted_today") else "⏳",
            "來源": "🌅 morning" if entry == "morning" else "⚡ late",
            "代號": it["stock_id"],
            "名稱": it.get("stock_name") or "",
            "產業": it.get("industry") or "",
            "型態": it.get("pattern_type") or "?",
            "目標價": round(float(target), 2) if target is not None else None,
            "昨收": round(float(prev_close), 2) if prev_close is not None else None,
            "昨高": round(float(it["prev_high"]), 2) if it.get("prev_high") is not None else None,
            "距目標%": round(dist_to_target, 2) if dist_to_target is not None else None,
            "昨量(張)": int(it["prev_volume"]) if it.get("prev_volume") is not None else None,
            "五日均量(張)": int(it["vol_ma5"]) if it.get("vol_ma5") is not None else None,
            "MA5": round(float(it["ma5"]), 2) if it.get("ma5") is not None else None,
            "MA10": round(float(it["ma10"]), 2) if it.get("ma10") is not None else None,
            "MA20": round(float(it["ma20"]), 2) if it.get("ma20") is not None else None,
            "MA60": round(float(it["ma60"]), 2) if it.get("ma60") is not None else None,
            "糾結度%": round(float(it["ma_spread_pct"]), 2) if it.get("ma_spread_pct") is not None else None,
            "漲幅cap%": float(it["max_gain_pct"]) if it.get("max_gain_pct") is not None else None,
        })

    df = pd.DataFrame(rows)

    total = len(df)
    alerted = int((df["推播"] == "✅").sum())
    near = int((df["距目標%"].fillna(99) <= 2.0).sum())
    n_a = int((df["型態"] == "A").sum())
    n_b = int((df["型態"] == "B").sum())
    n_morning = int((df["來源"] == "🌅 morning").sum())
    n_late = int((df["來源"] == "⚡ late").sum())
    cm1, cm2, cm3, cm4, cm5, cm6 = st.columns(6)
    cm1.metric("候選總數", total)
    cm2.metric("🌅 morning", n_morning)
    cm3.metric("⚡ late", n_late)
    cm4.metric("型態 A / B", f"{n_a} / {n_b}")
    cm5.metric("已推播", alerted)
    cm6.metric("距目標 ≤ 2%", near)

    st.dataframe(df, use_container_width=True, height=460)

st.markdown("---")

# ── 手動工具 ──────────────────────────────────────────────────────────────────
st.subheader("🛠 手動工具")
mc1, mc2, mc3 = st.columns(3)

with mc1:
    st.markdown("**重建今日候選清單**")
    st.caption(
        "立即跑一次 build_watchlist（拉科技股日 K → 計算指標 → 靜態 gate + 型態 A/B → 寫入 DB），"
        "適用於排程錯過或想驗證新參數時。"
    )
    squeeze_pct = st.slider(
        "型態 A 均線糾結度門檻（%）", min_value=2.0, max_value=6.0,
        value=4.0, step=0.5,
        help="科技股波動較大，預設 4%；越小越嚴。",
        key="v5_rebuild_squeeze",
    )
    use_gain_cap = st.checkbox(
        "啟用漲幅 cap（盤中 checker 會讀此值）",
        value=False, key="v5_rebuild_use_gain_cap",
    )
    gain_cap = None
    if use_gain_cap:
        gain_cap = st.slider(
            "今日漲幅上限（%）", min_value=3.0, max_value=9.0,
            value=6.0, step=0.5,
            help="預估全日漲幅超過此值的個股，盤中不會推播（防追高）。",
            key="v5_rebuild_gain_cap",
        )

    if st.button("立即重建", key="rebuild_v5_watchlist", use_container_width=True):
        with st.spinner("建構中…可能需要 30 秒～數分鐘（依快取狀態）"):
            try:
                from modules.v5_sniper_watchlist_builder import build_watchlist
                stats = build_watchlist(
                    pattern_a_squeeze_pct=squeeze_pct,
                    max_gain_pct=gain_cap,
                )
                st.success(
                    f"完成：scanned={stats.get('scanned', 0)}　"
                    f"A={stats.get('pattern_a', 0)}　B={stats.get('pattern_b', 0)}　"
                    f"written={stats.get('written', 0)}　"
                    f"errors={stats.get('errors', 0)}"
                )
                with st.expander("詳細統計"):
                    st.json(stats)
                st.info("重新整理頁面即可看到新清單。")
            except Exception as exc:
                st.error(f"重建失敗：{exc}")

with mc2:
    st.markdown("**主軌道盤中突破檢查**")
    st.caption(
        "對今日 morning watchlist 中尚未推播的候選跑一次 Shioaji 報價 + 突破/量增/漲幅判斷。"
        "Shioaji 未登入時整輪會安靜略過。"
    )
    if st.button("立即檢查", key="run_v5_breakout_now", use_container_width=True):
        with st.spinner("檢查中…"):
            try:
                from modules.intraday_v5_breakout import run_v5_breakout_check
                sent = run_v5_breakout_check()
                st.success(f"完成：本輪推播 {sent} 則")
            except Exception as exc:
                st.error(f"檢查失敗：{exc}")

with mc3:
    st.markdown("**late qualifier 補抓軌道**")
    st.caption(
        "對 ~300 檔科技股（排除已在 watchlist 者）做盤中 V5 完整重判 — "
        "patch 即時 K 棒 + 估全日量 → 重算指標 → 共同 gate + 型態 A/B + breakout buffer 3%。"
        "適合 09:15 後手動觸發驗證。"
    )
    if st.button("立即執行 late qualifier", key="run_v5_late_qualifier_now", use_container_width=True):
        with st.spinner("補抓中…可能需要 30–60 秒"):
            try:
                from modules.intraday_v5_late_qualifier import run_v5_late_qualifier_check
                sent = run_v5_late_qualifier_check()
                st.success(f"完成：本輪補抓推播 {sent} 則")
            except Exception as exc:
                st.error(f"執行失敗：{exc}")

st.markdown("---")

# ── 突破警示歷史 ──────────────────────────────────────────────────────────────
st.subheader("📜 突破警示歷史（近 14 天）")

date_to = date.today()
date_from = date_to - timedelta(days=14)

events_alert = query_events(
    event_type="v5_sniper_alert",
    date_from=date_from.isoformat(),
    date_to=date_to.isoformat(),
    limit=500,
)
events_failed = query_events(
    event_type="v5_sniper_alert_failed",
    date_from=date_from.isoformat(),
    date_to=date_to.isoformat(),
    limit=500,
)
events_late_alert = query_events(
    event_type="v5_late_qualifier_alert",
    date_from=date_from.isoformat(),
    date_to=date_to.isoformat(),
    limit=500,
)
events_late_failed = query_events(
    event_type="v5_late_qualifier_alert_failed",
    date_from=date_from.isoformat(),
    date_to=date_to.isoformat(),
    limit=500,
)
events_built = query_events(
    event_type="v5_sniper_watchlist_built",
    date_from=date_from.isoformat(),
    date_to=date_to.isoformat(),
    limit=50,
)

tab_alert, tab_late, tab_built = st.tabs(["🌅 主軌道推播", "⚡ late qualifier 推播", "watchlist 建構"])

with tab_alert:
    rows_a = []
    for e in events_alert + events_failed:
        try:
            payload = json.loads(e.get("payload_json") or "{}")
        except Exception:
            payload = {}
        rows_a.append({
            "時間": e["created_at"],
            "結果": "✅" if e["event_type"] == "v5_sniper_alert" else "❌",
            "代號": e.get("stock_id") or "",
            "名稱": e.get("stock_name") or "",
            "型態": payload.get("pattern_type") or "",
            "現價": payload.get("price"),
            "目標價": payload.get("breakout_target"),
            "預估全日量(張)": payload.get("estimated_volume"),
            "昨量(張)": payload.get("prev_volume"),
            "今日漲幅%": payload.get("gain_pct"),
            "摘要": e.get("summary") or "",
        })
    if rows_a:
        rows_a.sort(key=lambda r: r["時間"], reverse=True)
        st.dataframe(pd.DataFrame(rows_a), use_container_width=True, height=380)
    else:
        st.info("近 14 天無主軌道推播紀錄。")

with tab_late:
    rows_l = []
    for e in events_late_alert + events_late_failed:
        try:
            payload = json.loads(e.get("payload_json") or "{}")
        except Exception:
            payload = {}
        rows_l.append({
            "時間": e["created_at"],
            "結果": "✅" if e["event_type"] == "v5_late_qualifier_alert" else "❌",
            "代號": e.get("stock_id") or "",
            "名稱": e.get("stock_name") or "",
            "型態": payload.get("pattern_type") or "",
            "現價": payload.get("price"),
            "突破目標": payload.get("breakout_target"),
            "過突破點%": payload.get("breakout_pct"),
            "buffer%": payload.get("breakout_buffer_pct"),
            "預估全日量(張)": payload.get("estimated_volume_lots"),
            "今日漲幅%": payload.get("gain_pct"),
            "摘要": e.get("summary") or "",
        })
    if rows_l:
        rows_l.sort(key=lambda r: r["時間"], reverse=True)
        st.dataframe(pd.DataFrame(rows_l), use_container_width=True, height=380)
    else:
        st.info(
            "近 14 天無 late qualifier 推播紀錄。"
            "（軌道於 09:15–13:25 每 5 分鐘掃描，補抓晨間 watchlist 外的票。"
            "過熱濾網用「過前高 3% 內」，比主軌道嚴格。）"
        )

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
            "pattern_a": payload.get("pattern_a"),
            "pattern_b": payload.get("pattern_b"),
            "written": payload.get("written"),
            "skipped_no_data": payload.get("skipped_no_data"),
            "skipped_gates_failed": payload.get("skipped_gates_failed"),
            "errors": payload.get("errors"),
        })
    if rows_b:
        st.dataframe(pd.DataFrame(rows_b), use_container_width=True, height=300)
    else:
        st.info("近 14 天無 watchlist 建構紀錄（檢查 scheduler/jobs.py 是否啟動 v5 job）。")
