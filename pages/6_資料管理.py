"""
資料管理頁面

功能：
  - 背景預抓取工作器控制與監控
  - 本機快取狀態總覽
  - 手動觸發更新
"""
import time
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, date, timedelta

from db.database import init_db, vacuum_db
from db.price_cache import get_cache_summary, delete_old_prices, get_all_cached_stocks

init_db()

st.set_page_config(page_title="資料管理", page_icon="🗄️", layout="wide")
st.title("🗄️ 資料管理")
st.markdown("管理本機快取與背景預抓取工作器")

# ── 自動刷新控制（放在最上方，讓用戶可以隨時開關）──────────────
auto_refresh = st.toggle("🔄 自動刷新（每 5 秒）", value=False,
                          help="開啟後頁面每 5 秒自動更新一次，方便觀察抓取進度")
st.markdown("---")


# ── 取得工作器實例 ───────────────────────────────────────────────
@st.cache_resource
def _get_worker():
    try:
        from scheduler.prefetch import get_worker
        return get_worker()
    except Exception:
        return None


worker = _get_worker()

# ══ 區塊一：背景工作器控制 ═══════════════════════════════════════
st.subheader("⚙️ 背景預抓取工作器")

if worker is None:
    st.error("工作器載入失敗，請重新啟動應用程式")
else:
    s = worker.status()

    # ── 狀態指示燈 ────────────────────────────────────────────
    if s.get("rebuild_mode"):
        st.error(
            "🔴 **全速重建模式啟動中** — API 額度全開（600次/小時），"
            "請勿進行選股掃描等手動操作，避免額度衝突。"
        )
    elif not s["running"]:
        st.error("🔴 工作器已停止")
    elif s.get("pause_remaining_sec", 0) > 0:
        remain_min = s["pause_remaining_sec"] // 60
        remain_sec = s["pause_remaining_sec"] % 60
        resume_at  = s["paused_until"].strftime("%H:%M:%S") if s.get("paused_until") else "—"
        st.warning(
            f"🟠 遇到 429 限額，暫停中（第 {s.get('rate_limit_count', 1)} 次）　｜　"
            f"剩餘 **{remain_min} 分 {remain_sec} 秒**，預計 {resume_at} 自動恢復"
        )
    elif s["paused_for_market"]:
        st.warning("🟡 交易時間降速模式（09:00–15:05），每小時上限 100 次，保留額度給手動操作")
    else:
        st.success("🟢 工作器運行中（非交易時間，每小時上限 500 次）")

    # ── 指標列 ────────────────────────────────────────────────
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("本小時已用", f"{s['hour_fetched']} 次", f"上限 {s['hourly_limit']} 次/小時")
    c2.metric("本小時剩餘", f"{s['hourly_remaining']} 次")
    c3.metric("✅ 累計已抓", f"{s['total_fetched']} 次",
              help="只要這個數字在增加，工作器就在正常運作")
    c4.metric("待更新股票", f"{s['queue_size']} 檔",
              help="無快取 + 快取超過 5 天（已排除無資料股票）")
    c5.metric("正在抓取", s["current_stock"] or "—")
    c6.metric("⏭ 略過（無資料）", s.get("skip_count", 0),
              help="FinMind 無價格資料的股票（權證、下市股等），已從清單移除不再重試")
    c7.metric("遇到 429 次數", s.get("rate_limit_count", 0))

    # ── 進度條（用初始待更新數量當固定分母，避免分母跟著長）──────
    initial_q = s.get("initial_queue_size", 0)
    fetched   = s["total_fetched"]
    if initial_q > 0:
        progress = min(fetched / initial_q, 1.0)
        st.progress(progress, text=f"本次啟動進度：已完成 {fetched} / {initial_q} 檔（{progress*100:.1f}%）")

    if s["last_fetch_at"]:
        elapsed = int((datetime.now() - s["last_fetch_at"]).total_seconds())
        elapsed_str = f"{elapsed} 秒前" if elapsed < 60 else f"{elapsed//60} 分鐘前"
        st.caption(f"最近一次抓取：{s['last_fetch_at'].strftime('%H:%M:%S')}（{elapsed_str}）")

    # ── 控制按鈕 ──────────────────────────────────────────────
    is_paused = s.get("pause_remaining_sec", 0) > 0
    col_start, col_stop, col_resume, col_refresh, _ = st.columns([1, 1, 1, 1, 4])

    if col_start.button("▶ 啟動工作器", disabled=s["running"], use_container_width=True):
        worker.start()
        st.rerun()
    if col_stop.button("⏹ 停止工作器", disabled=not s["running"], use_container_width=True):
        worker.stop()
        st.rerun()
    if col_resume.button(
        "⚡ 立即恢復", disabled=not is_paused, use_container_width=True,
        help="跳過 429 暫停，立即繼續抓取",
        type="primary" if is_paused else "secondary",
    ):
        worker.resume()
        st.rerun()
    if col_refresh.button("🔄 重新整理", use_container_width=True):
        st.rerun()

    st.markdown("""
    **工作器說明：**
    - App 啟動時自動開始，不需手動啟動
    - **非交易時間**（15:05–隔日 09:00）：每小時最多 500 次，7 秒抓一檔
    - **交易時間**（09:00–15:05）：每小時上限降為 100 次，優先保留給手動掃描
    - 快取仍新鮮的股票（5 天內）自動跳過，不消耗額度
    - **遇到 429**：整體暫停 20 分鐘，暫停期間可按「⚡ 立即恢復」提前繼續
    - FinMind 免費帳號：600 次/小時（註冊會員）
    """)

    st.markdown("---")

    # ── 全速重建模式 ──────────────────────────────────────────
    st.markdown("#### 🔨 全速重建本機資料庫")
    is_rebuild = s.get("rebuild_mode", False)

    if is_rebuild:
        st.error(
            "**重建模式進行中**  \n"
            "API 額度已全開（600次/小時），請勿使用選股雷達或其他需要 API 的功能，"
            "避免額度衝突。  \n"
            "資料庫重建完成後，請手動按「停止重建模式」恢復正常運作。"
        )
        if st.button("⏹ 停止重建模式，恢復正常限速", type="secondary", use_container_width=False):
            if hasattr(worker, "disable_rebuild_mode"):
                worker.disable_rebuild_mode()
            else:
                worker.rebuild_mode = False
            st.rerun()
    else:
        st.markdown(
            "適用情境：首次安裝、資料庫損毀、或長時間未更新需要全面補齊快取。  \n"
            "啟動後 API 額度全開至 600 次/小時，**建議在非使用期間執行**（例如睡前）。"
        )

        # ── 第一步：按下重建按鈕 ──────────────────────────────
        if "rebuild_confirm" not in st.session_state:
            st.session_state.rebuild_confirm = False

        if not st.session_state.rebuild_confirm:
            if st.button("🔨 重建資料庫", type="secondary"):
                st.session_state.rebuild_confirm = True
                st.rerun()
        else:
            # ── 第二步：確認對話框 ────────────────────────────
            st.warning(
                "⚠️ **請確認以下事項後再繼續：**\n\n"
                "1. 重建期間 API 額度全開（600次/小時），選股掃描會與背景搶額度\n"
                "2. 全市場約 950 檔，每小時最多抓 600 檔，完整重建約需 1.5–2 小時\n"
                "3. 建議在不使用系統期間（例如睡前）執行"
            )
            col_confirm, col_cancel, _ = st.columns([1.2, 1, 5])
            if col_confirm.button("✅ 確認，全速重建", type="primary", use_container_width=True):
                st.session_state.rebuild_confirm = False
                if not worker.running:
                    worker.start()
                if hasattr(worker, "resume"):
                    worker.resume()        # 清除任何 429 暫停
                if hasattr(worker, "enable_rebuild_mode"):
                    worker.enable_rebuild_mode()
                else:
                    worker.rebuild_mode = True
                st.rerun()
            if col_cancel.button("取消", use_container_width=True):
                st.session_state.rebuild_confirm = False
                st.rerun()

st.markdown("---")

# ══ 區塊二：快取狀態總覽 ═════════════════════════════════════════
st.subheader("📦 本機快取狀態")

col_load, col_clean, col_vacuum, _ = st.columns([1, 1, 1, 5])
load_clicked   = col_load.button("載入快取摘要", use_container_width=True)
clean_clicked  = col_clean.button("清理 400 天前資料", use_container_width=True, type="secondary")
vacuum_clicked = col_vacuum.button("最佳化資料庫", use_container_width=True, type="secondary")

if clean_clicked:
    deleted = delete_old_prices(keep_days=400)
    st.success(f"已刪除 {deleted} 筆舊資料")

if vacuum_clicked:
    with st.spinner("最佳化中..."):
        vacuum_db()
    st.success("資料庫最佳化完成（碎片整理、空間回收）")

if load_clicked or "cache_summary" in st.session_state:
    with st.spinner("讀取快取摘要..."):
        summary = get_cache_summary()
        st.session_state["cache_summary"] = summary

if "cache_summary" in st.session_state:
    summary = st.session_state["cache_summary"]

    if summary.empty:
        st.info("快取為空，工作器啟動後將自動開始填充")
    else:
        today_str  = date.today().isoformat()
        stale_cut  = (date.today() - timedelta(days=5)).isoformat()

        total      = len(summary)
        fresh      = (summary["latest"] >= stale_cut).sum()
        stale      = total - fresh
        total_days = summary["days"].sum()

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("已快取股票", f"{total} 檔")
        m2.metric("快取新鮮", f"{fresh} 檔", f"最近 5 天內有更新")
        m3.metric("需要更新", f"{stale} 檔")
        m4.metric("資料總筆數", f"{total_days:,} 筆")

        # 快取新鮮度長條圖
        summary["status"] = summary["latest"].apply(
            lambda x: "新鮮（5 天內）" if x >= stale_cut else "過期"
        )
        fig = px.histogram(
            summary, x="latest", color="status",
            color_discrete_map={"新鮮（5 天內）": "#27ae60", "過期": "#e74c3c"},
            labels={"latest": "最新資料日期", "count": "股票數"},
            title="各股最新快取日期分佈",
            nbins=60,
        )
        fig.update_layout(
            height=300, template="plotly_dark",
            margin=dict(t=40, b=10), showlegend=True,
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)

        # 詳細列表（可搜尋）
        with st.expander("查看詳細清單", expanded=False):
            search = st.text_input("搜尋股票代碼", placeholder="2330")
            display = summary.copy()
            if search:
                display = display[display["stock_id"].str.contains(search)]
            display = display.rename(columns={
                "stock_id": "代碼", "earliest": "最早日期",
                "latest": "最新日期", "days": "天數"
            })
            st.dataframe(display, use_container_width=True, hide_index=True, height=400)

st.markdown("---")

# ══ 區塊三：手動補抓 ════════════════════════════════════════════
st.subheader("🔧 手動補抓")
st.markdown("若特定股票快取不足，可在此手動更新（消耗 FinMind API 額度）")

col_input, col_btn = st.columns([3, 1])
with col_input:
    manual_ids = st.text_input(
        "輸入股票代碼（多檔用逗號分隔）",
        placeholder="2330, 2317, 0050",
    )
with col_btn:
    st.markdown("<br>", unsafe_allow_html=True)
    manual_fetch = st.button("立即補抓", use_container_width=True, type="primary")

if manual_fetch and manual_ids.strip():
    ids = [x.strip() for x in manual_ids.split(",") if x.strip()]
    prog = st.progress(0)
    results = []
    for i, sid in enumerate(ids):
        prog.progress((i + 1) / len(ids))
        try:
            from data.finmind_client import smart_get_price
            df = smart_get_price(sid, required_days=150)
            rows = len(df) if not df.empty else 0
            results.append({"代碼": sid, "狀態": "✅ 成功", "筆數": rows})
        except Exception as e:
            results.append({"代碼": sid, "狀態": f"❌ {e}", "筆數": 0})
    prog.empty()
    st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
    # 清除快取摘要快照，讓下次載入時重新計算
    st.session_state.pop("cache_summary", None)

# ── 自動刷新執行（放在頁面最底部）────────────────────────────────
if auto_refresh:
    time.sleep(5)
    st.rerun()
