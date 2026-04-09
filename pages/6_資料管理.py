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
from db.price_cache import get_cache_summary, delete_old_prices, get_all_cached_stocks, diagnose_cache
from db.settings import is_market_closed, set_market_closed
from db.inst_cache import get_inst_cache_stats
from db.fundamental_cache import get_fundamental_stats

init_db()

st.set_page_config(page_title="資料管理", page_icon="🗄️", layout="wide")
st.title("🗄️ 資料管理")
st.markdown("管理本機快取與背景預抓取工作器")

# ── 休市模式 ──────────────────────────────────────────────────
_market_closed = is_market_closed()
col_mc, col_mc_info = st.columns([2, 5])
new_mc = col_mc.toggle(
    "🏖️ 休市模式",
    value=_market_closed,
    help="開啟後法人資料快取永不過期，完全不消耗 API 額度。適用於連假、停市期間。",
)
if new_mc != _market_closed:
    set_market_closed(new_mc)
    st.rerun()

if new_mc:
    col_mc_info.warning(
        "**休市模式啟用中** — 法人資料使用快取，不會重新呼叫 API。  \n"
        "恢復交易後請關閉此開關，系統會在下次掃描時自動抓取最新法人資料。"
    )
    # 顯示快取狀態
    stats = get_inst_cache_stats()
    if stats["stock_count"] > 0:
        newest = stats["newest_fetch"]
        if newest:
            newest_str = newest if isinstance(newest, str) else newest
            col_mc_info.caption(f"法人快取：**{stats['stock_count']}** 檔，最新抓取 {newest_str[:16]}")
    # 清除按鈕
    if col_mc.button("🗑️ 清除法人快取", help="清除後下次掃描會重新從 API 抓取所有法人資料"):
        from sqlalchemy import text
        from db.database import get_session
        with get_session() as sess:
            sess.execute(text("DELETE FROM inst_cache"))
            sess.commit()
        st.success("法人快取已清除，下次掃描將重新抓取")
        st.rerun()
else:
    col_mc_info.info(
        "**正常模式** — 法人資料快取 24 小時後自動更新。  \n"
        "若目前為連假或停市，建議開啟「休市模式」避免浪費 API 額度。"
    )

st.markdown("---")

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
    if s.get("backtest_rebuild_mode"):
        bt_queue = s.get("backtest_queue_size", 0)
        bt_initial = s.get("backtest_initial_queue_size", 0)
        done = max(bt_initial - bt_queue, 0) if bt_initial > 0 else 0
        st.warning(
            "🟠 **回測歷史重建模式啟動中** — 正在補抓全市場 10 年歷史資料。  \n"
            f"目前進度：**{done} / {bt_initial or '—'} 檔**，待處理 **{bt_queue} 檔**。"
        )
    elif s.get("rebuild_mode"):
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
    if s.get("backtest_rebuild_mode"):
        c4.metric("回測待補股票", f"{s.get('backtest_queue_size', 0)} 檔",
                  help="歷史深度不足 10 年、需要補抓回測資料的股票數")
    else:
        # 直接查 DB 取得即時數字，排除 worker 會略過的股票：
        # delisted / legacy no_update / 今日 suspended
        try:
            from db.price_cache import (
                get_cache_summary,
                get_known_stock_ids,
                get_delisted_stocks,
                get_suspended_stocks,
            )
            from data.finmind_client import resolve_latest_trading_day
            from datetime import date as _date
            _latest_td = resolve_latest_trading_day()
            _summary = get_cache_summary()
            _known = get_known_stock_ids()
            _skip_ids = set(get_delisted_stocks(include_legacy_no_update=True))
            _suspended_today = set(get_suspended_stocks(today_only=True))

            if _summary.empty:
                _real_pending = len([x for x in _known if x not in _skip_ids])
                _skip_cnt = len(_skip_ids)
            else:
                _cached_ids = set(_summary["stock_id"])
                _stale_ids = set(_summary.loc[
                    _summary["latest"] < _latest_td.isoformat(), "stock_id"
                ]) - _skip_ids - _suspended_today
                _missing_ids = set(_known) - _cached_ids - _skip_ids
                _real_pending = len(_stale_ids) + len(_missing_ids)
                _skip_cnt = len(_skip_ids)
        except Exception:
            _real_pending = None
            _skip_cnt = 0

        _worker_q = s['queue_size']
        if _real_pending is not None:
            c4.metric("待更新股票（即時）", f"{_real_pending} 檔",
                      delta=f"Worker快照：{_worker_q} 檔",
                      delta_color="off",
                      help=f"基準日：{_latest_td}，已排除 {_skip_cnt} 檔略過股票與今日失敗股票")
        else:
            c4.metric("待更新股票", f"{_worker_q} 檔",
                      help="無快取 + 快取未達最新交易日")
    c5.metric("正在抓取", s["current_stock"] or "—")
    c6.metric("⏭ 略過（無資料）", s.get("skip_count", 0),
              help="FinMind 無價格資料的股票（權證、下市股等），已從清單移除不再重試")
    c7.metric("遇到 429 次數", s.get("rate_limit_count", 0))

    # ── 進度條（用初始待更新數量當固定分母，避免分母跟著長）──────
    if s.get("backtest_rebuild_mode"):
        bt_initial = s.get("backtest_initial_queue_size", 0)
        bt_queue = s.get("backtest_queue_size", 0)
        if bt_initial > 0:
            bt_done = max(bt_initial - bt_queue, 0)
            bt_progress = min(bt_done / bt_initial, 1.0)
            st.progress(
                bt_progress,
                text=f"回測歷史重建進度：已完成 {bt_done} / {bt_initial} 檔（{bt_progress*100:.1f}%）"
            )
        else:
            st.info("正在建立回測重建清單，首次統計可能需要幾秒鐘。")
    elif s.get("rebuild_mode"):
        initial_q = s.get("initial_queue_size", 0)
        remain_q = s.get("queue_size", 0)
        if initial_q > 0:
            done_q = max(initial_q - remain_q, 0)
            progress = min(done_q / initial_q, 1.0)
            st.progress(progress, text=f"全速重建進度：已完成 {done_q} / {initial_q} 檔（{progress*100:.1f}%）")

    if s["last_fetch_at"]:
        elapsed = int((datetime.now() - s["last_fetch_at"]).total_seconds())
        elapsed_str = f"{elapsed} 秒前" if elapsed < 60 else f"{elapsed//60} 分鐘前"
        st.caption(f"最近一次抓取：{s['last_fetch_at'].strftime('%H:%M:%S')}（{elapsed_str}）")
    elif s.get("backtest_rebuild_mode"):
        st.caption("回測重建已啟動，等待第一筆資料抓取中。")

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

    if s.get("backtest_rebuild_mode"):
        st.markdown("""
        **回測重建觀察重點：**
        - 看「回測待補股票」是否持續下降
        - 看「正在抓取」是否持續變化（會顯示 `[回測] 股票代碼`）
        - 看「最近一次抓取」時間是否持續更新
        - 若遇到 429，頁面上方會顯示暫停倒數，可按「⚡ 立即恢復」提前繼續
        """)
    elif s.get("rebuild_mode"):
        st.markdown("""
        **全速重建觀察重點：**
        - 看「待更新股票」是否持續下降
        - 看「正在抓取」是否持續變化（會顯示 `[重建]` 或股票代碼）
        - 看「最近一次抓取」時間是否持續更新
        - 若遇到 429，頁面上方會顯示暫停倒數，可按「⚡ 立即恢復」提前繼續
        """)
    else:
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

    completed_at = s.get("rebuild_completed_at")
    if completed_at:
        st.success(
            f"✅ **全速重建已完成**（{completed_at.strftime('%m/%d %H:%M')}）  \n"
            "所有待更新股票已全數處理，系統已自動退出重建模式，恢復正常限速。"
        )
    elif is_rebuild:
        st.error(
            "**重建模式進行中**  \n"
            "API 額度已全開（600次/小時），請勿使用選股雷達或其他需要 API 的功能，"
            "避免額度衝突。  \n"
            "待更新股票歸零後會**自動退出重建模式**，不需手動停止。"
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

    # ── 回測歷史資料重建 ──────────────────────────────────────────
    st.markdown("#### 📼 回測歷史資料重建（最多往前 10 年）")

    is_bt_rebuild = s.get("backtest_rebuild_mode", False)
    bt_completed_at = s.get("backtest_completed_at")
    bt_queue = s.get("backtest_queue_size", 0)
    bt_initial = s.get("backtest_initial_queue_size", 0)

    if bt_completed_at:
        st.success(
            f"✅ **回測歷史重建已完成**（{bt_completed_at.strftime('%m/%d %H:%M')}）  \n"
            "所有股票均已具備 10 年歷史資料，系統已自動退出重建模式。"
        )
    elif is_bt_rebuild:
        st.warning(
            "**回測歷史重建進行中**  \n"
            f"目標：全市場每檔最多補充 10 年日K資料。待處理：**{bt_queue} 檔**  \n"
            "完成後自動退出，不需手動停止。"
        )
        if bt_initial > 0:
            bt_progress = min((bt_initial - bt_queue) / bt_initial, 1.0)
            st.progress(bt_progress,
                        text=f"進度：已完成 {bt_initial - bt_queue} / {bt_initial} 檔（{bt_progress*100:.1f}%）")
        if st.button("⏹ 停止回測重建", type="secondary"):
            if hasattr(worker, "disable_backtest_rebuild_mode"):
                worker.disable_backtest_rebuild_mode()
            st.rerun()
    else:
        st.markdown(
            "補充全市場股票最多 10 年的歷史日K資料，提升回測涵蓋範圍與準確度。  \n"
            "全市場約 950 檔 × 每檔 1 次 API = 約 950 次請求，**建議睡前啟動**。"
        )

        if "bt_rebuild_confirm" not in st.session_state:
            st.session_state.bt_rebuild_confirm = False

        if not st.session_state.bt_rebuild_confirm:
            if st.button("📼 重建回測歷史資料", type="secondary"):
                st.session_state.bt_rebuild_confirm = True
                st.rerun()
        else:
            st.warning(
                "⚠️ **請確認後再繼續：**\n\n"
                "1. 每檔股票抓取 10 年資料，每次請求量較大，約消耗 950 次 API 配額\n"
                "2. 已有足夠歷史的股票自動跳過，不重複消耗配額\n"
                "3. 建議在不使用系統期間（例如睡前）執行"
            )
            col_btc, col_btx, _ = st.columns([1.2, 1, 5])
            if col_btc.button("✅ 確認，開始重建", type="primary", use_container_width=True):
                st.session_state.bt_rebuild_confirm = False
                if not worker.running:
                    worker.start()
                if hasattr(worker, "resume"):
                    worker.resume()   # 清除既有 429 暫停，立即開始回測重建
                if hasattr(worker, "enable_backtest_rebuild_mode"):
                    worker.enable_backtest_rebuild_mode()
                st.rerun()
            if col_btx.button("取消", use_container_width=True):
                st.session_state.bt_rebuild_confirm = False
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

# ══ 區塊二．五：快取品質診斷 ════════════════════════════════════════
st.subheader("🔍 快取品質診斷")
st.markdown("找出完全缺失、資料不足或過舊的股票，並可一鍵批次補抓。")

diag_col1, diag_col2, diag_col3, _ = st.columns([1, 1, 1, 5])
min_days_thresh = diag_col1.number_input("最低天數門檻", min_value=30, max_value=1000, value=200, step=50,
                                          help="快取筆數低於此值視為資料不足")
stale_days_thresh = diag_col2.number_input("過舊天數門檻", min_value=3, max_value=60, value=7, step=1,
                                            help="最新日期超過幾天前視為過舊")
run_diag = diag_col3.button("執行診斷", type="primary", use_container_width=True)

if run_diag:
    with st.spinner("診斷中，讀取快取資料庫..."):
        diag = diagnose_cache(min_days=min_days_thresh, stale_days=stale_days_thresh)
    st.session_state["cache_diag"] = diag

if "cache_diag" in st.session_state:
    diag = st.session_state["cache_diag"]
    summary_df = diag["summary"]
    missing_ids = diag["missing"]
    thin_df = diag["thin"]
    stale_df = diag["stale"]
    problem_ids = diag["problem_ids"]

    # ── 診斷指標列 ──────────────────────────────────────────────
    delisted_df = diag.get("delisted", pd.DataFrame())
    suspend_ids = diag.get("suspend_ids", set())

    d1, d2, d3, d4, d5, d6 = st.columns(6)
    d1.metric("已知市場股票", f"{len(diag['missing']) + len(summary_df)} 檔",
              help="stock_info_cache 中的股票總數")
    d2.metric("已下市（排除）", f"{len(delisted_df)} 檔",
              help=f"ref/suspendList.csv 共 {len(suspend_ids)} 筆，快取中命中 {len(delisted_df)} 檔，已排除在問題清單之外")
    d3.metric("完全缺失", f"{len(missing_ids)} 檔",
              delta=f"-{len(missing_ids)}" if missing_ids else None,
              delta_color="inverse" if missing_ids else "off",
              help="stock_info_cache 有記錄、非下市，但 price_cache 完全沒有資料")
    d4.metric(f"資料不足（< {min_days_thresh} 天）", f"{len(thin_df)} 檔",
              delta_color="inverse")
    d5.metric(f"資料過舊（> {stale_days_thresh} 天）", f"{len(stale_df)} 檔",
              delta_color="inverse")
    d6.metric("需要補抓（合計）", f"{len(problem_ids)} 檔",
              delta_color="inverse")

    # ── 分頁顯示問題股票 ─────────────────────────────────────────
    if not summary_df.empty:
        # 資料筆數分佈
        fig_days = px.histogram(
            summary_df, x="days",
            nbins=50,
            labels={"days": "快取天數", "count": "股票數"},
            title="快取天數分佈",
            color_discrete_sequence=["#3498db"],
        )
        fig_days.add_vline(x=min_days_thresh, line_dash="dash", line_color="#e74c3c",
                           annotation_text=f"門檻 {min_days_thresh} 天")
        fig_days.update_layout(height=280, margin=dict(t=40, b=10))
        st.plotly_chart(fig_days, use_container_width=True)

    tab_missing, tab_thin, tab_stale, tab_delisted, tab_all = st.tabs(
        [f"完全缺失 ({len(missing_ids)})", f"資料不足 ({len(thin_df)})",
         f"資料過舊 ({len(stale_df)})", f"已下市 ({len(delisted_df)})",
         f"全部有快取 ({len(summary_df)})"]
    )

    with tab_missing:
        if missing_ids:
            st.warning(f"以下 **{len(missing_ids)}** 檔股票在股票清單中，但快取完全沒有資料：")
            # 每行顯示 10 個代碼
            chunks = [missing_ids[i:i+10] for i in range(0, len(missing_ids), 10)]
            for chunk in chunks:
                st.code("  ".join(chunk))
        else:
            st.success("所有已知股票皆有快取資料。")

    with tab_thin:
        if not thin_df.empty:
            st.warning(f"以下 **{len(thin_df)}** 檔股票快取天數不足 {min_days_thresh} 天：")
            show_thin = thin_df.rename(columns={
                "stock_id": "代碼", "earliest": "最早日期", "latest": "最新日期", "days": "天數", "status": "狀態"
            })
            st.dataframe(show_thin.sort_values("天數"), use_container_width=True, hide_index=True, height=320)
        else:
            st.success(f"所有快取股票均達 {min_days_thresh} 天門檻。")

    with tab_stale:
        if not stale_df.empty:
            st.warning(f"以下 **{len(stale_df)}** 檔股票最新資料超過 {stale_days_thresh} 天前：")
            show_stale = stale_df.rename(columns={
                "stock_id": "代碼", "earliest": "最早日期", "latest": "最新日期", "days": "天數", "status": "狀態"
            })
            st.dataframe(show_stale.sort_values("最新日期"), use_container_width=True, hide_index=True, height=320)
        else:
            st.success("所有快取資料均在時效內。")

    with tab_delisted:
        if not delisted_df.empty:
            st.info(
                f"以下 **{len(delisted_df)}** 檔股票已記錄於 `ref/suspendList.csv`，"
                "為已下市股票，快取資料僅供歷史參考，不需補抓，不計入問題清單。"
            )
            show_delisted = delisted_df[["stock_id", "earliest", "latest", "days"]].rename(columns={
                "stock_id": "代碼", "earliest": "最早日期", "latest": "最新日期", "days": "天數"
            })
            st.dataframe(show_delisted.sort_values("代碼"), use_container_width=True, hide_index=True, height=360)
        else:
            st.info("快取中目前沒有已下市股票的資料。")

    with tab_all:
        if not summary_df.empty:
            search_diag = st.text_input("搜尋代碼", placeholder="2330", key="diag_search")
            show_all = summary_df.copy()
            if search_diag:
                show_all = show_all[show_all["stock_id"].str.contains(search_diag)]
            show_all = show_all.rename(columns={
                "stock_id": "代碼", "earliest": "最早日期", "latest": "最新日期", "days": "天數", "status": "狀態"
            })
            # 問題股票標紅：用 status 欄位判斷
            st.dataframe(
                show_all.sort_values("天數"),
                use_container_width=True, hide_index=True, height=400,
            )

    # ── 批次補抓問題股票 ─────────────────────────────────────────
    if problem_ids:
        st.markdown("---")
        st.markdown("#### 批次補抓問題股票")
        st.info(
            f"診斷出 **{len(problem_ids)}** 檔有問題（缺失 + 不足 + 過舊）。  \n"
            "點擊下方按鈕將這些股票加入工作器優先佇列，或直接在下方手動補抓。"
        )
        # 提供預填到手動補抓欄位
        prob_str = ", ".join(problem_ids[:50])  # 最多顯示 50 個
        st.code(prob_str + ("..." if len(problem_ids) > 50 else ""), language=None)

        if worker is not None and st.button("🚀 排入工作器優先補抓", type="primary"):
            if hasattr(worker, "priority_enqueue"):
                worker.priority_enqueue(problem_ids)
                st.success(f"已將 {len(problem_ids)} 檔排入優先佇列，工作器將優先補抓。")
            else:
                st.warning("工作器不支援優先佇列，請使用下方「手動補抓」。")
            st.rerun()

st.markdown("---")

# ══ 區塊三：基本面快取 ═══════════════════════════════════════════
st.subheader("📊 基本面快取狀態")

fund_stats = get_fundamental_stats()
fund_stale = fund_stats["total"] - fund_stats["fresh"]
newest_fund = fund_stats.get("newest_fetch")
newest_fund_str = str(newest_fund)[:16] if newest_fund else "尚無資料"

bf1, bf2, bf3, bf4 = st.columns(4)
bf1.metric("已快取股票", f"{fund_stats['total']} 檔")
bf2.metric("仍然有效（90天內）", f"{fund_stats['fresh']} 檔")
bf3.metric("需要更新", f"{fund_stale} 檔")
bf4.metric("最新抓取", newest_fund_str)

# 顯示背景工作器基本面佇列大小
if worker is not None:
    fw_status = worker.status()
    if fw_status.get("fund_queue_size", 0) > 0:
        st.info(
            f"背景工作器待填充基本面：**{fw_status['fund_queue_size']} 檔**  \n"
            "價格快取全部補齊後，工作器會自動開始填充基本面資料。"
        )

col_fund_clear, _ = st.columns([1, 7])
if col_fund_clear.button("🗑️ 清除基本面快取", type="secondary"):
    from sqlalchemy import text
    from db.database import get_session
    with get_session() as sess:
        sess.execute(text("DELETE FROM fundamental_cache"))
        sess.commit()
    st.success("基本面快取已清除，背景工作器會在下次閒置時自動重新填充")
    st.rerun()

st.caption(
    "財報每季發布，90 天 TTL 已足夠。"
    "背景工作器會在所有價格快取補齊後，自動填充尚未快取的基本面資料。"
)

st.markdown("---")

# ══ 區塊四：手動補抓 ════════════════════════════════════════════
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
