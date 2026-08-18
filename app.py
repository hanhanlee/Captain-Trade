"""
台股交易輔助工具 — 主程式
啟動方式：streamlit run app.py
"""
import streamlit as st
from db.database import init_db
from modules.auth import require_login
from version import __version__
from scheduler._backup_lock import read_lock


st.set_page_config(
    page_title="台股交易輔助工具",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

require_login()


# ── 備份運行警示（必須在 init_db 之前；備份期間 srock.db 被 VACUUM 鎖住） ──
def _render_backup_warning() -> None:
    """
    讀 runtime/backup.lock 顯示醒目警示。
    備份期間任何寫入 DB 的操作（手動補抓、批次掃描、寫快取）會被 VACUUM
    擋住最多 120 秒（busy_timeout）才放棄；讀取不受影響。
    任務結束時 backup_gdrive.py 會刪除 lock 檔，下次頁面 rerun 時警示自動消失。
    """
    info = read_lock()
    if info is None:
        return
    started_at = info.get("started_at", "未知")
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, #b91c1c 0%, #dc2626 100%);
            color: #ffffff;
            padding: 18px 24px;
            border-radius: 10px;
            border-left: 8px solid #fbbf24;
            font-size: 17px;
            font-weight: 700;
            margin: 0 0 18px 0;
            box-shadow: 0 4px 12px rgba(220, 38, 38, 0.35);
            line-height: 1.5;
        ">
        📦 系統正在備份資料庫（VACUUM INTO + gzip + 上傳 Google Drive）
        <div style="font-size: 14px; font-weight: 400; margin-top: 8px; color: #fef3c7;">
        ⚠️ 此期間 <b>不要</b> 執行：手動補抓持股 / 批次選股 / 寫入快取等任何 <b>寫入 DB</b> 的操作（讀取頁面不受影響）。<br>
        備份預計耗時 3–5 分鐘，結束後此提示會自動消失。<br>
        <span style="opacity: 0.85;">開始時間：{started_at}　｜　PID：{info.get("pid", "?")}</span>
        </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button("🔄 我已等候 5 分鐘，重新檢查狀態", key="_backup_lock_recheck"):
        st.rerun()


_render_backup_warning()

# 資料庫初始化
init_db()


@st.cache_resource
def _auto_login_shioaji():
    """App 啟動時嘗試自動登入 Shioaji（credentials 來自 .env）。"""
    try:
        from broker.shioaji_adapter import get_adapter
        adapter = get_adapter()
        if not adapter.is_logged_in():
            adapter.login()
    except Exception:
        pass


_auto_login_shioaji()


@st.cache_resource
def _sync_intraday_monitor_scheduler():
    """啟動或停止內建盤中持股監控排程器。"""
    try:
        from scheduler.intraday_service import sync_intraday_scheduler_from_settings

        return sync_intraday_scheduler_from_settings()
    except Exception:
        return {"running": False, "last_error": "failed to sync intraday scheduler"}


intraday_scheduler = _sync_intraday_monitor_scheduler()


# ── 主頁面 ───────────────────────────────────────────────────────
st.title("📈 台股交易輔助工具")
st.caption(f"v{__version__}")
st.markdown("---")

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric(label="選股雷達", value="已啟用", delta="正常")
with col2:
    st.metric(label="持股監控", value="已啟用", delta="正常")
with col3:
    st.metric(label="風險控制", value="已啟用", delta="正常")
with col4:
    st.metric(label="市場環境", value="已啟用", delta="正常")
with col5:
    st.metric(label="交易日誌", value="已啟用", delta="正常")
with col6:
    try:
        from broker.shioaji_adapter import get_adapter
        _broker_ok = get_adapter().is_logged_in()
    except Exception:
        _broker_ok = False
    st.metric(
        label="Broker API",
        value="已連線" if _broker_ok else "未連線",
        delta="正常" if _broker_ok else "前往連線",
        delta_color="normal" if _broker_ok else "inverse",
    )

if not _broker_ok:
    st.warning(
        "⚠️ **Shioaji 尚未連線**：盤中監控目前使用 FinMind/Yahoo 報價。"
        "請至 **📡 Broker 市場輔助** 頁面登入以啟用即時報價與漲跌停警示。",
        icon=None,
    )

st.markdown("---")
st.markdown("""
### 使用說明

| 頁面 | 功能 |
|------|------|
| **1 - 選股雷達** | 掃描全市場，找出技術面強勢、值得關注的股票 |
| **2 - 持股監控** | 輸入持股，即時監控損益與賣出警示 |
| **3 - 風險控制** | 計算合理部位大小，控制總帳戶風險 |
| **4 - 市場環境** | 判讀目前大盤多空環境 |
| **5 - 交易日誌** | 記錄每筆交易，分析勝率與盈虧比 |
| **8 - 問題回報** | 回報使用問題或提出功能建議，存成 Markdown 方便後續處理 |

> 使用前請先在 `.env` 檔案設定你的 **FinMind API Token**。
> 免費申請：https://finmindtrade.com/
""")

# ── 背景工作器狀態快覽 ───────────────────────────────────────────
st.markdown("---")

# worker 由獨立進程 (PrefetchService) 管理，見 commit 4dee591；
# 首頁僅讀取狀態顯示，不在此啟動 in-process 工作器（auto_start=False）
try:
    from modules.worker_runtime import get_prefetch_worker
    worker = get_prefetch_worker(auto_start=False)
except Exception:
    worker = None

if worker is not None:
    s = worker.status()
    if s.get("rebuild_mode"):
        status_icon, status_text = "🔴", "全速重建模式（勿手動掃描）"
    elif s.get("pause_remaining_sec", 0) > 0:
        remain = f"{s['pause_remaining_sec']//60}分{s['pause_remaining_sec']%60}秒"
        status_icon, status_text = "🟠", f"FinMind 限流 / 配額暫停中（剩 {remain}）"
    elif not s["running"]:
        status_icon, status_text = "🔴", "已停止"
    elif s["paused_for_market"]:
        status_icon, status_text = "🟡", "交易時間降速"
    else:
        status_icon, status_text = "🟢", "運行中"
    st.caption(
        f"{status_icon} 背景預抓取工作器：{status_text}　｜　"
        f"本小時已用 {s['hour_fetched']}/{s['hourly_limit']} 次　｜　"
        f"待更新 {s['queue_size']} 檔　｜　"
        f"詳細管理請至 **6 - 資料管理**"
    )

st.sidebar.title("導航")
st.sidebar.info("請從左側選單選擇功能頁面")
