"""
台股交易輔助工具 — 主程式
啟動方式：streamlit run app.py
"""
import streamlit as st
from db.database import init_db
from modules.auth import require_login
from version import __version__


st.set_page_config(
    page_title="台股交易輔助工具",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

require_login()

# 資料庫初始化
init_db()


# ── 啟動背景預抓取（每次 App 啟動只建立一次）────────────────────
@st.cache_resource
def _start_prefetch_worker():
    """透過 cache_resource 確保整個 App 生命週期只啟動一個執行緒"""
    try:
        from scheduler.prefetch import get_worker
        worker = get_worker()
        worker.start()
        return worker
    except Exception as e:
        return None


worker = _start_prefetch_worker()


# ── 主頁面 ───────────────────────────────────────────────────────
st.title("📈 台股交易輔助工具")
st.caption(f"v{__version__}")
st.markdown("---")

col1, col2, col3, col4, col5 = st.columns(5)

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

> 使用前請先在 `.env` 檔案設定你的 **FinMind API Token**。
> 免費申請：https://finmindtrade.com/
""")

# ── 背景工作器狀態快覽 ───────────────────────────────────────────
st.markdown("---")
if worker is not None:
    s = worker.status()
    if s.get("rebuild_mode"):
        status_icon, status_text = "🔴", "全速重建模式（勿手動掃描）"
    elif s.get("pause_remaining_sec", 0) > 0:
        remain = f"{s['pause_remaining_sec']//60}分{s['pause_remaining_sec']%60}秒"
        status_icon, status_text = "🟠", f"429 暫停中（剩 {remain}）"
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
