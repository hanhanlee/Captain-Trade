"""
台股交易輔助工具 — 主程式
啟動方式：streamlit run app.py
"""
import streamlit as st
from db.database import init_db

# 初始化資料庫
init_db()

st.set_page_config(
    page_title="台股交易輔助工具",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


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
st.markdown("---")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(label="模組狀態", value="選股雷達", delta="已啟用")
with col2:
    st.metric(label="持股監控", value="開發中", delta=None)
with col3:
    st.metric(label="風險控制", value="開發中", delta=None)
with col4:
    st.metric(label="市場環境", value="開發中", delta=None)
with col5:
    st.metric(label="交易日誌", value="開發中", delta=None)

st.markdown("---")
st.markdown("""
### 使用說明

| 頁面 | 功能 |
|------|------|
| **1 - 選股雷達** | 掃描全市場，找出技術面強勢、值得關注的股票 |
| **2 - 持股監控** | 輸入持股，即時監控損益與賣出警示（開發中） |
| **3 - 風險控制** | 計算合理部位大小，控制總帳戶風險（開發中） |
| **4 - 市場環境** | 判讀目前大盤多空環境（開發中） |
| **5 - 交易日誌** | 記錄每筆交易，分析勝率與盈虧比（開發中） |

> 使用前請先在 `.env` 檔案設定你的 **FinMind API Token**。
> 免費申請：https://finmindtrade.com/
""")

# ── 背景預抓取狀態面板 ───────────────────────────────────────────
st.markdown("---")
with st.expander("🔄 背景資料預抓取狀態", expanded=False):
    if worker is None:
        st.warning("背景工作器未啟動")
    else:
        s = worker.status()

        c1, c2, c3, c4 = st.columns(4)
        running_label = "運行中" if s["running"] else "已停止"
        running_delta = "✅ 活躍" if s["running"] else "⛔ 停止"

        if s.get("paused_for_market"):
            running_label = "交易時間暫停"
            running_delta = "⏸ 09:00–15:05"

        c1.metric("工作器狀態", running_label, running_delta)
        c2.metric("今日已抓取", f"{s['today_fetched']} 次", f"剩餘 {s['budget_remaining']} 次")
        c3.metric("待更新股票", f"{s['queue_size']} 檔")
        c4.metric("正在抓取", s["current_stock"] or "—")

        if s["last_fetch_at"]:
            st.caption(f"最近一次抓取：{s['last_fetch_at'].strftime('%H:%M:%S')}")

        col_start, col_stop, _ = st.columns([1, 1, 6])
        if col_start.button("▶ 啟動", disabled=s["running"]):
            worker.start()
            st.rerun()
        if col_stop.button("⏹ 停止", disabled=not s["running"]):
            worker.stop()
            st.rerun()

    st.caption("背景工作器在非交易時間（15:05–09:00）自動將全市場股票存入本機快取，"
               "讓掃描時幾乎不需要呼叫 API。每日預算 500 次，保留 100 次給手動操作。")

st.sidebar.title("導航")
st.sidebar.info("請從左側選單選擇功能頁面")
