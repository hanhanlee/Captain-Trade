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

st.sidebar.title("導航")
st.sidebar.info("請從左側選單選擇功能頁面")
