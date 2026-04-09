# FinMind API 技術規格說明書 (Technical Specification)

## 1. 存取配額與限制 (Rate Limits)
* **註冊會員 (Registered)**: 每小時 **600 次** 請求。
* **未登入用戶 (Guest)**: 每小時 **300 次** 請求。
* **資料集總量**: 包含 **44 種** 核心金融數據集。

## 2. 資料集清單 (Data Catalog)

### A. 技術分析數據 (Technical Analysis)
* **價格基礎**: `TaiwanStockPrice` (台股總覽/股價資料表)、`TaiwanStockDayTrading` (當日沖銷標的/成交量)。
* **評價指標**: `TaiwanStockPER` (個股 PER/PBR 資料表)。
* **市場指數**: `TaiwanStockIndex` (台股加權指數)、`TaiwanStockIndexBoard` (加權/櫃買報酬指數)。
* **盤中統計**: 每 5 秒委託成交統計。

### B. 籌碼面數據 (Bargaining Chip)
* **法人動態**: `TaiwanStockInstitutionalInvestorsBuySell` (三大法人買賣表)、`TaiwanStockShareholding` (外資持股表)。
* **信用交易**: `TaiwanStockMarginPurchaseShortSale` (融資融券表)、信用額度總量管制餘額表。
* **借券資訊**: 借券成交明細、暫停融券賣出表 (融資回補日)。

### C. 基本面數據 (Fundamental Analysis)
* **財務報表**: `TaiwanStockFinancialStatements` (綜合損益表、現金流量表、資產負債表)。
* **營運指標**: `TaiwanStockMonthRevenue` (月營收表)、`TaiwanStockDividend` (股利政策表)、除權除息結果表。
* **資本異動**: 減資/分割/面額變更後之恢復買賣參考價格。

### D. 衍生性與國際金融 (Derivatives & Global)
* **期權數據**: 期貨/選擇權日成交資訊、即時報價總覽、三大法人期權買賣。
* **國際標的**: `USStockPrice` (美股股價)、美國國債 (1月~30年期)、黃金價格、原油價格。
* **總體經濟**: 央行利率 (12國)、外幣對台幣匯率 (19種)。

## 3. 開發邏輯建議 (Development Guidelines for AI)
* **本地快取 (Caching)**: 由於每小時僅 **600 次** 配額，針對 `TaiwanStockMonthRevenue` (月更新) 或 `TaiwanStockFinancialStatements` (季更新) 必須實作本地 SQLite 或 JSON 緩存。
* **批量請求**: 選股掃描時應優先使用 `TaiwanStockPrice` 進行初步過濾，再針對高分標的請求基本面數據。
* **錯誤處理**: 需偵測 HTTP 429 狀態碼並實作指數退避 (Exponential Backoff) 機制。

---

## 🚀 給 AI 的 Prompt 指令範例
「請讀取此 `finmind_api_spec.md`。我需要更新 `app.py` 中的選股雷達，請加入『營收成長』濾網。
條件：
1. 調用 `TaiwanStockMonthRevenue` 確保營收 YoY > 20%。
2. 調用 `TaiwanStockPER` 確保 PER < 20。
請務必實作 SQLite 緩存，避免重複請求導致超過每小時 600 次限制。」