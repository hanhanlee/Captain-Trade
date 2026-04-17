# 設計說明：Yahoo Bridge + 分階段資料載入

**版本：** v1.0  
**建立日期：** 2026-04-16  
**狀態：** 待實作

---

## 一、背景與動機

### 問題

台股 13:30 收盤，但 FinMind 通常要到 **15:30–19:00** 才發布當日資料。
這段空窗期長達 2–5 小時，系統快取停在昨日，掃描與持股監控無法反映今日收盤數字。

今日（2026-04-16）FinMind 更晚才更新（約 18:20），進一步暴露此問題。

### 解法概念

```
13:30 收盤
13:45 → Yahoo Finance 有延遲 15 分鐘資料（即收盤價）
       → Worker 批次抓取全市場今日收盤，存入 price_cache（INSERT OR IGNORE）
       → 使用者可用今日資料進行初步掃描

15:00+ → 開始探測 FinMind 是否更新
FinMind 更新後 → 優先補抓 Yahoo 未覆蓋的股票
              → FinMind 資料以 INSERT OR REPLACE 覆蓋 Yahoo 資料
              → 接續抓取附加資料（法人、融資融券）

附加資料完成 → 全功能掃描可用
```

---

## 二、資料分層定義

| 層級 | 資料內容 | 來源 | 掃描影響 |
|------|---------|------|---------|
| **核心資料（Core）** | OHLCV 日K | Yahoo / FinMind | 6 項必要條件全部依賴 |
| **附加資料（Supplementary）** | 三大法人、融資融券 | FinMind only | 加分條件：法人 +20、融資 +5 |

### Yahoo vs FinMind 欄位對照

| FinMind 欄位 | Yahoo 欄位 | 系統使用到？ |
|-------------|-----------|------------|
| `open` | `Open` | 是 |
| `max` | `High` | 是（ATR、突破）|
| `min` | `Low` | 是（ATR）|
| `close` | `Close` | 是（所有指標）|
| `Trading_Volume` | `Volume` | 是（量能條件）|
| `Trading_money` | 無 | 否 |
| `spread` | 無 | 否 |
| `Trading_turnover` | 無 | 否 |

**結論：掃描所需的 OHLCV 五欄 Yahoo 完全具備。**  
用 Yahoo Bridge 時，6 項必要條件不受影響，損失最多為加分條件 +25 分。

---

## 三、設計決策（已確認）

### 3.1 margin_cache 架構

**決定：累積式（方案 B），與 price_cache 同架構**

- 一行一天，(stock_id, date) 複合主鍵，永久保留
- 無 TTL，透過清理機制保留最近 400 天
- 優點：掃描器需要最近 5 天資料，累積式可直接 `WHERE date >= N天前` 查詢，不需重複抓取
- 避免非交易日 TTL 過期觸發無效 API 呼叫

### 3.2 Yahoo Bridge 重跑邏輯

**決定：方案 C + 預先過濾（不存 flag，不追蹤完成狀態）**

- **預先過濾**：每次 Bridge 執行前，查詢 price_cache 取得今日已有資料的股票清單，只對尚未有資料的股票打 Yahoo API
- App 重啟後重跑：預先過濾自動跳過已完成的股票，成本 = 一次 price_cache 查詢（ms 級）
- 無需管理額外 flag，自清：`WHERE date = 今天` 明天自然查不到昨日資料
- **前提**：預先過濾是必要實作，不論是否有重啟問題

### 3.3 Supplementary 完成定義

**決定：動態分母 + 100% 門檻 + 排除清單顯示**

- **分母**：全市場活躍股票，排除 `delisted` 和 `no_update` 狀態
- **門檻**：排除後 100% 視為完成
- **no_update 處理**：第一次嘗試失敗 → 標記 `no_update` → 自動排除分母，分母動態收斂
- **UI 呈現**：完成後顯示排除清單（法人缺失 N 檔、融資缺失 N 檔），讓使用者知道實際覆蓋率

---

## 四、實作範圍

### 任務依賴圖

```
#1 save_prices() replace 參數
  └─ #2 Yahoo 批次抓取函式
       └─ #3 Worker Yahoo Bridge 階段
            └─ #4 Worker FinMind 接手 + Supplementary ←─ #5 margin_cache 表
                 ├─ #6 資料管理頁進度儀表板
                 └─ #7 選股雷達資料就緒提示
```

---

## 五、各任務詳細說明

### Task 1：`save_prices()` 加 `replace` 參數

**檔案：** `db/price_cache.py`

**改動：**
```python
def save_prices(stock_id: str, df: pd.DataFrame, replace: bool = False) -> None:
    """
    replace=False（預設）: INSERT OR IGNORE — Yahoo Bridge 使用，不覆蓋已有資料
    replace=True:          INSERT OR REPLACE — FinMind 使用，永遠以 FinMind 為準
    """
```

- 現有所有呼叫 `save_prices()` 的地方維持 `replace=False`（預設，不破壞現行行為）
- FinMind Worker 在 `_fetch_one()` 成功後改為 `save_prices(stock_id, df, replace=True)`

---

### Task 2：Yahoo 批次抓取函式

**檔案：** `data/data_source.py`（或新建 `data/yahoo_client.py`）

**新增函式：**
```python
def fetch_yahoo_closing_batch(
    stock_ids: list[str],
    target_date: date,
    batch_size: int = 100,
) -> dict[str, pd.DataFrame]:
```

**實作細節：**
- 使用 `yf.download(tickers, period="2d", auto_adjust=True)` 批次抓取
- 每批 ≤ 100 檔，避免 Yahoo 封鎖
- tickers 格式：`["2330.TW", "2317.TW", ...]`，上櫃股改為 `.TWO`
  - 先統一試 `.TW`，若回傳空則自動改 `.TWO`（參考現有 `_yf_symbol()` 邏輯）
- 只取 `target_date` 當日那一筆
- 欄位對齊系統標準格式：`{date, open, max, min, close, Trading_Volume}`
- 失敗的股票記錄 warning log，不中斷整批
- 回傳：`{stock_id: pd.DataFrame}`（每個 DataFrame 只有一行）

**注意事項：**
- `auto_adjust=True` 表示 Yahoo 會進行除權息調整，與 FinMind 原始資料有細微差異
- 實測 2330 OHLC 完全一致，Volume 差約 4%，對量能條件影響極小，可接受

---

### Task 3：Worker Yahoo Bridge 階段

**檔案：** `scheduler/prefetch.py`

**新增狀態屬性（`PrefetchWorker.__init__`）：**
```python
self.yahoo_bridge_mode: bool = False
self.yahoo_bridge_completed_at: datetime | None = None
self.yahoo_core_total: int = 0    # 本次 Bridge 需要抓的股票數
self.yahoo_core_done: int = 0     # 本次 Bridge 已完成的股票數
```

**觸發條件（`_run_loop` 內，優先於一般抓取邏輯）：**
```
時間 >= 13:45
AND resolve_latest_trading_day() < date.today()（FinMind 尚未更新）
AND yahoo_bridge_completed_at is None（今次啟動尚未完成過）
```

**執行邏輯：**
1. 進入 `yahoo_bridge_mode = True`
2. **預先過濾**：查 `price_cache`，找出今日無資料的股票清單（排除 delisted/no_update）
3. 若清單為空 → Bridge 完成，設 `yahoo_bridge_completed_at`，離開
4. 分批（100 檔）呼叫 `fetch_yahoo_closing_batch()`
5. 每批結果逐一 `save_prices(stock_id, df, replace=False)`
6. 更新 `yahoo_core_done` 計數
7. 全部完成 → 設 `yahoo_bridge_completed_at`，`yahoo_bridge_mode = False`

**限速：**
- Yahoo Bridge 不消耗 FinMind 配額，不受每小時限制
- 批次間加短暫 sleep（約 1-2 秒）避免 Yahoo 封鎖

**`status()` dict 新增欄位：**
```python
"yahoo_bridge_mode":         self.yahoo_bridge_mode,
"yahoo_bridge_completed_at": self.yahoo_bridge_completed_at,
"yahoo_core_total":          self.yahoo_core_total,
"yahoo_core_done":           self.yahoo_core_done,
```

---

### Task 4：Worker FinMind 接手 + Supplementary 階段

**檔案：** `scheduler/prefetch.py`

#### 4a. FinMind 接手邏輯

當 `resolve_latest_trading_day() == date.today()` 時（FinMind 已更新）：

**`_get_stale_stocks()` 調整：**
- 現有邏輯不變（missing → stale → fresh 優先順序）
- Yahoo Bridge 已抓的股票，若 FinMind 也有今日資料，兩者 OHLC 幾乎一致
- FinMind 以 `save_prices(replace=True)` 覆蓋，確保最終快取為 FinMind 官方資料
- 未被 Yahoo 覆蓋的股票（Bridge 失敗或遺漏）自然在 stale 清單最前面

**`_fetch_one()` 調整：**
- 成功後改呼叫 `save_prices(stock_id, df, replace=True)`

#### 4b. Supplementary 階段

核心價格資料（全市場）完成後，接續抓取附加資料：

**新增狀態屬性：**
```python
self.inst_supplementary_total: int = 0
self.inst_supplementary_done: int = 0
self.margin_supplementary_total: int = 0
self.margin_supplementary_done: int = 0
self.supplementary_completed_at: datetime | None = None
```

**抓取順序：**
1. 法人資料（`TaiwanStockInstitutionalInvestorsBuySell`）→ `inst_cache`
   - 新增 `_fetch_inst_today(stock_id)` 函式
   - 邏輯類比現有 `_fetch_one_fundamental()`
   - 失敗標記 `inst_no_update`，排除分母

2. 融資融券（`TaiwanStockMarginPurchaseShortSale`）→ `margin_cache`
   - 新增 `_fetch_margin_today(stock_id)` 函式
   - 同樣的 no_update 處理邏輯

**完成判斷：**
- `inst_supplementary_done / inst_supplementary_total == 1.0`（動態分母）
- `margin_supplementary_done / margin_supplementary_total == 1.0`
- 兩者皆完成 → 設 `supplementary_completed_at`

**`status()` dict 新增欄位：**
```python
"inst_supplementary_total":    self.inst_supplementary_total,
"inst_supplementary_done":     self.inst_supplementary_done,
"margin_supplementary_total":  self.margin_supplementary_total,
"margin_supplementary_done":   self.margin_supplementary_done,
"supplementary_completed_at":  self.supplementary_completed_at,
```

---

### Task 5：margin_cache 資料表與存取函式

**檔案：** `db/models.py`、`db/margin_cache.py`、`db/database.py`

#### `db/models.py` 新增：
```python
class MarginCache(Base):
    __tablename__ = "margin_cache"
    stock_id        = Column(String, primary_key=True)
    date            = Column(String, primary_key=True)  # YYYY-MM-DD
    margin_buy      = Column(Integer)   # 融資買進（千股）
    margin_sell     = Column(Integer)   # 融資賣出（千股）
    margin_balance  = Column(Integer)   # 融資餘額（千股）
    short_buy       = Column(Integer)   # 融券買進（千股）
    short_sell      = Column(Integer)   # 融券賣出（千股）
    short_balance   = Column(Integer)   # 融券餘額（千股）
    fetch_at        = Column(String)    # ISO timestamp
```

#### `db/margin_cache.py` 新增函式：
```python
def save_margin(stock_id: str, df: pd.DataFrame) -> None
    """儲存融資融券資料，INSERT OR IGNORE（不覆蓋）"""

def get_margin(stock_id: str, days: int = 5) -> pd.DataFrame
    """讀取近 N 天融資融券資料（供掃描器使用）"""

def get_stocks_needing_margin(all_ids: list[str]) -> list[str]
    """回傳今日無融資快取的股票清單（排除 no_update）"""

def get_margin_stats() -> dict
    """回傳統計資訊：total, done_today, no_update_count, newest_fetch"""

def get_margin_no_update_stocks() -> list[str]
    """回傳永久無融資資料的股票清單（供 UI 排除清單顯示）"""

def delete_old_margin(keep_days: int = 400) -> int
    """清理 400 天前的資料，回傳刪除筆數"""
```

#### `db/database.py`：
- 確認 `init_db()` 建立 `margin_cache` 表

---

### Task 6：資料管理頁進度儀表板

**檔案：** `pages/6_資料管理.py`

#### 新增狀態燈（在現有狀態燈 elif 鏈加入）：
```
🔵 Yahoo Bridge 進行中（13:45 起，已完成 XX / YY 檔）
✅ Yahoo Bridge 完成（HH:MM，共 XX 檔）→ 等待 FinMind 接手
```

#### 指標列擴充（現有 7 格，新增至兩列）：

**第一列（現有）：** 本小時已用、本小時剩餘、累計已抓、待更新股票、正在抓取、略過無資料、429 次數

**第二列（新增）：**

| 指標 | 計算方式 |
|------|---------|
| 核心資料 | `price_cache 今日筆數 / 活躍股票總數`（%）|
| 法人資料 | `inst_cache 今日筆數 / 動態分母`（%）|
| 融資資料 | `margin_cache 今日筆數 / 動態分母`（%）|

#### 進度條：
```
核心資料：[████████████░░░] 87%  (826/950 檔)
法人資料：[░░░░░░░░░░░░░░░]  0%  (等待核心資料完成)
融資資料：[░░░░░░░░░░░░░░░]  0%  (等待核心資料完成)
```
- 顏色：0-50% 紅、50-90% 橘、90-100% 綠、100% 綠（完成標示）

#### 排除清單（可展開，附加資料完成後顯示）：
```
⚠️ 以下股票無附加資料（已從計算中排除）
  法人缺失：00679B、00687B ... (N 檔)  [展開]
  融資缺失：1234、5678 ... (N 檔)       [展開]
```

---

### Task 7：選股雷達資料就緒提示

**檔案：** `pages/1_選股雷達.py`

**觸發條件：** 使用者選擇今天日期進行掃描時

**提示邏輯（不擋掃描，只顯示 info banner）：**

```python
if scan_date == date.today():
    core_pct = query_core_completion()  # 查 price_cache 今日比例

    if core_pct < 1.0:
        st.info(
            f"今日價格資料仍在更新中（{core_pct*100:.0f}%），"
            "掃描結果可能不完整。"
        )

    if not supplementary_ready():  # inst_cache + margin_cache 今日完成率
        st.info(
            "法人／融資資料尚未就緒，相關加分條件暫時停用。"
        )
        # 自動關閉法人與融資條件，不需使用者手動設定
        include_institutional = False
        include_margin = False
```

**歷史日期掃描：完全不觸發以上邏輯。**

---

## 六、不在此次範圍內

- `pages/2_持股監控.py`：DataSourceManager 會自動套用新的 Yahoo Bridge 快取，無需修改
- `pages/7_個股分析.py`：同上
- 回測模組：使用獨立的歷史資料路徑，不受影響
- FinMind 付費方案的 `TaiwanStockHoldingSharesPer`（大戶持股比例）：確認為付費功能，暫緩

---

## 七、測試重點

1. **Yahoo 批次抓取**：確認 yfinance 批次對台股 .TW / .TWO 的回傳完整度
2. **INSERT OR IGNORE vs INSERT OR REPLACE**：確認 FinMind 資料能正確覆蓋 Yahoo 資料
3. **預先過濾**：確認 app 重啟後不重複抓取已有資料的股票
4. **Supplementary 動態分母**：確認 no_update 標記與排除邏輯正確
5. **跨日邊界**：確認隔天 0:00 後所有進度重置正常

---

## 八、開工前確認清單

- [x] margin_cache 架構：累積式（與 price_cache 同架構）
- [x] Yahoo Bridge 重跑：方案 C + 預先過濾
- [x] Supplementary 完成定義：動態分母 100% + 排除清單顯示
- [x] Yahoo 批次抓取實際測試：20 檔全數回傳，多層 columns（Close/High/Low/Open/Volume）
- [x] FinMind margin 資料欄位確認：主要欄位為 `MarginPurchaseTodayBalance`、`MarginPurchaseSell`、`MarginPurchaseBuy`、`ShortSaleTodayBalance`、`ShortSaleSell`、`ShortSaleBuy`
  - ⚠️ 既有 bug：`compute_margin_trend()` 使用 `MarginPurchaseBalance`（不存在），應改為 `MarginPurchaseTodayBalance`，Task 5 一併修正
