# FinMind 批次抓取能力整理 v2（重構 / Dispatcher 設計用）

> 更新日期：2026-04-21  
> 用途：協助重構 FinMind 抓取邏輯，讓 AI Agent 或人工維護者可依 capability map 精準分流。  
> 適用情境：你目前使用 `requests.get(...)` 自刻 API client，不依賴 FinMind Python SDK。

---

## 先講結論

FinMind 的「批次」不要只理解成一種能力。重構時至少要拆成四類：

1. **SDK fan-out 批次**  
   官方 Python SDK 提供 `*_id_list + use_async=True` 的語法糖。  
   這不代表 REST API 原生支援一個 request 帶多個 `data_id`。

2. **單日抓全市場（all_by_date）**  
   不帶 `data_id`，只帶 `start_date`，一次抓某一天全部資料，再在本地端過濾代號。

3. **特殊 endpoint 類**  
   不走一般 `/api/v4/data`，例如 snapshot 或特定籌碼資料。

4. **單檔 / 單日嚴格限制類**  
   例如部分資料集只能單檔、單日查詢，不能直接套用一般批次邏輯。

---

## 這份文件最重要的設計觀念

### 不要把 `async_multi_id` 理解成「API 原生 multi-id」

如果你的系統是直接用 `requests.get(FINMIND_API, params=...)` 打 REST API，則：

- 官方 SDK 的 `*_id_list + use_async=True`，本質上比較像 **client-side fan-out orchestration**
- 不等於 `/data` 可以直接送 `data_id=["2330", "2317"]`
- 真正原生支援多個 `data_id` 的，通常是少數特殊 endpoint（例如部分 snapshot）

因此在 capability map 裡，建議把原本的：

- `async_multi_id`

改成更不會誤導 Agent 的欄位，例如：

- `sdk_async_supported`
- `client_fanout_supported`

本文件後續以 `sdk_async_supported` 為主。

---

## 重構原則

### 原則 1：不要只靠資料集大類推斷

官方不同頁面對 batch 支援描述有落差：

- 總覽頁對某些類別寫不支援
- 但個別 dataset 頁面又新增了 `use_async=True` 或特殊批次能力

所以：

- **不要只用類別判斷**
- **一定要做 dataset 白名單**

### 原則 2：優先用 capability map 驅動 dispatcher

不要寫死成一大串散亂 if-else，應維護一份 `CAP`。

### 原則 3：對 `all_by_date` 強制做日期切片

只要 dataset 是 `all_by_date=True`，底層實作就應預設採用：

- **一天一天抓**
- 不要一口氣全市場抓整月 / 整季

這不是官方明文寫死的限制，而是**實務上的架構 guardrail**。  
原因是全市場資料量很大，若不帶 `data_id` 又給太寬的日期區間，實務上很容易遇到：

- 查詢過慢
- timeout
- 回傳過大
- 會員額度浪費

因此：

- `all_by_date=True` 不等於可以安全地抓長時間區間
- 正確做法是 **daily chunking**

---

## 建議 capability 欄位

建議每個 dataset 至少標以下欄位：

- `endpoint`: 使用的 API 路徑
- `query_mode`: `data`, `special_endpoint`, `snapshot`, `single_day_only` 等
- `sdk_async_supported`: 是否有官方 SDK `use_async=True` 類能力
- `all_by_date`: 是否支援不帶 `data_id` 的單日全市場抓法
- `single_day_only`: 是否只能單日查詢
- `special_endpoint`: 是否不是走一般 `/api/v4/data`
- `membership_gate`: 是否有限定會員等級
- `unique_key`: 寫入 DB 時建議的唯一鍵

範例：

```python
CAP = {
    "TaiwanStockPrice": {
        "endpoint": "/api/v4/data",
        "query_mode": "data_id_or_all_by_date",
        "sdk_async_supported": True,
        "all_by_date": True,
        "single_day_only": False,
        "special_endpoint": False,
        "membership_gate": None,
        "unique_key": ["date", "stock_id"],
    },
    "TaiwanStockInstitutionalInvestorsBuySell": {
        "endpoint": "/api/v4/data",
        "query_mode": "data_id_or_all_by_date",
        "sdk_async_supported": True,
        "all_by_date": True,
        "single_day_only": False,
        "special_endpoint": False,
        "membership_gate": "backer_or_sponsor_for_all_by_date",
        "unique_key": ["date", "stock_id", "name"],
    },
    "TaiwanStockTradingDailyReport": {
        "endpoint": "/api/v4/taiwan_stock_trading_daily_report",
        "query_mode": "special_endpoint",
        "sdk_async_supported": True,
        "all_by_date": False,
        "single_day_only": True,
        "special_endpoint": True,
        "membership_gate": "sponsor",
        "unique_key": ["date", "stock_id", "securities_trader_id", "price"],
    },
    "taiwan_stock_tick_snapshot": {
        "endpoint": "/api/v4/taiwan_stock_tick_snapshot",
        "query_mode": "snapshot",
        "sdk_async_supported": False,
        "all_by_date": False,
        "single_day_only": False,
        "special_endpoint": True,
        "membership_gate": None,
        "unique_key": ["date", "stock_id"],
    },
}
```

---

## A. 可當成「SDK fan-out / `sdk_async_supported=True`」白名單

> 這表示官方文件或個別 dataset 頁面已有 `*_id_list + use_async=True` 或等價能力。  
> 對你目前的 `requests` client 而言，這通常代表「可自行實作 client-side fan-out」，不是 REST 原生 multi-id。

### 1) 技術面

- `TaiwanStockPrice`
- `TaiwanStockWeekPrice`
- `TaiwanStockMonthPrice`
- `TaiwanStockPriceAdj`
- `TaiwanStockPriceTick`
- `TaiwanStockPER`
- `TaiwanStockDayTrading`
- `TaiwanStock10Year`
- `TaiwanStockKBar`
- `TaiwanStockPriceLimit`
- `TaiwanVariousIndicators5Seconds`
- `TaiwanStockEvery5SecondsIndex`

### 2) 籌碼面

- `TaiwanStockMarginPurchaseShortSale`
- `TaiwanStockInstitutionalInvestorsBuySell`
- `TaiwanStockShareholding`
- `TaiwanStockHoldingSharesPer`
- `TaiwanStockSecuritiesLending`
- `TaiwanStockMarginShortSaleSuspension`
- `TaiwanDailyShortSaleBalances`

### 3) 基本面

- `TaiwanStockFinancialStatements`
- `TaiwanStockBalanceSheet`
- `TaiwanStockCashFlowsStatement`
- `TaiwanStockDividend`
- `TaiwanStockDividendResult`
- `TaiwanStockMonthRevenue`
- `TaiwanStockCapitalReductionReferencePrice`
- `TaiwanStockDelisting`

### 4) 衍生性商品

- `TaiwanFuturesDaily`
- `TaiwanOptionDaily`
- `TaiwanOptionTick`
- `TaiwanFuturesInstitutionalInvestors`
- `TaiwanOptionInstitutionalInvestors`
- `TaiwanFuturesInstitutionalInvestorsAfterHours`
- `TaiwanOptionInstitutionalInvestorsAfterHours`
- `TaiwanFuturesDealerTradingVolumeDaily`
- `TaiwanOptionDealerTradingVolumeDaily`
- `TaiwanFuturesOpenInterestLargeTraders`
- `TaiwanOptionOpenInterestLargeTraders`
- `TaiwanFuturesSpreadTrading`
- `TaiwanFuturesFinalSettlementPrice`
- `TaiwanOptionFinalSettlementPrice`

---

## B. 可當成「單日抓全市場（all_by_date=True）」白名單

> 這一類最適合改成：  
> **按日期抓整市場一次 → 本地 filter 代號 → DB upsert**

### 1) 技術面

- `TaiwanStockPrice`
- `TaiwanStockWeekPrice`
- `TaiwanStockMonthPrice`
- `TaiwanStockPriceAdj`
- `TaiwanStockPriceTick`（單日）
- `TaiwanStockDayTrading`
- `TaiwanStock10Year`
- `TaiwanStockPriceLimit`

### 2) 籌碼面

- `TaiwanStockMarginPurchaseShortSale`
- `TaiwanStockInstitutionalInvestorsBuySell`
- `TaiwanStockShareholding`
- `TaiwanStockHoldingSharesPer`
- `TaiwanStockSecuritiesLending`
- `TaiwanStockMarginShortSaleSuspension`
- `TaiwanDailyShortSaleBalances`
- `TaiwanStockDispositionSecuritiesPeriod`
- `TaiwanStockDayTradingBorrowingFeeRate`

### 3) 基本面

- `TaiwanStockFinancialStatements`
- `TaiwanStockBalanceSheet`
- `TaiwanStockCashFlowsStatement`
- `TaiwanStockDividend`
- `TaiwanStockDividendResult`
- `TaiwanStockMonthRevenue`
- `TaiwanStockMarketValue`
- `TaiwanStockMarketValueWeight`

### 4) 衍生性商品

- `TaiwanFuturesDaily`
- `TaiwanOptionDaily`
- `TaiwanFuturesInstitutionalInvestors`
- `TaiwanOptionInstitutionalInvestors`
- `TaiwanFuturesInstitutionalInvestorsAfterHours`
- `TaiwanOptionInstitutionalInvestorsAfterHours`
- `TaiwanFuturesDealerTradingVolumeDaily`
- `TaiwanOptionDealerTradingVolumeDaily`
- `TaiwanFuturesOpenInterestLargeTraders`
- `TaiwanOptionOpenInterestLargeTraders`

---

## C. 不建議當成「一般 async 批次」處理的資料集

### 1) Info / 清單型資料

這類通常本身就是整包 metadata，不需要 multi-id fan-out：

- `TaiwanStockInfo`
- `TaiwanStockInfoWithWarrant`
- `TaiwanStockTradingDate`
- `TaiwanSecuritiesTraderInfo`
- `TaiwanFutOptDailyInfo`
- `TaiwanFutOptTickInfo`
- `TaiwanStockConvertibleBondInfo`
- `USStockInfo`

### 2) 總量 / 全市場彙總型資料

這類不是 per-stock / per-id，不要放進一般 multi-id dispatcher：

- `TaiwanStockTotalMarginPurchaseShortSale`
- `TaiwanStockTotalInstitutionalInvestors`
- `TaiwanTotalExchangeMarginMaintenance`

### 3) 可轉債類

文件總覽仍把 convertible bond 類列在 async batch 不支援，因此建議：

- 先不要放進一般 `sdk_async_supported=True` 白名單
- 需要時逐一驗證各 dataset

但注意：

- 有些可轉債 dataset 支援某一天全市場資料
- 所以它們比較像：
  - `sdk_async_supported = False`
  - `all_by_date = True`（僅部分 dataset）

### 4) 國際市場 / 匯率 / 利率

例如：

- `USStockPrice`
- `USStockPriceMinute`
- `TaiwanExchangeRate`
- `InterestRate`

若目前文件頁沒有明確 `*_id_list + use_async=True`，建議先視為：

- `sdk_async_supported = False`
- 走單一 `data_id` 迴圈

---

## D. 黑名單 / 強限制資料集（必須明列）

這一區很重要，因為最容易讓 AI Agent 自動推導錯誤。

### 1) `TaiwanStockTradingDailyReport` 不是「絕對不可批次」，但它是高風險特殊案例

建議標記為：

- `sdk_async_supported = True`
- `all_by_date = False`
- `single_day_only = True`
- `special_endpoint = True`

正確理解：

- 它**不能**用一般 `/data` 的 `all_by_date` 模式
- 它**不能**在單次請求中跨多日查詢
- 但官方文件有示範 `use_async=True` 的更新流程
- 它走的是專用 endpoint，不是一般 `/api/v4/data`

所以它的限制不是「不能批次」，而是：

- **只能單日**
- **不能 all_by_date**
- **要走特殊 handler**

如果你把它誤標成「絕對不可批次」，AI Agent 可能會把整條分點資料管線退化成最慢的手動逐檔流程。

### 2) Tick 類不能整類一刀切

不要寫成：

- `tick 類全部不能批次`

正確做法：

- 對每個 dataset 做白名單

例如：

- `TaiwanStockPriceTick`：已有批次能力整理
- `TaiwanOptionTick`：已有 `option_id_list + use_async=True`
- `TaiwanFuturesTick`：較接近單一 id / 單日查詢

### 3) Snapshot 類不要混進一般 `/data` dispatcher

例如：

- `taiwan_stock_tick_snapshot`
- `taiwan_futures_snapshot`
- `taiwan_options_snapshot`

這類應另開 handler，否則 Agent 很容易誤判其 request shape。

---

## E. 對 `requests` 自刻 client 的實作建議

### 1) 不要把 `aiohttp + asyncio` 寫成唯一解

如果你要模擬官方 SDK 的 fan-out 能力，可以考慮：

- `asyncio + aiohttp`
- `requests.Session() + ThreadPoolExecutor`
- 自製 bounded worker pool

真正該抽象的是：

- 併發上限
- quota / rate limit 保護
- retry / backoff
- 失敗重試與降級策略

### 2) 對回測系統來說，`all_by_date` 往往比複雜 async 更穩

若資料集同時支援：

- `sdk_async_supported=True`
- `all_by_date=True`

通常可以這樣選：

#### 策略 A：股票少、日期長

- 用 client fan-out（模擬 SDK async）

#### 策略 B：股票多、日期短

- 用 `all_by_date + daily chunking`

對回測補歷史資料而言，很多時候策略 B 更穩、更節省 request、也更容易 cache。

---

## F. `all_by_date` 的防禦機制：強制日切片

只要 `all_by_date=True`，建議 dispatcher 內部一律轉成：

```python
def fetch_all_by_date_chunked(dataset, start_date, end_date, **kwargs):
    frames = []
    for day in date_range(start_date, end_date):
        df = fetch_one_day_all_market(dataset, start_date=day, **kwargs)
        frames.append(df)
    return concat_frames(frames)
```

建議不要讓外層直接把：

- `start_date="2024-01-01"`
- `end_date="2024-01-31"`
- `data_id=None`

這種請求直接送給底層 API。

### 理由

- 全市場資料量大
- 單次查詢成本高
- 很容易超時或讓效能變差
- 對回測系統來說不穩定

### 建議規則

- `all_by_date=True` 時，底層永遠轉成逐日請求
- 上層不需要知道這個細節
- cache key 也以「dataset + single_day」為主

---

## G. DB 寫入策略：一定要用 UPSERT，不要單純 APPEND

既然走 `all_by_date`，一次拿回來的 DataFrame 可能是幾千筆到幾萬筆 row。

### 不建議的做法

- 單純 append
- 沒有 unique key 就直接寫入

風險：

- 同一天重抓資料時會重複寫入
- 回測指標可能因重複計算而失真
- 法人淨買超、成交量、分點資料都可能被灌水

### 建議做法

- 先為每個 dataset 定義 `unique_key`
- 寫入 DB 時採用 UPSERT
- 比起 SQLite 的 `INSERT OR REPLACE`，更建議：
  - `INSERT ... ON CONFLICT (...) DO UPDATE`

### 為什麼不要太依賴 `INSERT OR REPLACE`

因為 `REPLACE` 在 SQLite 語意上更接近：

- 先刪除舊列
- 再插入新列

若你的表有：

- foreign key
- trigger
- rowid 相依邏輯

就可能產生副作用。

### unique key 範例

- `TaiwanStockPrice` → `("date", "stock_id")`
- `TaiwanStockInstitutionalInvestorsBuySell` → `("date", "stock_id", "name")`
- `TaiwanStockTradingDailyReport` → `("date", "stock_id", "securities_trader_id", "price")`

> 注意：`unique_key` 仍應以你實際返回欄位與業務語意驗證後為準。

---

## H. 一定要特殊處理的例外

### 1) `taiwan_stock_tick_snapshot`

這不是一般 `/data`，而是專用 endpoint。

它支援：

- 單一 `data_id`
- `data_id = ["2330", "2317"]`
- `data_id = ""` 一次抓全部

因此建議：

- `special_endpoint = True`
- 另開 snapshot handler

### 2) `taiwan_futures_snapshot` / `taiwan_options_snapshot`

文件明確示範可一次抓全部，但沒有像台股 snapshot 那樣明示 list 型多 id。

因此建議先歸類：

- `sdk_async_supported = False`
- `special_endpoint = True`
- `all_now = True`（可自行新增欄位）

### 3) Tick 類不要以「整類規則」處理

- `TaiwanStockPriceTick`：已列入批次能力
- `TaiwanOptionTick`：已列入批次能力
- `TaiwanFuturesTick`：先當單檔 / 單日

---

## I. 如果你現在主要重構「法人 / 籌碼資料」，建議優先改法

下面這幾個最值得優先改成「按日期抓全市場，再本地 filter」：

- `TaiwanStockInstitutionalInvestorsBuySell`
- `TaiwanStockShareholding`
- `TaiwanStockMarginPurchaseShortSale`
- `TaiwanDailyShortSaleBalances`
- `TaiwanStockSecuritiesLending`

原因：

1. 一次通常會查很多股票
2. 一檔一檔抓很慢
3. 官方文件同時存在：
   - `*_id_list + use_async=True`
   - 不帶 `data_id` 的單日全市場抓法

實務上可選兩種策略：

### 策略 A：股票少、日期長

- 用 client fan-out
- 適合少量股票補長歷史

### 策略 B：股票多、日期少

- 用 `all_by_date + daily chunking`
- 適合選股池 / 回測批量補資料

---

## J. 建議實作策略

### 1) Dispatcher

```python
def fetch_dataset(dataset, ids=None, start_date=None, end_date=None, **kwargs):
    cap = CAP[dataset]

    if cap.get("special_endpoint"):
        return fetch_special_handler(dataset, ids=ids, start_date=start_date, end_date=end_date, **kwargs)

    if cap.get("all_by_date") and should_use_all_by_date(dataset, ids, start_date, end_date):
        return fetch_all_by_date_chunked(dataset, start_date=start_date, end_date=end_date, ids=ids, **kwargs)

    if cap.get("sdk_async_supported") and ids and len(ids) > 1:
        return fetch_client_fanout(dataset, ids=ids, start_date=start_date, end_date=end_date, **kwargs)

    return fetch_single_id_loop(dataset, ids=ids, start_date=start_date, end_date=end_date, **kwargs)
```

### 2) `should_use_all_by_date()`

```python
def should_use_all_by_date(dataset, ids, start_date, end_date):
    id_count = len(ids or [])
    day_count = count_days(start_date, end_date)

    # 只是示意，門檻請依會員方案、資料量、cache 命中率再調
    if id_count >= 20 and day_count <= 30:
        return True
    return False
```

### 3) 寫入策略

```python
def persist_dataframe(dataset, df, db):
    unique_key = CAP[dataset]["unique_key"]
    return upsert_dataframe(db=db, table=dataset, df=df, unique_key=unique_key)
```

---

## K. 建議你最後維護兩份名單

### 1) 穩定白名單（正式上線）

只放你已經：

- 文件確認過
- 實測確認過
- 欄位結構驗證過

的 dataset。

### 2) 觀察名單（待驗證）

把文件有提到、但你尚未實測過的 dataset 先放這裡。

這比單靠文件規則安全很多。

---

## L. 建議落地步驟

1. 列出目前程式有用到的所有 dataset  
2. 建立 `CAP` 對照表  
3. 先重構法人 / 籌碼資料路徑  
4. 補上：
   - retry
   - rate limit / quota 保護
   - request session 重用
   - cache
   - `all_by_date` 日切片
   - DB upsert
5. 最後再拆 snapshot / trading daily report / tick 類特殊 handler

---

## M. 參考來源

- FinMind Complete API / Dataset Reference  
  https://finmind.github.io/llms-full.txt
- FinMind API Overview  
  https://finmind.github.io/llms.txt
- FinMind 即時資料文件  
  https://finmind.github.io/tutor/TaiwanMarket/RealTime/
- FinMind 籌碼面文件  
  https://finmind.github.io/tutor/TaiwanMarket/Chip/

---

## N. 最後提醒

1. 官方文件不同頁面間，對 batch 支援描述確實存在落差。  
2. 這份文件適合當成 **重構初版設計規格**。  
3. 真正上線前，仍建議你以實測結果校正 CAP。  
4. 對 AI Agent 來說，最不容易產生幻覺的方式不是寫很多規則，而是：
   - dataset 白名單
   - capability map
   - 每個 dataset 的唯一鍵與 handler 類型

