# FinMind 目前邏輯對照與建議修正清單

這份文件是根據目前的 `finmind_client.py` 與 `prefetch.py` 實作，對照前面整理的 FinMind capability 規則後，彙整出的「可保留設計」與「建議修正點」。

目的不是要一次全部重寫，而是讓後續用 AI Agent（Cline / Roo Code / Codex 類工具）重構時，有一份可以直接照著落地的參考。

---

## 一、整體評估

目前的架構**方向大致正確**，不是推倒重來型。

你已經做對的幾件事：

1. **已經開始區分一般 `/data` 與特殊資料集的 Premium gate**。
2. **法人 / 融資已經有採用 `all_by_date` 的全市場單日抓取模式**，這和 FinMind 官方能力是對齊的。
3. **券商分點資料已經知道它是「單日限制」資料**，沒有誤當成一般長區間 dataset。
4. **已有快取優先、補抓缺段、背景 prefetch、Supplementary phase** 等正確方向。

所以這份 review 的重點，主要是修正幾個容易讓後面 dispatcher / prefetch / backfill 邏輯失真的地方。

---

## 二、建議保留的設計

### 1. 法人 / 融資改走 `all_by_date`

目前：
- `get_all_institutional_by_date(date_str)`
- `get_all_margin_by_date(date_str)`

這兩個函式都是不傳 `stock_id`，直接用單日抓全市場資料，這個方向是對的。

建議：**保留。**

後續若要重構，只需要把它們正式納入 capability map：

```python
"TaiwanStockInstitutionalInvestorsBuySell": {
    "all_by_date": True,
    "single_day_only_for_all_by_date": True,
}

"TaiwanStockMarginPurchaseShortSale": {
    "all_by_date": True,
    "single_day_only_for_all_by_date": True,
}
```

---

### 2. 券商分點資料已經知道是單日限制

`get_broker_trading_daily_report()` 已明確把查詢限制在單日，這個觀念是對的。

建議：**保留這個邏輯，但改 endpoint 與參數形式。**

---

### 3. `resolve_latest_trading_day()` 的快取設計

`resolve_latest_trading_day()` 用 2330 作為基準並加上 TTL，這個想法是合理的，能避免整個系統重複查最新交易日。

建議：**保留概念，不必推翻。**

但 fallback 細節要修。

---

## 三、優先修正（P0 / P1）

## P0-1. 目前把「超額 / 限流」幾乎都當成 429，但 FinMind 官方額度超限重點其實是 402

### 現況

- `prefetch.py` 的 `_is_429()` 只認 `429 / too many requests / rate limit`
- `_pause_for_rate_limit()` 的說明與控制流程也幾乎都綁在 429
- 但 `finmind_client.py` 的 `refresh_finmind_user_info()` 與 `_get()` 其實已經有在處理 `402/403`

### 問題

FinMind 官方文件明確寫到：**超過 API quota 時，會回 HTTP 402**，不是只會回 429。

所以現在的 worker 會出現一種情況：

- client 端其實已經進入 quota exhausted / degraded 狀態
- 但 worker 因為沒把 402 當成 rate-limit signal，所以**不一定會走 pause / cooldown**
- 結果就是：
  - UI 顯示與實際額度狀態不同步
  - 背景執行緒可能持續碰 premium gate / degraded state
  - 重建流程容易進入「一直失敗但不休眠」的狀態

### 建議修法

把 `_is_429()` 升級成更通用的 `_is_rate_limited()`：

```python
def _is_rate_limited(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        "429" in msg
        or "402" in msg
        or "too many requests" in msg
        or "rate limit" in msg
        or "upper limit" in msg
        or "quota" in msg
    )
```

並把 worker 內所有：
- `_is_429(e)`
- `_pause_for_rate_limit()`

統一改成更中性的語意，例如：
- `rate_limit_or_quota_pause`
- `quota_pause`
- `_is_rate_limited()`

### 結論

這一點是**最優先修**。因為它會影響整個背景 prefetch / backfill 的穩定度。

---

## P0-2. `TaiwanStockTradingDailyReport` 建議改成專用 endpoint，不要再當一般 `/data` dataset 使用

### 現況

目前 `get_broker_trading_daily_report()` 是透過：

```python
_get(
    "TaiwanStockTradingDailyReport",
    stock_id=stock_id,
    start_date=d,
    end_date=d,
)
```

也就是走統一的 `/api/v4/data` 包裝。

### 問題

根據 FinMind 官方文件，`TaiwanStockTradingDailyReport` 的正式 request 範例是走：

```text
/api/v4/taiwan_stock_trading_daily_report
```

而且它的語意是：
- sponsor only
- 單次只提供一天資料
- 可用 SDK async fan-out
- 屬於特殊 endpoint，不適合被當成一般 `/data` dataset 心智模型處理

如果繼續把它塞在一般 `_get()` 裡，後面 AI Agent 很容易誤判：
- 它跟 `TaiwanStockPrice` 類似
- 可以套統一 dispatcher
- 可以共用一般 `dataset + data_id + start_date + end_date` 邏輯

這在 capability map 角度是**錯誤抽象**。

### 建議修法

新增專用 wrapper：

```python
def get_broker_trading_daily_report_raw(stock_id: str, trade_date: str) -> pd.DataFrame:
    url = "https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {
        "data_id": stock_id,
        "date": trade_date,
    }
    ...
```

然後 `get_broker_trading_daily_report()` 再基於 raw wrapper 做欄位正規化。

### CAP 建議

```python
"TaiwanStockTradingDailyReport": {
    "endpoint": "/api/v4/taiwan_stock_trading_daily_report",
    "query_mode": "special_endpoint",
    "sdk_async_supported": True,
    "all_by_date": False,
    "single_day_only": True,
    "membership_gate": "sponsor",
}
```

### 結論

這一點是 **P0 / P1** 級別，因為它會直接影響後續 dispatcher 是否會長歪。

---

## P1-1. 現在有兩套 request 計數 / 限速系統，建議收斂成單一真實來源

### 現況

目前你同時有：

#### A. `finmind_client.py`
- `_wait_for_rate_limit()`
- `_request_times`
- `_requests_per_minute()`

#### B. `prefetch.py`
- `_hour_window`
- `_hour_count()`
- `_record_request()`
- `_current_hourly_limit()`

### 問題

這會產生幾個副作用：

1. **client 端已經有自己的節流器**，worker 又再做一次小時級節流。
2. 兩邊記帳時機不同：
   - client 是「送 request 前」記帳
   - worker 多半是「任務成功後」才 `_record_request()`
3. 某些子任務其實會打出多個 API，但 worker 只記一筆。
4. 最後會導致：
   - UI 顯示的 `hour_fetched` 不等於真實 request 數
   - worker 以為還有額度，其實 client limiter 已經在擋
   - 或反過來：worker 先停了，但 client 其實還有 quota

### 建議修法

二選一：

#### 方案 A（推薦）
把 **真實 request accounting 全部收斂到 `finmind_client.py`**，worker 只看 client 暴露出來的 quota 狀態。

例如：

```python
class FinMindRateLimiter:
    def acquire(...): ...
    def note_response(...): ...
    def current_usage(...): ...
    def current_quota(...): ...
```

worker 不再自己 `_record_request()`，只讀共享 limiter 狀態。

#### 方案 B
保留雙層，但明確區分：
- client limiter = 真實 HTTP request 限速
- worker counter = 只做 UI 進度顯示，不作為 quota 判斷依據

### 結論

目前這個問題不是立即爆炸型 bug，但它是後面所有 prefetch / backfill 調校變複雜的根源之一，**很值得盡快收斂**。

---

## P1-2. `_prefetch_one_premium_stock()` 會嚴重低估實際 request 數

### 現況

`_prefetch_one_premium_stock()` 會平行跑：
- `get_stock_risk_flags(...)`
- `get_holding_shares(...)`
- `smart_get_fundamentals(...)`
- `get_broker_main_force_series(...)`

而完成一個 task 後，只做一次：

```python
self._record_request()
```

### 問題

這裡的「一個 task」不等於「一次 HTTP request」。

例如：
- `smart_get_fundamentals()` 內部可能打 **3 個 dataset**
- `get_broker_main_force_series()` 可能為缺漏日期逐日打 **多次 daily report**
- `get_stock_risk_flags()` 內部可能組合多個 premium dataset

所以對 worker 來說：
- UI 上看起來只用了 4 次 request
- 實際上可能已經打了 10~40 次 request

這會直接導致：
- `_hour_count()` 嚴重失真
- premium prefetch 的配額管理不可信
- backfill 節奏難以調整

### 建議修法

#### 方案 A（推薦）
把「真實 request 計數」移到 `finmind_client._get()` / 特殊 endpoint wrapper 內。

例如：

```python
def _get(...):
    shared_rate_limiter.acquire()
    resp = requests.get(...)
    shared_rate_limiter.note_response(resp)
    return ...
```

這樣不管上層邏輯怎麼組合，request 數都是真的。

#### 方案 B
若暫時不重構 limiter，至少要把 `_prefetch_one_premium_stock()` 的 `_record_request()` 刪掉，避免給出錯誤的精確感。

### 結論

這一點和前一條是連動的，建議一起修。

---

## P1-3. Broker backfill 現在可能會「太早跑」，並把尚未更新的日期誤標成 `no_data`

### 現況

主迴圈中只要：

```python
if self.premium_broker_backfill_mode and not self._within_trading_hours():
    result = self.prefetch_market_broker_by_date(...)
```

也就是說，只要不在你定義的 trading-hours 內，就可能開始 broker backfill。

但你的 `_within_trading_hours()` 預設大約在 15:00 左右就結束，而 FinMind 官方文件對 `TaiwanStockTradingDailyReport` 的更新時間寫的是：**平日 21:00**。

### 問題

如果 broker backfill 在 15:00~21:00 之間跑：

1. 當天分點資料其實還沒更新
2. `get_broker_main_force_series(stock_id, [trade_date])` 會回空
3. `_prefetch_one_broker_date()` 會把它記成：

```python
status = "no_data"
```

4. `_filter_market_broker_missing()` 又把 `status in ('ok', 'no_data')` 當成已完成
5. 結果：**這一天就不會再重抓了**

### 這是實際邏輯 bug

這不是單純優化，而是真的可能造成：
- 當日 broker 資料永遠缺失
- 但系統卻以為已完成

### 建議修法

至少二選一：

#### 方案 A（推薦）
Broker backfill 加一個 dataset-ready guard：

```python
BROKER_READY_HHMM = (21, 15)
```

沒到這個時間，不跑當日 broker backfill。

#### 方案 B
在 official update window 之前，空資料不要寫成 `no_data`，而是寫：
- `pending`
- `not_ready`
- `pre_update_empty`

然後 `_filter_market_broker_missing()` 只把 `ok` 視為完成，或把 `no_data` 視為「官方更新時間之後才可終態」。

### 結論

這一點我會列成 **P1 高優先**，因為它會讓回測最重要的主力資料 silently missing。

---

## 四、中優先修正（P2）

## P2-1. `_latest_trading_day()` 的 fallback 不應該在平日直接回 `today`

### 現況

`prefetch.py` 的 `_latest_trading_day()` 在委派 `resolve_latest_trading_day()` 失敗時：
- 週六回週五
- 週日回週五
- 其他平日直接回 `today`

### 問題

如果 fallback 發生在：
- 平日收盤前
- 平日資料尚未更新前
- client 暫時出錯時

那 worker 會把「今天」當成 latest trading day，容易導致：
- 抓當日尚未完成的 supplementary
- broker / premium 判斷提早
- no_data 被過早寫入

### 建議修法

fallback 應該盡量模仿 `resolve_latest_trading_day()` 的保守策略：
- 平日失敗時，優先退回上一個交易日
- 不要直接回 today

### 結論

不是最致命，但建議修。

---

## P2-2. `INSERT OR REPLACE` 建議逐步升級為 `ON CONFLICT DO UPDATE`

### 現況

目前看到至少有：
- `stock_info_cache`
- `premium_fetch_status`

都用 `INSERT OR REPLACE`

### 問題

SQLite 的 `REPLACE` 實際語意比較接近：
- 先刪掉舊 row
- 再插入新 row

如果未來這些表：
- 掛 foreign key
- 有 trigger
- 需要保留 row identity

那 `REPLACE` 會比較危險。

### 建議修法

未來逐步改成：

```sql
INSERT INTO ...
ON CONFLICT(pk...)
DO UPDATE SET ...
```

### 結論

這是資料層健壯性優化，不是立即 blocking。

---

## P2-3. `get_stock_list()` 的註解和實作有小落差

### 現況

註解寫：
- 「取最舊的 updated_at 判斷是否過期」

但實作其實是：

```python
latest_str = max(r[3] for r in result if r[3])
```

也就是取最新，而不是最舊。

### 問題

這不一定會造成 bug，但會讓後續 AI Agent 在理解 cache TTL 時被誤導。

### 建議修法

擇一：
- 改註解
- 或真的改成用最舊時間判斷整批快取是否完整更新

---

## 五、可選強化（P3 / 架構優化）

## P3-1. 把 capability map 直接落地到程式碼

建議你下一步真的不要只保留在 MD，直接把它變成程式內的常數，例如：

```python
CAP = {
    "TaiwanStockPrice": {
        "endpoint": "/api/v4/data",
        "all_by_date": True,
        "single_day_only_for_all_by_date": True,
        "sdk_async_supported": True,
    },
    "TaiwanStockInstitutionalInvestorsBuySell": {
        "endpoint": "/api/v4/data",
        "all_by_date": True,
        "single_day_only_for_all_by_date": True,
        "sdk_async_supported": True,
    },
    "TaiwanStockTradingDailyReport": {
        "endpoint": "/api/v4/taiwan_stock_trading_daily_report",
        "special_endpoint": True,
        "single_day_only": True,
        "sdk_async_supported": True,
        "all_by_date": False,
    },
}
```

這樣後續 dispatcher / backfill policy / DB writer policy 都能統一從 CAP 讀，不需要散落在 if-else 中推理。

---

## P3-2. `all_by_date` 類 dataset 加上強制 daily chunk guard

雖然你現在法人 / 融資的 `get_all_*_by_date()` 都是單日查詢，方向正確，
但若未來 Agent 自動重構新的 dataset wrapper，建議在共用層直接加防呆：

```python
if cap[dataset]["all_by_date"]:
    assert start_date == end_date or end_date in (None, "")
```

避免未來有人直接把整月全市場資料一次丟進去。

---

## P3-3. Snapshot 類若未來要做批量，請獨立 wrapper

目前 `get_realtime_stock_snapshot(stock_id)` 是單一股票 wrapper。

如果未來你想利用 sponsor 能力做：
- `data_id=""` 全市場 snapshot
- 或多檔 snapshot

建議另外做：
- `get_realtime_stock_snapshot_one(stock_id)`
- `get_realtime_stock_snapshot_many(stock_ids)`
- `get_realtime_stock_snapshot_all()`

不要共用同一個函式簽名，避免 Agent 誤判回傳型別。

---

## 六、建議的重構優先順序

### 第一批：先修會影響正確性的
1. **把 402 也納入 rate-limit / quota exhausted 判斷**
2. **把 `TaiwanStockTradingDailyReport` 改成 special endpoint wrapper**
3. **修 broker backfill 的「太早跑 + no_data 當完成」問題**
4. **讓 request accounting 有單一真實來源**

### 第二批：再修穩定性與可維護性
5. `_latest_trading_day()` fallback 改保守
6. `INSERT OR REPLACE` 逐步改成 `ON CONFLICT DO UPDATE`
7. 註解與實作不一致的地方修齊

### 第三批：再做架構整理
8. 把 capability map 寫進 Python 常數檔
9. 把 dispatcher / backfill / writer policy 全部改成讀 CAP

---

## 七、我對目前架構的結論

**你的方向不是錯，甚至大方向是對的。**

真正需要調整的不是「你有沒有理解 FinMind」，而是：

1. **特殊 dataset 要不要抽離成 special endpoint wrapper**
2. **quota / request accounting 要不要只保留一套真實來源**
3. **`no_data` 何時可以視為終態，何時只能視為暫時未更新**

只要把這 3 件事修好，你現在這套快取 + prefetch + supplementary + premium backfill 的結構，其實就已經很接近可長期維護版本了。

---

## 八、下一步建議

下一步最值得做的是：

1. 先建立 `finmind_capability_map.py`
2. 再建立 `finmind_endpoints.py`，把特殊 endpoint 從 `_get()` 抽離
3. 最後把 worker 的 request 計數改成共用 limiter

這樣之後就算交給 AI Agent 接手，也比較不容易把邏輯改壞。
