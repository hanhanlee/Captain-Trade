# FinMind 下一輪可再改善的備忘錄

這份備忘錄只整理目前 **不阻擋上線**、但值得在下一輪重構或清理時一起處理的項目。  
目前版本已完成主要修正，並通過 smoke test；以下內容屬於可延後改善事項。

## 1. `securities_trader_id` 的函式簽名可再更明確

目前 `get_broker_trading_daily_report_secid_agg(...)` 與 `_get_broker_trading_daily_report_secid_agg_raw(...)`  
雖然在執行時已經把 `securities_trader_id` 視為必填，缺值會直接丟 `ValueError`，但函式簽名仍然保留：

```python
securities_trader_id: str = ""
```

這代表：
- 執行語意上是必填
- 型別與函式表面上看起來卻像可省略

### 下次可考慮改法
把簽名改成真正的必填參數，例如：

```python
def get_broker_trading_daily_report_secid_agg(
    stock_id: str,
    start_date: str,
    securities_trader_id: str,
    end_date: str = "",
) -> pd.DataFrame:
```

### 為什麼這只是非阻擋級
因為目前執行時 guard 已存在，功能正確，外部若漏傳會在本地端就被擋住，不會把錯誤送到 API 層。

---

## 2. `_wait_for_rate_limit()` 的註解可更新

目前相關註解仍偏向：

- `/api/v4/data requests`
- generic endpoint 的語意

但實際上這個 limiter 現在也用在：

- `taiwan_stock_trading_daily_report`
- `taiwan_stock_trading_daily_report_secid_agg`
- `taiwan_stock_tick_snapshot`

也就是說，它已經是 **client-wide FinMind HTTP limiter**，不只是 `/data` limiter。

### 下次可考慮改法
把註解改成更精準的敘述，例如：

- shared limiter for all FinMind HTTP requests initiated by this client
- includes `/api/v4/data` and dedicated special endpoints

### 為什麼這只是非阻擋級
因為程式行為本身是對的，影響的是可讀性與後續維護理解，不影響正確性。

---

## 3. client 與 worker 的交易時窗來源仍未完全統一

目前 client 端仍使用固定時窗：

- `09:00–15:00`

而 worker 端則有較動態／偏自適應的邏輯。  
這會造成：

- client 的限速策略偏保守
- worker 與 client 的時窗來源不完全一致

### 下次可考慮改法
讓 client 的 `_within_market_request_window()` 也改成讀取同一套設定來源，例如：

- `db.settings`
- `get_prefetch_optimal_time()`
- 或統一抽成 shared market-window helper

### 為什麼這只是非阻擋級
目前這種不一致是 **偏保守，不偏錯誤**。  
也就是說，最壞情況通常只是 client 比較慢，不會導致資料錯抓或路由錯誤。

---

## 4. capability map 與 runtime 行為可再持續對齊文件變化

目前 capability map 已可作為單一真實來源，這是好的方向。  
但 FinMind 某些 dataset 的文件與實際行為過去曾出現變動或描述落差。

### 下次可考慮改法
每次要新增新 dataset 或擴充新路由時，固定檢查：

- 官方 tutor 頁面
- 專用 endpoint 是否已公開文件化
- 是否有 `all_by_date`
- 是否有 `single_day_only`
- 是否有額外 required parameter

### 為什麼這只是非阻擋級
這不是當前 bug，而是未來擴充時的流程提醒。

---

## 5. 最後建議的維護策略

建議把目前這套規則固定成以下流程：

1. capability map 先定義 dataset 行為
2. generic `_get()` 僅處理 `endpoint_type="data"`
3. special / snapshot 一律經 dedicated wrapper
4. required parameter 在本地端先做 guard
5. smoke test 至少覆蓋：
   - success path
   - missing required parameter
   - premium disabled
   - quota low
   - generic route misuse

這樣之後即使交給較弱的 agent 幫忙改，也比較不容易把已修好的邏輯改壞。

---

## 目前狀態結論

目前版本的主要高風險問題已修正完成，並已通過你實際執行的 smoke test。  
這份備忘錄中的項目可以等下一輪整理、重構或文件同步時再一起處理，不需要阻擋這次上線。
