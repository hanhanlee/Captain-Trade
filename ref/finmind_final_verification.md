# FinMind 最終驗證結論（finmind_client(7).py）

## 總結

目前這版 `finmind_client(7).py` 已經把前面最重要的高風險問題修正到可接受程度，特別是：

- `TaiwanStockTradingDailyReport` 已走 dedicated special endpoint
- `TaiwanStockTradingDailyReportSecIdAgg` 已走 dedicated special endpoint
- `TaiwanStockTradingDailyReportSecIdAgg` 已改用 `data_id`，並支援 `securities_trader_id`
- `generic /api/v4/data` 對 special endpoint dataset 有 routing guard
- `all_by_date` 單日限制 guard 已存在
- request usage 已區分 `data` / `quota_probe`
- client 端盤中 Sponsor 限速已納入全域 limiter
- `broker main force` 已從 fail-fast 改為 partial failure tolerant
- `stock_info_cache` 更新已改成 staged refresh，且失敗會記錄 exception
- `get_broker_trading_daily_report_secid_agg()` 已將 `securities_trader_id` 對外暴露
- premium dataset 判斷已由 capability map 派生，避免雙份 hardcode 長期漂移

**結論：這版可視為接近最終可用版，沒有我目前會判定為「阻止上線」的明顯 high-risk bug。**

---

## 我認為已經可接受的部分

### 1. Special endpoint routing

目前 `TaiwanStockTradingDailyReport` 與 `TaiwanStockTradingDailyReportSecIdAgg` 都已明確走 dedicated wrapper，而不是 generic `_get("/api/v4/data")`。

這是目前最重要、也最正確的修正方向。

### 2. `TaiwanStockTradingDailyReportSecIdAgg` 實作

目前這段已具備：

- `_premium_gate(...)`
- `_wait_for_rate_limit()`
- `_note_http_request(kind=_REQUEST_KIND_DATA)`
- `data_id` 參數
- `securities_trader_id` optional
- `402 / 403` degraded handling
- `status != 200` 一致錯誤處理

這代表它已經不再是前幾版那種「孤島式補丁」。

### 3. Generic `/data` 保護

`_ensure_dataset_routing()` 已經能阻止 special / snapshot dataset 被靜默送去 generic `/api/v4/data`。

這對防止未來 agent 或人工維護時誤改非常重要。

### 4. All-by-date 單日限制

`_normalize_single_day_all_by_date()` 已對需要 single-day chunk 的 dataset 做 guard，避免不帶 `data_id` 時誤送寬日期區間。

### 5. Request accounting

`get_finmind_request_usage()` 已將：

- `data`
- `quota_probe`

拆開計數，並維持 `last_hour / total` 作為 data-only backward compatible 欄位。

這表示 worker 不會再被 quota probe 污染主要抓資料額度統計。

### 6. Partial failure 行為

`get_broker_main_force_series()` 已不再因單一日期失敗而整段中止，而是記錄失敗日期並繼續抓其他日期。

這對 production 穩定性是正確的。

### 7. Stock list refresh

`get_stock_list()` 已改成：

- temp stage table
- upsert 主表
- 最後清理不存在代碼
- exception logging

比早期 `DELETE + REPLACE` 安全很多。

---

## 目前剩下的注意事項（非阻擋級）

### A. `_build_dataset_sets()` 的 fallback 屬於 fail-open，建議改成更保守

目前 `_build_dataset_sets()` 在抓 `DATASET_CAP` 失敗時會回傳空集合：

- `_PREMIUM_DATASETS = set()`
- `_FUNDAMENTAL_DATASETS = set()`

這會讓 `_is_premium_dataset()` 失去作用，形成 **fail-open** 行為。

雖然在正常情況下 `finmind_capability_map.py` 已成功匯入，這件事大概率不會發生；但若未來 capability map 模組被改壞、部署不完整、或模組名稱變動，premium gate 可能被意外關掉。

**建議：**
- 最保守：改成 import 失敗就 `raise`
- 次佳：fallback 回「保守 hardcoded premium set」，不要 fallback 成空集合

這一點我列為**建議修**，不是目前的 blocker。

### B. `get_dataset_capability` 在 `_build_dataset_sets()` 內未使用

這是小型乾淨度問題，不影響正確性。可以刪掉函式內未使用的 import。

### C. client 與 worker 的交易時窗仍不是 100% 同一來源

client 仍用固定：

- `TRADING_HOURS_START_HHMM = (9, 0)`
- `TRADING_HOURS_END_HHMM = (15, 0)`

worker 那邊還有 adaptive window 機制。

這目前是「偏保守」而不是「偏錯誤」，所以不算 bug，但若你未來要完全一致，可以把 client 也接到同一個 settings source。

---

## 上線前最後驗證建議

### 1. 手動 smoke test：special endpoint

至少做下面幾個實際呼叫：

- `get_broker_trading_daily_report("2330", "2026-04-18")`
- `get_broker_trading_daily_report_secid_agg("2330", "2026-04-01", "2026-04-18")`
- `get_broker_trading_daily_report_secid_agg("2330", "2026-04-01", "2026-04-18", securities_trader_id="1160")`

驗證：

- 有資料時 DataFrame schema 是否合理
- 無資料時是否回空 DataFrame
- 權限不足時是否走 PremiumUnavailable / degraded 邏輯

### 2. 手動 smoke test：generic route guard

確認下面這類誤呼叫會被擋住：

- `_get("TaiwanStockTradingDailyReport", ...)`
- `_get("TaiwanStockTradingDailyReportSecIdAgg", ...)`
- `_get("taiwan_stock_tick_snapshot", ...)`

預期：應該 raise routing error，而不是偷偷送到 `/api/v4/data`。

### 3. 單日全市場 guard

驗證下面案例：

- `get_all_institutional_by_date("2026-04-18")` 應可正常
- 如果有人直接 `_get("TaiwanStockInstitutionalInvestorsBuySell", start_date="2026-04-01", end_date="2026-04-18")` 且不傳 stock_id，應被 single-day guard 擋下

### 4. Premium gate

測試三種情境：

- Premium 關閉
- Premium quota < 15%
- Premium runtime degraded

確認 premium dataset 呼叫會被正確阻擋。

### 5. Stock list refresh

測一次 `force_refresh=True`，並確認：

- refresh 成功時 cache 表正常更新
- refresh 失敗時 log 有 exception，不會靜默吞錯

---

## 我最後的判斷

### 可接受上線

- 是，這版已經到 **可接受上線 / 可進入最後驗證** 的程度。

### 我不再視為 blocker 的項目

- `TaiwanStockTradingDailyReportSecIdAgg` wrapper
- request usage 分流
- generic `/data` routing guard
- broker main force partial failure
- stock list refresh 方式

### 我仍會保留的提醒

- `_build_dataset_sets()` 不要 fail-open 成空集合
- client / worker 交易時窗若要完全一致，可再做一次整併

---

## 最後一句

如果你現在要進入最後驗證，我建議把重點放在：

1. special endpoint 實際呼叫結果
2. premium gate 邊界條件
3. generic route guard 是否真的能防誤用

而不是再大改架構。
