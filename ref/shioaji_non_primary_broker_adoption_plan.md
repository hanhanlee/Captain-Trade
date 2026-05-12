# Shioaji / 永豐 API 導入計劃書（非主要庫存券商版本）

> 適用專案：台股交易輔助工具 / Srock  
> 導入前提：目前持股多數不在永豐帳戶，永豐 API 先作為行情、商品檔、交易計畫校正與輔助資料來源。  
> 不做事項：第一階段不做自動下單、不做庫存主同步、不讓 AI 或 Claude 直接操作券商 API。  
> 核心定位：**Broker-assisted market data & trade planning support**，不是 automated trading。

---

## 1. 參考資料

| 名稱 | 用途 | URL |
|---|---|---|
| Shioaji llms-full.txt | 官方整合文件，包含商品檔、登入、行情、帳務、限制等內容 | https://sinotrade.github.io/llms-full.txt |
| Shioaji GitHub | Shioaji Python SDK 原始碼與版本資訊 | https://github.com/Sinotrade/Shioaji |
| Shioaji 官方文件首頁 | 官方 API 文件入口 | https://sinotrade.github.io/ |
| Shioaji PyPI | Python package 安裝資訊 | https://pypi.org/project/shioaji/ |
| 永豐 API Token 申請文件 | 申請 API key / secret key | https://sinotrade.github.io/tutor/prepare/token/ |
| Shioaji 使用限制 | API 流量、查詢次數、連線數限制 | https://sinotrade.github.io/tutor/limit/ |

---

## 2. 導入前提

目前使用者有多家券商，且多數股票不在永豐帳戶，因此不應將「永豐庫存同步」列為第一優先。

第一階段不以永豐庫存作為真實持股來源，而是把 Shioaji 當成：

1. 即時行情來源
2. 商品檔來源
3. 可交易性檢查來源
4. 交易計畫校正來源
5. 盤中持股 / 觀察清單監控輔助
6. API 使用量監控來源
7. 未來多券商整合的第一個 broker adapter 範例

---

## 3. 不導入範圍

| 項目 | 第一階段是否導入 | 原因 |
|---|---:|---|
| 自動下單 | 否 | 風險高，目前無需求 |
| 取消單 / 改價 / 改量 | 否 | 屬於交易執行功能，暫不開放 |
| 永豐庫存作為主持股來源 | 否 | 多數持股不在永豐 |
| Shioaji MCP Server | 否 | 不讓 Claude 直接操作券商 API |
| AI 自動建立委託 | 否 | 與輔助決策定位不符 |
| 全市場即時掃描 | 否 | API 限制與效能風險高 |
| ticks 高頻訂閱 | 否 | 不符合目前工具定位 |
| 多券商即時下單整合 | 否 | 長期方向，非第一階段 |

---

## 4. Shioaji 可用能力摘要

根據官方文件，Shioaji 可支援：

| 類別 | 可用功能 |
|---|---|
| 商品檔 | 股票、期貨、選擇權、指數商品檔 |
| 股票商品欄位 | code、name、unit、limit_up、limit_down、reference、day_trade、融資餘額、融券餘額 |
| 行情 | quote subscribe、ticks、kbars、snapshots、scanners |
| 帳務 | list_positions、list_profit_loss、account_balance |
| 委託 | place_order、update_order、cancel_order、list_trades |
| 狀態 | api.usage() 查詢連線數、已用流量、剩餘流量 |

第一階段只使用：

```text
login
logout
usage
Contracts
snapshots
kbars
scanners（可選）
account_balance（可選，僅顯示永豐帳戶狀態）
list_trades（可選，僅未來對帳用）
```

第一階段不使用：

```text
place_order
update_order
cancel_order
subscribe_trade
ticks 高頻查詢
```

---

## 5. 使用限制與設計約束

官方文件列出以下限制，設計時必須遵守：

| 類型 | 限制 |
|---|---|
| 行情查詢 | credit_enquire、short_stock_sources、snapshots、ticks、kbars 合計 5 秒上限 50 次 |
| ticks | 盤中查詢 ticks 不得超過 10 次 |
| kbars | 盤中查詢 kbars 不得超過 270 次 |
| 帳務查詢 | account_balance、list_positions、list_profit_loss 等合計 5 秒上限 25 次 |
| 訂閱數 | api.subscribe() 數量上限 200 |
| 連線數 | 同一 person_id 最多 5 個連線 |
| 登入 | api.login() 一天上限 1000 次 |
| 流量 | 無 API 成交金額時每日流量限制可能為 500MB |

因此第一階段設計必須遵守：

```text
只監控持股 + 觀察清單 + 選股雷達候選股
不做全市場即時行情
不做 tick 級資料處理
所有查詢都要有 throttle / cache
所有 API 呼叫都要記錄 usage
```

---

## 6. 建議導入目標

### 第一階段目標

在不依賴永豐庫存的前提下，先完成：

1. Shioaji read-only adapter
2. 商品檔同步
3. 即時 snapshot 查詢
4. kbar 輔助查詢
5. 可交易性檢查
6. 交易計畫價格校正
7. 持股 / 觀察清單盤中監控
8. API usage dashboard
9. Event Log 串接

---

## 7. 目標架構

```text
Srock
  ↓
broker/
  shioaji_adapter.py
  broker_market_data.py
  broker_contract_store.py
  broker_usage_monitor.py
  tradeability_checker.py
  trade_plan_price_checker.py
  broker_safety_guard.py
  ↓
Streamlit UI
  - Broker API 狀態
  - 商品檔查詢
  - 可交易性檢查
  - 交易計畫校正
  - 盤中監控
```

---

## 8. Phase 1：Read-only Shioaji Adapter

### 8.1 目標

建立只讀 Shioaji adapter，不開放下單相關功能。

### 8.2 功能

| 功能 | 說明 |
|---|---|
| login | 使用 API key / secret key 登入 |
| logout | 關閉連線 |
| usage | 查詢 API 流量與連線狀態 |
| contracts_status | 確認商品檔下載狀態 |
| get_stock_contract | 查詢單一股票商品檔 |
| get_snapshots | 查詢持股 / 觀察清單 / 候選股即時快照 |
| get_kbars | 查詢單一股票 K 線資料 |
| health_check | 檢查登入、商品檔、usage 狀態 |

### 8.3 明確禁止

```python
def place_order(*args, **kwargs):
    raise RuntimeError("Order placement is disabled.")

def cancel_order(*args, **kwargs):
    raise RuntimeError("Order cancellation is disabled.")

def update_order(*args, **kwargs):
    raise RuntimeError("Order update is disabled.")
```

### 8.4 設定檔

```toml
[broker]
enabled = true
provider = "shioaji"
mode = "read_only"
allow_place_order = false
allow_cancel_order = false
allow_update_order = false

[shioaji]
simulation = false
fetch_contract = true
contracts_timeout = 10000
subscribe_trade = false
max_snapshot_symbols = 100
snapshot_refresh_seconds = 60
```

### 8.5 .env

```text
SHIOAJI_API_KEY=your_api_key
SHIOAJI_SECRET_KEY=your_secret_key
```

注意：

```text
.env 必須加入 .gitignore
API key 不得寫死在程式碼
不得把 key 印到 log
不得匯出到 Review Package
```

---

## 9. Phase 2：商品檔同步與可交易性檢查

### 9.1 為什麼重要

即使不自動下單，商品檔也很有價值。

Stock contract 可提供：

| 欄位 | 用途 |
|---|---|
| code | 股票代碼 |
| name | 股票名稱 |
| unit | 交易單位 |
| limit_up | 漲停價 |
| limit_down | 跌停價 |
| reference | 參考價 |
| day_trade | 是否可當沖 |
| margin_trading_balance | 融資餘額 |
| short_selling_balance | 融券餘額 |

### 9.2 可交易性檢查

新增 Tradeability Check：

| 檢查 | 說明 |
|---|---|
| 距離漲停 | 太接近漲停不追價 |
| 距離跌停 | 風險警示 |
| 今日參考價 | 計算盤中漲跌幅 |
| 是否可當沖 | 不用來當沖，但可作為市場熱度資訊 |
| 交易單位 | 整股 / 零股規劃 |
| 融資融券餘額 | 補充風險資訊 |
| 商品檔更新日 | 避免使用過期商品資訊 |

### 9.3 可交易性輸出範例

```json
{
  "stock_id": "6213",
  "stock_name": "聯茂",
  "reference": 90.0,
  "limit_up": 99.0,
  "limit_down": 81.0,
  "last_price": 96.5,
  "distance_to_limit_up_pct": 2.59,
  "distance_to_limit_down_pct": 16.06,
  "tradeability_status": "warning",
  "warnings": [
    "距離漲停小於 3%，不建議追價",
    "股價高於計畫價 4.8%"
  ]
}
```

---

## 10. Phase 3：交易計畫價格校正

### 10.1 背景

目前工具已有交易計畫與風控審查。  
Shioaji 可補上盤中即時價格與商品檔資訊。

### 10.2 功能

當使用者建立交易計畫時，系統自動檢查：

| 檢查 | 說明 |
|---|---|
| 即時價 vs 計畫價 | 是否已經偏離太多 |
| 即時價 vs 停損價 | 真實風險是否擴大 |
| 即時價 vs 漲停價 | 是否接近漲停，不適合追 |
| 當日漲跌幅 | 是否已經大漲 |
| 量能狀態 | 是否具備流動性 |
| ATR 過熱 | 是否超過 MA20 + N × ATR |
| 是否仍符合原始 Decision Packet | 策略條件是否仍有效 |

### 10.3 使用者顯示範例

```text
交易計畫價格校正

股票：6213 聯茂
計畫買進價：92.0
目前即時價：96.5
偏離計畫價：+4.89%
停損價：86.0
原計畫風險：6.52%
即時價計算風險：10.88%
距離漲停：2.59%

結論：
不建議以目前價格追價。
建議等待回落至計畫價附近，或重新建立交易計畫。
```

### 10.4 final_opinion

| 狀態 | 說明 |
|---|---|
| `price_ok` | 即時價格仍接近計畫價 |
| `price_warning` | 價格偏離，需人工確認 |
| `chase_risk` | 追高風險明顯 |
| `limit_up_risk` | 接近漲停 |
| `plan_invalidated` | 原交易計畫已失效 |

---

## 11. Phase 4：持股與觀察清單盤中監控

### 11.1 前提

因為永豐不是主要庫存券商，所以不以永豐庫存作為持股來源。

資料來源改成：

```text
Srock 既有持股資料
+
使用者手動建立的觀察清單
+
選股雷達候選股
+
Shioaji snapshots / kbars
```

### 11.2 監控範圍

| 類型 | 是否監控 |
|---|---|
| Srock 持股監控中的股票 | 是 |
| 交易計畫中的股票 | 是 |
| 選股雷達今日前 N 名 | 是 |
| 使用者觀察清單 | 是 |
| 全市場股票 | 否 |

### 11.3 監控頻率

建議：

| 類型 | 頻率 |
|---|---|
| 持股 | 每 1–3 分鐘 |
| 交易計畫 | 每 1–3 分鐘 |
| 觀察清單 | 每 3–5 分鐘 |
| 選股雷達候選 | 每 5–10 分鐘 |

### 11.4 警示類型

```text
price_cross_stop_loss
price_deviation_from_plan
near_limit_up
near_limit_down
intraday_pullback
intraday_break_ma
volume_spike
api_data_stale
```

---

## 12. Phase 5：API 使用量與健康狀態監控

### 12.1 API Usage Dashboard

顯示：

| 欄位 | 說明 |
|---|---|
| 是否登入 | Shioaji login 狀態 |
| 連線數 | api.usage().connections |
| 已使用流量 | api.usage().bytes |
| 每日限制 | api.usage().limit_bytes |
| 剩餘流量 | api.usage().remaining_bytes |
| 商品檔狀態 | Contracts.status |
| 最近 snapshot 時間 | 最後行情更新時間 |
| 最近錯誤 | API error message |
| 是否接近限制 | usage ratio warning |

### 12.2 警示門檻

```toml
[shioaji_usage_guard]
warn_usage_pct = 70
block_usage_pct = 90
max_login_per_day = 20
min_snapshot_interval_seconds = 30
```

### 12.3 Event Log

新增事件：

```text
broker_login_success
broker_login_failed
broker_logout
broker_usage_checked
broker_usage_warning
broker_usage_blocked
broker_contracts_loaded
broker_snapshot_updated
broker_snapshot_failed
broker_api_rate_limited
```

---

## 13. Phase 6：多券商持股整合預備

### 13.1 為什麼要預備

目前使用者有多家券商，永豐只是其中之一。  
未來若要整合庫存，不應讓 Shioaji 成為唯一 broker model。

### 13.2 建議抽象資料模型

```text
BrokerAccount
BrokerPosition
BrokerTrade
BrokerSnapshot
```

### 13.3 broker_positions 表

```sql
CREATE TABLE IF NOT EXISTS broker_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_name TEXT NOT NULL,
    account_id_masked TEXT,
    stock_id TEXT NOT NULL,
    stock_name TEXT,
    shares INTEGER,
    avg_price REAL,
    market_price REAL,
    market_value REAL,
    unrealized_pnl REAL,
    source_type TEXT,
    synced_at TEXT,
    raw_json TEXT
);
```

### 13.4 source_type

| source_type | 說明 |
|---|---|
| `api_shioaji` | 永豐 API |
| `manual` | 使用者手動輸入 |
| `csv_import` | 券商 CSV 匯入 |
| `future_broker_api` | 未來其他券商 API |

第一階段不啟用主同步，但先保留設計。

---

## 14. 建議資料表

### 14.1 broker_api_events

```sql
CREATE TABLE IF NOT EXISTS broker_api_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    broker_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    status TEXT,
    summary TEXT,
    payload_json TEXT
);
```

---

### 14.2 broker_snapshots

```sql
CREATE TABLE IF NOT EXISTS broker_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    broker_name TEXT NOT NULL,
    stock_id TEXT NOT NULL,
    stock_name TEXT,
    last_price REAL,
    reference_price REAL,
    limit_up REAL,
    limit_down REAL,
    open REAL,
    high REAL,
    low REAL,
    volume REAL,
    raw_json TEXT
);
```

---

### 14.3 tradeability_checks

```sql
CREATE TABLE IF NOT EXISTS tradeability_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    stock_id TEXT NOT NULL,
    stock_name TEXT,
    source TEXT,
    last_price REAL,
    reference_price REAL,
    limit_up REAL,
    limit_down REAL,
    distance_to_limit_up_pct REAL,
    distance_to_limit_down_pct REAL,
    status TEXT,
    warnings_json TEXT,
    payload_json TEXT
);
```

---

### 14.4 trade_plan_price_checks

```sql
CREATE TABLE IF NOT EXISTS trade_plan_price_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    trade_plan_id TEXT,
    stock_id TEXT NOT NULL,
    planned_entry_price REAL,
    current_price REAL,
    stop_loss_price REAL,
    price_deviation_pct REAL,
    risk_pct_at_plan_price REAL,
    risk_pct_at_current_price REAL,
    distance_to_limit_up_pct REAL,
    final_opinion TEXT,
    warnings_json TEXT,
    payload_json TEXT
);
```

---

## 15. 安全設計

### 15.1 Read-only Mode

所有 broker adapter 都必須檢查：

```python
if settings.broker.mode != "read_only":
    raise RuntimeError("Only read-only broker mode is allowed in this phase.")
```

### 15.2 禁用交易行為

```python
if not settings.broker.allow_place_order:
    raise RuntimeError("Order placement is disabled.")
```

### 15.3 不暴露敏感資訊

不得記錄：

```text
API key
secret key
person_id
完整 account_id
憑證路徑
密碼
```

只允許 masked account id：

```text
1234567 → ***4567
```

### 15.4 不放進 Review Package

Review Package 不得包含：

```text
券商帳號
API key
帳戶餘額細節
完整委託資料
個人身份資訊
```

可包含：

```text
是否通過風控
價格偏離
距離漲停
是否接近限制
```

---

## 16. 建議 UI 頁面

### 16.1 Broker API 狀態

```text
登入狀態
商品檔狀態
流量使用
剩餘流量
最近更新時間
錯誤訊息
```

### 16.2 可交易性檢查

```text
輸入股票代碼
顯示即時價
顯示參考價
顯示漲停 / 跌停
顯示距離漲停 / 跌停
顯示交易單位
顯示 day_trade
顯示警告
```

### 16.3 交易計畫校正

```text
選擇交易計畫
即時價格校正
風險重新計算
追價提醒
是否仍可建立計畫
```

### 16.4 盤中監控

```text
持股
觀察清單
今日候選股
交易計畫標的
```

---

## 17. 建議 CLI

```bash
srock broker status
srock broker usage
srock broker contracts 6213
srock broker snapshot 6213
srock broker check-tradeability 6213
srock broker check-plan <plan_id>
srock broker monitor --watchlist
```

---

## 18. Roadmap 總表

| Phase | 名稱 | 重點 | 是否依賴永豐庫存 |
|---|---|---|---:|
| Phase 1 | Read-only Adapter | login、usage、contracts、snapshots | 否 |
| Phase 2 | Tradeability Check | 商品檔、漲跌停、交易單位、追價風險 | 否 |
| Phase 3 | Trade Plan Price Check | 交易計畫即時價格校正 | 否 |
| Phase 4 | Intraday Monitor | 持股/觀察清單/候選股盤中監控 | 否 |
| Phase 5 | API Usage Dashboard | 流量、連線、限制監控 | 否 |
| Phase 6 | Multi-broker Model | 為未來多券商庫存整合預備 | 部分 |
| Phase 7 | Broker Position Sync | 只有永豐持股變多後才導入 | 是 |

---

## 19. 最建議的第一個 MVP

### MVP 名稱

```text
Shioaji Market Assist MVP
```

### MVP 功能

1. Shioaji 登入
2. API usage 顯示
3. 查單一股票商品檔
4. 查單一股票 snapshot
5. 顯示漲停 / 跌停 / 參考價
6. 顯示距離漲停 / 跌停
7. 對交易計畫做即時價格校正
8. 寫入 Event Log

### MVP 不做

```text
不查庫存
不做庫存同步
不下單
不訂閱 ticks
不全市場掃描
不讓 AI 呼叫 broker
```

---

## 20. 一句話總結

因為永豐目前不是主要持股券商，Shioaji 第一階段不應該從「庫存同步」開始。

更適合的順序是：

```text
行情與商品檔
    ↓
可交易性檢查
    ↓
交易計畫即時價格校正
    ↓
持股 / 觀察清單盤中監控
    ↓
API 使用量監控
    ↓
未來多券商持股整合
```

這樣可以先把永豐 API 的價值用在「輔助判斷與風控」，而不是過早綁定單一券商庫存。
