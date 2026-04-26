# Event Log 設計說明

> 目的：把系統中發生過的重要事件，用統一格式記錄下來，作為未來 AI 分析、回測、績效檢討與除錯的基礎。

---

## 1. Event Log 是什麼？

Event Log 可以理解成系統的「黑盒子紀錄器」。

它不是用來取代現有的交易紀錄、持股資料或掃描紀錄，而是用來記錄：

- 什麼時間發生了什麼事
- 是哪個模組產生的事件
- 是否跟某支股票有關
- 當時使用了什麼策略與參數
- 當時系統判斷結果是什麼
- 使用者後來做了什麼決策

簡單說：

| 類型 | 目的 |
|---|---|
| 一般資料表 | 記錄目前狀態或功能資料 |
| Event Log | 記錄曾經發生過的事件與決策軌跡 |

---

## 2. 為什麼需要 Event Log？

如果只記交易結果，未來只能知道：

- 買了什麼
- 賣了什麼
- 賺多少
- 虧多少

但無法回答：

- 這支股票當初為什麼被選出來？
- 當時用的是哪一版策略？
- 當時有哪些條件被勾選？
- 系統是否有發出警示？
- 使用者是否忽略警示？
- 風控是否曾經擋下這筆交易？
- 被忽略的訊號後來表現如何？
- 被風控擋下的交易後來是漲還是跌？

Event Log 的價值在於：  
**把「結果」往前追溯到「訊號、策略、風控、推播、使用者決策」的完整流程。**

---

## 3. 最重要的原則

每一次選股掃描，都要記錄：

```text
strategy_name
strategy_version
settings_snapshot
data_status_snapshot
```

也就是說，不只要記「v4 入選」，還要記錄當時的完整策略設定。

因為今天的 v4 不一定等於三個月後的 v4。

例如這些設定未來都可能改變：

- RS 門檻：80 → 75
- 量比門檻：1.5 → 1.3
- ATR 過熱倍數：3.5 → 4.5
- 主力連買天數：3 日 → 2 日
- 掃描範圍：前日量前 100 → 前日量前 200
- 法人條件：加分 → 必要條件
- 產業輪動：開啟 → 關閉

如果不保存當時設定，未來回測和 AI 分析會失真。

---

## 4. 建議先記錄哪些事件？

第一版不用做太複雜，先記這幾種就很有價值。

| event_type | 說明 |
|---|---|
| `scan_completed` | 選股雷達掃描完成 |
| `stock_selected` | 某支股票入選 |
| `near_miss` | 某支股票接近入選但差幾個條件 |
| `alert_triggered` | 持股監控觸發警示 |
| `notification_sent` | LINE / Telegram 推播已送出 |
| `trade_plan_created` | 使用者建立交易計畫 |
| `risk_check_passed` | 交易計畫通過風控 |
| `risk_check_failed` | 交易計畫未通過風控 |
| `trade_executed` | 使用者登錄實際交易 |
| `user_cancelled` | 使用者取消交易計畫 |
| `user_ignored` | 使用者忽略警示或訊號 |

---

## 5. 建議資料表設計

### 5.1 最小版：只新增一張 event_log

```sql
CREATE TABLE IF NOT EXISTS event_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    module TEXT,
    scan_id TEXT,
    stock_id TEXT,
    stock_name TEXT,
    severity TEXT DEFAULT 'info',
    summary TEXT,
    payload_json TEXT
);
```

### 欄位說明

| 欄位 | 說明 |
|---|---|
| `created_at` | 事件發生時間 |
| `event_type` | 事件類型，例如 `stock_selected` |
| `module` | 來源模組，例如 `scanner`、`portfolio`、`risk` |
| `scan_id` | 若事件來自某次掃描，用來關聯掃描批次 |
| `stock_id` | 股票代碼，可空 |
| `stock_name` | 股票名稱，可空 |
| `severity` | `info`、`warning`、`danger` |
| `summary` | 給人看的簡短描述 |
| `payload_json` | 詳細內容，使用 JSON 保存 |

---

## 6. payload_json 應該記什麼？

不同事件需要記的資料不同，所以細節建議放在 `payload_json`。

### scan_completed 範例

```json
{
  "scan_id": "20260426_144500_v4",
  "strategy_name": "v4_leading_breakout",
  "strategy_version": "v4.2",
  "base_date": "2026-04-26",
  "scan_mode": "standard",
  "universe_count": 100,
  "selected_count": 5,
  "settings": {
    "rs_score_min": 80,
    "volume_ratio_min": 1.5,
    "atr_overheat_multiplier": 3.5,
    "main_force_buy_days": 3,
    "use_industry_rotation": true,
    "top_industry_count": 3,
    "use_institutional_filter": true,
    "institutional_filter_required": false,
    "use_margin_filter": true,
    "use_fundamental_filter": false
  },
  "data_status": {
    "finmind_mode": "sponsor",
    "institutional_data_ready": true,
    "main_force_cache_ready": true,
    "margin_cache_ready": true
  }
}
```

### stock_selected 範例

```json
{
  "scan_id": "20260426_144500_v4",
  "strategy_name": "v4_leading_breakout",
  "strategy_version": "v4.2",
  "rank": 1,
  "score": 130,
  "metrics": {
    "close": 92.1,
    "change_pct": 6.8,
    "volume_ratio": 2.1,
    "rs_score": 88,
    "atr14": 2.3,
    "ma20": 84.5
  },
  "required_rules_passed": {
    "first_day_cross_ma_5_10_20": true,
    "ma_convergence": true,
    "volume_breakout": true,
    "atr_overheat_guard": true,
    "rs_score": true,
    "new_60d_high": true,
    "main_force_3d_buy": true
  },
  "bonus_rules_hit": {
    "bollinger_bandwidth_shrink": true,
    "trust_first_buy": false,
    "margin_balance_decrease": true,
    "institutional_volume_ratio_10pct": true
  }
}
```

### risk_check_failed 範例

```json
{
  "planned_action": "buy",
  "planned_price": 92,
  "shares": 1000,
  "stop_loss": 86,
  "risk_amount": 6000,
  "risk_percent": 2.4,
  "max_allowed_risk_percent": 2.0,
  "reason": "單筆風險超過上限"
}
```

---

## 7. 建議的共用函式

所有模組都應該透過同一個函式寫入事件，避免每個地方各寫各的。

```python
import json
import sqlite3
from datetime import datetime

def log_event(
    db_path,
    event_type,
    module=None,
    scan_id=None,
    stock_id=None,
    stock_name=None,
    severity="info",
    summary="",
    payload=None,
):
    payload_json = json.dumps(payload or {}, ensure_ascii=False)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            '''
            INSERT INTO event_log (
                created_at,
                event_type,
                module,
                scan_id,
                stock_id,
                stock_name,
                severity,
                summary,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                event_type,
                module,
                scan_id,
                stock_id,
                stock_name,
                severity,
                summary,
                payload_json,
            ),
        )
        conn.commit()
```

---

## 8. 實作順序建議

### Phase 1：先開始記錄

先新增 `event_log` 表與 `log_event()` 函式。

接上三個地方：

1. 選股雷達掃描完成 → `scan_completed`
2. 每檔入選股票 → `stock_selected`
3. 持股警示觸發 → `alert_triggered`

### Phase 2：加入策略快照

每次掃描時建立一個 `scan_settings` dict，完整保存：

- 策略名稱
- 策略版本
- 掃描模式
- 基準日
- 掃描範圍
- 所有 checkbox 狀態
- 所有門檻參數
- 資料來源狀態

### Phase 3：加入交易計畫與風控事件

新增：

- `trade_plan_created`
- `risk_check_passed`
- `risk_check_failed`
- `trade_executed`
- `user_cancelled`

### Phase 4：建立 Event Log 查詢頁

在 Streamlit 加一個頁面：

- 依日期查詢
- 依股票查詢
- 依事件類型查詢
- 顯示事件時間線
- 可展開查看 payload_json

---

## 9. 未來可以回答的問題

有 Event Log 後，系統或 AI 可以回答：

- 最近 30 天哪些股票曾經入選？
- v4 入選後 5 日平均報酬是多少？
- 哪些條件最常造成 near miss？
- 被風控擋下的交易後來表現如何？
- 使用者忽略的警示後來是否造成虧損？
- 哪些 LINE 推播是真的有用？
- 我是否常常在過熱時追高？
- 我是否常常違反自己的風控規則？
- 哪一版策略的候選股後續表現最好？

---

## 10. 簡短結論

Event Log 的重點不是多存一份資料，而是建立完整的決策時間線。

最重要的原則是：

```text
每一次掃描，都要保存：
策略名稱 + 策略版本 + 參數快照 + 資料狀態快照
```

這樣未來不管做 AI 分析、回測、績效檢討或除錯，才知道當時的判斷是怎麼來的。
