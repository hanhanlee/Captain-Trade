# Event Log 實作計畫

> 依據 `ref/event_log_design.md` 設計，針對 srock tool 現有架構（SQLAlchemy + Streamlit）制定。
> 撰寫日期：2026-04-26

---

## 一、背景與目標

現有系統已有：
- `ScanSession`：記錄掃描的部分參數，但**不含策略版本、條件快照、每檔評分明細**
- `TradePlan`：記錄交易計畫與風控結果，但**不含事件時間線**
- `TradeJournal`：記錄已成交的交易，但**缺乏觸發鏈（選股→警示→計畫→執行）**

目標：新增統一的 `event_log` 表，建立完整決策時間線，供未來 AI 分析使用。

---

## 二、資料表設計

### 2.1 SQLAlchemy Model（新增至 `db/models.py`）

```python
class EventLog(Base):
    __tablename__ = "event_log"
    __table_args__ = (
        Index("idx_event_log_created_at", "created_at"),
        Index("idx_event_log_event_type", "event_type"),
        Index("idx_event_log_stock_id", "stock_id"),
        Index("idx_event_log_scan_id", "scan_id"),
    )

    id           = Column(Integer, primary_key=True)
    created_at   = Column(String(30), nullable=False)   # ISO datetime YYYY-MM-DD HH:MM:SS
    event_type   = Column(String(50), nullable=False)   # scan_completed / stock_selected / ...
    module       = Column(String(50))                   # scanner / portfolio / trade_plan / scheduler
    scan_id      = Column(String(50))                   # 同批次掃描的共同 key
    stock_id     = Column(String(10))
    stock_name   = Column(String(50))
    severity     = Column(String(10), default="info")   # info / warning / danger
    summary      = Column(Text)                         # 人看的簡短描述
    payload_json = Column(Text)                         # 詳細 JSON，策略快照放這
```

### 2.2 Migration（新增至 `db/database.py` → `_migrate_schema()`）

```python
if not _table_exists(conn, "event_log"):
    conn.execute(text("""
        CREATE TABLE event_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at   TEXT NOT NULL,
            event_type   TEXT NOT NULL,
            module       TEXT,
            scan_id      TEXT,
            stock_id     TEXT,
            stock_name   TEXT,
            severity     TEXT DEFAULT 'info',
            summary      TEXT,
            payload_json TEXT
        )
    """))
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_event_log_created_at ON event_log (created_at)",
        "CREATE INDEX IF NOT EXISTS idx_event_log_event_type ON event_log (event_type)",
        "CREATE INDEX IF NOT EXISTS idx_event_log_stock_id ON event_log (stock_id)",
        "CREATE INDEX IF NOT EXISTS idx_event_log_scan_id ON event_log (scan_id)",
    ]:
        conn.execute(text(idx_sql))
    logger.info("migration: 建立 event_log 表")
```

---

## 三、db/event_log.py — 統一寫入與查詢模組

新增 `db/event_log.py`，提供：

```
log_event(event_type, module, scan_id, stock_id, stock_name, severity, summary, payload)
query_events(event_type, stock_id, date_from, date_to, limit) -> list[dict]
get_scan_timeline(scan_id) -> list[dict]
```

**`log_event()` 核心原則：**
- 永遠不 raise（try/except，失敗只 log warning）
- 不依賴 SQLAlchemy session，直接用 `ENGINE.begin()`，避免 session 污染
- `payload` 接受 dict，內部做 `json.dumps(ensure_ascii=False)`

---

## 四、整合點（Phase by Phase）

### Phase 1：建基礎（最高優先）

| 檔案 | 工作 |
|---|---|
| `db/models.py` | 新增 `EventLog` class |
| `db/database.py` | `_migrate_schema()` 加建表邏輯 |
| `db/event_log.py` | 新增 `log_event()` + `query_events()` |

### Phase 2：接入選股雷達（核心價值）

**整合點：`pages/1_選股雷達.py`** — 掃描完成後的結果處理段

觸發：使用者點「開始掃描」按鈕，掃描完成後

```
掃描完成時 → log_event("scan_completed", module="scanner", scan_id=..., payload={
    strategy_name: "v4_leading_breakout",
    strategy_version: "v4.3",
    base_date: ...,
    scan_mode: ...,
    universe_count: ...,
    selected_count: ...,
    settings: { rs_score_min, volume_ratio_min, atr_overheat_multiplier,
                main_force_buy_days, use_industry_rotation, ... },
    data_status: { finmind_mode, institutional_data_ready, ... }
})

每檔入選股票 → log_event("stock_selected", stock_id=..., scan_id=..., payload={
    rank, score, metrics: { close, change_pct, volume_ratio, rs_score, atr14, ma20 },
    required_rules_passed: { ma_triple_breakout, ma_squeeze, volume_explosion,
                              atr_ok, rs_strong, breakout_60d, main_force_buy_3d },
    bonus_rules_hit: { bb_bandwidth_shrink, trust_first_buy, margin_clean, ... }
})

差一點入選的股票（通過 5/7 必要條件）→ log_event("near_miss", ...)
```

**scan_id 生成規則：** `f"{date}_{time}_{scan_mode}"` 例如 `"20260426_144500_standard"`

### Phase 3：接入交易計畫

**整合點：`modules/trade_plan.py`** — `create_plan()` 與 `execute_plan()` 與 `cancel_plan()`

```
create_plan() 後 → log_event("trade_plan_created", module="trade_plan", ...)
  payload: { direction, entry_price, stop_loss, target_price, shares, reason }

create_plan() 同時記錄風控結果：
  has_violation=False → log_event("risk_check_passed")
  has_violation=True  → log_event("risk_check_failed", severity="warning",
                                   payload: { failed_rules, risk_amount, risk_percent, ... })

execute_plan() → log_event("trade_executed", ...)
cancel_plan()  → log_event("user_cancelled", ...)
```

### Phase 4：接入持股警示

**整合點：`pages/2_持股監控.py`** — 警示觸發邏輯

```
警示觸發 → log_event("alert_triggered", module="portfolio", stock_id=...,
                      severity="warning",
                      payload: { alert_type, current_price, threshold, ... })
```

### Phase 5：接入排程推播

**整合點：`scheduler/jobs.py`** — LINE 推播呼叫後

```
推播成功 → log_event("notification_sent", module="scheduler",
                      payload: { channel, recipient_count, message_type, ... })
```

### Phase 6：Event Log 查詢頁（Streamlit UI）

新增 `pages/11_事件日誌.py`：
- 篩選欄：日期範圍 / 股票代碼 / 事件類型
- 時間線表格：created_at / event_type / stock_id / summary
- 展開欄：顯示 payload_json（格式化 JSON）
- scan_id 鑽取：點選 scan_id 顯示該次掃描全部事件

---

## 五、測試步驟

### 5.1 單元測試（手動執行）

```python
# 在 Python shell 或 scripts/test_event_log.py 執行
from db.database import init_db
from db.event_log import log_event, query_events

init_db()  # 確保 event_log 表已建立

# T1：基本寫入
log_event("scan_completed", module="test", summary="test scan",
          payload={"strategy_version": "v4.3", "selected_count": 3})

# T2：帶 stock_id 寫入
log_event("stock_selected", module="test", stock_id="2330", stock_name="台積電",
          scan_id="test_scan_001", summary="入選 rank=1",
          payload={"rank": 1, "score": 125})

# T3：查詢
events = query_events(event_type="stock_selected", stock_id="2330")
assert len(events) >= 1
assert events[0]["stock_id"] == "2330"

# T4：payload_json 反序列化正確
import json
assert json.loads(events[0]["payload_json"])["score"] == 125

print("✅ 所有單元測試通過")
```

### 5.2 整合測試（透過 UI）

1. 啟動 `streamlit run app.py`
2. 進入「選股雷達」，執行任意模式掃描
3. 掃描完成後，開啟 DB Browser（或 sqlite3 CLI）查詢：
   ```sql
   SELECT event_type, stock_id, summary, created_at
   FROM event_log ORDER BY id DESC LIMIT 20;
   ```
4. 確認：
   - 有 1 筆 `scan_completed`
   - 有 N 筆 `stock_selected`（N = 入選股票數）
   - `payload_json` 含完整 settings 快照
5. 進入「交易計畫」，建立一筆計畫，確認有 `trade_plan_created` 與 `risk_check_*`
6. 執行或取消計畫，確認有 `trade_executed` 或 `user_cancelled`

### 5.3 異常處理測試

1. 強制讓 `log_event()` 接到壞資料（`payload` 含 `datetime` object），確認不 raise
2. 在 DB 鎖定狀態下呼叫 `log_event()`，確認應用不崩潰只輸出 warning

---

## 六、驗證標準

| 項目 | 通過條件 |
|---|---|
| 表建立 | `PRAGMA table_info(event_log)` 回傳 9 欄 |
| 寫入不 raise | `log_event()` 在任何情況下不讓頁面崩潰 |
| 掃描事件 | 每次掃描 = 1 筆 scan_completed + N 筆 stock_selected |
| 策略快照完整 | payload_json 含 strategy_version、全部門檻參數、data_status |
| 交易計畫事件 | 建立/執行/取消均有對應 event_type |
| 查詢正確 | `query_events(stock_id="2330")` 只回傳 2330 的事件 |
| scan_id 關聯 | 同次掃描所有事件的 scan_id 一致 |
| 效能不影響 UI | 整合後選股雷達掃描時間增加 < 500ms |

---

## 七、實作順序（建議）

```
1. db/models.py           → 新增 EventLog class
2. db/database.py         → 新增 migration
3. db/event_log.py        → 實作 log_event + query_events
4. scripts/test_event_log.py → 單元測試腳本（驗證 Phase 1）
5. pages/1_選股雷達.py    → 接入 scan_completed + stock_selected
6. modules/trade_plan.py  → 接入 trade_plan_created + risk_check_* + trade_executed + user_cancelled
7. pages/2_持股監控.py    → 接入 alert_triggered
8. scheduler/jobs.py      → 接入 notification_sent
9. pages/11_事件日誌.py   → 查詢 UI
```

---

## 八、未來 AI 分析可回答的問題

完成後，可對 `event_log` 做查詢或交給 AI 分析：

- 最近 30 天哪些股票曾被 v4 選中？後續 5 日報酬如何？
- `near_miss` 最常缺哪個必要條件？
- 被風控擋下的計畫（`risk_check_failed`）後來是漲是跌？
- 哪些 `alert_triggered` 後使用者沒建計畫（`user_ignored` 模式）？
- 哪一版 settings 快照的入選股票後續表現最佳？

---

## 九、注意事項

1. **不取代現有資料表**：`event_log` 是附加記錄，不影響 `scan_session`、`trade_plan`、`trade_journal` 的現有功能
2. **scan_id 與 ScanSession 對齊**：選股雷達同時寫 `ScanSession` 和 `event_log`，scan_id 用相同 key
3. **WAL mode 已啟用**：現有 ENGINE 設定已有 `PRAGMA journal_mode=WAL`，多執行緒寫入安全
4. **payload 用 dict 傳入**：`log_event` 內部處理序列化，呼叫端不用 `json.dumps`
5. **`severity` 使用慣例**：`info`（一般事件）、`warning`（警示/違規）、`danger`（系統錯誤類事件）
