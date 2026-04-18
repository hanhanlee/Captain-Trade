# FinMind Sponsor Premium 升級施工依據

> 狀態：施工中  
> 日期：2026-04-19  
> 目的：FinMind 999 Sponsor 試用期間，以可熱插拔、可降級、可量化評估的方式導入 Premium 資料，不破壞現有 Free 模式與 v3/v4 核心策略。

---

## 1. 硬性原則

- v3 / v4 必要條件預設不動。
- Premium 關閉後，Free 模式必須照常運作。
- 缺 Premium 資料不等於條件不符。
- 不移除 rate limit。
- 嚴格防止 look-ahead bias。
- `.env` 只放 secrets，例如 `FINMIND_TOKEN`。
- Feature flags 與 tier 設定放在 `config.toml`。

---

## 2. Phase 0：基礎建設

### Step 0-1：Rate Limiter（已完成第一版）

在 `data/finmind_client.py` 的 `_get()` 加入節流。

規則：

| 情境 | 行為 |
|---|---|
| Free | 保守速率 |
| Sponsor | 可提高速率 |
| 實際硬上限 | 以 `user_info.api_request_limit` 為準 |
| quota < 15% | 暫停 Premium-only fetch |
| HTTP 402 / 403 | runtime degraded，Free 功能繼續 |

第一版可先用簡化實作：

- `threading.Lock`
- request timestamps sliding window
- 每次 `_get()` 前檢查
- 不需要一開始就做完整 token bucket

完成狀態：

- 已在 `data/finmind_client.py` 的 `_get()` 前加入 Premium gate 與 sliding-window rate limiter。
- Free 模式預設保守速率，Sponsor / Backer / auto 且 quota 足夠時可提高速率。
- Premium-only dataset 在 Premium 關閉時會被本機 gate 阻擋，不會打 API。

### Step 0-2：Config（已完成第一版）

在 `config.toml` 新增：

```toml
[finmind]
tier = "free"
premium_enabled = false

[finmind.features]
risk_flags = true
broker_branch = true
holding_shares = true
fundamentals_mode = "penalty"
```

`tier` 可用值：

- `free`
- `backer`
- `sponsor`
- `auto`

`.env` 僅保留：

```env
FINMIND_TOKEN=...
```

完成狀態：

- 已在 `config.toml` 加入 `[finmind]` 與 `[finmind.features]`。
- 已在 `srock/config.py` 的 `Config` dataclass 與 `load_config()` 中加入 FinMind tier / feature flags。

### Step 0-3：user_info quota check（已完成基礎版）

新增 FinMind user_info 查詢：

```text
GET https://api.web.finmindtrade.com/v2/user_info
Authorization: Bearer {token}
```

每小時查一次，快取在 memory。

注意：

- `user_info` 的認證方式與現有資料 API 不同。
- 現有 `data/finmind_client.py` 的 `_get()` 主要呼叫 `https://api.finmindtrade.com/api/v4/data`，token 目前走 query param。
- `user_info` 必須用獨立 request，並用 `Authorization: Bearer {token}` header。
- 不要讓 `user_info` 走現有 `_get()`，避免混用 API base URL 與認證方式。

完成狀態：

- 已新增 `refresh_finmind_user_info(force=False)`。
- `user_info` 使用獨立 request 與 Bearer token，不走 `_get()`。
- 預設每小時快取一次 quota 狀態。

### Step 0-4：Premium Runtime State（已完成基礎版）

不將 API 錯誤寫回 config，只維護執行期狀態。

```python
@dataclass
class PremiumState:
    user_enabled: bool
    tier: str
    quota_pct: float
    degraded: bool
    last_error: str
    last_quota_check: datetime | None
```

API 回 402 / 403 時：

- `degraded = True`
- Premium-only fetch 暫停
- 不修改 `config.toml`
- Free 功能繼續

完成狀態：

- 已新增 `PremiumState` 與 `get_premium_state()`。
- API 回 402 / 403 時只標記 runtime degraded，不寫回 config。
- `PremiumUnavailableError` 已被 `smart_get_fundamentals()` 特別處理，避免暫時性 Premium 狀態被錯快取成 90 天空資料。

待 Phase 1 補強：

- `PremiumUnavailableError` 需要正規化 reason，例如 `disabled` / `tier_free` / `quota_low` / `degraded` / `forbidden` / `http_402`。
- 基本面相關的 worker / scanner 入口需做前置判斷，避免 Free 模式逐股觸發 `_premium_gate()` exception 空轉。

### Step 0-5：資料管理頁 Premium 狀態 UI（已完成第一版）

在 `pages/6_資料管理.py` 顯示：

- tier
- premium_enabled
- quota %
- degraded
- last_error
- last_quota_check

完成狀態：

- 已在 `pages/6_資料管理.py` 顯示 FinMind tier、Premium runtime、quota、last quota check。
- 已加上手動「查詢 API 用量」按鈕。

### Step 0-6：待修項目（移入 Phase 1 基本面整合）

目前 `smart_get_fundamentals()` 已避免把 Premium 暫時不可用錯誤寫入 90 天空快取，但尚未完整消除 Free 模式的重複嘗試。

待修內容：

1. Worker 入口：`_get_funds_needing_fetch()` 在 Premium 未啟用或 tier 為 `free` 時直接回傳 `[]`，不排入基本面預抓佇列。
2. Scanner 入口：掃描前先做一次 Premium 狀態檢查。若 Premium 未啟用但使用者開啟基本面過濾，顯示一次 warning，整批跳過基本面，不進入逐股 `smart_get_fundamentals()`。
3. `PremiumUnavailableError reason`：在 Phase 1 基本面 penalty mode 一起正規化，區分 402 權限不足、quota 不足、runtime degraded、功能關閉等狀態。

---

## 3. Phase 1：高價值功能

### Step 1-6：官方風險旗標

資料源：

- `TaiwanStockDispositionSecuritiesPeriod`
- `TaiwanStockSuspended`
- `TaiwanStockPriceLimit`

新增表：

```text
risk_flags_cache(
  stock_id,
  date,
  flag_type,
  detail,
  fetched_at
)
```

`detail` 建議存 JSON，至少包含：

- `announcement_date`
- `effective_start_date`
- `effective_end_date`
- `reason`

整合方式：

| 模組 | 行為 |
|---|---|
| 選股雷達 | 顯示旗標；處置 `risk_penalty = 10`；暫停交易依基準日排除 |
| 持股監控 | 處置 / 暫停交易 LINE 警示 |
| 個股分析 | 顯示官方風險旗標 |

注意：

- 暫停交易排除必須以基準日或下一交易日可知資料為準。
- 不可用未來公告排除過去回測標的。

### Step 1-7：分點主力補強

沿用現有：

- `get_broker_main_force_series`
- `summarize_broker_main_force`
- `broker_main_force_cache`

在既有 `broker_main_force_cache` 加欄位：

```text
top5_buy_concentration
consecutive_buy_days
reversal_flag
```

計算：

| 指標 | 用途 |
|---|---|
| top5 concentration | 主力集中度 |
| consecutive buy days | 主力連續買超 |
| reversal flag | 主力反手風險 |

注意：

- `risk_penalty` 存正數。
- 缺資料應標記 missing，不可當作 0。
- 歷史回測不可使用基準日後才知道的分點資料。

### Step 1-8：基本面 Penalty Mode

現有 smart fundamentals 已有基礎，補上模式與分數整合。

新增 config：

```text
fundamentals_mode = off / warn / penalty / exclude
```

新增計算函式：

```python
compute_fundamental_penalty(metrics, mode) -> tuple[int, list[str]]
```

預設：

```text
penalty
```

整合進 scanner 的 Premium 分數，不改 v3 / v4 必要條件。

### Step 1-9：股權分散籌碼共振

資料源：

- `TaiwanStockHoldingSharesPer`

新增表：

```text
holding_shares_cache(
  stock_id,
  date,
  above_400_pct,
  above_1000_pct,
  below_10_pct,
  fetched_at
)
```

計算：

- 400 張以上連 2-3 週上升
- 1000 張以上連 2-3 週上升
- 10 張以下散戶比例下降
- 大戶上升 + 散戶下降共振

---

## 4. Phase 2：UI 與評估

### Step 2-10：Scanner 分數拆分

掃描結果加欄位：

```text
base_score
premium_score
risk_penalty
final_score
premium_positive_flags
premium_negative_flags
premium_missing_fields
```

公式：

```text
final_score = base_score + premium_score - risk_penalty
```

舊資料 migration：

```text
premium_score = 0
risk_penalty = 0
final_score = score
```

所有 schema migration 必須用安全方式處理舊資料，例如 `ALTER TABLE ADD COLUMN` 前先檢查欄位是否存在。

### Step 2-11：個股分析 Premium 區塊

在 `pages/7_個股分析.py` 加 expander：

- 分點集中度
- 主力反手
- 大戶 / 散戶趨勢
- 基本面 flags
- 風險旗標
- Premium missing fields

### Step 2-12：持股監控 Premium 警示

在 `pages/2_持股監控.py` 加：

- 主力反手
- 大戶籌碼鬆動
- 處置
- 暫停交易

LINE 只推明確風險，避免噪音。

### Step 2-13：試用期評估報表

在資料管理頁新增 tab，比較過去 30 天掃描結果：

- positive premium flags
- negative premium flags
- no premium flags

每組觀察：

- +3 日報酬
- +5 日報酬
- +10 日報酬
- 假突破率
- 候選數
- 分布圖 / box chart

---

## 5. 延後項目

暫不優先實作：

- 分K / Tick：不優先，之後只用於持股、前 10 候選股、個股分析手動查詢。
- 借券 / 軋空：等分點主力補強完成後再做。
- 週K / 月K：先用日K重組即可。

---

## 6. 最終開發順序

1. Rate Limiter (`finmind_client._get`)
2. Config `[finmind]` section
3. user_info quota check
4. Premium runtime state
5. 資料管理頁 Premium 狀態 UI
6. 官方風險旗標
7. 分點主力補強
8. 基本面 penalty mode
9. 股權分散
10. Scanner 分數欄位
11. 個股分析 Premium 區
12. 持股監控警示
13. 試用期評估報表

---

## 7. 與先前抽象計劃的修正

- `user_info` endpoint 使用 `https://api.web.finmindtrade.com/v2/user_info`，不是財報 endpoint。
- Rate limiter 以 `user_info.api_request_limit` 為硬上限。
- quota < 15% 時暫停 Premium-only fetch，而不是只降速。
- `risk_penalty` 存正數。
- 風險旗標要記錄公告日 / 生效日，防止 look-ahead bias。
- 評估報表拆成正面旗標 / 負面旗標 / 無旗標三組。
- 現有表加欄位要做 migration，舊資料預設值要安全。
