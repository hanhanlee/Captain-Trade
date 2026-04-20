## 第二十章：FinMind Sponsor Premium 試用功能

### 功能定位

Sponsor Premium 功能是這套工具的「增強層」，不是核心策略本身。v3 / v4 的技術條件、掃描流程、持股監控與風控原則都可以在 Free 模式下運作。Premium 資料只用來補強三件事：

- 風險辨識：官方處置、暫停交易、漲跌停等風險旗標。
- 籌碼觀察：分點主力、股權分散、大戶與小股東比例變化。
- 試用期評估：用歷史掃描結果驗證 Premium 訊號是否真的改善後續報酬。

### 降級原則

系統採用「可關閉、可降級、可觀察」的設計：

- Premium 未啟用或 tier 為 Free 時，不會硬打 Premium-only API。
- quota 過低或 API 回傳權限錯誤時，系統會進入 degraded 狀態，改讀本機 cache 或略過 Premium 欄位。
- 缺 Premium 資料不等於條件不符合，只代表目前不能用這個欄位判斷。
- 選股雷達的大量掃描不會逐檔呼叫 Premium API，避免快速耗盡額度。

### 在各頁面看到什麼

| 頁面 | Premium 相關內容 | 是否會額外呼叫 Premium API |
|------|------------------|----------------------------|
| 資料管理 | tier、quota、degraded、last quota check、Premium cache 完成度、Sponsor 主力券商背景補完 | 手動檢查狀態時會查 user_info；手動補完與背景補完會呼叫對應 API |
| 選股雷達 | score 拆分、risk_penalty、Premium / 風險 flags、試用期評估報表 | 掃描結果只讀 cache |
| 個股分析 | Sponsor 即時快照、官方風險旗標、Premium 摘要、大戶 / 主力 / 缺資料欄位 | 歷史模式只讀 cache；今日模式可為單股載入即時快照與缺少的主力/風險/大戶資料 |
| 持股監控 | Premium 風險摘要、官方風險、大戶比例、主力反手警示 | 只讀 cache |

### 目前資料種類與使用狀態

| 資料類別 | FinMind dataset | 權限 | 本機快取 / 狀態 | 目前用在哪裡 |
|----------|-----------------|------|-----------------|--------------|
| 股票清單 | `TaiwanStockInfo` | Free | `stock_info_cache` | 全市場掃描、產業分類、個股名稱 |
| 日K價格 | `TaiwanStockPrice` | Free | `price_cache` | 選股雷達、個股分析、回測、持股監控、RS/均線/ATR |
| 三大法人買賣超 | `TaiwanStockInstitutionalInvestorsBuySell` | Free 單股；全市場日期查詢需 Backer/Sponsor | `inst_cache` | 法人買超加分、投信第一天買超、資料管理 Supplementary |
| 融資融券 | `TaiwanStockMarginPurchaseShortSale` | Free 單股；全市場日期查詢需 Backer/Sponsor | `margin_cache` | 融資減少 / 籌碼集中、資料管理 Supplementary |
| 財報三表 | `TaiwanStockFinancialStatements` / `TaiwanStockBalanceSheet` / `TaiwanStockCashFlowsStatement` | 目前以 Premium gate 管控 | `fundamental_cache` | 基本面扣分/過濾、個股分析自訂條件、Premium 試用評估分組 |
| 券商分點主力 | `TaiwanStockTradingDailyReport` | Sponsor | `broker_main_force_cache` | v3/v4 必要條件「主力連 3 日」、主力買賣超圖、持股主力賣超/反手警示 |
| 股權分散 | `TaiwanStockHoldingSharesPer` | Backer/Sponsor | `holding_shares_cache` | 個股大戶比例、持股大戶下降/小股東上升警示、Premium 摘要 |
| 官方風險旗標 | `TaiwanStockDispositionSecuritiesPeriod` / `TaiwanStockSuspended` / `TaiwanStockAttentionSecuritiesPeriod` / `TaiwanStockTreasuryShares` | Backer/Sponsor | `risk_flags_cache` | 持股監控與個股分析的處置、暫停交易、注意股、庫藏股提示 |
| 申報轉讓 | `TaiwanStockShareholdingTransfer` | Backer/Sponsor | `risk_flags_cache` | 已保留顯示管線，但來源需再確認，暫列 Known issue |
| 個股即時快照 | `taiwan_stock_tick_snapshot` | Sponsor | 不落地，個股分析頁保留約 30 秒 session 快取；持股盤中監控即時讀取 | 個股分析今日模式的最新價、漲跌、買賣價、累積量；持股盤中監控的主要現價來源 |
| 分K現價 | `TaiwanStockKBar` | Sponsor | 不落地，只在即時快照無價格時備援讀取 | 盤中持股監控的備援現價來源 |
| 每日漲跌停價 | `TaiwanStockPriceLimit` | Backer/Sponsor | 目前未正式落地 | 已列入 Premium dataset gate，但尚未接入風險旗標顯示 |
| 分點彙總/逐筆 | `TaiwanStockTradingDailyReportSecIdAgg` / `TaiwanStockPriceTick` | Sponsor 或更高 | 未使用 | 目前程式只列在 Premium dataset gate，尚未接 UI 或策略 |

> 官方文件顯示：`taiwan_stock_tick_snapshot` 約 10 秒更新一次且只限 sponsor 會員使用；券商分點 `TaiwanStockTradingDailyReport` 與分K `TaiwanStockKBar` 也屬 sponsor 會員資料；股權分級、處置、有些一次拿全市場資料屬 backer/sponsor。系統手冊以「目前程式實際用到」為準，未使用的 Sponsor 資料不會自動消耗額度。

### Known Issues / 後續確認

- `TaiwanStockShareholdingTransfer`（申報轉讓）目前列入管線與顯示，但實際資料來源仍需確認。若個別股票新聞有申報轉讓、FinMind 回傳沒有資料，先不要把它視為程式漏抓。
- `TaiwanStockPriceLimit` 已列入 Premium gate，但目前不再顯示成持股監控的 `[Premium] 官方風險旗標：price_limit`。後續若要使用，應改成清楚文字，例如「達漲停/跌停價」或「漲跌停參考價」。
- 股權分級 `_holding_level_bounds` 若遇到 FinMind 回傳純數字 level，會回傳 `(n, n)`。這是安全預設，該筆不會被錯誤歸到大戶或小股東區間，後續可視實際資料再細化。
- 主力券商快取曾因舊版 `TaiwanStockTradingDailyReport` 欄位對應錯誤，產生 `broker_count > 0` 但 `buy_top15/sell_top15/net = 0` 的壞資料。2026-04-20 已修正 `buy` / `sell` 對應並清理壞快取；若看到主力券商補完突然從 4/17 重新大量寫入，這是修復後重新補正，不是重複浪費額度。

### 分數欄位怎麼看

選股雷達現在保留原本的 `score` 相容性，同時拆出更清楚的欄位：

```text
final_score = base_score + premium_score - risk_penalty
```

- `base_score`：原本 v3 / v4 技術面分數。
- `premium_score`：預留給正向 Premium 訊號的加分欄位。
- `risk_penalty`：基本面或官方風險造成的扣分。
- `final_score`：實際顯示與排序用的分數。
- `premium_positive_flags`：正向 Premium 訊號文字。
- `premium_negative_flags`：負向訊號文字，可能包含基本面扣分，因此不是純 Premium。
- `premium_missing_fields`：目前缺資料的 Premium 欄位。

### 試用期評估怎麼用

進入「選股雷達 → 歷史紀錄」，上方會看到 Premium 試用期評估。建議用法：

1. 先累積至少數次掃描紀錄。
2. 選擇納入最近幾次掃描。
3. 比較 `negative risk/fundamental flags`、`positive premium flags`、`no tracked flags` 三組的 5 / 10 / 20 日後續報酬與勝率。
4. 觀察有效樣本數，不要用太少樣本做結論。

這個報表的目的不是直接產生買賣訊號，而是回答「Sponsor Premium 資料是否值得繼續使用」。

---
