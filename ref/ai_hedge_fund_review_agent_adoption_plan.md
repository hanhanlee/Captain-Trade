# AI Hedge Fund 多 Agent Review 導入計劃書

> 適用專案：台股交易輔助工具 / Srock  
> 目標版本：v3.x 之後的 AI Review 架構升級方向  
> 參考專案：virattt/ai-hedge-fund  
> 導入原則：只借鑑「多 Agent 分析流程」與「風控先行」概念，不導入自動下單，不讓 LLM 直接決定買賣或股數。  
> Review Agent 第一階段不串接 API，而是產生完整 Markdown / JSON 分析包，讓使用者自行貼到 Gemini、ChatGPT 或其他 LLM 網頁討論。

---

## 1. 參考資料

| 名稱 | 用途 | URL |
|---|---|---|
| ai-hedge-fund GitHub | 原始多 Agent 投資決策 POC 專案 | https://github.com/virattt/ai-hedge-fund |
| ai-hedge-fund README raw | README 原始內容，可參考 CLI、Backtester、Web App、Ollama 等說明 | https://raw.githubusercontent.com/virattt/ai-hedge-fund/main/README.md |
| Portfolio Manager | 參考如何彙整 analyst signals、risk constraints 並產生最終 decision | https://github.com/virattt/ai-hedge-fund/blob/main/src/agents/portfolio_manager.py |
| Risk Manager | 參考如何以 volatility、correlation、position limit 管理風險 | https://github.com/virattt/ai-hedge-fund/blob/main/src/agents/risk_manager.py |
| ai-hedge-fund Web App | 參考 Web UI 形式與互動方式 | https://github.com/virattt/ai-hedge-fund/tree/main/app |
| ai-hedge-fund-tw | 台灣在地化改寫專案，可觀察本地化 Agent 命名與台股改造方向 | https://github.com/KuolungCheng/ai-hedge-fund-tw |

---

## 2. 導入目標

本計劃目標不是建立「AI 自動基金」，而是將目前工具升級成：

```text
台股 AI 投資委員會 Lite
```

核心目標：

1. 將單一策略分數升級為多面向評估
2. 將技術面、籌碼面、基本面、風控面拆成不同 Agent
3. 每個 Agent 產生標準化 signal、confidence、reasoning
4. Risk Agent 使用 deterministic rule，不交給 LLM 判斷
5. Review Agent 只負責整合資料與產生討論包
6. 使用者自行將 Review Package 貼到 Gemini / ChatGPT 網頁討論
7. 所有 Agent 輸出寫入 Event Log，方便未來回測 AI 判斷品質

---

## 3. 不導入範圍

| 項目 | 是否導入 | 原因 |
|---|---:|---|
| 自動下單 | 不導入 | 台股券商 API、憑證、錯單風險高 |
| LLM 直接決定買賣 | 不導入 | 與輔助決策定位不符 |
| LLM 決定股數 | 不導入 | 股數應由風控公式計算 |
| Short / Cover | 不導入 | 目前工具以現股波段為主 |
| 名人投資人 Agent | 暫不導入 | 台股資料特性不同，容易變成展示效果大於實用 |
| 直接串 OpenAI / Gemini API | 第一階段不導入 | 先以 Markdown / JSON 匯出給使用者自行貼到網頁討論 |
| 即時盤中 AI 決策 | 不導入 | 資料延遲與盤中交易風險較高 |

---

## 4. ai-hedge-fund 值得借鑑的核心概念

### 4.1 多 Agent 分工

ai-hedge-fund 不是單一 AI 判斷，而是多個 Agent 分別分析不同面向，再由 Portfolio Manager 彙整。

本工具可改造成：

```text
Technical Agent
Chip Agent
Fundamental Agent
Theme Agent
Risk Agent
Review Agent
```

---

### 4.2 每個 Agent 都要輸出結構化結果

每個 Agent 應輸出：

```text
signal
confidence
reasoning
risk_flags
supporting_data
```

這樣才能：

- 比較不同 Agent 的判斷
- 存進 Event Log
- 匯出給 LLM 討論
- 未來回測哪個 Agent 比較準

---

### 4.3 Risk Manager 必須在 Review Agent 前面

Risk Agent 不應該只是提供意見，而是要先給出硬限制。

例如：

- 單筆風險是否超過上限
- 單股曝險是否超過上限
- 同產業持股是否過度集中
- 是否處置股 / 五分盤
- 流動性是否不足
- 是否接近 ATR 過熱區

若 Risk Agent 判定 `risk_blocked`，Review Agent 只能解釋原因，不能建議進場。

---

### 4.4 Review Agent 不直接下決策

原始 ai-hedge-fund 會產生 buy / sell / hold 等動作。

本工具應改成較安全的輸出：

| final_opinion | 說明 |
|---|---|
| `watch_only` | 加入觀察，不建議建立交易計畫 |
| `allow_plan` | 可以建立交易計畫，但仍需使用者確認 |
| `risk_blocked` | 風控阻擋，不建議新倉 |
| `avoid` | 條件不足或風險過高 |
| `needs_manual_review` | 條件矛盾，需要人工判斷 |

---

## 5. 建議 Agent 設計

### 5.1 Technical Agent

**目的：** 解讀技術面訊號。

資料來源：

- v3 / v4 策略結果
- RS 相對強度
- MA5 / MA10 / MA20
- 量比
- ATR 過熱
- 60 日新高
- 布林通道
- 近失條件 near miss

輸出範例：

```json
{
  "agent_name": "technical_agent",
  "signal": "bullish",
  "confidence": 82,
  "reasoning": "v4 突破成立，RS 88，量比 2.1，今日站上 MA5/10/20 且突破 60 日新高。",
  "positive_points": [
    "v4 必要條件通過",
    "RS 高於 80",
    "量能明顯放大"
  ],
  "negative_points": [
    "接近 ATR 過熱門檻"
  ],
  "risk_flags": [
    "near_atr_overheat"
  ]
}
```

---

### 5.2 Chip Agent

**目的：** 解讀籌碼面。

資料來源：

- 三大法人買賣超
- 外資 / 投信 / 自營商
- 主力分點買賣超
- 融資融券
- 法人爆量比例
- 外資 / 投信佔比新上榜

輸出範例：

```json
{
  "agent_name": "chip_agent",
  "signal": "bullish",
  "confidence": 75,
  "reasoning": "主力分點連續 3 日買超，且法人買超佔成交量達 10% 以上。",
  "positive_points": [
    "主力連 3 日買超",
    "法人爆量達標"
  ],
  "negative_points": [
    "融資未明顯下降"
  ],
  "risk_flags": []
}
```

---

### 5.3 Fundamental Agent

**目的：** 檢查基本面是否有明顯地雷。

資料來源：

- EPS TTM
- 營業現金流
- ROE
- 負債比
- 營收成長
- 毛利率 / 營益率
- Premium 基本面旗標

輸出範例：

```json
{
  "agent_name": "fundamental_agent",
  "signal": "neutral",
  "confidence": 60,
  "reasoning": "公司仍維持獲利，但 ROE 未達高品質標準，基本面沒有明顯加分。",
  "positive_points": [
    "EPS TTM 為正",
    "營業現金流為正"
  ],
  "negative_points": [
    "ROE 未達 15%"
  ],
  "risk_flags": []
}
```

---

### 5.4 Theme Agent

**目的：** 解讀題材與族群熱度。

資料來源：

- 產業輪動排名
- HP Density 族群創高密度
- Turnover Ratio 資金流向比重
- 新聞 / 題材摘要
- 同族群入選數量
- 同族群強勢股比較

第一階段可先不串新聞 API，只用現有產業與族群資料。

輸出範例：

```json
{
  "agent_name": "theme_agent",
  "signal": "bullish",
  "confidence": 70,
  "reasoning": "所屬產業近 5 日漲幅排名第 2，且同族群多檔股票同步創高。",
  "positive_points": [
    "產業輪動排名靠前",
    "HP Density 達標"
  ],
  "negative_points": [
    "題材新聞尚未整合"
  ],
  "risk_flags": []
}
```

---

### 5.5 Risk Agent

**目的：** deterministic 風控審查。

Risk Agent 不使用 LLM，必須由規則計算。

資料來源：

- 帳戶資金
- 目前持股
- 停損價
- 進場價
- ATR
- 成交量
- 產業分類
- 處置股 / 注意股狀態
- 目前產業曝險
- 已有同族群持股

檢查項目：

| 項目 | 規則範例 |
|---|---|
| 單筆風險 | 不超過帳戶 1% / 2% |
| 單股曝險 | 不超過總資金 20% |
| 產業曝險 | 不超過總資金 40% |
| 流動性 | 預估下單不得超過 20 日均量 3% |
| ATR 過熱 | 超過 MA20 + N × ATR 則阻擋或警告 |
| 處置股 | 禁止新倉或要求人工確認 |
| 停損缺失 | 無停損價不得建立交易計畫 |

輸出範例：

```json
{
  "agent_name": "risk_agent",
  "signal": "review_required",
  "confidence": 90,
  "risk_status": "warning",
  "reasoning": "單筆風險可控，但股價接近 ATR 過熱區，且同產業曝險已達 35%。",
  "position_limit": {
    "max_shares": 1000,
    "max_position_value": 92000,
    "risk_amount": 6000,
    "risk_percent": 1.2
  },
  "risk_flags": [
    "near_atr_overheat",
    "industry_exposure_high"
  ],
  "hard_blocks": []
}
```

---

### 5.6 Review Agent

**目的：** 彙整所有 Agent 結果，產生可供 LLM 討論的完整資料包。

第一階段不直接串接 LLM API。

Review Agent 做的事：

1. 收集 Decision Packet
2. 收集 Agent Signals
3. 收集 Risk Agent 結果
4. 收集市場環境
5. 收集持股與交易計畫資訊
6. 產生 Markdown 匯出檔
7. 產生 JSON 匯出檔
8. 附上給 Gemini / ChatGPT 的建議 Prompt

Review Agent 不做的事：

- 不直接下單
- 不直接呼叫 OpenAI / Gemini API
- 不直接決定使用者是否買進
- 不產生保證性語句
- 不繞過 Risk Agent

---

## 6. Agent Signal Schema

### 6.1 agent_signals 資料表

```sql
CREATE TABLE IF NOT EXISTS agent_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    decision_id TEXT,
    scan_id TEXT,
    stock_id TEXT NOT NULL,
    stock_name TEXT,
    agent_name TEXT NOT NULL,
    signal TEXT NOT NULL,
    confidence INTEGER,
    reasoning TEXT,
    positive_points_json TEXT,
    negative_points_json TEXT,
    risk_flags_json TEXT,
    payload_json TEXT
);
```

---

### 6.2 signal 建議值

| signal | 說明 |
|---|---|
| `strong_bullish` | 強烈偏多 |
| `bullish` | 偏多 |
| `neutral` | 中性 |
| `bearish` | 偏空 |
| `avoid` | 避免 |
| `review_required` | 需要人工確認 |
| `risk_blocked` | 風控阻擋 |

---

## 7. Agent Consensus Schema

### 7.1 agent_consensus 資料表

```sql
CREATE TABLE IF NOT EXISTS agent_consensus (
    consensus_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    decision_id TEXT,
    scan_id TEXT,
    stock_id TEXT NOT NULL,
    stock_name TEXT,
    final_opinion TEXT NOT NULL,
    confidence INTEGER,
    summary TEXT,
    positive_points_json TEXT,
    negative_points_json TEXT,
    required_checks_json TEXT,
    agent_scores_json TEXT,
    export_markdown_path TEXT,
    export_json_path TEXT,
    payload_json TEXT
);
```

---

### 7.2 final_opinion 建議值

| final_opinion | 說明 |
|---|---|
| `allow_plan` | 可以建立交易計畫 |
| `watch_only` | 只建議觀察 |
| `risk_blocked` | 風控阻擋 |
| `avoid` | 避免 |
| `needs_manual_review` | 需要人工判斷 |
| `insufficient_data` | 資料不足，無法判斷 |

---

## 8. Review Package 設計

### 8.1 為什麼要匯出 Review Package

第一階段不串接 LLM API，而是讓工具產生完整資料。

好處：

1. 不需要 API key
2. 不產生額外 API 成本
3. 使用者可自由選 Gemini / ChatGPT / Claude
4. 不會把交易系統和 LLM 深度綁定
5. 使用者可手動檢查資料後再貼給 LLM
6. 安全性較高，不會讓 LLM 操作資料庫或下單

---

### 8.2 匯出格式

每次 Review 產生兩個檔案：

```text
exports/ai_review/
  20260427_6213_review.md
  20260427_6213_review.json
```

| 格式 | 用途 |
|---|---|
| Markdown | 給使用者閱讀，或貼到 Gemini / ChatGPT |
| JSON | 給未來程式化分析、回測、AI API 串接使用 |

---

### 8.3 Review Markdown 結構

```markdown
# AI 投資委員會 Review Package

## 1. 使用目的與限制

這份資料是由台股交易輔助工具自動整理，目的在於輔助討論。
請不要把以下內容視為投資建議，也不要直接根據 LLM 回答進行交易。

## 2. 使用者要問 LLM 的問題

請根據以下資料，幫我檢查這檔股票是否適合建立交易計畫。
請特別注意：
1. 技術面是否過熱
2. 籌碼面是否支持
3. 風控是否合理
4. 是否有不應該進場的風險
5. 如果只適合觀察，請明確說明原因

## 3. 標的摘要

- 股票代碼：
- 股票名稱：
- 分析日期：
- 所屬產業：
- 目前價格：
- 今日漲跌幅：

## 4. 策略 Decision Packet

```json
...
```

## 5. Agent Signals

### Technical Agent
...

### Chip Agent
...

### Fundamental Agent
...

### Theme Agent
...

### Risk Agent
...

## 6. 市場環境

- 大盤狀態：
- 櫃買狀態：
- 目前市場風格：

## 7. 風控審查

- 建議最大股數：
- 單筆風險：
- 單股曝險：
- 產業曝險：
- 是否有 hard block：

## 8. 正面因素

...

## 9. 負面因素 / 風險

...

## 10. 需要人工確認的項目

...

## 11. 請 LLM 回答格式

請用以下格式回答：

1. 總結判斷：可建立交易計畫 / 只觀察 / 避免 / 需要更多資料
2. 主要理由
3. 最大風險
4. 停損與部位是否合理
5. 隔日如果開高，是否應該追價
6. 需要補充查證的資料
```

---

## 9. Review Package Prompt 範本

產生 Markdown 時可自動附上以下 prompt：

```text
你是一位保守的交易風險審查顧問。

我會提供一份由台股交易輔助工具整理的 Review Package。
請你根據資料協助我檢查這檔股票是否適合建立交易計畫。

重要限制：
1. 請不要直接叫我買進或賣出。
2. 請不要保證獲利。
3. 請優先檢查風險，而不是追求報酬。
4. 如果資料不足，請明確指出。
5. 如果技術面強但風控不合理，請以風控為優先。
6. 請用台股波段交易角度分析，不要用美股長期投資邏輯。
7. 請特別注意處置股、流動性、開高追價、法人資料延遲與停損設定。

請最後用以下格式回答：

## 總結
- 結論：可建立交易計畫 / 只觀察 / 避免 / 需要更多資料
- 信心程度：高 / 中 / 低

## 支持理由
...

## 主要風險
...

## 風控檢查
...

## 隔日操作建議
- 若開高超過 3%：
- 若回測不破：
- 若跌破關鍵價位：

## 需要補充確認
...
```

---

## 10. Event Log 串接

新增事件類型：

| event_type | 說明 |
|---|---|
| `agent_signal_generated` | 單一 Agent 產生 signal |
| `agent_consensus_generated` | 多 Agent 共識產生 |
| `review_package_exported` | Review Markdown / JSON 已匯出 |
| `review_package_opened` | 使用者開啟 Review Package |
| `trade_plan_reviewed` | 某交易計畫完成 AI Review 資料整理 |
| `risk_hard_blocked` | Risk Agent 阻擋建立計畫 |
| `user_exported_for_llm` | 使用者匯出資料給外部 LLM 討論 |

---

## 11. UI 設計建議

### 11.1 新增頁面：AI 投資委員會

頁面名稱：

```text
AI 投資委員會
```

主要功能：

1. 從最近選股結果選一檔股票
2. 或手動輸入股票代碼
3. 顯示各 Agent 分析結果
4. 顯示 Risk Agent 審查
5. 顯示 Review Consensus
6. 提供「匯出 Markdown」按鈕
7. 提供「匯出 JSON」按鈕
8. 提供「複製 Prompt + Review Package」按鈕

---

### 11.2 UI 區塊

```text
[標的摘要]
[策略訊號 Decision Packet]
[Agent Signals]
  - Technical Agent
  - Chip Agent
  - Fundamental Agent
  - Theme Agent
  - Risk Agent
[共識結論]
[風控結果]
[需要人工確認]
[匯出給 LLM]
```

---

### 11.3 Agent 卡片樣式

每個 Agent 顯示一張卡片：

```text
Technical Agent
Signal: Bullish
Confidence: 82
Reasoning:
- v4 突破成立
- RS 高於 80
- 量能放大

Risk Flags:
- near_atr_overheat
```

---

## 12. CLI 設計建議

可加入以下指令：

```bash
srock review 6213
srock review 6213 --date 2026-04-27
srock review 6213 --export-md
srock review 6213 --export-json
srock review 6213 --copy-prompt
srock review --from-scan latest --top 5
```

用途：

| 指令 | 說明 |
|---|---|
| `srock review 6213` | 對單一股票產生 Agent Review |
| `--date` | 指定分析日期 |
| `--export-md` | 匯出 Markdown |
| `--export-json` | 匯出 JSON |
| `--copy-prompt` | 輸出可貼給 LLM 的 prompt |
| `--from-scan latest --top 5` | 對最近選股雷達前 5 名批次產生 Review |

---

## 13. 與 FinRL-X 導入計劃的關係

前一份 FinRL / FinRL-X 導入計劃重點是：

```text
資料層
策略層
Decision Packet
No-Lookahead Guard
Benchmark
Market Friction
```

本計劃重點是：

```text
多 Agent Review
Agent Signal
Risk Agent
Review Package
外部 LLM 討論流程
```

兩者整合後：

```text
FinRL-X Lite 架構
        ↓
Decision Packet
        ↓
AI Hedge Fund Style Multi-Agent Review
        ↓
Review Package
        ↓
使用者貼到 Gemini / ChatGPT 討論
        ↓
交易計畫 / 觀察 / 避免
```

---

## 14. 分階段 Roadmap

### Phase 0：文件與 Schema 定義

目標：先定義清楚，不急著串 UI。

工作項目：

- 定義 Agent Signal Schema
- 定義 Agent Consensus Schema
- 定義 Review Package Markdown 格式
- 定義 Review Package JSON 格式
- 定義 Event Log 事件類型
- 定義 LLM Prompt 範本

產出：

```text
docs/ai_investment_committee_plan.md
docs/agent_signal_schema.md
docs/review_package_template.md
```

---

### Phase 1：Technical / Chip / Risk Agent 規則式實作

目標：先不用 LLM，也能產生穩定 Agent Signals。

工作項目：

- Technical Agent 讀取 v3/v4 Decision Packet
- Chip Agent 讀取法人、主力、融資
- Risk Agent 讀取風控、持股、產業曝險
- 所有 Agent 寫入 `agent_signals`
- 所有 Agent 事件寫入 `event_log`

完成後可回答：

- 技術面是否偏多？
- 籌碼面是否支持？
- 風控是否允許建立交易計畫？
- 哪些股票雖然技術強，但被風控阻擋？

---

### Phase 2：Review Package 匯出

目標：產生可貼到 Gemini / ChatGPT 的完整資料包。

工作項目：

- 產生 Markdown Review Package
- 產生 JSON Review Package
- 加入標準 Prompt
- 加入免責聲明與使用限制
- 支援單檔匯出
- 支援選股雷達前 N 名批次匯出

完成後使用流程：

```text
選股雷達前 5 名
    ↓
點選某檔股票
    ↓
產生 AI 投資委員會 Review
    ↓
匯出 Markdown
    ↓
貼到 Gemini / ChatGPT
    ↓
使用者根據討論結果建立交易計畫或只觀察
```

---

### Phase 3：UI 頁面

目標：讓非工程使用者也能使用。

工作項目：

- 新增 Streamlit 頁面「AI 投資委員會」
- 顯示各 Agent 卡片
- 顯示共識結論
- 顯示 Risk Agent hard block
- 顯示匯出按鈕
- 顯示可複製 Prompt
- 顯示歷史 Review Package

---

### Phase 4：Trade Plan 整合

目標：讓 Review Package 能和交易計畫連動。

工作項目：

- 在交易計畫頁新增「產生 AI Review Package」
- 若 Risk Agent hard block，禁止直接建立買進計畫
- 若 Review final_opinion 為 `watch_only`，引導加入觀察清單
- 若 final_opinion 為 `allow_plan`，允許建立交易計畫
- 建立計畫時保存 consensus_id
- 日後交易日誌可追溯當時 AI Review 結果

---

### Phase 5：Agent 評估與回測

目標：檢查 Agent 判斷品質。

工作項目：

- 統計 Technical Agent bullish 後 5 / 10 / 20 日表現
- 統計 Chip Agent bullish 後表現
- 統計 Risk Agent warning 後是否降低虧損
- 統計 final_opinion = allow_plan 的後續表現
- 統計 final_opinion = watch_only 但後來大漲的案例
- 建立 Agent Accuracy Report

---

### Phase 6：未來可選 API 串接

第一階段不做 API，但未來可保留接口。

可選設計：

```text
AI_PROVIDER=disabled
AI_PROVIDER=openai
AI_PROVIDER=gemini
AI_PROVIDER=anthropic
AI_PROVIDER=ollama
```

但即使未來串 API，也必須遵守：

1. LLM 不可直接下單
2. LLM 不可決定股數
3. LLM 不可繞過 Risk Agent
4. 所有 LLM 回覆必須寫入 Event Log
5. 使用者仍需手動確認

---

## 15. 優先順序

| 優先級 | 項目 | 原因 |
|---|---|---|
| P0 | Agent Signal Schema | 所有 Agent 的標準輸出 |
| P0 | Risk Agent deterministic 實作 | 安全基礎 |
| P0 | Review Package Markdown | 符合目前不串 API 的需求 |
| P1 | Review Package JSON | 未來自動化與回測基礎 |
| P1 | Event Log 串接 | 保留決策時間線 |
| P1 | AI 投資委員會 UI | 提高可用性 |
| P2 | Trade Plan 整合 | 讓 Review 真正進入交易流程 |
| P2 | Agent Accuracy Report | 評估 Agent 是否有用 |
| P3 | 外部 LLM API 串接 | 未來選項，非必要 |
| P4 | 名人 Agent | 不建議優先做 |

---

## 16. 風險與控管

### 16.1 技術風險

| 風險 | 說明 | 控管方式 |
|---|---|---|
| Agent 太多導致複雜 | 一開始做太多會難維護 | 第一版只做 Technical / Chip / Risk / Review |
| LLM 回覆不穩定 | 網頁 LLM 可能每次回答不同 | 匯出固定資料包，讓輸入可追溯 |
| JSON 太大 | Review Package 過長 | 分成 summary 與 full payload |
| 使用者貼錯資料 | 手動貼到 LLM 可能漏貼 | 提供「複製完整 Prompt + Package」按鈕 |

---

### 16.2 交易風險

| 風險 | 說明 | 控管方式 |
|---|---|---|
| 使用者過度相信 LLM | LLM 可能講得很有說服力但錯誤 | Prompt 明確要求保守、風控優先 |
| AI 強化追高衝動 | 技術面強時容易被說服追價 | Risk Agent 必須標示 ATR 過熱與開高追價風險 |
| 忽略資料延遲 | 法人、分點資料通常盤後才完整 | Review Package 標示 data_status |
| 忽略處置股風險 | 台股處置股流動性特殊 | Risk Agent 必須硬性提示或阻擋 |

---

## 17. 最終架構願景

```text
選股雷達 / Decision Packet
        ↓
Technical Agent
Chip Agent
Fundamental Agent
Theme Agent
Risk Agent
        ↓
Agent Signals
        ↓
Review Agent
        ↓
Review Package Markdown / JSON
        ↓
使用者貼到 Gemini / ChatGPT 討論
        ↓
交易計畫 / 觀察清單 / 避免
        ↓
Event Log / 交易日誌 / 回測評估
```

---

## 18. 總結

ai-hedge-fund 對本工具最有價值的不是「AI 幫你交易」，而是：

1. 多 Agent 分析架構
2. 每個 Agent 輸出 signal / confidence / reasoning
3. Risk Manager 先於 Portfolio / Review Manager
4. 最終決策必須受風控限制
5. reasoning 必須保存，未來可回測 AI 判斷品質

本工具第一階段建議導入：

```text
Technical Agent
Chip Agent
Risk Agent
Review Agent
Review Package Markdown / JSON 匯出
Event Log 串接
```

最重要的一句話：

> LLM 不直接接管交易流程；LLM 只拿到完整資料，協助使用者討論與檢查盲點。
