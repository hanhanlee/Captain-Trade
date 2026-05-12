# FinRL / FinRL-X 導入計劃書

> 適用專案：台股交易輔助工具 / Srock  
> 目標版本：v3.x 之後的架構升級方向  
> 文件目的：參考 FinRL / FinRL-X 的量化交易架構，將目前工具升級為更模組化、可回測、可追溯、可 AI 分析的台股波段交易決策平台。  
> 重要原則：本工具仍維持「輔助決策系統」，不導入自動下單，不把 AI / DRL 作為直接買賣決策來源。

---

## 1. 參考資料

### FinRL / FinRL-X 官方與論文連結

| 名稱 | 用途 | URL |
|---|---|---|
| FinRL GitHub | 原始 FinRL 專案，重點在 DRL 交易研究框架與 train-test-trade pipeline | https://github.com/AI4Finance-Foundation/FinRL |
| FinRL 官方文件 | FinRL 三層架構、訓練/測試/交易流程與市場環境概念 | https://finrl.readthedocs.io/ |
| FinRL 三層架構文件 | Market Environment、DRL Agent、Financial Application 三層設計 | https://finrl.readthedocs.io/en/latest/start/three_layer.html |
| FinRL-Trading / FinRL-X GitHub | 新一代模組化、部署一致的交易架構，重點在 weight-centric pipeline | https://github.com/AI4Finance-Foundation/FinRL-Trading |
| FinRL-X arXiv | FinRL-X 論文：AI-Native Modular Infrastructure for Quantitative Trading | https://arxiv.org/abs/2603.21330 |
| FinRL-Meta GitHub | DataOps / market environment / dynamic datasets 參考 | https://github.com/AI4Finance-Foundation/FinRL-Meta |
| FinRL-Meta arXiv | Dynamic Datasets and Market Environments for Financial Reinforcement Learning | https://arxiv.org/abs/2304.13174 |

---

## 2. 導入目標

本計劃不是要把目前工具改成 FinRL，也不是要讓 AI 自動下單。

真正目標是吸收 FinRL / FinRL-X 的架構優點：

1. **資料、策略、回測、風控、交易計畫分層**
2. **策略輸出格式標準化**
3. **回測與真實使用流程一致**
4. **每次策略執行都能追溯當時設定**
5. **避免回測偷看未來資料**
6. **導入 benchmark 與 market friction**
7. **為未來 AI 分析、策略比較、復盤建立資料基礎**

---

## 3. 不導入範圍

以下項目不建議在目前階段導入：

| 項目 | 不建議原因 |
|---|---|
| DRL 自動交易 Agent | 台股資料頻率、可解釋性、過擬合風險高 |
| 自動下單 / 券商 API | 憑證、安全、錯單、法規與執行風險高 |
| 盤中高頻交易 | 目前工具以日 K、盤後法人、融資、分點資料為主 |
| AI 自主決策買賣 | 與工具定位不符，應維持使用者最後決策 |
| 複製 FinRL 程式碼 | 授權、依賴與架構差異大，應只參考設計思想 |

---

## 4. 目前工具現況摘要

目前工具已具備以下基礎：

| 模組 | 現況 |
|---|---|
| 選股雷達 | v3 / v4 策略、RS、量能、均線、ATR 過熱、法人/主力分點、族群偵測 |
| 持股監控 | 停損、停利、跌破均線、高點回撤、LINE / Telegram 警示 |
| 風險控制 | 根據資金、停損位置、單筆風險計算建議股數 |
| 交易計畫 | 進場前填寫計畫、風控審查、待執行清單、執行後寫入交易日誌 |
| 交易日誌 | BUY / SELL 紀錄、損益統計、情緒標記、持股同步 |
| 回測模組 | 台股交易成本、績效分析、AI 深度解讀、Markdown 報告匯出 |
| 事件日誌 | scan、stock selected、alert、risk check、trade plan、notification 等事件 |
| 資料備份 | SQLite DB 備份至 Google Drive |
| 資料來源 | FinMind Free / Sponsor、yfinance 備援、本機快取 |

目前最大的缺口不是功能不足，而是：

1. 策略輸出格式尚未完全標準化
2. 回測、交易計畫、事件日誌之間可以再更緊密
3. benchmark 比較不足
4. no-lookahead 檢查需要明確化
5. market friction 還可以更貼近台股實戰
6. 策略模組化程度可以再提高

---

## 5. FinRL / FinRL-X 可借鑑概念對照

### 5.1 FinRL Classic：三層架構

FinRL Classic 主要架構：

```text
Market Environment
        ↓
DRL Agent
        ↓
Financial Application
```

套用到本工具後，建議改成：

```text
台股資料層
        ↓
策略與風控層
        ↓
使用者決策應用層
```

對照表：

| FinRL 概念 | 本工具對應 |
|---|---|
| Market Environment | 日 K、分 K、法人、融資、分點、產業、新聞、快取 |
| DRL Agent | v3 / v4 策略、產業輪動、族群突破、風控規則 |
| Financial Application | 選股雷達、交易計畫、持股監控、回測、事件日誌、AI 分析 |

---

### 5.2 FinRL Classic：Train-Test-Trade Pipeline

FinRL 的 train-test-trade pipeline 可借鑑，但不直接套用。

本工具建議改成：

```text
Research → Backtest → Paper Plan → Real Tracking → Review
```

| 階段 | 對應功能 | 說明 |
|---|---|---|
| Research | 選股雷達、個股分析、產業族群 | 找候選標的與觀察條件 |
| Backtest | 回測模組、AI 解讀 | 驗證策略過去表現 |
| Paper Plan | 交易計畫、待執行清單 | 實際交易前先建立計畫 |
| Real Tracking | 持股監控、推播警示 | 追蹤持倉風險 |
| Review | 交易日誌、事件日誌、AI 匯出 | 事後復盤與策略修正 |

---

### 5.3 FinRL-X：Weight-Centric Interface

FinRL-X 的核心是 weight-centric interface，也就是策略最後輸出「目標持倉權重」，後面的回測與交易都吃同一種格式。

但本工具不做自動下單，因此不建議直接輸出 target weights。

建議改成：

```text
Decision Packet
```

也就是「策略決策包」。

每一次策略選出股票時，都產生一份標準格式的 decision packet。

---

## 6. Decision Packet 設計

### 6.1 設計目的

Decision Packet 是策略輸出的標準格式。

它的目的：

1. 讓 v3 / v4 / 未來策略都有一致輸出
2. 讓回測與交易計畫吃同一份資料
3. 讓 event log 可以保存完整策略判斷
4. 讓 AI 分析能知道當時為什麼選出這支股票
5. 讓未來新增策略不需要改整個系統

---

### 6.2 Decision Packet 範例

```json
{
  "decision_id": "20260427_144500_v4_6213",
  "scan_id": "20260427_144500_v4",
  "stock_id": "6213",
  "stock_name": "聯茂",
  "signal_date": "2026-04-27",
  "strategy": {
    "name": "v4_leading_breakout",
    "version": "v4.2",
    "display_name": "v4 領先攻擊版"
  },
  "suggested_action": "watchlist",
  "score": 132,
  "rank": 1,
  "evidence": {
    "rs_score": 88,
    "volume_ratio": 2.1,
    "change_pct": 6.8,
    "ma_convergence_pct_yesterday": 2.4,
    "main_force_buy_days": 3,
    "industry_rank": 2
  },
  "rules": {
    "required_passed": {
      "first_day_cross_ma_5_10_20": true,
      "ma_convergence": true,
      "volume_breakout": true,
      "atr_overheat_guard": true,
      "rs_score": true,
      "new_60d_high": true,
      "main_force_3d_buy": true
    },
    "bonus_hit": {
      "bollinger_bandwidth_shrink": true,
      "trust_first_buy": false,
      "margin_balance_decrease": true,
      "institutional_volume_ratio_10pct": true
    }
  },
  "risk_flags": [
    "near_atr_overheat"
  ],
  "next_steps": {
    "allow_create_trade_plan": true,
    "need_manual_review": true,
    "reason": "技術條件通過，但接近 ATR 過熱門檻，建議等回檔或盤中確認"
  }
}
```

---

### 6.3 suggested_action 建議值

| suggested_action | 說明 |
|---|---|
| `watchlist` | 加入觀察，不直接建議進場 |
| `candidate` | 候選標的，可進入交易計畫 |
| `avoid` | 條件不足或風險過高 |
| `review_required` | 需要人工確認 |
| `risk_blocked` | 被風控阻擋 |

---

## 7. Strategy Snapshot 設計

每一次掃描都要保存完整策略快照。

原因：

> 今天的 v4 不一定等於三個月後的 v4。

策略快照必須保存：

```text
strategy_name
strategy_version
base_date
scan_mode
universe
thresholds
checkboxes
data_status
market_context
```

### 7.1 Strategy Snapshot 範例

```json
{
  "strategy": {
    "name": "v4_leading_breakout",
    "version": "v4.2",
    "display_name": "v4 領先攻擊版"
  },
  "base_date": "2026-04-27",
  "scan_mode": "standard",
  "historical_mode": false,
  "universe": {
    "mode": "top_volume",
    "limit": 100,
    "min_avg_volume": 1000,
    "industry_rotation_enabled": true,
    "top_industry_count": 3
  },
  "thresholds": {
    "rs_score_min": 80,
    "volume_ratio_min": 1.5,
    "ma_convergence_max_pct": 3.0,
    "atr_overheat_multiplier": 3.5,
    "breakout_high_days": 60,
    "main_force_buy_days": 3
  },
  "checkboxes": {
    "use_institutional_filter": true,
    "institutional_filter_required": false,
    "use_margin_filter": true,
    "use_fundamental_filter": false,
    "use_hp_density": true,
    "use_turnover_ratio": false,
    "only_foreign_new_rank": false,
    "only_trust_new_rank": false,
    "only_institutional_volume_ratio_10pct": false
  },
  "data_status": {
    "finmind_mode": "sponsor",
    "price_cache_ready": true,
    "institutional_data_ready": true,
    "main_force_cache_ready": true,
    "margin_cache_ready": true,
    "fundamental_cache_ready": false
  },
  "market_context": {
    "market_regime": "偏多",
    "twse_above_ma20": true,
    "otc_above_ma20": true
  }
}
```

---

## 8. 建議資料表設計

### 8.1 scan_runs

記錄每一次掃描。

```sql
CREATE TABLE IF NOT EXISTS scan_runs (
    scan_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    base_date TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    scan_mode TEXT,
    universe_count INTEGER,
    selected_count INTEGER,
    settings_json TEXT NOT NULL,
    data_status_json TEXT,
    market_context_json TEXT
);
```

---

### 8.2 strategy_decisions

記錄每支股票的 Decision Packet。

```sql
CREATE TABLE IF NOT EXISTS strategy_decisions (
    decision_id TEXT PRIMARY KEY,
    scan_id TEXT NOT NULL,
    stock_id TEXT NOT NULL,
    stock_name TEXT,
    signal_date TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    score REAL,
    rank INTEGER,
    suggested_action TEXT,
    decision_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (scan_id) REFERENCES scan_runs(scan_id)
);
```

---

### 8.3 event_log

記錄決策時間線。

```sql
CREATE TABLE IF NOT EXISTS event_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    module TEXT,
    scan_id TEXT,
    decision_id TEXT,
    stock_id TEXT,
    stock_name TEXT,
    severity TEXT DEFAULT 'info',
    summary TEXT,
    payload_json TEXT
);
```

---

### 8.4 backtest_runs

記錄每次回測設定與結果。

```sql
CREATE TABLE IF NOT EXISTS backtest_runs (
    backtest_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    benchmark TEXT,
    cost_model_json TEXT,
    no_lookahead_policy_json TEXT,
    settings_json TEXT NOT NULL,
    summary_json TEXT NOT NULL
);
```

---

### 8.5 backtest_trades

記錄回測中的每筆模擬交易。

```sql
CREATE TABLE IF NOT EXISTS backtest_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    backtest_id TEXT NOT NULL,
    stock_id TEXT NOT NULL,
    stock_name TEXT,
    entry_date TEXT,
    entry_price REAL,
    exit_date TEXT,
    exit_price REAL,
    shares INTEGER,
    pnl REAL,
    pnl_pct REAL,
    max_drawdown_pct REAL,
    exit_reason TEXT,
    trade_json TEXT,
    FOREIGN KEY (backtest_id) REFERENCES backtest_runs(backtest_id)
);
```

---

## 9. 建議模組架構

建議逐步調整為以下結構：

```text
src/
  data/
    finmind_fetcher.py
    yfinance_fetcher.py
    cache_store.py
    feature_builder.py
    data_status.py

  strategies/
    base_strategy.py
    v3_ma_breakout.py
    v4_leading_breakout.py
    industry_rotation.py
    near_miss.py

  decisions/
    decision_packet.py
    decision_store.py

  backtest/
    engine.py
    benchmark.py
    metrics.py
    cost_model.py
    no_lookahead_guard.py
    market_friction.py

  risk/
    position_sizing.py
    guard_pipeline.py
    exposure.py

  events/
    event_log.py

  app/
    pages/
      scanner.py
      trade_plan.py
      backtest.py
      event_log.py
      settings.py
```

---

## 10. Base Strategy Interface

### 10.1 目標

所有策略都應該實作同一個介面。

這樣可以確保：

1. 回測模組不用知道策略細節
2. event log 可以統一紀錄
3. 交易計畫可以直接讀取 decision packet
4. 未來新增策略不會破壞現有功能

---

### 10.2 範例介面

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class StrategyContext:
    base_date: str
    historical_mode: bool
    market_context: Dict[str, Any]
    data_status: Dict[str, Any]


@dataclass
class StrategyResult:
    scan_id: str
    strategy_name: str
    strategy_version: str
    settings_snapshot: Dict[str, Any]
    decisions: List[Dict[str, Any]]


class BaseStockStrategy(ABC):
    name: str
    version: str
    display_name: str

    @abstractmethod
    def build_settings_snapshot(self, user_settings: Dict[str, Any]) -> Dict[str, Any]:
        pass

    @abstractmethod
    def scan(
        self,
        universe: List[str],
        data_bundle: Dict[str, Any],
        context: StrategyContext,
        settings: Dict[str, Any],
    ) -> StrategyResult:
        pass
```

---

## 11. No-Lookahead Data Guard

### 11.1 為什麼需要

回測最大的風險不是程式錯，而是「偷看未來」。

例如：

- 用今日盤後法人資料模擬今日盤中買進
- 用財報期間結束日，而不是公告日
- 用現在才知道的處置股狀態回推過去
- 用完整歷史排名資料計算當時不可能知道的排名

這會讓回測績效看起來很好，但實戰無法複製。

---

### 11.2 建議規則

| 資料類型 | 回測可用時間 |
|---|---|
| 日 K 收盤價 | 當日收盤後 |
| 分 K / 即時快照 | 只有盤中模擬才可使用 |
| 三大法人 | 依實際發布時間，通常盤後 |
| 融資融券 | 依實際發布時間，通常盤後或隔日 |
| 分點主力 | 依資料更新時間，不得假設盤中可得 |
| 財報 | 以公告日為準，不是財報期間結束日 |
| 注意股 / 處置股 | 以當時公告日為準 |
| 產業分類 | 可用，但需注意歷史分類變動 |

---

### 11.3 no_lookahead_policy_json 範例

```json
{
  "entry_timing": "next_trading_day_open_or_close",
  "institutional_data_available": "after_market_close",
  "margin_data_available": "after_market_close_or_next_day",
  "financial_statement_available": "announcement_date",
  "use_current_day_intraday_data": false,
  "reject_if_data_timestamp_after_decision_time": true
}
```

---

## 12. Market Friction 模型

### 12.1 為什麼需要

FinRL / FinRL-X 強調回測與實際交易要接近。  
對台股來說，只計算買賣手續費與交易稅還不夠。

還需要考慮：

- 滑價
- 漲跌停
- 處置股 / 五分盤
- 開盤跳空
- 流動性不足
- 最小交易單位
- 零股與整股差異
- 最大持倉數
- 最大產業曝險

---

### 12.2 cost_model_json 範例

```json
{
  "buy_fee_rate": 0.001425,
  "sell_fee_rate": 0.001425,
  "sell_tax_rate": 0.003,
  "min_fee": 20,
  "slippage_model": {
    "enabled": true,
    "base_slippage_pct": 0.001,
    "volume_sensitive": true,
    "max_participation_rate": 0.05
  },
  "limit_up_down_policy": {
    "cannot_buy_limit_up": true,
    "cannot_sell_limit_down": true
  },
  "disposition_stock_policy": {
    "block_new_entry": true,
    "allow_exit": true
  },
  "liquidity_policy": {
    "min_20d_avg_volume": 1000,
    "max_order_pct_of_daily_volume": 0.03
  }
}
```

---

## 13. Benchmark Upgrade

### 13.1 為什麼需要

策略賺錢不代表策略有效。

真正要比較的是：

> 這個策略是否比直接買大盤、ETF、或同產業更值得？

---

### 13.2 建議 benchmark

| 策略類型 | Benchmark |
|---|---|
| 全市場選股 | 加權指數、0050 |
| 中小型股策略 | 櫃買指數 |
| 電子股策略 | 電子類股指數 |
| 半導體策略 | 半導體類股指數、0050 |
| 高股息/防禦策略 | 0056、00878 |
| 產業輪動策略 | 被選中產業的平均報酬 |

---

### 13.3 建議回測報表指標

| 指標 | 說明 |
|---|---|
| Cumulative Return | 累積報酬 |
| Annualized Return | 年化報酬 |
| Max Drawdown | 最大回撤 |
| Sharpe Ratio | 風險調整後報酬 |
| Calmar Ratio | 年化報酬 / 最大回撤 |
| Win Rate | 勝率 |
| Profit Factor | 總獲利 / 總虧損 |
| Average R | 平均風險報酬倍數 |
| Exposure Days | 實際持倉天數 |
| Turnover | 換手率 |
| Benchmark Excess Return | 相對 benchmark 超額報酬 |

---

## 14. Event Log 與 FinRL-X 導入關係

Event Log 是本計劃的基礎。

FinRL-X 強調研究與部署流程一致，而本工具要做到這件事，必須知道每個決策的來源。

因此每次策略執行都要記錄：

```text
scan_completed
stock_selected
near_miss
risk_check_passed
risk_check_failed
trade_plan_created
trade_executed
alert_triggered
notification_sent
```

其中最重要的是：

```text
strategy_name
strategy_version
settings_snapshot
data_status_snapshot
decision_packet
```

---

## 15. AI 分析導入方式

不建議 AI 直接買賣。

建議 AI 只做以下四件事：

| AI 功能 | 說明 |
|---|---|
| 回測解讀 | 分析策略穩定性、過擬合、MDD、出場品質 |
| 事件日誌復盤 | 分析使用者是否常忽略警示、追高、違反風控 |
| 策略比較 | 比較 v3 / v4 / 客製策略在不同市場環境下的表現 |
| 交易計畫檢查 | 檢查進場理由、停損、風險報酬比是否合理 |

---

## 16. 分階段導入 Roadmap

### Phase 0：文件與設計確認

**目標：** 明確定義導入範圍，不讓專案變成 DRL 自動交易平台。

工作項目：

- 整理 FinRL / FinRL-X 參考文件
- 明確寫入「不導入自動下單、不導入 AI 自主交易」
- 定義 Decision Packet schema
- 定義 Strategy Snapshot schema
- 定義 No-Lookahead Policy
- 定義 Market Friction Policy

產出：

```text
docs/finrl_x_adoption_plan.md
docs/decision_packet_schema.md
docs/no_lookahead_policy.md
docs/market_friction_model.md
```

---

### Phase 1：Decision Packet + Strategy Snapshot

**目標：** 讓每次選股結果都有可追溯的標準格式。

工作項目：

- 新增 `strategy_decisions` 表
- 新增 `scan_runs` 表
- 掃描時建立 `scan_id`
- v3 / v4 都輸出 decision packet
- 每次掃描保存 settings snapshot
- 每次掃描保存 data status snapshot
- event_log 關聯 scan_id / decision_id

完成後可回答：

- 這支股票是在哪個策略版本被選出？
- 當時 RS、量比、ATR 門檻是多少？
- 當時哪些 checkbox 被勾選？
- 當時法人/主力/融資資料是否完整？

---

### Phase 2：Backtest Benchmark Upgrade

**目標：** 讓回測不只看絕對績效，也能看相對大盤與 ETF 是否有超額報酬。

工作項目：

- 新增 benchmark data loader
- 支援加權指數、櫃買指數、0050、0056、00878
- 回測報表新增 benchmark 對照
- 新增 excess return
- 新增 Sharpe / Calmar / Profit Factor / Average R
- AI 解讀加入 benchmark 評估

完成後可回答：

- 策略是否比大盤強？
- 策略是否只是跟著多頭一起漲？
- 策略承擔的回撤是否值得？
- 策略在空頭時是否真的有避險效果？

---

### Phase 3：No-Lookahead Guard

**目標：** 防止回測使用當時不可能知道的資料。

工作項目：

- 每種資料來源加入 `available_at`
- 回測時建立 `decision_time`
- 若資料時間晚於 decision_time，禁止使用
- 法人、融資、分點、財報資料分別定義可用時間
- 回測報表顯示本次使用的資料可得性規則

完成後可回答：

- 這次回測是否使用盤後資料模擬盤中進場？
- 財報資料是否用公告日而非季度結束日？
- 分點主力資料在當時是否已經可得？
- 這份回測是否可信？

---

### Phase 4：Market Friction 模型

**目標：** 讓回測更貼近台股真實交易限制。

工作項目：

- 加入滑價模型
- 加入漲跌停限制
- 加入處置股 / 五分盤限制
- 加入流動性限制
- 加入最大下單佔成交量比例
- 加入零股 / 整股處理
- 加入開盤跳空進場模型

完成後可回答：

- 訊號隔天開高是否還能買？
- 股票漲停是否假設買得到？
- 股票跌停是否假設賣得掉？
- 這檔股票成交量是否足夠承接建議部位？
- 處置股是否應該禁止新倉？

---

### Phase 5：策略模組化

**目標：** 將 v3 / v4 / 未來策略全部拆成標準策略模組。

工作項目：

- 建立 `BaseStockStrategy`
- v3 改寫成 `V3MaBreakoutStrategy`
- v4 改寫成 `V4LeadingBreakoutStrategy`
- 產業輪動改寫成獨立 filter / overlay
- 主力分點改寫成 feature provider
- 回測與 UI 都呼叫同一套 strategy interface

完成後可回答：

- 新增策略是否只需要新增一個 strategy file？
- 回測與實際選股是否吃同一套邏輯？
- event log 是否可自動記錄所有策略輸出？

---

### Phase 6：AI Review Dashboard

**目標：** 讓 AI 使用 event log、decision packet、backtest result 進行復盤。

工作項目：

- 新增 AI Review 頁面
- 支援選擇期間
- 支援分析策略表現
- 支援分析個人交易行為
- 支援分析忽略警示與風控違規
- 支援匯出 Markdown 給外部 AI 模型

可分析問題：

- 我是否常常追高？
- 哪些策略版本最穩？
- 哪些條件最常造成 near miss？
- 被風控擋下的股票後來表現如何？
- 我忽略的警示是否造成虧損？
- 哪種市場環境最適合 v4？

---

## 17. 優先順序總表

| 優先級 | 項目 | 原因 |
|---|---|---|
| P0 | Decision Packet | 所有後續分析的標準輸出 |
| P0 | Strategy Snapshot | 避免策略版本與參數失真 |
| P1 | Event Log 關聯 decision_id | 建立完整決策時間線 |
| P1 | Benchmark Upgrade | 判斷策略是否真的有超額報酬 |
| P1 | No-Lookahead Guard | 提升回測可信度 |
| P2 | Market Friction | 讓回測貼近台股實戰 |
| P2 | Strategy Module Interface | 降低未來維護成本 |
| P3 | AI Review Dashboard | 建立個人化策略復盤 |
| P4 | DRL / ML Strategy | 僅作研究，不進入實戰流程 |
| P5 | Broker API / 自動下單 | 暫不導入 |

---

## 18. 建議新增手冊章節

建議在使用說明手冊中新增一章：

```text
第二十五章：策略研究流程 — 借鑑 FinRL-X 的模組化決策架構
```

章節內容建議包含：

1. 為什麼不直接導入 DRL
2. 為什麼要標準化策略輸出
3. Decision Packet 是什麼
4. Strategy Snapshot 是什麼
5. 回測為什麼要做 benchmark 比較
6. No-Lookahead Guard 是什麼
7. 台股 Market Friction 有哪些
8. AI 在本工具中的角色：解釋、診斷、復盤，不是自動買賣

---

## 19. 風險與注意事項

### 19.1 技術風險

| 風險 | 說明 | 對策 |
|---|---|---|
| 架構過度複雜 | 導入太多 FinRL 概念可能讓工具變難用 | 分階段導入，每階段保持 UI 簡單 |
| Schema 太早定死 | Decision Packet 之後可能調整 | 使用 JSON 欄位保留彈性 |
| 回測變慢 | No-lookahead 與 benchmark 會增加計算量 | 加快取與批次計算 |
| 資料不足 | 台股部分 benchmark / 處置股資料可能不完整 | 允許 fallback 與資料狀態提示 |

---

### 19.2 交易風險

| 風險 | 說明 | 對策 |
|---|---|---|
| 使用者誤以為 AI 可保證獲利 | AI 分析可能被過度信任 | 明確標示僅供參考 |
| 回測過度擬合 | 調參後歷史績效漂亮但實戰失效 | 加入 out-of-sample 與 benchmark |
| 盤後資料誤用 | 用不可得資料模擬交易 | No-Lookahead Guard |
| 忽略流動性 | 小型股回測好但買賣困難 | Market Friction 模型 |

---

## 20. 最終架構願景

最終目標不是建立自動交易機器人，而是建立：

```text
台股波段版 FinRL-X Lite
```

核心流程：

```text
資料快取
  ↓
特徵工程
  ↓
策略掃描
  ↓
Decision Packet
  ↓
Event Log
  ↓
回測 / Benchmark / No-Lookahead Guard
  ↓
交易計畫
  ↓
風控審查
  ↓
手動執行
  ↓
持股監控
  ↓
交易日誌與 AI 復盤
```

一句話總結：

> 導入 FinRL / FinRL-X 的重點不是導入強化學習，而是導入「模組化、可追溯、可驗證、可復盤」的量化交易工程方法。
