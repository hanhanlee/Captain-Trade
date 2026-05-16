# v5 狙擊手版 — 測試清單

> 適用日：v5 完整 pipeline 上線後第一個交易日
> 涵蓋範圍：DB schema、08:55 build job、09:00–13:30 intraday checker、Page 14 UI、Page 1 realtime 模式切換、Telegram 推播、event_log

依「自動排程驗證 → 手動觸發 → 個別模組」三層由外往內測。

---

## A. 自動排程驗證（08:55–13:30 真實時序）

### A1. 08:55 自動 build
- [ ] 08:55 後檢查 Telegram：應收到「📋 v5 狙擊手版候選清單建構完成」訊息，含 scanned / A / B / written 數字
- [ ] 開 `pages/14_盤中v5狙擊` → 服務狀態區「今日 watchlist 總數」與 Telegram 數字一致
- [ ] event_log 應有一筆 `v5_sniper_watchlist_built`（在 Page 14 → 突破警示歷史 → watchlist 建構 tab 可看到）
- [ ] **特別注意**：v3 三線齊穿 watchlist（08:50）也應正常運作，未被 v5 影響

### A2. 09:00–13:30 自動 checker
- [ ] 開 Page 14，觀察「今日 watchlist 總數」候選的「推播」欄；當某檔現價突破目標價且量增條件成立時，幾分鐘內應變成 ✅
- [ ] 收到該檔 Telegram「🎯 v5 狙擊手版突破」訊息，內容應含：型態 A/B、目標價、現價、突破%、預估全日量 vs 昨量倍數、今日漲幅
- [ ] event_log 有對應 `v5_sniper_alert` 紀錄（Page 14 → 突破警示歷史 → 突破推播 tab）
- [ ] 同一檔不會推播第二次（alerted_today 防呆）

---

## B. Page 14（盤中 v5 狙擊頁面）

### B1. 顯示
- [ ] 頁面正常載入，無 Streamlit error
- [ ] 「⚙ 服務狀態」顯示 srock scheduler 燈號（若 scheduler 在跑 → 🟢）
- [ ] 「📋 今日候選清單」表格欄位齊全：代號 / 名稱 / 型態(A or B) / 目標價 / 昨收 / 昨高 / 距目標% / 昨量 / 五日均量 / MA5/10/20/60 / 糾結度% / 漲幅cap%
- [ ] Metrics 顯示：候選總數、型態 A 數、型態 B 數、已推播、距目標 ≤ 2%

### B2. 手動工具
- [ ] **立即重建**：保持預設（糾結度 4%、不啟用漲幅 cap）→ 按下後 30 秒~數分鐘內回傳統計，無 Exception
- [ ] 重新整理頁面 → 候選清單應更新（注意：重建會清掉今日舊資料）
- [ ] 試調糾結度 slider 到 2.5% → 重建 → 候選數應變少
- [ ] 試啟用漲幅 cap = 6% → 重建 → DB 內 row 應有 `max_gain_pct=6.0`（重建後表格「漲幅cap%」欄會顯示 6.0）
- [ ] **立即檢查**：按下後若 Shioaji 未登入，應顯示「完成：本輪推播 0 則」（不應 crash）

### B3. 歷史 tab
- [ ] 「突破推播」tab：若今日有推播，應列出時間 / 結果 / 代號 / 型態 / 現價 / 目標價 / 預估量 / 漲幅
- [ ] 「watchlist 建構」tab：應有今日 08:55 那一筆，含完整統計欄

---

## C. Page 1（選股雷達）v5 模式

- [ ] 在策略 radio 選「v5 狙擊手版」→ 應出現「🎯 v5 進階設定」expander
- [ ] expander 內三個控制項：漲幅 cap toggle + slider、**型態 A 糾結度 slider（新增，預設 3.0%，範圍 2.0–5.0）**、型態 B 模式 radio
- [ ] 跑掃描（任一範圍）→ 結果頁不 crash，候選為 0 或少量都正常（v5 設計上空倉率高）
- [ ] 試把糾結度從 3.0% 調到 4.0% → 重跑 → 命中數應變多（或一樣，不應變少）

---

## D. DB 層

- [ ] 啟動任何頁面後，sqlite 內應有 `v5_sniper_watchlist` 表（可用 sqlite3 CLI 或 DB Browser 確認）
- [ ] 表內欄位包含：scan_date, stock_id, pattern_type, breakout_target, prev_high, prev_close, prev_volume, vol_ma5, ma5/10/20/60, ma_spread_pct, max_gain_pct, alerted_today
- [ ] 唯一鍵 `(stock_id, scan_date)` 存在 → 重複 upsert 不會炸

---

## E. 邊界情境

- [ ] **非交易日**（週六）：08:55 不應觸發 build；scheduler log 應顯示 skip
- [ ] **Shioaji 未登入**：盤中 checker log 應出現「Shioaji 未登入，v5 盤中檢查本輪略過」，不應持續報錯或推播
- [ ] **全市場 0 命中**：Telegram 摘要應仍正常推（A=0, B=0, written=0），不視為錯誤
- [ ] **手動重建跑兩次**：第二次應 clear 今日 + 重寫，不會出現重複 row

---

## F. 不在此次測試範圍（已知限制，毋須回報）

- KD 指標未實作（v5 暫用 fallback True 放行，文件已記錄於 `FUTURE_FEATURES.md`）
- 盤中 checker 不檢查紅K / 實體 60%（設計如此，留收盤再驗）
- v5 Page 1 realtime 與 Page 14 intraday 預設糾結度不同（3% vs 4%，使用者可手動對齊）

---

## 回報格式

如果測試員回報任何 ❌，請附上：
1. 對應頁面截圖
2. Telegram 訊息原文
3. `event_log` 內相關 row 的 `payload_json`
