# Future Features

## ✅ 已完成（2026-08~09）

- **搶 V 型反轉策略**（2026-08-27 上線）：急跌→低檔爆量→止跌K→過高確認 + KD 低檔金叉，
  盤中三段通知（盤中預警 / 13:15 統整 / 收盤確認）。定義見 `ref/搶V反轉_策略定義.md`，
  總覽見 `ref/策略總覽索引.md`。
- **v5 KD 濾網已補上**（原本延後，見下方已劃掉的段落）：`indicators.kdj()` 實作、
  `scanner.compute_indicators()` 產出 `k_value/d_value/j_value`，v5 `kd_bull` 自此接真值。

## Worker service separation

Status: Consider later, not required for the current local workflow.

The current Streamlit app can start the prefetch worker in-process. This is convenient for local development, but a future long-running deployment could separate responsibilities:

- `srock-streamlit`: UI only, runs on port 8501.
- `srock-prefetch`: background data worker, runs independently.

Why consider it:

- Keeps data fetching alive even if the UI restarts.
- Avoids confusing in-memory worker state across different Python processes.
- Makes restart policies and logs easier to manage with systemd or another process supervisor.
- Reduces the chance of accidentally running multiple workers against the same SQLite database and FinMind quota.

Implementation notes for later:

- Add a cross-process lock or heartbeat to prevent duplicate workers.
- Persist worker status to SQLite so the UI reads status from the database instead of process memory.
- Remove automatic worker startup from `app.py` once an external worker service is configured.
- Add deployment docs and service files for Ubuntu/systemd.

## ~~v5 狙擊手版：補上 KD 指標濾網~~（✅ 已於 2026-08-27 完成，保留紀錄）

Status: **DONE（2026-08-27）**。下列三處都已實作（搶V反轉需要 KD 低檔金叉，一併補上，v5 受惠）。原始延後理由與作法保留備查：

`ref/stock_selection_radar.md` 第 6 節「技術指標輔助」要求 v5 帶 KD(9) 多頭條件（`K > D`），但 `modules/indicators.py` 目前未實作 `kdj()`，`scanner.compute_indicators()` 也沒產出 `k_value` / `d_value` 欄位。`modules/v5_sniper.py` 已預留 fallback：當缺欄位時 `kd_bull` 一律 True，不影響 gate 通過。

為何延後：v5 其他條件（破昨高、實體紅 K 60%、量增 1.5x、MACD 零軸上多頭）一旦同時成立，KD 幾乎必然 K > D，資訊重複度高，預估命中率影響 < 5%。

未來要補回時，動三處：

1. `modules/indicators.py` 加：
   ```python
   def kdj(df, n=9, m1=3, m2=3) -> tuple[pd.Series, pd.Series, pd.Series]:
       """回傳 (K, D, J)。台股慣用 n=9, m1=m2=3。"""
   ```
2. `modules/scanner.py::compute_indicators()` 加 `df["k_value"]` / `df["d_value"]` 兩欄。
3. `modules/v5_sniper.py::evaluate_v5()` 內既有的 `if "k_value" in df.columns ...` 分支會自動接管，fallback 自然失效。

判斷時機：跑 v5 一段時間後，若發現命中標的中常出現「型態漂亮但動能已疲」的雜訊，再補。否則維持現狀。
