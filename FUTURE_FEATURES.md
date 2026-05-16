# Future Features

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

## v5 狙擊手版：補上 KD 指標濾網

Status: Deferred. v5 上線時暫以 fallback（`kd_bull = True`）放行，待實測一段時間後再決定是否補。

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
