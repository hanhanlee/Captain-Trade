"""
Yahoo Finance 批次抓取客戶端

用途：Yahoo Bridge 模式下，在 FinMind 尚未更新當日資料時，
      以 Yahoo Finance 補充今日收盤資料（約 15 分鐘延遲）。

限制：
  - 僅提供 OHLCV，無法取得法人/融資等附加資料
  - 除權息調整（auto_adjust=True），與 FinMind 原始價格有細微差異
  - Volume 與 FinMind 差異約 4%（可接受）
"""
import time
import logging
import pandas as pd
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

BATCH_SIZE = 100          # 每批最多幾檔
BATCH_SLEEP_SEC = 1.5     # 批次間暫停（避免 Yahoo 封鎖）


def _to_yf_ticker(stock_id: str, otc_ids: Optional[set] = None) -> str:
    """台股代碼 → yfinance ticker（上市 .TW / 上櫃 .TWO）"""
    if otc_ids and stock_id in otc_ids:
        return f"{stock_id}.TWO"
    return f"{stock_id}.TW"


def _parse_batch_result(
    raw: pd.DataFrame,
    stock_ids: list[str],
    target_date: date,
    tw_map: dict[str, str],
) -> dict[str, pd.DataFrame]:
    """
    將 yf.download() 多層 columns 結果解析為
    {stock_id: single-row DataFrame（符合 price_cache 格式）}

    tw_map: {ticker: stock_id}，用於反查
    """
    result = {}
    if raw.empty:
        return result

    target_str = target_date.strftime("%Y-%m-%d")

    # yf.download 多檔時，columns 是 MultiIndex（欄位名, ticker）
    # 單檔時 columns 是一般 Index
    is_multi = isinstance(raw.columns, pd.MultiIndex)

    for ticker, sid in tw_map.items():
        try:
            if is_multi:
                if ticker not in raw.columns.get_level_values(1):
                    continue
                sub = raw.xs(ticker, axis=1, level=1)
            else:
                sub = raw  # 單檔情況

            if sub.empty:
                continue

            # 找 target_date 那一筆
            sub = sub.copy()
            sub.index = pd.to_datetime(sub.index).normalize()
            target_ts = pd.Timestamp(target_date)
            row = sub[sub.index == target_ts]

            if row.empty or row[["Close"]].isna().all(axis=None):
                continue

            r = row.iloc[0]
            df_row = pd.DataFrame([{
                "date":           pd.Timestamp(target_date),
                "open":           float(r["Open"])   if pd.notna(r.get("Open"))   else None,
                "max":            float(r["High"])   if pd.notna(r.get("High"))   else None,
                "min":            float(r["Low"])    if pd.notna(r.get("Low"))    else None,
                "close":          float(r["Close"])  if pd.notna(r.get("Close"))  else None,
                "Trading_Volume": float(r["Volume"]) if pd.notna(r.get("Volume")) else None,
            }])
            result[sid] = df_row

        except Exception as e:
            logger.debug(f"解析 {ticker}({sid}) 失敗：{e}")

    return result


def fetch_yahoo_closing_batch(
    stock_ids: list[str],
    target_date: date,
    otc_ids: Optional[set] = None,
) -> dict[str, pd.DataFrame]:
    """
    批次從 Yahoo Finance 抓取指定日期的收盤資料。

    Parameters
    ----------
    stock_ids : 台股代碼清單（已排除 delisted/no_update）
    target_date : 目標日期（通常為 date.today()）
    otc_ids : 上櫃股票代碼 set，用於決定 .TW / .TWO 後綴

    Returns
    -------
    dict[str, pd.DataFrame]
        key: stock_id
        value: 單列 DataFrame，欄位 = {date, open, max, min, close, Trading_Volume}
        只回傳成功且有當日資料的股票，失敗或無資料的直接略過（不中斷）
    """
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("yfinance 未安裝，請執行：pip install yfinance")

    result: dict[str, pd.DataFrame] = {}
    total = len(stock_ids)
    batches = [stock_ids[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    logger.info(f"Yahoo Bridge 批次抓取：{total} 檔，分 {len(batches)} 批")

    for batch_idx, batch in enumerate(batches):
        # 建立 ticker → stock_id 映射
        tw_map = {_to_yf_ticker(sid, otc_ids): sid for sid in batch}
        tickers = list(tw_map.keys())

        try:
            raw = yf.download(
                tickers,
                period="2d",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            batch_result = _parse_batch_result(raw, batch, target_date, tw_map)
            result.update(batch_result)

            ok = len(batch_result)
            logger.info(
                f"Yahoo Bridge 批次 {batch_idx+1}/{len(batches)}："
                f"{ok}/{len(batch)} 檔成功"
            )

        except Exception as e:
            logger.warning(f"Yahoo Bridge 批次 {batch_idx+1} 失敗：{e}")

        if batch_idx < len(batches) - 1:
            time.sleep(BATCH_SLEEP_SEC)

    logger.info(f"Yahoo Bridge 完成：共取得 {len(result)}/{total} 檔")
    return result


def get_today_cached_stock_ids(target_date: date) -> set[str]:
    """
    查詢 price_cache 中已有 target_date 資料的股票清單。
    供 Yahoo Bridge 預先過濾用，避免重複抓取。
    """
    from db.database import get_session
    from sqlalchemy import text

    with get_session() as sess:
        rows = sess.execute(
            text("SELECT stock_id FROM price_cache WHERE date = :d"),
            {"d": target_date.strftime("%Y-%m-%d")},
        ).fetchall()
    return {r[0] for r in rows}
