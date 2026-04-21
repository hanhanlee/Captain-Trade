"""
FinMind 抓取時間點記錄模組

記錄每次批次抓取的時間與股票數，用於分析 FinMind 各資料類型的發布規律：
  - 今日第一筆抓到的時間 / 今日最後抓到的時間
  - 歷史最早出現資料的時刻 / 歷史最晚更新完成的時刻
"""
from __future__ import annotations

from datetime import datetime, date
from sqlalchemy import text

from db.database import get_session

DATA_TYPES = ("inst", "margin", "price")


def log_fetch(
    trading_date: str | date,
    data_type: str,
    stock_count: int,
    active_total: int,
) -> None:
    """記錄一次批次抓取結果。每次抓到新資料後呼叫（無論是否完整）。"""
    if hasattr(trading_date, "isoformat"):
        trading_date = trading_date.isoformat()
    fetch_at = datetime.now().isoformat(timespec="seconds")
    with get_session() as sess:
        sess.execute(
            text("""
                INSERT INTO finmind_fetch_timing
                    (trading_date, data_type, fetch_at, stock_count, active_total)
                VALUES (:d, :t, :fa, :sc, :at)
            """),
            {
                "d":  trading_date,
                "t":  data_type,
                "fa": fetch_at,
                "sc": stock_count,
                "at": active_total,
            },
        )
        sess.commit()


def get_timing_report(trading_date: str | date | None = None) -> dict:
    """
    回傳抓取時間報告。

    回傳格式：
    {
        "today": {                       # 指定日期（預設今日）
            "inst":   {"first_at": ..., "last_at": ..., "first_count": ..., "last_count": ...},
            "margin": {...},
            "price":  {...},
        },
        "history": {                     # 跨日統計（含今日）
            "inst":   {"earliest_hhmm": "15:30", "latest_hhmm": "21:45", "days_sampled": 5},
            "margin": {...},
            "price":  {...},
        },
    }
    """
    if trading_date is None:
        trading_date = date.today()
    if hasattr(trading_date, "isoformat"):
        trading_date = trading_date.isoformat()

    result: dict = {"today": {}, "history": {}}

    with get_session() as sess:
        # ── 今日各類型的首次 / 末次紀錄 ──────────────────────────
        for dtype in DATA_TYPES:
            rows = sess.execute(
                text("""
                    SELECT fetch_at, stock_count, active_total
                    FROM   finmind_fetch_timing
                    WHERE  trading_date = :d AND data_type = :t
                    ORDER  BY fetch_at
                """),
                {"d": trading_date, "t": dtype},
            ).fetchall()

            if rows:
                result["today"][dtype] = {
                    "first_at":    rows[0][0],
                    "last_at":     rows[-1][0],
                    "first_count": rows[0][1],
                    "last_count":  rows[-1][1],
                    "active_total": rows[-1][2],
                    "fetch_count": len(rows),      # 本日共抓了幾次
                }
            else:
                result["today"][dtype] = None

        # ── 歷史各日的「首次出現」時刻彙總 ──────────────────────
        for dtype in DATA_TYPES:
            rows = sess.execute(
                text("""
                    SELECT
                        trading_date,
                        MIN(fetch_at) AS first_at,
                        MAX(fetch_at) AS last_at,
                        MAX(stock_count) AS peak_count
                    FROM   finmind_fetch_timing
                    WHERE  data_type = :t
                    GROUP  BY trading_date
                    ORDER  BY trading_date DESC
                    LIMIT  60
                """),
                {"t": dtype},
            ).fetchall()

            if rows:
                # 取各日 first_at 的 HH:MM 部分，取 MIN 為歷史最早
                first_times = [r[1][11:16] for r in rows if r[1]]   # 'HH:MM'
                last_times  = [r[2][11:16] for r in rows if r[2]]
                result["history"][dtype] = {
                    "earliest_hhmm": min(first_times) if first_times else None,
                    "latest_hhmm":   max(last_times)  if last_times  else None,
                    "days_sampled":  len(rows),
                    "recent": [
                        {
                            "date":        r[0],
                            "first_at":    r[1],
                            "last_at":     r[2],
                            "peak_count":  r[3],
                        }
                        for r in rows[:7]          # 最近 7 天明細
                    ],
                }
            else:
                result["history"][dtype] = None

    # ── 補充 price 的今日資料（從 price_cache 直接讀，不依賴 timing table）──
    try:
        from db.database import get_session as _gs
        with _gs() as sess:
            row = sess.execute(
                text("""
                    SELECT MIN(updated_at), MAX(updated_at),
                           COUNT(DISTINCT stock_id)
                    FROM   price_cache
                    WHERE  date = :d
                """),
                {"d": trading_date},
            ).fetchone()
            if row and row[0]:
                result["today"]["price"] = {
                    "first_at":    str(row[0]),
                    "last_at":     str(row[1]),
                    "first_count": None,           # price_cache 無法知道「第一批」筆數
                    "last_count":  row[2],
                    "active_total": None,
                    "fetch_count": None,
                }
    except Exception:
        pass

    return result
