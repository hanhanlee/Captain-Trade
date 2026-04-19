"""
融資融券本機快取

架構：累積式（與 price_cache 同架構）
  - 一行一天，(stock_id, date) 複合唯一鍵
  - 無 TTL，透過 delete_old_margin() 保留最近 400 天
  - 優點：掃描器需要最近 5 天資料，累積式可直接 WHERE date >= N天前 查詢

FinMind 欄位對應：
  MarginPurchaseBuy          → margin_buy
  MarginPurchaseSell         → margin_sell
  MarginPurchaseTodayBalance → margin_balance
  ShortSaleBuy               → short_buy
  ShortSaleSell              → short_sell
  ShortSaleTodayBalance      → short_balance
"""
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import text
from .database import get_session

MARGIN_RETAIN_DAYS = 400


def save_margin(stock_id: str, df: pd.DataFrame) -> int:
    """
    儲存融資融券資料至快取（INSERT OR REPLACE）。
    回傳儲存筆數。
    """
    if df.empty:
        return 0

    now_str = datetime.now().isoformat()
    rows = []

    col_map = {
        "MarginPurchaseBuy":          "margin_buy",
        "MarginPurchaseSell":         "margin_sell",
        "MarginPurchaseTodayBalance": "margin_balance",
        "ShortSaleBuy":               "short_buy",
        "ShortSaleSell":              "short_sell",
        "ShortSaleTodayBalance":      "short_balance",
    }

    for _, row in df.iterrows():
        d = row["date"]
        if hasattr(d, "date"):
            d = d.date().isoformat()
        elif hasattr(d, "isoformat"):
            d = d.isoformat()[:10]
        else:
            d = str(d)[:10]

        def _int(col):
            v = row.get(col)
            if v is None or (hasattr(v, "__class__") and pd.isna(v)):
                return None
            return int(v)

        rows.append({
            "stock_id":       stock_id,
            "date":           d,
            "margin_buy":     _int("MarginPurchaseBuy"),
            "margin_sell":    _int("MarginPurchaseSell"),
            "margin_balance": _int("MarginPurchaseTodayBalance"),
            "short_buy":      _int("ShortSaleBuy"),
            "short_sell":     _int("ShortSaleSell"),
            "short_balance":  _int("ShortSaleTodayBalance"),
            "fetch_at":       now_str,
        })

    if not rows:
        return 0

    sql = text("""
        INSERT OR REPLACE INTO margin_cache
            (stock_id, date, margin_buy, margin_sell, margin_balance,
             short_buy, short_sell, short_balance, fetch_at)
        VALUES
            (:stock_id, :date, :margin_buy, :margin_sell, :margin_balance,
             :short_buy, :short_sell, :short_balance, :fetch_at)
    """)
    with get_session() as sess:
        sess.execute(sql, rows)
        sess.commit()
    return len(rows)


def get_margin(stock_id: str, days: int = 5) -> pd.DataFrame:
    """
    讀取近 N 天融資融券資料，欄位名稱對齊 FinMind 原始格式，供掃描器使用。
    """
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with get_session() as sess:
        rows = sess.execute(
            text("""
                SELECT date, margin_buy, margin_sell, margin_balance,
                       short_buy, short_sell, short_balance
                FROM margin_cache
                WHERE stock_id = :sid AND date >= :start
                ORDER BY date ASC
            """),
            {"sid": stock_id, "start": start},
        ).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "date",
        "MarginPurchaseBuy", "MarginPurchaseSell", "MarginPurchaseTodayBalance",
        "ShortSaleBuy", "ShortSaleSell", "ShortSaleTodayBalance",
    ])
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_margin_for_date(stock_id: str, target_date, days: int = 14) -> pd.DataFrame:
    """讀取截至 target_date 的融資融券快取；歷史模式使用，不呼叫 live API。"""
    target = pd.Timestamp(target_date).strftime("%Y-%m-%d")
    start = (pd.Timestamp(target_date) - pd.Timedelta(days=days * 2)).strftime("%Y-%m-%d")
    with get_session() as sess:
        rows = sess.execute(
            text("""
                SELECT date, margin_buy, margin_sell, margin_balance,
                       short_buy, short_sell, short_balance
                FROM margin_cache
                WHERE stock_id = :sid AND date >= :start AND date <= :target
                ORDER BY date ASC
            """),
            {"sid": stock_id, "start": start, "target": target},
        ).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "date",
        "MarginPurchaseBuy", "MarginPurchaseSell", "MarginPurchaseTodayBalance",
        "ShortSaleBuy", "ShortSaleSell", "ShortSaleTodayBalance",
    ])
    df["date"] = pd.to_datetime(df["date"])
    return df


def get_stocks_needing_margin(all_ids: list[str], target_date: date | None = None) -> list[str]:
    """
    回傳今日尚無融資快取的股票清單。
    target_date 預設為 date.today()。
    """
    if target_date is None:
        target_date = date.today()
    target_str = target_date.strftime("%Y-%m-%d")

    with get_session() as sess:
        rows = sess.execute(
            text("SELECT DISTINCT stock_id FROM margin_cache WHERE date = :d"),
            {"d": target_str},
        ).fetchall()
    already_done = {r[0] for r in rows}
    return [sid for sid in all_ids if sid not in already_done]


def get_margin_stats(target_date: date | None = None) -> dict:
    """
    回傳統計資訊：
      total_cached   — margin_cache 中不重複股票總數
      done_today     — 今日有融資資料的股票數
      newest_fetch   — 最新一筆 fetch_at 時間
    """
    if target_date is None:
        target_date = date.today()
    target_str = target_date.strftime("%Y-%m-%d")

    with get_session() as sess:
        row = sess.execute(text("""
            SELECT
                COUNT(DISTINCT stock_id),
                MAX(fetch_at)
            FROM margin_cache
        """)).fetchone()
        done_row = sess.execute(text("""
            SELECT COUNT(DISTINCT stock_id)
            FROM margin_cache
            WHERE date = :d
        """), {"d": target_str}).fetchone()

    return {
        "total_cached":  row[0] or 0,
        "done_today":    done_row[0] or 0,
        "newest_fetch":  row[1],
    }


def delete_old_margin(keep_days: int = MARGIN_RETAIN_DAYS) -> int:
    """清理 keep_days 天前的資料，回傳刪除筆數。"""
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    with get_session() as sess:
        result = sess.execute(
            text("DELETE FROM margin_cache WHERE date < :cutoff"),
            {"cutoff": cutoff},
        )
        sess.commit()
        return result.rowcount
