"""
本機歷史價格快取管理
- 一次性下載，儲存到 SQLite，之後回測直接讀本機
- 支援增量更新（只補最新的資料）
"""
import pandas as pd
from datetime import date, datetime, timedelta
from sqlalchemy import text
from .database import get_session, ENGINE, init_db
from .models import PriceCache, Base


def init_cache_table():
    Base.metadata.create_all(ENGINE)


def get_cached_dates(stock_id: str) -> tuple:
    """回傳該股在快取中的最早/最新日期，若無則回傳 (None, None)"""
    with get_session() as sess:
        result = sess.execute(
            text("SELECT MIN(date), MAX(date) FROM price_cache WHERE stock_id = :sid"),
            {"sid": stock_id}
        ).fetchone()
        return result[0], result[1]


def save_prices(stock_id: str, df: pd.DataFrame):
    """
    將日K DataFrame 寫入快取，已存在的 (stock_id, date) 自動跳過
    df 欄位：date, open, max(=high), min(=low), close, Trading_Volume
    """
    if df.empty:
        return 0

    rows = []
    for _, row in df.iterrows():
        d = row["date"].date() if hasattr(row["date"], "date") else row["date"]
        rows.append({
            "stock_id": stock_id,
            "date": d,
            "open": row.get("open"),
            "high": row.get("max"),
            "low": row.get("min"),
            "close": row.get("close"),
            "volume": row.get("Trading_Volume"),
        })

    with get_session() as sess:
        inserted = 0
        for r in rows:
            exists = sess.execute(
                text("SELECT 1 FROM price_cache WHERE stock_id=:sid AND date=:d"),
                {"sid": r["stock_id"], "d": r["date"]}
            ).fetchone()
            if not exists:
                sess.execute(
                    text("""INSERT INTO price_cache
                            (stock_id, date, open, high, low, close, volume, updated_at)
                            VALUES (:stock_id, :date, :open, :high, :low, :close, :volume, :ts)"""),
                    {**r, "ts": datetime.now()}
                )
                inserted += 1
        sess.commit()
    return inserted


def load_prices(stock_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """從本機快取讀取日K資料"""
    query = "SELECT date, open, high, low, close, volume FROM price_cache WHERE stock_id = :sid"
    params = {"sid": stock_id}

    if start_date:
        query += " AND date >= :start"
        params["start"] = start_date
    if end_date:
        query += " AND date <= :end"
        params["end"] = end_date

    query += " ORDER BY date ASC"

    with get_session() as sess:
        result = sess.execute(text(query), params).fetchall()

    if not result:
        return pd.DataFrame()

    df = pd.DataFrame(result, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 對齊 finmind_client 的欄位名稱，讓 scanner 可以直接使用
    df = df.rename(columns={"high": "max", "low": "min", "volume": "Trading_Volume"})
    return df


def get_all_cached_stocks() -> list:
    """回傳快取中有資料的股票代碼列表"""
    with get_session() as sess:
        rows = sess.execute(
            text("SELECT DISTINCT stock_id FROM price_cache ORDER BY stock_id")
        ).fetchall()
    return [r[0] for r in rows]


def get_cache_summary() -> pd.DataFrame:
    """回傳快取狀態摘要表"""
    with get_session() as sess:
        rows = sess.execute(text("""
            SELECT stock_id,
                   MIN(date) as earliest,
                   MAX(date) as latest,
                   COUNT(*) as days
            FROM price_cache
            GROUP BY stock_id
            ORDER BY stock_id
        """)).fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=["stock_id", "earliest", "latest", "days"])
