"""
本機歷史價格快取管理

效能設計：
  - 批次 INSERT OR IGNORE（一次 SQL 取代逐行）
  - 複合索引 (stock_id, date) 加速查詢
  - WAL mode + 32MB cache（在 database.py 設定）
"""
import pandas as pd
from datetime import date, datetime, timedelta
from sqlalchemy import text
from .database import get_session, ENGINE, init_db
from .models import Base


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


def save_prices(stock_id: str, df: pd.DataFrame) -> int:
    """
    批次寫入日K資料，已存在的 (stock_id, date) 自動跳過

    df 欄位：date, open, max(=high), min(=low), close, Trading_Volume
    """
    if df.empty:
        return 0

    rows = []
    for _, row in df.iterrows():
        d = row["date"].date() if hasattr(row["date"], "date") else row["date"]
        rows.append({
            "stock_id": stock_id,
            "date": str(d),
            "open":   float(row["open"])            if pd.notna(row.get("open"))            else None,
            "high":   float(row.get("max", row.get("high", None))) if pd.notna(row.get("max", row.get("high"))) else None,
            "low":    float(row.get("min", row.get("low",  None))) if pd.notna(row.get("min", row.get("low")))  else None,
            "close":  float(row["close"])           if pd.notna(row.get("close"))           else None,
            "volume": float(row.get("Trading_Volume", row.get("volume", None)))
                      if pd.notna(row.get("Trading_Volume", row.get("volume"))) else None,
            "ts": datetime.now().isoformat(),
        })

    if not rows:
        return 0

    sql = text("""
        INSERT OR IGNORE INTO price_cache
            (stock_id, date, open, high, low, close, volume, updated_at)
        VALUES
            (:stock_id, :date, :open, :high, :low, :close, :volume, :ts)
    """)

    with get_session() as sess:
        sess.execute(sql, rows)   # 批次執行，一次送出所有 rows
        sess.commit()

    return len(rows)


def load_prices(stock_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """從本機快取讀取日K資料（利用複合索引快速查詢）"""
    query = "SELECT date, open, high, low, close, volume FROM price_cache WHERE stock_id = :sid"
    params: dict = {"sid": stock_id}

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

    # 對齊 scanner 期望的欄位名稱
    return df.rename(columns={"high": "max", "low": "min", "volume": "Trading_Volume"})


def load_prices_multi(stock_ids: list, start_date: str = None) -> dict:
    """
    批次讀取多檔股票的快取資料（一次 SQL，比逐檔查詢快）

    回傳：{stock_id: DataFrame}
    """
    if not stock_ids:
        return {}

    placeholders = ",".join(f":id{i}" for i in range(len(stock_ids)))
    params: dict = {f"id{i}": sid for i, sid in enumerate(stock_ids)}

    query = f"SELECT stock_id, date, open, high, low, close, volume FROM price_cache WHERE stock_id IN ({placeholders})"
    if start_date:
        query += " AND date >= :start"
        params["start"] = start_date
    query += " ORDER BY stock_id, date ASC"

    with get_session() as sess:
        rows = sess.execute(text(query), params).fetchall()

    if not rows:
        return {}

    df_all = pd.DataFrame(rows, columns=["stock_id", "date", "open", "high", "low", "close", "volume"])
    df_all["date"] = pd.to_datetime(df_all["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        df_all[col] = pd.to_numeric(df_all[col], errors="coerce")
    df_all = df_all.rename(columns={"high": "max", "low": "min", "volume": "Trading_Volume"})

    return {sid: grp.drop(columns="stock_id").reset_index(drop=True)
            for sid, grp in df_all.groupby("stock_id")}


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


def delete_old_prices(keep_days: int = 400):
    """
    刪除超過 keep_days 天的舊資料，控制資料庫大小

    建議定期執行（例如每月一次），之後搭配 vacuum_db() 回收空間
    """
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=keep_days)).isoformat()
    with get_session() as sess:
        result = sess.execute(
            text("DELETE FROM price_cache WHERE date < :cutoff"),
            {"cutoff": cutoff}
        )
        sess.commit()
        return result.rowcount
