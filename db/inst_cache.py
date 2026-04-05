"""
三大法人買賣超本機快取

TTL 設計：24 小時。
  - 交易日：每天最多抓一次，日內多次掃描共用快取
  - 休市期間：資料不變，直接讀快取，完全不消耗 API
"""
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from .database import get_session, ENGINE
from .models import Base

INST_CACHE_TTL_HOURS = 24


def is_inst_fresh(stock_id: str, max_age_hours: int = INST_CACHE_TTL_HOURS) -> bool:
    """
    回傳 True 表示快取夠新（不需重新 API 請求）。

    休市模式開啟時：只要有任何快取就視為新鮮（不過期），
    完全不消耗 API 額度。
    """
    with get_session() as sess:
        row = sess.execute(
            text("SELECT MAX(fetched_at) FROM inst_cache WHERE stock_id = :sid"),
            {"sid": stock_id},
        ).fetchone()
    if not row or not row[0]:
        return False  # 沒有任何快取，必須抓取

    # 休市模式：有快取就直接用，不管多舊
    from db.settings import is_market_closed
    if is_market_closed():
        return True

    fetched = datetime.fromisoformat(str(row[0])) if isinstance(row[0], str) else row[0]
    return (datetime.now() - fetched).total_seconds() < max_age_hours * 3600


def save_institutional(stock_id: str, df: pd.DataFrame) -> int:
    """儲存法人資料到快取，相同 (stock_id, date, name) 直接覆寫"""
    if df.empty:
        return 0

    now_str = datetime.now().isoformat()
    rows = []
    for _, row in df.iterrows():
        d = row["date"]
        if hasattr(d, "date"):
            d = d.date().isoformat()
        elif hasattr(d, "isoformat"):
            d = d.isoformat()[:10]
        else:
            d = str(d)[:10]
        rows.append({
            "stock_id": stock_id,
            "date": d,
            "name": str(row.get("name", "")),
            "buy":  float(row["buy"])  if pd.notna(row.get("buy"))  else 0.0,
            "sell": float(row["sell"]) if pd.notna(row.get("sell")) else 0.0,
            "net":  float(row["net"])  if pd.notna(row.get("net"))  else 0.0,
            "fetched_at": now_str,
        })

    if not rows:
        return 0

    sql = text("""
        INSERT OR REPLACE INTO inst_cache
            (stock_id, date, name, buy, sell, net, fetched_at)
        VALUES
            (:stock_id, :date, :name, :buy, :sell, :net, :fetched_at)
    """)
    with get_session() as sess:
        sess.execute(sql, rows)
        sess.commit()
    return len(rows)


def load_institutional(stock_id: str, days: int = 10) -> pd.DataFrame:
    """從快取讀取法人資料（欄位與 FinMind 原始格式一致）"""
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with get_session() as sess:
        rows = sess.execute(
            text("""
                SELECT date, name, buy, sell, net
                FROM inst_cache
                WHERE stock_id = :sid AND date >= :start
                ORDER BY date ASC
            """),
            {"sid": stock_id, "start": start},
        ).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["date", "name", "buy", "sell", "net"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def get_inst_cache_stats() -> dict:
    """回傳快取統計資訊（供資料管理頁面顯示）"""
    with get_session() as sess:
        row = sess.execute(text("""
            SELECT COUNT(DISTINCT stock_id),
                   MIN(fetched_at),
                   MAX(fetched_at)
            FROM inst_cache
        """)).fetchone()
    return {
        "stock_count": row[0] or 0,
        "oldest_fetch": row[1],
        "newest_fetch": row[2],
    }
