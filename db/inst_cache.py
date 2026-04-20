"""
三大法人買賣超本機快取

TTL 設計：24 小時。
  - 交易日：每天最多抓一次，日內多次掃描共用快取
  - 休市期間：資料不變，直接讀快取，完全不消耗 API
"""
import pandas as pd
from datetime import date, datetime, timedelta
from sqlalchemy import text
from .database import get_session, ENGINE
from .models import Base

INST_CACHE_TTL_HOURS = 24


def _default_target_date() -> date:
    today = date.today()
    if today.weekday() == 5:
        return today - timedelta(days=1)
    if today.weekday() == 6:
        return today - timedelta(days=2)
    if datetime.now().hour < 15:
        target = today - timedelta(days=1)
        while target.weekday() >= 5:
            target -= timedelta(days=1)
        return target
    return today


def is_inst_fresh(
    stock_id: str,
    max_age_hours: int = INST_CACHE_TTL_HOURS,
    target_date: date | str | None = None,
) -> bool:
    """
    回傳 True 表示快取夠新（不需重新 API 請求）。

    快取必須已涵蓋 target_date 才算新鮮。max_age_hours 保留為相容舊呼叫，
    但不再只靠 fetched_at 判斷，避免把舊交易日資料誤當最新資料。
    """
    if target_date is None:
        target_date = _default_target_date()
    target_str = target_date.isoformat() if hasattr(target_date, "isoformat") else str(target_date)[:10]

    with get_session() as sess:
        row = sess.execute(
            text("SELECT MAX(date), MAX(fetched_at) FROM inst_cache WHERE stock_id = :sid"),
            {"sid": stock_id},
        ).fetchone()
    if not row or not row[0]:
        return False  # 沒有任何快取，必須抓取

    latest_date = str(row[0])[:10]
    if latest_date >= target_str:
        return True
    return False


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


def save_institutional_batch(df: pd.DataFrame) -> int:
    """
    批次儲存全市場法人資料（df 須含 stock_id 欄位）。
    通常由批次抓取全市場資料後呼叫，一次寫入數千筆，效率遠優於逐檔寫入。
    """
    if df.empty or "stock_id" not in df.columns:
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
        sid = str(row.get("stock_id", "")).strip()
        if not sid or not d:
            continue
        rows.append({
            "stock_id": sid,
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


def load_institutional_for_date(stock_id: str, target_date, days: int = 14) -> pd.DataFrame:
    """歷史掃描專用：讀取截至 target_date 的法人快取（不呼叫 live API）。"""
    target = pd.Timestamp(target_date).strftime("%Y-%m-%d")
    start = (pd.Timestamp(target_date) - pd.Timedelta(days=days * 2)).strftime("%Y-%m-%d")
    with get_session() as sess:
        rows = sess.execute(
            text("""
                SELECT date, name, buy, sell, net
                FROM inst_cache
                WHERE stock_id = :sid AND date >= :start AND date <= :target
                ORDER BY date ASC
            """),
            {"sid": stock_id, "start": start, "target": target},
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
