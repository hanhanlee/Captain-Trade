"""
ETF 成分股持股快取

資料來源：FinMind TaiwanStockEtfHolding
TTL：ETF 持股每月調整一次，快取 TTL 設為 24 小時，避免每次重算都打 API。
"""
import logging
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import text
from .database import get_session, ENGINE

logger = logging.getLogger(__name__)

ETF_CACHE_TTL_HOURS = 24


def is_etf_fresh(etf_id: str, max_age_hours: int = ETF_CACHE_TTL_HOURS) -> bool:
    """回傳 True 表示快取仍在 TTL 內，不需重新抓取。"""
    with get_session() as sess:
        row = sess.execute(
            text("SELECT MAX(fetched_at) FROM etf_holding_cache WHERE etf_id = :eid"),
            {"eid": etf_id},
        ).fetchone()
    if not row or not row[0]:
        return False
    try:
        last_fetch = datetime.fromisoformat(str(row[0]))
        return (datetime.now() - last_fetch).total_seconds() < max_age_hours * 3600
    except Exception:
        return False


def save_etf_holdings(etf_id: str, df: pd.DataFrame) -> int:
    """
    儲存 ETF 成分股持股到快取。
    df 欄位：date, hold_stock_id, hold_stock_name, percentage
    相同 (etf_id, date, hold_stock_id) 直接覆寫。
    """
    if df.empty:
        return 0

    now_str = datetime.now().isoformat()
    rows = []
    for _, row in df.iterrows():
        d = row.get("date", "")
        if hasattr(d, "date"):
            d = d.date().isoformat()
        elif hasattr(d, "isoformat"):
            d = str(d)[:10]
        else:
            d = str(d)[:10]
        rows.append({
            "etf_id":          etf_id,
            "date":            d,
            "hold_stock_id":   str(row.get("hold_stock_id", "") or "").strip(),
            "hold_stock_name": str(row.get("hold_stock_name", "") or "").strip(),
            "percentage":      float(row.get("percentage") or 0),
            "fetched_at":      now_str,
        })

    if not rows:
        return 0

    with ENGINE.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO etf_holding_cache
                    (etf_id, date, hold_stock_id, hold_stock_name, percentage, fetched_at)
                VALUES
                    (:etf_id, :date, :hold_stock_id, :hold_stock_name, :percentage, :fetched_at)
                ON CONFLICT (etf_id, date, hold_stock_id) DO UPDATE SET
                    hold_stock_name = excluded.hold_stock_name,
                    percentage      = excluded.percentage,
                    fetched_at      = excluded.fetched_at
            """),
            rows,
        )
    return len(rows)


def load_etf_holdings(
    etf_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    從快取讀取 ETF 持股明細，回傳 DataFrame。
    欄位：etf_id, date, hold_stock_id, hold_stock_name, percentage
    """
    conditions = ["etf_id = :eid"]
    params: dict = {"eid": etf_id}
    if start_date:
        conditions.append("date >= :sd")
        params["sd"] = str(start_date)[:10]
    if end_date:
        conditions.append("date <= :ed")
        params["ed"] = str(end_date)[:10]

    sql = f"""
        SELECT etf_id, date, hold_stock_id, hold_stock_name, percentage
        FROM etf_holding_cache
        WHERE {' AND '.join(conditions)}
        ORDER BY date DESC, percentage DESC
    """
    with get_session() as sess:
        result = sess.execute(text(sql), params).fetchall()

    if not result:
        return pd.DataFrame(columns=["etf_id", "date", "hold_stock_id", "hold_stock_name", "percentage"])

    df = pd.DataFrame(result, columns=["etf_id", "date", "hold_stock_id", "hold_stock_name", "percentage"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["percentage"] = pd.to_numeric(df["percentage"], errors="coerce").fillna(0.0)
    return df


def get_latest_two_snapshots(etf_id: str) -> tuple[str, str]:
    """
    回傳最近兩個持股快照的日期 (latest, prev)。
    若快照不足兩個，prev 回傳空字串。
    """
    with get_session() as sess:
        rows = sess.execute(
            text("""
                SELECT DISTINCT date FROM etf_holding_cache
                WHERE etf_id = :eid
                ORDER BY date DESC LIMIT 2
            """),
            {"eid": etf_id},
        ).fetchall()

    dates = [str(r[0])[:10] for r in rows]
    latest = dates[0] if dates else ""
    prev   = dates[1] if len(dates) >= 2 else ""
    return latest, prev
