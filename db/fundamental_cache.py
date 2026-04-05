"""
基本面財務指標本機快取（90 天 TTL）

儲存從 TaiwanStockFinancialStatements 計算出的關鍵財務指標。
財報每季更新，90 天快取足夠；背景工作器空閒時自動填充。
"""
from datetime import datetime, timedelta
from sqlalchemy import text
from .database import get_session

FUNDAMENTAL_TTL_DAYS = 90


def is_fundamental_fresh(stock_id: str, max_age_days: int = FUNDAMENTAL_TTL_DAYS) -> bool:
    with get_session() as sess:
        row = sess.execute(
            text("SELECT fetched_at FROM fundamental_cache WHERE stock_id = :sid"),
            {"sid": stock_id},
        ).fetchone()
    if not row or not row[0]:
        return False
    fetched = datetime.fromisoformat(str(row[0])) if isinstance(row[0], str) else row[0]
    return (datetime.now() - fetched).days < max_age_days


def save_fundamental(stock_id: str, metrics: dict) -> None:
    """儲存（或更新）基本面指標快取"""
    now_str = datetime.now().isoformat()
    with get_session() as sess:
        sess.execute(text("""
            INSERT INTO fundamental_cache
                (stock_id, eps_ttm, roe, operating_cf, debt_ratio,
                 gross_margin_latest, gross_margin_yoy, data_date, fetched_at)
            VALUES
                (:sid, :eps, :roe, :ocf, :dr, :gml, :gmy, :dd, :fa)
            ON CONFLICT(stock_id) DO UPDATE SET
                eps_ttm=:eps, roe=:roe, operating_cf=:ocf, debt_ratio=:dr,
                gross_margin_latest=:gml, gross_margin_yoy=:gmy,
                data_date=:dd, fetched_at=:fa
        """), {
            "sid": stock_id,
            "eps":  metrics.get("eps_ttm"),
            "roe":  metrics.get("roe"),
            "ocf":  metrics.get("operating_cf"),
            "dr":   metrics.get("debt_ratio"),
            "gml":  metrics.get("gross_margin_latest"),
            "gmy":  metrics.get("gross_margin_yoy"),
            "dd":   metrics.get("data_date", ""),
            "fa":   now_str,
        })
        sess.commit()


def load_fundamental(stock_id: str) -> dict:
    """從快取讀取基本面指標，無資料回傳空 dict"""
    with get_session() as sess:
        row = sess.execute(text("""
            SELECT eps_ttm, roe, operating_cf, debt_ratio,
                   gross_margin_latest, gross_margin_yoy, data_date
            FROM fundamental_cache WHERE stock_id = :sid
        """), {"sid": stock_id}).fetchone()
    if not row:
        return {}
    return {
        "eps_ttm":             row[0],
        "roe":                 row[1],
        "operating_cf":        row[2],
        "debt_ratio":          row[3],
        "gross_margin_latest": row[4],
        "gross_margin_yoy":    row[5],
        "data_date":           row[6],
    }


def get_fundamental_stats() -> dict:
    """回傳快取統計（供資料管理頁面顯示）"""
    cutoff = (datetime.now() - timedelta(days=FUNDAMENTAL_TTL_DAYS)).isoformat()
    with get_session() as sess:
        row = sess.execute(text("""
            SELECT COUNT(*),
                   SUM(CASE WHEN fetched_at >= :cutoff THEN 1 ELSE 0 END),
                   MAX(fetched_at)
            FROM fundamental_cache
        """), {"cutoff": cutoff}).fetchone()
    return {
        "total":        row[0] or 0,
        "fresh":        row[1] or 0,
        "newest_fetch": row[2],
    }


def get_stocks_needing_fundamental(all_stock_ids: list) -> list:
    """回傳尚無新鮮基本面快取的股票清單"""
    cutoff = (datetime.now() - timedelta(days=FUNDAMENTAL_TTL_DAYS)).isoformat()
    with get_session() as sess:
        rows = sess.execute(text("""
            SELECT stock_id FROM fundamental_cache WHERE fetched_at >= :cutoff
        """), {"cutoff": cutoff}).fetchall()
    fresh_ids = {r[0] for r in rows}
    return [sid for sid in all_stock_ids if sid not in fresh_ids]
