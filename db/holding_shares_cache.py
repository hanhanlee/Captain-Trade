"""
Cache for FinMind TaiwanStockHoldingSharesPer summaries.

The raw dataset is distribution-level data. Store only the derived fields used by
the app so scans and pages can read Premium signals without repeatedly calling
the Premium API.
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
from sqlalchemy import text

from .database import get_session


def ensure_holding_shares_table() -> None:
    with get_session() as sess:
        sess.execute(text("""
            CREATE TABLE IF NOT EXISTS holding_shares_cache (
                stock_id       TEXT NOT NULL,
                date           TEXT NOT NULL,
                above_400_pct  REAL,
                above_1000_pct REAL,
                below_10_pct   REAL,
                fetched_at     TEXT,
                PRIMARY KEY (stock_id, date)
            )
        """))
        sess.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_holding_shares_stock_date
            ON holding_shares_cache (stock_id, date)
        """))
        sess.commit()


def save_holding_shares(rows: list[dict]) -> int:
    ensure_holding_shares_table()
    if not rows:
        return 0

    now = datetime.now().isoformat()
    payload = []
    for row in rows:
        stock_id = str(row.get("stock_id") or "").strip()
        d = row.get("date")
        if hasattr(d, "date"):
            d = d.date().isoformat()
        else:
            d = str(d or "")[:10]
        if not stock_id or not d:
            continue
        payload.append({
            "stock_id": stock_id,
            "date": d,
            "above_400_pct": _nullable_float(row.get("above_400_pct")),
            "above_1000_pct": _nullable_float(row.get("above_1000_pct")),
            "below_10_pct": _nullable_float(row.get("below_10_pct")),
            "fetched_at": row.get("fetched_at") or now,
        })

    if not payload:
        return 0

    with get_session() as sess:
        sess.execute(text("""
            INSERT OR REPLACE INTO holding_shares_cache
                (
                    stock_id, date, above_400_pct, above_1000_pct,
                    below_10_pct, fetched_at
                )
            VALUES
                (
                    :stock_id, :date, :above_400_pct, :above_1000_pct,
                    :below_10_pct, :fetched_at
                )
        """), payload)
        sess.commit()
    return len(payload)


def load_holding_shares(
    stock_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    ensure_holding_shares_table()
    clauses = []
    params: dict[str, str] = {}
    if stock_id:
        clauses.append("stock_id = :sid")
        params["sid"] = str(stock_id)
    if start_date:
        clauses.append("date >= :start")
        params["start"] = str(start_date)[:10]
    if end_date:
        clauses.append("date <= :end")
        params["end"] = str(end_date)[:10]

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with get_session() as sess:
        rows = sess.execute(text(f"""
            SELECT
                stock_id, date, above_400_pct, above_1000_pct,
                below_10_pct, fetched_at
            FROM holding_shares_cache
            {where}
            ORDER BY date ASC, stock_id ASC
        """), params).fetchall()

    cols = [
        "stock_id", "date", "above_400_pct", "above_1000_pct",
        "below_10_pct", "fetched_at"
    ]
    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows, columns=cols)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ["above_400_pct", "above_1000_pct", "below_10_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _nullable_float(value):
    if value is None or pd.isna(value):
        return None
    return float(value)
