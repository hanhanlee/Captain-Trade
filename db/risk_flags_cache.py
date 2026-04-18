"""
Premium official risk-flag cache.

Stores normalized risk flags from FinMind official datasets such as disposition,
suspended trading, and price limit data. The table is intentionally independent
from OHLCV price cache so Premium data can be disabled or cleared safely.
"""
from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from .database import get_session


def ensure_risk_flags_table() -> None:
    with get_session() as sess:
        sess.execute(text("""
            CREATE TABLE IF NOT EXISTS risk_flags_cache (
                stock_id    TEXT NOT NULL,
                date        TEXT NOT NULL,
                flag_type   TEXT NOT NULL,
                detail      TEXT,
                fetched_at  TEXT,
                PRIMARY KEY (stock_id, date, flag_type)
            )
        """))
        sess.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_risk_flags_stock_date
            ON risk_flags_cache (stock_id, date)
        """))
        sess.commit()


def save_risk_flags(rows: list[dict]) -> int:
    ensure_risk_flags_table()
    if not rows:
        return 0

    now = datetime.now().isoformat()
    payload = []
    for row in rows:
        stock_id = str(row.get("stock_id") or "").strip()
        flag_type = str(row.get("flag_type") or "").strip()
        d = row.get("date")
        if hasattr(d, "date"):
            d = d.date().isoformat()
        else:
            d = str(d or "")[:10]
        if not stock_id or not flag_type or not d:
            continue
        detail = row.get("detail") or {}
        payload.append({
            "stock_id": stock_id,
            "date": d,
            "flag_type": flag_type,
            "detail": json.dumps(detail, ensure_ascii=False, default=str),
            "fetched_at": now,
        })

    if not payload:
        return 0

    with get_session() as sess:
        sess.execute(text("""
            INSERT OR REPLACE INTO risk_flags_cache
                (stock_id, date, flag_type, detail, fetched_at)
            VALUES
                (:stock_id, :date, :flag_type, :detail, :fetched_at)
        """), payload)
        sess.commit()
    return len(payload)


def load_risk_flags(
    stock_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    ensure_risk_flags_table()
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
            SELECT stock_id, date, flag_type, detail, fetched_at
            FROM risk_flags_cache
            {where}
            ORDER BY date ASC, stock_id ASC, flag_type ASC
        """), params).fetchall()

    if not rows:
        return pd.DataFrame(columns=["stock_id", "date", "flag_type", "detail", "fetched_at"])

    df = pd.DataFrame(rows, columns=["stock_id", "date", "flag_type", "detail", "fetched_at"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["detail"] = df["detail"].apply(_decode_detail)
    return df


def _decode_detail(raw):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {"raw": raw}
