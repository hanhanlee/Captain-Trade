"""
Broker branch main-force cache.

FinMind TaiwanStockTradingDailyReport can only be queried one stock/day at a
time, so cache the derived daily top-15 broker net values locally.
"""
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from .database import get_session


def ensure_broker_cache_table():
    with get_session() as sess:
        sess.execute(text("""
            CREATE TABLE IF NOT EXISTS broker_main_force_cache (
                stock_id     TEXT NOT NULL,
                date         TEXT NOT NULL,
                buy_top15    REAL DEFAULT 0,
                sell_top15   REAL DEFAULT 0,
                net          REAL DEFAULT 0,
                broker_count INTEGER DEFAULT 0,
                fetched_at   TEXT,
                PRIMARY KEY (stock_id, date)
            )
        """))
        sess.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_broker_main_force_stock_date
            ON broker_main_force_cache (stock_id, date)
        """))
        sess.commit()


def load_broker_main_force(stock_id: str, dates: list[str]) -> pd.DataFrame:
    ensure_broker_cache_table()
    clean_dates = [str(d)[:10] for d in dates if d]
    if not clean_dates:
        return pd.DataFrame()

    placeholders = ",".join(f":d{i}" for i in range(len(clean_dates)))
    params = {"sid": stock_id}
    params.update({f"d{i}": d for i, d in enumerate(clean_dates)})

    with get_session() as sess:
        rows = sess.execute(text(f"""
            SELECT date, buy_top15, sell_top15, net, broker_count, fetched_at
            FROM broker_main_force_cache
            WHERE stock_id = :sid AND date IN ({placeholders})
            ORDER BY date ASC
        """), params).fetchall()

    if not rows:
        return pd.DataFrame(columns=[
            "date", "buy_top15", "sell_top15", "net", "broker_count", "fetched_at"
        ])

    df = pd.DataFrame(rows, columns=[
        "date", "buy_top15", "sell_top15", "net", "broker_count", "fetched_at"
    ])
    df["date"] = pd.to_datetime(df["date"])
    for col in ["buy_top15", "sell_top15", "net", "broker_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def save_broker_main_force(stock_id: str, rows: list[dict]) -> int:
    ensure_broker_cache_table()
    if not rows:
        return 0

    now = datetime.now().isoformat()
    payload = []
    for row in rows:
        d = row.get("date")
        if hasattr(d, "date"):
            d = d.date().isoformat()
        else:
            d = str(d)[:10]
        payload.append({
            "stock_id": stock_id,
            "date": d,
            "buy_top15": float(row.get("buy_top15") or 0),
            "sell_top15": float(row.get("sell_top15") or 0),
            "net": float(row.get("net") or 0),
            "broker_count": int(row.get("broker_count") or 0),
            "fetched_at": now,
        })

    with get_session() as sess:
        sess.execute(text("""
            INSERT OR REPLACE INTO broker_main_force_cache
                (stock_id, date, buy_top15, sell_top15, net, broker_count, fetched_at)
            VALUES
                (:stock_id, :date, :buy_top15, :sell_top15, :net, :broker_count, :fetched_at)
        """), payload)
        sess.commit()

    return len(payload)
