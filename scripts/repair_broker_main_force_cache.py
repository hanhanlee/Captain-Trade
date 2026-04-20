"""Remove broker-main-force cache rows produced by the old buy/sell mapping bug."""

from __future__ import annotations

import sqlite3
from pathlib import Path


DB_PATH = Path(__file__).resolve().parent.parent / "srock.db"


def main() -> int:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TEMP TABLE bad_broker AS
        SELECT stock_id, date
        FROM broker_main_force_cache
        WHERE broker_count > 0
          AND buy_top15 = 0
          AND sell_top15 = 0
          AND net = 0
    """)
    bad = int(cur.execute("SELECT COUNT(*) FROM bad_broker").fetchone()[0])
    cur.execute("""
        DELETE FROM broker_main_force_cache
        WHERE EXISTS (
            SELECT 1
            FROM bad_broker b
            WHERE b.stock_id = broker_main_force_cache.stock_id
              AND b.date = broker_main_force_cache.date
        )
    """)
    cur.execute("""
        DELETE FROM premium_fetch_status
        WHERE dataset = 'broker_main_force'
          AND EXISTS (
              SELECT 1
              FROM bad_broker b
              WHERE b.stock_id = premium_fetch_status.stock_id
                AND b.date = premium_fetch_status.as_of_date
          )
    """)
    con.commit()
    remaining = int(cur.execute("SELECT COUNT(*) FROM broker_main_force_cache").fetchone()[0])
    print({"deleted_bad_broker_rows": bad, "remaining_broker_rows": remaining})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
