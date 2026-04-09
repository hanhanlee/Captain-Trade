import argparse
import sqlite3
from pathlib import Path


TARGET_LATEST_DATE = "2026-04-02"


def get_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "srock.db"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clear stale no_update flags for stocks stuck on a specific latest price date."
    )
    parser.add_argument(
        "--latest-date",
        default=TARGET_LATEST_DATE,
        help="Only clear rows whose latest price date matches this value.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete matching no_update rows. Without this flag, only print the count.",
    )
    args = parser.parse_args()

    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    query = """
    WITH target AS (
        SELECT p.stock_id
        FROM (
            SELECT stock_id, MAX(date) AS latest_date
            FROM price_cache
            GROUP BY stock_id
        ) p
        JOIN price_fetch_status s
          ON s.stock_id = p.stock_id
        WHERE p.latest_date = ?
          AND s.status = 'no_update'
    )
    SELECT stock_id FROM target ORDER BY stock_id
    """

    stock_ids = [row[0] for row in cur.execute(query, (args.latest_date,)).fetchall()]
    print(f"db={db_path}")
    print(f"latest_date={args.latest_date}")
    print(f"matched_no_update={len(stock_ids)}")

    if not args.apply:
        print("dry_run=true")
        return 0

    cur.executemany(
        "DELETE FROM price_fetch_status WHERE stock_id = ? AND status = 'no_update'",
        [(sid,) for sid in stock_ids],
    )
    conn.commit()
    print(f"deleted={cur.rowcount}")
    print("dry_run=false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
