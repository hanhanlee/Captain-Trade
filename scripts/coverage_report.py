"""Coverage report for today's data fetch — prints to stdout and (optional) Telegram.

Usage:
    python scripts/coverage_report.py              # print only
    python scripts/coverage_report.py --telegram   # also push to TELEGRAM_STOCK_CHAT_ID
"""
import io
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")


def coverage(con: sqlite3.Connection, table: str) -> tuple[str | None, int, str | None, int]:
    rows = list(
        con.execute(
            f"SELECT date, COUNT(DISTINCT stock_id) FROM {table} "
            f"GROUP BY date ORDER BY date DESC LIMIT 2"
        )
    )
    if not rows:
        return None, 0, None, 0
    today_row = rows[0]
    prev = rows[1] if len(rows) > 1 else (None, 0)
    return today_row[0], today_row[1], prev[0], prev[1]


def build_report() -> str:
    db = ROOT / "srock.db"
    con = sqlite3.connect(str(db))
    labels = [("price_cache", "股價"), ("inst_cache", "法人"), ("margin_cache", "融資")]

    ref_date = None
    lines = []
    for tbl, lab in labels:
        d, n, prev_d, prev_n = coverage(con, tbl)
        if d and not ref_date:
            ref_date = d
        pct = round(100 * n / prev_n) if prev_n else 0
        lines.append(f"{lab}: {n}/{prev_n} 檔 ({pct}%)  vs {prev_d}")

    header = f"📊 資料覆蓋率（最新交易日 {ref_date or '—'}）"
    return header + "\n" + "\n".join(lines)


def main() -> int:
    if hasattr(sys.stdout, "buffer"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    msg = build_report()
    print(msg)

    if "--telegram" in sys.argv:
        from notifications.telegram_notify import send_message

        ok = send_message(msg)
        if not ok:
            print("[warn] Telegram send failed", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
