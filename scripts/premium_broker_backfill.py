"""Run date-first market broker backfill in a background-friendly process."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.database import init_db
from scheduler.prefetch import get_worker


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.quiet:
        logging.disable(logging.CRITICAL)

    init_db()
    worker = get_worker()
    result = worker.prefetch_market_broker_by_date(days=max(args.days, 1))
    print(result, flush=True)
    print("status", worker.status(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
