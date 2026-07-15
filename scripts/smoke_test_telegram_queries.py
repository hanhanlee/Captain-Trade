# Telegram 查詢指令直測（不經 Telegram、不發訊息）
import logging
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.WARNING)

from modules.telegram_queries import (
    query_price, query_holdings, query_watchlists,
    query_chips, query_pnl, query_brief,
)

CASES = [
    ("/p 2330", query_price, ("2330",)),
    ("/hold", query_holdings, ()),
    ("/watch", query_watchlists, ()),
    ("/chips 2330", query_chips, ("2330",)),
    ("/pnl", query_pnl, ()),
    ("/brief", query_brief, ()),
    ("/p 亂打", query_price, ("abc!!",)),
    ("/p 不存在", query_price, ("99999",)),
]

for label, fn, args in CASES:
    print("=" * 22, label, "=" * 22)
    try:
        out = fn(*args)
        print(out)
        assert isinstance(out, str) and out, "empty output"
        assert len(out) <= 4096, f"exceeds telegram limit: {len(out)}"
    except Exception:
        traceback.print_exc()
    print()

print("ALL DONE")
