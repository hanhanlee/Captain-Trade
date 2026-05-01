"""
排程任務守門工具。

提供 `is_today_trading_day()` 與 `@skip_if_not_trading_day` decorator，
確保盤中/盤後任務只在台股實際交易日執行（國定假日如勞動節、農曆春節等
即使落在週一到週五，也不會誤觸發）。

每日結果以 in-memory 快取，盤中每分鐘任務不會重複打 FinMind API。
"""
from __future__ import annotations

import functools
import logging
from datetime import date

logger = logging.getLogger(__name__)

_today_trading_cache: dict[date, bool] = {}


def is_today_trading_day() -> bool:
    """
    今日是否為台股交易日。

    透過 FinMind 官方交易日曆（`is_taiwan_stock_trading_day`）查詢，
    結果以日期為 key 快取，當日只會打一次 API。

    若 FinMind 查詢失敗（例如服務暫掛），保險起見視為交易日繼續執行——
    寧可多送一則訊息，也不要在開盤日整批靜默漏掉。
    """
    from data.finmind_client import is_taiwan_stock_trading_day

    today = date.today()
    cached = _today_trading_cache.get(today)
    if cached is not None:
        return cached

    try:
        result = bool(is_taiwan_stock_trading_day(today))
    except Exception as exc:
        logger.warning("交易日查詢失敗（%s），保險起見視為交易日", exc)
        return True

    _today_trading_cache.clear()
    _today_trading_cache[today] = result
    return result


def skip_if_not_trading_day(job):
    """Decorator：今日非台股交易日時，直接 return None 跳過任務。"""

    @functools.wraps(job)
    def wrapper(*args, **kwargs):
        if not is_today_trading_day():
            logger.info("今日非台股交易日，跳過任務：%s", job.__name__)
            return None
        return job(*args, **kwargs)

    return wrapper
