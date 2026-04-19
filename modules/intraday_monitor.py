"""
盤中持股監控

每分鐘掃描 intraday_monitor=True 的持股，判斷：
  - 現價 < MA5 / MA10 / MA20（日K MA，從本機快取計算）
  - 現價 <= stop_loss（若有設）
  - 現價 >= take_profit（若有設）

觸發時推 LINE 群播。同一檔同一條件 60 分鐘內只推一次（in-memory cooldown）。
"""
import logging
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

COOLDOWN_MINUTES = 60

# {stock_id: {condition_key: last_alert_datetime}}
_cooldown: dict[str, dict[str, datetime]] = {}


def _cooled_down(stock_id: str, key: str) -> bool:
    last = _cooldown.get(stock_id, {}).get(key)
    return last is None or (datetime.now() - last).total_seconds() >= COOLDOWN_MINUTES * 60


def _mark(stock_id: str, key: str) -> None:
    _cooldown.setdefault(stock_id, {})[key] = datetime.now()


def _daily_mas(stock_id: str) -> dict[str, float]:
    """從日K快取計算 MA5 / MA10 / MA20，回傳能算出的欄位。"""
    try:
        from db.price_cache import load_prices
        df = load_prices(stock_id, lookback_days=25)
        if df.empty or "close" not in df.columns:
            return {}
        closes = pd.to_numeric(df["close"], errors="coerce").dropna()
        result: dict[str, float] = {}
        for n, label in [(5, "ma5"), (10, "ma10"), (20, "ma20")]:
            if len(closes) >= n:
                result[label] = float(closes.tail(n).mean())
        return result
    except Exception as e:
        logger.debug(f"_daily_mas {stock_id}: {e}")
        return {}


def _check_one(holding: dict) -> list[str]:
    """
    檢查單一持股的盤中警示條件。
    回傳本次觸發的警示描述列表（已記錄 cooldown）。
    """
    from data.finmind_client import get_kbar_latest

    stock_id = str(holding["stock_id"])
    stop_loss = holding.get("stop_loss")
    take_profit = holding.get("take_profit")

    try:
        price = get_kbar_latest(stock_id)
    except Exception as e:
        logger.warning(f"get_kbar_latest {stock_id}: {e}")
        return []

    if price is None:
        return []

    mas = _daily_mas(stock_id)
    fired: list[str] = []

    for label, key in [("MA5", "ma5"), ("MA10", "ma10"), ("MA20", "ma20")]:
        val = mas.get(key)
        if val is None:
            continue
        cond = f"below_{key}"
        if price < val and _cooled_down(stock_id, cond):
            fired.append(f"現價 {price:.2f} 跌破 {label}（{val:.2f}）")
            _mark(stock_id, cond)

    if stop_loss and price <= stop_loss:
        if _cooled_down(stock_id, "stop_loss"):
            fired.append(f"現價 {price:.2f} 觸及停損（{stop_loss:.2f}）")
            _mark(stock_id, "stop_loss")

    if take_profit and price >= take_profit:
        if _cooled_down(stock_id, "take_profit"):
            fired.append(f"現價 {price:.2f} 觸及停利（{take_profit:.2f}）")
            _mark(stock_id, "take_profit")

    return fired


def run_intraday_check() -> int:
    """
    掃描所有 intraday_monitor=True 的持股，觸發警示則推 LINE。
    回傳本次送出的通知筆數（每檔最多一則，多條件合併成一訊息）。
    """
    from db.database import get_session
    from db.models import Portfolio
    from notifications.line_notify import send_multicast

    with get_session() as sess:
        rows = (
            sess.query(Portfolio)
            .filter(Portfolio.intraday_monitor == True)  # noqa: E712
            .all()
        )
        holdings = [
            {
                "stock_id": r.stock_id,
                "stock_name": r.stock_name or "",
                "stop_loss": r.stop_loss,
                "take_profit": r.take_profit,
            }
            for r in rows
        ]

    if not holdings:
        return 0

    sent = 0
    now_str = datetime.now().strftime("%H:%M")

    for h in holdings:
        alerts = _check_one(h)
        if not alerts:
            continue
        label = f"{h['stock_id']} {h['stock_name']}".strip()
        lines = [f"📡 盤中警示 {label}（{now_str}）"] + [f"  • {a}" for a in alerts]
        send_multicast("\n".join(lines))
        logger.info(f"盤中警示推播：{label} → {alerts}")
        sent += 1

    return sent
