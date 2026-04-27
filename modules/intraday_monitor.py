"""
盤中持股監控

每分鐘掃描 intraday_monitor=True 的持股，判斷：
  - 現價 < MA5 / MA10 / MA20（日K MA + 即時價納入視窗，與圖表工具一致）
  - 現價 <= stop_loss（若有設）
  - 現價 >= take_profit（若有設）

觸發時同步推 LINE + Telegram。同一檔同一條件 60 分鐘內只推一次（in-memory cooldown）。
"""
import logging
import time
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

COOLDOWN_MINUTES = 60

# {stock_id: {condition_key: last_alert_datetime}}
_cooldown: dict[str, dict[str, datetime]] = {}


def _yahoo_current_price(stock_id: str) -> float | None:
    """Yahoo Finance fallback when FinMind Premium is unavailable."""
    try:
        import yfinance as yf
        for suffix in (".TW", ".TWO"):
            hist = yf.Ticker(f"{stock_id}{suffix}").history(period="1d", interval="1m")
            if not hist.empty:
                return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.debug(f"_yahoo_current_price {stock_id}: {e}")
    return None


def _cooled_down(stock_id: str, key: str) -> bool:
    last = _cooldown.get(stock_id, {}).get(key)
    return last is None or (datetime.now() - last).total_seconds() >= COOLDOWN_MINUTES * 60


def _mark(stock_id: str, key: str) -> None:
    _cooldown.setdefault(stock_id, {})[key] = datetime.now()


def _unmark(stock_id: str, key: str) -> None:
    _cooldown.get(stock_id, {}).pop(key, None)


def _daily_mas(stock_id: str, current_price: float | None = None) -> dict[str, float]:
    """
    從日K快取計算 MA5 / MA10 / MA20。
    若提供 current_price，將其附加到收盤序列末端（模擬今日收盤），
    使結果與圖表工具的即時 MA 一致。
    """
    try:
        from db.price_cache import load_prices
        df = load_prices(stock_id, lookback_days=25)
        if df.empty or "close" not in df.columns:
            return {}
        closes = pd.to_numeric(df["close"], errors="coerce").dropna()
        if current_price is not None:
            closes = pd.concat([closes, pd.Series([current_price])], ignore_index=True)
        result: dict[str, float] = {}
        for n, label in [(5, "ma5"), (10, "ma10"), (20, "ma20")]:
            if len(closes) >= n:
                result[label] = float(closes.tail(n).mean())
        return result
    except Exception as e:
        logger.debug(f"_daily_mas {stock_id}: {e}")
        return {}


def _check_one(holding: dict) -> tuple[list[str], list[str], float | None]:
    """
    檢查單一持股的盤中警示條件。
    回傳 (alerts, keys_to_mark, price)；呼叫端發送成功後才呼叫 _mark()。
    """
    from data.finmind_client import get_kbar_latest, get_realtime_stock_snapshot

    stock_id = str(holding["stock_id"])
    stop_loss = holding.get("stop_loss")
    take_profit = holding.get("take_profit")
    cost_price = _to_float(holding.get("cost_price"))

    try:
        snapshot = get_realtime_stock_snapshot(stock_id)
        price = _to_float(snapshot.get("close")) if snapshot else None
    except Exception as e:
        logger.warning(f"get_realtime_stock_snapshot {stock_id}: {e}")
        price = None

    if price is None:
        try:
            price = get_kbar_latest(stock_id)
        except Exception as e:
            logger.warning(f"get_kbar_latest {stock_id}: {e}")

    if price is None:
        price = _yahoo_current_price(stock_id)

    if price is None:
        return [], [], None

    mas = _daily_mas(stock_id, current_price=price)
    alerts: list[str] = []
    keys: list[str] = []

    for label, key in [("MA5", "ma5"), ("MA10", "ma10"), ("MA20", "ma20")]:
        val = mas.get(key)
        if val is None:
            continue
        cond = f"below_{key}"
        if price < val and _cooled_down(stock_id, cond):
            alerts.append(f"現價 {price:.2f} 跌破 {label}（{val:.2f}）")
            keys.append(cond)

    if stop_loss and price <= stop_loss:
        if _cooled_down(stock_id, "stop_loss"):
            alerts.append(f"現價 {price:.2f} 觸及停損（{stop_loss:.2f}）")
            keys.append("stop_loss")

    if take_profit and price >= take_profit:
        if _cooled_down(stock_id, "take_profit"):
            alerts.append(f"現價 {price:.2f} 觸及停利（{take_profit:.2f}）")
            keys.append("take_profit")

    if not stop_loss and cost_price and cost_price > 0:
        pnl_pct = (price - cost_price) / cost_price * 100
        if pnl_pct <= -5 and _cooled_down(stock_id, "unrealized_loss"):
            alerts.append(f"現價 {price:.2f} / 成本 {cost_price:.2f}，未實現虧損 {pnl_pct:.1f}%，建議設定停損")
            keys.append("unrealized_loss")

    return alerts, keys, price


def _to_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        num = float(value)
        if pd.isna(num):
            return None
        return num
    except (TypeError, ValueError):
        return None


def run_intraday_check() -> int:
    """
    掃描所有 intraday_monitor=True 的持股，觸發警示則推 LINE。
    回傳本次送出的通知筆數（每檔最多一則，多條件合併成一訊息）。
    """
    from db.database import get_session
    from db.models import Portfolio
    from notifications.line_notify import send_multicast
    from notifications.telegram_notify import send_stock_alert
    from db.event_log import log_event

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
                "cost_price": r.cost_price,
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
        stock_id = str(h["stock_id"])
        stock_name = h["stock_name"]
        alerts, keys, price = _check_one(h)

        if price is None:
            log_event("intraday_price_fail", module="intraday_monitor",
                      stock_id=stock_id, stock_name=stock_name, severity="warning",
                      summary=f"{stock_id} 取價失敗，跳過本輪檢查")
            continue

        if not alerts:
            continue

        label = f"{stock_id} {stock_name}".strip()
        lines = [f"📡 盤中警示 {label}（{now_str}）"] + [f"  • {a}" for a in alerts]
        msg = "\n".join(lines)

        log_event("intraday_alert_triggered", module="intraday_monitor",
                  stock_id=stock_id, stock_name=stock_name, severity="warning",
                  summary=f"{label} 警示：{', '.join(alerts)}",
                  payload={"price": price, "alerts": alerts, "conditions": keys})

        # 樂觀標記：送出前先鎖住 cooldown，防止 retry 等待期間重複觸發
        for key in keys:
            _mark(stock_id, key)
        line_ok = send_multicast(msg)
        time.sleep(1)
        tg_ok = send_stock_alert(msg)

        if line_ok or tg_ok:
            logger.info(f"盤中警示推播：{label} → {alerts}")
            log_event("intraday_alert_sent", module="intraday_monitor",
                      stock_id=stock_id, stock_name=stock_name, severity="info",
                      summary=f"{label} 警示已送出（LINE={'✓' if line_ok else '✗'} TG={'✓' if tg_ok else '✗'}）",
                      payload={"line_ok": line_ok, "tg_ok": tg_ok, "alerts": alerts})
            sent += 1
        else:
            for key in keys:
                _unmark(stock_id, key)
            logger.warning(f"盤中警示推播失敗（Line+Telegram 均未送出），等待下一輪重試：{label}")
            log_event("intraday_alert_failed", module="intraday_monitor",
                      stock_id=stock_id, stock_name=stock_name, severity="error",
                      summary=f"{label} 警示推播失敗，cooldown 已清除，等待下一輪重試",
                      payload={"alerts": alerts})

        time.sleep(1)

    return sent
