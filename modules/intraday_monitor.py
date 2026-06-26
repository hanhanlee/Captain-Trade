"""
盤中持股監控

每分鐘掃描 intraday_monitor=True 的持股，判斷：
  - 現價 <= stop_loss（若有設）
  - 現價 >= take_profit（若有設）
  - 未設停損且未實現虧損 <= -5%（提醒設停損）
  - 接近漲停 / 接近跌停（需 Shioaji 漲跌停資料）

註：跌破 MA5 / MA10 / MA20 的提醒已於 2026-06-26 移除——盤中價在均線上下
穿梭雜訊太多，只保留停損等決策型提醒。

觸發時同步推 LINE + Telegram。同一檔同一條件 60 分鐘內只推一次（in-memory cooldown）。
"""
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

COOLDOWN_MINUTES = 60
_SHIOAJI_LOGIN_RETRY_MINUTES = 10   # 登入失敗後至少等幾分鐘再重試

# {stock_id: {condition_key: last_alert_datetime}}
_cooldown: dict[str, dict[str, datetime]] = {}
_last_shioaji_login_attempt: Optional[datetime] = None


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


def _check_one(
    holding: dict,
    broker_data: dict | None = None,
) -> tuple[list[str], list[str], float | None, str]:
    """
    檢查單一持股的盤中警示條件。
    broker_data 由 run_intraday_check() 批次預取（Shioaji snapshot），有則優先使用。
    回傳 (alerts, keys_to_mark, price, source)；呼叫端發送成功後才呼叫 _mark()。
    """
    from data.finmind_client import get_kbar_latest, get_realtime_stock_snapshot

    stock_id = str(holding["stock_id"])
    stop_loss = holding.get("stop_loss")
    take_profit = holding.get("take_profit")
    cost_price = _to_float(holding.get("cost_price"))

    # 優先使用 Shioaji 批次預取的即時報價
    price: float | None = None
    source: str = ""
    limit_up: float | None = None
    limit_down: float | None = None
    reference_price: float | None = None

    if broker_data:
        price = _to_float(broker_data.get("last_price"))
        if price is not None:
            source = "Shioaji"
            limit_up = _to_float(broker_data.get("limit_up"))
            limit_down = _to_float(broker_data.get("limit_down"))
            reference_price = _to_float(broker_data.get("reference_price"))

    # Fallback：FinMind 即時快照 → FinMind 分K → Yahoo
    if price is None:
        try:
            snapshot = get_realtime_stock_snapshot(stock_id)
            price = _to_float(snapshot.get("close")) if snapshot else None
            if price is not None:
                source = "FinMind 即時"
        except Exception as e:
            logger.warning(f"get_realtime_stock_snapshot {stock_id}: {e}")

    if price is None:
        try:
            price = get_kbar_latest(stock_id)
            if price is not None:
                source = "FinMind 分K"
        except Exception as e:
            logger.warning(f"get_kbar_latest {stock_id}: {e}")

    if price is None:
        price = _yahoo_current_price(stock_id)
        if price is not None:
            source = "Yahoo Finance"

    if price is None:
        return [], [], None, ""

    alerts: list[str] = []
    keys: list[str] = []

    # 均線跌破提醒（MA5/MA10/MA20）已於 2026-06-26 移除：盤中價在均線上下穿梭
    # 雜訊過多，只保留停損 / 停利 / 接近漲跌停 / 未設停損-5% 等決策型提醒。

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

    # ── Shioaji 加強警示（需要 broker_data 提供漲跌停）─────────────────────
    if limit_up and price:
        limit_up_suspect = False
        if reference_price and reference_price > 0:
            expected = reference_price * 1.10
            if abs(limit_up - expected) / expected > 0.01:
                limit_up_suspect = True
                logger.warning(
                    f"{stock_id} limit_up 資料可疑：limit_up={limit_up}, "
                    f"reference={reference_price}, expected≈{expected:.2f}"
                )

        dist_up_pct = (limit_up - price) / price * 100
        if dist_up_pct <= 3.0 and _cooled_down(stock_id, "near_limit_up"):
            msg = f"現價 {price:.2f} 接近漲停 {limit_up:.2f}（距離 {dist_up_pct:.1f}%），不建議追價"
            if limit_up_suspect:
                msg += "，目前漲停價資料可能有誤，請再人工確認"
            alerts.append(msg)
            keys.append("near_limit_up")

    if limit_down and price:
        dist_down_pct = (price - limit_down) / price * 100
        if 0 <= dist_down_pct <= 5.0 and _cooled_down(stock_id, "near_limit_down"):
            alerts.append(f"現價 {price:.2f} 接近跌停 {limit_down:.2f}（距離 {dist_down_pct:.1f}%）")
            keys.append("near_limit_down")

    return alerts, keys, price, source


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

    # Shioaji 批次預取（一次 API call 取得所有持股的即時報價 + 漲跌停）
    broker_snapshots: dict[str, dict] = {}
    try:
        from broker.shioaji_adapter import get_adapter
        adapter = get_adapter()

        # 尚未登入時嘗試自動登入（失敗後等 _SHIOAJI_LOGIN_RETRY_MINUTES 分鐘再重試）
        if not adapter.is_logged_in():
            global _last_shioaji_login_attempt
            now = datetime.now()
            retry_ok = (
                _last_shioaji_login_attempt is None
                or (now - _last_shioaji_login_attempt).total_seconds()
                >= _SHIOAJI_LOGIN_RETRY_MINUTES * 60
            )
            if retry_ok:
                logger.info("Shioaji 尚未登入，嘗試自動登入…")
                _last_shioaji_login_attempt = now
                adapter.login()

        if adapter.is_logged_in():
            all_ids = [str(h["stock_id"]) for h in holdings]
            broker_snapshots = adapter.get_snapshots(all_ids)
            if broker_snapshots:
                logger.info("Shioaji 批次報價：取得 %d 檔", len(broker_snapshots))
    except Exception as exc:
        logger.debug("Shioaji batch fetch skipped: %s", exc)

    sent = 0
    now_str = datetime.now().strftime("%H:%M")

    for h in holdings:
        stock_id = str(h["stock_id"])
        stock_name = h["stock_name"]
        broker_data = broker_snapshots.get(stock_id)
        alerts, keys, price, source = _check_one(h, broker_data=broker_data)

        if price is None:
            log_event("intraday_price_fail", module="intraday_monitor",
                      stock_id=stock_id, stock_name=stock_name, severity="warning",
                      summary=f"{stock_id} 取價失敗，跳過本輪檢查")
            continue

        if not alerts:
            continue

        label = f"{stock_id} {stock_name}".strip()
        lines = [f"📡 盤中警示 {label}（{now_str}）"] + [f"  • {a}" for a in alerts]
        if source:
            lines.append(f"📊 報價來源：{source}")
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
