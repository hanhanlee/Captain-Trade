"""
盤中 V3 三線齊穿突破即時偵測

每分鐘對 db.v3_breakout_watchlist.today_active() 中尚未推播的候選做下列檢查：
  1. 用 Shioaji 批次取即時 last_price 與今日累積成交量
  2. 突破條件：current_price > ma5 AND current_price > ma10 AND current_price > ma20
  3. 量能條件：預估全日量 > vol_ma5 × 1.5
     預估全日量 = 當前累積量 × (270 / 已過交易分鐘數)
     台股交易時間 09:00–13:30，共 270 分鐘
  4. 兩條件同時成立 → 推 Telegram，標記 alerted_today 防重複
"""
from __future__ import annotations

import logging
from datetime import datetime, time as dtime

logger = logging.getLogger(__name__)

VOLUME_MULT = 1.5
_MARKET_OPEN_H, _MARKET_OPEN_M = 9, 0
_TOTAL_TRADING_MINUTES = 270   # 09:00–13:30


def _estimate_full_day_volume(current_volume: float, now: datetime) -> tuple[float, float]:
    """
    依當前時刻換算「預估全日成交量」。

    回傳 (estimated_volume, scale_factor)。
    scale_factor = 270 / elapsed_minutes；
    elapsed 至少計 5 分鐘，避免開盤前幾分鐘係數過大失真。
    """
    market_open = now.replace(hour=_MARKET_OPEN_H, minute=_MARKET_OPEN_M,
                               second=0, microsecond=0)
    elapsed = max((now - market_open).total_seconds() / 60, 5)
    elapsed = min(elapsed, _TOTAL_TRADING_MINUTES)
    factor = _TOTAL_TRADING_MINUTES / elapsed
    return current_volume * factor, round(factor, 1)


def _to_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        num = float(value)
        if num != num:
            return None
        return num
    except (TypeError, ValueError):
        return None


def format_v3_alert(
    item: dict,
    price: float,
    total_volume: float | None,
    estimated_volume: float | None,
    scale_factor: float | None,
    *,
    now_str: str | None = None,
    quote_source: str = "Shioaji",
) -> str:
    sid = str(item.get("stock_id", ""))
    name = item.get("stock_name") or ""
    ind = item.get("industry") or ""
    ma5 = float(item.get("ma5") or 0)
    ma10 = float(item.get("ma10") or 0)
    ma20 = float(item.get("ma20") or 0)
    vol_ma5 = float(item.get("vol_ma5") or 0)
    spread = item.get("ma_spread_pct")
    last_close = item.get("last_close")

    when = now_str or datetime.now().strftime("%H:%M")
    label = f"{sid} {name}".strip()

    if estimated_volume and vol_ma5 > 0:
        vol_line = (
            f"  累積 {total_volume:.0f} 張"
            f"（×{scale_factor} 推估全日 {estimated_volume:.0f} 張"
            f" = {estimated_volume / vol_ma5:.2f}× 五日均量）"
        )
    else:
        vol_line = "  量能未知"

    lines = [
        f"📈 盤中三線齊穿突破：{label}（{when}）",
        f"產業：{ind}" if ind else "",
        "─ 均線結構 ─",
        f"  昨收 {last_close:.2f} → 今突破 MA5={ma5:.2f} / MA10={ma10:.2f} / MA20={ma20:.2f}",
        f"  三線糾結度：{spread:.1f}%" if spread is not None else "",
        "─ 量能（預估全日）─",
        vol_line,
        "─ 現價 ─",
        f"  {price:.2f}",
        "⚠ 首日突破，注意假突破；量能持續放大為真突破加分訊號",
        f"📊 報價來源：{quote_source}",
    ]
    return "\n".join(line for line in lines if line)


def _get_shioaji_snapshots(stock_ids: list[str]) -> dict[str, dict]:
    if not stock_ids:
        return {}
    try:
        from broker.shioaji_adapter import get_adapter
        adapter = get_adapter()
        if not adapter.is_logged_in():
            try:
                adapter.login()
            except Exception as exc:
                logger.debug("Shioaji 自動登入失敗：%s", exc)
        if not adapter.is_logged_in():
            logger.info("Shioaji 未登入，V3 盤中檢查本輪略過")
            return {}
        return adapter.get_snapshots(stock_ids) or {}
    except Exception as exc:
        logger.warning("Shioaji 批次報價失敗：%s", exc)
        return {}


def run_v3_breakout_check() -> int:
    """
    盤中對 watchlist 做一次 V3 三線齊穿突破檢查。
    回傳本輪推播的 Telegram 訊息數。
    """
    from db import v3_breakout_watchlist as wl
    from db.event_log import log_event
    from notifications.telegram_notify import send_stock_alert

    items = wl.today_active()
    if not items:
        return 0

    sids = [str(it["stock_id"]) for it in items]
    snapshots = _get_shioaji_snapshots(sids)
    if not snapshots:
        return 0

    sent = 0
    now_str = datetime.now().strftime("%H:%M")

    for it in items:
        sid = str(it["stock_id"])
        snap = snapshots.get(sid)
        if not snap:
            continue

        price = _to_float(snap.get("last_price"))
        if price is None:
            continue

        ma5 = _to_float(it.get("ma5"))
        ma10 = _to_float(it.get("ma10"))
        ma20 = _to_float(it.get("ma20"))
        vol_ma5 = _to_float(it.get("vol_ma5"))

        if not (ma5 and ma10 and ma20 and vol_ma5):
            continue

        # 三線齊穿：現價必須同時高於三條均線
        if not (price > ma5 and price > ma10 and price > ma20):
            continue

        # 量能確認：預估全日量 > 五日均量 × 1.5
        total_volume = _to_float(snap.get("total_volume"))
        if total_volume is None:
            continue
        estimated_vol, scale = _estimate_full_day_volume(total_volume, datetime.now())
        if estimated_vol < vol_ma5 * VOLUME_MULT:
            continue

        msg = format_v3_alert(
            it,
            price=price,
            total_volume=total_volume,
            estimated_volume=estimated_vol,
            scale_factor=scale,
            now_str=now_str,
            quote_source="Shioaji",
        )

        wl.mark_alerted(sid)
        try:
            ok = send_stock_alert(msg)
        except Exception as exc:
            ok = False
            logger.warning("Telegram 推播失敗 %s: %s", sid, exc)

        log_event(
            "v3_breakout_alert" if ok else "v3_breakout_alert_failed",
            module="intraday_v3_breakout",
            stock_id=sid,
            stock_name=it.get("stock_name"),
            severity="info" if ok else "error",
            summary=(
                f"{sid} {it.get('stock_name', '')} 三線齊穿"
                f"（MA5={ma5:.2f}/MA10={ma10:.2f}/MA20={ma20:.2f}）"
                f" 現價 {price:.2f}，量 {total_volume:.0f} 張"
                f"（推播 {'成功' if ok else '失敗'}）"
            ),
            payload={
                "price": price,
                "ma5": ma5, "ma10": ma10, "ma20": ma20,
                "vol_ma5": vol_ma5,
                "total_volume": total_volume,
                "estimated_volume": estimated_vol,
                "vol_scale_factor": scale,
            },
        )

        if ok:
            sent += 1
            logger.info("V3 三線齊穿推播：%s 現價 %.2f（MA5=%.2f/MA10=%.2f/MA20=%.2f）",
                        sid, price, ma5, ma10, ma20)

    return sent
