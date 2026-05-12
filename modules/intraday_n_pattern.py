"""
盤中 N 字底突破即時偵測

每分鐘對 db.n_pattern_watchlist.today_active() 中尚未推播的候選做下列檢查：
  1. 用 Shioaji 批次取即時 last_price 與今日累積成交量
  2. is_breakout(price, b_price)：D > B 即視為突破
  3. 組 Telegram 訊息（含 A/B/C 結構、量能 ✓/✗、假突破提醒）→ 推播
  4. 推播成功後標記 alerted_today 防同檔重複通知

設計原則：
  - 若 Shioaji 未登入，本輪安靜跳過（不 fallback 到 FinMind 個別取價，保護配額）
  - 突破判斷沿用 P0 的 is_breakout（嚴格 D > B，無緩衝）
  - 訊息中明示「留意假突破」交由使用者人工判斷
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def _to_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        num = float(value)
        if num != num:   # NaN
            return None
        return num
    except (TypeError, ValueError):
        return None


def _format_date(value) -> str:
    """支援 date/datetime/str 三種格式，回傳 MM-DD。"""
    if value is None:
        return "?"
    try:
        if hasattr(value, "strftime"):
            return value.strftime("%m-%d")
        s = str(value)
        return s[5:10] if len(s) >= 10 else s
    except Exception:
        return str(value)


def format_breakout_alert(
    item: dict,
    price: float,
    total_volume: float | None,
    *,
    now_str: str | None = None,
    quote_source: str = "Shioaji",
) -> str:
    """
    組裝盤中 N 字底突破的 Telegram 訊息（純函式，可單元測試）。

    item 為 db.n_pattern_watchlist.today_active() 回傳的 dict。
    total_volume 為今日累積成交量；若為 None，量倍數行顯示「未知」。
    """
    sid = str(item.get("stock_id", ""))
    name = item.get("stock_name") or ""
    ind = item.get("industry") or ""
    a_p = float(item.get("a_price") or 0)
    b_p = float(item.get("b_price") or 0)
    c_p = float(item.get("c_price") or 0)
    a_d = _format_date(item.get("a_date"))
    b_d = _format_date(item.get("b_date"))
    c_d = _format_date(item.get("c_date"))
    b_rise = item.get("b_rise_pct")
    c_retrace = item.get("c_retrace_pct")
    vol_ab = item.get("vol_a_to_b_increase")
    vol_bc = item.get("vol_b_to_c_decrease")
    avg_vol_bc = item.get("avg_volume_b_to_c")

    breakout_pct = (price - b_p) / b_p * 100 if b_p > 0 else 0.0

    if total_volume is not None and avg_vol_bc and avg_vol_bc > 0:
        vol_ratio_str = f"{total_volume / avg_vol_bc:.2f}× （B→C 日均量基準）"
    else:
        vol_ratio_str = "未知（缺基準量或即時量）"

    def tick(b: bool | None) -> str:
        if b is True:
            return "✓"
        if b is False:
            return "✗"
        return "—"

    label = f"{sid} {name}".strip()
    when = now_str or datetime.now().strftime("%H:%M")

    lines = [
        f"🎯 盤中 N 字底突破：{label}（{when}）",
        f"產業：{ind}" if ind else "",
        "─ 結構 ─",
        f"  A={a_p:.2f}({a_d}) → B={b_p:.2f}({b_d}) → C={c_p:.2f}({c_d})",
    ]
    if b_rise is not None and c_retrace is not None:
        lines.append(f"  B 漲幅 +{b_rise:.1f}%　C 回測 {c_retrace:.0f}%")

    lines += [
        "─ 量能驗證 ─",
        f"  A→B 量增：{tick(vol_ab)}",
        f"  B→C 量縮：{tick(vol_bc)}",
        f"  今日累積量：{vol_ratio_str}",
        "─ 現價 ─",
        f"  {price:.2f}（突破 B +{breakout_pct:.2f}%）",
        "⚠ 留意假突破：突破後若快速跌回 B 以下，可能為誘多",
        f"📊 報價來源：{quote_source}",
    ]
    return "\n".join(line for line in lines if line)


def _get_shioaji_snapshots(stock_ids: list[str]) -> dict[str, dict]:
    """嘗試批次抓 Shioaji 報價；失敗或未登入回空 dict。"""
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
            logger.info("Shioaji 未登入，N 字底盤中檢查本輪略過")
            return {}
        return adapter.get_snapshots(stock_ids) or {}
    except Exception as exc:
        logger.warning("Shioaji 批次報價失敗：%s", exc)
        return {}


def run_n_pattern_check() -> int:
    """
    盤中對 watchlist 做一次 N 字底突破檢查。
    回傳本輪推播的 Telegram 訊息數。
    """
    from db import n_pattern_watchlist as wl
    from db.event_log import log_event
    from notifications.telegram_notify import send_stock_alert
    from modules.n_pattern_detector import is_breakout

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

        b_price = _to_float(it.get("b_price"))
        if not is_breakout(price, b_price):
            continue

        # C 回測過深守門：確保不推播已被 builder 排除條件的舊 watchlist 殘留資料
        c_retrace = _to_float(it.get("c_retrace_pct"))
        if c_retrace is not None and c_retrace >= 50.0:
            continue

        total_volume = _to_float(snap.get("total_volume"))
        msg = format_breakout_alert(
            it,
            price=price,
            total_volume=total_volume,
            now_str=now_str,
            quote_source="Shioaji",
        )

        # 樂觀標記：先標記再送，避免重試期間重複觸發
        wl.mark_alerted(sid)
        try:
            ok = send_stock_alert(msg)
        except Exception as exc:
            ok = False
            logger.warning("Telegram 推播失敗 %s: %s", sid, exc)

        log_event(
            "n_pattern_breakout_alert" if ok else "n_pattern_breakout_alert_failed",
            module="intraday_n_pattern",
            stock_id=sid,
            stock_name=it.get("stock_name"),
            severity="info" if ok else "error",
            summary=f"{sid} {it.get('stock_name','')} 突破 B={b_price:.2f}，現價 {price:.2f}（推播 {'成功' if ok else '失敗'}）",
            payload={
                "price": price,
                "b_price": b_price,
                "total_volume": total_volume,
                "avg_volume_b_to_c": it.get("avg_volume_b_to_c"),
                "vol_a_to_b_increase": it.get("vol_a_to_b_increase"),
                "vol_b_to_c_decrease": it.get("vol_b_to_c_decrease"),
            },
        )

        if ok:
            sent += 1
            logger.info("N 字底突破推播：%s 現價 %.2f > B=%.2f", sid, price, b_price)

    return sent
