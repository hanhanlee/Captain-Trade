"""
盤中 13:15 收盤前彙整推播（distinct from per-minute alerts）

每個交易日 13:15 一次，產出兩份獨立的 Telegram 訊息：
  1. 機會彙整 run_final_summary_check：四軌（V3/V5主/V5late/N 字底）watchlist
     中現仍符合「>5000 張 + 過突破點 ≤ 3%」的票，給「進場」最後機會。
  2. 停損彙整 run_stop_loss_summary_check：Portfolio 有設停損的持股中現價
     ≤ 停損的票，給「出場」收尾風控提示，繞過 intraday_monitor 60 分鐘 cooldown。
兩個函式分別寫 event_log 防重發，互不依賴。

設計選擇：
  - 時點 gate：嚴格 13:15（hour=13, minute=15），其餘時間 return 0
  - 候選來源：`today_all()` 而非 `today_active()` — 含已被早盤推過的票，
    讓使用者在一份訊息看到「現在仍合格的全部標的」
  - 重跑條件（與三軌 push 同邏輯）：
      * 現價 ≥ 突破目標
      * 現價 ≤ 突破目標 × 1.03（buffer 3%）
      * 預估全日量 ≥ 前 5 日均量 × 1.3（V5/V3）或 B→C 期間日均量 × 1.3（N 字底）
      * 預估全日量 ≥ 5000 張（MIN_LOTS）
      * V5 型態 B 候選：盤中重驗守月線（沿用 intraday_v5_breakout 邏輯）
  - 防重發：每日只發一次。透過查 event_log 有沒有當日 `intraday_final_summary_sent`
    來阻擋同一分鐘 scheduler 多次觸發。
  - 推播格式：分軌表格式精簡列表（4–14 檔範圍下可讀）

與既有四軌推播的關係：
  - 不動既有 module 的邏輯
  - 此 module 純讀 + 重判，不寫 mark_alerted（不影響後續輪次行為）
  - scheduler 加新 job `final_summary`，cron 13:15 觸發
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Iterable

logger = logging.getLogger(__name__)

MIN_LOTS = 5000              # 與四軌共同的量底
BREAKOUT_BUFFER_PCT = 3.0    # 過熱濾網
VOLUME_MULT = 1.3            # 量倍門檻（V3/V5；N 字底走自己的 vol gate）
_TOTAL_TRADING_MINUTES = 270
_MARKET_OPEN_H, _MARKET_OPEN_M = 9, 0


def _is_summary_window(now: datetime) -> bool:
    """彙整推播時點：嚴格 13:15（hour=13, minute=15）。"""
    t = now.time()
    return t.hour == 13 and t.minute == 15


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


def _estimate_full_day_volume(current_volume: float, now: datetime) -> tuple[float, float]:
    """current_volume 單位「張」；回傳 (estimated_full_day_lots, scale)。"""
    market_open = now.replace(hour=_MARKET_OPEN_H, minute=_MARKET_OPEN_M,
                              second=0, microsecond=0)
    elapsed = max((now - market_open).total_seconds() / 60, 5)
    elapsed = min(elapsed, _TOTAL_TRADING_MINUTES)
    factor = _TOTAL_TRADING_MINUTES / elapsed
    return current_volume * factor, round(factor, 1)


def _already_sent_today() -> bool:
    """查 event_log 看今日是否已發過彙整推播，防止同分鐘重複觸發。"""
    try:
        from db.database import get_session
        from db.models import EventLog
        from sqlalchemy import select, and_

        today_str = datetime.now().strftime("%Y-%m-%d")
        with get_session() as s:
            row = s.execute(
                select(EventLog.id).where(
                    and_(
                        EventLog.event_type == "intraday_final_summary_sent",
                        EventLog.created_at >= f"{today_str} 00:00:00",
                        EventLog.created_at < f"{today_str} 23:59:59",
                    )
                ).limit(1)
            ).first()
            return row is not None
    except Exception as exc:
        logger.debug("查彙整重發紀錄失敗（保守當未發）：%s", exc)
        return False


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
            logger.info("Shioaji 未登入，13:15 彙整推播本輪略過")
            return {}
        return adapter.get_snapshots(stock_ids) or {}
    except Exception as exc:
        logger.warning("Shioaji 批次報價失敗（final summary）：%s", exc)
        return {}


def _check_v3(item: dict, snap: dict, now: datetime) -> dict | None:
    """V3 三線齊穿：現價同時 > MA5/10/20，過 max(MA) buffer 內，量 ≥ MIN_LOTS。"""
    price = _to_float(snap.get("last_price"))
    if price is None:
        return None
    ma5 = _to_float(item.get("ma5"))
    ma10 = _to_float(item.get("ma10"))
    ma20 = _to_float(item.get("ma20"))
    vol_ma5 = _to_float(item.get("vol_ma5"))
    if not (ma5 and ma10 and ma20 and vol_ma5):
        return None
    if not (price > ma5 and price > ma10 and price > ma20):
        return None
    ref = max(ma5, ma10, ma20)
    if ref <= 0 or price > ref * (1 + BREAKOUT_BUFFER_PCT / 100):
        return None

    total_vol = _to_float(snap.get("total_volume"))
    if total_vol is None:
        return None
    est_vol, _ = _estimate_full_day_volume(total_vol, now)
    if est_vol < vol_ma5 * VOLUME_MULT:
        return None
    if est_vol < MIN_LOTS:
        return None

    return {
        "stock_id": item["stock_id"],
        "stock_name": item.get("stock_name") or "",
        "industry": item.get("industry") or "",
        "price": price,
        "breakout_ref": ref,
        "breakout_pct": (price - ref) / ref * 100,
        "estimated_volume": est_vol,
        "track_extra": "三線齊穿",
    }


def _check_v5_common(item: dict, snap: dict, now: datetime) -> dict | None:
    """
    V5 主 / V5 late 共用：兩者 watchlist row 結構相同，唯一差異是 entry_path。
    pattern B 候選會在後面 verify 一次守月線。
    """
    price = _to_float(snap.get("last_price"))
    if price is None:
        return None
    target = _to_float(item.get("breakout_target"))
    avg5_vol = _to_float(item.get("vol_ma5"))
    if target is None or avg5_vol is None or avg5_vol <= 0:
        return None
    if price < target:
        return None
    if price > target * (1 + BREAKOUT_BUFFER_PCT / 100):
        return None

    total_vol = _to_float(snap.get("total_volume"))
    if total_vol is None:
        return None
    est_vol, _ = _estimate_full_day_volume(total_vol, now)
    if est_vol < avg5_vol * VOLUME_MULT:
        return None
    if est_vol < MIN_LOTS:
        return None

    pat = item.get("pattern_type") or "?"

    # 型態 B 盤中重驗守月線（沿用 intraday_v5_breakout 邏輯）
    if pat == "B":
        try:
            from modules.intraday_v5_breakout import _verify_pattern_b_still_valid
            hit, _ = _verify_pattern_b_still_valid(item["stock_id"], price, now.date())
            if not hit:
                return None
        except Exception as exc:
            logger.debug("V5 B 重驗失敗 %s：%s", item.get("stock_id"), exc)

    prev_close = _to_float(item.get("prev_close"))
    gain_pct = (price - prev_close) / prev_close * 100 if prev_close and prev_close > 0 else None

    return {
        "stock_id": item["stock_id"],
        "stock_name": item.get("stock_name") or "",
        "industry": item.get("industry") or "",
        "price": price,
        "breakout_target": target,
        "breakout_pct": (price - target) / target * 100,
        "estimated_volume": est_vol,
        "pattern_type": pat,
        "gain_pct": gain_pct,
        "track_extra": f"{pat} 型",
    }


def _check_n_pattern(item: dict, snap: dict, now: datetime) -> dict | None:
    """N 字底：D > B + 過 B 點 buffer 內 + 量倍 + MIN_LOTS。"""
    from modules.n_pattern_detector import is_breakout

    price = _to_float(snap.get("last_price"))
    if price is None:
        return None
    b_price = _to_float(item.get("b_price"))
    if not is_breakout(price, b_price):
        return None
    if b_price and price > b_price * (1 + BREAKOUT_BUFFER_PCT / 100):
        return None

    c_retrace = _to_float(item.get("c_retrace_pct"))
    if c_retrace is not None and c_retrace >= 50.0:
        return None

    total_vol = _to_float(snap.get("total_volume"))
    avg_vol_bc = _to_float(item.get("avg_volume_b_to_c"))
    if total_vol is None or avg_vol_bc is None or avg_vol_bc <= 0:
        return None

    # avg_volume_b_to_c 單位「股」、total_volume 是「張」；推估全日量轉「股」比較量倍
    market_open = now.replace(hour=_MARKET_OPEN_H, minute=_MARKET_OPEN_M,
                              second=0, microsecond=0)
    elapsed = max((now - market_open).total_seconds() / 60, 5)
    elapsed = min(elapsed, _TOTAL_TRADING_MINUTES)
    factor = _TOTAL_TRADING_MINUTES / elapsed
    est_full_day_shares = total_vol * 1000.0 * factor
    if est_full_day_shares / avg_vol_bc < VOLUME_MULT:
        return None

    est_lots = est_full_day_shares / 1000.0
    if est_lots < MIN_LOTS:
        return None

    return {
        "stock_id": item["stock_id"],
        "stock_name": item.get("stock_name") or "",
        "industry": item.get("industry") or "",
        "price": price,
        "b_price": b_price,
        "breakout_pct": (price - b_price) / b_price * 100 if b_price else 0.0,
        "estimated_volume": est_lots,
        "track_extra": "N 字底",
    }


def _format_kilo(vol_lots: float) -> str:
    """量能格式化：5000 → 5.0k、120000 → 120k。"""
    if vol_lots >= 10000:
        return f"{vol_lots / 1000:.0f}k"
    return f"{vol_lots / 1000:.1f}k"


def _format_summary(
    *,
    v3_hits: list[dict],
    v5_main_hits: list[dict],
    v5_late_hits: list[dict],
    n_hits: list[dict],
    now_str: str,
) -> str | None:
    total = len(v3_hits) + len(v5_main_hits) + len(v5_late_hits) + len(n_hits)
    if total == 0:
        return None

    lines = [
        f"🏁 13:15 收盤前彙整（共 {total} 檔符合條件）",
        f"距 13:30 收盤剩 15 分鐘，請評估是否進場",
        "",
    ]

    def section(label: str, hits: list[dict], extra_formatter):
        if not hits:
            lines.append(f"[{label}]")
            lines.append("  （無）")
            lines.append("")
            return
        lines.append(f"[{label}]")
        for h in hits:
            lines.append("  " + extra_formatter(h))
        lines.append("")

    def fmt_v5(h):
        return (
            f"{h['stock_id']} {h['stock_name']} {h['pattern_type']}型 "
            f"{h['price']:.2f} 突破+{h['breakout_pct']:.2f}% "
            f"est {_format_kilo(h['estimated_volume'])}張"
        )

    def fmt_v3(h):
        return (
            f"{h['stock_id']} {h['stock_name']} "
            f"{h['price']:.2f} 過 max(MA)+{h['breakout_pct']:.2f}% "
            f"est {_format_kilo(h['estimated_volume'])}張"
        )

    def fmt_n(h):
        return (
            f"{h['stock_id']} {h['stock_name']} "
            f"{h['price']:.2f} 突破B+{h['breakout_pct']:.2f}% "
            f"est {_format_kilo(h['estimated_volume'])}張"
        )

    section("V5 主 checker", v5_main_hits, fmt_v5)
    section("V5 補抓 (late)", v5_late_hits, fmt_v5)
    section("V3 三線齊穿", v3_hits, fmt_v3)
    section("N 字底", n_hits, fmt_n)

    lines.append("⚠ 條件：>5000 張 + 過突破點 ≤ 3%")
    lines.append("📊 報價來源：Shioaji")
    return "\n".join(lines)


def run_final_summary_check() -> int:
    """
    13:15 一次性彙整推播。回傳是否發送（0/1）。
    scheduler 端設 cron 13:15，函式內仍做時點 gate + 防重發守門。
    """
    from db import (
        n_pattern_watchlist as n_wl,
        v3_breakout_watchlist as v3_wl,
        v5_sniper_watchlist as v5_wl,
    )
    from db.event_log import log_event
    from notifications.telegram_notify import send_stock_alert

    now = datetime.now()
    if not _is_summary_window(now):
        return 0
    if _already_sent_today():
        return 0

    # 蒐齊所有 watchlist 候選（含已 alerted）
    v3_items = list(v3_wl.today_all())
    v5_items = list(v5_wl.today_all())
    n_items = list(n_wl.today_all())

    all_sids = {str(it["stock_id"]) for it in v3_items}
    all_sids |= {str(it["stock_id"]) for it in v5_items}
    all_sids |= {str(it["stock_id"]) for it in n_items}
    if not all_sids:
        logger.info("13:15 彙整：四軌 watchlist 都空，略過")
        return 0

    snapshots = _get_shioaji_snapshots(sorted(all_sids))
    if not snapshots:
        log_event(
            "intraday_final_summary_skipped",
            module="intraday_final_summary",
            severity="warning",
            summary="13:15 機會彙整：Shioaji snapshots 取得失敗，本輪略過",
            payload={"checked_sids": len(all_sids)},
        )
        return 0

    v3_hits: list[dict] = []
    v5_main_hits: list[dict] = []
    v5_late_hits: list[dict] = []
    n_hits: list[dict] = []

    for it in v3_items:
        snap = snapshots.get(str(it["stock_id"]))
        if not snap:
            continue
        hit = _check_v3(it, snap, now)
        if hit:
            v3_hits.append(hit)

    for it in v5_items:
        snap = snapshots.get(str(it["stock_id"]))
        if not snap:
            continue
        hit = _check_v5_common(it, snap, now)
        if not hit:
            continue
        # 依 entry_path 分到主 / late
        if (it.get("entry_path") or "morning") == "late":
            v5_late_hits.append(hit)
        else:
            v5_main_hits.append(hit)

    for it in n_items:
        snap = snapshots.get(str(it["stock_id"]))
        if not snap:
            continue
        hit = _check_n_pattern(it, snap, now)
        if hit:
            n_hits.append(hit)

    msg = _format_summary(
        v3_hits=v3_hits, v5_main_hits=v5_main_hits,
        v5_late_hits=v5_late_hits, n_hits=n_hits,
        now_str=now.strftime("%H:%M"),
    )

    total = len(v3_hits) + len(v5_main_hits) + len(v5_late_hits) + len(n_hits)

    # 即使 total=0 仍寫 event_log heartbeat（讓 _already_sent_today 擋住補觸發）
    if msg is None:
        log_event(
            "intraday_final_summary_empty",
            module="intraday_final_summary",
            severity="info",
            summary="13:15 彙整：四軌均無符合條件的標的",
            payload={"checked_v3": len(v3_items), "checked_v5": len(v5_items),
                     "checked_n": len(n_items)},
        )
        return 0

    try:
        ok = send_stock_alert(msg)
    except Exception as exc:
        ok = False
        logger.warning("13:15 彙整 Telegram 推播失敗：%s", exc)

    log_event(
        "intraday_final_summary_sent" if ok else "intraday_final_summary_failed",
        module="intraday_final_summary",
        severity="info" if ok else "error",
        summary=(
            f"13:15 彙整推播：共 {total} 檔"
            f"（V5主 {len(v5_main_hits)} / V5late {len(v5_late_hits)} / "
            f"V3 {len(v3_hits)} / N {len(n_hits)}，{'成功' if ok else '失敗'}）"
        ),
        payload={
            "total": total,
            "v5_main": [h["stock_id"] for h in v5_main_hits],
            "v5_late": [h["stock_id"] for h in v5_late_hits],
            "v3": [h["stock_id"] for h in v3_hits],
            "n": [h["stock_id"] for h in n_hits],
            "min_lots": MIN_LOTS,
            "breakout_buffer_pct": BREAKOUT_BUFFER_PCT,
        },
    )
    return 1 if ok else 0


# ---------------------------------------------------------------------------
# 13:15 持股停損彙整推播（與機會彙整 run_final_summary_check 並存）
# ---------------------------------------------------------------------------

def _check_stop_loss_hit(holding: dict, snap: dict) -> dict | None:
    """單一持股當下是否觸及停損，命中則回傳含 price / pnl / stop_loss 的 dict。"""
    price = _to_float(snap.get("last_price"))
    stop_loss = _to_float(holding.get("stop_loss"))
    if price is None or stop_loss is None or stop_loss <= 0:
        return None
    if price > stop_loss:
        return None
    cost = _to_float(holding.get("cost_price"))
    pnl_pct = (price - cost) / cost * 100 if cost and cost > 0 else None
    return {
        "stock_id": str(holding.get("stock_id", "")),
        "stock_name": holding.get("stock_name") or "",
        "price": price,
        "stop_loss": stop_loss,
        "cost_price": cost,
        "pnl_pct": pnl_pct,
    }


def _format_stop_loss_summary(hits: list[dict], now_str: str) -> str | None:
    if not hits:
        return None
    lines = [
        f"🛡 13:15 持股停損彙整（共 {len(hits)} 檔觸及）",
        f"距 13:30 收盤剩 15 分鐘，請評估是否認賠出場",
        "",
    ]
    for h in hits:
        label = f"{h['stock_id']} {h['stock_name']}".strip()
        pnl_str = f"，未實現 {h['pnl_pct']:+.1f}%" if h.get("pnl_pct") is not None else ""
        lines.append(
            f"  {label}　現價 {h['price']:.2f} ≤ 停損 {h['stop_loss']:.2f}{pnl_str}"
        )
    lines.append("")
    lines.append("⚠ 條件：現價 ≤ 設定停損價")
    lines.append("📊 報價來源：Shioaji")
    return "\n".join(lines)


def _already_sent_stop_loss_today() -> bool:
    """查 event_log 看今日是否已發過停損彙整，防止同分鐘重複觸發。"""
    try:
        from db.database import get_session
        from db.models import EventLog
        from sqlalchemy import select, and_

        today_str = datetime.now().strftime("%Y-%m-%d")
        with get_session() as s:
            row = s.execute(
                select(EventLog.id).where(
                    and_(
                        EventLog.event_type == "intraday_stop_loss_summary_sent",
                        EventLog.created_at >= f"{today_str} 00:00:00",
                        EventLog.created_at < f"{today_str} 23:59:59",
                    )
                ).limit(1)
            ).first()
            return row is not None
    except Exception as exc:
        logger.debug("查停損彙整重發紀錄失敗（保守當未發）：%s", exc)
        return False


def run_stop_loss_summary_check() -> int:
    """
    13:15 持股停損彙整推播：掃 Portfolio.stop_loss 有設定的所有持股，
    列出當下現價 ≤ 停損的票，組成一則 Telegram。回傳是否發送（0/1）。

    與機會彙整 run_final_summary_check 解耦：
      - 兩函式分別寫 event_log 防重發
      - 各自抓 Shioaji 報價（13:15 多一次 batch 可接受，避免共享狀態複雜化）
      - 四軌全空 + 停損也空 → 兩則訊息都不發，安靜
      - 機會空 / 停損空，任一邊有命中時，仍會發對應訊息
    """
    from db.database import get_session
    from db.event_log import log_event
    from db.models import Portfolio
    from notifications.telegram_notify import send_stock_alert

    now = datetime.now()
    if not _is_summary_window(now):
        return 0
    if _already_sent_stop_loss_today():
        return 0

    with get_session() as s:
        rows = (
            s.query(Portfolio)
            .filter(Portfolio.stop_loss != None)  # noqa: E711
            .all()
        )
        holdings = [
            {
                "stock_id": r.stock_id,
                "stock_name": r.stock_name or "",
                "stop_loss": r.stop_loss,
                "cost_price": r.cost_price,
            }
            for r in rows
        ]

    if not holdings:
        logger.info("13:15 停損彙整：無設停損的持股，略過")
        return 0

    sids = sorted({str(h["stock_id"]) for h in holdings})
    snapshots = _get_shioaji_snapshots(sids)
    if not snapshots:
        log_event(
            "intraday_stop_loss_summary_skipped",
            module="intraday_final_summary",
            severity="warning",
            summary="13:15 停損彙整：Shioaji snapshots 取得失敗，本輪略過",
            payload={"checked_holdings": len(holdings)},
        )
        return 0

    hits: list[dict] = []
    for h in holdings:
        snap = snapshots.get(str(h["stock_id"]))
        if not snap:
            continue
        hit = _check_stop_loss_hit(h, snap)
        if hit:
            hits.append(hit)

    msg = _format_stop_loss_summary(hits, now.strftime("%H:%M"))
    if msg is None:
        log_event(
            "intraday_stop_loss_summary_empty",
            module="intraday_final_summary",
            severity="info",
            summary="13:15 停損彙整：無持股觸及停損",
            payload={"checked": len(holdings)},
        )
        return 0

    try:
        ok = send_stock_alert(msg)
    except Exception as exc:
        ok = False
        logger.warning("13:15 停損彙整 Telegram 推播失敗：%s", exc)

    log_event(
        "intraday_stop_loss_summary_sent" if ok else "intraday_stop_loss_summary_failed",
        module="intraday_final_summary",
        severity="info" if ok else "error",
        summary=f"13:15 停損彙整推播：共 {len(hits)} 檔觸及（{'成功' if ok else '失敗'}）",
        payload={
            "hits": [
                {
                    "stock_id": h["stock_id"],
                    "price": h["price"],
                    "stop_loss": h["stop_loss"],
                    "pnl_pct": h["pnl_pct"],
                }
                for h in hits
            ],
            "checked": len(holdings),
        },
    )
    return 1 if ok else 0
