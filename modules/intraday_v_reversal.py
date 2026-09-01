"""
盤中搶 V 型反轉 — 三段式確認（比照 intraday_n_pattern）。

  ① 盤中即時預警 run_v_reversal_check()：09:00–13:30 每分鐘，對 today_active 候選檢查
     過高確認（現價 > 止跌K高 = trigger）+ 紅K（現價>今開）+ KD 低檔金叉（用現價重算當日
     KD：K<30 且 K>D）+ 流動性 gate（vol_ma5 ≥ 5000 張）→ 推「⚡盤中預警」，標記 + 跨日去重。
  ② 13:15 統整 get_current_hits()：回傳「此刻仍成立」的命中（重驗，不成立不列），供
     intraday_final_summary 併入 13:15 訊息。不推播、不看是否已預警。
  ③ 收盤確認 run_v_reversal_close_confirm()：13:35 收盤補正後，用定案收盤重驗
     （收盤 > trigger + 紅K + KD 日線金叉）→ 推「✅收盤確認」（教科書進場點）。

政策：Shioaji 未登入則本輪安靜跳過（保護配額，不 fallback）。
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

MIN_LOTS = 5000              # 前 5 日均量下限（張）— 沿用 N字底/v5 流動性 gate
KD_LOW = 30.0               # KD 低檔門檻
BREAKOUT_BUFFER_PCT = 3.0    # 過熱濾網：現價超過觸發價 3% 就不追（假突破/追高保護）


def _to_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        num = float(value)
        return None if num != num else num
    except (TypeError, ValueError):
        return None


def _format_date(value) -> str:
    if value is None:
        return "?"
    try:
        if hasattr(value, "strftime"):
            return value.strftime("%m-%d")
        s = str(value)
        return s[5:10] if len(s) >= 10 else s
    except Exception:
        return str(value)


def _is_push_window(now: datetime) -> bool:
    """盤中預警時段：平日 09:00–13:30。"""
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (t.hour == 9 and t.minute >= 0) or (10 <= t.hour <= 12) or \
           (t.hour == 13 and t.minute <= 30)


def _get_shioaji_snapshots(stock_ids: list[str]) -> dict[str, dict]:
    if not stock_ids:
        return {}
    try:
        from broker.shioaji_adapter import get_adapter
        adapter = get_adapter()
        if not adapter.is_logged_in():
            try:
                adapter.login()
            except Exception:
                pass
        if not adapter.is_logged_in():
            logger.info("Shioaji 未登入，V 反盤中檢查本輪略過")
            return {}
        return adapter.get_snapshots(stock_ids) or {}
    except Exception as exc:
        logger.warning("Shioaji 批次報價失敗：%s", exc)
        return {}


def _kd_state(stock_id: str, *, live_price: float | None = None,
              live_high: float | None = None, live_low: float | None = None
              ) -> tuple[float | None, float | None, bool]:
    """回傳 (K, D, 低檔金叉?)。

    live_price 有值 → 盤中：用現價當「今日收盤」重算當日 KD（覆蓋/補上今日列）。
    live_price None → 收盤確認：直接用快取定案日K（今日列已由 13:35 收盤補正寫入）。
    低檔金叉 = K < KD_LOW 且 K > D。
    """
    from db.price_cache import load_prices
    from modules.indicators import kdj
    df = load_prices(stock_id, start_date=(date.today() - timedelta(days=60)).isoformat())
    if df is None or len(df) < 12:
        return None, None, False
    df = df.copy()
    if live_price is not None:
        if "date" in df.columns:
            df = df[pd.to_datetime(df["date"]).dt.date < date.today()]
        hi = max(live_high or live_price, live_price)
        lo = min(live_low or live_price, live_price)
        df = pd.concat([df, pd.DataFrame([{
            "date": pd.Timestamp(date.today()),
            "open": live_price, "max": hi, "min": lo,
            "close": live_price, "Trading_Volume": 0,
        }])], ignore_index=True)
    k, d, _ = kdj(df, 9, 3, 3)
    if k.dropna().empty:
        return None, None, False
    kk = _to_float(k.iloc[-1])
    dd = _to_float(d.iloc[-1])
    if kk is None or dd is None:
        return None, None, False
    low_golden = (kk < KD_LOW) and (kk > dd)
    return round(kk, 2), round(dd, 2), low_golden


def format_v_reversal_alert(item: dict, price: float, *, kind: str,
                            kd_k=None, kd_d=None, now_str: str | None = None,
                            quote_source: str = "Shioaji") -> str:
    """組 Telegram 訊息（kind: '盤中預警' / '收盤確認' / '13:15彙整'）。"""
    sid = str(item.get("stock_id", ""))
    name = item.get("stock_name") or ""
    ind = item.get("industry") or ""
    trigger = float(item.get("trigger_price") or 0)
    drop_pct = item.get("drop_pct")
    down_days = item.get("down_days")
    spike_ratio = item.get("spike_vol_ratio")
    spike_d = _format_date(item.get("spike_date"))
    stab_d = _format_date(item.get("stabilize_date"))
    is_doji = item.get("stabilize_is_doji")
    is_red = item.get("stabilize_is_red")
    breakout_pct = (price - trigger) / trigger * 100 if trigger > 0 else 0.0
    kd_k = kd_k if kd_k is not None else item.get("kd_k")
    kd_d = kd_d if kd_d is not None else item.get("kd_d")

    icon = {"盤中預警": "⚡ 盤中預警", "收盤確認": "✅ 收盤確認"}.get(kind, f"📋 {kind}")
    when = now_str or datetime.now().strftime("%H:%M")
    sharp = []
    if drop_pct is not None:
        sharp.append(f"從高點 {drop_pct:.1f}%")
    if item.get("sharp_by_days") and down_days is not None:
        sharp.append(f"近10日跌{down_days}天")
    stab = "十字止跌" if is_doji else ("紅K止跌" if is_red else "止跌K")

    lines = [
        f"{icon} 搶V反轉：{sid} {name}（{when}）",
        f"產業：{ind}" if ind else "",
        "─ 型態 ─",
        f"  ① 急跌：{('、'.join(sharp)) or '—'}",
        f"  ② 低檔爆量：{spike_ratio:.1f}× 前5日均量（{spike_d}）" if spike_ratio else "  ② 低檔爆量：—",
        f"  ③ {stab}（{stab_d}）觸發價={trigger:.2f}",
        f"  ④ 過高確認：現價 {price:.2f} > {trigger:.2f}（+{breakout_pct:.2f}%）",
        f"  KD：K={kd_k} D={kd_d}（低檔金叉）" if kd_k is not None else "",
        "⚠ 逆勢抄底、僅供觀察；盤中預警未收盤，留意假突破" if kind == "盤中預警" else
        ("✅ 收盤站上止跌K高，型態成立（教科書進場點）" if kind == "收盤確認" else ""),
        f"📊 報價來源：{quote_source}",
    ]
    return "\n".join(x for x in lines if x)


def _passes_intraday(it: dict, snap: dict) -> tuple[bool, dict]:
    """檢查一檔是否此刻成立（過高 + 紅K + KD低檔金叉 + 流動性 + 過熱）。回 (ok, info)。"""
    price = _to_float(snap.get("last_price"))
    if price is None:
        return False, {}
    trigger = _to_float(it.get("trigger_price"))
    if trigger is None or price <= trigger:
        return False, {}
    if price > trigger * (1 + BREAKOUT_BUFFER_PCT / 100):
        return False, {}
    open_ = _to_float(snap.get("open"))
    if open_ is not None and price <= open_:   # 需紅K
        return False, {}
    vol_ma5 = _to_float(it.get("vol_ma5"))
    if vol_ma5 is None or vol_ma5 < MIN_LOTS:
        return False, {}
    kk, dd, low_golden = _kd_state(
        str(it["stock_id"]), live_price=price,
        live_high=_to_float(snap.get("high")), live_low=_to_float(snap.get("low")),
    )
    if not low_golden:
        return False, {}
    return True, {"price": price, "trigger": trigger, "kd_k": kk, "kd_d": dd,
                  "total_volume": _to_float(snap.get("total_volume"))}


# ── ① 盤中即時預警 ────────────────────────────────────────────────────
def run_v_reversal_check() -> int:
    """盤中對 watchlist 做一次過高確認；回傳本輪推播數。"""
    from db import v_reversal_watchlist as wl
    from db.event_log import log_event
    from notifications.telegram_notify import send_stock_alert

    now = datetime.now()
    if not _is_push_window(now):
        return 0
    items = wl.today_active()
    if not items:
        return 0
    snaps = _get_shioaji_snapshots([str(it["stock_id"]) for it in items])
    if not snaps:
        return 0

    sent = 0
    now_str = now.strftime("%H:%M")
    for it in items:
        sid = str(it["stock_id"])
        snap = snaps.get(sid)
        if not snap:
            continue
        stab_date = it.get("stabilize_date")
        if wl.is_structure_alerted(sid, stab_date):
            continue
        ok, info = _passes_intraday(it, snap)
        if not ok:
            continue

        msg = format_v_reversal_alert(it, info["price"], kind="盤中預警",
                                      kd_k=info["kd_k"], kd_d=info["kd_d"], now_str=now_str)
        wl.mark_alerted(sid)   # 樂觀標記，先標記再送避免重試重複
        try:
            ok_send = send_stock_alert(msg)
        except Exception as exc:
            ok_send = False
            logger.warning("V反 Telegram 推播失敗 %s: %s", sid, exc)
        if ok_send:
            wl.record_structure_alert(sid, stab_date,
                                      trigger_price=info["trigger"], spike_date=it.get("spike_date"))
            sent += 1
        log_event(
            "v_reversal_alert" if ok_send else "v_reversal_alert_failed",
            module="intraday_v_reversal", stock_id=sid, stock_name=it.get("stock_name"),
            severity="info" if ok_send else "error",
            summary=f"{sid} 過高確認 現價 {info['price']:.2f} > 觸發 {info['trigger']:.2f}（{'成功' if ok_send else '失敗'}）",
            payload={"price": info["price"], "trigger": info["trigger"],
                     "kd_k": info["kd_k"], "kd_d": info["kd_d"]},
        )
        if ok_send:
            logger.info("V反盤中預警：%s 現價 %.2f > 觸發 %.2f", sid, info["price"], info["trigger"])
    return sent


# ── ② 13:15 統整用：回傳此刻仍成立的命中（不推播、不看已預警）──────────
def get_current_hits() -> list[dict]:
    """13:15 統整用：對今日全部候選重驗，回傳此刻仍成立者（供併入統整訊息）。"""
    from db import v_reversal_watchlist as wl
    items = wl.today_all()
    if not items:
        return []
    snaps = _get_shioaji_snapshots([str(it["stock_id"]) for it in items])
    if not snaps:
        return []
    hits = []
    for it in items:
        snap = snaps.get(str(it["stock_id"]))
        if not snap:
            continue
        ok, info = _passes_intraday(it, snap)
        if ok:
            hits.append({**it, "price": info["price"], "kd_k": info["kd_k"], "kd_d": info["kd_d"]})
    return hits


# ── ③ 收盤確認 ────────────────────────────────────────────────────────
def run_v_reversal_close_confirm() -> int:
    """13:35 收盤補正後：用定案收盤重驗（收盤>trigger + 紅K + KD日線金叉）→ 推收盤確認。"""
    from db import v_reversal_watchlist as wl
    from db.event_log import log_event
    from db.price_cache import load_prices
    from notifications.telegram_notify import send_stock_alert

    items = [it for it in wl.today_all() if not it.get("confirmed_today")]
    if not items:
        return 0
    sent = 0
    now_str = datetime.now().strftime("%H:%M")
    for it in items:
        sid = str(it["stock_id"])
        df = load_prices(sid, start_date=(date.today() - timedelta(days=10)).isoformat())
        if df is None or df.empty:
            continue
        last = df.iloc[-1]
        # 需為今日定案列
        d = last.get("date")
        if d is not None and pd.to_datetime(d).date() != date.today():
            continue
        close = _to_float(last.get("close"))
        open_ = _to_float(last.get("open"))
        trigger = _to_float(it.get("trigger_price"))
        if close is None or trigger is None or close <= trigger:
            continue
        if open_ is not None and close <= open_:   # 需紅K
            continue
        vol_ma5 = _to_float(it.get("vol_ma5"))
        if vol_ma5 is None or vol_ma5 < MIN_LOTS:
            continue
        kk, dd, low_golden = _kd_state(sid)   # 定案日K
        if not low_golden:
            continue

        msg = format_v_reversal_alert(it, close, kind="收盤確認",
                                      kd_k=kk, kd_d=dd, now_str=now_str)
        wl.mark_confirmed(sid)
        try:
            ok_send = send_stock_alert(msg)
        except Exception as exc:
            ok_send = False
            logger.warning("V反收盤確認推播失敗 %s: %s", sid, exc)
        log_event(
            "v_reversal_close_confirm" if ok_send else "v_reversal_close_confirm_failed",
            module="intraday_v_reversal", stock_id=sid, stock_name=it.get("stock_name"),
            severity="info" if ok_send else "error",
            summary=f"{sid} 收盤確認 收盤 {close:.2f} > 觸發 {trigger:.2f}（{'成功' if ok_send else '失敗'}）",
            payload={"close": close, "trigger": trigger, "kd_k": kk, "kd_d": dd},
        )
        if ok_send:
            sent += 1
            logger.info("V反收盤確認：%s 收盤 %.2f > 觸發 %.2f", sid, close, trigger)
    return sent
