"""
盤中 v5 狙擊手版突破即時偵測

推播時點限定 09:15 一次 + 13:00–13:24 每分鐘掃描，對 db.v5_sniper_watchlist.today_active()
中尚未推播的候選做下列檢查：
  1. 時間 gate：僅在 09:15 或 13:00–13:24 才啟動，其餘時段 return 0
  2. 用 Shioaji 批次取即時 last_price 與今日累積成交量
  3. 突破：price >= breakout_target
       - 型態 A：breakout_target = 近 10 日最高
       - 型態 B：breakout_target = 昨日最高（破昨高即點火）
  4. 過熱濾網：現價 ≤ breakout_target × 1.03（過前高 3% 內才追，剛突破才有意義）
  5. 量增：預估全日量 >= 前 5 日均量 × 1.3
  6. 量底：預估全日量 >= 1000 張
  7. （選用）漲幅 cap：若 row 上有 max_gain_pct，當日漲幅須 ≤ 該值
  8. 型態 B 候選：盤中重驗守月線（用現價當今日 close、重算今日 MA20）
       若型態失效（例：今日跌破月線）則跳過推播
       底底高沿用 EOD 結果（盤中 low 雜訊大）
  8. 全部通過 → 推 Telegram，標記 alerted_today 防重複

設計選擇：
  - 推播時點來自 4-5 月回測：09:15 抓早盤強勢突破、13:00–13:24 抓持續性確認
  - 中間時段不推播 → 避免追到衝高拉回的中段
  - 不檢查「實體紅K 60%」與「站上 MA5」：盤中跳動雜訊大，留到收盤再說
  - 量倍對前 5 日均量（與 v3 一致）
  - 型態 B 守月線重驗：對「08:50 入名單，但今日早盤跌破月線」的候選做安全網
  - EOD df + 指標當日只算一次，模組級快取在 _EOD_DF_CACHE
  - 若 Shioaji 未登入，本輪安靜跳過
"""
from __future__ import annotations

import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)

VOLUME_MULT = 1.3            # 預估全日量 ÷ 前 5 日均量 倍數門檻
MIN_LOTS = 5000              # 預估全日量最低張數（2026-05-25 從 1000 提高，砍掉低流動性雜訊）
BREAKOUT_BUFFER_PCT = 3.0    # 過熱濾網：現價超過突破目標 3% 就不追（與 late qualifier 同邏輯）
_MARKET_OPEN_H, _MARKET_OPEN_M = 9, 0
_TOTAL_TRADING_MINUTES = 270   # 09:00–13:30


def _is_push_window(now: datetime) -> bool:
    """推播時點：09:15 一次 + 13:00–13:24 每分鐘（共 26 次/天）。回測支持此設計（見模組 docstring）。"""
    t = now.time()
    if t.hour == 9 and t.minute == 15:
        return True
    if t.hour == 13 and 0 <= t.minute <= 24:
        return True
    return False

# 模組級 EOD df 快取：key = (stock_id, today)，避免每分鐘對每檔重拉 + 重算指標
_EOD_DF_CACHE: dict[tuple[str, date], object] = {}


def _get_eod_df_with_indicators(stock_id: str, today: date):
    """取單檔截至昨日 EOD 的日 K（含指標），當日重用一次計算結果。"""
    key = (stock_id, today)
    cached = _EOD_DF_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        from db.price_cache import load_prices
        from modules.scanner import compute_indicators
        # 50 天足夠（MA20 + 守月線 5 日 + 底底高 20 日 + 緩衝）
        df = load_prices(stock_id, lookback_days=50)
        if df is None or df.empty:
            _EOD_DF_CACHE[key] = None
            return None
        df_ind = compute_indicators(df)
        _EOD_DF_CACHE[key] = df_ind
        return df_ind
    except Exception as exc:
        logger.warning("載入/算指標失敗 %s: %s", stock_id, exc)
        _EOD_DF_CACHE[key] = None
        return None


def _verify_pattern_b_still_valid(stock_id: str, current_price: float, today: date):
    """盤中重驗型態 B 是否仍成立。回傳 (hit, reason)。"""
    from modules.v5_sniper import verify_pattern_b_intraday

    df = _get_eod_df_with_indicators(stock_id, today)
    if df is None:
        return False, "無 EOD 資料"
    chk = verify_pattern_b_intraday(df, current_price=current_price)
    return chk.hit, chk.reason


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
    """
    依當前時刻換算「預估全日成交量」。
    回傳 (estimated_volume, scale_factor)；elapsed 最少 5 分鐘避免開盤頭幾分鐘失真。
    """
    market_open = now.replace(hour=_MARKET_OPEN_H, minute=_MARKET_OPEN_M,
                              second=0, microsecond=0)
    elapsed = max((now - market_open).total_seconds() / 60, 5)
    elapsed = min(elapsed, _TOTAL_TRADING_MINUTES)
    factor = _TOTAL_TRADING_MINUTES / elapsed
    return current_volume * factor, round(factor, 1)


def format_v5_alert(
    item: dict,
    *,
    price: float,
    total_volume: float | None,
    estimated_volume: float | None,
    scale_factor: float | None,
    gain_pct: float | None,
    now_str: str | None = None,
    quote_source: str = "Shioaji",
) -> str:
    sid = str(item.get("stock_id", ""))
    name = item.get("stock_name") or ""
    ind = item.get("industry") or ""
    pat = item.get("pattern_type") or "?"
    target = float(item.get("breakout_target") or 0)
    prev_high = float(item.get("prev_high") or 0)
    prev_close = float(item.get("prev_close") or 0)
    avg5_vol = float(item.get("vol_ma5") or 0)
    ma5 = float(item.get("ma5") or 0)
    ma10 = float(item.get("ma10") or 0)
    ma20 = float(item.get("ma20") or 0)
    ma60 = float(item.get("ma60") or 0)
    spread = item.get("ma_spread_pct")

    when = now_str or datetime.now().strftime("%H:%M")
    label = f"{sid} {name}".strip()

    pat_label = {
        "A": "型態 A：糾結突破（破近 10 日新高）",
        "B": "型態 B：N 字底回檔再上（破昨高）",
    }.get(pat, f"型態 {pat}")

    if estimated_volume is not None and avg5_vol > 0:
        vol_ratio = estimated_volume / avg5_vol
        vol_line = (
            f"  累積 {total_volume:.0f} 張"
            f"（×{scale_factor} 推估全日 {estimated_volume:.0f} 張"
            f" = {vol_ratio:.2f}× 5日均量 {avg5_vol:.0f} 張）"
        )
    else:
        vol_line = "  量能未知"

    breakout_pct = (price - target) / target * 100 if target > 0 else 0.0

    lines = [
        f"🎯 v5 狙擊手版突破：{label}（{when}）",
        f"產業：{ind}" if ind else "",
        f"{pat_label}",
        "─ 結構 ─",
        f"  昨收 {prev_close:.2f} / 昨高 {prev_high:.2f}",
        f"  MA5={ma5:.2f} / MA10={ma10:.2f} / MA20={ma20:.2f} / MA60={ma60:.2f}",
        f"  均線糾結度：{spread:.2f}%" if spread is not None else "",
        "─ 量能（預估全日 vs 5 日均量）─",
        vol_line,
        "─ 現價 ─",
        f"  {price:.2f}（突破目標 {target:.2f} +{breakout_pct:.2f}% / buffer {BREAKOUT_BUFFER_PCT:.1f}%）",
    ]
    if gain_pct is not None:
        lines.append(f"  今日漲幅 {gain_pct:+.2f}%")
    lines += [
        "⚠ 留意假突破：突破後若快速跌回目標下方，可能為誘多",
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
            logger.info("Shioaji 未登入，v5 盤中檢查本輪略過")
            return {}
        return adapter.get_snapshots(stock_ids) or {}
    except Exception as exc:
        logger.warning("Shioaji 批次報價失敗：%s", exc)
        return {}


def run_v5_breakout_check() -> int:
    """
    盤中對 v5 watchlist 做一次突破檢查。
    回傳本輪推播的 Telegram 訊息數。
    """
    from db import v5_sniper_watchlist as wl
    from db.event_log import log_event
    from notifications.telegram_notify import send_stock_alert

    if not _is_push_window(datetime.now()):
        return 0

    items = wl.today_active()
    if not items:
        return 0

    sids = [str(it["stock_id"]) for it in items]
    snapshots = _get_shioaji_snapshots(sids)
    if not snapshots:
        return 0

    sent = 0
    now = datetime.now()
    now_str = now.strftime("%H:%M")
    today = now.date()

    for it in items:
        sid = str(it["stock_id"])
        snap = snapshots.get(sid)
        if not snap:
            continue

        price = _to_float(snap.get("last_price"))
        if price is None:
            continue

        target = _to_float(it.get("breakout_target"))
        avg5_vol = _to_float(it.get("vol_ma5"))
        if target is None or avg5_vol is None or avg5_vol <= 0:
            continue

        # 1) 突破目標價（含等於）
        if price < target:
            continue

        # 2) 過熱濾網：現價超過突破目標 BREAKOUT_BUFFER_PCT% 就不追
        if price > target * (1 + BREAKOUT_BUFFER_PCT / 100):
            continue

        # 3) 量增：預估全日量 >= 前 5 日均量 × VOLUME_MULT
        total_volume = _to_float(snap.get("total_volume"))
        if total_volume is None:
            continue
        # Shioaji total_volume 單位通常為「張」，與 watchlist 內 vol_ma5 同單位
        estimated_vol, scale = _estimate_full_day_volume(total_volume, now)
        if estimated_vol < avg5_vol * VOLUME_MULT:
            continue

        # 4) 量底：預估全日量 >= MIN_LOTS
        if estimated_vol < MIN_LOTS:
            continue

        # 5) 漲幅 cap（選用，row 上有設才檢查）
        max_gain = _to_float(it.get("max_gain_pct"))
        prev_close = _to_float(it.get("prev_close"))
        gain_pct = None
        if prev_close and prev_close > 0:
            gain_pct = (price - prev_close) / prev_close * 100
            if max_gain is not None and gain_pct > max_gain:
                continue

        # 6) 型態 B 盤中重驗：守月線是否仍成立（底底高沿用 EOD）
        if it.get("pattern_type") == "B":
            hit, reason = _verify_pattern_b_still_valid(sid, price, today)
            if not hit:
                logger.info(
                    "v5 B 型 %s 盤中型態失效，跳過推播：%s（現價 %.2f）",
                    sid, reason, price,
                )
                log_event(
                    "v5_sniper_pattern_b_invalidated",
                    module="intraday_v5_breakout",
                    stock_id=sid,
                    stock_name=it.get("stock_name"),
                    severity="info",
                    summary=(
                        f"{sid} {it.get('stock_name', '')} 型態 B 盤中失效"
                        f"（{reason}，現價 {price:.2f}）"
                    ),
                    payload={"price": price, "reason": reason},
                )
                continue

        msg = format_v5_alert(
            it,
            price=price,
            total_volume=total_volume,
            estimated_volume=estimated_vol,
            scale_factor=scale,
            gain_pct=gain_pct,
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
            "v5_sniper_alert" if ok else "v5_sniper_alert_failed",
            module="intraday_v5_breakout",
            stock_id=sid,
            stock_name=it.get("stock_name"),
            severity="info" if ok else "error",
            summary=(
                f"{sid} {it.get('stock_name', '')} v5 {it.get('pattern_type')} 型突破"
                f"（目標 {target:.2f}，現價 {price:.2f}）"
                f"，預估量 {estimated_vol:.0f} 張"
                f"（推播 {'成功' if ok else '失敗'}）"
            ),
            payload={
                "pattern_type": it.get("pattern_type"),
                "price": price,
                "breakout_target": target,
                "breakout_pct": (price - target) / target * 100 if target > 0 else 0.0,
                "breakout_buffer_pct": BREAKOUT_BUFFER_PCT,
                "prev_high": it.get("prev_high"),
                "prev_close": prev_close,
                "vol_ma5": avg5_vol,
                "total_volume": total_volume,
                "estimated_volume": estimated_vol,
                "vol_scale_factor": scale,
                "gain_pct": gain_pct,
                "max_gain_pct": max_gain,
            },
        )

        if ok:
            sent += 1
            logger.info(
                "v5 狙擊手版突破推播：%s %s 型 現價 %.2f >= 目標 %.2f",
                sid, it.get("pattern_type"), price, target,
            )

    return sent
