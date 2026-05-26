"""
盤中 v5 補抓軌道 (late qualifier)

對「早上 08:50 沒進 watchlist 但盤中強勢」的科技股，盤中即時補做 V5 完整重判。

推播時點限定 09:15 一次 + 13:00–13:24 每分鐘（與三軌主推播同步），對候選池：
  1. 時間 gate：僅在 09:15 或 13:00–13:24 才啟動，其餘時段 return 0
  2. 載入截至昨日 EOD 的日 K（含指標，當日模組級快取）
  3. Shioaji 批次抓現價、累計量、open/high/low
  4. 把現價/估全日量當「今日 K 棒」patch 進 df 尾巴
  5. 重算指標 → evaluate_v5() 完整重判（共同 gate + 型態 A/B）
  6. 全條件通過 + 現價 ≤ 突破目標 ×(1+buffer) → 推 Telegram + upsert watchlist (entry_path="late")

與 intraday_v5_breakout.py 並行：
  - 主 checker：盯 watchlist 裡的 morning 票
  - late qualifier：補抓 watchlist 外、盤中才成形的票
  - 命中後寫進同一張 watchlist 但 entry_path="late"，避免重複推播

設計選擇：
  - 候選池：與 v5_sniper_watchlist_builder 同 TECH_INDUSTRIES（~300 檔）
  - 推播時點來自 4-5 月回測：09:15 抓早盤強勢突破、13:00–13:24 抓持續性確認
  - 中間時段不推播 → 避免追到衝高拉回的中段
  - 過熱濾網：現價 ≤ 突破目標 × 1.03（剛過前高才追，不抓拉開段的）
  - vol_breakout 比對 EOD 的「前 5 日均量」（與既有 V5 一致）
  - 預估全日量：(累計量 ÷ 已過分鐘) × 270，patch 進今日 K 棒當 Trading_Volume
"""
from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd

logger = logging.getLogger(__name__)

# 候選池與既有 v5_sniper_watchlist_builder 一致
TECH_INDUSTRIES: tuple[str, ...] = (
    "半導體業",
    "電子零組件業",
    "電腦及週邊設備業",
    "光電業",
    "通信網路業",
    "電子通路業",
    "資訊服務業",
    "其他電子業",
    "電子工業",
)

BREAKOUT_BUFFER_PCT = 3.0    # 過熱濾網：現價超過突破目標 3% 就不追（剛過前高才有意義）
MIN_LOTS = 5000              # 前 5 日均量下限（張）— 看「平常流動性」，2026-05-26 從「預估全日量」改基準
_TOTAL_TRADING_MINUTES = 270 # 09:00–13:30
_MARKET_OPEN_H, _MARKET_OPEN_M = 9, 0
_PATTERN_A_SQUEEZE_PCT = 4.0 # 科技股放寬到 4%（與 v5_sniper_watchlist_builder 同步）

# 模組級快取，避免每 5 分鐘重複載入
_EOD_DF_CACHE: dict[tuple[str, date], pd.DataFrame | None] = {}
_CANDIDATE_POOL_CACHE: dict[date, list[dict]] = {}


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


def _is_push_window(now: datetime) -> bool:
    """推播時點：09:15 一次 + 13:00–13:24 每分鐘（共 26 次/天）。與三軌主推播同步（見模組 docstring）。"""
    t = now.time()
    if t.hour == 9 and t.minute == 15:
        return True
    if t.hour == 13 and 0 <= t.minute <= 24:
        return True
    return False


def _estimate_full_day_volume(current_volume_lots: float, now: datetime) -> tuple[float, float]:
    """current_volume_lots: 累計成交「張」；回傳 (estimated_full_day_lots, scale)。"""
    market_open = now.replace(hour=_MARKET_OPEN_H, minute=_MARKET_OPEN_M,
                              second=0, microsecond=0)
    elapsed = max((now - market_open).total_seconds() / 60, 5)
    elapsed = min(elapsed, _TOTAL_TRADING_MINUTES)
    factor = _TOTAL_TRADING_MINUTES / elapsed
    return current_volume_lots * factor, round(factor, 1)


def _get_candidate_pool(today: date) -> list[dict]:
    """取得科技股候選池，per-day 快取。"""
    cached = _CANDIDATE_POOL_CACHE.get(today)
    if cached is not None:
        return cached
    try:
        from data.finmind_client import get_stock_list
        info_df = get_stock_list()
        if info_df is None or info_df.empty:
            _CANDIDATE_POOL_CACHE[today] = []
            return []
        info_df = info_df[info_df["industry_category"].isin(TECH_INDUSTRIES)].copy()
        pool = [
            {
                "stock_id": str(r["stock_id"]),
                "stock_name": r.get("stock_name"),
                "industry": r.get("industry_category"),
            }
            for _, r in info_df.iterrows()
        ]
        _CANDIDATE_POOL_CACHE[today] = pool
        logger.info("v5 late qualifier 候選池建立：%d 檔", len(pool))
        return pool
    except Exception as exc:
        logger.warning("v5 late qualifier 取得候選池失敗：%s", exc)
        _CANDIDATE_POOL_CACHE[today] = []
        return []


def _get_eod_df(stock_id: str, today: date) -> pd.DataFrame | None:
    """取單檔截至昨日 EOD 的日 K（未跑指標，patch 完今日才一起算）。"""
    key = (stock_id, today)
    if key in _EOD_DF_CACHE:
        return _EOD_DF_CACHE[key]
    try:
        from db.price_cache import load_prices
        # 120 天足夠（V5 需要 MA60 + n_pattern 80 根 lookback）
        df = load_prices(stock_id, lookback_days=120)
        if df is None or df.empty or len(df) < 60:
            _EOD_DF_CACHE[key] = None
            return None
        # 截掉今日（保險：若 EOD prefetch 已寫進今日資料，會干擾 patch）
        df = df[df["date"].dt.date < today].copy()
        if len(df) < 60:
            _EOD_DF_CACHE[key] = None
            return None
        _EOD_DF_CACHE[key] = df
        return df
    except Exception as exc:
        logger.debug("late qualifier 載入 %s EOD 失敗：%s", stock_id, exc)
        _EOD_DF_CACHE[key] = None
        return None


def _patch_intraday_bar(
    df_eod: pd.DataFrame,
    *,
    today: date,
    snap: dict,
    estimated_full_day_lots: float,
) -> pd.DataFrame | None:
    """把盤中即時資料 patch 成「今日 K 棒」附加到 EOD df 尾巴。"""
    last_price = _to_float(snap.get("last_price"))
    open_p = _to_float(snap.get("open"))
    high_p = _to_float(snap.get("high"))
    low_p = _to_float(snap.get("low"))
    if last_price is None or open_p is None or high_p is None or low_p is None:
        return None
    if last_price <= 0 or open_p <= 0:
        return None

    # Trading_Volume 單位「股」（=「張」×1000），對齊 FinMind EOD 資料
    today_volume_shares = estimated_full_day_lots * 1000.0

    today_row = pd.DataFrame([{
        "date": pd.Timestamp(today),
        "open": open_p,
        "max": high_p,
        "min": low_p,
        "close": last_price,
        "Trading_Volume": today_volume_shares,
    }])
    return pd.concat([df_eod, today_row], ignore_index=True)


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
            logger.info("Shioaji 未登入，v5 late qualifier 本輪略過")
            return {}
        return adapter.get_snapshots(stock_ids) or {}
    except Exception as exc:
        logger.warning("Shioaji 批次報價失敗（late qualifier）：%s", exc)
        return {}


def _format_late_alert(
    cand: dict,
    *,
    pattern_type: str,
    price: float,
    estimated_volume_lots: float,
    vol_ma5_lots: float,
    gain_pct: float,
    breakout_target: float,
    prev_close: float,
    prev_high: float,
    ma5: float, ma10: float, ma20: float, ma60: float,
    ma_spread_pct: float | None,
    now_str: str,
) -> str:
    sid = cand.get("stock_id", "")
    name = cand.get("stock_name") or ""
    ind = cand.get("industry") or ""

    pat_label = {
        "A": "型態 A：糾結突破（破近 10 日新高）",
        "B": "型態 B：回檔再上（破昨高）",
    }.get(pattern_type, f"型態 {pattern_type}")

    vol_ratio = estimated_volume_lots / vol_ma5_lots if vol_ma5_lots > 0 else 0.0
    breakout_pct = (price - breakout_target) / breakout_target * 100 if breakout_target > 0 else 0.0

    lines = [
        f"🎯 v5 補抓突破：{sid} {name}（{now_str}）",
        "⚡ 晨間未進 watchlist，盤中重判全條件通過",
        f"產業：{ind}" if ind else "",
        pat_label,
        "─ 結構 ─",
        f"  昨收 {prev_close:.2f} / 昨高 {prev_high:.2f}",
        f"  MA5={ma5:.2f} / MA10={ma10:.2f} / MA20={ma20:.2f} / MA60={ma60:.2f}",
        f"  均線糾結度：{ma_spread_pct:.2f}%" if ma_spread_pct is not None else "",
        "─ 量能（預估全日 vs 5 日均量）─",
        f"  推估全日 {estimated_volume_lots:.0f} 張 = {vol_ratio:.2f}× 5日均量 {vol_ma5_lots:.0f} 張",
        "─ 現價 ─",
        f"  {price:.2f}（突破目標 {breakout_target:.2f} +{breakout_pct:.2f}% / buffer {BREAKOUT_BUFFER_PCT:.1f}%）",
        f"  今日漲幅 {gain_pct:+.2f}%",
        "⚠ 補抓軌道：訊號質地略弱（昨日 EOD 未過 V5 靜態 gate），追入請斟酌",
        "📊 報價來源：Shioaji",
    ]
    return "\n".join(line for line in lines if line)


def run_v5_late_qualifier_check() -> int:
    """
    盤中對科技股池補抓 V5 完整突破。
    回傳本輪推播的 Telegram 訊息數。

    呼叫場景：scheduler 每分鐘觸發，函式內限定 09:15 + 13:00–13:24 才執行。
    """
    from db import v5_sniper_watchlist as wl
    from db.event_log import log_event
    from modules.scanner import compute_indicators
    from modules.v5_sniper import V5Params, evaluate_v5
    from notifications.telegram_notify import send_stock_alert

    now = datetime.now()
    if not _is_push_window(now):
        return 0

    today = now.date()
    now_str = now.strftime("%H:%M")

    pool = _get_candidate_pool(today)
    if not pool:
        return 0

    # 排除已在今日 watchlist 的（主 checker 已監控）
    existing = {it["stock_id"] for it in wl.today_all()}
    to_scan = [c for c in pool if c["stock_id"] not in existing]
    if not to_scan:
        return 0

    sids = [c["stock_id"] for c in to_scan]
    snapshots = _get_shioaji_snapshots(sids)
    if not snapshots:
        return 0

    # 把 body_ratio_min / max_gain_pct 解禁，讓 evaluate_v5 跑得到 pattern detection；
    # late_qualifier 自己用 _late_gate_passed + gain_pct 手動把關（與主 checker 一致：
    # 盤中 K 棒下影線雜訊大，body/range 不可信，這層留到收盤再驗）
    # min_lots 用 V5Params 預設（1000）即可——本軌新的流動性 gate 是「前 5 日均量 ≥ MIN_LOTS」，
    # 在迴圈內手動把關，evaluate_v5 內部的 vol_floor_ok（檢預估全日量）作為弱保底。
    params = V5Params(
        body_ratio_min=0.0,
        max_gain_pct=None,
        pattern_a_squeeze_pct=_PATTERN_A_SQUEEZE_PCT,
    )
    sent = 0
    cand_by_sid = {c["stock_id"]: c for c in to_scan}

    for sid, snap in snapshots.items():
        cand = cand_by_sid.get(sid)
        if cand is None:
            continue

        price = _to_float(snap.get("last_price"))
        total_volume_lots = _to_float(snap.get("total_volume"))
        if price is None or total_volume_lots is None or total_volume_lots <= 0:
            continue

        df_eod = _get_eod_df(sid, today)
        if df_eod is None:
            continue

        # 流動性 gate：前 5 日均量 ≥ MIN_LOTS（過濾平常無量的小型股；不受 9:15 外推誤差影響）
        try:
            prev_5_vol = pd.to_numeric(df_eod["Trading_Volume"].iloc[-5:],
                                        errors="coerce").dropna()
            vol_ma5_lots = float(prev_5_vol.mean()) / 1000.0 if not prev_5_vol.empty else 0.0
        except Exception:
            vol_ma5_lots = 0.0
        if vol_ma5_lots < MIN_LOTS:
            continue

        est_full_day_lots, scale = _estimate_full_day_volume(total_volume_lots, now)

        df_patched = _patch_intraday_bar(
            df_eod, today=today, snap=snap,
            estimated_full_day_lots=est_full_day_lots,
        )
        if df_patched is None or df_patched.empty:
            continue

        try:
            df_with_ind = compute_indicators(df_patched)
            result = evaluate_v5(df_with_ind, today=today, params=params)
        except Exception as exc:
            logger.debug("late qualifier %s evaluate_v5 失敗：%s", sid, exc)
            continue

        # 盤中重判規則：跳過 body_strong（與主 checker 一致），手動套漲幅 cap
        late_gate_passed = (
            result.ma_full_bull
            and result.red_candle
            and result.close_gt_ma5
            and result.high_gt_prev
            and result.vol_breakout
            and result.vol_floor_ok
            and result.kd_bull
            and result.macd_bull_zero_above
            and result.macd_osc_growing
        )
        if not late_gate_passed:
            continue
        if result.pattern_type not in ("A", "B"):
            continue

        # 蒐集突破目標 + watchlist 寫入用資料（vol_ma5_lots 已在流動性 gate 前算過）
        try:
            prev_row = df_eod.iloc[-1]
            prev_close = float(prev_row["close"])
            prev_high = float(prev_row["max"])
            prev_volume_lots = float(prev_row["Trading_Volume"]) / 1000.0

            # breakout_target：型態 A=近10日高（不含今日）；型態 B=昨日高
            if result.pattern_type == "A":
                breakout_target = float(df_eod["max"].iloc[-10:].max())
            else:
                breakout_target = prev_high

            last_ind = df_with_ind.iloc[-1]
            ma5 = float(last_ind["ma5"])
            ma10 = float(last_ind["ma10"])
            ma20 = float(last_ind["ma20"])
            ma60 = float(last_ind["ma60"])
            ma_vals = [ma5, ma10, ma20]
            ma_spread_pct = (
                (max(ma_vals) - min(ma_vals)) / ma20 * 100 if ma20 > 0 else None
            )
        except Exception as exc:
            logger.warning("late qualifier %s 蒐集 watchlist 欄位失敗：%s", sid, exc)
            continue

        # 過熱濾網：現價超過突破目標 BREAKOUT_BUFFER_PCT% 就不追
        if breakout_target <= 0:
            continue
        if price > breakout_target * (1 + BREAKOUT_BUFFER_PCT / 100):
            continue

        gain_pct = (price - prev_close) / prev_close * 100 if prev_close > 0 else 0.0

        msg = _format_late_alert(
            cand,
            pattern_type=result.pattern_type or "A",
            price=price,
            estimated_volume_lots=est_full_day_lots,
            vol_ma5_lots=vol_ma5_lots,
            gain_pct=gain_pct,
            breakout_target=breakout_target,
            prev_close=prev_close,
            prev_high=prev_high,
            ma5=ma5, ma10=ma10, ma20=ma20, ma60=ma60,
            ma_spread_pct=ma_spread_pct,
            now_str=now_str,
        )

        # 樂觀寫 DB：先寫入 + mark_alerted，避免下一輪重判重複推播
        try:
            wl.upsert_candidates([{
                "stock_id": sid,
                "stock_name": cand.get("stock_name"),
                "industry": cand.get("industry"),
                "pattern_type": result.pattern_type or "A",
                "breakout_target": round(breakout_target, 2),
                "prev_high": round(prev_high, 2),
                "prev_close": round(prev_close, 2),
                "prev_volume": round(prev_volume_lots, 0),
                "vol_ma5": round(vol_ma5_lots, 0),
                "ma5": round(ma5, 2),
                "ma10": round(ma10, 2),
                "ma20": round(ma20, 2),
                "ma60": round(ma60, 2),
                "ma_spread_pct": round(ma_spread_pct, 2) if ma_spread_pct is not None else None,
                "max_gain_pct": None,  # late 軌道改用 breakout buffer 取代 gain cap
                "entry_path": "late",
            }], scan_date=today)
            wl.mark_alerted(sid)
        except Exception as exc:
            logger.warning("late qualifier %s upsert/mark 失敗：%s", sid, exc)

        try:
            ok = send_stock_alert(msg)
        except Exception as exc:
            ok = False
            logger.warning("Telegram 推播失敗 %s: %s", sid, exc)

        log_event(
            "v5_late_qualifier_alert" if ok else "v5_late_qualifier_alert_failed",
            module="intraday_v5_late_qualifier",
            stock_id=sid,
            stock_name=cand.get("stock_name"),
            severity="info" if ok else "error",
            summary=(
                f"{sid} {cand.get('stock_name', '')} v5 補抓 {result.pattern_type} 型"
                f"（現價 {price:.2f}，目標 {breakout_target:.2f}，"
                f"漲幅 {gain_pct:+.2f}%，推播 {'成功' if ok else '失敗'}）"
            ),
            payload={
                "pattern_type": result.pattern_type,
                "price": price,
                "breakout_target": breakout_target,
                "breakout_pct": (price - breakout_target) / breakout_target * 100 if breakout_target > 0 else 0.0,
                "breakout_buffer_pct": BREAKOUT_BUFFER_PCT,
                "prev_close": prev_close,
                "prev_high": prev_high,
                "gain_pct": gain_pct,
                "estimated_volume_lots": est_full_day_lots,
                "vol_ma5_lots": vol_ma5_lots,
                "vol_scale_factor": scale,
                "entry_path": "late",
            },
        )

        if ok:
            sent += 1
            logger.info(
                "v5 補抓突破推播：%s %s 型 現價 %.2f >= 目標 %.2f",
                sid, result.pattern_type, price, breakout_target,
            )

    return sent
