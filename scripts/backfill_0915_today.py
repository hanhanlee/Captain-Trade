"""一次性：用 Shioaji 1-min kbars 重建今天 09:15 快照，補跑四軌 09:15 推播檢查。

今天 (2026-05-27) Shioaji token 整天過期，09:15 V3/V5/N 三個模組對所有 watchlist
呼叫 snapshots 全部失敗（broker_snapshot_failed × 約 200 檔/分鐘），各軌都沒有推播。

此 script：
1. 對每檔 watchlist 用 Shioaji kbars(today, today) 抓全日 1-min K
2. 截取 09:01–09:15 共 15 根 bar，合成「09:15 當下的 snapshot」
   - last_price = 第 15 根 close
   - total_volume = 1..15 根 volume 累加（張）
   - open/high/low = 對應聚合
3. 對 V3/V5/N watchlist 套用實際 09:15 推播模組的條件
   （含 vol_ma5 ≥ MIN_LOTS 流動性 gate）
4. 命中就以 [補發 HH:MM] 標頭送 Telegram + event_log

使用：python -m scripts.backfill_0915_today [--dry-run]
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, date

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


MIN_LOTS = 5000  # 與 V3/V5/N 09:15 推播模組一致（前 5 日均量下限）


def _format_kilo(vol_lots: float) -> str:
    if vol_lots >= 10000:
        return f"{vol_lots / 1000:.0f}k"
    return f"{vol_lots / 1000:.1f}k"


def _format_0915_summary(*, v3_hits, v5_main_hits, v5_late_hits, n_hits) -> str:
    total = len(v3_hits) + len(v5_main_hits) + len(v5_late_hits) + len(n_hits)
    lines = [
        f"🌅 09:15 開盤後 15 分鐘四軌彙整（共 {total} 檔符合條件）",
        "",
    ]

    def section(label, hits, fmt):
        lines.append(f"[{label}]")
        if not hits:
            lines.append("  （無）")
        else:
            for h in hits:
                lines.append("  " + fmt(h))
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

    return "\n".join(lines).rstrip()


def _reconstruct_0915_snapshot(adapter, sid: str, today: date) -> dict | None:
    """抓 today 全日 1-min kbars，截 09:01–09:15 合成 09:15 當下 snapshot。

    回傳格式對齊 ShioajiAdapter.get_snapshots() 的單檔 dict。
    """
    import pandas as pd

    today_str = today.strftime("%Y-%m-%d")
    df = adapter.get_kbars(sid, today_str, today_str)
    if df is None or df.empty:
        return None

    df = df.copy()
    df["ts_dt"] = pd.to_datetime(df["ts"])
    df = df[(df["ts_dt"].dt.hour == 9) & (df["ts_dt"].dt.minute.between(1, 15))]
    if df.empty:
        return None

    contract = adapter.get_contract(sid)
    name = getattr(contract, "name", "") if contract else ""

    return {
        "stock_id": sid,
        "stock_name": name,
        "last_price": float(df["Close"].iloc[-1]),
        "open": float(df["Open"].iloc[0]),
        "high": float(df["High"].max()),
        "low": float(df["Low"].min()),
        "total_volume": float(df["Volume"].sum()),  # 張
        "ts": "09:15:00",
    }


def main(dry_run: bool) -> int:
    from broker.shioaji_adapter import get_adapter
    from modules.intraday_final_summary import (
        _check_n_pattern,
        _check_v3,
        _check_v5_common,
    )
    from db import (
        n_pattern_watchlist as n_wl,
        v3_breakout_watchlist as v3_wl,
        v5_sniper_watchlist as v5_wl,
    )
    from db.event_log import log_event
    from notifications.telegram_notify import send_stock_alert

    adapter = get_adapter()
    if not adapter.is_logged_in():
        print("[init] Shioaji 未登入，嘗試登入…")
        if not adapter.login():
            print("[init] Shioaji 登入失敗，中止")
            return 1
    print(f"[init] Shioaji is_logged_in={adapter.is_logged_in()}")

    today = datetime.now().date()
    ref_now = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
    label = datetime.now().strftime("%H:%M")

    # ── 載入 watchlist 並套用 vol_ma5 ≥ MIN_LOTS 流動性 gate ──────────────
    def _liq_ok(it):
        v = it.get("vol_ma5")
        try:
            return float(v) >= MIN_LOTS
        except (TypeError, ValueError):
            return False

    v3_items = [it for it in v3_wl.today_all() if _liq_ok(it)]
    v5_items = [it for it in v5_wl.today_all() if _liq_ok(it)]
    n_items = [it for it in n_wl.today_all() if _liq_ok(it)]
    print(f"[wl] 流動性過濾後 V3={len(v3_items)} V5={len(v5_items)} N={len(n_items)}")

    all_sids = sorted(
        {str(it["stock_id"]) for it in v3_items}
        | {str(it["stock_id"]) for it in v5_items}
        | {str(it["stock_id"]) for it in n_items}
    )
    print(f"[wl] 待抓 kbars 共 {len(all_sids)} 檔（將呼叫 {len(all_sids)} 次 Shioaji）")

    # ── 抓 kbars 重建 09:15 snapshot ─────────────────────────────────────
    snapshots: dict[str, dict] = {}
    failed = 0
    for i, sid in enumerate(all_sids, 1):
        snap = _reconstruct_0915_snapshot(adapter, sid, today)
        if snap:
            snapshots[sid] = snap
        else:
            failed += 1
        if i % 30 == 0:
            print(f"[kbars] {i}/{len(all_sids)}（失敗 {failed}）")
    print(f"[kbars] 完成：成功 {len(snapshots)} / 總計 {len(all_sids)}（失敗 {failed}）")

    # ── 四軌條件檢查 ─────────────────────────────────────────────────────
    v3_hits, v5_main_hits, v5_late_hits, n_hits = [], [], [], []
    for it in v3_items:
        snap = snapshots.get(str(it["stock_id"]))
        if not snap:
            continue
        hit = _check_v3(it, snap, ref_now)
        if hit:
            v3_hits.append(hit)
    for it in v5_items:
        snap = snapshots.get(str(it["stock_id"]))
        if not snap:
            continue
        hit = _check_v5_common(it, snap, ref_now)
        if not hit:
            continue
        if (it.get("entry_path") or "morning") == "late":
            v5_late_hits.append(hit)
        else:
            v5_main_hits.append(hit)
    for it in n_items:
        snap = snapshots.get(str(it["stock_id"]))
        if not snap:
            continue
        hit = _check_n_pattern(it, snap, ref_now)
        if hit:
            n_hits.append(hit)

    total = len(v3_hits) + len(v5_main_hits) + len(v5_late_hits) + len(n_hits)
    print(f"[hits] V5主={len(v5_main_hits)} V5late={len(v5_late_hits)} "
          f"V3={len(v3_hits)} N={len(n_hits)} → 共 {total}")

    if total == 0:
        print("[result] 四軌均無命中，不發推播")
        return 0

    body = _format_0915_summary(
        v3_hits=v3_hits, v5_main_hits=v5_main_hits,
        v5_late_hits=v5_late_hits, n_hits=n_hits,
    )
    msg = (
        f"[補發 @{label}] 今日 09:15 因 Shioaji token 過期靜默，"
        f"以 Shioaji 1-min kbars 重建 09:15 快照補跑：\n\n{body}"
    )

    print("\n========== 09:15 補發訊息 ==========")
    print(msg)
    print("=====================================\n")

    if dry_run:
        print("[dry-run] 未送出 Telegram、未寫 event_log")
        return 0

    try:
        ok = send_stock_alert(msg)
    except Exception as exc:
        ok = False
        print(f"[push] Telegram 失敗：{exc}")
    print(f"[push] Telegram 推播：{'✓ 成功' if ok else '✗ 失敗'}")

    log_event(
        "intraday_morning_push_backfill_sent" if ok else "intraday_morning_push_backfill_failed",
        module="backfill_0915_today",
        severity="info" if ok else "error",
        summary=f"09:15 四軌補發：共 {total} 檔（{'成功' if ok else '失敗'}）",
        payload={
            "total": total,
            "v5_main": [h["stock_id"] for h in v5_main_hits],
            "v5_late": [h["stock_id"] for h in v5_late_hits],
            "v3": [h["stock_id"] for h in v3_hits],
            "n": [h["stock_id"] for h in n_hits],
            "note": "post-close backfill via kbars (Shioaji token 過期事件)",
        },
    )
    return 0 if ok else 2


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    raise SystemExit(main(dry))
