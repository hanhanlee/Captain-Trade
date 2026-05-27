"""一次性：用今天收盤後的 Shioaji 資料，補跑 13:15 兩則彙整推播。

今天 (2026-05-27) Shioaji token 整天過期，13:15 的 run_final_summary_check 與
run_stop_loss_summary_check 都在 `if not snapshots: return 0` silent return。
此 script 重用 modules/intraday_final_summary 的內部 helpers，把參考時間設為 13:30
(讓 _estimate_full_day_volume 的 factor=1，等同直接用全日量) 跑一次，
有命中就以 [補發 HH:MM] 標頭送 Telegram 並寫 event_log。

使用：python -m scripts.backfill_1315_today [--dry-run]
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

# 加 repo root 到 sys.path，讓 `python scripts/xxx.py` 也能 import 上層模組
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def main(dry_run: bool) -> int:
    from broker.shioaji_adapter import get_adapter
    from modules.intraday_final_summary import (
        _check_n_pattern,
        _check_stop_loss_hit,
        _check_v3,
        _check_v5_common,
        _format_stop_loss_summary,
        _format_summary,
    )
    from db import (
        n_pattern_watchlist as n_wl,
        v3_breakout_watchlist as v3_wl,
        v5_sniper_watchlist as v5_wl,
    )
    from db.database import get_session
    from db.event_log import log_event
    from db.models import Portfolio
    from notifications.telegram_notify import send_stock_alert

    adapter = get_adapter()
    if not adapter.is_logged_in():
        print("[init] Shioaji 未登入，嘗試登入…")
        ok = adapter.login()
        if not ok:
            print("[init] Shioaji 登入失敗，中止")
            return 1
    print(f"[init] Shioaji is_logged_in={adapter.is_logged_in()}")

    # 用 13:30（收盤）當參考時點：_estimate_full_day_volume 內部會把 elapsed 截到 270min，
    # factor=1.0 → 取目前 total_volume 當全日量（收盤後 snapshot 的 total_volume 即為全日量）
    ref_now = datetime.now().replace(hour=13, minute=30, second=0, microsecond=0)
    label = datetime.now().strftime("%H:%M")

    # ─── 機會彙整 ────────────────────────────────────────────────────────
    v3_items = list(v3_wl.today_all())
    v5_items = list(v5_wl.today_all())
    n_items = list(n_wl.today_all())
    all_sids = sorted(
        {str(it["stock_id"]) for it in v3_items}
        | {str(it["stock_id"]) for it in v5_items}
        | {str(it["stock_id"]) for it in n_items}
    )
    print(f"[opp] watchlist V3={len(v3_items)} V5={len(v5_items)} N={len(n_items)} "
          f"distinct={len(all_sids)}")

    snapshots = adapter.get_snapshots(all_sids) if all_sids else {}
    print(f"[opp] Shioaji snapshots 取得 {len(snapshots)} 檔")

    v3_hits, v5_main_hits, v5_late_hits, n_hits = [], [], [], []
    for it in v3_items:
        snap = snapshots.get(str(it["stock_id"]))
        if snap:
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
        if snap:
            hit = _check_n_pattern(it, snap, ref_now)
            if hit:
                n_hits.append(hit)

    total = len(v3_hits) + len(v5_main_hits) + len(v5_late_hits) + len(n_hits)
    print(f"[opp] hits V5主={len(v5_main_hits)} V5late={len(v5_late_hits)} "
          f"V3={len(v3_hits)} N={len(n_hits)} → 共 {total}")

    if total > 0:
        body = _format_summary(
            v3_hits=v3_hits, v5_main_hits=v5_main_hits,
            v5_late_hits=v5_late_hits, n_hits=n_hits,
            now_str="13:15",
        )
        opp_msg = (
            f"[補發 @{label}] 今日 13:15 因 Shioaji token 過期靜默，"
            f"以收盤後 Shioaji 資料補跑：\n\n{body}"
        )
        print("\n========== 機會彙整訊息 ==========")
        print(opp_msg)
        print("===================================\n")

        if not dry_run:
            try:
                ok = send_stock_alert(opp_msg)
            except Exception as exc:
                ok = False
                print(f"[opp] Telegram 失敗：{exc}")
            print(f"[opp] Telegram 推播：{'✓ 成功' if ok else '✗ 失敗'}")
            log_event(
                "intraday_final_summary_backfill_sent" if ok else "intraday_final_summary_backfill_failed",
                module="intraday_final_summary",
                severity="info" if ok else "error",
                summary=f"13:15 機會彙整補發：共 {total} 檔（{'成功' if ok else '失敗'}）",
                payload={
                    "total": total,
                    "v5_main": [h["stock_id"] for h in v5_main_hits],
                    "v5_late": [h["stock_id"] for h in v5_late_hits],
                    "v3": [h["stock_id"] for h in v3_hits],
                    "n": [h["stock_id"] for h in n_hits],
                    "note": "post-close backfill (Shioaji token 過期事件)",
                },
            )
    else:
        print("[opp] 四軌均無命中，不發機會彙整")

    # ─── 停損彙整 ────────────────────────────────────────────────────────
    with get_session() as s:
        rows = s.query(Portfolio).filter(Portfolio.stop_loss != None).all()  # noqa: E711
        holdings = [
            {
                "stock_id": r.stock_id,
                "stock_name": r.stock_name or "",
                "stop_loss": r.stop_loss,
                "cost_price": r.cost_price,
            }
            for r in rows
        ]
    print(f"[sl] 持股有設停損 = {len(holdings)} 檔")

    hold_sids = sorted({str(h["stock_id"]) for h in holdings})
    hold_snaps = adapter.get_snapshots(hold_sids) if hold_sids else {}
    print(f"[sl] 持股 snapshot 取得 {len(hold_snaps)} 檔")

    sl_hits = []
    for h in holdings:
        snap = hold_snaps.get(str(h["stock_id"]))
        if not snap:
            continue
        hit = _check_stop_loss_hit(h, snap)
        if hit:
            sl_hits.append(hit)
    print(f"[sl] 觸及停損 = {len(sl_hits)} 檔")

    if sl_hits:
        body = _format_stop_loss_summary(sl_hits, "13:15")
        sl_msg = (
            f"[補發 @{label}] 今日 13:15 因 Shioaji token 過期靜默，"
            f"以收盤後 Shioaji 資料補跑：\n\n{body}"
        )
        print("\n========== 停損彙整訊息 ==========")
        print(sl_msg)
        print("===================================\n")

        if not dry_run:
            try:
                ok = send_stock_alert(sl_msg)
            except Exception as exc:
                ok = False
                print(f"[sl] Telegram 失敗：{exc}")
            print(f"[sl] Telegram 推播：{'✓ 成功' if ok else '✗ 失敗'}")
            log_event(
                "intraday_stop_loss_summary_backfill_sent" if ok else "intraday_stop_loss_summary_backfill_failed",
                module="intraday_final_summary",
                severity="info" if ok else "error",
                summary=f"13:15 停損彙整補發：共 {len(sl_hits)} 檔（{'成功' if ok else '失敗'}）",
                payload={
                    "hits": [
                        {
                            "stock_id": h["stock_id"],
                            "price": h["price"],
                            "stop_loss": h["stop_loss"],
                            "pnl_pct": h["pnl_pct"],
                        }
                        for h in sl_hits
                    ],
                    "checked": len(holdings),
                    "note": "post-close backfill (Shioaji token 過期事件)",
                },
            )
    else:
        print("[sl] 無持股觸及停損，不發停損彙整")

    return 0


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    raise SystemExit(main(dry))
