"""
盤後法人 / 融資融券 缺口補抓（指定日期）

用途：
    服務晚啟動（如隔夜重開機沒起來、過午才啟動）會錯過盤後高效抓取窗口，
    prefetch 背景執行緒只會補「最新交易日」，較早一天（如前一日）的 inst/margin
    缺口不會自動補。本腳本針對指定日期，把「近期完整日有、但目標日缺」的股票
    逐檔補抓 inst + margin。

設計重點（免費版 600 req/hr 上限下）：
    - 每檔 days=8 一次請求即可同時回多日（6/20~目標日），順帶補滿鄰近交易日。
    - 對 402 / 429 / quota 例外自動退避（sleep 後重試），絕不寫壞資料。
    - 沿用 prefetch 既有的 get_*/save_* 函式與 inst/margin schema。
    - 故意保守節流（預設 2s/檔），與正在運行的 prefetch 共用同一 FinMind 帳號額度時
      也不至於暴力撞線；真的撞到就退避。

用法：
    python scripts/backfill_inst_margin.py                 # 預設補 2026-06-23
    python scripts/backfill_inst_margin.py 2026-06-23 2026-06-24
    python scripts/backfill_inst_margin.py --baseline 2026-06-22 2026-06-23
"""
from __future__ import annotations

import argparse
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import sqlite3  # noqa: E402

from data.finmind_client import get_institutional_investors, get_margin_trading  # noqa: E402
from db.inst_cache import save_institutional  # noqa: E402
from db.margin_cache import save_margin  # noqa: E402

DB_PATH = _PROJECT_ROOT / "srock.db"

# days 視窗：足以涵蓋 baseline 前幾天到目標日（含週末）。
FETCH_DAYS = 8
# 每檔請求間隔（秒）。免費版 600/hr ≈ 6s/req；2s 是樂觀值，撞線會自動退避。
SLEEP_SEC = 2.0
# 撞到 402/429/quota 時的退避秒數。
RATE_LIMIT_BACKOFF_SEC = 90


def _is_rate_limited(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(k in msg for k in ("429", "402", "403", "too many requests",
                                  "rate limit", "upper limit", "quota"))


def _distinct_ids(table: str, date_str: str) -> set[str]:
    con = sqlite3.connect(str(DB_PATH))
    try:
        rows = con.execute(
            f"SELECT DISTINCT stock_id FROM {table} WHERE date = ?", (date_str,)
        ).fetchall()
    finally:
        con.close()
    return {str(r[0]) for r in rows if r and r[0]}


def _has_date(df, date_str: str) -> bool:
    if df is None or df.empty:
        return False
    return any(str(x)[:10] == date_str for x in df["date"])


def _backfill_one_dataset(label: str, targets: list[str], target_date: str,
                          fetch_fn, save_fn) -> dict:
    stats = {"filled": 0, "no_source_data": 0, "error": 0, "requests": 0}
    total = len(targets)
    for i, sid in enumerate(targets, 1):
        while True:
            try:
                df = fetch_fn(sid, days=FETCH_DAYS)
                stats["requests"] += 1
                break
            except Exception as exc:
                if _is_rate_limited(exc):
                    print(f"  [{label}] 撞額度上限，退避 {RATE_LIMIT_BACKOFF_SEC}s … ({sid})",
                          flush=True)
                    time.sleep(RATE_LIMIT_BACKOFF_SEC)
                    continue
                stats["error"] += 1
                print(f"  [{label}] {sid} 抓取錯誤：{exc}", flush=True)
                df = None
                break

        if df is not None and not df.empty:
            # 存全部回傳日期（含鄰近交易日，順帶補滿）
            try:
                save_fn(sid, df)
            except Exception as exc:
                stats["error"] += 1
                print(f"  [{label}] {sid} 存檔失敗：{exc}", flush=True)
            if _has_date(df, target_date):
                stats["filled"] += 1
            else:
                # 來源就沒有目標日資料（小型股當天無進出 → 不出列），屬正常。
                stats["no_source_data"] += 1
        else:
            stats["no_source_data"] += 1

        if i % 50 == 0 or i == total:
            print(f"  [{label}] {i}/{total} 補到 {stats['filled']} | "
                  f"來源無資料 {stats['no_source_data']} | 錯誤 {stats['error']}",
                  flush=True)
        time.sleep(SLEEP_SEC)
    return stats


def backfill_date(target_date: str, baseline_date: str) -> None:
    print(f"\n=== 補抓 {target_date}（基準完整日 {baseline_date}）===", flush=True)

    inst_base = _distinct_ids("inst_cache", baseline_date)
    inst_done = _distinct_ids("inst_cache", target_date)
    inst_targets = sorted(inst_base - inst_done)

    mar_base = _distinct_ids("margin_cache", baseline_date)
    mar_done = _distinct_ids("margin_cache", target_date)
    mar_targets = sorted(mar_base - mar_done)

    print(f"inst 待補 {len(inst_targets)} 檔（基準 {len(inst_base)} / 已有 {len(inst_done)}）",
          flush=True)
    print(f"margin 待補 {len(mar_targets)} 檔（基準 {len(mar_base)} / 已有 {len(mar_done)}）",
          flush=True)

    if mar_targets:
        s = _backfill_one_dataset("融資", mar_targets, target_date,
                                  get_margin_trading, save_margin)
        print(f"融資完成：實補 {s['filled']} 檔，來源無資料 {s['no_source_data']}，"
              f"錯誤 {s['error']}，請求 {s['requests']}", flush=True)

    if inst_targets:
        s = _backfill_one_dataset("法人", inst_targets, target_date,
                                  get_institutional_investors, save_institutional)
        print(f"法人完成：實補 {s['filled']} 檔，來源無資料 {s['no_source_data']}，"
              f"錯誤 {s['error']}，請求 {s['requests']}", flush=True)

    # 補後覆蓋率
    print(f"--- {target_date} 補後 ---", flush=True)
    print(f"inst distinct = {len(_distinct_ids('inst_cache', target_date))}", flush=True)
    print(f"margin distinct = {len(_distinct_ids('margin_cache', target_date))}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="盤後 inst/margin 缺口補抓")
    ap.add_argument("dates", nargs="*", default=["2026-06-23"],
                    help="要補的目標日期（YYYY-MM-DD），可多個")
    ap.add_argument("--baseline", default="2026-06-22",
                    help="當作完整參考的交易日（預設 2026-06-22）")
    args = ap.parse_args()

    dates = args.dates or ["2026-06-23"]
    print(f"開始補抓 [{datetime.now():%Y-%m-%d %H:%M:%S}]  目標 {dates}  "
          f"基準 {args.baseline}  節流 {SLEEP_SEC}s/檔", flush=True)
    for d in dates:
        backfill_date(d, args.baseline)
    print(f"\n全部完成 [{datetime.now():%Y-%m-%d %H:%M:%S}]", flush=True)


if __name__ == "__main__":
    main()
