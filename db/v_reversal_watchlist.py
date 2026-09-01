"""
搶 V 型反轉候選清單 CRUD 包裝（比照 db/n_pattern_watchlist.py）。

  - scan_date 為單日鍵（每日 08:50 重建）
  - 同檔同日 upsert（stock_id + scan_date 唯一鍵）
  - VReversalAlertHistory 提供跨日去重：同 stock_id + stabilize_date 只推一次盤中預警
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .database import get_session, init_db
from .models import VReversalAlertHistory, VReversalWatchlist


_FIELDS = (
    "stock_name", "industry", "drop_pct", "down_days", "sharp_by_drop", "sharp_by_days",
    "spike_date", "spike_vol_ratio", "stabilize_date", "stabilize_is_doji", "stabilize_is_red",
    "trigger_price", "kd_k", "kd_d", "kd_low", "kd_golden_cross",
    "vol_ma5", "last_close", "distance_to_trigger_pct", "entry_path",
)


def _today() -> date:
    return date.today()


def upsert_candidates(items: Iterable[dict], scan_date: date | None = None) -> int:
    """批次寫入今日候選；同檔同日 upsert。回傳處理筆數。"""
    init_db()
    sd = scan_date or _today()
    n = 0
    with get_session() as sess:
        for it in items:
            sid = str(it["stock_id"])
            existing = (
                sess.query(VReversalWatchlist)
                .filter_by(stock_id=sid, scan_date=sd)
                .first()
            )
            if existing is None:
                kwargs = {f: it.get(f) for f in _FIELDS}
                sess.add(VReversalWatchlist(
                    scan_date=sd, stock_id=sid,
                    alerted_today=False, confirmed_today=False,
                    **kwargs,
                ))
            else:
                for f in _FIELDS:
                    if f in it:
                        setattr(existing, f, it[f])
                # 不重置 alerted_today / confirmed_today：重建時保留已推播狀態
            n += 1
        sess.commit()
    return n


def clear_today(scan_date: date | None = None, *, preserve_alerted: bool = False) -> int:
    """重建前清今日紀錄；preserve_alerted=True 保留已推播（盤中預警或收盤確認）的列。"""
    init_db()
    sd = scan_date or _today()
    with get_session() as sess:
        q = sess.query(VReversalWatchlist).filter(VReversalWatchlist.scan_date == sd)
        if preserve_alerted:
            q = q.filter(
                VReversalWatchlist.alerted_today == False,   # noqa: E712
                VReversalWatchlist.confirmed_today == False,  # noqa: E712
            )
        deleted = q.delete(synchronize_session=False)
        sess.commit()
    return int(deleted or 0)


def purge_old(older_than_days: int = 7) -> int:
    init_db()
    cutoff = _today() - timedelta(days=older_than_days)
    with get_session() as sess:
        deleted = (
            sess.query(VReversalWatchlist)
            .filter(VReversalWatchlist.scan_date < cutoff)
            .delete(synchronize_session=False)
        )
        sess.commit()
    return int(deleted or 0)


def today_active() -> list[dict]:
    """今日「尚未發盤中預警」的候選（盤中即時檢查用）。"""
    init_db()
    with get_session() as sess:
        rows = (
            sess.query(VReversalWatchlist)
            .filter(
                VReversalWatchlist.scan_date == _today(),
                VReversalWatchlist.alerted_today == False,  # noqa: E712
            )
            .all()
        )
        return [_row_to_dict(r) for r in rows]


def today_all() -> list[dict]:
    """今日全部候選（含已推播），UI / 收盤確認用。"""
    init_db()
    with get_session() as sess:
        rows = (
            sess.query(VReversalWatchlist)
            .filter(VReversalWatchlist.scan_date == _today())
            .order_by(VReversalWatchlist.distance_to_trigger_pct.asc())
            .all()
        )
        return [_row_to_dict(r) for r in rows]


def mark_alerted(stock_id: str, scan_date: date | None = None) -> bool:
    """標記盤中預警已推。"""
    return _set_flag(stock_id, "alerted_today", scan_date)


def mark_confirmed(stock_id: str, scan_date: date | None = None) -> bool:
    """標記收盤確認已推。"""
    return _set_flag(stock_id, "confirmed_today", scan_date)


def _set_flag(stock_id: str, field: str, scan_date: date | None) -> bool:
    init_db()
    sd = scan_date or _today()
    with get_session() as sess:
        row = (
            sess.query(VReversalWatchlist)
            .filter_by(stock_id=str(stock_id), scan_date=sd)
            .first()
        )
        if row is None:
            return False
        setattr(row, field, True)
        sess.commit()
        return True


# ── 跨日去重：VReversalAlertHistory ─────────────────────────────────────
def is_structure_alerted(stock_id: str, stabilize_date: date | None) -> bool:
    if stabilize_date is None:
        return False
    init_db()
    with get_session() as sess:
        return (
            sess.query(VReversalAlertHistory)
            .filter_by(stock_id=str(stock_id), stabilize_date=stabilize_date)
            .first()
        ) is not None


def record_structure_alert(stock_id: str, stabilize_date: date | None,
                           trigger_price: float | None = None,
                           spike_date: date | None = None) -> None:
    if stabilize_date is None:
        return
    init_db()
    stmt = (
        sqlite_insert(VReversalAlertHistory)
        .values(
            stock_id=str(stock_id), stabilize_date=stabilize_date,
            trigger_price=trigger_price, spike_date=spike_date,
            alerted_at=datetime.now(),
        )
        .on_conflict_do_nothing(index_elements=["stock_id", "stabilize_date"])
    )
    with get_session() as sess:
        sess.execute(stmt)
        sess.commit()


def purge_alert_history(older_than_days: int = 30) -> int:
    init_db()
    cutoff = date.today() - timedelta(days=older_than_days)
    with get_session() as sess:
        deleted = (
            sess.query(VReversalAlertHistory)
            .filter(VReversalAlertHistory.stabilize_date < cutoff)
            .delete(synchronize_session=False)
        )
        sess.commit()
    return int(deleted or 0)


def _row_to_dict(r: VReversalWatchlist) -> dict:
    return {
        "stock_id": r.stock_id, "stock_name": r.stock_name, "industry": r.industry,
        "drop_pct": r.drop_pct, "down_days": r.down_days,
        "sharp_by_drop": bool(r.sharp_by_drop), "sharp_by_days": bool(r.sharp_by_days),
        "spike_date": r.spike_date, "spike_vol_ratio": r.spike_vol_ratio,
        "stabilize_date": r.stabilize_date,
        "stabilize_is_doji": bool(r.stabilize_is_doji), "stabilize_is_red": bool(r.stabilize_is_red),
        "trigger_price": r.trigger_price,
        "kd_k": r.kd_k, "kd_d": r.kd_d, "kd_low": bool(r.kd_low),
        "kd_golden_cross": bool(r.kd_golden_cross),
        "vol_ma5": r.vol_ma5, "last_close": r.last_close,
        "distance_to_trigger_pct": r.distance_to_trigger_pct,
        "alerted_today": bool(r.alerted_today), "confirmed_today": bool(r.confirmed_today),
        "entry_path": r.entry_path, "scan_date": r.scan_date,
    }
