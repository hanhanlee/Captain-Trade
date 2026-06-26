"""
盤中 v5 狙擊手版候選清單 CRUD
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

from .database import get_session, init_db
from .models import V5SniperWatchlist


def _today() -> date:
    return date.today()


_UPSERT_FIELDS = (
    "stock_name", "industry", "pattern_type",
    "breakout_target", "prev_high", "prev_close", "prev_volume",
    "vol_ma5", "ma5", "ma10", "ma20", "ma60",
    "ma_spread_pct", "max_gain_pct", "entry_path",
)


def upsert_candidates(items: Iterable[dict], scan_date: date | None = None) -> int:
    init_db()
    sd = scan_date or _today()
    n = 0
    with get_session() as sess:
        for it in items:
            sid = str(it["stock_id"])
            existing = (
                sess.query(V5SniperWatchlist)
                .filter_by(stock_id=sid, scan_date=sd)
                .first()
            )
            if existing is None:
                sess.add(V5SniperWatchlist(
                    scan_date=sd,
                    stock_id=sid,
                    stock_name=it.get("stock_name"),
                    industry=it.get("industry"),
                    pattern_type=it["pattern_type"],
                    breakout_target=it["breakout_target"],
                    prev_high=it["prev_high"],
                    prev_close=it.get("prev_close"),
                    prev_volume=it["prev_volume"],
                    vol_ma5=it.get("vol_ma5"),
                    ma5=it.get("ma5"),
                    ma10=it.get("ma10"),
                    ma20=it.get("ma20"),
                    ma60=it.get("ma60"),
                    ma_spread_pct=it.get("ma_spread_pct"),
                    max_gain_pct=it.get("max_gain_pct"),
                    entry_path=it.get("entry_path", "morning"),
                    alerted_today=False,
                ))
            else:
                for field in _UPSERT_FIELDS:
                    if field in it:
                        setattr(existing, field, it[field])
                # 不重置 alerted_today：既有列若已推播過（早盤 checker 或 late
                # qualifier 標記），重建/再 upsert 時必須保留標記，否則頁面會把
                # 已推播票顯示成 ⏳、且該票會重新進入 today_active() 被重推。
            n += 1
        sess.commit()
    return n


def clear_today(scan_date: date | None = None, *,
                preserve_alerted_and_late: bool = False) -> int:
    """清空今日候選。

    preserve_alerted_and_late=True 時，保留「已推播」與「late qualifier 盤中補進」
    的列，只刪掉尚未推播的 morning 列——供當天重建（手動重建 / 服務重啟在 misfire
    grace 內二次觸發）使用，避免把盤中才補進、已推播的票整列洗掉。
    """
    init_db()
    sd = scan_date or _today()
    with get_session() as sess:
        q = sess.query(V5SniperWatchlist).filter(V5SniperWatchlist.scan_date == sd)
        if preserve_alerted_and_late:
            from sqlalchemy import or_
            q = q.filter(
                V5SniperWatchlist.alerted_today == False,  # noqa: E712
                or_(
                    V5SniperWatchlist.entry_path == None,   # noqa: E711
                    V5SniperWatchlist.entry_path != "late",
                ),
            )
        deleted = q.delete(synchronize_session=False)
        sess.commit()
    return int(deleted or 0)


def purge_old(older_than_days: int = 7) -> int:
    init_db()
    cutoff = _today() - timedelta(days=older_than_days)
    with get_session() as sess:
        deleted = (
            sess.query(V5SniperWatchlist)
            .filter(V5SniperWatchlist.scan_date < cutoff)
            .delete(synchronize_session=False)
        )
        sess.commit()
    return int(deleted or 0)


def today_active() -> list[dict]:
    """今日尚未推播的候選（盤中用）。"""
    init_db()
    sd = _today()
    with get_session() as sess:
        rows = (
            sess.query(V5SniperWatchlist)
            .filter(
                V5SniperWatchlist.scan_date == sd,
                V5SniperWatchlist.alerted_today == False,  # noqa: E712
            )
            .all()
        )
        return [_row_to_dict(r) for r in rows]


def today_all() -> list[dict]:
    """今日全部候選（含已推播），UI 顯示用。"""
    init_db()
    sd = _today()
    with get_session() as sess:
        rows = (
            sess.query(V5SniperWatchlist)
            .filter(V5SniperWatchlist.scan_date == sd)
            .all()
        )
        return [_row_to_dict(r) for r in rows]


def mark_alerted(stock_id: str, scan_date: date | None = None) -> bool:
    init_db()
    sd = scan_date or _today()
    with get_session() as sess:
        row = (
            sess.query(V5SniperWatchlist)
            .filter_by(stock_id=str(stock_id), scan_date=sd)
            .first()
        )
        if row is None:
            return False
        row.alerted_today = True
        sess.commit()
        return True


def _row_to_dict(r: V5SniperWatchlist) -> dict:
    return {
        "stock_id": r.stock_id,
        "stock_name": r.stock_name,
        "industry": r.industry,
        "pattern_type": r.pattern_type,
        "breakout_target": r.breakout_target,
        "prev_high": r.prev_high,
        "prev_close": r.prev_close,
        "prev_volume": r.prev_volume,
        "vol_ma5": r.vol_ma5,
        "ma5": r.ma5,
        "ma10": r.ma10,
        "ma20": r.ma20,
        "ma60": r.ma60,
        "ma_spread_pct": r.ma_spread_pct,
        "max_gain_pct": r.max_gain_pct,
        "entry_path": r.entry_path or "morning",
        "alerted_today": bool(r.alerted_today),
        "scan_date": r.scan_date,
    }
