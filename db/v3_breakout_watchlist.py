"""
盤中 V3 三線齊穿候選清單 CRUD
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

from .database import get_session, init_db
from .models import V3BreakoutWatchlist


def _today() -> date:
    return date.today()


def upsert_candidates(items: Iterable[dict], scan_date: date | None = None) -> int:
    init_db()
    sd = scan_date or _today()
    n = 0
    with get_session() as sess:
        for it in items:
            sid = str(it["stock_id"])
            existing = (
                sess.query(V3BreakoutWatchlist)
                .filter_by(stock_id=sid, scan_date=sd)
                .first()
            )
            if existing is None:
                sess.add(V3BreakoutWatchlist(
                    scan_date=sd,
                    stock_id=sid,
                    stock_name=it.get("stock_name"),
                    industry=it.get("industry"),
                    ma5=it["ma5"],
                    ma10=it["ma10"],
                    ma20=it["ma20"],
                    vol_ma5=it["vol_ma5"],
                    last_close=it.get("last_close"),
                    ma_spread_pct=it.get("ma_spread_pct"),
                    alerted_today=False,
                ))
            else:
                for field in ("stock_name", "industry", "ma5", "ma10", "ma20",
                              "vol_ma5", "last_close", "ma_spread_pct"):
                    if field in it:
                        setattr(existing, field, it[field])
                # 不重置 alerted_today：既有列若已推播過，重建/再 upsert 時必須保留，
                # 否則頁面把已推播票顯示成 ⏳，且該票會重新進入 today_active() 被重推。
            n += 1
        sess.commit()
    return n


def clear_today(scan_date: date | None = None, *,
                preserve_alerted: bool = False) -> int:
    """清空今日候選。

    preserve_alerted=True 時保留「已推播」的列，只刪尚未推播的——供當天重建
    （手動 / 服務重啟在 misfire grace 內二次觸發）使用，避免洗掉已推播的票。
    """
    init_db()
    sd = scan_date or _today()
    with get_session() as sess:
        q = sess.query(V3BreakoutWatchlist).filter(V3BreakoutWatchlist.scan_date == sd)
        if preserve_alerted:
            q = q.filter(V3BreakoutWatchlist.alerted_today == False)  # noqa: E712
        deleted = q.delete(synchronize_session=False)
        sess.commit()
    return int(deleted or 0)


def purge_old(older_than_days: int = 7) -> int:
    init_db()
    cutoff = _today() - timedelta(days=older_than_days)
    with get_session() as sess:
        deleted = (
            sess.query(V3BreakoutWatchlist)
            .filter(V3BreakoutWatchlist.scan_date < cutoff)
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
            sess.query(V3BreakoutWatchlist)
            .filter(
                V3BreakoutWatchlist.scan_date == sd,
                V3BreakoutWatchlist.alerted_today == False,  # noqa: E712
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
            sess.query(V3BreakoutWatchlist)
            .filter(V3BreakoutWatchlist.scan_date == sd)
            .all()
        )
        return [_row_to_dict(r) for r in rows]


def mark_alerted(stock_id: str, scan_date: date | None = None) -> bool:
    init_db()
    sd = scan_date or _today()
    with get_session() as sess:
        row = (
            sess.query(V3BreakoutWatchlist)
            .filter_by(stock_id=str(stock_id), scan_date=sd)
            .first()
        )
        if row is None:
            return False
        row.alerted_today = True
        sess.commit()
        return True


def _row_to_dict(r: V3BreakoutWatchlist) -> dict:
    return {
        "stock_id": r.stock_id,
        "stock_name": r.stock_name,
        "industry": r.industry,
        "ma5": r.ma5,
        "ma10": r.ma10,
        "ma20": r.ma20,
        "vol_ma5": r.vol_ma5,
        "last_close": r.last_close,
        "ma_spread_pct": r.ma_spread_pct,
        "alerted_today": bool(r.alerted_today),
        "scan_date": r.scan_date,
    }
