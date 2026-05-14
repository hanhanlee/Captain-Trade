"""
盤中 N 字底候選清單 CRUD 包裝

設計：
  - scan_date 為單日鍵（每日 8:50 重建）
  - 同檔同日 upsert（stock_id + scan_date 為唯一鍵）
  - 提供清理舊紀錄、批次寫入、今日尚未推播清單、推播後標記等基礎操作
  - NPatternAlertHistory 提供跨日去重：同 stock_id + b_date 只推一次
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .database import get_session, init_db
from .models import NPatternAlertHistory, NPatternWatchlist


def _today() -> date:
    return date.today()


def upsert_candidates(items: Iterable[dict], scan_date: date | None = None) -> int:
    """
    批次寫入今日候選清單。
    items 內每筆需含：stock_id, stock_name, industry, a_date, a_price, b_date, b_price,
                    c_date, c_price, b_rise_pct, c_retrace_pct, vol_a_to_b_increase,
                    vol_b_to_c_decrease, avg_volume_b_to_c, last_close, distance_to_b_pct
    回傳寫入筆數。
    """
    init_db()
    sd = scan_date or _today()
    n = 0
    with get_session() as sess:
        for it in items:
            sid = str(it["stock_id"])
            existing = (
                sess.query(NPatternWatchlist)
                .filter_by(stock_id=sid, scan_date=sd)
                .first()
            )
            if existing is None:
                sess.add(NPatternWatchlist(
                    scan_date=sd,
                    stock_id=sid,
                    stock_name=it.get("stock_name"),
                    industry=it.get("industry"),
                    a_date=it.get("a_date"),
                    a_price=it.get("a_price"),
                    b_date=it.get("b_date"),
                    b_price=it.get("b_price"),
                    c_date=it.get("c_date"),
                    c_price=it.get("c_price"),
                    b_rise_pct=it.get("b_rise_pct"),
                    c_retrace_pct=it.get("c_retrace_pct"),
                    vol_a_to_b_increase=it.get("vol_a_to_b_increase"),
                    vol_b_to_c_decrease=it.get("vol_b_to_c_decrease"),
                    avg_volume_b_to_c=it.get("avg_volume_b_to_c"),
                    last_close=it.get("last_close"),
                    distance_to_b_pct=it.get("distance_to_b_pct"),
                    alerted_today=False,
                ))
            else:
                for field in (
                    "stock_name", "industry", "a_date", "a_price", "b_date", "b_price",
                    "c_date", "c_price", "b_rise_pct", "c_retrace_pct",
                    "vol_a_to_b_increase", "vol_b_to_c_decrease",
                    "avg_volume_b_to_c", "last_close", "distance_to_b_pct",
                ):
                    if field in it:
                        setattr(existing, field, it[field])
                existing.alerted_today = False     # 重建即視為新候選，重置推播旗標
            n += 1
        sess.commit()
    return n


def clear_today(scan_date: date | None = None) -> int:
    """重建前清掉今日紀錄（避免 actionable 名單變動後遺留舊條目）。"""
    init_db()
    sd = scan_date or _today()
    with get_session() as sess:
        deleted = (
            sess.query(NPatternWatchlist)
            .filter(NPatternWatchlist.scan_date == sd)
            .delete(synchronize_session=False)
        )
        sess.commit()
    return int(deleted or 0)


def purge_old(older_than_days: int = 7) -> int:
    """清理超過 older_than_days 天的歷史紀錄。"""
    init_db()
    cutoff = _today() - timedelta(days=older_than_days)
    with get_session() as sess:
        deleted = (
            sess.query(NPatternWatchlist)
            .filter(NPatternWatchlist.scan_date < cutoff)
            .delete(synchronize_session=False)
        )
        sess.commit()
    return int(deleted or 0)


def today_active() -> list[dict]:
    """取得今日所有「尚未推播」的候選（盤中 P2 用）。"""
    init_db()
    sd = _today()
    with get_session() as sess:
        rows = (
            sess.query(NPatternWatchlist)
            .filter(
                NPatternWatchlist.scan_date == sd,
                NPatternWatchlist.alerted_today == False,  # noqa: E712
            )
            .all()
        )
        return [_row_to_dict(r) for r in rows]


def today_all() -> list[dict]:
    """取得今日全部候選（含已推播），UI 顯示用。"""
    init_db()
    sd = _today()
    with get_session() as sess:
        rows = (
            sess.query(NPatternWatchlist)
            .filter(NPatternWatchlist.scan_date == sd)
            .order_by(NPatternWatchlist.distance_to_b_pct.asc())
            .all()
        )
        return [_row_to_dict(r) for r in rows]


def mark_alerted(stock_id: str, scan_date: date | None = None) -> bool:
    """推播成功後標記，避免同檔當日重複通知。"""
    init_db()
    sd = scan_date or _today()
    with get_session() as sess:
        row = (
            sess.query(NPatternWatchlist)
            .filter_by(stock_id=str(stock_id), scan_date=sd)
            .first()
        )
        if row is None:
            return False
        row.alerted_today = True
        sess.commit()
        return True


# ---------------------------------------------------------------------------
# 跨日去重：NPatternAlertHistory
# ---------------------------------------------------------------------------

def is_b_point_alerted(stock_id: str, b_date: date | None) -> bool:
    """
    查詢此 B 點（stock_id + b_date）是否已在歷史中推播過。

    注意：唯一鍵為 stock_id + b_date，因此即使偵測器因新的 C 點而重新識別
    出「同 B date 但不同 C date」的結構，該 B 點已推播過仍會被擋掉。
    """
    if b_date is None:
        return False
    init_db()
    with get_session() as sess:
        return (
            sess.query(NPatternAlertHistory)
            .filter_by(stock_id=str(stock_id), b_date=b_date)
            .first()
        ) is not None


def record_b_point_alert(
    stock_id: str,
    b_date: date | None,
    b_price: float | None = None,
    a_date: date | None = None,
    c_date: date | None = None,
) -> None:
    """
    推播成功後寫入歷史，供後續同 B 點跨日去重使用。
    若 unique constraint 衝突（極罕見的並發重入），靜默略過，不拋錯。
    """
    if b_date is None:
        return
    init_db()
    stmt = (
        sqlite_insert(NPatternAlertHistory)
        .values(
            stock_id=str(stock_id),
            b_date=b_date,
            b_price=b_price,
            a_date=a_date,
            c_date=c_date,
            alerted_at=datetime.now(),
        )
        .on_conflict_do_nothing(index_elements=["stock_id", "b_date"])
    )
    with get_session() as sess:
        sess.execute(stmt)
        sess.commit()


def purge_alert_history(older_than_days: int = 90) -> int:
    """
    清理超過 older_than_days 天的推播歷史。
    預設 90 天，大於 watchlist builder 的最大回看範圍（lookback=80 交易日），
    確保任何仍可能出現在 watchlist 的 B 點都還在歷史記憶中。
    """
    init_db()
    cutoff = date.today() - timedelta(days=older_than_days)
    with get_session() as sess:
        deleted = (
            sess.query(NPatternAlertHistory)
            .filter(NPatternAlertHistory.b_date < cutoff)
            .delete(synchronize_session=False)
        )
        sess.commit()
    return int(deleted or 0)


def _row_to_dict(r: NPatternWatchlist) -> dict:
    return {
        "stock_id": r.stock_id,
        "stock_name": r.stock_name,
        "industry": r.industry,
        "a_date": r.a_date,
        "a_price": r.a_price,
        "b_date": r.b_date,
        "b_price": r.b_price,
        "c_date": r.c_date,
        "c_price": r.c_price,
        "b_rise_pct": r.b_rise_pct,
        "c_retrace_pct": r.c_retrace_pct,
        "vol_a_to_b_increase": r.vol_a_to_b_increase,
        "vol_b_to_c_decrease": r.vol_b_to_c_decrease,
        "avg_volume_b_to_c": r.avg_volume_b_to_c,
        "last_close": r.last_close,
        "distance_to_b_pct": r.distance_to_b_pct,
        "alerted_today": bool(r.alerted_today),
        "scan_date": r.scan_date,
    }
