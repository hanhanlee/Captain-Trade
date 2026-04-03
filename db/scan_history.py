"""
掃描歷史紀錄的存取 helper
"""
import json
from datetime import datetime
import pandas as pd
from .database import get_session
from .models import ScanSession


def save_scan_session(
    result_df: pd.DataFrame,
    scan_mode: str,
    min_price: float,
    vol_filter: str,
    sector_filter: str,
    require_weekly: bool,
    min_rs: float,
    include_institutional: bool,
    top_sectors: dict,
) -> int:
    """
    儲存一次掃描結果，回傳 session id
    """
    results_list = result_df.to_dict("records") if not result_df.empty else []

    session_obj = ScanSession(
        scanned_at=datetime.now(),
        scan_mode=scan_mode,
        min_price=min_price,
        vol_filter=vol_filter,
        sector_filter=sector_filter,
        require_weekly=require_weekly,
        min_rs=min_rs,
        include_institutional=include_institutional,
        result_count=len(results_list),
        results_json=json.dumps(results_list, ensure_ascii=False),
        top_sectors_json=json.dumps(top_sectors, ensure_ascii=False),
    )

    with get_session() as sess:
        sess.add(session_obj)
        sess.commit()
        return session_obj.id


def load_scan_history(limit: int = 20) -> list[dict]:
    """
    載入最近 limit 筆掃描紀錄（不含結果明細，只有 metadata）
    """
    with get_session() as sess:
        rows = (
            sess.query(ScanSession)
            .order_by(ScanSession.scanned_at.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "id": r.id,
                "scanned_at": r.scanned_at,
                "scan_mode": r.scan_mode,
                "min_price": r.min_price,
                "vol_filter": r.vol_filter,
                "sector_filter": r.sector_filter,
                "require_weekly": r.require_weekly,
                "min_rs": r.min_rs,
                "include_institutional": r.include_institutional,
                "result_count": r.result_count,
            }
            for r in rows
        ]


def load_session_results(session_id: int) -> pd.DataFrame:
    """
    載入指定 session 的完整掃描結果 DataFrame
    """
    with get_session() as sess:
        row = sess.query(ScanSession).filter(ScanSession.id == session_id).first()
        if not row or not row.results_json:
            return pd.DataFrame()
        return pd.DataFrame(json.loads(row.results_json))


def delete_scan_session(session_id: int):
    """刪除指定掃描紀錄"""
    with get_session() as sess:
        sess.query(ScanSession).filter(ScanSession.id == session_id).delete()
        sess.commit()
