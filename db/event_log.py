"""
Event Log — 統一寫入與查詢介面

所有模組透過 log_event() 寫入，永不 raise（失敗只 warning）。
"""
import json
import logging
from datetime import datetime

from .database import ENGINE
from sqlalchemy import text

logger = logging.getLogger(__name__)

STRATEGY_VERSION = "v4.3"


def log_event(
    event_type: str,
    module: str | None = None,
    scan_id: str | None = None,
    stock_id: str | None = None,
    stock_name: str | None = None,
    severity: str = "info",
    summary: str = "",
    payload: dict | None = None,
) -> None:
    """寫入一筆事件。失敗只 log warning，不影響呼叫端流程。"""
    try:
        payload_json = json.dumps(payload or {}, ensure_ascii=False, default=str)
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with ENGINE.begin() as conn:
            conn.execute(text("""
                INSERT INTO event_log
                    (created_at, event_type, module, scan_id, stock_id, stock_name,
                     severity, summary, payload_json)
                VALUES
                    (:created_at, :event_type, :module, :scan_id, :stock_id, :stock_name,
                     :severity, :summary, :payload_json)
            """), {
                "created_at": created_at,
                "event_type": event_type,
                "module": module,
                "scan_id": scan_id,
                "stock_id": stock_id,
                "stock_name": stock_name,
                "severity": severity,
                "summary": summary,
                "payload_json": payload_json,
            })
    except Exception as exc:
        logger.warning("event_log 寫入失敗（event_type=%s）：%s", event_type, exc)


def query_events(
    event_type: str | None = None,
    module: str | None = None,
    stock_id: str | None = None,
    scan_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    severity: str | None = None,
    limit: int = 500,
    offset: int = 0,
) -> list[dict]:
    """查詢事件，回傳 dict list（newest first）。"""
    try:
        clauses = []
        params: dict = {}
        if event_type:
            clauses.append("event_type = :event_type")
            params["event_type"] = event_type
        if module:
            clauses.append("module = :module")
            params["module"] = module
        if stock_id:
            clauses.append("stock_id = :stock_id")
            params["stock_id"] = stock_id
        if scan_id:
            clauses.append("scan_id = :scan_id")
            params["scan_id"] = scan_id
        if severity:
            clauses.append("severity = :severity")
            params["severity"] = severity
        if date_from:
            clauses.append("created_at >= :date_from")
            params["date_from"] = date_from
        if date_to:
            clauses.append("created_at <= :date_to")
            params["date_to"] = date_to + " 23:59:59"

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params["limit"] = limit
        params["offset"] = offset

        sql = f"""
            SELECT id, created_at, event_type, module, scan_id, stock_id, stock_name,
                   severity, summary, payload_json
            FROM event_log
            {where}
            ORDER BY id DESC
            LIMIT :limit OFFSET :offset
        """
        with ENGINE.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.warning("event_log 查詢失敗：%s", exc)
        return []


def count_events(
    event_type: str | None = None,
    module: str | None = None,
    stock_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> int:
    """回傳符合條件的事件總數（用於分頁）。"""
    try:
        clauses = []
        params: dict = {}
        if event_type:
            clauses.append("event_type = :event_type")
            params["event_type"] = event_type
        if module:
            clauses.append("module = :module")
            params["module"] = module
        if stock_id:
            clauses.append("stock_id = :stock_id")
            params["stock_id"] = stock_id
        if date_from:
            clauses.append("created_at >= :date_from")
            params["date_from"] = date_from
        if date_to:
            clauses.append("created_at <= :date_to")
            params["date_to"] = date_to + " 23:59:59"

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with ENGINE.connect() as conn:
            row = conn.execute(text(f"SELECT COUNT(*) FROM event_log {where}"), params).fetchone()
        return int(row[0]) if row else 0
    except Exception as exc:
        logger.warning("event_log count 失敗：%s", exc)
        return 0


def get_scan_timeline(scan_id: str) -> list[dict]:
    """取得某次掃描的全部事件（by scan_id）。"""
    return query_events(scan_id=scan_id, limit=1000)


def make_scan_id(scan_date: str, scan_mode: str) -> str:
    """產生掃描批次 ID，格式：YYYYMMDD_HHMMSS_<mode>。"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_mode = scan_mode.replace(" ", "_").replace("/", "-")[:20]
    return f"{ts}_{safe_mode}"
