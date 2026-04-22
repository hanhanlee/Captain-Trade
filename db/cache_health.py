from __future__ import annotations

from datetime import datetime

import pandas as pd
from sqlalchemy import text

from .database import get_session


def create_health_run(
    dataset: str,
    date_from: str,
    date_to: str,
    *,
    requested_by: str = "streamlit",
    notes: str = "",
) -> int:
    now = datetime.now().isoformat(timespec="seconds")
    with get_session() as sess:
        row = sess.execute(text("""
            INSERT INTO cache_health_run
            (
                dataset, date_from, date_to, requested_at, status, requested_by, notes
            )
            VALUES
            (
                :dataset, :date_from, :date_to, :requested_at, 'queued', :requested_by, :notes
            )
            RETURNING id
        """), {
            "dataset": dataset,
            "date_from": date_from,
            "date_to": date_to,
            "requested_at": now,
            "requested_by": requested_by,
            "notes": notes,
        }).fetchone()
        sess.commit()
    return int(row[0])


def update_health_run(run_id: int, **fields) -> None:
    if not fields:
        return
    assignments = []
    params = {"run_id": int(run_id)}
    for key, value in fields.items():
        assignments.append(f"{key} = :{key}")
        if isinstance(value, datetime):
            value = value.isoformat(timespec="seconds")
        params[key] = value
    with get_session() as sess:
        sess.execute(text(f"""
            UPDATE cache_health_run
            SET {', '.join(assignments)}
            WHERE id = :run_id
        """), params)
        sess.commit()


def get_health_run(run_id: int) -> dict | None:
    with get_session() as sess:
        row = sess.execute(text("""
            SELECT *
            FROM cache_health_run
            WHERE id = :run_id
        """), {"run_id": int(run_id)}).mappings().first()
    return dict(row) if row else None


def list_health_runs(*, dataset: str | None = None, limit: int = 20) -> pd.DataFrame:
    where_sql = ""
    params = {"limit": int(limit)}
    if dataset:
        where_sql = "WHERE dataset = :dataset"
        params["dataset"] = dataset
    with get_session() as sess:
        rows = sess.execute(text(f"""
            SELECT *
            FROM cache_health_run
            {where_sql}
            ORDER BY requested_at DESC, id DESC
            LIMIT :limit
        """), params).mappings().all()
    return pd.DataFrame([dict(r) for r in rows])


def list_latest_health_runs_by_dataset() -> pd.DataFrame:
    with get_session() as sess:
        rows = sess.execute(text("""
            SELECT r.*
            FROM cache_health_run r
            JOIN (
                SELECT dataset, MAX(id) AS max_id
                FROM cache_health_run
                GROUP BY dataset
            ) latest
              ON latest.dataset = r.dataset
             AND latest.max_id = r.id
            ORDER BY r.dataset ASC
        """
        )).mappings().all()
    return pd.DataFrame([dict(r) for r in rows])


def replace_daily_summary(run_id: int, dataset: str, rows: list[dict]) -> None:
    with get_session() as sess:
        sess.execute(text("DELETE FROM cache_health_daily_summary WHERE run_id = :run_id"), {
            "run_id": int(run_id)
        })
        if rows:
            payload = []
            for row in rows:
                payload.append({
                    "run_id": int(run_id),
                    "dataset": dataset,
                    "trade_date": row["trade_date"],
                    "expected_count": int(row.get("expected_count") or 0),
                    "present_count": int(row.get("present_count") or 0),
                    "missing_count": int(row.get("missing_count") or 0),
                    "completeness_pct": float(row.get("completeness_pct") or 0.0),
                })
            sess.execute(text("""
                INSERT INTO cache_health_daily_summary
                (
                    run_id, dataset, trade_date, expected_count,
                    present_count, missing_count, completeness_pct
                )
                VALUES
                (
                    :run_id, :dataset, :trade_date, :expected_count,
                    :present_count, :missing_count, :completeness_pct
                )
            """), payload)
        sess.commit()


def replace_gap_rows(run_id: int, dataset: str, rows: list[dict]) -> None:
    with get_session() as sess:
        sess.execute(text("DELETE FROM cache_health_gap WHERE run_id = :run_id"), {
            "run_id": int(run_id)
        })
        if rows:
            sess.execute(text("""
                INSERT INTO cache_health_gap
                (
                    run_id, dataset, trade_date, stock_id, gap_type,
                    severity, detail_json, repair_status
                )
                VALUES
                (
                    :run_id, :dataset, :trade_date, :stock_id, :gap_type,
                    :severity, :detail_json, :repair_status
                )
            """), rows)
        sess.commit()


def get_run_daily_summary(run_id: int) -> pd.DataFrame:
    with get_session() as sess:
        rows = sess.execute(text("""
            SELECT trade_date, expected_count, present_count, missing_count, completeness_pct
            FROM cache_health_daily_summary
            WHERE run_id = :run_id
            ORDER BY trade_date ASC
        """), {"run_id": int(run_id)}).mappings().all()
    return pd.DataFrame([dict(r) for r in rows])


def get_run_gaps(
    run_id: int,
    *,
    only_pending: bool = False,
    limit: int | None = None,
) -> pd.DataFrame:
    clauses = ["run_id = :run_id"]
    params: dict[str, object] = {"run_id": int(run_id)}
    if only_pending:
        clauses.append("repair_status IN ('pending', 'error')")
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT :limit"
        params["limit"] = int(limit)
    with get_session() as sess:
        rows = sess.execute(text(f"""
            SELECT id, dataset, trade_date, stock_id, gap_type, severity,
                   repair_status, repaired_at, repair_error
            FROM cache_health_gap
            WHERE {' AND '.join(clauses)}
            ORDER BY trade_date ASC, stock_id ASC
            {limit_sql}
        """), params).mappings().all()
    return pd.DataFrame([dict(r) for r in rows])


def create_repair_job(run_id: int, dataset: str, target_count: int) -> int:
    now = datetime.now().isoformat(timespec="seconds")
    with get_session() as sess:
        row = sess.execute(text("""
            INSERT INTO cache_health_repair_job
            (
                run_id, dataset, status, requested_at, target_count
            )
            VALUES
            (
                :run_id, :dataset, 'queued', :requested_at, :target_count
            )
            RETURNING id
        """), {
            "run_id": int(run_id),
            "dataset": dataset,
            "requested_at": now,
            "target_count": int(target_count),
        }).fetchone()
        sess.commit()
    return int(row[0])


def update_repair_job(job_id: int, **fields) -> None:
    if not fields:
        return
    assignments = []
    params = {"job_id": int(job_id)}
    for key, value in fields.items():
        assignments.append(f"{key} = :{key}")
        if isinstance(value, datetime):
            value = value.isoformat(timespec="seconds")
        params[key] = value
    with get_session() as sess:
        sess.execute(text(f"""
            UPDATE cache_health_repair_job
            SET {', '.join(assignments)}
            WHERE id = :job_id
        """), params)
        sess.commit()


def get_repair_job(job_id: int) -> dict | None:
    with get_session() as sess:
        row = sess.execute(text("""
            SELECT *
            FROM cache_health_repair_job
            WHERE id = :job_id
        """), {"job_id": int(job_id)}).mappings().first()
    return dict(row) if row else None


def list_repair_jobs(*, run_id: int | None = None, limit: int = 20) -> pd.DataFrame:
    where_sql = ""
    params = {"limit": int(limit)}
    if run_id is not None:
        where_sql = "WHERE run_id = :run_id"
        params["run_id"] = int(run_id)
    with get_session() as sess:
        rows = sess.execute(text(f"""
            SELECT *
            FROM cache_health_repair_job
            {where_sql}
            ORDER BY requested_at DESC, id DESC
            LIMIT :limit
        """), params).mappings().all()
    return pd.DataFrame([dict(r) for r in rows])


def mark_gap_repair_status(
    run_id: int,
    dataset: str,
    trade_date: str,
    stock_ids: list[str],
    *,
    status: str,
    repair_error: str = "",
) -> None:
    if not stock_ids:
        return
    placeholders = ",".join(f":sid{i}" for i in range(len(stock_ids)))
    params = {
        "run_id": int(run_id),
        "dataset": dataset,
        "trade_date": trade_date,
        "status": status,
        "repair_error": repair_error[:500],
        "repaired_at": datetime.now().isoformat(timespec="seconds") if status == "repaired" else None,
    }
    params.update({f"sid{i}": sid for i, sid in enumerate(stock_ids)})
    with get_session() as sess:
        sess.execute(text(f"""
            UPDATE cache_health_gap
            SET repair_status = :status,
                repaired_at = :repaired_at,
                repair_error = :repair_error
            WHERE run_id = :run_id
              AND dataset = :dataset
              AND trade_date = :trade_date
              AND stock_id IN ({placeholders})
        """), params)
        sess.commit()