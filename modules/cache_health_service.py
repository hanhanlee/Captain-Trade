from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from data.cache_health_registry import get_cache_health_dataset, list_cache_health_datasets
from data.finmind_client import (
    get_all_institutional_by_date,
    get_all_margin_by_date,
    get_all_prices_by_date,
    get_broker_main_force_series,
    get_taiwan_stock_trading_dates,
    is_taiwan_stock_trading_day,
)
from db.broker_cache import load_broker_main_force
from db.cache_health import (
    create_repair_job,
    get_health_run,
    get_repair_job,
    get_run_gaps,
    mark_gap_repair_status,
    replace_daily_summary,
    replace_gap_rows,
    update_health_run,
    update_repair_job,
)
from db.database import get_session
from db.inst_cache import save_institutional_batch
from db.margin_cache import save_margin_batch
from db.price_cache import (
    get_all_cached_stocks,
    get_delisted_stocks,
    get_known_stock_ids,
    load_suspend_ids,
    save_prices_batch,
)

logger = logging.getLogger(__name__)


def list_dataset_specs():
    return list_cache_health_datasets()


def _normalize_date_str(value) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _active_stock_ids() -> list[str]:
    known_ids = get_known_stock_ids()
    if not known_ids:
        known_ids = get_all_cached_stocks()
    if not known_ids:
        return []
    suspend_ids = load_suspend_ids()
    delisted_ids = set(get_delisted_stocks(include_legacy_no_update=True))
    active = [sid for sid in known_ids if sid not in suspend_ids and sid not in delisted_ids]
    return sorted(dict.fromkeys(active))


def _trading_days(date_from: str, date_to: str) -> list[str]:
    try:
        df = get_taiwan_stock_trading_dates(date_from, date_to)
        if not df.empty:
            return df["date"].dt.strftime("%Y-%m-%d").tolist()
    except Exception as exc:
        logger.warning("official trading date lookup failed %s ~ %s: %s", date_from, date_to, exc)
    dates = pd.bdate_range(date_from, date_to)
    return [d.strftime("%Y-%m-%d") for d in dates]


def _dataset_bounds(dataset: str) -> dict:
    spec = get_cache_health_dataset(dataset)
    with get_session() as sess:
        row = sess.execute(text(f"""
            SELECT MIN({spec.date_column}) AS earliest_cached_date,
                   MAX({spec.date_column}) AS latest_cached_date,
                   COUNT(*) AS row_count,
                   COUNT(DISTINCT stock_id) AS stock_count
            FROM {spec.table_name}
        """
        )).mappings().first()
    return dict(row or {})


def _present_stock_ids_by_date(dataset: str, date_from: str, date_to: str) -> dict[str, set[str]]:
    spec = get_cache_health_dataset(dataset)
    with get_session() as sess:
        rows = sess.execute(text(f"""
            SELECT {spec.date_column} AS trade_date, stock_id
            FROM {spec.table_name}
            WHERE {spec.date_column} >= :date_from
              AND {spec.date_column} <= :date_to
            GROUP BY {spec.date_column}, stock_id
            ORDER BY {spec.date_column} ASC, stock_id ASC
        """), {
            "date_from": date_from,
            "date_to": date_to,
        }).fetchall()
    mapping: dict[str, set[str]] = {}
    for trade_date, stock_id in rows:
        day = str(trade_date)[:10]
        mapping.setdefault(day, set()).add(str(stock_id))
    return mapping


def run_health_scan(run_id: int) -> dict:
    run = get_health_run(run_id)
    if not run:
        raise ValueError(f"Unknown health run: {run_id}")

    dataset = run["dataset"]
    date_from = _normalize_date_str(run["date_from"])
    date_to = _normalize_date_str(run["date_to"])

    update_health_run(run_id, status="running", started_at=datetime.now(), error_message="")

    active_ids = _active_stock_ids()
    trading_days = _trading_days(date_from, date_to)
    bounds = _dataset_bounds(dataset)
    present_by_date = _present_stock_ids_by_date(dataset, date_from, date_to)

    active_set = set(active_ids)
    expected_per_day = len(active_ids)
    daily_rows: list[dict] = []
    gap_rows: list[dict] = []
    total_present = 0

    for trade_date in trading_days:
        present_ids = present_by_date.get(trade_date, set()) & active_set
        missing_ids = sorted(active_set - present_ids)
        present_count = len(present_ids)
        missing_count = len(missing_ids)
        total_present += present_count
        completeness_pct = round((present_count / expected_per_day) * 100, 2) if expected_per_day else 0.0
        daily_rows.append({
            "trade_date": trade_date,
            "expected_count": expected_per_day,
            "present_count": present_count,
            "missing_count": missing_count,
            "completeness_pct": completeness_pct,
        })
        for stock_id in missing_ids:
            gap_rows.append({
                "run_id": int(run_id),
                "dataset": dataset,
                "trade_date": trade_date,
                "stock_id": stock_id,
                "gap_type": "missing",
                "severity": "normal",
                "detail_json": "",
                "repair_status": "pending",
            })

    replace_daily_summary(run_id, dataset, daily_rows)
    replace_gap_rows(run_id, dataset, gap_rows)

    total_expected = expected_per_day * len(trading_days)
    total_missing = max(total_expected - total_present, 0)
    completeness = round((total_present / total_expected) * 100, 2) if total_expected else 0.0
    update_health_run(
        run_id,
        status="completed",
        finished_at=datetime.now(),
        total_expected_units=total_expected,
        total_present_units=total_present,
        total_missing_units=total_missing,
        completeness_pct=completeness,
        earliest_cached_date=(str(bounds.get("earliest_cached_date") or "")[:10] or None),
        latest_cached_date=(str(bounds.get("latest_cached_date") or "")[:10] or None),
        notes="交易日優先使用 FinMind TaiwanStockTradingDate；若查詢失敗才退回週一到週五近似。",
    )
    return get_health_run(run_id) or {}


def _existing_ids_for_date(dataset: str, trade_date: str, stock_ids: list[str]) -> set[str]:
    if not stock_ids:
        return set()
    spec = get_cache_health_dataset(dataset)
    placeholders = ",".join(f":sid{i}" for i in range(len(stock_ids)))
    params = {"trade_date": trade_date}
    params.update({f"sid{i}": sid for i, sid in enumerate(stock_ids)})
    with get_session() as sess:
        rows = sess.execute(text(f"""
            SELECT DISTINCT stock_id
            FROM {spec.table_name}
            WHERE {spec.date_column} = :trade_date
              AND stock_id IN ({placeholders})
        """), params).fetchall()
    return {str(r[0]) for r in rows if r[0]}


def _repair_price_gaps(trade_date: str, stock_ids: list[str]) -> set[str]:
    df = get_all_prices_by_date(trade_date)
    if df.empty:
        return set()
    filtered = df[df["stock_id"].astype(str).isin(set(stock_ids))].copy()
    if filtered.empty:
        return set()
    save_prices_batch(filtered)
    return _existing_ids_for_date("price", trade_date, stock_ids)


def _repair_institutional_gaps(trade_date: str, stock_ids: list[str]) -> set[str]:
    df = get_all_institutional_by_date(trade_date)
    if df.empty:
        return set()
    filtered = df[df["stock_id"].astype(str).isin(set(stock_ids))].copy()
    if filtered.empty:
        return set()
    save_institutional_batch(filtered)
    return _existing_ids_for_date("institutional", trade_date, stock_ids)


def _repair_margin_gaps(trade_date: str, stock_ids: list[str]) -> set[str]:
    df = get_all_margin_by_date(trade_date)
    if df.empty:
        return set()
    filtered = df[df["stock_id"].astype(str).isin(set(stock_ids))].copy()
    if filtered.empty:
        return set()
    save_margin_batch(filtered)
    return _existing_ids_for_date("margin", trade_date, stock_ids)


def _repair_broker_gaps(trade_date: str, stock_ids: list[str]) -> set[str]:
    repaired: set[str] = set()
    for stock_id in stock_ids:
        try:
            get_broker_main_force_series(stock_id, [trade_date], force_refresh=True)
        except Exception as exc:
            logger.warning("broker repair failed %s %s: %s", stock_id, trade_date, exc)
            continue
        cached = load_broker_main_force(stock_id, [trade_date])
        if not cached.empty:
            repaired.add(stock_id)
    return repaired


def create_repair_job_for_run(run_id: int) -> int:
    run = get_health_run(run_id)
    if not run:
        raise ValueError(f"Unknown health run: {run_id}")
    gaps = get_run_gaps(run_id, only_pending=True)
    return create_repair_job(run_id, run["dataset"], len(gaps))


def run_repair_job(job_id: int) -> dict:
    job = get_repair_job(job_id)
    if not job:
        raise ValueError(f"Unknown repair job: {job_id}")

    run_id = int(job["run_id"])
    dataset = str(job["dataset"])
    gaps = get_run_gaps(run_id, only_pending=True)
    if gaps.empty:
        update_repair_job(job_id, status="completed", started_at=datetime.now(), finished_at=datetime.now(), target_count=0, done_count=0)
        return get_repair_job(job_id) or {}

    update_repair_job(job_id, status="running", started_at=datetime.now(), target_count=len(gaps), done_count=0, error_count=0, last_error="")

    done_count = 0
    error_count = 0
    last_error = ""
    handler_map = {
        "price": _repair_price_gaps,
        "institutional": _repair_institutional_gaps,
        "margin": _repair_margin_gaps,
        "broker_main_force": _repair_broker_gaps,
    }
    handler = handler_map[dataset]

    for trade_date, group in gaps.groupby("trade_date"):
        target_ids = sorted(group["stock_id"].astype(str).tolist())
        try:
            is_trading_day = is_taiwan_stock_trading_day(str(trade_date))
        except Exception as exc:
            logger.warning("trading day check failed %s: %s", trade_date, exc)
            is_trading_day = True
        if not is_trading_day:
            done_count += len(target_ids)
            mark_gap_repair_status(
                run_id,
                dataset,
                str(trade_date),
                target_ids,
                status="market_closed",
                repair_error="官方交易日資料顯示該日休市，無資料屬正常。",
            )
            update_repair_job(job_id, done_count=done_count, error_count=error_count, last_error=last_error)
            continue
        try:
            repaired_ids = handler(str(trade_date), target_ids)
            repaired_list = sorted(repaired_ids)
            failed_ids = sorted(set(target_ids) - repaired_ids)
            if repaired_list:
                done_count += len(repaired_list)
                mark_gap_repair_status(run_id, dataset, str(trade_date), repaired_list, status="repaired")
            if failed_ids:
                error_count += len(failed_ids)
                last_error = f"{trade_date}: {len(failed_ids)} unresolved"
                mark_gap_repair_status(
                    run_id,
                    dataset,
                    str(trade_date),
                    failed_ids,
                    status="error",
                    repair_error="補抓後仍缺漏，可能為上游無資料或個股當日無有效記錄。",
                )
        except Exception as exc:
            error_count += len(target_ids)
            last_error = f"{trade_date}: {exc}"
            mark_gap_repair_status(
                run_id,
                dataset,
                str(trade_date),
                target_ids,
                status="error",
                repair_error=str(exc),
            )
        update_repair_job(job_id, done_count=done_count, error_count=error_count, last_error=last_error)

    final_status = "completed" if error_count == 0 else "partial"
    update_repair_job(
        job_id,
        status=final_status,
        finished_at=datetime.now(),
        done_count=done_count,
        error_count=error_count,
        last_error=last_error,
    )
    return get_repair_job(job_id) or {}