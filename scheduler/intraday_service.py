"""
Built-in intraday portfolio monitor scheduler.

Runs inside the Streamlit process so users do not need a separate
``python scheduler/jobs.py`` window for per-minute portfolio monitoring.
"""
from __future__ import annotations

import logging
from datetime import datetime, time as dtime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None
_last_run_at: datetime | None = None
_last_sent_count: int = 0
_last_error: str = ""


def _job_intraday_monitor(*, ignore_cutoff: bool = False) -> None:
    """Run one intraday monitor pass with the same 13:30 cutoff as jobs.py."""
    global _last_run_at, _last_sent_count, _last_error

    now = datetime.now()
    if not ignore_cutoff and now.time() > dtime(13, 30):
        return

    _last_run_at = now
    try:
        from modules.intraday_monitor import run_intraday_check

        sent = int(run_intraday_check() or 0)
        _last_sent_count = sent
        _last_error = ""
        if sent:
            logger.info("內建盤中持股監控：推播 %s 則警示", sent)
    except Exception as exc:
        _last_error = str(exc)
        logger.exception("內建盤中持股監控失敗")


def start_intraday_scheduler() -> bool:
    """Start the built-in per-minute intraday monitor scheduler."""
    global _scheduler

    if _scheduler is not None and _scheduler.running:
        return True

    scheduler = BackgroundScheduler(timezone="Asia/Taipei", daemon=True)
    scheduler.add_job(
        _job_intraday_monitor,
        CronTrigger(day_of_week="mon-fri", hour="9-13", minute="*", timezone="Asia/Taipei"),
        id="built_in_intraday_monitor",
        name="內建盤中持股監控",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    scheduler.start()
    _scheduler = scheduler
    logger.info("內建盤中持股監控排程器已啟動")
    return True


def stop_intraday_scheduler() -> None:
    """Stop the built-in intraday monitor scheduler."""
    global _scheduler

    if _scheduler is not None:
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            logger.exception("停止內建盤中持股監控排程器失敗")
    _scheduler = None
    logger.info("內建盤中持股監控排程器已停止")


def sync_intraday_scheduler_from_settings() -> dict:
    """Start or stop the scheduler according to persisted app settings."""
    from db.settings import get_intraday_monitor_scheduler_enabled

    if get_intraday_monitor_scheduler_enabled():
        start_intraday_scheduler()
    else:
        stop_intraday_scheduler()
    return status()


def run_once() -> int:
    """Run one monitor pass immediately, useful for UI testing."""
    before = _last_sent_count
    _job_intraday_monitor(ignore_cutoff=True)
    return _last_sent_count if _last_run_at else before


def status() -> dict:
    """Return a compact status snapshot for the Streamlit UI."""
    running = bool(_scheduler is not None and _scheduler.running)
    next_run = None
    if running:
        try:
            job = _scheduler.get_job("built_in_intraday_monitor")
            next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
        except Exception:
            next_run = None
    return {
        "running": running,
        "last_run_at": _last_run_at,
        "last_sent_count": _last_sent_count,
        "last_error": _last_error,
        "next_run_time": next_run,
    }
