"""Shared helpers for starting the in-process prefetch worker."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def get_prefetch_worker(*, auto_start: bool = True):
    """
    Return the singleton PrefetchWorker and optionally ensure it is running.

    Streamlit pages are executed independently in multipage apps, so relying on
    app.py alone to start the worker is brittle. Pages that display or depend on
    worker state should use this helper.
    """
    from scheduler.prefetch import get_worker

    worker = get_worker()
    if auto_start:
        try:
            worker.start()
        except Exception:
            logger.exception("PrefetchWorker auto-start failed")
            raise
    return worker
