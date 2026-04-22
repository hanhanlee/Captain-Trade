from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def get_cache_health_worker(*, auto_start: bool = True):
    from modules.cache_health_worker import get_worker

    worker = get_worker()
    if auto_start:
        try:
            worker.start()
        except Exception:
            logger.exception("CacheHealthWorker auto-start failed")
            raise
    return worker