from __future__ import annotations

import logging
import queue
import threading

from db.cache_health import update_health_run, update_repair_job
from modules.cache_health_service import run_health_scan, run_repair_job

logger = logging.getLogger(__name__)


class CacheHealthWorker:
    def __init__(self):
        self._queue: queue.Queue[tuple[str, int]] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self.current_task: str = ""
        self.current_id: int | None = None
        self.running = False

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="cache-health-worker")
        self._thread.start()
        self.running = True

    def stop(self):
        self._stop_event.set()
        self.running = False

    def enqueue_scan(self, run_id: int):
        self._queue.put(("scan", int(run_id)))

    def enqueue_repair(self, job_id: int):
        self._queue.put(("repair", int(job_id)))

    def status(self) -> dict:
        return {
            "running": self.running,
            "queue_size": self._queue.qsize(),
            "current_task": self.current_task,
            "current_id": self.current_id,
        }

    def _run_loop(self):
        while not self._stop_event.is_set():
            try:
                task_type, item_id = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            self.current_task = task_type
            self.current_id = item_id
            try:
                if task_type == "scan":
                    run_health_scan(item_id)
                elif task_type == "repair":
                    run_repair_job(item_id)
            except Exception as exc:
                logger.exception("CacheHealthWorker %s %s failed", task_type, item_id)
                if task_type == "scan":
                    update_health_run(item_id, status="failed", error_message=str(exc))
                elif task_type == "repair":
                    update_repair_job(item_id, status="failed", last_error=str(exc))
            finally:
                self.current_task = ""
                self.current_id = None
                self._queue.task_done()


_WORKER: CacheHealthWorker | None = None
_LOCK = threading.Lock()


def get_worker() -> CacheHealthWorker:
    global _WORKER
    with _LOCK:
        if _WORKER is None:
            _WORKER = CacheHealthWorker()
    return _WORKER