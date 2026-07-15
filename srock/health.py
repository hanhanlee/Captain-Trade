"""
服務健康檢查（供 srock ensure 與 TUI 共用）

判定分兩層：
  running — 進程活著 / port 有開（沿用 services.py 的 status()）
  healthy — 進一步確認服務真的在做事：
      Streamlit  → HTTP GET /_stcore/health 回 200
      Scheduler  → runtime/scheduler.heartbeat 檔案夠新
      Prefetch   → runtime/prefetch.heartbeat 檔案夠新
      其他       → running 即 healthy

heartbeat 相容性規則（重要）：
  舊版進程不會寫 heartbeat 檔。因此「檔案不存在」不算不健康，
  只有「檔案存在但過期，且進程本身也跑超過門檻時間」才判殭屍——
  避免 ensure 誤殺升級前就在跑的健康進程，也給剛重啟的進程寫檔緩衝。
"""
from __future__ import annotations

import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# heartbeat 超過這個秒數沒更新 → 視為殭屍（scheduler 每 60s 寫一次，給 15 倍餘裕）
HEARTBEAT_STALE_SEC = 900


@dataclass
class HealthReport:
    name: str
    running: bool
    healthy: bool
    reason: str = ""


# ── heartbeat 讀寫 ──────────────────────────────────────────────

def write_heartbeat(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(datetime.now().isoformat(timespec="seconds"), encoding="ascii")
    except OSError:
        pass  # heartbeat 寫失敗不能拖垮本體


def heartbeat_age_seconds(path: Path) -> float | None:
    """回傳 heartbeat 檔距今幾秒；檔案不存在回 None。"""
    try:
        return max(0.0, time.time() - path.stat().st_mtime)
    except OSError:
        return None


def format_age(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 90:
        return f"{seconds:.0f}s 前"
    if seconds < 5400:
        return f"{seconds / 60:.0f}m 前"
    return f"{seconds / 3600:.1f}h 前"


# ── 個別檢查 ────────────────────────────────────────────────────

def streamlit_http_ok(port: int, timeout: float = 5.0) -> tuple[bool, str]:
    """打 Streamlit 內建 health endpoint，回 (ok, 說明)。"""
    url = f"http://127.0.0.1:{port}/_stcore/health"
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            ms = (time.monotonic() - t0) * 1000
            if resp.status == 200:
                return True, f"HTTP 200 ({ms:.0f}ms)"
            return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, f"HTTP 失敗：{type(e).__name__}"


def _process_uptime_seconds(pid: int | None) -> float | None:
    if pid is None:
        return None
    try:
        import psutil
        return max(0.0, time.time() - psutil.Process(pid).create_time())
    except Exception:
        return None


def check_heartbeat_health(
    hb_file: Path,
    pid: int | None,
    stale_sec: float = HEARTBEAT_STALE_SEC,
) -> tuple[bool, str]:
    """
    heartbeat 型服務（Scheduler / Prefetch）的殭屍判定。
    回 (healthy, 說明)。呼叫端須先確認進程 running。
    """
    age = heartbeat_age_seconds(hb_file)
    if age is None:
        return True, "無 heartbeat 檔（舊版進程，僅檢查 PID）"
    if age <= stale_sec:
        return True, f"♥ {format_age(age)}"

    uptime = _process_uptime_seconds(pid)
    if uptime is not None and uptime < stale_sec:
        # 剛重啟、還沒輪到第一次寫檔（或殘留舊檔）→ 給緩衝
        return True, f"剛啟動 {format_age(uptime)}，heartbeat 待更新"
    return False, f"heartbeat 停更 {format_age(age)}（疑似卡死）"
