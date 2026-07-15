"""
srock ensure — 冪等自癒檢查（設計給 Task Scheduler 每 5 分鐘跑一次）

哲學：不養常駐 watchdog daemon（daemon 自己也會死），
讓 OS 的工作排程器當監工，每次喚醒做一輪檢查就退出。

每輪流程：
  1. runtime/srock.hold 存在 → 使用者主動 srock down（維護模式），直接退出
  2. 依 profile 檢查各服務：沒跑 → 拉起；跑著但殭屍（HTTP 掛 / heartbeat 停更）→ 砍掉重啟
  3. 防抖動：同一服務兩次自動重啟至少間隔 COOLDOWN_SEC；
     連續 MAX_STRIKES 次拉不起來 → 推播告警一次，退避到 BACKOFF_SEC 再試
  4. 有動作才推 Telegram；結果一律追記 runtime/ensure.log

狀態跨執行保存在 runtime/ensure_state.json（每輪都是新進程）。
"""
from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from srock.config import Config, ROOT
from srock.health import (
    check_heartbeat_health,
    heartbeat_age_seconds,
    streamlit_http_ok,
)
from srock.services import (
    CaddyService,
    FunnelService,
    PrefetchService,
    SchedulerService,
    StreamlitService,
    TelegramBotService,
)

COOLDOWN_SEC = 600     # 同一服務兩次自動重啟的最小間隔
MAX_STRIKES = 3        # 連續重啟失敗這麼多次 → 告警 + 退避
BACKOFF_SEC = 3600     # 退避期間每小時才再試一次
_LOG_MAX_BYTES = 512 * 1024


@dataclass
class EnsureResult:
    name: str
    action: str   # ok / started / restarted / cooldown / backoff / failed / would-start / would-restart
    detail: str = ""


# ── 狀態檔 ──────────────────────────────────────────────────────

def _load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(path: Path, state: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    except OSError:
        pass


def _append_log(path: Path, lines: list[str]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.stat().st_size > _LOG_MAX_BYTES:
            # 簡易 rotation：砍掉前半，保留後半
            text = path.read_text(encoding="utf-8", errors="replace")
            path.write_text(text[len(text) // 2:], encoding="utf-8")
        with open(path, "a", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")
    except OSError:
        pass


# ── Telegram 推播 ───────────────────────────────────────────────

def _push(msg: str) -> None:
    try:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        from notifications.telegram_notify import send_system_message
        send_system_message(msg)
    except Exception:
        pass  # 推播失敗不影響自癒本體


# ── 健康判定 ────────────────────────────────────────────────────

def _diagnose(kind: str, cfg: Config, svc) -> tuple[bool, bool, str]:
    """回 (running, healthy, reason)。healthy=False 且 running=True 代表殭屍。"""
    st = svc.status()
    if not st.running:
        return False, False, "未運行"

    if kind == "streamlit":
        ok, detail = streamlit_http_ok(cfg.streamlit_port)
        return True, ok, detail
    if kind == "scheduler":
        ok, detail = check_heartbeat_health(cfg.scheduler_heartbeat_file, st.pid)
        return True, ok, detail
    if kind == "prefetch":
        ok, detail = check_heartbeat_health(cfg.prefetch_heartbeat_file, st.pid)
        return True, ok, detail
    return True, True, st.detail


# ── 主流程 ──────────────────────────────────────────────────────

def build_service_table(cfg: Config) -> list[tuple[str, object]]:
    """依 profile 決定 ensure 管哪些服務。"""
    profile = cfg.default_profile
    table: list[tuple[str, object]] = [("streamlit", StreamlitService(cfg))]
    if profile in ("full", "protected"):
        table.append(("caddy", CaddyService(cfg)))
    if profile == "full":
        table.append(("funnel", FunnelService(cfg)))
    table.append(("telegram", TelegramBotService(cfg)))
    table.append(("scheduler", SchedulerService(cfg)))
    table.append(("prefetch", PrefetchService(cfg)))
    return table


def run_ensure(cfg: Config, *, dry_run: bool = False) -> list[EnsureResult]:
    now = time.time()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if cfg.hold_file.exists():
        _append_log(cfg.ensure_log, [f"[{ts}] hold 檔存在（維護模式），跳過"])
        return [EnsureResult("*", "hold", "runtime/srock.hold 存在，維護模式中")]

    state = _load_state(cfg.ensure_state_file)
    results: list[EnsureResult] = []
    log_lines: list[str] = []
    pushes: list[str] = []

    for kind, svc in build_service_table(cfg):
        entry = state.setdefault(kind, {"strikes": 0, "last_restart": 0.0, "alerted": False})
        running, healthy, reason = _diagnose(kind, cfg, svc)

        if running and healthy:
            entry["strikes"] = 0
            entry["alerted"] = False
            results.append(EnsureResult(svc.name, "ok", reason))
            continue

        # 需要拉起（未運行）或砍掉重啟（殭屍）
        verb = "重啟" if running else "啟動"
        why = f"殭屍：{reason}" if running else "未運行"

        if entry["strikes"] >= MAX_STRIKES:
            if now - entry["last_restart"] < BACKOFF_SEC:
                results.append(EnsureResult(svc.name, "backoff",
                                            f"{why}；連續失敗 {entry['strikes']} 次，退避中"))
                continue
            entry["strikes"] = MAX_STRIKES - 1  # 退避期滿，再給一次機會

        if now - entry["last_restart"] < COOLDOWN_SEC:
            results.append(EnsureResult(svc.name, "cooldown", f"{why}；距上次重啟未滿 10 分鐘"))
            continue

        if dry_run:
            results.append(EnsureResult(svc.name, f"would-{'restart' if running else 'start'}", why))
            continue

        entry["last_restart"] = now
        try:
            if running:  # 殭屍 → 先停
                try:
                    svc.stop()
                    time.sleep(1.0)
                except Exception:
                    pass
            msg = svc.start()
            entry["strikes"] = 0
            entry["alerted"] = False
            action = "restarted" if running else "started"
            results.append(EnsureResult(svc.name, action, f"{why} → {msg}"))
            log_lines.append(f"[{ts}] {svc.name} {action}: {why} → {msg}")

            push_msg = f"🔧 srock ensure：{svc.name} 已自動{verb}\n原因：{why}"
            if kind == "funnel":
                url = svc.public_url() or "（URL 待取得）"
                push_msg += f"\n新網址:{url}"
            pushes.append(push_msg)
        except Exception as e:
            entry["strikes"] += 1
            results.append(EnsureResult(svc.name, "failed",
                                        f"{why}；{verb}失敗（{entry['strikes']}/{MAX_STRIKES}）：{e}"))
            log_lines.append(f"[{ts}] {svc.name} FAILED ({entry['strikes']}/{MAX_STRIKES}): {e}")
            if entry["strikes"] >= MAX_STRIKES and not entry["alerted"]:
                entry["alerted"] = True
                pushes.append(
                    f"🚨 srock ensure：{svc.name} 連續 {MAX_STRIKES} 次{verb}失敗，"
                    f"暫停自動重試一小時，請人工檢查。\n最後錯誤：{e}"
                )

    if not dry_run:
        _save_state(cfg.ensure_state_file, state)
        if not log_lines:
            log_lines.append(f"[{ts}] all healthy")
        _append_log(cfg.ensure_log, log_lines)
        for msg in pushes:
            _push(msg)

    return results
