"""
srock Textual TUI 控制台

取代舊版 rich Live console（srock console --classic 仍可用）。
純儀表板定位：自癒交給 Task Scheduler 的「Srock Ensure」任務，
這裡只負責看狀態、手動啟停、看 log。

操作：
  滑鼠  — 每列的 [重啟] [停止] [log] 按鈕、底部全域按鈕
  鍵盤  — r 全部重啟   s 全部停止   u 全部啟動   q 離開（服務繼續執行）
"""
from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, Footer, RichLog, Static

from rich.markup import escape as rich_escape

from srock.config import Config, ROOT
from srock.health import check_heartbeat_health, streamlit_http_ok
from srock.services import (
    CaddyService,
    FunnelService,
    PrefetchService,
    SchedulerService,
    StreamlitService,
    TelegramBotService,
)

_REFRESH_SEC = 3.0        # pid/port 快照
_HEALTH_REFRESH_SEC = 12.0  # HTTP / heartbeat 深度檢查（成本較高，頻率放低）


def _service_table(cfg: Config, profile: str) -> list[tuple[str, object, Path]]:
    """(kind, service, err_log) 清單；funnel 的 log 路徑 services 沒公開，自行組。"""
    rows: list[tuple[str, object, Path]] = [
        ("streamlit", StreamlitService(cfg), cfg.streamlit_err_log),
    ]
    if profile in ("full", "protected"):
        rows.append(("caddy", CaddyService(cfg), cfg.caddy_err_log))
    if profile == "full":
        rows.append(("funnel", FunnelService(cfg), cfg.runtime_dir / "cloudflared.err.log"))
    rows += [
        ("telegram", TelegramBotService(cfg), cfg.telegram_bot_err_log),
        ("scheduler", SchedulerService(cfg), cfg.scheduler_err_log),
        ("prefetch", PrefetchService(cfg), cfg.prefetch_err_log),
    ]
    return rows


class ServiceRow(Horizontal):
    """一列服務：狀態燈 + 名稱 + 詳情 + 動作按鈕。"""

    def __init__(self, kind: str, name: str) -> None:
        super().__init__(id=f"row-{kind}", classes="service-row")
        self.kind = kind
        self.svc_name = name

    def compose(self) -> ComposeResult:
        yield Static("○", id=f"dot-{self.kind}", classes="dot stopped")
        yield Static(self.svc_name, classes="svc-name")
        yield Static("…", id=f"detail-{self.kind}", classes="svc-detail")
        yield Button("重啟", id=f"restart-{self.kind}", classes="act", variant="primary")
        yield Button("停止", id=f"stop-{self.kind}", classes="act", variant="error")
        yield Button("log", id=f"log-{self.kind}", classes="act")

    def update_status(self, running: bool, detail: str) -> None:
        dot = self.query_one(f"#dot-{self.kind}", Static)
        dot.update("●" if running else "○")
        dot.set_classes("dot running" if running else "dot stopped")
        self.query_one(f"#detail-{self.kind}", Static).update(detail)
        self.query_one(f"#restart-{self.kind}", Button).label = "重啟" if running else "啟動"
        self.query_one(f"#stop-{self.kind}", Button).disabled = not running


class SrockTui(App):
    TITLE = "SROCK CONTROL"

    CSS = """
    Screen { layout: vertical; }

    #urlbar {
        height: 1;
        padding: 0 1;
        background: $surface;
        color: $text-muted;
    }

    #services { height: auto; padding: 0 1; }

    .service-row { height: 3; align-vertical: middle; }
    .dot { width: 2; content-align: center middle; height: 3; }
    .dot.running { color: $success; text-style: bold; }
    .dot.stopped { color: $error; text-style: bold; }
    .svc-name { width: 14; height: 3; content-align: left middle; text-style: bold; }
    .svc-detail { width: 1fr; height: 3; content-align: left middle; color: $text-muted; }
    .act { min-width: 8; margin: 0 1 0 0; }

    #bottom { height: 1fr; padding: 0 1; }
    #messages { width: 2fr; border: round $primary 30%; }
    #health { width: 1fr; border: round $primary 30%; padding: 0 1; }

    #globalbar { height: 3; padding: 0 1; }
    #globalbar Button { margin: 0 1 0 0; }
    #ensure-badge { width: 1fr; height: 3; content-align: right middle; color: $text-muted; }
    """

    BINDINGS = [
        ("r", "restart_all", "全部重啟"),
        ("s", "stop_all", "全部停止"),
        ("u", "start_all", "全部啟動"),
        ("q", "quit", "離開（服務續跑）"),
    ]

    def __init__(self, cfg: Config, profile: str = "full") -> None:
        super().__init__()
        self.cfg = cfg
        self.profile = profile
        self.rows = _service_table(cfg, profile)
        self._busy = False

    # ── 版面 ────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Static("", id="urlbar")
        with Vertical(id="services"):
            for kind, svc, _log in self.rows:
                yield ServiceRow(kind, svc.name)
        with Horizontal(id="bottom"):
            yield RichLog(id="messages", markup=True, wrap=True)
            yield Static("", id="health")
        with Horizontal(id="globalbar"):
            yield Button("全部重啟", id="all-restart", variant="primary")
            yield Button("全部停止", id="all-stop", variant="error")
            yield Button("全部啟動", id="all-start", variant="success")
            yield Static("", id="ensure-badge")
        yield Footer()

    def on_mount(self) -> None:
        self._msg("[dim]TUI 已啟動 — q 離開（服務繼續執行）；自癒由 Task Scheduler『Srock Ensure』負責[/dim]")
        self.set_interval(_REFRESH_SEC, self._refresh_status)
        self.set_interval(_HEALTH_REFRESH_SEC, self._refresh_health)
        self._refresh_status()
        self._refresh_health()

    # ── 訊息區 ──────────────────────────────────────────────────

    def _msg(self, text: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.query_one("#messages", RichLog).write(f"[dim]{ts}[/dim] {text}")

    # ── 狀態刷新（thread worker，避免 psutil 卡 UI）─────────────

    @work(thread=True, exclusive=True, group="status")
    def _refresh_status(self) -> None:
        snap = []
        for kind, svc, _log in self.rows:
            try:
                st = svc.status()
                snap.append((kind, st.running, f"{'PID ' + str(st.pid) if st.pid else '—'}  {st.detail}"))
            except Exception as e:
                snap.append((kind, False, f"status 失敗：{e}"))
        try:
            funnel = next(svc for kind, svc, _ in self.rows if kind == "funnel")
            pub = funnel.public_url()
        except StopIteration:
            pub = None
        local = f"http://127.0.0.1:{self.cfg.streamlit_port}"
        self.call_from_thread(self._apply_status, snap, local, pub)

    def _apply_status(self, snap, local: str, pub: str | None) -> None:
        for kind, running, detail in snap:
            self.query_one(f"#row-{kind}", ServiceRow).update_status(running, detail)
        url_text = f"Local [bold]{local}[/bold]"
        if pub:
            url_text += f"   Public [bold yellow]{pub}[/bold yellow]"
        self.query_one("#urlbar", Static).update(url_text)

    @work(thread=True, exclusive=True, group="health")
    def _refresh_health(self) -> None:
        cfg = self.cfg
        lines: list[str] = ["[bold]健康檢查[/bold]", ""]

        ok, detail = streamlit_http_ok(cfg.streamlit_port, timeout=3)
        mark = "[green]✓[/green]" if ok else "[red]✗[/red]"
        lines.append(f"{mark} Streamlit  {detail}")

        for label, hb, svc_cls in (
            ("Scheduler", cfg.scheduler_heartbeat_file, SchedulerService),
            ("Prefetch ", cfg.prefetch_heartbeat_file, PrefetchService),
        ):
            st = svc_cls(cfg).status()
            if not st.running:
                lines.append(f"[red]✗[/red] {label}  未運行")
                continue
            healthy, why = check_heartbeat_health(hb, st.pid)
            mark = "[green]✓[/green]" if healthy else "[red]✗[/red]"
            lines.append(f"{mark} {label}  {why}")

        lines.append("")
        if cfg.hold_file.exists():
            lines.append("[magenta]⏸ 維護模式（srock.hold）[/magenta]")
            lines.append("[dim]ensure 暫停看守，srock up/resume 解除[/dim]")
        else:
            last = self._last_ensure_line()
            lines.append("[bold]Watchdog[/bold]（Srock Ensure / 5 分鐘）")
            lines.append(f"[dim]{last}[/dim]" if last else "[yellow]尚無 ensure 紀錄[/yellow]")

        self.call_from_thread(
            self.query_one("#health", Static).update, "\n".join(lines)
        )
        badge = "⏸ 維護模式" if cfg.hold_file.exists() else "Watchdog: Srock Ensure ✓"
        self.call_from_thread(self.query_one("#ensure-badge", Static).update, badge)

    def _last_ensure_line(self) -> str:
        try:
            lines = self.cfg.ensure_log.read_text(encoding="utf-8", errors="replace").splitlines()
            return lines[-1][:60] if lines else ""
        except OSError:
            return ""

    # ── 動作 ────────────────────────────────────────────────────

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "all-restart":
            self.action_restart_all()
        elif bid == "all-stop":
            self.action_stop_all()
        elif bid == "all-start":
            self.action_start_all()
        elif "-" in bid:
            action, kind = bid.split("-", 1)
            if action == "log":
                self._show_log(kind)
            elif action in ("restart", "stop"):
                self._service_action(action, kind)

    def _find(self, kind: str):
        for k, svc, log in self.rows:
            if k == kind:
                return svc, log
        return None, None

    def _guard_busy(self) -> bool:
        if self._busy:
            self._msg("[yellow]有動作執行中，請稍候…[/yellow]")
            return True
        return False

    def _service_action(self, action: str, kind: str) -> None:
        if self._guard_busy():
            return
        svc, _log = self._find(kind)
        if svc is None:
            return
        running = False
        try:
            running = svc.status().running
        except Exception:
            pass
        label = "重啟" if (action == "restart" and running) else ("啟動" if action == "restart" else "停止")
        self._run_bg(action, kind, f"{label} {svc.name}")

    @work(thread=True, exclusive=True, group="action")
    def _run_bg(self, action: str, kind: str, label: str) -> None:
        self._busy = True
        self.call_from_thread(self._msg, f"[cyan]→ {label}…[/cyan]")
        svc, _log = self._find(kind)
        try:
            if action == "restart":
                msg = svc.restart() if svc.status().running else svc.start()
                if kind == "funnel":
                    self._notify_new_url()
            else:
                msg = svc.stop()
                self.call_from_thread(
                    self._msg,
                    "[dim]提示：Srock Ensure 會在 5 分鐘內自動拉回；要長時間停用請先 srock hold[/dim]",
                )
            self.call_from_thread(self._msg, f"[green]✓ {msg.splitlines()[-1]}[/green]")
        except Exception as e:
            self.call_from_thread(self._msg, f"[red]✗ {label} 失敗 — {e}[/red]")
        finally:
            self._busy = False
            self.call_from_thread(self._refresh_status)

    # ── 全域動作 ────────────────────────────────────────────────

    def action_restart_all(self) -> None:
        if self._guard_busy():
            return
        self._all_bg("restart")

    def action_stop_all(self) -> None:
        if self._guard_busy():
            return
        self._all_bg("stop")

    def action_start_all(self) -> None:
        if self._guard_busy():
            return
        self._all_bg("start")

    @work(thread=True, exclusive=True, group="action")
    def _all_bg(self, mode: str) -> None:
        self._busy = True
        verb = {"restart": "重啟", "stop": "停止", "start": "啟動"}[mode]
        self.call_from_thread(self._msg, f"[cyan]→ 全部{verb}…[/cyan]")
        try:
            if mode in ("restart", "stop"):
                for kind, svc, _log in reversed(self.rows):
                    try:
                        svc.stop()
                    except Exception as e:
                        self.call_from_thread(self._msg, f"[yellow]⚠ 停止 {svc.name} 失敗 — {e}[/yellow]")
            if mode in ("restart", "start"):
                time.sleep(0.5)
                for kind, svc, _log in self.rows:
                    try:
                        svc.start()
                        if kind == "funnel":
                            self._notify_new_url()
                    except Exception as e:
                        self.call_from_thread(self._msg, f"[yellow]⚠ 啟動 {svc.name} 失敗 — {e}[/yellow]")
            if mode == "stop":
                self.call_from_thread(
                    self._msg,
                    "[dim]提示：Srock Ensure 會在 5 分鐘內自動拉回；要長時間停用請先 srock hold[/dim]",
                )
            self.call_from_thread(self._msg, f"[green]✓ 全部{verb}完成[/green]")
        finally:
            self._busy = False
            self.call_from_thread(self._refresh_status)

    # ── log 檢視 ────────────────────────────────────────────────

    def _show_log(self, kind: str) -> None:
        svc, log = self._find(kind)
        if log is None or not log.exists():
            self._msg(f"[dim]（{kind} 無 log 檔案）[/dim]")
            return
        try:
            lines = log.read_text(encoding="utf-8", errors="replace").splitlines()[-15:]
        except OSError as e:
            self._msg(f"[red]讀 log 失敗：{e}[/red]")
            return
        self._msg(f"[bold]── {log.name}（最後 {len(lines)} 行）──[/bold]")
        rlog = self.query_one("#messages", RichLog)
        for line in lines:
            rlog.write(f"[dim]{rich_escape(line[:160])}[/dim]")

    # ── Tunnel URL 推播（沿用舊 console 行為）───────────────────

    def _notify_new_url(self) -> None:
        svc, _ = self._find("funnel")
        if svc is None:
            return
        url = svc.public_url() or "（URL 待取得）"
        msg = f"🔄 Srock Tunnel 已重啟\n新網址：{url}"
        try:
            if str(ROOT) not in sys.path:
                sys.path.insert(0, str(ROOT))
            try:
                from notifications.line_notify import send_multicast
                send_multicast(msg)
            except Exception:
                pass
            try:
                from notifications.telegram_notify import send_system_message
                send_system_message(msg)
            except Exception:
                pass
        except Exception:
            pass


def run_tui(cfg: Config, profile: str = "full") -> None:
    SrockTui(cfg, profile=profile).run()
