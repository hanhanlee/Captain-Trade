"""
srock 互動監控台

功能：
  - 即時顯示三個服務狀態（每 2 秒刷新）
  - 鍵盤快捷鍵操作（msvcrt on Windows，termios fallback on Unix）
  - --watchdog：背景自動偵測服務掛掉並重啟
  - Tunnel 重啟後自動推播新網址（LINE + Telegram）

鍵盤快捷鍵：
  r  重啟全部    s  停止全部    u  啟動全部
  t  重啟 Streamlit             c  重啟 Auth Proxy
  f  重啟 Tunnel                b  重啟 Telegram Bot
  l  顯示最近 log               q  離開 console（服務繼續執行）
  ?  說明
"""
from __future__ import annotations

import os
import sys
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

from rich.console import Console as RichConsole
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from srock.config import Config
from srock.services import CaddyService, FunnelService, StreamlitService, TelegramBotService


def _setup_windows_terminal():
    """
    PowerShell / conhost 預設不啟用 VT100 處理，Rich 的 escape code 會變亂碼。
    用 Win32 API 強制開啟 ENABLE_VIRTUAL_TERMINAL_PROCESSING，再切換 UTF-8 codepage。
    """
    if sys.platform != "win32":
        return
    import ctypes
    ENABLE_VT = 0x0004
    kernel32 = ctypes.windll.kernel32
    for handle_id in (-10, -11, -12):   # stdin / stdout / stderr
        h = kernel32.GetStdHandle(handle_id)
        mode = ctypes.c_ulong()
        if kernel32.GetConsoleMode(h, ctypes.byref(mode)):
            kernel32.SetConsoleMode(h, mode.value | ENABLE_VT)
    os.system("chcp 65001 > nul 2>&1")
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


_setup_windows_terminal()

_rc = RichConsole(legacy_windows=False, force_terminal=True)

_WATCHDOG_INTERVAL  = 20   # seconds between health checks
_WATCHDOG_COOLDOWN  = 90   # min seconds between auto-restarts of same service
_WATCHDOG_MAX_RETRY = 3    # give up after this many consecutive failures


# ── Key reader (cross-platform) ─────────────────────────────────

def _read_key_nonblocking() -> str | None:
    """Return a single key char if one is waiting, else None. Non-blocking."""
    if sys.platform == "win32":
        import msvcrt
        if msvcrt.kbhit():
            ch = msvcrt.getch()
            try:
                return ch.decode("utf-8").lower()
            except UnicodeDecodeError:
                return None
    else:
        import select, tty, termios
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            if select.select([sys.stdin], [], [], 0)[0]:
                return sys.stdin.read(1).lower()
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
    return None


# ── Service console ─────────────────────────────────────────────

class ServiceConsole:
    def __init__(self, cfg: Config, *, watchdog: bool = False, profile: str = "full"):
        self.cfg      = cfg
        self.watchdog_enabled = watchdog
        self.profile  = profile

        self.streamlit    = StreamlitService(cfg)
        self.caddy        = CaddyService(cfg)
        self.funnel       = FunnelService(cfg)
        self.telegram_bot = TelegramBotService(cfg)

        self._running    = True
        self._executing  = False          # True while a bg action is in progress
        self._key_queue: deque[str] = deque()
        self._msgs: deque[str] = deque(maxlen=6)  # recent status messages
        self._lock = threading.Lock()

        # Watchdog per-service state
        self._wd: dict[str, dict] = {
            name: {"was_running": None, "retries": 0, "last_restart": 0.0}
            for name in ("streamlit", "caddy", "funnel", "telegram")
        }

    # ── Message log ─────────────────────────────────────────────

    def _msg(self, text: str):
        ts = datetime.now().strftime("%H:%M:%S")
        with self._lock:
            self._msgs.append(f"[dim]{ts}[/dim] {text}")

    # ── Panel builder ────────────────────────────────────────────

    def _build_panel(self) -> Panel:
        t = Table.grid(padding=(0, 2))
        t.add_column(style="bold cyan", width=14)
        t.add_column(min_width=18)
        t.add_column(style="dim", no_wrap=True)

        for svc in [self.streamlit.status(), self.caddy.status(), self.funnel.status(), self.telegram_bot.status()]:
            pid_str = f"PID {svc.pid}" if svc.pid else "—"
            badge   = Text("● RUNNING", style="bold green") if svc.running \
                      else Text("○ STOPPED", style="bold red")
            t.add_row(svc.name, badge, f"{pid_str}  {svc.detail}")

        t.add_row()
        st_url = f"http://127.0.0.1:{self.cfg.streamlit_port}"
        t.add_row("Local", Text(st_url, style="link " + st_url), "")
        pub_url = self.funnel.public_url()
        if pub_url:
            t.add_row("Public", Text(pub_url, style="bold yellow", no_wrap=True), "")

        t.add_row()
        with self._lock:
            for m in self._msgs:
                t.add_row("", Text.from_markup(m), "")

        t.add_row()
        wd  = "[green]ON[/green]" if self.watchdog_enabled else "[dim]OFF[/dim]"
        act = "[yellow]執行中...[/yellow]" if self._executing else "[dim]idle[/dim]"
        t.add_row("[dim]Watchdog[/dim]", Text.from_markup(wd), "")
        t.add_row("[dim]Status[/dim]",   Text.from_markup(act), "")
        t.add_row()
        t.add_row(
            "[dim]快捷鍵[/dim]",
            Text("r=重啟全部  s=停止  u=啟動  t=Streamlit  c=Caddy", style="dim"),
            "",
        )
        t.add_row(
            "",
            Text("f=Tunnel  b=TgBot  l=log  q=離開console  ?=說明", style="dim"),
            "",
        )

        now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        title = f"[bold]SROCK CONSOLE[/bold]  [dim]{now}[/dim]"
        return Panel(t, title=title, border_style="bright_black")

    # ── Key reader thread ────────────────────────────────────────

    def _key_reader_worker(self):
        while self._running:
            key = _read_key_nonblocking()
            if key:
                self._key_queue.append(key)
            time.sleep(0.05)

    # ── Action execution (runs in bg thread) ─────────────────────

    def _run_action(self, fn, label: str):
        """Execute fn in a daemon thread so the display keeps refreshing."""
        if self._executing:
            self._msg("[yellow]有動作執行中，請稍候...[/yellow]")
            return

        def _bg():
            self._executing = True
            self._msg(f"[cyan]→ {label}...[/cyan]")
            try:
                fn()
            except Exception as e:
                self._msg(f"[red]✗ {label} 失敗 — {e}[/red]")
            finally:
                self._executing = False

        threading.Thread(target=_bg, daemon=True).start()

    def _restart_all(self):
        self.funnel.stop()
        self.caddy.stop()
        self.streamlit.stop()
        self.telegram_bot.stop()
        time.sleep(0.5)
        self.streamlit.start()
        if self.profile in ("full", "protected"):
            self.caddy.start()
        if self.profile == "full":
            self.funnel.start()
            self._notify_new_url()
        try:
            self.telegram_bot.start()
        except Exception as e:
            self._msg(f"[yellow]⚠ Telegram Bot 啟動失敗 — {e}[/yellow]")
        self._msg("[green]✓ 全部重啟完成[/green]")

    def _stop_all(self):
        self.funnel.stop()
        self.caddy.stop()
        self.streamlit.stop()
        self.telegram_bot.stop()
        self._msg("[yellow]● 全部服務已停止[/yellow]")

    def _start_all(self):
        self.streamlit.start()
        if self.profile in ("full", "protected"):
            self.caddy.start()
        if self.profile == "full":
            self.funnel.start()
            self._notify_new_url()
        try:
            self.telegram_bot.start()
        except Exception as e:
            self._msg(f"[yellow]⚠ Telegram Bot 啟動失敗 — {e}[/yellow]")
        self._msg("[green]✓ 全部服務已啟動[/green]")

    def _restart_tunnel(self):
        self.funnel.stop()
        time.sleep(1)
        self.funnel.start()
        self._notify_new_url()
        self._msg("[green]✓ Tunnel 重啟完成[/green]")

    def _show_logs(self):
        log = self.cfg.streamlit_err_log
        if not log.exists():
            self._msg("[dim]（無 log 檔案）[/dim]")
            return
        lines = log.read_text(encoding="utf-8", errors="replace").splitlines()[-8:]
        for line in lines:
            self._msg(f"[dim]{line[:100]}[/dim]")

    # ── Tunnel URL push ──────────────────────────────────────────

    def _notify_new_url(self):
        url = self.funnel.public_url() or "（URL 待取得）"
        msg = f"🔄 Srock Tunnel 已重啟\n新網址：{url}"
        try:
            from srock.config import ROOT
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

    # ── Key dispatch ─────────────────────────────────────────────

    def _dispatch(self, key: str):
        if key == "q":
            self._running = False
        elif key == "?":
            self._msg(
                "r=重啟全部  s=停止全部  u=啟動全部  "
                "t=Streamlit  c=Caddy  f=Tunnel  b=TgBot  l=log  q=離開"
            )
        elif key == "r":
            self._run_action(self._restart_all, "重啟全部服務")
        elif key == "s":
            self._run_action(self._stop_all, "停止全部服務")
        elif key == "u":
            self._run_action(self._start_all, "啟動全部服務")
        elif key == "t":
            self._run_action(
                lambda: (
                    self.streamlit.restart(),
                    self._msg("[green]✓ Streamlit 重啟完成[/green]"),
                ),
                "重啟 Streamlit",
            )
        elif key == "c":
            self._run_action(
                lambda: (
                    self.caddy.restart(),
                    self._msg("[green]✓ Auth Proxy 重啟完成[/green]"),
                ),
                "重啟 Auth Proxy",
            )
        elif key == "f":
            self._run_action(self._restart_tunnel, "重啟 Tunnel")
        elif key == "b":
            self._run_action(
                lambda: (
                    self.telegram_bot.restart(),
                    self._msg("[green]✓ Telegram Bot 重啟完成[/green]"),
                ),
                "重啟 Telegram Bot",
            )
        elif key == "l":
            self._show_logs()

    # ── Watchdog thread ──────────────────────────────────────────

    def _watchdog_worker(self):
        time.sleep(5)
        while self._running:
            self._watchdog_tick()
            time.sleep(_WATCHDOG_INTERVAL)

    def _watchdog_tick(self):
        now = time.monotonic()
        checks = [
            ("streamlit", self.streamlit),
            ("caddy",     self.caddy),
            ("funnel",    self.funnel),
            ("telegram",  self.telegram_bot),
        ]
        for key, svc in checks:
            state      = self._wd[key]
            is_running = svc.status().running

            if state["was_running"] is None:
                # First tick — just record state, don't act
                state["was_running"] = is_running
                continue

            if state["was_running"] and not is_running:
                if state["retries"] >= _WATCHDOG_MAX_RETRY:
                    continue
                if now - state["last_restart"] < _WATCHDOG_COOLDOWN:
                    continue

                state["retries"]     += 1
                state["last_restart"] = now
                self._msg(
                    f"[yellow]⚠ Watchdog: {svc.name} 掛掉，"
                    f"自動重啟（{state['retries']}/{_WATCHDOG_MAX_RETRY}）[/yellow]"
                )
                try:
                    if self._executing:
                        continue
                    self._executing = True
                    svc.start()
                    self._executing = False
                    self._msg(f"[green]✓ Watchdog: {svc.name} 重啟成功[/green]")
                    if key == "funnel":
                        self._notify_new_url()
                    state["was_running"] = True
                except Exception as e:
                    self._executing = False
                    self._msg(f"[red]✗ Watchdog: {svc.name} 重啟失敗 — {e}[/red]")
            else:
                state["was_running"] = is_running
                if is_running:
                    state["retries"] = 0   # healthy → reset retry counter

    # ── Main loop ────────────────────────────────────────────────

    def run(self):
        threading.Thread(target=self._key_reader_worker, daemon=True).start()

        if self.watchdog_enabled:
            threading.Thread(target=self._watchdog_worker, daemon=True).start()
            self._msg("[green]Watchdog 已啟動[/green]")

        self._run_action(self._start_all, "啟動全部服務")

        _rc.print("\n[dim]Console 已啟動（q = 離開，? = 說明，服務持續執行）[/dim]\n")

        try:
            with Live(console=_rc, refresh_per_second=2, screen=True) as live:
                while self._running:
                    while self._key_queue:
                        self._dispatch(self._key_queue.popleft())
                        if not self._running:
                            break
                    live.update(self._build_panel())
                    time.sleep(0.4)
        except KeyboardInterrupt:
            pass

        _rc.print("[dim]已離開 console，服務繼續執行中。[/dim]")


# ── Entry point ──────────────────────────────────────────────────

def run_console(cfg: Config, *, watchdog: bool = False, profile: str = "full"):
    ServiceConsole(cfg, watchdog=watchdog, profile=profile).run()
